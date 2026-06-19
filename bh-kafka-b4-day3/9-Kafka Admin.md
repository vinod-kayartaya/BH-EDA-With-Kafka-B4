# Kafka Topic Management Reference Guide

This guide provides a comprehensive reference on Kafka topic management using the Java AdminClient API, covering key abstractions, configuration properties, operational workflows, and practical coding patterns.

---

## 1. Introduction

### What is the Kafka AdminClient?
While Producers write events and Consumers read them, the Kafka `AdminClient` is the administrative control center of a Kafka application. It is a specialized client API used to programmatically manage and inspect cluster state, including topics, partitions, configurations, consumer groups, ACLs, and broker metadata.

### Topic Management Workflow (From Client to Cluster)
When an administrative operation (like creating or deleting a topic) is triggered, it goes through a coordinated sequence of internal steps:
1. **Metadata Discovery:** The `AdminClient` connects to bootstrap servers to discover the cluster topology and identify the **Active Controller** broker.
2. **Controller Routing:** Administrative requests (such as creating a topic, modifying configs, or increasing partitions) are routed directly to the Active Controller broker, which holds the write lock for cluster state.
3. **Controller Execution & Log Sync (KRaft/Zookeeper):** The Controller receives the request, validates config parameters, and writes the state changes to the metadata log (KRaft quorum or Zookeeper).
4. **Metadata Propagation:** The Controller sends metadata updates (`UpdateMetadataRequest` and `LeaderAndIsrRequest`) to all brokers in the cluster, instructing them to allocate local directories and initialize logs for the new partition replicas.
5. **Future Resolution:** Once all partition replicas are successfully provisioned or state updates are committed, the broker returns a success code to the client, which resolves the operation's asynchronous `KafkaFuture`.

### Scenario Context: System Administration
We utilize the **Payment Processing & Fraud Detection System** as our business scenario.
* **The Administrator's Role:** Before services can exchange events, topics must be properly provisioned. The administration service dynamically configures the `transactions` topic with appropriate partitions (for horizontal scaling) and configs (like `cleanup.policy=delete` and `retention.ms=86400000`).

---

## 2. Key Java Classes & Interfaces

To manage topics programmatically, developers interact with classes in the `org.apache.kafka.clients.admin` package.

### 1. `AdminClient` (Class)
The principal entry point for administration. It is a thread-safe client that manages connections to the cluster.
* *Key methods:*
  * `create(Properties)`: Instantiates a new client.
  * `createTopics(Collection<NewTopic>)`: Asynchronously creates new topics.
  * `deleteTopics(Collection<String>)`: Asynchronously deletes topics.
  * `listTopics()`: Lists topics in the cluster.
  * `describeTopics(Collection<String>)`: Retrieves detailed topic metadata (partitions, replicas, ISRs).
  * `describeConfigs(Collection<ConfigResource>)`: Retrieves topic or broker configurations.
  * `incrementalAlterConfigs(Map<ConfigResource, Collection<AlterConfigOp>>)`: Dynamically overrides configurations.
  * `createPartitions(Map<String, NewPartitions>)`: Increases partition counts.
  * `close()`: Closes the client and releases connections.

### 2. `NewTopic` (Class)
Represents a topic creation request. It wraps:
* `name` (String): The topic name.
* `numPartitions` (Integer): Number of partitions (or default if omitted).
* `replicationFactor` (Short): Number of replica copies across brokers.
* `configs` (Map<String, String>): Topic-level configuration overrides.

### 3. `TopicDescription` (Class)
Describes the topology of an existing topic. It provides:
* `name()`: Name of the topic.
* `isInternal()`: Boolean indicating if it is an internal system topic (like `__consumer_offsets`).
* `partitions()`: A list of `TopicPartitionInfo` detailing partition leader brokers, replicas, and In-Sync Replicas (ISRs).

### 4. `AlterConfigOp` (Class)
Represents a single configuration modification operation. It wraps a `ConfigEntry` (key-value property override) and an `OpType` (e.g., `SET` to modify/add a config, `DELETE` to clear/restore default config).

### 5. `NewPartitions` (Class)
Defines partition count updates. Use `NewPartitions.increaseTo(int totalCount)` to request partition expansion.

### 6. `KafkaFuture<T>` (Class)
A specialized asynchronous future class returned by `AdminClient` results. Calling `.get()` on the future blocks until the server confirms the change, or throws an exception if it fails.

---

## 3. Core Configuration Properties

Defining an `AdminClient` requires a properties map containing bootstrap information.

### Essential Configurations
* **`bootstrap.servers`** (Type: String, Default: *None*):
  List of brokers used to discover cluster metadata.
* **`request.timeout.ms`** (Type: Integer, Default: `30000` / 30 sec):
  The maximum time the client waits for the broker to respond to administrative requests.
* **`client.id`** (Type: String, Default: `""`):
  An optional tracking string to identify the admin client in broker logs.

---

## 4. Programming Patterns & Code Examples

Here are the primary administrative patterns used to manage topic resources.

### Pattern 1: Topic Creation
This pattern initializes the administrative client, wraps the details of the desired topic (name, partition count, and replica count) into a `NewTopic` configuration, and submits it to the cluster. The future is resolved using `.all().get()` to block until creation is confirmed.

```java
Properties props = new Properties();
props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093");

try (AdminClient admin = AdminClient.create(props)) {
    NewTopic newTopic = new NewTopic("transactions", 3, (short) 1);
    
    // Set optional configurations
    Map<String, String> configs = new HashMap<>();
    configs.put("cleanup.policy", "delete");
    newTopic.configs(configs);

    // Request asynchronous creation
    CreateTopicsResult result = admin.createTopics(Collections.singletonList(newTopic));
    
    // Block until broker confirms success
    result.all().get();
    System.out.println("Topic created successfully.");
} catch (Exception e) {
    e.printStackTrace();
}
```

### Pattern 2: Listing & Describing Topics
This pattern retrieves the list of existing topics from the cluster and fetches detailed metadata for a specific topic, printing its partitions, leader nodes, and replication statuses.

```java
try (AdminClient admin = AdminClient.create(props)) {
    // 1. List topics
    Set<String> topics = admin.listTopics().names().get();
    System.out.println("Existing topics: " + topics);

    // 2. Describe specific topic details
    Map<String, TopicDescription> descMap = admin.describeTopics(Collections.singletonList("transactions")).allTopicNames().get();
    TopicDescription desc = descMap.get("transactions");
    
    System.out.println("Topic Name: " + desc.name());
    desc.partitions().forEach(partition -> {
        System.out.printf("  Partition: %d | Leader: %s | Replicas: %s%n",
                partition.partition(), partition.leader(), partition.replicas());
    });
}
```

### Pattern 3: Altering Topic Configurations
This pattern overrides default topic configurations dynamically (e.g., updating message log retention time). It defines a topic resource and uses `incrementalAlterConfigs` to set the new key-value parameters.

```java
try (AdminClient admin = AdminClient.create(props)) {
    ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, "transactions");
    ConfigEntry retentionConfig = new ConfigEntry("retention.ms", "86400000"); // 1 day
    
    // Define the SET operation
    AlterConfigOp op = new AlterConfigOp(retentionConfig, AlterConfigOp.OpType.SET);
    Map<ConfigResource, Collection<AlterConfigOp>> updates = new HashMap<>();
    updates.put(resource, Collections.singletonList(op));

    // Execute the alter
    admin.incrementalAlterConfigs(updates).all().get();
    System.out.println("Topic configuration updated successfully.");
}
```

### Pattern 4: Increasing Partition Count
This pattern expands partition capacity to enable higher read/write throughput. Note that partition counts can only be increased, never decreased, because removing a partition would result in data loss.

```java
try (AdminClient admin = AdminClient.create(props)) {
    Map<String, NewPartitions> partitionsMap = new HashMap<>();
    
    // Request partition expansion to 6 partitions
    partitionsMap.put("transactions", NewPartitions.increaseTo(6));

    admin.createPartitions(partitionsMap).all().get();
    System.out.println("Partitions increased successfully.");
}
```

### Pattern 5: Topic Deletion
This pattern removes a topic and its associated transaction logs from the brokers. Once deleted, the action cannot be undone.

```java
try (AdminClient admin = AdminClient.create(props)) {
    DeleteTopicsResult result = admin.deleteTopics(Collections.singletonList("transactions"));
    result.all().get();
    System.out.println("Topic deleted successfully.");
}
```

---

## 5. Advanced Topic Management Recipes

### Overriding Configuration Hierarchy
Brokers maintain default settings for topics. When you define properties using the AdminClient:
1. **Topic-Level Override:** Explicit overrides (like `retention.ms`) set on a topic take absolute precedence.
2. **Broker-Default Fallback:** If a config is omitted or deleted using `AlterConfigOp.OpType.DELETE`, the topic automatically rolls back to the broker's default setting (defined in `server.properties` on the brokers).

### Partition Count Decisions
Increasing partitions on an active topic alters key-to-partition mapping (`hash(key) % partitionCount`). Any producer routing events using keys will begin writing related events to different partitions, breaking ordering guarantees for consumers. Ensure partition counts are sized appropriately up-front.
