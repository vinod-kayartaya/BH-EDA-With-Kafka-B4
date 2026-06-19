# Kafka Consumer Reference Guide

This guide provides a comprehensive reference on Kafka Consumers, covering key API abstractions, details on configuration properties, internal architecture, and coding patterns.

---

## 1. Introduction

### What is a Kafka Consumer?

In event-driven architectures, a Kafka Consumer is the client application that reads (consumes) records from Kafka topics. Unlike traditional messaging queues that push messages and delete them upon receipt, Kafka consumers use a **pull model**, fetching messages at their own pace. The messages remain stored durably on the brokers, allowing other consumers or groups to consume the same events.

### Message Lifecycle & Workflow (From Cluster to Client)
When a consumer client starts up and consumes records, it goes through a coordinated sequence of internal steps:
1. **Subscription & Coordinator Discovery:** The client calls `subscribe()` on the `KafkaConsumer` instance. It contacts the `bootstrap.servers` to discover the cluster metadata and locate the **Group Coordinator** broker assigned to its `group.id`.
2. **Group Coordination & Partition Assignment:** The consumer joins the group. The Group Coordinator orchestrates a rebalance, electing a consumer group leader to compute partition assignments based on the `partition.assignment.strategy` (e.g., Range, RoundRobin, Sticky). The coordinator distributes the final assignments back to all members.
3. **Offset Fetching:** The consumer fetches the last committed offsets for its assigned partitions from the coordinator. If no offset bookmark exists (e.g., a new consumer group), it initializes the offset based on `auto.offset.reset` (`earliest` or `latest`).
4. **Data Prefetching & Buffering:** The application thread invokes `.poll(Duration)`. Under the hood, the consumer's internal **Fetcher** component retrieves batches of compressed bytes from broker partition leaders, buffering them in client memory socket buffers.
5. **Deserialization & Poll Delivery:** The fetched byte batches are decrypted and deserialized using the configured `key.deserializer` and `value.deserializer`. They are wrapped into `ConsumerRecords` and returned to the application processing loop.
6. **Heartbeat Maintenance:** While the application thread is processing messages, an internal background thread periodically sends keep-alive pings to the Group Coordinator (at `heartbeat.interval.ms` intervals) to confirm the consumer is healthy.
7. **Offset Committing:** Once processed, the offsets are written back to the `__consumer_offsets` topic on the broker (either automatically at intervals or manually via `commitSync()` / `commitAsync()`), updating the bookmark position for the next poll cycle.

### Scenario Context: Fraud Detection System

We utilize the **Fraud Detection System** as our business scenario.

- **The Consumer's Role:** The Fraud Detection Service acts as the consumer group. It subscribes to the `transactions` topic, reading payment events in real-time to check for suspicious activity.
- **Goal:** Distribute the processing load across multiple instances of the service, handle partition rebalancing during crashes or scaling, and track offsets reliably to guarantee no events are skipped.

---

## 2. Key Java Classes & Interfaces

To write a Kafka consumer, developers interact with several core classes and interfaces inside the `org.apache.kafka.clients.consumer` package.

### 1. `KafkaConsumer<K, V>` (Class)

The entry point client. It is single-threaded and handles network connections, partition assignments, heartbeats, and offsets.

- _Key methods:_
  - `subscribe(Collection<String> topics)`: Subscribes to a list of topics.
  - `subscribe(Collection<String> topics, ConsumerRebalanceListener listener)`: Subscribes with rebalance callback handlers.
  - `poll(Duration)`: Pulls data from brokers.
  - `commitSync()` / `commitAsync()`: Commits offsets back to the cluster.
  - `close()`: Closes the consumer, triggering rebalancing immediately to release partitions.

### 2. `ConsumerRecord<K, V>` (Class)

An individual message fetched from a topic. It wraps:

- `topic()`: The source topic.
- `partition()`: The partition this message was read from.
- `offset()`: The message offset.
- `key()` / `value()`: The deserialized key and value.
- `timestamp()`: The time the record was written by the producer.

### 3. `ConsumerRecords<K, V>` (Class)

A container returned by `poll()`. It holds a batch of `ConsumerRecord` instances.

- It implements `Iterable<ConsumerRecord<K, V>>`, allowing developers to iterate over all records using a simple `for` loop.

### 4. `TopicPartition` (Class)

Represents a specific topic partition (e.g., Topic "transactions", Partition 2). Used during manual commits and partition assignments.

### 5. `OffsetAndMetadata` (Class)

Represents the offset value to commit, along with optional metadata strings.

### 6. `Deserializer<T>` (Interface)

Defines how byte arrays (`byte[]`) are deserialized back into Java objects.

- Built-in deserializers: `StringDeserializer`, `IntegerDeserializer`.
- _Deserialization Example:_ Converts raw bytes back to JSON strings.

### 7. `ConsumerRebalanceListener` (Interface)

A callback interface called when partitions are revoked or assigned. Developers use this to flush state or commit offsets _before_ a partition is handed over to another consumer.

---

## 3. Core Configuration Properties

Before instantiating a `KafkaConsumer`, developers define a `Properties` map. The following configuration properties dictate behavior:

### Essential Configurations (Bootstrapping & Deserialization)

- **`bootstrap.servers`** (Type: String, Default: _None_):
  Brokers list used for discovering cluster topology.
- **`group.id`** (Type: String, Default: _None_):
  A unique string that identifies the consumer group this consumer belongs to.
- **`key.deserializer`** / **`value.deserializer`** (Type: Class, Default: _None_):
  Deserializer classes for keys and values.

### Offset Management Configurations

- **`auto.offset.reset`** (Type: String, Default: `latest`):
  What to do when there is no initial offset committed:
  - `earliest`: Read from the beginning of the partition.
  - `latest`: Read only new messages arriving after the consumer starts.
  - `none`: Throw an exception if no offset is found.
- **`enable.auto.commit`** (Type: Boolean, Default: `true`):
  Enables background automatic offset committing.
- **`auto.commit.interval.ms`** (Type: Integer, Default: `5000` / 5 sec):
  Interval for auto-commit.

### Rebalance & Timeout Configurations

- **`session.timeout.ms`** (Type: Integer, Default: `45000` / 45 sec):
  Time coordinator broker waits for heartbeats before triggering a rebalance.
- **`heartbeat.interval.ms`** (Type: Integer, Default: `3000` / 3 sec):
  How often the consumer sends keep-alive heartbeats to the coordinator.
- **`max.poll.interval.ms`** (Type: Integer, Default: `300000` / 5 min):
  Max time allowed between subsequent `poll()` calls. If exceeded, the coordinator assumes the consumer thread is stuck and triggers a rebalance.
- **`max.poll.records`** (Type: Integer, Default: `500`):
  Maximum records returned in a single `poll()` call.
- **`group.instance.id`** (Type: String, Default: `null`):
  Enables Static Membership. Avoids rebalances on quick restarts.

---

## 4. Consumer Architecture & Rebalancing Mechanics

### The Pull Model & Poll Loop

Unlike traditional message brokers (like RabbitMQ) that push events, Kafka uses a **pull model**. The consumer runs a continuous loop calling `poll()`.

- Calling `poll()` is not just a network fetch. It manages cluster coordination, handles partition assignment, triggers background heartbeats, and fetches batches of messages.

```text
                                 CONSUMER CLIENT POLL LOOP INTERNALS
┌────────────────────────────────────────────────────────────────────────────────────────┐
│  KafkaConsumer Client Instance (Single-threaded)                                       │
│                                                                                        │
│     Call poll(Duration)                                                                │
│          │                                                                             │
│          ▼                                                                             │
│     [Network Client / Coordinator Discovery] ──(Checks/Joins group, fetches metadata)  │
│          │                                                                             │
│          ▼                                                                             │
│     [Heartbeat Thread] ────────────────────────(Background sends keep-alive pings)     │
│          │                                                                             │
│          ▼                                                                             │
│     [Fetcher Component] ───────────────────────(Pulls data from socket buffer)         │
│          │                                                                             │
│          ▼                                                                             │
│     [Deserializer] ────────────────────────────(Converts byte arrays to Java objects)  │
│          │                                                                             │
│          ▼                                                                             │
│     [ConsumerRecords Map] ─────────────────────(Returns records to application loop)   │
└──────────┬─────────────────────────────────────────────────────────────────────────────┘
           │ (Loop processes records)
           ▼
  [Application Business Logic]
           │
           ▼ (Success?)
   [Commit Offsets] ──(Sync or Async commit to __consumer_offsets topic)
```

### The Consumer Group Cardinality Rule

- If a topic has $N$ partitions, a consumer group can scale up to $N$ members.
- If you have $N$ partitions and $N+1$ consumers, the $(N+1)^{\text{th}}$ consumer sits **idle**.
- Multiple partitions can be assigned to a single consumer, but a single partition can **never** be concurrently read by more than one consumer in the same group.

### Partition Assignment Strategies & Rebalancing

When a consumer joins or leaves a group, partition ownership shifts. This is called a **Rebalance**.

- **Eager Rebalancing (Standard):** All consumers stop reading, revoke their assignments, and wait to receive new assignments. This causes a "stop-the-world" pause in processing.
- **Incremental Cooperative Rebalancing (CooperativeSticky):** Only the partitions being reassigned are paused. The unaffected consumers continue reading without disruption.

---

## 5. Programming Patterns & Code Examples

### Pattern 1: Basic Consumer with Auto-Commit



```java
Properties props = new Properties();
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
props.put(ConsumerConfig.GROUP_ID_CONFIG, "fraud-group");
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

// Auto-commit enabled by default
try (KafkaConsumer<String, String> c = new KafkaConsumer<>(props)) {
    c.subscribe(List.of("transactions"));

    while (true) {
        ConsumerRecords<String, String> rs = c.poll(Duration.ofSeconds(1));
        for (ConsumerRecord<String, String> r : rs) {
            System.out.printf("Partition=%d Offset=%d Value=%s%n",
                    r.partition(), r.offset(), r.value());
        }
    }
}
```

### Pattern 2: Manual Synchronous Commits (At-Least-Once)



```java
// Disable auto-commit
props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

KafkaConsumer<String, String> c = new KafkaConsumer<>(props);
c.subscribe(List.of("transactions"));

while (true) {
    ConsumerRecords<String, String> rs = c.poll(Duration.ofSeconds(1));
    for (ConsumerRecord<String, String> r : rs) {
        System.out.printf("Processing Partition=%d Offset=%d%n", r.partition(), r.offset());

        // Execute business logic (e.g. Fraud check)

        // Commit synchronously after successful processing
        c.commitSync();
    }
}
```

### Pattern 3: Failure Simulation & Reprocessing



```java
while (true) {
    ConsumerRecords<String, String> rs = c.poll(Duration.ofSeconds(1));
    for (ConsumerRecord<String, String> r : rs) {
        System.out.printf("Value=%s%n", r.value());

        // Throw an exception on malformed/specific payload
        if (r.value().contains("60000")) {
            throw new RuntimeException("Simulated processing failure!");
        }

        // Commit is skipped if exception is thrown above.
        // On restart, the consumer starts again from the last committed offset.
        c.commitSync();
    }
}
```

---

## 6. Consumer Lag & Monitoring

**Consumer Lag** represents the number of records written by the producer that have not yet been read by the consumer.

$$\text{Lag} = \text{Broker Log End Offset} - \text{Current Consumer Offset}$$

### Operational Challenges of Lag

- **Causes:** Slow SQL operations in the poll loop, API timeouts, or a consumer group with too few members.
- **The Poison Pill:** A message that consistently crashes your consumer thread. When the thread restarts, it pulls the same message, crashes again, and lag grows indefinitely.
- **Resolution:** Monitor lag metrics (such as `records-lag-max`) using tools like **Burrow**, **AKHQ**, or Prometheus/Grafana.
