package co.vinod.kafka.admin;

import co.vinod.kafka.config.KafkaConfig;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class TopicManager {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.get("bootstrap.servers"));

        String topicName = KafkaConfig.get("topic.name");

        try (AdminClient admin = AdminClient.create(props)) {
            System.out.println("--- 1. Listing Existing Topics ---");
            listTopics(admin);

            System.out.println("\n--- 2. Creating New Topic: " + topicName + " ---");
            createTopic(admin, topicName, 3, (short) 1);

            System.out.println("\n--- 3. Listing Topics After Creation ---");
            listTopics(admin);

            System.out.println("\n--- 4. Describing Topic: " + topicName + " ---");
            describeTopic(admin, topicName);

            System.out.println("\n--- 5. Altering Topic Configuration (retention.ms) ---");
            alterTopicRetention(admin, topicName, "86400000"); // 1 day

            System.out.println("\n--- 6. Describing Altered Topic Configuration ---");
            describeTopicConfig(admin, topicName);

            System.out.println("\n--- 7. Increasing Partitions to 6 ---");
            increasePartitions(admin, topicName, 6);

            System.out.println("\n--- 8. Describing Topic After Partition Increase ---");
            describeTopic(admin, topicName);

            System.out.println("\n--- 9. Deleting Topic: " + topicName + " ---");
            deleteTopic(admin, topicName);

            System.out.println("\n--- 10. Listing Topics After Deletion ---");
            listTopics(admin);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void listTopics(AdminClient admin) throws Exception {
        ListTopicsResult result = admin.listTopics();
        Set<String> names = result.names().get();
        if (names.isEmpty()) {
            System.out.println("No topics found in the cluster.");
        } else {
            System.out.println("Topics: " + names);
        }
    }

    private static void createTopic(AdminClient admin, String topicName, int numPartitions, short replicationFactor) throws Exception {
        NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
        
        // Optional: set initial topic configs
        Map<String, String> configs = new HashMap<>();
        configs.put("cleanup.policy", "delete");
        newTopic.configs(configs);

        CreateTopicsResult result = admin.createTopics(Collections.singletonList(newTopic));
        
        try {
            result.all().get();
            System.out.println("Topic '" + topicName + "' created successfully.");
        } catch (ExecutionException e) {
            System.out.println("Failed to create topic: " + e.getCause().getMessage());
        }
    }

    private static void describeTopic(AdminClient admin, String topicName) throws Exception {
        DescribeTopicsResult result = admin.describeTopics(Collections.singletonList(topicName));
        try {
            Map<String, TopicDescription> descriptionMap = result.allTopicNames().get();
            TopicDescription description = descriptionMap.get(topicName);
            System.out.println("Topic Name: " + description.name());
            System.out.println("Is Internal: " + description.isInternal());
            System.out.println("Partitions Detail:");
            description.partitions().forEach(partition -> {
                System.out.printf("  Partition: %d, Leader: %s, Replicas: %s, ISRs: %s%n",
                        partition.partition(),
                        partition.leader(),
                        partition.replicas(),
                        partition.isr());
            });
        } catch (ExecutionException e) {
            System.out.println("Failed to describe topic: " + e.getCause().getMessage());
        }
    }

    private static void alterTopicRetention(AdminClient admin, String topicName, String retentionMs) throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        ConfigEntry retentionEntry = new ConfigEntry("retention.ms", retentionMs);
        
        // Prepare incremental alter operation
        AlterConfigOp op = new AlterConfigOp(retentionEntry, AlterConfigOp.OpType.SET);
        Map<ConfigResource, Collection<AlterConfigOp>> configs = new HashMap<>();
        configs.put(resource, Collections.singletonList(op));

        admin.incrementalAlterConfigs(configs).all().get();
        System.out.println("Topic retention.ms config altered successfully to " + retentionMs + " ms.");
    }

    private static void describeTopicConfig(AdminClient admin, String topicName) throws Exception {
        ConfigResource resource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        DescribeConfigsResult result = admin.describeConfigs(Collections.singletonList(resource));
        
        Map<ConfigResource, org.apache.kafka.clients.admin.Config> configMap = result.all().get();
        org.apache.kafka.clients.admin.Config config = configMap.get(resource);
        
        ConfigEntry retentionEntry = config.get("retention.ms");
        ConfigEntry cleanupPolicy = config.get("cleanup.policy");
        
        System.out.printf("  retention.ms  : %s (Source: %s)%n", retentionEntry.value(), retentionEntry.source());
        System.out.printf("  cleanup.policy: %s (Source: %s)%n", cleanupPolicy.value(), cleanupPolicy.source());
    }

    private static void increasePartitions(AdminClient admin, String topicName, int totalPartitions) throws Exception {
        Map<String, NewPartitions> partitionsMap = new HashMap<>();
        partitionsMap.put(topicName, NewPartitions.increaseTo(totalPartitions));

        try {
            admin.createPartitions(partitionsMap).all().get();
            System.out.println("Partitions increased to " + totalPartitions + " successfully.");
        } catch (ExecutionException e) {
            System.out.println("Failed to increase partitions: " + e.getCause().getMessage());
        }
    }

    private static void deleteTopic(AdminClient admin, String topicName) throws Exception {
        DeleteTopicsResult result = admin.deleteTopics(Collections.singletonList(topicName));
        try {
            result.all().get();
            System.out.println("Topic '" + topicName + "' deleted successfully.");
        } catch (ExecutionException e) {
            System.out.println("Failed to delete topic: " + e.getCause().getMessage());
        }
    }
}
