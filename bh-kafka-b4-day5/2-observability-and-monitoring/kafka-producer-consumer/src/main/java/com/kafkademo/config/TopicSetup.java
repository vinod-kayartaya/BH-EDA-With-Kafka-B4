package com.kafkademo.config;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.TopicConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Creates all demo topics needed to populate the Grafana dashboards.
 * Run this once before starting producers/consumers.
 */
public class TopicSetup {

    private static final Logger log = LoggerFactory.getLogger(TopicSetup.class);

    // Bootstrap servers — matches the EXTERNAL listeners in your docker-compose
    private static final String BOOTSTRAP_SERVERS =
            System.getProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");

    // Topics to create: name → [partitions, replicationFactor]
    private static final Map<String, int[]> TOPICS = new LinkedHashMap<>();

    static {
        TOPICS.put("orders",          new int[]{6, 3});
        TOPICS.put("payments",        new int[]{6, 3});
        TOPICS.put("user-events",     new int[]{4, 3});
        TOPICS.put("inventory",       new int[]{3, 3});
        TOPICS.put("notifications",   new int[]{3, 3});
        TOPICS.put("audit-log",       new int[]{3, 3});
        TOPICS.put("dead-letter",     new int[]{2, 3});
    }

    public static void main(String[] args) throws Exception {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        adminProps.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "10000");

        try (AdminClient admin = AdminClient.create(adminProps)) {
            // Fetch existing topics
            Set<String> existing = admin.listTopics().names().get();
            log.info("Existing topics: {}", existing);

            List<NewTopic> toCreate = new ArrayList<>();
            for (Map.Entry<String, int[]> entry : TOPICS.entrySet()) {
                String name   = entry.getKey();
                int parts     = entry.getValue()[0];
                short replicas = (short) entry.getValue()[1];

                if (existing.contains(name)) {
                    log.info("Topic already exists, skipping: {}", name);
                    continue;
                }

                NewTopic topic = new NewTopic(name, parts, replicas);
                // 24h retention so log metrics stay interesting
                topic.configs(Map.of(
                        TopicConfig.RETENTION_MS_CONFIG, String.valueOf(24 * 60 * 60 * 1000L),
                        TopicConfig.SEGMENT_MS_CONFIG,   String.valueOf(60 * 60 * 1000L)
                ));
                toCreate.add(topic);
            }

            if (toCreate.isEmpty()) {
                log.info("All topics already exist. Nothing to create.");
                return;
            }

            CreateTopicsResult result = admin.createTopics(toCreate);
            result.all().get();

            log.info("✅ Created {} topic(s):", toCreate.size());
            toCreate.forEach(t -> log.info("   → {} ({} partitions, RF={})",
                    t.name(), t.numPartitions(), t.replicationFactor()));
        }
    }
}
