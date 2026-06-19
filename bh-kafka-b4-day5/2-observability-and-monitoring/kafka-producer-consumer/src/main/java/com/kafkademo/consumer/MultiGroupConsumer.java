package com.kafkademo.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Runs multiple consumer groups simultaneously to populate the Consumer Lag dashboard.
 *
 * Consumer Groups created:
 *   - order-processing-service     → consumes orders, payments  (fast — keeps up)
 *   - analytics-service            → consumes all topics        (slow — intentional lag)
 *   - notification-service         → consumes orders, user-events, notifications
 *   - audit-service                → consumes audit-log         (fast)
 *   - reporting-service            → consumes all topics        (very slow — big lag)
 *
 * Usage:
 *   java -cp kafka-metrics-demo-1.0-SNAPSHOT.jar com.kafkademo.consumer.MultiGroupConsumer
 *
 * Run this AFTER MultiTopicProducer has been running for at least 30 seconds.
 */
public class MultiGroupConsumer {

    private static final Logger log = LoggerFactory.getLogger(MultiGroupConsumer.class);

    private static final String BOOTSTRAP_SERVERS =
            System.getProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");

    private static final AtomicLong TOTAL_CONSUMED = new AtomicLong(0);

    // ---------------------------------------------------------------
    // Consumer group definitions
    // ---------------------------------------------------------------

    private record ConsumerGroupDef(
            String groupId,
            List<String> topics,
            long processingDelayMs   // simulated per-record processing time
    ) {}

    private static final List<ConsumerGroupDef> GROUPS = List.of(

            // Fast consumer — stays caught up
            new ConsumerGroupDef(
                    "order-processing-service",
                    List.of("orders", "payments"),
                    10L
            ),

            // Moderate consumer — slight lag visible in dashboard
            new ConsumerGroupDef(
                    "notification-service",
                    List.of("orders", "user-events", "notifications"),
                    50L
            ),

            // Fast audit consumer
            new ConsumerGroupDef(
                    "audit-service",
                    List.of("audit-log", "payments", "orders"),
                    5L
            ),

            // Slow analytics consumer — intentional lag for dashboard demo
            new ConsumerGroupDef(
                    "analytics-service",
                    List.of("orders", "payments", "user-events", "inventory",
                            "notifications", "audit-log"),
                    200L     // slow processing → visible lag
            ),

            // Very slow reporting consumer — large lag, good for alert testing
            new ConsumerGroupDef(
                    "reporting-service",
                    List.of("orders", "payments", "user-events",
                            "inventory", "notifications", "audit-log"),
                    800L     // very slow → will trigger lag alerts
            )
    );

    // ---------------------------------------------------------------
    // Consumer thread
    // ---------------------------------------------------------------

    private static void runConsumer(ConsumerGroupDef def) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,        BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,                 def.groupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,        "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,       true);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,  "1000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,         50);
        // Increase max poll interval for slow consumers so they don't get kicked out
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG,     String.valueOf(
                Math.max(300_000L, def.processingDelayMs() * 50 * 10)));
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG,       "30000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG,    "10000");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(def.topics());
            log.info("[{}] Subscribed to: {}", def.groupId(), def.topics());

            while (!Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    // Simulate processing work
                    if (def.processingDelayMs() > 0) {
                        try {
                            Thread.sleep(def.processingDelayMs());
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            return;
                        }
                    }
                    TOTAL_CONSUMED.incrementAndGet();
                }

                if (!records.isEmpty()) {
                    log.debug("[{}] Processed {} records", def.groupId(), records.count());
                }
            }
        } catch (Exception e) {
            log.error("[{}] Consumer error: {}", def.groupId(), e.getMessage(), e);
        }
    }

    // ---------------------------------------------------------------
    // Main
    // ---------------------------------------------------------------

    public static void main(String[] args) throws InterruptedException {
        log.info("Starting MultiGroupConsumer → {}", BOOTSTRAP_SERVERS);
        log.info("Launching {} consumer groups", GROUPS.size());

        ExecutorService pool = Executors.newFixedThreadPool(GROUPS.size() + 1);

        for (ConsumerGroupDef def : GROUPS) {
            pool.submit(() -> runConsumer(def));
        }

        // Stats reporter
        pool.submit(() -> {
            long lastCount = 0;
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(10_000);
                    long current = TOTAL_CONSUMED.get();
                    log.info("Total records consumed across all groups (last 10s): {}",
                            current - lastCount);
                    lastCount = current;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        });

        log.info("✅ Consumer groups running. Press Ctrl+C to stop.");
        log.info("Consumer groups active:");
        GROUPS.forEach(g -> log.info("  [{:35s}] topics={}, delay={}ms",
                g.groupId(), g.topics(), g.processingDelayMs()));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutting down consumers...");
            pool.shutdownNow();
        }));

        pool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
    }
}
