package com.kafkademo.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Continuously produces messages to multiple topics to populate Grafana dashboards.
 *
 * Simulates realistic traffic patterns:
 *   - orders        : moderate steady rate
 *   - payments      : slightly lower (not every order pays immediately)
 *   - user-events   : high rate (clicks, views, searches)
 *   - inventory     : low steady rate
 *   - notifications : burst then quiet
 *   - audit-log     : every significant event
 *   - dead-letter   : occasional failures
 *
 * Usage:
 *   java -cp kafka-metrics-demo-1.0-SNAPSHOT.jar com.kafkademo.producer.MultiTopicProducer
 *   java -Drate.multiplier=5 -cp ... com.kafkademo.producer.MultiTopicProducer   (5x faster)
 */
public class MultiTopicProducer {

    private static final Logger log = LoggerFactory.getLogger(MultiTopicProducer.class);

    private static final String BOOTSTRAP_SERVERS =
            System.getProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");

    // Global rate multiplier — increase to generate more lag faster
    private static final double RATE_MULTIPLIER =
            Double.parseDouble(System.getProperty("rate.multiplier", "1.0"));

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Random RANDOM = new Random();

    // Counters per topic for reporting
    private static final Map<String, AtomicLong> SENT = new ConcurrentHashMap<>();

    // ---------------------------------------------------------------
    // Message generators
    // ---------------------------------------------------------------

    private static Map<String, Object> order() {
        String[] statuses = {"PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED"};
        return Map.of(
                "orderId",    UUID.randomUUID().toString(),
                "customerId", "CUST-" + RANDOM.nextInt(10000),
                "amount",     Math.round(RANDOM.nextDouble() * 500 * 100.0) / 100.0,
                "currency",   "USD",
                "status",     statuses[RANDOM.nextInt(statuses.length)],
                "items",      RANDOM.nextInt(5) + 1,
                "timestamp",  Instant.now().toString()
        );
    }

    private static Map<String, Object> payment() {
        String[] methods = {"CARD", "UPI", "NETBANKING", "WALLET", "COD"};
        String[] statuses = {"INITIATED", "PROCESSING", "SUCCESS", "FAILED", "REFUNDED"};
        return Map.of(
                "paymentId",  UUID.randomUUID().toString(),
                "orderId",    UUID.randomUUID().toString(),
                "amount",     Math.round(RANDOM.nextDouble() * 500 * 100.0) / 100.0,
                "method",     methods[RANDOM.nextInt(methods.length)],
                "status",     statuses[RANDOM.nextInt(statuses.length)],
                "gateway",    RANDOM.nextBoolean() ? "STRIPE" : "RAZORPAY",
                "timestamp",  Instant.now().toString()
        );
    }

    private static Map<String, Object> userEvent() {
        String[] events = {"PAGE_VIEW", "CLICK", "SEARCH", "ADD_TO_CART", "REMOVE_FROM_CART",
                           "LOGIN", "LOGOUT", "PURCHASE", "WISHLIST_ADD", "REVIEW_SUBMIT"};
        return Map.of(
                "eventId",    UUID.randomUUID().toString(),
                "userId",     "USER-" + RANDOM.nextInt(50000),
                "event",      events[RANDOM.nextInt(events.length)],
                "page",       "/product/" + RANDOM.nextInt(1000),
                "sessionId",  "SESS-" + RANDOM.nextInt(100000),
                "deviceType", RANDOM.nextBoolean() ? "MOBILE" : "DESKTOP",
                "timestamp",  Instant.now().toString()
        );
    }

    private static Map<String, Object> inventoryUpdate() {
        String[] ops = {"STOCK_IN", "STOCK_OUT", "RESERVED", "RELEASED", "ADJUSTED"};
        return Map.of(
                "skuId",      "SKU-" + RANDOM.nextInt(5000),
                "warehouseId","WH-" + (RANDOM.nextInt(3) + 1),
                "operation",  ops[RANDOM.nextInt(ops.length)],
                "quantity",   RANDOM.nextInt(100) + 1,
                "stock",      RANDOM.nextInt(500),
                "timestamp",  Instant.now().toString()
        );
    }

    private static Map<String, Object> notification() {
        String[] channels = {"EMAIL", "SMS", "PUSH", "WHATSAPP"};
        String[] types    = {"ORDER_UPDATE", "PROMO", "SECURITY_ALERT", "PAYMENT_CONFIRM", "DELIVERY"};
        return Map.of(
                "notifId",    UUID.randomUUID().toString(),
                "userId",     "USER-" + RANDOM.nextInt(50000),
                "channel",    channels[RANDOM.nextInt(channels.length)],
                "type",       types[RANDOM.nextInt(types.length)],
                "delivered",  RANDOM.nextDouble() > 0.05,   // 95% delivery rate
                "timestamp",  Instant.now().toString()
        );
    }

    private static Map<String, Object> auditEntry() {
        String[] actors  = {"user", "service", "admin", "scheduler"};
        String[] actions = {"CREATE", "UPDATE", "DELETE", "READ", "LOGIN", "EXPORT"};
        String[] resources = {"Order", "Payment", "User", "Product", "Config"};
        return Map.of(
                "auditId",    UUID.randomUUID().toString(),
                "actor",      actors[RANDOM.nextInt(actors.length)] + "-" + RANDOM.nextInt(1000),
                "action",     actions[RANDOM.nextInt(actions.length)],
                "resource",   resources[RANDOM.nextInt(resources.length)],
                "resourceId", UUID.randomUUID().toString().substring(0, 8),
                "ip",         "10.0." + RANDOM.nextInt(256) + "." + RANDOM.nextInt(256),
                "timestamp",  Instant.now().toString()
        );
    }

    private static Map<String, Object> deadLetter() {
        String[] reasons = {"DESERIALIZATION_ERROR", "VALIDATION_FAILED",
                            "DOWNSTREAM_TIMEOUT", "SCHEMA_MISMATCH", "NULL_POINTER"};
        return Map.of(
                "originalTopic",  "orders",
                "errorReason",    reasons[RANDOM.nextInt(reasons.length)],
                "retryCount",     RANDOM.nextInt(3),
                "originalOffset", RANDOM.nextInt(100000),
                "timestamp",      Instant.now().toString()
        );
    }

    // ---------------------------------------------------------------
    // Producer task: send to one topic at a given rate
    // ---------------------------------------------------------------

    private record ProducerTask(
            String topic,
            int msDelay,             // base delay between sends
            java.util.function.Supplier<Map<String, Object>> payloadFn
    ) {}

    public static void main(String[] args) throws Exception {
        log.info("Starting MultiTopicProducer → {}", BOOTSTRAP_SERVERS);
        log.info("Rate multiplier: {}x", RATE_MULTIPLIER);

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,        BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,     StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,                     "all");         // wait for all ISR
        props.put(ProducerConfig.RETRIES_CONFIG,                  3);
        props.put(ProducerConfig.LINGER_MS_CONFIG,                5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,               16384);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,         "lz4");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,       true);

        // Define send rates (lower delay = higher throughput)
        List<ProducerTask> tasks = List.of(
                new ProducerTask("orders",        200,  MultiTopicProducer::order),
                new ProducerTask("payments",      300,  MultiTopicProducer::payment),
                new ProducerTask("user-events",   50,   MultiTopicProducer::userEvent),
                new ProducerTask("inventory",     500,  MultiTopicProducer::inventoryUpdate),
                new ProducerTask("notifications", 250,  MultiTopicProducer::notification),
                new ProducerTask("audit-log",     150,  MultiTopicProducer::auditEntry),
                new ProducerTask("dead-letter",   2000, MultiTopicProducer::deadLetter)
        );

        tasks.forEach(t -> SENT.put(t.topic(), new AtomicLong(0)));

        ExecutorService pool = Executors.newFixedThreadPool(tasks.size() + 1);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            // One thread per topic
            for (ProducerTask task : tasks) {
                pool.submit(() -> {
                    int delay = (int) (task.msDelay() / RATE_MULTIPLIER);
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            Map<String, Object> payload = task.payloadFn().get();
                            String key   = UUID.randomUUID().toString();
                            String value = MAPPER.writeValueAsString(payload);

                            producer.send(
                                    new ProducerRecord<>(task.topic(), key, value),
                                    (meta, ex) -> {
                                        if (ex != null) {
                                            log.error("Send failed to {}: {}", task.topic(), ex.getMessage());
                                        } else {
                                            SENT.get(task.topic()).incrementAndGet();
                                        }
                                    }
                            );
                            Thread.sleep(delay);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } catch (Exception e) {
                            log.error("Error in producer thread for {}: {}", task.topic(), e.getMessage());
                        }
                    }
                });
            }

            // Stats reporter thread
            pool.submit(() -> {
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        Thread.sleep(10_000);
                        log.info("=== Messages sent (last 10s) ===");
                        SENT.forEach((topic, count) ->
                                log.info("  {:20s} → {}", topic, count.getAndSet(0)));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            });

            log.info("✅ Producers running. Press Ctrl+C to stop.");
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down...");
                pool.shutdownNow();
                producer.flush();
            }));

            // Block main thread
            pool.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        }
    }
}
