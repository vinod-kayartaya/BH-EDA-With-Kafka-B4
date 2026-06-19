package com.example.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class CityCountsProducerApp {

    private static final Logger log = LoggerFactory.getLogger(CityCountsProducerApp.class);

    public static void main(String[] args) {
        String bootstrapServers = config("BOOTSTRAP_SERVERS", "localhost:9092");
        String schemaRegistryUrl = config("SCHEMA_REGISTRY_URL", "http://localhost:8081");

        log.info("Starting city-counts producer");
        log.info("  bootstrap.servers  = {}", bootstrapServers);
        log.info("  schema.registry    = {}", schemaRegistryUrl);

        Properties props = CityCountsProducerConfig.build(bootstrapServers, schemaRegistryUrl);

        try (CityCountsProducer producer = new CityCountsProducer(props)) {

            // ── Sample records ────────────────────────────────────────────────
            // Replace or extend this section with your real data source
            // (e.g. read from a file, REST API, or another Kafka topic).
            producer.send("Bangalore", 16);
            producer.send("Mumbai", 42);
            producer.send("Delhi", 31);

            // Send the same city again to demonstrate upsert behaviour.
            producer.send("Bangalore", 25);

            // Flush ensures all records are delivered before we close.
            producer.flush();

            log.info("All records sent successfully.");
        }
    }

    /**
     * Reads a config value from a system property first, then the environment.
     * Falls back to {@code defaultValue} if neither is set.
     */
    private static String config(String key, String defaultValue) {
        String value = System.getProperty(key);
        if (value == null || value.isBlank()) {
            value = System.getenv(key);
        }
        return (value != null && !value.isBlank()) ? value : defaultValue;
    }
}
