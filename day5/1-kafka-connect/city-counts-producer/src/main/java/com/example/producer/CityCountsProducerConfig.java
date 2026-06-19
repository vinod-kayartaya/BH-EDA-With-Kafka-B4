package com.example.producer;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Builds the {@link Properties} object used to construct a {@link org.apache.kafka.clients.producer.KafkaProducer}.
 *
 * <p>Centralising config here makes it easy to swap values (e.g. for tests)
 * without touching the producer or application entry-point.</p>
 */
public final class CityCountsProducerConfig {

    private CityCountsProducerConfig() {}

    /**
     * Returns producer properties wired to the given bootstrap servers and Schema Registry URL.
     *
     * @param bootstrapServers  comma-separated list, e.g. {@code localhost:9092,localhost:9093}
     * @param schemaRegistryUrl e.g. {@code http://localhost:8081}
     */
    public static Properties build(String bootstrapServers, String schemaRegistryUrl) {
        Properties props = new Properties();

        // ── Connection ────────────────────────────────────────────────────────
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        // ── Serialisers ───────────────────────────────────────────────────────
        // Key is a plain string (city name or any correlation key).
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Value is an Avro-encoded GenericRecord / SpecificRecord.
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        // ── Schema Registry ───────────────────────────────────────────────────
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
        // Register the schema automatically when first producing (safe for dev).
        props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, true);

        // ── Reliability ───────────────────────────────────────────────────────
        // Wait for all in-sync replicas to acknowledge before returning.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Retry up to 3 times on transient errors.
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        // Only one in-flight request per connection — preserves ordering on retry.
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        // ── Throughput tuning (sensible defaults for low-volume use) ──────────
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16_384);

        return props;
    }
}
