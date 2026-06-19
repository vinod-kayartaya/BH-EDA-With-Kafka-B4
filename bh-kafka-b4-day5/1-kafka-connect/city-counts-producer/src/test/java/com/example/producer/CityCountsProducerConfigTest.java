package com.example.producer;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class CityCountsProducerConfigTest {

    @Test
    void buildsRequiredProperties() {
        Properties props = CityCountsProducerConfig.build(
            "localhost:9092",
            "http://localhost:8081"
        );

        assertEquals("localhost:9092",          props.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals(StringSerializer.class.getName(), props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals("all",                     props.get(ProducerConfig.ACKS_CONFIG));
        assertNotNull(props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertNotNull(props.get("schema.registry.url"));
    }

    @Test
    void schemaRegistryUrlIsSet() {
        Properties props = CityCountsProducerConfig.build(
            "broker:9092",
            "http://schema-registry:8081"
        );

        assertEquals("http://schema-registry:8081", props.get("schema.registry.url"));
    }
}
