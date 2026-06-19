package co.vinod.kafka.config;

import co.vinod.kafka.model.Payment;
import co.vinod.kafka.serde.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class KafkaConfig {

    public static KafkaProducer<String, Payment> createProducer() {

        Properties props = new Properties();

        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");

        props.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        props.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                JsonSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    public static Properties streamsConfig() {

        Properties props = new Properties();

        props.put(
                StreamsConfig.APPLICATION_ID_CONFIG,
                "payment-analytics-app");

        props.put(
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");

        props.put(
                StreamsConfig.COMMIT_INTERVAL_MS_CONFIG,
                5000);

        props.put(
                StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
                StreamsConfig.AT_LEAST_ONCE);

        props.put(
                StreamsConfig.STATE_DIR_CONFIG,
                "/tmp/kafka-streams");

        return props;
    }
}