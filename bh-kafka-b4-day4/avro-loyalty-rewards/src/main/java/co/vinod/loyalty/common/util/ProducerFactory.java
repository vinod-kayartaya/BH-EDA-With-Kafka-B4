package co.vinod.loyalty.common.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

public class ProducerFactory {

    private ProducerFactory() {
    }

    public static <K, V> Producer<K, V> create(
            Properties props) {

        return new KafkaProducer<>(props);
    }
}