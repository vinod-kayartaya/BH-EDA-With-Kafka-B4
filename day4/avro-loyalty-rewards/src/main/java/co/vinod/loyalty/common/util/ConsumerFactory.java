package co.vinod.loyalty.common.util;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class ConsumerFactory {

    private ConsumerFactory() {
    }

    public static <K, V> Consumer<K, V> create(
            Properties props) {

        return new KafkaConsumer<>(props);
    }
}