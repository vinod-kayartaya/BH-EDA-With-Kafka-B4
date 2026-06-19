package co.vinod.protobuf.util;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import co.vinod.protobuf.model.Customer;

import java.util.Properties;

public final class ConsumerFactory {

    private ConsumerFactory() {
    }

    public static Consumer<String, Customer> createConsumer() {

        Properties props = new Properties();

        props.put("bootstrap.servers",
                KafkaConstants.BOOTSTRAP_SERVERS);

        props.put("group.id",
                KafkaConstants.CONSUMER_GROUP);

        props.put("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("value.deserializer",
                "co.vinod.protobuf.serializer.ProtobufDeserializer");

        props.put("auto.offset.reset",
                "earliest");

        return new KafkaConsumer<>(props);
    }
}