package co.vinod.protobuf.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import co.vinod.protobuf.model.Customer;

import java.util.Properties;

public final class ProducerFactory {

    private ProducerFactory() {
    }

    public static Producer<String, Customer> createProducer() {

        Properties props = new Properties();

        props.put("bootstrap.servers",
                KafkaConstants.BOOTSTRAP_SERVERS);

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "co.vinod.protobuf.serializer.ProtobufSerializer");

        return new KafkaProducer<>(props);
    }
}