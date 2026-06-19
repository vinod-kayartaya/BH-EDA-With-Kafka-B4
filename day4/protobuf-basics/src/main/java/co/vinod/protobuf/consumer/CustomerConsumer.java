package co.vinod.protobuf.consumer;

import co.vinod.protobuf.model.Customer;
import co.vinod.protobuf.util.ConsumerFactory;
import co.vinod.protobuf.util.KafkaConstants;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Collections;

public class CustomerConsumer {

    public static void main(String[] args) {

        System.out.println("\n====================================");
        System.out.println("PROTOBUF KAFKA CONSUMER");
        System.out.println("====================================\n");

        try (Consumer<String, Customer> consumer = ConsumerFactory.createConsumer()) {

            consumer.subscribe(
                    Collections.singletonList(
                            KafkaConstants.TOPIC_NAME));

            System.out.println(
                    "Listening on topic: "
                            + KafkaConstants.TOPIC_NAME);

            while (true) {

                ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, Customer> record : records) {

                    Customer customer = record.value();

                    System.out.println(
                            "\n------------------------------------");

                    System.out.println(
                            "Partition    : "
                                    + record.partition());

                    System.out.println(
                            "Offset       : "
                                    + record.offset());

                    System.out.println(
                            "Customer ID  : "
                                    + customer.getCustomerId());

                    System.out.println(
                            "Name         : "
                                    + customer.getName());

                    System.out.println(
                            "Email        : "
                                    + customer.getEmail());

                    System.out.println(
                            "Credit Limit : "
                                    + customer.getCreditLimit());

                    System.out.println(
                            "------------------------------------");
                }
            }
        }
    }
}