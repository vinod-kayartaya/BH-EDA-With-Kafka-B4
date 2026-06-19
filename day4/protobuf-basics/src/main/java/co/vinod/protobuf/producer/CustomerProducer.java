package co.vinod.protobuf.producer;

import co.vinod.protobuf.model.Customer;
import co.vinod.protobuf.util.KafkaConstants;
import co.vinod.protobuf.util.ProducerFactory;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;

public class CustomerProducer {

    public static void main(String[] args) throws Exception {

        System.out.println("\n====================================");
        System.out.println("PROTOBUF KAFKA PRODUCER");
        System.out.println("====================================\n");

        try (Producer<String, Customer> producer = ProducerFactory.createProducer()) {

            Customer customer = Customer.newBuilder()
                    .setCustomerId("C100")
                    .setName("John Doe")
                    .setEmail("john.doe@gmail.com")
                    .setCreditLimit(100000)
                    .build();

            ProducerRecord<String, Customer> record = new ProducerRecord<>(
                    KafkaConstants.TOPIC_NAME,
                    customer.getCustomerId(),
                    customer);

            Future<RecordMetadata> future = producer.send(record);

            RecordMetadata metadata = future.get();

            System.out.printf(
                    "Message sent successfully%n" +
                            "Topic     : %s%n" +
                            "Partition : %d%n" +
                            "Offset    : %d%n",
                    metadata.topic(),
                    metadata.partition(),
                    metadata.offset());
        }

        System.out.println("\nProducer completed.");
    }
}