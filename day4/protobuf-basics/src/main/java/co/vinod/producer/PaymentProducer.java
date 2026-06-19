package co.vinod.producer;

import co.vinod.model.Payment;
import co.vinod.protobuf.model.Customer;
import co.vinod.protobuf.util.KafkaConstants;
import co.vinod.protobuf.util.ProducerFactory;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.Future;

public class PaymentProducer {

        public static void main(String[] args) throws Exception {

                System.out.println("\n====================================");
                System.out.println("CSV KAFKA PRODUCER");
                System.out.println("====================================\n");

                Properties props = new Properties();

                props.put("bootstrap.servers",
                                KafkaConstants.BOOTSTRAP_SERVERS);

                props.put("key.serializer",
                                "org.apache.kafka.common.serialization.StringSerializer");

                props.put("value.serializer",
                                "co.vinod.serializer.CsvSerializer");

                try (Producer<String, Payment> producer = new KafkaProducer<>(props);) {

                        Payment p = new Payment();
                        p.setId("PM5959");
                        p.setCustomerId("C4949");
                        p.setAmount(309000.);
                        p.setType("UPI");

                        ProducerRecord<String, Payment> record = new ProducerRecord<>(
                                        "payments",
                                        p.getCustomerId(),
                                        p);

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