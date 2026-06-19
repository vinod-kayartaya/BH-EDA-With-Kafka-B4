package co.vinod.loyalty.v2;

import co.vinod.loyalty.avro.v2.CustomerPurchase;
import co.vinod.loyalty.common.config.PropertyLoader;
import co.vinod.loyalty.common.util.BannerPrinter;
import co.vinod.loyalty.common.util.ProducerFactory;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;

public class PurchaseProducer {

        public static void main(String[] args) {

                BannerPrinter.print("AVRO KAFKA PRODUCER");

                Properties fileProps = PropertyLoader.load("v2/application.properties");

                Properties props = new Properties();

                props.put(
                                "bootstrap.servers",
                                fileProps.getProperty("bootstrap.servers"));

                props.put(
                                "key.serializer",
                                fileProps.getProperty("producer.key.serializer"));

                props.put(
                                "value.serializer",
                                fileProps.getProperty("producer.value.serializer"));

                props.put(
                                "schema.registry.url",
                                fileProps.getProperty("schema.registry.url"));

                Producer<String, CustomerPurchase> producer = ProducerFactory.create(props);

                String topic = fileProps.getProperty("topic.name");

                try {

                        CustomerPurchase purchase = CustomerPurchase.newBuilder()
                                        .setCustomerId("C1001")
                                        .setPurchaseAmount(1500.00)
                                        .setPointsEarned(150)
                                        .build();

                        System.out.println("Created Purchase Event");
                        System.out.println(purchase);
                        System.out.println();

                        ProducerRecord<String, CustomerPurchase> record = new ProducerRecord<>(
                                        topic,
                                        purchase.getCustomerId().toString(),
                                        purchase);

                        producer.send(record, (metadata, exception) -> {

                                if (exception == null) {

                                        System.out.println(
                                                        "Message Sent Successfully");

                                        System.out.println(
                                                        "Topic     : "
                                                                        + metadata.topic());

                                        System.out.println(
                                                        "Partition : "
                                                                        + metadata.partition());

                                        System.out.println(
                                                        "Offset    : "
                                                                        + metadata.offset());

                                } else {

                                        exception.printStackTrace();
                                }
                        });

                        producer.flush();

                } catch (Exception e) {

                        e.printStackTrace();

                } finally {

                        producer.close();
                }
        }
}