package co.vinod.loyalty.v2;

import co.vinod.loyalty.avro.v2.CustomerPurchase;
import co.vinod.loyalty.common.config.PropertyLoader;
import co.vinod.loyalty.common.util.BannerPrinter;
import co.vinod.loyalty.common.util.ConsumerFactory;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class PurchaseConsumer {

        public static void main(String[] args) {

                BannerPrinter.print("AVRO KAFKA CONSUMER");

                Properties fileProps = PropertyLoader.load("v2/application.properties");

                Properties props = new Properties();

                props.put(
                                "bootstrap.servers",
                                fileProps.getProperty("bootstrap.servers"));

                props.put(
                                "group.id",
                                fileProps.getProperty("consumer.group.id"));

                props.put(
                                "key.deserializer",
                                fileProps.getProperty(
                                                "consumer.key.deserializer"));

                props.put(
                                "value.deserializer",
                                fileProps.getProperty(
                                                "consumer.value.deserializer"));

                props.put(
                                "auto.offset.reset",
                                fileProps.getProperty(
                                                "consumer.auto.offset.reset"));

                props.put(
                                "schema.registry.url",
                                fileProps.getProperty(
                                                "schema.registry.url"));

                props.put(
                                KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG,
                                true);

                Consumer<String, CustomerPurchase> consumer = ConsumerFactory.create(props);

                String topic = fileProps.getProperty("topic.name");

                consumer.subscribe(
                                Collections.singletonList(topic));

                System.out.println(
                                "Subscribed to topic : " + topic);

                try {

                        while (true) {

                                ConsumerRecords<String, CustomerPurchase> records = consumer.poll(
                                                Duration.ofSeconds(1));

                                for (ConsumerRecord<String, CustomerPurchase> record : records) {

                                        CustomerPurchase purchase = record.value();

                                        System.out.println();
                                        System.out.println(
                                                        "Received Purchase Event");

                                        System.out.println(
                                                        "Customer ID     : "
                                                                        + purchase.getCustomerId());

                                        System.out.println(
                                                        "Purchase Amount : "
                                                                        + purchase.getPurchaseAmount());

                                        System.out.println(
                                                        "Points Earned   : "
                                                                        + purchase.getPointsEarned());

                                        System.out.println(
                                                        "Partition       : "
                                                                        + record.partition());

                                        System.out.println(
                                                        "Offset          : "
                                                                        + record.offset());
                                }
                        }

                } catch (Exception e) {

                        e.printStackTrace();

                } finally {

                        consumer.close();
                }
        }
}