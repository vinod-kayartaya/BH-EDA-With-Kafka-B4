package co.vinod.kafka.producers;

import co.vinod.kafka.config.KafkaConfig;
import co.vinod.kafka.model.Transaction;
import co.vinod.kafka.util.JsonUtil;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class TransactionProducer_V1 {

        public static void main(String[] args) {

                Properties props = new Properties();

                props.put(
                                "bootstrap.servers",
                                KafkaConfig.get("bootstrap.servers"));

                props.put(
                                "client.id",
                                KafkaConfig.get("client.id"));

                props.put(
                                "key.serializer",
                                "org.apache.kafka.common.serialization.StringSerializer");

                props.put(
                                "value.serializer",
                                "org.apache.kafka.common.serialization.StringSerializer");

                props.put("linger.ms", "5000");

                KafkaProducer<String, String> producer = new KafkaProducer<>(props);

                String topic = KafkaConfig.get("topic.name");

                for (int i = 1; i <= 10; i++) {

                        Transaction tx = new Transaction(
                                        "TXN-" + i,
                                        "CUST-" + i,
                                        i * 10000);

                        String json = JsonUtil.toJson(tx);

                        ProducerRecord<String, String> record = new ProducerRecord<>(
                                        topic,
                                        json);

                        producer.send(record);

                        System.out.println("Published => " + json);

                }

                // System.out.println("Closing the producer...");

                // producer.flush();
                producer.close();

                // try {
                //         Thread.sleep(7000);
                // } catch (InterruptedException e) {
                //         e.printStackTrace();
                // }
                System.out.println("Producer closed. Exiting...");
        }
}