package co.vinod.kafka.producers;

import co.vinod.kafka.config.KafkaConfig;
import co.vinod.kafka.model.Transaction;
import co.vinod.kafka.util.JsonUtil;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class TransactionProducer_V2 {

    public static void main(String[] args)
            throws Exception {

        Properties props = new Properties();

        props.put("bootstrap.servers",
                KafkaConfig.get("bootstrap.servers"));

        props.put("client.id",
                KafkaConfig.get("client.id"));

        props.put("acks",
                KafkaConfig.get("acks"));

        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(props)) {

            String topic =
                    KafkaConfig.get("topic.name");

            int count =
                    Integer.parseInt(
                            KafkaConfig.get("message.count"));

            for (int i = 1; i <= count; i++) {

                Transaction tx =
                        new Transaction(
                                "TXN-" + i,
                                "CUST-" + i,
                                i * 10000);

                String json =
                        JsonUtil.toJson(tx);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(
                                topic,
                                json);

                RecordMetadata metadata =
                        producer.send(record).get();

                System.out.printf(
                        "Published=%s Partition=%d Offset=%d%n",
                        tx.getTransactionId(),
                        metadata.partition(),
                        metadata.offset());
            }


        }// producer.close() is called automatically here
    }
}