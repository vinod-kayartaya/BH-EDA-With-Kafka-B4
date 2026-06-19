package co.vinod.kafka.producers;

import co.vinod.kafka.config.KafkaConfig;
import co.vinod.kafka.model.Transaction;
import co.vinod.kafka.util.JsonUtil;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class TransactionProducer_V6 {

    public static void main(String[] args)
            throws Exception {

        Properties props = new Properties();

        props.put("bootstrap.servers",
                KafkaConfig.get("bootstrap.servers"));

        props.put("enable.idempotence",
                KafkaConfig.get("enable.idempotence"));

        props.put("retries",
                KafkaConfig.get("retries"));

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

            for (int i = 1; i <= 100; i++) {

                Transaction tx =
                        new Transaction(
                                "TXN-" + i,
                                "CUST-" + (i % 10),
                                i * 1000);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(
                                topic,
                                tx.getCustomerId(),
                                JsonUtil.toJson(tx));

                RecordMetadata metadata =
                        producer.send(record).get();

                System.out.printf(
                        "Published=%s Partition=%d Offset=%d%n",
                        tx.getTransactionId(),
                        metadata.partition(),
                        metadata.offset());
            }
        }
    }
}