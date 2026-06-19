package co.vinod.kafka.producers;

import co.vinod.kafka.config.KafkaConfig;
import co.vinod.kafka.model.Transaction;
import co.vinod.kafka.util.JsonUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class TransactionProducer_V5 {

    public static void main(String[] args) {

        Properties props = new Properties();

        props.put(
                "bootstrap.servers",
                KafkaConfig.get("bootstrap.servers"));

        props.put(
                "acks",
                KafkaConfig.get("acks"));

        props.put(
                "linger.ms",
                KafkaConfig.get("linger.ms"));

        props.put(
                "batch.size",
                KafkaConfig.get("batch.size"));

        props.put(
                "compression.type",
                KafkaConfig.get("compression.type"));

        props.put(
                "key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        props.put(
                "value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        int messageCount =
                Integer.parseInt(
                        KafkaConfig.get("message.count"));

        long startTime =
                System.currentTimeMillis();

        try (KafkaProducer<String, String> producer =
                     new KafkaProducer<>(props)) {

            String topic =
                    KafkaConfig.get("topic.name");

            for (int i = 1; i <= messageCount; i++) {

                Transaction tx =
                        new Transaction(
                                "TXN-" + i,
                                "CUST-" + (i % 50),
                                i * 100);

                ProducerRecord<String, String> record =
                        new ProducerRecord<>(
                                topic,
                                tx.getCustomerId(),
                                JsonUtil.toJson(tx));

                producer.send(record);
            }

            // Force all buffered records to be sent
            producer.flush();
        }

        long endTime =
                System.currentTimeMillis();

        long elapsedTime =
                endTime - startTime;

        System.out.println();
        System.out.println("================================");
        System.out.println("Producer Batching Demo");
        System.out.println("================================");
        System.out.printf(
                "Messages Sent : %d%n",
                messageCount);

        System.out.printf(
                "linger.ms     : %s%n",
                KafkaConfig.get("linger.ms"));

        System.out.printf(
                "batch.size    : %s%n",
                KafkaConfig.get("batch.size"));

        System.out.printf(
                "compression   : %s%n",
                KafkaConfig.get("compression.type"));

        System.out.println();

        System.out.printf(
                "Elapsed Time  : %d ms%n",
                elapsedTime);

        System.out.println("================================");
    }
}