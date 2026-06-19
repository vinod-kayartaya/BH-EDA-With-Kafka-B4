package co.vinod.kafka.producers;

import co.vinod.kafka.config.KafkaConfig;
import co.vinod.kafka.model.Transaction;
import co.vinod.kafka.util.JsonUtil;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class TransactionProducer_V4 {

        public static void main(String[] args)
                        throws Exception {

                Properties props = new Properties();

                props.put("bootstrap.servers",
                                KafkaConfig.get("bootstrap.servers"));

                props.put("acks",
                                KafkaConfig.get("acks"));

                props.put("key.serializer",
                                "org.apache.kafka.common.serialization.StringSerializer");

                props.put("value.serializer",
                                "org.apache.kafka.common.serialization.StringSerializer");

                try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

                        String topic = KafkaConfig.get("topic.name");

                        for (int i = 1; i <= 1000; i++) {

                                String customerId = "CUST-" + (i % 5);

                                Transaction tx = new Transaction(
                                                "TXN-" + i,
                                                customerId,
                                                i * 10000);

                                String json = JsonUtil.toJson(tx);

                                ProducerRecord<String, String> record = new ProducerRecord<>(
                                                topic,
                                                customerId,
                                                json);

                                RecordMetadata metadata = producer.send(record).get();

                                System.out.printf(
                                                "Customer=%s Partition=%d Offset=%d%n",
                                                customerId,
                                                metadata.partition(),
                                                metadata.offset());
                        }
                }
        }
}