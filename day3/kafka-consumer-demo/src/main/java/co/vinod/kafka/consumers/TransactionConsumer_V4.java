package co.vinod.kafka.consumers;

import co.vinod.kafka.config.KafkaConfig;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.*;

public class TransactionConsumer_V4 {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.get("bootstrap.servers"));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConfig.get("group.id"));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, KafkaConfig.get("auto.offset.reset"));

        KafkaConsumer<String, String> c = new KafkaConsumer<>(props);
        c.subscribe(List.of(KafkaConfig.get("topic.name")));
        System.out.println("Consumer V4 started...");
        while (true) {
            ConsumerRecords<String, String> rs = c.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> r : rs) {
                System.out.printf("Partition=%d Offset=%d Value=%s%n", r.partition(), r.offset(), r.value());

            }
        }
    }
}