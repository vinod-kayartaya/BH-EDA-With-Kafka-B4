package co.vinod.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class PaymentProducer {

        private static final String[] PAYMENT_TYPES = {
                        "UPI",
                        "CARD",
                        "NETBANKING",
                        "WALLET"
        };

        private static final Random random = new Random();

        public static void main(String[] args) throws Exception {

                Properties props = new Properties();

                props.put(
                                "bootstrap.servers",
                                "localhost:9092");

                props.put(
                                "key.serializer",
                                "org.apache.kafka.common.serialization.StringSerializer");

                props.put(
                                "value.serializer",
                                "org.apache.kafka.common.serialization.StringSerializer");

                KafkaProducer<String, String> producer = new KafkaProducer<>(props);

                ObjectMapper mapper = new ObjectMapper();

                long txId = 1;

                while (true) {

                        Map<String, Object> payment = new LinkedHashMap<>();

                        payment.put("tx_id", txId++);
                        payment.put(
                                        "paymentType",
                                        PAYMENT_TYPES[random.nextInt(PAYMENT_TYPES.length)]);

                        payment.put(
                                        "amount",
                                        100 + random.nextInt(9000));

                        payment.put(
                                        "cust_id",
                                        "CA" + (100 + random.nextInt(900)));

                        payment.put(
                                        "remarks",
                                        UUID.randomUUID().toString().substring(0, 8));

                        String json = mapper.writeValueAsString(payment);

                        producer.send(
                                        new ProducerRecord<>(
                                                        "payments",
                                                        String.valueOf(payment.get("cust_id")),
                                                        json));

                        System.out.println(
                                        "Produced: " + json);

                        int delay = 1000;
                        // int delay = 1000 + random.nextInt(3000);

                        Thread.sleep(delay);
                }
        }
}