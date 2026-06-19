package co.vinod.kafka.producer;

import co.vinod.kafka.config.KafkaConfig;
import co.vinod.kafka.model.Payment;
import co.vinod.kafka.util.RandomDataGenerator;
import co.vinod.kafka.util.TopicNames;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Random;
import java.util.UUID;

public class PaymentProducer {

        public static void main(String[] args)
                        throws Exception {

                System.out.println();
                System.out.println(
                                "====================================");
                System.out.println(
                                "BANK PAYMENT PRODUCER");
                System.out.println(
                                "====================================");

                KafkaProducer<String, Payment> producer = KafkaConfig.createProducer();

                Random random = new Random();

                while (true) {

                        Payment payment;
                        if (random.nextInt(10) == 0) {
                                payment = PaymentGenerator.generateFraudLikePayment();
                        } else {
                                payment = PaymentGenerator.generate();
                        }

                        producer.send(
                                        new ProducerRecord<>(
                                                        TopicNames.PAYMENTS_RECEIVED,
                                                        payment.getCustomerId(),
                                                        payment));

                        Thread.sleep(
                                        RandomDataGenerator.randomDelayMillis());
                }
        }

        public static Payment generateFraudLikePayment() {

                String customerId = "C999";

                return new Payment(
                                "PAY-" + UUID.randomUUID()
                                                .toString()
                                                .substring(0, 8),
                                customerId,
                                "M1001",
                                "Bangalore",
                                "UPI",
                                90000.00,
                                System.currentTimeMillis());
        }
}