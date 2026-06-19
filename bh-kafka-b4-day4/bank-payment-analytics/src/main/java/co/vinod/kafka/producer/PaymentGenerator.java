package co.vinod.kafka.producer;

import co.vinod.kafka.model.Payment;
import co.vinod.kafka.util.RandomDataGenerator;

public class PaymentGenerator {

        public static Payment generate() {

                return new Payment(
                                RandomDataGenerator.randomPaymentId(),
                                RandomDataGenerator.randomCustomerId(),
                                RandomDataGenerator.randomMerchantId(),
                                RandomDataGenerator.randomCity(),
                                RandomDataGenerator.randomPaymentMethod(),
                                RandomDataGenerator.randomAmount(),
                                RandomDataGenerator.currentTimestamp());
        }

        public static Payment generateFraudLikePayment() {

                return new Payment(
                                RandomDataGenerator.randomPaymentId(),
                                "C999",
                                "M1001",
                                "Bangalore",
                                "UPI",
                                90000.00,
                                System.currentTimeMillis());
        }
}