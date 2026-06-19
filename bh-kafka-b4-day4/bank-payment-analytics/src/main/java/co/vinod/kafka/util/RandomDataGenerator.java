package co.vinod.kafka.util;

import java.util.Random;
import java.util.UUID;

public final class RandomDataGenerator {

    private static final Random RANDOM = new Random();

    private static final String[] CITIES = {
            "Bangalore",
            "Mumbai",
            "Delhi",
            "Hyderabad",
            "Chennai",
            "Pune",
            "Kolkata"
    };

    private static final String[] PAYMENT_METHODS = {
            "UPI",
            "CARD",
            "NET_BANKING",
            "WALLET"
    };

    private RandomDataGenerator() {
    }

    public static String randomPaymentId() {

        return "PAY-" +
                UUID.randomUUID()
                        .toString()
                        .substring(0, 8)
                        .toUpperCase();
    }

    public static String randomCustomerId() {

        return "C" +
                (100 + RANDOM.nextInt(20));
    }

    public static String randomMerchantId() {

        return "M" +
                (1000 + RANDOM.nextInt(50));
    }

    public static String randomCity() {

        return CITIES[RANDOM.nextInt(CITIES.length)];
    }

    public static String randomPaymentMethod() {

        return PAYMENT_METHODS[RANDOM.nextInt(
                PAYMENT_METHODS.length)];
    }

    public static double randomAmount() {

        double amount = 100 +
                RANDOM.nextDouble() * 100000;

        return Math.round(amount * 100.0) / 100.0;
    }

    public static int randomDelayMillis() {

        return 1000 +
                RANDOM.nextInt(4000);
    }

    public static boolean randomFraudTrigger() {

        return RANDOM.nextInt(10) == 0;
    }

    public static long currentTimestamp() {

        return System.currentTimeMillis();
    }
}