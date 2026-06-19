package co.vinod.kafka.util;

public final class TopicNames {

    private TopicNames() {
        // Prevent instantiation
    }

    /**
     * Source Topic
     */
    public static final String PAYMENTS_RECEIVED = "payments-received";

    /**
     * Analytics Topics
     */
    public static final String PAYMENTS_PER_MINUTE = "payments-per-minute";

    public static final String PAYMENT_AMOUNT_SUMMARY = "payment-amount-summary";

    public static final String CITY_PAYMENT_SUMMARY = "city-payment-summary";

    public static final String PAYMENT_METHOD_SUMMARY = "payment-method-summary";

    /**
     * Alert Topics
     */
    public static final String HIGH_VALUE_PAYMENTS = "high-value-payments";

    public static final String FRAUD_ALERTS = "fraud-alerts";
}