package co.vinod.kafka.serde;

import co.vinod.kafka.model.AmountSummary;
import co.vinod.kafka.model.CitySummary;
import co.vinod.kafka.model.FraudAlert;
import co.vinod.kafka.model.HighValuePayment;
import co.vinod.kafka.model.Payment;
import co.vinod.kafka.model.PaymentMethodSummary;
import co.vinod.kafka.model.PaymentSummary;

public final class SerdesFactory {

    private SerdesFactory() {
        // Utility class
    }

    public static JsonSerde<Payment> paymentSerde() {
        return new JsonSerde<>(Payment.class);
    }

    public static JsonSerde<PaymentSummary> paymentSummarySerde() {
        return new JsonSerde<>(PaymentSummary.class);
    }

    public static JsonSerde<AmountSummary> amountSummarySerde() {
        return new JsonSerde<>(AmountSummary.class);
    }

    public static JsonSerde<CitySummary> citySummarySerde() {
        return new JsonSerde<>(CitySummary.class);
    }

    public static JsonSerde<PaymentMethodSummary> paymentMethodSummarySerde() {
        return new JsonSerde<>(PaymentMethodSummary.class);
    }

    public static JsonSerde<HighValuePayment> highValuePaymentSerde() {
        return new JsonSerde<>(HighValuePayment.class);
    }

    public static JsonSerde<FraudAlert> fraudAlertSerde() {
        return new JsonSerde<>(FraudAlert.class);
    }
}