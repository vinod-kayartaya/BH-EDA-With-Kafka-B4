package co.vinod.kafka.streams.topology;

import co.vinod.kafka.model.HighValuePayment;
import co.vinod.kafka.model.Payment;
import co.vinod.kafka.serde.SerdesFactory;
import co.vinod.kafka.util.TopicNames;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

public class HighValuePaymentTopology {

        public static void build(StreamsBuilder builder) {

                builder.stream(
                                TopicNames.PAYMENTS_RECEIVED,
                                Consumed.with(
                                                Serdes.String(),
                                                SerdesFactory.paymentSerde()))
                                .filter((key, payment) -> payment.getAmount() > 50000)
                                .mapValues(payment -> new HighValuePayment(
                                                payment.getPaymentId(),
                                                payment.getCustomerId(),
                                                payment.getAmount(),
                                                "HIGH_VALUE_PAYMENT"))
                                .to(
                                                TopicNames.HIGH_VALUE_PAYMENTS,
                                                Produced.with(
                                                                Serdes.String(),
                                                                SerdesFactory.highValuePaymentSerde()));
        }
}