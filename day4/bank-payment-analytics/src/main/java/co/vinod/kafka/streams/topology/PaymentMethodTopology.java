package co.vinod.kafka.streams.topology;

import co.vinod.kafka.model.Payment;
import co.vinod.kafka.model.PaymentMethodSummary;
import co.vinod.kafka.serde.SerdesFactory;
import co.vinod.kafka.util.TopicNames;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

public class PaymentMethodTopology {

        public static void build(StreamsBuilder builder) {

                builder.stream(
                                TopicNames.PAYMENTS_RECEIVED,
                                Consumed.with(
                                                Serdes.String(),
                                                SerdesFactory.paymentSerde()))
                                .groupBy(
                                                (key, payment) -> payment.getPaymentMethod(),
                                                Grouped.with(
                                                                Serdes.String(),
                                                                SerdesFactory.paymentSerde()))
                                .count()
                                .toStream()
                                .map((method, count) -> KeyValue.pair(
                                                method,
                                                new PaymentMethodSummary(
                                                                method,
                                                                count)))
                                .to(
                                                TopicNames.PAYMENT_METHOD_SUMMARY,
                                                Produced.with(
                                                                Serdes.String(),
                                                                SerdesFactory.paymentMethodSummarySerde()));
        }
}