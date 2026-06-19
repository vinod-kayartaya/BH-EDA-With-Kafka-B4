package co.vinod.kafka.streams.topology;

import co.vinod.kafka.model.AmountSummary;
import co.vinod.kafka.serde.SerdesFactory;
import co.vinod.kafka.util.TopicNames;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

public class AmountSummaryTopology {

        public static void build(StreamsBuilder builder) {

                builder.stream(
                                TopicNames.PAYMENTS_RECEIVED,
                                Consumed.with(
                                                Serdes.String(),
                                                SerdesFactory.paymentSerde()))
                                .groupByKey()
                                .windowedBy(
                                                TimeWindows.ofSizeWithNoGrace(
                                                                Duration.ofMinutes(1)))
                                .aggregate(
                                                () -> 0.0,
                                                (key, payment, total) -> total + payment.getAmount(),
                                                Materialized.with(
                                                                Serdes.String(),
                                                                Serdes.Double()))
                                .toStream()
                                .map((window, total) -> KeyValue.pair(
                                                window.key(),
                                                new AmountSummary(
                                                                window.window().start(),
                                                                window.window().end(),
                                                                total)))
                                .to(
                                                TopicNames.PAYMENT_AMOUNT_SUMMARY,
                                                Produced.with(
                                                                Serdes.String(),
                                                                SerdesFactory.amountSummarySerde()));
        }
}