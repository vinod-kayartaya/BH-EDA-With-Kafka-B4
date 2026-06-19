package co.vinod.kafka.streams.topology;

import co.vinod.kafka.model.Payment;
import co.vinod.kafka.model.PaymentSummary;
import co.vinod.kafka.serde.SerdesFactory;
import co.vinod.kafka.util.TopicNames;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

public class PaymentsPerMinuteTopology {

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
                                .count()
                                .toStream()
                                .map((window, count) -> KeyValue.pair(
                                                window.key(),
                                                new PaymentSummary(
                                                                window.window().start(),
                                                                window.window().end(),
                                                                count)))
                                .to(
                                                TopicNames.PAYMENTS_PER_MINUTE,
                                                Produced.with(
                                                                Serdes.String(),
                                                                SerdesFactory.paymentSummarySerde()));
        }
}