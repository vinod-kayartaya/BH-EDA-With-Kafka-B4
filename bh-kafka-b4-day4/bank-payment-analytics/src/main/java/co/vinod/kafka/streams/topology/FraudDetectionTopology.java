package co.vinod.kafka.streams.topology;

import co.vinod.kafka.model.FraudAlert;
import co.vinod.kafka.serde.SerdesFactory;
import co.vinod.kafka.util.TopicNames;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import org.apache.kafka.streams.KeyValue;

public class FraudDetectionTopology {

        public static void build(StreamsBuilder builder) {

                builder.stream(
                                TopicNames.PAYMENTS_RECEIVED,
                                Consumed.with(
                                                Serdes.String(),
                                                SerdesFactory.paymentSerde()))
                                .groupBy(
                                                (key, payment) -> payment.getCustomerId(),
                                                Grouped.with(
                                                                Serdes.String(),
                                                                SerdesFactory.paymentSerde()))
                                .windowedBy(
                                                SlidingWindows.ofTimeDifferenceWithNoGrace(
                                                                Duration.ofMinutes(1)))
                                .count()
                                .toStream()
                                .filter((window, count) -> count > 5)
                                .map((window, count) -> KeyValue.pair(
                                                window.key(),
                                                new FraudAlert(
                                                                window.key(),
                                                                count,
                                                                "EXCESSIVE_ACTIVITY")))
                                .to(
                                                TopicNames.FRAUD_ALERTS,
                                                Produced.with(
                                                                Serdes.String(),
                                                                SerdesFactory.fraudAlertSerde()));
        }
}