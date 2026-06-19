package co.vinod.kafka.streams.topology;

import co.vinod.kafka.model.CitySummary;
import co.vinod.kafka.model.Payment;
import co.vinod.kafka.serde.SerdesFactory;
import co.vinod.kafka.util.TopicNames;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;

public class CitySummaryTopology {

        public static void build(StreamsBuilder builder) {

                builder.stream(
                                TopicNames.PAYMENTS_RECEIVED,
                                Consumed.with(
                                                Serdes.String(),
                                                SerdesFactory.paymentSerde()))
                                .groupBy(
                                                (key, payment) -> payment.getCity(),
                                                Grouped.with(
                                                                Serdes.String(),
                                                                SerdesFactory.paymentSerde()))
                                .count()
                                .toStream()
                                .map((city, count) -> KeyValue.pair(
                                                city,
                                                new CitySummary(
                                                                city,
                                                                count)))
                                .to(
                                                TopicNames.CITY_PAYMENT_SUMMARY,
                                                Produced.with(
                                                                Serdes.String(),
                                                                SerdesFactory.citySummarySerde()));
        }
}