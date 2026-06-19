package com.example.payments;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import java.time.Duration;
import java.util.Properties;

public class PaymentWindowAggregationApp {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "payment-window-aggregation");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Double> payments =
            builder.stream("payment-received",
                Consumed.with(Serdes.String(), Serdes.Double()));

        KTable<Windowed<String>, Double> sums =
            payments
                .groupBy((k,v) -> "GLOBAL",
                    Grouped.with(Serdes.String(), Serdes.Double()))
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(10)))
                .aggregate(
                    () -> 0.0,
                    (k,v,total) -> total + v,
                    Materialized.with(Serdes.String(), Serdes.Double())
                );

        sums.toStream()
            .map((wk,sum) -> KeyValue.pair(
                "GLOBAL-" + wk.window().start(),
                sum))
            .to("payment-aggregates-10m",
                Produced.with(Serdes.String(), Serdes.Double()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
