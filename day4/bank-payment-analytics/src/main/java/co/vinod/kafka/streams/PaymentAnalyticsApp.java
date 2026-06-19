package co.vinod.kafka.streams;

import co.vinod.kafka.config.KafkaConfig;
import co.vinod.kafka.streams.topology.AmountSummaryTopology;
import co.vinod.kafka.streams.topology.CitySummaryTopology;
import co.vinod.kafka.streams.topology.FraudDetectionTopology;
import co.vinod.kafka.streams.topology.HighValuePaymentTopology;
import co.vinod.kafka.streams.topology.PaymentMethodTopology;
import co.vinod.kafka.streams.topology.PaymentsPerMinuteTopology;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

public class PaymentAnalyticsApp {

    public static void main(String[] args) {

        System.out.println();
        System.out.println("====================================");
        System.out.println("PAYMENT ANALYTICS APPLICATION");
        System.out.println("====================================");

        StreamsBuilder builder = new StreamsBuilder();

        PaymentsPerMinuteTopology.build(builder);
        AmountSummaryTopology.build(builder);
        CitySummaryTopology.build(builder);
        PaymentMethodTopology.build(builder);
        HighValuePaymentTopology.build(builder);
        FraudDetectionTopology.build(builder);

        KafkaStreams streams = new KafkaStreams(
                builder.build(),
                KafkaConfig.streamsConfig());

        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(streams::close));

        streams.start();

        System.out.println("Kafka Streams started");
    }
}