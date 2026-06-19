package co.vinod.streams;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import com.fasterxml.jackson.databind.ObjectMapper;

import co.vinod.model.Payment;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class PaymentSummaryStream {

        public static void main(String[] args) {

                Properties props = new Properties();
                props.put(StreamsConfig.APPLICATION_ID_CONFIG, "payment-summary-demo");
                props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

                StreamsBuilder builder = new StreamsBuilder();

                Serde<Payment> paymentSerde = getPaymentSerde();

                KStream<String, Payment> payments = builder.stream(
                                "payments",
                                Consumed.with(Serdes.String(), paymentSerde));

                payments
                                .groupBy((k, v) -> v.getPaymentType(),
                                                Grouped.with(Serdes.String(), paymentSerde))
                                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(30)))
                                .count()
                                .toStream()
                                .foreach((window, count) -> {
                                        System.out.printf(
                                                        "Window [%s - %s] -> %s=%d%n",
                                                        window.window().startTime(),
                                                        window.window().endTime(),
                                                        window.key(),
                                                        count);
                                });

                KafkaStreams streams = new KafkaStreams(builder.build(), props);

                // 3. CRITICAL: Use a latch and shutdown hook to keep the main thread alive
                CountDownLatch latch = new CountDownLatch(1);

                Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
                        @Override
                        public void run() {
                                streams.close();
                                latch.countDown();
                        }
                });

                try {
                        streams.start();
                        System.out.println("Stream started.. Press Ctrl+C to exit.");
                        latch.await(); // This pauses the main thread indefinitely
                } catch (Throwable e) {
                        System.exit(1);
                }
                System.exit(0);
        }

        private static Serde<Payment> getPaymentSerde() {
                ObjectMapper objectMapper = new ObjectMapper();
                Serializer<Payment> serializer = (topic, data) -> {
                        try {
                                return objectMapper.writeValueAsBytes(data);
                        } catch (Exception e) {
                                throw new RuntimeException(e);
                        }
                };
                Deserializer<Payment> deserializer = (topic, data) -> {
                        try {
                                return objectMapper.readValue(data, Payment.class);
                        } catch (Exception e) {
                                throw new RuntimeException(e);
                        }
                };

                Serde<Payment> paymentSerde = Serdes.serdeFrom(serializer, deserializer);
                return paymentSerde;
        }
}
