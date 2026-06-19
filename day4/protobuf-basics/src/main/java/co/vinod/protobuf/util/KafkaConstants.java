package co.vinod.protobuf.util;

public final class KafkaConstants {

    private KafkaConstants() {
    }

    public static final String BOOTSTRAP_SERVERS = "localhost:9092";

    public static final String TOPIC_NAME = "customer-events";

    public static final String CONSUMER_GROUP = "customer-group";
}