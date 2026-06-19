package co.vinod.kafka.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerde<T>
        implements Serde<T> {

    private final Serializer<T> serializer;
    private final Deserializer<T> deserializer;

    public JsonSerde(Class<T> clazz) {

        this.serializer = new JsonSerializer<>();

        this.deserializer = new JsonDeserializer<>(clazz);
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }
}
