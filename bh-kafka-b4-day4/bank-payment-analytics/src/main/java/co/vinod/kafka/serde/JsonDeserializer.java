package co.vinod.kafka.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

public class JsonDeserializer<T>
        implements Deserializer<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final Class<T> targetClass;

    public JsonDeserializer(Class<T> targetClass) {
        this.targetClass = targetClass;
    }

    @Override
    public T deserialize(String topic,
            byte[] data) {

        if (data == null || data.length == 0) {
            return null;
        }

        try {
            return MAPPER.readValue(
                    data,
                    targetClass);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Error deserializing JSON",
                    e);
        }
    }
}