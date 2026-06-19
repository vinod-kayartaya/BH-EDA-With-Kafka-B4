package co.vinod.kafka.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

public class JsonSerializer<T> implements Serializer<T> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public byte[] serialize(String topic, T data) {

        if (data == null) {
            return null;
        }

        try {
            return MAPPER.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException(
                    "Error serializing JSON", e);
        }
    }
}