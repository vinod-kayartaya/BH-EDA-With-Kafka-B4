package co.vinod.protobuf.serializer;

import com.google.protobuf.InvalidProtocolBufferException;
import co.vinod.protobuf.model.Customer;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class ProtobufDeserializer implements Deserializer<Customer> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration required
    }

    @Override
    public Customer deserialize(String topic, byte[] data) {

        if (data == null) {
            return null;
        }

        try {
            return Customer.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(
                    "Failed to deserialize Protobuf message", e);
        }
    }

    @Override
    public void close() {
        // Nothing to close
    }
}