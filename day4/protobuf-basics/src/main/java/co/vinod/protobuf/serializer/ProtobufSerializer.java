package co.vinod.protobuf.serializer;

import co.vinod.protobuf.model.Customer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class ProtobufSerializer implements Serializer<Customer> {

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // No configuration required
    }

    @Override
    public byte[] serialize(String topic, Customer data) {

        if (data == null) {
            return null;
        }

        return data.toByteArray();
    }

    @Override
    public void close() {
        // Nothing to close
    }
}