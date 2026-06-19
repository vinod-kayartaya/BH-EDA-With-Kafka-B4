package co.vinod.serializer;

import org.apache.kafka.common.serialization.Serializer;

import co.vinod.model.Payment;

public class CsvSerializer implements Serializer<Payment> {

    @Override
    public byte[] serialize(String topic, Payment data) {
        return Payment.toCSV(data).getBytes();
    }

}
