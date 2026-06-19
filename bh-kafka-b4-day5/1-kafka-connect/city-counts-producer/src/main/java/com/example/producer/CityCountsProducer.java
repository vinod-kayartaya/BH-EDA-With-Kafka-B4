package com.example.producer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.Future;

public class CityCountsProducer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(CityCountsProducer.class);

    public static final String TOPIC = "city-counts";

    // Inline schema — kept in sync with src/main/avro/CityCounts.avsc.
    private static final String SCHEMA_JSON = "{"
            + "\"type\":\"record\","
            + "\"name\":\"CityCounts\","
            + "\"namespace\":\"com.example.producer\","
            + "\"fields\":["
            + "  {\"name\":\"city\",        \"type\":\"string\"},"
            + "  {\"name\":\"total_count\", \"type\":\"int\"}"
            + "]"
            + "}";

    private final Schema schema;
    private final KafkaProducer<String, GenericRecord> producer;

    public CityCountsProducer(Properties props) {
        this.schema = new Schema.Parser().parse(SCHEMA_JSON);
        this.producer = new KafkaProducer<>(props);
        log.info("Producer created. Bootstrap servers: {}",
                props.getProperty("bootstrap.servers"));
    }

    public Future<RecordMetadata> send(String city, int totalCount) {
        GenericRecord record = new GenericData.Record(schema);
        record.put("city", city);
        record.put("total_count", totalCount);

        ProducerRecord<String, GenericRecord> producerRecord = new ProducerRecord<>(TOPIC, city, record);

        log.debug("Sending → topic={} key={} total_count={}", TOPIC, city, totalCount);

        return producer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send record for city '{}': {}", city, exception.getMessage(), exception);
            } else {
                log.info("Sent  ✓  city='{}' count={} → partition={} offset={}",
                        city, totalCount, metadata.partition(), metadata.offset());
            }
        });
    }

    public void flush() {
        producer.flush();
        log.debug("Producer flushed.");
    }

    @Override
    public void close() {
        producer.close();
        log.info("Producer closed.");
    }
}
