from confluent_kafka import Consumer, KafkaError  # Fix: Imported KafkaError correctly
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

# 1. Configure the Schema Registry Client
schema_registry_conf = {"url": "http://localhost:8081"}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# 2. Create the Avro Deserializer for the message value
avro_deserializer = AvroDeserializer(schema_registry_client)

# 3. Configure the Kafka Consumer
consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "customer-purchases-group",
    "auto.offset.reset": "earliest",
}
consumer = Consumer(consumer_conf)

# 4. Subscribe to the target topic
topic = "customer-purchases"
consumer.subscribe([topic])

print(f"Listening for Avro messages on topic: {topic}...")

# 5. Poll and process loop
try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            # Fix: Check if it is just the end of the partition partition
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Consumer error: {msg.error()}")
                continue

        try:
            # Deserialize the binary Avro value into a Python dictionary
            purchased_data = avro_deserializer(msg.value(), ctx=None)
            print(f"Successfully consumed message: {purchased_data}")

        except Exception as deserialization_error:
            print(f"Failed to deserialize message: {deserialization_error}")

except KeyboardInterrupt:
    print("Aborted by user.")

finally:
    # Close down consumer cleanly
    consumer.close()
