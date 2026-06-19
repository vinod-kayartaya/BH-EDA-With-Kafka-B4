from confluent_kafka import Consumer, KafkaError, KafkaException
from customer_pb2 import Customer  # generated from your .proto

KAFKA_CONFIG = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "customer-consumer-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

TOPIC = "customer-events"


def deserialize_customer(raw_bytes: bytes) -> Customer:
    customer = Customer()
    customer.ParseFromString(raw_bytes)
    return customer


def process(customer: Customer):
    print(f"Customer ID   : {customer.customerId}")
    print(f"Name          : {customer.name}")
    print(f"Email         : {customer.email}")
    print(f"Credit Limit  : {customer.creditLimit:.2f}")
    print("-" * 40)


def consume():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC])

    print(f"Listening on topic: {TOPIC}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print(f"End of partition: {msg.topic()} [{msg.partition()}]")
                else:
                    raise KafkaException(msg.error())
                continue

            try:
                customer = deserialize_customer(msg.value())
                process(customer)
                consumer.commit(message=msg)  # manual commit after successful processing
            except Exception as e:
                print(f"Failed to process message: {e}")

    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        consumer.close()


if __name__ == "__main__":
    consume()