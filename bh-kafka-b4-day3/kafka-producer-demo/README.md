# Kafka Producer Deep Dive

## Reference Guide for TransactionProducer_V1 to TransactionProducer_V6

# Introduction

In an Event-Driven Architecture (EDA), producers are responsible for publishing events to Kafka topics. These events can represent business activities such as:

- Payment completed
- Order created
- Customer registered
- Shipment dispatched
- Fraud detected

The examples in this guide use a simple Payment Processing System where transaction events are published to a Kafka topic named:

```text
transactions
```

Each successive producer version introduces a new Kafka concept while building on the previous version.

# Version 1: Basic Producer

## Learning Objective

Understand the minimum code required to publish an event to Kafka.

## Business Scenario

A payment application publishes completed transaction events.

```text
Payment Service
      |
      V
Kafka Producer
      |
      V
transactions Topic
```

## Key Kafka Concepts

- KafkaProducer
- ProducerRecord
- Topic
- Serialization

## Core Code

```java
KafkaProducer<String,String> producer =
        new KafkaProducer<>(props);
```

Creates a producer instance.

```java
ProducerRecord<String,String> record =
        new ProducerRecord<>(
                topic,
                json);
```

Creates a message.

```java
producer.send(record);
```

Publishes the message.

## What Happens Internally?

```text
Application
     |
Create Record
     |
Serialize JSON
     |
Send To Kafka
     |
Broker Stores Event
```

## Advantages

- Very simple
- Easy to understand
- Suitable for prototypes

## Limitation

The application does not know:

- Which partition received the message
- Whether Kafka stored it successfully
- What offset was assigned

## Real-World Use Cases

- Logging systems
- Telemetry data
- Demo applications
- Low criticality events

# Version 2: Synchronous Producer

## Learning Objective

Learn how Kafka acknowledges message delivery.

## Business Scenario

A banking application must know that a transaction was stored before continuing.

## New Concepts

- Future
- RecordMetadata
- Acknowledgements
- Offsets
- Partitions

## Core Code

```java
RecordMetadata metadata =
        producer.send(record).get();
```

The call blocks until Kafka acknowledges the write.

## Metadata Available

```java
metadata.topic();
metadata.partition();
metadata.offset();
metadata.timestamp();
```

## Execution Flow

```text
Producer
   |
Send Event
   |
Wait
   |
Broker Stores Event
   |
Acknowledgement
   |
Continue
```

## Benefits

- Reliable
- Easy to reason about
- Suitable for critical operations

## Drawbacks

The application waits for every message.

```text
Send
Wait

Send
Wait

Send
Wait
```

This reduces throughput.

## Real-World Use Cases

- Banking
- Payments
- Insurance claims
- Regulatory systems

# Version 3: Asynchronous Producer

## Learning Objective

Understand non-blocking publishing.

## Business Scenario

A payment platform processes thousands of transactions per second.

Waiting after every send would slow the system down.

## New Concepts

- Callback
- Asynchronous execution
- Throughput optimization

## Core Code

```java
producer.send(
        record,
        (metadata, exception) -> {

            if(exception == null) {

                System.out.println(
                        metadata.offset());
            }
        });
```

## Execution Flow

```text
Producer
   |
Send
   |
Continue Working
   |
Callback Invoked Later
```

## Benefits

Higher throughput

```text
Sync Producer----------
Send
Wait
Send
Wait

Async Producer-----------
Send
Send
Send
Send
Callback
Callback
```

## Error Handling

```java
if(exception != null) {
    exception.printStackTrace();
}
```

## Real-World Use Cases

- Online commerce
- IoT platforms
- Social media systems
- Streaming applications

# Version 4: Key-Based Partitioning

## Learning Objective

Understand how Kafka distributes events among partitions.

## Business Scenario

A customer's transactions must remain in order.

## New Concepts

- Message key
- Partition assignment
- Ordering guarantees

## Core Code

```java
ProducerRecord<String,String> record =
        new ProducerRecord<>(
                topic,
                customerId,
                json);
```

## Partitioning Logic

```text
CustomerId
     |
Hash Function
     |
Partition Number
```

## Example

```text
CUST-101
     |
Partition 2
```

All events for the same customer go to the same partition.

## Why Is This Important?

Consider:

```text
TXN-100
TXN-101
TXN-102
```

Kafka guarantees ordering within a partition.

## Benefits

- Consistent routing
- Event ordering
- Better stream processing

## Real-World Keys

- Customer ID
- Account Number
- Device ID
- Order ID

# Version 5: Producer Batching

## Learning Objective

Learn how Kafka improves throughput using batching.

## Problem

Without batching:

```text
Message
Network Request

Message
Network Request

Message
Network Request
```

Too many network calls.

## New Concepts

- linger.ms
- batch.size
- compression

## Configuration

```java
props.put(
        "linger.ms",
        "100");
```

```java
props.put(
        "batch.size",
        "16384");
```

```java
props.put(
        "compression.type",
        "snappy");
```

## Internal Flow

```text
Message
Message
Message
Message
     |
Create Batch
     |
Single Request
     |
Broker
```

## Benefits

- Fewer network calls
- Better throughput
- Reduced CPU utilization
- Lower network usage

## Compression

Common values:

```text
none
gzip
snappy
lz4
zstd
```

## Real-World Use Cases

- Event ingestion platforms
- Clickstream systems
- Log aggregation
- Telemetry collection

# Version 6: Idempotent Producer

## Learning Objective

Learn how Kafka prevents duplicate messages.

## The Problem

Consider this sequence:

```text
Producer
    |
Send
    |
Timeout
    |
Retry
```

Question:

Did the broker receive the first request?

Maybe.

The producer does not know.

## Possible Outcome

```text
TXN-100
TXN-100
```

Duplicate event.

## New Concepts

- Idempotence
- Retries
- Sequence Numbers
- Exactly Once Foundations

## Configuration

```java
props.put(
        "enable.idempotence",
        "true");
```

```java
props.put(
        "acks",
        "all");
```

```java
props.put(
        "retries",
        "10");
```

## Internal Flow

```text
Producer
    |
Sequence Number
    |
Broker
    |
Duplicate Detection
```

## Benefits

- Safe retries
- Duplicate prevention
- Stronger delivery guarantees

## Why Payment Systems Need This

Imagine:

```text
Transfer ₹10,000
```

If a duplicate event occurs:

```text
Debit ₹10,000
Debit ₹10,000
```

This is unacceptable.

Idempotence helps prevent such situations.

## Real-World Use Cases

- Banking systems
- Stock trading
- Payment gateways
- Order management
- Financial reconciliation

# Summary

| Version | Topic                  | Key Learning                         |
| ------- | ---------------------- | ------------------------------------ |
| V1      | Basic Producer         | Publishing events                    |
| V2      | Synchronous Producer   | Acknowledgements and metadata        |
| V3      | Asynchronous Producer  | High throughput publishing           |
| V4      | Key-Based Partitioning | Ordering and routing                 |
| V5      | Batching               | Performance optimization             |
| V6      | Idempotent Producer    | Reliability and duplicate prevention |

Together, these six examples form the foundation of Kafka Producer development and prepare students for advanced topics such as Kafka Transactions, Exactly-Once Semantics, Kafka Streams, and Event-Driven Microservices.

To execute the programs using `mvn` command:

```shell
mvn exec:java \
-Dprofile=v1 \
-Dexec.mainClass="co.vinod.kafka.producers.TransactionConsumer_V1"
```
