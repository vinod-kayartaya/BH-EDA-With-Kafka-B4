# Protobuf with Kafka

## Learning Objective

By the end of this exercise, you will be able to:

- Understand what Protocol Buffers (Protobuf) are
- Produce Protobuf messages to Kafka
- Consume Protobuf messages from Kafka
- Understand schema evolution
- Compare Protobuf with JSON and Avro

---

# Business Scenario

A loyalty management system publishes customer reward events.

Whenever a customer earns points, a message is sent to Kafka.

Example Event:

```json
{
  "customerId": "C100",
  "tier": "GOLD",
  "points": 250,
  "eventType": "POINTS_EARNED"
}
```

Instead of sending JSON text, we will serialize the message using Protobuf.

---

# What is Protobuf?

Protocol Buffers (Protobuf) is Google's language-neutral serialization format.

Think of it as:

```text
Java Object
      ↓
Protobuf
      ↓
Compact Binary Data
      ↓
Kafka Topic
```

Unlike JSON, Protobuf messages are stored in a compact binary format.

Benefits:

- Smaller payloads
- Faster serialization
- Faster deserialization
- Strong schema definition
- Excellent cross-language support

---

# Step 1 – Define the Schema

Create:

```proto
syntax = "proto3";

package co.vinod.protobuf;

option java_multiple_files = true;
option java_package = "co.vinod.protobuf.model";

message LoyaltyEvent {

  string customerId = 1;
  string tier = 2;
  int32 points = 3;
  string eventType = 4;
}
```

Save as:

```text
src/main/proto/loyalty-event.proto
```

---

# Step 2 – Generate Java Classes

Run:

```bash
mvn clean compile
```

Maven invokes the Protobuf compiler and generates Java classes automatically.

Generated class:

```java
LoyaltyEvent
```

---

# Step 3 – Producer

Create the message:

```java
LoyaltyEvent event =
        LoyaltyEvent.newBuilder()
                .setCustomerId("C100")
                .setTier("GOLD")
                .setPoints(250)
                .setEventType("POINTS_EARNED")
                .build();
```

Serialize:

```java
byte[] payload = event.toByteArray();
```

Send to Kafka:

```java
ProducerRecord<String, byte[]> record =
        new ProducerRecord<>(
                "loyalty-events",
                event.getCustomerId(),
                payload);

producer.send(record);
```

---

# Step 4 – Consumer

Read message:

```java
ConsumerRecord<String, byte[]> record;
```

Deserialize:

```java
LoyaltyEvent event =
        LoyaltyEvent.parseFrom(record.value());
```

Display:

```java
System.out.println(event);
```

Output:

```text
customerId: "C100"
tier: "GOLD"
points: 250
eventType: "POINTS_EARNED"
```

---

# Understanding the Message Flow

```text
Producer
   ↓
LoyaltyEvent Object
   ↓
Protobuf Serialization
   ↓
Binary Message
   ↓
Kafka Topic
   ↓
Consumer
   ↓
Protobuf Deserialization
   ↓
LoyaltyEvent Object
```

---

# Schema Evolution Example

Version 1

```proto
message LoyaltyEvent {

  string customerId = 1;
  string tier = 2;
  int32 points = 3;
  string eventType = 4;
}
```

---

Version 2

```proto
message LoyaltyEvent {

  string customerId = 1;
  string tier = 2;
  int32 points = 3;
  string eventType = 4;
  string city = 5;
}
```

Producer:

```text
Version 2
```

Consumer:

```text
Version 1
```

Result:

```text
Consumer ignores unknown field.
Processing continues successfully.
```

This is one of Protobuf's strongest features.

---

# Comparing JSON, Avro and Protobuf

## JSON

Example

```json
{
  "customerId": "C100",
  "tier": "GOLD",
  "points": 250,
  "eventType": "POINTS_EARNED"
}
```

### Advantages

- Human readable
- Easy debugging
- No tooling required
- Supported everywhere

### Disadvantages

- Large payload size
- No schema enforcement
- Slower serialization
- Slower deserialization

---

## Avro

Example

Schema:

```json
{
  "type": "record",
  "name": "LoyaltyEvent",
  "fields": [
    { "name": "customerId", "type": "string" },
    { "name": "tier", "type": "string" },
    { "name": "points", "type": "int" },
    { "name": "eventType", "type": "string" }
  ]
}
```

### Advantages

- Compact binary format
- Excellent schema evolution
- Tight integration with Schema Registry
- Very popular in Kafka ecosystems

### Disadvantages

- Requires schema management
- Not human readable
- Slightly more setup than JSON

---

## Protobuf

Example

```proto
message LoyaltyEvent {

  string customerId = 1;
  string tier = 2;
  int32 points = 3;
  string eventType = 4;
}
```

### Advantages

- Very compact
- Very fast
- Language neutral
- Strong schema definition
- Used heavily by Google and gRPC

### Disadvantages

- Binary format
- Field numbers must be managed carefully
- Schema Registry support requires additional configuration

---

# Quick Comparison Table

| Feature                     | JSON    | Avro          | Protobuf  |
| --------------------------- | ------- | ------------- | --------- |
| Human Readable              | Yes     | No            | No        |
| Schema Required             | No      | Yes           | Yes       |
| Binary Format               | No      | Yes           | Yes       |
| Kafka Ecosystem Support     | Good    | Excellent     | Excellent |
| Schema Evolution            | Weak    | Excellent     | Excellent |
| Payload Size                | Largest | Small         | Smallest  |
| Serialization Speed         | Slowest | Fast          | Fastest   |
| Schema Registry Integration | Limited | Native Choice | Supported |
| Cross-Language Support      | Good    | Good          | Excellent |

---

# When Should You Use What?

### JSON

Use when:

- Debugging simplicity is important
- Message volume is low
- Rapid prototyping is needed

---

### Avro

Use when:

- Building Kafka-centric systems
- Using Confluent Schema Registry
- Managing large event streams
- Strong schema governance is required

---

### Protobuf

Use when:

- Performance is critical
- Payload size matters
- Multiple programming languages are involved
- Using gRPC and Kafka together

---

# Key Takeaway

For Kafka applications:

```text
JSON
  ↓
Easy to learn
```

```text
Avro
  ↓
Best choice for Kafka ecosystems
```

```text
Protobuf
  ↓
Best choice for performance and cross-language systems
```

In most enterprise Kafka deployments, Avro is the most common choice because of its seamless integration with Schema Registry, while Protobuf is increasingly used when Kafka and gRPC-based microservices coexist in the same architecture.
