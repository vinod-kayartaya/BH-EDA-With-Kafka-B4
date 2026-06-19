# Apache Avro

## Learning Objectives

After completing this tutorial, you will be able to:

- Understand what Apache Avro is
- Create an Avro schema
- Generate Java classes from schemas
- Serialize and deserialize data using Avro
- Understand the role of Schema Registry
- Explain the benefits of Avro in Kafka applications

# What is Apache Avro?

Apache Avro is a data serialization framework that converts objects into a compact binary format.

It is widely used with Kafka because it provides:

- Smaller messages
- Faster serialization
- Strongly typed data contracts
- Schema evolution support

# Why Not Use JSON?

Consider the following event:

```json
{
  "customerId": "C1001",
  "purchaseAmount": 1500,
  "pointsEarned": 150
}
```

JSON is easy to read but has some drawbacks:

- Larger message size
- No formal schema
- Difficult to manage changes across multiple applications

# What is an Avro Schema?

An Avro schema defines the structure of a message.

Example:

```json
{
  "type": "record",
  "name": "CustomerPurchase",
  "namespace": "co.vinod.loyalty.avro.v1",

  "fields": [
    {
      "name": "customerId",
      "type": "string"
    },
    {
      "name": "purchaseAmount",
      "type": "double"
    },
    {
      "name": "pointsEarned",
      "type": "int"
    }
  ]
}
```

File extension:

```text
.avsc
```

# Code Generation

The Avro Maven Plugin generates Java classes automatically.

Schema:

```text
CustomerPurchase.avsc
```

Generated class:

```java
CustomerPurchase purchase =
        CustomerPurchase.newBuilder()
                .setCustomerId("C1001")
                .setPurchaseAmount(1500)
                .setPointsEarned(150)
                .build();
```

This eliminates manual POJO creation.

# Serialization and Deserialization

## Serialization

Convert Java object into Avro binary format.

```text
Java Object
      ↓
Avro Binary Data
```

## Deserialization

Convert Avro binary data back into a Java object.

```text
Avro Binary Data
      ↓
Java Object
```

# Avro with Kafka

A Kafka producer can publish Avro events directly.

```text
Producer
    ↓
Kafka Topic
    ↓
Consumer
```

Instead of sending JSON strings, producers send strongly-typed Avro objects.

Example:

```java
CustomerPurchase purchase =
        CustomerPurchase.newBuilder()
                .setCustomerId("C1001")
                .setPurchaseAmount(1500)
                .setPointsEarned(150)
                .build();
```

# What is Schema Registry?

Schema Registry is a centralized repository for storing and managing Avro schemas.

It provides:

- Schema versioning
- Compatibility checking
- Centralized schema management

# How Schema Registration Works

When a producer sends an Avro message:

```text
CustomerPurchase Object
         ↓
KafkaAvroSerializer
         ↓
Schema Registry
         ↓
Schema ID Assigned
         ↓
Kafka Topic
```

The schema is automatically registered the first time it is used.

Example subject:

```text
customer-purchases-value
```

# Why Use Avro and Schema Registry?

## 1. Smaller Messages

JSON contains field names in every message.

```json
{
  "customerId": "C1001",
  "purchaseAmount": 1500
}
```

Avro stores binary data and a schema reference, making messages significantly smaller.

## 2. Better Performance

Binary serialization is typically faster than text-based JSON parsing.

Benefits:

- Reduced CPU usage
- Faster network transfer
- Lower storage requirements

## 3. Strong Contracts

Schemas define exactly what data producers and consumers exchange.

Example:

```text
CustomerPurchase
 ├─ customerId
 ├─ purchaseAmount
 └─ pointsEarned
```

Everyone follows the same contract.

## 4. Safe Schema Evolution

Suppose we add a new field:

```json
{
  "name": "customerTier",
  "type": "string",
  "default": "SILVER"
}
```

Schema Registry can verify whether this change is compatible with existing consumers.

This prevents breaking production systems.

## 5. Version Management

Schema Registry maintains versions automatically.

Example:

```text
customer-purchases-value

Version 1
Version 2
Version 3
```

Developers can track schema history centrally.

# Typical Kafka + Avro Architecture

```text
Producer
    ↓
KafkaAvroSerializer
    ↓
Schema Registry
    ↓
Kafka Topic
    ↓
KafkaAvroDeserializer
    ↓
Consumer
```

# Summary

Apache Avro provides:

- Compact binary messages
- Strongly typed schemas
- Fast serialization
- Support for schema evolution

Schema Registry provides:

- Centralized schema storage
- Version management
- Compatibility enforcement
- Safe evolution of event contracts

Together, Avro and Schema Registry form the standard approach for building scalable, reliable Kafka-based event-driven systems.
