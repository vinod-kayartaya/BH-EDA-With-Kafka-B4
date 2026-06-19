# V1 User Guide - Avro Serialization and Deserialization

## Objective

In this version, we learn the fundamentals of Apache Avro without involving Kafka.

The goal is to understand:

- What an Avro schema is
- How Java classes are generated from a schema
- How Java objects are serialized into Avro format
- How Avro data is deserialized back into Java objects

This example forms the foundation for all subsequent versions of the project.

---

# Business Scenario

An e-commerce company maintains a loyalty rewards system.

Whenever a customer makes a purchase, the purchase information is captured as an event.

In Version 1, instead of sending the event to Kafka, we simply store it in an Avro file.

Example purchase:

| Field           | Value   |
| --------------- | ------- |
| Customer ID     | C1001   |
| Purchase Amount | 1500.00 |
| Points Earned   | 150     |

---

# Project Components

## Schema

Location:

```text
schemas/v1/CustomerPurchase.avsc
```

This schema defines the structure of the purchase event.

```json
{
  "type": "record",
  "name": "CustomerPurchase",
  "namespace": "co.vinod.loyalty.avro.v1",
  "fields": [
    { "name": "customerId", "type": "string" },
    { "name": "purchaseAmount", "type": "double" },
    { "name": "pointsEarned", "type": "int" }
  ]
}
```

---

## Generated Class

The Avro Maven Plugin generates the Java class automatically.

Generated class:

```text
target/generated-sources/avro/
└── co/vinod/loyalty/avro/v1/CustomerPurchase.java
```

This class should never be modified manually.

---

## PurchaseWriter

Location:

```text
co.vinod.loyalty.v1.PurchaseWriter
```

Responsibilities:

- Create a CustomerPurchase object
- Serialize it into Avro format
- Store it in a file

Output file:

```text
purchase.avro
```

---

## PurchaseReader

Location:

```text
co.vinod.loyalty.v1.PurchaseReader
```

Responsibilities:

- Read purchase.avro
- Deserialize Avro data
- Display the contents

---

# Understanding the Flow

## Step 1

Create Java Object

```text
CustomerPurchase
```

↓

## Step 2

Serialize

```text
Java Object
      ↓
Avro Binary Format
```

↓

## Step 3

Store in File

```text
purchase.avro
```

↓

## Step 4

Read File

```text
purchase.avro
```

↓

## Step 5

Deserialize

```text
Avro Binary Format
      ↓
Java Object
```

↓

## Step 6

Display Data

```text
Customer ID
Purchase Amount
Points Earned
```

---

# Generating Avro Classes

From the project root directory:

```bash
mvn clean generate-sources
```

This command performs:

1. Reads the .avsc file
2. Generates Java classes
3. Places generated classes in:

```text
target/generated-sources/avro
```

---

# Compiling the Project

```bash
mvn clean compile
```

Successful compilation indicates:

- Schema is valid
- Generated classes are available
- Source code compiles successfully

---

# Running the Writer

Execute:

```bash
mvn exec:java \
-Dexec.mainClass=co.vinod.loyalty.v1.PurchaseWriter
```

Expected Output:

```text
====================================
AVRO FILE WRITER
====================================

Record Created

{"customerId":"C1001","purchaseAmount":1500.0,"pointsEarned":150}

Record written successfully to purchase.avro
```

Verify that the file exists:

```text
purchase.avro
```

in the project root directory.

---

# Running the Reader

Execute:

```bash
mvn exec:java \
-Dexec.mainClass=co.vinod.loyalty.v1.PurchaseReader
```

Expected Output:

```text
====================================
AVRO FILE READER
====================================

Purchase Details
-------------------------
Customer ID     : C1001
Purchase Amount : 1500.0
Points Earned   : 150
```

---

# Important Avro Classes

## SpecificDatumWriter

Used by the writer.

Purpose:

Convert a Java object into Avro binary format.

```java
SpecificDatumWriter<CustomerPurchase>
```

---

## DataFileWriter

Used by the writer.

Purpose:

Write Avro data into a file.

```java
DataFileWriter<CustomerPurchase>
```

---

## SpecificDatumReader

Used by the reader.

Purpose:

Convert Avro binary data back into Java objects.

```java
SpecificDatumReader<CustomerPurchase>
```

---

## DataFileReader

Used by the reader.

Purpose:

Read Avro records from a file.

```java
DataFileReader<CustomerPurchase>
```

---

# What Makes Avro Useful?

Compared to plain JSON:

Advantages:

- Compact binary format
- Faster serialization
- Smaller message size
- Strong schema definition
- Supports schema evolution

Example:

JSON:

```json
{
  "customerId": "C1001",
  "purchaseAmount": 1500,
  "pointsEarned": 150
}
```

Avro:

```text
Binary encoded data
```

which is typically much smaller.

---

# Key Learning Outcomes

After completing this example, you should be able to:

- Create an Avro schema
- Generate Java classes from Avro schemas
- Serialize Java objects using Avro
- Deserialize Avro files
- Understand the relationship between schema and generated classes
- Explain the role of Avro in event-driven architectures

---

# What Comes Next?

Version 2 introduces Kafka.

Instead of writing the event to a file:

```text
CustomerPurchase
       ↓
purchase.avro
```

we will publish the event to a Kafka topic:

```text
CustomerPurchase
       ↓
Kafka Producer
       ↓
Kafka Topic
       ↓
Kafka Consumer
```

This demonstrates how Avro is commonly used in real-world event-driven systems.
