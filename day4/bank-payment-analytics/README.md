# Kafka Streams

## What is Kafka Streams?

Kafka Streams is a Java library for building **real-time stream processing applications** on top of Kafka.

It allows applications to:

- Read events from Kafka topics
- Process those events continuously
- Create new derived events
- Write results back to Kafka topics

Unlike a traditional consumer that simply reads and processes messages, Kafka Streams can perform:

- Filtering
- Transformations
- Aggregations
- Joins
- Windowing
- Stateful processing

## Why Kafka Streams?

Imagine a bank receives payment events:

```json
{
  "paymentId": "P1001",
  "customerId": "C101",
  "amount": 2500
}
```

A normal consumer may:

```text
Read Event
    |
    v
Store in Database
```

Kafka Streams can:

```text
Read Event
    |
    +--> Count Payments
    |
    +--> Detect Fraud
    |
    +--> Calculate Revenue
    |
    +--> Create Dashboard Metrics
    |
    v
Publish New Events
```

## Kafka Streams Architecture

Using our Payment Analytics example:

```text
payments-received
        |
        v
PaymentAnalyticsApp
        |
        +--> payments-per-minute
        |
        +--> payment-amount-summary
        |
        +--> city-payment-summary
        |
        +--> payment-method-summary
        |
        +--> high-value-payments
        |
        +--> fraud-alerts
```

Kafka Streams consumes one stream and produces many new streams.

# Core Concepts

## KStream

A KStream represents an unbounded stream of events.

Example:

```java
KStream<String, Payment> payments =
        builder.stream(
                TopicNames.PAYMENTS_RECEIVED
        );
```

Every payment event becomes part of this stream.

## Stateless Operations

These operations process one event at a time.

### Filter

Keep only high-value payments.

```java
payments.filter(
        (key, payment) ->
                payment.getAmount() > 50000
);
```

Input:

```text
₹1000
₹75000
₹2000
```

Output:

```text
₹75000
```

### Map

Transform events.

```java
payments.mapValues(
        payment ->
                payment.getAmount()
);
```

Input:

```text
Payment Object
```

Output:

```text
Double Amount
```

## Stateful Operations

These operations remember previous events.

Examples:

- Counts
- Sums
- Averages
- Windowing
- Joins

State is stored locally in Kafka Streams State Stores.

# Grouping

Suppose we want to count payments per customer.

```java
payments.groupBy(
        (key, payment) ->
                payment.getCustomerId()
);
```

Result:

```text
C101
C101
C102
C101
C102
```

Grouped by customer.

# Aggregation

Count events.

```java
.count()
```

Result:

```text
C101 -> 3
C102 -> 2
```

# Windowing

Streams never end.

So questions like:

> How many payments occurred?

are impossible.

Instead we ask:

> How many payments occurred in the last minute?

Windowing solves this problem.

## Tumbling Window

Fixed-size windows.

```java
.windowedBy(
        TimeWindows.ofSizeWithNoGrace(
                Duration.ofMinutes(1)
        )
)
```

Example:

```text
10:00 - 10:01
10:01 - 10:02
10:02 - 10:03
```

Each event belongs to exactly one window.

## Sliding Window

Useful for fraud detection.

```java
.windowedBy(
        SlidingWindows.ofTimeDifferenceWithNoGrace(
                Duration.ofMinutes(1)
        )
)
```

Question:

```text
Did this customer make
more than 5 payments
within any 1-minute period?
```

Perfect for suspicious activity detection.

# Example: Payments Per Minute

```java
builder.stream(
        TopicNames.PAYMENTS_RECEIVED
)
.groupByKey()
.windowedBy(
        TimeWindows.ofSizeWithNoGrace(
                Duration.ofMinutes(1)
        )
)
.count();
```

Result:

```json
{
  "windowStart": 1748774400000,
  "windowEnd": 1748774460000,
  "totalPayments": 42
}
```

Published to:

```text
payments-per-minute
```

# Example: High Value Payments

```java
builder.stream(
        TopicNames.PAYMENTS_RECEIVED
)
.filter(
        (key,payment) ->
                payment.getAmount() > 50000
)
.to(
        TopicNames.HIGH_VALUE_PAYMENTS
);
```

Published to:

```text
high-value-payments
```

# Example: Fraud Detection

```java
builder.stream(
        TopicNames.PAYMENTS_RECEIVED
)
.groupBy(
        (key,payment) ->
                payment.getCustomerId()
)
.windowedBy(
        SlidingWindows.ofTimeDifferenceWithNoGrace(
                Duration.ofMinutes(1)
        )
)
.count()
.toStream()
.filter(
        (window,count) ->
                count > 5
);
```

Output:

```json
{
  "customerId": "C999",
  "paymentCount": 8,
  "alertType": "EXCESSIVE_ACTIVITY"
}
```

Published to:

```text
fraud-alerts
```

# KStream vs KTable

## KStream

Every event is important.

```text
P1
P2
P3
P4
```

Represents a stream of facts.

## KTable

Represents the latest state.

```text
Customer -> TotalSpent
```

Example:

```text
C101 -> 5000
C101 -> 7000
C101 -> 9000
```

KTable stores:

```text
C101 -> 9000
```

only the latest value.

# Kafka Streams Advantages

### Simple

Pure Java library.

No separate cluster required.

### Scalable

Partitions automatically distribute workload.

### Fault Tolerant

State stores are backed by Kafka changelog topics.

### Real-Time

Processes events as they arrive.

### Event-Driven

Produces new events that other services can consume.

# Running Our Demo

### Terminal 1

Start Kafka.

### Terminal 2

Run Producer.

```bash
mvn exec:java \
-Dexec.mainClass=co.vinod.kafka.producer.PaymentProducer
```

### Terminal 3

Run Streams Application.

```bash
mvn exec:java \
-Dexec.mainClass=co.vinod.kafka.streams.PaymentAnalyticsApp
```

### Terminal 4+

Observe derived streams.

```bash
kafka-console-consumer.sh \
--topic payments-per-minute \
--from-beginning
```

```bash
kafka-console-consumer.sh \
--topic high-value-payments \
--from-beginning
```

```bash
kafka-console-consumer.sh \
--topic fraud-alerts \
--from-beginning
```

# Key Takeaway

A Kafka Consumer processes events and usually stops there.

A Kafka Streams application processes events, maintains state, performs aggregations/windowing, and continuously publishes new derived events back to Kafka.

```text
Events In
    |
    v
Kafka Streams
    |
    v
New Events Out
```

That ability to create real-time derived streams is what makes Kafka Streams one of the most powerful components in the Kafka ecosystem.
