# Failure Handling & Debugging in Apache Kafka

This module focuses on one of the most important production concerns in Kafka-based systems: **what happens when things go wrong?**

In a real payment processing system, not every message can be processed successfully. Systems fail, databases go down, invalid data arrives, network interruptions occur, and downstream services become unavailable.

A robust Kafka architecture must be able to:

* Detect failures
* Isolate problematic messages
* Retry transient failures
* Avoid endless processing loops
* Preserve data integrity
* Allow operators to investigate issues

The Day 5 Module 2 topics are: 

* Poison Messages
* Retry Strategies
* Dead Letter Queue (DLQ) Pattern

# 1. Understanding Failures in Event-Driven Systems

Unlike traditional request-response systems, event-driven systems are asynchronous.

A producer may successfully send a message, but processing can fail later.

Example:

```text
Customer Places Order
        |
        V
Kafka Topic
        |
        V
Payment Service
        |
        X
Database Down
```

Producer thinks everything is successful.

Consumer fails.

The challenge is deciding:

* Should the message be retried?
* Should it be discarded?
* Should it be sent elsewhere?
* How do we avoid data loss?

# Common Failure Categories

## 1. Temporary Failures

These eventually recover.

Examples:

* Database unavailable
* REST API timeout
* Network issue
* Service restart

```text
Payment Event
      |
      V
DB Timeout
      |
 Retry
      |
Success
```

These are good candidates for retries.

## 2. Permanent Failures

These will never succeed.

Examples:

```json
{
  "paymentId": null,
  "amount": "ABC"
}
```

Invalid data.

Retrying 100 times will not fix it.

These should go to a DLQ.

## 3. Infrastructure Failures

Examples:

* Broker crash
* Disk failure
* Network partition
* Cluster outage

Kafka's replication mechanism handles many of these automatically.

# 2. What is a Poison Message?

A poison message is a message that consistently causes processing failure.

Example:

```json
{
  "paymentId":"P1001",
  "amount":"INVALID"
}
```

Consumer code:

```java
double amount =
    Double.parseDouble(payment.getAmount());
```

Result:

```text
NumberFormatException
```

Every retry produces the same exception.

The message becomes poisonous.

# Why Poison Messages Are Dangerous

Suppose the topic contains:

```text
Offset 100 -> Bad Message
Offset 101 -> Good Message
Offset 102 -> Good Message
```

Consumer processing:

```text
Read Offset 100
Exception
Restart

Read Offset 100
Exception
Restart
```

Offsets 101 and 102 are never processed.

This is called:

## Consumer Stuck Condition

```text
Bad Message
     |
     V
Infinite Failure Loop
     |
     V
Consumer Lag Growth
```

Production systems must prevent this.

# Example in Payment Processing

Valid Event:

```json
{
  "paymentId":"PAY001",
  "customerId":"C100",
  "amount":2500
}
```

Invalid Event:

```json
{
  "paymentId":"",
  "customerId":"C100",
  "amount":-500
}
```

Business validation fails:

```java
if(payment.getAmount() <= 0){
    throw new IllegalArgumentException(
        "Invalid Amount");
}
```

This message should not be endlessly retried.

# 3. Detecting Poison Messages

Common indicators:

### Repeated Processing Failures

```text
Offset 245 failed
Offset 245 failed
Offset 245 failed
Offset 245 failed
```

### Same Exception

```text
IllegalArgumentException
IllegalArgumentException
IllegalArgumentException
```

### Retry Count Exceeded

```text
Retry Attempt 1
Retry Attempt 2
Retry Attempt 3
Retry Attempt 4
Move to DLQ
```

# Logging Best Practices

Always log:

```java
log.error(
    "Failed Payment Processing. "
    + "PaymentId={}, Offset={}",
    payment.getPaymentId(),
    record.offset()
);
```

Capture:

* Topic
* Partition
* Offset
* Key
* Exception
* Timestamp

These become critical during debugging.

# 4. Retry Strategies

Many failures are temporary.

Instead of immediately failing, retry processing.

Example:

```text
Database Down
     |
Retry
     |
Retry
     |
Retry
     |
Success
```

# Strategy 1 – Immediate Retry

```java
for(int i=1;i<=3;i++){
    try{
        processPayment(payment);
        break;
    }catch(Exception e){
        log.error("Retry {}", i);
    }
}
```

Advantages:

* Simple

Disadvantages:

* High CPU usage
* Can overload systems

Not ideal in production.

# Strategy 2 – Fixed Delay Retry

Wait before retrying.

```java
for(int i=1;i<=3;i++){
    try{
        process();
        break;
    }catch(Exception e){
        Thread.sleep(5000);
    }
}
```

Flow:

```text
Failure
  |
Wait 5 sec
  |
Retry
```

Useful for temporary outages.

# Strategy 3 – Exponential Backoff

Most commonly used.

Retry intervals increase gradually.

```text
Retry 1 -> 1 sec
Retry 2 -> 2 sec
Retry 3 -> 4 sec
Retry 4 -> 8 sec
Retry 5 -> 16 sec
```

Benefits:

* Reduces system pressure
* Gives downstream services time to recover

# Exponential Backoff Formula

Delay = BaseDelay \times 2^{RetryCount}

Example:

```text
Base Delay = 1 second
Retry Count = 4

Delay = 16 seconds
```

# Strategy 4 – Retry Topics Pattern

Widely used in Kafka architectures.

Topics:

```text
payment-topic
payment-retry-1
payment-retry-2
payment-retry-3
payment-dlq
```

Flow:

```text
Main Topic
    |
Failure
    V
Retry Topic 1
    |
Failure
    V
Retry Topic 2
    |
Failure
    V
Retry Topic 3
    |
Failure
    V
DLQ
```

Benefits:

* Non-blocking
* Scalable
* Production-friendly

# Example Retry Headers

When sending to retry topic:

```java
ProducerRecord<String, Payment> record =
        new ProducerRecord<>(
            "payment-retry-1",
            payment.getPaymentId(),
            payment
        );

record.headers().add(
    "retry-count",
    "1".getBytes()
);
```

Later:

```java
int retryCount =
 Integer.parseInt(
    new String(
      record.headers()
            .lastHeader("retry-count")
            .value()
    )
 );
```

# 5. Dead Letter Queue (DLQ)

A Dead Letter Queue is a special Kafka topic that stores messages that cannot be processed successfully.

Purpose:

* Prevent endless retries
* Preserve failed events
* Enable manual investigation

# DLQ Architecture

```text
Producer
    |
Payment Topic
    |
Consumer
    |
Failure
    |
DLQ Topic
```

# Example Topics

```text
payments
payments-dlq
```

or

```text
fraud-events
fraud-events-dlq
```

# DLQ Event Structure

Store useful information.

```json
{
  "paymentId":"PAY100",
  "reason":"Invalid Amount",
  "originalTopic":"payments",
  "offset":456,
  "timestamp":"2026-06-01T10:30:00"
}
```

Operations teams can analyze these messages later.

# Sending to DLQ

```java
try{
    processPayment(payment);
}
catch(Exception ex){

    kafkaTemplate.send(
        "payments-dlq",
        payment.getPaymentId(),
        payment
    );
}
```

# Production DLQ Enrichment

Add metadata.

```java
record.headers().add(
  "error-type",
  ex.getClass()
    .getSimpleName()
    .getBytes()
);
```

Useful for root cause analysis.

# 6. Debugging Kafka Consumer Failures

When a consumer fails, follow a structured approach.

## Step 1 – Check Consumer Logs

Look for:

```text
NullPointerException
SerializationException
TimeoutException
```

Example:

```text
ERROR PaymentConsumer
Failed at offset 230
```

## Step 2 – Check Consumer Lag

Monitor:

```text
Current Offset
End Offset
Lag
```

Example:

```text
Current Offset = 500
End Offset = 700

Lag = 200
```

Large lag often indicates processing problems.

## Step 3 – Inspect Problem Message

Using console consumer:

```bash
kafka-console-consumer.sh \
--topic payments \
--from-beginning
```

Look for malformed events.

## Step 4 – Verify Schema

Common issue:

Producer sends:

```json
{
 "amount":100
}
```

Consumer expects:

```json
{
 "amount":"100"
}
```

Result:

```text
SerializationException
```

Schema Registry helps avoid these issues.

## Step 5 – Check Downstream Dependencies

Verify:

* Database connectivity
* REST API health
* Authentication
* Network routes

Sometimes Kafka is healthy but dependencies are not.

# Practical Lab – Poison Message Simulation

## Step 1 Create Topic

```bash
kafka-topics.sh \
--create \
--topic payments
```

## Step 2 Produce Valid Event

```json
{
 "paymentId":"P100",
 "amount":5000
}
```

## Step 3 Produce Invalid Event

```json
{
 "paymentId":"P101",
 "amount":-100
}
```

## Step 4 Consumer Validation

```java
if(payment.getAmount() < 0){
    throw new RuntimeException(
        "Negative Amount"
    );
}
```

Observe repeated failures.

## Step 5 Send to DLQ

```java
catch(Exception ex){

    producer.send(
      new ProducerRecord<>(
        "payments-dlq",
        payment
      )
    );
}
```

Verify:

```bash
kafka-console-consumer.sh \
--topic payments-dlq
```

Failed message should appear in DLQ.

# Real-World Payment Processing Example

A payment authorization service:

```text
Payment Received
       |
       V
Fraud Check
       |
       V
Bank API Call
       |
       X
Bank Offline
```

Strategy:

```text
Retry 1
Retry 2
Retry 3
Retry 4
DLQ
```

Benefits:

* No data loss
* No consumer blocking
* Easy troubleshooting
* Better customer experience

# Best Practices

### Do

* Use exponential backoff
* Monitor retries
* Track DLQ size
* Include metadata in DLQ records
* Alert on growing DLQ topics
* Separate transient and permanent failures

### Avoid

* Infinite retries
* Swallowing exceptions
* Deleting failed messages
* Ignoring consumer lag
* Sending all failures directly to DLQ

# Summary

Failure handling is a core requirement for production Kafka systems. Kafka itself guarantees durable storage, but applications must handle processing failures intelligently.

Key concepts covered:

1. Poison Messages

   * Messages that always fail processing
   * Can block consumers

2. Retry Strategies

   * Immediate Retry
   * Fixed Delay Retry
   * Exponential Backoff
   * Retry Topics Pattern

3. Dead Letter Queue (DLQ)

   * Stores unprocessable events
   * Preserves failed data
   * Enables investigation and recovery

Together, these patterns form the foundation of resilient Kafka-based event-driven systems and are essential for building the Day 5 capstone payment processing platform. 
