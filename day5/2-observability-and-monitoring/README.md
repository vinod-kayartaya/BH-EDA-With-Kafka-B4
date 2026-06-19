# Business Scenario

Imagine you are operating a Real-Time Payment Processing Platform.

The system handles:

* 50,000 payment transactions per minute
* Fraud detection using Kafka Streams
* PostgreSQL database updates via Kafka Connect
* Multiple producer and consumer applications

One morning customer complaints begin arriving:

> "Payments are delayed by several minutes."

How do you determine:

* Is Kafka overloaded?
* Is a broker unhealthy?
* Is a topic receiving too much traffic?
* Are consumers unable to keep up?

Observability provides the answers.

Without observability:

* Problems are discovered by customers.

With observability:

* Problems are discovered before customers notice them.

---

# Understanding Observability in Kafka

Observability answers three questions:

### What is happening?

Metrics

Examples:

* Messages per second
* Consumer lag
* Network utilization

### Why is it happening?

Logs

Examples:

* Broker errors
* Authentication failures
* Rebalance events

### Where is it happening?

Tracing and correlation

Examples:

* Which service is causing delays
* Which partition is overloaded

In Kafka, metrics are the first line of defense.

---

# Broker Metrics

## What is a Broker?

A broker is a Kafka server responsible for:

* Storing partitions
* Serving producer requests
* Serving consumer requests
* Replicating data

A cluster may contain:

```text
Broker-1
Broker-2
Broker-3
```

Every broker continuously exposes operational metrics.

---

# Why Broker Metrics Matter

Broker metrics help answer:

* Is Kafka healthy?
* Is a broker overloaded?
* Is hardware sufficient?
* Are producers or consumers causing stress?

---

# Critical Broker Metrics

## 1. Bytes In Per Second

Measures:

```text
Data entering broker
```

Example:

```text
100 MB/sec
```

High values indicate:

* Heavy producer activity

Low values may indicate:

* Producer failure
* Traffic drop

---

## 2. Bytes Out Per Second

Measures:

```text
Data leaving broker
```

Example:

```text
250 MB/sec
```

High values indicate:

* Active consumers

Possible issue:

```text
Bytes In = 50 MB/sec
Bytes Out = 500 MB/sec
```

This could indicate excessive consumer fetches.

---

## 3. Messages In Per Second

Measures:

```text
Number of records received
```

Example:

```text
100,000 messages/sec
```

Useful for:

* Capacity planning
* Growth forecasting

---

## 4. Request Rate

Measures:

```text
Requests handled by broker
```

Examples:

```text
Produce requests
Fetch requests
Metadata requests
```

Sudden spikes may indicate:

* Application issues
* Infinite retry loops

---

## 5. Request Latency

Measures:

```text
Time taken to process requests
```

Healthy:

```text
5 ms
10 ms
15 ms
```

Concerning:

```text
500 ms
1 second
```

Possible causes:

* Slow disk
* CPU saturation
* Network issues

---

## 6. Active Controller Count

Exactly one broker should be controller.

Expected:

```text
1
```

Problem:

```text
0
```

or

```text
2
```

Could indicate:

* Cluster instability
* Election problems

---

## 7. Under Replicated Partitions (URP)

One of the most important metrics.

Expected:

```text
0
```

Example:

```text
Partition 0:
Leader = Broker-1
Replica = Broker-2
Replica = Broker-3

Broker-3 falls behind.
```

Result:

```text
URP = 1
```

Possible causes:

* Network issues
* Disk issues
* Broker overload

---

## 8. Offline Partitions Count

Expected:

```text
0
```

If greater than zero:

```text
Data unavailable
```

This is usually a critical alert.

---

## 9. Network Throughput

Measures:

```text
Incoming traffic
Outgoing traffic
```

Useful for identifying:

* Bandwidth saturation
* Replication bottlenecks

---

## 10. JVM Metrics

Kafka runs on Java.

Important JVM metrics:

### Heap Usage

```text
Memory utilization
```

### GC Time

```text
Garbage collection duration
```

High GC often causes:

* Latency spikes
* Consumer lag

---

# Broker Health Dashboard

Typical Grafana dashboard:

```text
Broker CPU
Broker Memory
Bytes In
Bytes Out
Request Latency
URP Count
Offline Partitions
```

At a glance you can determine:

```text
Healthy
Warning
Critical
```

---

# Topic Metrics

## Why Topic Metrics Matter

Broker metrics show cluster health.

Topic metrics show business workload health.

Example:

```text
payments
fraud-alerts
customer-notifications
```

Each topic behaves differently.

---

# Important Topic Metrics

## 1. Incoming Message Rate

Measures:

```text
Messages arriving per second
```

Example:

```text
payments

50,000 messages/sec
```

Useful for:

* Traffic monitoring
* Capacity planning

---

## 2. Topic Throughput

Measures:

```text
Bytes per second
```

Example:

```text
200 MB/sec
```

Helps determine:

* Storage growth
* Network utilization

---

## 3. Partition Distribution

Healthy:

```text
Partition 0 = 10%
Partition 1 = 12%
Partition 2 = 11%
Partition 3 = 9%
```

Unhealthy:

```text
Partition 0 = 90%
Others = 10%
```

This indicates:

```text
Hot partition
```

Common cause:

Poor key selection.

---

# Example

Bad key:

```java
producer.send(
    new ProducerRecord<>(
        "payments",
        "india",
        payment
    )
);
```

Every message goes to one partition.

---

Better:

```java
paymentId
customerId
transactionId
```

These distribute more evenly.

---

## 4. Retention Usage

Measures:

```text
Storage consumed by topic
```

Example:

```text
payments = 1.5 TB
```

Useful for:

* Capacity planning
* Storage forecasting

---

## 5. Log Size Growth

Monitors:

```text
How quickly topic size increases
```

Example:

```text
10 GB/hour
```

Can predict:

```text
Disk exhaustion
```

---

## 6. Replication Health

Measures:

```text
Replica synchronization
```

Important for:

* Fault tolerance
* Disaster recovery

---

## Topic-Level Alerts

Common alerts:

### Message Rate Drop

Expected:

```text
50,000/sec
```

Observed:

```text
100/sec
```

Possible cause:

Producer failure

---

### Throughput Spike

Expected:

```text
100 MB/sec
```

Observed:

```text
500 MB/sec
```

Possible cause:

Traffic surge

---

### Hot Partition

Expected:

```text
Even distribution
```

Observed:

```text
One partition overloaded
```

Possible cause:

Bad partition key

---

# Consumer Lag Monitoring

## What is Consumer Lag?

Consumer lag measures:

```text
How far behind a consumer is.
```

Formula:

```text
Lag = Latest Offset - Consumer Offset
```

Example:

```text
Latest Offset = 1000
Consumer Offset = 950
```

Lag:

```text
50
```

Consumer must process:

```text
50 messages
```

before catching up.

---

# Why Consumer Lag Matters

Consumer lag directly affects:

* Processing latency
* User experience
* Business SLAs

---

# Payment System Example

Producer:

```text
1000 payments/sec
```

Consumer:

```text
1000 payments/sec
```

Lag:

```text
0
```

Healthy.

---

Producer:

```text
1000 payments/sec
```

Consumer:

```text
500 payments/sec
```

Lag grows continuously.

```text
100
200
500
1000
5000
10000
```

Soon the system becomes unusable.

---

# Types of Lag

## Temporary Lag

Occurs during:

```text
Deployment
Restart
Rebalance
```

Usually acceptable.

---

## Sustained Lag

Occurs when:

```text
Consumer permanently slower
```

This is dangerous.

---

## Exploding Lag

Lag grows rapidly.

Example:

```text
0
100
500
5000
50000
```

Immediate investigation required.

---

# Common Causes of Consumer Lag

## Slow Consumer Logic

Example:

```java
Thread.sleep(1000);
```

Every record takes:

```text
1 second
```

Processing becomes slow.

---

## Insufficient Consumers

Example:

```text
8 partitions
1 consumer
```

Consumer becomes overloaded.

Solution:

```text
Increase consumers
```

---

## Database Bottlenecks

Example:

```text
Kafka -> Consumer -> PostgreSQL
```

Database becomes slow.

Consumer lag increases.

---

## Rebalancing

During rebalance:

```text
Consumers stop processing
```

Temporary lag appears.

---

## Network Problems

Slow communication:

```text
Consumer <-> Broker
```

Can increase lag significantly.

---

# Monitoring Consumer Lag

Most organizations monitor:

### Current Lag

```text
Current backlog
```

### Maximum Lag

```text
Worst partition
```

### Lag Trend

```text
Increasing
Decreasing
Stable
```

Trend is often more important than absolute value.

---

# Example Lag Dashboard

```text
Consumer Group:
payment-processor-group

Current Lag:
1,200

Maximum Lag:
500

Trend:
Increasing
```

Interpretation:

```text
Consumers are falling behind.
```

Action required.

---

# Lag Investigation Workflow

Step 1

Check:

```text
Consumer Lag
```

Step 2

Check:

```text
Consumer CPU
```

Step 3

Check:

```text
Database latency
```

Step 4

Check:

```text
Broker latency
```

Step 5

Check:

```text
Partition distribution
```

Root cause is usually found within these five steps.

---

# Hands-On Exercise

Using Prometheus and Grafana:

### Task 1

Observe:

```text
Bytes In
Bytes Out
Messages In
```

while generating payment events.

---

### Task 2

Create artificial load.

Observe:

```text
Request Latency
URP
Network Throughput
```

---

### Task 3

Slow down consumer processing.

Observe:

```text
Consumer Lag
Lag Trend
```

---

# Key Takeaways

* Broker metrics reveal Kafka cluster health.
* Topic metrics reveal workload behavior and scalability issues.
* Consumer lag is one of the most important operational metrics in Kafka.
* Monitoring trends is often more valuable than monitoring individual values.
* Most production Kafka incidents can be detected early through broker metrics, topic metrics, and consumer lag monitoring.
