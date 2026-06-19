# Kafka Streams Reference Guide

This guide provides a comprehensive reference on Kafka Streams, focusing on key API abstractions, architecture, windowing, and how it solves complex stream processing challenges that are difficult or impossible to solve with the regular Consumer API.

---

## 1. Introduction: What is Kafka Streams?

### Definition
Kafka Streams is a client library for building applications and microservices, where the input and output data are stored in Kafka clusters. It combines the simplicity of writing and deploying standard Java/Scala applications on the client side with the benefits of Kafka's server-side clustering technology.

### Lightweight & Serverless
Unlike other stream processing engines (e.g., Apache Flink, Apache Spark Streaming), Kafka Streams **does not require a separate processing cluster**. It is a library embedded directly within your application code. You run it as a standard Java process, and it scales horizontally by leveraging Kafka's consumer group protocol.

```text
+-----------------------------------------------------------+
|                   STREAMING APPLICATION                   |
|  +--------------------+           +--------------------+  |
|  |  Application Code  | <=======> | Kafka Streams Lib  |  |
|  +--------------------+           +--------------------+  |
|                                     (RocksDB Local State)|
+-----------------------------------------------------------+
                             ^
                             | (Read / Write Bytes)
                             v
                 +-----------------------+
                 |  KAFKA BROKER CLUSTER |
                 +-----------------------+
```

---

## 2. Consumer API vs. Kafka Streams API

In event-driven architectures, developers often wonder when to use a standard Kafka Consumer vs. the Kafka Streams API. While consumers are excellent for simple read-and-write workflows (e.g., reading a message and saving it to a database), they struggle when performing stateful operations.

Here is a direct comparison of the problems solved by Kafka Streams:

| Capability | Standard Kafka Consumer | Kafka Streams API |
| :--- | :--- | :--- |
| **State Storage** | None. State must be in-memory (lost on crash) or external (high network latency). | **Built-in Local State Stores** (RocksDB). Disk-persistent, sub-millisecond local reads/writes. |
| **State Fault Tolerance**| Manual. Recovery requires querying databases or replaying the topic from offset zero. | **Automated Recovery**. State changes are backed up to a Kafka changelog topic and replayed on startup. |
| **Stream-Table Joins** | Manual coding of caches and partition checks. Highly error-prone. | **Native Joins** (`KStream` to `KTable` / `GlobalKTable`) using key co-partitioning. |
| **Windowing** | Requires manual queues, timers, and expiration logs to calculate intervals. | **Native Windows** (Tumbling, Hopping, Sliding, Session) with built-in time-based expiration. |
| **Out-of-Order Events** | Hard to handle. Late events overwrite current state in database tables. | **Grace Periods**. Late-arriving events are incorporated into state windows if they fall within grace limits. |
| **Exactly-Once (EOS)** | Requires complex transactional code coordinating producer, consumer, and offsets. | **Declarative EOS**. Enabled via a single config line (`processing.guarantee=exactly_once_v2`). |

---

## 3. Deep Dive: Problems Solved by Kafka Streams

### Problem 1: Stateful Processing and Database Latency
Suppose you need to calculate a running total of payment amounts for each customer. 

* **The Consumer API Approach:**
  For every event received, the consumer must query a database (e.g., Redis or PostgreSQL) to fetch the current total, add the new amount, and write the updated total back. This introduces **network latency and round-trip bottlenecks** for every event, dropping throughput from 100,000+ messages/sec to a few thousand.
* **The Kafka Streams Approach:**
  Kafka Streams uses **RocksDB** (an embedded, high-performance key-value store written in C++) as a local state store. Reads and writes happen directly in-memory or on local SSDs without network round-trips. 

### Problem 2: State Durability and Rebalance Recovery
If a consumer instance dies, Kafka reassigns its partitions to another running instance. How does the new instance recover the state for the reassigned partitions?

* **The Consumer API Approach:**
  The new consumer must rebuild its cache by either scanning the external database (which might be out-of-sync or slow) or re-reading the entire Kafka partition from offset zero.
* **The Kafka Streams Approach:**
  Kafka Streams automatically creates a **changelog topic** in Kafka for each state store. Every write to the local RocksDB store also writes a compact change event to the changelog. If an instance dies and its partitions are reassigned, the new instance reads the changelog topic to reconstruct the RocksDB state locally, ensuring quick recovery.

### Problem 3: Windowing and Time Tracking
Fintech applications often need to group events by time (e.g., "detect if a user makes more than 5 payments within a 10-second window").

* **The Consumer API Approach:**
  You must build a manual memory cache of events, run background scheduler threads to check when windows expire, evict old messages, and handle time drift between servers.
* **The Kafka Streams Approach:**
  Kafka Streams manages windows natively. It extracts timestamps directly from the message metadata (**Event Time**) rather than using the application server clock. It tracks multiple active windows and allows you to specify a **grace period** to accommodate delayed messages.

### Problem 4: Stream-Table Duality
Event streams represent history (inserts), while database tables represent the current state (upserts). Combining these two structures is crucial for data enrichment (e.g., taking an incoming transaction event stream and joining it with a user profile table).

* **The Consumer API Approach:**
  You must write code to query database APIs or maintain custom caches of the database.
* **The Kafka Streams Approach:**
  Kafka Streams introduces native abstractions that represent this duality:
  * **`KStream` (Stream):** Represents a stream of records. Every record is an insert.
  * **`KTable` (Table):** Represents a changelog stream. Every record with the same key is an update (upsert) to the previous record.
  * **`GlobalKTable`:** Like a `KTable`, but it populates its state store with data from *all* partitions of the input topic on *every* running instance, enabling non-keyed lookup joins.

```text
KStream (Events):  [Key: A, Val: 1] ---> [Key: A, Val: 2] ---> [Key: B, Val: 3]
                   (Three independent events occurred)

KTable (State):    [Key: A, Val: 1] ---> [Key: A, Val: 2] ---> [Key: B, Val: 3]
                   (Result: Key A is updated to '2', Key B is set to '3')
```

---

## 4. Windowing Types in Kafka Streams

Kafka Streams provides four distinct models for grouping state over time:

### 1. Tumbling Windows
* **Definition:** Fixed-size, non-overlapping, contiguous time intervals.
* **Use Case:** Hourly transaction volume reports.
* **Example:** Windows are `[12:00 - 12:05]`, `[12:05 - 12:10]`, `[12:10 - 12:15]`.

### 2. Hopping Windows
* **Definition:** Fixed-size, overlapping time intervals. Defined by a window size and an advance interval.
* **Use Case:** A 5-minute moving average calculated every 1 minute.
* **Example:** `[12:00 - 12:05]`, `[12:01 - 12:06]`, `[12:02 - 12:07]`.

### 3. Sliding Windows
* **Definition:** Windows that are aligned to the timestamps of incoming records. A window is created dynamically whenever a record arrives, looking backward or forward by a set duration.
* **Use Case:** Checking if two payment events occur within 30 seconds of each other (fraud check).

### 4. Session Windows
* **Definition:** Dynamic, gap-based windows. A window remains open as long as events keep arriving. If no event arrives within the "inactivity gap" duration, the window closes.
* **Use Case:** User web session tracking.

---

## 5. Maven Configuration

To use Kafka Streams in a Maven project, add the following to your `pom.xml`:

```xml
<properties>
    <kafka.version>4.1.0</kafka.version>
</properties>

<dependencies>
    <!-- Kafka Streams Library -->
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-streams</artifactId>
        <version>${kafka.version}</version>
    </dependency>

    <!-- Jackson for JSON Serialization inside Serdes -->
    <dependency>
        <groupId>com.fasterxml.jackson.core</groupId>
        <artifactId>jackson-databind</artifactId>
        <version>2.20.0</version>
    </dependency>
</dependencies>
```

---

## 6. Practical Programming Patterns & Code Examples

Here is how to build a stateful stream processing application using the `simple-kafka-stream-demo` structure.

### 1. The Domain Model (`Payment.java`)
```java
package co.vinod.model;

public class Payment {
    private long tx_id;
    private String paymentType; // e.g., "CARD", "UPI", "NETBANK"
    private double amount;
    private String cust_id;
    private String remarks;

    public Payment() {}

    // Getters and Setters
    public long getTx_id() { return tx_id; }
    public void setTx_id(long tx_id) { this.tx_id = tx_id; }
    public String getPaymentType() { return paymentType; }
    public void setPaymentType(String paymentType) { this.paymentType = paymentType; }
    public double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }
    public String getCust_id() { return cust_id; }
    public void setCust_id(String cust_id) { this.cust_id = cust_id; }
    public String getRemarks() { return remarks; }
    public void setRemarks(String remarks) { this.remarks = remarks; }
}
```

### 2. Stream Topology Configuration (`PaymentSummaryStream.java`)
This application listens to the `payments` topic, groups messages by their payment type, aggregates event counts in 10-second tumbling windows, and logs the results.

```java
package co.vinod.streams;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import co.vinod.model.Payment;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class PaymentSummaryStream {

    public static void main(String[] args) {
        Properties props = new Properties();
        // unique identifier for the streams application (determines consumer group and state directories)
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "payment-summary-demo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        StreamsBuilder builder = new StreamsBuilder();
        Serde<Payment> paymentSerde = getPaymentSerde();

        // 1. Consume events from Kafka topic
        KStream<String, Payment> payments = builder.stream(
                "payments",
                Consumed.with(Serdes.String(), paymentSerde)
        );

        // 2. Stateful aggregation: Group, Window, and Count
        payments
                .groupBy(
                        (key, value) -> value.getPaymentType(), // Group by field
                        Grouped.with(Serdes.String(), paymentSerde)
                )
                .windowedBy(
                        TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(10)) // 10-sec tumbling window
                )
                .count() // Aggregate counts locally in RocksDB
                .toStream()
                .foreach((window, count) -> {
                    // Log the window boundary and aggregate calculation
                    System.out.printf(
                            "Window [%s - %s] -> %s = %d%n",
                            window.window().startTime(),
                            window.window().endTime(),
                            window.key(),
                            count
                    );
                });

        // Build the topology and start the engine
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        CountDownLatch latch = new CountDownLatch(1);

        // Shutdown hook to close local state stores and release threads
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            System.out.println("Streams Topology started. Press Ctrl+C to exit.");
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

    // Helper method to create a custom Serde for JSON serialization
    private static Serde<Payment> getPaymentSerde() {
        ObjectMapper objectMapper = new ObjectMapper();
        
        Serializer<Payment> serializer = (topic, data) -> {
            try {
                return objectMapper.writeValueAsBytes(data);
            } catch (Exception e) {
                throw new RuntimeException("Error writing bytes", e);
            }
        };
        
        Deserializer<Payment> deserializer = (topic, data) -> {
            try {
                return objectMapper.readValue(data, Payment.class);
            } catch (Exception e) {
                throw new RuntimeException("Error reading bytes", e);
            }
        };

        return Serdes.serdeFrom(serializer, deserializer);
    }
}
```

---

## 7. Common Pitfalls & Operational Troubleshooting

### 1. RocksDB Local Disk Space Bloat
* **Symptom:** Local disk space on your application server runs out quickly.
* **Cause:** By default, RocksDB saves state files locally. If your window retention time is very long or your keys have high cardinality, local files will accumulate.
* **Resolution:** 
  * Tune window retention durations (`.until(Duration)` or `.retentionTime()`).
  * Customize RocksDB settings by implementing a `RocksDBConfigSetter` to compress stored databases.

### 2. Thread Starvation / Slow Processing
* **Symptom:** Streams application falls behind (high lag) and consumes CPU at 100%.
* **Cause:** The number of stream threads is too low relative to your topic's partitions.
* **Resolution:** Increase the number of processing threads per instance using:
  ```properties
  props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4); // Default is 1
  ```

### 3. Out-of-Memory (OOM) Errors (Off-Heap Memory)
* **Symptom:** The JVM crashes with native memory errors, or the operating system terminates your process.
* **Cause:** RocksDB allocates memory **off-heap** (outside the JVM GC-managed memory). If many partitions are processed on a single machine, RocksDB cache allocations might exceed host limits.
* **Resolution:** Limit RocksDB block cache size programmatically or decrease `NUM_STREAM_THREADS_CONFIG` to process fewer active stores on a single instance.

### 4. Group Rebalances and Stream Disruptions
* **Symptom:** Frequent rebalances interrupt stream processing and cause state stores to reinitialize.
* **Cause:** Stream processing loops take longer to run than the consumer heartbeat timeout because of heavy calculations or blocking calls inside stream transformations.
* **Resolution:** 
  * **Never perform blocking I/O calls** (like HTTP requests or database calls) inside stream operations (`map`, `foreach`, `transform`).
  * If blocking calls are unavoidable, increase `max.poll.interval.ms` configuration so the coordinator doesn't mark the stream instance as dead.
