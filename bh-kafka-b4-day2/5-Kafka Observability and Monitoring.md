# Kafka Observability and Monitoring

In a distributed, event-driven architecture built on Apache Kafka, understanding the health, performance, and state of your cluster is critical. Because producers, brokers, and consumers operate asynchronously, traditional system-level monitoring (like CPU and memory usage) is insufficient. You need deep visibility into Kafka-specific signals to ensure messages are processed reliably and within acceptable SLAs.

This tutorial covers the core principles of Kafka observability, the key signals you must monitor, and how a complete monitoring stack (JMX Exporter + Kafka Exporter + Prometheus + Grafana) is designed and configured in our local cluster setup.

---

## 1. Core Observability Signals in Kafka

To understand the health of a Kafka cluster, you must monitor four key signals: **Consumer Lag**, **Throughput**, **Request Rates/Latencies**, and **Bytes In/Out**.

### A. Consumer Lag
<mark>**Consumer lag is the single most important metric for evaluating the health of an event-driven system.**</mark> It measures the backlog of unprocessed messages for a consumer group.

#### Concept
Lag is calculated as the difference between the latest message written to a partition (the **Log End Offset** or LEO) and the last message committed by a consumer group (the **Current Offset**).

```
Topic Partition Log:
┌───┬───┬───┬───┬───┬───┬───┬───┬───┬───┐
│ 0 │ 1 │ 2 │ 3 │ 4 │ 5 │ 6 │ 7 │ 8 │ 9 │  ... (offsets)
└───┴───┴───┴───┴───┴───┴───┴───┴───┴───┘
                  ▲                   ▲
                  │                   │
           Current Offset       Log End Offset (LEO)
           (Consumer Group)     (Producer Writes)
                  │                   │
                  └───── LAG = 5 ─────┘
```

*   **Log End Offset (LEO):** The offset of the next message to be written to the partition.
*   **Current Offset:** The offset of the last message successfully processed and committed by the consumer group.
*   **Lag:** The number of records the consumer is behind the producer.

#### Why It Matters
*   **SLA Breaches:** <mark>**High lag indicates that consumers cannot keep up with producers, leading to delays in downstream processing.**</mark>
*   **Resource Leaks / Outages:** If lag grows indefinitely, it indicates a stuck or failed consumer, or an under-provisioned consumer group.
*   **Data Loss Risk:** <mark>**If lag persists past the topic's retention period, messages will be deleted before they are consumed.**</mark>

#### Key Metrics
Kafka lag can be measured from two perspectives:
1.  **Broker-Side View (Aggregate):** Obtained by reading partition offsets and consumer group offsets from the broker.
    *   `kafka_consumergroup_lag`: Total lag in records for a specific consumer group, topic, and partition.
    *   `kafka_consumergroup_current_offset`: The current offset committed by the consumer group.
    *   `kafka_topic_partition_current_offset`: The latest offset (Log End Offset) of the topic partition.
2.  **Client-Side View (Consumer JVM):** Exposed by the consumer client application itself.
    *   `records-lag`: The current lag for a partition.
    *   `records-lag-max`: The maximum lag across all partitions assigned to this consumer instance (crucial for finding processing bottlenecks).

---

### B. Throughput
Throughput measures the volume of messages processed by the cluster over time.

#### Concept
Throughput is measured in two ways:
1.  **Message Rate:** The number of messages written or read per second.
2.  **Byte Rate:** The size of data written or read per second (see **Bytes In/Out** below).

#### Why It Matters
*   **Performance Baselines:** Allows you to establish normal usage patterns (e.g., peak hours vs. off-peak hours).
*   **Capacity Planning:** Helps identify when to scale the number of partitions or brokers.
*   **Anomaly Detection:** A sudden drop in message rate could indicate an upstream producer failure, while a sudden spike might suggest a retry loop or retry storm.

#### Key Metrics
*   `kafka_server_brokertopicmetrics_messagesinpersec` (OneMinuteRate): The rate of incoming messages per second, monitored globally for each broker and individually per topic.

---

### C. Request Rates & Latencies (Request In/Out)
Brokers interact with clients (producers and consumers) and other brokers via a custom TCP request-response protocol. Monitoring how fast these requests are processed is crucial for diagnosing network or disk bottlenecks.

#### The Request Lifecycle
When a request arrives at a broker, it goes through a pipeline before a response is returned:

```
                  ┌──────────────┐
                  │ Client       │
                  └──────┬───────┘
                         │ 1. TCP Request
                         ▼
             ┌───────────────────────┐
             │ Network Thread        │ (Reads request from socket)
             └───────────┬───────────┘
                         │ 2. Enqueue
                         ▼
             ┌───────────────────────┐
             │ Request Queue         │ (Holds requests waiting for processing)
             └───────────┬───────────┘
                         │ 3. Dequeue
                         ▼
             ┌───────────────────────┐
             │ I/O (Handler) Thread  │ (Processes request: writes/reads disk)
             └───────────┬───────────┘
                         │ 4. Enqueue
                         ▼
             ┌───────────────────────┐
             │ Response Queue        │ (Holds response waiting to be sent)
             └───────────┬───────────┘
                         │ 5. Dequeue
                         ▼
             ┌───────────────────────┐
             │ Network Thread        │ (Writes response back to socket)
             └───────────┬───────────┘
                         │ 6. TCP Response
                         ▼
                  ┌──────────────┐
                  │ Client       │
                  └──────────────┘
```

#### Why It Matters
<mark>**Slow processing in any of these stages causes requests to pile up, increasing latency and leading to client-side timeouts.**</mark>

#### Key Metrics
Kafka splits latency metrics into specific components to help you isolate bottlenecks:
*   `kafka_network_requestmetrics_requestspersec` (OneMinuteRate): The number of requests processed per second, categorized by request type (e.g., `Produce`, `FetchConsumer`, `FetchFollower`).
*   `kafka_network_requestmetrics_totaltimems` (Mean / 99thPercentile): The total time taken from the moment a request is received to when the response is sent back.
*   **Latency Breakdown Metrics (used for debugging high `TotalTimeMs`):**
    *   `LocalTimeMs`: Time spent in the I/O thread processing the request locally (e.g., writing to local disk). High local time indicates disk bottlenecks.
    *   `RemoteTimeMs`: Time spent waiting for replica acknowledgment (only applies to `Produce` requests with `acks=all`). High remote time indicates slow inter-broker replication or network issues.
    *   `ResponseQueueTimeMs`: Time spent in the response queue waiting for a network thread to send the response.

---

### D. Bytes In/Out
Bytes In/Out measures the raw network bandwidth consumed by the Kafka cluster.

#### Concept
Unlike messages-per-second, which measures record count, Bytes In/Out measures data volume (Megabytes per second).

#### Why It Matters
*   **Network Saturation:** <mark>**Network bandwidth is often the primary physical bottleneck in Kafka clusters.**</mark> 
*   **Replication Overhead:** Every message written to a leader partition must be replicated to follower partitions. If you have a replication factor of 3, your internal network usage will be significantly higher than your external input.
*   **Payload Size Correlation:** <mark>**Correlating `BytesInPerSec` with `MessagesInPerSec` allows you to calculate the average message size.**</mark> If average message sizes increase suddenly, it can lead to high memory usage, slower garbage collection, and replication lag.

#### Key Metrics
*   `kafka_server_brokertopicmetrics_bytesinpersec` (OneMinuteRate): Bytes written to the broker per second (producers + incoming replication).
*   `kafka_server_brokertopicmetrics_bytesoutpersec` (OneMinuteRate): Bytes read from the broker per second (consumers + outgoing replication).

---

## 2. Introduction to Monitoring Tools: Prometheus & Grafana

To collect and visualize these metrics, we use a standard, powerful observability stack consisting of **Prometheus** and **Grafana**.

```
   ┌──────────────────────────────────────────────────────────┐
   │                    KAFKA CLUSTER                         │
   │                                                          │
   │ ┌────────────────┐ ┌────────────────┐ ┌────────────────┐ │
   │ │  kafka1:7071   │ │  kafka2:7072   │ │  kafka3:7073   │ │
   │ │ (JMX Exporter) │ │ (JMX Exporter) │ │ (JMX Exporter) │ │
   │ └───────┬────────┘ └───────┬────────┘ └───────┬────────┘ │
   │         │                  │                  │          │
   │         └──────────────────┼──────────────────┘          │
   │                            │ Scrape JMX Metrics          │
   │                            ▼                             │
   │                 ┌────────────────────┐                   │
   │                 │   kafka-exporter   │◄───(Admin API)────┤
   │                 │   (Port: 9308)     │                   │
   │                 └──────────┬─────────┘                   │
   └────────────────────────────┼─────────────────────────────┘
                                │ Scrape Lag Metrics
                                ▼
                     ┌────────────────────┐
                     │ Prometheus (9090)  │ (TSDB / PromQL Engine)
                     └──────────┬─────────┘
                                │ Query
                                ▼
                     ┌────────────────────┐
                     │   Grafana (3000)   │ (Visual Dashboards)
                     └────────────────────┘
```

### Prometheus
Prometheus is a time-series database (TSDB) and monitoring system designed for scraping numeric metrics over HTTP.
*   **Pull Model:** <mark>**Instead of applications pushing metrics, Prometheus regularly pulls (scrapes) metrics from configured endpoints.**</mark>
*   **PromQL:** Prometheus features a flexible query language (PromQL) to calculate rates, averages, and percentiles on the fly.
*   **Alerting:** You can define rules in Prometheus to trigger alerts when metrics cross thresholds (e.g., if consumer lag > 10,000 for 5 minutes).

### Grafana
Grafana is an open-source visualization and analytics platform.
*   **Data Sources:** It connects to databases like Prometheus, executing queries behind the scenes.
*   **Dashboards:** It organizes metrics into visual panels (graphs, heatmaps, gauges, single-stat values).
*   **Alerting:** Provides visual and notification-based alerting (Slack, email, Webhooks).

---

## 3. Deep Dive: Monitoring Setup in `kafka-cluster`

Let's examine how the monitoring stack is wired together in the `kafka-cluster/docker-compose.yml` file.

### Step A: Broker JMX Exporting (via JMX Exporter agent)
<mark>**Kafka is built in Scala and Java, meaning all internal statistics are generated as JMX MBeans (Java Management Extensions).**</mark> Prometheus cannot read JMX natively. To bridge this gap, we run the **Prometheus JMX Exporter** as a Java agent inside each broker's JVM.

In [docker-compose.yml](file:///Users/vinod/Desktop/BH-EDA-With-Kafka-B4/bh-kafka-b4-day2/kafka-cluster/docker-compose.yml):
```yaml
  kafka1:
    image: confluentinc/cp-kafka:7.5.0
    ports:
      - "9092:9092"
      - "7071:7071" # Prometheus JMX scrape port
    volumes:
      - ./monitoring/jmx:/opt/jmx-exporter
      ...
    environment:
      ...
      KAFKA_OPTS: "-javaagent:/opt/jmx-exporter/jmx_prometheus_javaagent-0.20.0.jar=7071:/opt/jmx-exporter/kafka-jmx.yaml"
```
*   **JVM Agent (`-javaagent`):** Registers the JMX Exporter JAR file to run side-by-side with Kafka.
*   **Configuration (`kafka-jmx.yaml`):** The JMX Exporter listens on port `7071` (or `7072` / `7073` for the other brokers) and translates raw JMX MBeans into Prometheus-friendly format using regular expression rules defined in [kafka-jmx.yaml](file:///Users/vinod/Desktop/BH-EDA-With-Kafka-B4/bh-kafka-b4-day2/kafka-cluster/monitoring/jmx/kafka-jmx.yaml).

*Example Rule in `kafka-jmx.yaml`:*
```yaml
- pattern: "kafka.server<type=BrokerTopicMetrics, name=BytesInPerSec, topic=(.+)><>OneMinuteRate"
  name: kafka_server_brokertopicmetrics_bytesinpersec
  type: GAUGE
  labels:
    topic: "$1"
```
This regex captures the raw MBean `kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec,topic=orders` and transforms it into the metric:
```
kafka_server_brokertopicmetrics_bytesinpersec{topic="orders"} 104520.0
```

---

### Step B: Consumer Lag Exporting (via `kafka-exporter`)
Broker JMX is excellent for broker-level metrics, but it is not optimized for calculating consumer lag across a large number of dynamic consumer groups. <mark>**To capture consumer lag, we use Kafka Exporter, a lightweight Go service that queries the cluster directly.**</mark>

In [docker-compose.yml](file:///Users/vinod/Desktop/BH-EDA-With-Kafka-B4/bh-kafka-b4-day2/kafka-cluster/docker-compose.yml):
```yaml
  kafka-exporter:
    image: danielqsj/kafka-exporter:latest
    container_name: kafka-exporter
    ports:
      - "9308:9308"
    command:
      - "--kafka.server=kafka1:29092"
      - "--kafka.server=kafka2:29093"
      - "--kafka.server=kafka3:29094"
```
*   **Mechanism:** `kafka-exporter` connects to the brokers on the internal network (`29092/29093/29094`) using the standard Kafka protocol. It queries broker metadata to find the log end offsets and queries consumer group coordinators to fetch consumer offsets.
*   **Output:** It exposes calculated lag metrics over HTTP on port `9308` under `/metrics`.

---

### Step C: Prometheus Scraping Config
Prometheus must be instructed to scrape all of these exported endpoints.

In [prometheus.yaml](file:///Users/vinod/Desktop/BH-EDA-With-Kafka-B4/bh-kafka-b4-day2/kafka-cluster/prometheus/prometheus.yaml):
```yaml
scrape_configs:
  # Broker 1 JMX
  - job_name: "kafka-broker-1"
    static_configs:
      - targets: ["kafka1:7071"]
        labels:
          broker_id: "1"
          env: "local"

  # Broker 2 JMX
  - job_name: "kafka-broker-2"
    static_configs:
      - targets: ["kafka2:7072"]
        labels:
          broker_id: "2"
          env: "local"

  # Broker 3 JMX
  - job_name: "kafka-broker-3"
    static_configs:
      - targets: ["kafka3:7073"]
        labels:
          broker_id: "3"
          env: "local"

  # Consumer Lag Exporter
  - job_name: "kafka-exporter"
    static_configs:
      - targets: ["kafka-exporter:9308"]
        labels:
          env: "local"
```
*   **Scrape Interval:** Globally configured to scrape every 15 seconds. Every 15 seconds, Prometheus sends HTTP requests to `kafka1:7071`, `kafka2:7072`, `kafka3:7073`, and `kafka-exporter:9308` to fetch and store their latest metric states.

---

### Step D: Grafana Provisioning & Dashboards
To make the metrics immediately useful without manual UI configuration, Grafana is set up with **auto-provisioning**.

In [docker-compose.yml](file:///Users/vinod/Desktop/BH-EDA-With-Kafka-B4/bh-kafka-b4-day2/kafka-cluster/docker-compose.yml):
```yaml
  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - "grafana_vol:/var/lib/grafana"
      - ./grafana/provisioning:/etc/grafana/provisioning
      - ./grafana/dashboards:/var/lib/grafana/dashboards
```

1.  **Datasource Provisioning:**
    The file [prometheus.yml](file:///Users/vinod/Desktop/BH-EDA-With-Kafka-B4/bh-kafka-b4-day2/kafka-cluster/grafana/provisioning/datasources/prometheus.yml) automatically registers the Prometheus server (`http://prometheus:9090`) as the default datasource.
2.  **Dashboard Provisioning:**
    The configuration file [kafka.yml](file:///Users/vinod/Desktop/BH-EDA-With-Kafka-B4/bh-kafka-b4-day2/kafka-cluster/grafana/provisioning/dashboards/kafka.yml) tells Grafana to read all JSON files located in `/var/lib/grafana/dashboards`.
3.  **Available Dashboards:**
    Three pre-configured dashboards are mounted from the `./grafana/dashboards` folder:
    *   **Kafka Broker Dashboard (`kafka-broker.json`):** Tracks cluster health, JVM memory/GC, active controller count (should be exactly 1), partition counts, and replica statistics.
    *   **Kafka Topic Dashboard (`kafka-topic.json`):** Tracks topic-specific metrics like message write rate (`MessagesInPerSec`) and bytes in/out rate (`BytesInPerSec` / `BytesOutPerSec`).
    *   **Kafka Consumer Lag Dashboard (`kafka-consumer-lag.json`):** Displays consumer group names, lag values, consumption offsets, and partition end offsets, making it easy to identify lagging consumers.

---

## 4. Running the Monitoring Stack

To start the cluster alongside the monitoring tools, run the following commands:

```bash
# Navigate to the kafka-cluster folder
cd kafka-cluster

# Start the cluster in detached mode
./start-on-macos.sh
# (or docker compose up -d)
```

Once running, you can access the following web interfaces:

*   **Prometheus Web Console:** `http://localhost:9090`
    *   Useful for testing PromQL queries directly (e.g., type `kafka_consumergroup_lag` in the expression bar and click "Execute").
*   **Grafana UI:** `http://localhost:3000`
    *   Log in using the default credentials: Username `admin`, Password `admin`.
    *   Navigate to **Dashboards** to view the preloaded dashboards in the "Kafka" folder.
*   **Kafka UI:** `http://localhost:8080`
    *   A web-based administration console to view topics, messages, brokers, and consumer groups.
