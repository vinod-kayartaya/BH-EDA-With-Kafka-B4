# Event-Driven Architecture with Apache Kafka — Day 1 Training Workspace

Welcome to the training workspace for **Day 1: Introduction to EDA and Apache Kafka**. This directory contains all the tutorials, hands-on laboratories, and visualization tools designed to introduce the core principles of Event-Driven Architectures and Kafka.

---

## Trainer Info
*   **Trainer:** Vinod Kumar Kayartaya
*   **Email:** [vinod@vinod.co](mailto:vinod@vinod.co)
*   **Website:** [https://vinod.co](https://vinod.co)

---

## Workspace Map

This workspace is structured into tutorials, lab configurations, and interactive visual aids. Click the links below to open each resource:

### 1. Conceptual Tutorials
*   **[1-Introduction.md](1-Introduction.md):** An overview of the transition from synchronous Request-Response APIs (REST/gRPC) to asynchronous Event-Driven Architectures (EDA). Discusses event definitions, temporal/logical decoupling, backpressure, and scenarios where Kafka shines (and where it does not).
*   **[2-Apache Kafka.md](2-Apache%20Kafka.md):** Details what Apache Kafka is, its history and timeline from its creation at LinkedIn to the transition to KRaft mode (ZooKeeper deprecation), and breaks down cluster concepts (Brokers, Producers, Consumers, Consumer Groups, and Partition Replication).
*   **[3-Topics Partitions and Replication.md](3-Topics%20Partitions%20and%20Replication.md):** Explores topics and partitions storage layout, partition key hash-routing, message ordering guarantees, offset tracking metrics (LEO, High Watermark, Committed Offsets, and Consumer Lag), replica topologies (leader/follower), ISR sets, and `acks` and `min.insync.replicas` durability levels.
*   **[4-Kafka Message Types.md](4-Kafka%20Message%20Types.md):** Explores the anatomy of a Kafka record (Key, Value, Timestamp, Headers, Offset) and compares common message serialization formats (Plain String, JSON, Apache Avro with Schema Registry, and Protocol Buffers).

### 2. Interactive Sandbox Visualization
*   **[visualizations/v1.html](visualizations/v1.html):** A high-fidelity, interactive HTML5 web visualizer. Open this file in your browser to inspect:
    *   Message packets streaming from producers along curved SVG fiber paths.
    *   **KRaft Resiliency Failover:** Click a broker to switch it "Offline". Watch the KRaft control plane trigger a metadata consensus update and re-elect a new partition leader.
    *   **Consumer Group Rebalancing:** Pause group readers to see partition assignments reallocate automatically.
    *   **Consumer Lag & Commit Logs:** Observe Log End Offset (LEO) and High Watermark (HW) increments, pause consumers to build lag, and watch the live Grafana-style metrics chart.

### 3. Practical Labs
*   **[lab-1 (Multi-Broker Setup)](lab-1/README.md):** Configuration files (`docker-compose.yaml`) to spin up a local 3-broker KRaft cluster and the Provectus Kafka UI on your machine.
*   **[lab-2 (Kafka CLI Operations)](lab-2/README.md):** Steps to download Kafka binaries, set PATH variables (for macOS and Windows), and run CLI scripts to administer topics, produce key-partitioned messages, and troubleshoot consumer lag.

---

## Prerequisites for Day 1 Labs

To complete the labs, ensure your developer machine has the following tools installed:
1.  **Docker & Docker Compose** (for spinning up the local cluster in `lab-1`).
2.  **Java Runtime Environment (JRE/JDK 11+)** (required to run the native CLI scripts in `lab-2`).
3.  Any modern web browser (to run the visualizer in `visualizations/v1.html`).
