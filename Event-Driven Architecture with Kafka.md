# **Event-Driven Architecture with Kafka – Fintech Focus**

**Duration:** 3–5 days (4–5 hours/day)
**Delivery Mode:** On-Premise (Bengaluru)
**Batch Size:** 20–25 participants
**Approach:** Hands-on, Lab-heavy, Real-world fintech scenarios

## **Day 1 – Fundamentals & Core Concepts**

**Objective:** Build a strong conceptual foundation in EDA and Kafka before diving into fintech use cases.

1. **Introduction to Event-Driven Architecture**

   - What is EDA and why it matters in fintech
   - Components of EDA: Event Producers, Event Routers, Event Consumers
   - Types of Event Patterns: Event Notification, Event-Carried State Transfer, Event Sourcing
   - Advantages & Challenges in financial services (real-time fraud detection, payment processing, etc.)

2. **EDA vs. Traditional Architectures**

   - Comparison with request-response systems
   - Latency, scalability, fault tolerance considerations

3. **Introduction to Apache Kafka**

   - Kafka architecture: Brokers, Topics, Partitions, Offsets, Zookeeper/KRaft
   - Kafka use cases in payments, trade settlements, and credit risk systems
   - Message ordering, durability, and delivery guarantees

4. **Kafka Setup & First Hands-on**

   - On-premise Kafka installation (local/lab setup)
   - Creating topics, producing and consuming messages
   - CLI-based producer-consumer demo

**Lab 1:**

- Create a Kafka topic for “TransactionEvents” and publish sample JSON events
- Write a basic producer and consumer using Kafka CLI

## **Day 2 – Kafka Programming & Messaging Patterns**

**Objective:** Learn Kafka APIs and core messaging patterns for fintech workflows.

1. **Kafka Producer API**

   - Sending events asynchronously & synchronously
   - Key-based partitioning for transaction routing
   - Serialization/Deserialization (JSON, Avro, Protobuf in financial data)

2. **Kafka Consumer API**

   - Consumer groups, rebalancing, offset management
   - Pull-based consumption and backpressure handling

3. **Event Streaming Patterns**

   - Publish/Subscribe in fintech (real-time alerts)
   - Event Sourcing (transaction history)
   - CQRS (separating reads/writes for risk reports)

4. **Practical Labs**

   - Build a Java/Python producer to send transaction events
   - Build a consumer to read, transform, and store events in a database

**Lab 2:**

- Implement a producer that sends simulated payment transactions every second
- Implement a consumer that listens to the topic and flags transactions > ₹10,00,000 as “High Value”

## **Day 3 – Advanced Kafka & Real-World Fintech Use Case**

**Objective:** Implement a full fintech EDA pipeline with Kafka Streams.

1. **Kafka Streams & Real-Time Processing**

   - Stream processing vs. batch
   - Windowing, joins, and aggregations
   - Stateful vs. stateless processing in fraud detection

2. **Schema Management with Confluent Schema Registry**

   - Versioning in financial event schemas
   - Avoiding breaking changes in payment processing pipelines

3. **Fintech Use Case – Fraud Detection Pipeline**

   - Event producer → Kafka topic → Stream processing → Fraud alert topic → Consumer service
   - Threshold-based rules + anomaly detection basics

**Lab 3:**

- Create a transaction event producer
- Process events using Kafka Streams to identify multiple failed transactions from the same account in under 5 minutes
- Publish fraud alerts to a separate Kafka topic and consume them in an alert dashboard

## **Day 4 – Reliability, Scalability, and Integration**

**Objective:** Implement robust and scalable Kafka-based fintech solutions.

1. **Kafka Reliability & Performance Tuning**

   - Replication, partitioning, and leader election
   - Message delivery guarantees (at most once, at least once, exactly once)
   - Scaling consumers for high transaction throughput

2. **Integration with External Systems**

   - Kafka Connect for databases, APIs, and cloud services
   - Connecting to PostgreSQL/MySQL for transaction logging
   - Using Kafka with REST APIs for payment gateways

3. **End-to-End Fintech Scenario**

   - Payment initiation → transaction routing → AML checks → settlement notification
   - Simulating high-volume transactions for performance testing

**Lab 4:**

- Set up Kafka Connect to persist processed transactions into a PostgreSQL database
- Simulate 1000+ transactions per minute and monitor performance

## **Day 5 – Monitoring, Security, and Final Project**

**Objective:** Secure, monitor, and deploy a complete fintech event-driven system.

1. **Security in Kafka**

   - TLS/SSL encryption
   - SASL authentication (Plain, SCRAM)
   - ACLs for controlling access to topics in financial systems

2. **Monitoring & Observability**

   - Kafka metrics & JMX monitoring
   - Integrating with Prometheus & Grafana for real-time dashboards
   - Alerting for slow consumers and failed transactions

3. **Capstone Project – “Real-Time Payment Processing System”**

   - Multiple producers simulating transactions from different payment channels
   - Stream processing for fraud checks & transaction enrichment
   - Database persistence and alert notification via email/Slack

**Final Lab:**

- Teams implement the payment processing pipeline end-to-end
- Present architecture, code, and demo to the group

## **Delivery Notes**

- **Hands-on ratio:** \~60% labs, 40% theory
- **Prerequisites:** Basic programming knowledge (Java/Python), understanding of microservices
- **Infra:** Laptops with Docker/Kafka pre-installed or lab machines with pre-setup environment
- **Outcome:** Participants leave with working Kafka-based fintech EDA projects they can adapt to their organization
