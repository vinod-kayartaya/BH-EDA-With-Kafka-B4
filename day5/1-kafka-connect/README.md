# Kafka Connect

Kafka Connect is a framework within Apache Kafka that allows you to move data between Kafka and external systems without writing custom producer or consumer code.

# Why Kafka Connect?

Without Kafka Connect:

```text
Database → Custom Producer → Kafka
Kafka → Custom Consumer → Database
```

You must write, deploy, monitor, and maintain custom code.

With Kafka Connect:

```text
Database → Source Connector → Kafka
Kafka → Sink Connector → Database
```

No custom application is required.

# External systems

Kafka Connect is not limited to databases; it can integrate Kafka with virtually any system that stores, processes, or transmits data.
In the Kafka ecosystem, external systems are divided into Sources (where data comes from) and Sinks (where data goes). Hundreds of pre-built plugins exist to connect Kafka to almost any enterprise technology stack.

## 1. Databases (Relational & NoSQL)

While not the only option, databases are highly popular due to Change Data Capture (CDC) frameworks like Debezium.

- Relational (RDBMS): PostgreSQL, MySQL, Oracle, Microsoft SQL Server, and IBM DB2.
- NoSQL: MongoDB, Cassandra, ScyllaDB, Couchbase, and Redis.
- Data Warehouses: Snowflake, Google BigQuery, Amazon Redshift, and Databricks.

## 2. Cloud Object Storage

Kafka Connect is heavily used to archive high-throughput real-time data into cheap, long-term cloud storage for data lakes.

- Amazon S3
- Google Cloud Storage (GCS)
- Azure Blob Storage / Azure Data Lake Storage (ADLS)

## 3. Search and Analytics Engines

You can stream data directly into search engines to enable instant indexing and log analytics without writing custom application code.

- Elasticsearch
- OpenSearch
- Apache Solr
- Splunk

## 4. Messaging & Event Streaming Systems

Kafka Connect can bridge different generations of messaging infrastructure or connect distinct cloud ecosystems.

- Traditional MQ: IBM MQ, RabbitMQ, and ActiveMQ (via JMS).
- Cloud Native: AWS Kinesis, AWS SQS/SNS, Google Cloud Pub/Sub, and Azure Event Hubs.
- Other Protocols: MQTT (for IoT devices and sensors).

## 5. SaaS and HTTP Applications

You can ingest data from business software or push Kafka events directly to third-party web endpoints.

- SaaS Platforms: Salesforce, ServiceNow, and Jira.
- Webhooks / HTTP: General REST APIs (allowing you to send data to any HTTP endpoint or poll an external API for changes).

## 6. File Systems & Big Tech Ecosystems

- Legacy Systems: Local file systems (watching directories for new CSV/JSON files) and SFTP/FTP servers.
- Big Data: Hadoop HDFS and Apache Hive.

If you have a specific system in mind, let me know what external technology you are trying to connect to Kafka and whether you need to read from it or write to it. I can point you toward the exact connector plugin you need!

# Kafka Connect Architecture

```text
                +------------------+
                | Kafka Connect    |
                | Worker           |
                +--------+---------+
                         |
        +----------------+----------------+
        |                                 |
+-------v------+                 +--------v------+
| Source       |                 | Sink          |
| Connector    |                 | Connector     |
+-------+------+                 +--------+------+
        |                                 |
        |                                 |
+-------v------+                 +--------v------+
| Database     |                 | PostgreSQL    |
| MySQL        |                 | Elasticsearch |
+--------------+                 +---------------+

                Kafka Topics
```

### Components

#### Worker

A Kafka Connect process that executes connectors.

#### Connector

Defines the integration logic.

Examples:

- JDBC Source Connector
- JDBC Sink Connector
- Elasticsearch Sink Connector
- S3 Sink Connector

#### Tasks

Actual units of work executed by a connector.

One connector may run multiple tasks in parallel.

# Types of Connectors

## 1. Source Connector

Reads data from an external system and writes it into Kafka.

Example:

```text
MySQL Customer Table
        |
        v
JDBC Source Connector
        |
        v
customer-topic
```

## 2. Sink Connector

Reads data from Kafka and writes it to an external system.

Example:

```text
payment-topic
        |
        v
JDBC Sink Connector
        |
        v
PostgreSQL
```

# Standalone vs Distributed Mode

## Standalone Mode

Single worker process.

```text
Worker
  |
  +-- Connector
```

Suitable for:

- Development
- Testing
- Learning

## Distributed Mode

Multiple workers share workload.

```text
Worker 1
Worker 2
Worker 3

      |
      v

Kafka Connect Cluster
```

Suitable for:

- Production
- Fault tolerance
- Scalability

# Example: PostgreSQL → Kafka

Suppose we have a table:

```sql
CREATE TABLE payments (
    id INT,
    customer_id VARCHAR(20),
    amount DECIMAL(10,2)
);
```

JDBC Source Connector reads rows:

```text
payments table
      |
      v
JDBC Source Connector
      |
      v
payments-topic
```

Resulting Kafka message:

```json
{
  "id": 101,
  "customer_id": "C100",
  "amount": 2500
}
```

# Example: Kafka → PostgreSQL

A payment service produces events:

```json
{
  "paymentId": "P1001",
  "amount": 5000
}
```

JDBC Sink Connector writes them into PostgreSQL.

```text
payment-topic
      |
      v
JDBC Sink Connector
      |
      v
payments table
```

# Advantages of Kafka Connect

### Reduced Development Effort

No custom producer or consumer code.

### Scalability

Connectors can run multiple tasks.

### Fault Tolerance

Distributed mode automatically recovers from failures.

### Offset Management

Kafka Connect automatically tracks progress.

### Ecosystem Support

Hundreds of connectors are available for:

- Databases
- Elasticsearch
- MongoDB
- S3
- BigQuery
- Redis
- Snowflake

# Kafka Connect vs Custom Java Code

| Feature           | Kafka Connect         | Custom Java            |
| ----------------- | --------------------- | ---------------------- |
| Coding Required   | Minimal               | High                   |
| Maintenance       | Low                   | High                   |
| Scaling           | Built-in              | Manual                 |
| Offset Management | Automatic             | Manual                 |
| Best For          | Standard integrations | Complex business logic |

# Real-World Example (Payment System)

From your training project, a typical Day 4 integration would be:

```text
Payment Service
       |
       v
payment-topic
       |
       v
Kafka Connect JDBC Sink
       |
       v
PostgreSQL Database
```

This allows every payment event flowing through Kafka to be automatically persisted to the database without writing a consumer application.

# Key Takeaways

- Kafka Connect is Kafka's data integration framework.
- Source Connectors move data **into Kafka**.
- Sink Connectors move data **out of Kafka**.
- Connectors run inside Kafka Connect workers.
- Supports standalone and distributed deployments.
- Eliminates the need for boilerplate producer/consumer code.
- Ideal for database, search engine, cloud storage, and analytics integrations.
