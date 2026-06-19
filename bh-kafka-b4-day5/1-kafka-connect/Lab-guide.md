# Lab: Kafka Streams → PostgreSQL using Kafka Connect JDBC Sink

## Objective

Build a real-time pipeline:

```text
City Events
    |
    v
Kafka Streams Aggregation
    |
    v
city-counts Topic
    |
    v
Kafka Connect JDBC Sink
    |
    v
PostgreSQL city_counts Table
```

The sink connector should:

- Insert new cities
- Update existing city counts
- Require no custom consumer code

# Step 1: Add PostgreSQL and Kafka Connect to Docker Compose

Update the docker-compose-connect.yaml to add `postgres` and `kafka-connect` services:

```yaml
services:
  # existing services - zookeeper, kafka1, kafka2, ...

  postgres:
    image: postgres:16
    container_name: postgres
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: kafkadb
    ports:
      - '5432:5432'

  kafka-connect:
    image: confluentinc/cp-kafka-connect:7.6.1
    container_name: kafka-connect
    depends_on:
      - postgres
    ports:
      - '8083:8083'
    environment:
      CONNECT_BOOTSTRAP_SERVERS: broker1:9092,broker2:9092,broker3:9092
      CONNECT_GROUP_ID: connect-cluster
      CONNECT_CONFIG_STORAGE_TOPIC: connect-configs
      CONNECT_OFFSET_STORAGE_TOPIC: connect-offsets
      CONNECT_STATUS_STORAGE_TOPIC: connect-status
      CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR: 3
      CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR: 3
      CONNECT_STATUS_STORAGE_REPLICATION_FACTOR: 3
      CONNECT_KEY_CONVERTER: org.apache.kafka.connect.storage.StringConverter
      CONNECT_VALUE_CONVERTER: org.apache.kafka.connect.json.JsonConverter
      CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE: false
      CONNECT_REST_ADVERTISED_HOST_NAME: kafka-connect
      CONNECT_PLUGIN_PATH: /usr/share/java,/usr/share/confluent-hub-components
    command:
      - bash
      - -c
      - |
        confluent-hub install --no-prompt confluentinc/kafka-connect-jdbc:latest
        /etc/confluent/docker/run
```

# Step 2: Start the New Services

Start the services:

```bash
docker compose up -d
```

Verify:

```bash
docker ps
```

# Step 3: Verify Kafka Connect

Open:

```text
http://localhost:8083
```

or

```bash
curl http://localhost:8083
```

Expected:

```json
{
  "version": "..."
}
```

# Step 4: Verify JDBC Connector Installation

```bash
curl http://localhost:8083/connector-plugins
```

Look for:

```text
io.confluent.connect.jdbc.JdbcSinkConnector
```

# Step 5: Create PostgreSQL Table

Enter PostgreSQL:

```bash
docker exec -it postgres psql -U postgres -d kafkadb
```

Create table:

```sql
CREATE TABLE city_counts
(
    city VARCHAR(100) PRIMARY KEY,
    total_count INTEGER
);
```

Verify:

```sql
SELECT * FROM city_counts;
```

Initially:

```text
0 rows
```

# Step 6: Verify Kafka Streams Output

Your Kafka Streams demo should already be producing records into:

```text
city-counts
```

Verify:

```bash
kafka-console-consumer.sh \
--bootstrap-server localhost:9092 \
--topic city-counts \
--from-beginning
```

Expected:

```json
{
  "city": "Bangalore",
  "total_count": 15
}
```

```json
{
  "city": "Mumbai",
  "total_count": 8
}
```

# Step 7: Create Sink Connector Configuration

Create:

```text
city-count-sink.json
```

```json
{
  "name": "city-count-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "city-counts",
    "connection.url": "jdbc:postgresql://postgres:5432/kafkadb",
    "connection.user": "postgres",
    "connection.password": "postgres",
    "table.name.format": "city_counts",
    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "city",
    "auto.create": "false",
    "auto.evolve": "false"
  }
}
```

# Step 8: Register Connector

```bash
curl -X POST \
http://localhost:8083/connectors \
-H "Content-Type: application/json" \
-d @city-count-sink.json
```

Expected:

```json
{
  "name": "city-count-sink"
}
```

# Step 9: Check Connector Status

```bash
curl \
http://localhost:8083/connectors/city-count-sink/status
```

Expected:

```json
{
  "connector": {
    "state": "RUNNING"
  }
}
```

# Step 10: Observe Data Flow

Generate additional city events.

Example:

```json
{
  "city": "Bangalore",
  "total_count": 10
}
```

Kafka Streams updates the aggregate:

```json
{
  "city": "Bangalore",
  "total_count": 16
}
```

Kafka Connect receives the update and executes:

```sql
UPDATE city_counts
SET total_count = 16
WHERE city = 'Bangalore';
```

automatically.

# Step 11: Verify Data in PostgreSQL

Connect again:

```bash
docker exec -it postgres psql -U postgres -d kafkadb
```

Execute:

```sql
SELECT * FROM city_counts;
```

Expected:

```text
 city         | total_count
--------------+------------
 Bangalore    | 16
 Mumbai       | 8
 Delhi        | 12
```
