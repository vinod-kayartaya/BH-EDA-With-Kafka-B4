# Lab: Kafka → PostgreSQL using Kafka Connect JDBC Sink (Without Avro/Schemas)

## Objective

Build a real-time database ingestion pipeline using raw, schemaless JSON:

```text
  transactions (JSON Message)
                 |
                 v
     Kafka Connect JDBC Sink
                 |
                 v
   PostgreSQL transactions Table
```

### Is Avro Required?
**No, Avro is NOT required.** While Avro is excellent for schema enforcement and evolutionary safety in production, Kafka Connect can easily ingest standard JSON.

However, the JDBC Sink Connector requires structured records (`Struct` type in Kafka Connect) to map fields to SQL columns. If we pass raw, schemaless JSON directly (with `schemas.enable=false`), Kafka Connect parses it as a raw `HashMap` and fails with the following error:
`requires records with a non-null Struct or String value and non-null Struct or String schema, but found ... HashMap`

To solve this without Avro or a Schema Registry, we configure the JSON Converter with schemas enabled (`"value.converter.schemas.enable": "true"`). This expects each JSON message to be wrapped in a `schema` and `payload` envelope.

---

## Step 1: Verify Kafka Connect and JDBC Plugin

Make sure your Kafka cluster and Connect service are running. (If not, run `start.sh` in the `0-kafka-cluster` directory first).

Verify that the JDBC Sink Connector plugin is loaded:

```bash
curl -s http://localhost:8083/connector-plugins | grep -o "io.confluent.connect.jdbc.JdbcSinkConnector"
```

---

## Step 2: Create PostgreSQL Table `transactions`

Since we are not passing schema metadata within the Kafka message (no Avro/Registry, and no JSON Schema envelop), we **must** define the table structure in PostgreSQL before starting the connector.

Enter the PostgreSQL terminal:

```bash
docker exec -it postgres psql -U postgres -d kafkadb
```

Create the `transactions` table:

```sql
CREATE TABLE transactions (
    transaction_id VARCHAR(50) PRIMARY KEY,
    amount NUMERIC(10, 2) NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Verify that it exists:

```sql
SELECT * FROM transactions;
```

---

## Step 3: Create the Sink Connector Configuration

In the `1-kafka-connect` folder, create or locate the `transactions-sink.json` configuration file:

```json
{
  "name": "transactions-sink",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "tasks.max": "1",
    "topics": "transactions",
    "connection.url": "jdbc:postgresql://postgres:5432/kafkadb",
    "connection.user": "postgres",
    "connection.password": "postgres",
    "table.name.format": "transactions",
    "insert.mode": "upsert",
    "pk.mode": "record_value",
    "pk.fields": "transaction_id",
    "auto.create": "false",
    "auto.evolve": "false",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "true",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter"
  }
}
```

### Key Configurations:
- `"insert.mode": "upsert"`: Enables upserting. If a record with the same primary key exists, it updates it; otherwise, it inserts it.
- `"pk.mode": "record_value"`: Specifies that the primary key field will be read from the record's payload values.
- `"pk.fields": "transaction_id"`: Designates `transaction_id` as the primary key.
- `"value.converter.schemas.enable": "true"`: Ingests JSON documents wrapped in a `schema`/`payload` envelope, allowing the converter to build a `Struct` record expected by the JDBC connector.

---

## Step 4: Register the Sink Connector

Submit the configuration to the Kafka Connect REST API:

```bash
curl -X POST \
http://localhost:8083/connectors \
-H "Content-Type: application/json" \
-d @transactions-sink.json
```

---

## Step 5: Check Connector Status

Verify that the connector starts successfully and is in the `RUNNING` state:

```bash
curl -s http://localhost:8083/connectors/transactions-sink/status
```

---

## Step 6: Publish Raw JSON Messages

Start a console producer to send JSON events to the `transactions`.

**Option A (Using Docker container CLI - recommended)**:
```bash
docker exec -it kafka1 kafka-console-producer --bootstrap-server localhost:29092 --topic transactions
```

**Option B (Using local installation CLI)**:
```bash
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic transactions
```

Copy and paste the following JSON messages one by one into the producer terminal.

> [!IMPORTANT]
> The console producer expects each message to be on a single line. We have shown the pretty-printed structure first for readability, followed by the exact single-line version you must copy and paste.

### Message 1 (TXN_1001)
**Structure (for reference):**
```json
{
  "schema": {
    "type": "struct",
    "optional": false,
    "fields": [
      { "type": "string", "optional": false, "field": "transaction_id" },
      { "type": "double", "optional": false, "field": "amount" },
      { "type": "string", "optional": false, "field": "customer_name" },
      { "type": "string", "optional": false, "field": "status" }
    ]
  },
  "payload": {
    "transaction_id": "TXN_1001",
    "amount": 250.75,
    "customer_name": "David Miller",
    "status": "PENDING"
  }
}
```

**Single-line (copy and paste this):**
```json
{"schema":{"type":"struct","optional":false,"fields":[{"type":"string","optional":false,"field":"transaction_id"},{"type":"double","optional":false,"field":"amount"},{"type":"string","optional":false,"field":"customer_name"},{"type":"string","optional":false,"field":"status"}]},"payload":{"transaction_id":"TXN_1001","amount":250.75,"customer_name":"David Miller","status":"PENDING"}}
```

### Message 2 (TXN_1002)
**Structure (for reference):**
```json
{
  "schema": {
    "type": "struct",
    "optional": false,
    "fields": [
      { "type": "string", "optional": false, "field": "transaction_id" },
      { "type": "double", "optional": false, "field": "amount" },
      { "type": "string", "optional": false, "field": "customer_name" },
      { "type": "string", "optional": false, "field": "status" }
    ]
  },
  "payload": {
    "transaction_id": "TXN_1002",
    "amount": 15.00,
    "customer_name": "Emma Wilson",
    "status": "COMPLETED"
  }
}
```

**Single-line (copy and paste this):**
```json
{"schema":{"type":"struct","optional":false,"fields":[{"type":"string","optional":false,"field":"transaction_id"},{"type":"double","optional":false,"field":"amount"},{"type":"string","optional":false,"field":"customer_name"},{"type":"string","optional":false,"field":"status"}]},"payload":{"transaction_id":"TXN_1002","amount":15.00,"customer_name":"Emma Wilson","status":"COMPLETED"}}
```

---

## Step 7: Verify Data in PostgreSQL (Insert Verification)

Connect to PostgreSQL and verify that the records were inserted:

```sql
SELECT * FROM transactions;
```

### Expected Output:
```text
 transaction_id | amount | customer_name | status  |         created_at         
----------------+--------+---------------+---------+----------------------------
 TXN_1001       | 250.75 | David Miller  | PENDING | 2026-06-18 22:30:15.123456
 TXN_1002       |  15.00 | Emma Wilson   | COMPLETED | 2026-06-18 22:30:18.654321
```

---

## Step 8: Verify Upsert/Update Functionality

To test the upsert capability, copy and paste an update for the **same** transaction ID (`TXN_1001`) with an updated status (`APPROVED`).

**Structure (for reference):**
```json
{
  "schema": {
    "type": "struct",
    "optional": false,
    "fields": [
      { "type": "string", "optional": false, "field": "transaction_id" },
      { "type": "double", "optional": false, "field": "amount" },
      { "type": "string", "optional": false, "field": "customer_name" },
      { "type": "string", "optional": false, "field": "status" }
    ]
  },
  "payload": {
    "transaction_id": "TXN_1001",
    "amount": 250.75,
    "customer_name": "David Miller",
    "status": "APPROVED"
  }
}
```

**Single-line (copy and paste this):**
```json
{"schema":{"type":"struct","optional":false,"fields":[{"type":"string","optional":false,"field":"transaction_id"},{"type":"double","optional":false,"field":"amount"},{"type":"string","optional":false,"field":"customer_name"},{"type":"string","optional":false,"field":"status"}]},"payload":{"transaction_id":"TXN_1001","amount":250.75,"customer_name":"David Miller","status":"APPROVED"}}
```

Now, query PostgreSQL again to verify the status change:

```sql
SELECT * FROM transactions;
```

### Expected Output:
```text
 transaction_id | amount | customer_name | status   |         created_at         
----------------+--------+---------------+----------+----------------------------
 TXN_1002       |  15.00 | Emma Wilson   | COMPLETED| 2026-06-18 22:30:18.654321
 TXN_1001       | 250.75 | David Miller  | APPROVED | 2026-06-18 22:30:15.123456
```

Note that the status for `TXN_1001` has been updated from `PENDING` to `APPROVED` dynamically!
