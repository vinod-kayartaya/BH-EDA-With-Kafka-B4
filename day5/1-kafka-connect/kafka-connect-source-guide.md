# Lab: PostgreSQL → Kafka using Kafka Connect JDBC Source

## Objective

Build a real-time data ingestion pipeline:

```text
PostgreSQL payments Table (New Row)
              |
              v
    Kafka Connect JDBC Source
              |
              v
     new-payment-in-db Topic
```

The source connector should:
- Monitor the `payments` table for new inserts.
- Automatically capture new records without any polling/querying application code.
- Publish each new record as a JSON message to the `new-payment-in-db` topic.

---

## Step 1: Verify Kafka Connect and JDBC Plugin

Make sure your Kafka cluster and Connect service are running. (If not, run `start.sh` in the `0-kafka-cluster` directory first).

Verify that the JDBC Source Connector plugin is loaded:

```bash
curl -s http://localhost:8083/connector-plugins | grep -o "io.confluent.connect.jdbc.JdbcSourceConnector"
```

If it returns `io.confluent.connect.jdbc.JdbcSourceConnector`, you are ready to proceed.

---

## Step 2: Connect to PostgreSQL and Create the `payments` Table

Enter the PostgreSQL container terminal using `docker exec`:

```bash
docker exec -it postgres psql -U postgres -d kafkadb
```

Create the `payments` table. We need an auto-incrementing primary key (`id`) so the JDBC connector can detect new records:

```sql
CREATE TABLE payments (
    id SERIAL PRIMARY KEY,
    amount NUMERIC(10, 2) NOT NULL,
    customer_name VARCHAR(100) NOT NULL,
    payment_method VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Verify the table exists (it should be empty initially):

```sql
SELECT * FROM payments;
```

Keep this PostgreSQL session open or remember how to connect back to it, as we will insert records here later.

---

## Step 3: Create the Source Connector Configuration

In the `1-kafka-connect` folder, create or locate the `payments-source.json` configuration file:

```json
{
  "name": "payments-source",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "tasks.max": "1",
    "connection.url": "jdbc:postgresql://postgres:5432/kafkadb",
    "connection.user": "postgres",
    "connection.password": "postgres",
    "mode": "incrementing",
    "incrementing.column.name": "id",
    "table.whitelist": "payments",
    "topic.prefix": "jdbc-",
    "transforms": "renameTopic",
    "transforms.renameTopic.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.renameTopic.regex": ".*",
    "transforms.renameTopic.replacement": "new-payment-in-db",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false"
  }
}
```

### Explanation of Key Configuration Settings:
- `"mode": "incrementing"`: Tells the connector to detect new rows using an incrementing column.
- `"incrementing.column.name": "id"`: Identifies the auto-incrementing primary key.
- `"table.whitelist": "payments"`: Restricts the connector to poll only this table.
- **Single Message Transform (SMT) `renameTopic`**:
  By default, the connector publishes to a topic named `<topic.prefix><table_name>` (e.g. `jdbc-payments`). We use the `RegexRouter` transform to rename/route all captured records to exactly `new-payment-in-db`.
- `"value.converter.schemas.enable": "false"`: Strips out the Connect schema wrapper to produce clean, standard JSON messages.

---

## Step 4: Register the Source Connector

Submit the configuration to the Kafka Connect REST API:

```bash
curl -X POST \
http://localhost:8083/connectors \
-H "Content-Type: application/json" \
-d @payments-source.json
```

### Expected Response:
```json
{"name":"payments-source","config":{...},"tasks":[],"type":"source"}
```

---

## Step 5: Check Connector Status

Verify that the connector is registered and running successfully:

```bash
curl -s http://localhost:8083/connectors/payments-source/status
```

Look for `"state":"RUNNING"` under both the `connector` and the `tasks` array:
```json
{
  "name": "payments-source",
  "connector": {
    "state": "RUNNING",
    "worker_id": "kafka-connect:8083"
  },
  "tasks": [
    {
      "id": 0,
      "state": "RUNNING",
      "worker_id": "kafka-connect:8083"
    }
  ],
  "type": "source"
}
```

---

## Step 6: Start a Console Consumer

Start a console consumer to listen to the `new-payment-in-db` topic so you can watch messages arrive in real-time.

**Option A (Using Docker container CLI - recommended)**:
```bash
docker exec -it kafka1 kafka-console-consumer --bootstrap-server localhost:29092 --topic new-payment-in-db --from-beginning
```

**Option B (Using local installation CLI)**:
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic new-payment-in-db --from-beginning
```

---

## Step 7: Insert Records and Observe the Data Flow

Go back to your open PostgreSQL interactive terminal (`psql`) and insert a few records:

```sql
INSERT INTO payments (amount, customer_name, payment_method) 
VALUES (150.00, 'Alice Smith', 'CREDIT_CARD');

INSERT INTO payments (amount, customer_name, payment_method) 
VALUES (45.50, 'Bob Jones', 'DEBIT_CARD');

INSERT INTO payments (amount, customer_name, payment_method) 
VALUES (890.95, 'Charlie Brown', 'BANK_TRANSFER');
```

Verify that the records are successfully inserted:
```sql
SELECT * FROM payments;
```

### Observe Kafka Consumer Output
Switch back to your terminal running the console consumer. You should see three JSON messages printed automatically:

```json
{"id":1,"amount":150.00,"customer_name":"Alice Smith","payment_method":"CREDIT_CARD","created_at":1781878400000}
{"id":2,"amount":45.50,"customer_name":"Bob Jones","payment_method":"DEBIT_CARD","created_at":1781878415000}
{"id":3,"amount":890.95,"customer_name":"Charlie Brown","payment_method":"BANK_TRANSFER","created_at":1781878430000}
```

Notice that the JDBC connector automatically pulled the new rows, converted the database columns into JSON object keys, converted timestamps to epochs, and pushed them to the Kafka topic instantly!
