# city-counts-producer

A minimal Java/Maven Kafka producer that sends `CityCounts` Avro messages
to the `city-counts` topic, which is consumed by the JDBC Sink connector
and upserted into the `city_counts` PostgreSQL table.

## Project layout

```
city-counts-producer/
├── pom.xml
└── src/
    ├── main/
    │   ├── avro/
    │   │   └── CityCounts.avsc          ← Avro schema (source of truth)
    │   ├── java/com/example/producer/
    │   │   ├── CityCountsProducerApp.java    ← main()
    │   │   ├── CityCountsProducer.java       ← producer wrapper
    │   │   └── CityCountsProducerConfig.java ← KafkaProducer properties
    │   └── resources/
    │       └── logback.xml
    └── test/
        └── java/com/example/producer/
            └── CityCountsProducerConfigTest.java
```

## Prerequisites

- JDK 17+
- Maven 3.8+
- The Docker stack from this repo running (`docker compose up -d`)

## Build

```bash
mvn clean package -q
```

This generates the Avro Java classes from `CityCounts.avsc` and produces a
fat-jar at `target/city-counts-producer-1.0.0-SNAPSHOT.jar`.

## Run

```bash
# Against the local Docker stack (default)
java -jar target/city-counts-producer-1.0.0-SNAPSHOT.jar
```

Override connection details via system properties:

```bash
java \
  -DBOOTSTRAP_SERVERS=localhost:9092 \
  -DSCHEMA_REGISTRY_URL=http://localhost:8081 \
  -jar target/city-counts-producer-1.0.0-SNAPSHOT.jar
```

Or via environment variables:

```bash
export BOOTSTRAP_SERVERS=localhost:9092
export SCHEMA_REGISTRY_URL=http://localhost:8081
java -jar target/city-counts-producer-1.0.0-SNAPSHOT.jar
```

## Verify in PostgreSQL

```bash
docker exec -it postgres \
  psql -U postgres -d kafkadb \
  -c "SELECT * FROM city_counts;"
```

Expected (after running with the default sample data):

```
    city    | total_count
------------+-------------
 Mumbai     |          42
 Delhi      |          31
 Bangalore  |          25   ← upserted from 16 → 25
(3 rows)
```

## Extending the producer

To send your own data, edit the sample section in `CityCountsProducerApp.java`:

```java
producer.send("YourCity", 100);
```

Or read from a file, database, or any other source and loop:

```java
myDataList.forEach(row -> producer.send(row.city(), row.count()));
producer.flush();
```

## Notes

- The Avro schema is registered automatically in Schema Registry on the first
  run (`auto.register.schemas=true`). In production, disable this and manage
  schemas explicitly via CI.
- The producer uses `acks=all` and `max.in.flight.requests.per.connection=1`
  for strong durability and ordering guarantees.
- The fat-jar is built with `maven-shade-plugin`. Signature files from signed
  jars are stripped to avoid `SecurityException` at runtime.
