#!/usr/bin/env bash
# Produces a test Avro message to the city-counts topic via schema-registry.
# Run AFTER registering the schema (register-schema.sh) and the connector.

SCHEMA='{"type":"record","name":"CityCounts","fields":[{"name":"city","type":"string"},{"name":"total_count","type":"int"}]}'

echo '{"city":"Bangalore","total_count":16}' | \
  docker exec -it -e KAFKA_OPTS="" schema-registry \
    kafka-avro-console-producer \
      --broker-list kafka1:29092 \
      --topic city-counts \
      --property schema.registry.url=http://schema-registry:8081 \
      --property "value.schema=$SCHEMA"

echo "Message sent. Check postgres: docker exec -it postgres psql -U postgres -d kafkadb -c 'SELECT * FROM city_counts;'"