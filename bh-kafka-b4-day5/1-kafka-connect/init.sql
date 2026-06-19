-- Run this against the kafkadb database to create the target table.
-- The JDBC Sink connector requires the table to exist when auto.create=false.

CREATE TABLE IF NOT EXISTS city_counts (
    city        VARCHAR(255) PRIMARY KEY,
    total_count INTEGER NOT NULL
);
