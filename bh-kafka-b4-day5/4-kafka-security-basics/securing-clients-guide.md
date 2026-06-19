# Kafka Client Security Guide (SASL/PLAIN)

This guide demonstrates how to configure and execute console clients (producer/consumer) and Java clients to interact with the secured 3-broker KRaft Kafka cluster.

---

## 1. Preparing the Client Configurations

To authenticate with the Kafka brokers over `SASL_PLAINTEXT` using the `PLAIN` mechanism, clients must supply:
- **Security Protocol**: `SASL_PLAINTEXT`
- **SASL Mechanism**: `PLAIN`
- **JAAS Configuration**: A JAAS configuration snippet containing a valid username and password (e.g. user `vinod` with password `Welcome#123`).

We have pre-created a client properties file at `secrets/client-sasl.properties` (which is mounted into the containers as `/etc/kafka/secrets/client-sasl.properties`):

```properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="vinod" \
    password="Welcome#123";
```

---

## 2. Using Console Clients (CLI)

Since the `./secrets` folder is mounted into the `kafka1` container, we can easily run console clients inside the docker container using `docker exec`.

### Step A: Start Console Consumer
Open a terminal window and start the console consumer to listen to a topic called `secure-topic`:

```bash
docker exec -it kafka1 kafka-console-consumer \
  --bootstrap-server localhost:29092 \
  --topic secure-topic \
  --from-beginning \
  --consumer.config /etc/kafka/secrets/client-sasl.properties
```

### Step B: Start Console Producer
Open another terminal window and start the console producer:

```bash
docker exec -it kafka1 kafka-console-producer \
  --bootstrap-server localhost:29092 \
  --topic secure-topic \
  --producer.config /etc/kafka/secrets/client-sasl.properties
```

Type a few messages (e.g. `Hello Secure Kafka!`) in the producer window and verify they appear in the consumer window.

---

## 3. Using Java Clients

To build a Java client that connects to the secure cluster, configure the following connection properties:

### Java Producer Example

```java
package com.example;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SecureProducerApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        
        // 1. Connection configuration (points to the external port on the host IP)
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); 
        
        // 2. Security configuration (SASL_PLAINTEXT + PLAIN)
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, 
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"vinod\" " +
            "password=\"Welcome#123\";"
        );
        
        // 3. Serializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);
        
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>("secure-topic", "key1", "Hello Secure Java!");
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("✅ Sent successfully to topic " + metadata.topic() + " at offset " + metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
        } finally {
            producer.close();
        }
    }
}
```

### Java Consumer Example

```java
package com.example;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class SecureConsumerApp {
    public static void main(String[] args) {
        Properties props = new Properties();
        
        // 1. Connection configuration
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "secure-group-java");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        // 2. Security configuration (SASL_PLAINTEXT + PLAIN)
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG, 
            "org.apache.kafka.common.security.plain.PlainLoginModule required " +
            "username=\"vinod\" " +
            "password=\"Welcome#123\";"
        );
        
        // 3. Deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Consumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("secure-topic"));

        System.out.println("Listening for messages...");
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Received: Key = %s, Value = %s (offset = %d)\n", 
                        record.key(), record.value(), record.offset());
                }
            }
        } finally {
            consumer.close();
        }
    }
}
```
