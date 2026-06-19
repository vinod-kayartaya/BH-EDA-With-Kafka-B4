# Google Protocol Buffers (Protobuf) Reference Guide

This guide provides a comprehensive reference on Google Protocol Buffers (Protobuf) in the context of Apache Kafka, covering schemas, Maven code generation, custom and Confluent serialization/deserialization, and schema evolution rules.

---

## 1. Introduction

### What is Protocol Buffers?
Google Protocol Buffers (Protobuf) is a language-neutral, platform-neutral, extensible mechanism for serializing structured data. Unlike XML or JSON, Protobuf compiles schemas into an extremely optimized binary format, making it the format of choice for high-performance RPCs (like gRPC) and high-throughput event streaming systems (like Apache Kafka).

In a Kafka pipeline, Protobuf provides strongly-typed data contracts that are serialized into minimal byte arrays, resulting in significantly lower CPU processing overhead and network bandwidth consumption compared to text formats.

### Core Comparison: JSON vs. Avro vs. Protobuf
To choose the right format, it is critical to understand their underlying trade-offs:

| Feature | JSON | Apache Avro | Google Protobuf |
| :--- | :--- | :--- | :--- |
| **Human Readable** | **Yes** (Plain text) | No (Binary payload) | No (Binary payload) |
| **Schema Required** | No | **Yes** (JSON `.avsc`) | **Yes** (Proto `.proto`) |
| **Schema Identification** | Implicit | Reference in Message (Schema ID) | Field Tag Numbers |
| **Payload Structure** | Repeats field names as strings | Appends only values | Encodes values with field tags |
| **Serialization Speed** | Slowest | Fast | **Fastest** (Extremely optimized) |
| **Payload Size** | Largest | Small | **Smallest** (Varint compression) |
| **Schema Evolution** | Weak | Excellent (Strict registry rules) | Excellent (Rule-based tag handling) |
| **Native Registry Support** | Limited | Native (De facto standard) | Supported (Confluent v5.5+) |
| **Best Use Cases** | Public APIs, Logging | Kafka-centric data lakes | Cross-system integration, gRPC |

---

## 2. Protobuf Schema Syntax (proto3)

Protobuf schemas are saved in files ending with the `.proto` extension. The current standard version is **proto3**.

### Anatomy of a `.proto` Schema File
Below is the schema file `customer.proto` from the `protobuf-basics` module:

```protobuf
syntax = "proto3";

package co.vinod.protobuf;

// Java-specific generation parameters
option java_multiple_files = true;
option java_package = "co.vinod.protobuf.model";
option java_outer_classname = "CustomerProto";

message Customer {
  string customerId = 1;
  string name = 2;
  string email = 3;
  double creditLimit = 4;
}
```

### Key Elements of Schema Files:
1. **`syntax = "proto3";`**: Declares that this file uses the proto3 compiler rules (which simplifies fields by making them optional by default and removing explicit `required` constraints).
2. **`package co.vinod.protobuf;`**: Defines the namespace of the schema to prevent name clashes in multi-project setups.
3. **Java Options**:
   * `java_multiple_files = true`: Generates separate `.java` files for each message type (e.g., `Customer.java` and its builder). If `false`, all classes are generated as nested classes within a single outer class.
   * `java_package`: Specifies the target Java package for generated classes.
   * `java_outer_classname`: Defines the wrapper class name containing metadata.
4. **`message Customer`**: Defines the structured object.
5. **Field Declarations & Tag Numbers (`= 1`, `= 2`)**:
   * Every field in a message is assigned a unique **Tag Number**.
   * These tags are used to identify fields in the binary message format. They should **never** be changed once the schema is in use.
   * **Performance Tip:** Tag numbers `1` through `15` take exactly **1 byte** to encode along with the field value. Tags `16` through `2047` take **2 bytes**. Keep tags 1-15 reserved for the most frequently sent fields.

---

## 3. Maven Configuration & Code Generation

Like Avro, Protobuf requires compilation to translate `.proto` files into source code.

### Maven Build Lifecycle Configuration
Add the following dependencies and plugins to your `pom.xml`:

```xml
<properties>
    <protobuf.version>4.32.0</protobuf.version>
    <kafka.version>4.1.0</kafka.version>
</properties>

<dependencies>
    <!-- Protobuf Java Runtime Library -->
    <dependency>
        <groupId>com.google.protobuf</groupId>
        <artifactId>protobuf-java</artifactId>
        <version>${protobuf.version}</version>
    </dependency>

    <!-- Kafka Clients -->
    <dependency>
        <groupId>org.apache.kafka</groupId>
        <artifactId>kafka-clients</artifactId>
        <version>${kafka.version}</version>
    </dependency>
</dependencies>

<build>
    <!-- Extension to automatically detect OS architecture (needed for protoc compiler download) -->
    <extensions>
        <extension>
            <groupId>kr.motd.maven</groupId>
            <artifactId>os-maven-plugin</artifactId>
            <version>1.7.1</version>
        </extension>
    </extensions>

    <plugins>
        <!-- Protobuf Code Generator Plugin -->
        <plugin>
            <groupId>org.xolstice.maven.plugins</groupId>
            <artifactId>protobuf-maven-plugin</artifactId>
            <version>0.6.1</version>
            <configuration>
                <protocArtifact>
                    com.google.protobuf:protoc:${protobuf.version}:exe:${os.detected.classifier}
                </protocArtifact>
            </configuration>
            <executions>
                <execution>
                    <id>protobuf</id>
                    <goals>
                        <goal>compile</goal>
                    </goals>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

### Compiling and Generating Sources
Place `.proto` files inside:
```text
src/main/proto/
```

Run the compilation command:
```bash
mvn clean compile
```

This compiles all `.proto` schemas and places generated Java source files under:
```text
target/generated-sources/protobuf/java/
```

---

## 4. Serialization & Deserialization APIs

Once classes are generated, you manipulate objects using the built-in Builder pattern:

### 1. Object Instantiation (Builder Pattern)
```java
Customer customer = Customer.newBuilder()
        .setCustomerId("C100")
        .setName("John Doe")
        .setEmail("john.doe@gmail.com")
        .setCreditLimit(100000.0)
        .build();
```

### 2. Low-Level Serialization APIs
Protobuf classes include standard methods to serialize objects directly to byte arrays or streams:
* **To Byte Array:**
  ```java
  byte[] rawBytes = customer.toByteArray();
  ```
* **To OutputStream (e.g., File or Socket):**
  ```java
  FileOutputStream out = new FileOutputStream("customer.bin");
  customer.writeTo(out);
  ```

### 3. Low-Level Deserialization APIs
* **From Byte Array:**
  ```java
  Customer deserialized = Customer.parseFrom(rawBytes);
  ```
* **From InputStream:**
  ```java
  FileInputStream in = new FileInputStream("customer.bin");
  Customer deserialized = Customer.parseFrom(in);
  ```

---

## 5. Integrating Protobuf with Kafka

There are two primary architectural patterns to exchange Protobuf events inside Kafka:

### Pattern A: Custom Serialization (No Schema Registry)
In this pattern, applications handle byte conversion using a custom Kafka Serializer and Deserializer. This avoids setting up a Confluent Schema Registry but shifts schema governance overhead directly onto developer coordination.

```text
+--------------+        +--------------------+        +-------------+
| Java Message | -----> | Custom Serializer  | -----> | Kafka Topic |
| (Customer)   |        | (.toByteArray())   |        |             |
+--------------+        +--------------------+        +-------------+
```

#### Custom Serializer (`ProtobufSerializer.java`)
```java
package co.vinod.protobuf.serializer;

import co.vinod.protobuf.model.Customer;
import org.apache.kafka.common.serialization.Serializer;
import java.util.Map;

public class ProtobufSerializer implements Serializer<Customer> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public byte[] serialize(String topic, Customer data) {
        if (data == null) {
            return null;
        }
        return data.toByteArray(); // Converts Protobuf object directly to bytes
    }

    @Override
    public void close() {}
}
```

#### Custom Deserializer (`ProtobufDeserializer.java`)
```java
package co.vinod.protobuf.serializer;

import co.vinod.protobuf.model.Customer;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.common.serialization.Deserializer;
import java.util.Map;

public class ProtobufDeserializer implements Deserializer<Customer> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {}

    @Override
    public Customer deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return Customer.parseFrom(data); // Parses bytes back into Customer object
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException("Failed to deserialize Protobuf message", e);
        }
    }

    @Override
    public void close() {}
}
```

---

### Pattern B: Schema Registry Integration (Confluent Serializers)
Confluent Schema Registry (v5.5+) supports Protobuf. It registers the schema structure and prepends a **5-byte header** (Magic Byte `0x00` + 4-byte Schema ID) to the binary message, exactly like it does for Avro.

```text
+--------------+        +-----------------------------+        +-------------+
| Java Message | -----> | Confluent Serializer        | -----> | Kafka Topic |
| (Customer)   |        | (Checks registry + ID header) |      |             |
+--------------+        +-----------------------------+        +-------------+
```

#### Maven Dependency for Confluent Protobuf Serializer:
```xml
<dependency>
    <groupId>io.confluent</groupId>
    <artifactId>kafka-protobuf-serializer</artifactId>
    <version>${confluent.version}</version>
</dependency>
```

#### Configurations:
```properties
producer.value.serializer=io.confluent.kafka.serializers.protobuf.KafkaProtobufSerializer
consumer.value.deserializer=io.confluent.kafka.serializers.protobuf.KafkaProtobufDeserializer
schema.registry.url=http://localhost:8081
```

---

## 6. Complete Programming Examples (Pattern A)

Here are the complete Kafka Producer and Consumer codes mapping custom Protobuf serializers as configured in the `protobuf-basics` module.

### Kafka Producer Implementation (`CustomerProducer.java`)
```java
package co.vinod.protobuf.producer;

import co.vinod.protobuf.model.Customer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.Future;

public class CustomerProducer {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // Use the custom Protobuf Serializer class
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "co.vinod.protobuf.serializer.ProtobufSerializer");

        try (Producer<String, Customer> producer = new KafkaProducer<>(props)) {
            // Build the Protobuf event record
            Customer customer = Customer.newBuilder()
                    .setCustomerId("C100")
                    .setName("John Doe")
                    .setEmail("john.doe@gmail.com")
                    .setCreditLimit(100000.0)
                    .build();

            ProducerRecord<String, Customer> record = new ProducerRecord<>(
                    "customer-events",
                    customer.getCustomerId(),
                    customer
            );

            // Send synchronously to log confirmation details
            Future<RecordMetadata> future = producer.send(record);
            RecordMetadata metadata = future.get();

            System.out.printf(
                    "Message sent successfully%n" +
                    "Topic     : %s%n" +
                    "Partition : %d%n" +
                    "Offset    : %d%n",
                    metadata.topic(), metadata.partition(), metadata.offset()
            );
        }
    }
}
```

### Kafka Consumer Implementation (`CustomerConsumer.java`)
```java
package co.vinod.protobuf.consumer;

import co.vinod.protobuf.model.Customer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class CustomerConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "customer-group");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        
        // Use the custom Protobuf Deserializer class
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "co.vinod.protobuf.serializer.ProtobufDeserializer");

        try (Consumer<String, Customer> consumer = new KafkaConsumer<>(props)) {
            String topic = "customer-events";
            consumer.subscribe(Collections.singletonList(topic));
            System.out.println("Listening on topic: " + topic);

            while (true) {
                ConsumerRecords<String, Customer> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, Customer> record : records) {
                    Customer customer = record.value();
                    System.out.println("\n------------------------------------");
                    System.out.println("Partition    : " + record.partition());
                    System.out.println("Offset       : " + record.offset());
                    System.out.println("Customer ID  : " + customer.getCustomerId());
                    System.out.println("Name         : " + customer.getName());
                    System.out.println("Email        : " + customer.getEmail());
                    System.out.println("Credit Limit : " + customer.getCreditLimit());
                    System.out.println("------------------------------------");
                }
            }
        }
    }
}
```

---

## 7. Schema Evolution & Compatibility

Protobuf supports powerful schema evolution out of the box. Because messages are sent as serialized tags and values, binary parsing is highly resilient to schema changes.

### Core Evolution Rules:
1. **Never Change Tag Numbers:** The tag number is the identifier for the field in the binary encoding. Changing a tag (e.g., changing `name = 2` to `name = 5`) will corrupt parsing for any client using a different version.
2. **Adding Fields:**
   * You can add new fields to a message at any time. 
   * When an old consumer reads a message sent by a new producer, it encounters the new field tag. It does not crash; it simply ignores the field as an **unknown field**.
   * When a new consumer reads a message sent by an old producer, the field is missing. The new consumer initializes the missing field with the type's **default value** (e.g., empty string for `string`, `0` for numeric values, `false` for `bool`).
3. **Deleting Fields (The `reserved` Rule):**
   * If you delete a field, you must declare its tag and name as **`reserved`**.
   * This prevents future developers from reusing the same tag number or field name. Reusing a deleted tag number would cause old historical data in Kafka to be parsed incorrectly into the new field.
   * **Example of safe deletion:**
     ```protobuf
     message Customer {
       reserved 3; // Reserved the deleted field tag
       reserved "email"; // Reserved the deleted field name
       
       string customerId = 1;
       string name = 2;
       // email field was deleted from tag 3
       double creditLimit = 4;
     }
     ```
4. **Do Not Change Field Types:** Changing a field's data type (e.g., `int32` to `string`) is generally incompatible. Always create a new field with a new tag number if a data type change is required.

---

## 8. Troubleshooting & Common Pitfalls

### 1. Reusing Tag Numbers
* **Symptom:** Fields display garbage values, numbers map to names, or parsing crashes.
* **Cause:** A developer deleted a field and reused its tag number for a different field. When old payloads stored in Kafka partitions are read, the consumer tries to map the old data type to the new field definition.
* **Resolution:** Always use the `reserved` keyword when deleting a field.

### 2. Missing Default Values
* **Symptom:** In proto3, you cannot tell the difference between a field explicitly set to its default value (like `0` or `""`) and a field that was not set at all.
* **Cause:** In proto3, explicit field presence tracking was removed by default (i.e., fields do not have `hasFieldName()` methods in Java unless they are configured with the `optional` keyword).
* **Resolution:** If field presence tracking is required (i.e., distinguishing between `null` and `0`/`""`), prefix the field with `optional` (e.g., `optional string email = 3;`). This generates `hasEmail()` methods in Java.

### 3. Out-of-Sync Local Protoc Compiler
* **Symptom:** Compilation failures during `mvn compile` or IDE errors showing unresolved classes.
* **Cause:** Your local system lacks compile resources, or you forgot to run the build compilation after adding a field to a `.proto` file.
* **Resolution:** Run `mvn clean compile` to force the plugin to download the matching compiler (`protoc`) binary and regenerate the Java files.
