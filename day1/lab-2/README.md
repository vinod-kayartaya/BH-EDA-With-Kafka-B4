# Lab 2: Local CLI Client Installation & Core CLI Operations

This lab guide explains how to install the Apache Kafka command-line interface (CLI) client on your host computer (both macOS and Windows) and provides a cheat sheet of core CLI commands for working with your Kafka cluster.

We will use these local tools to connect to the 3-broker cluster we stood up in **Lab 1** (listening on host ports `localhost:29092`, `localhost:29093`, and `localhost:29094`).

---

## 1. Setting Up Kafka CLI Client

To run Kafka scripts natively from your terminal, you must download the binaries and append them to your system's execution path.

### Step 1: Download the Binaries
1. Go to the official Apache Kafka downloads page: [kafka.apache.org/downloads](https://kafka.apache.org/downloads)
2. Under the latest stable version, download the **Binary download** (choose the Scala version, e.g., `Scala 2.13` -> `kafka_2.13-3.7.0.tgz`).
3. Extract the downloaded archive:
   * **macOS:** Double-click or run `tar -xzf kafka_2.13-3.7.0.tgz` in terminal.
   * **Windows:** Use a tool like 7-Zip to extract the `.tgz` and subsequent `.tar` file.

Move the extracted folder to a permanent location:
* **macOS:** `/Users/Shared/kafka` (or in your user home folder `~/kafka`)
* **Windows:** `C:\kafka`

---

### Step 2: Configure System PATH Environment Variable

Exposing the `bin` folder to the system path lets you run commands directly without typing full paths (e.g. typing `kafka-topics` instead of `~/kafka/bin/kafka-topics.sh`).

#### A. Instructions for macOS (Zsh shell)
1. Open your terminal.
2. Edit your profile configuration file (usually `.zshrc`):
   ```bash
   nano ~/.zshrc
   ```
3. Add the following line at the bottom of the file (replace `/Users/Shared/kafka` with your actual path):
   ```bash
   export PATH="$PATH:/Users/Shared/kafka/bin"
   ```
4. Save and exit (press `Ctrl+O`, `Enter`, then `Ctrl+X`).
5. Reload the configuration in your active shell:
   ```bash
   source ~/.zshrc
   ```
6. Verify it works by running:
   ```bash
   kafka-topics --version
   ```

#### B. Instructions for Windows (Command Prompt / PowerShell)
Windows commands are stored in a special subfolder: `C:\kafka\bin\windows` containing command scripts (`.bat`).
1. Press the **Windows Key** and type `env`. Select **Edit the system environment variables**.
2. Click the **Environment Variables...** button at the bottom right.
3. Under **User variables** (or **System variables** if you have Admin access), select the variable named **`Path`** (or `PATH`) and click **Edit...**.
4. Click **New** and paste the path to your Windows binary folder:
   ```text
   C:\kafka\bin\windows
   ```
5. Click **OK** on all windows to apply.
6. Open a **new** Command Prompt or PowerShell window and test:
   ```cmd
   kafka-topics.bat --version
   ```

---

## 2. Core Kafka CLI Commands Reference

> [!NOTE]
> * **macOS / Linux users:** Run scripts using their standard names (e.g., `kafka-topics`).
> * **Windows users:** Append `.bat` to the script name (e.g., `kafka-topics.bat`).
> * **Bootstrap Connection:** We use `--bootstrap-server localhost:29092` to connect.

### A. Topic Administration

#### 1. Create a Topic
Create a topic named `order-events` with 3 partitions and a replication factor of 3 (distributing replicas across the 3 brokers we created in Lab 1):
```bash
# macOS / Linux
kafka-topics --create --bootstrap-server localhost:29092 --topic order-events --partitions 3 --replication-factor 3

# Windows
kafka-topics.bat --create --bootstrap-server localhost:29092 --topic order-events --partitions 3 --replication-factor 3
```

#### 2. List All Active Topics
Verify which topics are currently configured in the cluster metadata:
```bash
# macOS
kafka-topics --list --bootstrap-server localhost:29092

# Windows
kafka-topics.bat --list --bootstrap-server localhost:29092
```

#### 3. Describe Topic Topologies
Inspect the partition layouts, replication configurations, in-sync replicas (ISR), and leader assignments:
```bash
# macOS
kafka-topics --describe --bootstrap-server localhost:29092 --topic order-events

# Windows
kafka-topics.bat --describe --bootstrap-server localhost:29092 --topic order-events
```

---

### B. Producing Events (Console Producer)

#### 1. Publish Simple Messages
Start a console producer to stream messages line-by-line. Press `Enter` to emit each message:
```bash
# macOS
kafka-console-producer --bootstrap-server localhost:29092 --topic order-events

# Windows
kafka-console-producer.bat --bootstrap-server localhost:29092 --topic order-events
```
*To exit the interactive shell, press `Ctrl+C`.*

#### 2. Publish Messages with Partitioning Keys
By default, keyless messages are routed round-robin. To route messages to specific partitions based on a hashing key (e.g., matching a client ID or order ID):
```bash
# macOS
kafka-console-producer --bootstrap-server localhost:29092 --topic order-events --property parse.key=true --property key.separator=":"

# Windows
kafka-console-producer.bat --bootstrap-server localhost:29092 --topic order-events --property parse.key=true --property key.separator=":"
```
*Example input: `order_101:{"status":"created"}`. The key is `order_101`, and the value is the JSON payload.*

---

### C. Consuming Events (Console Consumer)

#### 1. Create a Consumer Group Member
Run the following command in a terminal. The `--group` argument registers this consumer with a specific group ID (`retail-processors`):
```bash
# macOS
kafka-console-consumer --bootstrap-server localhost:29092 --topic order-events --group retail-processors --from-beginning

# Windows
kafka-console-consumer.bat --bootstrap-server localhost:29092 --topic order-events --group retail-processors --from-beginning
```
* **Group Load Balancing:** Open a **second terminal tab** and run the exact same command. You will have two consumer instances in the same group. When you publish messages, Kafka will automatically partition the workload, sending some messages to the first terminal and others to the second.

#### 2. Create an Independent Consumer (Fan-out Pattern)
If you do not specify a `--group` flag, Kafka will generate a unique, randomized temporary group ID. This allows this consumer to receive **all** partitions and messages independently from other processors:
```bash
# macOS
kafka-console-consumer --bootstrap-server localhost:29092 --topic order-events --from-beginning

# Windows
kafka-console-consumer.bat --bootstrap-server localhost:29092 --topic order-events --from-beginning
```

---

### D. Consumer Group Troubleshooting

#### 1. List Active Consumer Groups
```bash
# macOS
kafka-consumer-groups --bootstrap-server localhost:29092 --list

# Windows
kafka-consumer-groups.bat --bootstrap-server localhost:29092 --list
```

#### 2. Describe Group Health & Lag Metrics
This is the most critical monitoring tool for checking consumer offsets and **Consumer Lag** (how far behind the consumer is from the producer's latest offset):
```bash
# macOS
kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group retail-processors

# Windows
kafka-consumer-groups.bat --bootstrap-server localhost:29092 --describe --group retail-processors
```
This prints a table showing:
* `LOG-END-OFFSET`: The latest offset written by producers.
* `CURRENT-OFFSET`: The latest offset committed by this consumer.
* `LAG`: The number of unread messages remaining in the partition queue.

---

## 3. Exercises for Day 1

Try these steps to verify your local installation:
1. Start your Docker cluster from **Lab 1**.
2. Open three terminals:
   * **Terminal 1:** Start a console producer using `kafka-console-producer`.
   * **Terminal 2:** Start group consumer instance 1 using `kafka-console-consumer --group classroom-group`.
   * **Terminal 3:** Start group consumer instance 2 using `kafka-console-consumer --group classroom-group`.
3. Publish 10 messages in Terminal 1. Notice how the messages are divided between Terminal 2 and Terminal 3.
4. Close Terminal 3. Publish 5 more messages in Terminal 1. Observe how Terminal 2 automatically takes over the remaining partitions and receives all messages (Rebalancing).
5. Run the describe command to review the lag:
   `kafka-consumer-groups --bootstrap-server localhost:29092 --describe --group classroom-group`
