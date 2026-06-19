#!/bin/zsh

# Color output helpers
green() { echo -e "\e[32m$1\e[0m" }
yellow() { echo -e "\e[33m$1\e[0m" }
red() { echo -e "\e[31m$1\e[0m" }

JAR_PATH="./kafka-metrics-demo-1.0-SNAPSHOT.jar"

# 1. Check if JAR exists, build if missing
if [[ ! -f "$JAR_PATH" ]]; then
    yellow "JAR file not found. Compiling the project using Maven..."
    (cd kafka-producer-consumer && mvn clean package)
    if [[ $? -ne 0 ]]; then
        red "Error: Maven build failed. Please check the Java/Maven setup."
        exit 1
    fi
fi

green "✅ Shaded JAR verified: $JAR_PATH"

# 2. Run Topic Setup
yellow "\n1. Running Topic Setup..."
java -cp "$JAR_PATH" com.kafkademo.config.TopicSetup
if [[ $? -ne 0 ]]; then
    red "Error: Topic setup failed. Is the Kafka cluster running?"
    exit 1
fi
green "✅ Topics configured successfully."

# Define cleanup function for graceful shutdown
cleanup() {
    yellow "\nStopping producers and consumers..."
    if [[ -n "$PRODUCER_PID" ]]; then
        kill "$PRODUCER_PID" 2>/dev/null
        wait "$PRODUCER_PID" 2>/dev/null
    fi
    if [[ -n "$CONSUMER_PID" ]]; then
        kill "$CONSUMER_PID" 2>/dev/null
        wait "$CONSUMER_PID" 2>/dev/null
    fi
    green "Shutdown complete. Have a great day!"
    exit 0
}

# Trap SIGINT (Ctrl+C) and SIGTERM
trap cleanup SIGINT SIGTERM

# 3. Start MultiTopicProducer
yellow "\n2. Starting MultiTopicProducer (producing events)..."
# Allow passing custom rate multiplier from CLI (default is 2.5)
RATE=${1:-2.5}
java -Drate.multiplier="$RATE" -cp "$JAR_PATH" com.kafkademo.producer.MultiTopicProducer &
PRODUCER_PID=$!
yellow "   → MultiTopicProducer started in background (PID: $PRODUCER_PID)"

# 4. Wait 15 seconds so messages pile up (generating lag for slow consumers)
yellow "\n3. Waiting 15 seconds for messages to populate partitions (generating initial lag)..."
for i in {15..1}; do
    echo -n "$i... "
    sleep 1
done
echo ""

# 5. Start MultiGroupConsumer
yellow "\n4. Starting MultiGroupConsumer..."
java -cp "$JAR_PATH" com.kafkademo.consumer.MultiGroupConsumer &
CONSUMER_PID=$!
yellow "   → MultiGroupConsumer started in background (PID: $CONSUMER_PID)"

green "\n🚀 Both programs are now running in the background!"
yellow "Open Grafana (http://localhost:3000) and view:"
yellow "  - 'Kafka Consumer Lag' to see slow/reporting groups lag behind."
yellow "  - 'Kafka Topic' to see throughput rates."
yellow "  - 'Kafka Broker' to see request latencies and JVM metrics."
echo ""
yellow "Press [Ctrl+C] to stop both programs and exit."

# Wait for background processes to keep script alive
wait
