#!/bin/zsh

# 1. Get the active local IP address on macOS
IP_ADDRESS=$(ipconfig getifaddr en0 || ipconfig getifaddr en1)

if [ -z "$IP_ADDRESS" ]; then
    echo "❌ Error: Could not detect an active IP address."
    exit 1
fi

echo "🌐 Detected IP Address: $IP_ADDRESS"

# 2. Update the KAFKA_BROKER_IP variable in the .env file
# Works natively with macOS sed (requires the empty string '' for in-place edits)
sed -i '' "s/^KAFKA_BROKER_IP=.*/KAFKA_BROKER_IP=$IP_ADDRESS/" .env

echo "✅ Updated .env file."

# 3. Start Docker Compose
echo "🐳 Starting Docker Compose..."
docker compose up -d
