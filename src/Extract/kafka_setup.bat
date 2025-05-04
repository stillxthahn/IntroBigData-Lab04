@echo off

docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Docker is not installed. Please install Docker before running this script.
    pause
    exit /b
)

echo "---Pulling Kafka image..."
docker pull apache/kafka
echo "------Kafka image pulled."

echo "---Starting Kafka container..."
docker run -d -p 9092:9092 --name kafka apache/kafka:latest
echo "------Kafka container started."

echo "---Waiting for the container to start properly..."
timeout /t 10 >nul
echo "------Container should now be ready."

echo "---Entering Kafka container..."
docker exec --workdir /opt/kafka/bin/ -it kafka sh
echo "------Inside Kafka container."

echo "---Creating Kafka topic: btc-price"
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic btc-price --partitions 1 --replication-factor 1
echo "------Kafka topic 'btc-price' has been created."

echo "---Creating Kafka topic: btc-price-moving"
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic btc-price-moving --partitions 1 --replication-factor 1
echo "------Kafka topic 'btc-price-moving' has been created."

echo "---Creating Kafka topic: btc-price-zscore"
./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic btc-price-zscore --partitions 1 --replication-factor 1
echo "------Kafka topic 'btc-price-zscore' has been created."