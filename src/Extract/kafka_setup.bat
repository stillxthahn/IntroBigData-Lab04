@REM @echo off

@REM docker --version >nul 2>&1
@REM if %errorlevel% neq 0 (
@REM     echo Docker is not installed. Please install Docker before running this script.
@REM     pause
@REM     exit /b
@REM )

@REM echo "---Pulling Kafka image..."
@REM docker pull apache/kafka
@REM echo "------Kafka image pulled."

@REM echo "---Starting Kafka container..."
@REM docker run -d -p 9092:9092 --name kafka apache/kafka:latest
@REM echo "------Kafka container started."

@REM echo "---Waiting for the container to start properly..."
@REM timeout /t 10 >nul
@REM echo "------Container should now be ready."

echo "---Entering Kafka container..."
docker exec --workdir /opt/kafka/bin/ -it kafka-broker sh
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