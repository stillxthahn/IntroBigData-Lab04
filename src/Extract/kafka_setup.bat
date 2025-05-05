@echo off

echo "---Creating Kafka topic: btc-price"
docker exec --workdir /opt/kafka/bin/ kafka-broker ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic btc-price --partitions 1 --replication-factor 1

echo "---Creating Kafka topic: btc-price-moving"
docker exec --workdir /opt/kafka/bin/ kafka-broker ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic btc-price-moving --partitions 1 --replication-factor 1

echo "---Creating Kafka topic: btc-price-zscore"
docker exec --workdir /opt/kafka/bin/ kafka-broker ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic btc-price-zscore --partitions 1 --replication-factor 1