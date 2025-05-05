from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("BTC Price Streaming") \
    .getOrCreate()

# -------- THANH: CHANGE BOOSTRAP SERVER FROM 127.0.0.1 to container host name
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \
    .option("subscribe", "btc-price-zscore") \
    .option("startingOffsets", "latest") \
    .load()