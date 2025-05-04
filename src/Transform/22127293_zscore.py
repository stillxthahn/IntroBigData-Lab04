from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

spark = SparkSession.builder \
    .appName("BTC Price Streaming") \
    .getOrCreate()

schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("timestamp", TimestampType())

df_price = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "btc-price") \
    .option("startingOffsets", "latest") \
    .load()

df_moving = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "btc-price-moving") \
    .option("startingOffsets", "latest") \
    .load()

