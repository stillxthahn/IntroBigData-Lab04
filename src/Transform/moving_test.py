from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, stddev, to_json, struct, array, lit, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, ArrayType
import pyspark.sql.functions as F
import os

KAFKA_BROKER = os.environ.get("KAFKA_BROKER")
spark = SparkSession.builder \
    .appName("BTC Price Moving Stats") \
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
    .getOrCreate()


schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", StringType()) \
    .add("timestamp", StringType())  

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \
    .option("subscribe", "btc-price") \
    .option("startingOffsets", "latest") \
    .load()

# bytes -> string -> JSON
json_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")  


# In ra terminal
console_query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

console_query.awaitTermination()


