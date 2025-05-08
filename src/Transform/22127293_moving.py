from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, stddev, to_json, struct, lit, when
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
import uuid
import logging
# Create Spark Session
spark = SparkSession.builder \
    .appName("BTC Price Moving Statistics") \
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR") 
# Define schema for incoming data
schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", StringType()) \
    .add("timestamp", StringType())

# Read streaming data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \
    .option("subscribe", "btc-price") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON data
parsed_df = df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), schema).alias("data")) \
    .select("data.*")


# In ra terminal
console_query = parsed_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()


# Convert price to double and timestamp to timestamp type
formatted_df = parsed_df \
    .select(
        col("symbol"),
        col("price").cast("double").alias("price"),
        col("timestamp").cast("timestamp").alias("timestamp")
    )

# In ra terminal
console_query = formatted_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Add watermark to handle late data
df_with_watermark = formatted_df.withWatermark("timestamp", "10 seconds")

# Process 30-second window
win_30s = df_with_watermark \
    .groupBy(window("timestamp", "30 seconds"), "symbol") \
    .agg(
        avg("price").alias("avg_price"),
        stddev("price").alias("std_price")
    ) \
    .select(
        col("window.end").alias("timestamp"),
        col("symbol"),
        lit("30s").alias("window"),
        col("avg_price"),
        when(col("std_price").isNull(), lit(0.0)).otherwise(col("std_price")).alias("std_price")
    )

# In ra terminal
console_query = win_30s.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Process 1-minute window
win_1m = df_with_watermark \
    .groupBy(window("timestamp", "1 minute"), "symbol") \
    .agg(
        avg("price").alias("avg_price"),
        stddev("price").alias("std_price")
    ) \
    .select(
        col("window.end").alias("timestamp"),
        col("symbol"),
        lit("1m").alias("window"),
        col("avg_price"),
        when(col("std_price").isNull(), lit(0.0)).otherwise(col("std_price")).alias("std_price")
    )

# In ra terminal
console_query = win_1m.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Process 5-minute window
win_5m = df_with_watermark \
    .groupBy(window("timestamp", "5 minutes"), "symbol") \
    .agg(
        avg("price").alias("avg_price"),
        stddev("price").alias("std_price")
    ) \
    .select(
        col("window.end").alias("timestamp"),
        col("symbol"),
        lit("5m").alias("window"),
        col("avg_price"),
        when(col("std_price").isNull(), lit(0.0)).otherwise(col("std_price")).alias("std_price")
    )


# In ra terminal
console_query = win_5m.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Process 15-minute window
win_15m = df_with_watermark \
    .groupBy(window("timestamp", "15 minutes"), "symbol") \
    .agg(
        avg("price").alias("avg_price"),
        stddev("price").alias("std_price")
    ) \
    .select(
        col("window.end").alias("timestamp"),
        col("symbol"),
        lit("15m").alias("window"),
        col("avg_price"),
        when(col("std_price").isNull(), lit(0.0)).otherwise(col("std_price")).alias("std_price")
    )


# In ra terminal
console_query = win_15m.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Process 30-minute window
win_30m = df_with_watermark \
    .groupBy(window("timestamp", "30 minutes"), "symbol") \
    .agg(
        avg("price").alias("avg_price"),
        stddev("price").alias("std_price")
    ) \
    .select(
        col("window.end").alias("timestamp"),
        col("symbol"),
        lit("30m").alias("window"),
        col("avg_price"),
        when(col("std_price").isNull(), lit(0.0)).otherwise(col("std_price")).alias("std_price")
    )


# In ra terminal
console_query = win_30m.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()


# Process 1-hour window
win_1h = df_with_watermark \
    .groupBy(window("timestamp", "1 hour"), "symbol") \
    .agg(
        avg("price").alias("avg_price"),
        stddev("price").alias("std_price")
    ) \
    .select(
        col("window.end").alias("timestamp"),
        col("symbol"),
        lit("1h").alias("window"),
        col("avg_price"),
        when(col("std_price").isNull(), lit(0.0)).otherwise(col("std_price")).alias("std_price")
    )

# In ra terminal
console_query = win_1h.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Union all statistics
all_stats = win_30s.union(win_1m).union(win_5m).union(win_15m).union(win_30m).union(win_1h)

# Format for output
output_df = all_stats.select(
    col("timestamp"),
    col("symbol"),
    col("window"),
    col("avg_price"),
    col("std_price"),
    to_json(struct(
        col("timestamp"),
        col("symbol"),
        col("window"),
        col("avg_price"),
        col("std_price")
    )).alias("value")
)

# Write to console for debugging
console_query = output_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Write to Kafka
kafka_query = output_df \
    .select("value") \
    .writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \
    .option("topic", "btc-price-moving") \
    .option("checkpointLocation", f"/tmp/checkpoints/moving_{uuid.uuid4()}") \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()