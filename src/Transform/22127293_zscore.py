from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, to_json, struct, collect_list, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, ArrayType
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka broker configuration
KAFKA_BROKER = "kafka-broker:9092"
checkpoint_base = "/tmp/checkpoint"

# Create Spark Session
spark = SparkSession.builder \
    .appName("BTC Price Z-Score Calculator") \
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
    .config("spark.sql.session.timeZone", "UTC") \
    .getOrCreate()

# Define schema for price data
price_schema = StructType([
    StructField("symbol", StringType(), False),
    StructField("price", StringType(), False),
    StructField("timestamp", StringType(), False)
])

# Define schema for moving statistics data
window_struct = StructType([
    StructField("window", StringType(), False),
    StructField("avg_price", DoubleType(), False),
    StructField("std_price", DoubleType(), False)
])

moving_stats_schema = StructType([
    StructField("timestamp", TimestampType(), False),
    StructField("symbol", StringType(), False),
    StructField("windows", ArrayType(window_struct), False)
])

# Read streaming data from Kafka for price updates
price_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "btc-price") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Read streaming data from Kafka for moving statistics
moving_stats_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", "btc-price-moving") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON data for price updates
parsed_price_df = price_df.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), price_schema)) \
    .select("data.*") \
    .withColumn("price", col("price").cast(DoubleType())) \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

# Parse JSON data for moving statistics
parsed_moving_stats_df = moving_stats_df.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), moving_stats_schema)) \
    .select(
        col("data.timestamp"),
        col("data.symbol"),
        col("data.windows")
    )

# Watermarking to handle late data
price_with_watermark = parsed_price_df.withWatermark("timestamp", "10 seconds")
moving_stats_with_watermark = parsed_moving_stats_df.withWatermark("timestamp", "10 seconds")

# Join the two streams based on timestamp and symbol
joined_df = price_with_watermark \
    .join(
        moving_stats_with_watermark,
        (price_with_watermark.timestamp == moving_stats_with_watermark.timestamp) & 
        (price_with_watermark.symbol == moving_stats_with_watermark.symbol),
        "inner"
    ) \
    .select(
        price_with_watermark.timestamp,
        price_with_watermark.symbol,
        price_with_watermark.price,
        moving_stats_with_watermark.windows
    )

# Explode the windows array to process each window separately
exploded_df = joined_df \
    .select(
        col("timestamp"),
        col("symbol"),
        col("price"),
        explode(col("windows")).alias("window_data")
    )

# Calculate Z-score for each window
zscore_df = exploded_df \
    .select(
        col("timestamp"),
        col("symbol"),
        col("window_data.window"),
        # Z-score = (price - avg) / std
        # Handle division by zero by using a default value when std is zero
        when(col("window_data.std_price") > 0, 
             (col("price") - col("window_data.avg_price")) / col("window_data.std_price"))
        .otherwise(lit(0.0)).alias("zscore_price")
    )

# Group by timestamp and symbol to create the array structure for output
grouped_zscore_df = zscore_df \
    .groupBy("timestamp", "symbol") \
    .agg(
        collect_list(
            struct(
                col("window"),
                col("zscore_price")
            )
        ).alias("windows")
    )

# Convert to JSON format for Kafka output
output_df = grouped_zscore_df.select(
    col("timestamp"),
    col("symbol"),
    col("windows"),
    to_json(
        struct(
            col("timestamp"),
            col("symbol"),
            col("windows")
        )
    ).alias("value")
)

# Write the output to Kafka
kafka_query = output_df \
    .select("value") \
    .writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("topic", "btc-price-zscore") \
    .option("checkpointLocation", f"{checkpoint_base}/zscore") \
    .start()

# Also write to console for debugging
console_query = output_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Wait for termination
spark.streams.awaitAnyTermination()

