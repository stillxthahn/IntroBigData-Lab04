from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, stddev, to_json, struct, array, lit, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, ArrayType
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("BTC Price Moving Stats") \
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
    .getOrCreate()

# Define schema for the incoming data
schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("timestamp", TimestampType())

# Read data from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "btc-price") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the incoming JSON data
parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Cast price to double to ensure it's treated as a numeric value
parsed_df = parsed_df.withColumn("price", col("price").cast("double"))

# Define window durations
window_durations = [
    ("30s", "30 seconds"),
    ("1m", "1 minute"),
    ("5m", "5 minutes"),
    ("15m", "15 minutes"),
    ("30m", "30 minutes"),
    ("1h", "1 hour")
]

# Create DataFrames for each window duration
window_dfs = []
for window_name, window_duration in window_durations:
    # Calculate moving average and standard deviation for each window
    window_df = parsed_df \
        .withWatermark("timestamp", "10 seconds") \
        .groupBy(
            window(col("timestamp"), window_duration, window_duration),
            col("symbol")
        ) \
        .agg(
            avg("price").alias(f"avg_price_{window_name}"),
            stddev("price").alias(f"std_price_{window_name}")
        ) \
        .select(
            col("window.end").alias("timestamp"),
            col("symbol"),
            lit(window_name).alias("window_name"),
            col(f"avg_price_{window_name}").alias("avg_price"),
            col(f"std_price_{window_name}").alias("std_price")
        )
    
    window_dfs.append(window_df)

# Union all window DataFrames
if len(window_dfs) > 1:
    result_df = window_dfs[0]
    for df in window_dfs[1:]:
        result_df = result_df.union(df)
else:
    result_df = window_dfs[0]

# Group by timestamp and symbol to create the required JSON structure
grouped_df = result_df \
    .groupBy("timestamp", "symbol") \
    .agg(
        F.collect_list(
            F.struct(
                F.col("window_name").alias("window"),
                F.col("avg_price"),
                F.col("std_price")
            )
        ).alias("windows")
    )

# Convert to the required output format
output_df = grouped_df.select(
    col("timestamp"),
    col("symbol"),
    col("windows")
)

# Convert to JSON for Kafka output
kafka_output_df = output_df.select(
    to_json(struct("timestamp", "symbol", "windows")).alias("value")
)

# Write to Kafka - change output mode to 'complete'
query = kafka_output_df.writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("topic", "btc-price-moving") \
    .option("checkpointLocation", "/tmp/checkpoint/moving") \
    .start()

# Also output to console for debugging
console_query = output_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Wait for termination
query.awaitTermination()
