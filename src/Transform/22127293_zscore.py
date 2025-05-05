from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr, when, to_json, struct, explode
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType, ArrayType, StructField

spark = SparkSession.builder \
    .appName("BTC Price Z-Score Calculator") \
    .getOrCreate()

# Define schema for the price data
price_schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("timestamp", TimestampType())

# Define schema for the moving stats data
window_stats_schema = StructType([
    StructField("window", StringType()),
    StructField("avg_price", DoubleType()),
    StructField("std_price", DoubleType())
])

moving_schema = StructType() \
    .add("timestamp", TimestampType()) \
    .add("symbol", StringType()) \
    .add("windows", ArrayType(window_stats_schema))

# Read data from btc-price Kafka topic

# -------- THANH: CHANGE BOOSTRAP SERVER FROM 127.0.0.1 to container host name
df_price = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \
    .option("subscribe", "btc-price") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the price data JSON
parsed_price_df = df_price.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), price_schema).alias("data")) \
    .select("data.*")

# Ensure price is treated as double
parsed_price_df = parsed_price_df.withColumn("price", col("price").cast("double"))

# Read data from btc-price-moving Kafka topic
df_moving = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("subscribe", "btc-price-moving") \
    .option("startingOffsets", "latest") \
    .load()

# Parse the moving stats data JSON
parsed_moving_df = df_moving.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), moving_schema).alias("data")) \
    .select("data.*")

# Join the two streams based on timestamp and symbol
# First, explode the windows array to have one row per window
exploded_moving_df = parsed_moving_df \
    .withWatermark("timestamp", "10 seconds") \
    .select(
        col("timestamp"),
        col("symbol"),
        explode(col("windows")).alias("window_data")
    ) \
    .select(
        col("timestamp"),
        col("symbol"),
        col("window_data.window").alias("window"),
        col("window_data.avg_price").alias("avg_price"),
        col("window_data.std_price").alias("std_price")
    )

# Join with a 10-second tolerance for late data
joined_df = parsed_price_df \
    .withWatermark("timestamp", "10 seconds") \
    .join(
        exploded_moving_df,
        (parsed_price_df.timestamp == exploded_moving_df.timestamp) &
        (parsed_price_df.symbol == exploded_moving_df.symbol),
        "inner"
    )

# Calculate Z-score for each window
# Z-score = (price - avg_price) / std_price
zscore_df = joined_df.select(
    joined_df.timestamp,
    joined_df.symbol,
    joined_df.window,
    joined_df.price,
    joined_df.avg_price,
    joined_df.std_price,
    when(col("std_price") > 0, (col("price") - col("avg_price")) / col("std_price"))
    .otherwise(0.0)
    .alias("zscore_price")
)

# Group by timestamp and symbol to create the required JSON structure
output_df = zscore_df \
    .groupBy("timestamp", "symbol") \
    .agg(
        expr("collect_list(struct(window, zscore_price))").alias("windows")
    )

# Convert to JSON for Kafka output
kafka_output_df = output_df.select(
    to_json(struct("timestamp", "symbol", "windows")).alias("value")
)

# Write to Kafka
query = kafka_output_df.writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
    .option("topic", "btc-price-zscore") \
    .option("checkpointLocation", "/tmp/checkpoint/zscore") \
    .start()

# Also output to console for debugging
console_query = output_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Wait for termination
query.awaitTermination()

