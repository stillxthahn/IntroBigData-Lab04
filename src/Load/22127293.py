from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

mongo_uri = "mongodb+srv://trungnghia24904:XakkiuYhdPEyAV5Q@cluster0.3q6zges.mongodb.net/crypto_db?retryWrites=true&w=majority"

spark = SparkSession.builder \
    .appName("BTC Price Streaming") \
    .config("spark.mongodb.output.uri", mongo_uri) \
    .getOrCreate()


# -------- THANH: CHANGE BOOSTRAP SERVER FROM 127.0.0.1 to container host name
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker:9092") \
    .option("subscribe", "btc-price-zscore") \
    .option("startingOffsets", "latest") \
    .load()

# Schema for JSON parsing
schema = StructType([
    StructField("timestamp", StringType()),
    StructField("symbol", StringType()),
    StructField("windows", ArrayType(StructType([
        StructField("window", StringType()),
        StructField("zscore_price", DoubleType())
    ])))
])

# Phân tích JSON
df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", schema).alias("data")) \
    .select("data.*")

# Xử lý dữ liệu trễ với watermark
df = df.withWatermark("timestamp", "10 seconds")

# Tách mảng windows thành các hàng riêng
df_exploded = df.select(
    col("timestamp"),
    col("symbol"),
    explode(col("windows")).alias("window_data")
).select(
    col("timestamp"),
    col("symbol"),
    col("window_data.window").alias("window"),
    col("window_data.zscore_price").alias("zscore_price")
)

# Danh sách các cửa sổ
windows = ["30s", "1m", "5m", "15m", "30m", "1h"]

# Hàm để ghi batch vào MongoDB
def write_to_mongodb(batch_df, batch_id):
    for window in windows:
        # Lọc dữ liệu cho cửa sổ hiện tại
        window_df = batch_df.filter(col("window") == window)
        if not window_df.isEmpty():
            # Ghi vào collection tương ứng
            window_df.writeStream \
                .format("mongodb") \
                .option("checkpointLocation", "/tmp/pyspark/") \
                .option("forceDeleteTempCheckpointLocation", "true") \
                .option("spark.mongodb.connection.uri", mongo_uri) \
                .option("spark.mongodb.database", "BigData") \
                .option("spark.mongodb.collection", "btc-price-zscore") \
                .outputMode("append") \
                .start()

# Ghi luồng vào MongoDB
query = df_exploded.writeStream \
    .foreachBatch(write_to_mongodb) \
    .start()

# Chờ luồng chạy
query.awaitTermination()