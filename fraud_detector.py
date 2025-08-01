from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# Initialize Spark
spark = SparkSession.builder \
    .appName("FraudDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.streaming.checkpointLocation", "./checkpoint") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define schema
schema = StructType([
    StructField("user_id", StringType()),
    StructField("transaction_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("timestamp", StringType()),
    StructField("location", StringType()),
    StructField("method", StringType())
])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load()

# Parse JSON and add timestamp
transactions = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*") \
.withColumn("event_time", to_timestamp(col("timestamp"))) \
.withWatermark("event_time", "1 minute")

# Fraud Detection Rules

# Rule 1: High-value transactions (>1000)
high_value = transactions.filter(col("amount") > 1000) \
    .withColumn("fraud_type", lit("high_value")) \
    .withColumn("reason", concat(lit("High amount: $"), col("amount")))

# Rule 2: Multiple transactions from same user in 1 minute
user_frequency = transactions \
    .groupBy(
        col("user_id"),
        window(col("event_time"), "1 minute")
    ) \
    .count() \
    .filter(col("count") > 3) \
    .select(
        col("user_id"),
        col("window.start").alias("window_start"),
        col("count")
    ) \
    .withColumn("fraud_type", lit("high_frequency")) \
    .withColumn("reason", concat(lit("Multiple transactions: "), col("count"), lit(" in 1 minute")))

# Rule 3: Multiple locations in 5 minutes
location_anomaly = transactions \
    .groupBy(
        col("user_id"),
        window(col("event_time"), "5 minutes")
    ) \
    .agg(countDistinct("location").alias("location_count")) \
    .filter(col("location_count") > 2) \
    .select(
        col("user_id"),
        col("window.start").alias("window_start"),
        col("location_count")
    ) \
    .withColumn("fraud_type", lit("location_anomaly")) \
    .withColumn("reason", concat(lit("Multiple locations: "), col("location_count"), lit(" in 5 minutes")))

# Output to console
console_query = high_value.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Output to Parquet
parquet_query = high_value.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", "./fraud_output") \
    .start()

# Output to Kafka fraud-alerts topic
kafka_query = high_value.select(
    to_json(struct("*")).alias("value")
).writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "fraud-alerts") \
    .outputMode("append") \
    .start()

# Wait for termination
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("Stopping streams...")
    spark.stop()