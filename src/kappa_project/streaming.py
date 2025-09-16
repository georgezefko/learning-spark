from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr
import os

kafka_bootstrap = os.environ.get("KAFKA_BROKERS", "localhost:9092")
connect_url = os.getenv("SPARK_CONNECT_URL", "sc://spark-connect:15002")
topic = "iot-telemetry"


# 1) Build local SparkSession
spark = (
    SparkSession.builder.remote(connect_url)
    .appName("LocalKafkaStreaming")
    .config("fs.s3a.threads.keepalivetime", "60000")  # 60 seconds = 60000 ms
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .config("fs.s3a.connection.establish.timeout", "30000")
    .config("fs.s3a.connection.timeout", "200000")
    .config("fs.s3a.multipart.purge.age", "86400000")
    .getOrCreate()
)


# 2) Read from your local Kafka
# Use host.docker.internal when running in devcontainer, localhost otherwise


df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap)
    .option("subscribe", topic)
    .option("startingOffsets", "earliest")
    .load()
)

# 3) Cast key/value to strings
df2 = df.select(
    col("topic"),
    expr("CAST(key AS STRING)").alias("key"),
    expr("CAST(value AS STRING)").alias("value"),
    col("timestamp"),
)

# For debugging
# --- Inspect Spark conf ---
# print("\n=== Spark conf (timeouts/ms/s3a) ===")
# for k, v in spark.sparkContext.getConf().getAll():
#     if ("s3a" in k) or k.endswith(".timeout") or k.endswith(".ms"):
#         print(f"{k} = {v}")

# # # --- Inspect Hadoop (S3A) conf ---
# # print("\n=== Hadoop conf (fs.s3a.*) ===")
# hc = spark._jsc.hadoopConfiguration()
# it = hc.iterator()
# while it.hasNext():
#     e = it.next()
#     k = e.getKey()
#     v = e.getValue()
#     if k.startswith("fs.s3a.") and v is not None:
#         print(f"{k} = {v}")


# 4) Write to console for testing
query = (
    df2.writeStream.format("console")
    .outputMode("append")
    .option("truncate", False)
    .option("checkpointLocation", "s3a://spark-demo/stream-1/checkpoints/")
    .start()
)

print("Streaming to console—press Ctrl+C to stop.")
query.awaitTermination()
