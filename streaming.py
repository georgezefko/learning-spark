from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

# 1) Build local SparkSession
spark = (
    SparkSession.builder
      .appName("LocalKafkaStreaming")
      .master("local[*]")
      .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")
      .config("spark.sql.streaming.checkpointLocation", "/tmp/spark/checkpoints")
      .getOrCreate()
      
)

# 2) Read from your local Kafka
kafka_bootstrap = "localhost:9092"
topic = "iot-telemetry"

df = (
    spark.readStream
         .format("kafka")
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
    col("timestamp")
)

# 4) Write to console for testing
query = (
    df2.writeStream
       .format("console")
       .outputMode("append")
       .option("truncate", False)
       .start()
)

print("Streaming to console—press Ctrl+C to stop.")
query.awaitTermination()