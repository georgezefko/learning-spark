from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import os


# =======================
# Config
# =======================
CONNECT_URL = os.getenv("SPARK_CONNECT_URL", "sc://spark-connect:15002")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BROKERS", "localhost:9092")
KAFKA_TOPIC = "iot-telemetry"

STARROCKS_FE = "starrocks-fe:8030"  # FE HTTP port for stream load
SR_JDBC_URL = (
    "jdbc:mysql://starrocks-fe-0:9030/kappa_analytics"  # FE MySQL port for JDBC reads
)
SR_DB = "kappa_analytics"
SR_TABLE = "fact_telemetry_5min"


CHECKPOINT_DIR = "s3a://spark-demo/stream-1/checkpoints/"
TRIGGER_EVERY = "30 seconds"  # tweak for demo latency
WATERMARK = "3 minutes"  # per your 2–3 min ask


# 1) Build local SparkSession
spark = (
    SparkSession.builder.remote(CONNECT_URL)
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

# Define the schema of payloads

telemetry_schema = T.StructType(
    [
        T.StructField("device_id", T.StringType(), False),
        T.StructField("timestamp", T.StringType(), False),
        T.StructField("temperature", T.DoubleType(), True),
    ]
)


# 2) Read from your local Kafka
# Use host.docker.internal when running in devcontainer, localhost otherwise
# Name the streams bronze to show that they are the first ingested data

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    # .option("failOnDataLoss", False)
    .load()
    # .select(F.from_json(F.col("value").cast("string"), telemetry_schema).alias("j"))
    # .select("j.*")
    # .withColumn("ts", F.to_timestamp("timestamp"))
)

telemetry = (
    raw.select(F.from_json(F.col("value").cast("string"), telemetry_schema).alias("j"))
    .select("j.*")
    # robust timestamp parse: try explicit ISO pattern, fall back to default
    .withColumn(
        "event_time",
        F.coalesce(
            F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
            F.to_timestamp("timestamp"),
        ),
    )
    .drop("timestamp")
)


# Load static dim (StarRocks via JDBC) & broadcast
# =======================
dim_device = (
    spark.read.format("jdbc")
    .option("url", SR_JDBC_URL)
    .option("dbtable", "dim_device")
    # .option("user", SR_USER)
    # .option("password", SR_PASS)
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .load()
    .select("device_id", "temp_anomaly_threshold", "location_id", "model")
)
dim_b = F.broadcast(dim_device)


# =======================
# 5-min window + 3-min watermark
# =======================
agg5 = (
    telemetry.withWatermark("event_time", WATERMARK)
    .dropDuplicates(["device_id", "event_time"])
    .groupBy(F.window("event_time", "5 minutes").alias("w"), F.col("device_id"))
    .agg(
        F.count_distinct(F.date_trunc("minute", F.col("event_time"))).alias(
            "cnt_minutes"
        ),
        F.avg("temperature").alias("avg_temperature"),
        F.min("temperature").alias("min_temperature"),
        F.max("temperature").alias("max_temperature"),
    )
    .select(
        F.col("device_id"),
        F.col("w.start").alias("window_start"),
        F.col("w.end").alias("window_end"),
        "cnt_points",
        "avg_temperature",
        "min_temperature",
        "max_temperature",
    )
)

# =======================
# Join + derive flags (in Spark)
# =======================
enriched = (
    agg5.join(dim_b, "device_id", "left")
    .withColumn("threshold_used", F.col("temp_anomaly_threshold"))
    .withColumn("incomplete_flag", F.col("cnt_points") < F.lit(5))
    .withColumn(
        "anomaly_flag", F.col("max_temperature") > F.col("temp_anomaly_threshold")
    )
    .withColumn(
        "anomaly_reason",
        F.when(F.col("anomaly_flag"), F.lit("MAX_TEMP>THRESHOLD")).otherwise(
            F.lit(None)
        ),
    )
    .withColumn("updated_at", F.current_timestamp())
    .select(
        "window_start",
        "window_end",
        "device_id",
        F.col("cnt_minutes").alias("cnt_points"),
        "avg_temperature",
        "min_temperature",
        "max_temperature",
        "incomplete_flag",
        "anomaly_flag",
        "anomaly_reason",
        "threshold_used",
        "location_id",
        "model",
        "updated_at",
    )
)


def upsert_to_starrocks(batch_df, batch_id: int):
    if batch_df.limit(1).count() == 0:
        return
    # Collect to driver as JSONL (fine for tutorial volume)
    # Fewer, larger transactions (tune to your cluster)
    to_write = batch_df.coalesce(4)  # or repartition(4) if you're pushing volume

    (
        to_write.write.format("jdbc")
        .option(
            "url",
            "jdbc:mysql://starrocks-fe-0:9030/kappa_analytics"
            "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC"
            "&rewriteBatchedStatements=true",
        )  # MySQL perf hint
        .option("dbtable", "fact_telemetry_5min")
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("batchsize", 5000)  # Spark JDBC option
        .option("isolationLevel", "READ_COMMITTED")  # optional
        # .option("user", "spark").option("password", "sparkpw")  # if you set creds
        .mode("append")
        .save()
    )


# =======================
# Start stream
# =======================
query = (
    enriched.writeStream.outputMode("append")
    .foreachBatch(upsert_to_starrocks)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime=TRIGGER_EVERY)
    .start()
)

query.awaitTermination()
