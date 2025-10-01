import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

# =======================
# Config (env-overridable)
# =======================
CONNECT_URL = os.getenv("SPARK_CONNECT_URL", "sc://spark-connect:15002")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC_TEL = os.getenv("TOPIC_TELEMETRY", "iot-telemetry")
TOPIC_EVT = os.getenv("TOPIC_EVENTS", "iot-events")

STARROCKS_FE_HTTP = os.getenv(
    "STARROCKS_FE_HTTP", "starrocks-fe:8030"
)  # not used if JDBC
SR_JDBC_URL = os.getenv("SR_JDBC_URL", "jdbc:mysql://starrocks-fe-0:9030/kappa_analytics")
SR_DB = os.getenv("SR_DB", "kappa_analytics")
SR_TABLE = os.getenv("SR_TABLE", "fact_telemetry_5min")

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "s3a://spark-demo/stream-1/checkpoints/")
TRIGGER_EVERY = os.getenv("TRIGGER_EVERY", "30 seconds")
WATERMARK = os.getenv("WATERMARK", "3 minutes")
WINDOW = os.getenv("WINDOW", "5 minutes")

# Completeness: expected points per device per 5-min window.
# Set explicitly for the tutorial, e.g. TELEMETRY_EPS=10, NUM_DEVICES=10 -> ~300/10 = 30 per device per 5 min.
EXPECTED_POINTS_PER_WINDOW = int(
    os.getenv("EXPECTED_POINTS_PER_WINDOW", "30")
)  # tweak in tutorial
COMPLETENESS_TOLERANCE = float(
    os.getenv("COMPLETENESS_TOLERANCE", "0.85")
)  # 85% by default

# =======================
# SparkSession
# =======================
spark = SparkSession.builder.remote(CONNECT_URL).appName("Kappa-WindowJoin").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", os.getenv("SPARK_SHUFFLE_PARTITIONS", "8"))

# =======================
# Schemas
# =======================
telemetry_schema = T.StructType(
    [
        T.StructField("device_id", T.StringType(), False),
        T.StructField("timestamp", T.StringType(), False),
        T.StructField("temperature", T.DoubleType(), True),
    ]
)

events_schema = T.StructType(
    [
        T.StructField("event_id", T.StringType(), False),
        T.StructField("device_id", T.StringType(), False),
        T.StructField("event_timestamp", T.StringType(), False),
        T.StructField(
            "event_type", T.StringType(), False
        ),  # failure | maintenance | inspection
        T.StructField("severity", T.StringType(), True),  # low | medium | high
    ]
)

# =======================
# Read from Kafka (two topics)
# =======================
raw_tel = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC_TEL)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", False)
    .load()
)

raw_evt = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC_EVT)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", False)
    .load()
)

# =======================
# Parse JSON + event-time columns
# =======================
telemetry = (
    raw_tel.select(
        F.from_json(F.col("value").cast("string"), telemetry_schema).alias("j")
    )
    .select("j.*")
    .withColumn(
        "event_time",
        F.coalesce(
            F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
            F.to_timestamp("timestamp"),
        ),
    )
    .filter(F.col("event_time").isNotNull())
    .drop("timestamp")
)

events = (
    raw_evt.select(F.from_json(F.col("value").cast("string"), events_schema).alias("j"))
    .select("j.*")
    .withColumn(
        "event_time",
        F.coalesce(
            F.to_timestamp("event_timestamp", "yyyy-MM-dd'T'HH:mm:ss.SSSX"),
            F.to_timestamp("event_timestamp"),
        ),
    )
    .filter(F.col("event_time").isNotNull())
    .drop("event_timestamp")
)

# =======================
# Static dim (StarRocks via JDBC) & broadcast
# =======================
dim_device = (
    spark.read.format("jdbc")
    .option("url", SR_JDBC_URL)
    .option("dbtable", "dim_device")
    .option("driver", "com.mysql.cj.jdbc.Driver")
    .load()
    .select("device_id", "temp_anomaly_threshold", "location_id", "model")
)
dim_b = F.broadcast(dim_device)

# =======================
# Windowed aggregates (telemetry & events)
# =======================
agg_tel = (
    telemetry.withWatermark("event_time", WATERMARK)
    .dropDuplicates(["device_id", "event_time"])
    .groupBy(F.window("event_time", WINDOW).alias("w"), "device_id")
    .agg(
        F.approx_count_distinct(F.date_trunc("minute", F.col("event_time"))).alias(
            "minutes_covered"
        ),
        F.count(F.lit(1)).alias("cnt_events"),
        F.avg("temperature").alias("avg_temperature"),
        F.min("temperature").alias("min_temperature"),
        F.max("temperature").alias("max_temperature"),
    )
)


agg_evt = (
    events.withWatermark("event_time", WATERMARK)
    .dropDuplicates(["event_id"])
    .groupBy(F.window("event_time", WINDOW).alias("w"), "device_id")
    .agg(
        F.count(F.lit(1)).alias("events_total"),
        F.sum(F.when(F.col("event_type") == "failure", 1).otherwise(0)).alias(
            "events_failure"
        ),
        F.sum(F.when(F.col("event_type") == "maintenance", 1).otherwise(0)).alias(
            "events_maintenance"
        ),
        F.sum(F.when(F.col("event_type") == "inspection", 1).otherwise(0)).alias(
            "events_inspection"
        ),
        F.sum(F.when(F.col("severity") == "high", 1).otherwise(0)).alias(
            "events_sev_high"
        ),
    )
)


# =======================
# Join windowed facts + dim and derive flags
# =======================
# to avoid this error Stream-stream LeftOuter join between two streaming DataFrame/Datasets
# is not supported without a watermark in the join keys, or a watermark on the nullable side
# and an appropriate range condition;
t = agg_tel.alias("t")
e = agg_evt.alias("e")
d = dim_b.alias("d")

expected_points = F.lit(EXPECTED_POINTS_PER_WINDOW)
threshold_points = (expected_points * F.lit(COMPLETENESS_TOLERANCE)).cast("int")

fact_5min = (
    t.join(
        e,
        (F.col("t.device_id") == F.col("e.device_id")) & (F.col("t.w") == F.col("e.w")),
        "leftOuter",
    )
    .na.fill(
        0,
        [
            "events_total",
            "events_failure",
            "events_maintenance",
            "events_inspection",
            "events_sev_high",
        ],
    )
    .join(d, F.col("t.device_id") == F.col("d.device_id"), "left")
    .withColumn("threshold_used", F.col("d.temp_anomaly_threshold"))
    .withColumn("incomplete_by_coverage", F.col("t.minutes_covered") < F.lit(5))
    .withColumn("incomplete_by_volume", F.col("t.cnt_events") < threshold_points)
    .withColumn(
        "incomplete_flag", F.col("incomplete_by_coverage") | F.col("incomplete_by_volume")
    )
    .withColumn(
        "anomaly_flag", F.col("t.max_temperature") > F.col("d.temp_anomaly_threshold")
    )
    .withColumn(
        "anomaly_reason",
        F.when(F.col("anomaly_flag"), F.lit("MAX_TEMP>THRESHOLD")).otherwise(F.lit(None)),
    )
    .withColumn("updated_at", F.current_timestamp())
    .select(
        F.col("t.w").getField("start").alias("window_start"),
        F.col("t.w").getField("end").alias("window_end"),
        F.col("t.device_id").alias("device_id"),
        F.col("t.minutes_covered").alias("minutes_covered"),
        F.col("t.cnt_events").alias("cnt_events"),
        F.col("t.avg_temperature").alias("avg_temperature"),
        F.col("t.min_temperature").alias("min_temperature"),
        F.col("t.max_temperature").alias("max_temperature"),
        F.col("events_total"),
        F.col("events_failure"),
        F.col("events_maintenance"),
        F.col("events_inspection"),
        F.col("events_sev_high"),
        F.col("incomplete_by_coverage"),
        F.col("incomplete_by_volume"),
        F.col("incomplete_flag"),
        F.col("anomaly_flag"),
        F.col("anomaly_reason"),
        F.col("threshold_used"),
        F.col("d.location_id").alias("location_id"),
        F.col("d.model").alias("model"),
        F.col("updated_at"),
    )
)


# =======================
# Sink — StarRocks via JDBC (PRIMARY KEY upsert-friendly)
# =======================
def upsert_to_starrocks(batch_df, batch_id: int):
    if batch_df.limit(1).count() == 0:
        return
    to_write = batch_df.coalesce(int(os.getenv("SINK_COALESCE", "4")))
    (
        to_write.write.format("jdbc")
        .option(
            "url",
            SR_JDBC_URL
            + "?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC&rewriteBatchedStatements=true",
        )
        .option("dbtable", SR_TABLE)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("batchsize", os.getenv("JDBC_BATCHSIZE", "5000"))
        .option("isolationLevel", "READ_COMMITTED")
        # .option("user", "...").option("password", "...")
        .mode("append")  # On StarRocks PRIMARY KEY tables, INSERT behaves as UPSERT.
        .save()
    )


# =======================
# Start stream
# =======================
query = (
    fact_5min.writeStream.outputMode(
        "append"
    )  # window joins can output updates; sink uses PK upsert
    .foreachBatch(upsert_to_starrocks)
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(processingTime=TRIGGER_EVERY)
    .start()
)
query.awaitTermination()
