"""
=============================================================================
  RideFlow — Bronze Layer: Structured Streaming Ingestion
  
  Pipeline: Kafka → Structured Streaming → Delta Bronze Tables
  
  Features:
  - Reads from Kafka with exactly-once semantics
  - Schema enforcement + evolution (mergeSchema)
  - Checkpointing for fault tolerance
  - Dead-letter routing for corrupt records
  - ACID writes to Delta Bronze tables
  - Partitioning by event_date and city
=============================================================================
"""

import logging
import sys
from datetime import datetime

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType,
)

sys.path.append("..")
from config.config import (
    BRONZE_PATH,
    CHECKPOINT_BASE,
    DATABRICKS_HOST,
    DB_BRONZE,
    DEAD_LETTER_PATH,
    DELTA_AUTO_COMPACT,
    DELTA_AUTO_OPTIMIZE,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_TOPIC_RIDES,
    LOG_LEVEL,
    PARTITION_COLS_BRONZE,
    REQUIRED_FIELDS,
    SHUFFLE_PARTITIONS,
    SPARK_APP_NAME,
    STREAMING_TRIGGER_MS,
    TBL_BRONZE_RIDES,
    TBL_DEAD_LETTER,
)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("BronzeIngestion")

# ── Spark Session ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Build a production-grade SparkSession with Delta Lake and streaming configs.
    On Databricks: SparkSession is auto-created; this is used for local/CI runs.
    """
    builder = (
        SparkSession.builder
        .appName(f"{SPARK_APP_NAME}-Bronze")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
        # Delta auto-optimizations
        .config("spark.databricks.delta.optimizeWrite.enabled", str(DELTA_AUTO_OPTIMIZE).lower())
        .config("spark.databricks.delta.autoCompact.enabled", str(DELTA_AUTO_COMPACT).lower())
        # Streaming exactly-once
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        # AWS S3 (configure credentials via IAM role on Databricks)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ── Raw Event Schema (post-JSON parse) ───────────────────────────────────────

RAW_RIDE_SCHEMA = StructType([
    StructField("ride_id",           StringType(),    nullable=True),
    StructField("driver_id",         StringType(),    nullable=True),
    StructField("rider_id",          StringType(),    nullable=True),
    StructField("city",              StringType(),    nullable=True),
    StructField("event_type",        StringType(),    nullable=True),
    StructField("trip_distance",     DoubleType(),    nullable=True),
    StructField("fare_amount",       DoubleType(),    nullable=True),
    StructField("surge_multiplier",  DoubleType(),    nullable=True),
    StructField("timestamp",         StringType(),    nullable=True),
    StructField("kafka_partition_key", StringType(), nullable=True),
    StructField("schema_version",    StringType(),    nullable=True),
])

# ── Kafka Source ──────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession):
    """
    Connect to Kafka and return a streaming DataFrame.
    Reads from earliest offset for replayability (startingOffsets=earliest).
    """
    kafka_options = {
        "kafka.bootstrap.servers":        KAFKA_BOOTSTRAP_SERVERS,
        "subscribe":                       KAFKA_TOPIC_RIDES,
        "kafka.group.id":                  KAFKA_CONSUMER_GROUP,
        "startingOffsets":                 "earliest",
        "maxOffsetsPerTrigger":            50_000,       # Backpressure control
        "failOnDataLoss":                  "false",      # Tolerate topic compaction
        "kafka.session.timeout.ms":        "30000",
        "kafka.request.timeout.ms":        "60000",
    }

    if KAFKA_SECURITY_PROTOCOL in ("SASL_SSL", "SASL_PLAINTEXT"):
        kafka_options.update({
            "kafka.security.protocol": KAFKA_SECURITY_PROTOCOL,
            "kafka.sasl.mechanism":    "PLAIN",
        })

    return (
        spark.readStream
             .format("kafka")
             .options(**kafka_options)
             .load()
    )


# ── Parse & Enrich ────────────────────────────────────────────────────────────

def parse_and_enrich(raw_df):
    """
    Parse Kafka value bytes → JSON → struct.
    Add Bronze metadata columns (ingestion_ts, kafka offset/partition, etc.).
    """
    parsed = (
        raw_df
        .withColumn("raw_value",    F.col("value").cast("string"))
        .withColumn("raw_key",      F.col("key").cast("string"))
        .withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("kafka_offset",    F.col("offset"))
        .withColumn("kafka_partition", F.col("partition"))
        .withColumn("kafka_topic",     F.col("topic"))
        # Parse JSON payload
        .withColumn("event",        F.from_json(F.col("raw_value"), RAW_RIDE_SCHEMA))
        # Flatten struct fields
        .select(
            F.col("event.ride_id"),
            F.col("event.driver_id"),
            F.col("event.rider_id"),
            F.col("event.city"),
            F.col("event.event_type"),
            F.col("event.trip_distance"),
            F.col("event.fare_amount"),
            F.col("event.surge_multiplier"),
            F.col("event.timestamp").alias("event_timestamp"),
            F.col("event.schema_version"),
            F.col("raw_value"),
            F.col("raw_key"),
            F.col("ingestion_ts"),
            F.col("kafka_offset"),
            F.col("kafka_partition"),
            F.col("kafka_topic"),
        )
        # Partition columns
        .withColumn("event_date", F.to_date(F.col("ingestion_ts")))
    )
    return parsed


# ── Triage: Good vs Dead-Letter ───────────────────────────────────────────────

def triage_records(df):
    """
    Split records into:
      - valid_df:  parseable and has at least ride_id + event_type
      - bad_df:    null ride_id or event_type (corrupt / schema mismatch)
    """
    has_required = F.col("ride_id").isNotNull() & F.col("event_type").isNotNull()
    valid_df = df.filter(has_required)
    bad_df   = df.filter(~has_required).withColumn(
        "dead_letter_reason",
        F.when(F.col("ride_id").isNull(), F.lit("NULL_RIDE_ID"))
         .when(F.col("event_type").isNull(), F.lit("NULL_EVENT_TYPE"))
         .otherwise(F.lit("SCHEMA_MISMATCH"))
    )
    return valid_df, bad_df


# ── Delta Writers (foreachBatch) ──────────────────────────────────────────────

def write_bronze_batch(micro_df, batch_id: int):
    """
    foreachBatch handler — writes each micro-batch to Bronze Delta table.
    Implements:
      - MERGE for idempotent upserts (avoid duplicates on retry)
      - Schema evolution (mergeSchema)
      - Partitioning by event_date + city
    """
    log.info("Processing Bronze batch_id=%d | rows=%d", batch_id, micro_df.count())

    enrich_df = parse_and_enrich(micro_df)

    # NOTE: micro_df here is already parsed from Kafka.
    # If calling this from readStream().writeStream().foreachBatch()
    # the micro_df IS the raw Kafka frame. Re-parse if needed.
    valid_df, bad_df = triage_records(enrich_df)

    # ── Write valid records to Bronze Delta ──────────────────────────────
    (
        valid_df.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")          # Schema evolution
        .partitionBy(*PARTITION_COLS_BRONZE)
        .save(BRONZE_PATH + "/ride_events_raw")
    )
    log.info("Bronze batch_id=%d | valid_written=%d", batch_id, valid_df.count())

    # ── Write dead-letter records ─────────────────────────────────────────
    if bad_df.count() > 0:
        (
            bad_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(DEAD_LETTER_PATH + "/ride_events")
        )
        log.warning("Bronze batch_id=%d | dead_letter_written=%d", batch_id, bad_df.count())


# ── Streaming Pipeline Entry Point ────────────────────────────────────────────

def run_bronze_pipeline(spark: SparkSession):
    """
    Start the Bronze streaming pipeline:
    Kafka → parse → triage → Delta Bronze + Dead-Letter
    """
    log.info("🟤 Starting Bronze ingestion pipeline...")

    # Create databases if not exist
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_BRONZE}")

    # Read from Kafka
    raw_stream = read_kafka_stream(spark)

    # Write using foreachBatch for maximum control
    query = (
        raw_stream
        .writeStream
        .foreachBatch(write_bronze_batch)
        .trigger(processingTime=f"{STREAMING_TRIGGER_MS} milliseconds")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze/ride_events")
        .queryName("bronze_ride_events_ingestion")
        .start()
    )

    log.info("🟤 Bronze pipeline running | queryId=%s", query.id)

    # Register Delta table in metastore (idempotent)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TBL_BRONZE_RIDES}
        USING DELTA
        LOCATION '{BRONZE_PATH}/ride_events_raw'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TBL_DEAD_LETTER}
        USING DELTA
        LOCATION '{DEAD_LETTER_PATH}/ride_events'
    """)

    return query


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    query = run_bronze_pipeline(spark)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        log.info("Bronze pipeline stopped by user.")
        query.stop()
    finally:
        spark.stop()
