"""
=============================================================================
  RideFlow — Silver Layer: Data Cleaning & Transformation
  
  Pipeline: Delta Bronze → Structured Streaming → Delta Silver Tables
  
  Features:
  - Deduplication on (ride_id, event_type)
  - Null value handling & imputation
  - Timestamp standardisation (UTC)
  - Business rule validation (fare, distance ranges)
  - MERGE (upsert) for idempotent writes
  - Z-Ordering for query acceleration
  - Partitioning by event_date and city
=============================================================================
"""

import logging
import sys

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

sys.path.append("..")
from config.config import (
    BRONZE_PATH,
    CHECKPOINT_BASE,
    DB_SILVER,
    PARTITION_COLS_SILVER,
    SILVER_PATH,
    SPARK_APP_NAME,
    STREAMING_TRIGGER_MS,
    TBL_SILVER_RIDES,
    VALID_EVENT_TYPES,
    MAX_FARE_AMOUNT,
    MIN_TRIP_DISTANCE,
    MAX_TRIP_DISTANCE,
    ZORDER_COLS_SILVER,
    LOG_LEVEL,
    SHUFFLE_PARTITIONS,
)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("SilverTransformation")

SILVER_TABLE_PATH = f"{SILVER_PATH}/ride_events_clean"

# ── Validation & Cleaning ─────────────────────────────────────────────────────

def clean_and_validate(df: DataFrame) -> DataFrame:
    """
    Apply business rules and data quality checks.
    
    Steps:
      1. Parse event_timestamp from ISO string to TimestampType
      2. Standardise city names (strip + title case)
      3. Validate event_type against allowed set
      4. Validate fare_amount and trip_distance in acceptable ranges
      5. Flag anomalies in a `data_quality_flag` column instead of dropping
      6. Deduplicate within the micro-batch on (ride_id, event_type)
      7. Add processing metadata columns
    """
    valid_event_set = list(VALID_EVENT_TYPES)

    cleaned = (
        df
        # ── 1. Parse timestamps ─────────────────────────────────────────
        .withColumn(
            "event_ts",
            F.to_timestamp(F.col("event_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")
            .cast(TimestampType())
        )
        # Fallback if timestamp parsing fails
        .withColumn(
            "event_ts",
            F.coalesce(F.col("event_ts"), F.current_timestamp())
        )

        # ── 2. Normalise city ───────────────────────────────────────────
        .withColumn("city", F.initcap(F.trim(F.col("city"))))

        # ── 3. Null imputation ──────────────────────────────────────────
        .withColumn(
            "trip_distance",
            F.when(F.col("trip_distance").isNull(), F.lit(0.0))
             .otherwise(F.col("trip_distance"))
        )
        .withColumn(
            "fare_amount",
            F.when(F.col("fare_amount").isNull(), F.lit(0.0))
             .otherwise(F.col("fare_amount"))
        )

        # ── 4. Data quality flags ───────────────────────────────────────
        .withColumn(
            "dq_invalid_event_type",
            ~F.col("event_type").isin(valid_event_set)
        )
        .withColumn(
            "dq_fare_out_of_range",
            (F.col("fare_amount") < 0) | (F.col("fare_amount") > MAX_FARE_AMOUNT)
        )
        .withColumn(
            "dq_distance_out_of_range",
            (F.col("trip_distance") < MIN_TRIP_DISTANCE) |
            (F.col("trip_distance") > MAX_TRIP_DISTANCE)
        )
        .withColumn(
            "dq_null_driver",
            F.col("driver_id").isNull()
        )
        .withColumn(
            "dq_null_rider",
            F.col("rider_id").isNull()
        )
        # Aggregate into a single flag
        .withColumn(
            "has_data_quality_issue",
            F.col("dq_invalid_event_type") |
            F.col("dq_fare_out_of_range") |
            F.col("dq_distance_out_of_range") |
            F.col("dq_null_driver") |
            F.col("dq_null_rider")
        )

        # ── 5. Derived columns ──────────────────────────────────────────
        .withColumn("event_date",  F.to_date(F.col("event_ts")))
        .withColumn("event_hour",  F.hour(F.col("event_ts")))
        .withColumn("event_minute", F.minute(F.col("event_ts")))
        # Estimated revenue after platform cut (18%)
        .withColumn("net_fare",    F.round(F.col("fare_amount") * 0.82, 2))

        # ── 6. Silver metadata ──────────────────────────────────────────
        .withColumn("silver_processed_ts", F.current_timestamp())
        .withColumn("silver_version",      F.lit("1.0"))
    )

    return cleaned


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Remove duplicate ride events within the micro-batch.
    Uses window function to keep the latest record per (ride_id, event_type).
    """
    from pyspark.sql import Window

    window = Window.partitionBy("ride_id", "event_type").orderBy(F.desc("ingestion_ts"))
    return (
        df.withColumn("_row_num", F.row_number().over(window))
          .filter(F.col("_row_num") == 1)
          .drop("_row_num")
    )


# ── MERGE into Silver Delta (Upsert) ─────────────────────────────────────────

def merge_to_silver(micro_df: DataFrame, silver_path: str, spark: SparkSession):
    """
    Upsert (MERGE) cleaned records into the Silver Delta table.
    Match on (ride_id, event_type) — the natural composite key.
    Updates existing rows if the incoming ingestion_ts is newer.
    """
    if DeltaTable.isDeltaTable(spark, silver_path):
        silver_table = DeltaTable.forPath(spark, silver_path)
        (
            silver_table.alias("target")
            .merge(
                micro_df.alias("source"),
                "target.ride_id = source.ride_id AND target.event_type = source.event_type"
            )
            .whenMatchedUpdate(
                condition="source.ingestion_ts > target.ingestion_ts",
                set={
                    "driver_id":             "source.driver_id",
                    "rider_id":              "source.rider_id",
                    "city":                  "source.city",
                    "trip_distance":         "source.trip_distance",
                    "fare_amount":           "source.fare_amount",
                    "net_fare":              "source.net_fare",
                    "surge_multiplier":      "source.surge_multiplier",
                    "event_ts":              "source.event_ts",
                    "event_date":            "source.event_date",
                    "event_hour":            "source.event_hour",
                    "event_minute":          "source.event_minute",
                    "has_data_quality_issue": "source.has_data_quality_issue",
                    "silver_processed_ts":   "source.silver_processed_ts",
                    "ingestion_ts":          "source.ingestion_ts",
                }
            )
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # First run — create the Delta table
        (
            micro_df.write
            .format("delta")
            .partitionBy(*PARTITION_COLS_SILVER)
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(silver_path)
        )


# ── foreachBatch Handler ──────────────────────────────────────────────────────

def process_silver_batch(micro_df: DataFrame, batch_id: int):
    """
    Orchestrates cleaning + deduplication + MERGE for each micro-batch.
    """
    spark = micro_df.sparkSession
    log.info("Silver batch_id=%d | input_rows=%d", batch_id, micro_df.count())

    # Cache the micro-batch — multiple passes (dedup + MERGE)
    micro_df.cache()

    try:
        cleaned = clean_and_validate(micro_df)
        deduped = deduplicate(cleaned)
        deduped.cache()

        valid_cnt    = deduped.filter(~F.col("has_data_quality_issue")).count()
        dq_issue_cnt = deduped.filter(F.col("has_data_quality_issue")).count()
        log.info(
            "Silver batch_id=%d | valid=%d | dq_flagged=%d",
            batch_id, valid_cnt, dq_issue_cnt
        )

        merge_to_silver(deduped, SILVER_TABLE_PATH, spark)
        log.info("Silver batch_id=%d | MERGE complete", batch_id)

        deduped.unpersist()
    except Exception as exc:
        log.exception("Silver batch_id=%d FAILED: %s", batch_id, exc)
        raise
    finally:
        micro_df.unpersist()


# ── Streaming Pipeline Entry Point ────────────────────────────────────────────

def run_silver_pipeline(spark: SparkSession):
    """
    Read Delta Bronze as a stream → transform → write Silver.
    Delta Change Data Feed (CDF) for efficient incremental reads.
    """
    log.info("🥈 Starting Silver transformation pipeline...")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_SILVER}")

    bronze_stream = (
        spark.readStream
             .format("delta")
             .option("readChangeFeed", "true")       # CDF for incremental reads
             .option("startingVersion", "0")
             .load(f"{BRONZE_PATH}/ride_events_raw")
             # Filter out CDF metadata rows; keep only inserts from Bronze
             .filter(F.col("_change_type").isin("insert", "update_postimage"))
    )

    query = (
        bronze_stream
        .writeStream
        .foreachBatch(process_silver_batch)
        .trigger(processingTime=f"{STREAMING_TRIGGER_MS} milliseconds")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/silver/ride_events")
        .queryName("silver_ride_events_transform")
        .start()
    )

    # Register table in Hive metastore
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TBL_SILVER_RIDES}
        USING DELTA
        LOCATION '{SILVER_TABLE_PATH}'
    """)

    # Enable CDF on Silver for Gold reads
    spark.sql(f"""
        ALTER TABLE {TBL_SILVER_RIDES}
        SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
    """)

    log.info("🥈 Silver pipeline running | queryId=%s", query.id)
    return query


# ── Z-Order Optimisation (run periodically via Workflow) ──────────────────────

def optimize_silver_table(spark: SparkSession):
    """
    Run OPTIMIZE + Z-ORDER on Silver table.
    Call this from a Databricks Workflow scheduled hourly/daily.
    """
    zorder_cols = ", ".join(ZORDER_COLS_SILVER)
    log.info("Running OPTIMIZE + ZORDER on %s ...", TBL_SILVER_RIDES)
    spark.sql(f"OPTIMIZE {TBL_SILVER_RIDES} ZORDER BY ({zorder_cols})")
    spark.sql(f"VACUUM {TBL_SILVER_RIDES} RETAIN 168 HOURS")   # 7 days
    log.info("Silver table optimised.")


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from ingestion.bronze_ingestion import create_spark_session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    query = run_silver_pipeline(spark)
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        log.info("Silver pipeline stopped by user.")
        query.stop()
    finally:
        optimize_silver_table(spark)
        spark.stop()
