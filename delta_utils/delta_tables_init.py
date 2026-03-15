"""
=============================================================================
  RideFlow — Delta Lake Utilities
  
  Covers:
  1. Table initialisation (DDL + TBLPROPERTIES)
  2. Time travel queries
  3. MERGE operations
  4. Schema enforcement & evolution
  5. VACUUM & OPTIMIZE
  6. Delta table metadata helpers
=============================================================================
"""

import logging
import sys
from typing import Optional

from delta import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

sys.path.append("..")
from config.config import (
    BRONZE_PATH, SILVER_PATH, GOLD_PATH, DEAD_LETTER_PATH,
    DB_BRONZE, DB_SILVER, DB_GOLD,
    TBL_BRONZE_RIDES, TBL_SILVER_RIDES,
    TBL_GOLD_CITY_METRICS, TBL_GOLD_DRIVER_UTIL, TBL_GOLD_SURGE,
    TBL_DEAD_LETTER,
    LOG_LEVEL,
)

log = logging.getLogger("DeltaUtils")

# ── 1. Database + Table Initialisation ───────────────────────────────────────

BRONZE_DDL = f"""
CREATE TABLE IF NOT EXISTS {TBL_BRONZE_RIDES} (
    ride_id            STRING       COMMENT 'Unique ride identifier',
    driver_id          STRING       COMMENT 'Driver identifier from driver pool',
    rider_id           STRING       COMMENT 'Rider identifier from rider pool',
    city               STRING       COMMENT 'City where the ride occurred',
    event_type         STRING       COMMENT 'Ride lifecycle event type',
    trip_distance      DOUBLE       COMMENT 'Trip distance in kilometers',
    fare_amount        DOUBLE       COMMENT 'Charged fare in USD',
    surge_multiplier   DOUBLE       COMMENT 'Surge pricing multiplier applied',
    event_timestamp    STRING       COMMENT 'ISO8601 event timestamp from producer',
    schema_version     STRING       COMMENT 'Payload schema version',
    raw_value          STRING       COMMENT 'Original raw JSON payload',
    raw_key            STRING       COMMENT 'Kafka partition key',
    ingestion_ts       TIMESTAMP    COMMENT 'Spark ingestion timestamp',
    kafka_offset       LONG         COMMENT 'Kafka topic offset',
    kafka_partition    INT          COMMENT 'Kafka partition number',
    kafka_topic        STRING       COMMENT 'Kafka topic name',
    event_date         DATE         COMMENT 'Partition column — event date'
)
USING DELTA
PARTITIONED BY (event_date, city)
LOCATION '{BRONZE_PATH}/ride_events_raw'
COMMENT 'Raw ride events ingested from Kafka — Bronze layer'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'              = 'true',
    'delta.autoOptimize.optimizeWrite'        = 'true',
    'delta.autoOptimize.autoCompact'          = 'true',
    'delta.dataSkippingNumIndexedCols'        = '8',
    'pipelines.autoOptimize.zOrderCols'       = 'ride_id,driver_id',
    'delta.columnMapping.mode'                = 'name',
    'delta.minReaderVersion'                  = '2',
    'delta.minWriterVersion'                  = '5'
)
"""

SILVER_DDL = f"""
CREATE TABLE IF NOT EXISTS {TBL_SILVER_RIDES} (
    ride_id                 STRING,
    driver_id               STRING,
    rider_id                STRING,
    city                    STRING,
    event_type              STRING,
    trip_distance           DOUBLE,
    fare_amount             DOUBLE,
    net_fare                DOUBLE    COMMENT 'fare_amount * 0.82 (after platform cut)',
    surge_multiplier        DOUBLE,
    event_ts                TIMESTAMP COMMENT 'Parsed UTC event timestamp',
    event_date              DATE      COMMENT 'Partition column',
    event_hour              INT,
    event_minute            INT,
    schema_version          STRING,
    has_data_quality_issue  BOOLEAN,
    dq_invalid_event_type   BOOLEAN,
    dq_fare_out_of_range    BOOLEAN,
    dq_distance_out_of_range BOOLEAN,
    dq_null_driver          BOOLEAN,
    dq_null_rider           BOOLEAN,
    ingestion_ts            TIMESTAMP,
    silver_processed_ts     TIMESTAMP,
    silver_version          STRING
)
USING DELTA
PARTITIONED BY (event_date, city)
LOCATION '{SILVER_PATH}/ride_events_clean'
COMMENT 'Cleaned and validated ride events — Silver layer'
TBLPROPERTIES (
    'delta.enableChangeDataFeed'        = 'true',
    'delta.autoOptimize.optimizeWrite'  = 'true',
    'delta.autoOptimize.autoCompact'    = 'true'
)
"""

GOLD_CITY_METRICS_DDL = f"""
CREATE TABLE IF NOT EXISTS {TBL_GOLD_CITY_METRICS} (
    window_start            TIMESTAMP,
    window_end              TIMESTAMP,
    window_start_date       DATE       COMMENT 'Partition column',
    city                    STRING,
    total_rides             LONG,
    unique_rides            LONG,
    completed_rides         LONG,
    requested_rides         LONG,
    payment_events          LONG,
    total_gross_revenue     DOUBLE,
    total_net_revenue       DOUBLE,
    avg_fare                DOUBLE,
    max_fare                DOUBLE,
    min_fare                DOUBLE,
    avg_distance_km         DOUBLE,
    total_distance_km       DOUBLE,
    avg_surge_multiplier    DOUBLE,
    completion_rate         DOUBLE,
    gold_processed_ts       TIMESTAMP
)
USING DELTA
PARTITIONED BY (window_start_date, city)
LOCATION '{GOLD_PATH}/city_ride_metrics'
COMMENT 'City-level ride metrics per 1-min window — Gold layer'
TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')
"""

GOLD_DRIVER_UTIL_DDL = f"""
CREATE TABLE IF NOT EXISTS {TBL_GOLD_DRIVER_UTIL} (
    window_start            TIMESTAMP,
    window_end              TIMESTAMP,
    window_start_date       DATE,
    driver_id               STRING,
    city                    STRING,
    unique_rides            LONG,
    trips_assigned          LONG,
    trips_completed         LONG,
    total_earnings          DOUBLE,
    avg_distance_km         DOUBLE,
    utilization_rate        DOUBLE,
    avg_earnings_per_trip   DOUBLE,
    efficiency_score        DOUBLE,
    gold_processed_ts       TIMESTAMP
)
USING DELTA
PARTITIONED BY (window_start_date, city)
LOCATION '{GOLD_PATH}/driver_utilization'
COMMENT 'Driver utilization metrics per 15-min sliding window — Gold layer'
"""

GOLD_SURGE_DDL = f"""
CREATE TABLE IF NOT EXISTS {TBL_GOLD_SURGE} (
    window_start                TIMESTAMP,
    window_end                  TIMESTAMP,
    window_start_date           DATE,
    city                        STRING,
    ride_requests               LONG,
    drivers_active              LONG,
    avg_surge_multiplier        DOUBLE,
    demand_supply_ratio         DOUBLE,
    surge_triggered             BOOLEAN,
    recommended_surge_multiplier DOUBLE,
    surge_tier                  STRING,
    gold_processed_ts           TIMESTAMP
)
USING DELTA
PARTITIONED BY (window_start_date, city)
LOCATION '{GOLD_PATH}/surge_pricing'
COMMENT 'Demand vs supply surge indicators per 5-min window — Gold layer'
"""

DEAD_LETTER_DDL = f"""
CREATE TABLE IF NOT EXISTS {TBL_DEAD_LETTER} (
    ride_id              STRING,
    event_type           STRING,
    raw_value            STRING,
    dead_letter_reason   STRING,
    ingestion_ts         TIMESTAMP,
    kafka_offset         LONG,
    kafka_partition      INT,
    event_date           DATE
)
USING DELTA
PARTITIONED BY (event_date)
LOCATION '{DEAD_LETTER_PATH}/ride_events'
COMMENT 'Failed / corrupt records from Bronze ingestion'
"""


def initialise_all_tables(spark: SparkSession):
    """
    Create all Bronze, Silver, Gold, and Dead-Letter Delta tables.
    Safe to run multiple times (IF NOT EXISTS).
    """
    log.info("Initialising all Delta tables...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_BRONZE}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_SILVER}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_GOLD}")

    for ddl in [BRONZE_DDL, SILVER_DDL, GOLD_CITY_METRICS_DDL,
                GOLD_DRIVER_UTIL_DDL, GOLD_SURGE_DDL, DEAD_LETTER_DDL]:
        try:
            spark.sql(ddl)
        except Exception as exc:
            log.warning("DDL execution warning (may already exist): %s", exc)

    log.info("✅ All tables initialised.")


# ── 2. Time Travel Queries ────────────────────────────────────────────────────

def read_table_at_version(spark: SparkSession, table: str, version: int) -> DataFrame:
    """Read a Delta table at a specific historical version."""
    log.info("Time travel: %s @ version=%d", table, version)
    return spark.read.format("delta").option("versionAsOf", version).table(table)


def read_table_at_timestamp(spark: SparkSession, table: str, ts: str) -> DataFrame:
    """
    Read a Delta table at a specific timestamp.
    ts format: '2024-01-15 10:00:00'
    """
    log.info("Time travel: %s @ timestamp='%s'", table, ts)
    return spark.read.format("delta").option("timestampAsOf", ts).table(table)


def show_table_history(spark: SparkSession, table: str, limit: int = 10):
    """Display Delta transaction log history."""
    spark.sql(f"DESCRIBE HISTORY {table} LIMIT {limit}").show(truncate=False)


def restore_table_to_version(spark: SparkSession, table: str, version: int):
    """
    Restore a Delta table to a previous version.
    ⚠️  Use with caution in production — this is a destructive operation.
    """
    log.warning("RESTORING %s to version=%d — this is irreversible!", table, version)
    spark.sql(f"RESTORE TABLE {table} TO VERSION AS OF {version}")
    log.info("Restore complete.")


# ── 3. MERGE / Upsert Patterns ────────────────────────────────────────────────

def merge_driver_status_update(spark: SparkSession, updates_df: DataFrame):
    """
    Example MERGE: Update driver online/offline status in Silver.
    Demonstrates MERGE with UPDATE + INSERT + DELETE.
    """
    silver = DeltaTable.forName(spark, TBL_SILVER_RIDES)
    (
        silver.alias("t")
        .merge(
            updates_df.alias("s"),
            "t.driver_id = s.driver_id AND t.event_date = s.event_date"
        )
        .whenMatchedUpdate(set={
            "city":       "s.city",
            "event_type": "s.event_type",
        })
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()   # Remove stale driver records
        .execute()
    )


# ── 4. Schema Enforcement Helpers ─────────────────────────────────────────────

def enforce_schema(df: DataFrame, required_cols: list) -> DataFrame:
    """Drop any unexpected columns and ensure required columns are present."""
    missing = set(required_cols) - set(df.columns)
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")
    return df.select(required_cols)


def add_schema_version_col(df: DataFrame, version: str = "1.0") -> DataFrame:
    return df.withColumn("schema_version", F.lit(version))


# ── 5. VACUUM & OPTIMIZE ──────────────────────────────────────────────────────

def run_maintenance(spark: SparkSession, retain_hours: int = 168):
    """
    Run VACUUM and OPTIMIZE on all tables.
    Default retention: 7 days (168 hours).
    Schedule this as a nightly Databricks Workflow task.
    """
    tables = [
        (TBL_BRONZE_RIDES,      "ride_id, driver_id"),
        (TBL_SILVER_RIDES,      "ride_id, driver_id, rider_id"),
        (TBL_GOLD_CITY_METRICS, "city, window_start"),
        (TBL_GOLD_DRIVER_UTIL,  "driver_id, city"),
        (TBL_GOLD_SURGE,        "city, surge_tier"),
        (TBL_DEAD_LETTER,       "ride_id"),
    ]

    for table, zorder_cols in tables:
        log.info("Optimising %s ...", table)
        try:
            spark.sql(f"OPTIMIZE {table} ZORDER BY ({zorder_cols})")
            spark.sql(f"VACUUM {table} RETAIN {retain_hours} HOURS")
        except Exception as exc:
            log.warning("Maintenance skipped for %s: %s", table, exc)

    log.info("✅ Maintenance complete for all tables.")


# ── 6. Table Metadata Helpers ─────────────────────────────────────────────────

def get_table_stats(spark: SparkSession, table: str) -> dict:
    """Return row count and latest commit info for a Delta table."""
    count = spark.table(table).count()
    history = (
        spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1")
        .collect()[0]
    )
    return {
        "table":          table,
        "row_count":      count,
        "latest_version": history["version"],
        "latest_ts":      str(history["timestamp"]),
        "operation":      history["operation"],
    }


def print_all_table_stats(spark: SparkSession):
    """Print a summary of all pipeline tables."""
    tables = [
        TBL_BRONZE_RIDES, TBL_SILVER_RIDES,
        TBL_GOLD_CITY_METRICS, TBL_GOLD_DRIVER_UTIL,
        TBL_GOLD_SURGE, TBL_DEAD_LETTER,
    ]
    for tbl in tables:
        try:
            stats = get_table_stats(spark, tbl)
            log.info("📋 %s | rows=%d | version=%s | ts=%s",
                     stats["table"], stats["row_count"],
                     stats["latest_version"], stats["latest_ts"])
        except Exception as exc:
            log.warning("Could not stat %s: %s", tbl, exc)


# ── Main (CLI) ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from ingestion.bronze_ingestion import create_spark_session
    import argparse

    parser = argparse.ArgumentParser(description="Delta Lake Utilities")
    parser.add_argument("--action", choices=["init", "maintain", "stats", "history"],
                        default="init")
    parser.add_argument("--table",   default=TBL_SILVER_RIDES)
    parser.add_argument("--version", type=int, default=0)
    args = parser.parse_args()

    spark = create_spark_session()

    if args.action == "init":
        initialise_all_tables(spark)
    elif args.action == "maintain":
        run_maintenance(spark)
    elif args.action == "stats":
        print_all_table_stats(spark)
    elif args.action == "history":
        show_table_history(spark, args.table)

    spark.stop()
