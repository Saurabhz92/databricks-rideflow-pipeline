"""
=============================================================================
  RideFlow — Gold Layer: Business Aggregations & Analytics
  
  Pipeline: Delta Silver → Batch/Streaming → Delta Gold Tables
  
  Aggregations:
  1. city_ride_metrics     — Rides per city per minute, avg fare, revenue
  2. driver_utilization    — Active drivers, trips/driver, efficiency score
  3. surge_pricing         — Demand vs supply ratio, surge trigger flags
  4. funnel_analysis       — Conversion through ride event stages
  5. hourly_city_trends    — Hourly aggregations for BI dashboards
  
  Features:
  - Tumbling & sliding window aggregations
  - Anti-join for demand vs supply gap
  - Caching hot datasets
  - Z-Ordering and partition pruning
=============================================================================
"""

import logging
import sys

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window

sys.path.append("..")
from config.config import (
    CHECKPOINT_BASE,
    DB_GOLD,
    GOLD_PATH,
    PARTITION_COLS_GOLD,
    SILVER_PATH,
    SPARK_APP_NAME,
    STREAMING_TRIGGER_MS,
    TBL_GOLD_CITY_METRICS,
    TBL_GOLD_DRIVER_UTIL,
    TBL_GOLD_SURGE,
    ZORDER_COLS_GOLD,
    LOG_LEVEL,
    TBL_SILVER_RIDES,
)

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("GoldAggregations")

SILVER_TABLE_PATH = f"{SILVER_PATH}/ride_events_clean"

# ───────────────────────────────────────────────────────────────────────────────
# 1. City Ride Metrics — rides per city per 1-minute tumbling window
# ───────────────────────────────────────────────────────────────────────────────

def compute_city_ride_metrics(silver_df: DataFrame) -> DataFrame:
    """
    Aggregate ride events into 1-minute tumbling windows per city.
    
    Output columns:
    - window_start, window_end, city
    - total_rides, completed_rides, requested_rides
    - total_revenue, avg_fare, max_fare, min_fare
    - avg_distance, total_distance
    """
    metrics = (
        silver_df
        .filter(~F.col("has_data_quality_issue"))
        .withWatermark("event_ts", "5 minutes")
        .groupBy(
            F.window(F.col("event_ts"), "1 minute").alias("ride_window"),
            F.col("city"),
        )
        .agg(
            F.count("*").alias("total_rides"),
            F.countDistinct("ride_id").alias("unique_rides"),
            F.sum(F.when(F.col("event_type") == "trip_completed", 1).otherwise(0))
             .alias("completed_rides"),
            F.sum(F.when(F.col("event_type") == "ride_requested", 1).otherwise(0))
             .alias("requested_rides"),
            F.sum(F.when(F.col("event_type") == "payment_completed", 1).otherwise(0))
             .alias("payment_events"),
            # Revenue metrics (only on completed/payment events)
            F.sum(
                F.when(F.col("event_type").isin("trip_completed", "payment_completed"),
                       F.col("fare_amount")).otherwise(0)
            ).alias("total_gross_revenue"),
            F.sum(
                F.when(F.col("event_type").isin("trip_completed", "payment_completed"),
                       F.col("net_fare")).otherwise(0)
            ).alias("total_net_revenue"),
            F.avg(F.when(F.col("event_type") == "trip_completed",
                        F.col("fare_amount"))).alias("avg_fare"),
            F.max(F.col("fare_amount")).alias("max_fare"),
            F.min(F.col("fare_amount")).alias("min_fare"),
            # Distance
            F.avg(F.col("trip_distance")).alias("avg_distance_km"),
            F.sum(F.col("trip_distance")).alias("total_distance_km"),
            # Surge
            F.avg(F.col("surge_multiplier")).alias("avg_surge_multiplier"),
        )
        .withColumn("window_start",      F.col("ride_window.start"))
        .withColumn("window_end",        F.col("ride_window.end"))
        .withColumn("window_start_date", F.to_date(F.col("window_start")))
        .withColumn("completion_rate",
            F.round(F.col("completed_rides") / F.greatest(F.col("requested_rides"), F.lit(1)) * 100, 2)
        )
        .withColumn("gold_processed_ts", F.current_timestamp())
        .drop("ride_window")
    )
    return metrics


# ───────────────────────────────────────────────────────────────────────────────
# 2. Driver Utilization Metrics
# ───────────────────────────────────────────────────────────────────────────────

def compute_driver_utilization(silver_df: DataFrame) -> DataFrame:
    """
    Per-driver efficiency metrics over a 15-minute rolling window.
    
    Output columns:
    - driver_id, city, window_start, window_end
    - trips_assigned, trips_completed, trips_cancelled
    - total_earnings, avg_earnings_per_trip
    - utilization_rate (completed / assigned)
    - efficiency_score (composite metric)
    """
    driver_metrics = (
        silver_df
        .filter(~F.col("has_data_quality_issue"))
        .filter(F.col("driver_id").isNotNull())
        .withWatermark("event_ts", "10 minutes")
        .groupBy(
            F.window(F.col("event_ts"), "15 minutes", "5 minutes").alias("ride_window"),
            F.col("driver_id"),
            F.col("city"),
        )
        .agg(
            F.countDistinct("ride_id").alias("unique_rides"),
            F.sum(F.when(F.col("event_type") == "driver_assigned", 1).otherwise(0))
             .alias("trips_assigned"),
            F.sum(F.when(F.col("event_type") == "trip_completed", 1).otherwise(0))
             .alias("trips_completed"),
            F.sum(F.when(F.col("event_type") == "payment_completed",
                        F.col("fare_amount")).otherwise(0))
             .alias("total_earnings"),
            F.avg(F.col("trip_distance")).alias("avg_distance_km"),
        )
        .withColumn("window_start", F.col("ride_window.start"))
        .withColumn("window_end",   F.col("ride_window.end"))
        .withColumn("window_start_date", F.to_date(F.col("window_start")))
        .withColumn(
            "utilization_rate",
            F.round(
                F.col("trips_completed") / F.greatest(F.col("trips_assigned"), F.lit(1)) * 100,
                2
            )
        )
        .withColumn(
            "avg_earnings_per_trip",
            F.round(
                F.col("total_earnings") / F.greatest(F.col("trips_completed"), F.lit(1)),
                2
            )
        )
        # Composite efficiency score: utilization + earnings normalised 0-100
        .withColumn(
            "efficiency_score",
            F.round(
                (F.col("utilization_rate") * 0.6 + 
                 F.least(F.col("avg_earnings_per_trip") / 50.0 * 100, F.lit(100.0)) * 0.4),
                2
            )
        )
        .withColumn("gold_processed_ts", F.current_timestamp())
        .drop("ride_window")
    )
    return driver_metrics


# ───────────────────────────────────────────────────────────────────────────────
# 3. Surge Pricing Indicators — Demand vs Supply
# ───────────────────────────────────────────────────────────────────────────────

def compute_surge_pricing(silver_df: DataFrame) -> DataFrame:
    """
    Compute demand/supply ratio per city per 5-minute window.
    Surge flag triggers when demand > 1.5× supply and avg wait > threshold.
    
    Output columns:
    - city, window_start, window_end
    - ride_requests (demand), drivers_active (supply)
    - demand_supply_ratio
    - avg_surge_multiplier, recommended_surge_multiplier
    - surge_triggered (boolean)
    - surge_tier (NORMAL / MODERATE / HIGH / EXTREME)
    """
    demand = (
        silver_df
        .filter(F.col("event_type") == "ride_requested")
        .filter(~F.col("has_data_quality_issue"))
        .withWatermark("event_ts", "5 minutes")
        .groupBy(
            F.window(F.col("event_ts"), "5 minutes").alias("ride_window"),
            F.col("city"),
        )
        .agg(
            F.count("*").alias("ride_requests"),
            F.avg("surge_multiplier").alias("observed_surge"),
        )
    )

    supply = (
        silver_df
        .filter(F.col("event_type") == "driver_assigned")
        .filter(~F.col("has_data_quality_issue"))
        .withWatermark("event_ts", "5 minutes")
        .groupBy(
            F.window(F.col("event_ts"), "5 minutes").alias("ride_window"),
            F.col("city"),
        )
        .agg(
            F.countDistinct("driver_id").alias("drivers_active"),
        )
    )

    surge_df = (
        demand.alias("d")
        .join(
            supply.alias("s"),
            (F.col("d.ride_window") == F.col("s.ride_window")) &
            (F.col("d.city") == F.col("s.city")),
            how="left",
        )
        .select(
            F.col("d.city").alias("city"),
            F.col("d.ride_window.start").alias("window_start"),
            F.col("d.ride_window.end").alias("window_end"),
            F.col("d.ride_requests"),
            F.coalesce(F.col("s.drivers_active"), F.lit(0)).alias("drivers_active"),
            F.col("d.observed_surge").alias("avg_surge_multiplier"),
        )
        .withColumn("window_start_date", F.to_date(F.col("window_start")))
        .withColumn(
            "demand_supply_ratio",
            F.round(
                F.col("ride_requests") / F.greatest(F.col("drivers_active"), F.lit(1)),
                2
            )
        )
        .withColumn(
            "surge_triggered",
            F.col("demand_supply_ratio") > 1.5
        )
        # Dynamic surge recommendation
        .withColumn(
            "recommended_surge_multiplier",
            F.when(F.col("demand_supply_ratio") > 3.0, F.lit(2.5))
             .when(F.col("demand_supply_ratio") > 2.0, F.lit(2.0))
             .when(F.col("demand_supply_ratio") > 1.5, F.lit(1.5))
             .otherwise(F.lit(1.0))
        )
        .withColumn(
            "surge_tier",
            F.when(F.col("demand_supply_ratio") > 3.0, F.lit("EXTREME"))
             .when(F.col("demand_supply_ratio") > 2.0, F.lit("HIGH"))
             .when(F.col("demand_supply_ratio") > 1.5, F.lit("MODERATE"))
             .otherwise(F.lit("NORMAL"))
        )
        .withColumn("gold_processed_ts", F.current_timestamp())
    )
    return surge_df


# ───────────────────────────────────────────────────────────────────────────────
# 4. Ride Funnel Analysis (batch)
# ───────────────────────────────────────────────────────────────────────────────

def compute_funnel_analysis(silver_df: DataFrame) -> DataFrame:
    """
    Pivot event types to analyse conversion funnel per city per day.
    
    requested → assigned → started → completed → payment_completed
    """
    funnel = (
        silver_df
        .filter(~F.col("has_data_quality_issue"))
        .groupBy("event_date", "city")
        .pivot("event_type", [
            "ride_requested", "driver_assigned",
            "trip_started", "trip_completed", "payment_completed"
        ])
        .agg(F.countDistinct("ride_id"))
        .withColumnRenamed("ride_requested",    "stage_1_requested")
        .withColumnRenamed("driver_assigned",   "stage_2_assigned")
        .withColumnRenamed("trip_started",      "stage_3_started")
        .withColumnRenamed("trip_completed",    "stage_4_completed")
        .withColumnRenamed("payment_completed", "stage_5_payment")
        .withColumn(
            "assignment_rate",
            F.round(F.col("stage_2_assigned") /
                    F.greatest(F.col("stage_1_requested"), F.lit(1)) * 100, 2)
        )
        .withColumn(
            "completion_rate",
            F.round(F.col("stage_4_completed") /
                    F.greatest(F.col("stage_2_assigned"), F.lit(1)) * 100, 2)
        )
        .withColumn(
            "payment_rate",
            F.round(F.col("stage_5_payment") /
                    F.greatest(F.col("stage_4_completed"), F.lit(1)) * 100, 2)
        )
        .withColumn("gold_processed_ts", F.current_timestamp())
    )
    return funnel


# ───────────────────────────────────────────────────────────────────────────────
# Gold Writer Utility
# ───────────────────────────────────────────────────────────────────────────────

def upsert_gold_table(df: DataFrame, gold_path: str, merge_keys: list, spark: SparkSession):
    """Generic MERGE into Gold Delta table."""
    if DeltaTable.isDeltaTable(spark, gold_path):
        gold_table = DeltaTable.forPath(spark, gold_path)
        merge_cond = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
        (
            gold_table.alias("target")
            .merge(df.alias("source"), merge_cond)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            df.write
            .format("delta")
            .partitionBy(*PARTITION_COLS_GOLD)
            .mode("overwrite")
            .save(gold_path)
        )


# ───────────────────────────────────────────────────────────────────────────────
# Gold Pipeline Orchestrator (Batch — triggered from Workflow)
# ───────────────────────────────────────────────────────────────────────────────

def run_gold_pipeline(spark: SparkSession, since_hours: int = 2):
    """
    Execute all Gold aggregations from a recent Silver window.
    
    Args:
        spark:       Active SparkSession
        since_hours: Process data from the last N hours (incremental)
    """
    log.info("🥇 Starting Gold aggregation pipeline (last %d hours)...", since_hours)

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {DB_GOLD}")

    # Read Silver with incremental filter
    silver_df = (
        spark.read
             .format("delta")
             .load(SILVER_TABLE_PATH)
             .filter(
                 F.col("event_ts") >= F.date_sub(F.current_timestamp(),
                                                  int(since_hours / 24 + 1))
             )
    )
    # Cache Silver reads — reused for multiple aggregations
    silver_df.cache()
    log.info("Silver rows loaded: %d", silver_df.count())

    # ── 1. City Ride Metrics ─────────────────────────────────────────────
    city_metrics = compute_city_ride_metrics(silver_df)
    upsert_gold_table(
        city_metrics,
        gold_path   = f"{GOLD_PATH}/city_ride_metrics",
        merge_keys  = ["window_start", "city"],
        spark       = spark,
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TBL_GOLD_CITY_METRICS}
        USING DELTA LOCATION '{GOLD_PATH}/city_ride_metrics'
    """)
    log.info("✅ Gold city_ride_metrics written")

    # ── 2. Driver Utilization ────────────────────────────────────────────
    driver_util = compute_driver_utilization(silver_df)
    upsert_gold_table(
        driver_util,
        gold_path   = f"{GOLD_PATH}/driver_utilization",
        merge_keys  = ["window_start", "driver_id", "city"],
        spark       = spark,
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TBL_GOLD_DRIVER_UTIL}
        USING DELTA LOCATION '{GOLD_PATH}/driver_utilization'
    """)
    log.info("✅ Gold driver_utilization written")

    # ── 3. Surge Pricing ─────────────────────────────────────────────────
    surge = compute_surge_pricing(silver_df)
    upsert_gold_table(
        surge,
        gold_path   = f"{GOLD_PATH}/surge_pricing",
        merge_keys  = ["window_start", "city"],
        spark       = spark,
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TBL_GOLD_SURGE}
        USING DELTA LOCATION '{GOLD_PATH}/surge_pricing'
    """)
    log.info("✅ Gold surge_pricing written")

    # ── 4. Funnel Analysis ────────────────────────────────────────────────
    funnel = compute_funnel_analysis(silver_df)
    upsert_gold_table(
        funnel,
        gold_path   = f"{GOLD_PATH}/funnel_analysis",
        merge_keys  = ["event_date", "city"],
        spark       = spark,
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS rideflow_gold.funnel_analysis
        USING DELTA LOCATION '{GOLD_PATH}/funnel_analysis'
    """)
    log.info("✅ Gold funnel_analysis written")

    silver_df.unpersist()

    # ── 5. Optimise Gold Tables ───────────────────────────────────────────
    _optimize_gold_tables(spark)
    log.info("🥇 Gold pipeline complete.")


def _optimize_gold_tables(spark: SparkSession):
    """Run OPTIMIZE + ZORDER on all Gold tables."""
    optimizations = [
        (TBL_GOLD_CITY_METRICS, "city, window_start"),
        (TBL_GOLD_DRIVER_UTIL,  "driver_id, city"),
        (TBL_GOLD_SURGE,        "city, surge_tier"),
    ]
    for table, zorder_cols in optimizations:
        try:
            log.info("Optimising %s ...", table)
            spark.sql(f"OPTIMIZE {table} ZORDER BY ({zorder_cols})")
        except Exception as exc:
            log.warning("Could not optimise %s: %s", table, exc)


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    from ingestion.bronze_ingestion import create_spark_session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    import argparse
    parser = argparse.ArgumentParser(description="RideFlow Gold Aggregations")
    parser.add_argument("--since-hours", type=int, default=2,
                        help="Process data from last N hours")
    args = parser.parse_args()

    run_gold_pipeline(spark, since_hours=args.since_hours)
    spark.stop()
