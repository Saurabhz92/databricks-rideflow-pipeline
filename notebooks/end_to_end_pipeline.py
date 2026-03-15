# Databricks notebook source
# MAGIC %md
# MAGIC # 🚗 RideFlow — End-to-End Real-Time Analytics Platform
# MAGIC ### FAANG-Level Databricks Lakehouse | Medallion Architecture
# MAGIC **Architecture:** Apache Kafka → Structured Streaming → Delta Bronze → Silver → Gold → BI Dashboards
# MAGIC
# MAGIC | Layer  | Purpose                        | Technology              |
# MAGIC |--------|--------------------------------|-------------------------|
# MAGIC | Bronze | Raw event ingestion            | Kafka + Delta Streaming |
# MAGIC | Silver | Cleaned & validated data       | PySpark + Delta MERGE   |
# MAGIC | Gold   | Business aggregations          | Windowed Aggregations   |

# COMMAND ----------

# MAGIC %md ## 🔧 Setup & Configuration

# COMMAND ----------

# Install Delta if running outside Databricks Runtime
# %pip install delta-spark confluent-kafka faker python-json-logger tenacity

# COMMAND ----------

import sys
sys.path.insert(0, "/Workspace/Repos/rideflow-pipeline")  # Adjust to your Repo path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# On Databricks, 'spark' is auto-created with Databricks Runtime
# spark = SparkSession.builder.getOrCreate()

print(f"Spark version:   {spark.version}")
print(f"Delta version:   {spark.conf.get('spark.databricks.delta.preview.enabled', 'N/A')}")
print(f"Cluster cores:   {sc.defaultParallelism}")

# COMMAND ----------

# MAGIC %md ## ⚙️ Step 1 — Initialise Delta Tables

# COMMAND ----------

from delta_utils.delta_tables_init import initialise_all_tables

# Creates all Bronze, Silver, Gold, Dead-Letter tables (idempotent)
initialise_all_tables(spark)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW DATABASES LIKE 'rideflow*'

# COMMAND ----------

# MAGIC %md ## 📡 Step 2 — Start Kafka Producer (Simulation)
# MAGIC Run the producer in a **separate terminal** or Databricks Job:
# MAGIC ```bash
# MAGIC python data_simulation/kafka_producer.py --events-per-sec 500 --duration-mins 60
# MAGIC ```
# MAGIC Or trigger it as a Databricks Job run on a single-node cluster.

# COMMAND ----------

# MAGIC %md ## 🟤 Step 3 — Bronze Layer: Structured Streaming from Kafka

# COMMAND ----------

from ingestion.bronze_ingestion import (
    create_spark_session, read_kafka_stream, parse_and_enrich, 
    triage_records, write_bronze_batch, run_bronze_pipeline
)

# Start Bronze streaming pipeline
bronze_query = run_bronze_pipeline(spark)

# Display current streaming status
for q in spark.streams.active:
    print(f"Query: {q.name} | Active: {q.isActive} | Status: {q.status}")

# COMMAND ----------

# MAGIC %md ### 🔍 Inspect Bronze Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     event_date,
# MAGIC     city,
# MAGIC     event_type,
# MAGIC     COUNT(*) AS event_count,
# MAGIC     ROUND(AVG(fare_amount), 2) AS avg_fare
# MAGIC FROM rideflow_bronze.ride_events_raw
# MAGIC GROUP BY event_date, city, event_type
# MAGIC ORDER BY event_date DESC, event_count DESC
# MAGIC LIMIT 50;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dead-letter analysis
# MAGIC SELECT dead_letter_reason, COUNT(*) AS count
# MAGIC FROM rideflow_bronze.ride_events_dead_letter
# MAGIC GROUP BY dead_letter_reason
# MAGIC ORDER BY count DESC;

# COMMAND ----------

# MAGIC %md ## 🥈 Step 4 — Silver Layer: Data Cleaning & Deduplication

# COMMAND ----------

from transformation.silver_transformation import run_silver_pipeline, optimize_silver_table

# Start Silver streaming pipeline (reads Bronze via Change Data Feed)
silver_query = run_silver_pipeline(spark)

display(spark.table("rideflow_silver.ride_events_clean").limit(20))

# COMMAND ----------

# MAGIC %md ### 📊 Silver Data Quality Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     has_data_quality_issue,
# MAGIC     dq_invalid_event_type,
# MAGIC     dq_fare_out_of_range,
# MAGIC     dq_distance_out_of_range,
# MAGIC     dq_null_driver,
# MAGIC     dq_null_rider,
# MAGIC     COUNT(*) AS record_count,
# MAGIC     ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_total
# MAGIC FROM rideflow_silver.ride_events_clean
# MAGIC GROUP BY ALL
# MAGIC ORDER BY record_count DESC;

# COMMAND ----------

# MAGIC %md ## 🥇 Step 5 — Gold Layer: Business Aggregations

# COMMAND ----------

from gold.gold_aggregations import (
    run_gold_pipeline,
    compute_city_ride_metrics,
    compute_driver_utilization,
    compute_surge_pricing,
    compute_funnel_analysis,
)

# Run Gold batch aggregations (last 2 hours)
run_gold_pipeline(spark, since_hours=2)

# COMMAND ----------

# MAGIC %md ### 🏙️ City Ride Metrics (Real-Time Dashboard)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     city,
# MAGIC     SUM(total_rides)                    AS rides_last_hour,
# MAGIC     ROUND(SUM(total_gross_revenue), 2)  AS gross_revenue_usd,
# MAGIC     ROUND(AVG(avg_fare), 2)             AS avg_fare,
# MAGIC     ROUND(AVG(avg_surge_multiplier), 2) AS avg_surge,
# MAGIC     ROUND(AVG(completion_rate), 2)      AS completion_rate_pct
# MAGIC FROM rideflow_gold.city_ride_metrics
# MAGIC WHERE window_start >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
# MAGIC GROUP BY city
# MAGIC ORDER BY rides_last_hour DESC;

# COMMAND ----------

# MAGIC %md ### ⚡ Surge Pricing Alerts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT city, surge_tier, demand_supply_ratio,
# MAGIC        recommended_surge_multiplier, window_start
# MAGIC FROM rideflow_gold.surge_pricing
# MAGIC WHERE surge_triggered = TRUE
# MAGIC   AND window_start >= CURRENT_TIMESTAMP() - INTERVAL 15 MINUTES
# MAGIC ORDER BY demand_supply_ratio DESC;

# COMMAND ----------

# MAGIC %md ### 🚗 Driver Utilization

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT driver_id, city,
# MAGIC        ROUND(AVG(efficiency_score), 2) AS efficiency_score,
# MAGIC        SUM(trips_completed) AS trips_completed,
# MAGIC        ROUND(SUM(total_earnings), 2) AS total_earnings_usd
# MAGIC FROM rideflow_gold.driver_utilization
# MAGIC WHERE window_start_date = CURRENT_DATE()
# MAGIC GROUP BY driver_id, city
# MAGIC HAVING SUM(trips_completed) > 0
# MAGIC ORDER BY efficiency_score DESC
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md ## 🕰️ Step 6 — Delta Lake: Time Travel & ACID Operations

# COMMAND ----------

from delta_utils.delta_tables_init import (
    show_table_history, read_table_at_version, read_table_at_timestamp
)

# Show Silver table history
show_table_history(spark, "rideflow_silver.ride_events_clean", limit=5)

# COMMAND ----------

# Read data at version 0 (first write)
df_v0 = read_table_at_version(spark, "rideflow_silver.ride_events_clean", version=0)
print(f"Row count at version 0: {df_v0.count()}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Time travel: data 1 hour ago
# MAGIC SELECT COUNT(*) AS rows_1hr_ago
# MAGIC FROM rideflow_silver.ride_events_clean
# MAGIC TIMESTAMP AS OF (CURRENT_TIMESTAMP() - INTERVAL 1 HOUR);

# COMMAND ----------

# MAGIC %md ## 🚀 Step 7 — Performance Optimisation

# COMMAND ----------

from delta_utils.delta_tables_init import run_maintenance

# Trigger optimize + ZORDER + VACUUM (normally done nightly via Workflow)
run_maintenance(spark, retain_hours=168)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify Z-Order stats
# MAGIC DESCRIBE DETAIL rideflow_silver.ride_events_clean;

# COMMAND ----------

# MAGIC %md ## 📈 Step 8 — Pipeline Monitoring

# COMMAND ----------

from monitoring.monitoring import PipelineHealthMonitor, setup_json_logger

monitor = PipelineHealthMonitor(spark, poll_interval_sec=30)

# Check all active streaming queries
query_statuses = monitor.check_streaming_queries()
for s in query_statuses:
    print(f"Query '{s['query_name']}' | rows/sec={s['input_rows_per_sec']:.1f} | batch={s['batch_id']}")

# COMMAND ----------

# Check dead-letter rate
dl_stats = monitor.check_dead_letter_rate()
print(f"Dead-letter rate: {dl_stats['dead_letter_pct']:.2f}% (threshold: {dl_stats['threshold_pct']}%)")

# COMMAND ----------

# Overall pipeline metrics
metrics = monitor.get_pipeline_metrics()
for k, v in metrics.items():
    print(f"  {k}: {v}")

# COMMAND ----------

# MAGIC %md ## 🛑 Step 9 — Graceful Shutdown

# COMMAND ----------

# Stop all active streaming queries
for query in spark.streams.active:
    print(f"Stopping query: {query.name}")
    query.stop()

print("All streaming queries stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 🏁 Pipeline Summary
# MAGIC
# MAGIC | Component | Status | Notes |
# MAGIC |-----------|--------|-------|
# MAGIC | Kafka Producer | ✅ | 500 events/sec, 10 cities |
# MAGIC | Bronze Ingestion | ✅ | Exactly-once, dead-letter routing |
# MAGIC | Silver Cleaning | ✅ | Dedup, DQ flags, MERGE upserts |
# MAGIC | Gold Aggregations | ✅ | City metrics, surge, driver util |
# MAGIC | Delta Lake | ✅ | ACID, Time Travel, Z-ORDER |
# MAGIC | Monitoring | ✅ | Slack alerts, health checks |
# MAGIC | Airflow DAG | ✅ | @hourly schedule + nightly maintenance |
# MAGIC
# MAGIC ### 📊 Capacity Estimate
# MAGIC - **Throughput**: 500 events/sec × 3600 = **1.8M events/hour**
# MAGIC - **Bronze storage**: ~4 KB/record compressed → **~7.2 GB/hour**
# MAGIC - **Silver (deduplicated)**: ~60% of Bronze = **~4.3 GB/hour**
# MAGIC - **Gold (aggregated)**: ~1-5% of Silver = minimal storage
