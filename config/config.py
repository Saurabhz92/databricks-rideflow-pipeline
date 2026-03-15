"""
=============================================================================
  RideFlow Analytics Platform — Central Configuration
  FAANG-Level Databricks Lakehouse | Real-Time Ride Sharing Pipeline
=============================================================================
"""

import os

# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------
ENV = os.getenv("PIPELINE_ENV", "dev")          # dev | staging | prod

# ---------------------------------------------------------------------------
# AWS / S3
# ---------------------------------------------------------------------------
AWS_REGION          = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET           = os.getenv("S3_BUCKET", "rideflow-lakehouse")
S3_BASE_PATH        = f"s3a://{S3_BUCKET}"
DELTA_BASE_PATH     = f"{S3_BASE_PATH}/delta"

# Layer paths
BRONZE_PATH         = f"{DELTA_BASE_PATH}/bronze"
SILVER_PATH         = f"{DELTA_BASE_PATH}/silver"
GOLD_PATH           = f"{DELTA_BASE_PATH}/gold"
DEAD_LETTER_PATH    = f"{DELTA_BASE_PATH}/dead_letter"
CHECKPOINT_BASE     = f"{S3_BASE_PATH}/checkpoints"

# ---------------------------------------------------------------------------
# Kafka
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_RIDES        = "ride-events"
KAFKA_TOPIC_PAYMENTS     = "payment-events"
KAFKA_CONSUMER_GROUP     = "databricks-rideflow-consumer"
KAFKA_SECURITY_PROTOCOL  = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")  # SASL_SSL in prod

# SASL credentials (set via Databricks secrets in prod)
KAFKA_SASL_USERNAME = os.getenv("KAFKA_SASL_USERNAME", "")
KAFKA_SASL_PASSWORD = os.getenv("KAFKA_SASL_PASSWORD", "")

# ---------------------------------------------------------------------------
# Delta Lake / Spark
# ---------------------------------------------------------------------------
SPARK_APP_NAME          = "RideFlow-Analytics-Platform"
SHUFFLE_PARTITIONS      = 200          # Increase for prod; 200 for 200 GB shuffles
STREAMING_TRIGGER_MS    = 30_000       # 30-second micro-batch trigger

# Delta optimizations
DELTA_AUTO_OPTIMIZE     = True
DELTA_AUTO_COMPACT      = True
TARGET_FILE_SIZE_MB     = 128

# Partitioning
PARTITION_COLS_BRONZE   = ["event_date", "city"]
PARTITION_COLS_SILVER   = ["event_date", "city"]
PARTITION_COLS_GOLD     = ["window_start_date", "city"]

# Z-Order columns
ZORDER_COLS_SILVER      = ["ride_id", "driver_id", "rider_id"]
ZORDER_COLS_GOLD        = ["city", "avg_fare"]

# ---------------------------------------------------------------------------
# Database / Table Names
# ---------------------------------------------------------------------------
DB_BRONZE       = "rideflow_bronze"
DB_SILVER       = "rideflow_silver"
DB_GOLD         = "rideflow_gold"

TBL_BRONZE_RIDES        = f"{DB_BRONZE}.ride_events_raw"
TBL_SILVER_RIDES        = f"{DB_SILVER}.ride_events_clean"
TBL_DEAD_LETTER         = f"{DB_BRONZE}.ride_events_dead_letter"
TBL_GOLD_CITY_METRICS   = f"{DB_GOLD}.city_ride_metrics"
TBL_GOLD_DRIVER_UTIL    = f"{DB_GOLD}.driver_utilization"
TBL_GOLD_SURGE          = f"{DB_GOLD}.surge_pricing"

# ---------------------------------------------------------------------------
# Schema Validation
# ---------------------------------------------------------------------------
REQUIRED_FIELDS = [
    "ride_id", "driver_id", "rider_id", "city",
    "event_type", "trip_distance", "fare_amount", "timestamp"
]

VALID_EVENT_TYPES = {
    "ride_requested", "driver_assigned", "trip_started",
    "trip_completed", "payment_completed"
}

MAX_FARE_AMOUNT     = 5_000.0   # USD — flag anomalies above this
MIN_TRIP_DISTANCE   = 0.1       # km
MAX_TRIP_DISTANCE   = 500.0     # km

# ---------------------------------------------------------------------------
# Data Simulation
# ---------------------------------------------------------------------------
SIM_CITIES          = ["New York", "San Francisco", "Chicago", "Los Angeles", "Seattle",
                       "Austin", "Miami", "Boston", "Denver", "Atlanta"]
SIM_EVENTS_PER_SEC  = 500       # Simulated throughput
SIM_BATCH_SIZE      = 100
SIM_SLEEP_MS        = 200       # ms between batches

# ---------------------------------------------------------------------------
# Monitoring / Alerting
# ---------------------------------------------------------------------------
LOG_LEVEL           = os.getenv("LOG_LEVEL", "INFO")
ALERT_EMAIL         = os.getenv("ALERT_EMAIL", "data-engineering-alerts@rideflow.com")
SLACK_WEBHOOK_URL   = os.getenv("SLACK_WEBHOOK_URL", "")
MAX_DEAD_LETTER_PCT = 5.0       # Alert if dead-letter > 5% of total
CHECKPOINT_INTERVAL = 100       # Checkpoint every N batches

# ---------------------------------------------------------------------------
# Airflow
# ---------------------------------------------------------------------------
AIRFLOW_DAG_ID               = "rideflow_pipeline"
AIRFLOW_SCHEDULE             = "@hourly"
AIRFLOW_DEFAULT_RETRIES      = 3
AIRFLOW_RETRY_DELAY_MINUTES  = 5

# ---------------------------------------------------------------------------
# Databricks
# ---------------------------------------------------------------------------
DATABRICKS_HOST         = os.getenv("DATABRICKS_HOST", "https://<workspace>.azuredatabricks.net")
DATABRICKS_TOKEN        = os.getenv("DATABRICKS_TOKEN", "")
DATABRICKS_CLUSTER_ID   = os.getenv("DATABRICKS_CLUSTER_ID", "")
DATABRICKS_JOB_ID       = os.getenv("DATABRICKS_JOB_ID", "")
