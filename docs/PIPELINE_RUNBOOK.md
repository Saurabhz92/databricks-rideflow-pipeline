# RideFlow Analytics Platform — Complete Pipeline Runbook

### *Step-by-Step Guide: From Zero to Production*
**Project:** Real-Time Ride Sharing Analytics | Databricks Lakehouse Medallion Architecture  
**Stack:** Apache Kafka · PySpark · Delta Lake · AWS S3 · Databricks · Airflow

---

## Table of Contents

1. [System Prerequisites](#1-system-prerequisites)
2. [Clone & Folder Structure](#2-clone--folder-structure)
3. [Python Environment Setup](#3-python-environment-setup)
4. [AWS S3 Infrastructure Setup](#4-aws-s3-infrastructure-setup)
5. [Apache Kafka Setup](#5-apache-kafka-setup)
6. [Databricks Workspace Configuration](#6-databricks-workspace-configuration)
7. [Environment Variables & Secrets](#7-environment-variables--secrets)
8. [Step 1 — Initialise All Delta Tables](#8-step-1--initialise-all-delta-tables)
9. [Step 2 — Start the Kafka Producer](#9-step-2--start-the-kafka-producer)
10. [Step 3 — Run the Bronze Layer](#10-step-3--run-the-bronze-layer)
11. [Step 4 — Run the Silver Layer](#11-step-4--run-the-silver-layer)
12. [Step 5 — Run the Gold Layer](#12-step-5--run-the-gold-layer)
13. [Step 6 — Run the Master Databricks Notebook](#13-step-6--run-the-master-databricks-notebook)
14. [Step 7 — Deploy Databricks Workflow](#14-step-7--deploy-databricks-workflow)
15. [Step 8 — Deploy Apache Airflow DAG](#15-step-8--deploy-apache-airflow-dag)
16. [Step 9 — Run Analytics SQL Queries](#16-step-9--run-analytics-sql-queries)
17. [Step 10 — Monitor the Pipeline](#17-step-10--monitor-the-pipeline)
18. [Delta Lake Operations (Time Travel, MERGE, VACUUM)](#18-delta-lake-operations)
19. [Troubleshooting Common Errors](#19-troubleshooting-common-errors)
20. [Performance Tuning Guide](#20-performance-tuning-guide)
21. [Production Deployment Checklist](#21-production-deployment-checklist)

---

## 1. System Prerequisites

Before running anything, ensure all of the following are installed and available.

### Local Machine Requirements

| Tool | Minimum Version | Purpose | Install Guide |
|------|----------------|---------|---------------|
| Python | 3.10+ | PySpark + scripts | [python.org](https://python.org) |
| Java (JDK) | 11 or 17 | Spark requires JVM | `winget install Microsoft.OpenJDK.17` |
| Apache Kafka | 3.6+ | Event streaming broker | [kafka.apache.org](https://kafka.apache.org/downloads) |
| Docker Desktop | 24+ | Local Kafka (optional) | [docker.com](https://docker.com) |
| AWS CLI | 2.x | S3 bucket management | `winget install Amazon.AWSCLI` |
| Databricks CLI | 0.200+ | Workspace management | `pip install databricks-cli` |
| Apache Airflow | 2.8+ | DAG scheduling | `pip install apache-airflow` |

### Verify Installations

```powershell
# Check Python
python --version        # Expected: Python 3.10.x or higher

# Check Java
java -version           # Expected: openjdk version "17.x.x"

# Check AWS CLI
aws --version           # Expected: aws-cli/2.x.x

# Check Databricks CLI
databricks --version    # Expected: Databricks CLI v0.200+
```

> **Windows Note:** Set `JAVA_HOME` in your System Environment Variables to your JDK installation path, e.g., `C:\Program Files\Microsoft\jdk-17.0.x`.

---

## 2. Clone & Folder Structure

### Project Root

All code lives under:
```
C:\Users\Shree\Desktop\DATABRICKS ADVANCED PIPELINE\
```

### Expected Directory Layout

```
DATABRICKS ADVANCED PIPELINE/
├── config/
│   └── config.py                   ← Central config (S3 paths, Kafka, Spark)
├── data_simulation/
│   └── kafka_producer.py           ← Synthetic ride event producer
├── ingestion/
│   └── bronze_ingestion.py         ← Bronze streaming layer
├── transformation/
│   └── silver_transformation.py    ← Silver cleaning layer
├── gold/
│   └── gold_aggregations.py        ← Gold aggregation layer
├── delta_utils/
│   └── delta_tables_init.py        ← Delta DDL + maintenance utilities
├── monitoring/
│   └── monitoring.py               ← Logging, health checks, alerts
├── orchestration/
│   ├── airflow_dag.py              ← Apache Airflow DAG
│   └── databricks_workflow.json    ← Databricks Workflow definition
├── notebooks/
│   └── end_to_end_pipeline.py      ← Master Databricks notebook
├── analytics/
│   └── analytics_queries.sql       ← 12 BI SQL queries
├── docs/
│   └── PIPELINE_RUNBOOK.md         ← This file
├── requirements.txt
└── README.md
```

### Verify all files exist

```powershell
Get-ChildItem -Recurse "C:\Users\Shree\Desktop\DATABRICKS ADVANCED PIPELINE" -File | Select-Object FullName
```

---

## 3. Python Environment Setup

> **Do this once.** This creates an isolated Python environment with all dependencies.

### Step 3.1 — Create Virtual Environment

```powershell
# Navigate to project root
cd "C:\Users\Shree\Desktop\DATABRICKS ADVANCED PIPELINE"

# Create virtual environment
python -m venv .venv

# Activate it (PowerShell)
.venv\Scripts\Activate.ps1

# Confirm activation — you should see (.venv) in the prompt
```

> If you get a script execution error, run:
> ```powershell
> Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
> ```

### Step 3.2 — Install Dependencies

```powershell
# Upgrade pip first
python -m pip install --upgrade pip

# Install all requirements
pip install -r requirements.txt
```

> **Note:** `pyspark` and `delta-spark` installs can take 3–5 minutes. Be patient.

### Step 3.3 — Verify PySpark Installation

```powershell
python -c "import pyspark; print('PySpark:', pyspark.__version__)"
python -c "import delta; print('Delta:', delta.__version__)"
python -c "from confluent_kafka import Producer; print('Confluent Kafka: OK')"
```

Expected output:
```
PySpark: 3.5.0
Delta: 3.1.0
Confluent Kafka: OK
```

---

## 4. AWS S3 Infrastructure Setup

Delta Lake tables are stored in S3. You need an S3 bucket configured before running any pipeline code.

### Step 4.1 — Configure AWS Credentials

```powershell
aws configure
```

Enter when prompted:
```
AWS Access Key ID:     [your-access-key]
AWS Secret Access Key: [your-secret-key]
Default region name:   us-east-1
Default output format: json
```

### Step 4.2 — Create the S3 Bucket

```powershell
# Create the main lakehouse bucket
aws s3 mb s3://rideflow-lakehouse --region us-east-1

# Create the folder structure inside S3
aws s3api put-object --bucket rideflow-lakehouse --key "delta/bronze/"
aws s3api put-object --bucket rideflow-lakehouse --key "delta/silver/"
aws s3api put-object --bucket rideflow-lakehouse --key "delta/gold/"
aws s3api put-object --bucket rideflow-lakehouse --key "delta/dead_letter/"
aws s3api put-object --bucket rideflow-lakehouse --key "checkpoints/"
aws s3api put-object --bucket rideflow-lakehouse --key "init-scripts/"

# Verify
aws s3 ls s3://rideflow-lakehouse/
```

### Step 4.3 — Update S3 Bucket Name in Config

Open `config/config.py` and update:

```python
S3_BUCKET = "rideflow-lakehouse"       # ← Replace with your actual bucket name
AWS_REGION = "us-east-1"              # ← Replace with your region
```

### Step 4.4 — Configure S3 Block Public Access (Security)

```powershell
aws s3api put-public-access-block \
    --bucket rideflow-lakehouse \
    --public-access-block-configuration \
    "BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true"
```

---

## 5. Apache Kafka Setup

You have two options — **Docker (recommended for local)** or **Manual install**.

---

### Option A: Docker (Fastest — Recommended Locally)

```powershell
# Start Kafka + Zookeeper using Docker Compose
# Create a docker-compose.yml file first:

@"
version: '3.8'
services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    ports:
      - "2181:2181"

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
    ports:
      - "9092:9092"
"@ | Out-File -FilePath docker-compose.yml -Encoding utf8

# Start services
docker-compose up -d

# Wait 15 seconds for Kafka to be ready
Start-Sleep -Seconds 15

# Verify Kafka is running
docker ps
```

---

### Option B: Manual Kafka Installation (Windows)

```powershell
# 1. Download Kafka from https://kafka.apache.org/downloads
#    Choose: kafka_2.13-3.6.0.tgz

# 2. Extract to C:\kafka

# 3. Start Zookeeper (Terminal 1)
C:\kafka\bin\windows\zookeeper-server-start.bat C:\kafka\config\zookeeper.properties

# 4. Start Kafka Broker (Terminal 2)
C:\kafka\bin\windows\kafka-server-start.bat C:\kafka\config\server.properties
```

---

### Step 5.1 — Create the Kafka Topic

```powershell
# Using Docker
docker exec -it <kafka-container-name> kafka-topics \
    --create \
    --topic ride-events \
    --bootstrap-server localhost:9092 \
    --partitions 10 \
    --replication-factor 1

# Verify topic exists
docker exec -it <kafka-container-name> kafka-topics \
    --list \
    --bootstrap-server localhost:9092
```

> Replace `<kafka-container-name>` with the actual container name from `docker ps` (e.g., `databricks-advanced-pipeline-kafka-1`).

### Step 5.2 — Update Kafka Config

Open `config/config.py`:

```python
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"    # ← Local Docker Kafka
KAFKA_TOPIC_RIDES        = "ride-events"
KAFKA_SECURITY_PROTOCOL  = "PLAINTEXT"         # ← Local only; use SASL_SSL in prod
```

---

## 6. Databricks Workspace Configuration

> **If running locally (without Databricks),** skip to Step 8 and run the Python scripts directly. The S3 and Spark configs in `config.py` handle the connection details.

### Step 6.1 — Configure Databricks CLI

```powershell
databricks configure --token
```

Enter when prompted:
```
Databricks Host:  https://your-workspace.azuredatabricks.net
Token:            dapi...your-personal-access-token...
```

Verify:
```powershell
databricks workspace ls /
```

### Step 6.2 — Upload Code to Databricks Repos

```powershell
# In Databricks UI: Repos → Add Repo → link your Git repo
# OR upload directly:

databricks workspace import_dir "C:\Users\Shree\Desktop\DATABRICKS ADVANCED PIPELINE" /Repos/rideflow-pipeline --overwrite
```

### Step 6.3 — Configure Databricks Secrets

Store sensitive values in Databricks Secret Scopes (never hardcode in notebooks):

```powershell
# Create a secret scope
databricks secrets create-scope --scope rideflow

# Add secrets
databricks secrets put --scope rideflow --key kafka_bootstrap_servers
# When prompted, type: your-kafka-broker:9092

databricks secrets put --scope rideflow --key slack_webhook
# When prompted, type: https://hooks.slack.com/services/...

databricks secrets put --scope rideflow --key aws_access_key
# When prompted, type your AWS access key

databricks secrets put --scope rideflow --key aws_secret_key
# When prompted, type your AWS secret key
```

These secrets are referenced in `databricks_workflow.json` as `{{secrets/rideflow/kafka_bootstrap_servers}}`.

### Step 6.4 — Create an Instance Profile for S3 Access

In the Databricks Admin Console:
1. Go to **Admin Settings → Instance Profiles**
2. Add the ARN of your IAM Instance Profile: `arn:aws:iam::YOUR_ACCOUNT_ID:instance-profile/rideflow-databricks-role`
3. Attach the profile to your clusters

The IAM role needs these S3 permissions:
```json
{
  "Effect": "Allow",
  "Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket"],
  "Resource": ["arn:aws:s3:::rideflow-lakehouse", "arn:aws:s3:::rideflow-lakehouse/*"]
}
```

---

## 7. Environment Variables & Secrets

### For Local Runs (PowerShell)

Set these in your PowerShell session before running any script:

```powershell
$env:PIPELINE_ENV            = "dev"
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
$env:KAFKA_SECURITY_PROTOCOL = "PLAINTEXT"
$env:S3_BUCKET               = "rideflow-lakehouse"
$env:AWS_REGION              = "us-east-1"
$env:LOG_LEVEL               = "INFO"
$env:ALERT_EMAIL             = "you@example.com"
$env:SLACK_WEBHOOK_URL       = ""          # Leave empty if not using Slack
$env:DATABRICKS_HOST         = "https://your-workspace.azuredatabricks.net"
$env:DATABRICKS_TOKEN        = "dapi..."
```

> **Tip:** Save these in a `.env` file and load with:
> ```powershell
> Get-Content .env | ForEach-Object { if ($_ -match '=') { $k,$v = $_ -split '=',2; [System.Environment]::SetEnvironmentVariable($k,$v) } }
> ```

### For Databricks Notebooks

Reference secrets directly:
```python
kafka_bootstrap = dbutils.secrets.get(scope="rideflow", key="kafka_bootstrap_servers")
slack_webhook   = dbutils.secrets.get(scope="rideflow", key="slack_webhook")
```

---

## 8. Step 1 — Initialise All Delta Tables

> **This must be run first, before any pipeline layer.** It creates all Bronze, Silver, Gold, and Dead-Letter Delta tables. It is idempotent — safe to run multiple times.

```powershell
# From project root with virtual environment active:
cd "C:\Users\Shree\Desktop\DATABRICKS ADVANCED PIPELINE"
.venv\Scripts\Activate.ps1

python delta_utils/delta_tables_init.py --action init
```

**Expected Console Output:**
```
2026-03-15T09:30:00Z DeltaUtils INFO Initialising all Delta tables...
2026-03-15T09:30:01Z DeltaUtils INFO CREATE DATABASE rideflow_bronze
2026-03-15T09:30:02Z DeltaUtils INFO CREATE DATABASE rideflow_silver
2026-03-15T09:30:03Z DeltaUtils INFO CREATE DATABASE rideflow_gold
2026-03-15T09:30:05Z DeltaUtils INFO ✅ All tables initialised.
```

**Tables Created:**

| Table | Layer | Location |
|-------|-------|----------|
| `rideflow_bronze.ride_events_raw` | Bronze | `s3a://rideflow-lakehouse/delta/bronze/ride_events_raw` |
| `rideflow_bronze.ride_events_dead_letter` | Bronze | `s3a://rideflow-lakehouse/delta/dead_letter/ride_events` |
| `rideflow_silver.ride_events_clean` | Silver | `s3a://rideflow-lakehouse/delta/silver/ride_events_clean` |
| `rideflow_gold.city_ride_metrics` | Gold | `s3a://rideflow-lakehouse/delta/gold/city_ride_metrics` |
| `rideflow_gold.driver_utilization` | Gold | `s3a://rideflow-lakehouse/delta/gold/driver_utilization` |
| `rideflow_gold.surge_pricing` | Gold | `s3a://rideflow-lakehouse/delta/gold/surge_pricing` |
| `rideflow_gold.funnel_analysis` | Gold | `s3a://rideflow-lakehouse/delta/gold/funnel_analysis` |

**Verify in Databricks SQL:**
```sql
SHOW DATABASES LIKE 'rideflow*';
SHOW TABLES IN rideflow_bronze;
SHOW TABLES IN rideflow_silver;
SHOW TABLES IN rideflow_gold;
```

---

## 9. Step 2 — Start the Kafka Producer

The Kafka producer simulates real-world ride-sharing events at **~500 events per second**. It generates ride events for 10 cities with realistic surge multipliers and injects ~2% corrupt records to test error handling.

### Run the Producer

Open a **new terminal window** (keep it running):

```powershell
cd "C:\Users\Shree\Desktop\DATABRICKS ADVANCED PIPELINE"
.venv\Scripts\Activate.ps1

# Default: 500 events/sec, runs indefinitely
python data_simulation/kafka_producer.py

# Custom: 1000 events/sec for 30 minutes
python data_simulation/kafka_producer.py --events-per-sec 1000 --duration-mins 30

# Low volume for development/testing:
python data_simulation/kafka_producer.py --events-per-sec 50
```

**Expected Console Output:**
```
2026-03-15T09:31:00Z [INFO] RideFlowProducer - 🚗 RideFlow Kafka Producer started | topic=ride-events | target=500 evt/s
2026-03-15T09:31:10Z [INFO] RideFlowProducer - 📊 Stats | sent=5000 | failed=0 | rate=500 evt/s
2026-03-15T09:31:20Z [INFO] RideFlowProducer - 📊 Stats | sent=10000 | failed=0 | rate=498 evt/s
```

**What Each Event Looks Like (JSON):**
```json
{
  "ride_id": "RIDE-A1B2C3D4E5F6",
  "driver_id": "DRV-9A8B7C6D",
  "rider_id": "RDR-AABBCCDD",
  "city": "New York",
  "event_type": "trip_completed",
  "trip_distance": 12.4,
  "fare_amount": 28.50,
  "surge_multiplier": 1.5,
  "timestamp": "2026-03-15T09:31:00.123456+00:00",
  "kafka_partition_key": "New York",
  "schema_version": "1.0"
}
```

**Verify events are flowing into Kafka:**
```powershell
# Docker-based Kafka
docker exec -it <kafka-container> kafka-console-consumer \
    --bootstrap-server localhost:9092 \
    --topic ride-events \
    --from-beginning \
    --max-messages 5
```

---

## 10. Step 3 — Run the Bronze Layer

The Bronze layer reads raw events from Kafka using **Spark Structured Streaming** and writes them to the Bronze Delta table. It also routes corrupt/unparseable records to a dead-letter Delta table.

### Run Bronze Ingestion

Open a **new terminal window** (this runs continuously):

```powershell
cd "C:\Users\Shree\Desktop\DATABRICKS ADVANCED PIPELINE"
.venv\Scripts\Activate.ps1

python ingestion/bronze_ingestion.py
```

**Expected Console Output:**
```
2026-03-15T09:32:00Z [INFO] BronzeIngestion - 🟤 Starting Bronze ingestion pipeline...
2026-03-15T09:32:05Z [INFO] BronzeIngestion - 🟤 Bronze pipeline running | queryId=abc-123-xyz
2026-03-15T09:32:35Z [INFO] BronzeIngestion - Processing Bronze batch_id=0 | rows=15240
2026-03-15T09:32:36Z [INFO] BronzeIngestion - Bronze batch_id=0 | valid_written=14952
2026-03-15T09:32:36Z [WARN] BronzeIngestion - Bronze batch_id=0 | dead_letter_written=288
```

### What Bronze Does (Step by Step)

1. **Connects to Kafka** with `readStream.format("kafka")`
2. **Reads binary Kafka messages** (key + value) every 30 seconds (trigger interval)
3. **Casts the Kafka value** bytes → UTF-8 string (raw JSON)
4. **Parses JSON** using the predefined `RAW_RIDE_SCHEMA` with `from_json()`
5. **Adds metadata columns**: `ingestion_ts`, `kafka_offset`, `kafka_partition`, `event_date`
6. **Triages records**:
   - Records with non-null `ride_id` AND `event_type` → **Bronze Delta table**
   - Records with null `ride_id` or `event_type` → **Dead-letter Delta table**
7. **Writes using `foreachBatch`** for MERGE and splitting control
8. **Checkpoints** to `s3a://rideflow-lakehouse/checkpoints/bronze/ride_events`

### Verify Bronze Data

```python
# In Databricks notebook or local PySpark session:
spark.table("rideflow_bronze.ride_events_raw").show(5, truncate=False)
spark.table("rideflow_bronze.ride_events_dead_letter").show(5)

# Count by event type
spark.sql("""
    SELECT event_type, COUNT(*) as cnt
    FROM rideflow_bronze.ride_events_raw
    GROUP BY event_type ORDER BY cnt DESC
""").show()
```

### Stop Bronze Gracefully

Press **`Ctrl + C`** in the terminal. PySpark will complete the current micro-batch before stopping.

---

## 11. Step 4 — Run the Silver Layer

The Silver layer reads **incrementally from Bronze** (using Delta Change Data Feed for efficiency), applies data quality checks, deduplicates records, and writes clean data to the Silver Delta table using MERGE.

### Run Silver Transformation

Open a **new terminal window** (runs simultaneously with Bronze):

```powershell
cd "C:\Users\Shree\Desktop\DATABRICKS ADVANCED PIPELINE"
.venv\Scripts\Activate.ps1

python transformation/silver_transformation.py
```

**Expected Console Output:**
```
2026-03-15T09:33:00Z [INFO] SilverTransformation - 🥈 Starting Silver transformation pipeline...
2026-03-15T09:33:30Z [INFO] SilverTransformation - Silver batch_id=0 | input_rows=14952
2026-03-15T09:33:31Z [INFO] SilverTransformation - Silver batch_id=0 | valid=14620 | dq_flagged=332
2026-03-15T09:33:32Z [INFO] SilverTransformation - Silver batch_id=0 | MERGE complete
```

### What Silver Does (Step by Step)

1. **Reads Bronze Delta via CDF** (`readChangeFeed=true`) — only processes newly inserted rows, not the entire table
2. **Cleans data**:
   - Parses ISO8601 `event_timestamp` → `TimestampType` column (`event_ts`)
   - Normalises city names: `"new york"` → `"New York"`
   - Imputes null `trip_distance` and `fare_amount` with `0.0`
3. **Applies Data Quality Flags** (non-destructive — flagged rows are kept, not dropped):
   - `dq_invalid_event_type` — event_type not in allowed set
   - `dq_fare_out_of_range` — fare < 0 or fare > $5,000
   - `dq_distance_out_of_range` — distance < 0.1 km or > 500 km
   - `dq_null_driver` / `dq_null_rider`
   - `has_data_quality_issue` — aggregate boolean of all DQ checks
4. **Derives new columns**: `event_date`, `event_hour`, `event_minute`, `net_fare` (fare × 0.82)
5. **Deduplicates** within each micro-batch using a window function on `(ride_id, event_type)`, keeping the latest record by `ingestion_ts`
6. **MERGE into Silver** — updates existing rows if the new record is newer; inserts new records
7. **Checkpoints** to `s3a://rideflow-lakehouse/checkpoints/silver/ride_events`

### Verify Silver Data

```sql
-- Run in Databricks SQL or notebook
SELECT
    has_data_quality_issue,
    COUNT(*) AS records,
    ROUND(AVG(fare_amount), 2) AS avg_fare,
    ROUND(AVG(trip_distance), 2) AS avg_distance_km
FROM rideflow_silver.ride_events_clean
GROUP BY has_data_quality_issue;
```

### Run Silver Optimization (optional, on-demand)

```powershell
python -c "
from ingestion.bronze_ingestion import create_spark_session
from transformation.silver_transformation import optimize_silver_table
spark = create_spark_session()
optimize_silver_table(spark)
spark.stop()
"
```

---

## 12. Step 5 — Run the Gold Layer

The Gold layer runs as a **scheduled batch job** (not continuous streaming). It reads Silver data from the last N hours and computes four business aggregations.

### Run Gold Aggregations

```powershell
cd "C:\Users\Shree\Desktop\DATABRICKS ADVANCED PIPELINE"
.venv\Scripts\Activate.ps1

# Process last 2 hours (default — run every hour via Airflow/Workflow)
python gold/gold_aggregations.py --since-hours 2

# Full backfill (use with caution — reads all Silver data)
python gold/gold_aggregations.py --since-hours 720
```

**Expected Console Output:**
```
2026-03-15T09:35:00Z [INFO] GoldAggregations - 🥇 Starting Gold aggregation pipeline (last 2 hours)...
2026-03-15T09:35:02Z [INFO] GoldAggregations - Silver rows loaded: 1,800,000
2026-03-15T09:35:15Z [INFO] GoldAggregations - ✅ Gold city_ride_metrics written
2026-03-15T09:35:28Z [INFO] GoldAggregations - ✅ Gold driver_utilization written
2026-03-15T09:35:40Z [INFO] GoldAggregations - ✅ Gold surge_pricing written
2026-03-15T09:35:52Z [INFO] GoldAggregations - ✅ Gold funnel_analysis written
2026-03-15T09:36:00Z [INFO] GoldAggregations - 🥇 Gold pipeline complete.
```

### Gold Aggregations Explained

| Gold Table | Window | Key Metrics | Use Case |
|-----------|--------|-------------|----------|
| `city_ride_metrics` | 1-minute tumbling | Total rides, revenue, avg fare, completion rate | Real-time dashboard KPIs |
| `driver_utilization` | 15-minute sliding (5-min slide) | Efficiency score, utilization rate, earnings | Driver app, HR analytics |
| `surge_pricing` | 5-minute tumbling | Demand/supply ratio, surge tier, recommended multiplier | Pricing engine, ops alerts |
| `funnel_analysis` | Daily (date + city) | Stage conversion rates (requested → payment) | Product analytics, PM insights |

### Verify Gold Data

```sql
-- City metrics
SELECT city, SUM(total_rides) AS rides, ROUND(SUM(total_gross_revenue), 2) AS revenue
FROM rideflow_gold.city_ride_metrics
WHERE window_start >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
GROUP BY city ORDER BY rides DESC;

-- Surge alerts
SELECT city, surge_tier, demand_supply_ratio, recommended_surge_multiplier
FROM rideflow_gold.surge_pricing
WHERE surge_triggered = TRUE
ORDER BY demand_supply_ratio DESC;
```

---

## 13. Step 6 — Run the Master Databricks Notebook

The master notebook `notebooks/end_to_end_pipeline.py` orchestrates all layers in one Databricks notebook with SQL cells, visualizations, and inline monitoring.

### Upload to Databricks

```powershell
# Upload the notebook
databricks workspace import \
    "C:\Users\Shree\Desktop\DATABRICKS ADVANCED PIPELINE\notebooks\end_to_end_pipeline.py" \
    /Repos/rideflow-pipeline/notebooks/end_to_end_pipeline \
    --language PYTHON \
    --overwrite
```

### Run in Databricks UI

1. Open your Databricks workspace → **Workspace → Repos → rideflow-pipeline → notebooks**
2. Open `end_to_end_pipeline`
3. Click **"Connect"** and select your cluster (DBR 14.3+)
4. Click **"Run All"** to execute all cells sequentially

### Notebook Cell-by-Cell Walkthrough

| Cell # | Type | What It Does |
|--------|------|-------------|
| 1 | Markdown | Architecture overview table |
| 2 | Python | Imports + SparkSession verification |
| 3 | Python | `initialise_all_tables(spark)` — create DDLs |
| 4 | SQL | `SHOW DATABASES LIKE 'rideflow*'` |
| 5 | Markdown | Producer instructions |
| 6 | Python | `run_bronze_pipeline(spark)` — start streaming Bronze |
| 7 | Python | `spark.streams.active` — show active queries |
| 8 | SQL | Bronze table inspection |
| 9 | SQL | Dead-letter count by reason |
| 10 | Python | `run_silver_pipeline(spark)` — start streaming Silver |
| 11 | Python | `display(silver_table.limit(20))` |
| 12 | SQL | Silver DQ summary |
| 13 | Python | `run_gold_pipeline(spark, 2)` — Gold batch |
| 14 | SQL | City metrics last hour |
| 15 | SQL | Surge pricing alerts |
| 16 | SQL | Driver leaderboard |
| 17 | Python | `show_table_history()` — Delta time travel |
| 18 | Python | Time travel at version 0 |
| 19 | SQL | `TIMESTAMP AS OF` time travel |
| 20 | Python | `run_maintenance(spark)` — OPTIMIZE + VACUUM |
| 21 | SQL | `DESCRIBE DETAIL` — Z-ORDER stats |
| 22 | Python | `PipelineHealthMonitor.check_streaming_queries()` |
| 23 | Python | Dead-letter rate check |
| 24 | Python | Pipeline metrics summary |
| 25 | Python | `query.stop()` — graceful shutdown |

---

## 14. Step 7 — Deploy Databricks Workflow

The Databricks Workflow (`orchestration/databricks_workflow.json`) runs all pipeline steps with 6 tasks on 3 dedicated auto-scaling clusters, triggered hourly.

### Deploy via Databricks CLI

```powershell
# Create the workflow job from the JSON definition
databricks jobs create --json-file "C:\Users\Shree\Desktop\DATABRICKS ADVANCED PIPELINE\orchestration\databricks_workflow.json"
```

This returns a `job_id`. Save it:
```
{"job_id": 1001}
```

### Update Workflow Job IDs in Config

Add the returned `job_id` to `config/config.py`:
```python
DATABRICKS_JOB_ID = "1001"
```

And set as Airflow Variables:
```powershell
airflow variables set RIDEFLOW_GOLD_JOB_ID 1003
```

### Trigger the Workflow Manually (First Run Test)

```powershell
# Trigger a one-time run
databricks jobs run-now --job-id 1001

# Check run status
databricks runs get --run-id <run_id_from_previous_command>
```

### View in Databricks UI

1. Go to **Workflows** in left sidebar
2. Find `rideflow-realtime-pipeline`
3. Click to see the DAG view with task dependencies:
```
delta_table_init
     ↓
bronze_streaming_ingestion
     ↓
silver_streaming_transform
     ↓
gold_batch_aggregation
     ↓
├── nightly_maintenance (00:00 UTC only)
└── pipeline_monitoring (always)
```

### Workflow Cluster Configuration Summary

| Cluster | Tasks | Size | Autoscale |
|---------|-------|------|-----------|
| `streaming_cluster` | Bronze + Silver | m5.2xlarge | 2–8 nodes |
| `compute_cluster` | Gold | m5.xlarge | 1–4 nodes |
| `utility_cluster` | Init + Maintenance + Monitor | m5.large | 1 node |

---

## 15. Step 8 — Deploy Apache Airflow DAG

Airflow provides external orchestration and scheduling, integrating with Databricks via the Databricks Airflow Provider.

### Step 15.1 — Initialize Airflow Database

```powershell
# Set Airflow home
$env:AIRFLOW_HOME = "C:\Users\Shree\airflow"

# Initialize database
airflow db init

# Create admin user
airflow users create `
    --username admin `
    --password admin `
    --firstname Admin `
    --lastname User `
    --role Admin `
    --email admin@rideflow.com
```

### Step 15.2 — Copy DAG to Airflow DAGs Folder

```powershell
# Create DAGs directory
New-Item -ItemType Directory -Force -Path "$env:AIRFLOW_HOME\dags"

# Copy the DAG file
Copy-Item "C:\Users\Shree\Desktop\DATABRICKS ADVANCED PIPELINE\orchestration\airflow_dag.py" `
          "$env:AIRFLOW_HOME\dags\rideflow_dag.py"
```

### Step 15.3 — Configure Databricks Connection in Airflow

```powershell
# Add the Databricks connection
airflow connections add databricks_default `
    --conn-type databricks `
    --conn-host "https://your-workspace.azuredatabricks.net" `
    --conn-password "dapi-your-token-here"
```

### Step 15.4 — Set Airflow Variables

```powershell
airflow variables set RIDEFLOW_BRONZE_JOB_ID 1001
airflow variables set RIDEFLOW_SILVER_JOB_ID 1002
airflow variables set RIDEFLOW_GOLD_JOB_ID   1003
airflow variables set RIDEFLOW_MAINT_JOB_ID  1004
airflow variables set KAFKA_BOOTSTRAP_SERVERS "localhost:9092"
airflow variables set DATABRICKS_HOST         "https://your-workspace.azuredatabricks.net"
airflow variables set DATABRICKS_TOKEN        "dapi..."
airflow variables set DATABRICKS_SQL_WAREHOUSE_ID "your-warehouse-id"
airflow variables set RIDEFLOW_SLACK_WEBHOOK  "https://hooks.slack.com/services/..."
```

### Step 15.5 — Start Airflow

```powershell
# Terminal 1 — Webserver
airflow webserver --port 8080

# Terminal 2 — Scheduler
airflow scheduler
```

### Step 15.6 — Activate and Monitor DAG

1. Open http://localhost:8080
2. Log in with `admin / admin`
3. Find DAG: `rideflow_realtime_pipeline`
4. Toggle the **ON** switch to activate it
5. Click **Trigger DAG** for the first manual run

### Airflow DAG Task Flow

```
check_kafka_health
      ↓
ensure_bronze_streaming_running
      ↓
ensure_silver_streaming_running
      ↓
run_gold_aggregation_job
      ↓
validate_gold_output
      ↓
branch_maintenance_decision
      ├── [if 00:00 UTC] run_maintenance_job ─┐
      └── [otherwise] skip_maintenance ────────┤
                                               ↓
                                    send_pipeline_report
```

---

## 16. Step 9 — Run Analytics SQL Queries

All 12 SQL queries in `analytics/analytics_queries.sql` are designed to run in **Databricks SQL**, **Tableau**, or **Power BI** against the Gold Delta tables.

### Where to Run

**Option A — Databricks SQL Editor:**
1. Go to **SQL Editor** in Databricks workspace
2. Select your SQL Warehouse
3. Open `analytics/analytics_queries.sql`
4. Run each query block

**Option B — From a Databricks Notebook:**
```python
spark.sql("""
    SELECT city,
           SUM(total_rides) AS rides_last_15min,
           ROUND(AVG(avg_fare), 2) AS avg_fare_usd
    FROM rideflow_gold.city_ride_metrics
    WHERE window_start >= CURRENT_TIMESTAMP() - INTERVAL 15 MINUTES
    GROUP BY city ORDER BY rides_last_15min DESC
""").show()
```

### Query Reference Sheet

| Query # | Name | Table Used | Purpose |
|---------|------|-----------|---------|
| 1 | Real-Time City Dashboard | `city_ride_metrics` | Top-level KPIs per city, last 15 min |
| 2 | Surge Pricing Alerts | `surge_pricing` | Active surge zones, real-time ops |
| 3 | Driver Leaderboard | `driver_utilization` | Top 20 drivers by efficiency score |
| 4 | Hourly Revenue Trend | `city_ride_metrics` | Last 24h revenue by city |
| 5 | Funnel Conversion | `funnel_analysis` | Ride request → payment conversion |
| 6 | Demand vs Supply Gap | `surge_pricing` | Unmet demand + dominant surge tier |
| 7 | Time Travel Revenue Comparison | `city_ride_metrics` | WoW revenue growth via Delta time travel |
| 8 | Data Quality Monitoring | `ride_events_dead_letter` | Dead-letter breakdown by reason |
| 9 | Peak Hour Analysis | `city_ride_metrics` | Best/worst hours per city over 30 days |
| 10 | ACID MERGE — Fare Correction | `ride_events_clean` | Reprocess and correct bad fares |
| 11 | Delta Time Travel | `ride_events_clean` | `VERSION AS OF` + `TIMESTAMP AS OF` |
| 12 | Z-ORDER Validation | `ride_events_clean` | EXPLAIN query to verify pruning |

---

## 17. Step 10 — Monitor the Pipeline

### Real-Time Query Status (In Notebook)

```python
from monitoring.monitoring import PipelineHealthMonitor

monitor = PipelineHealthMonitor(spark, poll_interval_sec=30)

# Check all streaming queries
query_statuses = monitor.check_streaming_queries()
for s in query_statuses:
    print(f"Query: {s['query_name']} | Active: {s['is_active']} | "
          f"Rows/sec: {s['input_rows_per_sec']:.1f} | Batch: {s['batch_id']}")
```

### Dead-Letter Rate Check

```python
dl_stats = monitor.check_dead_letter_rate()
print(f"Dead-letter rate: {dl_stats['dead_letter_pct']:.2f}%")
# Alert fires if > 5% (configured in config.py: MAX_DEAD_LETTER_PCT)
```

### Overall Pipeline Metrics

```python
metrics = monitor.get_pipeline_metrics()
# Returns: bronze_row_count, silver_row_count, gold_city_row_count, silver_dq_issue_rate
for k, v in metrics.items():
    print(f"  {k}: {v}")
```

### Databricks Streaming Query UI

In Databricks:
1. Go to **Compute → Your Cluster → Spark UI**
2. Click **Structured Streaming** tab
3. See real-time throughput, batch durations, and watermark progress

### Custom Slack Alert Setup

In `config/config.py`:
```python
SLACK_WEBHOOK_URL = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
MAX_DEAD_LETTER_PCT = 5.0   # Alert threshold
```

Alerts fire when:
- Any streaming query stops unexpectedly
- Dead-letter rate exceeds 5%
- Gold output validation returns 0 rows

---

## 18. Delta Lake Operations

These operations can be run on-demand from the Databricks notebook, SQL Editor, or locally.

### Time Travel — Read Historical Data

```sql
-- Read Silver table at version 5
SELECT COUNT(*) FROM rideflow_silver.ride_events_clean VERSION AS OF 5;

-- Read Silver table as it was 24 hours ago
SELECT city, COUNT(*) AS rides
FROM rideflow_silver.ride_events_clean
TIMESTAMP AS OF (CURRENT_TIMESTAMP() - INTERVAL 24 HOURS)
GROUP BY city;

-- View full transaction history
DESCRIBE HISTORY rideflow_silver.ride_events_clean;
```

```python
# Python equivalent
from delta_utils.delta_tables_init import read_table_at_version, read_table_at_timestamp, show_table_history

# View history
show_table_history(spark, "rideflow_silver.ride_events_clean", limit=10)

# Read at specific version
df_v2 = read_table_at_version(spark, "rideflow_silver.ride_events_clean", version=2)
print(f"Row count at version 2: {df_v2.count()}")
```

### MERGE — Update Existing Records

```sql
-- Correct invalid fares for New York on a specific date
MERGE INTO rideflow_silver.ride_events_clean AS target
USING (
    SELECT ride_id, event_type, fare_amount * 1.10 AS corrected_fare
    FROM rideflow_silver.ride_events_clean
    WHERE city = 'New York' AND event_date = '2026-03-15' AND dq_fare_out_of_range = TRUE
) AS source
ON target.ride_id = source.ride_id AND target.event_type = source.event_type
WHEN MATCHED THEN
    UPDATE SET
        target.fare_amount = source.corrected_fare,
        target.net_fare    = ROUND(source.corrected_fare * 0.82, 2),
        target.dq_fare_out_of_range = FALSE,
        target.has_data_quality_issue = FALSE;
```

### OPTIMIZE + Z-ORDER — Performance Tuning

```sql
-- Optimize Silver table (run nightly)
OPTIMIZE rideflow_silver.ride_events_clean ZORDER BY (ride_id, driver_id, rider_id);

-- Optimize Gold city metrics
OPTIMIZE rideflow_gold.city_ride_metrics ZORDER BY (city, window_start);
```

### VACUUM — Remove Old File Versions

```sql
-- Remove files older than 7 days (default retention)
VACUUM rideflow_silver.ride_events_clean RETAIN 168 HOURS;
VACUUM rideflow_gold.city_ride_metrics   RETAIN 168 HOURS;
```

```powershell
# Run all maintenance at once via Python script
python delta_utils/delta_tables_init.py --action maintain
```

### Schema Evolution — Add a New Field

If the Kafka producer adds a new field (e.g., `vehicle_type`), Bronze handles it automatically because:
```python
# In bronze_ingestion.py:
.option("mergeSchema", "true")   ← This allows new columns to be added
```

No downtime or schema migration needed.

---

## 19. Troubleshooting Common Errors

### Error 1: `JAVA_HOME is not set`
```
Error: JAVA_HOME is not set and Java could not be found in the PATH
```
**Fix:**
```powershell
$env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-17.0.x.x-hotspot"
$env:PATH += ";$env:JAVA_HOME\bin"
```

---

### Error 2: Kafka Connection Refused
```
org.apache.kafka.common.errors.TimeoutException: Failed to update metadata
```
**Fix:** Ensure Kafka is running:
```powershell
# Check Docker containers
docker ps | Select-String "kafka"

# If not running:
docker-compose up -d kafka
```

---

### Error 3: S3 Access Denied
```
com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied (403)
```
**Fix:** Check AWS credentials and IAM permissions:
```powershell
aws sts get-caller-identity                              # Verify identity
aws s3 ls s3://rideflow-lakehouse                        # Test S3 access
```

---

### Error 4: Delta Table Not Found
```
AnalysisException: Table or view not found: rideflow_bronze.ride_events_raw
```
**Fix:** Run table initialisation first:
```powershell
python delta_utils/delta_tables_init.py --action init
```

---

### Error 5: Streaming Query Fails with OOM
```
java.lang.OutOfMemoryError: GC overhead limit exceeded
```
**Fix:** Reduce `maxOffsetsPerTrigger` in `bronze_ingestion.py`:
```python
"maxOffsetsPerTrigger": "10000",    # Reduce from 50000
```
Also increase driver memory:
```python
.config("spark.driver.memory", "4g")
.config("spark.executor.memory", "4g")
```

---

### Error 6: Checkpoint Mismatch / Schema Incompatibility
```
StreamingQueryException: Schema is not compatible with the existing checkpoint schema
```
**Fix:** Delete the checkpoint and restart (only safe for non-production):
```powershell
aws s3 rm s3://rideflow-lakehouse/checkpoints/bronze/ride_events --recursive
python ingestion/bronze_ingestion.py
```

---

### Error 7: Dead-Letter Rate > 5% Alert Fires
Check what's causing corrupt records:
```sql
SELECT dead_letter_reason, COUNT(*) AS cnt
FROM rideflow_bronze.ride_events_dead_letter
WHERE event_date = CURRENT_DATE()
GROUP BY dead_letter_reason ORDER BY cnt DESC;
```
Then check the Kafka producer logs for schema changes.

---

### Error 8: Airflow DAG Import Error
```
ImportError: No module named 'airflow.providers.databricks'
```
**Fix:**
```powershell
pip install apache-airflow-providers-databricks==6.3.0
```

---

## 20. Performance Tuning Guide

### Spark Configuration Recommendations

| Config | Dev Value | Prod Value | Description |
|--------|-----------|------------|-------------|
| `spark.sql.shuffle.partitions` | 8 | 200 | Tune to data volume |
| `maxOffsetsPerTrigger` | 5,000 | 50,000 | Kafka batch size per trigger |
| `spark.streaming.backpressure.enabled` | true | true | Auto-throttle |
| `spark.executor.memory` | 2g | 8g | Per executor RAM |
| `spark.executor.cores` | 2 | 4 | Cores per executor |

### Delta Lake Tuning

| Action | When | Command |
|--------|------|---------|
| `OPTIMIZE` | Nightly | `OPTIMIZE table ZORDER BY (col1, col2)` |
| `VACUUM` | Weekly | `VACUUM table RETAIN 168 HOURS` |
| Auto-compact | Always on | `SET delta.autoOptimize.autoCompact = true` |
| Target file size | At creation | `delta.autoOptimize.target.file.size.bytes = 134217728` (128 MB) |

### Kafka Producer Tuning

```python
# In kafka_producer.py — increase throughput:
"linger.ms": 20,          # Allow longer batching → larger batches
"batch.size": 131072,     # 128 KB batches (from 64 KB)
"compression.type": "lz4" # LZ4 is faster than Snappy for high throughput
```

### Partitioning Strategy

- **Bronze/Silver**: Partitioned by `(event_date, city)` → enables date + city partition pruning
- **Gold**: Partitioned by `(window_start_date, city)` → time-range queries skip old partitions
- **Avoid over-partitioning**: Don't partition by high-cardinality cols like `driver_id`

---

## 21. Production Deployment Checklist

Use this checklist before going live with real traffic.

### Infrastructure
- [ ] S3 bucket created with versioning enabled
- [ ] IAM role attached to Databricks cluster with S3 read/write permissions
- [ ] Kafka cluster deployed with replication factor ≥ 3
- [ ] Databricks workspace provisioned in same AWS region as S3

### Security
- [ ] All secrets stored in Databricks Secret Scopes (no hardcoded credentials)
- [ ] `KAFKA_SECURITY_PROTOCOL = "SASL_SSL"` for production Kafka
- [ ] S3 block public access enabled
- [ ] Databricks cluster access control enabled

### Pipeline
- [ ] `delta_tables_init.py --action init` run successfully
- [ ] Bronze streaming query healthy (0 dead-letter on test events)
- [ ] Silver MERGE confirmed (no duplicate `(ride_id, event_type)` pairs)
- [ ] Gold tables populated for at least 1 full hour
- [ ] All 4 Gold tables have > 0 rows
- [ ] Analytics query #1 returns results

### Monitoring
- [ ] Slack webhook configured and tested (send a test alert)
- [ ] Dead-letter alert threshold set in `config.py`
- [ ] Databricks cluster auto-termination disabled for streaming clusters
- [ ] Airflow DAG activated and first run completed successfully

### Documentation
- [ ] Databricks Workflow job IDs recorded
- [ ] Airflow Variables set for all job IDs
- [ ] On-call runbook shared with team
- [ ] S3 bucket name updated in `config.py`

---

## Quick Reference — Commands Summary

```powershell
# ── Activate environment ─────────────────────────────────────────────
.venv\Scripts\Activate.ps1

# ── Step 0: Init tables (always first) ──────────────────────────────
python delta_utils/delta_tables_init.py --action init

# ── Step 1: Start Kafka producer (keep running) ──────────────────────
python data_simulation/kafka_producer.py --events-per-sec 500

# ── Step 2: Bronze streaming (keep running) ──────────────────────────
python ingestion/bronze_ingestion.py

# ── Step 3: Silver streaming (keep running) ──────────────────────────
python transformation/silver_transformation.py

# ── Step 4: Gold batch (run every hour) ─────────────────────────────
python gold/gold_aggregations.py --since-hours 2

# ── Maintenance (run nightly) ────────────────────────────────────────
python delta_utils/delta_tables_init.py --action maintain

# ── Table stats ──────────────────────────────────────────────────────
python delta_utils/delta_tables_init.py --action stats

# ── Table history (time travel) ──────────────────────────────────────
python delta_utils/delta_tables_init.py --action history --table rideflow_silver.ride_events_clean

# ── Deploy Databricks Workflow ───────────────────────────────────────
databricks jobs create --json-file orchestration/databricks_workflow.json

# ── Trigger Workflow manually ────────────────────────────────────────
databricks jobs run-now --job-id <JOB_ID>

# ── Start Airflow ────────────────────────────────────────────────────
airflow webserver --port 8080 &
airflow scheduler &
```

---

*End of RideFlow Pipeline Runbook — v1.0 | March 2026*
