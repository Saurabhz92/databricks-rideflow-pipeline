<<<<<<< HEAD
# databricks-rideflow-pipeline
FAANG-Level Real-Time Ride Sharing Analytics Platform — Databricks Lakehouse
=======
# 🚗 RideFlow Analytics Platform
## FAANG-Level Real-Time Ride Sharing Data Pipeline | Databricks Lakehouse

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com)
[![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?style=for-the-badge&logo=apache-kafka&logoColor=white)](https://kafka.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=for-the-badge)](https://delta.io)
[![AWS S3](https://img.shields.io/badge/AWS_S3-232F3E?style=for-the-badge&logo=amazonaws&logoColor=white)](https://aws.amazon.com/s3)

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    RideFlow Real-Time Analytics Architecture                │
└─────────────────────────────────────────────────────────────────────────────┘

  Mobile Apps / Ride Events
         │
         ▼
  ┌─────────────────┐     500 events/sec    ┌──────────────────────┐
  │  Kafka Producer │ ──────────────────►   │   Apache Kafka       │
  │ (data_simulation│                       │   Topic: ride-events │
  │  /kafka_producer│                       │   10 partitions      │
  │  .py)           │                       └──────────┬───────────┘
  └─────────────────┘                                  │
                                                       │ Structured Streaming
                                                       ▼
  ┌──────────────────────────────────────────────────────────────────────────┐
  │                    DATABRICKS LAKEHOUSE (AWS S3)                         │
  │                                                                          │
  │   ┌───────────────────┐   ┌────────────────────┐   ┌─────────────────┐  │
  │   │  🟤 BRONZE LAYER  │   │  🥈 SILVER LAYER   │   │ 🥇 GOLD LAYER  │  │
  │   │                   │   │                    │   │                 │  │
  │   │ • Raw Kafka events│──►│ • Cleaned records  │──►│ • City metrics  │  │
  │   │ • Schema evolution│   │ • Deduplication    │   │ • Driver util   │  │
  │   │ • Dead-letter     │   │ • DQ flags         │   │ • Surge pricing │  │
  │   │ • Partitioned by  │   │ • MERGE upserts    │   │ • Funnel stats  │  │
  │   │   date + city     │   │ • Partitioned by   │   │ • Partitioned   │  │
  │   │                   │   │   date + city      │   │   by date+city  │  │
  │   └───────────────────┘   └────────────────────┘   └────────┬────────┘  │
  │                                                              │           │
  │   ┌───────────────────────────────────────────────────────┐ │           │
  │   │  Delta Lake Features                                   │ │           │
  │   │  ✅ ACID Transactions  ✅ Time Travel                  │ │           │
  │   │  ✅ MERGE/Upserts     ✅ Schema Enforcement            │ │           │
  │   │  ✅ Z-Ordering        ✅ Auto-Optimize                 │ │           │
  │   └───────────────────────────────────────────────────────┘ │           │
  └──────────────────────────────────────────────────────────────┼───────────┘
                                                                 │
                                                    ┌────────────▼───────────┐
                                                    │   BI / Dashboards      │
                                                    │   Tableau / Power BI   │
                                                    │   Databricks SQL       │
                                                    └────────────────────────┘
  
  ┌──────────────────────────────────────────────────────────────────────────┐
  │  Orchestration & Monitoring                                              │
  │  Apache Airflow (@hourly DAG) + Databricks Workflows (6-task pipeline)  │
  │  Monitoring: Slack alerts, dead-letter rate checks, streaming health     │
  └──────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
DATABRICKS ADVANCED PIPELINE/
│
├── config/
│   └── config.py                    # Central configuration (S3, Kafka, Delta, Spark)
│
├── data_simulation/
│   └── kafka_producer.py            # High-throughput synthetic event producer (500 evt/s)
│
├── ingestion/
│   └── bronze_ingestion.py          # Bronze: Kafka → Delta streaming, dead-letter routing
│
├── transformation/
│   └── silver_transformation.py     # Silver: Cleaning, dedup, DQ flags, MERGE upserts
│
├── gold/
│   └── gold_aggregations.py         # Gold: City metrics, driver util, surge, funnel
│
├── delta_utils/
│   └── delta_tables_init.py         # Delta DDLs, time travel, MERGE, VACUUM/OPTIMIZE
│
├── orchestration/
│   ├── airflow_dag.py               # Airflow DAG with Databricks operators
│   └── databricks_workflow.json     # Databricks Workflow with 6 tasks, 3 cluster types
│
├── notebooks/
│   └── end_to_end_pipeline.py       # Master Databricks notebook (all layers + monitoring)
│
├── analytics/
│   └── analytics_queries.sql        # 12 production SQL queries for BI dashboards
│
├── monitoring/
│   └── monitoring.py                # JSON logging, health checks, alerts, retry decorator
│
├── requirements.txt
└── README.md
```

---

## 🛠️ Technology Stack

| Technology               | Purpose                                         |
|--------------------------|-------------------------------------------------|
| **Databricks Runtime 14.3** | Lakehouse platform, Spark execution engine   |
| **Apache Spark (PySpark)**  | Distributed data processing                  |
| **Apache Kafka**            | Real-time event streaming (ride events)      |
| **Delta Lake**              | ACID storage, time travel, schema evolution  |
| **AWS S3**                  | Object storage for Delta tables              |
| **Apache Airflow**          | External pipeline scheduling & orchestration |
| **Databricks Workflows**    | Native job orchestration within Databricks   |

---

## ⚡ Quick Start

### 1. Prerequisites
```bash
# Python 3.10+
pip install -r requirements.txt

# Set environment variables
export KAFKA_BOOTSTRAP_SERVERS="your-kafka-broker:9092"
export S3_BUCKET="your-data-lake-bucket"
export DATABRICKS_HOST="https://your-workspace.azuredatabricks.net"
export DATABRICKS_TOKEN="dapi..."
```

### 2. Initialise Delta Tables
```bash
python delta_utils/delta_tables_init.py --action init
```

### 3. Start Kafka Producer
```bash
python data_simulation/kafka_producer.py --events-per-sec 500 --duration-mins 60
```

### 4. Run Pipeline Layers
```bash
# Terminal 1 — Bronze
python ingestion/bronze_ingestion.py

# Terminal 2 — Silver  
python transformation/silver_transformation.py

# Terminal 3 — Gold (batch, run every hour)
python gold/gold_aggregations.py --since-hours 2
```

### 5. Run Monitoring
```bash
python monitoring/monitoring.py
```

---

## 🔑 Key Design Decisions

### Why `foreachBatch` over `writeStream.format("delta")`?
`foreachBatch` gives full control per micro-batch:
- Run the MERGE logic (upserts) that streaming Delta writes don't natively support
- Split valid vs dead-letter records in the same batch pass
- Cache the micro-batch for multiple transformations without re-reading Kafka

### Why Change Data Feed (CDF) from Bronze → Silver?
CDF enables **incremental reads** — Silver only processes newly inserted or updated Bronze rows instead of full table scans. Typically reduces Silver processing cost by **60-80%**.

### Why window-based aggregations with Watermarks?
Watermarks (`withWatermark("event_ts", "5 minutes")`) handle **late-arriving events** gracefully — a real-world concern with mobile apps dropping off network. Without watermarks, state for all open windows would accumulate indefinitely.

### Surge Pricing: Demand/Supply Join
The surge logic anti-joins ride requests (demand) against driver assignments (supply) within rolling 5-minute windows. The `left` join ensures cities with zero supply still appear in results with `demand_supply_ratio = ∞ → capped at drivers_active=1 via GREATEST()`.

---

## 📊 Data Model

### Bronze — `rideflow_bronze.ride_events_raw`
Raw events exactly as received from Kafka, with Kafka metadata columns.
Partitioned by `(event_date, city)`.

### Silver — `rideflow_silver.ride_events_clean`
Cleaned records with data quality flags. Deduplicated on `(ride_id, event_type)`.
Includes `net_fare` (after 18% platform cut), parsed `event_ts`, `event_hour`, `event_minute`.
Partitioned by `(event_date, city)`.

### Gold Tables
| Table                        | Granularity               | Key Metrics                             |
|------------------------------|---------------------------|-----------------------------------------|
| `city_ride_metrics`          | City × 1-min window       | Rides, revenue, avg fare, completion %  |
| `driver_utilization`         | Driver × 15-min sliding   | Efficiency score, utilization rate      |
| `surge_pricing`              | City × 5-min window       | Demand/supply ratio, surge tier         |
| `funnel_analysis`            | City × Day                | Requested → Payment conversion rates    |

---

## 🔒 Delta Lake Features Demonstrated

| Feature                | Where                                   |
|------------------------|-----------------------------------------|
| ACID Transactions      | All Delta writes                        |
| Time Travel            | `delta_tables_init.py` + `analytics_queries.sql` |
| MERGE (Upsert)         | Silver `process_silver_batch()`, Gold `upsert_gold_table()` |
| Schema Enforcement     | Bronze DDL `TBLPROPERTIES`              |
| Schema Evolution       | Bronze `option("mergeSchema", "true")`  |
| Change Data Feed (CDF) | Silver reads Bronze via CDF             |
| OPTIMIZE + Z-ORDER     | `run_maintenance()` in Delta utils      |
| VACUUM                 | Nightly cleanup (168 hours retention)   |
| Auto-Optimize          | `delta.autoOptimize.*` properties       |

---

## 📈 Capacity & Performance

| Metric                      | Value                              |
|-----------------------------|------------------------------------|
| Throughput (simulated)      | 500 events/sec = **1.8M/hour**     |
| Bronze data rate            | ~7.2 GB/hour (Snappy compressed)   |
| Silver (after dedup)        | ~4.3 GB/hour                       |
| Gold aggregations           | < 50 MB/hour                       |
| Cluster: Bronze/Silver      | 2-8 × m5.2xlarge (Spot + On-demand)|
| Cluster: Gold               | 1-4 × m5.xlarge                    |
| Checkpoint interval         | Every 30 seconds                   |
| Gold refresh latency        | < 5 minutes from event to dashboard|

---

## 🎯 Talking Points

1. **Exactly-Once Semantics**: Kafka idempotent producer + Spark checkpointing + Delta ACID = end-to-end exactly-once
2. **Backpressure**: `maxOffsetsPerTrigger` prevents Spark from overwhelming downstream Delta writers during traffic spikes
3. **Late Data Handling**: Watermarks (5 min on Gold windows) bound state size while tolerating mobile network delays
4. **Upsert Pattern**: MERGE ensures idempotent writes — safe to replay any Bronze batch without data corruption
5. **Cost Optimisation**: Spot instances with fallback (80% bid), CDF reduces Silver scan cost, Z-ORDER eliminates full partition scans
6. **Observability**: Structured JSON logs + dead-letter rate monitoring + Slack alerting = production-grade ops posture
7. **Schema Evolution**: `mergeSchema=true` on Bronze allows adding new event fields without reprocessing historical data

---

## 📮 Airflow DAG Schedule

```
@hourly: check_kafka_health → bronze → silver → gold → validate → (branch)
                                                                    ├── 00:00 UTC: run_maintenance
                                                                    └── Other hours: skip_maintenance
                                                                         ↓
                                                                  send_pipeline_report

