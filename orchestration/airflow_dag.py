"""
=============================================================================
  RideFlow — Apache Airflow DAG
  Pipeline Orchestration: Databricks Jobs + Health Checks
  
  Schedule:  Hourly (Gold batch) + Nightly (OPTIMIZE/VACUUM maintenance)
  
  Tasks:
  1. check_kafka_health        — Verify Kafka topic is receiving events
  2. run_bronze_job            — Trigger Databricks streaming Bronze job
  3. run_silver_job            — Trigger Databricks streaming Silver job
  4. run_gold_batch            — Trigger Databricks Gold batch aggregation job
  5. validate_gold_output      — Assert Gold row counts are non-zero
  6. run_maintenance           — Trigger nightly OPTIMIZE + VACUUM job
  7. send_pipeline_report      — Post metrics summary to Slack
=============================================================================
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.models import Variable
import logging

log = logging.getLogger("RideFlowDAG")

# ── DAG Configuration ─────────────────────────────────────────────────────────

DAG_ID   = "rideflow_realtime_pipeline"
OWNER    = "data-engineering"
SCHEDULE = "@hourly"

# Pull from Airflow Variables (set via Airflow UI or CLI)
DATABRICKS_CONN_ID  = "databricks_default"
DATABRICKS_JOB_IDS  = {
    "bronze":      int(Variable.get("RIDEFLOW_BRONZE_JOB_ID", default_var=1001)),
    "silver":      int(Variable.get("RIDEFLOW_SILVER_JOB_ID", default_var=1002)),
    "gold":        int(Variable.get("RIDEFLOW_GOLD_JOB_ID",   default_var=1003)),
    "maintenance": int(Variable.get("RIDEFLOW_MAINT_JOB_ID",  default_var=1004)),
}
SLACK_WEBHOOK = Variable.get("RIDEFLOW_SLACK_WEBHOOK", default_var="")

DEFAULT_ARGS = {
    "owner":            OWNER,
    "depends_on_past":  False,
    "email":            ["data-engineering-alerts@rideflow.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "start_date":       datetime(2024, 1, 1),
}

# ── Task Functions ────────────────────────────────────────────────────────────

def check_kafka_health(**context):
    """
    Lightweight connectivity check — verifies the Kafka bootstrap is reachable.
    In production, use the Kafka Admin Client to fetch topic metadata.
    """
    from confluent_kafka.admin import AdminClient

    kafka_bootstrap = Variable.get("KAFKA_BOOTSTRAP_SERVERS", default_var="localhost:9092")
    log.info("Checking Kafka health: %s", kafka_bootstrap)

    admin = AdminClient({"bootstrap.servers": kafka_bootstrap})
    cluster_meta = admin.list_topics(timeout=10)
    topics = list(cluster_meta.topics.keys())
    log.info("Kafka accessible. Topics: %s", topics)

    if "ride-events" not in topics:
        raise ValueError("Kafka topic 'ride-events' not found! Producer may not be running.")
    log.info("✅ Kafka health check passed.")


def validate_gold_output(**context):
    """
    Assert Gold aggregation tables have been written successfully.
    Uses Databricks REST API to query table row counts via SQL endpoint.
    """
    import requests

    databricks_host  = Variable.get("DATABRICKS_HOST")
    databricks_token = Variable.get("DATABRICKS_TOKEN")

    headers = {
        "Authorization": f"Bearer {databricks_token}",
        "Content-Type": "application/json",
    }

    # Simple SQL via Databricks SQL Warehouse REST API
    validation_queries = [
        "SELECT COUNT(*) AS cnt FROM rideflow_gold.city_ride_metrics",
        "SELECT COUNT(*) AS cnt FROM rideflow_gold.driver_utilization",
        "SELECT COUNT(*) AS cnt FROM rideflow_gold.surge_pricing",
    ]

    for query in validation_queries:
        payload = {
            "statement": query,
            "warehouse_id": Variable.get("DATABRICKS_SQL_WAREHOUSE_ID", default_var=""),
            "wait_timeout": "30s",
        }
        resp = requests.post(
            f"{databricks_host}/api/2.0/sql/statements",
            headers=headers, json=payload, timeout=60,
        )
        resp.raise_for_status()
        result = resp.json()
        count  = int(result["result"]["data_array"][0][0])
        log.info("Validation | query='%s' | count=%d", query, count)
        if count == 0:
            raise ValueError(f"Gold table returned 0 rows for: {query}")

    log.info("✅ Gold output validation passed.")


def send_pipeline_report(**context):
    """Post a pipeline completion report to Slack."""
    import requests

    if not SLACK_WEBHOOK:
        log.info("Slack webhook not set. Skipping report.")
        return

    execution_date = context.get("ds", "N/A")
    message = {
        "text": (
            f"*✅ RideFlow Pipeline Run Complete*\n"
            f"• Run Date: `{execution_date}`\n"
            f"• Bronze → Silver → Gold: All layers processed successfully\n"
            f"• DAG: `{DAG_ID}`\n"
            f"• Triggered by: Airflow Scheduler"
        ),
        "username": "RideFlow Airflow",
        "icon_emoji": ":car:",
    }
    resp = requests.post(SLACK_WEBHOOK, json=message, timeout=5)
    resp.raise_for_status()
    log.info("Pipeline report sent to Slack.")


def decide_maintenance_branch(**context):
    """
    Only run maintenance on nightly schedules (midnight UTC).
    Returns the task_id to branch to.
    """
    execution_hour = context["execution_date"].hour
    if execution_hour == 0:
        return "run_maintenance_job"
    return "skip_maintenance"


# ── DAG Definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id          = DAG_ID,
    default_args    = DEFAULT_ARGS,
    schedule_interval = SCHEDULE,
    catchup         = False,
    max_active_runs = 1,
    tags            = ["rideflow", "lakehouse", "databricks", "streaming"],
    description     = "RideFlow real-time ride-sharing analytics pipeline",
) as dag:

    # ── Task 1: Kafka Health Check ────────────────────────────────────────
    t_kafka_health = PythonOperator(
        task_id         = "check_kafka_health",
        python_callable = check_kafka_health,
    )

    # ── Task 2: Bronze Streaming Job (idempotent — always running) ────────
    # Bronze streaming runs continuously on Databricks.
    # This task only ensures the job is in a running state.
    t_bronze = DatabricksRunNowOperator(
        task_id           = "ensure_bronze_streaming_running",
        databricks_conn_id = DATABRICKS_CONN_ID,
        job_id            = DATABRICKS_JOB_IDS["bronze"],
        notebook_params   = {"layer": "bronze", "run_ts": "{{ ts }}"},
    )

    # ── Task 3: Silver Streaming Job ─────────────────────────────────────
    t_silver = DatabricksRunNowOperator(
        task_id           = "ensure_silver_streaming_running",
        databricks_conn_id = DATABRICKS_CONN_ID,
        job_id            = DATABRICKS_JOB_IDS["silver"],
        notebook_params   = {"layer": "silver", "run_ts": "{{ ts }}"},
    )

    # ── Task 4: Gold Batch Aggregation ───────────────────────────────────
    t_gold = DatabricksRunNowOperator(
        task_id           = "run_gold_aggregation_job",
        databricks_conn_id = DATABRICKS_CONN_ID,
        job_id            = DATABRICKS_JOB_IDS["gold"],
        notebook_params   = {
            "layer":       "gold",
            "since_hours": "2",
            "execution_date": "{{ ds }}",
        },
    )

    # ── Task 5: Validate Gold output ─────────────────────────────────────
    t_validate = PythonOperator(
        task_id         = "validate_gold_output",
        python_callable = validate_gold_output,
    )

    # ── Task 6: Conditional Maintenance (nightly only) ────────────────────
    t_branch_maintenance = BranchPythonOperator(
        task_id         = "branch_maintenance_decision",
        python_callable = decide_maintenance_branch,
    )

    t_maintenance = DatabricksRunNowOperator(
        task_id           = "run_maintenance_job",
        databricks_conn_id = DATABRICKS_CONN_ID,
        job_id            = DATABRICKS_JOB_IDS["maintenance"],
        notebook_params   = {"action": "maintain"},
    )

    t_skip_maintenance = EmptyOperator(task_id="skip_maintenance")

    # ── Task 7: Slack Report ─────────────────────────────────────────────
    t_report = PythonOperator(
        task_id         = "send_pipeline_report",
        python_callable = send_pipeline_report,
        trigger_rule    = "none_failed_min_one_success",   # Run even if maintenance skipped
    )

    # ── DAG Dependencies ─────────────────────────────────────────────────
    #
    #   check_kafka_health
    #         │
    #   bronze → silver → gold → validate → branch_maintenance
    #                                              │            │
    #                                   run_maintenance  skip_maintenance
    #                                              │            │
    #                                       send_pipeline_report
    #
    (
        t_kafka_health
        >> t_bronze
        >> t_silver
        >> t_gold
        >> t_validate
        >> t_branch_maintenance
        >> [t_maintenance, t_skip_maintenance]
        >> t_report
    )
