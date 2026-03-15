"""
=============================================================================
  RideFlow — Pipeline Monitoring & Logging
  
  Covers:
  - Structured JSON logging
  - Streaming query health checks
  - Dead-letter rate alerting
  - Slack / email notifications
  - Metric publishing (Delta metrics table)
=============================================================================
"""

import json
import logging
import smtplib
import time
from datetime import datetime, timezone
from email.mime.text import MIMEText
from typing import Optional

import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pythonjsonlogger import jsonlogger

import sys
sys.path.append("..")
from config.config import (
    ALERT_EMAIL, LOG_LEVEL, MAX_DEAD_LETTER_PCT,
    SLACK_WEBHOOK_URL, TBL_BRONZE_RIDES, TBL_DEAD_LETTER,
    TBL_SILVER_RIDES, TBL_GOLD_CITY_METRICS,
)

# ── Structured JSON Logger ────────────────────────────────────────────────────

def setup_json_logger(name: str) -> logging.Logger:
    """
    Create a JSON-structured logger compatible with Databricks log shipping.
    In Databricks, logs are streamed to log4j; JSON format aids downstream parsing.
    """
    logger    = logging.getLogger(name)
    handler   = logging.StreamHandler()
    formatter = jsonlogger.JsonFormatter(
        fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%SZ",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(getattr(logging, LOG_LEVEL))
    return logger


log = setup_json_logger("RideFlowMonitor")


# ── Pipeline Health Monitor ───────────────────────────────────────────────────

class PipelineHealthMonitor:
    """
    Monitors active streaming queries and overall pipeline health.
    Intended to run in a separate thread or Databricks notebook cell.
    """

    def __init__(self, spark: SparkSession, poll_interval_sec: int = 60):
        self.spark             = spark
        self.poll_interval     = poll_interval_sec
        self._query_snapshots  = {}

    def check_streaming_queries(self) -> list:
        """Return health status of all active streaming queries."""
        statuses = []
        for query in self.spark.streams.active:
            progress = query.lastProgress or {}
            status   = {
                "query_id":         str(query.id),
                "query_name":       query.name,
                "is_active":        query.isActive,
                "status":           query.status.get("message", "unknown"),
                "input_rows_per_sec":   progress.get("inputRowsPerSecond", 0),
                "processed_rows_per_sec": progress.get("processedRowsPerSecond", 0),
                "batch_id":         progress.get("batchId", -1),
                "num_input_rows":   progress.get("numInputRows", 0),
                "watermark":        progress.get("eventTime", {}).get("watermark", "N/A"),
                "checked_at":       datetime.now(timezone.utc).isoformat(),
            }
            statuses.append(status)

            # Alert on stalled query
            if not query.isActive:
                self._send_alert(
                    subject="🔴 STREAMING QUERY STOPPED",
                    message=f"Query '{query.name}' (id={query.id}) is no longer active.\n"
                            f"Last status: {query.status}",
                )
            elif status["input_rows_per_sec"] == 0 and status["batch_id"] > 5:
                log.warning("Query %s appears stalled (0 rows/sec)", query.name)

        return statuses

    def check_dead_letter_rate(self) -> dict:
        """
        Compute the dead-letter percentage over the last 1 hour.
        Alert if above configured threshold.
        """
        try:
            bronze_count = (
                self.spark.table(TBL_BRONZE_RIDES)
                .filter(F.col("ingestion_ts") >= F.date_sub(F.current_timestamp(), 1))
                .count()
            )
            dead_count = (
                self.spark.table(TBL_DEAD_LETTER)
                .filter(F.col("ingestion_ts") >= F.date_sub(F.current_timestamp(), 1))
                .count()
            )
            total     = bronze_count + dead_count
            dl_pct    = round(dead_count / max(total, 1) * 100, 2)

            result = {
                "bronze_count":  bronze_count,
                "dead_letter_count": dead_count,
                "dead_letter_pct": dl_pct,
                "threshold_pct": MAX_DEAD_LETTER_PCT,
                "alert_triggered": dl_pct > MAX_DEAD_LETTER_PCT,
            }

            if result["alert_triggered"]:
                self._send_alert(
                    subject=f"⚠️ Dead-letter rate {dl_pct:.1f}% exceeds threshold {MAX_DEAD_LETTER_PCT}%",
                    message=(
                        f"Dead-letter count: {dead_count} / {total} total records\n"
                        f"Dead-letter %: {dl_pct:.2f}%\n"
                        f"Investigate: {TBL_DEAD_LETTER}"
                    ),
                )
                log.error("Dead-letter alert: %.2f%% > threshold %.2f%%",
                          dl_pct, MAX_DEAD_LETTER_PCT)
            else:
                log.info("Dead-letter rate: %.2f%% (OK)", dl_pct)

            return result

        except Exception as exc:
            log.exception("Dead-letter check failed: %s", exc)
            return {"error": str(exc)}

    def get_pipeline_metrics(self) -> dict:
        """
        Collect high-level metrics across Bronze → Silver → Gold.
        Returns a summary dict suitable for dashboard / alerting.
        """
        metrics = {"collected_at": datetime.now(timezone.utc).isoformat()}
        tables  = {
            "bronze": TBL_BRONZE_RIDES,
            "silver": TBL_SILVER_RIDES,
            "gold_city": TBL_GOLD_CITY_METRICS,
        }
        for layer, table in tables.items():
            try:
                count = self.spark.table(table).count()
                metrics[f"{layer}_row_count"] = count
            except Exception:
                metrics[f"{layer}_row_count"] = -1

        # DQ issue rate
        try:
            silver_df = self.spark.table(TBL_SILVER_RIDES)
            total_silver = silver_df.count()
            dq_issues    = silver_df.filter(F.col("has_data_quality_issue")).count()
            metrics["silver_dq_issue_rate"] = round(dq_issues / max(total_silver, 1) * 100, 2)
        except Exception:
            metrics["silver_dq_issue_rate"] = -1.0

        log.info("Pipeline metrics: %s", json.dumps(metrics))
        return metrics

    def run_loop(self, duration_mins: int = 60):
        """
        Run the monitor loop for a specified duration.
        Called from a dedicated Databricks cluster.
        """
        end_time = time.time() + duration_mins * 60
        while time.time() < end_time:
            try:
                self.check_streaming_queries()
                self.check_dead_letter_rate()
                self.get_pipeline_metrics()
            except Exception as exc:
                log.exception("Monitor loop error: %s", exc)
            time.sleep(self.poll_interval)

    # ── Alerting Backends ─────────────────────────────────────────────────

    def _send_alert(self, subject: str, message: str):
        """Route alerts to Slack and/or email depending on configuration."""
        self._send_slack(subject, message)
        # self._send_email(subject, message)   # Uncomment to enable email

    def _send_slack(self, subject: str, message: str):
        """Post alert to Slack webhook."""
        if not SLACK_WEBHOOK_URL:
            log.debug("Slack webhook not configured — skipping alert: %s", subject)
            return
        payload = {
            "text": f"*{subject}*\n```{message}```",
            "username": "RideFlow Monitor",
            "icon_emoji": ":rotating_light:",
        }
        try:
            resp = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=5)
            resp.raise_for_status()
            log.info("Slack alert sent: %s", subject)
        except Exception as exc:
            log.error("Failed to send Slack alert: %s", exc)

    def _send_email(self, subject: str, message: str):
        """Send alert via SMTP (configure SMTP server in environment)."""
        if not ALERT_EMAIL:
            return
        msg       = MIMEText(message)
        msg["Subject"] = f"[RideFlow Alert] {subject}"
        msg["From"]    = "noreply@rideflow.com"
        msg["To"]      = ALERT_EMAIL
        try:
            with smtplib.SMTP("localhost") as server:
                server.sendmail("noreply@rideflow.com", [ALERT_EMAIL], msg.as_string())
        except Exception as exc:
            log.error("Email alert failed: %s", exc)


# ── Retry Decorator ───────────────────────────────────────────────────────────

def with_retry(max_attempts: int = 3, delay_sec: float = 5.0):
    """
    Decorator for automatic retry with exponential backoff.
    Use on Delta writes or external API calls.
    """
    import functools

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    if attempt == max_attempts:
                        log.error("Max retries (%d) exceeded for %s: %s",
                                  max_attempts, func.__name__, exc)
                        raise
                    sleep_for = delay_sec * (2 ** (attempt - 1))
                    log.warning("Attempt %d/%d failed for %s. Retrying in %.1fs...",
                                attempt, max_attempts, func.__name__, sleep_for)
                    time.sleep(sleep_for)
        return wrapper
    return decorator


# ── Log Pipeline Run ──────────────────────────────────────────────────────────

def log_pipeline_run(spark: SparkSession, layer: str, batch_id: int,
                     rows_in: int, rows_out: int, duration_sec: float,
                     status: str = "SUCCESS", error: Optional[str] = None):
    """
    Write a pipeline run log entry to a Delta audit table.
    Useful for operational dashboards and SLA tracking.
    """
    run_log = spark.createDataFrame([{
        "run_ts":        datetime.now(timezone.utc).isoformat(),
        "layer":         layer,
        "batch_id":      batch_id,
        "rows_in":       rows_in,
        "rows_out":      rows_out,
        "duration_sec":  duration_sec,
        "status":        status,
        "error_message": error or "",
    }])
    (
        run_log.write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .save("s3a://rideflow-lakehouse/delta/audit/pipeline_runs")
    )
