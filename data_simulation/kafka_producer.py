"""
=============================================================================
  RideFlow — High-Throughput Kafka Producer (Data Simulation)
  Simulates real-time ride-sharing events at ~500 events/second
  
  Usage:
      python kafka_producer.py
      python kafka_producer.py --events-per-sec 1000 --duration-mins 30
=============================================================================
"""

import argparse
import json
import logging
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, Any, List

from confluent_kafka import Producer, KafkaException
from faker import Faker

# ── Internal imports ─────────────────────────────────────────────────────────
sys.path.append("..")
from config.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_RIDES,
    KAFKA_SECURITY_PROTOCOL,
    KAFKA_SASL_USERNAME,
    KAFKA_SASL_PASSWORD,
    SIM_CITIES,
    SIM_EVENTS_PER_SEC,
    SIM_BATCH_SIZE,
    SIM_SLEEP_MS,
    VALID_EVENT_TYPES,
    LOG_LEVEL,
)

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("RideFlowProducer")

# ── Global shutdown flag ──────────────────────────────────────────────────────
_RUNNING = True


def _handle_signal(sig, frame):
    global _RUNNING
    log.info("Received shutdown signal. Flushing producer and exiting...")
    _RUNNING = False


signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)


# ── Kafka Producer Factory ────────────────────────────────────────────────────
def build_producer() -> Producer:
    """Build and return a configured Confluent Kafka Producer."""
    conf: Dict[str, Any] = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "security.protocol": KAFKA_SECURITY_PROTOCOL,
        # Throughput tuning
        "linger.ms": 10,                # Allow batching for 10 ms
        "batch.size": 65536,            # 64 KB batch
        "compression.type": "snappy",
        "acks": "all",                  # Full ISR acknowledgement
        "retries": 5,
        "retry.backoff.ms": 500,
        # Idempotent to avoid duplicates
        "enable.idempotence": True,
        "max.in.flight.requests.per.connection": 5,
    }

    if KAFKA_SECURITY_PROTOCOL in ("SASL_SSL", "SASL_PLAINTEXT"):
        conf.update({
            "sasl.mechanism": "PLAIN",
            "sasl.username": KAFKA_SASL_USERNAME,
            "sasl.password": KAFKA_SASL_PASSWORD,
        })

    return Producer(conf)


# ── Synthetic Event Generator ─────────────────────────────────────────────────
fake = Faker()

# Pre-generate a pool of driver/rider IDs to simulate realistic repeat users
_DRIVER_POOL = [f"DRV-{str(uuid.uuid4())[:8].upper()}" for _ in range(5_000)]
_RIDER_POOL  = [f"RDR-{str(uuid.uuid4())[:8].upper()}" for _ in range(50_000)]

# Event type probability weights (reflects real-world funnel)
_EVENT_WEIGHTS = {
    "ride_requested":    0.30,
    "driver_assigned":   0.25,
    "trip_started":      0.20,
    "trip_completed":    0.15,
    "payment_completed": 0.10,
}
_EVENT_TYPES   = list(_EVENT_WEIGHTS.keys())
_EVENT_PROBS   = list(_EVENT_WEIGHTS.values())

# City-level surge multipliers (simulate demand spikes)
_SURGE_MAP = {
    "New York":        1.5,
    "San Francisco":   1.8,
    "Chicago":         1.2,
    "Los Angeles":     1.4,
    "Seattle":         1.3,
    "Austin":          1.1,
    "Miami":           1.6,
    "Boston":          1.3,
    "Denver":          1.0,
    "Atlanta":         1.1,
}


def generate_ride_event(ride_id: str, city: str, event_type: str) -> Dict[str, Any]:
    """Generate a single synthetic ride event with realistic values."""
    surge      = _SURGE_MAP.get(city, 1.0)
    base_fare  = round(random.uniform(3.0, 80.0), 2)
    distance   = round(random.uniform(0.5, 45.0), 2)

    # Add realistic noise: ~2% corrupt/anomalous records
    if random.random() < 0.02:
        fare_amount = random.choice([None, -1.0, 99999.0, "INVALID"])
    else:
        fare_amount = round(base_fare * surge, 2)

    return {
        "ride_id":       ride_id,
        "driver_id":     random.choice(_DRIVER_POOL),
        "rider_id":      random.choice(_RIDER_POOL),
        "city":          city,
        "event_type":    event_type,
        "trip_distance": distance,
        "fare_amount":   fare_amount,
        "surge_multiplier": surge,
        "timestamp":     datetime.now(timezone.utc).isoformat(),
        "kafka_partition_key": city,  # Route by city for locality
        "schema_version": "1.0",
    }


def generate_batch(batch_size: int = SIM_BATCH_SIZE) -> List[Dict[str, Any]]:
    """Generate a batch of ride events with correlated ride_ids."""
    events = []
    for _ in range(batch_size):
        ride_id    = f"RIDE-{str(uuid.uuid4())[:12].upper()}"
        city       = random.choice(SIM_CITIES)
        event_type = random.choices(_EVENT_TYPES, weights=_EVENT_PROBS, k=1)[0]
        events.append(generate_ride_event(ride_id, city, event_type))
    return events


# ── Delivery Callback ─────────────────────────────────────────────────────────
_stats = {"sent": 0, "failed": 0, "start_time": time.time()}


def _delivery_callback(err, msg):
    global _stats
    if err:
        log.error("Delivery FAILED | topic=%s partition=%d | %s",
                  msg.topic(), msg.partition(), err)
        _stats["failed"] += 1
    else:
        _stats["sent"] += 1


# ── Main Producer Loop ────────────────────────────────────────────────────────
def run_producer(events_per_sec: int, duration_mins: int):
    """
    Produce ride events to Kafka at the specified rate.
    
    Args:
        events_per_sec: Target throughput (default 500 events/sec)
        duration_mins:  Run duration in minutes; 0 = run indefinitely
    """
    producer    = build_producer()
    batch_size  = min(events_per_sec, 1000)
    sleep_sec   = 1.0  # produce one burst per second
    end_time    = time.time() + (duration_mins * 60) if duration_mins > 0 else float("inf")
    batch_count = 0

    log.info("🚗 RideFlow Kafka Producer started | topic=%s | target=%d evt/s",
             KAFKA_TOPIC_RIDES, events_per_sec)

    while _RUNNING and time.time() < end_time:
        batch_start = time.time()

        try:
            batch = generate_batch(batch_size)
            for event in batch:
                producer.produce(
                    topic     = KAFKA_TOPIC_RIDES,
                    key       = event["kafka_partition_key"].encode("utf-8"),
                    value     = json.dumps(event).encode("utf-8"),
                    callback  = _delivery_callback,
                )
            producer.poll(0)  # Trigger callbacks without blocking

        except KafkaException as ke:
            log.error("KafkaException during produce: %s", ke)
        except Exception as exc:
            log.exception("Unexpected error producing event: %s", exc)

        batch_count += 1

        # Throughput reporting every 10 batches
        if batch_count % 10 == 0:
            elapsed  = time.time() - _stats["start_time"]
            rate     = _stats["sent"] / max(elapsed, 1)
            log.info("📊 Stats | sent=%d | failed=%d | rate=%.0f evt/s",
                     _stats["sent"], _stats["failed"], rate)

        # Pace the producer
        elapsed_batch = time.time() - batch_start
        sleep_remaining = sleep_sec - elapsed_batch
        if sleep_remaining > 0:
            time.sleep(sleep_remaining)

    # Final flush
    log.info("Flushing remaining messages...")
    producer.flush(timeout=30)

    # Final summary
    total_time = time.time() - _stats["start_time"]
    log.info(
        "✅ Producer completed | total_sent=%d | total_failed=%d | "
        "avg_rate=%.0f evt/s | duration=%.1f s",
        _stats["sent"], _stats["failed"],
        _stats["sent"] / max(total_time, 1), total_time,
    )


# ── CLI ───────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="RideFlow Kafka Producer")
    parser.add_argument("--events-per-sec", type=int, default=SIM_EVENTS_PER_SEC)
    parser.add_argument("--duration-mins",  type=int, default=0,
                        help="0 = run indefinitely")
    args = parser.parse_args()

    run_producer(events_per_sec=args.events_per_sec,
                 duration_mins=args.duration_mins)
