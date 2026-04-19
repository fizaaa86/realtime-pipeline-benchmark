"""
EdTech Pipeline — Intelligence Layer
=====================================
SurgeDetector  : rolling-window event-rate monitor; adaptive trigger interval.
AdaptiveBatchOptimizer : buffers Parquet writes and flushes to S3 in bulk.

Imported by stream_consumer.py — do not run directly.
"""

import io
import json
import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import boto3
import redis

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

log = logging.getLogger("intelligence")

# ── Config from environment (mirrors stream_consumer defaults) ─────────────────
S3_BUCKET  = os.getenv("S3_BUCKET", "edtech-pipeline-data")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# In production: set by /etc/edtech-spark.env (updated by scripts/apply_eip.sh).
# EC2-1 has a Terraform-managed Elastic IP — run `terraform output ec2_1_eip` for the value.
KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "localhost:9093")
PRIORITY_TOPIC   = os.getenv("PRIORITY_TOPIC",   "edtech-priority")

# ──────────────────────────────────────────────────────────────────────────────
# SurgeDetector
# ──────────────────────────────────────────────────────────────────────────────

class SurgeDetector:
    """
    Maintains a rolling 5-minute deque of (timestamp, event_count) tuples.

    After every micro-batch call to .update():
      - computes rolling_avg of counts in the window
      - compares current_rate = batch_count / trigger_interval_seconds
      - if current_rate > rolling_avg * 1.5 → declare SURGE
        * logs it, updates Redis, sends class_join+session_start → priority topic
      - if current_rate <= rolling_avg * 1.1 while in surge → declare NORMAL

    Returns the current surge boolean so the caller can update Prometheus.
    """

    WINDOW_SECS  = 300   # 5-minute rolling window
    SURGE_MULT   = 1.5
    NORMAL_MULT  = 1.1
    FAST_TRIGGER = 5     # seconds — used during surge
    SLOW_TRIGGER = 30    # seconds — used during normal

    MIN_SURGE_HOLD = 6   # keep SURGE active for at least this many batches after detection

    def __init__(self):
        self._window: deque = deque()   # (timestamp_float, count)
        self.in_surge        = False
        self._surge_hold     = 0        # batches remaining in forced-surge hold
        self._redis: redis.Redis | None = None
        self._kproducer      = None     # lazy Kafka producer

    # ── internal helpers ───────────────────────────────────────────────────────

    def _get_redis(self) -> redis.Redis:
        if self._redis is None:
            self._redis = redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
            )
        return self._redis

    def _get_kproducer(self):
        if self._kproducer is None:
            try:
                from kafka import KafkaProducer
                self._kproducer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks=1,
                    retries=2,
                )
            except Exception as e:
                log.warning("SurgeDetector: Kafka producer unavailable: %s", e)
        return self._kproducer

    def _rolling_avg(self) -> float:
        if not self._window:
            return 0.0
        return sum(c for _, c in self._window) / len(self._window)

    def _prune(self):
        now = time.time()
        while self._window and now - self._window[0][0] > self.WINDOW_SECS:
            self._window.popleft()

    def _send_priority(self, events: list[dict]):
        """Forward class_join + session_start events to the priority topic."""
        producer = self._get_kproducer()
        if producer is None:
            return
        try:
            priority_rows = [
                e for e in events
                if e.get("event_type") in ("class_join", "session_start")
            ]
            for ev in priority_rows:
                producer.send(
                    PRIORITY_TOPIC,
                    value=ev,
                    key=ev.get("user_id"),
                )
            producer.flush(timeout=5)
            log.info("SurgeDetector: forwarded %d events → %s",
                     len(priority_rows), PRIORITY_TOPIC)
        except Exception as e:
            log.warning("SurgeDetector: priority send failed: %s", e)

    # ── public API ─────────────────────────────────────────────────────────────

    def update(self, batch_count: int, trigger_interval_secs: int,
               events: list[dict] | None = None,
               priority_topic: str | None = None) -> bool:
        """
        Call once per micro-batch.
        Returns True if surge is currently active, False otherwise.

        Both rolling_avg and current_rate are stored/compared in events/sec
        so the threshold multipliers are scale-independent.
        """
        now          = time.time()
        current_rate = batch_count / max(trigger_interval_secs, 1)
        self._window.append((now, current_rate))   # store rate, not raw count
        self._prune()

        rolling_avg = self._rolling_avg()   # mean of per-sec rates in window

        if not self.in_surge and rolling_avg > 0 and current_rate > rolling_avg * self.SURGE_MULT:
            self.in_surge    = True
            self._surge_hold = self.MIN_SURGE_HOLD
            log.info("SURGE detected: current_rate=%.1f rolling_avg=%.1f (hold=%d batches)",
                     current_rate, rolling_avg, self._surge_hold)
            try:
                self._get_redis().set("pipeline:surge_detected", "1", ex=120)
            except Exception:
                pass
            if events is not None:
                self._send_priority(events)

        elif self.in_surge:
            if self._surge_hold > 0:
                self._surge_hold -= 1
                log.info("SURGE hold: %d batches remaining (current_rate=%.1f)",
                         self._surge_hold, current_rate)
            elif current_rate <= rolling_avg * self.NORMAL_MULT:
                self.in_surge = False
                log.info("surge=False — NORMAL restored (current_rate=%.1f rolling_avg=%.1f)",
                         current_rate, rolling_avg)
                try:
                    self._get_redis().set("pipeline:surge_detected", "0", ex=120)
                except Exception:
                    pass

        return self.in_surge

    @property
    def recommended_trigger_secs(self) -> int:
        """Return the recommended trigger interval based on current surge state."""
        return self.FAST_TRIGGER if self.in_surge else self.SLOW_TRIGGER


# ──────────────────────────────────────────────────────────────────────────────
# AdaptiveBatchOptimizer
# ──────────────────────────────────────────────────────────────────────────────

class AdaptiveBatchOptimizer:
    """
    Buffers Parquet writes in memory.
    Flushes to S3 when buffer >= 10 000 events OR 5 minutes have elapsed.

    Tracks cost savings vs. a naive "flush every micro-batch" baseline.
    Writes cost stats to Redis: pipeline:s3_cost_stats
    """

    FLUSH_EVENT_THRESHOLD = 10_000
    FLUSH_TIME_SECS       = 300          # 5 minutes

    def __init__(self, s3_bucket: str = S3_BUCKET, aws_region: str = AWS_REGION):
        self.s3_bucket    = s3_bucket
        self.aws_region   = aws_region
        self._buffer: list[dict] = []
        self._last_flush  = time.time()
        self._baseline_calls  = 0   # one call per micro-batch (naive)
        self._optimized_calls = 0   # actual S3 flushes
        self._s3_client   = None
        self._redis: redis.Redis | None = None

    def _get_s3(self):
        if self._s3_client is None:
            self._s3_client = boto3.client("s3", region_name=self.aws_region)
        return self._s3_client

    def _get_redis(self) -> redis.Redis:
        if self._redis is None:
            self._redis = redis.Redis(
                host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
            )
        return self._redis

    def _s3_key(self) -> str:
        now = datetime.now(timezone.utc)
        return (
            f"raw/year={now.year}/month={now.month:02d}"
            f"/day={now.day:02d}/hour={now.hour:02d}"
            f"/{int(now.timestamp())}.parquet"
        )

    def _flush(self):
        if not self._buffer:
            return
        try:
            import pandas as pd
            df = pd.DataFrame(self._buffer)
            buf = io.BytesIO()
            df.to_parquet(buf, index=False, engine="pyarrow")
            buf.seek(0)
            key = self._s3_key()
            self._get_s3().put_object(
                Bucket=self.s3_bucket,
                Key=key,
                Body=buf.read(),
            )
            log.info("S3 flush: %d events → s3://%s/%s",
                     len(self._buffer), self.s3_bucket, key)
            self._optimized_calls += 1
        except Exception as e:
            log.warning("S3 flush failed: %s", e)
        finally:
            self._buffer.clear()
            self._last_flush = time.time()
            self._write_cost_stats()

    def _write_cost_stats(self):
        if self._baseline_calls == 0:
            return
        reduction = round(
            (1 - self._optimized_calls / self._baseline_calls) * 100, 1
        )
        stats = {
            "baseline_s3_calls":  self._baseline_calls,
            "optimized_s3_calls": self._optimized_calls,
            "cost_reduction_pct": reduction,
        }
        try:
            self._get_redis().set(
                "pipeline:s3_cost_stats", json.dumps(stats), ex=3600
            )
        except Exception:
            pass

    def add(self, batch_df: "DataFrame"):
        """
        Add events from the current micro-batch to the buffer.
        Flushes to S3 if threshold is reached.
        """
        self._baseline_calls += 1   # naive baseline = one call per batch

        try:
            rows = [row.asDict() for row in batch_df.collect()]
            self._buffer.extend(rows)
        except Exception as e:
            log.warning("AdaptiveBatchOptimizer: collect failed: %s", e)
            return

        elapsed = time.time() - self._last_flush
        should_flush = (
            len(self._buffer) >= self.FLUSH_EVENT_THRESHOLD
            or elapsed >= self.FLUSH_TIME_SECS
        )
        if should_flush:
            self._flush()

    def add_events(self, events: list[dict]):
        """
        Add pre-collected events to the buffer (zero Spark actions).
        Use this instead of add(batch_df) when events are already a Python list.
        Flushes to S3 when buffer >= 10 000 events OR 5 minutes have elapsed.
        """
        self._baseline_calls += 1   # naive baseline = one call per batch
        self._buffer.extend(events)

        elapsed = time.time() - self._last_flush
        should_flush = (
            len(self._buffer) >= self.FLUSH_EVENT_THRESHOLD
            or elapsed >= self.FLUSH_TIME_SECS
        )
        if should_flush:
            self._flush()

    @property
    def buffer_size(self) -> int:
        return len(self._buffer)
