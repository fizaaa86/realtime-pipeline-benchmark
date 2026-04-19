"""
EdTech Pipeline — Spark Structured Streaming Consumer
======================================================
Reads from Kafka topic 'edtech-events' on EC2-1.
Computes rolling-window aggregations and writes to:
  • Redis    — live dashboard metrics
  • S3       — raw Parquet lake (via AdaptiveBatchOptimizer)
  • DynamoDB — per-user stats

Run on EC2-2:
  spark-submit \\
    --jars $(ls /opt/spark/jars/*.jar | tr '\\n' ',') \\
    /home/ubuntu/edtech-pipeline/spark/stream_consumer.py
"""

import io
import json
import logging
import os
import socket
import sys
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

# ── Prometheus ────────────────────────────────────────────────────────────────
from prometheus_client import (
    Counter, Gauge, Histogram, start_http_server,
)

PROM_PORT = 8000

batch_duration_h   = Histogram(
    "pipeline_batch_duration_seconds",
    "Seconds spent processing each micro-batch",
    buckets=[0.5, 1, 2, 5, 10, 20, 30, 60],
)
events_processed_c = Counter(
    "pipeline_events_processed_total",
    "Cumulative events processed from Kafka",
)
kafka_lag_g        = Gauge(
    "pipeline_kafka_consumer_lag",
    "Estimated number of events behind latest Kafka offset",
)
active_users_g     = Gauge(
    "pipeline_active_users_gauge",
    "Distinct logins seen in the last micro-batch",
)
dropout_rate_g     = Gauge(
    "pipeline_dropout_rate_gauge",
    "Dropout rate % (voluntary class_leave / class_join) in last batch",
)
avg_engagement_g   = Gauge(
    "pipeline_avg_engagement_gauge",
    "Average engagement score from session_end events in last batch",
)
surge_detected_g   = Gauge(
    "pipeline_surge_detected",
    "1 if a traffic surge is currently active, 0 otherwise",
)
# ── Labeled gauge vectors (feed bar/pie chart panels) ─────────────────────────
course_joins_g     = Gauge(
    "pipeline_course_joins",
    "class_join count per course in last batch window",
    ["course"],
)
device_events_g    = Gauge(
    "pipeline_device_events",
    "Event count by device type in last batch window",
    ["device"],
)
region_users_g     = Gauge(
    "pipeline_region_users",
    "Online users by region in last batch window",
    ["region"],
)
event_type_g       = Gauge(
    "pipeline_event_type_count",
    "Event count by event type in last batch window",
    ["event_type"],
)
difficulty_score_g = Gauge(
    "pipeline_avg_score_by_difficulty",
    "Average quiz score by difficulty level",
    ["difficulty"],
)
subscription_g     = Gauge(
    "pipeline_subscription_events",
    "Event count by subscription type in last batch window",
    ["subscription"],
)
s3_cost_reduction_g = Gauge(
    "pipeline_s3_cost_reduction_pct",
    "S3 PUT cost reduction % from adaptive batching",
)
events_deduped_c = Counter(
    "pipeline_events_deduped_total",
    "Duplicate events dropped by event_id Redis dedup window",
)

# ── Streaming query handle (set in main(), read in process_batch) ─────────────
_streaming_query = None

log = logging.getLogger("stream_consumer")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
# Suppress noisy kafka-python connection lifecycle logs
logging.getLogger("kafka").setLevel(logging.WARNING)
logging.getLogger("kafka.conn").setLevel(logging.WARNING)

# ── Config ─────────────────────────────────────────────────────────────────────
# In production: set by /etc/edtech-spark.env (updated by scripts/apply_eip.sh).
# EC2-1 has a Terraform-managed Elastic IP — run `terraform output ec2_1_eip` for the value.
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9093")
KAFKA_TOPIC     = os.getenv("KAFKA_TOPIC",     "edtech-events")
PRIORITY_TOPIC  = os.getenv("PRIORITY_TOPIC",  "edtech-priority")
REDIS_HOST      = os.getenv("REDIS_HOST",      "localhost")
REDIS_PORT      = int(os.getenv("REDIS_PORT",  "6379"))
S3_BUCKET       = os.getenv("S3_BUCKET",       "edtech-pipeline-data")
AWS_REGION      = os.getenv("AWS_REGION",      "ap-south-1")
TRIGGER_SECS    = int(os.getenv("TRIGGER_SECS", "10"))
# Unique per-pod so k3s pods on the same EC2 don't share a checkpoint directory
# (shared path causes Spark ConcurrentModificationException in multi-pod setups).
_hostname       = os.getenv("HOSTNAME", socket.gethostname())
CHECKPOINT_PATH = os.getenv(
    "CHECKPOINT_PATH",
    f"/tmp/edtech-checkpoints/stream-{_hostname}"
)

# Master consumer = the one instance responsible for writing live KPIs to Redis.
# EC2-2's Spark consumer (IS_MASTER_CONSUMER=true) writes Redis + Prometheus only.
IS_MASTER_CONSUMER = os.getenv("IS_MASTER_CONSUMER", "true").lower() == "true"

# Storage flag — when false, skip S3 and DynamoDB writes entirely.
# EC2-2's master Spark consumer sets STORAGE_ENABLED=false so it only handles
# Redis live metrics and Prometheus monitoring. The k8s storage_worker pods
# (consumer group edtech-storage-group) own all S3 and DynamoDB writes and
# scale horizontally from 1 to 10 replicas based on Kafka consumer lag (KEDA).
STORAGE_ENABLED = os.getenv("STORAGE_ENABLED", "true").lower() == "true"

# ── Intelligence ──────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from intelligence import SurgeDetector, AdaptiveBatchOptimizer  # noqa: E402

# ── Spark ─────────────────────────────────────────────────────────────────────
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, StringType,
    StructField, StructType,
)

def create_spark() -> SparkSession:
    # All JARs in /opt/spark/jars/ are already on the classpath via spark-submit.
    # Do NOT set spark.jars — that triggers a 331MB re-copy to /tmp at startup.
    # Memory bumped to handle 10× load (3,000 events/batch instead of 300).
    return (
        SparkSession.builder
        .appName("EdTechStreamConsumer")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.executor.memory", "1g")
        .config("spark.driver.memory",   "512m")
        .config("spark.executor.cores",  "2")
        .config("spark.driver.cores",    "2")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        .config("spark.hadoop.fs.s3a.endpoint",
                f"s3.{AWS_REGION}.amazonaws.com")
        .getOrCreate()
    )


# ── Full event schema (matches generator.py v4 output) ────────────────────────
EVENT_SCHEMA = StructType([
    # ── Core ──────────────────────────────────────────────────────────────────
    StructField("event_id",             StringType(),  True),
    StructField("user_id",              StringType(),  True),
    StructField("event_type",           StringType(),  True),
    StructField("timestamp",            StringType(),  True),
    StructField("schema_version",       StringType(),  True),
    # ── User profile ──────────────────────────────────────────────────────────
    StructField("user_role",            StringType(),  True),
    StructField("subscription_type",    StringType(),  True),
    StructField("user_age_group",       StringType(),  True),
    StructField("user_level",           StringType(),  True),
    StructField("region",               StringType(),  True),
    StructField("country",              StringType(),  True),
    StructField("timezone",             StringType(),  True),
    StructField("device",               StringType(),  True),
    StructField("os",                   StringType(),  True),
    StructField("app_version",          StringType(),  True),
    StructField("session_id",           StringType(),  True),
    # ── Derived temporal fields (new in Task 1) ────────────────────────────────
    StructField("hour_of_day",          IntegerType(), True),
    StructField("day_of_week",          StringType(),  True),
    StructField("is_weekend",           BooleanType(), True),
    StructField("is_premium",           BooleanType(), True),
    # ── Event-specific ────────────────────────────────────────────────────────
    StructField("active_duration_sec",  IntegerType(), True),   # logout
    StructField("class_id",             StringType(),  True),
    StructField("course_name",          StringType(),  True),
    StructField("course_category",      StringType(),  True),
    StructField("difficulty_level",     StringType(),  True),
    StructField("instructor_id",        StringType(),  True),
    StructField("session_type",         StringType(),  True),
    StructField("leave_reason",         StringType(),  True),   # class_leave
    StructField("class_duration_sec",   IntegerType(), True),   # class_leave
    StructField("interaction_type",     StringType(),  True),   # session_start
    StructField("content_id",           StringType(),  True),   # session_start
    StructField("content_duration",     IntegerType(), True),   # session_start
    StructField("session_status",       StringType(),  True),
    StructField("session_duration_sec", IntegerType(), True),   # session_end
    StructField("score",                DoubleType(),  True),   # session_end
    StructField("passed",               BooleanType(), True),   # session_end
    StructField("engagement_score",     DoubleType(),  True),   # session_end
])

# ── Redis helper ───────────────────────────────────────────────────────────────
_redis_client: Optional[object] = None


def get_redis():
    global _redis_client
    if _redis_client is None:
        import redis as _redis_lib
        _redis_client = _redis_lib.Redis(
            host=REDIS_HOST, port=REDIS_PORT, decode_responses=True
        )
    return _redis_client


def redis_write(mapping: dict, ttl: int = 120):
    """Bulk-write key→value pairs to Redis, all with the same TTL."""
    r = get_redis()
    pipe = r.pipeline()
    for key, val in mapping.items():
        if isinstance(val, (dict, list)):
            pipe.set(key, json.dumps(val), ex=ttl)
        else:
            pipe.set(key, str(val) if val is not None else "0", ex=ttl)
    pipe.execute()


# ── DynamoDB helper ────────────────────────────────────────────────────────────
_dynamo_table = None


def get_dynamo_table():
    global _dynamo_table
    if _dynamo_table is None:
        import boto3
        _dynamo_table = boto3.resource(
            "dynamodb", region_name=AWS_REGION
        ).Table("UserStats")
    return _dynamo_table


def write_user_stats_to_dynamo(events: list[dict]):
    """Aggregate per-user stats and atomically upsert into DynamoDB UserStats.

    Uses ADD for counters (total_events, total_sessions) so multiple concurrent
    consumers never overwrite each other's data — each consumer ADDs its delta
    and DynamoDB accumulates the total atomically.
    """
    if not events:
        return
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    agg: dict[str, dict] = {}

    for ev in events:
        uid = ev.get("user_id")
        if not uid:
            continue
        d = agg.setdefault(uid, {
            "count": 0, "last_type": None, "last_seen": None,
            "sessions": 0, "scores": [], "engagements": [],
        })
        d["count"]     += 1
        d["last_type"]  = ev.get("event_type")
        d["last_seen"]  = ev.get("timestamp")
        if ev.get("event_type") == "session_end":
            d["sessions"] += 1
            if ev.get("score") is not None:
                d["scores"].append(float(ev["score"]))
            if ev.get("engagement_score") is not None:
                d["engagements"].append(float(ev["engagement_score"]))

    table = get_dynamo_table()
    for uid, d in agg.items():
        # ADD atomically increments counters; SET overwrites last-seen fields (safe — latest wins)
        update_expr = (
            "ADD total_events :n, total_sessions :s"
            " SET last_event_type = :lt, last_seen = :ls"
        )
        expr_vals: dict = {
            ":n":  d["count"],
            ":s":  d["sessions"],
            ":lt": d["last_type"] or "unknown",
            ":ls": d["last_seen"] or "",
        }
        if d["scores"]:
            update_expr += ", avg_score = :sc"
            expr_vals[":sc"] = Decimal(str(round(
                sum(d["scores"]) / len(d["scores"]), 2
            )))
        if d["engagements"]:
            update_expr += ", avg_engagement = :eng"
            expr_vals[":eng"] = Decimal(str(round(
                sum(d["engagements"]) / len(d["engagements"]), 3
            )))
        try:
            table.update_item(
                Key={"user_id": uid, "date": today},
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_vals,
            )
        except Exception as e:
            log.warning("UserStats update_item failed uid=%s: %s", uid, e)


_course_stats_table = None


def get_course_stats_table():
    global _course_stats_table
    if _course_stats_table is None:
        import boto3
        _course_stats_table = boto3.resource(
            "dynamodb", region_name=AWS_REGION
        ).Table("CourseStats")
    return _course_stats_table


def write_course_stats_to_dynamo(events: list[dict]):
    """Aggregate per-course stats and atomically upsert into DynamoDB CourseStats.

    Uses ADD for counters (session_count, join_count) so concurrent consumers
    accumulate totals correctly instead of overwriting each other.
    """
    if not events:
        return
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Accumulate per course_id (= class_id in the event schema)
    agg: dict[str, dict] = {}
    for ev in events:
        cid = ev.get("class_id")
        if not cid:
            continue
        d = agg.setdefault(str(cid), {
            "sessions": 0, "scores": [], "engagements": [],
            "passed": 0, "joins": 0, "voluntary_leaves": 0,
        })
        etype = ev.get("event_type")
        if etype == "session_end":
            d["sessions"] += 1
            if ev.get("score") is not None:
                d["scores"].append(float(ev["score"]))
            if ev.get("engagement_score") is not None:
                d["engagements"].append(float(ev["engagement_score"]))
            if ev.get("passed"):
                d["passed"] += 1
        elif etype == "class_join":
            d["joins"] += 1
        elif etype == "class_leave" and ev.get("leave_reason") == "voluntary":
            d["voluntary_leaves"] += 1

    table = get_course_stats_table()
    for cid, d in agg.items():
        # ADD counters atomically; SET computed metrics (last-batch average is acceptable)
        update_expr = "ADD session_count :s, join_count :j"
        expr_vals: dict = {
            ":s": d["sessions"],
            ":j": d["joins"],
        }
        set_parts: list[str] = []
        if d["scores"]:
            set_parts.append("avg_score = :sc")
            set_parts.append("pass_rate = :pr")
            expr_vals[":sc"] = Decimal(str(round(
                sum(d["scores"]) / len(d["scores"]), 2
            )))
            expr_vals[":pr"] = Decimal(str(round(
                d["passed"] / len(d["scores"]) * 100, 1
            )))
        if d["engagements"]:
            set_parts.append("avg_engagement = :eng")
            expr_vals[":eng"] = Decimal(str(round(
                sum(d["engagements"]) / len(d["engagements"]), 3
            )))
        if d["joins"] > 0:
            set_parts.append("dropout_pct = :dp")
            expr_vals[":dp"] = Decimal(str(round(
                d["voluntary_leaves"] / d["joins"] * 100, 1
            )))
        if set_parts:
            update_expr += " SET " + ", ".join(set_parts)
        try:
            table.update_item(
                Key={"course_id": cid, "date": today},
                UpdateExpression=update_expr,
                ExpressionAttributeValues=expr_vals,
            )
        except Exception as e:
            log.warning("CourseStats update_item failed cid=%s: %s", cid, e)


# ── Singleton intelligence objects (created once, reused across batches) ───────
_surge_detector  = SurgeDetector()
_s3_optimizer    = AdaptiveBatchOptimizer(s3_bucket=S3_BUCKET, aws_region=AWS_REGION)


# ── Batch processor ────────────────────────────────────────────────────────────
def process_batch(batch_df: DataFrame, batch_id: int):
    """
    Called by Spark for every micro-batch (every TRIGGER_SECS seconds).

    10× load optimisation: ONE Spark action (collect) replaces the original
    21 separate actions.  All metrics are derived in Python from the collected
    list — negligible overhead at ≤30,000 events/batch.

    1. Single collect → events list
    2. Surge detection + S3 buffering (operate on list, not DataFrame)
    3. DynamoDB upserts
    4. All KPIs computed via Python dict/set comprehensions
    5. Redis bulk-write
    6. Prometheus gauges
    """
    t0 = time.time()

    # ── ONE Spark action: pull entire batch to driver ──────────────────────────
    # At baseline (300 events) this is ~300 KB.
    # At 10× load (3,000 events) this is ~3 MB — trivial on a t3.medium driver.
    # Every metric below is derived from this single Python list.
    events: list[dict] = [row.asDict() for row in batch_df.collect()]
    count = len(events)
    if count == 0:
        return

    log.info("Batch %d: %d events", batch_id, count)
    events_processed_c.inc(count)

    # ── Deduplication via Redis Set (1-hour sliding window) ────────────────────
    # SADD returns 1 for new event_ids and 0 for already-seen ones.
    # Covers generator retries and Kafka rebalance re-deliveries.
    try:
        r = get_redis()
        dedup_key = "pipeline:dedup_event_ids"
        pipe = r.pipeline()
        for ev in events:
            if ev.get("event_id"):
                pipe.sadd(dedup_key, ev["event_id"])
        add_results = pipe.execute()
        r.expire(dedup_key, 3600)
        result_iter = iter(add_results)
        unique_events = []
        for ev in events:
            if ev.get("event_id"):
                if next(result_iter):      # 1 = new, 0 = duplicate
                    unique_events.append(ev)
            else:
                unique_events.append(ev)   # no event_id, pass through
        dupes = count - len(unique_events)
        if dupes > 0:
            log.info("Batch %d: dropped %d duplicate event_ids", batch_id, dupes)
            events_deduped_c.inc(dupes)
        events = unique_events
        count = len(events)
        if count == 0:
            return
    except Exception as dedup_exc:
        log.warning("Dedup check failed, processing all events: %s", dedup_exc)

    # ── Surge detection (list-based, no additional Spark action) ──────────────
    is_surge = _surge_detector.update(count, TRIGGER_SECS, events)
    surge_detected_g.set(1 if is_surge else 0)

    # ── S3 + DynamoDB (storage worker pods own this when STORAGE_ENABLED=false) ─
    if STORAGE_ENABLED:
        _s3_optimizer.add_events(events)
        try:
            write_user_stats_to_dynamo(events)
        except Exception as exc:
            log.warning("DynamoDB UserStats write skipped: %s", exc)
        try:
            write_course_stats_to_dynamo(events)
        except Exception as exc:
            log.warning("DynamoDB CourseStats write skipped: %s", exc)
    else:
        log.debug("Batch %d: STORAGE_ENABLED=false — S3/DynamoDB handled by k8s storage-worker pods", batch_id)

    # ── All KPIs — pure Python, zero additional Spark actions ─────────────────

    # Window 1 — active users / event breakdown / regional online users
    active_users_now = len({e["user_id"] for e in events if e.get("user_id")})

    events_by_type: dict = {}
    for e in events:
        et = e.get("event_type")
        if et:
            events_by_type[et] = events_by_type.get(et, 0) + 1

    login_region_users: dict = {}   # region → set of user_ids from login events
    for e in events:
        if e.get("event_type") == "login" and e.get("region") and e.get("user_id"):
            login_region_users.setdefault(e["region"], set()).add(e["user_id"])
    online_by_region: dict = {r: len(u) for r, u in login_region_users.items()}

    # Window 2 — course analytics
    course_join_counts: dict = {}
    for e in events:
        if e.get("event_type") == "class_join" and e.get("class_id"):
            cid = e["class_id"]
            course_join_counts[cid] = course_join_counts.get(cid, 0) + 1
    top_5_courses: dict = dict(
        sorted(course_join_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    )

    eng_scores = [
        float(e["engagement_score"]) for e in events
        if e.get("event_type") == "session_end"
        and e.get("engagement_score") is not None
    ]
    avg_engagement: float = (
        round(sum(eng_scores) / len(eng_scores), 3) if eng_scores else 0.0
    )

    scores_by_diff: dict = {}
    for e in events:
        if (e.get("event_type") == "session_end"
                and e.get("score") is not None
                and e.get("difficulty_level")):
            scores_by_diff.setdefault(e["difficulty_level"], []).append(float(e["score"]))
    avg_score_by_difficulty: dict = {
        k: round(sum(v) / len(v), 2) for k, v in scores_by_diff.items()
    }

    class_joins = sum(1 for e in events if e.get("event_type") == "class_join")
    vol_leaves  = sum(
        1 for e in events
        if e.get("event_type") == "class_leave"
        and e.get("leave_reason") == "voluntary"
    )
    dropout_rate: float = (
        round(vol_leaves / class_joins * 100, 1) if class_joins > 0 else 0.0
    )

    premium_vs_free: dict = {}
    for e in events:
        sub = e.get("subscription_type") or "unknown"
        premium_vs_free[sub] = premium_vs_free.get(sub, 0) + 1

    device_breakdown: dict = {}
    for e in events:
        dev = e.get("device") or "unknown"
        device_breakdown[dev] = device_breakdown.get(dev, 0) + 1

    session_starts = sum(1 for e in events if e.get("event_type") == "session_start")
    completed_ends = sum(
        1 for e in events
        if e.get("event_type") == "session_end"
        and e.get("session_status") == "completed"
    )
    session_completion_rate: float = (
        round(completed_ends / session_starts * 100, 1)
        if session_starts > 0 else 0.0
    )

    # Window 3 — class-level aggregates
    active_classes_count: int = len({
        e["class_id"] for e in events
        if e.get("event_type") == "class_join" and e.get("class_id")
    })

    durations = [
        float(e["class_duration_sec"]) for e in events
        if e.get("event_type") == "class_leave"
        and e.get("class_duration_sec") is not None
    ]
    avg_class_duration_sec: float = (
        round(sum(durations) / len(durations), 1) if durations else 0.0
    )

    mobile_cnt  = sum(1 for e in events if e.get("device") == "mobile")
    desktop_cnt = sum(1 for e in events if e.get("device") == "desktop")
    mobile_vs_desktop_ratio: float = (
        round(mobile_cnt / desktop_cnt, 2) if desktop_cnt > 0 else 0.0
    )

    # ── Write all metrics to Redis (master consumer only) ─────────────────────
    # ASG instances skip this block — each consumer only sees its partition subset,
    # so partial writes from multiple consumers would corrupt the live KPI values.
    # Only EC2-2 (IS_MASTER_CONSUMER=true) writes the full aggregated view to Redis.
    now_iso = datetime.now(timezone.utc).isoformat()
    if IS_MASTER_CONSUMER:
        try:
            redis_write({
                # Window 1 keys (TTL 120s)
                "pipeline:active_users":           active_users_now,
                # Extrapolate batch count to per-minute rate (count is for TRIGGER_SECS window)
                "pipeline:events_per_minute":      count * (60 // max(TRIGGER_SECS, 1)),
                "pipeline:online_users_by_region": online_by_region,
                "pipeline:last_updated":           now_iso,
            }, ttl=120)

            redis_write({
                # Window 1 — separate because value is dict
                "pipeline:events_by_type": events_by_type,
            }, ttl=120)

            redis_write({
                # Window 2 keys (TTL 360s = 6 min)
                "pipeline:top_5_courses":            top_5_courses,
                "pipeline:avg_engagement_score":     avg_engagement,
                "pipeline:avg_score_by_difficulty":  avg_score_by_difficulty,
                "pipeline:dropout_rate":             dropout_rate,
                "pipeline:premium_vs_free":          premium_vs_free,
                "pipeline:device_breakdown":         device_breakdown,
                "pipeline:session_completion_rate":  session_completion_rate,
            }, ttl=360)

            redis_write({
                # Window 3 keys (TTL 900s = 15 min)
                "pipeline:active_classes_count":    active_classes_count,
                "pipeline:avg_class_duration_sec":  avg_class_duration_sec,
                "pipeline:mobile_vs_desktop_ratio": mobile_vs_desktop_ratio,
            }, ttl=900)

            log.info(
                "Redis updated — active_users=%d  dropout=%.1f%%  "
                "engagement=%.3f  s3_buffer=%d",
                active_users_now, dropout_rate, avg_engagement,
                _s3_optimizer.buffer_size,
            )
        except Exception as exc:
            log.error("Redis write failed: %s", exc)
    else:
        log.debug(
            "Batch %d: skipping Redis (non-master consumer, IS_MASTER_CONSUMER=false)",
            batch_id,
        )

    # ── Prometheus ─────────────────────────────────────────────────────────────
    active_users_g.set(active_users_now)
    dropout_rate_g.set(dropout_rate)
    avg_engagement_g.set(avg_engagement)

    # Labeled vectors — feed bar/pie chart panels
    for course, cnt in top_5_courses.items():
        course_joins_g.labels(course=str(course)).set(cnt)
    for device, cnt in device_breakdown.items():
        device_events_g.labels(device=str(device)).set(cnt)
    for region, cnt in online_by_region.items():
        region_users_g.labels(region=str(region)).set(cnt)
    for etype, cnt in events_by_type.items():
        event_type_g.labels(event_type=str(etype)).set(cnt)
    for diff, score in avg_score_by_difficulty.items():
        difficulty_score_g.labels(difficulty=str(diff)).set(score)
    for sub, cnt in premium_vs_free.items():
        subscription_g.labels(subscription=str(sub)).set(cnt)
    try:
        raw = get_redis().get("pipeline:s3_cost_stats")
        if raw:
            s3_stats = json.loads(raw)
            s3_cost_reduction_g.set(float(s3_stats.get("cost_reduction_pct", 0)))
    except Exception:
        pass

    # ── Kafka consumer lag ─────────────────────────────────────────────────────
    try:
        if _streaming_query is not None:
            progress = _streaming_query.lastProgress
            if progress:
                sources = progress.get("sources", [])
                spark_end = sources[0].get("endOffset", {}) if sources else {}
                # spark_end: {"edtech-events": {"0": "12345", ...}}
                from kafka import KafkaConsumer
                from kafka.structs import TopicPartition
                consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP)
                partitions = consumer.partitions_for_topic(KAFKA_TOPIC) or set()
                tps = [TopicPartition(KAFKA_TOPIC, p) for p in partitions]
                end_offsets = consumer.end_offsets(tps)
                consumer.close()
                lag = 0
                topic_spark = spark_end.get(KAFKA_TOPIC, {})
                for tp, latest_offset in end_offsets.items():
                    spark_offset = int(topic_spark.get(str(tp.partition), latest_offset))
                    lag += max(0, latest_offset - spark_offset)
                kafka_lag_g.set(lag)
    except Exception as _lag_exc:
        log.debug("Kafka lag estimate failed: %s", _lag_exc)

    elapsed = time.time() - t0
    batch_duration_h.observe(elapsed)
    log.info("Batch %d complete in %.2fs", batch_id, elapsed)


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    log.info("Starting Prometheus HTTP server on port %d", PROM_PORT)
    start_http_server(PROM_PORT)

    log.info("Initialising SparkSession (app=EdTechStreamConsumer)")
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    # ── Kafka source ───────────────────────────────────────────────────────────
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",  KAFKA_BOOTSTRAP)
        .option("kafka.group.id",           "edtech-monitoring-group")
        .option("subscribe",                KAFKA_TOPIC)
        .option("startingOffsets",          "latest")
        .option("failOnDataLoss",           "false")
        .option("maxOffsetsPerTrigger",     "30000")  # back-pressure guard (10× load)
        .load()
    )

    # ── Parse JSON payload ─────────────────────────────────────────────────────
    parsed = (
        raw
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), EVENT_SCHEMA).alias("e"))
        .select("e.*")
    )

    # ── Streaming query ────────────────────────────────────────────────────────
    global _streaming_query
    query = (
        parsed.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime=f"{TRIGGER_SECS} seconds")
        .start()
    )
    _streaming_query = query

    log.info(
        "Streaming query started.\n"
        "  Kafka:      %s  →  topic '%s'\n"
        "  Trigger:    every %ds\n"
        "  Checkpoint: %s\n"
        "  S3 bucket:  %s\n"
        "  Prometheus: http://localhost:%d/metrics",
        KAFKA_BOOTSTRAP, KAFKA_TOPIC, TRIGGER_SECS,
        CHECKPOINT_PATH, S3_BUCKET, PROM_PORT,
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
