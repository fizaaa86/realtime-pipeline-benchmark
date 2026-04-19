"""
EdTech Pipeline — Daily Batch Reports
======================================
Runs on EC2-2 every night at midnight via cron.
Reads yesterday's raw Parquet data from S3, computes daily analytics,
and writes results to DynamoDB DailyReports + Parquet reports/ prefix.

Cron entry (add with: crontab -e):
  0 0 * * * /opt/spark/bin/spark-submit \\
      /home/ubuntu/edtech-pipeline/spark/batch_reports.py \\
      >> /var/log/edtech_batch.log 2>&1

Override the target date with: --date YYYY-MM-DD
  spark-submit batch_reports.py -- --date 2026-04-11
"""

import argparse
import io
import json
import logging
import math
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from decimal import Decimal

log = logging.getLogger("batch_reports")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)

# ── Config ─────────────────────────────────────────────────────────────────────
S3_BUCKET  = os.getenv("S3_BUCKET",  "edtech-pipeline-data")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

# ── Spark ──────────────────────────────────────────────────────────────────────
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType, DoubleType, IntegerType, StringType,
    StructField, StructType,
)


def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("EdTechBatchReports")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.executor.memory", "512m")
        .config("spark.driver.memory",   "256m")
        .getOrCreate()
    )


# ── S3 / PyArrow helpers ───────────────────────────────────────────────────────

def _s3_prefix(date_str: str) -> str:
    """S3 key prefix for raw Parquet partition (no s3:// scheme)."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return (
        f"raw/year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
    )


def read_s3_parquet_as_spark(spark: SparkSession, date_str: str) -> "DataFrame":
    """
    Read raw Parquet from S3 using pyarrow.fs (IAM role auth) and return
    a Spark DataFrame.  Avoids the need for hadoop-aws JARs.
    """
    import pyarrow.dataset as ds
    from pyarrow.fs import S3FileSystem

    s3_fs  = S3FileSystem(region=AWS_REGION)
    prefix = _s3_prefix(date_str)
    s3_path = f"{S3_BUCKET}/{prefix}"
    log.info("Reading S3 via pyarrow: s3://%s", s3_path)

    dataset = ds.dataset(s3_path, filesystem=s3_fs, format="parquet")
    table   = dataset.to_table()
    log.info("PyArrow read complete: %d rows, %d columns",
             table.num_rows, table.num_columns)

    # Convert to pandas then Spark (streaming is stopped so RAM is available)
    pdf = table.to_pandas()
    return spark.createDataFrame(pdf)


def write_report_parquet_s3(report: dict, date_str: str):
    """Write flattened report dict as Parquet to S3 via boto3 (no Spark needed)."""
    import boto3
    import pyarrow as pa
    import pyarrow.parquet as pq

    flat = {k: json.dumps(v) if isinstance(v, (dict, list)) else v
            for k, v in report.items()}
    flat["report_date"] = date_str

    table = pa.Table.from_pydict({k: [v] for k, v in flat.items()})
    buf   = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    key = f"reports/daily/{date_str}/report.parquet"
    boto3.client("s3", region_name=AWS_REGION).put_object(
        Bucket=S3_BUCKET, Key=key, Body=buf.getvalue()
    )
    log.info("Parquet report uploaded to s3://%s/%s", S3_BUCKET, key)


# ── DynamoDB helper ────────────────────────────────────────────────────────────

def _to_dynamo(value):
    """Convert Python values to DynamoDB-safe types (no float, NaN, or Inf)."""
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None          # DynamoDB cannot store NaN/Inf
        return Decimal(str(round(value, 4)))
    if isinstance(value, dict):
        # Drop None-valued keys — DynamoDB rejects null attribute values
        result = {}
        for k, v in value.items():
            converted = _to_dynamo(v)
            if converted is not None:
                result[k] = converted
        return result or None    # return None if all values were null
    if isinstance(value, list):
        return [_to_dynamo(v) for v in value if _to_dynamo(v) is not None]
    return value


def write_daily_report(report_date: str, report: dict):
    """Write the full daily report as a single item in DynamoDB DailyReports."""
    import boto3
    table = boto3.resource("dynamodb", region_name=AWS_REGION).Table("DailyReports")
    item  = {"report_date": report_date}
    item.update(_to_dynamo(report))
    table.put_item(Item=item)
    log.info("DynamoDB DailyReports: wrote report for %s", report_date)




# ── Computation helpers ────────────────────────────────────────────────────────

def _safe_collect_map(df: DataFrame, key_col: str, val_col: str) -> dict:
    """Collect a two-column DataFrame into a plain dict (handles empty DFs)."""
    try:
        return (
            df.rdd
            .map(lambda r: (str(r[key_col]), r[val_col]))
            .collectAsMap()
        )
    except Exception:
        return {}


# ── Main report logic ──────────────────────────────────────────────────────────

def run_reports(spark: SparkSession, report_date: str):
    log.info("Computing daily report for %s", report_date)

    # ── Load yesterday's data ──────────────────────────────────────────────────
    try:
        df = read_s3_parquet_as_spark(spark, report_date)
    except Exception as exc:
        log.error("Failed to read Parquet for %s: %s", report_date, exc)
        sys.exit(1)

    total_events = df.count()
    log.info("Total events loaded: %d", total_events)

    if total_events == 0:
        log.warning("No events found for %s — skipping report.", report_date)
        return

    # Cache the full DataFrame; we'll filter it many times
    df.cache()

    # Partition into event-type views
    session_end_df  = df.filter(F.col("event_type") == "session_end")
    class_join_df   = df.filter(F.col("event_type") == "class_join")
    class_leave_df  = df.filter(F.col("event_type") == "class_leave")
    login_df        = df.filter(F.col("event_type") == "login")
    logout_df       = df.filter(F.col("event_type") == "logout")

    report: dict = {}

    # ── 1. Session-end analytics ───────────────────────────────────────────────
    log.info("Computing session-end analytics…")

    # daily_avg_score + daily_avg_engagement per course_id
    per_course = (
        session_end_df
        .filter(F.col("score").isNotNull())
        .groupBy("class_id")
        .agg(
            F.avg("score").alias("avg_score"),
            F.avg("engagement_score").alias("avg_engagement"),
            F.count("*").alias("session_count"),
        )
    )
    daily_avg_score: dict = _safe_collect_map(
        per_course.select("class_id",
                          F.round("avg_score", 2).alias("v")),
        "class_id", "v",
    )
    daily_avg_engagement: dict = _safe_collect_map(
        per_course.select("class_id",
                          F.round("avg_engagement", 3).alias("v")),
        "class_id", "v",
    )

    # pass_rate_by_course
    pass_by_course = (
        session_end_df
        .filter(F.col("score").isNotNull())
        .groupBy("class_id")
        .agg(
            F.avg(F.col("passed").cast("int")).alias("pass_rate"),
        )
    )
    pass_rate_by_course: dict = _safe_collect_map(
        pass_by_course.select("class_id",
                              F.round(F.col("pass_rate") * 100, 1).alias("v")),
        "class_id", "v",
    )

    # pass_rate_by_difficulty
    pass_by_diff = (
        session_end_df
        .filter(F.col("score").isNotNull())
        .groupBy("difficulty_level")
        .agg(F.avg(F.col("passed").cast("int")).alias("pass_rate"))
    )
    pass_rate_by_difficulty: dict = _safe_collect_map(
        pass_by_diff.select("difficulty_level",
                            F.round(F.col("pass_rate") * 100, 1).alias("v")),
        "difficulty_level", "v",
    )

    report.update({
        "daily_avg_score":          daily_avg_score,
        "daily_avg_engagement":     daily_avg_engagement,
        "pass_rate_by_course":      pass_rate_by_course,
        "pass_rate_by_difficulty":  pass_rate_by_difficulty,
    })
    log.info("Session analytics done: %d courses tracked", len(daily_avg_score))

    # ── 2. Class participation analytics ──────────────────────────────────────
    log.info("Computing class participation analytics…")

    # daily_class_participation: count of joins per class_id
    daily_class_participation: dict = _safe_collect_map(
        class_join_df.groupBy("class_id").count(),
        "class_id", "count",
    )

    # avg_class_duration_sec per course_id (from class_leave)
    avg_duration_df = (
        class_leave_df
        .filter(F.col("class_duration_sec").isNotNull())
        .groupBy("class_id")
        .agg(F.avg("class_duration_sec").alias("avg_dur"))
    )
    avg_class_duration_sec: dict = _safe_collect_map(
        avg_duration_df.select("class_id", F.round("avg_dur", 1).alias("v")),
        "class_id", "v",
    )

    # dropout_analysis: voluntary_leaves / joins * 100, per course_id
    joins_per_course = class_join_df.groupBy("class_id").count().withColumnRenamed("count", "joins")
    vol_leaves_per_course = (
        class_leave_df
        .filter(F.col("leave_reason") == "voluntary")
        .groupBy("class_id").count().withColumnRenamed("count", "leaves")
    )
    dropout_df = (
        joins_per_course
        .join(vol_leaves_per_course, on="class_id", how="left")
        .fillna(0, subset=["leaves"])
        .withColumn("dropout_pct",
                    F.round(F.col("leaves") / F.col("joins") * 100, 1))
    )
    dropout_analysis: dict = _safe_collect_map(
        dropout_df.select("class_id", "dropout_pct"),
        "class_id", "dropout_pct",
    )

    report.update({
        "daily_class_participation": daily_class_participation,
        "avg_class_duration_sec":    avg_class_duration_sec,
        "dropout_analysis":          dropout_analysis,
    })
    log.info("Class analytics done: %d classes tracked",
             len(daily_class_participation))

    # ── 3. Login / logout analytics ───────────────────────────────────────────
    log.info("Computing login/logout analytics…")

    # daily_active_users: distinct users who logged in
    daily_active_users: int = login_df.select("user_id").distinct().count()

    # avg_active_time_sec per user: from logout events (active_duration_sec)
    active_time_df = (
        logout_df
        .filter(F.col("active_duration_sec").isNotNull())
        .groupBy("user_id")
        .agg(F.avg("active_duration_sec").alias("avg_sec"))
    )
    avg_active_time_by_user: dict = _safe_collect_map(
        active_time_df.select("user_id", F.round("avg_sec", 1).alias("v")),
        "user_id", "v",
    )

    report.update({
        "daily_active_users":       daily_active_users,
        "avg_active_time_sec":      avg_active_time_by_user,
    })
    log.info("Login analytics done: %d active users", daily_active_users)

    # ── 4. Cross-event analytics ───────────────────────────────────────────────
    log.info("Computing cross-event analytics…")

    # hourly_event_distribution: count by hour_of_day
    hourly_dist_df = (
        df.filter(F.col("hour_of_day").isNotNull())
        .groupBy("hour_of_day").count()
        .orderBy("hour_of_day")
    )
    hourly_event_distribution: dict = _safe_collect_map(
        hourly_dist_df, "hour_of_day", "count"
    )

    # weekend_vs_weekday: count by is_weekend
    weekend_df = (
        df.filter(F.col("is_weekend").isNotNull())
        .groupBy("is_weekend").count()
    )
    wkend_raw: dict = _safe_collect_map(weekend_df, "is_weekend", "count")
    weekend_vs_weekday = {
        "weekend":  wkend_raw.get("true",  wkend_raw.get(True,  0)),
        "weekday":  wkend_raw.get("false", wkend_raw.get(False, 0)),
    }

    # top_regions: distinct users per region
    top_regions_df = (
        df.filter(F.col("region").isNotNull())
        .groupBy("region")
        .agg(F.countDistinct("user_id").alias("unique_users"))
        .orderBy(F.desc("unique_users"))
    )
    top_regions: dict = _safe_collect_map(
        top_regions_df, "region", "unique_users"
    )

    # device_usage_daily: count by device
    device_usage_df = (
        df.filter(F.col("device").isNotNull())
        .groupBy("device").count()
    )
    device_usage_daily: dict = _safe_collect_map(
        device_usage_df, "device", "count"
    )

    report.update({
        "hourly_event_distribution": hourly_event_distribution,
        "weekend_vs_weekday":        weekend_vs_weekday,
        "top_regions":               top_regions,
        "device_usage_daily":        device_usage_daily,
        "total_events":              total_events,
    })
    log.info(
        "Cross-event analytics done — regions=%d  devices=%d  hours=%d",
        len(top_regions), len(device_usage_daily), len(hourly_event_distribution),
    )

    # ── 5. Write Parquet report to S3 (via boto3, no s3a:// JARs needed) ────────
    log.info("Writing Parquet report to s3://%s/reports/daily/%s/",
             S3_BUCKET, report_date)
    try:
        write_report_parquet_s3(report, report_date)
    except Exception as exc:
        log.warning("Parquet report write failed (non-fatal): %s", exc)

    # ── 6. Write to DynamoDB DailyReports ─────────────────────────────────────
    log.info("Writing to DynamoDB DailyReports…")
    try:
        write_daily_report(report_date, report)
    except Exception as exc:
        log.warning("DynamoDB write failed (non-fatal): %s", exc)

    df.unpersist()
    log.info("Daily report for %s complete.", report_date)


# ── CLI ────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(
        description="EdTech Pipeline — daily batch report generator"
    )
    yesterday = (
        datetime.now(timezone.utc) - timedelta(days=1)
    ).strftime("%Y-%m-%d")
    p.add_argument(
        "--date", default=yesterday,
        help="Report date YYYY-MM-DD (default: yesterday)"
    )
    # spark-submit passes its own args before '--'; drop them
    argv = sys.argv[sys.argv.index("--") + 1:] if "--" in sys.argv else sys.argv[1:]
    return p.parse_args(argv)


def main():
    args  = parse_args()
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")
    log.info("Batch reports job started for date=%s", args.date)
    run_reports(spark, args.date)
    spark.stop()
    log.info("Job finished.")


if __name__ == "__main__":
    main()
