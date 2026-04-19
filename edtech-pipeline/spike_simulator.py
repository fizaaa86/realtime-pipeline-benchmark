"""
spike_simulator.py
──────────────────
Sends a sudden burst of events to Kinesis to trigger spike detection.
Run this while your normal generator is running.

Usage:
    python spike_simulator.py                  # default: 10x spike for 30s
    python spike_simulator.py --multiplier 5   # 5x spike
    python spike_simulator.py --duration 60    # spike lasts 60 seconds
"""

import boto3
import json
import uuid
import random
import time
import argparse
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
STREAM_NAME = "edtech-events-stream"
REGION      = "ap-south-1"

REGIONS      = ["Mumbai", "Delhi", "Bengaluru", "Hyderabad", "Kolkata", "Pune", "Chennai"]
DEVICES      = ["mobile", "desktop", "tablet"]
OS_MAP       = {"mobile": ["android", "ios"], "desktop": ["windows", "macos"], "tablet": ["ios", "android"]}
EVENT_TYPES  = ["login", "class_join", "class_join", "class_join"]   # weight class_join higher
CLASS_IDS    = ["csg527", "csg302", "csg415", "csg244", "csg175", "csg388", "csg499"]
COURSE_NAMES = {
    "csg527": "Cloud Computing",
    "csg302": "Data Structures",
    "csg415": "Machine Learning",
    "csg244": "Operating Systems",
    "csg175": "Computer Networks",
    "csg388": "Database Systems",
    "csg499": "Deep Learning",
}

kinesis = boto3.client("kinesis", region_name=REGION)


def make_event() -> dict:
    device   = random.choice(DEVICES)
    class_id = random.choice(CLASS_IDS)
    return {
        "event_id":         str(uuid.uuid4()),
        "user_id":          f"u{random.randint(1, 500)}",
        "event_type":       random.choice(EVENT_TYPES),
        "timestamp":        datetime.now(timezone.utc).isoformat(),
        "user_role":        random.choice(["student", "instructor"]),
        "subscription_type":random.choice(["free", "premium"]),
        "user_age_group":   random.choice(["18-24", "25-30", "31-40"]),
        "user_level":       random.choice(["beginner", "intermediate", "advanced"]),
        "region":           random.choice(REGIONS),
        "country":          "India",
        "timezone":         "Asia/Kolkata",
        "device":           device,
        "os":               random.choice(OS_MAP[device]),
        "app_version":      "v1.8.7",
        "schema_version":   "2.0",
        "session_id":       f"s{random.randint(10000, 99999)}",
        "class_id":         class_id,
        "course_name":      COURSE_NAMES[class_id],
        "course_category":  "CS",
        "difficulty_level": random.choice(["easy", "medium", "hard"]),
        "instructor_id":    f"i{random.randint(1, 10):02d}",
        "session_type":     random.choice(["live", "recorded"]),
    }


def send_batch(events: list):
    """Send up to 500 events in one Kinesis put_records call."""
    records = [
        {
            "Data":         json.dumps(e).encode("utf-8"),
            "PartitionKey": e["user_id"],
        }
        for e in events
    ]
    # Kinesis allows max 500 records per put_records
    for i in range(0, len(records), 500):
        chunk = records[i:i + 500]
        resp  = kinesis.put_records(StreamName=STREAM_NAME, Records=chunk)
        failed = resp.get("FailedRecordCount", 0)
        if failed:
            print(f"  ⚠️  {failed} records failed")


def run_spike(events_per_second: int, duration_seconds: int):
    print(f"\n🚨 SPIKE STARTING")
    print(f"   Rate     : {events_per_second} events/sec")
    print(f"   Duration : {duration_seconds} seconds")
    print(f"   Total    : ~{events_per_second * duration_seconds} events")
    print(f"   Stream   : {STREAM_NAME}\n")

    start     = time.time()
    total_sent = 0

    while time.time() - start < duration_seconds:
        batch      = [make_event() for _ in range(events_per_second)]
        send_batch(batch)
        total_sent += len(batch)
        elapsed    = time.time() - start
        print(f"  ✅ Sent {total_sent:>6} events  |  elapsed: {elapsed:.1f}s", end="\r")
        time.sleep(1)

    print(f"\n\n🏁 Spike complete. Sent {total_sent} events in {duration_seconds}s")
    print("   → Check CloudWatch dashboard — SpikeDetected should flip to 1")
    print("   → Check Lambda logs for '🚨 SPIKE DETECTED' line\n")


def run_normal(events_per_second: int, duration_seconds: int):
    """Send normal traffic before the spike to establish a baseline."""
    print(f"📊 Sending normal traffic first ({events_per_second} eps for {duration_seconds}s)...")
    start = time.time()
    sent  = 0
    while time.time() - start < duration_seconds:
        batch = [make_event() for _ in range(events_per_second)]
        send_batch(batch)
        sent += len(batch)
        time.sleep(1)
    print(f"   Normal baseline done. Sent {sent} events.\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="EdTech spike simulator")
    parser.add_argument("--multiplier", type=int,   default=10,
                        help="Spike multiplier over normal rate (default: 10)")
    parser.add_argument("--normal-eps", type=int,   default=5,
                        help="Normal events per second (default: 5)")
    parser.add_argument("--duration",   type=int,   default=30,
                        help="Spike duration in seconds (default: 30)")
    parser.add_argument("--skip-baseline", action="store_true",
                        help="Skip normal traffic phase and spike immediately")
    args = parser.parse_args()

    normal_eps = args.normal_eps
    spike_eps  = normal_eps * args.multiplier

    if not args.skip_baseline:
        # Send normal traffic for 20s so EMA has a baseline to compare against
        run_normal(normal_eps, duration_seconds=20)

    run_spike(spike_eps, args.duration)
