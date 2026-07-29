# EdTech Real-Time Analytics Pipeline — Complete Technical Deep Dive

**CSG527 · BITS Pilani Hyderabad · Two-pipeline comparative build**

---

## How to use this document

This is written so you can answer *any* question about the system. Every component section follows the same five-part shape:

1. **What it is** — the plain description
2. **The idea** — why it exists at all; the problem it solves
3. **How it's built** — real configuration and annotated code
4. **How it fails and recovers** — the fault-tolerance story
5. **How it's optimized** — what was tuned and what it bought

Cross-cutting chapters (Parts IV–VI) then re-slice the same material by **fault tolerance**, **optimization**, **tuning rationale**, **known flaws**, and a **Q&A answer bank**. If you're revising fast: read Part I (ideas), then Part IV (fault tolerance + optimization), then Part VI (Q&A).

**Contents:** Part I — Problem & core ideas · Part II — Pipeline 1 · Part III — Pipeline 2 (component deep dives) · Part IV — Cross-cutting (event trace, fault tolerance, optimization, tuning) · Part V — Flaws & critique · Part VI — Answer bank & reference

---

# PART I — PROBLEM, REQUIREMENTS, AND THE CORE IDEAS

## 1. The problem domain

An online learning platform with **~500 students across 10 courses** emits a continuous stream of activity events. Operators need live visibility: who's active, which courses are losing students, whether traffic is surging, how engagement is trending.

**Workload shape**
- Baseline **~30 events/second**, bursting **3×** during exam periods (tested to 10× = 300 ev/s)
- Six event types: `login`, `logout`, `class_join`, `class_leave`, `session_start`, `session_end`
- JSON payloads carrying `user_id`, `class_id`, device, region, `engagement_score` (0–1), quiz `score` (0–100)

**Why it's hard (state this when asked "what made this non-trivial?")**
- **Bursty, not uniform.** A system sized for 30 ev/s dies at 300; a system sized for 300 wastes money at 30. That tension forces *elasticity*, which is the whole engineering story.
- **Two incompatible freshness requirements in one system.** The dashboard needs sub-5-second KPIs; the data lake is read once a night. Serving both from one path over-engineers the cheap side and under-serves the fast side.
- **Loss is unacceptable, duplication is tolerable-ish.** Analytics can survive a rare duplicate; it can't survive silently dropping a student's quiz result.

**Functional requirements:** real-time KPIs (active users, events/min, surge state, session completion); per-course analytics (joins, dropout %, avg score by difficulty); per-user daily aggregates; nightly historical rollups.

**Non-functional requirements (NFR §5)** — these drive every design choice:

| NFR | Target | Where it's satisfied |
|---|---|---|
| Scalability | Absorb 10× spike | KEDA pods (1→10) + ASG nodes |
| Latency | < 5 s near-real-time | Redis hot path (measured 0.277 ms) |
| Reliability | ≥ 99.9% uptime | Checkpoints, systemd, k3s probes (measured 99.9918%) |
| Data quality | Validation + dedup | Schema-on-parse, `event_id` dedup |
| Cost efficiency | Minimize cost/GB | Adaptive batching (−87.6% PUTs), scale-to-zero |

---

## 2. The six design ideas behind everything

If you internalize only one section, make it this one. Every specific choice downstream is an instance of one of these.

### Idea 1 — Separate the hot path from the cold path
Live dashboard data and archival data have *completely different* freshness requirements: milliseconds vs. "sometime today." Fusing them forces the cheap path to pay the fast path's cost. So: **Redis for hot** (sub-ms, ephemeral, TTL'd) and **S3 Parquet for cold** (cheap, durable, batch-read). This single split is what makes the 87.6% S3 cost saving *free* — nobody is waiting on S3, so you can batch aggressively.

### Idea 2 — Isolate the monitoring path from the storage path
Monitoring is the highest-priority path; storage is second. If S3/DynamoDB back-pressure, the dashboard must not go stale. So the two run as **independent consumers** of the same topic: Spark (monitoring → Redis) and k3s pods (storage → S3/DynamoDB). They scale, fail, and degrade independently. `STORAGE_ENABLED=false` on Spark enforces the split.

### Idea 3 — Scale on the signal that actually means "I'm falling behind"
Storage workers are **I/O-bound** — they sit blocked on S3 `put_object` and DynamoDB `update_item`. CPU stays ~15–20% even when tens of thousands of events are backed up. A CPU-based autoscaler would *never* fire. The metric that genuinely means "behind" is **Kafka consumer-group lag**, so scale on that (KEDA).

### Idea 4 — Intelligence as rules in the data path, not ML beside it
Every adaptive behavior (batching, surge routing, scaling) is a small deterministic rule embedded directly in the processing loop or the orchestrator. No model, no training, no extra service, no inference latency. **Justification:** the decisions are low-dimensional and threshold-shaped; ML would add infrastructure and opacity for no measurable gain.

### Idea 5 — Never lose data; degrade features instead
Under stress the system sheds *freshness of low-priority outputs*, never events. Kafka's 24 h retention is the universal safety net: any consumer failure just means "re-read from the last committed offset." Degradation is tiered (see §20.4) and fully automatic.

### Idea 6 — Stable identity for stateful infrastructure
Distributed systems break when addresses move. Kafka bakes its address into `advertised.listeners`; EC2 changes public IP on stop/start. Hence a **Terraform-managed Elastic IP**. Same principle drives the SSM-stored k3s join token and tag-based instance lookup instead of hardcoded IPs.

---

## 3. The data model

### 3.1 Event lifecycle (a state machine, not random events)

The generator is **stateful** — this matters, because random events would produce statistically meaningless analytics (dropout rates need `class_leave` to actually follow `class_join`).

```
OFFLINE ──login──► ONLINE ──class_join──► IN_CLASS ──session_start──► IN_SESSION
   ▲                 │                        │                            │
   └────logout───────┘                        │◄──────session_end──────────┘
                                              │
                                     class_leave (→ ONLINE)
```

Transition probabilities from `generator.py::_transition`:

| Current state | Transition | Probability |
|---|---|---|
| `ONLINE` | → `class_join` | 80% |
| `ONLINE` | → `logout` | 20% |
| `IN_CLASS` | → `session_start` | 85% |
| `IN_CLASS` | → `class_leave` | 15% |
| `IN_SESSION` | → `session_end` | 100% |
| after `class_leave` | immediately re-join another class | 70% |

**Design detail worth quoting:** a churn manager runs every 2 s to hold **~60% of users online**, and events are dispatched **round-robin across online users** rather than randomly. Without round-robin, a high event rate would concentrate on a few users and `active_users` would under-report — the metric would move with *rate* instead of with *population*. That's a subtle correctness fix in the data generation itself.

### 3.2 Event schema (37 fields)

Grouped by purpose:

- **Core:** `event_id`, `user_id`, `event_type`, `timestamp`, `schema_version`
- **User profile:** `user_role`, `subscription_type`, `user_age_group`, `user_level`, `region`, `country`, `timezone`, `device`, `os`, `app_version`, `session_id`
- **Derived temporal:** `hour_of_day`, `day_of_week`, `is_weekend`, `is_premium` — precomputed at generation so the consumer never has to parse timestamps
- **Event-specific:** `class_id`, `course_name`, `course_category`, `difficulty_level`, `instructor_id`, `session_type`, `leave_reason`, `class_duration_sec`, `interaction_type`, `content_id`, `content_duration`, `session_status`, `session_duration_sec`, `score`, `passed`, `engagement_score`, `active_duration_sec`

**Why `event_id` exists:** it's the deduplication key. Producer retries and consumer rebalances can both re-deliver an event; `event_id` makes duplicates detectable.

**Why `schema_version` exists:** the honest answer — it's a forward-compatibility hook, since there's no schema registry. If asked, say: *"It's a poor-man's schema registry; in production I'd use Avro + Confluent Schema Registry and drop this field."*

---

# PART II — PIPELINE 1: THE SERVERLESS BASELINE

## 4. Pipeline 1, component by component

**Flow:** Generator (local) → Kinesis → Lambda → {S3 raw JSON → Glue → Athena, DynamoDB → Athena federated} → QuickSight; CloudWatch throughout.

### 4.1 Kinesis Data Streams — ingestion
**Idea:** get a managed, durable event bus with zero operational burden.
**Built:** stream `edtech-events-stream` in **on-demand mode** — no manual shard planning, absorbs bursts automatically.
**Fails/recovers:** managed, multi-AZ; retention allows re-read.
**Trade-off:** you don't control the scaling signal, and per-shard/on-demand pricing scales with throughput.

### 4.2 AWS Lambda — processing
**Idea:** event-driven compute with no servers; scales concurrency automatically per shard.
**Built:** triggered by Kinesis; decodes records, normalizes schema, extracts attributes, writes raw JSON → S3, updates operational state → DynamoDB (`ActiveSessions`, `ClassMetrics`), publishes throughput/latency metrics → CloudWatch.
**Fails/recovers:** automatic retries on the Kinesis shard iterator; a poison record blocks the shard until it ages out (this is why a **DLQ** is the standard fix).
**Limit that motivated Pipeline 2:** it is **stateless per invocation**.

### 4.3 Pipeline 1's intelligence layer — EMA spike detection
Worth knowing in detail, because "did P1 have intelligence too?" is a likely question. **Yes:**
- Events aggregated into **fixed 1-minute buckets**
- A rolling baseline maintained as an **Exponential Moving Average (EMA)**
- Spike declared when the current minute exceeds a **configurable multiple** of the EMA baseline
- **The EMA is updated only once per window** — deliberately, so multiple partial updates inside one interval can't bias the baseline
- Spike indicators published as CloudWatch metrics

**The instructive part:** because Lambda is stateless, the EMA and bucket counters must live in **DynamoDB**, so every detection step costs a network round-trip and needs careful read-modify-write handling. In Pipeline 2 the equivalent (`SurgeDetector`) is a `deque` **in process memory** — no I/O at all. *That contrast is the single cleanest illustration of why Pipeline 2 exists.*

### 4.4 S3 + DynamoDB — storage
- **S3:** raw events in **original JSON** (not Parquet — a known inefficiency; Athena scans more bytes)
- **DynamoDB:** `ActiveSessions` and `ClassMetrics` for low-latency operational state

### 4.5 Glue → Athena → QuickSight — catalog, query, visualization
- **Glue crawler** scans S3, infers JSON schema, populates the Data Catalog — no manual DDL
- **Athena** runs SQL over S3 *and*, via a **federated connector**, over DynamoDB — unified querying with no data duplication
- **QuickSight** connects to Athena for dashboards (activity trends, throughput, regional distribution, class engagement)

### 4.6 Documented challenges (say these; they justify Pipeline 2)
1. **Statelessness** — maintaining sessions/metrics forced all state into DynamoDB, adding complexity
2. **Debugging across services** — tracing one event through Kinesis→Lambda→S3→DynamoDB→Athena meant hopping CloudWatch log groups
3. **Cost at high event rates** — invocations scale linearly with events
4. **Spike detection in a stateless world** — windowing + rolling baselines in DynamoDB required careful update logic

---

# PART III — PIPELINE 2: THE SELF-MANAGED SYSTEM

## 5. Topology and the central design decision

```
EC2-1 (t2.medium, Elastic IP)            EC2-2 (t3.medium, 20 GB EBS)
├── generator.py                          ├── Spark Streaming (STORAGE_ENABLED=false)
└── Kafka 4.1.1 KRaft                     │    ├── SurgeDetector → edtech-priority
    ├── edtech-events   (10 partitions)   │    ├── Redis 7 (16 live pipeline:* keys)
    └── edtech-priority ( 3 partitions)   │    └── Prometheus metrics :8000
              │                           ├── k3s server → storage pods (1–10, KEDA)
              │  :9093 external           ├── batch_reports.py (cron, midnight UTC)
              ├───────────────┬──────────►└── Prometheus + Grafana (:3000)
              │               │
       checkpoint offsets   consumer group          AWS: S3 · DynamoDB×3 · IAM · SSM · ASG
       (Spark, monitoring)  edtech-storage-group
                            (pods, storage)
```

### 5.1 The dual-consumer split — the most important decision in the system

**Both consumers read every event independently.** This is deliberate, not accidental duplication.

| | Spark (`stream_consumer.py`) | Storage pods (`storage_worker.py`) |
|---|---|---|
| Offset management | **Spark checkpoint directory** | **Kafka consumer group** `edtech-storage-group` |
| Reads | `edtech-events` | `edtech-events` |
| Writes | Redis + Prometheus | S3 + DynamoDB |
| Scaling | Fixed — 1 systemd service | KEDA 1→10 pods on lag |
| Runtime | Spark JVM (~1.5 GB) | Python (~107 MB) |

**Why split at all?** Three reasons, in priority order:
1. **Blast-radius isolation** — a slow S3 or a DynamoDB throttle cannot delay the dashboard, because that work happens in a different process on a different scaling schedule.
2. **Independent scaling** — storage throughput needs to scale 10× under load; the monitoring aggregation does not (it's one aggregate view, and one process computes it correctly).
3. **Right runtime for the job** — storage work is trivial I/O, so paying the JVM's 1.5 GB is absurd. Python at ~107 MB gives roughly **6× the pod density per node**, which directly raises the throughput ceiling.

**Subtle detail that impresses interviewers:** Spark does **not** register a traditional Kafka consumer group. It manages offsets in its checkpoint directory, so `kafka-consumer-groups.sh --list` shows *only* `edtech-storage-group`. If asked "where's your Spark consumer group?" — that's the answer, and it's also why KEDA can measure storage lag cleanly without Spark's reads polluting the number.

**Why no duplicate writes?** `STORAGE_ENABLED=false` on Spark. The pods own every S3/DynamoDB write.

---

## 6. The generator and producer tuning

**Idea:** produce statistically valid, controllable load — and be a well-behaved Kafka producer.

```python
kwargs = {
    "bootstrap_servers": bootstrap_servers,
    "value_serializer":  lambda v: json.dumps(v).encode("utf-8"),
    "key_serializer":    lambda k: k.encode("utf-8") if k else None,
    "acks":              1,        # leader-only ack
    "retries":           3,        # transient failure resilience
    "linger_ms":         10,       # wait 10ms to fill a batch
    "batch_size":        65536,    # 64 KB batches
    "compression_type":  "lz4",    # fast compression
}
...
self._prod.send(self.topic, value=event, key=event.get("user_id"))
```

**Every parameter explained:**

| Setting | Value | Why |
|---|---|---|
| `key=user_id` | — | **Same user → same partition → per-user ordering guaranteed.** Kafka only orders *within* a partition, so keying is how you get "this user's `login` precedes their `class_join`." |
| `acks` | `1` | Leader acknowledges without waiting for replicas. Faster; acceptable at RF=1 (single broker) — with a replicated cluster you'd want `acks=all`. |
| `retries` | `3` | Survives transient broker blips — **and is precisely why deduplication is needed**: a retry after a partially-successful send can duplicate an event. |
| `linger_ms` | `10` | Wait up to 10 ms to accumulate a batch. Trades 10 ms latency for far better throughput and compression ratio. |
| `batch_size` | `65536` | 64 KB per batch — bigger batches compress better and cut request count. |
| `compression_type` | `lz4` | Chosen for **speed over ratio** (vs gzip). Network is the constraint; CPU on a t2.medium is precious. |

**Modes:** `continuous`, `spike` (pre/spike/post phases), `failure` (pause to simulate producer death), `file`, `--dry-run`. Rate control uses a **monotonic clock** so actual ev/s matches configured ev/s.

**Fault behavior:** `send()` returns `True/False`; failures are counted and logged but **never fatal** — the generator keeps running rather than crashing the load test.

> **Interview trap:** "You said zero data loss, but `acks=1` — can't you lose events?" **Correct answer:** *"Yes, in principle — `acks=1` means the leader acknowledged but replicas may not have. With RF=1 on a single broker it's moot, since there are no replicas either way. In a production multi-broker cluster I'd set `acks=all` with `min.insync.replicas=2`, and enable the idempotent producer to make retries safe. That's part of the same upgrade as going multi-broker."*

---

## 7. Apache Kafka — the backbone

### 7.1 Why Kafka (the idea)
The requirement was a **durable, replayable, partitioned log** — not a queue. Specifically:
- **Buffer** — absorb a 3× surge so no producer-side loss (24 h retention)
- **Replay** — reprocess from any offset when a consumer changes or fails
- **Partition parallelism** — the mechanism that makes consumer scaling *possible at all*
- **Consumer-controlled offsets** — which is what makes lag a measurable, scalable signal

### 7.2 KRaft mode
Kafka 4.1.1 in **KRaft** (Kafka Raft) — metadata lives in Kafka itself, **no ZooKeeper**. One less distributed system to install, secure, and monitor on a t2.medium. Modern default; ZooKeeper is deprecated.

### 7.3 Topics and partitioning

| Topic | Partitions | Purpose |
|---|---|---|
| `edtech-events` | **10** | Primary stream; all consumers |
| `edtech-priority` | **3** | Surge overflow lane (QoS isolation) |

**Why exactly 10 partitions?** Partition count *is* the parallelism ceiling — within a consumer group, one partition is read by at most one consumer. 10 partitions = max 10 useful pods. It was chosen to match the intended max pod count for the 10× load target (300 ev/s ÷ 10 pods ≈ 30 ev/s/pod, the rate a single pod comfortably handles). An 11th pod would be assigned nothing and sit idle.

**Listeners:** `9092` = PLAINTEXT internal (generator runs on EC2-1, uses `localhost:9092`); `9093` = EXTERNAL for EC2-2, pods, and KEDA.

### 7.4 The Elastic IP problem (a favorite war story)
**Symptom:** stop/start EC2-1 → every consumer loses the broker.
**Root cause:** Kafka advertises a fixed address to clients via `advertised.listeners`; EC2's public IP changes on stop/start, so the advertised address becomes a lie.
**Fix:** a Terraform-managed **Elastic IP** (`aws_eip.ec2_1` + association), plus `apply_eip.sh` which SSHes in, rewrites `server.properties` (`sed` on `EXTERNAL://...:9093`), restarts Kafka, updates `/etc/edtech-spark.env` on EC2-2, patches the k3s ConfigMap, and updates the local `configmap.yaml`.
**Lesson to state:** *"Stateful infrastructure needs stable identity — I made the address a managed resource rather than a fact I kept re-discovering."*

### 7.5 Failure behavior
Single broker = **SPOF** (own this openly). If it dies: consumption pauses, dashboard goes stale, **but nothing is lost** — the generator's sends fail and are logged, and everything already in Kafka survives on disk. Consumers reconnect automatically via the stable EIP. **Production fix:** 3 brokers, RF=3, `min.insync.replicas=2` — a broker loss becomes a leader election instead of an outage. Throughput ceiling on one t2.medium is **~800 ev/s**.

---

## 8. Spark Structured Streaming — the monitoring consumer

### 8.1 Why Spark (the idea)
Needed a **stateful** stream processor with Python support and durable offset management. Structured Streaming gives micro-batch semantics with checkpoint-based recovery, and the same API covers the batch job.

### 8.2 Source configuration

```python
raw = (spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe",            KAFKA_TOPIC)
    .option("startingOffsets",      "latest")
    .option("failOnDataLoss",       "false")
    .option("maxOffsetsPerTrigger", "30000")   # back-pressure guard
    .load())
```

| Option | Why |
|---|---|
| `startingOffsets=latest` | It's a live dashboard — on first start, skip history rather than replaying days of backlog |
| `failOnDataLoss=false` | If retention deletes an offset Spark expects, log and continue instead of crashing. Availability over strictness for a *monitoring* path |
| `maxOffsetsPerTrigger=30000` | **Back-pressure**: cap events per micro-batch so a huge backlog can't create an unbounded batch that OOMs the driver. 30,000 = 10× headroom over a 3,000-event 10× batch |

### 8.3 Session tuning

```python
.config("spark.sql.shuffle.partitions", "4")   # default is 200
.config("spark.executor.memory", "1g")
.config("spark.driver.memory",   "512m")
```

**`shuffle.partitions=4` is the highest-value tune here.** Spark's default of 200 would spawn 200 tasks per shuffle for batches of a few hundred rows — scheduling overhead would dwarf the actual work. Matching parallelism to data size (4 on the stream, 2 on the batch job) is basic but frequently missed.

**Deliberate non-setting:** `spark.jars` is *not* set. All JARs live in `/opt/spark/jars/` and are passed via `spark-submit`; setting `spark.jars` triggers a **331 MB re-copy to /tmp at every startup**, adding ~30 s to recovery.

### 8.4 The `foreachBatch` micro-batch model

`.trigger(processingTime="10 seconds")` + `.foreachBatch(process_batch)` — Spark hands you a normal DataFrame every 10 seconds and you do arbitrary Python with it. That's what makes writing to Redis, Kafka, and Prometheus (none of which are native Spark sinks) possible.

### 8.5 The single-`collect()` optimization — the deepest performance insight

The original implementation issued **21 separate Spark actions** per batch (a `count()`, `distinct().count()`, `groupBy().collect()` for each metric…). Each action re-scans the DataFrame and pays full job-scheduling overhead.

```python
# ONE Spark action: pull the entire batch to the driver
events: list[dict] = [row.asDict() for row in batch_df.collect()]
count = len(events)
# ...every KPI below is a pure-Python comprehension over `events`
active_users_now = len({e["user_id"] for e in events if e.get("user_id")})
```

**The reasoning:** at 300 events/batch (~300 KB) or even 3,000 (~3 MB), the data trivially fits in the driver. Distributed aggregation is *pure overhead* at this size — 21 scheduled jobs to process 3 MB. Pulling once and computing in Python collapsed **21 actions → 1** and is the main reason batch time is **2.0 s** rather than tens of seconds.

**The senior framing:** *"Spark is a distributed engine, but distribution has fixed costs. Below a few MB per batch the right move is to stop distributing. Knowing when* not *to use the distributed path is the optimization."* Caveat to volunteer: this stops scaling if batches reach hundreds of MB — at that point you'd go back to DataFrame aggregations.

### 8.6 Checkpointing

```python
CHECKPOINT_PATH = f"/tmp/edtech-checkpoints/stream-{_hostname}"
```

Per-hostname, because a shared checkpoint path across multiple pods causes `ConcurrentModificationException`. Contents after 17 h: `commits/`, `offsets/`, `metadata`, `sources/0/` — **~2.4 MB for 290 batches (~8.3 KB/batch)**.

**Semantics:** offsets are committed only *after* the batch's work succeeds, so a crash re-processes the last uncommitted batch rather than skipping it — **effectively-once**, at-least-once at the boundary.

**Measured cost:** 0.5 s of the 2.0 s batch = **25% overhead**. Recovery read: **< 1 s**.

### 8.7 Batch time breakdown (30 samples)

| Phase | Time |
|---|---|
| Kafka fetch + JSON deserialization | 0.8 s |
| `foreachBatch` user code (Redis, metrics, intelligence) | 0.7 s |
| Checkpoint write | 0.5 s |
| **Total** | **2.0 s** (P95 2.47 s) |

### 8.8 Deduplication — and a real bug to disclose

```python
dedup_key = "pipeline:dedup_event_ids"
pipe = r.pipeline()
for ev in events:
    if ev.get("event_id"):
        pipe.sadd(dedup_key, ev["event_id"])   # 1 = new, 0 = duplicate
add_results = pipe.execute()
r.expire(dedup_key, 3600)
```

**Idea:** producer retries (`retries=3`) and consumer rebalances both re-deliver. A Redis **Set** gives O(1) membership, and pipelining sends all `SADD`s in one round-trip. Events without `event_id` pass through unfiltered; duplicates increment `pipeline_events_deduped_total`.

> **⚠️ Known defect — disclose this proactively, it reads as rigor.** The docs describe a "1-hour **sliding window**," but `r.expire(dedup_key, 3600)` **resets the TTL on the whole set every batch**. Under continuous traffic the key never expires, so the set grows **unboundedly** — at 30 ev/s that's ~2.6 M members/day, and Redis memory grows without limit. It only actually clears after a full hour with *no* traffic. **Correct fix:** a Redis Set with per-member expiry isn't native, so use either (a) time-bucketed keys — `dedup:{YYYYMMDDHH}` with a real TTL per bucket, checking current+previous bucket, or (b) a Bloom filter / `SETEX` per `event_id`. Saying this unprompted is a strong signal.

---

## 9. Intelligence Component 1 — SurgeDetector (§7.3, SLA-aware processing)

### 9.1 The idea
Exam periods cause bursts. A burst that isn't recognized becomes growing consumer lag, then a stale dashboard, then a violated SLA. The system must (a) detect quickly, (b) give critical events a faster lane, and (c) **not oscillate** — a detector that flips state every 10 s is worse than none, because the dashboard flickers and any downstream reaction thrashes.

### 9.2 Annotated implementation

```python
class SurgeDetector:
    WINDOW_SECS    = 300   # 5-minute rolling window
    SURGE_MULT     = 1.5   # enter surge above 1.5× baseline
    NORMAL_MULT    = 1.1   # exit only below 1.1× — hysteresis band
    MIN_SURGE_HOLD = 6     # stay in surge ≥6 batches (~60 s)

    def update(self, batch_count, trigger_interval_secs, events=None):
        now          = time.time()
        current_rate = batch_count / max(trigger_interval_secs, 1)
        self._window.append((now, current_rate))   # store RATE, not raw count
        self._prune()                              # drop entries older than 300s
        rolling_avg = self._rolling_avg()

        if not self.in_surge and rolling_avg > 0 and current_rate > rolling_avg * self.SURGE_MULT:
            self.in_surge    = True
            self._surge_hold = self.MIN_SURGE_HOLD
            self._get_redis().set("pipeline:surge_detected", "1", ex=120)
            if events is not None:
                self._send_priority(events)
        elif self.in_surge:
            if self._surge_hold > 0:
                self._surge_hold -= 1              # burn the hold first
            elif current_rate <= rolling_avg * self.NORMAL_MULT:
                self.in_surge = False
                self._get_redis().set("pipeline:surge_detected", "0", ex=120)
        return self.in_surge
```

### 9.3 Four design decisions worth defending

**1. Store *rate*, not raw count.** `current_rate = batch_count / trigger_interval` makes the multipliers **scale-independent** — the same 1.5× rule works whether the trigger is 10 s or 30 s. Storing raw counts would couple the threshold to the trigger interval.

**2. Two thresholds, not one (1.5× in, 1.1× out).** This is **hysteresis** — the same principle as a thermostat. A single threshold means any rate hovering near it flips state constantly. The gap between 1.5 and 1.1 creates a dead band that must be fully crossed to change state.

**3. A minimum hold of 6 batches on top of hysteresis.** Belt and braces: even inside the dead band, once surge is declared it persists ≥6 batches (~60 s). Why: surges are *bursty* — a momentary dip below threshold doesn't mean the exam ended. Prevents `surge_detected` flipping every 10 s in Redis and destabilizing the dashboard.

**4. Only two event types are forwarded.**
```python
priority_rows = [e for e in events if e.get("event_type") in ("class_join", "session_start")]
```
`class_join` and `session_start` are the *leading indicators* of live engagement — the events that drive "who is active right now." `logout` and `session_end` are trailing and can wait. **Priority routing is about which events matter most under stress, not about volume.**

### 9.4 Measured results

| Metric | Value |
|---|---|
| Detection latency | **< 40 s** (one micro-batch) |
| Detection ratio | **1.84×** (obs 1), 1.89× (obs 2) |
| Margin above threshold | (1.84−1.5)/1.5 = **22.7%** |
| Events forwarded in one surge batch | 1,200 |
| Consumer lag during surge | **bounded at 3,091** |
| False positives in 17 h+ | **0** |

**Why 3× input produced only 1.84× measured ratio:** the rolling average is *still rising* while the spike is in progress — the baseline chases the signal — so the observed ratio is compressed. This is exactly why the threshold sits at 1.5 and not 2.5.

### 9.5 The honest gap
The priority topic **has no dedicated consumer**. `SurgeDetector` produces to `edtech-priority`, but neither Spark nor the storage pods subscribe to it (the docs concede: *"currently same consumer"*). So the producer half of a priority-QoS pattern shipped; the consumer half didn't, and the isolation benefit is **theoretical as-built**.

**How to say it:** *"It's a QoS lane, not extra throughput — it's meant to be drained by its own consumer group, independent of the storage group's 10-pod cap, so critical live-signal events never queue behind the storage backlog. I provisioned the topic and the routing but didn't deploy the consumer, so today it's a demonstrated hook. Finishing it means a small dedicated group — 1–3 pods with their own KEDA trigger, or a direct Spark→Redis fast path."*

---

## 10. Intelligence Component 2 — AdaptiveBatchOptimizer (§7.1, cost–latency)

### 10.1 The idea
Naive Structured Streaming writes **one S3 object per micro-batch**. At 30 ev/s with a 10 s trigger that's 300 events → a **5–10 KB Parquet file**, every 10 s → **8,640 PUTs/day**. Two problems: PUT-request cost, and Parquet's columnar compression barely works at that size (per-file metadata overhead dominates).

**The unlock is Idea 1:** S3 serves only the *nightly batch job*. Nobody is waiting on it. So **S3 freshness is a free variable** — you can delay writes by minutes with zero user-visible impact, while Redis keeps the dashboard sub-millisecond.

### 10.2 Implementation

```python
class AdaptiveBatchOptimizer:
    FLUSH_EVENT_THRESHOLD = 10_000   # throughput trigger
    FLUSH_TIME_SECS       = 300      # latency bound (5 min)

    def add_events(self, events: list[dict]):
        self._baseline_calls += 1          # what naive would have cost
        self._buffer.extend(events)
        elapsed = time.time() - self._last_flush
        if (len(self._buffer) >= self.FLUSH_EVENT_THRESHOLD
                or elapsed >= self.FLUSH_TIME_SECS):
            self._flush()
```

**A dual trigger is essential — this is the part to explain well:**
- **Event threshold alone** → at low traffic the buffer might take an hour to fill; S3 goes stale and a crash risks more buffered data.
- **Time threshold alone** → at high traffic you'd flush a huge buffer on a fixed clock, and file sizes would swing wildly.
- **Whichever fires first** → bounded staleness (**never > 5 min**) *and* bounded file size. High-throughput case flushes on count; low-throughput case flushes on time.

The optimizer also self-instruments: it counts `_baseline_calls` (one per micro-batch = what naive would have done) vs `_optimized_calls` (actual flushes) and publishes `pipeline:s3_cost_stats` to Redis, which Grafana renders as a live cost-reduction %.

### 10.3 Measured impact

| Strategy | PUTs / 1.5 h | PUTs / day | Cost / day | Avg file size |
|---|---|---|---|---|
| Naive (per-batch) | 225 | 8,640 | $0.0432 | ~8 KB |
| **AdaptiveBatch** | **28** | ~1,075 | **$0.0054** | **~536 KB** |
| **Improvement** | — | — | **87.6%** | **67× larger** |

**Why bigger files matter beyond PUT cost (three effects, name all three):**
1. **Compression** — Parquet's columnar encoding (dictionary, RLE) only pays off once there are enough rows per column chunk; at 8 KB, metadata dominates.
2. **Read performance** — the nightly job opens 125 files instead of 8,640; S3 GET latency is dominated by per-request round-trips, not bytes.
3. **Avoiding the small-file problem** — the classic data-lake pathology that eventually forces a compaction job.

### 10.4 Threshold asymmetry (a detail that shows care)
`FLUSH_EVENTS` is **10,000 in Spark** but **5,000 in the pods**. Why: each pod owns only a *subset* of partitions, so it sees a fraction of the stream. Halving the threshold keeps the *flush cadence* similar rather than letting pods buffer for very long stretches.

### 10.5 Failure behavior
The buffer is **in-memory and not persistent — deliberately**. On SIGKILL the buffered events are simply *not yet committed* in Kafka, so the replacement consumer re-reads them. Persisting the buffer would add a durability layer to protect data that is *already durable in Kafka*. Max at-risk window = `max_poll_interval_ms (120 s) × event rate` ≈ 3,600 events, all recoverable from 24 h retention.

---

## 11. Redis — the hot store

### 11.1 Why Redis
Sub-millisecond reads, a **native Grafana datasource plugin** (so the dashboard reads it directly — no API layer to build), and simple key/value + hash semantics that match "current value of a KPI." **Measured: 0.277 ms mean GET, P95 0.477 ms** — roughly 10,000× faster than the 5 s NFR.

### 11.2 Tiered TTLs — an under-appreciated design detail

```python
redis_write({...}, ttl=120)   # Window 1 — active users, events/min, regions
redis_write({...}, ttl=360)   # Window 2 — course analytics, engagement, dropout
redis_write({...}, ttl=900)   # Window 3 — class aggregates, device ratio
```

| Tier | TTL | Contents | Rationale |
|---|---|---|---|
| 1 | 120 s | active users, events/min, region breakdown | Volatile — a 2-minute-old "active users" is *wrong*, so let it expire and show N/A |
| 2 | 360 s | course analytics, engagement, dropout | Moderately stable |
| 3 | 900 s | class counts, avg duration, device ratio | Slow-moving; long TTL avoids gaps in sparse batches |

**The idea:** TTL encodes *how fast a metric becomes a lie*. It also makes staleness **self-healing** — if the pipeline dies, panels go blank rather than displaying confidently wrong numbers. Silent staleness is worse than a visible gap.

### 11.3 The master-consumer guard

```python
if IS_MASTER_CONSUMER:
    redis_write({...})
else:
    log.debug("skipping Redis (non-master consumer)")
```

**Why:** each consumer instance sees only *its* share of events. If several instances wrote KPIs to the same keys, they'd overwrite each other with *partial* views — `active_users` would flicker between fragments. So exactly one instance (EC2-2's Spark) is designated master and writes the aggregate view.

**Interview framing:** *"Aggregations that must be global need a single writer, or a merge strategy. Counters can be distributed (DynamoDB `ADD`); a set-cardinality like `active_users` cannot be merged from partial views without something like HyperLogLog — so I chose a single writer."* That last clause is exactly the kind of thing that reads as depth.

### 11.4 Failure behavior
Redis down → Grafana panels show N/A; Spark logs the failure and retries next batch; **S3/DynamoDB writes are unaffected** (different path). systemd restarts Redis. **No data loss** — Redis holds only derived, regenerable values, never source of truth. Worth saying explicitly: *"Redis is a cache, not a store. Everything in it can be recomputed from Kafka or S3."*

---

## 12. The storage worker pods

### 12.1 Why a plain Python consumer instead of Spark
| | Spark pod | Python pod |
|---|---|---|
| RAM | ~1.5 GB (JVM) | **~107 MB actual** |
| Pods per t3.medium | ~2 | **~8** |
| Startup | JVM + session init | ~seconds |

The work is trivial — poll, buffer, serialize Parquet, PUT, `update_item`. No joins, no windowing, no shuffles. Paying a distributed engine's overhead for that is waste; **6× the pod density directly raises the throughput ceiling** for the same node.

### 12.2 Consumer configuration and the rebalance-storm fix

```python
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    group_id=CONSUMER_GROUP_ID,          # edtech-storage-group
    auto_offset_reset="latest",
    enable_auto_commit=True,
    auto_commit_interval_ms=5000,
    fetch_max_bytes=52428800,            # 50 MB per poll
    max_partition_fetch_bytes=1048576,   # 1 MB per partition
    consumer_timeout_ms=2000,
    # ── scale-out race-condition mitigations ──
    heartbeat_interval_ms=10000,   # prove liveness every 10s
    session_timeout_ms=45000,      # tolerate 45s without heartbeat
    max_poll_interval_ms=120000,   # allow 2 min between polls
)
```

**The war story in full:**
- **Symptom:** scaling 1→10 pods made lag *spike* and pods thrash — the opposite of intended.
- **Root cause:** every pod joining a consumer group triggers a **group rebalance** (partitions are re-assigned, and consumers briefly stop). With the default `session_timeout_ms=10000`, when 8+ consumers negotiate simultaneously a pod can miss its heartbeat window mid-rebalance → the coordinator evicts it → eviction triggers *another* rebalance → cascade. Worse, the resulting lag spike made **KEDA add even more pods**, feeding the storm. A genuine positive-feedback failure loop.
- **Fix and the arithmetic:** `session_timeout_ms=45000` (survive a full multi-consumer rebalance cycle), `heartbeat_interval_ms=10000` (**must be < session_timeout/3** — 10 s < 15 s ✓, so up to two heartbeats can be lost before eviction), `max_poll_interval_ms=120000` (a slow S3 flush must not look like a dead consumer).
- **Lesson:** *"Elastic consumers and consumer-group rebalance timing interact violently. Autoscaling a stateful group means tuning the group protocol, not just the replica count."*

### 12.3 The main loop
```python
while _running:
    records = consumer.poll(timeout_ms=1000, max_records=2000)
    for _, messages in records.items():
        for msg in messages:
            if isinstance(msg.value, dict):
                buffer.append(msg.value)
    # ... lag gauge computed from assignment() / end_offsets() / committed()
    if len(buffer) >= FLUSH_EVENTS or (now - last_flush) >= FLUSH_SECS:
        _flush(buffer)          # S3 Parquet + DynamoDB UserStats + CourseStats
```

Each pod also computes its **own** lag (`end_offset − committed` over its assigned partitions) and exposes it on `:8001` — per-pod observability independent of KEDA's own polling.

### 12.4 SIGTERM graceful drain

```python
def _handle_signal(sig, frame):
    global _running
    log.info("Signal %s received — draining buffer and shutting down", sig)
    _running = False        # exit the loop; do NOT die immediately

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)
...
# after the loop:
_flush(buffer)      # drain up to 5,000 buffered events to S3 + DynamoDB
consumer.close()    # leave the group cleanly → fast, clean rebalance
```

**Why it matters:** in a KEDA-scaled deployment the *most common* pod death is a **planned** scale-in, not a crash. Kubernetes sends SIGTERM and waits `terminationGracePeriodSeconds` before SIGKILL. Without a handler, every scale-in would discard the in-memory buffer.

**Measured:** mean drain **4.7 s**, grace period **60 s** (≈12× headroom), **0 events lost across 3 forced terminations**.

`consumer.close()` is not cosmetic — leaving the group explicitly triggers an immediate clean rebalance instead of making the coordinator wait out `session_timeout_ms`.

### 12.5 Deployment mechanics — the ConfigMap-mounted script

No Docker image was built. The deployment uses stock `python:3.11-slim` with:
- an **init container** running `pip install kafka-python boto3 pyarrow prometheus_client lz4 -t /packages` into a shared `emptyDir`
- the main container running `PYTHONPATH=/packages python3 /scripts/storage_worker.py`, where `/scripts` is `storage_worker.py` mounted from a **ConfigMap**

**Trade-off to state honestly:** *"This avoids needing a registry and makes iteration instant — edit the ConfigMap, restart. The cost is ~60–90 s of pip install on every pod start, which is why `initialDelaySeconds` is 90 and why pod startup dominates recovery time. In production I'd bake an image and push to ECR; startup would drop to seconds and installs would be reproducible."* That's a textbook prototype-vs-production trade-off.

### 12.6 Probes and resources
```yaml
resources:
  requests: { cpu: "250m", memory: "256Mi" }   # what the scheduler reserves
  limits:   { cpu: "500m", memory: "512Mi" }   # hard ceiling
livenessProbe:  { path: /metrics, port: 8001, initialDelaySeconds: 90, periodSeconds: 30, failureThreshold: 3 }
readinessProbe: { path: /metrics, port: 8001, initialDelaySeconds: 90, periodSeconds: 15 }
terminationGracePeriodSeconds: 60
```
- **requests vs limits:** requests drive *scheduling* (10 pods × 250m = 2,500m > the 2,000m on a t3.medium → **4 pods go Pending**, which is precisely the trigger for Tier-2 node scaling). Limits prevent one pod starving others.
- **The `/metrics` endpoint doubles as the health check** — no separate health server. If the Prometheus HTTP thread is dead, the process is unhealthy by definition.
- **`initialDelaySeconds: 90`** exists *because of* the pip-install init container — probing earlier would kill pods during startup, creating a crash loop. Real dependency between two design choices.

---

## 13. k3s + KEDA — Tier 1 scaling

### 13.1 The layering (a very common point of confusion)
- **Kubernetes (k8s)** — the orchestration platform/API
- **k3s** — a *lightweight distribution* of Kubernetes; same API, single binary, minimal control-plane overhead. Fits on a t3.medium alongside Spark, Redis and Grafana. (Analogy: k8s is "a Linux," k3s is a small distro of it.)
- **KEDA** — an *add-on installed inside* the cluster (v2.13.1) that provides event-driven autoscaling

**Under the hood:** KEDA doesn't replace Kubernetes scaling — it **feeds an external metric into a standard HPA**. Chain: `KEDA (reads Kafka lag) → HPA → Deployment replicas → k3s schedules pods`. So "KEDA vs HPA" is a false dichotomy: KEDA *drives* an HPA with a better metric than CPU.

### 13.2 Why k3s over EKS
EKS costs a control plane (~$0.10/h) plus CNI/DNS overhead, on a 2-node student deployment. k3s gives identical pod lifecycle, HPA, probes, and scheduling for ~zero overhead. **When you'd switch:** ≥3 worker nodes, m5.large+, or when you want managed upgrades/HA control plane.

### 13.3 The ScaledObject

```yaml
spec:
  scaleTargetRef: { name: edtech-storage-worker }
  minReplicaCount: 1
  maxReplicaCount: 10          # = partition count
  pollingInterval: 15          # seconds
  cooldownPeriod: 120          # seconds at lag 0 before scaling in
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: "3.7.163.17:9093"
        consumerGroup:    "edtech-storage-group"
        topic:            "edtech-events"
        lagThreshold:     "100"
        offsetResetPolicy: "latest"
```

**The scaling math:** `desiredReplicas = ceil(totalLag / lagThreshold)`, clamped to `[1, 10]`.

| Load | ev/s | Est. lag | Formula | Actual |
|---|---|---|---|---|
| 1× | 30 | ~0 | ceil(0/100)=0 → 1 | 1 (min) |
| 3× | 90 | ~300 | ceil(300/100)=3 | 3 |
| 10× | 300 | ~3,000 | ceil(3000/100)=30 | **10 (capped)** |
| 100× | 3,000 | ~30,000 | ceil(30000/100)=300 | **10 (capped)** |

**Every parameter defended:**
- **`lagThreshold: 100`** — "how many events one pod may be behind before adding another." Lower = twitchier; higher = laggier. 100 is small enough to react within a poll but large enough not to scale on noise.
- **`maxReplicaCount: 10`** — hard-tied to partition count. **An 11th pod would receive no partition assignment and idle.** Naming this shows you understand the Kafka consumer model.
- **`minReplicaCount: 1`** — never scale to zero, so there's always a live consumer holding group membership and no cold-start on the first event.
- **`pollingInterval: 15 s`** — the reaction-time floor. Measured 1→5 pods in ~20 s (first poll after breach), 5→10 in ~40 s (second poll).
- **`cooldownPeriod: 120 s`** — prevents scale-in thrash. All 10 partitions can flush simultaneously, producing a *momentary* lag=0 that doesn't mean load has ended.

### 13.4 Why not CPU-based HPA — the crisp argument
Storage pods are **I/O-bound**: they block on network calls to S3 and DynamoDB. CPU stays **under 20%** even at 50,000 events of lag. An HPA watching CPU would sit at 1 replica while the backlog grew unbounded. **CPU is a proxy; lag is the actual quantity of interest.** Response time also differs: HPA on CPU needs a 1–3 min averaging window; KEDA reacts in 15 s.

### 13.5 Measured
1→5 pods in **20 s**; 5→10 in **40 s** total; steady-state lag **5–20 events per partition** (effectively zero); pod RAM **~107 MB**.

---

## 14. Terraform, ASG, CloudWatch — Tier 2 scaling and infrastructure

### 14.1 Why two tiers
KEDA can only place pods on **existing nodes**. At 10 pods × 250m = 2,500m requested against 2,000m available on EC2-2, **4 pods go Pending** — KEDA has hit its ceiling and more pods won't help. Only a *new node* fixes that.

| | Tier 1 — KEDA | Tier 2 — ASG |
|---|---|---|
| Unit | Pod (container) | EC2 node (VM) |
| Signal | Kafka lag | EC2-2 CPU > 70% (2 min) |
| Speed | ~20–40 s | ~3–4 min (boot + k3s join ≈ 90 s) |
| Range | 1→10 | 0→N |

Ordering matters: **pods first (cheap, fast), nodes only when pods can't schedule** — minimizes node churn since nodes are slow and expensive.

### 14.2 The CloudWatch "no data at zero" trap

```hcl
resource "aws_cloudwatch_metric_alarm" "high_cpu" {
  evaluation_periods = 2
  period             = 60
  threshold          = 70
  treat_missing_data = "notBreaching"
  dimensions = { InstanceId = data.aws_instance.ec2_2.id }   # EC2-2, NOT the ASG
}
```

**The problem:** the natural instinct is to watch the ASG's average CPU. But the ASG **starts at 0 instances** — with no instances there is no CPU metric, the alarm sits in `INSUFFICIENT_DATA`, and **scale-out can never trigger**. A bootstrapping paradox: the metric that would create the first node only exists after the first node exists.

**The fix:** watch **EC2-2**, which is always running. Its CPU rising is a valid proxy for "the k3s server is saturated and pods are stuck Pending." And `treat_missing_data = "notBreaching"` keeps the scale-in alarm silent when the ASG is at 0.

**Lesson to state:** *"An autoscaling trigger must be observable before the resource it scales exists."* Genuinely memorable.

### 14.3 Node bootstrap (launch template userdata)
1. `apt-get install curl jq awscli`
2. Read the k3s join token from **SSM SecureString** `/edtech/k3s-token`
3. Discover EC2-2's **private** IP via `describe-instances` filtered on `tag:Name=spark-redis` (tags, not hardcoded IPs — Idea 6)
4. `curl -sfL https://get.k3s.io | K3S_URL=... K3S_TOKEN=... INSTALL_K3S_EXEC="agent" sh -`
5. Node registers → the scheduler immediately places Pending pods

Note the deliberate use of the **private** IP: cluster traffic stays inside the VPC (cheaper, faster, safer) even though EC2-2 has a public EIP.

### 14.4 Policies
- **Scale-out:** +1 instance, `cooldown = 180 s` — deliberately longer than the ~90 s k3s agent boot, so the alarm can't add a second node before the first one is even useful.
- **Scale-in:** −1 instance when ASG avg CPU < 25% for **10 consecutive minutes**, `cooldown = 300 s`. Asymmetric on purpose: **scale out fast, scale in slow.** Being slow to release capacity is cheap; being slow to add it violates the SLA.
- `min_size = 0` → **zero idle EC2 cost** at baseline.

### 14.5 The rest of the Terraform estate
- **S3:** versioning on; **lifecycle → GLACIER after 30 days**; full public-access block
- **DynamoDB ×3:** `UserStats` (PK `user_id`, SK `date`), `CourseStats` (PK `course_id`, SK `date`), `DailyReports` (PK `report_date`) — all **PAY_PER_REQUEST** (no idle cost, no capacity planning)
- **IAM:** one role + instance profile; scoped S3/DynamoDB actions, `ec2:DescribeInstances` (for Prometheus EC2 service discovery), `ssm:GetParameter` on `/edtech/*`. **No credentials anywhere in code** — instance-profile auth throughout.
- **SSM SecureString** for the k3s token, with `lifecycle { ignore_changes = [value] }` so a later `terraform apply` can't clobber the real token written by `setup_k3s.sh`
- **EIP + association** for EC2-1
- **Security groups** for Kafka 9092/9093, Grafana 3000, Prometheus 9090, Spark metrics 8000, Redis 6379 (VPC-only), k3s API 6443, Flannel VXLAN 8472/udp

---

## 15. S3 — the Parquet data lake

### 15.1 What "Parquet lake" means
- **Data lake** — raw, event-granular data in cheap object storage, queried later (*schema-on-read*), as opposed to a warehouse that models data on write
- **Parquet** — Apache Parquet, a **columnar** file format: stores column-by-column so similar values sit together (far better compression) and a query reads only the columns it needs

**Important clarification:** these aren't two steps. The lake *is* Parquet — raw events are written directly in Parquet. "Raw" means **event-level granularity, not aggregated**; it does *not* mean raw JSON. (Pipeline 1 stored literal raw JSON; Pipeline 2 upgraded to Parquet.)

### 15.2 How data is actually written (a commonly-misunderstood sequence)
1. Event JSON arrives from Kafka → deserialized to a dict (a **row**)
2. Appended to an **in-memory buffer** — still row-oriented, uncompressed, nothing on S3 yet
3. **Flush trigger:** buffer ≥ threshold **OR** 5 minutes elapsed
4. The whole buffer is pivoted **rows → columns**, compressed with **snappy**, written as **one new Parquet object**
5. Buffer cleared, accumulate the next file

Columnar conversion happens **once, at flush**, not per-event. And every flush is a **separate immutable object** — you never append to an existing S3 object.

### 15.3 Partitioning
```
s3://edtech-pipeline-data-c956dd9c/raw/year=2026/month=04/day=16/hour=22/1776377657_6b9ac8dd.parquet
```
**Hive-style partitioning** (`key=value` directories) means query engines can do **partition pruning** — the nightly job reads exactly one day's prefix instead of scanning the bucket. The filename is `{unix_epoch}_{uuid8}` — the **UUID suffix prevents collisions between concurrent pods** flushing in the same second.

### 15.4 Compression choice
**snappy** — the Parquet default; optimized for speed over ratio. Same reasoning as lz4 on the wire: CPU is scarcer than storage.

### 15.5 Lifecycle
Objects transition to **GLACIER after 30 days**. Access pattern justifies it: the batch job reads *yesterday*; data older than a month is essentially never read but must be retained.

---

## 16. DynamoDB — analytical store

### 16.1 Why DynamoDB, and why PAY_PER_REQUEST
Serverless, **zero idle cost**, IAM instance-role auth, single-digit-ms item access, and — critically — **atomic item-level updates** from many concurrent writers. PAY_PER_REQUEST removes capacity planning for a spiky workload; provisioned capacity would need to be sized for the 10× peak and wasted at baseline.

### 16.2 The `ADD` vs `PUT` decision — the key correctness insight

```python
update_expr = ("ADD total_events :n, total_sessions :s"
               " SET last_event_type = :lt, last_seen = :ls")
table.update_item(
    Key={"user_id": uid, "date": today},
    UpdateExpression=update_expr,
    ExpressionAttributeValues=expr_vals,
)
```

Up to 10 pods write concurrently, each seeing a *different subset* of events.
- **`PUT`** (or read-modify-write) → last writer wins → **lost updates**. Pod A reads 100, Pod B reads 100, both write 150 → final 150 instead of 200.
- **`ADD`** → an **atomic server-side increment**. Each pod submits only its *delta*; DynamoDB serializes them. No read, no race, no locking.

**The general principle to articulate:** *"Make concurrent writes commutative and you don't need coordination."* `ADD` is commutative and associative, so ordering is irrelevant.

Counters use `ADD`; last-seen fields use `SET` (last-writer-wins is semantically correct for "most recent"). Averages are `SET` per batch — an acknowledged approximation (last batch's average, not a global one); a true running mean would require storing sum and count and dividing on read.

### 16.3 Tables
| Table | PK | SK | Written by |
|---|---|---|---|
| `UserStats` | `user_id` | `date` | storage pods (stream) |
| `CourseStats` | `course_id` | `date` | storage pods (stream) |
| `DailyReports` | `report_date` | — | nightly batch |

The composite `(id, date)` key gives natural daily partitioning and cheap per-day queries.

### 16.4 Failure behavior
`update_item` failures are caught per-item and logged — one bad user doesn't abort the batch. PAY_PER_REQUEST auto-scales; the SDK retries with exponential backoff. Because writes are idempotent-ish (`ADD` of a delta), a retried batch could double-count — a real trade-off, mitigated in practice by upstream dedup.

---

## 17. The nightly batch job

### 17.1 Why batch at all (justifying Lambda architecture)
Daily aggregates — pass-rate distributions, hourly histograms, per-course rollups — need **the whole day's data at once**, have **no latency requirement**, and are **cheaper computed once** than maintained continuously. That's the textbook case for batch. Streaming everything would be more complex *and* more expensive for numbers nobody reads until tomorrow.

### 17.2 Reading S3 without hadoop-aws
```python
import pyarrow.dataset as ds
from pyarrow.fs import S3FileSystem
s3_fs   = S3FileSystem(region=AWS_REGION)
dataset = ds.dataset(f"{S3_BUCKET}/{prefix}", filesystem=s3_fs, format="parquet")
table   = dataset.to_table()
pdf     = table.to_pandas()
return spark.createDataFrame(pdf)
```
**Why not `spark.read.parquet("s3a://...")`?** That needs the `hadoop-aws` + `aws-java-sdk` JARs, version-matched to Spark — a classic dependency-hell problem. PyArrow's `S3FileSystem` picks up the **IAM instance role** directly. Honest trade-off: it materializes through pandas (single-node memory) — fine at 267k events (~67 MiB), would need the JARs at much larger scale.

**Interview framing:** *"I optimized for operational simplicity at known data volume, and I know exactly where that choice breaks."*

### 17.3 Outputs and hygiene
Writes **DynamoDB `DailyReports`** (one item for the day) plus a Parquet summary at `reports/daily/{date}/report.parquet`. `_to_dynamo()` strips **NaN/Inf** (DynamoDB cannot store them) and converts floats to `Decimal` — a small but real correctness detail that bites people in production.

**Measured:** 267,759 events / 125 files / 66.9 MiB in **~113 s mean** (118 s single run), against an 8-hour overnight window — ~250× headroom. `df.cache()` before the many per-event-type filters avoids re-reading for each.

### 17.4 Scheduling: cron vs Airflow
Runs via `cron` at midnight UTC. A **production-ready Airflow DAG is provided** (`dags/batch_reports_dag.py`: `start → verify_s3_data → run_batch_report → end`, `0 0 * * *`, 2 retries, 30-min timeout) but is not deployed.

**The justification:** Airflow needs a Postgres metadata DB + scheduler + webserver (>1 GB RAM) on a t3.medium already running Spark, Redis, k3s and Grafana. For **one** independent daily job, cron delivers the same outcome at zero overhead. **When to switch:** ≥3 interdependent jobs, or when you need backfills, retries with visibility, SLA alerts, and a dependency DAG UI.

---

## 18. Prometheus and Grafana — observability

### 18.1 The two-datasource design
Grafana reads from **both**, and the split is meaningful:

| Datasource | Serves | Example |
|---|---|---|
| **Redis** | Current KPI **values** | active users *right now*, dropout %, top-5 courses |
| **Prometheus** | **Time series** / rates / histograms | batch duration over time, lag trend, events/sec, S3 flush counts |

Mental model: **Redis = live snapshot; Prometheus = history and system health; Grafana = the single screen rendering both.**

### 18.2 Metrics exposed
Spark on `:8000`, pods on `:8001` (different ports so both can be scraped on one node):
- `pipeline_batch_duration_seconds` (Histogram — gives P95 for free)
- `pipeline_events_processed_total`, `pipeline_events_deduped_total` (Counters)
- `pipeline_kafka_consumer_lag`, `pipeline_surge_detected`, `pipeline_active_users_gauge`, `pipeline_dropout_rate_gauge`, `pipeline_avg_engagement_gauge`, `pipeline_s3_cost_reduction_pct` (Gauges)
- Labeled vectors: `pipeline_course_joins{course}`, `pipeline_device_events{device}`, `pipeline_region_users{region}`, `pipeline_event_type_count{event_type}`, `pipeline_avg_score_by_difficulty{difficulty}`, `pipeline_subscription_events{subscription}`
- Worker side: `worker_events_processed_total`, `worker_s3_flushes_total`, `worker_flush_duration_seconds`, `worker_kafka_lag`, `worker_buffer_size`, `worker_dynamo_writes_total`

**Counter vs Gauge vs Histogram** — know the distinction cold: a **Counter** only increases (use `rate()` to get per-second); a **Gauge** goes up and down (current value); a **Histogram** buckets observations so you can compute quantiles. Choosing correctly is what makes the dashboard meaningful.

### 18.3 EC2 service discovery — the detail that shows operational thinking
```yaml
- job_name: 'spark_pipeline_asg'
  ec2_sd_configs:
    - region: ap-south-1
      port: 8000
      filters:
        - { name: "tag:Project", values: ["edtech-pipeline"] }
        - { name: "tag:Name",    values: ["edtech-consumer-asg"] }
        - { name: "instance-state-name", values: ["running"] }
  relabel_configs:
    - source_labels: [__meta_ec2_private_ip]
      target_label: __address__
      replacement: '${1}:8000'
```
**Why this is necessary:** ASG instances are ephemeral — you cannot maintain a static target list for machines that don't exist yet. Prometheus queries the EC2 API and discovers them by tag, then relabels to the **private IP** so scraping stays inside the VPC. **Autoscaling infrastructure requires auto-discovering monitoring**; a static config would silently stop covering new nodes exactly when load is highest.

---

# PART IV — CROSS-CUTTING ANALYSIS

## 19. Life of an event — the complete trace

**T+0 ms — Generation (EC2-1).** The state machine picks an online user in `IN_CLASS` and emits `session_end` with `event_id`, `user_id`, `score`, `engagement_score`, `class_id`, plus derived `hour_of_day` / `is_weekend`.

**T+0–10 ms — Produce.** Serialized to JSON, keyed by `user_id`, lz4-compressed, buffered up to `linger_ms=10`, sent to `edtech-events`. The key hashes to one of 10 partitions — the *same* partition every time for this user, so their events stay ordered. Broker acknowledges (`acks=1`) and writes to disk with 24 h retention.

**Now the event is consumed twice, independently.**

### Path A — Monitoring (latency-critical)
**T+0–10 s — wait.** The event sits until the next 10 s micro-batch trigger. *This wait dominates end-to-end latency.*
**T+~10 s — batch.** Spark fetches (capped at 30,000 offsets), parses JSON against the 37-field schema, and `collect()`s the batch to the driver — one action.
**Dedup:** `SADD pipeline:dedup_event_ids <event_id>` → 1 (new) → kept.
**Surge check:** `current_rate` vs the 5-min rolling average; if > 1.5×, set `pipeline:surge_detected=1` and forward `class_join`/`session_start` to `edtech-priority`.
**KPIs:** ~15 metrics computed in pure Python; this `session_end` contributes to `active_users`, `avg_engagement_score`, `avg_score_by_difficulty`, `session_completion_rate`.
**Write:** Redis MSET with tiered TTLs; Prometheus gauges updated; checkpoint committed (0.5 s).
**T+~12 s — visible.** Grafana polls Redis; **GET takes 0.277 ms**. Worst-case event→dashboard ≈ **12.6 s** (10 s trigger + 2.6 s batch).

### Path B — Storage (throughput-critical)
**T+0–1 s — poll.** A storage pod polls its assigned partition(s) and appends the event to its in-memory buffer. Lag gauge updated.
**T+0–5 min — buffer.** The event waits for a flush trigger (≥5,000 events or 300 s).
**At flush:** the whole buffer becomes one PyArrow table (15 typed columns) → snappy-compressed Parquet → `PUT` to `s3://.../raw/year=/month=/day=/hour=/{epoch}_{uuid8}.parquet`. Then per-user and per-course aggregation, and `update_item` with **`ADD`** deltas into `UserStats` / `CourseStats`. Offsets auto-commit every 5 s.

### Path C — Batch (next midnight)
Cron fires `batch_reports.py`; PyArrow reads that day's prefix (partition pruning), converts to a Spark DataFrame, computes daily aggregates (pass rates by course and difficulty, hourly histogram, device split, DAU), writes `DailyReports` + a Parquet summary. ~113 s for ~268k events.

**T+30 days —** the Parquet object transitions to Glacier.

---

## 20. Fault tolerance — the complete model

### 20.1 The layered guarantee

```
Layer 0 — Kafka durability          Every event on disk, 24h retention. The universal safety net.
Layer 1 — Spark checkpoint          Offsets committed only after batch success → effectively-once.
Layer 2 — Consumer group offsets    Pods auto-commit every 5s; uncommitted work is re-delivered.
Layer 3 — SIGTERM drain             Buffer flushed before exit on planned termination → 0 loss.
Layer 4 — k8s liveness probes       Dead pod detected and restarted; resumes from last offset.
Layer 5 — systemd restarts          Spark/Kafka/Redis auto-restart on crash.
Layer 6 — ASG health checks         Unhealthy node terminated and replaced; Kafka rebalances.
```

**The core reasoning to state:** *"Every processing component is a stateless reader over a durable log. That means no component's failure can lose data — the worst case is re-processing from the last committed offset. All the fault-tolerance work is really about (a) committing offsets at the right moment and (b) draining in-memory buffers before a planned exit."*

### 20.2 What the guarantees actually are (be precise — this earns credibility)
- **Kafka delivery:** **at-least-once**
- **Spark processing:** **effectively-once** — checkpoint commits after success, so a crash replays the last uncommitted batch; the replay is safe because Redis writes are idempotent overwrites
- **DynamoDB:** **commutative** — `ADD` deltas, so concurrency is safe, though a replayed batch can double-count
- **S3:** **at-most-once per flush under graceful shutdown; potential duplicate object under SIGKILL** mid-flush
- **Overall:** **effectively-once at the storage layer for all planned shutdowns**; not true exactly-once

**When asked "is it exactly-once?"** — the strong answer: *"No, and I'd be suspicious of anyone claiming it without a transactional sink. It's at-least-once delivery plus idempotent/commutative writes, which gives effectively-once for every planned shutdown path. True EOS needs either Kafka transactions end-to-end or a transactional sink like Iceberg or Delta — which is exactly the upgrade I'd make."*

### 20.3 Complete failure matrix

| Failure | Detection | Recovery | Data loss |
|---|---|---|---|
| Generator stops | Lag drops to 0; no new offsets | Kafka retains; Spark resumes from checkpoint on restart | **None** (measured: 113 s outage, backlog of 2,219 drained, batch resumed **5 s** after restart) |
| Spark/EC2-2 crash | systemd exit code | Restart in ~15 s; read checkpoint (<1 s); resume at last offset | **None** |
| Kafka broker crash | `BrokerNotAvailableException` | Elastic IP means the address survives; systemd restarts Kafka; consumers reconnect next batch | **None** (buffered in Kafka) |
| Pod OOM-kill / crash | Liveness probe fails (3×30 s) | k3s restarts pod; re-reads from last committed offset | **None** (uncommitted re-delivered) |
| Pod SIGTERM (scale-in / rolling update) | SIGTERM handler | Drain buffer → S3 + DynamoDB → close consumer | **None** — measured **0 lost**, 4.7 s mean drain |
| Pod SIGKILL (no grace) | Group session timeout | Buffer lost in memory, but events uncommitted in Kafka → re-delivered | **None** (≤ ~3,600 events at risk, all recoverable) |
| S3 write failure | boto3 exception | Spark retries micro-batch; checkpoint not committed until success | **None** |
| DynamoDB failure/throttle | SDK exception | PAY_PER_REQUEST scales; SDK retries with backoff; per-item catch | **None** |
| Redis down | Grafana panels show N/A | systemd restart; Spark retries next batch | **None** — derived data only |
| ASG node fails | EC2 health check | ASG terminates + replaces; Kafka rebalances partitions in ~30 s | **None** |
| Surge overload | Lag rises past threshold | SurgeDetector routes overflow; KEDA adds pods | **None** — lag bounded at 3,091 |

**9/9 scenarios covered; measured uptime 99.9918% over a 17-hour run** (target ≥ 99.9%).

### 20.4 Graceful degradation tiers
The system sheds *features*, never data, in a fixed priority order:

| Tier | Trigger | What degrades | What's protected |
|---|---|---|---|
| 0 — Normal | < 1.5× baseline | Nothing | Everything |
| 1 — Surge | rate > 1.5× rolling avg | S3/DynamoDB freshness (pods lag briefly) | Live Redis KPIs; dashboard |
| 2 — Storage backpressure | KEDA at 10 pods, lag > 1,000/partition | Cold-storage writes delayed | Monitoring path fully isolated |
| 3 — Node pressure | EC2-2 CPU > 70% for 2 min | 4 pods Pending until ASG adds a node | 6 running pods continue; Spark unaffected |
| 4 — Broker unavailable | Kafka down | All consumption pauses; dashboard stale | Events retained 24 h; nothing lost |
| 5 — Redis unavailable | Redis crash | Grafana shows N/A | S3/DynamoDB writes continue |

**Three principles behind the ordering:** (1) the monitoring path is *never* sacrificed; (2) data loss is *never* a degradation option; (3) every tier is automatic — no human in the loop.

---

## 21. Optimization catalogue — every optimization and what it bought

| # | Optimization | Where | Mechanism | Measured effect |
|---|---|---|---|---|
| 1 | **Adaptive S3 batching** | `intelligence.py` | Buffer; flush ≥10k events OR 300 s | **−87.6% PUTs**, files 8 KB → **536 KB (67×)** |
| 2 | **Single `collect()` per batch** | `stream_consumer.py` | 21 Spark actions → 1; KPIs in Python | Batch time held at **2.0 s** under 10× load |
| 3 | **`shuffle.partitions` 200 → 4** | Spark config | Match parallelism to data size | Removes ~196 no-op tasks/shuffle |
| 4 | **Python pods instead of Spark pods** | `storage_worker.py` | Drop the JVM | **107 MB vs 1.5 GB → ~6× pod density** |
| 5 | **Lag-based scaling (KEDA)** | `hpa.yaml` | `ceil(lag/100)` on the true signal | **1→10 pods in 40 s**; CPU-HPA would never fire |
| 6 | **Scale-to-zero nodes** | ASG `min_size=0` | Remove idle workers | **$0 idle EC2 cost** |
| 7 | **Rebalance-timeout tuning** | `storage_worker.py` | `session=45 s`, `heartbeat=10 s` | Eliminated the cascading rebalance storm |
| 8 | **Producer batching + lz4** | `generator.py` | `linger_ms=10`, `batch_size=64 KB` | Higher throughput, less network |
| 9 | **Keying by `user_id`** | `generator.py` | Partition affinity | Per-user ordering; even distribution |
| 10 | **Tiered Redis TTLs** | `stream_consumer.py` | 120 / 360 / 900 s | Correct staleness semantics per metric |
| 11 | **Atomic DynamoDB `ADD`** | both writers | Commutative updates | Correct under 10 concurrent writers, no locking |
| 12 | **Hive partitioning + snappy** | S3 layout | `year=/month=/day=/hour=` | Partition pruning; batch reads 125 files not 8,640 |
| 13 | **Glacier after 30 days** | S3 lifecycle | Tiering | Long-term storage cost cut |
| 14 | **PAY_PER_REQUEST DynamoDB** | Terraform | Serverless billing | No idle cost, no capacity planning |
| 15 | **`maxOffsetsPerTrigger`** | Spark source | Back-pressure cap | Prevents unbounded batch after backlog |
| 16 | **Avoiding `spark.jars`** | Spark submit | JARs pre-placed on classpath | Avoids **331 MB /tmp copy** per start |
| 17 | **PyArrow S3 instead of s3a** | `batch_reports.py` | IAM-native reads | No hadoop-aws JAR dependency hell |
| 18 | **`df.cache()` in batch** | `batch_reports.py` | Cache before many filters | Avoids re-reading per event type |
| 19 | **Redis pipelining for dedup** | `stream_consumer.py` | Batch all `SADD`s | One round-trip instead of N |
| 20 | **Prometheus EC2 discovery** | `prometheus.yml` | Tag-based `ec2_sd_configs` | Monitoring auto-covers new ASG nodes |

**If asked "what's your single best optimization?"** — the adaptive batcher, because it's the one that came from an *architectural* insight (hot/cold paths have different freshness needs) rather than a knob. **If asked "most surprising?"** — the single-`collect()`, because the fix was to *stop* using the distributed engine.

---

## 22. Tuning — why every number is what it is

| Parameter | Value | Reasoning |
|---|---|---|
| Kafka partitions | **10** | Parallelism ceiling; matches target max pods; 300 ev/s ÷ 10 ≈ 30 ev/s/pod |
| Priority partitions | **3** | Small dedicated lane; overflow is a fraction of total |
| Retention | **24 h** | Long enough to survive an overnight outage and replay a full day |
| Trigger interval | **10 s** | Balance: shorter → more checkpoint overhead per event; longer → worse latency. Dominates the ~12 s end-to-end |
| `maxOffsetsPerTrigger` | **30,000** | 10× headroom over a 3,000-event 10× batch; caps memory after a backlog |
| `shuffle.partitions` | **4** (stream) / **2** (batch) | Data is MBs, not GBs; default 200 is pure scheduling overhead |
| Surge threshold | **1.5×** | 1.2× → false positives from normal variance; 2.5× → lag accumulates before detection. Measured 1.84× gives **22.7% margin** |
| Surge exit | **1.1×** | Hysteresis dead band prevents flapping |
| Surge hold | **6 batches (~60 s)** | Bursts dip momentarily; prevents 10 s state flipping |
| Rolling window | **300 s** | Long enough to smooth noise, short enough to track real trend |
| `FLUSH_EVENTS` | **10,000** (Spark) / **5,000** (pod) | Pods see a partition subset → halve to keep flush cadence comparable |
| `FLUSH_SECS` | **300** | Max acceptable cold-storage staleness; nothing reads S3 before midnight |
| `lagThreshold` | **100** | Events-behind per pod before adding another; small enough to react, large enough to ignore noise |
| `pollingInterval` | **15 s** | Reaction-time floor; gives ~20 s to first scale action |
| `cooldownPeriod` | **120 s** | Simultaneous partition flushes create momentary lag=0; don't scale in on that |
| `maxReplicaCount` | **10** | = partitions; an 11th pod gets no assignment |
| `minReplicaCount` | **1** | Keep group membership warm; no cold start |
| `session_timeout_ms` | **45,000** | Survive a full multi-consumer rebalance (default 10 s caused eviction storms) |
| `heartbeat_interval_ms` | **10,000** | **Must be < session/3** (10 < 15 ✓); tolerates two lost heartbeats |
| `max_poll_interval_ms` | **120,000** | A slow S3 flush must not look like a dead consumer |
| `terminationGracePeriodSeconds` | **60** | ~12× the measured 4.7 s drain |
| `initialDelaySeconds` | **90** | pip-install init container takes ~60 s; probing earlier causes crash loops |
| CPU request / limit | **250m / 500m** | 10 × 250m = 2,500m > 2,000m available → 4 Pending → the Tier-2 trigger |
| ASG CPU high | **70% for 2 min** | Sustained saturation, not a transient spike |
| ASG CPU low | **25% for 10 min** | Deliberately slow scale-in — releasing capacity early is riskier than paying a little longer |
| Scale-out cooldown | **180 s** | Longer than the ~90 s k3s agent boot |
| Redis TTLs | **120 / 360 / 900 s** | Encode how quickly each metric becomes wrong |
| Glacier transition | **30 days** | Batch reads yesterday; month-old data is effectively archival |

---

# PART V — FLAWS, GAPS, AND CRITIQUE

## 23. Known defects and inconsistencies

Volunteering these is a strength — it demonstrates you audit your own work.

### 23.1 Real bugs
1. **Dedup set grows unboundedly.** `r.expire(dedup_key, 3600)` runs every batch, resetting the TTL, so under continuous traffic the set *never* expires (~2.6 M members/day at 30 ev/s). It is not the "sliding window" the docs claim. **Fix:** hourly bucketed keys (`dedup:{YYYYMMDDHH}`) checking current + previous, or per-`event_id` `SETEX`, or a Bloom filter.
2. **Priority topic has no consumer.** The producer half of the QoS pattern ships; nothing subscribes to `edtech-priority`, so the isolation benefit is theoretical.
3. **`acks=1` weakens the durability claim** in a replicated setup (moot at RF=1, but it's the wrong default to carry into production alongside `retries=3` without an idempotent producer).

### 23.2 Config inconsistencies
4. **`consumer_max_size` defaults to `5`** in `variables.tf` while all documentation says the ASG maxes at 10. The docs describe intent; the code caps at 5.
5. **`consumer_min_size` is declared but unused** — `main.tf` hardcodes `min_size = 0`.
6. **Checkpoint lives in `/tmp`** — cleared on reboot, so a host restart forces `startingOffsets=latest` behavior. Should be an EBS-backed path.

### 23.3 Security gaps
7. **`0.0.0.0/0` ingress** on Kafka 9092/9093 and k3s API 6443 — should be SG-to-SG references or VPC CIDRs.
8. **No TLS/SASL on Kafka** — plaintext traffic and no client authentication.
9. **`kafka_key.pem` and an AWS access-key CSV sit in the working tree.** Correctly gitignored so never committed, but they should live in a secrets manager, and those keys should be rotated.

### 23.4 Structural limitations
10. **Single-broker Kafka** — SPOF, ~800 ev/s ceiling, no replication.
11. **Reactive-only scaling** — responds after lag builds, though exam surges are predictable.
12. **~12 s end-to-end latency floor** — inherent to a 10 s micro-batch trigger.
13. **Resource contention on one t3.medium** — Spark + Redis + Grafana + Prometheus + k3s + cron on 2 vCPU / 4 GB is what forced both the `STORAGE_ENABLED` split and the cron-over-Airflow decision.
14. **No schema registry** — JSON with a hand-rolled `schema_version` field.
15. **No CI/CD, no automated tests** — the intelligence classes are pure and trivially unit-testable, but aren't tested; load testing is a manual script.
16. **Batch job is single-node-bound** via the pandas conversion.

## 24. What I'd change, in priority order

1. **Multi-broker Kafka (RF=3, `min.insync.replicas=2`, `acks=all`, idempotent producer)** — removes the SPOF and the throughput ceiling in one move.
2. **Iceberg or Delta Lake** instead of raw Parquet — ACID commits give true exactly-once sinks, plus schema evolution, compaction and time-travel; structurally solves the small-file problem the batcher works around.
3. **Fix the dedup memory leak** (bucketed keys) and **deploy the priority consumer**.
4. **Real-time OLAP (ClickHouse / Druid / Pinot)** replacing the pre-computed Redis-KPI + DynamoDB serving layer — arbitrary slice-and-dice instead of only the KPIs Spark was told to compute.
5. **Flink** (or Spark Continuous Processing) if sub-second latency is a product requirement.
6. **Managed where undifferentiated:** MSK, EKS at ≥3 nodes, Amazon Managed Prometheus/Grafana, and **Karpenter** replacing the CloudWatch-CPU→ASG node scaler (Karpenter scales on *pending pods* — the actual signal — instead of a CPU proxy).
7. **Schema registry + Avro/Protobuf.**
8. **Security hardening:** SG-to-SG rules, TLS + SASL, secrets manager, least-privilege IAM.
9. **CI/CD + GitOps** — Terraform plan/apply gates, ArgoCD/Flux for manifests, a baked container image in ECR.
10. **Data quality + tests** — Great Expectations/Deequ, a DLQ for malformed events, unit tests for `SurgeDetector`/`AdaptiveBatchOptimizer`, automated load and chaos tests.
11. **Predictive scaling** — pre-warm before known exam windows.

---

# PART VI — ANSWER BANK AND REFERENCE

## 25. Question bank

### Architecture & rationale
**Why build two pipelines?** To compare managed vs self-managed on the same workload. P1 optimizes time-to-value and zero ops; P2 optimizes control — over cost, over the scaling signal, over stateful processing. Neither is universally better.

**Why was P2 needed?** Six reasons: Lambda's statelessness made stateful intelligence awkward; per-event/per-scan pricing bends the wrong way at volume; the SLA needed explicit buffering/replay/backpressure; the project needed to own the scaling *signal*; observability across managed services was painful; portability and depth.

**Which would you ship in production?** Depends on volume and team. Small team, spiky/low volume → P1 plus Managed Flink for stateful bits. High sustained volume, cost pressure, or portability requirements → P2's shape, but with MSK/EKS rather than fully self-managed.

**Why Lambda architecture (batch + stream)?** Different freshness requirements. Streaming serves second-level KPIs; batch computes richer daily aggregates once, cheaply, where latency doesn't matter.

**What's the single most important design decision?** Splitting the monitoring consumer from the storage consumer. It's what makes the dashboard immune to storage backpressure and lets each side scale on its own terms.

### Kafka
**Why Kafka over Kinesis?** Replay, partition parallelism, no lock-in, and — decisively — consumer-controlled offsets, which is what makes lag a usable scaling signal.
**Why 10 partitions?** Parallelism ceiling; matches max pods; ~30 ev/s per pod at the 10× target.
**Why key by `user_id`?** Ordering is guaranteed only within a partition; keying pins a user to one partition so their lifecycle stays ordered.
**What's KRaft?** Kafka's built-in Raft metadata quorum — no ZooKeeper. One less system to run.
**What happens if the broker dies?** Consumption pauses, dashboard goes stale, nothing is lost. Production fix: 3 brokers RF=3, so it becomes a leader election.
**How do you handle backpressure?** Kafka *is* the buffer; `maxOffsetsPerTrigger` caps batch size; KEDA adds consumers; SurgeDetector reroutes overflow.

### Spark
**Why Spark over Flink?** Unified batch+stream API, first-class Python, mature checkpointing, and the batch job reuses the same engine. Flink is better for sub-second and richer state — it's what I'd revisit.
**Micro-batch vs continuous?** Micro-batch trades latency for throughput and simpler exactly-once-ish semantics. My 10 s trigger is why worst-case latency is ~12 s.
**How does checkpointing work?** Offsets + query metadata written after each successful batch (~8.3 KB); on restart Spark reads them and resumes from the exact offset. 25% of batch time; <1 s to read.
**Why doesn't Spark show up in `kafka-consumer-groups.sh`?** It manages offsets in its checkpoint directory rather than registering a group — which also keeps KEDA's lag measurement clean.
**Biggest Spark optimization?** Collapsing 21 actions into one `collect()` and computing KPIs in Python — the data is only MBs, so distribution was pure overhead.

### Scaling
**KEDA vs HPA?** Not either/or — KEDA feeds a Kafka-lag metric into an HPA. CPU is the wrong signal because the workers are I/O-bound (<20% CPU at 50k lag).
**Explain the two tiers.** KEDA scales *pods* on lag in ~20–40 s; the ASG scales *EC2 nodes* on EC2-2 CPU in ~3–4 min when pods can't schedule. Pods first (cheap/fast), nodes only when necessary.
**Why cap at 10 pods?** Kafka assigns at most one consumer per partition per group; an 11th pod would idle.
**Why watch EC2-2's CPU and not the ASG's?** The ASG starts at 0 instances, so it has no CPU metric — the alarm would sit in INSUFFICIENT_DATA and never fire. The trigger must be observable before the resource exists.
**Why is scale-in slower than scale-out?** Asymmetric risk: adding late violates the SLA; removing late costs a few cents.
**What breaks at 100×?** The single broker (~800 ev/s) and the 10-partition cap. Fix both, and the consumer tier scales linearly.

### Fault tolerance
**How do you guarantee no data loss?** Kafka is durable for 24 h; Spark commits offsets only after success; pods drain on SIGTERM; a SIGKILL just leaves offsets uncommitted for re-delivery. Every path recovers from Kafka. 0 lost across 9 scenarios.
**Exactly-once?** No — at-least-once delivery plus idempotent/commutative writes = effectively-once for planned shutdowns. True EOS needs a transactional sink.
**What's the worst-case data at risk?** `max_poll_interval_ms` × rate ≈ 3,600 events, all replayable from Kafka.
**How does the system degrade?** Six tiers, sacrificing storage freshness before monitoring freshness, and never sacrificing data.
**What if two pods write the same user?** DynamoDB `ADD` is atomic and commutative — each submits a delta, no lost updates.
**Recovery time?** Spark resumed **5 s** after the generator restarted (Exp 4); pod restart ~90 s, dominated by the pip-install init container.

### Cost & optimization
**Where does the money go, and what did you do about it?** Dominant variable cost was S3 PUTs → adaptive batching cut them 87.6%. Idle EC2 → scale-to-zero ASG. DynamoDB → PAY_PER_REQUEST. Storage → Glacier at 30 days.
**Why is delaying S3 writes safe?** Nothing reads S3 until midnight. Freshness there is a free variable; Redis serves everything latency-sensitive.
**Cheapest big win?** One config idea — buffer and flush on dual thresholds. No new infrastructure.

### Data & modeling
**Why Parquet?** Columnar → better compression and column pruning; the analytics standard on S3.
**Why partition by hour?** Partition pruning — the batch job reads exactly one day's prefix.
**How do you handle duplicates?** `event_id` in a Redis Set, pipelined. (And disclose the TTL-reset bug.)
**Schema evolution?** Not handled — a `schema_version` field only. Production answer: Avro + Schema Registry.
**Why both Redis and DynamoDB?** Redis is an ephemeral cache of *derived* KPIs for the dashboard; DynamoDB is durable, queryable aggregate state. Different lifetimes, different consumers.

### Meta
**What was hardest?** The KEDA rebalance storm — a positive-feedback loop where scaling out caused lag, which caused more scaling. Fixing it meant understanding the consumer-group protocol, not just the autoscaler.
**What are you least happy with?** Single-broker Kafka, and shipping the priority topic without its consumer.
**What did you learn?** That the metric you scale on matters more than the autoscaler you choose, and that knowing when *not* to use a distributed engine is as valuable as knowing how to use one.

---

## 26. Reference

### Redis keys
| Key | Type | TTL | Meaning |
|---|---|---|---|
| `pipeline:active_users` | int | 120 s | Distinct users in last batch |
| `pipeline:events_per_minute` | int | 120 s | Batch count extrapolated to /min |
| `pipeline:online_users_by_region` | JSON | 120 s | Region → distinct logins |
| `pipeline:events_by_type` | JSON | 120 s | Event type → count |
| `pipeline:last_updated` | ISO ts | 120 s | Freshness marker |
| `pipeline:top_5_courses` | JSON | 360 s | Top classes by joins |
| `pipeline:avg_engagement_score` | float | 360 s | Mean engagement |
| `pipeline:avg_score_by_difficulty` | JSON | 360 s | Difficulty → mean score |
| `pipeline:dropout_rate` | float | 360 s | Voluntary leaves ÷ joins |
| `pipeline:premium_vs_free` | JSON | 360 s | Subscription split |
| `pipeline:device_breakdown` | JSON | 360 s | Device split |
| `pipeline:session_completion_rate` | float | 360 s | Completed ÷ started |
| `pipeline:active_classes_count` | int | 900 s | Distinct classes joined |
| `pipeline:avg_class_duration_sec` | float | 900 s | Mean class duration |
| `pipeline:mobile_vs_desktop_ratio` | float | 900 s | Device ratio |
| `pipeline:surge_detected` | 0/1 | 120 s | Surge flag |
| `pipeline:s3_cost_stats` | JSON | 3600 s | Baseline vs optimized PUTs, reduction % |
| `pipeline:dedup_event_ids` | Set | 3600 s* | Seen `event_id`s (*TTL resets each batch — see §23.1) |

### Ports
| Port | Service |
|---|---|
| 9092 | Kafka PLAINTEXT (internal) |
| 9093 | Kafka EXTERNAL |
| 6379 | Redis (VPC-only) |
| 8000 | Spark Prometheus metrics |
| 8001 | Storage-worker Prometheus metrics |
| 9090 | Prometheus UI |
| 3000 | Grafana |
| 6443 | k3s API server |
| 8472/udp | Flannel VXLAN overlay |
| 5050 | Local generator console |

### Key file map
| File | Role |
|---|---|
| `generator.py` | Stateful event generator + Kafka producer |
| `spark/stream_consumer.py` | Spark monitoring consumer → Redis + Prometheus |
| `spark/intelligence.py` | `SurgeDetector`, `AdaptiveBatchOptimizer` |
| `spark/storage_worker.py` | Python k8s pod consumer → S3 + DynamoDB |
| `spark/batch_reports.py` | Nightly aggregates → DailyReports |
| `dags/batch_reports_dag.py` | Airflow equivalent (provided, not deployed) |
| `k8s/*.yaml` | Namespace, ConfigMap, Deployment, KEDA ScaledObject, script CM |
| `terraform/main.tf` | S3, DynamoDB×3, IAM, SSM, EIP, ASG, alarms, SGs |
| `scripts/setup_k3s.sh` | Install k3s, store token in SSM, install KEDA, deploy manifests |
| `scripts/apply_eip.sh` | Propagate the Elastic IP into Kafka + all consumers |
| `scripts/test_10x_load.sh` | Switch generator between 30 and 300 ev/s |
| `prometheus/prometheus.yml` | Static + EC2-discovery scrape config |

### Glossary
**KRaft** — Kafka's ZooKeeper-free metadata mode. **Consumer group** — set of consumers sharing partitions of a topic; one partition per consumer max. **Consumer lag** — `latest offset − committed offset`; how far behind a group is. **Rebalance** — partition re-assignment triggered when group membership changes. **Checkpoint** — Spark's durable record of committed offsets/state. **Micro-batch** — Structured Streaming's unit of work. **Hysteresis** — using different enter/exit thresholds to prevent state flapping. **KEDA** — event-driven autoscaler feeding external metrics to an HPA. **HPA** — Kubernetes Horizontal Pod Autoscaler. **ScaledObject** — KEDA CRD defining a scaling trigger. **k3s** — lightweight Kubernetes distribution. **Parquet** — columnar file format. **Hive partitioning** — `key=value` directory layout enabling partition pruning. **PAY_PER_REQUEST** — DynamoDB on-demand billing. **Effectively-once** — at-least-once delivery + idempotent writes yielding once-like outcomes.
