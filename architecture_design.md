# Architecture & Design Document
## CSG527 — EdTech Real-Time Analytics Pipeline
### BITS Pilani Hyderabad | April 2026

---

## 1. Problem Statement (Plain English)

An online learning platform with 500 students enrolled across 10 courses generates
a continuous stream of activity events — logins, course joins, quiz submissions, and
logouts. Without real-time visibility, platform operators cannot detect struggling
students, surging traffic during exams, or dropping engagement rates.

**What data arrives**: JSON events (login, logout, class_join, class_leave,
session_start, session_end) with user_id, course_id, device_type, region,
engagement score (0–1), quiz score (0–100).

**How fast**: Bursty — steady 30 events/second with spikes up to 3× during
peak periods. Kafka buffers these so no events are lost during spike.

**What insights are needed**:
- Real-time: active users, events/min, current surge status, session completion rate
- Per-course: join counts, dropout %, avg quiz score by difficulty level
- Per-user: daily engagement, total sessions, last active
- Historical: nightly daily aggregates for trend analysis

**What happens if slow/failed**:
- Dashboard goes stale → instructors can't see live student activity
- Consumer lag builds up → processing falls behind → SLA violated
- If Kafka goes down → generator events lost (no buffer on producer side)

**Success Metrics**:
- Dashboard KPI refresh latency < 5 seconds (Redis→Grafana path; Redis GET mean 0.277ms measured;
  Spark micro-batch mean 2.0s on t3.medium — see §10 for end-to-end latency analysis)
- Consumer lag stays bounded during 3× traffic spike with SurgeDetector
- S3 storage cost reduced by ≥ 50% via adaptive batching
- Pipeline auto-recovers within 60 seconds of Spark restart

---

## 2. Architecture Diagram

```
EC2-1: t2.medium — ap-south-1 (Kafka node, Elastic IP 3.7.163.17, Terraform-managed)
├── generator.py             → produces 30 ev/s (--rate 30, --users 500)
├── Apache Kafka 4.1.1 (KRaft, /opt/kafka/)
├── Topic: edtech-events     (10 partitions, primary stream, 24h retention)
└── Topic: edtech-priority   (3 partitions, surge overflow)
         │
         │ port 9093 (EXTERNAL listener)
         ├─────────────────────────────────────────────────────────────────┐
         │ Spark checkpoint-based offsets (no Kafka consumer group)        │ Consumer Group: edtech-storage-group
         ▼                                                                 ▼
EC2-2: t3.medium (Elastic IP 52.66.142.159, 20 GB EBS)        k3s Pods: edtech-storage-worker
├── Spark Structured Streaming (stream_consumer.py)             ├── 1–10 replicas (KEDA ScaledObject)
│    STORAGE_ENABLED=false (monitoring role only)               ├── image: python:3.11-slim (~250 MB RAM/pod)
│    ├── SurgeDetector → edtech-priority topic                 ├── init container: pip install deps
│    ├── Redis 7 → 16 live pipeline:* keys                     ├── → S3 Parquet (flush ≥5000 events / 5 min)
│    └── Prometheus metrics :8000                              ├── → DynamoDB UserStats + CourseStats
│                                                               └── → Prometheus metrics :8001
├── batch_reports.py (cron midnight UTC)
│    ├── Reads S3 Parquet                                       KEDA ScaledObject:
│    └── Writes DynamoDB DailyReports                          ├── pollingInterval: 15s
│                                                               ├── lagThreshold: 100 events/pod
├── k3s server (control plane)                                 ├── minReplicas: 1
│    └── schedules storage-worker pods                         └── maxReplicas: 10 (= partitions)
│
└── Prometheus + Grafana (Docker Compose, :3000)               ASG: edtech-consumer-asg
     ├── 20-panel EdTech Pipeline dashboard                    ├── 0–10 t3.medium EC2 instances
     └── Datasources: Prometheus + Redis                       ├── k3s agent nodes (join EC2-2 cluster)
                                                                ├── Trigger: EC2-2 CPU > 70% for 2 min
         └── AWS Managed Services                              └── Cooldown: 180s (k3s boot ~90s)
              ├── S3 (edtech-pipeline-data-c956dd9c)
              ├── DynamoDB (UserStats, CourseStats, DailyReports)
              ├── IAM role (edtech-ec2-2-role, instance profile)
              └── SSM (/edtech/k3s-token, SecureString)
```

---

## 3. Technology Choices and Justification

| Layer | Chosen | Alternatives Considered | Why This |
|-------|--------|------------------------|----------|
| Message broker | Apache Kafka 4.1.1 KRaft | Kinesis, RabbitMQ | Replay capability, partition-based parallelism, no ZooKeeper needed in KRaft mode |
| Stream processing | PySpark Structured Streaming | Apache Flink, Beam | Unified batch+stream API, Python support, checkpoint-based exactly-once |
| Hot storage | Redis 7 | Memcached, ElastiCache | Direct Grafana datasource plugin, sub-millisecond reads, simple hash key model |
| Cold storage | S3 + Parquet | EFS, Redshift | Serverless, columnar compression, Spark-native format, cheapest per GB |
| Analytical store | DynamoDB PAY_PER_REQUEST | RDS, Aurora | Serverless, no idle cost, IAM instance role auth, item-level updates from Spark |
| Observability | Prometheus + Grafana | CloudWatch, Datadog | Free, Python metrics endpoint, Redis + Prometheus combined in one dashboard |
| Infrastructure | Terraform | CDK, CloudFormation | Multi-cloud compatible, declarative, 30 resources in one plan |
| Monitoring deployment | systemd services | Docker Swarm | Sufficient for 2-node cluster, auto-restart, boot-time start |
| Container orchestration | k3s + KEDA v2.13.1 | EKS, Nomad | k3s: full k8s API with zero control-plane overhead on t3.medium. KEDA: event-driven pod scaling from Kafka lag without custom metrics pipeline |
| Pod auto-scaler | KEDA ScaledObject (Kafka trigger) | k8s HPA (CPU-based) | CPU does not reflect Kafka lag; KEDA directly measures consumer group lag and computes ceil(totalLag / lagThreshold) for precise scaling |
| Storage consumer | storage_worker.py (Python) | PySpark pods | Python consumer: ~250 MB RAM/pod vs 1.5 GB JVM → 6× more pods per node → higher throughput ceiling |

---

## 4. Batch vs Stream Decision

| Use Case | Processing Type | Reason |
|----------|----------------|--------|
| Live active users, events/min, surge detection | Stream (10s micro-batches) | Redis KPI refresh < 5ms to Grafana; Spark micro-batch mean 2.0s, P95 2.47s on t3.medium (Exp 8) |
| Per-user daily aggregates (UserStats, CourseStats) | Stream | Updated continuously so Grafana can show live per-user data |
| Daily report aggregates (pass rates, distributions) | Batch (midnight cron) | Only needed once per day, richer computation, no latency requirement |
| S3 data lake writes | Stream with adaptive batching | Optimized via AdaptiveBatchOptimizer to balance freshness and cost |

**Architecture type**: Lambda Architecture — both stream (live path) and batch (reporting path) write to DynamoDB, with different tables for different granularities.

---

## 5. Intelligence Components

### AdaptiveBatchOptimizer (Section 7.1 — Cost–Latency Optimization)
**Problem**: Naive Spark Structured Streaming writes one S3 file per micro-batch (every 10s).
At 30 events/s = 300 events per batch → tiny 5–10 KB files → 6 PUTs/min = 8,640/day.

**Solution**: Buffer events in memory. Flush only when:
- ≥ 10,000 events buffered (throughput trigger), OR
- ≥ 5 minutes elapsed since last flush (latency bound)

**Measured Result**: 87.6% fewer S3 PUT requests (28 actual vs 225 baseline) → larger
Parquet files (~500 KB each) → better columnar compression + lower API cost.

### SurgeDetector (Section 7.3 — SLA-Aware Data Processing)
**Problem**: Exam periods cause 3× traffic spikes. Single consumer cannot keep up.
Consumer lag builds → dashboard becomes stale → SLA violated.

**Solution**: Rolling 5-minute event-rate baseline. If current batch rate > 1.5× rolling_avg:
- Set `pipeline:surge_detected = 1` in Redis
- Route new events to `edtech-priority` topic (3 dedicated partitions)
- Hold surge state for 6 batches minimum to prevent oscillation

**Measured Result**: Surge detected at current_rate=229.1 ev/s vs rolling_avg=124.8 ev/s
(ratio 1.84×), with 1,200 events forwarded to edtech-priority topic in a single batch.

---

## 6. Failure Handling (Implemented)

| Failure | How Detected | Recovery |
|---------|-------------|----------|
| Generator stops | Kafka consumer sees no new messages; lag drops to 0 | Kafka retains events for 7 days. When generator restarts, Spark resumes from checkpoint offset — no data lost |
| EC2-2 (Spark) crashes | systemd exit code → auto-restarts within 15s | Spark reads `/tmp/edtech-checkpoints/stream` → resumes from last committed offset. Effectively-once semantics |
| EC2-1 (Kafka) crashes | Spark throws BrokerNotAvailableException | EC2-1 has a Terraform-managed Elastic IP — the IP never changes on restart. Kafka auto-restarts via systemd. Spark reconnects automatically on next micro-batch. |
| S3 write failure | Spark batch throws exception | Spark retries the micro-batch. Checkpoint not committed until write succeeds |
| DynamoDB write failure | AWS SDK exception | PAY_PER_REQUEST auto-scales. SDK retries with exponential backoff |
| Redis down | Grafana panels show "N/A" | Redis systemd service auto-restarts. Spark reconnects on next micro-batch |
| Surge overload | Consumer lag rises above threshold | SurgeDetector routes overflow to priority topic — lag suppressed |
| ASG instance fails | EC2 health check fails | ASG terminates the unhealthy instance and boots a replacement. Kafka rebalances its partitions to remaining consumers within 30s |
| k3s pod crashes on EC2-2 | Kubernetes liveness probe fails | k3s restarts the pod automatically. Spark reads from per-pod checkpoint directory and resumes from last committed Kafka offset |

---

## 7. Graceful Degradation Strategy (§7.3 + §7.4)

The pipeline degrades gracefully across three tiers. Each tier sacrifices a lower-priority
feature to protect a higher-priority one:

| Degradation Tier | Trigger | What Degrades | What Is Protected |
|---|---|---|---|
| **Tier 0 — Normal** | < 1.5× baseline rate | Nothing | All features active |
| **Tier 1 — Surge Mode** | rate > 1.5× rolling_avg | S3/DynamoDB freshness (storage pods lag briefly) | Live Redis KPIs < 5ms; dashboard unaffected |
| **Tier 2 — Storage Backpressure** | KEDA at max (10 pods); lag > 1,000/partition | Cold storage writes delayed; DynamoDB batch may queue | Monitoring path (Spark → Redis) isolated; no monitoring lag |
| **Tier 3 — Node Pressure** | EC2-2 CPU > 70% for 2 min | 4 storage pods Pending (can't schedule) | 6 Running pods continue consuming; Spark monitoring unaffected |
| **Tier 4 — Broker Unavailable** | EC2-1 Kafka crashes | All consumption pauses; dashboard goes stale | Events buffered in Kafka (24h retention); no data lost |
| **Tier 5 — Redis Unavailable** | Redis process crash | Grafana panels show "N/A" | S3/DynamoDB writes unaffected; stream_consumer retries Redis next batch |

**Design principles:**
1. **Monitoring path is never sacrificed** — Spark → Redis is always the highest priority path.
   Storage pods (k8s) are second priority. Batch job (cron) is third.
2. **Data loss is never a degradation option** — all tiers preserve every event
   (either buffered in Kafka, in-memory, or already written to S3/DynamoDB).
3. **Degradation is automatic** — no human intervention required for any tier.

---

## 8. Kubernetes Deployment (EC2-2) — KEDA-based Auto-scaling

k3s (lightweight Kubernetes) runs on EC2-2 as the control plane. The `edtech-storage-worker`
Deployment runs Python-based storage consumer pods, scaled by KEDA based on Kafka lag.

**Consumer role split:**

| Consumer | Offset Management | Reads | Writes | Scaling |
|----------|------------------|-------|--------|---------|
| `stream_consumer.py` (Spark) | Checkpoint-based (`/tmp/edtech-checkpoints/stream/`) | edtech-events | Redis + Prometheus | Fixed (1 systemd service) |
| `storage_worker.py` (k8s pods) | Kafka consumer group `edtech-storage-group` | edtech-events | S3 + DynamoDB | KEDA: 1–10 pods on Kafka lag |

Both paths receive ALL events independently. Spark manages its own offsets via the checkpoint
directory — it does not register a traditional Kafka consumer group visible via
`kafka-consumer-groups.sh`. Only `edtech-storage-group` appears in Kafka broker metadata.
`STORAGE_ENABLED=false` on the Spark master ensures zero duplicate S3/DynamoDB writes.

**k8s components (`k8s/`):**
- `namespace.yaml` — `edtech-pipeline` namespace with project labels
- `configmap.yaml` — Kafka address, S3 bucket, flush thresholds, consumer group ID
- `worker-script-cm.yaml` — `storage_worker.py` mounted as ConfigMap file (no Docker build needed)
- `deployment.yaml` — `edtech-storage-worker` Deployment:
  - Init container: `pip install kafka-python boto3 pyarrow prometheus_client lz4`
  - Resources: requests 256Mi / 250m, limits 512Mi / 500m per pod
  - Liveness + readiness probes: `GET /metrics:8001`, `initialDelaySeconds: 90`
  - `terminationGracePeriodSeconds: 60` — buffer drains to S3 before pod exits
- `hpa.yaml` — KEDA `ScaledObject`:

```yaml
triggers:
  - type: kafka
    metadata:
      bootstrapServers: "3.7.163.17:9093"
      consumerGroup: "edtech-storage-group"
      topic: "edtech-events"
      lagThreshold: "100"        # desiredPods = ceil(totalLag / 100)
      offsetResetPolicy: "latest"
minReplicaCount: 1
maxReplicaCount: 10              # = number of Kafka partitions
pollingInterval:  15             # seconds
cooldownPeriod:   120            # seconds after lag=0 before scale-in
```

**Measured scaling performance (10× load test, 2026-04-16):**
- 1 pod → 5 pods: **20 seconds** (first KEDA poll after lag > threshold)
- 5 pods → 10 pods: **40 seconds total** (second poll cycle)
- Kafka lag at steady state: **5–20 events per partition** (effectively zero)
- Pod RAM footprint: **~107 MB actual** (vs 1.5 GB for Spark JVM)

**Race condition mitigation (Kafka group rebalance during scale-out):**
Rapid pod additions (1→10) cause consecutive Kafka group rebalances. With the default
10s `session_timeout_ms`, pods can be evicted mid-rebalance, causing a cascading storm.
Fixed by setting in `storage_worker.py`:
- `session_timeout_ms = 45000` (pods survive through multi-consumer rebalance cycles)
- `heartbeat_interval_ms = 10000` (maintain group membership; must be < session_timeout / 3)
- `max_poll_interval_ms = 120000` (prevent eviction during S3 flush)

---

## 9. Two-Tier Horizontal Auto-scaling

The pipeline uses two independent auto-scaling tiers that operate at different timescales:

```
Tier 1 — KEDA Pod Scaling (fast, ~20–40 seconds)
  Trigger: Kafka consumer group lag > lagThreshold × replicaCount
  Action:  Add/remove storage_worker pods within the same k3s cluster
  Ceiling: 10 pods = 10 Kafka partitions (hard limit from Kafka protocol)

Tier 2 — CloudWatch→ASG Node Scaling (slow, ~3–4 minutes)
  Trigger: EC2-2 CPU > 70% for 2 consecutive minutes (Tier 1 has maxed pods)
  Action:  Launch new t3.medium, install k3s agent, join cluster
  Effect:  Pending pods (CPU-limited on EC2-2) schedule on new node → all 10 Running
  Scale-in: ASG CPU < 25% for 10 min → remove 1 node per 5-min cooldown
```

**Partition-to-pod mapping at different scales:**

```
Load    Rate      Tier 1 Pods   Tier 2 Nodes   Partitions/Pod   Throughput
──────  ────────  ───────────   ────────────   ─────────────    ──────────
1×      30 ev/s   1             0              10 (all)         ~30 ev/s
3×      90 ev/s   3             0              ~3.3 each        ~90 ev/s
10×     300 ev/s  10            0→1            1 each           ~300 ev/s
100×    3000 ev/s 10            +N             1 each           ~3000 ev/s*
```
*At 100×, multiple ASG nodes are needed to schedule all 10 pods. Kafka broker itself
saturates at ~800 ev/s on a single t2.medium; partitions would need to be increased too.

**AWS resources (Terraform-managed):**
- `aws_autoscaling_group.consumer`: min=0, max=10, desired=0 (alarm-driven)
- `aws_cloudwatch_metric_alarm.high_cpu`: watches EC2-2 `InstanceId` (not ASG avg —
  avoids the "no data when ASG=0" problem)
- `aws_cloudwatch_metric_alarm.low_cpu`: `treat_missing_data=notBreaching` (silent when ASG=0)
- `aws_launch_template.consumer`: k3s agent userdata — reads SSM token, discovers
  EC2-2 private IP via tag, installs k3s agent and joins cluster
- `aws_ssm_parameter.k3s_token`: `/edtech/k3s-token` SecureString — join token
  stored by `setup_k3s.sh`, read by ASG instance at boot

---

## 10. Known Trade-offs and Gap Justifications

**EC2 + k3s instead of EKS**:
EC2-2 is a t3.medium (2 vCPU, 4 GB RAM). EKS control plane requires a dedicated master
node (~$0.10/hour) plus cluster DNS and CNI overhead. k3s provides identical pod
lifecycle management, HPA, and health checks at zero overhead, which is appropriate
for a 2-node student deployment. EKS becomes appropriate at ≥3 worker nodes with
m5.large or larger instances.

**Airflow DAG provided (`dags/batch_reports_dag.py`)**:
A production-ready Airflow DAG is provided at `dags/batch_reports_dag.py`. It defines
the `edtech_nightly_batch` DAG: `start → verify_s3_data → run_batch_report → end`,
scheduled at `0 0 * * *` (midnight UTC), with 2 retries and a 30-minute timeout.
The current deployment uses cron as a lightweight equivalent — Airflow requires a
PostgreSQL metadata store + scheduler + web server (>1 GB RAM) which exceeds the
t3.medium budget shared with Spark, Redis, and k3s. The DAG is the drop-in replacement
for environments with ≥3 interdependent jobs or when a visual workflow UI is needed.

**No schema evolution**:
Schema is stable (6 fixed event types, fixed fields). In production with third-party sources,
Apache Avro with Confluent Schema Registry would handle backward/forward compatibility.

**Event deduplication (implemented)**:
`stream_consumer.py` deduplicates by `event_id` using a Redis Set with a 1-hour sliding
window (`pipeline:dedup_event_ids`). Each batch pipelines `SADD event_id` calls — Redis
returns 1 for new IDs and 0 for duplicates seen within the last hour. Events without an
`event_id` pass through unfiltered. Duplicate count is tracked by the
`pipeline_events_deduped_total` Prometheus counter. The 1-hour window catches generator
retries and Kafka consumer rebalance re-deliveries without false-positives on legitimate
repeated actions (e.g., a user logging in twice in one day, which carry distinct event_ids).

**Micro-batch latency vs <5s SLA**:
On t3.medium (2 vCPU, 4 GB RAM), Spark batches complete in **mean 2.0s, P95 2.47s** (30
samples measured). With `STORAGE_ENABLED=false`, Spark does only Redis writes and
intelligence logic — no DynamoDB or S3 writes. Redis serves real-time dashboard data
at mean 0.277ms. End-to-end event-to-dashboard worst case is ~12.6s (10s trigger wait
+ 2.6s batch), bounded by the micro-batch trigger interval — see Experiment 8.
