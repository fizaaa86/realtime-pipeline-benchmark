# EdTech Real-Time Analytics Pipeline

CSG527 Cloud Computing — Course Project | Group 7
BITS Pilani Hyderabad | April 2026

---

## Overview

A production-grade, event-driven analytics pipeline for an ed-tech platform. It ingests
simulated student activity events, processes them in real-time and in batch, surfaces live
metrics on a Grafana dashboard, and auto-scales horizontally to handle 10×–100× load spikes
using a two-tier architecture: KEDA pod scaling (seconds) + AWS ASG node scaling (minutes).

```
EC2-1 (t2.medium) — Kafka node  [Elastic IP: 3.7.163.17]
┌──────────────────────────────────┐
│ generator.py  30 ev/s / 500 users│
│ Apache Kafka 4.1.1 KRaft         │
│  • edtech-events    (10 parts)   │
│  • edtech-priority  (3 parts)    │
└────────────┬─────────────────────┘
             │ port 9093
     ┌───────┴──────────────────────────────────────────────┐
     │ Consumer Group A          │ Consumer Group B          │
     │ edtech-monitoring-group   │ edtech-storage-group      │
     ▼                           ▼
EC2-2 (t3.medium) [52.66.142.159, 20 GB EBS]
┌────────────────────────────────┐  ┌─────────────────────────────────────┐
│ stream_consumer.py (Spark)     │  │ k3s — edtech-storage-worker pods    │
│  STORAGE_ENABLED=false         │  │  • 1–10 replicas (KEDA ScaledObject)│
│  ├─ SurgeDetector              │  │  • image: python:3.11-slim (~250 MB)│
│  ├─ Redis 7 (16 KPI keys)      │  │  • → S3 Parquet (≥5k events/5 min) │
│  └─ Prometheus :8000           │  │  • → DynamoDB UserStats+CourseStats │
│                                │  │  • → Prometheus :8001               │
│ batch_reports.py (cron 00:00)  │  │                                     │
│  S3 Parquet → DailyReports     │  │ KEDA: ceil(totalLag/100) pods       │
│                                │  │ pollingInterval: 15s                │
│ Grafana + Prometheus (Docker)  │  │ maxReplicas: 10 (= Kafka partitions)│
│  :3000  20 panels              │  └─────────────────────────────────────┘
│                                │
│ k3s control plane              │  ← Tier 2: CloudWatch alarm fires when
└────────────────────────────────┘    EC2-2 CPU > 70% for 2 min
             │                        → ASG launches k3s agent node
┌────────────▼────────────────────────────────────────────────────┐
│ AWS ASG  edtech-consumer-asg   (0–10 × t3.medium k3s agents)   │
│  Userdata: read SSM k3s token → join EC2-2 cluster → run pods   │
└─────────────────────────────────────────────────────────────────┘
             │
┌────────────▼────────────────────┐
│ AWS Managed Services             │
│  S3 · DynamoDB · IAM · SSM      │
└──────────────────────────────────┘
```

---

## Architecture

### Event Flow

1. **Generator** (`generator.py`) — simulates 500 students across 10 courses, producing 30 ev/s to Kafka `edtech-events` (10 partitions). Spike mode: 300 ev/s for 10× load testing.
2. **SurgeDetector** (`spark/intelligence.py`) — detects spikes >1.5× rolling 5-min avg; forwards overflow to `edtech-priority` (3 partitions). Measured: detection in < 40s.
3. **AdaptiveBatchOptimizer** — buffers raw events; flushes to S3 Parquet when ≥5,000 events OR 5 minutes elapse. Measured: **87.6% S3 cost reduction**.
4. **Spark consumer** (`spark/stream_consumer.py`) — 10s micro-batches, `STORAGE_ENABLED=false`. Writes only to Redis (live KPIs) + Prometheus metrics. DynamoDB/S3 handled by k8s pods.
5. **storage_worker pods** (`spark/storage_worker.py`) — KEDA-scaled 1→10 pods. Each pod is a lightweight Python Kafka consumer (~250 MB RAM, no JVM). Writes S3 Parquet + DynamoDB.
6. **KEDA ScaledObject** — scales pods every 15s based on `edtech-storage-group` lag. Formula: `ceil(totalLag / 100)`. Measured: **1→10 pods in 40 seconds** at 300 ev/s.
7. **AWS ASG** — Tier 2 scaling. When EC2-2 CPU > 70%, boots new t3.medium k3s agent nodes. Pending pods schedule on new nodes. Scale-to-zero at baseline (0 idle EC2 cost).
8. **Batch job** (`spark/batch_reports.py`) — cron midnight UTC; reads S3 Parquet, writes DynamoDB DailyReports. Measured: 118 seconds for 267,759 events.

### Technology Stack

| Layer | Technology | Host |
|---|---|---|
| Message broker | Apache Kafka 4.1.1 KRaft (no ZooKeeper) | EC2-1 (t2.medium) |
| Stream processor | PySpark 3.5 / Spark Structured Streaming | EC2-2 (t3.medium) |
| Container orchestration | k3s v1.31 (lightweight Kubernetes) | EC2-2 |
| Pod auto-scaler | KEDA v2.13.1 (Kafka lag trigger) | k3s on EC2-2 |
| Storage consumer | storage_worker.py (Python, kafka-python + pyarrow) | k3s pods |
| EC2 node auto-scaler | AWS ASG + CloudWatch (EC2-2 CPU trigger) | AWS managed |
| Live metrics store | Redis 7 (systemd service) | EC2-2 |
| Dashboard | Grafana 10.0.0 + Prometheus (Docker Compose) | EC2-2, port 3000 |
| Data lake | S3 Parquet, partitioned by year/month/day/hour | AWS managed |
| Analytical store | DynamoDB PAY_PER_REQUEST | AWS managed |
| Config/secrets | AWS SSM Parameter Store (`/edtech/k3s-token`) | AWS managed |
| Infrastructure | Terraform 1.5+ (30 resources) | — |

---

## AWS Resources (Terraform-managed)

| Resource | Name / ID |
|---|---|
| S3 bucket | `edtech-pipeline-data-c956dd9c` |
| DynamoDB — user stats | `UserStats` (PK: `user_id`, SK: `date`) |
| DynamoDB — course stats | `CourseStats` (PK: `course_id`, SK: `date`) |
| DynamoDB — daily reports | `DailyReports` (PK: `report_date`) |
| IAM role (EC2-2) | `edtech-ec2-2-role` — S3 + DynamoDB + ec2:DescribeInstances |
| EC2-1 | `aws_eip.ec2_1` — Kafka node (t2.medium, ap-south-1, **Elastic IP assigned by Terraform**) |
| EC2-2 | `52.66.142.159` — Spark + Redis node (Elastic IP, t3.medium) |
| ASG | `edtech-consumer-asg` — consumer fleet (0–10 × t3.medium, CPU-alarm driven) |
| Launch Template | `edtech-consumer-*` — auto-configures new consumer instances |
| CloudWatch Alarms | `edtech-consumer-high-cpu`, `edtech-consumer-low-cpu` |

---

## Repository Layout

```
edtech-pipeline-2/
├── generator.py                  # Event producer (EC2-1 → Kafka)
├── console_server.py             # Flask control UI server
├── console.html                  # Browser console UI
├── requirements.txt              # Python deps (flask, kafka-python)
├── Dockerfile                    # Container image for stream_consumer
├── docker-compose.monitoring.yml # Prometheus + Grafana monitoring stack
│
├── spark/
│   ├── stream_consumer.py        # Spark Structured Streaming consumer
│   ├── batch_reports.py          # Nightly batch aggregation job
│   └── intelligence.py          # SurgeDetector + AdaptiveBatchOptimizer
│
├── k8s/                          # Kubernetes manifests (k3s on EC2-2)
│   ├── namespace.yaml            # edtech-pipeline namespace
│   ├── configmap.yaml            # All env config for storage-worker pods
│   ├── worker-script-cm.yaml     # storage_worker.py mounted as ConfigMap file
│   ├── deployment.yaml           # edtech-storage-worker Deployment (python:3.11-slim)
│   └── hpa.yaml                  # KEDA ScaledObject (1–10 pods, Kafka lag trigger)
│
├── scripts/
│   ├── apply_eip.sh              # Run ONCE after terraform apply — wires EIP into all services
│   ├── package_consumer.sh       # Package code → S3 artifact (for ASG)
│   ├── setup_k3s.sh              # One-shot k3s + KEDA install and manifest deploy on EC2-2
│   └── test_10x_load.sh          # Switch generator to 300 ev/s for load testing
│
├── terraform/
│   ├── main.tf                   # All AWS resources incl. ASG + alarms
│   ├── variables.tf              # EC2 IPs, region, ASG sizing
│   └── outputs.tf
│
├── grafana/
│   ├── dashboards/               # 20-panel dashboard JSON
│   └── provisioning/             # Prometheus + Redis datasource config
│
├── prometheus/
│   └── prometheus.yml            # Static scrape + EC2 service discovery
│
├── CHANGES_LOG.md                # Full change history (Phase 1–3)
│
└── docs/
    ├── architecture_design.md    # Full architecture + trade-off analysis
    ├── benchmarking_report.md    # Experiment results (7 experiments)
    ├── research_analysis.md      # Research sections §7.1 + §7.2 + §7.3
    ├── intelligence_layer.md     # Deep-dive: 4 intelligence components (§7.1–§7.4)
    ├── demo_slides.md            # Marp presentation slides (15 slides)
    ├── README_deliverables.md    # Deliverables checklist + key numbers
    └── experiment_results.md     # Raw captured metrics
```

---

## Quick Start

### Prerequisites

- AWS CLI configured with credentials (`~/.aws/credentials`)
- SSH key at `kafka_key.pem` (in project root)
- Python 3.10+ with `flask` and `kafka-python` (`pip install -r requirements.txt`)
- Terraform ≥ 1.5 (for infrastructure changes only)

### 1. Provision Infrastructure (first time only)

```bash
cd terraform
terraform init
terraform apply
```

After `terraform apply`, run the one-time setup script to wire the new EC2-1 Elastic IP
into Kafka, Spark, and k3s:

```bash
# From project root — run once after every fresh terraform apply
./scripts/apply_eip.sh

# Check the assigned EIP anytime:
cd terraform && terraform output ec2_1_eip
```

`apply_eip.sh` does all of this automatically:
1. Gets EC2-1 EIP from `terraform output`
2. SSHes to EC2-1 → updates Kafka `advertised.listeners` → restarts Kafka
3. SSHes to EC2-2 → updates `/etc/edtech-spark.env` → restarts Spark consumer
4. Updates the k3s ConfigMap → restarts the `spark-consumer` Deployment
5. Verifies generator is running on EC2-1
6. Prints the Grafana URL

### 2. Start the Control Console (local machine)

```bash
python console_server.py
# Open http://localhost:5050
```

The console lets you start/stop the event generator, choose simulation mode
(continuous / spike / failure), adjust rate and user count, and watch live logs.

### 3. Services on EC2-2 (auto-start on boot via systemd)

| Service | Description |
|---|---|
| `edtech-spark` | Spark Structured Streaming consumer |
| `edtech-monitoring` | Prometheus + Grafana via Docker Compose |

```bash
# Check status
ssh -i kafka_key.pem ubuntu@52.66.142.159 \
    'systemctl status edtech-spark edtech-monitoring'

# Tail Spark log
ssh -i kafka_key.pem ubuntu@52.66.142.159 \
    'tail -f /var/log/edtech-spark.log'
```

### 4. Services on EC2-1

```bash
# Get EC2-1's stable Elastic IP
EC2_1_IP=$(cd terraform && terraform output -raw ec2_1_eip)

ssh -i kafka_key.pem ubuntu@$EC2_1_IP \
    'systemctl status kafka; pgrep -a generator.py || echo "generator not running"'
```

---

## Accessing the Grafana Dashboard

**URL:** `http://52.66.142.159:3000`
**Username:** `admin`
**Password:** `edtech2026`

### Prerequisites before opening Grafana

All three of these must be running on EC2-2:

```bash
ssh -i kafka_key.pem ubuntu@52.66.142.159 bash <<'EOF'
# 1. Redis (stores live pipeline:* keys)
systemctl status redis

# 2. Spark consumer (publishes Prometheus metrics + writes Redis keys)
systemctl status edtech-spark

# 3. Monitoring stack (Prometheus scrapes :8000, Grafana reads from both)
systemctl status edtech-monitoring

# Start any that are stopped:
# sudo systemctl start redis edtech-spark edtech-monitoring
EOF
```

### What each panel shows and where data comes from

| Panel | Datasource | Metric / Key | Notes |
|---|---|---|---|
| Events / min | Prometheus | `rate(pipeline_events_processed_total[1m]) * 60` | Computed from counter rate |
| Active Users | Prometheus | `pipeline_active_users_gauge` | Distinct user_ids per batch |
| Surge Status | Prometheus | `pipeline_surge_detected` | 0=NORMAL, 1=SURGE |
| Session Completion % | Redis | `pipeline:session_completion_rate` | Float string in Redis |
| Top 5 Courses | Prometheus | `pipeline_course_joins{course=...}` | Labeled gauge |
| Avg Score by Difficulty | Prometheus | `pipeline_avg_score_by_difficulty{difficulty=...}` | Labeled gauge |
| Avg Engagement | Prometheus | `pipeline_avg_engagement_gauge` | 0–1 scale |
| Dropout Rate | Prometheus | `pipeline_dropout_rate_gauge` | % value |
| Device Split | Prometheus | `pipeline_device_events{device=...}` | Pie chart |
| Premium vs Free | Prometheus | `pipeline_subscription_events{subscription=...}` | Donut chart |
| Online Users by Region | Prometheus | `pipeline_region_users{region=...}` | Bar chart |
| Events by Type | Prometheus | `pipeline_event_type_count{event_type=...}` | Stacked bar |
| Kafka Consumer Lag | Prometheus | `pipeline_kafka_consumer_lag` | Time series |
| Spark Batch Duration p95 | Prometheus | `histogram_quantile(0.95, ...)` | Time series |
| Events Processed | Prometheus | `pipeline_events_processed_total` | Cumulative counter |
| S3 Cost Reduction % | Prometheus | `pipeline_s3_cost_reduction_pct` | From AdaptiveBatchOptimizer |
| Surge Active | Prometheus | `pipeline_surge_detected` | Full-width banner |

### Checking if data is flowing

```bash
# Redis keys (should show ~16 keys)
ssh -i kafka_key.pem ubuntu@52.66.142.159 \
    'redis-cli keys "pipeline:*"'

# Prometheus endpoint (raw metrics)
curl http://52.66.142.159:8000/metrics | grep pipeline_

# Latest Spark log line
ssh -i kafka_key.pem ubuntu@52.66.142.159 \
    'tail -3 /var/log/edtech-spark.log'
```

### If panels show "No data"

| Symptom | Cause | Fix |
|---|---|---|
| All panels empty after EC2-1 restart | EC2-1 had no Elastic IP — its public IP changed | Run `cd terraform && terraform apply` then `./scripts/apply_eip.sh` |
| All Prometheus panels empty | Spark not running or not yet processed a batch | Wait 30s after starting edtech-spark |
| Session Completion % empty | Redis not running or key expired | `sudo systemctl start redis` on EC2-2 |
| S3 Cost Reduction % shows nothing | First flush not yet triggered (needs 5 min or 10k events) | Wait 5 minutes |
| Surge Status stuck at 0 | Normal — no spike running | Run `./scripts/test_10x_load.sh spike` |

---

## Scaling

### Within EC2-2 — Kubernetes HPA (fault tolerance)

The stream consumer runs as a k3s Deployment. HPA adds pods under CPU load,
but since pods share EC2-2's 2 vCPU, this adds fault tolerance not raw throughput.

```bash
# Check pod status
sudo kubectl get pods -n edtech-pipeline

# Check HPA status
sudo kubectl get hpa -n edtech-pipeline
```

### Beyond EC2-2 — AWS Auto Scaling Group (true horizontal scaling)

When CPU stays above 60% for 2 consecutive minutes, CloudWatch boots a new
t3.medium EC2 instance. Each new instance runs the same consumer code and joins
the `edtech-spark-group` Kafka consumer group. Kafka automatically splits the
10 partitions across all running instances.

```
1 instance → 10 partitions (30 ev/s baseline)
3 instances →  3–4 partitions each (90 ev/s spike)
5 instances →  2 partitions each (150 ev/s, ASG max)
```

Scaling back in: CPU drops below 25% for 10 consecutive minutes → remove 1 instance.

### Testing 10× Load

```bash
# Ramp generator to 300 ev/s (5000 users needed)
./scripts/test_10x_load.sh spike

# Watch ASG in AWS Console → EC2 → Auto Scaling Groups
# Watch Surge in Grafana (Surge Status panel turns orange)

# Restore baseline
./scripts/test_10x_load.sh normal
```

### Deploying the ASG (first time)

```bash
# 1. Package consumer code into S3 artifact
./scripts/package_consumer.sh

# 2. Apply Terraform (creates ASG, Launch Template, CloudWatch alarms)
cd terraform && terraform apply

# 3. Deploy k3s on EC2-2 (one time)
ssh -i kafka_key.pem ubuntu@52.66.142.159 "curl -sfL https://get.k3s.io | sh -"
docker build -t edtech-consumer:latest .
docker save edtech-consumer:latest | ssh -i kafka_key.pem ubuntu@52.66.142.159 "sudo k3s ctr images import -"
ssh -i kafka_key.pem ubuntu@52.66.142.159 "sudo kubectl apply -f k8s/"
```

---

## DynamoDB Tables

### UserStats

Updated atomically every 10-second micro-batch using `update_item ADD`.
One item per `(user_id, date)`.

| Field | Type | Description |
|---|---|---|
| `user_id` | String (PK) | Simulated user ID (e.g. `u000042`) |
| `date` | String (SK) | `YYYY-MM-DD` |
| `total_events` | Number | Accumulated events today (ADD) |
| `total_sessions` | Number | Accumulated `session_end` count (ADD) |
| `avg_score` | Decimal | Mean quiz score from last batch (0–100) |
| `avg_engagement` | Decimal | Mean engagement from last batch (0–1) |
| `last_event_type` | String | Most recent event type |
| `last_seen` | String | ISO timestamp |

### CourseStats

Updated atomically every 10-second micro-batch. One item per `(course_id, date)`.

| Field | Type | Description |
|---|---|---|
| `course_id` | String (PK) | e.g. `csg527` |
| `date` | String (SK) | `YYYY-MM-DD` |
| `session_count` | Number | Accumulated `session_end` count (ADD) |
| `join_count` | Number | Accumulated `class_join` count (ADD) |
| `avg_score` | Decimal | Mean quiz score from last batch |
| `avg_engagement` | Decimal | Mean engagement from last batch |
| `pass_rate` | Decimal | % sessions passing (score ≥ 60) |
| `dropout_pct` | Decimal | Voluntary-leave / join × 100 |

### DailyReports

Written once per day by the nightly batch job. One item per `report_date`.

Key fields: `daily_active_users`, `pass_rate_by_difficulty` (easy/medium/hard),
`hourly_event_distribution`, `top_regions`, `device_usage_daily`, `total_events`.

---

## Nightly Batch Job

Runs at **00:00 UTC** via cron on EC2-2. Wrapper: `/home/ubuntu/run_batch_reports.sh`.

```
1. Stop edtech-spark (frees RAM for Spark batch session)
2. spark-submit batch_reports.py --date YYYY-MM-DD
   ├── Read S3 Parquet (pyarrow + boto3, IAM instance role auth)
   ├── Compute daily aggregates with Spark SQL
   ├── Write Parquet report → s3://edtech-pipeline-data-c956dd9c/reports/daily/YYYY-MM-DD/
   └── Write summary → DynamoDB DailyReports
3. Restart edtech-spark
```

To run manually for a specific date:

```bash
ssh -i kafka_key.pem ubuntu@52.66.142.159 \
    '/home/ubuntu/run_batch_reports.sh --date 2026-04-13'
```

---

## S3 Data Layout

```
edtech-pipeline-data-c956dd9c/
├── artifacts/
│   └── edtech-pipeline.tar.gz      ← consumer code artifact for ASG instances
├── raw/
│   └── year=YYYY/month=MM/day=DD/hour=HH/
│       └── TIMESTAMP.parquet       ← flushed by AdaptiveBatchOptimizer (~500 KB each)
└── reports/
    └── daily/
        └── YYYY-MM-DD/
            └── report.parquet      ← single-row summary from batch job
```

---

## Simulation Modes

The generator supports four modes (configurable via the console UI):

| Mode | Description |
|---|---|
| `continuous` | Steady-state at configured events/s |
| `spike` | Normal → spike (3×) → normal sequence (tests SurgeDetector) |
| `failure` | Simulates producer outage then recovery (tests checkpoint recovery) |
| `file` | Writes N events to a local JSON file (no Kafka, for testing) |

Event types: `login`, `logout`, `class_join`, `class_leave`, `session_start`, `session_end`

---

## Key Design Decisions

| Decision | Reason |
|---|---|
| k3s over EKS | EKS control plane costs $0.10/hr + needs m5.large nodes. k3s gives identical HPA + health checks at zero overhead on t3.medium |
| ASG over k3s for real scaling | k3s pods share EC2-2's 2 vCPU — adding pods doesn't add CPU. ASG adds whole new EC2 instances, each with 2 vCPU |
| IS_MASTER_CONSUMER flag | Each consumer sees only its Kafka partition subset. Only EC2-2 writes Redis keys (full view). ASG instances write S3 + DynamoDB only |
| DynamoDB `update_item ADD` | Concurrent consumers can't `put_item` the same key — last write wins and discards the other. `ADD` is atomic and accumulates correctly |
| Per-pod checkpoint path | k3s pods share EC2-2 filesystem. Unique `/tmp/edtech-checkpoints/stream-{hostname}` prevents `ConcurrentModificationException` |
| 1 Spark collect + Python | Original code had 21 Spark actions per batch. At 10× load this caused 5-min batch duration. Single collect + Python drops it to 8–12 seconds |
| cron over Airflow | Airflow needs Postgres + web server + scheduler > 1 GB RAM on a t3.medium already running Spark and Redis. Single nightly job doesn't justify the overhead |

---

## Cost Notes

- EC2-1 (t2.medium): ~$0.023/hr
- EC2-2 (t3.medium): ~$0.0416/hr (Elastic IP included when attached)
- ASG instances: only billed during scale-out events
- DynamoDB: PAY_PER_REQUEST (charges only on actual reads/writes)
- S3: ~MB/day at 30 events/s with adaptive batching
- S3 PUT cost: ~$0.0022/day (vs $0.018/day naive) — 87.6% reduction
