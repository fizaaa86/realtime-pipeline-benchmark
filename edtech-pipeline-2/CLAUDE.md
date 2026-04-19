# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

---

## Running the pipeline components

All components run on remote EC2 instances. SSH access requires `kafka_key.pem` (in the project root).

```bash
# EC2-1 (Kafka, 3.7.163.17) — start event generator
python generator.py --rate 30 --users 500 --bootstrap-servers localhost:9092

# EC2-2 (Spark/k3s, 52.66.142.159) — start stream consumer (systemd manages this in production)
spark-submit \
  --jars $(ls /opt/spark/jars/*.jar | tr '\n' ',') \
  /home/ubuntu/edtech-pipeline/spark/stream_consumer.py

# EC2-2 — start Grafana + Prometheus
docker compose -f docker-compose.monitoring.yml up -d

# EC2-2 — run nightly batch job manually
spark-submit spark/batch_reports.py --date 2026-04-13
```

**k3s / Kubernetes** (kubeconfig is at `~/.kube/edtech-k3s.yaml`):
```bash
export KUBECONFIG=~/.kube/edtech-k3s.yaml

kubectl get pods -n edtech-pipeline              # storage worker pods
kubectl get scaledobject -n edtech-pipeline      # KEDA ScaledObject
kubectl logs -n edtech-pipeline <pod-name>       # worker logs
kubectl get pods -n edtech-pipeline -w           # watch scaling events

# Full k3s + KEDA setup from scratch (run once after terraform apply)
bash scripts/setup_k3s.sh

# 10× load test
bash scripts/test_10x_load.sh
```

**Terraform:**
```bash
cd terraform
terraform plan
terraform apply
terraform output ec2_1_eip          # EC2-1 Elastic IP (Kafka bootstrap)
terraform output s3_bucket_name     # bucket name (random suffix)
```

**Local control console** (start/stop generator, switch modes — continuous/spike/failure):
```bash
pip install flask kafka-python   # once
python console_server.py         # open http://localhost:5050
```

**Environment config on EC2-2:** `/etc/edtech-spark.env` is loaded by systemd and sets `KAFKA_BOOTSTRAP`, `S3_BUCKET`, `STORAGE_ENABLED=false`, etc. Run `scripts/apply_eip.sh` when EC2-1's IP changes.

---

## Architecture

### Dual consumer group model (the most critical design decision)

Two completely independent consumer groups both read every event from `edtech-events`:

| Consumer | File | Group ID | Writes | Scaling |
|---|---|---|---|---|
| Spark (EC2-2 systemd) | `spark/stream_consumer.py` | `edtech-monitoring-group` | Redis + Prometheus only | Fixed (1 instance) |
| k8s pods (KEDA) | `spark/storage_worker.py` | `edtech-storage-group` | S3 + DynamoDB only | KEDA: 1–10 pods on Kafka lag |

`stream_consumer.py` always runs with `STORAGE_ENABLED=false` on EC2-2 — it never writes S3 or DynamoDB. The k8s pods own all durable storage writes. This prevents duplicate writes and lets each path scale independently.

A second flag, `IS_MASTER_CONSUMER`, controls whether a Spark instance writes Redis KPIs. Only EC2-2 sets this to `true`; any hypothetical ASG Spark nodes would set it `false` to avoid partial-aggregate corruption.

### Intelligence layer (`spark/intelligence.py`)

Imported exclusively by `stream_consumer.py` (not by `storage_worker.py`). Defines two classes:

- **`SurgeDetector`** — maintains a rolling 5-minute deque of per-second rates. When `current_rate > rolling_avg × 1.5`, sets `pipeline:surge_detected = 1` in Redis, forwards `class_join` + `session_start` events to `edtech-priority` Kafka topic, and holds the surge state for 6 batches minimum (prevents oscillation). Clears when `current_rate <= rolling_avg × 1.1`.

- **`AdaptiveBatchOptimizer`** — in-memory buffer that flushes to S3 only when ≥ 10,000 events are buffered OR ≥ 5 minutes have elapsed. Writes cost savings stats to `pipeline:s3_cost_stats` in Redis. Always call `add_events(events: list[dict])` (uses the pre-collected Python list, zero Spark actions). Do not call `add(batch_df)` — it calls `batch_df.collect()` internally and violates the single-collect pattern.

Note: `storage_worker.py` has its own simpler flush logic (5,000 events / 5 min) inline — it does not use `intelligence.py`.

### Single `collect()` pattern in `process_batch`

`stream_consumer.py:process_batch()` calls `batch_df.collect()` exactly once and derives all 16+ KPIs from the resulting Python list. Do not add additional Spark actions (`.count()`, `.filter()`, etc.) inside `process_batch` — each action is a distributed job and will severely degrade batch time on a single-node t3.medium.

### Checkpoint path is per-hostname

```python
CHECKPOINT_PATH = f"/tmp/edtech-checkpoints/stream-{_hostname}"
```
This prevents `ConcurrentModificationException` if multiple Spark instances ever run on the same EC2. Do not change to a fixed path.

### `storage_worker.py` deployed without a Docker build

The pod has no custom image. `k8s/worker-script-cm.yaml` mounts `storage_worker.py` as a ConfigMap file at `/scripts/storage_worker.py`. An init container runs `pip install kafka-python boto3 pyarrow prometheus_client lz4` at pod start. The file `k8s/hpa.yaml` contains a KEDA `ScaledObject` (not a standard Kubernetes HPA) despite the filename.

Three Kafka consumer settings in `storage_worker.py` prevent rebalance storms during KEDA scale-out: `session_timeout_ms=45000`, `heartbeat_interval_ms=10000`, `max_poll_interval_ms=120000`. Do not lower these — cascading rebalances caused event loss during 1→10 pod scale-out in testing.

### DynamoDB write pattern

Both `stream_consumer.py` and `storage_worker.py` use `ADD` for counters in DynamoDB UpdateExpression:
```
ADD total_events :n, total_sessions :s  SET last_event_type = :lt ...
```
This is intentional — multiple pods/consumers can safely write their partition's deltas concurrently without overwriting each other. Do not change `ADD` to `SET` for counter fields.

### S3 key format difference

- Spark / `AdaptiveBatchOptimizer`: `raw/year=.../TIMESTAMP.parquet` (no UUID)
- `storage_worker.py` pods: `raw/year=.../TIMESTAMP_{uuid8}.parquet` (UUID suffix)

The UUID suffix distinguishes pod writes from Spark writes in the S3 bucket and prevents filename collisions when multiple pods flush at the same second.

### Grafana access

URL: `http://52.66.142.159:3000` — credentials: `admin` / `edtech2026` (set in `docker-compose.monitoring.yml`).

Prometheus scrapes `:8000` (Spark) and `:8001` (storage worker pods). Both use `network_mode: host`.

---

## Key environment variables

| Variable | Default | Where set |
|---|---|---|
| `KAFKA_BOOTSTRAP` | `localhost:9093` | `/etc/edtech-spark.env` on EC2-2 |
| `S3_BUCKET` | `edtech-pipeline-data` | `/etc/edtech-spark.env`; also k8s ConfigMap |
| `STORAGE_ENABLED` | `true` | Set to `false` in EC2-2 systemd service |
| `IS_MASTER_CONSUMER` | `true` | Leave `true` on EC2-2 only |
| `TRIGGER_SECS` | `10` | Spark micro-batch interval |
| `CHECKPOINT_PATH` | `/tmp/edtech-checkpoints/stream-{hostname}` | Per-host auto-set |
| `FLUSH_EVENTS` | `5000` | storage_worker pod (k8s ConfigMap) |
| `FLUSH_SECS` | `300` | storage_worker pod (k8s ConfigMap) |

---

## Project layout

```
spark/
  stream_consumer.py    # Spark Structured Streaming — Redis/Prometheus path only
  intelligence.py       # SurgeDetector + AdaptiveBatchOptimizer (imported by stream_consumer)
  storage_worker.py     # Kafka→S3+DynamoDB — runs as k8s pods (no Spark/JVM)
  batch_reports.py      # Nightly cron job — S3 Parquet → DynamoDB DailyReports

k8s/                    # Applied by scripts/setup_k3s.sh
  namespace.yaml        # edtech-pipeline namespace
  configmap.yaml        # env vars for pods (KAFKA_BOOTSTRAP, S3_BUCKET, etc.)
  worker-script-cm.yaml # storage_worker.py mounted as ConfigMap (no Docker build)
  deployment.yaml       # edtech-storage-worker Deployment + probes + resource limits
  hpa.yaml              # KEDA ScaledObject (Kafka lag trigger, NOT standard k8s HPA)

terraform/
  main.tf               # 30 resource blocks: S3, DynamoDB, EC2, ASG, CloudWatch, IAM, SSM
  variables.tf
  outputs.tf            # ec2_1_eip, ec2_2_eip, s3_bucket_name, dynamodb_*

scripts/
  setup_k3s.sh          # One-shot: install k3s + KEDA + deploy k8s manifests on EC2-2
  apply_eip.sh          # Update /etc/edtech-spark.env when EC2-1 IP changes
  test_10x_load.sh      # Switch generator to 300 ev/s for scaling experiments
  package_consumer.sh   # Package code → S3 artifact (needed before ASG scale-out)

dags/
  batch_reports_dag.py  # Legacy Airflow DAG — NOT running; cron is used instead

docs/                   # All deliverables — see README_deliverables.md for index
generator.py            # Synthetic event producer (EC2-1)
console_server.py       # Local Flask control UI (port 5050) — start/stop generator
docker-compose.monitoring.yml   # Grafana + Prometheus (EC2-2, network_mode: host)
prometheus/prometheus.yml       # Scrape configs for :8000 and :8001
grafana/dashboards/edtech_dashboard.json   # All 20 panels
```
