# Changes Log — EdTech Analytics Pipeline
## CSG527 Cloud Computing | BITS Pilani Hyderabad | April 2026

All modifications made to the codebase after initial baseline deployment (2026-04-13).

---

## Phase 3 — Kubernetes + KEDA Auto-scaling (2026-04-16 to 2026-04-17)

### Problem Addressed
The pipeline met all functional requirements at 30 ev/s baseline but could not handle 10×
or 100× event loads as required by NFR §5 (Auto-Scaling). Spark Structured Streaming on a
single t3.medium EC2 was already running at 38–40s per 10s trigger at baseline load —
any spike would immediately violate the SLA. A lightweight, horizontally-scalable
consumer layer was needed.

---

### New Files Created

#### `spark/storage_worker.py`
- **What**: Lightweight Kafka→S3+DynamoDB consumer replacing the Spark JVM for storage duties.
  - No PySpark/JVM dependency — pure Python (kafka-python + pyarrow + boto3)
  - RAM: ~250 MB per pod vs 1.5 GB for Spark JVM (6× reduction)
  - Consumer group: `edtech-storage-group` (separate from Spark's `edtech-monitoring-group`)
  - Buffers events → flushes to S3 Parquet on ≥5,000 events OR ≥300s (whichever first)
  - Writes DynamoDB UserStats + CourseStats per flush
  - Prometheus metrics server on port 8001: `worker_events_processed_total`,
    `worker_s3_flushes_total`, `worker_kafka_lag`, `worker_buffer_size`,
    `worker_dynamo_writes_total`
  - SIGTERM/SIGINT graceful shutdown: drains buffer to S3 before exit (k8s rolling updates)
  - Race condition mitigations: `session_timeout_ms=45000`, `heartbeat_interval_ms=10000`,
    `max_poll_interval_ms=120000` (prevents cascading rebalance storm during scale-out)
- **Why**: Decouples monitoring (Spark+Redis, latency-sensitive) from storage (S3+DynamoDB,
  throughput-sensitive). Enables independent horizontal scaling of each.

#### `k8s/worker-script-cm.yaml`
- **What**: Kubernetes ConfigMap containing `storage_worker.py` as a mounted file.
  Pods mount this as `/scripts/storage_worker.py` at startup.
- **Why**: Avoids baking the script into a Docker image; script changes are deployed
  by updating the ConfigMap and doing a rolling restart (zero-downtime).

#### `scripts/setup_k3s.sh`
- **What**: One-shot bash script for fresh k3s + KEDA deployment on EC2-2:
  - Step 1: Install k3s server on EC2-2 (`--write-kubeconfig-mode 644`)
  - Step 2: Store cluster join token in AWS SSM (`/edtech/k3s-token`)
  - Step 3: Copy kubeconfig to `~/.kube/edtech-k3s.yaml` (with public IP substituted)
  - Step 4: Install KEDA v2.13.1 from upstream YAML
  - Step 5: Upload `storage_worker.py` to S3 artifacts bucket
  - Step 6: Apply all 5 k8s manifests (namespace, configmap, worker-script-cm, deployment, hpa)
  - Step 7: Disable storage on Spark master (`STORAGE_ENABLED=false` in systemd)
  - Step 8: Verify deployment
- **Why**: Reproducible, idempotent setup for EC2-2. Re-run safely after instance replacement.

---

### Modified Files

#### `spark/stream_consumer.py`
- **Change 1**: Added `STORAGE_ENABLED` environment variable flag:
  ```python
  STORAGE_ENABLED = os.getenv("STORAGE_ENABLED", "true").lower() == "true"
  ```
  When `false`, the Spark master skips all S3/DynamoDB writes (which are now handled
  exclusively by `storage_worker.py` pods). Redis writes are unaffected — Spark still
  owns real-time monitoring regardless of this flag.
- **Change 2**: Added kafka logger suppression to prevent BrokerConnection INFO spam
  that was filling `/var/log/edtech-spark.log` (92 MB → 21 KB):
  ```python
  logging.getLogger("kafka").setLevel(logging.WARNING)
  logging.getLogger("kafka.conn").setLevel(logging.WARNING)
  ```
- **Deployment**: EC2-2 systemd service (`edtech-spark.service`) updated live with
  `Environment=STORAGE_ENABLED=false` and restarted.

#### `k8s/deployment.yaml`
- **Before**: Spark deployment (image `bitnami/spark:3.5`, 3 replicas, HPA on CPU).
  Was aspirational — never actually deployed; Spark ran as systemd, not in k8s.
- **After**: Replaced entirely with `edtech-storage-worker` Deployment:
  - Image: `python:3.11-slim` (no JVM, ~150 MB base)
  - Init container `install-deps`: `pip install kafka-python boto3 pyarrow prometheus_client lz4`
    (packages written to shared `emptyDir` volume at `/packages`)
  - Main container: `PYTHONPATH=/packages python3 /scripts/storage_worker.py`
  - Resources: requests 256Mi/250m, limits 512Mi/500m per pod
  - Env overrides: `IS_MASTER_CONSUMER=false`, `STORAGE_ENABLED=true`
  - Liveness + readiness probes: HTTP GET `/metrics:8001`, `initialDelaySeconds: 90`
  - `terminationGracePeriodSeconds: 60` (allows buffer drain to S3 on pod shutdown)

#### `k8s/hpa.yaml`
- **Before**: CPU-based `HorizontalPodAutoscaler` (never worked; Spark wasn't in k8s).
- **After**: Replaced with KEDA `ScaledObject` (`keda.sh/v1alpha1`):
  ```yaml
  scaleTargetRef: edtech-storage-worker Deployment
  minReplicaCount: 1
  maxReplicaCount: 10        # hard cap = number of Kafka partitions on edtech-events
  pollingInterval: 15        # KEDA polls Kafka broker every 15 seconds
  cooldownPeriod:  120       # waits 2 min after lag=0 before scaling back to 1
  triggers:
    - type: kafka
      bootstrapServers: "3.7.163.17:9093"
      consumerGroup: "edtech-storage-group"
      topic: "edtech-events"
      lagThreshold: "100"    # scale formula: ceil(totalLag / 100)
  ```

#### `k8s/configmap.yaml`
- Added keys for storage-worker pods:
  - `CONSUMER_GROUP_ID: "edtech-storage-group"`
  - `FLUSH_EVENTS: "5000"` (buffer threshold per pod)
  - `FLUSH_SECS: "300"` (time threshold per pod)
  - `PROM_PORT: "8001"` (avoids conflict with Spark master on 8000)

#### `k8s/namespace.yaml`
- Added `labels: {project: edtech-pipeline}` for AWS resource tagging.

#### `terraform/main.tf`
Changes applied via `terraform apply` on 2026-04-17:

| Resource | Change | Reason |
|----------|--------|--------|
| `aws_security_group_rule.ec2_2_k3s_api` | **NEW** — port 6443 TCP ingress on EC2-2 | k3s API server for worker node joins and kubectl |
| `aws_security_group_rule.ec2_2_k3s_flannel` | **NEW** — port 8472 UDP ingress on EC2-2 | Flannel VXLAN pod overlay network |
| `aws_ssm_parameter.k3s_token` | **NEW** — `/edtech/k3s-token` SecureString | k3s join token for ASG worker nodes |
| `aws_iam_policy.edtech_access` | **UPDATED** — added `SSMk3sToken` statement | ASG nodes need SSM read at boot |
| `aws_cloudwatch_metric_alarm.high_cpu` | **UPDATED** — watches EC2-2 instance CPU (not ASG avg) | ASG is at 0 capacity → has no CPU metrics; EC2-2 CPU saturation is the real trigger |
| `aws_cloudwatch_metric_alarm.high_cpu` | **UPDATED** — threshold 60% → 70% | Reduce false positives; EC2-2 has background workload |
| `aws_cloudwatch_metric_alarm.high_cpu` | **UPDATED** — `treat_missing_data = notBreaching` | Prevents alarm when ASG is at 0 instances |
| `aws_cloudwatch_metric_alarm.low_cpu` | **UPDATED** — `treat_missing_data = notBreaching` | Prevents spurious scale-in alarm when ASG is at 0 |
| `aws_autoscaling_policy.scale_out` | **UPDATED** — cooldown 120s → 180s | k3s agent boot takes ~90s; allow new node to join before next scale-out |
| `aws_security_group.consumer_asg` | **UPDATED** — port 8000 → 8001, add 10250/8472 ingress, 6443 egress | Match storage-worker metrics port; kubelet + Flannel for k3s networking |
| `aws_launch_template.consumer` | **UPDATED** — replaced userdata | Old: Spark on ASG. New: k3s agent that joins EC2-2's cluster (reads SSM token, discovers EC2-2 private IP via tag, installs k3s agent) |
| `aws_autoscaling_group.consumer` | **UPDATED** — comment updated | `desired_capacity=0` is now correctly explained as CPU-alarm driven, not Cluster Autoscaler |

#### `scripts/test_10x_load.sh`
- Fixed generator path: `/home/ubuntu/edtech-pipeline/generator.py` → `/home/ubuntu/generator.py`
- Fixed log path: `/var/log/edtech-generator.log` → `/tmp/gen.log`
- Added `--topic edtech-events` flag (required by generator.py)
- Fixed `GENERATOR_PATH` and `LOG_PATH` variables to match actual EC2-1 filesystem

#### `docker-compose.monitoring.yml`
- Prometheus retention reduced: `retention.time=7d` → `2d`, added `retention.size=80MB`
- Reason: EC2-2 disk was at 96–99% due to log bloat + Prometheus TSDB growth.
  Combined with logrotate for application logs (see below).

---

### Live EC2-2 Changes (not in files, but applied directly)

| What | Command | Reason |
|------|---------|--------|
| EBS root volume resize | `aws ec2 modify-volume ... --size 20` + `growpart` + `resize2fs` | k3s needs ~500 MB to extract binaries; old 8 GB volume had only 234 MB free |
| Logrotate config | `/etc/logrotate.d/edtech` | Spark log grew to 92 MB; now rotated daily, 3 copies, 20 MB max |
| SSM parameter | `aws ssm put-parameter --name /edtech/k3s-token ...` | k3s join token stored for ASG worker nodes; later imported into Terraform state |
| k3s server install | `curl -sfL https://get.k3s.io \| INSTALL_K3S_EXEC="server ..." sh -` | k3s v1.31 installed with `--write-kubeconfig-mode 644` |
| KEDA v2.13.1 install | `kubectl apply -f https://github.com/kedacore/keda/.../keda-2.13.1.yaml` | CRD-only note: `scaledjobs.keda.sh` annotation too large (known issue), non-blocking |
| k3s data dir cleanup | `sudo rm -rf /var/lib/rancher/k3s/data/` + restart | First k3s install failed mid-way (disk full); partial state caused Flannel CNI missing. Clean dir fixed it. |
| STORAGE_ENABLED=false | `sed -i '/^Environment=KAFKA_BOOTSTRAP/a Environment=STORAGE_ENABLED=false' /etc/systemd/system/edtech-spark.service` | Prevents duplicate S3/DynamoDB writes now that k8s pods own storage |

---

### Infrastructure State After Phase 3

```
┌── EC2-1 (t2.medium, Elastic IP 3.7.163.17) ─────────────────┐
│  Apache Kafka 4.1.1 KRaft                                     │
│  Generator: 30 ev/s → edtech-events (10 partitions)          │
└──────────────────────────────────────────────────────────────┘
                      │ port 9093
           ┌──────────┴──────────┐
           │ Consumer Group A    │          │ Consumer Group B    │
           │ edtech-monitoring   │          │ edtech-storage      │
           ▼                     │          ▼                     │
┌── EC2-2 (t3.medium) ──────────┐│ ┌── k3s Pods (EC2-2) ───────┐│
│  Spark Structured Streaming   ││ │  edtech-storage-worker    ││
│  STORAGE_ENABLED=false        ││ │  1–10 replicas (KEDA)     ││
│  → Redis (16 live KPI keys)   ││ │  → S3 Parquet             ││
│  → Prometheus metrics :8000   ││ │  → DynamoDB UserStats     ││
│  → SurgeDetector              ││ │  → DynamoDB CourseStats   ││
│  k3s control plane            ││ │  → Prometheus :8001       ││
│  Grafana :3000                ││ └───────────────────────────┘│
└───────────────────────────────┘│                               │
                                 └────────────────────────────────
                                  ▲ ASG node joins k3s cluster
                                  │ when EC2-2 CPU > 70% for 2 min
                              ┌───┴──────────────────────────────┐
                              │ AWS ASG (0→N k3s agent nodes)    │
                              │ t3.medium, max 10 instances      │
                              │ CloudWatch → scale-out/scale-in  │
                              └──────────────────────────────────┘
```

---

## Phase 2 — Infrastructure Hardening (2026-04-12 to 2026-04-13)

### Problem Addressed
EC2-1 public IP was ephemeral — Kafka `advertised.listeners` broke on every stop/start.
ASG instances had broken userdata (S3 artifact path wrong).

### Changes
- `terraform/main.tf`: Added `aws_eip` + `aws_eip_association` for EC2-1 (Elastic IP `3.7.163.17`)
- `scripts/apply_eip.sh`: Updates Kafka `server.properties` and restarts services after EIP assignment
- Kafka `server.properties` on EC2-1: `advertised.listeners` updated to use EIP
- `k8s/configmap.yaml`: `KAFKA_BOOTSTRAP` set to `3.7.163.17:9093`
- `k8s/hpa.yaml` (KEDA): `bootstrapServers` set to `3.7.163.17:9093`
- Kafka retention: set to 24h (`log.retention.hours=24`) to prevent log disk bloat

---

## Phase 1 — Baseline Pipeline (2026-04-10 to 2026-04-12)

Initial working pipeline:

| Component | Details |
|-----------|---------|
| Event generator | `generator.py`: 500 users, 10 courses, 30 ev/s, Kafka producer with lz4 |
| Kafka | 4.1.1 KRaft, 2 topics: `edtech-events` (10 parts), `edtech-priority` (3 parts) |
| Spark consumer | PySpark 3.5 Structured Streaming, 10s micro-batch, checkpoint at `/tmp/edtech-checkpoints/` |
| Intelligence | `SurgeDetector` (1.5× threshold, priority routing) + `AdaptiveBatchOptimizer` (10k events / 5 min) |
| Storage | S3 Parquet (year/month/day/hour), DynamoDB (PAY_PER_REQUEST), Redis 7 |
| Monitoring | Prometheus (port 8000) + Grafana (port 3000), 20-panel dashboard |
| Batch | `batch_reports.py` via `run_batch_reports.sh`, cron midnight UTC |
| IaC | Terraform: S3, DynamoDB, IAM, ASG, Security Groups, Launch Template |

---

## Documentation Changes

| File | Change |
|------|--------|
| `README.md` | Updated architecture diagram, tech stack table, k8s/KEDA section, two-tier scaling description |
| `docs/architecture_design.md` | Added Section 6 (k3s/KEDA), Section 7 (two-tier scaling), updated architecture diagram, updated technology table |
| `docs/benchmarking_report.md` | Added Experiment 5 (KEDA pod scaling, measured), Experiment 6 (EC2 horizontal scaling), updated summary table, new statistical charts |
| `docs/research_analysis.md` | Added Section on Intelligent Auto-scaling (PDF §7.2), KEDA reactive vs predictive analysis |
| `docs/demo_slides.md` | Added slides 13–15 (k8s architecture, KEDA scaling demo, two-tier scaling) |
| `docs/README_deliverables.md` | Updated experiment count, k8s deliverable status, Phase 3 summary |
| `CHANGES_LOG.md` | **THIS FILE** — complete change history |
