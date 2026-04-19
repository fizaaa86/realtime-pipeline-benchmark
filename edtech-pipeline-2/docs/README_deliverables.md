# Deliverables Status — CSG527 EdTech Pipeline
## Updated: 2026-04-17 (Phase 3 complete — all 7 experiments complete)

---

## Status Overview

| Deliverable | Status | File |
|-------------|--------|------|
| D2 — Source Code & Deployment | ✅ DONE | On EC2s + k3s pods (~/edtech-pipeline-2/) |
| D1 — Architecture & Design Doc | ✅ DONE | docs/architecture_design.md |
| D3 — Benchmarking Report | ✅ DONE | docs/benchmarking_report.md (8 experiments + raw data appendix) |
| D4 — Research Analysis | ✅ DONE | docs/research_analysis.md |
| D5 — Demo Slides | ✅ DONE | docs/demo_slides.md (slides 1–15) |
| D6 — Intelligence Layer Analysis | ✅ DONE | docs/intelligence_layer.md |
| D7 — Change History | ✅ DONE | CHANGES_LOG.md |

All 8 experiments complete. Phase 3 (k3s + KEDA two-tier auto-scaling) deployed and measured. Experiment 7 (§7.4 Fault Tolerance): SIGTERM drain, checkpoint overhead, failure mode matrix, uptime SLA. Experiment 8 (NFR §5 Latency): Redis GET latency (mean 0.277ms), Spark batch time (mean 2.0s), end-to-end analysis. Raw data from experiment_results.md merged into benchmarking_report.md Appendix A.

---

## Experiment Results Summary

| # | Experiment | Key Result | Status |
|---|-----------|------------|--------|
| 1 | S3 Cost (AdaptiveBatch) | **87.6% reduction** (28 vs 225 PUTs) | ✅ |
| 2 | Surge Detection | Detected in **<40s**, 1,200 events → priority topic | ✅ |
| 3 | Batch Job Timing | **118 seconds** (267,759 events, 125 files, 66.9 MiB) | ✅ |
| 4 | Failure Recovery | **5 seconds** to resume (checkpoint-based) | ✅ |
| 5 | KEDA Pod Scaling | **1→10 pods in 40 seconds** at 10× load, lag bounded 5–20/partition | ✅ |
| 6 | EC2 Horizontal Scaling | Two-tier: KEDA+ASG, **zero idle EC2 cost** at baseline, CPU alarm calibrated for ≥1,000 ev/s | ✅ |
| 7 | Fault Tolerance (§7.4) | **0 events lost** on SIGTERM (3 runs, 4.7s mean); **99.9918% uptime** (17-hr run) | ✅ |
| 8 | End-to-End Latency | Redis GET **mean 0.277ms** (50 samples); Spark batch **mean 2.0s** (30 samples) | ✅ |

---

## Phase 3 Summary (2026-04-16 to 2026-04-17)

### What Was Added

| Component | Details |
|-----------|---------|
| `spark/storage_worker.py` | Python Kafka→S3+DynamoDB consumer; ~250 MB RAM/pod; SIGTERM buffer drain |
| `k8s/worker-script-cm.yaml` | ConfigMap mounting storage_worker.py as /scripts/storage_worker.py |
| `scripts/setup_k3s.sh` | One-shot k3s + KEDA installation and deployment script |
| k3s on EC2-2 | v1.31 server (control plane); kubeconfig at ~/.kube/edtech-k3s.yaml |
| KEDA v2.13.1 | Kafka trigger ScaledObject; desiredPods = ceil(lag/100); 1–10 replicas |
| Terraform changes | k3s SG ports (6443, 8472), SSM parameter, CloudWatch alarm fix (EC2-2 CPU) |
| ASG two-tier scaling | EC2-2 CPU > 70% triggers t3.medium k3s agent node; zero idle cost at baseline |

### Race Condition Fix
Rapid 1→10 pod scale-out caused cascading Kafka group rebalances.
Fixed via `storage_worker.py` KafkaConsumer parameters:
- `session_timeout_ms=45000` — pods survive multi-consumer rebalance
- `heartbeat_interval_ms=10000` — group membership maintained
- `max_poll_interval_ms=120000` — no eviction during S3 flush

### Verified S3 Writes from k8s Pods
6 Parquet files confirmed in S3 with UUID suffix (`_{uuid8}.parquet`), distinguishing pod writes from Spark writes.

---

## Key Numbers for Viva

| Question | Answer |
|----------|--------|
| Events/second (steady state) | 30 ev/s (1,800/min) |
| Events/second (10× test) | 300 ev/s (18,000/min) |
| Students simulated | 500 |
| Courses | 10 |
| Kafka: edtech-events partitions | 10 |
| Kafka: edtech-priority partitions | 3 |
| Spark micro-batch trigger | 10 seconds |
| **Actual batch time** | **118 seconds** (267,759 events, 125 files — t3.medium) |
| AdaptiveBatch flush threshold | 10,000 events (Spark) / 5,000 events (k8s pods) OR 5 minutes |
| **S3 cost reduction (measured)** | **87.6%** (28 vs 225 PUTs) |
| Surge detection threshold | 1.5× 5-min rolling average |
| **Surge detected at ratio** | **1.84×** (229.1 vs 124.8 ev/s) |
| Events to priority topic | 1,200 in one batch |
| **Failure recovery time** | **5 seconds** (checkpoint-based) |
| **KEDA scale-out 1→10 pods** | **40 seconds** (2 poll cycles × 15s) |
| KEDA lag threshold | 100 events/pod → ceil(totalLag/100) pods |
| Pod RAM footprint | ~107 MB actual (~250 MB requested) |
| ASG trigger | EC2-2 CPU > 70% for 2 consecutive minutes |
| ASG scale-in | CPU < 25% for 10 min → remove node |
| Idle cost at baseline | Zero ASG nodes, 1 KEDA pod |
| Grafana panels | 20 in 4 sections |
| Redis metric keys | 16 (pipeline:*) |
| DynamoDB tables | 3 (UserStats, CourseStats, DailyReports) |
| S3 bucket | edtech-pipeline-data-c956dd9c |
| Kafka version | 4.1.1 KRaft |
| k3s version | v1.31 |
| KEDA version | v2.13.1 |
| Terraform resources | 30 |
| **SIGTERM drain — events lost** | **0** (3 runs, mean 4.7 s) |
| **Checkpoint overhead** | **0.8 s (2.4% of batch)** |
| **Uptime (17-hour run)** | **99.9918%** (NFR: ≥99.9%) |
| Failure scenarios with recovery | 9 / 9 (all automatic) |
| SSH key | ~/.ssh/kafka_key.pem |
| EC2-1 (Kafka) IP | 3.7.163.17 (Elastic IP) |
| EC2-2 (Spark/k3s) IP | 52.66.142.159 (Elastic IP) |
| Generator location | EC2-1: ~/generator.py |
| Consumer group (Spark) | edtech-monitoring-group |
| Consumer group (k8s pods) | edtech-storage-group |
