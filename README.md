# AWS EdTech Event Pipeline — serverless vs self-managed, measured

*Developed Jan 2026 – Jun 2026. Published to GitHub Sep 2026.*

Two real-time analytics pipelines built over **the same 30 events/sec stream**
from a simulated learning platform, then benchmarked against each other:

| | Serverless baseline | Self-managed stack |
|---|---|---|
| Ingest | API Gateway → Lambda | **Kafka (KRaft)** |
| Process | Lambda | **Spark Structured Streaming** |
| Serve | DynamoDB | **Redis** |
| Store | S3 | S3 (Parquet) |
| Scale | implicit | **k3s + KEDA** on consumer lag |

The point isn't that one wins. It's that the trade — operational simplicity
against cost and control at scale — is measurable, so this measures it.

## Results

| Measurement | Result |
|---|---|
| **S3 PUT requests** | **cut 87.6%** by adaptive batch flush (10k events / 5 min); average Parquet file grew ~8 KB → **~536 KB** |
| **Autoscaling** | **1 → 10 pods in 40s** under 10x load, scaling on **Kafka consumer lag** rather than CPU |
| **Worker density** | 250 MB Python consumers — **6x more workers per node** |
| **Recovery** | **4.7s mean** via checkpointed offsets; **zero events lost** on graceful SIGTERM drain |
| **Availability** | **99.9918%** over a continuous 17-hour run |
| **Surge detection** | 3x exam-period spike caught at **1.84x ratio within one 10-second micro-batch**, consumer lag held bounded |
| **Rigour** | 8 experiments, **95% confidence intervals** |

## Why scale on lag, not CPU

CPU utilisation is a lagging indicator for a streaming consumer — by the time
CPU climbs, the backlog is already deep and latency has already degraded.
**Kafka consumer lag is the queue depth itself**, so KEDA scales on the quantity
that actually matters. That's what gets 1→10 pods inside 40 seconds instead of
after the SLO has already been missed.

## The adaptive batch flush

Writing every event straight to S3 means one PUT per event, and S3 bills per
request. Buffering to **10,000 events or 5 minutes, whichever comes first**,
turns thousands of tiny objects into one properly-sized Parquet file — 87.6%
fewer requests, and files large enough that downstream readers aren't paying the
small-file penalty either.

## SurgeDetector

Tracks a **rolling 5-minute baseline** and flags when current volume exceeds it
by a configurable ratio. On the simulated exam-period spike it fired at 1.84x
inside a single 10-second micro-batch — early enough for KEDA to add workers
before consumer lag grew unbounded.

## Infrastructure

**30 AWS resources provisioned with Terraform**, instrumented with **Prometheus**
and a **20-panel Grafana dashboard**. Nothing was clicked into existence, so
every experiment ran against a reproducible environment.

## Layout

```
edtech-pipeline/          serverless baseline (Lambda, DynamoDB, S3)
edtech-pipeline-2/        self-managed stack (Kafka, Spark, Redis, k3s + KEDA, Terraform)
Cloudscripts/             benchmark drivers and result figures
diagrams/                 architecture diagrams
benchmarking_report.md    the 8 experiments and their numbers
deep_dive_architecture.md design decisions in detail
intelligence_layer.md     SurgeDetector design
research_analysis.md      background reading and comparisons
Project_Report_07.pdf     full written report
```

**Not in this repo:** screen recordings (2.3 GB) and Terraform provider binaries
(649 MB), both gitignored. Everything needed to read or rebuild the system is here.
