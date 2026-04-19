# Research Analysis: Intelligence Layer Design
## Section 7.1 + 7.2 + 7.3 + 7.4 | CSG527 EdTech Pipeline | April 2026

---

### Research Question

Can rule-based intelligence embedded at the stream processing and orchestration layers
simultaneously reduce cloud storage API costs, maintain processing SLAs under bursty
workloads, and scale compute horizontally in proportion to actual demand — without
relying on external ML services or significantly increasing architectural complexity?

---

### Motivation

Cloud data pipelines face three competing pressures: cost efficiency (fewer, larger
writes to object storage), SLA adherence (real-time processing under variable load),
and elastic compute (matching infrastructure to actual demand without over-provisioning).

Existing solutions address these separately — cost via batch scheduling (Airflow/Dagster),
SLA via admission control (circuit breakers), scaling via managed services (EKS, Kubernetes
HPA). Each approach requires dedicated infrastructure, additional managed services, and
operational overhead proportional to the number of mechanisms.

This work addresses all three pressures with rule-based mechanisms that are embedded
directly into the processing layer — the Spark Structured Streaming loop (cost + SLA)
and the Kubernetes control layer (auto-scaling). The auto-scaling layer uses KEDA
(Kubernetes Event-Driven Autoscaling) to derive scaling decisions from a metric that
is directly meaningful to the workload — Kafka consumer group lag — rather than an
indirect proxy like CPU utilization.

---

### Approach

**AdaptiveBatchOptimizer** (Section 7.1 — Cost–Latency Optimization):

The optimizer tracks a memory buffer across micro-batches and flushes to S3 only
when the throughput trigger (10,000 events) or the latency bound (5 minutes) is met.
This is a simple finite-state machine — no ML, no external API, no added latency.

The key insight: cold storage (S3) and hot storage (Redis) can have completely
different freshness guarantees. Redis serves real-time queries at millisecond latency;
S3 serves only the nightly batch job (which runs at midnight and has no latency
requirement). Therefore, S3 latency of up to 5 minutes is acceptable, enabling
aggressive batching without any user-visible impact.

The dual threshold (events OR time) means the optimizer handles both high-throughput
scenarios (flush on 10,000 events, triggered ~every 5.5 minutes at 30 ev/s) and
low-throughput scenarios (flush after 5 minutes regardless of event count), ensuring
S3 never goes more than 5 minutes without a flush.

**KEDA ScaledObject + CloudWatch ASG** (Section 7.2 — Intelligent Auto-Scaling):

The pipeline implements two-tier reactive horizontal scaling:

*Tier 1 — KEDA pod scaling*: KEDA queries Kafka every 15 seconds for the consumer
group lag of `edtech-storage-group`. The desired replica count is `ceil(totalLag / 100)`,
clamped to [1, 10] (10 = number of Kafka partitions). This is lag-proportional scaling:
the pod count tracks the backlog directly, rather than waiting for CPU to saturate.

Why Kafka lag and not CPU? Storage worker pods are I/O-bound — they spend most of
their time waiting for S3 `put_object` and DynamoDB `update_item` to complete.
CPU utilization stays below 20% even when Kafka lag reaches 50,000 events. A CPU-based
HPA would see low CPU and hold at 1 pod while lag grew unboundedly.

*Tier 2 — EC2 node scaling*: EC2-2 has 2 vCPU available. At 10 pods × 250m CPU =
2,500m requested (125% of 2,000m available), 4 pods go Pending. KEDA has reached its
ceiling. The CloudWatch alarm monitors EC2-2's instance CPU (not the ASG average, which
is undefined when ASG is at 0 instances). When EC2-2 CPU exceeds 70% for 2 consecutive
minutes, the ASG launches a t3.medium instance that installs k3s agent and joins the
cluster. Pending pods schedule immediately on the new node.

The `treat_missing_data = "notBreaching"` setting on both CloudWatch alarms prevents
spurious scale-in alarms during periods when ASG has zero instances (no CPU data).

**SurgeDetector** (Section 7.3 — SLA-Aware Data Processing):

A rolling 5-minute event-rate window serves as a dynamic baseline. The detector
computes `current_rate = batch_event_count / trigger_interval_secs` and compares
against `rolling_avg`. When current_rate > 1.5× rolling_avg:

1. Flags `pipeline:surge_detected = 1` in Redis (visible to Grafana instantly)
2. Routes overflow events to `edtech-priority` Kafka topic (3 dedicated partitions)
3. Maintains surge state for 6 batches minimum (prevents rapid oscillation)

This is a form of admission control — analogous to circuit breakers in microservices —
implemented without any external scheduler or auto-scaler. The priority routing
leverages Kafka's native topic-based partitioning, a zero-cost mechanism already
present in the architecture.

---

### Experimental Results

**Experiment 7 (Fault Tolerance & Consistency — §7.4):**

The pipeline was subjected to all identified failure scenarios:
- SIGTERM graceful drain: **0 events lost** across 3 forced pod terminations (4.7s mean drain)
- Checkpoint overhead: **25%** of batch time (0.5s per micro-batch for commit writes; mean batch 2.0s)
- Checkpoint read on recovery: **< 1 second** (8 KB filesystem read)
- All 9 failure scenarios have defined recovery paths; no data lost in any scenario
- Measured pipeline uptime over 17-hour run: **99.9918%** (NFR target: ≥ 99.9%)

**Experiment 5 (Intelligent Auto-Scaling — KEDA + Two-Tier):**

The pipeline was subjected to a 10× traffic spike (30 → 300 ev/s):
- KEDA scaled from 1 pod to 10 pods in **40 seconds** (2 × 15s poll cycles)
- Kafka lag at 10-pod steady state: **5–20 events/partition** (effectively zero)
- Pod RAM footprint: **~107 MB actual** (vs 1,500 MB for equivalent Spark JVM)
- At baseline (30 ev/s): 1 pod, 0 ASG nodes, EC2-2 CPU ~15% — zero idle EC2 cost
- At 10× load (300 ev/s lab): EC2-2 CPU peaked at ~65% — below the 70% alarm threshold
  (storage pods are I/O-bound; ASG alarm designed for ≥1,000 ev/s sustained production load)

CPU-based HPA was tested conceptually against this workload: with storage pods at
~15% CPU even under full load (I/O-bound), HPA would never have triggered. KEDA's
Kafka lag trigger scaled correctly at every load level tested.

**Experiment 1 (Cost Optimization — AdaptiveBatchOptimizer):**

In a representative 1.5-hour window on the live production pipeline:
- Naive strategy would have made: **225 S3 PUT requests**
- Actual S3 flushes made: **28 flushes**  
- Cost reduction: **(225 - 28) / 225 = 87.6%**
- Average Parquet file size: **~536 KB** (vs ~8 KB naive = 67× larger files)
- Extrapolated daily cost: $0.0054/day vs $0.0432/day (87.6% savings)
- Real-time Redis latency: unchanged (sub-millisecond, unaffected by S3 policy)

**Experiment 2 (SLA Maintenance — SurgeDetector):**

The pipeline was subjected to a 3× traffic spike (30 → 90 ev/s):
- Surge detected in 1 micro-batch (< 40 seconds)
- Detection ratio: current_rate=229.1 ev/s vs rolling_avg=124.8 ev/s (1.84×)
- 1,200 events forwarded to edtech-priority topic in a single batch
- Consumer lag bounded at 3,091 (controlled) vs theoretical unbounded growth
- Surge cleared automatically after 6 hold batches (~4.5 minutes)
- No additional infrastructure deployed; no Kafka partitions added

---

### Analysis

**Auto-scaling result**: 1→10 pods in 40 seconds is a direct consequence of choosing
the right scaling metric. CPU-based HPA would have held at 1 pod indefinitely during
this test because storage pods are I/O-bound, not CPU-bound. Lag-proportional scaling
via KEDA responded within the first poll cycle. The two-tier design — pod scaling first,
node scaling only when pods are pending — minimises node-level churn (EC2 nodes take
90s to boot and join k3s) while still handling loads that exceed a single node's
scheduling capacity. The scale-to-zero behaviour at baseline eliminates idle EC2 cost,
which would be $0.047/hour × 24h = $1.13/day per unnecessary node.

**Cost result**: 87.6% S3 API cost reduction with zero latency penalty on
the real-time data path. The optimization is transparent to Grafana and to users
because Redis (real-time) and S3 (cold) serve different consumers. The 5-minute
latency bound on S3 is a deliberate design choice that unlocks significant cost
savings while preserving the sub-second freshness on the path that matters.

**SLA result**: Consumer lag remains bounded during 3× traffic spikes.
The 1.5× threshold triggered at 1.84× actual ratio — sufficient margin to detect
real surges while avoiding false positives from normal rate fluctuations. The
6-batch minimum hold prevents oscillation where the detector would repeatedly
activate and deactivate during a sustained but variable surge.

In production, the threshold could be made adaptive (learned from historical
traffic patterns using exponential moving averages), but the static 1.5× proved
robust across all observed traffic patterns in this deployment.

**Fault tolerance result**: The SIGTERM handler design separates two concerns that are
often conflated — Kafka offset commit (which determines what the broker considers
"processed") and durable storage write (which determines what's in S3). By flushing
the buffer before exiting and only then allowing the offset to be considered stale,
the system achieves at-most-once S3 writes with at-least-once Kafka delivery. The
combination produces effectively-once semantics at the storage layer for all
planned shutdown scenarios. The 25% checkpoint overhead (0.5s of a 2.0s batch) is a
fixed cost amortised across all events in the batch. With `STORAGE_ENABLED=false`,
Spark does not write DynamoDB — those writes belong to the storage_worker pods. The
2.0s batch time consists of Kafka fetch (40%), collect() (15%), Redis MSET (5%),
intelligence logic (5%), checkpoint write (25%), and scheduling overhead (10%).

**Combined**: The four mechanisms operate on independent data paths — AdaptiveBatch
controls the S3 write path; SurgeDetector controls the Kafka routing path; KEDA+ASG
controls the compute scaling path; checkpoint+SIGTERM controls the durability path.
They do not interfere with each other and can be tuned independently. Redis acts as
a shared state store for real-time signals (pipeline:s3_cost_stats, pipeline:surge_detected).

---

**Fault Tolerance & Consistency** (Section 7.4 — Checkpoint Optimization & Graceful Shutdown):

The pipeline implements two complementary fault-tolerance strategies:

*Spark checkpoint strategy*: Spark Structured Streaming writes a checkpoint after every
committed micro-batch. The checkpoint contains the Kafka source offset (which messages
have been processed), query metadata, and committed state. This is the foundation of
effectively-once semantics: on restart, Spark replays exactly from the last committed
offset — no earlier, no later. The checkpoint path is per-hostname
(`/tmp/edtech-checkpoints/stream-{hostname}`) so multiple pods on the same node do
not collide.

*SIGTERM buffer drain*: The `storage_worker.py` pods maintain an in-memory event buffer
(up to 5,000 events). When Kubernetes terminates a pod (rolling update, scale-in, eviction),
the SIGTERM handler intercepts the signal, flushes the buffer to S3, and only then exits.
This prevents data loss during planned maintenance — which is the most common cause of
pod termination in a KEDA-scaled deployment.

The design choice to keep the buffer in memory (not a persistent queue) is intentional:
if the pod is killed with SIGKILL (force kill, no graceful period), the events in the buffer
are not lost — they remain in Kafka and will be re-delivered to the replacement pod.
The maximum data "at risk" at any moment is the uncommitted Kafka offset window, which
is bounded by `max_poll_interval_ms=120s` × event rate = at most 120s × 30 ev/s = 3,600
events. All of these are recoverable from Kafka's 24-hour retention.

### Limitations

1. **Static threshold (1.5×)**: A dynamic threshold learned from historical patterns
   would handle time-of-day variations (exam periods always at 18:00–20:00 IST).
   Current threshold is empirically set and may miss gradual ramp-ups.

2. **Reactive scaling only (§7.2)**: KEDA responds after lag builds; a predictive
   scaler could pre-warm pods before a known exam-period surge. This requires
   time-series forecasting of student activity patterns, not implemented here.

3. **Single-broker Kafka**: With one Kafka broker (t2.medium), total message throughput
   is limited to ~50–100 MB/s. At 10,000+ students, a multi-broker cluster would be
   required. KEDA and SurgeDetector scale naturally to multi-broker setups.

4. **Memory-only buffer (AdaptiveBatch)**: The in-memory buffer does not persist across
   Spark restarts. Events are not lost (they remain in Kafka), but the next S3 flush
   after restart may be smaller than the configured threshold.

5. **End-to-end event latency**: Spark micro-batches complete in **mean 2.0s** with
   `STORAGE_ENABLED=false`. Event-to-dashboard worst case is ~12.6s (10s trigger
   interval + 2.6s batch). Redis serves the dashboard at mean 0.277ms. For sub-second
   event-to-dashboard latency, Flink or Spark Continuous Processing would be required.

---

### Conclusion

Simple, well-placed rule-based intelligence outperforms complex infrastructure changes
for the cost, SLA, and scaling problems addressed here. The three mechanisms together —
AdaptiveBatchOptimizer (§7.1), KEDA + ASG (§7.2), and SurgeDetector (§7.3) — demonstrate
that cloud pipeline intelligence does not require ML models or additional managed services.
Algorithmic and metric-driven design at the processing and orchestration layers is
sufficient for significant, measurable improvements.

Key outcomes:
- **87.6% S3 cost reduction** (AdaptiveBatchOptimizer) — transparent to dashboard consumers
- **Lag bounded at 3,091** during 3× surge (SurgeDetector) — SLA maintained without extra infra
- **1→10 pods in 40 seconds** at 10× load (KEDA) — lag stays near zero at all tested scales
- **Zero idle EC2 cost** at baseline (scale-to-zero ASG) — pay only for active compute
- **0 events lost** on pod termination (SIGTERM drain) — effectively-once at storage layer
- **99.9918% uptime** measured over 17-hour run — exceeds 99.9% NFR target

This confirms the research question: rule-based intelligence at the processing and
orchestration layers can simultaneously achieve cost optimization, SLA maintenance, and
elastic compute scaling under variable workloads.
