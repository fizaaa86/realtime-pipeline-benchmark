# RECORDING PHASES — CSG527 EdTech Pipeline
## 6 separate cuts. Edit together in sequence. ~33 minutes total.

---

## PIPELINE 1 AWS RESOURCES (ap-south-1) — quick reference

| Component | Resource |
|---|---|
| Kinesis stream | `edtech-events-stream` |
| Lambda function | `edtech-stream-processor` (Python 3.14, batch size 100, trigger: Kinesis) |
| S3 bucket | `edtech-raw-data` (raw/ = JSON files, processed/ = Glue views) |
| DynamoDB tables | `active_sessions` (PK: session_id), `class_metrics` (PK: class_id) |
| Glue database | `edtech-analytics` |
| Glue table | `raw_raw` → `s3://edtech-raw-data/raw/` |

## PIPELINE 2 AWS RESOURCES

| Component | Resource |
|---|---|
| EC2-1 (Kafka + generator) | `3.7.163.17` |
| EC2-2 (Spark + k3s + Redis) | `52.66.142.159` |
| Grafana | `http://52.66.142.159:3000` — admin / edtech2026 |
| S3 bucket | `edtech-pipeline-data-c956dd9c` |
| DynamoDB tables | `UserStats`, `CourseStats`, `DailyReports` |

---

## ONE-TIME SETUP — do this once before your first cut

### Open iTerm2 with 4 tabs and SSH into each:

```
Tab 1  [EC2-1 Kafka]     ssh -i kafka_key.pem ubuntu@3.7.163.17
Tab 2  [EC2-2 Spark]     ssh -i kafka_key.pem ubuntu@52.66.142.159
Tab 3  [EC2-2 kubectl]   ssh -i kafka_key.pem ubuntu@52.66.142.159
Tab 4  [Local Mac]       cd /Users/krishnaagrawal/Desktop/CloudProject/edtech-pipeline-2
```

### Open browser with 2 tabs:
```
Tab 1 — Grafana:     http://52.66.142.159:3000   (admin / edtech2026)
Tab 2 — AWS Console: console.aws.amazon.com       (region: ap-south-1)
```
Grafana settings: time range = **Last 15 minutes**, auto-refresh = **10s**

### AWS Console pre-check — open each resource and confirm it has data:
```
Kinesis → edtech-events-stream → Monitoring tab → IncomingRecords graph visible
Lambda → edtech-stream-processor → Monitor tab → CloudWatch graphs visible
S3 → edtech-raw-data → raw/ → files exist
DynamoDB → active_sessions → Explore items → records visible
Athena → Query editor → run the query below manually once to confirm it works:
```
```sql
SELECT event_type, COUNT(*) AS cnt
FROM "edtech-analytics"."raw_raw"
GROUP BY event_type
ORDER BY cnt DESC;
```

---

## HEALTH CHECK — run before EVERY cut

```bash
# Tab 1 (EC2-1)
systemctl status kafka --no-pager | head -3        # must: active (running)
pgrep -a -f generator.py                            # must: show PID at rate 30

# If generator is dead, restart it:
nohup python3 /home/ubuntu/generator.py \
  --bootstrap-servers localhost:9092 --rate 30 --users 500 \
  >> /tmp/gen.log 2>&1 &

# Tab 2 (EC2-2)
systemctl status edtech-spark --no-pager | head -3  # must: active (running)
docker ps --format "table {{.Names}}\t{{.Status}}"  # must: grafana + prometheus Up
export KUBECONFIG=~/.kube/edtech-k3s.yaml
kubectl get pods -n edtech-pipeline                  # must: 1 pod Running
redis-cli ping                                       # must: PONG

# Tab 4 (Local Mac)
EC2_1_IP=3.7.163.17 bash scripts/test_10x_load.sh status
# must: print generator PID — if error, fix SSH before recording
```

---
---

# CUT 1 — Intro + Pipeline 1 AWS Tour
## Duration: ~6 min | No waits

**Before pressing record:**
- Browser: AWS Console already open at Kinesis → Data streams
- Terminal: any tab visible in background (won't type during this cut)
- Screen layout: full browser

---

### [START RECORDING]

**Say:**
> "This project implements two real-time analytics pipelines for an online learning platform —
> simulating events like logins, class joins, and session activity from students.
>
> Pipeline 1 was our first attempt — a fully serverless AWS architecture.
> It worked, but hit serious limits at scale.
>
> Pipeline 2 is the production-grade system: Apache Kafka, PySpark, Kubernetes,
> intelligence for surge detection and cost optimization, and true fault tolerance.
>
> I'll start by showing Pipeline 1 live in AWS, then demonstrate its limitations,
> then walk through everything Pipeline 2 does differently — live experiments,
> intentional failures, and automatic recovery."

---

### Step 1 — Kinesis
```
Navigate to: Kinesis → Data streams → edtech-events-stream → Monitoring tab
Show: IncomingRecords and IncomingBytes graphs
```
**Say:**
> "Pipeline 1 flow: a Python event generator on my local Mac pushed events into
> Amazon Kinesis. A Lambda function was triggered by each Kinesis batch — it
> processed events and wrote to both S3 and DynamoDB. S3 data was crawled by Glue,
> queryable via Athena, visualised in QuickSight. Fully serverless — zero infrastructure.
>
> Kinesis was the ingestion layer. One shard handles 1,000 records per second —
> fine at 30 ev/s baseline. But each additional shard costs $0.015 per hour
> whether used or not. There's no dynamic scaling."

---

### Step 2 — Lambda
```
Navigate to: Lambda → Functions → edtech-stream-processor
Scroll through code editor briefly
Click: Configuration → Triggers → show Kinesis trigger
```
**Say:**
> "Lambda was triggered by Kinesis. Batch size is 100 records — at 30 events per second
> that's a Lambda invocation roughly every 3 seconds, about 28,000 invocations per day.
> No state, no memory between invocations, no way to detect trends across batches."

```
Click: Monitor → View CloudWatch metrics
Show: Invocations graph + Duration graph
```
**Say:**
> "These Duration spikes — the tall ones — are Lambda cold starts.
> Every time Lambda hadn't received traffic for a few minutes, the next invocation
> took 200 to 800 milliseconds just to spin up a new container.
> For a real-time analytics system, that's unacceptable latency.
> And Invocations scale linearly with traffic: triple the events, triple the Lambda calls,
> triple the cost. There's no batching optimization possible."

---

### Step 3 — S3
```
Navigate to: S3 → edtech-raw-data → raw/ prefix
Scroll through — show many small files
```
**Say:**
> "Every Lambda invocation wrote one JSON file to S3.
> At 30 events per second that's thousands of individual files per day — each a few kilobytes.
> S3 charges $0.005 per 1,000 PUT requests. Thousands of files per day means
> hundreds of thousands of PUT calls per month.
> And Athena opens each file individually — the small file problem makes queries slow and expensive."

---

### Step 4 — DynamoDB
```
Navigate to: DynamoDB → Tables → active_sessions → Explore items
Show a few records (session_id as partition key)

Navigate to: DynamoDB → Tables → class_metrics → Explore items
Show a few records (class_id as partition key)
```
**Say:**
> "Two Pipeline 1 DynamoDB tables: active_sessions tracks live user sessions,
> class_metrics aggregates per-class stats.
> Lambda wrote on every single event — no batching, no atomic delta updates.
> If two Lambda instances processed overlapping events simultaneously,
> a SET write from one could overwrite the other's result.
> Pipeline 2 uses atomic ADD operations — multiple pods write concurrently,
> each adding their delta, with zero possibility of overwriting each other."

---

### Step 5 — Athena
```
Navigate to: Athena → Query editor
Select database: edtech-analytics
```
**Type and run:**
```sql
SELECT event_type, COUNT(*) AS cnt
FROM "edtech-analytics"."raw_raw"
GROUP BY event_type
ORDER BY cnt DESC;
```
**[Wait 10–30 seconds for results]**

**Say:**
> "Athena runs SQL on the S3 files via the Glue Catalog.
> Note the query time and data scanned — it's scanning all those individual JSON files.
> With thousands of tiny files, even a simple COUNT is slow and expensive.
> Pipeline 2 stores Parquet with year/month/day/hour partitioning —
> the same query reads only the relevant partition. Orders of magnitude faster and cheaper."

### [STOP RECORDING — CUT 1 DONE]

---
---

# CUT 2 — Pipeline 1 Limitations + Transition + Pipeline 2 Running
## Duration: ~4 min | No waits

**Before pressing record:**
- Browser Tab 1: AWS Console → Lambda → edtech-stream-processor → Monitor → CloudWatch metrics (Duration graph visible)
- Browser Tab 2: Grafana (all panels live and green)
- Terminal Tab 2 (EC2-2): `sudo tail -f /var/log/edtech-spark.log` scrolling
- Screen layout: start on CloudWatch, switch to Grafana, switch to Terminal mid-cut

---

### [START RECORDING]

**On CloudWatch Duration graph:**

**Say:**
> "I want to highlight three specific limitations that Pipeline 1 cannot solve."

**Limitation 1 — Cold Starts:**
> "Cold starts. Every spike above the baseline is Lambda spending 200–800ms
> initialising a new execution environment. In a real EdTech scenario —
> first class of the morning — every student's first event hits a cold start.
> Pipeline 2 has no cold starts: Spark is always running, Redis is always warm,
> sub-millisecond response from the very first event."

**Limitation 2 — No Surge Detection:**
> "When 500 students join a live class simultaneously, Lambda invokes more — cost triples.
> But there is no detection that a surge is happening. No rerouting to a priority queue.
> No circuit breaker. No operator alert.
> Pipeline 2 detects surges within 40 seconds using a 5-minute rolling window,
> reroutes overflow to a dedicated priority topic, and Grafana shows the surge status live.
> I'll demonstrate that in the next cut."

**Limitation 3 — No Fault Tolerance:**
> "If Lambda threw an unhandled exception on a Kinesis batch, that batch is retried
> a fixed number of times and then dropped. Lambda is stateless — no checkpoint.
> Reprocessing yesterday's data would require rebuilding the pipeline manually.
> Pipeline 2 uses Kafka consumer offsets and Spark checkpointing.
> Any failure resumes exactly from the last committed offset. Nothing is ever lost."

---

**[Switch to Grafana]**

**Say:**
> "Pipeline 1 taught us exactly what a real analytics system needs.
> Not just storage — intelligence. Not just scale-out — the right kind of scale-out.
> Not just availability — genuine fault tolerance with zero data loss.
>
> Pipeline 2 replaces Kinesis with Kafka, Lambda with PySpark and Kubernetes,
> and adds a full intelligence layer that Pipeline 1 never had.
> Let me show you the live system."

---

**[Switch to Terminal Tab 2 — Spark logs scrolling]**

**Say:**
> "The pipeline is live. Two EC2 instances in ap-south-1.
> EC2-1 runs Kafka 4.1 KRaft mode — no ZooKeeper — and the event generator at 30 events per second.
> EC2-2 runs PySpark Structured Streaming, Redis, k3s Kubernetes, Prometheus, and Grafana.
>
> Spark pulls a micro-batch from Kafka every 10 seconds, computes 16 KPIs —
> active users, engagement, dropout rate, surge status — and writes them to Redis.
> Grafana reads Redis every 10 seconds. Dashboard is always fresh."

**[Switch to Grafana — walk panels:]**
- Events/min → ~1,476 (live)
- Active Users → live number
- Surge Status → 0 green
- Consumer Lag → near zero

**Say:**
> "20 panels, all live. Consumer lag near zero — perfectly keeping up."

**[Tab 2 EC2-2 — type:]**
```bash
redis-cli get pipeline:active_users
redis-cli get pipeline:events_per_minute
redis-cli get pipeline:dropout_rate
redis-cli get pipeline:avg_engagement_score
```
**Say:**
> "Redis is the hot storage layer — sub-millisecond reads directly from Spark.
> Active users, events per minute, dropout rate, engagement score — all updated every batch.
> 15 live keys, all written by Spark, read by Grafana every 10 seconds."

**[Tab 1 EC2-1 — type:]**
```bash
/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092

/opt/kafka/bin/kafka-topics.sh --describe --topic edtech-events \
  --bootstrap-server localhost:9092

/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe --group edtech-storage-group 2>/dev/null | head -12
```
**Say:**
> "Two topics: edtech-events with 10 partitions — compare to Pipeline 1's single Kinesis stream.
> edtech-priority handles surge overflow.
> Two independent consumer groups read every event: one for real-time Redis, one for durable storage.
> Both at near-zero lag. Fully caught up."

### [STOP RECORDING — CUT 2 DONE]

---
---

# CUT 3 — Surge Detection (Live)
## Duration: ~4 min | Wait: ~40s for SURGE log line

**Before pressing record:**
- Left half screen: Terminal Tab 2 (EC2-2) — `sudo tail -f /var/log/edtech-spark.log` running
- Right half screen: Grafana — Surge Status panel showing 0 (green)
- Terminal Tab 1 (EC2-1): ready to type
- Verify generator at 30 ev/s: `pgrep -a -f generator.py`

---

### [START RECORDING]

**Say:**
> "This is the intelligence Pipeline 1 never had.
> Normal state: 30 events per second. I'm tripling the rate to 90 right now.
> Watch the Spark logs on the left and the Surge Status panel on Grafana.
> Pipeline 1 would have just invoked Lambda three times as often — no detection at all."

**Tab 1 (EC2-1):**
```bash
pkill -f "generator.py" 2>/dev/null; sleep 1
nohup python3 /home/ubuntu/generator.py \
  --bootstrap-servers localhost:9092 --rate 90 --users 500 \
  >> /tmp/gen.log 2>&1 &
echo "Generator at 90 ev/s — $(date -u)"
```

**[While waiting ~40 seconds — keep talking:]**
> "SurgeDetector maintains a 5-minute rolling window of per-second event rates.
> Current batch rate at 90 ev/s produces around 229 events per micro-batch window.
> The threshold is 1.5 times the rolling average — approximately 187.
> We are at 229. It will trigger any moment now."

**[Watch for in Spark logs:]**
```
INFO  SURGE detected: current_rate=229.1 rolling_avg=124.8 (hold=6 batches)
INFO  SurgeDetector: forwarded 1200 events → edtech-priority
```

**[Point to Grafana — Surge Status turning 1 / red:]**
> "SURGE DETECTED. 1.84 times above baseline — past the 1.5 threshold.
> 1,200 overflow events routed to the priority topic.
> Consumer lag stays bounded. The monitoring path is completely unaffected.
> This is the workload-aware intelligence Pipeline 1 was missing entirely."

**Verify priority topic (Tab 1 EC2-1):**
```bash
/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 \
  --topic edtech-priority
# Expected: offsets > 0 on all 3 partitions
```

**Restore rate:**
```bash
pkill -f "generator.py" 2>/dev/null; sleep 1
nohup python3 /home/ubuntu/generator.py \
  --bootstrap-servers localhost:9092 --rate 30 --users 500 \
  >> /tmp/gen.log 2>&1 &
echo "Restored to 30 ev/s — $(date -u)"
```

**Say:**
> "Rate is back to 30. Surge Status panel stays active for about 4 more minutes —
> SurgeDetector holds state for 6 batches minimum to prevent oscillation.
> It clears automatically. Zero manual action needed."

### [STOP RECORDING — CUT 3 DONE]
### ⚠️ WAIT: do NOT start Cut 4 until Surge Status on Grafana returns to 0 (green).

---
---

# CUT 4 — Intentional Failures + Automatic Recovery
## Duration: ~8 min | Professor requirement — do NOT cut mid-failure

**Before pressing record:**
- Left half: Terminal Tab 2 (EC2-2) — `sudo tail -f /var/log/edtech-spark.log`
- Right half: Grafana — all panels live and green
- Terminal Tab 1 (EC2-1): ready
- Terminal Tab 3 (EC2-2 kubectl): `export KUBECONFIG=~/.kube/edtech-k3s.yaml` already run

**Final check before record:**
```bash
# EC2-2
systemctl status edtech-spark --no-pager | head -3   # active (running)
redis-cli ping                                         # PONG
export KUBECONFIG=~/.kube/edtech-k3s.yaml
kubectl get pods -n edtech-pipeline                    # exactly 1 Running pod
# EC2-1
pgrep -a -f generator.py                               # running at rate 30
```

---

### [START RECORDING]

**Say:**
> "The professor requirement: intentionally break the pipeline at three different layers
> and show automatic recovery in each case.
> Zero manual intervention after the initial kill command — the system heals itself."

---

### FAILURE 1 — Generator Killed (Ingestion Layer)

**Say:**
> "Failure 1: killing the event generator — simulating a producer outage.
> In Pipeline 1, if the generator stopped, events just stopped. No guarantee of recovery.
> Watch what happens in Pipeline 2."

**Tab 1 (EC2-1):**
```bash
pkill -f "generator.py"
echo ">>> Generator KILLED at $(date -u)"
```

**[Watch Spark logs — batches drain over ~60–80 seconds:]**
```
Batch 251: 1157 events    ← draining Kafka backlog
Batch 252:  113 events
Batch 253:    0 events    ← idle
```

**Say while watching:**
> "Kafka is retaining every event — 24-hour retention.
> Spark is draining the backlog in order: 1,157... 113... zero. Nothing lost."

**[~2 minutes later — Grafana panels stop updating, values are frozen]**

**Say:**
> "Grafana is now showing stale values — the numbers are frozen, no longer ticking up.
> Redis keys have expired. The pipeline has stopped producing new data.
> Grafana holds the last known value rather than showing errors to the user."

**Show checkpoint (Tab 3 EC2-2):**
```bash
  cat /tmp/edtech-checkpoints/stream/offsets/$(ls           
  /tmp/edtech-checkpoints/stream/offsets/ | sort -n | tail -1)
```
**Expected:**
```json
{"edtech-events":{"0":18420,"1":18380,"2":18410,...}}
```
**Say:**
> "This 8 KB file is the Spark checkpoint — last committed Kafka offset per partition.
> When Spark restarts, it reads this and knows exactly where to resume.
> Pipeline 1 had no equivalent. Lambda was stateless."

**Restart generator (Tab 1 EC2-1):**
```bash
nohup python3 /home/ubuntu/generator.py \
  --bootstrap-servers localhost:9092 --rate 30 --users 500 \
  >> /tmp/gen.log 2>&1 &
echo ">>> Generator RESTARTED at $(date -u)"
```

**[First batch fires within 5 seconds — Grafana comes back live]**

**Say:**
> "5 seconds to full recovery. Resumed from checkpoint, not from the beginning.
> Effectively-once processing. Zero events lost. Zero human intervention."

---

### FAILURE 2 — Spark Process Crash (Processing Layer)

**Say:**
> "Failure 2: crashing Spark itself with SIGKILL — a simulated process crash.
> systemd is configured to restart the service on any non-zero exit code.
> Watch it bring Spark back without me typing a single restart command."

**Tab 2 (EC2-2) — hard kill the JVM:**
```bash
sudo kill -9 $(pgrep -f SparkSubmit)
echo ">>> Spark SIGKILL at $(date -u)"
```

**[Logs stop immediately]**

**Say:**
> "Spark is gone. Kafka is still running — events are accumulating. Watch the lag."

**Tab 1 (EC2-1) — show lag building in the storage group:**
```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --describe --group edtech-storage-group
# LAG column should be rising — events accumulating while Spark is down
```

**[Watch Tab 2 — systemd auto-restarts within ~15 seconds:]**
```
INFO  Starting Prometheus HTTP server on port 8000
INFO  Initialising SparkSession
INFO  Resuming from Kafka offsets: {partition 0: 18420, ...}
INFO  Batch 252: 3847 events    ← catch-up batch
INFO  Batch 252 complete in 44.1s
```

**Say:**
> "systemd detected the non-zero exit code and restarted the service.
> Spark read the checkpoint, resumed from the last committed offset,
> processed everything that built up during the outage in one catch-up batch.
> Grafana panels are back. Zero events lost. I typed one command — the kill — and nothing else."

---

### FAILURE 3 — Storage Pod Force-Killed (Worker Layer)

**[Switch left half to Tab 3 (kubectl watch)]**
```bash
export KUBECONFIG=~/.kube/edtech-k3s.yaml
kubectl get pods -n edtech-pipeline -w
```

**Tab 2 (EC2-2) — force-kill the pod:**
```bash
export KUBECONFIG=~/.kube/edtech-k3s.yaml
POD=$(kubectl get pods -n edtech-pipeline --no-headers \
  -o custom-columns=":metadata.name" | head -1)
echo ">>> Force-killing pod: $POD at $(date -u)"
kubectl delete pod $POD -n edtech-pipeline --grace-period=0 --force
```

**[Watch pod watch:]**
```
edtech-storage-worker-xxxx-aaa   1/1   Terminating         0   12m
edtech-storage-worker-xxxx-bbb   0/1   ContainerCreating   0   3s
edtech-storage-worker-xxxx-bbb   1/1   Running             0   38s
```

**Say:**
> "k3s replaced the pod immediately. Kafka rebalanced — new pod picks up from
> the last committed offset. Events in the killed pod's buffer were not yet committed,
> so Kafka replays them to the new pod. Zero events lost."

**Check new pod logs:**
```bash
NEW_POD=$(kubectl get pods -n edtech-pipeline --no-headers \
  -o custom-columns=":metadata.name" | head -1)
kubectl logs -n edtech-pipeline $NEW_POD --tail=8
# Expected: "Kafka consumer started", "Assigned partitions: [...]", "Buffering events..."
```

**Now show graceful SIGTERM path — note S3 file count before:**
```bash
aws s3 ls s3://edtech-pipeline-data-c956dd9c/raw/ --recursive | wc -l
```

**Delete without --force (SIGTERM fires, 60s grace period to flush):**
```bash
kubectl delete pod $NEW_POD -n edtech-pipeline
```

**Watch pod go Terminating → new pod created (on the -w pane):**
```
edtech-storage-worker-xxxx   1/1   Terminating   0   2m
edtech-storage-worker-yyyy   0/1   Pending       0   0s
edtech-storage-worker-yyyy   1/1   Running       0   38s
```

**After new pod is Running — check S3 file count again:**
```bash
aws s3 ls s3://edtech-pipeline-data-c956dd9c/raw/ --recursive | wc -l
# Count should be higher — the terminating pod flushed its buffer to S3
```

**Say:**
> "SIGTERM — the graceful path used during rolling updates and scale-in.
> The pod had 60 seconds to drain its buffer before k8s force-killed it.
> You can see the S3 file count increased — the buffered events were written
> to S3 before the pod exited. Zero events dropped.
>
> Two failure modes on this single layer — SIGKILL uses Kafka offset replay,
> SIGTERM uses the buffer drain. Both result in zero data loss.
> Three layers broken and recovered. Zero events lost in any scenario.
> This is the fault tolerance Pipeline 1 simply could not provide."

### [STOP RECORDING — CUT 4 DONE]
### ⚠️ WAIT: let pods settle back to 1 Running before starting Cut 5. Check with:
### `kubectl get pods -n edtech-pipeline`

---
---

# CUT 5 — KEDA Auto-Scaling + EC2 ASG + Latency NFR
## Duration: ~7 min | Wait: ~40s for 10 pods

**Before pressing record:**
- Left half: Terminal Tab 3 (EC2-2 kubectl) — `export KUBECONFIG=~/.kube/edtech-k3s.yaml`
- Right half: Grafana — live
- Terminal Tab 4 (Local Mac): `cd /Users/krishnaagrawal/Desktop/CloudProject/edtech-pipeline-2`
- Browser AWS Console: ready (needed mid-cut for ASG section)
- Confirm 1 Running pod: `kubectl get pods -n edtech-pipeline`

---

### [START RECORDING]

**Tab 3 (kubectl watch):**
```bash
export KUBECONFIG=~/.kube/edtech-k3s.yaml
kubectl get pods -n edtech-pipeline
# Show: 1 Running pod

kubectl get pods -n edtech-pipeline -w
# Leave running — watch pods appear
```

**Say:**
> "One storage pod at baseline — all we need at 30 events per second.
> KEDA polls Kafka consumer lag every 15 seconds.
> Scaling formula: ceil(totalLag / 100). Lag of 500 = 5 pods. Lag of 1,000 = 10 pods.
> Maximum is 10 — one pod per Kafka partition. Perfect horizontal distribution.
>
> Compare to Pipeline 1: Lambda invoked more on traffic spikes with no upper bound,
> no lag concept, cost growing linearly. KEDA uses the right metric and stops at 10."

**Tab 4 (Local Mac):**
```bash
EC2_1_IP=3.7.163.17 bash scripts/test_10x_load.sh spike
```

**[Watch pods appear at T+20s and T+40s]**

**At T+20s:**
> "First KEDA poll — lag crossed threshold, scaling to 5 pods."

**At T+40s — all 10 Running:**
> "40 seconds. 10 pods. Each owns exactly one Kafka partition.
> Consumer lag collapsing to zero. Pipeline absorbing 10× load."

```bash
kubectl top pods -n edtech-pipeline 2>/dev/null || echo "metrics-server not available — skip"
# If works: shows ~15m CPU, ~107Mi RAM per pod
```

**Say:**
> "107 MB RAM per pod — versus 1.5 GB for a Spark JVM.
> 15 milliCPU each — storage pods are I/O-bound, not CPU-bound.
> This is why CPU-based scaling like Lambda auto-scaling would have been the wrong signal.
> KEDA uses Kafka lag — the correct metric."

**Restore:**
```bash
EC2_1_IP=3.7.163.17 bash scripts/test_10x_load.sh normal
```

**Say:**
> "Back to 30 ev/s. KEDA scales back to 1 pod after the 120-second cooldown.
> Zero idle pods. Zero wasted compute at baseline."

---

### EC2 ASG — Tier 2 Scaling

**[Switch to AWS Console]**
```
CloudWatch → Alarms → edtech-consumer-high-cpu
Show: threshold = 70%, comparison = GreaterThanThreshold, 2 consecutive datapoints
```
```
EC2 → Auto Scaling Groups → edtech-consumer-asg
Show: Min=0, Max=3, Desired=0 — zero idle cost at baseline
```

**Say:**
> "Tier 2 — infrastructure level.
> The CloudWatch alarm is configured: EC2-2 CPU above 70% for two consecutive minutes.
> At 300 events per second, CPU peaked at around 65 to 70 percent — storage workers
> are I/O-bound, not CPU-bound, so a single t3.medium handles this load.
> But at true production scale — say a thousand events per second — KEDA maxes
> at 10 pods, all 10 are running, CPU saturates.
> CloudWatch fires this alarm. ASG launches a new t3.medium,
> it auto-joins k3s via the SSM bootstrap token, and the Pending pods
> that couldn't schedule now have a node to land on.
> Two-tier scaling: pods scale in 40 seconds, new nodes in 3 minutes.
> The ASG sits at zero desired capacity at baseline — zero idle EC2 cost.
> Pipeline 1 had no infrastructure-level scaling at all."

---

### Latency NFR — <5 seconds

**[Switch back to Terminal Tab 2 (EC2-2)]**
```bash
watch -n 1 "redis-cli get pipeline:last_updated && redis-cli get pipeline:active_users"
```
**[Timestamp changes every ~33–38s when Spark commits a batch]**

**Say:**
> "The <5 second NFR is for the dashboard read path — Redis to Grafana.
> From the moment Spark writes to Redis, Grafana reads the new value
> in under 5 milliseconds."

```bash
redis-cli --latency -h localhost -p 6379
# Expected: min: 0, max: 1, avg: 0.19 — all under 1ms
```

**Say:**
> "Sub-millisecond Redis reads. Grafana auto-refreshes every 10 seconds.
> Dashboard users see fresh data within 5 seconds of Redis being updated.
>
> Compare Pipeline 1: Lambda cold starts added 200–800ms latency per invocation.
> QuickSight dashboards refreshed on a schedule — not real-time at all.
>
> The Spark micro-batch itself is 33–38 seconds on this shared t3.medium — a known
> trade-off documented in the architecture. On a dedicated Spark cluster it drops to 3–5s.
> We separated hot and cold paths precisely for this: Redis for instant reads,
> Spark feeds it every batch."

### [STOP RECORDING — CUT 5 DONE]

---
---

# CUT 6 — Storage Comparison + Closing
## Duration: ~4 min | No waits

**Before pressing record:**
- Terminal Tab 2 (EC2-2): ready to type
- Browser: Grafana — all panels stable and live
- Browser: AWS Console ready (for DynamoDB navigation mid-cut)

---

### [START RECORDING]

**Tab 2 (EC2-2):**
```bash
aws s3 ls s3://edtech-pipeline-data-c956dd9c/raw/ --recursive | sort | tail -10
# UUID-suffix = k8s pod writes, no UUID = Spark AdaptiveBatchOptimizer writes
```
**Say:**
> "Pipeline 2 S3: Parquet files partitioned by year/month/day/hour.
> UUID-suffixed files from k8s pods, no-UUID from Spark's AdaptiveBatchOptimizer."

```bash
aws s3 ls s3://edtech-pipeline-data-c956dd9c/raw/ --recursive | awk '{print $3}' | sort -n | tail -5
# Shows file sizes — each ~500 KB
```
**Say:**
> "Each file roughly 500 KB — 67 times larger than Pipeline 1's per-invocation JSON files.
> Better compression, faster batch reads, far fewer S3 PUT calls."

```bash
# Count Pipeline 2 files (large Parquet, few files)
aws s3 ls s3://edtech-pipeline-data-c956dd9c/raw/ --recursive | wc -l

# Count Pipeline 1 files (tiny JSON, many files)
aws s3 ls s3://edtech-raw-data/raw/ --recursive | wc -l
```
**Say:**
> "The difference in file count is the 87.6% PUT reduction made visible.
> Pipeline 2 has far fewer files because the k8s storage pods buffer up to 5,000 events
> before writing — one large Parquet file instead of dozens of tiny JSON files.
> Fewer S3 PUT calls means lower cost and faster Athena-equivalent queries."

**[AWS Console — DynamoDB]**
```
DynamoDB → Tables → UserStats → Explore items
DynamoDB → Tables → CourseStats → Explore items
DynamoDB → Tables → DailyReports → Explore items
```
**Say:**
> "Three Pipeline 2 DynamoDB tables. UserStats and CourseStats updated live by k8s pods
> using atomic ADD — multiple pods write concurrently, no overwrites.
> DailyReports populated by the nightly batch job at midnight UTC — Spark SQL on all
> that day's Parquet data written to DynamoDB for fast queries.
> Pipeline 1 had one table written on every single event — no batching, no daily summaries."

---

### CLOSING

**[Switch to Grafana — all panels live and green]**

**Say:**
> "Everything you saw was live — real AWS infrastructure, real events, real failures.
>
> Pipeline 1 worked. Easy to deploy, zero operational overhead, fine at 30 events per second.
> But no intelligence, no fault tolerance with replay, no workload-aware scaling,
> and the small file problem made storage increasingly expensive at scale.
>
> Pipeline 2 addresses every one of those gaps:
>
> Surge detection: triggered and cleared automatically — consumer lag stayed bounded.
> Three intentional failures across ingestion, processing, and worker layers —
> every one recovered automatically. Zero events lost in any scenario.
> KEDA scaling: 1 pod to 10 in 40 seconds under 10× load, back to 1 at baseline.
> Latency: sub-millisecond Redis reads versus 200–800ms Lambda cold starts.
> Storage cost: 87.6% PUT reduction — k8s pods batch 5,000 events per S3 write versus Lambda's one file per invocation.
> Uptime: 99.9918% measured over a 17-hour continuous run.
>
> The core insight is architectural: Lambda is stateless by design —
> intelligence and fault tolerance are impossible at the processing layer.
> Moving to stateful stream processing with Kafka and Spark unlocks both:
> checkpointing, surge detection, and cost optimization as first-class features,
> not bolt-ons."

### [STOP RECORDING — CUT 6 DONE — ALL CUTS COMPLETE]

---
---

# EMERGENCY COMMANDS

**Spark crashes unexpectedly (not during Failure 2):**
```bash
sudo systemctl start edtech-spark
sudo tail -f /var/log/edtech-spark.log
```

**Generator dies unexpectedly:**
```bash
EC2_1_IP=3.7.163.17 bash scripts/test_10x_load.sh normal
```

**Redis is down:**
```bash
sudo systemctl restart redis 2>/dev/null || sudo systemctl restart redis-server
redis-cli ping
```

**Grafana / Prometheus down:**
```bash
cd /home/ubuntu/edtech-pipeline-2
docker compose -f docker-compose.monitoring.yml up -d
```

**kubectl says no config:**
```bash
export KUBECONFIG=~/.kube/edtech-k3s.yaml
```

**Pods stuck in Pending after Cut 4:**
```bash
kubectl describe pods -n edtech-pipeline | grep -A5 "Events:"
kubectl get nodes
```

**Athena query returns no results:**
```
AWS Glue → Crawlers → run crawler on edtech-raw-data → wait for it to finish → retry Athena query
```

**Spark kill didn't work (pgrep returned nothing):**
```bash
sudo kill -9 $(ps aux | grep -i spark | grep -v grep | awk '{print $2}')
```

---

# QUICK REFERENCE

| Cut | Content | Duration | Wait inside | Covers |
|---|---|---|---|---|
| 1 | Intro + Pipeline 1 AWS Tour | ~6 min | none | P1 architecture, each component live |
| 2 | P1 Limitations + Transition + P2 Running | ~4 min | none | Cold starts, no surge, no replay; P2 live |
| 3 | Surge Detection | ~4 min | ~40s for SURGE log | Intelligence, SLA-aware routing |
| 4 | 3 Failures + Recovery | ~8 min | F1: ~2 min; F2: ~15s; F3: ~38s | Professor requirement, fault tolerance |
| 5 | KEDA Scaling + ASG + Latency | ~7 min | ~40s for 10 pods | Auto-scale, 2-tier scaling, <5s NFR |
| 6 | Storage Comparison + Closing | ~4 min | none | Cost efficiency, P1 vs P2 summary |
| **Total** | | **~33 min** | | **All PDF requirements covered** |

**Between cuts:**
- Always run the health check at the top of this file
- After Cut 3: wait for Surge Status on Grafana → 0 before Cut 4
- After Cut 4: wait for `kubectl get pods` to show 1 Running before Cut 5
