# Screenshots Guide — CSG527 EdTech Pipeline
## 24 Screenshots with Exact Commands & What to Capture

**Before starting**: Confirm all services are healthy.
```bash
EC2_1="3.7.163.17"
EC2_2="52.66.142.159"
KEY="~/Desktop/CloudProject/edtech-pipeline-2/kafka_key.pem"

# Check EC2-2 services
ssh -i $KEY ubuntu@$EC2_2 \
  'systemctl is-active edtech-spark redis && kubectl get pods -n edtech-pipeline'
# Expected: active / active / 1+ pods Running
```

---

## GROUP 1 — AWS Console (5 screenshots)

### SS-01: EC2 Instances Overview
**Where**: AWS Console → EC2 → Instances (ap-south-1)

**Verify from CLI:**
```bash
aws ec2 describe-instances --region ap-south-1 \
  --query 'Reservations[*].Instances[*].{Name:Tags[?Key==`Name`]|[0].Value,Type:InstanceType,State:State.Name,IP:PublicIpAddress}' \
  --output table
```

**What to capture**: Both instances **running** — EC2-1 (t2.medium, 3.7.163.17) and EC2-2 (t3.medium, 52.66.142.159).

---

### SS-02: S3 Bucket Overview
**Where**: AWS Console → S3 → edtech-pipeline-data-c956dd9c

**Verify from CLI:**
```bash
aws s3 ls s3://edtech-pipeline-data-c956dd9c/ --region ap-south-1
# Expected: PRE raw/
```

**What to capture**: Bucket name, region ap-south-1, `raw/` folder visible.

---

### SS-03: S3 Parquet Files (partitioned structure, pod writes)
**Where**: AWS Console → S3 → edtech-pipeline-data-c956dd9c → raw/ → year=2026/ → month=04/

**Verify from CLI:**
```bash
# Show files — pod writes have UUID suffix: {epoch}_{uuid8}.parquet
aws s3 ls "s3://edtech-pipeline-data-c956dd9c/raw/year=2026/month=04/" \
  --recursive --region ap-south-1 | tail -15
```

**What to capture**: Partitioned folder path (`year=2026/month=04/day=XX/hour=XX/`) and several `.parquet` files. Files from k8s pods have UUID suffix (e.g. `1776377657_6b9ac8dd.parquet`).

---

### SS-04: DynamoDB Tables List
**Where**: AWS Console → DynamoDB → Tables (ap-south-1)

**Verify from CLI:**
```bash
aws dynamodb list-tables --region ap-south-1
# Expected: {"TableNames": ["CourseStats", "DailyReports", "UserStats"]}
```

**What to capture**: All 3 tables — `UserStats`, `CourseStats`, `DailyReports` — all **Active**, PAY_PER_REQUEST.

---

### SS-05: DynamoDB DailyReports — Batch Job Output
**Where**: AWS Console → DynamoDB → DailyReports → Explore items

**Verify from CLI:**
```bash
python3 -c "
import boto3, json
d = boto3.resource('dynamodb', region_name='ap-south-1')
r = d.Table('DailyReports').get_item(Key={'report_date': '2026-04-13'})
item = r.get('Item', {})
print('report_date:', item.get('report_date'))
print('total_events:', item.get('total_events'))
print('daily_active_users:', item.get('daily_active_users'))
print('pass_rate_by_difficulty:', item.get('pass_rate_by_difficulty'))
"
```
**Expected**: `total_events: 267759`, `daily_active_users: 499`, `pass_rate_by_difficulty: {easy: 85.1, medium: 75.4, hard: 64.4}`

**What to capture**: Item detail showing `report_date`, `total_events: 267759`, `daily_active_users: 499`.

---

## GROUP 2 — Kafka (3 screenshots)

### SS-06: Kafka Topics List
**Command:**
```bash
ssh -i $KEY ubuntu@$EC2_1 \
  '/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list'
```
**Expected:**
```
edtech-events
edtech-priority
```

**What to capture**: Both topics listed.

---

### SS-07: Kafka Topic Details (partition counts)
**Command:**
```bash
ssh -i $KEY ubuntu@$EC2_1 \
  '/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
   --describe --topic edtech-events && \
   /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
   --describe --topic edtech-priority'
```
**Expected**: `edtech-events` PartitionCount: **10**, `edtech-priority` PartitionCount: **3**

**What to capture**: Both describe outputs — 10 partitions and 3 partitions.

---

### SS-08: Kafka Consumer Group + Lag (storage-group)
**Command:**
```bash
ssh -i $KEY ubuntu@$EC2_1 \
  '/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list'
```
**Expected output:**
```
edtech-storage-group
```
Note: Only `edtech-storage-group` appears. Spark (`stream_consumer.py`) uses
checkpoint-based offset management — it does not register a traditional Kafka consumer
group visible via this command.

Then show lag:
```bash
ssh -i $KEY ubuntu@$EC2_1 \
  '/opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
   --describe --group edtech-storage-group'
```
**Expected**: 10 rows (one per partition), LAG column near 0.

**What to capture**: `edtech-storage-group` listed + lag table showing near-zero lag across all 10 partitions.

---

## GROUP 3 — Kubernetes + KEDA (4 screenshots)

**Prerequisites**: SSH to EC2-2 and confirm k3s is running.
```bash
ssh -i $KEY ubuntu@$EC2_2
export KUBECONFIG=/etc/rancher/k3s/k3s.yaml   # or ~/.kube/edtech-k3s.yaml
```

### SS-09: k3s Pod Status (normal 1-pod baseline)
**Command:**
```bash
kubectl get pods -n edtech-pipeline -o wide
```
**Expected at 30 ev/s baseline:**
```
NAME                                     READY   STATUS    RESTARTS   NODE
edtech-storage-worker-XXXXXX-XXXXX       1/1     Running   0          ec2-2-...
```

**What to capture**: 1 pod Running. Include the `-o wide` output to show node name.

---

### SS-10: KEDA ScaledObject Status
**Command:**
```bash
kubectl get scaledobject -n edtech-pipeline
kubectl describe scaledobject edtech-storage-worker -n edtech-pipeline | head -40
```
**Expected:**
```
NAME                    SCALETARGETKIND      SCALETARGETNAME         MIN   MAX   TRIGGERS   READY   ACTIVE
edtech-storage-worker   apps/Deployment      edtech-storage-worker   1     10    kafka      True    True
```

**What to capture**: ScaledObject showing READY=True, TRIGGERS=kafka, MIN=1 MAX=10.

---

### SS-11: KEDA Scaling in Action (pods scaling up to 10)
**Steps:**
```bash
# On your Mac — start 10x load on EC2-1
ssh -i $KEY ubuntu@$EC2_1 \
  'pkill -f generator.py; nohup python3 ~/generator.py \
   --bootstrap-servers localhost:9092 --topic edtech-events \
   --users 500 --rate 300 > /tmp/gen.log 2>&1 &'

# On EC2-2 — watch pods scale up in real time
kubectl get pods -n edtech-pipeline -w
```
**Expected timeline:**
```
edtech-storage-worker-xxx   1/1   Running   → (after ~20s)
edtech-storage-worker-yyy   0/1   Pending
edtech-storage-worker-zzz   0/1   ContainerCreating
...                         (after ~40s: 10 pods Running)
```

**What to capture**: The `-w` watch output showing pods scaling from 1 → 5 → 10 with timestamps.

**After screenshot, restore normal rate:**
```bash
ssh -i $KEY ubuntu@$EC2_1 \
  'pkill -f generator.py; nohup python3 ~/generator.py \
   --bootstrap-servers localhost:9092 --topic edtech-events \
   --users 500 --rate 30 > /tmp/gen.log 2>&1 &'
```

---

### SS-12: storage_worker Pod Logs (S3 flush proof)
**Command:**
```bash
# Get first pod name
POD=$(kubectl get pods -n edtech-pipeline -o name | head -1)
kubectl logs $POD -n edtech-pipeline | grep -E "flush|S3|DynamoDB|events" | tail -20
```
**Expected:**
```
INFO Flushed 5123 events to s3://edtech-pipeline-data-c956dd9c/raw/year=2026/month=04/day=17/hour=10/1776XXXXXX_6b9ac8dd.parquet
INFO DynamoDB: updated 487 users, 10 courses
```

**What to capture**: Log lines showing S3 flush with the UUID-suffixed filename and DynamoDB updates.

---

## GROUP 4 — Grafana Dashboard (6 screenshots)

**Open Grafana**: http://52.66.142.159:3000 (login: admin / edtech2026)

**Before screenshots, ensure generator is running at 30 ev/s:**
```bash
ssh -i $KEY ubuntu@$EC2_1 \
  'ps aux | grep generator | grep -v grep || \
   nohup python3 ~/generator.py --bootstrap-servers localhost:9092 \
   --topic edtech-events --users 500 --rate 30 > /tmp/gen.log 2>&1 &'
# Wait 30 seconds for panels to populate
```

---

### SS-13: Grafana — Full Dashboard (all 20 panels)
Zoom browser to 67%, scroll to show all 4 sections.

**What to capture**: All 20 panels visible — Overview, Student Engagement, Learning Performance, System Health. Live data updating.

---

### SS-14: Grafana — Overview Section
**What to capture**: Top 4 panels:
- Total Events Processed (267,000+)
- Events/min (~1,800 at 30 ev/s)
- Active Users (~395)
- Kafka Consumer Lag (~0 at steady state)

---

### SS-15: Grafana — Student Engagement + Learning Performance
**What to capture**:
- Online Users by Region (Mumbai, Bengaluru, etc.)
- Device Breakdown (mobile ~52%, desktop ~30%, tablet ~18%)
- Avg Engagement Score (~0.696)
- Dropout Rate (~3.8%)

---

### SS-16: Grafana — System Health Section
**What to capture**:
- S3 Cost Savings gauge (**87.6%** — key metric)
- Spark Batch Duration (~2s on t3.medium, STORAGE_ENABLED=false)
- Surge Detected = **0 (OFF)** at steady state

---

### SS-17: Grafana — During Surge (SURGE ACTIVE)
**Steps to trigger surge:**
```bash
ssh -i $KEY ubuntu@$EC2_1 \
  'pkill -f generator.py; nohup python3 ~/generator.py \
   --bootstrap-servers localhost:9092 --topic edtech-events \
   --users 500 --rate 90 > /tmp/gen_spike.log 2>&1 &'
# Wait ~40 seconds for surge detection
```

**Verify surge detected:**
```bash
ssh -i $KEY ubuntu@$EC2_2 \
  'curl -s http://localhost:8000/metrics | grep surge_detected'
# Expected: pipeline_surge_detected 1.0
```

**What to capture**: Grafana showing Surge Detected = **ACTIVE (1.0)**, Events/min at ~5,400, Consumer Lag bounded.

**After screenshot, restore:**
```bash
ssh -i $KEY ubuntu@$EC2_1 \
  'pkill -f generator.py; nohup python3 ~/generator.py \
   --bootstrap-servers localhost:9092 --topic edtech-events \
   --users 500 --rate 30 > /tmp/gen.log 2>&1 &'
```

---

### SS-18: Grafana — KEDA Scaling Panel (optional, if panel added)
If the dashboard has a pod count panel:

**What to capture**: Pod count rising from 1→10 during 10× load. Otherwise use `kubectl get pods -w` output (SS-11).

---

## GROUP 5 — Prometheus Metrics (2 screenshots)

### SS-19: Prometheus Metrics Endpoint (Spark — port 8000)
**URL**: http://52.66.142.159:8000/metrics

**Or from terminal:**
```bash
curl http://52.66.142.159:8000/metrics | grep pipeline_
```
**Expected key metrics:**
```
pipeline_events_processed_total 267759.0
pipeline_kafka_consumer_lag 0.0
pipeline_active_users_gauge 395.0
pipeline_surge_detected 0.0
pipeline_s3_cost_reduction_pct 87.6
```

**What to capture**: `pipeline_*` metrics visible. Highlight `pipeline_events_processed_total`, `pipeline_surge_detected`, `pipeline_s3_cost_reduction_pct`.

---

### SS-20: storage_worker Metrics Endpoint (k8s pods — port 8001)
**From EC2-2:**
```bash
ssh -i $KEY ubuntu@$EC2_2
# Get pod IP (or port-forward)
kubectl port-forward -n edtech-pipeline deploy/edtech-storage-worker 8001:8001 &
curl http://localhost:8001/metrics | grep worker_
```
**Expected:**
```
worker_events_processed_total 45230.0
worker_s3_flushes_total 9.0
worker_kafka_lag 0.0
worker_buffer_size 1234.0
worker_dynamo_writes_total 9.0
```

**What to capture**: `worker_*` metrics — proves the k8s pods have their own separate metrics pipeline.

---

## GROUP 6 — Spark Logs (3 screenshots)

### SS-21: Spark Service Status + Live Log
**Command:**
```bash
ssh -i $KEY ubuntu@$EC2_2 \
  'systemctl status edtech-spark --no-pager -l | head -20 && \
   echo "=== LAST 15 LOG LINES ===" && \
   sudo tail -15 /var/log/edtech-spark.log'
```
**Expected:**
```
● edtech-spark.service
   Active: active (running) since ...
   Environment: STORAGE_ENABLED=false
...
=== LAST 15 LOG LINES ===
XX:XX:XX  INFO  Batch 28X: 300 events
XX:XX:XX  INFO  Batch 28X complete in 2.1s
```

**What to capture**: Service `Active: active (running)` and `STORAGE_ENABLED=false` in environment.

---

### SS-22: Spark Log — Surge Detection Lifecycle
**Command:**
```bash
ssh -i $KEY ubuntu@$EC2_2 \
  'sudo grep -E "SURGE detected|forwarded.*edtech-priority|SURGE hold|surge=False" \
   /var/log/edtech-spark.log | head -20'
```
**Expected:**
```
07:28:27  INFO  SURGE detected: current_rate=229.1 rolling_avg=124.8 (hold=6 batches)
07:28:29  INFO  SurgeDetector: forwarded 1200 events → edtech-priority
07:29:01  INFO  SURGE hold: 5 batches remaining (current_rate=178.6)
07:33:19  INFO  surge=False — NORMAL restored (current_rate=104.4 rolling_avg=130.0)
```

**What to capture**: Full surge lifecycle — DETECTED → forwarded 1200 → hold → NORMAL.

---

### SS-23: Spark Log — Failure Recovery
**Command:**
```bash
ssh -i $KEY ubuntu@$EC2_2 \
  'sudo grep -E "Batch 25[0-9]:" /var/log/edtech-spark.log | tail -5'
```
**What to capture**: Last batch before idle, then first batch after restart — 5 seconds gap proving checkpoint recovery.

If log has rotated, trigger fresh recovery:
```bash
ssh -i $KEY ubuntu@$EC2_1 'pkill -f generator.py'
# Wait 30s, then:
ssh -i $KEY ubuntu@$EC2_1 \
  'nohup python3 ~/generator.py --bootstrap-servers localhost:9092 \
   --topic edtech-events --users 500 --rate 30 > /tmp/gen.log 2>&1 &'
ssh -i $KEY ubuntu@$EC2_2 'sudo tail -f /var/log/edtech-spark.log'
# Watch for "Batch XXX: X events" appearing ~5 seconds after generator starts
```

---

### SS-24: ASG + CloudWatch (optional — AWS Console)
**Where**: AWS Console → EC2 → Auto Scaling Groups → edtech-consumer-asg

**What to capture**: ASG showing `Desired: 0` (or 1+ during load test), CloudWatch alarm `edtech-consumer-high-cpu` showing threshold 70% on EC2-2.

---

## Screenshot Checklist

| # | Screenshot | Key Thing to Show | Done |
|---|---|---|---|
| SS-01 | EC2 Instances | Both running — EC2-1 t2.medium, EC2-2 t3.medium | ☐ |
| SS-02 | S3 Bucket Overview | Bucket name, region, raw/ folder | ☐ |
| SS-03 | S3 Parquet Files | Partitioned path + UUID-suffixed pod files | ☐ |
| SS-04 | DynamoDB Tables | All 3 tables Active, PAY_PER_REQUEST | ☐ |
| SS-05 | DynamoDB DailyReports | 267,759 events, 499 users | ☐ |
| SS-06 | Kafka Topics List | edtech-events + edtech-priority | ☐ |
| SS-07 | Kafka Topic Describe | 10 partitions + 3 partitions | ☐ |
| SS-08 | Kafka Consumer Groups | Both groups listed, storage-group lag ~0 | ☐ |
| SS-09 | k3s Pod Status | 1 pod Running at baseline | ☐ |
| SS-10 | KEDA ScaledObject | READY=True, kafka trigger, min=1 max=10 | ☐ |
| SS-11 | KEDA Scaling 1→10 | kubectl get pods -w during 300 ev/s | ☐ |
| SS-12 | Pod Logs S3 Flush | UUID-suffixed parquet filename in log | ☐ |
| SS-13 | Grafana Full Dashboard | All 20 panels at 67% zoom | ☐ |
| SS-14 | Grafana Overview | Events/min ~1800, Lag ~0 | ☐ |
| SS-15 | Grafana Engagement + Performance | Region map, device split, engagement | ☐ |
| SS-16 | Grafana System Health | S3 savings **87.6%** gauge | ☐ |
| SS-17 | Grafana Surge ACTIVE | Surge=1, Events/min ~5400 | ☐ |
| SS-18 | Grafana KEDA Scaling | Pod count panel (if available) | ☐ |
| SS-19 | Prometheus :8000 /metrics | pipeline_* metrics, surge_detected, s3 savings | ☐ |
| SS-20 | storage_worker :8001 /metrics | worker_* metrics from k8s pods | ☐ |
| SS-21 | Spark Service Status | Active (running) + STORAGE_ENABLED=false | ☐ |
| SS-22 | Spark Surge Logs | DETECTED → 1200 forwarded → NORMAL | ☐ |
| SS-23 | Spark Recovery Logs | 5-second gap proving checkpoint recovery | ☐ |
| SS-24 | ASG + CloudWatch | Desired=0 at baseline, alarm threshold 70% | ☐ |

---

## Efficient Order

1. **AWS Console** (no SSH needed): SS-01 → SS-02 → SS-03 → SS-04 → SS-05 → SS-24
2. **SSH EC2-1 (Kafka)**: SS-06 → SS-07 → SS-08
3. **SSH EC2-2 (k3s)**: SS-09 → SS-10 → SS-21
4. **Grafana steady state** (http://52.66.142.159:3000): SS-13 → SS-14 → SS-15 → SS-16
5. **Trigger 10× load**: SS-11 (kubectl watch) + SS-19 + SS-20
6. **Trigger surge (90 ev/s)**: SS-17
7. **Prometheus**: SS-19 → SS-20
8. **Spark logs**: SS-22 → SS-23 → SS-12 (pod logs)
