#!/usr/bin/env bash
# test_10x_load.sh
# Switches the generator on EC2-1 between normal (30 ev/s) and 10× load (300 ev/s).
# Use this to demonstrate surge detection and ASG scale-out during evaluation.
#
# Usage:
#   ./scripts/test_10x_load.sh spike     # → 300 ev/s (10× load, 5000 users)
#   ./scripts/test_10x_load.sh normal    # → 30 ev/s  (baseline, 500 users)
#   ./scripts/test_10x_load.sh status    # → show generator PID and current rate
#
# Requires: kafka_key.pem in project root, EC2-1 reachable at EC2_1_IP

set -euo pipefail

# EC2-1 has a Terraform-managed Elastic IP — auto-fetch it from Terraform output.
# Override with EC2_1_IP env var if needed: EC2_1_IP=x.x.x.x ./test_10x_load.sh spike
if [[ -z "${EC2_1_IP:-}" ]]; then
  TF_DIR="$(dirname "$0")/../terraform"
  EC2_1_IP=$(terraform -chdir="$TF_DIR" output -raw ec2_1_eip 2>/dev/null || true)
  if [[ -z "$EC2_1_IP" ]]; then
    echo "ERROR: Could not get EC2-1 EIP from Terraform. Run 'terraform apply' first,"
    echo "       or set EC2_1_IP manually: EC2_1_IP=x.x.x.x ./scripts/test_10x_load.sh spike"
    exit 1
  fi
fi
SSH_KEY="${SSH_KEY:-$(dirname "$0")/../kafka_key.pem}"
SSH="ssh -i $SSH_KEY -o StrictHostKeyChecking=no ubuntu@$EC2_1_IP"

GENERATOR_PATH="/home/ubuntu/generator.py"
LOG_PATH="/tmp/gen.log"

MODE="${1:-}"

case "$MODE" in

  spike)
    echo "==> Switching generator to 10× load: 5000 users, 300 ev/s"
    $SSH bash <<'REMOTE'
      # Stop current generator
      pkill -f "generator.py" 2>/dev/null || true
      sleep 1
      # Start at 10× parameters
      # 5000 users are needed because the per-user state machine caps throughput
      # at ~(users × ~0.06 ev/s). 5000 users × 300 rate saturates the pipeline.
      nohup python3 /home/ubuntu/generator.py \
          --bootstrap-servers localhost:9092 \
          --topic edtech-events \
          --rate 300 \
          --users 5000 \
        >> /tmp/gen.log 2>&1 &
      echo "Generator restarted at 300 ev/s with 5000 users (PID: $!)"
REMOTE
    echo "==> Done. Watch ASG scale-out in AWS Console → EC2 → Auto Scaling Groups."
    echo "    Watch SurgeDetector in Grafana or: ssh $EC2_1_IP tail -f /var/log/edtech-spark.log"
    ;;

  normal)
    echo "==> Restoring generator to baseline: 500 users, 30 ev/s"
    $SSH bash <<'REMOTE'
      pkill -f "generator.py" 2>/dev/null || true
      sleep 1
      nohup python3 /home/ubuntu/generator.py \
          --bootstrap-servers localhost:9092 \
          --topic edtech-events \
          --rate 30 \
          --users 500 \
        >> /tmp/gen.log 2>&1 &
      echo "Generator restored to 30 ev/s with 500 users (PID: $!)"
REMOTE
    echo "==> Done. ASG will scale back in after CPU stays below 25% for 10 minutes."
    ;;

  status)
    echo "==> Generator status on EC2-1 ($EC2_1_IP):"
    $SSH bash <<'REMOTE'
      PID=$(pgrep -f "generator.py" || true)
      if [ -z "$PID" ]; then
        echo "  Generator: NOT RUNNING"
      else
        CMD=$(ps -p "$PID" -o args --no-headers)
        echo "  Generator PID: $PID"
        echo "  Command: $CMD"
        echo "  Last log lines:"
        tail -5 /var/log/edtech-generator.log 2>/dev/null || echo "  (no log)"
      fi
REMOTE
    ;;

  *)
    echo "Usage: $0 {spike|normal|status}"
    echo "  spike   — 10× load (300 ev/s, 5000 users)"
    echo "  normal  — baseline (30 ev/s, 500 users)"
    echo "  status  — show current generator state"
    exit 1
    ;;

esac
