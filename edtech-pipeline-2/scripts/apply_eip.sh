#!/usr/bin/env bash
# apply_eip.sh
# ─────────────────────────────────────────────────────────────────────────────
# Run ONCE after `terraform apply` to wire the new EC2-1 Elastic IP into every
# service that needs it. After this script, the pipeline is fully self-healing:
# stopping and restarting EC2-1 will never break the pipeline again.
#
# Prerequisites:
#   - terraform apply completed successfully (aws_eip.ec2_1 exists)
#   - kafka_key.pem is in the project root (or set SSH_KEY env var)
#   - AWS CLI configured (for `terraform output`)
#
# Usage:
#   cd /path/to/edtech-pipeline-2
#   ./scripts/apply_eip.sh
#
#   Override the SSH key path:
#   SSH_KEY=~/.ssh/my_key.pem ./scripts/apply_eip.sh
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SSH_KEY="${SSH_KEY:-$PROJECT_ROOT/kafka_key.pem}"

# ── 0. Verify SSH key exists ──────────────────────────────────────────────────
if [[ ! -f "$SSH_KEY" ]]; then
  echo "ERROR: SSH key not found at $SSH_KEY"
  echo "       Set SSH_KEY=/path/to/kafka_key.pem and re-run."
  exit 1
fi
chmod 400 "$SSH_KEY"

# ── 1. Get the EIP from Terraform output ─────────────────────────────────────
echo "==> Fetching EC2-1 Elastic IP from Terraform..."
cd "$PROJECT_ROOT/terraform"
EIP=$(terraform output -raw ec2_1_eip 2>/dev/null)
if [[ -z "$EIP" ]]; then
  echo "ERROR: terraform output ec2_1_eip returned empty."
  echo "       Make sure you ran 'terraform apply' first."
  exit 1
fi
EC2_2_IP=$(terraform output -raw ec2_2_eip 2>/dev/null || echo "52.66.142.159")
cd "$PROJECT_ROOT"

echo "    EC2-1 EIP : $EIP"
echo "    EC2-2 IP  : $EC2_2_IP"

SSH1="ssh -i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=15 ubuntu@$EIP"
SSH2="ssh -i $SSH_KEY -o StrictHostKeyChecking=no -o ConnectTimeout=15 ubuntu@$EC2_2_IP"

# ── 2. Wait for EC2-1 to be reachable ────────────────────────────────────────
echo ""
echo "==> Waiting for EC2-1 ($EIP) to be SSH-reachable..."
for i in $(seq 1 20); do
  if $SSH1 exit 2>/dev/null; then
    echo "    EC2-1 is up."
    break
  fi
  if [[ $i -eq 20 ]]; then
    echo "ERROR: EC2-1 not reachable after 100 seconds. Check security groups allow port 22."
    exit 1
  fi
  echo "    Attempt $i/20 — retrying in 5s..."
  sleep 5
done

# ── 3. Fix Kafka advertised.listeners on EC2-1 ───────────────────────────────
echo ""
echo "==> Updating Kafka advertised.listeners on EC2-1 to $EIP..."
$SSH1 bash <<REMOTE
  set -e

  CFG=/opt/kafka/config/server.properties

  # Replace the EXTERNAL listener address with the new EIP
  sudo sed -i "s|EXTERNAL://[^,]*:9093|EXTERNAL://${EIP}:9093|g" "\$CFG"

  # Verify the change
  echo "    Current advertised.listeners:"
  grep "^advertised.listeners" "\$CFG" || echo "    (not set — checking listeners)"
  grep "^listeners" "\$CFG" || true

  # Restart Kafka
  sudo systemctl restart kafka
  echo "    Kafka restarted."

  # Wait for Kafka to be ready
  for i in \$(seq 1 12); do
    if /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
        --list &>/dev/null; then
      echo "    Kafka is accepting connections."
      break
    fi
    echo "    Waiting for Kafka to be ready (\$i/12)..."
    sleep 5
  done
REMOTE

# ── 4. Verify Kafka is reachable from this machine ───────────────────────────
echo ""
echo "==> Verifying Kafka port 9093 is reachable at $EIP..."
if nc -z -w5 "$EIP" 9093 2>/dev/null; then
  echo "    Port 9093 is open."
else
  echo "WARNING: Port 9093 not immediately reachable. Security group may need a moment."
  echo "         This is non-fatal — continuing."
fi

# ── 5. Update Spark consumer env on EC2-2 ────────────────────────────────────
echo ""
echo "==> Updating KAFKA_BOOTSTRAP on EC2-2 ($EC2_2_IP)..."
$SSH2 bash <<REMOTE
  set -e

  ENV_FILE=/etc/edtech-spark.env

  if [[ -f "\$ENV_FILE" ]]; then
    sudo sed -i "s|^KAFKA_BOOTSTRAP=.*|KAFKA_BOOTSTRAP=${EIP}:9093|" "\$ENV_FILE"
    echo "    Updated \$ENV_FILE"
  else
    echo "WARNING: \$ENV_FILE not found — creating it"
    sudo tee "\$ENV_FILE" > /dev/null <<ENV
KAFKA_BOOTSTRAP=${EIP}:9093
KAFKA_TOPIC=edtech-events
PRIORITY_TOPIC=edtech-priority
KAFKA_GROUP_ID=edtech-spark-group
REDIS_HOST=localhost
S3_BUCKET=edtech-pipeline-data-c956dd9c
AWS_REGION=ap-south-1
TRIGGER_SECS=10
IS_MASTER_CONSUMER=true
ENV
  fi

  # Restart the Spark consumer systemd service
  if sudo systemctl is-active --quiet edtech-spark; then
    sudo systemctl restart edtech-spark
    echo "    edtech-spark service restarted."
  else
    sudo systemctl start edtech-spark || true
    echo "    edtech-spark service started."
  fi
REMOTE

# ── 6. Update k3s ConfigMap on EC2-2 ─────────────────────────────────────────
echo ""
echo "==> Updating k3s ConfigMap KAFKA_BOOTSTRAP on EC2-2..."
$SSH2 bash <<REMOTE
  set -e

  if command -v kubectl &>/dev/null; then
    kubectl patch configmap edtech-config -n edtech-pipeline \
      --type merge -p "{\"data\":{\"KAFKA_BOOTSTRAP\":\"${EIP}:9093\"}}" 2>/dev/null \
      && echo "    ConfigMap updated." \
      || echo "    ConfigMap not found (k3s may not be running) — skipping."

    # Restart the k3s deployment so pods pick up the new ConfigMap
    kubectl rollout restart deployment/spark-consumer -n edtech-pipeline 2>/dev/null \
      && echo "    spark-consumer deployment restarted." \
      || echo "    Deployment not found — skipping."
  else
    echo "    kubectl not found on EC2-2 — skipping k3s update."
  fi
REMOTE

# ── 7. Verify generator is running on EC2-1 ───────────────────────────────────
echo ""
echo "==> Checking generator on EC2-1..."
$SSH1 bash <<'REMOTE'
  PID=$(pgrep -f "generator.py" || true)
  if [[ -z "$PID" ]]; then
    echo "    Generator is NOT running — starting it..."
    nohup python3 /home/ubuntu/edtech-pipeline/generator.py \
        --bootstrap-servers localhost:9092 \
        --rate 30 \
        --users 500 \
      >> /var/log/edtech-generator.log 2>&1 &
    sleep 2
    PID=$(pgrep -f "generator.py" || true)
    echo "    Generator started (PID: $PID)"
  else
    echo "    Generator is running (PID: $PID)"
  fi
REMOTE

# ── 8. Update local k8s/configmap.yaml with the new EIP ─────────────────────
echo ""
echo "==> Updating local k8s/configmap.yaml with EIP..."
sed -i.bak "s|KAFKA_BOOTSTRAP: \".*:9093\"|KAFKA_BOOTSTRAP: \"${EIP}:9093\"|" \
  "$PROJECT_ROOT/k8s/configmap.yaml" && rm -f "$PROJECT_ROOT/k8s/configmap.yaml.bak"
echo "    k8s/configmap.yaml updated."

# ── 9. Summary ────────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════"
echo "  Pipeline fix complete!"
echo ""
echo "  EC2-1 Elastic IP  : $EIP  (permanent — survives stop/start)"
echo "  EC2-2 IP          : $EC2_2_IP"
echo ""
echo "  Grafana dashboard : http://$EC2_2_IP:3000"
echo "                      Login: admin / edtech2024"
echo ""
echo "  Wait 30-60 seconds for Spark to process its first batch,"
echo "  then panels should show live data."
echo ""
echo "  If Grafana is still empty after 60s, check:"
echo "    ssh -i $SSH_KEY ubuntu@$EC2_2_IP 'sudo journalctl -u edtech-spark -n 50'"
echo "════════════════════════════════════════════════════════"
