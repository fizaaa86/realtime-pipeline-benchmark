#!/usr/bin/env bash
# scripts/setup_k3s.sh
# ──────────────────────────────────────────────────────────────────────────────
# One-time setup: install k3s server on EC2-2, store join token in SSM,
# install KEDA, and deploy all k8s manifests.
#
# Run once after `terraform apply` (or whenever EC2-2 is rebuilt):
#   bash scripts/setup_k3s.sh
#
# Prerequisites:
#   - SSH key at ~/.ssh/kafka_key.pem
#   - EC2-2 reachable at 52.66.142.159
#   - AWS CLI configured (terraform outputs accessible)
#   - kubectl available locally (or use kubeconfig from EC2-2)
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

EC2_2="52.66.142.159"
SSH="ssh -i ~/.ssh/kafka_key.pem -o StrictHostKeyChecking=no ubuntu@${EC2_2}"
SCP="scp -i ~/.ssh/kafka_key.pem -o StrictHostKeyChecking=no"
AWS_REGION="ap-south-1"
REPO_DIR="$(cd "$(dirname "$0")/.." && pwd)"

echo "╔══════════════════════════════════════════════╗"
echo "║  EdTech k3s Setup — EC2-2 (${EC2_2})  ║"
echo "╚══════════════════════════════════════════════╝"

# ── Step 1: Install k3s server on EC2-2 ──────────────────────────────────────
echo ""
echo "── Step 1: Installing k3s server on EC2-2 ──────────────────────────────"
$SSH 'bash -s' << 'REMOTE'
set -euo pipefail

# Skip if already installed
if command -v k3s &>/dev/null && systemctl is-active k3s &>/dev/null; then
  echo "k3s already running — skipping install"
  exit 0
fi

echo "Installing k3s server..."
# Install k3s with write-kubeconfig-mode so ubuntu user can read it
curl -sfL https://get.k3s.io | \
  INSTALL_K3S_EXEC="server --write-kubeconfig-mode 644" sh -

echo "Waiting for k3s to be ready..."
until kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml get nodes &>/dev/null; do
  sleep 3
done

echo "k3s server ready:"
kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml get nodes
REMOTE

echo "k3s server installed on EC2-2"

# ── Step 2: Store join token in SSM ──────────────────────────────────────────
echo ""
echo "── Step 2: Storing k3s join token in SSM ────────────────────────────────"
K3S_TOKEN=$($SSH 'sudo cat /var/lib/rancher/k3s/server/node-token')

aws ssm put-parameter \
  --name "/edtech/k3s-token" \
  --value "$K3S_TOKEN" \
  --type "SecureString" \
  --overwrite \
  --region "$AWS_REGION"

echo "Join token stored in SSM: /edtech/k3s-token"

# ── Step 3: Copy kubeconfig locally ──────────────────────────────────────────
echo ""
echo "── Step 3: Copying kubeconfig to ~/.kube/edtech-k3s.yaml ───────────────"
mkdir -p ~/.kube
$SCP "${EC2_2}:/etc/rancher/k3s/k3s.yaml" /tmp/edtech-k3s.yaml
# Replace 127.0.0.1 with EC2-2's public IP for remote kubectl
sed "s|127.0.0.1|${EC2_2}|g" /tmp/edtech-k3s.yaml > ~/.kube/edtech-k3s.yaml
chmod 600 ~/.kube/edtech-k3s.yaml
echo "Kubeconfig written to ~/.kube/edtech-k3s.yaml"
echo "  To use: export KUBECONFIG=~/.kube/edtech-k3s.yaml"

# ── Step 4: Install KEDA ──────────────────────────────────────────────────────
echo ""
echo "── Step 4: Installing KEDA in k3s cluster ───────────────────────────────"
$SSH "kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml apply \
  -f https://github.com/kedacore/keda/releases/download/v2.13.1/keda-2.13.1.yaml"

echo "Waiting for KEDA operator to be ready..."
$SSH "kubectl --kubeconfig /etc/rancher/k3s/k3s.yaml wait \
  --namespace keda \
  --for=condition=ready pod \
  --selector=app=keda-operator \
  --timeout=120s" || echo "KEDA operator not ready yet — check with: kubectl get pods -n keda"

# ── Step 5: Upload storage_worker.py to S3 (for ASG nodes as fallback) ───────
echo ""
echo "── Step 5: Uploading storage_worker.py to S3 ────────────────────────────"
S3_BUCKET=$(cd "${REPO_DIR}/terraform" && terraform output -raw s3_bucket_name 2>/dev/null || echo "")
if [ -n "$S3_BUCKET" ]; then
  aws s3 cp "${REPO_DIR}/spark/storage_worker.py" \
    "s3://${S3_BUCKET}/artifacts/storage_worker.py" \
    --region "$AWS_REGION"
  echo "Uploaded to s3://${S3_BUCKET}/artifacts/storage_worker.py"
else
  echo "Could not determine S3 bucket — skipping upload"
fi

# ── Step 6: Apply k8s manifests ──────────────────────────────────────────────
echo ""
echo "── Step 6: Applying k8s manifests ───────────────────────────────────────"

# Copy manifests to EC2-2
$SCP "${REPO_DIR}/k8s/namespace.yaml"          "${EC2_2}:/tmp/k8s-namespace.yaml"
$SCP "${REPO_DIR}/k8s/configmap.yaml"          "${EC2_2}:/tmp/k8s-configmap.yaml"
$SCP "${REPO_DIR}/k8s/worker-script-cm.yaml"   "${EC2_2}:/tmp/k8s-worker-script-cm.yaml"
$SCP "${REPO_DIR}/k8s/deployment.yaml"         "${EC2_2}:/tmp/k8s-deployment.yaml"
$SCP "${REPO_DIR}/k8s/hpa.yaml"                "${EC2_2}:/tmp/k8s-hpa.yaml"

$SSH "KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl apply -f /tmp/k8s-namespace.yaml"
$SSH "KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl apply -f /tmp/k8s-configmap.yaml"
$SSH "KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl apply -f /tmp/k8s-worker-script-cm.yaml"
$SSH "KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl apply -f /tmp/k8s-deployment.yaml"
$SSH "KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl apply -f /tmp/k8s-hpa.yaml"

echo "Manifests applied"

# ── Step 7: Update EC2-2 edtech-spark to STORAGE_ENABLED=false ───────────────
echo ""
echo "── Step 7: Disabling storage in Spark master (EC2-2) ────────────────────"
$SSH "sudo sed -i '/^Environment=KAFKA_BOOTSTRAP/a Environment=STORAGE_ENABLED=false' \
  /etc/systemd/system/edtech-spark.service && \
  sudo systemctl daemon-reload && \
  sudo systemctl restart edtech-spark"

echo "Spark master restarted with STORAGE_ENABLED=false"

# ── Step 8: Verify deployment ─────────────────────────────────────────────────
echo ""
echo "── Step 8: Verifying deployment ─────────────────────────────────────────"
sleep 10
$SSH "KUBECONFIG=/etc/rancher/k3s/k3s.yaml kubectl get pods -n edtech-pipeline -o wide"

echo ""
echo "╔══════════════════════════════════════════════════════════════════════╗"
echo "║  Setup complete!                                                     ║"
echo "║                                                                      ║"
echo "║  Kubernetes:  kubectl --kubeconfig ~/.kube/edtech-k3s.yaml          ║"
echo "║  Namepsace:   edtech-pipeline                                        ║"
echo "║  Deployment:  edtech-storage-worker (1 pod, scales via KEDA)        ║"
echo "║  KEDA:        scales 1-10 pods based on Kafka consumer group lag     ║"
echo "║                                                                      ║"
echo "║  To demo 10x load:                                                   ║"
echo "║    scripts/test_10x_load.sh                                          ║"
echo "║  Watch scaling:                                                       ║"
echo "║    kubectl get pods -n edtech-pipeline -w                            ║"
echo "╚══════════════════════════════════════════════════════════════════════╝"
