#!/usr/bin/env bash
# package_consumer.sh
# Packages the Spark consumer code and uploads it to S3.
# Run this once before deploying the ASG, and again whenever consumer code changes.
# Every new ASG instance pulls this artifact on first boot (see Launch Template userdata).
#
# Usage:
#   ./scripts/package_consumer.sh
#
# Requires: aws CLI configured (or run on EC2-2 with instance profile)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ── Resolve S3 bucket (read from Terraform output or fall back to env) ─────────
if command -v terraform &>/dev/null && [ -d "$REPO_ROOT/terraform" ]; then
    S3_BUCKET="$(terraform -chdir="$REPO_ROOT/terraform" output -raw s3_bucket_name 2>/dev/null || true)"
fi
S3_BUCKET="${S3_BUCKET:-${S3_BUCKET_OVERRIDE:-}}"

if [ -z "$S3_BUCKET" ]; then
    echo "ERROR: S3_BUCKET is not set."
    echo "  Either run terraform apply first, or set S3_BUCKET_OVERRIDE=<bucket-name>"
    exit 1
fi

ARTIFACT_NAME="edtech-pipeline.tar.gz"
ARTIFACT_PATH="/tmp/$ARTIFACT_NAME"
S3_KEY="artifacts/$ARTIFACT_NAME"

echo "==> Packaging consumer code from $REPO_ROOT"

# Pack only what the consumer needs — exclude secrets, __pycache__, .git
tar -czf "$ARTIFACT_PATH" \
    -C "$REPO_ROOT" \
    --exclude=".git" \
    --exclude="__pycache__" \
    --exclude="*.pyc" \
    --exclude=".env" \
    --exclude="kafka_key.pem" \
    --exclude="terraform" \
    --exclude="docs" \
    --exclude="grafana" \
    --exclude="scripts" \
    spark/ \
    requirements.txt \
    Dockerfile

echo "==> Uploading s3://$S3_BUCKET/$S3_KEY"
aws s3 cp "$ARTIFACT_PATH" "s3://$S3_BUCKET/$S3_KEY"

echo "==> Done. New ASG instances will pull this artifact on boot."
echo "    To force existing instances to update, trigger an instance refresh:"
echo "    aws autoscaling start-instance-refresh --auto-scaling-group-name edtech-consumer-asg"
