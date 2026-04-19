terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.0"
    }
  }
  required_version = ">= 1.5"
}

provider "aws" {
  region = var.aws_region
  # Credentials come from ~/.aws/credentials or the EC2 instance profile —
  # never hardcoded here.
}

# ──────────────────────────────────────────────────────────────────────────────
# Random suffix — keeps the bucket name globally unique across accounts
# ──────────────────────────────────────────────────────────────────────────────
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# ──────────────────────────────────────────────────────────────────────────────
# 1. S3 Bucket
# ──────────────────────────────────────────────────────────────────────────────
resource "aws_s3_bucket" "edtech" {
  bucket        = "edtech-pipeline-data-${random_id.bucket_suffix.hex}"
  force_destroy = false

  tags = {
    Project = "edtech-pipeline"
    Env     = "production"
  }
}

resource "aws_s3_bucket_versioning" "edtech" {
  bucket = aws_s3_bucket.edtech.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "edtech" {
  bucket = aws_s3_bucket.edtech.id

  rule {
    id     = "glacier-after-30-days"
    status = "Enabled"

    filter {}   # applies to all objects

    transition {
      days          = 30
      storage_class = "GLACIER"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "edtech" {
  bucket                  = aws_s3_bucket.edtech.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ──────────────────────────────────────────────────────────────────────────────
# 2–4. DynamoDB tables
# ──────────────────────────────────────────────────────────────────────────────
resource "aws_dynamodb_table" "user_stats" {
  name         = "UserStats"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "user_id"
  range_key    = "date"

  attribute {
    name = "user_id"
    type = "S"
  }
  attribute {
    name = "date"
    type = "S"
  }

  tags = { Project = "edtech-pipeline" }
}

resource "aws_dynamodb_table" "daily_reports" {
  name         = "DailyReports"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "report_date"

  attribute {
    name = "report_date"
    type = "S"
  }

  tags = { Project = "edtech-pipeline" }
}

resource "aws_dynamodb_table" "course_stats" {
  name         = "CourseStats"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "course_id"
  range_key    = "date"

  attribute {
    name = "course_id"
    type = "S"
  }
  attribute {
    name = "date"
    type = "S"
  }

  tags = { Project = "edtech-pipeline" }
}

# ──────────────────────────────────────────────────────────────────────────────
# 5. Security group rules on existing EC2 instances
#    We look up each instance by its public IP and add ingress rules to
#    its first attached security group.
# ──────────────────────────────────────────────────────────────────────────────

# ── Look up EC2 instances by NAME TAG (not IP — IPs change on stop/start) ─────
# EC2-1 tag: Name=kafka_broker  (instance i-03d3112bd7120a09c)
# EC2-2 tag: Name=spark-redis   (instance i-06f01db8cc1b263dd)
data "aws_instance" "ec2_1" {
  filter {
    name   = "tag:Name"
    values = ["kafka_broker"]
  }
  filter {
    name   = "instance-state-name"
    values = ["running"]
  }
}

data "aws_instance" "ec2_2" {
  filter {
    name   = "tag:Name"
    values = ["spark-redis"]
  }
  filter {
    name   = "instance-state-name"
    values = ["running"]
  }
}

# ── EC2-1 security group rules (Kafka) ────────────────────────────────────────
resource "aws_security_group_rule" "ec2_1_kafka_9092" {
  type              = "ingress"
  description       = "Kafka PLAINTEXT (internal)"
  from_port         = 9092
  to_port           = 9092
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = tolist(data.aws_instance.ec2_1.vpc_security_group_ids)[0]
}

resource "aws_security_group_rule" "ec2_1_kafka_9093" {
  type              = "ingress"
  description       = "Kafka EXTERNAL (Mac + EC2-2)"
  from_port         = 9093
  to_port           = 9093
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = tolist(data.aws_instance.ec2_1.vpc_security_group_ids)[0]
}

# ── EC2-2 security group rules (Spark + monitoring) ───────────────────────────
resource "aws_security_group_rule" "ec2_2_grafana" {
  type              = "ingress"
  description       = "Grafana dashboard"
  from_port         = 3000
  to_port           = 3000
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = tolist(data.aws_instance.ec2_2.vpc_security_group_ids)[0]
}

resource "aws_security_group_rule" "ec2_2_prometheus" {
  type              = "ingress"
  description       = "Prometheus UI"
  from_port         = 9090
  to_port           = 9090
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = tolist(data.aws_instance.ec2_2.vpc_security_group_ids)[0]
}

resource "aws_security_group_rule" "ec2_2_spark_metrics" {
  type              = "ingress"
  description       = "Spark Prometheus metrics"
  from_port         = 8000
  to_port           = 8000
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = tolist(data.aws_instance.ec2_2.vpc_security_group_ids)[0]
}

resource "aws_security_group_rule" "ec2_2_redis" {
  type              = "ingress"
  description       = "Redis (VPC-internal only)"
  from_port         = 6379
  to_port           = 6379
  protocol          = "tcp"
  cidr_blocks       = ["172.31.0.0/16"]   # default VPC CIDR — restrict to VPC
  security_group_id = tolist(data.aws_instance.ec2_2.vpc_security_group_ids)[0]
}

# k3s API server — ASG worker nodes must reach EC2-2 on 6443 to join the cluster
resource "aws_security_group_rule" "ec2_2_k3s_api" {
  type              = "ingress"
  description       = "k3s API server (worker node join + kubectl)"
  from_port         = 6443
  to_port           = 6443
  protocol          = "tcp"
  cidr_blocks       = ["0.0.0.0/0"]
  security_group_id = tolist(data.aws_instance.ec2_2.vpc_security_group_ids)[0]
}

# k3s Flannel VXLAN — pod-to-pod overlay network between EC2-2 and ASG workers
resource "aws_security_group_rule" "ec2_2_k3s_flannel" {
  type              = "ingress"
  description       = "k3s Flannel VXLAN (pod overlay)"
  from_port         = 8472
  to_port           = 8472
  protocol          = "udp"
  cidr_blocks       = ["172.31.0.0/16"]
  security_group_id = tolist(data.aws_instance.ec2_2.vpc_security_group_ids)[0]
}

# ──────────────────────────────────────────────────────────────────────────────
# SSM Parameter — k3s cluster join token
#
# After `terraform apply` and k3s installation on EC2-2, store the join token:
#   scripts/setup_k3s.sh  (run once after k3s is installed)
# ASG worker instances read this token at boot to join the k3s cluster.
# ──────────────────────────────────────────────────────────────────────────────
resource "aws_ssm_parameter" "k3s_token" {
  name        = "/edtech/k3s-token"
  type        = "SecureString"
  value       = "placeholder-run-setup_k3s.sh-to-set-real-token"
  description = "k3s cluster join token for ASG worker nodes. Updated by scripts/setup_k3s.sh."

  lifecycle {
    # Don't overwrite the real token if it was set by setup_k3s.sh
    ignore_changes = [value]
  }

  tags = { Project = "edtech-pipeline" }
}

# ──────────────────────────────────────────────────────────────────────────────
# 6. IAM role + instance profile for EC2-2
#    (S3 + DynamoDB access without hardcoded credentials)
# ──────────────────────────────────────────────────────────────────────────────
resource "aws_iam_role" "ec2_2_role" {
  name = "edtech-ec2-2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })

  tags = { Project = "edtech-pipeline" }
}

resource "aws_iam_policy" "edtech_access" {
  name        = "edtech-pipeline-access"
  description = "S3 + DynamoDB access for the EdTech Spark consumer"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3Access"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:GetObject",
          "s3:ListBucket",
          "s3:DeleteObject",
        ]
        Resource = [
          aws_s3_bucket.edtech.arn,
          "${aws_s3_bucket.edtech.arn}/*",
        ]
      },
      {
        Sid    = "DynamoDBAccess"
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:GetItem",
          "dynamodb:Query",
          "dynamodb:Scan",
          "dynamodb:UpdateItem",
          "dynamodb:BatchWriteItem",
        ]
        Resource = [
          aws_dynamodb_table.user_stats.arn,
          aws_dynamodb_table.daily_reports.arn,
          aws_dynamodb_table.course_stats.arn,
        ]
      },
      {
        # Prometheus ec2_sd_configs needs DescribeInstances to find ASG nodes
        Sid      = "PrometheusEC2Discovery"
        Effect   = "Allow"
        Action   = ["ec2:DescribeInstances"]
        Resource = ["*"]
      },
      {
        # ASG worker nodes read the k3s join token from SSM at boot
        Sid    = "SSMk3sToken"
        Effect = "Allow"
        Action = [
          "ssm:GetParameter",
          "ssm:GetParameters",
        ]
        Resource = ["arn:aws:ssm:${var.aws_region}:*:parameter/edtech/*"]
      },
    ]
  })
}

resource "aws_iam_role_policy_attachment" "ec2_2_attach" {
  role       = aws_iam_role.ec2_2_role.name
  policy_arn = aws_iam_policy.edtech_access.arn
}

resource "aws_iam_instance_profile" "ec2_2_profile" {
  name = "edtech-ec2-2-profile"
  role = aws_iam_role.ec2_2_role.name
}

# ──────────────────────────────────────────────────────────────────────────────
# 7. Elastic IP for EC2-1 (Kafka node)
#
#    Without an EIP, EC2-1 gets a new public IP every time it is stopped and
#    restarted. That breaks Kafka's advertised.listeners and every downstream
#    consumer (Spark, generator) that connects via the external port 9093.
#
#    With this EIP:
#    - EC2-1 ALWAYS has the same public IP regardless of stop/start
#    - The Launch Template (ASG instances) bakes the correct IP at apply time
#    - scripts/apply_eip.sh runs once after `terraform apply` to update Kafka
#      server.properties and restart all services on EC2-1 and EC2-2
# ──────────────────────────────────────────────────────────────────────────────
resource "aws_eip" "ec2_1" {
  domain = "vpc"

  tags = {
    Project = "edtech-pipeline"
    Name    = "ec2-1-kafka-eip"
  }
}

resource "aws_eip_association" "ec2_1" {
  instance_id   = data.aws_instance.ec2_1.id
  allocation_id = aws_eip.ec2_1.id
}

# ──────────────────────────────────────────────────────────────────────────────
# 8. Security group for auto-scaled consumer instances
# ──────────────────────────────────────────────────────────────────────────────
resource "aws_security_group" "consumer_asg" {
  name        = "edtech-consumer-asg-sg"
  description = "Allow outbound to Kafka + Redis + AWS APIs; inbound Prometheus scrape"

  # Outbound: Kafka on EC2-1
  egress {
    description = "Kafka EXTERNAL"
    from_port   = 9093
    to_port     = 9093
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  # Outbound: Redis on EC2-2 (private IP)
  egress {
    description = "Redis on EC2-2"
    from_port   = 6379
    to_port     = 6379
    protocol    = "tcp"
    cidr_blocks = ["172.31.0.0/16"]   # VPC-internal
  }
  # Outbound: HTTPS (AWS APIs — S3, DynamoDB)
  egress {
    description = "AWS APIs"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  # Inbound: Prometheus metrics scrape (storage-worker pods expose port 8001)
  ingress {
    description = "Prometheus scrape - storage-worker pods"
    from_port   = 8001
    to_port     = 8001
    protocol    = "tcp"
    cidr_blocks = ["172.31.0.0/16"]
  }
  # Inbound: k3s kubelet API (EC2-2 control plane needs to reach worker nodes)
  ingress {
    description = "k3s kubelet API (control plane to worker)"
    from_port   = 10250
    to_port     = 10250
    protocol    = "tcp"
    cidr_blocks = ["172.31.0.0/16"]
  }
  # Inbound: k3s Flannel VXLAN overlay
  ingress {
    description = "k3s Flannel VXLAN (pod overlay)"
    from_port   = 8472
    to_port     = 8472
    protocol    = "udp"
    cidr_blocks = ["172.31.0.0/16"]
  }
  # Outbound: k3s API server on EC2-2 (worker → control plane join)
  egress {
    description = "k3s API server join"
    from_port   = 6443
    to_port     = 6443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }
  # Note: SSM reads (port 443) are covered by the "AWS APIs" egress rule above.

  tags = { Project = "edtech-pipeline" }
}

# ──────────────────────────────────────────────────────────────────────────────
# 9. Launch Template — ASG worker nodes that join the k3s cluster on EC2-2.
#
#    Architecture:
#      EC2-2 runs k3s SERVER (control plane + first worker node).
#      ASG instances run k3s AGENT (worker nodes only).
#      KEDA ScaledObject schedules storage-worker pods across all nodes.
#
#    On first boot each ASG instance:
#      1. Reads the k3s join token from SSM Parameter Store
#      2. Installs k3s agent and joins EC2-2's cluster (K3S_URL + K3S_TOKEN)
#      3. k3s agent registers with the control plane; KEDA schedules pods here
#         automatically when more capacity is needed
#
#    No Spark / JVM on ASG nodes — storage_worker.py is pure Python (250 MB RAM)
#    vs Spark's 1.5 GB JVM. Each t3.medium can run ~8 storage-worker pods.
# ──────────────────────────────────────────────────────────────────────────────
resource "aws_launch_template" "consumer" {
  name_prefix   = "edtech-consumer-"
  image_id      = var.consumer_ami
  instance_type = var.consumer_instance_type

  # Attach the same IAM role as EC2-2 (S3 + DynamoDB + SSM access)
  iam_instance_profile {
    name = aws_iam_instance_profile.ec2_2_profile.name
  }

  vpc_security_group_ids = [aws_security_group.consumer_asg.id]

  # userdata: join the k3s cluster on EC2-2 as a worker node.
  # Pods (storage_worker.py) are scheduled here by KEDA when lag grows.
  user_data = base64encode(<<-EOF
    #!/bin/bash
    set -euo pipefail
    exec > /var/log/k3s-agent-setup.log 2>&1

    echo "=== $(date -u) Starting k3s agent setup ==="

    # Install curl and jq if not present
    apt-get update -y -q
    apt-get install -y -q curl jq awscli

    # ── Read k3s join token from SSM ───────────────────────────────────────
    REGION="${var.aws_region}"
    K3S_TOKEN=$(aws ssm get-parameter \
      --name /edtech/k3s-token \
      --with-decryption \
      --region "$REGION" \
      --query Parameter.Value \
      --output text)

    if [ -z "$K3S_TOKEN" ] || [ "$K3S_TOKEN" = "placeholder-run-setup_k3s.sh-to-set-real-token" ]; then
      echo "ERROR: k3s token not set in SSM. Run scripts/setup_k3s.sh on EC2-2 first."
      exit 1
    fi

    # ── EC2-2 private IP = k3s server address ─────────────────────────────
    # EC2-2 EIP is ${var.ec2_2_ip} but we need its private IP for cluster traffic.
    EC2_2_PRIVATE_IP=$(aws ec2 describe-instances \
      --region "$REGION" \
      --filters "Name=tag:Name,Values=spark-redis" "Name=instance-state-name,Values=running" \
      --query "Reservations[0].Instances[0].PrivateIpAddress" \
      --output text)

    K3S_URL="https://$${EC2_2_PRIVATE_IP}:6443"
    echo "Joining k3s cluster at $K3S_URL"

    # ── Install k3s agent ──────────────────────────────────────────────────
    curl -sfL https://get.k3s.io | \
      K3S_URL="$K3S_URL" \
      K3S_TOKEN="$K3S_TOKEN" \
      INSTALL_K3S_EXEC="agent" \
      sh -

    echo "=== $(date -u) k3s agent joined cluster successfully ==="
    # KEDA will schedule storage-worker pods on this node automatically
    # as Kafka consumer group lag grows beyond the lagThreshold.
  EOF
  )

  tag_specifications {
    resource_type = "instance"
    tags = {
      Name    = "edtech-consumer-asg"
      Project = "edtech-pipeline"
    }
  }
}

# ──────────────────────────────────────────────────────────────────────────────
# 10. Auto Scaling Group — k3s worker nodes
#
#    Two-tier horizontal scaling:
#      Tier 1 (fast, seconds):  KEDA scales k8s pods 1→10 based on Kafka lag.
#      Tier 2 (slower, minutes): THIS ASG scales EC2 nodes when EC2-2 CPU
#                                 is saturated and pods are stuck Pending.
#
#    Scale-out trigger: EC2-2 (k3s server) CPU > 70% for 2 min.
#      When KEDA has maxed pods on EC2-2 and CPU is pegged, a new k3s agent
#      node is added. Pending pods immediately schedule there.
#
#    Scale-in trigger: ASG instance avg CPU < 25% for 10 min.
#      Worker nodes are removed one at a time once load drops.
#      ASG returns to 0 nodes at steady state (EC2-2 handles baseline alone).
# ──────────────────────────────────────────────────────────────────────────────
data "aws_subnets" "default" {
  filter {
    name   = "default-for-az"
    values = ["true"]
  }
}

resource "aws_autoscaling_group" "consumer" {
  name                = "edtech-consumer-asg"
  min_size            = 0                       # 0 — no idle nodes at steady state
  max_size            = var.consumer_max_size   # 10 — hard cap (10 Kafka partitions)
  desired_capacity    = 0                       # scaled by CPU alarm, not manually
  vpc_zone_identifier = data.aws_subnets.default.ids

  launch_template {
    id      = aws_launch_template.consumer.id
    version = "$Latest"
  }

  # Replace instances gradually during updates (rolling deploy)
  instance_refresh {
    strategy = "Rolling"
    preferences {
      min_healthy_percentage = 50
    }
  }

  health_check_type         = "EC2"
  health_check_grace_period = 120   # wait 2 min before marking instance unhealthy

  tag {
    key                 = "Project"
    value               = "edtech-pipeline"
    propagate_at_launch = true
  }
}

# ──────────────────────────────────────────────────────────────────────────────
# 11. Scaling policies — scale out fast, scale in slowly
#
#    Scale-out uses EC2-2's CPU (not ASG avg) so the alarm fires even when
#    the ASG is at 0 instances and has no CPU metrics of its own.
#    Scale-in watches the ASG instance avg CPU; treat_missing_data=notBreaching
#    keeps the alarm silent when ASG is at 0 (no instances → no data).
# ──────────────────────────────────────────────────────────────────────────────

# Scale OUT: add one instance when EC2-2 (k3s server) CPU > 70% for 2 min.
# EC2-2 gets saturated when KEDA has scaled to max pods and they're all running
# there — that's the signal that a new k3s worker node is needed.
resource "aws_autoscaling_policy" "scale_out" {
  name                   = "edtech-consumer-scale-out"
  autoscaling_group_name = aws_autoscaling_group.consumer.name
  adjustment_type        = "ChangeInCapacity"
  scaling_adjustment     = 1      # add 1 node at a time
  cooldown               = 180    # wait 3 min — k3s agent boot takes ~90s
}

resource "aws_cloudwatch_metric_alarm" "high_cpu" {
  alarm_name          = "edtech-consumer-high-cpu"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/EC2"
  period              = 60
  statistic           = "Average"
  threshold           = 70
  alarm_description   = "EC2-2 CPU > 70% for 2 min — k3s server saturated, add a worker node"
  treat_missing_data  = "notBreaching"

  # Watch EC2-2 (k3s server node), not ASG instances.
  # ASG starts at 0 so it has no CPU metrics to watch for the initial scale-out.
  dimensions = {
    InstanceId = data.aws_instance.ec2_2.id
  }

  alarm_actions = [aws_autoscaling_policy.scale_out.arn]
}

# Scale IN: remove one instance when ASG avg CPU < 25% for 10 consecutive minutes.
# treat_missing_data=notBreaching: when ASG is at 0 instances there is no CPU
# data — treat it as "not breaching" so the alarm does not fire spuriously.
resource "aws_autoscaling_policy" "scale_in" {
  name                   = "edtech-consumer-scale-in"
  autoscaling_group_name = aws_autoscaling_group.consumer.name
  adjustment_type        = "ChangeInCapacity"
  scaling_adjustment     = -1     # remove 1 node at a time
  cooldown               = 300    # wait 5 min before next scale-in
}

resource "aws_cloudwatch_metric_alarm" "low_cpu" {
  alarm_name          = "edtech-consumer-low-cpu"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 10
  metric_name         = "CPUUtilization"
  namespace           = "AWS/EC2"
  period              = 60
  statistic           = "Average"
  threshold           = 25
  alarm_description   = "ASG worker CPU < 25% for 10 min — load dropped, remove a node"
  treat_missing_data  = "notBreaching"   # ASG at 0 → no data → stay silent

  dimensions = {
    AutoScalingGroupName = aws_autoscaling_group.consumer.name
  }

  alarm_actions = [aws_autoscaling_policy.scale_in.arn]
}
