variable "aws_region" {
  description = "AWS region for all resources"
  type        = string
  default     = "ap-south-1"
}

# ec2_1_ip removed — EC2-1 is now looked up by tag:Name=kafka_broker, not IP.
# Its public IP is managed by the aws_eip.ec2_1 Elastic IP resource.
# Use `terraform output ec2_1_eip` to get the current stable IP.

variable "ec2_2_ip" {
  description = "Elastic IP of EC2-2 (Spark + Redis node, pre-assigned)"
  type        = string
  default     = "52.66.142.159"
}

variable "consumer_ami" {
  description = "Ubuntu 22.04 AMI ID in ap-south-1 (pre-baked with Java + Python + consumer code)"
  type        = string
  default     = "ami-0f58b397bc5c1f2e8"   # Ubuntu 22.04 LTS ap-south-1
}

variable "consumer_instance_type" {
  description = "EC2 instance type for each auto-scaled consumer node"
  type        = string
  default     = "t3.medium"   # 2 vCPU, 4 GB RAM — same spec as EC2-2
}

variable "consumer_min_size" {
  description = "Minimum number of consumer EC2 instances"
  type        = number
  default     = 1
}

variable "consumer_max_size" {
  description = "Maximum number of consumer EC2 instances (10 = one per Kafka partition)"
  type        = number
  default     = 5
}
