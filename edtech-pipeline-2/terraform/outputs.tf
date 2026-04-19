output "ec2_1_eip" {
  description = "Elastic IP permanently assigned to EC2-1 (Kafka node). Use this in all config files."
  value       = aws_eip.ec2_1.public_ip
}

output "ec2_1_eip_allocation_id" {
  description = "EIP allocation ID for EC2-1 (for debugging / re-association)"
  value       = aws_eip.ec2_1.id
}

output "ec2_2_eip" {
  description = "Elastic IP of EC2-2 (Spark + Redis node). Used by apply_eip.sh."
  value       = var.ec2_2_ip
}

output "s3_bucket_name" {
  description = "Name of the EdTech data lake S3 bucket"
  value       = aws_s3_bucket.edtech.bucket
}

output "s3_bucket_arn" {
  description = "ARN of the EdTech data lake S3 bucket"
  value       = aws_s3_bucket.edtech.arn
}

output "dynamodb_table_arns" {
  description = "ARNs of all three DynamoDB tables"
  value = {
    user_stats    = aws_dynamodb_table.user_stats.arn
    daily_reports = aws_dynamodb_table.daily_reports.arn
    course_stats  = aws_dynamodb_table.course_stats.arn
  }
}

output "iam_instance_profile_name" {
  description = "IAM instance profile to attach to EC2-2"
  value       = aws_iam_instance_profile.ec2_2_profile.name
}

output "iam_role_arn" {
  description = "ARN of the IAM role attached to EC2-2"
  value       = aws_iam_role.ec2_2_role.arn
}
