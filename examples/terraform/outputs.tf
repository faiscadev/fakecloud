# SQS
output "sqs_standard_url" {
  description = "URL of the standard SQS queue"
  value       = aws_sqs_queue.standard.url
}

output "sqs_standard_arn" {
  description = "ARN of the standard SQS queue"
  value       = aws_sqs_queue.standard.arn
}

output "sqs_fifo_url" {
  description = "URL of the FIFO SQS queue"
  value       = aws_sqs_queue.fifo.url
}

output "sqs_fifo_arn" {
  description = "ARN of the FIFO SQS queue"
  value       = aws_sqs_queue.fifo.arn
}

# SNS
output "sns_topic_arn" {
  description = "ARN of the SNS topic"
  value       = aws_sns_topic.notifications.arn
}

output "sns_subscription_arn" {
  description = "ARN of the SNS-to-SQS subscription"
  value       = aws_sns_topic_subscription.sqs_sub.arn
}

# SSM
output "ssm_string_param_name" {
  description = "Name of the String SSM parameter"
  value       = aws_ssm_parameter.string_param.name
}

output "ssm_secure_param_name" {
  description = "Name of the SecureString SSM parameter"
  value       = aws_ssm_parameter.secure_param.name
}

# IAM
output "iam_role_arn" {
  description = "ARN of the IAM role"
  value       = aws_iam_role.lambda_exec.arn
}

output "iam_policy_arn" {
  description = "ARN of the IAM policy"
  value       = aws_iam_policy.lambda_logging.arn
}

# EventBridge
output "eventbridge_rule_arn" {
  description = "ARN of the EventBridge rule"
  value       = aws_cloudwatch_event_rule.every_minute.arn
}
