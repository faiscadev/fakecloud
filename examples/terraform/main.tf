terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region                      = "us-east-1"
  access_key                  = "test"
  secret_key                  = "test"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true

  endpoints {
    s3               = "http://localhost:4566"
    sqs              = "http://localhost:4566"
    sns              = "http://localhost:4566"
    iam              = "http://localhost:4566"
    sts              = "http://localhost:4566"
    ssm              = "http://localhost:4566"
    eventbridge      = "http://localhost:4566"
  }

  s3_use_path_style = true
}

# ---------------------------------------------------------------------------
# SQS — standard queue
# ---------------------------------------------------------------------------
resource "aws_sqs_queue" "standard" {
  name                       = "tf-test-standard"
  delay_seconds              = 0
  max_message_size           = 262144
  message_retention_seconds  = 345600
  visibility_timeout_seconds = 30
}

# SQS — FIFO queue
resource "aws_sqs_queue" "fifo" {
  name                        = "tf-test-fifo.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
}

# ---------------------------------------------------------------------------
# SNS — topic + SQS subscription
# ---------------------------------------------------------------------------
resource "aws_sns_topic" "notifications" {
  name = "tf-test-notifications"
}

resource "aws_sns_topic_subscription" "sqs_sub" {
  topic_arn = aws_sns_topic.notifications.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.standard.arn
}

# ---------------------------------------------------------------------------
# SSM Parameter Store
# ---------------------------------------------------------------------------
resource "aws_ssm_parameter" "string_param" {
  name  = "/tf-test/config/greeting"
  type  = "String"
  value = "hello from terraform"
}

resource "aws_ssm_parameter" "secure_param" {
  name  = "/tf-test/secret/api-key"
  type  = "SecureString"
  value = "supersecret123"
}

# ---------------------------------------------------------------------------
# IAM — role + policy
# ---------------------------------------------------------------------------
resource "aws_iam_role" "lambda_exec" {
  name = "tf-test-lambda-exec"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_policy" "lambda_logging" {
  name        = "tf-test-lambda-logging"
  description = "Allow Lambda to write CloudWatch logs"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents",
      ]
      Effect   = "Allow"
      Resource = "arn:aws:logs:*:*:*"
    }]
  })
}

# ---------------------------------------------------------------------------
# EventBridge — rule + target
# ---------------------------------------------------------------------------
resource "aws_cloudwatch_event_rule" "every_minute" {
  name                = "tf-test-every-minute"
  description         = "Fires every minute"
  schedule_expression = "rate(1 minute)"
}

resource "aws_cloudwatch_event_target" "sqs_target" {
  rule = aws_cloudwatch_event_rule.every_minute.name
  arn  = aws_sqs_queue.standard.arn
}
