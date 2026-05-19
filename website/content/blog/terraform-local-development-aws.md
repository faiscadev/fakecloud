+++
title = "Terraform local development for AWS: full flow with fakecloud"
date = 2026-04-22
description = "Run Terraform against AWS locally with fakecloud. Provider endpoints config, tflocal, CI setup. Free, open-source, no account required. Works with the upstream terraform-provider-aws TestAcc suites."

[extra]
author = "Lucas Vieira"
+++

Writing Terraform for AWS means iterating against AWS. Every `terraform apply` costs money, takes minutes, and leaves resources behind that you have to remember to destroy. For local dev and CI, you want the same AWS provider running against something that behaves like AWS but isn't.

This guide walks through local Terraform development for AWS with [fakecloud](https://github.com/faiscadev/fakecloud) — a free, open-source AWS emulator that targets 100% behavioral conformance, depth-first. fakecloud's CI runs the upstream `hashicorp/terraform-provider-aws` `TestAcc*` suites against itself on every commit, so Terraform flows that work against real AWS should work against fakecloud.

## Install fakecloud

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Listens on `http://localhost:4566`. ~500ms startup. No account, no token.

## Option 1: provider `endpoints` block (explicit)

Most explicit and portable. Works with any Terraform/OpenTofu version.

```hcl
# main.tf
provider "aws" {
  access_key                  = "test"
  secret_key                  = "test"
  region                      = "us-east-1"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true

  endpoints {
    s3             = "http://localhost:4566"
    sqs            = "http://localhost:4566"
    sns            = "http://localhost:4566"
    dynamodb       = "http://localhost:4566"
    lambda         = "http://localhost:4566"
    iam            = "http://localhost:4566"
    sts            = "http://localhost:4566"
    kms            = "http://localhost:4566"
    secretsmanager = "http://localhost:4566"
    ssm            = "http://localhost:4566"
    logs           = "http://localhost:4566"
    cloudformation = "http://localhost:4566"
    events         = "http://localhost:4566"
    scheduler      = "http://localhost:4566"
    ses            = "http://localhost:4566"
    sesv2          = "http://localhost:4566"
    cognitoidp     = "http://localhost:4566"
    kinesis        = "http://localhost:4566"
    rds            = "http://localhost:4566"
    elasticache    = "http://localhost:4566"
    sfn            = "http://localhost:4566"
    apigatewayv2   = "http://localhost:4566"
    bedrock        = "http://localhost:4566"
    bedrockruntime = "http://localhost:4566"
  }
}
```

Now `terraform init && terraform apply` talks to fakecloud.

## Option 2: `AWS_ENDPOINT_URL` (minimal config)

Cleaner. AWS provider v5.63+ respects `AWS_ENDPOINT_URL` from environment. No `endpoints` block needed.

```sh
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_REGION=us-east-1

terraform init
terraform apply
```

The provider block collapses to just auth scaffolding:

```hcl
provider "aws" {
  access_key                  = "test"
  secret_key                  = "test"
  region                      = "us-east-1"
  skip_credentials_validation = true
  skip_metadata_api_check     = true
  skip_requesting_account_id  = true
  s3_use_path_style           = true
}
```

## Option 3: `tflocal` wrapper

If you're coming from LocalStack and have a `tflocal` setup, the endpoint override works the same way — just point at fakecloud instead.

```sh
AWS_ENDPOINT_URL=http://localhost:4566 tflocal apply
```

## End-to-end example: S3 + Lambda + SQS

```hcl
# main.tf
resource "aws_s3_bucket" "uploads" {
  bucket = "uploads-${random_id.suffix.hex}"
}

resource "random_id" "suffix" {
  byte_length = 4
}

resource "aws_sqs_queue" "jobs" {
  name = "jobs"
}

resource "aws_iam_role" "lambda" {
  name = "lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action = "sts:AssumeRole"
    }]
  })
}

resource "aws_lambda_function" "processor" {
  filename      = data.archive_file.fn.output_path
  function_name = "processor"
  role          = aws_iam_role.lambda.arn
  handler       = "index.handler"
  runtime       = "nodejs20.x"
  source_code_hash = data.archive_file.fn.output_base64sha256
}

data "archive_file" "fn" {
  type        = "zip"
  output_path = "fn.zip"
  source {
    filename = "index.js"
    content  = "exports.handler = async (event) => ({ ok: true, received: event })"
  }
}

resource "aws_lambda_event_source_mapping" "sqs_to_lambda" {
  event_source_arn = aws_sqs_queue.jobs.arn
  function_name    = aws_lambda_function.processor.function_name
  batch_size       = 1
}

resource "aws_s3_bucket_notification" "uploads_to_sqs" {
  bucket = aws_s3_bucket.uploads.id
  queue {
    queue_arn = aws_sqs_queue.jobs.arn
    events    = ["s3:ObjectCreated:*"]
  }
}

output "bucket_id" {
  value = aws_s3_bucket.uploads.id
}
```

Apply it against fakecloud:

```sh
terraform apply -auto-approve
```

Upload an object and watch the Lambda fire:

```sh
echo "hello" | aws --endpoint-url http://localhost:4566 s3 cp - s3://$(terraform output -raw bucket_id)/file.txt
aws --endpoint-url http://localhost:4566 logs tail /aws/lambda/processor
```

That's S3 notification -> SQS queue -> Lambda event source mapping -> real Node runtime executing your handler. End-to-end, no stubs. Same flow as real AWS.

## CI: Terraform plan/apply in GitHub Actions

```yaml
jobs:
  terraform:
    runs-on: ubuntu-latest
    env:
      AWS_ENDPOINT_URL: http://localhost:4566
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_REGION: us-east-1
    steps:
      - uses: actions/checkout@v4
      - uses: hashicorp/setup-terraform@v3

      - name: Start fakecloud
        run: |
          curl -fsSL https://fakecloud.dev/install.sh | bash
          fakecloud &
          for i in $(seq 1 30); do curl -sf http://localhost:4566/_fakecloud/health && break; sleep 1; done
          curl -sf http://localhost:4566/_fakecloud/health

      - name: terraform init
        run: terraform init

      - name: terraform plan
        run: terraform plan -out=tfplan

      - name: terraform apply
        run: terraform apply -auto-approve tfplan
```

~500ms startup means this adds negligible time to the job.

## State backend

For local dev, default `local` backend is fine. If your real config uses `s3` backend, you can point that at fakecloud too:

```hcl
terraform {
  backend "s3" {
    bucket                      = "tf-state"
    key                         = "terraform.tfstate"
    region                      = "us-east-1"
    endpoint                    = "http://localhost:4566"
    skip_credentials_validation = true
    skip_metadata_api_check     = true
    skip_region_validation      = true
    force_path_style            = true
  }
}
```

Pre-create the bucket:

```sh
aws --endpoint-url http://localhost:4566 s3 mb s3://tf-state
```

## Why this works at parity

fakecloud targets 100% behavioral conformance per implemented service, validated on every commit against AWS's own Smithy models. On top of that, CI runs the upstream `hashicorp/terraform-provider-aws` `TestAcc*` suites — the same acceptance tests HashiCorp runs against real AWS — so provider-level behavior (waiters, retries, field presence, ARN formats) matches.

If a Terraform flow that works against real AWS doesn't work against fakecloud, that's a bug. [Open an issue](https://github.com/faiscadev/fakecloud/issues) and it gets fixed.

## Links

- Install: `curl -fsSL https://fakecloud.dev/install.sh | bash`
- Repo: [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- LocalStack migration guide: [Migrating from LocalStack to fakecloud](/blog/migrate-from-localstack/)
- CI guide: [Integration testing AWS in GitHub Actions](/blog/integration-testing-aws-in-ci/)
- Issues: [github.com/faiscadev/fakecloud/issues](https://github.com/faiscadev/fakecloud/issues)
