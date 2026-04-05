# Terraform AWS Provider + FakeCloud

This example provisions AWS resources against a local FakeCloud server using the
official `hashicorp/aws` Terraform provider.

## Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.0
- FakeCloud server built and available (`cargo build --bin fakecloud-server`)

## Usage

1. Start FakeCloud:

   ```sh
   cd <fakecloud-repo-root>
   cargo run --bin fakecloud-server   # listens on 0.0.0.0:4566 by default
   ```

2. In another terminal, run Terraform:

   ```sh
   cd examples/terraform
   terraform init
   terraform plan
   terraform apply -auto-approve
   ```

3. Tear down:

   ```sh
   terraform destroy -auto-approve
   ```

## Resources provisioned

| Resource                         | Type             |
|----------------------------------|------------------|
| `aws_sqs_queue.standard`         | SQS standard     |
| `aws_sqs_queue.fifo`             | SQS FIFO         |
| `aws_sns_topic.notifications`    | SNS topic        |
| `aws_sns_topic_subscription`     | SNS sub (SQS)    |
| `aws_ssm_parameter.string_param` | SSM String       |
| `aws_ssm_parameter.secure_param` | SSM SecureString |
| `aws_iam_role.lambda_exec`       | IAM role         |
| `aws_iam_policy.lambda_logging`  | IAM policy       |
| `aws_cloudwatch_event_rule`      | EventBridge rule |
| `aws_cloudwatch_event_target`    | EventBridge tgt  |

## Notes

The Terraform AWS provider calls additional read-back and tagging APIs after
creating each resource. All the actions referenced by the resources above are
now implemented in FakeCloud, including `ListQueueTags`, `TagQueue`, `GetPolicy`,
`ListRolePolicies`, `DetachRolePolicy`, and SSM `PutParameter` with overwrite
support.

S3 resources can also be managed via Terraform by adding the S3 endpoint:

```hcl
endpoints {
  s3 = "http://localhost:4566"
}
```

Example S3 resources: `aws_s3_bucket`, `aws_s3_object`, `aws_s3_bucket_policy`,
`aws_s3_bucket_versioning`, `aws_s3_bucket_lifecycle_configuration`.
