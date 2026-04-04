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

| Resource                         | Type             | Status          |
|----------------------------------|------------------|-----------------|
| `aws_sqs_queue.standard`         | SQS standard     | BLOCKED         |
| `aws_sqs_queue.fifo`             | SQS FIFO         | BLOCKED         |
| `aws_sns_topic.notifications`    | SNS topic        | BLOCKED         |
| `aws_sns_topic_subscription`     | SNS sub (SQS)    | BLOCKED (dep)   |
| `aws_ssm_parameter.string_param` | SSM String       | OK (create)     |
| `aws_ssm_parameter.secure_param` | SSM SecureString  | OK (create)     |
| `aws_iam_role.lambda_exec`       | IAM role         | BLOCKED         |
| `aws_iam_policy.lambda_logging`  | IAM policy       | BLOCKED         |
| `aws_cloudwatch_event_rule`      | EventBridge rule | OK              |
| `aws_cloudwatch_event_target`    | EventBridge tgt  | BLOCKED (dep)   |

## Compatibility issues found

The Terraform AWS provider calls additional read-back and tagging APIs after
creating each resource. FakeCloud is missing several of these:

### SQS — missing `ListQueueTags` and `TagQueue`

After `CreateQueue`, Terraform calls `ListQueueTags` to read tags. This action
is not implemented in `fakecloud-sqs`.

**Error:**
```
listing tags for SQS Queue: operation error SQS: ListQueueTags,
StatusCode: 501, api error InvalidAction: action ListQueueTags not implemented
```

**Fix:** Implement `ListQueueTags` and `TagQueue` in `crates/fakecloud-sqs/src/service.rs`.

### SNS — `GetTopicAttributes` returns empty `Policy` attribute

After `CreateTopic`, Terraform calls `GetTopicAttributes` and tries to parse the
`Policy` attribute as JSON. FakeCloud returns an empty string, causing:

**Error:**
```
reading SNS Topic: parsing policy: unexpected end of JSON input
```

**Fix:** Return a valid default policy JSON in `GetTopicAttributes` in
`crates/fakecloud-sns/src/service.rs`. The `Policy` attribute should contain a
valid IAM policy document (e.g., the default SNS topic policy).

### IAM — missing `GetPolicy` and `ListRolePolicies`

Terraform reads back both resources after creation:
- `GetPolicy` to confirm the policy was created and read its metadata
- `ListRolePolicies` to enumerate inline policies on the role

**Errors:**
```
reading IAM Policy: operation error IAM: GetPolicy, StatusCode: 501,
  api error InvalidAction: action GetPolicy not implemented

reading inline policies for IAM role: operation error IAM: ListRolePolicies,
  StatusCode: 501, api error InvalidAction: action ListRolePolicies not implemented
```

**Fix:** Implement `GetPolicy`, `DeletePolicy`, `ListRolePolicies`, and
`DetachRolePolicy` in `crates/fakecloud-iam/src/iam_service.rs`.
(`DeletePolicy` and `DetachRolePolicy` will be needed for `terraform destroy`.)

### SSM — `PutParameter` rejects overwrites (idempotency)

On a second `terraform apply` (e.g., after partial failure), SSM returns
`ParameterAlreadyExists` because FakeCloud treats `PutParameter` as create-only.
AWS allows overwrite when the `Overwrite` flag is set (which Terraform sends).

**Fix:** Honor the `Overwrite` field in `PutParameter` in
`crates/fakecloud-ssm/src/service.rs`.

## Summary of missing actions for Terraform compatibility

| Service | Missing action       | Needed for                |
|---------|---------------------|---------------------------|
| SQS     | `ListQueueTags`     | read-back after create    |
| SQS     | `TagQueue`          | tagging support           |
| SNS     | (bug in Policy attr)| read-back after create    |
| IAM     | `GetPolicy`         | read-back after create    |
| IAM     | `DeletePolicy`      | terraform destroy         |
| IAM     | `ListRolePolicies`  | read-back after create    |
| IAM     | `DetachRolePolicy`  | terraform destroy         |
| SSM     | (PutParameter bug)  | idempotent apply          |
