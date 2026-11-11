+++
title = "Cross-service integration tests"
description = "fakecloud actually executes the wiring between services. Here's every supported integration."
weight = 2
+++

The hardest bugs in AWS applications live in the wiring between services. A Lambda that's supposed to trigger on S3 uploads but doesn't. An EventBridge rule that fires but delivers to the wrong target. A Step Functions state machine that waits forever because the Lambda integration is shaped wrong.

Mocks can't catch those bugs. They test each service in isolation, and the wiring between them is exactly where the bugs hide.

fakecloud actually executes the cross-service wiring. When an EventBridge rule matches, it really delivers to the target. When an SES receipt rule evaluates, it really invokes the Lambda. When an S3 object is uploaded, it really publishes the notification. Tests that exercise end-to-end behavior work the same way they would against real AWS.

## Supported integrations

**Messaging and eventing:**

- **SNS -> SQS / Lambda / HTTP** — Fan-out delivery to all subscription types.
- **S3 -> SNS / SQS / Lambda / EventBridge** — Bucket notifications on object create/delete.
- **S3 -> Object Lambda Access Points** — `GetObject` against an Object Lambda Access Point invokes the configured transforming Lambda. The Lambda's `WriteGetObjectResponse` body is actually persisted and returned to the caller (including status code, headers, and `ErrorCode` / `ErrorMessage`), so the response your code sees matches what AWS would return.
- **EventBridge -> SNS / SQS / Lambda / Logs / Kinesis / Step Functions / HTTP** — Rules deliver to targets on schedule or event match, including API Destinations.
- **SQS -> Lambda** — Event source mapping polls queues. Honors `FilterCriteria` (drop non-matching), `MaximumBatchingWindowInSeconds` (hold partial batches), and `FunctionResponseTypes=[ReportBatchItemFailures]` (Lambda response `{"batchItemFailures":[{"itemIdentifier":...}]}` retries only the failed messages).
- **Kinesis -> Lambda** — Event source mapping polls shards. Honors `FilterCriteria` (advances past dropped records) and `StartingPosition` (`TRIM_HORIZON` / `LATEST` / `AT_TIMESTAMP`) on first poll.
- **DynamoDB Streams -> Lambda** — Event source mapping polls stream records. Honors `FilterCriteria` (advances past dropped records) and `StartingPosition` (`TRIM_HORIZON` / `LATEST`).
- **DynamoDB -> Kinesis** — Table changes stream to Kinesis Data Streams.
- **CloudWatch Logs -> Lambda / Kinesis / SQS** — Subscription filters deliver log events.
- **CloudWatch Logs delivery configuration** — `PutDeliverySource` / `PutDeliveryDestination` / `CreateDelivery` persist the source/destination/delivery tuples that AWS uses to wire services like API GW, AppSync, and Bedrock into Logs / Firehose / S3. The standard `delivery-destination-policy` templates (Logs, Firehose, S3) are accepted and returned exactly as AWS does, and the configuration survives across `Describe*` calls so IaC tools see a stable shape.
- **Lambda async destinations -> SQS / SNS / EventBridge / Lambda** — `InvocationType=Event` invocations route their result through `OnSuccess` / `OnFailure` using AWS's standard destinations record schema.
- **Lambda -> CloudWatch Metrics** — Every `Invoke` (sync or async) publishes the standard `AWS/Lambda` namespace metrics (`Invocations`, `Errors`, `Throttles`, `Duration`, `ConcurrentExecutions`) dimensioned by `FunctionName`. `cloudwatch:GetMetricStatistics` and `GetMetricData` return the recorded data points, so dashboards and alarm tests work without a separate metrics stub.

**Identity and auth:**

- **Cognito -> Lambda** — All 12 triggers: PreSignUp, PostConfirmation, PreAuthentication, PostAuthentication, CustomMessage, PreTokenGeneration, UserMigration, DefineAuthChallenge, CreateAuthChallenge, VerifyAuthChallengeResponse, CustomEmailSender, CustomSMSSender.
- **Cognito -> SES** — Verification emails for SignUp / ResendConfirmationCode / ForgotPassword / GetUserAttributeVerificationCode dispatch through SES (CustomEmailSender Lambda takes precedence when configured).
- **Cognito -> SNS** — SMS verification codes dispatch through SNS as `sms_messages` (CustomSMSSender Lambda takes precedence when configured).

**Email:**

- **SES -> SNS / EventBridge / Kinesis / Firehose / CloudWatch Logs** — Configuration set event destinations fan out send/delivery/bounce/complaint/open/click/reject/renderingFailure events to every destination type AWS supports. The same event payload AWS would emit is delivered to each destination — `PutRecord` for Kinesis, `PutRecordBatch` for Firehose, `PutLogEvents` for CloudWatch Logs.
- **SES Inbound -> S3 / SNS / Lambda** — Receipt rules evaluate inbound email and execute S3, SNS, and Lambda actions for real.
- **SES -> SMTP relay (outbound)** — Set `FAKECLOUD_SES_SMTP_RELAY=smtp://user:pass@host:port` and `SendEmail` / `SendRawEmail` / `SendBulkEmail` actually deliver to the configured SMTP server in addition to recording the message. Useful for end-to-end tests against MailHog, Mailpit, or a real mail server. Unset (the default) keeps SES purely in-memory.

  ```bash
  FAKECLOUD_SES_SMTP_RELAY=smtp://mailhog:1025 fakecloud
  ```

**Orchestration and APIs:**

- **Step Functions -> Lambda / SQS / SNS / EventBridge / DynamoDB** — Task states invoke Lambda, send SQS messages, publish to SNS topics, put EventBridge events, and read/write DynamoDB items.
- **Step Functions -> Step Functions (nested executions)**: Task states with resource `arn:aws:states:::states:startExecution` (and the `.sync`, `.sync:2` and `.waitForTaskToken` variants) start a child state machine execution. The `.sync` forms wait for the child to terminate and return its description, with the child's output in `Output`; the fire-and-forget form returns immediately with the child's `ExecutionArn` and `StartDate`.
- **API Gateway v2 -> Lambda** — HTTP API routes invoke Lambda functions with proxy integration v2.0 format.
- **API Gateway v2 -> CloudWatch Logs (access logs)** — Stages with `AccessLogSettings.DestinationArn` set to a Logs log group publish one `PutLogEvents` entry per request, formatted per the stage's `Format` template (defaulting to the standard `$context` placeholders).

**Infrastructure:**

- **CloudFormation -> Lambda / SNS** — Custom resources invoke via `ServiceToken`, stack events notify via `NotificationARNs`.
- **CloudFormation nested stacks + SAM transform** — `AWS::CloudFormation::Stack` resources fetch the child template (`TemplateURL` from S3 or an inline body) and provision its resources in a child stack, propagating outputs back via `Fn::GetAtt`. `Transform: AWS::Serverless-2016-10-31` expands `AWS::Serverless::Function` / `Api` / `SimpleTable` into native Lambda / API GW v2 / DynamoDB resources before provisioning.
- **CloudFormation provisioners (Firehose, Glue, Athena, Application Auto Scaling, SES, WAFv2)** — Stack templates create and update real resources for `AWS::KinesisFirehose::DeliveryStream`, `AWS::Glue::Database` / `Table` / `Crawler` / `Job`, `AWS::Athena::WorkGroup` / `NamedQuery`, `AWS::ApplicationAutoScaling::ScalableTarget` / `ScalingPolicy`, `AWS::SES::ConfigurationSet` / `EventDestination` / `Template`, and `AWS::WAFv2::WebACL` / `IPSet` / `RuleGroup`. `Fn::GetAtt` on SES `ConfigurationSet.Arn` and WAFv2 `WebACL.Arn` / `Id` / `Capacity` returns the live values.
- **Secrets Manager -> Lambda** — Rotation invokes Lambda for all 4 steps.
- **Secrets Manager -> KMS** - When a secret has `KmsKeyId`, `CreateSecret` / `PutSecretValue` call `kms:GenerateDataKey` and `GetSecretValue` calls `kms:Decrypt` with the encryption context Secrets Manager uses, `{SecretARN: <arn>, SecretVersionId: <version>}`. The `aws/secretsmanager` AWS-managed key is created on first use. A key KMS refuses fails the call with Secrets Manager's error (`AccessDeniedException` "Access to KMS is not allowed", `InvalidParameterException` for a missing key). All KMS calls are recorded at `/_fakecloud/kms/usage`.
- **SSM SecureString -> KMS** - `PutParameter` with `Type=SecureString` calls `kms:Encrypt`; `GetParameter*` with `WithDecryption=true` calls `kms:Decrypt`. The encryption context is `{PARAMETER_ARN: <arn>}` and the default `aws/ssm` key is created on first use; pass `KeyId` for a customer-managed key. A key KMS refuses is `AccessDeniedException` or `InvalidKeyId`.
- **S3 SSE-KMS -> KMS** - `PutObject` with `ServerSideEncryption=aws:kms` calls `kms:GenerateDataKey`; `GetObject` decrypts via `kms:Decrypt`. The encryption context is `{aws:s3:arn: arn:aws:s3:::<bucket>/<key>}` (the bucket ARN with an S3 Bucket Key) and ranged reads are sliced from plaintext, not the stored ciphertext envelope. The default `aws/s3` key is created on first use. A key KMS refuses is 403 `AccessDenied`, a missing one `KMS.NotFoundException`.
- **SQS encrypted queue -> KMS** — `SendMessage` on a queue with `KmsMasterKeyId` calls `kms:GenerateDataKey`; `ReceiveMessage` calls `kms:Decrypt` and returns the plaintext body. Encryption context: `{aws:sqs:arn: <queue-arn>}`. The on-queue body is the ciphertext envelope; only the returned copy is decrypted.
- **SNS encrypted topic -> KMS** - `Publish` to a topic with `KmsMasterKeyId` records the matching `kms:GenerateDataKey` and `kms:Decrypt` audit-trail records (SNS encrypts at rest then decrypts to fan-out). Encryption context: `{aws:sns:topicArn: <topic-arn>}`. A key KMS refuses fails `Publish` with `KMSAccessDenied` / `KMSDisabled` / `KMSInvalidState` / `KMSNotFound`.
- **SNS -> SQS encrypted queue (SSE fan-out)** — SNS topic fan-out into an SSE-enabled SQS queue stores the ciphertext envelope on the queue and records `kms:GenerateDataKey` against the queue's CMK. `ReceiveMessage` decrypts back to the original SNS notification body. The KMS audit trail at `/_fakecloud/kms/usage` reflects both the SNS-side and SQS-side calls.
- **SNS -> SMTP relay (email subscriptions)** — Email and email-json subscriptions deliver via the same `FAKECLOUD_SES_SMTP_RELAY` relay that SES uses. Without the env var, email deliveries are recorded in memory; with it set, the relay actually sends a MIME message.
- **DynamoDB encrypted table -> KMS** — `PutItem` / `UpdateItem` on a table with `SSESpecification.SSEType=KMS` records `kms:GenerateDataKey`; `GetItem` / `Query` / `Scan` records `kms:Decrypt`. Item bodies are not actually encrypted — fakecloud emits the audit-trail records the AWS API would produce so callers can assert KMS usage on encrypted tables.
- **S3 Lifecycle** — Background expiration and storage class transitions.
- **EventBridge Scheduler** — Cron and rate-based rules fire on schedule.
- **RDS Postgres -> Lambda (`aws_lambda` extension)** — The prebuilt `ghcr.io/faiscadev/fakecloud-postgres` image ships the `aws_lambda` extension. `SELECT aws_lambda.invoke('my-fn', ...)` reaches the fakecloud Lambda control plane and returns the function's payload, so tests for Postgres triggers that fan out to Lambda run end-to-end.
- **RDS Postgres -> S3 (`aws_s3` extension)** — `aws_s3.query_export_to_s3` and `aws_s3.table_import_from_s3` move CSV between Postgres and fakecloud S3 buckets via the same image.
- **RDS MySQL / MariaDB -> Lambda (Aurora `lambda_async`)** — The MySQL and MariaDB images expose `mysql.lambda_async('arn:...', '{"payload":1}')` and fire-and-forget invoke against fakecloud Lambda.
- **RDS -> EventBridge** — DB instance and snapshot lifecycle ops (create, modify, delete, reboot, start, stop, snapshot create/delete, restore) emit `aws.rds` events that match the AWS event schema.
- **ECS -> EventBridge** — Task state transitions emit `ECS Task State Change` events on the default bus. Event `detail` carries the task ARN, cluster ARN, last status, stop code/reason on STOPPED, and a per-container summary including exit code.
- **ECS -> ELBv2** — A service registered with `loadBalancers[]` calls `elbv2:RegisterTargets` against the named target group every time a task reaches `RUNNING`, and `DeregisterTargets` when the task stops or the service scales down. The target group's `Targets` list reflects the live task IPs, so `DescribeTargetHealth` returns what the data plane would.
- **ECS -> ECR** — Task definitions that reference AWS-private-ECR URIs (`<account>.dkr.ecr.<region>.amazonaws.com/<repo>:<tag>`) resolve against fakecloud's local OCI v2 endpoint at runtime. The runtime pulls from `127.0.0.1:<port>/<repo>:<tag>`, retags to the AWS URI, and runs the container under the user-visible image name. Same resolution applies to Lambda functions deployed with `PackageType=Image` and a fakecloud ECR `Code.ImageUri`.
- **ECS awslogs -> CloudWatch Logs** — Containers declaring `logDriver=awslogs` get every captured stdout/stderr line forwarded to fakecloud Logs. The runtime honors `awslogs-create-group=true`, creates a stream named `<prefix>/<container-name>/<task-id>`, and downstream subscription filters fire on the appended events.
- **ECS task secrets -> Secrets Manager / SSM** — Container `secrets[]` entries with a SecretsManager or SSM Parameter Store ARN are resolved synchronously at task launch and injected as environment variables before `docker run`. Missing values fail the task with `stopCode=TaskFailedToStart`, mirroring real ECS.
- **ECS task role -> IAM credentials** — Tasks registered with a `taskRoleArn` get the ECS agent's credentials path in `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` (`/v2/credentials/<task-id>`), answered at `http://169.254.170.2` inside the task's network. AWS SDKs pick this up via the default credential-provider chain so `aws sts get-caller-identity` (and any other SDK call) works from inside the container. If the task's network can't be routed to fakecloud (no `NET_ADMIN`, no NAT support), the task falls back to an `AWS_CONTAINER_CREDENTIALS_FULL_URI` that the SDKs refuse; see [ECS task role credentials](/docs/services/ecs/#task-role-credentials).
- **ECR signing -> KMS** — When a repository has `cosign` keyed-mode (ECDSA-P256) verification configured, `PutImage` requires a matching signature manifest in the registry. Unsigned or wrong-key images are rejected.
- **IAM PassRole trust enforcement** — Lambda `CreateFunction` and ECS `RegisterTaskDefinition` / `RunTask` overrides reject role ARNs whose `AssumeRolePolicyDocument` doesn't list the calling service principal (`lambda.amazonaws.com`, `ecs-tasks.amazonaws.com`), the same way real AWS does.

## Testing a cross-service flow

The pattern is the same for all of them: configure the wiring via the normal AWS SDK, trigger the upstream event, then assert on the downstream state via the fakecloud SDK.

Example — S3 upload triggers Lambda via an event source:

```typescript
import { FakeCloud } from "fakecloud";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const fc = new FakeCloud();

// (assume bucket, Lambda, and notification config already created via SDK)

await new S3Client({ endpoint: "http://localhost:4566", /* ... */ }).send(
  new PutObjectCommand({ Bucket: "my-bucket", Key: "uploads/hello.txt", Body: "hi" })
);

// Lambda really ran. Assert on its invocations.
const { invocations } = await fc.lambda.getInvocations();
expect(invocations).toHaveLength(1);
expect(invocations[0].event.Records[0].s3.object.key).toBe("uploads/hello.txt");
```

No mocks. The S3 upload actually triggered the notification, which actually invoked the Lambda, which actually ran and recorded its invocation — all in the same fakecloud process. If any step of that wiring is broken in your code, the test fails the same way it would against real AWS.

## What doesn't exist yet

If you need a cross-service integration that isn't in the list above, [open an issue](https://github.com/faiscadev/fakecloud/issues). The ones that exist were driven by real user needs, and the list keeps growing.
