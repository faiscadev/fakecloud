+++
title = "Lambda"
description = "Real code execution in Docker containers across 31 runtimes. Event source mappings, warm container reuse."
weight = 5
+++

fakecloud implements **73 of 73** Lambda operations at 100% Smithy conformance. Unlike most emulators, **Lambda functions actually execute** — fakecloud runs your code inside real Docker containers.

## Supported features

- **Function CRUD** — create, update, delete, list, get
- **Real code execution** — functions run in Docker containers with the official AWS Lambda runtime images
- **31 runtimes** — Node.js (16/18/20/22/24/26), Python (3.8 through 3.15), Java (`java8.al2`, `java8.al2023`, 11, `java11.al2023`, 17, `java17.al2023`, 21, 25), Go (1.x), Ruby (3.2/3.3/3.4/4.0), .NET (8/10), `provided.al2`, `provided.al2023`. Node.js 26 and Python 3.15 run on AWS's `-preview` base images, the only ones published for them so far
- **Event source mappings** — SQS, Kinesis, DynamoDB Streams polling loops with **`FilterCriteria`** (the same pattern matcher as EventBridge rules, with SQS JSON body decode and base64 Kinesis `data` decode), **`StartingPosition`** (`TRIM_HORIZON` / `LATEST` / `AT_TIMESTAMP` for Kinesis, `TRIM_HORIZON` / `LATEST` for DDB Streams), **`MaximumBatchingWindowInSeconds`** (SQS), and **`FunctionResponseTypes=[ReportBatchItemFailures]`** for SQS partial-batch failure semantics
- **Layers** — create, publish, attach to functions; layer ZIP content is extracted into `/opt` of the runtime container at invoke time, so Python `import`, Node `require`, and `LD_LIBRARY_PATH` lookups resolve against attached layers exactly as on real AWS
- **Environment variables**: the function's own variables plus the environment real Lambda provides: `AWS_REGION`/`AWS_DEFAULT_REGION` (the function's region), execution-role credentials (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`, a registered session for the function's role named after the function, so SDK calls sign as `assumed-role/<role>/<function>` and verify under `--verify-sigv4`), and `AWS_LAMBDA_FUNCTION_NAME`/`_VERSION`/`_MEMORY_SIZE`, `AWS_LAMBDA_LOG_GROUP_NAME`/`_LOG_STREAM_NAME`, `AWS_EXECUTION_ENV`. `AWS_ENDPOINT_URL` points the SDK back at fakecloud (overridable by setting it on the function). Reserved keys are rejected on `CreateFunction`/`UpdateFunctionConfiguration` with `InvalidParameterValueException`, as on AWS. The execution role must trust `lambda.amazonaws.com` (the trust policy read is the one of the role the ARN names, in the account that owns it, and for another account's role also that of the same-named role in the function's account, which the session is minted for); under `--iam strict` it must also be in the caller's account (`AccessDeniedException: Cross-account pass role is not allowed.` otherwise; `--iam soft` logs the would-be denial), while the default mode accepts another account's role (e.g. `000000000000` templates) and mints the session in the function's account. Applied on create, update and CloudFormation alike
- **Aliases and versions** — publish, point aliases at versions; alias-based weighted routing (`RoutingConfig`) is enforced at invoke time so traffic splits between versions exactly as on AWS
- **Concurrency controls** — reserved concurrency enforced at invocation time: per-function reservation caps in-flight invocations and excess requests are rejected with `TooManyRequestsException` (HTTP 429) and `Reason=ReservedFunctionConcurrentInvocationLimitExceeded`
- **`UpdateFunctionCode` from S3** — `S3Bucket`/`S3Key`/`S3ObjectVersion` fetches the ZIP from the fakecloud S3 implementation; the stored `CodeSha256` is the real SHA-256 of the fetched bytes
- **CloudWatch metrics** — every invoke publishes `Invocations`, `Errors`, `Duration`, `Throttles`, and `ConcurrentExecutions` to the `AWS/Lambda` namespace, queryable via `GetMetricStatistics` / `GetMetricData`
- **Resource-based policies** — the statement API (`AddPermission` / `RemovePermission` / `GetPolicy`) and the document API (`PutResourcePolicy` / `GetResourcePolicy` / `DeleteResourcePolicy`) address one policy per function or qualifier: a `RevisionId` acts as an optimistic-concurrency precondition (`PreconditionFailedException` on mismatch) and an `Allow` open to every principal with no `Condition` is rejected with `PublicPolicyException`
- **`GetAccountSettings`** — returns real `AccountUsage` counters (`FunctionCount`, `TotalCodeSize`) and `AccountLimit` so SDKs that pre-flight account quotas see live values
- **Warm container reuse**: subsequent invocations of the same function reuse the container; each version has its own warm pool, and a configuration change (environment, memory, timeout, role, handler, tags) starts a fresh instance once the old one finishes any in-flight invocation
- **Async invoke destinations** — `OnSuccess` / `OnFailure` routes the invocation result to SQS, SNS, EventBridge, or another Lambda by ARN scheme; record matches the AWS destinations schema (`requestContext`, `requestPayload`, `responseContext`, `responsePayload`)
- **`InvocationType` honored** — `Event` returns 202 and runs in the background, `RequestResponse` blocks for the result, `DryRun` validates without executing

## Protocol

REST. Path-based routing for invoke operations, JSON for control plane.

## Introspection

- `GET /_fakecloud/lambda/invocations` — list all Lambda invocations with input/output/errors
- `GET /_fakecloud/lambda/warm-containers` — list currently warm containers
- `POST /_fakecloud/lambda/{function-name}/evict-container` — force a cold start on the next invoke
- `GET /_fakecloud/lambda/layer-content/{account-id}/{layer-name}/{version}.zip` — download the raw layer ZIP. Returned as the `Content.Location` from `PublishLayerVersion` and `GetLayerVersion`, so AWS SDK / Terraform clients that re-download a layer get the actual bytes

## Event source mapping example: FilterCriteria + partial batch failure

```typescript
import { LambdaClient, CreateEventSourceMappingCommand } from "@aws-sdk/client-lambda";

await new LambdaClient({ endpoint: "http://localhost:4566" }).send(
  new CreateEventSourceMappingCommand({
    FunctionName: "process-orders",
    EventSourceArn: "arn:aws:sqs:us-east-1:000000000000:orders",
    BatchSize: 10,
    MaximumBatchingWindowInSeconds: 5,
    // Only deliver paid orders.
    FilterCriteria: {
      Filters: [{ Pattern: '{"body": {"status": ["paid"]}}' }],
    },
    // Opt into partial-batch failure: Lambda returns
    // {"batchItemFailures":[{"itemIdentifier":"<msgId>"}]}
    // and only those messages stay on the queue for retry.
    FunctionResponseTypes: ["ReportBatchItemFailures"],
  })
);
```

## Cross-service triggers

Lambda is a target for most event-producing services:

- **SQS -> Lambda** — Event source mapping polls the queue
- **Kinesis -> Lambda** — Event source mapping polls shards
- **DynamoDB Streams -> Lambda** — Event source mapping polls stream records
- **S3 -> Lambda** — Bucket notifications
- **SNS -> Lambda** — Topic subscriptions
- **EventBridge -> Lambda** — Rule targets
- **API Gateway v2 -> Lambda** — HTTP API proxy integration
- **Cognito -> Lambda** — Triggers (pre-signup, post-confirmation, pre/post-auth, custom message, token generation, migration, custom auth challenge)
- **Secrets Manager -> Lambda** — Rotation (all 4 steps)
- **CloudFormation -> Lambda** — Custom resources via `ServiceToken`
- **SES Inbound -> Lambda** — Receipt rule actions
- **Step Functions -> Lambda** — Task state integrations
- **CloudWatch Logs -> Lambda** — Subscription filters

## Gotchas

- **Requires a Docker socket.** Lambda needs access to `/var/run/docker.sock` to start and stop containers. Only use in environments you trust — Docker socket access is effectively host-level privilege.
- **First invocation of a runtime pulls the image.** Expect a slower first run while the Lambda runtime image downloads. Subsequent invocations are fast. For `PackageType=Image` functions, a transient failure pulling the function's image (rate limiting, a registry 5xx, a network timeout) is retried with backoff and falls back to a copy already cached locally; a deleted or access-denied image fails the start.
- **Cold vs. warm containers.** fakecloud reuses containers between invocations for the same function. Force a cold start via `/_fakecloud/lambda/{name}/evict-container`.

## Source

- [`crates/fakecloud-lambda`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-lambda)
- [AWS Lambda API reference](https://docs.aws.amazon.com/lambda/latest/api/welcome.html)
