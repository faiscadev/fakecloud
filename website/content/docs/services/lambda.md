+++
title = "Lambda"
description = "Real code execution in Docker containers across 31 runtimes. Event source mappings, warm container reuse."
weight = 5
+++

fakecloud implements **88 of 88** Lambda operations at 100% Smithy conformance. Unlike most emulators, **Lambda functions actually execute** — fakecloud runs your code inside real Docker containers.

## Supported features

- **Function CRUD** — create, update, delete, list, get
- **Real code execution** — functions run in Docker containers with the official AWS Lambda runtime images
- **Regional, like AWS**: functions, versions, aliases, function URL configs, concurrency settings (and the account concurrency limit), event source mappings, layers, code signing configs and account settings live in one region. The same function name can exist in several regions with different code and configuration, `ListFunctions` returns the request region's functions only, and a function ARN from another region is refused with `ResourceNotFoundException: Functions from '<region>' are not reachable in this region ('<region>')`. An `Invoke` by ARN or partial ARN reaches the account the ARN names; cross-service triggers (SNS, SQS/Kinesis/DynamoDB Streams pollers, EventBridge, Step Functions, API Gateway, S3, Cognito, CloudFormation) resolve the function in the region and account of its ARN
- **31 runtimes** — Node.js (16/18/20/22/24/26), Python (3.8 through 3.15), Java (`java8.al2`, `java8.al2023`, 11, `java11.al2023`, 17, `java17.al2023`, 21, 25), Go (1.x), Ruby (3.2/3.3/3.4/4.0), .NET (8/10), `provided.al2`, `provided.al2023`. Node.js 26 and Python 3.15 run on AWS's `-preview` base images, the only ones published for them so far
- **Event source mappings** — SQS, Kinesis, DynamoDB Streams polling loops with **`FilterCriteria`** (the same pattern matcher as EventBridge rules, with SQS JSON body decode and base64 Kinesis `data` decode), **`StartingPosition`** (`TRIM_HORIZON` / `LATEST` / `AT_TIMESTAMP` for Kinesis, `TRIM_HORIZON` / `LATEST` for DDB Streams), **`MaximumBatchingWindowInSeconds`** (SQS), and **`FunctionResponseTypes=[ReportBatchItemFailures]`** for SQS partial-batch failure semantics
- **Layers** — create, publish, attach to functions; layer ZIP content is extracted into `/opt` of the runtime container at invoke time, so Python `import`, Node `require`, and `LD_LIBRARY_PATH` lookups resolve against attached layers exactly as on real AWS
- **Layer sharing across accounts**: every layer operation accepts a layer ARN as `LayerName` and acts on the account and region that ARN names (so publishing through your own layer's ARN adds a version to that layer). A `LayerName` outside the model's length and pattern is refused with `ValidationException` before anything is looked up, except that the region of a layer ARN may be the request's own region even when it does not look like an AWS region (`--region local`), so the ARNs fakecloud mints there address their layer; the rest of the pattern (partition, 12-digit account, layer name, length) still applies. `AddLayerVersionPermission` writes the version's resource-based policy the way Lambda does: `Principal` must be a 12-digit account ID, an account root ARN in the layer's partition, or `*`; an account ID becomes `{"AWS": "arn:<partition>:iam::<account>:root"}`; `OrganizationId` (only with `Principal` `*`) adds a `StringEquals` `aws:PrincipalOrgID` condition; only `lambda:GetLayerVersion` can be granted; a duplicate `StatementId` is a `ResourceConflictException`. The policy carries a `RevisionId` that stays the same until the policy changes, and a stale `RevisionId` on `AddLayerVersionPermission` / `RemoveLayerVersionPermission` is a `PreconditionFailedException`. Another account reads a version (`GetLayerVersion`, `GetLayerVersionByArn`) only when that policy grants it to the account, to everyone, or to the account's organization (membership comes from fakecloud's Organizations state); otherwise the call fails with `AccessDeniedException: User: <caller> is not authorized to perform: lambda:GetLayerVersion on resource: <layer version ARN>`, with the same answer for a version that does not exist. `ListLayerVersions`, `PublishLayerVersion`, `DeleteLayerVersion`, `GetLayerVersionPolicy`, `AddLayerVersionPermission` and `RemoveLayerVersionPermission` through another account's ARN are always refused with `AccessDeniedException` (a layer version policy cannot grant them) and never touch the caller's own account. At attach time (`CreateFunction`, `UpdateFunctionConfiguration`, CloudFormation `AWS::Lambda::Function`): a missing version of your own layer is `InvalidParameterValueException: Layer version <arn> does not exist.`; a version of another account's layer that fakecloud holds needs the same grant, else `AccessDeniedException` (also for a missing version of that layer); a layer of another account that fakecloud holds no copy of at all (an AWS-published or third-party layer such as Powertools) attaches with `CodeSize` 0 and nothing mounted, so templates that use public layers keep deploying. That last case does let a caller tell an unknown foreign layer name from an unshared one, which the read operations do not. A function keeps the layer versions it already attached when the owner later revokes the grant.
- **Environment variables**: the function's own variables plus the environment real Lambda provides: `AWS_REGION`/`AWS_DEFAULT_REGION` (the function's region), execution-role credentials (`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`/`AWS_SESSION_TOKEN`, a registered session for the function's role named after the function, so SDK calls sign as `assumed-role/<role>/<function>` and verify under `--verify-sigv4`), and `AWS_LAMBDA_FUNCTION_NAME`/`_VERSION`/`_MEMORY_SIZE`, `AWS_LAMBDA_LOG_GROUP_NAME`/`_LOG_STREAM_NAME`, `AWS_EXECUTION_ENV`. `AWS_ENDPOINT_URL` points the SDK back at fakecloud (overridable by setting it on the function). Reserved keys are rejected on `CreateFunction`/`UpdateFunctionConfiguration` with `InvalidParameterValueException`, as on AWS. The execution role must trust `lambda.amazonaws.com` (the trust policy read is the one of the role the ARN names, in the account that owns it, and for another account's role also that of the same-named role in the function's account, which the session is minted for); under `--iam strict` it must also be in the caller's account (`AccessDeniedException: Cross-account pass role is not allowed.` otherwise; `--iam soft` logs the would-be denial), while the default mode accepts another account's role (e.g. `000000000000` templates) and mints the session in the function's account. Applied on create, update and CloudFormation alike
- **VPC config** — `VpcConfig` subnets and security groups are looked up in EC2 on `CreateFunction` / `UpdateFunctionConfiguration` (and CloudFormation): unknown ids or ones spanning VPCs are rejected with `InvalidParameterValueException`, and the response reports the shared `VpcId` (empty once detached)
- **Aliases and versions** — publish, point aliases at versions; alias-based weighted routing (`RoutingConfig`) is enforced at invoke time so traffic splits between versions exactly as on AWS
- **Concurrency controls** — reserved concurrency enforced at invocation time: per-function reservation caps in-flight invocations and excess requests are rejected with `TooManyRequestsException` (HTTP 429) and `Reason=ReservedFunctionConcurrentInvocationLimitExceeded`
- **`UpdateFunctionCode` from S3** — `S3Bucket`/`S3Key`/`S3ObjectVersion` fetches the ZIP from the fakecloud S3 implementation; the stored `CodeSha256` is the real SHA-256 of the fetched bytes
- **CloudWatch metrics** — every invoke publishes `Invocations`, `Errors`, `Duration`, `Throttles`, and `ConcurrentExecutions` to the `AWS/Lambda` namespace, queryable via `GetMetricStatistics` / `GetMetricData`
- **Resource-based policies** — the statement API (`AddPermission` / `RemovePermission` / `GetPolicy`) and the document API (`PutResourcePolicy` / `GetResourcePolicy` / `DeleteResourcePolicy`) address one policy per function or qualifier: a `RevisionId` acts as an optimistic-concurrency precondition (`PreconditionFailedException` on mismatch) and an `Allow` open to every principal with no `Condition` is rejected with `PublicPolicyException`
- **`GetAccountSettings`** — returns real `AccountUsage` counters (`FunctionCount`, `TotalCodeSize`) and `AccountLimit` so SDKs that pre-flight account quotas see live values
- **Warm container reuse**: subsequent invocations of the same function reuse the container; each version has its own warm pool, and a configuration change (environment, memory, timeout, role, handler, tags) starts a fresh instance once the old one finishes any in-flight invocation
- **Async invoke destinations** — `OnSuccess` / `OnFailure` routes the invocation result to SQS, SNS, EventBridge, or another Lambda by ARN scheme; record matches the AWS destinations schema (`requestContext`, `requestPayload`, `responseContext`, `responsePayload`)
- **`InvocationType` honored** — `Event` returns 202 and runs in the background, `RequestResponse` blocks for the result, `DryRun` validates without executing
- **Service Quotas**: with "Function and layer storage" (`L-2ACBD22F`) enforced in [Service Quotas](@/docs/services/servicequotas.md#enforceable-quotas), `CreateFunction`, `UpdateFunctionCode`, `PublishVersion` and `PublishLayerVersion` are refused with `CodeStorageExceededException` when the code of every `$LATEST`, published version and layer version in the region would pass the applied value (off by default). `GetAccountSettings` reports the applied concurrency and storage quotas, and `PutFunctionConcurrency` keeps at least 100 executions unreserved

## Protocol

REST. Path-based routing for invoke operations, JSON for control plane.

## Introspection

- `GET /_fakecloud/lambda/invocations` — list invocations delivered by other services (SQS, Kinesis, DynamoDB Streams, SNS, EventBridge, ...) with `functionArn`, `payload` (the event, as structured JSON), `source`, and `timestamp`
- `GET /_fakecloud/lambda/warm-containers` — list currently warm containers
- `POST /_fakecloud/lambda/{function-name}/evict-container` — force a cold start on the next invoke
- `GET /_fakecloud/lambda/layer-content/{account-id}/{layer-name}/{version}.zip[?region=<region>]` — download the raw layer ZIP. Returned as the `Content.Location` from `PublishLayerVersion` and `GetLayerVersion` (with the layer's `region`), so AWS SDK / Terraform clients that re-download a layer get the actual bytes. Without `region` the server's `--region` is tried first, then the account's other regions
- `GET /_fakecloud/lambda/function-code/{account-id}/{function-name}/{latest|<version>}.zip[?region=<region>]`: download a function's deployment package, returned as `Code.Location` from `GetFunction`; the same `region` rule applies

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
