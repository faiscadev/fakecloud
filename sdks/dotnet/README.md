# fakecloud

C#/.NET client SDK for [fakecloud](https://github.com/faiscadev/fakecloud) — a local AWS cloud emulator.

Provides typed access to the fakecloud introspection and simulation API (`/_fakecloud/*` endpoints), letting you inspect emulator state and trigger time-based processors in tests.

Requires **.NET 10+**. Uses `HttpClient` and `System.Text.Json` — no third-party dependencies.

## Installation

```bash
dotnet add package FakeCloud
```

Or in your test project file:

```xml
<ItemGroup>
  <PackageReference Include="FakeCloud" Version="0.44.1" />
</ItemGroup>
```

## Quick start

```csharp
using FakeCloud;

var fc = new FakeCloudClient("http://localhost:4566");

// Check server health
var health = await fc.HealthAsync();
Console.WriteLine($"{health.Version} {string.Join(",", health.Services!)}");

// Reset all state between tests
await fc.ResetAsync();

// Inspect SES emails sent during a test
var emails = (await fc.Ses.GetEmailsAsync()).Emails;
Console.WriteLine($"Sent {emails!.Count} emails");

// Inspect SNS messages
var messages = (await fc.Sns.GetMessagesAsync()).Messages;

// Inspect SQS messages across all queues
var queues = (await fc.Sqs.GetMessagesAsync()).Queues;

// Advance DynamoDB TTL processor
int expired = (await fc.DynamoDb.TickTtlAsync()).ExpiredItems;

// Advance S3 lifecycle processor
int expiredObjects = (await fc.S3.TickLifecycleAsync()).ExpiredObjects;
```

All methods are async and accept an optional trailing `CancellationToken`.

## API reference

### `FakeCloudClient`

```csharp
var fc = new FakeCloudClient();                        // defaults to http://localhost:4566
var fc = new FakeCloudClient("http://localhost:4566"); // explicit base URL
```

| Method                                   | Description                                |
| ---------------------------------------- | ------------------------------------------ |
| `HealthAsync()`                          | Server health check                        |
| `ResetAsync()`                           | Reset all service state                    |
| `ResetServiceAsync(service)`             | Reset a single service                     |
| `CredentialsAsync()`                     | Fetch container/instance credentials (`GET /_fakecloud/credentials`) |
| `InstanceIdentityDocumentAsync()`        | EC2 instance identity document (`/latest/dynamic/instance-identity/document`) |
| `DnsResolveAsync(name, type)`            | Resolve a name against Route 53 records like the `--dns` resolver (`GET /_fakecloud/dns/resolve`) |
| `CreateAdminAsync(accountId, userName)`  | Bootstrap an admin IAM user in an account  |

### `fc.Lambda`

| Method                                                            | Description                                                              |
| ----------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `GetInvocationsAsync()`                                           | List recorded Lambda invocations                                         |
| `GetWarmContainersAsync()`                                        | List warm (cached) Lambda containers                                     |
| `EvictContainerAsync(functionName)`                               | Evict a warm container                                                   |
| `DownloadFunctionCodeAsync(accountId, functionName, qualifier)`   | Download the stored zip for a function version (`"latest"` or version)   |
| `DownloadLayerContentAsync(accountId, layerName, version)`        | Download the stored zip for a layer version                              |

### `fc.Ses`

| Method                                     | Description                                       |
| ------------------------------------------ | ------------------------------------------------- |
| `GetEmailsAsync()`                         | List all sent emails                              |
| `SimulateInboundAsync(req)`                | Simulate an inbound email (receipt rules)         |
| `GetMetricsAsync()`                        | Return aggregated send/bounce/complaint counters  |
| `SetMailFromStatusAsync(identity, status)` | Force a MAIL FROM verification status             |
| `GetDkimPublicKeyAsync(identity)`          | Fetch the DKIM public key for an identity         |
| `SetSandboxAsync(sandbox)`                 | Toggle the account-level sandbox flag             |
| `GetBouncesAsync()`                        | List captured bounce events                       |
| `GetMessageInsightsAsync(messageId)`       | Fetch Message Insights for a sent message         |
| `GetSmtpSubmissionsAsync()`                | List SMTP submission attempts                     |
| `GetEventDestinationDeliveriesAsync()`     | List event-destination delivery records           |

### `fc.Sns`

| Method                            | Description                                                                  |
| --------------------------------- | ---------------------------------------------------------------------------- |
| `GetMessagesAsync()`              | List all published messages                                                  |
| `GetPendingConfirmationsAsync()`  | List subscriptions pending confirmation                                      |
| `ConfirmSubscriptionAsync(req)`   | Confirm a pending subscription                                               |
| `GetCertPemAsync()`               | Return the PEM-encoded SNS signing cert used by message signature validators |
| `GetSmsMessagesAsync()`           | List captured SMS messages SNS has "delivered"                               |

### `fc.Sqs`

| Method                      | Description                           |
| --------------------------- | ------------------------------------- |
| `GetMessagesAsync()`        | List all messages across all queues   |
| `TickExpirationAsync()`     | Tick the message expiration processor |
| `ForceDlqAsync(queueName)`  | Force all messages to the queue's DLQ |

### `fc.Events`

| Method                | Description                            |
| --------------------- | -------------------------------------- |
| `GetHistoryAsync()`   | Get event history and delivery records |
| `FireRuleAsync(req)`  | Fire an EventBridge rule manually      |

### `fc.Scheduler`

| Method                              | Description                                  |
| ----------------------------------- | -------------------------------------------- |
| `GetSchedulesAsync()`               | List EventBridge Scheduler schedules         |
| `FireScheduleAsync(group, name)`    | Fire a single schedule immediately           |

### `fc.Glue`

| Method                      | Description                                  |
| --------------------------- | -------------------------------------------- |
| `GetJobsAsync()`            | List registered Glue jobs                    |
| `GetJobRunsAsync(jobName?)` | List Glue job runs, optionally filtered      |
| `GetCrawlersAsync()`        | List Glue crawlers with state and targets    |

### `fc.CloudWatch`

| Method               | Description                                  |
| -------------------- | -------------------------------------------- |
| `GetAlarmsAsync()`   | List metric and composite alarms             |
| `GetMetricsAsync()`  | List unique metric series with latest value  |

### `fc.Firehose`

| Method                       | Description                                                       |
| ---------------------------- | ----------------------------------------------------------------- |
| `GetDeliveryStreamsAsync()`  | List delivery streams with type, status, encryption, dest count   |

### `fc.S3`

| Method                              | Description                                  |
| ----------------------------------- | -------------------------------------------- |
| `GetNotificationsAsync()`           | List S3 notification events                  |
| `TickLifecycleAsync()`              | Tick the lifecycle processor                 |
| `GetAccessPointsAsync()`            | List S3 access points                        |
| `GetObjectLambdaResponsesAsync()`   | List S3 Object Lambda responses captured     |

### `fc.DynamoDb`

| Method                        | Description            |
| ----------------------------- | ---------------------- |
| `TickTtlAsync()`              | Tick the TTL processor |
| `SaveSnapshotAsync(dataPath)` | Save a DynamoDB snapshot on demand (`null` dataPath -> configured store) |

### `fc.SecretsManager`

| Method                 | Description                 |
| ---------------------- | --------------------------- |
| `TickRotationAsync()`  | Tick the rotation scheduler |

### `fc.Cognito`

| Method                                     | Description                                                                                   |
| ------------------------------------------ | --------------------------------------------------------------------------------------------- |
| `GetUserCodesAsync(poolId, username)`      | Get confirmation codes for a user                                                             |
| `GetConfirmationCodesAsync()`              | List all confirmation codes                                                                   |
| `ConfirmUserAsync(req)`                    | Confirm a user (bypass verification)                                                          |
| `GetTokensAsync()`                         | List all active tokens                                                                        |
| `ExpireTokensAsync(req)`                   | Expire tokens (optionally filtered)                                                           |
| `GetAuthEventsAsync()`                     | List auth events                                                                              |
| `GetPreTokenGenInvocationsAsync()`         | List PreTokenGeneration Lambda trigger invocations recorded by `InitiateAuth`                 |
| `MintAuthorizationCodeAsync(req)`          | Mint a single-use OAuth2 authorization code (programmatic alternative to `/oauth2/authorize`) |
| `SetCompromisedPasswordsAsync(req)`        | Seed the compromised-password list used by Advanced Security                                  |
| `GetWebAuthnCredentialsAsync()`            | List stored WebAuthn credentials                                                              |

### `fc.Rds`

| Method                     | Description                                                                  |
| -------------------------- | ---------------------------------------------------------------------------- |
| `GetInstancesAsync()`      | List RDS instances with runtime metadata                                     |
| `LambdaInvokeAsync(req)`   | Bridge endpoint for the PostgreSQL `aws_lambda` extension                    |
| `S3ImportAsync(req)`       | Bridge endpoint for the PostgreSQL `aws_s3` extension (fetch object)         |
| `S3ExportAsync(req)`       | Bridge equivalent of `S3:PutObject` from inside the DB container             |

### `fc.ElastiCache`

| Method                          | Description                          |
| ------------------------------- | ------------------------------------ |
| `GetClustersAsync()`            | List ElastiCache cache clusters      |
| `GetReplicationGroupsAsync()`   | List ElastiCache replication groups  |
| `GetServerlessCachesAsync()`    | List ElastiCache serverless caches   |
| `GetAclsAsync()`                | List ElastiCache (Valkey/Redis) ACLs |

### `fc.Ec2`

| Method                          | Description                                                                                                                                                            |
| ------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GetInstancesAsync()`           | List EC2 instances with control-plane + runtime metadata                                                                                                               |
| `GetInstanceNetworksAsync()`    | Inspect each instance's backing network (Docker/Podman network or k8s NetworkPolicy), container IP, isolation backend, and whether security-group enforcement is active |

### `fc.Ecr`

| Method                                        | Description                                  |
| --------------------------------------------- | -------------------------------------------- |
| `GetRepositoriesAsync()`                      | List ECR repositories                        |
| `GetImagesAsync()`                            | List all ECR images                          |
| `GetImagesForRepositoryAsync(repositoryName)` | Filter images by repository                  |
| `GetPullThroughRulesAsync()`                  | List configured pull-through cache rules     |

### `fc.Logs`

| Method                                | Description                                        |
| ------------------------------------- | -------------------------------------------------- |
| `InjectAnomalyAsync(req)`             | Inject a Log Anomaly Detector anomaly              |
| `GetDeliveryConfigAsync()`            | Persisted CloudWatch Logs delivery configurations  |
| `GetFieldIndexesAsync(logGroupName)`  | Parsed `Fields` from index policies on a log group |

### `fc.StepFunctions`

| Method                           | Description                                                    |
| -------------------------------- | -------------------------------------------------------------- |
| `GetExecutionsAsync()`           | List all state machine execution history                       |
| `GetSyncExecutionsAsync()`       | List synchronous (`StartSyncExecution`) results                |
| `GetExecutionTreeAsync(arn)`     | Full parent/child execution tree under an execution ARN        |
| `EnqueueActivityTaskAsync(req)`  | Push a synthetic activity task onto an activity worker queue   |

### `fc.ApiGatewayV2`

| Method                                    | Description                                                                  |
| ----------------------------------------- | ---------------------------------------------------------------------------- |
| `GetRequestsAsync()`                      | List all HTTP API requests received                                          |
| `GetConnectionsAsync()`                   | List every active WebSocket connection tracked by API Gateway v2             |
| `GetDomainNameMtlsInfoAsync(domainName)`  | Fetch the mTLS truststore info for a custom domain (free-form JSON)          |
| `WsUrl(apiId, stage?)`                    | Build the WebSocket URL for the given API id (default `$default` stage)      |

### `fc.Bedrock`

| Method                                   | Description                                                          |
| ---------------------------------------- | -------------------------------------------------------------------- |
| `GetInvocationsAsync()`                  | List recorded Bedrock runtime invocations                            |
| `SetModelResponseAsync(modelId, text)`   | Configure a single canned response for a model                       |
| `SetResponseRulesAsync(modelId, rules)`  | Replace prompt-conditional response rules for a model                |
| `ClearResponseRulesAsync(modelId)`       | Clear all prompt-conditional response rules for a model              |
| `QueueFaultAsync(rule)`                  | Queue a fault rule for the next N calls                              |
| `GetFaultsAsync()`                       | List currently queued fault rules                                    |
| `ClearFaultsAsync()`                     | Clear all queued fault rules                                         |

### `fc.BedrockAgent`

| Method             | Description                                  |
| ------------------ | -------------------------------------------- |
| `GetAgentsAsync()` | List Bedrock Agent (control plane) agents    |

### `fc.BedrockAgentRuntime`

| Method                  | Description                            |
| ----------------------- | -------------------------------------- |
| `GetInvocationsAsync()` | List Bedrock Agent Runtime invocations |

### `fc.Ecs`

| Method                                | Description                                                                        |
| ------------------------------------- | ---------------------------------------------------------------------------------- |
| `GetClustersAsync()`                  | List ECS clusters                                                                  |
| `GetTasksAsync(cluster?, status?)`    | List tracked tasks, optionally filtered by cluster and status                      |
| `GetTaskAsync(taskId)`                | Fetch a single task snapshot by task ID                                            |
| `GetTaskLogsAsync(taskId)`            | Captured stdout/stderr for a task plus its exit code if known                      |
| `ForceStopTaskAsync(taskId)`          | SIGTERM (then SIGKILL after 10s) the task's running container                      |
| `MarkTaskFailedAsync(taskId, req)`    | Flip a task to `STOPPED` without killing the container                             |
| `GetEventsAsync()`                    | Replay the lifecycle event log                                                     |
| `GetTaskMetadataAsync(taskArn)`       | Aggregated v4 metadata dump for a task (by full ARN)                               |
| `GetCredentialsAsync(taskId)`         | Return short-lived IAM credentials for a task (matches the wire shape ECS exposes) |
| `GetMetadataV3Async(taskId)`          | Raw v3 task metadata document (free-form `JsonElement`)                            |
| `GetMetadataV4Async(taskId)`          | Raw v4 task metadata document (free-form `JsonElement`)                            |

### `fc.Elbv2`

| Method                     | Description                                                                      |
| -------------------------- | -------------------------------------------------------------------------------- |
| `GetLoadBalancersAsync()`  | List ELBv2 load balancers (ALB/NLB/GWLB)                                         |
| `GetTargetGroupsAsync()`   | List ELBv2 target groups                                                         |
| `GetListenersAsync()`      | List ELBv2 listeners                                                             |
| `GetRulesAsync()`          | List ELBv2 listener rules                                                        |
| `FlushAccessLogsAsync()`   | Force every buffered access-log + connection-log line to flush to S3 immediately |
| `GetWafCountsAsync()`      | Return WAFv2 association/evaluation counts the ELBv2 service has accumulated     |

### `fc.Route53`

| Method                                          | Description                                                                                                 |
| ----------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `SetHealthCheckStatusAsync(id, status, reason?)`| Flip a health check between `Success` / `Failure` / `Timeout` / `DnsError` / `InsufficientDataPoints` / `Unknown` |
| `GetDnssecMaterialAsync(zoneId)`                | Fetch DNSSEC material (DNSKEY + DS digest) for a hosted zone with at least one ACTIVE KSK                   |
| `SignDnssecAsync(zoneId, req)`                  | Sign an RRset under the zone's first ACTIVE KSK and return raw RRSIG fields                                 |

### `fc.Acm`

| Method                                               | Description                                                                                   |
| ---------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `SetCertificateStatusAsync(arnOrId, status, reason?)`| Flip an ACM certificate's status synchronously (`ISSUED` / `FAILED` / `VALIDATION_TIMED_OUT`) |
| `ApproveCertificateAsync(arnOrId)`                   | Approve a `PENDING_VALIDATION` certificate (simulates the email click)                        |
| `GetCertificateChainInfoAsync(arnOrId)`              | Inspect a stored cert's PEM block counts and byte sizes                                       |

### `fc.ApplicationAutoScaling`

| Method                 | Description                                       |
| ---------------------- | ------------------------------------------------- |
| `TickAsync()`          | Tick the Application Auto Scaling alarm evaluator |
| `ScheduledTickAsync()` | Tick the scheduled-action processor               |

### `fc.Athena`

| Method                    | Description                                   |
| ------------------------- | --------------------------------------------- |
| `GetNamedQueriesAsync()`  | List every named query stored in the registry |

### `fc.Organizations`

| Method                                 | Description                                                                           |
| -------------------------------------- | ------------------------------------------------------------------------------------- |
| `GetAccountsAsync()`                   | List every member account in the org (state, parent OU, tags, directly-attached SCPs) |
| `GetResponsibilityTransfersAsync()`    | List every billing responsibility transfer (direction, status, active handshake)      |

### `fc.Ssm`

| Method                                                  | Description                                                               |
| ------------------------------------------------------- | ------------------------------------------------------------------------- |
| `SetCommandStatusAsync(commandId, accountId, status)`   | Flip the status of every invocation under a `SendCommand` command id      |
| `FailCommandAsync(commandId, req?)`                     | Force a command (or specific invocation) into `Failed`                    |
| `GetParameterPolicyEventsAsync(accountId?)`             | Return every parameter-policy event recorded for the account              |
| `InjectSessionAsync(req)`                               | Drop a fake Session Manager session into state (bypassing `StartSession`) |

### `fc.Kms`

| Method            | Description                                     |
| ----------------- | ----------------------------------------------- |
| `GetUsageAsync()` | Return every recorded KMS data-plane invocation |

### `fc.WafV2`

| Method                   | Description                                                                              |
| ------------------------ | ---------------------------------------------------------------------------------------- |
| `EvaluateAsync(request)` | Evaluate an arbitrary request payload against the stored WAFv2 rule set (free-form JSON) |

### `fc.CloudFront`

| Method                                               | Description                                                                                |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `GetDistributionsAsync()`                            | List CloudFront distributions with their `<id>.cloudfront.net` domain and serving state    |
| `SetDistributionStatusAsync(distributionId, status)` | Flip a stored CloudFront Distribution's status (`Deployed` / `InProgress`) without waiting |

#### Full test loop — asserting on Bedrock calls

```csharp
using FakeCloud;

var fc = new FakeCloudClient();
var modelId = "anthropic.claude-3-haiku-20240307-v1:0";

// per-test setup
await fc.ResetAsync();

// Prime prompt-conditional responses
await fc.Bedrock.SetResponseRulesAsync(modelId, new[]
{
    new BedrockResponseRule("buy now", """{"label":"spam"}"""),
    new BedrockResponseRule(null, """{"label":"ham"}"""), // catch-all
});

await Classify("hello friend");           // user code calls Bedrock
await Classify("buy now cheap pills");

var invocations = (await fc.Bedrock.GetInvocationsAsync()).Invocations!;
Assert.Equal(2, invocations.Count);
Assert.Contains("ham", invocations[0].Output);
Assert.Contains("spam", invocations[1].Output);
```

### Error handling

All methods throw `FakeCloudException` on non-2xx responses:

```csharp
using FakeCloud;

try
{
    await fc.Cognito.ConfirmUserAsync(new ConfirmUserRequest("pool-1", "nobody"));
}
catch (FakeCloudException err)
{
    Console.WriteLine(err.Status); // 404
    Console.WriteLine(err.Body);   // "user not found"
}
```

## Local development

```bash
# Build fakecloud (from repo root)
cargo build --release

# Run unit tests
cd sdks/dotnet
dotnet test

# Run E2E tests against the freshly-built binary
../../target/release/fakecloud &
dotnet test
```

The E2E tests in `tests/FakeCloud.Tests/E2ETests.cs` no-op when no server is reachable at `http://localhost:4566` (override with `FAKECLOUD_ENDPOINT`).
