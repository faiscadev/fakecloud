+++
title = "Go SDK"
description = "Install and use the fakecloud SDK for Go tests."
weight = 3
+++

## Install

```sh
go get github.com/faiscadev/fakecloud/sdks/go
```

Go 1.21+.

## Initialize

```go
import fakecloud "github.com/faiscadev/fakecloud/sdks/go"

fc := fakecloud.New("http://localhost:4566")
```

Sub-clients are accessed via methods: `fc.SES()`, `fc.SNS()`, `fc.Lambda()`, etc.

## Top-level

| Method | Description |
|--------|-------------|
| `New(baseURL)` | Create a new client |
| `Health(ctx)` | Server health check |
| `Reset(ctx)` | Reset all service state |
| `ResetService(ctx, service)` | Reset a single service |
| `CreateAdmin(ctx, accountID, userName)` | Bootstrap an admin user in a secondary account |

## `fc.SES()`

| Method | Description |
|--------|-------------|
| `GetEmails(ctx)` | List all sent emails |
| `SimulateInbound(ctx, req)` | Simulate an inbound email |
| `GetMetrics(ctx)` | Aggregate send/delivery metrics |
| `SetMailFromStatus(ctx, req)` | Drive custom MAIL FROM verification state |
| `GetDkimPublicKey(ctx, identity)` | Fetch the synthetic DKIM public key for an identity |
| `GetBounces(ctx)` | List simulated bounce/complaint events |
| `GetMessageInsights(ctx)` | Per-message insights |
| `GetSmtpSubmissions(ctx)` | List submissions made through the SMTP endpoint |
| `GetEventDestinationDeliveries(ctx)` | List event-destination delivery attempts |
| `SetSandbox(ctx, req)` | Toggle SES sandbox mode |

## `fc.SNS()`

| Method | Description |
|--------|-------------|
| `GetMessages(ctx)` | List published messages |
| `GetPendingConfirmations(ctx)` | List pending subscription confirmations |
| `ConfirmSubscription(ctx, req)` | Confirm a subscription |
| `GetCertPEM(ctx)` | Fetch the PEM used to sign SNS HTTP/S notifications |
| `GetSMSMessages(ctx)` | List SMS messages delivered to phone numbers |

## `fc.SQS()`

| Method | Description |
|--------|-------------|
| `GetMessages(ctx)` | List all messages across queues |
| `TickExpiration(ctx)` | Tick the expiration processor |
| `ForceDLQ(ctx, queueName)` | Force messages to DLQ |

## `fc.Events()`

| Method | Description |
|--------|-------------|
| `GetHistory(ctx)` | Get event history and deliveries |
| `FireRule(ctx, req)` | Manually fire a rule |

## `fc.Scheduler()`

| Method | Description |
|--------|-------------|
| `GetSchedules(ctx)` | List EventBridge Scheduler schedules with next-fire metadata |
| `FireSchedule(ctx, req)` | Manually fire a schedule once |

## `fc.Glue()`

| Method | Description |
|--------|-------------|
| `GetJobs(ctx)` | List Glue job definitions |
| `GetJobRuns(ctx)` | List Glue job runs with status |

## `fc.S3()`

| Method | Description |
|--------|-------------|
| `GetNotifications(ctx)` | List notification events |
| `TickLifecycle(ctx)` | Tick the lifecycle processor |
| `GetAccessPoints(ctx)` | List S3 access points |
| `GetObjectLambdaResponses(ctx)` | List Object Lambda transformed responses |

## `fc.Lambda()`

| Method | Description |
|--------|-------------|
| `GetInvocations(ctx)` | List Lambda invocations |
| `GetWarmContainers(ctx)` | List warm containers |
| `DownloadFunctionCode(ctx, functionName)` | Download a function's deployment package |
| `DownloadLayerContent(ctx, layerName, versionNumber)` | Download a layer version's zip content |
| `EvictContainer(ctx, functionName)` | Evict a warm container |

## `fc.RDS()`

| Method | Description |
|--------|-------------|
| `GetInstances(ctx)` | List RDS instances with runtime metadata |
| `LambdaInvoke(ctx, req)` | Drive the `aws_lambda` Postgres extension bridge |
| `S3Import(ctx, req)` | Drive the `aws_s3.table_import_from_s3` bridge |
| `S3Export(ctx, req)` | Drive the `aws_s3.query_export_to_s3` bridge |

## `fc.ElastiCache()`

| Method | Description |
|--------|-------------|
| `GetClusters(ctx)` | List cache clusters |
| `GetReplicationGroups(ctx)` | List replication groups |
| `GetServerlessCaches(ctx)` | List serverless caches |
| `GetElastiCacheAcls(ctx)` | List RBAC users and ACLs |

## `fc.Athena()`

| Method | Description |
|--------|-------------|
| `GetNamedQueries(ctx)` | List saved named queries |

## `fc.ECR()`

| Method | Description |
|--------|-------------|
| `GetRepositories(ctx)` | List ECR repositories |
| `GetImages(ctx)` | List images across repositories |
| `GetPullThroughRules(ctx)` | List pull-through cache rules |

## `fc.DynamoDB()`

| Method | Description |
|--------|-------------|
| `TickTTL(ctx)` | Tick the TTL processor |

## `fc.SecretsManager()`

| Method | Description |
|--------|-------------|
| `TickRotation(ctx)` | Tick the rotation scheduler |

## `fc.Cognito()`

| Method | Description |
|--------|-------------|
| `GetUserCodes(ctx, poolID, username)` | Get codes for a user |
| `GetConfirmationCodes(ctx)` | List all confirmation codes |
| `ConfirmUser(ctx, req)` | Confirm a user |
| `GetTokens(ctx)` | List active tokens |
| `ExpireTokens(ctx, req)` | Expire tokens |
| `GetAuthEvents(ctx)` | List auth events |
| `MintAuthorizationCode(ctx, req)` | Mint a single-use OAuth2 authorization code |
| `SetCompromisedPasswords(ctx, req)` | Mark passwords as compromised to drive advanced security |
| `GetPreTokenGenInvocations(ctx)` | List pre-token-generation Lambda invocations |
| `GetWebAuthnCredentials(ctx)` | List registered WebAuthn credentials |

## `fc.ApiGatewayV2()`

| Method | Description |
|--------|-------------|
| `GetRequests(ctx)` | List all HTTP API requests received |
| `GetConnections(ctx)` | List active WebSocket connections |
| `GetDomainNameMtlsInfo(ctx, domainName)` | Inspect mTLS trust-store config for a custom domain |
| `WsURL(stage, apiID)` | Build a `ws://` URL for a WebSocket stage |

## `fc.StepFunctions()`

| Method | Description |
|--------|-------------|
| `GetExecutions(ctx)` | List all state machine execution history |
| `GetSyncExecutions(ctx)` | List `StartSyncExecution` results (Express workflows) |
| `GetExecutionTree(ctx, executionArn)` | Get the parent/child execution tree |
| `EnqueueActivityTask(ctx, req)` | Enqueue an activity-task heartbeat/result for a worker |

## `fc.Bedrock()`

| Method | Description |
|--------|-------------|
| `GetInvocations(ctx)` | List runtime invocations (with `Error` field) |
| `SetModelResponse(ctx, modelID, text)` | Single canned response for a model |
| `SetResponseRules(ctx, modelID, rules)` | Prompt-conditional rules |
| `ClearResponseRules(ctx, modelID)` | Clear rules for a model |
| `QueueFault(ctx, rule)` | Queue a fault rule |
| `GetFaults(ctx)` | List queued fault rules |
| `ClearFaults(ctx)` | Clear all queued fault rules |

## `fc.BedrockAgent()` / `fc.BedrockAgentRuntime()`

| Method | Description |
|--------|-------------|
| `BedrockAgent().GetAgents(ctx)` | List configured Bedrock agents |
| `BedrockAgentRuntime().GetInvocations(ctx)` | List recorded agent runtime invocations |

## `fc.ECS()`

| Method | Description |
|--------|-------------|
| `GetClusters(ctx)` | List ECS clusters |
| `GetTasks(ctx)` | List tasks |
| `GetTask(ctx, taskArn)` | Get a single task |
| `GetTaskLogs(ctx, taskArn)` | Get logs for a task |
| `ForceStopTask(ctx, taskArn)` | Forcibly stop a task |
| `MarkTaskFailed(ctx, taskArn, req)` | Inject a failure into a task |
| `GetEvents(ctx)` | List ECS lifecycle events |
| `GetTaskMetadata(ctx, taskArn)` | Container-agent task metadata |
| `GetTaskCredentials(ctx, credentialID)` | Task-role credentials served to containers |
| `GetTaskMetadataV3(ctx, taskArn)` | Task metadata endpoint v3 |
| `GetTaskMetadataV4(ctx, taskArn)` | Task metadata endpoint v4 |

## `fc.ELBv2()`

| Method | Description |
|--------|-------------|
| `GetLoadBalancers(ctx)` | List load balancers (ALB/NLB/GWLB) |
| `GetTargetGroups(ctx)` | List target groups |
| `GetListeners(ctx)` | List listeners |
| `GetRules(ctx)` | List listener rules |
| `GetWafCounts(ctx)` | WAF allow/block counters per listener |
| `FlushAccessLogs(ctx)` | Flush buffered access logs to S3 |

## `fc.Route53()`

| Method | Description |
|--------|-------------|
| `SetHealthCheckStatus(ctx, id, req)` | Flip a health check between `Success` / `Failure` / `Timeout` / `DnsError` / `InsufficientDataPoints` / `Unknown` |
| `GetDnssecMaterial(ctx, hostedZoneID)` | Fetch DNSSEC KSK/ZSK material for a hosted zone |
| `SignRRset(ctx, req)` | Produce an RRSIG over an RRset for offline verification |

## `fc.ACM()`

| Method | Description |
|--------|-------------|
| `SetCertificateStatus(ctx, arn, req)` | Force a certificate into `ISSUED`/`FAILED`/etc. |
| `ApproveCertificate(ctx, arn)` | Approve a pending certificate |
| `GetCertificateChainInfo(ctx, arn)` | Inspect issuer and chain metadata |

## `fc.Logs()`

| Method | Description |
|--------|-------------|
| `InjectAnomaly(ctx, req)` | Inject a Log Anomaly Detection finding |
| `GetDeliveryConfig(ctx)` | Inspect log-delivery configurations |
| `GetFieldIndexes(ctx)` | List configured field indexes |

## `fc.ApplicationAutoScaling()`

| Method | Description |
|--------|-------------|
| `Tick(ctx)` | Run scaling evaluation once |
| `ScheduledTick(ctx)` | Run scheduled-action evaluation once |

## `fc.Organizations()`

| Method | Description |
|--------|-------------|
| `GetAccounts(ctx)` | List accounts in the organization |

## `fc.SSM()`

| Method | Description |
|--------|-------------|
| `SetCommandStatus(ctx, commandID, req)` | Drive a Run Command to `Success`/`Failed`/etc. |
| `FailCommand(ctx, commandID, req)` | Convenience helper to fail a command with a reason |
| `GetParameterPolicyEvents(ctx)` | List parameter-policy expiration/notification events |
| `InjectSession(ctx, req)` | Inject a Session Manager session record |

## `fc.KMS()`

| Method | Description |
|--------|-------------|
| `GetUsage(ctx)` | Per-key usage counters (encrypt/decrypt/sign/verify) |

## `fc.WAFv2()`

| Method | Description |
|--------|-------------|
| `Evaluate(ctx, req)` | Evaluate a request against a Web ACL and return the verdict |

## `fc.CloudFront()`

| Method | Description |
|--------|-------------|
| `SetDistributionStatus(ctx, id, req)` | Force a distribution into `Deployed`/`InProgress` |

## Error handling

Methods return `*fakecloud.APIError` on non-2xx responses:

```go
_, err := fc.Cognito().ConfirmUser(ctx, fakecloud.ConfirmUserRequest{
    UserPoolID: "pool-1",
    Username:   "nobody",
})
if err != nil {
    var apiErr *fakecloud.APIError
    if errors.As(err, &apiErr) {
        fmt.Println(apiErr.StatusCode) // 404
        fmt.Println(apiErr.Body)
    }
}
```

## Example: end-to-end test

```go
package main_test

import (
    "context"
    "testing"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
    fakecloud "github.com/faiscadev/fakecloud/sdks/go"
)

func TestAppPublishesToSQS(t *testing.T) {
    ctx := context.Background()
    fc := fakecloud.New("http://localhost:4566")
    if err := fc.Reset(ctx); err != nil {
        t.Fatal(err)
    }

    cfg, _ := config.LoadDefaultConfig(ctx,
        config.WithRegion("us-east-1"),
        config.WithCredentialsProvider(aws.AnonymousCredentialsProvider{}),
    )
    sqsClient := sqs.NewFromConfig(cfg, func(o *sqs.Options) {
        o.BaseEndpoint = aws.String("http://localhost:4566")
    })

    _, err := sqsClient.SendMessage(ctx, &sqs.SendMessageInput{
        QueueUrl:    aws.String("http://localhost:4566/000000000000/my-queue"),
        MessageBody: aws.String("hello"),
    })
    if err != nil {
        t.Fatal(err)
    }

    messages, err := fc.SQS().GetMessages(ctx)
    if err != nil {
        t.Fatal(err)
    }
    if len(messages.Messages) != 1 {
        t.Fatalf("expected 1 message, got %d", len(messages.Messages))
    }
    if messages.Messages[0].Body != "hello" {
        t.Fatalf("expected body 'hello', got %q", messages.Messages[0].Body)
    }
}
```

## Source

- [`sdks/go`](https://github.com/faiscadev/fakecloud/tree/main/sdks/go)
- [Source README](https://github.com/faiscadev/fakecloud/blob/main/sdks/go/README.md) — always-current method list
