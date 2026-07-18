# fakecloud Go SDK

Go client for the [fakecloud](https://github.com/faiscadev/fakecloud) introspection and simulation API.

## Installation

```sh
go get github.com/faiscadev/fakecloud/sdks/go
```

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"

	fakecloud "github.com/faiscadev/fakecloud/sdks/go"
)

func main() {
	fc := fakecloud.New("http://localhost:4566")
	ctx := context.Background()

	// Check health
	health, err := fc.Health(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Status: %s, Version: %s\n", health.Status, health.Version)

	// List sent emails
	emails, err := fc.SES().GetEmails(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, e := range emails.Emails {
		fmt.Printf("Email %s: %s -> %v\n", e.MessageID, e.From, e.To)
	}

	// List SNS messages
	msgs, err := fc.SNS().GetMessages(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("SNS messages: %d\n", len(msgs.Messages))

	// Reset all state
	if err := fc.Reset(ctx); err != nil {
		log.Fatal(err)
	}
}
```

## API reference

### Top-level

| Method | Description |
|--------|-------------|
| `New(baseURL)` | Create a new client |
| `Health(ctx)` | Check server health |
| `Reset(ctx)` | Reset all service state |
| `ResetService(ctx, service)` | Reset a single service |
| `Credentials(ctx)` | Fetch container/instance credentials (`GET /_fakecloud/credentials`) |
| `InstanceIdentityDocument(ctx)` | EC2 instance identity document (`/latest/dynamic/instance-identity/document`) |
| `DNSResolve(ctx, name, recordType)` | Resolve a name against Route 53 records like the `--dns` resolver (`GET /_fakecloud/dns/resolve`) |
| `CreateAdmin(ctx, accountID, userName)` | Bootstrap an admin user in a secondary account |

### SES - `fc.SES()`

| Method | Description |
|--------|-------------|
| `GetEmails(ctx)` | List all sent emails |
| `SimulateInbound(ctx, req)` | Simulate an inbound email |
| `GetMetrics(ctx)` | Aggregate send/delivery metrics |
| `SetMailFromStatus(ctx, req)` | Drive custom MAIL FROM verification state |
| `GetDkimPublicKey(ctx, identity)` | Fetch the synthetic DKIM public key for an identity |
| `GetBounces(ctx)` | List simulated bounce/complaint events |
| `GetMessageInsights(ctx)` | Per-message insights (placements, engagement) |
| `GetSmtpSubmissions(ctx)` | List submissions made through the SMTP endpoint |
| `GetEventDestinationDeliveries(ctx)` | List event-destination delivery attempts |
| `SetSandbox(ctx, req)` | Toggle SES sandbox mode |

### SNS - `fc.SNS()`

| Method | Description |
|--------|-------------|
| `GetMessages(ctx)` | List published messages |
| `GetPendingConfirmations(ctx)` | List pending subscription confirmations |
| `ConfirmSubscription(ctx, req)` | Confirm a subscription |
| `GetCertPEM(ctx)` | Fetch the PEM used to sign SNS HTTP/S notifications |
| `GetSMSMessages(ctx)` | List SMS messages delivered to phone numbers |

### SQS - `fc.SQS()`

| Method | Description |
|--------|-------------|
| `GetMessages(ctx)` | List all messages across queues |
| `TickExpiration(ctx)` | Tick the expiration processor |
| `ForceDLQ(ctx, queueName)` | Force messages to DLQ |

### EventBridge - `fc.Events()`

| Method | Description |
|--------|-------------|
| `GetHistory(ctx)` | Get event history and deliveries |
| `FireRule(ctx, req)` | Manually fire a rule |

### Scheduler - `fc.Scheduler()`

| Method | Description |
|--------|-------------|
| `GetSchedules(ctx)` | List EventBridge Scheduler schedules with next-fire metadata |
| `FireSchedule(ctx, req)` | Manually fire a schedule once |

### Glue - `fc.Glue()`

| Method | Description |
|--------|-------------|
| `GetJobs(ctx)` | List Glue job definitions |
| `GetJobRuns(ctx)` | List Glue job runs with status |
| `GetCrawlers(ctx)` | List Glue crawlers with state and target summary |

### CloudWatch - `fc.CloudWatch()`

| Method | Description |
|--------|-------------|
| `GetAlarms(ctx)` | List metric and composite alarms across accounts/regions |
| `GetMetrics(ctx)` | List unique metric series with datapoint count and latest value |

### S3 - `fc.S3()`

| Method | Description |
|--------|-------------|
| `GetNotifications(ctx)` | List notification events |
| `TickLifecycle(ctx)` | Tick the lifecycle processor |
| `GetAccessPoints(ctx)` | List S3 access points |
| `GetObjectLambdaResponses(ctx)` | List Object Lambda transformed responses |

### Lambda - `fc.Lambda()`

| Method | Description |
|--------|-------------|
| `GetInvocations(ctx)` | List recorded invocations |
| `GetWarmContainers(ctx)` | List warm containers |
| `DownloadFunctionCode(ctx, functionName)` | Download a function's deployment package |
| `DownloadLayerContent(ctx, layerName, versionNumber)` | Download a layer version's zip content |
| `EvictContainer(ctx, functionName)` | Evict a warm container |

### RDS - `fc.RDS()`

| Method | Description |
|--------|-------------|
| `GetInstances(ctx)` | List RDS instances with runtime metadata |
| `LambdaInvoke(ctx, req)` | Drive the `aws_lambda` Postgres extension bridge |
| `S3Import(ctx, req)` | Drive the `aws_s3.table_import_from_s3` bridge |
| `S3Export(ctx, req)` | Drive the `aws_s3.query_export_to_s3` bridge |

### EC2 - `fc.EC2()`

| Method | Description |
|--------|-------------|
| `GetInstances(ctx)` | List EC2 instances with control-plane + runtime metadata |
| `GetInstanceNetworks(ctx)` | Inspect each instance's backing network, container IP, isolation backend, and security-group enforcement state |

### ElastiCache - `fc.ElastiCache()`

| Method | Description |
|--------|-------------|
| `GetClusters(ctx)` | List cache clusters |
| `GetReplicationGroups(ctx)` | List replication groups |
| `GetServerlessCaches(ctx)` | List serverless caches |
| `GetElastiCacheAcls(ctx)` | List RBAC users and ACLs |

### Athena - `fc.Athena()`

| Method | Description |
|--------|-------------|
| `GetNamedQueries(ctx)` | List saved named queries |

### ECR - `fc.ECR()`

| Method | Description |
|--------|-------------|
| `GetRepositories(ctx)` | List ECR repositories |
| `GetImages(ctx)` | List images across repositories |
| `GetPullThroughRules(ctx)` | List pull-through cache rules |

### DynamoDB - `fc.DynamoDB()`

| Method | Description |
|--------|-------------|
| `TickTTL(ctx)` | Tick the TTL processor |
| `SaveSnapshot(ctx, dataPath)` | Save a DynamoDB snapshot on demand (empty dataPath -> configured store) |

### SecretsManager - `fc.SecretsManager()`

| Method | Description |
|--------|-------------|
| `TickRotation(ctx)` | Tick the rotation scheduler |

### Cognito - `fc.Cognito()`

| Method | Description |
|--------|-------------|
| `GetUserCodes(ctx, poolID, username)` | Get codes for a user |
| `GetConfirmationCodes(ctx)` | List all confirmation codes |
| `ConfirmUser(ctx, req)` | Confirm a user |
| `GetTokens(ctx)` | List active tokens |
| `ExpireTokens(ctx, req)` | Expire tokens |
| `GetAuthEvents(ctx)` | List auth events |
| `MintAuthorizationCode(ctx, req)` | Mint a single-use OAuth2 authorization code (programmatic alternative to driving /oauth2/authorize) |
| `SetCompromisedPasswords(ctx, req)` | Mark a set of passwords as compromised to drive advanced security |
| `GetPreTokenGenInvocations(ctx)` | List pre-token-generation Lambda invocations |
| `GetWebAuthnCredentials(ctx)` | List registered WebAuthn credentials |

### API Gateway v2 - `fc.ApiGatewayV2()`

| Method | Description |
|--------|-------------|
| `GetRequests(ctx)` | List all HTTP API requests received |
| `GetConnections(ctx)` | List active WebSocket connections |
| `GetDomainNameMtlsInfo(ctx, domainName)` | Inspect mTLS trust-store config for a custom domain |
| `WsURL(stage, apiID)` | Build a `ws://` URL for a WebSocket stage |

### Step Functions - `fc.StepFunctions()`

| Method | Description |
|--------|-------------|
| `GetExecutions(ctx)` | List all state machine execution history |
| `GetSyncExecutions(ctx)` | List `StartSyncExecution` results (Express workflows) |
| `GetExecutionTree(ctx, executionArn)` | Get the parent/child execution tree |
| `EnqueueActivityTask(ctx, req)` | Enqueue an activity-task heartbeat/result for a worker |

### Bedrock - `fc.Bedrock()`

| Method | Description |
|--------|-------------|
| `GetInvocations(ctx)` | List recorded Bedrock runtime invocations (each has `Error *string`) |
| `SetModelResponse(ctx, modelID, text)` | Configure a single canned response for a model |
| `SetResponseRules(ctx, modelID, rules)` | Replace prompt-conditional response rules for a model |
| `ClearResponseRules(ctx, modelID)` | Clear all prompt-conditional response rules for a model |
| `QueueFault(ctx, rule)` | Queue a fault rule (e.g. `ThrottlingException`) for the next N calls |
| `GetFaults(ctx)` | List currently queued fault rules |
| `ClearFaults(ctx)` | Clear all queued fault rules |

### Bedrock Agent - `fc.BedrockAgent()` / `fc.BedrockAgentRuntime()`

| Method | Description |
|--------|-------------|
| `BedrockAgent().GetAgents(ctx)` | List configured Bedrock agents |
| `BedrockAgentRuntime().GetInvocations(ctx)` | List recorded agent runtime invocations |

### ECS - `fc.ECS()`

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

### ELBv2 - `fc.ELBv2()`

| Method | Description |
|--------|-------------|
| `GetLoadBalancers(ctx)` | List load balancers (ALB/NLB/GWLB) |
| `GetTargetGroups(ctx)` | List target groups |
| `GetListeners(ctx)` | List listeners |
| `GetRules(ctx)` | List listener rules |
| `GetWafCounts(ctx)` | WAF allow/block counters per listener |
| `FlushAccessLogs(ctx)` | Flush buffered access logs to S3 |

### Route 53 - `fc.Route53()`

| Method | Description |
|--------|-------------|
| `SetHealthCheckStatus(ctx, id, req)` | Flip a health check between `Success` / `Failure` / `Timeout` / `DnsError` / `InsufficientDataPoints` / `Unknown` to drive failover routing in tests; reason is appended to the `<Status>` element for failure-flavoured statuses |
| `GetDnssecMaterial(ctx, hostedZoneID)` | Fetch DNSSEC KSK/ZSK material for a hosted zone |
| `SignRRset(ctx, req)` | Produce an RRSIG over an RRset for offline verification |

### ACM - `fc.ACM()`

| Method | Description |
|--------|-------------|
| `SetCertificateStatus(ctx, arn, req)` | Force a certificate into `ISSUED`/`FAILED`/etc. |
| `ApproveCertificate(ctx, arn)` | Approve a pending certificate |
| `GetCertificateChainInfo(ctx, arn)` | Inspect issuer and chain metadata |

### Logs - `fc.Logs()`

| Method | Description |
|--------|-------------|
| `InjectAnomaly(ctx, req)` | Inject a Log Anomaly Detection finding |
| `GetDeliveryConfig(ctx)` | Inspect log-delivery configurations |
| `GetFieldIndexes(ctx)` | List configured field indexes |

### Application Auto Scaling - `fc.ApplicationAutoScaling()`

| Method | Description |
|--------|-------------|
| `Tick(ctx)` | Run scaling evaluation once |
| `ScheduledTick(ctx)` | Run scheduled-action evaluation once |

### Organizations - `fc.Organizations()`

| Method | Description |
|--------|-------------|
| `GetAccounts(ctx)` | List accounts in the organization |

### SSM - `fc.SSM()`

| Method | Description |
|--------|-------------|
| `SetCommandStatus(ctx, commandID, req)` | Drive a Run Command to `Success`/`Failed`/etc. |
| `FailCommand(ctx, commandID, req)` | Convenience helper to fail a command with a reason |
| `GetParameterPolicyEvents(ctx)` | List parameter-policy expiration/notification events |
| `InjectSession(ctx, req)` | Inject a Session Manager session record |

### KMS - `fc.KMS()`

| Method | Description |
|--------|-------------|
| `GetUsage(ctx)` | Per-key usage counters (encrypt/decrypt/sign/verify) |

### WAFv2 - `fc.WAFv2()`

| Method | Description |
|--------|-------------|
| `Evaluate(ctx, req)` | Evaluate a request against a Web ACL and return the verdict |

### CloudFront - `fc.CloudFront()`

| Method | Description |
|--------|-------------|
| `GetDistributions(ctx)` | List distributions, each with its `.cloudfront.net` domain and whether the in-process data plane serves it |
| `SetDistributionStatus(ctx, id, req)` | Force a distribution into `Deployed`/`InProgress` |

#### Testing Bedrock-calling code end-to-end

```go
func TestClassifierBranchesOnSpamVsHam(t *testing.T) {
    ctx := context.Background()
    fc := fakecloud.New("http://localhost:4566")
    if err := fc.Reset(ctx); err != nil {
        t.Fatal(err)
    }

    modelID := "anthropic.claude-3-haiku-20240307-v1:0"
    spam := "buy now"
    _, err := fc.Bedrock().SetResponseRules(ctx, modelID, []fakecloud.BedrockResponseRule{
        {PromptContains: &spam, Response: `{"label":"spam"}`},
        {PromptContains: nil, Response: `{"label":"ham"}`}, // catch-all
    })
    if err != nil {
        t.Fatal(err)
    }

    classify(t, "hello friend")
    classify(t, "buy now cheap pills")

    invs, err := fc.Bedrock().GetInvocations(ctx)
    if err != nil {
        t.Fatal(err)
    }
    if len(invs.Invocations) != 2 {
        t.Fatalf("expected 2 invocations, got %d", len(invs.Invocations))
    }
    if !strings.Contains(invs.Invocations[0].Output, "ham") ||
        !strings.Contains(invs.Invocations[1].Output, "spam") {
        t.Errorf("routing broken")
    }
}

func TestRetriesOnThrottling(t *testing.T) {
    ctx := context.Background()
    fc := fakecloud.New("http://localhost:4566")
    if err := fc.Reset(ctx); err != nil {
        t.Fatal(err)
    }

    _, err := fc.Bedrock().QueueFault(ctx, fakecloud.BedrockFaultRule{
        ErrorType:  "ThrottlingException",
        Message:    "Rate exceeded",
        HTTPStatus: 429,
        Count:      1, // first call faults; retry succeeds
    })
    if err != nil {
        t.Fatal(err)
    }

    classify(t, "hello")

    invs, err := fc.Bedrock().GetInvocations(ctx)
    if err != nil {
        t.Fatal(err)
    }
    if len(invs.Invocations) != 2 {
        t.Fatalf("expected 2 invocations, got %d", len(invs.Invocations))
    }
    if invs.Invocations[0].Error == nil || !strings.Contains(*invs.Invocations[0].Error, "ThrottlingException") {
        t.Errorf("first call should be faulted")
    }
    if invs.Invocations[1].Error != nil {
        t.Errorf("retry should succeed")
    }
}
```

### Error handling

Non-2xx responses return `*fakecloud.APIError`:

```go
resp, err := fc.Health(ctx)
if err != nil {
	var apiErr *fakecloud.APIError
	if errors.As(err, &apiErr) {
		fmt.Printf("HTTP %d: %s\n", apiErr.StatusCode, apiErr.Body)
	}
}
```
