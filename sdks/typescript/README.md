# fakecloud

TypeScript client SDK for [fakecloud](https://github.com/faiscadev/fakecloud) -- a local AWS cloud emulator.

Provides typed access to the fakecloud introspection and simulation API (`/_fakecloud/*` endpoints), letting you inspect emulator state and trigger time-based processors in tests.

## Installation

```bash
npm install fakecloud
```

## Quick start

```typescript
import { FakeCloud } from "fakecloud";

const fc = new FakeCloud("http://localhost:4566");

// Check server health
const health = await fc.health();
console.log(health.version, health.services);

// Reset all state between tests
await fc.reset();

// Inspect SES emails sent during a test
const { emails } = await fc.ses.getEmails();
console.log(`Sent ${emails.length} emails`);

// Inspect SNS messages
const { messages } = await fc.sns.getMessages();

// Inspect SQS messages across all queues
const { queues } = await fc.sqs.getMessages();

// Advance DynamoDB TTL processor
const { expiredItems } = await fc.dynamodb.tickTtl();

// Advance S3 lifecycle processor
const { expiredObjects } = await fc.s3.tickLifecycle();
```

## API reference

### `FakeCloud`

```typescript
const fc = new FakeCloud(baseUrl?: string);
```

Top-level client. Defaults to `http://localhost:4566`.

| Method                             | Description                                |
| ---------------------------------- | ------------------------------------------ |
| `health()`                         | Server health check                        |
| `reset()`                          | Reset all service state                    |
| `resetService(service)`            | Reset a single service                     |
| `createAdmin(accountId, userName)` | Bootstrap an admin IAM user for an account |

### `fc.acm`

| Method                               | Description                                                                 |
| ------------------------------------ | --------------------------------------------------------------------------- |
| `setCertificateStatus(arnOrId, req)` | Flip a certificate's status (`ISSUED` / `FAILED` / `VALIDATION_TIMED_OUT`)  |
| `approveCertificate(arnOrId)`        | Approve a `PENDING_VALIDATION` cert synchronously (email-validation bypass) |
| `getCertificateChainInfo(arnOrId)`   | Inspect stored PEM block counts and byte sizes for a certificate            |

### `fc.apigatewayv2`

| Method                    | Description                                                            |
| ------------------------- | ---------------------------------------------------------------------- |
| `getRequests()`           | List recorded HTTP API requests                                        |
| `getConnections()`        | List every live WebSocket connection currently tracked                 |
| `getMtlsInfo(domainName)` | Inspect the mTLS trust store / chain configuration for a custom domain |
| `wsUrl(apiId, stage?)`    | Build the `ws://`/`wss://` URL for a WebSocket API stage               |

### `fc.applicationAutoscaling`

| Method            | Description                                                |
| ----------------- | ---------------------------------------------------------- |
| `tick()`          | Force an immediate scaling-policy evaluation pass          |
| `scheduledTick()` | Force the scheduled-action watcher to fire due actions now |

### `fc.athena`

| Method              | Description                                                          |
| ------------------- | -------------------------------------------------------------------- |
| `getNamedQueries()` | List every named query across all workgroups for the default account |

### `fc.bedrock`

| Method                             | Description                                                          |
| ---------------------------------- | -------------------------------------------------------------------- |
| `getInvocations()`                 | List recorded Bedrock runtime invocations (each has `error` field)   |
| `setModelResponse(modelId, text)`  | Configure a single canned response for a model                       |
| `setResponseRules(modelId, rules)` | Replace prompt-conditional response rules for a model                |
| `clearResponseRules(modelId)`      | Clear all prompt-conditional response rules for a model              |
| `queueFault(rule)`                 | Queue a fault rule (e.g. `ThrottlingException`) for the next N calls |
| `getFaults()`                      | List currently queued fault rules                                    |
| `clearFaults()`                    | Clear all queued fault rules                                         |

### `fc.bedrockAgent`

| Method        | Description                                                                         |
| ------------- | ----------------------------------------------------------------------------------- |
| `getAgents()` | List every Bedrock Agent with aliases, versions, knowledge bases, and collaborators |

### `fc.bedrockAgentRuntime`

| Method             | Description                                                                          |
| ------------------ | ------------------------------------------------------------------------------------ |
| `getInvocations()` | List recorded `InvokeAgent` / `InvokeInlineAgent` / `InvokeFlow` / `Retrieve*` calls |

### `fc.cloudfront`

| Method                           | Description                                                       |
| -------------------------------- | ----------------------------------------------------------------- |
| `setDistributionStatus(id, req)` | Flip a Distribution synchronously into `Deployed` or `InProgress` |

### `fc.cognito`

| Method                           | Description                                                                                   |
| -------------------------------- | --------------------------------------------------------------------------------------------- |
| `getUserCodes(poolId, username)` | Get confirmation codes for a single user                                                      |
| `getConfirmationCodes()`         | List all pending confirmation codes                                                           |
| `confirmUser(req)`               | Force-confirm a user                                                                          |
| `getTokens()`                    | List active tokens                                                                            |
| `expireTokens(req)`              | Expire tokens for a pool/user                                                                 |
| `getAuthEvents()`                | List auth events                                                                              |
| `getPreTokenGenInvocations()`    | List PreTokenGeneration Lambda trigger invocations with parsed claim mutations                |
| `mintAuthorizationCode(req)`     | Mint a single-use OAuth2 authorization code (programmatic alternative to `/oauth2/authorize`) |
| `setCompromisedPasswords(req)`   | Replace the compromised-password list used by adaptive auth                                   |
| `getWebAuthnCredentials()`       | List stored WebAuthn credentials                                                              |

### `fc.dynamodb`

| Method      | Description            |
| ----------- | ---------------------- |
| `tickTtl()` | Tick the TTL processor |

### `fc.ec2`

| Method                  | Description                                                                                                                                                             |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `getInstances()`        | List EC2 instances with control-plane + runtime metadata                                                                                                                |
| `getInstanceNetworks()` | Inspect each instance's backing network (Docker/Podman network or k8s NetworkPolicy), container IP, isolation backend, and whether security-group enforcement is active |

### `fc.ecr`

| Method                  | Description                                         |
| ----------------------- | --------------------------------------------------- |
| `getRepositories()`     | List every ECR repository fakecloud has seen        |
| `getImages(repo?)`      | List images, optionally filtered by repository name |
| `getPullThroughRules()` | List pull-through cache rules                       |

### `fc.ecs`

| Method                        | Description                                                   |
| ----------------------------- | ------------------------------------------------------------- |
| `getClusters()`               | List every ECS cluster across every account                   |
| `getTasks(opts?)`             | List tracked tasks; optional `cluster` / `status` filters     |
| `getTask(taskId)`             | Fetch a single task snapshot by ID                            |
| `getTaskLogs(taskId)`         | Captured docker stdout/stderr + exit code for a task          |
| `forceStopTask(taskId)`       | SIGTERM (then SIGKILL after 10s) the task's running container |
| `markTaskFailed(taskId, req)` | Flip a task to STOPPED without killing the container          |
| `getEvents()`                 | Replay the lifecycle event log                                |
| `getTaskMetadata(taskArn)`    | v4 metadata dump keyed by full task ARN                       |
| `getTaskCredentials(taskId)`  | IMDS-style temporary credentials minted for an ECS task       |
| `getTaskMetadataV3(taskId)`   | v3 task metadata dump (`ECS_CONTAINER_METADATA_URI`)          |
| `getTaskMetadataV4(taskId)`   | v4 task metadata dump (`ECS_CONTAINER_METADATA_URI_V4`)       |

### `fc.elasticache`

| Method                   | Description                         |
| ------------------------ | ----------------------------------- |
| `getClusters()`          | List ElastiCache cache clusters     |
| `getReplicationGroups()` | List ElastiCache replication groups |
| `getServerlessCaches()`  | List ElastiCache serverless caches  |
| `getElastiCacheAcls()`   | List Redis user-group ACLs          |

### `fc.elbv2`

| Method               | Description                                                            |
| -------------------- | ---------------------------------------------------------------------- |
| `getLoadBalancers()` | List every ELBv2 load balancer (ALB / NLB / GWLB) across every account |
| `getListeners()`     | List every listener                                                    |
| `getRules()`         | List every listener rule                                               |
| `getTargetGroups()`  | List every target group                                                |
| `flushAccessLogs()`  | Flush buffered ALB access-log + connection-log lines to S3 now         |
| `getWafCounts()`     | Return current WAF association / evaluation counts                     |

### `fc.events`

| Method          | Description                            |
| --------------- | -------------------------------------- |
| `getHistory()`  | Get event history and delivery records |
| `fireRule(req)` | Fire an EventBridge rule manually      |

### `fc.glue`

| Method              | Description                                    |
| ------------------- | ---------------------------------------------- |
| `getJobs()`         | List every Glue job                            |
| `getJobRuns(name?)` | List job runs, optionally filtered by job name |
| `getCrawlers()`     | List every Glue crawler with state and targets |

### `fc.cloudwatch`

| Method         | Description                                           |
| -------------- | ----------------------------------------------------- |
| `getAlarms()`  | List metric and composite alarms across accounts      |
| `getMetrics()` | List unique metric series with datapoint count/latest |

### `fc.firehose`

| Method                 | Description                                                                      |
| ---------------------- | -------------------------------------------------------------------------------- |
| `getDeliveryStreams()` | List every delivery stream with type, status, encryption summary, and dest count |

### `fc.kms`

| Method       | Description                                                             |
| ------------ | ----------------------------------------------------------------------- |
| `getUsage()` | Return every recorded KMS usage record (one per server-side encrypt op) |

### `fc.lambda`

| Method                                                      | Description                                                   |
| ----------------------------------------------------------- | ------------------------------------------------------------- |
| `getInvocations()`                                          | List recorded Lambda invocations                              |
| `getWarmContainers()`                                       | List warm (cached) Lambda containers                          |
| `evictContainer(functionName)`                              | Evict a warm container                                        |
| `downloadFunctionCode(accountId, functionName, qualifier?)` | Download the raw ZIP for a function's code (default `latest`) |
| `downloadLayerContent(accountId, layerName, version)`       | Download the raw ZIP for a Lambda layer version               |

### `fc.logs`

| Method                          | Description                                                           |
| ------------------------------- | --------------------------------------------------------------------- |
| `injectAnomaly(req)`            | Inject a synthetic anomaly into the anomaly detector                  |
| `getDeliveryConfig()`           | Return persisted CloudWatch Logs delivery configurations              |
| `getFieldIndexes(logGroupName)` | Parsed `Fields` from index policies on a log group (404 when missing) |

### `fc.organizations`

| Method                         | Description                                                                                 |
| ------------------------------ | ------------------------------------------------------------------------------------------- |
| `getAccounts()`                | List every member account with lifecycle state, parent OU, tags, and directly-attached SCPs |
| `getResponsibilityTransfers()` | List every billing responsibility transfer with direction, status, and the active handshake |

### `fc.rds`

| Method              | Description                                                           |
| ------------------- | --------------------------------------------------------------------- |
| `getInstances()`    | List RDS instances with runtime metadata                              |
| `lambdaInvoke(req)` | Bridge endpoint the PostgreSQL `aws_lambda` extension dispatches into |
| `s3Import(req)`     | Bridge for the PostgreSQL `aws_s3` extension's import path            |
| `s3Export(req)`     | Bridge for the PostgreSQL `aws_s3` extension's export path            |

### `fc.route53`

| Method                          | Description                                                                                                        |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------------ |
| `setHealthCheckStatus(id, req)` | Flip a health check status (`Success` / `Failure` / `Timeout` / `DnsError` / `InsufficientDataPoints` / `Unknown`) |
| `getDnssecMaterial(zoneId)`     | DNSKEY public key + DS digest derived from the zone's first ACTIVE KSK                                             |
| `signDnssecRrset(zoneId, req)`  | Sign an RRset under the zone's first ACTIVE KSK and return the raw RRSIG fields                                    |

### `fc.s3`

| Method                       | Description                                                   |
| ---------------------------- | ------------------------------------------------------------- |
| `getNotifications()`         | List S3 notification events                                   |
| `tickLifecycle()`            | Tick the lifecycle processor                                  |
| `getAccessPoints()`          | List configured S3 access points                              |
| `getObjectLambdaResponses()` | List recorded S3 Object Lambda `WriteGetObjectResponse` calls |

### `fc.scheduler`

| Method                      | Description                                       |
| --------------------------- | ------------------------------------------------- |
| `getSchedules()`            | List every EventBridge Scheduler schedule         |
| `fireSchedule(group, name)` | Fire a schedule manually, bypassing the cron tick |

### `fc.secretsmanager`

| Method           | Description                 |
| ---------------- | --------------------------- |
| `tickRotation()` | Tick the rotation scheduler |

### `fc.ses`

| Method                                | Description                                                            |
| ------------------------------------- | ---------------------------------------------------------------------- |
| `getEmails()`                         | List all sent emails                                                   |
| `simulateInbound(req)`                | Simulate an inbound email (receipt rules)                              |
| `getMetrics()`                        | Aggregate counters for sends / bounces / complaints / deliveries       |
| `getBounces()`                        | List recorded bounce events                                            |
| `setSandbox(sandbox)`                 | Toggle SES account sandbox mode                                        |
| `getEventDestinationDeliveries()`     | List event-destination delivery records (SNS / Firehose / EventBridge) |
| `getDkimPublicKey(identity)`          | Return the DKIM public key fakecloud minted for an identity            |
| `setMailFromStatus(identity, status)` | Flip a custom MAIL FROM domain's verification status synchronously     |
| `getMessageInsights(messageId)`       | Return Message Insights for a sent message                             |
| `getSmtpSubmissions()`                | List messages submitted via the SMTP endpoint                          |

### `fc.sns`

| Method                      | Description                                                        |
| --------------------------- | ------------------------------------------------------------------ |
| `getMessages()`             | List all published messages                                        |
| `getPendingConfirmations()` | List subscriptions pending confirmation                            |
| `confirmSubscription(req)`  | Confirm a pending subscription                                     |
| `getCertPem()`              | Return the PEM-encoded SNS signing certificate (raw PEM, not JSON) |
| `getSms()`                  | List recorded SMS messages dispatched via `SNS Publish`            |

### `fc.sqs`

| Method                | Description                           |
| --------------------- | ------------------------------------- |
| `getMessages()`       | List all messages across all queues   |
| `tickExpiration()`    | Tick the message expiration processor |
| `forceDlq(queueName)` | Force all messages to the queue's DLQ |

### `fc.ssm`

| Method                                 | Description                                                                |
| -------------------------------------- | -------------------------------------------------------------------------- |
| `setCommandStatus(commandId, req)`     | Flip a Run Command's status synchronously                                  |
| `failCommand(commandId, req?)`         | Mark a Run Command's invocations as failed (all or a single instance)      |
| `getParameterPolicyEvents(accountId?)` | List recorded SSM parameter-policy lifecycle events                        |
| `injectSession(req)`                   | Inject a fake Session Manager session without going through `StartSession` |

### `fc.stepfunctions`

| Method                     | Description                                                         |
| -------------------------- | ------------------------------------------------------------------- |
| `getExecutions()`          | List all state machine executions                                   |
| `getSyncExecutions()`      | List recorded `StartSyncExecution` results                          |
| `getExecutionTree(arn)`    | Return the full execution tree (parent + nested child workflows)    |
| `enqueueActivityTask(req)` | Enqueue an Activity task directly for a worker to `GetActivityTask` |

### `fc.wafv2`

| Method          | Description                                                                   |
| --------------- | ----------------------------------------------------------------------------- |
| `evaluate(req)` | Run an arbitrary request payload against the configured WebACLs (opaque JSON) |

#### Full test loop -- asserting on Bedrock calls

```typescript
import { FakeCloud } from "fakecloud";
import {
  BedrockRuntimeClient,
  InvokeModelCommand,
} from "@aws-sdk/client-bedrock-runtime";

const fc = new FakeCloud();
const modelId = "anthropic.claude-3-haiku-20240307-v1:0";

beforeEach(() => fc.reset());

test("classifier branches on spam vs ham", async () => {
  await fc.bedrock.setResponseRules(modelId, [
    { promptContains: "buy now", response: '{"label":"spam"}' },
    { promptContains: null, response: '{"label":"ham"}' }, // catch-all
  ]);

  await classify("hello friend"); // user code calls Bedrock
  await classify("buy now cheap pills");

  const { invocations } = await fc.bedrock.getInvocations();
  expect(invocations).toHaveLength(2);
  expect(invocations[0].output).toContain("ham");
  expect(invocations[1].output).toContain("spam");
});

test("retries on ThrottlingException", async () => {
  await fc.bedrock.queueFault({
    errorType: "ThrottlingException",
    message: "Rate exceeded",
    httpStatus: 429,
    count: 1, // only the first call faults; the retry succeeds
  });

  await classify("hello");

  const { invocations } = await fc.bedrock.getInvocations();
  expect(invocations).toHaveLength(2);
  expect(invocations[0].error).toContain("ThrottlingException");
  expect(invocations[1].error).toBeNull();
});
```

### Error handling

All methods throw `FakeCloudError` on non-2xx responses:

```typescript
import { FakeCloudError } from "fakecloud";

try {
  await fc.cognito.confirmUser({ userPoolId: "pool-1", username: "nobody" });
} catch (err) {
  if (err instanceof FakeCloudError) {
    console.log(err.status); // 404
    console.log(err.body); // "user not found"
  }
}
```
