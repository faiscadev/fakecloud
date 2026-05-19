+++
title = "Java SDK"
description = "Install and use the fakecloud SDK for JVM tests (JUnit, Spring Boot, Micronaut, Quarkus)."
weight = 4
+++

## Install

Gradle (Kotlin DSL):

```kotlin
dependencies {
    testImplementation("dev.fakecloud:fakecloud:0.15.0")
}
```

Maven:

```xml
<dependency>
    <groupId>dev.fakecloud</groupId>
    <artifactId>fakecloud</artifactId>
    <version>0.15.0</version>
    <scope>test</scope>
</dependency>
```

Requires Java 17+. Uses the JDK's built-in `HttpClient` and Jackson for JSON.

## Initialize

```java
import dev.fakecloud.FakeCloud;

FakeCloud fc = new FakeCloud();                         // defaults to http://localhost:4566
FakeCloud fc2 = new FakeCloud("http://localhost:5000"); // explicit base URL
```

## Top-level

| Method                              | Description                              |
| ----------------------------------- | ---------------------------------------- |
| `health()`                          | Server health check                      |
| `reset()`                           | Reset all service state                  |
| `resetService(service)`             | Reset a single service                   |
| `createAdmin(accountId, userName)`  | Bootstrap an admin IAM user in an account |

## `fc.lambda()`

| Method                                                       | Description                                                              |
| ------------------------------------------------------------ | ------------------------------------------------------------------------ |
| `getInvocations()`                                           | List recorded Lambda invocations                                         |
| `getWarmContainers()`                                        | List warm (cached) Lambda containers                                     |
| `evictContainer(functionName)`                               | Evict a warm container                                                   |
| `downloadFunctionCode(accountId, functionName, qualifier)`   | Download the stored zip for a function version (`"latest"` or version)   |
| `downloadLayerContent(accountId, layerName, version)`        | Download the stored zip for a layer version                              |

## `fc.rds()`

| Method                  | Description                                                                  |
| ----------------------- | ---------------------------------------------------------------------------- |
| `getInstances()`        | List RDS instances with runtime metadata                                     |
| `lambdaInvoke(req)`     | Bridge endpoint for the PostgreSQL `aws_lambda` extension                    |
| `s3Import(req)`         | Bridge endpoint for the PostgreSQL `aws_s3` extension (fetch object)         |
| `s3Export(req)`         | Bridge equivalent of `S3:PutObject` from inside the DB container             |

## `fc.elasticache()`

| Method                    | Description                         |
| ------------------------- | ----------------------------------- |
| `getClusters()`           | List ElastiCache cache clusters     |
| `getReplicationGroups()`  | List ElastiCache replication groups |
| `getServerlessCaches()`   | List ElastiCache serverless caches  |
| `getElastiCacheAcls()`    | List ElastiCache (Valkey/Redis) ACLs |

## `fc.ecr()`

| Method                                  | Description                                                  |
| --------------------------------------- | ------------------------------------------------------------ |
| `getRepositories()`                     | List ECR repositories                                        |
| `getImages()`                           | List all ECR images                                          |
| `getImagesForRepository(repositoryName)`| Filter images by repository                                  |
| `getPullThroughRules()`                 | List configured pull-through cache rules                     |

## `fc.logs()`

| Method                            | Description                                       |
| --------------------------------- | ------------------------------------------------- |
| `injectAnomaly(req)`              | Inject a Log Anomaly Detector anomaly             |
| `getDeliveryConfig()`             | Persisted CloudWatch Logs delivery configurations |
| `getFieldIndexes(logGroupName)`   | Parsed `Fields` from index policies on a log group |

## `fc.ses()`

| Method                              | Description                                       |
| ----------------------------------- | ------------------------------------------------- |
| `getEmails()`                       | List all sent emails                              |
| `simulateInbound(req)`              | Simulate an inbound email (receipt rules)         |
| `getMetrics()`                      | Return aggregated send/bounce/complaint counters  |
| `setMailFromStatus(identity, status)`| Force a MAIL FROM verification status              |
| `getDkimPublicKey(identity)`        | Fetch the DKIM public key for an identity         |
| `setSandbox(sandbox)`               | Toggle the account-level sandbox flag             |
| `getBounces()`                      | List captured bounce events                       |
| `getMessageInsights(messageId)`     | Fetch Message Insights for a sent message         |
| `getSmtpSubmissions()`              | List SMTP submission attempts                     |
| `getEventDestinationDeliveries()`   | List event-destination delivery records           |

## `fc.sns()`

| Method                       | Description                                                                                  |
| ---------------------------- | -------------------------------------------------------------------------------------------- |
| `getMessages()`              | List all published messages                                                                  |
| `getPendingConfirmations()`  | List subscriptions pending confirmation                                                      |
| `confirmSubscription(req)`   | Confirm a pending subscription                                                               |
| `getCertPem()`               | Return the PEM-encoded SNS signing cert used by message signature validators                 |
| `getSmsMessages()`           | List captured SMS messages SNS has "delivered"                                               |

## `fc.sqs()`

| Method                 | Description                           |
| ---------------------- | ------------------------------------- |
| `getMessages()`        | List all messages across all queues   |
| `tickExpiration()`     | Tick the message expiration processor |
| `forceDlq(queueName)`  | Force all messages to the queue's DLQ |

## `fc.events()`

| Method           | Description                            |
| ---------------- | -------------------------------------- |
| `getHistory()`   | Get event history and delivery records |
| `fireRule(req)`  | Fire an EventBridge rule manually      |

## `fc.scheduler()`

| Method                          | Description                                  |
| ------------------------------- | -------------------------------------------- |
| `getSchedules()`                | List EventBridge Scheduler schedules         |
| `fireSchedule(group, name)`     | Fire a single schedule immediately           |

## `fc.glue()`

| Method                   | Description                                  |
| ------------------------ | -------------------------------------------- |
| `getJobs()`              | List registered Glue jobs                    |
| `getJobRuns()`           | List all Glue job runs                       |
| `getJobRuns(jobName)`    | Filter job runs by job name                  |

## `fc.s3()`

| Method                          | Description                                  |
| ------------------------------- | -------------------------------------------- |
| `getNotifications()`            | List S3 notification events                  |
| `tickLifecycle()`               | Tick the lifecycle processor                 |
| `getAccessPoints()`             | List S3 access points                        |
| `getObjectLambdaResponses()`    | List S3 Object Lambda responses captured     |

## `fc.dynamodb()`

| Method       | Description            |
| ------------ | ---------------------- |
| `tickTtl()`  | Tick the TTL processor |

## `fc.secretsmanager()`

| Method            | Description                 |
| ----------------- | --------------------------- |
| `tickRotation()`  | Tick the rotation scheduler |

## `fc.cognito()`

| Method                                | Description                                                                                  |
| ------------------------------------- | -------------------------------------------------------------------------------------------- |
| `getUserCodes(poolId, username)`      | Get confirmation codes for a user                                                            |
| `getConfirmationCodes()`              | List all confirmation codes                                                                  |
| `confirmUser(req)`                    | Force-confirm a user (bypass verification)                                                   |
| `getTokens()`                         | List all active tokens                                                                       |
| `expireTokens(req)`                   | Expire tokens (optionally filtered)                                                          |
| `getAuthEvents()`                     | List auth events                                                                             |
| `getPreTokenGenInvocations()`         | List PreTokenGeneration Lambda trigger invocations recorded by `InitiateAuth`                |
| `mintAuthorizationCode(req)`          | Mint a single-use OAuth2 authorization code (programmatic alternative to `/oauth2/authorize`) |
| `setCompromisedPasswords(req)`        | Seed the compromised-password list used by Advanced Security                                 |
| `getWebAuthnCredentials()`            | List stored WebAuthn credentials                                                             |

## `fc.apigatewayv2()`

| Method                              | Description                                                                                  |
| ----------------------------------- | -------------------------------------------------------------------------------------------- |
| `getRequests()`                     | List all HTTP API requests received                                                          |
| `getConnections()`                  | List every active WebSocket connection tracked by API Gateway v2                             |
| `getDomainNameMtlsInfo(domainName)` | Fetch the mTLS truststore info for a custom domain (free-form JSON map)                      |
| `wsUrl(apiId)`                      | Build the WebSocket URL for the given API id on the default `$default` stage                 |
| `wsUrl(apiId, stage)`               | Build the WebSocket URL for the given API id and stage                                       |

## `fc.stepfunctions()`

| Method                       | Description                                                                  |
| ---------------------------- | ---------------------------------------------------------------------------- |
| `getExecutions()`            | List all state machine execution history                                     |
| `getSyncExecutions()`        | List synchronous (`StartSyncExecution`) results                              |
| `getExecutionTree(arn)`      | Full parent/child execution tree under an execution ARN                      |
| `enqueueActivityTask(req)`   | Push a synthetic activity task onto an activity worker queue                 |

## `fc.bedrock()`

| Method                              | Description                                                          |
| ----------------------------------- | -------------------------------------------------------------------- |
| `getInvocations()`                  | List recorded Bedrock runtime invocations                            |
| `setModelResponse(modelId, text)`   | Configure a single canned response for a model                       |
| `setResponseRules(modelId, rules)`  | Replace prompt-conditional response rules for a model                |
| `clearResponseRules(modelId)`       | Clear all prompt-conditional response rules for a model              |
| `queueFault(rule)`                  | Queue a fault rule for the next N calls                              |
| `getFaults()`                       | List currently queued fault rules                                    |
| `clearFaults()`                     | Clear all queued fault rules                                         |

## `fc.bedrockAgent()`

| Method        | Description                                  |
| ------------- | -------------------------------------------- |
| `getAgents()` | List Bedrock Agent (control plane) agents    |

## `fc.bedrockAgentRuntime()`

| Method             | Description                                  |
| ------------------ | -------------------------------------------- |
| `getInvocations()` | List Bedrock Agent Runtime invocations       |

## `fc.ecs()`

| Method                              | Description                                                                                  |
| ----------------------------------- | -------------------------------------------------------------------------------------------- |
| `getClusters()`                     | List ECS clusters                                                                            |
| `getTasks(cluster, status)`         | List tracked tasks, optionally filtered by cluster and status                                |
| `getTask(taskId)`                   | Fetch a single task snapshot by task ID                                                      |
| `getTaskLogs(taskId)`               | Captured stdout/stderr for a task plus its exit code if known                                |
| `forceStopTask(taskId)`             | SIGTERM (then SIGKILL after 10s) the task's running container                                |
| `markTaskFailed(taskId, req)`       | Flip a task to `STOPPED` without killing the container                                       |
| `getEvents()`                       | Replay the lifecycle event log                                                               |
| `getTaskMetadata(taskArn)`          | Aggregated v4 metadata dump for a task (by full ARN)                                         |
| `getCredentials(taskId)`            | Return short-lived IAM credentials for a task (matches the wire shape ECS exposes)           |
| `getMetadataV3(taskId)`             | Raw v3 task metadata document (free-form `Map<String, Object>`)                              |
| `getMetadataV4(taskId)`             | Raw v4 task metadata document (free-form `Map<String, Object>`)                              |

## `fc.elbv2()`

| Method               | Description                                                                                  |
| -------------------- | -------------------------------------------------------------------------------------------- |
| `getLoadBalancers()` | List ELBv2 load balancers (ALB/NLB/GWLB)                                                     |
| `getTargetGroups()`  | List ELBv2 target groups                                                                     |
| `getListeners()`     | List ELBv2 listeners                                                                         |
| `getRules()`         | List ELBv2 listener rules                                                                    |
| `flushAccessLogs()`  | Force every buffered access-log + connection-log line to flush to S3 immediately             |
| `getWafCounts()`     | Return WAFv2 association/evaluation counts the ELBv2 service has accumulated                 |

## `fc.route53()`

| Method                                              | Description                                                                                                                          |
| --------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `setHealthCheckStatus(id, status, reason)`          | Flip a health check between `Success` / `Failure` to drive failover routing in tests; reason is appended to `<Status>` (pass `null` to omit) |
| `getDnssecMaterial(zoneId)`                         | Fetch DNSSEC material (DNSKEY + DS digest) for a hosted zone with at least one ACTIVE KSK                                            |
| `signDnssec(zoneId, req)`                           | Sign an RRset under the zone's first ACTIVE KSK and return raw RRSIG fields                                                          |

## `fc.acm()`

| Method                                       | Description                                                                                  |
| -------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `setCertificateStatus(arnOrId, status, reason)` | Flip an ACM certificate's status synchronously (`ISSUED` / `FAILED` / `VALIDATION_TIMED_OUT`) |
| `approveCertificate(arnOrId)`                | Approve a `PENDING_VALIDATION` certificate (simulates the email click)                       |
| `getCertificateChainInfo(arnOrId)`           | Inspect a stored cert's PEM block counts and byte sizes                                      |

## `fc.applicationAutoscaling()`

| Method            | Description                                            |
| ----------------- | ------------------------------------------------------ |
| `tick()`          | Tick the Application Auto Scaling alarm evaluator      |
| `scheduledTick()` | Tick the scheduled-action processor                    |

## `fc.athena()`

| Method               | Description                                  |
| -------------------- | -------------------------------------------- |
| `getNamedQueries()`  | List every named query stored in the registry |

## `fc.organizations()`

| Method            | Description                                                                                  |
| ----------------- | -------------------------------------------------------------------------------------------- |
| `getAccounts()`   | List every member account in the org (state, parent OU, tags, directly-attached SCPs)        |

## `fc.ssm()`

| Method                                                  | Description                                                                                  |
| ------------------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `setCommandStatus(commandId, accountId, status)`        | Flip the status of every invocation under a `SendCommand` command id                         |
| `failCommand(commandId, req)`                           | Force a command (or specific invocation) into `Failed`                                       |
| `getParameterPolicyEvents(accountId)`                   | Return every parameter-policy event recorded for the account                                 |
| `injectSession(req)`                                    | Drop a fake Session Manager session into state (bypassing `StartSession`)                    |

## `fc.kms()`

| Method        | Description                                  |
| ------------- | -------------------------------------------- |
| `getUsage()`  | Return every recorded KMS data-plane invocation |

## `fc.wafv2()`

| Method              | Description                                                                                  |
| ------------------- | -------------------------------------------------------------------------------------------- |
| `evaluate(request)` | Evaluate an arbitrary request payload against the stored WAFv2 rule set (free-form JSON)     |

## `fc.cloudfront()`

| Method                                          | Description                                                                                  |
| ----------------------------------------------- | -------------------------------------------------------------------------------------------- |
| `setDistributionStatus(distributionId, status)` | Flip a stored CloudFront Distribution's status (`Deployed` / `InProgress`) without waiting   |

## Error handling

All methods throw `FakeCloudError` (a `RuntimeException`) on non-2xx responses:

```java
import dev.fakecloud.FakeCloudError;
import dev.fakecloud.Types.ConfirmUserRequest;

try {
    fc.cognito().confirmUser(new ConfirmUserRequest("pool-1", "nobody"));
} catch (FakeCloudError err) {
    System.out.println(err.status()); // 404
    System.out.println(err.body());   // error body from fakecloud
}
```

## Example: full test loop

```java
import dev.fakecloud.FakeCloud;
import dev.fakecloud.Types.BedrockFaultRule;
import dev.fakecloud.Types.BedrockResponseRule;
import java.util.List;

FakeCloud fc = new FakeCloud();
String modelId = "anthropic.claude-3-haiku-20240307-v1:0";

@BeforeEach
void reset() { fc.reset(); }

@Test
void classifierBranchesOnSpamVsHam() {
    fc.bedrock().setResponseRules(modelId, List.of(
            new BedrockResponseRule("buy now", "{\"label\":\"spam\"}"),
            new BedrockResponseRule(null, "{\"label\":\"ham\"}")));

    classify("hello friend");
    classify("buy now cheap pills");

    var invocations = fc.bedrock().getInvocations().invocations();
    assertEquals(2, invocations.size());
    assertTrue(invocations.get(0).output().contains("ham"));
    assertTrue(invocations.get(1).output().contains("spam"));
}

@Test
void retriesOnThrottlingException() {
    fc.bedrock().queueFault(new BedrockFaultRule(
            "ThrottlingException", "Rate exceeded", 429, 1, null, null));

    classify("hello");

    var invocations = fc.bedrock().getInvocations().invocations();
    assertEquals(2, invocations.size());
    assertTrue(invocations.get(0).error().contains("ThrottlingException"));
    assertNull(invocations.get(1).error());
}
```

## Source

- [`sdks/java`](https://github.com/faiscadev/fakecloud/tree/main/sdks/java)
- [Source README](https://github.com/faiscadev/fakecloud/blob/main/sdks/java/README.md) — always-current method list
