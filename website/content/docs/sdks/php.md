+++
title = "PHP SDK"
description = "Install and use the fakecloud SDK for PHP tests (PHPUnit, Pest, Laravel, Symfony)."
weight = 4
+++

## Install

```bash
composer require fakecloud/fakecloud
```

Requires PHP 8.1+. Uses the built-in `curl` extension and `json_decode`/`json_encode`. No external dependencies.

## Initialize

```php
use FakeCloud\FakeCloud;

$fc = new FakeCloud();                         // defaults to http://localhost:4566
$fc = new FakeCloud('http://localhost:5000');   // explicit base URL
```

## Top-level

| Method                                | Description                                          |
| ------------------------------------- | ---------------------------------------------------- |
| `baseUrl()`                           | Return the configured base URL                       |
| `health()`                            | Server health check                                  |
| `reset()`                             | Reset all service state                              |
| `resetService($service)`              | Reset a single service                               |
| `createAdmin($accountId, $userName)`  | Bootstrap an admin IAM user in an additional account |

## `$fc->lambda()`

| Method                                                        | Description                                         |
| ------------------------------------------------------------- | --------------------------------------------------- |
| `getInvocations()`                                            | List recorded Lambda invocations                    |
| `getWarmContainers()`                                         | List warm (cached) Lambda containers                |
| `evictContainer($functionName)`                               | Evict a warm container                              |
| `downloadFunctionCode($accountId, $functionName, $qualifier)` | Download a function's stored zip bundle (raw bytes) |
| `downloadLayerContent($accountId, $layerName, $version)`      | Download a layer version's stored zip (raw bytes)   |

## `$fc->rds()`

| Method                | Description                                                                |
| --------------------- | -------------------------------------------------------------------------- |
| `getInstances()`      | List RDS instances with runtime metadata                                   |
| `lambdaInvoke($req)`  | Bridge endpoint for the PostgreSQL `aws_lambda` extension                  |
| `s3Import($req)`      | Bridge endpoint for the PostgreSQL `aws_s3` extension (S3 -> DB)           |
| `s3Export($req)`      | Bridge endpoint for the PostgreSQL `aws_s3` extension (DB -> S3 PutObject) |

## `$fc->elasticache()`

| Method                   | Description                         |
| ------------------------ | ----------------------------------- |
| `getClusters()`          | List ElastiCache cache clusters     |
| `getReplicationGroups()` | List ElastiCache replication groups |
| `getServerlessCaches()`  | List ElastiCache serverless caches  |
| `getElastiCacheAcls()`   | List ElastiCache user ACLs          |

## `$fc->ec2()`

| Method                  | Description                                                                                                                                                  |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `getInstances()`        | List EC2 instances with control-plane + runtime metadata                                                                                                    |
| `getInstanceNetworks()` | Inspect each instance's backing network (Docker/Podman network or k8s NetworkPolicy), container IP, isolation backend, and whether security-group enforcement is active |

## `$fc->ecr()`

| Method                              | Description                           |
| ----------------------------------- | ------------------------------------- |
| `getRepositories()`                 | List ECR repositories                 |
| `getImages($repositoryName = null)` | List ECR images (optionally per repo) |
| `getPullThroughRules()`             | List ECR pull-through cache rules     |

## `$fc->logs()`

| Method                           | Description                                   |
| -------------------------------- | --------------------------------------------- |
| `injectAnomaly($req)`            | Inject a CloudWatch Logs anomaly for tests    |
| `getDeliveryConfig()`            | Get the current Logs delivery config snapshot |
| `getFieldIndexes($logGroupName)` | Get configured field indexes for a log group  |

## `$fc->ses()`

| Method                                  | Description                                                  |
| --------------------------------------- | ------------------------------------------------------------ |
| `getEmails()`                           | List all sent emails                                         |
| `simulateInbound($req)`                 | Simulate an inbound email (drive receipt rules)              |
| `getMetrics()`                          | Get SES send/bounce/complaint metrics                        |
| `setMailFromStatus($identity, $status)` | Force a MAIL FROM domain verification status                 |
| `getDkimPublicKey($identity)`           | Get the simulated DKIM public key for an identity            |
| `setSandbox($sandbox)`                  | Toggle sandbox mode for the account                          |
| `getBounces()`                          | List recorded SES bounce events                              |
| `getMessageInsights($messageId)`        | Get SES message insights for a sent message                  |
| `getSmtpSubmissions()`                  | List messages submitted via the SMTP submission endpoint     |
| `getEventDestinationDeliveries()`       | List deliveries fanned out to configuration set destinations |

## `$fc->sns()`

| Method                      | Description                                              |
| --------------------------- | -------------------------------------------------------- |
| `getMessages()`             | List all published messages                              |
| `getPendingConfirmations()` | List subscriptions pending confirmation                  |
| `confirmSubscription($req)` | Confirm a pending subscription                           |
| `getSigningCertPem()`       | Get the PEM body served by the SNS signing-cert endpoint |
| `getSmsMessages()`          | List SMS messages dispatched via SNS                     |

## `$fc->sqs()`

| Method                 | Description                           |
| ---------------------- | ------------------------------------- |
| `getMessages()`        | List all messages across all queues   |
| `tickExpiration()`     | Tick the message expiration processor |
| `forceDlq($queueName)` | Force all messages to the queue's DLQ |

## `$fc->applicationAutoscaling()`

| Method            | Description                                               |
| ----------------- | --------------------------------------------------------- |
| `tick()`          | Tick the Application Auto Scaling metric/policy processor |
| `scheduledTick()` | Tick the scheduled-action processor                       |

## `$fc->athena()`

| Method              | Description                          |
| ------------------- | ------------------------------------ |
| `getNamedQueries()` | List all stored Athena named queries |

## `$fc->events()`

| Method           | Description                            |
| ---------------- | -------------------------------------- |
| `getHistory()`   | Get event history and delivery records |
| `fireRule($req)` | Fire an EventBridge rule manually      |

## `$fc->scheduler()`

| Method                        | Description                             |
| ----------------------------- | --------------------------------------- |
| `getSchedules()`              | List EventBridge Scheduler schedules    |
| `fireSchedule($group, $name)` | Fire a scheduled invocation immediately |

## `$fc->glue()`

| Method                        | Description                             |
| ----------------------------- | --------------------------------------- |
| `getJobs()`                   | List all Glue jobs                      |
| `getJobRuns($jobName = null)` | List Glue job runs (optionally per job) |

## `$fc->s3()`

| Method                       | Description                                                 |
| ---------------------------- | ----------------------------------------------------------- |
| `getNotifications()`         | List S3 notification events                                 |
| `tickLifecycle()`            | Tick the lifecycle processor                                |
| `getAccessPoints()`          | List S3 access points                                       |
| `getObjectLambdaResponses()` | List recorded S3 Object Lambda WriteGetObjectResponse calls |

## `$fc->dynamodb()`

| Method      | Description            |
| ----------- | ---------------------- |
| `tickTtl()` | Tick the TTL processor |

## `$fc->secretsmanager()`

| Method           | Description                 |
| ---------------- | --------------------------- |
| `tickRotation()` | Tick the rotation scheduler |

## `$fc->cognito()`

| Method                             | Description                                                                    |
| ---------------------------------- | ------------------------------------------------------------------------------ |
| `getUserCodes($poolId, $username)` | Get confirmation codes for a user                                              |
| `getConfirmationCodes()`           | List all confirmation codes                                                    |
| `confirmUser($req)`                | Confirm a user (bypass verification)                                           |
| `getTokens()`                      | List all active tokens                                                         |
| `expireTokens($req)`               | Expire tokens (optionally filtered)                                            |
| `getAuthEvents()`                  | List auth events                                                               |
| `getPreTokenGenInvocations()`      | List PreTokenGeneration Lambda trigger invocations                             |
| `mintAuthorizationCode($req)`      | Mint a single-use OAuth2 authorization code (alternative to /oauth2/authorize) |
| `setCompromisedPasswords($req)`    | Mark passwords as compromised for the advanced-security simulator              |
| `getWebAuthnCredentials()`         | List enrolled WebAuthn credentials                                             |

## `$fc->apigatewayv2()`

| Method                         | Description                                  |
| ------------------------------ | -------------------------------------------- |
| `getRequests()`                | List all HTTP API requests received          |
| `getConnections()`             | List active WebSocket API connections        |
| `mtlsInfo($domainName)`        | Get mTLS truststore info for a custom domain |
| `wsUrl($apiId, $stage = null)` | Compute the WebSocket URL for an API/stage   |

## `$fc->stepfunctions()`

| Method                      | Description                                               |
| --------------------------- | --------------------------------------------------------- |
| `getExecutions()`           | List all state machine execution history                  |
| `getSyncExecutions()`       | List `StartSyncExecution` results                         |
| `getExecutionTree($arn)`    | Get the full parent/child execution tree for an execution |
| `enqueueActivityTask($req)` | Enqueue an activity task for `GetActivityTask` consumers  |

## `$fc->bedrock()`

| Method                               | Description                                             |
| ------------------------------------ | ------------------------------------------------------- |
| `getInvocations()`                   | List recorded Bedrock runtime invocations               |
| `setModelResponse($modelId, $text)`  | Configure a single canned response for a model          |
| `setResponseRules($modelId, $rules)` | Replace prompt-conditional response rules for a model   |
| `clearResponseRules($modelId)`       | Clear all prompt-conditional response rules for a model |
| `queueFault($rule)`                  | Queue a fault rule for the next N calls                 |
| `getFaults()`                        | List currently queued fault rules                       |
| `clearFaults()`                      | Clear all queued fault rules                            |

## `$fc->bedrockAgent()`

| Method        | Description                    |
| ------------- | ------------------------------ |
| `getAgents()` | List Bedrock Agent definitions |

## `$fc->bedrockAgentRuntime()`

| Method             | Description                            |
| ------------------ | -------------------------------------- |
| `getInvocations()` | List Bedrock Agent runtime invocations |

## `$fc->ecs()`

| Method                                      | Description                                                   |
| ------------------------------------------- | ------------------------------------------------------------- |
| `getClusters()`                             | List ECS clusters                                             |
| `getTasks($cluster = null, $status = null)` | List ECS tasks (optionally filtered by cluster and status)    |
| `getTask($taskId)`                          | Get a single ECS task                                         |
| `getTaskLogs($taskId)`                      | Get captured stdout/stderr logs for a task                    |
| `forceStopTask($taskId)`                    | Force-stop a running task                                     |
| `markTaskFailed($taskId, $req)`             | Mark a task as failed with the supplied reason                |
| `getEvents()`                               | List ECS service/task lifecycle events                        |
| `getTaskMetadata($taskArn)`                 | Get ECS task metadata (control-plane shape)                   |
| `getTaskCredentials($taskId)`               | Get the IAM credentials served via the task metadata endpoint |
| `getTaskMetadataV3($taskId)`                | Get the task metadata v3 payload                              |
| `getTaskMetadataV4($taskId)`                | Get the task metadata v4 payload                              |

## `$fc->elbv2()`

| Method               | Description                                         |
| -------------------- | --------------------------------------------------- |
| `getLoadBalancers()` | List ELBv2 load balancers                           |
| `getTargetGroups()`  | List ELBv2 target groups                            |
| `getListeners()`     | List ELBv2 listeners                                |
| `getRules()`         | List ELBv2 listener rules                           |
| `flushAccessLogs()`  | Flush buffered ELBv2 access logs to their S3 bucket |
| `getWafCounts()`     | Get WAF allow/block counts seen by ELBv2 listeners  |

## `$fc->route53()`

| Method                                        | Description                                                                                                                                  |
| --------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `setHealthCheckStatus($id, $status, $reason)` | Flip a health check between `Success` / `Failure` / `Timeout` / `DnsError` / `InsufficientDataPoints` / `Unknown` to drive failover routing  |
| `getDnssecMaterial($zoneId)`                  | Get the simulated DNSSEC key material for a hosted zone                                                                                      |
| `signDnssec($zoneId, $req)`                   | Sign a payload with the zone's DNSSEC key                                                                                                    |

## `$fc->acm()`

| Method                                             | Description                                     |
| -------------------------------------------------- | ----------------------------------------------- |
| `setCertificateStatus($arnOrId, $status, $reason)` | Force an ACM certificate into a specific status |
| `approveCertificate($arnOrId)`                     | Approve a pending ACM certificate request       |
| `getCertificateChainInfo($arnOrId)`                | Get parsed chain info for an ACM certificate    |

## `$fc->organizations()`

| Method          | Description                        |
| --------------- | ---------------------------------- |
| `getAccounts()` | List Organizations member accounts |

## `$fc->ssm()`

| Method                                                               | Description                                                |
| -------------------------------------------------------------------- | ---------------------------------------------------------- |
| `setCommandStatus($commandId, $status, $accountId = null)`           | Force an SSM Run Command into a specific terminal status   |
| `failCommand($commandId, $accountId = null, $errorCode = null, ...)` | Fail an SSM Run Command with structured error metadata     |
| `getParameterPolicyEvents($accountId = null)`                        | List Parameter Store policy expiration/notification events |
| `injectSession($req)`                                                | Inject a Session Manager session for inspection in tests   |

## `$fc->kms()`

| Method       | Description                                       |
| ------------ | ------------------------------------------------- |
| `getUsage()` | Get KMS key usage counters (encrypt/decrypt/sign) |

## `$fc->wafv2()`

| Method            | Description                                                       |
| ----------------- | ----------------------------------------------------------------- |
| `evaluate($body)` | Evaluate a request against a WAFv2 WebACL and return the decision |

## `$fc->cloudfront()`

| Method                                            | Description                                   |
| ------------------------------------------------- | --------------------------------------------- |
| `setDistributionStatus($distributionId, $status)` | Force a CloudFront distribution into a status |

## Error handling

All methods throw `FakeCloudError` (a `RuntimeException`) on non-2xx responses:

```php
use FakeCloud\FakeCloudError;
use FakeCloud\ConfirmUserRequest;

try {
    $fc->cognito()->confirmUser(new ConfirmUserRequest('pool-1', 'nobody'));
} catch (FakeCloudError $err) {
    echo $err->status; // 404
    echo $err->body;   // error body from fakecloud
}
```

## Example: full test loop

```php
use FakeCloud\FakeCloud;
use FakeCloud\BedrockFaultRule;
use FakeCloud\BedrockResponseRule;

$fc = new FakeCloud();
$modelId = 'anthropic.claude-3-haiku-20240307-v1:0';

// PHPUnit setUp
$fc->reset();

// Test: classifier branches on spam vs ham
$fc->bedrock()->setResponseRules($modelId, [
    new BedrockResponseRule('buy now', '{"label":"spam"}'),
    new BedrockResponseRule(null, '{"label":"ham"}'),
]);

classify('hello friend');
classify('buy now cheap pills');

$invocations = $fc->bedrock()->getInvocations()->invocations;
$this->assertCount(2, $invocations);
$this->assertStringContainsString('ham', $invocations[0]->output);
$this->assertStringContainsString('spam', $invocations[1]->output);

// Test: retries on throttling
$fc->reset();
$fc->bedrock()->queueFault(
    new BedrockFaultRule('ThrottlingException', 'Rate exceeded', 429, 1)
);

classify('hello');

$invocations = $fc->bedrock()->getInvocations()->invocations;
$this->assertCount(2, $invocations);
$this->assertStringContainsString('ThrottlingException', $invocations[0]->error);
$this->assertNull($invocations[1]->error);
```

## Source

- [`sdks/php`](https://github.com/faiscadev/fakecloud/tree/main/sdks/php)
- [Source README](https://github.com/faiscadev/fakecloud/blob/main/sdks/php/README.md) — always-current method list
