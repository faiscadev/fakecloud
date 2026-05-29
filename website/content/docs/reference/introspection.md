+++
title = "Introspection endpoints"
description = "Every /_fakecloud/* endpoint for test assertions, simulation, and state control."
weight = 3
+++

fakecloud exposes `/_fakecloud/*` endpoints for testing behaviors that AWS runs asynchronously (TTL expiration, scheduled rotation, lifecycle, etc.) and for asserting on state from within tests. The first-party SDKs wrap these into ergonomic helpers -- see [SDK setup](/docs/getting-started/sdk-setup/) -- but the raw endpoints are documented here as the source of truth.

This page lists every `/_fakecloud/*` endpoint shipped today: 85 routes across 27 service areas. Endpoints marked **NEW** were added in the last two weeks.
This page lists every `/_fakecloud/*` endpoint shipped today: 84 routes across 27 service areas. Endpoints marked **NEW** were added in the last two weeks.

## Health and reset

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/health` | GET | Returns `{"status":"ok","version":"<v>","services":[...]}`. |
| `/_fakecloud/reset/{service}` | POST | Reset all state for a single service. |
| `/_fakecloud/reset/{service}/{account_id}` | POST | Reset a single service for one account (multi-account setups). |

```sh
curl http://localhost:4566/_fakecloud/health
```

```json
{
  "status": "ok",
  "version": "0.13.3",
  "services": [
    "apigatewayv2", "bedrock", "cloudformation", "cognito-idp",
    "dynamodb", "elasticache", "events", "iam", "kinesis", "kms",
    "lambda", "logs", "rds", "s3", "secretsmanager", "ses", "sns",
    "sqs", "ssm", "states", "sts"
  ]
}
```

## ACM

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/acm/certificates/{arn_or_id}/status` | POST | Change a certificate's status. |
| `/_fakecloud/acm/certificates/{arn_or_id}/approve` | POST | Approve a pending certificate. |
| `/_fakecloud/acm/certificates/{arn_or_id}/chain-info` | GET | **NEW** -- Inspect issued chain (subject, issuer, SANs, validity). |

## API Gateway v2

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/apigatewayv2/requests` | GET | List captured HTTP API requests. |
| `/_fakecloud/apigatewayv2/connections` | GET | List active WebSocket connections. |
| `/_fakecloud/apigatewayv2/ws/{api_id}` | WS | WebSocket upgrade endpoint backing a deployed WebSocket API. |
| `/_fakecloud/apigatewayv2/domain-names/{name}/mtls-info` | GET | **NEW** -- Inspect attached mTLS trust store and client-CA bundle. |

## Application Auto Scaling

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/application-autoscaling/tick` | POST | Trigger one policy-evaluation tick. |
| `/_fakecloud/application-autoscaling/scheduled-tick` | POST | Run scheduled scaling actions that are due. |

## Athena

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/athena/named-queries` | GET | **NEW** -- List every named query across workgroups with `lastUsedAt` bumped each time `StartQueryExecution` resolves the query by id. |

## Bedrock

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/bedrock/invocations` | GET | List model invocations captured by the runtime. |
| `/_fakecloud/bedrock/models/{model_id}/response` | POST | Set a single canned response for a model. |
| `/_fakecloud/bedrock/models/{model_id}/responses` | POST | Set prompt-conditional response rules. |
| `/_fakecloud/bedrock/faults` | POST | Inject faults (throttle, timeout, error) into the next N invocations. |

## Bedrock Agent

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/bedrock-agent/agents` | GET | **NEW** -- List agents flattened with aliases, versions, knowledge bases, and collaborators. |

## Bedrock Agent Runtime

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/bedrock-agent-runtime/invocations` | GET | **NEW** -- List InvokeAgent / InvokeInlineAgent / InvokeFlow / Retrieve / RetrieveAndGenerate / CreateInvocation calls. |

## CloudFront

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/cloudfront/distributions/{id}/status` | POST | Change a distribution's status (deployed / in-progress / failed). |

## CloudWatch

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/cloudwatch/alarms` | GET | **NEW** -- Every metric and composite alarm across all accounts and regions, with current state, actions, and (metric alarms) namespace/metric/threshold/comparison or (composite alarms) the alarm rule. Sorted by account, region, name. |
| `/_fakecloud/cloudwatch/metrics` | GET | **NEW** -- Every unique metric series (account, region, namespace, metric, dimensions) with its datapoint count and latest datapoint. Sorted by account, region, namespace, metric. |

`GET /_fakecloud/cloudwatch/alarms` response:

```json
{
  "alarms": [
    {
      "accountId": "123456789012",
      "region": "us-east-1",
      "name": "HighErrors",
      "type": "metric",
      "state": "INSUFFICIENT_DATA",
      "stateReason": "Unchecked: Initial alarm creation",
      "stateUpdatedTimestamp": "2026-05-29T00:00:00+00:00",
      "actionsEnabled": true,
      "alarmActions": ["arn:aws:sns:us-east-1:123456789012:ops"],
      "okActions": [],
      "insufficientDataActions": [],
      "namespace": "MyApp",
      "metricName": "Errors",
      "threshold": 10.0,
      "comparisonOperator": "GreaterThanThreshold"
    },
    {
      "accountId": "123456789012",
      "region": "us-east-1",
      "name": "comp",
      "type": "composite",
      "state": "INSUFFICIENT_DATA",
      "stateReason": "",
      "stateUpdatedTimestamp": "2026-05-29T00:00:00+00:00",
      "actionsEnabled": true,
      "alarmActions": [],
      "okActions": [],
      "insufficientDataActions": [],
      "alarmRule": "ALARM(HighErrors)"
    }
  ]
}
```

`GET /_fakecloud/cloudwatch/metrics` response:

```json
{
  "metrics": [
    {
      "accountId": "123456789012",
      "region": "us-east-1",
      "namespace": "MyApp",
      "metricName": "Requests",
      "dimensions": [{ "name": "Service", "value": "api" }],
      "datapointCount": 2,
      "latest": { "timestamp": "2026-05-29T00:00:00+00:00", "value": 42.0, "unit": "Count" }
    }
  ]
}
```

## Cognito

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/cognito/confirmation-codes` | GET | List all pending sign-up/forgot-password codes. |
| `/_fakecloud/cognito/confirmation-codes/{pool_id}/{username}` | GET | Codes for a specific user. |
| `/_fakecloud/cognito/confirm-user` | POST | Force-confirm a user without code entry. |
| `/_fakecloud/cognito/tokens` | GET | List currently active access/ID/refresh tokens. |
| `/_fakecloud/cognito/expire-tokens` | POST | Forcibly expire tokens. |
| `/_fakecloud/cognito/auth-events` | GET | List authentication events recorded by adaptive auth. |
| `/_fakecloud/cognito/authorization-codes` | POST | Mint an OAuth2 authorization code (for hosted-UI flow simulation). |
| `/_fakecloud/cognito/compromised-passwords` | POST | **NEW** -- Mark a password as compromised so the next AdminInitiateAuth surfaces the advisory. |
| `/_fakecloud/cognito/webauthn-credentials` | GET | **NEW** -- List registered WebAuthn credentials with their attestation details. |
| `/_fakecloud/cognito/pretokengen/invocations` | GET | **NEW** -- List PreTokenGeneration Lambda trigger invocations with parsed claim overrides, suppressed claims, and group overrides. |

## DynamoDB

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/dynamodb/ttl-processor/tick` | POST | Expire TTL items that are due. |

## ECR

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/ecr/repositories` | GET | List all repositories with image counts. |
| `/_fakecloud/ecr/images` | GET | List images across repositories. |
| `/_fakecloud/ecr/pull-through-rules` | GET | List configured pull-through cache rules. |

## ECS

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/ecs/clusters` | GET | List clusters and their service/task counts. |
| `/_fakecloud/ecs/tasks` | GET | List tasks across clusters. |
| `/_fakecloud/ecs/tasks/{task_id}` | GET | Full task details (container state, exit codes, network). |
| `/_fakecloud/ecs/tasks/{task_id}/logs` | GET | Captured stdout/stderr for task containers. |
| `/_fakecloud/ecs/tasks/{task_id}/force-stop` | POST | Force-stop a running task. |
| `/_fakecloud/ecs/tasks/{task_id}/mark-failed` | POST | Mark a task as failed for testing failure handling. |
| `/_fakecloud/ecs/events` | GET | List ECS service/task lifecycle events. |
| `/_fakecloud/ecs/creds/{task_id}` | GET | Task IAM credentials (used by the ECS Exec data plane). |
| `/_fakecloud/ecs/v3/{task_id}` | GET | **NEW** -- ECS metadata v3 endpoint exposed to task containers. |
| `/_fakecloud/ecs/v4/{task_id}` | GET | **NEW** -- ECS metadata v4 endpoint exposed to task containers. |
| `/_fakecloud/ecs/metadata/{task_arn}` | GET | Aggregated v4 metadata dump keyed by full task ARN (URL-encoded). Same shape as `ECS_CONTAINER_METADATA_URI_V4`, addressable from tests holding a RunTask response. |

## ElastiCache

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/elasticache/clusters` | GET | List cache clusters and node state. |
| `/_fakecloud/elasticache/replication-groups` | GET | List replication groups with primary/replica layout. |
| `/_fakecloud/elasticache/serverless-caches` | GET | List serverless cache resources. |
| `/_fakecloud/elasticache/acls` | GET | List ACL state (users + user groups) for replication groups with user groups attached. |

## ELBv2

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/elbv2/load-balancers` | GET | List ALB/NLB/GWLB load balancers. |
| `/_fakecloud/elbv2/target-groups` | GET | List target groups and target health. |
| `/_fakecloud/elbv2/listeners` | GET | List listeners across load balancers. |
| `/_fakecloud/elbv2/rules` | GET | List listener rules. |
| `/_fakecloud/elbv2/access-logs/flush` | POST | Force-flush buffered access logs to S3. |
| `/_fakecloud/elbv2/waf-counts` | GET | **NEW** -- Per-target-group WAF allow/block counters. |

## EventBridge

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/events/history` | GET | All events seen plus per-rule delivery results. |
| `/_fakecloud/events/fire-rule` | POST | Manually fire a rule against the current bus state. |

## Glue

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/glue/jobs` | GET | **NEW** -- All Glue Jobs (CreateJob ledger) across accounts. |
| `/_fakecloud/glue/job-runs` | GET | **NEW** -- All JobRun state across accounts. Optional `?job_name=foo` filter. |
| `/_fakecloud/glue/crawlers` | GET | **NEW** -- All Glue crawlers across accounts with role, database, state, a target summary, and timestamps. Sorted by account, name. |

## IAM

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/iam/create-admin` | POST | Bootstrap an admin user in a non-default account (multi-account setups). |

## KMS

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/kms/usage` | GET | Per-key usage records (op, principal, timestamp) for billing/audit simulation. |

## Lambda

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/lambda/invocations` | GET | All recorded invocations (function, payload, response, duration). |
| `/_fakecloud/lambda/warm-containers` | GET | Current warm execution environments. |
| `/_fakecloud/lambda/{function_name}/evict-container` | POST | Force a cold start by evicting warm containers. |
| `/_fakecloud/lambda/layer-content/{account_id}/{layer_name}/{file}` | GET | Serve a layer zip (used by the invoke runtime to mount `/opt`). |

## Logs

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/logs/anomalies/inject` | POST | Inject anomaly findings against a log group/anomaly detector. |
| `/_fakecloud/logs/delivery-config` | GET | **NEW** -- Persisted delivery configurations: `Delivery` joined with its `DeliverySource.logType`, plus `recordFields`, `fieldDelimiter`, `s3DeliveryConfiguration`, `createdAt`. |
| `/_fakecloud/logs/field-indexes/{log_group_name}` | GET | **NEW** -- Parsed `Fields` from each index policy on a log group. 404 when the group does not exist. |

## Organizations

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/organizations/accounts` | GET | **NEW** -- List every member account with lifecycle state, parent OU, tags, and directly-attached SCPs. |

## RDS

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/rds/instances` | GET | List DB instances with engine, status, and lifecycle metadata. |
| `/_fakecloud/rds/lambda-invoke` | POST | Invoke a Lambda from the RDS aws_lambda extension bridge. |
| `/_fakecloud/rds/s3-import` | POST | Run an aws_s3 import (S3 -> Postgres/MySQL). |
| `/_fakecloud/rds/s3-export` | POST | Run an aws_s3 export (Postgres/MySQL -> S3). |

## Route53

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/route53/health-checks/{id}/status` | POST | Flip a health check status (healthy / unhealthy). |
| `/_fakecloud/route53/zones/{id}/dnssec` | GET | **NEW** -- Inspect a zone's DNSSEC configuration. |
| `/_fakecloud/route53/zones/{id}/dnssec/sign` | POST | **NEW** -- Sign a zone's DNSSEC records. |

## S3

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/s3/notifications` | GET | All bucket notification events captured. |
| `/_fakecloud/s3/lifecycle-processor/tick` | POST | Run one lifecycle evaluation tick. |
| `/_fakecloud/s3/access-points` | GET | **NEW** -- Registry of S3 access points across all accounts. |
| `/_fakecloud/s3/object-lambda-responses` | GET | **NEW** -- Stored bodies from WriteGetObjectResponse calls (S3 Object Lambda). |

## Scheduler (EventBridge Scheduler)

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/scheduler/schedules` | GET | List all schedules across groups. |
| `/_fakecloud/scheduler/fire/{group}/{name}` | POST | Manually fire a schedule's target. |

## Secrets Manager

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/secretsmanager/rotation-scheduler/tick` | POST | Rotate any secrets whose rotation window is due. |

## SES

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/ses/emails` | GET | List sent emails with full envelope and body. |
| `/_fakecloud/ses/inbound` | POST | Simulate an inbound email being received by a rule set. |
| `/_fakecloud/ses/account/sandbox` | POST | Move the account in or out of the SES sandbox. |
| `/_fakecloud/ses/identities/{name}/mail-from-status` | POST | Set the custom MAIL FROM verification state. |
| `/_fakecloud/ses/identities/{name}/dkim-public-key` | GET | Fetch the published DKIM public key for an identity. |
| `/_fakecloud/ses/metrics` | GET | **NEW** -- Aggregate send metrics (sends, bounces, complaints, deliveries). |
| `/_fakecloud/ses/bounces` | GET | List bounces queued via `SendBounce` with per-recipient DSN fields. |
| `/_fakecloud/ses/messages/{message_id}/insights` | GET | Per-message delivery tracking (sends, deliveries, bounces, complaints). |
| `/_fakecloud/ses/smtp/submissions` | GET | Messages accepted via the SMTP submission listener. |
| `/_fakecloud/ses/event-destinations/deliveries` | GET | Log of every event dispatched to a configured event destination. |

## SNS

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/sns/messages` | GET | All Publish calls with per-subscriber delivery results. |
| `/_fakecloud/sns/pending-confirmations` | GET | List subscriptions waiting on confirmation. |
| `/_fakecloud/sns/confirm-subscription` | POST | Force-confirm a pending subscription. |
| `/_fakecloud/sns/sms` | GET | List delivered SMS messages. |
| `/_fakecloud/sns/cert.pem` | GET | Serve the SNS signing certificate (PEM) used to verify message signatures. |

## SQS

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/sqs/messages` | GET | List messages across queues. |
| `/_fakecloud/sqs/expiration-processor/tick` | POST | Expire messages whose retention window has passed. |
| `/_fakecloud/sqs/{queue_name}/force-dlq` | POST | Force-move in-flight messages to the dead-letter queue. |

## SSM

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/ssm/commands/{command_id}/status` | POST | Change a Run Command invocation status. |
| `/_fakecloud/ssm/commands/{command_id}/fail` | POST | Mark all invocations of a command as failed. |
| `/_fakecloud/ssm/parameter-policy-events` | GET | List Parameter Store policy events (Expiration/NoChangeNotification). |
| `/_fakecloud/ssm/parameter-policy-events` | DELETE | Clear recorded policy events. |
| `/_fakecloud/ssm/sessions/inject` | POST | **NEW** -- Inject a fully-formed Session Manager session for testing. |

## Step Functions

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/stepfunctions/executions` | GET | List state-machine executions and step history. |
| `/_fakecloud/stepfunctions/enqueue-activity-task` | POST | Enqueue an activity task for a worker to pick up. |

## WAFv2

| Endpoint | Method | Description |
| -------- | ------ | ----------- |
| `/_fakecloud/wafv2/evaluate` | POST | Evaluate an arbitrary request against a Web ACL's rules and return the verdict. |

## Conventions

- All endpoints return JSON unless otherwise noted (the SNS `cert.pem` route returns `application/x-pem-file`, the Lambda `layer-content` route returns `application/zip`, the API Gateway v2 WebSocket route is an upgrade endpoint).
- Path parameters use `{snake_case}` placeholders.
- `POST` endpoints that mutate state always accept an empty body when no parameters are required.
- All endpoints are gated behind the fakecloud server itself -- they are never exposed by the real AWS SDKs, so production traffic cannot reach them by accident.

> **NEW** markers reflect endpoints added in the last two weeks (2026-04-27 through 2026-05-11). Older entries with no marker are stable parts of the introspection surface.
