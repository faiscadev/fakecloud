# Roadmap

fakecloud's goal is to be the best free AWS emulator for integration testing and local development. This roadmap outlines what's coming next.

For every service we implement, the standard is the same: full API coverage, real behavior (not stubs), conformance testing against AWS Smithy models, and cross-service integrations where applicable.

## Up next

### Kinesis

Kinesis Data Streams and Kinesis Data Firehose. This also unlocks DynamoDB Streams, which depends on a Kinesis-compatible streaming backend.

### RDS

Full RDS API with real database engines. The approach: implement the complete AWS API surface (CreateDBInstance, ModifyDBInstance, snapshots, parameter groups, etc.) and run actual PostgreSQL and MySQL instances via Docker — the same pattern fakecloud uses for Lambda execution. Your tests talk to real databases, managed through the standard RDS API.

### ECR + ECS

Container registry and container orchestration. ECR provides image storage and lifecycle management. ECS provides clusters, services, task definitions, and task execution — backed by real Docker containers.

### ElastiCache

Full ElastiCache API backed by real Redis instances via Docker. Create, modify, and delete cache clusters through the standard API, with actual Redis available for your application to connect to.

### Elastic Load Balancing

Application Load Balancers, target groups, listeners, and routing rules. Configuration management and basic request routing.

### CloudFront

Distribution configuration, cache behaviors, origins, and invalidations.

### API Gateway v2

HTTP APIs and WebSocket APIs. REST API v1 is available in LocalStack Community; HTTP API v2 is not. Integrates with Lambda (already supported).

### Step Functions

Amazon States Language interpreter with full execution semantics. Task, Choice, Parallel, Map, Wait, and all other state types. Integrates with Lambda and other fakecloud services.

### CloudWatch Metrics

Metric storage, alarms, dashboards, and math expressions. Completes the CloudWatch story alongside our existing CloudWatch Logs implementation (113 operations).

## Testing APIs

fakecloud is built for testing. Beyond emulating the AWS API, fakecloud exposes its own `/_fakecloud/*` endpoints that give you capabilities AWS doesn't — inspecting internal state, simulating events, and setting up test scenarios.

### Introspection *(shipped)*

Read internal state that AWS doesn't expose. Useful for test assertions.

- **`GET /_fakecloud/ses/emails`** — Every email sent through SES, with full headers and body.
- **`GET /_fakecloud/lambda/invocations`** — Every Lambda invocation with request payload and response.
- **`GET /_fakecloud/sns/messages`** — All messages published to SNS topics.
- **`GET /_fakecloud/sqs/messages`** — All messages across all SQS queues with receive counts.
- **`GET /_fakecloud/events/history`** — All EventBridge events and target deliveries.
- **`GET /_fakecloud/s3/notifications`** — All S3 notification events that fired.
- **`GET /_fakecloud/sns/pending-confirmations`** — SNS subscriptions awaiting confirmation.
- **`GET /_fakecloud/lambda/warm-containers`** — Lambda containers currently warm.

### Simulation *(shipped)*

Trigger things that normally come from AWS infrastructure or external systems.

- **`POST /_fakecloud/ses/inbound`** — Simulate receiving an email. Evaluates receipt rules and executes S3/SNS/Lambda actions.
- **`POST /_fakecloud/events/fire-rule`** — Fire a specific EventBridge rule immediately, regardless of its schedule.
- **`POST /_fakecloud/dynamodb/ttl-processor/tick`** — Expire DynamoDB items whose TTL attribute is in the past.
- **`POST /_fakecloud/secretsmanager/rotation-scheduler/tick`** — Rotate secrets whose rotation schedule is due.
- **`POST /_fakecloud/sqs/expiration-processor/tick`** — Remove expired messages from all SQS queues.
- **`POST /_fakecloud/sqs/{queue-name}/force-dlq`** — Force messages to dead-letter queue without waiting for more receives.
- **`POST /_fakecloud/s3/lifecycle-processor/tick`** — Run S3 lifecycle rules (expiration, transitions) immediately.
- **`POST /_fakecloud/sns/confirm-subscription`** — Force-confirm a pending SNS subscription.
- **`POST /_fakecloud/lambda/{function-name}/evict-container`** — Force cold start by evicting warm container.

### State setup *(shipped)*

- **`POST /_fakecloud/reset`** — Reset all state across all services.
- **`POST /_fakecloud/reset/{service}`** — Reset only a specific service's state.

### SDKs

Client libraries for TypeScript, Python, Go, Rust, and Java will wrap the `/_fakecloud/*` endpoints for cleaner test code. The HTTP APIs are stable and usable directly.

## Design principles

**Smart proxy pattern** — For services that wrap stateful software (RDS, ElastiCache, ECS), fakecloud implements the full AWS API and delegates execution to real software via Docker. This gives you API compatibility and real behavior in one package.

**No stubs** — Every operation either does what AWS does or returns an explicit error. We don't return fake success responses for things we haven't implemented.

**Conformance testing** — Every service is tested against AWS Smithy models with thousands of auto-generated test variants covering boundary values, optional field permutations, and negative cases.

## Suggesting a service

Open an issue on [GitHub](https://github.com/faiscadev/fakecloud/issues) with the service name and your use case. Real-world demand drives prioritization.
