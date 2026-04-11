# fakecloud

TypeScript client SDK for [fakecloud](https://github.com/faiscadev/fakecloud) — a local AWS cloud emulator.

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

| Method                  | Description             |
| ----------------------- | ----------------------- |
| `health()`              | Server health check     |
| `reset()`               | Reset all service state |
| `resetService(service)` | Reset a single service  |

### `fc.lambda`

| Method                         | Description                          |
| ------------------------------ | ------------------------------------ |
| `getInvocations()`             | List recorded Lambda invocations     |
| `getWarmContainers()`          | List warm (cached) Lambda containers |
| `evictContainer(functionName)` | Evict a warm container               |

### `fc.ses`

| Method                 | Description                               |
| ---------------------- | ----------------------------------------- |
| `getEmails()`          | List all sent emails                      |
| `simulateInbound(req)` | Simulate an inbound email (receipt rules) |

### `fc.sns`

| Method                      | Description                             |
| --------------------------- | --------------------------------------- |
| `getMessages()`             | List all published messages             |
| `getPendingConfirmations()` | List subscriptions pending confirmation |
| `confirmSubscription(req)`  | Confirm a pending subscription          |

### `fc.sqs`

| Method                | Description                           |
| --------------------- | ------------------------------------- |
| `getMessages()`       | List all messages across all queues   |
| `tickExpiration()`    | Tick the message expiration processor |
| `forceDlq(queueName)` | Force all messages to the queue's DLQ |

### `fc.events`

| Method          | Description                            |
| --------------- | -------------------------------------- |
| `getHistory()`  | Get event history and delivery records |
| `fireRule(req)` | Fire an EventBridge rule manually      |

### `fc.s3`

| Method               | Description                  |
| -------------------- | ---------------------------- |
| `getNotifications()` | List S3 notification events  |
| `tickLifecycle()`    | Tick the lifecycle processor |

### `fc.dynamodb`

| Method      | Description            |
| ----------- | ---------------------- |
| `tickTtl()` | Tick the TTL processor |

### `fc.secretsmanager`

| Method           | Description                 |
| ---------------- | --------------------------- |
| `tickRotation()` | Tick the rotation scheduler |

### `fc.cognito`

| Method                           | Description                          |
| -------------------------------- | ------------------------------------ |
| `getUserCodes(poolId, username)` | Get confirmation codes for a user    |
| `getConfirmationCodes()`         | List all confirmation codes          |
| `confirmUser(req)`               | Confirm a user (bypass verification) |
| `getTokens()`                    | List all active tokens               |
| `expireTokens(req)`              | Expire tokens (optionally filtered)  |
| `getAuthEvents()`                | List auth events                     |

### `fc.rds`

| Method           | Description                              |
| ---------------- | ---------------------------------------- |
| `getInstances()` | List RDS instances with runtime metadata |

### `fc.elasticache`

| Method                   | Description                         |
| ------------------------ | ----------------------------------- |
| `getClusters()`          | List ElastiCache cache clusters     |
| `getReplicationGroups()` | List ElastiCache replication groups |
| `getServerlessCaches()`  | List ElastiCache serverless caches  |

### `fc.stepfunctions`

| Method            | Description                              |
| ----------------- | ---------------------------------------- |
| `getExecutions()` | List all state machine execution history |

### `fc.apigatewayv2`

| Method          | Description                         |
| --------------- | ----------------------------------- |
| `getRequests()` | List all HTTP API requests received |

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
