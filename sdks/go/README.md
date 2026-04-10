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

### SES - `fc.SES()`

| Method | Description |
|--------|-------------|
| `GetEmails(ctx)` | List all sent emails |
| `SimulateInbound(ctx, req)` | Simulate an inbound email |

### SNS - `fc.SNS()`

| Method | Description |
|--------|-------------|
| `GetMessages(ctx)` | List published messages |
| `GetPendingConfirmations(ctx)` | List pending subscription confirmations |
| `ConfirmSubscription(ctx, req)` | Confirm a subscription |

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

### S3 - `fc.S3()`

| Method | Description |
|--------|-------------|
| `GetNotifications(ctx)` | List notification events |
| `TickLifecycle(ctx)` | Tick the lifecycle processor |

### Lambda - `fc.Lambda()`

| Method | Description |
|--------|-------------|
| `GetInvocations(ctx)` | List recorded invocations |
| `GetWarmContainers(ctx)` | List warm containers |
| `EvictContainer(ctx, functionName)` | Evict a warm container |

### DynamoDB - `fc.DynamoDB()`

| Method | Description |
|--------|-------------|
| `TickTTL(ctx)` | Tick the TTL processor |

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

### RDS - `fc.RDS()`

| Method | Description |
|--------|-------------|
| `GetInstances(ctx)` | List RDS instances with runtime metadata |

### ElastiCache - `fc.ElastiCache()`

| Method | Description |
|--------|-------------|
| `GetClusters(ctx)` | List cache clusters |
| `GetReplicationGroups(ctx)` | List replication groups |
| `GetServerlessCaches(ctx)` | List serverless caches |

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
