# fakecloud

Python SDK for [fakecloud](https://github.com/faiscadev/fakecloud) — a local AWS cloud emulator.

This package provides async and sync clients for the fakecloud introspection and simulation API (`/_fakecloud/*` endpoints), letting you inspect sent emails, published messages, Lambda invocations, and more from your tests.

## Installation

```bash
pip install fakecloud
```

## Quick start

### Async

```python
import asyncio
from fakecloud import FakeCloud

async def main():
    async with FakeCloud("http://localhost:4566") as fc:
        # Check server health
        health = await fc.health()
        print(health.status, health.version)

        # List sent SES emails
        emails = await fc.ses.get_emails()
        for email in emails.emails:
            print(f"{email.from_addr} -> {email.to}: {email.subject}")

        # List SNS messages
        messages = await fc.sns.get_messages()
        for msg in messages.messages:
            print(f"{msg.topic_arn}: {msg.message}")

        # Inspect Lambda invocations
        invocations = await fc.lambda_.get_invocations()
        for inv in invocations.invocations:
            print(f"{inv.function_arn}: {inv.payload}")

        # Reset all state between tests
        await fc.reset()

asyncio.run(main())
```

### Sync

```python
from fakecloud import FakeCloudSync

with FakeCloudSync("http://localhost:4566") as fc:
    health = fc.health()
    print(health.status)

    emails = fc.ses.get_emails()
    for email in emails.emails:
        print(email.subject)
```

## API reference

### `FakeCloud` / `FakeCloudSync`

Top-level client. Pass `base_url` (default `http://localhost:4566`).

| Method | Description |
|---|---|
| `health()` | Server health check |
| `reset()` | Reset all service state |
| `reset_service(service)` | Reset a single service |

### Service sub-clients

Access via properties on the main client:

| Property | Service | Methods |
|---|---|---|
| `lambda_` | Lambda | `get_invocations()`, `get_warm_containers()`, `evict_container(name)` |
| `ses` | SES | `get_emails()`, `simulate_inbound(req)` |
| `sns` | SNS | `get_messages()`, `get_pending_confirmations()`, `confirm_subscription(req)` |
| `sqs` | SQS | `get_messages()`, `tick_expiration()`, `force_dlq(queue_name)` |
| `events` | EventBridge | `get_history()`, `fire_rule(req)` |
| `s3` | S3 | `get_notifications()`, `tick_lifecycle()` |
| `dynamodb` | DynamoDB | `tick_ttl()` |
| `secretsmanager` | SecretsManager | `tick_rotation()` |
| `cognito` | Cognito | `get_user_codes(pool_id, username)`, `get_confirmation_codes()`, `confirm_user(req)`, `get_tokens()`, `expire_tokens(req)`, `get_auth_events()` |
| `rds` | RDS | `get_instances()` |
| `elasticache` | ElastiCache | `get_clusters()`, `get_replication_groups()`, `get_serverless_caches()` |

### Error handling

All methods raise `FakeCloudError` on non-2xx responses:

```python
from fakecloud.client import FakeCloudError

try:
    await fc.health()
except FakeCloudError as e:
    print(e.status, e.body)
```

## pytest fixture example

```python
import pytest
from fakecloud import FakeCloudSync

@pytest.fixture(autouse=True)
def reset_fakecloud():
    fc = FakeCloudSync()
    fc.reset()
    yield fc
    fc.close()

def test_email_sent(reset_fakecloud):
    # ... your code that sends an email via SES ...
    emails = reset_fakecloud.ses.get_emails()
    assert len(emails.emails) == 1
    assert emails.emails[0].subject == "Welcome"
```
