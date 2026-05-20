+++
title = "fakecloud vs DynamoDB Local"
description = "How fakecloud compares to DynamoDB Local. Same DynamoDB behavior, plus cross-service triggers, Lambda execution, and the 22 other AWS services around it."
template = "page.html"
+++

DynamoDB Local is AWS's official local DynamoDB: a downloadable JAR (or Docker image) that runs DynamoDB for offline development. Good at what it does.

fakecloud is a broader tool that happens to include DynamoDB.

## When to pick DynamoDB Local

- You only need DynamoDB, nothing else.
- Your app talks to DynamoDB and nothing else from AWS.
- You're on a JVM-heavy stack and prefer AWS's official tooling.

DynamoDB Local is focused and battle-tested. For pure DynamoDB tests, nothing wrong with it.

## When to pick fakecloud

- Your tests exercise **DynamoDB + something else** (Lambda reading via Streams, SNS publishing when items change, S3 notification triggering Lambda that writes to DynamoDB).
- You want **DynamoDB Streams -> Lambda** to actually fire end-to-end (DynamoDB Local emits stream records; it doesn't have Lambda to consume them).
- You want the same test harness to cover multiple services without running multiple emulators.
- You want test-assertion SDKs for the wider AWS surface (what emails got sent, what SNS messages got published, what Lambda invocations fired).

## Feature-level comparison

| | fakecloud | DynamoDB Local |
|---|---|---|
| DynamoDB operations | 57 | Full (100%) |
| DynamoDB Streams | Yes | Yes |
| PartiQL | Yes | Yes |
| Transactions | Yes | Yes |
| Global tables | Yes | Yes (limited) |
| Lambda consumes Streams (real) | **Yes** | **No** (no Lambda service) |
| S3 writes trigger DynamoDB updates via Lambda | **Yes** | **No** |
| Other AWS services available | 22 more | None |
| Runtime | Single Rust binary (~19 MB) | Java JAR or Docker image |
| Startup | ~300ms | ~2s |
| Install size | ~19 MB | ~60 MB JAR + JVM |

## Same DynamoDB call works against both

```python
import boto3
ddb = boto3.client('dynamodb', endpoint_url='http://localhost:4566')  # fakecloud
# OR
ddb = boto3.client('dynamodb', endpoint_url='http://localhost:8000')  # DynamoDB Local
```

If you already test against DynamoDB Local and need nothing more, no reason to switch. If you're expanding tests to cover downstream services, fakecloud replaces DynamoDB Local without requiring a second emulator process.

## Links

- [fakecloud GitHub](https://github.com/faiscadev/fakecloud)
- [DynamoDB Local docs](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html)
- [DynamoDB emulator](/dynamodb-emulator/)
