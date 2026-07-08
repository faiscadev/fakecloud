+++
title = "fakecloud vs ElasticMQ"
description = "How fakecloud compares to ElasticMQ. Both provide local SQS; fakecloud adds SNS fan-out, Lambda event source mappings, and 21 other AWS services."
template = "page.html"
+++

[ElasticMQ](https://github.com/softwaremill/elasticmq) is a message queue server with an Amazon SQS-compatible interface. Scala-based, focused, battle-tested. Good at what it does.

fakecloud's SQS is one of 98 services and ties into the rest (SNS fan-out, Lambda event source mappings, DLQ to other services, IAM policy enforcement).

## When to pick ElasticMQ

- You need SQS and nothing else from AWS.
- You're on a JVM-heavy stack and already run Scala or Java services.
- You want production-grade message queue semantics at scale (ElasticMQ is deployable as real infrastructure).
- You want a single, focused tool rather than a multi-service emulator.

## When to pick fakecloud

- Your tests exercise **SQS + Lambda** — ElasticMQ doesn't have Lambda; your Lambda event source mappings have nowhere to go.
- Your tests exercise **SNS -> SQS fan-out** — ElasticMQ has no SNS service.
- Your tests exercise **SQS DLQ + other services** — the DLQ receiver might be a Lambda or an SNS notification.
- Multi-language codebase — any AWS SDK works against fakecloud's HTTP endpoint.

## Feature-level comparison

| | fakecloud | ElasticMQ |
|---|---|---|
| SQS operations | 23 | Full SQS API |
| FIFO queues | Yes | Yes |
| DLQ | Yes | Yes |
| Long polling | Yes | Yes |
| SNS -> SQS fan-out | **Yes** | **No** (no SNS service) |
| SQS -> Lambda event source mapping | **Yes** (Lambda runs for real) | **No** (no Lambda service) |
| IAM policy enforcement on SQS | Yes (opt-in `--iam strict`) | No |
| Other AWS services | 22 more | None |
| Runtime | Rust binary (~19 MB) | Scala/JVM |
| Startup | ~300ms | ~2-3s (JVM) |

## Same SQS call works against both

```python
sqs = boto3.client('sqs',
    endpoint_url='http://localhost:4566',  # fakecloud
    # OR 'http://localhost:9324' for ElasticMQ default port
    ...)
```

Both implement the SQS wire protocol.

## Links

- [fakecloud GitHub](https://github.com/faiscadev/fakecloud)
- [ElasticMQ GitHub](https://github.com/softwaremill/elasticmq)
- [SQS emulator](/sqs-emulator/)
