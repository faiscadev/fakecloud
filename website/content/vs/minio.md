+++
title = "fakecloud vs MinIO"
description = "How fakecloud compares to MinIO. MinIO is an S3-compatible object store; fakecloud is an AWS emulator with S3 plus 22 other services."
template = "page.html"
+++

MinIO is a high-performance S3-compatible object store. It's production-ready, scales, and is often deployed as real storage infrastructure.

fakecloud is a local AWS emulator for testing. Different tool for a different job.

## When to pick MinIO

- You want a **production-grade S3-compatible object store** (self-hosted alternative to S3, for real workloads).
- You need performance characteristics comparable to real S3 or better.
- You're deploying to on-premises or bare-metal and need S3 semantics at scale.
- Your need is S3, period — no other AWS services.

MinIO is excellent for these. It's not primarily a testing tool; it's real infrastructure.

## When to pick fakecloud

- You want **integration tests against AWS**, not production object storage.
- Your tests exercise S3 **plus** Lambda / SNS / SQS / DynamoDB / any other AWS service.
- You need S3 notifications to actually fire Lambda end-to-end (MinIO emits events but has no Lambda service).
- You want the full AWS API surface (S3 + IAM + STS + KMS + everything else) against one endpoint.
- You want a lightweight local-dev experience (~300ms startup, ~10 MiB idle memory) rather than a production storage daemon.

## Feature-level comparison

| | fakecloud | MinIO |
|---|---|---|
| S3 operations | 107 | Full S3 API |
| Production-grade storage | **No** (testing tool) | **Yes** |
| Distributed / clustered | No | Yes |
| S3 notifications | Yes | Yes |
| Notifications fire Lambda | **Yes** (real Lambda runs) | **No** (no Lambda service) |
| IAM + STS API | Yes | MinIO-specific IAM (not AWS IAM API) |
| Other AWS services (Lambda, DynamoDB, SQS, etc.) | **22 more** | **None** |
| Encryption via KMS | Yes (real AWS KMS emulation) | MinIO-specific KMS gateway |
| Startup | ~300ms | ~1-2s |
| Use case | Local integration testing | Real object storage |

## Using both

Some teams run MinIO as production storage **and** fakecloud for tests. This works:

- MinIO = real object store at cdn.example.com
- fakecloud = `http://localhost:4566` in tests, exercising S3 + Lambda + IAM + KMS together

They don't compete for the same job.

## Same S3 call works against both

```python
import boto3
s3 = boto3.client('s3',
    endpoint_url='http://localhost:4566',  # fakecloud for tests
    # OR endpoint_url='https://minio.example.com' for MinIO in prod
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1')
```

Both are S3-compatible.

## Links

- [fakecloud GitHub](https://github.com/faiscadev/fakecloud)
- [MinIO GitHub](https://github.com/minio/minio)
- [Local S3 for integration tests](/local-s3-for-tests/)
- [Fake AWS server for tests](/fake-aws-server/)
