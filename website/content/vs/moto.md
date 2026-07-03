+++
title = "fakecloud vs Moto"
description = "How fakecloud differs from Moto. Architectural split (HTTP server vs in-process Python library), language support, cross-service wiring, Lambda execution."
template = "page.html"
+++

Moto is the longest-lived open-source AWS mocking project. Python library that patches `boto3` inside a test process. Great at what it does.

fakecloud is a different tool for a different problem.

## The architectural split

**Moto** is an **in-process Python library**. You import it, decorate your test, it monkey-patches `boto3` SDK calls inside your test process. Very fast, zero external dependencies, trivial setup for Python. Does not speak the AWS wire protocol — it intercepts SDK method calls directly.

**fakecloud** is a **real HTTP server** speaking the AWS wire protocol on port 4566. Your code uses the regular AWS SDK with `endpoint_url` set to `http://localhost:4566`. Any AWS SDK in any language works. Cross-service wiring runs server-side.

## What that means practically

| | fakecloud | Moto |
|---|---|---|
| Languages | Any (Python, Node, Go, Java, Kotlin, Rust, PHP, more) | Python only |
| Works with Terraform | Yes | **No** (Moto is in-process, Terraform runs as a separate binary) |
| Works with CDK | Yes | **No** (same reason) |
| Works with AWS CLI | Yes | **No** |
| Real Lambda execution | Yes (23 runtimes, real containers) | **No** (stubbed responses) |
| Real RDS databases | Yes (PostgreSQL/MySQL/MariaDB via Docker) | **No** (in-memory stubs) |
| Real ElastiCache | Yes (Redis/Valkey/Memcached via Docker) | **No** |
| Real `docker push`/`pull` to ECR | Yes (OCI v2 Distribution) | **No** (Moto ships the JSON control plane only) |
| Real cross-service wiring | Yes (S3 -> Lambda, SNS fan-out, etc fire end-to-end) | Partial (Moto simulates some events, not real execution) |
| Setup for Python unit tests | HTTP endpoint override + fakecloud running | `@mock_aws` decorator |
| Service count | 47 at true 100% conformance (depth-first) | 100+ at varying depth (breadth-first) |
| Conformance methodology | Smithy-validated, 148,218/148,218 test variants pass on every commit | Not published |
| License | AGPL-3.0 | Apache-2.0 |

## When Moto is the right pick

- **Python-only codebase.**
- **Unit tests** where you just need boto3 to respond with plausible shapes.
- **Very fast test suite** where per-test process-isolated mocking is a feature.
- **You don't exercise cross-service flows** (no Lambda execution, no real DB).

Moto is excellent at this. Years of maturity, huge contributor base.

## When fakecloud is the right pick

- **Non-Python codebase** (Go, Node, Java, Kotlin, Rust, PHP).
- **Integration tests** that exercise real cross-service wiring (S3 -> Lambda, EventBridge -> Step Functions, SES inbound -> S3/SNS/Lambda).
- **Tests against Terraform, CDK, or IaC tooling** — these need a real HTTP endpoint.
- **Tests that need Lambda to actually execute** your function code.
- **Tests that need real stateful backends** (Postgres schema, Redis data structures).

## Complementary, not competitive

Lots of Python teams use both: Moto for fast-iterating unit tests inside Python, fakecloud for integration tests that span services or involve Terraform/CDK/other-language components.

## Example: the same test, both ways

**With Moto (Python-only):**

```python
from moto import mock_aws
import boto3

@mock_aws
def test_put_and_get():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test')
    s3.put_object(Bucket='test', Key='k', Body=b'v')
    assert s3.get_object(Bucket='test', Key='k')['Body'].read() == b'v'
```

**With fakecloud (any language):**

```python
import boto3
import os
os.environ['AWS_ENDPOINT_URL'] = 'http://localhost:4566'
os.environ['AWS_ACCESS_KEY_ID'] = 'test'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'

def test_put_and_get():
    s3 = boto3.client('s3')
    s3.create_bucket(Bucket='test')
    s3.put_object(Bucket='test', Key='k', Body=b'v')
    assert s3.get_object(Bucket='test', Key='k')['Body'].read() == b'v'
```

Same test. Moto needs Python + the decorator. fakecloud needs the emulator running; the test itself is just plain boto3.

## Install fakecloud

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Moto equivalent for Go/Java/Node:** [/blog/moto-equivalent-go-java-node/](/blog/moto-equivalent-go-java-node/)
- **Moto:** [github.com/getmoto/moto](https://github.com/getmoto/moto)
