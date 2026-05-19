+++
title = "Integration testing AWS in GitHub Actions without mocks"
date = 2026-04-22
description = "Run real AWS integration tests in GitHub Actions, GitLab CI, and CircleCI with fakecloud. Copy-paste workflows for SQS, SNS, DynamoDB, Lambda, S3. No account, no auth token, no paid tier."

[extra]
author = "Lucas Vieira"
+++

Most "AWS integration tests" in CI are not integration tests. They are unit tests with a mocking library — moto, aws-sdk-client-mock, something similar — that intercepts the AWS SDK inside the test process and returns fabricated responses. Those tests pass when your code is wrong, because the mock only knows what you told it.

A real integration test talks to a real AWS-shaped service over HTTP. Testcontainers-LocalStack is the historical answer, but LocalStack's Community Edition went proprietary in March 2026 and now requires an account and an auth token just to pull the image.

This post shows how to run real AWS integration tests in CI against [fakecloud](https://github.com/faiscadev/fakecloud) — a free, open-source AWS emulator — with copy-paste configs for GitHub Actions, GitLab CI, and CircleCI.

## Why run fakecloud in CI

- ~500ms startup vs ~3s for a LocalStack container. Over hundreds of test runs, this matters.
- ~19 MB binary, ~10 MiB idle memory. Lightweight on shared runners.
- Single binary, no Docker required for most services (Docker still needed for Lambda, RDS, ElastiCache).
- No account, no auth token, no license key. `fakecloud` and you're running.
- Covers 23 services at 100% conformance per service, including Cognito, SES v2, RDS, ElastiCache, API Gateway v2, Bedrock — the ones LocalStack moved behind the paywall.

## GitHub Actions: install-and-run pattern

The fastest CI setup is to install the binary and background it:

```yaml
name: test
on: [push, pull_request]

jobs:
  integration:
    runs-on: ubuntu-latest
    env:
      AWS_ENDPOINT_URL: http://localhost:4566
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_REGION: us-east-1
    steps:
      - uses: actions/checkout@v4

      - name: Install fakecloud
        run: curl -fsSL https://fakecloud.dev/install.sh | bash

      - name: Start fakecloud
        run: fakecloud &

      - name: Wait for fakecloud
        run: |
          for i in $(seq 1 30); do
            curl -sf http://localhost:4566/_fakecloud/health && exit 0
            sleep 1
          done
          echo "fakecloud did not start"
          exit 1

      - uses: actions/setup-node@v4
        with:
          node-version: 20
      - run: npm ci
      - run: npm test
```

`AWS_ENDPOINT_URL` is respected by the AWS SDK v3 automatically — you do not need to configure your application code separately for CI vs local.

## GitHub Actions: service container pattern

If you prefer a Docker service container (longer startup, cleaner isolation):

```yaml
jobs:
  integration:
    runs-on: ubuntu-latest
    services:
      fakecloud:
        image: ghcr.io/faiscadev/fakecloud:latest
        ports:
          - 4566:4566
    env:
      AWS_ENDPOINT_URL: http://localhost:4566
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_REGION: us-east-1
    steps:
      - uses: actions/checkout@v4
      - run: npm ci
      - run: npm test
```

Use this if your test harness expects the service to be ready before the job's steps run (service containers start first).

## Lambda in GitHub Actions

Lambda needs Docker-in-Docker (fakecloud launches real Lambda runtime containers). On the default `ubuntu-latest` runner, Docker is already available — you just need to mount the socket:

```yaml
services:
  fakecloud:
    image: ghcr.io/faiscadev/fakecloud:latest
    ports:
      - 4566:4566
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
```

Or with the install-and-run pattern, Docker is available on the host directly — nothing extra needed. The fakecloud process launches Lambda containers on the host's Docker.

## GitLab CI

```yaml
stages:
  - test

integration:
  stage: test
  image: node:20
  services:
    - name: ghcr.io/faiscadev/fakecloud:latest
      alias: fakecloud
  variables:
    AWS_ENDPOINT_URL: http://fakecloud:4566
    AWS_ACCESS_KEY_ID: test
    AWS_SECRET_ACCESS_KEY: test
    AWS_REGION: us-east-1
  script:
    - npm ci
    - npm test
```

Note the endpoint uses the service alias (`fakecloud`), not `localhost`.

## CircleCI

```yaml
version: 2.1
jobs:
  integration:
    docker:
      - image: cimg/node:20.11
      - image: ghcr.io/faiscadev/fakecloud:latest
    environment:
      AWS_ENDPOINT_URL: http://localhost:4566
      AWS_ACCESS_KEY_ID: test
      AWS_SECRET_ACCESS_KEY: test
      AWS_REGION: us-east-1
    steps:
      - checkout
      - run: npm ci
      - run: npm test

workflows:
  build:
    jobs:
      - integration
```

CircleCI's secondary Docker images share the primary image's localhost, so `http://localhost:4566` works.

## Example test: SQS consumer integration

Your code under test:

```ts
// src/queue.ts
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";

export async function enqueue(body: string, queueUrl: string) {
  const sqs = new SQSClient({});
  await sqs.send(new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: body }));
}
```

Your integration test:

```ts
// test/queue.test.ts
import { SQSClient, CreateQueueCommand, ReceiveMessageCommand } from "@aws-sdk/client-sqs";
import { enqueue } from "../src/queue";

const sqs = new SQSClient({});

test("enqueue actually enqueues", async () => {
  const { QueueUrl } = await sqs.send(new CreateQueueCommand({ QueueName: "test-" + Date.now() }));
  await enqueue("hello", QueueUrl!);
  const res = await sqs.send(new ReceiveMessageCommand({ QueueUrl, WaitTimeSeconds: 1 }));
  expect(res.Messages?.[0]?.Body).toBe("hello");
});
```

No mocks. The test creates a real queue, enqueues a real message, receives a real message. If your code is wrong, the test fails the way a real-AWS test would fail.

## Asserting on fan-out and async flows

For flows that cross services (SQS -> Lambda -> SNS -> DynamoDB), use the fakecloud test-assertion SDK to inspect what each hop did:

```ts
import { FakeCloud } from "fakecloud";

const fc = new FakeCloud();

test("order publishes and lambda invokes", async () => {
  await placeOrder({ id: "o1" }); // your code under test

  const { messages } = await fc.sns.getPublishedMessages({ topicName: "orders" });
  expect(messages).toHaveLength(1);

  const { invocations } = await fc.lambda.getInvocations({ functionName: "on-order" });
  expect(invocations).toHaveLength(1);
  expect(invocations[0].statusCode).toBe(200);
});

afterEach(() => fc.reset());
```

SDK available in TypeScript, Python, Go, PHP, Java, Rust. Docs: [fakecloud.dev/docs/sdks](https://fakecloud.dev/docs/sdks).

## Tips

- **Reset between tests.** Either `await fc.reset()` in an `afterEach`, or use per-test resources with unique names (`test-${Date.now()}`). Reset is faster for large suites.
- **Run integration tests in a separate job** from unit tests. Unit tests should not need fakecloud; integration tests should not run alongside mocked ones.
- **Pin the image tag** in CI (`ghcr.io/faiscadev/fakecloud:0.10.1`) if you want deterministic behavior across builds. `latest` is fine for most teams.
- **Don't mix fakecloud with mocks.** Pick one per test. If you mock inside a test that also talks to fakecloud, you will get confusing failures.

## One call-out

**Performance benchmarking** is different from correctness testing. fakecloud and AWS have different performance characteristics (by design — fakecloud is a local process, AWS is a distributed global system). Benchmark performance against real AWS. Benchmark correctness and behavior against fakecloud.

fakecloud's goal is every AWS service at 100% conformance with 100% of cross-service integrations. If a service your tests hit isn't in the [supported list](https://github.com/faiscadev/fakecloud#supported-services) yet, [file an issue](https://github.com/faiscadev/fakecloud/issues) — the roadmap is demand-driven and services land one at a time with the full conformance target, not a partial-surface-first approach.

## Links

- Install: `curl -fsSL https://fakecloud.dev/install.sh | bash`
- Repo: [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- Migration from LocalStack: [Migrating from LocalStack to fakecloud](/blog/migrate-from-localstack/)
- Lambda tutorial: [How to test Lambda locally](/blog/test-lambda-locally/)
- Issues: [github.com/faiscadev/fakecloud/issues](https://github.com/faiscadev/fakecloud/issues)
