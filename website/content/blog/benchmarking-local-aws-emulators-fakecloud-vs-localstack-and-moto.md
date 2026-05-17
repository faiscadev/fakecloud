+++
title = "Benchmarking Local AWS Emulators: fakecloud vs LocalStack and Moto"
date = 2026-05-17
description = "A performance and conformance comparison between fakecloud, LocalStack, and Moto for local AWS development as of May 2026."

[extra]
author = "Lucas Vieira"
+++

In modern backend engineering, the "inner loop"—the time between writing a line of code and seeing it run—is the primary bottleneck for developer velocity. When your application depends on AWS services like S3, Lambda, or DynamoDB, that loop often breaks. You are forced to choose between slow, expensive deployments to a sandbox account or local emulators that vary wildly in fidelity and overhead.

As of May 13, 2026, the landscape for local AWS emulation has shifted. The incumbent, LocalStack, transitioned its Community Edition to a proprietary model in March 2026, now requiring an account and authentication token for all users. This change has introduced friction into CI/CD pipelines and local environments that previously relied on zero-config Docker pulls. 

This benchmark evaluates [fakecloud](https://github.com/faiscadev/fakecloud) against LocalStack and Moto, focusing on the metrics that matter to engineers: startup latency, binary overhead, and API conformance.

## The Dev Loop Hurdle: Why Latency and Friction Matter

Every second added to a test suite or a local environment start time is a tax on focus. If your local AWS environment takes 10 seconds to start, you stop running tests after every change. If it requires an auth token or an internet connection to verify a license, your workflow is tethered to external systems. 

[fakecloud](https://github.com/faiscadev/fakecloud) is built on a "depth-first" philosophy. Instead of offering partial coverage for hundreds of services, it provides 100% API conformance for 33 core AWS services. It does this through a single, 19MB static binary that starts in approximately 500ms. 

### Performance by the Numbers

The following table compares the resource requirements and startup performance of the leading local AWS emulators as of May 13, 2026.

| Metric | fakecloud | LocalStack (Hobby/Pro) | Moto (Server Mode) |
| :--- | :--- | :--- | :--- |
| **Binary/Image Size** | ~19 MB (Static Binary) | ~1.2 GB (Docker Image) | ~50 MB (Python Env) |
| **Startup Time** | ~500 ms | 5–15 seconds | 1–3 seconds |
| **Idle Memory** | ~10 MiB | 400+ MiB | ~80 MiB |
| **Auth Required** | No | Yes (as of March 2026) | No |
| **Internet Required** | No | Yes (for initial auth) | No |
| **License** | AGPL-3.0 | Proprietary / Tiered | Apache 2.0 |

## API Conformance: 2,422 Operations and 59,000 Smithy Tests

An emulator is only as good as its fidelity to the real AWS API. If `PutItem` works but `ConditionExpression` behaves differently than it does in us-east-1, your local tests are lying to you. 

[fakecloud](https://github.com/faiscadev/fakecloud) achieves 100% conformance across its 33 supported services by using AWS's own Smithy models. On every commit, the fakecloud engine is validated against 59,000+ generated test variants. This ensures that field presence, waiter behavior, and error codes match the official AWS specification exactly.

### Supported Services and Operations

As of May 13, 2026, [fakecloud](https://github.com/faiscadev/fakecloud) supports 2,422 operations across 33 services, including:

*   **Compute:** Lambda (23 runtimes), ECS, ECR (OCI v2 compatible).
*   **Storage:** S3 (including multipart uploads and lifecycle policies), EBS.
*   **Database:** DynamoDB (full expression support), RDS (Postgres, MySQL, MariaDB, Oracle, SQL Server), ElastiCache (Redis, Valkey, Memcached).
*   **Messaging:** SQS, SNS (with fan-out and filter policies), EventBridge.
*   **AI/ML:** Bedrock and Bedrock Runtime (111 operations).
*   **Security:** IAM, STS, Secrets Manager, KMS, WAF v2.
*   **Networking:** API Gateway v1/v2, ELBv2 (ALB/NLB), Route 53, CloudFront.

In contrast, Moto often relies on manual mocks that may miss edge cases in the AWS wire protocol. LocalStack provides a broader catalog but gates critical features—like IAM enforcement, RDS engine variety, and advanced Bedrock operations—behind paid tiers.

## Zero-Auth Workflow: Eliminating the Token Tax

Since the March 2026 update to LocalStack, developers must manage `LOCALSTACK_AUTH_TOKEN` in their environments. This introduces a new failure mode: expired tokens or failed auth checks breaking CI pipelines. 

[fakecloud](https://github.com/faiscadev/fakecloud) requires no account, no token, and no internet connection. You run the binary, and it listens on port 4566. 

### Comparative Setup Steps

**Starting fakecloud:**
1. Download the binary.
2. Run `./fakecloud`.

**Starting LocalStack (as of 2026):**
1. Create an account at app.localstack.cloud.
2. Generate an auth token.
3. Export `LOCALSTACK_AUTH_TOKEN` in your shell.
4. Run `docker pull` (requires auth).
5. Run `localstack start`.

**Using fakecloud with the AWS CLI:**

```sh
# No configuration needed beyond the endpoint-url
aws --endpoint-url http://localhost:4566 s3 mb s3://my-local-bucket
```

## Real Infrastructure for Stateful Services

One of the primary weaknesses of traditional mocks is the lack of a real stateful backend. If you are testing a complex SQL query against a mocked RDS, you aren't testing the query; you're testing the mock's regular expression parser. 

[fakecloud](https://github.com/faiscadev/fakecloud) takes a different approach for stateful services. When you request an RDS instance, fakecloud manages a real instance of PostgreSQL, MySQL, or MariaDB. When you use ElastiCache, it spins up a real Valkey or Redis process. This ensures that your application's interaction with the data layer is 100% authentic.

## AI Development: Full Bedrock Support

For teams building AI-native applications, local emulation of LLM providers is critical. As of May 13, 2026, [fakecloud](https://github.com/faiscadev/fakecloud) provides the most comprehensive local Bedrock implementation available. 

While LocalStack's Ultimate tier supports 4 Bedrock operations backed by Ollama, [fakecloud](https://github.com/faiscadev/fakecloud) supports the full surface of 111 operations. This includes not just `InvokeModel` and `Converse` (with streaming), but also the full control plane: guardrails, custom model jobs, and prompt management. 

### Bedrock Local Testing Example

```sh
# Invoke a local model with fakecloud
aws bedrock-runtime invoke-model \
    --endpoint-url http://localhost:4566 \
    --model-id anthropic.claude-3-sonnet-20240229-v1:0 \
    --body '{"prompt": "Hello", "max_tokens": 10}' \
    output.json
```

Because [fakecloud](https://github.com/faiscadev/fakecloud) implements the full API shape, your AI coding agents (like Cursor or GitHub Copilot) can write and test Bedrock-integrated code locally without incurring costs or hitting rate limits.

## SDK-Client Separation: Asserting on Side Effects

In a standard integration test, you use the AWS SDK to trigger an action (e.g., uploading a file to S3). But how do you verify that an SNS message was sent or an email was dispatched via SES without writing complex polling logic? 

[fakecloud](https://github.com/faiscadev/fakecloud) provides first-party SDKs for TypeScript, Python, Go, PHP, Java, and Rust. These SDKs allow your tests to "reach into" the emulator and assert on state that isn't visible through the standard AWS API.

### Example: Asserting on SES Emails in TypeScript

```ts
import { FakeCloud } from "fakecloud";
const fc = new FakeCloud();

// Your application code uses the standard AWS SDK
await myApp.registerUser("dev@example.com");

// Your test uses the fakecloud SDK to verify the side effect
const { emails } = await fc.ses.getEmails();
expect(emails).toHaveLength(1);
expect(emails[0].subject).toBe("Welcome!");

await fc.reset();
```

This separation ensures that your application code remains clean and unaware of the testing environment, while your tests gain deep visibility into the infrastructure's behavior.

## Cross-Service Integrations

[fakecloud](https://github.com/faiscadev/fakecloud) is not a collection of isolated mocks. It is a wired environment. It supports 30+ service-to-service integrations that execute end-to-end:

*   **S3 to Lambda:** Uploading a file triggers a Lambda function.
*   **SNS to SQS:** Messages published to a topic are fanned out to subscribed queues.
*   **EventBridge to Step Functions:** Events trigger state machine executions.
*   **Cognito to Lambda:** User signup triggers a post-confirmation Lambda.

These flows are validated against the official AWS behavior, ensuring that the event payloads and retry logic match production exactly.

## Conclusion: The High-Fidelity Choice

The transition of LocalStack to a proprietary, account-based model in early 2026 has forced a re-evaluation of local development tools. For engineers who prioritize a zero-friction, high-performance "inner loop," the choice is clear. 

Moto remains a viable option for simple Python unit tests that don't require a real HTTP server. LocalStack remains the choice for teams that need its massive breadth of 100+ services and are willing to pay the "token tax" and subscription fees. 

However, for the 33 core services that power the vast majority of cloud-native applications, [fakecloud](https://github.com/faiscadev/fakecloud) provides a faster, smaller, and more conformant alternative. Its ~500ms startup time and 19MB binary footprint make it the ideal choice for both local development and high-speed CI/CD pipelines.

To verify these benchmarks in your own environment, run the following command to install the fakecloud binary and start your first local AWS session:

```sh
curl -fsSL https://raw.githubusercontent.com/faiscadev/fakecloud/main/install.sh | bash && fakecloud
```