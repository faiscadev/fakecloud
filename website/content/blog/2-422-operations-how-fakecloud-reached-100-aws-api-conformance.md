+++
title = "2,591 Operations: How fakecloud Reached 100% AWS API Conformance"
date = 2026-05-19
description = "How fakecloud uses the Smithy protocol and 86,000+ test variants to achieve 100% API conformance across 2,591 AWS operations in a zero-friction local environment."

[extra]
author = "Lucas Vieira"
+++

For senior developers and tooling engineers, the "local cloud" has historically been a minefield of broken promises. Traditional mocks are brittle, often failing to capture the nuanced error codes or side effects of real AWS infrastructure. Heavyweight emulators, meanwhile, have evolved into complex platforms that require accounts, auth tokens, and significant memory overhead just to run a simple integration test.

As of May 2026, the landscape of local AWS development has shifted. While incumbent tools have moved toward account-gated models and mandatory internet connectivity for license verification, [fakecloud](https://github.com/faiscadev/fakecloud) provides a high-fidelity, zero-friction alternative. By focusing on 100% API conformance across 2,591 operations, fakecloud ensures that your local environment behaves like infrastructure, not a simulation.

## The Credibility Hurdle: Why Developers Distrust Mocks

Most local AWS tools fall into the "mocking" trap. They intercept SDK calls and return hardcoded JSON responses. This works for happy-path unit tests but fails the moment your application relies on real cross-service integrations. 

If your SNS fanout doesn't actually trigger a Lambda, or if your S3 bucket policy doesn't actually block an unauthorized `PutObject` call, your local tests are lying to you. This "fidelity gap" leads to the dreaded "it worked on my machine" failure during the first deployment to a real staging environment.

fakecloud solves this by moving from mocking to **emulation**. Instead of guessing how a service should respond, fakecloud implements the actual logic of the AWS service, backed by the same models AWS uses to define its own APIs.

## The "No" List: Zero-Friction Development

Engineering pragmatism dictates that a tool should not be harder to manage than the problem it solves. Many local emulators now require a registered account and an active internet connection to function. fakecloud eliminates these hurdles entirely.

*   **No account required:** You never have to sign up or provide an email.
*   **No auth tokens:** There are no `LOCALSTACK_AUTH_TOKEN` environment variables to manage or rotate.
*   **No internet connection:** The binary is fully self-contained. You can develop on a plane, in a secure air-gapped environment, or during a network outage.
*   **No paid subscriptions:** The local development environment is open-source under the AGPL-3.0 license.
*   **No 1GB+ Docker images:** The entire fakecloud environment is delivered as a ~32 MB compressed download (~85 MB unpacked binary) with a sub-second startup time.

## 100% Conformance Across 2,591 Operations

Reliability in an emulator is measured by its API surface area and its conformance to the expected behavior of that surface. fakecloud currently supports 39 AWS services, covering the most critical components of modern cloud-native architectures.

| Service | Operations Supported | Key Features |
| :--- | :--- | :--- |
| **S3** | 107 | Multipart uploads, bucket policies, object tagging |
| **Lambda** | 85 | Cross-service triggers, environment variables, layers |
| **DynamoDB** | 57 | TTL, Global Secondary Indexes (GSI), Transactions |
| **SQS/SNS** | 65 | Dead-letter queues, SNS-to-SQS fanout, filtering |
| **Bedrock** | 106 | Foundation model invocation, listing models, tags |
| **IAM** | 176 | Policy evaluation, role assumption, credential validation |

Reaching 100% conformance across 2,591 operations is not a manual task. It is the result of a rigorous, model-driven engineering process.

## The Smithy Protocol: 86,000+ Test Variants

To ensure that fakecloud behaves exactly like the real AWS, we leverage **Smithy**, the interface definition language (IDL) used by AWS to build their own SDKs and services.

fakecloud uses official AWS Smithy models to generate a massive suite of test variants—86,327 in total, with 100% currently passing against the local binary.

### How the Test Suite Works

1.  **Model Extraction:** We pull the latest Smithy models for each supported AWS service.
2.  **Variant Generation:** For every operation (e.g., `PutItem` in DynamoDB), the test generator creates thousands of permutations, including edge cases like empty strings, maximum payload sizes, and invalid characters.
3.  **Dual Execution:** Each test variant is run against both the real AWS production environment and the local fakecloud binary.
4.  **Assertion:** The responses—including headers, status codes, and error messages—must match exactly. If AWS returns a `400 Bad Request` with a specific `SerializationException`, fakecloud must return the same.

This protocol ensures that when you use the official AWS SDK in your application, it cannot distinguish between fakecloud and the real cloud.

## Performance by the Numbers: ~32 MB and ~300ms

For a senior developer, the "inner loop" of development—code, test, repeat—must be as fast as possible. Waiting for a heavy Docker container to pull or a complex emulator to initialize is a productivity killer.

*   **Download Size:** ~32 MB compressed (~85 MB unpacked). Small enough for any CI cache and faster to pull than a multi-gigabyte Docker image.
*   **Startup Time:** ~300ms cold-start on a modern laptop. fakecloud is ready to accept requests before you can switch from your terminal to your IDE.
*   **Memory Footprint:** Minimal. Unlike Java-based or Python-based emulators that can consume gigabytes of RAM, fakecloud's Rust-based architecture is optimized for low-resource environments.

```sh
# Start fakecloud in the background
./fakecloud &

# Point your AWS CLI to the local endpoint
aws s3 mb s3://my-local-bucket --endpoint-url http://localhost:4566
```

## AI Development with Full Bedrock Support

In 2026, AI integration is no longer optional. Developing applications that use Amazon Bedrock typically requires a paid AWS account and incurs per-token costs even during the prototyping phase. 

fakecloud provides support for 106 Bedrock operations (plus the Bedrock Agent, Agent Runtime, and Runtime APIs). This allows you to build and test your AI orchestration logic—such as prompt chaining, model selection, and [guardrail integration](/blog/bedrock-guardrails-local/)—locally and for free. While fakecloud does not ship with the multi-gigabyte weights of frontier models, it provides the API structure necessary to test your application's interaction with the Bedrock service. You can learn more in our guide to [local Bedrock testing](/blog/bedrock-local-testing/).

## SDK-Client Separation: Testing with Confidence

One of the most powerful features of fakecloud is the separation between your application's SDK and the fakecloud testing SDK. 

Your application uses the standard AWS SDK (e.g., `aws-sdk-go-v2` or `boto3`). It doesn't know fakecloud exists; it just sees an endpoint. Meanwhile, your test suite uses the **fakecloud SDK** to perform "out-of-band" assertions. 

For example, you can use the fakecloud SDK to inspect the internal state of an SQS queue or to manually trigger a Lambda without going through the standard AWS API. This allows for deep integration testing that is impossible with simple mocks.

```go
// Your App Code
s3Client.PutObject(ctx, &s3.PutObjectInput{...})

// Your Test Code (using fakecloud SDK)
assertions.AssertBucketCount(t, "my-local-bucket", 1)
```

## Integrate fakecloud into Your CI Pipeline

Because fakecloud is a standalone binary with no external dependencies, integrating it into a CI/CD pipeline is trivial. There are no Docker-in-Docker complexities or auth secrets to configure.

1.  **Download the binary:** Use a simple `curl` or `wget` in your CI script.
2.  **Start the service:** Run `./fakecloud`.
3.  **Run your tests:** Point your test suite to `http://localhost:4566`.

This approach ensures that your CI environment is an exact replica of your local development environment, further reducing the risk of deployment-time surprises.

## The Future of Local Cloud Development

The trend toward "cloud-only" development is a response to the difficulty of building high-fidelity emulators. fakecloud proves that with a model-driven approach and a commitment to API conformance, the local environment can remain the most productive place for a developer to work.

By eliminating the friction of accounts, tokens, and costs, fakecloud returns control to the engineer. You can focus on building features and refining architecture, confident that your local tests are backed by 86,000+ Smithy-generated test variants and 100% conformance to the AWS API.

To get started, download the latest binary for your architecture and run the start command to begin developing against a local AWS environment that actually works.

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```