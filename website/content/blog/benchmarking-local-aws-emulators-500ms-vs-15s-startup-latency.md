+++
title = "Benchmarking Local AWS Emulators: 500ms vs 15s Startup Latency"
date = 2026-05-20
description = "A performance comparison between fakecloud and LocalStack, focusing on startup latency, memory footprint, and developer productivity."

[extra]
author = "Lucas Vieira"
+++

In modern software engineering, the inner loop — the cycle of coding, building, and testing — is the most critical factor in developer productivity. When your local environment relies on cloud-emulation tools that introduce multi-second latencies, that inner loop breaks. As of 2026-05-20, the landscape for local AWS development has shifted significantly. With major players moving toward mandatory accounts and auth tokens, the need for a high-fidelity, zero-friction binary has never been greater.

This benchmark analyzes the performance gap between fakecloud and the industry incumbent, LocalStack. We focus on two primary metrics: startup latency and binary footprint. These metrics are not just vanity numbers: they represent the difference between a fluid development experience and a fragmented one.

## The Cost of Slow Feedback Loops

Every time a developer runs a test suite, the environment must be ready. If the emulator takes 15 seconds to initialize, and a developer runs tests 40 times a day, they lose 10 minutes daily to pure overhead. Across a team of 50 engineers, this scales to over 40 hours of lost productivity every week. 

Beyond the time lost, there is a cognitive cost. A 15-second delay is long enough for a human brain to switch context, often leading to distractions like checking email or messaging apps. A sub-second startup, like the ~500ms provided by fakecloud, keeps the developer in the flow state.

```sh
# Start the entire fakecloud environment instantly
fakecloud start
```

## Performance Benchmark: fakecloud vs. LocalStack

The following data was captured on 2026-05-20 using a standard engineering workstation (Apple M3 Max, 64GB RAM). LocalStack was tested using its latest consolidated Docker image (v2026.04.0), while fakecloud was tested as a standalone native binary.

| Metric | fakecloud (v-next) | LocalStack (2026.04.0) |
| :--- | :--- | :--- |
| Startup Time | ~500 ms | 5 to 15 seconds |
| Binary/Image Size | ~19 MiB | 200 MiB to 1 GiB+ |
| Idle Memory Usage | ~10 MiB | 150 MiB to 400 MiB |
| Auth Required | No | Yes (Auth Token) |
| Internet Required | No | Yes (for initial auth) |
| Account Required | No | Yes |

## Architecture: Why the Gap Exists

The performance disparity is a result of fundamental architectural choices. LocalStack operates as a containerized environment, often wrapping various open-source tools (like Moto, LocalStack's own Python logic, and various sidecars) into a Docker image. Starting LocalStack requires the Docker daemon to pull or verify layers, initialize the container, start a Python runtime, and then boot the individual service mocks.

fakecloud takes a different approach. It is a single, statically linked binary written in a high-performance systems language. When you run `fakecloud start`, there is no container overhead. The binary maps the [2,422 operations](/blog/2-422-operations-how-fakecloud-reached-100-aws-api-conformance/) directly into memory. It does not wait for services to initialize because all 33+ services are active by default from the first millisecond.

### Memory Efficiency

In a CI/CD environment, memory is a finite resource. Running a 400 MiB container just to test a simple S3 upload is inefficient. fakecloud's 10 MiB idle footprint allows it to run on the smallest, most cost-effective CI runners without risk of Out-Of-Memory (OOM) errors. This efficiency enables developers to run a full AWS stack for every single feature branch or even every individual test file without slowing down the host machine.

## Service Conformance and Radical Accuracy

Speed is irrelevant if the emulator does not behave like the real AWS. As of 2026-05-20, fakecloud achieves 100% API conformance across 2,422 operations. This is not achieved through manual mocking, which is error-prone and difficult to maintain. Instead, fakecloud uses Smithy models — the same modeling language AWS uses to define its own APIs.

Every commit to the [fakecloud repository](https://github.com/faiscadev/fakecloud) is validated against 59,000+ generated test variants. This ensures that edge cases, such as specific error codes, header requirements, and XML/JSON serialization quirks, match the behavior of the real AWS production environment.

### Supported Services (Partial List)

*   **S3**: Full support for multipart uploads, bucket policies, and lifecycle configurations.
*   **Lambda**: Support for 23 runtimes with sub-second execution overhead.
*   **DynamoDB**: 100% conformance on TTL, Global Secondary Indexes (GSI), and atomic increments.
*   **SQS/SNS**: Real cross-service integration, including SNS fanout to multiple SQS queues.
*   **Bedrock**: 111 operations supported for local AI development and agent orchestration.

## AI Development with Local Bedrock

In 2026, AI-driven applications are the standard. Developing these applications usually requires a constant connection to AWS Bedrock, leading to high costs and latency during the development phase. fakecloud provides full [Bedrock support](/blog/bedrock-local-testing/) with 111 operations, allowing you to test your agentic workflows, prompt templates, and model invocations locally.

Because fakecloud requires no internet connection, you can develop AI features on a plane, in a secure facility, or in areas with poor connectivity. Your application calls the same Bedrock APIs it would in production, but the responses are served locally, instantly, and at zero cost.

## The "No" List: Eliminating Friction

Engineering tools should get out of the way. Most modern cloud emulators have introduced layers of friction that serve the vendor's business model rather than the developer's workflow. fakecloud is built on a philosophy of radical utility, which is best expressed by what you do not have to do:

1.  **No Account Required**: You do not need to sign up for a fakecloud account to use the tool.
2.  **No Auth Token**: There are no API keys or CI tokens to manage. Your CI secrets remain clean.
3.  **No Internet Connection**: fakecloud works entirely offline. It does not phone home or require a heartbeat to stay active.
4.  **No Docker Required for Core Services**: While Lambda and RDS can use Docker for high-fidelity execution, the core fakecloud binary and most services (S3, DynamoDB, SQS) run natively.
5.  **No Paid Tier for Local Dev**: The AGPL-3.0 license ensures that the tool remains free for local development and testing.

## Integration Testing with First-Party SDKs

One of the most difficult aspects of local AWS testing is asserting that an asynchronous event actually happened. For example, if a Lambda function is supposed to send an email via SES, how do you verify that in a test? 

fakecloud solves this with first-party SDKs for 6+ programming languages (Go, Python, TypeScript, Java, Rust, and .NET). These SDKs allow your tests to query the internal state of the emulator. 

```ts
import { FakecloudClient } from 'fakecloud';

const client = new FakecloudClient();
const sentEmails = await client.ses.getSentEmails();
expect(sentEmails).toContain(e => e.destination === 'user@example.com');
```

This capability transforms the emulator from a simple mock into a verifiable piece of infrastructure. You are no longer guessing if your code works: you are asserting on the actual side effects produced by the system.

## CI/CD Pipeline Efficiency

In a typical CI pipeline, the "setup" phase often takes longer than the actual tests. Pulling a 1 GiB Docker image for an AWS emulator can take 30 to 60 seconds depending on the network speed of the CI runner. 

With fakecloud, the 19 MiB binary can be cached or downloaded in under a second. Because it starts in ~500ms, your CI jobs begin executing tests almost immediately. This reduction in wall-clock time directly translates to faster PR feedback and lower CI costs.

### Example GitHub Action Snippet

```yaml
steps:
  - name: Install fakecloud
    run: curl -fsSL https://fakecloud.dev/install.sh | bash
  - name: Start fakecloud
    run: fakecloud start &
  - name: Run Tests
    run: npm test
```

## Engineering Pragmatism over Marketing Narrative

We do not use words like "seamless" or "effortless" because engineering is rarely either. Instead, we provide a binary that starts in 500ms and conforms to 2,422 operations. We provide a tool that respects your time by not requiring an account or an internet connection. We provide a high-fidelity environment that behaves like the real AWS so that your tests are meaningful.

The goal of fakecloud is to shrink the gap between your local machine and the cloud. By eliminating the latency of containers and the friction of mandatory authentication, we return those lost minutes to your day. 

To begin using the fastest local AWS environment available as of 2026-05-20, run the installation command and start the binary. No configuration flags are required: every service is ready when you are.

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud start
```