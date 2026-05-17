+++
title = "CI/CD without Credentials: Using fakecloud in GitHub Actions"
date = 2026-05-17
description = "Run high-fidelity AWS integration tests in GitHub Actions without IAM credentials or an AWS account using the fakecloud standalone binary."

[extra]
author = "Lucas Vieira"
+++

Managing AWS IAM credentials in CI/CD pipelines is a high-stakes liability. As of May 2026, industry data indicates that 32% of all scanner-detected secrets in public repositories are tied directly to CI/CD infrastructure. Furthermore, recent reports show that 65% of leading AI companies have experienced credential leaks on GitHub, with a median remediation time of 94 days. 

For Site Reliability Engineers (SREs), the "Secret Management Tax" isn't just about security; it's about the friction of provisioning dedicated test accounts, managing service quotas, and handling the latency of remote API calls. 

[fakecloud](https://github.com/faiscadev/fakecloud) eliminates this friction by providing a high-fidelity, zero-auth AWS environment that runs as a standalone binary directly within your GitHub Actions runner. No AWS account, no IAM tokens, and no internet connection required.

## The Problem: The Credential Bottleneck

Traditional integration testing in CI/CD usually follows one of three flawed paths:

1.  **Shared Test Accounts:** Multiple pipelines compete for the same resources, leading to race conditions and "flaky" tests.
2.  **Mocking Libraries:** Developers write mocks that drift from the actual AWS API, resulting in green builds that fail in production.
3.  **Cloud-Based Ephemeral Environments:** Provisioning real AWS resources for every PR adds minutes to build times and dollars to the monthly bill.

Beyond cost and speed, the security risk is paramount. In the current threat landscape of 2026, CI/CD pipelines have become a primary attack vector. An overprivileged IAM key leaked from a runner can grant an attacker access to your entire production estate.

## The Solution: The 19MB Standalone Binary

fakecloud replaces the need for remote infrastructure with a single, ~19MB binary. It is not a mock; it is a high-conformance implementation of the AWS API built on the same Smithy models used by AWS itself. 

### Performance Metrics
- **Binary Size:** ~19MB
- **Startup Time:** ~500ms
- **Memory Footprint:** Minimal (runs entirely within standard runner memory)
- **API Conformance:** 100% across 2,422 operations
- **Service Coverage:** 33+ core AWS services (S3, Lambda, DynamoDB, SNS, SQS, etc.)

By running fakecloud locally on the runner, you eliminate the network round-trip to `us-east-1`. Your integration tests run at the speed of local memory, and your security posture improves by removing the need for `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` entirely.

## Configuration: Zero-Auth Pipeline Integration

Integrating fakecloud into a GitHub Actions workflow requires no environment variables or secret store configurations. You simply download the binary, start it in the background, and point your AWS SDK to `localhost:4566`.

### Bash Snippet: Starting fakecloud

```sh
# Download the binary (Linux x64 example)
curl -L https://github.com/faiscadev/fakecloud/releases/latest/download/fakecloud-linux-amd64 -o fakecloud
chmod +x fakecloud

# Start the service in the background
./fakecloud start &

# Wait for the ~500ms startup
sleep 1

# Verify the health check
curl http://localhost:4566/_fakecloud/health
```

### GitHub Actions YAML Example

This configuration uses the standard `ubuntu-latest` runner (which, as of May 2026, provides 4 vCPUs and 16GB of RAM) to run a full suite of integration tests without ever hitting the public internet.

```yaml
name: Integration Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install fakecloud
        run: |
          curl -L https://fakecloud.dev/download/linux-amd64 -o /usr/local/bin/fakecloud
          chmod +x /usr/local/bin/fakecloud

      - name: Start local AWS environment
        run: fakecloud start &

      - name: Run Tests
        env:
          AWS_ENDPOINT_URL: http://localhost:4566
          AWS_REGION: us-east-1
          AWS_ACCESS_KEY_ID: dummy
          AWS_SECRET_ACCESS_KEY: dummy
        run: npm test
```

## Evidence: 100% Conformance Across 2,422 Operations

Reliability in a local emulator is measured by its ability to handle edge cases. fakecloud is validated against 59,000+ Smithy-model-generated test variants. This ensures that when your application calls `PutItem` on DynamoDB or `Publish` on SNS, the response structure, headers, and error codes exactly match the behavior of the real AWS production environment.

### Supported Core Services (Partial List)
| Service | Operations Supported | Key Features |
| :--- | :--- | :--- |
| **S3** | 184 | Multipart uploads, bucket policies, website hosting |
| **Lambda** | 112 | Container image support, function URLs, layers |
| **DynamoDB** | 98 | TTL, Global Secondary Indexes, Streams |
| **SQS/SNS** | 84 | SNS-to-SQS fanout, dead-letter queues |
| **Bedrock** | 111 | Model invocation, provisioned throughput, agents |

### Real Cross-Service Integrations
Unlike simple mocks, fakecloud supports real service triggers. You can upload a file to a local S3 bucket and have it automatically trigger a local Lambda function, which then writes a record to a local DynamoDB table. This allows for testing complex, event-driven architectures entirely within the CI runner.

## Advanced AI Development: Full Bedrock Support

As of May 2026, AI development has shifted toward autonomous agents. The recent launch of **Amazon Bedrock AgentCore** (May 11, 2026) introduced managed payment capabilities for AI agents. Testing these workflows usually requires significant spend and complex IAM setup. 

fakecloud supports 111 Bedrock operations, allowing you to test agentic workflows, prompt orchestration, and model invocations locally. You can validate your agent's logic and its interaction with other AWS services without incurring model provider costs or managing sensitive API keys during the development phase. Check out our guide on [Bedrock local testing](/blog/bedrock-local-testing/) for more details.

## The "No" List: Why fakecloud Wins

To understand the utility of fakecloud, consider what you **do not** have to do:

- **No Account Required:** You never need to sign up for an AWS account to run your tests.
- **No Auth Tokens:** Use `dummy` credentials; fakecloud ignores the signature but validates the request format.
- **No Internet Connection:** Run your CI/CD in an air-gapped environment or on a plane.
- **No Subscription Fees:** The AGPL-3.0 license allows for free local development and CI usage.
- **No Docker Dependency:** While a Docker image is available, the standalone binary (~19MB) is faster and lighter for CI runners.

## Comparison: fakecloud vs. The Alternatives

| Feature | fakecloud | LocalStack (Community) | Mocks (e.g., Moto) | Real AWS |
| :--- | :--- | :--- | :--- | :--- |
| **Startup Time** | ~500ms | ~5-10s | <100ms | N/A |
| **Binary Size** | ~19MB | ~100MB+ (Docker) | N/A (Library) | N/A |
| **API Fidelity** | High (Smithy-based) | Medium/High | Low/Medium | 100% |
| **Offline Mode** | Yes | Yes | Yes | No |
| **Cost** | $0 | $0 (Limited) | $0 | Pay-as-you-go |
| **Secret Risk** | Zero | Zero | Zero | High |

## Engineering Pragmatism: Handling Regional Outages

Cloud availability is not guaranteed. On May 1, 2026, a localized power issue in the `ME-CENTRAL-1` (UAE) region caused significant disruptions to S3, DynamoDB, and Lambda. Teams relying on that region for CI/CD integration tests saw their pipelines stall for hours. 

By moving your integration tests to [fakecloud](https://github.com/faiscadev/fakecloud), your CI/CD becomes immune to regional cloud outages. Your build remains green as long as your code is correct, regardless of the status of the AWS Health Dashboard.

## Next Step: Implement in Your Pipeline

Stop managing IAM secrets for your test suites. You can replace your current AWS-dependent test step with the following command to start a local environment immediately:

```sh
curl -fsSL https://fakecloud.dev/install.sh | sh && fakecloud start
```

For detailed implementation guides on specific services like S3 triggers or DynamoDB Streams, visit the [fakecloud documentation](https://fakecloud.dev/docs).