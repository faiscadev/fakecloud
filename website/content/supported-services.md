+++
title = "AWS Service Coverage & API Conformance"
description = "fakecloud provides 100% API conformance across 2,422 operations. Explore our supported AWS services for local development."
template = "page.html"
+++

fakecloud provides 100% API conformance across 2,422 operations. Unlike mocks, fakecloud is built against official AWS Smithy models to ensure wire-protocol compatibility and deterministic behavior for local development.

## Coverage Summary
- **Total Services**: 33+
- **Total Operations**: 2,422
- **Conformance Engine**: 59,000+ Smithy-based test cases
- **Startup Time**: ~500ms

## Supported Services

### Compute & Containers
- **Lambda**: Full execution environment, cross-service triggers (S3, SNS, SQS, EventBridge).
- **ECR**: 58 operations. Full OCI v2 Distribution protocol support for `docker push` and `docker pull`.

### Storage & Databases
- **S3**: 100% coverage including Bucket lifecycle, Object tagging, and Multipart uploads.
- **DynamoDB**: Support for TTL, GSI/LSI, and DynamoDB Streams.

### AI & Machine Learning
- **Bedrock**: 111 operations supported including Guardrails, Model Customization, and Provisioned Throughput.
- **Bedrock Runtime**: Deterministic `InvokeModel`, `InvokeModelWithResponseStream`, and `Converse` APIs.

### Messaging & Integration
- **SQS**: Standard and FIFO queues, Dead Letter Queues (DLQ).
- **SNS**: Topic management and fan-out to SQS/Lambda.
- **EventBridge**: Rules, Targets, and EventBridge Scheduler support.

### Security & Management
- **IAM**: Policy evaluation and role simulation.
- **STS**: Local token generation and session management.
- **SSM**: Parameter Store and Secrets Manager integration.

## Technical Conformance Data
fakecloud is validated against the same Smithy models used by the official AWS SDKs. This ensures that every request and response matches the expected wire format exactly, eliminating 'works on my machine' bugs caused by shallow mocks.

[View the full machine-readable API manifest on GitHub](https://github.com/faiscadev/fakecloud)