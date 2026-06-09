+++
title = "AWS Service Coverage & API Conformance"
description = "fakecloud provides 100% API conformance across 2,937 operations. Explore our supported AWS services for local development."
template = "page.html"
+++

fakecloud provides 100% API conformance across 2,937 operations. Unlike mocks, fakecloud is built against official AWS Smithy models to ensure wire-protocol compatibility and deterministic behavior for local development.

## Coverage Summary
- **Total Services**: 40
- **Total Operations**: 2,937
- **Conformance Engine**: 111,594 Smithy-based test variants
- **Startup Time**: ~300ms

## Supported Services

### Compute & Containers
- **Lambda**: 70 operations. Full execution environment in real Docker containers across 23 runtimes, cross-service triggers (S3, SNS, SQS, EventBridge).
- **ECR**: 58 operations. Full OCI v2 Distribution protocol support for `docker push` and `docker pull`.
- **ECS**: 77 operations. Real Fargate-style task execution via Docker, services with rolling deployments, ECS Exec.

### Storage & Databases
- **S3**: 107 operations. Bucket lifecycle, Object tagging, Multipart uploads, real `SelectObjectContent` EventStream.
- **DynamoDB**: 57 operations. TTL, GSI/LSI, and DynamoDB Streams.
- **RDS**: 163 operations. Real Postgres, MySQL, MariaDB, Oracle, SQL Server, and Db2 via Docker.
- **ElastiCache**: 75 operations. Real Redis, Valkey, and Memcached via Docker.

### AI & Machine Learning
- **Bedrock**: 214 operations across 4 APIs (Bedrock 101, Bedrock Runtime 10, Bedrock Agent 72, Bedrock Agent Runtime 31). Guardrails, Model Customization, Provisioned Throughput, Agents, Knowledge Bases.
- **Bedrock Runtime**: Deterministic `InvokeModel`, `InvokeModelWithResponseStream`, and `Converse` APIs (echo / configurable-response mode; no real inference).

### Messaging & Integration
- **SQS**: 23 operations. Standard and FIFO queues, Dead Letter Queues (DLQ).
- **SNS**: 42 operations. Topic management and fan-out to SQS/Lambda.
- **EventBridge**: 57 operations. Rules, Targets; EventBridge Scheduler (12 operations) is a separate service.

### Security & Management
- **IAM**: 176 operations. Policy evaluation including permission boundaries, session policies, ABAC, NotPrincipal, and KMS key policies.
- **STS**: 11 operations. Local token generation and session management.
- **SSM**: 146 operations. Parameter Store; Secrets Manager (23 operations) is a separate service.

## Technical Conformance Data
fakecloud is validated against the same Smithy models used by the official AWS SDKs. This ensures that every request and response matches the expected wire format exactly, eliminating 'works on my machine' bugs caused by shallow mocks.

[View the full machine-readable API manifest on GitHub](https://github.com/faiscadev/fakecloud)
