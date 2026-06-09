+++
title = "fakecloud vs SAM Local"
description = "How fakecloud compares to AWS SAM Local. Scope difference: SAM Local runs Lambda + limited API Gateway; fakecloud runs 40 services with real cross-service wiring."
template = "page.html"
+++

AWS SAM Local is AWS's official tool for running Lambda functions locally. It invokes Lambda inside a Docker container with the real AWS runtime image, and provides a limited HTTP / API Gateway surface in front.

fakecloud runs Lambda the same way — real runtime containers, 23 runtimes supported — and also runs 39 other AWS services end-to-end at true 100% Smithy conformance (113,614/113,614 variants pass).

## Scope difference

| | fakecloud | SAM Local |
|---|---|---|
| Lambda real code execution | Yes (23 runtimes in Docker) | Yes (all AWS-managed runtimes in Docker) |
| API Gateway v2 | 103 ops, HTTP APIs, JWT/Lambda authorizers | Limited (REST API emulation) |
| API Gateway v1 | Not yet | Yes (SAM focuses here) |
| S3 | 107 ops, real storage + notifications | **No** |
| SQS | 23 ops, real queues + event source mappings | **No** |
| SNS | 42 ops, real fan-out | **No** |
| DynamoDB | 57 ops, transactions, PartiQL, streams | **No** |
| EventBridge | 57 ops + Scheduler (12 ops) | **No** |
| Step Functions | 37 ops, full ASL interpreter | **No** |
| RDS | 163 ops, real PostgreSQL/MySQL/MariaDB | **No** |
| Cognito | 122 ops, full auth flows | **No** |
| Cross-service triggers | S3 -> Lambda, SQS -> Lambda, SNS -> Lambda, EventBridge -> Lambda all fire | Synthetic events only (you generate JSON and hand it to your handler) |

## The difference in practice

**SAM Local's model:** run your Lambda. If your Lambda reads from DynamoDB, you run your Lambda against fake event JSON you generate. The DynamoDB side is not emulated — you mock or point at real AWS.

**fakecloud's model:** run your Lambda *and* the services it talks to. If your Lambda reads from DynamoDB, fakecloud runs the DynamoDB. If it's triggered by S3 upload, fakecloud fires the trigger for real when the upload happens. If it publishes to SNS, the topic actually fans out to subscribers.

## When SAM Local is the right pick

- **You're only building a Lambda function** (nothing else to emulate).
- **Your Lambda reads from fixed data** or pointed at a shared/staging AWS.
- **You're heavy on SAM templates / CloudFormation** and want AWS's own SAM tooling.
- **You need API Gateway v1** specifically (fakecloud supports v2; v1 is on the roadmap).

## When fakecloud is the right pick

- **Your Lambda talks to other AWS services** (DynamoDB, SQS, SNS, S3, Cognito, etc) — fakecloud runs them.
- **Your Lambda is triggered by other services** (S3 PutObject, SQS message, EventBridge rule) — fakecloud fires the trigger end-to-end.
- **You want to test the whole system locally**, not just the function handler.
- **You're not on SAM templates** — fakecloud supports CDK, Terraform, serverless-framework, or no IaC at all.

## Using both

Some teams run SAM Local for Lambda synthesis + local invoke, and fakecloud for the rest of AWS behind the function. This works — they don't conflict.

## Install fakecloud

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Lambda in fakecloud works the same way SAM Local does: pull the runtime container, mount your code, invoke the handler. The difference is everything else in the system also runs.

- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Test Lambda locally:** [/test-lambda-locally/](/test-lambda-locally/)
- **Full Lambda tutorial:** [/blog/test-lambda-locally/](/blog/test-lambda-locally/)
- **CDK local testing:** [/blog/cdk-local-testing/](/blog/cdk-local-testing/)
