+++
title = "Testing S3 and Lambda: A Guide to Zero-Auth Local Integration"
date = 2026-05-17
description = "A guide to testing S3 and Lambda integrations locally with fakecloud, eliminating the need for AWS accounts, auth tokens, or internet access."

[extra]
author = "Lucas Vieira"
+++

Cloud-native development in 2026 has reached a point of extreme friction. While AWS now offers over 240 fully featured services, the tools meant to simplify local development have moved toward proprietary, account-gated models. As of May 13, 2026, the industry standard for local emulation has shifted, requiring developers to manage auth tokens and internet-connected accounts just to run a local integration test. This overhead kills the primary benefit of local development: speed.

When you are building an event-driven architecture—specifically one where an S3 bucket upload triggers a Lambda function—you cannot rely on simple mocks. Mocks fail to capture the complex JSON structure of an S3 event notification, the asynchronous nature of the trigger, and the IAM permission boundaries that govern the interaction. You need a high-fidelity environment that behaves like real infrastructure but starts in milliseconds.

## The Hurdle: Why Mocks and Cloud-First Testing Fail

Testing S3-to-Lambda integrations usually falls into two traps. The first is the "Mock Trap." You use a library to intercept calls to the AWS SDK. This works for unit tests but fails for integration. It doesn't verify if your S3 bucket is actually configured to send notifications to your Lambda. It doesn't check if the Lambda has the correct `lambda:InvokeFunction` policy. It certainly doesn't simulate the 500ms to 2s latency of a real cloud trigger.

The second trap is "Cloud-First Testing." You deploy your code to a 'dev' account for every change. This introduces a massive feedback loop. You wait for CloudFormation or Terraform to update, wait for the S3 event to propagate, and then dig through CloudWatch Logs to see if it worked. In 2026, with the complexity of frontier AI models and massive data streams, this cycle is unsustainable.

fakecloud solves this by providing a local AWS environment that requires:
- No AWS account
- No auth tokens
- No internet connection
- No paid subscriptions

It is a standalone binary (~19MB) that starts in approximately 500ms, giving you a 100% conformant API surface for 33 core AWS services.

## Solution: Real Cross-Service Integration

fakecloud doesn't just mock responses; it wires services together. When you configure an S3 bucket notification to trigger a Lambda, fakecloud's internal event bus handles the fanout. It generates the exact Smithy-compliant JSON event, serializes it, and invokes your local Lambda function—whether it's running as a separate process or inside a Docker container.

### The "No" List
To maintain engineering pragmatism, we explicitly eliminate the following requirements from your workflow:
- **No `LOCALSTACK_AUTH_TOKEN`**: You don't need to sign in to a vendor platform to run your own tests.
- **No Internet**: Your CI/CD pipeline can run in a completely air-gapped environment.
- **No IAM Management**: While fakecloud supports IAM operations, it defaults to a permissive state for local development, so you aren't fighting `AccessDenied` errors while trying to fix a bug.
- **No Latency**: The sub-second startup and local execution mean your test suite runs as fast as your CPU allows.

## 100% Conformance Across 2,422 Operations

Reliability in an emulator is measured by its conformance to the official AWS API. fakecloud is built on top of 59,000+ Smithy-model-generated test variants. This ensures that when your application calls `s3:PutBucketNotificationConfiguration`, the request and response shapes are identical to what you would see in `us-east-1`.

| Feature | fakecloud | Traditional Mocks | Cloud-First |
| :--- | :--- | :--- | :--- |
| **Startup Time** | ~500ms | <100ms | Minutes |
| **Auth Required** | None | None | IAM/SSO |
| **API Conformance** | 100% (2,422 ops) | Low/Manual | 100% |
| **Cross-Service Triggers** | Yes (S3, SNS, SQS) | No | Yes |
| **Binary Size** | ~19MB | N/A | N/A |

## Code Snippet: Setting Up the S3-to-Lambda Trigger

You don't need complex YAML to get started. Use the standard AWS CLI or your preferred IaC tool. Point the endpoint URL to `http://localhost:4566`.

```bash
# 1. Start fakecloud in the background
./fakecloud &

# 2. Create the Lambda function
aws --endpoint-url http://localhost:4566 lambda create-function \
    --function-name processor-func \
    --runtime nodejs20.x \
    --handler index.handler \
    --zip-file fileb://function.zip \
    --role arn:aws:iam::000000000000:role/lambda-role

# 3. Create the S3 bucket
aws --endpoint-url http://localhost:4566 s3 mb s3://incoming-data

# 4. Link the bucket to the Lambda
aws --endpoint-url http://localhost:4566 s3api put-bucket-notification-configuration \
    --bucket incoming-data \
    --notification-configuration '{
        "LambdaFunctionConfigurations": [{
            "LambdaFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:processor-func",
            "Events": ["s3:ObjectCreated:*"]
        }]
    }'
```

Once this is run, any file uploaded to `s3://incoming-data` will immediately trigger `processor-func`. There is no polling and no manual event injection required.

## Evidence: Asserting Side Effects with First-Party SDKs

The biggest challenge in integration testing is knowing what happened *after* the trigger. If your Lambda processes an S3 upload and then sends an SNS message or calls an AI model via Bedrock, how do you verify that without complex logging?

fakecloud provides first-party SDKs for TypeScript, Python, Go, PHP, Java, and Rust. These SDKs allow your test code to query the internal state of the emulator. While your application uses the standard `aws-sdk`, your test uses the `fakecloud-sdk` to assert on side effects.

### Example: TypeScript Assertion

```ts
import { FakeCloud } from "fakecloud";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";

const fc = new FakeCloud();
const s3 = new S3Client({ endpoint: "http://localhost:4566" });

async function testS3Trigger() {
    // 1. Upload a file using the standard AWS SDK
    await s3.send(new PutObjectCommand({
        Bucket: "incoming-data",
        Key: "test-file.json",
        Body: JSON.stringify({ data: "hello" })
    }));

    // 2. Use fakecloud SDK to assert the Lambda was invoked
    const invocations = await fc.lambda.getInvocations("processor-func");
    expect(invocations).toHaveLength(1);
    expect(invocations[0].payload).toContain("test-file.json");

    // 3. Reset for the next test
    await fc.reset();
}
```

This separation of concerns is critical. Your application code remains clean and cloud-agnostic, while your tests gain deep visibility into the infrastructure's behavior.

## AI Development: Local Bedrock Support

As of May 2026, AI integration is no longer optional. fakecloud includes full support for Amazon Bedrock, covering 111 operations across the runtime and control plane. This includes `InvokeModel` and `Converse` (with streaming support) for the latest frontier models like Claude 4.7 and GPT-5.5.

Testing AI workflows locally is notoriously difficult due to the cost and rate limits of cloud APIs. fakecloud allows you to:
- Simulate model responses with configurable payloads.
- Test Bedrock Guardrails by triggering real content evaluation and PII detection locally.
- Verify asynchronous batch jobs via S3 without incurring AWS costs.
- Inject faults (e.g., 429 Too Many Requests) to test your application's retry logic.

This is not a simple mock that returns a static string. It is a full implementation of the Bedrock API shape, allowing you to build and test complex AI agents entirely on your laptop.

## Performance and Portability

The fakecloud binary is optimized for the modern developer's machine. At ~19MB, it is smaller than most PDF documents. It doesn't require a heavy Docker daemon to run core services, though a Docker image is available if your workflow prefers it. The ~500ms startup time means you can start and stop the entire AWS environment for every single test file in your suite, ensuring total isolation without the performance penalty usually associated with local cloud emulators.

## Beyond S3 and Lambda: 33+ Supported Services

While S3 and Lambda are the workhorses of the cloud, fakecloud's coverage extends to the entire core stack. This includes:
- **DynamoDB**: Full support for Global Tables, TTL, and DynamoDB Streams.
- **SQS/SNS**: Real message fanout and dead-letter queue (DLQ) logic.
- **EventBridge**: Support for the EventBridge Scheduler and complex rule matching.
- **Cognito**: Local User Pools for testing authentication flows without hitting the public internet.
- **Step Functions**: Execute state machines locally, including task integrations with other fakecloud services.

Every service is validated against the same Smithy conformance suite, ensuring that your local development environment is a high-fidelity mirror of your production infrastructure.

## Next Step: Run Your First Integration

Stop fighting with auth tokens and cloud latency. You can have a full AWS environment running on your machine in seconds. Download the binary, point your SDK at localhost, and start building.

Run the following command to install the latest version of fakecloud and start your local environment:

`curl -fsSL https://raw.githubusercontent.com/faiscadev/fakecloud/main/install.sh | bash && fakecloud`