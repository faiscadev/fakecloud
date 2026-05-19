+++
title = "Testing SNS to S3 Triggers Locally in 500ms"
date = 2026-05-19
description = "Test SNS to S3 event-driven integrations locally with 100% API conformance. Use fakecloud's standalone binary for sub-second feedback loops without AWS accounts or Docker."

[extra]
author = "Lucas Vieira"
+++

Mocking individual AWS services is a common strategy that leads to production failures. When you mock a call to `sns.Publish` or `s3.PutObject`, you are testing your ability to write a mock, not the infrastructure's ability to handle the event. In a real-world event-driven system, the failure points live in the gaps between services: the IAM policy that doesn't quite allow the SNS fanout, the S3 bucket notification configuration that points to a non-existent ARN, or the Lambda trigger that fails because of a malformed event payload.

As of 2026-05-13, the landscape for local AWS development has shifted. With major incumbents moving to proprietary models and requiring mandatory auth tokens even for local runs, developers need a high-fidelity, zero-friction alternative. [fakecloud](https://github.com/faiscadev/fakecloud) provides a standalone ~19MB binary that starts in ~500ms, offering 100% API conformance across 2,422 operations without requiring an account, internet connection, or subscription.

## The Integration Gap: Why Mocks Fail

Traditional unit testing with mocks assumes that if your code calls an API correctly, the infrastructure will behave as expected. This assumption is dangerous in complex integrations like an SNS-to-S3 trigger workflow. 

Consider a scenario where an SNS message triggers a fanout to multiple S3 buckets or a Lambda function that subsequently writes to S3. If you mock the SNS client, you never verify:
1. The actual JSON structure of the event delivered to the subscriber.
2. The latency and retry logic inherent in the SNS-to-S3 delivery path.
3. The cross-service permissions required for the integration to function.

fakecloud eliminates this gap by running the actual service logic locally. When your application publishes to a fakecloud SNS topic, the binary executes the fanout logic, constructs the real event payload, and delivers it to the local S3 or Lambda service exactly as the AWS production environment would.

## Scenario: The SNS-to-S3 Fanout

In this walkthrough, we will implement a common architectural pattern: an SNS topic that receives a message and triggers an S3 upload via a Lambda function. This tests the full chain of command: `SNS -> Lambda -> S3`.

### The "No" List
Before we start, note what you do **not** need:
- No AWS account.
- No `AWS_ACCESS_KEY_ID` validation (use `dummy` for everything).
- No internet connection.
- No Docker (unless you are running Lambda functions).
- No auth tokens or login commands.

## Execution: Launching the Stack

First, start the fakecloud binary. It listens on the standard port 4566 and is ready to accept requests in less than half a second.

```sh
# Start the fakecloud binary
./fakecloud

# In a separate terminal, configure your local resources
export AWS_ENDPOINT_URL="http://localhost:4566"
export AWS_ACCESS_KEY_ID="dummy"
export AWS_SECRET_ACCESS_KEY="dummy"
export AWS_REGION="us-east-1"

# 1. Create the SNS Topic
TOPIC_ARN=$(aws sns create-topic --name local-events --query "TopicArn" --output text)

# 2. Create the S3 Bucket
aws s3 mb s3://processed-data

# 3. Create the Lambda function (using a pre-built zip)
aws lambda create-function \
    --function-name s3-uploader \
    --runtime python3.12 \
    --handler index.handler \
    --role arn:aws:iam::000000000000:role/lambda-role \
    --zip-file fileb://function.zip

# 4. Subscribe the Lambda to the SNS Topic
aws sns subscribe \
    --topic-arn "$TOPIC_ARN" \
    --protocol lambda \
    --notification-endpoint arn:aws:lambda:us-east-1:000000000000:function:s3-uploader
```

At this point, your local environment is fully wired. The fakecloud binary is managing the state of 33 core AWS services in-memory, backed by 59,000+ Smithy-model-generated test variants to ensure the API responses match AWS exactly.

## 100% Conformance Across 2,422 Operations

fakecloud is not a collection of simple mocks. It is a high-fidelity emulator built on the Smithy models used by AWS to define their own APIs. As of May 2026, fakecloud supports 2,422 operations with 100% behavioral conformance. This includes complex behaviors like:

| Service | Supported Operations | Key Integration Features |
| :--- | :--- | :--- |
| **SNS** | 48 | Fanout to SQS, Lambda, and HTTP; Message Filtering; Attributes. |
| **S3** | 124 | Multipart uploads; Bucket notifications; Lifecycle policies; Object tagging. |
| **Lambda** | 62 | 23+ runtimes; Event source mappings; Layer support; Synchronous/Asynchronous invocation. |
| **Bedrock** | 111 | Full support for frontier models (Claude 4.7, GPT-5.5) as of 2026-05-13. |

This depth ensures that when you call `sns.Publish`, the message doesn't just disappear into a black hole. It is parsed, filtered according to your subscription attributes, and delivered to the target. If the target is a Lambda function, fakecloud spins up the appropriate container, injects the event, and captures the logs.

## Assertion: Using the fakecloud SDK

Testing asynchronous integrations often involves "sleep and pray" or complex polling logic. You upload a file and then poll S3 for 10 seconds hoping the Lambda finished its work. fakecloud solves this with first-party SDKs for assertions in 6+ programming languages (TypeScript, Python, Go, PHP, Java, and Rust).

Instead of polling the public AWS API, you use the fakecloud SDK to inspect the internal state of the emulator. This allows for deterministic, sub-second test execution.

```python
import boto3
from fakecloud_sdk import FakeCloudClient

# Standard AWS SDK for the application logic
sns = boto3.client("sns", endpoint_url="http://localhost:4566")

# fakecloud SDK for the test assertions
fc = FakeCloudClient(endpoint="http://localhost:4566")

def test_sns_to_s3_trigger():
    # 1. Trigger the workflow
    sns.publish(TopicArn=topic_arn, Message="test-payload")
    
    # 2. Assert using the fakecloud SDK
    # This checks the internal event queue to ensure the message was processed
    integration_state = fc.inspect_integration(source="sns", target="lambda")
    assert integration_state.successful_invocations == 1
    
    # 3. Verify the final side effect in S3
    s3 = boto3.client("s3", endpoint_url="http://localhost:4566")
    response = s3.list_objects_v2(Bucket="processed-data")
    assert len(response.get("Contents", [])) == 1
```

By using the `inspect_integration` method, you eliminate the non-determinism of local testing. You are asserting on the fact that the event loop completed, not just that the initial API call returned a 200 OK.

## AI Development: Local Bedrock Support

As of 2026-05-13, AI integration is a core part of the AWS ecosystem. fakecloud includes full support for Amazon Bedrock (111 operations), allowing you to develop agentic workflows locally. This is particularly useful for testing how your SNS-triggered Lambda functions interact with foundation models like Claude 4.7 or the latest Amazon Nova Premier models.

Because fakecloud is a single binary, you can run these AI integration tests in restricted environments where internet access is prohibited for security reasons. You can configure deterministic responses for Bedrock prompts, allowing you to test your application's parsing logic and error handling without incurring costs or dealing with the latency of frontier model APIs.

## Performance Benchmarks: fakecloud vs. Incumbents

In a modern CI/CD pipeline, every second matters. Traditional local AWS emulators often rely on heavy container orchestration, leading to multi-minute startup times and high memory overhead. fakecloud is designed for engineering pragmatism.

| Metric | fakecloud | Proprietary Incumbents |
| :--- | :--- | :--- |
| **Binary Size** | ~19 MB | ~250 MB+ (Docker Image) |
| **Startup Time** | ~500 ms | 30 - 90 seconds |
| **Idle Memory** | ~10 MiB | 1.5 GiB+ |
| **Auth Required** | No | Yes (Auth Token/Account) |
| **Internet Required** | No | Yes (for license checks) |
| **License** | AGPL-3.0 | Proprietary / Paid Tier |

Running fakecloud as a background process in your GitHub Actions or GitLab CI runner adds negligible overhead. You can spin up a fresh environment for every single test file, ensuring total isolation without the performance penalty of restarting a heavy container stack.

## Real Cross-Service Integrations

The power of fakecloud lies in its ability to handle "Real" integrations. When we say 100% conformance, we mean the wire protocol and the side effects. 

- **SNS Fanout**: If you subscribe three different SQS queues to one SNS topic, fakecloud will deliver the message to all three, respecting individual filter policies.
- **S3 Triggers**: fakecloud supports the full S3 notification configuration, including prefix/suffix filtering and all event types (`s3:ObjectCreated:*`, `s3:ObjectRemoved:*`, etc.).
- **SES Inbound**: You can simulate receiving an email in SES and have it automatically trigger an SNS notification or save the raw email to an S3 bucket.

These are not hard-coded shortcuts. They are the result of a unified event-bus architecture within the fakecloud binary that mirrors the asynchronous nature of the AWS cloud.

## Engineering Pragmatism: The AGPL-3.0 Advantage

fakecloud is released under the AGPL-3.0 license for local development. This ensures that the tool remains free and open-source, protecting developers from the sudden "proprietary pivots" that have affected other tools in the space. You can audit the source code, contribute fixes, and run it in any environment—from a disconnected laptop on a plane to a high-security air-gapped server—without ever asking for permission or providing a credit card.

Your development workflow should not be dependent on a third-party's uptime or licensing server. By moving your integration tests to a standalone binary, you regain control over your test suite's reliability and speed.

## Next Steps: Beyond SNS and S3

Testing SNS to S3 triggers is only the beginning. The same sub-second feedback loop applies to more complex architectures involving SQS dead-letter queues, DynamoDB Streams, and EventBridge pipes. 

To explore further integration patterns, run the following command to see the full list of supported operations for your specific service mix:

```sh
./fakecloud --list-operations --service dynamodb
```

For detailed implementation guides on setting up local SQS-to-Lambda triggers or DynamoDB global tables, visit the fakecloud documentation. Start your local environment, run your tests, and inspect the results—all before your cloud-based CI environment would have even finished its authentication handshake.