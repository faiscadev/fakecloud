+++
title = "How to test Lambda locally: the full guide for 2026"
date = 2026-04-22
description = "Run Lambda locally against a real runtime in seconds. Covers all 13 AWS Lambda runtimes, event source mappings, and cross-service triggers (S3, SQS, SNS, EventBridge). No mocks, no SAM, no account required."

[extra]
author = "Lucas Vieira"
+++

If you want to run AWS Lambda locally, you have a few options, and most of them are worse than they need to be.

- **SAM Local** invokes Lambda inside a Docker container, which is close to the real thing, but its integration with other AWS services is thin. If your function reads from SQS or publishes to SNS or is triggered by S3, SAM hands you a bag of event-shaped JSON and wishes you luck.
- **serverless-offline** simulates API Gateway -> Lambda only. Everything downstream is mocked.
- **Pure unit tests with mocks** tell you your code compiles. They do not tell you your code works.

This guide shows how to test Lambda locally end-to-end: real function code executing in a real runtime, triggered by real events from other AWS services, asserting on real side effects. No account, no auth token, no paid tier.

We'll use [fakecloud](https://github.com/faiscadev/fakecloud) — a free, open-source AWS emulator that runs Lambda in real Docker containers across all 13 official runtimes and wires it up to 22 other AWS services that trigger and consume it.

## What you need

- Docker (fakecloud boots Lambda runtime containers)
- The AWS CLI, or any AWS SDK
- ~30 seconds

## Install fakecloud

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

That's it. It listens on `http://localhost:4566`.

If you prefer Docker:

```sh
docker run --rm \
  -p 4566:4566 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  ghcr.io/faiscadev/fakecloud
```

The Docker socket mount is only needed because Lambda execution needs Docker-in-Docker. The single-binary install above doesn't need it.

## A Node.js Lambda, end-to-end

Create the function:

```sh
cat > index.js <<'EOF'
exports.handler = async (event) => {
  return { statusCode: 200, body: JSON.stringify({ event }) };
};
EOF
zip fn.zip index.js
```

Deploy it to fakecloud:

```sh
aws --endpoint-url http://localhost:4566 iam create-role \
  --role-name lambda-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

aws --endpoint-url http://localhost:4566 lambda create-function \
  --function-name hello \
  --runtime nodejs20.x \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --handler index.handler \
  --zip-file fileb://fn.zip
```

Invoke it:

```sh
aws --endpoint-url http://localhost:4566 lambda invoke \
  --function-name hello \
  --payload '{"hello":"world"}' \
  --cli-binary-format raw-in-base64-out \
  out.json
cat out.json
```

Output:

```json
{"statusCode":200,"body":"{\"event\":{\"hello\":\"world\"}}"}
```

That's your Node.js code executing in a real Node 20 container. fakecloud pulled the runtime image, mounted your zip, and invoked `handler`.

## Python, Java, Go, .NET, Ruby, custom

Same flow, different runtime string. fakecloud supports all 13 AWS Lambda runtimes:

- `nodejs18.x`, `nodejs20.x`, `nodejs22.x`
- `python3.9`, `python3.10`, `python3.11`, `python3.12`
- `java11`, `java17`, `java21`
- `dotnet6`, `dotnet8`
- `ruby3.2`
- `go1.x`, `provided.al2`, `provided.al2023` (custom runtimes)

Example — Python:

```sh
cat > lambda_function.py <<'EOF'
def handler(event, context):
    return {"ok": True, "event": event}
EOF
zip fn.zip lambda_function.py

aws --endpoint-url http://localhost:4566 lambda create-function \
  --function-name py-hello \
  --runtime python3.12 \
  --role arn:aws:iam::000000000000:role/lambda-role \
  --handler lambda_function.handler \
  --zip-file fileb://fn.zip

aws --endpoint-url http://localhost:4566 lambda invoke \
  --function-name py-hello \
  --payload '{"x":1}' \
  --cli-binary-format raw-in-base64-out \
  out.json
```

## Triggers that actually fire

This is where "test Lambda locally" usually breaks down. Your Lambda is not invoked by a human calling Invoke — it's triggered by S3, SQS, SNS, EventBridge, DynamoDB Streams, API Gateway, or an event source mapping. fakecloud has those wired up for real.

### SQS event source mapping

```sh
aws --endpoint-url http://localhost:4566 sqs create-queue --queue-name jobs

QUEUE_URL=http://localhost:4566/000000000000/jobs
QUEUE_ARN=arn:aws:sqs:us-east-1:000000000000:jobs

aws --endpoint-url http://localhost:4566 lambda create-event-source-mapping \
  --function-name hello \
  --event-source-arn $QUEUE_ARN \
  --batch-size 1

aws --endpoint-url http://localhost:4566 sqs send-message \
  --queue-url $QUEUE_URL \
  --message-body '{"job":"resize","id":42}'
```

fakecloud polls the queue, batches the message, invokes your Lambda with the SQS event shape, and deletes the message on success. Same contract as real AWS.

### S3 -> Lambda

```sh
aws --endpoint-url http://localhost:4566 s3 mb s3://uploads
aws --endpoint-url http://localhost:4566 lambda add-permission \
  --function-name hello \
  --statement-id s3invoke \
  --action lambda:InvokeFunction \
  --principal s3.amazonaws.com \
  --source-arn arn:aws:s3:::uploads

aws --endpoint-url http://localhost:4566 s3api put-bucket-notification-configuration \
  --bucket uploads \
  --notification-configuration '{
    "LambdaFunctionConfigurations": [{
      "LambdaFunctionArn": "arn:aws:lambda:us-east-1:000000000000:function:hello",
      "Events": ["s3:ObjectCreated:*"]
    }]
  }'

echo "hi" | aws --endpoint-url http://localhost:4566 s3 cp - s3://uploads/file.txt
```

Your Lambda fires with the S3 event. End-to-end, no stubs.

### EventBridge -> Lambda

```sh
aws --endpoint-url http://localhost:4566 events put-rule \
  --name on-order \
  --event-pattern '{"source":["store"],"detail-type":["OrderPlaced"]}'

aws --endpoint-url http://localhost:4566 events put-targets \
  --rule on-order \
  --targets 'Id=1,Arn=arn:aws:lambda:us-east-1:000000000000:function:hello'

aws --endpoint-url http://localhost:4566 events put-events \
  --entries 'Source=store,DetailType=OrderPlaced,Detail={"orderId":"o1"}'
```

Your Lambda fires with the EventBridge event envelope.

## Asserting on side effects

fakecloud ships test-assertion SDKs that let your tests check what happened without raw HTTP:

```ts
import { FakeCloud } from "fakecloud";
const fc = new FakeCloud();

// Your app publishes to SNS inside a Lambda. Your test asserts it happened.
const { invocations } = await fc.lambda.getInvocations({ functionName: "hello" });
expect(invocations).toHaveLength(1);
expect(invocations[0].statusCode).toBe(200);

const { messages } = await fc.sns.getPublishedMessages({ topicName: "orders" });
expect(messages[0].message).toContain("o1");

await fc.reset();
```

SDKs in TypeScript, Python, Go, PHP, Java, Rust. Reference: [fakecloud.dev/docs/sdks](https://fakecloud.dev/docs/sdks).

## Watching the logs

Lambda stdout goes to CloudWatch Logs, which fakecloud also emulates:

```sh
aws --endpoint-url http://localhost:4566 logs tail /aws/lambda/hello --follow
```

## Running in CI

```yaml
# .github/workflows/test.yml
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: curl -fsSL https://fakecloud.dev/install.sh | bash
      - run: fakecloud &
      - run: |
          for i in $(seq 1 30); do
            curl -sf http://localhost:4566/_fakecloud/health && break
            sleep 1
          done
      - run: npm ci && npm test
        env:
          AWS_ENDPOINT_URL: http://localhost:4566
          AWS_ACCESS_KEY_ID: test
          AWS_SECRET_ACCESS_KEY: test
          AWS_REGION: us-east-1
```

~500ms startup. A whole Lambda-integration test suite completes in seconds on a cold runner.

## Correctness vs performance

fakecloud runs your function code in the real AWS Lambda runtime containers, so behavior matches real Lambda. What it doesn't replicate is AWS's distributed infrastructure timing: cold-start latency, concurrency-accounting scheduling, and VPC networking come from AWS's own implementation, not from a local process, so numbers on those will not match real AWS. Use real AWS for those measurements, use fakecloud for everything correctness-related.

## Links

- Install: `curl -fsSL https://fakecloud.dev/install.sh | bash`
- Repo: [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- Lambda docs: [fakecloud.dev/docs/services/lambda](https://fakecloud.dev/docs/services/lambda)
- SDKs: [fakecloud.dev/docs/sdks](https://fakecloud.dev/docs/sdks)
- Issues: [github.com/faiscadev/fakecloud/issues](https://github.com/faiscadev/fakecloud/issues)
