+++
title = "SQS emulator for tests"
description = "Run SQS locally for integration tests with fakecloud. 23 SQS operations, FIFO, DLQs, long polling, real Lambda event source mappings. Any AWS SDK, free."
template = "page.html"
+++

Need an SQS emulator for integration tests? Use [fakecloud](https://github.com/faiscadev/fakecloud). Not a mock library — a real server that speaks the SQS wire protocol.

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Point your AWS SDK at `http://localhost:4566`.

## Why fakecloud for SQS

- **23 SQS operations** at 100% conformance — queues, messages, long polling, FIFO queues with message group IDs and deduplication, dead-letter queues, batch operations, message attributes.
- **Real SQS -> Lambda event source mappings.** Your Lambda actually runs when messages arrive. `CreateEventSourceMapping` polls the queue, batches messages, invokes your function with the SQS event shape, deletes on success, routes failures to DLQ — same contract as real AWS.
- **Real SNS -> SQS fan-out.** Topic subscriptions deliver to queues for real.
- **Any AWS SDK in any language.** Real HTTP server on port 4566.
- **No account, no auth token, no paid tier.** AGPL-3.0.

## Quick examples

### Python (boto3)

```python
import boto3
sqs = boto3.client('sqs',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1')

q = sqs.create_queue(QueueName='jobs')
sqs.send_message(QueueUrl=q['QueueUrl'], MessageBody='{"job": "resize", "id": 42}')
resp = sqs.receive_message(QueueUrl=q['QueueUrl'], WaitTimeSeconds=1)
print(resp.get('Messages', []))
```

### Node.js (AWS SDK v3)

```ts
process.env.AWS_ENDPOINT_URL = 'http://localhost:4566';
process.env.AWS_ACCESS_KEY_ID = 'test';
process.env.AWS_SECRET_ACCESS_KEY = 'test';
process.env.AWS_REGION = 'us-east-1';

import { SQSClient, CreateQueueCommand, SendMessageCommand } from '@aws-sdk/client-sqs';
const sqs = new SQSClient({});

const { QueueUrl } = await sqs.send(new CreateQueueCommand({ QueueName: 'jobs' }));
await sqs.send(new SendMessageCommand({ QueueUrl, MessageBody: JSON.stringify({ job: 'resize' }) }));
```

### Go

```go
cfg, _ := config.LoadDefaultConfig(ctx,
    config.WithRegion("us-east-1"),
    config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
    config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
        func(s, r string, o ...interface{}) (aws.Endpoint, error) {
            return aws.Endpoint{URL: "http://localhost:4566"}, nil
        },
    )),
)
sqs := sqs.NewFromConfig(cfg)
```

### AWS CLI

```sh
aws --endpoint-url http://localhost:4566 sqs create-queue --queue-name jobs
aws --endpoint-url http://localhost:4566 sqs send-message \
  --queue-url http://localhost:4566/000000000000/jobs \
  --message-body '{"job":"resize","id":42}'
```

## FIFO queues

```sh
aws --endpoint-url http://localhost:4566 sqs create-queue \
  --queue-name orders.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=true

aws --endpoint-url http://localhost:4566 sqs send-message \
  --queue-url http://localhost:4566/000000000000/orders.fifo \
  --message-body '{"order":"o1"}' \
  --message-group-id orders
```

Real FIFO semantics: within a message group, messages are processed in order. Deduplication works.

## Dead-letter queues

```sh
aws --endpoint-url http://localhost:4566 sqs create-queue --queue-name jobs-dlq

# Link primary queue to DLQ via RedrivePolicy
aws --endpoint-url http://localhost:4566 sqs set-queue-attributes \
  --queue-url http://localhost:4566/000000000000/jobs \
  --attributes '{"RedrivePolicy":"{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:jobs-dlq\",\"maxReceiveCount\":\"3\"}"}'
```

After `maxReceiveCount` failed deliveries, messages route to the DLQ automatically. Same contract as real AWS.

## SQS -> Lambda event source mapping

```sh
aws --endpoint-url http://localhost:4566 lambda create-event-source-mapping \
  --function-name processor \
  --event-source-arn arn:aws:sqs:us-east-1:000000000000:jobs \
  --batch-size 10
```

fakecloud polls the queue, batches messages, invokes your Lambda with the SQS event shape, deletes messages on success, routes to DLQ on failure. Your function code actually runs (fakecloud pulls the Lambda runtime container).

## Assertions from tests

```ts
import { FakeCloud } from 'fakecloud';
const fc = new FakeCloud();

test('enqueue and process', async () => {
  await enqueue({ job: 'resize', id: 42 });

  const { invocations } = await fc.lambda.getInvocations({ functionName: 'processor' });
  expect(invocations).toHaveLength(1);
});

afterEach(() => fc.reset());
```

SDKs available in TypeScript, Python, Go, PHP, Java, Rust.

## How it differs from alternatives

| Tool | Multi-language | FIFO | DLQ | Real Lambda trigger | Real SNS fan-out |
|---|---|---|---|---|---|
| fakecloud | Any | Yes | Yes | Yes (real code runs) | Yes |
| ElasticMQ | Any | Yes | Yes | No (no Lambda service) | N/A |
| Moto (`mock_sqs`) | Python only | Yes | Yes | Stubbed | Stubbed |
| LocalStack Community | Any | Yes | Yes | Yes (post-paywall auth required) | Yes (paywall) |
| aws-sdk-client-mock | Node only | Stubbed | Stubbed | N/A | N/A |

If you need SQS + cross-service (SNS -> SQS, SQS -> Lambda actually firing), pick fakecloud.

## Links

- **Install:** `curl -fsSL https://fakecloud.dev/install.sh | bash`
- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Related:** [Fake AWS server for tests](/fake-aws-server/), [Test Lambda locally](/test-lambda-locally/), [Integration testing AWS in CI](/blog/integration-testing-aws-in-ci/)
