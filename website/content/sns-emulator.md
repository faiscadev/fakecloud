+++
title = "SNS emulator for tests"
description = "Run SNS locally for integration tests with fakecloud. 42 SNS operations, fan-out to SQS/Lambda/HTTP, filter policies, real cross-service delivery. Any AWS SDK, free, no account required."
template = "page.html"
+++

Need an SNS emulator for integration tests? Use [fakecloud](https://github.com/faiscadev/fakecloud). Not a mock library — a real server that speaks the SNS wire protocol.

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Point your AWS SDK at `http://localhost:4566`.

## Why fakecloud for SNS

- **42 SNS operations** at 100% conformance — topics, subscriptions, publishing, filter policies (exact, prefix, anything-but, numeric, exists), message attributes.
- **Real fan-out delivery.** SNS -> SQS, SNS -> Lambda, SNS -> HTTP/HTTPS endpoints all execute end-to-end. Your subscriber Lambda actually runs when a message is published.
- **Filter policies are real.** Messages match filter policies before delivery, same semantics as AWS.
- **Any AWS SDK in any language.** Real HTTP server on port 4566.
- **No account, no auth token, no paid tier.** AGPL-3.0.

## Quick examples

### Python (boto3)

```python
import boto3
sns = boto3.client('sns',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1')

topic = sns.create_topic(Name='orders')
sns.publish(TopicArn=topic['TopicArn'], Message='{"order": "o1"}')
```

### Node.js (AWS SDK v3)

```ts
process.env.AWS_ENDPOINT_URL = 'http://localhost:4566';
process.env.AWS_ACCESS_KEY_ID = 'test';
process.env.AWS_SECRET_ACCESS_KEY = 'test';
process.env.AWS_REGION = 'us-east-1';

import { SNSClient, CreateTopicCommand, PublishCommand } from '@aws-sdk/client-sns';
const sns = new SNSClient({});

const { TopicArn } = await sns.send(new CreateTopicCommand({ Name: 'orders' }));
await sns.send(new PublishCommand({ TopicArn, Message: JSON.stringify({ order: 'o1' }) }));
```

### AWS CLI

```sh
aws --endpoint-url http://localhost:4566 sns create-topic --name orders
aws --endpoint-url http://localhost:4566 sns publish \
  --topic-arn arn:aws:sns:us-east-1:000000000000:orders \
  --message '{"order":"o1"}'
```

## SNS -> SQS fan-out

```sh
aws --endpoint-url http://localhost:4566 sqs create-queue --queue-name orders-queue

aws --endpoint-url http://localhost:4566 sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:000000000000:orders \
  --protocol sqs \
  --notification-endpoint arn:aws:sqs:us-east-1:000000000000:orders-queue
```

Publishing to the topic delivers to the queue for real. `ReceiveMessage` returns the SNS-wrapped envelope.

## SNS -> Lambda

```sh
aws --endpoint-url http://localhost:4566 sns subscribe \
  --topic-arn arn:aws:sns:us-east-1:000000000000:orders \
  --protocol lambda \
  --notification-endpoint arn:aws:lambda:us-east-1:000000000000:function:on-order
```

Your Lambda fires with the SNS event shape when messages are published. Not a stub — the Lambda code runs in a real runtime container.

## Filter policies

```sh
aws --endpoint-url http://localhost:4566 sns set-subscription-attributes \
  --subscription-arn <arn> \
  --attribute-name FilterPolicy \
  --attribute-value '{"eventType":["orderPlaced","orderShipped"]}'
```

Real filter matching — messages published without matching attributes do not deliver.

## Assertions

```ts
import { FakeCloud } from 'fakecloud';
const fc = new FakeCloud();

const { messages } = await fc.sns.getPublishedMessages({ topicName: 'orders' });
expect(messages).toHaveLength(1);
expect(JSON.parse(messages[0].message).order).toBe('o1');

await fc.reset();
```

SDKs for TypeScript, Python, Go, PHP, Java, Rust.

## How it differs from alternatives

| Tool | Multi-language | Fan-out to SQS | Fan-out to Lambda (real) | Filter policies |
|---|---|---|---|---|
| fakecloud | Any | Yes | Yes (real code runs) | Yes |
| LocalStack Community | Any (auth required post-paywall) | Yes | Yes | Yes |
| Moto (`mock_sns`) | Python only | Yes | Stubbed | Partial |
| aws-sdk-client-mock | Node only | Stubbed | N/A | Stubbed |

## Links

- **Install:** `curl -fsSL https://fakecloud.dev/install.sh | bash`
- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Related:** [SQS emulator](/sqs-emulator/), [Test Lambda locally](/test-lambda-locally/), [Fake AWS server for tests](/fake-aws-server/)
