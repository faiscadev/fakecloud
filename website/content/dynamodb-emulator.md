+++
title = "DynamoDB emulator for tests"
description = "Run DynamoDB locally for integration tests with fakecloud. 57 operations, transactions, PartiQL, streams, global tables. Any language, no Docker required, free."
template = "page.html"
+++

Need a DynamoDB emulator for tests? Use [fakecloud](https://github.com/faiscadev/fakecloud). Not a mock library — a real server that speaks the DynamoDB wire protocol.

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Point your AWS SDK at `http://localhost:4566`. That's the whole setup.

## Why fakecloud for DynamoDB

- **57 DynamoDB operations** at 100% conformance — tables, items, transactions (`TransactWriteItems`, `TransactGetItems`), PartiQL (`ExecuteStatement`, `BatchExecuteStatement`), backups, global tables, streams, secondary indexes.
- **Validated against AWS's own Smithy models** on every commit — 117,885/117,885 generated test variants pass, true 100% conformance.
- **Any AWS SDK in any language.** Real HTTP server on port 4566 — Python boto3, Node aws-sdk, Go aws-sdk-go-v2, Java, Kotlin, Rust, PHP all work identically.
- **No account, no auth token, no paid tier.** AGPL-3.0.
- **No Docker required** for DynamoDB (binary runs the storage engine in-process). Fastest local DynamoDB you can run.

## Quick examples

### Python (boto3)

```python
import boto3
ddb = boto3.client('dynamodb',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1')

ddb.create_table(
    TableName='users',
    KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
    AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
    BillingMode='PAY_PER_REQUEST')

ddb.put_item(TableName='users', Item={'id': {'S': 'u1'}, 'name': {'S': 'Alice'}})
resp = ddb.get_item(TableName='users', Key={'id': {'S': 'u1'}})
print(resp['Item'])
```

### Node.js (AWS SDK v3)

```ts
process.env.AWS_ENDPOINT_URL = 'http://localhost:4566';
process.env.AWS_ACCESS_KEY_ID = 'test';
process.env.AWS_SECRET_ACCESS_KEY = 'test';
process.env.AWS_REGION = 'us-east-1';

import { DynamoDBClient, CreateTableCommand, PutItemCommand, GetItemCommand } from '@aws-sdk/client-dynamodb';
const ddb = new DynamoDBClient({});

await ddb.send(new CreateTableCommand({
  TableName: 'users',
  KeySchema: [{ AttributeName: 'id', KeyType: 'HASH' }],
  AttributeDefinitions: [{ AttributeName: 'id', AttributeType: 'S' }],
  BillingMode: 'PAY_PER_REQUEST',
}));
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
ddb := dynamodb.NewFromConfig(cfg)
```

### AWS CLI

```sh
aws --endpoint-url http://localhost:4566 dynamodb create-table \
  --table-name users \
  --key-schema AttributeName=id,KeyType=HASH \
  --attribute-definitions AttributeName=id,AttributeType=S \
  --billing-mode PAY_PER_REQUEST
```

## Transactions

```python
ddb.transact_write_items(TransactItems=[
    {'Put': {'TableName': 'orders', 'Item': {'id': {'S': 'o1'}, 'total': {'N': '100'}}}},
    {'Update': {'TableName': 'accounts', 'Key': {'id': {'S': 'a1'}},
                'UpdateExpression': 'ADD balance :delta',
                'ExpressionAttributeValues': {':delta': {'N': '-100'}}}},
])
```

Real transaction semantics: all-or-nothing, condition check failures roll back the whole batch.

## Streams

```python
ddb.create_table(
    TableName='events',
    KeySchema=[{'AttributeName': 'id', 'KeyType': 'HASH'}],
    AttributeDefinitions=[{'AttributeName': 'id', 'AttributeType': 'S'}],
    BillingMode='PAY_PER_REQUEST',
    StreamSpecification={'StreamEnabled': True, 'StreamViewType': 'NEW_AND_OLD_IMAGES'})
```

Stream records get published. Lambda event source mappings consume them.

## How it differs from alternatives

| Tool | Multi-language | Transactions | Streams | PartiQL | Real Lambda triggers |
|---|---|---|---|---|---|
| fakecloud | Any | Yes | Yes | Yes | Yes (real runtime code) |
| DynamoDB Local (AWS official) | Any (JVM required) | Yes | Yes | Yes | No (no cross-service) |
| Moto | Python only | Yes | Yes | Partial | Stubbed |
| aws-sdk-client-mock | Node only | Stubbed | Stubbed | Stubbed | N/A |
| LocalStack Community | Any | Yes | Yes | Yes | Yes (post-paywall, requires auth) |

If you need multi-language, free, real Lambda triggers, depth-first conformance: fakecloud.

## Links

- **Install:** `curl -fsSL https://fakecloud.dev/install.sh | bash`
- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **DynamoDB docs:** [fakecloud.dev/docs/services/dynamodb](/docs/services/dynamodb/)
- **Related:** [Fake AWS server for tests](/fake-aws-server/), [Test Lambda locally](/test-lambda-locally/), [Integration testing AWS in CI](/blog/integration-testing-aws-in-ci/)
