<p align="center">
  <strong>FakeCloud</strong><br>
  <em>Local AWS cloud emulator. Free forever.</em>
</p>

<p align="center">
  <a href="https://github.com/faiscadev/fakecloud/actions"><img src="https://img.shields.io/github/actions/workflow/status/faiscadev/fakecloud/ci.yml?branch=main&label=CI" alt="CI"></a>
  <a href="https://github.com/faiscadev/fakecloud/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-AGPL--3.0-blue" alt="License"></a>
  <a href="https://hub.docker.com/r/faiscadev/fakecloud"><img src="https://img.shields.io/docker/pulls/faiscadev/fakecloud" alt="Docker Pulls"></a>
  <a href="https://crates.io/crates/fakecloud-server"><img src="https://img.shields.io/crates/v/fakecloud-server" alt="crates.io"></a>
  <a href="https://fakecloud.dev"><img src="https://img.shields.io/badge/docs-fakecloud.dev-green" alt="Docs"></a>
</p>

---

FakeCloud is an open-source local AWS cloud emulator. It runs on a single port
(`4566`), requires no account or auth token, and aims to faithfully replicate
AWS service behavior for local development and testing.

Part of the [faisca project family](https://github.com/faiscadev).

## Why FakeCloud?

In March 2026, LocalStack dropped its free Community Edition, leaving developers
without a reliable open-source option for local AWS emulation. FakeCloud was
built to fill that gap -- with a focus on correctness and simplicity.

### Comparison

| Feature | FakeCloud | LocalStack |
|---|---|---|
| License | AGPL-3.0 | Proprietary (was open) |
| Auth required | No | Yes (since March 2026) |
| Free tier | Fully open source | Removed (was Community Ed.) |
| Single port | Yes (4566) | Yes (4566) |
| SQS | 20 actions | Paid |
| SNS | 16 actions | Paid |
| EventBridge | 15 actions | Paid |
| IAM / STS | 16 actions | Paid |
| SSM Parameter Store | 28 actions | Paid |
| Cross-service delivery | Yes | Yes |
| Scheduled rules fire | Yes | Yes |

## Quick Start

### From source

```sh
# Requires Rust 1.85+
git clone https://github.com/faiscadev/fakecloud.git
cd fakecloud
cargo run --release --bin fakecloud-server
```

### Docker

```sh
docker run --rm -p 4566:4566 faiscadev/fakecloud
```

### Docker Compose

```yaml
# docker-compose.yml
services:
  fakecloud:
    image: faiscadev/fakecloud
    ports:
      - "4566:4566"
    environment:
      FAKECLOUD_LOG: info
```

```sh
docker compose up
```

FakeCloud is now listening at `http://localhost:4566`.

## Supported Services

### SQS (20 actions)

CreateQueue, DeleteQueue, ListQueues, GetQueueUrl, GetQueueAttributes,
SetQueueAttributes, SendMessage, SendMessageBatch, ReceiveMessage,
DeleteMessage, DeleteMessageBatch, PurgeQueue, ChangeMessageVisibility,
ChangeMessageVisibilityBatch, ListQueueTags, TagQueue, UntagQueue,
AddPermission, RemovePermission, ListDeadLetterSourceQueues

Key features: real MD5 hashing, long polling (WaitTimeSeconds), FIFO queues
with message group ordering and content-based deduplication, dead-letter queues,
message attributes with MD5 computation, batch operations, system attribute
filtering.

### SNS (16 actions)

CreateTopic, DeleteTopic, ListTopics, GetTopicAttributes, SetTopicAttributes,
Subscribe, ConfirmSubscription, Unsubscribe, Publish, ListSubscriptions,
ListSubscriptionsByTopic, GetSubscriptionAttributes, SetSubscriptionAttributes,
TagResource, UntagResource, ListTagsForResource

Key features: SQS fan-out delivery, HTTP/HTTPS endpoint delivery, subscription
filter policies (exact match, prefix, anything-but, numeric, exists).

### EventBridge (15 actions)

CreateEventBus, DeleteEventBus, ListEventBuses, DescribeEventBus, PutRule,
DeleteRule, ListRules, DescribeRule, PutTargets, RemoveTargets,
ListTargetsByRule, PutEvents, TagResource, UntagResource, ListTagsForResource

Key features: pattern-based rules (nested fields, numeric comparisons, prefix,
exists, anything-but), scheduled rules (rate and cron expressions) that actually
fire, targets deliver to SNS topics and SQS queues.

### IAM / STS (16 actions)

**IAM:** CreateUser, GetUser, DeleteUser, ListUsers, CreateAccessKey,
DeleteAccessKey, ListAccessKeys, CreateRole, GetRole, DeleteRole, ListRoles,
CreatePolicy, ListPolicies, AttachRolePolicy

**STS:** GetCallerIdentity, AssumeRole

### SSM Parameter Store (28 actions)

**Parameters**: PutParameter, GetParameter, GetParameters, GetParametersByPath,
DeleteParameter, DeleteParameters, DescribeParameters, GetParameterHistory,
LabelParameterVersion, UnlabelParameterVersion

**Tags**: AddTagsToResource, RemoveTagsFromResource, ListTagsForResource

**Documents**: CreateDocument, GetDocument, DeleteDocument, UpdateDocument,
DescribeDocument, UpdateDocumentDefaultVersion, ListDocuments,
DescribeDocumentPermission, ModifyDocumentPermission

**Commands**: SendCommand, ListCommands, GetCommandInvocation,
ListCommandInvocations, CancelCommand

Key features: String / StringList / SecureString types, automatic versioning,
parameter history, hierarchical path queries with recursive option, pagination
with NextToken, labels, version limits, parameter name normalization (leading
slash optional), tag-based filtering.

### Cross-Service Delivery

FakeCloud implements real cross-service message delivery:

- **EventBridge -> SNS -> SQS**: Events matching rules are published to SNS
  topics, which fan out to SQS subscriptions.
- **EventBridge -> SQS**: Rules can target SQS queues directly.
- **SNS -> SQS**: Publishing to an SNS topic delivers to all SQS subscriptions.
- **SNS -> HTTP/HTTPS**: Publishing to an SNS topic delivers to HTTP endpoints.

## SDK Examples

### AWS CLI

```sh
# Point the CLI at FakeCloud
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

# SQS
aws sqs create-queue --queue-name my-queue
aws sqs send-message --queue-url http://localhost:4566/000000000000/my-queue \
    --message-body "hello"
aws sqs receive-message --queue-url http://localhost:4566/000000000000/my-queue

# SNS
aws sns create-topic --name my-topic
aws sns subscribe --topic-arn arn:aws:sns:us-east-1:000000000000:my-topic \
    --protocol sqs \
    --notification-endpoint arn:aws:sqs:us-east-1:000000000000:my-queue

# SSM
aws ssm put-parameter --name /app/db-host --value "localhost" --type String
aws ssm get-parameter --name /app/db-host
```

### Python (boto3)

```python
import boto3

session = boto3.Session(
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)

sqs = session.client("sqs", endpoint_url="http://localhost:4566")
queue = sqs.create_queue(QueueName="demo-queue")
sqs.send_message(QueueUrl=queue["QueueUrl"], MessageBody="hello from python")
response = sqs.receive_message(QueueUrl=queue["QueueUrl"])
print(response["Messages"][0]["Body"])
```

### Node.js (aws-sdk v3)

```javascript
import { SQSClient, CreateQueueCommand, SendMessageCommand, ReceiveMessageCommand } from "@aws-sdk/client-sqs";

const sqs = new SQSClient({
  endpoint: "http://localhost:4566",
  region: "us-east-1",
  credentials: { accessKeyId: "test", secretAccessKey: "test" },
});

const { QueueUrl } = await sqs.send(new CreateQueueCommand({ QueueName: "demo-queue" }));
await sqs.send(new SendMessageCommand({ QueueUrl, MessageBody: "hello from node" }));
const { Messages } = await sqs.send(new ReceiveMessageCommand({ QueueUrl }));
console.log(Messages[0].Body);
```

### Go (aws-sdk-go-v2)

```go
package main

import (
    "context"
    "fmt"
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
)

func main() {
    cfg, _ := config.LoadDefaultConfig(context.TODO(),
        config.WithRegion("us-east-1"),
        config.WithBaseEndpoint("http://localhost:4566"),
    )
    client := sqs.NewFromConfig(cfg)

    out, _ := client.CreateQueue(context.TODO(), &sqs.CreateQueueInput{
        QueueName: aws.String("demo-queue"),
    })
    client.SendMessage(context.TODO(), &sqs.SendMessageInput{
        QueueUrl:    out.QueueUrl,
        MessageBody: aws.String("hello from go"),
    })
    recv, _ := client.ReceiveMessage(context.TODO(), &sqs.ReceiveMessageInput{
        QueueUrl: out.QueueUrl,
    })
    fmt.Println(*recv.Messages[0].Body)
}
```

### Rust (aws-sdk-rust)

```rust
use aws_sdk_sqs::{Client, Config};
use aws_types::region::Region;

#[tokio::main]
async fn main() {
    let config = Config::builder()
        .region(Region::new("us-east-1"))
        .endpoint_url("http://localhost:4566")
        .behavior_version_latest()
        .build();
    let client = Client::from_conf(config);

    let queue = client.create_queue()
        .queue_name("demo-queue")
        .send().await.unwrap();
    let queue_url = queue.queue_url().unwrap();

    client.send_message()
        .queue_url(queue_url)
        .message_body("hello from rust")
        .send().await.unwrap();

    let msgs = client.receive_message()
        .queue_url(queue_url)
        .send().await.unwrap();
    println!("{}", msgs.messages()[0].body().unwrap());
}
```

## Configuration

FakeCloud is configured via CLI flags or environment variables.

| Flag | Env Var | Default | Description |
|---|---|---|---|
| `--addr` | `FAKECLOUD_ADDR` | `0.0.0.0:4566` | Listen address and port |
| `--region` | `FAKECLOUD_REGION` | `us-east-1` | AWS region to advertise |
| `--account-id` | `FAKECLOUD_ACCOUNT_ID` | `000000000000` | AWS account ID |
| `--log-level` | `FAKECLOUD_LOG` | `info` | Log level (trace, debug, info, warn, error) |

```sh
# Examples
fakecloud-server --addr 127.0.0.1:5000 --log-level debug
FAKECLOUD_LOG=trace cargo run --bin fakecloud-server
```

## Health Check

```sh
curl http://localhost:4566/_fakecloud/health
```

```json
{
  "status": "ok",
  "version": "0.1.0",
  "services": ["sqs", "sns", "events", "iam", "sts", "ssm"]
}
```

## Architecture

FakeCloud is organized as a Cargo workspace:

| Crate | Purpose |
|---|---|
| `fakecloud-server` | Binary entry point (clap CLI, Axum HTTP server) |
| `fakecloud-core` | `AwsService` trait, service registry, request dispatch, protocol parsing |
| `fakecloud-aws` | Shared AWS types (ARNs, error builders, SigV4 parser) |
| `fakecloud-sqs` | SQS implementation |
| `fakecloud-sns` | SNS implementation with delivery |
| `fakecloud-eventbridge` | EventBridge implementation with scheduler |
| `fakecloud-iam` | IAM and STS implementation |
| `fakecloud-ssm` | SSM Parameter Store implementation |
| `fakecloud-e2e` | End-to-end tests using aws-sdk-rust |

Protocol handling:
- **Query protocol** (SQS, SNS, IAM, STS): form-encoded body, `Action` parameter, XML responses
- **JSON protocol** (SSM, EventBridge): JSON body, `X-Amz-Target` header, JSON responses
- SigV4 signatures are parsed for service routing but never validated

## Testing

```sh
cargo test --workspace              # unit tests
cargo build && cargo test -p fakecloud-e2e  # E2E tests (86 tests)
cargo clippy --workspace -- -D warnings     # lint
cargo fmt --check                           # format check
```

E2E tests use the official `aws-sdk-rust` crates and spawn a real FakeCloud
server per test.

## Contributing

Contributions are welcome. FakeCloud is still in early development (Phase 1).

1. Fork the repository
2. Create a feature branch: `git checkout -b feat/my-feature`
3. Follow existing conventions:
   - Conventional commits (`feat:`, `fix:`, `chore:`, `test:`, `refactor:`)
   - Per-action error enums with `thiserror` (no god enums)
   - Add E2E tests for new actions
4. Run the full test suite: `cargo test --workspace`
5. Open a pull request

### Planned services (Phase 2)

S3, DynamoDB, Lambda, CloudWatch Logs, Secrets Manager.

## License

FakeCloud is licensed under the [GNU Affero General Public License v3.0](https://www.gnu.org/licenses/agpl-3.0.html) (AGPL-3.0-or-later).

This means you can freely use, modify, and distribute FakeCloud, but if you run
a modified version as a network service, you must make the source code available
to users of that service.

---

<p align="center">
  Built by <a href="https://github.com/faiscadev">faiscadev</a> | <a href="https://fakecloud.dev">fakecloud.dev</a>
</p>
