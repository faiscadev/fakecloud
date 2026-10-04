+++
title = "SQS"
description = "FIFO queues, dead-letter queues, long polling, batch operations."
weight = 2
+++

fakecloud implements **23 of 23** SQS operations at 100% Smithy conformance.

## Supported features

- **Queue management** — CRUD, attributes, tags
- **FIFO queues** — deduplication, ordering, message group IDs
- **Dead-letter queues** — redrive policies, `maxReceiveCount` enforcement
- **Long polling** — `WaitTimeSeconds` on Receive
- **Batch operations** — SendMessageBatch, DeleteMessageBatch, ChangeMessageVisibilityBatch
- **MD5 hashing** — body and attribute MD5s returned exactly as AWS does
- **Message retention** — expiration via `/_fakecloud/sqs/expiration-processor/tick`
- **Visibility timeout** — ChangeMessageVisibility, per-receive timeout

## Regions

Queues are regional. The same queue name can exist independently in every region of an account, and `ListQueues`, `GetQueueUrl` and every queue-addressed call see only the queues of the region the request is signed for. A fakecloud QueueUrl (`<endpoint>/<account>/<name>`) carries no region, since one endpoint serves every region, so the same URL addresses a different queue in each region; the queue ARN (`arn:aws:sqs:<region>:<account>:<name>`) always names its own region. Deliveries by queue ARN (SNS, EventBridge, S3 notifications, Lambda event source mappings, Pipes) land in the ARN's region and account. A Step Functions `sqs:sendMessage` task resolves its `QueueUrl` in the execution's region first, and otherwise in the one other region that has a queue of that account and name.

## Protocol

Query protocol. Form-encoded body, `Action` parameter, XML responses.

## Introspection

- `GET /_fakecloud/sqs/messages` — list all messages across all queues
- `POST /_fakecloud/sqs/expiration-processor/tick` — expire messages past retention
- `POST /_fakecloud/sqs/{queue_name}/force-dlq` — force-move messages exceeding `maxReceiveCount` to DLQ

## Cross-service delivery

- **SQS -> Lambda** — Event source mapping polls and invokes

## Source

- [`crates/fakecloud-sqs`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-sqs)
- [AWS SQS API reference](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/APIReference/Welcome.html)
