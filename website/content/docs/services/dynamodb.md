+++
title = "DynamoDB"
description = "Tables, items, transactions, PartiQL, backups, global tables, streams, TTL."
weight = 6
+++

fakecloud implements **57 of 57** DynamoDB operations at 100% Smithy conformance.

## Supported features

- **Tables** — CRUD, attributes, indexes (GSI, LSI), billing modes, tags
- **Items** — GetItem, PutItem, UpdateItem, DeleteItem, BatchGetItem, BatchWriteItem
- **Transactions** — TransactGetItems, TransactWriteItems with conditional checks
- **Query and Scan** — full expression support (key conditions, filter expressions)
- **PartiQL** — ExecuteStatement, BatchExecuteStatement, ExecuteTransaction
- **Update expressions** — SET, REMOVE, ADD, DELETE with function support (`size`, `attribute_exists`, `begins_with`, `contains`, `attribute_type`)
- **Condition expressions** — full operator support with correct type coercion
- **Global tables** — replica management, replica status reporting
- **Backups** — CreateBackup, DescribeBackup, RestoreTableFromBackup
- **Streams** — shard iterators, record retrieval, delivery to Lambda/Kinesis
- **TTL** — expire items via `/_fakecloud/dynamodb/ttl-processor/tick`
- **Exports and imports** — S3 exports (recorded), S3 imports (recorded)
- **ConsumedCapacity + ItemCollectionMetrics** — every data-plane op (`GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`, `Query`, `Scan`, `BatchGetItem`, `BatchWriteItem`, `TransactGetItems`, `TransactWriteItems`, PartiQL variants) returns `ConsumedCapacity` when the caller requests it via `ReturnConsumedCapacity = TOTAL` / `INDEXES`. Capacity units are synthesized from the serialized item byte size using AWS's documented 4 KB read / 1 KB write rounding, broken out per table + per index. `ItemCollectionMetrics` is emitted on writes touching tables that have a local secondary index, with `SizeEstimateRangeGB` rounded to the AWS-documented `[lower, upper]` shape
- **`TableName` accepts ARNs** — every operation that takes a `TableName` parameter also accepts the full `arn:aws:dynamodb:<region>:<account>:table/<name>` form, and resolves it back to the local table. The same applies to global secondary index identifiers when an ARN form is supplied. Matches the real AWS API change that landed in 2024 so cross-region / cross-account SDK call patterns work without rewriting test fixtures

## Protocol

JSON protocol. `X-Amz-Target` header, JSON body, JSON responses.

## Introspection

- `POST /_fakecloud/dynamodb/ttl-processor/tick` — expire items whose TTL attribute is in the past
- `POST /_fakecloud/dynamodb/snapshot/save` — write the current DynamoDB state as a canonical snapshot on demand. An optional JSON body `{"dataPath": "<dir>"}` writes to `<dir>/dynamodb/snapshot.json`; with no body it writes to the configured persistent store. Returns `{"saved": true}`, `400` when neither a store nor `dataPath` is available, and `500` on write failure. Lets import/export tooling populate DynamoDB through the normal API and then have fakecloud emit the canonical snapshot format instead of reproducing snapshot internals out of tree.

## Cross-service delivery

- **DynamoDB Streams -> Lambda** — Event source mapping polls and invokes
- **DynamoDB -> Kinesis** — Table changes stream to Kinesis Data Streams

## Source

- [`crates/fakecloud-dynamodb`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-dynamodb)
- [AWS DynamoDB API reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/Welcome.html)
