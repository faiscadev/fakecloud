+++
title = "DynamoDB"
description = "Tables, items, transactions, PartiQL, backups, global tables, streams, TTL."
weight = 6
+++

fakecloud implements **57 of 57** DynamoDB operations at 100% Smithy conformance.

## Supported features

- **Tables** — CRUD, attributes, indexes (GSI, LSI), billing modes, tags, resource-based policies on tables and streams (`PutResourcePolicy` / `GetResourcePolicy` / `DeleteResourcePolicy` with `ExpectedRevisionId`, `CreateTable` `ResourcePolicy`)
- **Items** — GetItem, PutItem, UpdateItem, DeleteItem, BatchGetItem, BatchWriteItem
- **Transactions** — TransactGetItems, TransactWriteItems with conditional checks
- **Query and Scan** — full expression support (key conditions, filter expressions)
- **PartiQL** — ExecuteStatement, BatchExecuteStatement, ExecuteTransaction; `SELECT ... FROM "table"."index"` reads the index (rows carrying its key, with its projected attributes)
- **Update expressions** — SET, REMOVE, ADD, DELETE with function support (`size`, `attribute_exists`, `begins_with`, `contains`, `attribute_type`)
- **Condition expressions** — full operator support with correct type coercion
- **Global tables** — replica management, replica status reporting
- **Backups** — CreateBackup, DescribeBackup, RestoreTableFromBackup
- **Streams** — shard iterators, record retrieval, delivery to Lambda/Kinesis
- **TTL** — expire items via `/_fakecloud/dynamodb/ttl-processor/tick`
- **Exports and imports**: background jobs that report `IN_PROGRESS -> COMPLETED`/`FAILED`. `ExportTableToPointInTime` writes the AWS export layout (`AWSDynamoDB/<id>/` with `manifest-summary.json`, `manifest-files.json` and gzip `data/*.json.gz` or `*.ion.gz`) in DynamoDB JSON or Ion. `ImportTable` creates the table as `CREATING`, reads DynamoDB JSON, Ion or CSV objects with `InputCompressionType` GZIP, ZSTD or NONE, applies the `TableCreationParameters` (billing mode, throughput, GSIs, SSE), and flips the table `ACTIVE`; a table exported here re-imports to identical items
- **ConsumedCapacity + ItemCollectionMetrics** — every data-plane op (`GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`, `Query`, `Scan`, `BatchGetItem`, `BatchWriteItem`, `TransactGetItems`, `TransactWriteItems`, PartiQL variants) returns `ConsumedCapacity` when the caller requests it via `ReturnConsumedCapacity = TOTAL` / `INDEXES`. Capacity units are synthesized from the serialized item byte size using AWS's documented 4 KB read / 1 KB write rounding, broken out per table + per index. `ItemCollectionMetrics` is emitted on writes touching tables that have a local secondary index, with `SizeEstimateRangeGB` rounded to the AWS-documented `[lower, upper]` shape
- **IAM enforcement** — with `FAKECLOUD_IAM=strict` (or `soft`), every DynamoDB and DynamoDB Streams operation is authorized against the caller's policies using the actions and resource ARNs AWS uses: the table, index, stream, backup, export or import ARN; the batch action on every table in a batch; the per-item action on each table in a transaction; `PartiQLSelect` / `PartiQLInsert` / `PartiQLUpdate` / `PartiQLDelete` for PartiQL; `aws:ResourceTag` / `aws:RequestTag` / `aws:TagKeys` conditions on table tags; fine-grained access control through `dynamodb:LeadingKeys`, `dynamodb:Attributes`, `dynamodb:Select`, `dynamodb:ReturnValues`, `dynamodb:ReturnConsumedCapacity`, `dynamodb:EnclosingOperation` and `dynamodb:FullTableScan`; and table and stream resource-based policies, whose explicit Deny wins and whose Allow grants same-account principals on its own. See [SigV4 verification and IAM enforcement](@/docs/reference/security.md)
- **`TableName` accepts ARNs, including other accounts' tables**: every operation that takes a `TableName` (or `ResourceArn`, `StreamArn`) also accepts the full `arn:aws:dynamodb:<region>:<account>:table/<name>` form. An ARN naming another account reaches that account's table for the operations AWS supports cross-account: `GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`, `Query`, `Scan`, `BatchGetItem`, `BatchWriteItem`, `TransactGetItems`, `TransactWriteItems` (a batch or transaction can mix tables from several accounts, and a transaction stays atomic across them), `DescribeTable`, `UpdateTable`, `DeleteTable`, `ListTagsOfResource`, `TagResource`, `UntagResource`, and the Streams `DescribeStream`, `GetShardIterator` and `GetRecords`. With IAM enforcement on, such a request needs both the caller's identity policy and the table's (or stream's) resource-based policy. Any other operation (PartiQL, backups, point-in-time recovery, TTL, Kinesis streaming, resource-policy operations, imports and exports) does not find another account's table, and an ARN naming a region other than the request's is not found for any operation

## Protocol

JSON protocol. `X-Amz-Target` header, JSON body, JSON responses.

## Introspection

- `POST /_fakecloud/dynamodb/ttl-processor/tick` — expire items whose TTL attribute is in the past
- `POST /_fakecloud/dynamodb/snapshot/save` — write the current DynamoDB state as a canonical snapshot on demand. An optional JSON body `{"dataPath": "<dir>"}` writes to `<dir>/dynamodb/snapshot.json`; with no body it writes to the configured persistent store. Returns `{"saved": true}`, `400` when neither a store nor `dataPath` is available, and `500` on write failure. Lets import/export tooling populate DynamoDB through the normal API and then have fakecloud emit the canonical snapshot format instead of reproducing snapshot internals out of tree.

## Importing an AWS export at startup

Seed one or many local tables from real DynamoDB S3 exports, bulk-loaded directly into the store before the server starts serving. Single-table and multi-table each use their own flag — the two are mutually exclusive (startup aborts if both are set).

### Single-table mode

- `--dynamodb-import-path` (`FAKECLOUD_DYNAMODB_IMPORT_PATH`) — the local `AWSDynamoDB/<export-id>/` folder that holds `manifest-summary.json` (as produced by an AWS DynamoDB S3 export).
- `--dynamodb-import-describe-table` (`FAKECLOUD_DYNAMODB_DESCRIBE_TABLE`) — an `aws dynamodb describe-table` JSON dump supplying the table shape (key schema, attribute definitions, indexes, billing mode).

Both are required together.

```sh
fakecloud \
  --dynamodb-import-path ./AWSDynamoDB/01234567890123-abcdef01 \
  --dynamodb-import-describe-table ./describe-table.json
```

### Multi-table mode

- `--dynamodb-import-dir` (`FAKECLOUD_DYNAMODB_IMPORT_DIR`) — a root directory of per-table subdirectories, each self-contained with its own `describe-table.json` alongside that table's `manifest-summary.json` / `manifest-files.json` / `data/*.json.gz`. Subdirectory names carry no meaning — each table's name comes from its own `describe-table.json`.

```
root/
  Music/
    describe-table.json
    manifest-summary.json
    manifest-files.json
    data/0001.json.gz
  Orders/
    describe-table.json
    manifest-summary.json
    manifest-files.json
    data/0001.json.gz
```

```sh
fakecloud --dynamodb-import-dir ./root
```

Every subdirectory is imported using the same rules as single-table mode.

### Constraints (both modes)

- **`--dynamodb-import-describe-table` alone, without `--dynamodb-import-path`, aborts startup** — it has nothing to pair with.
- **`--dynamodb-import-path`/`--dynamodb-import-describe-table` and `--dynamodb-import-dir` are mutually exclusive** — passing both aborts startup before any import runs.
- **Idempotent, per table.** Each import creates a new table. If a table of that name already exists, that table's import is skipped with a warning and its existing data is left untouched (no merge, no append, no overwrite). This makes restarting with the flags still set safe.
- **Additive:** tables are materialised straight in the store. They do not go through `BatchWriteItem` and do not touch the modeled `ImportTable` API operation.
- **Targets the default (single) account** named by `--account-id` in the configured region.
- Only the AWS **`DYNAMODB_JSON`** export format is supported (manifests plus gzipped `data/*.json.gz` files); ION and CSV are not.
- Every imported item must carry the key attributes declared in its describe-table `KeySchema` with the type declared in `AttributeDefinitions` (the same presence and type checks the normal write path enforces). If a table's manifests declare an `itemCount` that disagrees with the data actually read, the whole import is rejected as truncated or corrupt. Any bad or unreadable input — including a multi-table root with no subdirectories, or a subdirectory missing its `describe-table.json` — aborts startup loudly before any table is written to state.
- Works in either storage mode. Under `--storage-mode=persistent` imported tables are persisted like any other state (written once after the whole batch, not per table), so on a later restart they're already present and skipped (see the idempotent behavior above) rather than re-imported.

## Cross-service delivery

- **DynamoDB Streams -> Lambda** — Event source mapping polls and invokes
- **DynamoDB -> Kinesis** — Table changes stream to Kinesis Data Streams

## Source

- [`crates/fakecloud-dynamodb`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-dynamodb)
- [AWS DynamoDB API reference](https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/Welcome.html)
