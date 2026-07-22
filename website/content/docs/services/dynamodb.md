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
