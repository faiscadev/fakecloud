+++
title = "Amazon Timestream"
description = "Amazon Timestream on fakecloud: one crate serving BOTH the Timestream Write and Timestream Query SDK clients over a shared store -- databases, tables, WriteRecords ingestion, a bounded SQL Query engine, scheduled queries, batch-load tasks, account settings, and endpoint discovery that resolves back to fakecloud -- with account-partitioned persistence."
weight = 79
+++

fakecloud implements **Amazon Timestream** (`timestream`), the serverless
time-series database. AWS ships it as two separate SDK clients --
`aws-sdk-timestreamwrite` and `aws-sdk-timestreamquery` -- but they belong to
the same service family and, in the wire protocol, share the **same**
awsJson1_0 target prefix `Timestream_20181101.<Operation>`. fakecloud serves
both from a single crate over one account-partitioned store, so a database or
table created through the Write client is read straight back by the Query
client. All **30 distinct operations** ship now (19 write + 15 query, with
`DescribeEndpoints`, `ListTagsForResource`, `TagResource`, and `UntagResource`
shared), backed by state that persists across restarts in persistent mode.

Every operation runs model-driven input validation first (`required` /
`length` / `range` / `enum`), then real, persisted behavior -- no stubbed
success responses. The declared exceptions are used throughout:
`ValidationException`, `ResourceNotFoundException`, `ConflictException`,
`RejectedRecordsException`, `ThrottlingException`, `AccessDeniedException`,
`InternalServerException`, `ServiceQuotaExceededException`, and
`InvalidEndpointException`.

## Endpoint discovery

Timestream SDKs use endpoint discovery: the client can call `DescribeEndpoints`
and route subsequent calls to the returned address. fakecloud's
`DescribeEndpoints` returns an `Endpoints` list whose `Address` points back at
the fakecloud host serving the request (derived from the `Host` header) with a
large `CachePeriodInMinutes` (1440), so a real SDK's discovery step resolves
back to fakecloud rather than to a real AWS endpoint.

The end-to-end tests drive the real `aws-sdk-timestreamwrite` and
`aws-sdk-timestreamquery` clients against a live fakecloud server: create
database -> create table -> `WriteRecords` -> `Query` returns the written rows.
(The AWS SDK's *opt-in* auto-discovery mode rewrites the discovered address to
`https://<address>`, which a plain-HTTP fakecloud server can't serve, so the
tests use the default non-discovery client pinned to the fakecloud endpoint and
call `DescribeEndpoints` explicitly to prove the host echo.)

## Databases and tables

`CreateDatabase` mints `arn:aws:timestream:<region>:<account>:database/<name>`
and stores the optional `KmsKeyId`, a `TableCount`, and `CreationTime`;
`DescribeDatabase` / `ListDatabases` / `UpdateDatabase` / `DeleteDatabase`
round-trip it. Deleting a database that still holds tables is a
`ValidationException`, matching AWS.

`CreateTable` lives under a database (missing database ->
`ResourceNotFoundException`), mints
`arn:aws:timestream:<region>:<account>:database/<db>/table/<name>`, echoes back
`RetentionProperties` (`MemoryStoreRetentionPeriodInHours` +
`MagneticStoreRetentionPeriodInDays`), `MagneticStoreWriteProperties`, and the
`Schema` (composite partition key), and reports status `ACTIVE`. `ListTables`
filters by `DatabaseName`.

## Ingestion

`WriteRecords` validates each record (dimensions, `MeasureName`, `MeasureValue`
/ `MeasureValueType`, `Time` / `TimeUnit`), merges `CommonAttributes` into every
record, normalizes the time to nanoseconds, and stores the points in a bounded
in-memory buffer per table so they become queryable. It returns
`RecordsIngested` (`Total` / `MemoryStore` / `MagneticStore`). Malformed records
are collected and returned as `RejectedRecords` inside a
`RejectedRecordsException` (the batch is rejected atomically, as on AWS).

## Query

`Query` runs a real, bounded SQL handler over the ingested points.
`PrepareQuery` returns the query's `ColumnInfo` plus any named `Parameters`;
`CancelQuery` tears down an in-flight query id. Results paginate with a
round-tripping `NextToken`.

**Honest gap -- the query SQL subset.** A faithful Timestream SQL engine (the
full Trino-derived time-series dialect) is out of scope. The `Query` handler is
a genuine interpreter for the shapes SDKs, the e2e suite, and conformance
exercise:

* `SELECT * FROM "database"."table"` -- every stored point as a row, with
  AWS-shaped `ColumnInfo`/`Datum` typing: one `VARCHAR` column per dimension
  name, `measure_name`, a `measure_value::<type>` column per measure value type
  present, and `time` (`TIMESTAMP`).
* `SELECT COUNT(*) FROM "database"."table"` -- a single `BIGINT` row.
* an optional `WHERE time <op> ago(<n><unit>)` time filter.
* an optional `ORDER BY time [ASC|DESC]`.
* an optional `LIMIT <n>`.

Any construct outside this subset returns a well-formed `ValidationException`
naming the unsupported construct, rather than a wrong or silently-empty success.
There is no real time-series storage engine underneath: ingested points live in
a bounded buffer, not a columnar time-partitioned store.

## Scheduled queries, batch load, and account settings

Scheduled queries support the full lifecycle -- `CreateScheduledQuery` (ARN
`.../scheduled-query/<name>-<id>`), `Describe` / `List` / `Update` (state
`ENABLED` / `DISABLED`) / `Delete`, and `ExecuteScheduledQuery` (records a manual
run). Batch-load tasks support `Create` / `Describe` / `List` / `Resume`.
`DescribeAccountSettings` / `UpdateAccountSettings` round-trip `MaxQueryTCU`,
`QueryPricingModel` (`BYTES_SCANNED` / `COMPUTE_UNITS`), and the provisioned
`QueryCompute` capacity.

## Tagging

`TagResource` / `UntagResource` / `ListTagsForResource` are ARN-keyed and shared
across databases, tables, and scheduled queries.
