//! Amazon Timestream (`timestream`) awsJson1_0 service for fakecloud.
//!
//! One crate serves BOTH the Timestream **Write** and Timestream **Query**
//! SDK clients. AWS ships them as two separate SDK clients
//! (`aws-sdk-timestreamwrite` / `aws-sdk-timestreamquery`), but both share the
//! same service family and the SAME awsJson1_0 target prefix
//! `Timestream_20181101.<Operation>`, so fakecloud routes both to this single
//! handler over a shared, account-partitioned state store: databases and
//! tables are created on the write side and read back by the query side.
//!
//! 30 distinct operations (19 write + 15 query, with `DescribeEndpoints`,
//! `ListTagsForResource`, `TagResource`, and `UntagResource` shared):
//!
//! * **Databases** -- `CreateDatabase` mints
//!   `arn:aws:timestream:<region>:<account>:database/<name>` and stores the
//!   optional `KmsKeyId`, `TableCount`, `CreationTime`; `DescribeDatabase` /
//!   `ListDatabases` / `UpdateDatabase` / `DeleteDatabase` (deleting a database
//!   that still holds tables is a `ValidationException`).
//! * **Tables** -- `CreateTable` under a database, ARN
//!   `.../database/<db>/table/<name>`, `RetentionProperties`,
//!   `MagneticStoreWriteProperties`, and `Schema` (composite partition key)
//!   echoed back; status `ACTIVE`; `Describe` / `List` (filtered by
//!   `DatabaseName`) / `Update` / `Delete`.
//! * **Ingestion** -- `WriteRecords` validates each record (dimensions,
//!   measure name/value/type, time/unit, version) against the table, merges
//!   `CommonAttributes`, stores the points in a bounded in-memory buffer so
//!   they are queryable, and returns `RecordsIngested`. Malformed records come
//!   back as `RejectedRecords` inside a `RejectedRecordsException`.
//! * **Query** -- `Query` runs a real, bounded SQL handler over the ingested
//!   points (see "Honest gap" below), `PrepareQuery` returns `ColumnInfo` +
//!   `Parameters`, `CancelQuery` tears down an in-flight query id. Round-trips
//!   a `NextToken`.
//! * **Scheduled queries** -- full CRUD (`Create` / `Describe` / `List` /
//!   `Update` / `Delete`) plus `ExecuteScheduledQuery`, state persisted.
//! * **Batch load** -- `CreateBatchLoadTask` / `Describe` / `List` / `Resume`.
//! * **Account settings** -- `DescribeAccountSettings` / `UpdateAccountSettings`
//!   (`MaxQueryTCU`, `QueryPricingModel`).
//! * **Endpoint discovery** -- `DescribeEndpoints` returns an `Endpoints` list
//!   whose `Address` points back at the fakecloud host serving the request
//!   (derived from the `Host` header) with a large `CachePeriodInMinutes`, so a
//!   real SDK's discovery step resolves back to fakecloud.
//! * **Tagging** -- ARN-keyed `TagResource` / `UntagResource` /
//!   `ListTagsForResource`.
//!
//! Every operation runs model-driven input validation first (required / enum /
//! range), then real, account-partitioned, persisted behavior.
//!
//! Honest gap -- the query SQL subset: a faithful Timestream SQL engine (the
//! full Trino-derived time-series dialect) is out of scope. [`query`] implements
//! a real, bounded interpreter for the shapes the SDK/e2e/conformance exercise:
//! `SELECT * FROM "db"."table"`, `SELECT COUNT(*) FROM "db"."table"`, an
//! optional `WHERE time > ago(<n><unit>)` / `WHERE time <op> <literal>` time
//! filter, `ORDER BY time [ASC|DESC]`, and `LIMIT <n>`. Ingested points come
//! back as rows with AWS-shaped `ColumnInfo`/`Datum` typing (one column per
//! dimension, `measure_name`, `measure_value::<type>`, `time`). Query shapes
//! beyond this subset return a well-formed `ValidationException` naming the
//! unsupported construct rather than a wrong or empty success. Endpoint
//! discovery's optional auto-mode in the AWS SDK forces `https://<address>`, so
//! the e2e drives the default (non-discovery) client pinned to fakecloud and
//! calls `DescribeEndpoints` explicitly to prove the host echo.

pub mod persistence;
pub mod query;
pub mod service;
pub mod shared;
pub mod state;
mod validate;

pub use service::{TimestreamService, TIMESTREAM_ACTIONS};
pub use state::{
    SharedTimestreamState, TimestreamData, TimestreamSnapshot, TIMESTREAM_SNAPSHOT_SCHEMA_VERSION,
};
