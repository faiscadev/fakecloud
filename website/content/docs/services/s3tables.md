+++
title = "Amazon S3 Tables"
description = "Amazon S3 Tables (s3tables) on fakecloud: a complete 49-operation implementation (100% conformance) — table buckets, namespaces, Iceberg tables, metadata-location pointers with version-token concurrency, and every encryption/policy/maintenance/replication sub-resource. restJson1."
weight = 63
+++

fakecloud implements **Amazon S3 Tables** as a restJson1 service (sigv4
signing name `s3tables`). All **49 operations** ship with **100% conformance**
against AWS's own Smithy model, backed by account-partitioned state that
persists across restarts in persistent mode.

S3 Tables organizes Apache-Iceberg tables under a **table bucket ->
namespace -> table** hierarchy. fakecloud models the whole control plane; the
Iceberg data itself is tracked as an opaque metadata-location pointer (no
Iceberg query engine runs), the same way the real service references metadata
in S3.

## The bucket -> namespace -> table hierarchy

- **Table buckets** — `CreateTableBucket` mints an ARN
  (`arn:aws:s3tables:<region>:<account>:bucket/<name>`);
  `GetTableBucket` / `ListTableBuckets` / `DeleteTableBucket` round-trip.
- **Namespaces** — `CreateNamespace` / `GetNamespace` / `ListNamespaces`
  (paginated) / `DeleteNamespace`, scoped to a table bucket.
- **Tables** — `CreateTable` seeds a table (format `ICEBERG`, a
  `metadataLocation`, and a `versionToken`); `GetTable` (by name or table ARN),
  `ListTables` (paginated, namespace/prefix filtered), `RenameTable` (new name
  and/or namespace), and `DeleteTable` complete the lifecycle.

## Metadata location + optimistic concurrency

`GetTableMetadataLocation` returns the stored Iceberg metadata pointer;
`UpdateTableMetadataLocation` sets a new one. Every table mutation rotates the
table's `versionToken`, and `UpdateTableMetadataLocation` / `DeleteTable`
enforce it — a supplied token that no longer matches raises
`ConflictException`, exactly as AWS does for concurrent writers.

## Sub-resource configuration

Every per-bucket and per-table sub-resource is real (`Put` stores, `Get`
returns, `Delete` removes):

- Encryption (`GetTableBucketEncryption` / `PutTableBucketEncryption` /
  `DeleteTableBucketEncryption`, plus the per-table variants).
- Resource policies (`PutTableBucketPolicy` / `PutTablePolicy` + get/delete).
- Maintenance configuration + `GetTableBucketMaintenanceConfiguration` /
  `GetTableMaintenanceConfiguration` and `GetTableMaintenanceJobStatus`.
- Metrics configuration, replication (+ `GetTableReplicationStatus`), storage
  class, and record-expiration configuration + `GetTableRecordExpirationJobStatus`.

Maintenance, record-expiration, and replication **jobs** settle to a terminal
status synchronously — a control-plane mock runs no real compaction or
expiration engine.

## Validation + persistence

Model-derived `@length` / `@pattern` / enum / required-member validation is
enforced, so malformed requests get the same `BadRequestException` /
`NotFoundException` / `ConflictException` AWS returns. All state is
account-partitioned and persisted across restarts in persistent mode.
