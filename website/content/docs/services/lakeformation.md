+++
title = "AWS Lake Formation"
description = "AWS Lake Formation (lakeformation) on fakecloud: a complete 61-operation implementation (100% conformance) — LF-tags, fine-grained permission grants, registered resources, data lake settings, governance transactions, and data-cell filters. restJson1."
weight = 64
+++

fakecloud implements **AWS Lake Formation** as a restJson1 service (sigv4
signing name `lakeformation`). All **61 operations** ship with **100%
conformance** against AWS's own Smithy model, backed by account-partitioned
state that persists across restarts in persistent mode.

Lake Formation is the data-lake governance layer over Glue. fakecloud models
the whole control plane as real, persisted CRUD; the surfaces that require a
backing Glue catalogue or query engine (temporary-credential vending, query
planning) return well-formed synthetic values.

## LF-tags and tag-based access

`CreateLFTag` / `GetLFTag` / `UpdateLFTag` / `DeleteLFTag` / `ListLFTags`
manage the tag ontology; `CreateLFTagExpression` and friends manage reusable
expressions. `AddLFTagsToResource` / `RemoveLFTagsFromResource` /
`GetResourceLFTags` attach and read tags on Glue resources (catalog, database,
table, table-with-columns), and `SearchDatabasesByLFTags` /
`SearchTablesByLFTags` resolve resources by expression.

## Fine-grained permissions

`GrantPermissions` / `RevokePermissions` (and the `BatchGrantPermissions` /
`BatchRevokePermissions` bulk forms, which return per-entry failure lists)
record `(principal, resource, permissions)` grants. `ListPermissions` filters
by principal, resource, or type, and `GetEffectivePermissionsForPath` returns
the grants for a resource path.

## Registered resources + data lake settings

`RegisterResource` binds an S3 location to a role (`DeregisterResource`,
`DescribeResource`, `ListResources`, `UpdateResource`); `RoleArn`,
`HybridAccessEnabled`, and `WithFederation` round-trip exactly.
`PutDataLakeSettings` / `GetDataLakeSettings` persist the data-lake admins,
default database/table permissions, and parameters verbatim.

## Governance transactions + filters

`StartTransaction` returns a transaction id that `CommitTransaction` /
`CancelTransaction` / `ExtendTransaction` / `DescribeTransaction` /
`ListTransactions` drive through its lifecycle (settling synchronously).
Data-cell filters (`CreateDataCellsFilter` + get/update/delete/list), opt-ins,
Identity Center configuration, governed-table objects
(`GetTableObjects`/`UpdateTableObjects`), and storage optimizers are all real
persisted state.

## Synthetic surfaces

Temporary-credential vending (`GetTemporaryGlueTableCredentials`,
`GetTemporaryGluePartitionCredentials`, `GetTemporaryDataLocationCredentials`,
`AssumeDecoratedRoleWithSAML`) returns well-formed STS-style credentials, and
query planning (`StartQueryPlanning` -> `GetQueryState` / `GetWorkUnits` /
`GetWorkUnitResults`) settles to a terminal state — there is no backing Glue
query engine, matching how fakecloud frames other control-plane mocks. Requests
are validated (`@length` / `@range` / enum) so malformed input gets the same
`InvalidInputException` AWS returns. All state is account-partitioned and
persisted.
