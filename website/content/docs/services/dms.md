+++
title = "Database Migration Service"
description = "AWS Database Migration Service (dms) control plane on fakecloud: replication instances, endpoints, replication tasks, subnet groups, event subscriptions, certificates, serverless replication configs, data providers, instance profiles, migration projects, schema conversion, and Fleet Advisor — real account-partitioned, persisted state."
weight = 52
+++

fakecloud implements the **AWS Database Migration Service** (`dms`) control
plane. The full **119-operation awsJson1.1 API** ships now: replication
instances, endpoints, replication tasks, subnet groups, event subscriptions,
certificates, serverless replication configs, data providers, instance profiles,
migration projects, schema-conversion / metadata-model requests, Fleet Advisor,
recommendations, account attributes, and tagging. Every resource is real,
account-partitioned state that persists across restarts in persistent mode, so
what one session creates the next session still sees.

DMS is a control-plane emulator: there is no real data-migration engine — the
actual movement of rows between a source and target database is out of scope,
the same way LocalStack Community mocks DMS. Everything up to and including the
control plane that drives a migration (creating and configuring the instances,
endpoints, tasks, and configs, testing connections, and transitioning task
state) is real.

## Supported features

- **Replication instances** (`CreateReplicationInstance`,
  `DescribeReplicationInstances`, `ModifyReplicationInstance`,
  `DeleteReplicationInstance`, `RebootReplicationInstance`). New instances get an
  `arn:aws:dms:<region>:<acct>:rep:<id>` ARN, a synthesized subnet group, and
  settle straight to `available` so `replication-instance-available` waiters
  complete.
- **Endpoints** (`CreateEndpoint`, `DescribeEndpoints`, `ModifyEndpoint`,
  `DeleteEndpoint`). Per-engine settings blobs (`S3Settings`, `KinesisSettings`,
  `PostgreSQLSettings`, `MongoDbSettings`, ...) round-trip verbatim.
- **Connections** (`TestConnection`, `DeleteConnection`, `DescribeConnections`).
  `TestConnection` validates the referenced instance and endpoint exist and
  returns a `successful` connection.
- **Replication tasks** (`CreateReplicationTask`, `DescribeReplicationTasks`,
  `ModifyReplicationTask`, `DeleteReplicationTask`, `StartReplicationTask`,
  `StopReplicationTask`, `MoveReplicationTask`, `StartReplicationTaskAssessment`).
  Tasks transition `ready` -> `running` -> `stopped` synchronously so waiters
  complete. Assessment runs, table statistics, and individual assessments are
  modelled.
- **Replication subnet groups**, **event subscriptions**, and **certificates**
  (`ImportCertificate` + describe + delete) — full CRUD.
- **Serverless replication** (`CreateReplicationConfig`, `StartReplication`,
  `StopReplication`, `DescribeReplications`, `ReloadReplicationTables`) drives the
  serverless replication config lifecycle and its `Replication` state.
- **Schema conversion** — data providers, instance profiles, migration projects,
  conversion configuration, and the `StartMetadataModel*` / `DescribeMetadataModel*`
  request family, plus `ExportMetadataModelAssessment` and Fleet Advisor
  collectors, databases, schemas, and LSA analysis.
- **Recommendations** (`StartRecommendations`, `BatchStartRecommendations`,
  `DescribeRecommendations`, `DescribeRecommendationLimitations`).
- **Account attributes** (`DescribeAccountAttributes` reflects live resource
  counts), the orderable-instance / endpoint-type / engine-version catalogues,
  and **tagging** (`AddTagsToResource`, `RemoveTagsFromResource`,
  `ListTagsForResource`) keyed by resource ARN.

All `Describe` operations honor `Marker`/`MaxRecords` pagination and the DMS
`Filters` list, and `@length`, `@range`, and enum input constraints are enforced
with the model's declared error codes.

## Not implemented

- **The data-migration engine.** DMS on fakecloud does not read from a source
  database and write to a target — no rows are moved. This mirrors LocalStack
  Community, which mocks DMS as a control plane only.

100% conformance: all 3,344 generated Smithy probe variants pass.
