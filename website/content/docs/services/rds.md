+++
title = "RDS"
description = "Real PostgreSQL, MySQL, MariaDB, Oracle, SQL Server, and Db2 instances via Docker. Snapshots, read replicas, parameter groups."
weight = 17
+++

fakecloud implements **163 of 163** RDS operations at 100% Smithy conformance. DB instances run in **real Docker containers** — your code connects to a real database, not a mock.

## Supported features

- **DB instances** — CreateDBInstance, ModifyDBInstance, DeleteDBInstance, DescribeDBInstances, RebootDBInstance
- **Real engines via Docker** — PostgreSQL, MySQL, MariaDB, Oracle (gvenzl/oracle-free), SQL Server (mssql/server Express), Db2 (db2_community)
- **Snapshots** — automated and manual, CreateDBSnapshot, RestoreDBInstanceFromDBSnapshot, CopyDBSnapshot, DeleteDBSnapshot
- **Read replicas** — CreateDBInstanceReadReplica, PromoteReadReplica
- **Parameter groups** — DBParameterGroup and DBClusterParameterGroup CRUD, parameter management
- **Option groups** — CRUD
- **Subnet groups** — CRUD
- **DB clusters** — Aurora-style clusters with full lifecycle: ModifyDBCluster (every mutable field on AWS's surface, including ServerlessV2 scaling, log-export updates, VPC SGs, NewDBClusterIdentifier rename), StartDBCluster / StopDBCluster (status transitions with `InvalidDBClusterStateFault` validation), RebootDBCluster, FailoverDBCluster (auto-picks a replica when no target is provided, swaps the writer flag, rejects non-member targets when the cluster tracks members), BacktrackDBCluster (Aurora MySQL only — `InvalidParameterCombination` on Aurora PostgreSQL — records BacktrackTo and the change-record count, append-tracked under DescribeDBClusterBacktracks)
- **Events** — DescribeEvents, DescribeEventCategories, DescribeEventSubscriptions
- **Engine discovery** — DescribeDBEngineVersions with real engine metadata
- **Tagging** — AddTagsToResource, RemoveTagsFromResource
- **Dump and restore** — MySQL and MariaDB database dumps for snapshot/restore flows
- **License models** — tracking
- **EventBridge events** — lifecycle ops emit `aws.rds` events on the `default` bus, deliverable to SQS, SNS, Lambda, etc. via standard EB rules
- **PostgreSQL `aws_lambda` extension** — call fakecloud Lambda functions from inside RDS PostgreSQL via `CREATE EXTENSION aws_lambda CASCADE` and `aws_lambda.invoke(...)` (subset of the AWS RDS extension surface; see below)
- **PostgreSQL `aws_s3` extension** — import objects from fakecloud S3 into tables (`aws_s3.table_import_from_s3`) and export query results back to S3 (`aws_s3.query_export_to_s3`); see below
- **MySQL / MariaDB Aurora Lambda bridge** — Aurora-compatible `mysql.lambda_async` / `mysql.lambda_sync` stored procedures invoke fakecloud Lambda functions from inside the DB container; see below

## EventBridge integration

Lifecycle ops emit events matching the AWS event schema (`source: "aws.rds"`, detail-type per source kind):

| Operation                       | EventID         | Source type        | Categories            |
|---------------------------------|-----------------|--------------------|-----------------------|
| `CreateDBInstance`              | RDS-EVENT-0005  | DB_INSTANCE        | creation              |
| `DeleteDBInstance`              | RDS-EVENT-0003  | DB_INSTANCE        | deletion              |
| `ModifyDBInstance`              | RDS-EVENT-0014  | DB_INSTANCE        | configuration change  |
| `RebootDBInstance`              | RDS-EVENT-0006  | DB_INSTANCE        | availability          |
| `StartDBInstance`               | RDS-EVENT-0088  | DB_INSTANCE        | notification          |
| `StopDBInstance`                | RDS-EVENT-0089  | DB_INSTANCE        | notification          |
| `CreateDBInstanceReadReplica`   | RDS-EVENT-0005  | DB_INSTANCE        | creation, read replica|
| `RestoreDBInstanceFromDBSnapshot` | RDS-EVENT-0043 | DB_INSTANCE       | creation              |
| `CreateDBSnapshot`              | RDS-EVENT-0042  | DB_SNAPSHOT        | creation              |
| `DeleteDBSnapshot`              | RDS-EVENT-0041  | DB_SNAPSHOT        | deletion              |
| `ModifyDBCluster`               | RDS-EVENT-0016  | DB_CLUSTER         | configuration change  |
| `RebootDBCluster`               | RDS-EVENT-0006  | DB_CLUSTER         | notification          |
| `StartDBCluster`                | RDS-EVENT-0150  | DB_CLUSTER         | notification          |
| `StopDBCluster`                 | RDS-EVENT-0151  | DB_CLUSTER         | notification          |
| `FailoverDBCluster`             | RDS-EVENT-0072  | DB_CLUSTER         | failover              |
| `BacktrackDBCluster`            | RDS-EVENT-0095  | DB_CLUSTER         | notification          |

Match with an EventBridge rule pattern like:

```json
{ "source": ["aws.rds"], "detail-type": ["RDS DB Instance Event"] }
```

## Protocol

Query protocol. Form-encoded body, `Action` parameter, XML responses.

## Introspection

- `GET /_fakecloud/rds/instances` — list fakecloud-managed DB instances with runtime metadata (container id, host port)
- `POST /_fakecloud/rds/lambda-invoke` — internal bridge used by the PostgreSQL `aws_lambda` extension to invoke fakecloud Lambda functions from inside the DB container
- `POST /_fakecloud/rds/s3-import` / `POST /_fakecloud/rds/s3-export` — internal bridges used by the PostgreSQL `aws_s3` extension to read/write fakecloud S3 objects from inside the DB container

## PostgreSQL `aws_lambda` extension

Matches the AWS RDS extension of the same name. Lets SQL running inside an RDS-managed PostgreSQL instance invoke fakecloud Lambda functions:

```sql
CREATE EXTENSION IF NOT EXISTS aws_lambda CASCADE;

SELECT aws_commons.create_lambda_function_arn('my_function');

SELECT * FROM aws_lambda.invoke(
    'my_function',
    '{"body":"Hello!"}'::json
);
```

Implemented function signatures (subset of the AWS RDS Lambda API):

- `aws_lambda.invoke(function_name text, payload json, region text DEFAULT NULL, invocation_type text DEFAULT 'RequestResponse')` -> returns `(status_code int, payload json, executed_version text, log_result text)`
- `aws_lambda.invoke(function_name aws_commons._lambda_function_arn_1, payload json, region text DEFAULT NULL, invocation_type text DEFAULT 'RequestResponse')` (composite-typed overload)
- `aws_commons.create_lambda_function_arn(function_name text, region text DEFAULT NULL)` -> composite of `(function_name, region)`

`invocation_type = 'Event'` returns `(202, NULL, '$LATEST', NULL)` immediately and runs the Lambda asynchronously.

The first time you create a PostgreSQL DB instance, fakecloud lazily builds a `fakecloud-postgres:<major>-<hash>` Docker image off `postgres:<major>` that bakes in `plpython3u` and the extension files. The build typically takes ~60s and the image is cached locally for subsequent runs (the hash invalidates the cache when fakecloud changes the embedded extension definitions).

Inside the container, the extension's `plpython3u` body POSTs to `http://host.docker.internal:<server_port>/_fakecloud/rds/lambda-invoke`, which routes through fakecloud's standard Lambda invocation path.

## PostgreSQL `aws_s3` extension

Matches the AWS RDS extension of the same name. Lets SQL running inside an RDS-managed PostgreSQL instance read objects from fakecloud S3 directly into tables and write query results back as objects:

```sql
CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;

-- Import a CSV file into a table
SELECT * FROM aws_s3.table_import_from_s3(
    'people',
    '',                           -- column list (empty = all columns)
    'format csv',                 -- COPY options
    aws_commons.create_s3_uri('my-bucket', 'people.csv', 'us-east-1')
);

-- Export a query result back to S3
SELECT * FROM aws_s3.query_export_to_s3(
    'SELECT id, name FROM people ORDER BY id',
    aws_commons.create_s3_uri('my-bucket', 'export.csv', 'us-east-1'),
    'format csv'
);
```

Implemented function signatures (subset of the AWS RDS S3 import/export API):

- `aws_s3.table_import_from_s3(table_name text, column_list text, options text, bucket text, file_path text, region text DEFAULT NULL)` -> returns `(rows_imported bigint, file_compression text, bytes_processed bigint)`
- `aws_s3.table_import_from_s3(table_name text, column_list text, options text, s3_info aws_commons._s3_uri_1)` (composite-typed overload)
- `aws_s3.query_export_to_s3(query text, bucket text, file_path text, region text DEFAULT NULL, options text DEFAULT NULL)` -> returns `(rows_uploaded bigint, files_uploaded bigint, bytes_uploaded bigint)`
- `aws_s3.query_export_to_s3(query text, s3_info aws_commons._s3_uri_1, options text DEFAULT NULL)` (composite-typed overload)
- `aws_commons.create_s3_uri(bucket text, file_path text, region text DEFAULT NULL)` -> composite of `(bucket, file_path, region)`

The `options` argument is forwarded verbatim into the underlying postgres `COPY` `WITH (...)` clause (`format csv`, `header true`, `delimiter ','`, etc.). `file_compression` is always returned empty — fakecloud does not autodetect compression on import; pre-decompress objects before calling.

The bridges (`/_fakecloud/rds/s3-import`, `/_fakecloud/rds/s3-export`) read and write the in-memory S3 state of the same fakecloud server, so any object that's visible to a `GetObject`/`PutObject` call against fakecloud is reachable from `aws_s3`.

## MySQL / MariaDB Aurora Lambda bridge

Aurora MySQL exposes Lambda invocation as built-in stored procedures (`mysql.lambda_async`, `mysql.lambda_sync`). fakecloud's prebuilt `fakecloud-mysql` and `fakecloud-mariadb` images provide the same surface so SQL inside an RDS-managed instance can invoke fakecloud Lambda functions:

```sql
-- Async, fire-and-forget. Returns immediately.
CALL mysql.lambda_async('my_function', '{"k":1}');

-- Synchronous, returns the function payload as a JSON string.
SELECT mysql.lambda_sync('my_function', '{"hello":"world"}');
```

Implemented procedures (subset of the AWS Aurora MySQL Lambda surface):

- `mysql.lambda_async(function_name TEXT, payload TEXT)` — `Event`-style invocation; returns nothing.
- `mysql.lambda_sync(function_name TEXT, payload TEXT) RETURNS TEXT` — `RequestResponse`; returns the function payload as JSON.

Under the hood the prebuilt image bakes a small libcurl-backed UDF (`fakecloud_post`, `fakecloud_post_async`) that POSTs to `/_fakecloud/rds/lambda-invoke` against `host.docker.internal`. A bootstrap script renders the host endpoint, account ID, and region from the container's `FAKECLOUD_*` env vars (set automatically by `RdsRuntime`) so SQL never has to know the host. Like the postgres image, the runtime tries to pull the published `fakecloud-mysql:<major>-<fakecloud-version>` (or `fakecloud-mariadb:<major>-<fakecloud-version>`) tag first and falls back to a local build when the pull fails.

## Asynchronous instance creation

`CreateDBInstance` returns ~immediately with `DBInstanceStatus = "creating"`. The container start (and the underlying image pull/build for postgres) runs in the background; `DescribeDBInstances` returns the live status. Callers should poll until the status flips to `available` before connecting:

```python
import boto3
import time

rds = boto3.client("rds", endpoint_url="http://localhost:4566")
rds.create_db_instance(
    DBInstanceIdentifier="my-db",
    Engine="postgres",
    EngineVersion="16.3",
    MasterUsername="admin",
    MasterUserPassword="secret123",
    AllocatedStorage=20,
    DBInstanceClass="db.t3.micro",
)

while True:
    desc = rds.describe_db_instances(DBInstanceIdentifier="my-db")
    if desc["DBInstances"][0]["DBInstanceStatus"] == "available":
        break
    time.sleep(1)
```

This matches AWS RDS behavior — real `CreateDBInstance` also never blocks on the container coming up. The `Endpoint` element is omitted from create/describe responses while the instance is still in `creating`.

## How the Docker integration works

When you call `CreateDBInstance` for PostgreSQL/MySQL/MariaDB/Oracle/SQL Server/Db2, fakecloud starts a real Docker container running the upstream image for that engine and version, waits for it to be ready, and reports the mapped host port. Your application connects to that port like it would connect to any database.

`DeleteDBInstance` stops and removes the container. `RebootDBInstance` restarts it. Snapshots serialize the DB state so it can be restored into a fresh container.

### Engine -> image map

| Engine | Image | Port | Wait probe |
|--------|-------|------|------------|
| `postgres` | `ghcr.io/faiscadev/fakecloud-postgres:<major>-<fakecloud-version>` (prebuilt with `plpython3u` + the `aws_commons`, `aws_lambda`, and `aws_s3` extensions on top of `postgres:<major>`; falls back to a local build if the pull fails) | 5432 | `tokio-postgres` ping |
| `mysql` | `ghcr.io/faiscadev/fakecloud-mysql:<major>-<fakecloud-version>` (prebuilt with the libcurl-backed `fakecloud_post` UDF + Aurora-compatible `mysql.lambda_async` / `mysql.lambda_sync` stored procedures on top of `mysql:<major>`; falls back to a local build if the pull fails) | 3306 | `mysql_async` ping |
| `mariadb` | `ghcr.io/faiscadev/fakecloud-mariadb:<major>-<fakecloud-version>` (same UDF + stored procedures, on top of `mariadb:<major>`) | 3306 | `mysql_async` ping |
| `oracle-ee` / `oracle-se2` (+`-cdb`) | `gvenzl/oracle-free:23-slim` | 1521 | log marker `DATABASE IS READY TO USE!` + TCP probe |
| `sqlserver-ee` / `-se` / `-ex` / `-web` | `mcr.microsoft.com/mssql/server:2022-latest` | 1433 | log marker `SQL Server is now ready for client connections` + TCP probe |
| `db2-se` / `db2-ae` | `icr.io/db2_community/db2:latest` | 50000 | log marker `Setup has completed` + TCP probe |

The Oracle / SQL Server / Db2 images are large (1-3 GB) and take 30-300 s to first-boot. fakecloud passes the engine-specific license-acceptance environment variables (`ACCEPT_EULA`, `LICENSE`) automatically. Db2 launches with `--privileged` because the container needs it to set kernel parameters during startup.

### Prebuilt PostgreSQL image

`ghcr.io/faiscadev/fakecloud-postgres:<major>-<fakecloud-version>` is published on every fakecloud release tag (workflow: `.github/workflows/docker-rds-images.yml`) for postgres `13`, `14`, `15`, `16`, `17`, both `linux/amd64` and `linux/arm64`. Each release also gets a rolling `:<major>` tag pointing at the latest version for that major. Resolution order at runtime:

1. Image already on the local Docker daemon -> use it.
2. `docker pull` of the version-pinned tag -> use it.
3. Local `docker build` from the embedded `crates/fakecloud-rds/assets/postgres/` Dockerfile (covers dev / unreleased / airgapped setups).

Override knobs (env vars, both optional):

- `FAKECLOUD_POSTGRES_REGISTRY=registry.example.com/team` — point at a private mirror (default `ghcr.io/faiscadev`).
- `FAKECLOUD_REBUILD_POSTGRES_IMAGE=1` — skip inspect + pull and force a fresh local build. Use after editing the embedded Dockerfile or extension SQL during development.

## Describe filters

The `Filters` parameter is honored on the Describe operations AWS documents it for. Filters are AND-ed with each other and with the operation's own identifier parameter; the values inside one filter are OR-ed. Names and values are case-sensitive, and wildcards are not supported (same as AWS).

| Operation | Supported filter names |
| --- | --- |
| `DescribeDBInstances` | `db-cluster-id`, `db-instance-id`, `dbi-resource-id`, `domain`, `engine` |
| `DescribeDBSnapshots` | `db-instance-id`, `db-snapshot-id`, `dbi-resource-id`, `engine`, `snapshot-type` |
| `DescribeDBClusters` | `clone-group-id`, `db-cluster-id`, `db-cluster-resource-id`, `domain`, `engine` |
| `DescribeDBClusterSnapshots` | `db-cluster-id`, `db-cluster-snapshot-id`, `engine`, `snapshot-type` |

Filters documented as accepting "identifiers and ARNs" (`db-instance-id`, `db-cluster-id`, `db-snapshot-id`, `db-cluster-snapshot-id`) match either form.

```bash
# Only the instance with this resource id -- what the Terraform/OpenTofu
# AWS provider uses to read a DB instance back after creating it.
aws rds describe-db-instances \
  --endpoint-url http://localhost:4566 \
  --filters "Name=dbi-resource-id,Values=db-1b8e439e59564e849b38b04191c40b8e"
```

`DescribeDBSnapshots` honors `SnapshotType` including `shared` and `public`, which report snapshots another account shared with the caller (or with everyone) through `ModifyDBSnapshotAttribute`'s `restore` attribute; `IncludeShared` / `IncludePublic` add those to an otherwise-unqualified listing. `DescribeDBClusterSnapshots` honors the same `shared` / `public` values, backed by `ModifyDBClusterSnapshotAttribute`, and the top-level `SnapshotType` parameter, and `DescribeDBClusterSnapshots` honors `DBClusterSnapshotIdentifier` / `DBClusterIdentifier`. A named resource that doesn't exist raises the operation's declared fault (`DBInstanceNotFound`, `DBSnapshotNotFound`, `DBClusterNotFoundFault`, `DBClusterSnapshotNotFoundFault`) rather than returning an empty list, so a client can tell "gone" from "no match".

Identifier parameters accept an ARN as well as a plain id (`DBInstanceIdentifier`, `DBSnapshotIdentifier`, `DBClusterIdentifier`, `DBClusterSnapshotIdentifier`, and the `SnapshotIdentifier` on the restore operations), matching what the Terraform provider stores in `snapshot_identifier`.

`clone-group-id` is populated by `RestoreDBClusterToPointInTime` with `RestoreType=copy-on-write`, which puts the clone and its source in one clone group; a full-copy restore is an independent cluster and carries no group.

Real RDS rejects a filter name an operation doesn't support with `InvalidParameterValue`. fakecloud can't: that error isn't declared on any of these operations in the Smithy model, so returning it would emit an undeclared error shape. An unrecognized filter name matches no resource instead, so a caller that asked to narrow gets an empty result rather than the full list.

## Gotchas

- **Persistence snapshots are one-way across the v3 bump.** RDS state written by this version declares snapshot schema v3 (final snapshots are typed `manual`, matching AWS; earlier builds wrote `automated`). An older fakecloud refuses to load a v3 file and exits, so downgrade by removing `<data-path>/rds/snapshot.json` first.

- **Requires a Docker socket.** RDS needs access to `/var/run/docker.sock` to start and stop containers.
- **First use pulls the image.** Expect a slower first run while the database image downloads. Heavy engines (Oracle/SQL Server/Db2) can pull 1-3 GB on first use. The PostgreSQL image is custom (`ghcr.io/faiscadev/fakecloud-postgres:<major>-<version>`) and is pulled from the registry when available; otherwise it's built locally (~60 s).
- **Aurora is partially supported.** Aurora-specific features (Global Database, Serverless v2, I/O-optimized) are recorded but don't affect the real container.
- **Db2 needs `--privileged`.** fakecloud sets it automatically; the host must allow privileged containers.
- **Heavy-engine boot is slow.** Oracle takes 30-90 s to first-boot, Db2 30-60 s, SQL Server ~30 s. Factor this into test budgets.

## Source

- [`crates/fakecloud-rds`](https://github.com/faiscadev/fakecloud/tree/main/crates/fakecloud-rds)
- [AWS RDS API reference](https://docs.aws.amazon.com/AmazonRDS/latest/APIReference/Welcome.html)
