+++
title = "Local RDS for integration tests"
description = "Run local RDS for integration tests with fakecloud. 163 RDS operations, real PostgreSQL/MySQL/MariaDB/Oracle/SQL Server/Db2 engines via Docker, snapshots, read replicas. Free, AGPL-3.0."
template = "page.html"
+++

Need local RDS for integration tests? Use [fakecloud](https://github.com/faiscadev/fakecloud).

```sh
curl -fsSL https://fakecloud.dev/install.sh | bash
fakecloud
```

Point your AWS SDK at `http://localhost:4566`. Docker required because fakecloud runs **real** PostgreSQL/MySQL/MariaDB/Oracle/SQL Server/Db2 engines.

## Why fakecloud for RDS

- **163 RDS operations** at 100% conformance — DB instances, snapshots, read replicas, parameter groups, subnet groups, engine/version discovery, tagging, upgrades.
- **Real database engines.** fakecloud pulls real PostgreSQL / MySQL / MariaDB / Oracle / SQL Server / Db2 Docker images and runs them as the RDS instance. Your SQL schema, indexes, triggers, and extensions all work because they are real engines. PostgreSQL uses a prebuilt `ghcr.io/faiscadev/fakecloud-postgres:<major>` image that bakes in `plpython3u` and the AWS RDS `aws_lambda` and `aws_s3` extensions.
- **Endpoint works.** `DescribeDBInstances` returns a real connectable host. Your application connects with the usual PostgreSQL / MySQL / Oracle / SQL Server / Db2 driver.
- **Paid on LocalStack; free here.** RDS has always been LocalStack Pro-only.
- **No account, no auth token, no paid tier.** AGPL-3.0.

## Create a PostgreSQL instance

```sh
aws --endpoint-url http://localhost:4566 rds create-db-instance \
  --db-instance-identifier mydb \
  --engine postgres \
  --engine-version 16 \
  --db-instance-class db.t3.micro \
  --allocated-storage 20 \
  --master-username admin \
  --master-user-password password \
  --publicly-accessible

aws --endpoint-url http://localhost:4566 rds wait db-instance-available \
  --db-instance-identifier mydb
```

Get the endpoint:

```sh
aws --endpoint-url http://localhost:4566 rds describe-db-instances \
  --db-instance-identifier mydb \
  --query 'DBInstances[0].Endpoint'
```

Connect with psql (or any Postgres driver):

```sh
PGPASSWORD=password psql -h <endpoint> -p 5432 -U admin -d postgres
```

It's a real Postgres 16. `\l`, `CREATE EXTENSION pg_trgm`, `CREATE TRIGGER`, JSONB — all work.

## MySQL / MariaDB

```sh
aws --endpoint-url http://localhost:4566 rds create-db-instance \
  --db-instance-identifier mydb \
  --engine mysql \
  --engine-version 8.0 \
  --db-instance-class db.t3.micro \
  --allocated-storage 20 \
  --master-username admin \
  --master-user-password password
```

`--engine mariadb` for MariaDB.

## Oracle / SQL Server / Db2

The same `create-db-instance` flow works for Oracle (`oracle-ee`, `oracle-se2`), SQL Server (`sqlserver-ee`/`-se`/`-ex`/`-web`), and Db2 (`db2-se`, `db2-ae`). fakecloud pulls the upstream free-tier images (`gvenzl/oracle-free`, `mcr.microsoft.com/mssql/server`, `icr.io/db2_community/db2`), accepts their licenses on your behalf, and reports back the mapped host port. First-run image pulls are large (1-3 GB) and engine boot takes 30-300 s, so plan test budgets accordingly.

```sh
aws --endpoint-url http://localhost:4566 rds create-db-instance \
  --db-instance-identifier oracle-test \
  --engine oracle-ee --engine-version 23.0.0 \
  --db-instance-class db.t3.micro --allocated-storage 20 \
  --master-username admin --master-user-password 'Aa1234567'
```

## Snapshots

```sh
aws --endpoint-url http://localhost:4566 rds create-db-snapshot \
  --db-instance-identifier mydb \
  --db-snapshot-identifier before-migration

# ... do stuff that might break ...

aws --endpoint-url http://localhost:4566 rds restore-db-instance-from-db-snapshot \
  --db-instance-identifier mydb-restored \
  --db-snapshot-identifier before-migration
```

Real snapshot — pg_dump / mysqldump of the running instance.

## Read replicas

```sh
aws --endpoint-url http://localhost:4566 rds create-db-instance-read-replica \
  --db-instance-identifier mydb-replica \
  --source-db-instance-identifier mydb
```

Real streaming replication on the Postgres side; binlog replication on MySQL/MariaDB.

## In tests

```ts
import { RDSClient, CreateDBInstanceCommand, waitUntilDBInstanceAvailable } from '@aws-sdk/client-rds';
import { Client } from 'pg';

const rds = new RDSClient({ endpoint: 'http://localhost:4566' });

beforeAll(async () => {
  await rds.send(new CreateDBInstanceCommand({
    DBInstanceIdentifier: 'test-db',
    Engine: 'postgres',
    EngineVersion: '16',
    DBInstanceClass: 'db.t3.micro',
    AllocatedStorage: 20,
    MasterUsername: 'admin',
    MasterUserPassword: 'password',
  }));
  await waitUntilDBInstanceAvailable(
    { client: rds, maxWaitTime: 60 },
    { DBInstanceIdentifier: 'test-db' }
  );
});

test('app writes to real postgres via RDS emulation', async () => {
  const pg = new Client({
    host: 'localhost', port: 5432,
    user: 'admin', password: 'password', database: 'postgres',
  });
  await pg.connect();
  await pg.query('CREATE TABLE t (id int)');
  await pg.query('INSERT INTO t VALUES (1)');
  const r = await pg.query('SELECT * FROM t');
  expect(r.rows).toEqual([{ id: 1 }]);
});
```

Your Postgres integration tests run against a real Postgres that RDS manages. No compromises.

## How it differs from alternatives

| Tool | Real Postgres | Real MySQL | Snapshots | Read replicas | Price |
|---|---|---|---|---|---|
| fakecloud | Yes (Docker) | Yes (Docker) | Yes | Yes | Free |
| LocalStack Pro | Yes | Yes | Yes | Yes | Paid |
| LocalStack Community | **No** | No | No | No | — (not available) |
| Plain `docker run postgres` | Yes (DB only) | Yes | Manual | Manual | Free, but no RDS API |
| Moto | Stubbed | Stubbed | Stubbed | Stubbed | Free |

If you want the RDS API + real engines without paying LocalStack Pro, fakecloud is the option.

## Links

- **Install:** `curl -fsSL https://fakecloud.dev/install.sh | bash`
- **Repo:** [github.com/faiscadev/fakecloud](https://github.com/faiscadev/fakecloud)
- **Related:** [Local ElastiCache for tests](/local-elasticache/), [Fake AWS server for tests](/fake-aws-server/)
