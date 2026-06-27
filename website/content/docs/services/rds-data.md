+++
title = "RDS Data API"
description = "Run real SQL over HTTP with the RDS Data API (ExecuteStatement) against fakecloud's real PostgreSQL and MySQL containers — typed parameters and results, including binary round-trips."
weight = 18
+++

fakecloud implements the **RDS Data API** (`rds-data`) against the same **real
Docker-backed database containers** the RDS service manages. `ExecuteStatement`
runs your SQL on a genuine PostgreSQL or MySQL/MariaDB engine and returns
AWS-shaped typed `Field` records — this is the wedge: other local emulators
serve canned rows or return `{}` for binary columns, while fakecloud runs the
real query.

## Supported features

- **ExecuteStatement** — runs the SQL against the DB the `resourceArn` resolves
  to (DB instance or Aurora cluster ARN), on the real container engine.
- **Typed parameters** — `:name` named parameters with `stringValue`,
  `longValue`, `doubleValue`, `booleanValue`, `blobValue`, and `isNull`,
  coerced to the column's real type.
- **Typed results** — `records` as typed `Field`s (integers → `longValue`,
  text → `stringValue`, `bytea`/`BLOB` → `blobValue`, etc.), with optional
  `columnMetadata` (`includeResultMetadata`) and `formattedRecords`
  (`formatRecordsAs=JSON`). Writes report `numberOfRecordsUpdated`.
- **Binary round-trips** — a `bytea` / `BLOB` column round-trips through
  `blobValue`, where rival emulators return an empty value.

## Coming next

`BatchExecuteStatement` and transactions (`BeginTransaction` /
`CommitTransaction` / `RollbackTransaction`) over a held connection.

## Example

```python
import boto3
data = boto3.client("rds-data", endpoint_url="http://localhost:4566")
arn = "arn:aws:rds:us-east-1:123456789012:db:mydb"
secret = "arn:aws:secretsmanager:us-east-1:123456789012:secret:db"

data.execute_statement(resourceArn=arn, secretArn=secret,
    sql="CREATE TABLE t (id int, name text)")
data.execute_statement(resourceArn=arn, secretArn=secret,
    sql="INSERT INTO t (id, name) VALUES (:id, :name)",
    parameters=[{"name": "id", "value": {"longValue": 1}},
                {"name": "name", "value": {"stringValue": "alice"}}])
resp = data.execute_statement(resourceArn=arn, secretArn=secret,
    sql="SELECT id, name FROM t", includeResultMetadata=True)
# resp["records"] -> [[{"longValue": 1}, {"stringValue": "alice"}]]
```
