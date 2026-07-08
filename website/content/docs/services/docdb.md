+++
title = "DocumentDB"
description = "Amazon DocumentDB control plane: clusters, instances, snapshots, restore, parameter/subnet/global groups, event subscriptions, and tagging. RDS-shaped Query API."
weight = 17
+++

# Amazon DocumentDB

fakecloud implements the full **55-operation** Amazon DocumentDB (docdb)
control plane. DocumentDB reuses Amazon RDS's wire contract: it speaks the
AWS **Query** protocol (form-encoded requests, XML responses), signs SigV4
with the `rds` scope, and is reached at `rds.<region>.amazonaws.com`. The
real `aws-sdk-docdb` client is disambiguated from `aws-sdk-rds` by the
`api/docdb` token it stamps into its `user-agent`; the conformance probe
signs the `docdb` scope directly.

## What is real

Everything in the control plane is real and account-partitioned, and
persists across restarts in persistent mode:

- **DB clusters** — `CreateDBCluster`, `DeleteDBCluster`, `ModifyDBCluster`,
  `DescribeDBClusters`, `StartDBCluster`, `StopDBCluster`,
  `FailoverDBCluster`. Clusters mint an `arn:aws:rds:<region>:<acct>:cluster:<id>`
  ARN, a `cluster-XXXX` resource id, and writer
  (`<id>.cluster-XXXX.<region>.docdb.amazonaws.com`) + reader
  (`<id>.cluster-ro-XXXX.…`) endpoints. `Engine` is `docdb`.
- **DB instances** — `CreateDBInstance`, `DeleteDBInstance`,
  `ModifyDBInstance`, `DescribeDBInstances`, `RebootDBInstance`. Instances
  attach to a cluster and join its `DBClusterMembers` list; the first
  becomes the writer.
- **Snapshots** — `CreateDBClusterSnapshot`, `CopyDBClusterSnapshot`,
  `DeleteDBClusterSnapshot`, `DescribeDBClusterSnapshots`,
  `ModifyDBClusterSnapshotAttribute`, `DescribeDBClusterSnapshotAttributes`.
  `RestoreDBClusterFromSnapshot` and `RestoreDBClusterToPointInTime`
  recreate a cluster.
- **Parameter groups** — create / copy / delete / describe / modify /
  reset cluster parameter groups; set values round-trip through
  `DescribeDBClusterParameters`. `DescribeEngineDefaultClusterParameters`
  returns engine defaults.
- **Subnet groups**, **global clusters** (with failover / switchover /
  remove-from-global), and **event subscriptions** (SNS topic + source
  filters) — full CRUD.
- **Catalog + tagging** — `DescribeCertificates`,
  `DescribeDBEngineVersions`, `DescribeOrderableDBInstanceOptions`,
  `DescribeEventCategories`, `DescribeEvents`,
  `DescribePendingMaintenanceActions`, `ApplyPendingMaintenanceAction`,
  and `AddTagsToResource` / `RemoveTagsFromResource` /
  `ListTagsForResource`.

Model-derived faults are returned with the correct wire code and HTTP
status — `DBClusterNotFoundFault`, `DBInstanceNotFound`,
`DBClusterAlreadyExistsFault`, `DBClusterSnapshotNotFoundFault`,
`DBParameterGroupNotFound`, `DBSubnetGroupNotFoundFault`,
`GlobalClusterNotFoundFault`, `SubscriptionNotFound`, and the rest.

## Honest gap: no data plane

fakecloud does not run a real DocumentDB (MongoDB-compatible) engine. RDS
boots real Postgres/MySQL containers, but there is no equivalent
DocumentDB engine image, so **DocumentDB is control-plane only**: clusters
and instances are records with well-formed endpoints that accept no wire
connections. Everything else — lifecycle, snapshots, restore, parameter /
subnet / global groups, event subscriptions, and tags — is real and
persisted.
