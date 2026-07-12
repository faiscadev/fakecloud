+++
title = "Neptune"
description = "Amazon Neptune control plane: clusters, instances, cluster endpoints, snapshots, restore, parameter/subnet/global groups, IAM role associations, event subscriptions, and tagging. RDS-shaped Query API."
weight = 17
+++

# Amazon Neptune

fakecloud implements the full **70-operation** Amazon Neptune (neptune)
control plane. Neptune reuses Amazon RDS's wire contract: it speaks the
AWS **Query** protocol (form-encoded requests, XML responses), signs SigV4
with the `rds` scope, and is reached at `rds.<region>.amazonaws.com`. The
real `aws-sdk-neptune` client is disambiguated from `aws-sdk-rds` by the
`api/neptune` token it stamps into its `user-agent`; the conformance probe
signs the `neptune` scope directly.

## What is real

Everything in the control plane is real and account-partitioned, and
persists across restarts in persistent mode:

- **DB clusters** — `CreateDBCluster`, `DeleteDBCluster`, `ModifyDBCluster`,
  `DescribeDBClusters`, `StartDBCluster`, `StopDBCluster`,
  `FailoverDBCluster`, `PromoteReadReplicaDBCluster`. Clusters mint an
  `arn:aws:rds:<region>:<acct>:cluster:<id>` ARN, a `cluster-XXXX` resource
  id, and writer (`<id>.cluster-XXXX.<region>.neptune.amazonaws.com`) +
  reader (`<id>.cluster-ro-XXXX.…`) endpoints. `Engine` is `neptune`.
- **DB instances** — `CreateDBInstance`, `DeleteDBInstance`,
  `ModifyDBInstance`, `DescribeDBInstances`, `RebootDBInstance`,
  `DescribeValidDBInstanceModifications`. Instances attach to a cluster and
  join its `DBClusterMembers` list; the first becomes the writer.
- **Cluster endpoints** — `CreateDBClusterEndpoint`,
  `DeleteDBClusterEndpoint`, `ModifyDBClusterEndpoint`,
  `DescribeDBClusterEndpoints`. Custom / reader endpoints get their own
  DNS name and `arn:aws:rds:<region>:<acct>:cluster-endpoint:<id>` ARN.
- **IAM roles** — `AddRoleToDBCluster` / `RemoveRoleFromDBCluster`
  associate S3 bulk-load / neptune-stream roles onto a cluster, surfaced
  in `AssociatedRoles`.
- **Snapshots** — `CreateDBClusterSnapshot`, `CopyDBClusterSnapshot`,
  `DeleteDBClusterSnapshot`, `DescribeDBClusterSnapshots`,
  `ModifyDBClusterSnapshotAttribute`, `DescribeDBClusterSnapshotAttributes`.
  `RestoreDBClusterFromSnapshot` and `RestoreDBClusterToPointInTime`
  recreate a cluster.
- **Parameter groups** — create / copy / delete / describe / modify /
  reset both **cluster** and **DB (instance)** parameter groups; set values
  round-trip through `DescribeDBClusterParameters` / `DescribeDBParameters`.
  `DescribeEngineDefaultClusterParameters` and
  `DescribeEngineDefaultParameters` return engine defaults.
- **Subnet groups**, **global clusters** (with failover / switchover /
  remove-from-global), and **event subscriptions** (SNS topic + source
  filters) — full CRUD.
- **Catalog + tagging** — `DescribeDBEngineVersions`,
  `DescribeOrderableDBInstanceOptions`, `DescribeEventCategories`,
  `DescribeEvents`, `DescribePendingMaintenanceActions`,
  `ApplyPendingMaintenanceAction`, and `AddTagsToResource` /
  `RemoveTagsFromResource` / `ListTagsForResource`.

Model-derived faults are returned with the correct wire code and HTTP
status — `DBClusterNotFoundFault`, `DBInstanceNotFound`,
`DBClusterAlreadyExistsFault`, `DBClusterSnapshotNotFoundFault`,
`DBClusterEndpointNotFoundFault`, `DBClusterParameterGroupNotFound`,
`DBParameterGroupNotFound`, `DBSubnetGroupNotFoundFault`,
`GlobalClusterNotFoundFault`, `SubscriptionNotFound`, and the rest.

## Honest gap: no data plane

fakecloud does not run a real Neptune (Gremlin/SPARQL graph) engine. RDS
boots real Postgres/MySQL containers, but there is no equivalent Neptune
engine image, so **Neptune is control-plane only**: clusters, instances,
and cluster endpoints are records with well-formed DNS endpoints that
accept no wire connections. Everything else — lifecycle, cluster
endpoints, snapshots, restore, parameter / subnet / global groups, IAM
role associations, event subscriptions, and tags — is real and persisted.
