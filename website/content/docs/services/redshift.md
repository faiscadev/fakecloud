+++
title = "Redshift"
description = "A local Amazon Redshift control plane — clusters, snapshots, parameter/subnet groups, snapshot schedules, endpoint access, and logging, all as real persisted state with AWS-faithful responses."
weight = 19
+++

fakecloud implements the **Amazon Redshift** control plane (`redshift`, the
`2012-12-01` Query/XML API) as real persisted state. Unlike RDS or ElastiCache
there is no data plane container to run — Redshift's SQL surface is the separate
`redshift-data` API — so every one of the 141 operations is modelled as genuine
CRUD with validation, pagination, and AWS-shaped error codes rather than stubs.

## Supported features

- **Clusters** — `CreateCluster` / `ModifyCluster` / `DeleteCluster` /
  `DescribeClusters` and the lifecycle actions (`RebootCluster`,
  `PauseCluster`, `ResumeCluster`, `ResizeCluster`, `RotateEncryptionKey`).
  New clusters progress straight to `available` with a synthetic `Endpoint`
  (`<id>.<random>.<region>.redshift.amazonaws.com:5439`), a leader
  `ClusterNode`, a cluster public key, and a `MultiAZ` status, so the Terraform
  provider's create waiter completes immediately.
- **Parameter groups** — create/modify/reset/describe, with `DescribeClusterParameters`
  honouring the `Source=user` filter so only explicitly-modified parameters are
  returned (no perpetual Terraform drift on engine defaults).
- **Subnet & security groups** — full CRUD, ingress authorize/revoke, and tag
  round-tripping.
- **Snapshots** — manual and cluster snapshots carrying a real `SnapshotArn`,
  `RestoreFromClusterSnapshot`, `RestoreTableFromClusterSnapshot`, batch
  operations, cross-region snapshot-copy configuration
  (`EnableSnapshotCopy` / `ModifySnapshotCopyRetentionPeriod`), and snapshot
  schedules with cluster associations surfaced under `AssociatedClusters`.
- **Endpoint access** — Redshift-managed VPC endpoints that inherit the
  cluster's security groups (or a default), settle to `active`, and expose a
  well-formed interface VPC endpoint with a network interface.
- **HSM objects, event subscriptions, usage limits, snapshot copy grants,
  scheduled actions, authentication profiles, IAM-role attach, partner
  registration** — all real CRUD.
- **Per-cluster logging** — `EnableLogging` / `DisableLogging` /
  `DescribeLoggingStatus` round-trip the `LogExports`, destination type, and
  bucket/prefix.
- **Tagging** — `CreateTags` / `DeleteTags` / `DescribeTags` resolve the
  resource ARN to real per-resource tag lists (a `ResourceNotFoundFault` is
  returned for an ARN that does not resolve).

## Out of scope

- **SQL / data plane** — querying a warehouse is the separate `redshift-data`
  API, not part of the Redshift control plane.
- **Data sharing and zero-ETL integrations** — the Terraform resources for
  these first create a **Redshift Serverless** namespace, a separate AWS
  service fakecloud does not implement.

## Example

```python
import boto3
rs = boto3.client("redshift", endpoint_url="http://localhost:4566")

rs.create_cluster(
    ClusterIdentifier="analytics",
    NodeType="ra3.xlplus",
    MasterUsername="admin",
    MasterUserPassword="Passw0rd123",
    ClusterType="single-node",
)

cluster = rs.describe_clusters(ClusterIdentifier="analytics")["Clusters"][0]
# cluster["ClusterStatus"] -> "available"
# cluster["Endpoint"] -> {"Address": "analytics-....redshift.amazonaws.com", "Port": 5439}

rs.create_cluster_snapshot(SnapshotIdentifier="nightly", ClusterIdentifier="analytics")
snap = rs.describe_cluster_snapshots(ClusterIdentifier="analytics")["Snapshots"][0]
# snap["SnapshotArn"] -> "arn:aws:redshift:us-east-1:123456789012:snapshot:analytics/nightly"
```
