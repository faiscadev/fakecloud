+++
title = "MemoryDB"
description = "Amazon MemoryDB (memorydb) on fakecloud: full 45-operation control plane for Redis/Valkey clusters, shards, ACLs, users, parameter and subnet groups, snapshots, and multi-region clusters, with persistence."
weight = 45
+++

fakecloud implements **Amazon MemoryDB** (`memorydb`), the Redis/Valkey-compatible
in-memory database service. All **45 operations** from the AWS Smithy model ship
now, backed by account-partitioned state that persists across restarts in
persistent mode.

## Supported features

- **Clusters** (`CreateCluster`, `DescribeClusters`, `UpdateCluster`,
  `DeleteCluster`, `BatchUpdateCluster`, `FailoverShard`). Clusters are created
  with the requested shard/replica topology and transition `creating` ->
  `available` on describe. `NumShards` (1-500) and `NumReplicasPerShard` (0-5)
  are validated against the model bounds.
- **Access control** (`CreateACL`, `DescribeACLs`, `UpdateACL`, `DeleteACL`,
  `CreateUser`, `DescribeUsers`, `UpdateUser`, `DeleteUser`). A default
  `open-access` ACL and `default` user are seeded per account, matching AWS.
- **Parameter and subnet groups** (`CreateParameterGroup`,
  `DescribeParameterGroups`, `DescribeParameters`, `UpdateParameterGroup`,
  `ResetParameterGroup`, `DeleteParameterGroup`, `CreateSubnetGroup`,
  `DescribeSubnetGroups`, `UpdateSubnetGroup`, `DeleteSubnetGroup`).
- **Snapshots** (`CreateSnapshot`, `CopySnapshot`, `DescribeSnapshots`,
  `DeleteSnapshot`) capture cluster configuration.
- **Multi-region clusters** (`CreateMultiRegionCluster`,
  `DescribeMultiRegionClusters`, `UpdateMultiRegionCluster`,
  `DeleteMultiRegionCluster`, `ListAllowedMultiRegionClusterUpdates`,
  `DescribeMultiRegionParameterGroups`, `DescribeMultiRegionParameters`).
- **Reserved nodes** (`DescribeReservedNodes`,
  `DescribeReservedNodesOfferings`, `PurchaseReservedNodesOffering`).
- **Node-type updates** (`ListAllowedNodeTypeUpdates`).
- **Discovery, tagging, and events** (`DescribeEngineVersions`,
  `DescribeServiceUpdates`, `DescribeEvents`, `ListTags`, `TagResource`,
  `UntagResource`).

100% conformance: all 1,201 generated Smithy probe variants pass.

## Control plane vs data plane

MemoryDB ships as a full control plane today: every resource is real, validated,
account-partitioned, and persisted. The Redis/Valkey **data-plane** container
backing (a real engine you can `SET`/`GET` against, as ElastiCache already
provides) is a roadmap item, mirroring how Aurora DSQL shipped its control plane
first.

## Example

```python
import boto3
mdb = boto3.client("memorydb", endpoint_url="http://localhost:4566")

mdb.create_cluster(
    ClusterName="app-cache",
    NodeType="db.r6g.large",
    ACLName="open-access",
    NumShards=2,
    NumReplicasPerShard=1,
)

cluster = mdb.describe_clusters(ClusterName="app-cache")["Clusters"][0]
print(cluster["Status"], cluster["NumberOfShards"])
```
