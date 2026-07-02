+++
title = "EKS"
description = "Amazon EKS (eks) on fakecloud: Elastic Kubernetes Service control plane — cluster lifecycle, config/version updates with update tracking, and tagging. restJson1."
weight = 46
+++

fakecloud implements **Amazon EKS** (`eks`), the managed Kubernetes service, as a
restJson1 control plane. This is **batch 1** of the full 65-operation surface:
the **cluster control plane** ships now, backed by account-partitioned state that
persists across restarts in persistent mode.

## Supported now (11 operations)

- **Cluster lifecycle** — `CreateCluster`, `DescribeCluster`, `ListClusters`,
  `DeleteCluster`. Clusters are created with the requested `roleArn`,
  `resourcesVpcConfig`, `version` (default 1.31), and tags, and transition
  `CREATING` -> `ACTIVE` on describe (deterministic, no background timer).
- **Updates** — `UpdateClusterConfig`, `UpdateClusterVersion`, each minting a
  tracked `Update` that settles `InProgress` -> `Successful` on describe;
  `DescribeUpdate` and `ListUpdates` return the update history.
- **Tagging** — `TagResource`, `UntagResource`, `ListTagsForResource` (EKS uses
  a `map<String,String>` tag shape, keyed by cluster ARN).

100% conformance across every shipped operation: all 316 generated Smithy probe
variants for these 11 operations pass.

## In progress (roadmap)

The remaining EKS surface is being added in subsequent batches: managed **node
groups**, **Fargate profiles**, **add-ons**, **access entries**, **identity
provider configs**, **pod identity associations**, and **EKS Anywhere /
subscription** operations. There is no real Kubernetes API-server endpoint; the
control plane models the AWS management API, not `kubectl` traffic.

## Example

```python
import boto3
eks = boto3.client("eks", endpoint_url="http://localhost:4566")

eks.create_cluster(
    name="app",
    roleArn="arn:aws:iam::123456789012:role/eksClusterRole",
    resourcesVpcConfig={"subnetIds": ["subnet-1", "subnet-2"]},
    version="1.31",
)

cluster = eks.describe_cluster(name="app")["cluster"]
print(cluster["status"], cluster["version"])  # ACTIVE 1.31
```
