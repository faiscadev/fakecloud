+++
title = "EKS"
description = "Amazon EKS (eks) on fakecloud: Elastic Kubernetes Service control plane — cluster lifecycle, config/version updates with update tracking, and tagging. restJson1."
weight = 46
+++

fakecloud implements **Amazon EKS** (`eks`), the managed Kubernetes service, as a
restJson1 control plane. **28 of the full 65-operation surface** ship now — the
**cluster**, **managed node group**, **Fargate profile**, and **add-on** control
planes — backed by account-partitioned state that persists across restarts in
persistent mode.

## Supported now (28 operations)

- **Cluster lifecycle** — `CreateCluster`, `DescribeCluster`, `ListClusters`,
  `DeleteCluster`. Clusters are created with the requested `roleArn`,
  `resourcesVpcConfig`, `version` (default 1.31), and tags, and transition
  `CREATING` -> `ACTIVE` on describe (deterministic, no background timer).
- **Cluster updates** — `UpdateClusterConfig`, `UpdateClusterVersion`, each
  minting a tracked `Update` that settles `InProgress` -> `Successful` on
  describe; `DescribeUpdate` and `ListUpdates` return the update history.
- **Managed node groups** — `CreateNodegroup`, `DescribeNodegroup`,
  `ListNodegroups`, `DeleteNodegroup`, plus `UpdateNodegroupConfig` and
  `UpdateNodegroupVersion` (tracked updates). Node groups carry `nodeRole`,
  `subnets`, `scalingConfig`, and transition `CREATING` -> `ACTIVE` on describe.
- **Fargate profiles** — `CreateFargateProfile`, `DescribeFargateProfile`,
  `ListFargateProfiles`, `DeleteFargateProfile` with `podExecutionRoleArn` and
  `selectors`, their own `CREATING` -> `ACTIVE` transition.
- **Add-ons** — `CreateAddon`, `DescribeAddon`, `ListAddons`, `DeleteAddon`,
  `UpdateAddon` (tracked version updates), plus the read-only catalogue ops
  `DescribeAddonVersions` (vpc-cni, coredns, kube-proxy, aws-ebs-csi-driver,
  aws-efs-csi-driver) and `DescribeAddonConfiguration`.
- **Tagging** — `TagResource`, `UntagResource`, `ListTagsForResource` (EKS uses
  a `map<String,String>` tag shape, keyed by resource ARN).

100% conformance across every shipped operation: all 865 generated Smithy probe
variants for these 28 operations pass.

## In progress (roadmap)

The remaining EKS surface is being added in subsequent batches: **access
entries** and access policies, **identity provider configs**, **pod identity
associations**, **insights**, and **EKS Anywhere / subscription** operations.
There is no real Kubernetes API-server endpoint; the control plane models the
AWS management API, not `kubectl` traffic.

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
