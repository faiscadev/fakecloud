+++
title = "EKS"
description = "Amazon EKS (eks) on fakecloud: complete 70-op Elastic Kubernetes Service control plane — clusters, node groups, Fargate profiles, add-ons, access entries, identity-provider configs, pod identity, insights, capabilities, certificate authorities, and EKS Anywhere. restJson1."
weight = 46
+++

fakecloud implements **Amazon EKS** (`eks`), the managed Kubernetes service, as a
restJson1 control plane. **The complete 70-operation surface** ships — clusters,
managed node groups, Fargate profiles, add-ons, access entries, OIDC
identity-provider configs, pod-identity associations, upgrade insights,
capabilities, cluster certificate authorities, connected-cluster registration,
encryption config, and EKS Anywhere subscriptions — backed by account-partitioned state that persists across restarts
in persistent mode.

## Supported now (all 70 operations)

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
- **Access entries** — `CreateAccessEntry`, `DescribeAccessEntry`,
  `ListAccessEntries`, `DeleteAccessEntry`, `UpdateAccessEntry`, plus
  `AssociateAccessPolicy`, `DisassociateAccessPolicy`,
  `ListAssociatedAccessPolicies` (cluster/namespace `accessScope`), and the
  read-only `ListAccessPolicies` catalogue of the `AmazonEKS*` cluster-access
  policies.
- **OIDC identity-provider configs** — `AssociateIdentityProviderConfig` and
  `DisassociateIdentityProviderConfig` (each minting a tracked cluster `Update`),
  `DescribeIdentityProviderConfig`, `ListIdentityProviderConfigs`.
- **Pod-identity associations** — `CreatePodIdentityAssociation`,
  `DescribePodIdentityAssociation`, `ListPodIdentityAssociations`,
  `UpdatePodIdentityAssociation`, `DeletePodIdentityAssociation` (map a
  `namespace`/`serviceAccount` to a `roleArn`, `a-`-prefixed `associationId`).
- **Upgrade insights** — `ListInsights`, `DescribeInsight` (seeded `PASSING`
  UPGRADE_READINESS findings per cluster), `StartInsightsRefresh`,
  `DescribeInsightsRefresh`.
- **Capabilities** — `CreateCapability`, `DescribeCapability`, `ListCapabilities`,
  `UpdateCapability` (tracked `Update`), `DeleteCapability`.
- **Certificate authorities** — `CreateCertificateAuthority`,
  `DescribeCertificateAuthority`, `ListCertificateAuthorities`,
  `DeleteCertificateAuthority`, `ActivateCertificateAuthority`. Every cluster is
  created with an EKS-signed CA already `IN_USE`; a customer-created CA starts
  `NOT_USED`/`IN_PROGRESS`, settles to `COMPLETE` on describe, and activation
  promotes it while demoting the previous signer to `NOT_USED` with
  `rollbackAvailable`. Deleting the signing CA is refused with
  `ResourceInUseException`.
- **Connected clusters** — `RegisterCluster` (creates a `PENDING` cluster with a
  `connectorConfig`) and `DeregisterCluster`.
- **Cluster maintenance** — `AssociateEncryptionConfig` and `CancelUpdate` (both
  operate on tracked `Update` records), plus the read-only `DescribeClusterVersions`
  catalogue (Kubernetes 1.28-1.32 with platform versions and standard/extended
  support windows).
- **EKS Anywhere subscriptions** — `CreateEksAnywhereSubscription`,
  `DescribeEksAnywhereSubscription`, `ListEksAnywhereSubscriptions`,
  `UpdateEksAnywhereSubscription`, `DeleteEksAnywhereSubscription`
  (account-scoped, `term`/`licenseQuantity`/`autoRenew`).
- **Tagging** — `TagResource`, `UntagResource`, `ListTagsForResource` (EKS uses
  a `map<String,String>` tag shape, keyed by resource ARN).

100% conformance across the full surface: all 1,995 generated Smithy probe
variants for the 70 operations pass. There is no real Kubernetes API-server
endpoint; the control plane models the AWS management API, not `kubectl` traffic.

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
