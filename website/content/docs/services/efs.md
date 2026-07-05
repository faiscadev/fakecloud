+++
title = "Amazon EFS"
description = "Amazon Elastic File System (elasticfilesystem) on fakecloud: a complete 31-operation control plane (100% conformance) - file systems, mount targets, access points, lifecycle/backup/policy configuration, replication, tagging, and account preferences. restJson1."
weight = 68
+++

fakecloud implements **Amazon Elastic File System** (EFS) as a restJson1
service. All **31 operations** ship with **100% conformance** against AWS's own
Smithy model, backed by account-partitioned state that persists across restarts
in persistent mode.

EFS on fakecloud is a faithful **control plane**: every `Create`/`Put`/`Update`
is reflected by its `Describe`, every `Delete` deletes, and AWS's asynchronous
resource lifecycle is modelled - not faked. No real NFS file system is served
(fakecloud models the AWS management API, not the mount data plane).

## Resources

- **File systems** - `CreateFileSystem` returns a `fs-`-prefixed id with a
  `LifeCycleState` of `creating` and settles to `available` on the next
  `DescribeFileSystems` (an interrupted transition reconciles on restart).
  `CreationToken` is honoured as the idempotency token, so a repeat create with
  the same token returns `FileSystemAlreadyExists`. The description carries the
  `SizeInBytes` breakdown, `PerformanceMode`, `ThroughputMode`
  (`ProvisionedThroughputInMibps` when provisioned), `Encrypted` + `KmsKeyId`,
  the live `NumberOfMountTargets`, and `FileSystemProtection`
  (`ReplicationOverwriteProtection`, toggled by `UpdateFileSystemProtection`).
  `DeleteFileSystem` returns `FileSystemInUse` while mount targets remain.
- **Mount targets** - one per Availability Zone per file system
  (`MountTargetConflict` otherwise). The AZ, AZ id, VPC, and - when the
  referenced subnet was created through fakecloud's EC2 service - the IP address
  are resolved from the real subnet; the network interface (`eni-...`) and
  security groups are synthesized. `fsmt-` ids, `LifeCycleState`
  (`creating` -> `available`), and `OwnerId` match AWS.
  `ModifyMountTargetSecurityGroups` / `DescribeMountTargetSecurityGroups`
  round-trip the security-group list.
- **Access points** - `fsap-` ids, `PosixUser`, and `RootDirectory` (defaulting
  to `/`), with `ClientToken` idempotency (`AccessPointAlreadyExists`).
- **Lifecycle configuration**, **backup policy**, and **file-system resource
  policy** each persist and read back through their `Put`/`Describe` pair
  (`DescribeFileSystemPolicy` returns `PolicyNotFound` when unset).
- **Replication configurations** - `CreateReplicationConfiguration` records a
  destination (synthesizing the destination file system id) and settles the
  destination `Status` to `ENABLED`; `DescribeReplicationConfigurations` and
  `DeleteReplicationConfiguration` complete the lifecycle.
- **Tagging** - both the resource-id tagging API (`TagResource`,
  `UntagResource`, `ListTagsForResource` over `fs-`/`fsap-` resources) and the
  deprecated per-file-system API (`CreateTags`, `DeleteTags`, `DescribeTags`).
  A `Name` tag surfaces as the file system's `Name`.
- **Account preferences** - `PutAccountPreferences` / `DescribeAccountPreferences`
  persist the resource-id-type preference.

## Identifiers and shapes

Resource ids (`fs-`/`fsmt-`/`fsap-` + 17 hex characters), ARNs
(`arn:aws:elasticfilesystem:<region>:<account>:file-system/<id>` and
`.../access-point/<id>`), `OwnerId`, and timestamps match AWS's formats.
Model-derived `@length`/`@range`/enum/pattern constraints are enforced, so
malformed input returns `BadRequest` (or the operation's declared not-found)
exactly as AWS does.
