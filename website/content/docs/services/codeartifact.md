+++
title = "AWS CodeArtifact"
description = "AWS CodeArtifact (codeartifact) on fakecloud: a complete 48-operation implementation (100% conformance) covering domains, repositories, external connections, package groups, packages and their versions/assets/dependencies, permission policies, authorization tokens, and tagging. restJson1."
weight = 66
+++

fakecloud implements **AWS CodeArtifact** as a restJson1 service (sigv4 signing
name `codeartifact`, `@http` method + path routing). All **48 operations** ship
with **100% conformance** against AWS's own Smithy model, backed by
account-partitioned state that persists across restarts in persistent mode.

CodeArtifact is a managed artifact repository for npm, PyPI, Maven, NuGet,
generic, Ruby, Swift, and Cargo packages. fakecloud models the whole control
plane as real, persisted CRUD; there is no package-manager proxy, so external
upstream fetching is out of scope, but a `PublishPackageVersion` really stores
the asset bytes so a later `GetPackageVersionAsset` streams back exactly what
was published.

## Domains

`CreateDomain` / `DescribeDomain` / `DeleteDomain` / `ListDomains` manage
domains. A `CreateDomain` validates the domain name against the model
`@length` (2..50) and `@pattern`, mints the domain ARN
(`arn:aws:codeartifact:<region>:<account>:domain/<name>`), assigns an
`encryptionKey` (a synthetic KMS key ARN unless one is supplied), and records
`createdTime`, `repositoryCount`, `assetSizeBytes`, and an `s3BucketArn`.
`Put` / `Get` / `DeleteDomainPermissionsPolicy` manage the domain resource
policy, each returning a `ResourcePolicy` with a fresh `revision`.

## Repositories

`CreateRepository` / `DescribeRepository` / `UpdateRepository` /
`DeleteRepository` manage repositories, keyed within their domain. A repository
carries `upstreams` and `externalConnections` (both always present as arrays so
Terraform's default-empty comparisons round-trip). `GetRepositoryEndpoint`
returns the exact AWS endpoint form
(`https://<domain>-<owner>.d.codeartifact.<region>.amazonaws.com/<format>/<repo>/`)
per package format. `AssociateExternalConnection` /
`DisassociateExternalConnection` attach public upstreams (for example
`public:npmjs`), deriving the package format from the connection name.
`ListRepositories` and `ListRepositoriesInDomain` page over the stored
repositories with prefix filtering, and repository permission policies work the
same way as domain policies.

## Packages, versions, and assets

`PublishPackageVersion` creates a package and a version, computes the asset
`SHA-256`, and stores the bytes; `GetPackageVersionAsset` streams them back with
the `X-AssetName` / `X-PackageVersion` / `X-PackageVersionRevision` headers.
`ListPackages` / `DescribePackage` / `DeletePackage` /
`PutPackageOriginConfiguration` manage packages, and
`ListPackageVersions` / `DescribePackageVersion` / `DeletePackageVersions` /
`DisposePackageVersions` / `UpdatePackageVersionsStatus` /
`CopyPackageVersions` manage versions, returning the AWS
`successfulVersions` / `failedVersions` batch shape with per-version error
codes. `GetPackageVersionReadme`, `ListPackageVersionAssets`, and
`ListPackageVersionDependencies` read version metadata. The `PackageFormat`,
`PackageVersionStatus`, and origin-restriction enums are validated against the
model.

## Package groups and authorization

`CreatePackageGroup` / `DescribePackageGroup` / `UpdatePackageGroup` /
`DeletePackageGroup` / `ListPackageGroups` / `ListSubPackageGroups` manage
package groups with their origin configuration, and
`UpdatePackageGroupOriginConfiguration` / `ListAllowedRepositoriesForGroup`
manage per-restriction-type allowed repositories.
`GetAssociatedPackageGroup` and `ListAssociatedPackages` resolve the group a
package inherits from. `GetAuthorizationToken` mints a synthetic bearer token
with an `expiration` derived from the requested `durationSeconds`. `TagResource`
/ `UntagResource` / `ListTagsForResource` provide resource tagging keyed by ARN.
