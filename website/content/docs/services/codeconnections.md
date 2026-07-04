+++
title = "AWS CodeConnections"
description = "AWS CodeConnections (codeconnections) on fakecloud: a complete 27-operation implementation (100% conformance) — connections, hosts, repository links, sync configurations, and the sync-status/blocker read surface. awsJson1.0."
weight = 65
+++

fakecloud implements **AWS CodeConnections** as an awsJson1.0 service (sigv4
signing name `codeconnections`, target prefix `CodeConnections_20231201`). All
**27 operations** ship with **100% conformance** against AWS's own Smithy
model, backed by account-partitioned state that persists across restarts in
persistent mode.

CodeConnections is the renamed successor to CodeStar Connections: it connects
AWS services such as CodePipeline to external source repositories (GitHub,
Bitbucket, GitLab, Azure DevOps, and their self-managed variants). fakecloud
models the whole control plane as real, persisted CRUD.

## Connections

`CreateConnection` / `GetConnection` / `ListConnections` / `DeleteConnection`
manage connections to third-party providers. A new connection is created in the
`PENDING` state — creating a connection only registers the resource; completing
the OAuth handshake happens in the AWS console (via an *installation*), which
fakecloud does not emulate. So a connection stays `PENDING`, which is the honest
default state of a connection that has never completed a handshake. The
`ProviderType` is validated against the model's enum (`Bitbucket`, `GitHub`,
`GitHubEnterpriseServer`, `GitLab`, `GitLabSelfManaged`, `AzureDevOps`).
Connection ARNs are minted in the real form
`arn:aws:codeconnections:<region>:<acct>:connection/<uuid>`.

## Hosts

`CreateHost` / `GetHost` / `UpdateHost` / `DeleteHost` / `ListHosts` manage the
*hosts* used to connect to installed provider types such as GitHub Enterprise
Server and GitLab Self Managed. A host's `VpcConfiguration` (VPC id, subnet ids,
security-group ids, optional TLS certificate) round-trips verbatim. Hosts settle
to `PENDING`, a valid completed target state for host creation (manual setup of
the provider app is otherwise a console step).

## Repository links and sync configurations

`CreateRepositoryLink` and friends link a connection to a specific repository;
the link inherits its `ProviderType` from the connection. `CreateSyncConfiguration`
and friends configure CloudFormation Git sync (`CFN_STACK_SYNC`) between a linked
repository branch and a stack, with `ListSyncConfigurations` and
`ListRepositorySyncDefinitions` over them.

## Sync status and blockers

`GetRepositorySyncStatus`, `GetResourceSyncStatus`, `GetSyncBlockerSummary`, and
`UpdateSyncBlocker` form the sync-status read surface. fakecloud runs no Git-sync
engine, so a resource that was never synced reports "not found" for its sync
status, and a sync configuration with no active blockers returns an empty blocker
summary — the honest shape of a control-plane mock with no backing sync engine.

## Tagging

`TagResource` / `UntagResource` / `ListTagsForResource` operate over connections,
hosts, and repository links by ARN.

## Validation

Model-derived `@length`, `@range`, and enum constraints are enforced (connection
names, host names/endpoints, `ProviderType`/`SyncConfigurationType` enums,
`MaxResults` range, `NextToken` length), so invalid requests are rejected exactly
as AWS would.
