+++
title = "AWS CodeBuild"
description = "AWS CodeBuild (codebuild) on fakecloud: a complete 59-operation implementation (100% conformance) — build projects, builds and build batches, report groups and reports, fleets, webhooks, source credentials, resource policies, and sandboxes. awsJson1.1."
weight = 65
+++

fakecloud implements **AWS CodeBuild** as an awsJson1.1 service (sigv4 signing
name `codebuild`, target prefix `CodeBuild_20161006`). All **59 operations**
ship with **100% conformance** against AWS's own Smithy model, backed by
account-partitioned state that persists across restarts in persistent mode.

CodeBuild is the managed build service. fakecloud models the whole control
plane as real, persisted CRUD; there is no build container engine, so a
`StartBuild` produces a build that settles deterministically to a terminal
`SUCCEEDED` state (returning the same phase list AWS emits) rather than running
a real build.

## Build projects

`CreateProject` / `UpdateProject` / `DeleteProject` / `BatchGetProjects` /
`ListProjects` manage build projects. The `source`, `artifacts`, `environment`,
`cache`, `logsConfig`, `vpcConfig`, and `fileSystemLocations` blocks round-trip
verbatim; the create-time `badgeEnabled` flag is rendered as the output `badge`
structure, and each project mints an ARN in exact AWS form
(`arn:aws:codebuild:<region>:<acct>:project/<name>`). `UpdateProjectVisibility`
toggles `PUBLIC_READ` / `PRIVATE` (and returns a `publicProjectAlias`),
`InvalidateProjectCache` clears the cache, and `ListSharedProjects` reports the
account's shareable project ARNs. Model-derived `@length` (project name 2-150),
enum (`SourceType` / `ArtifactsType` / `EnvironmentType` / `ComputeType`), and
sort validation is enforced.

## Builds and build batches

`StartBuild` resolves the project (applying the `*Override` inputs), mints a
build id (`<project>:<uuid>`) and ARN, and returns a build in `IN_PROGRESS`.
The first `BatchGetBuilds` read settles it through the full phase sequence
(`SUBMITTED` -> `QUEUED` -> `PROVISIONING` -> ... -> `COMPLETED`) to
`buildStatus: SUCCEEDED`, the same lazy-settle pattern EKS and Cloud Map use.
`StopBuild`, `RetryBuild`, `BatchDeleteBuilds`, `ListBuilds`, and
`ListBuildsForProject` complete the build surface. The batch variants
(`StartBuildBatch` / `StopBuildBatch` / `RetryBuildBatch` / `DeleteBuildBatch` /
`BatchGetBuildBatches` / `ListBuildBatches` / `ListBuildBatchesForProject`)
mirror it for `BuildBatch` records.

## Report groups, reports, and test data

`CreateReportGroup` / `UpdateReportGroup` / `DeleteReportGroup` /
`BatchGetReportGroups` / `ListReportGroups` / `ListSharedReportGroups` manage
report groups (`TEST` or `CODE_COVERAGE`), and `GetReportGroupTrend` returns
trend stats. `BatchGetReports` / `ListReports` / `ListReportsForReportGroup` /
`DeleteReport` cover reports, and `DescribeTestCases` / `DescribeCodeCoverages`
return the paginated test-case and coverage surfaces.

## Fleets, webhooks, and credentials

`CreateFleet` / `UpdateFleet` / `DeleteFleet` / `BatchGetFleets` / `ListFleets`
manage reserved-capacity fleets. `CreateWebhook` / `UpdateWebhook` /
`DeleteWebhook` attach a webhook to a project (minting a payload URL and
secret). `ImportSourceCredentials` / `DeleteSourceCredentials` /
`ListSourceCredentials` store per-server-type credentials — the token ARN
renders the server type lowercase (`token/github`) exactly as AWS does.
`PutResourcePolicy` / `GetResourcePolicy` / `DeleteResourcePolicy` attach a
resource policy to an existing project or report group, and
`ListCuratedEnvironmentImages` returns the curated managed-image catalogue.

## Sandboxes

`StartSandbox` / `StopSandbox` / `StartSandboxConnection` / `BatchGetSandboxes`
/ `ListSandboxes` / `ListSandboxesForProject` and the command-execution surface
(`StartCommandExecution` / `BatchGetCommandExecutions` /
`ListCommandExecutionsForSandbox`) model CodeBuild's interactive sandbox
primitive as real, persisted CRUD.

## Not implemented

There is no build container engine — the actual build execution (running
buildspec phases in a container) is out of scope, matching how LocalStack
Community mocks CodeBuild. Builds settle to a terminal status synchronously,
and test-case / code-coverage reads return well-formed empty result sets for
synthetically created reports.
