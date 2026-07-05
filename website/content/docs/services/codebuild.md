+++
title = "AWS CodeBuild"
description = "AWS CodeBuild (codebuild) on fakecloud: a complete 59-operation implementation (100% conformance) — build projects, builds and build batches, report groups and reports, fleets, webhooks, source credentials, resource policies, and sandboxes. awsJson1.1."
weight = 65
+++

fakecloud implements **AWS CodeBuild** as an awsJson1.1 service (sigv4 signing
name `codebuild`, target prefix `CodeBuild_20161006`). All **59 operations**
ship with **100% conformance** against AWS's own Smithy model, backed by
account-partitioned state that persists across restarts in persistent mode.

CodeBuild is the managed build service, and fakecloud **runs the build for
real**. `StartBuild` returns immediately with the build `IN_PROGRESS`; a
background task then resolves the environment image, parses the buildspec, and
executes each phase's `commands` in a real Docker/Podman container, settling
`buildStatus` on the actual container exit codes. Build output streams to
CloudWatch Logs and declared `S3` artifacts are uploaded — see
[Real build execution](#real-build-execution). The whole control plane is real,
persisted CRUD on top of that. When no container runtime is available (or the
backend is disabled via `FAKECLOUD_CODEBUILD_DISABLE_BACKEND`), a build settles
deterministically to `SUCCEEDED` on read instead, so API shapes are unchanged.

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
build id (`<project>:<uuid>`) and ARN, and returns a build in `IN_PROGRESS`
without blocking the handler on the image pull or container run. `BatchGetBuilds`
returns the real per-phase breakdown as the build progresses and its terminal
`buildStatus` (`SUCCEEDED` / `FAILED` / `STOPPED` / `TIMED_OUT`) once it settles.
`StopBuild` kills the running container and settles `STOPPED`; `RetryBuild`
re-runs the resolved buildspec. `BatchDeleteBuilds`, `ListBuilds`, and
`ListBuildsForProject` complete the build surface. The batch variants
(`StartBuildBatch` / `StopBuildBatch` / `RetryBuildBatch` / `DeleteBuildBatch` /
`BatchGetBuildBatches` / `ListBuildBatches` / `ListBuildBatchesForProject`)
mirror the single-build execution path for `BuildBatch` records.

## Real build execution

When a container runtime is available, the background build task:

- **Resolves the image** from `environment.image`. A user-supplied image is used
  verbatim; an AWS-curated `aws/codebuild/*` image (not publicly pullable) maps
  to a small runnable Ubuntu so the buildspec `commands` execute unchanged.
- **Parses the buildspec** — the inline `source.buildspec` or a
  `StartBuild.buildspecOverride` — reading `env.variables` and the
  `install` / `pre_build` / `build` / `post_build` phase `commands` and the
  `artifacts` block.
- **Runs the phases in the container, carrying state across them** (cwd +
  exported variables are threaded phase-to-phase), so `cd` and `export` in one
  phase persist into the next exactly like AWS — while a failing command
  (including a `exit N`) fails only that phase (recorded FAILED) rather than
  silently aborting the build. The standard
  CodeBuild environment variables are set (`CODEBUILD_BUILD_ID`,
  `CODEBUILD_BUILD_ARN`, `CODEBUILD_SOURCE_VERSION`, `CODEBUILD_BUILD_NUMBER`,
  plus the project's `environmentVariables` and the buildspec `env.variables`).
  `PARAMETER_STORE` and `SECRETS_MANAGER` environment variables are resolved
  cross-service from SSM / Secrets Manager and injected into the container. A
  failing command fails its phase; a failed `install`/`pre_build`/`build` skips
  ahead but `post_build` still runs — matching AWS's phase-failure semantics.
  Each `phases[]` entry carries the real `phaseStatus`, `startTime`, `endTime`,
  `durationInSeconds`, and `contexts`. A build that exceeds its
  `timeoutInMinutes` (default 60, up to 480) is killed and settles `TIMED_OUT`.
- **Streams logs to CloudWatch Logs** into the project's `logsConfig` group and
  stream (or the default `/aws/codebuild/<project>` group), so `Build.logs`
  points at a real, readable log group/stream.
- **Uploads S3 artifacts** — when `artifacts.type == S3`, the declared
  `artifacts.files` glob patterns (`**/*`, `target/*.jar`, `base-directory`,
  `discard-paths`) are matched against the build output and written to the S3
  location (`NONE` or `ZIP` packaging). A pattern that matches nothing fails the
  build, matching AWS. `NO_ARTIFACTS` skips this phase.

`StartBuildBatch` mirrors this single-build execution for real (running the
resolved buildspec in a container and settling `buildBatchStatus` on the exit
code), and reports the BuildBatch-shaped phases (`DOWNLOAD_BATCHSPEC`,
`IN_PROGRESS`, `COMBINE_ARTIFACTS`) with `logConfig`. fakecloud does not fan a
batch out into a build matrix/graph of child builds.

`buildStatus` settles on the real container exit codes. A build that is still
`IN_PROGRESS` when the server restarts (its container is gone) is reconciled to
`FAILED` on the next start rather than left a zombie. Set
`FAKECLOUD_CODEBUILD_DISABLE_BACKEND=1` to force the deterministic
settle-to-`SUCCEEDED`-on-read path instead of running containers.

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
