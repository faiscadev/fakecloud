+++
title = "AWS Amplify"
description = "AWS Amplify (amplify) on fakecloud: a complete 37-operation hosting control plane (100% conformance). restJson1."
weight = 75
+++

fakecloud implements **AWS Amplify** as a restJson1 service. All **37
operations** ship with **100% conformance** against AWS's own Smithy model,
backed by account-partitioned state that persists across restarts in persistent
mode. Amplify signs SigV4 with the `amplify` scope and is routed by HTTP method
plus `@http` URI path (`POST /apps`, `GET /apps/{appId}`,
`POST /apps/{appId}/branches`,
`DELETE /apps/{appId}/branches/{branchName}/jobs/{jobId}/stop`, ...).

AWS Amplify on fakecloud is a faithful **hosting control plane**: every
`CreateApp` / `CreateBranch` / `CreateDomainAssociation` is reflected by its
`Get*` / `List*`, every `Delete*` deletes, and AWS's async domain-verification
and build-job lifecycles are modelled by advancing the stored status on the next
read, with any interrupted transition reconciled on restart.

**No separately-emulable data plane.** Amplify is a hosting control plane: the
built application artifacts are produced by AWS's managed CI/CD build farm, which
has no local equivalent. Rather than fabricate a running build, fakecloud settles
build jobs deterministically through their documented states (`PENDING` ->
`PROVISIONING` -> `RUNNING` -> `SUCCEED`) and records a completed `BUILD` step,
and manual deployments hand back presigned S3 upload URLs. This is the honest
control-plane treatment, not a stub.

## Resources

- **Apps** - `CreateApp` (`POST /apps`) validates the required `name`, the
  `platform` / `cacheConfig.type` / `jobConfig.buildComputeType` enums, and the
  model's `@length` constraints, then mints a `d`-prefixed app id
  (`^d[a-z0-9]+$`), an `arn:aws:amplify:<region>:<account>:apps/<id>` ARN, and a
  `<id>.amplifyapp.com` default domain. `GetApp` / `ListApps` echo the full
  `App` shape (name, description, repository, platform, environment variables,
  custom rules, custom headers, auto-branch-creation config, cache config,
  job/compute config, basic auth, ...). `UpdateApp` applies the changed members
  and bumps `updateTime`; `DeleteApp` removes the app and cascades its webhooks.

- **Branches** - `CreateBranch` (`POST /apps/{appId}/branches`) requires an
  existing app (else `NotFoundException`), stores the branch under its app with a
  per-branch job counter, and returns the full `Branch` shape (stage, framework,
  environment variables, TTL, thumbnail URL, ...). `Get`/`List`/`Update`/`Delete`
  round-trip the branch; deleting a branch drops its jobs.

- **Domain associations** - `CreateDomainAssociation`
  (`POST /apps/{appId}/domains`) requires `domainName` + `subDomainSettings`,
  builds the `subDomains` (each with a CNAME `dnsRecord`) plus the ACM
  `certificateVerificationDNSRecord`, and starts the association `CREATING`. The
  verification lifecycle settles to `AVAILABLE` on the next read and marks each
  subdomain `verified`. `Update` re-enters verification; `Get`/`List`/`Delete`
  behave as expected.

- **Webhooks** - `CreateWebhook` (`POST /apps/{appId}/webhooks`) requires an
  existing app + branch and returns a generated
  `https://webhooks.amplify.<region>.amazonaws.com/...` trigger URL. Webhooks are
  addressed directly by id (`GET`/`POST`/`DELETE /webhooks/{webhookId}`) as well
  as listed per app (`GET /apps/{appId}/webhooks`).

- **Backend environments** - `CreateBackendEnvironment`
  (`POST /apps/{appId}/backendenvironments`) stores the environment (stack name,
  deployment artifacts) under its app; `Get`/`List`/`Delete` round-trip it, and
  `ListBackendEnvironments` honours the `environmentName` filter query.

- **Jobs & deployments** - `StartJob`
  (`POST /apps/{appId}/branches/{branchName}/jobs`) validates the required
  `jobType` enum, allocates a numeric job id, and creates the job `PENDING`. On
  successive reads (`GetJob` / `ListJobs`) the build settles `PENDING` ->
  `PROVISIONING` -> `RUNNING` -> `SUCCEED`, stamping `endTime` and a completed
  `BUILD` step. `StopJob` moves a job to `CANCELLED`; `DeleteJob` removes it.
  `CreateDeployment` returns presigned S3 `fileUploadUrls` + `zipUploadUrl` (and
  reserves a `CREATED` job when the branch exists); `StartDeployment` begins the
  build.

- **Artifacts** - a settled (`SUCCEED`) build exposes its build log via
  `ListArtifacts` (`.../jobs/{jobId}/artifacts`). `GetArtifactUrl`
  (`GET /artifacts/{artifactId}`) resolves a reversible artifact id back to a
  presigned download URL, returning `NotFoundException` for an unknown id.

- **Access logs** - `GenerateAccessLogs` (`POST /apps/{appId}/accesslogs`)
  validates the required `domainName` against an existing app and returns a
  presigned `logUrl`.

- **Tagging** - `TagResource` / `UntagResource` / `ListTagsForResource` address
  a resource by its ARN (`/tags/{resourceArn}`); tags live on apps and branches
  (the taggable Amplify resources) so they round-trip on `GetApp` / `GetBranch`
  and survive restarts. An arn that does not match `^arn:aws:amplify:` is a
  `BadRequestException`; one that resolves to no resource is a
  `ResourceNotFoundException`. `UntagResource` reads its repeated multi-value
  `tagKeys` query parameter intact.

## Validation

Input validation is derived from the Smithy model: required members, enum
membership (`Platform`, `Stage`, `JobType`, `SourceUrlType`, `CertificateType`,
`CacheConfigType`, `BuildComputeType`), string `@length` / `@pattern` limits (app
id, branch name, and the app/branch configuration strings), and `maxResults` /
`nextToken` bounds on the list operations. Failures return each operation's
declared error code (`BadRequestException`, `NotFoundException`, or
`ResourceNotFoundException` for the tag operations) so clients see AWS-shaped
responses.

## Persistence

All Amplify state is account-partitioned and persisted in persistent mode;
in-flight domain-verification and build-job transitions reconcile on restart.
