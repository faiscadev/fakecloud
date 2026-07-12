+++
title = "AWS Elemental MediaConvert"
description = "AWS Elemental MediaConvert (mediaconvert) on fakecloud: a complete 34-operation video-transcoding control plane (100% conformance). restJson1."
weight = 75
+++

fakecloud implements **AWS Elemental MediaConvert** as a restJson1 service. All
**34 operations** ship with **100% conformance** against AWS's own Smithy model,
backed by account-partitioned state that persists across restarts in persistent
mode. MediaConvert signs SigV4 with the `mediaconvert` scope and is routed by
HTTP method plus `@http` URI path under the `/2017-08-29` prefix
(`POST /2017-08-29/queues`, `GET /2017-08-29/jobs/{Id}`,
`PUT /2017-08-29/presets/{Name}`, `POST /2017-08-29/endpoints`, ...).

AWS Elemental MediaConvert on fakecloud is a faithful **transcoding control
plane**: every `CreateQueue` / `CreatePreset` / `CreateJobTemplate` /
`CreateJob` is reflected by its `Get*` / `List*`, every `Delete*` deletes, and a
submitted job's lifecycle is modelled by advancing its stored status on the next
read.

## The transcoding data plane is not implemented (documented, not stubbed)

fakecloud does **not** run a real video transcoder. No media file is read from
the input location and no rendition is written to the output location. A job is
created `SUBMITTED` and settles to `COMPLETE` on the next read with
correctly-shaped `outputGroupDetails` (one `OutputGroupDetail` per configured
output group, each with an empty `outputDetails` list, because nothing was
actually produced) and a populated `timing`. `Probe` returns an empty
`probeResults` list rather than fabricating container/track metadata for a file
it never opened. This is the honest control-plane treatment: clients that create
jobs, poll `GetJob` for terminal status, and manage queues/presets/templates
behave exactly as against AWS, but no pixels are transcoded.

## Resources

- **Queues** - `CreateQueue` (`POST /2017-08-29/queues`) validates the required
  `name` and the `pricingPlan` (`ON_DEMAND` / `RESERVED`), `status`
  (`ACTIVE` / `PAUSED`), and `reservationPlanSettings` enums, then mints an
  `arn:aws:mediaconvert:<region>:<account>:queues/<name>` ARN. A `RESERVED`
  queue materialises a `reservationPlan` (commitment, renewal type, reserved
  slots, purchased/expires timestamps) from its settings. Every account is
  seeded with the `Default` SYSTEM queue, which is `ACTIVE`, `ON_DEMAND`, and
  cannot be deleted (`DeleteQueue` on it returns `ConflictException`).
  `GetQueue` / `ListQueues` / `UpdateQueue` / `DeleteQueue` round-trip the
  stored `Queue`; `ListQueues` honors `listBy` / `order` / `maxResults` /
  `nextToken`.
- **Presets** - `CreatePreset` / `GetPreset` / `ListPresets` / `UpdatePreset` /
  `DeletePreset` over custom presets. The required `settings` (`PresetSettings`:
  audio/caption descriptions, container settings, video description) are stored
  verbatim so a read echoes exactly what the create persisted. `ListPresets`
  filters by `category`.
- **Job templates** - `CreateJobTemplate` / `GetJobTemplate` /
  `ListJobTemplates` / `UpdateJobTemplate` / `DeleteJobTemplate` over custom
  templates, validating the `priority` range (`-50..50`),
  `statusUpdateInterval` enum, and `accelerationSettings.mode`, and storing the
  `JobTemplateSettings` verbatim.
- **Jobs** - `CreateJob` (`POST /2017-08-29/jobs`) validates the required
  `role` and `settings`, mints an `arn:aws:mediaconvert:...:jobs/<id>` job id of
  AWS's `<epoch-millis>-<suffix>` form, defaults the `queue` to the account's
  `Default` queue, and persists the settings, role, priority, hop destinations,
  acceleration settings and user metadata. The job is created `SUBMITTED` and
  settles to `COMPLETE` on the next `GetJob` / `ListJobs` read.
  `CancelJob` moves a non-terminal job to `CANCELED` (a terminal job returns
  `ConflictException`). `ListJobs` / `SearchJobs` filter by `queue`, `status`,
  and (for search) input `fileInput`, honoring `order` and pagination.
- **Policy** - `PutPolicy` / `GetPolicy` / `DeletePolicy` manage the account
  input-restriction policy (`httpInputs` / `httpsInputs` / `s3Inputs`, each
  `ALLOWED` or `DISALLOWED`), defaulting an omitted class to `ALLOWED`.
- **Endpoints** - `DescribeEndpoints` (`POST /2017-08-29/endpoints`) echoes a
  deterministic account-specific endpoint URL that points back at this fakecloud
  host, so an SDK configured to discover endpoints resolves back to the running
  server.
- **Certificates and sharing** - `AssociateCertificate` /
  `DisassociateCertificate` track ACM certificate ARNs; `CreateResourceShare`
  marks a job shared.
- **Jobs queries and versions** - `StartJobsQuery` snapshots the account's jobs
  and `GetJobsQueryResults` returns them deterministically; `ListVersions`
  advertises the engine version.
- **Tagging** - `TagResource` / `UntagResource` / `ListTagsForResource` over
  queue / preset / job-template / job ARNs.

## Validation

Model-derived validation rejects contract violations with the exact error codes
each operation declares: `BadRequestException` for a missing required member, an
out-of-set enum, or an out-of-range integer; `NotFoundException` for a missing
queue / preset / template / job; and `ConflictException` for a duplicate create
or an attempt to delete the `Default` queue.

## Persistence

All queues, presets, job templates, jobs, the policy, certificate associations,
jobs-query records, and tags are account-partitioned and persist across restarts
in persistent mode. The seeded `Default` queue is recreated for any account that
does not yet have one.
