+++
title = "Amazon Pinpoint"
description = "Amazon Pinpoint (pinpoint) on fakecloud: a complete 122-operation control plane (100% conformance). restJson1."
weight = 77
+++

fakecloud implements **Amazon Pinpoint** as a restJson1 service. All **122
operations** ship with **100% conformance** against AWS's own Smithy model,
backed by account-partitioned state that persists across restarts in persistent
mode. Pinpoint signs SigV4 with the `mobiletargeting` scope (aliased internally
to `pinpoint`) and is routed by HTTP method plus `@http` URI path under the
`/v1` prefix (`POST /v1/apps`, `GET /v1/apps/{ApplicationId}`,
`PUT /v1/apps/{ApplicationId}/campaigns/{CampaignId}`,
`POST /v1/templates/{TemplateName}/email`, `GET /v1/tags/{ResourceArn}`, ...).

Amazon Pinpoint on fakecloud is a faithful **control plane**: every
`CreateApp` / `CreateCampaign` / `CreateSegment` / `CreateJourney` /
`Create<Type>Template` is reflected by its `Get*` / `List*`, every `Update`
bumps the version and is listed by the matching `Get*Versions`, and every
`Delete` deletes.

## Resources

- **Apps (projects).** `CreateApp` mints a 32-hex application id plus an
  `arn:aws:mobiletargeting:<region>:<account>:apps/<id>` ARN and stores the name
  and tags. `GetApp` / `DeleteApp` project the `ApplicationResponse`; `GetApps`
  paginates with a round-tripping token. `GetApplicationSettings` /
  `UpdateApplicationSettings` store and return the campaign-hook / limits /
  quiet-time settings resource.
- **Campaigns & Segments.** Versioned: each `UpdateCampaign` / `UpdateSegment`
  bumps `Version` and appends a version record, and `GetCampaignVersions` /
  `GetSegmentVersions` (plus the by-version reads) list them. `CreateSegment`
  derives `SegmentType` (`IMPORT` when an import definition is supplied, else
  `DIMENSIONAL`). `GetCampaignActivities`, `GetSegmentImportJobs`, and
  `GetSegmentExportJobs` return their (empty) lists.
- **Endpoints & Users.** Endpoints are stored per `(appId, endpointId)` with
  their address / channel / attributes / user / opt-out; `UpdateEndpointsBatch`
  upserts many; `GetUserEndpoints` / `DeleteUserEndpoints` operate over the
  endpoints sharing a `User.UserId`; `RemoveAttributes` clears an attribute set.
- **Channels.** Each platform channel (ADM, APNS + sandbox / VoIP / VoIP-sandbox
  variants, Baidu, Email, GCM, SMS, Voice) stores its `Enabled` flag and echoes
  it back on `Get`/`Update` with the platform-specific `Platform` value.
  `GetChannels` returns the configured channel map.
- **Journeys.** `CreateJourney` starts in `DRAFT`; `UpdateJourneyState` drives
  the `DRAFT` -> `ACTIVE` -> ... state machine; `GetJourneyRuns` paginates the
  run list.
- **Templates.** Email / Push / SMS / Voice / InApp templates are versioned;
  `Create`/`Update` append a version, `UpdateTemplateActiveVersion` pins the
  active one, and `ListTemplates` / `ListTemplateVersions` paginate.
- **Import / Export jobs & Event streams.** Jobs mint a `JobId` and are stored
  settled to `COMPLETED`. `PutEventStream` / `GetEventStream` /
  `DeleteEventStream` store the Kinesis/Firehose destination ARN plus role.
- **Recommender configurations & Tags.** Recommenders are a global, persisted
  resource family; tags are ARN-keyed (`TagResource` / `UntagResource` /
  `ListTagsForResource`).

Model-derived validation rejects contract violations with the error codes
Pinpoint declares (`BadRequestException`, `NotFoundException`,
`ForbiddenException`, `TooManyRequestsException`,
`InternalServerErrorException`, `PayloadTooLargeException`,
`MethodNotAllowedException`, `ConflictException`). State is account-partitioned
and persisted.

## Honest gaps

fakecloud's Pinpoint is a control plane, not a delivery engine:

- **No real message delivery.** `SendMessages`, `SendUsersMessages`,
  `SendOTPMessage`, `PutEvents`, and `VerifyOTPMessage` validate their input and
  return a structurally-correct `MessageResponse` / `SendUsersMessageResponse`
  with a per-address delivery-status entry, but transmit nothing to any email /
  SMS / push provider.
- **No analytics engine.** The KPI / execution-metric reads
  (`GetApplicationDateRangeKpi`, `GetCampaignDateRangeKpi`,
  `GetJourneyDateRangeKpi`, `GetJourneyExecutionMetrics`, and the run /
  activity-metric variants) return well-formed responses with an empty / zeroed
  metric result set rather than fabricated numbers.
- **No S3 job processing.** Import / export jobs mint a job id and settle to
  `COMPLETED` so the read path is realistic, but no CSV/JSON is actually read
  from or written to S3.
