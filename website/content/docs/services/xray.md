+++
title = "AWS X-Ray"
description = "AWS X-Ray (xray) on fakecloud: a complete 38-operation surface (100% conformance) with a real in-memory trace data plane — segment ingestion, service-graph derivation, sampling rules, and groups. restJson1."
weight = 74
+++

fakecloud implements **AWS X-Ray** as a restJson1 service. All **38 operations**
ship with **100% conformance** against AWS's own Smithy model, backed by
account-partitioned state that persists across restarts in persistent mode.
X-Ray signs SigV4 with the `xray` scope; every operation is a fixed
`POST /<UriPath>` (e.g. `POST /TraceSegments`, `POST /ServiceGraph`), routed by
its `@http` URI path.

Unlike a pure control plane, X-Ray on fakecloud has a **real trace data plane**
modelled deterministically in memory — not stubbed. Trace segment documents you
send are parsed and stored, and the reads that assemble traces and derive the
service graph compute over that real ingested data.

## Data plane: trace ingestion and the service graph

- **`PutTraceSegments`** ingests a batch of X-Ray
  [segment documents](https://docs.aws.amazon.com/xray/latest/devguide/xray-api-segmentdocuments.html)
  (JSON strings). Each document is parsed for its `trace_id`, `id`, `name`,
  `start_time`, `end_time`, `parent_id`, `origin`, the `http` request/response
  (method, URL, status), the `error` / `fault` / `throttle` flags, and its
  nested `subsegments` (walking `namespace: "remote"` subsegments as downstream
  service calls). Segments are stored keyed by trace id; a malformed document
  comes back in `UnprocessedTraceSegments` with an error code. The raw document
  is retained verbatim.

- **`BatchGetTraces`** reassembles stored traces for the requested trace ids,
  returning each `Trace` with its `Duration` and its `Segments` (each segment's
  `Id` plus its original `Document`). Unknown ids come back in
  `UnprocessedTraceIds`.

- **`GetTraceSummaries`** returns a `TraceSummary` for every stored trace whose
  start time falls in `[StartTime, EndTime]`, with `Duration`, `ResponseTime`,
  the `HasFault` / `HasError` / `HasThrottle` flags, the entry-point `Http`
  info, and the `ServiceIds` in the trace.

- **`GetServiceGraph`** and **`GetTraceGraph`** derive the X-Ray service graph
  from the stored segments: a node per service (named by the segment `name`,
  typed by its `origin`), an edge per `namespace: "remote"` downstream call, and
  per-node / per-edge `SummaryStatistics` (`OkCount`, `ErrorStatistics`,
  `FaultStatistics`, `TotalCount`, `TotalResponseTime`). Entry nodes are flagged
  `Root`. `GetServiceGraph` derives from a time range; `GetTraceGraph` from an
  explicit set of trace ids.

- **`GetTimeSeriesServiceStatistics`** buckets the window by `Period` and
  aggregates the same service statistics per bucket.

## Control plane

- **Sampling rules** - `CreateSamplingRule` / `GetSamplingRules` /
  `UpdateSamplingRule` / `DeleteSamplingRule`, plus `GetSamplingTargets` and
  `GetSamplingStatisticSummaries`. Every account is seeded with the built-in
  **`Default`** sampling rule (priority 10000, 1 req/s reservoir + 5% of the
  rest), which `GetSamplingRules` returns and which cannot be deleted.

- **Groups** - `CreateGroup` / `GetGroup` / `GetGroups` / `UpdateGroup` /
  `DeleteGroup`, each carrying a `GroupName`, an
  `arn:aws:xray:<region>:<account>:group/<name>/<id>` ARN, a `FilterExpression`,
  and an `InsightsConfiguration`.

- **Encryption config** - `GetEncryptionConfig` returns the account config
  (defaulting to `Type: NONE`, `Status: ACTIVE`); `PutEncryptionConfig` sets
  `NONE` or a `KMS` key.

- **Resource policies** - `PutResourcePolicy` / `DeleteResourcePolicy` /
  `ListResourcePolicies`.

- **Indexing rule + trace-segment destination** - `GetIndexingRules` /
  `UpdateIndexingRule`, and `GetTraceSegmentDestination` /
  `UpdateTraceSegmentDestination` (`XRay` or `CloudWatchLogs`).

- **Tags** - ARN-keyed `TagResource` / `UntagResource` / `ListTagsForResource`
  over groups and sampling rules, returning `ResourceNotFoundException` for an
  unknown ARN.

- **Telemetry** - `PutTelemetryRecords` accepts the X-Ray daemon's internal
  telemetry batch.

Every operation applies model-derived validation (required members, string
`@length`, integer `@range`, and enum sets), returning the error code each
operation declares — `InvalidRequestException` universally, plus
`ResourceNotFoundException`, `RuleLimitExceededException`, and
`TooManyTagsException` where declared.

## Known limitations

- **Filter expressions** - `GetTraceSummaries` implements a documented subset of
  the X-Ray filter-expression language: an empty expression matches every trace;
  the boolean keywords `fault`, `error`, and `throttle` match traces carrying
  that flag; and `service("name")` matches traces containing that service node.
  Any richer expression is treated leniently as a match (it never silently drops
  data) rather than fully parsed.

- **Insights** - X-Ray insights are generated by AWS from anomaly detection over
  ingested traces. fakecloud does not run anomaly detection, so `GetInsight` /
  `GetInsightEvents` / `GetInsightImpactGraph` report a nonexistent insight and
  `GetInsightSummaries` returns an empty page.

- **Transaction Search retrieval** - `StartTraceRetrieval` /
  `ListRetrievedTraces` / `GetRetrievedTracesGraph` / `CancelTraceRetrieval`
  track retrieval tokens and derive the retrieved-traces service graph from the
  stored segments, but return the retrieved trace spans as metadata only.
