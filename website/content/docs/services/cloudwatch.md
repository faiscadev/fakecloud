+++
title = "CloudWatch (Metrics & Alarms)"
description = "Amazon CloudWatch metrics, alarms, dashboards, anomaly detectors, insight rules, and metric streams. awsQuery, awsJson1_0 and Smithy RPC v2 CBOR protocols."
weight = 33
+++

fakecloud implements Amazon CloudWatch's metrics-and-alarms surface (the `monitoring` SigV4 service) — distinct from [CloudWatch Logs](/docs/services/logs/), which is a separate service. CloudWatch's Smithy model advertises Smithy RPC v2 CBOR, `awsJson1_0` and the legacy `awsQuery` protocol (XML); fakecloud accepts all three, and each caller receives a response in the protocol it used. Current aws-sdk-rust speaks RPC v2 CBOR (`POST /service/GraniteServiceVersion20100801/operation/<Operation>` with `smithy-protocol: rpc-v2-cbor` and CBOR bodies: tag-1 epoch timestamps, byte-string blobs), aws-sdk-js-v3 and botocore send `X-Amz-Target: GraniteServiceVersion20100801.<Operation>` with a JSON body, and older SDKs use awsQuery. CloudWatch is `awsQueryCompatible`, so CBOR and JSON errors carry the awsQuery error code in the `x-amzn-query-error` header the SDKs use to pick the modeled error. All 49 operations are implemented with persisted in-memory state.

**Status: full control plane. Metrics are stored in memory and do not persist across server restarts; alarm evaluation is driven by the metric data you publish, not by a background sampling loop.**

## Supported today

- **Metrics** — `PutMetricData`, `GetMetricData`, `GetMetricStatistics`, `ListMetrics`, `GetMetricWidgetImage` (returns a deterministic PNG blob). Custom namespaces, dimensions, and statistics round-trip.
- **Alarms** — `PutMetricAlarm`, `PutCompositeAlarm`, `DescribeAlarms`, `DescribeAlarmsForMetric`, `DescribeAlarmHistory`, `DeleteAlarms`, `SetAlarmState`, `EnableAlarmActions`, `DisableAlarmActions`, `DescribeAlarmContributors`. Threshold transitions trigger configured SNS / Application Auto Scaling / EC2 actions.
- **Dashboards** — `PutDashboard`, `GetDashboard`, `ListDashboards`, `DeleteDashboards`.
- **Anomaly detectors** — `PutAnomalyDetector`, `DescribeAnomalyDetectors`, `DeleteAnomalyDetector` (single-metric, metric-math, and metric-stat detectors).
- **Insight rules** — `PutInsightRule`, `DescribeInsightRules`, `EnableInsightRules`, `DisableInsightRules`, `DeleteInsightRules`, `GetInsightRuleReport`, plus managed rules (`PutManagedInsightRules`, `ListManagedInsightRules`).
- **Metric streams** — `PutMetricStream`, `GetMetricStream`, `ListMetricStreams`, `StartMetricStreams`, `StopMetricStreams`, `DeleteMetricStream`. State flips between `running` and `stopped`.
- **Alarm mute rules** — `PutAlarmMuteRule`, `GetAlarmMuteRule`, `ListAlarmMuteRules`, `DeleteAlarmMuteRule`.
- **OTel enrichment** — `GetOTelEnrichment`, `StartOTelEnrichment`, `UpdateOTelEnrichment`, `StopOTelEnrichment`, with per-namespace include / exclude metric filters (`UpdateOTelEnrichment` replaces both lists and requires enrichment to be running).
- **Resource metrics configurations**: `CreateResourceMetricsConfiguration`, `GetResourceMetricsConfiguration`, `UpdateResourceMetricsConfiguration`, `DeleteResourceMetricsConfiguration`: one configuration per resource ARN with an optional metric selection; a duplicate create is `ConflictException`.
- **Tagging** — `TagResource`, `UntagResource`, `ListTagsForResource`.

## Introspection

Two IAM-bypass admin endpoints expose CloudWatch state so test assertions don't have to round-trip through the AWS SDK:

- `GET /_fakecloud/cloudwatch/alarms` — every metric **and** composite alarm across all accounts and regions. Each entry carries `accountId`, `region`, `name`, `type` (`metric` or `composite`), `state`, `stateReason`, `stateUpdatedTimestamp`, `actionsEnabled`, and the `alarmActions` / `okActions` / `insufficientDataActions` lists. Metric alarms add `namespace`, `metricName`, `threshold`, `comparisonOperator`; composite alarms add `alarmRule`. Sorted by account, region, name.
- `GET /_fakecloud/cloudwatch/metrics` — every unique metric series keyed by (account, region, namespace, metric, dimensions). Each entry carries `dimensions` (`[{name, value}]`), `datapointCount`, and `latest` (`{timestamp, value, unit}` or `null`). Sorted by account, region, namespace, metric.

All first-party SDKs ship a `cloudwatch` sub-client wrapping these endpoints (`getAlarms()`, `getMetrics()`). See [`reference/introspection`](/docs/reference/introspection/) for the full endpoint catalog.

## Not implemented

- No background metric sampling — alarms evaluate against the data points you publish via `PutMetricData` / `SetAlarmState`.
- Metric data is in-memory only and is lost on restart.
- Metric streams persist configuration and state but do not actually fan out data points to the configured Firehose.
