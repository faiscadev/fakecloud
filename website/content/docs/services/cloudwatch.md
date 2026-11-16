+++
title = "CloudWatch (Metrics & Alarms)"
description = "Amazon CloudWatch metrics, alarms, dashboards, anomaly detectors, insight rules, and metric streams. awsQuery, awsJson1_0 and Smithy RPC v2 CBOR protocols."
weight = 33
+++

fakecloud implements Amazon CloudWatch's metrics-and-alarms surface (the `monitoring` SigV4 service) — distinct from [CloudWatch Logs](/docs/services/logs/), which is a separate service. CloudWatch's Smithy model advertises Smithy RPC v2 CBOR, `awsJson1_0` and the legacy `awsQuery` protocol (XML); fakecloud accepts all three, and each caller receives a response in the protocol it used. Current aws-sdk-rust speaks RPC v2 CBOR (`POST /service/GraniteServiceVersion20100801/operation/<Operation>` with `smithy-protocol: rpc-v2-cbor` and CBOR bodies: tag-1 epoch timestamps, byte-string blobs), aws-sdk-js-v3 and botocore send `X-Amz-Target: GraniteServiceVersion20100801.<Operation>` with a JSON body, and older SDKs use awsQuery. CloudWatch is `awsQueryCompatible`, so CBOR and JSON errors carry the awsQuery error code in the `x-amzn-query-error` header the SDKs use to pick the modeled error. All 55 operations are implemented with persisted in-memory state.

**Status: full control plane. Metrics are stored in memory and do not persist across server restarts; alarms evaluate the metric data you publish (and are re-evaluated on a timer as it ages); resources are not sampled for metrics in the background.**

## Supported today

- **Metrics** — `PutMetricData`, `GetMetricData`, `GetMetricStatistics`, `ListMetrics`, `GetMetricWidgetImage` (returns a deterministic PNG blob). Custom namespaces, dimensions, and statistics round-trip.
- **Alarms** — `PutMetricAlarm`, `PutCompositeAlarm`, `DescribeAlarms`, `DescribeAlarmsForMetric`, `DescribeAlarmHistory`, `DeleteAlarms`, `SetAlarmState`, `EnableAlarmActions`, `DisableAlarmActions`, `DescribeAlarmContributors`. When an alarm with `ActionsEnabled` changes state (by evaluation or `SetAlarmState`), the actions listed for the new state run: SNS topics receive the AWS alarm notification JSON (Subject `ALARM: "<name>" in <Region name>`), Lambda functions are invoked asynchronously with the alarm event, EC2 actions (`arn:aws:automate:<region>:ec2:stop|terminate|reboot|recover` and the `arn:aws:swf:...:action/actions/AWS_EC2.InstanceId.<Op>/1.0` forms) act on the instance in the alarm's `InstanceId` dimension through EC2 (`recover` reboots it), and Application Auto Scaling policies scale their targets. Each SNS, Lambda and EC2 action is recorded in `DescribeAlarmHistory` as an `Action` item (`Successfully executed action <arn>` / `Failed to execute action <arn>`); other action types (EC2 Auto Scaling group policies, Systems Manager OpsItems, Incident Manager) are recorded as failed. Alarms are evaluated on every `PutMetricData`, `SetAlarmState`, `Put*Alarm` and `DescribeAlarms`, and on a 10-second timer. Alarm mute rules are stored but do not mute actions. Alarms return the computed state outputs: `StateReasonData` (the evaluated datapoints for a metric alarm, the triggering children for a composite alarm, or the value given to `SetAlarmState`), `StateTransitionedTimestamp` (moves only when `StateValue` changes, unlike `StateUpdatedTimestamp`), `EvaluationState` (`EVALUATION_ERROR` for a single-metric alarm whose configuration can't be evaluated, such as one with no `Threshold`), and on composite alarms with an `ActionsSuppressor`, `ActionsSuppressedBy` / `ActionsSuppressedReason` (`WaitPeriod`, `Alarm`, `ExtensionPeriod`). A suppressed composite alarm performs no actions; if it is still in ALARM when the suppression ends, it performs them then. A composite alarm set with `SetAlarmState` keeps that state until a child alarm changes state or its configuration is updated. `DescribeAlarmHistory` records each transition with both states' reasons and honors `AlarmTypes` (metric alarms only by default).
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

- No background metric sampling: alarms evaluate against the data points you publish via `PutMetricData`, or the state you set with `SetAlarmState`.
- Alarm mute rules don't mute alarm actions, and EC2 Auto Scaling group policy, Systems Manager and Incident Manager alarm actions aren't performed.
- Metric data is in-memory only and is lost on restart.
- Metric streams persist configuration and state but do not actually fan out data points to the configured Firehose.
