+++
title = "CloudWatch (Metrics & Alarms)"
description = "Amazon CloudWatch metrics, alarms, dashboards, anomaly detectors, insight rules, and metric streams. awsQuery protocol."
weight = 33
+++

fakecloud implements Amazon CloudWatch's metrics-and-alarms surface (the `monitoring` SigV4 service, awsQuery protocol) — distinct from [CloudWatch Logs](/docs/services/logs/), which is a separate service. All 46 operations are implemented with persisted in-memory state.

**Status: full control plane. Metrics are stored in memory and do not persist across server restarts; alarm evaluation is driven by the metric data you publish, not by a background sampling loop.**

## Supported today

- **Metrics** — `PutMetricData`, `GetMetricData`, `GetMetricStatistics`, `ListMetrics`, `GetMetricWidgetImage` (returns a deterministic PNG blob). Custom namespaces, dimensions, and statistics round-trip.
- **Alarms** — `PutMetricAlarm`, `PutCompositeAlarm`, `DescribeAlarms`, `DescribeAlarmsForMetric`, `DescribeAlarmHistory`, `DeleteAlarms`, `SetAlarmState`, `EnableAlarmActions`, `DisableAlarmActions`, `DescribeAlarmContributors`. Threshold transitions trigger configured SNS / Application Auto Scaling / EC2 actions.
- **Dashboards** — `PutDashboard`, `GetDashboard`, `ListDashboards`, `DeleteDashboards`.
- **Anomaly detectors** — `PutAnomalyDetector`, `DescribeAnomalyDetectors`, `DeleteAnomalyDetector` (single-metric, metric-math, and metric-stat detectors).
- **Insight rules** — `PutInsightRule`, `DescribeInsightRules`, `EnableInsightRules`, `DisableInsightRules`, `DeleteInsightRules`, `GetInsightRuleReport`, plus managed rules (`PutManagedInsightRules`, `ListManagedInsightRules`).
- **Metric streams** — `PutMetricStream`, `GetMetricStream`, `ListMetricStreams`, `StartMetricStreams`, `StopMetricStreams`, `DeleteMetricStream`. State flips between `running` and `stopped`.
- **Alarm mute rules** — `PutAlarmMuteRule`, `GetAlarmMuteRule`, `ListAlarmMuteRules`, `DeleteAlarmMuteRule`.
- **OTel enrichment** — `GetOTelEnrichment`, `StartOTelEnrichment`, `StopOTelEnrichment`.
- **Tagging** — `TagResource`, `UntagResource`, `ListTagsForResource`.

## Not implemented

- No background metric sampling — alarms evaluate against the data points you publish via `PutMetricData` / `SetAlarmState`.
- Metric data is in-memory only and is lost on restart.
- Metric streams persist configuration and state but do not actually fan out data points to the configured Firehose.
