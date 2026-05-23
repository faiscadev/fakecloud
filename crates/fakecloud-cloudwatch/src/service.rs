use std::collections::{BTreeMap, HashMap};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::StatusCode;

use fakecloud_core::query::{
    optional_query_param, query_metadata_only_xml, query_response_xml, required_query_param,
};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use std::sync::Arc;

use fakecloud_persistence::SnapshotStore;
use tokio::sync::Mutex;

use crate::state::{
    AlarmState, CloudWatchSnapshot, Dashboard, MetricAlarm, MetricDatum, SharedCloudWatchState,
    StatisticSet, CLOUDWATCH_SNAPSHOT_SCHEMA_VERSION,
};

const NS: &str = "http://monitoring.amazonaws.com/doc/2010-08-01/";

const SUPPORTED_ACTIONS: &[&str] = &[
    "PutMetricData",
    "GetMetricStatistics",
    "GetMetricData",
    "ListMetrics",
    "PutMetricAlarm",
    "DescribeAlarms",
    "DescribeAlarmsForMetric",
    "DeleteAlarms",
    "EnableAlarmActions",
    "DisableAlarmActions",
    "SetAlarmState",
    "DescribeAlarmHistory",
];

pub struct CloudWatchService {
    state: SharedCloudWatchState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<Mutex<()>>,
}

impl CloudWatchService {
    pub fn new(state: SharedCloudWatchState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Attach a `SnapshotStore` so alarms / dashboards / metrics survive
    /// restarts. Without this, all CloudWatch state is in-memory only —
    /// alarms wired to actions fire on a freshly-started process.
    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Persist current state as a snapshot. Cloned + serialized under
    /// the snapshot lock so concurrent mutators can't race a stale-last
    /// write.
    pub(crate) async fn save_snapshot(&self) {
        let Some(store) = self.snapshot_store.clone() else {
            return;
        };
        let _guard = self.snapshot_lock.lock().await;
        let snapshot = CloudWatchSnapshot {
            schema_version: CLOUDWATCH_SNAPSHOT_SCHEMA_VERSION,
            accounts: self.state.read().clone_for_snapshot(),
        };
        let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let bytes = serde_json::to_vec(&snapshot)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            store.save(&bytes)
        })
        .await;
        match join {
            Ok(Ok(())) => {}
            Ok(Err(err)) => tracing::error!(%err, "failed to write cloudwatch snapshot"),
            Err(err) => tracing::error!(%err, "cloudwatch snapshot task panicked"),
        }
    }
}

#[async_trait]
impl AwsService for CloudWatchService {
    fn service_name(&self) -> &str {
        "monitoring"
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = matches!(
            req.action.as_str(),
            "PutMetricData"
                | "PutMetricAlarm"
                | "DeleteAlarms"
                | "EnableAlarmActions"
                | "DisableAlarmActions"
                | "SetAlarmState"
                | "PutDashboard"
                | "DeleteDashboards"
        );
        let result = match req.action.as_str() {
            "PutMetricData" => self.put_metric_data(&req),
            "GetMetricStatistics" => self.get_metric_statistics(&req),
            "GetMetricData" => self.get_metric_data(&req),
            "ListMetrics" => self.list_metrics(&req),
            "PutMetricAlarm" => self.put_metric_alarm(&req),
            "DescribeAlarms" => self.describe_alarms(&req),
            "DescribeAlarmsForMetric" => self.describe_alarms_for_metric(&req),
            "DeleteAlarms" => self.delete_alarms(&req),
            "EnableAlarmActions" => self.enable_alarm_actions(&req),
            "DisableAlarmActions" => self.disable_alarm_actions(&req),
            "SetAlarmState" => self.set_alarm_state(&req),
            "DescribeAlarmHistory" => self.describe_alarm_history(&req),
            "PutDashboard" => self.put_dashboard(&req),
            "GetDashboard" => self.get_dashboard(&req),
            "DeleteDashboards" => self.delete_dashboards(&req),
            "ListDashboards" => self.list_dashboards(&req),
            _ => Err(AwsServiceError::action_not_implemented(
                "monitoring",
                &req.action,
            )),
        };
        if mutates && result.is_ok() {
            self.save_snapshot().await;
        }
        result
    }
}

fn xml_response(action: &str, inner: &str, request_id: &str) -> AwsResponse {
    AwsResponse::xml(
        StatusCode::OK,
        query_response_xml(action, NS, inner, request_id),
    )
}

fn empty_metadata_response(action: &str, request_id: &str) -> AwsResponse {
    AwsResponse::xml(
        StatusCode::OK,
        query_metadata_only_xml(action, NS, request_id),
    )
}

fn invalid_param(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidParameterValue", message)
}

fn collect_indexed(req: &AwsRequest, prefix: &str) -> Vec<HashMap<String, String>> {
    let mut by_index: BTreeMap<u32, HashMap<String, String>> = BTreeMap::new();
    let needle = format!("{prefix}.member.");
    for (k, v) in req.query_params.iter() {
        let Some(rest) = k.strip_prefix(&needle) else {
            continue;
        };
        let mut parts = rest.splitn(2, '.');
        let Some(idx_str) = parts.next() else {
            continue;
        };
        let Ok(idx) = idx_str.parse::<u32>() else {
            continue;
        };
        let field = parts.next().unwrap_or("").to_string();
        by_index.entry(idx).or_default().insert(field, v.clone());
    }
    by_index.into_values().collect()
}

fn parse_dimensions(member: &HashMap<String, String>, prefix: &str) -> BTreeMap<String, String> {
    let mut dims: BTreeMap<u32, (Option<String>, Option<String>)> = BTreeMap::new();
    let needle = format!("{prefix}.member.");
    for (k, v) in member.iter() {
        let Some(rest) = k.strip_prefix(&needle) else {
            continue;
        };
        let mut parts = rest.splitn(2, '.');
        let Some(idx_str) = parts.next() else {
            continue;
        };
        let Ok(idx) = idx_str.parse::<u32>() else {
            continue;
        };
        let field = parts.next().unwrap_or("");
        let entry = dims.entry(idx).or_default();
        match field {
            "Name" => entry.0 = Some(v.clone()),
            "Value" => entry.1 = Some(v.clone()),
            _ => {}
        }
    }
    let mut out = BTreeMap::new();
    for (_, (name, value)) in dims {
        if let (Some(n), Some(v)) = (name, value) {
            out.insert(n, v);
        }
    }
    out
}

fn parse_dimensions_query(req: &AwsRequest, prefix: &str) -> BTreeMap<String, String> {
    let mut dims: BTreeMap<u32, (Option<String>, Option<String>)> = BTreeMap::new();
    let needle = format!("{prefix}.member.");
    for (k, v) in req.query_params.iter() {
        let Some(rest) = k.strip_prefix(&needle) else {
            continue;
        };
        let mut parts = rest.splitn(2, '.');
        let Some(idx_str) = parts.next() else {
            continue;
        };
        let Ok(idx) = idx_str.parse::<u32>() else {
            continue;
        };
        let field = parts.next().unwrap_or("");
        let entry = dims.entry(idx).or_default();
        match field {
            "Name" => entry.0 = Some(v.clone()),
            "Value" => entry.1 = Some(v.clone()),
            _ => {}
        }
    }
    let mut out = BTreeMap::new();
    for (_, (name, value)) in dims {
        if let (Some(n), Some(v)) = (name, value) {
            out.insert(n, v);
        }
    }
    out
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Per-datapoint aggregation summary covering both the simple `Value` form
/// and the `StatisticValues` form so callers don't lose the count or
/// min/max baked into a `StatisticSet`.
#[derive(Clone, Copy)]
struct DatumStats {
    sum: f64,
    min: f64,
    max: f64,
    count: f64,
}

fn datum_stats(d: &MetricDatum) -> Option<DatumStats> {
    if let Some(v) = d.value {
        return Some(DatumStats {
            sum: v,
            min: v,
            max: v,
            count: 1.0,
        });
    }
    if let Some(s) = &d.statistic_values {
        return Some(DatumStats {
            sum: s.sum,
            min: s.minimum,
            max: s.maximum,
            count: s.sample_count,
        });
    }
    None
}

fn merge_stats(acc: &mut DatumStats, other: DatumStats) {
    acc.sum += other.sum;
    acc.count += other.count;
    if other.min < acc.min {
        acc.min = other.min;
    }
    if other.max > acc.max {
        acc.max = other.max;
    }
}

fn stat_value(stat: &str, agg: DatumStats) -> Option<f64> {
    match stat {
        "Sum" => Some(agg.sum),
        "Average" => {
            if agg.count > 0.0 {
                Some(agg.sum / agg.count)
            } else {
                None
            }
        }
        "Minimum" => Some(agg.min),
        "Maximum" => Some(agg.max),
        "SampleCount" => Some(agg.count),
        _ => None,
    }
}

fn render_dimensions(dims: &BTreeMap<String, String>) -> String {
    let mut s = String::from("<Dimensions>");
    for (name, value) in dims.iter() {
        s.push_str(&format!(
            "<member><Name>{}</Name><Value>{}</Value></member>",
            xml_escape(name),
            xml_escape(value),
        ));
    }
    s.push_str("</Dimensions>");
    s
}

impl CloudWatchService {
    fn put_metric_data(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let namespace = required_query_param(req, "Namespace")?;
        let members = collect_indexed(req, "MetricData");
        if members.is_empty() {
            return Err(invalid_param(
                "PutMetricData requires at least one MetricData entry",
            ));
        }

        let now = Utc::now();
        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        let metrics_map = acct.metrics_in_mut(&req.region);
        let bucket = metrics_map.entry(namespace.clone()).or_default();

        for member in members {
            let metric_name = member
                .get("MetricName")
                .cloned()
                .ok_or_else(|| invalid_param("MetricData.member.N.MetricName is required"))?;
            let value = member
                .get("Value")
                .map(|s| s.parse::<f64>())
                .transpose()
                .map_err(|_| invalid_param("Value must be a valid number"))?;
            let timestamp = member
                .get("Timestamp")
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|d| d.with_timezone(&Utc))
                .unwrap_or(now);
            let unit = member.get("Unit").cloned();
            let storage_resolution = member
                .get("StorageResolution")
                .and_then(|s| s.parse::<i64>().ok());
            let dimensions = parse_dimensions(&member, "Dimensions");

            let statistic_values = if let (Some(sc), Some(sum), Some(min), Some(max)) = (
                member.get("StatisticValues.SampleCount"),
                member.get("StatisticValues.Sum"),
                member.get("StatisticValues.Minimum"),
                member.get("StatisticValues.Maximum"),
            ) {
                Some(StatisticSet {
                    sample_count: sc.parse::<f64>().map_err(|_| {
                        invalid_param("StatisticValues.SampleCount must be a number")
                    })?,
                    sum: sum
                        .parse::<f64>()
                        .map_err(|_| invalid_param("StatisticValues.Sum must be a number"))?,
                    minimum: min
                        .parse::<f64>()
                        .map_err(|_| invalid_param("StatisticValues.Minimum must be a number"))?,
                    maximum: max
                        .parse::<f64>()
                        .map_err(|_| invalid_param("StatisticValues.Maximum must be a number"))?,
                })
            } else {
                None
            };

            if value.is_none() && statistic_values.is_none() {
                return Err(invalid_param(
                    "MetricData entry must supply either Value or StatisticValues",
                ));
            }

            bucket.push(MetricDatum {
                metric_name,
                dimensions,
                timestamp,
                value,
                statistic_values,
                unit,
                storage_resolution,
            });
        }

        Ok(empty_metadata_response("PutMetricData", &req.request_id))
    }

    fn list_metrics(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let namespace = optional_query_param(req, "Namespace");
        let metric_name = optional_query_param(req, "MetricName");
        let dim_filter = parse_dimensions_query(req, "Dimensions");

        let state = self.state.read();
        let mut out = String::from("<Metrics>");
        if let Some(acct) = state.get(&req.account_id) {
            if let Some(map) = acct.metrics_in(&req.region) {
                for (ns, data) in map.iter() {
                    if let Some(filter_ns) = namespace.as_ref() {
                        if ns != filter_ns {
                            continue;
                        }
                    }
                    let mut seen: BTreeMap<(String, BTreeMap<String, String>), ()> =
                        BTreeMap::new();
                    for d in data.iter() {
                        if let Some(filter_name) = metric_name.as_ref() {
                            if &d.metric_name != filter_name {
                                continue;
                            }
                        }
                        if !dim_filter.is_empty()
                            && !dim_filter
                                .iter()
                                .all(|(k, v)| d.dimensions.get(k) == Some(v))
                        {
                            continue;
                        }
                        seen.insert((d.metric_name.clone(), d.dimensions.clone()), ());
                    }
                    for ((name, dims), _) in seen {
                        out.push_str("<member>");
                        out.push_str(&format!("<Namespace>{}</Namespace>", xml_escape(ns)));
                        out.push_str(&format!("<MetricName>{}</MetricName>", xml_escape(&name)));
                        out.push_str(&render_dimensions(&dims));
                        out.push_str("</member>");
                    }
                }
            }
        }
        out.push_str("</Metrics>");

        Ok(xml_response("ListMetrics", &out, &req.request_id))
    }

    fn get_metric_statistics(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let namespace = required_query_param(req, "Namespace")?;
        let metric_name = required_query_param(req, "MetricName")?;
        let start = required_query_param(req, "StartTime")?;
        let end = required_query_param(req, "EndTime")?;
        let period = required_query_param(req, "Period")?
            .parse::<i64>()
            .map_err(|_| invalid_param("Period must be an integer"))?;
        if period <= 0 {
            return Err(invalid_param("Period must be positive"));
        }
        let start_ts = DateTime::parse_from_rfc3339(&start)
            .map_err(|_| invalid_param("StartTime must be ISO 8601"))?
            .with_timezone(&Utc);
        let end_ts = DateTime::parse_from_rfc3339(&end)
            .map_err(|_| invalid_param("EndTime must be ISO 8601"))?
            .with_timezone(&Utc);

        let mut statistics: Vec<String> = Vec::new();
        for (k, v) in req.query_params.iter() {
            if k.starts_with("Statistics.member.") {
                statistics.push(v.clone());
            }
        }
        if statistics.is_empty() {
            return Err(invalid_param("At least one Statistic is required"));
        }

        let dim_filter = parse_dimensions_query(req, "Dimensions");

        let state = self.state.read();
        let mut datapoints: Vec<(DateTime<Utc>, BTreeMap<String, f64>)> = Vec::new();
        if let Some(acct) = state.get(&req.account_id) {
            if let Some(map) = acct.metrics_in(&req.region) {
                if let Some(data) = map.get(&namespace) {
                    let mut buckets: BTreeMap<DateTime<Utc>, DatumStats> = BTreeMap::new();
                    for d in data.iter() {
                        if d.metric_name != metric_name {
                            continue;
                        }
                        if !dim_filter
                            .iter()
                            .all(|(k, v)| d.dimensions.get(k) == Some(v))
                        {
                            continue;
                        }
                        if d.timestamp < start_ts || d.timestamp >= end_ts {
                            continue;
                        }
                        let Some(stats) = datum_stats(d) else {
                            continue;
                        };
                        let secs = d.timestamp.timestamp();
                        let bucket_secs = secs - secs.rem_euclid(period);
                        let bucket_ts =
                            DateTime::<Utc>::from_timestamp(bucket_secs, 0).unwrap_or(d.timestamp);
                        buckets
                            .entry(bucket_ts)
                            .and_modify(|acc| merge_stats(acc, stats))
                            .or_insert(stats);
                    }
                    for (ts, agg) in buckets {
                        let mut stats = BTreeMap::new();
                        for stat in statistics.iter() {
                            if let Some(v) = stat_value(stat, agg) {
                                stats.insert(stat.clone(), v);
                            }
                        }
                        datapoints.push((ts, stats));
                    }
                }
            }
        }

        let mut inner = format!("<Label>{}</Label>", xml_escape(&metric_name));
        inner.push_str("<Datapoints>");
        for (ts, stats) in datapoints {
            inner.push_str("<member>");
            inner.push_str(&format!(
                "<Timestamp>{}</Timestamp>",
                ts.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
            ));
            for (name, value) in stats {
                inner.push_str(&format!("<{name}>{value}</{name}>"));
            }
            inner.push_str("</member>");
        }
        inner.push_str("</Datapoints>");

        Ok(xml_response("GetMetricStatistics", &inner, &req.request_id))
    }

    fn get_metric_data(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let start = required_query_param(req, "StartTime")?;
        let end = required_query_param(req, "EndTime")?;
        let start_ts = DateTime::parse_from_rfc3339(&start)
            .map_err(|_| invalid_param("StartTime must be ISO 8601"))?
            .with_timezone(&Utc);
        let end_ts = DateTime::parse_from_rfc3339(&end)
            .map_err(|_| invalid_param("EndTime must be ISO 8601"))?
            .with_timezone(&Utc);

        let queries = collect_indexed(req, "MetricDataQueries");
        if queries.is_empty() {
            return Err(invalid_param(
                "MetricDataQueries must contain at least one entry",
            ));
        }

        let state = self.state.read();
        let mut inner = String::from("<MetricDataResults>");
        for q in queries {
            let id = q.get("Id").cloned().unwrap_or_default();
            let label = q.get("Label").cloned().unwrap_or_else(|| id.clone());
            let stat = q
                .get("MetricStat.Stat")
                .cloned()
                .unwrap_or_else(|| "Sum".to_string());
            let metric_name = q.get("MetricStat.Metric.MetricName").cloned();
            let namespace = q.get("MetricStat.Metric.Namespace").cloned();
            let period: i64 = q
                .get("MetricStat.Period")
                .and_then(|s| s.parse::<i64>().ok())
                .unwrap_or(60);
            if period <= 0 {
                return Err(invalid_param(
                    "MetricStat.Period must be a positive integer",
                ));
            }
            let dim_filter = parse_dimensions(&q, "MetricStat.Metric.Dimensions");

            let (mut timestamps, mut values): (Vec<String>, Vec<f64>) = (Vec::new(), Vec::new());
            if let (Some(metric_name), Some(namespace)) = (metric_name, namespace) {
                if let Some(acct) = state.get(&req.account_id) {
                    if let Some(map) = acct.metrics_in(&req.region) {
                        if let Some(data) = map.get(&namespace) {
                            let mut buckets: BTreeMap<DateTime<Utc>, DatumStats> = BTreeMap::new();
                            for d in data.iter() {
                                if d.metric_name != metric_name {
                                    continue;
                                }
                                if !dim_filter
                                    .iter()
                                    .all(|(k, v)| d.dimensions.get(k) == Some(v))
                                {
                                    continue;
                                }
                                if d.timestamp < start_ts || d.timestamp >= end_ts {
                                    continue;
                                }
                                let Some(stats) = datum_stats(d) else {
                                    continue;
                                };
                                let secs = d.timestamp.timestamp();
                                let bucket_secs = secs - secs.rem_euclid(period);
                                let bucket_ts = DateTime::<Utc>::from_timestamp(bucket_secs, 0)
                                    .unwrap_or(d.timestamp);
                                buckets
                                    .entry(bucket_ts)
                                    .and_modify(|acc| merge_stats(acc, stats))
                                    .or_insert(stats);
                            }
                            for (ts, agg) in buckets {
                                let Some(v) = stat_value(&stat, agg) else {
                                    continue;
                                };
                                timestamps
                                    .push(ts.to_rfc3339_opts(chrono::SecondsFormat::Millis, true));
                                values.push(v);
                            }
                        }
                    }
                }
            }

            inner.push_str("<member>");
            inner.push_str(&format!("<Id>{}</Id>", xml_escape(&id)));
            inner.push_str(&format!("<Label>{}</Label>", xml_escape(&label)));
            inner.push_str("<StatusCode>Complete</StatusCode>");
            inner.push_str("<Timestamps>");
            for ts in timestamps {
                inner.push_str(&format!("<member>{ts}</member>"));
            }
            inner.push_str("</Timestamps>");
            inner.push_str("<Values>");
            for v in values {
                inner.push_str(&format!("<member>{v}</member>"));
            }
            inner.push_str("</Values>");
            inner.push_str("</member>");
        }
        inner.push_str("</MetricDataResults>");
        inner.push_str("<Messages></Messages>");

        Ok(xml_response("GetMetricData", &inner, &req.request_id))
    }

    fn put_metric_alarm(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let alarm_name = required_query_param(req, "AlarmName")?;
        let comparison = required_query_param(req, "ComparisonOperator")?;
        let evaluation_periods = required_query_param(req, "EvaluationPeriods")?
            .parse::<i64>()
            .map_err(|_| invalid_param("EvaluationPeriods must be an integer"))?;

        let alarm_description = optional_query_param(req, "AlarmDescription");
        let actions_enabled = optional_query_param(req, "ActionsEnabled")
            .map(|s| s.eq_ignore_ascii_case("true"))
            .unwrap_or(true);

        let metric_name = optional_query_param(req, "MetricName");
        let namespace = optional_query_param(req, "Namespace");
        let statistic = optional_query_param(req, "Statistic");
        let extended_statistic = optional_query_param(req, "ExtendedStatistic");
        let period = optional_query_param(req, "Period").and_then(|s| s.parse::<i64>().ok());
        let unit = optional_query_param(req, "Unit");
        let datapoints_to_alarm =
            optional_query_param(req, "DatapointsToAlarm").and_then(|s| s.parse::<i64>().ok());
        let threshold = optional_query_param(req, "Threshold").and_then(|s| s.parse::<f64>().ok());
        let treat_missing_data = optional_query_param(req, "TreatMissingData");
        let evaluate_low_sample_count_percentile =
            optional_query_param(req, "EvaluateLowSampleCountPercentile");
        let dimensions = parse_dimensions_query(req, "Dimensions");

        let mut ok_actions = Vec::new();
        let mut alarm_actions = Vec::new();
        let mut insufficient_data_actions = Vec::new();
        for (k, v) in req.query_params.iter() {
            if k.starts_with("OKActions.member.") {
                ok_actions.push(v.clone());
            } else if k.starts_with("AlarmActions.member.") {
                alarm_actions.push(v.clone());
            } else if k.starts_with("InsufficientDataActions.member.") {
                insufficient_data_actions.push(v.clone());
            }
        }

        let arn = format!(
            "arn:aws:cloudwatch:{}:{}:alarm:{}",
            req.region, req.account_id, alarm_name
        );
        let now = Utc::now();

        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        let alarms = acct.alarms_in_mut(&req.region);
        let existing = alarms.get(&alarm_name).cloned();
        let alarm = MetricAlarm {
            alarm_name: alarm_name.clone(),
            alarm_arn: arn,
            alarm_description,
            actions_enabled,
            ok_actions,
            alarm_actions,
            insufficient_data_actions,
            state_value: existing
                .as_ref()
                .map(|a| a.state_value)
                .unwrap_or(AlarmState::InsufficientData),
            state_reason: existing
                .as_ref()
                .map(|a| a.state_reason.clone())
                .unwrap_or_else(|| "Unchecked: Initial alarm creation".to_string()),
            state_updated_timestamp: existing
                .as_ref()
                .map(|a| a.state_updated_timestamp)
                .unwrap_or(now),
            metric_name,
            namespace,
            statistic,
            extended_statistic,
            dimensions,
            period,
            unit,
            evaluation_periods,
            datapoints_to_alarm,
            threshold,
            comparison_operator: comparison,
            treat_missing_data,
            evaluate_low_sample_count_percentile,
            configuration_updated_timestamp: existing
                .as_ref()
                .map(|a| a.configuration_updated_timestamp)
                .unwrap_or(now),
            alarm_configuration_updated_timestamp: now,
        };
        alarms.insert(alarm_name, alarm);

        Ok(empty_metadata_response("PutMetricAlarm", &req.request_id))
    }

    fn describe_alarms(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mut filter_names: Vec<String> = Vec::new();
        for (k, v) in req.query_params.iter() {
            if k.starts_with("AlarmNames.member.") {
                filter_names.push(v.clone());
            }
        }
        let prefix = optional_query_param(req, "AlarmNamePrefix");
        let state_filter = optional_query_param(req, "StateValue");
        let action_prefix = optional_query_param(req, "ActionPrefix");

        let state = self.state.read();
        let mut inner = String::from("<MetricAlarms>");
        if let Some(acct) = state.get(&req.account_id) {
            if let Some(alarms) = acct.alarms_in(&req.region) {
                for alarm in alarms.values() {
                    if !filter_names.is_empty() && !filter_names.contains(&alarm.alarm_name) {
                        continue;
                    }
                    if let Some(p) = prefix.as_ref() {
                        if !alarm.alarm_name.starts_with(p) {
                            continue;
                        }
                    }
                    if let Some(sv) = state_filter.as_ref() {
                        if alarm.state_value.as_str() != sv {
                            continue;
                        }
                    }
                    if let Some(ap) = action_prefix.as_ref() {
                        let any = alarm
                            .alarm_actions
                            .iter()
                            .chain(alarm.ok_actions.iter())
                            .chain(alarm.insufficient_data_actions.iter())
                            .any(|a| a.starts_with(ap));
                        if !any {
                            continue;
                        }
                    }
                    inner.push_str(&render_alarm(alarm));
                }
            }
        }
        inner.push_str("</MetricAlarms>");
        inner.push_str("<CompositeAlarms></CompositeAlarms>");

        Ok(xml_response("DescribeAlarms", &inner, &req.request_id))
    }

    fn describe_alarms_for_metric(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let metric_name = required_query_param(req, "MetricName")?;
        let namespace = required_query_param(req, "Namespace")?;
        let dim_filter = parse_dimensions_query(req, "Dimensions");

        let state = self.state.read();
        let mut inner = String::from("<MetricAlarms>");
        if let Some(acct) = state.get(&req.account_id) {
            if let Some(alarms) = acct.alarms_in(&req.region) {
                for alarm in alarms.values() {
                    if alarm.metric_name.as_deref() != Some(&metric_name) {
                        continue;
                    }
                    if alarm.namespace.as_deref() != Some(&namespace) {
                        continue;
                    }
                    if !dim_filter.is_empty() && alarm.dimensions != dim_filter {
                        continue;
                    }
                    inner.push_str(&render_alarm(alarm));
                }
            }
        }
        inner.push_str("</MetricAlarms>");

        Ok(xml_response(
            "DescribeAlarmsForMetric",
            &inner,
            &req.request_id,
        ))
    }

    fn delete_alarms(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mut names: Vec<String> = Vec::new();
        for (k, v) in req.query_params.iter() {
            if k.starts_with("AlarmNames.member.") {
                names.push(v.clone());
            }
        }
        if names.is_empty() {
            return Err(invalid_param("AlarmNames must contain at least one name"));
        }

        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        let alarms = acct.alarms_in_mut(&req.region);
        for name in names {
            alarms.remove(&name);
        }

        Ok(empty_metadata_response("DeleteAlarms", &req.request_id))
    }

    fn enable_alarm_actions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.toggle_alarm_actions(req, true, "EnableAlarmActions")
    }

    fn disable_alarm_actions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.toggle_alarm_actions(req, false, "DisableAlarmActions")
    }

    fn toggle_alarm_actions(
        &self,
        req: &AwsRequest,
        enabled: bool,
        action_name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut names: Vec<String> = Vec::new();
        for (k, v) in req.query_params.iter() {
            if k.starts_with("AlarmNames.member.") {
                names.push(v.clone());
            }
        }
        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        let alarms = acct.alarms_in_mut(&req.region);
        for name in names {
            if let Some(alarm) = alarms.get_mut(&name) {
                alarm.actions_enabled = enabled;
                alarm.alarm_configuration_updated_timestamp = Utc::now();
            }
        }
        Ok(empty_metadata_response(action_name, &req.request_id))
    }

    fn set_alarm_state(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let alarm_name = required_query_param(req, "AlarmName")?;
        let state_value = required_query_param(req, "StateValue")?;
        let state_reason = required_query_param(req, "StateReason")?;
        let new_state = AlarmState::parse(&state_value)
            .ok_or_else(|| invalid_param("StateValue must be OK | ALARM | INSUFFICIENT_DATA"))?;

        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        let alarms = acct.alarms_in_mut(&req.region);
        let alarm = alarms.get_mut(&alarm_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFound",
                format!("Alarm {alarm_name} not found"),
            )
        })?;
        alarm.state_value = new_state;
        alarm.state_reason = state_reason;
        alarm.state_updated_timestamp = Utc::now();

        Ok(empty_metadata_response("SetAlarmState", &req.request_id))
    }

    fn describe_alarm_history(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Minimal implementation: return empty history. AWS pagination tokens are
        // not tracked locally, so callers see an empty list rather than a stub.
        let inner = String::from("<AlarmHistoryItems></AlarmHistoryItems>");
        Ok(xml_response(
            "DescribeAlarmHistory",
            &inner,
            &req.request_id,
        ))
    }

    fn put_dashboard(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let dashboard_name = req
            .query_params
            .get("DashboardName")
            .ok_or_else(|| invalid_param("DashboardName is required"))?
            .clone();
        let body = req
            .query_params
            .get("DashboardBody")
            .ok_or_else(|| invalid_param("DashboardBody is required"))?
            .clone();
        // AWS validates that DashboardBody parses as JSON; we do the same so
        // bad bodies surface a useful error before persisting.
        if serde_json::from_str::<serde_json::Value>(&body).is_err() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterInput",
                "DashboardBody must be a valid JSON object",
            ));
        }
        let arn = format!(
            "arn:aws:cloudwatch::{}:dashboard/{dashboard_name}",
            req.account_id
        );
        let dashboard = Dashboard {
            name: dashboard_name.clone(),
            arn,
            size_bytes: body.len() as i64,
            body,
            last_modified: Utc::now(),
        };
        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        acct.dashboards.insert(dashboard_name, dashboard);
        // PutDashboard returns DashboardValidationMessages — empty when the
        // body parses cleanly.
        let inner = String::from("<DashboardValidationMessages/>");
        Ok(xml_response("PutDashboard", &inner, &req.request_id))
    }

    fn get_dashboard(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = req
            .query_params
            .get("DashboardName")
            .ok_or_else(|| invalid_param("DashboardName is required"))?
            .clone();
        let state = self.state.read();
        let dashboard = state
            .get(&req.account_id)
            .and_then(|a| a.dashboards.get(&name))
            .cloned()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFound",
                    format!("Dashboard {name} does not exist"),
                )
            })?;
        let inner = format!(
            "<DashboardArn>{}</DashboardArn><DashboardBody>{}</DashboardBody><DashboardName>{}</DashboardName>",
            xml_escape(&dashboard.arn),
            xml_escape(&dashboard.body),
            xml_escape(&dashboard.name),
        );
        Ok(xml_response("GetDashboard", &inner, &req.request_id))
    }

    fn delete_dashboards(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mut names: Vec<String> = Vec::new();
        for (k, v) in req.query_params.iter() {
            if k.starts_with("DashboardNames.member.") {
                names.push(v.clone());
            }
        }
        if names.is_empty() {
            return Err(invalid_param(
                "DashboardNames must contain at least one name",
            ));
        }
        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        for n in names {
            acct.dashboards.remove(&n);
        }
        Ok(empty_metadata_response("DeleteDashboards", &req.request_id))
    }

    fn list_dashboards(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let prefix = req.query_params.get("DashboardNamePrefix").cloned();
        let state = self.state.read();
        let dashboards: Vec<Dashboard> = state
            .get(&req.account_id)
            .map(|a| {
                a.dashboards
                    .values()
                    .filter(|d| prefix.as_ref().is_none_or(|p| d.name.starts_with(p)))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        let mut entries = String::new();
        for d in &dashboards {
            entries.push_str("<member>");
            entries.push_str(&format!(
                "<DashboardArn>{}</DashboardArn><DashboardName>{}</DashboardName><LastModified>{}</LastModified><Size>{}</Size>",
                xml_escape(&d.arn),
                xml_escape(&d.name),
                d.last_modified.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
                d.size_bytes,
            ));
            entries.push_str("</member>");
        }
        let inner = format!("<DashboardEntries>{entries}</DashboardEntries>");
        Ok(xml_response("ListDashboards", &inner, &req.request_id))
    }
}

fn render_alarm(alarm: &MetricAlarm) -> String {
    let mut s = String::from("<member>");
    s.push_str(&format!(
        "<AlarmName>{}</AlarmName>",
        xml_escape(&alarm.alarm_name)
    ));
    s.push_str(&format!(
        "<AlarmArn>{}</AlarmArn>",
        xml_escape(&alarm.alarm_arn)
    ));
    if let Some(d) = &alarm.alarm_description {
        s.push_str(&format!(
            "<AlarmDescription>{}</AlarmDescription>",
            xml_escape(d)
        ));
    }
    s.push_str(&format!(
        "<ActionsEnabled>{}</ActionsEnabled>",
        alarm.actions_enabled
    ));
    push_action_list(&mut s, "OKActions", &alarm.ok_actions);
    push_action_list(&mut s, "AlarmActions", &alarm.alarm_actions);
    push_action_list(
        &mut s,
        "InsufficientDataActions",
        &alarm.insufficient_data_actions,
    );
    s.push_str(&format!(
        "<StateValue>{}</StateValue>",
        alarm.state_value.as_str()
    ));
    s.push_str(&format!(
        "<StateReason>{}</StateReason>",
        xml_escape(&alarm.state_reason)
    ));
    s.push_str(&format!(
        "<StateUpdatedTimestamp>{}</StateUpdatedTimestamp>",
        alarm
            .state_updated_timestamp
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    ));
    if let Some(m) = &alarm.metric_name {
        s.push_str(&format!("<MetricName>{}</MetricName>", xml_escape(m)));
    }
    if let Some(n) = &alarm.namespace {
        s.push_str(&format!("<Namespace>{}</Namespace>", xml_escape(n)));
    }
    if let Some(stat) = &alarm.statistic {
        s.push_str(&format!("<Statistic>{}</Statistic>", xml_escape(stat)));
    }
    if let Some(ext) = &alarm.extended_statistic {
        s.push_str(&format!(
            "<ExtendedStatistic>{}</ExtendedStatistic>",
            xml_escape(ext)
        ));
    }
    s.push_str(&render_dimensions(&alarm.dimensions));
    if let Some(p) = alarm.period {
        s.push_str(&format!("<Period>{p}</Period>"));
    }
    if let Some(u) = &alarm.unit {
        s.push_str(&format!("<Unit>{}</Unit>", xml_escape(u)));
    }
    s.push_str(&format!(
        "<EvaluationPeriods>{}</EvaluationPeriods>",
        alarm.evaluation_periods
    ));
    if let Some(d) = alarm.datapoints_to_alarm {
        s.push_str(&format!("<DatapointsToAlarm>{d}</DatapointsToAlarm>"));
    }
    if let Some(t) = alarm.threshold {
        s.push_str(&format!("<Threshold>{t}</Threshold>"));
    }
    s.push_str(&format!(
        "<ComparisonOperator>{}</ComparisonOperator>",
        xml_escape(&alarm.comparison_operator)
    ));
    if let Some(t) = &alarm.treat_missing_data {
        s.push_str(&format!(
            "<TreatMissingData>{}</TreatMissingData>",
            xml_escape(t)
        ));
    }
    if let Some(e) = &alarm.evaluate_low_sample_count_percentile {
        s.push_str(&format!(
            "<EvaluateLowSampleCountPercentile>{}</EvaluateLowSampleCountPercentile>",
            xml_escape(e)
        ));
    }
    s.push_str(&format!(
        "<AlarmConfigurationUpdatedTimestamp>{}</AlarmConfigurationUpdatedTimestamp>",
        alarm
            .alarm_configuration_updated_timestamp
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    ));
    s.push_str("</member>");
    s
}

fn push_action_list(s: &mut String, name: &str, actions: &[String]) {
    s.push_str(&format!("<{name}>"));
    for action in actions {
        s.push_str(&format!("<member>{}</member>", xml_escape(action)));
    }
    s.push_str(&format!("</{name}>"));
}
