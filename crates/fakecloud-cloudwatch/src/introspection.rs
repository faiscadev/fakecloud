//! CloudWatch admin introspection helpers consumed by
//! `/_fakecloud/cloudwatch/*` routes.
//!
//! These read the in-memory state cross-account/region and produce
//! assertion-friendly rows. They intentionally bypass IAM — admin
//! endpoints never authenticate.

use chrono::{DateTime, Utc};

use crate::state::{MetricDatum, SharedCloudWatchState};

/// A single name/value metric dimension pair.
#[derive(Debug, Clone)]
pub struct DimensionRow {
    pub name: String,
    pub value: String,
}

/// One alarm (metric or composite) flattened for introspection.
#[derive(Debug, Clone)]
pub struct AlarmRow {
    pub account_id: String,
    pub region: String,
    pub name: String,
    /// "metric" or "composite".
    pub kind: &'static str,
    /// OK / ALARM / INSUFFICIENT_DATA.
    pub state: String,
    pub state_reason: String,
    pub state_updated_timestamp: Option<DateTime<Utc>>,
    pub actions_enabled: bool,
    pub alarm_actions: Vec<String>,
    pub ok_actions: Vec<String>,
    pub insufficient_data_actions: Vec<String>,
    // metric-only
    pub namespace: Option<String>,
    pub metric_name: Option<String>,
    pub threshold: Option<f64>,
    pub comparison_operator: Option<String>,
    // composite-only
    pub alarm_rule: Option<String>,
}

/// Latest datapoint summary for a metric series.
#[derive(Debug, Clone)]
pub struct LatestDatapoint {
    pub timestamp: DateTime<Utc>,
    pub value: Option<f64>,
    pub unit: Option<String>,
}

/// One unique metric series (account, region, namespace, metric, dims).
#[derive(Debug, Clone)]
pub struct MetricRow {
    pub account_id: String,
    pub region: String,
    pub namespace: String,
    pub metric_name: String,
    pub dimensions: Vec<DimensionRow>,
    pub datapoint_count: usize,
    pub latest: Option<LatestDatapoint>,
}

fn dims_to_rows(dims: &std::collections::BTreeMap<String, String>) -> Vec<DimensionRow> {
    dims.iter()
        .map(|(name, value)| DimensionRow {
            name: name.clone(),
            value: value.clone(),
        })
        .collect()
}

/// Flatten every metric alarm and composite alarm across all accounts and
/// regions into one sorted list.
pub fn list_all_alarms(state: &SharedCloudWatchState) -> Vec<AlarmRow> {
    let accounts = state.read();
    let mut rows: Vec<AlarmRow> = Vec::new();
    for (account_id, acct) in &accounts.accounts {
        for (region, alarms) in &acct.alarms {
            for (name, a) in alarms {
                rows.push(AlarmRow {
                    account_id: account_id.clone(),
                    region: region.clone(),
                    name: name.clone(),
                    kind: "metric",
                    state: a.state_value.as_str().to_string(),
                    state_reason: a.state_reason.clone(),
                    state_updated_timestamp: Some(a.state_updated_timestamp),
                    actions_enabled: a.actions_enabled,
                    alarm_actions: a.alarm_actions.clone(),
                    ok_actions: a.ok_actions.clone(),
                    insufficient_data_actions: a.insufficient_data_actions.clone(),
                    namespace: a.namespace.clone(),
                    metric_name: a.metric_name.clone(),
                    threshold: a.threshold,
                    comparison_operator: Some(a.comparison_operator.clone()),
                    alarm_rule: None,
                });
            }
        }
        for (region, alarms) in &acct.composite_alarms {
            for (name, a) in alarms {
                rows.push(AlarmRow {
                    account_id: account_id.clone(),
                    region: region.clone(),
                    name: name.clone(),
                    kind: "composite",
                    state: a.state_value.as_str().to_string(),
                    state_reason: a.state_reason.clone(),
                    state_updated_timestamp: Some(a.state_updated_timestamp),
                    actions_enabled: a.actions_enabled,
                    alarm_actions: a.alarm_actions.clone(),
                    ok_actions: a.ok_actions.clone(),
                    insufficient_data_actions: a.insufficient_data_actions.clone(),
                    namespace: None,
                    metric_name: None,
                    threshold: None,
                    comparison_operator: None,
                    alarm_rule: Some(a.alarm_rule.clone()),
                });
            }
        }
    }
    rows.sort_by(|a, b| {
        a.account_id
            .cmp(&b.account_id)
            .then_with(|| a.region.cmp(&b.region))
            .then_with(|| a.name.cmp(&b.name))
    });
    rows
}

/// Collapse stored metric data points into one row per unique
/// (account, region, namespace, metricName, dimensions) series, with a
/// datapoint count and the most-recent datapoint.
pub fn list_all_metrics(state: &SharedCloudWatchState) -> Vec<MetricRow> {
    let accounts = state.read();
    let mut rows: Vec<MetricRow> = Vec::new();
    for (account_id, acct) in &accounts.accounts {
        // region -> namespace -> Vec<MetricDatum>
        for (region, by_namespace) in &acct.metrics {
            for (namespace, data) in by_namespace {
                // Group this namespace's data by (metric_name, dimensions).
                type SeriesKey = (String, Vec<(String, String)>);
                let mut groups: std::collections::BTreeMap<SeriesKey, Vec<&MetricDatum>> =
                    std::collections::BTreeMap::new();
                for d in data {
                    let dim_key: Vec<(String, String)> = d
                        .dimensions
                        .iter()
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    groups
                        .entry((d.metric_name.clone(), dim_key))
                        .or_default()
                        .push(d);
                }
                for ((metric_name, _), series) in groups {
                    let dimensions = series
                        .first()
                        .map(|d| dims_to_rows(&d.dimensions))
                        .unwrap_or_default();
                    let latest =
                        series
                            .iter()
                            .max_by_key(|d| d.timestamp)
                            .map(|d| LatestDatapoint {
                                timestamp: d.timestamp,
                                value: d.value,
                                unit: d.unit.clone(),
                            });
                    rows.push(MetricRow {
                        account_id: account_id.clone(),
                        region: region.clone(),
                        namespace: namespace.clone(),
                        metric_name,
                        dimensions,
                        datapoint_count: series.len(),
                        latest,
                    });
                }
            }
        }
    }
    rows.sort_by(|a, b| {
        a.account_id
            .cmp(&b.account_id)
            .then_with(|| a.region.cmp(&b.region))
            .then_with(|| a.namespace.cmp(&b.namespace))
            .then_with(|| a.metric_name.cmp(&b.metric_name))
            .then_with(|| a.dimensions.len().cmp(&b.dimensions.len()))
    });
    rows
}
