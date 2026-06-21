use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub type SharedCloudWatchState = Arc<RwLock<CloudWatchAccounts>>;

/// On-disk snapshot envelope for CloudWatch state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudWatchSnapshot {
    pub schema_version: u32,
    pub accounts: CloudWatchAccounts,
}

pub const CLOUDWATCH_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CloudWatchAccounts {
    pub accounts: BTreeMap<String, CloudWatchState>,
}

impl CloudWatchAccounts {
    pub fn new() -> Self {
        Self::default()
    }

    /// Deep-clone for snapshot serialization. `Clone` isn't derived
    /// because the live state is held behind a `RwLock` and the rest of
    /// the crate references it by reference — this helper makes the
    /// intent explicit at the persistence boundary.
    pub fn clone_for_snapshot(&self) -> CloudWatchAccounts {
        CloudWatchAccounts {
            accounts: self.accounts.clone(),
        }
    }

    pub fn get_or_create(&mut self, account_id: &str) -> &mut CloudWatchState {
        self.accounts
            .entry(account_id.to_string())
            .or_insert_with(|| CloudWatchState::new(account_id))
    }

    pub fn get(&self, account_id: &str) -> Option<&CloudWatchState> {
        self.accounts.get(account_id)
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CloudWatchState {
    pub account_id: String,
    /// region -> namespace -> Vec<MetricDatum>
    pub metrics: BTreeMap<String, BTreeMap<String, Vec<MetricDatum>>>,
    /// region -> alarm_name -> MetricAlarm
    pub alarms: BTreeMap<String, BTreeMap<String, MetricAlarm>>,
    /// region -> alarm_name -> CompositeAlarm
    #[serde(default)]
    pub composite_alarms: BTreeMap<String, BTreeMap<String, CompositeAlarm>>,
    /// Dashboards keyed by name (CloudWatch dashboards are global per
    /// account, not regional).
    #[serde(default)]
    pub dashboards: BTreeMap<String, Dashboard>,
    /// region -> (namespace, metric, stat, dims) key -> AnomalyDetector
    #[serde(default)]
    pub anomaly_detectors: BTreeMap<String, BTreeMap<String, AnomalyDetector>>,
    /// region -> rule_name -> InsightRule
    #[serde(default)]
    pub insight_rules: BTreeMap<String, BTreeMap<String, InsightRule>>,
    /// region -> resource_arn -> Vec<ManagedRule>
    #[serde(default)]
    pub managed_rules: BTreeMap<String, BTreeMap<String, Vec<ManagedRule>>>,
    /// region -> stream_name -> MetricStream
    #[serde(default)]
    pub metric_streams: BTreeMap<String, BTreeMap<String, MetricStream>>,
    /// region -> rule_name -> AlarmMuteRule
    #[serde(default)]
    pub mute_rules: BTreeMap<String, BTreeMap<String, AlarmMuteRule>>,
    /// region -> dataset_identifier -> Dataset (KMS key association)
    #[serde(default)]
    pub datasets: BTreeMap<String, BTreeMap<String, Dataset>>,
    /// resource_arn -> tag_key -> tag_value (tags are account-global by ARN)
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
    /// OTel enrichment is on when true (per account).
    #[serde(default)]
    pub otel_enrichment_running: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dashboard {
    pub name: String,
    pub arn: String,
    pub body: String,
    pub last_modified: DateTime<Utc>,
    pub size_bytes: i64,
}

/// A CloudWatch dataset and its optional KMS key association. Datasets are
/// referenced by an identifier; there is no Create API, so an entry is
/// materialized the first time a KMS key is associated with an identifier.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dataset {
    pub id: String,
    pub arn: String,
    pub kms_key_arn: Option<String>,
}

impl CloudWatchState {
    pub fn new(account_id: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            ..Default::default()
        }
    }

    pub fn metrics_in(&self, region: &str) -> Option<&BTreeMap<String, Vec<MetricDatum>>> {
        self.metrics.get(region)
    }

    pub fn metrics_in_mut(&mut self, region: &str) -> &mut BTreeMap<String, Vec<MetricDatum>> {
        self.metrics.entry(region.to_string()).or_default()
    }

    pub fn alarms_in(&self, region: &str) -> Option<&BTreeMap<String, MetricAlarm>> {
        self.alarms.get(region)
    }

    pub fn alarms_in_mut(&mut self, region: &str) -> &mut BTreeMap<String, MetricAlarm> {
        self.alarms.entry(region.to_string()).or_default()
    }

    pub fn composite_alarms_in(&self, region: &str) -> Option<&BTreeMap<String, CompositeAlarm>> {
        self.composite_alarms.get(region)
    }

    pub fn composite_alarms_in_mut(
        &mut self,
        region: &str,
    ) -> &mut BTreeMap<String, CompositeAlarm> {
        self.composite_alarms.entry(region.to_string()).or_default()
    }

    pub fn anomaly_detectors_in(&self, region: &str) -> Option<&BTreeMap<String, AnomalyDetector>> {
        self.anomaly_detectors.get(region)
    }

    pub fn anomaly_detectors_in_mut(
        &mut self,
        region: &str,
    ) -> &mut BTreeMap<String, AnomalyDetector> {
        self.anomaly_detectors
            .entry(region.to_string())
            .or_default()
    }

    pub fn insight_rules_in(&self, region: &str) -> Option<&BTreeMap<String, InsightRule>> {
        self.insight_rules.get(region)
    }

    pub fn insight_rules_in_mut(&mut self, region: &str) -> &mut BTreeMap<String, InsightRule> {
        self.insight_rules.entry(region.to_string()).or_default()
    }

    pub fn managed_rules_in(&self, region: &str) -> Option<&BTreeMap<String, Vec<ManagedRule>>> {
        self.managed_rules.get(region)
    }

    pub fn managed_rules_in_mut(
        &mut self,
        region: &str,
    ) -> &mut BTreeMap<String, Vec<ManagedRule>> {
        self.managed_rules.entry(region.to_string()).or_default()
    }

    pub fn metric_streams_in(&self, region: &str) -> Option<&BTreeMap<String, MetricStream>> {
        self.metric_streams.get(region)
    }

    pub fn metric_streams_in_mut(&mut self, region: &str) -> &mut BTreeMap<String, MetricStream> {
        self.metric_streams.entry(region.to_string()).or_default()
    }

    pub fn mute_rules_in(&self, region: &str) -> Option<&BTreeMap<String, AlarmMuteRule>> {
        self.mute_rules.get(region)
    }

    pub fn mute_rules_in_mut(&mut self, region: &str) -> &mut BTreeMap<String, AlarmMuteRule> {
        self.mute_rules.entry(region.to_string()).or_default()
    }

    pub fn datasets_in(&self, region: &str) -> Option<&BTreeMap<String, Dataset>> {
        self.datasets.get(region)
    }

    pub fn datasets_in_mut(&mut self, region: &str) -> &mut BTreeMap<String, Dataset> {
        self.datasets.entry(region.to_string()).or_default()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricDatum {
    pub metric_name: String,
    pub dimensions: BTreeMap<String, String>,
    pub timestamp: DateTime<Utc>,
    pub value: Option<f64>,
    pub statistic_values: Option<StatisticSet>,
    pub unit: Option<String>,
    pub storage_resolution: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StatisticSet {
    pub sample_count: f64,
    pub sum: f64,
    pub minimum: f64,
    pub maximum: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricAlarm {
    pub alarm_name: String,
    pub alarm_arn: String,
    pub alarm_description: Option<String>,
    pub actions_enabled: bool,
    pub ok_actions: Vec<String>,
    pub alarm_actions: Vec<String>,
    pub insufficient_data_actions: Vec<String>,
    pub state_value: AlarmState,
    pub state_reason: String,
    pub state_updated_timestamp: DateTime<Utc>,
    pub metric_name: Option<String>,
    pub namespace: Option<String>,
    pub statistic: Option<String>,
    pub extended_statistic: Option<String>,
    pub dimensions: BTreeMap<String, String>,
    pub period: Option<i64>,
    pub unit: Option<String>,
    pub evaluation_periods: i64,
    pub datapoints_to_alarm: Option<i64>,
    pub threshold: Option<f64>,
    pub comparison_operator: String,
    pub treat_missing_data: Option<String>,
    pub evaluate_low_sample_count_percentile: Option<String>,
    /// `ThresholdMetricId` — references the metric-math id that produces an
    /// anomaly-detection band (used with the `*UpperThreshold` /
    /// `*LowerThreshold` comparison operators instead of a static Threshold).
    #[serde(default)]
    pub threshold_metric_id: Option<String>,
    pub configuration_updated_timestamp: DateTime<Utc>,
    pub alarm_configuration_updated_timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum AlarmState {
    Ok,
    Alarm,
    InsufficientData,
}

impl AlarmState {
    pub fn as_str(&self) -> &'static str {
        match self {
            AlarmState::Ok => "OK",
            AlarmState::Alarm => "ALARM",
            AlarmState::InsufficientData => "INSUFFICIENT_DATA",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "OK" => Some(AlarmState::Ok),
            "ALARM" => Some(AlarmState::Alarm),
            "INSUFFICIENT_DATA" => Some(AlarmState::InsufficientData),
            _ => None,
        }
    }
}

/// A composite alarm (defined by an `AlarmRule` expression over other alarms).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompositeAlarm {
    pub alarm_name: String,
    pub alarm_arn: String,
    pub alarm_description: Option<String>,
    pub alarm_rule: String,
    pub actions_enabled: bool,
    pub ok_actions: Vec<String>,
    pub alarm_actions: Vec<String>,
    pub insufficient_data_actions: Vec<String>,
    pub actions_suppressor: Option<String>,
    pub actions_suppressor_wait_period: Option<i64>,
    pub actions_suppressor_extension_period: Option<i64>,
    pub state_value: AlarmState,
    pub state_reason: String,
    pub state_updated_timestamp: DateTime<Utc>,
    pub alarm_configuration_updated_timestamp: DateTime<Utc>,
}

/// An anomaly detection model on a metric (single-metric or metric-math).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnomalyDetector {
    /// Stable key derived from namespace/metric/stat/dims (single) or a hash
    /// of the metric-math expression so Put/Delete/Describe agree.
    pub key: String,
    pub namespace: Option<String>,
    pub metric_name: Option<String>,
    pub stat: Option<String>,
    pub dimensions: BTreeMap<String, String>,
    pub metric_math: bool,
    pub state_value: String,
}

/// A Contributor Insights rule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InsightRule {
    pub name: String,
    pub state: String,
    pub schema: String,
    pub definition: String,
    pub managed: bool,
    pub apply_on_transformed_logs: bool,
}

/// A managed Contributor Insights rule (template applied to a resource ARN).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManagedRule {
    pub template_name: String,
    pub resource_arn: String,
}

/// A metric stream (control-plane config; no data-plane delivery).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricStream {
    pub name: String,
    pub arn: String,
    pub firehose_arn: String,
    pub role_arn: String,
    pub output_format: String,
    /// "running" or "stopped".
    pub state: String,
    pub include_filters: Vec<MetricStreamFilter>,
    pub exclude_filters: Vec<MetricStreamFilter>,
    pub include_linked_accounts_metrics: bool,
    pub creation_date: DateTime<Utc>,
    pub last_update_date: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricStreamFilter {
    pub namespace: Option<String>,
    pub metric_names: Vec<String>,
}

/// An alarm mute rule. `Rule` is a nested `Schedule` structure on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlarmMuteRule {
    pub name: String,
    pub arn: String,
    pub description: Option<String>,
    pub schedule_expression: Option<String>,
    pub schedule_duration: Option<String>,
    pub schedule_timezone: Option<String>,
    pub mute_target_alarm_names: Vec<String>,
    pub start_date: Option<DateTime<Utc>>,
    pub expire_date: Option<DateTime<Utc>>,
    pub last_updated_timestamp: DateTime<Utc>,
}
