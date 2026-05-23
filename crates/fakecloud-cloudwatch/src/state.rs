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
    /// Dashboards keyed by name (CloudWatch dashboards are global per
    /// account, not regional).
    #[serde(default)]
    pub dashboards: BTreeMap<String, Dashboard>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dashboard {
    pub name: String,
    pub arn: String,
    pub body: String,
    pub last_modified: DateTime<Utc>,
    pub size_bytes: i64,
}

impl CloudWatchState {
    pub fn new(account_id: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            metrics: BTreeMap::new(),
            alarms: BTreeMap::new(),
            dashboards: BTreeMap::new(),
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
