use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub use crate::flat_members::FlatMembers;

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

    /// Bring state loaded from an older snapshot up to date: alarm mute rule
    /// ARNs written as `alarm-mute-rule/<name>` become
    /// `alarm-mute-rule:<name>`, their tags moving with them.
    pub fn migrate_loaded(&mut self) {
        for acct in self.accounts.values_mut() {
            let mut moved = Vec::new();
            for rules in acct.mute_rules.values_mut() {
                for rule in rules.values_mut() {
                    if let Some((prefix, name)) = rule.arn.split_once(":alarm-mute-rule/") {
                        let new = format!("{prefix}:alarm-mute-rule:{name}");
                        moved.push((std::mem::replace(&mut rule.arn, new.clone()), new));
                    }
                }
            }
            for (old, new) in moved {
                if let Some(tags) = acct.tags.remove(&old) {
                    acct.tags.entry(new).or_default().extend(tags);
                }
            }
        }
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
    /// region -> alarm_name -> LogAlarm
    #[serde(default)]
    pub log_alarms: BTreeMap<String, BTreeMap<String, LogAlarm>>,
    /// region -> alarm_name -> history items (newest appended last). Populated
    /// by PutMetricAlarm (ConfigurationUpdate), SetAlarmState (StateUpdate) and
    /// DeleteAlarms so DescribeAlarmHistory reflects real transitions.
    #[serde(default)]
    pub alarm_history: BTreeMap<String, BTreeMap<String, Vec<AlarmHistoryItem>>>,
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
    /// region -> alarm_name -> the alarm's current mute window (present
    /// while a mute rule mutes the alarm).
    #[serde(default)]
    pub alarm_mutes: BTreeMap<String, BTreeMap<String, MuteWindow>>,
    /// region -> dataset_identifier -> Dataset (KMS key association)
    #[serde(default)]
    pub datasets: BTreeMap<String, BTreeMap<String, Dataset>>,
    /// resource_arn -> tag_key -> tag_value (tags are account-global by ARN)
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
    /// OTel enrichment is on when true (per account).
    #[serde(default)]
    pub otel_enrichment_running: bool,
    /// Filters and timestamps stored while OTel enrichment is running.
    #[serde(default)]
    pub otel_enrichment: OTelEnrichmentConfig,
    /// region -> resource_arn -> ResourceMetricsConfiguration
    #[serde(default)]
    pub resource_metrics_configurations:
        BTreeMap<String, BTreeMap<String, ResourceMetricsConfiguration>>,
    /// Time-ordered index over `metrics` for alarm evaluation (a cache).
    #[serde(skip)]
    pub(crate) series_index: crate::series_index::SeriesIndex,
    /// Identifies this incarnation of the account's state: a reset or a
    /// snapshot load makes a new one, so actions queued against the old
    /// state are dropped instead of dispatched.
    #[serde(skip, default = "next_epoch")]
    pub(crate) epoch: u64,
    /// Actions owed by state changes, in transition order, waiting for the
    /// service's dispatcher. Every writer (API handlers, the cross-service
    /// metrics delivery, the evaluation timer) queues here under the state
    /// lock, and a reset drops them with the state.
    #[serde(skip)]
    pub(crate) pending_actions: Vec<crate::alarm_actions::AlarmNotification>,
    /// Set when an evaluation changed alarm state that needs persisting.
    #[serde(skip)]
    pub(crate) evaluation_dirty: bool,
}

/// A process-unique [`CloudWatchState::epoch`].
pub(crate) fn next_epoch() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// One `OTelEnrichmentMetricSelector`: a namespace plus optional metric names
/// (an empty list selects every metric in the namespace).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct OTelMetricSelector {
    pub namespace: String,
    pub metric_names: Vec<String>,
}

/// The account's stored OTel enrichment configuration. Empty filter lists mean
/// "every supported namespace" (include) and "nothing excluded" (exclude).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct OTelEnrichmentConfig {
    #[serde(default)]
    pub include_filters: Vec<OTelMetricSelector>,
    #[serde(default)]
    pub exclude_filters: Vec<OTelMetricSelector>,
    #[serde(default)]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub updated_at: Option<DateTime<Utc>>,
}

/// Detailed-metrics collection config for one AWS resource. `include_metrics`
/// is `None` when every available detailed metric is collected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceMetricsConfiguration {
    pub resource_arn: String,
    pub include_metrics: Option<Vec<String>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
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
            epoch: next_epoch(),
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

    pub fn log_alarms_in(&self, region: &str) -> Option<&BTreeMap<String, LogAlarm>> {
        self.log_alarms.get(region)
    }

    pub fn log_alarms_in_mut(&mut self, region: &str) -> &mut BTreeMap<String, LogAlarm> {
        self.log_alarms.entry(region.to_string()).or_default()
    }

    pub fn alarm_history_in(
        &self,
        region: &str,
    ) -> Option<&BTreeMap<String, Vec<AlarmHistoryItem>>> {
        self.alarm_history.get(region)
    }

    pub fn alarm_history_in_mut(
        &mut self,
        region: &str,
    ) -> &mut BTreeMap<String, Vec<AlarmHistoryItem>> {
        self.alarm_history.entry(region.to_string()).or_default()
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

    pub fn resource_metrics_in(
        &self,
        region: &str,
    ) -> Option<&BTreeMap<String, ResourceMetricsConfiguration>> {
        self.resource_metrics_configurations.get(region)
    }

    pub fn resource_metrics_in_mut(
        &mut self,
        region: &str,
    ) -> &mut BTreeMap<String, ResourceMetricsConfiguration> {
        self.resource_metrics_configurations
            .entry(region.to_string())
            .or_default()
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
    /// `Metrics` — the metric-math / cross-account alarm definition (a list of
    /// `MetricDataQuery`). Set instead of the single-metric fields when the
    /// alarm evaluates an expression or a metric in another account.
    #[serde(default)]
    pub metrics: Vec<AlarmMetricQuery>,
    /// Whether the current state was forced by `SetAlarmState`. AWS reverts a
    /// manual state on the next evaluation, but the emulator keeps it sticky
    /// (so it can be used to drive composite alarms) until real datapoints
    /// arrive — at which point evaluation clears this and data governs the
    /// state. It only suppresses the default missing-data INSUFFICIENT_DATA
    /// transition, never a real threshold crossing.
    #[serde(default)]
    pub state_manually_set: bool,
    /// `ActionConfiguration`, kept in flat-key form.
    #[serde(default)]
    pub action_configuration: Option<FlatMembers>,
    /// `WarmUpConfiguration`, kept in flat-key form.
    #[serde(default)]
    pub warm_up_configuration: Option<FlatMembers>,
    /// `EvaluationCriteria` (PromQL alarms), kept in flat-key form.
    #[serde(default)]
    pub evaluation_criteria: Option<FlatMembers>,
    /// `EvaluationWindow` (sliding or wall-clock), kept in flat-key form.
    #[serde(default)]
    pub evaluation_window: Option<FlatMembers>,
    /// `EvaluationInterval` in seconds (alarms using `EvaluationCriteria`).
    #[serde(default)]
    pub evaluation_interval: Option<i64>,
    /// `StateReasonData`: the JSON reason the evaluator computed for the last
    /// transition, or the value given to `SetAlarmState`.
    #[serde(default)]
    pub state_reason_data: Option<String>,
    /// `StateTransitionedTimestamp`: when `StateValue` last changed. `None` on
    /// snapshots written before it was tracked (rendered as
    /// `state_updated_timestamp` then).
    #[serde(default)]
    pub state_transitioned_timestamp: Option<DateTime<Utc>>,
    /// `EvaluationState`: set when the last evaluation could not compute the
    /// alarm normally, absent otherwise.
    #[serde(default)]
    pub evaluation_state: Option<EvaluationState>,
    /// When the alarm last left ALARM (what an `ActionsSuppressor`'s
    /// extension period counts from).
    #[serde(default)]
    pub alarm_exit: AlarmExit,
    /// A state forced by `SetAlarmState` holds until this time (one period
    /// later), when the alarm is evaluated normally again.
    #[serde(default)]
    pub manual_hold_until: Option<DateTime<Utc>>,
    /// The next period boundary, when time passing alone can change the
    /// evaluation outcome. The evaluation timer skips the alarm until then.
    /// Not persisted: after a restart every alarm is evaluated once.
    #[serde(skip)]
    pub next_evaluation_at: Option<DateTime<Utc>>,
}

/// When an alarm last left the ALARM state. `tracked` is false on alarms
/// persisted before this was recorded; their exit time is then read from the
/// alarm history.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq)]
pub struct AlarmExit {
    #[serde(default)]
    pub tracked: bool,
    #[serde(default)]
    pub at: Option<DateTime<Utc>>,
}

impl AlarmExit {
    /// A new alarm's (it has never been in ALARM).
    pub fn new() -> Self {
        AlarmExit {
            tracked: true,
            at: None,
        }
    }

    /// Record a state change from `old` to `new` at `at`.
    pub fn note(&mut self, old: AlarmState, new: AlarmState, at: DateTime<Utc>) {
        if old == AlarmState::Alarm && new != AlarmState::Alarm {
            self.at = Some(at);
        }
    }
}

/// The `EvaluationState` enum of a metric or log alarm.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum EvaluationState {
    PartialData,
    EvaluationFailure,
    EvaluationError,
}

impl EvaluationState {
    pub fn as_str(&self) -> &'static str {
        match self {
            EvaluationState::PartialData => "PARTIAL_DATA",
            EvaluationState::EvaluationFailure => "EVALUATION_FAILURE",
            EvaluationState::EvaluationError => "EVALUATION_ERROR",
        }
    }
}

/// The `ActionsSuppressedBy` enum of a composite alarm.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ActionsSuppressedBy {
    WaitPeriod,
    ExtensionPeriod,
    Alarm,
}

impl ActionsSuppressedBy {
    pub fn as_str(&self) -> &'static str {
        match self {
            ActionsSuppressedBy::WaitPeriod => "WaitPeriod",
            ActionsSuppressedBy::ExtensionPeriod => "ExtensionPeriod",
            ActionsSuppressedBy::Alarm => "Alarm",
        }
    }
}

/// A single `MetricDataQuery` entry in a `PutMetricAlarm` `Metrics` list.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AlarmMetricQuery {
    pub id: String,
    #[serde(default)]
    pub metric_stat: Option<AlarmMetricStat>,
    #[serde(default)]
    pub expression: Option<String>,
    #[serde(default)]
    pub label: Option<String>,
    #[serde(default)]
    pub return_data: Option<bool>,
    #[serde(default)]
    pub account_id: Option<String>,
    #[serde(default)]
    pub period: Option<i64>,
}

/// The `MetricStat` of an [`AlarmMetricQuery`] (a metric plus how to aggregate
/// it).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AlarmMetricStat {
    #[serde(default)]
    pub namespace: Option<String>,
    #[serde(default)]
    pub metric_name: Option<String>,
    #[serde(default)]
    pub dimensions: BTreeMap<String, String>,
    #[serde(default)]
    pub period: Option<i64>,
    #[serde(default)]
    pub stat: Option<String>,
    #[serde(default)]
    pub unit: Option<String>,
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

/// A single alarm-history record returned by `DescribeAlarmHistory`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlarmHistoryItem {
    pub alarm_name: String,
    /// `MetricAlarm` or `CompositeAlarm`.
    pub alarm_type: String,
    pub timestamp: DateTime<Utc>,
    /// One of the `HistoryItemType` enum values (ConfigurationUpdate /
    /// StateUpdate / Action).
    pub history_item_type: String,
    pub history_summary: String,
    /// JSON blob (`HistoryData`) describing the transition.
    pub history_data: String,
}

/// The ARN of alarm `name` (metric, composite or log alarm) in `region`'s
/// partition.
pub fn alarm_arn(region: &str, account_id: &str, name: &str) -> String {
    fakecloud_aws::arn::Arn::regional("cloudwatch", region, account_id, &format!("alarm:{name}"))
        .to_string()
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
    /// `ActionConfiguration`, kept in flat-key form.
    #[serde(default)]
    pub action_configuration: Option<FlatMembers>,
    /// `StateReasonData`: the triggering alarms of the last rule transition,
    /// or the value given to `SetAlarmState`.
    #[serde(default)]
    pub state_reason_data: Option<String>,
    /// `StateTransitionedTimestamp`: when `StateValue` last changed.
    #[serde(default)]
    pub state_transitioned_timestamp: Option<DateTime<Utc>>,
    /// Why the alarm's actions are currently suppressed by its
    /// `ActionsSuppressor`, recomputed on every evaluation.
    #[serde(default)]
    pub actions_suppressed_by: Option<ActionsSuppressedBy>,
    #[serde(default)]
    pub actions_suppressed_reason: Option<String>,
    /// Whether the current state came from `SetAlarmState`. AWS keeps it until
    /// a child alarm changes state or the configuration is updated.
    #[serde(default)]
    pub state_manually_set: bool,
    /// The state before the last transition while that transition's actions
    /// are still owed (held back by the `ActionsSuppressor`).
    #[serde(default)]
    pub pending_action_from: Option<PreviousState>,
    #[serde(default)]
    pub alarm_exit: AlarmExit,
}

/// An alarm's mute window: what it was when the window started.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MuteWindow {
    /// The alarm's state when the window started. When the window ends, the
    /// actions of the alarm's state then run if it differs.
    pub start: PreviousState,
    /// A composite alarm's actions its `ActionsSuppressor` was already
    /// holding when the window started (`pending_action_from`), kept if
    /// nothing replaced them during the window.
    #[serde(default)]
    pub composite_pending: Option<PreviousState>,
}

/// An alarm's state before a transition, as alarm notifications report it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PreviousState {
    pub value: AlarmState,
    pub reason: String,
    #[serde(default)]
    pub reason_data: Option<String>,
    pub timestamp: DateTime<Utc>,
}

/// A log alarm: evaluates a scheduled CloudWatch Logs query's results against
/// a threshold. The scheduled query itself is service-managed, so the
/// configuration is stored verbatim and echoed back on describe.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogAlarm {
    pub alarm_name: String,
    pub alarm_arn: String,
    pub alarm_description: Option<String>,
    /// The `ScheduledQueryConfiguration` members, stored as supplied.
    pub query_string: String,
    pub scheduled_query_role_arn: String,
    pub schedule_expression: Option<String>,
    pub schedule_start_time_offset: Option<i64>,
    pub schedule_end_time_offset: Option<i64>,
    pub aggregation_expression: String,
    pub log_group_identifiers: Vec<String>,
    pub query_arn: Option<String>,
    pub query_results_to_evaluate: i64,
    pub query_results_to_alarm: i64,
    pub threshold: f64,
    pub comparison_operator: String,
    pub treat_missing_data: Option<String>,
    pub action_log_line_count: Option<i64>,
    pub action_log_line_role_arn: Option<String>,
    pub actions_enabled: bool,
    pub ok_actions: Vec<String>,
    pub alarm_actions: Vec<String>,
    pub insufficient_data_actions: Vec<String>,
    pub state_value: AlarmState,
    pub state_reason: String,
    pub state_updated_timestamp: DateTime<Utc>,
    pub alarm_configuration_updated_timestamp: DateTime<Utc>,
    /// `WarmUpConfiguration`, kept in flat-key form.
    #[serde(default)]
    pub warm_up_configuration: Option<FlatMembers>,
    /// `ActionConfiguration`, kept in flat-key form.
    #[serde(default)]
    pub action_configuration: Option<FlatMembers>,
    /// `StateReasonData` given to `SetAlarmState`.
    #[serde(default)]
    pub state_reason_data: Option<String>,
    /// `StateTransitionedTimestamp`: when `StateValue` last changed.
    #[serde(default)]
    pub state_transitioned_timestamp: Option<DateTime<Utc>>,
    #[serde(default)]
    pub alarm_exit: AlarmExit,
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
    /// `Configuration` (`MetricTimezone` + `ExcludedTimeRanges`) supplied on
    /// PutAnomalyDetector; echoed back on DescribeAnomalyDetectors.
    #[serde(default)]
    pub configuration: Option<AnomalyDetectorConfiguration>,
    /// `MetricCharacteristics.PeriodicSpikes`.
    #[serde(default)]
    pub periodic_spikes: Option<bool>,
}

/// The `Configuration` of an anomaly detector.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AnomalyDetectorConfiguration {
    #[serde(default)]
    pub excluded_time_ranges: Vec<ExcludedTimeRange>,
    #[serde(default)]
    pub metric_timezone: Option<String>,
    /// `AdaptationSpeed`: `FAST`, `NORMAL` or `SLOW`.
    #[serde(default)]
    pub adaptation_speed: Option<String>,
    /// `OutlierFilteringStrength`: `MINIMAL`, `MODERATE` or `STRICT`.
    #[serde(default)]
    pub outlier_filtering_strength: Option<String>,
}

/// A single `ExcludedTimeRanges` entry (`Range`).
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ExcludedTimeRange {
    #[serde(default)]
    pub start_time: Option<String>,
    #[serde(default)]
    pub end_time: Option<String>,
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
    /// When the rule was first created (kept across updates).
    #[serde(default)]
    pub created_timestamp: Option<DateTime<Utc>>,
}
