use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;

pub type SharedLogsState = Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<LogsState>>>;

impl fakecloud_core::multi_account::AccountState for LogsState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

/// JSON object keys must be strings, so serialize
/// `HashMap<(String,String), AccountPolicy>` as a list of
/// `[policy_name, policy_type, policy]` tuples.
mod account_policy_map_serde {
    use super::AccountPolicy;
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;

    pub fn serialize<S: Serializer>(
        map: &BTreeMap<(String, String), AccountPolicy>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        let entries: Vec<(&String, &String, &AccountPolicy)> = map
            .iter()
            .map(|((name, kind), p)| (name, kind, p))
            .collect();
        entries.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<BTreeMap<(String, String), AccountPolicy>, D::Error> {
        let entries: Vec<(String, String, AccountPolicy)> = Vec::deserialize(d)?;
        Ok(entries
            .into_iter()
            .map(|(name, kind, p)| ((name, kind), p))
            .collect())
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct LogsState {
    pub account_id: String,
    pub region: String,
    pub log_groups: BTreeMap<String, LogGroup>,
    pub metric_filters: Vec<MetricFilter>,
    pub resource_policies: BTreeMap<String, ResourcePolicy>,
    pub destinations: BTreeMap<String, Destination>,
    pub queries: BTreeMap<String, QueryInfo>,
    pub export_tasks: Vec<ExportTask>,
    pub delivery_destinations: BTreeMap<String, DeliveryDestination>,
    pub delivery_sources: BTreeMap<String, DeliverySource>,
    pub deliveries: BTreeMap<String, Delivery>,
    pub query_definitions: BTreeMap<String, QueryDefinition>,
    /// Account policies keyed by (policy_name, policy_type)
    #[serde(with = "account_policy_map_serde")]
    pub account_policies: BTreeMap<(String, String), AccountPolicy>,
    /// Anomaly detectors keyed by detector ARN
    pub anomaly_detectors: BTreeMap<String, AnomalyDetector>,
    /// Import tasks keyed by import ID
    pub import_tasks: BTreeMap<String, ImportTask>,
    /// Integrations keyed by integration name
    pub integrations: BTreeMap<String, Integration>,
    /// Lookup tables keyed by ARN
    pub lookup_tables: BTreeMap<String, LookupTable>,
    /// Scheduled queries keyed by identifier (ARN)
    pub scheduled_queries: BTreeMap<String, ScheduledQuery>,
    /// S3 table integration sources keyed by integration ARN -> list of source identifiers
    pub s3_table_sources: BTreeMap<String, Vec<String>>,
    /// Bearer token authentication flag per log group
    pub bearer_token_auth: BTreeMap<String, bool>,
    /// Internal export storage: keyed by "bucket/prefix/..." path, value is exported data.
    /// Used by CreateExportTask and delivery pipeline when direct S3 access is unavailable.
    pub export_storage: BTreeMap<String, Vec<u8>>,
    /// Detected log anomalies keyed by anomaly id. Populated via the
    /// `/_fakecloud/logs/anomalies/inject` admin endpoint and surfaced
    /// through ListAnomalies / UpdateAnomaly.
    #[serde(default)]
    pub anomalies: BTreeMap<String, LogAnomaly>,
    /// Syslog configurations keyed by log group name. Each enables syslog
    /// ingestion into the log group through a VPC endpoint, surfaced through
    /// PutSyslogConfiguration / ListSyslogConfigurations / DeleteSyslogConfiguration.
    #[serde(default)]
    pub syslog_configurations: BTreeMap<String, SyslogConfiguration>,
    /// Account-level storage-tier policy set via PutStorageTierPolicy and
    /// read back through GetStorageTierPolicy. `STANDARD` or `INTELLIGENT_TIERING`.
    #[serde(default)]
    pub storage_tier: Option<String>,
    /// Epoch-millis timestamp of the last PutStorageTierPolicy call.
    #[serde(default)]
    pub storage_tier_last_updated: Option<i64>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SyslogConfiguration {
    pub log_group_arn: String,
    pub source_type: String,
    pub vpc_endpoint_id: Option<String>,
    pub created_at: i64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct LogAnomaly {
    pub anomaly_id: String,
    pub anomaly_detector_arn: String,
    pub log_group_arn_list: Vec<String>,
    pub pattern_id: String,
    pub pattern_string: String,
    pub first_seen: i64,
    pub last_seen: i64,
    pub priority: String,
    pub state: String,
    pub suppressed: bool,
}

impl LogsState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            log_groups: BTreeMap::new(),
            metric_filters: Vec::new(),
            resource_policies: BTreeMap::new(),
            destinations: BTreeMap::new(),
            queries: BTreeMap::new(),
            export_tasks: Vec::new(),
            delivery_destinations: BTreeMap::new(),
            delivery_sources: BTreeMap::new(),
            deliveries: BTreeMap::new(),
            query_definitions: BTreeMap::new(),
            account_policies: BTreeMap::new(),
            anomaly_detectors: BTreeMap::new(),
            import_tasks: BTreeMap::new(),
            integrations: BTreeMap::new(),
            lookup_tables: BTreeMap::new(),
            scheduled_queries: BTreeMap::new(),
            s3_table_sources: BTreeMap::new(),
            bearer_token_auth: BTreeMap::new(),
            export_storage: BTreeMap::new(),
            anomalies: BTreeMap::new(),
            syslog_configurations: BTreeMap::new(),
            storage_tier: None,
            storage_tier_last_updated: None,
        }
    }

    pub fn reset(&mut self) {
        self.log_groups.clear();
        self.metric_filters.clear();
        self.resource_policies.clear();
        self.destinations.clear();
        self.queries.clear();
        self.export_tasks.clear();
        self.delivery_destinations.clear();
        self.delivery_sources.clear();
        self.deliveries.clear();
        self.query_definitions.clear();
        self.account_policies.clear();
        self.anomaly_detectors.clear();
        self.import_tasks.clear();
        self.integrations.clear();
        self.lookup_tables.clear();
        self.scheduled_queries.clear();
        self.s3_table_sources.clear();
        self.bearer_token_auth.clear();
        self.export_storage.clear();
        self.anomalies.clear();
        self.syslog_configurations.clear();
        self.storage_tier = None;
        self.storage_tier_last_updated = None;
    }
}

/// The ARN of log group `name` in `region`'s partition, without the `:*`
/// suffix that `DescribeLogGroups` reports on `arn`.
pub fn log_group_arn(region: &str, account_id: &str, name: &str) -> String {
    fakecloud_aws::arn::Arn::regional("logs", region, account_id, &format!("log-group:{name}"))
        .to_string()
}

/// The ARN stored on [`LogGroup::arn`]: [`log_group_arn`] plus the trailing
/// `:*`, the form `DescribeLogGroups` reports on `arn`. Every path that creates
/// a log group (CreateLogGroup, implicit creation on ingest or EventBridge
/// delivery) stores this form so the group describes the same way regardless
/// of how it came to exist.
pub fn log_group_stored_arn(region: &str, account_id: &str, name: &str) -> String {
    format!("{}:*", log_group_arn(region, account_id, name))
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct LogGroup {
    pub name: String,
    pub arn: String,
    pub creation_time: i64,
    pub retention_in_days: Option<i32>,
    pub kms_key_id: Option<String>,
    pub tags: BTreeMap<String, String>,
    pub log_streams: BTreeMap<String, LogStream>,
    pub stored_bytes: i64,
    pub subscription_filters: Vec<SubscriptionFilter>,
    pub data_protection_policy: Option<DataProtectionPolicy>,
    pub index_policies: Vec<IndexPolicy>,
    pub transformer: Option<Transformer>,
    pub deletion_protection: bool,
    /// `STANDARD` (default), `INFREQUENT_ACCESS`, or `DELIVERY`. Set at
    /// creation time via `CreateLogGroup`'s `logGroupClass` parameter.
    /// Tracked here so `DescribeLogGroups` round-trips it correctly.
    pub log_group_class: Option<String>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct LogStream {
    /// Stable disk identity; a recreated stream must never reuse old segments.
    #[serde(default = "new_stream_generation")]
    pub persistence_id: String,
    /// Preserve the high-water mark even after retention removes every event.
    #[serde(default)]
    pub last_sequence: u64,
    pub name: String,
    pub arn: String,
    pub creation_time: i64,
    pub first_event_timestamp: Option<i64>,
    pub last_event_timestamp: Option<i64>,
    pub last_ingestion_time: Option<i64>,
    pub upload_sequence_token: String,
    pub events: Vec<LogEvent>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct LogEvent {
    pub timestamp: i64,
    pub message: String,
    pub ingestion_time: i64,
    /// A stable, monotonically-increasing per-stream identifier assigned at
    /// ingestion. Unlike an array index it never shifts when an earlier event
    /// is inserted, so a `FilterLogEvents` pagination cursor built from it
    /// resumes correctly even after the stream is re-sorted. Defaults to 0 for
    /// events restored from snapshots that predate this field.
    #[serde(default)]
    pub seq: u64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct SubscriptionFilter {
    pub filter_name: String,
    pub log_group_name: String,
    pub filter_pattern: String,
    pub destination_arn: String,
    pub role_arn: Option<String>,
    pub distribution: String,
    pub creation_time: i64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricFilter {
    pub filter_name: String,
    pub filter_pattern: String,
    pub log_group_name: String,
    pub metric_transformations: Vec<MetricTransformation>,
    pub creation_time: i64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricTransformation {
    pub metric_name: String,
    pub metric_namespace: String,
    pub metric_value: String,
    pub default_value: Option<f64>,
    /// CloudWatch unit for the published metric. AWS always reports it on
    /// DescribeMetricFilters, defaulting to `None` when unset, and the
    /// Terraform `aws_cloudwatch_log_metric_filter` resource asserts on it.
    #[serde(default)]
    pub unit: Option<String>,
    /// Dimensions to publish with the metric (name -> value template). Echoed
    /// on DescribeMetricFilters; the Terraform resource round-trips it.
    #[serde(default)]
    pub dimensions: Option<std::collections::BTreeMap<String, String>>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ResourcePolicy {
    pub policy_name: String,
    pub policy_document: String,
    pub last_updated_time: i64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Destination {
    pub destination_name: String,
    pub target_arn: String,
    pub role_arn: String,
    pub arn: String,
    pub access_policy: Option<String>,
    pub creation_time: i64,
    pub tags: BTreeMap<String, String>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct QueryInfo {
    pub query_id: String,
    pub log_group_name: String,
    /// Every log group / identifier referenced by this query, used by
    /// `ListLogGroupsForQuery`. Always includes `log_group_name` plus any
    /// names from `logGroupNames` / identifiers from `logGroupIdentifiers`
    /// passed at start time.
    #[serde(default)]
    pub log_group_identifiers: Vec<String>,
    pub query_string: String,
    pub start_time: i64,
    pub end_time: i64,
    pub status: String,
    pub create_time: i64,
    /// The `GetQueryResults` payload, computed once on the first poll. A
    /// query's results are a point-in-time snapshot on AWS, so later polls
    /// return the same rows without rescanning (and re-cloning) every event.
    /// Not persisted: after a restart the first poll recomputes it.
    #[serde(skip)]
    pub results: Option<std::sync::Arc<serde_json::Value>>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ExportTask {
    pub task_id: String,
    pub task_name: Option<String>,
    pub log_group_name: String,
    pub log_stream_name_prefix: Option<String>,
    pub from_time: i64,
    pub to_time: i64,
    pub destination: String,
    pub destination_prefix: String,
    pub status_code: String,
    pub status_message: String,
    #[serde(default)]
    pub creation_time: i64,
    #[serde(default)]
    pub completion_time: Option<i64>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct DeliveryDestination {
    pub name: String,
    pub arn: String,
    pub output_format: Option<String>,
    pub delivery_destination_configuration: BTreeMap<String, String>,
    /// `CWL`/`S3`/`FH`/`XRAY` — derived from the destination resource ARN when
    /// the caller does not specify it. AWS always reports it on read.
    #[serde(default)]
    pub delivery_destination_type: String,
    pub tags: BTreeMap<String, String>,
    pub delivery_destination_policy: Option<String>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct DeliverySource {
    pub name: String,
    pub arn: String,
    pub resource_arns: Vec<String>,
    pub service: String,
    pub log_type: String,
    pub tags: BTreeMap<String, String>,
    #[serde(default)]
    pub created_at: i64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Delivery {
    pub id: String,
    pub delivery_source_name: String,
    pub delivery_destination_arn: String,
    pub delivery_destination_type: String,
    pub arn: String,
    pub tags: BTreeMap<String, String>,
    #[serde(default)]
    pub field_delimiter: Option<String>,
    #[serde(default)]
    pub record_fields: Vec<String>,
    #[serde(default)]
    pub s3_delivery_configuration: Option<serde_json::Value>,
    #[serde(default)]
    pub created_at: i64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct QueryDefinition {
    pub query_definition_id: String,
    pub name: String,
    pub query_string: String,
    pub log_group_names: Vec<String>,
    pub last_modified: i64,
    /// Query language (CWLI / SQL / PPL). Echoed on DescribeQueryDefinitions;
    /// defaults to CWLI when the caller omits it.
    #[serde(default)]
    pub query_language: Option<String>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct AccountPolicy {
    pub policy_name: String,
    pub policy_type: String,
    pub policy_document: String,
    pub scope: Option<String>,
    pub selection_criteria: Option<String>,
    pub account_id: String,
    pub last_updated_time: i64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct DataProtectionPolicy {
    pub policy_document: String,
    pub last_updated_time: i64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexPolicy {
    pub policy_name: String,
    pub policy_document: String,
    pub last_updated_time: i64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Transformer {
    pub transformer_config: serde_json::Value,
    pub creation_time: i64,
    pub last_modified_time: i64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct AnomalyDetector {
    pub detector_name: String,
    pub arn: String,
    pub log_group_arn_list: Vec<String>,
    pub evaluation_frequency: Option<String>,
    pub filter_pattern: Option<String>,
    pub anomaly_visibility_time: Option<i64>,
    pub creation_time: i64,
    pub last_modified_time: i64,
    pub enabled: bool,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ImportTask {
    pub import_id: String,
    pub import_source_arn: String,
    pub import_role_arn: String,
    pub log_group_name: Option<String>,
    pub status: String,
    pub creation_time: i64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct Integration {
    pub integration_name: String,
    pub integration_type: String,
    pub resource_config: serde_json::Value,
    pub status: String,
    pub creation_time: i64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct LookupTable {
    pub lookup_table_name: String,
    pub arn: String,
    pub table_body: String,
    pub creation_time: i64,
    pub last_modified_time: i64,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub kms_key_id: Option<String>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct ScheduledQuery {
    pub name: String,
    pub arn: String,
    pub query_string: String,
    pub query_language: String,
    pub schedule_expression: String,
    pub execution_role_arn: String,
    pub status: String,
    pub creation_time: i64,
    pub last_modified_time: i64,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub timezone: Option<String>,
    #[serde(default)]
    pub schedule_start_time: Option<i64>,
    #[serde(default)]
    pub schedule_end_time: Option<i64>,
    /// The desired ENABLED/DISABLED state (defaults to ENABLED). Distinct from
    /// `status` (the server-derived lifecycle status).
    #[serde(default)]
    pub state: Option<String>,
}

/// On-disk snapshot envelope for CloudWatch Logs state. Versioned so
/// format changes fail loudly on upgrade.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct LogsSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<LogsState>>,
    #[serde(default)]
    pub state: Option<LogsState>,
}

pub const LOGS_SNAPSHOT_SCHEMA_VERSION: u32 = 2;

/// The only `retentionInDays` values CloudWatch Logs accepts. Retention deletes
/// stored events, so an out-of-set value (0, negative) must never reach state.
pub const VALID_RETENTION_DAYS: [i64; 22] = [
    1, 3, 5, 7, 14, 30, 60, 90, 120, 150, 180, 365, 400, 545, 731, 1096, 1827, 2192, 2557, 2922,
    3288, 3653,
];

fn new_stream_generation() -> String {
    uuid::Uuid::new_v4().to_string()
}

impl LogsState {
    pub(crate) fn metadata(&self) -> Self {
        Self {
            account_id: self.account_id.clone(),
            region: self.region.clone(),
            log_groups: self
                .log_groups
                .iter()
                .map(|(k, v)| (k.clone(), v.metadata()))
                .collect(),
            metric_filters: self.metric_filters.clone(),
            resource_policies: self.resource_policies.clone(),
            destinations: self.destinations.clone(),
            queries: self.queries.clone(),
            export_tasks: self.export_tasks.clone(),
            delivery_destinations: self.delivery_destinations.clone(),
            delivery_sources: self.delivery_sources.clone(),
            deliveries: self.deliveries.clone(),
            query_definitions: self.query_definitions.clone(),
            account_policies: self.account_policies.clone(),
            anomaly_detectors: self.anomaly_detectors.clone(),
            import_tasks: self.import_tasks.clone(),
            integrations: self.integrations.clone(),
            lookup_tables: self.lookup_tables.clone(),
            scheduled_queries: self.scheduled_queries.clone(),
            s3_table_sources: self.s3_table_sources.clone(),
            bearer_token_auth: self.bearer_token_auth.clone(),
            export_storage: self.export_storage.clone(),
            anomalies: self.anomalies.clone(),
            syslog_configurations: self.syslog_configurations.clone(),
            storage_tier: self.storage_tier.clone(),
            storage_tier_last_updated: self.storage_tier_last_updated,
        }
    }
}

impl LogGroup {
    /// The group's ARN without the trailing `:*` (the `logGroupArn` form, and
    /// the prefix of its log streams' ARNs). Tolerates a stored ARN that lacks
    /// the suffix, as snapshots from before the stored form was unified do.
    pub fn log_group_arn(&self) -> &str {
        self.arn.strip_suffix(":*").unwrap_or(&self.arn)
    }

    /// The group's ARN with the trailing `:*` (the `DescribeLogGroups` `arn`).
    pub fn wildcard_arn(&self) -> String {
        format!("{}:*", self.log_group_arn())
    }

    /// The ARN of log stream `stream_name` in this group.
    pub fn stream_arn(&self, stream_name: &str) -> String {
        format!("{}:log-stream:{stream_name}", self.log_group_arn())
    }

    pub(crate) fn metadata(&self) -> Self {
        Self {
            name: self.name.clone(),
            arn: self.arn.clone(),
            creation_time: self.creation_time,
            retention_in_days: self.retention_in_days,
            kms_key_id: self.kms_key_id.clone(),
            tags: self.tags.clone(),
            log_streams: self
                .log_streams
                .iter()
                .map(|(k, v)| (k.clone(), v.metadata()))
                .collect(),
            stored_bytes: self.stored_bytes,
            subscription_filters: self.subscription_filters.clone(),
            data_protection_policy: self.data_protection_policy.clone(),
            index_policies: self.index_policies.clone(),
            transformer: self.transformer.clone(),
            deletion_protection: self.deletion_protection,
            log_group_class: self.log_group_class.clone(),
        }
    }
}

impl LogStream {
    pub(crate) fn metadata(&self) -> Self {
        Self {
            persistence_id: self.persistence_id.clone(),
            last_sequence: self.last_sequence,
            name: self.name.clone(),
            arn: self.arn.clone(),
            creation_time: self.creation_time,
            first_event_timestamp: self.first_event_timestamp,
            last_event_timestamp: self.last_event_timestamp,
            last_ingestion_time: self.last_ingestion_time,
            upload_sequence_token: self.upload_sequence_token.clone(),
            events: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_initializes_empty() {
        let state = LogsState::new("123456789012", "us-east-1");
        assert_eq!(state.account_id, "123456789012");
        assert_eq!(state.region, "us-east-1");
        assert!(state.log_groups.is_empty());
        assert!(state.queries.is_empty());
    }

    #[test]
    fn reset_clears_state() {
        let mut state = LogsState::new("123456789012", "us-east-1");
        state.bearer_token_auth.insert("g".to_string(), true);
        state.reset();
        assert!(state.bearer_token_auth.is_empty());
    }
}
