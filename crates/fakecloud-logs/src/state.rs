use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

pub type SharedLogsState = Arc<RwLock<LogsState>>;

pub struct LogsState {
    pub account_id: String,
    pub region: String,
    pub log_groups: HashMap<String, LogGroup>,
    pub metric_filters: Vec<MetricFilter>,
    pub resource_policies: HashMap<String, ResourcePolicy>,
    pub destinations: HashMap<String, Destination>,
    pub queries: HashMap<String, QueryInfo>,
    pub export_tasks: Vec<ExportTask>,
    pub delivery_destinations: HashMap<String, DeliveryDestination>,
    pub delivery_sources: HashMap<String, DeliverySource>,
    pub deliveries: HashMap<String, Delivery>,
    pub query_definitions: HashMap<String, QueryDefinition>,
    /// Account policies keyed by (policy_name, policy_type)
    pub account_policies: HashMap<(String, String), AccountPolicy>,
    /// Anomaly detectors keyed by detector ARN
    pub anomaly_detectors: HashMap<String, AnomalyDetector>,
    /// Import tasks keyed by import ID
    pub import_tasks: HashMap<String, ImportTask>,
    /// Integrations keyed by integration name
    pub integrations: HashMap<String, Integration>,
    /// Lookup tables keyed by ARN
    pub lookup_tables: HashMap<String, LookupTable>,
    /// Scheduled queries keyed by identifier (ARN)
    pub scheduled_queries: HashMap<String, ScheduledQuery>,
    /// S3 table integration sources keyed by integration ARN -> list of source identifiers
    pub s3_table_sources: HashMap<String, Vec<String>>,
    /// Bearer token authentication flag per log group
    pub bearer_token_auth: HashMap<String, bool>,
    /// Internal export storage: keyed by "bucket/prefix/..." path, value is exported data.
    /// Used by CreateExportTask and delivery pipeline when direct S3 access is unavailable.
    pub export_storage: HashMap<String, Vec<u8>>,
}

impl LogsState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            log_groups: HashMap::new(),
            metric_filters: Vec::new(),
            resource_policies: HashMap::new(),
            destinations: HashMap::new(),
            queries: HashMap::new(),
            export_tasks: Vec::new(),
            delivery_destinations: HashMap::new(),
            delivery_sources: HashMap::new(),
            deliveries: HashMap::new(),
            query_definitions: HashMap::new(),
            account_policies: HashMap::new(),
            anomaly_detectors: HashMap::new(),
            import_tasks: HashMap::new(),
            integrations: HashMap::new(),
            lookup_tables: HashMap::new(),
            scheduled_queries: HashMap::new(),
            s3_table_sources: HashMap::new(),
            bearer_token_auth: HashMap::new(),
            export_storage: HashMap::new(),
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
    }
}

pub struct LogGroup {
    pub name: String,
    pub arn: String,
    pub creation_time: i64,
    pub retention_in_days: Option<i32>,
    pub kms_key_id: Option<String>,
    pub tags: HashMap<String, String>,
    pub log_streams: HashMap<String, LogStream>,
    pub stored_bytes: i64,
    pub subscription_filters: Vec<SubscriptionFilter>,
    pub data_protection_policy: Option<DataProtectionPolicy>,
    pub index_policies: Vec<IndexPolicy>,
    pub transformer: Option<Transformer>,
    pub deletion_protection: bool,
}

pub struct LogStream {
    pub name: String,
    pub arn: String,
    pub creation_time: i64,
    pub first_event_timestamp: Option<i64>,
    pub last_event_timestamp: Option<i64>,
    pub last_ingestion_time: Option<i64>,
    pub upload_sequence_token: String,
    pub events: Vec<LogEvent>,
}

#[derive(Clone)]
pub struct LogEvent {
    pub timestamp: i64,
    pub message: String,
    pub ingestion_time: i64,
}

pub struct SubscriptionFilter {
    pub filter_name: String,
    pub log_group_name: String,
    pub filter_pattern: String,
    pub destination_arn: String,
    pub role_arn: Option<String>,
    pub distribution: String,
    pub creation_time: i64,
}

pub struct MetricFilter {
    pub filter_name: String,
    pub filter_pattern: String,
    pub log_group_name: String,
    pub metric_transformations: Vec<MetricTransformation>,
    pub creation_time: i64,
}

pub struct MetricTransformation {
    pub metric_name: String,
    pub metric_namespace: String,
    pub metric_value: String,
    pub default_value: Option<f64>,
}

pub struct ResourcePolicy {
    pub policy_name: String,
    pub policy_document: String,
    pub last_updated_time: i64,
}

pub struct Destination {
    pub destination_name: String,
    pub target_arn: String,
    pub role_arn: String,
    pub arn: String,
    pub access_policy: Option<String>,
    pub creation_time: i64,
    pub tags: HashMap<String, String>,
}

pub struct QueryInfo {
    pub query_id: String,
    pub log_group_name: String,
    pub query_string: String,
    pub start_time: i64,
    pub end_time: i64,
    pub status: String,
    pub create_time: i64,
}

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
}

pub struct DeliveryDestination {
    pub name: String,
    pub arn: String,
    pub output_format: Option<String>,
    pub delivery_destination_configuration: HashMap<String, String>,
    pub tags: HashMap<String, String>,
    pub delivery_destination_policy: Option<String>,
}

pub struct DeliverySource {
    pub name: String,
    pub arn: String,
    pub resource_arns: Vec<String>,
    pub service: String,
    pub log_type: String,
    pub tags: HashMap<String, String>,
}

pub struct Delivery {
    pub id: String,
    pub delivery_source_name: String,
    pub delivery_destination_arn: String,
    pub delivery_destination_type: String,
    pub arn: String,
    pub tags: HashMap<String, String>,
}

pub struct QueryDefinition {
    pub query_definition_id: String,
    pub name: String,
    pub query_string: String,
    pub log_group_names: Vec<String>,
    pub last_modified: i64,
}

pub struct AccountPolicy {
    pub policy_name: String,
    pub policy_type: String,
    pub policy_document: String,
    pub scope: Option<String>,
    pub selection_criteria: Option<String>,
    pub account_id: String,
    pub last_updated_time: i64,
}

pub struct DataProtectionPolicy {
    pub policy_document: String,
    pub last_updated_time: i64,
}

pub struct IndexPolicy {
    pub policy_name: String,
    pub policy_document: String,
    pub last_updated_time: i64,
}

pub struct Transformer {
    pub transformer_config: serde_json::Value,
    pub creation_time: i64,
    pub last_modified_time: i64,
}

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
}

pub struct ImportTask {
    pub import_id: String,
    pub import_source_arn: String,
    pub import_role_arn: String,
    pub log_group_name: Option<String>,
    pub status: String,
    pub creation_time: i64,
}

pub struct Integration {
    pub integration_name: String,
    pub integration_type: String,
    pub resource_config: serde_json::Value,
    pub status: String,
    pub creation_time: i64,
}

pub struct LookupTable {
    pub lookup_table_name: String,
    pub arn: String,
    pub table_body: String,
    pub creation_time: i64,
    pub last_modified_time: i64,
}

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
}
