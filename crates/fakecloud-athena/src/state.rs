//! In-memory state for Athena.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub type SharedAthenaState = Arc<RwLock<AthenaAccounts>>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AthenaAccounts {
    pub accounts: BTreeMap<String, AccountState>,
}

impl AthenaAccounts {
    pub fn new() -> Self {
        Self::default()
    }
}

/// On-disk snapshot envelope for Athena state. Versioned so format changes fail
/// loudly on upgrade rather than silently mis-parsing.
#[derive(Clone, Serialize, Deserialize)]
pub struct AthenaSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<AthenaAccounts>,
}

pub const ATHENA_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AccountState {
    pub work_groups: BTreeMap<String, WorkGroup>,
    pub data_catalogs: BTreeMap<String, DataCatalog>,
    pub named_queries: BTreeMap<String, NamedQuery>,
    /// Keyed by `(workgroup, statement_name)`. JSON object keys must be
    /// strings, so this tuple-keyed map is (de)serialized as a sequence of
    /// `[workgroup, name, statement]` triples for persistence snapshots.
    #[serde(with = "prepared_statements_serde")]
    pub prepared_statements: BTreeMap<(String, String), PreparedStatement>,
    pub query_executions: BTreeMap<String, QueryExecution>,
    pub notebooks: BTreeMap<String, Notebook>,
    pub sessions: BTreeMap<String, Session>,
    pub calculations: BTreeMap<String, Calculation>,
    pub capacity_reservations: BTreeMap<String, CapacityReservation>,
    pub capacity_assignment_config: Option<CapacityAssignmentConfiguration>,
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
    pub initialized: bool,
}

/// (De)serialize a `(workgroup, name) -> PreparedStatement` map as a sequence
/// of `(workgroup, name, statement)` triples. JSON object keys must be strings,
/// so the natural tuple-keyed map cannot be serialized directly.
mod prepared_statements_serde {
    use std::collections::BTreeMap;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    use super::PreparedStatement;

    pub fn serialize<S: Serializer>(
        map: &BTreeMap<(String, String), PreparedStatement>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        let entries: Vec<(&String, &String, &PreparedStatement)> =
            map.iter().map(|((wg, name), ps)| (wg, name, ps)).collect();
        entries.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<(String, String), PreparedStatement>, D::Error> {
        let entries: Vec<(String, String, PreparedStatement)> = Vec::deserialize(deserializer)?;
        Ok(entries
            .into_iter()
            .map(|(wg, name, ps)| ((wg, name), ps))
            .collect())
    }
}

impl AccountState {
    /// Seed the default `primary` workgroup the first time the account is touched
    /// — Athena always exposes a primary workgroup that callers expect to exist.
    pub fn ensure_initialized(&mut self) {
        if self.initialized {
            return;
        }
        self.initialized = true;
        let primary = WorkGroup {
            name: "primary".to_string(),
            state: "ENABLED".to_string(),
            description: Some("default primary workgroup".to_string()),
            configuration: Some(default_workgroup_configuration()),
            creation_time: Utc::now(),
            engine_version: Some("AUTO".to_string()),
        };
        self.work_groups.insert("primary".to_string(), primary);

        let default_catalog = DataCatalog {
            name: "AwsDataCatalog".to_string(),
            description: Some("Default AWS data catalog".to_string()),
            cat_type: "GLUE".to_string(),
            parameters: BTreeMap::new(),
            status: "CREATE_COMPLETE".to_string(),
            connection_type: None,
            error: None,
        };
        self.data_catalogs
            .insert("AwsDataCatalog".to_string(), default_catalog);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkGroup {
    pub name: String,
    pub state: String,
    pub description: Option<String>,
    pub configuration: Option<Value>,
    pub creation_time: DateTime<Utc>,
    pub engine_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataCatalog {
    pub name: String,
    pub description: Option<String>,
    pub cat_type: String,
    pub parameters: BTreeMap<String, String>,
    pub status: String,
    pub connection_type: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedQuery {
    pub named_query_id: String,
    pub name: String,
    pub description: Option<String>,
    pub database: String,
    pub query_string: String,
    pub work_group: String,
    /// Last time this named query was referenced by `StartQueryExecution`.
    /// `None` until the first invocation; populated by the
    /// `/_fakecloud/athena/named-queries` introspection endpoint so test
    /// authors can assert that a saved query was actually used.
    #[serde(default)]
    pub last_used_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreparedStatement {
    pub statement_name: String,
    pub work_group_name: String,
    pub query_statement: String,
    pub description: Option<String>,
    pub last_modified_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryExecution {
    pub query_execution_id: String,
    pub query: String,
    pub statement_type: String,
    pub work_group: String,
    pub state: String,
    pub state_change_reason: Option<String>,
    pub submission_time: DateTime<Utc>,
    pub completion_time: Option<DateTime<Utc>>,
    pub query_execution_context: Option<Value>,
    pub result_configuration: Option<Value>,
    pub engine_version: Option<Value>,
    pub data_scanned_bytes: i64,
    pub engine_execution_time_ms: i64,
    pub query_planning_time_ms: i64,
    pub total_execution_time_ms: i64,
    pub result_rows: Vec<Vec<String>>,
    pub result_columns: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notebook {
    pub notebook_id: String,
    pub name: String,
    pub work_group: String,
    pub creation_time: DateTime<Utc>,
    pub last_modified_time: DateTime<Utc>,
    pub payload: String,
    pub notebook_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    pub session_id: String,
    pub work_group: String,
    /// Notebook that started this session, when StartSession was called
    /// with a NotebookId. `None` for ad-hoc work-group sessions.
    #[serde(default)]
    pub notebook_id: Option<String>,
    pub description: Option<String>,
    pub engine_version: Option<String>,
    pub state: String,
    pub start_date_time: DateTime<Utc>,
    pub end_date_time: Option<DateTime<Utc>>,
    pub idle_since_date_time: Option<DateTime<Utc>>,
    pub configuration: Option<Value>,
    pub notebook_version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Calculation {
    pub calculation_execution_id: String,
    pub session_id: String,
    pub description: Option<String>,
    pub state: String,
    pub state_change_reason: Option<String>,
    pub working_directory: Option<String>,
    pub code_block: Option<String>,
    pub submission_date_time: DateTime<Utc>,
    pub completion_date_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityReservation {
    pub name: String,
    pub status: String,
    pub target_dpus: i32,
    pub allocated_dpus: i32,
    pub creation_time: DateTime<Utc>,
    pub last_allocation: Option<DateTime<Utc>>,
    pub last_successful_allocation_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapacityAssignmentConfiguration {
    pub capacity_reservation_name: String,
    pub capacity_assignments: Vec<Value>,
}

fn default_workgroup_configuration() -> Value {
    serde_json::json!({
        "ResultConfiguration": {},
        "EnforceWorkGroupConfiguration": false,
        "PublishCloudWatchMetricsEnabled": false,
        "RequesterPaysEnabled": false,
        "EngineVersion": {"SelectedEngineVersion": "AUTO", "EffectiveEngineVersion": "Athena engine version 3"},
    })
}
