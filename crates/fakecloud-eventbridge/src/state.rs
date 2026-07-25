use chrono::{DateTime, Utc};
use fakecloud_aws::arn::Arn;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBus {
    pub name: String,
    pub arn: String,
    pub tags: BTreeMap<String, String>,
    pub policy: Option<Value>,
    pub description: Option<String>,
    pub kms_key_identifier: Option<String>,
    pub dead_letter_config: Option<Value>,
    pub creation_time: DateTime<Utc>,
    pub last_modified_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventRule {
    pub name: String,
    pub arn: String,
    pub event_bus_name: String,
    pub event_pattern: Option<String>,
    pub schedule_expression: Option<String>,
    pub state: String,
    pub description: Option<String>,
    pub role_arn: Option<String>,
    pub managed_by: Option<String>,
    pub created_by: Option<String>,
    pub targets: Vec<EventTarget>,
    pub tags: BTreeMap<String, String>,
    pub last_fired: Option<DateTime<Utc>>,
}

/// Composite key for rules: (event_bus_name, rule_name)
pub type RuleKey = (String, String);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventTarget {
    pub id: String,
    pub arn: String,
    pub input: Option<String>,
    pub input_path: Option<String>,
    pub input_transformer: Option<Value>,
    pub sqs_parameters: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_arn: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dead_letter_config: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ecs_parameters: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub batch_parameters: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kinesis_parameters: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redshift_data_parameters: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_parameters: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sage_maker_pipeline_parameters: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_sync_parameters: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_command_parameters: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutEvent {
    pub event_id: String,
    pub source: String,
    pub detail_type: String,
    pub detail: String,
    pub event_bus_name: String,
    pub time: DateTime<Utc>,
    pub resources: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Archive {
    pub name: String,
    pub arn: String,
    pub event_source_arn: String,
    pub description: Option<String>,
    pub event_pattern: Option<String>,
    pub retention_days: i64,
    pub state: String,
    pub creation_time: DateTime<Utc>,
    pub event_count: i64,
    pub size_bytes: i64,
    pub events: Vec<PutEvent>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Connection {
    pub name: String,
    pub arn: String,
    pub description: Option<String>,
    pub authorization_type: String,
    pub auth_parameters: Value,
    pub connection_state: String,
    pub secret_arn: String,
    pub creation_time: DateTime<Utc>,
    pub last_modified_time: DateTime<Utc>,
    pub last_authorized_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiDestination {
    pub name: String,
    pub arn: String,
    pub description: Option<String>,
    pub connection_arn: String,
    pub invocation_endpoint: String,
    pub http_method: String,
    pub invocation_rate_limit_per_second: Option<i64>,
    pub state: String,
    pub creation_time: DateTime<Utc>,
    pub last_modified_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Replay {
    pub name: String,
    pub arn: String,
    pub description: Option<String>,
    pub event_source_arn: String,
    pub destination: Value,
    pub event_start_time: DateTime<Utc>,
    pub event_end_time: DateTime<Utc>,
    pub state: String,
    pub replay_start_time: DateTime<Utc>,
    pub replay_end_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Endpoint {
    pub name: String,
    pub arn: String,
    pub endpoint_id: String,
    pub endpoint_url: Option<String>,
    pub description: Option<String>,
    pub routing_config: Value,
    pub replication_config: Option<Value>,
    pub event_buses: Vec<Value>,
    pub role_arn: Option<String>,
    pub state: String,
    pub creation_time: DateTime<Utc>,
    pub last_modified_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartnerEventSource {
    pub name: String,
    pub arn: String,
    pub account: String,
    pub creation_time: DateTime<Utc>,
    pub expiration_time: Option<DateTime<Utc>>,
    pub state: String,
}

/// A recorded Lambda invocation from EventBridge delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LambdaInvocation {
    pub function_arn: String,
    pub payload: String,
    pub timestamp: DateTime<Utc>,
}

/// A recorded CloudWatch Logs delivery from EventBridge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogDelivery {
    pub log_group_arn: String,
    pub payload: String,
    pub timestamp: DateTime<Utc>,
}

/// A recorded Step Functions invocation from EventBridge delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepFunctionExecution {
    pub state_machine_arn: String,
    pub payload: String,
    pub timestamp: DateTime<Utc>,
}

/// JSON object keys must be strings, so serialize `HashMap<(String,String), V>`
/// as a list of `[bus, rule, value]` tuples.
mod rule_map_serde {
    use super::{EventRule, RuleKey};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;

    pub fn serialize<S: Serializer>(
        map: &BTreeMap<RuleKey, EventRule>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        let entries: Vec<(&String, &String, &EventRule)> = map
            .iter()
            .map(|((bus, name), rule)| (bus, name, rule))
            .collect();
        entries.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<BTreeMap<RuleKey, EventRule>, D::Error> {
        let entries: Vec<(String, String, EventRule)> = Vec::deserialize(d)?;
        Ok(entries
            .into_iter()
            .map(|(bus, name, rule)| ((bus, name), rule))
            .collect())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBridgeState {
    pub account_id: String,
    pub region: String,
    pub buses: BTreeMap<String, EventBus>,
    #[serde(with = "rule_map_serde")]
    pub rules: BTreeMap<RuleKey, EventRule>,
    pub events: Vec<PutEvent>,
    pub archives: BTreeMap<String, Archive>,
    pub connections: BTreeMap<String, Connection>,
    pub api_destinations: BTreeMap<String, ApiDestination>,
    pub replays: BTreeMap<String, Replay>,
    /// Partner event sources: name -> PartnerEventSource
    pub partner_event_sources: BTreeMap<String, PartnerEventSource>,
    /// Endpoints: name -> Endpoint
    pub endpoints: BTreeMap<String, Endpoint>,
    /// Recorded Lambda invocations (stub deliveries).
    pub lambda_invocations: Vec<LambdaInvocation>,
    /// Recorded CloudWatch Logs deliveries (stub deliveries).
    pub log_deliveries: Vec<LogDelivery>,
    /// Recorded Step Functions executions (stub deliveries).
    pub step_function_executions: Vec<StepFunctionExecution>,
}

impl EventBridgeState {
    pub fn new(account_id: &str, region: &str) -> Self {
        let now = Utc::now();
        let default_bus_arn =
            Arn::new("events", region, account_id, "event-bus/default").to_string();
        let mut buses = BTreeMap::new();
        buses.insert(
            "default".to_string(),
            EventBus {
                name: "default".to_string(),
                arn: default_bus_arn,
                tags: BTreeMap::new(),
                policy: None,
                description: None,
                kms_key_identifier: None,
                dead_letter_config: None,
                creation_time: now,
                last_modified_time: now,
            },
        );

        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            buses,
            rules: BTreeMap::new(),
            events: Vec::new(),
            archives: BTreeMap::new(),
            connections: BTreeMap::new(),
            api_destinations: BTreeMap::new(),
            replays: BTreeMap::new(),
            partner_event_sources: BTreeMap::new(),
            endpoints: BTreeMap::new(),
            lambda_invocations: Vec::new(),
            log_deliveries: Vec::new(),
            step_function_executions: Vec::new(),
        }
    }

    /// Get the bus name from an ARN or a plain name.
    pub fn resolve_bus_name(&self, name_or_arn: &str) -> String {
        if name_or_arn.starts_with("arn:") {
            // Extract bus name from ARN: arn:aws:events:region:account:event-bus/NAME
            name_or_arn
                .rsplit_once("event-bus/")
                .map(|(_, n)| n.to_string())
                .unwrap_or_else(|| name_or_arn.to_string())
        } else {
            name_or_arn.to_string()
        }
    }

    pub fn reset(&mut self) {
        self.buses.clear();
        self.rules.clear();
        self.events.clear();
        self.partner_event_sources.clear();
        self.endpoints.clear();
        self.lambda_invocations.clear();
        self.log_deliveries.clear();
        self.step_function_executions.clear();
        // Re-create default bus. NOTE: the default bus is seeded here (and at
        // init) before any request exists, so this stored ARN carries the frozen
        // server region rather than the request's credential-scope region. This
        // seed remains the storage key/existence record; handlers that RETURN the
        // default-bus ARN (DescribeEventBus / ListEventBuses / etc.) restamp the
        // region from req.region at read time via `arn_with_request_region`.
        let default_bus_arn = format!(
            "arn:aws:events:{}:{}:event-bus/default",
            self.region, self.account_id
        );
        self.buses.insert(
            "default".to_string(),
            EventBus {
                name: "default".to_string(),
                arn: default_bus_arn,
                tags: BTreeMap::new(),
                policy: None,
                description: None,
                kms_key_identifier: None,
                dead_letter_config: None,
                creation_time: Utc::now(),
                last_modified_time: Utc::now(),
            },
        );
    }
}

pub type SharedEventBridgeState =
    Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<EventBridgeState>>>;

impl fakecloud_core::multi_account::AccountState for EventBridgeState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_creates_default_bus() {
        let state = EventBridgeState::new("123456789012", "us-east-1");
        assert!(state.buses.contains_key("default"));
        assert_eq!(state.account_id, "123456789012");
        assert_eq!(state.region, "us-east-1");
    }

    #[test]
    fn resolve_bus_name_from_arn() {
        let state = EventBridgeState::new("123456789012", "us-east-1");
        assert_eq!(
            state.resolve_bus_name("arn:aws:events:us-east-1:123456789012:event-bus/my-bus"),
            "my-bus"
        );
    }

    #[test]
    fn resolve_bus_name_plain() {
        let state = EventBridgeState::new("123456789012", "us-east-1");
        assert_eq!(state.resolve_bus_name("my-bus"), "my-bus");
    }

    #[test]
    fn resolve_bus_name_invalid_arn_falls_back() {
        let state = EventBridgeState::new("123456789012", "us-east-1");
        // ARN-looking string without event-bus/ prefix
        assert_eq!(
            state.resolve_bus_name("arn:aws:events:us-east-1:123456789012:rule/r"),
            "arn:aws:events:us-east-1:123456789012:rule/r"
        );
    }

    #[test]
    fn reset_recreates_default_bus() {
        let mut state = EventBridgeState::new("123456789012", "us-east-1");
        state.buses.clear();
        assert!(!state.buses.contains_key("default"));
        state.reset();
        assert!(state.buses.contains_key("default"));
    }
}

/// On-disk snapshot envelope for EventBridge state. Versioned so
/// format changes fail loudly on upgrade.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBridgeSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiAccountState<EventBridgeState>>,
    #[serde(default)]
    pub state: Option<EventBridgeState>,
}

pub const EVENTBRIDGE_SNAPSHOT_SCHEMA_VERSION: u32 = 2;
