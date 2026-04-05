use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct EventBus {
    pub name: String,
    pub arn: String,
    pub tags: HashMap<String, String>,
    pub policy: Option<Value>,
    pub description: Option<String>,
    pub kms_key_identifier: Option<String>,
    pub dead_letter_config: Option<Value>,
    pub creation_time: DateTime<Utc>,
    pub last_modified_time: DateTime<Utc>,
}

#[derive(Debug, Clone)]
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
    pub tags: HashMap<String, String>,
    pub last_fired: Option<DateTime<Utc>>,
}

/// Composite key for rules: (event_bus_name, rule_name)
pub type RuleKey = (String, String);

#[derive(Debug, Clone)]
pub struct EventTarget {
    pub id: String,
    pub arn: String,
    pub input: Option<String>,
    pub input_path: Option<String>,
    pub input_transformer: Option<Value>,
    pub sqs_parameters: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct PutEvent {
    pub event_id: String,
    pub source: String,
    pub detail_type: String,
    pub detail: String,
    pub event_bus_name: String,
    pub time: DateTime<Utc>,
    pub resources: Vec<String>,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

/// A recorded Lambda invocation from EventBridge delivery.
#[derive(Debug, Clone)]
pub struct LambdaInvocation {
    pub function_arn: String,
    pub payload: String,
    pub timestamp: DateTime<Utc>,
}

/// A recorded CloudWatch Logs delivery from EventBridge.
#[derive(Debug, Clone)]
pub struct LogDelivery {
    pub log_group_arn: String,
    pub payload: String,
    pub timestamp: DateTime<Utc>,
}

/// A recorded Step Functions invocation from EventBridge delivery.
#[derive(Debug, Clone)]
pub struct StepFunctionExecution {
    pub state_machine_arn: String,
    pub payload: String,
    pub timestamp: DateTime<Utc>,
}

pub struct EventBridgeState {
    pub account_id: String,
    pub region: String,
    pub buses: HashMap<String, EventBus>,
    pub rules: HashMap<RuleKey, EventRule>,
    pub events: Vec<PutEvent>,
    pub archives: HashMap<String, Archive>,
    pub connections: HashMap<String, Connection>,
    pub api_destinations: HashMap<String, ApiDestination>,
    pub replays: HashMap<String, Replay>,
    /// Partner event sources: name -> account
    pub partner_event_sources: HashMap<String, String>,
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
        let default_bus_arn = format!("arn:aws:events:{region}:{account_id}:event-bus/default");
        let mut buses = HashMap::new();
        buses.insert(
            "default".to_string(),
            EventBus {
                name: "default".to_string(),
                arn: default_bus_arn,
                tags: HashMap::new(),
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
            rules: HashMap::new(),
            events: Vec::new(),
            archives: HashMap::new(),
            connections: HashMap::new(),
            api_destinations: HashMap::new(),
            replays: HashMap::new(),
            partner_event_sources: HashMap::new(),
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
        self.lambda_invocations.clear();
        self.log_deliveries.clear();
        self.step_function_executions.clear();
        // Re-create default bus
        let default_bus_arn = format!(
            "arn:aws:events:{}:{}:event-bus/default",
            self.region, self.account_id
        );
        self.buses.insert(
            "default".to_string(),
            EventBus {
                name: "default".to_string(),
                arn: default_bus_arn,
                tags: HashMap::new(),
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

pub type SharedEventBridgeState = Arc<RwLock<EventBridgeState>>;
