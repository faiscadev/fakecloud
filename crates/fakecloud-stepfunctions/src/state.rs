use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde_json::Value;

pub type SharedStepFunctionsState = Arc<RwLock<StepFunctionsState>>;

pub struct StepFunctionsState {
    pub account_id: String,
    pub region: String,
    /// State machines keyed by ARN.
    pub state_machines: HashMap<String, StateMachine>,
    /// Executions keyed by execution ARN.
    pub executions: HashMap<String, Execution>,
}

#[derive(Debug, Clone)]
pub struct StateMachine {
    pub name: String,
    pub arn: String,
    pub definition: String,
    pub role_arn: String,
    pub machine_type: StateMachineType,
    pub status: StateMachineStatus,
    pub creation_date: DateTime<Utc>,
    pub update_date: DateTime<Utc>,
    pub tags: HashMap<String, String>,
    pub revision_id: String,
    pub logging_configuration: Option<Value>,
    pub tracing_configuration: Option<Value>,
    pub description: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateMachineType {
    Standard,
    Express,
}

impl StateMachineType {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Standard => "STANDARD",
            Self::Express => "EXPRESS",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "STANDARD" => Some(Self::Standard),
            "EXPRESS" => Some(Self::Express),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateMachineStatus {
    Active,
    Deleting,
}

impl StateMachineStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "ACTIVE",
            Self::Deleting => "DELETING",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Execution {
    pub execution_arn: String,
    pub state_machine_arn: String,
    pub state_machine_name: String,
    pub name: String,
    pub status: ExecutionStatus,
    pub input: Option<String>,
    pub output: Option<String>,
    pub start_date: DateTime<Utc>,
    pub stop_date: Option<DateTime<Utc>>,
    pub error: Option<String>,
    pub cause: Option<String>,
    pub history_events: Vec<HistoryEvent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionStatus {
    Running,
    Succeeded,
    Failed,
    TimedOut,
    Aborted,
    PendingRedrive,
}

impl ExecutionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Running => "RUNNING",
            Self::Succeeded => "SUCCEEDED",
            Self::Failed => "FAILED",
            Self::TimedOut => "TIMED_OUT",
            Self::Aborted => "ABORTED",
            Self::PendingRedrive => "PENDING_REDRIVE",
        }
    }
}

#[derive(Debug, Clone)]
pub struct HistoryEvent {
    pub id: i64,
    pub event_type: String,
    pub timestamp: DateTime<Utc>,
    pub previous_event_id: i64,
    pub details: Value,
}

impl StepFunctionsState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            state_machines: HashMap::new(),
            executions: HashMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.state_machines.clear();
        self.executions.clear();
    }

    pub fn state_machine_arn(&self, name: &str) -> String {
        format!(
            "arn:aws:states:{}:{}:stateMachine:{}",
            self.region, self.account_id, name
        )
    }

    pub fn execution_arn(&self, state_machine_name: &str, execution_name: &str) -> String {
        format!(
            "arn:aws:states:{}:{}:execution:{}:{}",
            self.region, self.account_id, state_machine_name, execution_name
        )
    }
}
