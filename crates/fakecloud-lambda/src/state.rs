use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct LambdaFunction {
    pub function_name: String,
    pub function_arn: String,
    pub runtime: String,
    pub role: String,
    pub handler: String,
    pub description: String,
    pub timeout: i64,
    pub memory_size: i64,
    pub code_sha256: String,
    pub code_size: i64,
    pub version: String,
    pub last_modified: DateTime<Utc>,
    pub tags: HashMap<String, String>,
    pub environment: HashMap<String, String>,
    pub architectures: Vec<String>,
    pub package_type: String,
}

#[derive(Debug, Clone)]
pub struct EventSourceMapping {
    pub uuid: String,
    pub function_arn: String,
    pub event_source_arn: String,
    pub batch_size: i64,
    pub enabled: bool,
    pub state: String,
    pub last_modified: DateTime<Utc>,
}

/// A recorded Lambda invocation from cross-service delivery.
#[derive(Debug, Clone)]
pub struct LambdaInvocation {
    pub function_arn: String,
    pub payload: String,
    pub timestamp: DateTime<Utc>,
    pub source: String,
}

pub struct LambdaState {
    pub account_id: String,
    pub region: String,
    pub functions: HashMap<String, LambdaFunction>,
    pub event_source_mappings: HashMap<String, EventSourceMapping>,
    /// Recorded invocations from cross-service integrations (SQS, EventBridge, etc.)
    pub invocations: Vec<LambdaInvocation>,
}

impl LambdaState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            functions: HashMap::new(),
            event_source_mappings: HashMap::new(),
            invocations: Vec::new(),
        }
    }

    pub fn reset(&mut self) {
        self.functions.clear();
        self.event_source_mappings.clear();
        self.invocations.clear();
    }
}

pub type SharedLambdaState = Arc<RwLock<LambdaState>>;
