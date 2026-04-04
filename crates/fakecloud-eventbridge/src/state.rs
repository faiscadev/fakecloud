use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct EventBus {
    pub name: String,
    pub arn: String,
    pub tags: HashMap<String, String>,
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
    pub targets: Vec<EventTarget>,
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct EventTarget {
    pub id: String,
    pub arn: String,
}

#[derive(Debug, Clone)]
pub struct PutEvent {
    pub event_id: String,
    pub source: String,
    pub detail_type: String,
    pub detail: String,
    pub event_bus_name: String,
    pub time: DateTime<Utc>,
}

pub struct EventBridgeState {
    pub account_id: String,
    pub region: String,
    pub buses: HashMap<String, EventBus>,  // name -> bus
    pub rules: HashMap<String, EventRule>, // name -> rule
    pub events: Vec<PutEvent>,
}

impl EventBridgeState {
    pub fn new(account_id: &str, region: &str) -> Self {
        let default_bus_arn = format!("arn:aws:events:{region}:{account_id}:event-bus/default");
        let mut buses = HashMap::new();
        buses.insert(
            "default".to_string(),
            EventBus {
                name: "default".to_string(),
                arn: default_bus_arn,
                tags: HashMap::new(),
            },
        );

        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            buses,
            rules: HashMap::new(),
            events: Vec::new(),
        }
    }
}

pub type SharedEventBridgeState = Arc<RwLock<EventBridgeState>>;
