use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;

use fakecloud_core::delivery::{DeliveryBus, EventBridgeDelivery};

use crate::service::matches_pattern;
use crate::state::{PutEvent, SharedEventBridgeState};

/// Implements EventBridgeDelivery so other services (SES) can put events
/// on an EventBridge bus with full rule matching and target delivery.
pub struct EventBridgeDeliveryImpl {
    state: SharedEventBridgeState,
    delivery: Arc<DeliveryBus>,
}

impl EventBridgeDeliveryImpl {
    pub fn new(state: SharedEventBridgeState, delivery: Arc<DeliveryBus>) -> Self {
        Self { state, delivery }
    }
}

impl EventBridgeDelivery for EventBridgeDeliveryImpl {
    fn put_event(&self, source: &str, detail_type: &str, detail: &str, event_bus_name: &str) {
        let event_id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();

        let event = PutEvent {
            event_id: event_id.clone(),
            source: source.to_string(),
            detail_type: detail_type.to_string(),
            detail: detail.to_string(),
            event_bus_name: event_bus_name.to_string(),
            time: now,
            resources: Vec::new(),
        };

        let mut state = self.state.write();
        state.events.push(event);

        // Find matching rules and their targets
        let account_id = state.account_id.clone();
        let region = state.region.clone();
        let matching_targets: Vec<_> = state
            .rules
            .values()
            .filter(|r| {
                r.event_bus_name == event_bus_name
                    && r.state == "ENABLED"
                    && matches_pattern(
                        r.event_pattern.as_deref(),
                        source,
                        detail_type,
                        detail,
                        &account_id,
                        &region,
                        &[],
                    )
            })
            .flat_map(|r| r.targets.clone())
            .collect();

        // Drop the lock before delivering
        drop(state);

        if matching_targets.is_empty() {
            return;
        }

        // Build the EventBridge event envelope
        let detail_value: serde_json::Value =
            serde_json::from_str(detail).unwrap_or(serde_json::json!({}));
        let event_json = serde_json::json!({
            "version": "0",
            "id": event_id,
            "source": source,
            "account": account_id,
            "detail-type": detail_type,
            "detail": detail_value,
            "time": now.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "region": region,
            "resources": [],
        });
        let event_str = event_json.to_string();

        for target in matching_targets {
            let arn = &target.arn;
            if arn.contains(":sqs:") {
                self.delivery.send_to_sqs(arn, &event_str, &HashMap::new());
            } else if arn.contains(":sns:") {
                self.delivery
                    .publish_to_sns(arn, &event_str, Some(detail_type));
            }
            // Lambda and other targets could be added here
        }
    }
}
