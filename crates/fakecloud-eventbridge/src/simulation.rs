use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use serde_json::json;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_lambda::state::{LambdaInvocation, SharedLambdaState};
use fakecloud_logs::state::SharedLogsState;

use crate::state::{EventTarget, SharedEventBridgeState};

/// Result of firing a rule via simulation.
#[derive(Debug)]
pub struct FiredTarget {
    /// The target type (e.g. "sqs", "sns", "lambda", "logs").
    pub target_type: String,
    /// The target ARN.
    pub arn: String,
}

/// Fire a specific rule by bus name and rule name, delivering to all its
/// targets regardless of the rule's enabled/disabled state.
///
/// Returns `Ok(targets)` with the list of targets that were delivered to,
/// or `Err(message)` if the bus or rule doesn't exist.
pub fn fire_rule(
    state: &SharedEventBridgeState,
    delivery: &Arc<DeliveryBus>,
    lambda_state: &Option<SharedLambdaState>,
    logs_state: &Option<SharedLogsState>,
    container_runtime: &Option<Arc<fakecloud_lambda::runtime::ContainerRuntime>>,
    bus_name: &str,
    rule_name: &str,
) -> Result<Vec<FiredTarget>, String> {
    let (targets, account_id, region) = {
        let state = state.read();

        // Verify bus exists
        if !state.buses.contains_key(bus_name) {
            return Err(format!("Event bus '{bus_name}' not found"));
        }

        let key = (bus_name.to_string(), rule_name.to_string());
        let rule = match state.rules.get(&key) {
            Some(r) => r,
            None => return Err(format!("Rule '{rule_name}' not found on bus '{bus_name}'")),
        };

        (
            rule.targets.clone(),
            state.account_id.clone(),
            state.region.clone(),
        )
    };

    if targets.is_empty() {
        return Ok(Vec::new());
    }

    let now = Utc::now();
    let event_id = uuid::Uuid::new_v4().to_string();

    // Build the scheduled-event envelope (same shape as the real scheduler)
    let event_json = json!({
        "version": "0",
        "id": event_id,
        "source": "aws.events",
        "account": account_id,
        "detail-type": "Scheduled Event",
        "detail": {},
        "time": now.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        "region": region,
        "resources": [],
    });
    let event_str = event_json.to_string();

    // Record the event in state
    {
        let mut s = state.write();
        s.events.push(crate::state::PutEvent {
            event_id: event_id.clone(),
            source: "aws.events".to_string(),
            detail_type: "Scheduled Event".to_string(),
            detail: "{}".to_string(),
            event_bus_name: bus_name.to_string(),
            time: now,
            resources: Vec::new(),
        });
    }

    let mut fired = Vec::new();

    for target in &targets {
        let arn = &target.arn;
        let body_str = resolve_target_body(target, &event_json, &event_str);

        if arn.contains(":sqs:") {
            // Extract MessageGroupId from SqsParameters if present (required for FIFO queues)
            let message_group_id = target
                .sqs_parameters
                .as_ref()
                .and_then(|sp| sp["MessageGroupId"].as_str())
                .map(|s| s.to_string());

            if message_group_id.is_some() {
                delivery.send_to_sqs_with_attrs(
                    arn,
                    &body_str,
                    &HashMap::new(),
                    message_group_id.as_deref(),
                    None,
                );
            } else {
                delivery.send_to_sqs(arn, &body_str, &HashMap::new());
            }
            fired.push(FiredTarget {
                target_type: "sqs".to_string(),
                arn: arn.clone(),
            });
        } else if arn.contains(":sns:") {
            delivery.publish_to_sns(arn, &body_str, Some("Scheduled Event"));
            fired.push(FiredTarget {
                target_type: "sns".to_string(),
                arn: arn.clone(),
            });
        } else if arn.contains(":lambda:") {
            let mut s = state.write();
            s.lambda_invocations.push(crate::state::LambdaInvocation {
                function_arn: arn.clone(),
                payload: body_str.clone(),
                timestamp: now,
            });
            drop(s);
            if let Some(ref ls) = lambda_state {
                ls.write().invocations.push(LambdaInvocation {
                    function_arn: arn.clone(),
                    payload: body_str.clone(),
                    timestamp: now,
                    source: "aws:events".to_string(),
                });
            }
            crate::service::invoke_lambda_async(container_runtime, lambda_state, arn, &body_str);
            fired.push(FiredTarget {
                target_type: "lambda".to_string(),
                arn: arn.clone(),
            });
        } else if arn.contains(":logs:") {
            let mut s = state.write();
            s.log_deliveries.push(crate::state::LogDelivery {
                log_group_arn: arn.clone(),
                payload: body_str.clone(),
                timestamp: now,
            });
            drop(s);
            if let Some(ref log_state) = logs_state {
                crate::service::deliver_to_logs(log_state, arn, &body_str, now);
            }
            fired.push(FiredTarget {
                target_type: "logs".to_string(),
                arn: arn.clone(),
            });
        }
    }

    Ok(fired)
}

/// Compute the message body for a target, applying Input / InputPath if
/// present.
///
/// **Limitations**: `InputTransformer` is not yet implemented — if a target
/// has one configured, we fall through to the full event envelope. Real AWS
/// evaluates the `InputPathsMap` + `InputTemplate` to build the payload;
/// implementing that requires a JSONPath evaluator. `InputPath` supports
/// only the simple `$.field` case (single top-level key); deeper paths
/// fall back to the full event.
fn resolve_target_body(
    target: &EventTarget,
    event_json: &serde_json::Value,
    event_str: &str,
) -> String {
    if let Some(ref input) = target.input {
        return input.clone();
    }

    if let Some(ref input_path) = target.input_path {
        // Support simple top-level JSONPath like "$.detail"
        if let Some(key) = input_path.strip_prefix("$.") {
            if !key.contains('.') && !key.contains('[') {
                if let Some(val) = event_json.get(key) {
                    return val.to_string();
                }
            }
        }
    }

    // InputTransformer is not yet supported — fall through to full event.

    event_str.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{EventBridgeState, EventRule};
    use parking_lot::RwLock;

    fn make_state() -> SharedEventBridgeState {
        Arc::new(RwLock::new(EventBridgeState::new(
            "123456789012",
            "us-east-1",
        )))
    }

    fn add_rule(
        state: &SharedEventBridgeState,
        bus: &str,
        name: &str,
        enabled: bool,
        targets: Vec<EventTarget>,
    ) {
        let mut s = state.write();
        let key = (bus.to_string(), name.to_string());
        s.rules.insert(
            key,
            EventRule {
                name: name.to_string(),
                arn: format!("arn:aws:events:us-east-1:123456789012:rule/{bus}/{name}"),
                event_bus_name: bus.to_string(),
                event_pattern: None,
                schedule_expression: Some("rate(1 minute)".to_string()),
                state: if enabled { "ENABLED" } else { "DISABLED" }.to_string(),
                description: None,
                role_arn: None,
                managed_by: None,
                created_by: None,
                targets,
                tags: HashMap::new(),
                last_fired: None,
            },
        );
    }

    #[test]
    fn fire_rule_with_valid_rule() {
        let state = make_state();
        let delivery = Arc::new(DeliveryBus::new());

        add_rule(
            &state,
            "default",
            "my-rule",
            true,
            vec![EventTarget {
                id: "t1".to_string(),
                arn: "arn:aws:sqs:us-east-1:123456789012:target-queue".to_string(),
                input: None,
                input_path: None,
                input_transformer: None,
                sqs_parameters: None,
            }],
        );

        let result = fire_rule(&state, &delivery, &None, &None, &None, "default", "my-rule");
        let targets = result.unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].target_type, "sqs");
        assert_eq!(
            targets[0].arn,
            "arn:aws:sqs:us-east-1:123456789012:target-queue"
        );

        // Verify event was recorded
        let s = state.read();
        assert!(s.events.iter().any(|e| e.source == "aws.events"));
    }

    #[test]
    fn fire_rule_nonexistent_rule() {
        let state = make_state();
        let delivery = Arc::new(DeliveryBus::new());

        let result = fire_rule(
            &state,
            &delivery,
            &None,
            &None,
            &None,
            "default",
            "no-such-rule",
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found"));
    }

    #[test]
    fn fire_rule_disabled_still_fires() {
        let state = make_state();
        let delivery = Arc::new(DeliveryBus::new());

        add_rule(
            &state,
            "default",
            "disabled-rule",
            false, // DISABLED
            vec![EventTarget {
                id: "t1".to_string(),
                arn: "arn:aws:sqs:us-east-1:123456789012:target-queue".to_string(),
                input: None,
                input_path: None,
                input_transformer: None,
                sqs_parameters: None,
            }],
        );

        let result = fire_rule(
            &state,
            &delivery,
            &None,
            &None,
            &None,
            "default",
            "disabled-rule",
        );
        // Simulation overrides disabled state
        let targets = result.unwrap();
        assert_eq!(targets.len(), 1);
    }
}
