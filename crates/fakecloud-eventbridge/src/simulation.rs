use std::sync::Arc;

use chrono::Utc;
use serde_json::json;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_lambda::SharedLambdaState;
use fakecloud_logs::SharedLogsState;

use crate::state::SharedEventBridgeState;

/// Result of firing a rule via simulation.
#[derive(Debug)]
pub struct FiredTarget {
    /// The target type (e.g. "sqs", "sns", "lambda", "logs").
    pub target_type: String,
    /// The target ARN.
    pub arn: String,
}

/// Borrowed context passed to `fire_rule` — all the surrounding state
/// it needs to deliver to the different target protocols. Bundled so
/// the callers don't have to thread five positional args through.
pub struct FireRuleContext<'a> {
    pub state: &'a SharedEventBridgeState,
    pub delivery: &'a Arc<DeliveryBus>,
    pub lambda_state: &'a Option<SharedLambdaState>,
    pub logs_state: &'a Option<SharedLogsState>,
    pub container_runtime: &'a Option<Arc<fakecloud_lambda::runtime::ContainerRuntime>>,
}

/// Fire a specific rule by bus name and rule name, delivering to all its
/// targets regardless of the rule's enabled/disabled state.
///
/// Returns `Ok(targets)` with the list of targets that were delivered to,
/// or `Err(message)` if the bus or rule doesn't exist.
pub fn fire_rule(
    ctx: &FireRuleContext<'_>,
    bus_name: &str,
    rule_name: &str,
) -> Result<Vec<FiredTarget>, String> {
    let state = ctx.state;
    let delivery = ctx.delivery;
    let lambda_state = ctx.lambda_state;
    let logs_state = ctx.logs_state;
    let container_runtime = ctx.container_runtime;

    let (targets, rule_arn, account_id, region) = {
        let eb_accounts = state.read();
        let eb_state = eb_accounts.default_ref();

        // Verify bus exists
        if !eb_state.buses.contains_key(bus_name) {
            return Err(format!("Event bus '{bus_name}' not found"));
        }

        let key = (bus_name.to_string(), rule_name.to_string());
        let rule = match eb_state.rules.get(&key) {
            Some(r) => r,
            None => return Err(format!("Rule '{rule_name}' not found on bus '{bus_name}'")),
        };

        (
            rule.targets.clone(),
            rule.arn.clone(),
            eb_state.account_id.clone(),
            eb_state.region.clone(),
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

    // Record the event in state
    {
        let mut s_accounts = state.write();
        let s = s_accounts.default_mut();
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

    // Deliver through the shared single-target dispatch so the simulation
    // endpoint honours exactly the same target handling as real PutEvents /
    // scheduler delivery — Input / InputPath / InputTransformer resolution and
    // the SQS(FIFO) / SNS / Lambda / Logs / Kinesis / StepFunctions /
    // api-destination / HTTP branches — instead of a reduced local copy.
    let ctx = crate::service::EventDispatchContext {
        state,
        delivery,
        lambda_state: lambda_state.as_ref(),
        logs_state: logs_state.as_ref(),
        container_runtime,
        account_id: &account_id,
        region: &region,
    };

    let mut fired = Vec::new();
    for target in &targets {
        crate::service::dispatch_event_target(
            &ctx,
            target,
            &event_json,
            &event_id,
            "Scheduled Event",
            Some(&rule_arn),
        );
        if let Some(target_type) = classify_target_type(&target.arn) {
            fired.push(FiredTarget {
                target_type,
                arn: target.arn.clone(),
            });
        }
    }

    Ok(fired)
}

/// Map a target ARN to the target-type label reported by the simulation
/// endpoint. Mirrors the branch selection in `dispatch_event_target`.
fn classify_target_type(arn: &str) -> Option<String> {
    let ty = if arn.contains(":sqs:") {
        "sqs"
    } else if arn.contains(":sns:") {
        "sns"
    } else if arn.contains(":lambda:") {
        "lambda"
    } else if arn.contains(":logs:") {
        "logs"
    } else if arn.contains(":kinesis:") {
        "kinesis"
    } else if arn.contains(":states:") {
        "stepfunctions"
    } else if arn.contains(":api-destination/") {
        "api-destination"
    } else if arn.starts_with("https://") || arn.starts_with("http://") {
        "http"
    } else {
        return None;
    };
    Some(ty.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{EventRule, EventTarget};
    use fakecloud_aws::arn::Arn;
    use parking_lot::RwLock;
    use std::collections::BTreeMap;

    fn make_state() -> SharedEventBridgeState {
        Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ))
    }

    fn add_rule(
        state: &SharedEventBridgeState,
        bus: &str,
        name: &str,
        enabled: bool,
        targets: Vec<EventTarget>,
    ) {
        let mut s_accounts = state.write();
        let s = s_accounts.default_mut();
        let key = (bus.to_string(), name.to_string());
        s.rules.insert(
            key,
            EventRule {
                name: name.to_string(),
                arn: Arn::new(
                    "events",
                    "us-east-1",
                    "123456789012",
                    &format!("rule/{bus}/{name}"),
                )
                .to_string(),
                event_bus_name: bus.to_string(),
                event_pattern: None,
                schedule_expression: Some("rate(1 minute)".to_string()),
                state: if enabled { "ENABLED" } else { "DISABLED" }.to_string(),
                description: None,
                role_arn: None,
                managed_by: None,
                created_by: None,
                targets,
                tags: BTreeMap::new(),
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
                ..Default::default()
            }],
        );

        let ctx = FireRuleContext {
            state: &state,
            delivery: &delivery,
            lambda_state: &None,
            logs_state: &None,
            container_runtime: &None,
        };
        let result = fire_rule(&ctx, "default", "my-rule");
        let targets = result.unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].target_type, "sqs");
        assert_eq!(
            targets[0].arn,
            "arn:aws:sqs:us-east-1:123456789012:target-queue"
        );

        // Verify event was recorded
        let s_accounts = state.read();
        let s = s_accounts.default_ref();
        assert!(s.events.iter().any(|e| e.source == "aws.events"));
    }

    #[test]
    fn fire_rule_nonexistent_rule() {
        let state = make_state();
        let delivery = Arc::new(DeliveryBus::new());

        let ctx = FireRuleContext {
            state: &state,
            delivery: &delivery,
            lambda_state: &None,
            logs_state: &None,
            container_runtime: &None,
        };
        let result = fire_rule(&ctx, "default", "no-such-rule");
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
                ..Default::default()
            }],
        );

        let ctx = FireRuleContext {
            state: &state,
            delivery: &delivery,
            lambda_state: &None,
            logs_state: &None,
            container_runtime: &None,
        };
        let result = fire_rule(&ctx, "default", "disabled-rule");
        // Simulation overrides disabled state
        let targets = result.unwrap();
        assert_eq!(targets.len(), 1);
    }

    #[test]
    fn fire_rule_unknown_bus_errors() {
        let state = make_state();
        let delivery = Arc::new(DeliveryBus::new());
        let ctx = FireRuleContext {
            state: &state,
            delivery: &delivery,
            lambda_state: &None,
            logs_state: &None,
            container_runtime: &None,
        };
        let err = fire_rule(&ctx, "missing-bus", "rule").unwrap_err();
        assert!(err.contains("missing-bus"));
    }

    #[test]
    fn fire_rule_no_targets_returns_empty() {
        let state = make_state();
        let delivery = Arc::new(DeliveryBus::new());
        add_rule(&state, "default", "no-targets", true, Vec::new());
        let ctx = FireRuleContext {
            state: &state,
            delivery: &delivery,
            lambda_state: &None,
            logs_state: &None,
            container_runtime: &None,
        };
        let targets = fire_rule(&ctx, "default", "no-targets").unwrap();
        assert!(targets.is_empty());
    }

    #[test]
    fn fire_rule_with_sns_and_lambda_and_logs_targets() {
        let state = make_state();
        let delivery = Arc::new(DeliveryBus::new());
        add_rule(
            &state,
            "default",
            "multi",
            true,
            vec![
                EventTarget {
                    id: "t-sns".to_string(),
                    arn: "arn:aws:sns:us-east-1:123456789012:topic".to_string(),
                    input: None,
                    input_path: None,
                    input_transformer: None,
                    sqs_parameters: None,
                    ..Default::default()
                },
                EventTarget {
                    id: "t-lambda".to_string(),
                    arn: "arn:aws:lambda:us-east-1:123456789012:function:F".to_string(),
                    input: None,
                    input_path: None,
                    input_transformer: None,
                    sqs_parameters: None,
                    ..Default::default()
                },
                EventTarget {
                    id: "t-logs".to_string(),
                    arn: "arn:aws:logs:us-east-1:123456789012:log-group:lg".to_string(),
                    input: None,
                    input_path: None,
                    input_transformer: None,
                    sqs_parameters: None,
                    ..Default::default()
                },
            ],
        );
        let ctx = FireRuleContext {
            state: &state,
            delivery: &delivery,
            lambda_state: &None,
            logs_state: &None,
            container_runtime: &None,
        };
        let fired = fire_rule(&ctx, "default", "multi").unwrap();
        let types: Vec<&str> = fired.iter().map(|t| t.target_type.as_str()).collect();
        assert!(types.contains(&"sns"));
        assert!(types.contains(&"lambda"));
        assert!(types.contains(&"logs"));
    }

    #[test]
    fn fire_rule_with_sqs_fifo_message_group() {
        let state = make_state();
        let delivery = Arc::new(DeliveryBus::new());
        add_rule(
            &state,
            "default",
            "fifo",
            true,
            vec![EventTarget {
                id: "t1".to_string(),
                arn: "arn:aws:sqs:us-east-1:123456789012:queue.fifo".to_string(),
                input: None,
                input_path: None,
                input_transformer: None,
                sqs_parameters: Some(json!({"MessageGroupId": "g1"})),
                ..Default::default()
            }],
        );
        let ctx = FireRuleContext {
            state: &state,
            delivery: &delivery,
            lambda_state: &None,
            logs_state: &None,
            container_runtime: &None,
        };
        let fired = fire_rule(&ctx, "default", "fifo").unwrap();
        assert_eq!(fired.len(), 1);
        assert_eq!(fired[0].target_type, "sqs");
    }

    #[test]
    fn fire_rule_constant_input_delivered_via_shared_dispatch() {
        // The simulation path now reuses dispatch_event_target, so a target
        // with a constant Input delivers that constant, and InputPath /
        // InputTransformer resolution is honoured identically to PutEvents.
        let state = make_state();
        let recorder = Arc::new(TestRecorder::default());
        let bus = Arc::new(DeliveryBus::new().with_sqs(recorder.clone()));
        add_rule(
            &state,
            "default",
            "constant",
            true,
            vec![EventTarget {
                id: "t1".to_string(),
                arn: "arn:aws:sqs:us-east-1:123456789012:q".to_string(),
                input: Some("{\"constant\":true}".to_string()),
                ..Default::default()
            }],
        );
        let ctx = FireRuleContext {
            state: &state,
            delivery: &bus,
            lambda_state: &None,
            logs_state: &None,
            container_runtime: &None,
        };
        let fired = fire_rule(&ctx, "default", "constant").unwrap();
        assert_eq!(fired.len(), 1);
        let calls = recorder.sqs.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1, "{\"constant\":true}");
    }

    #[derive(Default)]
    struct TestRecorder {
        sqs: std::sync::Mutex<Vec<(String, String)>>,
    }

    impl fakecloud_core::delivery::SqsDelivery for TestRecorder {
        fn deliver_to_queue(
            &self,
            arn: &str,
            body: &str,
            _attrs: &std::collections::HashMap<String, String>,
        ) {
            self.sqs
                .lock()
                .unwrap()
                .push((arn.to_string(), body.to_string()));
        }

        fn deliver_to_queue_with_attrs(
            &self,
            arn: &str,
            body: &str,
            _attrs: &std::collections::HashMap<
                String,
                fakecloud_core::delivery::SqsMessageAttribute,
            >,
            _group: Option<&str>,
            _dedup: Option<&str>,
        ) {
            self.sqs
                .lock()
                .unwrap()
                .push((arn.to_string(), body.to_string()));
        }
    }
}
