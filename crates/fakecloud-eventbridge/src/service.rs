use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::StatusCode;
use serde_json::{json, Value};

use std::collections::HashMap;
use std::sync::Arc;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{EventBus, EventRule, EventTarget, PutEvent, SharedEventBridgeState};

pub struct EventBridgeService {
    state: SharedEventBridgeState,
    delivery: Arc<DeliveryBus>,
}

impl EventBridgeService {
    pub fn new(state: SharedEventBridgeState, delivery: Arc<DeliveryBus>) -> Self {
        Self { state, delivery }
    }
}

#[async_trait]
impl AwsService for EventBridgeService {
    fn service_name(&self) -> &str {
        "events"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match req.action.as_str() {
            "CreateEventBus" => self.create_event_bus(&req),
            "DeleteEventBus" => self.delete_event_bus(&req),
            "ListEventBuses" => self.list_event_buses(&req),
            "DescribeEventBus" => self.describe_event_bus(&req),
            "PutRule" => self.put_rule(&req),
            "DeleteRule" => self.delete_rule(&req),
            "ListRules" => self.list_rules(&req),
            "DescribeRule" => self.describe_rule(&req),
            "PutTargets" => self.put_targets(&req),
            "RemoveTargets" => self.remove_targets(&req),
            "ListTargetsByRule" => self.list_targets_by_rule(&req),
            "PutEvents" => self.put_events(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            _ => Err(AwsServiceError::action_not_implemented(
                "events",
                &req.action,
            )),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "CreateEventBus",
            "DeleteEventBus",
            "ListEventBuses",
            "DescribeEventBus",
            "PutRule",
            "DeleteRule",
            "ListRules",
            "DescribeRule",
            "PutTargets",
            "RemoveTargets",
            "ListTargetsByRule",
            "PutEvents",
            "TagResource",
            "UntagResource",
            "ListTagsForResource",
        ]
    }
}

fn parse_body(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Object(Default::default()))
}

fn json_resp(body: Value) -> AwsResponse {
    AwsResponse::json(StatusCode::OK, serde_json::to_string(&body).unwrap())
}

impl EventBridgeService {
    fn create_event_bus(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();

        let mut state = self.state.write();

        if state.buses.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceAlreadyExistsException",
                format!("Event bus {name} already exists."),
            ));
        }

        let arn = format!(
            "arn:aws:events:{}:{}:event-bus/{}",
            state.region, state.account_id, name
        );
        let bus = EventBus {
            name: name.clone(),
            arn: arn.clone(),
            tags: HashMap::new(),
        };
        state.buses.insert(name, bus);

        Ok(json_resp(json!({ "EventBusArn": arn })))
    }

    fn delete_event_bus(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        if name == "default" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Cannot delete the default event bus.",
            ));
        }

        let mut state = self.state.write();
        state.buses.remove(name);
        state.rules.retain(|_, r| r.event_bus_name != name);

        Ok(json_resp(json!({})))
    }

    fn list_event_buses(&self, _req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let buses: Vec<Value> = state
            .buses
            .values()
            .map(|b| json!({ "Name": b.name, "Arn": b.arn }))
            .collect();

        Ok(json_resp(json!({ "EventBuses": buses })))
    }

    fn describe_event_bus(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().unwrap_or("default");

        let state = self.state.read();
        let bus = state.buses.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Event bus {name} does not exist."),
            )
        })?;

        Ok(json_resp(json!({
            "Name": bus.name,
            "Arn": bus.arn,
        })))
    }

    fn put_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        let event_bus_name = body["EventBusName"]
            .as_str()
            .unwrap_or("default")
            .to_string();
        let event_pattern = body["EventPattern"].as_str().map(|s| s.to_string());
        let schedule_expression = body["ScheduleExpression"].as_str().map(|s| s.to_string());
        let description = body["Description"].as_str().map(|s| s.to_string());
        let rule_state = body["State"].as_str().unwrap_or("ENABLED").to_string();

        let mut state = self.state.write();

        if !state.buses.contains_key(&event_bus_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Event bus {event_bus_name} does not exist."),
            ));
        }

        let arn = format!(
            "arn:aws:events:{}:{}:rule/{}/{}",
            state.region, state.account_id, event_bus_name, name
        );

        let targets = state
            .rules
            .get(&name)
            .map(|r| r.targets.clone())
            .unwrap_or_default();

        let rule = EventRule {
            name: name.clone(),
            arn: arn.clone(),
            event_bus_name,
            event_pattern,
            schedule_expression,
            state: rule_state,
            description,
            targets,
            tags: HashMap::new(),
        };

        state.rules.insert(name, rule);
        Ok(json_resp(json!({ "RuleArn": arn })))
    }

    fn delete_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        self.state.write().rules.remove(name);
        Ok(json_resp(json!({})))
    }

    fn list_rules(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let event_bus_name = body["EventBusName"].as_str().unwrap_or("default");

        let state = self.state.read();
        let rules: Vec<Value> = state
            .rules
            .values()
            .filter(|r| r.event_bus_name == event_bus_name)
            .map(|r| {
                json!({
                    "Name": r.name,
                    "Arn": r.arn,
                    "EventBusName": r.event_bus_name,
                    "State": r.state,
                    "Description": r.description,
                    "EventPattern": r.event_pattern,
                    "ScheduleExpression": r.schedule_expression,
                })
            })
            .collect();

        Ok(json_resp(json!({ "Rules": rules })))
    }

    fn describe_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let state = self.state.read();
        let rule = state.rules.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Rule {name} does not exist."),
            )
        })?;

        Ok(json_resp(json!({
            "Name": rule.name,
            "Arn": rule.arn,
            "EventBusName": rule.event_bus_name,
            "State": rule.state,
            "Description": rule.description,
            "EventPattern": rule.event_pattern,
            "ScheduleExpression": rule.schedule_expression,
        })))
    }

    fn put_targets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let rule_name = body["Rule"].as_str().ok_or_else(|| missing("Rule"))?;
        let targets = body["Targets"]
            .as_array()
            .ok_or_else(|| missing("Targets"))?;

        let mut state = self.state.write();
        let rule = state.rules.get_mut(rule_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Rule {rule_name} does not exist."),
            )
        })?;

        for target in targets {
            let id = target["Id"].as_str().unwrap_or("").to_string();
            let arn = target["Arn"].as_str().unwrap_or("").to_string();
            // Remove existing target with same ID
            rule.targets.retain(|t| t.id != id);
            rule.targets.push(EventTarget { id, arn });
        }

        Ok(json_resp(json!({
            "FailedEntryCount": 0,
            "FailedEntries": [],
        })))
    }

    fn remove_targets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let rule_name = body["Rule"].as_str().ok_or_else(|| missing("Rule"))?;
        let ids = body["Ids"].as_array().ok_or_else(|| missing("Ids"))?;

        let target_ids: Vec<String> = ids
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();

        let mut state = self.state.write();
        let rule = state.rules.get_mut(rule_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Rule {rule_name} does not exist."),
            )
        })?;

        rule.targets.retain(|t| !target_ids.contains(&t.id));

        Ok(json_resp(json!({
            "FailedEntryCount": 0,
            "FailedEntries": [],
        })))
    }

    fn list_targets_by_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let rule_name = body["Rule"].as_str().ok_or_else(|| missing("Rule"))?;

        let state = self.state.read();
        let rule = state.rules.get(rule_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Rule {rule_name} does not exist."),
            )
        })?;

        let targets: Vec<Value> = rule
            .targets
            .iter()
            .map(|t| json!({ "Id": t.id, "Arn": t.arn }))
            .collect();

        Ok(json_resp(json!({ "Targets": targets })))
    }

    fn put_events(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let entries = body["Entries"]
            .as_array()
            .ok_or_else(|| missing("Entries"))?;

        let mut state = self.state.write();
        let mut result_entries = Vec::new();
        let mut events_to_deliver = Vec::new();

        for entry in entries {
            let event_id = uuid::Uuid::new_v4().to_string();
            let source = entry["Source"].as_str().unwrap_or("").to_string();
            let detail_type = entry["DetailType"].as_str().unwrap_or("").to_string();
            let detail = entry["Detail"].as_str().unwrap_or("{}").to_string();
            let event_bus_name = entry["EventBusName"]
                .as_str()
                .unwrap_or("default")
                .to_string();
            let time = entry["Time"]
                .as_str()
                .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.with_timezone(&Utc))
                .unwrap_or_else(Utc::now);

            let event = PutEvent {
                event_id: event_id.clone(),
                source: source.clone(),
                detail_type: detail_type.clone(),
                detail: detail.clone(),
                event_bus_name: event_bus_name.clone(),
                time,
            };
            state.events.push(event);

            // Find matching rules and their targets
            let matching_targets: Vec<EventTarget> = state
                .rules
                .values()
                .filter(|r| {
                    r.event_bus_name == event_bus_name
                        && r.state == "ENABLED"
                        && matches_pattern(
                            r.event_pattern.as_deref(),
                            &source,
                            &detail_type,
                            &detail,
                        )
                })
                .flat_map(|r| r.targets.clone())
                .collect();

            if !matching_targets.is_empty() {
                events_to_deliver.push((
                    event_id.clone(),
                    source,
                    detail_type,
                    detail,
                    time,
                    matching_targets,
                ));
            }

            result_entries.push(json!({ "EventId": event_id }));
        }

        // Drop the lock before delivering
        drop(state);

        // Deliver to targets
        for (event_id, source, detail_type, detail, time, targets) in events_to_deliver {
            let event_json = json!({
                "version": "0",
                "id": event_id,
                "source": source,
                "detail-type": detail_type,
                "detail": serde_json::from_str::<Value>(&detail).unwrap_or(json!({})),
                "time": time.to_rfc3339(),
                "region": "us-east-1",
            });
            let event_str = event_json.to_string();

            for target in targets {
                let arn = &target.arn;
                if arn.contains(":sqs:") {
                    self.delivery
                        .send_to_sqs(arn, &event_str, &std::collections::HashMap::new());
                } else if arn.contains(":sns:") {
                    self.delivery
                        .publish_to_sns(arn, &event_str, Some(&detail_type));
                }
            }
        }

        Ok(json_resp(json!({
            "FailedEntryCount": 0,
            "Entries": result_entries,
        })))
    }

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let arn = body["ResourceARN"]
            .as_str()
            .ok_or_else(|| missing("ResourceARN"))?;
        let tags = body["Tags"].as_array().ok_or_else(|| missing("Tags"))?;

        let mut state = self.state.write();

        // Find resource by ARN (bus or rule)
        let tag_map = find_tags_mut(&mut state, arn)?;

        for tag in tags {
            if let (Some(key), Some(val)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                tag_map.insert(key.to_string(), val.to_string());
            }
        }

        Ok(json_resp(json!({})))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let arn = body["ResourceARN"]
            .as_str()
            .ok_or_else(|| missing("ResourceARN"))?;
        let tag_keys = body["TagKeys"]
            .as_array()
            .ok_or_else(|| missing("TagKeys"))?;

        let mut state = self.state.write();
        let tag_map = find_tags_mut(&mut state, arn)?;

        for key in tag_keys {
            if let Some(k) = key.as_str() {
                tag_map.remove(k);
            }
        }

        Ok(json_resp(json!({})))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let arn = body["ResourceARN"]
            .as_str()
            .ok_or_else(|| missing("ResourceARN"))?;

        let state = self.state.read();
        let tag_map = find_tags(&state, arn)?;

        let tags: Vec<Value> = tag_map
            .iter()
            .map(|(k, v)| json!({"Key": k, "Value": v}))
            .collect();

        Ok(json_resp(json!({ "Tags": tags })))
    }
}

/// Find the tags map for a resource by ARN (mutable).
fn find_tags_mut<'a>(
    state: &'a mut crate::state::EventBridgeState,
    arn: &str,
) -> Result<&'a mut HashMap<String, String>, AwsServiceError> {
    // Check buses
    for bus in state.buses.values_mut() {
        if bus.arn == arn {
            return Ok(&mut bus.tags);
        }
    }
    // Check rules
    for rule in state.rules.values_mut() {
        if rule.arn == arn {
            return Ok(&mut rule.tags);
        }
    }
    Err(AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundException",
        format!("Resource {arn} not found."),
    ))
}

/// Find the tags map for a resource by ARN (immutable).
fn find_tags<'a>(
    state: &'a crate::state::EventBridgeState,
    arn: &str,
) -> Result<&'a HashMap<String, String>, AwsServiceError> {
    for bus in state.buses.values() {
        if bus.arn == arn {
            return Ok(&bus.tags);
        }
    }
    for rule in state.rules.values() {
        if rule.arn == arn {
            return Ok(&rule.tags);
        }
    }
    Err(AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundException",
        format!("Resource {arn} not found."),
    ))
}

/// Match an event against an EventBridge event pattern.
/// Supports matching on source, detail-type, detail fields (with nested matching),
/// and advanced matchers: prefix, exists, anything-but.
fn matches_pattern(
    pattern_json: Option<&str>,
    source: &str,
    detail_type: &str,
    detail: &str,
) -> bool {
    let pattern_json = match pattern_json {
        Some(p) => p,
        None => return true, // No pattern = match everything (schedule rules)
    };

    let pattern: Value = match serde_json::from_str(pattern_json) {
        Ok(v) => v,
        Err(_) => return false,
    };

    let pattern_obj = match pattern.as_object() {
        Some(o) => o,
        None => return false,
    };

    // Build the event as a JSON object for unified matching
    let detail_value: Value = serde_json::from_str(detail).unwrap_or(json!({}));
    let event = json!({
        "source": source,
        "detail-type": detail_type,
        "detail": detail_value,
    });

    // Each top-level key in the pattern must match
    for (key, pattern_value) in pattern_obj {
        let event_value = &event[key];
        if !matches_value(pattern_value, event_value) {
            return false;
        }
    }

    true
}

/// Recursively match a pattern node against an event value.
///
/// Pattern nodes can be:
/// - An object: each key must match recursively against the corresponding key in the event value
/// - An array: the event value must match at least one element (OR semantics).
///   Array elements can be plain values (exact match) or matcher objects (prefix, exists, anything-but).
fn matches_value(pattern: &Value, event_value: &Value) -> bool {
    match pattern {
        Value::Object(obj) => {
            // This is a nested pattern object - each key must match recursively
            for (key, sub_pattern) in obj {
                let sub_value = &event_value[key];
                if !matches_value(sub_pattern, sub_value) {
                    return false;
                }
            }
            true
        }
        Value::Array(arr) => {
            // The event value must match at least one element in the array
            arr.iter().any(|elem| matches_single(elem, event_value))
        }
        _ => false,
    }
}

/// Match a single pattern element against an event value.
/// The element can be a plain value (exact match) or a matcher object.
fn matches_single(pattern_elem: &Value, event_value: &Value) -> bool {
    match pattern_elem {
        Value::Object(obj) => {
            // Matcher object: prefix, exists, anything-but
            if let Some(prefix_val) = obj.get("prefix") {
                if let (Some(prefix), Some(actual)) = (prefix_val.as_str(), event_value.as_str()) {
                    return actual.starts_with(prefix);
                }
                return false;
            }
            if let Some(exists_val) = obj.get("exists") {
                let should_exist = exists_val.as_bool().unwrap_or(true);
                let does_exist = !event_value.is_null();
                return should_exist == does_exist;
            }
            if let Some(anything_but_val) = obj.get("anything-but") {
                return match anything_but_val {
                    Value::String(s) => event_value.as_str() != Some(s.as_str()),
                    Value::Array(arr) => !arr.iter().any(|v| values_equal(v, event_value)),
                    Value::Number(_) => event_value != anything_but_val,
                    _ => true,
                };
            }
            if let Some(numeric_val) = obj.get("numeric") {
                return matches_numeric(numeric_val, event_value);
            }
            // Unknown matcher, no match
            false
        }
        // Plain value: exact match (string, number, bool)
        _ => values_equal(pattern_elem, event_value),
    }
}

/// Match a numeric pattern array against an event value.
/// The array contains pairs of (operator, value): e.g. [">", 100] or [">=", 50, "<", 200].
fn matches_numeric(numeric_arr: &Value, event_value: &Value) -> bool {
    let arr = match numeric_arr.as_array() {
        Some(a) => a,
        None => return false,
    };
    let actual = match event_value.as_f64() {
        Some(n) => n,
        None => return false,
    };
    // Process pairs of (operator, value)
    let mut i = 0;
    while i + 1 < arr.len() {
        let op = match arr[i].as_str() {
            Some(s) => s,
            None => return false,
        };
        let threshold = match arr[i + 1].as_f64() {
            Some(n) => n,
            None => return false,
        };
        let ok = match op {
            ">" => actual > threshold,
            ">=" => actual >= threshold,
            "<" => actual < threshold,
            "<=" => actual <= threshold,
            "=" => (actual - threshold).abs() < f64::EPSILON,
            _ => return false,
        };
        if !ok {
            return false;
        }
        i += 2;
    }
    true
}

/// Compare two JSON values for equality (used for exact matching).
fn values_equal(a: &Value, b: &Value) -> bool {
    // For string comparison: pattern "foo" should match event value "foo"
    // serde_json Value equality handles this correctly
    a == b
}

fn missing(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        format!("The request must contain the parameter {name}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pattern_matches_source() {
        assert!(matches_pattern(
            Some(r#"{"source": ["my.app"]}"#),
            "my.app",
            "OrderPlaced",
            "{}"
        ));
        assert!(!matches_pattern(
            Some(r#"{"source": ["other.app"]}"#),
            "my.app",
            "OrderPlaced",
            "{}"
        ));
    }

    #[test]
    fn pattern_matches_detail_type() {
        assert!(matches_pattern(
            Some(r#"{"detail-type": ["OrderPlaced"]}"#),
            "my.app",
            "OrderPlaced",
            "{}"
        ));
        assert!(!matches_pattern(
            Some(r#"{"detail-type": ["OrderShipped"]}"#),
            "my.app",
            "OrderPlaced",
            "{}"
        ));
    }

    #[test]
    fn pattern_matches_detail_field() {
        assert!(matches_pattern(
            Some(r#"{"detail": {"status": ["ACTIVE"]}}"#),
            "my.app",
            "StatusChange",
            r#"{"status": "ACTIVE"}"#
        ));
        assert!(!matches_pattern(
            Some(r#"{"detail": {"status": ["ACTIVE"]}}"#),
            "my.app",
            "StatusChange",
            r#"{"status": "INACTIVE"}"#
        ));
    }

    #[test]
    fn no_pattern_matches_everything() {
        assert!(matches_pattern(None, "any", "any", "{}"));
    }

    #[test]
    fn combined_pattern() {
        let pattern = r#"{"source": ["orders"], "detail-type": ["OrderPlaced"]}"#;
        assert!(matches_pattern(
            Some(pattern),
            "orders",
            "OrderPlaced",
            "{}"
        ));
        assert!(!matches_pattern(
            Some(pattern),
            "orders",
            "OrderShipped",
            "{}"
        ));
        assert!(!matches_pattern(
            Some(pattern),
            "other",
            "OrderPlaced",
            "{}"
        ));
    }

    #[test]
    fn nested_detail_pattern() {
        // Nested object matching: {"detail": {"order": {"status": ["PLACED"]}}}
        let pattern = r#"{"detail": {"order": {"status": ["PLACED"]}}}"#;
        assert!(matches_pattern(
            Some(pattern),
            "my.app",
            "OrderEvent",
            r#"{"order": {"status": "PLACED", "id": "123"}}"#
        ));
        assert!(!matches_pattern(
            Some(pattern),
            "my.app",
            "OrderEvent",
            r#"{"order": {"status": "SHIPPED", "id": "123"}}"#
        ));
        // Missing nested field
        assert!(!matches_pattern(
            Some(pattern),
            "my.app",
            "OrderEvent",
            r#"{"order": {"id": "123"}}"#
        ));
    }

    #[test]
    fn deeply_nested_detail_pattern() {
        let pattern = r#"{"detail": {"a": {"b": {"c": ["deep"]}}}}"#;
        assert!(matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"a": {"b": {"c": "deep"}}}"#
        ));
        assert!(!matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"a": {"b": {"c": "shallow"}}}"#
        ));
    }

    #[test]
    fn prefix_matcher() {
        let pattern = r#"{"source": [{"prefix": "com.myapp"}]}"#;
        assert!(matches_pattern(
            Some(pattern),
            "com.myapp.orders",
            "OrderPlaced",
            "{}"
        ));
        assert!(matches_pattern(
            Some(pattern),
            "com.myapp",
            "OrderPlaced",
            "{}"
        ));
        assert!(!matches_pattern(
            Some(pattern),
            "com.other",
            "OrderPlaced",
            "{}"
        ));
    }

    #[test]
    fn prefix_matcher_in_detail() {
        let pattern = r#"{"detail": {"region": [{"prefix": "us-"}]}}"#;
        assert!(matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"region": "us-east-1"}"#
        ));
        assert!(!matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"region": "eu-west-1"}"#
        ));
    }

    #[test]
    fn exists_matcher() {
        // exists: true - field must be present
        let pattern = r#"{"detail": {"error": [{"exists": true}]}}"#;
        assert!(matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"error": "something broke"}"#
        ));
        assert!(!matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"status": "ok"}"#
        ));

        // exists: false - field must NOT be present
        let pattern = r#"{"detail": {"error": [{"exists": false}]}}"#;
        assert!(matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"status": "ok"}"#
        ));
        assert!(!matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"error": "something broke"}"#
        ));
    }

    #[test]
    fn anything_but_matcher() {
        // Single value
        let pattern = r#"{"source": [{"anything-but": "internal"}]}"#;
        assert!(matches_pattern(Some(pattern), "external", "Event", "{}"));
        assert!(!matches_pattern(Some(pattern), "internal", "Event", "{}"));

        // Array of values
        let pattern = r#"{"source": [{"anything-but": ["internal", "test"]}]}"#;
        assert!(matches_pattern(Some(pattern), "external", "Event", "{}"));
        assert!(!matches_pattern(Some(pattern), "internal", "Event", "{}"));
        assert!(!matches_pattern(Some(pattern), "test", "Event", "{}"));
    }

    #[test]
    fn anything_but_in_detail() {
        let pattern = r#"{"detail": {"env": [{"anything-but": "prod"}]}}"#;
        assert!(matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"env": "staging"}"#
        ));
        assert!(!matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"env": "prod"}"#
        ));
    }

    #[test]
    fn numeric_greater_than() {
        let pattern = r#"{"detail": {"count": [{"numeric": [">", 100]}]}}"#;
        assert!(matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 150}"#
        ));
        assert!(!matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 100}"#
        ));
        assert!(!matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 50}"#
        ));
    }

    #[test]
    fn numeric_less_than() {
        let pattern = r#"{"detail": {"count": [{"numeric": ["<", 10]}]}}"#;
        assert!(matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 5}"#
        ));
        assert!(!matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 10}"#
        ));
        assert!(!matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 15}"#
        ));
    }

    #[test]
    fn numeric_range() {
        let pattern = r#"{"detail": {"count": [{"numeric": [">=", 50, "<", 200]}]}}"#;
        assert!(matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 50}"#
        ));
        assert!(matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 100}"#
        ));
        assert!(!matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 200}"#
        ));
        assert!(!matches_pattern(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 49}"#
        ));
    }

    #[test]
    fn mixed_matchers_and_literals() {
        // Mix of literal match and prefix match in same array (OR semantics)
        let pattern = r#"{"source": ["exact.match", {"prefix": "com.myapp"}]}"#;
        assert!(matches_pattern(Some(pattern), "exact.match", "Event", "{}"));
        assert!(matches_pattern(
            Some(pattern),
            "com.myapp.orders",
            "Event",
            "{}"
        ));
        assert!(!matches_pattern(
            Some(pattern),
            "other.source",
            "Event",
            "{}"
        ));
    }
}
