use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::StatusCode;
use serde_json::{json, Value};

use std::collections::HashMap;
use std::sync::Arc;

use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{
    ApiDestination, Archive, Connection, EventBus, EventRule, EventTarget, PutEvent, Replay,
    SharedEventBridgeState,
};

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
            "EnableRule" => self.enable_rule(&req),
            "DisableRule" => self.disable_rule(&req),
            "PutTargets" => self.put_targets(&req),
            "RemoveTargets" => self.remove_targets(&req),
            "ListTargetsByRule" => self.list_targets_by_rule(&req),
            "ListRuleNamesByTarget" => self.list_rule_names_by_target(&req),
            "PutEvents" => self.put_events(&req),
            "PutPermission" => self.put_permission(&req),
            "RemovePermission" => self.remove_permission(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "CreateArchive" => self.create_archive(&req),
            "DescribeArchive" => self.describe_archive(&req),
            "ListArchives" => self.list_archives(&req),
            "UpdateArchive" => self.update_archive(&req),
            "DeleteArchive" => self.delete_archive(&req),
            "CreateConnection" => self.create_connection(&req),
            "DescribeConnection" => self.describe_connection(&req),
            "ListConnections" => self.list_connections(&req),
            "UpdateConnection" => self.update_connection(&req),
            "DeleteConnection" => self.delete_connection(&req),
            "CreateApiDestination" => self.create_api_destination(&req),
            "DescribeApiDestination" => self.describe_api_destination(&req),
            "ListApiDestinations" => self.list_api_destinations(&req),
            "UpdateApiDestination" => self.update_api_destination(&req),
            "DeleteApiDestination" => self.delete_api_destination(&req),
            "StartReplay" => self.start_replay(&req),
            "DescribeReplay" => self.describe_replay(&req),
            "ListReplays" => self.list_replays(&req),
            "CancelReplay" => self.cancel_replay(&req),
            "CreatePartnerEventSource" => self.create_partner_event_source(&req),
            "DescribePartnerEventSource" => self.describe_partner_event_source(&req),
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
            "EnableRule",
            "DisableRule",
            "PutTargets",
            "RemoveTargets",
            "ListTargetsByRule",
            "ListRuleNamesByTarget",
            "PutEvents",
            "PutPermission",
            "RemovePermission",
            "TagResource",
            "UntagResource",
            "ListTagsForResource",
            "CreateArchive",
            "DescribeArchive",
            "ListArchives",
            "UpdateArchive",
            "DeleteArchive",
            "CreateConnection",
            "DescribeConnection",
            "ListConnections",
            "UpdateConnection",
            "DeleteConnection",
            "CreateApiDestination",
            "DescribeApiDestination",
            "ListApiDestinations",
            "UpdateApiDestination",
            "DeleteApiDestination",
            "StartReplay",
            "DescribeReplay",
            "ListReplays",
            "CancelReplay",
            "CreatePartnerEventSource",
            "DescribePartnerEventSource",
        ]
    }
}

fn parse_body(req: &AwsRequest) -> Value {
    serde_json::from_slice(&req.body).unwrap_or(Value::Object(Default::default()))
}

fn json_resp(body: Value) -> AwsResponse {
    AwsResponse::json(StatusCode::OK, serde_json::to_string(&body).unwrap())
}

fn parse_tags(body: &Value) -> HashMap<String, String> {
    let mut tags = HashMap::new();
    if let Some(arr) = body["Tags"].as_array() {
        for tag in arr {
            if let (Some(key), Some(val)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                tags.insert(key.to_string(), val.to_string());
            }
        }
    }
    tags
}

fn parse_target(target: &Value) -> EventTarget {
    EventTarget {
        id: target["Id"].as_str().unwrap_or("").to_string(),
        arn: target["Arn"].as_str().unwrap_or("").to_string(),
        input: target["Input"].as_str().map(|s| s.to_string()),
        input_path: target["InputPath"].as_str().map(|s| s.to_string()),
        input_transformer: target.get("InputTransformer").cloned(),
        sqs_parameters: target.get("SqsParameters").cloned(),
    }
}

fn target_to_json(t: &EventTarget) -> Value {
    let mut obj = json!({ "Id": t.id, "Arn": t.arn });
    if let Some(ref input) = t.input {
        obj["Input"] = json!(input);
    }
    if let Some(ref input_path) = t.input_path {
        obj["InputPath"] = json!(input_path);
    }
    if let Some(ref it) = t.input_transformer {
        obj["InputTransformer"] = it.clone();
    }
    if let Some(ref sp) = t.sqs_parameters {
        obj["SqsParameters"] = sp.clone();
    }
    obj
}

// ─── Event Bus Operations ───────────────────────────────────────────
impl EventBridgeService {
    fn create_event_bus(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();

        // Validate name doesn't contain '/' (unless partner bus)
        if name.contains('/') && !name.starts_with("aws.partner/") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Event bus name must not contain '/'.",
            ));
        }

        // Partner event bus validation
        if name.starts_with("aws.partner/") {
            let event_source = body["EventSourceName"].as_str().unwrap_or("");
            let state_r = self.state.read();
            let has_source = state_r.partner_event_sources.contains_key(event_source);
            drop(state_r);
            if !has_source {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Event source {event_source} does not exist."),
                ));
            }
        }

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
            req.region, state.account_id, name
        );
        let now = Utc::now();
        let description = body["Description"].as_str().map(|s| s.to_string());
        let kms_key_identifier = body["KmsKeyIdentifier"].as_str().map(|s| s.to_string());
        let dead_letter_config = body.get("DeadLetterConfig").cloned();

        let tags = parse_tags(&body);

        let bus = EventBus {
            name: name.clone(),
            arn: arn.clone(),
            tags,
            policy: None,
            description,
            kms_key_identifier,
            dead_letter_config,
            creation_time: now,
            last_modified_time: now,
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
                format!("Cannot delete event bus {name}."),
            ));
        }

        let mut state = self.state.write();
        state.buses.remove(name);
        state.rules.retain(|k, _| k.0 != name);

        Ok(json_resp(json!({})))
    }

    fn list_event_buses(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name_prefix = body["NamePrefix"].as_str();

        let state = self.state.read();
        let buses: Vec<Value> = state
            .buses
            .values()
            .filter(|b| match name_prefix {
                Some(prefix) => b.name.starts_with(prefix),
                None => true,
            })
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
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Event bus {name} does not exist."),
            )
        })?;

        let mut resp = json!({
            "Name": bus.name,
            "Arn": bus.arn,
            "CreationTime": bus.creation_time.timestamp() as f64,
            "LastModifiedTime": bus.last_modified_time.timestamp() as f64,
        });

        if let Some(ref policy) = bus.policy {
            resp["Policy"] = Value::String(serde_json::to_string(policy).unwrap());
        }
        if let Some(ref desc) = bus.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref kms) = bus.kms_key_identifier {
            resp["KmsKeyIdentifier"] = json!(kms);
        }
        if let Some(ref dlc) = bus.dead_letter_config {
            resp["DeadLetterConfig"] = dlc.clone();
        }

        Ok(json_resp(resp))
    }

    // ─── Permission Operations ──────────────────────────────────────────

    fn put_permission(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let event_bus_name = body["EventBusName"].as_str().unwrap_or("default");

        let mut state = self.state.write();

        let bus = state.buses.get_mut(event_bus_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Event bus {event_bus_name} does not exist."),
            )
        })?;

        // Check if Policy is provided (new-style)
        if let Some(policy_str) = body["Policy"].as_str() {
            if let Ok(policy) = serde_json::from_str::<Value>(policy_str) {
                bus.policy = Some(policy);
                return Ok(json_resp(json!({})));
            }
        }

        // Old-style: Action, Principal, StatementId
        let action = body["Action"].as_str().unwrap_or("");
        let principal = body["Principal"].as_str().unwrap_or("");
        let statement_id = body["StatementId"].as_str().unwrap_or("");

        // Validate action
        if action != "events:PutEvents" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Provided value in parameter 'action' is not supported.",
            ));
        }

        let statement = json!({
            "Sid": statement_id,
            "Effect": "Allow",
            "Principal": { "AWS": format!("arn:aws:iam::{principal}:root") },
            "Action": action,
            "Resource": bus.arn,
        });

        let policy = bus.policy.get_or_insert_with(|| {
            json!({
                "Version": "2012-10-17",
                "Statement": [],
            })
        });

        if let Some(stmts) = policy["Statement"].as_array_mut() {
            stmts.push(statement);
        }

        Ok(json_resp(json!({})))
    }

    fn remove_permission(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let event_bus_name = body["EventBusName"].as_str().unwrap_or("default");
        let statement_id = body["StatementId"].as_str().unwrap_or("");
        let remove_all = body["RemoveAllPermissions"].as_bool().unwrap_or(false);

        let mut state = self.state.write();

        let bus = state.buses.get_mut(event_bus_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Event bus {event_bus_name} does not exist."),
            )
        })?;

        if remove_all {
            bus.policy = None;
            return Ok(json_resp(json!({})));
        }

        let policy = bus.policy.as_mut().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "EventBus does not have a policy.",
            )
        })?;

        if let Some(stmts) = policy["Statement"].as_array_mut() {
            let before = stmts.len();
            stmts.retain(|s| s["Sid"].as_str() != Some(statement_id));
            if stmts.len() == before {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "Statement with the provided id does not exist.",
                ));
            }
            if stmts.is_empty() {
                bus.policy = None;
            }
        }

        Ok(json_resp(json!({})))
    }

    // ─── Rule Operations ────────────────────────────────────────────────

    fn put_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();

        let raw_bus = body["EventBusName"]
            .as_str()
            .unwrap_or("default")
            .to_string();

        let mut state = self.state.write();
        let event_bus_name = state.resolve_bus_name(&raw_bus);

        let event_pattern = body["EventPattern"].as_str().and_then(|s| {
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        });
        let schedule_expression = body["ScheduleExpression"].as_str().and_then(|s| {
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        });
        let description = body["Description"].as_str().map(|s| s.to_string());
        let role_arn = body["RoleArn"].as_str().map(|s| s.to_string());
        let rule_state = body["State"].as_str().unwrap_or("ENABLED").to_string();

        // Validate: schedule expressions only on default bus
        if schedule_expression.is_some() && event_bus_name != "default" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "ScheduleExpression is supported only on the default event bus.",
            ));
        }

        if !state.buses.contains_key(&event_bus_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Event bus {event_bus_name} does not exist."),
            ));
        }

        let arn = if event_bus_name == "default" {
            format!(
                "arn:aws:events:{}:{}:rule/{}",
                req.region, state.account_id, name
            )
        } else {
            format!(
                "arn:aws:events:{}:{}:rule/{}/{}",
                req.region, state.account_id, event_bus_name, name
            )
        };

        let key = (event_bus_name.clone(), name.clone());
        let targets = state
            .rules
            .get(&key)
            .map(|r| r.targets.clone())
            .unwrap_or_default();

        let tags = parse_tags(&body);

        let rule = EventRule {
            name: name.clone(),
            arn: arn.clone(),
            event_bus_name,
            event_pattern,
            schedule_expression,
            state: rule_state,
            description,
            role_arn,
            managed_by: None,
            created_by: None,
            targets,
            tags,
            last_fired: None,
        };

        state.rules.insert(key, rule);
        Ok(json_resp(json!({ "RuleArn": arn })))
    }

    fn delete_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let event_bus_name = body["EventBusName"].as_str().unwrap_or("default");

        let mut state = self.state.write();
        let bus_name = state.resolve_bus_name(event_bus_name);
        let key = (bus_name, name.to_string());

        // Check if rule has targets
        if let Some(rule) = state.rules.get(&key) {
            if !rule.targets.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "Rule can't be deleted since it has targets.",
                ));
            }
        }

        state.rules.remove(&key);
        Ok(json_resp(json!({})))
    }

    fn list_rules(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let event_bus_name = body["EventBusName"].as_str().unwrap_or("default");
        let name_prefix = body["NamePrefix"].as_str();
        let limit = body["Limit"].as_u64().map(|n| n as usize);
        let next_token = body["NextToken"].as_str();

        let state = self.state.read();
        let bus_name = state.resolve_bus_name(event_bus_name);

        let mut rules: Vec<&EventRule> = state
            .rules
            .values()
            .filter(|r| r.event_bus_name == bus_name)
            .filter(|r| match name_prefix {
                Some(prefix) => r.name.starts_with(prefix),
                None => true,
            })
            .collect();
        rules.sort_by(|a, b| a.name.cmp(&b.name));

        // Pagination
        let start = next_token
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let rules_slice = &rules[start..];

        let (page, new_next_token) = if let Some(lim) = limit {
            if rules_slice.len() > lim {
                (&rules_slice[..lim], Some((start + lim).to_string()))
            } else {
                (rules_slice, None)
            }
        } else {
            (rules_slice, None)
        };

        let rules_json: Vec<Value> = page
            .iter()
            .map(|r| {
                let mut obj = json!({
                    "Name": r.name,
                    "Arn": r.arn,
                    "EventBusName": r.event_bus_name,
                    "State": r.state,
                });
                if let Some(ref desc) = r.description {
                    obj["Description"] = json!(desc);
                }
                if let Some(ref ep) = r.event_pattern {
                    obj["EventPattern"] = json!(ep);
                }
                if let Some(ref se) = r.schedule_expression {
                    obj["ScheduleExpression"] = json!(se);
                }
                if let Some(ref mb) = r.managed_by {
                    obj["ManagedBy"] = json!(mb);
                }
                obj
            })
            .collect();

        let mut resp = json!({ "Rules": rules_json });
        if let Some(token) = new_next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(json_resp(resp))
    }

    fn describe_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let event_bus_name = body["EventBusName"].as_str().unwrap_or("default");

        let state = self.state.read();
        let bus_name = state.resolve_bus_name(event_bus_name);
        let key = (bus_name.clone(), name.to_string());

        let rule = state.rules.get(&key).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Rule {name} does not exist."),
            )
        })?;

        let mut resp = json!({
            "Name": rule.name,
            "Arn": rule.arn,
            "EventBusName": rule.event_bus_name,
            "State": rule.state,
        });

        if let Some(ref desc) = rule.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref ep) = rule.event_pattern {
            resp["EventPattern"] = json!(ep);
        }
        if let Some(ref se) = rule.schedule_expression {
            resp["ScheduleExpression"] = json!(se);
        }
        if let Some(ref role) = rule.role_arn {
            resp["RoleArn"] = json!(role);
        }
        if let Some(ref mb) = rule.managed_by {
            resp["ManagedBy"] = json!(mb);
        }
        if let Some(ref cb) = rule.created_by {
            resp["CreatedBy"] = json!(cb);
        }
        // If non-default bus, set CreatedBy to account_id
        if rule.event_bus_name != "default" && rule.created_by.is_none() {
            resp["CreatedBy"] = json!(state.account_id);
        }

        Ok(json_resp(resp))
    }

    fn enable_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let event_bus_name = body["EventBusName"].as_str().unwrap_or("default");

        let mut state = self.state.write();
        let bus_name = state.resolve_bus_name(event_bus_name);
        let key = (bus_name, name.to_string());

        let rule = state.rules.get_mut(&key).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Rule {name} does not exist."),
            )
        })?;

        rule.state = "ENABLED".to_string();
        Ok(json_resp(json!({})))
    }

    fn disable_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        let event_bus_name = body["EventBusName"].as_str().unwrap_or("default");

        let mut state = self.state.write();
        let bus_name = state.resolve_bus_name(event_bus_name);
        let key = (bus_name, name.to_string());

        let rule = state.rules.get_mut(&key).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Rule {name} does not exist."),
            )
        })?;

        rule.state = "DISABLED".to_string();
        Ok(json_resp(json!({})))
    }

    // ─── Target Operations ──────────────────────────────────────────────

    fn put_targets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let rule_name = body["Rule"].as_str().ok_or_else(|| missing("Rule"))?;
        let event_bus_name = body["EventBusName"].as_str().unwrap_or("default");
        let targets = body["Targets"]
            .as_array()
            .ok_or_else(|| missing("Targets"))?;

        // Validate targets - check for FIFO SQS without SqsParameters
        for target in targets {
            let target_id = target["Id"].as_str().unwrap_or("");
            let target_arn = target["Arn"].as_str().unwrap_or("");

            if target_arn.ends_with(".fifo") && target.get("SqsParameters").is_none() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "Parameter(s) SqsParameters must be specified for target: {target_id}."
                    ),
                ));
            }

            // Validate ARN format
            if !target_arn.starts_with("arn:") {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "Parameter {target_arn} is not valid. Reason: Provided Arn is not in correct format."
                    ),
                ));
            }
        }

        let mut state = self.state.write();
        let bus_name = state.resolve_bus_name(event_bus_name);
        let key = (bus_name.clone(), rule_name.to_string());

        let rule = state.rules.get_mut(&key).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Rule {rule_name} does not exist on EventBus {bus_name}."),
            )
        })?;

        for target in targets {
            let et = parse_target(target);
            // Remove existing target with same ID
            rule.targets.retain(|t| t.id != et.id);
            rule.targets.push(et);
        }

        Ok(json_resp(json!({
            "FailedEntryCount": 0,
            "FailedEntries": [],
        })))
    }

    fn remove_targets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let rule_name = body["Rule"].as_str().ok_or_else(|| missing("Rule"))?;
        let event_bus_name = body["EventBusName"].as_str().unwrap_or("default");
        let ids = body["Ids"].as_array().ok_or_else(|| missing("Ids"))?;

        let target_ids: Vec<String> = ids
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();

        let mut state = self.state.write();
        let bus_name = state.resolve_bus_name(event_bus_name);
        let key = (bus_name.clone(), rule_name.to_string());

        let rule = state.rules.get_mut(&key).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Rule {rule_name} does not exist on EventBus {bus_name}."),
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
        let event_bus_name = body["EventBusName"].as_str().unwrap_or("default");
        let limit = body["Limit"].as_u64().map(|n| n as usize);
        let next_token = body["NextToken"].as_str();

        let state = self.state.read();
        let bus_name = state.resolve_bus_name(event_bus_name);
        let key = (bus_name, rule_name.to_string());

        let rule = state.rules.get(&key).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Rule {rule_name} does not exist."),
            )
        })?;

        let all_targets = &rule.targets;
        let start = next_token
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let slice = &all_targets[start..];

        let (page, new_next_token) = if let Some(lim) = limit {
            if slice.len() > lim {
                (&slice[..lim], Some((start + lim).to_string()))
            } else {
                (slice, None)
            }
        } else {
            (slice, None)
        };

        let targets: Vec<Value> = page.iter().map(target_to_json).collect();

        let mut resp = json!({ "Targets": targets });
        if let Some(token) = new_next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(json_resp(resp))
    }

    fn list_rule_names_by_target(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let target_arn = body["TargetArn"]
            .as_str()
            .ok_or_else(|| missing("TargetArn"))?;
        let event_bus_name = body["EventBusName"].as_str().unwrap_or("default");
        let limit = body["Limit"].as_u64().map(|n| n as usize);
        let next_token = body["NextToken"].as_str();

        let state = self.state.read();
        let bus_name = state.resolve_bus_name(event_bus_name);

        // Deduplicate rule names
        let mut rule_names: Vec<String> = Vec::new();
        for rule in state.rules.values() {
            if rule.event_bus_name == bus_name
                && rule.targets.iter().any(|t| t.arn == target_arn)
                && !rule_names.contains(&rule.name)
            {
                rule_names.push(rule.name.clone());
            }
        }
        rule_names.sort();

        let start = next_token
            .and_then(|t| t.parse::<usize>().ok())
            .unwrap_or(0);
        let slice = &rule_names[start..];

        let (page, new_next_token) = if let Some(lim) = limit {
            if slice.len() > lim {
                (&slice[..lim], Some((start + lim).to_string()))
            } else {
                (slice, None)
            }
        } else {
            (slice, None)
        };

        let mut resp = json!({ "RuleNames": page });
        if let Some(token) = new_next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(json_resp(resp))
    }

    // ─── Partner Event Sources ────────────���───────────────────────────

    fn create_partner_event_source(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        let account = body["Account"]
            .as_str()
            .ok_or_else(|| missing("Account"))?
            .to_string();

        let mut state = self.state.write();
        if state.partner_event_sources.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceAlreadyExistsException",
                format!("Partner event source {name} already exists."),
            ));
        }
        state
            .partner_event_sources
            .insert(name.clone(), account.clone());

        Ok(json_resp(json!({})))
    }

    fn describe_partner_event_source(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();

        let state = self.state.read();
        if !state.partner_event_sources.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Partner event source {name} does not exist."),
            ));
        }

        let arn = format!(
            "arn:aws:events:{}::event-source/aws.partner/{}",
            state.region, name
        );

        Ok(json_resp(json!({
            "Arn": arn,
            "Name": name,
        })))
    }

    // ─── PutEvents ───────────────���──────────────────────────────────────

    fn put_events(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let entries = body["Entries"]
            .as_array()
            .ok_or_else(|| missing("Entries"))?;

        // Validate max 10 entries
        if entries.len() > 10 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "1 validation error detected: Value '[PutEventsRequestEntry]' at 'entries' failed to satisfy constraint: Member must have length less than or equal to 10",
            ));
        }

        let mut state = self.state.write();
        let mut result_entries = Vec::new();
        let mut events_to_deliver = Vec::new();
        let mut failed_count = 0;

        for entry in entries {
            let source = entry["Source"].as_str().unwrap_or("").to_string();
            let detail_type = entry["DetailType"].as_str().unwrap_or("").to_string();
            let detail = entry["Detail"].as_str().unwrap_or("").to_string();

            // Validate required fields
            if source.is_empty() {
                failed_count += 1;
                result_entries.push(json!({
                    "ErrorCode": "InvalidArgument",
                    "ErrorMessage": "Parameter Source is not valid. Reason: Source is a required argument.",
                }));
                continue;
            }
            if detail_type.is_empty() {
                failed_count += 1;
                result_entries.push(json!({
                    "ErrorCode": "InvalidArgument",
                    "ErrorMessage": "Parameter DetailType is not valid. Reason: DetailType is a required argument.",
                }));
                continue;
            }
            if detail.is_empty() {
                failed_count += 1;
                result_entries.push(json!({
                    "ErrorCode": "InvalidArgument",
                    "ErrorMessage": "Parameter Detail is not valid. Reason: Detail is a required argument.",
                }));
                continue;
            }

            // Validate Detail is valid JSON
            if serde_json::from_str::<Value>(&detail).is_err() {
                failed_count += 1;
                result_entries.push(json!({
                    "ErrorCode": "MalformedDetail",
                    "ErrorMessage": "Detail is malformed.",
                }));
                continue;
            }

            let event_id = uuid::Uuid::new_v4().to_string();
            let raw_bus = entry["EventBusName"]
                .as_str()
                .unwrap_or("default")
                .to_string();
            let event_bus_name = state.resolve_bus_name(&raw_bus);
            let time = if let Some(s) = entry["Time"].as_str() {
                DateTime::parse_from_rfc3339(s)
                    .map(|dt| dt.with_timezone(&Utc))
                    .unwrap_or_else(|_| Utc::now())
            } else if let Some(ts) = entry["Time"].as_f64() {
                DateTime::from_timestamp(ts as i64, ((ts.fract()) * 1_000_000_000.0) as u32)
                    .unwrap_or_else(Utc::now)
            } else if let Some(ts) = entry["Time"].as_i64() {
                DateTime::from_timestamp(ts, 0).unwrap_or_else(Utc::now)
            } else {
                Utc::now()
            };
            let resources: Vec<String> = entry["Resources"]
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();

            let event = PutEvent {
                event_id: event_id.clone(),
                source: source.clone(),
                detail_type: detail_type.clone(),
                detail: detail.clone(),
                event_bus_name: event_bus_name.clone(),
                time,
                resources: resources.clone(),
            };

            // Archive matching events
            let archive_keys: Vec<String> = state.archives.keys().cloned().collect();
            for akey in archive_keys {
                let archive_bus = {
                    let a = &state.archives[&akey];
                    state.resolve_bus_name(&a.event_source_arn)
                };
                if archive_bus == event_bus_name {
                    if let Some(archive) = state.archives.get_mut(&akey) {
                        if archive.state == "ENABLED" {
                            archive.event_count += 1;
                            archive.size_bytes += detail.len() as i64;
                            archive.events.push(event.clone());
                        }
                    }
                }
            }

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
                            &req.account_id,
                            &req.region,
                            &resources,
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
                    resources,
                    matching_targets,
                ));
            }

            result_entries.push(json!({ "EventId": event_id }));
        }

        // Drop the lock before delivering
        drop(state);

        // Deliver to targets
        for (event_id, source, detail_type, detail, time, resources, targets) in events_to_deliver {
            let detail_value: Value = serde_json::from_str(&detail).unwrap_or(json!({}));
            let event_json = json!({
                "version": "0",
                "id": event_id,
                "source": source,
                "account": req.account_id,
                "detail-type": detail_type,
                "detail": detail_value,
                "time": time.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                "region": req.region,
                "resources": resources,
            });
            let event_str = event_json.to_string();

            for target in targets {
                let arn = &target.arn;
                // Compute the message body, applying InputTransformer if present
                let body_str = if let Some(ref transformer) = target.input_transformer {
                    apply_input_transformer(transformer, &event_json)
                } else if let Some(ref input) = target.input {
                    input.clone()
                } else if let Some(ref input_path) = target.input_path {
                    resolve_json_path(&event_json, input_path)
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| event_str.clone())
                } else {
                    event_str.clone()
                };

                if arn.contains(":sqs:") {
                    // Extract FIFO parameters (MessageGroupId)
                    let group_id = target
                        .sqs_parameters
                        .as_ref()
                        .and_then(|p| p["MessageGroupId"].as_str())
                        .map(|s| s.to_string());
                    if group_id.is_some() {
                        // FIFO queue: send with group ID but no dedup ID.
                        // Queues with content-based dedup will auto-generate one;
                        // queues without it will reject the message.
                        self.delivery.send_to_sqs_with_attrs(
                            arn,
                            &body_str,
                            &HashMap::new(),
                            group_id.as_deref(),
                            None,
                        );
                    } else {
                        self.delivery.send_to_sqs(arn, &body_str, &HashMap::new());
                    }
                } else if arn.contains(":sns:") {
                    self.delivery
                        .publish_to_sns(arn, &body_str, Some(&detail_type));
                }
            }
        }

        Ok(json_resp(json!({
            "FailedEntryCount": failed_count,
            "Entries": result_entries,
        })))
    }

    // ─── Tagging ────────────────────────────────────────────────────────

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let arn = body["ResourceARN"]
            .as_str()
            .ok_or_else(|| missing("ResourceARN"))?;
        let tags = body["Tags"].as_array().ok_or_else(|| missing("Tags"))?;

        let mut state = self.state.write();

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

    // ─── Archive Operations ─────────────────────────────────────────────

    fn create_archive(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["ArchiveName"]
            .as_str()
            .ok_or_else(|| missing("ArchiveName"))?
            .to_string();
        let event_source_arn = body["EventSourceArn"]
            .as_str()
            .ok_or_else(|| missing("EventSourceArn"))?
            .to_string();
        let description = body["Description"].as_str().map(|s| s.to_string());
        let event_pattern = body["EventPattern"].as_str().map(|s| s.to_string());
        let retention_days = body["RetentionDays"].as_i64().unwrap_or(0);

        // Validate name length
        if name.len() > 48 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    " 1 validation error detected: Value '{}' at 'archiveName' failed to satisfy constraint: Member must have length less than or equal to 48",
                    name
                ),
            ));
        }

        // Validate event pattern if provided
        if let Some(ref pattern) = event_pattern {
            validate_event_pattern(pattern)?;
        }

        let mut state = self.state.write();

        // Validate event bus exists
        let bus_name = state.resolve_bus_name(&event_source_arn);
        if !state.buses.contains_key(&bus_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Event bus {bus_name} does not exist."),
            ));
        }

        // Check duplicate
        if state.archives.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceAlreadyExistsException",
                format!("Archive {name} already exists."),
            ));
        }

        let now = Utc::now();
        let arn = format!(
            "arn:aws:events:{}:{}:archive/{}",
            req.region, state.account_id, name
        );

        let archive = Archive {
            name: name.clone(),
            arn: arn.clone(),
            event_source_arn: event_source_arn.clone(),
            description,
            event_pattern: event_pattern.clone(),
            retention_days,
            state: "ENABLED".to_string(),
            creation_time: now,
            event_count: 0,
            size_bytes: 0,
            events: Vec::new(),
        };
        state.archives.insert(name.clone(), archive);

        // Create the archive rule
        let rule_name = format!("Events-Archive-{name}");
        let rule_arn = format!(
            "arn:aws:events:{}:{}:rule/{}",
            req.region, state.account_id, rule_name
        );
        // Merge archive event pattern with replay-name filter
        let rule_event_pattern = {
            let mut merged = if let Some(ref ep) = event_pattern {
                serde_json::from_str::<Value>(ep).unwrap_or_else(|_| json!({}))
            } else {
                json!({})
            };
            if let Some(obj) = merged.as_object_mut() {
                obj.insert("replay-name".to_string(), json!([{"exists": false}]));
            }
            serde_json::to_string(&merged).unwrap_or_default()
        };

        let archive_rule = EventRule {
            name: rule_name.clone(),
            arn: rule_arn,
            event_bus_name: bus_name.clone(),
            event_pattern: Some(rule_event_pattern),
            schedule_expression: None,
            state: "ENABLED".to_string(),
            description: None,
            role_arn: None,
            managed_by: Some("prod.vhs.events.aws.internal".to_string()),
            created_by: Some(state.account_id.clone()),
            targets: Vec::new(),
            tags: HashMap::new(),
            last_fired: None,
        };
        let key = (bus_name, rule_name);
        state.rules.insert(key, archive_rule);

        Ok(json_resp(json!({
            "ArchiveArn": arn,
            "CreationTime": now.timestamp() as f64,
            "State": "ENABLED",
        })))
    }

    fn describe_archive(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["ArchiveName"]
            .as_str()
            .ok_or_else(|| missing("ArchiveName"))?;

        let state = self.state.read();
        let archive = state.archives.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Archive {name} does not exist."),
            )
        })?;

        let mut resp = json!({
            "ArchiveArn": archive.arn,
            "ArchiveName": archive.name,
            "CreationTime": archive.creation_time.timestamp() as f64,
            "EventCount": archive.event_count,
            "EventSourceArn": archive.event_source_arn,
            "RetentionDays": archive.retention_days,
            "SizeBytes": archive.size_bytes,
            "State": archive.state,
        });
        if let Some(ref desc) = archive.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref ep) = archive.event_pattern {
            resp["EventPattern"] = json!(ep);
        }

        Ok(json_resp(resp))
    }

    fn list_archives(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name_prefix = body["NamePrefix"].as_str();
        let source_arn = body["EventSourceArn"].as_str();
        let archive_state = body["State"].as_str();

        // Validate at most one filter
        let filter_count = [
            name_prefix.is_some(),
            source_arn.is_some(),
            archive_state.is_some(),
        ]
        .iter()
        .filter(|&&x| x)
        .count();
        if filter_count > 1 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "At most one filter is allowed for ListArchives. Use either : State, EventSourceArn, or NamePrefix.",
            ));
        }

        // Validate state
        if let Some(s) = archive_state {
            let valid = [
                "ENABLED",
                "DISABLED",
                "CREATING",
                "UPDATING",
                "CREATE_FAILED",
                "UPDATE_FAILED",
            ];
            if !valid.contains(&s) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "1 validation error detected: Value '{}' at 'state' failed to satisfy constraint: Member must satisfy enum value set: [ENABLED, DISABLED, CREATING, UPDATING, CREATE_FAILED, UPDATE_FAILED]",
                        s
                    ),
                ));
            }
        }

        let state = self.state.read();
        let archives: Vec<Value> = state
            .archives
            .values()
            .filter(|a| {
                if let Some(prefix) = name_prefix {
                    a.name.starts_with(prefix)
                } else if let Some(arn) = source_arn {
                    a.event_source_arn == arn
                } else if let Some(s) = archive_state {
                    a.state == s
                } else {
                    true
                }
            })
            .map(|a| {
                json!({
                    "ArchiveName": a.name,
                    "CreationTime": a.creation_time.timestamp() as f64,
                    "EventCount": a.event_count,
                    "EventSourceArn": a.event_source_arn,
                    "RetentionDays": a.retention_days,
                    "SizeBytes": a.size_bytes,
                    "State": a.state,
                })
            })
            .collect();

        Ok(json_resp(json!({ "Archives": archives })))
    }

    fn update_archive(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["ArchiveName"]
            .as_str()
            .ok_or_else(|| missing("ArchiveName"))?;

        // Validate event pattern if provided
        if let Some(pattern) = body["EventPattern"].as_str() {
            validate_event_pattern(pattern)?;
        }

        let mut state = self.state.write();
        let archive = state.archives.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Archive {name} does not exist."),
            )
        })?;

        if let Some(desc) = body["Description"].as_str() {
            archive.description = Some(desc.to_string());
        }
        if let Some(pattern) = body["EventPattern"].as_str() {
            archive.event_pattern = Some(pattern.to_string());
        }
        if let Some(days) = body["RetentionDays"].as_i64() {
            archive.retention_days = days;
        }

        Ok(json_resp(json!({
            "ArchiveArn": archive.arn,
            "CreationTime": archive.creation_time.timestamp() as f64,
            "State": archive.state,
        })))
    }

    fn delete_archive(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["ArchiveName"]
            .as_str()
            .ok_or_else(|| missing("ArchiveName"))?;

        let mut state = self.state.write();
        if !state.archives.contains_key(name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Archive {name} does not exist."),
            ));
        }

        state.archives.remove(name);

        // Remove the archive rule
        let rule_name = format!("Events-Archive-{name}");
        state.rules.retain(|k, _| k.1 != rule_name);

        Ok(json_resp(json!({})))
    }

    // ─── Connection Operations ──────────────────────────────────────────

    fn create_connection(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        let description = body["Description"].as_str().map(|s| s.to_string());
        let auth_type = body["AuthorizationType"]
            .as_str()
            .ok_or_else(|| missing("AuthorizationType"))?
            .to_string();
        let auth_params = body["AuthParameters"].clone();

        let mut state = self.state.write();
        let now = Utc::now();
        let conn_uuid = uuid::Uuid::new_v4();
        let arn = format!(
            "arn:aws:events:{}:{}:connection/{}/{}",
            req.region, state.account_id, name, conn_uuid
        );
        let secret_arn = format!(
            "arn:aws:secretsmanager:{}:{}:secret:events!connection/{}/{}",
            req.region, state.account_id, name, conn_uuid
        );

        let conn = Connection {
            name: name.clone(),
            arn: arn.clone(),
            description,
            authorization_type: auth_type.clone(),
            auth_parameters: auth_params,
            connection_state: "AUTHORIZED".to_string(),
            secret_arn: secret_arn.clone(),
            creation_time: now,
            last_modified_time: now,
            last_authorized_time: now,
        };
        state.connections.insert(name, conn);

        Ok(json_resp(json!({
            "ConnectionArn": arn,
            "ConnectionState": "AUTHORIZED",
            "CreationTime": now.timestamp() as f64,
            "LastModifiedTime": now.timestamp() as f64,
        })))
    }

    fn describe_connection(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let state = self.state.read();
        let conn = state.connections.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Connection '{name}' does not exist."),
            )
        })?;

        // Build auth parameters response - strip secrets
        let auth_params_response =
            build_auth_params_response(&conn.authorization_type, &conn.auth_parameters);

        let mut resp = json!({
            "ConnectionArn": conn.arn,
            "Name": conn.name,
            "AuthorizationType": conn.authorization_type,
            "AuthParameters": auth_params_response,
            "ConnectionState": conn.connection_state,
            "SecretArn": conn.secret_arn,
            "CreationTime": conn.creation_time.timestamp() as f64,
            "LastModifiedTime": conn.last_modified_time.timestamp() as f64,
            "LastAuthorizedTime": conn.last_authorized_time.timestamp() as f64,
        });
        if let Some(ref desc) = conn.description {
            resp["Description"] = json!(desc);
        }

        Ok(json_resp(resp))
    }

    fn list_connections(&self, _req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let conns: Vec<Value> = state
            .connections
            .values()
            .map(|c| {
                let mut obj = json!({
                    "ConnectionArn": c.arn,
                    "Name": c.name,
                    "AuthorizationType": c.authorization_type,
                    "ConnectionState": c.connection_state,
                    "CreationTime": c.creation_time.timestamp() as f64,
                    "LastModifiedTime": c.last_modified_time.timestamp() as f64,
                    "LastAuthorizedTime": c.last_authorized_time.timestamp() as f64,
                });
                if let Some(ref desc) = c.description {
                    obj["Description"] = json!(desc);
                }
                obj
            })
            .collect();

        Ok(json_resp(json!({ "Connections": conns })))
    }

    fn update_connection(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let mut state = self.state.write();
        let conn = state.connections.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Connection '{name}' does not exist."),
            )
        })?;

        if let Some(desc) = body["Description"].as_str() {
            conn.description = Some(desc.to_string());
        }
        if let Some(auth_type) = body["AuthorizationType"].as_str() {
            conn.authorization_type = auth_type.to_string();
        }
        if body.get("AuthParameters").is_some() {
            conn.auth_parameters = body["AuthParameters"].clone();
        }
        conn.last_modified_time = Utc::now();

        Ok(json_resp(json!({
            "ConnectionArn": conn.arn,
            "ConnectionState": conn.connection_state,
            "CreationTime": conn.creation_time.timestamp() as f64,
            "LastModifiedTime": conn.last_modified_time.timestamp() as f64,
            "LastAuthorizedTime": conn.last_authorized_time.timestamp() as f64,
        })))
    }

    fn delete_connection(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let mut state = self.state.write();
        let conn = state.connections.remove(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Connection '{name}' does not exist."),
            )
        })?;

        Ok(json_resp(json!({
            "ConnectionArn": conn.arn,
            "ConnectionState": conn.connection_state,
            "CreationTime": conn.creation_time.timestamp() as f64,
            "LastModifiedTime": conn.last_modified_time.timestamp() as f64,
            "LastAuthorizedTime": conn.last_authorized_time.timestamp() as f64,
        })))
    }

    // ─── API Destination Operations ─────────────────────────────────────

    fn create_api_destination(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        let description = body["Description"].as_str().map(|s| s.to_string());
        let connection_arn = body["ConnectionArn"]
            .as_str()
            .ok_or_else(|| missing("ConnectionArn"))?
            .to_string();
        let endpoint = body["InvocationEndpoint"]
            .as_str()
            .ok_or_else(|| missing("InvocationEndpoint"))?
            .to_string();
        let http_method = body["HttpMethod"]
            .as_str()
            .ok_or_else(|| missing("HttpMethod"))?
            .to_string();
        let rate_limit = body["InvocationRateLimitPerSecond"].as_i64();

        let mut state = self.state.write();
        let now = Utc::now();
        let dest_uuid = uuid::Uuid::new_v4();
        let arn = format!(
            "arn:aws:events:{}:{}:api-destination/{}/{}",
            req.region, state.account_id, name, dest_uuid
        );

        let dest = ApiDestination {
            name: name.clone(),
            arn: arn.clone(),
            description,
            connection_arn,
            invocation_endpoint: endpoint,
            http_method,
            invocation_rate_limit_per_second: rate_limit,
            state: "ACTIVE".to_string(),
            creation_time: now,
            last_modified_time: now,
        };
        state.api_destinations.insert(name, dest);

        Ok(json_resp(json!({
            "ApiDestinationArn": arn,
            "ApiDestinationState": "ACTIVE",
            "CreationTime": now.timestamp() as f64,
            "LastModifiedTime": now.timestamp() as f64,
        })))
    }

    fn describe_api_destination(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let state = self.state.read();
        let dest = state.api_destinations.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("An api-destination '{name}' does not exist."),
            )
        })?;

        let mut resp = json!({
            "ApiDestinationArn": dest.arn,
            "Name": dest.name,
            "ConnectionArn": dest.connection_arn,
            "InvocationEndpoint": dest.invocation_endpoint,
            "HttpMethod": dest.http_method,
            "ApiDestinationState": dest.state,
            "CreationTime": dest.creation_time.timestamp() as f64,
            "LastModifiedTime": dest.last_modified_time.timestamp() as f64,
        });
        if let Some(ref desc) = dest.description {
            resp["Description"] = json!(desc);
        }
        if let Some(rate) = dest.invocation_rate_limit_per_second {
            resp["InvocationRateLimitPerSecond"] = json!(rate);
        }

        Ok(json_resp(resp))
    }

    fn list_api_destinations(&self, _req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let dests: Vec<Value> = state
            .api_destinations
            .values()
            .map(|d| {
                let mut obj = json!({
                    "ApiDestinationArn": d.arn,
                    "Name": d.name,
                    "ConnectionArn": d.connection_arn,
                    "InvocationEndpoint": d.invocation_endpoint,
                    "HttpMethod": d.http_method,
                    "ApiDestinationState": d.state,
                    "CreationTime": d.creation_time.timestamp() as f64,
                    "LastModifiedTime": d.last_modified_time.timestamp() as f64,
                });
                if let Some(ref desc) = d.description {
                    obj["Description"] = json!(desc);
                }
                if let Some(rate) = d.invocation_rate_limit_per_second {
                    obj["InvocationRateLimitPerSecond"] = json!(rate);
                }
                obj
            })
            .collect();

        Ok(json_resp(json!({ "ApiDestinations": dests })))
    }

    fn update_api_destination(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let mut state = self.state.write();
        let dest = state.api_destinations.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("An api-destination '{name}' does not exist."),
            )
        })?;

        if let Some(desc) = body["Description"].as_str() {
            dest.description = Some(desc.to_string());
        }
        if let Some(endpoint) = body["InvocationEndpoint"].as_str() {
            dest.invocation_endpoint = endpoint.to_string();
        }
        if let Some(method) = body["HttpMethod"].as_str() {
            dest.http_method = method.to_string();
        }
        if let Some(rate) = body["InvocationRateLimitPerSecond"].as_i64() {
            dest.invocation_rate_limit_per_second = Some(rate);
        }
        if let Some(conn) = body["ConnectionArn"].as_str() {
            dest.connection_arn = conn.to_string();
        }
        dest.last_modified_time = Utc::now();

        Ok(json_resp(json!({
            "ApiDestinationArn": dest.arn,
            "ApiDestinationState": dest.state,
            "CreationTime": dest.creation_time.timestamp() as f64,
            "LastModifiedTime": dest.last_modified_time.timestamp() as f64,
        })))
    }

    fn delete_api_destination(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let mut state = self.state.write();
        if !state.api_destinations.contains_key(name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("An api-destination '{name}' does not exist."),
            ));
        }
        state.api_destinations.remove(name);

        Ok(json_resp(json!({})))
    }

    // ─── Replay Operations ──────────────────────────────────────────────

    fn start_replay(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["ReplayName"]
            .as_str()
            .ok_or_else(|| missing("ReplayName"))?
            .to_string();
        let description = body["Description"].as_str().map(|s| s.to_string());
        let event_source_arn = body["EventSourceArn"]
            .as_str()
            .ok_or_else(|| missing("EventSourceArn"))?
            .to_string();
        let destination = body["Destination"].clone();
        let event_start_time_f = body["EventStartTime"].as_f64();
        let event_end_time_f = body["EventEndTime"].as_f64();

        let event_start_time = event_start_time_f
            .and_then(|f| DateTime::from_timestamp(f as i64, 0))
            .unwrap_or_else(Utc::now);
        let event_end_time = event_end_time_f
            .and_then(|f| DateTime::from_timestamp(f as i64, 0))
            .unwrap_or_else(Utc::now);

        // Validate destination ARN
        let dest_arn = destination["Arn"].as_str().unwrap_or("");
        if !dest_arn.contains(":event-bus/") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Parameter Destination.Arn is not valid. Reason: Must contain an event bus ARN.",
            ));
        }

        let mut state = self.state.write();

        // Validate event bus exists
        let bus_name = state.resolve_bus_name(dest_arn);
        if !state.buses.contains_key(&bus_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Event bus {bus_name} does not exist."),
            ));
        }

        // Validate archive exists
        let archive_name = event_source_arn
            .rsplit_once("archive/")
            .map(|(_, n)| n.to_string())
            .unwrap_or_default();
        if !state.archives.contains_key(&archive_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "Parameter EventSourceArn is not valid. Reason: Archive {archive_name} does not exist."
                ),
            ));
        }

        // Validate archive bus matches destination bus
        let archive = state.archives.get(&archive_name).unwrap();
        let archive_bus = state.resolve_bus_name(&archive.event_source_arn);
        if archive_bus != bus_name {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Parameter Destination.Arn is not valid. Reason: Cross event bus replay is not permitted.",
            ));
        }

        // Validate end time after start time
        if event_end_time <= event_start_time {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Parameter EventEndTime is not valid. Reason: EventStartTime must be before EventEndTime.",
            ));
        }

        // Check duplicate
        if state.replays.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceAlreadyExistsException",
                format!("Replay {name} already exists."),
            ));
        }

        let now = Utc::now();
        let arn = format!(
            "arn:aws:events:{}:{}:replay/{}",
            req.region, state.account_id, name
        );

        let replay = Replay {
            name: name.clone(),
            arn: arn.clone(),
            description,
            event_source_arn,
            destination,
            event_start_time,
            event_end_time,
            state: "COMPLETED".to_string(), // Mock completes immediately
            replay_start_time: now,
            replay_end_time: Some(now),
        };
        state.replays.insert(name, replay);

        Ok(json_resp(json!({
            "ReplayArn": arn,
            "ReplayStartTime": now.timestamp() as f64,
            "State": "STARTING",
        })))
    }

    fn describe_replay(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["ReplayName"]
            .as_str()
            .ok_or_else(|| missing("ReplayName"))?;

        let state = self.state.read();
        let replay = state.replays.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Replay {name} does not exist."),
            )
        })?;

        let mut resp = json!({
            "Destination": replay.destination,
            "EventSourceArn": replay.event_source_arn,
            "EventStartTime": replay.event_start_time.timestamp() as f64,
            "EventEndTime": replay.event_end_time.timestamp() as f64,
            "ReplayArn": replay.arn,
            "ReplayName": replay.name,
            "ReplayStartTime": replay.replay_start_time.timestamp() as f64,
            "State": replay.state,
        });
        if let Some(ref desc) = replay.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref end) = replay.replay_end_time {
            resp["ReplayEndTime"] = json!(end.timestamp() as f64);
        }

        Ok(json_resp(resp))
    }

    fn list_replays(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name_prefix = body["NamePrefix"].as_str();
        let source_arn = body["EventSourceArn"].as_str();
        let replay_state = body["State"].as_str();

        // Validate at most one filter
        let filter_count = [
            name_prefix.is_some(),
            source_arn.is_some(),
            replay_state.is_some(),
        ]
        .iter()
        .filter(|&&x| x)
        .count();
        if filter_count > 1 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "At most one filter is allowed for ListReplays. Use either : State, EventSourceArn, or NamePrefix.",
            ));
        }

        // Validate state
        if let Some(s) = replay_state {
            let valid = [
                "CANCELLED",
                "CANCELLING",
                "COMPLETED",
                "FAILED",
                "RUNNING",
                "STARTING",
            ];
            if !valid.contains(&s) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "1 validation error detected: Value '{}' at 'state' failed to satisfy constraint: Member must satisfy enum value set: [CANCELLED, CANCELLING, COMPLETED, FAILED, RUNNING, STARTING]",
                        s
                    ),
                ));
            }
        }

        let state = self.state.read();
        let replays: Vec<Value> = state
            .replays
            .values()
            .filter(|r| {
                if let Some(prefix) = name_prefix {
                    r.name.starts_with(prefix)
                } else if let Some(arn) = source_arn {
                    r.event_source_arn == arn
                } else if let Some(s) = replay_state {
                    r.state == s
                } else {
                    true
                }
            })
            .map(|r| {
                let mut obj = json!({
                    "EventSourceArn": r.event_source_arn,
                    "EventStartTime": r.event_start_time.timestamp() as f64,
                    "EventEndTime": r.event_end_time.timestamp() as f64,
                    "ReplayName": r.name,
                    "ReplayStartTime": r.replay_start_time.timestamp() as f64,
                    "State": r.state,
                });
                if let Some(ref end) = r.replay_end_time {
                    obj["ReplayEndTime"] = json!(end.timestamp() as f64);
                }
                obj
            })
            .collect();

        Ok(json_resp(json!({ "Replays": replays })))
    }

    fn cancel_replay(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let name = body["ReplayName"]
            .as_str()
            .ok_or_else(|| missing("ReplayName"))?;

        let mut state = self.state.write();
        let replay = state.replays.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Replay {name} does not exist."),
            )
        })?;

        // Can only cancel STARTING or RUNNING replays (or COMPLETED in our mock)
        if replay.state == "CANCELLED" || replay.state == "CANCELLING" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "IllegalStatusException",
                format!("Replay {name} is not in a valid state for this operation."),
            ));
        }

        let arn = replay.arn.clone();
        replay.state = "CANCELLED".to_string();

        Ok(json_resp(json!({
            "ReplayArn": arn,
            "State": "CANCELLING",
        })))
    }
}

// ─── Tag Lookup Helpers ─────────────────────────────────────────────────

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

    // Parse ARN to give better error messages
    let error_msg = if arn.contains(":rule/") {
        // Extract rule name and bus from ARN
        let parts: Vec<&str> = arn.rsplitn(2, ":rule/").collect();
        if let Some(rule_path) = parts.first() {
            if let Some((bus, rule_name)) = rule_path.rsplit_once('/') {
                format!("Rule {rule_name} does not exist on EventBus {bus}.")
            } else {
                format!("Rule {} does not exist on EventBus default.", rule_path)
            }
        } else {
            format!("Resource {arn} not found.")
        }
    } else {
        format!("Resource {arn} not found.")
    };

    Err(AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        error_msg,
    ))
}

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

    let error_msg = if arn.contains(":rule/") {
        let parts: Vec<&str> = arn.rsplitn(2, ":rule/").collect();
        if let Some(rule_path) = parts.first() {
            if let Some((bus, rule_name)) = rule_path.rsplit_once('/') {
                format!("Rule {rule_name} does not exist on EventBus {bus}.")
            } else {
                format!("Rule {} does not exist on EventBus default.", rule_path)
            }
        } else {
            format!("Resource {arn} not found.")
        }
    } else {
        format!("Resource {arn} not found.")
    };

    Err(AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        error_msg,
    ))
}

// ─── Event Pattern Validation ────────────────────────────────────────

fn validate_event_pattern(pattern: &str) -> Result<(), AwsServiceError> {
    let parsed: Value = serde_json::from_str(pattern).map_err(|_| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidEventPatternException",
            "Event pattern is not valid. Reason: Invalid JSON",
        )
    })?;

    validate_pattern_values(&parsed, "")?;
    Ok(())
}

fn validate_pattern_values(value: &Value, path: &str) -> Result<(), AwsServiceError> {
    match value {
        Value::Object(obj) => {
            for (key, val) in obj {
                let new_path = if path.is_empty() {
                    key.clone()
                } else {
                    format!("{path}.{key}")
                };
                match val {
                    Value::Object(_) => validate_pattern_values(val, &new_path)?,
                    Value::Array(_) => {} // Arrays are fine at leaf level
                    _ => {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidEventPatternException",
                            format!(
                                "Event pattern is not valid. Reason: '{}' must be an object or an array",
                                key
                            ),
                        ));
                    }
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

// ─── Connection Auth Params Response Builder ────────────────────────

fn build_auth_params_response(auth_type: &str, params: &Value) -> Value {
    match auth_type {
        "API_KEY" => {
            let mut resp = json!({});
            if let Some(api_key) = params.get("ApiKeyAuthParameters") {
                resp["ApiKeyAuthParameters"] = json!({
                    "ApiKeyName": api_key["ApiKeyName"],
                });
            }
            resp
        }
        "BASIC" => {
            let mut resp = json!({});
            if let Some(basic) = params.get("BasicAuthParameters") {
                resp["BasicAuthParameters"] = json!({
                    "Username": basic["Username"],
                });
            }
            resp
        }
        "OAUTH_CLIENT_CREDENTIALS" => {
            let mut resp = json!({});
            if let Some(oauth) = params.get("OAuthParameters") {
                resp["OAuthParameters"] = json!({
                    "AuthorizationEndpoint": oauth["AuthorizationEndpoint"],
                    "HttpMethod": oauth["HttpMethod"],
                    "ClientParameters": {
                        "ClientID": oauth.get("ClientParameters").and_then(|c| c.get("ClientID")),
                    },
                });
            }
            resp
        }
        _ => params.clone(),
    }
}

// ─── Event Pattern Matching ─────────────────────────────────────────

/// Match an event against an EventBridge event pattern.
fn matches_pattern(
    pattern_json: Option<&str>,
    source: &str,
    detail_type: &str,
    detail: &str,
    account: &str,
    region: &str,
    resources: &[String],
) -> bool {
    let pattern_json = match pattern_json {
        Some(p) => p,
        None => return true,
    };

    let pattern: Value = match serde_json::from_str(pattern_json) {
        Ok(v) => v,
        Err(_) => return false,
    };

    let pattern_obj = match pattern.as_object() {
        Some(o) => o,
        None => return false,
    };

    let detail_value: Value = serde_json::from_str(detail).unwrap_or(json!({}));
    let event = json!({
        "source": source,
        "detail-type": detail_type,
        "detail": detail_value,
        "account": account,
        "region": region,
        "resources": resources,
    });

    for (key, pattern_value) in pattern_obj {
        let event_value = &event[key];
        if !matches_value(pattern_value, event_value) {
            return false;
        }
    }

    true
}

fn matches_value(pattern: &Value, event_value: &Value) -> bool {
    match pattern {
        Value::Object(obj) => {
            for (key, sub_pattern) in obj {
                let sub_value = &event_value[key];
                if !matches_value(sub_pattern, sub_value) {
                    return false;
                }
            }
            true
        }
        Value::Array(arr) => arr.iter().any(|elem| matches_single(elem, event_value)),
        _ => false,
    }
}

fn matches_single(pattern_elem: &Value, event_value: &Value) -> bool {
    match pattern_elem {
        Value::Object(obj) => {
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
            false
        }
        _ => values_equal(pattern_elem, event_value),
    }
}

fn matches_numeric(numeric_arr: &Value, event_value: &Value) -> bool {
    let arr = match numeric_arr.as_array() {
        Some(a) => a,
        None => return false,
    };
    let actual = match event_value.as_f64() {
        Some(n) => n,
        None => return false,
    };
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

fn values_equal(a: &Value, b: &Value) -> bool {
    a == b
}

/// Resolve a simple JSON path like `$.detail.name` against an event JSON value.
fn resolve_json_path(event: &Value, path: &str) -> Option<Value> {
    let path = path.strip_prefix('$').unwrap_or(path);
    let mut current = event;
    for segment in path.split('.') {
        if segment.is_empty() {
            continue;
        }
        current = current.get(segment)?;
    }
    Some(current.clone())
}

/// Apply an EventBridge InputTransformer to an event.
fn apply_input_transformer(transformer: &Value, event: &Value) -> String {
    let input_paths_map = transformer
        .get("InputPathsMap")
        .and_then(|v| v.as_object())
        .cloned()
        .unwrap_or_default();
    let template = transformer
        .get("InputTemplate")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Resolve all input paths
    let mut resolved: HashMap<String, Value> = HashMap::new();
    for (var_name, path_val) in &input_paths_map {
        if let Some(path_str) = path_val.as_str() {
            if let Some(val) = resolve_json_path(event, path_str) {
                resolved.insert(var_name.clone(), val);
            }
        }
    }

    // Replace <varName> placeholders in template
    let mut result = template;
    for (var_name, val) in &resolved {
        let placeholder = format!("<{var_name}>");
        let replacement = match val {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        result = result.replace(&placeholder, &replacement);
    }

    result
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

    /// Test helper that calls matches_pattern with default account/region/resources
    fn test_matches(
        pattern_json: Option<&str>,
        source: &str,
        detail_type: &str,
        detail: &str,
    ) -> bool {
        matches_pattern(
            pattern_json,
            source,
            detail_type,
            detail,
            "123456789012",
            "us-east-1",
            &[],
        )
    }

    #[test]
    fn pattern_matches_source() {
        assert!(test_matches(
            Some(r#"{"source": ["my.app"]}"#),
            "my.app",
            "OrderPlaced",
            "{}"
        ));
        assert!(!test_matches(
            Some(r#"{"source": ["other.app"]}"#),
            "my.app",
            "OrderPlaced",
            "{}"
        ));
    }

    #[test]
    fn pattern_matches_detail_type() {
        assert!(test_matches(
            Some(r#"{"detail-type": ["OrderPlaced"]}"#),
            "my.app",
            "OrderPlaced",
            "{}"
        ));
        assert!(!test_matches(
            Some(r#"{"detail-type": ["OrderShipped"]}"#),
            "my.app",
            "OrderPlaced",
            "{}"
        ));
    }

    #[test]
    fn pattern_matches_detail_field() {
        assert!(test_matches(
            Some(r#"{"detail": {"status": ["ACTIVE"]}}"#),
            "my.app",
            "StatusChange",
            r#"{"status": "ACTIVE"}"#
        ));
        assert!(!test_matches(
            Some(r#"{"detail": {"status": ["ACTIVE"]}}"#),
            "my.app",
            "StatusChange",
            r#"{"status": "INACTIVE"}"#
        ));
    }

    #[test]
    fn no_pattern_matches_everything() {
        assert!(test_matches(None, "any", "any", "{}"));
    }

    #[test]
    fn combined_pattern() {
        let pattern = r#"{"source": ["orders"], "detail-type": ["OrderPlaced"]}"#;
        assert!(test_matches(Some(pattern), "orders", "OrderPlaced", "{}"));
        assert!(!test_matches(Some(pattern), "orders", "OrderShipped", "{}"));
        assert!(!test_matches(Some(pattern), "other", "OrderPlaced", "{}"));
    }

    #[test]
    fn nested_detail_pattern() {
        let pattern = r#"{"detail": {"order": {"status": ["PLACED"]}}}"#;
        assert!(test_matches(
            Some(pattern),
            "my.app",
            "OrderEvent",
            r#"{"order": {"status": "PLACED", "id": "123"}}"#
        ));
        assert!(!test_matches(
            Some(pattern),
            "my.app",
            "OrderEvent",
            r#"{"order": {"status": "SHIPPED", "id": "123"}}"#
        ));
        assert!(!test_matches(
            Some(pattern),
            "my.app",
            "OrderEvent",
            r#"{"order": {"id": "123"}}"#
        ));
    }

    #[test]
    fn deeply_nested_detail_pattern() {
        let pattern = r#"{"detail": {"a": {"b": {"c": ["deep"]}}}}"#;
        assert!(test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"a": {"b": {"c": "deep"}}}"#
        ));
        assert!(!test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"a": {"b": {"c": "shallow"}}}"#
        ));
    }

    #[test]
    fn prefix_matcher() {
        let pattern = r#"{"source": [{"prefix": "com.myapp"}]}"#;
        assert!(test_matches(
            Some(pattern),
            "com.myapp.orders",
            "OrderPlaced",
            "{}"
        ));
        assert!(test_matches(
            Some(pattern),
            "com.myapp",
            "OrderPlaced",
            "{}"
        ));
        assert!(!test_matches(
            Some(pattern),
            "com.other",
            "OrderPlaced",
            "{}"
        ));
    }

    #[test]
    fn prefix_matcher_in_detail() {
        let pattern = r#"{"detail": {"region": [{"prefix": "us-"}]}}"#;
        assert!(test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"region": "us-east-1"}"#
        ));
        assert!(!test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"region": "eu-west-1"}"#
        ));
    }

    #[test]
    fn exists_matcher() {
        let pattern = r#"{"detail": {"error": [{"exists": true}]}}"#;
        assert!(test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"error": "something broke"}"#
        ));
        assert!(!test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"status": "ok"}"#
        ));

        let pattern = r#"{"detail": {"error": [{"exists": false}]}}"#;
        assert!(test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"status": "ok"}"#
        ));
        assert!(!test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"error": "something broke"}"#
        ));
    }

    #[test]
    fn anything_but_matcher() {
        let pattern = r#"{"source": [{"anything-but": "internal"}]}"#;
        assert!(test_matches(Some(pattern), "external", "Event", "{}"));
        assert!(!test_matches(Some(pattern), "internal", "Event", "{}"));

        let pattern = r#"{"source": [{"anything-but": ["internal", "test"]}]}"#;
        assert!(test_matches(Some(pattern), "external", "Event", "{}"));
        assert!(!test_matches(Some(pattern), "internal", "Event", "{}"));
        assert!(!test_matches(Some(pattern), "test", "Event", "{}"));
    }

    #[test]
    fn anything_but_in_detail() {
        let pattern = r#"{"detail": {"env": [{"anything-but": "prod"}]}}"#;
        assert!(test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"env": "staging"}"#
        ));
        assert!(!test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"env": "prod"}"#
        ));
    }

    #[test]
    fn numeric_greater_than() {
        let pattern = r#"{"detail": {"count": [{"numeric": [">", 100]}]}}"#;
        assert!(test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 150}"#
        ));
        assert!(!test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 100}"#
        ));
        assert!(!test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 50}"#
        ));
    }

    #[test]
    fn numeric_less_than() {
        let pattern = r#"{"detail": {"count": [{"numeric": ["<", 10]}]}}"#;
        assert!(test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 5}"#
        ));
        assert!(!test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 10}"#
        ));
        assert!(!test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 15}"#
        ));
    }

    #[test]
    fn numeric_range() {
        let pattern = r#"{"detail": {"count": [{"numeric": [">=", 50, "<", 200]}]}}"#;
        assert!(test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 50}"#
        ));
        assert!(test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 100}"#
        ));
        assert!(!test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 200}"#
        ));
        assert!(!test_matches(
            Some(pattern),
            "src",
            "type",
            r#"{"count": 49}"#
        ));
    }

    #[test]
    fn mixed_matchers_and_literals() {
        let pattern = r#"{"source": ["exact.match", {"prefix": "com.myapp"}]}"#;
        assert!(test_matches(Some(pattern), "exact.match", "Event", "{}"));
        assert!(test_matches(
            Some(pattern),
            "com.myapp.orders",
            "Event",
            "{}"
        ));
        assert!(!test_matches(Some(pattern), "other.source", "Event", "{}"));
    }
}
