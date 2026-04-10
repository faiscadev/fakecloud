use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::StatusCode;
use serde_json::{json, Value};

use std::collections::HashMap;
use std::sync::Arc;

use fakecloud_aws::arn::Arn;
use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::pagination::paginate;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;

use fakecloud_lambda::runtime::ContainerRuntime;
use fakecloud_lambda::state::{LambdaInvocation, SharedLambdaState};
use fakecloud_logs::state::SharedLogsState;

use crate::state::{
    ApiDestination, Archive, Connection, Endpoint, EventBus, EventRule, EventTarget,
    PartnerEventSource, PutEvent, Replay, SharedEventBridgeState,
};

pub struct EventBridgeService {
    state: SharedEventBridgeState,
    delivery: Arc<DeliveryBus>,
    lambda_state: Option<SharedLambdaState>,
    logs_state: Option<SharedLogsState>,
    container_runtime: Option<Arc<ContainerRuntime>>,
}

impl EventBridgeService {
    pub fn new(state: SharedEventBridgeState, delivery: Arc<DeliveryBus>) -> Self {
        Self {
            state,
            delivery,
            lambda_state: None,
            logs_state: None,
            container_runtime: None,
        }
    }

    pub fn with_lambda(mut self, lambda_state: SharedLambdaState) -> Self {
        self.lambda_state = Some(lambda_state);
        self
    }

    pub fn with_logs(mut self, logs_state: SharedLogsState) -> Self {
        self.logs_state = Some(logs_state);
        self
    }

    pub fn with_runtime(mut self, runtime: Arc<ContainerRuntime>) -> Self {
        self.container_runtime = Some(runtime);
        self
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
            "DeletePartnerEventSource" => self.delete_partner_event_source(&req),
            "DescribePartnerEventSource" => self.describe_partner_event_source(&req),
            "ListPartnerEventSources" => self.list_partner_event_sources(&req),
            "ListPartnerEventSourceAccounts" => self.list_partner_event_source_accounts(&req),
            "ActivateEventSource" => self.activate_event_source(&req),
            "DeactivateEventSource" => self.deactivate_event_source(&req),
            "DescribeEventSource" => self.describe_event_source(&req),
            "ListEventSources" => self.list_event_sources(&req),
            "PutPartnerEvents" => self.put_partner_events(&req),
            "TestEventPattern" => self.test_event_pattern(&req),
            "UpdateEventBus" => self.update_event_bus(&req),
            "CreateEndpoint" => self.create_endpoint(&req),
            "DeleteEndpoint" => self.delete_endpoint(&req),
            "DescribeEndpoint" => self.describe_endpoint(&req),
            "ListEndpoints" => self.list_endpoints(&req),
            "UpdateEndpoint" => self.update_endpoint(&req),
            "DeauthorizeConnection" => self.deauthorize_connection(&req),
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
            "DeletePartnerEventSource",
            "DescribePartnerEventSource",
            "ListPartnerEventSources",
            "ListPartnerEventSourceAccounts",
            "ActivateEventSource",
            "DeactivateEventSource",
            "DescribeEventSource",
            "ListEventSources",
            "PutPartnerEvents",
            "TestEventPattern",
            "UpdateEventBus",
            "CreateEndpoint",
            "DeleteEndpoint",
            "DescribeEndpoint",
            "ListEndpoints",
            "UpdateEndpoint",
            "DeauthorizeConnection",
        ]
    }
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
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_string_length("name", &name, 1, 256)?;
        validate_optional_string_length(
            "eventSourceName",
            body["EventSourceName"].as_str(),
            1,
            256,
        )?;
        validate_optional_string_length("description", body["Description"].as_str(), 0, 512)?;
        validate_optional_string_length(
            "kmsKeyIdentifier",
            body["KmsKeyIdentifier"].as_str(),
            0,
            2048,
        )?;

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

        Ok(AwsResponse::ok_json(json!({ "EventBusArn": arn })))
    }

    fn delete_event_bus(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_string_length("name", name, 1, 256)?;

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

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_event_buses(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("namePrefix", body["NamePrefix"].as_str(), 1, 256)?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 2048)?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 100)?;
        let name_prefix = body["NamePrefix"].as_str();
        let limit = body["Limit"].as_i64().unwrap_or(100) as usize;
        if let Some(t) = body["NextToken"].as_str() {
            t.parse::<usize>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidNextTokenException",
                    format!("Invalid NextToken value: '{t}'"),
                )
            })?;
        }

        let state = self.state.read();
        let filtered: Vec<&_> = state
            .buses
            .values()
            .filter(|b| match name_prefix {
                Some(prefix) => b.name.starts_with(prefix),
                None => true,
            })
            .collect();

        let (page, next_token) = paginate(&filtered, body["NextToken"].as_str(), limit);
        let buses: Vec<Value> = page
            .iter()
            .map(|b| json!({ "Name": b.name, "Arn": b.arn }))
            .collect();
        let mut resp = json!({ "EventBuses": buses });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    fn describe_event_bus(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("name", body["Name"].as_str(), 1, 1600)?;
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

        Ok(AwsResponse::ok_json(resp))
    }

    // ─── Permission Operations ──────────────────────────────────────────

    fn put_permission(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("eventBusName", body["EventBusName"].as_str(), 1, 256)?;
        validate_optional_string_length("action", body["Action"].as_str(), 1, 64)?;
        validate_optional_string_length("principal", body["Principal"].as_str(), 1, 12)?;
        validate_optional_string_length("statementId", body["StatementId"].as_str(), 1, 64)?;
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
                return Ok(AwsResponse::ok_json(json!({})));
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
            "Principal": { "AWS": Arn::global("iam", principal, "root").to_string() },
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

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn remove_permission(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("statementId", body["StatementId"].as_str(), 1, 64)?;
        validate_optional_string_length("eventBusName", body["EventBusName"].as_str(), 1, 256)?;
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
            return Ok(AwsResponse::ok_json(json!({})));
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

        Ok(AwsResponse::ok_json(json!({})))
    }

    // ─── Rule Operations ────────────────────────────────────────────────

    fn put_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_string_length("name", &name, 1, 64)?;
        validate_optional_string_length("eventBusName", body["EventBusName"].as_str(), 1, 1600)?;
        validate_optional_string_length(
            "scheduleExpression",
            body["ScheduleExpression"].as_str(),
            0,
            256,
        )?;
        validate_optional_string_length("eventPattern", body["EventPattern"].as_str(), 0, 4096)?;
        validate_optional_enum(
            "state",
            body["State"].as_str(),
            &[
                "ENABLED",
                "DISABLED",
                "ENABLED_WITH_ALL_CLOUDTRAIL_MANAGEMENT_EVENTS",
            ],
        )?;
        validate_optional_string_length("description", body["Description"].as_str(), 0, 512)?;
        validate_optional_string_length("roleArn", body["RoleArn"].as_str(), 1, 1600)?;

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
        Ok(AwsResponse::ok_json(json!({ "RuleArn": arn })))
    }

    fn delete_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_string_length("name", name, 1, 64)?;
        validate_optional_string_length("eventBusName", body["EventBusName"].as_str(), 1, 1600)?;
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
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_rules(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("namePrefix", body["NamePrefix"].as_str(), 1, 64)?;
        validate_optional_string_length("eventBusName", body["EventBusName"].as_str(), 1, 1600)?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 2048)?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 100)?;
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
            .unwrap_or(0)
            .min(rules.len());
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

        Ok(AwsResponse::ok_json(resp))
    }

    fn describe_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_string_length("name", name, 1, 64)?;
        validate_optional_string_length("eventBusName", body["EventBusName"].as_str(), 1, 1600)?;
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

        Ok(AwsResponse::ok_json(resp))
    }

    fn enable_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_string_length("name", name, 1, 64)?;
        validate_optional_string_length("eventBusName", body["EventBusName"].as_str(), 1, 1600)?;
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
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn disable_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_string_length("name", name, 1, 64)?;
        validate_optional_string_length("eventBusName", body["EventBusName"].as_str(), 1, 1600)?;
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
        Ok(AwsResponse::ok_json(json!({})))
    }

    // ─── Target Operations ──────────────────────────────────────────────

    fn put_targets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Rule", &body["Rule"])?;
        let rule_name = body["Rule"].as_str().ok_or_else(|| missing("Rule"))?;
        validate_string_length("rule", rule_name, 1, 64)?;
        validate_optional_string_length("eventBusName", body["EventBusName"].as_str(), 1, 1600)?;
        validate_required("Targets", &body["Targets"])?;
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

        Ok(AwsResponse::ok_json(json!({
            "FailedEntryCount": 0,
            "FailedEntries": [],
        })))
    }

    fn remove_targets(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Rule", &body["Rule"])?;
        let rule_name = body["Rule"].as_str().ok_or_else(|| missing("Rule"))?;
        validate_string_length("rule", rule_name, 1, 64)?;
        validate_optional_string_length("eventBusName", body["EventBusName"].as_str(), 1, 1600)?;
        validate_required("Ids", &body["Ids"])?;
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

        Ok(AwsResponse::ok_json(json!({
            "FailedEntryCount": 0,
            "FailedEntries": [],
        })))
    }

    fn list_targets_by_rule(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Rule", &body["Rule"])?;
        let rule_name = body["Rule"].as_str().ok_or_else(|| missing("Rule"))?;
        validate_string_length("rule", rule_name, 1, 64)?;
        validate_optional_string_length("eventBusName", body["EventBusName"].as_str(), 1, 1600)?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 2048)?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 100)?;
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
            .unwrap_or(0)
            .min(all_targets.len());
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

        Ok(AwsResponse::ok_json(resp))
    }

    fn list_rule_names_by_target(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("TargetArn", &body["TargetArn"])?;
        let target_arn = body["TargetArn"]
            .as_str()
            .ok_or_else(|| missing("TargetArn"))?;
        validate_string_length("targetArn", target_arn, 1, 1600)?;
        validate_optional_string_length("eventBusName", body["EventBusName"].as_str(), 1, 1600)?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 2048)?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 100)?;
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
            .unwrap_or(0)
            .min(rule_names.len());
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

        Ok(AwsResponse::ok_json(resp))
    }

    // ─── Partner Event Sources ────────────���───────────────────────────

    fn create_partner_event_source(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_string_length("name", &name, 1, 256)?;
        validate_required("Account", &body["Account"])?;
        let account = body["Account"]
            .as_str()
            .ok_or_else(|| missing("Account"))?
            .to_string();
        validate_string_length("account", &account, 12, 12)?;

        let mut state = self.state.write();
        if state.partner_event_sources.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceAlreadyExistsException",
                format!("Partner event source {name} already exists."),
            ));
        }
        let arn = format!(
            "arn:aws:events:{}::event-source/aws.partner/{}",
            state.region, name
        );
        let now = Utc::now();
        let ps = PartnerEventSource {
            name: name.clone(),
            arn: arn.clone(),
            account,
            creation_time: now,
            expiration_time: None,
            state: "ACTIVE".to_string(),
        };
        state.partner_event_sources.insert(name.clone(), ps);

        Ok(AwsResponse::ok_json(json!({ "EventSourceArn": arn })))
    }

    fn delete_partner_event_source(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_required("Account", &body["Account"])?;
        let account = body["Account"]
            .as_str()
            .ok_or_else(|| missing("Account"))?
            .to_string();

        let mut state = self.state.write();
        match state.partner_event_sources.get(&name) {
            Some(ps) if ps.account == account => {
                state.partner_event_sources.remove(&name);
            }
            Some(_) => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFoundException",
                    format!("Partner event source {name} does not exist for account {account}."),
                ));
            }
            None => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ResourceNotFoundException",
                    format!("Partner event source {name} does not exist."),
                ));
            }
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn describe_partner_event_source(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_string_length("name", &name, 1, 256)?;

        let state = self.state.read();
        let ps = state.partner_event_sources.get(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Partner event source {name} does not exist."),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({
            "Arn": ps.arn,
            "Name": ps.name,
        })))
    }

    fn list_partner_event_sources(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("namePrefix", &body["NamePrefix"])?;
        let name_prefix = body["NamePrefix"]
            .as_str()
            .ok_or_else(|| missing("NamePrefix"))?;
        validate_string_length("namePrefix", name_prefix, 1, 256)?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 100)?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 2048)?;
        let limit = body["Limit"].as_i64().unwrap_or(100) as usize;

        let state = self.state.read();
        let all: Vec<Value> = state
            .partner_event_sources
            .values()
            .filter(|ps| ps.name.starts_with(name_prefix))
            .map(|ps| {
                json!({
                    "Arn": ps.arn,
                    "Name": ps.name,
                })
            })
            .collect();

        let (sources, next_token) = paginate(&all, body["NextToken"].as_str(), limit);
        let mut resp = json!({ "PartnerEventSources": sources });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    fn list_partner_event_source_accounts(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("EventSourceName", &body["EventSourceName"])?;
        let event_source_name = body["EventSourceName"]
            .as_str()
            .ok_or_else(|| missing("EventSourceName"))?;
        validate_string_length("eventSourceName", event_source_name, 1, 256)?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 100)?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 2048)?;

        let state = self.state.read();
        let accounts: Vec<Value> = state
            .partner_event_sources
            .values()
            .filter(|ps| ps.name == event_source_name)
            .map(|ps| json!({ "Account": ps.account }))
            .collect();

        Ok(AwsResponse::ok_json(json!({
            "PartnerEventSourceAccounts": accounts
        })))
    }

    fn activate_event_source(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();

        let mut state = self.state.write();
        let ps = state.partner_event_sources.get_mut(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Event source {name} does not exist."),
            )
        })?;
        ps.state = "ACTIVE".to_string();

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn deactivate_event_source(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();

        let mut state = self.state.write();
        let ps = state.partner_event_sources.get_mut(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Event source {name} does not exist."),
            )
        })?;
        ps.state = "INACTIVE".to_string();

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn describe_event_source(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();

        let state = self.state.read();
        let ps = state.partner_event_sources.get(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Event source {name} does not exist."),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({
            "Arn": ps.arn,
            "Name": ps.name,
            "CreatedBy": ps.account,
            "CreationTime": ps.creation_time.timestamp() as f64,
            "State": ps.state,
        })))
    }

    fn list_event_sources(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("namePrefix", body["NamePrefix"].as_str(), 1, 256)?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 100)?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 2048)?;
        let name_prefix = body["NamePrefix"].as_str();
        let limit = body["Limit"].as_i64().unwrap_or(100) as usize;

        let state = self.state.read();
        let all: Vec<Value> = state
            .partner_event_sources
            .values()
            .filter(|ps| match name_prefix {
                Some(prefix) => ps.name.starts_with(prefix),
                None => true,
            })
            .map(|ps| {
                json!({
                    "Arn": ps.arn,
                    "Name": ps.name,
                    "CreatedBy": ps.account,
                    "CreationTime": ps.creation_time.timestamp() as f64,
                    "State": ps.state,
                })
            })
            .collect();

        let (sources, next_token) = paginate(&all, body["NextToken"].as_str(), limit);
        let mut resp = json!({ "EventSources": sources });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    fn put_partner_events(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Entries", &body["Entries"])?;
        let entries = body["Entries"]
            .as_array()
            .ok_or_else(|| missing("Entries"))?;

        let mut result_entries = Vec::new();
        for _entry in entries {
            let event_id = uuid::Uuid::new_v4().to_string();
            result_entries.push(json!({ "EventId": event_id }));
        }

        Ok(AwsResponse::ok_json(json!({
            "FailedEntryCount": 0,
            "Entries": result_entries,
        })))
    }

    // ─── TestEventPattern ────────────────────────────────────────────────

    fn test_event_pattern(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("EventPattern", &body["EventPattern"])?;
        validate_required("Event", &body["Event"])?;
        let event_pattern = body["EventPattern"]
            .as_str()
            .ok_or_else(|| missing("EventPattern"))?;
        let event_str = body["Event"].as_str().ok_or_else(|| missing("Event"))?;

        // Parse the event JSON
        let event: Value = serde_json::from_str(event_str).map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidEventPatternException",
                "Event is not valid JSON.",
            )
        })?;

        // Parse the pattern JSON
        let _pattern: Value = serde_json::from_str(event_pattern).map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidEventPatternException",
                "Event pattern is not valid JSON.",
            )
        })?;

        let source = event["source"].as_str().unwrap_or("");
        let detail_type = event["detail-type"].as_str().unwrap_or("");
        let detail = event
            .get("detail")
            .map(|v| serde_json::to_string(v).unwrap_or_default())
            .unwrap_or_else(|| "{}".to_string());
        let account = event["account"].as_str().unwrap_or("");
        let region = event["region"].as_str().unwrap_or("");
        let resources: Vec<String> = event["resources"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let result = matches_pattern(
            Some(event_pattern),
            source,
            detail_type,
            &detail,
            account,
            region,
            &resources,
        );

        Ok(AwsResponse::ok_json(json!({ "Result": result })))
    }

    // ─── UpdateEventBus ─────────────────────────────────────────────────

    fn update_event_bus(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("description", body["Description"].as_str(), 0, 512)?;
        validate_optional_string_length(
            "kmsKeyIdentifier",
            body["KmsKeyIdentifier"].as_str(),
            0,
            2048,
        )?;
        let name = body["Name"].as_str().unwrap_or("default");

        let mut state = self.state.write();
        let bus = state.buses.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Event bus {name} does not exist."),
            )
        })?;

        if let Some(desc) = body["Description"].as_str() {
            bus.description = Some(desc.to_string());
        }
        if let Some(kms) = body["KmsKeyIdentifier"].as_str() {
            bus.kms_key_identifier = Some(kms.to_string());
        }
        if let Some(dlc) = body.get("DeadLetterConfig") {
            bus.dead_letter_config = Some(dlc.clone());
        }
        bus.last_modified_time = Utc::now();

        let arn = bus.arn.clone();
        let bus_name = bus.name.clone();

        Ok(AwsResponse::ok_json(json!({
            "Arn": arn,
            "Name": bus_name,
        })))
    }

    // ─── Endpoint Operations ────────────────────────────────────────────

    fn create_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_string_length("name", &name, 1, 64)?;
        validate_required("RoutingConfig", &body["RoutingConfig"])?;
        validate_required("EventBuses", &body["EventBuses"])?;

        let description = body["Description"].as_str().map(|s| s.to_string());
        let routing_config = body["RoutingConfig"].clone();
        let replication_config = body.get("ReplicationConfig").cloned();
        let event_buses = body["EventBuses"].as_array().cloned().unwrap_or_default();
        let role_arn = body["RoleArn"].as_str().map(|s| s.to_string());

        let mut state = self.state.write();
        if state.endpoints.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ResourceAlreadyExistsException",
                format!("Endpoint {name} already exists."),
            ));
        }

        let endpoint_id = format!("{}.abc123", name);
        let arn = format!(
            "arn:aws:events:{}:{}:endpoint/{}",
            req.region, state.account_id, name
        );
        let endpoint_url = format!(
            "https://{}.endpoint.events.{}.amazonaws.com",
            endpoint_id, req.region
        );
        let now = Utc::now();

        let endpoint = Endpoint {
            name: name.clone(),
            arn: arn.clone(),
            endpoint_id: endpoint_id.clone(),
            endpoint_url: Some(endpoint_url),
            description,
            routing_config: routing_config.clone(),
            replication_config: replication_config.clone(),
            event_buses: event_buses.clone(),
            role_arn: role_arn.clone(),
            state: "ACTIVE".to_string(),
            creation_time: now,
            last_modified_time: now,
        };
        state.endpoints.insert(name.clone(), endpoint);

        let mut resp = json!({
            "Name": name,
            "Arn": arn,
            "State": "ACTIVE",
            "RoutingConfig": routing_config,
            "EventBuses": event_buses,
        });
        if let Some(ref rc) = replication_config {
            resp["ReplicationConfig"] = rc.clone();
        }
        if let Some(ref ra) = role_arn {
            resp["RoleArn"] = json!(ra);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    fn delete_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let mut state = self.state.write();
        state.endpoints.remove(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Endpoint '{name}' does not exist."),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn describe_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let state = self.state.read();
        let ep = state.endpoints.get(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Endpoint '{name}' does not exist."),
            )
        })?;

        let mut resp = json!({
            "Name": ep.name,
            "Arn": ep.arn,
            "EndpointId": ep.endpoint_id,
            "State": ep.state,
            "RoutingConfig": ep.routing_config,
            "EventBuses": ep.event_buses,
            "CreationTime": ep.creation_time.timestamp() as f64,
            "LastModifiedTime": ep.last_modified_time.timestamp() as f64,
        });
        if let Some(ref url) = ep.endpoint_url {
            resp["EndpointUrl"] = json!(url);
        }
        if let Some(ref desc) = ep.description {
            resp["Description"] = json!(desc);
        }
        if let Some(ref rc) = ep.replication_config {
            resp["ReplicationConfig"] = rc.clone();
        }
        if let Some(ref ra) = ep.role_arn {
            resp["RoleArn"] = json!(ra);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    fn list_endpoints(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("namePrefix", body["NamePrefix"].as_str(), 1, 64)?;
        validate_optional_string_length("homeRegion", body["HomeRegion"].as_str(), 9, 20)?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 2048)?;
        validate_optional_range_i64("maxResults", body["MaxResults"].as_i64(), 1, 100)?;
        let name_prefix = body["NamePrefix"].as_str();
        let limit = body["MaxResults"].as_i64().unwrap_or(100) as usize;

        let state = self.state.read();
        let all: Vec<Value> = state
            .endpoints
            .values()
            .filter(|ep| match name_prefix {
                Some(prefix) => ep.name.starts_with(prefix),
                None => true,
            })
            .map(|ep| {
                let mut obj = json!({
                    "Name": ep.name,
                    "Arn": ep.arn,
                    "EndpointId": ep.endpoint_id,
                    "State": ep.state,
                    "RoutingConfig": ep.routing_config,
                    "EventBuses": ep.event_buses,
                    "CreationTime": ep.creation_time.timestamp() as f64,
                    "LastModifiedTime": ep.last_modified_time.timestamp() as f64,
                });
                if let Some(ref url) = ep.endpoint_url {
                    obj["EndpointUrl"] = json!(url);
                }
                obj
            })
            .collect();

        let (endpoints, next_token) = paginate(&all, body["NextToken"].as_str(), limit);
        let mut resp = json!({ "Endpoints": endpoints });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    fn update_endpoint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;

        let mut state = self.state.write();
        let ep = state.endpoints.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Endpoint '{name}' does not exist."),
            )
        })?;

        if let Some(desc) = body["Description"].as_str() {
            ep.description = Some(desc.to_string());
        }
        if !body["RoutingConfig"].is_null() {
            ep.routing_config = body["RoutingConfig"].clone();
        }
        if let Some(rc) = body.get("ReplicationConfig") {
            ep.replication_config = Some(rc.clone());
        }
        if let Some(buses) = body["EventBuses"].as_array() {
            ep.event_buses = buses.clone();
        }
        if let Some(ra) = body["RoleArn"].as_str() {
            ep.role_arn = Some(ra.to_string());
        }
        ep.last_modified_time = Utc::now();

        let resp = json!({
            "Name": ep.name,
            "Arn": ep.arn,
            "EndpointId": ep.endpoint_id,
            "State": ep.state,
            "RoutingConfig": ep.routing_config,
            "EventBuses": ep.event_buses,
        });

        Ok(AwsResponse::ok_json(resp))
    }

    // ─── DeauthorizeConnection ──────────────────────────────────────────

    fn deauthorize_connection(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_string_length("name", name, 1, 64)?;

        let mut state = self.state.write();
        let conn = state.connections.get_mut(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Connection '{name}' does not exist."),
            )
        })?;

        conn.connection_state = "DEAUTHORIZING".to_string();
        conn.last_modified_time = Utc::now();

        let resp = json!({
            "ConnectionArn": conn.arn,
            "ConnectionState": conn.connection_state,
            "CreationTime": conn.creation_time.timestamp() as f64,
            "LastModifiedTime": conn.last_modified_time.timestamp() as f64,
            "LastAuthorizedTime": conn.last_authorized_time.timestamp() as f64,
        });

        Ok(AwsResponse::ok_json(resp))
    }

    // ─── PutEvents ──────────────────────────────────────────────────────

    fn put_events(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Entries", &body["Entries"])?;
        validate_optional_string_length("endpointId", body["EndpointId"].as_str(), 1, 50)?;
        let entries = body["Entries"]
            .as_array()
            .ok_or_else(|| missing("Entries"))?;

        // Validate entries count
        if entries.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "1 validation error detected: Value '[PutEventsRequestEntry]' at 'entries' failed to satisfy constraint: Member must have length greater than or equal to 1",
            ));
        }
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
                let (archive_bus, archive_pattern, archive_enabled) = {
                    let a = &state.archives[&akey];
                    (
                        state.resolve_bus_name(&a.event_source_arn),
                        a.event_pattern.clone(),
                        a.state == "ENABLED",
                    )
                };
                if archive_bus == event_bus_name && archive_enabled {
                    // Check if event matches archive's event pattern
                    let pattern_matches = matches_pattern(
                        archive_pattern.as_deref(),
                        &source,
                        &detail_type,
                        &detail,
                        &req.account_id,
                        &req.region,
                        &resources,
                    );
                    if pattern_matches {
                        if let Some(archive) = state.archives.get_mut(&akey) {
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
                } else if arn.contains(":lambda:") {
                    tracing::info!(
                        function_arn = %arn,
                        payload = %body_str,
                        "EventBridge delivering to Lambda function"
                    );
                    let now = Utc::now();
                    let mut state = self.state.write();
                    state
                        .lambda_invocations
                        .push(crate::state::LambdaInvocation {
                            function_arn: arn.clone(),
                            payload: body_str.clone(),
                            timestamp: now,
                        });
                    drop(state);
                    // Record in Lambda state for cross-service visibility
                    if let Some(ref ls) = self.lambda_state {
                        ls.write().invocations.push(LambdaInvocation {
                            function_arn: arn.clone(),
                            payload: body_str.clone(),
                            timestamp: now,
                            source: "aws:events".to_string(),
                        });
                    }
                    // Actually invoke the Lambda function if a container runtime is available
                    invoke_lambda_async(
                        &self.container_runtime,
                        &self.lambda_state,
                        arn,
                        &body_str,
                    );
                } else if arn.contains(":logs:") {
                    tracing::info!(
                        log_group_arn = %arn,
                        payload = %body_str,
                        "EventBridge delivering to CloudWatch Logs"
                    );
                    let now = Utc::now();
                    let mut state = self.state.write();
                    state.log_deliveries.push(crate::state::LogDelivery {
                        log_group_arn: arn.clone(),
                        payload: body_str.clone(),
                        timestamp: now,
                    });
                    drop(state);
                    // Write event to CloudWatch Logs state
                    if let Some(ref log_state) = self.logs_state {
                        deliver_to_logs(log_state, arn, &body_str, now);
                    }
                } else if arn.contains(":kinesis:") {
                    tracing::info!(
                        stream_arn = %arn,
                        "EventBridge delivering to Kinesis stream"
                    );
                    // Use event ID as partition key for even distribution
                    self.delivery.send_to_kinesis(arn, &body_str, &event_id);
                } else if arn.contains(":states:") {
                    tracing::info!(
                        state_machine_arn = %arn,
                        payload = %body_str,
                        "EventBridge delivering to Step Functions (stub)"
                    );
                    let mut state = self.state.write();
                    state
                        .step_function_executions
                        .push(crate::state::StepFunctionExecution {
                            state_machine_arn: arn.clone(),
                            payload: body_str.clone(),
                            timestamp: Utc::now(),
                        });
                } else if arn.contains(":api-destination/") {
                    // ApiDestination target: look up destination + connection, then POST
                    let state = self.state.read();
                    let dest = state.api_destinations.values().find(|d| d.arn == *arn);
                    if let Some(dest) = dest {
                        let url = dest.invocation_endpoint.clone();
                        let method = dest.http_method.clone();
                        let conn = state
                            .connections
                            .values()
                            .find(|c| c.arn == dest.connection_arn)
                            .cloned();
                        drop(state);

                        let payload = body_str.clone();
                        tokio::spawn(async move {
                            let client = reqwest::Client::new();
                            let mut req_builder = match method.as_str() {
                                "GET" => client.get(&url),
                                "PUT" => client.put(&url),
                                "DELETE" => client.delete(&url),
                                "PATCH" => client.patch(&url),
                                "HEAD" => client.head(&url),
                                _ => client.post(&url),
                            };
                            req_builder = req_builder.header("Content-Type", "application/json");

                            // Apply auth from connection
                            if let Some(conn) = conn {
                                req_builder = apply_connection_auth(req_builder, &conn);
                            }

                            let result = req_builder.body(payload).send().await;
                            if let Err(e) = result {
                                tracing::warn!(
                                    endpoint = %url,
                                    error = %e,
                                    "EventBridge ApiDestination delivery failed"
                                );
                            }
                        });
                    }
                } else if arn.starts_with("https://") || arn.starts_with("http://") {
                    // HTTP target — fire-and-forget POST
                    let url = arn.clone();
                    let payload = body_str.clone();
                    tokio::spawn(async move {
                        let client = reqwest::Client::new();
                        let result = client
                            .post(&url)
                            .header("Content-Type", "application/json")
                            .body(payload)
                            .send()
                            .await;
                        if let Err(e) = result {
                            tracing::warn!(
                                endpoint = %url,
                                error = %e,
                                "EventBridge HTTP target delivery failed"
                            );
                        }
                    });
                }
            }
        }

        let resp = json!({
            "FailedEntryCount": failed_count,
            "Entries": result_entries,
        });

        Ok(AwsResponse::ok_json(resp))
    }

    // ─── Tagging ────────────────────────────────────────────────────────

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("ResourceARN", &body["ResourceARN"])?;
        let arn = body["ResourceARN"]
            .as_str()
            .ok_or_else(|| missing("ResourceARN"))?;
        validate_string_length("resourceARN", arn, 1, 1600)?;
        validate_required("Tags", &body["Tags"])?;

        let mut state = self.state.write();
        let tag_map = find_tags_mut(&mut state, arn)?;

        fakecloud_core::tags::apply_tags(tag_map, &body, "Tags", "Key", "Value").map_err(|f| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("{f} must be a list"),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("ResourceARN", &body["ResourceARN"])?;
        let arn = body["ResourceARN"]
            .as_str()
            .ok_or_else(|| missing("ResourceARN"))?;
        validate_string_length("resourceARN", arn, 1, 1600)?;
        validate_required("TagKeys", &body["TagKeys"])?;

        let mut state = self.state.write();
        let tag_map = find_tags_mut(&mut state, arn)?;

        fakecloud_core::tags::remove_tags(tag_map, &body, "TagKeys").map_err(|f| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("{f} must be a list"),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("ResourceARN", &body["ResourceARN"])?;
        let arn = body["ResourceARN"]
            .as_str()
            .ok_or_else(|| missing("ResourceARN"))?;
        validate_string_length("resourceARN", arn, 1, 1600)?;

        let state = self.state.read();
        let tag_map = find_tags(&state, arn)?;

        let tags = fakecloud_core::tags::tags_to_json(tag_map, "Key", "Value");

        Ok(AwsResponse::ok_json(json!({ "Tags": tags })))
    }

    // ─── Archive Operations ─────────────────────────────────────────────

    fn create_archive(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("ArchiveName", &body["ArchiveName"])?;
        let name = body["ArchiveName"]
            .as_str()
            .ok_or_else(|| missing("ArchiveName"))?
            .to_string();
        validate_string_length("archiveName", &name, 1, 48)?;
        validate_required("EventSourceArn", &body["EventSourceArn"])?;
        let event_source_arn = body["EventSourceArn"]
            .as_str()
            .ok_or_else(|| missing("EventSourceArn"))?
            .to_string();
        validate_string_length("eventSourceArn", &event_source_arn, 1, 1600)?;
        validate_optional_string_length("description", body["Description"].as_str(), 0, 512)?;
        validate_optional_string_length("eventPattern", body["EventPattern"].as_str(), 0, 4096)?;
        if let Some(rd) = body["RetentionDays"].as_i64() {
            validate_range_i64("retentionDays", rd, 0, i64::MAX)?;
        }
        let description = body["Description"].as_str().map(|s| s.to_string());
        let event_pattern = body["EventPattern"].as_str().map(|s| s.to_string());
        let retention_days = body["RetentionDays"].as_i64().unwrap_or(0);

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

        // Build the archive target with InputTransformer
        let archive_target = EventTarget {
            id: name.clone(),
            arn: format!("arn:aws:events:{}:::", req.region),
            input: None,
            input_path: None,
            input_transformer: Some(json!({
                "InputPathsMap": {},
                "InputTemplate": format!(
                    "{{\"archive-arn\": \"{}\", \"event\": <aws.events.event.json>, \"ingestion-time\": <aws.events.event.ingestion-time>}}",
                    arn
                )
            })),
            sqs_parameters: None,
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
            targets: vec![archive_target],
            tags: HashMap::new(),
            last_fired: None,
        };
        let key = (bus_name, rule_name);
        state.rules.insert(key, archive_rule);

        Ok(AwsResponse::ok_json(json!({
            "ArchiveArn": arn,
            "CreationTime": now.timestamp() as f64,
            "State": "ENABLED",
        })))
    }

    fn describe_archive(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("ArchiveName", &body["ArchiveName"])?;
        let name = body["ArchiveName"]
            .as_str()
            .ok_or_else(|| missing("ArchiveName"))?;
        validate_string_length("archiveName", name, 1, 48)?;

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

        Ok(AwsResponse::ok_json(resp))
    }

    fn list_archives(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("namePrefix", body["NamePrefix"].as_str(), 1, 48)?;
        validate_optional_string_length(
            "eventSourceArn",
            body["EventSourceArn"].as_str(),
            1,
            1600,
        )?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 2048)?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 100)?;
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

        let limit = body["Limit"].as_i64().unwrap_or(100) as usize;

        let state = self.state.read();
        let all: Vec<Value> = state
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

        let (archives, next_token) = paginate(&all, body["NextToken"].as_str(), limit);
        let mut resp = json!({ "Archives": archives });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    fn update_archive(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("ArchiveName", &body["ArchiveName"])?;
        let name = body["ArchiveName"]
            .as_str()
            .ok_or_else(|| missing("ArchiveName"))?;
        validate_string_length("archiveName", name, 1, 48)?;
        validate_optional_string_length("description", body["Description"].as_str(), 0, 512)?;
        validate_optional_string_length("eventPattern", body["EventPattern"].as_str(), 0, 4096)?;
        if let Some(rd) = body["RetentionDays"].as_i64() {
            validate_range_i64("retentionDays", rd, 0, i64::MAX)?;
        }

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

        Ok(AwsResponse::ok_json(json!({
            "ArchiveArn": archive.arn,
            "CreationTime": archive.creation_time.timestamp() as f64,
            "State": archive.state,
        })))
    }

    fn delete_archive(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("ArchiveName", &body["ArchiveName"])?;
        let name = body["ArchiveName"]
            .as_str()
            .ok_or_else(|| missing("ArchiveName"))?;
        validate_string_length("archiveName", name, 1, 48)?;

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

        Ok(AwsResponse::ok_json(json!({})))
    }

    // ─── Connection Operations ──────────────────────────────────────────

    fn create_connection(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_string_length("name", &name, 1, 64)?;
        validate_optional_string_length("description", body["Description"].as_str(), 0, 512)?;
        validate_required("AuthorizationType", &body["AuthorizationType"])?;
        let description = body["Description"].as_str().map(|s| s.to_string());
        let auth_type = body["AuthorizationType"]
            .as_str()
            .ok_or_else(|| missing("AuthorizationType"))?
            .to_string();
        validate_enum(
            "authorizationType",
            &auth_type,
            &["BASIC", "OAUTH_CLIENT_CREDENTIALS", "API_KEY"],
        )?;
        validate_optional_string_length(
            "kmsKeyIdentifier",
            body["KmsKeyIdentifier"].as_str(),
            0,
            2048,
        )?;
        validate_required("AuthParameters", &body["AuthParameters"])?;
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

        Ok(AwsResponse::ok_json(json!({
            "ConnectionArn": arn,
            "ConnectionState": "AUTHORIZED",
            "CreationTime": now.timestamp() as f64,
            "LastModifiedTime": now.timestamp() as f64,
        })))
    }

    fn describe_connection(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_string_length("name", name, 1, 64)?;

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

        Ok(AwsResponse::ok_json(resp))
    }

    fn list_connections(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("namePrefix", body["NamePrefix"].as_str(), 1, 64)?;
        validate_optional_enum(
            "connectionState",
            body["ConnectionState"].as_str(),
            &[
                "CREATING",
                "UPDATING",
                "DELETING",
                "AUTHORIZED",
                "DEAUTHORIZED",
                "AUTHORIZING",
                "DEAUTHORIZING",
                "ACTIVE",
                "FAILED_CONNECTIVITY",
            ],
        )?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 2048)?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 100)?;

        let name_prefix = body["NamePrefix"].as_str();
        let connection_state = body["ConnectionState"].as_str();
        let limit = body["Limit"].as_i64().unwrap_or(100) as usize;

        let state = self.state.read();
        let all: Vec<Value> = state
            .connections
            .values()
            .filter(|c| {
                if let Some(prefix) = name_prefix {
                    if !c.name.starts_with(prefix) {
                        return false;
                    }
                }
                if let Some(cs) = connection_state {
                    if c.connection_state != cs {
                        return false;
                    }
                }
                true
            })
            .map(|c| {
                json!({
                    "ConnectionArn": c.arn,
                    "Name": c.name,
                    "AuthorizationType": c.authorization_type,
                    "ConnectionState": c.connection_state,
                    "CreationTime": c.creation_time.timestamp() as f64,
                    "LastModifiedTime": c.last_modified_time.timestamp() as f64,
                    "LastAuthorizedTime": c.last_authorized_time.timestamp() as f64,
                })
            })
            .collect();

        let (conns, next_token) = paginate(&all, body["NextToken"].as_str(), limit);
        let mut resp = json!({ "Connections": conns });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    fn update_connection(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_string_length("name", name, 1, 64)?;
        validate_optional_string_length("description", body["Description"].as_str(), 0, 512)?;
        validate_optional_enum(
            "authorizationType",
            body["AuthorizationType"].as_str(),
            &["BASIC", "OAUTH_CLIENT_CREDENTIALS", "API_KEY"],
        )?;

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

        Ok(AwsResponse::ok_json(json!({
            "ConnectionArn": conn.arn,
            "ConnectionState": conn.connection_state,
            "CreationTime": conn.creation_time.timestamp() as f64,
            "LastModifiedTime": conn.last_modified_time.timestamp() as f64,
            "LastAuthorizedTime": conn.last_authorized_time.timestamp() as f64,
        })))
    }

    fn delete_connection(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_string_length("name", name, 1, 64)?;

        let mut state = self.state.write();
        let conn = state.connections.remove(name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Connection '{name}' does not exist."),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({
            "ConnectionArn": conn.arn,
            "ConnectionState": conn.connection_state,
            "CreationTime": conn.creation_time.timestamp() as f64,
            "LastModifiedTime": conn.last_modified_time.timestamp() as f64,
            "LastAuthorizedTime": conn.last_authorized_time.timestamp() as f64,
        })))
    }

    // ─── API Destination Operations ─────────────────────────────────────

    fn create_api_destination(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_string_length("name", &name, 1, 64)?;
        validate_optional_string_length("description", body["Description"].as_str(), 0, 512)?;
        validate_required("ConnectionArn", &body["ConnectionArn"])?;
        let description = body["Description"].as_str().map(|s| s.to_string());
        let connection_arn = body["ConnectionArn"]
            .as_str()
            .ok_or_else(|| missing("ConnectionArn"))?
            .to_string();
        validate_string_length("connectionArn", &connection_arn, 1, 1600)?;
        validate_required("InvocationEndpoint", &body["InvocationEndpoint"])?;
        let endpoint = body["InvocationEndpoint"]
            .as_str()
            .ok_or_else(|| missing("InvocationEndpoint"))?
            .to_string();
        validate_string_length("invocationEndpoint", &endpoint, 1, 2048)?;
        validate_required("HttpMethod", &body["HttpMethod"])?;
        let http_method = body["HttpMethod"]
            .as_str()
            .ok_or_else(|| missing("HttpMethod"))?
            .to_string();
        validate_enum(
            "httpMethod",
            &http_method,
            &["POST", "GET", "HEAD", "OPTIONS", "PUT", "PATCH", "DELETE"],
        )?;
        let rate_limit = body["InvocationRateLimitPerSecond"].as_i64();
        if let Some(r) = rate_limit {
            validate_range_i64("invocationRateLimitPerSecond", r, 1, i64::MAX)?;
        }

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

        Ok(AwsResponse::ok_json(json!({
            "ApiDestinationArn": arn,
            "ApiDestinationState": "ACTIVE",
            "CreationTime": now.timestamp() as f64,
            "LastModifiedTime": now.timestamp() as f64,
        })))
    }

    fn describe_api_destination(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_string_length("name", name, 1, 64)?;

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

        Ok(AwsResponse::ok_json(resp))
    }

    fn list_api_destinations(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("namePrefix", body["NamePrefix"].as_str(), 1, 64)?;
        validate_optional_string_length("connectionArn", body["ConnectionArn"].as_str(), 1, 1600)?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 2048)?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 100)?;

        let name_prefix = body["NamePrefix"].as_str();
        let connection_arn = body["ConnectionArn"].as_str();
        let limit = body["Limit"].as_i64().unwrap_or(100) as usize;

        let state = self.state.read();
        let all: Vec<Value> = state
            .api_destinations
            .values()
            .filter(|d| {
                if let Some(prefix) = name_prefix {
                    if !d.name.starts_with(prefix) {
                        return false;
                    }
                }
                if let Some(arn) = connection_arn {
                    if d.connection_arn != arn {
                        return false;
                    }
                }
                true
            })
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
                if let Some(rate) = d.invocation_rate_limit_per_second {
                    obj["InvocationRateLimitPerSecond"] = json!(rate);
                }
                obj
            })
            .collect();

        let (dests, next_token) = paginate(&all, body["NextToken"].as_str(), limit);
        let mut resp = json!({ "ApiDestinations": dests });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    fn update_api_destination(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_string_length("name", name, 1, 64)?;
        validate_optional_string_length("description", body["Description"].as_str(), 0, 512)?;
        validate_optional_string_length("connectionArn", body["ConnectionArn"].as_str(), 1, 1600)?;
        validate_optional_string_length(
            "invocationEndpoint",
            body["InvocationEndpoint"].as_str(),
            1,
            2048,
        )?;
        validate_optional_enum(
            "httpMethod",
            body["HttpMethod"].as_str(),
            &["POST", "GET", "HEAD", "OPTIONS", "PUT", "PATCH", "DELETE"],
        )?;
        if let Some(r) = body["InvocationRateLimitPerSecond"].as_i64() {
            validate_range_i64("invocationRateLimitPerSecond", r, 1, i64::MAX)?;
        }

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

        Ok(AwsResponse::ok_json(json!({
            "ApiDestinationArn": dest.arn,
            "ApiDestinationState": dest.state,
            "CreationTime": dest.creation_time.timestamp() as f64,
            "LastModifiedTime": dest.last_modified_time.timestamp() as f64,
        })))
    }

    fn delete_api_destination(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("Name", &body["Name"])?;
        let name = body["Name"].as_str().ok_or_else(|| missing("Name"))?;
        validate_string_length("name", name, 1, 64)?;

        let mut state = self.state.write();
        if !state.api_destinations.contains_key(name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("An api-destination '{name}' does not exist."),
            ));
        }
        state.api_destinations.remove(name);

        Ok(AwsResponse::ok_json(json!({})))
    }

    // ─── Replay Operations ──────────────────────────────────────────────

    fn start_replay(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("ReplayName", &body["ReplayName"])?;
        let name = body["ReplayName"]
            .as_str()
            .ok_or_else(|| missing("ReplayName"))?
            .to_string();
        validate_string_length("replayName", &name, 1, 64)?;
        validate_optional_string_length("description", body["Description"].as_str(), 0, 512)?;
        validate_required("EventSourceArn", &body["EventSourceArn"])?;
        let description = body["Description"].as_str().map(|s| s.to_string());
        let event_source_arn = body["EventSourceArn"]
            .as_str()
            .ok_or_else(|| missing("EventSourceArn"))?
            .to_string();
        validate_string_length("eventSourceArn", &event_source_arn, 1, 1600)?;
        validate_required("EventStartTime", &body["EventStartTime"])?;
        validate_required("EventEndTime", &body["EventEndTime"])?;
        validate_required("Destination", &body["Destination"])?;
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

        // Collect archived events within the replay time range
        let archive = state.archives.get(&archive_name).unwrap();
        let replay_events: Vec<PutEvent> = archive
            .events
            .iter()
            .filter(|e| e.time >= event_start_time && e.time < event_end_time)
            .cloned()
            .collect();

        // Find matching rules and their targets for each replayed event
        let mut events_to_deliver: Vec<(PutEvent, Vec<EventTarget>)> = Vec::new();

        for event in &replay_events {
            let matching_targets: Vec<EventTarget> = state
                .rules
                .values()
                .filter(|r| {
                    r.event_bus_name == bus_name
                        && r.state == "ENABLED"
                        && matches_pattern(
                            r.event_pattern.as_deref(),
                            &event.source,
                            &event.detail_type,
                            &event.detail,
                            &req.account_id,
                            &req.region,
                            &event.resources,
                        )
                })
                .flat_map(|r| r.targets.clone())
                .collect();

            if !matching_targets.is_empty() {
                events_to_deliver.push((event.clone(), matching_targets));
            }
        }

        let replay = Replay {
            name: name.clone(),
            arn: arn.clone(),
            description,
            event_source_arn,
            destination,
            event_start_time,
            event_end_time,
            state: "COMPLETED".to_string(),
            replay_start_time: now,
            replay_end_time: Some(now),
        };
        state.replays.insert(name, replay);

        // Drop the lock before delivering
        drop(state);

        // Deliver replayed events to targets (same logic as PutEvents)
        for (event, targets) in events_to_deliver {
            let detail_value: Value = serde_json::from_str(&event.detail).unwrap_or(json!({}));
            let event_json = json!({
                "version": "0",
                "id": event.event_id,
                "source": event.source,
                "account": req.account_id,
                "detail-type": event.detail_type,
                "detail": detail_value,
                "time": event.time.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
                "region": req.region,
                "resources": event.resources,
                "replay-name": arn,
            });
            let event_str = event_json.to_string();

            for target in targets {
                let target_arn = &target.arn;
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

                if target_arn.contains(":sqs:") {
                    let group_id = target
                        .sqs_parameters
                        .as_ref()
                        .and_then(|p| p["MessageGroupId"].as_str())
                        .map(|s| s.to_string());
                    if group_id.is_some() {
                        self.delivery.send_to_sqs_with_attrs(
                            target_arn,
                            &body_str,
                            &HashMap::new(),
                            group_id.as_deref(),
                            None,
                        );
                    } else {
                        self.delivery
                            .send_to_sqs(target_arn, &body_str, &HashMap::new());
                    }
                } else if target_arn.contains(":sns:") {
                    self.delivery
                        .publish_to_sns(target_arn, &body_str, Some(&event.detail_type));
                } else if target_arn.contains(":lambda:") {
                    let mut state = self.state.write();
                    state
                        .lambda_invocations
                        .push(crate::state::LambdaInvocation {
                            function_arn: target_arn.clone(),
                            payload: body_str.clone(),
                            timestamp: Utc::now(),
                        });
                    drop(state);
                    if let Some(ref ls) = self.lambda_state {
                        ls.write().invocations.push(LambdaInvocation {
                            function_arn: target_arn.clone(),
                            payload: body_str.clone(),
                            timestamp: Utc::now(),
                            source: "aws:events".to_string(),
                        });
                    }
                    invoke_lambda_async(
                        &self.container_runtime,
                        &self.lambda_state,
                        target_arn,
                        &body_str,
                    );
                } else if target_arn.contains(":logs:") {
                    let mut state = self.state.write();
                    state.log_deliveries.push(crate::state::LogDelivery {
                        log_group_arn: target_arn.clone(),
                        payload: body_str.clone(),
                        timestamp: Utc::now(),
                    });
                    drop(state);
                    if let Some(ref log_state) = self.logs_state {
                        deliver_to_logs(log_state, target_arn, &body_str, Utc::now());
                    }
                } else if target_arn.contains(":states:") {
                    let mut state = self.state.write();
                    state
                        .step_function_executions
                        .push(crate::state::StepFunctionExecution {
                            state_machine_arn: target_arn.clone(),
                            payload: body_str.clone(),
                            timestamp: Utc::now(),
                        });
                } else if target_arn.starts_with("https://") || target_arn.starts_with("http://") {
                    let url = target_arn.clone();
                    let payload = body_str.clone();
                    tokio::spawn(async move {
                        let client = reqwest::Client::new();
                        let _ = client
                            .post(&url)
                            .header("Content-Type", "application/json")
                            .body(payload)
                            .send()
                            .await;
                    });
                }
            }
        }

        Ok(AwsResponse::ok_json(json!({
            "ReplayArn": arn,
            "ReplayStartTime": now.timestamp() as f64,
            "State": "STARTING",
        })))
    }

    fn describe_replay(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("ReplayName", &body["ReplayName"])?;
        let name = body["ReplayName"]
            .as_str()
            .ok_or_else(|| missing("ReplayName"))?;
        validate_string_length("replayName", name, 1, 64)?;

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

        Ok(AwsResponse::ok_json(resp))
    }

    fn list_replays(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("namePrefix", body["NamePrefix"].as_str(), 1, 64)?;
        validate_optional_string_length(
            "eventSourceArn",
            body["EventSourceArn"].as_str(),
            1,
            1600,
        )?;
        validate_optional_string_length("nextToken", body["NextToken"].as_str(), 1, 2048)?;
        validate_optional_range_i64("limit", body["Limit"].as_i64(), 1, 100)?;
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

        let limit = body["Limit"].as_i64().unwrap_or(100) as usize;

        let state = self.state.read();
        let all: Vec<Value> = state
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

        let (replays, next_token) = paginate(&all, body["NextToken"].as_str(), limit);
        let mut resp = json!({ "Replays": replays });
        if let Some(token) = next_token {
            resp["NextToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(resp))
    }

    fn cancel_replay(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_required("ReplayName", &body["ReplayName"])?;
        let name = body["ReplayName"]
            .as_str()
            .ok_or_else(|| missing("ReplayName"))?;
        validate_string_length("replayName", name, 1, 64)?;

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

        Ok(AwsResponse::ok_json(json!({
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
pub fn matches_pattern(
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

/// Extract a Lambda function name from its ARN.
///
/// Handles both unqualified (`arn:aws:lambda:region:account:function:NAME`)
/// and qualified (`arn:aws:lambda:region:account:function:NAME:alias`) ARNs.
fn function_name_from_arn(arn: &str) -> &str {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() >= 7 && parts[5] == "function" {
        parts[6]
    } else {
        arn
    }
}

/// Spawn a background task to invoke a Lambda function via ContainerRuntime.
/// This is fire-and-forget: EventBridge delivery is asynchronous.
pub fn invoke_lambda_async(
    container_runtime: &Option<Arc<ContainerRuntime>>,
    lambda_state: &Option<SharedLambdaState>,
    function_arn: &str,
    payload: &str,
) {
    let runtime = match container_runtime {
        Some(rt) => rt.clone(),
        None => return,
    };
    let lambda_state = match lambda_state {
        Some(ls) => ls.clone(),
        None => return,
    };
    let func_name = function_name_from_arn(function_arn).to_string();
    let payload = payload.as_bytes().to_vec();

    tokio::spawn(async move {
        let func = {
            let state = lambda_state.read();
            state.functions.get(&func_name).cloned()
        };
        let func = match func {
            Some(f) => f,
            None => {
                tracing::warn!(
                    function = %func_name,
                    "EventBridge Lambda target not found, skipping invocation"
                );
                return;
            }
        };
        match runtime.invoke(&func, &payload).await {
            Ok(_) => {
                tracing::info!(function = %func_name, "EventBridge Lambda invocation succeeded");
            }
            Err(e) => {
                tracing::warn!(
                    function = %func_name,
                    error = %e,
                    "EventBridge Lambda invocation failed"
                );
            }
        }
    });
}

/// Deliver an EventBridge event to CloudWatch Logs by writing a log event
/// to the appropriate log group and stream.
pub fn deliver_to_logs(
    logs_state: &SharedLogsState,
    log_group_arn: &str,
    payload: &str,
    timestamp: chrono::DateTime<chrono::Utc>,
) {
    // Extract log group name from ARN: arn:aws:logs:region:account:log-group:NAME
    // or just the name if it's not an ARN
    let group_name = if log_group_arn.contains(":log-group:") {
        log_group_arn
            .split(":log-group:")
            .nth(1)
            .unwrap_or(log_group_arn)
            .trim_end_matches(":*")
    } else {
        log_group_arn
    };

    let stream_name = "events".to_string();
    let ts_millis = timestamp.timestamp_millis();

    let mut state = logs_state.write();
    let region = state.region.clone();
    let account_id = state.account_id.clone();

    // Auto-create log group and stream if they don't exist
    let group = state
        .log_groups
        .entry(group_name.to_string())
        .or_insert_with(|| fakecloud_logs::state::LogGroup {
            name: group_name.to_string(),
            arn: Arn::new(
                "logs",
                &region,
                &account_id,
                &format!("log-group:{group_name}"),
            )
            .to_string(),
            creation_time: ts_millis,
            retention_in_days: None,
            kms_key_id: None,
            tags: HashMap::new(),
            log_streams: HashMap::new(),
            stored_bytes: 0,
            subscription_filters: Vec::new(),
            data_protection_policy: None,
            index_policies: Vec::new(),
            transformer: None,
            deletion_protection: false,
        });

    let stream = group
        .log_streams
        .entry(stream_name.clone())
        .or_insert_with(|| fakecloud_logs::state::LogStream {
            name: stream_name,
            arn: format!("{}:log-stream:events", group.arn),
            creation_time: ts_millis,
            first_event_timestamp: None,
            last_event_timestamp: None,
            last_ingestion_time: None,
            upload_sequence_token: "1".to_string(),
            events: Vec::new(),
        });

    stream.events.push(fakecloud_logs::state::LogEvent {
        timestamp: ts_millis,
        message: payload.to_string(),
        ingestion_time: ts_millis,
    });
    stream.last_event_timestamp = Some(ts_millis);
    stream.last_ingestion_time = Some(ts_millis);
    if stream.first_event_timestamp.is_none() {
        stream.first_event_timestamp = Some(ts_millis);
    }
}

/// Apply connection auth parameters to an outgoing HTTP request.
fn apply_connection_auth(
    mut builder: reqwest::RequestBuilder,
    conn: &Connection,
) -> reqwest::RequestBuilder {
    match conn.authorization_type.as_str() {
        "API_KEY" => {
            if let Some(params) = conn.auth_parameters.get("ApiKeyAuthParameters") {
                if let (Some(name), Some(value)) = (
                    params["ApiKeyName"].as_str(),
                    params["ApiKeyValue"].as_str(),
                ) {
                    builder = builder.header(name, value);
                }
            }
        }
        "BASIC" => {
            if let Some(params) = conn.auth_parameters.get("BasicAuthParameters") {
                if let (Some(user), Some(pass)) =
                    (params["Username"].as_str(), params["Password"].as_str())
                {
                    builder = builder.basic_auth(user, Some(pass));
                }
            }
        }
        "OAUTH_CLIENT_CREDENTIALS" => {
            // For OAuth, in a real implementation we'd exchange credentials for a token.
            // Here we pass client credentials as basic auth as a reasonable approximation.
            if let Some(params) = conn.auth_parameters.get("OAuthParameters") {
                if let (Some(client_id), Some(client_secret)) = (
                    params["ClientParameters"]["ClientID"].as_str(),
                    params["ClientParameters"]["ClientSecret"].as_str(),
                ) {
                    builder = builder.basic_auth(client_id, Some(client_secret));
                }
            }
        }
        _ => {}
    }
    builder
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

    // ---- list_connections / list_api_destinations filtering & pagination ----

    use crate::state::EventBridgeState;
    use fakecloud_core::delivery::DeliveryBus;
    use parking_lot::RwLock;

    fn make_service() -> EventBridgeService {
        let state = Arc::new(RwLock::new(EventBridgeState::new(
            "123456789012",
            "us-east-1",
        )));
        let delivery = Arc::new(DeliveryBus::new());
        EventBridgeService::new(state, delivery)
    }

    fn make_request(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "events".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-id".to_string(),
            headers: http::HeaderMap::new(),
            query_params: HashMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        }
    }

    fn create_connection(svc: &EventBridgeService, name: &str) {
        let req = make_request(
            "CreateConnection",
            json!({
                "Name": name,
                "AuthorizationType": "API_KEY",
                "AuthParameters": {
                    "ApiKeyAuthParameters": {
                        "ApiKeyName": "x-api-key",
                        "ApiKeyValue": "secret"
                    }
                }
            }),
        );
        svc.create_connection(&req).unwrap();
    }

    fn create_api_destination(svc: &EventBridgeService, name: &str, conn_name: &str) {
        let conn_arn_field = {
            let state = svc.state.read();
            state.connections.get(conn_name).unwrap().arn.clone()
        };
        let req = make_request(
            "CreateApiDestination",
            json!({
                "Name": name,
                "ConnectionArn": conn_arn_field,
                "InvocationEndpoint": "https://example.com",
                "HttpMethod": "POST"
            }),
        );
        svc.create_api_destination(&req).unwrap();
    }

    // -- ListConnections tests --

    #[test]
    fn list_connections_returns_all_by_default() {
        let svc = make_service();
        create_connection(&svc, "conn-alpha");
        create_connection(&svc, "conn-beta");
        create_connection(&svc, "conn-gamma");

        let req = make_request("ListConnections", json!({}));
        let resp = svc.list_connections(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Connections"].as_array().unwrap().len(), 3);
        assert!(body["NextToken"].is_null());
    }

    #[test]
    fn list_connections_name_prefix_filter() {
        let svc = make_service();
        create_connection(&svc, "prod-conn-1");
        create_connection(&svc, "prod-conn-2");
        create_connection(&svc, "dev-conn-1");

        let req = make_request("ListConnections", json!({ "NamePrefix": "prod-" }));
        let resp = svc.list_connections(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let names: Vec<&str> = body["Connections"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["Name"].as_str().unwrap())
            .collect();
        assert_eq!(names.len(), 2);
        assert!(names.iter().all(|n| n.starts_with("prod-")));
    }

    #[test]
    fn list_connections_state_filter() {
        let svc = make_service();
        create_connection(&svc, "conn-a");
        create_connection(&svc, "conn-b");

        // All connections start as AUTHORIZED; change one
        {
            let mut state = svc.state.write();
            state
                .connections
                .get_mut("conn-b")
                .unwrap()
                .connection_state = "DEAUTHORIZED".to_string();
        }

        let req = make_request(
            "ListConnections",
            json!({ "ConnectionState": "AUTHORIZED" }),
        );
        let resp = svc.list_connections(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let conns = body["Connections"].as_array().unwrap();
        assert_eq!(conns.len(), 1);
        assert_eq!(conns[0]["Name"].as_str().unwrap(), "conn-a");
    }

    #[test]
    fn list_connections_pagination() {
        let svc = make_service();
        for i in 0..5 {
            create_connection(&svc, &format!("conn-{i:02}"));
        }

        // First page: limit 2
        let req = make_request("ListConnections", json!({ "Limit": 2 }));
        let resp = svc.list_connections(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Connections"].as_array().unwrap().len(), 2);
        let token = body["NextToken"].as_str().unwrap();
        assert_eq!(token, "2");

        // Second page
        let req = make_request("ListConnections", json!({ "Limit": 2, "NextToken": token }));
        let resp = svc.list_connections(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Connections"].as_array().unwrap().len(), 2);
        let token = body["NextToken"].as_str().unwrap();
        assert_eq!(token, "4");

        // Third page (only 1 remaining)
        let req = make_request("ListConnections", json!({ "Limit": 2, "NextToken": token }));
        let resp = svc.list_connections(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Connections"].as_array().unwrap().len(), 1);
        assert!(body["NextToken"].is_null());
    }

    #[test]
    fn list_connections_pagination_with_filter() {
        let svc = make_service();
        for i in 0..4 {
            create_connection(&svc, &format!("prod-{i:02}"));
        }
        create_connection(&svc, "dev-00");

        let req = make_request(
            "ListConnections",
            json!({ "NamePrefix": "prod-", "Limit": 2 }),
        );
        let resp = svc.list_connections(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Connections"].as_array().unwrap().len(), 2);
        assert!(body["NextToken"].as_str().is_some());
    }

    // -- ListApiDestinations tests --

    #[test]
    fn list_api_destinations_returns_all_by_default() {
        let svc = make_service();
        create_connection(&svc, "my-conn");
        create_api_destination(&svc, "dest-alpha", "my-conn");
        create_api_destination(&svc, "dest-beta", "my-conn");

        let req = make_request("ListApiDestinations", json!({}));
        let resp = svc.list_api_destinations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ApiDestinations"].as_array().unwrap().len(), 2);
        assert!(body["NextToken"].is_null());
    }

    #[test]
    fn list_api_destinations_name_prefix_filter() {
        let svc = make_service();
        create_connection(&svc, "my-conn");
        create_api_destination(&svc, "prod-dest-1", "my-conn");
        create_api_destination(&svc, "prod-dest-2", "my-conn");
        create_api_destination(&svc, "dev-dest-1", "my-conn");

        let req = make_request("ListApiDestinations", json!({ "NamePrefix": "prod-" }));
        let resp = svc.list_api_destinations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let names: Vec<&str> = body["ApiDestinations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["Name"].as_str().unwrap())
            .collect();
        assert_eq!(names.len(), 2);
        assert!(names.iter().all(|n| n.starts_with("prod-")));
    }

    #[test]
    fn list_api_destinations_connection_arn_filter() {
        let svc = make_service();
        create_connection(&svc, "conn-a");
        create_connection(&svc, "conn-b");
        create_api_destination(&svc, "dest-1", "conn-a");
        create_api_destination(&svc, "dest-2", "conn-b");
        create_api_destination(&svc, "dest-3", "conn-a");

        let conn_a_arn = {
            let state = svc.state.read();
            state.connections.get("conn-a").unwrap().arn.clone()
        };

        let req = make_request(
            "ListApiDestinations",
            json!({ "ConnectionArn": conn_a_arn }),
        );
        let resp = svc.list_api_destinations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        let names: Vec<&str> = body["ApiDestinations"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["Name"].as_str().unwrap())
            .collect();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"dest-1"));
        assert!(names.contains(&"dest-3"));
    }

    #[test]
    fn list_api_destinations_pagination() {
        let svc = make_service();
        create_connection(&svc, "my-conn");
        for i in 0..5 {
            create_api_destination(&svc, &format!("dest-{i:02}"), "my-conn");
        }

        // First page
        let req = make_request("ListApiDestinations", json!({ "Limit": 2 }));
        let resp = svc.list_api_destinations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ApiDestinations"].as_array().unwrap().len(), 2);
        let token = body["NextToken"].as_str().unwrap();
        assert_eq!(token, "2");

        // Second page
        let req = make_request(
            "ListApiDestinations",
            json!({ "Limit": 2, "NextToken": token }),
        );
        let resp = svc.list_api_destinations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ApiDestinations"].as_array().unwrap().len(), 2);
        let token = body["NextToken"].as_str().unwrap();
        assert_eq!(token, "4");

        // Last page
        let req = make_request(
            "ListApiDestinations",
            json!({ "Limit": 2, "NextToken": token }),
        );
        let resp = svc.list_api_destinations(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ApiDestinations"].as_array().unwrap().len(), 1);
        assert!(body["NextToken"].is_null());
    }

    // -- ListEventBuses pagination tests --

    fn create_event_bus(svc: &EventBridgeService, name: &str) {
        let req = make_request("CreateEventBus", json!({ "Name": name }));
        svc.create_event_bus(&req).unwrap();
    }

    #[test]
    fn list_event_buses_pagination() {
        let svc = make_service();
        // "default" bus already exists, create 4 more
        for i in 0..4 {
            create_event_bus(&svc, &format!("bus-{i:02}"));
        }

        // First page: limit 2
        let req = make_request("ListEventBuses", json!({ "Limit": 2 }));
        let resp = svc.list_event_buses(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["EventBuses"].as_array().unwrap().len(), 2);
        let token = body["NextToken"].as_str().unwrap();
        assert_eq!(token, "2");

        // Second page
        let req = make_request("ListEventBuses", json!({ "Limit": 2, "NextToken": token }));
        let resp = svc.list_event_buses(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["EventBuses"].as_array().unwrap().len(), 2);
        let token = body["NextToken"].as_str().unwrap();
        assert_eq!(token, "4");

        // Third page (only 1 remaining)
        let req = make_request("ListEventBuses", json!({ "Limit": 2, "NextToken": token }));
        let resp = svc.list_event_buses(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["EventBuses"].as_array().unwrap().len(), 1);
        assert!(body["NextToken"].is_null());
    }

    #[test]
    fn list_event_buses_no_pagination_returns_all() {
        let svc = make_service();
        create_event_bus(&svc, "bus-alpha");
        create_event_bus(&svc, "bus-beta");

        let req = make_request("ListEventBuses", json!({}));
        let resp = svc.list_event_buses(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        // default + 2 custom = 3
        assert_eq!(body["EventBuses"].as_array().unwrap().len(), 3);
        assert!(body["NextToken"].is_null());
    }

    // -- PutEvents EndpointId tests --

    #[test]
    fn put_events_never_includes_endpoint_id_in_response() {
        let svc = make_service();
        // Even when EndpointId is provided in the request, it must not appear in the response
        let req = make_request(
            "PutEvents",
            json!({
                "EndpointId": "my-endpoint.abc123",
                "Entries": [{
                    "Source": "my.source",
                    "DetailType": "MyType",
                    "Detail": "{}",
                    "EventBusName": "default"
                }]
            }),
        );
        let resp = svc.put_events(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert!(
            !body.as_object().unwrap().contains_key("EndpointId"),
            "EndpointId should never be in the PutEvents response"
        );
        assert_eq!(body["FailedEntryCount"], 0);
    }

    // -- ListArchives pagination tests --

    fn create_archive(svc: &EventBridgeService, name: &str) {
        let req = make_request(
            "CreateArchive",
            json!({
                "ArchiveName": name,
                "EventSourceArn": "arn:aws:events:us-east-1:123456789012:event-bus/default"
            }),
        );
        svc.create_archive(&req).unwrap();
    }

    #[test]
    fn list_archives_pagination() {
        let svc = make_service();
        for i in 0..5 {
            create_archive(&svc, &format!("archive-{i:02}"));
        }

        // First page: limit 2
        let req = make_request("ListArchives", json!({ "Limit": 2 }));
        let resp = svc.list_archives(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Archives"].as_array().unwrap().len(), 2);
        let token = body["NextToken"].as_str().unwrap();
        assert_eq!(token, "2");

        // Second page
        let req = make_request("ListArchives", json!({ "Limit": 2, "NextToken": token }));
        let resp = svc.list_archives(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Archives"].as_array().unwrap().len(), 2);
        let token = body["NextToken"].as_str().unwrap();
        assert_eq!(token, "4");

        // Third page (only 1 remaining)
        let req = make_request("ListArchives", json!({ "Limit": 2, "NextToken": token }));
        let resp = svc.list_archives(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Archives"].as_array().unwrap().len(), 1);
        assert!(body["NextToken"].is_null());
    }

    // -- ListReplays pagination tests --

    fn create_replay(svc: &EventBridgeService, name: &str) {
        // Need an archive first for the replay's event source
        let archive_arn = {
            let state = svc.state.read();
            if state.archives.contains_key("replay-archive") {
                state.archives["replay-archive"].arn.clone()
            } else {
                drop(state);
                create_archive(svc, "replay-archive");
                svc.state.read().archives["replay-archive"].arn.clone()
            }
        };
        let req = make_request(
            "StartReplay",
            json!({
                "ReplayName": name,
                "EventSourceArn": archive_arn,
                "EventStartTime": 1000000.0,
                "EventEndTime": 2000000.0,
                "Destination": {
                    "Arn": "arn:aws:events:us-east-1:123456789012:event-bus/default"
                }
            }),
        );
        svc.start_replay(&req).unwrap();
    }

    #[test]
    fn list_replays_pagination() {
        let svc = make_service();
        for i in 0..5 {
            create_replay(&svc, &format!("replay-{i:02}"));
        }

        // First page: limit 2
        let req = make_request("ListReplays", json!({ "Limit": 2 }));
        let resp = svc.list_replays(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Replays"].as_array().unwrap().len(), 2);
        let token = body["NextToken"].as_str().unwrap();
        assert_eq!(token, "2");

        // Second page
        let req = make_request("ListReplays", json!({ "Limit": 2, "NextToken": token }));
        let resp = svc.list_replays(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Replays"].as_array().unwrap().len(), 2);
        let token = body["NextToken"].as_str().unwrap();
        assert_eq!(token, "4");

        // Third page (only 1 remaining)
        let req = make_request("ListReplays", json!({ "Limit": 2, "NextToken": token }));
        let resp = svc.list_replays(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Replays"].as_array().unwrap().len(), 1);
        assert!(body["NextToken"].is_null());
    }

    #[test]
    fn list_event_buses_invalid_next_token_returns_error() {
        let svc = make_service();

        let req = make_request("ListEventBuses", json!({ "NextToken": "not-a-number" }));
        let result = svc.list_event_buses(&req);
        assert!(
            result.is_err(),
            "non-numeric NextToken should return an error"
        );
    }

    // ---- TestEventPattern tests ----

    #[test]
    fn test_event_pattern_match() {
        let svc = make_service();
        let req = make_request(
            "TestEventPattern",
            json!({
                "EventPattern": r#"{"source": ["my.app"]}"#,
                "Event": r#"{"source": "my.app", "detail-type": "Test", "detail": {}}"#
            }),
        );
        let resp = svc.test_event_pattern(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Result"], true);
    }

    #[test]
    fn test_event_pattern_no_match() {
        let svc = make_service();
        let req = make_request(
            "TestEventPattern",
            json!({
                "EventPattern": r#"{"source": ["other.app"]}"#,
                "Event": r#"{"source": "my.app", "detail-type": "Test", "detail": {}}"#
            }),
        );
        let resp = svc.test_event_pattern(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Result"], false);
    }

    #[test]
    fn test_event_pattern_detail_match() {
        let svc = make_service();
        let req = make_request(
            "TestEventPattern",
            json!({
                "EventPattern": r#"{"detail": {"status": ["PLACED"]}}"#,
                "Event": r#"{"source": "my.app", "detail-type": "Order", "detail": {"status": "PLACED", "id": "123"}}"#
            }),
        );
        let resp = svc.test_event_pattern(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Result"], true);
    }

    // ---- UpdateEventBus tests ----

    #[test]
    fn update_event_bus_description() {
        let svc = make_service();
        create_event_bus(&svc, "my-bus");

        let req = make_request(
            "UpdateEventBus",
            json!({ "Name": "my-bus", "Description": "Updated desc" }),
        );
        let resp = svc.update_event_bus(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Name"], "my-bus");

        // Verify via describe
        let req = make_request("DescribeEventBus", json!({ "Name": "my-bus" }));
        let resp = svc.describe_event_bus(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Description"], "Updated desc");
    }

    #[test]
    fn update_event_bus_not_found() {
        let svc = make_service();
        let req = make_request(
            "UpdateEventBus",
            json!({ "Name": "ghost-bus", "Description": "nope" }),
        );
        assert!(svc.update_event_bus(&req).is_err());
    }

    // ---- Endpoint CRUD tests ----

    fn create_endpoint_helper(svc: &EventBridgeService, name: &str) {
        let req = make_request(
            "CreateEndpoint",
            json!({
                "Name": name,
                "RoutingConfig": {
                    "FailoverConfig": {
                        "Primary": { "HealthCheck": "" },
                        "Secondary": { "Route": "us-west-2" }
                    }
                },
                "EventBuses": [
                    { "EventBusArn": "arn:aws:events:us-east-1:123456789012:event-bus/default" }
                ]
            }),
        );
        svc.create_endpoint(&req).unwrap();
    }

    #[test]
    fn endpoint_create_describe_delete() {
        let svc = make_service();
        create_endpoint_helper(&svc, "my-endpoint");

        // Describe
        let req = make_request("DescribeEndpoint", json!({ "Name": "my-endpoint" }));
        let resp = svc.describe_endpoint(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Name"], "my-endpoint");
        assert_eq!(body["State"], "ACTIVE");
        assert!(body["EndpointId"].as_str().unwrap().contains("my-endpoint"));

        // Delete
        let req = make_request("DeleteEndpoint", json!({ "Name": "my-endpoint" }));
        svc.delete_endpoint(&req).unwrap();

        // Verify gone
        let req = make_request("DescribeEndpoint", json!({ "Name": "my-endpoint" }));
        assert!(svc.describe_endpoint(&req).is_err());
    }

    #[test]
    fn endpoint_list_and_update() {
        let svc = make_service();
        create_endpoint_helper(&svc, "ep-alpha");
        create_endpoint_helper(&svc, "ep-beta");

        // List all
        let req = make_request("ListEndpoints", json!({}));
        let resp = svc.list_endpoints(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Endpoints"].as_array().unwrap().len(), 2);

        // Update
        let req = make_request(
            "UpdateEndpoint",
            json!({ "Name": "ep-alpha", "Description": "updated" }),
        );
        let resp = svc.update_endpoint(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Name"], "ep-alpha");

        // Verify description
        let req = make_request("DescribeEndpoint", json!({ "Name": "ep-alpha" }));
        let resp = svc.describe_endpoint(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Description"], "updated");
    }

    #[test]
    fn endpoint_duplicate_fails() {
        let svc = make_service();
        create_endpoint_helper(&svc, "dup-ep");
        let req = make_request(
            "CreateEndpoint",
            json!({
                "Name": "dup-ep",
                "RoutingConfig": {},
                "EventBuses": []
            }),
        );
        assert!(svc.create_endpoint(&req).is_err());
    }

    // ---- DeauthorizeConnection tests ----

    #[test]
    fn deauthorize_connection_sets_state() {
        let svc = make_service();
        create_connection(&svc, "deauth-conn");

        let req = make_request("DeauthorizeConnection", json!({ "Name": "deauth-conn" }));
        let resp = svc.deauthorize_connection(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ConnectionState"], "DEAUTHORIZING");
        assert!(body["ConnectionArn"]
            .as_str()
            .unwrap()
            .contains("deauth-conn"));

        // Verify via describe
        let req = make_request("DescribeConnection", json!({ "Name": "deauth-conn" }));
        let resp = svc.describe_connection(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["ConnectionState"], "DEAUTHORIZING");
    }

    #[test]
    fn deauthorize_connection_not_found() {
        let svc = make_service();
        let req = make_request("DeauthorizeConnection", json!({ "Name": "ghost-conn" }));
        assert!(svc.deauthorize_connection(&req).is_err());
    }

    // ---- Partner event source tests ----

    #[test]
    fn partner_event_source_crud() {
        let svc = make_service();

        // Create
        let req = make_request(
            "CreatePartnerEventSource",
            json!({ "Name": "partner/test", "Account": "123456789012" }),
        );
        svc.create_partner_event_source(&req).unwrap();

        // Describe
        let req = make_request(
            "DescribePartnerEventSource",
            json!({ "Name": "partner/test" }),
        );
        let resp = svc.describe_partner_event_source(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Name"], "partner/test");

        // List
        let req = make_request("ListPartnerEventSources", json!({"NamePrefix": "partner/"}));
        let resp = svc.list_partner_event_sources(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["PartnerEventSources"].as_array().unwrap().len(), 1);

        // ListPartnerEventSourceAccounts
        let req = make_request(
            "ListPartnerEventSourceAccounts",
            json!({ "EventSourceName": "partner/test" }),
        );
        let resp = svc.list_partner_event_source_accounts(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(
            body["PartnerEventSourceAccounts"].as_array().unwrap().len(),
            1
        );

        // DescribeEventSource
        let req = make_request("DescribeEventSource", json!({ "Name": "partner/test" }));
        let resp = svc.describe_event_source(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["Name"], "partner/test");
        assert_eq!(body["State"], "ACTIVE");

        // ListEventSources
        let req = make_request("ListEventSources", json!({}));
        let resp = svc.list_event_sources(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["EventSources"].as_array().unwrap().len(), 1);

        // Delete
        let req = make_request(
            "DeletePartnerEventSource",
            json!({ "Name": "partner/test", "Account": "123456789012" }),
        );
        svc.delete_partner_event_source(&req).unwrap();

        // Verify gone
        let req = make_request(
            "DescribePartnerEventSource",
            json!({ "Name": "partner/test" }),
        );
        assert!(svc.describe_partner_event_source(&req).is_err());
    }

    #[test]
    fn activate_deactivate_event_source() {
        let svc = make_service();

        // Create a partner event source first
        let req = make_request(
            "CreatePartnerEventSource",
            json!({ "Name": "aws.partner/test", "Account": "123456789012" }),
        );
        svc.create_partner_event_source(&req).unwrap();

        // Deactivate it
        let req = make_request(
            "DeactivateEventSource",
            json!({ "Name": "aws.partner/test" }),
        );
        svc.deactivate_event_source(&req).unwrap();
        {
            let state = svc.state.read();
            assert_eq!(
                state.partner_event_sources["aws.partner/test"].state,
                "INACTIVE"
            );
        }

        // Activate it
        let req = make_request("ActivateEventSource", json!({ "Name": "aws.partner/test" }));
        svc.activate_event_source(&req).unwrap();
        {
            let state = svc.state.read();
            assert_eq!(
                state.partner_event_sources["aws.partner/test"].state,
                "ACTIVE"
            );
        }

        // Not-found returns error
        let req = make_request("ActivateEventSource", json!({ "Name": "nonexistent" }));
        assert!(svc.activate_event_source(&req).is_err());

        let req = make_request("DeactivateEventSource", json!({ "Name": "nonexistent" }));
        assert!(svc.deactivate_event_source(&req).is_err());
    }

    #[test]
    fn delete_partner_event_source_verifies_account() {
        let svc = make_service();

        // Create a partner event source
        let req = make_request(
            "CreatePartnerEventSource",
            json!({ "Name": "aws.partner/test", "Account": "123456789012" }),
        );
        svc.create_partner_event_source(&req).unwrap();

        // Deleting with wrong account fails
        let req = make_request(
            "DeletePartnerEventSource",
            json!({ "Name": "aws.partner/test", "Account": "999999999999" }),
        );
        assert!(svc.delete_partner_event_source(&req).is_err());
        // Source still exists
        assert!(svc
            .state
            .read()
            .partner_event_sources
            .contains_key("aws.partner/test"));

        // Deleting with correct account succeeds
        let req = make_request(
            "DeletePartnerEventSource",
            json!({ "Name": "aws.partner/test", "Account": "123456789012" }),
        );
        svc.delete_partner_event_source(&req).unwrap();
        assert!(!svc
            .state
            .read()
            .partner_event_sources
            .contains_key("aws.partner/test"));

        // Deleting non-existent source returns error
        let req = make_request(
            "DeletePartnerEventSource",
            json!({ "Name": "aws.partner/test", "Account": "123456789012" }),
        );
        assert!(svc.delete_partner_event_source(&req).is_err());
    }

    #[test]
    fn put_partner_events() {
        let svc = make_service();
        let req = make_request(
            "PutPartnerEvents",
            json!({
                "Entries": [
                    { "Source": "partner.app", "DetailType": "Test", "Detail": "{}" }
                ]
            }),
        );
        let resp = svc.put_partner_events(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["FailedEntryCount"], 0);
        assert_eq!(body["Entries"].as_array().unwrap().len(), 1);
        assert!(body["Entries"][0]["EventId"].as_str().is_some());
    }

    // ---- Archive + Replay delivery tests ----

    /// Helper: create a service with a mock SQS delivery that records messages.
    #[allow(clippy::type_complexity)]
    fn make_service_with_sqs_recorder() -> (
        EventBridgeService,
        Arc<parking_lot::Mutex<Vec<(String, String)>>>,
    ) {
        use fakecloud_core::delivery::SqsDelivery;

        struct RecordingSqsDelivery {
            messages: Arc<parking_lot::Mutex<Vec<(String, String)>>>,
        }

        impl SqsDelivery for RecordingSqsDelivery {
            fn deliver_to_queue(
                &self,
                queue_arn: &str,
                message_body: &str,
                _attributes: &HashMap<String, String>,
            ) {
                self.messages
                    .lock()
                    .push((queue_arn.to_string(), message_body.to_string()));
            }
        }

        let messages: Arc<parking_lot::Mutex<Vec<(String, String)>>> =
            Arc::new(parking_lot::Mutex::new(Vec::new()));
        let state = Arc::new(RwLock::new(EventBridgeState::new(
            "123456789012",
            "us-east-1",
        )));
        let delivery = Arc::new(DeliveryBus::new().with_sqs(Arc::new(RecordingSqsDelivery {
            messages: messages.clone(),
        })));
        let svc = EventBridgeService::new(state, delivery);
        (svc, messages)
    }

    #[test]
    fn start_replay_delivers_archived_events_to_sqs_target() {
        let (svc, messages) = make_service_with_sqs_recorder();
        let queue_arn = "arn:aws:sqs:us-east-1:123456789012:replay-queue";

        // Create a rule with an SQS target
        let req = make_request(
            "PutRule",
            json!({
                "Name": "replay-test-rule",
                "EventPattern": r#"{"source": ["my.app"]}"#,
                "State": "ENABLED"
            }),
        );
        svc.put_rule(&req).unwrap();

        let req = make_request(
            "PutTargets",
            json!({
                "Rule": "replay-test-rule",
                "Targets": [{
                    "Id": "sqs-target",
                    "Arn": queue_arn
                }]
            }),
        );
        svc.put_targets(&req).unwrap();

        // Create an archive on the default bus
        let req = make_request(
            "CreateArchive",
            json!({
                "ArchiveName": "test-archive",
                "EventSourceArn": "arn:aws:events:us-east-1:123456789012:event-bus/default"
            }),
        );
        svc.create_archive(&req).unwrap();

        // PutEvents: these should get archived and delivered
        let req = make_request(
            "PutEvents",
            json!({
                "Entries": [
                    {
                        "Source": "my.app",
                        "DetailType": "OrderCreated",
                        "Detail": "{\"orderId\": \"1\"}",
                        "EventBusName": "default"
                    },
                    {
                        "Source": "my.app",
                        "DetailType": "OrderShipped",
                        "Detail": "{\"orderId\": \"2\"}",
                        "EventBusName": "default"
                    }
                ]
            }),
        );
        let resp = svc.put_events(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["FailedEntryCount"], 0);

        // Verify archive has 2 events
        {
            let state = svc.state.read();
            let archive = state.archives.get("test-archive").unwrap();
            assert_eq!(archive.events.len(), 2);
            assert_eq!(archive.event_count, 2);
        }

        // Clear recorded messages from PutEvents delivery
        messages.lock().clear();

        // StartReplay: should re-deliver the archived events
        let archive_arn = {
            let state = svc.state.read();
            state.archives.get("test-archive").unwrap().arn.clone()
        };

        // Use a wide time range to capture all events
        let start_ts = 0.0_f64;
        let end_ts = (chrono::Utc::now().timestamp() + 3600) as f64;

        let req = make_request(
            "StartReplay",
            json!({
                "ReplayName": "my-replay",
                "EventSourceArn": archive_arn,
                "Destination": {
                    "Arn": "arn:aws:events:us-east-1:123456789012:event-bus/default"
                },
                "EventStartTime": start_ts,
                "EventEndTime": end_ts
            }),
        );
        let resp = svc.start_replay(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["State"], "STARTING");

        // Verify the replay delivered events to SQS
        let delivered = messages.lock();
        assert_eq!(
            delivered.len(),
            2,
            "expected 2 replayed events delivered to SQS"
        );
        for (arn, msg) in delivered.iter() {
            assert_eq!(arn, queue_arn);
            let event: Value = serde_json::from_str(msg).unwrap();
            assert_eq!(event["source"], "my.app");
            // Replayed events should include replay-name
            assert!(event["replay-name"].as_str().is_some());
        }

        // Verify replay is marked as COMPLETED
        let state = svc.state.read();
        let replay = state.replays.get("my-replay").unwrap();
        assert_eq!(replay.state, "COMPLETED");
    }

    #[test]
    fn apply_connection_auth_api_key() {
        let conn = Connection {
            name: "test-conn".to_string(),
            arn: "arn:aws:events:us-east-1:123456789012:connection/test-conn/uuid".to_string(),
            description: None,
            authorization_type: "API_KEY".to_string(),
            auth_parameters: json!({
                "ApiKeyAuthParameters": {
                    "ApiKeyName": "x-api-key",
                    "ApiKeyValue": "my-secret"
                }
            }),
            connection_state: "AUTHORIZED".to_string(),
            secret_arn: "arn:aws:secretsmanager:us-east-1:123456789012:secret:test".to_string(),
            creation_time: Utc::now(),
            last_modified_time: Utc::now(),
            last_authorized_time: Utc::now(),
        };

        let client = reqwest::Client::new();
        let builder = client
            .post("http://localhost:12345/test")
            .header("Content-Type", "application/json");
        let builder = apply_connection_auth(builder, &conn);

        // Build and verify the header was applied
        let request = builder.body("{}").build().unwrap();
        assert_eq!(
            request
                .headers()
                .get("x-api-key")
                .unwrap()
                .to_str()
                .unwrap(),
            "my-secret"
        );
    }

    #[test]
    fn apply_connection_auth_basic() {
        let conn = Connection {
            name: "basic-conn".to_string(),
            arn: "arn:aws:events:us-east-1:123456789012:connection/basic-conn/uuid".to_string(),
            description: None,
            authorization_type: "BASIC".to_string(),
            auth_parameters: json!({
                "BasicAuthParameters": {
                    "Username": "user",
                    "Password": "pass"
                }
            }),
            connection_state: "AUTHORIZED".to_string(),
            secret_arn: "arn:aws:secretsmanager:us-east-1:123456789012:secret:test".to_string(),
            creation_time: Utc::now(),
            last_modified_time: Utc::now(),
            last_authorized_time: Utc::now(),
        };

        let client = reqwest::Client::new();
        let builder = client.post("http://localhost:12345/test");
        let builder = apply_connection_auth(builder, &conn);

        let request = builder.body("{}").build().unwrap();
        let auth_header = request
            .headers()
            .get("authorization")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            auth_header.starts_with("Basic "),
            "Expected Basic auth header, got: {auth_header}"
        );
    }

    #[tokio::test]
    async fn put_events_with_api_destination_target_resolves_destination() {
        // This test verifies that the PutEvents code path correctly identifies
        // api-destination ARN targets and resolves the destination metadata.
        // The actual HTTP call goes to a non-existent host (fire-and-forget).
        let state = Arc::new(RwLock::new(EventBridgeState::new(
            "123456789012",
            "us-east-1",
        )));
        let delivery = Arc::new(DeliveryBus::new());
        let svc = EventBridgeService::new(state, delivery);

        // Create connection and api destination
        create_connection(&svc, "my-conn");
        let conn_arn = {
            let state = svc.state.read();
            state.connections.get("my-conn").unwrap().arn.clone()
        };
        let req = make_request(
            "CreateApiDestination",
            json!({
                "Name": "my-dest",
                "ConnectionArn": conn_arn,
                "InvocationEndpoint": "http://127.0.0.1:1/noop",
                "HttpMethod": "POST"
            }),
        );
        svc.create_api_destination(&req).unwrap();

        let dest_arn = {
            let state = svc.state.read();
            state.api_destinations.get("my-dest").unwrap().arn.clone()
        };

        // Create a rule that targets the api-destination
        let req = make_request(
            "PutRule",
            json!({
                "Name": "api-dest-rule",
                "EventPattern": r#"{"source":["test.app"]}"#,
                "State": "ENABLED"
            }),
        );
        svc.put_rule(&req).unwrap();

        let req = make_request(
            "PutTargets",
            json!({
                "Rule": "api-dest-rule",
                "Targets": [{ "Id": "dest-target", "Arn": dest_arn }]
            }),
        );
        svc.put_targets(&req).unwrap();

        // PutEvents - should match the rule and attempt delivery to ApiDestination
        let req = make_request(
            "PutEvents",
            json!({
                "Entries": [{
                    "Source": "test.app",
                    "DetailType": "TestEvent",
                    "Detail": r#"{"key":"value"}"#
                }]
            }),
        );
        let resp = svc.put_events(&req).unwrap();
        let body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(body["FailedEntryCount"], 0);
        assert_eq!(body["Entries"].as_array().unwrap().len(), 1);
        assert!(body["Entries"][0]["EventId"].as_str().is_some());
    }

    #[test]
    fn test_function_name_from_arn() {
        // Unqualified ARN
        assert_eq!(
            super::function_name_from_arn("arn:aws:lambda:us-east-1:123456789012:function:my-func"),
            "my-func"
        );
        // Qualified ARN with alias
        assert_eq!(
            super::function_name_from_arn(
                "arn:aws:lambda:us-east-1:123456789012:function:my-func:prod"
            ),
            "my-func"
        );
        // Qualified ARN with version
        assert_eq!(
            super::function_name_from_arn(
                "arn:aws:lambda:us-east-1:123456789012:function:my-func:42"
            ),
            "my-func"
        );
        // Plain function name (not an ARN)
        assert_eq!(super::function_name_from_arn("my-func"), "my-func");
    }
}
