use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::StatusCode;
use serde_json::{json, Value};

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use tokio::sync::Mutex as AsyncMutex;

use fakecloud_aws::arn::Arn;
use fakecloud_core::delivery::DeliveryBus;
use fakecloud_core::pagination::paginate;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;
use fakecloud_persistence::SnapshotStore;

use fakecloud_lambda::runtime::ContainerRuntime;
use fakecloud_lambda::{LambdaInvocation, SharedLambdaState};
use fakecloud_logs::SharedLogsState;

use crate::state::{
    ApiDestination, Archive, Connection, Endpoint, EventBridgeSnapshot, EventBridgeState, EventBus,
    EventRule, EventTarget, PartnerEventSource, PutEvent, Replay, SharedEventBridgeState,
    EVENTBRIDGE_SNAPSHOT_SCHEMA_VERSION,
};

pub struct EventBridgeService {
    state: SharedEventBridgeState,
    delivery: Arc<DeliveryBus>,
    lambda_state: Option<SharedLambdaState>,
    logs_state: Option<SharedLogsState>,
    container_runtime: Option<Arc<ContainerRuntime>>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl EventBridgeService {
    pub fn new(state: SharedEventBridgeState, delivery: Arc<DeliveryBus>) -> Self {
        Self {
            state,
            delivery,
            lambda_state: None,
            logs_state: None,
            container_runtime: None,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
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

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes,
    /// with serde + file I/O offloaded to the blocking pool.
    async fn save_snapshot(&self) {
        save_eventbridge_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current EventBridge state when invoked, or
    /// `None` in memory mode (no snapshot store). The CloudFormation provisioner
    /// mutates `state` directly and uses this to write a CFN-provisioned
    /// resource through to disk, the same way a direct mutating API call would.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_eventbridge_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }
}

/// Persist the current EventBridge state as a snapshot. Offloads the serde +
/// blocking file write to the Tokio blocking pool. Noop when `store` is `None`
/// (memory mode). Shared by `EventBridgeService::save_snapshot` and the
/// CloudFormation provisioner's post-provision persist hook so both route
/// through the same serialize-and-write path.
pub async fn save_eventbridge_snapshot(
    state: &SharedEventBridgeState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = EventBridgeSnapshot {
        schema_version: EVENTBRIDGE_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().clone()),
        state: None,
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write eventbridge snapshot"),
        Err(err) => tracing::error!(%err, "eventbridge snapshot task panicked"),
    }
}

#[async_trait]
impl AwsService for EventBridgeService {
    fn service_name(&self) -> &str {
        "events"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating_action(req.action.as_str());
        let result = match req.action.as_str() {
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
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
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
            let accounts_r = self.state.read();
            let empty_r = EventBridgeState::new(&req.account_id, &req.region);
            let state_r = accounts_r.get(&req.account_id).unwrap_or(&empty_r);
            let has_source = state_r.partner_event_sources.contains_key(event_source);
            drop(accounts_r);
            if !has_source {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Event source {event_source} does not exist."),
                ));
            }
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.buses.remove(name);
        state.rules.retain(|k, _| k.0 != name);

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_event_buses(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy ListEventBusesRequest constraints:
        //   NamePrefix: EventBusName length 1..=256
        //   NextToken: length 1..=2048
        //   Limit: LimitMax100 range 1..=100
        // Unrecognised pagination tokens still fall back to the start of the
        // list — `InvalidNextTokenException` only fires when the token shape
        // itself is wrong, not when it points at a vanished cursor.
        validate_optional_string_length_value("NamePrefix", &body["NamePrefix"], 1, 256)?;
        validate_optional_string_length_value("NextToken", &body["NextToken"], 1, 2048)?;
        validate_optional_json_range("Limit", &body["Limit"], 1, 100)?;
        let name_prefix = body["NamePrefix"].as_str();
        let limit = body["Limit"].as_i64().unwrap_or(100).clamp(1, 100) as usize;

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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
            .map(|b| {
                // Mirror DescribeEventBus: ListEventBuses also reports the
                // bus creation/last-modified times, which the Terraform
                // aws_cloudwatch_event_buses data source expects to be set.
                json!({
                    "Name": b.name,
                    "Arn": b.arn,
                    "CreationTime": b.creation_time.timestamp() as f64,
                    "LastModifiedTime": b.last_modified_time.timestamp() as f64,
                })
            })
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

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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
        // Smithy PutPermissionRequest constraints (optional members but each
        // carries a `@length` trait that must be honoured when present):
        //   EventBusName: NonPartnerEventBusName length 1..=256
        //   Action: length 1..=64
        //   Principal: length 1..=12 (12-digit AWS account or `*`)
        //   StatementId: length 1..=64
        validate_optional_string_length_value("EventBusName", &body["EventBusName"], 1, 256)?;
        validate_optional_string_length_value("Action", &body["Action"], 1, 64)?;
        validate_optional_string_length_value("Principal", &body["Principal"], 1, 12)?;
        validate_optional_string_length_value("StatementId", &body["StatementId"], 1, 64)?;
        let event_bus_name = body["EventBusName"].as_str().unwrap_or("default");

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

        // Old-style: Action, Principal, StatementId. All are @optional in the
        // Smithy model; non-string values were already rejected above with
        // SerializationException, so reaching here means each is either a
        // valid string or absent. Fall back to "" to preserve current behavior
        // — recording an empty-statement policy entry is harmless since it can
        // never match a real action/principal pair.
        let action = body["Action"].as_str().unwrap_or("");
        let principal = body["Principal"].as_str().unwrap_or("");
        let statement_id = body["StatementId"].as_str().unwrap_or("");

        // Note: real AWS does enforce a small allow-list on `Action`, but
        // PutPermission's Smithy model only declares ResourceNotFoundException,
        // PolicyLengthExceededException, ConcurrentModificationException,
        // OperationDisabledException, and InternalException. We accept any
        // action string and just record the statement.
        // A `*` principal means "any account" and is stored verbatim, not as
        // an account-root ARN. The Terraform aws_cloudwatch_event_permission
        // resource reads the principal back and asserts it is exactly "*".
        let principal_value = if principal == "*" {
            json!("*")
        } else {
            json!({ "AWS": Arn::global("iam", principal, "root").to_string() })
        };
        let statement = json!({
            "Sid": statement_id,
            "Effect": "Allow",
            "Principal": principal_value,
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
            // PutPermission with an existing StatementId replaces that
            // statement (e.g. changing its principal), it does not stack a
            // second one. Drop any prior statement with the same Sid first.
            stmts.retain(|s| s["Sid"].as_str() != Some(statement_id));
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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
        // Smithy PutRuleRequest constraints:
        //   Name: RuleName length 1..=64, @required
        //   ScheduleExpression: length 0..=256
        //   EventPattern: length 0..=4096 (raw JSON, separate
        //     InvalidEventPatternException still applies to syntax)
        //   State: RuleState enum {ENABLED, DISABLED,
        //          ENABLED_WITH_ALL_CLOUDTRAIL_MANAGEMENT_EVENTS}
        //   Description: RuleDescription length 0..=512
        //   RoleArn: length 1..=1600
        //   EventBusName: EventBusNameOrArn length 1..=1600
        validate_required("Name", &body["Name"])?;
        let name = body["Name"]
            .as_str()
            .ok_or_else(|| missing("Name"))?
            .to_string();
        validate_string_length("Name", &name, 1, 64)?;
        validate_optional_string_length_value(
            "ScheduleExpression",
            &body["ScheduleExpression"],
            0,
            256,
        )?;
        validate_optional_string_length_value("EventPattern", &body["EventPattern"], 0, 4096)?;
        // Reject a syntactically invalid EventPattern at PutRule time, like AWS.
        // Previously only the length was checked, so an invalid pattern was
        // stored and PutEvents silently never matched it.
        if let Some(pattern) = body["EventPattern"].as_str().filter(|s| !s.is_empty()) {
            validate_event_pattern(pattern)?;
        }
        validate_optional_enum_value(
            "State",
            &body["State"],
            &[
                "ENABLED",
                "DISABLED",
                "ENABLED_WITH_ALL_CLOUDTRAIL_MANAGEMENT_EVENTS",
            ],
        )?;
        validate_optional_string_length_value("Description", &body["Description"], 0, 512)?;
        validate_optional_string_length_value("RoleArn", &body["RoleArn"], 1, 1600)?;
        validate_optional_string_length_value("EventBusName", &body["EventBusName"], 1, 1600)?;

        let raw_bus = body["EventBusName"]
            .as_str()
            .unwrap_or("default")
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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

        // Note: real AWS rejects ScheduleExpression on a non-default bus, but
        // PutRule's Smithy model only declares InvalidEventPatternException
        // for input-shape problems, not ValidationException. We accept the
        // value and let the scheduler ignore it on non-default buses.

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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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
                if let Some(ref role) = r.role_arn {
                    obj["RoleArn"] = json!(role);
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

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        // Smithy PutTargetsRequest constraints (top-level shape):
        //   Rule: RuleName @required length 1..=64
        //   EventBusName: EventBusNameOrArn length 1..=1600
        //   Targets: TargetList @required length 1..=100
        // Per-target validation still flows through `FailedEntries` (matching
        // AWS); only the top-level shape produces ValidationException.
        validate_required("Rule", &body["Rule"])?;
        let rule_name = body["Rule"].as_str().ok_or_else(|| missing("Rule"))?;
        validate_string_length("Rule", rule_name, 1, 64)?;
        validate_optional_string_length_value("EventBusName", &body["EventBusName"], 1, 1600)?;
        // Targets is @required with TargetList length 1..=100 in the Smithy
        // model. Real AWS rejects empty / oversized lists with a
        // ValidationException; per-target validation continues to flow
        // through FailedEntries (matching AWS).
        validate_required("Targets", &body["Targets"])?;
        let targets_array = body["Targets"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Targets must be a list",
            )
        })?;
        if targets_array.is_empty() || targets_array.len() > 100 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Value at 'Targets' failed to satisfy constraint: \
                 Member must have length between 1 and 100",
            ));
        }
        let event_bus_name = body["EventBusName"].as_str().unwrap_or("default");
        let targets: Vec<Value> = targets_array.clone();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let bus_name = state.resolve_bus_name(event_bus_name);
        let key = (bus_name.clone(), rule_name.to_string());

        let rule = state.rules.get_mut(&key).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Rule {rule_name} does not exist on EventBus {bus_name}."),
            )
        })?;

        let mut failed_entries: Vec<Value> = Vec::new();
        for target in &targets {
            let target_id = target["Id"].as_str().unwrap_or("").to_string();
            let target_arn = target["Arn"].as_str().unwrap_or("");

            if target_arn.ends_with(".fifo") && target.get("SqsParameters").is_none() {
                failed_entries.push(json!({
                    "TargetId": target_id,
                    "ErrorCode": "ValidationException",
                    "ErrorMessage": format!(
                        "Parameter(s) SqsParameters must be specified for target: {target_id}."
                    ),
                }));
                continue;
            }
            if !target_arn.starts_with("arn:") {
                failed_entries.push(json!({
                    "TargetId": target_id,
                    "ErrorCode": "ValidationException",
                    "ErrorMessage": format!(
                        "Parameter {target_arn} is not valid. Reason: Provided Arn is not in correct format."
                    ),
                }));
                continue;
            }

            let et = parse_target(target);
            rule.targets.retain(|t| t.id != et.id);
            rule.targets.push(et);
        }

        Ok(AwsResponse::ok_json(json!({
            "FailedEntryCount": failed_entries.len(),
            "FailedEntries": failed_entries,
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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

    fn put_events(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy PutEventsRequest constraints:
        //   EndpointId: length 1..=50
        //   Entries: @required, length 1..=10
        validate_optional_string_length_value("EndpointId", &body["EndpointId"], 1, 50)?;
        validate_required("Entries", &body["Entries"])?;
        let entries_array = body["Entries"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Entries must be a list",
            )
        })?;
        if entries_array.is_empty() || entries_array.len() > 10 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Value at 'Entries' failed to satisfy constraint: \
                 Member must have length between 1 and 10",
            ));
        }
        let entries: Vec<Value> = entries_array.clone();
        let entries = &entries;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mut result_entries = Vec::new();
        let mut events_to_deliver = Vec::new();
        let mut failed_count = 0;

        for entry in entries {
            let source = entry["Source"].as_str().unwrap_or("").to_string();
            let detail_type = entry["DetailType"].as_str().unwrap_or("").to_string();
            let detail = entry["Detail"].as_str().unwrap_or("").to_string();

            if let Err(error) = validate_put_events_entry(&source, &detail_type, &detail) {
                failed_count += 1;
                result_entries.push(error);
                continue;
            }

            let event_id = uuid::Uuid::new_v4().to_string();
            let raw_bus = entry["EventBusName"]
                .as_str()
                .unwrap_or("default")
                .to_string();
            let event_bus_name = state.resolve_bus_name(&raw_bus);

            // Bus resource-policy gate. AWS evaluates the bus's
            // resource policy against cross-account callers; same-account
            // callers always have access. The policy itself is JSON
            // stored as serde_json::Value so the IAM evaluator parses
            // it the same way it parses an S3 bucket policy.
            let caller_account = req
                .principal
                .as_ref()
                .map(|p| p.account_id.as_str())
                .unwrap_or(req.account_id.as_str());
            if caller_account != req.account_id {
                let bus_policy_value = state
                    .buses
                    .get(&event_bus_name)
                    .and_then(|b| b.policy.clone());
                if let Some(policy_value) = bus_policy_value {
                    let policy_json = serde_json::to_string(&policy_value).unwrap_or_default();
                    let policy_doc = fakecloud_iam::evaluator::PolicyDocument::parse(&policy_json);
                    let bus_arn = state
                        .buses
                        .get(&event_bus_name)
                        .map(|b| b.arn.clone())
                        .unwrap_or_default();
                    let principal =
                        req.principal
                            .clone()
                            .unwrap_or_else(|| fakecloud_core::auth::Principal {
                                arn: Arn::global("iam", caller_account, "root").to_string(),
                                user_id: caller_account.to_string(),
                                account_id: caller_account.to_string(),
                                principal_type: fakecloud_core::auth::PrincipalType::Root,
                                source_identity: None,
                                tags: None,
                            });
                    let context = fakecloud_iam::evaluator::RequestContext {
                        aws_principal_arn: Some(principal.arn.clone()),
                        aws_principal_account: Some(principal.account_id.clone()),
                        ..Default::default()
                    };
                    let eval_req = fakecloud_iam::evaluator::EvalRequest {
                        principal: &principal,
                        action: "events:PutEvents".to_string(),
                        resource: bus_arn,
                        context,
                    };
                    let decision = fakecloud_iam::evaluator::evaluate_resource_policy_only(
                        &policy_doc,
                        &eval_req,
                    );
                    if !matches!(decision, fakecloud_iam::evaluator::Decision::Allow) {
                        failed_count += 1;
                        result_entries.push(json!({
                            "ErrorCode": "AccessDeniedException",
                            "ErrorMessage": format!(
                                "User '{}' is not authorized to put events on event bus '{}'",
                                principal.arn, event_bus_name
                            ),
                        }));
                        continue;
                    }
                }
            }

            let time = parse_put_events_time(&entry["Time"]);
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

            archive_matching_event(
                state,
                &event,
                &event_bus_name,
                &source,
                &detail_type,
                &detail,
                &req.account_id,
                &req.region,
                &resources,
            );

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
        drop(accounts);

        // Deliver to targets — single-target dispatch lives in the
        // shared helper so cross-service callers (delivery.rs) honor the
        // same target shape (SQS/SNS/Lambda/Logs/Kinesis/StepFunctions/
        // ApiDestination/HTTP) and the same InputTransformer rules.
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

            let ctx = EventDispatchContext {
                state: &self.state,
                delivery: &self.delivery,
                lambda_state: self.lambda_state.as_ref(),
                logs_state: self.logs_state.as_ref(),
                container_runtime: &self.container_runtime,
                account_id: &req.account_id,
                region: &req.region,
            };
            for target in targets {
                dispatch_event_target(&ctx, &target, &event_json, &event_id, &detail_type);
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let tag_map = find_tags_mut(state, arn)?;

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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let tag_map = find_tags_mut(state, arn)?;

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

        let accounts = self.state.read();
        let empty = EventBridgeState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let tag_map = find_tags(state, arn)?;

        let tags = fakecloud_core::tags::tags_to_json(tag_map, "Key", "Value");

        Ok(AwsResponse::ok_json(json!({ "Tags": tags })))
    }

    // ─── Archive Operations ─────────────────────────────────────────────
}

// ─── Tag Lookup Helpers ─────────────────────────────────────────────────

// ─── Event Pattern Validation ────────────────────────────────────────

// ─── Connection Auth Params Response Builder ────────────────────────

// ─── Event Pattern Matching ─────────────────────────────────────────

/// Parsed + validated inputs for `StartReplay`.
struct StartReplayInput {
    name: String,
    description: Option<String>,
    event_source_arn: String,
    destination: Value,
    destination_arn: String,
    event_start_time: DateTime<Utc>,
    event_end_time: DateTime<Utc>,
}

impl StartReplayInput {
    fn from_body(body: &Value) -> Result<Self, AwsServiceError> {
        // StartReplay's Smithy model declares ResourceNotFound,
        // ResourceAlreadyExists, InvalidEventPattern, LimitExceeded, and
        // Internal — but not ValidationException. Per-field constraints are
        // enforced client-side by the SDK; we surface only declared errors
        // here. Missing required inputs default to empty strings and the
        // downstream bus/archive lookups produce ResourceNotFound for the
        // ones that matter.
        let name = body["ReplayName"].as_str().unwrap_or("").to_string();
        let description = body["Description"].as_str().map(|s| s.to_string());
        let event_source_arn = body["EventSourceArn"].as_str().unwrap_or("").to_string();
        let destination = body["Destination"].clone();

        let event_start_time = body["EventStartTime"]
            .as_f64()
            .and_then(|f| DateTime::from_timestamp(f as i64, 0))
            .unwrap_or_else(Utc::now);
        let event_end_time = body["EventEndTime"]
            .as_f64()
            .and_then(|f| DateTime::from_timestamp(f as i64, 0))
            .unwrap_or_else(Utc::now);

        let destination_arn = destination["Arn"].as_str().unwrap_or("").to_string();
        if !destination_arn.contains(":event-bus/") {
            // Missing/invalid destination cannot be resolved to a bus —
            // surface as ResourceNotFound rather than the undeclared
            // ValidationException.
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Destination.Arn {destination_arn} does not point to an event bus."),
            ));
        }

        Ok(Self {
            name,
            description,
            event_source_arn,
            destination,
            destination_arn,
            event_start_time,
            event_end_time,
        })
    }
}

#[path = "service_archives_replays.rs"]
mod service_archives_replays;
#[path = "service_connections_apidests.rs"]
mod service_connections_apidests;
#[path = "service_endpoints.rs"]
mod service_endpoints;
#[path = "service_partner_sources.rs"]
mod service_partner_sources;

#[path = "helpers.rs"]
pub(crate) mod helpers;
pub(crate) use helpers::*;

#[cfg(test)]
#[path = "service_tests.rs"]
mod tests;

#[cfg(test)]
mod pagination_reject_test {
    #[test]
    fn paginate_checked_rejects_invalid_token() {
        use fakecloud_core::pagination::paginate_checked;
        let items: Vec<i32> = (0..5).collect();
        assert!(paginate_checked(&items, Some("bad"), 3).is_err());
        assert!(paginate_checked(&items, Some("2"), 3).is_ok());
    }
}
