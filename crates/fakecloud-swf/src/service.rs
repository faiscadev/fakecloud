//! Amazon SWF awsJson1_0 dispatch + operation handlers.
//!
//! Requests carry `X-Amz-Target: SimpleWorkflowService.<Operation>`; dispatch
//! keys off `req.action`. Every operation runs model-driven input validation
//! first (required / length / range / enum), then real, account-partitioned,
//! persisted behavior: domains, versioned activity/workflow types, and workflow
//! executions with a working decider/worker state machine (poll a decision task,
//! respond with decisions that append the right history events, poll the
//! scheduled activity task, respond, observe the result in the next decision
//! task, close the execution).
//!
//! Dereferencing a domain / type / execution / task token that does not exist
//! returns SWF's canonical `UnknownResourceFault`; a duplicate domain or type
//! returns `DomainAlreadyExistsFault` / `TypeAlreadyExistsFault`; missing
//! start-time defaults return `DefaultUndefinedFault`; a second open execution
//! of the same workflow id returns `WorkflowExecutionAlreadyStartedFault`.

use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::shared::{domain_arn, new_run_id, new_task_token, now_epoch};
use crate::state::{
    ActivityRecord, ActivityRef, ActivityState, Domain, Execution, SharedSwfState, SwfData,
    TypeRecord,
};

/// Every operation name in the Amazon SWF Smithy model (39 operations).
pub const SWF_ACTIONS: &[&str] = &[
    "CountClosedWorkflowExecutions",
    "CountOpenWorkflowExecutions",
    "CountPendingActivityTasks",
    "CountPendingDecisionTasks",
    "DeleteActivityType",
    "DeleteWorkflowType",
    "DeprecateActivityType",
    "DeprecateDomain",
    "DeprecateWorkflowType",
    "DescribeActivityType",
    "DescribeDomain",
    "DescribeWorkflowExecution",
    "DescribeWorkflowType",
    "GetWorkflowExecutionHistory",
    "ListActivityTypes",
    "ListClosedWorkflowExecutions",
    "ListDomains",
    "ListOpenWorkflowExecutions",
    "ListTagsForResource",
    "ListWorkflowTypes",
    "PollForActivityTask",
    "PollForDecisionTask",
    "RecordActivityTaskHeartbeat",
    "RegisterActivityType",
    "RegisterDomain",
    "RegisterWorkflowType",
    "RequestCancelWorkflowExecution",
    "RespondActivityTaskCanceled",
    "RespondActivityTaskCompleted",
    "RespondActivityTaskFailed",
    "RespondDecisionTaskCompleted",
    "SignalWorkflowExecution",
    "StartWorkflowExecution",
    "TagResource",
    "TerminateWorkflowExecution",
    "UndeprecateActivityType",
    "UndeprecateDomain",
    "UndeprecateWorkflowType",
    "UntagResource",
];

/// Read-only verbs; any other action mutates persisted state and triggers a
/// snapshot after success. Poll/Respond/Record mutate history, so only the
/// pure getters are read-only.
fn is_mutating_action(action: &str) -> bool {
    const READ_PREFIXES: &[&str] = &["Describe", "List", "Count", "Get"];
    !READ_PREFIXES.iter().any(|p| action.starts_with(p))
}

pub struct SwfService {
    state: SharedSwfState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl SwfService {
    pub fn new(state: SharedSwfState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    async fn save(&self) {
        crate::persistence::save_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Persist hook for the CloudFormation provisioner; `None` in memory mode.
    pub fn snapshot_hook(&self) -> Option<fakecloud_persistence::SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let state = state.clone();
            let store = store.clone();
            let lock = lock.clone();
            Box::pin(async move {
                crate::persistence::save_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    fn with_account_mut<R>(&self, req: &AwsRequest, f: impl FnOnce(&mut SwfData) -> R) -> R {
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        f(acct)
    }
}

#[async_trait]
impl AwsService for SwfService {
    fn service_name(&self) -> &str {
        "swf"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let action = req.action.clone();
        if let Err(msg) = crate::validate::validate_input(&action, &req.json_body()) {
            return Err(invalid_request(msg));
        }
        let result = self.dispatch(&action, &req);
        if is_mutating_action(&action)
            && matches!(result.as_ref(), Ok(resp) if resp.status.is_success())
        {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        SWF_ACTIONS
    }
}

impl SwfService {
    fn dispatch(&self, action: &str, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        match action {
            // Domains
            "RegisterDomain" => self.register_domain(req, &body),
            "DeprecateDomain" => self.deprecate_domain(req, &body),
            "UndeprecateDomain" => self.undeprecate_domain(req, &body),
            "DescribeDomain" => self.describe_domain(req, &body),
            "ListDomains" => self.list_domains(req, &body),
            // Activity types
            "RegisterActivityType" => self.register_type(req, &body, TypeKind::Activity),
            "DeprecateActivityType" => self.deprecate_type(req, &body, TypeKind::Activity),
            "UndeprecateActivityType" => self.undeprecate_type(req, &body, TypeKind::Activity),
            "DeleteActivityType" => self.delete_type(req, &body, TypeKind::Activity),
            "DescribeActivityType" => self.describe_type(req, &body, TypeKind::Activity),
            "ListActivityTypes" => self.list_types(req, &body, TypeKind::Activity),
            // Workflow types
            "RegisterWorkflowType" => self.register_type(req, &body, TypeKind::Workflow),
            "DeprecateWorkflowType" => self.deprecate_type(req, &body, TypeKind::Workflow),
            "UndeprecateWorkflowType" => self.undeprecate_type(req, &body, TypeKind::Workflow),
            "DeleteWorkflowType" => self.delete_type(req, &body, TypeKind::Workflow),
            "DescribeWorkflowType" => self.describe_type(req, &body, TypeKind::Workflow),
            "ListWorkflowTypes" => self.list_types(req, &body, TypeKind::Workflow),
            // Executions
            "StartWorkflowExecution" => self.start_workflow_execution(req, &body),
            "DescribeWorkflowExecution" => self.describe_workflow_execution(req, &body),
            "GetWorkflowExecutionHistory" => self.get_workflow_execution_history(req, &body),
            "ListOpenWorkflowExecutions" => self.list_executions(req, &body, true),
            "ListClosedWorkflowExecutions" => self.list_executions(req, &body, false),
            "CountOpenWorkflowExecutions" => self.count_executions(req, &body, true),
            "CountClosedWorkflowExecutions" => self.count_executions(req, &body, false),
            "CountPendingActivityTasks" => self.count_pending_tasks(req, &body, true),
            "CountPendingDecisionTasks" => self.count_pending_tasks(req, &body, false),
            "SignalWorkflowExecution" => self.signal_workflow_execution(req, &body),
            "RequestCancelWorkflowExecution" => self.request_cancel_workflow_execution(req, &body),
            "TerminateWorkflowExecution" => self.terminate_workflow_execution(req, &body),
            // Decider / worker loop
            "PollForDecisionTask" => self.poll_for_decision_task(req, &body),
            "PollForActivityTask" => self.poll_for_activity_task(req, &body),
            "RespondDecisionTaskCompleted" => self.respond_decision_task_completed(req, &body),
            "RespondActivityTaskCompleted" => {
                self.respond_activity_task(req, &body, ActivityOutcome::Completed)
            }
            "RespondActivityTaskFailed" => {
                self.respond_activity_task(req, &body, ActivityOutcome::Failed)
            }
            "RespondActivityTaskCanceled" => {
                self.respond_activity_task(req, &body, ActivityOutcome::Canceled)
            }
            "RecordActivityTaskHeartbeat" => self.record_activity_task_heartbeat(req, &body),
            // Tagging
            "TagResource" => self.tag_resource(req, &body),
            "UntagResource" => self.untag_resource(req, &body),
            "ListTagsForResource" => self.list_tags_for_resource(req, &body),
            _ => Err(AwsServiceError::action_not_implemented(
                self.service_name(),
                action,
            )),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TypeKind {
    Activity,
    Workflow,
}

#[derive(Debug, Clone, Copy)]
enum ActivityOutcome {
    Completed,
    Failed,
    Canceled,
}

// ---- error / response helpers --------------------------------------------

fn fault(code: &str, msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, code, msg.into())
}

fn invalid_request(msg: impl Into<String>) -> AwsServiceError {
    fault("ValidationException", msg)
}

fn unknown_resource(msg: impl Into<String>) -> AwsServiceError {
    fault("UnknownResourceFault", msg)
}

fn ok(value: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::ok_json(value))
}

fn empty_ok() -> Result<AwsResponse, AwsServiceError> {
    ok(json!({}))
}

// ---- small member helpers -------------------------------------------------

fn str_member<'a>(body: &'a Value, name: &str) -> Option<&'a str> {
    body.get(name).and_then(Value::as_str)
}

/// Copy each named optional member from `body` into `obj` when present and
/// non-null.
fn copy_members(body: &Value, obj: &mut Map<String, Value>, names: &[&str]) {
    for name in names {
        if let Some(v) = body.get(*name).filter(|v| !v.is_null()) {
            obj.insert((*name).to_string(), v.clone());
        }
    }
}

/// Read a `TaskList` member (`{ "name": ... }`) as its name string.
fn task_list_name(v: Option<&Value>) -> Option<String> {
    v.and_then(|t| t.get("name"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Extract `(name, version)` from an `ActivityType` / `WorkflowType` member.
fn type_name_version(v: Option<&Value>) -> Option<(String, String)> {
    let v = v?;
    let name = v.get("name").and_then(Value::as_str)?;
    let version = v.get("version").and_then(Value::as_str)?;
    Some((name.to_string(), version.to_string()))
}

// ---- domains --------------------------------------------------------------

impl SwfService {
    fn register_domain(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "name").unwrap_or_default().to_string();
        let retention = str_member(body, "workflowExecutionRetentionPeriodInDays")
            .unwrap_or_default()
            .to_string();
        let description = str_member(body, "description").map(str::to_string);
        let region = req.region.clone();
        let account = req.account_id.clone();
        let tags = body.get("tags").cloned();
        self.with_account_mut(req, |data| {
            if let Some(existing) = data.domains.get(&name) {
                if existing.status == "REGISTERED" {
                    return Err(fault(
                        "DomainAlreadyExistsFault",
                        format!("Domain already exists: {name}"),
                    ));
                }
            }
            let arn = domain_arn(&region, &account, &name);
            data.domains.insert(
                name.clone(),
                Domain {
                    name: name.clone(),
                    status: "REGISTERED".to_string(),
                    description,
                    retention_days: retention,
                    arn: arn.clone(),
                },
            );
            if let Some(tag_list) = tags.as_ref() {
                let map = resource_tags_to_map(tag_list);
                if !map.is_empty() {
                    data.tags.insert(arn, map);
                }
            }
            empty_ok()
        })
    }

    fn deprecate_domain(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "name").unwrap_or_default().to_string();
        self.with_account_mut(req, |data| {
            let Some(domain) = data.domains.get_mut(&name) else {
                return Err(unknown_resource(format!("Unknown domain: {name}")));
            };
            if domain.status == "DEPRECATED" {
                return Err(fault(
                    "DomainDeprecatedFault",
                    format!("Domain already deprecated: {name}"),
                ));
            }
            domain.status = "DEPRECATED".to_string();
            empty_ok()
        })
    }

    fn undeprecate_domain(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "name").unwrap_or_default().to_string();
        self.with_account_mut(req, |data| {
            let Some(domain) = data.domains.get_mut(&name) else {
                return Err(unknown_resource(format!("Unknown domain: {name}")));
            };
            if domain.status == "REGISTERED" {
                return Err(fault(
                    "DomainAlreadyExistsFault",
                    format!("Domain already registered: {name}"),
                ));
            }
            domain.status = "REGISTERED".to_string();
            empty_ok()
        })
    }

    fn describe_domain(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "name").unwrap_or_default().to_string();
        self.with_account_mut(req, |data| {
            let Some(domain) = data.domains.get(&name) else {
                return Err(unknown_resource(format!("Unknown domain: {name}")));
            };
            ok(json!({
                "domainInfo": wire_domain_info(domain),
                "configuration": {
                    "workflowExecutionRetentionPeriodInDays": domain.retention_days,
                },
            }))
        })
    }

    fn list_domains(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let status = str_member(body, "registrationStatus")
            .unwrap_or("REGISTERED")
            .to_string();
        let reverse = body
            .get("reverseOrder")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        self.with_account_mut(req, |data| {
            let mut infos: Vec<Value> = data
                .domains
                .values()
                .filter(|d| d.status == status)
                .map(wire_domain_info)
                .collect();
            if reverse {
                infos.reverse();
            }
            ok(json!({ "domainInfos": infos }))
        })
    }
}

fn wire_domain_info(domain: &Domain) -> Value {
    let mut obj = Map::new();
    obj.insert("name".into(), json!(domain.name));
    obj.insert("status".into(), json!(domain.status));
    obj.insert("arn".into(), json!(domain.arn));
    if let Some(desc) = &domain.description {
        obj.insert("description".into(), json!(desc));
    }
    Value::Object(obj)
}

// ---- activity / workflow types -------------------------------------------

impl SwfService {
    fn register_type(
        &self,
        req: &AwsRequest,
        body: &Value,
        kind: TypeKind,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = str_member(body, "domain").unwrap_or_default().to_string();
        let name = str_member(body, "name").unwrap_or_default().to_string();
        let version = str_member(body, "version").unwrap_or_default().to_string();
        let description = str_member(body, "description").map(str::to_string);
        let config_members = config_member_names(kind);
        let mut configuration = Map::new();
        copy_members(body, &mut configuration, config_members);
        self.with_account_mut(req, |data| {
            if !data.domains.contains_key(&domain) {
                return Err(unknown_resource(format!("Unknown domain: {domain}")));
            }
            let key = SwfData::type_map_key(&domain, &name, &version);
            let map = type_map_mut(data, kind);
            if let Some(existing) = map.get(&key) {
                if existing.status == "REGISTERED" {
                    return Err(fault(
                        "TypeAlreadyExistsFault",
                        format!("Type already exists: {name} {version}"),
                    ));
                }
            }
            map.insert(
                key,
                TypeRecord {
                    name,
                    version,
                    status: "REGISTERED".to_string(),
                    description,
                    creation_date: now_epoch(),
                    deprecation_date: None,
                    configuration,
                },
            );
            empty_ok()
        })
    }

    fn deprecate_type(
        &self,
        req: &AwsRequest,
        body: &Value,
        kind: TypeKind,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = str_member(body, "domain").unwrap_or_default().to_string();
        let member = type_member_name(kind);
        let Some((name, version)) = type_name_version(body.get(member)) else {
            return Err(invalid_request(format!("{member} is required")));
        };
        self.with_account_mut(req, |data| {
            if !data.domains.contains_key(&domain) {
                return Err(unknown_resource(format!("Unknown domain: {domain}")));
            }
            let key = SwfData::type_map_key(&domain, &name, &version);
            let map = type_map_mut(data, kind);
            let Some(rec) = map.get_mut(&key) else {
                return Err(unknown_resource(format!("Unknown type: {name} {version}")));
            };
            if rec.status == "DEPRECATED" {
                return Err(fault(
                    "TypeDeprecatedFault",
                    format!("Type already deprecated: {name} {version}"),
                ));
            }
            rec.status = "DEPRECATED".to_string();
            rec.deprecation_date = Some(now_epoch());
            empty_ok()
        })
    }

    fn undeprecate_type(
        &self,
        req: &AwsRequest,
        body: &Value,
        kind: TypeKind,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = str_member(body, "domain").unwrap_or_default().to_string();
        let member = type_member_name(kind);
        let Some((name, version)) = type_name_version(body.get(member)) else {
            return Err(invalid_request(format!("{member} is required")));
        };
        self.with_account_mut(req, |data| {
            if !data.domains.contains_key(&domain) {
                return Err(unknown_resource(format!("Unknown domain: {domain}")));
            }
            let key = SwfData::type_map_key(&domain, &name, &version);
            let map = type_map_mut(data, kind);
            let Some(rec) = map.get_mut(&key) else {
                return Err(unknown_resource(format!("Unknown type: {name} {version}")));
            };
            if rec.status == "REGISTERED" {
                return Err(fault(
                    "TypeAlreadyExistsFault",
                    format!("Type already registered: {name} {version}"),
                ));
            }
            rec.status = "REGISTERED".to_string();
            rec.deprecation_date = None;
            empty_ok()
        })
    }

    fn delete_type(
        &self,
        req: &AwsRequest,
        body: &Value,
        kind: TypeKind,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = str_member(body, "domain").unwrap_or_default().to_string();
        let member = type_member_name(kind);
        let Some((name, version)) = type_name_version(body.get(member)) else {
            return Err(invalid_request(format!("{member} is required")));
        };
        self.with_account_mut(req, |data| {
            if !data.domains.contains_key(&domain) {
                return Err(unknown_resource(format!("Unknown domain: {domain}")));
            }
            let key = SwfData::type_map_key(&domain, &name, &version);
            let map = type_map_mut(data, kind);
            let Some(rec) = map.get(&key) else {
                return Err(unknown_resource(format!("Unknown type: {name} {version}")));
            };
            if rec.status != "DEPRECATED" {
                return Err(fault(
                    "TypeNotDeprecatedFault",
                    format!("Type must be deprecated before deletion: {name} {version}"),
                ));
            }
            map.remove(&key);
            empty_ok()
        })
    }

    fn describe_type(
        &self,
        req: &AwsRequest,
        body: &Value,
        kind: TypeKind,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = str_member(body, "domain").unwrap_or_default().to_string();
        let member = type_member_name(kind);
        let Some((name, version)) = type_name_version(body.get(member)) else {
            return Err(invalid_request(format!("{member} is required")));
        };
        self.with_account_mut(req, |data| {
            if !data.domains.contains_key(&domain) {
                return Err(unknown_resource(format!("Unknown domain: {domain}")));
            }
            let key = SwfData::type_map_key(&domain, &name, &version);
            let map = type_map_mut(data, kind);
            let Some(rec) = map.get(&key) else {
                return Err(unknown_resource(format!("Unknown type: {name} {version}")));
            };
            ok(json!({
                "typeInfo": wire_type_info(rec, kind),
                "configuration": Value::Object(rec.configuration.clone()),
            }))
        })
    }

    fn list_types(
        &self,
        req: &AwsRequest,
        body: &Value,
        kind: TypeKind,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = str_member(body, "domain").unwrap_or_default().to_string();
        let status = str_member(body, "registrationStatus")
            .unwrap_or("REGISTERED")
            .to_string();
        let name_filter = str_member(body, "name").map(str::to_string);
        let reverse = body
            .get("reverseOrder")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let prefix = format!("{domain}\u{1}");
        self.with_account_mut(req, |data| {
            if !data.domains.contains_key(&domain) {
                return Err(unknown_resource(format!("Unknown domain: {domain}")));
            }
            let map = type_map_mut(data, kind);
            let mut infos: Vec<Value> = map
                .iter()
                .filter(|(k, _)| k.starts_with(&prefix))
                .map(|(_, rec)| rec)
                .filter(|rec| rec.status == status)
                .filter(|rec| name_filter.as_ref().is_none_or(|n| &rec.name == n))
                .map(|rec| wire_type_info(rec, kind))
                .collect();
            if reverse {
                infos.reverse();
            }
            ok(json!({ "typeInfos": infos }))
        })
    }
}

fn wire_type_info(rec: &TypeRecord, kind: TypeKind) -> Value {
    let type_member = match kind {
        TypeKind::Activity => "activityType",
        TypeKind::Workflow => "workflowType",
    };
    let mut obj = Map::new();
    obj.insert(
        type_member.into(),
        json!({ "name": rec.name, "version": rec.version }),
    );
    obj.insert("status".into(), json!(rec.status));
    obj.insert("creationDate".into(), json!(rec.creation_date));
    if let Some(desc) = &rec.description {
        obj.insert("description".into(), json!(desc));
    }
    if let Some(dep) = rec.deprecation_date {
        obj.insert("deprecationDate".into(), json!(dep));
    }
    Value::Object(obj)
}

fn config_member_names(kind: TypeKind) -> &'static [&'static str] {
    match kind {
        TypeKind::Activity => &[
            "defaultTaskStartToCloseTimeout",
            "defaultTaskHeartbeatTimeout",
            "defaultTaskList",
            "defaultTaskPriority",
            "defaultTaskScheduleToStartTimeout",
            "defaultTaskScheduleToCloseTimeout",
        ],
        TypeKind::Workflow => &[
            "defaultTaskStartToCloseTimeout",
            "defaultExecutionStartToCloseTimeout",
            "defaultTaskList",
            "defaultTaskPriority",
            "defaultChildPolicy",
            "defaultLambdaRole",
        ],
    }
}

fn type_member_name(kind: TypeKind) -> &'static str {
    match kind {
        TypeKind::Activity => "activityType",
        TypeKind::Workflow => "workflowType",
    }
}

fn type_map_mut(
    data: &mut SwfData,
    kind: TypeKind,
) -> &mut std::collections::BTreeMap<String, TypeRecord> {
    match kind {
        TypeKind::Activity => &mut data.activity_types,
        TypeKind::Workflow => &mut data.workflow_types,
    }
}

// ---- workflow executions --------------------------------------------------

impl SwfService {
    fn start_workflow_execution(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = str_member(body, "domain").unwrap_or_default().to_string();
        let workflow_id = str_member(body, "workflowId")
            .unwrap_or_default()
            .to_string();
        let Some((wf_name, wf_version)) = type_name_version(body.get("workflowType")) else {
            return Err(invalid_request("workflowType is required"));
        };
        let input = str_member(body, "input").map(str::to_string);
        let lambda_role = str_member(body, "lambdaRole").map(str::to_string);
        let task_priority = str_member(body, "taskPriority").map(str::to_string);
        let tag_list: Vec<String> = body
            .get("tagList")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();

        self.with_account_mut(req, |data| {
            if !data.domains.contains_key(&domain) {
                return Err(unknown_resource(format!("Unknown domain: {domain}")));
            }
            let key = SwfData::type_map_key(&domain, &wf_name, &wf_version);
            let Some(wf_type) = data.workflow_types.get(&key) else {
                return Err(unknown_resource(format!(
                    "Unknown workflow type: {wf_name} {wf_version}"
                )));
            };
            if wf_type.status == "DEPRECATED" {
                return Err(fault(
                    "TypeDeprecatedFault",
                    format!("Workflow type is deprecated: {wf_name} {wf_version}"),
                ));
            }
            let cfg = &wf_type.configuration;
            // Resolve each start-time default from the request, then the type's
            // registered default; a value defined by neither is a
            // DefaultUndefinedFault (exactly as AWS reports it).
            let task_list = task_list_name(body.get("taskList"))
                .or_else(|| task_list_name(cfg.get("defaultTaskList")))
                .ok_or_else(|| default_undefined("taskList"))?;
            let child_policy = str_member(body, "childPolicy")
                .map(str::to_string)
                .or_else(|| {
                    cfg.get("defaultChildPolicy")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .ok_or_else(|| default_undefined("childPolicy"))?;
            let exec_timeout = str_member(body, "executionStartToCloseTimeout")
                .map(str::to_string)
                .or_else(|| {
                    cfg.get("defaultExecutionStartToCloseTimeout")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .ok_or_else(|| default_undefined("executionStartToCloseTimeout"))?;
            let task_timeout = str_member(body, "taskStartToCloseTimeout")
                .map(str::to_string)
                .or_else(|| {
                    cfg.get("defaultTaskStartToCloseTimeout")
                        .and_then(Value::as_str)
                        .map(str::to_string)
                })
                .ok_or_else(|| default_undefined("taskStartToCloseTimeout"))?;

            if data
                .executions
                .values()
                .any(|e| e.workflow_id == workflow_id && e.status == "OPEN")
            {
                return Err(fault(
                    "WorkflowExecutionAlreadyStartedFault",
                    format!("Workflow execution already started: {workflow_id}"),
                ));
            }

            let run_id = new_run_id();
            let mut exec = Execution {
                domain: domain.clone(),
                workflow_id: workflow_id.clone(),
                run_id: run_id.clone(),
                workflow_type_name: wf_name.clone(),
                workflow_type_version: wf_version.clone(),
                task_list: task_list.clone(),
                task_priority: task_priority.clone(),
                status: "OPEN".to_string(),
                close_status: None,
                start_timestamp: now_epoch(),
                close_timestamp: None,
                tag_list: tag_list.clone(),
                input: input.clone(),
                child_policy: child_policy.clone(),
                task_start_to_close_timeout: task_timeout.clone(),
                execution_start_to_close_timeout: exec_timeout.clone(),
                lambda_role: lambda_role.clone(),
                cancel_requested: false,
                latest_execution_context: None,
                events: Vec::new(),
                next_event_id: 1,
                decision_scheduled: false,
                decision_scheduled_event_id: None,
                decision_started_event_id: None,
                previous_started_event_id: 0,
                decision_task_token: None,
                activities: Vec::new(),
            };

            // Seed the history: WorkflowExecutionStarted, then the first
            // DecisionTaskScheduled so a decider's first poll has work.
            let mut started_attrs = Map::new();
            started_attrs.insert(
                "workflowType".into(),
                json!({ "name": wf_name, "version": wf_version }),
            );
            started_attrs.insert("taskList".into(), json!({ "name": task_list }));
            started_attrs.insert("childPolicy".into(), json!(child_policy));
            started_attrs.insert("executionStartToCloseTimeout".into(), json!(exec_timeout));
            started_attrs.insert("taskStartToCloseTimeout".into(), json!(task_timeout));
            if let Some(i) = &input {
                started_attrs.insert("input".into(), json!(i));
            }
            if !tag_list.is_empty() {
                started_attrs.insert("tagList".into(), json!(tag_list));
            }
            if let Some(lr) = &lambda_role {
                started_attrs.insert("lambdaRole".into(), json!(lr));
            }
            push_event(
                &mut exec,
                "WorkflowExecutionStarted",
                "workflowExecutionStartedEventAttributes",
                Value::Object(started_attrs),
            );
            schedule_decision_task(&mut exec);

            data.executions.insert(run_id.clone(), exec);
            ok(json!({ "runId": run_id }))
        })
    }

    fn describe_workflow_execution(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = str_member(body, "domain").unwrap_or_default().to_string();
        let workflow_id = execution_workflow_id(body);
        let run_id = execution_run_id(body);
        self.with_account_mut(req, |data| {
            if !data.domains.contains_key(&domain) {
                return Err(unknown_resource(format!("Unknown domain: {domain}")));
            }
            let Some(exec) = data.find_execution(&workflow_id, run_id.as_deref()) else {
                return Err(unknown_resource(format!(
                    "Unknown execution: {workflow_id}"
                )));
            };
            let mut out = Map::new();
            out.insert("executionInfo".into(), wire_execution_info(exec));
            out.insert("executionConfiguration".into(), wire_execution_config(exec));
            out.insert("openCounts".into(), wire_open_counts(exec));
            if let Some(ctx) = &exec.latest_execution_context {
                out.insert("latestExecutionContext".into(), json!(ctx));
            }
            ok(Value::Object(out))
        })
    }

    fn get_workflow_execution_history(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = str_member(body, "domain").unwrap_or_default().to_string();
        let workflow_id = execution_workflow_id(body);
        let run_id = execution_run_id(body);
        let reverse = body
            .get("reverseOrder")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        self.with_account_mut(req, |data| {
            if !data.domains.contains_key(&domain) {
                return Err(unknown_resource(format!("Unknown domain: {domain}")));
            }
            let Some(exec) = data.find_execution(&workflow_id, run_id.as_deref()) else {
                return Err(unknown_resource(format!(
                    "Unknown execution: {workflow_id}"
                )));
            };
            let mut events = exec.events.clone();
            if reverse {
                events.reverse();
            }
            ok(json!({ "events": events }))
        })
    }

    fn list_executions(
        &self,
        req: &AwsRequest,
        body: &Value,
        open: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = str_member(body, "domain").unwrap_or_default().to_string();
        let reverse = body
            .get("reverseOrder")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        self.with_account_mut(req, |data| {
            if !data.domains.contains_key(&domain) {
                return Err(unknown_resource(format!("Unknown domain: {domain}")));
            }
            let want = if open { "OPEN" } else { "CLOSED" };
            let mut infos: Vec<Value> = data
                .executions
                .values()
                .filter(|e| e.domain == domain && e.status == want)
                .filter(|e| execution_matches_filters(e, body, open))
                .map(wire_execution_info)
                .collect();
            if reverse {
                infos.reverse();
            }
            ok(json!({ "executionInfos": infos }))
        })
    }

    fn count_executions(
        &self,
        req: &AwsRequest,
        body: &Value,
        open: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = str_member(body, "domain").unwrap_or_default().to_string();
        self.with_account_mut(req, |data| {
            if !data.domains.contains_key(&domain) {
                return Err(unknown_resource(format!("Unknown domain: {domain}")));
            }
            let want = if open { "OPEN" } else { "CLOSED" };
            let count = data
                .executions
                .values()
                .filter(|e| e.domain == domain && e.status == want)
                .filter(|e| execution_matches_filters(e, body, open))
                .count();
            ok(json!({ "count": count, "truncated": false }))
        })
    }

    fn count_pending_tasks(
        &self,
        req: &AwsRequest,
        body: &Value,
        activity: bool,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = str_member(body, "domain").unwrap_or_default().to_string();
        let task_list = task_list_name(body.get("taskList")).unwrap_or_default();
        self.with_account_mut(req, |data| {
            if !data.domains.contains_key(&domain) {
                return Err(unknown_resource(format!("Unknown domain: {domain}")));
            }
            let count = if activity {
                data.executions
                    .values()
                    .filter(|e| e.domain == domain)
                    .flat_map(|e| e.activities.iter())
                    .filter(|a| a.state == ActivityState::Scheduled && a.task_list == task_list)
                    .count()
            } else {
                data.executions
                    .values()
                    .filter(|e| {
                        e.domain == domain
                            && e.task_list == task_list
                            && e.decision_scheduled
                            && e.decision_task_token.is_none()
                    })
                    .count()
            };
            ok(json!({ "count": count, "truncated": false }))
        })
    }

    fn signal_workflow_execution(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = str_member(body, "domain").unwrap_or_default().to_string();
        let workflow_id = str_member(body, "workflowId")
            .unwrap_or_default()
            .to_string();
        let run_id = str_member(body, "runId").map(str::to_string);
        let signal_name = str_member(body, "signalName")
            .unwrap_or_default()
            .to_string();
        let input = str_member(body, "input").map(str::to_string);
        self.with_account_mut(req, |data| {
            if !data.domains.contains_key(&domain) {
                return Err(unknown_resource(format!("Unknown domain: {domain}")));
            }
            let Some(exec) = data.find_execution_mut(&workflow_id, run_id.as_deref()) else {
                return Err(unknown_resource(format!(
                    "Unknown execution: {workflow_id}"
                )));
            };
            let mut attrs = Map::new();
            attrs.insert("signalName".into(), json!(signal_name));
            if let Some(i) = &input {
                attrs.insert("input".into(), json!(i));
            }
            push_event(
                exec,
                "WorkflowExecutionSignaled",
                "workflowExecutionSignaledEventAttributes",
                Value::Object(attrs),
            );
            schedule_decision_task(exec);
            empty_ok()
        })
    }

    fn request_cancel_workflow_execution(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = str_member(body, "domain").unwrap_or_default().to_string();
        let workflow_id = str_member(body, "workflowId")
            .unwrap_or_default()
            .to_string();
        let run_id = str_member(body, "runId").map(str::to_string);
        self.with_account_mut(req, |data| {
            if !data.domains.contains_key(&domain) {
                return Err(unknown_resource(format!("Unknown domain: {domain}")));
            }
            let Some(exec) = data.find_execution_mut(&workflow_id, run_id.as_deref()) else {
                return Err(unknown_resource(format!(
                    "Unknown execution: {workflow_id}"
                )));
            };
            exec.cancel_requested = true;
            push_event(
                exec,
                "WorkflowExecutionCancelRequested",
                "workflowExecutionCancelRequestedEventAttributes",
                json!({ "cause": "CHILD_POLICY_APPLIED" }),
            );
            schedule_decision_task(exec);
            empty_ok()
        })
    }

    fn terminate_workflow_execution(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = str_member(body, "domain").unwrap_or_default().to_string();
        let workflow_id = str_member(body, "workflowId")
            .unwrap_or_default()
            .to_string();
        let run_id = str_member(body, "runId").map(str::to_string);
        let reason = str_member(body, "reason").map(str::to_string);
        let details = str_member(body, "details").map(str::to_string);
        let child_policy = str_member(body, "childPolicy").map(str::to_string);
        self.with_account_mut(req, |data| {
            if !data.domains.contains_key(&domain) {
                return Err(unknown_resource(format!("Unknown domain: {domain}")));
            }
            let Some(exec) = data.find_execution_mut(&workflow_id, run_id.as_deref()) else {
                return Err(unknown_resource(format!(
                    "Unknown execution: {workflow_id}"
                )));
            };
            let mut attrs = Map::new();
            if let Some(r) = &reason {
                attrs.insert("reason".into(), json!(r));
            }
            if let Some(d) = &details {
                attrs.insert("details".into(), json!(d));
            }
            attrs.insert(
                "childPolicy".into(),
                json!(child_policy
                    .clone()
                    .unwrap_or_else(|| exec.child_policy.clone())),
            );
            push_event(
                exec,
                "WorkflowExecutionTerminated",
                "workflowExecutionTerminatedEventAttributes",
                Value::Object(attrs),
            );
            close_execution(exec, "TERMINATED");
            empty_ok()
        })
    }
}

fn default_undefined(field: &str) -> AwsServiceError {
    fault(
        "DefaultUndefinedFault",
        format!("No default configured for {field} and none supplied"),
    )
}

fn execution_workflow_id(body: &Value) -> String {
    body.get("execution")
        .and_then(|e| e.get("workflowId"))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn execution_run_id(body: &Value) -> Option<String> {
    body.get("execution")
        .and_then(|e| e.get("runId"))
        .and_then(Value::as_str)
        .map(str::to_string)
}

fn wire_execution_info(exec: &Execution) -> Value {
    let mut obj = Map::new();
    obj.insert(
        "execution".into(),
        json!({ "workflowId": exec.workflow_id, "runId": exec.run_id }),
    );
    obj.insert(
        "workflowType".into(),
        json!({ "name": exec.workflow_type_name, "version": exec.workflow_type_version }),
    );
    obj.insert("startTimestamp".into(), json!(exec.start_timestamp));
    obj.insert("executionStatus".into(), json!(exec.status));
    obj.insert("cancelRequested".into(), json!(exec.cancel_requested));
    if let Some(ct) = exec.close_timestamp {
        obj.insert("closeTimestamp".into(), json!(ct));
    }
    if let Some(cs) = &exec.close_status {
        obj.insert("closeStatus".into(), json!(cs));
    }
    if !exec.tag_list.is_empty() {
        obj.insert("tagList".into(), json!(exec.tag_list));
    }
    Value::Object(obj)
}

fn wire_execution_config(exec: &Execution) -> Value {
    let mut obj = Map::new();
    obj.insert(
        "taskStartToCloseTimeout".into(),
        json!(exec.task_start_to_close_timeout),
    );
    obj.insert(
        "executionStartToCloseTimeout".into(),
        json!(exec.execution_start_to_close_timeout),
    );
    obj.insert("taskList".into(), json!({ "name": exec.task_list }));
    obj.insert("childPolicy".into(), json!(exec.child_policy));
    if let Some(tp) = &exec.task_priority {
        obj.insert("taskPriority".into(), json!(tp));
    }
    if let Some(lr) = &exec.lambda_role {
        obj.insert("lambdaRole".into(), json!(lr));
    }
    Value::Object(obj)
}

fn wire_open_counts(exec: &Execution) -> Value {
    let open_activities = exec
        .activities
        .iter()
        .filter(|a| a.state != ActivityState::Closed)
        .count();
    let open_decisions = i64::from(exec.decision_scheduled || exec.decision_task_token.is_some());
    // Timers / child workflows / lambdas are derived from the event history:
    // each is opened by a *start* event and closed by a matching terminal one.
    // The count is (started - closed), clamped at zero.
    let count_events = |types: &[&str]| -> i64 {
        exec.events
            .iter()
            .filter(|ev| {
                ev.get("eventType")
                    .and_then(Value::as_str)
                    .is_some_and(|t| types.contains(&t))
            })
            .count() as i64
    };
    let open_timers =
        (count_events(&["TimerStarted"]) - count_events(&["TimerFired", "TimerCanceled"])).max(0);
    let open_children = (count_events(&["StartChildWorkflowExecutionInitiated"])
        - count_events(&[
            "ChildWorkflowExecutionCompleted",
            "ChildWorkflowExecutionFailed",
            "ChildWorkflowExecutionTimedOut",
            "ChildWorkflowExecutionCanceled",
            "ChildWorkflowExecutionTerminated",
        ]))
    .max(0);
    let open_lambdas = (count_events(&["LambdaFunctionScheduled"])
        - count_events(&[
            "LambdaFunctionCompleted",
            "LambdaFunctionFailed",
            "LambdaFunctionTimedOut",
        ]))
    .max(0);
    json!({
        "openActivityTasks": open_activities,
        "openDecisionTasks": open_decisions,
        "openTimers": open_timers,
        "openChildWorkflowExecutions": open_children,
        "openLambdaFunctions": open_lambdas,
    })
}

/// Apply the List/Count*WorkflowExecutions filters to a single execution.
/// `executionFilter`, `typeFilter` and `tagFilter` are mutually exclusive on
/// AWS, but any that are present are ANDed with the time/close-status filters.
/// `closeStatusFilter`/`closeTimeFilter` only apply to closed executions.
fn execution_matches_filters(e: &Execution, body: &Value, open: bool) -> bool {
    // executionFilter.workflowId
    if let Some(wid) = body
        .get("executionFilter")
        .and_then(|f| f.get("workflowId"))
        .and_then(Value::as_str)
    {
        if e.workflow_id != wid {
            return false;
        }
    }
    // typeFilter.name (+ optional version)
    if let Some(tf) = body.get("typeFilter") {
        if let Some(name) = tf.get("name").and_then(Value::as_str) {
            if e.workflow_type_name != name {
                return false;
            }
        }
        if let Some(version) = tf.get("version").and_then(Value::as_str) {
            if e.workflow_type_version != version {
                return false;
            }
        }
    }
    // tagFilter.tag
    if let Some(tag) = body
        .get("tagFilter")
        .and_then(|f| f.get("tag"))
        .and_then(Value::as_str)
    {
        if !e.tag_list.iter().any(|t| t == tag) {
            return false;
        }
    }
    // startTimeFilter.oldestDate / latestDate (epoch seconds)
    if let Some(stf) = body.get("startTimeFilter") {
        if let Some(oldest) = stf.get("oldestDate").and_then(Value::as_f64) {
            if e.start_timestamp < oldest {
                return false;
            }
        }
        if let Some(latest) = stf.get("latestDate").and_then(Value::as_f64) {
            if e.start_timestamp > latest {
                return false;
            }
        }
    }
    if !open {
        // closeStatusFilter.status
        if let Some(status) = body
            .get("closeStatusFilter")
            .and_then(|f| f.get("status"))
            .and_then(Value::as_str)
        {
            if e.close_status.as_deref() != Some(status) {
                return false;
            }
        }
        // closeTimeFilter.oldestDate / latestDate
        if let Some(ctf) = body.get("closeTimeFilter") {
            let close = e.close_timestamp;
            if let Some(oldest) = ctf.get("oldestDate").and_then(Value::as_f64) {
                if close.is_none_or(|c| c < oldest) {
                    return false;
                }
            }
            if let Some(latest) = ctf.get("latestDate").and_then(Value::as_f64) {
                if close.is_none_or(|c| c > latest) {
                    return false;
                }
            }
        }
    }
    true
}

// ---- decider / worker loop ------------------------------------------------

impl SwfService {
    fn poll_for_decision_task(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = str_member(body, "domain").unwrap_or_default().to_string();
        let task_list = task_list_name(body.get("taskList")).unwrap_or_default();
        self.with_account_mut(req, |data| {
            if !data.domains.contains_key(&domain) {
                return Err(unknown_resource(format!("Unknown domain: {domain}")));
            }
            // Find the next execution in this task list with a scheduled,
            // not-yet-started decision task.
            let run_id = data
                .executions
                .values()
                .find(|e| {
                    e.domain == domain
                        && e.status == "OPEN"
                        && e.task_list == task_list
                        && e.decision_scheduled
                        && e.decision_task_token.is_none()
                })
                .map(|e| e.run_id.clone());
            let Some(run_id) = run_id else {
                // Long-poll timeout: an empty task carries no taskToken.
                return ok(json!({ "taskToken": "", "startedEventId": 0, "previousStartedEventId": 0 }));
            };
            let token = new_task_token();
            let exec = data.executions.get_mut(&run_id).expect("run id just found");
            let scheduled_id = exec.decision_scheduled_event_id.unwrap_or(0);
            let started_id = push_event(
                exec,
                "DecisionTaskStarted",
                "decisionTaskStartedEventAttributes",
                json!({ "scheduledEventId": scheduled_id }),
            );
            exec.decision_scheduled = false;
            exec.decision_started_event_id = Some(started_id);
            exec.decision_task_token = Some(token.clone());
            let previous_started = exec.previous_started_event_id;
            let events = exec.events.clone();
            let out = json!({
                "taskToken": token,
                "startedEventId": started_id,
                "previousStartedEventId": previous_started,
                "workflowExecution": { "workflowId": exec.workflow_id, "runId": exec.run_id },
                "workflowType": { "name": exec.workflow_type_name, "version": exec.workflow_type_version },
                "events": events,
            });
            data.decision_tokens.insert(token, run_id);
            ok(out)
        })
    }

    fn poll_for_activity_task(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let domain = str_member(body, "domain").unwrap_or_default().to_string();
        let task_list = task_list_name(body.get("taskList")).unwrap_or_default();
        self.with_account_mut(req, |data| {
            if !data.domains.contains_key(&domain) {
                return Err(unknown_resource(format!("Unknown domain: {domain}")));
            }
            let found = data
                .executions
                .values()
                .filter(|e| e.domain == domain && e.status == "OPEN")
                .flat_map(|e| {
                    e.activities
                        .iter()
                        .filter(|a| a.state == ActivityState::Scheduled && a.task_list == task_list)
                        .map(move |a| (e.run_id.clone(), a.activity_id.clone()))
                })
                .next();
            let Some((run_id, activity_id)) = found else {
                return ok(json!({ "taskToken": "" }));
            };
            let token = new_task_token();
            let exec = data.executions.get_mut(&run_id).expect("run id just found");
            let workflow_id = exec.workflow_id.clone();
            let (scheduled_id, act_name, act_version, input) = {
                let a = exec
                    .activities
                    .iter()
                    .find(|a| a.activity_id == activity_id)
                    .expect("activity just found");
                (
                    a.scheduled_event_id,
                    a.activity_type_name.clone(),
                    a.activity_type_version.clone(),
                    a.input.clone(),
                )
            };
            let started_id = push_event(
                exec,
                "ActivityTaskStarted",
                "activityTaskStartedEventAttributes",
                json!({ "scheduledEventId": scheduled_id }),
            );
            if let Some(a) = exec
                .activities
                .iter_mut()
                .find(|a| a.activity_id == activity_id)
            {
                a.state = ActivityState::Started;
                a.started_event_id = Some(started_id);
                a.task_token = token.clone();
            }
            let mut out = Map::new();
            out.insert("taskToken".into(), json!(token));
            out.insert("activityId".into(), json!(activity_id));
            out.insert("startedEventId".into(), json!(started_id));
            out.insert(
                "workflowExecution".into(),
                json!({ "workflowId": workflow_id, "runId": run_id }),
            );
            out.insert(
                "activityType".into(),
                json!({ "name": act_name, "version": act_version }),
            );
            if let Some(i) = input {
                out.insert("input".into(), json!(i));
            }
            data.activity_tokens.insert(
                token,
                ActivityRef {
                    run_id,
                    activity_id,
                },
            );
            ok(Value::Object(out))
        })
    }

    fn respond_decision_task_completed(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let token = str_member(body, "taskToken")
            .unwrap_or_default()
            .to_string();
        let execution_context = str_member(body, "executionContext").map(str::to_string);
        let decisions = body
            .get("decisions")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        self.with_account_mut(req, |data| {
            let Some(run_id) = data.decision_tokens.remove(&token) else {
                return Err(unknown_resource("Unknown or expired decision task token"));
            };
            let exec = data
                .executions
                .get_mut(&run_id)
                .ok_or_else(|| unknown_resource("Unknown execution for decision token"))?;
            let scheduled_id = exec.decision_scheduled_event_id.unwrap_or(0);
            let started_id = exec.decision_started_event_id.unwrap_or(0);
            let mut dtc_attrs = Map::new();
            dtc_attrs.insert("scheduledEventId".into(), json!(scheduled_id));
            dtc_attrs.insert("startedEventId".into(), json!(started_id));
            if let Some(ctx) = &execution_context {
                dtc_attrs.insert("executionContext".into(), json!(ctx));
            }
            let dtc_id = push_event(
                exec,
                "DecisionTaskCompleted",
                "decisionTaskCompletedEventAttributes",
                Value::Object(dtc_attrs),
            );
            exec.previous_started_event_id = started_id;
            exec.decision_task_token = None;
            exec.decision_started_event_id = None;
            exec.decision_scheduled_event_id = None;
            if let Some(ctx) = execution_context {
                exec.latest_execution_context = Some(ctx);
            }
            for decision in &decisions {
                if exec.status != "OPEN" {
                    break;
                }
                apply_decision(exec, decision, dtc_id);
            }
            empty_ok()
        })
    }

    fn respond_activity_task(
        &self,
        req: &AwsRequest,
        body: &Value,
        outcome: ActivityOutcome,
    ) -> Result<AwsResponse, AwsServiceError> {
        let token = str_member(body, "taskToken")
            .unwrap_or_default()
            .to_string();
        let result = str_member(body, "result").map(str::to_string);
        let reason = str_member(body, "reason").map(str::to_string);
        let details = str_member(body, "details").map(str::to_string);
        self.with_account_mut(req, |data| {
            let Some(act_ref) = data.activity_tokens.remove(&token) else {
                return Err(unknown_resource("Unknown or expired activity task token"));
            };
            let exec = data
                .executions
                .get_mut(&act_ref.run_id)
                .ok_or_else(|| unknown_resource("Unknown execution for activity token"))?;
            let (scheduled_id, started_id) = {
                let a = exec
                    .activities
                    .iter_mut()
                    .find(|a| a.activity_id == act_ref.activity_id)
                    .ok_or_else(|| unknown_resource("Unknown activity for token"))?;
                a.state = ActivityState::Closed;
                (a.scheduled_event_id, a.started_event_id.unwrap_or(0))
            };
            let (event_type, attrs_key, mut attrs) = match outcome {
                ActivityOutcome::Completed => {
                    let mut m = Map::new();
                    if let Some(r) = &result {
                        m.insert("result".into(), json!(r));
                    }
                    (
                        "ActivityTaskCompleted",
                        "activityTaskCompletedEventAttributes",
                        m,
                    )
                }
                ActivityOutcome::Failed => {
                    let mut m = Map::new();
                    if let Some(r) = &reason {
                        m.insert("reason".into(), json!(r));
                    }
                    if let Some(d) = &details {
                        m.insert("details".into(), json!(d));
                    }
                    ("ActivityTaskFailed", "activityTaskFailedEventAttributes", m)
                }
                ActivityOutcome::Canceled => {
                    let mut m = Map::new();
                    if let Some(d) = &details {
                        m.insert("details".into(), json!(d));
                    }
                    (
                        "ActivityTaskCanceled",
                        "activityTaskCanceledEventAttributes",
                        m,
                    )
                }
            };
            attrs.insert("scheduledEventId".into(), json!(scheduled_id));
            attrs.insert("startedEventId".into(), json!(started_id));
            push_event(exec, event_type, attrs_key, Value::Object(attrs));
            // The activity result becomes decider-visible: schedule the next
            // decision task so the next PollForDecisionTask sees it.
            schedule_decision_task(exec);
            empty_ok()
        })
    }

    fn record_activity_task_heartbeat(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let token = str_member(body, "taskToken")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |data| {
            let Some(act_ref) = data.activity_tokens.get(&token).cloned() else {
                return Err(unknown_resource("Unknown or expired activity task token"));
            };
            let cancel_requested = data
                .executions
                .get(&act_ref.run_id)
                .map(|e| e.cancel_requested)
                .unwrap_or(false);
            ok(json!({ "cancelRequested": cancel_requested }))
        })
    }
}

/// Apply a single decision from `RespondDecisionTaskCompleted`, appending the
/// history events it produces. `dtc_id` is the `DecisionTaskCompleted` event id
/// the produced events reference.
fn apply_decision(exec: &mut Execution, decision: &Value, dtc_id: i64) {
    let decision_type = decision
        .get("decisionType")
        .and_then(Value::as_str)
        .unwrap_or_default();
    match decision_type {
        "ScheduleActivityTask" => {
            let attrs = decision
                .get("scheduleActivityTaskDecisionAttributes")
                .cloned()
                .unwrap_or(Value::Null);
            let activity_id = attrs
                .get("activityId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let (act_name, act_version) =
                type_name_version(attrs.get("activityType")).unwrap_or_default();
            let input = attrs
                .get("input")
                .and_then(Value::as_str)
                .map(str::to_string);
            let task_list =
                task_list_name(attrs.get("taskList")).unwrap_or_else(|| exec.task_list.clone());
            let mut ev_attrs = Map::new();
            ev_attrs.insert("activityId".into(), json!(activity_id));
            ev_attrs.insert(
                "activityType".into(),
                json!({ "name": act_name, "version": act_version }),
            );
            ev_attrs.insert("taskList".into(), json!({ "name": task_list }));
            ev_attrs.insert("decisionTaskCompletedEventId".into(), json!(dtc_id));
            if let Some(i) = &input {
                ev_attrs.insert("input".into(), json!(i));
            }
            let scheduled_id = push_event(
                exec,
                "ActivityTaskScheduled",
                "activityTaskScheduledEventAttributes",
                Value::Object(ev_attrs),
            );
            exec.activities.push(ActivityRecord {
                activity_id,
                activity_type_name: act_name,
                activity_type_version: act_version,
                input,
                task_list,
                scheduled_event_id: scheduled_id,
                started_event_id: None,
                state: ActivityState::Scheduled,
                task_token: String::new(),
            });
        }
        "CompleteWorkflowExecution" => {
            let attrs = decision
                .get("completeWorkflowExecutionDecisionAttributes")
                .cloned()
                .unwrap_or(Value::Null);
            let mut ev = Map::new();
            ev.insert("decisionTaskCompletedEventId".into(), json!(dtc_id));
            if let Some(r) = attrs.get("result").and_then(Value::as_str) {
                ev.insert("result".into(), json!(r));
            }
            push_event(
                exec,
                "WorkflowExecutionCompleted",
                "workflowExecutionCompletedEventAttributes",
                Value::Object(ev),
            );
            close_execution(exec, "COMPLETED");
        }
        "FailWorkflowExecution" => {
            let attrs = decision
                .get("failWorkflowExecutionDecisionAttributes")
                .cloned()
                .unwrap_or(Value::Null);
            let mut ev = Map::new();
            ev.insert("decisionTaskCompletedEventId".into(), json!(dtc_id));
            if let Some(r) = attrs.get("reason").and_then(Value::as_str) {
                ev.insert("reason".into(), json!(r));
            }
            if let Some(d) = attrs.get("details").and_then(Value::as_str) {
                ev.insert("details".into(), json!(d));
            }
            push_event(
                exec,
                "WorkflowExecutionFailed",
                "workflowExecutionFailedEventAttributes",
                Value::Object(ev),
            );
            close_execution(exec, "FAILED");
        }
        "CancelWorkflowExecution" => {
            let attrs = decision
                .get("cancelWorkflowExecutionDecisionAttributes")
                .cloned()
                .unwrap_or(Value::Null);
            let mut ev = Map::new();
            ev.insert("decisionTaskCompletedEventId".into(), json!(dtc_id));
            if let Some(d) = attrs.get("details").and_then(Value::as_str) {
                ev.insert("details".into(), json!(d));
            }
            push_event(
                exec,
                "WorkflowExecutionCanceled",
                "workflowExecutionCanceledEventAttributes",
                Value::Object(ev),
            );
            close_execution(exec, "CANCELED");
        }
        "ContinueAsNewWorkflowExecution" => {
            push_event(
                exec,
                "WorkflowExecutionContinuedAsNew",
                "workflowExecutionContinuedAsNewEventAttributes",
                json!({ "decisionTaskCompletedEventId": dtc_id, "newExecutionRunId": new_run_id() }),
            );
            close_execution(exec, "CONTINUED_AS_NEW");
        }
        "RecordMarker" => {
            let attrs = decision
                .get("recordMarkerDecisionAttributes")
                .cloned()
                .unwrap_or(Value::Null);
            let marker_name = attrs
                .get("markerName")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let mut ev = Map::new();
            ev.insert("decisionTaskCompletedEventId".into(), json!(dtc_id));
            ev.insert("markerName".into(), json!(marker_name));
            if let Some(d) = attrs.get("details").and_then(Value::as_str) {
                ev.insert("details".into(), json!(d));
            }
            push_event(
                exec,
                "MarkerRecorded",
                "markerRecordedEventAttributes",
                Value::Object(ev),
            );
        }
        "StartTimer" => {
            let attrs = decision
                .get("startTimerDecisionAttributes")
                .cloned()
                .unwrap_or(Value::Null);
            let timer_id = attrs
                .get("timerId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let timeout = attrs
                .get("startToFireTimeout")
                .and_then(Value::as_str)
                .unwrap_or("0");
            push_event(
                exec,
                "TimerStarted",
                "timerStartedEventAttributes",
                json!({
                    "timerId": timer_id,
                    "startToFireTimeout": timeout,
                    "decisionTaskCompletedEventId": dtc_id,
                }),
            );
        }
        "CancelTimer" => {
            let attrs = decision
                .get("cancelTimerDecisionAttributes")
                .cloned()
                .unwrap_or(Value::Null);
            let timer_id = attrs
                .get("timerId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            push_event(
                exec,
                "TimerCanceled",
                "timerCanceledEventAttributes",
                json!({
                    "timerId": timer_id,
                    "startedEventId": 0,
                    "decisionTaskCompletedEventId": dtc_id,
                }),
            );
        }
        "RequestCancelActivityTask" => {
            let attrs = decision
                .get("requestCancelActivityTaskDecisionAttributes")
                .cloned()
                .unwrap_or(Value::Null);
            let activity_id = attrs
                .get("activityId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            push_event(
                exec,
                "ActivityTaskCancelRequested",
                "activityTaskCancelRequestedEventAttributes",
                json!({
                    "activityId": activity_id,
                    "decisionTaskCompletedEventId": dtc_id,
                }),
            );
        }
        "SignalExternalWorkflowExecution" => {
            push_event(
                exec,
                "SignalExternalWorkflowExecutionInitiated",
                "signalExternalWorkflowExecutionInitiatedEventAttributes",
                signal_external_attrs(decision, dtc_id),
            );
        }
        "RequestCancelExternalWorkflowExecution" => {
            let attrs = decision
                .get("requestCancelExternalWorkflowExecutionDecisionAttributes")
                .cloned()
                .unwrap_or(Value::Null);
            let workflow_id = attrs
                .get("workflowId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            push_event(
                exec,
                "RequestCancelExternalWorkflowExecutionInitiated",
                "requestCancelExternalWorkflowExecutionInitiatedEventAttributes",
                json!({
                    "workflowId": workflow_id,
                    "decisionTaskCompletedEventId": dtc_id,
                }),
            );
        }
        "StartChildWorkflowExecution" => {
            let attrs = decision
                .get("startChildWorkflowExecutionDecisionAttributes")
                .cloned()
                .unwrap_or(Value::Null);
            let (name, version) = type_name_version(attrs.get("workflowType")).unwrap_or_default();
            let workflow_id = attrs
                .get("workflowId")
                .and_then(Value::as_str)
                .unwrap_or_default();
            push_event(
                exec,
                "StartChildWorkflowExecutionInitiated",
                "startChildWorkflowExecutionInitiatedEventAttributes",
                json!({
                    "workflowId": workflow_id,
                    "workflowType": { "name": name, "version": version },
                    "taskList": { "name": exec.task_list },
                    "childPolicy": exec.child_policy,
                    "decisionTaskCompletedEventId": dtc_id,
                }),
            );
        }
        "ScheduleLambdaFunction" => {
            let attrs = decision
                .get("scheduleLambdaFunctionDecisionAttributes")
                .cloned()
                .unwrap_or(Value::Null);
            let id = attrs.get("id").and_then(Value::as_str).unwrap_or_default();
            let name = attrs
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or_default();
            push_event(
                exec,
                "LambdaFunctionScheduled",
                "lambdaFunctionScheduledEventAttributes",
                json!({
                    "id": id,
                    "name": name,
                    "decisionTaskCompletedEventId": dtc_id,
                }),
            );
        }
        _ => {}
    }
}

fn signal_external_attrs(decision: &Value, dtc_id: i64) -> Value {
    let attrs = decision
        .get("signalExternalWorkflowExecutionDecisionAttributes")
        .cloned()
        .unwrap_or(Value::Null);
    let workflow_id = attrs
        .get("workflowId")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let signal_name = attrs
        .get("signalName")
        .and_then(Value::as_str)
        .unwrap_or_default();
    json!({
        "workflowId": workflow_id,
        "signalName": signal_name,
        "decisionTaskCompletedEventId": dtc_id,
    })
}

/// Append a `HistoryEvent` and return its assigned `eventId`.
fn push_event(exec: &mut Execution, event_type: &str, attrs_key: &str, attrs: Value) -> i64 {
    let id = exec.alloc_event_id();
    let mut ev = Map::new();
    ev.insert("eventTimestamp".into(), json!(now_epoch()));
    ev.insert("eventType".into(), json!(event_type));
    ev.insert("eventId".into(), json!(id));
    if !attrs.is_null() {
        ev.insert(attrs_key.into(), attrs);
    }
    exec.events.push(Value::Object(ev));
    id
}

/// Schedule a fresh decision task on an open execution when none is pending or
/// in flight, appending the `DecisionTaskScheduled` event.
fn schedule_decision_task(exec: &mut Execution) {
    if exec.status != "OPEN" {
        return;
    }
    if exec.decision_scheduled || exec.decision_task_token.is_some() {
        return;
    }
    let id = push_event(
        exec,
        "DecisionTaskScheduled",
        "decisionTaskScheduledEventAttributes",
        json!({
            "taskList": { "name": exec.task_list },
            "startToCloseTimeout": exec.task_start_to_close_timeout,
        }),
    );
    exec.decision_scheduled = true;
    exec.decision_scheduled_event_id = Some(id);
}

fn close_execution(exec: &mut Execution, close_status: &str) {
    exec.status = "CLOSED".to_string();
    exec.close_status = Some(close_status.to_string());
    exec.close_timestamp = Some(now_epoch());
    exec.decision_scheduled = false;
    exec.decision_scheduled_event_id = None;
}

// ---- tagging --------------------------------------------------------------

impl SwfService {
    fn tag_resource(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "resourceArn")
            .unwrap_or_default()
            .to_string();
        let tags = body.get("tags").cloned().unwrap_or(Value::Null);
        self.with_account_mut(req, |data| {
            if !domain_exists_by_arn(data, &arn) {
                return Err(unknown_resource(format!("Unknown resource: {arn}")));
            }
            let map = data.tags.entry(arn).or_default();
            for (k, v) in resource_tags_to_map(&tags) {
                map.insert(k, v);
            }
            empty_ok()
        })
    }

    fn untag_resource(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "resourceArn")
            .unwrap_or_default()
            .to_string();
        let keys: Vec<String> = body
            .get("tagKeys")
            .and_then(Value::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|v| v.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default();
        self.with_account_mut(req, |data| {
            if !domain_exists_by_arn(data, &arn) {
                return Err(unknown_resource(format!("Unknown resource: {arn}")));
            }
            if let Some(map) = data.tags.get_mut(&arn) {
                for k in &keys {
                    map.remove(k);
                }
            }
            empty_ok()
        })
    }

    fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "resourceArn")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |data| {
            if !domain_exists_by_arn(data, &arn) {
                return Err(unknown_resource(format!("Unknown resource: {arn}")));
            }
            let tags: Vec<Value> = data
                .tags
                .get(&arn)
                .map(|m| {
                    m.iter()
                        .map(|(k, v)| json!({ "key": k, "value": v }))
                        .collect()
                })
                .unwrap_or_default();
            ok(json!({ "tags": tags }))
        })
    }
}

/// True when `arn` names a registered domain in this account.
fn domain_exists_by_arn(data: &SwfData, arn: &str) -> bool {
    data.domains.values().any(|d| d.arn == arn)
}

/// Convert a `ResourceTagList` (`[{key,value}]`) into a sorted key/value map.
fn resource_tags_to_map(tags: &Value) -> BTreeMapStr {
    let mut out = std::collections::BTreeMap::new();
    if let Some(arr) = tags.as_array() {
        for t in arr {
            if let Some(k) = t.get("key").and_then(Value::as_str) {
                let v = t.get("value").and_then(Value::as_str).unwrap_or("");
                out.insert(k.to_string(), v.to_string());
            }
        }
    }
    out
}

type BTreeMapStr = std::collections::BTreeMap<String, String>;

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn service() -> SwfService {
        SwfService::new(Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        ))))
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "swf".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "000000000000".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: String::new(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn call(svc: &SwfService, action: &str, body: Value) -> Result<Value, String> {
        let r = req(action, body);
        crate::validate::validate_input(action, &r.json_body())?;
        match svc.dispatch(action, &r) {
            Ok(resp) => Ok(serde_json::from_slice(resp.body.expect_bytes()).unwrap_or(Value::Null)),
            Err(AwsServiceError::AwsError { code, .. }) => Err(code),
            Err(e) => Err(format!("{e:?}")),
        }
    }

    #[test]
    fn register_and_describe_domain() {
        let svc = service();
        call(
            &svc,
            "RegisterDomain",
            json!({ "name": "d1", "workflowExecutionRetentionPeriodInDays": "30" }),
        )
        .unwrap();
        let out = call(&svc, "DescribeDomain", json!({ "name": "d1" })).unwrap();
        assert_eq!(out["domainInfo"]["status"], "REGISTERED");
        assert_eq!(
            out["configuration"]["workflowExecutionRetentionPeriodInDays"],
            "30"
        );
    }

    #[test]
    fn duplicate_domain_rejected() {
        let svc = service();
        let body = json!({ "name": "d1", "workflowExecutionRetentionPeriodInDays": "1" });
        call(&svc, "RegisterDomain", body.clone()).unwrap();
        let err = call(&svc, "RegisterDomain", body).unwrap_err();
        assert_eq!(err, "DomainAlreadyExistsFault");
    }

    #[test]
    fn describe_unknown_domain_faults() {
        let svc = service();
        let err = call(&svc, "DescribeDomain", json!({ "name": "nope" })).unwrap_err();
        assert_eq!(err, "UnknownResourceFault");
    }

    #[test]
    fn register_type_requires_domain() {
        let svc = service();
        let err = call(
            &svc,
            "RegisterWorkflowType",
            json!({ "domain": "missing", "name": "wf", "version": "1" }),
        )
        .unwrap_err();
        assert_eq!(err, "UnknownResourceFault");
    }

    #[test]
    fn start_workflow_needs_defaults() {
        let svc = service();
        call(
            &svc,
            "RegisterDomain",
            json!({ "name": "d", "workflowExecutionRetentionPeriodInDays": "1" }),
        )
        .unwrap();
        call(
            &svc,
            "RegisterWorkflowType",
            json!({ "domain": "d", "name": "wf", "version": "1" }),
        )
        .unwrap();
        // No taskList / timeouts / childPolicy defaults -> DefaultUndefinedFault.
        let err = call(
            &svc,
            "StartWorkflowExecution",
            json!({ "domain": "d", "workflowId": "w1", "workflowType": { "name": "wf", "version": "1" } }),
        )
        .unwrap_err();
        assert_eq!(err, "DefaultUndefinedFault");
    }

    #[test]
    fn full_decider_worker_loop() {
        let svc = service();
        call(
            &svc,
            "RegisterDomain",
            json!({ "name": "d", "workflowExecutionRetentionPeriodInDays": "1" }),
        )
        .unwrap();
        call(
            &svc,
            "RegisterWorkflowType",
            json!({
                "domain": "d", "name": "wf", "version": "1",
                "defaultTaskList": { "name": "dtl" },
                "defaultChildPolicy": "TERMINATE",
                "defaultExecutionStartToCloseTimeout": "3600",
                "defaultTaskStartToCloseTimeout": "60"
            }),
        )
        .unwrap();
        call(
            &svc,
            "RegisterActivityType",
            json!({ "domain": "d", "name": "act", "version": "1" }),
        )
        .unwrap();
        let start = call(
            &svc,
            "StartWorkflowExecution",
            json!({ "domain": "d", "workflowId": "w1", "workflowType": { "name": "wf", "version": "1" }, "input": "hello" }),
        )
        .unwrap();
        let run_id = start["runId"].as_str().unwrap().to_string();

        // Decider polls, gets a decision task with the seeded history.
        let dt = call(
            &svc,
            "PollForDecisionTask",
            json!({ "domain": "d", "taskList": { "name": "dtl" } }),
        )
        .unwrap();
        let dtoken = dt["taskToken"].as_str().unwrap().to_string();
        assert!(!dtoken.is_empty());
        let types: Vec<&str> = dt["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["eventType"].as_str().unwrap())
            .collect();
        assert!(types.contains(&"WorkflowExecutionStarted"));
        assert!(types.contains(&"DecisionTaskStarted"));

        // Schedule an activity.
        call(
            &svc,
            "RespondDecisionTaskCompleted",
            json!({
                "taskToken": dtoken,
                "decisions": [{
                    "decisionType": "ScheduleActivityTask",
                    "scheduleActivityTaskDecisionAttributes": {
                        "activityId": "a1",
                        "activityType": { "name": "act", "version": "1" },
                        "input": "hello",
                        "taskList": { "name": "atl" }
                    }
                }]
            }),
        )
        .unwrap();

        // Worker polls, runs, completes.
        let at = call(
            &svc,
            "PollForActivityTask",
            json!({ "domain": "d", "taskList": { "name": "atl" } }),
        )
        .unwrap();
        let atoken = at["taskToken"].as_str().unwrap().to_string();
        assert_eq!(at["activityId"], "a1");
        assert_eq!(at["input"], "hello");
        call(
            &svc,
            "RespondActivityTaskCompleted",
            json!({ "taskToken": atoken, "result": "42" }),
        )
        .unwrap();

        // Next decision task shows ActivityTaskCompleted; decider completes wf.
        let dt2 = call(
            &svc,
            "PollForDecisionTask",
            json!({ "domain": "d", "taskList": { "name": "dtl" } }),
        )
        .unwrap();
        let dtoken2 = dt2["taskToken"].as_str().unwrap().to_string();
        let types2: Vec<&str> = dt2["events"]
            .as_array()
            .unwrap()
            .iter()
            .map(|e| e["eventType"].as_str().unwrap())
            .collect();
        assert!(types2.contains(&"ActivityTaskCompleted"));
        call(
            &svc,
            "RespondDecisionTaskCompleted",
            json!({
                "taskToken": dtoken2,
                "decisions": [{
                    "decisionType": "CompleteWorkflowExecution",
                    "completeWorkflowExecutionDecisionAttributes": { "result": "42" }
                }]
            }),
        )
        .unwrap();

        // Execution is closed COMPLETED and shows in the closed list.
        let desc = call(
            &svc,
            "DescribeWorkflowExecution",
            json!({ "domain": "d", "execution": { "workflowId": "w1", "runId": run_id } }),
        )
        .unwrap();
        assert_eq!(desc["executionInfo"]["executionStatus"], "CLOSED");
        assert_eq!(desc["executionInfo"]["closeStatus"], "COMPLETED");

        let closed = call(
            &svc,
            "CountClosedWorkflowExecutions",
            json!({ "domain": "d" }),
        )
        .unwrap();
        assert_eq!(closed["count"], 1);
    }

    #[test]
    fn list_and_count_open_executions_apply_filters() {
        // List/Count*WorkflowExecutions previously ignored every filter; they
        // must honor executionFilter / typeFilter / tagFilter (bug-hunt).
        let svc = service();
        call(
            &svc,
            "RegisterDomain",
            json!({ "name": "d", "workflowExecutionRetentionPeriodInDays": "1" }),
        )
        .unwrap();
        for (name, version) in [("wfA", "1"), ("wfB", "1")] {
            call(
                &svc,
                "RegisterWorkflowType",
                json!({
                    "domain": "d", "name": name, "version": version,
                    "defaultTaskList": { "name": "dtl" },
                    "defaultChildPolicy": "TERMINATE",
                    "defaultExecutionStartToCloseTimeout": "3600",
                    "defaultTaskStartToCloseTimeout": "60"
                }),
            )
            .unwrap();
        }
        // w1: type wfA, tag "red"; w2: type wfB, tag "blue".
        call(
            &svc,
            "StartWorkflowExecution",
            json!({ "domain": "d", "workflowId": "w1", "workflowType": {"name": "wfA", "version": "1"}, "tagList": ["red"] }),
        )
        .unwrap();
        call(
            &svc,
            "StartWorkflowExecution",
            json!({ "domain": "d", "workflowId": "w2", "workflowType": {"name": "wfB", "version": "1"}, "tagList": ["blue"] }),
        )
        .unwrap();

        // No filter -> both.
        let all = call(
            &svc,
            "ListOpenWorkflowExecutions",
            json!({ "domain": "d", "startTimeFilter": { "oldestDate": 0 } }),
        )
        .unwrap();
        assert_eq!(all["executionInfos"].as_array().unwrap().len(), 2);

        // executionFilter by workflowId.
        let by_id = call(
            &svc,
            "ListOpenWorkflowExecutions",
            json!({ "domain": "d", "startTimeFilter": { "oldestDate": 0 }, "executionFilter": { "workflowId": "w1" } }),
        )
        .unwrap();
        let ids = by_id["executionInfos"].as_array().unwrap();
        assert_eq!(ids.len(), 1);
        assert_eq!(ids[0]["execution"]["workflowId"], "w1");

        // typeFilter by name.
        let by_type = call(
            &svc,
            "CountOpenWorkflowExecutions",
            json!({ "domain": "d", "startTimeFilter": { "oldestDate": 0 }, "typeFilter": { "name": "wfB" } }),
        )
        .unwrap();
        assert_eq!(by_type["count"], 1);

        // tagFilter.
        let by_tag = call(
            &svc,
            "CountOpenWorkflowExecutions",
            json!({ "domain": "d", "startTimeFilter": { "oldestDate": 0 }, "tagFilter": { "tag": "red" } }),
        )
        .unwrap();
        assert_eq!(by_tag["count"], 1);

        // A tag that matches nothing.
        let none = call(
            &svc,
            "CountOpenWorkflowExecutions",
            json!({ "domain": "d", "startTimeFilter": { "oldestDate": 0 }, "tagFilter": { "tag": "green" } }),
        )
        .unwrap();
        assert_eq!(none["count"], 0);
    }

    #[test]
    fn describe_execution_reports_real_open_counts() {
        // openTimers / openChildWorkflowExecutions / openLambdaFunctions were
        // hardcoded to 0; they must be derived from the event history (bug-hunt).
        let svc = service();
        call(
            &svc,
            "RegisterDomain",
            json!({ "name": "d", "workflowExecutionRetentionPeriodInDays": "1" }),
        )
        .unwrap();
        call(
            &svc,
            "RegisterWorkflowType",
            json!({
                "domain": "d", "name": "wf", "version": "1",
                "defaultTaskList": { "name": "dtl" },
                "defaultChildPolicy": "TERMINATE",
                "defaultExecutionStartToCloseTimeout": "3600",
                "defaultTaskStartToCloseTimeout": "60"
            }),
        )
        .unwrap();
        let start = call(
            &svc,
            "StartWorkflowExecution",
            json!({ "domain": "d", "workflowId": "w1", "workflowType": { "name": "wf", "version": "1" } }),
        )
        .unwrap();
        let run_id = start["runId"].as_str().unwrap().to_string();

        // Inject history: 2 timers started, 1 fired -> 1 open; 1 child started,
        // none closed -> 1 open; 1 lambda scheduled -> 1 open.
        {
            let mut guard = svc.state.write();
            let data = guard.get_or_create("000000000000");
            let exec = data.executions.get_mut(&run_id).unwrap();
            for t in [
                "TimerStarted",
                "TimerStarted",
                "TimerFired",
                "StartChildWorkflowExecutionInitiated",
                "LambdaFunctionScheduled",
            ] {
                exec.events.push(json!({ "eventType": t }));
            }
        }

        let desc = call(
            &svc,
            "DescribeWorkflowExecution",
            json!({ "domain": "d", "execution": { "workflowId": "w1", "runId": run_id } }),
        )
        .unwrap();
        let oc = &desc["openCounts"];
        assert_eq!(oc["openTimers"], 1);
        assert_eq!(oc["openChildWorkflowExecutions"], 1);
        assert_eq!(oc["openLambdaFunctions"], 1);
    }
}
