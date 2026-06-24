//! EventBridge Scheduler REST-JSON service handler.
//!
//! Implements the twelve operations in the AWS SDK model:
//! Create/Get/Update/Delete/ListSchedule, Create/Get/Delete/ListScheduleGroup,
//! Tag/Untag/ListTagsForResource. The firing loop, SQS delivery, and DLQ
//! routing live in separate modules added in Batch 2.

use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use http::{Method, StatusCode};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    group_arn, schedule_arn, DeadLetterConfig, FlexibleTimeWindow, RetryPolicy, Schedule,
    ScheduleGroup, SchedulerSnapshot, SharedSchedulerState, SqsParameters, Target, DEFAULT_GROUP,
    SCHEDULER_SNAPSHOT_SCHEMA_VERSION,
};

const NAME_MAX: usize = 64;
const NAME_PATTERN_DESC: &str = r"^[0-9a-zA-Z-_.]+$";

pub struct SchedulerService {
    state: SharedSchedulerState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl SchedulerService {
    pub fn new(state: SharedSchedulerState) -> Self {
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

    async fn save_snapshot(&self) {
        save_scheduler_snapshot(
            &self.state,
            self.snapshot_store.clone(),
            &self.snapshot_lock,
        )
        .await;
    }

    /// Build a hook that persists the current Scheduler state when invoked, or
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
                save_scheduler_snapshot(&state, Some(store), &lock).await;
            })
        }))
    }

    fn resolve_action(req: &AwsRequest) -> Option<(&'static str, PathArgs)> {
        let segs = &req.path_segments;
        let first = segs.first().map(|s| s.as_str());
        // `path_segments` is split on `/` with empty parts stripped, so a
        // request like `POST /schedules/` collapses to `["schedules"]` and
        // would route to ListSchedules. AWS's contract for these single-
        // resource paths is that an empty Name is a ValidationException,
        // not "operation not found." Detect the trailing slash on a
        // resource-collection path and re-promote it to a 2-segment route
        // with an empty Name; the handler then returns the right error.
        // raw_path is the original URI before query stripping; a trailing
        // `/` means the resource name was empty.
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let collection_with_trailing_slash =
            segs.len() == 1 && raw.matches('/').count() >= 2 && raw.ends_with('/');
        let (effective_len, effective_name) = if collection_with_trailing_slash
            && matches!(first, Some("schedules" | "schedule-groups" | "tags"))
        {
            (2, String::new())
        } else if segs.len() == 2 {
            (2, segs[1].clone())
        } else {
            (segs.len(), String::new())
        };
        match (&req.method, effective_len, first) {
            (&Method::POST, 2, Some("schedules")) => {
                Some(("CreateSchedule", PathArgs::Name(effective_name)))
            }
            (&Method::GET, 2, Some("schedules")) => {
                Some(("GetSchedule", PathArgs::Name(effective_name)))
            }
            (&Method::PUT, 2, Some("schedules")) => {
                Some(("UpdateSchedule", PathArgs::Name(effective_name)))
            }
            (&Method::DELETE, 2, Some("schedules")) => {
                Some(("DeleteSchedule", PathArgs::Name(effective_name)))
            }
            (&Method::GET, 1, Some("schedules")) => Some(("ListSchedules", PathArgs::None)),
            (&Method::POST, 2, Some("schedule-groups")) => {
                Some(("CreateScheduleGroup", PathArgs::Name(effective_name)))
            }
            (&Method::GET, 2, Some("schedule-groups")) => {
                Some(("GetScheduleGroup", PathArgs::Name(effective_name)))
            }
            (&Method::DELETE, 2, Some("schedule-groups")) => {
                Some(("DeleteScheduleGroup", PathArgs::Name(effective_name)))
            }
            (&Method::GET, 1, Some("schedule-groups")) => {
                Some(("ListScheduleGroups", PathArgs::None))
            }
            (&Method::POST, 2, Some("tags")) => Some((
                "TagResource",
                PathArgs::Arn(percent_decode(&effective_name)),
            )),
            (&Method::DELETE, 2, Some("tags")) => Some((
                "UntagResource",
                PathArgs::Arn(percent_decode(&effective_name)),
            )),
            (&Method::GET, 2, Some("tags")) => Some((
                "ListTagsForResource",
                PathArgs::Arn(percent_decode(&effective_name)),
            )),
            _ => None,
        }
    }

    fn create_schedule(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        validate_name("Name", name)?;

        let group_name = body
            .get("GroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_GROUP)
            .to_string();
        validate_name("GroupName", &group_name)?;

        let expr = body
            .get("ScheduleExpression")
            .and_then(|v| v.as_str())
            .ok_or_else(|| validation("ScheduleExpression is required"))?
            .to_string();
        if expr.is_empty() {
            return Err(validation("ScheduleExpression is required"));
        }

        let flex = parse_flexible_time_window(body.get("FlexibleTimeWindow"))?;
        let target = parse_target(body.get("Target"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if !state.groups.contains_key(&group_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ResourceNotFoundException",
                format!("Schedule group not found: {group_name}"),
            ));
        }

        let key = (group_name.clone(), name.to_string());
        if state.schedules.contains_key(&key) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ConflictException",
                format!("Schedule {name} already exists in group {group_name}"),
            ));
        }

        let now = Utc::now();
        let arn = schedule_arn(&state.region, &state.account_id, &group_name, name);
        let sched = Schedule {
            arn: arn.clone(),
            name: name.to_string(),
            group_name: group_name.clone(),
            schedule_expression: expr,
            schedule_expression_timezone: body
                .get("ScheduleExpressionTimezone")
                .and_then(|v| v.as_str())
                .map(String::from),
            start_date: parse_optional_timestamp(body.get("StartDate")),
            end_date: parse_optional_timestamp(body.get("EndDate")),
            description: body
                .get("Description")
                .and_then(|v| v.as_str())
                .map(String::from),
            state: body
                .get("State")
                .and_then(|v| v.as_str())
                .unwrap_or("ENABLED")
                .to_string(),
            kms_key_arn: body
                .get("KmsKeyArn")
                .and_then(|v| v.as_str())
                .map(String::from),
            action_after_completion: body
                .get("ActionAfterCompletion")
                .and_then(|v| v.as_str())
                .unwrap_or("NONE")
                .to_string(),
            flexible_time_window: flex,
            target,
            creation_date: now,
            last_modification_date: now,
            last_fired: None,
        };
        state.schedules.insert(key, sched);

        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "ScheduleArn": arn }).to_string(),
        ))
    }

    fn get_schedule(&self, req: &AwsRequest, name: &str) -> Result<AwsResponse, AwsServiceError> {
        validate_name("Name", name)?;
        let group_name = req
            .query_params
            .get("groupName")
            .cloned()
            .unwrap_or_else(|| DEFAULT_GROUP.to_string());

        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(not_found_schedule(name, &group_name))?;
        let sched = state
            .schedules
            .get(&(group_name.clone(), name.to_string()))
            .ok_or_else(not_found_schedule(name, &group_name))?;

        Ok(AwsResponse::json(
            StatusCode::OK,
            schedule_output_json(sched).to_string(),
        ))
    }

    fn update_schedule(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        validate_name("Name", name)?;

        let group_name = body
            .get("GroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(DEFAULT_GROUP)
            .to_string();
        validate_name("GroupName", &group_name)?;

        let expr = body
            .get("ScheduleExpression")
            .and_then(|v| v.as_str())
            .ok_or_else(|| validation("ScheduleExpression is required"))?
            .to_string();
        if expr.is_empty() {
            return Err(validation("ScheduleExpression is required"));
        }
        let flex = parse_flexible_time_window(body.get("FlexibleTimeWindow"))?;
        let target = parse_target(body.get("Target"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let key = (group_name.clone(), name.to_string());
        let sched = state
            .schedules
            .get_mut(&key)
            .ok_or_else(not_found_schedule(name, &group_name))?;

        sched.schedule_expression = expr;
        sched.schedule_expression_timezone = body
            .get("ScheduleExpressionTimezone")
            .and_then(|v| v.as_str())
            .map(String::from);
        sched.start_date = parse_optional_timestamp(body.get("StartDate"));
        sched.end_date = parse_optional_timestamp(body.get("EndDate"));
        sched.description = body
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);
        sched.state = body
            .get("State")
            .and_then(|v| v.as_str())
            .unwrap_or("ENABLED")
            .to_string();
        sched.kms_key_arn = body
            .get("KmsKeyArn")
            .and_then(|v| v.as_str())
            .map(String::from);
        sched.action_after_completion = body
            .get("ActionAfterCompletion")
            .and_then(|v| v.as_str())
            .unwrap_or("NONE")
            .to_string();
        sched.flexible_time_window = flex;
        sched.target = target;
        sched.last_modification_date = Utc::now();
        // Reset last_fired on update so the modified schedule is
        // evaluated against its new expression from scratch.
        sched.last_fired = None;

        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "ScheduleArn": sched.arn }).to_string(),
        ))
    }

    fn delete_schedule(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_name("Name", name)?;
        let group_name = req
            .query_params
            .get("groupName")
            .cloned()
            .unwrap_or_else(|| DEFAULT_GROUP.to_string());

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .schedules
            .remove(&(group_name.clone(), name.to_string()))
            .ok_or_else(not_found_schedule(name, &group_name))?;
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_schedules(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_list_query(req)?;
        let group_filter = req.query_params.get("ScheduleGroup").cloned();
        if let Some(g) = group_filter.as_deref() {
            validate_name("ScheduleGroup", g)?;
        }
        let prefix = req.query_params.get("NamePrefix").cloned();
        if let Some(p) = prefix.as_deref() {
            validate_name_prefix(p)?;
        }
        let state_filter = req.query_params.get("State").cloned();
        if let Some(s) = state_filter.as_deref() {
            if s != "ENABLED" && s != "DISABLED" {
                return Err(validation(format!("Invalid State: {s}")));
            }
        }
        let max_results = req
            .query_params
            .get("MaxResults")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(100);
        let next_token = req.query_params.get("NextToken").cloned();

        let accounts = self.state.read();
        let Some(state) = accounts.get(&req.account_id) else {
            return Ok(AwsResponse::json(
                StatusCode::OK,
                json!({ "Schedules": [] }).to_string(),
            ));
        };

        let mut schedules: Vec<&Schedule> = state
            .schedules
            .values()
            .filter(|s| {
                group_filter.as_ref().is_none_or(|g| &s.group_name == g)
                    && prefix.as_ref().is_none_or(|p| s.name.starts_with(p))
                    && state_filter.as_ref().is_none_or(|st| &s.state == st)
            })
            .collect();
        schedules.sort_by(|a, b| {
            a.group_name
                .cmp(&b.group_name)
                .then_with(|| a.name.cmp(&b.name))
        });

        let (page, token) = paginate_checked(&schedules, next_token.as_deref(), max_results)
            .map_err(|_| validation("Invalid NextToken"))?;
        let summaries: Vec<Value> = page.iter().map(|s| schedule_summary_json(s)).collect();

        let mut out = json!({ "Schedules": summaries });
        if let Some(t) = token {
            out["NextToken"] = Value::String(t);
        }
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    fn create_schedule_group(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_name("Name", name)?;
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if state.groups.contains_key(name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "ConflictException",
                format!("Schedule group {name} already exists"),
            ));
        }

        let mut tags = BTreeMap::new();
        if let Err(field) =
            fakecloud_core::tags::apply_tags(&mut tags, &body, "Tags", "Key", "Value")
        {
            return Err(validation(format!("invalid field {field}")));
        }

        let now = Utc::now();
        let arn = group_arn(&state.region, &state.account_id, name);
        state.groups.insert(
            name.to_string(),
            ScheduleGroup {
                arn: arn.clone(),
                name: name.to_string(),
                state: "ACTIVE".to_string(),
                creation_date: now,
                last_modification_date: now,
                tags,
            },
        );

        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "ScheduleGroupArn": arn }).to_string(),
        ))
    }

    fn get_schedule_group(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_name("Name", name)?;
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(not_found_group(name))?;
        let group = state.groups.get(name).ok_or_else(not_found_group(name))?;
        Ok(AwsResponse::json(
            StatusCode::OK,
            schedule_group_output_json(group).to_string(),
        ))
    }

    fn delete_schedule_group(
        &self,
        req: &AwsRequest,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_name("Name", name)?;
        if name == DEFAULT_GROUP {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "The default schedule group cannot be deleted",
            ));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .groups
            .remove(name)
            .ok_or_else(not_found_group(name))?;
        // Cascade delete schedules in the group (AWS deletes the group
        // asynchronously; we do it synchronously since fakecloud is
        // synchronous-by-design for test determinism).
        state.schedules.retain(|(g, _), _| g != name);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_schedule_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_list_query(req)?;
        let prefix = req.query_params.get("NamePrefix").cloned();
        if let Some(p) = prefix.as_deref() {
            validate_name_prefix(p)?;
        }
        let max_results = req
            .query_params
            .get("MaxResults")
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(100);
        let next_token = req.query_params.get("NextToken").cloned();

        let accounts = self.state.read();
        let Some(state) = accounts.get(&req.account_id) else {
            return Ok(AwsResponse::json(
                StatusCode::OK,
                json!({ "ScheduleGroups": [] }).to_string(),
            ));
        };

        let mut groups: Vec<&ScheduleGroup> = state
            .groups
            .values()
            .filter(|g| prefix.as_ref().is_none_or(|p| g.name.starts_with(p)))
            .collect();
        groups.sort_by(|a, b| a.name.cmp(&b.name));

        let (page, token) = paginate_checked(&groups, next_token.as_deref(), max_results)
            .map_err(|_| validation("Invalid NextToken"))?;
        let summaries: Vec<Value> = page
            .iter()
            .map(|g| schedule_group_summary_json(g))
            .collect();

        let mut out = json!({ "ScheduleGroups": summaries });
        if let Some(t) = token {
            out["NextToken"] = Value::String(t);
        }
        Ok(AwsResponse::json(StatusCode::OK, out.to_string()))
    }

    fn tag_resource(
        &self,
        req: &AwsRequest,
        resource_arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_resource_arn(resource_arn)?;
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        let tags_array = body
            .get("Tags")
            .and_then(|v| v.as_array())
            .ok_or_else(|| validation("Tags is required"))?;
        if tags_array.is_empty() {
            return Err(validation("Tags must contain at least one entry"));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let group_name = group_name_from_tag_arn(resource_arn)?;
        let group = state
            .groups
            .get_mut(&group_name)
            .ok_or_else(not_found_arn(resource_arn))?;
        if let Err(field) =
            fakecloud_core::tags::apply_tags(&mut group.tags, &body, "Tags", "Key", "Value")
        {
            return Err(validation(format!("invalid field {field}")));
        }
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn untag_resource(
        &self,
        req: &AwsRequest,
        resource_arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_resource_arn(resource_arn)?;
        let group_name = group_name_from_tag_arn(resource_arn)?;
        // Scheduler encodes TagKeys as a repeated query parameter
        // `TagKeys`. raw_query preserves repeated keys; we parse it
        // manually since HashMap<String, String> deduplicates.
        let keys: Vec<String> = parse_multi_query(&req.raw_query, "TagKeys");
        if keys.is_empty() {
            return Err(validation("TagKeys is required"));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let group = state
            .groups
            .get_mut(&group_name)
            .ok_or_else(not_found_arn(resource_arn))?;
        for k in keys {
            group.tags.remove(&k);
        }
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
        resource_arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_resource_arn(resource_arn)?;
        let group_name = group_name_from_tag_arn(resource_arn)?;
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(not_found_arn(resource_arn))?;
        let group = state
            .groups
            .get(&group_name)
            .ok_or_else(not_found_arn(resource_arn))?;
        let tags = fakecloud_core::tags::tags_to_json(&group.tags, "Key", "Value");
        Ok(AwsResponse::json(
            StatusCode::OK,
            json!({ "Tags": tags }).to_string(),
        ))
    }
}

/// Persist the current Scheduler state as a snapshot. Cloned + serialized under
/// the snapshot lock. Noop when `store` is `None` (memory mode). Shared by
/// `SchedulerService::save_snapshot` and the CloudFormation provisioner's
/// post-provision persist hook so both route through the same
/// serialize-and-write path.
pub async fn save_scheduler_snapshot(
    state: &SharedSchedulerState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: &AsyncMutex<()>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = SchedulerSnapshot {
        schema_version: SCHEDULER_SNAPSHOT_SCHEMA_VERSION,
        accounts: state.read().clone(),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write scheduler snapshot"),
        Err(err) => tracing::error!(%err, "scheduler snapshot task panicked"),
    }
}

#[async_trait]
impl AwsService for SchedulerService {
    fn service_name(&self) -> &str {
        "scheduler"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let (action, args) = Self::resolve_action(&req).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UnknownOperationException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            )
        })?;

        let mutates = matches!(
            action,
            "CreateSchedule"
                | "UpdateSchedule"
                | "DeleteSchedule"
                | "CreateScheduleGroup"
                | "DeleteScheduleGroup"
                | "TagResource"
                | "UntagResource"
        );

        let result = match (action, &args) {
            ("CreateSchedule", PathArgs::Name(n)) => self.create_schedule(&req, n),
            ("GetSchedule", PathArgs::Name(n)) => self.get_schedule(&req, n),
            ("UpdateSchedule", PathArgs::Name(n)) => self.update_schedule(&req, n),
            ("DeleteSchedule", PathArgs::Name(n)) => self.delete_schedule(&req, n),
            ("ListSchedules", PathArgs::None) => self.list_schedules(&req),
            ("CreateScheduleGroup", PathArgs::Name(n)) => self.create_schedule_group(&req, n),
            ("GetScheduleGroup", PathArgs::Name(n)) => self.get_schedule_group(&req, n),
            ("DeleteScheduleGroup", PathArgs::Name(n)) => self.delete_schedule_group(&req, n),
            ("ListScheduleGroups", PathArgs::None) => self.list_schedule_groups(&req),
            ("TagResource", PathArgs::Arn(a)) => self.tag_resource(&req, a),
            ("UntagResource", PathArgs::Arn(a)) => self.untag_resource(&req, a),
            ("ListTagsForResource", PathArgs::Arn(a)) => self.list_tags_for_resource(&req, a),
            _ => Err(AwsServiceError::action_not_implemented("scheduler", action)),
        };

        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "CreateSchedule",
            "GetSchedule",
            "UpdateSchedule",
            "DeleteSchedule",
            "ListSchedules",
            "CreateScheduleGroup",
            "GetScheduleGroup",
            "DeleteScheduleGroup",
            "ListScheduleGroups",
            "TagResource",
            "UntagResource",
            "ListTagsForResource",
        ]
    }

    fn iam_enforceable(&self) -> bool {
        true
    }

    fn iam_action_for(&self, request: &AwsRequest) -> Option<fakecloud_core::auth::IamAction> {
        let (raw_action, args) = Self::resolve_action(request)?;
        let action: &'static str = match raw_action {
            "CreateSchedule" => "CreateSchedule",
            "GetSchedule" => "GetSchedule",
            "UpdateSchedule" => "UpdateSchedule",
            "DeleteSchedule" => "DeleteSchedule",
            "ListSchedules" => "ListSchedules",
            "CreateScheduleGroup" => "CreateScheduleGroup",
            "GetScheduleGroup" => "GetScheduleGroup",
            "DeleteScheduleGroup" => "DeleteScheduleGroup",
            "ListScheduleGroups" => "ListScheduleGroups",
            "TagResource" => "TagResource",
            "UntagResource" => "UntagResource",
            "ListTagsForResource" => "ListTagsForResource",
            _ => return None,
        };
        let region = request.region.as_str();
        let account = request
            .principal
            .as_ref()
            .map(|p| p.account_id.as_str())
            .unwrap_or(request.account_id.as_str());
        let resource = match (action, &args) {
            ("CreateSchedule", PathArgs::Name(n)) | ("UpdateSchedule", PathArgs::Name(n)) => {
                // Create/Update pass GroupName in the request body.
                let group = Self::body_field(&request.body, "GroupName")
                    .unwrap_or_else(|| DEFAULT_GROUP.to_string());
                schedule_arn(region, account, &group, n)
            }
            ("GetSchedule", PathArgs::Name(n)) | ("DeleteSchedule", PathArgs::Name(n)) => {
                // Get/Delete pass GroupName as a query string parameter.
                let group = schedule_group_from_request(request);
                schedule_arn(region, account, &group, n)
            }
            ("CreateScheduleGroup", PathArgs::Name(n))
            | ("GetScheduleGroup", PathArgs::Name(n))
            | ("DeleteScheduleGroup", PathArgs::Name(n)) => group_arn(region, account, n),
            ("TagResource", PathArgs::Arn(a))
            | ("UntagResource", PathArgs::Arn(a))
            | ("ListTagsForResource", PathArgs::Arn(a)) => a.clone(),
            ("ListSchedules", _) | ("ListScheduleGroups", _) => "*".to_string(),
            _ => "*".to_string(),
        };
        Some(fakecloud_core::auth::IamAction {
            service: "scheduler",
            action,
            resource,
        })
    }

    fn iam_condition_keys_for(
        &self,
        request: &AwsRequest,
        action: &fakecloud_core::auth::IamAction,
    ) -> BTreeMap<String, Vec<String>> {
        let mut out = BTreeMap::new();
        // `scheduler:ScheduleGroup` is the only documented service-
        // specific condition key on Scheduler operations. Emit it
        // whenever the request targets a specific group.
        let group = match action.action {
            "CreateSchedule" | "UpdateSchedule" => Self::body_field(&request.body, "GroupName")
                .unwrap_or_else(|| DEFAULT_GROUP.to_string()),
            "GetSchedule" | "DeleteSchedule" => schedule_group_from_request(request),
            "CreateScheduleGroup" | "GetScheduleGroup" | "DeleteScheduleGroup" => {
                // The resource ARN already contains the group name.
                arn_group_name(&action.resource).unwrap_or_default()
            }
            _ => return out,
        };
        if !group.is_empty() {
            out.insert("scheduler:schedulegroup".to_string(), vec![group]);
        }
        out
    }

    fn resource_tags_for(&self, resource_arn: &str) -> Option<HashMap<String, String>> {
        let group_name = group_name_from_tag_arn(resource_arn).ok()?;
        let accounts = self.state.read();
        let account_id = arn_account_id(resource_arn)?;
        let state = accounts.get(&account_id)?;
        state
            .groups
            .get(&group_name)
            .map(|g| g.tags.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
    }

    fn request_tags_from(
        &self,
        request: &AwsRequest,
        action: &str,
    ) -> Option<HashMap<String, String>> {
        match action {
            "CreateScheduleGroup" | "TagResource" => {
                let body: Value = serde_json::from_slice(&request.body).ok()?;
                let arr = body.get("Tags").and_then(|v| v.as_array())?;
                let mut out = HashMap::new();
                for tag in arr {
                    if let (Some(k), Some(v)) = (
                        tag.get("Key").and_then(|v| v.as_str()),
                        tag.get("Value").and_then(|v| v.as_str()),
                    ) {
                        out.insert(k.to_string(), v.to_string());
                    }
                }
                Some(out)
            }
            _ => None,
        }
    }
}

impl SchedulerService {
    fn body_field(body: &[u8], key: &str) -> Option<String> {
        serde_json::from_slice::<Value>(body)
            .ok()
            .and_then(|v| v.get(key).and_then(|f| f.as_str()).map(String::from))
    }
}

/// Resolve the schedule group a request targets: explicit `groupName`
/// query param (Get/Delete) takes priority, otherwise default.
fn schedule_group_from_request(request: &AwsRequest) -> String {
    request
        .query_params
        .get("groupName")
        .cloned()
        .unwrap_or_else(|| DEFAULT_GROUP.to_string())
}

fn arn_group_name(arn: &str) -> Option<String> {
    arn.split(':')
        .nth(5)
        .and_then(|r| r.strip_prefix("schedule-group/").map(str::to_string))
}

fn arn_account_id(arn: &str) -> Option<String> {
    arn.split(':').nth(4).map(str::to_string)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

enum PathArgs {
    None,
    Name(String),
    Arn(String),
}

fn validate_name(field: &str, value: &str) -> Result<(), AwsServiceError> {
    if value.is_empty() || value.len() > NAME_MAX {
        return Err(validation(format!(
            "{field} must be 1-{NAME_MAX} characters"
        )));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return Err(validation(format!(
            "{field} must match {NAME_PATTERN_DESC}"
        )));
    }
    Ok(())
}

fn validation(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}

/// Validate the `NamePrefix` query filter shared by ListSchedules and
/// ListScheduleGroups. The Smithy `NamePrefix` shape has the same
/// constraints as `Name` (length 1..64, charset [0-9a-zA-Z-_.]).
fn validate_name_prefix(value: &str) -> Result<(), AwsServiceError> {
    if value.is_empty() || value.len() > NAME_MAX {
        return Err(validation(format!(
            "NamePrefix must be 1-{NAME_MAX} characters"
        )));
    }
    if !value
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    {
        return Err(validation(format!(
            "NamePrefix must match {NAME_PATTERN_DESC}"
        )));
    }
    Ok(())
}

/// Validate the standard `MaxResults` (range 1..100) and `NextToken`
/// (length 1..2048) query params shared by all List operations.
fn validate_list_query(req: &AwsRequest) -> Result<(), AwsServiceError> {
    if let Some(raw) = req.query_params.get("MaxResults") {
        let n: i64 = raw
            .parse()
            .map_err(|_| validation("MaxResults must be an integer"))?;
        if !(1..=100).contains(&n) {
            return Err(validation("MaxResults must be between 1 and 100"));
        }
    }
    if let Some(t) = req.query_params.get("NextToken") {
        if t.is_empty() || t.len() > 2048 {
            return Err(validation("NextToken must be 1-2048 characters"));
        }
    }
    Ok(())
}

/// Validate the ResourceArn path label used by Tag/Untag/ListTagsForResource.
/// Smithy `ResourceArn` has length 1..1600.
fn validate_resource_arn(arn: &str) -> Result<(), AwsServiceError> {
    if arn.is_empty() || arn.len() > 1600 {
        return Err(validation("ResourceArn must be 1-1600 characters"));
    }
    Ok(())
}

fn not_found_schedule(name: &str, group: &str) -> impl Fn() -> AwsServiceError + 'static {
    let name = name.to_string();
    let group = group.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Schedule not found: {group}/{name}"),
        )
    }
}

fn not_found_group(name: &str) -> impl Fn() -> AwsServiceError + 'static {
    let name = name.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Schedule group not found: {name}"),
        )
    }
}

fn not_found_arn(arn: &str) -> impl Fn() -> AwsServiceError + 'static {
    let arn = arn.to_string();
    move || {
        AwsServiceError::aws_error(
            StatusCode::NOT_FOUND,
            "ResourceNotFoundException",
            format!("Resource not found: {arn}"),
        )
    }
}

/// Parse restJson1 timestamp: epoch-seconds number (integer or
/// fractional) or RFC3339 string (tolerant fallback — the aws-sdk-js
/// v3 default serializes numbers but users may hand-roll requests).
fn parse_optional_timestamp(v: Option<&Value>) -> Option<DateTime<Utc>> {
    let v = v?;
    if let Some(n) = v.as_f64() {
        let secs = n.trunc() as i64;
        let nanos = ((n.fract()) * 1_000_000_000.0) as u32;
        return Utc.timestamp_opt(secs, nanos).single();
    }
    if let Some(n) = v.as_i64() {
        return Utc.timestamp_opt(n, 0).single();
    }
    if let Some(s) = v.as_str() {
        return DateTime::parse_from_rfc3339(s)
            .ok()
            .map(|d| d.with_timezone(&Utc));
    }
    None
}

fn timestamp_to_number(t: DateTime<Utc>) -> Value {
    let secs = t.timestamp() as f64;
    let frac = t.timestamp_subsec_millis() as f64 / 1000.0;
    Value::from(secs + frac)
}

fn parse_flexible_time_window(v: Option<&Value>) -> Result<FlexibleTimeWindow, AwsServiceError> {
    let v = v.ok_or_else(|| validation("FlexibleTimeWindow is required"))?;
    let mode = v
        .get("Mode")
        .and_then(|m| m.as_str())
        .ok_or_else(|| validation("FlexibleTimeWindow.Mode is required"))?
        .to_string();
    if mode != "OFF" && mode != "FLEXIBLE" {
        return Err(validation(format!(
            "FlexibleTimeWindow.Mode must be OFF or FLEXIBLE, got {mode}"
        )));
    }
    let maximum_window_in_minutes = v.get("MaximumWindowInMinutes").and_then(|m| m.as_i64());
    if mode == "FLEXIBLE" && maximum_window_in_minutes.is_none() {
        return Err(validation(
            "FlexibleTimeWindow.MaximumWindowInMinutes is required when Mode is FLEXIBLE",
        ));
    }
    Ok(FlexibleTimeWindow {
        mode,
        maximum_window_in_minutes,
    })
}

fn parse_target(v: Option<&Value>) -> Result<Target, AwsServiceError> {
    let v = v.ok_or_else(|| validation("Target is required"))?;
    let arn = v
        .get("Arn")
        .and_then(|a| a.as_str())
        .ok_or_else(|| validation("Target.Arn is required"))?
        .to_string();
    let role_arn = v
        .get("RoleArn")
        .and_then(|a| a.as_str())
        .ok_or_else(|| validation("Target.RoleArn is required"))?
        .to_string();
    Ok(Target {
        arn,
        role_arn,
        input: v.get("Input").and_then(|i| i.as_str()).map(String::from),
        dead_letter_config: v.get("DeadLetterConfig").map(|d| DeadLetterConfig {
            arn: d.get("Arn").and_then(|a| a.as_str()).map(String::from),
        }),
        // Store the RetryPolicy exactly as supplied (None when the caller omits
        // the block) so the simulated delivery ticker honors the real retry
        // budget — a schedule with no RetryPolicy retries 0 times. AWS's
        // reporting defaults (86400 / 185) are applied only in the
        // GetSchedule serialization, not in stored state.
        retry_policy: v.get("RetryPolicy").map(|p| RetryPolicy {
            maximum_event_age_in_seconds: p
                .get("MaximumEventAgeInSeconds")
                .and_then(|n| n.as_i64()),
            maximum_retry_attempts: p.get("MaximumRetryAttempts").and_then(|n| n.as_i64()),
        }),
        sqs_parameters: v.get("SqsParameters").map(|s| SqsParameters {
            message_group_id: s
                .get("MessageGroupId")
                .and_then(|m| m.as_str())
                .map(String::from),
        }),
        ecs_parameters: v.get("EcsParameters").cloned(),
        eventbridge_parameters: v.get("EventBridgeParameters").cloned(),
        kinesis_parameters: v.get("KinesisParameters").cloned(),
        sagemaker_pipeline_parameters: v.get("SageMakerPipelineParameters").cloned(),
    })
}

fn schedule_output_json(s: &Schedule) -> Value {
    let mut out = json!({
        "Arn": s.arn,
        "Name": s.name,
        "GroupName": s.group_name,
        "ScheduleExpression": s.schedule_expression,
        "State": s.state,
        "ActionAfterCompletion": s.action_after_completion,
        "FlexibleTimeWindow": flexible_time_window_json(&s.flexible_time_window),
        "Target": target_json(&s.target),
        "CreationDate": timestamp_to_number(s.creation_date),
        "LastModificationDate": timestamp_to_number(s.last_modification_date),
    });
    if let Some(ref tz) = s.schedule_expression_timezone {
        out["ScheduleExpressionTimezone"] = Value::String(tz.clone());
    }
    if let Some(t) = s.start_date {
        out["StartDate"] = timestamp_to_number(t);
    }
    if let Some(t) = s.end_date {
        out["EndDate"] = timestamp_to_number(t);
    }
    if let Some(ref d) = s.description {
        out["Description"] = Value::String(d.clone());
    }
    if let Some(ref k) = s.kms_key_arn {
        out["KmsKeyArn"] = Value::String(k.clone());
    }
    out
}

fn schedule_summary_json(s: &Schedule) -> Value {
    json!({
        "Arn": s.arn,
        "Name": s.name,
        "GroupName": s.group_name,
        "State": s.state,
        "CreationDate": timestamp_to_number(s.creation_date),
        "LastModificationDate": timestamp_to_number(s.last_modification_date),
        // ScheduleSummary.Target is a TargetSummary (only Arn). The full
        // Target (with RoleArn etc.) is returned by GetSchedule.
        "Target": {
            "Arn": s.target.arn,
        },
    })
}

fn schedule_group_output_json(g: &ScheduleGroup) -> Value {
    json!({
        "Arn": g.arn,
        "Name": g.name,
        "State": g.state,
        "CreationDate": timestamp_to_number(g.creation_date),
        "LastModificationDate": timestamp_to_number(g.last_modification_date),
    })
}

fn schedule_group_summary_json(g: &ScheduleGroup) -> Value {
    json!({
        "Arn": g.arn,
        "Name": g.name,
        "State": g.state,
        "CreationDate": timestamp_to_number(g.creation_date),
        "LastModificationDate": timestamp_to_number(g.last_modification_date),
    })
}

fn flexible_time_window_json(f: &FlexibleTimeWindow) -> Value {
    let mut out = json!({ "Mode": f.mode });
    if let Some(n) = f.maximum_window_in_minutes {
        out["MaximumWindowInMinutes"] = Value::from(n);
    }
    out
}

fn target_json(t: &Target) -> Value {
    let mut out = json!({
        "Arn": t.arn,
        "RoleArn": t.role_arn,
    });
    if let Some(ref i) = t.input {
        out["Input"] = Value::String(i.clone());
    }
    if let Some(ref d) = t.dead_letter_config {
        let mut dl = serde_json::Map::new();
        if let Some(ref a) = d.arn {
            dl.insert("Arn".to_string(), Value::String(a.clone()));
        }
        out["DeadLetterConfig"] = Value::Object(dl);
    }
    // AWS always reports a RetryPolicy on a target, defaulting the fields the
    // caller omitted (MaximumEventAgeInSeconds = 86400 / 24h,
    // MaximumRetryAttempts = 185). The Terraform resource reads both
    // unconditionally. Stored state keeps the supplied values (or None) so the
    // delivery ticker uses the real retry budget; the defaults are presentation
    // only.
    {
        let p = t.retry_policy.as_ref();
        let mut rp = serde_json::Map::new();
        rp.insert(
            "MaximumEventAgeInSeconds".to_string(),
            Value::from(
                p.and_then(|p| p.maximum_event_age_in_seconds)
                    .unwrap_or(86400),
            ),
        );
        rp.insert(
            "MaximumRetryAttempts".to_string(),
            Value::from(p.and_then(|p| p.maximum_retry_attempts).unwrap_or(185)),
        );
        out["RetryPolicy"] = Value::Object(rp);
    }
    if let Some(ref s) = t.sqs_parameters {
        let mut sp = serde_json::Map::new();
        if let Some(ref g) = s.message_group_id {
            sp.insert("MessageGroupId".to_string(), Value::String(g.clone()));
        }
        out["SqsParameters"] = Value::Object(sp);
    }
    if let Some(ref v) = t.ecs_parameters {
        out["EcsParameters"] = v.clone();
    }
    if let Some(ref v) = t.eventbridge_parameters {
        out["EventBridgeParameters"] = v.clone();
    }
    if let Some(ref v) = t.kinesis_parameters {
        out["KinesisParameters"] = v.clone();
    }
    if let Some(ref v) = t.sagemaker_pipeline_parameters {
        out["SageMakerPipelineParameters"] = v.clone();
    }
    out
}

fn percent_decode(s: &str) -> String {
    percent_encoding::percent_decode_str(s)
        .decode_utf8_lossy()
        .into_owned()
}

/// Parse a repeated query-string key into a Vec of decoded values.
/// Scheduler uses `?TagKeys=k1&TagKeys=k2` which HashMap<String,String>
/// can't represent faithfully; we parse raw_query instead.
fn parse_multi_query(raw_query: &str, key: &str) -> Vec<String> {
    let prefix = format!("{key}=");
    let key_eq = prefix.as_str();
    raw_query
        .split('&')
        .filter(|pair| pair.starts_with(key_eq))
        .map(|pair| percent_decode(&pair[key_eq.len()..]))
        .collect()
}

/// Extract the schedule-group name from a scheduler ARN. AWS
/// EventBridge Scheduler only permits tagging on schedule-group
/// resources; schedule ARNs are rejected with a ValidationException to
/// match the real service.
fn group_name_from_tag_arn(arn: &str) -> Result<String, AwsServiceError> {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() < 6 || parts[0] != "arn" || parts[2] != "scheduler" {
        return Err(validation(format!("Invalid scheduler ARN: {arn}")));
    }
    let resource = parts[5];
    if let Some(name) = resource.strip_prefix("schedule-group/") {
        if name.is_empty() {
            return Err(validation(format!("Invalid schedule-group ARN: {arn}")));
        }
        Ok(name.to_string())
    } else {
        Err(validation(format!(
            "Tagging is only supported on schedule-group resources: {arn}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use fakecloud_aws::arn::Arn;
    use http::HeaderMap;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_state() -> SharedSchedulerState {
        Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("111122223333", "us-east-1", ""),
        ))
    }

    fn make_request(method: Method, path: &str, body: &str) -> AwsRequest {
        let (p, q) = match path.find('?') {
            Some(i) => (&path[..i], &path[i + 1..]),
            None => (path, ""),
        };
        let path_segments: Vec<String> = p
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        let query_params: HashMap<String, String> = q
            .split('&')
            .filter(|s| !s.is_empty())
            .filter_map(|pair| {
                let (k, v) = pair.split_once('=')?;
                Some((k.to_string(), v.to_string()))
            })
            .collect();
        AwsRequest {
            service: "scheduler".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: "111122223333".to_string(),
            request_id: "test".to_string(),
            headers: HeaderMap::new(),
            query_params,
            body: Bytes::from(body.to_string()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments,
            raw_path: p.to_string(),
            raw_query: q.to_string(),
            method,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn create_body(name_hint: &str) -> String {
        json!({
            "ScheduleExpression": "rate(1 minute)",
            "FlexibleTimeWindow": { "Mode": "OFF" },
            "Target": {
                "Arn": Arn::new("sqs", "us-east-1", "111122223333", &format!("{name_hint}-q")).to_string(),
                "RoleArn": "arn:aws:iam::111122223333:role/scheduler"
            }
        })
        .to_string()
    }

    /// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
    #[test]
    fn snapshot_hook_is_none_without_store() {
        let svc = SchedulerService::new(make_state());
        assert!(svc.snapshot_hook().is_none());
    }

    #[tokio::test]
    async fn snapshot_hook_fires_with_store() {
        let store: Arc<dyn fakecloud_persistence::SnapshotStore> =
            Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
        let svc = SchedulerService::new(make_state()).with_snapshot_store(store);
        let hook = svc
            .snapshot_hook()
            .expect("hook present when a store is set");
        hook().await;
    }

    #[tokio::test]
    async fn create_get_delete_schedule_round_trip() {
        let svc = SchedulerService::new(make_state());
        let body = create_body("t1");
        let resp = svc
            .handle(make_request(Method::POST, "/schedules/my-schedule", &body))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            v["ScheduleArn"].as_str().unwrap(),
            "arn:aws:scheduler:us-east-1:111122223333:schedule/default/my-schedule"
        );

        let resp = svc
            .handle(make_request(Method::GET, "/schedules/my-schedule", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["Name"], "my-schedule");
        assert_eq!(v["GroupName"], "default");
        assert_eq!(v["ScheduleExpression"], "rate(1 minute)");
        assert_eq!(v["State"], "ENABLED");
        assert_eq!(v["FlexibleTimeWindow"]["Mode"], "OFF");
        assert_eq!(v["ActionAfterCompletion"], "NONE");

        let resp = svc
            .handle(make_request(Method::DELETE, "/schedules/my-schedule", ""))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);

        let err = svc
            .handle(make_request(Method::GET, "/schedules/my-schedule", ""))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn create_schedule_rejects_duplicate() {
        let svc = SchedulerService::new(make_state());
        let body = create_body("dup");
        svc.handle(make_request(Method::POST, "/schedules/dup", &body))
            .await
            .unwrap();
        let err = svc
            .handle(make_request(Method::POST, "/schedules/dup", &body))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::CONFLICT);
        assert_eq!(err.code(), "ConflictException");
    }

    #[tokio::test]
    async fn create_schedule_rejects_missing_group() {
        let svc = SchedulerService::new(make_state());
        let body = json!({
            "GroupName": "does-not-exist",
            "ScheduleExpression": "rate(1 minute)",
            "FlexibleTimeWindow": { "Mode": "OFF" },
            "Target": {
                "Arn": "arn:aws:sqs:us-east-1:111122223333:q",
                "RoleArn": "arn:aws:iam::111122223333:role/scheduler"
            }
        })
        .to_string();
        let err = svc
            .handle(make_request(Method::POST, "/schedules/s", &body))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn update_schedule_changes_expression_idempotently() {
        let svc = SchedulerService::new(make_state());
        svc.handle(make_request(
            Method::POST,
            "/schedules/up",
            &create_body("up"),
        ))
        .await
        .unwrap();

        let new_body = json!({
            "ScheduleExpression": "rate(5 minutes)",
            "FlexibleTimeWindow": { "Mode": "OFF" },
            "Target": {
                "Arn": "arn:aws:sqs:us-east-1:111122223333:up-q",
                "RoleArn": "arn:aws:iam::111122223333:role/scheduler",
                "Input": "{\"v\":1}"
            }
        })
        .to_string();
        svc.handle(make_request(Method::PUT, "/schedules/up", &new_body))
            .await
            .unwrap();
        let resp = svc
            .handle(make_request(Method::GET, "/schedules/up", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["ScheduleExpression"], "rate(5 minutes)");
        assert_eq!(v["Target"]["Input"], "{\"v\":1}");
    }

    #[tokio::test]
    async fn list_schedules_filters_by_group_and_prefix() {
        let svc = SchedulerService::new(make_state());
        svc.handle(make_request(Method::POST, "/schedule-groups/gA", "{}"))
            .await
            .unwrap();
        for (name, group) in [
            ("alpha-1", DEFAULT_GROUP),
            ("alpha-2", "gA"),
            ("beta-1", "gA"),
        ] {
            let body = json!({
                "GroupName": group,
                "ScheduleExpression": "rate(1 minute)",
                "FlexibleTimeWindow": { "Mode": "OFF" },
                "Target": {
                    "Arn": "arn:aws:sqs:us-east-1:111122223333:q",
                    "RoleArn": "arn:aws:iam::111122223333:role/s"
                }
            })
            .to_string();
            svc.handle(make_request(
                Method::POST,
                &format!("/schedules/{name}"),
                &body,
            ))
            .await
            .unwrap();
        }

        let resp = svc
            .handle(make_request(
                Method::GET,
                "/schedules?ScheduleGroup=gA&NamePrefix=alpha",
                "",
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let names: Vec<&str> = v["Schedules"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["Name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["alpha-2"]);
    }

    #[tokio::test]
    async fn schedule_group_lifecycle() {
        let svc = SchedulerService::new(make_state());
        let resp = svc
            .handle(make_request(
                Method::POST,
                "/schedule-groups/grp",
                r#"{"Tags":[{"Key":"env","Value":"test"}]}"#,
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            v["ScheduleGroupArn"],
            "arn:aws:scheduler:us-east-1:111122223333:schedule-group/grp"
        );

        let resp = svc
            .handle(make_request(Method::GET, "/schedule-groups/grp", ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(v["Name"], "grp");
        assert_eq!(v["State"], "ACTIVE");

        svc.handle(make_request(Method::DELETE, "/schedule-groups/grp", ""))
            .await
            .unwrap();
        let err = svc
            .handle(make_request(Method::GET, "/schedule-groups/grp", ""))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn default_group_cannot_be_deleted() {
        let svc = SchedulerService::new(make_state());
        let err = svc
            .handle(make_request(Method::DELETE, "/schedule-groups/default", ""))
            .await
            .err()
            .unwrap();
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn tag_and_list_tags_on_group() {
        let svc = SchedulerService::new(make_state());
        svc.handle(make_request(Method::POST, "/schedule-groups/tg", "{}"))
            .await
            .unwrap();
        let arn = "arn:aws:scheduler:us-east-1:111122223333:schedule-group/tg";
        let encoded =
            percent_encoding::utf8_percent_encode(arn, percent_encoding::NON_ALPHANUMERIC)
                .to_string();
        let body = r#"{"Tags":[{"Key":"env","Value":"prod"}]}"#;
        svc.handle(make_request(
            Method::POST,
            &format!("/tags/{encoded}"),
            body,
        ))
        .await
        .unwrap();

        let resp = svc
            .handle(make_request(Method::GET, &format!("/tags/{encoded}"), ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let tags = v["Tags"].as_array().unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["Key"], "env");
        assert_eq!(tags[0]["Value"], "prod");
    }

    #[tokio::test]
    async fn untag_removes_keys() {
        let svc = SchedulerService::new(make_state());
        svc.handle(make_request(
            Method::POST,
            "/schedule-groups/tg2",
            r#"{"Tags":[{"Key":"a","Value":"1"},{"Key":"b","Value":"2"}]}"#,
        ))
        .await
        .unwrap();
        let arn = "arn:aws:scheduler:us-east-1:111122223333:schedule-group/tg2";
        let encoded =
            percent_encoding::utf8_percent_encode(arn, percent_encoding::NON_ALPHANUMERIC)
                .to_string();
        let path = format!("/tags/{encoded}?TagKeys=a");
        let mut req = make_request(Method::DELETE, &path, "");
        // raw_query captured by the test harness above; ensure repeated-key parsing works
        req.raw_query = "TagKeys=a".to_string();
        svc.handle(req).await.unwrap();

        let resp = svc
            .handle(make_request(Method::GET, &format!("/tags/{encoded}"), ""))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let tags = v["Tags"].as_array().unwrap();
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0]["Key"], "b");
    }

    #[tokio::test]
    async fn validates_name_length_and_pattern() {
        let svc = SchedulerService::new(make_state());
        let body = create_body("bad");
        let err = svc
            .handle(make_request(Method::POST, "/schedules/has%20space", &body))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ValidationException");
    }

    // bug-audit 2026-05-28, 1.7: a malformed NextToken must be rejected with
    // ValidationException, not silently treated as the first page.
    #[tokio::test]
    async fn list_schedules_rejects_invalid_next_token() {
        let svc = SchedulerService::new(make_state());
        let err = svc
            .handle(make_request(
                Method::GET,
                "/schedules?NextToken=not-a-valid-token",
                "",
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ValidationException");
    }

    #[tokio::test]
    async fn list_schedule_groups_rejects_invalid_next_token() {
        let svc = SchedulerService::new(make_state());
        let err = svc
            .handle(make_request(
                Method::GET,
                "/schedule-groups?NextToken=not-a-valid-token",
                "",
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ValidationException");
    }

    #[tokio::test]
    async fn tag_resource_requires_tags_field() {
        let svc = SchedulerService::new(make_state());
        svc.handle(make_request(Method::POST, "/schedule-groups/tr-req", "{}"))
            .await
            .unwrap();
        let arn = "arn:aws:scheduler:us-east-1:111122223333:schedule-group/tr-req";
        let encoded =
            percent_encoding::utf8_percent_encode(arn, percent_encoding::NON_ALPHANUMERIC)
                .to_string();
        let err = svc
            .handle(make_request(
                Method::POST,
                &format!("/tags/{encoded}"),
                "{}",
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ValidationException");
    }

    #[tokio::test]
    async fn untag_resource_requires_tag_keys() {
        let svc = SchedulerService::new(make_state());
        svc.handle(make_request(Method::POST, "/schedule-groups/ut-req", "{}"))
            .await
            .unwrap();
        let arn = "arn:aws:scheduler:us-east-1:111122223333:schedule-group/ut-req";
        let encoded =
            percent_encoding::utf8_percent_encode(arn, percent_encoding::NON_ALPHANUMERIC)
                .to_string();
        let err = svc
            .handle(make_request(
                Method::DELETE,
                &format!("/tags/{encoded}"),
                "",
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ValidationException");
    }

    #[tokio::test]
    async fn tag_resource_rejects_schedule_arn() {
        let svc = SchedulerService::new(make_state());
        let arn = "arn:aws:scheduler:us-east-1:111122223333:schedule/default/foo";
        let encoded =
            percent_encoding::utf8_percent_encode(arn, percent_encoding::NON_ALPHANUMERIC)
                .to_string();
        let err = svc
            .handle(make_request(
                Method::POST,
                &format!("/tags/{encoded}"),
                r#"{"Tags":[{"Key":"env","Value":"prod"}]}"#,
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "ValidationException");
    }

    #[tokio::test]
    async fn iam_action_for_maps_create_schedule_to_schedule_arn() {
        let svc = SchedulerService::new(make_state());
        let body = json!({
            "GroupName": "prod",
            "ScheduleExpression": "rate(1 minute)",
            "FlexibleTimeWindow": { "Mode": "OFF" },
            "Target": {
                "Arn": "arn:aws:sqs:us-east-1:111122223333:q",
                "RoleArn": "arn:aws:iam::111122223333:role/s"
            }
        })
        .to_string();
        let req = make_request(Method::POST, "/schedules/s1", &body);
        let action = svc.iam_action_for(&req).unwrap();
        assert_eq!(action.service, "scheduler");
        assert_eq!(action.action, "CreateSchedule");
        assert_eq!(
            action.resource,
            "arn:aws:scheduler:us-east-1:111122223333:schedule/prod/s1"
        );
    }

    #[tokio::test]
    async fn iam_action_for_get_schedule_uses_group_name_query_param() {
        let svc = SchedulerService::new(make_state());
        let req = make_request(Method::GET, "/schedules/s1?groupName=mygrp", "");
        let action = svc.iam_action_for(&req).unwrap();
        assert_eq!(action.action, "GetSchedule");
        assert!(action.resource.contains("schedule/mygrp/s1"));
    }

    #[tokio::test]
    async fn iam_action_for_schedule_group_ops() {
        let svc = SchedulerService::new(make_state());
        let action = svc
            .iam_action_for(&make_request(Method::POST, "/schedule-groups/g1", "{}"))
            .unwrap();
        assert_eq!(action.action, "CreateScheduleGroup");
        assert_eq!(
            action.resource,
            "arn:aws:scheduler:us-east-1:111122223333:schedule-group/g1"
        );
    }

    #[tokio::test]
    async fn iam_action_for_list_ops_return_wildcard() {
        let svc = SchedulerService::new(make_state());
        assert_eq!(
            svc.iam_action_for(&make_request(Method::GET, "/schedules", ""))
                .unwrap()
                .resource,
            "*"
        );
        assert_eq!(
            svc.iam_action_for(&make_request(Method::GET, "/schedule-groups", ""))
                .unwrap()
                .resource,
            "*"
        );
    }

    #[tokio::test]
    async fn iam_condition_keys_for_emits_schedule_group() {
        let svc = SchedulerService::new(make_state());
        let body = json!({
            "GroupName": "prod",
            "ScheduleExpression": "rate(1 minute)",
            "FlexibleTimeWindow": { "Mode": "OFF" },
            "Target": {
                "Arn": "arn:aws:sqs:us-east-1:1:q",
                "RoleArn": "arn:aws:iam::1:role/s"
            }
        })
        .to_string();
        let req = make_request(Method::POST, "/schedules/s", &body);
        let action = svc.iam_action_for(&req).unwrap();
        let keys = svc.iam_condition_keys_for(&req, &action);
        assert_eq!(
            keys.get("scheduler:schedulegroup"),
            Some(&vec!["prod".to_string()])
        );
    }

    #[tokio::test]
    async fn iam_condition_keys_for_get_uses_query_group() {
        let svc = SchedulerService::new(make_state());
        let req = make_request(Method::GET, "/schedules/s?groupName=alt", "");
        let action = svc.iam_action_for(&req).unwrap();
        let keys = svc.iam_condition_keys_for(&req, &action);
        assert_eq!(
            keys.get("scheduler:schedulegroup"),
            Some(&vec!["alt".to_string()])
        );
    }

    #[tokio::test]
    async fn iam_condition_keys_for_list_is_empty() {
        let svc = SchedulerService::new(make_state());
        let req = make_request(Method::GET, "/schedules", "");
        let action = svc.iam_action_for(&req).unwrap();
        assert!(svc.iam_condition_keys_for(&req, &action).is_empty());
    }

    #[tokio::test]
    async fn resource_tags_for_returns_group_tags() {
        let svc = SchedulerService::new(make_state());
        svc.handle(make_request(
            Method::POST,
            "/schedule-groups/tagged",
            r#"{"Tags":[{"Key":"env","Value":"prod"}]}"#,
        ))
        .await
        .unwrap();
        let tags = svc
            .resource_tags_for("arn:aws:scheduler:us-east-1:111122223333:schedule-group/tagged")
            .unwrap();
        assert_eq!(tags.get("env"), Some(&"prod".to_string()));
    }

    #[tokio::test]
    async fn resource_tags_for_unknown_group_returns_none() {
        let svc = SchedulerService::new(make_state());
        let tags = svc
            .resource_tags_for("arn:aws:scheduler:us-east-1:111122223333:schedule-group/missing");
        // Group missing -> None; distinguishable from "group exists, no tags" (Some(empty))
        assert!(tags.is_none());
    }

    #[tokio::test]
    async fn request_tags_from_extracts_create_group_tags() {
        let svc = SchedulerService::new(make_state());
        let req = make_request(
            Method::POST,
            "/schedule-groups/g",
            r#"{"Tags":[{"Key":"a","Value":"1"},{"Key":"b","Value":"2"}]}"#,
        );
        let tags = svc.request_tags_from(&req, "CreateScheduleGroup").unwrap();
        assert_eq!(tags.get("a"), Some(&"1".to_string()));
        assert_eq!(tags.get("b"), Some(&"2".to_string()));
    }

    #[tokio::test]
    async fn request_tags_from_returns_none_for_non_tag_actions() {
        let svc = SchedulerService::new(make_state());
        let req = make_request(Method::GET, "/schedules", "");
        assert!(svc.request_tags_from(&req, "ListSchedules").is_none());
    }

    #[tokio::test]
    async fn iam_enforceable_is_true() {
        let svc = SchedulerService::new(make_state());
        assert!(svc.iam_enforceable());
    }

    #[tokio::test]
    async fn at_expression_accepted() {
        let svc = SchedulerService::new(make_state());
        let body = json!({
            "ScheduleExpression": "at(2030-01-01T12:00:00)",
            "FlexibleTimeWindow": { "Mode": "OFF" },
            "Target": {
                "Arn": "arn:aws:sqs:us-east-1:111122223333:q",
                "RoleArn": "arn:aws:iam::111122223333:role/s"
            },
            "ActionAfterCompletion": "DELETE"
        })
        .to_string();
        let resp = svc
            .handle(make_request(Method::POST, "/schedules/once", &body))
            .await
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
    }
}
