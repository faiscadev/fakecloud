//! AWS EventBridge Pipes restJson1 service dispatch + control plane.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use http::{Method, StatusCode};
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::{SnapshotHook, SnapshotStore};

use crate::state::{
    is_transient_state, PipesSnapshot, SharedPipesState, PIPES_SNAPSHOT_SCHEMA_VERSION,
    STATE_CREATING, STATE_DELETING, STATE_RUNNING, STATE_STARTING, STATE_STOPPED, STATE_STOPPING,
    STATE_UPDATING,
};

const SUPPORTED_ACTIONS: &[&str] = &[
    "CreatePipe",
    "DeletePipe",
    "DescribePipe",
    "ListPipes",
    "ListTagsForResource",
    "StartPipe",
    "StopPipe",
    "TagResource",
    "UntagResource",
    "UpdatePipe",
];

/// How long a pipe lingers in a transient state before the async tick settles
/// it. Short enough that tests poll through it quickly, non-zero so the
/// transient state is observable (AWS CreatePipe genuinely returns CREATING).
const SETTLE_DELAY: Duration = Duration::from_millis(120);

/// Fields a client may set/replace via CreatePipe; echoed back on Describe.
const PIPE_INPUT_FIELDS: &[&str] = &[
    "Description",
    "Source",
    "SourceParameters",
    "Enrichment",
    "EnrichmentParameters",
    "Target",
    "TargetParameters",
    "RoleArn",
    "LogConfiguration",
    "KmsKeyIdentifier",
];

/// Fields UpdatePipe may replace (Source/Name/Tags are immutable on update).
const PIPE_UPDATE_FIELDS: &[&str] = &[
    "Description",
    "SourceParameters",
    "Enrichment",
    "EnrichmentParameters",
    "Target",
    "TargetParameters",
    "RoleArn",
    "LogConfiguration",
    "KmsKeyIdentifier",
];

/// AWS always echoes the source-typed parameter sub-block on DescribePipe,
/// populated with defaults, even when the caller sent no `SourceParameters`.
/// For an SQS source that block is `SqsQueueParameters` with `BatchSize=10`
/// and `MaximumBatchingWindowInSeconds=0`; the terraform-provider-aws
/// `aws_pipes_pipe` resource reads it back unconditionally, so a pipe created
/// without source parameters must still report these defaults. Existing
/// values (e.g. a caller-supplied batch size, or a `FilterCriteria` alongside)
/// are preserved.
pub fn ensure_source_param_defaults(pipe: &mut Map<String, Value>, source_arn: &str) {
    if !source_arn.contains(":sqs:") {
        return;
    }
    let source_params = pipe
        .entry("SourceParameters".to_string())
        .or_insert_with(|| json!({}));
    let Some(source_params) = source_params.as_object_mut() else {
        return;
    };
    let sqs_params = source_params
        .entry("SqsQueueParameters".to_string())
        .or_insert_with(|| json!({}));
    if let Some(sqs_params) = sqs_params.as_object_mut() {
        sqs_params
            .entry("BatchSize".to_string())
            .or_insert_with(|| json!(10));
        sqs_params
            .entry("MaximumBatchingWindowInSeconds".to_string())
            .or_insert_with(|| json!(0));
    }
}

/// AWS treats an empty `InputTemplate` as "no transform": DescribePipe omits
/// it rather than echoing `""`. The terraform-provider-aws resource clears a
/// target transform by sending `TargetParameters.InputTemplate = ""` (see its
/// update code: "have to set the input to an empty string otherwise it doesn't
/// get overwritten"), and then asserts the attribute is absent. Strip an empty
/// `InputTemplate` from the target/enrichment parameter blocks, and drop a
/// block that becomes empty so it reads back as absent.
///
/// The same "empty to overwrite" pattern applies to the source filter: to clear
/// `filter_criteria` the provider sends `SourceParameters.FilterCriteria = {}`
/// (an empty `FilterCriteria` with no `Filters`; see `expandUpdatePipeSourceParameters`),
/// and AWS's flatten emits a `filter_criteria` block only when `FilterCriteria`
/// is non-nil. So a FilterCriteria carrying no filters must read back as absent —
/// strip it, or the provider sees one (empty) block where it expects zero.
pub fn normalize_empty_input_templates(pipe: &mut Map<String, Value>) {
    for key in ["TargetParameters", "EnrichmentParameters"] {
        let drop = match pipe.get_mut(key).and_then(Value::as_object_mut) {
            Some(params) => {
                if params.get("InputTemplate").and_then(Value::as_str) == Some("") {
                    params.remove("InputTemplate");
                }
                params.is_empty()
            }
            None => false,
        };
        if drop {
            pipe.remove(key);
        }
    }

    // Drop an empty `SourceParameters.FilterCriteria` (Filters absent or empty)
    // so it reads back as no filter at all. Leave the surrounding
    // SourceParameters block in place — it still carries the source-typed
    // parameter defaults (e.g. SqsQueueParameters).
    if let Some(sp) = pipe
        .get_mut("SourceParameters")
        .and_then(Value::as_object_mut)
    {
        let drop_fc = match sp.get("FilterCriteria").and_then(Value::as_object) {
            Some(fc) => fc
                .get("Filters")
                .and_then(Value::as_array)
                .map(|f| f.is_empty())
                .unwrap_or(true),
            None => false,
        };
        if drop_fc {
            sp.remove("FilterCriteria");
        }
    }
}

pub struct PipesService {
    state: SharedPipesState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl PipesService {
    pub fn new(state: SharedPipesState) -> Self {
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

    /// CloudFormation provisioner hook: write the whole pipes state through to
    /// disk after the provisioner mutates it directly.
    pub fn snapshot_hook(&self) -> Option<SnapshotHook> {
        let store = self.snapshot_store.clone()?;
        let state = self.state.clone();
        let lock = self.snapshot_lock.clone();
        Some(Arc::new(move || {
            let store = store.clone();
            let state = state.clone();
            let lock = lock.clone();
            Box::pin(async move {
                save_snapshot_static(state, Some(store), lock).await;
            })
        }))
    }

    async fn save_snapshot(&self) {
        save_snapshot_static(
            self.state.clone(),
            self.snapshot_store.clone(),
            self.snapshot_lock.clone(),
        )
        .await;
    }

    /// Re-drive any pipe left mid-transition by a restart. Called once at boot
    /// after the snapshot is loaded; a single pass under the write lock resets
    /// transient pipes and spawns exactly one settle task each, so no pipe ends
    /// up with two concurrent tickers.
    pub async fn recover_persisted_pipes(&self) {
        let pending: Vec<(String, String)> = {
            let accounts = self.state.read();
            let mut out = Vec::new();
            for (account_id, st) in &accounts.accounts {
                for (name, pipe) in &st.pipes {
                    let cur = pipe
                        .get("CurrentState")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    if is_transient_state(cur) {
                        out.push((account_id.clone(), name.clone()));
                    }
                }
            }
            out
        };
        for (account_id, name) in pending {
            self.spawn_settle(account_id, name);
        }
    }

    /// Spawn the async tick that advances a transient pipe to its settled
    /// state (and re-saves the snapshot). Mirrors the elasticache cluster
    /// transition pattern.
    fn spawn_settle(&self, account_id: String, name: String) {
        let state = self.state.clone();
        let store = self.snapshot_store.clone();
        let lock = self.snapshot_lock.clone();
        tokio::spawn(async move {
            tokio::time::sleep(SETTLE_DELAY).await;
            {
                let mut accounts = state.write();
                if let Some(st) = accounts.accounts.get_mut(&account_id) {
                    settle_pipe(st, &name);
                }
            }
            save_snapshot_static(state, store, lock).await;
        });
    }

    fn arn(&self, account: &str, region: &str, name: &str) -> String {
        Arn::new("pipes", region, account, &format!("pipe/{name}")).to_string()
    }

    /// Map an incoming restJson1 request to its operation name from the URI
    /// path + method, per the Pipes Smithy `@http` traits.
    fn resolve_action(req: &AwsRequest) -> Option<&'static str> {
        let segs = &req.path_segments;
        // `path_segments` splits on `/` and strips empty parts, so a request
        // like `POST /v1/pipes/` collapses to `["v1","pipes"]` (a 2-segment
        // collection route) and `GET /tags/` collapses to `["tags"]`. AWS's
        // contract for these single-resource paths is that an empty path label
        // (Name / resourceArn) is a ValidationException, not "operation not
        // found." Detect the trailing slash on the original URI and promote the
        // request to the resource route with an empty label so the handler
        // returns the right validation error instead of a 404.
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let trailing_slash = raw.ends_with('/') && raw.matches('/').count() >= 2;

        // Tag family: /tags/{resourceArn}
        if segs.first().map(String::as_str) == Some("tags") {
            return match req.method {
                Method::GET => Some("ListTagsForResource"),
                Method::POST => Some("TagResource"),
                Method::DELETE => Some("UntagResource"),
                _ => None,
            };
        }
        if segs.first().map(String::as_str) != Some("v1") {
            return None;
        }
        if segs.get(1).map(String::as_str) != Some("pipes") {
            return None;
        }
        // A trailing slash on `/v1/pipes/` means an empty {Name} path label:
        // route to the single-resource op so the handler emits a
        // ValidationException rather than falling through to ListPipes / 404.
        let effective_len = if segs.len() == 2 && trailing_slash {
            3
        } else {
            segs.len()
        };
        match (effective_len, &req.method) {
            // /v1/pipes
            (2, &Method::GET) => Some("ListPipes"),
            // /v1/pipes/{Name}
            (3, &Method::POST) => Some("CreatePipe"),
            (3, &Method::GET) => Some("DescribePipe"),
            (3, &Method::PUT) => Some("UpdatePipe"),
            (3, &Method::DELETE) => Some("DeletePipe"),
            // /v1/pipes/{Name}/start | /stop
            (4, &Method::POST) => match segs.get(3).map(String::as_str) {
                Some("start") => Some("StartPipe"),
                Some("stop") => Some("StopPipe"),
                _ => None,
            },
            _ => None,
        }
    }

    fn dispatch(&self, action: &str, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match action {
            "CreatePipe" => self.create_pipe(req),
            "DescribePipe" => self.describe_pipe(req),
            "ListPipes" => self.list_pipes(req),
            "UpdatePipe" => self.update_pipe(req),
            "DeletePipe" => self.delete_pipe(req),
            "StartPipe" => self.start_pipe(req),
            "StopPipe" => self.stop_pipe(req),
            "TagResource" => self.tag_resource(req),
            "UntagResource" => self.untag_resource(req),
            "ListTagsForResource" => self.list_tags_for_resource(req),
            other => Err(AwsServiceError::action_not_implemented("pipes", other)),
        }
    }

    fn create_pipe(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = pipe_name_from_path(req)?;
        validate_pipe_name(&name)?;
        let body = req.json_body();

        let source = body
            .get("Source")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| validation_error("Source is required"))?
            .to_string();
        let target = body
            .get("Target")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| validation_error("Target is required"))?
            .to_string();
        let role_arn = body
            .get("RoleArn")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| validation_error("RoleArn is required"))?
            .to_string();

        let desired = desired_state_from(body.get("DesiredState"), STATE_RUNNING);
        let arn = self.arn(&req.account_id, &req.region, &name);
        let now = now_epoch_secs();

        let mut pipe = Map::new();
        pipe.insert("Name".into(), json!(name));
        pipe.insert("Arn".into(), json!(arn));
        pipe.insert("Source".into(), json!(source));
        pipe.insert("Target".into(), json!(target));
        pipe.insert("RoleArn".into(), json!(role_arn));
        for field in PIPE_INPUT_FIELDS {
            if let Some(v) = body.get(*field) {
                pipe.insert((*field).into(), v.clone());
            }
        }
        pipe.insert("DesiredState".into(), json!(desired));
        pipe.insert("CurrentState".into(), json!(STATE_CREATING));
        pipe.insert("StateReason".into(), json!("Pipe is being created"));
        pipe.insert("CreationTime".into(), json!(now));
        pipe.insert("LastModifiedTime".into(), json!(now));
        ensure_source_param_defaults(&mut pipe, &source);
        normalize_empty_input_templates(&mut pipe);

        let tags = tag_map_from(body.get("Tags"));

        {
            let mut accounts = self.state.write();
            let st = accounts.get_or_create(&req.account_id);
            if st.pipes.contains_key(&name) {
                return Err(conflict_error(
                    format!("Pipe with Name {name} already exists."),
                    &name,
                ));
            }
            if !tags.is_empty() {
                st.tags.insert(arn.clone(), tags.clone());
                pipe.insert(
                    "Tags".into(),
                    json!(tags
                        .iter()
                        .map(|(k, v)| (k.clone(), Value::String(v.clone())))
                        .collect::<Map<String, Value>>()),
                );
            }
            st.pipes.insert(name.clone(), Value::Object(pipe));
        }
        self.spawn_settle(req.account_id.clone(), name.clone());

        Ok(AwsResponse::ok_json(json!({
            "Arn": arn,
            "Name": name,
            "DesiredState": desired,
            "CurrentState": STATE_CREATING,
            "CreationTime": now,
            "LastModifiedTime": now,
        })))
    }

    fn describe_pipe(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = pipe_name_from_path(req)?;
        // Lazy settle: if the pipe is transient and has been so for longer than
        // SETTLE_DELAY, advance it now (removing it if it was DELETING). The
        // per-request `spawn_settle` task and the runner drain normally do this,
        // but both depend on a background task getting scheduled promptly; on a
        // heavily oversubscribed CI runner that can lag past the provider's
        // delete/create waiter budget, hanging a pipe in DELETING. Driving the
        // settle from the client's own DescribePipe poll makes the lifecycle
        // converge on the poll cadence with no dependency on background
        // scheduling — the waiter that is asking "is it gone yet?" is exactly
        // what finishes the deletion.
        self.settle_if_overdue(&req.account_id, &name);
        let accounts = self.state.read();
        let pipe = accounts
            .get(&req.account_id)
            .and_then(|st| st.pipes.get(&name))
            .ok_or_else(|| not_found(&name))?;
        Ok(AwsResponse::ok_json(pipe.clone()))
    }

    /// Advance a single pipe to its settled state if it has sat in a transient
    /// state longer than `SETTLE_DELAY`. Read-locks to check first and only
    /// takes the write lock when there is actually work to do, so the common
    /// case (a settled pipe) stays on the read path.
    fn settle_if_overdue(&self, account_id: &str, name: &str) {
        let now = now_epoch_secs();
        let overdue = {
            let accounts = self.state.read();
            accounts
                .get(account_id)
                .and_then(|st| st.pipes.get(name))
                .map(|pipe| {
                    let cur = pipe
                        .get("CurrentState")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    let last = pipe
                        .get("LastModifiedTime")
                        .and_then(Value::as_f64)
                        .unwrap_or(0.0);
                    is_transient_state(cur) && now - last >= SETTLE_DELAY.as_secs_f64()
                })
                .unwrap_or(false)
        };
        if !overdue {
            return;
        }
        let mut accounts = self.state.write();
        if let Some(st) = accounts.accounts.get_mut(account_id) {
            settle_pipe(st, name);
        }
    }

    fn list_pipes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let q = &req.query_params;
        let name_prefix = q.get("NamePrefix").map(String::as_str);
        let desired = q.get("DesiredState").map(String::as_str);
        let current = q.get("CurrentState").map(String::as_str);
        let source_prefix = q.get("SourcePrefix").map(String::as_str);
        let target_prefix = q.get("TargetPrefix").map(String::as_str);

        // Validate the query-string filters against their Smithy constraints so
        // that malformed inputs return a ValidationException instead of being
        // silently ignored (which would mask a client bug behind an empty page).
        if let Some(p) = name_prefix {
            validate_pipe_name(p)?;
        }
        if let Some(v) = current {
            validate_pipe_state(v)?;
        }
        if let Some(v) = desired {
            validate_requested_pipe_state(v)?;
        }
        if let Some(v) = source_prefix {
            validate_resource_arn_len("SourcePrefix", v)?;
        }
        if let Some(v) = target_prefix {
            validate_resource_arn_len("TargetPrefix", v)?;
        }
        if let Some(t) = q.get("NextToken") {
            if t.is_empty() || t.len() > 2048 {
                return Err(validation_error("NextToken must be 1-2048 characters"));
            }
        }
        let limit: usize = match q.get("Limit") {
            Some(raw) => {
                let n: i64 = raw
                    .parse()
                    .map_err(|_| validation_error("Limit must be an integer"))?;
                if !(1..=100).contains(&n) {
                    return Err(validation_error("Limit must be between 1 and 100"));
                }
                n as usize
            }
            None => 100,
        };
        let after = q.get("NextToken").cloned();

        let accounts = self.state.read();
        let mut summaries: Vec<Value> = Vec::new();
        if let Some(st) = accounts.get(&req.account_id) {
            // BTreeMap iterates names in sorted order, giving a stable page
            // boundary for the opaque NextToken (= last name returned).
            for (name, pipe) in &st.pipes {
                if let Some(p) = name_prefix {
                    if !name.starts_with(p) {
                        continue;
                    }
                }
                if let Some(after) = &after {
                    if name <= after {
                        continue;
                    }
                }
                if let Some(d) = desired {
                    if pipe.get("DesiredState").and_then(Value::as_str) != Some(d) {
                        continue;
                    }
                }
                if let Some(c) = current {
                    if pipe.get("CurrentState").and_then(Value::as_str) != Some(c) {
                        continue;
                    }
                }
                if let Some(sp) = source_prefix {
                    if !pipe
                        .get("Source")
                        .and_then(Value::as_str)
                        .is_some_and(|s| s.starts_with(sp))
                    {
                        continue;
                    }
                }
                if let Some(tp) = target_prefix {
                    if !pipe
                        .get("Target")
                        .and_then(Value::as_str)
                        .is_some_and(|s| s.starts_with(tp))
                    {
                        continue;
                    }
                }
                summaries.push(pipe_summary(pipe));
            }
        }

        let mut next_token = Value::Null;
        if summaries.len() > limit {
            let last = summaries[limit - 1]
                .get("Name")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            summaries.truncate(limit);
            next_token = json!(last);
        }

        Ok(AwsResponse::ok_json(json!({
            "Pipes": summaries,
            "NextToken": next_token,
        })))
    }

    fn update_pipe(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = pipe_name_from_path(req)?;
        let body = req.json_body();
        if body
            .get("RoleArn")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .is_none()
        {
            return Err(validation_error("RoleArn is required"));
        }
        let now = now_epoch_secs();

        let response = {
            let mut accounts = self.state.write();
            let st = accounts.get_or_create(&req.account_id);
            let pipe = st.pipes.get_mut(&name).ok_or_else(|| not_found(&name))?;
            let obj = pipe
                .as_object_mut()
                .ok_or_else(|| validation_error("corrupt pipe state"))?;
            // UpdatePipe is a full replace of the updatable fields: a field the
            // client omits is cleared, not left at its previous value. AWS does
            // this (e.g. dropping `TargetParameters.InputTemplate` on a later
            // apply must make DescribePipe stop reporting it), and the
            // terraform-provider-aws resource asserts the omitted attribute is
            // gone.
            for field in PIPE_UPDATE_FIELDS {
                match body.get(*field) {
                    Some(v) => {
                        obj.insert((*field).into(), v.clone());
                    }
                    None => {
                        obj.remove(*field);
                    }
                }
            }
            let desired = if body.get("DesiredState").is_some() {
                let d = desired_state_from(body.get("DesiredState"), STATE_RUNNING);
                obj.insert("DesiredState".into(), json!(d));
                d
            } else {
                obj.get("DesiredState")
                    .and_then(Value::as_str)
                    .unwrap_or(STATE_RUNNING)
                    .to_string()
            };
            obj.insert("CurrentState".into(), json!(STATE_UPDATING));
            obj.insert("StateReason".into(), json!("Pipe is being updated"));
            obj.insert("LastModifiedTime".into(), json!(now));
            if let Some(source) = obj
                .get("Source")
                .and_then(Value::as_str)
                .map(str::to_string)
            {
                ensure_source_param_defaults(obj, &source);
            }
            normalize_empty_input_templates(obj);
            let arn = obj
                .get("Arn")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let creation = obj.get("CreationTime").cloned().unwrap_or(json!(now));
            json!({
                "Arn": arn,
                "Name": name,
                "DesiredState": desired,
                "CurrentState": STATE_UPDATING,
                "CreationTime": creation,
                "LastModifiedTime": now,
            })
        };
        self.spawn_settle(req.account_id.clone(), name.clone());
        Ok(AwsResponse::ok_json(response))
    }

    fn start_pipe(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.transition(req, STATE_STARTING, STATE_RUNNING)
    }

    fn stop_pipe(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        self.transition(req, STATE_STOPPING, STATE_STOPPED)
    }

    /// Shared StartPipe/StopPipe body: flip into the transient state, set the
    /// desired settled state, and let the async tick complete it.
    fn transition(
        &self,
        req: &AwsRequest,
        transient: &str,
        desired: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = pipe_name_from_path(req)?;
        let now = now_epoch_secs();
        let response = {
            let mut accounts = self.state.write();
            let st = accounts.get_or_create(&req.account_id);
            let pipe = st.pipes.get_mut(&name).ok_or_else(|| not_found(&name))?;
            let obj = pipe
                .as_object_mut()
                .ok_or_else(|| validation_error("corrupt pipe state"))?;
            obj.insert("DesiredState".into(), json!(desired));
            obj.insert("CurrentState".into(), json!(transient));
            obj.insert("LastModifiedTime".into(), json!(now));
            let arn = obj
                .get("Arn")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let creation = obj.get("CreationTime").cloned().unwrap_or(json!(now));
            json!({
                "Arn": arn,
                "Name": name,
                "DesiredState": desired,
                "CurrentState": transient,
                "CreationTime": creation,
                "LastModifiedTime": now,
            })
        };
        self.spawn_settle(req.account_id.clone(), name.clone());
        Ok(AwsResponse::ok_json(response))
    }

    fn delete_pipe(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = pipe_name_from_path(req)?;
        let now = now_epoch_secs();
        let response = {
            let mut accounts = self.state.write();
            let st = accounts.get_or_create(&req.account_id);
            let pipe = st.pipes.get_mut(&name).ok_or_else(|| not_found(&name))?;
            let obj = pipe
                .as_object_mut()
                .ok_or_else(|| validation_error("corrupt pipe state"))?;
            obj.insert("CurrentState".into(), json!(STATE_DELETING));
            obj.insert("StateReason".into(), json!("Pipe is being deleted"));
            obj.insert("LastModifiedTime".into(), json!(now));
            let arn = obj
                .get("Arn")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let creation = obj.get("CreationTime").cloned().unwrap_or(json!(now));
            json!({
                "Arn": arn,
                "Name": name,
                "DesiredState": "STOPPED",
                "CurrentState": STATE_DELETING,
                "CreationTime": creation,
                "LastModifiedTime": now,
            })
        };
        self.spawn_settle(req.account_id.clone(), name.clone());
        Ok(AwsResponse::ok_json(response))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = resource_arn_from_path(req)?;
        let accounts = self.state.read();
        let tags = accounts
            .get(&req.account_id)
            .and_then(|st| st.tags.get(&arn))
            .cloned()
            .unwrap_or_default();
        let map: Map<String, Value> = tags
            .into_iter()
            .map(|(k, v)| (k, Value::String(v)))
            .collect();
        Ok(AwsResponse::ok_json(json!({ "tags": map })))
    }

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = resource_arn_from_path(req)?;
        let body = req.json_body();
        let new_tags = tag_map_from(body.get("tags"));
        {
            let mut accounts = self.state.write();
            let st = accounts.get_or_create(&req.account_id);
            let entry = st.tags.entry(arn.clone()).or_default();
            for (k, v) in new_tags {
                entry.insert(k, v);
            }
            sync_tags_into_pipe(st, &arn);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = resource_arn_from_path(req)?;
        // tagKeys is a repeated query param; raw_query preserves repeats.
        let keys: Vec<String> = req
            .raw_query
            .split('&')
            .filter_map(|kv| kv.strip_prefix("tagKeys="))
            .map(url_decode)
            .collect();
        {
            let mut accounts = self.state.write();
            let st = accounts.get_or_create(&req.account_id);
            if let Some(entry) = st.tags.get_mut(&arn) {
                for k in &keys {
                    entry.remove(k);
                }
            }
            sync_tags_into_pipe(st, &arn);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }
}

#[async_trait]
impl AwsService for PipesService {
    fn service_name(&self) -> &str {
        "pipes"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let Some(action) = Self::resolve_action(&req) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            ));
        };
        let result = self.dispatch(action, &req);
        if matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) && is_mutating(action) {
            self.save_snapshot().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }
}

fn is_mutating(action: &str) -> bool {
    matches!(
        action,
        "CreatePipe"
            | "UpdatePipe"
            | "DeletePipe"
            | "StartPipe"
            | "StopPipe"
            | "TagResource"
            | "UntagResource"
    )
}

/// Advance a transient pipe to its settled state. Removes the pipe (and tags)
/// when it was DELETING; otherwise resolves to its DesiredState or the natural
/// terminus of the in-flight transition.
fn settle_pipe(st: &mut crate::state::PipesState, name: &str) {
    let Some(pipe) = st.pipes.get(name) else {
        return;
    };
    let cur = pipe
        .get("CurrentState")
        .and_then(Value::as_str)
        .unwrap_or("");
    if cur == STATE_DELETING {
        if let Some(arn) = pipe.get("Arn").and_then(Value::as_str) {
            st.tags.remove(arn);
            // Purge this pipe's source checkpoints. They're keyed by the pipe
            // ARN, so leaving them behind lets a later same-name pipe (which
            // gets the same ARN) resume from a dead pipe's cursor and silently
            // skip its own backlog (bug-hunt 2026-07-01 M2/L1).
            let prefix = format!("{arn}#");
            st.source_checkpoints
                .retain(|k, _| k != arn && !k.starts_with(&prefix));
        }
        st.pipes.remove(name);
        return;
    }
    let desired = pipe
        .get("DesiredState")
        .and_then(Value::as_str)
        .unwrap_or(STATE_RUNNING)
        .to_string();
    let settled = match cur {
        STATE_STARTING => STATE_RUNNING,
        STATE_STOPPING => STATE_STOPPED,
        // CREATING / UPDATING resolve to whatever the client asked for.
        _ => {
            if desired == STATE_STOPPED {
                STATE_STOPPED
            } else {
                STATE_RUNNING
            }
        }
    };
    if let Some(obj) = st.pipes.get_mut(name).and_then(Value::as_object_mut) {
        obj.insert("CurrentState".into(), json!(settled));
        let reason = if settled == STATE_RUNNING {
            "Pipe is running"
        } else {
            "Pipe is stopped"
        };
        obj.insert("StateReason".into(), json!(reason));
    }
}

/// Rescue any pipe that has been sitting in a transient state (CREATING /
/// UPDATING / STARTING / STOPPING / DELETING) for at least `min_age_secs`,
/// settling it the same way its per-request `spawn_settle` task would.
///
/// Each lifecycle call spawns a one-shot task that sleeps `SETTLE_DELAY` and
/// then settles the pipe. On a CPU-starved runner (e.g. a 2-core CI box under
/// several concurrent terraform applies) those spawned tasks can be delayed
/// long enough that the provider's delete/update waiter times out first — a
/// pipe stuck in DELETING never gets removed and the waiter blocks for its full
/// 30-minute budget. The Pipes runner already ticks on a persistent task, so it
/// drains overdue transients as a backstop that doesn't depend on fresh task
/// spawns landing promptly. The `min_age_secs` gate keeps this off the fast
/// path: under normal load the spawned task settles within `SETTLE_DELAY` and
/// the pipe is gone before it ages past the threshold, so this rescues only
/// genuinely-starved settles.
pub fn drain_overdue_transient_pipes(state: &SharedPipesState, min_age_secs: f64) {
    let now = now_epoch_secs();

    // Read-lock-first: collect the overdue `(account, name)` pairs under a
    // shared read lock, and only escalate to the write lock when there is
    // actually something to settle. The common case — no transient pipe past
    // the age gate — takes only the read lock, so the runner's per-tick drain
    // does not convoy against the provider's DescribePipe read flood.
    let overdue: Vec<(String, String)> = {
        let accounts = state.read();
        let mut out = Vec::new();
        for (account_id, st) in &accounts.accounts {
            for (name, pipe) in &st.pipes {
                let cur = pipe
                    .get("CurrentState")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                if !is_transient_state(cur) {
                    continue;
                }
                let last = pipe
                    .get("LastModifiedTime")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0);
                if now - last >= min_age_secs {
                    out.push((account_id.clone(), name.clone()));
                }
            }
        }
        out
    };
    if overdue.is_empty() {
        return;
    }

    let mut accounts = state.write();
    for (account_id, name) in overdue {
        if let Some(st) = accounts.accounts.get_mut(&account_id) {
            settle_pipe(st, &name);
        }
    }
}

/// Build a `Pipe` list summary (a subset of the full describe object).
fn pipe_summary(pipe: &Value) -> Value {
    let mut out = Map::new();
    for field in [
        "Name",
        "Arn",
        "DesiredState",
        "CurrentState",
        "StateReason",
        "CreationTime",
        "LastModifiedTime",
        "Source",
        "Target",
        "Enrichment",
    ] {
        if let Some(v) = pipe.get(field) {
            out.insert(field.into(), v.clone());
        }
    }
    Value::Object(out)
}

/// Re-materialize a pipe's stored `Tags` from the authoritative per-ARN tag
/// store after a TagResource/UntagResource so Describe stays consistent.
fn sync_tags_into_pipe(st: &mut crate::state::PipesState, arn: &str) {
    let tags = st.tags.get(arn).cloned().unwrap_or_default();
    let name = st
        .pipes
        .iter()
        .find(|(_, p)| p.get("Arn").and_then(Value::as_str) == Some(arn))
        .map(|(n, _)| n.clone());
    if let Some(name) = name {
        if let Some(obj) = st.pipes.get_mut(&name).and_then(Value::as_object_mut) {
            if tags.is_empty() {
                obj.remove("Tags");
            } else {
                obj.insert(
                    "Tags".into(),
                    json!(tags
                        .into_iter()
                        .map(|(k, v)| (k, Value::String(v)))
                        .collect::<Map<String, Value>>()),
                );
            }
        }
    }
}

fn pipe_name_from_path(req: &AwsRequest) -> Result<String, AwsServiceError> {
    req.path_segments
        .get(2)
        .map(|s| url_decode(s))
        .filter(|s| !s.is_empty())
        .ok_or_else(|| validation_error("Pipe name is required"))
}

fn resource_arn_from_path(req: &AwsRequest) -> Result<String, AwsServiceError> {
    // /tags/{resourceArn} — the ARN's own slashes are URL-encoded by the SDK,
    // but tolerate the decoded form by re-joining any trailing segments.
    let joined = req.path_segments[1..].join("/");
    let arn = url_decode(&joined);
    if arn.is_empty() {
        return Err(validation_error("resourceArn is required"));
    }
    // Smithy `PipeArn` is length 1..1600; reject oversize labels rather than
    // silently accepting a garbage ARN.
    if arn.len() > 1600 {
        return Err(validation_error("resourceArn must be 1-1600 characters"));
    }
    // `PipeArn` carries the pattern `^arn:aws...`. A value that is not an ARN
    // (e.g. an SDK-omitted `{resourceArn}` label left literal by a probe) is a
    // ValidationException, matching real EventBridge Pipes.
    if !arn.starts_with("arn:") {
        return Err(validation_error(format!(
            "resourceArn does not match the required ARN pattern: {arn}"
        )));
    }
    Ok(arn)
}

/// Validate a `ResourceArn`-shaped value (length 1..1600). Shared by the
/// ListPipes `SourcePrefix` / `TargetPrefix` query filters.
fn validate_resource_arn_len(field: &str, value: &str) -> Result<(), AwsServiceError> {
    if value.is_empty() || value.len() > 1600 {
        return Err(validation_error(format!(
            "{field} must be 1-1600 characters"
        )));
    }
    Ok(())
}

/// Valid `PipeState` enum values (CurrentState filter on ListPipes).
const PIPE_STATES: &[&str] = &[
    "RUNNING",
    "STOPPED",
    "CREATING",
    "UPDATING",
    "DELETING",
    "STARTING",
    "STOPPING",
    "CREATE_FAILED",
    "UPDATE_FAILED",
    "START_FAILED",
    "STOP_FAILED",
    "DELETE_FAILED",
    "CREATE_ROLLBACK_FAILED",
    "DELETE_ROLLBACK_FAILED",
    "UPDATE_ROLLBACK_FAILED",
];

fn validate_pipe_state(value: &str) -> Result<(), AwsServiceError> {
    if PIPE_STATES.contains(&value) {
        Ok(())
    } else {
        Err(validation_error(format!(
            "CurrentState must be one of: {}",
            PIPE_STATES.join(", ")
        )))
    }
}

/// Valid `RequestedPipeState` enum values (DesiredState filter on ListPipes).
fn validate_requested_pipe_state(value: &str) -> Result<(), AwsServiceError> {
    if matches!(value, "RUNNING" | "STOPPED") {
        Ok(())
    } else {
        Err(validation_error(
            "DesiredState must be one of: RUNNING, STOPPED",
        ))
    }
}

fn validate_pipe_name(name: &str) -> Result<(), AwsServiceError> {
    if name.is_empty() || name.len() > 64 {
        return Err(validation_error(
            "Pipe name must be between 1 and 64 characters",
        ));
    }
    if !name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'))
    {
        return Err(validation_error(
            "Pipe name may only contain letters, numbers, and the characters . - _",
        ));
    }
    Ok(())
}

fn desired_state_from(v: Option<&Value>, default: &str) -> String {
    match v.and_then(Value::as_str) {
        Some(STATE_STOPPED) => STATE_STOPPED.to_string(),
        Some(STATE_RUNNING) => STATE_RUNNING.to_string(),
        _ => default.to_string(),
    }
}

fn tag_map_from(v: Option<&Value>) -> std::collections::BTreeMap<String, String> {
    v.and_then(Value::as_object)
        .map(|m| {
            m.iter()
                .filter_map(|(k, val)| val.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn now_epoch_secs() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

fn validation_error(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg.into())
}

fn not_found(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "NotFoundException",
        format!("Pipe {name} does not exist."),
    )
}

fn conflict_error(msg: impl Into<String>, resource_id: &str) -> AwsServiceError {
    AwsServiceError::aws_error_with_fields(
        StatusCode::CONFLICT,
        "ConflictException",
        msg.into(),
        vec![
            ("resourceId".to_string(), resource_id.to_string()),
            ("resourceType".to_string(), "pipe".to_string()),
        ],
    )
}

/// Minimal percent-decoder for path/query segments (the SDK encodes ARNs and
/// names). Mirrors the decode used elsewhere in the gateway.
fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hi = from_hex(bytes[i + 1]);
                let lo = from_hex(bytes[i + 2]);
                if let (Some(hi), Some(lo)) = (hi, lo) {
                    out.push(hi << 4 | lo);
                    i += 3;
                    continue;
                }
                out.push(b'%');
                i += 1;
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn from_hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

async fn save_snapshot_static(
    state: SharedPipesState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: Arc<AsyncMutex<()>>,
) {
    let Some(store) = store else {
        return;
    };
    let _guard = lock.lock().await;
    let snapshot = PipesSnapshot {
        schema_version: PIPES_SNAPSHOT_SCHEMA_VERSION,
        accounts: Some(state.read().clone()),
    };
    let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let bytes = serde_json::to_vec(&snapshot)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
        store.save(&bytes)
    })
    .await;
    match join {
        Ok(Ok(())) => {}
        Ok(Err(err)) => tracing::error!(%err, "failed to write pipes snapshot"),
        Err(err) => tracing::error!(%err, "pipes snapshot task panicked"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SQS: &str = "arn:aws:sqs:us-east-1:000000000000:src";

    /// A `PipesService` whose account holds one pipe in `state` with the given
    /// CurrentState/DesiredState and a LastModifiedTime `age_secs` in the past.
    fn service_with_pipe(
        current: &str,
        desired: &str,
        age_secs: f64,
    ) -> (PipesService, SharedPipesState) {
        use parking_lot::RwLock;
        let state: SharedPipesState = Arc::new(RwLock::new(crate::state::PipesAccounts::new()));
        {
            let mut acc = state.write();
            let st = acc.get_or_create("123456789012");
            st.pipes.insert(
                "p1".into(),
                json!({
                    "Name": "p1",
                    "Arn": "arn:aws:pipes:us-east-1:123456789012:pipe/p1",
                    "CurrentState": current,
                    "DesiredState": desired,
                    "LastModifiedTime": now_epoch_secs() - age_secs,
                }),
            );
        }
        (PipesService::new(state.clone()), state)
    }

    #[test]
    fn lazy_settle_removes_overdue_deleting_pipe() {
        // A DELETING pipe older than SETTLE_DELAY is removed by a describe-time
        // settle, so the provider's delete waiter converges on its own poll.
        let (svc, state) = service_with_pipe(STATE_DELETING, "STOPPED", 5.0);
        svc.settle_if_overdue("123456789012", "p1");
        assert!(state.read().get("123456789012").unwrap().pipes.is_empty());
    }

    #[test]
    fn deleting_pipe_purges_its_source_checkpoints() {
        // A same-name recreate reuses the ARN, so stale checkpoints must not
        // survive the delete (bug-hunt 2026-07-01 M2/L1).
        let (svc, state) = service_with_pipe(STATE_DELETING, "STOPPED", 5.0);
        let arn = "arn:aws:pipes:us-east-1:123456789012:pipe/p1";
        {
            let mut st = state.write();
            let s = st.get_or_create("123456789012");
            s.source_checkpoints
                .insert(format!("{arn}#shardId-000000000000"), "42".into());
            s.source_checkpoints.insert(arn.to_string(), "seq-9".into());
            // An unrelated pipe's checkpoint must be left intact.
            s.source_checkpoints.insert(
                "arn:aws:pipes:us-east-1:123456789012:pipe/other".into(),
                "x".into(),
            );
        }
        svc.settle_if_overdue("123456789012", "p1");
        let st = state.read();
        let cps = &st.get("123456789012").unwrap().source_checkpoints;
        assert!(!cps.keys().any(|k| k.starts_with(arn)));
        assert!(cps.contains_key("arn:aws:pipes:us-east-1:123456789012:pipe/other"));
    }

    #[test]
    fn lazy_settle_advances_overdue_creating_pipe() {
        let (svc, state) = service_with_pipe(STATE_CREATING, STATE_RUNNING, 5.0);
        svc.settle_if_overdue("123456789012", "p1");
        assert_eq!(
            state.read().get("123456789012").unwrap().pipes["p1"]["CurrentState"],
            STATE_RUNNING
        );
    }

    #[test]
    fn lazy_settle_leaves_fresh_transient_pipe() {
        // Within SETTLE_DELAY the transient state is still observable.
        let (svc, state) = service_with_pipe(STATE_CREATING, STATE_RUNNING, 0.0);
        svc.settle_if_overdue("123456789012", "p1");
        assert_eq!(
            state.read().get("123456789012").unwrap().pipes["p1"]["CurrentState"],
            STATE_CREATING
        );
    }

    #[test]
    fn sqs_source_gets_default_sqs_queue_parameters() {
        let mut pipe = Map::new();
        ensure_source_param_defaults(&mut pipe, SQS);
        let sqp = &pipe["SourceParameters"]["SqsQueueParameters"];
        assert_eq!(sqp["BatchSize"], json!(10));
        assert_eq!(sqp["MaximumBatchingWindowInSeconds"], json!(0));
    }

    #[test]
    fn existing_values_and_sibling_params_preserved() {
        let mut pipe = Map::new();
        pipe.insert(
            "SourceParameters".into(),
            json!({
                "FilterCriteria": {"Filters": [{"Pattern": "{}"}]},
                "SqsQueueParameters": {"BatchSize": 5}
            }),
        );
        ensure_source_param_defaults(&mut pipe, SQS);
        let sp = &pipe["SourceParameters"];
        // A caller-supplied batch size is kept; only the missing window default
        // is filled, and the sibling FilterCriteria is untouched.
        assert_eq!(sp["SqsQueueParameters"]["BatchSize"], json!(5));
        assert_eq!(
            sp["SqsQueueParameters"]["MaximumBatchingWindowInSeconds"],
            json!(0)
        );
        assert!(sp["FilterCriteria"]["Filters"].is_array());
    }

    #[test]
    fn non_sqs_source_untouched() {
        let mut pipe = Map::new();
        ensure_source_param_defaults(&mut pipe, "arn:aws:kinesis:us-east-1:000000000000:stream/s");
        assert!(pipe.get("SourceParameters").is_none());
    }

    #[test]
    fn empty_input_template_only_block_is_dropped() {
        let mut pipe = Map::new();
        pipe.insert("TargetParameters".into(), json!({"InputTemplate": ""}));
        normalize_empty_input_templates(&mut pipe);
        assert!(pipe.get("TargetParameters").is_none());
    }

    #[test]
    fn empty_input_template_stripped_but_siblings_kept() {
        let mut pipe = Map::new();
        pipe.insert(
            "TargetParameters".into(),
            json!({"InputTemplate": "", "KinesisStreamParameters": {"PartitionKey": "pk"}}),
        );
        normalize_empty_input_templates(&mut pipe);
        let tp = &pipe["TargetParameters"];
        assert!(tp.get("InputTemplate").is_none());
        assert_eq!(tp["KinesisStreamParameters"]["PartitionKey"], "pk");
    }

    #[test]
    fn non_empty_input_template_preserved() {
        let mut pipe = Map::new();
        pipe.insert("TargetParameters".into(), json!({"InputTemplate": "<$.x>"}));
        normalize_empty_input_templates(&mut pipe);
        assert_eq!(pipe["TargetParameters"]["InputTemplate"], "<$.x>");
    }

    #[test]
    fn empty_filter_criteria_stripped_defaults_kept() {
        // Clearing a source filter sends FilterCriteria = {} alongside the
        // source-typed defaults. The empty FilterCriteria must be dropped so it
        // reads back as absent, but SqsQueueParameters must survive.
        let mut pipe = Map::new();
        pipe.insert(
            "SourceParameters".into(),
            json!({
                "SqsQueueParameters": {"BatchSize": 10, "MaximumBatchingWindowInSeconds": 0},
                "FilterCriteria": {},
            }),
        );
        normalize_empty_input_templates(&mut pipe);
        assert!(pipe["SourceParameters"].get("FilterCriteria").is_none());
        assert_eq!(
            pipe["SourceParameters"]["SqsQueueParameters"]["BatchSize"],
            10
        );
    }

    #[test]
    fn empty_filters_array_filter_criteria_stripped() {
        let mut pipe = Map::new();
        pipe.insert(
            "SourceParameters".into(),
            json!({"FilterCriteria": {"Filters": []}}),
        );
        normalize_empty_input_templates(&mut pipe);
        assert!(pipe["SourceParameters"].get("FilterCriteria").is_none());
    }

    #[test]
    fn non_empty_filter_criteria_preserved() {
        let mut pipe = Map::new();
        pipe.insert(
            "SourceParameters".into(),
            json!({"FilterCriteria": {"Filters": [{"Pattern": "{\"x\":[1]}"}]}}),
        );
        normalize_empty_input_templates(&mut pipe);
        assert_eq!(
            pipe["SourceParameters"]["FilterCriteria"]["Filters"][0]["Pattern"],
            "{\"x\":[1]}"
        );
    }

    fn make_request(method: Method, path: &str) -> AwsRequest {
        use http::HeaderMap;
        let (p, q) = match path.find('?') {
            Some(i) => (&path[..i], &path[i + 1..]),
            None => (path, ""),
        };
        let path_segments: Vec<String> = p
            .split('/')
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
            .collect();
        let query_params: std::collections::HashMap<String, String> = q
            .split('&')
            .filter(|s| !s.is_empty())
            .filter_map(|pair| {
                let (k, v) = pair.split_once('=')?;
                Some((k.to_string(), v.to_string()))
            })
            .collect();
        AwsRequest {
            service: "pipes".to_string(),
            action: String::new(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: HeaderMap::new(),
            query_params,
            body: Default::default(),
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

    #[test]
    fn pipe_state_enum_validation() {
        assert!(validate_pipe_state("RUNNING").is_ok());
        assert!(validate_pipe_state("CREATE_FAILED").is_ok());
        let err = validate_pipe_state("BOGUS").unwrap_err();
        assert_eq!(err.code(), "ValidationException");
    }

    #[test]
    fn requested_pipe_state_enum_validation() {
        assert!(validate_requested_pipe_state("RUNNING").is_ok());
        assert!(validate_requested_pipe_state("STOPPED").is_ok());
        // CREATING is a valid PipeState but not a valid *requested* state.
        assert_eq!(
            validate_requested_pipe_state("CREATING")
                .unwrap_err()
                .code(),
            "ValidationException"
        );
    }

    #[test]
    fn resource_arn_len_validation() {
        assert!(validate_resource_arn_len("SourcePrefix", "arn:aws:sqs:us-east-1:1:q").is_ok());
        assert_eq!(
            validate_resource_arn_len("SourcePrefix", "")
                .unwrap_err()
                .code(),
            "ValidationException"
        );
        let long = "a".repeat(1601);
        assert_eq!(
            validate_resource_arn_len("TargetPrefix", &long)
                .unwrap_err()
                .code(),
            "ValidationException"
        );
    }

    #[test]
    fn resource_arn_from_path_rejects_non_arn_and_oversize() {
        // A valid ARN passes.
        let req = make_request(
            Method::GET,
            "/tags/arn%3Aaws%3Apipes%3Aus-east-1%3A1%3Apipe%2Fp",
        );
        assert_eq!(
            resource_arn_from_path(&req).unwrap(),
            "arn:aws:pipes:us-east-1:1:pipe/p"
        );
        // A literal `{resourceArn}` placeholder (SDK-omitted label) is rejected.
        let req = make_request(Method::GET, "/tags/{resourceArn}");
        assert_eq!(
            resource_arn_from_path(&req).unwrap_err().code(),
            "ValidationException"
        );
        // Empty label is rejected.
        let req = make_request(Method::GET, "/tags/");
        assert_eq!(
            resource_arn_from_path(&req).unwrap_err().code(),
            "ValidationException"
        );
    }

    #[test]
    fn trailing_slash_routes_to_single_resource_op() {
        // `POST /v1/pipes/` (empty {Name}) must reach CreatePipe (which then
        // rejects the empty name) rather than collapsing to a 404 or ListPipes.
        assert_eq!(
            PipesService::resolve_action(&make_request(Method::POST, "/v1/pipes/")),
            Some("CreatePipe")
        );
        assert_eq!(
            PipesService::resolve_action(&make_request(Method::GET, "/v1/pipes/")),
            Some("DescribePipe")
        );
        assert_eq!(
            PipesService::resolve_action(&make_request(Method::DELETE, "/v1/pipes/")),
            Some("DeletePipe")
        );
        // No trailing slash: the collection GET is still ListPipes.
        assert_eq!(
            PipesService::resolve_action(&make_request(Method::GET, "/v1/pipes")),
            Some("ListPipes")
        );
    }
}
