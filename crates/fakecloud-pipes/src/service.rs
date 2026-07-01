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
    if let Some(sp) = pipe.get_mut("SourceParameters").and_then(Value::as_object_mut) {
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
        match (segs.len(), &req.method) {
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
        let accounts = self.state.read();
        let pipe = accounts
            .get(&req.account_id)
            .and_then(|st| st.pipes.get(&name))
            .ok_or_else(|| not_found(&name))?;
        Ok(AwsResponse::ok_json(pipe.clone()))
    }

    fn list_pipes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let q = &req.query_params;
        let name_prefix = q.get("NamePrefix").map(String::as_str);
        let desired = q.get("DesiredState").map(String::as_str);
        let current = q.get("CurrentState").map(String::as_str);
        let source_prefix = q.get("SourcePrefix").map(String::as_str);
        let target_prefix = q.get("TargetPrefix").map(String::as_str);
        let limit: usize = q
            .get("Limit")
            .and_then(|s| s.parse().ok())
            .filter(|n| (1..=100).contains(n))
            .unwrap_or(100);
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
    Ok(arn)
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
}
