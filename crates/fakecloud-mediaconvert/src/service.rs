//! AWS Elemental MediaConvert (`mediaconvert`) restJson1 dispatch + handlers.
//!
//! The full 34-operation MediaConvert control plane. Requests are routed to an
//! operation by HTTP method + `@http` URI path under the `/2017-08-29` prefix;
//! path labels are captured positionally (percent-decoded, so an ARN label whose
//! slashes/colons arrive percent-encoded survives intact) and query parameters
//! are read from the raw query string. State is account-partitioned and
//! persisted; each resource is stored as its already-output-valid wire object so
//! `Get*` echoes exactly what `Create*` / `Update*` persisted.

use std::sync::Arc;

use async_trait::async_trait;
use http::{Method, StatusCode};
use percent_encoding::percent_decode_str;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::shared;
use crate::state::{MediaConvertData, SharedMediaConvertState};

/// Every operation name in the AWS Elemental MediaConvert Smithy model (34).
pub const MEDIACONVERT_ACTIONS: &[&str] = &[
    "AssociateCertificate",
    "CancelJob",
    "CreateJob",
    "CreateJobTemplate",
    "CreatePreset",
    "CreateQueue",
    "CreateResourceShare",
    "DeleteJobTemplate",
    "DeletePolicy",
    "DeletePreset",
    "DeleteQueue",
    "DescribeEndpoints",
    "DisassociateCertificate",
    "GetJob",
    "GetJobTemplate",
    "GetJobsQueryResults",
    "GetPolicy",
    "GetPreset",
    "GetQueue",
    "ListJobTemplates",
    "ListJobs",
    "ListPresets",
    "ListQueues",
    "ListTagsForResource",
    "ListVersions",
    "Probe",
    "PutPolicy",
    "SearchJobs",
    "StartJobsQuery",
    "TagResource",
    "UntagResource",
    "UpdateJobTemplate",
    "UpdatePreset",
    "UpdateQueue",
];

/// Operations that mutate persisted state on success (so a snapshot is taken).
const MUTATING: &[&str] = &[
    "AssociateCertificate",
    "CancelJob",
    "CreateJob",
    "CreateJobTemplate",
    "CreatePreset",
    "CreateQueue",
    "DeleteJobTemplate",
    "DeletePolicy",
    "DeletePreset",
    "DeleteQueue",
    "DisassociateCertificate",
    "PutPolicy",
    "StartJobsQuery",
    "TagResource",
    "UntagResource",
    "UpdateJobTemplate",
    "UpdatePreset",
    "UpdateQueue",
];

pub struct MediaConvertService {
    state: SharedMediaConvertState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl MediaConvertService {
    pub fn new(state: SharedMediaConvertState) -> Self {
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
        save_snapshot(
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

    /// Settle any in-flight job lifecycle transition for the account. A job in
    /// `SUBMITTED` or `PROGRESSING` settles to `COMPLETE` on the next read.
    /// Returns `true` if a transition fired (so the caller persists the change).
    fn reconcile(&self, account: &str) -> bool {
        let mut guard = self.state.write();
        let data = guard.get_or_create(account);
        let mut changed = false;
        for job in data.jobs.values_mut() {
            if settle_job(job) {
                changed = true;
            }
        }
        changed
    }

    /// Route a request to an operation name + captured path labels by HTTP
    /// method + `@http` URI path. Returns `None` when no route matches.
    fn resolve_action(req: &AwsRequest) -> Option<(&'static str, Vec<String>)> {
        let raw = req.raw_path.split('?').next().unwrap_or(&req.raw_path);
        let trimmed = raw.strip_prefix('/').unwrap_or(raw);
        let segs: Vec<String> = if trimmed.is_empty() {
            Vec::new()
        } else {
            trimmed
                .split('/')
                .map(|s| percent_decode_str(s).decode_utf8_lossy().into_owned())
                .collect()
        };
        let s: Vec<&str> = segs.iter().map(String::as_str).collect();
        let m = &req.method;
        let get = m == Method::GET;
        let post = m == Method::POST;
        let put = m == Method::PUT;
        let del = m == Method::DELETE;
        let l1 = |a: &str| vec![a.to_string()];
        let (action, labels): (&'static str, Vec<String>) = match s.as_slice() {
            ["2017-08-29", "queues"] if post => ("CreateQueue", vec![]),
            ["2017-08-29", "queues"] if get => ("ListQueues", vec![]),
            ["2017-08-29", "queues", name] if get => ("GetQueue", l1(name)),
            ["2017-08-29", "queues", name] if put => ("UpdateQueue", l1(name)),
            ["2017-08-29", "queues", name] if del => ("DeleteQueue", l1(name)),
            ["2017-08-29", "presets"] if post => ("CreatePreset", vec![]),
            ["2017-08-29", "presets"] if get => ("ListPresets", vec![]),
            ["2017-08-29", "presets", name] if get => ("GetPreset", l1(name)),
            ["2017-08-29", "presets", name] if put => ("UpdatePreset", l1(name)),
            ["2017-08-29", "presets", name] if del => ("DeletePreset", l1(name)),
            ["2017-08-29", "jobTemplates"] if post => ("CreateJobTemplate", vec![]),
            ["2017-08-29", "jobTemplates"] if get => ("ListJobTemplates", vec![]),
            ["2017-08-29", "jobTemplates", name] if get => ("GetJobTemplate", l1(name)),
            ["2017-08-29", "jobTemplates", name] if put => ("UpdateJobTemplate", l1(name)),
            ["2017-08-29", "jobTemplates", name] if del => ("DeleteJobTemplate", l1(name)),
            ["2017-08-29", "jobs"] if post => ("CreateJob", vec![]),
            ["2017-08-29", "jobs"] if get => ("ListJobs", vec![]),
            ["2017-08-29", "jobs", id] if get => ("GetJob", l1(id)),
            ["2017-08-29", "jobs", id] if del => ("CancelJob", l1(id)),
            ["2017-08-29", "endpoints"] if post => ("DescribeEndpoints", vec![]),
            ["2017-08-29", "policy"] if put => ("PutPolicy", vec![]),
            ["2017-08-29", "policy"] if get => ("GetPolicy", vec![]),
            ["2017-08-29", "policy"] if del => ("DeletePolicy", vec![]),
            ["2017-08-29", "tags"] if post => ("TagResource", vec![]),
            ["2017-08-29", "tags", arn] if get => ("ListTagsForResource", l1(arn)),
            ["2017-08-29", "tags", arn] if put => ("UntagResource", l1(arn)),
            ["2017-08-29", "certificates"] if post => ("AssociateCertificate", vec![]),
            ["2017-08-29", "certificates", arn] if del => ("DisassociateCertificate", l1(arn)),
            ["2017-08-29", "resourceShares"] if post => ("CreateResourceShare", vec![]),
            ["2017-08-29", "jobsQueries"] if post => ("StartJobsQuery", vec![]),
            ["2017-08-29", "jobsQueries", id] if get => ("GetJobsQueryResults", l1(id)),
            ["2017-08-29", "versions"] if get => ("ListVersions", vec![]),
            ["2017-08-29", "search"] if get => ("SearchJobs", vec![]),
            ["2017-08-29", "probe"] if post => ("Probe", vec![]),
            _ => return None,
        };
        Some((action, labels))
    }
}

#[async_trait]
impl AwsService for MediaConvertService {
    fn service_name(&self) -> &str {
        "mediaconvert"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let Some((action, labels)) = Self::resolve_action(&req) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Unknown operation: {} {}", req.method, req.raw_path),
            ));
        };
        let (result, settled) = self.dispatch(action, &labels, &req);
        let success = matches!(result.as_ref(), Ok(resp) if resp.status.is_success());
        if settled || (MUTATING.contains(&action) && success) {
            self.save().await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        MEDIACONVERT_ACTIONS
    }
}

/// Per-request account + region context.
struct Ctx {
    account: String,
    region: String,
}

impl MediaConvertService {
    fn dispatch(
        &self,
        action: &str,
        labels: &[String],
        req: &AwsRequest,
    ) -> (Result<AwsResponse, AwsServiceError>, bool) {
        let body = match parse_body(req) {
            Ok(b) => b,
            Err(e) => return (Err(e), false),
        };
        if let Err(e) = crate::validate::validate_input(action, &body) {
            return (Err(e), false);
        }
        let ctx = Ctx {
            account: req.account_id.clone(),
            region: req.region.clone(),
        };
        let q = parse_query(&req.raw_query);
        // Settle any in-flight job transition before the handler reads state,
        // remembering whether it fired so the change is persisted even for a
        // pure read.
        let settled = self.reconcile(&ctx.account);
        let a = |i: usize| labels.get(i).map(String::as_str).unwrap_or_default();
        let result = match action {
            "CreateQueue" => self.create_queue(&ctx, &body),
            "GetQueue" => self.get_queue(&ctx, a(0)),
            "UpdateQueue" => self.update_queue(&ctx, a(0), &body),
            "DeleteQueue" => self.delete_queue(&ctx, a(0)),
            "ListQueues" => self.list_queues(&ctx, &q),
            "CreatePreset" => self.create_preset(&ctx, &body),
            "GetPreset" => self.get_preset(&ctx, a(0)),
            "UpdatePreset" => self.update_preset(&ctx, a(0), &body),
            "DeletePreset" => self.delete_preset(&ctx, a(0)),
            "ListPresets" => self.list_presets(&ctx, &q),
            "CreateJobTemplate" => self.create_job_template(&ctx, &body),
            "GetJobTemplate" => self.get_job_template(&ctx, a(0)),
            "UpdateJobTemplate" => self.update_job_template(&ctx, a(0), &body),
            "DeleteJobTemplate" => self.delete_job_template(&ctx, a(0)),
            "ListJobTemplates" => self.list_job_templates(&ctx, &q),
            "CreateJob" => self.create_job(&ctx, &body),
            "GetJob" => self.get_job(&ctx, a(0)),
            "CancelJob" => self.cancel_job(&ctx, a(0)),
            "ListJobs" => self.list_jobs(&ctx, &q),
            "SearchJobs" => self.search_jobs(&ctx, &q),
            "PutPolicy" => self.put_policy(&ctx, &body),
            "GetPolicy" => self.get_policy(&ctx),
            "DeletePolicy" => self.delete_policy(&ctx),
            "DescribeEndpoints" => self.describe_endpoints(&body, &q),
            "TagResource" => self.tag_resource(&ctx, &body),
            "UntagResource" => self.untag_resource(&ctx, a(0), &body),
            "ListTagsForResource" => self.list_tags(&ctx, a(0)),
            "AssociateCertificate" => self.associate_certificate(&ctx, &body),
            "DisassociateCertificate" => self.disassociate_certificate(&ctx, a(0)),
            "CreateResourceShare" => self.create_resource_share(&ctx, &body),
            "StartJobsQuery" => self.start_jobs_query(&ctx, &body, &q),
            "GetJobsQueryResults" => self.get_jobs_query_results(&ctx, a(0)),
            "ListVersions" => self.list_versions(&q),
            "Probe" => self.probe(&body),
            _ => Err(AwsServiceError::action_not_implemented(
                "mediaconvert",
                action,
            )),
        };
        (result, settled)
    }
}

// ===================== helpers =====================

fn ok(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::OK, v))
}

fn created(v: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json_value(StatusCode::CREATED, v))
}

fn accepted_empty() -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json(StatusCode::ACCEPTED, "{}"))
}

fn ok_empty() -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::json(StatusCode::OK, "{}"))
}

fn parse_body(req: &AwsRequest) -> Result<Value, AwsServiceError> {
    if req.body.is_empty() {
        return Ok(json!({}));
    }
    serde_json::from_slice(&req.body)
        .map_err(|e| bad_request(&format!("The request body is malformed: {e}")))
}

fn bad_request(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg)
}

fn not_found(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "NotFoundException", msg)
}

fn conflict(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::CONFLICT, "ConflictException", msg)
}

fn parse_query(raw: &str) -> Vec<(String, String)> {
    raw.split('&')
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (
                percent_decode_str(k).decode_utf8_lossy().into_owned(),
                percent_decode_str(v).decode_utf8_lossy().into_owned(),
            )
        })
        .collect()
}

fn query_one<'a>(q: &'a [(String, String)], key: &str) -> Option<&'a str> {
    q.iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
        .filter(|v| !v.is_empty())
}

/// Reject a query param whose value is outside its declared enum with
/// `BadRequestException`, matching AWS's rejection of malformed `@httpQuery`
/// enum members.
fn check_query_enum(
    q: &[(String, String)],
    key: &str,
    allowed: &[&str],
) -> Result<(), AwsServiceError> {
    if let Some(v) = query_one(q, key) {
        if !allowed.contains(&v) {
            return Err(bad_request(&format!(
                "'{key}' must be one of [{}], got '{v}'.",
                allowed.join(", ")
            )));
        }
    }
    Ok(())
}

/// Look up a body member tolerating the restJson1 `@jsonName` (camelCase) vs the
/// raw Smithy member name (PascalCase) first-letter case difference, so a client
/// that sends either spelling is validated the same way.
fn body_field<'a>(v: &'a Value, camel: &str) -> Option<&'a Value> {
    if let Some(x) = v.get(camel) {
        return Some(x);
    }
    let mut chars = camel.chars();
    let first = chars.next()?;
    let pascal: String = first.to_ascii_uppercase().to_string() + chars.as_str();
    v.get(&pascal)
}

const ORDER: &[&str] = &["ASCENDING", "DESCENDING"];
const JOB_STATUS: &[&str] = &["SUBMITTED", "PROGRESSING", "COMPLETE", "CANCELED", "ERROR"];
const QUEUE_LIST_BY: &[&str] = &["NAME", "CREATION_DATE"];
const PRESET_LIST_BY: &[&str] = &["NAME", "CREATION_DATE", "SYSTEM"];
const JOB_TEMPLATE_LIST_BY: &[&str] = &["NAME", "CREATION_DATE", "SYSTEM"];

/// Reject a path-label ARN that is absent or not an ARN (a missing required
/// label surfaces to the router as the literal `{Arn}` placeholder or an empty
/// segment) with `BadRequestException`.
fn check_arn_label(arn: &str) -> Result<(), AwsServiceError> {
    if arn.is_empty() || !arn.starts_with("arn:") {
        return Err(bad_request(
            "The request failed because it is missing a valid resource ARN.",
        ));
    }
    Ok(())
}

fn str_or(body: &Value, key: &str, default: &str) -> String {
    body.get(key)
        .and_then(Value::as_str)
        .unwrap_or(default)
        .to_string()
}

/// Copy each named optional member from `body` into `out` verbatim when present
/// and non-null.
fn echo(out: &mut Map<String, Value>, body: &Value, keys: &[&str]) {
    for key in keys {
        if let Some(v) = body.get(*key) {
            if !v.is_null() {
                out.insert((*key).to_string(), v.clone());
            }
        }
    }
}

/// Paginate wire objects using the request's `maxResults` / `nextToken` query
/// params. MediaConvert list ops cap `maxResults` at `[1, 20]`.
fn paginate(
    items: Vec<Value>,
    q: &[(String, String)],
) -> Result<(Vec<Value>, Option<String>), AwsServiceError> {
    let max = match query_one(q, "maxResults") {
        Some(v) => {
            let n: i64 = v
                .parse()
                .map_err(|_| bad_request("maxResults must be an integer."))?;
            if !(1..=20).contains(&n) {
                return Err(bad_request("maxResults must be between 1 and 20."));
            }
            n as usize
        }
        None => 20,
    };
    let start = query_one(q, "nextToken")
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(0);
    let end = (start + max).min(items.len());
    let page: Vec<Value> = items.get(start..end).unwrap_or(&[]).to_vec();
    let next = if end < items.len() {
        Some(end.to_string())
    } else {
        None
    };
    Ok((page, next))
}

/// Store the create-time `tags` map (if any) under a resource ARN.
fn store_tags(data: &mut MediaConvertData, arn: &str, body: &Value) {
    if let Some(tags) = body.get("tags").and_then(Value::as_object) {
        if tags.is_empty() {
            return;
        }
        let entry = data.tags.entry(arn.to_string()).or_default();
        for (k, v) in tags {
            if let Some(s) = v.as_str() {
                entry.insert(k.clone(), s.to_string());
            }
        }
    }
}

/// Advance a job from `SUBMITTED`/`PROGRESSING` to `COMPLETE`, attaching
/// well-formed (but media-free) completion metadata. Returns `true` on change.
///
/// fakecloud does not run a real transcoder: no media is read or written. The
/// job settles to `COMPLETE` with correctly-shaped `outputGroupDetails` /
/// `timing` so clients that poll `GetJob` for terminal status behave correctly,
/// but the `outputDetails` are empty because nothing was actually produced.
fn settle_job(job: &mut Value) -> bool {
    let Some(obj) = job.as_object_mut() else {
        return false;
    };
    let status = obj.get("status").and_then(Value::as_str).unwrap_or("");
    if status != "SUBMITTED" && status != "PROGRESSING" {
        return false;
    }
    let now = shared::now_epoch();
    let submit = obj.get("createdAt").and_then(Value::as_f64).unwrap_or(now);
    // Derive one OutputGroupDetail per configured output group; no media is
    // produced so the per-output detail list is empty.
    let group_details: Vec<Value> = obj
        .get("settings")
        .and_then(|s| s.get("outputGroups"))
        .and_then(Value::as_array)
        .map(|groups| {
            groups
                .iter()
                .map(|_| json!({ "outputDetails": [] }))
                .collect()
        })
        .unwrap_or_default();
    obj.insert("status".into(), json!("COMPLETE"));
    obj.insert("currentPhase".into(), json!("UPLOADING"));
    obj.insert("jobPercentComplete".into(), json!(100));
    obj.insert("retryCount".into(), json!(0));
    obj.insert("outputGroupDetails".into(), json!(group_details));
    obj.insert(
        "timing".into(),
        json!({ "submitTime": submit, "startTime": submit, "finishTime": now }),
    );
    true
}

impl MediaConvertService {
    // ------------------------------ Queues ------------------------------

    fn build_queue(&self, ctx: &Ctx, name: &str, body: &Value) -> Value {
        let now = shared::now_epoch();
        let pricing = str_or(body, "pricingPlan", "ON_DEMAND");
        let mut q = Map::new();
        q.insert(
            "arn".into(),
            json!(shared::queue_arn(&ctx.region, &ctx.account, name)),
        );
        q.insert("name".into(), json!(name));
        q.insert("type".into(), json!("CUSTOM"));
        q.insert("status".into(), json!(str_or(body, "status", "ACTIVE")));
        q.insert("pricingPlan".into(), json!(pricing));
        q.insert("createdAt".into(), json!(now));
        q.insert("lastUpdated".into(), json!(now));
        q.insert("progressingJobsCount".into(), json!(0));
        q.insert("submittedJobsCount".into(), json!(0));
        echo(
            &mut q,
            body,
            &["description", "concurrentJobs", "maximumConcurrentFeeds"],
        );
        // A RESERVED queue materialises a reservation plan from its settings.
        if pricing == "RESERVED" {
            if let Some(rp) = body
                .get("reservationPlanSettings")
                .and_then(Value::as_object)
            {
                let year = 365.0 * 24.0 * 3600.0;
                q.insert(
                    "reservationPlan".into(),
                    json!({
                        "commitment": rp.get("commitment").cloned().unwrap_or_else(|| json!("ONE_YEAR")),
                        "renewalType": rp.get("renewalType").cloned().unwrap_or_else(|| json!("EXPIRE")),
                        "reservedSlots": rp.get("reservedSlots").cloned().unwrap_or_else(|| json!(0)),
                        "status": "ACTIVE",
                        "purchasedAt": now,
                        "expiresAt": now + year,
                    }),
                );
            }
        }
        Value::Object(q)
    }

    fn create_queue(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = str_or(body, "name", "");
        let queue = self.build_queue(ctx, &name, body);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.queues.contains_key(&name) {
            return Err(conflict(&format!(
                "The queue you are trying to create, '{name}', already exists."
            )));
        }
        let arn = shared::queue_arn(&ctx.region, &ctx.account, &name);
        store_tags(data, &arn, body);
        data.queues.insert(name, queue.clone());
        created(json!({ "queue": queue }))
    }

    fn get_queue(&self, ctx: &Ctx, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        match guard.get(&ctx.account).and_then(|d| d.queues.get(name)) {
            Some(q) => ok(json!({ "queue": q })),
            None => Err(not_found(&format!("The queue '{name}' was not found."))),
        }
    }

    fn update_queue(
        &self,
        ctx: &Ctx,
        name: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(q) = data.queues.get_mut(name).and_then(Value::as_object_mut) else {
            return Err(not_found(&format!("The queue '{name}' was not found.")));
        };
        for key in [
            "description",
            "status",
            "concurrentJobs",
            "maximumConcurrentFeeds",
        ] {
            if let Some(v) = body.get(key) {
                q.insert(key.to_string(), v.clone());
            }
        }
        if let Some(rp) = body
            .get("reservationPlanSettings")
            .and_then(Value::as_object)
        {
            if let Some(plan) = q.get_mut("reservationPlan").and_then(Value::as_object_mut) {
                for k in ["commitment", "renewalType", "reservedSlots"] {
                    if let Some(v) = rp.get(k) {
                        plan.insert(k.to_string(), v.clone());
                    }
                }
            }
        }
        q.insert("lastUpdated".into(), json!(shared::now_epoch()));
        ok(json!({ "queue": Value::Object(q.clone()) }))
    }

    fn delete_queue(&self, ctx: &Ctx, name: &str) -> Result<AwsResponse, AwsServiceError> {
        if name == "Default" {
            return Err(conflict(
                "You cannot delete the 'Default' queue that MediaConvert provides.",
            ));
        }
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.queues.remove(name).is_none() {
            return Err(not_found(&format!("The queue '{name}' was not found.")));
        }
        data.tags
            .remove(&shared::queue_arn(&ctx.region, &ctx.account, name));
        accepted_empty()
    }

    fn list_queues(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        check_query_enum(q, "order", ORDER)?;
        check_query_enum(q, "listBy", QUEUE_LIST_BY)?;
        let guard = self.state.read();
        let mut queues: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| d.queues.values().cloned().collect())
            .unwrap_or_default();
        sort_by_list_order(&mut queues, q, "name");
        let (page, next) = paginate(queues, q)?;
        let mut out =
            json!({ "queues": page, "totalConcurrentJobs": 0, "unallocatedConcurrentJobs": 0 });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    // ------------------------------ Presets -----------------------------

    fn build_preset(&self, ctx: &Ctx, name: &str, body: &Value) -> Value {
        let now = shared::now_epoch();
        let mut p = Map::new();
        p.insert(
            "arn".into(),
            json!(shared::preset_arn(&ctx.region, &ctx.account, name)),
        );
        p.insert("name".into(), json!(name));
        p.insert("type".into(), json!("CUSTOM"));
        p.insert("createdAt".into(), json!(now));
        p.insert("lastUpdated".into(), json!(now));
        p.insert(
            "settings".into(),
            body.get("settings").cloned().unwrap_or_else(|| json!({})),
        );
        echo(&mut p, body, &["description", "category"]);
        Value::Object(p)
    }

    fn create_preset(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = str_or(body, "name", "");
        let preset = self.build_preset(ctx, &name, body);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.presets.contains_key(&name) {
            return Err(conflict(&format!(
                "The preset you are trying to create, '{name}', already exists."
            )));
        }
        let arn = shared::preset_arn(&ctx.region, &ctx.account, &name);
        store_tags(data, &arn, body);
        data.presets.insert(name, preset.clone());
        created(json!({ "preset": preset }))
    }

    fn get_preset(&self, ctx: &Ctx, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        match guard.get(&ctx.account).and_then(|d| d.presets.get(name)) {
            Some(p) => ok(json!({ "preset": p })),
            None => Err(not_found(&format!("The preset '{name}' was not found."))),
        }
    }

    fn update_preset(
        &self,
        ctx: &Ctx,
        name: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(p) = data.presets.get_mut(name).and_then(Value::as_object_mut) else {
            return Err(not_found(&format!("The preset '{name}' was not found.")));
        };
        for key in ["description", "category", "settings"] {
            if let Some(v) = body.get(key) {
                p.insert(key.to_string(), v.clone());
            }
        }
        p.insert("lastUpdated".into(), json!(shared::now_epoch()));
        ok(json!({ "preset": Value::Object(p.clone()) }))
    }

    fn delete_preset(&self, ctx: &Ctx, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.presets.remove(name).is_none() {
            return Err(not_found(&format!("The preset '{name}' was not found.")));
        }
        data.tags
            .remove(&shared::preset_arn(&ctx.region, &ctx.account, name));
        accepted_empty()
    }

    fn list_presets(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        check_query_enum(q, "order", ORDER)?;
        check_query_enum(q, "listBy", PRESET_LIST_BY)?;
        let category = query_one(q, "category");
        let guard = self.state.read();
        let mut presets: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.presets
                    .values()
                    .filter(|p| {
                        category.is_none() || p.get("category").and_then(Value::as_str) == category
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        sort_by_list_order(&mut presets, q, "name");
        let (page, next) = paginate(presets, q)?;
        let mut out = json!({ "presets": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    // --------------------------- Job templates --------------------------

    fn build_job_template(&self, ctx: &Ctx, name: &str, body: &Value) -> Value {
        let now = shared::now_epoch();
        let mut t = Map::new();
        t.insert(
            "arn".into(),
            json!(shared::job_template_arn(&ctx.region, &ctx.account, name)),
        );
        t.insert("name".into(), json!(name));
        t.insert("type".into(), json!("CUSTOM"));
        t.insert("createdAt".into(), json!(now));
        t.insert("lastUpdated".into(), json!(now));
        t.insert(
            "settings".into(),
            body.get("settings").cloned().unwrap_or_else(|| json!({})),
        );
        echo(
            &mut t,
            body,
            &[
                "description",
                "category",
                "queue",
                "priority",
                "accelerationSettings",
                "hopDestinations",
                "statusUpdateInterval",
            ],
        );
        Value::Object(t)
    }

    fn create_job_template(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let name = str_or(body, "name", "");
        let tpl = self.build_job_template(ctx, &name, body);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.job_templates.contains_key(&name) {
            return Err(conflict(&format!(
                "The job template you are trying to create, '{name}', already exists."
            )));
        }
        let arn = shared::job_template_arn(&ctx.region, &ctx.account, &name);
        store_tags(data, &arn, body);
        data.job_templates.insert(name, tpl.clone());
        created(json!({ "jobTemplate": tpl }))
    }

    fn get_job_template(&self, ctx: &Ctx, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        match guard
            .get(&ctx.account)
            .and_then(|d| d.job_templates.get(name))
        {
            Some(t) => ok(json!({ "jobTemplate": t })),
            None => Err(not_found(&format!(
                "The job template '{name}' was not found."
            ))),
        }
    }

    fn update_job_template(
        &self,
        ctx: &Ctx,
        name: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(t) = data
            .job_templates
            .get_mut(name)
            .and_then(Value::as_object_mut)
        else {
            return Err(not_found(&format!(
                "The job template '{name}' was not found."
            )));
        };
        for key in [
            "description",
            "category",
            "queue",
            "priority",
            "settings",
            "accelerationSettings",
            "hopDestinations",
            "statusUpdateInterval",
        ] {
            if let Some(v) = body.get(key) {
                t.insert(key.to_string(), v.clone());
            }
        }
        t.insert("lastUpdated".into(), json!(shared::now_epoch()));
        ok(json!({ "jobTemplate": Value::Object(t.clone()) }))
    }

    fn delete_job_template(&self, ctx: &Ctx, name: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if data.job_templates.remove(name).is_none() {
            return Err(not_found(&format!(
                "The job template '{name}' was not found."
            )));
        }
        data.tags
            .remove(&shared::job_template_arn(&ctx.region, &ctx.account, name));
        accepted_empty()
    }

    fn list_job_templates(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        check_query_enum(q, "order", ORDER)?;
        check_query_enum(q, "listBy", JOB_TEMPLATE_LIST_BY)?;
        let category = query_one(q, "category");
        let guard = self.state.read();
        let mut tpls: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.job_templates
                    .values()
                    .filter(|t| {
                        category.is_none() || t.get("category").and_then(Value::as_str) == category
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        sort_by_list_order(&mut tpls, q, "name");
        let (page, next) = paginate(tpls, q)?;
        let mut out = json!({ "jobTemplates": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    // ------------------------------- Jobs -------------------------------

    fn build_job(&self, ctx: &Ctx, id: &str, body: &Value) -> Value {
        let now = shared::now_epoch();
        let queue = body
            .get("queue")
            .and_then(Value::as_str)
            .map(|s| s.to_string())
            .unwrap_or_else(|| shared::queue_arn(&ctx.region, &ctx.account, "Default"));
        let mut j = Map::new();
        j.insert("id".into(), json!(id));
        j.insert(
            "arn".into(),
            json!(shared::job_arn(&ctx.region, &ctx.account, id)),
        );
        j.insert("role".into(), json!(str_or(body, "role", "")));
        j.insert(
            "settings".into(),
            body.get("settings").cloned().unwrap_or_else(|| json!({})),
        );
        j.insert("status".into(), json!("SUBMITTED"));
        j.insert("createdAt".into(), json!(now));
        j.insert("queue".into(), json!(queue));
        j.insert(
            "priority".into(),
            json!(body.get("priority").and_then(Value::as_i64).unwrap_or(0)),
        );
        j.insert("jobPercentComplete".into(), json!(0));
        j.insert("currentPhase".into(), json!("PROBING"));
        j.insert("retryCount".into(), json!(0));
        j.insert(
            "accelerationStatus".into(),
            json!(match body
                .get("accelerationSettings")
                .and_then(|a| a.get("mode"))
                .and_then(Value::as_str)
            {
                Some("ENABLED") | Some("PREFERRED") => "IN_PROGRESS",
                _ => "NOT_APPLICABLE",
            }),
        );
        j.insert("timing".into(), json!({ "submitTime": now }));
        j.insert(
            "userMetadata".into(),
            body.get("userMetadata")
                .cloned()
                .unwrap_or_else(|| json!({})),
        );
        echo(
            &mut j,
            body,
            &[
                "accelerationSettings",
                "billingTagsSource",
                "clientRequestToken",
                "hopDestinations",
                "jobTemplate",
                "simulateReservedQueue",
                "statusUpdateInterval",
            ],
        );
        Value::Object(j)
    }

    fn create_job(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let id = shared::new_job_id();
        let job = self.build_job(ctx, &id, body);
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let arn = shared::job_arn(&ctx.region, &ctx.account, &id);
        store_tags(data, &arn, body);
        data.jobs.insert(id, job.clone());
        created(json!({ "job": job }))
    }

    fn get_job(&self, ctx: &Ctx, id: &str) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        match guard.get(&ctx.account).and_then(|d| d.jobs.get(id)) {
            Some(j) => ok(json!({ "job": j })),
            None => Err(not_found(&format!("The job '{id}' was not found."))),
        }
    }

    fn cancel_job(&self, ctx: &Ctx, id: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let Some(j) = data.jobs.get_mut(id).and_then(Value::as_object_mut) else {
            return Err(not_found(&format!("The job '{id}' was not found.")));
        };
        let status = j.get("status").and_then(Value::as_str).unwrap_or("");
        if status == "COMPLETE" || status == "ERROR" || status == "CANCELED" {
            return Err(conflict(&format!(
                "The job '{id}' is in a terminal state and cannot be canceled."
            )));
        }
        j.insert("status".into(), json!("CANCELED"));
        accepted_empty()
    }

    fn list_jobs(&self, ctx: &Ctx, q: &[(String, String)]) -> Result<AwsResponse, AwsServiceError> {
        check_query_enum(q, "order", ORDER)?;
        check_query_enum(q, "status", JOB_STATUS)?;
        let queue = query_one(q, "queue");
        let status = query_one(q, "status");
        let guard = self.state.read();
        let mut jobs: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.jobs
                    .values()
                    .filter(|j| queue_matches(j, queue) && status_matches(j, status))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        order_by_created(&mut jobs, q);
        let (page, next) = paginate(jobs, q)?;
        let mut out = json!({ "jobs": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    fn search_jobs(
        &self,
        ctx: &Ctx,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        // SearchJobs filters the account's jobs by queue/status/input-file. We
        // hold no per-input index, so `inputFile` narrows to jobs whose settings
        // reference that file URL.
        check_query_enum(q, "order", ORDER)?;
        check_query_enum(q, "status", JOB_STATUS)?;
        let queue = query_one(q, "queue");
        let status = query_one(q, "status");
        let input_file = query_one(q, "inputFile");
        let guard = self.state.read();
        let mut jobs: Vec<Value> = guard
            .get(&ctx.account)
            .map(|d| {
                d.jobs
                    .values()
                    .filter(|j| {
                        queue_matches(j, queue)
                            && status_matches(j, status)
                            && input_file_matches(j, input_file)
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        order_by_created(&mut jobs, q);
        let (page, next) = paginate(jobs, q)?;
        let mut out = json!({ "jobs": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    // ------------------------------ Policy ------------------------------

    fn put_policy(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let policy = normalize_policy(body.get("policy"));
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.policy = Some(policy.clone());
        ok(json!({ "policy": policy }))
    }

    fn get_policy(&self, ctx: &Ctx) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let policy = guard
            .get(&ctx.account)
            .and_then(|d| d.policy.clone())
            .unwrap_or_else(|| normalize_policy(None));
        ok(json!({ "policy": policy }))
    }

    fn delete_policy(&self, ctx: &Ctx) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        guard.get_or_create(&ctx.account).policy = None;
        ok_empty()
    }

    // ---------------------------- Endpoints -----------------------------

    fn describe_endpoints(
        &self,
        body: &Value,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        const MODE: &[&str] = &["DEFAULT", "GET_ONLY"];
        check_query_enum(q, "mode", MODE)?;
        if let Some(v) = body_field(body, "mode").and_then(Value::as_str) {
            if !MODE.contains(&v) {
                return Err(bad_request(&format!(
                    "'mode' must be one of [{}], got '{v}'.",
                    MODE.join(", ")
                )));
            }
        }
        let guard = self.state.read();
        let url = shared::endpoint_url(guard.endpoint(), guard.region());
        ok(json!({ "endpoints": [ { "url": url } ] }))
    }

    // ------------------------------ Tags --------------------------------

    fn tag_resource(&self, ctx: &Ctx, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_or(body, "arn", "");
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let entry = data.tags.entry(arn).or_default();
        if let Some(tags) = body.get("tags").and_then(Value::as_object) {
            for (k, v) in tags {
                if let Some(s) = v.as_str() {
                    entry.insert(k.clone(), s.to_string());
                }
            }
        }
        ok_empty()
    }

    fn untag_resource(
        &self,
        ctx: &Ctx,
        arn: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_arn_label(arn)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if let Some(entry) = data.tags.get_mut(arn) {
            if let Some(keys) = body.get("tagKeys").and_then(Value::as_array) {
                for k in keys {
                    if let Some(k) = k.as_str() {
                        entry.remove(k);
                    }
                }
            }
            if entry.is_empty() {
                data.tags.remove(arn);
            }
        }
        ok_empty()
    }

    fn list_tags(&self, ctx: &Ctx, arn: &str) -> Result<AwsResponse, AwsServiceError> {
        check_arn_label(arn)?;
        let guard = self.state.read();
        let tags = guard
            .get(&ctx.account)
            .and_then(|d| d.tags.get(arn))
            .cloned()
            .unwrap_or_default();
        ok(json!({ "resourceTags": { "arn": arn, "tags": tags } }))
    }

    // -------------------------- Certificates ----------------------------

    fn associate_certificate(
        &self,
        ctx: &Ctx,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_or(body, "arn", "");
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if !data.certificates.contains(&arn) {
            data.certificates.push(arn);
        }
        Ok(AwsResponse::json(StatusCode::CREATED, "{}"))
    }

    fn disassociate_certificate(
        &self,
        ctx: &Ctx,
        arn: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        check_arn_label(arn)?;
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        data.certificates.retain(|c| c != arn);
        accepted_empty()
    }

    // -------------------------- Resource share --------------------------

    fn create_resource_share(
        &self,
        ctx: &Ctx,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let job_id = str_or(body, "jobId", "");
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        if let Some(j) = data.jobs.get_mut(&job_id).and_then(Value::as_object_mut) {
            j.insert("shareStatus".into(), json!("SHARED"));
            j.insert(
                "lastShareDetails".into(),
                json!(str_or(body, "supportCaseId", "")),
            );
        } else {
            return Err(not_found(&format!("The job '{job_id}' was not found.")));
        }
        accepted_empty()
    }

    // ---------------------------- Jobs query ----------------------------

    fn start_jobs_query(
        &self,
        ctx: &Ctx,
        body: &Value,
        q: &[(String, String)],
    ) -> Result<AwsResponse, AwsServiceError> {
        // The SDK carries `maxResults` / `order` in the JSON body, but a client
        // may also pass them as query params; validate both against the model.
        check_query_enum(q, "order", ORDER)?;
        if let Some(v) = query_one(q, "maxResults") {
            let n: i64 = v
                .parse()
                .map_err(|_| bad_request("maxResults must be an integer."))?;
            if !(1..=20).contains(&n) {
                return Err(bad_request("maxResults must be between 1 and 20."));
            }
        }
        // `maxResults` / `order` also arrive in the JSON body; validate both the
        // camelCase and PascalCase spellings against the model.
        if let Some(v) = body_field(body, "order").and_then(Value::as_str) {
            if !ORDER.contains(&v) {
                return Err(bad_request(&format!(
                    "'order' must be one of [{}], got '{v}'.",
                    ORDER.join(", ")
                )));
            }
        }
        if let Some(v) = body_field(body, "maxResults") {
            if !v.is_null() {
                let n = v
                    .as_i64()
                    .ok_or_else(|| bad_request("maxResults must be an integer."))?;
                if !(1..=20).contains(&n) {
                    return Err(bad_request("maxResults must be between 1 and 20."));
                }
            }
        }
        let id = shared::new_query_id();
        // Snapshot the account's jobs at query time so GetJobsQueryResults is
        // deterministic and echoes a stable result set.
        let mut guard = self.state.write();
        let data = guard.get_or_create(&ctx.account);
        let jobs: Vec<Value> = data.jobs.values().cloned().collect();
        let record = json!({
            "status": "COMPLETE",
            "jobs": jobs,
            "filterList": body.get("filterList").cloned().unwrap_or_else(|| json!([])),
        });
        data.jobs_queries.insert(id.clone(), record);
        created(json!({ "id": id }))
    }

    fn get_jobs_query_results(&self, ctx: &Ctx, id: &str) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        match guard.get(&ctx.account).and_then(|d| d.jobs_queries.get(id)) {
            Some(rec) => {
                let jobs = rec.get("jobs").cloned().unwrap_or_else(|| json!([]));
                let status = rec
                    .get("status")
                    .cloned()
                    .unwrap_or_else(|| json!("COMPLETE"));
                ok(json!({ "jobs": jobs, "status": status }))
            }
            None => Err(not_found(&format!("The jobs query '{id}' was not found."))),
        }
    }

    // ----------------------------- Versions -----------------------------

    fn list_versions(&self, q: &[(String, String)]) -> Result<AwsResponse, AwsServiceError> {
        // The engine versions MediaConvert advertises. fakecloud runs a single
        // synthetic engine version; its expiration is a year out.
        let year = 365.0 * 24.0 * 3600.0;
        let versions = vec![json!({
            "version": "2017-08-29",
            "expirationDate": shared::now_epoch() + year,
        })];
        let (page, next) = paginate(versions, q)?;
        let mut out = json!({ "versions": page });
        if let Some(n) = next {
            out["nextToken"] = json!(n);
        }
        ok(out)
    }

    // ------------------------------- Probe ------------------------------

    fn probe(&self, _body: &Value) -> Result<AwsResponse, AwsServiceError> {
        // Probe inspects the media container/track layout of input files.
        // fakecloud does not read media, so it returns an empty (well-formed)
        // probeResults list rather than fabricating container metadata.
        ok(json!({ "probeResults": [] }))
    }
}

// -------------------------- filter/order helpers -------------------------

fn queue_matches(job: &Value, queue: Option<&str>) -> bool {
    match queue {
        None => true,
        Some(qname) => {
            let jq = job.get("queue").and_then(Value::as_str).unwrap_or("");
            // Match on either the full ARN or the trailing queue name.
            jq == qname || jq.rsplit('/').next() == Some(qname)
        }
    }
}

fn status_matches(job: &Value, status: Option<&str>) -> bool {
    match status {
        None => true,
        Some(st) => job.get("status").and_then(Value::as_str) == Some(st),
    }
}

fn input_file_matches(job: &Value, input_file: Option<&str>) -> bool {
    let Some(target) = input_file else {
        return true;
    };
    job.get("settings")
        .and_then(|s| s.get("inputs"))
        .and_then(Value::as_array)
        .map(|inputs| {
            inputs
                .iter()
                .any(|i| i.get("fileInput").and_then(Value::as_str) == Some(target))
        })
        .unwrap_or(false)
}

/// Order a job list by `createdAt` honoring the `order` query param
/// (`ASCENDING` default, `DESCENDING` newest-first).
fn order_by_created(jobs: &mut [Value], q: &[(String, String)]) {
    jobs.sort_by(|a, b| {
        let ta = a.get("createdAt").and_then(Value::as_f64).unwrap_or(0.0);
        let tb = b.get("createdAt").and_then(Value::as_f64).unwrap_or(0.0);
        ta.partial_cmp(&tb).unwrap_or(std::cmp::Ordering::Equal)
    });
    if query_one(q, "order") == Some("DESCENDING") {
        jobs.reverse();
    }
}

/// Order a name-keyed resource list honoring `order` (default ASCENDING).
fn sort_by_list_order(items: &mut [Value], q: &[(String, String)], key: &str) {
    items.sort_by(|a, b| {
        let ka = a.get(key).and_then(Value::as_str).unwrap_or("");
        let kb = b.get(key).and_then(Value::as_str).unwrap_or("");
        ka.cmp(kb)
    });
    if query_one(q, "order") == Some("DESCENDING") {
        items.reverse();
    }
}

/// Coerce a `PutPolicy` policy body into a full `Policy` object, defaulting each
/// input class to `ALLOWED` (MediaConvert's default when a class is omitted).
fn normalize_policy(policy: Option<&Value>) -> Value {
    let get = |k: &str| {
        policy
            .and_then(|p| p.get(k))
            .and_then(Value::as_str)
            .filter(|v| *v == "ALLOWED" || *v == "DISALLOWED")
            .unwrap_or("ALLOWED")
    };
    json!({
        "httpInputs": get("httpInputs"),
        "httpsInputs": get("httpsInputs"),
        "s3Inputs": get("s3Inputs"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn svc() -> MediaConvertService {
        MediaConvertService::new(Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "http://localhost:4566",
        ))))
    }

    fn ctx() -> Ctx {
        Ctx {
            account: "000000000000".to_string(),
            region: "us-east-1".to_string(),
        }
    }

    #[test]
    fn create_get_queue_round_trips() {
        let s = svc();
        let c = ctx();
        let resp = s
            .create_queue(&c, &json!({ "name": "MyQueue", "description": "d" }))
            .unwrap();
        assert_eq!(resp.status, StatusCode::CREATED);
        let g = s.get_queue(&c, "MyQueue").unwrap();
        assert_eq!(g.status, StatusCode::OK);
    }

    #[test]
    fn default_queue_cannot_be_deleted() {
        let s = svc();
        assert!(s.delete_queue(&ctx(), "Default").is_err());
    }

    #[test]
    fn duplicate_queue_conflicts() {
        let s = svc();
        let c = ctx();
        s.create_queue(&c, &json!({ "name": "Q" })).unwrap();
        assert!(s.create_queue(&c, &json!({ "name": "Q" })).is_err());
    }

    #[test]
    fn job_settles_complete_on_reconcile() {
        let s = svc();
        let c = ctx();
        let resp = s
            .create_job(
                &c,
                &json!({ "role": "arn:aws:iam::000000000000:role/r", "settings": { "outputGroups": [ {} ] } }),
            )
            .unwrap();
        assert_eq!(resp.status, StatusCode::CREATED);
        // A job is created SUBMITTED and settles COMPLETE on the next reconcile.
        assert!(s.reconcile(&c.account));
        let id = {
            let g = s.state.read();
            g.get(&c.account)
                .unwrap()
                .jobs
                .keys()
                .next()
                .unwrap()
                .clone()
        };
        let g = s.state.read();
        let job = &g.get(&c.account).unwrap().jobs[&id];
        assert_eq!(job["status"], "COMPLETE");
        assert_eq!(job["jobPercentComplete"], 100);
        assert!(job["outputGroupDetails"].as_array().unwrap().len() == 1);
    }

    #[test]
    fn describe_endpoints_echoes_configured_url() {
        let s = svc();
        let resp = s.describe_endpoints(&json!({}), &[]).unwrap();
        assert_eq!(resp.status, StatusCode::OK);
    }

    #[test]
    fn tag_and_list_tags() {
        let s = svc();
        let c = ctx();
        s.tag_resource(&c, &json!({ "arn": "arn:x", "tags": { "k": "v" } }))
            .unwrap();
        let resp = s.list_tags(&c, "arn:x").unwrap();
        assert_eq!(resp.status, StatusCode::OK);
    }

    #[test]
    fn put_get_policy_defaults_allowed() {
        let s = svc();
        let c = ctx();
        let resp = s
            .put_policy(&c, &json!({ "policy": { "s3Inputs": "DISALLOWED" } }))
            .unwrap();
        assert_eq!(resp.status, StatusCode::OK);
        let g = s.get_policy(&c).unwrap();
        assert_eq!(g.status, StatusCode::OK);
    }
}
