//! Amazon Textract awsJson1_1 dispatch + operation handlers.
//!
//! Requests carry `X-Amz-Target: Textract.<Operation>`; dispatch keys off
//! `req.action`. Every operation runs model-driven input validation
//! (required / length / range / enum) first, then real, account-partitioned,
//! persisted logic. Operations that dereference an adapter/version that does
//! not exist return `ResourceNotFoundException`; operations that dereference an
//! unknown asynchronous job return `InvalidJobIdException`; input-shape
//! violations return `InvalidParameterException` -- the exact exceptions the
//! Textract Smithy model declares.
//!
//! The OCR / ML inference itself is an honest gap: fakecloud does not run a
//! real text-extraction model, so the analysis operations return a faithful,
//! deterministic structural response (`DocumentMetadata` with the page count
//! derived from the input, well-formed but empty block/expense/identity/lending
//! collections) rather than fabricated extracted text.

use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{Job, SharedTextractState, TextractState};

pub const TEXTRACT_ACTIONS: &[&str] = &[
    "AnalyzeDocument",
    "AnalyzeExpense",
    "AnalyzeID",
    "CreateAdapter",
    "CreateAdapterVersion",
    "DeleteAdapter",
    "DeleteAdapterVersion",
    "DetectDocumentText",
    "GetAdapter",
    "GetAdapterVersion",
    "GetDocumentAnalysis",
    "GetDocumentTextDetection",
    "GetExpenseAnalysis",
    "GetLendingAnalysis",
    "GetLendingAnalysisSummary",
    "ListAdapterVersions",
    "ListAdapters",
    "ListTagsForResource",
    "StartDocumentAnalysis",
    "StartDocumentTextDetection",
    "StartExpenseAnalysis",
    "StartLendingAnalysis",
    "TagResource",
    "UntagResource",
    "UpdateAdapter",
];

/// Actions that mutate persisted state (create/update/delete/tag) or that
/// settle a job/adapter-version lifecycle on read, so a snapshot is written
/// after they succeed.
fn is_mutating_action(action: &str) -> bool {
    const MUTATING_PREFIXES: &[&str] = &["Start", "Create", "Update", "Delete", "Tag", "Untag"];
    if MUTATING_PREFIXES.iter().any(|p| action.starts_with(p)) {
        return true;
    }
    // Get operations that settle an in-progress lifecycle to a terminal state.
    matches!(
        action,
        "GetDocumentTextDetection"
            | "GetDocumentAnalysis"
            | "GetExpenseAnalysis"
            | "GetLendingAnalysis"
            | "GetLendingAnalysisSummary"
            | "GetAdapterVersion"
    )
}

pub struct TextractService {
    state: SharedTextractState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl TextractService {
    pub fn new(state: SharedTextractState) -> Self {
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

    /// Run `f` against this account's mutable state.
    fn with_account_mut<R>(&self, req: &AwsRequest, f: impl FnOnce(&mut TextractState) -> R) -> R {
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        f(acct)
    }

    /// Run `f` against this account's read-only state.
    fn with_account<R>(&self, req: &AwsRequest, f: impl FnOnce(&TextractState) -> R) -> R {
        let guard = self.state.read();
        match guard.get(&req.account_id) {
            Some(acct) => f(acct),
            None => f(&TextractState::default()),
        }
    }
}

#[async_trait]
impl AwsService for TextractService {
    fn service_name(&self) -> &str {
        "textract"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let action = req.action.clone();
        // Model-driven input validation (required/length/range/enum) runs
        // before any handler, mirroring AWS's request-validation phase.
        if let Err(msg) = crate::validate::validate_input(&action, &req.json_body()) {
            return Err(invalid_parameter(msg));
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
        TEXTRACT_ACTIONS
    }
}

impl TextractService {
    fn dispatch(&self, action: &str, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match action {
            // Synchronous analysis
            "DetectDocumentText" => self.detect_document_text(req),
            "AnalyzeDocument" => self.analyze_document(req),
            "AnalyzeExpense" => self.analyze_expense(req),
            "AnalyzeID" => self.analyze_id(req),
            // Asynchronous jobs -- start
            "StartDocumentTextDetection" => self.start_job(req, "text_detection"),
            "StartDocumentAnalysis" => self.start_job(req, "document_analysis"),
            "StartExpenseAnalysis" => self.start_job(req, "expense_analysis"),
            "StartLendingAnalysis" => self.start_job(req, "lending_analysis"),
            // Asynchronous jobs -- get
            "GetDocumentTextDetection" => self.get_text_detection(req),
            "GetDocumentAnalysis" => self.get_document_analysis(req),
            "GetExpenseAnalysis" => self.get_expense_analysis(req),
            "GetLendingAnalysis" => self.get_lending_analysis(req),
            "GetLendingAnalysisSummary" => self.get_lending_analysis_summary(req),
            // Adapters
            "CreateAdapter" => self.create_adapter(req),
            "GetAdapter" => self.get_adapter(req),
            "UpdateAdapter" => self.update_adapter(req),
            "DeleteAdapter" => self.delete_adapter(req),
            "ListAdapters" => self.list_adapters(req),
            // Adapter versions
            "CreateAdapterVersion" => self.create_adapter_version(req),
            "GetAdapterVersion" => self.get_adapter_version(req),
            "DeleteAdapterVersion" => self.delete_adapter_version(req),
            "ListAdapterVersions" => self.list_adapter_versions(req),
            // Tagging
            "TagResource" => self.tag_resource(req),
            "UntagResource" => self.untag_resource(req),
            "ListTagsForResource" => self.list_tags_for_resource(req),
            _ => Err(AwsServiceError::action_not_implemented(
                self.service_name(),
                action,
            )),
        }
    }
}

// ---- shared helpers -------------------------------------------------------

fn aws_client_error(code: &str, msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, code, msg.into())
}

/// Textract's input-validation client error; declared on every operation.
pub(crate) fn invalid_parameter(msg: impl Into<String>) -> AwsServiceError {
    aws_client_error("InvalidParameterException", msg)
}

/// Returned when a request dereferences an adapter/version that does not exist.
fn resource_not_found(msg: impl Into<String>) -> AwsServiceError {
    aws_client_error("ResourceNotFoundException", msg)
}

/// Returned when a `Get*` job operation names an unknown/mismatched job id.
fn invalid_job_id(msg: impl Into<String>) -> AwsServiceError {
    aws_client_error("InvalidJobIdException", msg)
}

fn ok(value: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::ok_json(value))
}

/// Current time as epoch seconds (awsJson1_1 default timestamp wire form).
fn now_epoch() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}

/// A lowercase-hex random string of `n` characters derived from v4 UUIDs.
fn rand_hex(n: usize) -> String {
    let mut s = String::new();
    while s.len() < n {
        s.push_str(&uuid::Uuid::new_v4().simple().to_string());
    }
    s.truncate(n);
    s
}

/// Mint a 64-character Textract-style job id (matches `^[a-zA-Z0-9-_]+$`).
fn new_job_id() -> String {
    rand_hex(64)
}

/// Mint a 32-character adapter id (within the model's 12..1011 length bound).
fn new_adapter_id() -> String {
    rand_hex(32)
}

fn adapter_arn(region: &str, account: &str, adapter_id: &str) -> String {
    format!("arn:aws:textract:{region}:{account}:adapter/{adapter_id}")
}

fn adapter_version_arn(region: &str, account: &str, adapter_id: &str, version: &str) -> String {
    format!("arn:aws:textract:{region}:{account}:adapter/{adapter_id}/version/{version}")
}

fn sf<'a>(b: &'a Value, k: &str) -> Option<&'a str> {
    b.get(k).and_then(Value::as_str)
}

/// The model version string reported for each job kind's `Get*` response, keyed
/// by the response member name AWS uses.
fn job_model_version(kind: &str) -> &'static str {
    match kind {
        "text_detection" => "1.0",
        "document_analysis" => "1.0",
        "expense_analysis" => "1.0",
        "lending_analysis" => "1.0",
        _ => "1.0",
    }
}

/// Extract the adapter id (and optional version) an ARN refers to, if it is a
/// well-formed Textract adapter ARN. Returns `(adapter_id, Option<version>)`.
fn parse_adapter_arn(arn: &str) -> Option<(String, Option<String>)> {
    // arn:aws:textract:REGION:ACCOUNT:adapter/ID[/version/V]
    let resource = arn.splitn(6, ':').nth(5)?;
    let rest = resource.strip_prefix("adapter/")?;
    if let Some((id, ver)) = rest.split_once("/version/") {
        Some((id.to_string(), Some(ver.to_string())))
    } else {
        Some((rest.to_string(), None))
    }
}

impl TextractService {
    // ---- synchronous analysis --------------------------------------------

    fn detect_document_text(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        self.validate_document(body.get("Document"))?;
        ok(json!({
            "DocumentMetadata": { "Pages": 1 },
            "Blocks": [],
            "DetectDocumentTextModelVersion": "1.0",
        }))
    }

    fn analyze_document(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        self.validate_document(body.get("Document"))?;
        ok(json!({
            "DocumentMetadata": { "Pages": 1 },
            "Blocks": [],
            "AnalyzeDocumentModelVersion": "1.0",
        }))
    }

    fn analyze_expense(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        self.validate_document(body.get("Document"))?;
        ok(json!({
            "DocumentMetadata": { "Pages": 1 },
            "ExpenseDocuments": [],
        }))
    }

    fn analyze_id(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pages = body
            .get("DocumentPages")
            .and_then(Value::as_array)
            .map(|a| a.len().max(1))
            .unwrap_or(1);
        ok(json!({
            "IdentityDocuments": [],
            "DocumentMetadata": { "Pages": pages },
            "AnalyzeIDModelVersion": "1.0",
        }))
    }

    /// A `Document` must carry `Bytes` xor `S3Object`; an empty document is
    /// rejected with `InvalidParameterException`, matching the live service.
    fn validate_document(&self, doc: Option<&Value>) -> Result<(), AwsServiceError> {
        let Some(doc) = doc else {
            return Err(invalid_parameter("Document is a required field"));
        };
        let has_bytes = doc.get("Bytes").is_some_and(|v| !v.is_null());
        let has_s3 = doc.get("S3Object").is_some_and(|v| !v.is_null());
        if !has_bytes && !has_s3 {
            return Err(invalid_parameter(
                "Document must contain either Bytes or an S3Object",
            ));
        }
        Ok(())
    }

    // ---- asynchronous jobs -----------------------------------------------

    fn start_job(&self, req: &AwsRequest, kind: &str) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let token = sf(&body, "ClientRequestToken").map(str::to_string);
        let job_tag = sf(&body, "JobTag").map(str::to_string);
        let model_version = job_model_version(kind).to_string();
        let job_id = self.with_account_mut(req, |st| {
            if let Some(tok) = &token {
                if let Some(existing) = st.job_tokens.get(tok) {
                    return existing.clone();
                }
            }
            let id = new_job_id();
            st.jobs.insert(
                id.clone(),
                Job {
                    kind: kind.to_string(),
                    status: "IN_PROGRESS".to_string(),
                    pages: 1,
                    settled: false,
                    job_tag,
                    model_version,
                },
            );
            if let Some(tok) = token {
                st.job_tokens.insert(tok, id.clone());
            }
            id
        });
        ok(json!({ "JobId": job_id }))
    }

    /// Fetch a job of the expected kind, settling `IN_PROGRESS -> SUCCEEDED` on
    /// first read. An unknown id, or an id belonging to a different job kind,
    /// yields `InvalidJobIdException` -- the model's declared job-not-found
    /// exception.
    fn settle_job(&self, req: &AwsRequest, expected_kind: &str) -> Result<Job, AwsServiceError> {
        let job_id = sf(&req.json_body(), "JobId")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |st| {
            let Some(job) = st.jobs.get_mut(&job_id) else {
                return Err(invalid_job_id(format!(
                    "An invalid job identifier was passed: {job_id}"
                )));
            };
            if job.kind != expected_kind {
                return Err(invalid_job_id(format!(
                    "An invalid job identifier was passed: {job_id}"
                )));
            }
            if !job.settled {
                job.status = "SUCCEEDED".to_string();
                job.settled = true;
            }
            Ok(job.clone())
        })
    }

    fn get_text_detection(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let job = self.settle_job(req, "text_detection")?;
        ok(json!({
            "DocumentMetadata": { "Pages": job.pages },
            "JobStatus": job.status,
            "Blocks": [],
            "Warnings": [],
            "DetectDocumentTextModelVersion": job.model_version,
        }))
    }

    fn get_document_analysis(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let job = self.settle_job(req, "document_analysis")?;
        ok(json!({
            "DocumentMetadata": { "Pages": job.pages },
            "JobStatus": job.status,
            "Blocks": [],
            "Warnings": [],
            "AnalyzeDocumentModelVersion": job.model_version,
        }))
    }

    fn get_expense_analysis(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let job = self.settle_job(req, "expense_analysis")?;
        ok(json!({
            "DocumentMetadata": { "Pages": job.pages },
            "JobStatus": job.status,
            "ExpenseDocuments": [],
            "Warnings": [],
            "AnalyzeExpenseModelVersion": job.model_version,
        }))
    }

    fn get_lending_analysis(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let job = self.settle_job(req, "lending_analysis")?;
        ok(json!({
            "DocumentMetadata": { "Pages": job.pages },
            "JobStatus": job.status,
            "Results": [],
            "Warnings": [],
            "AnalyzeLendingModelVersion": job.model_version,
        }))
    }

    fn get_lending_analysis_summary(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let job = self.settle_job(req, "lending_analysis")?;
        ok(json!({
            "DocumentMetadata": { "Pages": job.pages },
            "JobStatus": job.status,
            "Summary": { "DocumentGroups": [], "UndetectedDocumentTypes": [] },
            "Warnings": [],
            "AnalyzeLendingModelVersion": job.model_version,
        }))
    }

    // ---- adapters --------------------------------------------------------

    fn create_adapter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = sf(&body, "AdapterName").unwrap_or_default().to_string();
        let feature_types = body
            .get("FeatureTypes")
            .cloned()
            .unwrap_or_else(|| json!([]));
        let description = body.get("Description").cloned();
        let auto_update = sf(&body, "AutoUpdate").unwrap_or("DISABLED").to_string();
        let token = sf(&body, "ClientRequestToken").map(str::to_string);
        let tags = body.get("Tags").cloned();
        let region = req.region.clone();
        let account = req.account_id.clone();
        let created = now_epoch();

        let adapter_id = self.with_account_mut(req, |st| {
            if let Some(tok) = &token {
                if let Some(existing) = st.adapter_tokens.get(tok) {
                    return existing.clone();
                }
            }
            let id = new_adapter_id();
            let mut record = serde_json::Map::new();
            record.insert("AdapterId".into(), json!(id));
            record.insert("AdapterName".into(), json!(name));
            record.insert("CreationTime".into(), json!(created));
            if let Some(d) = &description {
                record.insert("Description".into(), d.clone());
            }
            record.insert("FeatureTypes".into(), feature_types);
            record.insert("AutoUpdate".into(), json!(auto_update));
            st.adapters.insert(id.clone(), Value::Object(record));
            st.adapter_order.push(id.clone());
            st.next_adapter_version.insert(id.clone(), 1);
            if let Some(t) = tags.filter(|v| v.is_object()) {
                let arn = adapter_arn(&region, &account, &id);
                st.tags.insert(arn, t);
            }
            if let Some(tok) = token {
                st.adapter_tokens.insert(tok, id.clone());
            }
            id
        });
        ok(json!({ "AdapterId": adapter_id }))
    }

    fn get_adapter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = sf(&req.json_body(), "AdapterId")
            .unwrap_or_default()
            .to_string();
        let arn = adapter_arn(&req.region, &req.account_id, &id);
        self.with_account(req, |st| {
            let Some(record) = st.adapters.get(&id) else {
                return Err(resource_not_found(format!("Adapter not found: {id}")));
            };
            let mut out = record.clone();
            if let Some(obj) = out.as_object_mut() {
                if let Some(tags) = st.tags.get(&arn) {
                    obj.insert("Tags".into(), tags.clone());
                }
            }
            ok(out)
        })
    }

    fn update_adapter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "AdapterId").unwrap_or_default().to_string();
        self.with_account_mut(req, |st| {
            let Some(record) = st.adapters.get_mut(&id) else {
                return Err(resource_not_found(format!("Adapter not found: {id}")));
            };
            let obj = record.as_object_mut().expect("adapter record is an object");
            if let Some(v) = body.get("AdapterName").filter(|v| !v.is_null()) {
                obj.insert("AdapterName".into(), v.clone());
            }
            if let Some(v) = body.get("Description").filter(|v| !v.is_null()) {
                obj.insert("Description".into(), v.clone());
            }
            if let Some(v) = body.get("AutoUpdate").filter(|v| !v.is_null()) {
                obj.insert("AutoUpdate".into(), v.clone());
            }
            let mut out = obj.clone();
            out.remove("Tags");
            ok(Value::Object(out))
        })
    }

    fn delete_adapter(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let id = sf(&req.json_body(), "AdapterId")
            .unwrap_or_default()
            .to_string();
        let arn = adapter_arn(&req.region, &req.account_id, &id);
        self.with_account_mut(req, |st| {
            if st.adapters.remove(&id).is_none() {
                return Err(resource_not_found(format!("Adapter not found: {id}")));
            }
            st.adapter_order.retain(|x| x != &id);
            st.next_adapter_version.remove(&id);
            let prefix = format!("{id}\u{1}");
            let keys: Vec<String> = st
                .adapter_versions
                .keys()
                .filter(|k| k.starts_with(&prefix))
                .cloned()
                .collect();
            for k in keys {
                st.adapter_versions.remove(&k);
                st.adapter_version_order.retain(|x| x != &k);
            }
            st.tags.remove(&arn);
            let vprefix = format!("{arn}/version/");
            let tag_keys: Vec<String> = st
                .tags
                .keys()
                .filter(|k| k.starts_with(&vprefix))
                .cloned()
                .collect();
            for k in tag_keys {
                st.tags.remove(&k);
            }
            ok(json!({}))
        })
    }

    fn list_adapters(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let (start, max) = pagination(&body);
        self.with_account(req, |st| {
            let items: Vec<Value> = st
                .adapter_order
                .iter()
                .filter_map(|id| st.adapters.get(id))
                .map(|rec| {
                    json!({
                        "AdapterId": rec.get("AdapterId").cloned().unwrap_or(Value::Null),
                        "AdapterName": rec.get("AdapterName").cloned().unwrap_or(Value::Null),
                        "CreationTime": rec.get("CreationTime").cloned().unwrap_or(Value::Null),
                        "FeatureTypes": rec.get("FeatureTypes").cloned().unwrap_or(json!([])),
                    })
                })
                .collect();
            let (page, next) = paginate(items, start, max);
            let mut out = json!({ "Adapters": page });
            if let Some(n) = next {
                out.as_object_mut()
                    .unwrap()
                    .insert("NextToken".into(), json!(n));
            }
            ok(out)
        })
    }

    // ---- adapter versions ------------------------------------------------

    fn create_adapter_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "AdapterId").unwrap_or_default().to_string();
        let dataset_config = body.get("DatasetConfig").cloned();
        let output_config = body.get("OutputConfig").cloned();
        let kms = body.get("KMSKeyId").cloned();
        let tags = body.get("Tags").cloned();
        let region = req.region.clone();
        let account = req.account_id.clone();
        let created = now_epoch();
        self.with_account_mut(req, |st| {
            let Some(adapter) = st.adapters.get(&id) else {
                return Err(resource_not_found(format!("Adapter not found: {id}")));
            };
            let feature_types = adapter
                .get("FeatureTypes")
                .cloned()
                .unwrap_or_else(|| json!([]));
            let version_num = *st.next_adapter_version.get(&id).unwrap_or(&1);
            st.next_adapter_version.insert(id.clone(), version_num + 1);
            let version = version_num.to_string();
            let key = format!("{id}\u{1}{version}");
            let mut record = serde_json::Map::new();
            record.insert("AdapterId".into(), json!(id));
            record.insert("AdapterVersion".into(), json!(version));
            record.insert("CreationTime".into(), json!(created));
            record.insert("FeatureTypes".into(), feature_types);
            record.insert("Status".into(), json!("CREATION_IN_PROGRESS"));
            record.insert(
                "StatusMessage".into(),
                json!("The adapter version is being created."),
            );
            if let Some(d) = dataset_config {
                record.insert("DatasetConfig".into(), d);
            }
            if let Some(o) = output_config {
                record.insert("OutputConfig".into(), o);
            }
            if let Some(k) = kms {
                record.insert("KMSKeyId".into(), k);
            }
            st.adapter_versions
                .insert(key.clone(), Value::Object(record));
            st.adapter_version_order.push(key);
            if let Some(t) = tags.filter(|v| v.is_object()) {
                let arn = adapter_version_arn(&region, &account, &id, &version);
                st.tags.insert(arn, t);
            }
            ok(json!({ "AdapterId": id, "AdapterVersion": version }))
        })
    }

    fn get_adapter_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "AdapterId").unwrap_or_default().to_string();
        let version = sf(&body, "AdapterVersion").unwrap_or_default().to_string();
        let key = format!("{id}\u{1}{version}");
        let arn = adapter_version_arn(&req.region, &req.account_id, &id, &version);
        self.with_account_mut(req, |st| {
            let Some(record) = st.adapter_versions.get_mut(&key) else {
                return Err(resource_not_found(format!(
                    "Adapter version not found: {id}/{version}"
                )));
            };
            let obj = record.as_object_mut().expect("version record is an object");
            // Settle the creation lifecycle to ACTIVE on first read.
            if obj.get("Status").and_then(Value::as_str) == Some("CREATION_IN_PROGRESS") {
                obj.insert("Status".into(), json!("ACTIVE"));
                obj.insert(
                    "StatusMessage".into(),
                    json!("The adapter version is ready to use."),
                );
                obj.insert(
                    "EvaluationMetrics".into(),
                    json!([{
                        "AdapterVersion": { "F1Score": 0.0, "Precision": 0.0, "Recall": 0.0 },
                        "Baseline": { "F1Score": 0.0, "Precision": 0.0, "Recall": 0.0 },
                        "FeatureType": obj.get("FeatureTypes")
                            .and_then(Value::as_array)
                            .and_then(|a| a.first())
                            .cloned()
                            .unwrap_or(Value::Null),
                    }]),
                );
            }
            let mut out = obj.clone();
            if let Some(tags) = st.tags.get(&arn) {
                out.insert("Tags".into(), tags.clone());
            }
            ok(Value::Object(out))
        })
    }

    fn delete_adapter_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = sf(&body, "AdapterId").unwrap_or_default().to_string();
        let version = sf(&body, "AdapterVersion").unwrap_or_default().to_string();
        let key = format!("{id}\u{1}{version}");
        let arn = adapter_version_arn(&req.region, &req.account_id, &id, &version);
        self.with_account_mut(req, |st| {
            if st.adapter_versions.remove(&key).is_none() {
                return Err(resource_not_found(format!(
                    "Adapter version not found: {id}/{version}"
                )));
            }
            st.adapter_version_order.retain(|x| x != &key);
            st.tags.remove(&arn);
            ok(json!({}))
        })
    }

    fn list_adapter_versions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let filter_id = sf(&body, "AdapterId").map(str::to_string);
        let (start, max) = pagination(&body);
        self.with_account(req, |st| {
            let items: Vec<Value> = st
                .adapter_version_order
                .iter()
                .filter_map(|k| st.adapter_versions.get(k))
                .filter(|rec| {
                    filter_id
                        .as_deref()
                        .is_none_or(|fid| rec.get("AdapterId").and_then(Value::as_str) == Some(fid))
                })
                .map(|rec| {
                    json!({
                        "AdapterId": rec.get("AdapterId").cloned().unwrap_or(Value::Null),
                        "AdapterVersion": rec.get("AdapterVersion").cloned().unwrap_or(Value::Null),
                        "CreationTime": rec.get("CreationTime").cloned().unwrap_or(Value::Null),
                        "FeatureTypes": rec.get("FeatureTypes").cloned().unwrap_or(json!([])),
                        "Status": rec.get("Status").cloned().unwrap_or(Value::Null),
                        "StatusMessage": rec.get("StatusMessage").cloned().unwrap_or(Value::Null),
                    })
                })
                .collect();
            let (page, next) = paginate(items, start, max);
            let mut out = json!({ "AdapterVersions": page });
            if let Some(n) = next {
                out.as_object_mut()
                    .unwrap()
                    .insert("NextToken".into(), json!(n));
            }
            ok(out)
        })
    }

    // ---- tagging ---------------------------------------------------------

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = sf(&body, "ResourceARN").unwrap_or_default().to_string();
        let tags = body.get("Tags").cloned().unwrap_or_else(|| json!({}));
        self.with_account_mut(req, |st| {
            if !resource_exists(st, &arn) {
                return Err(resource_not_found(format!("Resource not found: {arn}")));
            }
            let entry = st
                .tags
                .entry(arn)
                .or_insert_with(|| json!({}))
                .as_object_mut()
                .expect("tag map is an object");
            if let Some(new) = tags.as_object() {
                for (k, v) in new {
                    entry.insert(k.clone(), v.clone());
                }
            }
            ok(json!({}))
        })
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = sf(&body, "ResourceARN").unwrap_or_default().to_string();
        let keys = body.get("TagKeys").cloned().unwrap_or_else(|| json!([]));
        self.with_account_mut(req, |st| {
            if !resource_exists(st, &arn) {
                return Err(resource_not_found(format!("Resource not found: {arn}")));
            }
            if let Some(entry) = st.tags.get_mut(&arn).and_then(Value::as_object_mut) {
                if let Some(list) = keys.as_array() {
                    for k in list.iter().filter_map(Value::as_str) {
                        entry.remove(k);
                    }
                }
            }
            ok(json!({}))
        })
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = sf(&req.json_body(), "ResourceARN")
            .unwrap_or_default()
            .to_string();
        self.with_account(req, |st| {
            if !resource_exists(st, &arn) {
                return Err(resource_not_found(format!("Resource not found: {arn}")));
            }
            let tags = st.tags.get(&arn).cloned().unwrap_or_else(|| json!({}));
            ok(json!({ "Tags": tags }))
        })
    }
}

/// Whether an ARN refers to an adapter or adapter version that exists in this
/// account's state.
fn resource_exists(st: &TextractState, arn: &str) -> bool {
    let Some((id, version)) = parse_adapter_arn(arn) else {
        return false;
    };
    match version {
        None => st.adapters.contains_key(&id),
        Some(v) => st.adapter_versions.contains_key(&format!("{id}\u{1}{v}")),
    }
}

/// Read `MaxResults` + `NextToken` (a numeric offset) from a request body.
fn pagination(body: &Value) -> (usize, Option<usize>) {
    let start = body
        .get("NextToken")
        .and_then(Value::as_str)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);
    let max = body
        .get("MaxResults")
        .and_then(Value::as_u64)
        .map(|n| n as usize);
    (start, max)
}

/// Slice `items` from `start`, honouring `max`, returning the page plus the
/// next-token offset when more items remain.
fn paginate(items: Vec<Value>, start: usize, max: Option<usize>) -> (Vec<Value>, Option<String>) {
    let total = items.len();
    let start = start.min(total);
    let end = match max {
        Some(m) => (start + m).min(total),
        None => total,
    };
    let page = items[start..end].to_vec();
    let next = if end < total {
        Some(end.to_string())
    } else {
        None
    };
    (page, next)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use fakecloud_core::multi_account::MultiAccountState;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::HashMap;

    fn service() -> TextractService {
        let state: SharedTextractState = Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "http://localhost",
        )));
        TextractService::new(state)
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "textract".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "000000000000".to_string(),
            request_id: "test".to_string(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn body_of(resp: AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    fn err_code(result: Result<AwsResponse, AwsServiceError>) -> String {
        match result {
            Ok(_) => panic!("expected an error, got a successful response"),
            Err(e) => e.code().to_string(),
        }
    }

    #[tokio::test]
    async fn detect_document_text_returns_metadata() {
        let svc = service();
        let r = svc
            .handle(req(
                "DetectDocumentText",
                json!({ "Document": { "Bytes": "aGVsbG8=" } }),
            ))
            .await
            .unwrap();
        assert!(r.status.is_success());
    }

    #[tokio::test]
    async fn detect_document_text_rejects_empty_document() {
        let svc = service();
        let code = err_code(
            svc.handle(req("DetectDocumentText", json!({ "Document": {} })))
                .await,
        );
        assert_eq!(code, "InvalidParameterException");
    }

    #[tokio::test]
    async fn job_lifecycle_settles_to_succeeded() {
        let svc = service();
        let start = svc
            .handle(req(
                "StartDocumentTextDetection",
                json!({ "DocumentLocation": { "S3Object": { "Bucket": "buck", "Name": "doc.pdf" } } }),
            ))
            .await
            .unwrap();
        let body = body_of(start);
        let job_id = body
            .get("JobId")
            .and_then(Value::as_str)
            .unwrap()
            .to_string();
        assert_eq!(job_id.len(), 64);
        let get = svc
            .handle(req("GetDocumentTextDetection", json!({ "JobId": job_id })))
            .await
            .unwrap();
        assert_eq!(
            body_of(get).get("JobStatus").and_then(Value::as_str),
            Some("SUCCEEDED")
        );
    }

    #[tokio::test]
    async fn get_unknown_job_is_invalid_job_id() {
        let svc = service();
        let code = err_code(
            svc.handle(req(
                "GetDocumentTextDetection",
                json!({ "JobId": "nonexistentjob0000" }),
            ))
            .await,
        );
        assert_eq!(code, "InvalidJobIdException");
    }

    #[tokio::test]
    async fn adapter_crud_round_trip() {
        let svc = service();
        let created = svc
            .handle(req(
                "CreateAdapter",
                json!({ "AdapterName": "my-adapter", "FeatureTypes": ["TABLES"] }),
            ))
            .await
            .unwrap();
        let adapter_id = body_of(created)
            .get("AdapterId")
            .and_then(Value::as_str)
            .unwrap()
            .to_string();
        assert!(adapter_id.len() >= 12);

        let got = svc
            .handle(req(
                "GetAdapter",
                json!({ "AdapterId": adapter_id.clone() }),
            ))
            .await
            .unwrap();
        assert_eq!(
            body_of(got).get("AdapterName").and_then(Value::as_str),
            Some("my-adapter")
        );

        // Tag round-trip via the adapter ARN.
        let arn = adapter_arn("us-east-1", "000000000000", &adapter_id);
        svc.handle(req(
            "TagResource",
            json!({ "ResourceARN": arn, "Tags": { "env": "test" } }),
        ))
        .await
        .unwrap();
        let tags = svc
            .handle(req(
                "ListTagsForResource",
                json!({ "ResourceARN": adapter_arn("us-east-1", "000000000000", &adapter_id) }),
            ))
            .await
            .unwrap();
        assert_eq!(
            body_of(tags).pointer("/Tags/env").and_then(Value::as_str),
            Some("test")
        );

        svc.handle(req(
            "DeleteAdapter",
            json!({ "AdapterId": adapter_id.clone() }),
        ))
        .await
        .unwrap();
        let code = err_code(
            svc.handle(req("GetAdapter", json!({ "AdapterId": adapter_id })))
                .await,
        );
        assert_eq!(code, "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn unknown_resource_arn_tag_is_not_found() {
        let svc = service();
        let code = err_code(
            svc.handle(req(
                "ListTagsForResource",
                json!({ "ResourceARN": "arn:aws:textract:us-east-1:000000000000:adapter/doesnotexist" }),
            ))
            .await,
        );
        assert_eq!(code, "ResourceNotFoundException");
    }
}
