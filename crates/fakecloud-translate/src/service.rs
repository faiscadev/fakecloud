//! Amazon Translate awsJson1_1 dispatch + operation handlers.
//!
//! Requests carry `X-Amz-Target: AWSShineFrontendService_20170701.<Operation>`;
//! dispatch keys off `req.action`. Every operation runs model-driven input
//! validation first (required / length / range / enum / pattern), then real,
//! account-partitioned, persisted CRUD. Dereferencing a resource that does not
//! exist returns Translate's `ResourceNotFoundException`; a duplicate parallel-
//! data name returns `ConflictException`.
//!
//! Honest machine-translation gap: fakecloud runs no MT model, so `TranslateText`
//! / `TranslateDocument` echo the input content verbatim as the translated
//! output with the requested target language applied -- a structurally correct
//! passthrough, never a fabricated translation.

use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::shared::{
    data_location, decode_blob, job_id, now_epoch, parse_file, parse_resource_arn, resource_arn,
};
use crate::state::{SharedTranslateState, TranslateData};

/// Every operation name in the Amazon Translate Smithy model (19 operations).
pub const TRANSLATE_ACTIONS: &[&str] = &[
    "CreateParallelData",
    "DeleteParallelData",
    "DeleteTerminology",
    "DescribeTextTranslationJob",
    "GetParallelData",
    "GetTerminology",
    "ImportTerminology",
    "ListLanguages",
    "ListParallelData",
    "ListTagsForResource",
    "ListTerminologies",
    "ListTextTranslationJobs",
    "StartTextTranslationJob",
    "StopTextTranslationJob",
    "TagResource",
    "TranslateDocument",
    "TranslateText",
    "UntagResource",
    "UpdateParallelData",
];

/// Read-only verbs; any other action mutates persisted state and triggers a
/// snapshot after success. `TranslateText` / `TranslateDocument` are pure reads
/// (they persist nothing), so they are excluded from the mutating set too.
fn is_mutating_action(action: &str) -> bool {
    const READ_PREFIXES: &[&str] = &["Get", "List", "Describe"];
    if action == "TranslateText" || action == "TranslateDocument" {
        return false;
    }
    !READ_PREFIXES.iter().any(|p| action.starts_with(p))
}

pub struct TranslateService {
    state: SharedTranslateState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl TranslateService {
    pub fn new(state: SharedTranslateState) -> Self {
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
    fn with_account_mut<R>(&self, req: &AwsRequest, f: impl FnOnce(&mut TranslateData) -> R) -> R {
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        f(acct)
    }
}

#[async_trait]
impl AwsService for TranslateService {
    fn service_name(&self) -> &str {
        "translate"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let action = req.action.clone();
        if let Err(msg) = crate::validate::validate_input(&action, &req.json_body()) {
            let code = crate::validate::validation_error_code(&action);
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                code,
                msg,
            ));
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
        TRANSLATE_ACTIONS
    }
}

impl TranslateService {
    fn dispatch(&self, action: &str, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        match action {
            // Synchronous translation
            "TranslateText" => self.translate_text(req, &body),
            "TranslateDocument" => self.translate_document(req, &body),
            // Batch translation jobs
            "StartTextTranslationJob" => self.start_text_translation_job(req, &body),
            "DescribeTextTranslationJob" => self.describe_text_translation_job(req, &body),
            "ListTextTranslationJobs" => self.list_text_translation_jobs(req, &body),
            "StopTextTranslationJob" => self.stop_text_translation_job(req, &body),
            // Parallel data
            "CreateParallelData" => self.create_parallel_data(req, &body),
            "GetParallelData" => self.get_parallel_data(req, &body),
            "UpdateParallelData" => self.update_parallel_data(req, &body),
            "ListParallelData" => self.list_parallel_data(req, &body),
            "DeleteParallelData" => self.delete_parallel_data(req, &body),
            // Custom terminologies
            "ImportTerminology" => self.import_terminology(req, &body),
            "GetTerminology" => self.get_terminology(req, &body),
            "ListTerminologies" => self.list_terminologies(req, &body),
            "DeleteTerminology" => self.delete_terminology(req, &body),
            // Supported languages
            "ListLanguages" => self.list_languages(req, &body),
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

// ---- error / response helpers --------------------------------------------

fn invalid_parameter(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterValueException",
        msg.into(),
    )
}

fn conflict(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::CONFLICT, "ConflictException", msg.into())
}

fn not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundException",
        msg.into(),
    )
}

fn ok(value: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::ok_json(value))
}

// ---- small member helpers -------------------------------------------------

fn str_member<'a>(body: &'a Value, name: &str) -> Option<&'a str> {
    body.get(name).and_then(Value::as_str)
}

/// Copy each named member from `body` into `obj` when present and non-null.
fn copy_members(body: &Value, obj: &mut Map<String, Value>, names: &[&str]) {
    for name in names {
        if let Some(v) = body.get(*name).filter(|v| !v.is_null()) {
            obj.insert((*name).to_string(), v.clone());
        }
    }
}

/// Convert a `TagList` (`[{Key,Value}]`) into a sorted key/value map.
fn taglist_to_map(tags: &Value) -> std::collections::BTreeMap<String, String> {
    let mut out = std::collections::BTreeMap::new();
    if let Some(arr) = tags.as_array() {
        for t in arr {
            if let (Some(k), Some(v)) = (str_member(t, "Key"), str_member(t, "Value")) {
                out.insert(k.to_string(), v.to_string());
            }
        }
    }
    out
}

/// Render a key/value map back into a `TagList`.
fn map_to_taglist(map: &std::collections::BTreeMap<String, String>) -> Value {
    Value::Array(
        map.iter()
            .map(|(k, v)| json!({ "Key": k, "Value": v }))
            .collect(),
    )
}

impl TranslateData {
    /// Record `Tags` from a create/import request under `arn`, if any.
    fn store_tags(&mut self, arn: &str, body: &Value) {
        if let Some(tags) = body.get("Tags").filter(|v| !v.is_null()) {
            let map = taglist_to_map(tags);
            if !map.is_empty() {
                self.tags.insert(arn.to_string(), map);
            }
        }
    }

    /// True if the resource named by an ARN's `(type, name)` exists.
    fn resource_exists(&self, rtype: &str, name: &str) -> bool {
        match rtype {
            "terminology" => self.terminologies.contains_key(name),
            "parallel-data" => self.parallel_data.contains_key(name),
            _ => false,
        }
    }
}

// ---- synchronous translation ----------------------------------------------

impl TranslateService {
    fn translate_text(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let text = str_member(body, "Text").unwrap_or_default().to_string();
        let source = str_member(body, "SourceLanguageCode")
            .unwrap_or_default()
            .to_string();
        let target = str_member(body, "TargetLanguageCode")
            .unwrap_or_default()
            .to_string();
        let applied = self.applied_terminologies(req, body)?;
        let mut out = Map::new();
        // Honest MT gap: no translation model runs -- the input text is echoed
        // verbatim as the translated output with the requested target applied.
        out.insert("TranslatedText".into(), json!(text));
        out.insert("SourceLanguageCode".into(), json!(source));
        out.insert("TargetLanguageCode".into(), json!(target));
        if let Some(a) = applied {
            out.insert("AppliedTerminologies".into(), a);
        }
        if let Some(s) = body.get("Settings").filter(|v| !v.is_null()) {
            out.insert("AppliedSettings".into(), s.clone());
        }
        ok(Value::Object(out))
    }

    fn translate_document(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let source = str_member(body, "SourceLanguageCode")
            .unwrap_or_default()
            .to_string();
        let target = str_member(body, "TargetLanguageCode")
            .unwrap_or_default()
            .to_string();
        // Echo the document content verbatim (no MT model runs).
        let content = body
            .get("Document")
            .and_then(|d| d.get("Content"))
            .cloned()
            .unwrap_or(json!(""));
        let applied = self.applied_terminologies(req, body)?;
        let mut out = Map::new();
        out.insert("TranslatedDocument".into(), json!({ "Content": content }));
        out.insert("SourceLanguageCode".into(), json!(source));
        out.insert("TargetLanguageCode".into(), json!(target));
        if let Some(a) = applied {
            out.insert("AppliedTerminologies".into(), a);
        }
        if let Some(s) = body.get("Settings").filter(|v| !v.is_null()) {
            out.insert("AppliedSettings".into(), s.clone());
        }
        ok(Value::Object(out))
    }

    /// Resolve the `AppliedTerminologies` list for a translate call: each
    /// requested terminology must exist, else `ResourceNotFoundException`.
    /// Returns `None` when no terminology names were requested.
    fn applied_terminologies(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<Option<Value>, AwsServiceError> {
        let Some(names) = body.get("TerminologyNames").and_then(Value::as_array) else {
            return Ok(None);
        };
        if names.is_empty() {
            return Ok(None);
        }
        self.with_account_mut(req, |d| {
            let mut applied = Vec::new();
            for n in names.iter().filter_map(Value::as_str) {
                if !d.terminologies.contains_key(n) {
                    return Err(not_found(format!(
                        "The resource you are looking for has not been found. Review the resource you're looking for and try again: {n}"
                    )));
                }
                // No terms are actually applied (no MT model runs), so the
                // applied entry carries an empty term list.
                applied.push(json!({ "Name": n, "Terms": [] }));
            }
            Ok(Some(Value::Array(applied)))
        })
    }
}

// ---- batch translation jobs -----------------------------------------------

impl TranslateService {
    fn start_text_translation_job(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_account_mut(req, |d| {
            // Referenced terminologies / parallel data must exist.
            for n in body
                .get("TerminologyNames")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
            {
                if !d.terminologies.contains_key(n) {
                    return Err(not_found(format!(
                        "The resource you are looking for has not been found. Review the resource you're looking for and try again: {n}"
                    )));
                }
            }
            for n in body
                .get("ParallelDataNames")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
            {
                if !d.parallel_data.contains_key(n) {
                    return Err(not_found(format!(
                        "The resource you are looking for has not been found. Review the resource you're looking for and try again: {n}"
                    )));
                }
            }
            let id = job_id();
            let now = now_epoch();
            let mut obj = Map::new();
            obj.insert("JobId".into(), json!(id));
            obj.insert("JobStatus".into(), json!("SUBMITTED"));
            obj.insert("SubmittedTime".into(), json!(now));
            copy_members(
                body,
                &mut obj,
                &[
                    "JobName",
                    "InputDataConfig",
                    "OutputDataConfig",
                    "DataAccessRoleArn",
                    "SourceLanguageCode",
                    "TargetLanguageCodes",
                    "TerminologyNames",
                    "ParallelDataNames",
                    "Settings",
                ],
            );
            d.text_translation_jobs
                .insert(id.clone(), Value::Object(obj));
            ok(json!({ "JobId": id, "JobStatus": "SUBMITTED" }))
        })
    }

    fn describe_text_translation_job(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = str_member(body, "JobId").unwrap_or_default().to_string();
        self.with_account_mut(req, |d| {
            d.reconcile();
            let Some(job) = d.text_translation_jobs.get(&id).cloned() else {
                return Err(not_found(format!(
                    "Cannot find the resource you are looking for. Review the resource and try again: {id}"
                )));
            };
            ok(json!({ "TextTranslationJobProperties": job }))
        })
    }

    fn list_text_translation_jobs(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_account_mut(req, |d| {
            d.reconcile();
            let filter = body.get("Filter");
            let name_eq = filter.and_then(|f| str_member(f, "JobName"));
            let status_eq = filter.and_then(|f| str_member(f, "JobStatus"));
            let jobs: Vec<Value> = d
                .text_translation_jobs
                .values()
                .filter(|j| {
                    name_eq
                        .map(|n| j.get("JobName").and_then(Value::as_str) == Some(n))
                        .unwrap_or(true)
                        && status_eq
                            .map(|s| j.get("JobStatus").and_then(Value::as_str) == Some(s))
                            .unwrap_or(true)
                })
                .cloned()
                .collect();
            ok(json!({ "TextTranslationJobPropertiesList": jobs }))
        })
    }

    fn stop_text_translation_job(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = str_member(body, "JobId").unwrap_or_default().to_string();
        self.with_account_mut(req, |d| {
            let Some(job) = d.text_translation_jobs.get_mut(&id) else {
                return Err(not_found(format!(
                    "Cannot find the resource you are looking for. Review the resource and try again: {id}"
                )));
            };
            let status = job
                .get("JobStatus")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let new_status = if matches!(status.as_str(), "SUBMITTED" | "IN_PROGRESS") {
                if let Some(obj) = job.as_object_mut() {
                    obj.insert("JobStatus".into(), json!("STOP_REQUESTED"));
                }
                "STOP_REQUESTED".to_string()
            } else {
                status
            };
            ok(json!({ "JobId": id, "JobStatus": new_status }))
        })
    }
}

// ---- parallel data --------------------------------------------------------

impl TranslateService {
    fn create_parallel_data(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "Name").unwrap_or_default().to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            if d.parallel_data.contains_key(&name) {
                return Err(conflict(format!(
                    "Parallel data with the name {name} already exists. Use a different name and try again."
                )));
            }
            let now = now_epoch();
            let arn = resource_arn(&region, &account, "parallel-data", &name);
            let mut obj = Map::new();
            obj.insert("Name".into(), json!(name));
            obj.insert("Arn".into(), json!(arn));
            obj.insert("Status".into(), json!("CREATING"));
            obj.insert("ImportedDataSize".into(), json!(0));
            obj.insert("ImportedRecordCount".into(), json!(0));
            obj.insert("FailedRecordCount".into(), json!(0));
            obj.insert("SkippedRecordCount".into(), json!(0));
            obj.insert("CreatedAt".into(), json!(now));
            obj.insert("LastUpdatedAt".into(), json!(now));
            copy_members(
                body,
                &mut obj,
                &["Description", "ParallelDataConfig", "EncryptionKey"],
            );
            d.parallel_data
                .insert(name.clone(), Value::Object(obj));
            d.store_tags(&arn, body);
            ok(json!({ "Name": name, "Status": "CREATING" }))
        })
    }

    fn get_parallel_data(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "Name").unwrap_or_default().to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            d.reconcile();
            let Some(pd) = d.parallel_data.get(&name).cloned() else {
                return Err(not_found(format!(
                    "The resource you are looking for has not been found. Review the resource you're looking for and try again: {name}"
                )));
            };
            let loc = json!({
                "RepositoryType": "S3",
                "Location": data_location(&region, &account, "parallel-data", &name, "input.tsv"),
            });
            ok(json!({
                "ParallelDataProperties": pd,
                "DataLocation": loc,
            }))
        })
    }

    fn update_parallel_data(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "Name").unwrap_or_default().to_string();
        self.with_account_mut(req, |d| {
            d.reconcile();
            let Some(pd) = d.parallel_data.get_mut(&name) else {
                return Err(not_found(format!(
                    "The resource you are looking for has not been found. Review the resource you're looking for and try again: {name}"
                )));
            };
            let now = now_epoch();
            if let Some(obj) = pd.as_object_mut() {
                obj.insert("LatestUpdateAttemptStatus".into(), json!("UPDATING"));
                obj.insert("LatestUpdateAttemptAt".into(), json!(now));
                obj.insert("LastUpdatedAt".into(), json!(now));
                if let Some(cfg) = body.get("ParallelDataConfig").filter(|v| !v.is_null()) {
                    obj.insert("ParallelDataConfig".into(), cfg.clone());
                }
                if let Some(desc) = body.get("Description").filter(|v| !v.is_null()) {
                    obj.insert("Description".into(), desc.clone());
                }
            }
            let status = pd
                .get("Status")
                .cloned()
                .unwrap_or(json!("ACTIVE"));
            ok(json!({
                "Name": name,
                "Status": status,
                "LatestUpdateAttemptStatus": "UPDATING",
                "LatestUpdateAttemptAt": now,
            }))
        })
    }

    fn list_parallel_data(
        &self,
        req: &AwsRequest,
        _body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_account_mut(req, |d| {
            d.reconcile();
            let list: Vec<Value> = d.parallel_data.values().cloned().collect();
            ok(json!({ "ParallelDataPropertiesList": list }))
        })
    }

    fn delete_parallel_data(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "Name").unwrap_or_default().to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            if d.parallel_data.remove(&name).is_none() {
                return Err(not_found(format!(
                    "The resource you are looking for has not been found. Review the resource you're looking for and try again: {name}"
                )));
            }
            d.tags
                .remove(&resource_arn(&region, &account, "parallel-data", &name));
            ok(json!({ "Name": name, "Status": "DELETING" }))
        })
    }
}

// ---- custom terminologies -------------------------------------------------

impl TranslateService {
    fn import_terminology(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "Name").unwrap_or_default().to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        let td = body.get("TerminologyData");
        let format = td
            .and_then(|t| str_member(t, "Format"))
            .unwrap_or("CSV")
            .to_string();
        let directionality = td
            .and_then(|t| str_member(t, "Directionality"))
            .unwrap_or("UNI")
            .to_string();
        let facts = td
            .and_then(|t| str_member(t, "File"))
            .map(|f| parse_file(&decode_blob(f), &format))
            .unwrap_or_default();
        self.with_account_mut(req, |d| {
            let now = now_epoch();
            // MergeStrategy is OVERWRITE: re-importing replaces, preserving the
            // original CreatedAt when the terminology already exists.
            let created_at = d
                .terminologies
                .get(&name)
                .and_then(|t| t.get("CreatedAt"))
                .cloned()
                .unwrap_or(json!(now));
            let arn = resource_arn(&region, &account, "terminology", &name);
            let mut obj = Map::new();
            obj.insert("Name".into(), json!(name));
            obj.insert("Arn".into(), json!(arn));
            obj.insert("Format".into(), json!(format));
            obj.insert("Directionality".into(), json!(directionality));
            obj.insert("SizeBytes".into(), json!(facts.size_bytes));
            obj.insert("TermCount".into(), json!(facts.record_count));
            obj.insert("SkippedTermCount".into(), json!(0));
            obj.insert("CreatedAt".into(), created_at);
            obj.insert("LastUpdatedAt".into(), json!(now));
            if let Some(src) = &facts.source_language {
                obj.insert("SourceLanguageCode".into(), json!(src));
            }
            if !facts.target_languages.is_empty() {
                obj.insert("TargetLanguageCodes".into(), json!(facts.target_languages));
            }
            copy_members(body, &mut obj, &["Description", "EncryptionKey"]);
            d.terminologies
                .insert(name.clone(), Value::Object(obj.clone()));
            d.store_tags(&arn, body);
            ok(json!({ "TerminologyProperties": Value::Object(obj) }))
        })
    }

    fn get_terminology(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "Name").unwrap_or_default().to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            let Some(term) = d.terminologies.get(&name).cloned() else {
                return Err(not_found(format!(
                    "The resource you are looking for has not been found. Review the resource you're looking for and try again: {name}"
                )));
            };
            let format = term
                .get("Format")
                .and_then(Value::as_str)
                .unwrap_or("CSV")
                .to_lowercase();
            let loc = json!({
                "RepositoryType": "S3",
                "Location": data_location(&region, &account, "terminology", &name, &format!("terminology.{format}")),
            });
            ok(json!({
                "TerminologyProperties": term,
                "TerminologyDataLocation": loc,
            }))
        })
    }

    fn list_terminologies(
        &self,
        req: &AwsRequest,
        _body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_account_mut(req, |d| {
            let list: Vec<Value> = d.terminologies.values().cloned().collect();
            ok(json!({ "TerminologyPropertiesList": list }))
        })
    }

    fn delete_terminology(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "Name").unwrap_or_default().to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            if d.terminologies.remove(&name).is_none() {
                return Err(not_found(format!(
                    "The resource you are looking for has not been found. Review the resource you're looking for and try again: {name}"
                )));
            }
            d.tags
                .remove(&resource_arn(&region, &account, "terminology", &name));
            ok(json!({}))
        })
    }
}

// ---- supported languages --------------------------------------------------

/// The languages Amazon Translate supports, as `(LanguageCode, English name)`.
/// `LanguageName` is returned in English regardless of `DisplayLanguageCode`;
/// localized display names are out of scope for the emulator. The set itself is
/// real (the published supported-languages list).
const SUPPORTED_LANGUAGES: &[(&str, &str)] = &[
    ("af", "Afrikaans"),
    ("sq", "Albanian"),
    ("am", "Amharic"),
    ("ar", "Arabic"),
    ("hy", "Armenian"),
    ("az", "Azerbaijani"),
    ("bn", "Bengali"),
    ("bs", "Bosnian"),
    ("bg", "Bulgarian"),
    ("ca", "Catalan"),
    ("zh", "Chinese (Simplified)"),
    ("zh-TW", "Chinese (Traditional)"),
    ("hr", "Croatian"),
    ("cs", "Czech"),
    ("da", "Danish"),
    ("fa-AF", "Dari"),
    ("nl", "Dutch"),
    ("en", "English"),
    ("et", "Estonian"),
    ("fa", "Farsi (Persian)"),
    ("tl", "Filipino, Tagalog"),
    ("fi", "Finnish"),
    ("fr", "French"),
    ("fr-CA", "French (Canada)"),
    ("ka", "Georgian"),
    ("de", "German"),
    ("el", "Greek"),
    ("gu", "Gujarati"),
    ("ht", "Haitian Creole"),
    ("ha", "Hausa"),
    ("he", "Hebrew"),
    ("hi", "Hindi"),
    ("hu", "Hungarian"),
    ("is", "Icelandic"),
    ("id", "Indonesian"),
    ("ga", "Irish"),
    ("it", "Italian"),
    ("ja", "Japanese"),
    ("kn", "Kannada"),
    ("kk", "Kazakh"),
    ("ko", "Korean"),
    ("lv", "Latvian"),
    ("lt", "Lithuanian"),
    ("mk", "Macedonian"),
    ("ms", "Malay"),
    ("ml", "Malayalam"),
    ("mt", "Maltese"),
    ("mr", "Marathi"),
    ("mn", "Mongolian"),
    ("no", "Norwegian (Bokmal)"),
    ("ps", "Pashto"),
    ("pl", "Polish"),
    ("pt", "Portuguese (Brazil)"),
    ("pt-PT", "Portuguese (Portugal)"),
    ("pa", "Punjabi"),
    ("ro", "Romanian"),
    ("ru", "Russian"),
    ("sr", "Serbian"),
    ("si", "Sinhala"),
    ("sk", "Slovak"),
    ("sl", "Slovenian"),
    ("so", "Somali"),
    ("es", "Spanish"),
    ("es-MX", "Spanish (Mexico)"),
    ("sw", "Swahili"),
    ("sv", "Swedish"),
    ("ta", "Tamil"),
    ("te", "Telugu"),
    ("th", "Thai"),
    ("tr", "Turkish"),
    ("uk", "Ukrainian"),
    ("ur", "Urdu"),
    ("uz", "Uzbek"),
    ("vi", "Vietnamese"),
    ("cy", "Welsh"),
];

impl TranslateService {
    fn list_languages(
        &self,
        _req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let display = str_member(body, "DisplayLanguageCode").unwrap_or("en");
        let languages: Vec<Value> = SUPPORTED_LANGUAGES
            .iter()
            .map(|(code, name)| json!({ "LanguageCode": code, "LanguageName": name }))
            .collect();
        ok(json!({
            "Languages": languages,
            "DisplayLanguageCode": display,
        }))
    }
}

// ---- tagging --------------------------------------------------------------

impl TranslateService {
    fn tag_resource(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "ResourceArn")
            .unwrap_or_default()
            .to_string();
        let Some((rtype, name)) = parse_resource_arn(&arn) else {
            return Err(invalid_parameter(format!("Invalid resource ARN: {arn}")));
        };
        self.with_account_mut(req, |d| {
            if !d.resource_exists(&rtype, &name) {
                return Err(not_found(format!(
                    "The resource you are looking for has not been found. Review the resource you're looking for and try again: {arn}"
                )));
            }
            let entry = d.tags.entry(arn.clone()).or_default();
            for (k, v) in taglist_to_map(body.get("Tags").unwrap_or(&Value::Null)) {
                entry.insert(k, v);
            }
            ok(json!({}))
        })
    }

    fn untag_resource(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "ResourceArn")
            .unwrap_or_default()
            .to_string();
        let Some((rtype, name)) = parse_resource_arn(&arn) else {
            return Err(invalid_parameter(format!("Invalid resource ARN: {arn}")));
        };
        self.with_account_mut(req, |d| {
            if !d.resource_exists(&rtype, &name) {
                return Err(not_found(format!(
                    "The resource you are looking for has not been found. Review the resource you're looking for and try again: {arn}"
                )));
            }
            if let Some(entry) = d.tags.get_mut(&arn) {
                if let Some(keys) = body.get("TagKeys").and_then(Value::as_array) {
                    for k in keys.iter().filter_map(Value::as_str) {
                        entry.remove(k);
                    }
                }
            }
            ok(json!({}))
        })
    }

    fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "ResourceArn")
            .unwrap_or_default()
            .to_string();
        let Some((rtype, name)) = parse_resource_arn(&arn) else {
            return Err(invalid_parameter(format!("Invalid resource ARN: {arn}")));
        };
        self.with_account_mut(req, |d| {
            if !d.resource_exists(&rtype, &name) {
                return Err(not_found(format!(
                    "The resource you are looking for has not been found. Review the resource you're looking for and try again: {arn}"
                )));
            }
            let tags = d.tags.get(&arn).map(map_to_taglist).unwrap_or(json!([]));
            ok(json!({ "Tags": tags }))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn service() -> TranslateService {
        TranslateService::new(Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        ))))
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "translate".to_string(),
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

    fn body_of(resp: AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    #[tokio::test]
    async fn translate_text_echoes_passthrough() {
        let svc = service();
        let resp = svc
            .handle(req(
                "TranslateText",
                json!({ "Text": "hello", "SourceLanguageCode": "en", "TargetLanguageCode": "fr" }),
            ))
            .await
            .unwrap();
        let v = body_of(resp);
        assert_eq!(v["TranslatedText"], "hello");
        assert_eq!(v["SourceLanguageCode"], "en");
        assert_eq!(v["TargetLanguageCode"], "fr");
    }

    #[tokio::test]
    async fn terminology_round_trip_and_tagging() {
        let svc = service();
        let file = base64::engine::general_purpose::STANDARD
            .encode("en,fr\nhello,bonjour\ndog,chien\n".as_bytes());
        svc.handle(req(
            "ImportTerminology",
            json!({
                "Name": "my-term",
                "MergeStrategy": "OVERWRITE",
                "TerminologyData": { "File": file, "Format": "CSV" }
            }),
        ))
        .await
        .unwrap();

        let got = body_of(
            svc.handle(req("GetTerminology", json!({ "Name": "my-term" })))
                .await
                .unwrap(),
        );
        let props = &got["TerminologyProperties"];
        assert_eq!(props["Name"], "my-term");
        assert_eq!(props["SourceLanguageCode"], "en");
        assert_eq!(props["TargetLanguageCodes"][0], "fr");
        assert_eq!(props["TermCount"], 2);
        assert!(got["TerminologyDataLocation"]["Location"].is_string());

        // Tagging.
        let arn = "arn:aws:translate:us-east-1:000000000000:terminology/my-term";
        svc.handle(req(
            "TagResource",
            json!({ "ResourceArn": arn, "Tags": [{ "Key": "team", "Value": "loc" }] }),
        ))
        .await
        .unwrap();
        let tags = body_of(
            svc.handle(req("ListTagsForResource", json!({ "ResourceArn": arn })))
                .await
                .unwrap(),
        );
        assert_eq!(tags["Tags"][0]["Key"], "team");

        // Tagging a missing resource -> ResourceNotFoundException.
        let bad = svc
            .handle(req(
                "ListTagsForResource",
                json!({ "ResourceArn": "arn:aws:translate:us-east-1:000000000000:terminology/nope" }),
            ))
            .await;
        assert_eq!(bad.err().unwrap().code(), "ResourceNotFoundException");

        // Delete -> subsequent Get 404s.
        svc.handle(req("DeleteTerminology", json!({ "Name": "my-term" })))
            .await
            .unwrap();
        let after = svc
            .handle(req("GetTerminology", json!({ "Name": "my-term" })))
            .await;
        assert_eq!(after.err().unwrap().code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn batch_job_settles_to_completed() {
        let svc = service();
        let started = body_of(
            svc.handle(req(
                "StartTextTranslationJob",
                json!({
                    "InputDataConfig": { "S3Uri": "s3://in/", "ContentType": "text/plain" },
                    "OutputDataConfig": { "S3Uri": "s3://out/" },
                    "DataAccessRoleArn": "arn:aws:iam::000000000000:role/translate",
                    "SourceLanguageCode": "en",
                    "TargetLanguageCodes": ["fr"],
                    "ClientToken": "tok-1"
                }),
            ))
            .await
            .unwrap(),
        );
        let id = started["JobId"].as_str().unwrap().to_string();
        assert_eq!(started["JobStatus"], "SUBMITTED");
        assert_eq!(id.len(), 32);

        let described = body_of(
            svc.handle(req("DescribeTextTranslationJob", json!({ "JobId": id })))
                .await
                .unwrap(),
        );
        assert_eq!(
            described["TextTranslationJobProperties"]["JobStatus"],
            "COMPLETED"
        );

        // Missing job -> ResourceNotFoundException.
        let missing = svc
            .handle(req(
                "DescribeTextTranslationJob",
                json!({ "JobId": "doesnotexist" }),
            ))
            .await;
        assert_eq!(missing.err().unwrap().code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn parallel_data_lifecycle() {
        let svc = service();
        svc.handle(req(
            "CreateParallelData",
            json!({
                "Name": "pd1",
                "ParallelDataConfig": { "S3Uri": "s3://b/pd.tsv", "Format": "TSV" },
                "ClientToken": "tok-2"
            }),
        ))
        .await
        .unwrap();
        let got = body_of(
            svc.handle(req("GetParallelData", json!({ "Name": "pd1" })))
                .await
                .unwrap(),
        );
        assert_eq!(got["ParallelDataProperties"]["Status"], "ACTIVE");

        // Duplicate -> ConflictException.
        let dup = svc
            .handle(req(
                "CreateParallelData",
                json!({ "Name": "pd1", "ParallelDataConfig": {}, "ClientToken": "t" }),
            ))
            .await;
        assert_eq!(dup.err().unwrap().code(), "ConflictException");

        svc.handle(req("DeleteParallelData", json!({ "Name": "pd1" })))
            .await
            .unwrap();
        let after = svc
            .handle(req("GetParallelData", json!({ "Name": "pd1" })))
            .await;
        assert_eq!(after.err().unwrap().code(), "ResourceNotFoundException");
    }

    #[tokio::test]
    async fn list_languages_returns_catalogue() {
        let svc = service();
        let v = body_of(svc.handle(req("ListLanguages", json!({}))).await.unwrap());
        let langs = v["Languages"].as_array().unwrap();
        assert!(langs.iter().any(|l| l["LanguageCode"] == "en"));
        assert!(langs.iter().any(|l| l["LanguageCode"] == "fr"));
    }

    #[tokio::test]
    async fn missing_required_is_validation_error() {
        let svc = service();
        // TranslateText missing Text -> InvalidRequestException (its declared
        // validation error).
        let err = svc
            .handle(req(
                "TranslateText",
                json!({ "SourceLanguageCode": "en", "TargetLanguageCode": "fr" }),
            ))
            .await
            .err()
            .unwrap();
        assert_eq!(err.code(), "InvalidRequestException");
    }
}
