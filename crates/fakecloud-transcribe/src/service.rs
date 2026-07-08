//! Amazon Transcribe awsJson1_1 dispatch + operation handlers.
//!
//! Requests carry `X-Amz-Target: Transcribe.<Operation>`; dispatch keys off
//! `req.action`. Every operation runs model-driven input validation first
//! (required / length / range / enum / pattern), then real, account-partitioned,
//! persisted CRUD. Dereferencing a resource that does not exist returns
//! Transcribe's canonical `NotFoundException`; a duplicate name returns
//! `ConflictException`.

use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use serde_json::{json, Map, Value};
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::shared::{
    bucket_output_uri, download_uri, now_epoch, parse_resource_arn, resource_arn,
    service_managed_uri,
};
use crate::state::{SharedTranscribeState, TranscribeData};

/// Every operation name in the Amazon Transcribe Smithy model (43 operations).
pub const TRANSCRIBE_ACTIONS: &[&str] = &[
    "CreateCallAnalyticsCategory",
    "CreateLanguageModel",
    "CreateMedicalVocabulary",
    "CreateVocabulary",
    "CreateVocabularyFilter",
    "DeleteCallAnalyticsCategory",
    "DeleteCallAnalyticsJob",
    "DeleteLanguageModel",
    "DeleteMedicalScribeJob",
    "DeleteMedicalTranscriptionJob",
    "DeleteMedicalVocabulary",
    "DeleteTranscriptionJob",
    "DeleteVocabulary",
    "DeleteVocabularyFilter",
    "DescribeLanguageModel",
    "GetCallAnalyticsCategory",
    "GetCallAnalyticsJob",
    "GetMedicalScribeJob",
    "GetMedicalTranscriptionJob",
    "GetMedicalVocabulary",
    "GetTranscriptionJob",
    "GetVocabulary",
    "GetVocabularyFilter",
    "ListCallAnalyticsCategories",
    "ListCallAnalyticsJobs",
    "ListLanguageModels",
    "ListMedicalScribeJobs",
    "ListMedicalTranscriptionJobs",
    "ListMedicalVocabularies",
    "ListTagsForResource",
    "ListTranscriptionJobs",
    "ListVocabularies",
    "ListVocabularyFilters",
    "StartCallAnalyticsJob",
    "StartMedicalScribeJob",
    "StartMedicalTranscriptionJob",
    "StartTranscriptionJob",
    "TagResource",
    "UntagResource",
    "UpdateCallAnalyticsCategory",
    "UpdateMedicalVocabulary",
    "UpdateVocabulary",
    "UpdateVocabularyFilter",
];

/// Read-only verbs; any other action mutates persisted state and triggers a
/// snapshot after success. The inverse formulation guarantees no mutation is
/// ever missed if a new op is added.
fn is_mutating_action(action: &str) -> bool {
    const READ_PREFIXES: &[&str] = &["Get", "List", "Describe"];
    !READ_PREFIXES.iter().any(|p| action.starts_with(p))
}

pub struct TranscribeService {
    state: SharedTranscribeState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl TranscribeService {
    pub fn new(state: SharedTranscribeState) -> Self {
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
    fn with_account_mut<R>(&self, req: &AwsRequest, f: impl FnOnce(&mut TranscribeData) -> R) -> R {
        let mut guard = self.state.write();
        let acct = guard.get_or_create(&req.account_id);
        f(acct)
    }
}

#[async_trait]
impl AwsService for TranscribeService {
    fn service_name(&self) -> &str {
        "transcribe"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let action = req.action.clone();
        if let Err(msg) = crate::validate::validate_input(&action, &req.json_body()) {
            return Err(bad_request(msg));
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
        TRANSCRIBE_ACTIONS
    }
}

impl TranscribeService {
    fn dispatch(&self, action: &str, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        match action {
            // Transcription jobs
            "StartTranscriptionJob" => self.start_transcription_job(req, &body),
            "GetTranscriptionJob" => self.get_transcription_job(req, &body),
            "ListTranscriptionJobs" => self.list_transcription_jobs(req, &body),
            "DeleteTranscriptionJob" => self.delete_transcription_job(req, &body),
            // Medical transcription jobs
            "StartMedicalTranscriptionJob" => self.start_medical_transcription_job(req, &body),
            "GetMedicalTranscriptionJob" => self.get_medical_transcription_job(req, &body),
            "ListMedicalTranscriptionJobs" => self.list_medical_transcription_jobs(req, &body),
            "DeleteMedicalTranscriptionJob" => self.delete_medical_transcription_job(req, &body),
            // Call-analytics jobs
            "StartCallAnalyticsJob" => self.start_call_analytics_job(req, &body),
            "GetCallAnalyticsJob" => self.get_call_analytics_job(req, &body),
            "ListCallAnalyticsJobs" => self.list_call_analytics_jobs(req, &body),
            "DeleteCallAnalyticsJob" => self.delete_call_analytics_job(req, &body),
            // Call-analytics categories
            "CreateCallAnalyticsCategory" => self.create_call_analytics_category(req, &body),
            "GetCallAnalyticsCategory" => self.get_call_analytics_category(req, &body),
            "UpdateCallAnalyticsCategory" => self.update_call_analytics_category(req, &body),
            "ListCallAnalyticsCategories" => self.list_call_analytics_categories(req, &body),
            "DeleteCallAnalyticsCategory" => self.delete_call_analytics_category(req, &body),
            // Medical-scribe jobs
            "StartMedicalScribeJob" => self.start_medical_scribe_job(req, &body),
            "GetMedicalScribeJob" => self.get_medical_scribe_job(req, &body),
            "ListMedicalScribeJobs" => self.list_medical_scribe_jobs(req, &body),
            "DeleteMedicalScribeJob" => self.delete_medical_scribe_job(req, &body),
            // Vocabularies
            "CreateVocabulary" => self.create_vocabulary(req, &body),
            "GetVocabulary" => self.get_vocabulary(req, &body),
            "UpdateVocabulary" => self.update_vocabulary(req, &body),
            "ListVocabularies" => self.list_vocabularies(req, &body),
            "DeleteVocabulary" => self.delete_vocabulary(req, &body),
            // Medical vocabularies
            "CreateMedicalVocabulary" => self.create_medical_vocabulary(req, &body),
            "GetMedicalVocabulary" => self.get_medical_vocabulary(req, &body),
            "UpdateMedicalVocabulary" => self.update_medical_vocabulary(req, &body),
            "ListMedicalVocabularies" => self.list_medical_vocabularies(req, &body),
            "DeleteMedicalVocabulary" => self.delete_medical_vocabulary(req, &body),
            // Vocabulary filters
            "CreateVocabularyFilter" => self.create_vocabulary_filter(req, &body),
            "GetVocabularyFilter" => self.get_vocabulary_filter(req, &body),
            "UpdateVocabularyFilter" => self.update_vocabulary_filter(req, &body),
            "ListVocabularyFilters" => self.list_vocabulary_filters(req, &body),
            "DeleteVocabularyFilter" => self.delete_vocabulary_filter(req, &body),
            // Custom language models
            "CreateLanguageModel" => self.create_language_model(req, &body),
            "DescribeLanguageModel" => self.describe_language_model(req, &body),
            "ListLanguageModels" => self.list_language_models(req, &body),
            "DeleteLanguageModel" => self.delete_language_model(req, &body),
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

fn bad_request(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "BadRequestException", msg.into())
}

fn conflict(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::CONFLICT, "ConflictException", msg.into())
}

fn not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, "NotFoundException", msg.into())
}

fn ok(value: Value) -> Result<AwsResponse, AwsServiceError> {
    Ok(AwsResponse::ok_json(value))
}

// ---- small member helpers -------------------------------------------------

/// Copy each named member from `body` into `obj` when present and non-null.
fn copy_members(body: &Value, obj: &mut Map<String, Value>, names: &[&str]) {
    for name in names {
        if let Some(v) = body.get(*name).filter(|v| !v.is_null()) {
            obj.insert((*name).to_string(), v.clone());
        }
    }
}

fn str_member<'a>(body: &'a Value, name: &str) -> Option<&'a str> {
    body.get(name).and_then(Value::as_str)
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

impl TranscribeData {
    /// Record `Tags` from a create/start request under `arn`, if any.
    fn store_tags(&mut self, arn: &str, body: &Value) {
        if let Some(tags) = body.get("Tags").filter(|v| !v.is_null()) {
            let map = taglist_to_map(tags);
            if !map.is_empty() {
                self.tags.insert(arn.to_string(), map);
            }
        }
    }

    /// Splice the stored tags for `arn` into `obj` as a `Tags` member.
    fn splice_tags(&self, arn: &str, obj: &mut Map<String, Value>) {
        if let Some(map) = self.tags.get(arn) {
            if !map.is_empty() {
                obj.insert("Tags".into(), map_to_taglist(map));
            }
        }
    }

    /// True if the resource named by an ARN's `(type, name)` exists.
    fn resource_exists(&self, rtype: &str, name: &str) -> bool {
        match rtype {
            "transcription-job" => self.transcription_jobs.contains_key(name),
            "medical-transcription-job" => self.medical_transcription_jobs.contains_key(name),
            "call-analytics-job" => self.call_analytics_jobs.contains_key(name),
            "medical-scribe-job" => self.medical_scribe_jobs.contains_key(name),
            "vocabulary" => self.vocabularies.contains_key(name),
            "medical-vocabulary" => self.medical_vocabularies.contains_key(name),
            "vocabulary-filter" => self.vocabulary_filters.contains_key(name),
            "language-model" => self.language_models.contains_key(name),
            "call-analytics-category" => self.call_analytics_categories.contains_key(name),
            _ => false,
        }
    }
}

/// Copy the named summary fields out of a stored job object into a new map.
fn summary_from(job: &Value, fields: &[&str]) -> Value {
    let mut out = Map::new();
    if let Some(obj) = job.as_object() {
        for f in fields {
            if let Some(v) = obj.get(*f) {
                out.insert((*f).to_string(), v.clone());
            }
        }
    }
    Value::Object(out)
}

/// Apply `NameContains` (case-sensitive substring) + `MaxResults` list paging
/// primitives shared by the `List*` operations. Returns matching values.
fn filter_map<'a>(
    map: &'a std::collections::BTreeMap<String, Value>,
    contains: Option<&str>,
    status_field: Option<(&str, &str)>,
) -> Vec<&'a Value> {
    map.iter()
        .filter(|(name, v)| {
            contains.map(|c| name.contains(c)).unwrap_or(true)
                && status_field
                    .map(|(field, want)| v.get(field).and_then(Value::as_str) == Some(want))
                    .unwrap_or(true)
        })
        .map(|(_, v)| v)
        .collect()
}

// ---- transcription jobs ---------------------------------------------------

impl TranscribeService {
    fn start_transcription_job(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "TranscriptionJobName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            if d.transcription_jobs.contains_key(&name) {
                return Err(conflict(
                    "The requested job name already exists. Use a different job name.",
                ));
            }
            let now = now_epoch();
            let mut obj = Map::new();
            obj.insert("TranscriptionJobName".into(), json!(name));
            obj.insert("TranscriptionJobStatus".into(), json!("QUEUED"));
            obj.insert("StartTime".into(), json!(now));
            obj.insert("CreationTime".into(), json!(now));
            copy_members(
                body,
                &mut obj,
                &[
                    "LanguageCode",
                    "MediaSampleRateHertz",
                    "MediaFormat",
                    "Media",
                    "Settings",
                    "ModelSettings",
                    "JobExecutionSettings",
                    "ContentRedaction",
                    "IdentifyLanguage",
                    "IdentifyMultipleLanguages",
                    "LanguageOptions",
                    "LanguageIdSettings",
                    "ToxicityDetection",
                ],
            );

            // Output-file URI + location type (input-only members kept out of
            // the stored wire object; the transcript is spliced in on settle).
            let (uri, loc_type) = transcript_uri(body, &region, &account, &name, "asrOutput.json");
            let mut out = Map::new();
            out.insert("uri".into(), json!(uri));
            out.insert("outputLocationType".into(), json!(loc_type));
            if body.get("ContentRedaction").is_some() {
                let (r, _) = transcript_uri(body, &region, &account, &name, "redacted.json");
                out.insert("redacted".into(), json!(r));
            }
            if let Some(subs) = body.get("Subtitles").filter(|v| !v.is_null()) {
                let formats = subs.get("Formats").cloned().unwrap_or(json!([]));
                let start_index = subs.get("OutputStartIndex").cloned().unwrap_or(json!(0));
                let uris: Vec<Value> = formats
                    .as_array()
                    .map(|fs| {
                        fs.iter()
                            .filter_map(Value::as_str)
                            .map(|f| {
                                let (u, _) = transcript_uri(
                                    body,
                                    &region,
                                    &account,
                                    &name,
                                    &format!("subtitles.{f}"),
                                );
                                json!(u)
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                out.insert(
                    "subtitles".into(),
                    json!({
                        "Formats": formats,
                        "SubtitleFileUris": uris,
                        "OutputStartIndex": start_index,
                    }),
                );
            }
            d.job_outputs
                .insert(format!("transcription/{name}"), Value::Object(out));

            d.transcription_jobs
                .insert(name.clone(), Value::Object(obj.clone()));
            let arn = resource_arn(&region, &account, "transcription-job", &name);
            d.store_tags(&arn, body);
            d.splice_tags(&arn, &mut obj);
            ok(json!({ "TranscriptionJob": Value::Object(obj) }))
        })
    }

    fn get_transcription_job(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "TranscriptionJobName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            d.reconcile();
            let Some(job) = d.transcription_jobs.get(&name).cloned() else {
                return Err(not_found(
                    "The requested job couldn't be found. Check the job name and try your request again.",
                ));
            };
            let mut obj = job.as_object().cloned().unwrap_or_default();
            let arn = resource_arn(&region, &account, "transcription-job", &name);
            d.splice_tags(&arn, &mut obj);
            ok(json!({ "TranscriptionJob": Value::Object(obj) }))
        })
    }

    fn list_transcription_jobs(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_account_mut(req, |d| {
            d.reconcile();
            let status = str_member(body, "Status");
            let contains = str_member(body, "JobNameContains");
            let jobs = filter_map(
                &d.transcription_jobs,
                contains,
                status.map(|s| ("TranscriptionJobStatus", s)),
            );
            let summaries: Vec<Value> = jobs
                .iter()
                .map(|j| {
                    let mut s = summary_from(
                        j,
                        &[
                            "TranscriptionJobName",
                            "CreationTime",
                            "StartTime",
                            "CompletionTime",
                            "LanguageCode",
                            "TranscriptionJobStatus",
                            "FailureReason",
                            "ContentRedaction",
                            "ModelSettings",
                            "IdentifyLanguage",
                            "IdentifyMultipleLanguages",
                            "IdentifiedLanguageScore",
                            "LanguageCodes",
                            "ToxicityDetection",
                        ],
                    );
                    if let (Some(name), Some(obj)) = (
                        j.get("TranscriptionJobName").and_then(Value::as_str),
                        s.as_object_mut(),
                    ) {
                        let loc = d
                            .job_outputs
                            .get(&format!("transcription/{name}"))
                            .and_then(|o| o.get("outputLocationType"))
                            .cloned();
                        if let Some(l) = loc {
                            obj.insert("OutputLocationType".into(), l);
                        }
                    }
                    s
                })
                .collect();
            ok(json!({ "TranscriptionJobSummaries": summaries }))
        })
    }

    fn delete_transcription_job(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "TranscriptionJobName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        // NotFoundException is not declared for this op; deletion is idempotent.
        self.with_account_mut(req, |d| {
            d.transcription_jobs.remove(&name);
            d.job_outputs.remove(&format!("transcription/{name}"));
            d.tags
                .remove(&resource_arn(&region, &account, "transcription-job", &name));
        });
        ok(json!({}))
    }
}

/// Resolve a job's transcript-output URI + `OutputLocationType` from its
/// output-bucket inputs.
fn transcript_uri(
    body: &Value,
    region: &str,
    account: &str,
    name: &str,
    default_file: &str,
) -> (String, &'static str) {
    if let Some(bucket) = str_member(body, "OutputBucketName").filter(|s| !s.is_empty()) {
        let key = str_member(body, "OutputKey")
            .map(String::from)
            .unwrap_or_else(|| format!("{name}.json"));
        (bucket_output_uri(region, bucket, &key), "CUSTOMER_BUCKET")
    } else if let Some(loc) = str_member(body, "OutputLocation").filter(|s| !s.is_empty()) {
        // Call-analytics carries a full `OutputLocation` URI.
        (loc.to_string(), "CUSTOMER_BUCKET")
    } else {
        (
            service_managed_uri(region, account, name, default_file),
            "SERVICE_BUCKET",
        )
    }
}

// ---- medical transcription jobs -------------------------------------------

impl TranscribeService {
    fn start_medical_transcription_job(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "MedicalTranscriptionJobName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            if d.medical_transcription_jobs.contains_key(&name) {
                return Err(conflict(
                    "The requested job name already exists. Use a different job name.",
                ));
            }
            let now = now_epoch();
            let mut obj = Map::new();
            obj.insert("MedicalTranscriptionJobName".into(), json!(name));
            obj.insert("TranscriptionJobStatus".into(), json!("QUEUED"));
            obj.insert("StartTime".into(), json!(now));
            obj.insert("CreationTime".into(), json!(now));
            copy_members(
                body,
                &mut obj,
                &[
                    "LanguageCode",
                    "MediaSampleRateHertz",
                    "MediaFormat",
                    "Media",
                    "Settings",
                    "ContentIdentificationType",
                    "Specialty",
                    "Type",
                ],
            );
            let (uri, loc_type) = transcript_uri(body, &region, &account, &name, "medical.json");
            d.job_outputs.insert(
                format!("medical/{name}"),
                json!({ "uri": uri, "outputLocationType": loc_type }),
            );
            d.medical_transcription_jobs
                .insert(name.clone(), Value::Object(obj.clone()));
            let arn = resource_arn(&region, &account, "medical-transcription-job", &name);
            d.store_tags(&arn, body);
            d.splice_tags(&arn, &mut obj);
            ok(json!({ "MedicalTranscriptionJob": Value::Object(obj) }))
        })
    }

    fn get_medical_transcription_job(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "MedicalTranscriptionJobName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            d.reconcile();
            let Some(job) = d.medical_transcription_jobs.get(&name).cloned() else {
                return Err(not_found("The requested job couldn't be found. Check the job name and try your request again."));
            };
            let mut obj = job.as_object().cloned().unwrap_or_default();
            let arn = resource_arn(&region, &account, "medical-transcription-job", &name);
            d.splice_tags(&arn, &mut obj);
            ok(json!({ "MedicalTranscriptionJob": Value::Object(obj) }))
        })
    }

    fn list_medical_transcription_jobs(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_account_mut(req, |d| {
            d.reconcile();
            let status = str_member(body, "Status");
            let contains = str_member(body, "JobNameContains");
            let jobs = filter_map(
                &d.medical_transcription_jobs,
                contains,
                status.map(|s| ("TranscriptionJobStatus", s)),
            );
            let summaries: Vec<Value> = jobs
                .iter()
                .map(|j| {
                    summary_from(
                        j,
                        &[
                            "MedicalTranscriptionJobName",
                            "CreationTime",
                            "StartTime",
                            "CompletionTime",
                            "LanguageCode",
                            "TranscriptionJobStatus",
                            "FailureReason",
                            "Specialty",
                            "ContentIdentificationType",
                            "Type",
                        ],
                    )
                })
                .collect();
            ok(json!({ "MedicalTranscriptionJobSummaries": summaries }))
        })
    }

    fn delete_medical_transcription_job(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "MedicalTranscriptionJobName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            d.medical_transcription_jobs.remove(&name);
            d.job_outputs.remove(&format!("medical/{name}"));
            d.tags.remove(&resource_arn(
                &region,
                &account,
                "medical-transcription-job",
                &name,
            ));
        });
        ok(json!({}))
    }
}

// ---- call-analytics jobs --------------------------------------------------

impl TranscribeService {
    fn start_call_analytics_job(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "CallAnalyticsJobName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            if d.call_analytics_jobs.contains_key(&name) {
                return Err(conflict(
                    "The requested job name already exists. Use a different job name.",
                ));
            }
            let now = now_epoch();
            let mut obj = Map::new();
            obj.insert("CallAnalyticsJobName".into(), json!(name));
            obj.insert("CallAnalyticsJobStatus".into(), json!("QUEUED"));
            obj.insert("StartTime".into(), json!(now));
            obj.insert("CreationTime".into(), json!(now));
            copy_members(
                body,
                &mut obj,
                &[
                    "Media",
                    "Settings",
                    "DataAccessRoleArn",
                    "ChannelDefinitions",
                ],
            );
            let (uri, loc_type) = transcript_uri(body, &region, &account, &name, "analytics.json");
            d.job_outputs.insert(
                format!("call/{name}"),
                json!({ "uri": uri, "outputLocationType": loc_type }),
            );
            d.call_analytics_jobs
                .insert(name.clone(), Value::Object(obj.clone()));
            let arn = resource_arn(&region, &account, "call-analytics-job", &name);
            d.store_tags(&arn, body);
            d.splice_tags(&arn, &mut obj);
            ok(json!({ "CallAnalyticsJob": Value::Object(obj) }))
        })
    }

    fn get_call_analytics_job(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "CallAnalyticsJobName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            d.reconcile();
            let Some(job) = d.call_analytics_jobs.get(&name).cloned() else {
                return Err(not_found("The requested job couldn't be found. Check the job name and try your request again."));
            };
            let mut obj = job.as_object().cloned().unwrap_or_default();
            let arn = resource_arn(&region, &account, "call-analytics-job", &name);
            d.splice_tags(&arn, &mut obj);
            ok(json!({ "CallAnalyticsJob": Value::Object(obj) }))
        })
    }

    fn list_call_analytics_jobs(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_account_mut(req, |d| {
            d.reconcile();
            let status = str_member(body, "Status");
            let contains = str_member(body, "JobNameContains");
            let jobs = filter_map(
                &d.call_analytics_jobs,
                contains,
                status.map(|s| ("CallAnalyticsJobStatus", s)),
            );
            let summaries: Vec<Value> = jobs
                .iter()
                .map(|j| {
                    summary_from(
                        j,
                        &[
                            "CallAnalyticsJobName",
                            "CreationTime",
                            "StartTime",
                            "CompletionTime",
                            "LanguageCode",
                            "CallAnalyticsJobStatus",
                            "CallAnalyticsJobDetails",
                            "FailureReason",
                        ],
                    )
                })
                .collect();
            ok(json!({ "CallAnalyticsJobSummaries": summaries }))
        })
    }

    fn delete_call_analytics_job(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "CallAnalyticsJobName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            d.call_analytics_jobs.remove(&name);
            d.job_outputs.remove(&format!("call/{name}"));
            d.tags.remove(&resource_arn(
                &region,
                &account,
                "call-analytics-job",
                &name,
            ));
        });
        ok(json!({}))
    }
}

// ---- call-analytics categories --------------------------------------------

impl TranscribeService {
    fn build_category(body: &Value, now: f64) -> Map<String, Value> {
        let mut obj = Map::new();
        copy_members(body, &mut obj, &["CategoryName", "Rules", "InputType"]);
        obj.insert("CreateTime".into(), json!(now));
        obj.insert("LastUpdateTime".into(), json!(now));
        obj
    }

    fn create_call_analytics_category(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "CategoryName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            if d.call_analytics_categories.contains_key(&name) {
                return Err(conflict(
                    "The category name already exists. Use a different category name.",
                ));
            }
            let now = now_epoch();
            let mut obj = Self::build_category(body, now);
            d.call_analytics_categories
                .insert(name.clone(), Value::Object(obj.clone()));
            let arn = resource_arn(&region, &account, "call-analytics-category", &name);
            d.store_tags(&arn, body);
            d.splice_tags(&arn, &mut obj);
            ok(json!({ "CategoryProperties": Value::Object(obj) }))
        })
    }

    fn get_call_analytics_category(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "CategoryName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            let Some(cat) = d.call_analytics_categories.get(&name).cloned() else {
                return Err(not_found("The requested category couldn't be found. Check the category name and try your request again."));
            };
            let mut obj = cat.as_object().cloned().unwrap_or_default();
            let arn = resource_arn(&region, &account, "call-analytics-category", &name);
            d.splice_tags(&arn, &mut obj);
            ok(json!({ "CategoryProperties": Value::Object(obj) }))
        })
    }

    fn update_call_analytics_category(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "CategoryName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            let Some(existing) = d.call_analytics_categories.get(&name).cloned() else {
                return Err(not_found("The requested category couldn't be found. Check the category name and try your request again."));
            };
            let create_time = existing.get("CreateTime").cloned().unwrap_or(json!(now_epoch()));
            let now = now_epoch();
            let mut obj = Map::new();
            obj.insert("CategoryName".into(), json!(name));
            copy_members(body, &mut obj, &["Rules", "InputType"]);
            obj.insert("CreateTime".into(), create_time);
            obj.insert("LastUpdateTime".into(), json!(now));
            d.call_analytics_categories.insert(name.clone(), Value::Object(obj.clone()));
            let arn = resource_arn(&region, &account, "call-analytics-category", &name);
            d.splice_tags(&arn, &mut obj);
            ok(json!({ "CategoryProperties": Value::Object(obj) }))
        })
    }

    fn list_call_analytics_categories(
        &self,
        req: &AwsRequest,
        _body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_account_mut(req, |d| {
            let cats: Vec<Value> = d.call_analytics_categories.values().cloned().collect();
            ok(json!({ "Categories": cats }))
        })
    }

    fn delete_call_analytics_category(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "CategoryName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            if d.call_analytics_categories.remove(&name).is_none() {
                return Err(not_found("The requested category couldn't be found. Check the category name and try your request again."));
            }
            d.tags
                .remove(&resource_arn(&region, &account, "call-analytics-category", &name));
            ok(json!({}))
        })
    }
}

// ---- medical-scribe jobs --------------------------------------------------

impl TranscribeService {
    fn start_medical_scribe_job(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "MedicalScribeJobName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            if d.medical_scribe_jobs.contains_key(&name) {
                return Err(conflict(
                    "The requested job name already exists. Use a different job name.",
                ));
            }
            let now = now_epoch();
            let mut obj = Map::new();
            obj.insert("MedicalScribeJobName".into(), json!(name));
            obj.insert("MedicalScribeJobStatus".into(), json!("QUEUED"));
            obj.insert("LanguageCode".into(), json!("en-US"));
            obj.insert("StartTime".into(), json!(now));
            obj.insert("CreationTime".into(), json!(now));
            obj.insert(
                "MedicalScribeContextProvided".into(),
                json!(body
                    .get("MedicalScribeContext")
                    .filter(|v| !v.is_null())
                    .is_some()),
            );
            copy_members(
                body,
                &mut obj,
                &[
                    "Media",
                    "Settings",
                    "DataAccessRoleArn",
                    "ChannelDefinitions",
                ],
            );
            // Output lives under the requested bucket as two files.
            let bucket = str_member(body, "OutputBucketName").unwrap_or_default();
            let (t_uri, _) = if bucket.is_empty() {
                (
                    service_managed_uri(&region, &account, &name, "transcript.json"),
                    "",
                )
            } else {
                (
                    bucket_output_uri(&region, bucket, &format!("{name}/transcript.json")),
                    "",
                )
            };
            let (c_uri, _) = if bucket.is_empty() {
                (
                    service_managed_uri(&region, &account, &name, "summary.json"),
                    "",
                )
            } else {
                (
                    bucket_output_uri(&region, bucket, &format!("{name}/summary.json")),
                    "",
                )
            };
            d.job_outputs.insert(
                format!("scribe/{name}"),
                json!({ "uri": t_uri, "clinical": c_uri }),
            );
            d.medical_scribe_jobs
                .insert(name.clone(), Value::Object(obj.clone()));
            let arn = resource_arn(&region, &account, "medical-scribe-job", &name);
            d.store_tags(&arn, body);
            d.splice_tags(&arn, &mut obj);
            ok(json!({ "MedicalScribeJob": Value::Object(obj) }))
        })
    }

    fn get_medical_scribe_job(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "MedicalScribeJobName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            d.reconcile();
            let Some(job) = d.medical_scribe_jobs.get(&name).cloned() else {
                return Err(not_found("The requested job couldn't be found. Check the job name and try your request again."));
            };
            let mut obj = job.as_object().cloned().unwrap_or_default();
            let arn = resource_arn(&region, &account, "medical-scribe-job", &name);
            d.splice_tags(&arn, &mut obj);
            ok(json!({ "MedicalScribeJob": Value::Object(obj) }))
        })
    }

    fn list_medical_scribe_jobs(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_account_mut(req, |d| {
            d.reconcile();
            let status = str_member(body, "Status");
            let contains = str_member(body, "JobNameContains");
            let jobs = filter_map(
                &d.medical_scribe_jobs,
                contains,
                status.map(|s| ("MedicalScribeJobStatus", s)),
            );
            let summaries: Vec<Value> = jobs
                .iter()
                .map(|j| {
                    summary_from(
                        j,
                        &[
                            "MedicalScribeJobName",
                            "CreationTime",
                            "StartTime",
                            "CompletionTime",
                            "LanguageCode",
                            "MedicalScribeJobStatus",
                            "FailureReason",
                        ],
                    )
                })
                .collect();
            ok(json!({ "MedicalScribeJobSummaries": summaries }))
        })
    }

    fn delete_medical_scribe_job(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "MedicalScribeJobName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            d.medical_scribe_jobs.remove(&name);
            d.job_outputs.remove(&format!("scribe/{name}"));
            d.tags.remove(&resource_arn(
                &region,
                &account,
                "medical-scribe-job",
                &name,
            ));
        });
        ok(json!({}))
    }
}

// ---- vocabularies ---------------------------------------------------------

impl TranscribeService {
    fn create_vocabulary(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "VocabularyName")
            .unwrap_or_default()
            .to_string();
        let lang = str_member(body, "LanguageCode")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            if d.vocabularies.contains_key(&name) {
                return Err(conflict("A vocabulary with the same name already exists. Use a different name and try your request again."));
            }
            let now = now_epoch();
            let dl = download_uri(&region, &account, "vocabulary", &name);
            let obj = json!({
                "VocabularyName": name,
                "LanguageCode": lang,
                "VocabularyState": "PENDING",
                "LastModifiedTime": now,
                "DownloadUri": dl,
            });
            d.vocabularies.insert(name.clone(), obj.clone());
            let arn = resource_arn(&region, &account, "vocabulary", &name);
            d.store_tags(&arn, body);
            ok(json!({
                "VocabularyName": obj["VocabularyName"],
                "LanguageCode": obj["LanguageCode"],
                "VocabularyState": obj["VocabularyState"],
                "LastModifiedTime": obj["LastModifiedTime"],
            }))
        })
    }

    fn get_vocabulary(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "VocabularyName")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            d.reconcile();
            let Some(v) = d.vocabularies.get(&name).cloned() else {
                return Err(not_found("The requested vocabulary couldn't be found. Check the vocabulary name and try your request again."));
            };
            ok(v)
        })
    }

    fn update_vocabulary(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "VocabularyName")
            .unwrap_or_default()
            .to_string();
        let lang = str_member(body, "LanguageCode")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            if !d.vocabularies.contains_key(&name) {
                return Err(not_found("The requested vocabulary couldn't be found. Check the vocabulary name and try your request again."));
            }
            let now = now_epoch();
            let dl = download_uri(&region, &account, "vocabulary", &name);
            let obj = json!({
                "VocabularyName": name,
                "LanguageCode": lang,
                "VocabularyState": "PENDING",
                "LastModifiedTime": now,
                "DownloadUri": dl,
            });
            d.vocabularies.insert(name.clone(), obj);
            ok(json!({
                "VocabularyName": name,
                "LanguageCode": lang,
                "LastModifiedTime": now,
                "VocabularyState": "PENDING",
            }))
        })
    }

    fn list_vocabularies(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_account_mut(req, |d| {
            d.reconcile();
            let state = str_member(body, "StateEquals");
            let contains = str_member(body, "NameContains");
            let vocabs = filter_map(
                &d.vocabularies,
                contains,
                state.map(|s| ("VocabularyState", s)),
            );
            let infos: Vec<Value> = vocabs
                .iter()
                .map(|v| {
                    summary_from(
                        v,
                        &[
                            "VocabularyName",
                            "LanguageCode",
                            "LastModifiedTime",
                            "VocabularyState",
                        ],
                    )
                })
                .collect();
            ok(json!({ "Vocabularies": infos }))
        })
    }

    fn delete_vocabulary(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "VocabularyName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            if d.vocabularies.remove(&name).is_none() {
                return Err(not_found("The requested vocabulary couldn't be found. Check the vocabulary name and try your request again."));
            }
            d.tags.remove(&resource_arn(&region, &account, "vocabulary", &name));
            ok(json!({}))
        })
    }
}

// ---- medical vocabularies -------------------------------------------------

impl TranscribeService {
    fn create_medical_vocabulary(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "VocabularyName")
            .unwrap_or_default()
            .to_string();
        let lang = str_member(body, "LanguageCode")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            if d.medical_vocabularies.contains_key(&name) {
                return Err(conflict("A vocabulary with the same name already exists. Use a different name and try your request again."));
            }
            let now = now_epoch();
            let dl = download_uri(&region, &account, "medical-vocabulary", &name);
            let obj = json!({
                "VocabularyName": name,
                "LanguageCode": lang,
                "VocabularyState": "PENDING",
                "LastModifiedTime": now,
                "DownloadUri": dl,
            });
            d.medical_vocabularies.insert(name.clone(), obj);
            let arn = resource_arn(&region, &account, "medical-vocabulary", &name);
            d.store_tags(&arn, body);
            ok(json!({
                "VocabularyName": name,
                "LanguageCode": lang,
                "VocabularyState": "PENDING",
                "LastModifiedTime": now,
            }))
        })
    }

    fn get_medical_vocabulary(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "VocabularyName")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            d.reconcile();
            let Some(v) = d.medical_vocabularies.get(&name).cloned() else {
                return Err(not_found("The requested vocabulary couldn't be found. Check the vocabulary name and try your request again."));
            };
            ok(v)
        })
    }

    fn update_medical_vocabulary(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "VocabularyName")
            .unwrap_or_default()
            .to_string();
        let lang = str_member(body, "LanguageCode")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            if !d.medical_vocabularies.contains_key(&name) {
                return Err(not_found("The requested vocabulary couldn't be found. Check the vocabulary name and try your request again."));
            }
            let now = now_epoch();
            let dl = download_uri(&region, &account, "medical-vocabulary", &name);
            let obj = json!({
                "VocabularyName": name,
                "LanguageCode": lang,
                "VocabularyState": "PENDING",
                "LastModifiedTime": now,
                "DownloadUri": dl,
            });
            d.medical_vocabularies.insert(name.clone(), obj);
            ok(json!({
                "VocabularyName": name,
                "LanguageCode": lang,
                "LastModifiedTime": now,
                "VocabularyState": "PENDING",
            }))
        })
    }

    fn list_medical_vocabularies(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_account_mut(req, |d| {
            d.reconcile();
            let state = str_member(body, "StateEquals");
            let contains = str_member(body, "NameContains");
            let vocabs = filter_map(
                &d.medical_vocabularies,
                contains,
                state.map(|s| ("VocabularyState", s)),
            );
            let infos: Vec<Value> = vocabs
                .iter()
                .map(|v| {
                    summary_from(
                        v,
                        &[
                            "VocabularyName",
                            "LanguageCode",
                            "LastModifiedTime",
                            "VocabularyState",
                        ],
                    )
                })
                .collect();
            ok(json!({ "Vocabularies": infos }))
        })
    }

    fn delete_medical_vocabulary(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "VocabularyName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            if d.medical_vocabularies.remove(&name).is_none() {
                return Err(not_found("The requested vocabulary couldn't be found. Check the vocabulary name and try your request again."));
            }
            d.tags
                .remove(&resource_arn(&region, &account, "medical-vocabulary", &name));
            ok(json!({}))
        })
    }
}

// ---- vocabulary filters ---------------------------------------------------

impl TranscribeService {
    fn create_vocabulary_filter(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "VocabularyFilterName")
            .unwrap_or_default()
            .to_string();
        let lang = str_member(body, "LanguageCode")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            if d.vocabulary_filters.contains_key(&name) {
                return Err(conflict("A vocabulary filter with the same name already exists. Use a different name and try your request again."));
            }
            let now = now_epoch();
            let dl = download_uri(&region, &account, "vocabulary-filter", &name);
            let obj = json!({
                "VocabularyFilterName": name,
                "LanguageCode": lang,
                "LastModifiedTime": now,
                "DownloadUri": dl,
            });
            d.vocabulary_filters.insert(name.clone(), obj);
            let arn = resource_arn(&region, &account, "vocabulary-filter", &name);
            d.store_tags(&arn, body);
            ok(json!({
                "VocabularyFilterName": name,
                "LanguageCode": lang,
                "LastModifiedTime": now,
            }))
        })
    }

    fn get_vocabulary_filter(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "VocabularyFilterName")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            let Some(v) = d.vocabulary_filters.get(&name).cloned() else {
                return Err(not_found("The requested vocabulary filter couldn't be found. Check the vocabulary filter name and try your request again."));
            };
            ok(v)
        })
    }

    fn update_vocabulary_filter(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "VocabularyFilterName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            let Some(existing) = d.vocabulary_filters.get(&name).cloned() else {
                return Err(not_found("The requested vocabulary filter couldn't be found. Check the vocabulary filter name and try your request again."));
            };
            let lang = existing.get("LanguageCode").cloned().unwrap_or(Value::Null);
            let now = now_epoch();
            let dl = download_uri(&region, &account, "vocabulary-filter", &name);
            let obj = json!({
                "VocabularyFilterName": name,
                "LanguageCode": lang,
                "LastModifiedTime": now,
                "DownloadUri": dl,
            });
            d.vocabulary_filters.insert(name.clone(), obj.clone());
            ok(json!({
                "VocabularyFilterName": name,
                "LanguageCode": obj["LanguageCode"],
                "LastModifiedTime": now,
            }))
        })
    }

    fn list_vocabulary_filters(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_account_mut(req, |d| {
            let contains = str_member(body, "NameContains");
            let filters = filter_map(&d.vocabulary_filters, contains, None);
            let infos: Vec<Value> = filters
                .iter()
                .map(|v| {
                    summary_from(
                        v,
                        &["VocabularyFilterName", "LanguageCode", "LastModifiedTime"],
                    )
                })
                .collect();
            ok(json!({ "VocabularyFilters": infos }))
        })
    }

    fn delete_vocabulary_filter(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "VocabularyFilterName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            if d.vocabulary_filters.remove(&name).is_none() {
                return Err(not_found("The requested vocabulary filter couldn't be found. Check the vocabulary filter name and try your request again."));
            }
            d.tags
                .remove(&resource_arn(&region, &account, "vocabulary-filter", &name));
            ok(json!({}))
        })
    }
}

// ---- custom language models -----------------------------------------------

impl TranscribeService {
    fn create_language_model(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "ModelName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        self.with_account_mut(req, |d| {
            if d.language_models.contains_key(&name) {
                return Err(conflict("A model with the same name already exists. Use a different name and try your request again."));
            }
            let now = now_epoch();
            let mut model = Map::new();
            model.insert("ModelName".into(), json!(name));
            model.insert("CreateTime".into(), json!(now));
            model.insert("LastModifiedTime".into(), json!(now));
            model.insert("ModelStatus".into(), json!("IN_PROGRESS"));
            model.insert("UpgradeAvailability".into(), json!(false));
            copy_members(body, &mut model, &["LanguageCode", "BaseModelName", "InputDataConfig"]);
            d.language_models.insert(name.clone(), Value::Object(model.clone()));
            let arn = resource_arn(&region, &account, "language-model", &name);
            d.store_tags(&arn, body);
            ok(json!({
                "LanguageCode": model.get("LanguageCode").cloned().unwrap_or(Value::Null),
                "BaseModelName": model.get("BaseModelName").cloned().unwrap_or(Value::Null),
                "ModelName": name,
                "InputDataConfig": model.get("InputDataConfig").cloned().unwrap_or(Value::Null),
                "ModelStatus": "IN_PROGRESS",
            }))
        })
    }

    fn describe_language_model(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "ModelName")
            .unwrap_or_default()
            .to_string();
        self.with_account_mut(req, |d| {
            d.reconcile();
            let Some(model) = d.language_models.get(&name).cloned() else {
                return Err(not_found("The requested model couldn't be found. Check the model name and try your request again."));
            };
            ok(json!({ "LanguageModel": model }))
        })
    }

    fn list_language_models(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.with_account_mut(req, |d| {
            d.reconcile();
            let status = str_member(body, "StatusEquals");
            let contains = str_member(body, "NameContains");
            let models = filter_map(
                &d.language_models,
                contains,
                status.map(|s| ("ModelStatus", s)),
            );
            let out: Vec<Value> = models.into_iter().cloned().collect();
            ok(json!({ "Models": out }))
        })
    }

    fn delete_language_model(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = str_member(body, "ModelName")
            .unwrap_or_default()
            .to_string();
        let region = req.region.clone();
        let account = req.account_id.clone();
        // NotFoundException is not declared for this op; deletion is idempotent.
        self.with_account_mut(req, |d| {
            d.language_models.remove(&name);
            d.tags
                .remove(&resource_arn(&region, &account, "language-model", &name));
        });
        ok(json!({}))
    }
}

// ---- tagging --------------------------------------------------------------

impl TranscribeService {
    fn tag_resource(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let arn = str_member(body, "ResourceArn")
            .unwrap_or_default()
            .to_string();
        let Some((rtype, name)) = parse_resource_arn(&arn) else {
            return Err(bad_request(format!("The resource ARN is not valid: {arn}")));
        };
        self.with_account_mut(req, |d| {
            if !d.resource_exists(&rtype, &name) {
                return Err(not_found(format!(
                    "The requested resource couldn't be found. Check that the specified resource exists: {arn}"
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
            return Err(bad_request(format!("The resource ARN is not valid: {arn}")));
        };
        self.with_account_mut(req, |d| {
            if !d.resource_exists(&rtype, &name) {
                return Err(not_found(format!(
                    "The requested resource couldn't be found. Check that the specified resource exists: {arn}"
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
            return Err(bad_request(format!("The resource ARN is not valid: {arn}")));
        };
        self.with_account_mut(req, |d| {
            if !d.resource_exists(&rtype, &name) {
                return Err(not_found(format!(
                    "The requested resource couldn't be found. Check that the specified resource exists: {arn}"
                )));
            }
            let tags = d.tags.get(&arn).map(map_to_taglist).unwrap_or(json!([]));
            ok(json!({ "ResourceArn": arn, "Tags": tags }))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn service() -> TranscribeService {
        TranscribeService::new(Arc::new(RwLock::new(MultiAccountState::new(
            "000000000000",
            "us-east-1",
            "",
        ))))
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "transcribe".to_string(),
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

    #[tokio::test]
    async fn transcription_job_round_trip() {
        let svc = service();
        let start = svc
            .handle(req(
                "StartTranscriptionJob",
                json!({
                    "TranscriptionJobName": "job1",
                    "LanguageCode": "en-US",
                    "Media": { "MediaFileUri": "s3://bucket/audio.wav" },
                    "OutputBucketName": "out-bucket"
                }),
            ))
            .await
            .unwrap();
        assert!(start.status.is_success());

        // Duplicate name -> ConflictException.
        let dup = svc
            .handle(req(
                "StartTranscriptionJob",
                json!({
                    "TranscriptionJobName": "job1",
                    "Media": { "MediaFileUri": "s3://bucket/audio.wav" }
                }),
            ))
            .await;
        assert_eq!(dup.err().unwrap().code(), "ConflictException");

        // Get settles to COMPLETED with a Transcript.
        let got = svc
            .handle(req(
                "GetTranscriptionJob",
                json!({ "TranscriptionJobName": "job1" }),
            ))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(got.body.expect_bytes()).unwrap();
        assert_eq!(v["TranscriptionJob"]["TranscriptionJobStatus"], "COMPLETED");
        assert!(v["TranscriptionJob"]["Transcript"]["TranscriptFileUri"]
            .as_str()
            .unwrap()
            .contains("out-bucket"));

        // List shows it.
        let listed = svc
            .handle(req("ListTranscriptionJobs", json!({})))
            .await
            .unwrap();
        let lv: Value = serde_json::from_slice(listed.body.expect_bytes()).unwrap();
        assert_eq!(lv["TranscriptionJobSummaries"].as_array().unwrap().len(), 1);

        // Missing job -> NotFoundException.
        let missing = svc
            .handle(req(
                "GetTranscriptionJob",
                json!({ "TranscriptionJobName": "nope" }),
            ))
            .await;
        assert_eq!(missing.err().unwrap().code(), "NotFoundException");
    }

    #[tokio::test]
    async fn vocabulary_and_tagging_round_trip() {
        let svc = service();
        svc.handle(req(
            "CreateVocabulary",
            json!({ "VocabularyName": "v1", "LanguageCode": "en-US", "Phrases": ["hello"] }),
        ))
        .await
        .unwrap();

        let got = svc
            .handle(req("GetVocabulary", json!({ "VocabularyName": "v1" })))
            .await
            .unwrap();
        let v: Value = serde_json::from_slice(got.body.expect_bytes()).unwrap();
        assert_eq!(v["VocabularyState"], "READY");

        let arn = "arn:aws:transcribe:us-east-1:000000000000:vocabulary/v1";
        svc.handle(req(
            "TagResource",
            json!({ "ResourceArn": arn, "Tags": [{ "Key": "team", "Value": "asr" }] }),
        ))
        .await
        .unwrap();
        let tags = svc
            .handle(req("ListTagsForResource", json!({ "ResourceArn": arn })))
            .await
            .unwrap();
        let tv: Value = serde_json::from_slice(tags.body.expect_bytes()).unwrap();
        assert_eq!(tv["Tags"][0]["Key"], "team");

        // Tagging a nonexistent resource -> NotFoundException.
        let bad = svc
            .handle(req(
                "ListTagsForResource",
                json!({ "ResourceArn": "arn:aws:transcribe:us-east-1:000000000000:vocabulary/nope" }),
            ))
            .await;
        assert_eq!(bad.err().unwrap().code(), "NotFoundException");

        svc.handle(req("DeleteVocabulary", json!({ "VocabularyName": "v1" })))
            .await
            .unwrap();
        let after = svc
            .handle(req("GetVocabulary", json!({ "VocabularyName": "v1" })))
            .await;
        assert_eq!(after.err().unwrap().code(), "NotFoundException");
    }
}
