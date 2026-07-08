//! Account-partitioned, serializable state for Amazon Transcribe (`transcribe`).
//!
//! Every resource is stored as its already-output-valid wire JSON object
//! (`serde_json::Value`, PascalCase members matching the awsJson1_1 member
//! names) so a `Get*` echoes exactly what its `Start*` / `Create*` persisted.
//! Storing the wire shape directly keeps nested configuration (`Settings`,
//! `ContentRedaction`, `ChannelDefinitions`, ...) round-tripping verbatim.
//!
//! Input-only members that must not leak into a response (a job's requested
//! output bucket / key) are kept out of the stored wire object; the computed
//! output-file URIs a completed job exposes are stashed in `job_outputs` and
//! spliced into the `Transcript` on settle. Tags live in an ARN-keyed side map
//! so `ListTagsForResource` has a single source of truth.
//!
//! The asynchronous lifecycles are modelled by advancing the stored status on
//! the next read; the same reconciliation runs on restart so an interrupted
//! transition never wedges. Jobs settle `QUEUED` -> `COMPLETED` (stamping
//! `CompletionTime` and a well-formed `Transcript`), vocabularies settle
//! `PENDING` -> `READY`, and custom language models settle `IN_PROGRESS` ->
//! `COMPLETED`.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

use crate::shared::now_epoch;

pub const TRANSCRIBE_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Per-account Amazon Transcribe state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TranscribeData {
    /// The account id this partition belongs to (for ARN / URI synthesis).
    #[serde(default)]
    pub account_id: String,
    /// The region this partition belongs to (for ARN / URI synthesis).
    #[serde(default)]
    pub region: String,

    /// Transcription jobs keyed by `TranscriptionJobName`; `TranscriptionJob`.
    #[serde(default)]
    pub transcription_jobs: BTreeMap<String, Value>,
    /// Medical transcription jobs keyed by name; `MedicalTranscriptionJob`.
    #[serde(default)]
    pub medical_transcription_jobs: BTreeMap<String, Value>,
    /// Call-analytics jobs keyed by name; `CallAnalyticsJob`.
    #[serde(default)]
    pub call_analytics_jobs: BTreeMap<String, Value>,
    /// Medical-scribe jobs keyed by name; `MedicalScribeJob`.
    #[serde(default)]
    pub medical_scribe_jobs: BTreeMap<String, Value>,
    /// Custom vocabularies keyed by `VocabularyName`; carries the wire object
    /// (`VocabularyName` / `LanguageCode` / `VocabularyState` /
    /// `LastModifiedTime` plus the persisted `DownloadUri`).
    #[serde(default)]
    pub vocabularies: BTreeMap<String, Value>,
    /// Medical vocabularies keyed by `VocabularyName`.
    #[serde(default)]
    pub medical_vocabularies: BTreeMap<String, Value>,
    /// Vocabulary filters keyed by `VocabularyFilterName`.
    #[serde(default)]
    pub vocabulary_filters: BTreeMap<String, Value>,
    /// Custom language models keyed by `ModelName`; `LanguageModel`.
    #[serde(default)]
    pub language_models: BTreeMap<String, Value>,
    /// Call-analytics categories keyed by `CategoryName`; `CategoryProperties`.
    #[serde(default)]
    pub call_analytics_categories: BTreeMap<String, Value>,

    /// Computed output-file URIs a completed job exposes, keyed by
    /// `"{family}/{name}"` (e.g. `"transcription/my-job"`). Populated at
    /// `Start*` and spliced into the job's `Transcript` when it settles.
    #[serde(default)]
    pub job_outputs: BTreeMap<String, Value>,

    /// Resource tags keyed by resource ARN.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

impl AccountState for TranscribeData {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            ..Default::default()
        }
    }
}

pub type SharedTranscribeState = Arc<RwLock<MultiAccountState<TranscribeData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct TranscribeSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<TranscribeData>,
}

/// A job status that is not yet terminal (still settling toward `COMPLETED`).
fn is_in_flight_job(status: &str) -> bool {
    matches!(status, "QUEUED" | "IN_PROGRESS")
}

impl TranscribeData {
    /// Advance every in-flight lifecycle one step to its settled state so an
    /// interrupted transition never wedges. Used on the next read and on
    /// restart. Returns `true` if anything changed.
    pub fn reconcile(&mut self) -> bool {
        let mut changed = false;
        let now = now_epoch();

        // Transcription jobs -> COMPLETED with a well-formed Transcript.
        for (name, job) in self.transcription_jobs.iter_mut() {
            let out = self.job_outputs.get(&format!("transcription/{name}"));
            if settle_job(job, now, "TranscriptionJobStatus", |obj| {
                if let Some(uri) = out.and_then(|o| o.get("uri")).and_then(Value::as_str) {
                    let mut t = Map::new();
                    t.insert("TranscriptFileUri".into(), json!(uri));
                    if let Some(r) = out.and_then(|o| o.get("redacted")).and_then(Value::as_str) {
                        t.insert("RedactedTranscriptFileUri".into(), json!(r));
                    }
                    obj.insert("Transcript".into(), Value::Object(t));
                }
                if let Some(subs) = out.and_then(|o| o.get("subtitles")).cloned() {
                    obj.insert("Subtitles".into(), subs);
                }
            }) {
                changed = true;
            }
        }

        // Medical transcription jobs.
        for (name, job) in self.medical_transcription_jobs.iter_mut() {
            let out = self.job_outputs.get(&format!("medical/{name}"));
            if settle_job(job, now, "TranscriptionJobStatus", |obj| {
                if let Some(uri) = out.and_then(|o| o.get("uri")).and_then(Value::as_str) {
                    obj.insert("Transcript".into(), json!({ "TranscriptFileUri": uri }));
                }
            }) {
                changed = true;
            }
        }

        // Call-analytics jobs.
        for (name, job) in self.call_analytics_jobs.iter_mut() {
            let out = self.job_outputs.get(&format!("call/{name}"));
            if settle_job(job, now, "CallAnalyticsJobStatus", |obj| {
                if let Some(uri) = out.and_then(|o| o.get("uri")).and_then(Value::as_str) {
                    obj.insert("Transcript".into(), json!({ "TranscriptFileUri": uri }));
                }
            }) {
                changed = true;
            }
        }

        // Medical-scribe jobs.
        for (name, job) in self.medical_scribe_jobs.iter_mut() {
            let out = self.job_outputs.get(&format!("scribe/{name}"));
            if settle_job(job, now, "MedicalScribeJobStatus", |obj| {
                if let Some(o) = out {
                    let mut m = Map::new();
                    if let Some(t) = o.get("uri").and_then(Value::as_str) {
                        m.insert("TranscriptFileUri".into(), json!(t));
                    }
                    if let Some(c) = o.get("clinical").and_then(Value::as_str) {
                        m.insert("ClinicalDocumentUri".into(), json!(c));
                    }
                    obj.insert("MedicalScribeOutput".into(), Value::Object(m));
                }
            }) {
                changed = true;
            }
        }

        // Vocabularies -> READY.
        for vocab in self
            .vocabularies
            .values_mut()
            .chain(self.medical_vocabularies.values_mut())
        {
            if settle_state(vocab, now, "VocabularyState", "PENDING", "READY") {
                changed = true;
            }
        }

        // Custom language models -> COMPLETED.
        for model in self.language_models.values_mut() {
            if settle_state(model, now, "ModelStatus", "IN_PROGRESS", "COMPLETED") {
                changed = true;
            }
        }

        changed
    }
}

/// Settle a job's `status_field` from an in-flight state to `COMPLETED`,
/// stamping `CompletionTime` and running `fill` (which adds the output shape).
/// Returns `true` if it fired.
fn settle_job(
    job: &mut Value,
    now: f64,
    status_field: &str,
    fill: impl FnOnce(&mut Map<String, Value>),
) -> bool {
    let Some(obj) = job.as_object_mut() else {
        return false;
    };
    let status = obj.get(status_field).and_then(Value::as_str).unwrap_or("");
    if !is_in_flight_job(status) {
        return false;
    }
    obj.insert(status_field.into(), json!("COMPLETED"));
    obj.insert("CompletionTime".into(), json!(now));
    fill(obj);
    true
}

/// Settle a resource's `status_field` from `from` to `to`, refreshing
/// `LastModifiedTime`. Returns `true` if it fired.
fn settle_state(res: &mut Value, now: f64, status_field: &str, from: &str, to: &str) -> bool {
    let Some(obj) = res.as_object_mut() else {
        return false;
    };
    if obj.get(status_field).and_then(Value::as_str) != Some(from) {
        return false;
    }
    obj.insert(status_field.into(), json!(to));
    obj.insert("LastModifiedTime".into(), json!(now));
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data() -> TranscribeData {
        TranscribeData::new_for_account("000000000000", "us-east-1", "")
    }

    #[test]
    fn transcription_job_settles_to_completed_with_transcript() {
        let mut d = data();
        d.transcription_jobs.insert(
            "j1".into(),
            json!({ "TranscriptionJobName": "j1", "TranscriptionJobStatus": "QUEUED" }),
        );
        d.job_outputs.insert(
            "transcription/j1".into(),
            json!({ "uri": "s3://bucket/j1.json" }),
        );
        assert!(d.reconcile());
        let j = &d.transcription_jobs["j1"];
        assert_eq!(j["TranscriptionJobStatus"], json!("COMPLETED"));
        assert_eq!(
            j["Transcript"]["TranscriptFileUri"],
            json!("s3://bucket/j1.json")
        );
        assert!(j.get("CompletionTime").is_some());
        // Terminal is a no-op.
        assert!(!d.reconcile());
    }

    #[test]
    fn vocabulary_settles_to_ready() {
        let mut d = data();
        d.vocabularies.insert(
            "v1".into(),
            json!({ "VocabularyName": "v1", "VocabularyState": "PENDING" }),
        );
        assert!(d.reconcile());
        assert_eq!(d.vocabularies["v1"]["VocabularyState"], json!("READY"));
        assert!(!d.reconcile());
    }

    #[test]
    fn language_model_settles_to_completed() {
        let mut d = data();
        d.language_models.insert(
            "m1".into(),
            json!({ "ModelName": "m1", "ModelStatus": "IN_PROGRESS" }),
        );
        assert!(d.reconcile());
        assert_eq!(d.language_models["m1"]["ModelStatus"], json!("COMPLETED"));
    }
}
