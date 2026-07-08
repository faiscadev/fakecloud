//! Account-partitioned, serializable state for Amazon Translate (`translate`).
//!
//! Every resource is stored as its already-output-valid wire JSON object
//! (`serde_json::Value`, PascalCase members matching the awsJson1_1 member
//! names) so a `Get*` echoes exactly what its `Create*` / `Import*` / `Start*`
//! persisted. Tags live in an ARN-keyed side map so `ListTagsForResource` has a
//! single source of truth.
//!
//! The asynchronous lifecycles are modelled by advancing the stored status on
//! the next read; the same reconciliation runs on restart so an interrupted
//! transition never wedges. Batch translation jobs settle `SUBMITTED` ->
//! `COMPLETED` (stamping `EndTime` and a `JobDetails` count), `STOP_REQUESTED`
//! -> `STOPPED`; parallel data settles `CREATING` -> `ACTIVE` and an update's
//! `LatestUpdateAttemptStatus` `UPDATING` -> `ACTIVE`.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

use crate::shared::now_epoch;

pub const TRANSLATE_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Per-account Amazon Translate state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TranslateData {
    /// The account id this partition belongs to (for ARN synthesis).
    #[serde(default)]
    pub account_id: String,
    /// The region this partition belongs to (for ARN synthesis).
    #[serde(default)]
    pub region: String,

    /// Custom terminologies keyed by `Name`; stores `TerminologyProperties`.
    #[serde(default)]
    pub terminologies: BTreeMap<String, Value>,
    /// Parallel data keyed by `Name`; stores `ParallelDataProperties`.
    #[serde(default)]
    pub parallel_data: BTreeMap<String, Value>,
    /// Batch text-translation jobs keyed by `JobId`; stores
    /// `TextTranslationJobProperties`.
    #[serde(default)]
    pub text_translation_jobs: BTreeMap<String, Value>,

    /// Resource tags keyed by resource ARN.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

impl AccountState for TranslateData {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            ..Default::default()
        }
    }
}

pub type SharedTranslateState = Arc<RwLock<MultiAccountState<TranslateData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct TranslateSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<TranslateData>,
}

impl TranslateData {
    /// Advance every in-flight lifecycle one step to its settled state so an
    /// interrupted transition never wedges. Used on the next read and on
    /// restart. Returns `true` if anything changed.
    pub fn reconcile(&mut self) -> bool {
        let mut changed = false;
        let now = now_epoch();

        // Batch translation jobs settle straight to their terminal state.
        for job in self.text_translation_jobs.values_mut() {
            let Some(obj) = job.as_object_mut() else {
                continue;
            };
            match obj.get("JobStatus").and_then(Value::as_str) {
                Some("SUBMITTED") | Some("IN_PROGRESS") => {
                    obj.insert("JobStatus".into(), json!("COMPLETED"));
                    obj.insert("EndTime".into(), json!(now));
                    obj.entry("JobDetails").or_insert(json!({
                        "TranslatedDocumentsCount": 0,
                        "DocumentsWithErrorsCount": 0,
                        "InputDocumentsCount": 0,
                    }));
                    changed = true;
                }
                Some("STOP_REQUESTED") => {
                    obj.insert("JobStatus".into(), json!("STOPPED"));
                    obj.insert("EndTime".into(), json!(now));
                    changed = true;
                }
                _ => {}
            }
        }

        // Parallel data settles CREATING -> ACTIVE; a pending update's
        // LatestUpdateAttemptStatus settles UPDATING -> ACTIVE.
        for pd in self.parallel_data.values_mut() {
            let Some(obj) = pd.as_object_mut() else {
                continue;
            };
            if obj.get("Status").and_then(Value::as_str) == Some("CREATING") {
                obj.insert("Status".into(), json!("ACTIVE"));
                obj.insert("LastUpdatedAt".into(), json!(now));
                changed = true;
            }
            if obj.get("LatestUpdateAttemptStatus").and_then(Value::as_str) == Some("UPDATING") {
                obj.insert("LatestUpdateAttemptStatus".into(), json!("ACTIVE"));
                obj.insert("Status".into(), json!("ACTIVE"));
                obj.insert("LastUpdatedAt".into(), json!(now));
                changed = true;
            }
        }

        changed
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data() -> TranslateData {
        TranslateData::new_for_account("000000000000", "us-east-1", "")
    }

    #[test]
    fn job_settles_to_completed() {
        let mut d = data();
        d.text_translation_jobs.insert(
            "j1".into(),
            json!({ "JobId": "j1", "JobStatus": "SUBMITTED" }),
        );
        assert!(d.reconcile());
        let j = &d.text_translation_jobs["j1"];
        assert_eq!(j["JobStatus"], json!("COMPLETED"));
        assert!(j.get("EndTime").is_some());
        assert!(j.get("JobDetails").is_some());
        assert!(!d.reconcile());
    }

    #[test]
    fn stop_requested_settles_to_stopped() {
        let mut d = data();
        d.text_translation_jobs.insert(
            "j2".into(),
            json!({ "JobId": "j2", "JobStatus": "STOP_REQUESTED" }),
        );
        assert!(d.reconcile());
        assert_eq!(d.text_translation_jobs["j2"]["JobStatus"], json!("STOPPED"));
    }

    #[test]
    fn parallel_data_settles_to_active() {
        let mut d = data();
        d.parallel_data
            .insert("p1".into(), json!({ "Name": "p1", "Status": "CREATING" }));
        assert!(d.reconcile());
        assert_eq!(d.parallel_data["p1"]["Status"], json!("ACTIVE"));
        assert!(!d.reconcile());
    }

    #[test]
    fn parallel_data_update_settles() {
        let mut d = data();
        d.parallel_data.insert(
            "p2".into(),
            json!({ "Name": "p2", "Status": "ACTIVE", "LatestUpdateAttemptStatus": "UPDATING" }),
        );
        assert!(d.reconcile());
        assert_eq!(
            d.parallel_data["p2"]["LatestUpdateAttemptStatus"],
            json!("ACTIVE")
        );
    }
}
