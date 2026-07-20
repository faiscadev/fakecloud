//! Account-partitioned, serializable state for Amazon Comprehend (`comprehend`).
//!
//! Every resource is stored as its already-output-valid wire JSON object
//! (`serde_json::Value`, PascalCase members matching the awsJson1_1 member
//! names) so a `Describe*` echoes exactly what its `Create*` / `Start*`
//! persisted. Storing the wire shape directly keeps nested configuration
//! (`InputDataConfig`, `OutputDataConfig`, `VpcConfig`, `TaskConfig`, ...)
//! round-tripping verbatim.
//!
//! Analysis jobs live in one `jobs` map keyed by their generated `JobId`; the
//! job family (which `Describe*Job` / `List*Jobs` / `Stop*Job` a job belongs to)
//! is tracked in the `job_families` side map so a family's `List` can filter and
//! a `Describe` can reject a job id from another family. Tags live in an
//! ARN-keyed side map so `ListTagsForResource` has a single source of truth.
//!
//! The asynchronous lifecycles are modelled by advancing the stored status on
//! the next read; the same reconciliation runs on restart so an interrupted
//! transition never wedges.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

use crate::shared::now_epoch;

pub const COMPREHEND_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Per-account Amazon Comprehend state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ComprehendData {
    /// The account id this partition belongs to (for ARN synthesis).
    #[serde(default)]
    pub account_id: String,
    /// The region this partition belongs to (for ARN synthesis).
    #[serde(default)]
    pub region: String,

    /// Asynchronous analysis jobs keyed by `JobId`; each value is the family's
    /// `*JobProperties` wire object.
    #[serde(default)]
    pub jobs: BTreeMap<String, Value>,
    /// `JobId` -> family key (e.g. `"sentiment"`, `"topics"`). Lets a family's
    /// `List`/`Describe`/`Stop` scope to its own jobs.
    #[serde(default)]
    pub job_families: BTreeMap<String, String>,

    /// Custom document classifiers keyed by ARN; `DocumentClassifierProperties`.
    #[serde(default)]
    pub document_classifiers: BTreeMap<String, Value>,
    /// Custom entity recognizers keyed by ARN; `EntityRecognizerProperties`.
    #[serde(default)]
    pub entity_recognizers: BTreeMap<String, Value>,
    /// Real-time inference endpoints keyed by ARN; `EndpointProperties`.
    #[serde(default)]
    pub endpoints: BTreeMap<String, Value>,
    /// Flywheels keyed by ARN; `FlywheelProperties`.
    #[serde(default)]
    pub flywheels: BTreeMap<String, Value>,
    /// Datasets keyed by ARN; `DatasetProperties`.
    #[serde(default)]
    pub datasets: BTreeMap<String, Value>,
    /// Flywheel iterations keyed by `"{flywheelArn}#{iterationId}"`;
    /// `FlywheelIterationProperties`.
    #[serde(default)]
    pub flywheel_iterations: BTreeMap<String, Value>,

    /// Resource policies keyed by the target resource ARN. Each value carries
    /// `ResourcePolicy` / `PolicyRevisionId` / `CreationTime` /
    /// `LastModifiedTime`.
    #[serde(default)]
    pub resource_policies: BTreeMap<String, Value>,

    /// Resource tags keyed by resource ARN.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

impl AccountState for ComprehendData {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            ..Default::default()
        }
    }
}

pub type SharedComprehendState = Arc<RwLock<MultiAccountState<ComprehendData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ComprehendSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<ComprehendData>,
}

impl ComprehendData {
    /// Advance every in-flight lifecycle one step to its settled state so an
    /// interrupted transition never wedges. Used on the next read and on
    /// restart. Returns `true` if anything changed.
    pub fn reconcile(&mut self) -> bool {
        let now = now_epoch();
        let mut changed = false;

        // Analysis jobs -> COMPLETED (or STOPPED after a stop request).
        for job in self.jobs.values_mut() {
            if settle_status(
                job,
                "JobStatus",
                &["SUBMITTED", "IN_PROGRESS"],
                "COMPLETED",
                &[("EndTime", json!(now))],
            ) || settle_status(
                job,
                "JobStatus",
                &["STOP_REQUESTED"],
                "STOPPED",
                &[("EndTime", json!(now))],
            ) {
                changed = true;
            }
        }

        // Custom classifiers / recognizers -> TRAINED (or STOPPED).
        for model in self
            .document_classifiers
            .values_mut()
            .chain(self.entity_recognizers.values_mut())
        {
            if settle_status(
                model,
                "Status",
                &["SUBMITTED", "TRAINING"],
                "TRAINED",
                &[
                    ("TrainingStartTime", json!(now)),
                    ("TrainingEndTime", json!(now)),
                    ("EndTime", json!(now)),
                ],
            ) || settle_status(
                model,
                "Status",
                &["STOP_REQUESTED"],
                "STOPPED",
                &[("EndTime", json!(now))],
            ) {
                changed = true;
            }
        }

        // Endpoints -> IN_SERVICE, publishing the desired inference units and
        // promoting the desired ModelArn / DataAccessRoleArn to their active
        // fields. Without this promotion an UpdateEndpoint settles the status but
        // DescribeEndpoint keeps returning the pre-update ModelArn/role forever
        // (the new values sit unread under Desired*) (bug-hunt).
        for ep in self.endpoints.values_mut() {
            let desired = ep.get("DesiredInferenceUnits").cloned().unwrap_or(json!(1));
            let mut stamp: Vec<(&str, Value)> = vec![
                ("CurrentInferenceUnits", desired),
                ("LastModifiedTime", json!(now)),
            ];
            if let Some(m) = ep.get("DesiredModelArn").filter(|v| !v.is_null()).cloned() {
                stamp.push(("ModelArn", m));
            }
            if let Some(r) = ep
                .get("DesiredDataAccessRoleArn")
                .filter(|v| !v.is_null())
                .cloned()
            {
                stamp.push(("DataAccessRoleArn", r));
            }
            if settle_status(
                ep,
                "Status",
                &["CREATING", "UPDATING"],
                "IN_SERVICE",
                &stamp,
            ) {
                changed = true;
            }
        }

        // Flywheels -> ACTIVE.
        for fw in self.flywheels.values_mut() {
            if settle_status(
                fw,
                "Status",
                &["CREATING", "UPDATING"],
                "ACTIVE",
                &[("LastModifiedTime", json!(now))],
            ) {
                changed = true;
            }
        }

        // Datasets -> COMPLETED.
        for ds in self.datasets.values_mut() {
            if settle_status(
                ds,
                "Status",
                &["CREATING"],
                "COMPLETED",
                &[("EndTime", json!(now)), ("NumberOfDocuments", json!(0))],
            ) {
                changed = true;
            }
        }

        // Flywheel iterations -> COMPLETED (or STOPPED).
        for it in self.flywheel_iterations.values_mut() {
            if settle_status(
                it,
                "Status",
                &["TRAINING", "EVALUATING"],
                "COMPLETED",
                &[("EndTime", json!(now))],
            ) || settle_status(
                it,
                "Status",
                &["STOP_REQUESTED"],
                "STOPPED",
                &[("EndTime", json!(now))],
            ) {
                changed = true;
            }
        }

        changed
    }
}

/// Settle a resource's `status_field` from any of `from` to `to`, stamping each
/// `(key, value)` in `stamp`. Returns `true` if it fired.
fn settle_status(
    res: &mut Value,
    status_field: &str,
    from: &[&str],
    to: &str,
    stamp: &[(&str, Value)],
) -> bool {
    let Some(obj) = res.as_object_mut() else {
        return false;
    };
    let cur = obj.get(status_field).and_then(Value::as_str).unwrap_or("");
    if !from.contains(&cur) {
        return false;
    }
    obj.insert(status_field.to_string(), json!(to));
    for (k, v) in stamp {
        obj.insert((*k).to_string(), v.clone());
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data() -> ComprehendData {
        ComprehendData::new_for_account("000000000000", "us-east-1", "")
    }

    #[test]
    fn job_settles_to_completed() {
        let mut d = data();
        d.jobs.insert(
            "j1".into(),
            json!({ "JobId": "j1", "JobStatus": "SUBMITTED" }),
        );
        d.job_families.insert("j1".into(), "sentiment".into());
        assert!(d.reconcile());
        assert_eq!(d.jobs["j1"]["JobStatus"], "COMPLETED");
        assert!(d.jobs["j1"].get("EndTime").is_some());
        assert!(!d.reconcile());
    }

    #[test]
    fn stop_requested_job_settles_to_stopped() {
        let mut d = data();
        d.jobs.insert(
            "j1".into(),
            json!({ "JobId": "j1", "JobStatus": "STOP_REQUESTED" }),
        );
        assert!(d.reconcile());
        assert_eq!(d.jobs["j1"]["JobStatus"], "STOPPED");
    }

    #[test]
    fn classifier_settles_to_trained() {
        let mut d = data();
        d.document_classifiers
            .insert("arn".into(), json!({ "Status": "SUBMITTED" }));
        assert!(d.reconcile());
        assert_eq!(d.document_classifiers["arn"]["Status"], "TRAINED");
    }

    #[test]
    fn endpoint_settles_in_service() {
        let mut d = data();
        d.endpoints.insert(
            "arn".into(),
            json!({ "Status": "CREATING", "DesiredInferenceUnits": 2 }),
        );
        assert!(d.reconcile());
        assert_eq!(d.endpoints["arn"]["Status"], "IN_SERVICE");
        assert_eq!(d.endpoints["arn"]["CurrentInferenceUnits"], 2);
    }

    #[test]
    fn updating_endpoint_promotes_desired_model_and_role() {
        // An UpdateEndpoint sets Desired* fields and status UPDATING; settling
        // must promote them to the active ModelArn / DataAccessRoleArn so a
        // DescribeEndpoint returns the new values, not the pre-update ones.
        let mut d = data();
        d.endpoints.insert(
            "arn".into(),
            json!({
                "Status": "UPDATING",
                "ModelArn": "arn:old-model",
                "DataAccessRoleArn": "arn:old-role",
                "DesiredInferenceUnits": 3,
                "DesiredModelArn": "arn:new-model",
                "DesiredDataAccessRoleArn": "arn:new-role"
            }),
        );
        assert!(d.reconcile());
        let ep = &d.endpoints["arn"];
        assert_eq!(ep["Status"], "IN_SERVICE");
        assert_eq!(ep["CurrentInferenceUnits"], 3);
        assert_eq!(ep["ModelArn"], "arn:new-model");
        assert_eq!(ep["DataAccessRoleArn"], "arn:new-role");
    }

    #[test]
    fn flywheel_and_dataset_settle() {
        let mut d = data();
        d.flywheels
            .insert("arn".into(), json!({ "Status": "CREATING" }));
        d.datasets
            .insert("arn".into(), json!({ "Status": "CREATING" }));
        assert!(d.reconcile());
        assert_eq!(d.flywheels["arn"]["Status"], "ACTIVE");
        assert_eq!(d.datasets["arn"]["Status"], "COMPLETED");
    }
}
