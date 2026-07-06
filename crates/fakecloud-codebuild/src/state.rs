//! Account-partitioned, serializable state for AWS CodeBuild.
//!
//! Resources are stored as assembled response-shaped JSON values so a read
//! returns exactly what was written (round-trip fidelity). Builds and build
//! batches driven by the real container backend are tracked in
//! [`CodeBuildState::real_backed_builds`] so `BatchGetBuilds` leaves their
//! settling to the execution task, and so an orphan can be failed on restart.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::Utc;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const CODEBUILD_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// The account-scoped CodeBuild state for one AWS account.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CodeBuildState {
    /// Projects keyed by project name; value is the assembled `Project` JSON.
    #[serde(default)]
    pub projects: BTreeMap<String, Value>,
    /// Builds keyed by build id (`<project>:<uuid>`); value is a `Build` JSON.
    #[serde(default)]
    pub builds: BTreeMap<String, Value>,
    /// Insertion order of build ids (most-recent last) for time-ordered lists.
    #[serde(default)]
    pub build_order: Vec<String>,
    /// Per-project monotonic build-number counter, keyed by project name. Only
    /// ever increments (never reused after a build is deleted), matching AWS's
    /// per-project `buildNumber` semantics.
    #[serde(default)]
    pub build_numbers: BTreeMap<String, i64>,
    /// Build batches keyed by batch id (`<project>:<uuid>`); `BuildBatch` JSON.
    #[serde(default)]
    pub build_batches: BTreeMap<String, Value>,
    #[serde(default)]
    pub build_batch_order: Vec<String>,
    /// Per-project monotonic build-batch-number counter, keyed by project name.
    #[serde(default)]
    pub build_batch_numbers: BTreeMap<String, i64>,
    /// Report groups keyed by ARN; value is a `ReportGroup` JSON.
    #[serde(default)]
    pub report_groups: BTreeMap<String, Value>,
    /// Reports keyed by ARN; value is a `Report` JSON.
    #[serde(default)]
    pub reports: BTreeMap<String, Value>,
    #[serde(default)]
    pub report_order: Vec<String>,
    /// Fleets keyed by ARN; value is a `Fleet` JSON.
    #[serde(default)]
    pub fleets: BTreeMap<String, Value>,
    /// Source credentials keyed by ARN; value is a `SourceCredentialsInfo` JSON.
    #[serde(default)]
    pub source_credentials: BTreeMap<String, Value>,
    /// Resource policies keyed by resource ARN; value is the policy document.
    #[serde(default)]
    pub resource_policies: BTreeMap<String, String>,
    /// Sandboxes keyed by sandbox id; value is a `Sandbox` JSON.
    #[serde(default)]
    pub sandboxes: BTreeMap<String, Value>,
    #[serde(default)]
    pub sandbox_order: Vec<String>,
    /// Command executions keyed by id; value is a `CommandExecution` JSON.
    #[serde(default)]
    pub command_executions: BTreeMap<String, Value>,
    #[serde(default)]
    pub command_execution_order: Vec<String>,
    /// Ids of builds whose execution is (or was) driven by a REAL Docker
    /// container backend rather than the deterministic settle-on-read path.
    /// A member id is settled by its background execution task, so the lazy
    /// `settle_build` on `BatchGetBuilds` must skip it. The set is pruned when
    /// a build reaches a terminal state; a member that survives a restart had
    /// its container killed by the process exit and is reconciled to `FAILED`
    /// by [`CodeBuildState::reconcile_builds`]. Not part of any wire shape.
    #[serde(default)]
    pub real_backed_builds: BTreeSet<String>,
    /// Build-batch analogue of [`Self::real_backed_builds`].
    #[serde(default)]
    pub real_backed_build_batches: BTreeSet<String>,
}

/// Settle a real-backed record that is still `IN_PROGRESS` to `FAILED` because
/// the container backing it is gone (the process that ran it has exited).
/// Mirrors the AWS behavior of a build whose compute vanished mid-run. Returns
/// true if the record was mutated.
fn fail_orphaned(record: &mut Value, status_key: &str) -> bool {
    let in_progress = record
        .get(status_key)
        .and_then(Value::as_str)
        .map(|s| s == "IN_PROGRESS")
        .unwrap_or(false);
    if !in_progress {
        return false;
    }
    let now = ts_ms(Utc::now().timestamp_millis());
    let complete_key = if status_key == "buildBatchStatus" {
        "complete"
    } else {
        "buildComplete"
    };
    if let Some(obj) = record.as_object_mut() {
        obj.insert(status_key.into(), json!("FAILED"));
        obj.insert("currentPhase".into(), json!("COMPLETED"));
        obj.insert(complete_key.into(), json!(true));
        obj.insert("endTime".into(), now.clone());
        // Append a FINALIZING+COMPLETED marker carrying the orphan reason so a
        // reader sees why the build failed after a restart.
        if let Some(phases) = obj.get_mut("phases").and_then(Value::as_array_mut) {
            phases.push(json!({
                "phaseType": "FINALIZING",
                "phaseStatus": "FAILED",
                "endTime": now,
                "durationInSeconds": 0,
                "contexts": [{
                    "statusCode": "BUILD_CONTAINER_TERMINATED",
                    "message": "Build container was terminated before the build completed (fakecloud server restarted).",
                }],
            }));
            phases.push(json!({ "phaseType": "COMPLETED", "endTime": now }));
        }
    }
    true
}

/// Serialize an epoch-millis timestamp as the fractional-epoch-seconds number
/// AWS uses for CodeBuild `startTime`/`endTime`. Kept in state so the reconcile
/// path doesn't depend on the service module's formatter.
fn ts_ms(millis: i64) -> Value {
    json!(millis as f64 / 1000.0)
}

impl CodeBuildState {
    /// Reconcile builds/build-batches that were driven by a real container
    /// backend and were persisted mid-run: their container died with the
    /// previous process, so an `IN_PROGRESS` record can never settle on its
    /// own. Flip each such orphan to `FAILED` and clear the tracking set.
    /// Returns true if anything was mutated. Called on snapshot load, matching
    /// MQ's `reconcile_brokers` restart contract.
    pub fn reconcile_builds(&mut self) -> bool {
        let mut mutated = false;
        for id in std::mem::take(&mut self.real_backed_builds) {
            if let Some(build) = self.builds.get_mut(&id) {
                mutated |= fail_orphaned(build, "buildStatus");
            }
        }
        for id in std::mem::take(&mut self.real_backed_build_batches) {
            if let Some(batch) = self.build_batches.get_mut(&id) {
                mutated |= fail_orphaned(batch, "buildBatchStatus");
            }
        }
        mutated
    }
}

impl AccountState for CodeBuildState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedCodeBuildState = Arc<RwLock<MultiAccountState<CodeBuildState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct CodeBuildSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<CodeBuildState>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconcile_fails_orphaned_real_backed_build() {
        let mut st = CodeBuildState::default();
        st.builds.insert(
            "proj:abc".into(),
            json!({
                "id": "proj:abc",
                "buildStatus": "IN_PROGRESS",
                "phases": [{ "phaseType": "BUILD" }],
            }),
        );
        st.real_backed_builds.insert("proj:abc".into());

        assert!(st.reconcile_builds());
        let build = &st.builds["proj:abc"];
        assert_eq!(build["buildStatus"], "FAILED");
        assert_eq!(build["buildComplete"], true);
        // The tracking set is cleared so a later BatchGetBuilds can't settle it.
        assert!(st.real_backed_builds.is_empty());
        // A terminal COMPLETED phase and the orphan reason were appended.
        let phases = build["phases"].as_array().unwrap();
        assert!(phases.iter().any(|p| p["phaseType"] == "COMPLETED"));
    }

    #[test]
    fn reconcile_leaves_terminal_and_untracked_builds_untouched() {
        let mut st = CodeBuildState::default();
        // A terminal real-backed build must not be re-failed.
        st.builds.insert(
            "proj:done".into(),
            json!({ "id": "proj:done", "buildStatus": "SUCCEEDED" }),
        );
        st.real_backed_builds.insert("proj:done".into());
        // A deterministic (untracked) IN_PROGRESS build keeps its lazy-settle
        // behavior — reconcile must not touch it.
        st.builds.insert(
            "proj:lazy".into(),
            json!({ "id": "proj:lazy", "buildStatus": "IN_PROGRESS" }),
        );

        assert!(!st.reconcile_builds());
        assert_eq!(st.builds["proj:done"]["buildStatus"], "SUCCEEDED");
        assert_eq!(st.builds["proj:lazy"]["buildStatus"], "IN_PROGRESS");
    }

    #[test]
    fn reconcile_fails_orphaned_build_batch() {
        let mut st = CodeBuildState::default();
        st.build_batches.insert(
            "proj:batch".into(),
            json!({ "id": "proj:batch", "buildBatchStatus": "IN_PROGRESS", "phases": [] }),
        );
        st.real_backed_build_batches.insert("proj:batch".into());

        assert!(st.reconcile_builds());
        let batch = &st.build_batches["proj:batch"];
        assert_eq!(batch["buildBatchStatus"], "FAILED");
        assert_eq!(batch["complete"], true);
        assert!(st.real_backed_build_batches.is_empty());
    }
}
