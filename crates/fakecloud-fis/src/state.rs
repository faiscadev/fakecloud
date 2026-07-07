//! Account-partitioned, serializable state for AWS FIS's (`fis`) control plane.
//!
//! Each experiment template and experiment is stored as its already-output-valid
//! wire JSON object (`serde_json::Value`, camelCase members matching the
//! restJson1 member names -- FIS declares no `jsonName` overrides) keyed by its
//! id. Storing the wire shape directly keeps nested targets / actions /
//! stopConditions / logConfiguration round-tripping verbatim and guarantees
//! `GetExperimentTemplate` / `GetExperiment` echo exactly what was persisted. The
//! experiment lifecycle mutates the stored object's `state.status` on the next
//! read.
//!
//! All map keys are plain `String`s (template id, experiment id, account id,
//! resource ARN) so the snapshot never depends on the tuple-key serde adapter
//! that has silently broken snapshot serialization on other services.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const FIS_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Per-account AWS FIS state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct FisData {
    /// Experiment templates keyed by `id`, stored as their
    /// `GetExperimentTemplate`-shaped `ExperimentTemplate` wire object.
    #[serde(default)]
    pub templates: BTreeMap<String, Value>,
    /// Experiments keyed by `id`, stored as their `GetExperiment`-shaped
    /// `Experiment` wire object. The lifecycle mutates `state.status` on read.
    #[serde(default)]
    pub experiments: BTreeMap<String, Value>,
    /// Per-template target-account configurations: template id -> account id ->
    /// `TargetAccountConfiguration` wire object.
    #[serde(default)]
    pub target_account_configs: BTreeMap<String, BTreeMap<String, Value>>,
    /// Per-experiment target-account configurations copied at `StartExperiment`:
    /// experiment id -> account id -> config wire object.
    #[serde(default)]
    pub experiment_target_account_configs: BTreeMap<String, BTreeMap<String, Value>>,
    /// Safety levers keyed by `id`, storing only levers whose state has been
    /// explicitly updated (an un-updated lever reads back as `disengaged`).
    #[serde(default)]
    pub safety_levers: BTreeMap<String, Value>,
}

impl FisData {
    /// Step every in-flight experiment lifecycle one stage so an interrupted run
    /// never wedges. The deterministic progression is
    /// `pending` -> `initiating` -> `running` -> `completed`, plus
    /// `stopping` -> `stopped`; terminal states are left untouched. Used both on
    /// the next read and on restart. Returns `true` if anything changed.
    pub fn reconcile_experiments(&mut self) -> bool {
        let mut changed = false;
        let ids: Vec<String> = self.experiments.keys().cloned().collect();
        for id in ids {
            let Some(exp) = self.experiments.get_mut(&id).and_then(Value::as_object_mut) else {
                continue;
            };
            let status = exp
                .get("state")
                .and_then(|s| s.get("status"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let next = match status.as_str() {
                "pending" => Some(("initiating", "Experiment is initiating.")),
                "initiating" => Some(("running", "Experiment is running.")),
                "running" => Some(("completed", "Experiment completed.")),
                "stopping" => Some(("stopped", "Experiment stopped.")),
                _ => None,
            };
            let Some((new_status, reason)) = next else {
                continue;
            };
            set_experiment_status(exp, new_status, reason);
            changed = true;
        }
        changed
    }
}

/// Apply a status transition to a stored experiment wire object: update
/// `state`, stamp `startTime` when it first starts running, stamp `endTime` on a
/// terminal transition, and mirror the status onto each action's `state`.
pub fn set_experiment_status(exp: &mut Map<String, Value>, status: &str, reason: &str) {
    exp.insert(
        "state".into(),
        json!({ "status": status, "reason": reason }),
    );
    let now = crate::shared::now_epoch();
    if status == "running" && exp.get("startTime").is_none() {
        exp.insert("startTime".into(), json!(now));
    }
    if matches!(status, "completed" | "stopped" | "failed" | "cancelled") {
        exp.entry("endTime").or_insert_with(|| json!(now));
    }
    // Mirror onto each action so the experiment's action states track the run.
    let action_status = match status {
        "pending" | "initiating" => "pending",
        "running" => "running",
        "completed" => "completed",
        "stopping" => "stopping",
        "stopped" => "stopped",
        other => other,
    };
    if let Some(actions) = exp.get_mut("actions").and_then(Value::as_object_mut) {
        for (_name, action) in actions.iter_mut() {
            if let Some(a) = action.as_object_mut() {
                a.insert(
                    "state".into(),
                    json!({ "status": action_status, "reason": reason }),
                );
                if action_status == "running" && a.get("startTime").is_none() {
                    a.insert("startTime".into(), json!(now));
                }
                if matches!(action_status, "completed" | "stopped" | "failed") {
                    a.entry("endTime").or_insert_with(|| json!(now));
                }
            }
        }
    }
}

impl AccountState for FisData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedFisState = Arc<RwLock<MultiAccountState<FisData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct FisSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<FisData>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data_with_experiment(status: &str) -> FisData {
        let mut d = FisData::default();
        d.experiments.insert(
            "EXP1".to_string(),
            json!({
                "id": "EXP1",
                "state": { "status": status, "reason": "x" },
                "actions": { "a1": { "actionId": "aws:ec2:stop-instances", "state": { "status": status } } }
            }),
        );
        d
    }

    #[test]
    fn reconcile_steps_initiating_to_running() {
        let mut d = data_with_experiment("initiating");
        assert!(d.reconcile_experiments());
        assert_eq!(d.experiments["EXP1"]["state"]["status"], json!("running"));
        assert!(d.experiments["EXP1"]["startTime"].is_number());
        assert_eq!(
            d.experiments["EXP1"]["actions"]["a1"]["state"]["status"],
            json!("running")
        );
    }

    #[test]
    fn reconcile_steps_running_to_completed_with_endtime() {
        let mut d = data_with_experiment("running");
        assert!(d.reconcile_experiments());
        assert_eq!(d.experiments["EXP1"]["state"]["status"], json!("completed"));
        assert!(d.experiments["EXP1"]["endTime"].is_number());
    }

    #[test]
    fn reconcile_steps_stopping_to_stopped() {
        let mut d = data_with_experiment("stopping");
        assert!(d.reconcile_experiments());
        assert_eq!(d.experiments["EXP1"]["state"]["status"], json!("stopped"));
    }

    #[test]
    fn reconcile_terminal_is_noop() {
        let mut d = data_with_experiment("completed");
        assert!(!d.reconcile_experiments());
    }
}
