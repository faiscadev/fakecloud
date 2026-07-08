//! Account-partitioned, serializable state for AWS Amplify's (`amplify`)
//! control plane.
//!
//! Each resource is stored as its already-output-valid wire JSON object
//! (`serde_json::Value`, camelCase members matching the restJson1 member
//! names) so `Get*` echoes exactly what `Create*` / `Update*` persisted.
//! Storing the wire shape directly keeps nested configuration
//! (`customRules`, `cacheConfig`, `subDomains`, ...) round-tripping verbatim.
//!
//! Apps own their branches, domain associations, backend environments, and
//! per-branch jobs; webhooks live in an account-global map keyed by webhook id
//! (they are addressed directly by `GET /webhooks/{webhookId}`). Every map key
//! is a plain `String` so the snapshot never depends on the tuple-key serde
//! adapter that has silently broken snapshot serialization on other services.
//!
//! The async domain-verification and job-build lifecycles are modelled by
//! advancing the stored `domainStatus` / job `status` on the next read; the
//! same reconciliation runs on restart so an interrupted lifecycle never
//! wedges.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const AMPLIFY_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// One Amplify app plus the child resources it owns.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AppRecord {
    /// The `App` wire object (camelCase members matching `GetApp`).
    pub app: Value,
    /// Branches keyed by `branchName`; each value is a `Branch` wire object.
    #[serde(default)]
    pub branches: BTreeMap<String, Value>,
    /// Domain associations keyed by `domainName`; `DomainAssociation` objects.
    #[serde(default)]
    pub domains: BTreeMap<String, Value>,
    /// Backend environments keyed by `environmentName`.
    #[serde(default)]
    pub backend_environments: BTreeMap<String, Value>,
    /// Jobs keyed by `branchName` then `jobId`; each value is a `Job` object
    /// (`{ "summary": {...}, "steps": [...] }`).
    #[serde(default)]
    pub jobs: BTreeMap<String, BTreeMap<String, Value>>,
    /// Next job id to allocate, per branch (Amplify job ids are numeric and
    /// monotonically increasing within a branch).
    #[serde(default)]
    pub next_job_id: BTreeMap<String, u64>,
}

/// Per-account AWS Amplify state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AmplifyData {
    /// Apps keyed by `appId`.
    #[serde(default)]
    pub apps: BTreeMap<String, AppRecord>,
    /// Webhooks keyed by `webhookId`; each value is a `Webhook` wire object
    /// (carrying its owning `appId`).
    #[serde(default)]
    pub webhooks: BTreeMap<String, Value>,
}

/// The ordered, non-terminal job build stages. `StartJob` enters at `PENDING`;
/// each read advances one stage until `SUCCEED`.
const JOB_STAGES: &[&str] = &["PENDING", "PROVISIONING", "RUNNING", "SUCCEED"];

fn next_job_stage(current: &str) -> Option<&'static str> {
    let idx = JOB_STAGES.iter().position(|s| *s == current)?;
    JOB_STAGES.get(idx + 1).copied()
}

impl AmplifyData {
    /// Advance any in-flight domain / job lifecycle one step so an interrupted
    /// transition never wedges. Used both on the next read and on restart.
    /// Returns `true` if anything changed.
    pub fn reconcile(&mut self) -> bool {
        let mut changed = false;
        for app in self.apps.values_mut() {
            // Domains: settle a freshly-created / updating domain to AVAILABLE
            // and mark its subdomains verified.
            for domain in app.domains.values_mut() {
                if advance_domain(domain) {
                    changed = true;
                }
            }
            // Jobs: advance each non-terminal build one stage.
            let app_id = app
                .app
                .get("appId")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            for (branch, jobs) in app.jobs.iter_mut() {
                for (job_id, job) in jobs.iter_mut() {
                    if advance_job(job, &app_id, branch, job_id) {
                        changed = true;
                    }
                }
            }
        }
        changed
    }
}

/// Advance a domain association toward `AVAILABLE`. A `CREATING` /
/// `IN_PROGRESS` / `UPDATING` / `PENDING_VERIFICATION` domain becomes
/// `AVAILABLE` and its subdomains are marked verified.
fn advance_domain(domain: &mut Value) -> bool {
    let Some(obj) = domain.as_object_mut() else {
        return false;
    };
    let status = obj
        .get("domainStatus")
        .and_then(Value::as_str)
        .unwrap_or("");
    let in_flight = matches!(
        status,
        "CREATING"
            | "IN_PROGRESS"
            | "UPDATING"
            | "PENDING_VERIFICATION"
            | "PENDING_DEPLOYMENT"
            | "REQUESTING_CERTIFICATE"
    );
    if !in_flight {
        return false;
    }
    obj.insert("domainStatus".into(), json!("AVAILABLE"));
    obj.insert("updateStatus".into(), json!("UPDATE_COMPLETE"));
    if let Some(subs) = obj.get_mut("subDomains").and_then(Value::as_array_mut) {
        for sub in subs.iter_mut() {
            if let Some(s) = sub.as_object_mut() {
                s.insert("verified".into(), json!(true));
            }
        }
    }
    true
}

/// Advance a job's build one stage. On reaching `SUCCEED` it stamps `endTime`
/// and records a single completed `BUILD` step. Terminal jobs (`SUCCEED`,
/// `FAILED`, `CANCELLED`) are left untouched.
fn advance_job(job: &mut Value, app_id: &str, branch: &str, job_id: &str) -> bool {
    let Some(obj) = job.as_object_mut() else {
        return false;
    };
    let status = obj
        .get("summary")
        .and_then(|s| s.get("status"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    let Some(next) = next_job_stage(&status) else {
        return false;
    };
    let now = crate::shared::now_epoch();
    if let Some(summary) = obj.get_mut("summary").and_then(Value::as_object_mut) {
        summary.insert("status".into(), json!(next));
        if next == "SUCCEED" {
            summary.insert("endTime".into(), json!(now));
        }
    }
    if next == "SUCCEED" {
        let _ = (app_id, branch, job_id);
        let start = obj
            .get("summary")
            .and_then(|s| s.get("startTime"))
            .cloned()
            .unwrap_or_else(|| json!(now));
        obj.insert(
            "steps".into(),
            json!([{
                "stepName": "BUILD",
                "startTime": start,
                "status": "SUCCEED",
                "endTime": now,
            }]),
        );
    }
    true
}

impl AccountState for AmplifyData {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedAmplifyState = Arc<RwLock<MultiAccountState<AmplifyData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct AmplifySnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<AmplifyData>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app_with_domain(status: &str) -> AmplifyData {
        let mut d = AmplifyData::default();
        let mut rec = AppRecord {
            app: json!({ "appId": "d1" }),
            ..Default::default()
        };
        rec.domains.insert(
            "example.com".to_string(),
            json!({
                "domainStatus": status,
                "subDomains": [{ "verified": false }]
            }),
        );
        d.apps.insert("d1".to_string(), rec);
        d
    }

    #[test]
    fn reconcile_settles_domain_to_available() {
        let mut d = app_with_domain("CREATING");
        assert!(d.reconcile());
        let dom = &d.apps["d1"].domains["example.com"];
        assert_eq!(dom["domainStatus"], json!("AVAILABLE"));
        assert_eq!(dom["subDomains"][0]["verified"], json!(true));
    }

    #[test]
    fn reconcile_available_domain_is_noop() {
        let mut d = app_with_domain("AVAILABLE");
        assert!(!d.reconcile());
    }

    #[test]
    fn reconcile_advances_job_one_stage() {
        let mut d = AmplifyData::default();
        let mut rec = AppRecord {
            app: json!({ "appId": "d1" }),
            ..Default::default()
        };
        let mut branch_jobs = BTreeMap::new();
        branch_jobs.insert(
            "1".to_string(),
            json!({ "summary": { "status": "PENDING", "startTime": 1.0 }, "steps": [] }),
        );
        rec.jobs.insert("main".to_string(), branch_jobs);
        d.apps.insert("d1".to_string(), rec);

        assert!(d.reconcile());
        assert_eq!(
            d.apps["d1"].jobs["main"]["1"]["summary"]["status"],
            json!("PROVISIONING")
        );
        // Walk to terminal.
        d.reconcile(); // RUNNING
        d.reconcile(); // SUCCEED
        let job = &d.apps["d1"].jobs["main"]["1"];
        assert_eq!(job["summary"]["status"], json!("SUCCEED"));
        assert!(job["summary"].get("endTime").is_some());
        assert_eq!(job["steps"][0]["stepName"], json!("BUILD"));
        // Terminal is a no-op.
        assert!(!d.reconcile());
    }
}
