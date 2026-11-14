//! Auto-upgrade analyses of jobs configured with `GlueVersion=auto`
//! (GetJobUpgradeAnalysis / StartJobUpgradeAnalysis / StopJobUpgradeAnalysis,
//! and the `Job.AutoUpgradeMetadata` they drive).
//!
//! Enabling `auto` on a job starts an analysis in `OBSERVING`. Glue observes
//! the job's runs on its current version; once a run has succeeded since the
//! analysis began, the analysis waits for the customer's consent
//! (`WAITING_FOR_CONSENT`). Approving it upgrades the job to the target
//! version (`UPGRADING`, settling to `SUCCEEDED`, after which runs use the
//! target version); declining or stopping it ends it in `STOPPED`.

use chrono::Utc;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::common::{entity_not_found, invalid_input, req_str};
use crate::service::GlueService;
use crate::state::{GlueState, Job, JobUpgradeAnalysis};

/// The Glue version an auto-upgrade moves a job to: the current one.
pub(crate) const AUTO_UPGRADE_TARGET_VERSION: &str = "6.0";

const TERMINAL: &[&str] = &["SUCCEEDED", "FAILED", "TIMEOUT", "STOPPED"];

/// The analysis's status as of now: an observing analysis is waiting for
/// consent once one of the job's runs has succeeded since it began, and an
/// approved upgrade has completed.
fn effective_status(state: &GlueState, a: &JobUpgradeAnalysis) -> String {
    match a.status.as_str() {
        "OBSERVING" => {
            let observed = state.job_runs.values().any(|r| {
                r.job_name == a.job_name && r.state == "SUCCEEDED" && r.started_on >= a.started_on
            });
            if observed {
                "WAITING_FOR_CONSENT".to_string()
            } else {
                a.status.clone()
            }
        }
        "UPGRADING" => "SUCCEEDED".to_string(),
        other => other.to_string(),
    }
}

/// Persist an analysis's effective status before acting on it.
fn settle(state: &mut GlueState, analysis_id: &str) {
    let Some(a) = state.job_upgrade_analyses.get(analysis_id) else {
        return;
    };
    let status = effective_status(state, a);
    if let Some(a) = state.job_upgrade_analyses.get_mut(analysis_id) {
        a.status = status;
    }
}

/// Start an auto-upgrade analysis for a job that just switched to `auto`.
pub(crate) fn begin_auto_upgrade(
    state: &mut GlueState,
    job_name: &str,
    source_version: Option<&str>,
) -> String {
    let analysis_id = format!("ja-{}", uuid::Uuid::new_v4());
    state.job_upgrade_analyses.insert(
        analysis_id.clone(),
        JobUpgradeAnalysis {
            analysis_id: analysis_id.clone(),
            job_name: job_name.to_string(),
            status: "OBSERVING".to_string(),
            source_version: source_version.filter(|v| *v != "auto").map(str::to_owned),
            target_version: AUTO_UPGRADE_TARGET_VERSION.to_string(),
            started_on: Utc::now(),
            error_message: None,
        },
    );
    analysis_id
}

/// A job left `auto`: its in-progress analysis stops.
pub(crate) fn end_auto_upgrade(state: &mut GlueState, analysis_id: &str) {
    settle(state, analysis_id);
    if let Some(a) = state.job_upgrade_analyses.get_mut(analysis_id) {
        if !TERMINAL.contains(&a.status.as_str()) {
            a.status = "STOPPED".to_string();
        }
    }
}

/// `Job.AutoUpgradeMetadata` for an `auto` job; `None` for any other job.
pub(crate) fn auto_upgrade_metadata(state: &GlueState, job: &Job) -> Option<Value> {
    if job.glue_version.as_deref() != Some("auto") {
        return None;
    }
    let a = state
        .job_upgrade_analyses
        .get(job.auto_upgrade_analysis_id.as_deref()?)?;
    let status = match effective_status(state, a).as_str() {
        "SUCCEEDED" => "UPGRADE_SUCCEEDED".to_string(),
        "FAILED" | "TIMEOUT" => "UPGRADE_FAILED".to_string(),
        other => other.to_string(),
    };
    let mut v = json!({
        "TargetVersion": a.target_version,
        "Status": status,
        "AnalysisId": a.analysis_id,
    });
    if let Some(s) = &a.source_version {
        v["SourceVersion"] = json!(s);
    }
    Some(v)
}

/// The Glue version an `auto` job's runs use: the target version once the
/// upgrade succeeded, else the version it ran before (or the default).
pub(crate) fn resolved_run_version(state: &GlueState, job: &Job, default: &str) -> String {
    let Some(a) = job
        .auto_upgrade_analysis_id
        .as_deref()
        .and_then(|id| state.job_upgrade_analyses.get(id))
    else {
        return default.to_string();
    };
    if effective_status(state, a) == "SUCCEEDED" {
        a.target_version.clone()
    } else {
        a.source_version
            .clone()
            .unwrap_or_else(|| default.to_string())
    }
}

fn validate_analysis_id(id: &str) -> Result<(), AwsServiceError> {
    let ok = id.strip_prefix("ja-").is_some_and(|rest| {
        let groups: Vec<&str> = rest.split('-').collect();
        groups.len() == 5
            && groups.iter().zip([8, 4, 4, 4, 12]).all(|(g, n)| {
                g.len() == n
                    && g.bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
    });
    if ok {
        Ok(())
    } else {
        Err(invalid_input(format!(
            "analysisId {id} is not a valid analysis id."
        )))
    }
}

/// The job's analysis `analysis_id`, after checking both exist and belong
/// together.
fn job_analysis<'a>(
    state: &'a GlueState,
    job_name: &str,
    analysis_id: &str,
) -> Result<&'a JobUpgradeAnalysis, AwsServiceError> {
    if !state.jobs.contains_key(job_name) {
        return Err(entity_not_found(format!("Job {job_name} not found")));
    }
    state
        .job_upgrade_analyses
        .get(analysis_id)
        .filter(|a| a.job_name == job_name)
        .ok_or_else(|| {
            entity_not_found(format!(
                "Upgrade analysis {analysis_id} not found for job {job_name}"
            ))
        })
}

impl GlueService {
    pub(crate) fn get_job_upgrade_analysis(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let job_name = req_str(&body, "jobName")?;
        let analysis_id = req_str(&body, "analysisId")?;
        validate_analysis_id(analysis_id)?;
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(|| entity_not_found(format!("Job {job_name} not found")))?;
        let a = job_analysis(state, job_name, analysis_id)?;
        let mut out = json!({
            "jobName": a.job_name,
            "analysisId": a.analysis_id,
            "status": effective_status(state, a),
            "targetVersion": a.target_version,
            "analysisType": "AUTO_UPGRADE",
        });
        if let Some(s) = &a.source_version {
            out["sourceVersion"] = json!(s);
        }
        if let Some(m) = &a.error_message {
            out["errorMessage"] = json!(m);
        }
        Ok(AwsResponse::ok_json(out))
    }

    pub(crate) fn start_job_upgrade_analysis(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let job_name = req_str(&body, "jobName")?.to_string();
        let consent = body["upgradeConsent"].as_str().unwrap_or("APPROVE");
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        let job = state
            .jobs
            .get(&job_name)
            .ok_or_else(|| entity_not_found(format!("Job {job_name} not found")))?;
        let analysis_id = match body["analysisId"].as_str() {
            Some(id) => {
                validate_analysis_id(id)?;
                id.to_string()
            }
            None => job.auto_upgrade_analysis_id.clone().ok_or_else(|| {
                entity_not_found(format!("Job {job_name} has no upgrade analysis"))
            })?,
        };
        job_analysis(state, &job_name, &analysis_id)?;
        settle(state, &analysis_id);
        let a = state
            .job_upgrade_analyses
            .get_mut(&analysis_id)
            .expect("analysis checked above");
        if a.status != "WAITING_FOR_CONSENT" {
            return Err(invalid_input(format!(
                "Upgrade analysis {analysis_id} is {}; only an analysis in WAITING_FOR_CONSENT can be resumed.",
                a.status
            )));
        }
        a.status = if consent == "DECLINE" {
            "STOPPED"
        } else {
            "UPGRADING"
        }
        .to_string();
        Ok(AwsResponse::ok_json(json!({
            "analysisId": analysis_id,
            "jobName": job_name,
        })))
    }

    pub(crate) fn stop_job_upgrade_analysis(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let job_name = req_str(&body, "jobName")?.to_string();
        let analysis_id = req_str(&body, "analysisId")?.to_string();
        validate_analysis_id(&analysis_id)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id, &req.region);
        job_analysis(state, &job_name, &analysis_id)?;
        settle(state, &analysis_id);
        let a = state
            .job_upgrade_analyses
            .get_mut(&analysis_id)
            .expect("analysis checked above");
        if TERMINAL.contains(&a.status.as_str()) {
            return Err(invalid_input(format!(
                "Upgrade analysis {analysis_id} is already {}.",
                a.status
            )));
        }
        a.status = "STOPPED".to_string();
        Ok(AwsResponse::ok_json(json!({})))
    }
}
