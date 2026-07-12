//! Account-partitioned, serializable state for Amazon SWF (`swf`).
//!
//! Everything an SWF caller can create lives here: domains (with their
//! `REGISTERED` / `DEPRECATED` status), registered activity and workflow types
//! (versioned, deprecatable), and workflow executions. Each execution carries
//! its full ordered event history plus the in-flight decision and activity
//! tasks that drive the real decider/worker state machine. Configuration blobs
//! (a type's `default*` timeouts, an execution's timeouts / child policy) are
//! stored as their already-output-valid wire JSON so a `Describe*` echoes
//! exactly what its `Register*` / `Start*` persisted.
//!
//! Types are keyed by `name\u{1}version` (see [`crate::shared::type_key`]);
//! executions by their minted `runId`. Task tokens index back to the owning
//! execution so a `Respond*` / `RecordActivityTaskHeartbeat` resolves in O(1).
//! Tags live in an ARN-keyed side map so `ListTagsForResource` has a single
//! source of truth.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const SWF_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// A registered SWF domain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Domain {
    pub name: String,
    /// `REGISTERED` or `DEPRECATED`.
    pub status: String,
    #[serde(default)]
    pub description: Option<String>,
    /// The retention period in days, stored as the wire string SWF uses.
    pub retention_days: String,
    pub arn: String,
}

/// A registered activity or workflow type.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeRecord {
    pub name: String,
    pub version: String,
    /// `REGISTERED` or `DEPRECATED`.
    pub status: String,
    #[serde(default)]
    pub description: Option<String>,
    pub creation_date: f64,
    #[serde(default)]
    pub deprecation_date: Option<f64>,
    /// The `default*` configuration members, stored as their wire JSON object
    /// so `Describe*Type`'s `configuration` echoes verbatim.
    #[serde(default)]
    pub configuration: Map<String, Value>,
}

/// The lifecycle state of a scheduled/started activity task inside an execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActivityState {
    Scheduled,
    Started,
    Closed,
}

/// One activity task tracked by an execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityRecord {
    pub activity_id: String,
    pub activity_type_name: String,
    pub activity_type_version: String,
    #[serde(default)]
    pub input: Option<String>,
    pub task_list: String,
    pub scheduled_event_id: i64,
    #[serde(default)]
    pub started_event_id: Option<i64>,
    pub state: ActivityState,
    pub task_token: String,
}

/// A workflow execution and everything the state machine needs to drive it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Execution {
    pub domain: String,
    pub workflow_id: String,
    pub run_id: String,
    pub workflow_type_name: String,
    pub workflow_type_version: String,
    pub task_list: String,
    #[serde(default)]
    pub task_priority: Option<String>,
    /// `OPEN` or `CLOSED`.
    pub status: String,
    /// `COMPLETED` / `FAILED` / `CANCELED` / `TERMINATED` / `TIMED_OUT` /
    /// `CONTINUED_AS_NEW` once closed.
    #[serde(default)]
    pub close_status: Option<String>,
    pub start_timestamp: f64,
    #[serde(default)]
    pub close_timestamp: Option<f64>,
    #[serde(default)]
    pub tag_list: Vec<String>,
    #[serde(default)]
    pub input: Option<String>,
    pub child_policy: String,
    pub task_start_to_close_timeout: String,
    pub execution_start_to_close_timeout: String,
    #[serde(default)]
    pub lambda_role: Option<String>,
    #[serde(default)]
    pub cancel_requested: bool,
    #[serde(default)]
    pub latest_execution_context: Option<String>,

    /// The ordered event history, each entry an already-output-valid
    /// `HistoryEvent` wire object.
    #[serde(default)]
    pub events: Vec<Value>,
    /// The next `eventId` to assign (1-based, monotonic).
    pub next_event_id: i64,

    /// A decision task is scheduled and awaiting a poll.
    #[serde(default)]
    pub decision_scheduled: bool,
    /// The `eventId` of the outstanding `DecisionTaskScheduled` event.
    #[serde(default)]
    pub decision_scheduled_event_id: Option<i64>,
    /// The `eventId` of the `DecisionTaskStarted` for an in-flight decision.
    #[serde(default)]
    pub decision_started_event_id: Option<i64>,
    /// The `eventId` of the previous `DecisionTaskStarted` (for the next poll).
    #[serde(default)]
    pub previous_started_event_id: i64,
    /// The task token handed out for the in-flight decision task, if any.
    #[serde(default)]
    pub decision_task_token: Option<String>,

    /// Activity tasks tracked by this execution, in schedule order.
    #[serde(default)]
    pub activities: Vec<ActivityRecord>,
}

/// Per-account Amazon SWF state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SwfData {
    #[serde(default)]
    pub account_id: String,
    #[serde(default)]
    pub region: String,

    /// Domains keyed by name.
    #[serde(default)]
    pub domains: BTreeMap<String, Domain>,
    /// Activity types keyed by `"{domain}\u{1}{name}\u{1}{version}"`.
    #[serde(default)]
    pub activity_types: BTreeMap<String, TypeRecord>,
    /// Workflow types keyed by `"{domain}\u{1}{name}\u{1}{version}"`.
    #[serde(default)]
    pub workflow_types: BTreeMap<String, TypeRecord>,
    /// Workflow executions keyed by `runId`.
    #[serde(default)]
    pub executions: BTreeMap<String, Execution>,
    /// Decision task token -> owning `runId`.
    #[serde(default)]
    pub decision_tokens: BTreeMap<String, String>,
    /// Activity task token -> `(runId, activityId)`.
    #[serde(default)]
    pub activity_tokens: BTreeMap<String, ActivityRef>,
    /// Resource tags keyed by domain ARN.
    #[serde(default)]
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

/// A back-reference from an activity task token to its execution and activity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActivityRef {
    pub run_id: String,
    pub activity_id: String,
}

impl AccountState for SwfData {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            ..Default::default()
        }
    }
}

pub type SharedSwfState = Arc<RwLock<MultiAccountState<SwfData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct SwfSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<SwfData>,
}

impl SwfData {
    /// Composite key for a type record within a domain.
    pub fn type_map_key(domain: &str, name: &str, version: &str) -> String {
        format!("{domain}\u{1}{name}\u{1}{version}")
    }

    /// Find the currently-open execution for a `workflow_id` (SWF allows only
    /// one open execution per workflow id at a time), preferring an exact
    /// `run_id` when supplied.
    pub fn find_execution<'a>(
        &'a self,
        workflow_id: &str,
        run_id: Option<&str>,
    ) -> Option<&'a Execution> {
        if let Some(rid) = run_id.filter(|r| !r.is_empty()) {
            return self
                .executions
                .get(rid)
                .filter(|e| e.workflow_id == workflow_id);
        }
        // Prefer an open execution; fall back to any (most recent by start).
        self.executions
            .values()
            .filter(|e| e.workflow_id == workflow_id)
            .max_by(|a, b| {
                (a.status == "OPEN", a.start_timestamp)
                    .partial_cmp(&(b.status == "OPEN", b.start_timestamp))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    }

    pub fn find_execution_mut<'a>(
        &'a mut self,
        workflow_id: &str,
        run_id: Option<&str>,
    ) -> Option<&'a mut Execution> {
        let rid = match run_id.filter(|r| !r.is_empty()) {
            Some(rid) => Some(rid.to_string()),
            None => self
                .executions
                .values()
                .filter(|e| e.workflow_id == workflow_id)
                .max_by(|a, b| {
                    (a.status == "OPEN", a.start_timestamp)
                        .partial_cmp(&(b.status == "OPEN", b.start_timestamp))
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .map(|e| e.run_id.clone()),
        }?;
        self.executions
            .get_mut(&rid)
            .filter(|e| e.workflow_id == workflow_id)
    }
}

impl Execution {
    /// Allocate the next monotonic event id.
    pub fn alloc_event_id(&mut self) -> i64 {
        let id = self.next_event_id;
        self.next_event_id += 1;
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_execution_prefers_open() {
        let mut d = SwfData::new_for_account("000000000000", "us-east-1", "");
        d.executions
            .insert("run-old".into(), exec("wf-1", "run-old", "CLOSED", 1.0));
        d.executions
            .insert("run-new".into(), exec("wf-1", "run-new", "OPEN", 2.0));
        assert_eq!(d.find_execution("wf-1", None).unwrap().run_id, "run-new");
        assert_eq!(
            d.find_execution("wf-1", Some("run-old")).unwrap().run_id,
            "run-old"
        );
    }

    fn exec(wf: &str, run: &str, status: &str, ts: f64) -> Execution {
        Execution {
            domain: "d".into(),
            workflow_id: wf.into(),
            run_id: run.into(),
            workflow_type_name: "t".into(),
            workflow_type_version: "1".into(),
            task_list: "tl".into(),
            task_priority: None,
            status: status.into(),
            close_status: None,
            start_timestamp: ts,
            close_timestamp: None,
            tag_list: vec![],
            input: None,
            child_policy: "TERMINATE".into(),
            task_start_to_close_timeout: "10".into(),
            execution_start_to_close_timeout: "3600".into(),
            lambda_role: None,
            cancel_requested: false,
            latest_execution_context: None,
            events: vec![],
            next_event_id: 1,
            decision_scheduled: false,
            decision_scheduled_event_id: None,
            decision_started_event_id: None,
            previous_started_event_id: 0,
            decision_task_token: None,
            activities: vec![],
        }
    }
}
