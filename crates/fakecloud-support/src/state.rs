//! Account-partitioned, serializable state for AWS Support (`support`).
//!
//! Cases are stored as their already-output-valid `CaseDetails` wire object
//! (`serde_json::Value`, camelCase members matching the awsJson1_1 member
//! names) so a `DescribeCases` echoes exactly what `CreateCase` persisted. The
//! per-case communication thread lives in a side map keyed by case id, and
//! attachment sets / individual attachments live in their own maps so
//! `DescribeAttachment` has a single source of truth.
//!
//! The Trusted Advisor refresh status is a per-check state machine
//! (`none -> enqueued -> processing -> success`) stored in `ta_refresh`;
//! `RefreshTrustedAdvisorCheck` enqueues and each
//! `DescribeTrustedAdvisorCheckRefreshStatuses` read advances it one step.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const SUPPORT_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// Per-account AWS Support state.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SupportData {
    /// The account id this partition belongs to (for id synthesis).
    #[serde(default)]
    pub account_id: String,
    /// The region this partition belongs to.
    #[serde(default)]
    pub region: String,

    /// Support cases keyed by `caseId`; each value is the `CaseDetails` wire
    /// object.
    #[serde(default)]
    pub cases: BTreeMap<String, Value>,
    /// Per-case communication thread keyed by `caseId`; each entry is a
    /// `Communication` wire object, newest last.
    #[serde(default)]
    pub communications: BTreeMap<String, Vec<Value>>,

    /// Attachment sets keyed by `attachmentSetId`. Each value carries
    /// `expiryTime` and the list of member `attachmentId`s.
    #[serde(default)]
    pub attachment_sets: BTreeMap<String, Value>,
    /// Individual attachments keyed by `attachmentId`; each value carries
    /// `fileName` and base64 `data` (the `Attachment` wire object).
    #[serde(default)]
    pub attachments: BTreeMap<String, Value>,

    /// Trusted Advisor per-check refresh status keyed by `checkId`; one of
    /// `none` / `enqueued` / `processing` / `success`.
    #[serde(default)]
    pub ta_refresh: BTreeMap<String, String>,
}

impl AccountState for SupportData {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            ..Default::default()
        }
    }
}

impl SupportData {
    /// There is no timer-driven lifecycle to resume: the Trusted Advisor
    /// refresh state machine advances only on an explicit read, so a restart
    /// leaves it exactly as persisted. Kept for symmetry with the other
    /// services' persistence hook. Always returns `false` (nothing settled).
    pub fn reconcile(&mut self) -> bool {
        false
    }

    /// Advance one check's refresh status one step toward `success`, returning
    /// the resulting status. `none`/absent stays `none` until an explicit
    /// refresh enqueues it.
    pub fn advance_refresh(&mut self, check_id: &str) -> String {
        let cur = self
            .ta_refresh
            .get(check_id)
            .cloned()
            .unwrap_or_else(|| "none".to_string());
        let next = match cur.as_str() {
            "enqueued" => "processing",
            "processing" => "success",
            other => other,
        };
        self.ta_refresh
            .insert(check_id.to_string(), next.to_string());
        next.to_string()
    }
}

pub type SharedSupportState = Arc<RwLock<MultiAccountState<SupportData>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct SupportSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<SupportData>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data() -> SupportData {
        SupportData::new_for_account("000000000000", "us-east-1", "")
    }

    #[test]
    fn refresh_advances_through_states() {
        let mut d = data();
        // Unrefreshed check stays none.
        assert_eq!(d.advance_refresh("Qch7DwouX1"), "none");
        // Enqueue, then each read advances one step.
        d.ta_refresh.insert("Qch7DwouX1".into(), "enqueued".into());
        assert_eq!(d.advance_refresh("Qch7DwouX1"), "processing");
        assert_eq!(d.advance_refresh("Qch7DwouX1"), "success");
        // Terminal: stays success.
        assert_eq!(d.advance_refresh("Qch7DwouX1"), "success");
    }

    #[test]
    fn reconcile_is_noop() {
        let mut d = data();
        assert!(!d.reconcile());
    }
}
