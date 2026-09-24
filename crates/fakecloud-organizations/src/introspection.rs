//! Organizations admin introspection helpers consumed by
//! `/_fakecloud/organizations/*` routes.
//!
//! These read the in-memory org state and produce assertion-friendly
//! rows. They intentionally bypass IAM — admin endpoints never
//! authenticate. The org state is `Option`-wrapped (no org until one is
//! created), so a `None` state yields an empty list.

use chrono::{DateTime, Utc};

use crate::state::{ResponsibilityTransfer, SharedOrganizationsState};

/// One billing-responsibility transfer flattened for introspection.
#[derive(Debug, Clone)]
pub struct ResponsibilityTransferRow {
    /// The organization that holds the transfer. The listing spans every
    /// organization, so without this a reader cannot tell them apart.
    pub organization_id: String,
    pub id: String,
    pub arn: String,
    pub name: String,
    pub transfer_type: String,
    pub status: String,
    /// `INBOUND` / `OUTBOUND`, relative to this row's `organization_id`.
    /// A transfer is recorded once, in the source organization, so every
    /// row reads `OUTBOUND`; the target organization reads the same
    /// transfer as inbound through `ListInboundResponsibilityTransfers`,
    /// which resolves by party rather than by this field.
    pub direction: String,
    pub source_management_account_id: String,
    pub source_management_account_email: String,
    pub target_management_account_id: String,
    pub target_management_account_email: String,
    pub start_timestamp: DateTime<Utc>,
    pub end_timestamp: Option<DateTime<Utc>>,
    pub active_handshake_id: Option<String>,
}

fn transfer_to_row(org_id: &str, t: &ResponsibilityTransfer) -> ResponsibilityTransferRow {
    ResponsibilityTransferRow {
        organization_id: org_id.to_string(),
        id: t.id.clone(),
        arn: t.arn.clone(),
        name: t.name.clone(),
        transfer_type: t.transfer_type.clone(),
        status: t.status.clone(),
        direction: t.direction.clone(),
        source_management_account_id: t.source_management_account_id.clone(),
        source_management_account_email: t.source_management_account_email.clone(),
        target_management_account_id: t.target_management_account_id.clone(),
        target_management_account_email: t.target_management_account_email.clone(),
        start_timestamp: t.start_timestamp,
        end_timestamp: t.end_timestamp,
        active_handshake_id: t.active_handshake_id.clone(),
    }
}

/// List every billing-responsibility transfer across every organization,
/// sorted by id. Empty when no organization has been created.
pub fn list_all_responsibility_transfers(
    state: &SharedOrganizationsState,
) -> Vec<ResponsibilityTransferRow> {
    // Expire past-due handshakes first, exactly as the Organizations API
    // does on every call. Without this the route answered `REQUESTED`
    // with a live `activeHandshakeId` for a transfer that
    // `DescribeResponsibilityTransfer` already reported as `EXPIRED` --
    // one object, two contradictory answers, for as long as no API call
    // happened to arrive.
    let now = Utc::now();
    if state.read().has_stale_handshakes(now) {
        state.write().expire_stale_handshakes(now);
    }
    let guard = state.read();
    let mut rows: Vec<ResponsibilityTransferRow> = guard
        .iter()
        .flat_map(|org| {
            org.responsibility_transfers
                .values()
                .map(|t| transfer_to_row(&org.org_id, t))
        })
        .collect();
    rows.sort_by(|a, b| a.id.cmp(&b.id));
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn empty_when_no_org() {
        let state: SharedOrganizationsState = Arc::new(parking_lot::RwLock::new(
            crate::state::OrganizationsRegistry::default(),
        ));
        assert!(list_all_responsibility_transfers(&state).is_empty());
    }

    #[test]
    fn lists_transfers_sorted_by_id() {
        let mut org = crate::state::OrganizationState::bootstrap("111111111111");
        for id in ["rt-b", "rt-a"] {
            org.responsibility_transfers.insert(
                id.to_string(),
                ResponsibilityTransfer {
                    id: id.to_string(),
                    arn: format!(
                        "arn:aws:organizations::111111111111:responsibility_transfer/{id}"
                    ),
                    name: id.to_string(),
                    transfer_type: "BILLING".to_string(),
                    status: "REQUESTED".to_string(),
                    direction: "OUTBOUND".to_string(),
                    source_management_account_id: "111111111111".to_string(),
                    source_management_account_email: "src@example.com".to_string(),
                    target_management_account_id: "222222222222".to_string(),
                    target_management_account_email: "dst@example.com".to_string(),
                    start_timestamp: Utc::now(),
                    end_timestamp: None,
                    active_handshake_id: Some("h-1".to_string()),
                },
            );
        }
        let state: SharedOrganizationsState = Arc::new(parking_lot::RwLock::new(org.into()));
        let rows = list_all_responsibility_transfers(&state);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].id, "rt-a");
        assert_eq!(rows[1].id, "rt-b");
        assert_eq!(rows[0].direction, "OUTBOUND");
    }
}
