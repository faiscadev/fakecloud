//! Background lifecycle advancement for DSQL clusters and streams.
//!
//! AWS reports a freshly created cluster as `CREATING` and only later flips it
//! to `ACTIVE`; a deleted cluster passes through `DELETING` before it vanishes.
//! Streams follow the same `CREATING -> ACTIVE` / `DELETING -> DELETED` shape.
//! This ticker mirrors that observable behavior so clients that poll `GetCluster`
//! until `ACTIVE` (the documented pattern, and what the Terraform/SDK waiters do)
//! actually converge, instead of seeing a cluster stuck in `CREATING` forever.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use tokio::sync::Mutex as AsyncMutex;

use fakecloud_persistence::SnapshotStore;

use crate::persistence::save_snapshot;
use crate::state::SharedDsqlState;

/// How long a cluster/stream lingers in a transient state before advancing.
/// Short enough that tests and interactive use converge quickly, long enough
/// that a `CREATING` status is actually observable on the create response.
const TRANSITION_AFTER: chrono::Duration = chrono::Duration::milliseconds(400);

/// Poll interval for the advancement loop.
const TICK: Duration = Duration::from_millis(200);

/// Spawn the lifecycle loop. Advances transient statuses and removes clusters
/// that have finished deleting, persisting after any change.
pub fn spawn(
    state: SharedDsqlState,
    store: Option<Arc<dyn SnapshotStore>>,
    lock: Arc<AsyncMutex<()>>,
) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(TICK).await;
            let changed = advance(&state);
            if changed {
                save_snapshot(&state, store.clone(), &lock).await;
            }
        }
    });
}

/// Advance every account's clusters/streams one step. Returns whether anything
/// changed (so the caller only persists on real transitions).
pub fn advance(state: &SharedDsqlState) -> bool {
    let now = Utc::now();
    let mut changed = false;
    let mut accounts = state.write();
    for (_id, acct) in accounts.iter_mut() {
        let mut to_remove = Vec::new();
        for (cid, cluster) in acct.clusters.iter_mut() {
            let elapsed = now.signed_duration_since(cluster.creation_time);
            match cluster.status.as_str() {
                "CREATING" | "PENDING_SETUP" if elapsed >= TRANSITION_AFTER => {
                    cluster.status = "ACTIVE".to_string();
                    changed = true;
                }
                "DELETING" | "PENDING_DELETE" => {
                    // DeleteCluster stamped `deleted_at`; remove only once the
                    // grace window has elapsed, so the DELETING state is
                    // observable and persisted (not gone on the next tick).
                    let deleting_for = cluster
                        .deleted_at
                        .map(|t| now.signed_duration_since(t))
                        .unwrap_or(TRANSITION_AFTER);
                    if deleting_for >= TRANSITION_AFTER {
                        to_remove.push(cid.clone());
                        changed = true;
                    }
                }
                _ => {}
            }
            // Advance / reap streams within the cluster.
            let mut streams_to_remove = Vec::new();
            for (sid, stream) in cluster.streams.iter_mut() {
                match stream.status.as_str() {
                    "CREATING"
                        if now.signed_duration_since(stream.creation_time) >= TRANSITION_AFTER =>
                    {
                        stream.status = "ACTIVE".to_string();
                        changed = true;
                    }
                    "DELETING" => {
                        let deleting_for = stream
                            .deleted_at
                            .map(|t| now.signed_duration_since(t))
                            .unwrap_or(TRANSITION_AFTER);
                        if deleting_for >= TRANSITION_AFTER {
                            streams_to_remove.push(sid.clone());
                            changed = true;
                        }
                    }
                    _ => {}
                }
            }
            for sid in streams_to_remove {
                cluster.streams.remove(&sid);
            }
        }
        for cid in to_remove {
            acct.clusters.remove(&cid);
        }
    }
    changed
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{cluster_arn, cluster_endpoint, Cluster, EncryptionDetails};
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::collections::BTreeMap;

    fn cluster(status: &str, age_ms: i64) -> Cluster {
        Cluster {
            identifier: "abcdefghij0123456789klmnop".into(),
            arn: cluster_arn("us-east-1", "000000000000", "abcdefghij0123456789klmnop"),
            status: status.into(),
            creation_time: Utc::now() - chrono::Duration::milliseconds(age_ms),
            deletion_protection_enabled: true,
            multi_region_properties: None,
            encryption_details: EncryptionDetails {
                encryption_type: "AWS_OWNED_KMS_KEY".into(),
                kms_key_arn: None,
                encryption_status: "ENABLED".into(),
            },
            endpoint: cluster_endpoint("abcdefghij0123456789klmnop", "us-east-1"),
            tags: BTreeMap::new(),
            policy: None,
            policy_version: 0,
            client_token: None,
            deleted_at: None,
            streams: BTreeMap::new(),
        }
    }

    fn state_with(c: Cluster) -> SharedDsqlState {
        let mut accounts: MultiAccountState<crate::state::DsqlState> =
            MultiAccountState::new("000000000000", "us-east-1", "");
        accounts
            .default_mut()
            .clusters
            .insert(c.identifier.clone(), c);
        Arc::new(RwLock::new(accounts))
    }

    #[test]
    fn creating_advances_to_active_after_window() {
        let state = state_with(cluster("CREATING", 1000));
        assert!(advance(&state));
        assert_eq!(
            state.read().get("000000000000").unwrap().clusters["abcdefghij0123456789klmnop"].status,
            "ACTIVE"
        );
    }

    #[test]
    fn creating_stays_within_window() {
        let state = state_with(cluster("CREATING", 0));
        assert!(!advance(&state));
    }

    #[test]
    fn deleting_removed_after_grace_window() {
        // deleted_at older than the window: reaped this tick.
        let mut c = cluster("DELETING", 0);
        c.deleted_at = Some(Utc::now() - chrono::Duration::milliseconds(1000));
        let state = state_with(c);
        assert!(advance(&state));
        assert!(state
            .read()
            .get("000000000000")
            .unwrap()
            .clusters
            .is_empty());
    }

    #[test]
    fn deleting_survives_within_grace_window() {
        // Just-requested deletion stays observable until the window elapses.
        let mut c = cluster("DELETING", 0);
        c.deleted_at = Some(Utc::now());
        let state = state_with(c);
        assert!(!advance(&state));
        assert_eq!(
            state.read().get("000000000000").unwrap().clusters["abcdefghij0123456789klmnop"].status,
            "DELETING"
        );
    }
}
