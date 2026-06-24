use std::sync::Arc;

use chrono::Utc;

use fakecloud_core::delivery::DeliveryBus;

use crate::state::{SecretVersion, SharedSecretsManagerState};

/// Check all secrets for due rotations and trigger them.
///
/// For each secret with `rotation_enabled == true`, checks whether
/// `last_rotated_at + rotation_days <= now`. If so, performs the same
/// rotation logic as `RotateSecret`: creates an AWSPENDING version and
/// invokes the rotation Lambda through all four steps.
///
/// Returns the list of secret names that were rotated.
///
/// `snapshot_store`, when present, is written through after any rotation so the
/// new AWSCURRENT version survives a restart. A scheduled rotation mutates
/// secret state directly here, outside the normal action-dispatch path that is
/// otherwise the only thing that snapshots -- without this the secret reverts to
/// its pre-rotation value after a restart (bug-audit 2026-06-20, 0.A3).
pub async fn check_and_rotate(
    state: &SharedSecretsManagerState,
    delivery_bus: Option<&Arc<DeliveryBus>>,
    snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>>,
) -> Vec<String> {
    let now = Utc::now();
    let mut rotated = Vec::new();

    // Collect secrets that need rotation while holding the lock briefly.
    let due_secrets: Vec<DueSecret> = {
        let accounts = state.read();
        accounts
            .iter()
            .flat_map(|(_, acct)| acct.secrets.values())
            .filter_map(|secret| {
                if secret.deleted {
                    return None;
                }
                if secret.rotation_enabled != Some(true) {
                    return None;
                }
                let rules = secret.rotation_rules.as_ref()?;
                let days = rules.automatically_after_days?;
                let last = secret.last_rotated_at?;
                let due_at = last + chrono::Duration::days(days);
                if now < due_at {
                    return None;
                }
                Some(DueSecret {
                    name: secret.name.clone(),
                    arn: secret.arn.clone(),
                    lambda_arn: secret.rotation_lambda_arn.clone(),
                })
            })
            .collect()
    };

    // Now perform rotation for each due secret.
    for due in due_secrets {
        let version_id = uuid::Uuid::new_v4().to_string();

        // Mutate state: create pending version, update timestamps
        let (invocation, version_created) = {
            let mut accounts = state.write();
            // Find the account that owns this secret by ARN prefix
            let account_id = due.arn.split(':').nth(4).unwrap_or("").to_string();
            let acct = match accounts.get_mut(&account_id) {
                Some(a) => a,
                None => continue,
            };
            let secret = match acct.secrets.get_mut(&due.name) {
                Some(s) => s,
                None => continue,
            };

            secret.last_rotated_at = Some(now);
            secret.last_changed_at = now;

            // Get current value to clone into pending version
            let current_value = secret
                .current_version_id
                .as_ref()
                .and_then(|vid| secret.versions.get(vid))
                .cloned();

            let mut version_created = false;

            if let Some(cv) = current_value {
                if due.lambda_arn.is_some() {
                    // With Lambda: create AWSPENDING version
                    let version = SecretVersion {
                        version_id: version_id.clone(),
                        secret_string: cv.secret_string.clone(),
                        secret_binary: cv.secret_binary.clone(),
                        stages: vec!["AWSPENDING".to_string()],
                        created_at: now,
                    };
                    secret.versions.insert(version_id.clone(), version);
                } else {
                    // Without Lambda: simple rotation
                    if let Some(old_vid) = secret.current_version_id.clone() {
                        if let Some(old_v) = secret.versions.get_mut(&old_vid) {
                            old_v.stages.retain(|s| s != "AWSCURRENT");
                            if !old_v.stages.contains(&"AWSPREVIOUS".to_string()) {
                                old_v.stages.push("AWSPREVIOUS".to_string());
                            }
                        }
                    }
                    let version = SecretVersion {
                        version_id: version_id.clone(),
                        secret_string: cv.secret_string.clone(),
                        secret_binary: cv.secret_binary.clone(),
                        stages: vec!["AWSCURRENT".to_string()],
                        created_at: now,
                    };
                    secret.versions.insert(version_id.clone(), version);
                    secret.current_version_id = Some(version_id.clone());
                }
                version_created = true;
            }

            let invocation = if version_created {
                due.lambda_arn.as_ref().map(|arn| RotationInvocation {
                    lambda_arn: arn.clone(),
                    secret_arn: due.arn.clone(),
                    client_request_token: version_id.clone(),
                })
            } else {
                None
            };

            (invocation, version_created)
        };

        // Invoke Lambda outside the lock
        if let Some(inv) = invocation {
            if let Some(bus) = delivery_bus {
                for step in &["createSecret", "setSecret", "testSecret", "finishSecret"] {
                    let payload = serde_json::json!({
                        "SecretId": inv.secret_arn,
                        "ClientRequestToken": inv.client_request_token,
                        "Step": step,
                    });
                    let payload_str = payload.to_string();
                    match bus.invoke_lambda(&inv.lambda_arn, &payload_str).await {
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            tracing::warn!(
                                step = step,
                                error = %e,
                                "scheduled rotation Lambda invocation failed"
                            );
                        }
                        None => {
                            tracing::warn!(
                                lambda_arn = %inv.lambda_arn,
                                step = step,
                                "rotation Lambda delivery not configured; skipped"
                            );
                            break;
                        }
                    }
                }
            }
        }

        if version_created {
            rotated.push(due.name);
        }
    }

    // Write the rotated state through to disk. The snapshot file is written
    // atomically (temp + rename), so a fresh local lock is sufficient to guard
    // the in-memory clone for this one persist.
    if !rotated.is_empty() {
        let lock = tokio::sync::Mutex::new(());
        crate::service::save_secretsmanager_snapshot(state, snapshot_store, &lock).await;
    }

    rotated
}

struct DueSecret {
    name: String,
    arn: String,
    lambda_arn: Option<String>,
}

struct RotationInvocation {
    lambda_arn: String,
    secret_arn: String,
    client_request_token: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::*;
    use chrono::Duration;
    use parking_lot::RwLock;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    fn make_state() -> SharedSecretsManagerState {
        Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(
                "123456789012",
                "us-east-1",
                "http://localhost:4566",
            ),
        ))
    }

    fn make_secret(
        name: &str,
        rotation_enabled: bool,
        days: Option<i64>,
        last_rotated_ago_days: Option<i64>,
    ) -> Secret {
        let now = Utc::now();
        let last_rotated = last_rotated_ago_days.map(|d| now - Duration::days(d));
        let version_id = "v1".to_string();

        let mut versions = BTreeMap::new();
        versions.insert(
            version_id.clone(),
            SecretVersion {
                version_id: version_id.clone(),
                secret_string: Some("secret-value".to_string()),
                secret_binary: None,
                stages: vec!["AWSCURRENT".to_string()],
                created_at: now,
            },
        );

        Secret {
            name: name.to_string(),
            arn: format!(
                "arn:aws:secretsmanager:us-east-1:123456789012:secret:{}",
                name
            ),
            description: None,
            kms_key_id: None,
            versions,
            current_version_id: Some(version_id),
            tags: vec![],
            tags_ever_set: false,
            deleted: false,
            deletion_date: None,
            created_at: now,
            last_changed_at: now,
            last_accessed_at: None,
            rotation_enabled: Some(rotation_enabled),
            rotation_lambda_arn: None, // no Lambda for unit tests
            rotation_rules: days.map(|d| RotationRules {
                automatically_after_days: Some(d),
                duration: None,
                schedule_expression: None,
            }),
            last_rotated_at: last_rotated,
            resource_policy: None,
            replica_regions: Vec::new(),
        }
    }

    #[tokio::test]
    async fn rotation_due_triggers_rotation() {
        let state = make_state();
        // Rotation enabled, 1 day interval, last rotated 2 days ago → due
        let secret = make_secret("due-secret", true, Some(1), Some(2));
        state
            .write()
            .default_mut()
            .secrets
            .insert("due-secret".to_string(), secret);

        let rotated = check_and_rotate(&state, None, None).await;
        assert_eq!(rotated, vec!["due-secret"]);

        // Verify a new version was created (simple rotation without Lambda)
        let _accts = state.read();
        let s = _accts.default_ref();
        let secret = &s.secrets["due-secret"];
        assert!(secret.versions.len() > 1, "new version should be created");
    }

    #[tokio::test]
    async fn rotation_not_due_skipped() {
        let state = make_state();
        // Rotation enabled, 30 day interval, last rotated 1 day ago → not due
        let secret = make_secret("not-due", true, Some(30), Some(1));
        state
            .write()
            .default_mut()
            .secrets
            .insert("not-due".to_string(), secret);

        let rotated = check_and_rotate(&state, None, None).await;
        assert!(rotated.is_empty());
    }

    #[tokio::test]
    async fn rotation_disabled_skipped() {
        let state = make_state();
        let secret = make_secret("disabled", false, Some(1), Some(2));
        state
            .write()
            .default_mut()
            .secrets
            .insert("disabled".to_string(), secret);

        let rotated = check_and_rotate(&state, None, None).await;
        assert!(rotated.is_empty());
    }

    #[tokio::test]
    async fn rotation_without_rules_skipped() {
        let state = make_state();
        let secret = make_secret("no-rules", true, None, Some(2));
        state
            .write()
            .default_mut()
            .secrets
            .insert("no-rules".to_string(), secret);

        let rotated = check_and_rotate(&state, None, None).await;
        assert!(rotated.is_empty());
    }

    #[tokio::test]
    async fn rotation_without_last_rotated_skipped() {
        let state = make_state();
        let secret = make_secret("no-last", true, Some(1), None);
        state
            .write()
            .default_mut()
            .secrets
            .insert("no-last".to_string(), secret);

        let rotated = check_and_rotate(&state, None, None).await;
        assert!(rotated.is_empty());
    }

    #[tokio::test]
    async fn deleted_secret_skipped() {
        let state = make_state();
        let mut secret = make_secret("deleted", true, Some(1), Some(2));
        secret.deleted = true;
        state
            .write()
            .default_mut()
            .secrets
            .insert("deleted".to_string(), secret);

        let rotated = check_and_rotate(&state, None, None).await;
        assert!(rotated.is_empty());
    }

    /// A SnapshotStore that records the last bytes saved, so a test can assert
    /// the rotation wrote through (the real MemorySnapshotStore is a no-op).
    #[derive(Default)]
    struct RecordingStore {
        bytes: std::sync::Mutex<Option<Vec<u8>>>,
    }
    impl fakecloud_persistence::SnapshotStore for RecordingStore {
        fn load(&self) -> std::io::Result<Option<Vec<u8>>> {
            Ok(self.bytes.lock().unwrap().clone())
        }
        fn save(&self, bytes: &[u8]) -> std::io::Result<()> {
            *self.bytes.lock().unwrap() = Some(bytes.to_vec());
            Ok(())
        }
    }

    #[tokio::test]
    async fn rotation_persists_through_snapshot_store() {
        // A scheduled (no-Lambda) rotation mutates secret state directly. Without
        // write-through the new AWSCURRENT version is lost on restart and the
        // secret reverts to its old value (bug-audit 2026-06-20, 0.A3).
        let state = make_state();
        let secret = make_secret("due-secret", true, Some(1), Some(2));
        let original_vid = secret.current_version_id.clone();
        state
            .write()
            .default_mut()
            .secrets
            .insert("due-secret".to_string(), secret);

        let store = Arc::new(RecordingStore::default());
        let rotated = check_and_rotate(
            &state,
            None,
            Some(store.clone() as Arc<dyn fakecloud_persistence::SnapshotStore>),
        )
        .await;
        assert_eq!(rotated, vec!["due-secret"]);

        // The rotated state was written through, so a reload sees the new
        // AWSCURRENT version, not the pre-rotation one.
        let bytes = fakecloud_persistence::SnapshotStore::load(store.as_ref())
            .unwrap()
            .expect("rotation must persist a snapshot");
        let snap: crate::SecretsManagerSnapshot = serde_json::from_slice(&bytes).unwrap();
        let accounts = snap.accounts.expect("multi-account snapshot");
        let persisted = &accounts.default_ref().secrets["due-secret"];
        assert_ne!(
            persisted.current_version_id, original_vid,
            "persisted snapshot must hold the rotated version"
        );
    }
}
