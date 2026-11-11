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
    kms_hook: Option<&dyn fakecloud_core::delivery::KmsHook>,
    snapshot_store: Option<Arc<dyn fakecloud_persistence::SnapshotStore>>,
) -> Vec<String> {
    let now = Utc::now();
    let mut rotated = Vec::new();
    let mut failed: Vec<String> = Vec::new();

    // Collect secrets that need rotation while holding the lock briefly.
    let due_secrets: Vec<DueSecret> = {
        let accounts = state.read();
        accounts
            .iter_regional()
            .flat_map(|(_, _, st)| st.secrets.values())
            .filter_map(|secret| {
                // A replica rotates with its primary (in the primary's
                // region), never on its own.
                if secret.deleted || secret.primary_region.is_some() {
                    return None;
                }
                if secret.rotation_enabled != Some(true) {
                    return None;
                }
                let rules = secret.rotation_rules.as_ref()?;
                let days = rules.automatically_after_days?;
                // A failed scheduled attempt waits a whole interval, as
                // a rotation does: the next attempt is the next window.
                let last = match (secret.last_rotated_at?, secret.rotation_failed_at) {
                    (rotated, Some(failed)) if failed > rotated => failed,
                    (rotated, _) => rotated,
                };
                // Never due if the interval is not representable.
                let due_at =
                    chrono::Duration::try_days(days).and_then(|d| last.checked_add_signed(d))?;
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
            // The account and region that own this secret, from its ARN.
            let secret = match accounts
                .by_arn_mut(&due.arn)
                .and_then(|st| st.secrets.get_mut(&due.name))
            {
                Some(s) => s,
                None => continue,
            };

            // Get current value to clone into pending version
            let current_value = secret
                .current_version_id
                .as_ref()
                .and_then(|vid| secret.versions.get(vid))
                .cloned();

            // The value is re-sealed for the new version first: a value KMS
            // will not open or seal (its key disabled or gone) fails this
            // rotation, which leaves the secret's values and rotation
            // configuration untouched (rotation stays enabled) and records
            // the failed attempt, so the next one comes a whole interval
            // later, at the next scheduled window, not on every check.
            let resealed = match current_value.as_ref().map(|cv| {
                crate::service::reseal_version(
                    kms_hook,
                    fakecloud_aws::arn::account_of(&due.arn).unwrap_or_default(),
                    &due.arn,
                    cv,
                    fakecloud_aws::arn::region_of(&due.arn).unwrap_or_default(),
                    &due.arn,
                    &version_id,
                    secret.kms_key_id.as_deref(),
                )
            }) {
                Some(Err(err)) => {
                    tracing::warn!(
                        secret_arn = %due.arn,
                        error = %err.message(),
                        "scheduled rotation failed: the secret's value could not be re-encrypted; retrying at the next scheduled window"
                    );
                    secret.rotation_failed_at = Some(now);
                    failed.push(due.name.clone());
                    continue;
                }
                Some(Ok(values)) => Some(values),
                None => None,
            };

            secret.last_rotated_at = Some(now);
            secret.rotation_failed_at = None;
            secret.last_changed_at = now;

            let mut version_created = false;

            if let (Some(_cv), Some(values)) = (current_value, resealed) {
                if due.lambda_arn.is_some() {
                    // With Lambda: create AWSPENDING version
                    let version = SecretVersion {
                        version_id: version_id.clone(),
                        secret_string: values.secret_string.clone(),
                        secret_binary: values.secret_binary.clone(),
                        stages: vec!["AWSPENDING".to_string()],
                        created_at: now,
                        kms_key_ids: values.kms_key_ids.clone(),
                    };
                    secret.versions.insert(version_id.clone(), version);
                } else {
                    // Without Lambda: simple rotation. Demote the outgoing
                    // AWSCURRENT to AWSPREVIOUS, keeping AWSPREVIOUS unique.
                    if let Some(old_vid) = secret.current_version_id.clone() {
                        crate::service::demote_current_to_previous(&mut secret.versions, &old_vid);
                    }
                    let version = SecretVersion {
                        version_id: version_id.clone(),
                        secret_string: values.secret_string.clone(),
                        secret_binary: values.secret_binary.clone(),
                        stages: vec!["AWSCURRENT".to_string()],
                        created_at: now,
                        kms_key_ids: values.kms_key_ids.clone(),
                    };
                    secret.versions.insert(version_id.clone(), version);
                    secret.current_version_id = Some(version_id.clone());
                    // Prune deprecated versions (no staging labels) so they
                    // don't leak, matching PutSecretValue / UpdateSecret /
                    // RotateSecret.
                    secret.versions.retain(|_, v| !v.stages.is_empty());
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

            // Carry the new version to the secret's replicas.
            if version_created {
                if let (Some(account), Some(region)) = (
                    fakecloud_aws::arn::account_of(&due.arn),
                    fakecloud_aws::arn::region_of(&due.arn),
                ) {
                    crate::service::sync_replicas(
                        &mut accounts,
                        kms_hook,
                        account,
                        region,
                        &due.name,
                    );
                }
            }

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
                    // A step runs only when the previous one succeeded.
                    match bus.invoke_lambda(&inv.lambda_arn, &payload_str).await {
                        Some(Ok(output)) if output.is_function_error() => {
                            tracing::warn!(
                                step = step,
                                error = %String::from_utf8_lossy(&output.payload),
                                "scheduled rotation Lambda step failed; rotation stopped"
                            );
                            break;
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            tracing::warn!(
                                step = step,
                                error = %e,
                                "scheduled rotation Lambda invocation failed; rotation stopped"
                            );
                            break;
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
    // A failed attempt is persisted too, so a restart does not retry it at
    // once.
    if !rotated.is_empty() || !failed.is_empty() {
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
                kms_key_ids: Vec::new(),
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
            rotation_failed_at: None,
            resource_policy: None,
            replica_regions: Vec::new(),
            replica_settings: BTreeMap::new(),
            primary_region: None,
        }
    }

    #[tokio::test]
    async fn rotation_due_triggers_rotation() {
        let state = make_state();
        // Rotation enabled, 1 day interval, last rotated 2 days ago → due
        let secret = make_secret("due-secret", true, Some(1), Some(2));
        state
            .write()
            .default_regional_mut()
            .secrets
            .insert("due-secret".to_string(), secret);

        let rotated = check_and_rotate(&state, None, None, None).await;
        assert_eq!(rotated, vec!["due-secret"]);

        // Verify a new version was created (simple rotation without Lambda)
        let _accts = state.read();
        let s = _accts.default_regional().unwrap();
        let secret = &s.secrets["due-secret"];
        assert!(secret.versions.len() > 1, "new version should be created");
    }

    /// A KMS hook whose key is disabled, counting decrypt attempts.
    #[derive(Default)]
    struct DisabledKeyKms(std::sync::atomic::AtomicUsize);
    impl fakecloud_core::delivery::KmsHook for DisabledKeyKms {
        fn encrypt(
            &self,
            _account_id: &str,
            _region: &str,
            _key_id: &str,
            _plaintext: &[u8],
            _caller: fakecloud_core::delivery::KmsCaller<'_>,
            _ctx: std::collections::HashMap<String, String>,
        ) -> Result<String, fakecloud_core::delivery::KmsError> {
            unreachable!()
        }
        fn decrypt(
            &self,
            _account_id: &str,
            _ciphertext: &str,
            _caller: fakecloud_core::delivery::KmsCaller<'_>,
            _ctx: std::collections::HashMap<String, String>,
        ) -> Result<Vec<u8>, fakecloud_core::delivery::KmsError> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Err(fakecloud_core::delivery::KmsError::new(
                fakecloud_core::delivery::KmsErrorKind::Disabled,
                "key is disabled",
            ))
        }
        fn is_ciphertext(&self, _account_id: &str, stored: &str) -> bool {
            stored.starts_with("ENC:")
        }
    }

    /// A scheduled rotation whose value KMS will not re-seal fails, leaves
    /// rotation enabled and the value as it was, and waits for the next
    /// scheduled window instead of retrying on every check.
    #[tokio::test]
    async fn failed_scheduled_rotation_waits_for_the_next_window() {
        let state = make_state();
        let mut secret = make_secret("sealed", true, Some(1), Some(2));
        secret.versions.get_mut("v1").unwrap().secret_string = Some("ENC:v".to_string());
        state
            .write()
            .default_regional_mut()
            .secrets
            .insert("sealed".to_string(), secret);
        let kms = DisabledKeyKms::default();

        assert!(check_and_rotate(&state, None, Some(&kms), None)
            .await
            .is_empty());
        assert_eq!(kms.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        {
            let accts = state.read();
            let secret = &accts.default_regional().unwrap().secrets["sealed"];
            assert_eq!(secret.rotation_enabled, Some(true));
            assert!(secret.rotation_failed_at.is_some());
            assert_eq!(secret.versions.len(), 1);
            assert_eq!(secret.current_version_id.as_deref(), Some("v1"));
        }

        // The next check does not try again: the next window is a day out.
        assert!(check_and_rotate(&state, None, Some(&kms), None)
            .await
            .is_empty());
        assert_eq!(kms.0.load(std::sync::atomic::Ordering::SeqCst), 1);

        // Once that window comes, it is attempted again.
        state
            .write()
            .default_regional_mut()
            .secrets
            .get_mut("sealed")
            .unwrap()
            .rotation_failed_at = Some(Utc::now() - Duration::days(2));
        assert!(check_and_rotate(&state, None, Some(&kms), None)
            .await
            .is_empty());
        assert_eq!(kms.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn rotation_not_due_skipped() {
        let state = make_state();
        // Rotation enabled, 30 day interval, last rotated 1 day ago → not due
        let secret = make_secret("not-due", true, Some(30), Some(1));
        state
            .write()
            .default_regional_mut()
            .secrets
            .insert("not-due".to_string(), secret);

        let rotated = check_and_rotate(&state, None, None, None).await;
        assert!(rotated.is_empty());
    }

    #[tokio::test]
    async fn huge_persisted_rotation_interval_is_never_due_and_does_not_panic() {
        let state = make_state();
        let secret = make_secret("huge", true, Some(i64::MAX), Some(1));
        state
            .write()
            .default_regional_mut()
            .secrets
            .insert("huge".to_string(), secret);
        let rotated = check_and_rotate(&state, None, None, None).await;
        assert!(rotated.is_empty());
    }

    #[tokio::test]
    async fn rotation_disabled_skipped() {
        let state = make_state();
        let secret = make_secret("disabled", false, Some(1), Some(2));
        state
            .write()
            .default_regional_mut()
            .secrets
            .insert("disabled".to_string(), secret);

        let rotated = check_and_rotate(&state, None, None, None).await;
        assert!(rotated.is_empty());
    }

    #[tokio::test]
    async fn rotation_without_rules_skipped() {
        let state = make_state();
        let secret = make_secret("no-rules", true, None, Some(2));
        state
            .write()
            .default_regional_mut()
            .secrets
            .insert("no-rules".to_string(), secret);

        let rotated = check_and_rotate(&state, None, None, None).await;
        assert!(rotated.is_empty());
    }

    #[tokio::test]
    async fn rotation_without_last_rotated_skipped() {
        let state = make_state();
        let secret = make_secret("no-last", true, Some(1), None);
        state
            .write()
            .default_regional_mut()
            .secrets
            .insert("no-last".to_string(), secret);

        let rotated = check_and_rotate(&state, None, None, None).await;
        assert!(rotated.is_empty());
    }

    #[tokio::test]
    async fn deleted_secret_skipped() {
        let state = make_state();
        let mut secret = make_secret("deleted", true, Some(1), Some(2));
        secret.deleted = true;
        state
            .write()
            .default_regional_mut()
            .secrets
            .insert("deleted".to_string(), secret);

        let rotated = check_and_rotate(&state, None, None, None).await;
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
            .default_regional_mut()
            .secrets
            .insert("due-secret".to_string(), secret);

        let store = Arc::new(RecordingStore::default());
        let rotated = check_and_rotate(
            &state,
            None,
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
        let persisted = &accounts.default_regional().unwrap().secrets["due-secret"];
        assert_ne!(
            persisted.current_version_id, original_vid,
            "persisted snapshot must hold the rotated version"
        );
    }
}
