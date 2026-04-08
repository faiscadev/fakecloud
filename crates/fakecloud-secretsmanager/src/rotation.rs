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
pub async fn check_and_rotate(
    state: &SharedSecretsManagerState,
    delivery_bus: Option<&Arc<DeliveryBus>>,
) -> Vec<String> {
    let now = Utc::now();
    let mut rotated = Vec::new();

    // Collect secrets that need rotation while holding the lock briefly.
    let due_secrets: Vec<DueSecret> = {
        let state = state.read();
        state
            .secrets
            .values()
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
        let invocation = {
            let mut state = state.write();
            let secret = match state.secrets.get_mut(&due.name) {
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
            }

            due.lambda_arn.as_ref().map(|arn| RotationInvocation {
                lambda_arn: arn.clone(),
                secret_arn: due.arn.clone(),
                client_request_token: version_id.clone(),
            })
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

        rotated.push(due.name);
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
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_state() -> SharedSecretsManagerState {
        Arc::new(RwLock::new(SecretsManagerState::new(
            "123456789012",
            "us-east-1",
        )))
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

        let mut versions = HashMap::new();
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
            }),
            last_rotated_at: last_rotated,
            resource_policy: None,
        }
    }

    #[tokio::test]
    async fn rotation_due_triggers_rotation() {
        let state = make_state();
        // Rotation enabled, 1 day interval, last rotated 2 days ago → due
        let secret = make_secret("due-secret", true, Some(1), Some(2));
        state
            .write()
            .secrets
            .insert("due-secret".to_string(), secret);

        let rotated = check_and_rotate(&state, None).await;
        assert_eq!(rotated, vec!["due-secret"]);

        // Verify a new version was created (simple rotation without Lambda)
        let s = state.read();
        let secret = &s.secrets["due-secret"];
        assert!(secret.versions.len() > 1, "new version should be created");
    }

    #[tokio::test]
    async fn rotation_not_due_skipped() {
        let state = make_state();
        // Rotation enabled, 30 day interval, last rotated 1 day ago → not due
        let secret = make_secret("not-due", true, Some(30), Some(1));
        state.write().secrets.insert("not-due".to_string(), secret);

        let rotated = check_and_rotate(&state, None).await;
        assert!(rotated.is_empty());
    }

    #[tokio::test]
    async fn rotation_disabled_skipped() {
        let state = make_state();
        let secret = make_secret("disabled", false, Some(1), Some(2));
        state.write().secrets.insert("disabled".to_string(), secret);

        let rotated = check_and_rotate(&state, None).await;
        assert!(rotated.is_empty());
    }

    #[tokio::test]
    async fn rotation_without_rules_skipped() {
        let state = make_state();
        let secret = make_secret("no-rules", true, None, Some(2));
        state.write().secrets.insert("no-rules".to_string(), secret);

        let rotated = check_and_rotate(&state, None).await;
        assert!(rotated.is_empty());
    }

    #[tokio::test]
    async fn rotation_without_last_rotated_skipped() {
        let state = make_state();
        let secret = make_secret("no-last", true, Some(1), None);
        state.write().secrets.insert("no-last".to_string(), secret);

        let rotated = check_and_rotate(&state, None).await;
        assert!(rotated.is_empty());
    }

    #[tokio::test]
    async fn deleted_secret_skipped() {
        let state = make_state();
        let mut secret = make_secret("deleted", true, Some(1), Some(2));
        secret.deleted = true;
        state.write().secrets.insert("deleted".to_string(), secret);

        let rotated = check_and_rotate(&state, None).await;
        assert!(rotated.is_empty());
    }
}
