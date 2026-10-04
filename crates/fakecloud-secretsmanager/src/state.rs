use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Secret {
    pub name: String,
    pub arn: String,
    pub description: Option<String>,
    pub kms_key_id: Option<String>,
    pub versions: BTreeMap<String, SecretVersion>,
    pub current_version_id: Option<String>,
    pub tags: Vec<(String, String)>,
    pub tags_ever_set: bool,
    pub deleted: bool,
    pub deletion_date: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub last_changed_at: DateTime<Utc>,
    pub last_accessed_at: Option<DateTime<Utc>>,
    pub rotation_enabled: Option<bool>,
    pub rotation_lambda_arn: Option<String>,
    pub rotation_rules: Option<RotationRules>,
    pub last_rotated_at: Option<DateTime<Utc>>,
    pub resource_policy: Option<String>,
    /// Replica regions added via ReplicateSecretToRegions (or CreateSecret's
    /// AddReplicaRegions), in the order they were added. Set on a primary
    /// secret only; each healthy replica exists as its own [`Secret`] in that
    /// region's state. Reflected in ReplicationStatus.
    #[serde(default)]
    pub replica_regions: Vec<String>,
    /// Per-replica-region settings of a primary secret, keyed by region.
    #[serde(default)]
    pub replica_settings: BTreeMap<String, ReplicaSetting>,
    /// Set on a replica secret: the region of its primary. A replica is
    /// read-only and kept in sync with the primary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary_region: Option<String>,
}

/// One replica region of a primary secret.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct ReplicaSetting {
    /// The KMS key the replica is encrypted with (`AddReplicaRegions[].KmsKeyId`);
    /// `None` for the region's `aws/secretsmanager` key.
    #[serde(default)]
    pub kms_key_id: Option<String>,
    /// `InSync` or `Failed`.
    pub status: String,
    pub status_message: String,
}

impl ReplicaSetting {
    pub fn in_sync(kms_key_id: Option<String>) -> Self {
        Self {
            kms_key_id,
            status: "InSync".to_string(),
            status_message: "Replication succeeded".to_string(),
        }
    }

    pub fn is_in_sync(&self) -> bool {
        self.status == "InSync"
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RotationRules {
    pub automatically_after_days: Option<i64>,
    /// The length of the rotation window, e.g. "2h" / "1d". AWS's modern
    /// rotation scheduling form alongside ScheduleExpression.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub duration: Option<String>,
    /// A cron() or rate() expression for the rotation schedule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule_expression: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SecretVersion {
    pub version_id: String,
    pub secret_string: Option<String>,
    pub secret_binary: Option<Vec<u8>>,
    pub stages: Vec<String>,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SecretsManagerState {
    pub account_id: String,
    pub region: String,
    pub secrets: BTreeMap<String, Secret>,
}

impl SecretsManagerState {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            secrets: BTreeMap::new(),
        }
    }

    pub fn reset(&mut self) {
        self.secrets.clear();
    }

    /// Make `name` available to a new secret, as CreateSecret does before
    /// minting one. A secret of that name that is scheduled for deletion and
    /// whose recovery window has passed is purged; one still inside its
    /// recovery window blocks the create with AWS's `InvalidRequestException`.
    /// A live secret of that name is left in place for the caller to handle
    /// (CreateSecret's idempotency path, or a name-conflict error).
    pub fn clear_name_for_create(
        &mut self,
        name: &str,
        now: DateTime<Utc>,
    ) -> Result<(), fakecloud_core::service::AwsServiceError> {
        let Some(existing) = self.secrets.get(name) else {
            return Ok(());
        };
        if !existing.deleted {
            return Ok(());
        }
        if crate::service::secret_recovery_window_elapsed(existing, now) {
            self.secrets.remove(name);
            return Ok(());
        }
        Err(fakecloud_core::service::AwsServiceError::aws_error(
            http::StatusCode::BAD_REQUEST,
            "InvalidRequestException",
            "You can't create this secret because a secret with this name is already scheduled for deletion.",
        ))
    }

    /// The `secrets` map key (the secret name) for a `SecretId`: a name, a
    /// full ARN, or a partial ARN (the full ARN without its random
    /// six-character suffix). Does not apply recovery-window expiry.
    pub fn secret_key(&self, secret_id: &str) -> Option<String> {
        if self.secrets.contains_key(secret_id) {
            return Some(secret_id.to_string());
        }
        if let Some((key, _)) = self.secrets.iter().find(|(_, s)| s.arn == secret_id) {
            return Some(key.clone());
        }
        if fakecloud_aws::arn::arn_resource(secret_id, "secretsmanager").is_some() {
            return self
                .secrets
                .iter()
                .find(|(_, s)| is_partial_secret_arn(&s.arn, secret_id))
                .map(|(key, _)| key.clone());
        }
        None
    }
}

/// Whether `partial` is `stored` minus its `-XXXXXX` random suffix: the
/// partial-ARN form AWS accepts. A bare prefix does not count: the partial
/// ARN of `app` must not resolve `app-db`.
fn is_partial_secret_arn(stored: &str, partial: &str) -> bool {
    stored
        .strip_prefix(partial)
        .and_then(|rest| rest.strip_prefix('-'))
        .is_some_and(|suffix| suffix.chars().count() == 6)
}

/// Secrets Manager state partitioned by account and region: secrets are
/// regional, so the same secret name can exist independently in two regions of
/// one account, and a request only sees the secrets of its own region. A
/// replicated secret exists once per region: the primary in its region and a
/// read-only replica (ARN naming the replica region) in each replica region.
pub type SharedSecretsManagerState =
    Arc<RwLock<fakecloud_core::multi_account::MultiRegionState<SecretsManagerState>>>;

impl fakecloud_core::multi_account::AccountState for SecretsManagerState {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

impl fakecloud_core::multi_account::SplitByRegion for SecretsManagerState {
    /// Every secret goes to the region its ARN names.
    fn split_by_region(self, into: &mut fakecloud_core::multi_account::RegionalState<Self>) {
        for (key, secret) in self.secrets {
            let region = fakecloud_aws::arn::region_of(&secret.arn).map(str::to_string);
            into.region_or_default_mut(region.as_deref())
                .secrets
                .insert(key, secret);
        }
    }
}

/// On-disk snapshot envelope for Secrets Manager state. Versioned so
/// format changes fail loudly on upgrade.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SecretsManagerSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<fakecloud_core::multi_account::MultiRegionState<SecretsManagerState>>,
    /// Only set when a v1 (single-account) snapshot is migrated: that one
    /// account's state split by region, for the caller to merge into its own
    /// container.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<fakecloud_core::multi_account::RegionalState<SecretsManagerState>>,
}

/// v3: state partitioned by (account, region). v2 kept one state per
/// account; v1 a single account's.
pub const SECRETSMANAGER_SNAPSHOT_SCHEMA_VERSION: u32 = 3;

/// The shape v1 and v2 snapshots stored: one state per account.
#[derive(Debug, serde::Deserialize)]
struct LegacySecretsManagerSnapshot {
    #[serde(default)]
    accounts: Option<fakecloud_core::multi_account::MultiAccountState<SecretsManagerState>>,
    #[serde(default)]
    state: Option<SecretsManagerState>,
}

#[derive(serde::Deserialize)]
struct SnapshotVersion {
    schema_version: u32,
}

/// Parse a persisted Secrets Manager snapshot, migrating older schemas to the
/// current one by moving every secret into the region its ARN names. A
/// snapshot newer than this build comes back with its on-disk
/// `schema_version` and no state, for the caller to refuse.
pub fn parse_secretsmanager_snapshot(
    bytes: &[u8],
) -> Result<SecretsManagerSnapshot, serde_json::Error> {
    let SnapshotVersion { schema_version } = serde_json::from_slice(bytes)?;
    if schema_version > SECRETSMANAGER_SNAPSHOT_SCHEMA_VERSION {
        return Ok(SecretsManagerSnapshot {
            schema_version,
            accounts: None,
            state: None,
        });
    }
    if schema_version == SECRETSMANAGER_SNAPSHOT_SCHEMA_VERSION {
        return serde_json::from_slice(bytes);
    }
    let legacy: LegacySecretsManagerSnapshot = serde_json::from_slice(bytes)?;
    Ok(SecretsManagerSnapshot {
        schema_version: SECRETSMANAGER_SNAPSHOT_SCHEMA_VERSION,
        accounts: legacy.accounts.map(|a| a.into_regional()),
        state: legacy.state.map(|s| {
            let (account, region) = (s.account_id.clone(), s.region.clone());
            fakecloud_core::multi_account::RegionalState::from_legacy(&account, &region, "", s)
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_initializes_empty() {
        let state = SecretsManagerState::new("123456789012", "us-east-1");
        assert_eq!(state.account_id, "123456789012");
        assert_eq!(state.region, "us-east-1");
        assert!(state.secrets.is_empty());
    }

    #[test]
    fn reset_clears_secrets() {
        let mut state = SecretsManagerState::new("123456789012", "us-east-1");
        state.secrets.insert(
            "s1".to_string(),
            Secret {
                name: "s1".to_string(),
                arn: "arn".to_string(),
                description: None,
                kms_key_id: None,
                versions: BTreeMap::new(),
                current_version_id: None,
                tags: vec![],
                tags_ever_set: false,
                deleted: false,
                deletion_date: None,
                created_at: Utc::now(),
                last_changed_at: Utc::now(),
                last_accessed_at: None,
                rotation_enabled: None,
                rotation_lambda_arn: None,
                rotation_rules: None,
                last_rotated_at: None,
                resource_policy: None,
                replica_regions: Vec::new(),
                replica_settings: BTreeMap::new(),
                primary_region: None,
            },
        );
        state.reset();
        assert!(state.secrets.is_empty());
    }

    fn secret(name: &str, arn: &str) -> Secret {
        Secret {
            name: name.to_string(),
            arn: arn.to_string(),
            description: None,
            kms_key_id: None,
            versions: BTreeMap::new(),
            current_version_id: None,
            tags: vec![],
            tags_ever_set: false,
            deleted: false,
            deletion_date: None,
            created_at: Utc::now(),
            last_changed_at: Utc::now(),
            last_accessed_at: None,
            rotation_enabled: None,
            rotation_lambda_arn: None,
            rotation_rules: None,
            last_rotated_at: None,
            resource_policy: None,
            replica_regions: Vec::new(),
            replica_settings: BTreeMap::new(),
            primary_region: None,
        }
    }

    #[test]
    fn clear_name_for_create_purges_expired_and_blocks_pending() {
        let mut state = SecretsManagerState::new("123456789012", "us-east-1");
        let now = Utc::now();
        let mut pending = secret("pending", "arn-p");
        pending.deleted = true;
        pending.deletion_date = Some(now + chrono::Duration::days(1));
        let mut expired = secret("expired", "arn-e");
        expired.deleted = true;
        expired.deletion_date = Some(now - chrono::Duration::seconds(1));
        state.secrets.insert("pending".into(), pending);
        state.secrets.insert("expired".into(), expired);
        state.secrets.insert("live".into(), secret("live", "arn-l"));

        let err = state
            .clear_name_for_create("pending", now)
            .expect_err("recovery window still open");
        assert_eq!(err.code(), "InvalidRequestException");
        assert!(state.secrets.contains_key("pending"));

        state.clear_name_for_create("expired", now).unwrap();
        assert!(!state.secrets.contains_key("expired"));

        // A secret marked deleted with no deletion date has no elapsed
        // recovery window, so it still blocks the name (CreateSecret's
        // behavior).
        let mut undated = secret("undated", "arn-u");
        undated.deleted = true;
        undated.deletion_date = None;
        state.secrets.insert("undated".into(), undated);
        let err = state
            .clear_name_for_create("undated", now)
            .expect_err("no deletion date: still in its recovery window");
        assert_eq!(err.code(), "InvalidRequestException");
        assert!(state.secrets.contains_key("undated"));

        // Live and absent names are left for the caller.
        state.clear_name_for_create("live", now).unwrap();
        assert!(state.secrets.contains_key("live"));
        state.clear_name_for_create("absent", now).unwrap();
    }

    #[test]
    fn secret_key_resolves_name_full_and_partial_arn() {
        let mut state = SecretsManagerState::new("123456789012", "us-east-1");
        let app = "arn:aws:secretsmanager:us-east-1:123456789012:secret:app-AbC123";
        let app_db = "arn:aws:secretsmanager:us-east-1:123456789012:secret:app-db-XyZ789";
        state.secrets.insert("app".into(), secret("app", app));
        state
            .secrets
            .insert("app-db".into(), secret("app-db", app_db));
        // A legacy entry keyed by its ARN still resolves to its real key.
        let legacy = "arn:aws:secretsmanager:us-east-1:123456789012:secret:old-QwErTy";
        state.secrets.insert(legacy.into(), secret("old", legacy));

        assert_eq!(state.secret_key("app").as_deref(), Some("app"));
        assert_eq!(state.secret_key(app).as_deref(), Some("app"));
        assert_eq!(state.secret_key(app_db).as_deref(), Some("app-db"));
        assert_eq!(
            state
                .secret_key("arn:aws:secretsmanager:us-east-1:123456789012:secret:app")
                .as_deref(),
            Some("app")
        );
        assert_eq!(
            state
                .secret_key("arn:aws:secretsmanager:us-east-1:123456789012:secret:app-db")
                .as_deref(),
            Some("app-db")
        );
        assert_eq!(state.secret_key(legacy).as_deref(), Some(legacy));
        assert_eq!(state.secret_key("missing"), None);
        assert_eq!(
            state.secret_key("arn:aws:secretsmanager:us-east-1:123456789012:secret:ap"),
            None
        );
    }
}
