//! Minting of short-lived container / instance credentials.
//!
//! Apps that run on EC2 or ECS carry no static keys -- the AWS SDK default
//! credential chain fetches temporary credentials from the environment (the
//! ECS container-credentials endpoint via `AWS_CONTAINER_CREDENTIALS_FULL_URI`
//! / `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, or IMDS at `169.254.169.254`).
//! To run those apps unmodified against fakecloud, the server exposes an
//! endpoint that vends credentials in that same format.
//!
//! Unlike the hardcoded ECS-Exec credential blob, these credentials are
//! *minted and registered* in IAM state exactly like an `AssumeRole` session,
//! so a subsequent signed request presenting them is accepted even under
//! `--verify-sigv4` (the credential resolver finds the registered temp key).
//!
//! [`ContainerCredentialCache`] caches one live set per role and reuses it
//! until it is close to expiry, then rotates -- mirroring real IMDS/ECS (which
//! return the *same* credentials across refetches within the validity window,
//! and keep a superseded set valid until its real expiration so concurrent
//! holders are never cut off). This also bounds IAM-state growth on this
//! unauthenticated endpoint to roughly one live temp credential per role.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Duration, Utc};
use fakecloud_core::auth::{SessionCredentialIssuer, SessionCredentials};
use parking_lot::Mutex;

use super::{
    assumed_role_name, extract_account_from_arn, format_assumed_role_arn, format_expiration,
};
use crate::state::{CredentialIdentity, SharedIamState, StsTempCredential};
use crate::xml_responses::StsCredentials;

/// Session name stamped onto container/instance credentials minted by
/// fakecloud. Surfaces in the assumed-role ARN and `GetCallerIdentity`.
pub const CONTAINER_CREDENTIALS_SESSION_NAME: &str = "fakecloud-local";

/// Default lifetime of minted container/instance credentials (1 hour). SDKs
/// refetch shortly before expiry, matching real IMDS/ECS credential rotation.
pub const DEFAULT_CONTAINER_CREDENTIALS_DURATION: Duration = Duration::hours(1);

/// Re-mint when a cached credential has less than this left. AWS SDK credential
/// providers refresh a few minutes before expiry, so keeping a margin means the
/// creds we hand out are never on the verge of expiring.
pub const CONTAINER_CREDENTIALS_REFRESH_WINDOW: Duration = Duration::minutes(5);

/// Credentials handed back to a caller of the container/instance credential
/// endpoint, already registered in IAM state so they verify on later requests.
#[derive(Debug, Clone)]
pub struct ContainerCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: String,
    pub expiration: DateTime<Utc>,
    /// When these credentials were minted. Reported by IMDS as `LastUpdated`;
    /// stays fixed across cache reuse (unlike `now`), matching real IMDS/ECS.
    pub issued_at: DateTime<Utc>,
    /// The IAM role ARN these credentials represent (as returned to the caller).
    pub role_arn: String,
    /// The `arn:aws:sts::…:assumed-role/…` principal ARN reported by
    /// `GetCallerIdentity` for these credentials.
    pub assumed_role_arn: String,
    /// Account the credential was registered under (derived from `role_arn`),
    /// so the cache can evict it from the right account's state on rotation.
    account_id: String,
}

impl ContainerCredentials {
    /// Expiration formatted as the ISO-8601 string AWS metadata endpoints emit
    /// (e.g. `2026-07-17T12:34:56Z`).
    pub fn expiration_iso8601(&self) -> String {
        format_expiration(self.expiration)
    }

    /// Mint time formatted as the ISO-8601 string AWS reports for `LastUpdated`.
    pub fn issued_at_iso8601(&self) -> String {
        format_expiration(self.issued_at)
    }

    /// The credentials as the AWS container/ECS-relative JSON shape
    /// (`AccessKeyId`, `SecretAccessKey`, `Token`, `Expiration`, `RoleArn`).
    /// Single source shared by `/_fakecloud/credentials`, the ECS relative-URI
    /// endpoint, and any other surface that vends this shape.
    pub fn to_container_json(&self) -> serde_json::Value {
        serde_json::json!({
            "AccessKeyId": self.access_key_id,
            "SecretAccessKey": self.secret_access_key,
            "Token": self.session_token,
            "Expiration": self.expiration_iso8601(),
            "RoleArn": self.role_arn,
        })
    }
}

pub use fakecloud_aws::arn::partition_of;

/// A deterministic, fixed-length suffix derived from `input` over a
/// power-of-two-sized `alphabet`. Stable across builds and toolchain versions
/// (FNV-1a seed + an LCG whose high bits index the alphabet — the low bits of
/// an LCG have a short seed-dependent period and would collide). Shared by the
/// synthetic-ID generators (assumed-role ID, IMDS instance ID) so the algorithm
/// lives in one place.
///
/// Panics in debug if `alphabet.len()` is not a power of two in `2..=256`.
pub fn deterministic_suffix(input: &str, alphabet: &[u8], len: usize) -> String {
    debug_assert!(
        alphabet.len().is_power_of_two() && (2..=256).contains(&alphabet.len()),
        "alphabet must be a power-of-two size in 2..=256"
    );
    let bits = alphabet.len().trailing_zeros();
    let shift = 64 - bits;
    let mask = alphabet.len() as u64 - 1;
    // FNV-1a 64-bit seed.
    let mut seed: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in input.bytes() {
        seed ^= byte as u64;
        seed = seed.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let mut out = String::with_capacity(len);
    for _ in 0..len {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        out.push(alphabet[((seed >> shift) & mask) as usize] as char);
    }
    out
}

/// A stable `AROA`-prefixed role ID derived deterministically from the role ARN.
///
/// Real AWS keeps a role's unique ID (the `AROA…` prefix of the caller
/// `UserId`) constant across sessions; only the session-name suffix changes.
/// Deriving it from the ARN (rather than a fresh random ID per mint) preserves
/// that invariant so a caller comparing `UserId` across credential refreshes
/// sees the same role.
fn deterministic_role_id(role_arn: &str) -> String {
    // AWS unique-ID bodies use uppercase A-Z + 2-7.
    format!(
        "AROA{}",
        deterministic_suffix(role_arn, b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567", 17)
    )
}

/// Mint a short-lived credential set for `role_arn`, register it in IAM state
/// (so it resolves + verifies on later signed requests), and return it.
///
/// The registration account and assumed-role principal are derived from
/// `role_arn` (falling back to `default_account_id` when the ARN carries no
/// account), so the returned `RoleArn` and the principal reported by
/// `GetCallerIdentity` always agree -- matching real AssumeRole, and matching
/// the sibling `sts_service::assume::assume_role` path.
///
/// Note: unlike `AssumeRole`, this does *not* require the role to exist in IAM
/// state. The endpoint's purpose is zero-config local credentials; the default
/// role (`arn:<partition>:iam::<account>:role/fakecloud`) is never created, so
/// a role-existence gate would break the common case. Under `--iam strict` a
/// role with no attached policies simply denies later actions, which is the
/// correct outcome for an unconfigured role.
pub fn mint_container_credentials(
    iam: &SharedIamState,
    default_account_id: &str,
    role_arn: &str,
    duration: Duration,
) -> ContainerCredentials {
    mint_session_credentials(
        iam,
        default_account_id,
        role_arn,
        CONTAINER_CREDENTIALS_SESSION_NAME,
        duration,
    )
}

/// [`mint_container_credentials`] with a caller-chosen role session name, for
/// surfaces whose real counterpart names the session after the workload (a
/// Lambda execution role's session is named after the function).
pub fn mint_session_credentials(
    iam: &SharedIamState,
    default_account_id: &str,
    role_arn: &str,
    session_name: &str,
    duration: Duration,
) -> ContainerCredentials {
    let creds = StsCredentials::generate();
    let issued_at = Utc::now();
    let expiration = issued_at + duration;
    let (account_id, assumed_role_arn) =
        session_principal(default_account_id, role_arn, session_name);
    let user_id;
    {
        let mut accounts = iam.write();
        let state = accounts.get_or_create(&account_id);
        let session_role_arn = state.minted_session_role_arn(role_arn);
        // `aws:userid` is `<role id>:<session>`, as AssumeRole issues it: the
        // unique ID of the role whose policies the session gets when it
        // exists in IAM (what key policies and KMS grants naming the role
        // recorded), a stable stand-in for a role that was never created
        // (the default `role/fakecloud`).
        let role_id = super::role_for_arn(state, &session_role_arn)
            .map(|role| role.role_id.clone())
            .unwrap_or_else(|| deterministic_role_id(role_arn));
        user_id = format!("{role_id}:{session_name}");
        state.credential_identities.insert(
            creds.access_key_id.clone(),
            CredentialIdentity {
                arn: assumed_role_arn.clone(),
                user_id: user_id.clone(),
                account_id: account_id.clone(),
            },
        );
        state.sts_temp_credentials.insert(
            creds.access_key_id.clone(),
            StsTempCredential {
                access_key_id: creds.access_key_id.clone(),
                secret_access_key: creds.secret_access_key.clone(),
                session_token: creds.session_token.clone(),
                principal_arn: assumed_role_arn.clone(),
                user_id,
                account_id: account_id.clone(),
                expiration,
                session_policies: Vec::new(),
                mfa_present: false,
                issued_at,
                federated_provider: None,
                // The real ARN of the role whose policies the session gets
                // when it exists; the configured one otherwise (the default
                // `role/fakecloud` never does).
                role_arn: Some(session_role_arn),
            },
        );
    }

    ContainerCredentials {
        access_key_id: creds.access_key_id,
        secret_access_key: creds.secret_access_key,
        session_token: creds.session_token,
        expiration,
        issued_at,
        role_arn: role_arn.to_string(),
        assumed_role_arn,
        account_id,
    }
}

/// The account a session for `role_arn` is registered under and its
/// assumed-role principal ARN.
fn session_principal(
    default_account_id: &str,
    role_arn: &str,
    session_name: &str,
) -> (String, String) {
    let account_id =
        extract_account_from_arn(role_arn).unwrap_or_else(|| default_account_id.to_string());
    let assumed_role_arn = format_assumed_role_arn(
        partition_of(role_arn),
        &account_id,
        assumed_role_name(role_arn),
        session_name,
    );
    (account_id, assumed_role_arn)
}

/// Unregister every registered session of `role_arn` named `session_name`,
/// whoever holds it. For sessions minted by an earlier server run whose
/// holder is gone (persisted IAM state outlives the in-memory caches), e.g.
/// the sessions of ECS tasks a restart stopped. Returns how many were removed.
pub fn revoke_sessions_named(
    iam: &SharedIamState,
    default_account_id: &str,
    role_arn: &str,
    session_name: &str,
) -> usize {
    let (account_id, assumed_role_arn) =
        session_principal(default_account_id, role_arn, session_name);
    let mut accounts = iam.write();
    let Some(state) = accounts.get_mut(&account_id) else {
        return 0;
    };
    let keys: Vec<String> = state
        .sts_temp_credentials
        .iter()
        .filter(|(_, c)| c.principal_arn == assumed_role_arn)
        .map(|(k, _)| k.clone())
        .collect();
    for key in &keys {
        state.sts_temp_credentials.remove(key);
        state.credential_identities.remove(key);
    }
    keys.len()
}

/// True if `creds` is still registered in IAM state (i.e. not cleared by a
/// `/_reset`). Used so the cache never hands back credentials that have been
/// purged out from under it.
fn credential_registered(iam: &SharedIamState, creds: &ContainerCredentials) -> bool {
    iam.read()
        .get(&creds.account_id)
        .map(|s| s.sts_temp_credentials.contains_key(&creds.access_key_id))
        .unwrap_or(false)
}

/// Remove a minted container credential from IAM state (both maps). Idempotent
/// -- a no-op if the key is already gone (e.g. after a reset).
fn evict_container_credentials(iam: &SharedIamState, creds: &ContainerCredentials) {
    unregister_credentials(iam, &creds.account_id, &creds.access_key_id);
}

fn unregister_credentials(iam: &SharedIamState, account_id: &str, access_key_id: &str) {
    // `get_mut`, not `get_or_create`: after a reset the account may be gone,
    // and revoking must not bring it back.
    if let Some(state) = iam.write().get_mut(account_id) {
        state.credential_identities.remove(access_key_id);
        state.sts_temp_credentials.remove(access_key_id);
    }
}

impl From<ContainerCredentials> for SessionCredentials {
    fn from(c: ContainerCredentials) -> Self {
        Self {
            access_key_id: c.access_key_id,
            secret_access_key: c.secret_access_key,
            session_token: c.session_token,
            expiration: c.expiration,
            account_id: c.account_id,
        }
    }
}

/// [`SessionCredentialIssuer`] over IAM state: every set it issues is minted
/// and registered exactly like [`mint_session_credentials`], so the code it
/// is handed to signs requests as the assumed role, and is unregistered again
/// on [`SessionCredentialIssuer::revoke`].
pub struct IamSessionCredentialIssuer {
    iam: SharedIamState,
    default_account_id: String,
}

impl IamSessionCredentialIssuer {
    /// `default_account_id` registers sessions for role ARNs that carry no
    /// account of their own.
    pub fn shared(
        iam: SharedIamState,
        default_account_id: impl Into<String>,
    ) -> Arc<dyn SessionCredentialIssuer> {
        Arc::new(Self {
            iam,
            default_account_id: default_account_id.into(),
        })
    }
}

impl SessionCredentialIssuer for IamSessionCredentialIssuer {
    fn issue(&self, role_arn: &str, session_name: &str, duration: Duration) -> SessionCredentials {
        mint_session_credentials(
            &self.iam,
            &self.default_account_id,
            role_arn,
            session_name,
            duration,
        )
        .into()
    }

    fn revoke(&self, credentials: &SessionCredentials) {
        unregister_credentials(
            &self.iam,
            &credentials.account_id,
            &credentials.access_key_id,
        );
    }
}

/// The live + recently-superseded credentials for one role.
struct RoleCreds {
    /// The credential currently handed to callers.
    current: ContainerCredentials,
    /// Superseded credentials still within their advertised validity, kept
    /// registered until they actually expire so a client that already holds one
    /// is not cut off. Purged (and evicted from IAM) once expired.
    retired: Vec<ContainerCredentials>,
}

/// Caches the credentials vended by the container/instance credential endpoint,
/// one live set per role. A GET reuses the cached set while it still has more
/// than [`CONTAINER_CREDENTIALS_REFRESH_WINDOW`] left and is still registered;
/// otherwise it mints a fresh set, keeping the superseded one valid until its
/// real expiration (matching IMDS/ECS overlapping validity).
///
/// Lock order: the internal `Mutex` is taken *before* the IAM `RwLock` (via
/// mint/evict/registration checks). No path takes the IAM lock and then this
/// one, so the ordering cannot deadlock; keep it that way.
#[derive(Default)]
pub struct ContainerCredentialCache {
    by_role: Mutex<HashMap<String, RoleCreds>>,
}

impl ContainerCredentialCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Return credentials for `role_arn`, minting (and caching) a fresh set only
    /// when there is none cached, the cached set is within the refresh window of
    /// expiring, or it was cleared from IAM state (e.g. by `/_reset`).
    /// `default_account_id` is the server account used only when `role_arn`
    /// carries no account of its own.
    pub fn get_or_mint(
        &self,
        iam: &SharedIamState,
        default_account_id: &str,
        role_arn: &str,
        duration: Duration,
    ) -> ContainerCredentials {
        let now = Utc::now();
        let mut cache = self.by_role.lock();

        if let Some(rc) = cache.get_mut(role_arn) {
            return rc.reuse_or_rotate(iam, now, || {
                mint_container_credentials(iam, default_account_id, role_arn, duration)
            });
        }

        let fresh = mint_container_credentials(iam, default_account_id, role_arn, duration);
        cache.insert(role_arn.to_string(), RoleCreds::new(fresh.clone()));
        fresh
    }
}

impl RoleCreds {
    fn new(current: ContainerCredentials) -> Self {
        Self {
            current,
            retired: Vec::new(),
        }
    }

    /// Hand back the current credential while it is comfortably valid and
    /// still registered; otherwise rotate to a fresh one from `mint`, keeping
    /// the superseded set valid until its real expiration.
    fn reuse_or_rotate(
        &mut self,
        iam: &SharedIamState,
        now: DateTime<Utc>,
        mint: impl FnOnce() -> ContainerCredentials,
    ) -> ContainerCredentials {
        // Drop any superseded creds that have now actually expired.
        self.retired.retain(|c| {
            if c.expiration <= now {
                evict_container_credentials(iam, c);
                false
            } else {
                true
            }
        });
        // Reuse the current credential while it is comfortably valid AND
        // still registered (a /_reset clears IAM state under the cache).
        if self.current.expiration - now > CONTAINER_CREDENTIALS_REFRESH_WINDOW
            && credential_registered(iam, &self.current)
        {
            return self.current.clone();
        }
        // Rotate. Keep the superseded credential valid until its real
        // expiration (overlapping validity) unless it is already expired or
        // already gone from state.
        let fresh = mint();
        let old = std::mem::replace(&mut self.current, fresh.clone());
        if old.expiration > now && credential_registered(iam, &old) {
            self.retired.push(old);
        } else {
            evict_container_credentials(iam, &old);
        }
        fresh
    }

    /// Unregister every credential this entry handed out.
    fn evict_all(&self, iam: &SharedIamState) {
        evict_container_credentials(iam, &self.current);
        for c in &self.retired {
            evict_container_credentials(iam, c);
        }
    }
}

/// The session a workload runs under: its role and the session name AWS
/// gives it (an ECS task's role session is named after the task ID).
struct WorkloadCreds {
    role_arn: String,
    session_name: String,
    creds: RoleCreds,
}

/// Caches the credentials vended to running workloads (ECS tasks), one live
/// set per workload, each for that workload's own role and session name.
///
/// Refetches within the validity window return the same set; near expiry a
/// fresh set is minted and the superseded one stays valid until it expires,
/// as the ECS agent does. [`WorkloadCredentialCache::revoke`] unregisters
/// every set a workload was handed once it stops.
///
/// Lock order matches [`ContainerCredentialCache`]: this `Mutex` before the
/// IAM `RwLock`, never the reverse.
#[derive(Default)]
pub struct WorkloadCredentialCache {
    by_workload: Mutex<HashMap<String, WorkloadCreds>>,
}

impl WorkloadCredentialCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Credentials for the workload `key` running as `role_arn` with role
    /// session `session_name`: the cached set while it is comfortably valid
    /// and still registered, a freshly minted (and registered) one otherwise.
    /// A workload whose role changed has its old sessions revoked first.
    pub fn get_or_mint(
        &self,
        iam: &SharedIamState,
        default_account_id: &str,
        key: &str,
        role_arn: &str,
        session_name: &str,
        duration: Duration,
    ) -> ContainerCredentials {
        let now = Utc::now();
        let mint =
            || mint_session_credentials(iam, default_account_id, role_arn, session_name, duration);
        let mut cache = self.by_workload.lock();
        if let Some(entry) = cache.get_mut(key) {
            if entry.role_arn == role_arn && entry.session_name == session_name {
                return entry.creds.reuse_or_rotate(iam, now, mint);
            }
            entry.creds.evict_all(iam);
            cache.remove(key);
        }
        let fresh = mint();
        cache.insert(
            key.to_string(),
            WorkloadCreds {
                role_arn: role_arn.to_string(),
                session_name: session_name.to_string(),
                creds: RoleCreds::new(fresh.clone()),
            },
        );
        fresh
    }

    /// Unregister every credential set the workload `key` was handed, so
    /// requests signed with them no longer authenticate. Idempotent; returns
    /// whether the workload had any.
    pub fn revoke(&self, iam: &SharedIamState, key: &str) -> bool {
        let removed = self.by_workload.lock().remove(key);
        match removed {
            Some(entry) => {
                entry.creds.evict_all(iam);
                true
            }
            None => false,
        }
    }

    /// Revoke every workload for which `still_running` returns false.
    pub fn revoke_unless(&self, iam: &SharedIamState, still_running: impl Fn(&str) -> bool) {
        let mut cache = self.by_workload.lock();
        cache.retain(|key, entry| {
            if still_running(key) {
                true
            } else {
                entry.creds.evict_all(iam);
                false
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential_resolver::IamCredentialResolver;
    use crate::state::IamState;
    use fakecloud_core::auth::{CredentialResolver, PrincipalType};
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn shared() -> SharedIamState {
        Arc::new(RwLock::new(MultiAccountState::<IamState>::new(
            "123456789012",
            "us-east-1",
            "",
        )))
    }

    fn temp_cred_count(iam: &SharedIamState, account: &str) -> usize {
        let mut accounts = iam.write();
        accounts.get_or_create(account).sts_temp_credentials.len()
    }

    /// The session records the real ARN of the role whose policies it gets,
    /// whatever path the configured ARN gives; the configured ARN when no
    /// such role exists (the zero-config `role/fakecloud`).
    #[test]
    fn minted_session_records_the_real_role_arn() {
        let iam = shared();
        iam.write().get_or_create("123456789012").roles.insert(
            "app".into(),
            crate::state::IamRole {
                role_name: "app".into(),
                role_id: "AROAAPP".into(),
                arn: "arn:aws:iam::123456789012:role/ops/app".into(),
                path: "/ops/".into(),
                assume_role_policy_document: "{}".into(),
                created_at: Utc::now(),
                description: None,
                max_session_duration: 3600,
                tags: Vec::new(),
                permissions_boundary: None,
            },
        );
        let resolver = IamCredentialResolver::new(iam.clone());
        let recorded = |configured: &str| {
            let minted = mint_container_credentials(
                &iam,
                "123456789012",
                configured,
                DEFAULT_CONTAINER_CREDENTIALS_DURATION,
            );
            resolver.resolve(&minted.access_key_id).unwrap().role_arn
        };
        for configured in [
            "arn:aws:iam::123456789012:role/app",
            "arn:aws:iam::123456789012:role/ops/app",
        ] {
            assert_eq!(
                recorded(configured).as_deref(),
                Some("arn:aws:iam::123456789012:role/ops/app"),
                "{configured}"
            );
        }
        assert_eq!(
            recorded("arn:aws:iam::123456789012:role/fakecloud").as_deref(),
            Some("arn:aws:iam::123456789012:role/fakecloud")
        );
    }

    #[test]
    fn minted_creds_resolve_and_verify() {
        let iam = shared();
        let minted = mint_container_credentials(
            &iam,
            "123456789012",
            "arn:aws:iam::123456789012:role/fakecloud",
            DEFAULT_CONTAINER_CREDENTIALS_DURATION,
        );

        assert!(minted.access_key_id.starts_with("FSIA"), "{minted:?}");
        assert_eq!(
            minted.assumed_role_arn,
            "arn:aws:sts::123456789012:assumed-role/fakecloud/fakecloud-local"
        );

        // The credential resolver finds the registered key with the matching
        // secret + session token -- i.e. a request signed with these creds
        // verifies even under --verify-sigv4.
        let resolver = IamCredentialResolver::new(iam);
        let resolved = resolver
            .resolve(&minted.access_key_id)
            .expect("minted key resolves");
        assert_eq!(resolved.secret_access_key, minted.secret_access_key);
        assert_eq!(
            resolved.session_token.as_deref(),
            Some(minted.session_token.as_str())
        );
        assert_eq!(
            resolved.principal.principal_type,
            PrincipalType::AssumedRole
        );
        assert_eq!(resolved.principal.arn, minted.assumed_role_arn);
    }

    #[test]
    fn role_path_is_dropped_and_partition_parsed() {
        assert_eq!(
            partition_of("arn:aws-cn:iam::123456789012:role/x"),
            "aws-cn"
        );
        assert_eq!(partition_of("not-an-arn"), "aws");
        // AWS drops the path in the assumed-role principal.
        assert_eq!(
            assumed_role_name("arn:aws:iam::123456789012:role/team/app-role"),
            "app-role"
        );
        assert_eq!(assumed_role_name("plain"), "plain");
    }

    #[test]
    fn cross_account_role_registers_under_role_account() {
        let iam = shared();
        let minted = mint_container_credentials(
            &iam,
            "123456789012",
            "arn:aws:iam::999999999999:role/app",
            DEFAULT_CONTAINER_CREDENTIALS_DURATION,
        );
        // Principal and returned RoleArn share the role's account.
        assert_eq!(
            minted.assumed_role_arn,
            "arn:aws:sts::999999999999:assumed-role/app/fakecloud-local"
        );
        // Registered under the role's account, and resolvable there.
        let resolver = IamCredentialResolver::new(iam);
        let resolved = resolver.resolve(&minted.access_key_id).unwrap();
        assert_eq!(resolved.principal.arn, minted.assumed_role_arn);
    }

    #[test]
    fn aws_cn_partition_in_assumed_role_arn() {
        let iam = shared();
        let minted = mint_container_credentials(
            &iam,
            "123456789012",
            "arn:aws-cn:iam::123456789012:role/fakecloud",
            DEFAULT_CONTAINER_CREDENTIALS_DURATION,
        );
        assert_eq!(
            minted.assumed_role_arn,
            "arn:aws-cn:sts::123456789012:assumed-role/fakecloud/fakecloud-local"
        );
    }

    /// A minted session's `aws:userid` is `<role id>:<session>` with the
    /// unique ID of the role when it exists in IAM (also when the configured
    /// ARN leaves out its path), as AssumeRole issues it, so policies and
    /// grants that recorded the role's ID match; a role never created keeps
    /// the stable stand-in.
    #[test]
    fn minted_sessions_carry_the_roles_unique_id() {
        let iam = shared();
        iam.write().get_or_create("123456789012").roles.insert(
            "app".to_string(),
            crate::state::IamRole {
                role_name: "app".into(),
                role_id: "AROAREALAPPROLEID".into(),
                arn: "arn:aws:iam::123456789012:role/team/app".into(),
                path: "/team/".into(),
                assume_role_policy_document: "{}".into(),
                created_at: Utc::now(),
                tags: Vec::new(),
                max_session_duration: 3600,
                permissions_boundary: None,
                description: None,
            },
        );
        let resolver = IamCredentialResolver::new(iam.clone());
        for configured in [
            "arn:aws:iam::123456789012:role/team/app",
            "arn:aws:iam::123456789012:role/app",
        ] {
            let minted = mint_session_credentials(
                &iam,
                "123456789012",
                configured,
                "fn",
                DEFAULT_CONTAINER_CREDENTIALS_DURATION,
            );
            let resolved = resolver.resolve(&minted.access_key_id).unwrap();
            assert_eq!(
                resolved.principal.user_id, "AROAREALAPPROLEID:fn",
                "{configured}"
            );
        }
        let default_role = "arn:aws:iam::123456789012:role/fakecloud";
        let minted = mint_container_credentials(
            &iam,
            "123456789012",
            default_role,
            DEFAULT_CONTAINER_CREDENTIALS_DURATION,
        );
        let resolved = resolver.resolve(&minted.access_key_id).unwrap();
        assert_eq!(
            resolved.principal.user_id,
            format!(
                "{}:{CONTAINER_CREDENTIALS_SESSION_NAME}",
                deterministic_role_id(default_role)
            )
        );
    }

    #[test]
    fn deterministic_role_id_is_stable_per_role() {
        let a = deterministic_role_id("arn:aws:iam::123456789012:role/fakecloud");
        let b = deterministic_role_id("arn:aws:iam::123456789012:role/fakecloud");
        let c = deterministic_role_id("arn:aws:iam::123456789012:role/other");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert!(a.starts_with("AROA") && a.len() == 21, "{a}");
    }

    #[test]
    fn cache_reuses_within_window() {
        let iam = shared();
        let cache = ContainerCredentialCache::new();
        let role = "arn:aws:iam::123456789012:role/fakecloud";

        let first = cache.get_or_mint(
            &iam,
            "123456789012",
            role,
            DEFAULT_CONTAINER_CREDENTIALS_DURATION,
        );
        let second = cache.get_or_mint(
            &iam,
            "123456789012",
            role,
            DEFAULT_CONTAINER_CREDENTIALS_DURATION,
        );
        assert_eq!(first.access_key_id, second.access_key_id);
        assert_eq!(temp_cred_count(&iam, "123456789012"), 1);
    }

    #[test]
    fn cache_rotation_keeps_superseded_key_until_expiry() {
        let iam = shared();
        let cache = ContainerCredentialCache::new();
        let role = "arn:aws:iam::123456789012:role/fakecloud";

        // A short (2 min) validity is inside the 5-min refresh window, so the
        // second call rotates. The superseded key is still valid, so it must
        // stay registered (overlapping validity) -- both keys present.
        let short = chrono::Duration::minutes(2);
        let a = cache.get_or_mint(&iam, "123456789012", role, short);
        let b = cache.get_or_mint(&iam, "123456789012", role, short);
        assert_ne!(
            a.access_key_id, b.access_key_id,
            "should rotate near expiry"
        );
        let resolver = IamCredentialResolver::new(iam.clone());
        assert!(
            resolver.resolve(&a.access_key_id).is_some(),
            "superseded-but-unexpired key must stay valid"
        );
        assert!(resolver.resolve(&b.access_key_id).is_some());
        assert_eq!(temp_cred_count(&iam, "123456789012"), 2);
    }

    #[test]
    fn cache_evicts_already_expired_key_on_rotation() {
        let iam = shared();
        let cache = ContainerCredentialCache::new();
        let role = "arn:aws:iam::123456789012:role/fakecloud";

        // Already-expired validity: on rotation the superseded key is past its
        // expiration, so it is evicted immediately rather than retained.
        let expired = chrono::Duration::seconds(-1);
        let a = cache.get_or_mint(&iam, "123456789012", role, expired);
        let b = cache.get_or_mint(&iam, "123456789012", role, expired);
        assert_ne!(a.access_key_id, b.access_key_id);
        assert_eq!(
            temp_cred_count(&iam, "123456789012"),
            1,
            "expired superseded key must be evicted"
        );
    }

    #[test]
    fn cache_remints_after_state_reset() {
        let iam = shared();
        let cache = ContainerCredentialCache::new();
        let role = "arn:aws:iam::123456789012:role/fakecloud";

        let first = cache.get_or_mint(
            &iam,
            "123456789012",
            role,
            DEFAULT_CONTAINER_CREDENTIALS_DURATION,
        );
        // Simulate a /_reset clearing IAM state out from under the cache.
        iam.write()
            .get_or_create("123456789012")
            .sts_temp_credentials
            .clear();

        let second = cache.get_or_mint(
            &iam,
            "123456789012",
            role,
            DEFAULT_CONTAINER_CREDENTIALS_DURATION,
        );
        assert_ne!(
            first.access_key_id, second.access_key_id,
            "must re-mint after reset instead of serving a purged key"
        );
        let resolver = IamCredentialResolver::new(iam);
        assert!(
            resolver.resolve(&second.access_key_id).is_some(),
            "re-minted key must be registered"
        );
    }

    #[test]
    fn expiration_iso8601_format() {
        let iam = shared();
        let minted = mint_container_credentials(
            &iam,
            "123456789012",
            "arn:aws:iam::123456789012:role/fakecloud",
            Duration::hours(1),
        );
        let iso = minted.expiration_iso8601();
        assert!(iso.ends_with('Z') && iso.len() == 20, "{iso}");
    }

    #[test]
    fn issued_session_is_named_after_the_caller_and_revocable() {
        let iam = shared();
        let issuer = IamSessionCredentialIssuer::shared(iam.clone(), "123456789012");
        let creds = issuer.issue(
            "arn:aws:iam::999999999999:role/service-role/exec",
            "my-function",
            Duration::hours(1),
        );
        assert_eq!(creds.account_id, "999999999999");

        let resolver = IamCredentialResolver::new(iam.clone());
        let resolved = resolver
            .resolve(&creds.access_key_id)
            .expect("issued key resolves");
        assert_eq!(
            resolved.principal.arn,
            "arn:aws:sts::999999999999:assumed-role/exec/my-function"
        );
        assert_eq!(
            resolved.session_token.as_deref(),
            Some(creds.session_token.as_str())
        );

        issuer.revoke(&creds);
        assert!(resolver.resolve(&creds.access_key_id).is_none());
        assert_eq!(temp_cred_count(&iam, "999999999999"), 0);
        // Revoking twice is a no-op.
        issuer.revoke(&creds);
    }

    #[test]
    fn workload_cache_names_the_session_and_revokes_every_rotation() {
        let iam = shared();
        let cache = WorkloadCredentialCache::new();
        let role = "arn:aws:iam::123456789012:role/app";
        let resolver = IamCredentialResolver::new(iam.clone());

        let long = cache.get_or_mint(&iam, "123456789012", "a/t1", role, "t1", Duration::hours(1));
        let again = cache.get_or_mint(&iam, "123456789012", "a/t1", role, "t1", Duration::hours(1));
        assert_eq!(
            long.access_key_id, again.access_key_id,
            "reuse within window"
        );
        assert_eq!(
            long.assumed_role_arn,
            "arn:aws:sts::123456789012:assumed-role/app/t1"
        );

        // Near expiry a fresh set is minted; the superseded one stays valid.
        let short = Duration::minutes(2);
        let a = cache.get_or_mint(&iam, "123456789012", "a/t2", role, "t2", short);
        let b = cache.get_or_mint(&iam, "123456789012", "a/t2", role, "t2", short);
        assert_ne!(a.access_key_id, b.access_key_id, "rotates near expiry");
        assert!(resolver.resolve(&a.access_key_id).is_some());

        // Revoking the workload unregisters the current and superseded sets,
        // and leaves other workloads alone.
        assert!(cache.revoke(&iam, "a/t2"));
        assert!(resolver.resolve(&a.access_key_id).is_none());
        assert!(resolver.resolve(&b.access_key_id).is_none());
        assert!(resolver.resolve(&long.access_key_id).is_some());
        assert!(!cache.revoke(&iam, "a/t2"), "idempotent");

        cache.revoke_unless(&iam, |key| key != "a/t1");
        assert!(resolver.resolve(&long.access_key_id).is_none());
        assert_eq!(temp_cred_count(&iam, "123456789012"), 0);
    }

    #[test]
    fn workload_cache_role_change_revokes_the_old_session() {
        let iam = shared();
        let cache = WorkloadCredentialCache::new();
        let resolver = IamCredentialResolver::new(iam.clone());
        let first = cache.get_or_mint(
            &iam,
            "123456789012",
            "k",
            "arn:aws:iam::123456789012:role/one",
            "t",
            Duration::hours(1),
        );
        let second = cache.get_or_mint(
            &iam,
            "123456789012",
            "k",
            "arn:aws:iam::123456789012:role/two",
            "t",
            Duration::hours(1),
        );
        assert_eq!(
            second.assumed_role_arn,
            "arn:aws:sts::123456789012:assumed-role/two/t"
        );
        assert!(resolver.resolve(&first.access_key_id).is_none());
        assert!(resolver.resolve(&second.access_key_id).is_some());
    }

    #[test]
    fn sessions_named_after_a_workload_are_revoked_without_the_cache() {
        let iam = shared();
        let role = "arn:aws:iam::123456789012:role/app";
        // Minted by a cache that no longer exists (an earlier server run).
        let a = WorkloadCredentialCache::new().get_or_mint(
            &iam,
            "123456789012",
            "k",
            role,
            "task-1",
            Duration::hours(1),
        );
        let other =
            mint_session_credentials(&iam, "123456789012", role, "task-2", Duration::hours(1));
        assert_eq!(
            revoke_sessions_named(&iam, "123456789012", role, "task-1"),
            1
        );
        let resolver = IamCredentialResolver::new(iam.clone());
        assert!(resolver.resolve(&a.access_key_id).is_none());
        assert!(resolver.resolve(&other.access_key_id).is_some());
        assert_eq!(
            revoke_sessions_named(&iam, "123456789012", role, "task-1"),
            0
        );
    }
}
