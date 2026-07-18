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

use chrono::{DateTime, Duration, Utc};
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
}

/// Derive the partition (`arn:<partition>:…`) from an ARN, defaulting to `aws`.
pub fn partition_of(arn: &str) -> &str {
    arn.split(':')
        .nth(1)
        .filter(|p| !p.is_empty())
        .unwrap_or("aws")
}

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
    let creds = StsCredentials::generate();
    let issued_at = Utc::now();
    let expiration = issued_at + duration;
    let partition = partition_of(role_arn);
    let account_id =
        extract_account_from_arn(role_arn).unwrap_or_else(|| default_account_id.to_string());
    let role_name = assumed_role_name(role_arn);
    let session_name = CONTAINER_CREDENTIALS_SESSION_NAME;
    let assumed_role_arn = format_assumed_role_arn(partition, &account_id, role_name, session_name);
    let user_id = format!("{}:{}", deterministic_role_id(role_arn), session_name);

    {
        let mut accounts = iam.write();
        let state = accounts.get_or_create(&account_id);
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
    let mut accounts = iam.write();
    let state = accounts.get_or_create(&creds.account_id);
    state.credential_identities.remove(&creds.access_key_id);
    state.sts_temp_credentials.remove(&creds.access_key_id);
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
            // Drop any superseded creds that have now actually expired.
            rc.retired.retain(|c| {
                if c.expiration <= now {
                    evict_container_credentials(iam, c);
                    false
                } else {
                    true
                }
            });
            // Reuse the current credential while it is comfortably valid AND
            // still registered (a /_reset clears IAM state under the cache).
            if rc.current.expiration - now > CONTAINER_CREDENTIALS_REFRESH_WINDOW
                && credential_registered(iam, &rc.current)
            {
                return rc.current.clone();
            }
            // Rotate. Keep the superseded credential valid until its real
            // expiration (overlapping validity) unless it is already expired or
            // already gone from state.
            let fresh = mint_container_credentials(iam, default_account_id, role_arn, duration);
            let old = std::mem::replace(&mut rc.current, fresh.clone());
            if old.expiration > now && credential_registered(iam, &old) {
                rc.retired.push(old);
            } else {
                evict_container_credentials(iam, &old);
            }
            return fresh;
        }

        let fresh = mint_container_credentials(iam, default_account_id, role_arn, duration);
        cache.insert(
            role_arn.to_string(),
            RoleCreds {
                current: fresh.clone(),
                retired: Vec::new(),
            },
        );
        fresh
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
}
