//! Adapter that implements [`fakecloud_core::auth::CredentialResolver`] over
//! the shared IAM state.
//!
//! SigV4 verification (and later IAM enforcement) runs in `fakecloud-core`,
//! which intentionally doesn't depend on `fakecloud-iam`. The trait lives in
//! core and the concrete resolver lives here, keeping the dependency edge
//! pointing the right way.

use std::sync::Arc;

use fakecloud_core::auth::{CredentialResolver, Principal, PrincipalType, ResolvedCredential};

use crate::state::SharedIamState;

/// [`CredentialResolver`] backed by an [`IamState`] shared via
/// [`SharedIamState`]. Expired STS temporary credentials never resolve;
/// [`CredentialResolver::is_expired`] reports them so dispatch can answer
/// `ExpiredToken`.
#[derive(Clone)]
pub struct IamCredentialResolver {
    state: SharedIamState,
}

impl IamCredentialResolver {
    pub fn new(state: SharedIamState) -> Self {
        Self { state }
    }

    pub fn shared(state: SharedIamState) -> Arc<dyn CredentialResolver> {
        Arc::new(Self::new(state))
    }
}

impl CredentialResolver for IamCredentialResolver {
    fn resolve(&self, access_key_id: &str) -> Option<ResolvedCredential> {
        let mut states = self.state.write();
        // Sweep STS credentials long past expiry while the write lock is
        // held, so the table stays bounded without a separate task.
        let now = chrono::Utc::now();
        for (_, account_state) in states.iter_mut() {
            account_state.prune_expired_sts_credentials(now);
        }
        // Search ALL accounts' credentials — a full scan is fine for a
        // testing tool with a small number of accounts.
        for (_, account_state) in states.iter_mut() {
            if let Some(lookup) = account_state.credential_secret(access_key_id) {
                let principal_type = PrincipalType::from_arn(&lookup.principal_arn);
                return Some(ResolvedCredential {
                    secret_access_key: lookup.secret_access_key,
                    session_token: lookup.session_token,
                    principal: Principal {
                        arn: lookup.principal_arn,
                        user_id: lookup.user_id,
                        account_id: lookup.account_id,
                        principal_type,
                        source_identity: None,
                        tags: lookup.principal_tags.map(|m| m.into_iter().collect()),
                    },
                    session_policies: lookup.session_policies,
                    mfa_present: lookup.mfa_present,
                    token_issued_at: lookup.token_issued_at,
                    federated_provider: lookup.federated_provider,
                });
            }
        }
        None
    }

    fn is_expired(&self, access_key_id: &str) -> bool {
        self.state
            .read()
            .iter()
            .any(|(_, s)| s.sts_credential_expired(access_key_id))
    }
}

fn _assert_impl<T: CredentialResolver>() {}
const _: fn() = || {
    _assert_impl::<IamCredentialResolver>();
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{IamAccessKey, IamState, IamUser};
    use chrono::Utc;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    /// Helper: create a `SharedIamState` (multi-account) pre-populated with
    /// one account whose state is set up by the caller.
    fn shared(state: IamState) -> SharedIamState {
        let account_id = state.account_id.clone();
        let mut mas = MultiAccountState::<IamState>::new(&account_id, "us-east-1", "");
        // Replace the auto-created default with the caller's state.
        *mas.get_or_create(&account_id) = state;
        Arc::new(RwLock::new(mas))
    }

    #[test]
    fn resolves_iam_user_secret_from_state() {
        let mut state = IamState::new("123456789012");
        state.users.insert(
            "alice".to_string(),
            IamUser {
                user_name: "alice".into(),
                user_id: "AIDAALICE".into(),
                arn: "arn:aws:iam::123456789012:user/alice".into(),
                path: "/".into(),
                created_at: Utc::now(),
                tags: Vec::new(),
                permissions_boundary: None,
            },
        );
        state.access_keys.insert(
            "alice".to_string(),
            vec![IamAccessKey {
                access_key_id: "FKIAALICE".into(),
                secret_access_key: "the-secret".into(),
                user_name: "alice".into(),
                status: "Active".into(),
                created_at: Utc::now(),
            }],
        );
        let resolver = IamCredentialResolver::new(shared(state));
        let resolved = resolver.resolve("FKIAALICE").unwrap();
        assert_eq!(resolved.secret_access_key, "the-secret");
        assert_eq!(
            resolved.principal.arn,
            "arn:aws:iam::123456789012:user/alice"
        );
        assert_eq!(resolved.principal.principal_type, PrincipalType::User);
        assert_eq!(resolved.session_token, None);
    }

    #[test]
    fn returns_none_for_unknown_akid() {
        let state = IamState::new("123456789012");
        let resolver = IamCredentialResolver::new(shared(state));
        assert!(resolver.resolve("FKIANONE").is_none());
    }

    #[test]
    fn classifies_sts_assumed_role_principal() {
        use crate::state::StsTempCredential;
        let mut state = IamState::new("123456789012");
        state.sts_temp_credentials.insert(
            "FSIATEMP".to_string(),
            StsTempCredential {
                access_key_id: "FSIATEMP".into(),
                secret_access_key: "temp-secret".into(),
                session_token: "temp-token".into(),
                principal_arn: "arn:aws:sts::123456789012:assumed-role/ops/session".into(),
                user_id: "AROA:session".into(),
                account_id: "123456789012".into(),
                expiration: Utc::now() + chrono::Duration::minutes(30),
                session_policies: Vec::new(),
                mfa_present: false,
                issued_at: Utc::now(),
                federated_provider: None,
            },
        );
        let resolver = IamCredentialResolver::new(shared(state));
        let resolved = resolver.resolve("FSIATEMP").unwrap();
        assert_eq!(
            resolved.principal.principal_type,
            PrincipalType::AssumedRole
        );
        assert_eq!(resolved.session_token.as_deref(), Some("temp-token"));
    }

    #[test]
    fn resolves_across_accounts() {
        let mas = MultiAccountState::<IamState>::new("111111111111", "us-east-1", "");
        let shared_state: SharedIamState = Arc::new(RwLock::new(mas));

        // Add user in account A
        {
            let mut states = shared_state.write();
            let a = states.get_or_create("111111111111");
            a.users.insert(
                "alice".into(),
                IamUser {
                    user_name: "alice".into(),
                    user_id: "AIDAALICE".into(),
                    arn: "arn:aws:iam::111111111111:user/alice".into(),
                    path: "/".into(),
                    created_at: Utc::now(),
                    tags: Vec::new(),
                    permissions_boundary: None,
                },
            );
            a.access_keys.insert(
                "alice".into(),
                vec![IamAccessKey {
                    access_key_id: "FKIAALICE".into(),
                    secret_access_key: "secret-a".into(),
                    user_name: "alice".into(),
                    status: "Active".into(),
                    created_at: Utc::now(),
                }],
            );

            // Add user in account B
            let b = states.get_or_create("222222222222");
            b.users.insert(
                "bob".into(),
                IamUser {
                    user_name: "bob".into(),
                    user_id: "AIDABOB".into(),
                    arn: "arn:aws:iam::222222222222:user/bob".into(),
                    path: "/".into(),
                    created_at: Utc::now(),
                    tags: Vec::new(),
                    permissions_boundary: None,
                },
            );
            b.access_keys.insert(
                "bob".into(),
                vec![IamAccessKey {
                    access_key_id: "FKIABOB".into(),
                    secret_access_key: "secret-b".into(),
                    user_name: "bob".into(),
                    status: "Active".into(),
                    created_at: Utc::now(),
                }],
            );
        }

        let resolver = IamCredentialResolver::new(shared_state);

        // Resolve from account A
        let a = resolver.resolve("FKIAALICE").unwrap();
        assert_eq!(a.principal.account_id, "111111111111");

        // Resolve from account B
        let b = resolver.resolve("FKIABOB").unwrap();
        assert_eq!(b.principal.account_id, "222222222222");

        // Unknown key
        assert!(resolver.resolve("FKIANONE").is_none());
    }

    #[test]
    fn resolves_iam_user_tags_for_principal() {
        use crate::state::Tag;
        let mut state = IamState::new("123456789012");
        state.users.insert(
            "bob".to_string(),
            IamUser {
                user_name: "bob".into(),
                user_id: "AIDABOB".into(),
                arn: "arn:aws:iam::123456789012:user/bob".into(),
                path: "/".into(),
                created_at: Utc::now(),
                tags: vec![
                    Tag {
                        key: "Team".into(),
                        value: "platform".into(),
                    },
                    Tag {
                        key: "Environment".into(),
                        value: "prod".into(),
                    },
                ],
                permissions_boundary: None,
            },
        );
        state.access_keys.insert(
            "bob".to_string(),
            vec![IamAccessKey {
                access_key_id: "FKIABOB".into(),
                secret_access_key: "bob-secret".into(),
                user_name: "bob".into(),
                status: "Active".into(),
                created_at: Utc::now(),
            }],
        );
        let resolver = IamCredentialResolver::new(shared(state));
        let resolved = resolver.resolve("FKIABOB").unwrap();
        let tags = resolved.principal.tags.as_ref().unwrap();
        assert_eq!(tags.get("Team").map(|s| s.as_str()), Some("platform"));
        assert_eq!(tags.get("Environment").map(|s| s.as_str()), Some("prod"));
    }

    #[test]
    fn resolves_assumed_role_tags_for_principal() {
        use crate::state::{IamRole, StsTempCredential, Tag};
        let mut state = IamState::new("123456789012");
        state.roles.insert(
            "ops".to_string(),
            IamRole {
                role_name: "ops".into(),
                role_id: "AROAOPS".into(),
                arn: "arn:aws:iam::123456789012:role/ops".into(),
                path: "/".into(),
                assume_role_policy_document: "{}".into(),
                created_at: Utc::now(),
                tags: vec![Tag {
                    key: "Department".into(),
                    value: "engineering".into(),
                }],
                max_session_duration: 3600,
                permissions_boundary: None,
                description: None,
            },
        );
        state.sts_temp_credentials.insert(
            "FSIAOPS".to_string(),
            StsTempCredential {
                access_key_id: "FSIAOPS".into(),
                secret_access_key: "ops-secret".into(),
                session_token: "ops-token".into(),
                principal_arn: "arn:aws:sts::123456789012:assumed-role/ops/session".into(),
                user_id: "AROAOPS:session".into(),
                account_id: "123456789012".into(),
                expiration: Utc::now() + chrono::Duration::minutes(30),
                session_policies: Vec::new(),
                mfa_present: false,
                issued_at: Utc::now(),
                federated_provider: None,
            },
        );
        let resolver = IamCredentialResolver::new(shared(state));
        let resolved = resolver.resolve("FSIAOPS").unwrap();
        let tags = resolved.principal.tags.as_ref().unwrap();
        assert_eq!(
            tags.get("Department").map(|s| s.as_str()),
            Some("engineering")
        );
    }

    #[test]
    fn no_tags_yields_none() {
        let mut state = IamState::new("123456789012");
        state.users.insert(
            "empty".to_string(),
            IamUser {
                user_name: "empty".into(),
                user_id: "AIDAEMPTY".into(),
                arn: "arn:aws:iam::123456789012:user/empty".into(),
                path: "/".into(),
                created_at: Utc::now(),
                tags: Vec::new(),
                permissions_boundary: None,
            },
        );
        state.access_keys.insert(
            "empty".to_string(),
            vec![IamAccessKey {
                access_key_id: "FKIAEMPTY".into(),
                secret_access_key: "s".into(),
                user_name: "empty".into(),
                status: "Active".into(),
                created_at: Utc::now(),
            }],
        );
        let resolver = IamCredentialResolver::new(shared(state));
        let resolved = resolver.resolve("FKIAEMPTY").unwrap();
        assert!(resolved.principal.tags.is_none());
    }
}
