//! KMS implementation of [`ResourcePolicyProvider`].
//!
//! Every KMS key carries a key policy (stored in [`KmsKey::policy`]).
//! `CreateKey` auto-generates a default policy granting `kms:*` to the
//! account root, matching AWS behavior. `PutKeyPolicy` / `GetKeyPolicy`
//! replace and read it. This provider is the read-side bridge into the
//! `fakecloud-core::auth::ResourcePolicyProvider` trait so the IAM
//! evaluator can combine key policies with identity policies.

//!
//! It also answers whether a KMS **grant** authorizes a direct API call the
//! policies left undecided ([`ResourcePolicyProvider::grant_authorization`]),
//! with the same grant matching the service hook uses
//! ([`crate::hook::grant_allows`]).

use std::collections::HashMap;
use std::sync::Arc;

use fakecloud_core::auth::{GrantAuthorization, IamAction, Principal, ResourcePolicyProvider};
use fakecloud_core::service::AwsRequest;

use crate::hook::GrantRequest;
use crate::state::SharedKmsState;

pub struct KmsResourcePolicyProvider {
    state: SharedKmsState,
}

impl KmsResourcePolicyProvider {
    pub fn new(state: SharedKmsState) -> Self {
        Self { state }
    }

    pub fn shared(state: SharedKmsState) -> Arc<dyn ResourcePolicyProvider> {
        Arc::new(Self::new(state))
    }
}

impl ResourcePolicyProvider for KmsResourcePolicyProvider {
    fn resource_policy(&self, service: &str, resource_arn: &str) -> Option<String> {
        if !service.eq_ignore_ascii_case("kms") {
            return None;
        }
        let key_id = parse_key_id_from_arn(resource_arn)?;
        let account_id = resource_arn.split(':').nth(4)?;
        let accounts = self.state.read();
        let state = accounts.get(account_id)?;
        let key = state.keys.get(&key_id)?;
        Some(key.policy.clone())
    }

    /// The account of an existing key, from its ARN: a key ARN of another
    /// account names a cross-account request, which even that account's root
    /// may make only when the key policy or a grant allows it.
    fn resource_owner_account(&self, service: &str, resource_arn: &str) -> Option<String> {
        if !service.eq_ignore_ascii_case("kms") {
            return None;
        }
        let key_id = parse_key_id_from_arn(resource_arn)?;
        let account_id = resource_arn.split(':').nth(4)?;
        let accounts = self.state.read();
        accounts
            .get(account_id)?
            .keys
            .contains_key(&key_id)
            .then(|| account_id.to_string())
    }

    /// Whether a grant on the key lets `principal` perform `action`.
    ///
    /// Only the grant operations can be granted, and a grant on an
    /// AWS-managed key is for the service it belongs to. A grant naming the
    /// calling identity (a user, a federated user or assumed-role session,
    /// or the session's role) authorizes it on its own; one naming the
    /// caller's account (its root, or the bare account id) delegates to the
    /// account. Encryption-context constraints are checked against the
    /// context the operation carries (`SourceEncryptionContext` for
    /// `ReEncryptFrom`, `DestinationEncryptionContext` for `ReEncryptTo`) and
    /// ignored for operations without one; a `CreateGrant` only creates a
    /// grant for some of the parent grant's operations with constraints at
    /// least as strict.
    fn grant_authorization(
        &self,
        service: &str,
        request: &AwsRequest,
        action: &IamAction,
        principal: &Principal,
    ) -> Option<GrantAuthorization> {
        if !service.eq_ignore_ascii_case("kms") {
            return None;
        }
        let key_id = parse_key_id_from_arn(&action.resource)?;
        let account_id = action.resource.split(':').nth(4)?;
        let body = request.json_body();
        let context_of = |field: &str| -> HashMap<String, String> {
            crate::aws_managed::encryption_context_map(&body[field])
                .into_iter()
                .collect()
        };
        let context;
        let operations: Vec<String>;
        let grant_request = match action.action {
            "Encrypt"
            | "Decrypt"
            | "GenerateDataKey"
            | "GenerateDataKeyWithoutPlaintext"
            | "GenerateDataKeyPair"
            | "GenerateDataKeyPairWithoutPlaintext" => {
                context = context_of("EncryptionContext");
                GrantRequest::Context(&context)
            }
            "ReEncryptFrom" => {
                context = context_of("SourceEncryptionContext");
                GrantRequest::Context(&context)
            }
            "ReEncryptTo" => {
                context = context_of("DestinationEncryptionContext");
                GrantRequest::Context(&context)
            }
            "Sign" | "Verify" | "GetPublicKey" | "GenerateMac" | "VerifyMac"
            | "DeriveSharedSecret" | "DescribeKey" | "RetireGrant" => GrantRequest::NoContext,
            "CreateGrant" => {
                operations = body["Operations"]
                    .as_array()
                    .map(|ops| {
                        ops.iter()
                            .filter_map(|op| op.as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_default();
                GrantRequest::CreateGrant {
                    operations: &operations,
                    constraints: &body["Constraints"],
                }
            }
            _ => return None,
        };
        let accounts = self.state.read();
        let state = accounts.get(account_id)?;
        let key = state.keys.get(&key_id)?;
        if key.key_manager == "AWS" {
            return None;
        }
        let grants = |names_grantee: &dyn Fn(&str) -> bool| {
            crate::hook::grant_allows(
                &state.grants,
                key,
                names_grantee,
                action.action,
                grant_request,
            )
        };
        if grants(&|grantee| names_identity(grantee, principal)) {
            return Some(GrantAuthorization::Grantee);
        }
        if grants(&|grantee| names_account(grantee, principal)) {
            return Some(GrantAuthorization::Account);
        }
        None
    }
}

/// Whether a grant's `grantee` principal names the calling identity: its
/// ARN, or for an assumed-role session the role it assumed (named by its IAM
/// ARN, whose path the session ARN leaves out; a role name is unique in its
/// account whatever its path).
fn names_identity(grantee: &str, principal: &Principal) -> bool {
    if grantee == principal.arn {
        return true;
    }
    let Some((account, role)) = crate::hook::assumed_role_parts(&principal.arn) else {
        return false;
    };
    let mut fields = grantee.splitn(6, ':');
    let (Some("arn"), Some(_), Some("iam"), Some(""), Some(grantee_account), Some(resource)) = (
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
        fields.next(),
    ) else {
        return false;
    };
    grantee_account == account
        && resource.starts_with("role/")
        && resource.rsplit('/').next() == Some(role)
}

/// Whether a grant's `grantee` principal names the caller's account: its
/// root ARN in the caller's partition, or the bare account id.
fn names_account(grantee: &str, principal: &Principal) -> bool {
    let partition = principal.arn.split(':').nth(1).unwrap_or("aws");
    grantee == principal.account_id
        || grantee == format!("arn:{partition}:iam::{}:root", principal.account_id)
}

/// Extract the key UUID from a KMS key ARN.
///
/// KMS key ARNs have the form `arn:PARTITION:kms:REGION:ACCOUNT:key/KEY_ID`.
/// Returns `None` for wildcard (`*`) or malformed ARNs.
fn parse_key_id_from_arn(arn: &str) -> Option<String> {
    if arn == "*" {
        return None;
    }
    let (_region, _account, resource) = crate::state::parse_kms_arn(arn)?;
    let key_id = resource.strip_prefix("key/")?;
    if key_id.is_empty() {
        return None;
    }
    Some(key_id.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_key_id_valid_arn() {
        assert_eq!(
            parse_key_id_from_arn("arn:aws:kms:us-east-1:123456789012:key/abc-123"),
            Some("abc-123".to_string())
        );
    }

    #[test]
    fn parse_key_id_wildcard_returns_none() {
        assert_eq!(parse_key_id_from_arn("*"), None);
    }

    #[test]
    fn parse_key_id_not_kms_arn() {
        assert_eq!(parse_key_id_from_arn("arn:aws:s3:::my-bucket"), None);
    }

    #[test]
    fn parse_key_id_empty_key_part() {
        assert_eq!(
            parse_key_id_from_arn("arn:aws:kms:us-east-1:123456789012:key/"),
            None
        );
    }

    #[test]
    fn parse_key_id_mrk_key() {
        assert_eq!(
            parse_key_id_from_arn("arn:aws:kms:us-east-1:123456789012:key/mrk-abc123"),
            Some("mrk-abc123".to_string())
        );
    }

    #[test]
    fn parse_key_id_missing_key_segment() {
        assert_eq!(
            parse_key_id_from_arn("arn:aws:kms:us-east-1:123456789012:alias/my-alias"),
            None
        );
    }

    #[test]
    fn parse_key_id_other_partition() {
        assert_eq!(
            parse_key_id_from_arn("arn:aws-cn:kms:cn-north-1:123:key/abc"),
            Some("abc".to_string())
        );
        assert_eq!(
            parse_key_id_from_arn("arn:bogus:kms:cn-north-1:123:key/abc"),
            None
        );
    }

    fn make_state() -> SharedKmsState {
        use crate::state::KmsState;
        use fakecloud_core::multi_account::MultiAccountState;
        use parking_lot::RwLock;
        let multi: MultiAccountState<KmsState> =
            MultiAccountState::new("123456789012", "us-east-1", "http://localhost");
        Arc::new(RwLock::new(multi))
    }

    #[test]
    fn resource_policy_non_kms_service_returns_none() {
        let state = make_state();
        let provider = KmsResourcePolicyProvider::new(state);
        assert!(provider
            .resource_policy("s3", "arn:aws:kms:us-east-1:123:key/abc")
            .is_none());
    }

    #[test]
    fn resource_policy_unknown_key_returns_none() {
        let state = make_state();
        let provider = KmsResourcePolicyProvider::new(state);
        assert!(provider
            .resource_policy("kms", "arn:aws:kms:us-east-1:123:key/missing")
            .is_none());
    }

    #[test]
    fn resource_policy_case_insensitive_service_name() {
        let state = make_state();
        let provider = KmsResourcePolicyProvider::new(state);
        assert!(provider
            .resource_policy("KMS", "arn:aws:kms:us-east-1:123:key/missing")
            .is_none());
    }

    const KEY_ACCOUNT: &str = "111111111111";
    const KEY_ID: &str = "0b5f2a4e-0000-4000-8000-000000000001";

    fn key_arn() -> String {
        format!("arn:aws:kms:us-east-1:{KEY_ACCOUNT}:key/{KEY_ID}")
    }

    /// State with one customer key in `KEY_ACCOUNT` carrying `grants`
    /// (grantee, operations, constraints).
    fn state_with_grants(grants: &[(&str, &[&str], Option<serde_json::Value>)]) -> SharedKmsState {
        let state = make_state();
        {
            let mut accounts = state.write();
            let s = accounts.get_or_create(KEY_ACCOUNT);
            let (minted, _) = crate::hook::ensure_aws_managed_key(s, "us-east-1", "aws/granted");
            let mut key = s.keys.remove(minted.rsplit('/').next().unwrap()).unwrap();
            key.key_id = KEY_ID.to_string();
            key.arn = key_arn();
            key.key_manager = "CUSTOMER".into();
            s.keys.insert(KEY_ID.to_string(), key);
            for (grantee, operations, constraints) in grants {
                s.grants.push(crate::state::KmsGrant {
                    grant_id: uuid::Uuid::new_v4().to_string(),
                    grant_token: uuid::Uuid::new_v4().to_string(),
                    key_id: KEY_ID.to_string(),
                    grantee_principal: grantee.to_string(),
                    retiring_principal: None,
                    operations: operations.iter().map(|o| o.to_string()).collect(),
                    constraints: constraints.clone(),
                    name: None,
                    creation_date: 0.0,
                });
            }
        }
        state
    }

    fn principal(arn: &str) -> Principal {
        let account = arn.split(':').nth(4).unwrap().to_string();
        Principal {
            arn: arn.to_string(),
            user_id: "AIDAEXAMPLE".into(),
            account_id: account,
            principal_type: fakecloud_core::auth::PrincipalType::from_arn(arn),
            source_identity: None,
            tags: None,
        }
    }

    fn request(body: serde_json::Value) -> AwsRequest {
        AwsRequest {
            service: "kms".into(),
            action: String::new(),
            region: "us-east-1".into(),
            account_id: KEY_ACCOUNT.into(),
            request_id: "rid".into(),
            headers: http::HeaderMap::new(),
            query_params: HashMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    /// The grant match for `caller` performing `action` with `body`.
    fn ask(
        state: &SharedKmsState,
        caller: &str,
        action: &'static str,
        body: serde_json::Value,
    ) -> Option<GrantAuthorization> {
        KmsResourcePolicyProvider::new(state.clone()).grant_authorization(
            "kms",
            &request(body),
            &IamAction {
                service: "kms",
                action,
                resource: key_arn(),
            },
            &principal(caller),
        )
    }

    const USER: &str = "arn:aws:iam::111111111111:user/alice";

    #[test]
    fn a_grant_to_the_caller_covers_only_its_operations() {
        let state = state_with_grants(&[(USER, &["Encrypt", "Decrypt"], None)]);
        let empty = serde_json::json!({});
        assert_eq!(
            ask(&state, USER, "Encrypt", empty.clone()),
            Some(GrantAuthorization::Grantee)
        );
        assert_eq!(
            ask(&state, USER, "Decrypt", empty.clone()),
            Some(GrantAuthorization::Grantee)
        );
        assert_eq!(ask(&state, USER, "GenerateDataKey", empty.clone()), None);
        // Another identity, and another service, get nothing.
        assert_eq!(
            ask(
                &state,
                "arn:aws:iam::111111111111:user/bob",
                "Encrypt",
                empty.clone()
            ),
            None
        );
        assert_eq!(
            KmsResourcePolicyProvider::new(state.clone()).grant_authorization(
                "s3",
                &request(empty.clone()),
                &IamAction {
                    service: "kms",
                    action: "Encrypt",
                    resource: key_arn(),
                },
                &principal(USER),
            ),
            None
        );
    }

    /// Only the grant operations can be granted: an operation outside them
    /// is never authorized by a grant, whatever the grant lists.
    #[test]
    fn a_grant_never_authorizes_a_non_grant_operation() {
        let state = state_with_grants(&[(USER, &["ScheduleKeyDeletion", "PutKeyPolicy"], None)]);
        let empty = serde_json::json!({"KeyId": key_arn()});
        assert_eq!(
            ask(&state, USER, "ScheduleKeyDeletion", empty.clone()),
            None
        );
        assert_eq!(ask(&state, USER, "PutKeyPolicy", empty), None);
    }

    #[test]
    fn a_grant_to_the_account_only_delegates() {
        let other_user = "arn:aws:iam::222222222222:user/carol";
        for grantee in ["arn:aws:iam::222222222222:root", "222222222222"] {
            let state = state_with_grants(&[(grantee, &["Decrypt"], None)]);
            assert_eq!(
                ask(&state, other_user, "Decrypt", serde_json::json!({})),
                Some(GrantAuthorization::Account),
                "{grantee}"
            );
            assert_eq!(ask(&state, USER, "Decrypt", serde_json::json!({})), None);
        }
    }

    /// A session of a role matches a grant to the role, whose ARN carries
    /// the path the session ARN leaves out; another role of the same name in
    /// another account does not.
    #[test]
    fn an_assumed_role_session_matches_a_grant_to_its_role() {
        let session = "arn:aws:sts::111111111111:assumed-role/app/session-1";
        let state = state_with_grants(&[(
            "arn:aws:iam::111111111111:role/team/app",
            &["Encrypt"],
            None,
        )]);
        assert_eq!(
            ask(&state, session, "Encrypt", serde_json::json!({})),
            Some(GrantAuthorization::Grantee)
        );
        assert_eq!(
            ask(
                &state,
                "arn:aws:sts::222222222222:assumed-role/app/session-1",
                "Encrypt",
                serde_json::json!({})
            ),
            None
        );
        assert_eq!(
            ask(
                &state,
                "arn:aws:sts::111111111111:assumed-role/other/s",
                "Encrypt",
                serde_json::json!({})
            ),
            None
        );
    }

    /// Encryption-context constraints are checked against the context the
    /// operation carries, and ignored for operations without one.
    #[test]
    fn encryption_context_constraints_follow_the_operation() {
        let state = state_with_grants(&[(
            USER,
            &["Decrypt", "ReEncryptFrom", "ReEncryptTo", "DescribeKey"],
            Some(serde_json::json!({"EncryptionContextSubset": {"Department": "IT"}})),
        )]);
        let granted = Some(GrantAuthorization::Grantee);
        let it = serde_json::json!({"Department": "IT", "Purpose": "Test"});
        assert_eq!(
            ask(
                &state,
                USER,
                "Decrypt",
                serde_json::json!({"EncryptionContext": it})
            ),
            granted
        );
        assert_eq!(
            ask(
                &state,
                USER,
                "Decrypt",
                serde_json::json!({"EncryptionContext": {"Department": "HR"}})
            ),
            None
        );
        assert_eq!(ask(&state, USER, "Decrypt", serde_json::json!({})), None);
        // ReEncrypt: the source context for ReEncryptFrom, the destination
        // context for ReEncryptTo.
        let re_encrypt = serde_json::json!({
            "SourceEncryptionContext": {"Department": "IT"},
            "DestinationEncryptionContext": {"Department": "HR"},
        });
        assert_eq!(
            ask(&state, USER, "ReEncryptFrom", re_encrypt.clone()),
            granted
        );
        assert_eq!(ask(&state, USER, "ReEncryptTo", re_encrypt), None);
        // DescribeKey takes no context: the constraint is ignored.
        assert_eq!(
            ask(&state, USER, "DescribeKey", serde_json::json!({})),
            granted
        );

        let equals = state_with_grants(&[(
            USER,
            &["Decrypt"],
            Some(serde_json::json!({"EncryptionContextEquals": {"Department": "IT"}})),
        )]);
        assert_eq!(
            ask(
                &equals,
                USER,
                "Decrypt",
                serde_json::json!({"EncryptionContext": {"Department": "IT"}})
            ),
            granted
        );
        assert_eq!(
            ask(
                &equals,
                USER,
                "Decrypt",
                serde_json::json!({"EncryptionContext": it})
            ),
            None
        );
    }

    /// A grant with `CreateGrant` only lets its grantee create grants for
    /// some of its own operations, with constraints at least as strict.
    #[test]
    fn create_grant_from_a_grant_only_delegates_what_it_holds() {
        let state = state_with_grants(&[(
            USER,
            &["CreateGrant", "Decrypt", "GenerateDataKey"],
            Some(serde_json::json!({"EncryptionContextSubset": {"Department": "IT"}})),
        )]);
        let create = |operations: &[&str], constraints: serde_json::Value| {
            ask(
                &state,
                USER,
                "CreateGrant",
                serde_json::json!({"Operations": operations, "Constraints": constraints}),
            )
        };
        let granted = Some(GrantAuthorization::Grantee);
        let subset =
            serde_json::json!({"EncryptionContextSubset": {"Department": "IT", "Team": "a"}});
        assert_eq!(create(&["Decrypt"], subset.clone()), granted);
        assert_eq!(
            create(
                &["Decrypt", "CreateGrant"],
                serde_json::json!({"EncryptionContextEquals": {"Department": "IT"}})
            ),
            granted
        );
        // An operation the parent lacks, a dropped pair, or no constraint.
        assert_eq!(create(&["Encrypt"], subset), None);
        assert_eq!(
            create(
                &["Decrypt"],
                serde_json::json!({"EncryptionContextSubset": {"Team": "a"}})
            ),
            None
        );
        assert_eq!(create(&["Decrypt"], serde_json::Value::Null), None);

        // A parent's EncryptionContextEquals must be kept exactly.
        let equals = state_with_grants(&[(
            USER,
            &["CreateGrant", "Decrypt"],
            Some(serde_json::json!({"EncryptionContextEquals": {"Department": "IT"}})),
        )]);
        let create_equals = |constraints: serde_json::Value| {
            ask(
                &equals,
                USER,
                "CreateGrant",
                serde_json::json!({"Operations": ["Decrypt"], "Constraints": constraints}),
            )
        };
        assert_eq!(
            create_equals(serde_json::json!({"EncryptionContextEquals": {"Department": "IT"}})),
            granted
        );
        assert_eq!(
            create_equals(serde_json::json!({"EncryptionContextSubset": {"Department": "IT"}})),
            None
        );
    }

    /// A grant on an AWS-managed key is the owning service's, never a
    /// direct caller's.
    #[test]
    fn grants_on_aws_managed_keys_do_not_authorize_direct_calls() {
        let state = state_with_grants(&[(USER, &["Decrypt"], None)]);
        state
            .write()
            .get_or_create(KEY_ACCOUNT)
            .keys
            .get_mut(KEY_ID)
            .unwrap()
            .key_manager = "AWS".into();
        assert_eq!(ask(&state, USER, "Decrypt", serde_json::json!({})), None);
    }

    #[test]
    fn resource_owner_account_is_the_existing_keys_account() {
        let state = state_with_grants(&[]);
        let provider = KmsResourcePolicyProvider::new(state);
        assert_eq!(
            provider
                .resource_owner_account("kms", &key_arn())
                .as_deref(),
            Some(KEY_ACCOUNT)
        );
        assert_eq!(
            provider
                .resource_owner_account("kms", "arn:aws:kms:us-east-1:111111111111:key/missing"),
            None
        );
        assert_eq!(provider.resource_owner_account("kms", "*"), None);
        assert_eq!(provider.resource_owner_account("s3", &key_arn()), None);
    }

    #[test]
    fn shared_helper_returns_arc_provider() {
        let state = make_state();
        let arc: Arc<dyn ResourcePolicyProvider> = KmsResourcePolicyProvider::shared(state);
        assert!(arc
            .resource_policy("kms", "arn:aws:kms:us-east-1:123:key/missing")
            .is_none());
    }
}
