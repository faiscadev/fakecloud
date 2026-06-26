pub mod jwt;
pub(crate) mod service;
pub mod srp;
pub(crate) mod state;
pub mod triggers;
pub mod user_status;
pub mod webauthn;

pub use service::{
    ensure_pool_signing_key, handle_oauth2_authorize, handle_oauth2_revoke, handle_oauth2_token,
    handle_oauth2_userinfo, mint_authorization_code, oidc_discovery_document,
    pool_existence_and_domain, pool_jwks_document, save_cognito_snapshot, CognitoIdentityService,
    CognitoService, MintAuthorizationCodeError, MintAuthorizationCodeRequest, OAuth2AuthorizeError,
    OAuth2AuthorizeOutcome, OAuth2AuthorizeRequest, OAuthRevokeError, OAuthTokenError,
    OAuthTokenResponse, OAuthUserInfoError,
};
pub use state::{
    default_schema_attributes, AccountRecoverySetting, AdminCreateUserConfig,
    AuthorizationCodeData, CognitoIdentityProvider, CognitoSnapshot, CognitoState,
    CustomDomainConfig, EmailConfiguration, FederatedIdentity, IdentityPool,
    IdentityPoolRoleAttachment, PasswordPolicy, PoolPolicies, PreTokenGenInvocation,
    RecoveryOption, SchemaAttribute, SharedCognitoState, SignInPolicy, SmsConfiguration, UserPool,
    UserPoolClient, UserPoolDomain, COGNITO_SNAPSHOT_SCHEMA_VERSION,
};

/// `CognitoJwtVerifier` impl backed by the in-process Cognito state.
/// Wired by fakecloud-server so cross-service consumers (API Gateway v1
/// `COGNITO_USER_POOLS` authorizer) can verify pool-issued JWTs without
/// taking a hard dep on `fakecloud-cognito`.
pub struct StateBackedJwtVerifier {
    state: SharedCognitoState,
}

impl StateBackedJwtVerifier {
    pub fn new(state: SharedCognitoState) -> Self {
        Self { state }
    }
}

impl fakecloud_core::delivery::CognitoJwtVerifier for StateBackedJwtVerifier {
    fn verify_token(
        &self,
        account_id: &str,
        user_pool_arn: &str,
        token: &str,
    ) -> Result<serde_json::Value, String> {
        // Resolve the pool by ARN inside the requested account. ARN form:
        // `arn:aws:cognito-idp:<region>:<account>:userpool/<pool-id>`.
        let pool_id = user_pool_arn
            .rsplit_once("userpool/")
            .map(|(_, id)| id.to_string())
            .ok_or_else(|| format!("invalid Cognito user pool ARN: {user_pool_arn}"))?;
        let accounts = self.state.read();
        let state = accounts
            .get(account_id)
            .ok_or_else(|| format!("no Cognito state for account {account_id}"))?;
        let pool = state
            .user_pools
            .get(&pool_id)
            .ok_or_else(|| format!("user pool {pool_id} not found"))?;
        let pem = pool
            .signing_key_pem
            .as_deref()
            .ok_or_else(|| format!("user pool {pool_id} has no signing key"))?;
        let (_header, payload) = jwt::verify_rs256(token, pem)?;

        // Validate exp and iss now that the signature has been confirmed.
        // Cognito-issued tokens always carry both. Treat a missing `exp`
        // as invalid (fail closed): a signed token with no expiry claim
        // would otherwise be accepted as never-expiring, which is exactly
        // the bypass an attacker minting a long-lived/forged-but-signed
        // token would want.
        let exp = payload
            .get("exp")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| "token missing exp claim".to_string())?;
        let now = chrono::Utc::now().timestamp();
        if now >= exp {
            return Err("token expired".to_string());
        }
        if let Some(iss) = payload.get("iss").and_then(|v| v.as_str()) {
            // `iss` matches `https://cognito-idp.<region>.amazonaws.com/<pool-id>`
            // exactly for pools that haven't customized their issuer.
            if !iss.ends_with(&format!("/{pool_id}")) {
                return Err(format!("token issuer {iss} does not match pool {pool_id}"));
            }
        }
        Ok(payload)
    }
}

#[cfg(test)]
mod jwt_exp_tests {
    //! bug-hunt 2026-06-15 §5.5: a signed token missing the `exp` claim was
    //! accepted as never-expiring. It must now fail closed.
    use super::*;
    use fakecloud_core::delivery::CognitoJwtVerifier;
    use parking_lot::RwLock;
    use std::sync::Arc;

    const POOL_ID: &str = "us-east-1_pool";
    const POOL_ARN: &str = "arn:aws:cognito-idp:us-east-1:123456789012:userpool/us-east-1_pool";

    fn seed_pool_with_key() -> (SharedCognitoState, String) {
        let signing = jwt::generate_pool_signing_key();
        let pem = signing.private_key_pem.clone();
        let state: SharedCognitoState = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(
                "123456789012",
                "us-east-1",
                "http://localhost:4569",
            ),
        ));
        {
            let mut mas = state.write();
            let s = mas.default_mut();
            let pool = UserPool {
                id: POOL_ID.to_string(),
                name: "TestPool".to_string(),
                arn: POOL_ARN.to_string(),
                status: "Enabled".to_string(),
                creation_date: chrono::Utc::now(),
                last_modified_date: chrono::Utc::now(),
                policies: PoolPolicies {
                    password_policy: PasswordPolicy::default(),
                    sign_in_policy: SignInPolicy {
                        allowed_first_auth_factors: vec!["PASSWORD".to_string()],
                    },
                },
                auto_verified_attributes: vec![],
                username_attributes: None,
                alias_attributes: None,
                schema_attributes: vec![],
                lambda_config: None,
                mfa_configuration: "OFF".to_string(),
                email_configuration: None,
                sms_configuration: None,
                admin_create_user_config: None,
                user_pool_tags: std::collections::BTreeMap::new(),
                account_recovery_setting: None,
                deletion_protection: None,
                estimated_number_of_users: 0,
                software_token_mfa_configuration: None,
                sms_mfa_configuration: None,
                user_pool_tier: "ESSENTIALS".to_string(),
                verification_message_template: None,
                signing_key_pem: Some(pem.clone()),
                signing_kid: Some(signing.kid),
                email_verification_message: None,
                email_verification_subject: None,
                sms_verification_message: None,
                sms_authentication_message: None,
                device_configuration: None,
                user_attribute_update_settings: None,
                user_pool_add_ons: None,
                username_configuration: None,
            };
            s.user_pools.insert(POOL_ID.to_string(), pool);
        }
        (state, pem)
    }

    fn sign(payload: serde_json::Value, pem: &str) -> String {
        let header = serde_json::json!({ "alg": "RS256", "typ": "JWT", "kid": "test" });
        jwt::sign_rs256(&header, &payload, pem).expect("sign")
    }

    #[test]
    fn token_with_valid_exp_is_accepted() {
        let (state, pem) = seed_pool_with_key();
        let verifier = StateBackedJwtVerifier::new(state);
        let future = chrono::Utc::now().timestamp() + 3600;
        let token = sign(
            serde_json::json!({
                "sub": "user-1",
                "exp": future,
                "iss": format!("https://cognito-idp.us-east-1.amazonaws.com/{POOL_ID}"),
            }),
            &pem,
        );
        let claims = verifier
            .verify_token("123456789012", POOL_ARN, &token)
            .expect("valid token should verify");
        assert_eq!(claims.get("sub").and_then(|v| v.as_str()), Some("user-1"));
    }

    #[test]
    fn token_missing_exp_is_rejected() {
        let (state, pem) = seed_pool_with_key();
        let verifier = StateBackedJwtVerifier::new(state);
        // Signature is valid; only the exp claim is absent.
        let token = sign(
            serde_json::json!({
                "sub": "user-1",
                "iss": format!("https://cognito-idp.us-east-1.amazonaws.com/{POOL_ID}"),
            }),
            &pem,
        );
        let err = verifier
            .verify_token("123456789012", POOL_ARN, &token)
            .expect_err("token without exp must be rejected (fail closed)");
        assert!(
            err.contains("exp"),
            "expected an exp-related rejection, got: {err}"
        );
    }

    #[test]
    fn expired_token_is_rejected() {
        let (state, pem) = seed_pool_with_key();
        let verifier = StateBackedJwtVerifier::new(state);
        let past = chrono::Utc::now().timestamp() - 60;
        let token = sign(
            serde_json::json!({
                "sub": "user-1",
                "exp": past,
                "iss": format!("https://cognito-idp.us-east-1.amazonaws.com/{POOL_ID}"),
            }),
            &pem,
        );
        let err = verifier
            .verify_token("123456789012", POOL_ARN, &token)
            .expect_err("expired token must be rejected");
        assert!(err.contains("expired"), "got: {err}");
    }
}
