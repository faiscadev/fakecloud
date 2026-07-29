use std::collections::BTreeMap;

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{
    AccessTokenData, AuthEvent, ChallengeResult, CognitoState, PreTokenGenInvocation,
    RefreshTokenData, SessionData, SharedCognitoState, User, UserAttribute, UserPool,
};
use crate::triggers::{self, TriggerSource};
use crate::user_status;

use super::{
    ensure_user_pool_exists, generate_confirmation_code, parse_user_attributes, require_str,
    validate_password, CognitoService, TokenSet,
};

struct AdminAuthInput {
    pool_id: String,
    client_id: String,
    auth_flow: String,
    username: String,
    password: String,
}

impl AdminAuthInput {
    fn from_request(body: &Value) -> Result<Self, AwsServiceError> {
        let pool_id = require_str(body, "UserPoolId")?.to_string();
        let client_id = require_str(body, "ClientId")?.to_string();
        let auth_flow = require_str(body, "AuthFlow")?.to_string();

        match auth_flow.as_str() {
            "ADMIN_NO_SRP_AUTH" | "ADMIN_USER_PASSWORD_AUTH" => {}
            other => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Unsupported auth flow: {other}"),
                ));
            }
        }

        let auth_params = body["AuthParameters"].as_object().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "AuthParameters is required",
            )
        })?;

        let username = auth_params
            .get("USERNAME")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "USERNAME is required in AuthParameters",
                )
            })?
            .to_string();

        let password = auth_params
            .get("PASSWORD")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "PASSWORD is required in AuthParameters",
                )
            })?
            .to_string();

        Ok(Self {
            pool_id,
            client_id,
            auth_flow,
            username,
            password,
        })
    }
}

/// Per-request snapshot collected under the read lock that the trigger
/// invocation needs after the lock is dropped.
struct AdminAuthLookup {
    user_attrs: Vec<UserAttribute>,
    region: String,
    account_id: String,
}

enum AdminAuthOutcome {
    Tokens(TokenSet),
    NewPasswordRequired {
        session: String,
    },
    /// A second-factor MFA challenge (SOFTWARE_TOKEN_MFA / SMS_MFA / MFA_SETUP)
    /// that must be satisfied before tokens are minted.
    MfaChallenge {
        challenge_name: &'static str,
        session: String,
        /// Masked destination for SMS_MFA CodeDeliveryDetails, if any.
        sms_destination: Option<String>,
    },
}

/// Decide whether a successful first authentication factor (password or SRP)
/// must be followed by a second-factor MFA challenge, and which one.
///
/// Returns the AWS `ChallengeName` to issue, or `None` to proceed straight to
/// tokens. Mirrors real Cognito: a pool with `MfaConfiguration = OFF` never
/// challenges; `ON` always requires a second factor (falling back to
/// `MFA_SETUP` when the user has enrolled none); `OPTIONAL` challenges only
/// users who have actually enabled a factor.
fn required_mfa_challenge(pool: &UserPool, user: &User) -> Option<&'static str> {
    let cfg = pool.mfa_configuration.as_str();
    if !matches!(cfg, "ON" | "OPTIONAL") {
        // "OFF" (or any unrecognized/empty value) => no MFA.
        return None;
    }

    let prefs = user.mfa_preferences.as_ref();
    // A verified TOTP counts as software-token MFA unless the user explicitly
    // disabled it via SetUserMFAPreference.
    let software = user.totp_verified && prefs.map(|p| p.software_token_enabled).unwrap_or(true);
    // SMS MFA needs both the preference and a phone number to deliver to.
    let sms = prefs.map(|p| p.sms_enabled).unwrap_or(false)
        && user.attributes.iter().any(|a| a.name == "phone_number");

    match (software, sms) {
        (true, true) => {
            if prefs.map(|p| p.sms_preferred).unwrap_or(false) {
                Some("SMS_MFA")
            } else {
                Some("SOFTWARE_TOKEN_MFA")
            }
        }
        (true, false) => Some("SOFTWARE_TOKEN_MFA"),
        (false, true) => Some("SMS_MFA"),
        // No usable factor enrolled: a mandatory pool forces setup; an
        // OPTIONAL pool lets the user through.
        (false, false) => (cfg == "ON").then_some("MFA_SETUP"),
    }
}

/// Mask a phone number for `CodeDeliveryDetails`, e.g. `+15551234567` ->
/// `+*******4567`, matching how Cognito reports the SMS destination.
fn mask_phone(phone: &str) -> String {
    let n = phone.chars().count();
    if n <= 4 {
        return "*".repeat(n);
    }
    let visible: String = phone.chars().skip(n - 4).collect();
    format!("{}{}", "*".repeat(n - 4), visible)
}

mod admin;
mod challenges;
mod compromised;
mod initiate;
mod password;
mod signout;
mod signup;

impl CognitoService {}

/// Append one PreTokenGeneration trigger invocation to the per-account
/// introspection log. Pulls out the override block already so callers
/// at `/_fakecloud/cognito/pretokengen/invocations` get parsed
/// `claims_added` / `claims_overridden` / `group_overrides` instead of
/// having to walk the raw Lambda response themselves.
#[allow(clippy::too_many_arguments)]
pub(crate) fn record_pre_token_gen_invocation(
    state: &SharedCognitoState,
    account_id_key: &str,
    pool_id: &str,
    region: &str,
    account_id: &str,
    username: &str,
    function_arn: &str,
    event: &Value,
    raw_response: Option<&Value>,
    invoked_at: chrono::DateTime<Utc>,
    duration_ms: u64,
) {
    let user_pool_arn = format!("arn:aws:cognito-idp:{region}:{account_id}:userpool/{pool_id}");

    let mut claims_added: Vec<String> = Vec::new();
    let mut claims_overridden: Vec<String> = Vec::new();
    let mut group_overrides: Vec<String> = Vec::new();

    if let Some(resp) = raw_response {
        let ov = &resp["response"];
        let v2 = &ov["claimsAndScopeOverrideDetails"];
        let v1 = &ov["claimsOverrideDetails"];
        let id_block = if !v2.is_null() {
            &v2["idTokenGeneration"]
        } else {
            v1
        };
        let access_block = if !v2.is_null() {
            &v2["accessTokenGeneration"]
        } else {
            v1
        };
        let group_block = if !v2.is_null() {
            &v2["groupOverrideDetails"]
        } else {
            &v1["groupOverrideDetails"]
        };
        for block in [id_block, access_block] {
            if let Some(adds) = block["claimsToAddOrOverride"].as_object() {
                for k in adds.keys() {
                    if !claims_added.contains(k) {
                        claims_added.push(k.clone());
                    }
                }
            }
            if let Some(suppress) = block["claimsToSuppress"].as_array() {
                for v in suppress {
                    if let Some(k) = v.as_str() {
                        let k = k.to_string();
                        if !claims_overridden.contains(&k) {
                            claims_overridden.push(k);
                        }
                    }
                }
            }
        }
        if let Some(arr) = group_block["groupsToOverride"].as_array() {
            for v in arr {
                if let Some(s) = v.as_str() {
                    group_overrides.push(s.to_string());
                }
            }
        }
    }

    let invocation = PreTokenGenInvocation {
        pool_id: pool_id.to_string(),
        user_pool_arn,
        username: username.to_string(),
        trigger_source: TriggerSource::TokenGenerationAuthentication
            .as_str()
            .to_string(),
        lambda_arn: function_arn.to_string(),
        request_payload: event.clone(),
        response_payload: raw_response.cloned(),
        claims_added,
        claims_overridden,
        group_overrides,
        invoked_at,
        duration_ms,
    };

    let mut accounts = state.write();
    let s = accounts.get_or_create(account_id_key);
    s.pre_token_gen_invocations.push(invocation);
}

#[cfg(test)]
mod risk_tests {
    use super::*;
    use serde_json::json;

    fn make_service() -> CognitoService {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        CognitoService::new(state)
    }

    fn seed_compromised(svc: &CognitoService, password: &str) {
        use sha2::{Digest, Sha256};
        let mut accounts = svc.state.write();
        let s = accounts.get_or_create("123456789012");
        let mut hasher = Sha256::new();
        hasher.update(password.as_bytes());
        let hash = format!("{:x}", hasher.finalize());
        s.compromised_password_hashes.insert(hash);
    }

    fn seed_risk_block(svc: &CognitoService, pool_id: &str, client_id: &str) {
        let mut accounts = svc.state.write();
        let s = accounts.get_or_create("123456789012");
        let key = format!("{pool_id}:{client_id}");
        s.risk_configurations.insert(
            key,
            json!({
                "CompromisedCredentialsRiskConfiguration": {
                    "Actions": {"EventAction": "BLOCK"}
                }
            }),
        );
    }

    #[test]
    fn no_risk_config_passes() {
        let svc = make_service();
        seed_compromised(&svc, "Password123!");
        let res = svc.evaluate_compromised_credentials(
            "123456789012",
            "pool-x",
            "client-x",
            "Password123!",
        );
        assert!(res.is_ok());
    }

    #[test]
    fn block_rejects_compromised_password() {
        let svc = make_service();
        seed_compromised(&svc, "Password123!");
        seed_risk_block(&svc, "pool-x", "client-x");
        let err = svc
            .evaluate_compromised_credentials("123456789012", "pool-x", "client-x", "Password123!")
            .unwrap_err();
        match err {
            AwsServiceError::AwsError { code, .. } => assert_eq!(code, "NotAuthorizedException"),
            other => panic!("expected AwsError, got {other:?}"),
        }
    }

    #[test]
    fn block_accepts_clean_password() {
        let svc = make_service();
        seed_compromised(&svc, "Password123!");
        seed_risk_block(&svc, "pool-x", "client-x");
        let res = svc.evaluate_compromised_credentials(
            "123456789012",
            "pool-x",
            "client-x",
            "DifferentPassword!",
        );
        assert!(res.is_ok());
    }

    #[test]
    fn pretokengen_invocation_records_parsed_overrides() {
        let svc = make_service();
        let event = json!({
            "version": "2",
            "triggerSource": "TokenGeneration_Authentication",
            "userPoolId": "pool-x",
            "userName": "alice",
        });
        let response = json!({
            "response": {
                "claimsAndScopeOverrideDetails": {
                    "idTokenGeneration": {
                        "claimsToAddOrOverride": {"role": "admin", "tier": "gold"},
                        "claimsToSuppress": ["email"],
                    },
                    "accessTokenGeneration": {
                        "claimsToAddOrOverride": {"scope_extra": "x"},
                        "claimsToSuppress": ["phone_number"],
                    },
                    "groupOverrideDetails": {
                        "groupsToOverride": ["admins", "beta"],
                    },
                }
            }
        });
        record_pre_token_gen_invocation(
            &svc.state,
            "123456789012",
            "pool-x",
            "us-east-1",
            "123456789012",
            "alice",
            "arn:aws:lambda:us-east-1:123456789012:function:pretoken",
            &event,
            Some(&response),
            chrono::Utc::now(),
            42,
        );

        let accounts = svc.state.read();
        let s = accounts.get("123456789012").expect("account exists");
        assert_eq!(s.pre_token_gen_invocations.len(), 1);
        let inv = &s.pre_token_gen_invocations[0];
        assert_eq!(inv.pool_id, "pool-x");
        assert_eq!(inv.username, "alice");
        assert_eq!(
            inv.user_pool_arn,
            "arn:aws:cognito-idp:us-east-1:123456789012:userpool/pool-x"
        );
        assert_eq!(
            inv.lambda_arn,
            "arn:aws:lambda:us-east-1:123456789012:function:pretoken"
        );
        assert_eq!(inv.duration_ms, 42);
        assert!(inv.claims_added.contains(&"role".to_string()));
        assert!(inv.claims_added.contains(&"tier".to_string()));
        assert!(inv.claims_added.contains(&"scope_extra".to_string()));
        assert!(inv.claims_overridden.contains(&"email".to_string()));
        assert!(inv.claims_overridden.contains(&"phone_number".to_string()));
        assert_eq!(
            inv.group_overrides,
            vec!["admins".to_string(), "beta".to_string()]
        );
        assert_eq!(inv.trigger_source, "TokenGeneration_Authentication");
    }

    #[test]
    fn pretokengen_invocation_records_no_overrides() {
        let svc = make_service();
        let event = json!({"triggerSource": "TokenGeneration_Authentication"});
        record_pre_token_gen_invocation(
            &svc.state,
            "123456789012",
            "pool-x",
            "us-east-1",
            "123456789012",
            "bob",
            "arn:aws:lambda:us-east-1:123456789012:function:pretoken",
            &event,
            None,
            chrono::Utc::now(),
            7,
        );
        let accounts = svc.state.read();
        let s = accounts.get("123456789012").expect("account exists");
        assert_eq!(s.pre_token_gen_invocations.len(), 1);
        let inv = &s.pre_token_gen_invocations[0];
        assert!(inv.claims_added.is_empty());
        assert!(inv.claims_overridden.is_empty());
        assert!(inv.group_overrides.is_empty());
        assert!(inv.response_payload.is_none());
    }

    #[test]
    fn audit_only_does_not_block() {
        let svc = make_service();
        seed_compromised(&svc, "Password123!");
        {
            let mut accounts = svc.state.write();
            let s = accounts.get_or_create("123456789012");
            s.risk_configurations.insert(
                "pool-x:client-x".to_string(),
                json!({
                    "CompromisedCredentialsRiskConfiguration": {
                        "Actions": {"EventAction": "NO_ACTION"}
                    }
                }),
            );
        }
        let res = svc.evaluate_compromised_credentials(
            "123456789012",
            "pool-x",
            "client-x",
            "Password123!",
        );
        assert!(res.is_ok());
    }
}
