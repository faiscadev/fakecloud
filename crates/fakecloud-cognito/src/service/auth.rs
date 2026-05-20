use std::collections::BTreeMap;

use chrono::Utc;
use fakecloud_aws::arn::Arn;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{
    AccessTokenData, AuthEvent, ChallengeResult, CognitoState, PreTokenGenInvocation,
    RefreshTokenData, SessionData, SharedCognitoState, UserAttribute,
};
use crate::triggers::{self, TriggerSource};
use crate::user_status;

use super::{
    ensure_user_pool_exists, generate_confirmation_code, generate_tokens, parse_user_attributes,
    require_str, validate_password, CognitoService, TokenSet,
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
    NewPasswordRequired { session: String },
}

impl CognitoService {
    /// CompromisedCredentialsRiskConfiguration enforcement.
    /// Returns `Err(NotAuthorizedException)` when the pool's risk
    /// config has `Actions.EventAction = BLOCK` and the password
    /// matches a known compromised hash.
    pub(super) fn evaluate_compromised_credentials(
        &self,
        account_id: &str,
        pool_id: &str,
        client_id: &str,
        password: &str,
    ) -> Result<(), AwsServiceError> {
        use sha2::{Digest, Sha256};
        let accounts = self.state.read();
        let Some(state) = accounts.get(account_id) else {
            return Ok(());
        };
        // Risk config keyed by (pool, client) with a fallback to (pool, "").
        let pool_key = format!("{pool_id}:{client_id}");
        let pool_default = format!("{pool_id}:");
        let cfg = state
            .risk_configurations
            .get(&pool_key)
            .or_else(|| state.risk_configurations.get(&pool_default));
        let Some(cfg) = cfg else {
            return Ok(());
        };
        let action = cfg["CompromisedCredentialsRiskConfiguration"]["Actions"]["EventAction"]
            .as_str()
            .unwrap_or("");
        if !action.eq_ignore_ascii_case("BLOCK") {
            return Ok(());
        }
        let mut hasher = Sha256::new();
        hasher.update(password.as_bytes());
        let hash = format!("{:x}", hasher.finalize());
        if state.compromised_password_hashes.contains(&hash) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Password has been found in a previous data breach. \
                 This password cannot be used. Please use a different password.",
            ));
        }
        Ok(())
    }

    pub(super) async fn admin_initiate_auth(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let input = AdminAuthInput::from_request(&req.json_body())?;
        let lookup = self.admin_auth_lookup(&input, req)?;

        if let Some(ctx) = self.delivery_ctx.as_ref() {
            if let Some(function_arn) = triggers::get_trigger_arn(
                &self.state,
                &input.pool_id,
                TriggerSource::PreAuthenticationAuthentication,
            ) {
                let event = triggers::build_trigger_event(
                    TriggerSource::PreAuthenticationAuthentication,
                    &input.pool_id,
                    Some(&input.client_id),
                    &input.username,
                    &lookup.user_attrs,
                    &lookup.region,
                    &lookup.account_id,
                );
                if triggers::invoke_trigger(ctx, &function_arn, &event)
                    .await
                    .is_none()
                {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "NotAuthorizedException",
                        "PreAuthentication Lambda trigger rejected the request.",
                    ));
                }
            }
        }

        let tokens = match self.admin_auth_verify(&input, &lookup.region, req)? {
            AdminAuthOutcome::NewPasswordRequired { session } => {
                return Ok(AwsResponse::ok_json(json!({
                    "ChallengeName": "NEW_PASSWORD_REQUIRED",
                    "Session": session,
                    "ChallengeParameters": {
                        "USER_ID_FOR_SRP": input.username,
                        "requiredAttributes": "[]",
                        "userAttributes": "{}"
                    }
                })));
            }
            AdminAuthOutcome::Tokens(tokens) => tokens,
        };

        if let Some(ctx) = self.delivery_ctx.as_ref() {
            if let Some(function_arn) = triggers::get_trigger_arn(
                &self.state,
                &input.pool_id,
                TriggerSource::PostAuthenticationAuthentication,
            ) {
                let event = triggers::build_trigger_event(
                    TriggerSource::PostAuthenticationAuthentication,
                    &input.pool_id,
                    Some(&input.client_id),
                    &input.username,
                    &lookup.user_attrs,
                    &lookup.region,
                    &lookup.account_id,
                );
                triggers::invoke_trigger_fire_and_forget(ctx, function_arn, event);
            }
        }

        Ok(AwsResponse::ok_json(json!({
            "AuthenticationResult": {
                "AccessToken": tokens.access_token,
                "IdToken": tokens.id_token,
                "RefreshToken": tokens.refresh_token,
                "TokenType": "Bearer",
                "ExpiresIn": 3600
            }
        })))
    }

    fn admin_auth_lookup(
        &self,
        input: &AdminAuthInput,
        req: &AwsRequest,
    ) -> Result<AdminAuthLookup, AwsServiceError> {
        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        ensure_user_pool_exists(state, &input.pool_id)?;

        let client = state
            .user_pool_clients
            .get(&input.client_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("User pool client {} does not exist.", input.client_id),
                )
            })?;
        if client.user_pool_id != input.pool_id {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool client {} does not exist.", input.client_id),
            ));
        }

        let allowed = match input.auth_flow.as_str() {
            "ADMIN_NO_SRP_AUTH" => client
                .explicit_auth_flows
                .iter()
                .any(|f| f == "ADMIN_NO_SRP_AUTH" || f == "ALLOW_ADMIN_USER_PASSWORD_AUTH"),
            "ADMIN_USER_PASSWORD_AUTH" => client
                .explicit_auth_flows
                .iter()
                .any(|f| f == "ADMIN_USER_PASSWORD_AUTH" || f == "ALLOW_ADMIN_USER_PASSWORD_AUTH"),
            _ => false,
        };
        if !allowed {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "Client is not allowed for this auth flow.",
            ));
        }

        let user = state
            .users
            .get(&input.pool_id)
            .and_then(|users| users.get(&input.username))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "UserNotFoundException",
                    "User does not exist.",
                )
            })?;

        if !user.enabled {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "User is disabled.",
            ));
        }

        Ok(AdminAuthLookup {
            user_attrs: triggers::collect_user_attributes(user),
            region: state.region.clone(),
            account_id: state.account_id.clone(),
        })
    }

    fn admin_auth_verify(
        &self,
        input: &AdminAuthInput,
        region: &str,
        req: &AwsRequest,
    ) -> Result<AdminAuthOutcome, AwsServiceError> {
        self.evaluate_compromised_credentials(
            &req.account_id,
            &input.pool_id,
            &input.client_id,
            &input.password,
        )?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let user = state
            .users
            .get(&input.pool_id)
            .and_then(|users| users.get(&input.username))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "UserNotFoundException",
                    "User does not exist.",
                )
            })?;

        let password_matches = match (&user.password, &user.temporary_password) {
            (Some(p), _) if p == &input.password => true,
            (_, Some(tp)) if tp == &input.password => true,
            _ => false,
        };
        if !password_matches {
            state.auth_events.push(AuthEvent {
                event_id: Uuid::new_v4().to_string(),
                event_type: "SIGN_IN_FAILURE".to_string(),
                username: input.username.clone(),
                user_pool_id: input.pool_id.clone(),
                client_id: Some(input.client_id.clone()),
                timestamp: Utc::now(),
                success: false,
                feedback_value: None,
            });
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Incorrect username or password.",
            ));
        }

        if user.user_status == user_status::FORCE_CHANGE_PASSWORD {
            let session = Uuid::new_v4().to_string();
            state.sessions.insert(
                session.clone(),
                SessionData {
                    user_pool_id: input.pool_id.clone(),
                    username: input.username.clone(),
                    client_id: input.client_id.clone(),
                    challenge_name: "NEW_PASSWORD_REQUIRED".to_string(),
                    challenge_results: vec![],
                    challenge_metadata: None,
                },
            );
            return Ok(AdminAuthOutcome::NewPasswordRequired { session });
        }

        let sub = user.sub.clone();
        let pool_signing_owned = state.user_pools.get(&input.pool_id).and_then(|pool| {
            pool.signing_key_pem
                .as_ref()
                .zip(pool.signing_kid.as_ref())
                .map(|(p, k)| (p.clone(), k.clone()))
        });
        let signing = pool_signing_owned
            .as_ref()
            .map(|(p, k)| (p.as_str(), k.as_str()));
        let tokens = generate_tokens(
            &input.pool_id,
            &input.client_id,
            &sub,
            &input.username,
            region,
            signing,
        );

        state.refresh_tokens.insert(
            tokens.refresh_token.clone(),
            RefreshTokenData {
                user_pool_id: input.pool_id.clone(),
                username: input.username.clone(),
                client_id: input.client_id.clone(),
                issued_at: Utc::now(),
            },
        );

        state.access_tokens.insert(
            tokens.access_token.clone(),
            AccessTokenData {
                user_pool_id: input.pool_id.clone(),
                username: input.username.clone(),
                client_id: input.client_id.clone(),
                issued_at: Utc::now(),
            },
        );

        state.auth_events.push(AuthEvent {
            event_id: Uuid::new_v4().to_string(),
            event_type: "SIGN_IN".to_string(),
            username: input.username.clone(),
            user_pool_id: input.pool_id.clone(),
            client_id: Some(input.client_id.clone()),
            timestamp: Utc::now(),
            success: true,
            feedback_value: None,
        });

        Ok(AdminAuthOutcome::Tokens(tokens))
    }

    pub(super) async fn initiate_auth(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let client_id = require_str(&body, "ClientId")?;
        let auth_flow = require_str(&body, "AuthFlow")?;

        // Resolve pool_id and auth flows from client in a scoped lock
        let (pool_id, explicit_auth_flows) = {
            let accounts = self.state.read();
            let empty = CognitoState::new(&req.account_id, &req.region);
            let state = accounts.get(&req.account_id).unwrap_or(&empty);
            let client = state.user_pool_clients.get(client_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("User pool client {client_id} does not exist."),
                )
            })?;
            (
                client.user_pool_id.clone(),
                client.explicit_auth_flows.clone(),
            )
        };

        match auth_flow {
            "USER_PASSWORD_AUTH" => {
                self.initiate_user_password_auth(
                    &body,
                    client_id,
                    &pool_id,
                    &explicit_auth_flows,
                    req,
                )
                .await
            }
            "CUSTOM_AUTH" => {
                self.initiate_custom_auth(&body, client_id, &pool_id, &explicit_auth_flows, req)
                    .await
            }
            "REFRESH_TOKEN_AUTH" | "REFRESH_TOKEN" => {
                self.initiate_refresh_token_auth(&body, client_id, &explicit_auth_flows, req)
            }
            other => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                format!("Unsupported auth flow: {other}"),
            )),
        }
    }

    async fn initiate_user_password_auth(
        &self,
        body: &Value,
        client_id: &str,
        pool_id: &str,
        explicit_auth_flows: &[String],
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        if !explicit_auth_flows
            .iter()
            .any(|f| f == "ALLOW_USER_PASSWORD_AUTH")
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "USER_PASSWORD_AUTH flow is not enabled for this client.",
            ));
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
            })?;

        let password = auth_params
            .get("PASSWORD")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "PASSWORD is required in AuthParameters",
                )
            })?;

        // CompromisedCredentialsRiskConfiguration: when the pool has a
        // risk config with `EventAction = BLOCK` for sign-in events and
        // the password is in the compromised-password hash set, reject
        // with NotAuthorizedException. The hash set is populated via
        // `/_fakecloud/cognito/compromised-passwords` for deterministic
        // tests.
        self.evaluate_compromised_credentials(&req.account_id, pool_id, client_id, password)?;

        let (user_attrs, region, account_id) = {
            let accounts = self.state.read();
            let empty = CognitoState::new(&req.account_id, &req.region);
            let state = accounts.get(&req.account_id).unwrap_or(&empty);

            let user = state
                .users
                .get(pool_id)
                .and_then(|users| users.get(username))
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "NotAuthorizedException",
                        "Incorrect username or password.",
                    )
                })?;

            if !user.enabled {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotAuthorizedException",
                    "User is disabled.",
                ));
            }

            let user_attrs = triggers::collect_user_attributes(user);
            let region = state.region.clone();
            let account_id = state.account_id.clone();

            (user_attrs, region, account_id)
        };

        if let Some(ctx) = self.delivery_ctx.as_ref() {
            if let Some(function_arn) = triggers::get_trigger_arn(
                &self.state,
                pool_id,
                TriggerSource::PreAuthenticationAuthentication,
            ) {
                let event = triggers::build_trigger_event(
                    TriggerSource::PreAuthenticationAuthentication,
                    pool_id,
                    Some(client_id),
                    username,
                    &user_attrs,
                    &region,
                    &account_id,
                );
                if triggers::invoke_trigger(ctx, &function_arn, &event)
                    .await
                    .is_none()
                {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "NotAuthorizedException",
                        "PreAuthentication Lambda trigger rejected the request.",
                    ));
                }
            }
        }

        let pretoken_setup = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);

            let user = state
                .users
                .get(pool_id)
                .and_then(|users| users.get(username))
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "NotAuthorizedException",
                        "Incorrect username or password.",
                    )
                })?;

            let password_matches = match (&user.password, &user.temporary_password) {
                (Some(p), _) if p == password => true,
                (_, Some(tp)) if tp == password => true,
                _ => false,
            };
            if !password_matches {
                state.auth_events.push(AuthEvent {
                    event_id: Uuid::new_v4().to_string(),
                    event_type: "SIGN_IN_FAILURE".to_string(),
                    username: username.to_string(),
                    user_pool_id: pool_id.to_string(),
                    client_id: Some(client_id.to_string()),
                    timestamp: Utc::now(),
                    success: false,
                    feedback_value: None,
                });
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotAuthorizedException",
                    "Incorrect username or password.",
                ));
            }

            if user.user_status == user_status::FORCE_CHANGE_PASSWORD {
                let session = Uuid::new_v4().to_string();
                state.sessions.insert(
                    session.clone(),
                    SessionData {
                        user_pool_id: pool_id.to_string(),
                        username: username.to_string(),
                        client_id: client_id.to_string(),
                        challenge_name: "NEW_PASSWORD_REQUIRED".to_string(),
                        challenge_results: vec![],
                        challenge_metadata: None,
                    },
                );
                return Ok(AwsResponse::ok_json(json!({
                    "ChallengeName": "NEW_PASSWORD_REQUIRED",
                    "Session": session,
                    "ChallengeParameters": {
                        "USER_ID_FOR_SRP": username,
                        "requiredAttributes": "[]",
                        "userAttributes": "{}"
                    }
                })));
            }

            let sub = user.sub.clone();
            let pool_signing_owned = state.user_pools.get(pool_id).and_then(|pool| {
                pool.signing_key_pem
                    .as_ref()
                    .zip(pool.signing_kid.as_ref())
                    .map(|(p, k)| (p.clone(), k.clone()))
            });
            (sub, pool_signing_owned)
        };
        let (sub, pool_signing_owned) = pretoken_setup;

        let pretoken_overrides = if let Some(ctx) = self.delivery_ctx.as_ref() {
            if let Some(function_arn) = triggers::get_trigger_arn(
                &self.state,
                pool_id,
                TriggerSource::TokenGenerationAuthentication,
            ) {
                let event = triggers::build_trigger_event(
                    TriggerSource::TokenGenerationAuthentication,
                    pool_id,
                    Some(client_id),
                    username,
                    &user_attrs,
                    &region,
                    &account_id,
                );
                let started = std::time::Instant::now();
                let invoked_at = chrono::Utc::now();
                let raw_response = triggers::invoke_trigger(ctx, &function_arn, &event).await;
                let duration_ms = started.elapsed().as_millis() as u64;
                let overrides = raw_response
                    .as_ref()
                    .and_then(|resp| resp.get("response").cloned());

                record_pre_token_gen_invocation(
                    &self.state,
                    &req.account_id,
                    pool_id,
                    &region,
                    &account_id,
                    username,
                    &function_arn,
                    &event,
                    raw_response.as_ref(),
                    invoked_at,
                    duration_ms,
                );

                overrides
            } else {
                None
            }
        } else {
            None
        };

        let signing = pool_signing_owned
            .as_ref()
            .map(|(p, k)| (p.as_str(), k.as_str()));
        let tokens = super::generate_tokens_with_overrides(
            pool_id,
            client_id,
            &sub,
            username,
            &region,
            signing,
            None,
            None,
            pretoken_overrides.as_ref(),
        );

        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            state.refresh_tokens.insert(
                tokens.refresh_token.clone(),
                RefreshTokenData {
                    user_pool_id: pool_id.to_string(),
                    username: username.to_string(),
                    client_id: client_id.to_string(),
                    issued_at: Utc::now(),
                },
            );

            state.access_tokens.insert(
                tokens.access_token.clone(),
                AccessTokenData {
                    user_pool_id: pool_id.to_string(),
                    username: username.to_string(),
                    client_id: client_id.to_string(),
                    issued_at: Utc::now(),
                },
            );

            state.auth_events.push(AuthEvent {
                event_id: Uuid::new_v4().to_string(),
                event_type: "SIGN_IN".to_string(),
                username: username.to_string(),
                user_pool_id: pool_id.to_string(),
                client_id: Some(client_id.to_string()),
                timestamp: Utc::now(),
                success: true,
                feedback_value: None,
            });
        }

        if let Some(ctx) = self.delivery_ctx.as_ref() {
            if let Some(function_arn) = triggers::get_trigger_arn(
                &self.state,
                pool_id,
                TriggerSource::PostAuthenticationAuthentication,
            ) {
                let event = triggers::build_trigger_event(
                    TriggerSource::PostAuthenticationAuthentication,
                    pool_id,
                    Some(client_id),
                    username,
                    &user_attrs,
                    &region,
                    &account_id,
                );
                triggers::invoke_trigger_fire_and_forget(ctx, function_arn, event);
            }
        }

        Ok(AwsResponse::ok_json(json!({
            "AuthenticationResult": {
                "AccessToken": tokens.access_token,
                "IdToken": tokens.id_token,
                "RefreshToken": tokens.refresh_token,
                "TokenType": "Bearer",
                "ExpiresIn": 3600
            }
        })))
    }

    async fn initiate_custom_auth(
        &self,
        body: &Value,
        client_id: &str,
        pool_id: &str,
        explicit_auth_flows: &[String],
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        if !explicit_auth_flows.iter().any(|f| f == "ALLOW_CUSTOM_AUTH") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "CUSTOM_AUTH flow is not enabled for this client.",
            ));
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
            })?;

        let (user_attrs, region, account_id) = {
            let accounts = self.state.read();
            let empty = CognitoState::new(&req.account_id, &req.region);
            let state = accounts.get(&req.account_id).unwrap_or(&empty);
            let user = state
                .users
                .get(pool_id)
                .and_then(|users| users.get(username))
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "NotAuthorizedException",
                        "Incorrect username or password.",
                    )
                })?;

            if !user.enabled {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotAuthorizedException",
                    "User is disabled.",
                ));
            }

            let user_attrs = triggers::collect_user_attributes(user);
            let region = state.region.clone();
            let account_id = state.account_id.clone();
            (user_attrs, region, account_id)
        };

        let challenge_results: Vec<ChallengeResult> = vec![];

        // DefineAuthChallenge Lambda is mandatory for CUSTOM_AUTH; without it
        // there is no policy to drive the challenge graph forward.
        let ctx = self.delivery_ctx.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidLambdaResponseException",
                "No Lambda trigger configured for DefineAuthChallenge.",
            )
        })?;

        let define_arn = triggers::get_trigger_arn(
            &self.state,
            pool_id,
            TriggerSource::DefineAuthChallengeAuthentication,
        )
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidLambdaResponseException",
                "No Lambda trigger configured for DefineAuthChallenge.",
            )
        })?;

        let define_event = triggers::build_define_auth_challenge_event(
            pool_id,
            Some(client_id),
            username,
            &user_attrs,
            &challenge_results,
            &region,
            &account_id,
        );

        let define_response = triggers::invoke_trigger(ctx, &define_arn, &define_event)
            .await
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidLambdaResponseException",
                    "DefineAuthChallenge Lambda did not return a response.",
                )
            })?;

        let issue_tokens = define_response["response"]["issueTokens"]
            .as_bool()
            .unwrap_or(false);
        let fail_auth = define_response["response"]["failAuthentication"]
            .as_bool()
            .unwrap_or(false);

        if fail_auth {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            state.auth_events.push(AuthEvent {
                event_id: Uuid::new_v4().to_string(),
                event_type: "SIGN_IN_FAILURE".to_string(),
                username: username.to_string(),
                user_pool_id: pool_id.to_string(),
                client_id: Some(client_id.to_string()),
                timestamp: Utc::now(),
                success: false,
                feedback_value: None,
            });
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "DefineAuthChallenge Lambda rejected authentication.",
            ));
        }

        if issue_tokens {
            return self.custom_auth_issue_tokens(pool_id, client_id, username, &region, req);
        }

        let challenge_name = define_response["response"]["challengeName"]
            .as_str()
            .unwrap_or("CUSTOM_CHALLENGE")
            .to_string();

        let create_arn = triggers::get_trigger_arn(
            &self.state,
            pool_id,
            TriggerSource::CreateAuthChallengeAuthentication,
        );

        let mut public_challenge_params = serde_json::Map::new();
        let mut challenge_metadata: Option<String> = None;

        if let Some(create_arn) = create_arn {
            let create_ctx = triggers::AuthChallengeContext {
                pool_id,
                client_id: Some(client_id),
                username,
                user_attributes: &user_attrs,
                region: &region,
                account_id: &account_id,
            };
            let create_event = triggers::build_create_auth_challenge_event(
                &create_ctx,
                &challenge_name,
                &challenge_results,
            );
            if let Some(create_response) =
                triggers::invoke_trigger(ctx, &create_arn, &create_event).await
            {
                if let Some(params) =
                    create_response["response"]["publicChallengeParameters"].as_object()
                {
                    public_challenge_params = params.clone();
                }
                challenge_metadata = create_response["response"]["challengeMetadata"]
                    .as_str()
                    .map(|s| s.to_string());
            }
        }

        let session = Uuid::new_v4().to_string();
        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            state.sessions.insert(
                session.clone(),
                SessionData {
                    user_pool_id: pool_id.to_string(),
                    username: username.to_string(),
                    client_id: client_id.to_string(),
                    challenge_name: challenge_name.clone(),
                    challenge_results,
                    challenge_metadata,
                },
            );
        }

        let mut response = json!({
            "ChallengeName": challenge_name,
            "Session": session,
            "ChallengeParameters": public_challenge_params,
        });
        response["ChallengeParameters"]["USERNAME"] = json!(username);

        Ok(AwsResponse::ok_json(response))
    }

    /// Mint and persist tokens for a CUSTOM_AUTH flow that DefineAuthChallenge
    /// resolved with `issueTokens: true` on the very first call (no challenge
    /// round-trip needed).
    fn custom_auth_issue_tokens(
        &self,
        pool_id: &str,
        client_id: &str,
        username: &str,
        region: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let user = state
            .users
            .get(pool_id)
            .and_then(|users| users.get(username))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotAuthorizedException",
                    "Incorrect username or password.",
                )
            })?;

        let sub = user.sub.clone();
        let pool_signing_owned = state.user_pools.get(pool_id).and_then(|pool| {
            pool.signing_key_pem
                .as_ref()
                .zip(pool.signing_kid.as_ref())
                .map(|(p, k)| (p.clone(), k.clone()))
        });
        let signing = pool_signing_owned
            .as_ref()
            .map(|(p, k)| (p.as_str(), k.as_str()));
        let tokens = generate_tokens(pool_id, client_id, &sub, username, region, signing);

        state.refresh_tokens.insert(
            tokens.refresh_token.clone(),
            RefreshTokenData {
                user_pool_id: pool_id.to_string(),
                username: username.to_string(),
                client_id: client_id.to_string(),
                issued_at: Utc::now(),
            },
        );
        state.access_tokens.insert(
            tokens.access_token.clone(),
            AccessTokenData {
                user_pool_id: pool_id.to_string(),
                username: username.to_string(),
                client_id: client_id.to_string(),
                issued_at: Utc::now(),
            },
        );
        state.auth_events.push(AuthEvent {
            event_id: Uuid::new_v4().to_string(),
            event_type: "SIGN_IN".to_string(),
            username: username.to_string(),
            user_pool_id: pool_id.to_string(),
            client_id: Some(client_id.to_string()),
            timestamp: Utc::now(),
            success: true,
            feedback_value: None,
        });

        Ok(AwsResponse::ok_json(json!({
            "AuthenticationResult": {
                "AccessToken": tokens.access_token,
                "IdToken": tokens.id_token,
                "RefreshToken": tokens.refresh_token,
                "TokenType": "Bearer",
                "ExpiresIn": 3600
            }
        })))
    }

    fn initiate_refresh_token_auth(
        &self,
        body: &Value,
        client_id: &str,
        explicit_auth_flows: &[String],
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        if !explicit_auth_flows
            .iter()
            .any(|f| f == "ALLOW_REFRESH_TOKEN_AUTH")
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "REFRESH_TOKEN_AUTH flow is not enabled for this client.",
            ));
        }

        let auth_params = body["AuthParameters"].as_object().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "AuthParameters is required",
            )
        })?;

        let refresh_token = auth_params
            .get("REFRESH_TOKEN")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "REFRESH_TOKEN is required in AuthParameters",
                )
            })?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let token_data = state.refresh_tokens.get(refresh_token).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid refresh token.",
            )
        })?;

        if token_data.client_id != client_id {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid refresh token.",
            ));
        }

        let token_pool_id = token_data.user_pool_id.clone();
        let token_username = token_data.username.clone();

        let user = state
            .users
            .get(&token_pool_id)
            .and_then(|users| users.get(&token_username))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotAuthorizedException",
                    "User does not exist.",
                )
            })?;

        let region = state.region.clone();
        let sub = user.sub.clone();
        let pool_signing_owned = state.user_pools.get(&token_pool_id).and_then(|pool| {
            pool.signing_key_pem
                .as_ref()
                .zip(pool.signing_kid.as_ref())
                .map(|(p, k)| (p.clone(), k.clone()))
        });
        let signing = pool_signing_owned
            .as_ref()
            .map(|(p, k)| (p.as_str(), k.as_str()));
        let tokens = generate_tokens(
            &token_pool_id,
            client_id,
            &sub,
            &token_username,
            &region,
            signing,
        );

        state.access_tokens.insert(
            tokens.access_token.clone(),
            AccessTokenData {
                user_pool_id: token_pool_id,
                username: token_username,
                client_id: client_id.to_string(),
                issued_at: Utc::now(),
            },
        );

        Ok(AwsResponse::ok_json(json!({
            "AuthenticationResult": {
                "AccessToken": tokens.access_token,
                "IdToken": tokens.id_token,
                "TokenType": "Bearer",
                "ExpiresIn": 3600
            }
        })))
    }

    pub(super) async fn respond_to_auth_challenge(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let client_id = require_str(&body, "ClientId")?;
        let challenge_name = require_str(&body, "ChallengeName")?;
        let session = require_str(&body, "Session")?;

        self.handle_auth_challenge_response(client_id, challenge_name, session, &body, req)
            .await
    }

    pub(super) async fn admin_respond_to_auth_challenge(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let client_id = require_str(&body, "ClientId")?;
        let challenge_name = require_str(&body, "ChallengeName")?;
        let session = require_str(&body, "Session")?;

        // Validate session's pool ID matches the provided one
        {
            let accounts = self.state.read();
            let empty = CognitoState::new(&req.account_id, &req.region);
            let state = accounts.get(&req.account_id).unwrap_or(&empty);
            if let Some(session_data) = state.sessions.get(session) {
                if session_data.user_pool_id != pool_id {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "NotAuthorizedException",
                        "Invalid session.",
                    ));
                }
            }
            // If session doesn't exist, handle_auth_challenge_response will return the error
        }

        self.handle_auth_challenge_response(client_id, challenge_name, session, &body, req)
            .await
    }

    pub(super) async fn handle_auth_challenge_response(
        &self,
        client_id: &str,
        challenge_name: &str,
        session: &str,
        body: &Value,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        match challenge_name {
            "NEW_PASSWORD_REQUIRED" => {
                self.respond_new_password_required(client_id, session, body, req)
            }
            "CUSTOM_CHALLENGE" => {
                self.respond_custom_challenge(client_id, session, body, req)
                    .await
            }
            _ => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                format!("Unsupported challenge: {challenge_name}"),
            )),
        }
    }

    fn respond_new_password_required(
        &self,
        client_id: &str,
        session: &str,
        body: &Value,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let challenge_responses = body["ChallengeResponses"].as_object().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "ChallengeResponses is required",
            )
        })?;

        let new_password = challenge_responses
            .get("NEW_PASSWORD")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "NEW_PASSWORD is required in ChallengeResponses",
                )
            })?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let session_data = state.sessions.remove(session).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid session.",
            )
        })?;

        if session_data.client_id != client_id
            || session_data.challenge_name != "NEW_PASSWORD_REQUIRED"
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid session.",
            ));
        }

        let password_policy = state
            .user_pools
            .get(&session_data.user_pool_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "User pool does not exist.",
                )
            })?
            .policies
            .password_policy
            .clone();
        validate_password(new_password, &password_policy)?;

        let region = state.region.clone();

        let user = state
            .users
            .get_mut(&session_data.user_pool_id)
            .and_then(|users| users.get_mut(&session_data.username))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "UserNotFoundException",
                    "User does not exist.",
                )
            })?;

        user.password = Some(new_password.to_string());
        user.temporary_password = None;
        user.user_status = user_status::CONFIRMED.to_string();
        user.user_last_modified_date = Utc::now();

        let sub = user.sub.clone();
        let username = user.username.clone();
        let pool_id = session_data.user_pool_id.clone();

        let pool_signing_owned = state.user_pools.get(&pool_id).and_then(|pool| {
            pool.signing_key_pem
                .as_ref()
                .zip(pool.signing_kid.as_ref())
                .map(|(p, k)| (p.clone(), k.clone()))
        });
        let signing = pool_signing_owned
            .as_ref()
            .map(|(p, k)| (p.as_str(), k.as_str()));
        let tokens = generate_tokens(&pool_id, client_id, &sub, &username, &region, signing);

        state.refresh_tokens.insert(
            tokens.refresh_token.clone(),
            RefreshTokenData {
                user_pool_id: pool_id.clone(),
                username: username.clone(),
                client_id: client_id.to_string(),
                issued_at: Utc::now(),
            },
        );

        state.access_tokens.insert(
            tokens.access_token.clone(),
            AccessTokenData {
                user_pool_id: pool_id,
                username,
                client_id: client_id.to_string(),
                issued_at: Utc::now(),
            },
        );

        Ok(AwsResponse::ok_json(json!({
            "AuthenticationResult": {
                "AccessToken": tokens.access_token,
                "IdToken": tokens.id_token,
                "RefreshToken": tokens.refresh_token,
                "TokenType": "Bearer",
                "ExpiresIn": 3600
            }
        })))
    }

    async fn respond_custom_challenge(
        &self,
        client_id: &str,
        session: &str,
        body: &Value,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let challenge_responses = body["ChallengeResponses"].as_object().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "ChallengeResponses is required",
            )
        })?;

        let answer = challenge_responses
            .get("ANSWER")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "ANSWER is required in ChallengeResponses",
                )
            })?;

        let (pool_id, username, session_client_id, mut challenge_results, challenge_metadata) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            let session_data = state.sessions.remove(session).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotAuthorizedException",
                    "Invalid session.",
                )
            })?;

            if session_data.client_id != client_id
                || session_data.challenge_name != "CUSTOM_CHALLENGE"
            {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotAuthorizedException",
                    "Invalid session.",
                ));
            }

            (
                session_data.user_pool_id,
                session_data.username,
                session_data.client_id,
                session_data.challenge_results,
                session_data.challenge_metadata,
            )
        };

        let (user_attrs, region, account_id) = {
            let accounts = self.state.read();
            let empty = CognitoState::new(&req.account_id, &req.region);
            let state = accounts.get(&req.account_id).unwrap_or(&empty);
            let user = state
                .users
                .get(&pool_id)
                .and_then(|users| users.get(&username))
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "NotAuthorizedException",
                        "User does not exist.",
                    )
                })?;
            let user_attrs = triggers::collect_user_attributes(user);
            let region = state.region.clone();
            let account_id = state.account_id.clone();
            (user_attrs, region, account_id)
        };

        let ctx = self.delivery_ctx.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidLambdaResponseException",
                "No Lambda trigger configured for VerifyAuthChallengeResponse.",
            )
        })?;

        let verify_arn = triggers::get_trigger_arn(
            &self.state,
            &pool_id,
            TriggerSource::VerifyAuthChallengeResponseAuthentication,
        )
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidLambdaResponseException",
                "No Lambda trigger configured for VerifyAuthChallengeResponse.",
            )
        })?;

        let verify_ctx = triggers::AuthChallengeContext {
            pool_id: &pool_id,
            client_id: Some(&session_client_id),
            username: &username,
            user_attributes: &user_attrs,
            region: &region,
            account_id: &account_id,
        };
        let verify_event = triggers::build_verify_auth_challenge_event(
            &verify_ctx,
            answer,
            challenge_metadata.as_deref(),
        );

        let verify_response = triggers::invoke_trigger(ctx, &verify_arn, &verify_event)
            .await
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidLambdaResponseException",
                    "VerifyAuthChallengeResponse Lambda did not return a response.",
                )
            })?;

        let answer_correct = verify_response["response"]["answerCorrect"]
            .as_bool()
            .unwrap_or(false);

        challenge_results.push(ChallengeResult {
            challenge_name: "CUSTOM_CHALLENGE".to_string(),
            challenge_result: answer_correct,
            challenge_metadata: challenge_metadata.clone(),
        });

        let define_arn = triggers::get_trigger_arn(
            &self.state,
            &pool_id,
            TriggerSource::DefineAuthChallengeAuthentication,
        )
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidLambdaResponseException",
                "No Lambda trigger configured for DefineAuthChallenge.",
            )
        })?;

        let define_event = triggers::build_define_auth_challenge_event(
            &pool_id,
            Some(&session_client_id),
            &username,
            &user_attrs,
            &challenge_results,
            &region,
            &account_id,
        );

        let define_response = triggers::invoke_trigger(ctx, &define_arn, &define_event)
            .await
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidLambdaResponseException",
                    "DefineAuthChallenge Lambda did not return a response.",
                )
            })?;

        let issue_tokens = define_response["response"]["issueTokens"]
            .as_bool()
            .unwrap_or(false);
        let fail_auth = define_response["response"]["failAuthentication"]
            .as_bool()
            .unwrap_or(false);

        if fail_auth {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            state.auth_events.push(AuthEvent {
                event_id: Uuid::new_v4().to_string(),
                event_type: "SIGN_IN_FAILURE".to_string(),
                username,
                user_pool_id: pool_id,
                client_id: Some(session_client_id),
                timestamp: Utc::now(),
                success: false,
                feedback_value: None,
            });
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "DefineAuthChallenge Lambda rejected authentication.",
            ));
        }

        if issue_tokens {
            return self.custom_challenge_issue_tokens(
                &pool_id,
                &session_client_id,
                &username,
                &region,
                req,
            );
        }

        let next_challenge_name = define_response["response"]["challengeName"]
            .as_str()
            .unwrap_or("CUSTOM_CHALLENGE")
            .to_string();

        let create_arn = triggers::get_trigger_arn(
            &self.state,
            &pool_id,
            TriggerSource::CreateAuthChallengeAuthentication,
        );

        let mut public_challenge_params = serde_json::Map::new();
        let mut new_challenge_metadata: Option<String> = None;

        if let Some(create_arn) = create_arn {
            let create_ctx = triggers::AuthChallengeContext {
                pool_id: &pool_id,
                client_id: Some(&session_client_id),
                username: &username,
                user_attributes: &user_attrs,
                region: &region,
                account_id: &account_id,
            };
            let create_event = triggers::build_create_auth_challenge_event(
                &create_ctx,
                &next_challenge_name,
                &challenge_results,
            );
            if let Some(create_response) =
                triggers::invoke_trigger(ctx, &create_arn, &create_event).await
            {
                if let Some(params) =
                    create_response["response"]["publicChallengeParameters"].as_object()
                {
                    public_challenge_params = params.clone();
                }
                new_challenge_metadata = create_response["response"]["challengeMetadata"]
                    .as_str()
                    .map(|s| s.to_string());
            }
        }

        let new_session = Uuid::new_v4().to_string();
        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            state.sessions.insert(
                new_session.clone(),
                SessionData {
                    user_pool_id: pool_id,
                    username: username.clone(),
                    client_id: session_client_id,
                    challenge_name: next_challenge_name.clone(),
                    challenge_results,
                    challenge_metadata: new_challenge_metadata,
                },
            );
        }

        let mut response = json!({
            "ChallengeName": next_challenge_name,
            "Session": new_session,
            "ChallengeParameters": public_challenge_params,
        });
        response["ChallengeParameters"]["USERNAME"] = json!(username);

        Ok(AwsResponse::ok_json(response))
    }

    /// Mint and persist tokens for a CUSTOM_CHALLENGE round whose final
    /// DefineAuthChallenge response set `issueTokens: true`. Mirrors the
    /// success-path bookkeeping that USER_PASSWORD_AUTH does.
    fn custom_challenge_issue_tokens(
        &self,
        pool_id: &str,
        client_id: &str,
        username: &str,
        region: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let user = state
            .users
            .get(pool_id)
            .and_then(|users| users.get(username))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotAuthorizedException",
                    "User does not exist.",
                )
            })?;

        let sub = user.sub.clone();
        let pool_signing_owned = state.user_pools.get(pool_id).and_then(|pool| {
            pool.signing_key_pem
                .as_ref()
                .zip(pool.signing_kid.as_ref())
                .map(|(p, k)| (p.clone(), k.clone()))
        });
        let signing = pool_signing_owned
            .as_ref()
            .map(|(p, k)| (p.as_str(), k.as_str()));
        let tokens = generate_tokens(pool_id, client_id, &sub, username, region, signing);

        state.refresh_tokens.insert(
            tokens.refresh_token.clone(),
            RefreshTokenData {
                user_pool_id: pool_id.to_string(),
                username: username.to_string(),
                client_id: client_id.to_string(),
                issued_at: Utc::now(),
            },
        );
        state.access_tokens.insert(
            tokens.access_token.clone(),
            AccessTokenData {
                user_pool_id: pool_id.to_string(),
                username: username.to_string(),
                client_id: client_id.to_string(),
                issued_at: Utc::now(),
            },
        );
        state.auth_events.push(AuthEvent {
            event_id: Uuid::new_v4().to_string(),
            event_type: "SIGN_IN".to_string(),
            username: username.to_string(),
            user_pool_id: pool_id.to_string(),
            client_id: Some(client_id.to_string()),
            timestamp: Utc::now(),
            success: true,
            feedback_value: None,
        });

        Ok(AwsResponse::ok_json(json!({
            "AuthenticationResult": {
                "AccessToken": tokens.access_token,
                "IdToken": tokens.id_token,
                "RefreshToken": tokens.refresh_token,
                "TokenType": "Bearer",
                "ExpiresIn": 3600
            }
        })))
    }

    pub(super) async fn sign_up(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let client_id = require_str(&body, "ClientId")?;
        let username = require_str(&body, "Username")?;
        let password = body["Password"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "Password is required",
                )
            })?;

        let (pool_id, sub, user, region, account_id) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);

            // Find pool from client
            let client = state.user_pool_clients.get(client_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("User pool client {client_id} does not exist."),
                )
            })?;
            let pool_id = client.user_pool_id.clone();

            // Validate password against pool policy
            let pool = state.user_pools.get(&pool_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "User pool does not exist.",
                )
            })?;
            validate_password(password, &pool.policies.password_policy)?;

            // Check username unique
            let pool_users = state.users.entry(pool_id.clone()).or_default();
            if pool_users.contains_key(username) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "UsernameExistsException",
                    "User account already exists.",
                ));
            }

            let now = Utc::now();
            let sub = Uuid::new_v4().to_string();

            let mut attributes = parse_user_attributes(&body["UserAttributes"]);

            // Ensure sub attribute
            if !attributes.iter().any(|a| a.name == "sub") {
                attributes.push(UserAttribute {
                    name: "sub".to_string(),
                    value: sub.clone(),
                });
            }

            let user = crate::state::User {
                username: username.to_string(),
                sub: sub.clone(),
                attributes,
                enabled: true,
                user_status: user_status::UNCONFIRMED.to_string(),
                user_create_date: now,
                user_last_modified_date: now,
                password: Some(password.to_string()),
                temporary_password: None,
                confirmation_code: None,
                attribute_verification_codes: BTreeMap::new(),
                mfa_preferences: None,
                totp_secret: None,
                totp_verified: false,
                devices: BTreeMap::new(),
                linked_providers: Vec::new(),
            };

            pool_users.insert(username.to_string(), user.clone());

            let region = state.region.clone();
            let account_id = state.account_id.clone();

            state.auth_events.push(AuthEvent {
                event_id: Uuid::new_v4().to_string(),
                event_type: "SIGN_UP".to_string(),
                username: username.to_string(),
                user_pool_id: pool_id.clone(),
                client_id: Some(client_id.to_string()),
                timestamp: Utc::now(),
                success: true,
                feedback_value: None,
            });

            (pool_id, sub, user, region, account_id)
        };

        // PreSignUp_SignUp trigger (synchronous — response can auto-confirm)
        let mut auto_confirm = false;
        if let Some(ref ctx) = self.delivery_ctx {
            if let Some(function_arn) =
                triggers::get_trigger_arn(&self.state, &pool_id, TriggerSource::PreSignUpSignUp)
            {
                let event = triggers::build_trigger_event(
                    TriggerSource::PreSignUpSignUp,
                    &pool_id,
                    Some(client_id),
                    username,
                    &triggers::collect_user_attributes(&user),
                    &region,
                    &account_id,
                );
                if let Some(response) = triggers::invoke_trigger(ctx, &function_arn, &event).await {
                    if response["response"]["autoConfirmUser"].as_bool() == Some(true) {
                        auto_confirm = true;
                    }
                }
            }
        }

        if auto_confirm {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            if let Some(u) = state
                .users
                .get_mut(&pool_id)
                .and_then(|users| users.get_mut(username))
            {
                u.user_status = user_status::CONFIRMED.to_string();
                u.user_last_modified_date = Utc::now();
            }
        } else {
            // Generate a verification code and dispatch through SES (or
            // CustomEmailSender if the pool has it wired).
            let code = generate_confirmation_code();
            let user_attrs = {
                let mut accounts = self.state.write();
                let state = accounts.get_or_create(&req.account_id);
                let attrs = if let Some(u) = state
                    .users
                    .get_mut(&pool_id)
                    .and_then(|users| users.get_mut(username))
                {
                    u.confirmation_code = Some(code.clone());
                    u.attributes.clone()
                } else {
                    Vec::new()
                };
                attrs
            };
            if let Some(email) = user_attrs
                .iter()
                .find(|a| a.name == "email")
                .map(|a| a.value.clone())
            {
                self.dispatch_verification_email(
                    &pool_id,
                    Some(client_id),
                    username,
                    &user_attrs,
                    &email,
                    &code,
                    TriggerSource::CustomMessageSignUp,
                    TriggerSource::CustomEmailSenderSignUp,
                    &region,
                    &account_id,
                );
            }
        }

        Ok(AwsResponse::ok_json(json!({
            "UserConfirmed": auto_confirm,
            "UserSub": sub
        })))
    }

    pub(super) async fn confirm_sign_up(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let client_id = require_str(&body, "ClientId")?;
        let username = require_str(&body, "Username")?;
        let code = body["ConfirmationCode"].as_str().unwrap_or("");

        if code.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "ConfirmationCode is required",
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let client = state.user_pool_clients.get(client_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool client {client_id} does not exist."),
            )
        })?;
        let pool_id = client.user_pool_id.clone();

        let user = state
            .users
            .get_mut(&pool_id)
            .and_then(|users| users.get_mut(username))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "UserNotFoundException",
                    "User does not exist.",
                )
            })?;

        user.user_status = user_status::CONFIRMED.to_string();
        user.user_last_modified_date = Utc::now();

        let user_attrs = triggers::collect_user_attributes(user);
        let region = state.region.clone();
        let account_id = state.account_id.clone();
        drop(accounts);

        // PostConfirmation_ConfirmSignUp trigger (fire-and-forget)
        if let Some(ref ctx) = self.delivery_ctx {
            if let Some(function_arn) = triggers::get_trigger_arn(
                &self.state,
                &pool_id,
                TriggerSource::PostConfirmationConfirmSignUp,
            ) {
                let event = triggers::build_trigger_event(
                    TriggerSource::PostConfirmationConfirmSignUp,
                    &pool_id,
                    Some(client_id),
                    username,
                    &user_attrs,
                    &region,
                    &account_id,
                );
                triggers::invoke_trigger_fire_and_forget(ctx, function_arn, event);
            }
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) async fn admin_confirm_sign_up(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let username = require_str(&body, "Username")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Validate pool exists
        ensure_user_pool_exists(state, pool_id)?;

        let user = state
            .users
            .get_mut(pool_id)
            .and_then(|users| users.get_mut(username))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "UserNotFoundException",
                    "User does not exist.",
                )
            })?;

        user.user_status = user_status::CONFIRMED.to_string();
        user.user_last_modified_date = Utc::now();

        let user_attrs = triggers::collect_user_attributes(user);
        let region = state.region.clone();
        let account_id = state.account_id.clone();
        drop(accounts);

        // PostConfirmation_AdminConfirmSignUp trigger (fire-and-forget)
        if let Some(ref ctx) = self.delivery_ctx {
            if let Some(function_arn) = triggers::get_trigger_arn(
                &self.state,
                pool_id,
                TriggerSource::PostConfirmationAdminConfirmSignUp,
            ) {
                let event = triggers::build_trigger_event(
                    TriggerSource::PostConfirmationAdminConfirmSignUp,
                    pool_id,
                    None,
                    username,
                    &user_attrs,
                    &region,
                    &account_id,
                );
                triggers::invoke_trigger_fire_and_forget(ctx, function_arn, event);
            }
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn change_password(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let access_token = require_str(&body, "AccessToken")?;
        let previous_password = require_str(&body, "PreviousPassword")?;
        let proposed_password = require_str(&body, "ProposedPassword")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Look up user from access token
        let token_data = state.access_tokens.get(access_token).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid access token.",
            )
        })?;
        let pool_id = token_data.user_pool_id.clone();
        let username = token_data.username.clone();

        // Validate password against pool policy
        let password_policy = state
            .user_pools
            .get(&pool_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "User pool does not exist.",
                )
            })?
            .policies
            .password_policy
            .clone();

        let user = state
            .users
            .get_mut(&pool_id)
            .and_then(|users| users.get_mut(&username))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotAuthorizedException",
                    "Invalid access token.",
                )
            })?;

        // Verify previous password
        let password_matches = match (&user.password, &user.temporary_password) {
            (Some(p), _) if p == previous_password => true,
            (_, Some(tp)) if tp == previous_password => true,
            _ => false,
        };
        if !password_matches {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Incorrect username or password.",
            ));
        }

        validate_password(proposed_password, &password_policy)?;

        user.password = Some(proposed_password.to_string());
        user.temporary_password = None;
        user.user_last_modified_date = Utc::now();

        state.auth_events.push(AuthEvent {
            event_id: Uuid::new_v4().to_string(),
            event_type: "PASSWORD_CHANGE".to_string(),
            username,
            user_pool_id: pool_id,
            client_id: None,
            timestamp: Utc::now(),
            success: true,
            feedback_value: None,
        });

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) async fn forgot_password(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let client_id = require_str(&body, "ClientId")?;
        let username = require_str(&body, "Username")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Find pool from client
        let client = state.user_pool_clients.get(client_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool client {client_id} does not exist."),
            )
        })?;
        let pool_id = client.user_pool_id.clone();

        let user = state
            .users
            .get_mut(&pool_id)
            .and_then(|users| users.get_mut(username))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "UserNotFoundException",
                    "User does not exist.",
                )
            })?;

        let code = generate_confirmation_code();
        user.confirmation_code = Some(code.clone());

        // Find email from user attributes for CodeDeliveryDetails
        let email = user
            .attributes
            .iter()
            .find(|a| a.name == "email")
            .map(|a| a.value.clone());

        let user_attrs = triggers::collect_user_attributes(user);

        let destination = email
            .clone()
            .map(|e| {
                // Mask email: show first char + *** + @domain
                if let Some(at_pos) = e.find('@') {
                    let first = e.chars().next().unwrap_or('*');
                    let domain = &e[at_pos..];
                    format!("{first}***{domain}")
                } else {
                    "***".to_string()
                }
            })
            .unwrap_or_else(|| "***".to_string());

        let region = state.region.clone();
        let account_id = state.account_id.clone();

        state.auth_events.push(AuthEvent {
            event_id: Uuid::new_v4().to_string(),
            event_type: "FORGOT_PASSWORD".to_string(),
            username: username.to_string(),
            user_pool_id: pool_id.clone(),
            client_id: Some(client_id.to_string()),
            timestamp: Utc::now(),
            success: true,
            feedback_value: None,
        });

        drop(accounts);

        if let Some(addr) = email {
            self.dispatch_verification_email(
                &pool_id,
                Some(client_id),
                username,
                &user_attrs,
                &addr,
                &code,
                TriggerSource::CustomMessageForgotPassword,
                TriggerSource::CustomEmailSenderForgotPassword,
                &region,
                &account_id,
            );
        }

        // CustomMessage_ForgotPassword trigger (fire-and-forget)
        if let Some(ref ctx) = self.delivery_ctx {
            if let Some(function_arn) = triggers::get_trigger_arn(
                &self.state,
                &pool_id,
                TriggerSource::CustomMessageForgotPassword,
            ) {
                let event = triggers::build_trigger_event(
                    TriggerSource::CustomMessageForgotPassword,
                    &pool_id,
                    Some(client_id),
                    username,
                    &user_attrs,
                    &region,
                    &account_id,
                );
                triggers::invoke_trigger_fire_and_forget(ctx, function_arn, event);
            }
        }

        Ok(AwsResponse::ok_json(json!({
            "CodeDeliveryDetails": {
                "Destination": destination,
                "DeliveryMedium": "EMAIL",
                "AttributeName": "email"
            }
        })))
    }

    pub(super) fn confirm_forgot_password(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let client_id = require_str(&body, "ClientId")?;
        let username = require_str(&body, "Username")?;
        let confirmation_code = require_str(&body, "ConfirmationCode")?;
        let password = require_str(&body, "Password")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Find pool from client
        let client = state.user_pool_clients.get(client_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool client {client_id} does not exist."),
            )
        })?;
        let pool_id = client.user_pool_id.clone();

        // Validate password against pool policy
        let password_policy = state
            .user_pools
            .get(&pool_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "User pool does not exist.",
                )
            })?
            .policies
            .password_policy
            .clone();

        let user = state
            .users
            .get_mut(&pool_id)
            .and_then(|users| users.get_mut(username))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "UserNotFoundException",
                    "User does not exist.",
                )
            })?;

        // Validate confirmation code
        match &user.confirmation_code {
            Some(code) if code == confirmation_code => {}
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "CodeMismatchException",
                    "Invalid verification code provided, please try again.",
                ));
            }
        }

        validate_password(password, &password_policy)?;

        user.password = Some(password.to_string());
        user.temporary_password = None;
        user.confirmation_code = None;
        user.user_status = user_status::CONFIRMED.to_string();
        user.user_last_modified_date = Utc::now();

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn admin_reset_user_password(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let username = require_str(&body, "Username")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Validate pool exists
        ensure_user_pool_exists(state, pool_id)?;

        let user = state
            .users
            .get_mut(pool_id)
            .and_then(|users| users.get_mut(username))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "UserNotFoundException",
                    "User does not exist.",
                )
            })?;

        user.user_status = user_status::RESET_REQUIRED.to_string();
        user.confirmation_code = Some(generate_confirmation_code());
        user.user_last_modified_date = Utc::now();

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn global_sign_out(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let access_token = require_str(&body, "AccessToken")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Look up user from access token
        let token_data = state.access_tokens.get(access_token).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid access token.",
            )
        })?;
        let pool_id = token_data.user_pool_id.clone();
        let username = token_data.username.clone();

        // Invalidate all refresh tokens for this user
        state
            .refresh_tokens
            .retain(|_, v| !(v.user_pool_id == pool_id && v.username == username));

        // Invalidate all access tokens for this user
        state
            .access_tokens
            .retain(|_, v| !(v.user_pool_id == pool_id && v.username == username));

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn admin_user_global_sign_out(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let username = require_str(&body, "Username")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Validate pool exists
        ensure_user_pool_exists(state, pool_id)?;

        // Validate user exists
        if !state
            .users
            .get(pool_id)
            .is_some_and(|users| users.contains_key(username))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UserNotFoundException",
                "User does not exist.",
            ));
        }

        // Invalidate all refresh tokens for this user
        state
            .refresh_tokens
            .retain(|_, v| !(v.user_pool_id == pool_id && v.username == username));

        // Invalidate all access tokens for this user
        state
            .access_tokens
            .retain(|_, v| !(v.user_pool_id == pool_id && v.username == username));

        Ok(AwsResponse::ok_json(json!({})))
    }
}

/// Append one PreTokenGeneration trigger invocation to the per-account
/// introspection log. Pulls out the override block already so callers
/// at `/_fakecloud/cognito/pretokengen/invocations` get parsed
/// `claims_added` / `claims_overridden` / `group_overrides` instead of
/// having to walk the raw Lambda response themselves.
#[allow(clippy::too_many_arguments)]
fn record_pre_token_gen_invocation(
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
    let user_pool_arn = Arn::new(
        "cognito-idp",
        region,
        account_id,
        &format!("userpool/{pool_id}"),
    )
    .to_string();

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
