//! `CognitoService` `admin` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl CognitoService {
    pub(super) fn admin_auth_lookup(
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

    pub(super) fn admin_auth_verify(
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

    pub(crate) async fn admin_respond_to_auth_challenge(
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

    pub(crate) async fn admin_confirm_sign_up(
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

    pub(crate) fn admin_reset_user_password(
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

    pub(crate) fn admin_user_global_sign_out(
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
