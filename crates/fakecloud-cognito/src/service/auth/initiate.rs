//! `CognitoService` `initiate` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl CognitoService {
    pub(crate) async fn admin_initiate_auth(
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

    pub(crate) async fn initiate_auth(
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

    pub(super) async fn initiate_user_password_auth(
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
        let tokens = crate::service::generate_tokens_with_overrides(
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

    pub(super) async fn initiate_custom_auth(
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

    pub(super) fn initiate_refresh_token_auth(
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
}
