//! `CognitoService` `challenges` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl CognitoService {
    /// Mint and persist tokens for a CUSTOM_AUTH flow that DefineAuthChallenge
    /// resolved with `issueTokens: true` on the very first call (no challenge
    /// round-trip needed).
    pub(super) fn custom_auth_issue_tokens(
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

    pub(crate) async fn respond_to_auth_challenge(
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
            "PASSWORD_VERIFIER" => self.respond_password_verifier(client_id, session, body, req),
            "SELECT_CHALLENGE" => self.respond_select_challenge(client_id, session, body, req),
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

    /// If the user still owes a forced password reset, return a
    /// `NEW_PASSWORD_REQUIRED` challenge instead of minting tokens. The SRP /
    /// SELECT_CHALLENGE success path only proves the user's *current* (possibly
    /// temporary) password; an admin-created user with a temporary password
    /// must still set a permanent one, exactly as the `USER_PASSWORD_AUTH`
    /// (initiate.rs) and `AdminInitiateAuth` (admin.rs) paths require. Returns
    /// `Some(challenge)` when a reset is owed, `None` to proceed to tokens.
    fn maybe_force_new_password(
        &self,
        pool_id: &str,
        client_id: &str,
        username: &str,
        req: &AwsRequest,
    ) -> Option<AwsResponse> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let needs_reset = state
            .users
            .get(pool_id)
            .and_then(|users| users.get(username))
            .map(|u| u.user_status == user_status::FORCE_CHANGE_PASSWORD)
            .unwrap_or(false);
        if !needs_reset {
            return None;
        }
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
        Some(AwsResponse::ok_json(json!({
            "ChallengeName": "NEW_PASSWORD_REQUIRED",
            "Session": session,
            "ChallengeParameters": {
                "USER_ID_FOR_SRP": username,
                "requiredAttributes": "[]",
                "userAttributes": "{}"
            }
        })))
    }

    /// `RespondToAuthChallenge(ChallengeName=PASSWORD_VERIFIER)`: verify the
    /// client's SRP6a proof against the handshake stashed at InitiateAuth time
    /// and, on success, mint real tokens. Reuses the shared token issuer.
    pub(super) fn respond_password_verifier(
        &self,
        client_id: &str,
        session: &str,
        body: &Value,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        use base64::Engine;

        let bad_creds = || {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Incorrect username or password.",
            )
        };
        let responses = body["ChallengeResponses"].as_object().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "ChallengeResponses is required",
            )
        })?;
        let client_sig = responses
            .get("PASSWORD_CLAIM_SIGNATURE")
            .and_then(|v| v.as_str())
            .ok_or_else(bad_creds)?;
        let timestamp = responses
            .get("TIMESTAMP")
            .and_then(|v| v.as_str())
            .ok_or_else(bad_creds)?;

        let (pool_id, username, region, stash_json) = {
            let accounts = self.state.read();
            let empty = CognitoState::new(&req.account_id, &req.region);
            let state = accounts.get(&req.account_id).unwrap_or(&empty);
            let sess = state.sessions.get(session).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotAuthorizedException",
                    "Invalid session for the user, session is expired.",
                )
            })?;
            if sess.challenge_name != "PASSWORD_VERIFIER" || sess.client_id != client_id {
                return Err(bad_creds());
            }
            let stash = sess.challenge_metadata.clone().ok_or_else(bad_creds)?;
            (
                sess.user_pool_id.clone(),
                sess.username.clone(),
                state.region.clone(),
                stash,
            )
        };

        let stash: Value = serde_json::from_str(&stash_json).map_err(|_| bad_creds())?;
        let get_hex = |k: &str| {
            stash
                .get(k)
                .and_then(|v| v.as_str())
                .and_then(crate::srp::parse_hex)
                .ok_or_else(bad_creds)
        };
        let server_private_b = get_hex("b")?;
        let server_public_b = get_hex("B")?;
        let salt = get_hex("salt")?;
        let verifier = get_hex("v")?;
        let a_hex = stash
            .get("A")
            .and_then(|v| v.as_str())
            .ok_or_else(bad_creds)?;
        let pool_name = stash
            .get("pool_name")
            .and_then(|v| v.as_str())
            .ok_or_else(bad_creds)?;
        let user_id = stash
            .get("user_id")
            .and_then(|v| v.as_str())
            .ok_or_else(bad_creds)?;
        let secret_block = stash
            .get("secret_block")
            .and_then(|v| v.as_str())
            .and_then(|s| base64::engine::general_purpose::STANDARD.decode(s).ok())
            .ok_or_else(bad_creds)?;

        let handshake = crate::srp::ServerHandshake {
            salt,
            server_public_b,
            server_private_b,
        };
        let expected = crate::srp::expected_signature(
            &handshake,
            &verifier,
            a_hex,
            pool_name,
            user_id,
            &secret_block,
            timestamp,
        )
        .ok_or_else(bad_creds)?;

        if !crate::srp::ct_eq(expected.as_bytes(), client_sig.as_bytes()) {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&req.account_id);
            state.auth_events.push(AuthEvent {
                event_id: Uuid::new_v4().to_string(),
                event_type: "SIGN_IN_FAILURE".to_string(),
                username: username.clone(),
                user_pool_id: pool_id.clone(),
                client_id: Some(client_id.to_string()),
                timestamp: Utc::now(),
                success: false,
                feedback_value: None,
            });
            return Err(bad_creds());
        }

        // Single-use session.
        {
            let mut accounts = self.state.write();
            accounts
                .get_or_create(&req.account_id)
                .sessions
                .remove(session);
        }

        // A valid SRP proof of a *temporary* password still owes a reset.
        if let Some(resp) = self.maybe_force_new_password(&pool_id, client_id, &username, req) {
            return Ok(resp);
        }

        self.custom_auth_issue_tokens(&pool_id, client_id, &username, &region, req)
    }

    /// `RespondToAuthChallenge(ChallengeName=SELECT_CHALLENGE)` for the
    /// `USER_AUTH` flow: the client's `ANSWER` picks a challenge. `PASSWORD_SRP`
    /// kicks off the SRP handshake (reusing the shared builder); `PASSWORD`
    /// verifies the password directly and mints tokens.
    pub(super) fn respond_select_challenge(
        &self,
        client_id: &str,
        session: &str,
        body: &Value,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let bad_creds = || {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Incorrect username or password.",
            )
        };
        let responses = body["ChallengeResponses"].as_object().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "ChallengeResponses is required",
            )
        })?;
        let answer = responses
            .get("ANSWER")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "ANSWER is required in ChallengeResponses",
                )
            })?;

        let (pool_id, username, region) = {
            let accounts = self.state.read();
            let empty = CognitoState::new(&req.account_id, &req.region);
            let state = accounts.get(&req.account_id).unwrap_or(&empty);
            let sess = state.sessions.get(session).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotAuthorizedException",
                    "Invalid session for the user, session is expired.",
                )
            })?;
            if sess.challenge_name != "SELECT_CHALLENGE" || sess.client_id != client_id {
                return Err(bad_creds());
            }
            (
                sess.user_pool_id.clone(),
                sess.username.clone(),
                state.region.clone(),
            )
        };
        // The SELECT_CHALLENGE session is single-use.
        {
            let mut accounts = self.state.write();
            accounts
                .get_or_create(&req.account_id)
                .sessions
                .remove(session);
        }

        match answer {
            "PASSWORD_SRP" => {
                let srp_a = responses
                    .get("SRP_A")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidParameterException",
                            "SRP_A is required for the PASSWORD_SRP challenge",
                        )
                    })?;
                self.build_srp_challenge(&pool_id, client_id, &username, srp_a, req)
            }
            "PASSWORD" => {
                let password = responses
                    .get("PASSWORD")
                    .and_then(|v| v.as_str())
                    .ok_or_else(bad_creds)?;
                let ok = {
                    let accounts = self.state.read();
                    let empty = CognitoState::new(&req.account_id, &req.region);
                    let state = accounts.get(&req.account_id).unwrap_or(&empty);
                    state
                        .users
                        .get(&pool_id)
                        .and_then(|users| users.get(&username))
                        .filter(|u| u.enabled)
                        .map(|u| {
                            u.password.as_deref() == Some(password)
                                || u.temporary_password.as_deref() == Some(password)
                        })
                        .unwrap_or(false)
                };
                if !ok {
                    return Err(bad_creds());
                }
                // A temporary password still owes a forced reset.
                if let Some(resp) =
                    self.maybe_force_new_password(&pool_id, client_id, &username, req)
                {
                    return Ok(resp);
                }
                self.custom_auth_issue_tokens(&pool_id, client_id, &username, &region, req)
            }
            other => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                format!("Unsupported challenge answer: {other}"),
            )),
        }
    }

    pub(super) fn respond_new_password_required(
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

    pub(super) async fn respond_custom_challenge(
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
    pub(super) fn custom_challenge_issue_tokens(
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
}
