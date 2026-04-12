use std::collections::HashMap;

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{
    AccessTokenData, AuthEvent, ChallengeResult, RefreshTokenData, SessionData, UserAttribute,
};
use crate::triggers::{self, TriggerSource};

use super::{
    generate_confirmation_code, generate_tokens, parse_user_attributes, require_str,
    validate_password, CognitoService,
};

impl CognitoService {
    pub(super) async fn admin_initiate_auth(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let client_id = require_str(&body, "ClientId")?;
        let auth_flow = require_str(&body, "AuthFlow")?;

        let auth_params = body["AuthParameters"].as_object().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "AuthParameters is required",
            )
        })?;

        match auth_flow {
            "ADMIN_NO_SRP_AUTH" | "ADMIN_USER_PASSWORD_AUTH" => {}
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    format!("Unsupported auth flow: {auth_flow}"),
                ));
            }
        }

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

        // First lock scope: validate user exists, extract trigger data, then drop lock
        let (user_attrs, region, account_id, pool_id_owned, username_owned, client_id_owned) = {
            let state = self.state.read();

            // Validate pool exists
            if !state.user_pools.contains_key(pool_id) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("User pool {pool_id} does not exist."),
                ));
            }

            // Validate client exists and belongs to pool
            let client = state.user_pool_clients.get(client_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("User pool client {client_id} does not exist."),
                )
            })?;
            if client.user_pool_id != pool_id {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("User pool client {client_id} does not exist."),
                ));
            }

            // Validate ExplicitAuthFlows allows this auth flow
            let allowed = match auth_flow {
                "ADMIN_NO_SRP_AUTH" => client
                    .explicit_auth_flows
                    .iter()
                    .any(|f| f == "ADMIN_NO_SRP_AUTH" || f == "ALLOW_ADMIN_USER_PASSWORD_AUTH"),
                "ADMIN_USER_PASSWORD_AUTH" => client.explicit_auth_flows.iter().any(|f| {
                    f == "ADMIN_USER_PASSWORD_AUTH" || f == "ALLOW_ADMIN_USER_PASSWORD_AUTH"
                }),
                _ => false,
            };
            if !allowed {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "Client is not allowed for this auth flow.",
                ));
            }

            // Validate user exists and is enabled
            let user = state
                .users
                .get(pool_id)
                .and_then(|users| users.get(username))
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

            // Collect user attributes for triggers
            let user_attrs = triggers::collect_user_attributes(user);
            let region = state.region.clone();
            let account_id = state.account_id.clone();

            (
                user_attrs,
                region,
                account_id,
                pool_id.to_string(),
                username.to_string(),
                client_id.to_string(),
            )
        };

        // PreAuthentication_Authentication trigger (synchronous — can reject auth)
        if let Some(ref ctx) = self.delivery_ctx {
            if let Some(function_arn) = triggers::get_trigger_arn(
                &self.state,
                &pool_id_owned,
                TriggerSource::PreAuthenticationAuthentication,
            ) {
                let event = triggers::build_trigger_event(
                    TriggerSource::PreAuthenticationAuthentication,
                    &pool_id_owned,
                    Some(&client_id_owned),
                    &username_owned,
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

        // Second lock scope: password check, token generation, state mutations
        let tokens = {
            let mut state = self.state.write();

            // Re-validate user exists (could have been modified between lock scopes)
            let user = state
                .users
                .get(pool_id)
                .and_then(|users| users.get(username))
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "UserNotFoundException",
                        "User does not exist.",
                    )
                })?;

            // Validate password
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

            // Check if user needs to change password
            if user.user_status == "FORCE_CHANGE_PASSWORD" {
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

            // Generate tokens
            let sub = user.sub.clone();
            let tokens = generate_tokens(pool_id, client_id, &sub, username, &region);

            // Store refresh token
            state.refresh_tokens.insert(
                tokens.refresh_token.clone(),
                RefreshTokenData {
                    user_pool_id: pool_id.to_string(),
                    username: username.to_string(),
                    client_id: client_id.to_string(),
                    issued_at: Utc::now(),
                },
            );

            // Store access token
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

            tokens
        };

        // PostAuthentication_Authentication trigger (fire-and-forget)
        if let Some(ref ctx) = self.delivery_ctx {
            if let Some(function_arn) = triggers::get_trigger_arn(
                &self.state,
                &pool_id_owned,
                TriggerSource::PostAuthenticationAuthentication,
            ) {
                let event = triggers::build_trigger_event(
                    TriggerSource::PostAuthenticationAuthentication,
                    &pool_id_owned,
                    Some(&client_id_owned),
                    &username_owned,
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

    pub(super) async fn initiate_auth(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let client_id = require_str(&body, "ClientId")?;
        let auth_flow = require_str(&body, "AuthFlow")?;

        // Resolve pool_id and auth flows from client in a scoped lock
        let (pool_id, explicit_auth_flows) = {
            let state = self.state.read();
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
                // Validate client allows this flow
                if !explicit_auth_flows.contains(&"ALLOW_USER_PASSWORD_AUTH".to_string()) {
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

                // First lock scope: validate user exists, extract trigger data, then drop lock
                let (user_attrs, region, account_id) = {
                    let state = self.state.read();

                    let user = state
                        .users
                        .get(&pool_id)
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

                    // Collect user attributes for triggers
                    let user_attrs = triggers::collect_user_attributes(user);
                    let region = state.region.clone();
                    let account_id = state.account_id.clone();

                    (user_attrs, region, account_id)
                };

                let username_owned = username.to_string();
                let client_id_owned = client_id.to_string();

                // PreAuthentication_Authentication trigger (synchronous — can reject auth)
                if let Some(ref ctx) = self.delivery_ctx {
                    if let Some(function_arn) = triggers::get_trigger_arn(
                        &self.state,
                        &pool_id,
                        TriggerSource::PreAuthenticationAuthentication,
                    ) {
                        let event = triggers::build_trigger_event(
                            TriggerSource::PreAuthenticationAuthentication,
                            &pool_id,
                            Some(&client_id_owned),
                            &username_owned,
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

                // Second lock scope: password check, token generation, state mutations
                let tokens = {
                    let mut state = self.state.write();

                    // Re-validate user exists (could have been modified between lock scopes)
                    let user = state
                        .users
                        .get(&pool_id)
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

                    if user.user_status == "FORCE_CHANGE_PASSWORD" {
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
                    let tokens = generate_tokens(&pool_id, client_id, &sub, username, &region);

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

                    tokens
                };

                // PostAuthentication_Authentication trigger (fire-and-forget)
                if let Some(ref ctx) = self.delivery_ctx {
                    if let Some(function_arn) = triggers::get_trigger_arn(
                        &self.state,
                        &pool_id,
                        TriggerSource::PostAuthenticationAuthentication,
                    ) {
                        let event = triggers::build_trigger_event(
                            TriggerSource::PostAuthenticationAuthentication,
                            &pool_id,
                            Some(&client_id_owned),
                            &username_owned,
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
            "CUSTOM_AUTH" => {
                // Validate client allows this flow
                if !explicit_auth_flows.contains(&"ALLOW_CUSTOM_AUTH".to_string()) {
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

                // Look up user and collect attributes
                let (user_attrs, region, account_id) = {
                    let state = self.state.read();
                    let user = state
                        .users
                        .get(&pool_id)
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

                let username_owned = username.to_string();
                let client_id_owned = client_id.to_string();
                let challenge_results: Vec<ChallengeResult> = vec![];

                // DefineAuthChallenge Lambda is required for CUSTOM_AUTH
                let ctx = self.delivery_ctx.as_ref().ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidLambdaResponseException",
                        "No Lambda trigger configured for DefineAuthChallenge.",
                    )
                })?;

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
                    Some(&client_id_owned),
                    &username_owned,
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
                    let mut state = self.state.write();
                    state.auth_events.push(AuthEvent {
                        event_id: Uuid::new_v4().to_string(),
                        event_type: "SIGN_IN_FAILURE".to_string(),
                        username: username_owned.clone(),
                        user_pool_id: pool_id.clone(),
                        client_id: Some(client_id_owned.clone()),
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
                    // Issue tokens immediately
                    let mut state = self.state.write();
                    let user = state
                        .users
                        .get(&pool_id)
                        .and_then(|users| users.get(&username_owned))
                        .ok_or_else(|| {
                            AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "NotAuthorizedException",
                                "Incorrect username or password.",
                            )
                        })?;

                    let sub = user.sub.clone();
                    let tokens =
                        generate_tokens(&pool_id, &client_id_owned, &sub, &username_owned, &region);

                    state.refresh_tokens.insert(
                        tokens.refresh_token.clone(),
                        RefreshTokenData {
                            user_pool_id: pool_id.clone(),
                            username: username_owned.clone(),
                            client_id: client_id_owned.clone(),
                            issued_at: Utc::now(),
                        },
                    );
                    state.access_tokens.insert(
                        tokens.access_token.clone(),
                        AccessTokenData {
                            user_pool_id: pool_id.clone(),
                            username: username_owned.clone(),
                            client_id: client_id_owned.clone(),
                            issued_at: Utc::now(),
                        },
                    );
                    state.auth_events.push(AuthEvent {
                        event_id: Uuid::new_v4().to_string(),
                        event_type: "SIGN_IN".to_string(),
                        username: username_owned,
                        user_pool_id: pool_id,
                        client_id: Some(client_id_owned),
                        timestamp: Utc::now(),
                        success: true,
                        feedback_value: None,
                    });

                    return Ok(AwsResponse::ok_json(json!({
                        "AuthenticationResult": {
                            "AccessToken": tokens.access_token,
                            "IdToken": tokens.id_token,
                            "RefreshToken": tokens.refresh_token,
                            "TokenType": "Bearer",
                            "ExpiresIn": 3600
                        }
                    })));
                }

                // DefineAuthChallenge wants to issue a challenge
                let challenge_name = define_response["response"]["challengeName"]
                    .as_str()
                    .unwrap_or("CUSTOM_CHALLENGE")
                    .to_string();

                // Invoke CreateAuthChallenge Lambda
                let create_arn = triggers::get_trigger_arn(
                    &self.state,
                    &pool_id,
                    TriggerSource::CreateAuthChallengeAuthentication,
                );

                let mut public_challenge_params = serde_json::Map::new();
                let mut challenge_metadata: Option<String> = None;

                if let Some(create_arn) = create_arn {
                    let create_event = triggers::build_create_auth_challenge_event(
                        &pool_id,
                        Some(&client_id_owned),
                        &username_owned,
                        &user_attrs,
                        &challenge_name,
                        &challenge_results,
                        &region,
                        &account_id,
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

                // Store session
                let session = Uuid::new_v4().to_string();
                {
                    let mut state = self.state.write();
                    state.sessions.insert(
                        session.clone(),
                        SessionData {
                            user_pool_id: pool_id,
                            username: username_owned,
                            client_id: client_id_owned,
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

                // Add USERNAME to challenge parameters (AWS always includes it)
                response["ChallengeParameters"]["USERNAME"] = json!(username);

                Ok(AwsResponse::ok_json(response))
            }
            "REFRESH_TOKEN_AUTH" | "REFRESH_TOKEN" => {
                // Validate client allows this flow
                if !explicit_auth_flows.contains(&"ALLOW_REFRESH_TOKEN_AUTH".to_string()) {
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

                let mut state = self.state.write();

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
                let tokens =
                    generate_tokens(&token_pool_id, client_id, &sub, &token_username, &region);

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
            _ => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                format!("Unsupported auth flow: {auth_flow}"),
            )),
        }
    }

    pub(super) async fn respond_to_auth_challenge(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let client_id = require_str(&body, "ClientId")?;
        let challenge_name = require_str(&body, "ChallengeName")?;
        let session = require_str(&body, "Session")?;

        self.handle_auth_challenge_response(client_id, challenge_name, session, &body)
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
            let state = self.state.read();
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

        self.handle_auth_challenge_response(client_id, challenge_name, session, &body)
            .await
    }

    pub(super) async fn handle_auth_challenge_response(
        &self,
        client_id: &str,
        challenge_name: &str,
        session: &str,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        match challenge_name {
            "NEW_PASSWORD_REQUIRED" => {
                let challenge_responses =
                    body["ChallengeResponses"].as_object().ok_or_else(|| {
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

                let mut state = self.state.write();

                let session_data = state.sessions.remove(session).ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "NotAuthorizedException",
                        "Invalid session.",
                    )
                })?;

                if session_data.client_id != client_id {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "NotAuthorizedException",
                        "Invalid session.",
                    ));
                }

                if session_data.challenge_name != "NEW_PASSWORD_REQUIRED" {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "NotAuthorizedException",
                        "Invalid session.",
                    ));
                }

                // Validate password against pool policy (clone to release immutable borrow)
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
                user.user_status = "CONFIRMED".to_string();
                user.user_last_modified_date = Utc::now();

                let sub = user.sub.clone();
                let username = user.username.clone();
                let pool_id = session_data.user_pool_id.clone();

                let tokens = generate_tokens(&pool_id, client_id, &sub, &username, &region);

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
            "CUSTOM_CHALLENGE" => {
                let challenge_responses =
                    body["ChallengeResponses"].as_object().ok_or_else(|| {
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

                // Extract session data (remove session to consume it)
                let (
                    pool_id,
                    username,
                    session_client_id,
                    mut challenge_results,
                    challenge_metadata,
                ) = {
                    let mut state = self.state.write();
                    let session_data = state.sessions.remove(session).ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "NotAuthorizedException",
                            "Invalid session.",
                        )
                    })?;

                    if session_data.client_id != client_id {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "NotAuthorizedException",
                            "Invalid session.",
                        ));
                    }

                    if session_data.challenge_name != "CUSTOM_CHALLENGE" {
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

                // Get user attributes
                let (user_attrs, region, account_id) = {
                    let state = self.state.read();
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

                // Invoke VerifyAuthChallengeResponse Lambda
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

                let verify_event = triggers::build_verify_auth_challenge_event(
                    &pool_id,
                    Some(&session_client_id),
                    &username,
                    &user_attrs,
                    answer,
                    challenge_metadata.as_deref(),
                    &region,
                    &account_id,
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

                // Record this challenge result
                challenge_results.push(ChallengeResult {
                    challenge_name: "CUSTOM_CHALLENGE".to_string(),
                    challenge_result: answer_correct,
                    challenge_metadata: challenge_metadata.clone(),
                });

                // Invoke DefineAuthChallenge again with updated session
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
                    let mut state = self.state.write();
                    state.auth_events.push(AuthEvent {
                        event_id: Uuid::new_v4().to_string(),
                        event_type: "SIGN_IN_FAILURE".to_string(),
                        username: username.clone(),
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
                    // Issue tokens
                    let mut state = self.state.write();
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

                    let sub = user.sub.clone();
                    let tokens =
                        generate_tokens(&pool_id, &session_client_id, &sub, &username, &region);

                    state.refresh_tokens.insert(
                        tokens.refresh_token.clone(),
                        RefreshTokenData {
                            user_pool_id: pool_id.clone(),
                            username: username.clone(),
                            client_id: session_client_id.clone(),
                            issued_at: Utc::now(),
                        },
                    );
                    state.access_tokens.insert(
                        tokens.access_token.clone(),
                        AccessTokenData {
                            user_pool_id: pool_id.clone(),
                            username: username.clone(),
                            client_id: session_client_id.clone(),
                            issued_at: Utc::now(),
                        },
                    );
                    state.auth_events.push(AuthEvent {
                        event_id: Uuid::new_v4().to_string(),
                        event_type: "SIGN_IN".to_string(),
                        username,
                        user_pool_id: pool_id,
                        client_id: Some(session_client_id),
                        timestamp: Utc::now(),
                        success: true,
                        feedback_value: None,
                    });

                    return Ok(AwsResponse::ok_json(json!({
                        "AuthenticationResult": {
                            "AccessToken": tokens.access_token,
                            "IdToken": tokens.id_token,
                            "RefreshToken": tokens.refresh_token,
                            "TokenType": "Bearer",
                            "ExpiresIn": 3600
                        }
                    })));
                }

                // Another challenge round — invoke CreateAuthChallenge
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
                    let create_event = triggers::build_create_auth_challenge_event(
                        &pool_id,
                        Some(&session_client_id),
                        &username,
                        &user_attrs,
                        &next_challenge_name,
                        &challenge_results,
                        &region,
                        &account_id,
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

                // Store new session
                let new_session = Uuid::new_v4().to_string();
                {
                    let mut state = self.state.write();
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
            _ => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                format!("Unsupported challenge: {challenge_name}"),
            )),
        }
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
            let mut state = self.state.write();

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
                user_status: "UNCONFIRMED".to_string(),
                user_create_date: now,
                user_last_modified_date: now,
                password: Some(password.to_string()),
                temporary_password: None,
                confirmation_code: None,
                attribute_verification_codes: HashMap::new(),
                mfa_preferences: None,
                totp_secret: None,
                totp_verified: false,
                devices: HashMap::new(),
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
            let mut state = self.state.write();
            if let Some(u) = state
                .users
                .get_mut(&pool_id)
                .and_then(|users| users.get_mut(username))
            {
                u.user_status = "CONFIRMED".to_string();
                u.user_last_modified_date = Utc::now();
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

        let mut state = self.state.write();

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

        user.user_status = "CONFIRMED".to_string();
        user.user_last_modified_date = Utc::now();

        let user_attrs = triggers::collect_user_attributes(user);
        let region = state.region.clone();
        let account_id = state.account_id.clone();
        drop(state);

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

        let mut state = self.state.write();

        // Validate pool exists
        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

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

        user.user_status = "CONFIRMED".to_string();
        user.user_last_modified_date = Utc::now();

        let user_attrs = triggers::collect_user_attributes(user);
        let region = state.region.clone();
        let account_id = state.account_id.clone();
        drop(state);

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

        let mut state = self.state.write();

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

        let mut state = self.state.write();

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
        user.confirmation_code = Some(code);

        // Find email from user attributes for CodeDeliveryDetails
        let email = user
            .attributes
            .iter()
            .find(|a| a.name == "email")
            .map(|a| a.value.clone());

        let user_attrs = triggers::collect_user_attributes(user);

        let destination = email
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

        drop(state);

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

        let mut state = self.state.write();

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
        user.user_status = "CONFIRMED".to_string();
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

        let mut state = self.state.write();

        // Validate pool exists
        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

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

        user.user_status = "RESET_REQUIRED".to_string();
        user.confirmation_code = Some(generate_confirmation_code());
        user.user_last_modified_date = Utc::now();

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn global_sign_out(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let access_token = require_str(&body, "AccessToken")?;

        let mut state = self.state.write();

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

        let mut state = self.state.write();

        // Validate pool exists
        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

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
