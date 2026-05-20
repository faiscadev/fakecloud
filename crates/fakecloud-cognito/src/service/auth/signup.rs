//! `CognitoService` `signup` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl CognitoService {
    pub(crate) async fn sign_up(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(crate) async fn confirm_sign_up(
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
}
