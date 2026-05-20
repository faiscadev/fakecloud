//! `CognitoService` `password` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl CognitoService {
    pub(crate) fn change_password(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(crate) async fn forgot_password(
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

    pub(crate) fn confirm_forgot_password(
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
}
