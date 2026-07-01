use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::CognitoState;
use crate::state::LinkedProvider;
use crate::state::MfaOption;

use super::{require_str, CognitoService};

/// Parse the legacy `MFAOptions` list shape
/// (`[{DeliveryMedium, AttributeName}]`) into stored options.
fn parse_mfa_options(value: &Value) -> Vec<MfaOption> {
    value
        .as_array()
        .map(|arr| {
            arr.iter()
                .map(|o| MfaOption {
                    delivery_medium: o["DeliveryMedium"].as_str().unwrap_or_default().to_string(),
                    attribute_name: o["AttributeName"].as_str().unwrap_or_default().to_string(),
                })
                .collect()
        })
        .unwrap_or_default()
}

impl CognitoService {
    // ── Legacy MFA Settings ────────────────────────────────────────────

    /// Legacy operation: sets MFA options for a user (deprecated in favor of AdminSetUserMFAPreference)
    pub(super) fn admin_set_user_settings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?.to_string();
        let username = require_str(&body, "Username")?.to_string();
        let mfa_options = parse_mfa_options(&body["MFAOptions"]);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Resolve an email/phone alias -> stored username for
        // UsernameAttributes pools.
        let username = crate::service::resolve_alias_username(state, &pool_id, &username);

        // Validate pool and user exist
        let users = state.users.get_mut(&pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        let user = users.get_mut(&username).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UserNotFoundException",
                format!("User {username} does not exist."),
            )
        })?;

        // Legacy operation — persist the MFA options so GetUser echoes them
        // (maps loosely to AdminSetUserMFAPreference).
        user.mfa_options = mfa_options;
        Ok(AwsResponse::ok_json(json!({})))
    }

    /// Legacy user-facing operation: sets MFA options (deprecated in favor of SetUserMFAPreference)
    pub(super) fn set_user_settings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let access_token = require_str(&body, "AccessToken")?.to_string();
        let mfa_options = parse_mfa_options(&body["MFAOptions"]);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let (pool_id, username) = {
            let token_data = state.valid_access_token(&access_token).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotAuthorizedException",
                    "Invalid access token.",
                )
            })?;
            (token_data.user_pool_id.clone(), token_data.username.clone())
        };

        let user = state
            .users
            .get_mut(&pool_id)
            .and_then(|u| u.get_mut(&username))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "User not found.",
                )
            })?;

        user.mfa_options = mfa_options;
        Ok(AwsResponse::ok_json(json!({})))
    }

    // ── Provider Linking ───────────────────────────────────────────────

    pub(super) fn admin_disable_provider_for_user(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;

        let provider_name = body["User"]["ProviderName"].as_str().unwrap_or_default();
        let provider_attr_value = body["User"]["ProviderAttributeValue"]
            .as_str()
            .unwrap_or_default();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let users = state.users.get_mut(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        // Find the user by linked provider
        let user = users
            .values_mut()
            .find(|u| {
                u.linked_providers.iter().any(|lp| {
                    lp.provider_name == provider_name
                        && lp.provider_attribute_value.as_deref() == Some(provider_attr_value)
                })
            })
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "User with the specified provider not found.",
                )
            })?;

        user.linked_providers.retain(|lp| {
            !(lp.provider_name == provider_name
                && lp.provider_attribute_value.as_deref() == Some(provider_attr_value))
        });

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn admin_link_provider_for_user(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;

        let dest_provider = body["DestinationUser"]["ProviderName"]
            .as_str()
            .unwrap_or("Cognito");
        let dest_attr_value = body["DestinationUser"]["ProviderAttributeValue"]
            .as_str()
            .unwrap_or_default();

        let source_provider = body["SourceUser"]["ProviderName"]
            .as_str()
            .unwrap_or_default();
        let source_attr_name = body["SourceUser"]["ProviderAttributeName"]
            .as_str()
            .map(|s| s.to_string());
        let source_attr_value = body["SourceUser"]["ProviderAttributeValue"]
            .as_str()
            .map(|s| s.to_string());

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // For a Cognito destination the ProviderAttributeValue IS the username,
        // which may be an email/phone alias in a UsernameAttributes pool.
        let dest_username = if dest_provider == "Cognito" {
            crate::service::resolve_alias_username(state, pool_id, dest_attr_value)
        } else {
            dest_attr_value.to_string()
        };

        let users = state.users.get_mut(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        // Find the destination user (by Cognito username or provider attribute)
        let user = if dest_provider == "Cognito" {
            users.get_mut(&dest_username)
        } else {
            users.values_mut().find(|u| {
                u.linked_providers.iter().any(|lp| {
                    lp.provider_name == dest_provider
                        && lp.provider_attribute_value.as_deref() == Some(dest_attr_value)
                })
            })
        };

        let user = user.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "Destination user not found.",
            )
        })?;

        user.linked_providers.push(LinkedProvider {
            provider_name: source_provider.to_string(),
            provider_attribute_name: source_attr_name,
            provider_attribute_value: source_attr_value,
        });

        Ok(AwsResponse::ok_json(json!({})))
    }

    // ── Auth Events ────────────────────────────────────────────────────

    pub(super) fn admin_list_user_auth_events(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let username = require_str(&body, "Username")?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(10).clamp(1, 60) as usize;
        let next_token = body["NextToken"].as_str();

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        // Resolve an email/phone alias -> stored username for
        // UsernameAttributes pools.
        let resolved = crate::service::resolve_alias_username(state, pool_id, username);
        let username = resolved.as_str();

        // Validate pool and user
        let users = state.users.get(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        if !users.contains_key(username) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UserNotFoundException",
                format!("User {username} does not exist."),
            ));
        }

        // Filter events for this user and pool
        let events: Vec<&crate::state::AuthEvent> = state
            .auth_events
            .iter()
            .filter(|e| e.user_pool_id == pool_id && e.username == username)
            .collect();

        // A stale token (its event aged out) ends the listing rather than
        // silently restarting at page 1 (bug-audit 2026-06-20, 1.14).
        let start = match next_token {
            None => 0,
            Some(t) => events
                .iter()
                .position(|e| e.event_id == t)
                .unwrap_or(events.len()),
        };

        let page: Vec<Value> = events
            .iter()
            .skip(start)
            .take(max_results)
            .map(|e| {
                let mut ev = json!({
                    "EventId": e.event_id,
                    "EventType": e.event_type,
                    "CreationDate": e.timestamp.timestamp() as f64,
                    "EventResponse": if e.success { "Pass" } else { "Fail" },
                });
                if let Some(ref fb) = e.feedback_value {
                    ev["EventFeedback"] = json!({
                        "FeedbackValue": fb,
                        "Provider": "COGNITO",
                    });
                }
                ev
            })
            .collect();

        let has_more = start + max_results < events.len();
        let mut result = json!({ "AuthEvents": page });
        if has_more {
            if let Some(last) = events.get(start + max_results) {
                result["NextToken"] = json!(last.event_id);
            }
        }

        Ok(AwsResponse::ok_json(result))
    }

    pub(super) fn admin_update_auth_event_feedback(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let username = require_str(&body, "Username")?;
        let event_id = require_str(&body, "EventId")?;
        let feedback_value = require_str(&body, "FeedbackValue")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Resolve an email/phone alias -> stored username for
        // UsernameAttributes pools.
        let resolved = crate::service::resolve_alias_username(state, pool_id, username);
        let username = resolved.as_str();

        // Validate pool and user
        if !state
            .users
            .get(pool_id)
            .is_some_and(|u| u.contains_key(username))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UserNotFoundException",
                format!("User {username} does not exist."),
            ));
        }

        let event = state
            .auth_events
            .iter_mut()
            .find(|e| e.event_id == event_id && e.user_pool_id == pool_id && e.username == username)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Event {event_id} not found."),
                )
            })?;

        event.feedback_value = Some(feedback_value.to_string());

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn update_auth_event_feedback(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let username = require_str(&body, "Username")?;
        let event_id = require_str(&body, "EventId")?;
        let _feedback_token = require_str(&body, "FeedbackToken")?;
        let feedback_value = require_str(&body, "FeedbackValue")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Resolve an email/phone alias -> stored username for
        // UsernameAttributes pools.
        let resolved = crate::service::resolve_alias_username(state, pool_id, username);
        let username = resolved.as_str();

        // Validate pool and user
        if !state
            .users
            .get(pool_id)
            .is_some_and(|u| u.contains_key(username))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UserNotFoundException",
                format!("User {username} does not exist."),
            ));
        }

        let event = state
            .auth_events
            .iter_mut()
            .find(|e| e.event_id == event_id && e.user_pool_id == pool_id && e.username == username)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Event {event_id} not found."),
                )
            })?;

        event.feedback_value = Some(feedback_value.to_string());

        Ok(AwsResponse::ok_json(json!({})))
    }
}
