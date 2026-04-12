use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::LinkedProvider;

use super::{require_str, CognitoService};

impl CognitoService {
    // ── Legacy MFA Settings ────────────────────────────────────────────

    /// Legacy operation: sets MFA options for a user (deprecated in favor of AdminSetUserMFAPreference)
    pub(super) fn admin_set_user_settings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let username = require_str(&body, "Username")?;
        let _mfa_options = body["MFAOptions"].as_array();

        let state = self.state.read();

        // Validate pool and user exist
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

        // Legacy operation — accept but don't change behavior (maps to AdminSetUserMFAPreference)
        Ok(AwsResponse::ok_json(json!({})))
    }

    /// Legacy user-facing operation: sets MFA options (deprecated in favor of SetUserMFAPreference)
    pub(super) fn set_user_settings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let access_token = require_str(&body, "AccessToken")?;
        let _mfa_options = body["MFAOptions"].as_array();

        let state = self.state.read();

        let token_data = state.access_tokens.get(access_token).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid access token.",
            )
        })?;

        // Validate user exists
        if !state
            .users
            .get(&token_data.user_pool_id)
            .is_some_and(|u| u.contains_key(&token_data.username))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "User not found.",
            ));
        }

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

        let mut state = self.state.write();

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

        let mut state = self.state.write();

        let users = state.users.get_mut(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        // Find the destination user (by Cognito username or provider attribute)
        let user = if dest_provider == "Cognito" {
            users.get_mut(dest_attr_value)
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

        let state = self.state.read();

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

        let start = next_token
            .and_then(|t| events.iter().position(|e| e.event_id == t))
            .unwrap_or(0);

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

        let mut state = self.state.write();

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

        let mut state = self.state.write();

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
