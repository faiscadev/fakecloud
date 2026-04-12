use std::collections::HashMap;

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::UserAttribute;
use crate::triggers::{self, TriggerSource};

use super::{
    generate_confirmation_code, matches_filter, parse_filter_expression, parse_string_array,
    parse_user_attributes, require_str, user_to_json, validate_password, CognitoService,
};

impl CognitoService {
    pub(super) async fn admin_create_user(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = body["UserPoolId"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "UserPoolId is required",
                )
            })?;

        let username = body["Username"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "Username is required",
                )
            })?;

        let (response, user_clone, region, account_id, pool_id_owned, username_owned) = {
            let mut state = self.state.write();

            // Validate pool exists
            if !state.user_pools.contains_key(pool_id) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("User pool {pool_id} does not exist."),
                ));
            }

            // Check username doesn't already exist
            let pool_users = state.users.entry(pool_id.to_string()).or_default();
            if pool_users.contains_key(username) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "UsernameExistsException",
                    "User account already exists.",
                ));
            }

            let now = Utc::now();
            let sub_val = Uuid::new_v4().to_string();

            // Parse user attributes
            let mut attributes = parse_user_attributes(&body["UserAttributes"]);

            // Ensure sub attribute is present
            if !attributes.iter().any(|a| a.name == "sub") {
                attributes.push(UserAttribute {
                    name: "sub".to_string(),
                    value: sub_val.clone(),
                });
            }

            let temporary_password = body["TemporaryPassword"].as_str().map(|s| s.to_string());

            let user = crate::state::User {
                username: username.to_string(),
                sub: sub_val,
                attributes,
                enabled: true,
                user_status: "FORCE_CHANGE_PASSWORD".to_string(),
                user_create_date: now,
                user_last_modified_date: now,
                password: None,
                temporary_password,
                confirmation_code: None,
                attribute_verification_codes: HashMap::new(),
                mfa_preferences: None,
                totp_secret: None,
                totp_verified: false,
                devices: HashMap::new(),
            };

            let resp = user_to_json(&user);
            let uc = user.clone();
            pool_users.insert(username.to_string(), user);

            let region = state.region.clone();
            let account_id = state.account_id.clone();

            (
                resp,
                uc,
                region,
                account_id,
                pool_id.to_string(),
                username.to_string(),
            )
        };

        // PreSignUp_AdminCreateUser trigger (synchronous)
        if let Some(ref ctx) = self.delivery_ctx {
            if let Some(function_arn) = triggers::get_trigger_arn(
                &self.state,
                &pool_id_owned,
                TriggerSource::PreSignUpAdminCreateUser,
            ) {
                let event = triggers::build_trigger_event(
                    TriggerSource::PreSignUpAdminCreateUser,
                    &pool_id_owned,
                    None,
                    &username_owned,
                    &triggers::collect_user_attributes(&user_clone),
                    &region,
                    &account_id,
                );
                if let Some(response) = triggers::invoke_trigger(ctx, &function_arn, &event).await {
                    if response["response"]["autoConfirmUser"].as_bool() == Some(true) {
                        let mut state = self.state.write();
                        if let Some(u) = state
                            .users
                            .get_mut(&pool_id_owned)
                            .and_then(|users| users.get_mut(&username_owned))
                        {
                            u.user_status = "CONFIRMED".to_string();
                            u.user_last_modified_date = Utc::now();
                        }
                    }
                }
            }
        }

        Ok(AwsResponse::ok_json(json!({ "User": response })))
    }

    pub(super) fn admin_get_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = body["UserPoolId"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "UserPoolId is required",
                )
            })?;

        let username = body["Username"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "Username is required",
                )
            })?;

        let state = self.state.read();

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
            .get(pool_id)
            .and_then(|users| users.get(username))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "UserNotFoundException",
                    "User does not exist.",
                )
            })?;

        // AdminGetUser returns a flat response (not wrapped in User)
        let response = json!({
            "Username": user.username,
            "UserAttributes": user.attributes.iter().map(|a| {
                json!({ "Name": a.name, "Value": a.value })
            }).collect::<Vec<Value>>(),
            "UserCreateDate": user.user_create_date.timestamp() as f64,
            "UserLastModifiedDate": user.user_last_modified_date.timestamp() as f64,
            "UserStatus": user.user_status,
            "Enabled": user.enabled,
        });

        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn admin_delete_user(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = body["UserPoolId"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "UserPoolId is required",
                )
            })?;

        let username = body["Username"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "Username is required",
                )
            })?;

        let mut state = self.state.write();

        // Validate pool exists
        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let pool_users = state.users.get_mut(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UserNotFoundException",
                "User does not exist.",
            )
        })?;

        if pool_users.remove(username).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UserNotFoundException",
                "User does not exist.",
            ));
        }

        // Clean up group memberships for the deleted user
        if let Some(pool_groups) = state.user_groups.get_mut(pool_id) {
            pool_groups.remove(username);
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn admin_disable_user(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = body["UserPoolId"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "UserPoolId is required",
                )
            })?;

        let username = body["Username"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "Username is required",
                )
            })?;

        let mut state = self.state.write();

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

        user.enabled = false;
        user.user_last_modified_date = Utc::now();

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn admin_enable_user(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = body["UserPoolId"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "UserPoolId is required",
                )
            })?;

        let username = body["Username"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "Username is required",
                )
            })?;

        let mut state = self.state.write();

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

        user.enabled = true;
        user.user_last_modified_date = Utc::now();

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn admin_update_user_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = body["UserPoolId"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "UserPoolId is required",
                )
            })?;

        let username = body["Username"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "Username is required",
                )
            })?;

        let new_attrs = parse_user_attributes(&body["UserAttributes"]);

        let mut state = self.state.write();

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

        // Update or add attributes
        for new_attr in new_attrs {
            if let Some(existing) = user.attributes.iter_mut().find(|a| a.name == new_attr.name) {
                existing.value = new_attr.value;
            } else {
                user.attributes.push(new_attr);
            }
        }

        user.user_last_modified_date = Utc::now();

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn admin_delete_user_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = body["UserPoolId"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "UserPoolId is required",
                )
            })?;

        let username = body["Username"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "Username is required",
                )
            })?;

        let attr_names = parse_string_array(&body["UserAttributeNames"]);

        let mut state = self.state.write();

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

        user.attributes.retain(|a| !attr_names.contains(&a.name));
        user.user_last_modified_date = Utc::now();

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_users(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        if pool_id.len() > 55 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "UserPoolId must be 55 characters or fewer",
            ));
        }

        let limit = match body.get("Limit") {
            Some(Value::Number(n)) => {
                let limit = n.as_i64().ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterException",
                        "Limit must be between 1 and 60",
                    )
                })?;
                if !(1..=60).contains(&limit) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterException",
                        "Limit must be between 1 and 60",
                    ));
                }
                limit as usize
            }
            Some(_) => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "Limit must be between 1 and 60",
                ));
            }
            None => 60,
        };
        let pagination_token = body["PaginationToken"].as_str();
        let filter_str = body["Filter"].as_str();

        if let Some(filter) = filter_str {
            if filter.len() > 256 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "Filter must be 256 characters or fewer",
                ));
            }
        }

        if let Some(token) = pagination_token {
            if token.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "PaginationToken must not be empty",
                ));
            }
        }

        let state = self.state.read();

        // Validate pool exists
        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let empty = std::collections::HashMap::new();
        let pool_users = state.users.get(pool_id).unwrap_or(&empty);

        // Sort users by creation date for consistent pagination
        let mut users: Vec<&crate::state::User> = pool_users.values().collect();
        users.sort_by_key(|u| u.user_create_date);

        // Apply filter if present
        if let Some(filter) = filter_str {
            if let Some(parsed) = parse_filter_expression(filter) {
                users.retain(|u| matches_filter(u, &parsed));
            }
        }

        // Find start index from PaginationToken
        let start_idx = if let Some(token) = pagination_token {
            users.iter().position(|u| u.username == token).unwrap_or(0)
        } else {
            0
        };

        let page: Vec<Value> = users
            .iter()
            .skip(start_idx)
            .take(limit)
            .map(|u| user_to_json(u))
            .collect();

        let has_more = start_idx + limit < users.len();
        let mut response = json!({ "Users": page });
        if has_more {
            if let Some(last_user) = users.get(start_idx + limit) {
                response["PaginationToken"] = json!(last_user.username);
            }
        }

        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn admin_set_user_password(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
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
        let permanent = body["Permanent"].as_bool().unwrap_or(false);

        let mut state = self.state.write();

        let pool = state.user_pools.get(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        validate_password(password, &pool.policies.password_policy)?;

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

        if permanent {
            user.password = Some(password.to_string());
            user.temporary_password = None;
            user.user_status = "CONFIRMED".to_string();
        } else {
            user.temporary_password = Some(password.to_string());
        }
        user.user_last_modified_date = Utc::now();

        Ok(AwsResponse::ok_json(json!({})))
    }

    // ── Self-service user operations ───────────────────────────────────

    pub(super) fn get_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let access_token = require_str(&body, "AccessToken")?;

        let state = self.state.read();

        let token_data = state.access_tokens.get(access_token).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid access token.",
            )
        })?;
        let pool_id = &token_data.user_pool_id;
        let username = &token_data.username;

        let user = state
            .users
            .get(pool_id)
            .and_then(|users| users.get(username.as_str()))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotAuthorizedException",
                    "Invalid access token.",
                )
            })?;

        let response = json!({
            "Username": user.username,
            "UserAttributes": user.attributes.iter().map(|a| {
                json!({ "Name": a.name, "Value": a.value })
            }).collect::<Vec<Value>>(),
            "UserCreateDate": user.user_create_date.timestamp() as f64,
            "UserLastModifiedDate": user.user_last_modified_date.timestamp() as f64,
            "UserStatus": user.user_status,
            "MFAOptions": [],
        });

        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn delete_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let access_token = require_str(&body, "AccessToken")?;

        let mut state = self.state.write();

        let token_data = state.access_tokens.get(access_token).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid access token.",
            )
        })?;
        let pool_id = token_data.user_pool_id.clone();
        let username = token_data.username.clone();

        // Delete the user
        let pool_users = state.users.get_mut(&pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid access token.",
            )
        })?;

        if pool_users.remove(&username).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid access token.",
            ));
        }

        // Clean up access tokens for this user
        state
            .access_tokens
            .retain(|_, v| !(v.user_pool_id == pool_id && v.username == username));

        // Clean up refresh tokens for this user
        state
            .refresh_tokens
            .retain(|_, v| !(v.user_pool_id == pool_id && v.username == username));

        // Clean up sessions for this user
        state
            .sessions
            .retain(|_, v| !(v.user_pool_id == pool_id && v.username == username));

        // Clean up group memberships for the deleted user
        if let Some(pool_groups) = state.user_groups.get_mut(&pool_id) {
            pool_groups.remove(&username);
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn update_user_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let access_token = require_str(&body, "AccessToken")?;
        let new_attrs = parse_user_attributes(&body["UserAttributes"]);

        let mut state = self.state.write();

        let token_data = state.access_tokens.get(access_token).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid access token.",
            )
        })?;
        let pool_id = token_data.user_pool_id.clone();
        let username = token_data.username.clone();

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

        for new_attr in new_attrs {
            if let Some(existing) = user.attributes.iter_mut().find(|a| a.name == new_attr.name) {
                existing.value = new_attr.value;
            } else {
                user.attributes.push(new_attr);
            }
        }

        user.user_last_modified_date = Utc::now();

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn delete_user_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let access_token = require_str(&body, "AccessToken")?;
        let attr_names = parse_string_array(&body["UserAttributeNames"]);

        let mut state = self.state.write();

        let token_data = state.access_tokens.get(access_token).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid access token.",
            )
        })?;
        let pool_id = token_data.user_pool_id.clone();
        let username = token_data.username.clone();

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

        user.attributes.retain(|a| !attr_names.contains(&a.name));
        user.user_last_modified_date = Utc::now();

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn get_user_attribute_verification_code(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let access_token = require_str(&body, "AccessToken")?;
        let attribute_name = require_str(&body, "AttributeName")?;

        let mut state = self.state.write();

        let token_data = state.access_tokens.get(access_token).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid access token.",
            )
        })?;
        let pool_id = token_data.user_pool_id.clone();
        let username = token_data.username.clone();

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

        let code = generate_confirmation_code();
        user.attribute_verification_codes
            .insert(attribute_name.to_string(), code);

        // Determine delivery details based on attribute
        let (delivery_medium, destination) = if attribute_name == "phone_number" {
            let phone = user
                .attributes
                .iter()
                .find(|a| a.name == "phone_number")
                .map(|a| {
                    // Mask phone: show last 4 digits
                    let len = a.value.len();
                    if len > 4 {
                        let first: String = a.value.chars().take(1).collect();
                        let last4: String = a.value.chars().skip(len.saturating_sub(4)).collect();
                        format!("{first}***{last4}")
                    } else {
                        "***".to_string()
                    }
                })
                .unwrap_or_else(|| "***".to_string());
            ("SMS", phone)
        } else {
            let email = user
                .attributes
                .iter()
                .find(|a| a.name == "email")
                .map(|a| {
                    if let Some(at_pos) = a.value.find('@') {
                        let first = a.value.chars().next().unwrap_or('*');
                        let domain = &a.value[at_pos..];
                        format!("{first}***{domain}")
                    } else {
                        "***".to_string()
                    }
                })
                .unwrap_or_else(|| "***".to_string());
            ("EMAIL", email)
        };

        Ok(AwsResponse::ok_json(json!({
            "CodeDeliveryDetails": {
                "Destination": destination,
                "DeliveryMedium": delivery_medium,
                "AttributeName": attribute_name
            }
        })))
    }

    pub(super) fn verify_user_attribute(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let access_token = require_str(&body, "AccessToken")?;
        let attribute_name = require_str(&body, "AttributeName")?;
        let code = require_str(&body, "Code")?;

        let mut state = self.state.write();

        let token_data = state.access_tokens.get(access_token).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid access token.",
            )
        })?;
        let pool_id = token_data.user_pool_id.clone();
        let username = token_data.username.clone();

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

        // Validate the code
        let stored_code = user
            .attribute_verification_codes
            .get(attribute_name)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "CodeMismatchException",
                    "Invalid verification code provided, please try again.",
                )
            })?;

        if stored_code != code {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CodeMismatchException",
                "Invalid verification code provided, please try again.",
            ));
        }

        // Remove the used code
        user.attribute_verification_codes.remove(attribute_name);

        // Set the corresponding verified attribute to true
        let verified_attr_name = format!("{attribute_name}_verified");
        if let Some(existing) = user
            .attributes
            .iter_mut()
            .find(|a| a.name == verified_attr_name)
        {
            existing.value = "true".to_string();
        } else {
            user.attributes.push(UserAttribute {
                name: verified_attr_name,
                value: "true".to_string(),
            });
        }

        user.user_last_modified_date = Utc::now();

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn resend_confirmation_code(
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

        let destination = email
            .map(|e| {
                if let Some(at_pos) = e.find('@') {
                    let first = e.chars().next().unwrap_or('*');
                    let domain = &e[at_pos..];
                    format!("{first}***{domain}")
                } else {
                    "***".to_string()
                }
            })
            .unwrap_or_else(|| "***".to_string());

        Ok(AwsResponse::ok_json(json!({
            "CodeDeliveryDetails": {
                "Destination": destination,
                "DeliveryMedium": "EMAIL",
                "AttributeName": "email"
            }
        })))
    }
}
