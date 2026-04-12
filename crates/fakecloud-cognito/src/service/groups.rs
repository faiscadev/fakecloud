use std::collections::HashMap;

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::Group;

use super::{group_to_json, require_str, user_to_json, CognitoService};

impl CognitoService {
    pub(super) fn create_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let group_name = require_str(&body, "GroupName")?;
        let pool_id = require_str(&body, "UserPoolId")?;
        let description = body["Description"].as_str().map(|s| s.to_string());
        let precedence = body["Precedence"].as_i64();
        let role_arn = body["RoleArn"].as_str().map(|s| s.to_string());

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let pool_groups = state.groups.entry(pool_id.to_string()).or_default();
        if pool_groups.contains_key(group_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "GroupExistsException",
                format!("A group with the name {group_name} already exists."),
            ));
        }

        let now = Utc::now();
        let group = Group {
            group_name: group_name.to_string(),
            user_pool_id: pool_id.to_string(),
            description: description.clone(),
            precedence,
            role_arn: role_arn.clone(),
            creation_date: now,
            last_modified_date: now,
        };

        pool_groups.insert(group_name.to_string(), group.clone());

        Ok(AwsResponse::ok_json(json!({
            "Group": group_to_json(&group)
        })))
    }

    pub(super) fn delete_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let group_name = require_str(&body, "GroupName")?;
        let pool_id = require_str(&body, "UserPoolId")?;

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let removed = state
            .groups
            .get_mut(pool_id)
            .and_then(|groups| groups.remove(group_name));

        if removed.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "Group not found.",
            ));
        }

        // Remove group from all user-group associations in this pool
        if let Some(pool_user_groups) = state.user_groups.get_mut(pool_id) {
            for group_list in pool_user_groups.values_mut() {
                group_list.retain(|g| g != group_name);
            }
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn get_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let group_name = require_str(&body, "GroupName")?;
        let pool_id = require_str(&body, "UserPoolId")?;

        let state = self.state.read();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let group = state
            .groups
            .get(pool_id)
            .and_then(|groups| groups.get(group_name))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "Group not found.",
                )
            })?;

        Ok(AwsResponse::ok_json(json!({
            "Group": group_to_json(group)
        })))
    }

    pub(super) fn update_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let group_name = require_str(&body, "GroupName")?;
        let pool_id = require_str(&body, "UserPoolId")?;

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let group = state
            .groups
            .get_mut(pool_id)
            .and_then(|groups| groups.get_mut(group_name))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "Group not found.",
                )
            })?;

        if let Some(desc) = body["Description"].as_str() {
            group.description = Some(desc.to_string());
        }
        if let Some(prec) = body["Precedence"].as_i64() {
            group.precedence = Some(prec);
        }
        if let Some(arn) = body["RoleArn"].as_str() {
            group.role_arn = Some(arn.to_string());
        }

        group.last_modified_date = Utc::now();

        let group_json = group_to_json(group);

        Ok(AwsResponse::ok_json(json!({
            "Group": group_json
        })))
    }

    pub(super) fn list_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let limit = body["Limit"].as_i64().unwrap_or(60).clamp(1, 60) as usize;
        let next_token = body["NextToken"].as_str();

        let state = self.state.read();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let empty = std::collections::HashMap::new();
        let pool_groups = state.groups.get(pool_id).unwrap_or(&empty);

        let mut groups: Vec<&Group> = pool_groups.values().collect();
        groups.sort_by_key(|g| g.creation_date);

        let start_idx = if let Some(token) = next_token {
            groups
                .iter()
                .position(|g| g.group_name == token)
                .unwrap_or(0)
        } else {
            0
        };

        let page: Vec<Value> = groups
            .iter()
            .skip(start_idx)
            .take(limit)
            .map(|g| group_to_json(g))
            .collect();

        let has_more = start_idx + limit < groups.len();
        let mut response = json!({ "Groups": page });
        if has_more {
            if let Some(last_group) = groups.get(start_idx + limit) {
                response["NextToken"] = json!(last_group.group_name);
            }
        }

        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn admin_add_user_to_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let username = require_str(&body, "Username")?;
        let group_name = require_str(&body, "GroupName")?;

        let mut state = self.state.write();

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

        // Validate group exists
        if !state
            .groups
            .get(pool_id)
            .is_some_and(|groups| groups.contains_key(group_name))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "Group not found.",
            ));
        }

        let user_group_list = state
            .user_groups
            .entry(pool_id.to_string())
            .or_default()
            .entry(username.to_string())
            .or_default();

        if !user_group_list.contains(&group_name.to_string()) {
            user_group_list.push(group_name.to_string());
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn admin_remove_user_from_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let username = require_str(&body, "Username")?;
        let group_name = require_str(&body, "GroupName")?;

        let mut state = self.state.write();

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

        // Validate group exists
        if !state
            .groups
            .get(pool_id)
            .is_some_and(|groups| groups.contains_key(group_name))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "Group not found.",
            ));
        }

        if let Some(user_group_list) = state
            .user_groups
            .get_mut(pool_id)
            .and_then(|m| m.get_mut(username))
        {
            user_group_list.retain(|g| g != group_name);
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn admin_list_groups_for_user(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let username = require_str(&body, "Username")?;
        let limit = body["Limit"].as_i64().unwrap_or(60).clamp(1, 60) as usize;
        let next_token = body["NextToken"].as_str();

        let state = self.state.read();

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

        let empty_user_groups: HashMap<String, Vec<String>> = HashMap::new();
        let empty_group_list: Vec<String> = Vec::new();
        let user_group_names = state
            .user_groups
            .get(pool_id)
            .unwrap_or(&empty_user_groups)
            .get(username)
            .unwrap_or(&empty_group_list);

        let empty_groups: HashMap<String, Group> = HashMap::new();
        let pool_groups = state.groups.get(pool_id).unwrap_or(&empty_groups);

        // Collect and sort groups by creation date
        let mut groups: Vec<&Group> = user_group_names
            .iter()
            .filter_map(|name| pool_groups.get(name))
            .collect();
        groups.sort_by_key(|g| g.creation_date);

        let start_idx = if let Some(token) = next_token {
            groups
                .iter()
                .position(|g| g.group_name == token)
                .unwrap_or(0)
        } else {
            0
        };

        let page: Vec<Value> = groups
            .iter()
            .skip(start_idx)
            .take(limit)
            .map(|g| group_to_json(g))
            .collect();

        let has_more = start_idx + limit < groups.len();
        let mut response = json!({ "Groups": page });
        if has_more {
            if let Some(last_group) = groups.get(start_idx + limit) {
                response["NextToken"] = json!(last_group.group_name);
            }
        }

        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn list_users_in_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let group_name = require_str(&body, "GroupName")?;
        let limit = body["Limit"].as_i64().unwrap_or(60).clamp(1, 60) as usize;
        let next_token = body["NextToken"].as_str();

        let state = self.state.read();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        // Validate group exists
        if !state
            .groups
            .get(pool_id)
            .is_some_and(|groups| groups.contains_key(group_name))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "Group not found.",
            ));
        }

        let empty_user_groups: HashMap<String, Vec<String>> = HashMap::new();
        let pool_user_groups = state.user_groups.get(pool_id).unwrap_or(&empty_user_groups);

        // Find all usernames that belong to this group
        let mut usernames_in_group: Vec<&str> = pool_user_groups
            .iter()
            .filter(|(_, groups)| groups.contains(&group_name.to_string()))
            .map(|(username, _)| username.as_str())
            .collect();
        usernames_in_group.sort();

        let empty_users = std::collections::HashMap::new();
        let pool_users = state.users.get(pool_id).unwrap_or(&empty_users);

        // Sort by creation date for consistent pagination
        let mut users_in_group: Vec<&crate::state::User> = usernames_in_group
            .iter()
            .filter_map(|username| pool_users.get(*username))
            .collect();
        users_in_group.sort_by_key(|u| u.user_create_date);

        let start_idx = if let Some(token) = next_token {
            users_in_group
                .iter()
                .position(|u| u.username == token)
                .unwrap_or(0)
        } else {
            0
        };

        let page: Vec<Value> = users_in_group
            .iter()
            .skip(start_idx)
            .take(limit)
            .map(|u| user_to_json(u))
            .collect();

        let has_more = start_idx + limit < users_in_group.len();
        let mut response = json!({ "Users": page });
        if has_more {
            if let Some(last_user) = users_in_group.get(start_idx + limit) {
                response["NextToken"] = json!(last_user.username);
            }
        }

        Ok(AwsResponse::ok_json(response))
    }
}
