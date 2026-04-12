use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{default_schema_attributes, PoolPolicies, UserPool, UserPoolClient};

use super::{
    generate_client_id, generate_client_secret, generate_pool_id, parse_account_recovery_setting,
    parse_admin_create_user_config, parse_email_configuration, parse_password_policy,
    parse_schema_attribute, parse_sms_configuration, parse_string_array, parse_tags,
    parse_token_validity_units, user_pool_client_to_json, user_pool_to_json, CognitoService,
};

impl CognitoService {
    pub(super) fn create_user_pool(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_name = body["PoolName"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "1 validation error detected: Value at 'poolName' failed to satisfy constraint: Member must not be null",
                )
            })?;

        let mut state = self.state.write();
        let pool_id = generate_pool_id(&state.region);
        let arn = format!(
            "arn:aws:cognito-idp:{}:{}:userpool/{}",
            state.region, state.account_id, pool_id
        );

        let now = Utc::now();

        // Parse password policy or use defaults
        let password_policy = parse_password_policy(&body["Policies"]["PasswordPolicy"]);

        // Parse auto verified attributes
        let auto_verified_attributes = parse_string_array(&body["AutoVerifiedAttributes"]);

        // Parse username/alias attributes
        let username_attributes = if body["UsernameAttributes"].is_array() {
            Some(parse_string_array(&body["UsernameAttributes"]))
        } else {
            None
        };

        let alias_attributes = if body["AliasAttributes"].is_array() {
            Some(parse_string_array(&body["AliasAttributes"]))
        } else {
            None
        };

        // Parse schema — merge with defaults
        let mut schema_attributes = default_schema_attributes();
        if let Some(custom_attrs) = body["Schema"].as_array() {
            for attr_val in custom_attrs {
                if let Some(attr) = parse_schema_attribute(attr_val) {
                    // Only add custom attributes (don't override defaults)
                    if !schema_attributes.iter().any(|a| a.name == attr.name) {
                        schema_attributes.push(attr);
                    }
                }
            }
        }

        // Lambda config — store raw JSON
        let lambda_config = if body["LambdaConfig"].is_object() {
            Some(body["LambdaConfig"].clone())
        } else {
            None
        };

        let mfa_configuration = body["MfaConfiguration"]
            .as_str()
            .unwrap_or("OFF")
            .to_string();

        let email_configuration = parse_email_configuration(&body["EmailConfiguration"]);
        let sms_configuration = parse_sms_configuration(&body["SmsConfiguration"]);
        let admin_create_user_config =
            parse_admin_create_user_config(&body["AdminCreateUserConfig"]);

        let user_pool_tags = parse_tags(&body["UserPoolTags"]);
        let account_recovery_setting =
            parse_account_recovery_setting(&body["AccountRecoverySetting"]);

        let deletion_protection = body["DeletionProtection"].as_str().map(|s| s.to_string());

        let pool = UserPool {
            id: pool_id.clone(),
            name: pool_name.to_string(),
            arn,
            status: "ACTIVE".to_string(),
            creation_date: now,
            last_modified_date: now,
            policies: PoolPolicies { password_policy },
            auto_verified_attributes,
            username_attributes,
            alias_attributes,
            schema_attributes,
            lambda_config,
            mfa_configuration,
            email_configuration,
            sms_configuration,
            admin_create_user_config,
            user_pool_tags,
            account_recovery_setting,
            deletion_protection,
            estimated_number_of_users: 0,
            software_token_mfa_configuration: None,
            sms_mfa_configuration: None,
        };

        let response = user_pool_to_json(&pool);
        state.user_pools.insert(pool_id, pool);

        Ok(AwsResponse::ok_json(json!({ "UserPool": response })))
    }

    pub(super) fn describe_user_pool(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = body["UserPoolId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "UserPoolId is required",
            )
        })?;

        let state = self.state.read();
        let pool = state.user_pools.get(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        // Count actual users
        let user_count = state
            .users
            .get(pool_id)
            .map(|u| u.len() as i64)
            .unwrap_or(0);
        let mut pool_clone = pool.clone();
        pool_clone.estimated_number_of_users = user_count;

        let response = user_pool_to_json(&pool_clone);
        Ok(AwsResponse::ok_json(json!({ "UserPool": response })))
    }

    pub(super) fn update_user_pool(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = body["UserPoolId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "UserPoolId is required",
            )
        })?;

        let mut state = self.state.write();
        let pool = state.user_pools.get_mut(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        // Update fields that are present in the request
        if body["Policies"]["PasswordPolicy"].is_object() {
            pool.policies.password_policy =
                parse_password_policy(&body["Policies"]["PasswordPolicy"]);
        }

        if body["AutoVerifiedAttributes"].is_array() {
            pool.auto_verified_attributes = parse_string_array(&body["AutoVerifiedAttributes"]);
        }

        if body["LambdaConfig"].is_object() {
            pool.lambda_config = Some(body["LambdaConfig"].clone());
        }

        if let Some(mfa) = body["MfaConfiguration"].as_str() {
            pool.mfa_configuration = mfa.to_string();
        }

        if body["EmailConfiguration"].is_object() {
            pool.email_configuration = parse_email_configuration(&body["EmailConfiguration"]);
        }

        if body["SmsConfiguration"].is_object() {
            pool.sms_configuration = parse_sms_configuration(&body["SmsConfiguration"]);
        }

        if body["AdminCreateUserConfig"].is_object() {
            pool.admin_create_user_config =
                parse_admin_create_user_config(&body["AdminCreateUserConfig"]);
        }

        if body["UserPoolTags"].is_object() {
            pool.user_pool_tags = parse_tags(&body["UserPoolTags"]);
        }

        if body["AccountRecoverySetting"].is_object() {
            pool.account_recovery_setting =
                parse_account_recovery_setting(&body["AccountRecoverySetting"]);
        }

        if let Some(dp) = body["DeletionProtection"].as_str() {
            pool.deletion_protection = Some(dp.to_string());
        }

        pool.last_modified_date = Utc::now();

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn delete_user_pool(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = body["UserPoolId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "UserPoolId is required",
            )
        })?;

        let mut state = self.state.write();

        if state.user_pools.remove(pool_id).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        // Remove associated users
        state.users.remove(pool_id);

        // Remove associated clients
        state
            .user_pool_clients
            .retain(|_, c| c.user_pool_id != pool_id);

        // Remove associated groups and user-group associations
        state.groups.remove(pool_id);
        state.user_groups.remove(pool_id);

        // Remove associated identity providers
        state.identity_providers.remove(pool_id);

        // Remove associated resource servers
        state.resource_servers.remove(pool_id);

        // Remove associated domains
        state.domains.retain(|_, d| d.user_pool_id != pool_id);

        // Remove associated tags (match by pool ARN)
        let arn_prefix = format!(
            "arn:aws:cognito-idp:{}:{}:userpool/{}",
            state.region, state.account_id, pool_id
        );
        state.tags.remove(&arn_prefix);

        // Remove associated import jobs
        state.import_jobs.remove(pool_id);

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_user_pools(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let max_results = body["MaxResults"].as_i64().unwrap_or(60).clamp(1, 60) as usize;

        let next_token = body["NextToken"].as_str();

        let state = self.state.read();

        // Sort pools by creation date for consistent pagination
        let mut pools: Vec<&UserPool> = state.user_pools.values().collect();
        pools.sort_by_key(|p| p.creation_date);

        // Find start index from NextToken
        let start_idx = if let Some(token) = next_token {
            pools.iter().position(|p| p.id == token).unwrap_or(0)
        } else {
            0
        };

        let page: Vec<Value> = pools
            .iter()
            .skip(start_idx)
            .take(max_results)
            .map(|p| {
                let mut obj = json!({
                    "Id": p.id,
                    "Name": p.name,
                    "CreationDate": p.creation_date.timestamp() as f64,
                    "LastModifiedDate": p.last_modified_date.timestamp() as f64,
                    "Status": p.status,
                });
                if let Some(ref lc) = p.lambda_config {
                    obj["LambdaConfig"] = lc.clone();
                }
                obj
            })
            .collect();

        let has_more = start_idx + max_results < pools.len();
        let mut response = json!({ "UserPools": page });
        if has_more {
            if let Some(last_pool) = pools.get(start_idx + max_results) {
                response["NextToken"] = json!(last_pool.id);
            }
        }

        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn create_user_pool_client(
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

        let client_name = body["ClientName"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "ClientName is required",
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

        let client_id = generate_client_id();
        let generate_secret = body["GenerateSecret"].as_bool().unwrap_or(false);
        let client_secret = if generate_secret {
            Some(generate_client_secret())
        } else {
            None
        };

        let now = Utc::now();

        let client = UserPoolClient {
            client_id: client_id.clone(),
            client_name: client_name.to_string(),
            user_pool_id: pool_id.to_string(),
            client_secret,
            explicit_auth_flows: parse_string_array(&body["ExplicitAuthFlows"]),
            token_validity_units: parse_token_validity_units(&body["TokenValidityUnits"]),
            access_token_validity: body["AccessTokenValidity"].as_i64(),
            id_token_validity: body["IdTokenValidity"].as_i64(),
            refresh_token_validity: body["RefreshTokenValidity"].as_i64(),
            callback_urls: parse_string_array(&body["CallbackURLs"]),
            logout_urls: parse_string_array(&body["LogoutURLs"]),
            supported_identity_providers: parse_string_array(&body["SupportedIdentityProviders"]),
            allowed_o_auth_flows: parse_string_array(&body["AllowedOAuthFlows"]),
            allowed_o_auth_scopes: parse_string_array(&body["AllowedOAuthScopes"]),
            allowed_o_auth_flows_user_pool_client: body["AllowedOAuthFlowsUserPoolClient"]
                .as_bool()
                .unwrap_or(false),
            prevent_user_existence_errors: body["PreventUserExistenceErrors"]
                .as_str()
                .map(|s| s.to_string()),
            read_attributes: parse_string_array(&body["ReadAttributes"]),
            write_attributes: parse_string_array(&body["WriteAttributes"]),
            creation_date: now,
            last_modified_date: now,
            enable_token_revocation: body["EnableTokenRevocation"].as_bool().unwrap_or(true),
            auth_session_validity: body["AuthSessionValidity"].as_i64(),
        };

        let response = user_pool_client_to_json(&client);
        state.user_pool_clients.insert(client_id, client);

        Ok(AwsResponse::ok_json(json!({ "UserPoolClient": response })))
    }

    pub(super) fn describe_user_pool_client(
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

        let client_id = body["ClientId"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "ClientId is required",
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

        let client = state.user_pool_clients.get(client_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool client {client_id} does not exist."),
            )
        })?;

        // Validate client belongs to the specified pool
        if client.user_pool_id != pool_id {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool client {client_id} does not exist."),
            ));
        }

        let response = user_pool_client_to_json(client);
        Ok(AwsResponse::ok_json(json!({ "UserPoolClient": response })))
    }

    pub(super) fn update_user_pool_client(
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

        let client_id = body["ClientId"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "ClientId is required",
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

        let client = state.user_pool_clients.get_mut(client_id).ok_or_else(|| {
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

        // Update fields that are present
        if let Some(name) = body["ClientName"].as_str() {
            if name.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "ClientName cannot be empty",
                ));
            }
            client.client_name = name.to_string();
        }
        if body["ExplicitAuthFlows"].is_array() {
            client.explicit_auth_flows = parse_string_array(&body["ExplicitAuthFlows"]);
        }
        if body["TokenValidityUnits"].is_object() {
            client.token_validity_units = parse_token_validity_units(&body["TokenValidityUnits"]);
        }
        if let Some(v) = body["AccessTokenValidity"].as_i64() {
            client.access_token_validity = Some(v);
        }
        if let Some(v) = body["IdTokenValidity"].as_i64() {
            client.id_token_validity = Some(v);
        }
        if let Some(v) = body["RefreshTokenValidity"].as_i64() {
            client.refresh_token_validity = Some(v);
        }
        if body["CallbackURLs"].is_array() {
            client.callback_urls = parse_string_array(&body["CallbackURLs"]);
        }
        if body["LogoutURLs"].is_array() {
            client.logout_urls = parse_string_array(&body["LogoutURLs"]);
        }
        if body["SupportedIdentityProviders"].is_array() {
            client.supported_identity_providers =
                parse_string_array(&body["SupportedIdentityProviders"]);
        }
        if body["AllowedOAuthFlows"].is_array() {
            client.allowed_o_auth_flows = parse_string_array(&body["AllowedOAuthFlows"]);
        }
        if body["AllowedOAuthScopes"].is_array() {
            client.allowed_o_auth_scopes = parse_string_array(&body["AllowedOAuthScopes"]);
        }
        if let Some(v) = body["AllowedOAuthFlowsUserPoolClient"].as_bool() {
            client.allowed_o_auth_flows_user_pool_client = v;
        }
        if let Some(v) = body["PreventUserExistenceErrors"].as_str() {
            client.prevent_user_existence_errors = Some(v.to_string());
        }
        if body["ReadAttributes"].is_array() {
            client.read_attributes = parse_string_array(&body["ReadAttributes"]);
        }
        if body["WriteAttributes"].is_array() {
            client.write_attributes = parse_string_array(&body["WriteAttributes"]);
        }
        if let Some(v) = body["EnableTokenRevocation"].as_bool() {
            client.enable_token_revocation = v;
        }
        if let Some(v) = body["AuthSessionValidity"].as_i64() {
            client.auth_session_validity = Some(v);
        }

        client.last_modified_date = Utc::now();

        let response = user_pool_client_to_json(client);
        Ok(AwsResponse::ok_json(json!({ "UserPoolClient": response })))
    }

    pub(super) fn delete_user_pool_client(
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

        let client_id = body["ClientId"]
            .as_str()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "ClientId is required",
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

        // Check client exists and belongs to the pool
        match state.user_pool_clients.get(client_id) {
            Some(c) if c.user_pool_id == pool_id => {}
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("User pool client {client_id} does not exist."),
                ));
            }
        }

        state.user_pool_clients.remove(client_id);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_user_pool_clients(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = body["UserPoolId"].as_str().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "UserPoolId is required",
            )
        })?;

        let max_results = body["MaxResults"].as_i64().unwrap_or(60).clamp(1, 60) as usize;
        let next_token = body["NextToken"].as_str();

        let state = self.state.read();

        // Validate pool exists
        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        // Filter clients for this pool, sort by creation date
        let mut clients: Vec<&UserPoolClient> = state
            .user_pool_clients
            .values()
            .filter(|c| c.user_pool_id == pool_id)
            .collect();
        clients.sort_by_key(|c| c.creation_date);

        // Find start index from NextToken
        let start_idx = if let Some(token) = next_token {
            clients
                .iter()
                .position(|c| c.client_id == token)
                .unwrap_or(0)
        } else {
            0
        };

        let page: Vec<Value> = clients
            .iter()
            .skip(start_idx)
            .take(max_results)
            .map(|c| {
                json!({
                    "ClientId": c.client_id,
                    "ClientName": c.client_name,
                    "UserPoolId": c.user_pool_id,
                })
            })
            .collect();

        let has_more = start_idx + max_results < clients.len();
        let mut response = json!({ "UserPoolClients": page });
        if has_more {
            if let Some(last_client) = clients.get(start_idx + max_results) {
                response["NextToken"] = json!(last_client.client_id);
            }
        }

        Ok(AwsResponse::ok_json(response))
    }
}
