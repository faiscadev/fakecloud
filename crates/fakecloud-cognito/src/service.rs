use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{
    default_schema_attributes, AccountRecoverySetting, AdminCreateUserConfig, EmailConfiguration,
    InviteMessageTemplate, PasswordPolicy, PoolPolicies, RecoveryOption, RefreshTokenData,
    SchemaAttribute, SessionData, SharedCognitoState, SmsConfiguration, StringAttributeConstraints,
    TokenValidityUnits, User, UserAttribute, UserPool, UserPoolClient,
};

pub struct CognitoService {
    state: SharedCognitoState,
}

impl CognitoService {
    pub fn new(state: SharedCognitoState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl AwsService for CognitoService {
    fn service_name(&self) -> &str {
        "cognito-idp"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match req.action.as_str() {
            "CreateUserPool" => self.create_user_pool(&req),
            "DescribeUserPool" => self.describe_user_pool(&req),
            "UpdateUserPool" => self.update_user_pool(&req),
            "DeleteUserPool" => self.delete_user_pool(&req),
            "ListUserPools" => self.list_user_pools(&req),
            "CreateUserPoolClient" => self.create_user_pool_client(&req),
            "DescribeUserPoolClient" => self.describe_user_pool_client(&req),
            "UpdateUserPoolClient" => self.update_user_pool_client(&req),
            "DeleteUserPoolClient" => self.delete_user_pool_client(&req),
            "ListUserPoolClients" => self.list_user_pool_clients(&req),
            "AdminCreateUser" => self.admin_create_user(&req),
            "AdminGetUser" => self.admin_get_user(&req),
            "AdminDeleteUser" => self.admin_delete_user(&req),
            "AdminDisableUser" => self.admin_disable_user(&req),
            "AdminEnableUser" => self.admin_enable_user(&req),
            "AdminUpdateUserAttributes" => self.admin_update_user_attributes(&req),
            "AdminDeleteUserAttributes" => self.admin_delete_user_attributes(&req),
            "ListUsers" => self.list_users(&req),
            "AdminSetUserPassword" => self.admin_set_user_password(&req),
            "AdminInitiateAuth" => self.admin_initiate_auth(&req),
            "InitiateAuth" => self.initiate_auth(&req),
            "RespondToAuthChallenge" => self.respond_to_auth_challenge(&req),
            "AdminRespondToAuthChallenge" => self.admin_respond_to_auth_challenge(&req),
            "SignUp" => self.sign_up(&req),
            "ConfirmSignUp" => self.confirm_sign_up(&req),
            "AdminConfirmSignUp" => self.admin_confirm_sign_up(&req),
            _ => Err(AwsServiceError::action_not_implemented(
                "cognito-idp",
                &req.action,
            )),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "CreateUserPool",
            "DescribeUserPool",
            "UpdateUserPool",
            "DeleteUserPool",
            "ListUserPools",
            "CreateUserPoolClient",
            "DescribeUserPoolClient",
            "UpdateUserPoolClient",
            "DeleteUserPoolClient",
            "ListUserPoolClients",
            "AdminCreateUser",
            "AdminGetUser",
            "AdminDeleteUser",
            "AdminDisableUser",
            "AdminEnableUser",
            "AdminUpdateUserAttributes",
            "AdminDeleteUserAttributes",
            "ListUsers",
            "AdminSetUserPassword",
            "AdminInitiateAuth",
            "InitiateAuth",
            "RespondToAuthChallenge",
            "AdminRespondToAuthChallenge",
            "SignUp",
            "ConfirmSignUp",
            "AdminConfirmSignUp",
        ]
    }
}

impl CognitoService {
    fn create_user_pool(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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
        };

        let response = user_pool_to_json(&pool);
        state.user_pools.insert(pool_id, pool);

        Ok(AwsResponse::ok_json(json!({ "UserPool": response })))
    }

    fn describe_user_pool(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn update_user_pool(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn delete_user_pool(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_user_pools(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn create_user_pool_client(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn describe_user_pool_client(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn update_user_pool_client(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn delete_user_pool_client(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn list_user_pool_clients(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn admin_create_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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
        let sub = Uuid::new_v4().to_string();

        // Parse user attributes
        let mut attributes = parse_user_attributes(&body["UserAttributes"]);

        // Ensure sub attribute is present
        if !attributes.iter().any(|a| a.name == "sub") {
            attributes.push(UserAttribute {
                name: "sub".to_string(),
                value: sub.clone(),
            });
        }

        let temporary_password = body["TemporaryPassword"].as_str().map(|s| s.to_string());

        let user = User {
            username: username.to_string(),
            sub: sub.clone(),
            attributes,
            enabled: true,
            user_status: "FORCE_CHANGE_PASSWORD".to_string(),
            user_create_date: now,
            user_last_modified_date: now,
            password: None,
            temporary_password,
        };

        let response = user_to_json(&user);
        pool_users.insert(username.to_string(), user);

        Ok(AwsResponse::ok_json(json!({ "User": response })))
    }

    fn admin_get_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn admin_delete_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn admin_disable_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn admin_enable_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn admin_update_user_attributes(
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

    fn admin_delete_user_attributes(
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

    fn list_users(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

        let limit = body["Limit"].as_i64().unwrap_or(60).clamp(1, 60) as usize;
        let pagination_token = body["PaginationToken"].as_str();
        let filter_str = body["Filter"].as_str();

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
        let mut users: Vec<&User> = pool_users.values().collect();
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

    fn admin_set_user_password(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn admin_initiate_auth(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

        let mut state = self.state.write();

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
            "ADMIN_USER_PASSWORD_AUTH" => client
                .explicit_auth_flows
                .iter()
                .any(|f| f == "ADMIN_USER_PASSWORD_AUTH" || f == "ALLOW_ADMIN_USER_PASSWORD_AUTH"),
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

        // Validate password
        let password_matches = match (&user.password, &user.temporary_password) {
            (Some(p), _) if p == password => true,
            (_, Some(tp)) if tp == password => true,
            _ => false,
        };
        if !password_matches {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Incorrect username or password.",
            ));
        }

        let region = state.region.clone();

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

    fn initiate_auth(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let client_id = require_str(&body, "ClientId")?;
        let auth_flow = require_str(&body, "AuthFlow")?;

        let mut state = self.state.write();

        // Find client
        let client = state.user_pool_clients.get(client_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool client {client_id} does not exist."),
            )
        })?;

        let pool_id = client.user_pool_id.clone();
        let explicit_auth_flows = client.explicit_auth_flows.clone();

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

                let password_matches = match (&user.password, &user.temporary_password) {
                    (Some(p), _) if p == password => true,
                    (_, Some(tp)) if tp == password => true,
                    _ => false,
                };
                if !password_matches {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "NotAuthorizedException",
                        "Incorrect username or password.",
                    ));
                }

                let region = state.region.clone();

                if user.user_status == "FORCE_CHANGE_PASSWORD" {
                    let session = Uuid::new_v4().to_string();
                    state.sessions.insert(
                        session.clone(),
                        SessionData {
                            user_pool_id: pool_id.to_string(),
                            username: username.to_string(),
                            client_id: client_id.to_string(),
                            challenge_name: "NEW_PASSWORD_REQUIRED".to_string(),
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

    fn respond_to_auth_challenge(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let client_id = require_str(&body, "ClientId")?;
        let challenge_name = require_str(&body, "ChallengeName")?;
        let session = require_str(&body, "Session")?;

        self.handle_auth_challenge_response(client_id, challenge_name, session, &body)
    }

    fn admin_respond_to_auth_challenge(
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
    }

    fn handle_auth_challenge_response(
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
            _ => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                format!("Unsupported challenge: {challenge_name}"),
            )),
        }
    }

    fn sign_up(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

        let user = User {
            username: username.to_string(),
            sub: sub.clone(),
            attributes,
            enabled: true,
            user_status: "UNCONFIRMED".to_string(),
            user_create_date: now,
            user_last_modified_date: now,
            password: Some(password.to_string()),
            temporary_password: None,
        };

        pool_users.insert(username.to_string(), user);

        Ok(AwsResponse::ok_json(json!({
            "UserConfirmed": false,
            "UserSub": sub
        })))
    }

    fn confirm_sign_up(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn admin_confirm_sign_up(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

        Ok(AwsResponse::ok_json(json!({})))
    }
}

/// Generate a pool ID in the format `{region}_{9 random alphanumeric chars}`.
fn generate_pool_id(region: &str) -> String {
    let random_part: String = Uuid::new_v4()
        .to_string()
        .replace('-', "")
        .chars()
        .filter(|c| c.is_alphanumeric())
        .take(9)
        .collect();
    // Ensure we always have exactly 9 chars (UUID v4 hex is 32 chars, so this is safe)
    format!("{}_{}", region, random_part)
}

/// Generate a client ID: 26 lowercase alphanumeric characters (like AWS).
fn generate_client_id() -> String {
    // Use two UUIDs to get enough alphanumeric chars (each UUID gives 32 hex chars)
    let uuid1 = Uuid::new_v4().to_string().replace('-', "");
    let uuid2 = Uuid::new_v4().to_string().replace('-', "");
    format!("{}{}", uuid1, uuid2)
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .take(26)
        .collect::<String>()
        .to_lowercase()
}

/// Generate a client secret: 51 base64 characters (like AWS).
fn generate_client_secret() -> String {
    use base64::Engine;
    // Generate enough random bytes via UUIDs to produce 51+ base64 chars
    let mut bytes = Vec::with_capacity(48);
    for _ in 0..3 {
        bytes.extend_from_slice(Uuid::new_v4().as_bytes());
    }
    let encoded = base64::engine::general_purpose::STANDARD.encode(&bytes);
    encoded.chars().take(51).collect()
}

fn parse_token_validity_units(val: &Value) -> Option<TokenValidityUnits> {
    if !val.is_object() {
        return None;
    }
    Some(TokenValidityUnits {
        access_token: val["AccessToken"].as_str().map(|s| s.to_string()),
        id_token: val["IdToken"].as_str().map(|s| s.to_string()),
        refresh_token: val["RefreshToken"].as_str().map(|s| s.to_string()),
    })
}

/// Convert a UserPoolClient to the JSON format AWS returns.
fn user_pool_client_to_json(client: &UserPoolClient) -> Value {
    let mut obj = json!({
        "ClientId": client.client_id,
        "ClientName": client.client_name,
        "UserPoolId": client.user_pool_id,
        "CreationDate": client.creation_date.timestamp() as f64,
        "LastModifiedDate": client.last_modified_date.timestamp() as f64,
        "ExplicitAuthFlows": client.explicit_auth_flows,
        "AllowedOAuthFlowsUserPoolClient": client.allowed_o_auth_flows_user_pool_client,
        "EnableTokenRevocation": client.enable_token_revocation,
    });

    if let Some(ref secret) = client.client_secret {
        obj["ClientSecret"] = json!(secret);
    }
    if let Some(ref tvu) = client.token_validity_units {
        let mut units = json!({});
        if let Some(ref v) = tvu.access_token {
            units["AccessToken"] = json!(v);
        }
        if let Some(ref v) = tvu.id_token {
            units["IdToken"] = json!(v);
        }
        if let Some(ref v) = tvu.refresh_token {
            units["RefreshToken"] = json!(v);
        }
        obj["TokenValidityUnits"] = units;
    }
    if let Some(v) = client.access_token_validity {
        obj["AccessTokenValidity"] = json!(v);
    }
    if let Some(v) = client.id_token_validity {
        obj["IdTokenValidity"] = json!(v);
    }
    if let Some(v) = client.refresh_token_validity {
        obj["RefreshTokenValidity"] = json!(v);
    }
    if !client.callback_urls.is_empty() {
        obj["CallbackURLs"] = json!(client.callback_urls);
    }
    if !client.logout_urls.is_empty() {
        obj["LogoutURLs"] = json!(client.logout_urls);
    }
    if !client.supported_identity_providers.is_empty() {
        obj["SupportedIdentityProviders"] = json!(client.supported_identity_providers);
    }
    if !client.allowed_o_auth_flows.is_empty() {
        obj["AllowedOAuthFlows"] = json!(client.allowed_o_auth_flows);
    }
    if !client.allowed_o_auth_scopes.is_empty() {
        obj["AllowedOAuthScopes"] = json!(client.allowed_o_auth_scopes);
    }
    if let Some(ref v) = client.prevent_user_existence_errors {
        obj["PreventUserExistenceErrors"] = json!(v);
    }
    if !client.read_attributes.is_empty() {
        obj["ReadAttributes"] = json!(client.read_attributes);
    }
    if !client.write_attributes.is_empty() {
        obj["WriteAttributes"] = json!(client.write_attributes);
    }
    if let Some(v) = client.auth_session_validity {
        obj["AuthSessionValidity"] = json!(v);
    }

    obj
}

fn parse_password_policy(val: &Value) -> PasswordPolicy {
    if val.is_null() || !val.is_object() {
        return PasswordPolicy::default();
    }

    PasswordPolicy {
        minimum_length: val["MinimumLength"].as_i64().unwrap_or(8),
        require_uppercase: val["RequireUppercase"].as_bool().unwrap_or(false),
        require_lowercase: val["RequireLowercase"].as_bool().unwrap_or(false),
        require_numbers: val["RequireNumbers"].as_bool().unwrap_or(false),
        require_symbols: val["RequireSymbols"].as_bool().unwrap_or(false),
        temporary_password_validity_days: val["TemporaryPasswordValidityDays"]
            .as_i64()
            .unwrap_or(7),
    }
}

fn parse_string_array(val: &Value) -> Vec<String> {
    val.as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_schema_attribute(val: &Value) -> Option<SchemaAttribute> {
    let name = val["Name"].as_str()?;
    Some(SchemaAttribute {
        name: name.to_string(),
        attribute_data_type: val["AttributeDataType"]
            .as_str()
            .unwrap_or("String")
            .to_string(),
        developer_only_attribute: val["DeveloperOnlyAttribute"].as_bool().unwrap_or(false),
        mutable: val["Mutable"].as_bool().unwrap_or(true),
        required: val["Required"].as_bool().unwrap_or(false),
        string_attribute_constraints: if val["StringAttributeConstraints"].is_object() {
            Some(StringAttributeConstraints {
                min_length: val["StringAttributeConstraints"]["MinLength"]
                    .as_str()
                    .map(|s| s.to_string()),
                max_length: val["StringAttributeConstraints"]["MaxLength"]
                    .as_str()
                    .map(|s| s.to_string()),
            })
        } else {
            None
        },
        number_attribute_constraints: None,
    })
}

fn parse_email_configuration(val: &Value) -> Option<EmailConfiguration> {
    if !val.is_object() {
        return None;
    }
    Some(EmailConfiguration {
        source_arn: val["SourceArn"].as_str().map(|s| s.to_string()),
        reply_to_email_address: val["ReplyToEmailAddress"].as_str().map(|s| s.to_string()),
        email_sending_account: val["EmailSendingAccount"].as_str().map(|s| s.to_string()),
        from_email_address: val["From"].as_str().map(|s| s.to_string()),
        configuration_set: val["ConfigurationSet"].as_str().map(|s| s.to_string()),
    })
}

fn parse_sms_configuration(val: &Value) -> Option<SmsConfiguration> {
    if !val.is_object() {
        return None;
    }
    Some(SmsConfiguration {
        sns_caller_arn: val["SnsCallerArn"].as_str().map(|s| s.to_string()),
        external_id: val["ExternalId"].as_str().map(|s| s.to_string()),
        sns_region: val["SnsRegion"].as_str().map(|s| s.to_string()),
    })
}

fn parse_admin_create_user_config(val: &Value) -> Option<AdminCreateUserConfig> {
    if !val.is_object() {
        return None;
    }
    let invite = if val["InviteMessageTemplate"].is_object() {
        Some(InviteMessageTemplate {
            email_message: val["InviteMessageTemplate"]["EmailMessage"]
                .as_str()
                .map(|s| s.to_string()),
            email_subject: val["InviteMessageTemplate"]["EmailSubject"]
                .as_str()
                .map(|s| s.to_string()),
            sms_message: val["InviteMessageTemplate"]["SMSMessage"]
                .as_str()
                .map(|s| s.to_string()),
        })
    } else {
        None
    };
    Some(AdminCreateUserConfig {
        allow_admin_create_user_only: val["AllowAdminCreateUserOnly"].as_bool(),
        invite_message_template: invite,
        unused_account_validity_days: val["UnusedAccountValidityDays"].as_i64(),
    })
}

fn parse_tags(val: &Value) -> std::collections::HashMap<String, String> {
    let mut tags = std::collections::HashMap::new();
    if let Some(obj) = val.as_object() {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                tags.insert(k.clone(), s.to_string());
            }
        }
    }
    tags
}

fn parse_account_recovery_setting(val: &Value) -> Option<AccountRecoverySetting> {
    if !val.is_object() {
        return None;
    }
    let mechanisms = val["RecoveryMechanisms"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    Some(RecoveryOption {
                        name: v["Name"].as_str()?.to_string(),
                        priority: v["Priority"].as_i64().unwrap_or(1),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Some(AccountRecoverySetting {
        recovery_mechanisms: mechanisms,
    })
}

fn parse_user_attributes(val: &Value) -> Vec<UserAttribute> {
    val.as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    let name = v["Name"].as_str()?;
                    let value = v["Value"].as_str().unwrap_or("");
                    Some(UserAttribute {
                        name: name.to_string(),
                        value: value.to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Convert a User to the JSON format AWS returns (for ListUsers and AdminCreateUser response).
fn user_to_json(user: &User) -> Value {
    json!({
        "Username": user.username,
        "Attributes": user.attributes.iter().map(|a| {
            json!({ "Name": a.name, "Value": a.value })
        }).collect::<Vec<Value>>(),
        "UserCreateDate": user.user_create_date.timestamp() as f64,
        "UserLastModifiedDate": user.user_last_modified_date.timestamp() as f64,
        "UserStatus": user.user_status,
        "Enabled": user.enabled,
    })
}

/// A parsed filter expression for ListUsers.
#[derive(Debug)]
struct FilterExpression {
    attribute: String,
    operator: FilterOp,
    value: String,
}

#[derive(Debug)]
enum FilterOp {
    Equals,
    StartsWith,
}

/// Parse a Cognito ListUsers filter expression like `email = "foo@bar.com"` or `email ^= "foo"`.
fn parse_filter_expression(filter: &str) -> Option<FilterExpression> {
    let filter = filter.trim();

    // Try ^= first (starts with)
    if let Some((attr, val)) = filter.split_once("^=") {
        let attribute = attr.trim().trim_matches('"').to_string();
        let value = val.trim().trim_matches('"').to_string();
        return Some(FilterExpression {
            attribute,
            operator: FilterOp::StartsWith,
            value,
        });
    }

    // Try = (equals)
    if let Some((attr, val)) = filter.split_once('=') {
        let attribute = attr.trim().trim_matches('"').to_string();
        let value = val.trim().trim_matches('"').to_string();
        return Some(FilterExpression {
            attribute,
            operator: FilterOp::Equals,
            value,
        });
    }

    None
}

/// Check if a user matches a filter expression.
fn matches_filter(user: &User, filter: &FilterExpression) -> bool {
    let user_value = match filter.attribute.as_str() {
        "username" => Some(user.username.as_str()),
        "sub" => Some(user.sub.as_str()),
        "cognito:user_status" | "status" => Some(user.user_status.as_str()),
        attr => user
            .attributes
            .iter()
            .find(|a| a.name == attr)
            .map(|a| a.value.as_str()),
    };

    match (&filter.operator, user_value) {
        (FilterOp::Equals, Some(v)) => v == filter.value,
        (FilterOp::StartsWith, Some(v)) => v.starts_with(&filter.value),
        _ => false,
    }
}

/// Convert a UserPool to the JSON format AWS returns.
fn user_pool_to_json(pool: &UserPool) -> Value {
    let mut obj = json!({
        "Id": pool.id,
        "Name": pool.name,
        "Arn": pool.arn,
        "Status": pool.status,
        "CreationDate": pool.creation_date.timestamp() as f64,
        "LastModifiedDate": pool.last_modified_date.timestamp() as f64,
        "Policies": {
            "PasswordPolicy": {
                "MinimumLength": pool.policies.password_policy.minimum_length,
                "RequireUppercase": pool.policies.password_policy.require_uppercase,
                "RequireLowercase": pool.policies.password_policy.require_lowercase,
                "RequireNumbers": pool.policies.password_policy.require_numbers,
                "RequireSymbols": pool.policies.password_policy.require_symbols,
                "TemporaryPasswordValidityDays": pool.policies.password_policy.temporary_password_validity_days,
            }
        },
        "AutoVerifiedAttributes": pool.auto_verified_attributes,
        "MfaConfiguration": pool.mfa_configuration,
        "EstimatedNumberOfUsers": pool.estimated_number_of_users,
        "UserPoolTags": pool.user_pool_tags,
        "SchemaAttributes": pool.schema_attributes.iter().map(|a| {
            let mut attr = json!({
                "Name": a.name,
                "AttributeDataType": a.attribute_data_type,
                "DeveloperOnlyAttribute": a.developer_only_attribute,
                "Mutable": a.mutable,
                "Required": a.required,
            });
            if let Some(ref sc) = a.string_attribute_constraints {
                attr["StringAttributeConstraints"] = json!({});
                if let Some(ref min) = sc.min_length {
                    attr["StringAttributeConstraints"]["MinLength"] = json!(min);
                }
                if let Some(ref max) = sc.max_length {
                    attr["StringAttributeConstraints"]["MaxLength"] = json!(max);
                }
            }
            if let Some(ref nc) = a.number_attribute_constraints {
                attr["NumberAttributeConstraints"] = json!({});
                if let Some(ref min) = nc.min_value {
                    attr["NumberAttributeConstraints"]["MinValue"] = json!(min);
                }
                if let Some(ref max) = nc.max_value {
                    attr["NumberAttributeConstraints"]["MaxValue"] = json!(max);
                }
            }
            attr
        }).collect::<Vec<Value>>(),
    });

    if let Some(ref ua) = pool.username_attributes {
        obj["UsernameAttributes"] = json!(ua);
    }
    if let Some(ref aa) = pool.alias_attributes {
        obj["AliasAttributes"] = json!(aa);
    }
    if let Some(ref lc) = pool.lambda_config {
        obj["LambdaConfig"] = lc.clone();
    }
    if let Some(ref ec) = pool.email_configuration {
        let mut email = json!({});
        if let Some(ref v) = ec.source_arn {
            email["SourceArn"] = json!(v);
        }
        if let Some(ref v) = ec.reply_to_email_address {
            email["ReplyToEmailAddress"] = json!(v);
        }
        if let Some(ref v) = ec.email_sending_account {
            email["EmailSendingAccount"] = json!(v);
        }
        if let Some(ref v) = ec.from_email_address {
            email["From"] = json!(v);
        }
        if let Some(ref v) = ec.configuration_set {
            email["ConfigurationSet"] = json!(v);
        }
        obj["EmailConfiguration"] = email;
    }
    if let Some(ref sc) = pool.sms_configuration {
        let mut sms = json!({});
        if let Some(ref v) = sc.sns_caller_arn {
            sms["SnsCallerArn"] = json!(v);
        }
        if let Some(ref v) = sc.external_id {
            sms["ExternalId"] = json!(v);
        }
        if let Some(ref v) = sc.sns_region {
            sms["SnsRegion"] = json!(v);
        }
        obj["SmsConfiguration"] = sms;
    }
    if let Some(ref ac) = pool.admin_create_user_config {
        let mut admin = json!({});
        if let Some(v) = ac.allow_admin_create_user_only {
            admin["AllowAdminCreateUserOnly"] = json!(v);
        }
        if let Some(ref imt) = ac.invite_message_template {
            let mut tmpl = json!({});
            if let Some(ref v) = imt.email_message {
                tmpl["EmailMessage"] = json!(v);
            }
            if let Some(ref v) = imt.email_subject {
                tmpl["EmailSubject"] = json!(v);
            }
            if let Some(ref v) = imt.sms_message {
                tmpl["SMSMessage"] = json!(v);
            }
            admin["InviteMessageTemplate"] = tmpl;
        }
        if let Some(v) = ac.unused_account_validity_days {
            admin["UnusedAccountValidityDays"] = json!(v);
        }
        obj["AdminCreateUserConfig"] = admin;
    }
    if let Some(ref ars) = pool.account_recovery_setting {
        obj["AccountRecoverySetting"] = json!({
            "RecoveryMechanisms": ars.recovery_mechanisms.iter().map(|r| {
                json!({
                    "Name": r.name,
                    "Priority": r.priority,
                })
            }).collect::<Vec<Value>>(),
        });
    }
    if let Some(ref dp) = pool.deletion_protection {
        obj["DeletionProtection"] = json!(dp);
    }

    obj
}

/// Helper to extract a required string field from JSON.
fn require_str<'a>(body: &'a Value, field: &str) -> Result<&'a str, AwsServiceError> {
    body[field]
        .as_str()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                format!("{field} is required"),
            )
        })
}

/// Validate a password against a pool's password policy.
fn validate_password(password: &str, policy: &PasswordPolicy) -> Result<(), AwsServiceError> {
    if (password.len() as i64) < policy.minimum_length {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidPasswordException",
            format!(
                "Password did not conform with policy: Password not long enough (minimum {})",
                policy.minimum_length
            ),
        ));
    }
    if policy.require_uppercase && !password.chars().any(|c| c.is_ascii_uppercase()) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidPasswordException",
            "Password did not conform with policy: Password must have uppercase characters",
        ));
    }
    if policy.require_lowercase && !password.chars().any(|c| c.is_ascii_lowercase()) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidPasswordException",
            "Password did not conform with policy: Password must have lowercase characters",
        ));
    }
    if policy.require_numbers && !password.chars().any(|c| c.is_ascii_digit()) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidPasswordException",
            "Password did not conform with policy: Password must have numeric characters",
        ));
    }
    if policy.require_symbols
        && !password
            .chars()
            .any(|c| !c.is_ascii_alphanumeric() && c.is_ascii())
    {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidPasswordException",
            "Password did not conform with policy: Password must have symbol characters",
        ));
    }
    Ok(())
}

struct TokenSet {
    id_token: String,
    access_token: String,
    refresh_token: String,
}

/// Generate structurally valid JWTs for Cognito auth responses.
fn generate_tokens(
    pool_id: &str,
    client_id: &str,
    sub: &str,
    username: &str,
    region: &str,
) -> TokenSet {
    let b64url = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let now = Utc::now().timestamp();
    let jti = Uuid::new_v4().to_string();
    let iss = format!("https://cognito-idp.{region}.amazonaws.com/{pool_id}");

    // ID Token
    let id_header = json!({"kid": "fakecloud-key-1", "alg": "RS256"});
    let id_payload = json!({
        "sub": sub,
        "iss": iss,
        "aud": client_id,
        "cognito:username": username,
        "token_use": "id",
        "auth_time": now,
        "exp": now + 3600,
        "iat": now,
        "jti": jti,
    });
    let id_token = sign_jwt(&id_header, &id_payload, &b64url);

    // Access Token
    let access_jti = Uuid::new_v4().to_string();
    let access_header = json!({"kid": "fakecloud-key-1", "alg": "RS256"});
    let access_payload = json!({
        "sub": sub,
        "iss": iss,
        "client_id": client_id,
        "token_use": "access",
        "scope": "aws.cognito.signin.user.admin",
        "jti": access_jti,
        "exp": now + 3600,
        "iat": now,
    });
    let access_token = sign_jwt(&access_header, &access_payload, &b64url);

    // Refresh Token — random base64url string
    let mut refresh_bytes = Vec::with_capacity(72);
    for _ in 0..5 {
        refresh_bytes.extend_from_slice(Uuid::new_v4().as_bytes());
    }
    let refresh_token = b64url.encode(&refresh_bytes);

    TokenSet {
        id_token,
        access_token,
        refresh_token,
    }
}

/// Create a JWT with header.payload.signature using SHA256.
fn sign_jwt(
    header: &Value,
    payload: &Value,
    engine: &base64::engine::general_purpose::GeneralPurpose,
) -> String {
    let header_b64 = engine.encode(header.to_string().as_bytes());
    let payload_b64 = engine.encode(payload.to_string().as_bytes());
    let signing_input = format!("{header_b64}.{payload_b64}");
    let mut hasher = Sha256::new();
    hasher.update(signing_input.as_bytes());
    let signature = hasher.finalize();
    let sig_b64 = engine.encode(signature);
    format!("{header_b64}.{payload_b64}.{sig_b64}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_id_format() {
        let id = generate_pool_id("us-east-1");
        assert!(
            id.starts_with("us-east-1_"),
            "ID should start with region prefix: {id}"
        );
        let suffix = id.strip_prefix("us-east-1_").unwrap();
        assert_eq!(suffix.len(), 9, "Suffix should be 9 chars: {suffix}");
        assert!(
            suffix.chars().all(|c| c.is_alphanumeric()),
            "Suffix should be alphanumeric: {suffix}"
        );
    }

    #[test]
    fn pool_id_format_other_region() {
        let id = generate_pool_id("eu-west-1");
        assert!(id.starts_with("eu-west-1_"));
        let suffix = id.strip_prefix("eu-west-1_").unwrap();
        assert_eq!(suffix.len(), 9);
    }

    #[test]
    fn default_password_policy_values() {
        let policy = PasswordPolicy::default();
        assert_eq!(policy.minimum_length, 8);
        assert!(policy.require_uppercase);
        assert!(policy.require_lowercase);
        assert!(policy.require_numbers);
        assert!(policy.require_symbols);
        assert_eq!(policy.temporary_password_validity_days, 7);
    }

    #[test]
    fn parse_password_policy_from_json() {
        let val = json!({
            "MinimumLength": 12,
            "RequireUppercase": false,
            "RequireLowercase": true,
            "RequireNumbers": true,
            "RequireSymbols": false,
            "TemporaryPasswordValidityDays": 3,
        });
        let policy = parse_password_policy(&val);
        assert_eq!(policy.minimum_length, 12);
        assert!(!policy.require_uppercase);
        assert!(policy.require_lowercase);
        assert!(policy.require_numbers);
        assert!(!policy.require_symbols);
        assert_eq!(policy.temporary_password_validity_days, 3);
    }

    #[test]
    fn parse_password_policy_null_returns_default() {
        let policy = parse_password_policy(&Value::Null);
        assert_eq!(policy.minimum_length, 8);
        assert!(policy.require_uppercase);
    }

    #[test]
    fn default_schema_has_expected_attributes() {
        let attrs = default_schema_attributes();
        let names: Vec<&str> = attrs.iter().map(|a| a.name.as_str()).collect();
        assert!(names.contains(&"sub"));
        assert!(names.contains(&"email"));
        assert!(names.contains(&"phone_number"));
        assert!(names.contains(&"email_verified"));
        assert!(names.contains(&"phone_number_verified"));
        assert!(names.contains(&"updated_at"));
    }

    #[test]
    fn create_user_pool_missing_name() {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(crate::state::CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        let svc = CognitoService::new(state);
        let req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "CreateUserPool".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(r#"{}"#),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        };
        match svc.create_user_pool(&req) {
            Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
            Ok(_) => panic!("Expected InvalidParameterException error"),
        }
    }

    #[test]
    fn client_id_format() {
        let id = generate_client_id();
        assert_eq!(id.len(), 26, "Client ID should be 26 chars: {id}");
        assert!(
            id.chars().all(|c| c.is_ascii_alphanumeric()),
            "Client ID should be alphanumeric: {id}"
        );
        assert!(
            id.chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()),
            "Client ID should be lowercase: {id}"
        );
    }

    #[test]
    fn client_id_uniqueness() {
        let id1 = generate_client_id();
        let id2 = generate_client_id();
        assert_ne!(id1, id2, "Client IDs should be unique");
    }

    #[test]
    fn client_secret_format() {
        let secret = generate_client_secret();
        assert_eq!(
            secret.len(),
            51,
            "Client secret should be 51 chars: {secret}"
        );
    }

    #[test]
    fn client_secret_not_generated_by_default() {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(crate::state::CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        let svc = CognitoService::new(state.clone());

        // First create a pool
        let create_pool_req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "CreateUserPool".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(r#"{"PoolName":"test"}"#),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        };
        let pool_resp = svc.create_user_pool(&create_pool_req).unwrap();
        let pool_json: Value =
            serde_json::from_str(core::str::from_utf8(&pool_resp.body).unwrap()).unwrap();
        let pool_id = pool_json["UserPool"]["Id"].as_str().unwrap();

        // Create client without GenerateSecret
        let req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "CreateUserPoolClient".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({
                    "UserPoolId": pool_id,
                    "ClientName": "test-client"
                }))
                .unwrap(),
            ),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        };
        let resp = svc.create_user_pool_client(&req).unwrap();
        let resp_json: Value =
            serde_json::from_str(core::str::from_utf8(&resp.body).unwrap()).unwrap();
        assert!(resp_json["UserPoolClient"]["ClientSecret"].is_null());
    }

    #[test]
    fn client_secret_generated_when_requested() {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(crate::state::CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        let svc = CognitoService::new(state.clone());

        // Create a pool
        let create_pool_req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "CreateUserPool".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(r#"{"PoolName":"test"}"#),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        };
        let pool_resp = svc.create_user_pool(&create_pool_req).unwrap();
        let pool_json: Value =
            serde_json::from_str(core::str::from_utf8(&pool_resp.body).unwrap()).unwrap();
        let pool_id = pool_json["UserPool"]["Id"].as_str().unwrap();

        // Create client with GenerateSecret=true
        let req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "CreateUserPoolClient".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({
                    "UserPoolId": pool_id,
                    "ClientName": "secret-client",
                    "GenerateSecret": true
                }))
                .unwrap(),
            ),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        };
        let resp = svc.create_user_pool_client(&req).unwrap();
        let resp_json: Value =
            serde_json::from_str(core::str::from_utf8(&resp.body).unwrap()).unwrap();
        let secret = resp_json["UserPoolClient"]["ClientSecret"]
            .as_str()
            .unwrap();
        assert_eq!(secret.len(), 51);
    }

    #[test]
    fn client_belongs_to_correct_pool() {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(crate::state::CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        let svc = CognitoService::new(state.clone());

        // Create two pools
        for name in &["pool-a", "pool-b"] {
            let req = AwsRequest {
                service: "cognito-idp".to_string(),
                action: "CreateUserPool".to_string(),
                region: "us-east-1".to_string(),
                account_id: "123456789012".to_string(),
                request_id: "test".to_string(),
                headers: http::HeaderMap::new(),
                query_params: std::collections::HashMap::new(),
                body: bytes::Bytes::from(
                    serde_json::to_string(&json!({"PoolName": name})).unwrap(),
                ),
                path_segments: vec![],
                raw_path: "/".to_string(),
                raw_query: String::new(),
                method: http::Method::POST,
                is_query_protocol: false,
                access_key_id: None,
            };
            svc.create_user_pool(&req).unwrap();
        }

        let s = state.read();
        let pool_ids: Vec<String> = s.user_pools.keys().cloned().collect();
        drop(s);

        // Create client in pool A
        let req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "CreateUserPoolClient".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({
                    "UserPoolId": pool_ids[0],
                    "ClientName": "client-a"
                }))
                .unwrap(),
            ),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        };
        let resp = svc.create_user_pool_client(&req).unwrap();
        let resp_json: Value =
            serde_json::from_str(core::str::from_utf8(&resp.body).unwrap()).unwrap();
        let client_id = resp_json["UserPoolClient"]["ClientId"]
            .as_str()
            .unwrap()
            .to_string();

        // Describe client with pool B should fail
        let req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "DescribeUserPoolClient".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({
                    "UserPoolId": pool_ids[1],
                    "ClientId": client_id
                }))
                .unwrap(),
            ),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        };
        match svc.describe_user_pool_client(&req) {
            Err(e) => assert_eq!(e.code(), "ResourceNotFoundException"),
            Ok(_) => panic!("Expected ResourceNotFoundException"),
        }
    }

    #[test]
    fn parse_user_attributes_from_json() {
        let val = json!([
            { "Name": "email", "Value": "test@example.com" },
            { "Name": "name", "Value": "Test User" }
        ]);
        let attrs = parse_user_attributes(&val);
        assert_eq!(attrs.len(), 2);
        assert_eq!(attrs[0].name, "email");
        assert_eq!(attrs[0].value, "test@example.com");
        assert_eq!(attrs[1].name, "name");
        assert_eq!(attrs[1].value, "Test User");
    }

    #[test]
    fn parse_user_attributes_null() {
        let attrs = parse_user_attributes(&Value::Null);
        assert!(attrs.is_empty());
    }

    #[test]
    fn parse_filter_expression_equals() {
        let filter = parse_filter_expression(r#"email = "test@example.com""#).unwrap();
        assert_eq!(filter.attribute, "email");
        assert_eq!(filter.value, "test@example.com");
        assert!(matches!(filter.operator, FilterOp::Equals));
    }

    #[test]
    fn parse_filter_expression_starts_with() {
        let filter = parse_filter_expression(r#"email ^= "test""#).unwrap();
        assert_eq!(filter.attribute, "email");
        assert_eq!(filter.value, "test");
        assert!(matches!(filter.operator, FilterOp::StartsWith));
    }

    #[test]
    fn filter_matches_username() {
        let user = User {
            username: "testuser".to_string(),
            sub: Uuid::new_v4().to_string(),
            attributes: vec![],
            enabled: true,
            user_status: "CONFIRMED".to_string(),
            user_create_date: Utc::now(),
            user_last_modified_date: Utc::now(),
            password: None,
            temporary_password: None,
        };

        let filter = parse_filter_expression(r#"username = "testuser""#).unwrap();
        assert!(matches_filter(&user, &filter));

        let filter = parse_filter_expression(r#"username = "other""#).unwrap();
        assert!(!matches_filter(&user, &filter));

        let filter = parse_filter_expression(r#"username ^= "test""#).unwrap();
        assert!(matches_filter(&user, &filter));
    }

    #[test]
    fn filter_matches_attribute() {
        let user = User {
            username: "testuser".to_string(),
            sub: Uuid::new_v4().to_string(),
            attributes: vec![UserAttribute {
                name: "email".to_string(),
                value: "test@example.com".to_string(),
            }],
            enabled: true,
            user_status: "CONFIRMED".to_string(),
            user_create_date: Utc::now(),
            user_last_modified_date: Utc::now(),
            password: None,
            temporary_password: None,
        };

        let filter = parse_filter_expression(r#"email = "test@example.com""#).unwrap();
        assert!(matches_filter(&user, &filter));

        let filter = parse_filter_expression(r#"email ^= "test@""#).unwrap();
        assert!(matches_filter(&user, &filter));

        let filter = parse_filter_expression(r#"email = "other@example.com""#).unwrap();
        assert!(!matches_filter(&user, &filter));
    }

    #[test]
    fn filter_matches_user_status() {
        let user = User {
            username: "testuser".to_string(),
            sub: Uuid::new_v4().to_string(),
            attributes: vec![],
            enabled: true,
            user_status: "FORCE_CHANGE_PASSWORD".to_string(),
            user_create_date: Utc::now(),
            user_last_modified_date: Utc::now(),
            password: None,
            temporary_password: None,
        };

        let filter =
            parse_filter_expression(r#"cognito:user_status = "FORCE_CHANGE_PASSWORD""#).unwrap();
        assert!(matches_filter(&user, &filter));

        let filter = parse_filter_expression(r#"status = "FORCE_CHANGE_PASSWORD""#).unwrap();
        assert!(matches_filter(&user, &filter));
    }

    #[test]
    fn user_default_status_is_force_change_password() {
        // When a user is admin-created, the status should be FORCE_CHANGE_PASSWORD
        let state = std::sync::Arc::new(parking_lot::RwLock::new(crate::state::CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        let svc = CognitoService::new(state.clone());

        // Create a pool
        let req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "CreateUserPool".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(r#"{"PoolName":"test"}"#),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        };
        let pool_resp = svc.create_user_pool(&req).unwrap();
        let pool_json: Value =
            serde_json::from_str(core::str::from_utf8(&pool_resp.body).unwrap()).unwrap();
        let pool_id = pool_json["UserPool"]["Id"].as_str().unwrap();

        // Admin create user
        let req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "AdminCreateUser".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({
                    "UserPoolId": pool_id,
                    "Username": "testuser"
                }))
                .unwrap(),
            ),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        };
        let resp = svc.admin_create_user(&req).unwrap();
        let resp_json: Value =
            serde_json::from_str(core::str::from_utf8(&resp.body).unwrap()).unwrap();

        assert_eq!(
            resp_json["User"]["UserStatus"].as_str().unwrap(),
            "FORCE_CHANGE_PASSWORD"
        );
        assert!(resp_json["User"]["Enabled"].as_bool().unwrap());

        // Verify sub is in attributes
        let attrs = resp_json["User"]["Attributes"].as_array().unwrap();
        let sub_attr = attrs.iter().find(|a| a["Name"] == "sub").unwrap();
        assert!(!sub_attr["Value"].as_str().unwrap().is_empty());
    }

    #[test]
    fn jwt_format_three_base64url_segments() {
        let tokens = generate_tokens(
            "us-east-1_abc123456",
            "client123",
            "sub-uuid",
            "user1",
            "us-east-1",
        );
        // Each token should have 3 dot-separated segments
        for (name, token) in [("id", &tokens.id_token), ("access", &tokens.access_token)] {
            let parts: Vec<&str> = token.split('.').collect();
            assert_eq!(
                parts.len(),
                3,
                "{name} token should have 3 segments, got {}",
                parts.len()
            );
            // Each segment should be valid base64url (no padding, no + or /)
            for (i, part) in parts.iter().enumerate() {
                assert!(
                    !part.is_empty(),
                    "{name} token segment {i} should not be empty"
                );
                assert!(
                    !part.contains('+'),
                    "{name} token segment {i} should not contain '+'"
                );
                assert!(
                    !part.contains('/'),
                    "{name} token segment {i} should not contain '/'"
                );
                assert!(
                    !part.contains('='),
                    "{name} token segment {i} should not contain '='"
                );
            }
        }
        // Refresh token is just a random base64url string (no dots)
        assert!(
            !tokens.refresh_token.is_empty(),
            "refresh token should not be empty"
        );
        assert!(
            tokens.refresh_token.len() >= 96,
            "refresh token should be at least 96 chars, got {}",
            tokens.refresh_token.len()
        );
    }

    #[test]
    fn jwt_id_token_payload_contains_required_fields() {
        let b64url = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let tokens = generate_tokens(
            "us-east-1_abc123456",
            "client123",
            "sub-uuid",
            "user1",
            "us-east-1",
        );
        let parts: Vec<&str> = tokens.id_token.split('.').collect();
        let header: Value = serde_json::from_slice(&b64url.decode(parts[0]).unwrap()).unwrap();
        let payload: Value = serde_json::from_slice(&b64url.decode(parts[1]).unwrap()).unwrap();

        assert_eq!(header["alg"], "RS256");
        assert_eq!(header["kid"], "fakecloud-key-1");
        assert_eq!(payload["sub"], "sub-uuid");
        assert_eq!(payload["aud"], "client123");
        assert_eq!(payload["cognito:username"], "user1");
        assert_eq!(payload["token_use"], "id");
        assert!(payload["iss"]
            .as_str()
            .unwrap()
            .contains("us-east-1_abc123456"));
        assert!(payload["exp"].is_number());
        assert!(payload["iat"].is_number());
        assert!(payload["jti"].is_string());
    }

    #[test]
    fn jwt_access_token_payload_contains_required_fields() {
        let b64url = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let tokens = generate_tokens(
            "us-east-1_abc123456",
            "client123",
            "sub-uuid",
            "user1",
            "us-east-1",
        );
        let parts: Vec<&str> = tokens.access_token.split('.').collect();
        let payload: Value = serde_json::from_slice(&b64url.decode(parts[1]).unwrap()).unwrap();

        assert_eq!(payload["sub"], "sub-uuid");
        assert_eq!(payload["client_id"], "client123");
        assert_eq!(payload["token_use"], "access");
        assert_eq!(payload["scope"], "aws.cognito.signin.user.admin");
        assert!(payload["exp"].is_number());
        assert!(payload["iat"].is_number());
    }

    #[test]
    fn password_policy_rejects_short_password() {
        let policy = PasswordPolicy {
            minimum_length: 8,
            require_uppercase: false,
            require_lowercase: false,
            require_numbers: false,
            require_symbols: false,
            temporary_password_validity_days: 7,
        };
        let err = validate_password("short", &policy).unwrap_err();
        assert_eq!(err.code(), "InvalidPasswordException");
    }

    #[test]
    fn password_policy_rejects_missing_uppercase() {
        let policy = PasswordPolicy {
            minimum_length: 1,
            require_uppercase: true,
            require_lowercase: false,
            require_numbers: false,
            require_symbols: false,
            temporary_password_validity_days: 7,
        };
        let err = validate_password("lowercase", &policy).unwrap_err();
        assert_eq!(err.code(), "InvalidPasswordException");
        assert!(validate_password("Uppercase", &policy).is_ok());
    }

    #[test]
    fn password_policy_rejects_missing_numbers() {
        let policy = PasswordPolicy {
            minimum_length: 1,
            require_uppercase: false,
            require_lowercase: false,
            require_numbers: true,
            require_symbols: false,
            temporary_password_validity_days: 7,
        };
        let err = validate_password("nodigits", &policy).unwrap_err();
        assert_eq!(err.code(), "InvalidPasswordException");
        assert!(validate_password("has1digit", &policy).is_ok());
    }

    #[test]
    fn password_policy_rejects_missing_symbols() {
        let policy = PasswordPolicy {
            minimum_length: 1,
            require_uppercase: false,
            require_lowercase: false,
            require_numbers: false,
            require_symbols: true,
            temporary_password_validity_days: 7,
        };
        let err = validate_password("nosymbols", &policy).unwrap_err();
        assert_eq!(err.code(), "InvalidPasswordException");
        assert!(validate_password("has!symbol", &policy).is_ok());
    }

    #[test]
    fn session_token_is_uuid_format() {
        let session = Uuid::new_v4().to_string();
        // UUID v4 format: 8-4-4-4-12 hex chars
        assert_eq!(session.len(), 36);
        let parts: Vec<&str> = session.split('-').collect();
        assert_eq!(parts.len(), 5);
    }
}
