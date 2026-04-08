use std::collections::HashMap;

use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{
    default_schema_attributes, AccessTokenData, AccountRecoverySetting, AdminCreateUserConfig,
    AuthEvent, CustomDomainConfig, Device, EmailConfiguration, Group, IdentityProvider,
    InviteMessageTemplate, MfaPreferences, PasswordPolicy, PoolPolicies, RecoveryOption,
    RefreshTokenData, ResourceServer, ResourceServerScope, SchemaAttribute, SessionData,
    SharedCognitoState, SmsConfiguration, SmsMfaConfiguration, SoftwareTokenMfaConfiguration,
    StringAttributeConstraints, TokenValidityUnits, User, UserAttribute, UserImportJob, UserPool,
    UserPoolClient, UserPoolDomain,
};
use crate::triggers::{self, CognitoDeliveryContext, TriggerSource};

pub struct CognitoService {
    state: SharedCognitoState,
    delivery_ctx: Option<CognitoDeliveryContext>,
}

impl CognitoService {
    pub fn new(state: SharedCognitoState) -> Self {
        Self {
            state,
            delivery_ctx: None,
        }
    }

    /// Attach a delivery context for Lambda trigger invocation.
    pub fn with_delivery(mut self, ctx: CognitoDeliveryContext) -> Self {
        self.delivery_ctx = Some(ctx);
        self
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
            "AdminCreateUser" => self.admin_create_user(&req).await,
            "AdminGetUser" => self.admin_get_user(&req),
            "AdminDeleteUser" => self.admin_delete_user(&req),
            "AdminDisableUser" => self.admin_disable_user(&req),
            "AdminEnableUser" => self.admin_enable_user(&req),
            "AdminUpdateUserAttributes" => self.admin_update_user_attributes(&req),
            "AdminDeleteUserAttributes" => self.admin_delete_user_attributes(&req),
            "ListUsers" => self.list_users(&req),
            "AdminSetUserPassword" => self.admin_set_user_password(&req),
            "AdminInitiateAuth" => self.admin_initiate_auth(&req).await,
            "InitiateAuth" => self.initiate_auth(&req).await,
            "RespondToAuthChallenge" => self.respond_to_auth_challenge(&req),
            "AdminRespondToAuthChallenge" => self.admin_respond_to_auth_challenge(&req),
            "SignUp" => self.sign_up(&req).await,
            "ConfirmSignUp" => self.confirm_sign_up(&req).await,
            "AdminConfirmSignUp" => self.admin_confirm_sign_up(&req).await,
            "ChangePassword" => self.change_password(&req),
            "ForgotPassword" => self.forgot_password(&req).await,
            "ConfirmForgotPassword" => self.confirm_forgot_password(&req),
            "AdminResetUserPassword" => self.admin_reset_user_password(&req),
            "GlobalSignOut" => self.global_sign_out(&req),
            "AdminUserGlobalSignOut" => self.admin_user_global_sign_out(&req),
            "CreateGroup" => self.create_group(&req),
            "DeleteGroup" => self.delete_group(&req),
            "GetGroup" => self.get_group(&req),
            "UpdateGroup" => self.update_group(&req),
            "ListGroups" => self.list_groups(&req),
            "AdminAddUserToGroup" => self.admin_add_user_to_group(&req),
            "AdminRemoveUserFromGroup" => self.admin_remove_user_from_group(&req),
            "AdminListGroupsForUser" => self.admin_list_groups_for_user(&req),
            "ListUsersInGroup" => self.list_users_in_group(&req),
            "GetUser" => self.get_user(&req),
            "DeleteUser" => self.delete_user(&req),
            "UpdateUserAttributes" => self.update_user_attributes(&req),
            "DeleteUserAttributes" => self.delete_user_attributes(&req),
            "GetUserAttributeVerificationCode" => self.get_user_attribute_verification_code(&req),
            "VerifyUserAttribute" => self.verify_user_attribute(&req),
            "ResendConfirmationCode" => self.resend_confirmation_code(&req),
            "SetUserPoolMfaConfig" => self.set_user_pool_mfa_config(&req),
            "GetUserPoolMfaConfig" => self.get_user_pool_mfa_config(&req),
            "AdminSetUserMFAPreference" => self.admin_set_user_mfa_preference(&req),
            "SetUserMFAPreference" => self.set_user_mfa_preference(&req),
            "AssociateSoftwareToken" => self.associate_software_token(&req),
            "VerifySoftwareToken" => self.verify_software_token(&req),
            "CreateIdentityProvider" => self.create_identity_provider(&req),
            "DescribeIdentityProvider" => self.describe_identity_provider(&req),
            "UpdateIdentityProvider" => self.update_identity_provider(&req),
            "DeleteIdentityProvider" => self.delete_identity_provider(&req),
            "ListIdentityProviders" => self.list_identity_providers(&req),
            "CreateResourceServer" => self.create_resource_server(&req),
            "DescribeResourceServer" => self.describe_resource_server(&req),
            "UpdateResourceServer" => self.update_resource_server(&req),
            "DeleteResourceServer" => self.delete_resource_server(&req),
            "ListResourceServers" => self.list_resource_servers(&req),
            "CreateUserPoolDomain" => self.create_user_pool_domain(&req),
            "DescribeUserPoolDomain" => self.describe_user_pool_domain(&req),
            "UpdateUserPoolDomain" => self.update_user_pool_domain(&req),
            "DeleteUserPoolDomain" => self.delete_user_pool_domain(&req),
            "AdminGetDevice" => self.admin_get_device(&req),
            "AdminListDevices" => self.admin_list_devices(&req),
            "AdminForgetDevice" => self.admin_forget_device(&req),
            "AdminUpdateDeviceStatus" => self.admin_update_device_status(&req),
            "ConfirmDevice" => self.confirm_device(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "GetCSVHeader" => self.get_csv_header(&req),
            "CreateUserImportJob" => self.create_user_import_job(&req),
            "DescribeUserImportJob" => self.describe_user_import_job(&req),
            "ListUserImportJobs" => self.list_user_import_jobs(&req),
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
            "ChangePassword",
            "ForgotPassword",
            "ConfirmForgotPassword",
            "AdminResetUserPassword",
            "GlobalSignOut",
            "AdminUserGlobalSignOut",
            "CreateGroup",
            "DeleteGroup",
            "GetGroup",
            "UpdateGroup",
            "ListGroups",
            "AdminAddUserToGroup",
            "AdminRemoveUserFromGroup",
            "AdminListGroupsForUser",
            "ListUsersInGroup",
            "GetUser",
            "DeleteUser",
            "UpdateUserAttributes",
            "DeleteUserAttributes",
            "GetUserAttributeVerificationCode",
            "VerifyUserAttribute",
            "ResendConfirmationCode",
            "SetUserPoolMfaConfig",
            "GetUserPoolMfaConfig",
            "AdminSetUserMFAPreference",
            "SetUserMFAPreference",
            "AssociateSoftwareToken",
            "VerifySoftwareToken",
            "CreateIdentityProvider",
            "DescribeIdentityProvider",
            "UpdateIdentityProvider",
            "DeleteIdentityProvider",
            "ListIdentityProviders",
            "CreateResourceServer",
            "DescribeResourceServer",
            "UpdateResourceServer",
            "DeleteResourceServer",
            "ListResourceServers",
            "CreateUserPoolDomain",
            "DescribeUserPoolDomain",
            "UpdateUserPoolDomain",
            "DeleteUserPoolDomain",
            "AdminGetDevice",
            "AdminListDevices",
            "AdminForgetDevice",
            "AdminUpdateDeviceStatus",
            "ConfirmDevice",
            "TagResource",
            "UntagResource",
            "ListTagsForResource",
            "GetCSVHeader",
            "CreateUserImportJob",
            "DescribeUserImportJob",
            "ListUserImportJobs",
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
            software_token_mfa_configuration: None,
            sms_mfa_configuration: None,
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

    async fn admin_create_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

            let user = User {
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

        // Clean up group memberships for the deleted user
        if let Some(pool_groups) = state.user_groups.get_mut(pool_id) {
            pool_groups.remove(username);
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

    async fn admin_initiate_auth(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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
                    event_type: "SIGN_IN_FAILURE".to_string(),
                    username: username.to_string(),
                    user_pool_id: pool_id.to_string(),
                    client_id: Some(client_id.to_string()),
                    timestamp: Utc::now(),
                    success: false,
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
                event_type: "SIGN_IN".to_string(),
                username: username.to_string(),
                user_pool_id: pool_id.to_string(),
                client_id: Some(client_id.to_string()),
                timestamp: Utc::now(),
                success: true,
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

    async fn initiate_auth(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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
                            event_type: "SIGN_IN_FAILURE".to_string(),
                            username: username.to_string(),
                            user_pool_id: pool_id.to_string(),
                            client_id: Some(client_id.to_string()),
                            timestamp: Utc::now(),
                            success: false,
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
                        event_type: "SIGN_IN".to_string(),
                        username: username.to_string(),
                        user_pool_id: pool_id.to_string(),
                        client_id: Some(client_id.to_string()),
                        timestamp: Utc::now(),
                        success: true,
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
            _ => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                format!("Unsupported challenge: {challenge_name}"),
            )),
        }
    }

    async fn sign_up(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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
                confirmation_code: None,
                attribute_verification_codes: HashMap::new(),
                mfa_preferences: None,
                totp_secret: None,
                totp_verified: false,
                devices: HashMap::new(),
            };

            pool_users.insert(username.to_string(), user.clone());

            let region = state.region.clone();
            let account_id = state.account_id.clone();

            state.auth_events.push(AuthEvent {
                event_type: "SIGN_UP".to_string(),
                username: username.to_string(),
                user_pool_id: pool_id.clone(),
                client_id: Some(client_id.to_string()),
                timestamp: Utc::now(),
                success: true,
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

    async fn confirm_sign_up(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    async fn admin_confirm_sign_up(
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

    fn change_password(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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
            event_type: "PASSWORD_CHANGE".to_string(),
            username,
            user_pool_id: pool_id,
            client_id: None,
            timestamp: Utc::now(),
            success: true,
        });

        Ok(AwsResponse::ok_json(json!({})))
    }

    async fn forgot_password(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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
            event_type: "FORGOT_PASSWORD".to_string(),
            username: username.to_string(),
            user_pool_id: pool_id.clone(),
            client_id: Some(client_id.to_string()),
            timestamp: Utc::now(),
            success: true,
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

    fn confirm_forgot_password(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn admin_reset_user_password(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn global_sign_out(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn admin_user_global_sign_out(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    // ── Group management ────────────────────────────────────────────────

    fn create_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn delete_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn get_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn update_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn list_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn admin_add_user_to_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn admin_remove_user_from_group(
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

    fn admin_list_groups_for_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn list_users_in_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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
        let mut users_in_group: Vec<&User> = usernames_in_group
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

    // ── Self-service user operations ───────────────────────────────────

    fn get_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn delete_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn update_user_attributes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn delete_user_attributes(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn get_user_attribute_verification_code(
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

    fn verify_user_attribute(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn resend_confirmation_code(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    fn set_user_pool_mfa_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;

        let mut state = self.state.write();

        let pool = state.user_pools.get_mut(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        if let Some(mfa_config) = body["MfaConfiguration"].as_str() {
            pool.mfa_configuration = mfa_config.to_string();
        }

        if !body["SoftwareTokenMfaConfiguration"].is_null() {
            let enabled = body["SoftwareTokenMfaConfiguration"]["Enabled"]
                .as_bool()
                .unwrap_or(false);
            pool.software_token_mfa_configuration = Some(SoftwareTokenMfaConfiguration { enabled });
        }

        if !body["SmsMfaConfiguration"].is_null() {
            let enabled = body["SmsMfaConfiguration"]["Enabled"]
                .as_bool()
                .unwrap_or(false);
            let sms_configuration = if !body["SmsMfaConfiguration"]["SmsConfiguration"].is_null() {
                Some(SmsConfiguration {
                    sns_caller_arn: body["SmsMfaConfiguration"]["SmsConfiguration"]["SnsCallerArn"]
                        .as_str()
                        .map(|s| s.to_string()),
                    external_id: body["SmsMfaConfiguration"]["SmsConfiguration"]["ExternalId"]
                        .as_str()
                        .map(|s| s.to_string()),
                    sns_region: body["SmsMfaConfiguration"]["SmsConfiguration"]["SnsRegion"]
                        .as_str()
                        .map(|s| s.to_string()),
                })
            } else {
                None
            };
            pool.sms_mfa_configuration = Some(SmsMfaConfiguration {
                enabled,
                sms_configuration,
            });
        }

        pool.last_modified_date = Utc::now();

        let mut response = json!({
            "MfaConfiguration": pool.mfa_configuration,
        });

        if let Some(ref stmc) = pool.software_token_mfa_configuration {
            response["SoftwareTokenMfaConfiguration"] = json!({
                "Enabled": stmc.enabled,
            });
        }

        if let Some(ref smc) = pool.sms_mfa_configuration {
            let mut sms_json = json!({ "Enabled": smc.enabled });
            if let Some(ref sc) = smc.sms_configuration {
                let mut sc_json = json!({});
                if let Some(ref arn) = sc.sns_caller_arn {
                    sc_json["SnsCallerArn"] = json!(arn);
                }
                if let Some(ref eid) = sc.external_id {
                    sc_json["ExternalId"] = json!(eid);
                }
                if let Some(ref r) = sc.sns_region {
                    sc_json["SnsRegion"] = json!(r);
                }
                sms_json["SmsConfiguration"] = sc_json;
            }
            response["SmsMfaConfiguration"] = sms_json;
        }

        Ok(AwsResponse::ok_json(response))
    }

    fn get_user_pool_mfa_config(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;

        let state = self.state.read();

        let pool = state.user_pools.get(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        let mut response = json!({
            "MfaConfiguration": pool.mfa_configuration,
        });

        if let Some(ref stmc) = pool.software_token_mfa_configuration {
            response["SoftwareTokenMfaConfiguration"] = json!({
                "Enabled": stmc.enabled,
            });
        }

        if let Some(ref smc) = pool.sms_mfa_configuration {
            let mut sms_json = json!({ "Enabled": smc.enabled });
            if let Some(ref sc) = smc.sms_configuration {
                let mut sc_json = json!({});
                if let Some(ref arn) = sc.sns_caller_arn {
                    sc_json["SnsCallerArn"] = json!(arn);
                }
                if let Some(ref eid) = sc.external_id {
                    sc_json["ExternalId"] = json!(eid);
                }
                if let Some(ref r) = sc.sns_region {
                    sc_json["SnsRegion"] = json!(r);
                }
                sms_json["SmsConfiguration"] = sc_json;
            }
            response["SmsMfaConfiguration"] = sms_json;
        }

        Ok(AwsResponse::ok_json(response))
    }

    fn admin_set_user_mfa_preference(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let username = require_str(&body, "Username")?;

        let mut state = self.state.write();

        // Verify pool exists
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

        let sms_enabled = body["SMSMfaSettings"]["Enabled"].as_bool().unwrap_or(false);
        let sms_preferred = body["SMSMfaSettings"]["PreferredMfa"]
            .as_bool()
            .unwrap_or(false);
        let software_token_enabled = body["SoftwareTokenMfaSettings"]["Enabled"]
            .as_bool()
            .unwrap_or(false);
        let software_token_preferred = body["SoftwareTokenMfaSettings"]["PreferredMfa"]
            .as_bool()
            .unwrap_or(false);

        user.mfa_preferences = Some(MfaPreferences {
            sms_enabled,
            sms_preferred,
            software_token_enabled,
            software_token_preferred,
        });
        user.user_last_modified_date = Utc::now();

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn set_user_mfa_preference(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

        let sms_enabled = body["SMSMfaSettings"]["Enabled"].as_bool().unwrap_or(false);
        let sms_preferred = body["SMSMfaSettings"]["PreferredMfa"]
            .as_bool()
            .unwrap_or(false);
        let software_token_enabled = body["SoftwareTokenMfaSettings"]["Enabled"]
            .as_bool()
            .unwrap_or(false);
        let software_token_preferred = body["SoftwareTokenMfaSettings"]["PreferredMfa"]
            .as_bool()
            .unwrap_or(false);

        user.mfa_preferences = Some(MfaPreferences {
            sms_enabled,
            sms_preferred,
            software_token_enabled,
            software_token_preferred,
        });
        user.user_last_modified_date = Utc::now();

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn associate_software_token(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let mut state = self.state.write();

        // Identify user from access token or session
        let (pool_id, username) = if let Some(access_token) = body["AccessToken"].as_str() {
            let token_data = state.access_tokens.get(access_token).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotAuthorizedException",
                    "Invalid access token.",
                )
            })?;
            (token_data.user_pool_id.clone(), token_data.username.clone())
        } else if let Some(session) = body["Session"].as_str() {
            let session_data = state.sessions.get(session).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotAuthorizedException",
                    "Invalid session.",
                )
            })?;
            (
                session_data.user_pool_id.clone(),
                session_data.username.clone(),
            )
        } else {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "AccessToken or Session is required",
            ));
        };

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

        let secret = generate_totp_secret();
        user.totp_secret = Some(secret.clone());
        user.totp_verified = false;
        user.user_last_modified_date = Utc::now();

        let new_session = Uuid::new_v4().to_string();

        Ok(AwsResponse::ok_json(json!({
            "SecretCode": secret,
            "Session": new_session,
        })))
    }

    fn verify_software_token(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let user_code = require_str(&body, "UserCode")?;

        // Validate it's a 6-digit code
        if user_code.len() != 6 || !user_code.chars().all(|c| c.is_ascii_digit()) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "EnableSoftwareTokenMFAException",
                "Invalid user code.",
            ));
        }

        let mut state = self.state.write();

        // Identify user from access token or session
        let (pool_id, username) = if let Some(access_token) = body["AccessToken"].as_str() {
            let token_data = state.access_tokens.get(access_token).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotAuthorizedException",
                    "Invalid access token.",
                )
            })?;
            (token_data.user_pool_id.clone(), token_data.username.clone())
        } else if let Some(session) = body["Session"].as_str() {
            let session_data = state.sessions.get(session).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotAuthorizedException",
                    "Invalid session.",
                )
            })?;
            (
                session_data.user_pool_id.clone(),
                session_data.username.clone(),
            )
        } else {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "AccessToken or Session is required",
            ));
        };

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

        if user.totp_secret.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "EnableSoftwareTokenMFAException",
                "Software token MFA has not been associated.",
            ));
        }

        // For local emulator: accept any valid 6-digit code
        user.totp_verified = true;
        user.user_last_modified_date = Utc::now();

        Ok(AwsResponse::ok_json(json!({
            "Status": "SUCCESS",
        })))
    }

    // ── Identity Provider operations ──────────────────────────────────

    fn create_identity_provider(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let provider_name = require_str(&body, "ProviderName")?;
        let provider_type = require_str(&body, "ProviderType")?;

        validate_provider_type(provider_type)?;

        let provider_details = parse_string_map(&body["ProviderDetails"]);
        let attribute_mapping = parse_string_map(&body["AttributeMapping"]);
        let idp_identifiers = body["IdpIdentifiers"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let pool_providers = state
            .identity_providers
            .entry(pool_id.to_string())
            .or_default();
        if pool_providers.contains_key(provider_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DuplicateProviderException",
                format!(
                    "A provider with the name {provider_name} already exists in this user pool."
                ),
            ));
        }

        let now = Utc::now();
        let idp = IdentityProvider {
            user_pool_id: pool_id.to_string(),
            provider_name: provider_name.to_string(),
            provider_type: provider_type.to_string(),
            provider_details,
            attribute_mapping,
            idp_identifiers,
            creation_date: now,
            last_modified_date: now,
        };

        pool_providers.insert(provider_name.to_string(), idp.clone());

        Ok(AwsResponse::ok_json(json!({
            "IdentityProvider": identity_provider_to_json(&idp)
        })))
    }

    fn describe_identity_provider(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let provider_name = require_str(&body, "ProviderName")?;

        let state = self.state.read();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let idp = state
            .identity_providers
            .get(pool_id)
            .and_then(|providers| providers.get(provider_name))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Identity provider {provider_name} does not exist."),
                )
            })?;

        Ok(AwsResponse::ok_json(json!({
            "IdentityProvider": identity_provider_to_json(idp)
        })))
    }

    fn update_identity_provider(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let provider_name = require_str(&body, "ProviderName")?;

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let idp = state
            .identity_providers
            .get_mut(pool_id)
            .and_then(|providers| providers.get_mut(provider_name))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Identity provider {provider_name} does not exist."),
                )
            })?;

        if body["ProviderDetails"].is_object() {
            idp.provider_details = parse_string_map(&body["ProviderDetails"]);
        }
        if body["AttributeMapping"].is_object() {
            idp.attribute_mapping = parse_string_map(&body["AttributeMapping"]);
        }
        if let Some(arr) = body["IdpIdentifiers"].as_array() {
            idp.idp_identifiers = arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
        }
        idp.last_modified_date = Utc::now();

        let idp = idp.clone();

        Ok(AwsResponse::ok_json(json!({
            "IdentityProvider": identity_provider_to_json(&idp)
        })))
    }

    fn delete_identity_provider(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let provider_name = require_str(&body, "ProviderName")?;

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let removed = state
            .identity_providers
            .get_mut(pool_id)
            .and_then(|providers| providers.remove(provider_name));

        if removed.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Identity provider {provider_name} does not exist."),
            ));
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_identity_providers(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(60).clamp(1, 60) as usize;
        let next_token = body["NextToken"].as_str();

        let state = self.state.read();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let empty = HashMap::new();
        let pool_providers = state.identity_providers.get(pool_id).unwrap_or(&empty);

        let mut providers: Vec<&IdentityProvider> = pool_providers.values().collect();
        providers.sort_by_key(|p| p.creation_date);

        let start_idx = if let Some(token) = next_token {
            providers
                .iter()
                .position(|p| p.provider_name == token)
                .unwrap_or(0)
        } else {
            0
        };

        let page: Vec<Value> = providers
            .iter()
            .skip(start_idx)
            .take(max_results)
            .map(|p| {
                json!({
                    "ProviderName": p.provider_name,
                    "ProviderType": p.provider_type,
                    "CreationDate": p.creation_date.timestamp() as f64,
                    "LastModifiedDate": p.last_modified_date.timestamp() as f64,
                })
            })
            .collect();

        let has_more = start_idx + max_results < providers.len();
        let mut response = json!({ "Providers": page });
        if has_more {
            if let Some(last) = providers.get(start_idx + max_results) {
                response["NextToken"] = json!(last.provider_name);
            }
        }

        Ok(AwsResponse::ok_json(response))
    }

    // ── Resource Servers ──────────────────────────────────────────────

    fn create_resource_server(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let identifier = require_str(&body, "Identifier")?;
        let name = require_str(&body, "Name")?;

        let scopes = parse_resource_server_scopes(&body["Scopes"]);

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let pool_servers = state
            .resource_servers
            .entry(pool_id.to_string())
            .or_default();
        if pool_servers.contains_key(identifier) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                format!(
                    "A resource server with identifier {identifier} already exists in this user pool."
                ),
            ));
        }

        let rs = ResourceServer {
            user_pool_id: pool_id.to_string(),
            identifier: identifier.to_string(),
            name: name.to_string(),
            scopes,
        };

        pool_servers.insert(identifier.to_string(), rs.clone());

        Ok(AwsResponse::ok_json(json!({
            "ResourceServer": resource_server_to_json(&rs)
        })))
    }

    fn describe_resource_server(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let identifier = require_str(&body, "Identifier")?;

        let state = self.state.read();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let rs = state
            .resource_servers
            .get(pool_id)
            .and_then(|m| m.get(identifier))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Resource server {identifier} does not exist."),
                )
            })?;

        Ok(AwsResponse::ok_json(json!({
            "ResourceServer": resource_server_to_json(rs)
        })))
    }

    fn update_resource_server(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let identifier = require_str(&body, "Identifier")?;
        let name = require_str(&body, "Name")?;
        let scopes = parse_resource_server_scopes(&body["Scopes"]);

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let rs = state
            .resource_servers
            .get_mut(pool_id)
            .and_then(|m| m.get_mut(identifier))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Resource server {identifier} does not exist."),
                )
            })?;

        rs.name = name.to_string();
        rs.scopes = scopes;

        Ok(AwsResponse::ok_json(json!({
            "ResourceServer": resource_server_to_json(rs)
        })))
    }

    fn delete_resource_server(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let identifier = require_str(&body, "Identifier")?;

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let removed = state
            .resource_servers
            .get_mut(pool_id)
            .and_then(|m| m.remove(identifier));

        if removed.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Resource server {identifier} does not exist."),
            ));
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_resource_servers(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(50).clamp(1, 50) as usize;
        let next_token = body["NextToken"].as_str();

        let state = self.state.read();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let empty = HashMap::new();
        let pool_servers = state.resource_servers.get(pool_id).unwrap_or(&empty);

        let mut servers: Vec<&ResourceServer> = pool_servers.values().collect();
        servers.sort_by_key(|s| &s.identifier);

        let start_idx = if let Some(token) = next_token {
            servers
                .iter()
                .position(|s| s.identifier == token)
                .unwrap_or(0)
        } else {
            0
        };

        let page: Vec<Value> = servers
            .iter()
            .skip(start_idx)
            .take(max_results)
            .map(|s| resource_server_to_json(s))
            .collect();

        let has_more = start_idx + max_results < servers.len();
        let mut response = json!({ "ResourceServers": page });
        if has_more {
            if let Some(last) = servers.get(start_idx + max_results) {
                response["NextToken"] = json!(last.identifier);
            }
        }

        Ok(AwsResponse::ok_json(response))
    }

    // ── Domains ───────────────────────────────────────────────────────

    fn create_user_pool_domain(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let domain = require_str(&body, "Domain")?;

        let custom_domain_config =
            body["CustomDomainConfig"]["CertificateArn"]
                .as_str()
                .map(|arn| CustomDomainConfig {
                    certificate_arn: arn.to_string(),
                });

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        if state.domains.contains_key(domain) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                format!("Domain {domain} is already associated with a user pool."),
            ));
        }

        let domain_obj = UserPoolDomain {
            user_pool_id: pool_id.to_string(),
            domain: domain.to_string(),
            status: "ACTIVE".to_string(),
            custom_domain_config,
            creation_date: Utc::now(),
        };

        state.domains.insert(domain.to_string(), domain_obj);

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn describe_user_pool_domain(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let domain = require_str(&body, "Domain")?;

        let state = self.state.read();

        // AWS returns empty DomainDescription if not found (no error)
        let description = match state.domains.get(domain) {
            Some(d) => domain_description_to_json(d, &state.account_id),
            None => json!({}),
        };

        Ok(AwsResponse::ok_json(json!({
            "DomainDescription": description
        })))
    }

    fn update_user_pool_domain(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let domain = require_str(&body, "Domain")?;

        let custom_domain_config =
            body["CustomDomainConfig"]["CertificateArn"]
                .as_str()
                .map(|arn| CustomDomainConfig {
                    certificate_arn: arn.to_string(),
                });

        let mut state = self.state.write();

        let d = state.domains.get_mut(domain).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Domain {domain} does not exist."),
            )
        })?;

        if d.user_pool_id != pool_id {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Domain {domain} does not exist."),
            ));
        }

        d.custom_domain_config = custom_domain_config;

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn delete_user_pool_domain(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let pool_id = require_str(&body, "UserPoolId")?;
        let domain = require_str(&body, "Domain")?;

        let mut state = self.state.write();

        match state.domains.get(domain) {
            Some(d) if d.user_pool_id == pool_id => {
                state.domains.remove(domain);
            }
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Domain {domain} does not exist."),
                ));
            }
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    // ── Device Management ───────────────────────────────────────────────

    fn admin_get_device(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let username = require_str(&body, "Username")?;
        let device_key = require_str(&body, "DeviceKey")?;

        let state = self.state.read();

        let users = state.users.get(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        let user = users.get(username).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User {username} does not exist."),
            )
        })?;

        let device = user.devices.get(device_key).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Device {device_key} does not exist."),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({
            "Device": device_to_json(device)
        })))
    }

    fn admin_list_devices(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let username = require_str(&body, "Username")?;
        let limit = body["Limit"].as_i64().unwrap_or(10).clamp(1, 60) as usize;
        let pagination_token = body["PaginationToken"].as_str();

        let state = self.state.read();

        let users = state.users.get(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        let user = users.get(username).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User {username} does not exist."),
            )
        })?;

        let mut devices: Vec<&Device> = user.devices.values().collect();
        devices.sort_by(|a, b| a.device_create_date.cmp(&b.device_create_date));

        let start = pagination_token
            .and_then(|t| devices.iter().position(|d| d.device_key == t))
            .unwrap_or(0);

        let page = &devices[start..devices.len().min(start + limit)];
        let next_token = if start + limit < devices.len() {
            devices.get(start + limit).map(|d| d.device_key.clone())
        } else {
            None
        };

        let mut result = json!({
            "Devices": page.iter().map(|d| device_to_json(d)).collect::<Vec<_>>()
        });
        if let Some(token) = next_token {
            result["PaginationToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(result))
    }

    fn admin_forget_device(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let username = require_str(&body, "Username")?;
        let device_key = require_str(&body, "DeviceKey")?;

        let mut state = self.state.write();

        let users = state.users.get_mut(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        let user = users.get_mut(username).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User {username} does not exist."),
            )
        })?;

        if user.devices.remove(device_key).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Device {device_key} does not exist."),
            ));
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn admin_update_device_status(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let username = require_str(&body, "Username")?;
        let device_key = require_str(&body, "DeviceKey")?;
        let status = body["DeviceRememberedStatus"]
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

        let user = users.get_mut(username).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User {username} does not exist."),
            )
        })?;

        let device = user.devices.get_mut(device_key).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Device {device_key} does not exist."),
            )
        })?;

        device.device_remembered_status = status;
        device.device_last_modified_date = Utc::now();

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn confirm_device(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let access_token = require_str(&body, "AccessToken")?;
        let device_key = require_str(&body, "DeviceKey")?;
        let device_name = body["DeviceName"].as_str();

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

        let now = Utc::now();
        let mut device_attributes = HashMap::new();
        if let Some(name) = device_name {
            device_attributes.insert("device_name".to_string(), name.to_string());
        }

        user.devices.insert(
            device_key.to_string(),
            Device {
                device_key: device_key.to_string(),
                device_attributes,
                device_create_date: now,
                device_last_modified_date: now,
                device_last_authenticated_date: Some(now),
                device_remembered_status: None,
            },
        );

        Ok(AwsResponse::ok_json(json!({
            "UserConfirmationNecessary": false
        })))
    }

    // ── Tags ────────────────────────────────────────────────────────────

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_arn = require_str(&body, "ResourceArn")?;

        let tags: HashMap<String, String> = body["Tags"]
            .as_object()
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();

        let mut state = self.state.write();

        // Validate that the ARN matches a known user pool
        let pool_exists = state.user_pools.values().any(|p| p.arn == resource_arn);
        if !pool_exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Resource {resource_arn} not found."),
            ));
        }

        let existing = state.tags.entry(resource_arn.to_string()).or_default();
        for (k, v) in tags {
            existing.insert(k, v);
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_arn = require_str(&body, "ResourceArn")?;

        let tag_keys: Vec<String> = body["TagKeys"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let mut state = self.state.write();

        let pool_exists = state.user_pools.values().any(|p| p.arn == resource_arn);
        if !pool_exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Resource {resource_arn} not found."),
            ));
        }

        if let Some(tags) = state.tags.get_mut(resource_arn) {
            for key in &tag_keys {
                tags.remove(key);
            }
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_arn = require_str(&body, "ResourceArn")?;

        let state = self.state.read();

        let pool_exists = state.user_pools.values().any(|p| p.arn == resource_arn);
        if !pool_exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Resource {resource_arn} not found."),
            ));
        }

        let tags = state.tags.get(resource_arn).cloned().unwrap_or_default();

        Ok(AwsResponse::ok_json(json!({ "Tags": tags })))
    }

    // ── Import Jobs ─────────────────────────────────────────────────────

    fn get_csv_header(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;

        let state = self.state.read();

        let pool = state.user_pools.get(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        let csv_header: Vec<String> = pool
            .schema_attributes
            .iter()
            .map(|a| a.name.clone())
            .collect();

        Ok(AwsResponse::ok_json(json!({
            "UserPoolId": pool_id,
            "CSVHeader": csv_header
        })))
    }

    fn create_user_import_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let job_name = require_str(&body, "JobName")?;
        let cw_role_arn = require_str(&body, "CloudWatchLogsRoleArn")?;

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let job_id = format!("import-{}", Uuid::new_v4());
        let now = Utc::now();

        let job = UserImportJob {
            job_id: job_id.clone(),
            job_name: job_name.to_string(),
            user_pool_id: pool_id.to_string(),
            cloud_watch_logs_role_arn: cw_role_arn.to_string(),
            status: "Created".to_string(),
            creation_date: now,
            start_date: None,
            completion_date: None,
            pre_signed_url: Some(format!(
                "https://fakecloud-import.s3.amazonaws.com/{pool_id}/{job_id}/upload.csv"
            )),
        };

        let resp = import_job_to_json(&job);

        state
            .import_jobs
            .entry(pool_id.to_string())
            .or_default()
            .insert(job_id, job);

        Ok(AwsResponse::ok_json(json!({
            "UserImportJob": resp
        })))
    }

    fn describe_user_import_job(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let job_id = require_str(&body, "JobId")?;

        let state = self.state.read();

        let job = state
            .import_jobs
            .get(pool_id)
            .and_then(|jobs| jobs.get(job_id))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Import job {job_id} does not exist."),
                )
            })?;

        Ok(AwsResponse::ok_json(json!({
            "UserImportJob": import_job_to_json(job)
        })))
    }

    fn list_user_import_jobs(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(10).clamp(1, 60) as usize;
        let pagination_token = body["PaginationToken"].as_str();

        let state = self.state.read();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let mut jobs: Vec<&UserImportJob> = state
            .import_jobs
            .get(pool_id)
            .map(|m| m.values().collect())
            .unwrap_or_default();
        jobs.sort_by(|a, b| a.creation_date.cmp(&b.creation_date));

        let start = pagination_token
            .and_then(|t| jobs.iter().position(|j| j.job_id == t))
            .unwrap_or(0);

        let page = &jobs[start..jobs.len().min(start + max_results)];
        let next_token = if start + max_results < jobs.len() {
            jobs.get(start + max_results).map(|j| j.job_id.clone())
        } else {
            None
        };

        let mut result = json!({
            "UserImportJobs": page.iter().map(|j| import_job_to_json(j)).collect::<Vec<_>>()
        });
        if let Some(token) = next_token {
            result["PaginationToken"] = json!(token);
        }

        Ok(AwsResponse::ok_json(result))
    }
}

fn device_to_json(device: &Device) -> Value {
    let attrs: Vec<Value> = device
        .device_attributes
        .iter()
        .map(|(k, v)| json!({"Name": k, "Value": v}))
        .collect();

    let mut obj = json!({
        "DeviceKey": device.device_key,
        "DeviceAttributes": attrs,
        "DeviceCreateDate": device.device_create_date.timestamp() as f64,
        "DeviceLastModifiedDate": device.device_last_modified_date.timestamp() as f64,
    });
    if let Some(auth_date) = device.device_last_authenticated_date {
        obj["DeviceLastAuthenticatedDate"] = json!(auth_date.timestamp() as f64);
    }
    if let Some(ref status) = device.device_remembered_status {
        obj["DeviceRememberedStatus"] = json!(status);
    }
    obj
}

fn import_job_to_json(job: &UserImportJob) -> Value {
    let mut obj = json!({
        "JobId": job.job_id,
        "JobName": job.job_name,
        "UserPoolId": job.user_pool_id,
        "CloudWatchLogsRoleArn": job.cloud_watch_logs_role_arn,
        "Status": job.status,
        "CreationDate": job.creation_date.timestamp() as f64,
    });
    if let Some(url) = &job.pre_signed_url {
        obj["PreSignedUrl"] = json!(url);
    }
    if let Some(d) = job.start_date {
        obj["StartDate"] = json!(d.timestamp() as f64);
    }
    if let Some(d) = job.completion_date {
        obj["CompletionDate"] = json!(d.timestamp() as f64);
    }
    obj
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

/// Generate a 6-digit confirmation code for password reset flows.
fn generate_confirmation_code() -> String {
    // Use UUID bytes to generate a numeric code
    let uuid = Uuid::new_v4();
    let bytes = uuid.as_bytes();
    let num = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    format!("{:06}", num % 1_000_000)
}

/// Generate a TOTP secret key: 32 base32 characters (like AWS).
fn generate_totp_secret() -> String {
    // Base32 alphabet (RFC 4648)
    const BASE32_ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    let mut result = String::with_capacity(32);
    // Use UUID bytes as random source
    let mut bytes = Vec::with_capacity(32);
    for _ in 0..2 {
        bytes.extend_from_slice(Uuid::new_v4().as_bytes());
    }
    for &b in bytes.iter().take(32) {
        let idx = (b as usize) % BASE32_ALPHABET.len();
        result.push(BASE32_ALPHABET[idx] as char);
    }
    result
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

/// Convert a Group to the JSON format AWS returns.
fn group_to_json(group: &Group) -> Value {
    let mut val = json!({
        "GroupName": group.group_name,
        "UserPoolId": group.user_pool_id,
        "CreationDate": group.creation_date.timestamp() as f64,
        "LastModifiedDate": group.last_modified_date.timestamp() as f64,
    });
    if let Some(ref desc) = group.description {
        val["Description"] = json!(desc);
    }
    if let Some(prec) = group.precedence {
        val["Precedence"] = json!(prec);
    }
    if let Some(ref arn) = group.role_arn {
        val["RoleArn"] = json!(arn);
    }
    val
}

fn identity_provider_to_json(idp: &IdentityProvider) -> Value {
    let mut val = json!({
        "UserPoolId": idp.user_pool_id,
        "ProviderName": idp.provider_name,
        "ProviderType": idp.provider_type,
        "CreationDate": idp.creation_date.timestamp() as f64,
        "LastModifiedDate": idp.last_modified_date.timestamp() as f64,
    });
    if !idp.provider_details.is_empty() {
        val["ProviderDetails"] = json!(idp.provider_details);
    }
    if !idp.attribute_mapping.is_empty() {
        val["AttributeMapping"] = json!(idp.attribute_mapping);
    }
    if !idp.idp_identifiers.is_empty() {
        val["IdpIdentifiers"] = json!(idp.idp_identifiers);
    }
    val
}

const VALID_PROVIDER_TYPES: &[&str] = &[
    "SAML",
    "Facebook",
    "Google",
    "LoginWithAmazon",
    "SignInWithApple",
    "OIDC",
];

fn validate_provider_type(provider_type: &str) -> Result<(), AwsServiceError> {
    if !VALID_PROVIDER_TYPES.contains(&provider_type) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterException",
            format!(
                "Invalid ProviderType: {provider_type}. Must be one of: SAML, Facebook, Google, LoginWithAmazon, SignInWithApple, OIDC"
            ),
        ));
    }
    Ok(())
}

fn parse_string_map(val: &Value) -> HashMap<String, String> {
    val.as_object()
        .map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_resource_server_scopes(val: &Value) -> Vec<ResourceServerScope> {
    val.as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    let name = v["ScopeName"].as_str()?;
                    let desc = v["ScopeDescription"].as_str().unwrap_or("");
                    Some(ResourceServerScope {
                        scope_name: name.to_string(),
                        scope_description: desc.to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn resource_server_to_json(rs: &ResourceServer) -> Value {
    let scopes: Vec<Value> = rs
        .scopes
        .iter()
        .map(|s| {
            json!({
                "ScopeName": s.scope_name,
                "ScopeDescription": s.scope_description,
            })
        })
        .collect();

    json!({
        "UserPoolId": rs.user_pool_id,
        "Identifier": rs.identifier,
        "Name": rs.name,
        "Scopes": scopes,
    })
}

fn domain_description_to_json(d: &UserPoolDomain, account_id: &str) -> Value {
    let mut val = json!({
        "UserPoolId": d.user_pool_id,
        "AWSAccountId": account_id,
        "Domain": d.domain,
        "Status": d.status,
        "Version": "20130630",
    });
    if let Some(ref config) = d.custom_domain_config {
        val["CustomDomainConfig"] = json!({
            "CertificateArn": config.certificate_arn,
        });
        val["CloudFrontDistribution"] = json!(format!("d111111abcdef8.cloudfront.net"));
    }
    val
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

    /// Helper to run an async fn in sync test context.
    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        tokio::runtime::Runtime::new().unwrap().block_on(f)
    }

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
            confirmation_code: None,
            attribute_verification_codes: HashMap::new(),
            mfa_preferences: None,
            totp_secret: None,
            totp_verified: false,
            devices: HashMap::new(),
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
            confirmation_code: None,
            attribute_verification_codes: HashMap::new(),
            mfa_preferences: None,
            totp_secret: None,
            totp_verified: false,
            devices: HashMap::new(),
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
            confirmation_code: None,
            attribute_verification_codes: HashMap::new(),
            mfa_preferences: None,
            totp_secret: None,
            totp_verified: false,
            devices: HashMap::new(),
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
        let resp = block_on(svc.admin_create_user(&req)).unwrap();
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

    #[test]
    fn confirmation_code_is_six_digits() {
        for _ in 0..100 {
            let code = generate_confirmation_code();
            assert_eq!(code.len(), 6, "Code should be 6 chars: {code}");
            assert!(
                code.chars().all(|c| c.is_ascii_digit()),
                "Code should be all digits: {code}"
            );
        }
    }

    #[test]
    fn confirmation_code_uniqueness() {
        let code1 = generate_confirmation_code();
        // Generate many codes and check we get at least some different ones
        let mut found_different = false;
        for _ in 0..20 {
            if generate_confirmation_code() != code1 {
                found_different = true;
                break;
            }
        }
        assert!(found_different, "Codes should vary across calls");
    }

    #[test]
    fn access_token_lookup() {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(crate::state::CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        {
            let mut s = state.write();
            s.access_tokens.insert(
                "test-access-token".to_string(),
                AccessTokenData {
                    user_pool_id: "us-east-1_TestPool1".to_string(),
                    username: "testuser".to_string(),
                    client_id: "testclient123".to_string(),
                    issued_at: Utc::now(),
                },
            );
        }

        let s = state.read();
        let token_data = s.access_tokens.get("test-access-token");
        assert!(token_data.is_some());
        let data = token_data.unwrap();
        assert_eq!(data.user_pool_id, "us-east-1_TestPool1");
        assert_eq!(data.username, "testuser");
        assert_eq!(data.client_id, "testclient123");

        // Non-existent token returns None
        assert!(!s.access_tokens.contains_key("nonexistent"));
    }

    #[test]
    fn group_name_uniqueness() {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(crate::state::CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        let svc = CognitoService::new(state.clone());

        // Create a pool first
        let create_pool_req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "CreateUserPool".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({ "PoolName": "test-pool" })).unwrap(),
            ),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        };
        let resp = svc.create_user_pool(&create_pool_req).unwrap();
        let resp_json: Value =
            serde_json::from_str(core::str::from_utf8(&resp.body).unwrap()).unwrap();
        let pool_id = resp_json["UserPool"]["Id"].as_str().unwrap().to_string();

        // Create a group
        let create_group_req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "CreateGroup".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({
                    "UserPoolId": pool_id,
                    "GroupName": "admins"
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
        let result = svc.create_group(&create_group_req);
        assert!(result.is_ok());

        // Creating the same group again should fail with GroupExistsException
        let result = svc.create_group(&create_group_req);
        match result {
            Err(e) => {
                let msg = format!("{e:?}");
                assert!(
                    msg.contains("GroupExistsException"),
                    "Should be GroupExistsException: {msg}"
                );
            }
            Ok(_) => panic!("Expected GroupExistsException"),
        }
    }

    #[test]
    fn user_group_association() {
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
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({ "PoolName": "test-pool" })).unwrap(),
            ),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        };
        let resp = svc.create_user_pool(&create_pool_req).unwrap();
        let resp_json: Value =
            serde_json::from_str(core::str::from_utf8(&resp.body).unwrap()).unwrap();
        let pool_id = resp_json["UserPool"]["Id"].as_str().unwrap().to_string();

        // Create a user
        let create_user_req = AwsRequest {
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
        block_on(svc.admin_create_user(&create_user_req)).unwrap();

        // Create a group
        let create_group_req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "CreateGroup".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({
                    "UserPoolId": pool_id,
                    "GroupName": "admins"
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
        svc.create_group(&create_group_req).unwrap();

        // Add user to group
        let add_req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "AdminAddUserToGroup".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({
                    "UserPoolId": pool_id,
                    "Username": "testuser",
                    "GroupName": "admins"
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
        svc.admin_add_user_to_group(&add_req).unwrap();

        // Verify membership via state
        {
            let s = state.read();
            let groups = s.user_groups.get(&pool_id).unwrap();
            let user_groups = groups.get("testuser").unwrap();
            assert!(user_groups.contains(&"admins".to_string()));
        }

        // Remove user from group
        let remove_req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "AdminRemoveUserFromGroup".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({
                    "UserPoolId": pool_id,
                    "Username": "testuser",
                    "GroupName": "admins"
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
        svc.admin_remove_user_from_group(&remove_req).unwrap();

        // Verify no longer in group
        {
            let s = state.read();
            let groups = s.user_groups.get(&pool_id).unwrap();
            let user_groups = groups.get("testuser").unwrap();
            assert!(!user_groups.contains(&"admins".to_string()));
        }
    }

    #[test]
    fn self_service_get_user_via_access_token() {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(crate::state::CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        let svc = CognitoService::new(state.clone());

        // Create pool
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
        let pool_id = pool_json["UserPool"]["Id"].as_str().unwrap().to_string();

        // Create user
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
                    "Username": "selfuser",
                    "UserAttributes": [
                        {"Name": "email", "Value": "self@example.com"}
                    ]
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
        block_on(svc.admin_create_user(&req)).unwrap();

        // Manually insert an access token
        {
            let mut s = state.write();
            s.access_tokens.insert(
                "test-access-token".to_string(),
                crate::state::AccessTokenData {
                    user_pool_id: pool_id.clone(),
                    username: "selfuser".to_string(),
                    client_id: "test-client".to_string(),
                    issued_at: Utc::now(),
                },
            );
        }

        // GetUser with valid token
        let req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "GetUser".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({"AccessToken": "test-access-token"})).unwrap(),
            ),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        };
        let resp = svc.get_user(&req).unwrap();
        let resp_json: Value =
            serde_json::from_str(core::str::from_utf8(&resp.body).unwrap()).unwrap();
        assert_eq!(resp_json["Username"], "selfuser");

        // GetUser with invalid token
        let req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "GetUser".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({"AccessToken": "invalid-token"})).unwrap(),
            ),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        };
        match svc.get_user(&req) {
            Err(e) => assert_eq!(e.code(), "NotAuthorizedException"),
            Ok(_) => panic!("Expected NotAuthorizedException"),
        }
    }

    #[test]
    fn self_service_delete_user_cleans_up_tokens() {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(crate::state::CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        let svc = CognitoService::new(state.clone());

        // Create pool
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
        let pool_id = pool_json["UserPool"]["Id"].as_str().unwrap().to_string();

        // Create user
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
                    "Username": "deluser"
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
        block_on(svc.admin_create_user(&req)).unwrap();

        // Insert access token and refresh token
        {
            let mut s = state.write();
            s.access_tokens.insert(
                "del-token".to_string(),
                crate::state::AccessTokenData {
                    user_pool_id: pool_id.clone(),
                    username: "deluser".to_string(),
                    client_id: "test-client".to_string(),
                    issued_at: Utc::now(),
                },
            );
            s.refresh_tokens.insert(
                "del-refresh".to_string(),
                crate::state::RefreshTokenData {
                    user_pool_id: pool_id.clone(),
                    username: "deluser".to_string(),
                    client_id: "test-client".to_string(),
                    issued_at: Utc::now(),
                },
            );
        }

        // Delete user via self-service
        let req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "DeleteUser".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({"AccessToken": "del-token"})).unwrap(),
            ),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        };
        svc.delete_user(&req).unwrap();

        // Verify cleanup
        let s = state.read();
        assert!(s.access_tokens.is_empty());
        assert!(s.refresh_tokens.is_empty());
        assert!(s
            .users
            .get(&pool_id)
            .and_then(|u| u.get("deluser"))
            .is_none());
    }

    #[test]
    fn verify_user_attribute_with_correct_code() {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(crate::state::CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        let svc = CognitoService::new(state.clone());

        // Create pool
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
        let pool_id = pool_json["UserPool"]["Id"].as_str().unwrap().to_string();

        // Create user with email
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
                    "Username": "verifyuser",
                    "UserAttributes": [{"Name": "email", "Value": "verify@example.com"}]
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
        block_on(svc.admin_create_user(&req)).unwrap();

        // Insert access token
        {
            let mut s = state.write();
            s.access_tokens.insert(
                "verify-token".to_string(),
                crate::state::AccessTokenData {
                    user_pool_id: pool_id.clone(),
                    username: "verifyuser".to_string(),
                    client_id: "test-client".to_string(),
                    issued_at: Utc::now(),
                },
            );
        }

        // Get verification code
        let req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "GetUserAttributeVerificationCode".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({
                    "AccessToken": "verify-token",
                    "AttributeName": "email"
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
        let resp = svc.get_user_attribute_verification_code(&req).unwrap();
        let resp_json: Value =
            serde_json::from_str(core::str::from_utf8(&resp.body).unwrap()).unwrap();
        assert_eq!(resp_json["CodeDeliveryDetails"]["DeliveryMedium"], "EMAIL");
        assert_eq!(resp_json["CodeDeliveryDetails"]["AttributeName"], "email");

        // Read the code from state
        let code = {
            let s = state.read();
            let user = s.users.get(&pool_id).unwrap().get("verifyuser").unwrap();
            user.attribute_verification_codes
                .get("email")
                .unwrap()
                .clone()
        };

        // Verify with correct code
        let req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "VerifyUserAttribute".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({
                    "AccessToken": "verify-token",
                    "AttributeName": "email",
                    "Code": code
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
        svc.verify_user_attribute(&req).unwrap();

        // Verify email_verified is set
        let s = state.read();
        let user = s.users.get(&pool_id).unwrap().get("verifyuser").unwrap();
        let email_verified = user
            .attributes
            .iter()
            .find(|a| a.name == "email_verified")
            .unwrap();
        assert_eq!(email_verified.value, "true");

        // Verify with wrong code should fail
        drop(s);
        // First get a new code
        let req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "GetUserAttributeVerificationCode".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({
                    "AccessToken": "verify-token",
                    "AttributeName": "email"
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
        svc.get_user_attribute_verification_code(&req).unwrap();

        let req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "VerifyUserAttribute".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({
                    "AccessToken": "verify-token",
                    "AttributeName": "email",
                    "Code": "000000"
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
        match svc.verify_user_attribute(&req) {
            Err(e) => assert_eq!(e.code(), "CodeMismatchException"),
            Ok(_) => panic!("Expected CodeMismatchException"),
        }
    }

    #[test]
    fn totp_secret_format() {
        let secret = generate_totp_secret();
        assert_eq!(secret.len(), 32, "TOTP secret should be 32 chars: {secret}");
        assert!(
            secret
                .chars()
                .all(|c| "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567".contains(c)),
            "TOTP secret should be base32: {secret}"
        );
    }

    #[test]
    fn totp_secret_uniqueness() {
        let s1 = generate_totp_secret();
        let s2 = generate_totp_secret();
        assert_ne!(s1, s2, "TOTP secrets should be unique");
    }

    #[test]
    fn mfa_preference_storage() {
        use crate::state::CognitoState;
        use std::sync::Arc;

        let state = Arc::new(parking_lot::RwLock::new(CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        let svc = CognitoService::new(state.clone());

        // Create a pool and user first
        let create_pool_req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "CreateUserPool".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({ "PoolName": "mfa-pool" })).unwrap(),
            ),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        };
        let pool_resp = svc.create_user_pool(&create_pool_req).unwrap();
        let pool_body: Value = serde_json::from_slice(&pool_resp.body).unwrap();
        let pool_id = pool_body["UserPool"]["Id"].as_str().unwrap().to_string();

        let create_user_req = AwsRequest {
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
                    "Username": "mfauser"
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
        block_on(svc.admin_create_user(&create_user_req)).unwrap();

        // Set MFA preference via admin
        let set_pref_req = AwsRequest {
            service: "cognito-idp".to_string(),
            action: "AdminSetUserMFAPreference".to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(
                serde_json::to_string(&json!({
                    "UserPoolId": pool_id,
                    "Username": "mfauser",
                    "SoftwareTokenMfaSettings": {
                        "Enabled": true,
                        "PreferredMfa": true
                    },
                    "SMSMfaSettings": {
                        "Enabled": false,
                        "PreferredMfa": false
                    }
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
        svc.admin_set_user_mfa_preference(&set_pref_req).unwrap();

        // Verify preferences were stored
        let st = state.read();
        let user = st.users.get(&pool_id).unwrap().get("mfauser").unwrap();
        let prefs = user.mfa_preferences.as_ref().unwrap();
        assert!(prefs.software_token_enabled);
        assert!(prefs.software_token_preferred);
        assert!(!prefs.sms_enabled);
        assert!(!prefs.sms_preferred);
    }

    fn make_req(action: &str, body: &str) -> AwsRequest {
        AwsRequest {
            service: "cognito-idp".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(body.to_string()),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
        }
    }

    fn setup_svc_with_pool() -> (CognitoService, String) {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(crate::state::CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        let svc = CognitoService::new(state);
        let req = make_req("CreateUserPool", r#"{"PoolName":"test"}"#);
        let resp = svc.create_user_pool(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        let pool_id = resp_body["UserPool"]["Id"].as_str().unwrap().to_string();
        (svc, pool_id)
    }

    #[test]
    fn identity_provider_name_uniqueness() {
        let (svc, pool_id) = setup_svc_with_pool();

        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "ProviderName": "MyGoogle",
            "ProviderType": "Google",
            "ProviderDetails": {"client_id": "123", "client_secret": "secret"}
        }))
        .unwrap();
        let req = make_req("CreateIdentityProvider", &body);
        svc.create_identity_provider(&req).unwrap();

        // Duplicate name should fail
        let req2 = make_req("CreateIdentityProvider", &body);
        match svc.create_identity_provider(&req2) {
            Err(e) => assert_eq!(e.code(), "DuplicateProviderException"),
            Ok(_) => panic!("Expected DuplicateProviderException"),
        }
    }

    #[test]
    fn identity_provider_type_validation() {
        let (svc, pool_id) = setup_svc_with_pool();

        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "ProviderName": "MyInvalid",
            "ProviderType": "InvalidType",
            "ProviderDetails": {}
        }))
        .unwrap();
        let req = make_req("CreateIdentityProvider", &body);
        match svc.create_identity_provider(&req) {
            Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
            Ok(_) => panic!("Expected InvalidParameterException"),
        }

        // Valid types should all work
        for provider_type in &[
            "SAML",
            "Facebook",
            "Google",
            "LoginWithAmazon",
            "SignInWithApple",
            "OIDC",
        ] {
            let body = serde_json::to_string(&json!({
                "UserPoolId": pool_id,
                "ProviderName": format!("prov_{provider_type}"),
                "ProviderType": provider_type,
                "ProviderDetails": {}
            }))
            .unwrap();
            let req = make_req("CreateIdentityProvider", &body);
            assert!(
                svc.create_identity_provider(&req).is_ok(),
                "ProviderType {provider_type} should be valid"
            );
        }
    }

    #[test]
    fn resource_server_identifier_uniqueness() {
        let (svc, pool_id) = setup_svc_with_pool();

        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "Identifier": "https://api.example.com",
            "Name": "My API",
            "Scopes": [{"ScopeName": "read", "ScopeDescription": "Read access"}]
        }))
        .unwrap();
        let req = make_req("CreateResourceServer", &body);
        svc.create_resource_server(&req).unwrap();

        // Duplicate identifier should fail
        let body2 = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "Identifier": "https://api.example.com",
            "Name": "My API 2",
            "Scopes": []
        }))
        .unwrap();
        let req2 = make_req("CreateResourceServer", &body2);
        match svc.create_resource_server(&req2) {
            Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
            Ok(_) => panic!("Expected InvalidParameterException for duplicate identifier"),
        }
    }

    #[test]
    fn domain_uniqueness() {
        let (svc, pool_id) = setup_svc_with_pool();

        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "Domain": "my-unique-domain"
        }))
        .unwrap();
        let req = make_req("CreateUserPoolDomain", &body);
        svc.create_user_pool_domain(&req).unwrap();

        // Duplicate domain should fail
        let body2 = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "Domain": "my-unique-domain"
        }))
        .unwrap();
        let req2 = make_req("CreateUserPoolDomain", &body2);
        match svc.create_user_pool_domain(&req2) {
            Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
            Ok(_) => panic!("Expected InvalidParameterException for duplicate domain"),
        }
    }

    fn setup_svc_with_pool_and_user() -> (CognitoService, String, String) {
        let (svc, pool_id) = setup_svc_with_pool();
        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "Username": "deviceuser",
            "TemporaryPassword": "Temp1234!"
        }))
        .unwrap();
        let req = make_req("AdminCreateUser", &body);
        block_on(svc.admin_create_user(&req)).unwrap();
        (svc, pool_id, "deviceuser".to_string())
    }

    #[test]
    fn device_key_storage() {
        let (svc, pool_id, username) = setup_svc_with_pool_and_user();

        // Directly insert a device into the user's devices map
        {
            let mut state = svc.state.write();
            let user = state
                .users
                .get_mut(&pool_id)
                .unwrap()
                .get_mut(&username)
                .unwrap();
            user.devices.insert(
                "dev-key-1".to_string(),
                Device {
                    device_key: "dev-key-1".to_string(),
                    device_attributes: HashMap::new(),
                    device_create_date: Utc::now(),
                    device_last_modified_date: Utc::now(),
                    device_last_authenticated_date: None,
                    device_remembered_status: None,
                },
            );
        }

        // AdminGetDevice
        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "Username": username,
            "DeviceKey": "dev-key-1"
        }))
        .unwrap();
        let req = make_req("AdminGetDevice", &body);
        let resp = svc.admin_get_device(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(resp_body["Device"]["DeviceKey"], "dev-key-1");

        // AdminForgetDevice
        let req = make_req("AdminForgetDevice", &body);
        svc.admin_forget_device(&req).unwrap();

        // Device should be gone
        let req = make_req("AdminGetDevice", &body);
        match svc.admin_get_device(&req) {
            Err(e) => assert_eq!(e.code(), "ResourceNotFoundException"),
            Ok(_) => panic!("Expected ResourceNotFoundException"),
        }
    }

    #[test]
    fn tag_management() {
        let (svc, pool_id) = setup_svc_with_pool();
        let state = svc.state.read();
        let arn = state.user_pools.get(&pool_id).unwrap().arn.clone();
        drop(state);

        // Tag
        let body = serde_json::to_string(&json!({
            "ResourceArn": arn,
            "Tags": {"env": "test", "team": "core"}
        }))
        .unwrap();
        let req = make_req("TagResource", &body);
        svc.tag_resource(&req).unwrap();

        // List
        let body = serde_json::to_string(&json!({"ResourceArn": arn})).unwrap();
        let req = make_req("ListTagsForResource", &body);
        let resp = svc.list_tags_for_resource(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(resp_body["Tags"]["env"], "test");
        assert_eq!(resp_body["Tags"]["team"], "core");

        // Untag
        let body = serde_json::to_string(&json!({
            "ResourceArn": arn,
            "TagKeys": ["team"]
        }))
        .unwrap();
        let req = make_req("UntagResource", &body);
        svc.untag_resource(&req).unwrap();

        // Verify
        let body = serde_json::to_string(&json!({"ResourceArn": arn})).unwrap();
        let req = make_req("ListTagsForResource", &body);
        let resp = svc.list_tags_for_resource(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(resp_body["Tags"]["env"], "test");
        assert!(resp_body["Tags"]["team"].is_null());
    }

    #[test]
    fn import_job_creation() {
        let (svc, pool_id) = setup_svc_with_pool();

        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "JobName": "my-import",
            "CloudWatchLogsRoleArn": "arn:aws:iam::123456789012:role/CognitoImport"
        }))
        .unwrap();
        let req = make_req("CreateUserImportJob", &body);
        let resp = svc.create_user_import_job(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        let job = &resp_body["UserImportJob"];
        assert_eq!(job["JobName"], "my-import");
        assert_eq!(job["Status"], "Created");
        assert!(job["JobId"].as_str().unwrap().starts_with("import-"));
        assert!(job["PreSignedUrl"].as_str().is_some());

        // Describe
        let job_id = job["JobId"].as_str().unwrap();
        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "JobId": job_id
        }))
        .unwrap();
        let req = make_req("DescribeUserImportJob", &body);
        let resp = svc.describe_user_import_job(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(resp_body["UserImportJob"]["JobName"], "my-import");

        // List
        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "MaxResults": 10
        }))
        .unwrap();
        let req = make_req("ListUserImportJobs", &body);
        let resp = svc.list_user_import_jobs(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        assert_eq!(resp_body["UserImportJobs"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn auth_events_recorded_on_sign_up() {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(crate::state::CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        let svc = CognitoService::new(state.clone());

        // Create pool and client
        let req = make_req("CreateUserPool", r#"{"PoolName": "evpool"}"#);
        let resp = svc.create_user_pool(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        let pool_id = resp_body["UserPool"]["Id"].as_str().unwrap().to_string();

        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "ClientName": "evclient",
            "ExplicitAuthFlows": ["ALLOW_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"]
        }))
        .unwrap();
        let req = make_req("CreateUserPoolClient", &body);
        let resp = svc.create_user_pool_client(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        let client_id = resp_body["UserPoolClient"]["ClientId"]
            .as_str()
            .unwrap()
            .to_string();

        // Sign up
        let body = serde_json::to_string(&json!({
            "ClientId": client_id,
            "Username": "testevuser",
            "Password": "P@ssw0rd!",
            "UserAttributes": [{"Name": "email", "Value": "test@example.com"}]
        }))
        .unwrap();
        let req = make_req("SignUp", &body);
        block_on(svc.sign_up(&req)).unwrap();

        // Check auth events
        let st = state.read();
        assert_eq!(st.auth_events.len(), 1);
        assert_eq!(st.auth_events[0].event_type, "SIGN_UP");
        assert_eq!(st.auth_events[0].username, "testevuser");
        assert!(st.auth_events[0].success);
    }

    #[test]
    fn auth_events_recorded_on_sign_in_and_failure() {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(crate::state::CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        let svc = CognitoService::new(state.clone());

        // Create pool, client, user
        let req = make_req("CreateUserPool", r#"{"PoolName": "authpool"}"#);
        let resp = svc.create_user_pool(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        let pool_id = resp_body["UserPool"]["Id"].as_str().unwrap().to_string();

        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "ClientName": "authclient",
            "ExplicitAuthFlows": ["ALLOW_ADMIN_USER_PASSWORD_AUTH", "ALLOW_REFRESH_TOKEN_AUTH"]
        }))
        .unwrap();
        let req = make_req("CreateUserPoolClient", &body);
        let resp = svc.create_user_pool_client(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        let client_id = resp_body["UserPoolClient"]["ClientId"]
            .as_str()
            .unwrap()
            .to_string();

        // Create user and set permanent password
        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "Username": "authuser",
            "TemporaryPassword": "TempP@ss1!"
        }))
        .unwrap();
        let req = make_req("AdminCreateUser", &body);
        block_on(svc.admin_create_user(&req)).unwrap();

        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "Username": "authuser",
            "Password": "P@ssw0rd!",
            "Permanent": true
        }))
        .unwrap();
        let req = make_req("AdminSetUserPassword", &body);
        svc.admin_set_user_password(&req).unwrap();

        // Successful auth
        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "ClientId": client_id,
            "AuthFlow": "ADMIN_USER_PASSWORD_AUTH",
            "AuthParameters": {"USERNAME": "authuser", "PASSWORD": "P@ssw0rd!"}
        }))
        .unwrap();
        let req = make_req("AdminInitiateAuth", &body);
        block_on(svc.admin_initiate_auth(&req)).unwrap();

        // Failed auth
        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "ClientId": client_id,
            "AuthFlow": "ADMIN_USER_PASSWORD_AUTH",
            "AuthParameters": {"USERNAME": "authuser", "PASSWORD": "WrongPass!"}
        }))
        .unwrap();
        let req = make_req("AdminInitiateAuth", &body);
        let _ = block_on(svc.admin_initiate_auth(&req));

        // Check events
        let st = state.read();
        assert_eq!(st.auth_events.len(), 2);
        assert_eq!(st.auth_events[0].event_type, "SIGN_IN");
        assert!(st.auth_events[0].success);
        assert_eq!(st.auth_events[1].event_type, "SIGN_IN_FAILURE");
        assert!(!st.auth_events[1].success);
    }

    #[test]
    fn auth_events_cleared_on_reset() {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(crate::state::CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        state.write().auth_events.push(AuthEvent {
            event_type: "SIGN_UP".to_string(),
            username: "test".to_string(),
            user_pool_id: "pool".to_string(),
            client_id: None,
            timestamp: Utc::now(),
            success: true,
        });
        assert_eq!(state.read().auth_events.len(), 1);
        state.write().reset();
        assert!(state.read().auth_events.is_empty());
    }
}
