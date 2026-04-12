mod auth;
mod config;
mod groups;
mod identity_providers;
mod mfa;
mod misc;
mod user_pools;
mod users;

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
    AccountRecoverySetting, AdminCreateUserConfig, Device, EmailConfiguration, Group,
    IdentityProvider, InviteMessageTemplate, PasswordPolicy, RecoveryOption, ResourceServer,
    ResourceServerScope, SchemaAttribute, SharedCognitoState, SmsConfiguration,
    StringAttributeConstraints, TokenValidityUnits, User, UserAttribute, UserImportJob, UserPool,
    UserPoolClient, UserPoolDomain,
};
use crate::triggers::CognitoDeliveryContext;

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
            "AddCustomAttributes" => self.add_custom_attributes(&req),
            "AddUserPoolClientSecret" => self.add_user_pool_client_secret(&req),
            "DeleteUserPoolClientSecret" => self.delete_user_pool_client_secret(&req),
            "ListUserPoolClientSecrets" => self.list_user_pool_client_secrets(&req),
            "GetSigningCertificate" => self.get_signing_certificate(&req),
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
            "RespondToAuthChallenge" => self.respond_to_auth_challenge(&req).await,
            "AdminRespondToAuthChallenge" => self.admin_respond_to_auth_challenge(&req).await,
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
            "GetUserAuthFactors" => self.get_user_auth_factors(&req),
            "CreateIdentityProvider" => self.create_identity_provider(&req),
            "DescribeIdentityProvider" => self.describe_identity_provider(&req),
            "UpdateIdentityProvider" => self.update_identity_provider(&req),
            "DeleteIdentityProvider" => self.delete_identity_provider(&req),
            "ListIdentityProviders" => self.list_identity_providers(&req),
            "GetIdentityProviderByIdentifier" => self.get_identity_provider_by_identifier(&req),
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
            "ForgetDevice" => self.forget_device(&req),
            "GetDevice" => self.get_device(&req),
            "ListDevices" => self.list_devices(&req),
            "UpdateDeviceStatus" => self.update_device_status(&req),
            "RevokeToken" => self.revoke_token(&req),
            "GetTokensFromRefreshToken" => self.get_tokens_from_refresh_token(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "GetCSVHeader" => self.get_csv_header(&req),
            "CreateUserImportJob" => self.create_user_import_job(&req),
            "DescribeUserImportJob" => self.describe_user_import_job(&req),
            "ListUserImportJobs" => self.list_user_import_jobs(&req),
            "GetUICustomization" => self.get_ui_customization(&req),
            "SetUICustomization" => self.set_ui_customization(&req),
            "GetLogDeliveryConfiguration" => self.get_log_delivery_configuration(&req),
            "SetLogDeliveryConfiguration" => self.set_log_delivery_configuration(&req),
            "DescribeRiskConfiguration" => self.describe_risk_configuration(&req),
            "SetRiskConfiguration" => self.set_risk_configuration(&req),
"StartUserImportJob" => self.start_user_import_job(&req),
            "StopUserImportJob" => self.stop_user_import_job(&req),
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
            "AddCustomAttributes",
            "AddUserPoolClientSecret",
            "DeleteUserPoolClientSecret",
            "ListUserPoolClientSecrets",
            "GetSigningCertificate",
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
            "GetUserAuthFactors",
            "CreateIdentityProvider",
            "DescribeIdentityProvider",
            "UpdateIdentityProvider",
            "DeleteIdentityProvider",
            "ListIdentityProviders",
            "GetIdentityProviderByIdentifier",
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
            "ForgetDevice",
            "GetDevice",
            "ListDevices",
            "UpdateDeviceStatus",
            "RevokeToken",
            "GetTokensFromRefreshToken",
            "TagResource",
            "UntagResource",
            "ListTagsForResource",
            "GetCSVHeader",
            "CreateUserImportJob",
            "DescribeUserImportJob",
            "ListUserImportJobs",
            "GetUICustomization",
            "SetUICustomization",
            "GetLogDeliveryConfiguration",
            "SetLogDeliveryConfiguration",
            "DescribeRiskConfiguration",
            "SetRiskConfiguration",
"StartUserImportJob",
            "StopUserImportJob",
        ]
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

/// Validate that a string value is one of the allowed enum values.
fn validate_enum(value: &str, field: &str, allowed: &[&str]) -> Result<(), AwsServiceError> {
    if !allowed.contains(&value) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterException",
            format!(
                "1 validation error detected: Value '{}' at '{}' failed to satisfy constraint: Member must satisfy enum value set: [{}]",
                value, field, allowed.join(", ")
            ),
        ));
    }
    Ok(())
}

/// Validate string length is within min..=max bounds.
fn validate_string_length(
    value: &str,
    field: &str,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    let len = value.len();
    if len < min {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterException",
            format!(
                "1 validation error detected: Value '{}' at '{}' failed to satisfy constraint: Member must have length greater than or equal to {}",
                value, field, min
            ),
        ));
    }
    if len > max {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterException",
            format!(
                "1 validation error detected: Value at '{}' failed to satisfy constraint: Member must have length less than or equal to {}",
                field, max
            ),
        ));
    }
    Ok(())
}

/// Validate an integer is within min..=max bounds.
fn validate_range(value: i64, field: &str, min: i64, max: i64) -> Result<(), AwsServiceError> {
    if value < min || value > max {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterException",
            format!(
                "1 validation error detected: Value '{}' at '{}' failed to satisfy constraint: Member must have value between {} and {}",
                value, field, min, max
            ),
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

    use crate::state::{
        default_schema_attributes, AccessTokenData, AuthEvent, ChallengeResult, SessionData,
    };
    use crate::triggers;

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
    fn list_users_requires_user_pool_id() {
        let (svc, _) = setup_svc_with_pool();

        for body in [r#"{}"#, ""] {
            let req = make_req("ListUsers", body);
            match svc.list_users(&req) {
                Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
                Ok(_) => panic!("Expected InvalidParameterException for body {body:?}"),
            }
        }
    }

    #[test]
    fn list_users_validates_limit_bounds() {
        let (svc, pool_id) = setup_svc_with_pool();

        for limit in [0, 61] {
            let body = serde_json::to_string(&json!({
                "UserPoolId": pool_id,
                "Limit": limit,
            }))
            .unwrap();
            let req = make_req("ListUsers", &body);
            match svc.list_users(&req) {
                Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
                Ok(_) => panic!("Expected InvalidParameterException for limit {limit}"),
            }
        }
    }

    #[test]
    fn list_users_validates_optional_field_lengths() {
        let (svc, pool_id) = setup_svc_with_pool();

        let long_filter = "a".repeat(257);
        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "Filter": long_filter,
        }))
        .unwrap();
        let req = make_req("ListUsers", &body);
        match svc.list_users(&req) {
            Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
            Ok(_) => panic!("Expected InvalidParameterException for oversized filter"),
        }

        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "PaginationToken": "",
        }))
        .unwrap();
        let req = make_req("ListUsers", &body);
        match svc.list_users(&req) {
            Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
            Ok(_) => panic!("Expected InvalidParameterException for empty pagination token"),
        }
    }

    #[test]
    fn list_users_validates_user_pool_id_length() {
        let (svc, _) = setup_svc_with_pool();

        let body = serde_json::to_string(&json!({
            "UserPoolId": "",
        }))
        .unwrap();
        let req = make_req("ListUsers", &body);
        match svc.list_users(&req) {
            Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
            Ok(_) => panic!("Expected InvalidParameterException for empty UserPoolId"),
        }

        let long_pool_id = format!("{}suffix", "a".repeat(50));
        let body = serde_json::to_string(&json!({
            "UserPoolId": long_pool_id,
        }))
        .unwrap();
        let req = make_req("ListUsers", &body);
        match svc.list_users(&req) {
            Err(e) => assert_eq!(e.code(), "InvalidParameterException"),
            Ok(_) => panic!("Expected InvalidParameterException for oversized UserPoolId"),
        }
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

    #[test]
    fn custom_auth_rejected_when_not_in_explicit_auth_flows() {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(crate::state::CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        let svc = CognitoService::new(state.clone());

        // Create pool
        let req = make_req("CreateUserPool", r#"{"PoolName": "capool"}"#);
        let resp = svc.create_user_pool(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        let pool_id = resp_body["UserPool"]["Id"].as_str().unwrap().to_string();

        // Create client WITHOUT ALLOW_CUSTOM_AUTH
        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "ClientName": "caclient",
            "ExplicitAuthFlows": ["ALLOW_USER_PASSWORD_AUTH"]
        }))
        .unwrap();
        let req = make_req("CreateUserPoolClient", &body);
        let resp = svc.create_user_pool_client(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        let client_id = resp_body["UserPoolClient"]["ClientId"]
            .as_str()
            .unwrap()
            .to_string();

        // Try CUSTOM_AUTH — should fail
        let body = serde_json::to_string(&json!({
            "ClientId": client_id,
            "AuthFlow": "CUSTOM_AUTH",
            "AuthParameters": {"USERNAME": "someuser"}
        }))
        .unwrap();
        let req = make_req("InitiateAuth", &body);
        let result = block_on(svc.initiate_auth(&req));
        let err = result.err().expect("Expected error for CUSTOM_AUTH");
        let err_str = format!("{err}");
        assert!(
            err_str.contains("CUSTOM_AUTH flow is not enabled"),
            "Expected CUSTOM_AUTH rejection, got: {err_str}"
        );
    }

    #[test]
    fn custom_auth_fails_without_delivery_context() {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(crate::state::CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        // No delivery context — no Lambda support
        let svc = CognitoService::new(state.clone());

        // Create pool and client
        let req = make_req("CreateUserPool", r#"{"PoolName": "capool2"}"#);
        let resp = svc.create_user_pool(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        let pool_id = resp_body["UserPool"]["Id"].as_str().unwrap().to_string();

        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "ClientName": "caclient2",
            "ExplicitAuthFlows": ["ALLOW_CUSTOM_AUTH"]
        }))
        .unwrap();
        let req = make_req("CreateUserPoolClient", &body);
        let resp = svc.create_user_pool_client(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        let client_id = resp_body["UserPoolClient"]["ClientId"]
            .as_str()
            .unwrap()
            .to_string();

        // Create user
        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "Username": "customuser",
            "TemporaryPassword": "TempP@ss1!"
        }))
        .unwrap();
        let req = make_req("AdminCreateUser", &body);
        block_on(svc.admin_create_user(&req)).unwrap();

        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "Username": "customuser",
            "Password": "P@ssw0rd!",
            "Permanent": true
        }))
        .unwrap();
        let req = make_req("AdminSetUserPassword", &body);
        svc.admin_set_user_password(&req).unwrap();

        // Try CUSTOM_AUTH without delivery context
        let body = serde_json::to_string(&json!({
            "ClientId": client_id,
            "AuthFlow": "CUSTOM_AUTH",
            "AuthParameters": {"USERNAME": "customuser"}
        }))
        .unwrap();
        let req = make_req("InitiateAuth", &body);
        let err = block_on(svc.initiate_auth(&req))
            .err()
            .expect("Expected error for missing delivery context");
        let err_str = format!("{err}");
        assert!(
            err_str.contains("InvalidLambdaResponseException")
                || err_str.contains("DefineAuthChallenge"),
            "Expected Lambda error, got: {err_str}"
        );
    }

    #[test]
    fn custom_auth_fails_without_define_trigger_configured() {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(crate::state::CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        let delivery_bus = std::sync::Arc::new(fakecloud_core::delivery::DeliveryBus::new());
        let ctx = triggers::CognitoDeliveryContext {
            delivery_bus: delivery_bus.clone(),
        };
        let svc = CognitoService::new(state.clone()).with_delivery(ctx);

        // Create pool WITHOUT DefineAuthChallenge Lambda configured
        let req = make_req("CreateUserPool", r#"{"PoolName": "capool3"}"#);
        let resp = svc.create_user_pool(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        let pool_id = resp_body["UserPool"]["Id"].as_str().unwrap().to_string();

        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "ClientName": "caclient3",
            "ExplicitAuthFlows": ["ALLOW_CUSTOM_AUTH"]
        }))
        .unwrap();
        let req = make_req("CreateUserPoolClient", &body);
        let resp = svc.create_user_pool_client(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        let client_id = resp_body["UserPoolClient"]["ClientId"]
            .as_str()
            .unwrap()
            .to_string();

        // Create user
        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "Username": "customuser2",
            "TemporaryPassword": "TempP@ss1!"
        }))
        .unwrap();
        let req = make_req("AdminCreateUser", &body);
        block_on(svc.admin_create_user(&req)).unwrap();

        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "Username": "customuser2",
            "Password": "P@ssw0rd!",
            "Permanent": true
        }))
        .unwrap();
        let req = make_req("AdminSetUserPassword", &body);
        svc.admin_set_user_password(&req).unwrap();

        // CUSTOM_AUTH — has delivery context but no DefineAuthChallenge Lambda
        let body = serde_json::to_string(&json!({
            "ClientId": client_id,
            "AuthFlow": "CUSTOM_AUTH",
            "AuthParameters": {"USERNAME": "customuser2"}
        }))
        .unwrap();
        let req = make_req("InitiateAuth", &body);
        let err = block_on(svc.initiate_auth(&req))
            .err()
            .expect("Expected error for missing DefineAuthChallenge trigger");
        let err_str = format!("{err}");
        assert!(
            err_str.contains("DefineAuthChallenge"),
            "Expected DefineAuthChallenge error, got: {err_str}"
        );
    }

    #[test]
    fn custom_challenge_response_fails_without_delivery_context() {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(crate::state::CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        let svc = CognitoService::new(state.clone());

        // Create pool, client, and user so we get past user lookup
        let req = make_req("CreateUserPool", r#"{"PoolName": "ccpool"}"#);
        let resp = svc.create_user_pool(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        let pool_id = resp_body["UserPool"]["Id"].as_str().unwrap().to_string();

        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "ClientName": "ccclient",
            "ExplicitAuthFlows": ["ALLOW_CUSTOM_AUTH"]
        }))
        .unwrap();
        let req = make_req("CreateUserPoolClient", &body);
        let resp = svc.create_user_pool_client(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        let client_id = resp_body["UserPoolClient"]["ClientId"]
            .as_str()
            .unwrap()
            .to_string();

        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "Username": "ccuser",
            "TemporaryPassword": "TempP@ss1!"
        }))
        .unwrap();
        let req = make_req("AdminCreateUser", &body);
        block_on(svc.admin_create_user(&req)).unwrap();

        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "Username": "ccuser",
            "Password": "P@ssw0rd!",
            "Permanent": true
        }))
        .unwrap();
        let req = make_req("AdminSetUserPassword", &body);
        svc.admin_set_user_password(&req).unwrap();

        // Manually insert a CUSTOM_CHALLENGE session
        let session_token = "test-session-123".to_string();
        {
            let mut st = state.write();
            st.sessions.insert(
                session_token.clone(),
                SessionData {
                    user_pool_id: pool_id,
                    username: "ccuser".to_string(),
                    client_id: client_id.clone(),
                    challenge_name: "CUSTOM_CHALLENGE".to_string(),
                    challenge_results: vec![],
                    challenge_metadata: None,
                },
            );
        }

        let body = serde_json::to_string(&json!({
            "ClientId": client_id,
            "ChallengeName": "CUSTOM_CHALLENGE",
            "Session": session_token,
            "ChallengeResponses": {"ANSWER": "my-answer"}
        }))
        .unwrap();
        let req = make_req("RespondToAuthChallenge", &body);
        let err = block_on(svc.respond_to_auth_challenge(&req))
            .err()
            .expect("Expected error for missing VerifyAuthChallengeResponse trigger");
        let err_str = format!("{err}");
        assert!(
            err_str.contains("InvalidLambdaResponseException")
                || err_str.contains("VerifyAuthChallengeResponse"),
            "Expected Lambda error, got: {err_str}"
        );
    }

    #[test]
    fn custom_challenge_response_requires_answer() {
        let state = std::sync::Arc::new(parking_lot::RwLock::new(crate::state::CognitoState::new(
            "123456789012",
            "us-east-1",
        )));
        let svc = CognitoService::new(state.clone());

        // Create pool and client so we have valid IDs
        let req = make_req("CreateUserPool", r#"{"PoolName": "anspool"}"#);
        let resp = svc.create_user_pool(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        let pool_id = resp_body["UserPool"]["Id"].as_str().unwrap().to_string();

        let body = serde_json::to_string(&json!({
            "UserPoolId": pool_id,
            "ClientName": "ansclient",
            "ExplicitAuthFlows": ["ALLOW_CUSTOM_AUTH"]
        }))
        .unwrap();
        let req = make_req("CreateUserPoolClient", &body);
        let resp = svc.create_user_pool_client(&req).unwrap();
        let resp_body: Value = serde_json::from_slice(&resp.body).unwrap();
        let client_id = resp_body["UserPoolClient"]["ClientId"]
            .as_str()
            .unwrap()
            .to_string();

        let session_token = "test-session-456".to_string();
        {
            let mut st = state.write();
            st.sessions.insert(
                session_token.clone(),
                SessionData {
                    user_pool_id: pool_id,
                    username: "testuser".to_string(),
                    client_id: client_id.clone(),
                    challenge_name: "CUSTOM_CHALLENGE".to_string(),
                    challenge_results: vec![],
                    challenge_metadata: None,
                },
            );
        }

        // Missing ANSWER
        let body = serde_json::to_string(&json!({
            "ClientId": client_id,
            "ChallengeName": "CUSTOM_CHALLENGE",
            "Session": session_token,
            "ChallengeResponses": {}
        }))
        .unwrap();
        let req = make_req("RespondToAuthChallenge", &body);
        let err = block_on(svc.respond_to_auth_challenge(&req))
            .err()
            .expect("Expected error for missing ANSWER");
        let err_str = format!("{err}");
        assert!(
            err_str.contains("ANSWER"),
            "Expected ANSWER required error, got: {err_str}"
        );
    }

    #[test]
    fn session_data_stores_challenge_results() {
        let cr = ChallengeResult {
            challenge_name: "CUSTOM_CHALLENGE".to_string(),
            challenge_result: true,
            challenge_metadata: None,
        };
        let session = SessionData {
            user_pool_id: "pool-1".to_string(),
            username: "user1".to_string(),
            client_id: "client-1".to_string(),
            challenge_name: "CUSTOM_CHALLENGE".to_string(),
            challenge_results: vec![cr.clone()],
            challenge_metadata: Some("meta".to_string()),
        };
        assert_eq!(session.challenge_results.len(), 1);
        assert!(session.challenge_results[0].challenge_result);
        assert_eq!(session.challenge_metadata.as_deref(), Some("meta"));
    }
}
