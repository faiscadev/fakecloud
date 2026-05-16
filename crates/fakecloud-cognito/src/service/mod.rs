mod auth;
mod branding;
mod config;
mod groups;
mod identity_pools;
mod identity_providers;
mod legacy;
mod mfa;
mod misc;
mod resource_servers;
mod user_pools;
mod users;

pub use identity_pools::CognitoIdentityService;

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use tokio::sync::Mutex as AsyncMutex;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_persistence::SnapshotStore;

use crate::state::{
    AccountRecoverySetting, AdminCreateUserConfig, CognitoSnapshot, Device, EmailConfiguration,
    Group, IdentityProvider, InviteMessageTemplate, PasswordPolicy, RecoveryOption, ResourceServer,
    ResourceServerScope, SchemaAttribute, SharedCognitoState, SignInPolicy, SmsConfiguration,
    StringAttributeConstraints, TokenValidityUnits, User, UserAttribute, UserImportJob, UserPool,
    UserPoolClient, UserPoolDomain, VerificationMessageTemplate, COGNITO_SNAPSHOT_SCHEMA_VERSION,
};
use crate::triggers::CognitoDeliveryContext;

pub struct CognitoService {
    pub(crate) state: SharedCognitoState,
    delivery_ctx: Option<CognitoDeliveryContext>,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: Arc<AsyncMutex<()>>,
}

impl CognitoService {
    pub fn new(state: SharedCognitoState) -> Self {
        Self {
            state,
            delivery_ctx: None,
            snapshot_store: None,
            snapshot_lock: Arc::new(AsyncMutex::new(())),
        }
    }

    /// Attach a delivery context for Lambda trigger invocation.
    pub fn with_delivery(mut self, ctx: CognitoDeliveryContext) -> Self {
        self.delivery_ctx = Some(ctx);
        self
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    /// Dispatch a Cognito verification email. Routes through the
    /// CustomEmailSender Lambda when configured on the pool; otherwise
    /// invokes the CustomMessage trigger (synchronously, so its response
    /// can override subject+body) and then sends through the wired SES
    /// dispatcher. No-op when neither is wired. Returns immediately —
    /// all work happens in a spawned task.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn dispatch_verification_email(
        &self,
        pool_id: &str,
        client_id: Option<&str>,
        username: &str,
        user_attributes: &[crate::state::UserAttribute],
        to_email: &str,
        code: &str,
        custom_message_trigger: crate::triggers::TriggerSource,
        custom_sender_trigger: crate::triggers::TriggerSource,
        region: &str,
        account_id: &str,
    ) {
        let Some(ref ctx) = self.delivery_ctx else {
            return;
        };

        // Custom sender path takes precedence per AWS behavior.
        if let Some(function_arn) =
            crate::triggers::get_trigger_arn(&self.state, pool_id, custom_sender_trigger)
        {
            let event = crate::triggers::build_custom_sender_event(
                custom_sender_trigger,
                pool_id,
                client_id,
                username,
                user_attributes,
                code,
                region,
                account_id,
            );
            crate::triggers::invoke_trigger_fire_and_forget(ctx, function_arn, event);
            return;
        }

        let Some(ref dispatcher) = ctx.email else {
            return;
        };
        let custom_message_arn =
            crate::triggers::get_trigger_arn(&self.state, pool_id, custom_message_trigger);
        let pool_settings = {
            let accounts = self.state.read();
            accounts
                .default_ref()
                .user_pools
                .get(pool_id)
                .map(|p| {
                    let from = p
                        .email_configuration
                        .as_ref()
                        .and_then(|c| c.from_email_address.clone());
                    let template = p.verification_message_template.clone();
                    (from, template)
                })
                .unwrap_or((None, None))
        };
        let (from_addr, vmt) = pool_settings;
        let (subject_tpl_default, msg_tpl_default) = vmt
            .map(|t| (t.email_subject, t.email_message))
            .unwrap_or((None, None));

        let dispatcher = dispatcher.clone();
        let ctx = ctx.clone();
        let username_owned = username.to_string();
        let client_id_owned = client_id.map(|s| s.to_string());
        let pool_id_owned = pool_id.to_string();
        let region_owned = region.to_string();
        let account_id_owned = account_id.to_string();
        let user_attrs_owned: Vec<crate::state::UserAttribute> = user_attributes.to_vec();
        let to_email_owned = to_email.to_string();
        let code_owned = code.to_string();
        tokio::spawn(async move {
            let mut subject_tpl = subject_tpl_default;
            let mut msg_tpl = msg_tpl_default;
            if let Some(function_arn) = custom_message_arn {
                let mut event = crate::triggers::build_trigger_event(
                    custom_message_trigger,
                    &pool_id_owned,
                    client_id_owned.as_deref(),
                    &username_owned,
                    &user_attrs_owned,
                    &region_owned,
                    &account_id_owned,
                );
                event["request"]["codeParameter"] = serde_json::json!("{####}");
                event["request"]["usernameParameter"] = serde_json::json!(username_owned);
                if let Some(resp) =
                    crate::triggers::invoke_trigger(&ctx, &function_arn, &event).await
                {
                    if let Some(s) = resp["response"]["emailSubject"].as_str() {
                        subject_tpl = Some(s.to_string());
                    }
                    if let Some(m) = resp["response"]["emailMessage"].as_str() {
                        msg_tpl = Some(m.to_string());
                    }
                }
            }
            let rendered = crate::triggers::render_verification_email(
                from_addr.as_deref(),
                subject_tpl.as_deref(),
                msg_tpl.as_deref(),
                &username_owned,
                &code_owned,
            );
            dispatcher.send_email(
                &account_id_owned,
                &rendered.from,
                &to_email_owned,
                &rendered.subject,
                &rendered.body_text,
                rendered.body_html.as_deref(),
            );
        });
    }

    /// Dispatch a Cognito verification SMS. Routes through the
    /// CustomSMSSender Lambda when configured, otherwise through the
    /// wired SNS dispatcher. No-op when neither is wired.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn dispatch_verification_sms(
        &self,
        pool_id: &str,
        client_id: Option<&str>,
        username: &str,
        user_attributes: &[crate::state::UserAttribute],
        phone_number: &str,
        code: &str,
        custom_sender_trigger: crate::triggers::TriggerSource,
        region: &str,
        account_id: &str,
    ) {
        let Some(ref ctx) = self.delivery_ctx else {
            return;
        };

        if let Some(function_arn) =
            crate::triggers::get_trigger_arn(&self.state, pool_id, custom_sender_trigger)
        {
            let event = crate::triggers::build_custom_sender_event(
                custom_sender_trigger,
                pool_id,
                client_id,
                username,
                user_attributes,
                code,
                region,
                account_id,
            );
            crate::triggers::invoke_trigger_fire_and_forget(ctx, function_arn, event);
            return;
        }

        let Some(ref dispatcher) = ctx.sms else {
            return;
        };
        let sms_template = {
            let accounts = self.state.read();
            accounts
                .default_ref()
                .user_pools
                .get(pool_id)
                .and_then(|p| {
                    p.verification_message_template
                        .as_ref()
                        .and_then(|t| t.sms_message.clone())
                })
        };
        let message =
            crate::triggers::render_verification_sms(sms_template.as_deref(), username, code);
        let dispatcher = dispatcher.clone();
        let acct = account_id.to_string();
        let phone = phone_number.to_string();
        tokio::spawn(async move {
            dispatcher.send_sms(&acct, &phone, &message);
        });
    }

    /// Persist current state as a snapshot. Held across the
    /// clone-serialize-write sequence to prevent stale-last writes,
    /// with serde + file I/O offloaded to the blocking pool.
    async fn save_snapshot(&self) {
        let Some(store) = self.snapshot_store.clone() else {
            return;
        };
        let _guard = self.snapshot_lock.lock().await;
        let snapshot = CognitoSnapshot {
            schema_version: COGNITO_SNAPSHOT_SCHEMA_VERSION,
            accounts: Some(self.state.read().clone()),
            state: None,
        };
        let join = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            let bytes = serde_json::to_vec(&snapshot)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))?;
            store.save(&bytes)
        })
        .await;
        match join {
            Ok(Ok(())) => {}
            Ok(Err(err)) => tracing::error!(%err, "failed to write cognito snapshot"),
            Err(err) => tracing::error!(%err, "cognito snapshot task panicked"),
        }
    }
}

/// Actions that mutate persistent state. List excludes pure reads
/// (`Describe*`, `List*`, `Get*`, `AdminGet*`, `AdminList*`) and any
/// other action that doesn't change state stored in `CognitoState`.
fn is_mutating_action(action: &str) -> bool {
    !matches!(
        action,
        "DescribeUserPool"
            | "DescribeUserPoolClient"
            | "DescribeUserPoolDomain"
            | "DescribeIdentityProvider"
            | "DescribeResourceServer"
            | "DescribeRiskConfiguration"
            | "DescribeManagedLoginBranding"
            | "DescribeManagedLoginBrandingByClient"
            | "DescribeUserImportJob"
            | "DescribeTerms"
            | "ListUserPools"
            | "ListUserPoolClients"
            | "ListUserPoolClientSecrets"
            | "ListUsers"
            | "ListUsersInGroup"
            | "ListGroups"
            | "ListIdentityProviders"
            | "ListResourceServers"
            | "ListDevices"
            | "ListTagsForResource"
            | "ListUserImportJobs"
            | "ListTerms"
            | "ListWebAuthnCredentials"
            | "AdminGetUser"
            | "AdminGetDevice"
            | "AdminListDevices"
            | "AdminListGroupsForUser"
            | "AdminListUserAuthEvents"
            | "GetUser"
            | "GetGroup"
            | "GetDevice"
            | "GetUserPoolMfaConfig"
            | "GetUserAuthFactors"
            | "GetUICustomization"
            | "GetLogDeliveryConfiguration"
            | "GetSigningCertificate"
            | "GetCSVHeader"
            | "GetIdentityProviderByIdentifier"
    )
}

#[async_trait]
impl AwsService for CognitoService {
    fn service_name(&self) -> &str {
        "cognito-idp"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating_action(req.action.as_str());
        let result = match req.action.as_str() {
            "CreateUserPool" => self.create_user_pool(&req).await,
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
            "AdminSetUserSettings" => self.admin_set_user_settings(&req),
            "SetUserSettings" => self.set_user_settings(&req),
            "AdminDisableProviderForUser" => self.admin_disable_provider_for_user(&req),
            "AdminLinkProviderForUser" => self.admin_link_provider_for_user(&req),
            "AdminListUserAuthEvents" => self.admin_list_user_auth_events(&req),
            "AdminUpdateAuthEventFeedback" => self.admin_update_auth_event_feedback(&req),
            "UpdateAuthEventFeedback" => self.update_auth_event_feedback(&req),
            "StartUserImportJob" => self.start_user_import_job(&req),
            "StopUserImportJob" => self.stop_user_import_job(&req),
            "GetUICustomization" => self.get_ui_customization(&req),
            "SetUICustomization" => self.set_ui_customization(&req),
            "GetLogDeliveryConfiguration" => self.get_log_delivery_configuration(&req),
            "SetLogDeliveryConfiguration" => self.set_log_delivery_configuration(&req),
            "DescribeRiskConfiguration" => self.describe_risk_configuration(&req),
            "SetRiskConfiguration" => self.set_risk_configuration(&req),
            "CreateManagedLoginBranding" => self.create_managed_login_branding(&req),
            "DeleteManagedLoginBranding" => self.delete_managed_login_branding(&req),
            "DescribeManagedLoginBranding" => self.describe_managed_login_branding(&req),
            "DescribeManagedLoginBrandingByClient" => {
                self.describe_managed_login_branding_by_client(&req)
            }
            "UpdateManagedLoginBranding" => self.update_managed_login_branding(&req),
            "CreateTerms" => self.create_terms(&req),
            "DeleteTerms" => self.delete_terms(&req),
            "DescribeTerms" => self.describe_terms(&req),
            "ListTerms" => self.list_terms(&req),
            "UpdateTerms" => self.update_terms(&req),
            "StartWebAuthnRegistration" => self.start_web_authn_registration(&req),
            "CompleteWebAuthnRegistration" => self.complete_web_authn_registration(&req),
            "DeleteWebAuthnCredential" => self.delete_web_authn_credential(&req),
            "ListWebAuthnCredentials" => self.list_web_authn_credentials(&req),
            _ => Err(AwsServiceError::action_not_implemented(
                "cognito-idp",
                &req.action,
            )),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            self.save_snapshot().await;
        }
        result
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
            "AdminSetUserSettings",
            "SetUserSettings",
            "AdminDisableProviderForUser",
            "AdminLinkProviderForUser",
            "AdminListUserAuthEvents",
            "AdminUpdateAuthEventFeedback",
            "UpdateAuthEventFeedback",
            "StartUserImportJob",
            "StopUserImportJob",
            "GetUICustomization",
            "SetUICustomization",
            "GetLogDeliveryConfiguration",
            "SetLogDeliveryConfiguration",
            "DescribeRiskConfiguration",
            "SetRiskConfiguration",
            "CreateManagedLoginBranding",
            "DeleteManagedLoginBranding",
            "DescribeManagedLoginBranding",
            "DescribeManagedLoginBrandingByClient",
            "UpdateManagedLoginBranding",
            "CreateTerms",
            "DeleteTerms",
            "DescribeTerms",
            "ListTerms",
            "UpdateTerms",
            "StartWebAuthnRegistration",
            "CompleteWebAuthnRegistration",
            "DeleteWebAuthnCredential",
            "ListWebAuthnCredentials",
        ]
    }
}

/// Confirm that ``pool_id`` refers to a known user pool, returning the standard
/// ``ResourceNotFoundException`` otherwise. Most ``CognitoService`` operations
/// take a ``UserPoolId`` and validate it before touching any other state, so
/// this helper collapses what would otherwise be the same 7-line guard written
/// at every call site.
fn ensure_user_pool_exists(
    state: &crate::state::CognitoState,
    pool_id: &str,
) -> Result<(), AwsServiceError> {
    if state.user_pools.contains_key(pool_id) {
        Ok(())
    } else {
        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ResourceNotFoundException",
            format!("User pool {pool_id} does not exist."),
        ))
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

fn parse_refresh_token_rotation(val: &Value) -> Option<crate::state::RefreshTokenRotationConfig> {
    if !val.is_object() {
        return None;
    }
    let feature = val["Feature"].as_str()?.to_string();
    Some(crate::state::RefreshTokenRotationConfig {
        feature,
        retry_grace_period_seconds: val["RetryGracePeriodSeconds"].as_i64(),
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
    if let Some(ref rot) = client.refresh_token_rotation {
        let mut r = json!({"Feature": rot.feature});
        if let Some(g) = rot.retry_grace_period_seconds {
            r["RetryGracePeriodSeconds"] = json!(g);
        }
        obj["RefreshTokenRotation"] = r;
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

fn default_sign_in_policy() -> SignInPolicy {
    SignInPolicy {
        allowed_first_auth_factors: vec!["PASSWORD".to_string()],
    }
}

pub(super) fn parse_sign_in_policy(val: &Value) -> SignInPolicy {
    if !val.is_object() {
        return default_sign_in_policy();
    }
    let factors = val["AllowedFirstAuthFactors"]
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if factors.is_empty() {
        default_sign_in_policy()
    } else {
        SignInPolicy {
            allowed_first_auth_factors: factors,
        }
    }
}

pub(super) fn parse_verification_message_template(
    val: &Value,
) -> Option<VerificationMessageTemplate> {
    if !val.is_object() {
        return None;
    }
    Some(VerificationMessageTemplate {
        default_email_option: val["DefaultEmailOption"]
            .as_str()
            .unwrap_or("CONFIRM_WITH_CODE")
            .to_string(),
        email_message: val["EmailMessage"].as_str().map(|s| s.to_string()),
        email_subject: val["EmailSubject"].as_str().map(|s| s.to_string()),
        email_message_by_link: val["EmailMessageByLink"].as_str().map(|s| s.to_string()),
        email_subject_by_link: val["EmailSubjectByLink"].as_str().map(|s| s.to_string()),
        sms_message: val["SmsMessage"].as_str().map(|s| s.to_string()),
    })
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

fn parse_tags(val: &Value) -> std::collections::BTreeMap<String, String> {
    let mut tags = std::collections::BTreeMap::new();
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

fn parse_string_map(val: &Value) -> BTreeMap<String, String> {
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
            },
            "SignInPolicy": {
                "AllowedFirstAuthFactors": pool.policies.sign_in_policy.allowed_first_auth_factors,
            },
        },
        "AutoVerifiedAttributes": pool.auto_verified_attributes,
        "MfaConfiguration": pool.mfa_configuration,
        "EstimatedNumberOfUsers": pool.estimated_number_of_users,
        "UserPoolTags": pool.user_pool_tags,
        "UserPoolTier": pool.user_pool_tier,
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

    if let Some(ref v) = pool.email_verification_message {
        obj["EmailVerificationMessage"] = json!(v);
    }
    if let Some(ref v) = pool.email_verification_subject {
        obj["EmailVerificationSubject"] = json!(v);
    }
    if let Some(ref v) = pool.sms_verification_message {
        obj["SmsVerificationMessage"] = json!(v);
    }
    if let Some(ref v) = pool.sms_authentication_message {
        obj["SmsAuthenticationMessage"] = json!(v);
    }
    if let Some(ref v) = pool.device_configuration {
        obj["DeviceConfiguration"] = v.clone();
    }
    if let Some(ref v) = pool.user_attribute_update_settings {
        obj["UserAttributeUpdateSettings"] = v.clone();
    }
    if let Some(ref v) = pool.user_pool_add_ons {
        obj["UserPoolAddOns"] = v.clone();
    }
    if let Some(ref v) = pool.username_configuration {
        obj["UsernameConfiguration"] = v.clone();
    }
    if let Some(ref ua) = pool.username_attributes {
        obj["UsernameAttributes"] = json!(ua);
    }
    if let Some(ref aa) = pool.alias_attributes {
        obj["AliasAttributes"] = json!(aa);
    }
    if let Some(ref lc) = pool.lambda_config {
        obj["LambdaConfig"] = lc.clone();
    }
    {
        let mut email = json!({
            "EmailSendingAccount": "COGNITO_DEFAULT",
        });
        if let Some(ref ec) = pool.email_configuration {
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
        // Real Cognito always emits SnsRegion, defaulting to the pool's
        // home region when the caller doesn't override it.
        let region = sc.sns_region.clone().unwrap_or_else(|| {
            pool.arn
                .split(':')
                .nth(3)
                .unwrap_or("us-east-1")
                .to_string()
        });
        sms["SnsRegion"] = json!(region);
        obj["SmsConfiguration"] = sms;
    }
    {
        // Real Cognito always emits AdminCreateUserConfig with
        // AllowAdminCreateUserOnly=false and UnusedAccountValidityDays=7
        // as documented defaults; honour caller-provided values.
        let mut admin = json!({
            "AllowAdminCreateUserOnly": false,
            "UnusedAccountValidityDays": 7,
        });
        if let Some(ref ac) = pool.admin_create_user_config {
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
        }
        obj["AdminCreateUserConfig"] = admin;
    }
    {
        let mechanisms: Vec<Value> = match pool.account_recovery_setting {
            Some(ref ars) if !ars.recovery_mechanisms.is_empty() => ars
                .recovery_mechanisms
                .iter()
                .map(|r| {
                    json!({
                        "Name": r.name,
                        "Priority": r.priority,
                    })
                })
                .collect(),
            _ => vec![json!({ "Name": "verified_email", "Priority": 1 })],
        };
        obj["AccountRecoverySetting"] = json!({ "RecoveryMechanisms": mechanisms });
    }
    {
        let mut vmt = json!({
            "DefaultEmailOption": "CONFIRM_WITH_CODE",
        });
        if let Some(ref t) = pool.verification_message_template {
            vmt["DefaultEmailOption"] = json!(t.default_email_option);
            if let Some(ref v) = t.email_message {
                vmt["EmailMessage"] = json!(v);
            }
            if let Some(ref v) = t.email_subject {
                vmt["EmailSubject"] = json!(v);
            }
            if let Some(ref v) = t.email_message_by_link {
                vmt["EmailMessageByLink"] = json!(v);
            }
            if let Some(ref v) = t.email_subject_by_link {
                vmt["EmailSubjectByLink"] = json!(v);
            }
            if let Some(ref v) = t.sms_message {
                vmt["SmsMessage"] = json!(v);
            }
        }
        obj["VerificationMessageTemplate"] = vmt;
    }
    obj["DeletionProtection"] = json!(pool.deletion_protection.as_deref().unwrap_or("INACTIVE"));

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

/// Generate Cognito ID/Access/Refresh tokens. `signing` is the pool's
/// PKCS#8 PEM + JWKS kid; production callers always pass `Some` because
/// `CreateUserPool` allocates a keypair eagerly and
/// `ensure_pool_signing_key` backfills any legacy snapshot. When `None`
/// (only reachable from in-process unit tests that intentionally skip
/// keygen for speed), we synthesize a one-shot keypair so the resulting
/// JWT still has a real RS256 signature — never a placeholder.
fn generate_tokens(
    pool_id: &str,
    client_id: &str,
    sub: &str,
    username: &str,
    region: &str,
    signing: Option<(&str, &str)>,
) -> TokenSet {
    generate_tokens_with_scope(
        pool_id, client_id, sub, username, region, signing, None, None,
    )
}

/// Like [`generate_tokens`] but lets callers stamp custom `scope` and
/// `nonce` claims into the issued tokens — used by the
/// `authorization_code` grant which carries the scopes and nonce the
/// user originally consented to at `/oauth2/authorize`.
#[allow(clippy::too_many_arguments)]
fn generate_tokens_with_scope(
    pool_id: &str,
    client_id: &str,
    sub: &str,
    username: &str,
    region: &str,
    signing: Option<(&str, &str)>,
    scope: Option<&str>,
    nonce: Option<&str>,
) -> TokenSet {
    generate_tokens_with_overrides(
        pool_id, client_id, sub, username, region, signing, scope, nonce, None,
    )
}

/// Like [`generate_tokens_with_scope`] but accepts a
/// `claimsAndScopeOverrideDetails`-shaped JSON value from a
/// PreTokenGeneration trigger response.
///
/// - `idTokenGeneration.claimsToAddOrOverride` -> merged into id token
/// - `idTokenGeneration.claimsToSuppress` -> removed from id token
/// - `accessTokenGeneration.claimsToAddOrOverride` -> merged into access token
/// - `groupOverrideDetails.groupsToOverride` -> set as `cognito:groups`
#[allow(clippy::too_many_arguments)]
fn generate_tokens_with_overrides(
    pool_id: &str,
    client_id: &str,
    sub: &str,
    username: &str,
    region: &str,
    signing: Option<(&str, &str)>,
    scope: Option<&str>,
    nonce: Option<&str>,
    overrides: Option<&Value>,
) -> TokenSet {
    let b64url = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let now = Utc::now().timestamp();
    let jti = Uuid::new_v4().to_string();
    let iss = format!("https://cognito-idp.{region}.amazonaws.com/{pool_id}");

    let owned_signing = signing
        .map(|(p, k)| (p.to_string(), k.to_string()))
        .unwrap_or_else(|| {
            let key = crate::jwt::generate_pool_signing_key();
            (key.private_key_pem, key.kid)
        });
    let pem = owned_signing.0.as_str();
    let kid = owned_signing.1.as_str();

    let id_header = json!({"kid": kid, "alg": "RS256", "typ": "JWT"});
    let mut id_payload = json!({
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
    if let Some(n) = nonce {
        id_payload
            .as_object_mut()
            .expect("id_payload is always a JSON object")
            .insert("nonce".to_string(), Value::String(n.to_string()));
    }

    let access_jti = Uuid::new_v4().to_string();
    let access_header = json!({"kid": kid, "alg": "RS256", "typ": "JWT"});
    let access_scope = scope
        .filter(|s| !s.is_empty())
        .unwrap_or("aws.cognito.signin.user.admin");
    let mut access_payload = json!({
        "sub": sub,
        "iss": iss,
        "client_id": client_id,
        "token_use": "access",
        "scope": access_scope,
        "jti": access_jti,
        "exp": now + 3600,
        "iat": now,
    });

    // PreTokenGeneration trigger merge. Schema (v2):
    //   claimsAndScopeOverrideDetails: {
    //     idTokenGeneration:    { claimsToAddOrOverride, claimsToSuppress },
    //     accessTokenGeneration:{ claimsToAddOrOverride, claimsToSuppress, scopesToAdd, scopesToSuppress },
    //     groupOverrideDetails: { groupsToOverride, preferredRole, iamRolesToOverride },
    //   }
    // Real Cognito also accepts the v1 flat `claimsOverrideDetails` shape.
    if let Some(ov) = overrides {
        let v2 = &ov["claimsAndScopeOverrideDetails"];
        let v1 = &ov["claimsOverrideDetails"];
        let id_block = if !v2.is_null() {
            &v2["idTokenGeneration"]
        } else {
            v1
        };
        let access_block = if !v2.is_null() {
            &v2["accessTokenGeneration"]
        } else {
            v1
        };
        let group_block = if !v2.is_null() {
            &v2["groupOverrideDetails"]
        } else {
            &v1["groupOverrideDetails"]
        };
        apply_claim_overrides(id_payload.as_object_mut().unwrap(), id_block);
        apply_claim_overrides(access_payload.as_object_mut().unwrap(), access_block);
        if let Some(arr) = group_block["groupsToOverride"].as_array() {
            let groups: Vec<Value> = arr.iter().filter(|v| v.is_string()).cloned().collect();
            if !groups.is_empty() {
                id_payload["cognito:groups"] = Value::Array(groups.clone());
                access_payload["cognito:groups"] = Value::Array(groups);
            }
        }
        if let Some(arr) = access_block["scopesToAdd"].as_array() {
            let extra: Vec<String> = arr
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            if !extra.is_empty() {
                let merged = format!("{} {}", access_scope, extra.join(" "));
                access_payload["scope"] = Value::String(merged);
            }
        }
    }

    let id_token = sign_jwt(&id_header, &id_payload, pem);
    let access_token = sign_jwt(&access_header, &access_payload, pem);

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

/// Merge a PreTokenGeneration claim block into a token payload.
/// `claimsToAddOrOverride`: object whose entries are inserted (overwriting).
/// `claimsToSuppress`: array of claim names to delete after the merge.
fn apply_claim_overrides(map: &mut serde_json::Map<String, Value>, block: &Value) {
    if let Some(adds) = block["claimsToAddOrOverride"].as_object() {
        for (k, v) in adds {
            map.insert(k.clone(), v.clone());
        }
    }
    if let Some(suppress) = block["claimsToSuppress"].as_array() {
        for v in suppress {
            if let Some(k) = v.as_str() {
                map.remove(k);
            }
        }
    }
}

/// Sign a Cognito-shaped JWT with the pool's PKCS#8 private key.
/// Always RS256 — there is no placeholder fallback, callers must pass
/// a valid pool PEM.
fn sign_jwt(header: &Value, payload: &Value, private_key_pem: &str) -> String {
    crate::jwt::sign_rs256(header, payload, private_key_pem)
        .expect("pool PEM must be a valid PKCS#8 RSA private key")
}

/// Look up a pool's signing key, generating one off the runtime thread if
/// missing. Returns `(pkcs8_pem, kid)` for callers that need to sign a
/// JWT or render a JWKS document. The expensive RSA-2048 keygen only
/// runs once per pool — subsequent calls hit the cache.
///
/// Pre-Y1 snapshots may have a stored PEM with no `kid`, or neither;
/// we fill in whatever's missing so the JWKS document and the JWT
/// header always agree on a stable kid derived from the public key.
pub async fn ensure_pool_signing_key(
    state: &SharedCognitoState,
    pool_id: &str,
) -> Option<(String, String)> {
    {
        let mas = state.read();
        for (_, account) in mas.iter() {
            if let Some(pool) = account.user_pools.get(pool_id) {
                if let (Some(p), Some(k)) = (&pool.signing_key_pem, &pool.signing_kid) {
                    return Some((p.clone(), k.clone()));
                }
                break;
            }
        }
    }
    let generated = tokio::task::spawn_blocking(crate::jwt::generate_pool_signing_key)
        .await
        .ok()?;
    let mut mas = state.write();
    let mut result = None;
    for (_, account) in mas.iter_mut() {
        if let Some(pool) = account.user_pools.get_mut(pool_id) {
            if pool.signing_key_pem.is_none() {
                pool.signing_key_pem = Some(generated.private_key_pem.clone());
                pool.signing_kid = Some(generated.kid.clone());
            } else if pool.signing_kid.is_none() {
                // PEM was carried forward from an older snapshot but
                // the kid wasn't — re-derive it from the public half
                // so JWKS and JWT header stay consistent.
                if let Some(pem) = pool.signing_key_pem.as_deref() {
                    pool.signing_kid = derive_kid_from_pem(pem);
                }
            }
            if let (Some(p), Some(k)) = (&pool.signing_key_pem, &pool.signing_kid) {
                result = Some((p.clone(), k.clone()));
            }
            break;
        }
    }
    result
}

/// Re-derive a pool's `kid` from a stored PEM. Used to backfill
/// older snapshots that predate the deterministic kid scheme.
fn derive_kid_from_pem(pem: &str) -> Option<String> {
    use rsa::pkcs8::DecodePrivateKey;
    use rsa::{RsaPrivateKey, RsaPublicKey};
    let private_key = RsaPrivateKey::from_pkcs8_pem(pem).ok()?;
    let public_key = RsaPublicKey::from(&private_key);
    Some(crate::jwt::compute_kid(&public_key))
}

/// JWKS document for a pool (`{"keys": [<jwk>]}`). Triggers lazy keypair
/// generation when the pool was created before any sign call ran.
pub async fn pool_jwks_document(state: &SharedCognitoState, pool_id: &str) -> Option<Value> {
    let (pem, kid) = ensure_pool_signing_key(state, pool_id).await?;
    Some(crate::jwt::jwks_document(&pem, &kid))
}

/// Whether a pool with `pool_id` exists in any account, plus the first
/// `UserPoolDomain` (if any) attached to it. Used by the OIDC discovery
/// route so it can emit OAuth endpoints only when a domain is configured
/// — matching real Cognito, which omits those endpoints from discovery
/// until the pool has a hosted-UI domain.
pub fn pool_existence_and_domain(
    state: &SharedCognitoState,
    pool_id: &str,
) -> (bool, Option<String>) {
    let mas = state.read();
    let mut exists = false;
    let mut domain = None;
    for (_, account) in mas.iter() {
        if account.user_pools.contains_key(pool_id) {
            exists = true;
            // First domain wins; pools rarely have more than one.
            for (name, d) in account.domains.iter() {
                if d.user_pool_id == pool_id {
                    domain = Some(name.clone());
                    break;
                }
            }
            break;
        }
    }
    (exists, domain)
}

/// Cognito-shaped OpenID-Connect discovery document for a pool. Mirrors
/// the document AWS publishes at
/// `https://cognito-idp.<region>.amazonaws.com/<pool>/.well-known/openid-configuration`.
/// `base_url` is the externally-reachable origin (scheme+host+port) that
/// clients use to reach fakecloud, so the URLs we hand back resolve.
///
/// `pool_domain` is the hosted-UI domain prefix attached to the pool (if
/// any). Real Cognito omits the OAuth2 endpoints from this document until
/// a domain has been attached — those URLs literally don't resolve before
/// then — so we mirror the same behaviour.
pub fn oidc_discovery_document(
    pool_id: &str,
    region: &str,
    base_url: &str,
    pool_domain: Option<&str>,
) -> Value {
    let issuer = format!("https://cognito-idp.{region}.amazonaws.com/{pool_id}");
    let trimmed = base_url.trim_end_matches('/');
    let mut doc = serde_json::json!({
        "issuer": issuer,
        "jwks_uri": format!("{trimmed}/{pool_id}/.well-known/jwks.json"),
        "response_types_supported": ["code", "token"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
        "token_endpoint_auth_methods_supported": [
            "client_secret_basic",
            "client_secret_post",
            "none",
        ],
        "scopes_supported": ["openid", "email", "phone", "profile", "aws.cognito.signin.user.admin"],
        "claims_supported": [
            "sub",
            "iss",
            "aud",
            "name",
            "email",
            "email_verified",
            "phone_number",
            "phone_number_verified",
            "cognito:username",
            "cognito:groups",
            "auth_time",
            "exp",
            "iat",
            "token_use",
            "jti",
        ],
        "code_challenge_methods_supported": ["S256", "plain"],
        "grant_types_supported": ["authorization_code", "implicit", "refresh_token", "client_credentials"],
    });
    if pool_domain.is_some() {
        // OAuth2 endpoints are served by fakecloud at the same base URL
        // as everything else. AWS Cognito routes them through
        // `https://<domain>.auth.<region>.amazoncognito.com/...`, but
        // those hosts don't resolve in a fakecloud test environment, so
        // pointing clients at our local `<base_url>/oauth2/...` keeps
        // discovery -> redeem -> userinfo flows actually working.
        if let Some(map) = doc.as_object_mut() {
            map.insert(
                "authorization_endpoint".into(),
                serde_json::Value::String(format!("{trimmed}/oauth2/authorize")),
            );
            map.insert(
                "token_endpoint".into(),
                serde_json::Value::String(format!("{trimmed}/oauth2/token")),
            );
            map.insert(
                "userinfo_endpoint".into(),
                serde_json::Value::String(format!("{trimmed}/oauth2/userInfo")),
            );
            map.insert(
                "revocation_endpoint".into(),
                serde_json::Value::String(format!("{trimmed}/oauth2/revoke")),
            );
        }
    }
    doc
}

/// Result of `/oauth2/token` handling. The HTTP wrapper translates
/// `Err(OAuthTokenError)` into the OAuth2-shaped JSON `{"error": ...}`
/// body with the appropriate HTTP status (400 for client errors, 401
/// for auth failures).
#[derive(Debug)]
pub enum OAuthTokenError {
    InvalidRequest(&'static str),
    UnsupportedGrantType,
    InvalidClient,
    InvalidGrant,
    /// The client is recognised but isn't allowed to use the requested
    /// grant_type (RFC 6749 §5.2).
    UnauthorizedClient,
    /// The requested `scope` is not a subset of what the client is
    /// allowed (RFC 6749 §5.2).
    InvalidScope,
}

impl OAuthTokenError {
    pub fn status_code(&self) -> u16 {
        match self {
            OAuthTokenError::InvalidClient => 401,
            _ => 400,
        }
    }

    pub fn as_oauth_code(&self) -> &'static str {
        match self {
            OAuthTokenError::InvalidRequest(_) => "invalid_request",
            OAuthTokenError::UnsupportedGrantType => "unsupported_grant_type",
            OAuthTokenError::InvalidClient => "invalid_client",
            OAuthTokenError::InvalidGrant => "invalid_grant",
            OAuthTokenError::UnauthorizedClient => "unauthorized_client",
            OAuthTokenError::InvalidScope => "invalid_scope",
        }
    }

    pub fn description(&self) -> Option<&'static str> {
        match self {
            OAuthTokenError::InvalidRequest(msg) => Some(msg),
            _ => None,
        }
    }
}

/// Cognito-shaped `/oauth2/token` response body.
#[derive(Debug)]
pub struct OAuthTokenResponse {
    pub access_token: String,
    pub id_token: Option<String>,
    pub refresh_token: Option<String>,
    pub expires_in: i64,
    pub token_type: String,
}

impl OAuthTokenResponse {
    pub fn to_json(&self) -> Value {
        let mut obj = serde_json::Map::new();
        obj.insert("access_token".into(), json!(self.access_token));
        if let Some(ref id) = self.id_token {
            obj.insert("id_token".into(), json!(id));
        }
        if let Some(ref rt) = self.refresh_token {
            obj.insert("refresh_token".into(), json!(rt));
        }
        obj.insert("expires_in".into(), json!(self.expires_in));
        obj.insert("token_type".into(), json!(self.token_type));
        Value::Object(obj)
    }
}

/// Handle a Cognito `/oauth2/token` POST. Implements all three Cognito
/// Hosted-UI grants — `authorization_code`, `client_credentials`, and
/// `refresh_token` — with optional HTTP Basic client authentication
/// (RFC 6749 §2.3.1) layered on top of form-body credentials.
///
/// `params` is the form-decoded body (Cognito uses
/// `application/x-www-form-urlencoded`). `basic_auth` is the optional
/// `(client_id, client_secret)` parsed from a `Authorization: Basic …`
/// header — when both that header and a form `client_secret`/`client_id`
/// are present, AWS treats the Basic header as authoritative and fails
/// on conflict; we follow the same rule here.
/// `region` is the AWS region used for the JWT `iss` claim.
pub async fn handle_oauth2_token(
    state: &SharedCognitoState,
    params: &BTreeMap<String, String>,
    basic_auth: Option<(&str, &str)>,
    region: &str,
) -> Result<OAuthTokenResponse, OAuthTokenError> {
    let grant_type = params
        .get("grant_type")
        .map(String::as_str)
        .ok_or(OAuthTokenError::InvalidRequest("grant_type is required"))?;

    // Per RFC 6749 §2.3.1 a single client MUST NOT use more than one
    // authentication mechanism in the same request. When a Basic
    // header is supplied AND a form `client_id`/`client_secret`, both
    // must point to the same client and same secret — anything else
    // is an `invalid_client`.
    let form_client_id = params.get("client_id").map(String::as_str);
    let form_client_secret = params.get("client_secret").map(String::as_str);
    let client_id = match (basic_auth, form_client_id) {
        (Some((b_id, _)), Some(f_id)) if b_id != f_id => {
            return Err(OAuthTokenError::InvalidClient);
        }
        (Some((b_id, _)), _) => b_id,
        (None, Some(f_id)) => f_id,
        (None, None) => {
            return Err(OAuthTokenError::InvalidRequest("client_id is required"));
        }
    };
    let supplied_secret = match (basic_auth, form_client_secret) {
        (Some((_, b_sec)), Some(f_sec)) if b_sec != f_sec => {
            return Err(OAuthTokenError::InvalidClient);
        }
        (Some((_, b_sec)), _) => Some(b_sec),
        (None, Some(f_sec)) => Some(f_sec),
        (None, None) => None,
    };

    let (pool_id, stored_secret, allowed_flows, allowed_scopes, oauth_flows_enabled) = {
        let mas = state.read();
        let mut found = None;
        for (_, account) in mas.iter() {
            if let Some(client) = account.user_pool_clients.get(client_id) {
                found = Some((
                    client.user_pool_id.clone(),
                    client.client_secret.clone(),
                    client.allowed_o_auth_flows.clone(),
                    client.allowed_o_auth_scopes.clone(),
                    client.allowed_o_auth_flows_user_pool_client,
                ));
                break;
            }
        }
        found.ok_or(OAuthTokenError::InvalidClient)?
    };

    // SECRET_HASH equivalence: when an app client has a secret the
    // request MUST present it (Basic header or `client_secret` form
    // field). RFC 6749 §2.3.1.
    if let Some(secret) = stored_secret.as_ref() {
        if supplied_secret != Some(secret.as_str()) {
            return Err(OAuthTokenError::InvalidClient);
        }
    }

    match grant_type {
        "authorization_code" => {
            handle_authorization_code_grant(
                state,
                params,
                client_id,
                &pool_id,
                &allowed_flows,
                oauth_flows_enabled,
                region,
            )
            .await
        }
        "refresh_token" => {
            handle_refresh_token_grant(state, params, client_id, &pool_id, region).await
        }
        "client_credentials" => {
            handle_client_credentials_grant(
                state,
                params,
                client_id,
                &pool_id,
                stored_secret.as_deref(),
                &allowed_flows,
                &allowed_scopes,
                oauth_flows_enabled,
                region,
            )
            .await
        }
        _ => Err(OAuthTokenError::UnsupportedGrantType),
    }
}

async fn handle_authorization_code_grant(
    state: &SharedCognitoState,
    params: &BTreeMap<String, String>,
    client_id: &str,
    pool_id: &str,
    allowed_flows: &[String],
    oauth_flows_enabled: bool,
    region: &str,
) -> Result<OAuthTokenResponse, OAuthTokenError> {
    // Cognito only accepts authorization_code on app clients that have
    // explicitly opted into Hosted-UI OAuth flows. When the client
    // declares any flows at all, `code` must be on the list.
    if oauth_flows_enabled
        && !allowed_flows.is_empty()
        && !allowed_flows.iter().any(|f| f.eq_ignore_ascii_case("code"))
    {
        return Err(OAuthTokenError::UnauthorizedClient);
    }
    let code = params
        .get("code")
        .map(String::as_str)
        .filter(|s| !s.is_empty())
        .ok_or(OAuthTokenError::InvalidRequest("code is required"))?;
    let redirect_uri = params
        .get("redirect_uri")
        .map(String::as_str)
        .ok_or(OAuthTokenError::InvalidRequest("redirect_uri is required"))?;
    let code_verifier = params.get("code_verifier").map(String::as_str);

    let consumed = {
        let mut mas = state.write();
        let mut found: Option<crate::state::AuthorizationCodeData> = None;
        for (_, account) in mas.iter_mut() {
            if let Some(stored) = account.authorization_codes.get(code).cloned() {
                if stored.client_id != client_id || stored.user_pool_id != pool_id {
                    return Err(OAuthTokenError::InvalidGrant);
                }
                if stored.redirect_uri != redirect_uri {
                    return Err(OAuthTokenError::InvalidGrant);
                }
                // Codes expire after 5 minutes per Cognito docs.
                let age = Utc::now()
                    .signed_duration_since(stored.issued_at)
                    .num_seconds();
                if age > 300 {
                    account.authorization_codes.remove(code);
                    return Err(OAuthTokenError::InvalidGrant);
                }
                if let Some(challenge) = stored.code_challenge.as_deref() {
                    let verifier = code_verifier.ok_or(OAuthTokenError::InvalidGrant)?;
                    if !verify_pkce(challenge, stored.code_challenge_method.as_deref(), verifier) {
                        return Err(OAuthTokenError::InvalidGrant);
                    }
                }
                // Single-use: remove on consumption.
                account.authorization_codes.remove(code);
                found = Some(stored);
                break;
            }
        }
        found.ok_or(OAuthTokenError::InvalidGrant)?
    };

    let sub = {
        let mas = state.read();
        let mut sub = String::new();
        for (_, account) in mas.iter() {
            if let Some(user) = account
                .users
                .get(&consumed.user_pool_id)
                .and_then(|users| users.get(&consumed.username))
            {
                sub = user.sub.clone();
                break;
            }
        }
        sub
    };

    let signing = ensure_pool_signing_key(state, pool_id).await;
    let signing_ref = signing.as_ref().map(|(p, k)| (p.as_str(), k.as_str()));
    let scope_str = if consumed.scopes.is_empty() {
        None
    } else {
        Some(consumed.scopes.join(" "))
    };
    let tokens = generate_tokens_with_scope(
        pool_id,
        client_id,
        &sub,
        &consumed.username,
        region,
        signing_ref,
        scope_str.as_deref(),
        consumed.nonce.as_deref(),
    );

    {
        let mut mas = state.write();
        for (_, account) in mas.iter_mut() {
            if !account.user_pool_clients.contains_key(client_id) {
                continue;
            }
            account.refresh_tokens.insert(
                tokens.refresh_token.clone(),
                crate::state::RefreshTokenData {
                    user_pool_id: pool_id.to_string(),
                    username: consumed.username.clone(),
                    client_id: client_id.to_string(),
                    issued_at: Utc::now(),
                },
            );
            account.access_tokens.insert(
                tokens.access_token.clone(),
                crate::state::AccessTokenData {
                    user_pool_id: pool_id.to_string(),
                    username: consumed.username.clone(),
                    client_id: client_id.to_string(),
                    issued_at: Utc::now(),
                },
            );
            break;
        }
    }

    Ok(OAuthTokenResponse {
        access_token: tokens.access_token,
        id_token: Some(tokens.id_token),
        refresh_token: Some(tokens.refresh_token),
        expires_in: 3600,
        token_type: "Bearer".to_string(),
    })
}

async fn handle_refresh_token_grant(
    state: &SharedCognitoState,
    params: &BTreeMap<String, String>,
    client_id: &str,
    pool_id: &str,
    region: &str,
) -> Result<OAuthTokenResponse, OAuthTokenError> {
    let refresh_token = params
        .get("refresh_token")
        .map(String::as_str)
        .ok_or(OAuthTokenError::InvalidRequest("refresh_token is required"))?;
    let (username, sub) = {
        let mas = state.read();
        let mut found = Err(OAuthTokenError::InvalidGrant);
        for (_, account) in mas.iter() {
            if let Some(rt) = account.refresh_tokens.get(refresh_token) {
                if rt.client_id != client_id {
                    found = Err(OAuthTokenError::InvalidGrant);
                    break;
                }
                let sub = account
                    .users
                    .get(&rt.user_pool_id)
                    .and_then(|users| users.get(&rt.username))
                    .map(|u| u.sub.clone())
                    .unwrap_or_default();
                found = Ok((rt.username.clone(), sub));
                break;
            }
        }
        found?
    };
    let signing = ensure_pool_signing_key(state, pool_id).await;
    let signing_ref = signing.as_ref().map(|(p, k)| (p.as_str(), k.as_str()));
    let tokens = generate_tokens(pool_id, client_id, &sub, &username, region, signing_ref);
    let rotated_refresh = {
        let mut mas = state.write();
        let mut new_rt = None;
        for (_, account) in mas.iter_mut() {
            if !account.user_pool_clients.contains_key(client_id) {
                continue;
            }
            let rotation_enabled = account
                .user_pool_clients
                .get(client_id)
                .and_then(|c| c.refresh_token_rotation.as_ref())
                .map(|r| r.feature.eq_ignore_ascii_case("ENABLED"))
                .unwrap_or(false);
            if rotation_enabled {
                if let Some(old) = account.refresh_tokens.get(refresh_token).cloned() {
                    let token = format!("rt-{}", Uuid::new_v4());
                    account.refresh_tokens.insert(
                        token.clone(),
                        crate::state::RefreshTokenData {
                            user_pool_id: old.user_pool_id,
                            username: old.username,
                            client_id: old.client_id,
                            issued_at: Utc::now(),
                        },
                    );
                    account.refresh_tokens.remove(refresh_token);
                    new_rt = Some(token);
                }
            }
            account.access_tokens.insert(
                tokens.access_token.clone(),
                crate::state::AccessTokenData {
                    user_pool_id: pool_id.to_string(),
                    username: username.clone(),
                    client_id: client_id.to_string(),
                    issued_at: Utc::now(),
                },
            );
            break;
        }
        new_rt
    };
    Ok(OAuthTokenResponse {
        access_token: tokens.access_token,
        id_token: Some(tokens.id_token),
        refresh_token: rotated_refresh,
        expires_in: 3600,
        token_type: "Bearer".to_string(),
    })
}

#[allow(clippy::too_many_arguments)]
async fn handle_client_credentials_grant(
    state: &SharedCognitoState,
    params: &BTreeMap<String, String>,
    client_id: &str,
    pool_id: &str,
    stored_secret: Option<&str>,
    allowed_flows: &[String],
    allowed_scopes: &[String],
    oauth_flows_enabled: bool,
    region: &str,
) -> Result<OAuthTokenResponse, OAuthTokenError> {
    // client_credentials demands a confidential client (always has a
    // secret) and an explicit `client_credentials` flow allowance.
    if stored_secret.is_none() {
        return Err(OAuthTokenError::InvalidClient);
    }
    if oauth_flows_enabled
        && !allowed_flows.is_empty()
        && !allowed_flows
            .iter()
            .any(|f| f.eq_ignore_ascii_case("client_credentials"))
    {
        return Err(OAuthTokenError::UnauthorizedClient);
    }

    // Per RFC 6749 §3.3 the requested scope MUST be a subset of what
    // the client is allowed to use; AWS Cognito enforces this against
    // resource-server scopes registered on the pool.
    let requested = params
        .get("scope")
        .map(String::as_str)
        .unwrap_or("")
        .split_whitespace()
        .filter(|s| !s.is_empty())
        .collect::<Vec<_>>();
    if !requested.is_empty() && !allowed_scopes.is_empty() {
        for scope in &requested {
            if !allowed_scopes.iter().any(|s| s == scope) {
                return Err(OAuthTokenError::InvalidScope);
            }
        }
    }
    let granted = if requested.is_empty() {
        allowed_scopes.join(" ")
    } else {
        requested.join(" ")
    };

    let signing = ensure_pool_signing_key(state, pool_id).await;
    let signing_ref = signing.as_ref().map(|(p, k)| (p.as_str(), k.as_str()));
    let access_token = build_client_credentials_access_token(
        pool_id,
        client_id,
        Some(&granted),
        region,
        signing_ref,
    );

    {
        let mut mas = state.write();
        for (_, account) in mas.iter_mut() {
            if !account.user_pool_clients.contains_key(client_id) {
                continue;
            }
            // client_credentials access tokens have no associated
            // user; record `username = client_id` so revoke/userInfo
            // can still resolve the token owner.
            account.access_tokens.insert(
                access_token.clone(),
                crate::state::AccessTokenData {
                    user_pool_id: pool_id.to_string(),
                    username: client_id.to_string(),
                    client_id: client_id.to_string(),
                    issued_at: Utc::now(),
                },
            );
            break;
        }
    }

    Ok(OAuthTokenResponse {
        access_token,
        id_token: None,
        refresh_token: None,
        expires_in: 3600,
        token_type: "Bearer".to_string(),
    })
}

/// Verify a PKCE `code_verifier` against the stored `code_challenge`
/// and `code_challenge_method`. Implements RFC 7636 §4.6 — only
/// `S256` and `plain` are recognised; an unknown method is treated
/// as a verification failure rather than silently downgrading.
fn verify_pkce(challenge: &str, method: Option<&str>, verifier: &str) -> bool {
    let method = method.unwrap_or("S256");
    match method {
        "plain" => challenge == verifier,
        "S256" => {
            use rsa::sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(verifier.as_bytes());
            let digest = hasher.finalize();
            let computed = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(digest);
            challenge == computed
        }
        _ => false,
    }
}

fn build_client_credentials_access_token(
    pool_id: &str,
    client_id: &str,
    scope: Option<&str>,
    region: &str,
    signing: Option<(&str, &str)>,
) -> String {
    let now = Utc::now().timestamp();
    let jti = Uuid::new_v4().to_string();
    let iss = format!("https://cognito-idp.{region}.amazonaws.com/{pool_id}");

    // Synthesize a keypair on the fly when callers haven't loaded one
    // (only happens on legacy snapshots that lack a stored PEM and
    // pre-date `ensure_pool_signing_key`). Production paths always
    // pass `Some`.
    let owned_signing = signing
        .map(|(p, k)| (p.to_string(), k.to_string()))
        .unwrap_or_else(|| {
            let key = crate::jwt::generate_pool_signing_key();
            (key.private_key_pem, key.kid)
        });
    let pem = owned_signing.0.as_str();
    let kid = owned_signing.1.as_str();

    let header = json!({"kid": kid, "alg": "RS256", "typ": "JWT"});
    let payload = json!({
        "sub": client_id,
        "iss": iss,
        "client_id": client_id,
        "token_use": "access",
        "scope": scope.unwrap_or(""),
        "jti": jti,
        "exp": now + 3600,
        "iat": now,
    });
    sign_jwt(&header, &payload, pem)
}

#[derive(Debug, Clone)]
pub enum OAuthUserInfoError {
    InvalidToken,
}

/// RFC 7662-style userInfo. Resolves bearer access token in
/// state.access_tokens, returns OIDC standard claims sourced from the
/// user's attributes.
pub fn handle_oauth2_userinfo(
    state: &SharedCognitoState,
    bearer_token: &str,
) -> Result<Value, OAuthUserInfoError> {
    let mas = state.read();
    for (_, account) in mas.iter() {
        let Some(token_data) = account.access_tokens.get(bearer_token) else {
            continue;
        };
        let user = account
            .users
            .get(&token_data.user_pool_id)
            .and_then(|users| users.get(&token_data.username))
            .ok_or(OAuthUserInfoError::InvalidToken)?;
        let mut info = serde_json::Map::new();
        info.insert("sub".to_string(), Value::String(user.sub.clone()));
        info.insert("username".to_string(), Value::String(user.username.clone()));
        for attr in &user.attributes {
            info.insert(attr.name.clone(), Value::String(attr.value.clone()));
        }
        return Ok(Value::Object(info));
    }
    Err(OAuthUserInfoError::InvalidToken)
}

#[derive(Debug, Clone)]
pub enum OAuthRevokeError {
    InvalidClient,
    UnsupportedTokenType,
}

/// RFC 7009 token revocation. Cognito accepts refresh tokens; revoking a
/// refresh token also invalidates every access token issued from it
/// (matched by client_id + username + issued_at >= refresh issued_at).
/// Per RFC 7009 §2.2, revoking an unknown token is a 200, not an error.
pub fn handle_oauth2_revoke(
    state: &SharedCognitoState,
    params: &BTreeMap<String, String>,
) -> Result<(), OAuthRevokeError> {
    let token = match params.get("token") {
        Some(t) => t.clone(),
        None => return Ok(()),
    };
    if let Some(hint) = params.get("token_type_hint") {
        if hint != "refresh_token" {
            return Err(OAuthRevokeError::UnsupportedTokenType);
        }
    }
    let client_id = params
        .get("client_id")
        .map(String::as_str)
        .ok_or(OAuthRevokeError::InvalidClient)?;

    let stored_secret = {
        let mas = state.read();
        let mut found = None;
        for (_, account) in mas.iter() {
            if let Some(client) = account.user_pool_clients.get(client_id) {
                found = Some(client.client_secret.clone());
                break;
            }
        }
        found.ok_or(OAuthRevokeError::InvalidClient)?
    };
    if let Some(secret) = stored_secret.as_ref() {
        let supplied = params.get("client_secret").map(String::as_str);
        if supplied != Some(secret.as_str()) {
            return Err(OAuthRevokeError::InvalidClient);
        }
    }

    let mut mas = state.write();
    for (_, account) in mas.iter_mut() {
        if let Some(rt) = account.refresh_tokens.get(&token).cloned() {
            if rt.client_id != client_id {
                return Err(OAuthRevokeError::InvalidClient);
            }
            account.refresh_tokens.remove(&token);
            account.access_tokens.retain(|_, at| {
                !(at.client_id == rt.client_id
                    && at.username == rt.username
                    && at.user_pool_id == rt.user_pool_id
                    && at.issued_at >= rt.issued_at)
            });
            return Ok(());
        }
        if let Some(at) = account.access_tokens.get(&token).cloned() {
            if at.client_id != client_id {
                return Err(OAuthRevokeError::InvalidClient);
            }
            account.access_tokens.remove(&token);
            return Ok(());
        }
    }
    Ok(())
}

/// Inputs accepted by [`mint_authorization_code`]. Mirrors the
/// authorization-code request shape from RFC 6749 §4.1.1: the values
/// the user agent originally sent to `/oauth2/authorize`. Y4 will fill
/// these in from the consent flow; for now it's used by the admin
/// `/_fakecloud/cognito/authorization-codes` endpoint and by tests.
#[derive(Debug, Clone)]
pub struct MintAuthorizationCodeRequest {
    pub user_pool_id: String,
    pub client_id: String,
    pub username: String,
    pub redirect_uri: String,
    pub scopes: Vec<String>,
    pub code_challenge: Option<String>,
    pub code_challenge_method: Option<String>,
    pub nonce: Option<String>,
}

/// Validation failures from [`mint_authorization_code`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MintAuthorizationCodeError {
    /// `client_id` doesn't exist in any pool, or the supplied
    /// `user_pool_id` doesn't match the client's pool.
    InvalidClient,
    /// `redirect_uri` isn't one of the client's registered
    /// `callback_urls`.
    InvalidRedirectUri,
    /// User isn't enrolled in the pool.
    UserNotFound,
}

/// Issue a single-use authorization code and store it in pool state so
/// the next `/oauth2/token` `authorization_code` POST can consume it.
/// Returns the opaque code string.
///
/// Validates the request the same way real Cognito does at
/// `/oauth2/authorize`: client must exist, `user_pool_id` must match,
/// `redirect_uri` must be one of the client's registered callback URLs,
/// and the user must exist in the pool.
pub fn mint_authorization_code(
    state: &SharedCognitoState,
    req: &MintAuthorizationCodeRequest,
) -> Result<String, MintAuthorizationCodeError> {
    let mut mas = state.write();
    for (_, account) in mas.iter_mut() {
        let Some(client) = account.user_pool_clients.get(&req.client_id) else {
            continue;
        };
        if client.user_pool_id != req.user_pool_id {
            return Err(MintAuthorizationCodeError::InvalidClient);
        }
        // AWS allows callback_urls to be empty during early dev, so we
        // only enforce the match when at least one URL is registered.
        if !client.callback_urls.is_empty()
            && !client.callback_urls.iter().any(|u| u == &req.redirect_uri)
        {
            return Err(MintAuthorizationCodeError::InvalidRedirectUri);
        }
        let user_exists = account
            .users
            .get(&req.user_pool_id)
            .map(|users| users.contains_key(&req.username))
            .unwrap_or(false);
        if !user_exists {
            return Err(MintAuthorizationCodeError::UserNotFound);
        }
        let code = format!("ac-{}", Uuid::new_v4());
        account.authorization_codes.insert(
            code.clone(),
            crate::state::AuthorizationCodeData {
                user_pool_id: req.user_pool_id.clone(),
                client_id: req.client_id.clone(),
                username: req.username.clone(),
                redirect_uri: req.redirect_uri.clone(),
                scopes: req.scopes.clone(),
                code_challenge: req.code_challenge.clone(),
                code_challenge_method: req.code_challenge_method.clone(),
                nonce: req.nonce.clone(),
                issued_at: Utc::now(),
            },
        );
        return Ok(code);
    }
    Err(MintAuthorizationCodeError::InvalidClient)
}

/// Query parameters accepted by [`handle_oauth2_authorize`]. Mirrors
/// the OAuth 2.0 Authorization Request shape (RFC 6749 §4.1.1 /
/// §4.2.1) plus the synthetic `username`/`password` pair fakecloud
/// uses in lieu of a real Hosted-UI login form. Real Cognito would
/// render an HTML page that POSTs credentials back to itself; for
/// scripted E2E tests it's simpler to accept the credentials inline
/// on the initial GET.
#[derive(Debug, Clone)]
pub struct OAuth2AuthorizeRequest {
    pub response_type: String,
    pub client_id: String,
    pub redirect_uri: String,
    pub scope: Option<String>,
    pub state: Option<String>,
    pub code_challenge: Option<String>,
    pub code_challenge_method: Option<String>,
    pub nonce: Option<String>,
    /// Test-only synthetic login: when both supplied we attempt a
    /// password-based auth against the user pool. AWS does this via
    /// the Hosted UI HTML form; we collapse it into one query so E2E
    /// tests don't have to scrape HTML.
    pub username: Option<String>,
    pub password: Option<String>,
}

/// Result of [`handle_oauth2_authorize`]. The HTTP wrapper turns
/// `Redirect` into a 302 with the encoded URL, and `LoginRequired`
/// into a 200 HTML form (or a JSON error if you'd rather skip the
/// UI step).
#[derive(Debug, Clone)]
pub enum OAuth2AuthorizeOutcome {
    /// Authorization succeeded; redirect the user agent to this
    /// absolute URL. For `response_type=code` the params are in the
    /// query string; for `response_type=token` they're in the URL
    /// fragment per RFC 6749 §4.2.2.
    Redirect(String),
    /// Credentials weren't supplied (or weren't valid) — the wrapper
    /// should render a synthetic login form pointing back at
    /// `/oauth2/authorize` with the same query params plus
    /// `username`/`password` fields.
    LoginRequired { html: String },
}

/// Failure modes that bubble out of [`handle_oauth2_authorize`] before
/// we know it's safe to redirect — i.e. the redirect_uri itself is
/// untrusted, so per RFC 6749 §4.1.2.1 we MUST NOT bounce errors
/// back to it. The wrapper returns these as 400 JSON bodies.
#[derive(Debug, Clone)]
pub enum OAuth2AuthorizeError {
    /// `client_id` doesn't resolve to any registered app client.
    InvalidClient,
    /// `redirect_uri` isn't on the client's registered `CallbackURLs`.
    InvalidRedirectUri,
}

/// Failure modes safe to return *via* the redirect_uri (RFC 6749
/// §4.1.2.1 / §4.2.2.1) — these encode `error=...` back to the
/// client's redirect URL with the original `state` preserved.
#[derive(Debug, Clone, Copy)]
enum AuthorizeRedirectError {
    UnsupportedResponseType,
    InvalidScope,
    UnauthorizedClient,
    AccessDenied,
    InvalidRequest,
}

impl AuthorizeRedirectError {
    fn as_oauth_code(self) -> &'static str {
        match self {
            Self::UnsupportedResponseType => "unsupported_response_type",
            Self::InvalidScope => "invalid_scope",
            Self::UnauthorizedClient => "unauthorized_client",
            Self::AccessDenied => "access_denied",
            Self::InvalidRequest => "invalid_request",
        }
    }
}

/// Handle a Cognito Hosted-UI `/oauth2/authorize` request. Implements
/// both Authorization Code (`response_type=code`) and Implicit
/// (`response_type=token`) flows.
///
/// Validation order matches the RFC: client_id and redirect_uri are
/// checked first (failures here surface as 400 JSON, never as a
/// redirect, because we can't trust the supplied redirect_uri yet).
/// Anything past that point — bad scope, bad credentials, unsupported
/// response_type — is encoded back via the redirect_uri so the
/// client-side app can react gracefully.
pub async fn handle_oauth2_authorize(
    state: &SharedCognitoState,
    req: &OAuth2AuthorizeRequest,
    region: &str,
) -> Result<OAuth2AuthorizeOutcome, OAuth2AuthorizeError> {
    // Look up the client and validate redirect_uri *before* trusting
    // anything else. RFC 6749 §3.1.2.4: an invalid redirect_uri MUST
    // NOT be redirected to.
    let (pool_id, callback_urls, allowed_flows, allowed_scopes, oauth_flows_enabled) = {
        let mas = state.read();
        let mut found = None;
        for (_, account) in mas.iter() {
            if let Some(client) = account.user_pool_clients.get(&req.client_id) {
                found = Some((
                    client.user_pool_id.clone(),
                    client.callback_urls.clone(),
                    client.allowed_o_auth_flows.clone(),
                    client.allowed_o_auth_scopes.clone(),
                    client.allowed_o_auth_flows_user_pool_client,
                ));
                break;
            }
        }
        found.ok_or(OAuth2AuthorizeError::InvalidClient)?
    };

    // AWS allows callback_urls to be empty during early dev; only
    // enforce when at least one URL is registered.
    if !callback_urls.is_empty() && !callback_urls.iter().any(|u| u == &req.redirect_uri) {
        return Err(OAuth2AuthorizeError::InvalidRedirectUri);
    }

    // From here on, every error redirects back to the client.
    let response_type = req.response_type.as_str();
    let requested_scopes: Vec<String> = req
        .scope
        .as_deref()
        .unwrap_or("")
        .split_whitespace()
        .map(|s| s.to_string())
        .collect();

    // Scope subset check (RFC 6749 §3.3). Skip when the client has
    // no AllowedOAuthScopes registered (matches mint_authorization_code).
    if !requested_scopes.is_empty() && !allowed_scopes.is_empty() {
        for s in &requested_scopes {
            if !allowed_scopes.iter().any(|a| a == s) {
                return Ok(OAuth2AuthorizeOutcome::Redirect(build_error_redirect(
                    &req.redirect_uri,
                    AuthorizeRedirectError::InvalidScope,
                    req.state.as_deref(),
                    matches!(response_type, "token"),
                )));
            }
        }
    }

    // Map response_type -> required flow (RFC 6749 §3.1.1). Cognito
    // only honours `code`/`implicit` when the client opted into the
    // Hosted UI flows.
    let required_flow = match response_type {
        "code" => "code",
        "token" => "implicit",
        _ => {
            return Ok(OAuth2AuthorizeOutcome::Redirect(build_error_redirect(
                &req.redirect_uri,
                AuthorizeRedirectError::UnsupportedResponseType,
                req.state.as_deref(),
                false,
            )));
        }
    };
    if oauth_flows_enabled
        && !allowed_flows.is_empty()
        && !allowed_flows
            .iter()
            .any(|f| f.eq_ignore_ascii_case(required_flow))
    {
        return Ok(OAuth2AuthorizeOutcome::Redirect(build_error_redirect(
            &req.redirect_uri,
            AuthorizeRedirectError::UnauthorizedClient,
            req.state.as_deref(),
            response_type == "token",
        )));
    }

    // Synthetic login: real Cognito redirects to its hosted form, but
    // for scripted E2E we accept username/password on the query.
    let (Some(username), Some(password)) = (req.username.as_deref(), req.password.as_deref())
    else {
        return Ok(OAuth2AuthorizeOutcome::LoginRequired {
            html: render_login_form(req),
        });
    };

    // Verify credentials inline. We don't run the
    // PreAuthentication/PostAuthentication Lambda triggers here —
    // those fire from the AdminInitiateAuth path and the Hosted UI
    // is meant to be a thin shell on top of it; we keep this handler
    // self-contained to avoid the full delivery_ctx plumbing.
    let credentials_ok = {
        let mas = state.read();
        let mut ok = false;
        for (_, account) in mas.iter() {
            if let Some(user) = account
                .users
                .get(&pool_id)
                .and_then(|users| users.get(username))
            {
                if !user.enabled {
                    break;
                }
                let matches = match (&user.password, &user.temporary_password) {
                    (Some(p), _) if p == password => true,
                    (_, Some(tp)) if tp == password => true,
                    _ => false,
                };
                if matches {
                    ok = true;
                }
                break;
            }
        }
        ok
    };
    if !credentials_ok {
        return Ok(OAuth2AuthorizeOutcome::Redirect(build_error_redirect(
            &req.redirect_uri,
            AuthorizeRedirectError::AccessDenied,
            req.state.as_deref(),
            response_type == "token",
        )));
    }

    match response_type {
        "code" => {
            let mint_req = MintAuthorizationCodeRequest {
                user_pool_id: pool_id.clone(),
                client_id: req.client_id.clone(),
                username: username.to_string(),
                redirect_uri: req.redirect_uri.clone(),
                scopes: requested_scopes,
                code_challenge: req.code_challenge.clone(),
                code_challenge_method: req.code_challenge_method.clone(),
                nonce: req.nonce.clone(),
            };
            let code = match mint_authorization_code(state, &mint_req) {
                Ok(c) => c,
                // Already validated above; treat residual mismatches
                // as access_denied to stay honest with the redirect.
                Err(_) => {
                    return Ok(OAuth2AuthorizeOutcome::Redirect(build_error_redirect(
                        &req.redirect_uri,
                        AuthorizeRedirectError::InvalidRequest,
                        req.state.as_deref(),
                        false,
                    )));
                }
            };
            let mut url = req.redirect_uri.clone();
            url.push(if url.contains('?') { '&' } else { '?' });
            url.push_str("code=");
            url.push_str(&urlencoding_encode(&code));
            if let Some(s) = req.state.as_deref() {
                url.push_str("&state=");
                url.push_str(&urlencoding_encode(s));
            }
            Ok(OAuth2AuthorizeOutcome::Redirect(url))
        }
        "token" => {
            // Implicit grant: mint id_token + access_token, return in
            // fragment per RFC 6749 §4.2.2. No refresh_token (RFC
            // §4.2.2 explicitly forbids it on implicit).
            let sub = {
                let mas = state.read();
                let mut sub = String::new();
                for (_, account) in mas.iter() {
                    if let Some(user) = account
                        .users
                        .get(&pool_id)
                        .and_then(|users| users.get(username))
                    {
                        sub = user.sub.clone();
                        break;
                    }
                }
                sub
            };
            let scope_str = if requested_scopes.is_empty() {
                None
            } else {
                Some(requested_scopes.join(" "))
            };
            let signing = ensure_pool_signing_key(state, &pool_id).await;
            let signing_ref = signing.as_ref().map(|(p, k)| (p.as_str(), k.as_str()));
            let tokens = generate_tokens_with_scope(
                &pool_id,
                &req.client_id,
                &sub,
                username,
                region,
                signing_ref,
                scope_str.as_deref(),
                req.nonce.as_deref(),
            );
            // Persist the access_token so /oauth2/userInfo and
            // /oauth2/revoke can resolve it back to the user.
            {
                let mut mas = state.write();
                for (_, account) in mas.iter_mut() {
                    if !account.user_pool_clients.contains_key(&req.client_id) {
                        continue;
                    }
                    account.access_tokens.insert(
                        tokens.access_token.clone(),
                        crate::state::AccessTokenData {
                            user_pool_id: pool_id.clone(),
                            username: username.to_string(),
                            client_id: req.client_id.clone(),
                            issued_at: Utc::now(),
                        },
                    );
                    break;
                }
            }
            let mut fragment = format!(
                "access_token={}&id_token={}&token_type=Bearer&expires_in=3600",
                urlencoding_encode(&tokens.access_token),
                urlencoding_encode(&tokens.id_token),
            );
            if let Some(s) = req.state.as_deref() {
                fragment.push_str("&state=");
                fragment.push_str(&urlencoding_encode(s));
            }
            let url = format!("{}#{fragment}", req.redirect_uri);
            Ok(OAuth2AuthorizeOutcome::Redirect(url))
        }
        _ => unreachable!("response_type already validated"),
    }
}

/// Append an `error` (and `state`) param to `redirect_uri` per RFC
/// 6749 §4.1.2.1 (code flow uses query) / §4.2.2.1 (implicit flow
/// uses fragment).
fn build_error_redirect(
    redirect_uri: &str,
    err: AuthorizeRedirectError,
    state: Option<&str>,
    use_fragment: bool,
) -> String {
    let mut params = format!("error={}", err.as_oauth_code());
    if let Some(s) = state {
        params.push_str("&state=");
        params.push_str(&urlencoding_encode(s));
    }
    if use_fragment {
        format!("{redirect_uri}#{params}")
    } else {
        let sep = if redirect_uri.contains('?') { '&' } else { '?' };
        format!("{redirect_uri}{sep}{params}")
    }
}

/// Minimal HTML login page rendered when the user agent hits
/// `/oauth2/authorize` without `username`/`password` query params.
/// The form POSTs/GETs back to the same endpoint with all the
/// original OAuth params preserved as hidden fields. Real Cognito's
/// Hosted UI is far prettier; ours is enough to keep manual smoke
/// tests honest.
fn render_login_form(req: &OAuth2AuthorizeRequest) -> String {
    fn hidden(name: &str, value: &str) -> String {
        format!(
            r#"<input type="hidden" name="{}" value="{}">"#,
            html_escape(name),
            html_escape(value),
        )
    }
    let mut hiddens = String::new();
    hiddens.push_str(&hidden("response_type", &req.response_type));
    hiddens.push_str(&hidden("client_id", &req.client_id));
    hiddens.push_str(&hidden("redirect_uri", &req.redirect_uri));
    if let Some(s) = req.scope.as_deref() {
        hiddens.push_str(&hidden("scope", s));
    }
    if let Some(s) = req.state.as_deref() {
        hiddens.push_str(&hidden("state", s));
    }
    if let Some(c) = req.code_challenge.as_deref() {
        hiddens.push_str(&hidden("code_challenge", c));
    }
    if let Some(m) = req.code_challenge_method.as_deref() {
        hiddens.push_str(&hidden("code_challenge_method", m));
    }
    if let Some(n) = req.nonce.as_deref() {
        hiddens.push_str(&hidden("nonce", n));
    }
    format!(
        r#"<!doctype html>
<html><head><meta charset="utf-8"><title>fakecloud login</title></head>
<body>
<h1>fakecloud Cognito Hosted UI</h1>
<p>This is a test stand-in for the real Cognito Hosted UI login form.</p>
<form method="get" action="/oauth2/authorize">
{hiddens}
<label>Username <input name="username" autocomplete="username"></label><br>
<label>Password <input name="password" type="password" autocomplete="current-password"></label><br>
<button type="submit">Sign in</button>
</form>
</body></html>
"#,
    )
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// Minimal `application/x-www-form-urlencoded` value encoder. Avoids
/// pulling in a new crate just for this — we only need the
/// unreserved-set rule (RFC 3986 §2.3) plus space-as-`%20` (NOT `+`,
/// since these values land in URL fragments and query strings where
/// `+` is reserved on the fragment side).
fn urlencoding_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for byte in input.as_bytes() {
        let c = *byte;
        if c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.' | b'~') {
            out.push(c as char);
        } else {
            out.push_str(&format!("%{c:02X}"));
        }
    }
    out
}

#[cfg(test)]
mod tests;
