//! Lambda trigger integration for Cognito User Pool lifecycle events.
//!
//! Cognito User Pools can invoke Lambda functions at various points during
//! authentication and user management. This module builds the trigger event
//! payloads and dispatches them through the DeliveryBus.

use std::sync::Arc;

use serde_json::{json, Value};

use fakecloud_core::delivery::DeliveryBus;

use crate::state::{SharedCognitoState, User, UserAttribute};

/// Shared references needed for Lambda trigger delivery.
#[derive(Clone)]
pub struct CognitoDeliveryContext {
    pub delivery_bus: Arc<DeliveryBus>,
}

/// The trigger source identifies which Cognito action initiated the trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerSource {
    PreSignUpSignUp,
    PreSignUpAdminCreateUser,
    PostConfirmationConfirmSignUp,
    PostConfirmationAdminConfirmSignUp,
    PreAuthenticationAuthentication,
    PostAuthenticationAuthentication,
    CustomMessageSignUp,
    CustomMessageForgotPassword,
    TokenGenerationAuthentication,
    UserMigrationAuthentication,
}

impl TriggerSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PreSignUpSignUp => "PreSignUp_SignUp",
            Self::PreSignUpAdminCreateUser => "PreSignUp_AdminCreateUser",
            Self::PostConfirmationConfirmSignUp => "PostConfirmation_ConfirmSignUp",
            Self::PostConfirmationAdminConfirmSignUp => "PostConfirmation_AdminConfirmSignUp",
            Self::PreAuthenticationAuthentication => "PreAuthentication_Authentication",
            Self::PostAuthenticationAuthentication => "PostAuthentication_Authentication",
            Self::CustomMessageSignUp => "CustomMessage_SignUp",
            Self::CustomMessageForgotPassword => "CustomMessage_ForgotPassword",
            Self::TokenGenerationAuthentication => "TokenGeneration_Authentication",
            Self::UserMigrationAuthentication => "UserMigration_Authentication",
        }
    }

    /// The Lambda config key used to look up the function ARN.
    fn lambda_config_key(self) -> &'static str {
        match self {
            Self::PreSignUpSignUp | Self::PreSignUpAdminCreateUser => "PreSignUp",
            Self::PostConfirmationConfirmSignUp | Self::PostConfirmationAdminConfirmSignUp => {
                "PostConfirmation"
            }
            Self::PreAuthenticationAuthentication => "PreAuthentication",
            Self::PostAuthenticationAuthentication => "PostAuthentication",
            Self::CustomMessageSignUp | Self::CustomMessageForgotPassword => "CustomMessage",
            Self::TokenGenerationAuthentication => "PreTokenGeneration",
            Self::UserMigrationAuthentication => "UserMigration",
        }
    }
}

/// Look up the Lambda function ARN from the pool's LambdaConfig for a trigger.
pub fn get_trigger_arn(
    cognito_state: &SharedCognitoState,
    pool_id: &str,
    trigger_source: TriggerSource,
) -> Option<String> {
    let state = cognito_state.read();
    let pool = state.user_pools.get(pool_id)?;
    let lambda_config = pool.lambda_config.as_ref()?;
    let key = trigger_source.lambda_config_key();
    lambda_config
        .get(key)
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Build the trigger event JSON matching the AWS Cognito Lambda trigger format.
pub fn build_trigger_event(
    trigger_source: TriggerSource,
    pool_id: &str,
    client_id: Option<&str>,
    username: &str,
    user_attributes: &[UserAttribute],
    region: &str,
    account_id: &str,
) -> Value {
    let user_attrs: Value = user_attributes
        .iter()
        .map(|a| (a.name.clone(), Value::String(a.value.clone())))
        .collect::<serde_json::Map<String, Value>>()
        .into();

    let mut event = json!({
        "version": "1",
        "triggerSource": trigger_source.as_str(),
        "region": region,
        "userPoolId": pool_id,
        "userName": username,
        "callerContext": {
            "awsSdkVersion": "fakecloud",
            "clientId": client_id.unwrap_or(""),
        },
        "request": {
            "userAttributes": user_attrs,
        },
        "response": {},
    });

    // Add trigger-specific request/response fields
    match trigger_source {
        TriggerSource::PreSignUpSignUp | TriggerSource::PreSignUpAdminCreateUser => {
            event["response"]["autoConfirmUser"] = json!(false);
            event["response"]["autoVerifyPhone"] = json!(false);
            event["response"]["autoVerifyEmail"] = json!(false);
        }
        TriggerSource::PostAuthenticationAuthentication => {
            event["request"]["newDeviceUsed"] = json!(false);
        }
        _ => {}
    }

    // Add account ID to callerContext
    event["callerContext"]["accountId"] = json!(account_id);

    event
}

/// Invoke a Lambda trigger synchronously (waits for response).
/// Used for PreSignUp and PreAuthentication triggers where the response
/// can modify the operation outcome.
///
/// Returns `Ok(Some(response))` if the trigger was invoked successfully,
/// `Ok(None)` if no trigger is configured or the delivery bus has no Lambda,
/// or silently returns `Ok(None)` on invocation errors (triggers should not
/// fail the Cognito operation).
pub async fn invoke_trigger(
    ctx: &CognitoDeliveryContext,
    function_arn: &str,
    event: &Value,
) -> Option<Value> {
    let payload = event.to_string();
    match ctx.delivery_bus.invoke_lambda(function_arn, &payload).await {
        Some(Ok(bytes)) => match serde_json::from_slice::<Value>(&bytes) {
            Ok(response) => Some(response),
            Err(e) => {
                tracing::warn!(
                    function_arn = %function_arn,
                    error = %e,
                    "Failed to parse Lambda trigger response as JSON"
                );
                None
            }
        },
        Some(Err(e)) => {
            tracing::warn!(
                function_arn = %function_arn,
                error = %e,
                "Lambda trigger invocation failed, skipping"
            );
            None
        }
        None => None,
    }
}

/// Invoke a Lambda trigger in fire-and-forget mode.
/// Used for PostConfirmation, PostAuthentication, and CustomMessage triggers.
pub fn invoke_trigger_fire_and_forget(
    ctx: &CognitoDeliveryContext,
    function_arn: String,
    event: Value,
) {
    let bus = ctx.delivery_bus.clone();
    tokio::spawn(async move {
        let payload = event.to_string();
        match bus.invoke_lambda(&function_arn, &payload).await {
            Some(Ok(_)) => {
                tracing::debug!(
                    function_arn = %function_arn,
                    "Fire-and-forget Lambda trigger completed"
                );
            }
            Some(Err(e)) => {
                tracing::warn!(
                    function_arn = %function_arn,
                    error = %e,
                    "Fire-and-forget Lambda trigger failed"
                );
            }
            None => {}
        }
    });
}

/// Helper: collect user attributes from state for trigger event construction.
pub fn collect_user_attributes(user: &User) -> Vec<UserAttribute> {
    user.attributes.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_source_as_str() {
        assert_eq!(TriggerSource::PreSignUpSignUp.as_str(), "PreSignUp_SignUp");
        assert_eq!(
            TriggerSource::PreSignUpAdminCreateUser.as_str(),
            "PreSignUp_AdminCreateUser"
        );
        assert_eq!(
            TriggerSource::PostConfirmationConfirmSignUp.as_str(),
            "PostConfirmation_ConfirmSignUp"
        );
        assert_eq!(
            TriggerSource::PostConfirmationAdminConfirmSignUp.as_str(),
            "PostConfirmation_AdminConfirmSignUp"
        );
        assert_eq!(
            TriggerSource::PreAuthenticationAuthentication.as_str(),
            "PreAuthentication_Authentication"
        );
        assert_eq!(
            TriggerSource::PostAuthenticationAuthentication.as_str(),
            "PostAuthentication_Authentication"
        );
        assert_eq!(
            TriggerSource::CustomMessageForgotPassword.as_str(),
            "CustomMessage_ForgotPassword"
        );
    }

    #[test]
    fn lambda_config_key_mapping() {
        assert_eq!(
            TriggerSource::PreSignUpSignUp.lambda_config_key(),
            "PreSignUp"
        );
        assert_eq!(
            TriggerSource::PreSignUpAdminCreateUser.lambda_config_key(),
            "PreSignUp"
        );
        assert_eq!(
            TriggerSource::PostConfirmationConfirmSignUp.lambda_config_key(),
            "PostConfirmation"
        );
        assert_eq!(
            TriggerSource::PreAuthenticationAuthentication.lambda_config_key(),
            "PreAuthentication"
        );
        assert_eq!(
            TriggerSource::PostAuthenticationAuthentication.lambda_config_key(),
            "PostAuthentication"
        );
        assert_eq!(
            TriggerSource::CustomMessageForgotPassword.lambda_config_key(),
            "CustomMessage"
        );
        assert_eq!(
            TriggerSource::TokenGenerationAuthentication.lambda_config_key(),
            "PreTokenGeneration"
        );
        assert_eq!(
            TriggerSource::UserMigrationAuthentication.lambda_config_key(),
            "UserMigration"
        );
    }

    #[test]
    fn build_pre_sign_up_event() {
        let attrs = vec![
            UserAttribute {
                name: "sub".to_string(),
                value: "abc-123".to_string(),
            },
            UserAttribute {
                name: "email".to_string(),
                value: "user@example.com".to_string(),
            },
        ];

        let event = build_trigger_event(
            TriggerSource::PreSignUpSignUp,
            "us-east-1_abc",
            Some("client-id-1"),
            "testuser",
            &attrs,
            "us-east-1",
            "123456789012",
        );

        assert_eq!(event["version"], "1");
        assert_eq!(event["triggerSource"], "PreSignUp_SignUp");
        assert_eq!(event["region"], "us-east-1");
        assert_eq!(event["userPoolId"], "us-east-1_abc");
        assert_eq!(event["userName"], "testuser");
        assert_eq!(event["callerContext"]["clientId"], "client-id-1");
        assert_eq!(
            event["request"]["userAttributes"]["email"],
            "user@example.com"
        );
        assert_eq!(event["request"]["userAttributes"]["sub"], "abc-123");
        // PreSignUp should have autoConfirmUser in response
        assert_eq!(event["response"]["autoConfirmUser"], false);
        assert_eq!(event["response"]["autoVerifyEmail"], false);
    }

    #[test]
    fn build_post_auth_event_has_new_device_used() {
        let attrs = vec![UserAttribute {
            name: "sub".to_string(),
            value: "abc-123".to_string(),
        }];

        let event = build_trigger_event(
            TriggerSource::PostAuthenticationAuthentication,
            "us-east-1_abc",
            Some("client-id-1"),
            "testuser",
            &attrs,
            "us-east-1",
            "123456789012",
        );

        assert_eq!(event["triggerSource"], "PostAuthentication_Authentication");
        assert_eq!(event["request"]["newDeviceUsed"], false);
    }

    #[test]
    fn build_event_without_client_id() {
        let event = build_trigger_event(
            TriggerSource::PostConfirmationAdminConfirmSignUp,
            "us-east-1_abc",
            None,
            "testuser",
            &[],
            "us-east-1",
            "123456789012",
        );

        assert_eq!(event["callerContext"]["clientId"], "");
        assert_eq!(
            event["triggerSource"],
            "PostConfirmation_AdminConfirmSignUp"
        );
    }

    #[test]
    fn get_trigger_arn_from_lambda_config() {
        use crate::state::CognitoState;
        use parking_lot::RwLock;
        use std::sync::Arc;

        let state = Arc::new(RwLock::new(CognitoState::new("123456789012", "us-east-1")));

        // No pool -> None
        assert!(get_trigger_arn(&state, "pool-1", TriggerSource::PreSignUpSignUp).is_none());

        // Add pool with lambda config
        {
            let mut s = state.write();
            s.user_pools.insert(
                "pool-1".to_string(),
                crate::state::UserPool {
                    id: "pool-1".to_string(),
                    name: "TestPool".to_string(),
                    arn: "arn:aws:cognito-idp:us-east-1:123456789012:userpool/pool-1".to_string(),
                    status: "Enabled".to_string(),
                    creation_date: chrono::Utc::now(),
                    last_modified_date: chrono::Utc::now(),
                    policies: crate::state::PoolPolicies {
                        password_policy: crate::state::PasswordPolicy::default(),
                    },
                    auto_verified_attributes: vec![],
                    username_attributes: None,
                    alias_attributes: None,
                    schema_attributes: vec![],
                    lambda_config: Some(serde_json::json!({
                        "PreSignUp": "arn:aws:lambda:us-east-1:123456789012:function:my-pre-signup",
                        "PostConfirmation": "arn:aws:lambda:us-east-1:123456789012:function:my-post-confirm",
                    })),
                    mfa_configuration: "OFF".to_string(),
                    email_configuration: None,
                    sms_configuration: None,
                    admin_create_user_config: None,
                    user_pool_tags: std::collections::HashMap::new(),
                    account_recovery_setting: None,
                    deletion_protection: None,
                    estimated_number_of_users: 0,
                    software_token_mfa_configuration: None,
                    sms_mfa_configuration: None,
                },
            );
        }

        // PreSignUp trigger configured
        let arn = get_trigger_arn(&state, "pool-1", TriggerSource::PreSignUpSignUp);
        assert_eq!(
            arn.as_deref(),
            Some("arn:aws:lambda:us-east-1:123456789012:function:my-pre-signup")
        );

        // PostConfirmation trigger configured
        let arn = get_trigger_arn(
            &state,
            "pool-1",
            TriggerSource::PostConfirmationConfirmSignUp,
        );
        assert_eq!(
            arn.as_deref(),
            Some("arn:aws:lambda:us-east-1:123456789012:function:my-post-confirm")
        );

        // PreAuthentication not configured -> None
        assert!(get_trigger_arn(
            &state,
            "pool-1",
            TriggerSource::PreAuthenticationAuthentication
        )
        .is_none());
    }
}
