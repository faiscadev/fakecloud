//! Lambda trigger integration for Cognito User Pool lifecycle events.
//!
//! Cognito User Pools can invoke Lambda functions at various points during
//! authentication and user management. This module builds the trigger event
//! payloads and dispatches them through the DeliveryBus.

use std::sync::Arc;

use serde_json::{json, Value};

use fakecloud_core::delivery::{DeliveryBus, EmailDispatcher, SmsDispatcher};
use fakecloud_core::service::AwsServiceError;

use crate::state::{ChallengeResult, SharedCognitoState, User, UserAttribute};

/// Shared references needed for Lambda trigger delivery and outbound
/// notification dispatch (verification email via SES, SMS MFA via SNS).
#[derive(Clone)]
pub struct CognitoDeliveryContext {
    pub delivery_bus: Arc<DeliveryBus>,
    pub email: Option<Arc<dyn EmailDispatcher>>,
    pub sms: Option<Arc<dyn SmsDispatcher>>,
}

impl CognitoDeliveryContext {
    pub fn new(delivery_bus: Arc<DeliveryBus>) -> Self {
        Self {
            delivery_bus,
            email: None,
            sms: None,
        }
    }

    pub fn with_email(mut self, email: Arc<dyn EmailDispatcher>) -> Self {
        self.email = Some(email);
        self
    }

    pub fn with_sms(mut self, sms: Arc<dyn SmsDispatcher>) -> Self {
        self.sms = Some(sms);
        self
    }
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
    CustomMessageAdminCreateUser,
    CustomMessageResendCode,
    CustomMessageForgotPassword,
    CustomMessageVerifyUserAttribute,
    TokenGenerationAuthentication,
    TokenGenerationRefreshTokens,
    TokenGenerationHostedAuth,
    UserMigrationAuthentication,
    DefineAuthChallengeAuthentication,
    CreateAuthChallengeAuthentication,
    VerifyAuthChallengeResponseAuthentication,
    CustomEmailSenderSignUp,
    CustomEmailSenderForgotPassword,
    CustomEmailSenderResendCode,
    CustomEmailSenderUpdateUserAttribute,
    CustomEmailSenderVerifyUserAttribute,
    CustomEmailSenderAdminCreateUser,
    CustomEmailSenderAccountTakeOverNotification,
    CustomSmsSenderSignUp,
    CustomSmsSenderForgotPassword,
    CustomSmsSenderResendCode,
    CustomSmsSenderAuthentication,
    CustomSmsSenderAdminCreateUser,
    CustomSmsSenderUpdateUserAttribute,
    CustomSmsSenderVerifyUserAttribute,
}

impl TriggerSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::PreSignUpSignUp => "PreSignUp_SignUp",
            Self::PreSignUpAdminCreateUser => "PreSignUp_AdminCreateUser",
            Self::PostConfirmationConfirmSignUp => "PostConfirmation_ConfirmSignUp",
            // AdminConfirmSignUp reports the same trigger source as
            // ConfirmSignUp (Cognito developer guide, "Connecting API
            // operations to Lambda triggers").
            Self::PostConfirmationAdminConfirmSignUp => "PostConfirmation_ConfirmSignUp",
            Self::PreAuthenticationAuthentication => "PreAuthentication_Authentication",
            Self::PostAuthenticationAuthentication => "PostAuthentication_Authentication",
            Self::CustomMessageSignUp => "CustomMessage_SignUp",
            Self::CustomMessageAdminCreateUser => "CustomMessage_AdminCreateUser",
            Self::CustomMessageResendCode => "CustomMessage_ResendCode",
            Self::CustomMessageForgotPassword => "CustomMessage_ForgotPassword",
            Self::CustomMessageVerifyUserAttribute => "CustomMessage_VerifyUserAttribute",
            Self::TokenGenerationAuthentication => "TokenGeneration_Authentication",
            Self::TokenGenerationRefreshTokens => "TokenGeneration_RefreshTokens",
            Self::TokenGenerationHostedAuth => "TokenGeneration_HostedAuth",
            Self::UserMigrationAuthentication => "UserMigration_Authentication",
            Self::DefineAuthChallengeAuthentication => "DefineAuthChallenge_Authentication",
            Self::CreateAuthChallengeAuthentication => "CreateAuthChallenge_Authentication",
            Self::VerifyAuthChallengeResponseAuthentication => {
                "VerifyAuthChallengeResponse_Authentication"
            }
            Self::CustomEmailSenderSignUp => "CustomEmailSender_SignUp",
            Self::CustomEmailSenderForgotPassword => "CustomEmailSender_ForgotPassword",
            Self::CustomEmailSenderResendCode => "CustomEmailSender_ResendCode",
            Self::CustomEmailSenderUpdateUserAttribute => "CustomEmailSender_UpdateUserAttribute",
            Self::CustomEmailSenderVerifyUserAttribute => "CustomEmailSender_VerifyUserAttribute",
            Self::CustomEmailSenderAdminCreateUser => "CustomEmailSender_AdminCreateUser",
            Self::CustomEmailSenderAccountTakeOverNotification => {
                "CustomEmailSender_AccountTakeOverNotification"
            }
            Self::CustomSmsSenderSignUp => "CustomSMSSender_SignUp",
            Self::CustomSmsSenderForgotPassword => "CustomSMSSender_ForgotPassword",
            Self::CustomSmsSenderResendCode => "CustomSMSSender_ResendCode",
            Self::CustomSmsSenderAuthentication => "CustomSMSSender_Authentication",
            Self::CustomSmsSenderAdminCreateUser => "CustomSMSSender_AdminCreateUser",
            Self::CustomSmsSenderUpdateUserAttribute => "CustomSMSSender_UpdateUserAttribute",
            Self::CustomSmsSenderVerifyUserAttribute => "CustomSMSSender_VerifyUserAttribute",
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
            Self::CustomMessageSignUp
            | Self::CustomMessageAdminCreateUser
            | Self::CustomMessageResendCode
            | Self::CustomMessageForgotPassword
            | Self::CustomMessageVerifyUserAttribute => "CustomMessage",
            Self::TokenGenerationAuthentication
            | Self::TokenGenerationRefreshTokens
            | Self::TokenGenerationHostedAuth => "PreTokenGeneration",
            Self::UserMigrationAuthentication => "UserMigration",
            Self::DefineAuthChallengeAuthentication => "DefineAuthChallenge",
            Self::CreateAuthChallengeAuthentication => "CreateAuthChallenge",
            Self::VerifyAuthChallengeResponseAuthentication => "VerifyAuthChallengeResponse",
            Self::CustomEmailSenderSignUp
            | Self::CustomEmailSenderForgotPassword
            | Self::CustomEmailSenderResendCode
            | Self::CustomEmailSenderUpdateUserAttribute
            | Self::CustomEmailSenderVerifyUserAttribute
            | Self::CustomEmailSenderAdminCreateUser
            | Self::CustomEmailSenderAccountTakeOverNotification => "CustomEmailSender",
            Self::CustomSmsSenderSignUp
            | Self::CustomSmsSenderForgotPassword
            | Self::CustomSmsSenderResendCode
            | Self::CustomSmsSenderAuthentication
            | Self::CustomSmsSenderAdminCreateUser
            | Self::CustomSmsSenderUpdateUserAttribute
            | Self::CustomSmsSenderVerifyUserAttribute => "CustomSMSSender",
        }
    }
}

/// Look up the Lambda function ARN from the pool's LambdaConfig for a trigger.
///
/// Most triggers are stored as `LambdaConfig.<Trigger> = "<arn>"`.
/// CustomEmailSender / CustomSMSSender are nested:
///   `LambdaConfig.CustomEmailSender = { LambdaArn, LambdaVersion }`
/// — fall back to that shape when the flat string form isn't present.
pub fn get_trigger_arn(
    cognito_state: &SharedCognitoState,
    pool_id: &str,
    trigger_source: TriggerSource,
) -> Option<String> {
    let accounts = cognito_state.read();
    // Pool ids are unique: the pool lives in whichever account created it.
    let pool = accounts
        .iter()
        .find_map(|(_, state)| state.user_pools.get(pool_id))?;
    let lambda_config = pool.lambda_config.as_ref()?;
    let key = trigger_source.lambda_config_key();
    let raw = lambda_config.get(key)?;
    if let Some(s) = raw.as_str() {
        return Some(s.to_string());
    }
    raw.get("LambdaArn")
        .or_else(|| raw.get("lambdaArn"))
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
///
/// Returns `Ok(Some(response))` when the trigger ran and returned a JSON
/// response, and `Ok(None)` when no Lambda is wired, the invocation could
/// not run, or the response was not JSON (triggers that cannot run do not
/// fail the Cognito operation). A trigger that ran and returned a function
/// error fails the operation the way Cognito does:
/// `UserLambdaValidationException` "`<Trigger>` failed with error
/// `<errorMessage>`."
pub async fn invoke_trigger(
    ctx: &CognitoDeliveryContext,
    function_arn: &str,
    event: &Value,
) -> Result<Option<Value>, AwsServiceError> {
    let payload = event.to_string();
    match ctx.delivery_bus.invoke_lambda(function_arn, &payload).await {
        Some(Ok(output)) => {
            if output.is_function_error() {
                return Err(trigger_function_error(event, &output));
            }
            match serde_json::from_slice::<Value>(&output.payload) {
                Ok(response) => Ok(Some(response)),
                Err(e) => {
                    tracing::warn!(
                        function_arn = %function_arn,
                        error = %e,
                        "Failed to parse Lambda trigger response as JSON"
                    );
                    Ok(None)
                }
            }
        }
        Some(Err(e)) => {
            tracing::warn!(
                function_arn = %function_arn,
                error = %e,
                "Lambda trigger invocation failed, skipping"
            );
            Ok(None)
        }
        None => Ok(None),
    }
}

/// The trigger name Cognito reports in a trigger failure: the Lambda config
/// key the event's `triggerSource` belongs to (`PreSignUp_SignUp` ->
/// `PreSignUp`, `TokenGeneration_*` -> `PreTokenGeneration`).
fn trigger_name(event: &Value) -> String {
    let source = event["triggerSource"].as_str().unwrap_or("");
    let prefix = source.split('_').next().unwrap_or(source);
    match prefix {
        "TokenGeneration" => "PreTokenGeneration".to_string(),
        "" => "Lambda trigger".to_string(),
        other => other.to_string(),
    }
}

/// `UserLambdaValidationException` for a trigger that returned a function
/// error.
pub(crate) fn trigger_function_error(
    event: &Value,
    output: &fakecloud_core::delivery::LambdaInvocationOutput,
) -> AwsServiceError {
    let message = output
        .error_message()
        .unwrap_or_else(|| String::from_utf8_lossy(&output.payload).into_owned());
    AwsServiceError::aws_error(
        http::StatusCode::BAD_REQUEST,
        "UserLambdaValidationException",
        format!("{} failed with error {message}.", trigger_name(event)),
    )
}

/// Invoke a custom sender trigger (CustomEmailSender / CustomSMSSender).
/// "Except for custom sender Lambda triggers, Amazon Cognito invokes Lambda
/// functions synchronously" (Cognito developer guide, "Things to know about
/// Lambda triggers"): custom senders are Event invocations, so a function
/// error is Lambda's to retry and never fails the Cognito call.
pub fn invoke_custom_sender(ctx: &CognitoDeliveryContext, function_arn: &str, event: &Value) {
    match ctx
        .delivery_bus
        .invoke_lambda_async(function_arn, &event.to_string())
    {
        Some(Ok(())) => {
            tracing::debug!(function_arn = %function_arn, "Custom sender trigger queued");
        }
        Some(Err(e)) => {
            tracing::warn!(
                function_arn = %function_arn,
                error = %e,
                "Custom sender trigger could not be invoked"
            );
        }
        None => {}
    }
}

/// Build a DefineAuthChallenge trigger event.
///
/// The `session` array contains previous challenge results: each entry has
/// `challengeName`, `challengeResult`, and `challengeMetadata`.
pub fn build_define_auth_challenge_event(
    pool_id: &str,
    client_id: Option<&str>,
    username: &str,
    user_attributes: &[UserAttribute],
    challenge_results: &[ChallengeResult],
    region: &str,
    account_id: &str,
) -> Value {
    let user_attrs: Value = user_attributes
        .iter()
        .map(|a| (a.name.clone(), Value::String(a.value.clone())))
        .collect::<serde_json::Map<String, Value>>()
        .into();

    let session: Vec<Value> = challenge_results
        .iter()
        .map(|cr| {
            let mut entry = json!({
                "challengeName": cr.challenge_name,
                "challengeResult": cr.challenge_result,
            });
            if let Some(ref metadata) = cr.challenge_metadata {
                entry["challengeMetadata"] = json!(metadata);
            }
            entry
        })
        .collect();

    let mut event = json!({
        "version": "1",
        "triggerSource": TriggerSource::DefineAuthChallengeAuthentication.as_str(),
        "region": region,
        "userPoolId": pool_id,
        "userName": username,
        "callerContext": {
            "awsSdkVersion": "fakecloud",
            "clientId": client_id.unwrap_or(""),
            "accountId": account_id,
        },
        "request": {
            "userAttributes": user_attrs,
            "session": session,
        },
        "response": {
            "challengeName": null,
            "issueTokens": false,
            "failAuthentication": false,
        },
    });

    event["callerContext"]["accountId"] = json!(account_id);
    event
}

/// Caller/user/pool identity bundled for auth-challenge trigger events.
pub struct AuthChallengeContext<'a> {
    pub pool_id: &'a str,
    pub client_id: Option<&'a str>,
    pub username: &'a str,
    pub user_attributes: &'a [UserAttribute],
    pub region: &'a str,
    pub account_id: &'a str,
}

impl<'a> AuthChallengeContext<'a> {
    fn caller_context(&self) -> Value {
        json!({
            "awsSdkVersion": "fakecloud",
            "clientId": self.client_id.unwrap_or(""),
            "accountId": self.account_id,
        })
    }

    fn user_attrs_value(&self) -> Value {
        self.user_attributes
            .iter()
            .map(|a| (a.name.clone(), Value::String(a.value.clone())))
            .collect::<serde_json::Map<String, Value>>()
            .into()
    }
}

/// Build a CreateAuthChallenge trigger event.
pub fn build_create_auth_challenge_event(
    ctx: &AuthChallengeContext<'_>,
    challenge_name: &str,
    challenge_results: &[ChallengeResult],
) -> Value {
    let session: Vec<Value> = challenge_results
        .iter()
        .map(|cr| {
            let mut entry = json!({
                "challengeName": cr.challenge_name,
                "challengeResult": cr.challenge_result,
            });
            if let Some(ref metadata) = cr.challenge_metadata {
                entry["challengeMetadata"] = json!(metadata);
            }
            entry
        })
        .collect();

    json!({
        "version": "1",
        "triggerSource": TriggerSource::CreateAuthChallengeAuthentication.as_str(),
        "region": ctx.region,
        "userPoolId": ctx.pool_id,
        "userName": ctx.username,
        "callerContext": ctx.caller_context(),
        "request": {
            "userAttributes": ctx.user_attrs_value(),
            "challengeName": challenge_name,
            "session": session,
        },
        "response": {
            "publicChallengeParameters": {},
            "privateChallengeParameters": {},
            "challengeMetadata": null,
        },
    })
}

/// Build a VerifyAuthChallengeResponse trigger event.
pub fn build_verify_auth_challenge_event(
    ctx: &AuthChallengeContext<'_>,
    challenge_answer: &str,
    challenge_metadata: Option<&str>,
) -> Value {
    json!({
        "version": "1",
        "triggerSource": TriggerSource::VerifyAuthChallengeResponseAuthentication.as_str(),
        "region": ctx.region,
        "userPoolId": ctx.pool_id,
        "userName": ctx.username,
        "callerContext": ctx.caller_context(),
        "request": {
            "userAttributes": ctx.user_attrs_value(),
            "challengeAnswer": challenge_answer,
            "privateChallengeParameters": {},
            "challengeMetadata": challenge_metadata.unwrap_or(""),
        },
        "response": {
            "answerCorrect": false,
        },
    })
}

/// Helper: collect user attributes from state for trigger event construction.
pub fn collect_user_attributes(user: &User) -> Vec<UserAttribute> {
    user.attributes.clone()
}

/// Build a CustomEmailSender / CustomSMSSender trigger event payload.
/// AWS includes the verification code under `request.code` (encrypted in
/// real AWS via the user pool's KMS key; we pass it base64-encoded so a
/// receiving Lambda has something deterministic to assert on, ahead of the
/// KMS-hooks batch wiring real envelope encryption here).
#[allow(clippy::too_many_arguments)]
pub fn build_custom_sender_event(
    trigger_source: TriggerSource,
    pool_id: &str,
    client_id: Option<&str>,
    username: &str,
    user_attributes: &[UserAttribute],
    code: &str,
    region: &str,
    account_id: &str,
) -> Value {
    use base64::Engine;
    let user_attrs: Value = user_attributes
        .iter()
        .map(|a| (a.name.clone(), Value::String(a.value.clone())))
        .collect::<serde_json::Map<String, Value>>()
        .into();
    let request_type = match trigger_source {
        TriggerSource::CustomSmsSenderSignUp
        | TriggerSource::CustomSmsSenderForgotPassword
        | TriggerSource::CustomSmsSenderResendCode
        | TriggerSource::CustomSmsSenderAuthentication
        | TriggerSource::CustomSmsSenderAdminCreateUser
        | TriggerSource::CustomSmsSenderUpdateUserAttribute
        | TriggerSource::CustomSmsSenderVerifyUserAttribute => "customSMSSenderRequestV1",
        _ => "customEmailSenderRequestV1",
    };
    json!({
        "version": "1",
        "triggerSource": trigger_source.as_str(),
        "region": region,
        "userPoolId": pool_id,
        "userName": username,
        "callerContext": {
            "awsSdkVersion": "fakecloud",
            "clientId": client_id.unwrap_or(""),
            "accountId": account_id,
        },
        "request": {
            "userAttributes": user_attrs,
            "code": base64::engine::general_purpose::STANDARD.encode(code.as_bytes()),
            "type": request_type,
        },
        "response": {},
    })
}

/// Email message materialized from a verification template.
pub struct VerificationEmail {
    pub from: String,
    pub subject: String,
    pub body_text: String,
    pub body_html: Option<String>,
}

/// Render a Cognito verification email message by substituting the
/// `{####}` and `{username}` placeholders. Falls back to defaults that
/// match Cognito's out-of-the-box templates when the pool has no
/// custom `VerificationMessageTemplate`.
pub fn render_verification_email(
    from_address: Option<&str>,
    subject_template: Option<&str>,
    message_template: Option<&str>,
    username: &str,
    code: &str,
) -> VerificationEmail {
    let subject = subject_template
        .unwrap_or("Your verification code")
        .replace("{####}", code)
        .replace("{username}", username);
    let body_template = message_template.unwrap_or("Your confirmation code is {####}");
    let body_html = body_template
        .replace("{####}", code)
        .replace("{username}", username);
    // Strip the template's markup, not the values put into it: a code or a
    // temporary password may hold `<` or `>`.
    let body_text = strip_html(body_template)
        .replace("{####}", code)
        .replace("{username}", username);
    VerificationEmail {
        from: from_address
            .unwrap_or("no-reply@fakecloud.local")
            .to_string(),
        subject,
        body_text,
        body_html: Some(body_html),
    }
}

fn strip_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut in_tag = false;
    for c in s.chars() {
        match c {
            '<' => in_tag = true,
            '>' => in_tag = false,
            _ if !in_tag => out.push(c),
            _ => {}
        }
    }
    out
}

/// Render an SMS verification message by substituting placeholders.
pub fn render_verification_sms(sms_template: Option<&str>, username: &str, code: &str) -> String {
    sms_template
        .unwrap_or("Your verification code is {####}")
        .replace("{####}", code)
        .replace("{username}", username)
}

#[cfg(test)]
mod tests {
    #[test]
    fn text_body_keeps_markup_like_characters_of_the_code() {
        let email = render_verification_email(
            None,
            None,
            Some("<p>Your password is <b>{####}</b></p>"),
            "u",
            "a<b>c&d",
        );
        assert_eq!(email.body_text, "Your password is a<b>c&d");
    }

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
            "PostConfirmation_ConfirmSignUp"
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
        assert_eq!(
            TriggerSource::CustomMessageAdminCreateUser.as_str(),
            "CustomMessage_AdminCreateUser"
        );
        assert_eq!(
            TriggerSource::CustomMessageResendCode.as_str(),
            "CustomMessage_ResendCode"
        );
        assert_eq!(
            TriggerSource::CustomMessageVerifyUserAttribute.as_str(),
            "CustomMessage_VerifyUserAttribute"
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
        assert_eq!(event["triggerSource"], "PostConfirmation_ConfirmSignUp");
    }

    #[test]
    fn get_trigger_arn_from_lambda_config() {
        use parking_lot::RwLock;
        use std::sync::Arc;

        let state = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new(
                "123456789012",
                "us-east-1",
                "http://localhost:4569",
            ),
        ));

        // No pool -> None
        assert!(get_trigger_arn(&state, "pool-1", TriggerSource::PreSignUpSignUp).is_none());

        // Add pool with lambda config
        {
            let mut mas = state.write();
            let s = mas.default_mut();
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
                        sign_in_policy: crate::state::SignInPolicy {
                            allowed_first_auth_factors: vec!["PASSWORD".to_string()],
                        },
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
                    user_pool_tags: std::collections::BTreeMap::new(),
                    account_recovery_setting: None,
                    deletion_protection: None,
                    estimated_number_of_users: 0,
                    software_token_mfa_configuration: None,
                    sms_mfa_configuration: None,
                    user_pool_tier: "ESSENTIALS".to_string(),
                    verification_message_template: None,
                    signing_key_pem: None,
                    signing_kid: None,
                    email_verification_message: None,
                    email_verification_subject: None,
                    sms_verification_message: None,
                    sms_authentication_message: None,
                    device_configuration: None,
                    user_attribute_update_settings: None,
                    user_pool_add_ons: None,
                    username_configuration: None,
                    acr_configuration: Default::default(),
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

    #[test]
    fn custom_auth_trigger_source_strings() {
        assert_eq!(
            TriggerSource::DefineAuthChallengeAuthentication.as_str(),
            "DefineAuthChallenge_Authentication"
        );
        assert_eq!(
            TriggerSource::CreateAuthChallengeAuthentication.as_str(),
            "CreateAuthChallenge_Authentication"
        );
        assert_eq!(
            TriggerSource::VerifyAuthChallengeResponseAuthentication.as_str(),
            "VerifyAuthChallengeResponse_Authentication"
        );
    }

    #[test]
    fn custom_auth_trigger_lambda_config_keys() {
        assert_eq!(
            TriggerSource::DefineAuthChallengeAuthentication.lambda_config_key(),
            "DefineAuthChallenge"
        );
        assert_eq!(
            TriggerSource::CreateAuthChallengeAuthentication.lambda_config_key(),
            "CreateAuthChallenge"
        );
        assert_eq!(
            TriggerSource::VerifyAuthChallengeResponseAuthentication.lambda_config_key(),
            "VerifyAuthChallengeResponse"
        );
    }

    #[test]
    fn build_define_auth_challenge_event_structure() {
        let attrs = vec![UserAttribute {
            name: "sub".to_string(),
            value: "abc-123".to_string(),
        }];

        let results = vec![ChallengeResult {
            challenge_name: "CUSTOM_CHALLENGE".to_string(),
            challenge_result: true,
            challenge_metadata: None,
        }];

        let event = build_define_auth_challenge_event(
            "us-east-1_abc",
            Some("client-id-1"),
            "testuser",
            &attrs,
            &results,
            "us-east-1",
            "123456789012",
        );

        assert_eq!(event["version"], "1");
        assert_eq!(event["triggerSource"], "DefineAuthChallenge_Authentication");
        assert_eq!(event["userName"], "testuser");
        assert_eq!(event["request"]["userAttributes"]["sub"], "abc-123");

        let session = event["request"]["session"].as_array().unwrap();
        assert_eq!(session.len(), 1);
        assert_eq!(session[0]["challengeName"], "CUSTOM_CHALLENGE");
        assert_eq!(session[0]["challengeResult"], true);

        assert_eq!(event["response"]["issueTokens"], false);
        assert_eq!(event["response"]["failAuthentication"], false);
    }

    #[test]
    fn build_create_auth_challenge_event_structure() {
        let attrs = vec![UserAttribute {
            name: "email".to_string(),
            value: "user@example.com".to_string(),
        }];
        let ctx = AuthChallengeContext {
            pool_id: "us-east-1_abc",
            client_id: Some("client-id-1"),
            username: "testuser",
            user_attributes: &attrs,
            region: "us-east-1",
            account_id: "123456789012",
        };

        let event = build_create_auth_challenge_event(&ctx, "CUSTOM_CHALLENGE", &[]);

        assert_eq!(event["triggerSource"], "CreateAuthChallenge_Authentication");
        assert_eq!(event["request"]["challengeName"], "CUSTOM_CHALLENGE");
        assert!(event["request"]["session"].as_array().unwrap().is_empty());
        assert!(event["response"]["publicChallengeParameters"]
            .as_object()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn build_verify_auth_challenge_event_structure() {
        let attrs = vec![UserAttribute {
            name: "sub".to_string(),
            value: "abc-123".to_string(),
        }];
        let ctx = AuthChallengeContext {
            pool_id: "us-east-1_abc",
            client_id: Some("client-id-1"),
            username: "testuser",
            user_attributes: &attrs,
            region: "us-east-1",
            account_id: "123456789012",
        };

        let event = build_verify_auth_challenge_event(&ctx, "my-answer", Some("challenge-meta"));

        assert_eq!(
            event["triggerSource"],
            "VerifyAuthChallengeResponse_Authentication"
        );
        assert_eq!(event["request"]["challengeAnswer"], "my-answer");
        assert_eq!(event["request"]["challengeMetadata"], "challenge-meta");
        assert_eq!(event["response"]["answerCorrect"], false);
    }

    #[test]
    fn define_auth_challenge_event_empty_session() {
        let event = build_define_auth_challenge_event(
            "us-east-1_abc",
            Some("client-id-1"),
            "testuser",
            &[],
            &[],
            "us-east-1",
            "123456789012",
        );

        assert!(event["request"]["session"].as_array().unwrap().is_empty());
    }
}
