use chrono::Utc;
use http::StatusCode;
use serde_json::json;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{
    MfaPreferences, SmsConfiguration, SmsMfaConfiguration, SoftwareTokenMfaConfiguration,
};
// AccessTokenData is used via state.access_tokens.get()

use super::{generate_totp_secret, require_str, CognitoService};

impl CognitoService {
    pub(super) fn set_user_pool_mfa_config(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn get_user_pool_mfa_config(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn admin_set_user_mfa_preference(
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

    pub(super) fn set_user_mfa_preference(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn associate_software_token(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn verify_software_token(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn get_user_auth_factors(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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
            .get(pool_id.as_str())
            .and_then(|users| users.get(username.as_str()))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    "User not found.",
                )
            })?;

        // Build configured auth factors
        let mut factors = vec!["PASSWORD"];
        if user.totp_verified {
            factors.push("SMS_OTP");
        }

        // Build MFA settings
        let mut mfa_settings = Vec::new();
        let mut preferred = None;
        if let Some(ref prefs) = user.mfa_preferences {
            if prefs.sms_enabled {
                mfa_settings.push("SMS_MFA");
                if prefs.sms_preferred {
                    preferred = Some("SMS_MFA");
                }
            }
            if prefs.software_token_enabled {
                mfa_settings.push("SOFTWARE_TOKEN_MFA");
                if prefs.software_token_preferred {
                    preferred = Some("SOFTWARE_TOKEN_MFA");
                }
            }
        }

        let mut resp = json!({
            "Username": username,
            "ConfiguredUserAuthFactors": factors,
        });
        if !mfa_settings.is_empty() {
            resp["UserMFASettingList"] = json!(mfa_settings);
        }
        if let Some(pref) = preferred {
            resp["PreferredMfaSetting"] = json!(pref);
        }

        Ok(AwsResponse::ok_json(resp))
    }
}
