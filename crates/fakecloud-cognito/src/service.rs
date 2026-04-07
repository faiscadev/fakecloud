use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{
    default_schema_attributes, AccountRecoverySetting, AdminCreateUserConfig, EmailConfiguration,
    InviteMessageTemplate, PasswordPolicy, PoolPolicies, RecoveryOption, SchemaAttribute,
    SharedCognitoState, SmsConfiguration, StringAttributeConstraints, UserPool,
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
}
