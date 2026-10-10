use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{
    default_schema_attributes, ClientSecretDescriptor, CognitoState, PoolPolicies, UserPool,
    UserPoolClient,
};

use super::{
    ensure_user_pool_exists, generate_client_id, generate_client_secret, generate_pool_id,
    parse_account_recovery_setting, parse_admin_create_user_config, parse_email_configuration,
    parse_password_policy, parse_refresh_token_rotation, parse_schema_attribute,
    parse_sign_in_policy, parse_sms_configuration, parse_string_array, parse_tags,
    parse_token_validity_units, parse_verification_message_template, require_str,
    resolve_token_validity, user_pool_client_to_json, user_pool_to_json, validate_enum,
    validate_range, validate_string_length, CognitoService,
};

/// Reserved `UserPoolTags` key (and `ClientName` prefix, as
/// `_custom_id_:<id>`) that picks the id of a new user pool or app client
/// instead of a random one, the same convention LocalStack uses.
pub const CUSTOM_ID_TAG: &str = "_custom_id_";

type CognitoAccounts = fakecloud_core::multi_account::MultiAccountState<CognitoState>;

/// Validate a `_custom_id_` user pool tag value. The region is read back out
/// of the pool id, so it must be `<region>_<alphanumeric id>` in `region`.
pub fn custom_user_pool_id(tag_value: &str, region: &str) -> Result<String, String> {
    let valid = tag_value
        .strip_prefix(region)
        .and_then(|rest| rest.strip_prefix('_'))
        .is_some_and(|suffix| {
            !suffix.is_empty() && suffix.chars().all(|c| c.is_ascii_alphanumeric())
        })
        && tag_value.len() <= 55;
    if !valid {
        return Err(format!(
            "Invalid {CUSTOM_ID_TAG} tag value {tag_value}: a custom user pool id must be <region>_<alphanumeric id> in the request's region ({region}) and at most 55 characters."
        ));
    }
    Ok(tag_value.to_string())
}

/// The custom client id a `_custom_id_:<id>` client name asks for, if any.
/// App clients have no tags, so the name carries it.
pub fn custom_client_id(client_name: &str) -> Result<Option<String>, String> {
    let Some(id) = client_name
        .strip_prefix(CUSTOM_ID_TAG)
        .and_then(|rest| rest.strip_prefix(':'))
    else {
        return Ok(None);
    };
    let valid = (1..=128).contains(&id.len())
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '+');
    if !valid {
        return Err(format!(
            "Invalid custom client id {id}: it must be 1-128 characters of letters, digits, _ and +."
        ));
    }
    Ok(Some(id.to_string()))
}

/// Pool ids are global: the JWKS, discovery and hosted-UI routes find a pool
/// by id across every account, so a custom id must be unused everywhere.
pub fn ensure_user_pool_id_unused(accounts: &CognitoAccounts, id: &str) -> Result<(), String> {
    if accounts.iter().any(|(_, a)| a.user_pools.contains_key(id)) {
        return Err(format!("User pool {id} already exists."));
    }
    Ok(())
}

/// Client ids are global: the OAuth2 endpoints find a client by id across
/// every account, so a custom id must be unused everywhere.
pub fn ensure_user_pool_client_id_unused(
    accounts: &CognitoAccounts,
    id: &str,
) -> Result<(), String> {
    if accounts
        .iter()
        .any(|(_, a)| a.user_pool_clients.contains_key(id))
    {
        return Err(format!("User pool client {id} already exists."));
    }
    Ok(())
}

fn invalid_parameter(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterException",
        msg.into(),
    )
}

impl CognitoService {
    pub(super) async fn create_user_pool(
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

        validate_string_length(pool_name, "poolName", 1, 128)?;

        if let Some(dp) = body["DeletionProtection"].as_str() {
            validate_enum(dp, "deletionProtection", &["ACTIVE", "INACTIVE"])?;
        }

        if let Some(mfa) = body["MfaConfiguration"].as_str() {
            validate_enum(mfa, "mfaConfiguration", &["OFF", "ON", "OPTIONAL"])?;
        }

        if let Some(tier) = body["UserPoolTier"].as_str() {
            validate_enum(tier, "userPoolTier", &["LITE", "ESSENTIALS", "PLUS"])?;
        }

        if let Some(msg) = body["EmailVerificationMessage"].as_str() {
            validate_string_length(msg, "emailVerificationMessage", 6, 20000)?;
        }

        if let Some(subj) = body["EmailVerificationSubject"].as_str() {
            validate_string_length(subj, "emailVerificationSubject", 1, 140)?;
        }

        if let Some(msg) = body["SmsAuthenticationMessage"].as_str() {
            validate_string_length(msg, "smsAuthenticationMessage", 6, 140)?;
        }

        if let Some(msg) = body["SmsVerificationMessage"].as_str() {
            validate_string_length(msg, "smsVerificationMessage", 6, 140)?;
        }

        // Custom ACR level names need the Essentials or Plus feature plan.
        let acr_configuration = match body.get("AcrConfiguration").filter(|v| !v.is_null()) {
            Some(v) => crate::acr::parse_acr_configuration(v)?,
            None => Default::default(),
        };
        let tier = body["UserPoolTier"].as_str().unwrap_or("ESSENTIALS");
        if !acr_configuration.is_empty() && !crate::acr::tier_supports_acr(tier) {
            return Err(crate::acr::feature_unavailable(
                "Custom ACR level names require the Essentials or Plus feature plan.",
            ));
        }

        let custom_pool_id = body["UserPoolTags"][CUSTOM_ID_TAG]
            .as_str()
            .map(|id| custom_user_pool_id(id, req.region.as_str()))
            .transpose()
            .map_err(invalid_parameter)?;

        // Generate the per-pool RSA-2048 keypair eagerly so every
        // token-issuing path (InitiateAuth, RespondToAuthChallenge,
        // AdminInitiateAuth, GetTokensFromRefreshToken, OAuth2 token
        // grant) signs with a real RS256 signature out of the box.
        // Real AWS Cognito assigns the keypair at pool creation time
        // and derives the JWKS `kid` from a hash of the public half so
        // it stays stable across snapshots.
        //
        // RSA-2048 keygen is CPU-bound and costs 100-500 ms — run it on
        // a blocking thread before touching any locks so the async
        // runtime stays responsive under bursty CreateUserPool
        // workloads (conformance probes, integration tests).
        let signing = tokio::task::spawn_blocking(crate::jwt::generate_pool_signing_key)
            .await
            .map_err(|e| {
                AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalErrorException",
                    format!("keygen task failed: {e}"),
                )
            })?;
        let signing_key_pem = signing.private_key_pem;
        let signing_kid = signing.kid;

        let mut accounts = self.state.write();
        if let Some(id) = &custom_pool_id {
            ensure_user_pool_id_unused(&accounts, id).map_err(invalid_parameter)?;
        }
        let state = accounts.get_or_create(&req.account_id);
        // The pool id bakes in the request's credential-scope region
        // (`{region}_{rand}`) so it becomes the single source of truth for the
        // pool's region; the ARN and every issuer/discovery derivation read the
        // region back out of the pool id. Storage is keyed by pool id, unchanged.
        // A `_custom_id_` tag picks the id instead, so local setups can create
        // a pool whose id is known in advance.
        let pool_id = custom_pool_id.unwrap_or_else(|| generate_pool_id(req.region.as_str()));
        let arn = crate::user_pool_arn(req.region.as_str(), &state.account_id, &pool_id);

        let now = Utc::now();

        // Parse password policy or use defaults
        let password_policy = parse_password_policy(&body["Policies"]["PasswordPolicy"]);
        let sign_in_policy = parse_sign_in_policy(&body["Policies"]["SignInPolicy"]);

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

        let user_pool_tier = body["UserPoolTier"]
            .as_str()
            .unwrap_or("ESSENTIALS")
            .to_string();

        let verification_message_template =
            parse_verification_message_template(&body["VerificationMessageTemplate"]);

        let pool = UserPool {
            id: pool_id.clone(),
            name: pool_name.to_string(),
            arn,
            status: "ACTIVE".to_string(),
            creation_date: now,
            last_modified_date: now,
            policies: PoolPolicies {
                password_policy,
                sign_in_policy,
            },
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
            user_pool_tier,
            verification_message_template,
            signing_key_pem: Some(signing_key_pem),
            signing_kid: Some(signing_kid),
            email_verification_message: body["EmailVerificationMessage"]
                .as_str()
                .map(|s| s.to_string()),
            email_verification_subject: body["EmailVerificationSubject"]
                .as_str()
                .map(|s| s.to_string()),
            sms_verification_message: body["SmsVerificationMessage"]
                .as_str()
                .map(|s| s.to_string()),
            sms_authentication_message: body["SmsAuthenticationMessage"]
                .as_str()
                .map(|s| s.to_string()),
            device_configuration: if body["DeviceConfiguration"].is_object() {
                Some(body["DeviceConfiguration"].clone())
            } else {
                None
            },
            user_attribute_update_settings: if body["UserAttributeUpdateSettings"].is_object() {
                Some(body["UserAttributeUpdateSettings"].clone())
            } else {
                None
            },
            user_pool_add_ons: if body["UserPoolAddOns"].is_object() {
                Some(body["UserPoolAddOns"].clone())
            } else {
                None
            },
            username_configuration: if body["UsernameConfiguration"].is_object() {
                Some(body["UsernameConfiguration"].clone())
            } else {
                None
            },
            acr_configuration,
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

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let pool = state.user_pools.get_mut(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        // Validate the ACR level names (and the feature plan they need)
        // before changing anything, so a rejected request leaves the pool as
        // it was. AcrConfiguration replaces the pool's custom names; levels it
        // leaves out go back to their default names.
        let acr_configuration = match body.get("AcrConfiguration").filter(|v| !v.is_null()) {
            Some(v) => Some(crate::acr::parse_acr_configuration(v)?),
            None => None,
        };
        if let Some(tier) = body["UserPoolTier"].as_str() {
            validate_enum(tier, "userPoolTier", &["LITE", "ESSENTIALS", "PLUS"])?;
        }
        let new_tier = body["UserPoolTier"]
            .as_str()
            .unwrap_or(pool.user_pool_tier.as_str());
        let custom_names = acr_configuration
            .as_ref()
            .unwrap_or(&pool.acr_configuration);
        if !custom_names.is_empty() && !crate::acr::tier_supports_acr(new_tier) {
            return Err(crate::acr::feature_unavailable(
                "Custom ACR level names require the Essentials or Plus feature plan.",
            ));
        }
        if let Some(names) = acr_configuration {
            pool.acr_configuration = names;
        }

        // Update fields that are present in the request
        if body["Policies"]["PasswordPolicy"].is_object() {
            pool.policies.password_policy =
                parse_password_policy(&body["Policies"]["PasswordPolicy"]);
        }

        if body["Policies"]["SignInPolicy"].is_object() {
            pool.policies.sign_in_policy = parse_sign_in_policy(&body["Policies"]["SignInPolicy"]);
        }

        if body["VerificationMessageTemplate"].is_object() {
            pool.verification_message_template =
                parse_verification_message_template(&body["VerificationMessageTemplate"]);
        }

        if let Some(tier) = body["UserPoolTier"].as_str() {
            validate_enum(tier, "userPoolTier", &["LITE", "ESSENTIALS", "PLUS"])?;
            pool.user_pool_tier = tier.to_string();
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

        // Settable fields the handler previously dropped: custom verification
        // copy and the device/attribute-update/add-ons configs. The most common
        // UpdateUserPool change (custom verification email/SMS) was silently
        // lost (bug-audit 2026-06-20, 1.16). Update each only when present.
        if let Some(s) = body["EmailVerificationMessage"].as_str() {
            pool.email_verification_message = Some(s.to_string());
        }
        if let Some(s) = body["EmailVerificationSubject"].as_str() {
            pool.email_verification_subject = Some(s.to_string());
        }
        if let Some(s) = body["SmsVerificationMessage"].as_str() {
            pool.sms_verification_message = Some(s.to_string());
        }
        if let Some(s) = body["SmsAuthenticationMessage"].as_str() {
            pool.sms_authentication_message = Some(s.to_string());
        }
        if body["DeviceConfiguration"].is_object() {
            pool.device_configuration = Some(body["DeviceConfiguration"].clone());
        }
        if body["UserAttributeUpdateSettings"].is_object() {
            pool.user_attribute_update_settings = Some(body["UserAttributeUpdateSettings"].clone());
        }
        if body["UserPoolAddOns"].is_object() {
            pool.user_pool_add_ons = Some(body["UserPoolAddOns"].clone());
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let Some(pool) = state.user_pools.remove(pool_id) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        };

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

        // Remove associated multi-region replicas
        state.user_pool_replicas.remove(pool_id);

        // Remove associated domains
        state.domains.retain(|_, d| d.user_pool_id != pool_id);

        // Remove the tags stored under the pool's ARN.
        state.tags.remove(&pool.arn);

        // Remove associated import jobs
        state.import_jobs.remove(pool_id);

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_user_pools(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        let max_results = body["MaxResults"]
            .as_i64()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "1 validation error detected: Value at 'maxResults' failed to satisfy constraint: Member must not be null",
                )
            })?;
        validate_range(max_results, "maxResults", 1, 60)?;
        let max_results = max_results as usize;

        let next_token = body["NextToken"].as_str();
        if let Some(token) = next_token {
            validate_string_length(token, "nextToken", 1, usize::MAX)?;
        }

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        // Sort pools by creation date for consistent pagination
        let mut pools: Vec<&UserPool> = state.user_pools.values().collect();
        pools.sort_by_key(|p| p.creation_date);

        // Find start index from NextToken
        let start_idx = if let Some(token) = next_token {
            pools
                .iter()
                .position(|p| p.id == token)
                .unwrap_or(pools.len())
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

        let validity = resolve_token_validity(
            body["AccessTokenValidity"].as_i64(),
            body["IdTokenValidity"].as_i64(),
            body["RefreshTokenValidity"].as_i64(),
            parse_token_validity_units(&body["TokenValidityUnits"]),
        )
        .map_err(|m| {
            AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidParameterException", m)
        })?;

        // App clients have no tags, so a `_custom_id_:<id>` ClientName picks
        // the client id instead.
        let custom_client_id = custom_client_id(client_name).map_err(invalid_parameter)?;

        let mut accounts = self.state.write();
        if let Some(id) = &custom_client_id {
            ensure_user_pool_client_id_unused(&accounts, id).map_err(invalid_parameter)?;
        }
        let state = accounts.get_or_create(&req.account_id);

        // Validate pool exists
        ensure_user_pool_exists(state, pool_id)?;

        let client_id = custom_client_id.unwrap_or_else(generate_client_id);
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
            token_validity_units: validity.units,
            access_token_validity: validity.access,
            id_token_validity: validity.id,
            // AWS defaults RefreshTokenValidity to 30 days when unset (or 0)
            // and always reports it, in the client's refresh-token unit;
            // access/id token validity stay unset (0).
            refresh_token_validity: Some(validity.refresh),
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
            // AWS defaults AuthSessionValidity to 3 (minutes) when unset and
            // always reports it.
            auth_session_validity: Some(body["AuthSessionValidity"].as_i64().unwrap_or(3)),
            enable_propagate_additional_user_context_data: body
                ["EnablePropagateAdditionalUserContextData"]
                .as_bool()
                .unwrap_or(false),
            client_secrets: Vec::new(),
            refresh_token_rotation: parse_refresh_token_rotation(&body["RefreshTokenRotation"]),
            analytics_configuration: body
                .get("AnalyticsConfiguration")
                .filter(|v| !v.is_null())
                .cloned(),
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

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        // Validate pool exists
        ensure_user_pool_exists(state, pool_id)?;

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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Validate pool exists
        ensure_user_pool_exists(state, pool_id)?;

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

        // UpdateUserPoolClient resets omitted fields to their defaults, so the
        // token lifetimes resolve from the request alone (never from stored
        // values that may be in different units). Validated before mutating.
        let validity = resolve_token_validity(
            body["AccessTokenValidity"].as_i64(),
            body["IdTokenValidity"].as_i64(),
            body["RefreshTokenValidity"].as_i64(),
            parse_token_validity_units(&body["TokenValidityUnits"]),
        )
        .map_err(|m| {
            AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidParameterException", m)
        })?;

        // UpdateUserPoolClient replaces the client's configuration: every
        // setting the request omits goes back to its default (what
        // CreateUserPoolClient would give it), as AWS documents -- it is not
        // a merge. Only the name (and the client's id and secret) carry over
        // when not sent.
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
        client.explicit_auth_flows = parse_string_array(&body["ExplicitAuthFlows"]);
        client.token_validity_units = validity.units;
        client.access_token_validity = validity.access;
        client.id_token_validity = validity.id;
        client.refresh_token_validity = Some(validity.refresh);
        client.callback_urls = parse_string_array(&body["CallbackURLs"]);
        client.logout_urls = parse_string_array(&body["LogoutURLs"]);
        client.supported_identity_providers =
            parse_string_array(&body["SupportedIdentityProviders"]);
        client.allowed_o_auth_flows = parse_string_array(&body["AllowedOAuthFlows"]);
        client.allowed_o_auth_scopes = parse_string_array(&body["AllowedOAuthScopes"]);
        client.allowed_o_auth_flows_user_pool_client = body["AllowedOAuthFlowsUserPoolClient"]
            .as_bool()
            .unwrap_or(false);
        client.prevent_user_existence_errors = body["PreventUserExistenceErrors"]
            .as_str()
            .map(|s| s.to_string());
        client.read_attributes = parse_string_array(&body["ReadAttributes"]);
        client.write_attributes = parse_string_array(&body["WriteAttributes"]);
        client.enable_token_revocation = body["EnableTokenRevocation"].as_bool().unwrap_or(true);
        client.auth_session_validity = Some(body["AuthSessionValidity"].as_i64().unwrap_or(3));
        client.enable_propagate_additional_user_context_data = body
            ["EnablePropagateAdditionalUserContextData"]
            .as_bool()
            .unwrap_or(false);
        client.refresh_token_rotation = parse_refresh_token_rotation(&body["RefreshTokenRotation"]);
        client.analytics_configuration = body
            .get("AnalyticsConfiguration")
            .filter(|v| !v.is_null())
            .cloned();

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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // Validate pool exists
        ensure_user_pool_exists(state, pool_id)?;

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

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        // Validate pool exists
        ensure_user_pool_exists(state, pool_id)?;

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
                .unwrap_or(clients.len())
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

    pub(super) fn add_custom_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;

        let custom_attrs = body["CustomAttributes"].as_array().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "CustomAttributes is required",
            )
        })?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let pool = state.user_pools.get_mut(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            )
        })?;

        for attr_val in custom_attrs {
            if let Some(mut attr) = parse_schema_attribute(attr_val) {
                // Ensure custom attributes have the custom: prefix
                if !attr.name.starts_with("custom:") {
                    attr.name = format!("custom:{}", attr.name);
                }
                // Don't add duplicates
                if !pool.schema_attributes.iter().any(|a| a.name == attr.name) {
                    pool.schema_attributes.push(attr);
                }
            }
        }

        pool.last_modified_date = Utc::now();

        Ok(AwsResponse::ok_json(json!({})))
    }

    // ── Client Secrets ─────────────────────────────────────────────────

    pub(super) fn add_user_pool_client_secret(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let client_id = require_str(&body, "ClientId")?;
        let custom_secret = body["ClientSecret"].as_str();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        ensure_user_pool_exists(state, pool_id)?;

        let upc = state.user_pool_clients.get_mut(client_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Client {client_id} does not exist."),
            )
        })?;

        let now = Utc::now();
        let secret_value = custom_secret
            .map(|s| s.to_string())
            .unwrap_or_else(generate_client_secret);
        let secret_id = format!("{}--{}", client_id, now.timestamp());

        let descriptor = ClientSecretDescriptor {
            client_secret_id: secret_id,
            client_secret_value: secret_value,
            client_secret_create_date: now,
        };

        let resp = json!({
            "ClientSecretDescriptor": {
                "ClientSecretId": descriptor.client_secret_id,
                "ClientSecretValue": descriptor.client_secret_value,
                "ClientSecretCreateDate": descriptor.client_secret_create_date.timestamp() as f64,
            }
        });

        upc.client_secrets.push(descriptor);

        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn delete_user_pool_client_secret(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let client_id = require_str(&body, "ClientId")?;
        let secret_id = require_str(&body, "ClientSecretId")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        ensure_user_pool_exists(state, pool_id)?;

        let upc = state.user_pool_clients.get_mut(client_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Client {client_id} does not exist."),
            )
        })?;

        let idx = upc
            .client_secrets
            .iter()
            .position(|s| s.client_secret_id == secret_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Client secret {secret_id} does not exist."),
                )
            })?;

        upc.client_secrets.remove(idx);

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_user_pool_client_secrets(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let client_id = require_str(&body, "ClientId")?;
        let _next_token = body["NextToken"].as_str();

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        ensure_user_pool_exists(state, pool_id)?;

        let upc = state.user_pool_clients.get(client_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Client {client_id} does not exist."),
            )
        })?;

        let secrets: Vec<Value> = upc
            .client_secrets
            .iter()
            .map(|s| {
                json!({
                    "ClientSecretId": s.client_secret_id,
                    "ClientSecretCreateDate": s.client_secret_create_date.timestamp() as f64,
                })
            })
            .collect();

        Ok(AwsResponse::ok_json(json!({
            "ClientSecrets": secrets
        })))
    }

    pub(super) fn get_signing_certificate(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        ensure_user_pool_exists(state, pool_id)?;

        // Issue a real self-signed X.509 cert bound to the pool's RSA
        // signing key. Verifying the cert's public key against pool-issued
        // JWTs roundtrips: the JWT was signed by the same private key
        // whose SubjectPublicKeyInfo lives in this cert.
        let pem = state
            .user_pools
            .get(pool_id)
            .and_then(|p| p.signing_key_pem.clone())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "InternalErrorException",
                    "user pool has no signing key",
                )
            })?;
        drop(accounts);

        let der = build_signing_cert_der(pool_id, &pem).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "InternalErrorException",
                "failed to build signing certificate",
            )
        })?;
        let cert_b64 = {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.encode(&der)
        };

        Ok(AwsResponse::ok_json(json!({
            "Certificate": cert_b64
        })))
    }
}

/// Build a self-signed X.509 cert whose subject is `CN=<pool_id>` and
/// whose key is the pool's existing RSA signing key. Returns DER bytes.
/// Returns `None` if `signing_key_pem` is not a parseable RSA PKCS#8 PEM
/// or rcgen rejects the params.
fn build_signing_cert_der(pool_id: &str, signing_key_pem: &str) -> Option<Vec<u8>> {
    use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair};

    let key_pair = KeyPair::from_pem(signing_key_pem).ok()?;
    let mut params = CertificateParams::new(vec![pool_id.to_string()]).ok()?;
    let mut dn = DistinguishedName::new();
    dn.push(DnType::CommonName, pool_id);
    dn.push(DnType::OrganizationName, "fakecloud Cognito");
    params.distinguished_name = dn;

    let cert = params.self_signed(&key_pair).ok()?;
    Some(cert.der().to_vec())
}
