//! Cognito Federated Identity Pools (`cognito-identity` service).
//!
//! Distinct from `cognito-idp` (User Pools): identity pools issue
//! temporary AWS credentials by federating a login provider (Cognito
//! User Pool, Google, Facebook, SAML, OIDC, or developer-authenticated)
//! through an attached IAM role. This module implements the
//! `AWSCognitoIdentityService.*` JSON-RPC operations against the
//! `identity_pools` / `identity_pool_role_attachments` /
//! `federated_identities` maps in [`CognitoState`].
//!
//! Credential issuance (`GetCredentialsForIdentity`) walks the role
//! attachment to pick `authenticated` vs `unauthenticated` based on
//! whether the caller supplied any `Logins`, then mints real
//! STS-style temporary credentials by writing into the IAM crate's
//! `sts_temp_credentials` map. This is what makes the end-to-end
//! Cognito Identity flow gate the IAM evaluator the same way real
//! AssumeRoleWithWebIdentity does.

use std::collections::BTreeMap;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_iam::xml_responses::{generate_role_id, StsCredentials};
use fakecloud_iam::{CredentialIdentity, SharedIamState, StsTempCredential};

use crate::state::{
    CognitoIdentityProvider, CognitoState, FederatedIdentity, IdentityPool,
    IdentityPoolRoleAttachment, PrincipalTagAttributeMap, SharedCognitoState,
};

/// Service handler for `cognito-identity` (Federated Identity Pools).
///
/// Shares the same `SharedCognitoState` as the `CognitoService` (which
/// handles `cognito-idp`) so identity pools live next to user pools in
/// state. Holds an `Arc<RwLock>` handle on the IAM state so that
/// `GetCredentialsForIdentity` can write minted credentials directly
/// into the IAM crate's `sts_temp_credentials` map; the SigV4 verifier
/// then resolves them on subsequent calls just like an AssumeRole
/// session.
pub struct CognitoIdentityService {
    state: SharedCognitoState,
    iam_state: SharedIamState,
}

impl CognitoIdentityService {
    pub fn new(state: SharedCognitoState, iam_state: SharedIamState) -> Self {
        Self { state, iam_state }
    }
}

#[async_trait]
impl AwsService for CognitoIdentityService {
    fn service_name(&self) -> &str {
        "cognito-identity"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match req.action.as_str() {
            "CreateIdentityPool" => self.create_identity_pool(&req),
            "DescribeIdentityPool" => self.describe_identity_pool(&req),
            "UpdateIdentityPool" => self.update_identity_pool(&req),
            "DeleteIdentityPool" => self.delete_identity_pool(&req),
            "ListIdentityPools" => self.list_identity_pools(&req),
            "GetId" => self.get_id(&req),
            "GetOpenIdToken" => self.get_open_id_token(&req),
            "GetOpenIdTokenForDeveloperIdentity" => {
                self.get_open_id_token_for_developer_identity(&req)
            }
            "GetCredentialsForIdentity" => self.get_credentials_for_identity(&req),
            "GetIdentityPoolRoles" => self.get_identity_pool_roles(&req),
            "SetIdentityPoolRoles" => self.set_identity_pool_roles(&req),
            "MergeDeveloperIdentities" => self.merge_developer_identities(&req),
            "UnlinkDeveloperIdentity" => self.unlink_developer_identity(&req),
            "LookupDeveloperIdentity" => self.lookup_developer_identity(&req),
            "UnlinkIdentity" => self.unlink_identity(&req),
            "ListIdentities" => self.list_identities(&req),
            "DescribeIdentity" => self.describe_identity(&req),
            "TagResource" => self.tag_resource(&req),
            "UntagResource" => self.untag_resource(&req),
            "ListTagsForResource" => self.list_tags_for_resource(&req),
            "DeleteIdentities" => self.delete_identities(&req),
            "GetPrincipalTagAttributeMap" => self.get_principal_tag_attribute_map(&req),
            "SetPrincipalTagAttributeMap" => self.set_principal_tag_attribute_map(&req),
            _ => Err(AwsServiceError::action_not_implemented(
                "cognito-identity",
                &req.action,
            )),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "CreateIdentityPool",
            "DescribeIdentityPool",
            "UpdateIdentityPool",
            "DeleteIdentityPool",
            "ListIdentityPools",
            "GetId",
            "GetOpenIdToken",
            "GetOpenIdTokenForDeveloperIdentity",
            "GetCredentialsForIdentity",
            "GetIdentityPoolRoles",
            "SetIdentityPoolRoles",
            "MergeDeveloperIdentities",
            "UnlinkDeveloperIdentity",
            "LookupDeveloperIdentity",
            "UnlinkIdentity",
            "ListIdentities",
            "DescribeIdentity",
            "TagResource",
            "UntagResource",
            "ListTagsForResource",
            "DeleteIdentities",
            "GetPrincipalTagAttributeMap",
            "SetPrincipalTagAttributeMap",
        ]
    }
}

// --- helpers ----------------------------------------------------------------

/// Return `InvalidParameterException` — the one error every cognito-identity
/// op in the Smithy model declares for input validation.
fn invalid_parameter(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterException",
        msg.into(),
    )
}

/// Enforce a Smithy `@length(min, max)` trait. Returns
/// `InvalidParameterException` (the declared error shape for every op
/// that uses it).
fn validate_length(field: &str, val: &str, min: usize, max: usize) -> Result<(), AwsServiceError> {
    let len = val.chars().count();
    if len < min || len > max {
        return Err(invalid_parameter(format!(
            "{field} length must be between {min} and {max} characters; got {len}"
        )));
    }
    Ok(())
}

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

fn parse_string_map(val: &Value) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(obj) = val.as_object() {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                out.insert(k.clone(), s.to_string());
            }
        }
    }
    out
}

fn parse_cognito_providers(val: &Value) -> Vec<CognitoIdentityProvider> {
    val.as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|item| {
                    let provider_name = item["ProviderName"].as_str()?.to_string();
                    let client_id = item["ClientId"].as_str()?.to_string();
                    let server_side_token_check =
                        item["ServerSideTokenCheck"].as_bool().unwrap_or(false);
                    Some(CognitoIdentityProvider {
                        provider_name,
                        client_id,
                        server_side_token_check,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
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

/// Generate an identity-pool id of the form `<region>:<uuid>`.
/// Real Cognito uses a 36-char UUID — match exactly so SDKs that
/// validate the shape still pass.
fn generate_identity_pool_id(region: &str) -> String {
    format!("{region}:{}", Uuid::new_v4())
}

/// Generate a federated identity id of the form `<region>:<uuid>`.
fn generate_identity_id(region: &str) -> String {
    format!("{region}:{}", Uuid::new_v4())
}

fn ensure_identity_pool_exists(state: &CognitoState, pool_id: &str) -> Result<(), AwsServiceError> {
    if state.identity_pools.contains_key(pool_id) {
        Ok(())
    } else {
        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ResourceNotFoundException",
            format!("IdentityPool '{pool_id}' not found."),
        ))
    }
}

fn identity_pool_to_json(pool: &IdentityPool) -> Value {
    let cognito_providers: Vec<Value> = pool
        .cognito_identity_providers
        .iter()
        .map(|p| {
            json!({
                "ProviderName": p.provider_name,
                "ClientId": p.client_id,
                "ServerSideTokenCheck": p.server_side_token_check,
            })
        })
        .collect();

    let mut obj = json!({
        "IdentityPoolId": pool.identity_pool_id,
        "IdentityPoolName": pool.identity_pool_name,
        "AllowUnauthenticatedIdentities": pool.allow_unauthenticated_identities,
        "AllowClassicFlow": pool.allow_classic_flow,
        "CognitoIdentityProviders": cognito_providers,
        "OpenIdConnectProviderARNs": pool.open_id_connect_provider_arns,
        "SamlProviderARNs": pool.saml_provider_arns,
        "SupportedLoginProviders": pool.supported_login_providers,
        "IdentityPoolTags": pool.identity_pool_tags,
    });
    if let Some(ref name) = pool.developer_provider_name {
        obj["DeveloperProviderName"] = json!(name);
    }
    if let Some(ref streams) = pool.cognito_streams {
        obj["CognitoStreams"] = streams.clone();
    }
    if let Some(ref push) = pool.push_sync {
        obj["PushSync"] = push.clone();
    }
    obj
}

fn identity_to_json(identity: &FederatedIdentity) -> Value {
    let logins: Vec<String> = identity.logins.keys().cloned().collect();
    json!({
        "IdentityId": identity.identity_id,
        "Logins": logins,
        "CreationDate": identity.creation_date.timestamp() as f64,
        "LastModifiedDate": identity.last_modified_date.timestamp() as f64,
    })
}

impl CognitoIdentityService {
    // --- pool CRUD ---------------------------------------------------------

    fn create_identity_pool(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let identity_pool_name = require_str(&body, "IdentityPoolName")?.to_string();
        // Smithy: `IdentityPoolName` is `@length(min: 1, max: 128)`.
        validate_length("IdentityPoolName", &identity_pool_name, 1, 128)?;
        // Smithy: `AllowUnauthenticatedIdentities` is `@required`. Real
        // Cognito Identity rejects requests without it as
        // `InvalidParameterException`. We previously defaulted silently
        // to `false`, masking real callers' bugs.
        let allow_unauth = body["AllowUnauthenticatedIdentities"]
            .as_bool()
            .ok_or_else(|| invalid_parameter("AllowUnauthenticatedIdentities is required"))?;
        let allow_classic = body["AllowClassicFlow"].as_bool().unwrap_or(false);
        let developer_provider_name = body["DeveloperProviderName"]
            .as_str()
            .map(|s| s.to_string());
        if let Some(ref dpn) = developer_provider_name {
            // Smithy: `DeveloperProviderName` is `@length(min: 1, max: 128)`.
            validate_length("DeveloperProviderName", dpn, 1, 128)?;
        }

        let cognito_providers = parse_cognito_providers(&body["CognitoIdentityProviders"]);
        let oidc_provider_arns = parse_string_array(&body["OpenIdConnectProviderARNs"]);
        let saml_provider_arns = parse_string_array(&body["SamlProviderARNs"]);
        let supported_login_providers = parse_string_map(&body["SupportedLoginProviders"]);
        let identity_pool_tags = parse_string_map(&body["IdentityPoolTags"]);

        let pool_id = generate_identity_pool_id(&req.region);
        let pool = IdentityPool {
            identity_pool_id: pool_id.clone(),
            identity_pool_name,
            allow_unauthenticated_identities: allow_unauth,
            allow_classic_flow: allow_classic,
            developer_provider_name,
            cognito_identity_providers: cognito_providers,
            open_id_connect_provider_arns: oidc_provider_arns,
            saml_provider_arns,
            supported_login_providers,
            cognito_streams: None,
            push_sync: None,
            identity_pool_tags,
            creation_date: Utc::now(),
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.identity_pools.insert(pool_id.clone(), pool.clone());

        Ok(AwsResponse::ok_json(identity_pool_to_json(&pool)))
    }

    fn describe_identity_pool(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "IdentityPoolId")?;

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let pool = state.identity_pools.get(pool_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("IdentityPool '{pool_id}' not found."),
            )
        })?;

        Ok(AwsResponse::ok_json(identity_pool_to_json(pool)))
    }

    fn update_identity_pool(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "IdentityPoolId")?.to_string();
        // Real Cognito Identity returns the merged pool from UpdateIdentityPool;
        // mirror the request shape: every top-level field (except the pool id
        // itself) is replace-on-write when present.
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        ensure_identity_pool_exists(state, &pool_id)?;

        let pool = state.identity_pools.get_mut(&pool_id).unwrap();
        if let Some(name) = body["IdentityPoolName"].as_str() {
            pool.identity_pool_name = name.to_string();
        }
        if let Some(b) = body["AllowUnauthenticatedIdentities"].as_bool() {
            pool.allow_unauthenticated_identities = b;
        }
        if let Some(b) = body["AllowClassicFlow"].as_bool() {
            pool.allow_classic_flow = b;
        }
        if body["DeveloperProviderName"].is_string() {
            pool.developer_provider_name = body["DeveloperProviderName"]
                .as_str()
                .map(|s| s.to_string());
        }
        if body["CognitoIdentityProviders"].is_array() {
            pool.cognito_identity_providers =
                parse_cognito_providers(&body["CognitoIdentityProviders"]);
        }
        if body["OpenIdConnectProviderARNs"].is_array() {
            pool.open_id_connect_provider_arns =
                parse_string_array(&body["OpenIdConnectProviderARNs"]);
        }
        if body["SamlProviderARNs"].is_array() {
            pool.saml_provider_arns = parse_string_array(&body["SamlProviderARNs"]);
        }
        if body["SupportedLoginProviders"].is_object() {
            pool.supported_login_providers = parse_string_map(&body["SupportedLoginProviders"]);
        }
        if body["IdentityPoolTags"].is_object() {
            pool.identity_pool_tags = parse_string_map(&body["IdentityPoolTags"]);
        }

        Ok(AwsResponse::ok_json(identity_pool_to_json(&pool.clone())))
    }

    fn delete_identity_pool(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "IdentityPoolId")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if state.identity_pools.remove(pool_id).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("IdentityPool '{pool_id}' not found."),
            ));
        }
        // Cascade deletes: scrub role attachments and any identities that
        // belonged to this pool. Real Cognito Identity GCs both — leaving
        // them stranded would let stale identity ids accumulate forever.
        state.identity_pool_role_attachments.remove(pool_id);
        state
            .federated_identities
            .retain(|_, ident| ident.identity_pool_id != pool_id);

        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_identity_pools(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Smithy: `MaxResults` is `@required`, `@range(min: 1, max: 60)`.
        let max_results_raw = body["MaxResults"]
            .as_i64()
            .ok_or_else(|| invalid_parameter("MaxResults is required"))?;
        if !(1..=60).contains(&max_results_raw) {
            return Err(invalid_parameter(format!(
                "MaxResults must be between 1 and 60; got {max_results_raw}"
            )));
        }
        let max_results = max_results_raw as usize;
        let next_token = body["NextToken"].as_str();
        if let Some(token) = next_token {
            // Smithy: `PaginationKey` is `@length(min: 1, max: 65535)`.
            validate_length("NextToken", token, 1, 65535)?;
        }

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let mut pools: Vec<&IdentityPool> = state.identity_pools.values().collect();
        pools.sort_by_key(|p| p.creation_date);

        let start_idx = if let Some(token) = next_token {
            pools
                .iter()
                .position(|p| p.identity_pool_id == token)
                .unwrap_or(0)
        } else {
            0
        };

        let page: Vec<Value> = pools
            .iter()
            .skip(start_idx)
            .take(max_results)
            .map(|p| {
                json!({
                    "IdentityPoolId": p.identity_pool_id,
                    "IdentityPoolName": p.identity_pool_name,
                })
            })
            .collect();

        let mut response = json!({ "IdentityPools": page });
        if start_idx + max_results < pools.len() {
            if let Some(last) = pools.get(start_idx + max_results) {
                response["NextToken"] = json!(last.identity_pool_id);
            }
        }

        Ok(AwsResponse::ok_json(response))
    }

    // --- role attachments --------------------------------------------------

    fn get_identity_pool_roles(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "IdentityPoolId")?;

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        ensure_identity_pool_exists(state, pool_id)?;

        let attachment = state
            .identity_pool_role_attachments
            .get(pool_id)
            .cloned()
            .unwrap_or(IdentityPoolRoleAttachment {
                identity_pool_id: pool_id.to_string(),
                attachment_id: String::new(),
                roles: BTreeMap::new(),
                role_mappings: BTreeMap::new(),
            });

        Ok(AwsResponse::ok_json(json!({
            "IdentityPoolId": pool_id,
            "Roles": attachment.roles,
            "RoleMappings": attachment.role_mappings,
        })))
    }

    fn set_identity_pool_roles(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "IdentityPoolId")?.to_string();
        let roles = parse_string_map(&body["Roles"]);
        let role_mappings: BTreeMap<String, Value> = body["RoleMappings"]
            .as_object()
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        ensure_identity_pool_exists(state, &pool_id)?;

        let attachment = state
            .identity_pool_role_attachments
            .entry(pool_id.clone())
            .or_insert(IdentityPoolRoleAttachment {
                identity_pool_id: pool_id.clone(),
                attachment_id: Uuid::new_v4().to_string(),
                roles: BTreeMap::new(),
                role_mappings: BTreeMap::new(),
            });
        attachment.roles = roles;
        attachment.role_mappings = role_mappings;

        Ok(AwsResponse::ok_json(json!({})))
    }

    // --- identity lifecycle ------------------------------------------------

    fn get_id(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "IdentityPoolId")?.to_string();
        let logins = parse_string_map(&body["Logins"]);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        ensure_identity_pool_exists(state, &pool_id)?;

        let pool = state.identity_pools.get(&pool_id).unwrap();
        if logins.is_empty() && !pool.allow_unauthenticated_identities {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                format!(
                    "Unauthenticated access is not supported for this identity pool: {pool_id}"
                ),
            ));
        }

        // If the caller already presented a login that maps to an
        // existing identity, return the same id (real Cognito Identity
        // is sticky-by-login). Otherwise mint a new one.
        if !logins.is_empty() {
            for ident in state.federated_identities.values() {
                if ident.identity_pool_id != pool_id {
                    continue;
                }
                if logins.iter().any(|(k, v)| {
                    ident
                        .logins
                        .get(k)
                        .map(|stored| stored == v)
                        .unwrap_or(false)
                }) {
                    return Ok(AwsResponse::ok_json(json!({
                        "IdentityId": ident.identity_id,
                    })));
                }
            }
        }

        let identity_id = generate_identity_id(&req.region);
        let now = Utc::now();
        let identity = FederatedIdentity {
            identity_id: identity_id.clone(),
            identity_pool_id: pool_id,
            logins,
            developer_logins: BTreeMap::new(),
            creation_date: now,
            last_modified_date: now,
        };
        state
            .federated_identities
            .insert(identity_id.clone(), identity);

        Ok(AwsResponse::ok_json(json!({
            "IdentityId": identity_id,
        })))
    }

    fn describe_identity(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let identity_id = require_str(&body, "IdentityId")?;

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let identity = state.federated_identities.get(identity_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Identity '{identity_id}' not found."),
            )
        })?;

        Ok(AwsResponse::ok_json(identity_to_json(identity)))
    }

    fn list_identities(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "IdentityPoolId")?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(60).clamp(1, 60) as usize;
        let next_token = body["NextToken"].as_str();
        let hide_disabled = body["HideDisabled"].as_bool().unwrap_or(false);
        let _ = hide_disabled; // identities aren't disable-able in this emulator

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        ensure_identity_pool_exists(state, pool_id)?;

        let mut identities: Vec<&FederatedIdentity> = state
            .federated_identities
            .values()
            .filter(|i| i.identity_pool_id == pool_id)
            .collect();
        identities.sort_by_key(|i| i.creation_date);

        let start_idx = if let Some(token) = next_token {
            identities
                .iter()
                .position(|i| i.identity_id == token)
                .unwrap_or(0)
        } else {
            0
        };

        let page: Vec<Value> = identities
            .iter()
            .skip(start_idx)
            .take(max_results)
            .map(|i| identity_to_json(i))
            .collect();

        let mut response = json!({
            "IdentityPoolId": pool_id,
            "Identities": page,
        });
        if start_idx + max_results < identities.len() {
            if let Some(last) = identities.get(start_idx + max_results) {
                response["NextToken"] = json!(last.identity_id);
            }
        }

        Ok(AwsResponse::ok_json(response))
    }

    // --- OpenID Connect tokens --------------------------------------------

    fn get_open_id_token(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let identity_id = require_str(&body, "IdentityId")?.to_string();
        let logins = parse_string_map(&body["Logins"]);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let identity = state
            .federated_identities
            .get_mut(&identity_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Identity '{identity_id}' not found."),
                )
            })?;

        // Merge any new logins from the request into the stored identity
        // so subsequent `GetCredentialsForIdentity` calls find them.
        for (k, v) in &logins {
            identity.logins.insert(k.clone(), v.clone());
        }
        identity.last_modified_date = Utc::now();

        // Mint an opaque OIDC token. Real Cognito returns a signed JWT;
        // for the emulator a base64-of-uuid is sufficient since nothing
        // downstream verifies the token cryptographically.
        let token = format!("oidct.{}.{}", identity_id, Uuid::new_v4());

        Ok(AwsResponse::ok_json(json!({
            "IdentityId": identity_id,
            "Token": token,
        })))
    }

    fn get_open_id_token_for_developer_identity(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "IdentityPoolId")?.to_string();
        let logins = parse_string_map(&body["Logins"]);
        if logins.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "Logins is required for GetOpenIdTokenForDeveloperIdentity",
            ));
        }
        let identity_id_in = body["IdentityId"].as_str().map(|s| s.to_string());

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        ensure_identity_pool_exists(state, &pool_id)?;

        // Reuse an existing identity matching any of the developer
        // logins, otherwise bind to the supplied IdentityId or mint one.
        let identity_id = match identity_id_in {
            Some(id) => {
                if !state.federated_identities.contains_key(&id) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ResourceNotFoundException",
                        format!("Identity '{id}' not found."),
                    ));
                }
                id
            }
            None => {
                let mut found = None;
                for ident in state.federated_identities.values() {
                    if ident.identity_pool_id != pool_id {
                        continue;
                    }
                    if logins.iter().any(|(k, v)| {
                        ident
                            .developer_logins
                            .get(k)
                            .map(|stored| stored == v)
                            .unwrap_or(false)
                    }) {
                        found = Some(ident.identity_id.clone());
                        break;
                    }
                }
                match found {
                    Some(id) => id,
                    None => {
                        let id = generate_identity_id(&req.region);
                        let now = Utc::now();
                        state.federated_identities.insert(
                            id.clone(),
                            FederatedIdentity {
                                identity_id: id.clone(),
                                identity_pool_id: pool_id.clone(),
                                logins: BTreeMap::new(),
                                developer_logins: BTreeMap::new(),
                                creation_date: now,
                                last_modified_date: now,
                            },
                        );
                        id
                    }
                }
            }
        };

        let identity = state.federated_identities.get_mut(&identity_id).unwrap();
        for (k, v) in &logins {
            identity.developer_logins.insert(k.clone(), v.clone());
        }
        identity.last_modified_date = Utc::now();

        let token = format!("oidct-dev.{identity_id}.{}", Uuid::new_v4());
        Ok(AwsResponse::ok_json(json!({
            "IdentityId": identity_id,
            "Token": token,
        })))
    }

    // --- credential issuance (the load-bearing one) ------------------------

    fn get_credentials_for_identity(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let identity_id = require_str(&body, "IdentityId")?.to_string();
        let logins = parse_string_map(&body["Logins"]);
        let custom_role_arn = body["CustomRoleArn"].as_str().map(|s| s.to_string());

        // Pull the identity + role attachment out of cognito state under
        // a short-lived read lock so the IAM write below can run without
        // nested locks.
        let (pool_id, role_arn) = {
            let accounts = self.state.read();
            let empty = CognitoState::new(&req.account_id, &req.region);
            let state = accounts.get(&req.account_id).unwrap_or(&empty);

            let identity = state
                .federated_identities
                .get(&identity_id)
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "ResourceNotFoundException",
                        format!("Identity '{identity_id}' not found."),
                    )
                })?;
            let pool_id = identity.identity_pool_id.clone();
            let attachment = state.identity_pool_role_attachments.get(&pool_id);

            let resolved = custom_role_arn.clone().or_else(|| {
                attachment.and_then(|a| {
                    let key = if !logins.is_empty() || !identity.logins.is_empty() {
                        "authenticated"
                    } else {
                        "unauthenticated"
                    };
                    a.roles.get(key).cloned()
                })
            });

            (pool_id, resolved)
        };

        let role_arn = role_arn.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidIdentityPoolConfigurationException",
                format!(
                    "Identity pool '{pool_id}' has no role attached for the requested authorization context",
                ),
            )
        })?;

        // Mint STS-style temporary credentials and write them into IAM
        // state so subsequent SigV4 lookups (and IAM enforcement) treat
        // them just like AssumeRoleWithWebIdentity output.
        let creds = StsCredentials::generate();
        let role_name = role_arn.rsplit('/').next().unwrap_or("unknown");
        let target_account = role_arn
            .split(':')
            .nth(4)
            .map(|s| s.to_string())
            .unwrap_or_else(|| req.account_id.clone());
        let role_id = {
            let accounts = self.iam_state.read();
            accounts
                .get(&target_account)
                .and_then(|s| s.roles.get(role_name).map(|r| r.role_id.clone()))
                .unwrap_or_else(generate_role_id)
        };
        let session_name = format!("CognitoIdentityCredentials-{}", &identity_id);
        let assumed_role_arn =
            format!("arn:aws:sts::{target_account}:assumed-role/{role_name}/{session_name}");
        let assumed_role_id = format!("{role_id}:{session_name}");
        let expiration = Utc::now() + chrono::Duration::hours(1);

        {
            let mut accounts = self.iam_state.write();
            let target_state = accounts.get_or_create(&target_account);
            target_state.credential_identities.insert(
                creds.access_key_id.clone(),
                CredentialIdentity {
                    arn: assumed_role_arn.clone(),
                    user_id: assumed_role_id.clone(),
                    account_id: target_account.clone(),
                },
            );
            target_state.sts_temp_credentials.insert(
                creds.access_key_id.clone(),
                StsTempCredential {
                    access_key_id: creds.access_key_id.clone(),
                    secret_access_key: creds.secret_access_key.clone(),
                    session_token: creds.session_token.clone(),
                    principal_arn: assumed_role_arn,
                    user_id: assumed_role_id,
                    account_id: target_account.clone(),
                    expiration,
                    session_policies: Vec::new(),
                    mfa_present: false,
                    issued_at: Utc::now(),
                    federated_provider: Some("cognito-identity.amazonaws.com".to_string()),
                },
            );
        }

        Ok(AwsResponse::ok_json(json!({
            "IdentityId": identity_id,
            "Credentials": {
                "AccessKeyId": creds.access_key_id,
                "SecretKey": creds.secret_access_key,
                "SessionToken": creds.session_token,
                "Expiration": expiration.timestamp() as f64,
            },
        })))
    }

    // --- developer identity bookkeeping -----------------------------------

    fn merge_developer_identities(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let source_user = require_str(&body, "SourceUserIdentifier")?.to_string();
        let dest_user = require_str(&body, "DestinationUserIdentifier")?.to_string();
        let provider_name = require_str(&body, "DeveloperProviderName")?.to_string();
        let pool_id = require_str(&body, "IdentityPoolId")?.to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        ensure_identity_pool_exists(state, &pool_id)?;

        // Find source + destination identities by their developer login
        // entries within the pool. Real Cognito Identity merges the
        // source identity into the destination, deleting the source.
        let mut source_id: Option<String> = None;
        let mut dest_id: Option<String> = None;
        for ident in state.federated_identities.values() {
            if ident.identity_pool_id != pool_id {
                continue;
            }
            if ident.developer_logins.get(&provider_name) == Some(&source_user) {
                source_id = Some(ident.identity_id.clone());
            }
            if ident.developer_logins.get(&provider_name) == Some(&dest_user) {
                dest_id = Some(ident.identity_id.clone());
            }
        }
        let source_id = source_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Source developer identity '{source_user}' not found."),
            )
        })?;
        let dest_id = dest_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Destination developer identity '{dest_user}' not found."),
            )
        })?;

        let source = state.federated_identities.remove(&source_id).unwrap();
        let dest = state.federated_identities.get_mut(&dest_id).unwrap();
        for (k, v) in source.developer_logins {
            dest.developer_logins.insert(k, v);
        }
        for (k, v) in source.logins {
            dest.logins.insert(k, v);
        }
        dest.last_modified_date = Utc::now();

        Ok(AwsResponse::ok_json(json!({
            "IdentityId": dest_id,
        })))
    }

    fn unlink_developer_identity(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let identity_id = require_str(&body, "IdentityId")?;
        let _pool_id = require_str(&body, "IdentityPoolId")?;
        let provider_name = require_str(&body, "DeveloperProviderName")?;
        let user_id = require_str(&body, "DeveloperUserIdentifier")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let identity = state
            .federated_identities
            .get_mut(identity_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Identity '{identity_id}' not found."),
                )
            })?;
        if identity
            .developer_logins
            .get(provider_name)
            .map(|s| s.as_str())
            != Some(user_id)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceConflictException",
                format!(
                    "Developer identifier '{user_id}' is not linked to identity '{identity_id}'."
                ),
            ));
        }
        identity.developer_logins.remove(provider_name);
        identity.last_modified_date = Utc::now();
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn lookup_developer_identity(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "IdentityPoolId")?;
        let identity_id_in = body["IdentityId"].as_str();
        let dev_user_in = body["DeveloperUserIdentifier"].as_str();
        let max_results = body["MaxResults"].as_i64().unwrap_or(60).clamp(1, 60) as usize;

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        ensure_identity_pool_exists(state, pool_id)?;

        let mut matched_identity_id: Option<String> = None;
        let mut user_ids: Vec<String> = Vec::new();
        for ident in state.federated_identities.values() {
            if ident.identity_pool_id != pool_id {
                continue;
            }
            let id_match = identity_id_in
                .map(|i| ident.identity_id == i)
                .unwrap_or(false);
            let user_match = dev_user_in
                .map(|u| ident.developer_logins.values().any(|v| v == u))
                .unwrap_or(false);
            if id_match || user_match {
                matched_identity_id = Some(ident.identity_id.clone());
                for v in ident.developer_logins.values() {
                    user_ids.push(v.clone());
                    if user_ids.len() >= max_results {
                        break;
                    }
                }
                break;
            }
        }

        let identity_id = matched_identity_id.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "No matching developer identity.".to_string(),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({
            "IdentityId": identity_id,
            "DeveloperUserIdentifierList": user_ids,
        })))
    }

    // --- identity link/unlink ---------------------------------------------

    fn unlink_identity(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let identity_id = require_str(&body, "IdentityId")?;
        let logins = parse_string_map(&body["Logins"]);
        let logins_to_remove = parse_string_array(&body["LoginsToRemove"]);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let identity = state
            .federated_identities
            .get_mut(identity_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Identity '{identity_id}' not found."),
                )
            })?;
        for k in logins.keys() {
            identity.logins.remove(k);
        }
        for k in &logins_to_remove {
            identity.logins.remove(k);
        }
        identity.last_modified_date = Utc::now();
        Ok(AwsResponse::ok_json(json!({})))
    }

    // --- tagging ----------------------------------------------------------

    fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_arn = require_str(&body, "ResourceArn")?;
        // Smithy: `ARNString` is `@length(min: 20, max: 2048)`.
        validate_length("ResourceArn", resource_arn, 20, 2048)?;
        // Smithy: `Tags` is `@required` on `TagResourceInput`. Real
        // Cognito Identity rejects callers that omit the field.
        if !body["Tags"].is_object() {
            return Err(invalid_parameter("Tags is required"));
        }
        let tags = parse_string_map(&body["Tags"]);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let entry = state.tags.entry(resource_arn.to_string()).or_default();
        for (k, v) in tags {
            entry.insert(k, v);
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_arn = require_str(&body, "ResourceArn")?;
        // Smithy: `ARNString` is `@length(min: 20, max: 2048)`.
        validate_length("ResourceArn", resource_arn, 20, 2048)?;
        // Smithy: `TagKeys` is `@required` on `UntagResourceInput`.
        if !body["TagKeys"].is_array() {
            return Err(invalid_parameter("TagKeys is required"));
        }
        let keys = parse_string_array(&body["TagKeys"]);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(tags) = state.tags.get_mut(resource_arn) {
            for k in &keys {
                tags.remove(k);
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    fn list_tags_for_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_arn = require_str(&body, "ResourceArn")?;
        // Smithy: `ARNString` is `@length(min: 20, max: 2048)`.
        validate_length("ResourceArn", resource_arn, 20, 2048)?;
        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let tags = state.tags.get(resource_arn).cloned().unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({
            "Tags": tags,
        })))
    }

    // --- identity bulk delete --------------------------------------------

    /// `DeleteIdentities`: bulk-delete federated identities by id.
    /// Returns an `UnprocessedIdentityIds` list for entries that didn't
    /// exist — matching real Cognito Identity's contract.
    fn delete_identities(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let ids_value = &body["IdentityIdsToDelete"];
        let ids = ids_value
            .as_array()
            .ok_or_else(|| invalid_parameter("IdentityIdsToDelete is required"))?;
        // Smithy: `IdentityIdList` is `@length(min: 1, max: 60)`.
        if ids.is_empty() || ids.len() > 60 {
            return Err(invalid_parameter(format!(
                "IdentityIdsToDelete must contain between 1 and 60 entries; got {}",
                ids.len()
            )));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let mut unprocessed: Vec<Value> = Vec::new();
        for id_val in ids {
            let Some(id) = id_val.as_str() else { continue };
            if state.federated_identities.remove(id).is_none() {
                unprocessed.push(json!({
                    "IdentityId": id,
                    "ErrorCode": "ResourceNotFoundException",
                }));
            }
        }

        Ok(AwsResponse::ok_json(json!({
            "UnprocessedIdentityIds": unprocessed,
        })))
    }

    // --- principal-tag attribute maps ------------------------------------

    fn get_principal_tag_attribute_map(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "IdentityPoolId")?.to_string();
        validate_length("IdentityPoolId", &pool_id, 1, 55)?;
        let provider_name = require_str(&body, "IdentityProviderName")?.to_string();
        validate_length("IdentityProviderName", &provider_name, 1, 128)?;

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let key = format!("{pool_id}\x1f{provider_name}");
        let map = state
            .principal_tag_attribute_maps
            .get(&key)
            .cloned()
            .unwrap_or_else(|| PrincipalTagAttributeMap {
                identity_pool_id: pool_id.clone(),
                identity_provider_name: provider_name.clone(),
                use_defaults: true,
                principal_tags: BTreeMap::new(),
            });

        Ok(AwsResponse::ok_json(json!({
            "IdentityPoolId": map.identity_pool_id,
            "IdentityProviderName": map.identity_provider_name,
            "UseDefaults": map.use_defaults,
            "PrincipalTags": map.principal_tags,
        })))
    }

    fn set_principal_tag_attribute_map(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "IdentityPoolId")?.to_string();
        validate_length("IdentityPoolId", &pool_id, 1, 55)?;
        let provider_name = require_str(&body, "IdentityProviderName")?.to_string();
        validate_length("IdentityProviderName", &provider_name, 1, 128)?;
        let use_defaults = body["UseDefaults"].as_bool().unwrap_or(true);
        let principal_tags = parse_string_map(&body["PrincipalTags"]);

        let map = PrincipalTagAttributeMap {
            identity_pool_id: pool_id.clone(),
            identity_provider_name: provider_name.clone(),
            use_defaults,
            principal_tags: principal_tags.clone(),
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let key = format!("{pool_id}\x1f{provider_name}");
        state.principal_tag_attribute_maps.insert(key, map);

        Ok(AwsResponse::ok_json(json!({
            "IdentityPoolId": pool_id,
            "IdentityProviderName": provider_name,
            "UseDefaults": use_defaults,
            "PrincipalTags": principal_tags,
        })))
    }
}
