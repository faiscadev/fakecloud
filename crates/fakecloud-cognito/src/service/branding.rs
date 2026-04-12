use chrono::Utc;
use http::StatusCode;
use serde_json::json;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::{require_str, CognitoService};

impl CognitoService {
    // ── Managed Login Branding ─────────────────────────────────────────

    pub(super) fn create_managed_login_branding(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let client_id = require_str(&body, "ClientId")?;

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let branding_id = Uuid::new_v4().to_string();
        let now = Utc::now();

        let branding = json!({
            "ManagedLoginBrandingId": branding_id,
            "UserPoolId": pool_id,
            "ClientId": client_id,
            "UseCognitoProvidedValues": body["UseCognitoProvidedValues"].as_bool().unwrap_or(false),
            "Settings": body["Settings"].clone(),
            "Assets": body["Assets"].clone(),
            "CreationDate": now.timestamp() as f64,
            "LastModifiedDate": now.timestamp() as f64,
        });

        state
            .managed_login_brandings
            .insert(branding_id, branding.clone());

        Ok(AwsResponse::ok_json(json!({
            "ManagedLoginBranding": branding
        })))
    }

    pub(super) fn delete_managed_login_branding(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let branding_id = require_str(&body, "ManagedLoginBrandingId")?;
        let _pool_id = require_str(&body, "UserPoolId")?;

        let mut state = self.state.write();

        if state.managed_login_brandings.remove(branding_id).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Managed login branding {branding_id} does not exist."),
            ));
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn describe_managed_login_branding(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let _pool_id = require_str(&body, "UserPoolId")?;
        let branding_id = require_str(&body, "ManagedLoginBrandingId")?;

        let state = self.state.read();

        let branding = state
            .managed_login_brandings
            .get(branding_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Managed login branding {branding_id} does not exist."),
                )
            })?;

        Ok(AwsResponse::ok_json(json!({
            "ManagedLoginBranding": branding
        })))
    }

    pub(super) fn describe_managed_login_branding_by_client(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let client_id = require_str(&body, "ClientId")?;

        let state = self.state.read();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let branding = state
            .managed_login_brandings
            .values()
            .find(|b| {
                b["UserPoolId"].as_str() == Some(pool_id)
                    && b["ClientId"].as_str() == Some(client_id)
            })
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Managed login branding for client {client_id} does not exist."),
                )
            })?;

        Ok(AwsResponse::ok_json(json!({
            "ManagedLoginBranding": branding
        })))
    }

    pub(super) fn update_managed_login_branding(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let branding_id = body["ManagedLoginBrandingId"].as_str().unwrap_or_default();
        let pool_id = body["UserPoolId"].as_str().unwrap_or_default();

        let mut state = self.state.write();

        let branding = state
            .managed_login_brandings
            .get_mut(branding_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Managed login branding {branding_id} does not exist."),
                )
            })?;

        if body["Settings"].is_object() {
            branding["Settings"] = body["Settings"].clone();
        }
        if body["Assets"].is_array() {
            branding["Assets"] = body["Assets"].clone();
        }
        if let Some(use_cognito) = body["UseCognitoProvidedValues"].as_bool() {
            branding["UseCognitoProvidedValues"] = json!(use_cognito);
        }
        if !pool_id.is_empty() {
            branding["UserPoolId"] = json!(pool_id);
        }
        branding["LastModifiedDate"] = json!(Utc::now().timestamp() as f64);

        let branding = branding.clone();

        Ok(AwsResponse::ok_json(json!({
            "ManagedLoginBranding": branding
        })))
    }

    // ── Terms of Service ───────────────────────────────────────────────

    pub(super) fn create_terms(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let client_id = require_str(&body, "ClientId")?;
        let terms_name = require_str(&body, "TermsName")?;
        let terms_source = &body["TermsSource"];
        let enforcement = &body["Enforcement"];

        let mut state = self.state.write();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let terms_id = Uuid::new_v4().to_string();
        let now = Utc::now();

        let terms = json!({
            "TermsId": terms_id,
            "UserPoolId": pool_id,
            "ClientId": client_id,
            "TermsName": terms_name,
            "TermsSource": terms_source,
            "Enforcement": enforcement,
            "Links": body["Links"].clone(),
            "CreationDate": now.timestamp() as f64,
            "LastModifiedDate": now.timestamp() as f64,
            "Status": "ACTIVE",
        });

        state.terms.insert(terms_id, terms.clone());

        Ok(AwsResponse::ok_json(json!({ "Terms": terms })))
    }

    pub(super) fn delete_terms(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let terms_id = require_str(&body, "TermsId")?;
        let _pool_id = require_str(&body, "UserPoolId")?;

        let mut state = self.state.write();

        if state.terms.remove(terms_id).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Terms {terms_id} does not exist."),
            ));
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn describe_terms(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let terms_id = require_str(&body, "TermsId")?;
        let _pool_id = require_str(&body, "UserPoolId")?;

        let state = self.state.read();

        let terms = state.terms.get(terms_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Terms {terms_id} does not exist."),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({ "Terms": terms })))
    }

    pub(super) fn list_terms(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(25) as usize;
        let next_token = body["NextToken"].as_str();

        let state = self.state.read();

        if !state.user_pools.contains_key(pool_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("User pool {pool_id} does not exist."),
            ));
        }

        let mut terms_list: Vec<&serde_json::Value> = state
            .terms
            .values()
            .filter(|t| t["UserPoolId"].as_str() == Some(pool_id))
            .collect();
        terms_list.sort_by(|a, b| {
            a["CreationDate"]
                .as_f64()
                .partial_cmp(&b["CreationDate"].as_f64())
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let start = next_token
            .and_then(|t| {
                terms_list
                    .iter()
                    .position(|tr| tr["TermsId"].as_str() == Some(t))
            })
            .unwrap_or(0);

        let page: Vec<serde_json::Value> = terms_list
            .iter()
            .skip(start)
            .take(max_results)
            .map(|t| {
                json!({
                    "TermsId": t["TermsId"],
                    "TermsName": t["TermsName"],
                    "UserPoolId": t["UserPoolId"],
                    "ClientId": t["ClientId"],
                    "Status": t["Status"],
                    "CreationDate": t["CreationDate"],
                    "LastModifiedDate": t["LastModifiedDate"],
                })
            })
            .collect();

        let has_more = start + max_results < terms_list.len();
        let mut result = json!({ "Terms": page });
        if has_more {
            if let Some(last) = terms_list.get(start + max_results) {
                result["NextToken"] = last["TermsId"].clone();
            }
        }

        Ok(AwsResponse::ok_json(result))
    }

    pub(super) fn update_terms(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let terms_id = require_str(&body, "TermsId")?;
        let _pool_id = require_str(&body, "UserPoolId")?;

        let mut state = self.state.write();

        let terms = state.terms.get_mut(terms_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Terms {terms_id} does not exist."),
            )
        })?;

        if let Some(name) = body["TermsName"].as_str() {
            terms["TermsName"] = json!(name);
        }
        if body["TermsSource"].is_object() {
            terms["TermsSource"] = body["TermsSource"].clone();
        }
        if body["Enforcement"].is_object() || body["Enforcement"].is_string() {
            terms["Enforcement"] = body["Enforcement"].clone();
        }
        if !body["Links"].is_null() {
            terms["Links"] = body["Links"].clone();
        }
        terms["LastModifiedDate"] = json!(Utc::now().timestamp() as f64);

        let terms = terms.clone();

        Ok(AwsResponse::ok_json(json!({ "Terms": terms })))
    }

    // ── WebAuthn ───────────────────────────────────────────────────────

    pub(super) fn start_webauthn_registration(
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

        // Generate a WebAuthn challenge
        let challenge = base64::Engine::encode(
            &base64::engine::general_purpose::URL_SAFE_NO_PAD,
            Uuid::new_v4().as_bytes(),
        );

        Ok(AwsResponse::ok_json(json!({
            "CredentialCreationOptions": {
                "challenge": challenge,
                "rp": {
                    "id": format!("cognito-idp.{}.amazonaws.com", state.region),
                    "name": "fakecloud"
                },
                "user": {
                    "id": user.sub,
                    "name": username,
                    "displayName": username
                },
                "pubKeyCredParams": [
                    {"type": "public-key", "alg": -7},
                    {"type": "public-key", "alg": -257}
                ],
                "timeout": 60000,
                "attestation": "none"
            }
        })))
    }

    pub(super) fn complete_webauthn_registration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let access_token = require_str(&body, "AccessToken")?;
        let _credential = &body["Credential"];

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

        let key = format!("{pool_id}:{username}");
        let credential_id = Uuid::new_v4().to_string();

        let cred = crate::state::WebAuthnCredential {
            credential_id,
            friendly_credential_name: None,
            relying_party_id: format!("cognito-idp.{}.amazonaws.com", state.region),
            authenticator_attachment: Some("platform".to_string()),
            authenticator_transport: vec!["internal".to_string()],
            created_at: Utc::now(),
        };

        state
            .webauthn_credentials
            .entry(key)
            .or_default()
            .push(cred);

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn delete_webauthn_credential(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let access_token = require_str(&body, "AccessToken")?;
        let credential_id = require_str(&body, "CredentialId")?;

        let mut state = self.state.write();

        let token_data = state.access_tokens.get(access_token).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid access token.",
            )
        })?;

        let key = format!("{}:{}", token_data.user_pool_id, token_data.username);

        let creds = state.webauthn_credentials.get_mut(&key).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "No WebAuthn credentials found.",
            )
        })?;

        let idx = creds
            .iter()
            .position(|c| c.credential_id == credential_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Credential {credential_id} not found."),
                )
            })?;

        creds.remove(idx);

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_webauthn_credentials(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let access_token = require_str(&body, "AccessToken")?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(25) as usize;
        let _next_token = body["NextToken"].as_str();

        let state = self.state.read();

        let token_data = state.access_tokens.get(access_token).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid access token.",
            )
        })?;

        let key = format!("{}:{}", token_data.user_pool_id, token_data.username);
        let empty = Vec::new();
        let creds = state.webauthn_credentials.get(&key).unwrap_or(&empty);

        let page: Vec<serde_json::Value> = creds
            .iter()
            .take(max_results)
            .map(|c| {
                let mut obj = json!({
                    "CredentialId": c.credential_id,
                    "RelyingPartyId": c.relying_party_id,
                    "AuthenticatorTransports": c.authenticator_transport,
                    "CreatedAt": c.created_at.timestamp() as f64,
                });
                if let Some(ref name) = c.friendly_credential_name {
                    obj["FriendlyCredentialName"] = json!(name);
                }
                if let Some(ref attach) = c.authenticator_attachment {
                    obj["AuthenticatorAttachment"] = json!(attach);
                }
                obj
            })
            .collect();

        Ok(AwsResponse::ok_json(json!({
            "Credentials": page
        })))
    }
}
