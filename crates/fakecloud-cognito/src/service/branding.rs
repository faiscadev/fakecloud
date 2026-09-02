use chrono::Utc;
use http::StatusCode;
use serde_json::json;
use uuid::Uuid;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::WebAuthnCredential;

use crate::state::CognitoState;

use super::{ensure_user_pool_exists, require_str, CognitoService};

impl CognitoService {
    // ── Managed Login Branding ────────────────────────────────────────

    pub(super) fn create_managed_login_branding(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let client_id = require_str(&body, "ClientId")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        ensure_user_pool_exists(state, pool_id)?;

        if !state.user_pool_clients.contains_key(client_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Client {client_id} does not exist."),
            ));
        }

        let branding_id = Uuid::new_v4().to_string();
        let now = Utc::now();

        let use_cognito = body["UseCognitoProvidedValues"].as_bool().unwrap_or(false);

        let mut branding = json!({
            "ManagedLoginBrandingId": branding_id,
            "UserPoolId": pool_id,
            "ClientId": client_id,
            "UseCognitoProvidedValues": use_cognito,
            "CreationDate": now.timestamp() as f64,
            "LastModifiedDate": now.timestamp() as f64,
        });

        if !body["Settings"].is_null() {
            branding["Settings"] = body["Settings"].clone();
        }
        if !body["Assets"].is_null() {
            branding["Assets"] = body["Assets"].clone();
        }

        state
            .managed_login_brandings
            .insert(branding_id.clone(), branding.clone());

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
        let pool_id = require_str(&body, "UserPoolId")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        ensure_user_pool_exists(state, pool_id)?;

        match state.managed_login_brandings.get(branding_id) {
            Some(b) if b["UserPoolId"].as_str() == Some(pool_id) => {
                state.managed_login_brandings.remove(branding_id);
            }
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Managed login branding {branding_id} does not exist."),
                ));
            }
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn describe_managed_login_branding(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let branding_id = require_str(&body, "ManagedLoginBrandingId")?;
        let pool_id = require_str(&body, "UserPoolId")?;
        let return_merged = body["ReturnMergedResources"].as_bool().unwrap_or(false);

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        ensure_user_pool_exists(state, pool_id)?;

        let branding = state
            .managed_login_brandings
            .get(branding_id)
            .filter(|b| b["UserPoolId"].as_str() == Some(pool_id))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Managed login branding {branding_id} does not exist."),
                )
            })?;

        let mut result = branding.clone();
        if return_merged {
            // For merged resources, just return the same branding (no real merging in fakecloud)
            result["ReturnMergedResources"] = json!(true);
        }

        Ok(AwsResponse::ok_json(json!({
            "ManagedLoginBranding": result
        })))
    }

    pub(super) fn describe_managed_login_branding_by_client(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let client_id = require_str(&body, "ClientId")?;
        let return_merged = body["ReturnMergedResources"].as_bool().unwrap_or(false);

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        ensure_user_pool_exists(state, pool_id)?;

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
                    format!(
                        "No managed login branding found for client {client_id} in pool {pool_id}."
                    ),
                )
            })?;

        let mut result = branding.clone();
        if return_merged {
            result["ReturnMergedResources"] = json!(true);
        }

        Ok(AwsResponse::ok_json(json!({
            "ManagedLoginBranding": result
        })))
    }

    pub(super) fn update_managed_login_branding(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let branding_id = require_str(&body, "ManagedLoginBrandingId")?;
        let pool_id = require_str(&body, "UserPoolId")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        ensure_user_pool_exists(state, pool_id)?;

        let branding = state
            .managed_login_brandings
            .get_mut(branding_id)
            .filter(|b| b["UserPoolId"].as_str() == Some(pool_id))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Managed login branding {branding_id} does not exist."),
                )
            })?;

        let now = Utc::now();
        branding["LastModifiedDate"] = json!(now.timestamp() as f64);

        if let Some(use_cognito) = body["UseCognitoProvidedValues"].as_bool() {
            branding["UseCognitoProvidedValues"] = json!(use_cognito);
        }
        if !body["Settings"].is_null() {
            branding["Settings"] = body["Settings"].clone();
        }
        if !body["Assets"].is_null() {
            branding["Assets"] = body["Assets"].clone();
        }

        let result = branding.clone();

        Ok(AwsResponse::ok_json(json!({
            "ManagedLoginBranding": result
        })))
    }

    // ── Terms ─────────────────────────────────────────────────────────

    pub(super) fn create_terms(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let terms_name = require_str(&body, "TermsName")?;
        let client_id = body["ClientId"].as_str().unwrap_or("");

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        ensure_user_pool_exists(state, pool_id)?;

        let terms_id = Uuid::new_v4().to_string();
        let now = Utc::now();

        let mut terms = json!({
            "TermsId": terms_id,
            "UserPoolId": pool_id,
            "ClientId": client_id,
            "TermsName": terms_name,
            "CreationDate": now.timestamp() as f64,
            "LastModifiedDate": now.timestamp() as f64,
        });

        if !body["TermsSource"].is_null() {
            terms["TermsSource"] = body["TermsSource"].clone();
        }
        if !body["Enforcement"].is_null() {
            terms["Enforcement"] = body["Enforcement"].clone();
        }
        if !body["Links"].is_null() {
            terms["Links"] = body["Links"].clone();
        }

        state.terms.insert(terms_id, terms.clone());

        Ok(AwsResponse::ok_json(json!({
            "Terms": terms
        })))
    }

    pub(super) fn delete_terms(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let terms_id = require_str(&body, "TermsId")?;
        let pool_id = require_str(&body, "UserPoolId")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        ensure_user_pool_exists(state, pool_id)?;

        match state.terms.get(terms_id) {
            Some(t) if t["UserPoolId"].as_str() == Some(pool_id) => {
                state.terms.remove(terms_id);
            }
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Terms {terms_id} does not exist."),
                ));
            }
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn describe_terms(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let terms_id = require_str(&body, "TermsId")?;
        let pool_id = require_str(&body, "UserPoolId")?;

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        ensure_user_pool_exists(state, pool_id)?;

        let terms = state
            .terms
            .get(terms_id)
            .filter(|t| t["UserPoolId"].as_str() == Some(pool_id))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Terms {terms_id} does not exist."),
                )
            })?;

        Ok(AwsResponse::ok_json(json!({
            "Terms": terms
        })))
    }

    pub(super) fn list_terms(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let pool_id = require_str(&body, "UserPoolId")?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(10).clamp(1, 60) as usize;
        let next_token = body["NextToken"].as_str();

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        ensure_user_pool_exists(state, pool_id)?;

        let mut terms_list: Vec<&serde_json::Value> = state
            .terms
            .values()
            .filter(|t| t["UserPoolId"].as_str() == Some(pool_id))
            .collect();

        terms_list.sort_by(|a, b| {
            let a_date = a["CreationDate"].as_f64().unwrap_or(0.0);
            let b_date = b["CreationDate"].as_f64().unwrap_or(0.0);
            a_date
                .partial_cmp(&b_date)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        // A stale token (its item was deleted) ends the listing rather than
        // silently restarting at page 1 (bug-audit 2026-06-20, 1.14).
        let start = match next_token {
            None => 0,
            Some(t) => terms_list
                .iter()
                .position(|term| term["TermsId"].as_str() == Some(t))
                .unwrap_or(terms_list.len()),
        };

        let page: Vec<serde_json::Value> = terms_list
            .iter()
            .skip(start)
            .take(max_results)
            .cloned()
            .cloned()
            .collect();

        let has_more = start + max_results < terms_list.len();
        let mut result = json!({ "Terms": page });
        if has_more {
            if let Some(last) = terms_list.get(start + max_results) {
                if let Some(id) = last["TermsId"].as_str() {
                    result["NextToken"] = json!(id);
                }
            }
        }

        Ok(AwsResponse::ok_json(result))
    }

    pub(super) fn update_terms(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let terms_id = require_str(&body, "TermsId")?;
        let pool_id = require_str(&body, "UserPoolId")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        ensure_user_pool_exists(state, pool_id)?;

        let terms = state
            .terms
            .get_mut(terms_id)
            .filter(|t| t["UserPoolId"].as_str() == Some(pool_id))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Terms {terms_id} does not exist."),
                )
            })?;

        let now = Utc::now();
        terms["LastModifiedDate"] = json!(now.timestamp() as f64);

        if let Some(name) = body["TermsName"].as_str() {
            terms["TermsName"] = json!(name);
        }
        if !body["TermsSource"].is_null() {
            terms["TermsSource"] = body["TermsSource"].clone();
        }
        if !body["Enforcement"].is_null() {
            terms["Enforcement"] = body["Enforcement"].clone();
        }
        if !body["Links"].is_null() {
            terms["Links"] = body["Links"].clone();
        }

        let result = terms.clone();

        Ok(AwsResponse::ok_json(json!({
            "Terms": result
        })))
    }

    // ── WebAuthn ──────────────────────────────────────────────────────

    pub(super) fn start_web_authn_registration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let access_token = require_str(&body, "AccessToken")?;

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let token_data = state.valid_access_token(access_token).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid access token.",
            )
        })?;

        let pool_id = &token_data.user_pool_id;
        let username = &token_data.username;

        // Validate user exists
        if !state
            .users
            .get(pool_id.as_str())
            .is_some_and(|u| u.contains_key(username.as_str()))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "User not found.",
            ));
        }

        let challenge = Uuid::new_v4().to_string();
        let user_id = Uuid::new_v4().to_string();

        // The relying-party id host region is read out of the pool id prefix so
        // it matches the pool's issuer/ARN region, not a hardcoded default.
        let rp_region = pool_id.split('_').next().unwrap_or("us-east-1");
        let credential_creation_options = json!({
            "rp": {
                "id": format!("cognito-idp.{rp_region}.amazonaws.com/{pool_id}"),
                "name": "Amazon Cognito"
            },
            "user": {
                "id": user_id,
                "name": username,
                "displayName": username
            },
            "challenge": challenge,
            "pubKeyCredParams": [
                { "type": "public-key", "alg": -7 },
                { "type": "public-key", "alg": -257 }
            ],
            "timeout": 60000,
            "attestation": "none",
            "authenticatorSelection": {
                "userVerification": "preferred",
                "residentKey": "preferred"
            }
        });

        Ok(AwsResponse::ok_json(json!({
            "CredentialCreationOptions": credential_creation_options
        })))
    }

    pub(super) fn complete_web_authn_registration(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let access_token = require_str(&body, "AccessToken")?;
        let credential = &body["Credential"];

        if credential.is_null() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterException",
                "Credential is required",
            ));
        }

        let mut attestation_info: Option<crate::state::WebAuthnAttestationInfo> = None;
        if let Some(att_b64) = credential["response"]["attestationObject"].as_str() {
            use base64::Engine;
            match crate::webauthn::parse_packed_attestation(att_b64) {
                Ok(parsed) => {
                    let mut self_attest_verified = false;
                    if let Some(cd_b64) = credential["response"]["clientDataJSON"].as_str() {
                        let cd_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
                            .decode(cd_b64.trim_end_matches('='))
                            .or_else(|_| base64::engine::general_purpose::STANDARD.decode(cd_b64));
                        if let Ok(cd_bytes) = cd_bytes {
                            match crate::webauthn::verify_packed_attestation(&parsed, &cd_bytes) {
                                Ok(()) => {
                                    self_attest_verified = parsed.x5c.is_empty();
                                }
                                Err(e) => {
                                    return Err(AwsServiceError::aws_error(
                                        StatusCode::BAD_REQUEST,
                                        "InvalidParameterException",
                                        format!("WebAuthn attestation invalid: {e}").as_str(),
                                    ));
                                }
                            }
                        }
                    }
                    attestation_info = Some(crate::state::WebAuthnAttestationInfo {
                        fmt: "packed".to_string(),
                        alg: parsed.alg,
                        signature_len: parsed.sig.len(),
                        x5c_chain_len: parsed.x5c.len(),
                        cose_public_key_present: parsed.cose_public_key.is_some(),
                        self_attest_verified,
                    });
                }
                Err(crate::webauthn::AttestationError::UnsupportedFormat(fmt)) => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "UnsupportedAttestationFormatException",
                        format!("Unsupported WebAuthn attestation format: {fmt}").as_str(),
                    ));
                }
                Err(e) => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterException",
                        format!("WebAuthn attestationObject invalid: {e}").as_str(),
                    ));
                }
            }
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let token_data = state.valid_access_token(access_token).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid access token.",
            )
        })?;

        let pool_id = token_data.user_pool_id.clone();
        let username = token_data.username.clone();

        // Validate user exists
        if !state
            .users
            .get(pool_id.as_str())
            .is_some_and(|u| u.contains_key(username.as_str()))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                "User not found.",
            ));
        }

        let credential_id = credential["id"]
            .as_str()
            .unwrap_or(&Uuid::new_v4().to_string())
            .to_string();

        let friendly_name = credential["clientExtensionResults"]["credProps"]
            ["authenticatorDisplayName"]
            .as_str()
            .map(|s| s.to_string());

        let authenticator_attachment = credential["authenticatorAttachment"]
            .as_str()
            .map(|s| s.to_string());

        let authenticator_transport: Vec<String> = credential["response"]["transports"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let now = Utc::now();
        let cred = WebAuthnCredential {
            credential_id,
            friendly_credential_name: friendly_name,
            relying_party_id: format!(
                "cognito-idp.{}.amazonaws.com/{pool_id}",
                pool_id.split('_').next().unwrap_or("us-east-1")
            ),
            authenticator_attachment,
            authenticator_transport,
            created_at: now,
            attestation_info,
        };

        let key = format!("{pool_id}:{username}");
        state
            .webauthn_credentials
            .entry(key)
            .or_default()
            .push(cred);

        Ok(AwsResponse::ok_json(json!({
            "CredentialId": state
                .webauthn_credentials
                .get(&format!("{pool_id}:{username}"))
                .and_then(|v| v.last())
                .map(|c| c.credential_id.as_str())
                .unwrap_or("")
        })))
    }

    pub(super) fn delete_web_authn_credential(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let access_token = require_str(&body, "AccessToken")?;
        let credential_id = require_str(&body, "CredentialId")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let token_data = state.valid_access_token(access_token).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid access token.",
            )
        })?;

        let pool_id = token_data.user_pool_id.clone();
        let username = token_data.username.clone();

        let key = format!("{pool_id}:{username}");
        let creds = state.webauthn_credentials.get_mut(&key).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Credential {credential_id} does not exist."),
            )
        })?;

        let before_len = creds.len();
        creds.retain(|c| c.credential_id != credential_id);

        if creds.len() == before_len {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourceNotFoundException",
                format!("Credential {credential_id} does not exist."),
            ));
        }

        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn list_web_authn_credentials(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let access_token = require_str(&body, "AccessToken")?;
        let max_results = body["MaxResults"].as_i64().unwrap_or(10).clamp(1, 60) as usize;
        let next_token = body["NextToken"].as_str();

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let token_data = state.valid_access_token(access_token).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotAuthorizedException",
                "Invalid access token.",
            )
        })?;

        let pool_id = &token_data.user_pool_id;
        let username = &token_data.username;

        let key = format!("{pool_id}:{username}");
        let all_creds = state.webauthn_credentials.get(&key);

        let creds: &[WebAuthnCredential] = all_creds.map(|v| v.as_slice()).unwrap_or(&[]);

        let start = match next_token {
            None => 0,
            Some(t) => creds
                .iter()
                .position(|c| c.credential_id == t)
                .unwrap_or(creds.len()),
        };

        let page: Vec<serde_json::Value> = creds
            .iter()
            .skip(start)
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
                if let Some(ref attachment) = c.authenticator_attachment {
                    obj["AuthenticatorAttachment"] = json!(attachment);
                }
                obj
            })
            .collect();

        let has_more = start + max_results < creds.len();
        let mut result = json!({ "Credentials": page });
        if has_more {
            if let Some(c) = creds.get(start + max_results) {
                result["NextToken"] = json!(c.credential_id);
            }
        }

        Ok(AwsResponse::ok_json(result))
    }
}

impl CognitoService {
    /// `DescribeTermsByClient` — resolve a terms document by the app client it
    /// is attached to plus its name, rather than by terms id.
    ///
    /// The stored record already carries `ClientId` and `TermsName`, so this is
    /// a real lookup over the same store `DescribeTerms` reads; a terms
    /// document attached to a different client, or a different pool, does not
    /// match.
    pub(super) fn describe_terms_by_client(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let client_id = require_str(&body, "ClientId")?;
        let pool_id = require_str(&body, "UserPoolId")?;
        let terms_name = require_str(&body, "TermsName")?;

        let accounts = self.state.read();
        let empty = CognitoState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        ensure_user_pool_exists(state, pool_id)?;

        let terms = state
            .terms
            .values()
            .find(|t| {
                t["UserPoolId"].as_str() == Some(pool_id)
                    && t["ClientId"].as_str() == Some(client_id)
                    && t["TermsName"].as_str() == Some(terms_name)
            })
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ResourceNotFoundException",
                    format!("Terms {terms_name} does not exist for client {client_id}."),
                )
            })?;

        Ok(AwsResponse::ok_json(json!({ "Terms": terms })))
    }
}
