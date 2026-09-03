//! ACM's ACME surface: endpoints, external account bindings, pre-validated
//! domains, and the ACME accounts registered against an endpoint.
//!
//! The four families are genuinely related, and the handlers below keep those
//! relationships real rather than storing four independent bags: a binding and
//! a domain validation each belong to an endpoint (and are listed by it),
//! deleting an endpoint cascades to both, and an account resolves through the
//! binding it was created under.

use std::collections::BTreeMap;

use chrono::{Duration, Utc};
use http::StatusCode;
use serde_json::{json, Value};
use uuid::Uuid;

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::AcmService;
use crate::state::{AcmeBinding, AcmeDomainValidation, AcmeEndpoint};

pub(crate) const ACME_ACTIONS: &[&str] = &[
    "CreateAcmeEndpoint",
    "DescribeAcmeEndpoint",
    "ListAcmeEndpoints",
    "UpdateAcmeEndpoint",
    "DeleteAcmeEndpoint",
    "CreateAcmeExternalAccountBinding",
    "DescribeAcmeExternalAccountBinding",
    "ListAcmeExternalAccountBindings",
    "RevokeAcmeExternalAccountBinding",
    "DeleteAcmeExternalAccountBinding",
    "GetAcmeExternalAccountBindingCredentials",
    "CreateAcmeDomainValidation",
    "DescribeAcmeDomainValidation",
    "ListAcmeDomainValidations",
    "UpdateAcmeDomainValidation",
    "DeleteAcmeDomainValidation",
    "DescribeAcmeAccount",
    "ListAcmeAccounts",
    "RevokeAcmeAccount",
];

/// Operations that change persisted state and so must trigger a snapshot.
pub(crate) const ACME_MUTATING: &[&str] = &[
    "CreateAcmeEndpoint",
    "UpdateAcmeEndpoint",
    "DeleteAcmeEndpoint",
    "CreateAcmeExternalAccountBinding",
    "RevokeAcmeExternalAccountBinding",
    "DeleteAcmeExternalAccountBinding",
    "CreateAcmeDomainValidation",
    "UpdateAcmeDomainValidation",
    "DeleteAcmeDomainValidation",
    "RevokeAcmeAccount",
];

fn validation(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}

fn not_found(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ResourceNotFoundException", msg)
}

fn conflict(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ConflictException", msg)
}

fn require(body: &Value, field: &str) -> Result<String, AwsServiceError> {
    body.get(field)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| validation(format!("{field} is required")))
}

fn tags_of(body: &Value, field: &str) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    if let Some(arr) = body.get(field).and_then(Value::as_array) {
        for t in arr {
            if let Some(k) = t.get("Key").and_then(Value::as_str) {
                out.insert(
                    k.to_string(),
                    t.get("Value")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_string(),
                );
            }
        }
    }
    out
}

fn tag_list(tags: &BTreeMap<String, String>) -> Vec<Value> {
    tags.iter()
        .map(|(k, v)| json!({ "Key": k, "Value": v }))
        .collect()
}

/// Shared pagination for the three ACME list operations.
fn page(items: Vec<Value>, body: &Value, list_key: &str) -> Result<AwsResponse, AwsServiceError> {
    let max_results = body
        .get("MaxResults")
        .and_then(Value::as_u64)
        .map(|n| n as usize)
        .unwrap_or(100);
    if !(1..=1000).contains(&max_results) {
        return Err(validation("MaxResults must be between 1 and 1000"));
    }
    let next_token = body.get("NextToken").and_then(Value::as_str);
    let (items, token) = paginate_checked(&items, next_token, max_results)
        .map_err(|_| validation("Invalid NextToken"))?;
    let mut out = json!({ list_key: items });
    if let Some(t) = token {
        out["NextToken"] = json!(t);
    }
    Ok(AwsResponse::ok_json(out))
}

fn endpoint_json(e: &AcmeEndpoint) -> Value {
    let mut out = json!({
        "AcmeEndpointArn": e.arn,
        "EndpointUrl": e.endpoint_url,
        "Status": e.status,
        "AuthorizationBehavior": e.authorization_behavior,
        "CertificateAuthority": e.certificate_authority,
        "CreatedAt": e.created_at.timestamp() as f64,
        "UpdatedAt": e.updated_at.timestamp() as f64,
    });
    if let Some(c) = &e.contact {
        out["Contact"] = json!(c);
    }
    if !e.certificate_tags.is_empty() {
        out["CertificateTags"] = json!(tag_list(&e.certificate_tags));
    }
    out
}

fn binding_json(b: &AcmeBinding) -> Value {
    let mut out = json!({
        "AcmeExternalAccountBindingArn": b.arn,
        "AcmeEndpointArn": b.endpoint_arn,
        "RoleArn": b.role_arn,
        "CreatedAt": b.created_at.timestamp() as f64,
        "UpdatedAt": b.updated_at.timestamp() as f64,
    });
    for (key, ts) in [
        ("ExpiresAt", b.expires_at),
        ("RevokedAt", b.revoked_at),
        ("LastUsedAt", b.last_used_at),
    ] {
        if let Some(t) = ts {
            out[key] = json!(t.timestamp() as f64);
        }
    }
    out
}

fn domain_validation_json(d: &AcmeDomainValidation) -> Value {
    let mut details = json!({});
    if let Some(s) = &d.domain_scope {
        details["DomainScope"] = json!(s);
    }
    if let Some(z) = &d.hosted_zone_id {
        details["HostedZoneId"] = json!(z);
    }
    details["ResourceRecord"] = json!({
        "Name": d.record_name,
        "Type": "CNAME",
        "Value": d.record_value,
    });
    json!({
        "AcmeDomainValidationArn": d.arn,
        "AcmeEndpointArn": d.endpoint_arn,
        "DomainName": d.domain_name,
        "PrevalidationType": d.prevalidation_type,
        "PrevalidationDetails": { "DnsPrevalidation": details },
        "Status": d.status,
        "CreatedAt": d.created_at.timestamp() as f64,
        "UpdatedAt": d.updated_at.timestamp() as f64,
    })
}

impl AcmService {
    // ---- endpoints ----

    pub(crate) fn create_acme_endpoint(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let behavior = require(&body, "AuthorizationBehavior")?;
        if behavior != "PRE_APPROVED" {
            return Err(validation(format!(
                "AuthorizationBehavior has an invalid value '{behavior}'"
            )));
        }
        let ca = body
            .get("CertificateAuthority")
            .filter(|v| v.get("PublicCertificateAuthority").is_some())
            .cloned()
            .ok_or_else(|| {
                validation("CertificateAuthority.PublicCertificateAuthority is required")
            })?;
        if let Some(contact) = body.get("Contact").and_then(Value::as_str) {
            if !matches!(contact, "REQUIRED" | "NOT_REQUIRED") {
                return Err(validation(format!(
                    "Contact has an invalid value '{contact}'"
                )));
            }
        }
        let token = body
            .get("IdempotencyToken")
            .and_then(Value::as_str)
            .map(str::to_string);

        let region = req.region.clone();
        let account_id = req.account_id.clone();
        let mut state = self.state.write();
        let acct = crate::service::account_mut(&mut state, &req.account_id);

        // A repeat create with the same token returns the endpoint the first
        // call made, rather than minting a second one.
        if let Some(t) = &token {
            if let Some(existing) = acct
                .acme_endpoints
                .values()
                .find(|e| e.idempotency_token.as_deref() == Some(t.as_str()))
            {
                return Ok(AwsResponse::ok_json(
                    json!({ "AcmeEndpointArn": existing.arn }),
                ));
            }
        }

        let id = Uuid::new_v4().simple().to_string();
        let arn = format!("arn:aws:acm:{region}:{account_id}:acme-endpoint/{id}");
        let now = Utc::now();
        let endpoint = AcmeEndpoint {
            arn: arn.clone(),
            endpoint_url: format!("https://acme.{region}.amazonaws.com/{id}/directory"),
            // Creation is synchronous here, so the endpoint is usable at once.
            status: "ACTIVE".to_string(),
            authorization_behavior: behavior,
            contact: body
                .get("Contact")
                .and_then(Value::as_str)
                .map(str::to_string),
            certificate_authority: ca,
            certificate_tags: tags_of(&body, "CertificateTags"),
            tags: tags_of(&body, "Tags"),
            created_at: now,
            updated_at: now,
            idempotency_token: token,
        };
        acct.acme_endpoints.insert(arn.clone(), endpoint);
        Ok(AwsResponse::ok_json(json!({ "AcmeEndpointArn": arn })))
    }

    pub(crate) fn describe_acme_endpoint(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = require(&body, "AcmeEndpointArn")?;
        let state = self.state.read();
        let e = state
            .accounts
            .get(&req.account_id)
            .and_then(|a| a.acme_endpoints.get(&arn))
            .ok_or_else(|| not_found(format!("ACME endpoint not found: {arn}")))?;
        Ok(AwsResponse::ok_json(
            json!({ "AcmeEndpoint": endpoint_json(e) }),
        ))
    }

    pub(crate) fn list_acme_endpoints(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let state = self.state.read();
        let items: Vec<Value> = state
            .accounts
            .get(&req.account_id)
            .map(|a| a.acme_endpoints.values().map(endpoint_json).collect())
            .unwrap_or_default();
        page(items, &body, "AcmeEndpoints")
    }

    pub(crate) fn update_acme_endpoint(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = require(&body, "AcmeEndpointArn")?;
        let mut state = self.state.write();
        let acct = crate::service::account_mut(&mut state, &req.account_id);
        let e = acct
            .acme_endpoints
            .get_mut(&arn)
            .ok_or_else(|| not_found(format!("ACME endpoint not found: {arn}")))?;

        if let Some(behavior) = body.get("AuthorizationBehavior").and_then(Value::as_str) {
            if behavior != "PRE_APPROVED" {
                return Err(validation(format!(
                    "AuthorizationBehavior has an invalid value '{behavior}'"
                )));
            }
            e.authorization_behavior = behavior.to_string();
        }
        if let Some(contact) = body.get("Contact").and_then(Value::as_str) {
            if !matches!(contact, "REQUIRED" | "NOT_REQUIRED") {
                return Err(validation(format!(
                    "Contact has an invalid value '{contact}'"
                )));
            }
            e.contact = Some(contact.to_string());
        }
        if let Some(ca) = body.get("CertificateAuthority") {
            if ca.get("PublicCertificateAuthority").is_none() {
                return Err(validation(
                    "CertificateAuthority.PublicCertificateAuthority is required",
                ));
            }
            e.certificate_authority = ca.clone();
        }
        e.updated_at = Utc::now();
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn delete_acme_endpoint(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = require(&body, "AcmeEndpointArn")?;
        let mut state = self.state.write();
        let acct = crate::service::account_mut(&mut state, &req.account_id);
        if acct.acme_endpoints.remove(&arn).is_none() {
            return Err(not_found(format!("ACME endpoint not found: {arn}")));
        }
        // Bindings, domain validations and accounts belong to the endpoint;
        // leaving them behind would strand children pointing at a gone parent.
        acct.acme_bindings.retain(|_, b| b.endpoint_arn != arn);
        acct.acme_domain_validations
            .retain(|_, d| d.endpoint_arn != arn);
        acct.acme_accounts.retain(|_, a| a.endpoint_arn != arn);
        Ok(AwsResponse::ok_json(json!({})))
    }

    // ---- external account bindings ----

    fn require_endpoint(
        acct: &crate::state::AccountState,
        arn: &str,
    ) -> Result<(), AwsServiceError> {
        if acct.acme_endpoints.contains_key(arn) {
            Ok(())
        } else {
            Err(not_found(format!("ACME endpoint not found: {arn}")))
        }
    }

    pub(crate) fn create_acme_external_account_binding(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let endpoint_arn = require(&body, "AcmeEndpointArn")?;
        let role_arn = require(&body, "RoleArn")?;
        let token = body
            .get("IdempotencyToken")
            .and_then(Value::as_str)
            .map(str::to_string);

        // An expiration is `{Value, Type}` in MINUTES / HOURS / DAYS.
        let expires_at = match body.get("Expiration") {
            None => None,
            Some(e) => {
                let value = e
                    .get("Value")
                    .and_then(Value::as_i64)
                    .filter(|v| *v > 0)
                    .ok_or_else(|| validation("Expiration.Value must be a positive integer"))?;
                let unit = e
                    .get("Type")
                    .and_then(Value::as_str)
                    .ok_or_else(|| validation("Expiration.Type is required"))?;
                let d = match unit {
                    "MINUTES" => Duration::minutes(value),
                    "HOURS" => Duration::hours(value),
                    "DAYS" => Duration::days(value),
                    other => {
                        return Err(validation(format!(
                            "Expiration.Type has an invalid value '{other}'"
                        )))
                    }
                };
                Some(Utc::now() + d)
            }
        };

        let region = req.region.clone();
        let account_id = req.account_id.clone();
        let mut state = self.state.write();
        let acct = crate::service::account_mut(&mut state, &req.account_id);
        Self::require_endpoint(acct, &endpoint_arn)?;

        if let Some(t) = &token {
            if let Some(existing) = acct
                .acme_bindings
                .values()
                .find(|b| b.idempotency_token.as_deref() == Some(t.as_str()))
            {
                return Ok(AwsResponse::ok_json(
                    json!({ "ExternalAccountBinding": binding_json(existing) }),
                ));
            }
        }

        let id = Uuid::new_v4().simple().to_string();
        let arn = format!("arn:aws:acm:{region}:{account_id}:acme-external-account-binding/{id}");
        let now = Utc::now();
        let binding = AcmeBinding {
            arn: arn.clone(),
            endpoint_arn,
            role_arn,
            key_id: id.clone(),
            // A real HMAC secret, generated once and only readable through
            // GetAcmeExternalAccountBindingCredentials.
            mac_key: base64::Engine::encode(
                &base64::engine::general_purpose::URL_SAFE_NO_PAD,
                Uuid::new_v4().as_bytes(),
            ),
            expires_at,
            revoked_at: None,
            last_used_at: None,
            created_at: now,
            updated_at: now,
            tags: tags_of(&body, "Tags"),
            idempotency_token: token,
        };
        let out = binding_json(&binding);
        acct.acme_bindings.insert(arn, binding);
        Ok(AwsResponse::ok_json(
            json!({ "ExternalAccountBinding": out }),
        ))
    }

    pub(crate) fn describe_acme_external_account_binding(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = require(&body, "AcmeExternalAccountBindingArn")?;
        let state = self.state.read();
        let b = state
            .accounts
            .get(&req.account_id)
            .and_then(|a| a.acme_bindings.get(&arn))
            .ok_or_else(|| not_found(format!("External account binding not found: {arn}")))?;
        Ok(AwsResponse::ok_json(
            json!({ "ExternalAccountBinding": binding_json(b) }),
        ))
    }

    pub(crate) fn list_acme_external_account_bindings(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let endpoint_arn = require(&body, "AcmeEndpointArn")?;
        let state = self.state.read();
        let empty = Default::default();
        let acct = state.accounts.get(&req.account_id).unwrap_or(&empty);
        Self::require_endpoint(acct, &endpoint_arn)?;
        let items: Vec<Value> = acct
            .acme_bindings
            .values()
            .filter(|b| b.endpoint_arn == endpoint_arn)
            .map(binding_json)
            .collect();
        page(items, &body, "ExternalAccountBindings")
    }

    pub(crate) fn revoke_acme_external_account_binding(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = require(&body, "AcmeExternalAccountBindingArn")?;
        let mut state = self.state.write();
        let acct = crate::service::account_mut(&mut state, &req.account_id);
        let b = acct
            .acme_bindings
            .get_mut(&arn)
            .ok_or_else(|| not_found(format!("External account binding not found: {arn}")))?;
        if b.revoked_at.is_some() {
            return Err(conflict(format!("Binding {arn} is already revoked")));
        }
        let now = Utc::now();
        b.revoked_at = Some(now);
        b.updated_at = now;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn delete_acme_external_account_binding(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = require(&body, "AcmeExternalAccountBindingArn")?;
        let mut state = self.state.write();
        let acct = crate::service::account_mut(&mut state, &req.account_id);
        if acct.acme_bindings.remove(&arn).is_none() {
            return Err(not_found(format!(
                "External account binding not found: {arn}"
            )));
        }
        // Accounts registered through the binding lose their link to it.
        for account in acct.acme_accounts.values_mut() {
            if account.binding_arn.as_deref() == Some(arn.as_str()) {
                account.binding_arn = None;
            }
        }
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn get_acme_external_account_binding_credentials(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = require(&body, "AcmeExternalAccountBindingArn")?;
        let mut state = self.state.write();
        let acct = crate::service::account_mut(&mut state, &req.account_id);
        let b = acct
            .acme_bindings
            .get_mut(&arn)
            .ok_or_else(|| not_found(format!("External account binding not found: {arn}")))?;
        // Revoked or expired credentials are no longer usable, so they are not
        // handed out.
        if b.revoked_at.is_some() {
            return Err(conflict(format!("Binding {arn} is revoked")));
        }
        if b.expires_at.is_some_and(|e| e <= Utc::now()) {
            return Err(conflict(format!("Binding {arn} has expired")));
        }
        b.last_used_at = Some(Utc::now());
        Ok(AwsResponse::ok_json(json!({
            "KeyId": b.key_id,
            "MacKey": b.mac_key,
        })))
    }

    // ---- domain validations ----

    pub(crate) fn create_acme_domain_validation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let endpoint_arn = require(&body, "AcmeEndpointArn")?;
        let domain_name = require(&body, "DomainName")?;
        let options = body
            .get("PrevalidationOptions")
            .and_then(|o| o.get("DnsPrevalidation"))
            .cloned()
            .ok_or_else(|| validation("PrevalidationOptions.DnsPrevalidation is required"))?;
        let domain_scope = options
            .get("DomainScope")
            .and_then(Value::as_str)
            .map(str::to_string);
        if let Some(s) = &domain_scope {
            if !matches!(
                s.as_str(),
                "DOMAIN" | "SUBDOMAINS" | "DOMAIN_AND_SUBDOMAINS"
            ) {
                return Err(validation(format!(
                    "DomainScope has an invalid value '{s}'"
                )));
            }
        }
        let token = body
            .get("IdempotencyToken")
            .and_then(Value::as_str)
            .map(str::to_string);

        let region = req.region.clone();
        let account_id = req.account_id.clone();
        let mut state = self.state.write();
        let acct = crate::service::account_mut(&mut state, &req.account_id);
        Self::require_endpoint(acct, &endpoint_arn)?;

        if let Some(t) = &token {
            if let Some(existing) = acct
                .acme_domain_validations
                .values()
                .find(|d| d.idempotency_token.as_deref() == Some(t.as_str()))
            {
                return Ok(AwsResponse::ok_json(
                    json!({ "AcmeDomainValidationArn": existing.arn }),
                ));
            }
        }
        // One pre-validation per domain per endpoint.
        if acct
            .acme_domain_validations
            .values()
            .any(|d| d.endpoint_arn == endpoint_arn && d.domain_name == domain_name)
        {
            return Err(conflict(format!(
                "A domain validation for {domain_name} already exists on this endpoint"
            )));
        }

        let id = Uuid::new_v4().simple().to_string();
        let arn = format!("arn:aws:acm:{region}:{account_id}:acme-domain-validation/{id}");
        let now = Utc::now();
        let validation_record = AcmeDomainValidation {
            arn: arn.clone(),
            endpoint_arn,
            record_name: format!("_acme-challenge.{domain_name}"),
            record_value: format!("{id}.acm-validations.aws"),
            domain_name,
            prevalidation_type: "DNS_PREVALIDATION".to_string(),
            domain_scope,
            hosted_zone_id: options
                .get("HostedZoneId")
                .and_then(Value::as_str)
                .map(str::to_string),
            // Nothing checks DNS here, so the record starts unvalidated and
            // settles on the first describe — the same lazy transition the
            // rest of fakecloud uses.
            status: "VALIDATING".to_string(),
            created_at: now,
            updated_at: now,
            tags: tags_of(&body, "Tags"),
            idempotency_token: token,
        };
        acct.acme_domain_validations
            .insert(arn.clone(), validation_record);
        Ok(AwsResponse::ok_json(
            json!({ "AcmeDomainValidationArn": arn }),
        ))
    }

    pub(crate) fn describe_acme_domain_validation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = require(&body, "AcmeDomainValidationArn")?;
        let mut state = self.state.write();
        let acct = crate::service::account_mut(&mut state, &req.account_id);
        let d = acct
            .acme_domain_validations
            .get_mut(&arn)
            .ok_or_else(|| not_found(format!("Domain validation not found: {arn}")))?;
        if d.status == "VALIDATING" {
            d.status = "VALID".to_string();
            d.updated_at = Utc::now();
        }
        Ok(AwsResponse::ok_json(
            json!({ "AcmeDomainValidation": domain_validation_json(d) }),
        ))
    }

    pub(crate) fn list_acme_domain_validations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let endpoint_arn = require(&body, "AcmeEndpointArn")?;
        let state = self.state.read();
        let empty = Default::default();
        let acct = state.accounts.get(&req.account_id).unwrap_or(&empty);
        Self::require_endpoint(acct, &endpoint_arn)?;
        let items: Vec<Value> = acct
            .acme_domain_validations
            .values()
            .filter(|d| d.endpoint_arn == endpoint_arn)
            .map(domain_validation_json)
            .collect();
        page(items, &body, "AcmeDomainValidations")
    }

    pub(crate) fn update_acme_domain_validation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = require(&body, "AcmeDomainValidationArn")?;
        let mut state = self.state.write();
        let acct = crate::service::account_mut(&mut state, &req.account_id);
        let d = acct
            .acme_domain_validations
            .get_mut(&arn)
            .ok_or_else(|| not_found(format!("Domain validation not found: {arn}")))?;
        if let Some(options) = body
            .get("PrevalidationOptions")
            .and_then(|o| o.get("DnsPrevalidation"))
        {
            if let Some(s) = options.get("DomainScope").and_then(Value::as_str) {
                if !matches!(s, "DOMAIN" | "SUBDOMAINS" | "DOMAIN_AND_SUBDOMAINS") {
                    return Err(validation(format!(
                        "DomainScope has an invalid value '{s}'"
                    )));
                }
                d.domain_scope = Some(s.to_string());
            }
            if let Some(z) = options.get("HostedZoneId").and_then(Value::as_str) {
                d.hosted_zone_id = Some(z.to_string());
            }
        }
        d.updated_at = Utc::now();
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(crate) fn delete_acme_domain_validation(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn = require(&body, "AcmeDomainValidationArn")?;
        let mut state = self.state.write();
        let acct = crate::service::account_mut(&mut state, &req.account_id);
        acct.acme_domain_validations
            .remove(&arn)
            .ok_or_else(|| not_found(format!("Domain validation not found: {arn}")))?;
        Ok(AwsResponse::ok_json(json!({})))
    }

    // ---- accounts ----

    pub(crate) fn describe_acme_account(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let endpoint_arn = require(&body, "AcmeEndpointArn")?;
        let account_url = require(&body, "AccountUrl")?;
        let state = self.state.read();
        let empty = Default::default();
        let acct = state.accounts.get(&req.account_id).unwrap_or(&empty);
        Self::require_endpoint(acct, &endpoint_arn)?;
        let a = acct
            .acme_accounts
            .values()
            .find(|a| a.endpoint_arn == endpoint_arn && a.account_url == account_url)
            .ok_or_else(|| not_found(format!("ACME account not found: {account_url}")))?;
        let mut out = json!({
            "AccountUrl": a.account_url,
            "PublicKeyThumbprint": a.public_key_thumbprint,
            "Status": a.status,
            "CreatedAt": a.created_at.timestamp() as f64,
            "Contacts": a.contacts,
        });
        if let Some(b) = &a.binding_arn {
            out["AcmeExternalAccountBindingArn"] = json!(b);
        }
        Ok(AwsResponse::ok_json(json!({ "AcmeAccount": out })))
    }

    pub(crate) fn list_acme_accounts(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let endpoint_arn = require(&body, "AcmeEndpointArn")?;
        let state = self.state.read();
        let empty = Default::default();
        let acct = state.accounts.get(&req.account_id).unwrap_or(&empty);
        Self::require_endpoint(acct, &endpoint_arn)?;
        let items: Vec<Value> = acct
            .acme_accounts
            .values()
            .filter(|a| a.endpoint_arn == endpoint_arn)
            .map(|a| {
                json!({
                    "AccountUrl": a.account_url,
                    "PublicKeyThumbprint": a.public_key_thumbprint,
                    "Status": a.status,
                    "CreatedAt": a.created_at.timestamp() as f64,
                    "Contacts": a.contacts,
                })
            })
            .collect();
        page(items, &body, "AcmeAccounts")
    }

    pub(crate) fn revoke_acme_account(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let endpoint_arn = require(&body, "AcmeEndpointArn")?;
        let account_url = require(&body, "AccountUrl")?;
        let mut state = self.state.write();
        let acct = crate::service::account_mut(&mut state, &req.account_id);
        Self::require_endpoint(acct, &endpoint_arn)?;
        let a = acct
            .acme_accounts
            .values_mut()
            .find(|a| a.endpoint_arn == endpoint_arn && a.account_url == account_url)
            .ok_or_else(|| not_found(format!("ACME account not found: {account_url}")))?;
        if a.status == "REVOKED" {
            return Err(conflict(format!(
                "Account {account_url} is already revoked"
            )));
        }
        a.status = "REVOKED".to_string();
        Ok(AwsResponse::ok_json(json!({})))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{AcmAccounts, SharedAcmState};
    use fakecloud_core::service::AwsService;

    fn svc() -> AcmService {
        AcmService::default()
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "acm".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "rid".to_string(),
            headers: http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: bytes::Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn json_of(resp: AwsResponse) -> Value {
        serde_json::from_slice(resp.body.expect_bytes()).unwrap()
    }

    fn ca() -> Value {
        json!({ "PublicCertificateAuthority": { "AllowedKeyAlgorithms": ["RSA_2048"] } })
    }

    fn make_endpoint(s: &AcmService) -> String {
        let body = json!({ "AuthorizationBehavior": "PRE_APPROVED", "CertificateAuthority": ca() });
        json_of(
            s.create_acme_endpoint(&req("CreateAcmeEndpoint", body))
                .unwrap(),
        )["AcmeEndpointArn"]
            .as_str()
            .unwrap()
            .to_string()
    }

    fn make_binding(s: &AcmService, endpoint: &str) -> String {
        let body = json!({
            "AcmeEndpointArn": endpoint,
            "RoleArn": "arn:aws:iam::123456789012:role/acme",
        });
        json_of(
            s.create_acme_external_account_binding(&req("CreateAcmeExternalAccountBinding", body))
                .unwrap(),
        )["ExternalAccountBinding"]["AcmeExternalAccountBindingArn"]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[test]
    fn endpoint_create_describe_update_delete() {
        let s = svc();
        let arn = make_endpoint(&s);
        assert!(arn.contains(":acme-endpoint/"), "{arn}");

        let e = json_of(
            s.describe_acme_endpoint(&req(
                "DescribeAcmeEndpoint",
                json!({ "AcmeEndpointArn": arn }),
            ))
            .unwrap(),
        )["AcmeEndpoint"]
            .clone();
        assert_eq!(e["Status"], "ACTIVE");
        assert_eq!(e["AuthorizationBehavior"], "PRE_APPROVED");
        assert!(e["EndpointUrl"].as_str().unwrap().ends_with("/directory"));

        s.update_acme_endpoint(&req(
            "UpdateAcmeEndpoint",
            json!({ "AcmeEndpointArn": arn, "Contact": "REQUIRED" }),
        ))
        .unwrap();
        let e = json_of(
            s.describe_acme_endpoint(&req(
                "DescribeAcmeEndpoint",
                json!({ "AcmeEndpointArn": arn }),
            ))
            .unwrap(),
        )["AcmeEndpoint"]
            .clone();
        assert_eq!(e["Contact"], "REQUIRED");

        let listed = json_of(
            s.list_acme_endpoints(&req("ListAcmeEndpoints", json!({})))
                .unwrap(),
        );
        assert_eq!(listed["AcmeEndpoints"].as_array().unwrap().len(), 1);

        s.delete_acme_endpoint(&req(
            "DeleteAcmeEndpoint",
            json!({ "AcmeEndpointArn": arn }),
        ))
        .unwrap();
        let err = s
            .describe_acme_endpoint(&req(
                "DescribeAcmeEndpoint",
                json!({ "AcmeEndpointArn": arn }),
            ))
            .err()
            .unwrap();
        assert_eq!(err.code(), "ResourceNotFoundException");
    }

    #[test]
    fn idempotency_token_returns_the_same_endpoint() {
        let s = svc();
        let body = json!({
            "AuthorizationBehavior": "PRE_APPROVED",
            "CertificateAuthority": ca(),
            "IdempotencyToken": "tok-1",
        });
        let first = json_of(
            s.create_acme_endpoint(&req("CreateAcmeEndpoint", body.clone()))
                .unwrap(),
        );
        let second = json_of(
            s.create_acme_endpoint(&req("CreateAcmeEndpoint", body))
                .unwrap(),
        );
        assert_eq!(first["AcmeEndpointArn"], second["AcmeEndpointArn"]);
        let listed = json_of(
            s.list_acme_endpoints(&req("ListAcmeEndpoints", json!({})))
                .unwrap(),
        );
        assert_eq!(listed["AcmeEndpoints"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn binding_credentials_are_withheld_once_revoked() {
        let s = svc();
        let endpoint = make_endpoint(&s);
        let binding = make_binding(&s, &endpoint);

        let creds = json_of(
            s.get_acme_external_account_binding_credentials(&req(
                "GetAcmeExternalAccountBindingCredentials",
                json!({ "AcmeExternalAccountBindingArn": binding }),
            ))
            .unwrap(),
        );
        assert!(!creds["KeyId"].as_str().unwrap().is_empty());
        assert!(!creds["MacKey"].as_str().unwrap().is_empty());

        // Fetching credentials records the use.
        let b = json_of(
            s.describe_acme_external_account_binding(&req(
                "DescribeAcmeExternalAccountBinding",
                json!({ "AcmeExternalAccountBindingArn": binding }),
            ))
            .unwrap(),
        )["ExternalAccountBinding"]
            .clone();
        assert!(b.get("LastUsedAt").is_some());

        s.revoke_acme_external_account_binding(&req(
            "RevokeAcmeExternalAccountBinding",
            json!({ "AcmeExternalAccountBindingArn": binding }),
        ))
        .unwrap();
        // Revoked credentials are no longer handed out, and a second revoke
        // conflicts.
        let err = s
            .get_acme_external_account_binding_credentials(&req(
                "GetAcmeExternalAccountBindingCredentials",
                json!({ "AcmeExternalAccountBindingArn": binding }),
            ))
            .err()
            .unwrap();
        assert_eq!(err.code(), "ConflictException");
        let err = s
            .revoke_acme_external_account_binding(&req(
                "RevokeAcmeExternalAccountBinding",
                json!({ "AcmeExternalAccountBindingArn": binding }),
            ))
            .err()
            .unwrap();
        assert_eq!(err.code(), "ConflictException");
    }

    #[test]
    fn expired_binding_credentials_are_withheld() {
        let s = svc();
        let endpoint = make_endpoint(&s);
        let body = json!({
            "AcmeEndpointArn": endpoint,
            "RoleArn": "arn:aws:iam::123456789012:role/acme",
            "Expiration": { "Value": 1, "Type": "MINUTES" },
        });
        let binding = json_of(
            s.create_acme_external_account_binding(&req("CreateAcmeExternalAccountBinding", body))
                .unwrap(),
        )["ExternalAccountBinding"]["AcmeExternalAccountBindingArn"]
            .as_str()
            .unwrap()
            .to_string();

        // Wind the expiry into the past.
        {
            let mut state = s.state.write();
            let acct = crate::service::account_mut(&mut state, "123456789012");
            acct.acme_bindings.get_mut(&binding).unwrap().expires_at =
                Some(Utc::now() - Duration::minutes(1));
        }
        let err = s
            .get_acme_external_account_binding_credentials(&req(
                "GetAcmeExternalAccountBindingCredentials",
                json!({ "AcmeExternalAccountBindingArn": binding }),
            ))
            .err()
            .unwrap();
        assert_eq!(err.code(), "ConflictException");
    }

    #[test]
    fn domain_validation_settles_and_rejects_a_duplicate() {
        let s = svc();
        let endpoint = make_endpoint(&s);
        let body = json!({
            "AcmeEndpointArn": endpoint,
            "DomainName": "example.com",
            "PrevalidationOptions": { "DnsPrevalidation": { "DomainScope": "DOMAIN" } },
        });
        let arn = json_of(
            s.create_acme_domain_validation(&req("CreateAcmeDomainValidation", body.clone()))
                .unwrap(),
        )["AcmeDomainValidationArn"]
            .as_str()
            .unwrap()
            .to_string();

        // Describe settles VALIDATING -> VALID and carries the DNS record an
        // operator has to publish.
        let d = json_of(
            s.describe_acme_domain_validation(&req(
                "DescribeAcmeDomainValidation",
                json!({ "AcmeDomainValidationArn": arn }),
            ))
            .unwrap(),
        )["AcmeDomainValidation"]
            .clone();
        assert_eq!(d["Status"], "VALID");
        assert_eq!(d["PrevalidationType"], "DNS_PREVALIDATION");
        let record = &d["PrevalidationDetails"]["DnsPrevalidation"]["ResourceRecord"];
        assert_eq!(record["Type"], "CNAME");
        assert_eq!(record["Name"], "_acme-challenge.example.com");

        // One pre-validation per domain per endpoint.
        let err = s
            .create_acme_domain_validation(&req("CreateAcmeDomainValidation", body))
            .err()
            .unwrap();
        assert_eq!(err.code(), "ConflictException");
    }

    #[test]
    fn children_are_listed_by_endpoint_and_cascade_on_delete() {
        let s = svc();
        let first = make_endpoint(&s);
        let second = make_endpoint(&s);
        let binding = make_binding(&s, &first);
        make_binding(&s, &second);
        s.create_acme_domain_validation(&req(
            "CreateAcmeDomainValidation",
            json!({
                "AcmeEndpointArn": first,
                "DomainName": "a.example.com",
                "PrevalidationOptions": { "DnsPrevalidation": {} },
            }),
        ))
        .unwrap();

        // Each list is scoped to its endpoint, not the whole account.
        let listed = json_of(
            s.list_acme_external_account_bindings(&req(
                "ListAcmeExternalAccountBindings",
                json!({ "AcmeEndpointArn": first }),
            ))
            .unwrap(),
        );
        assert_eq!(
            listed["ExternalAccountBindings"].as_array().unwrap().len(),
            1
        );
        let listed = json_of(
            s.list_acme_domain_validations(&req(
                "ListAcmeDomainValidations",
                json!({ "AcmeEndpointArn": second }),
            ))
            .unwrap(),
        );
        assert!(listed["AcmeDomainValidations"]
            .as_array()
            .unwrap()
            .is_empty());

        // Deleting the endpoint takes its children with it; the other
        // endpoint's binding survives.
        s.delete_acme_endpoint(&req(
            "DeleteAcmeEndpoint",
            json!({ "AcmeEndpointArn": first }),
        ))
        .unwrap();
        let err = s
            .describe_acme_external_account_binding(&req(
                "DescribeAcmeExternalAccountBinding",
                json!({ "AcmeExternalAccountBindingArn": binding }),
            ))
            .err()
            .unwrap();
        assert_eq!(err.code(), "ResourceNotFoundException");
        let listed = json_of(
            s.list_acme_external_account_bindings(&req(
                "ListAcmeExternalAccountBindings",
                json!({ "AcmeEndpointArn": second }),
            ))
            .unwrap(),
        );
        assert_eq!(
            listed["ExternalAccountBindings"].as_array().unwrap().len(),
            1
        );
    }

    #[test]
    fn accounts_resolve_and_revoke_through_their_endpoint() {
        let s = svc();
        let endpoint = make_endpoint(&s);
        {
            let mut state = s.state.write();
            let acct = crate::service::account_mut(&mut state, "123456789012");
            acct.acme_accounts.insert(
                "acct-1".to_string(),
                crate::state::AcmeAccount {
                    endpoint_arn: endpoint.clone(),
                    account_url: "https://acme.example/acct/1".to_string(),
                    public_key_thumbprint: "thumb".to_string(),
                    status: "VALID".to_string(),
                    binding_arn: None,
                    contacts: vec!["mailto:ops@example.com".to_string()],
                    created_at: Utc::now(),
                },
            );
        }

        let a = json_of(
            s.describe_acme_account(&req(
                "DescribeAcmeAccount",
                json!({
                    "AcmeEndpointArn": endpoint,
                    "AccountUrl": "https://acme.example/acct/1",
                }),
            ))
            .unwrap(),
        )["AcmeAccount"]
            .clone();
        assert_eq!(a["Status"], "VALID");
        assert_eq!(a["Contacts"][0], "mailto:ops@example.com");

        s.revoke_acme_account(&req(
            "RevokeAcmeAccount",
            json!({
                "AcmeEndpointArn": endpoint,
                "AccountUrl": "https://acme.example/acct/1",
            }),
        ))
        .unwrap();
        let a = json_of(
            s.describe_acme_account(&req(
                "DescribeAcmeAccount",
                json!({
                    "AcmeEndpointArn": endpoint,
                    "AccountUrl": "https://acme.example/acct/1",
                }),
            ))
            .unwrap(),
        )["AcmeAccount"]
            .clone();
        assert_eq!(a["Status"], "REVOKED");
    }

    #[test]
    fn required_members_and_enums_are_validated() {
        let s = svc();
        let endpoint = make_endpoint(&s);

        // AuthorizationBehavior and the CA union are both required on create.
        for body in [
            json!({ "CertificateAuthority": ca() }),
            json!({ "AuthorizationBehavior": "PRE_APPROVED" }),
            json!({ "AuthorizationBehavior": "OPEN", "CertificateAuthority": ca() }),
        ] {
            let err = s
                .create_acme_endpoint(&req("CreateAcmeEndpoint", body))
                .err()
                .unwrap();
            assert_eq!(err.code(), "ValidationException");
        }

        // Children must name an endpoint that exists.
        let err = s
            .create_acme_external_account_binding(&req(
                "CreateAcmeExternalAccountBinding",
                json!({
                    "AcmeEndpointArn": "arn:aws:acm:us-east-1:123456789012:acme-endpoint/ghost",
                    "RoleArn": "arn:aws:iam::123456789012:role/acme",
                }),
            ))
            .err()
            .unwrap();
        assert_eq!(err.code(), "ResourceNotFoundException");

        // An unknown Expiration unit is rejected.
        let err = s
            .create_acme_external_account_binding(&req(
                "CreateAcmeExternalAccountBinding",
                json!({
                    "AcmeEndpointArn": endpoint,
                    "RoleArn": "arn:aws:iam::123456789012:role/acme",
                    "Expiration": { "Value": 1, "Type": "FORTNIGHTS" },
                }),
            ))
            .err()
            .unwrap();
        assert_eq!(err.code(), "ValidationException");

        // DomainScope is enum-bound.
        let err = s
            .create_acme_domain_validation(&req(
                "CreateAcmeDomainValidation",
                json!({
                    "AcmeEndpointArn": endpoint,
                    "DomainName": "x.example.com",
                    "PrevalidationOptions": { "DnsPrevalidation": { "DomainScope": "PLANET" } },
                }),
            ))
            .err()
            .unwrap();
        assert_eq!(err.code(), "ValidationException");
    }

    #[test]
    fn acme_actions_are_all_dispatchable() {
        // Every ACME action must be reachable through `handle`, or the probe
        // reports it NOT_IMPLEMENTED however well the handler works.
        let s = svc();
        for action in ACME_ACTIONS {
            assert!(
                s.supported_actions().contains(action),
                "{action} missing from supported_actions"
            );
        }
        let _: SharedAcmState =
            std::sync::Arc::new(parking_lot::RwLock::new(AcmAccounts::default()));
    }
}
