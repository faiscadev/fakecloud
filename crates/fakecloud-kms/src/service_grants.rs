// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use chrono::Utc;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

/// The grant operations that take an encryption context, plus those AWS lets
/// a grant with encryption-context constraints include although they take
/// none (`DescribeKey`, `RetireGrant`: the constraint does not apply to
/// them) and `CreateGrant` (the constraint binds the grants it creates).
const EC_CONSTRAINABLE_OPERATIONS: &[&str] = &[
    "Decrypt",
    "Encrypt",
    "GenerateDataKey",
    "GenerateDataKeyWithoutPlaintext",
    "GenerateDataKeyPair",
    "GenerateDataKeyPairWithoutPlaintext",
    "ReEncryptFrom",
    "ReEncryptTo",
    "CreateGrant",
    "DescribeKey",
    "RetireGrant",
];

/// AWS's rule for encryption-context grant constraints (CreateGrant API
/// reference, `Constraints`): they are supported only on grant operations
/// that include an `EncryptionContext` parameter, so only in a grant for a
/// symmetric encryption key ("You cannot use an encryption context grant
/// constraint for cryptographic operations with asymmetric KMS keys or HMAC
/// KMS keys"), and such a grant may include no other operation than
/// `DescribeKey`, `RetireGrant` and `CreateGrant`. A violation is a
/// `ValidationException` (AWS documents the error, not its message).
fn validate_encryption_context_constraints(
    constraints: &Value,
    operations: &[String],
    key_spec: &str,
) -> Result<(), AwsServiceError> {
    let constrains_context = ["EncryptionContextSubset", "EncryptionContextEquals"]
        .iter()
        .any(|name| !constraints[*name].is_null());
    if !constrains_context {
        return Ok(());
    }
    let invalid = |detail: String| {
        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!(
                "Encryption context grant constraints are supported only on grant operations that include an EncryptionContext parameter: {detail}"
            ),
        ))
    };
    if key_spec != "SYMMETRIC_DEFAULT" {
        return invalid(format!(
            "operations with a {key_spec} KMS key do not support an encryption context."
        ));
    }
    if let Some(op) = operations
        .iter()
        .find(|op| !EC_CONSTRAINABLE_OPERATIONS.contains(&op.as_str()))
    {
        return invalid(format!("{op} does not support an encryption context."));
    }
    Ok(())
}

/// The grantee and retiring principals `CreateGrant` takes, as its API
/// reference requires them: exactly one of `GranteePrincipal` (an AWS
/// principal) and `GranteeServicePrincipal` (an AWS service principal), at
/// most one of `RetiringPrincipal` and `RetiringServicePrincipal`, and a
/// grant to a service principal also needs a `SourceArn` constraint and a
/// retiring principal of either kind. A violation is a `ValidationException`
/// (AWS documents the rules, not its messages).
fn validate_grant_principals(
    body: &Value,
    service_grantee: bool,
    retiring: bool,
    retiring_service: bool,
) -> Result<(), AwsServiceError> {
    let invalid = |message: &str| {
        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            message,
        ))
    };
    let aws_grantee = body["GranteePrincipal"].as_str().is_some();
    if aws_grantee == service_grantee {
        return invalid(
            "You must specify either GranteePrincipal or GranteeServicePrincipal, but not both.",
        );
    }
    if retiring && retiring_service {
        return invalid(
            "You can specify either RetiringPrincipal or RetiringServicePrincipal, but not both.",
        );
    }
    if service_grantee {
        if body["Constraints"]["SourceArn"].as_str().is_none() {
            return invalid(
                "When you specify a GranteeServicePrincipal, you must also specify a SourceArn grant constraint.",
            );
        }
        if !retiring && !retiring_service {
            return invalid(
                "When you specify a GranteeServicePrincipal, you must specify either a RetiringPrincipal or a RetiringServicePrincipal.",
            );
        }
    }
    Ok(())
}

impl KmsService {
    pub(super) fn create_grant(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let grantee_principal = body["GranteePrincipal"].as_str().unwrap_or("").to_string();
        let grantee_service_principal =
            body["GranteeServicePrincipal"].as_str().map(str::to_string);
        let retiring_principal = body["RetiringPrincipal"].as_str().map(|s| s.to_string());
        let retiring_service_principal = body["RetiringServicePrincipal"]
            .as_str()
            .map(str::to_string);
        validate_grant_principals(
            &body,
            grantee_service_principal.is_some(),
            retiring_principal.is_some(),
            retiring_service_principal.is_some(),
        )?;
        let operations: Vec<String> = body["Operations"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let constraints = if body["Constraints"].is_null() {
            None
        } else {
            Some(body["Constraints"].clone())
        };
        let name = body["Name"].as_str().map(|s| s.to_string());
        if let Some(constraints) = &constraints {
            let key_spec = {
                let accounts = self.state.read();
                accounts
                    .get(&req.account_id)
                    .and_then(|state| state.keys.get(&resolved))
                    .map(|key| key.key_spec.clone())
                    .unwrap_or_default()
            };
            validate_encryption_context_constraints(constraints, &operations, &key_spec)?;
        }

        let grant_id = Uuid::new_v4().to_string();
        let grant_token = Uuid::new_v4().to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.grants.push(KmsGrant {
            grant_id: grant_id.clone(),
            grant_token: grant_token.clone(),
            key_id: resolved,
            grantee_principal,
            grantee_service_principal,
            retiring_principal,
            retiring_service_principal,
            operations,
            constraints,
            name,
            creation_date: Utc::now().timestamp() as f64,
        });

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "GrantId": grant_id,
                "GrantToken": grant_token,
            }))
            .unwrap(),
        ))
    }

    pub(super) fn list_grants(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let grant_id_filter = body["GrantId"].as_str();
        // "You can specify either GranteePrincipal or GranteeServicePrincipal,
        // but not both."
        let grantee_filter = body["GranteePrincipal"].as_str();
        let service_grantee_filter = body["GranteeServicePrincipal"].as_str();
        if grantee_filter.is_some() && service_grantee_filter.is_some() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "You can specify either GranteePrincipal or GranteeServicePrincipal, but not both.",
            ));
        }

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let grants: Vec<Value> = state
            .grants
            .iter()
            .filter(|g| g.key_id == resolved)
            .filter(|g| grant_id_filter.is_none_or(|gid| g.grant_id == gid))
            .filter(|g| grantee_filter.is_none_or(|p| g.grantee_principal == p))
            .filter(|g| {
                service_grantee_filter
                    .is_none_or(|p| g.grantee_service_principal.as_deref() == Some(p))
            })
            .map(|g| grant_to_json(g, state, &req.region))
            .collect();

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&json!({
                "Grants": grants,
                "Truncated": false,
            }))
            .unwrap(),
        ))
    }

    pub(super) fn list_retirable_grants(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();

        // ListRetirableGrants declares InvalidArn / InvalidMarker / NotFound /
        // KMSInternal / DependencyTimeout. Map RetiringPrincipal shape
        // failures onto InvalidArnException and Limit/Marker onto
        // InvalidMarkerException.
        // "You must specify either RetiringPrincipal or
        // RetiringServicePrincipal, but not both."
        let service_principal = body["RetiringServicePrincipal"].as_str();
        if service_principal.is_some() && !body["RetiringPrincipal"].is_null() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "You must specify either RetiringPrincipal or RetiringServicePrincipal, but not both.",
            ));
        }
        let retiring_principal = match service_principal {
            Some(_) => None,
            None => {
                recoded("InvalidArnException", || {
                    validate_required("RetiringPrincipal", &body["RetiringPrincipal"])
                })?;
                let principal = body["RetiringPrincipal"].as_str().ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidArnException",
                        "RetiringPrincipal must be a string",
                    )
                })?;
                recoded("InvalidArnException", || {
                    validate_string_length("retiringPrincipal", principal, 1, 256)
                })?;
                Some(principal)
            }
        };
        recoded("InvalidMarkerException", || {
            validate_optional_json_range("limit", &body["Limit"], 1, 1000)
        })?;
        recoded("InvalidMarkerException", || {
            validate_optional_string_length("marker", body["Marker"].as_str(), 1, 320)
        })?;

        let limit = body["Limit"].as_i64().unwrap_or(1000) as usize;
        let marker = body["Marker"].as_str();

        let accounts = self.state.read();
        let empty = KmsState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let mut all_grants: Vec<Value> = state
            .grants
            .iter()
            .filter(|g| match retiring_principal {
                Some(principal) => g.retiring_principal.as_deref() == Some(principal),
                None => g.retiring_service_principal.as_deref() == service_principal,
            })
            .map(|g| grant_to_json(g, state, &req.region))
            .collect();
        // Sort by GrantId so the marker (the GrantId of the last item on the
        // previous page) is a stable total-ordering key. Grants are stored in an
        // insertion-order Vec, so without this the marker could not be resolved
        // by comparison.
        all_grants.sort_by(|a, b| a["GrantId"].as_str().cmp(&b["GrantId"].as_str()));

        // Resume at the first grant whose GrantId is strictly greater than the
        // marker, so a marker whose grant was retired/revoked between pages still
        // advances instead of falling back to 0 and restarting the listing.
        let start = match marker {
            Some(m) => all_grants
                .iter()
                .position(|g| g["GrantId"].as_str() > Some(m))
                .unwrap_or(all_grants.len()),
            None => 0,
        };

        let page = &all_grants[start..all_grants.len().min(start + limit)];
        let truncated = start + limit < all_grants.len();

        let mut result = json!({
            "Grants": page,
            "Truncated": truncated,
        });

        if truncated {
            if let Some(last) = page.last() {
                result["NextMarker"] = last["GrantId"].clone();
            }
        }

        Ok(AwsResponse::json(
            StatusCode::OK,
            serde_json::to_string(&result).unwrap(),
        ))
    }

    pub(super) fn revoke_grant(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let key_id = Self::require_key_id(&body)?;
        let grant_id = body["GrantId"].as_str().unwrap_or("");

        let resolved = self
            .resolve_key_id_for(&req.account_id, &req.region, &key_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "NotFoundException",
                    format!("Key '{key_id}' does not exist"),
                )
            })?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let idx = state
            .grants
            .iter()
            .position(|g| g.key_id == resolved && g.grant_id == grant_id);

        match idx {
            Some(i) => {
                state.grants.remove(i);
                Ok(AwsResponse::json(StatusCode::OK, "{}"))
            }
            None => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                format!("Grant ID {grant_id} not found"),
            )),
        }
    }

    pub(super) fn retire_grant(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let grant_token = body["GrantToken"].as_str();
        let grant_id = body["GrantId"].as_str();
        let key_id = body["KeyId"].as_str();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let idx = if let Some(token) = grant_token {
            state.grants.iter().position(|g| g.grant_token == token)
        } else if let (Some(kid), Some(gid)) = (key_id, grant_id) {
            let resolved = Self::resolve_key_id_with_state(state, &req.region, kid);
            resolved.and_then(|r| {
                state
                    .grants
                    .iter()
                    .position(|g| g.key_id == r && g.grant_id == gid)
            })
        } else {
            None
        };

        match idx {
            Some(i) => {
                state.grants.remove(i);
                Ok(AwsResponse::json(StatusCode::OK, "{}"))
            }
            None => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "NotFoundException",
                "Grant not found",
            )),
        }
    }
}
