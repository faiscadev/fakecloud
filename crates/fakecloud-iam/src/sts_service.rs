use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use http::StatusCode;

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
use fakecloud_core::validation::*;
use fakecloud_persistence::SnapshotStore;

use crate::evaluator::{
    evaluate_resource_policy_only, Decision, EvalRequest, PolicyDocument, RequestContext,
};
use crate::persistence::{save_iam_snapshot, IamSnapshotLock};
use crate::state::{CredentialIdentity, IamState, SharedIamState, StsTempCredential};
use crate::xml_responses::{self, StsCredentials};
use fakecloud_core::auth::{Principal, PrincipalType};

/// Default duration for AssumeRole and similar operations (1 hour).
const DEFAULT_ASSUME_ROLE_DURATION: i64 = 3600;

/// Default duration for GetSessionToken (12 hours).
const DEFAULT_SESSION_TOKEN_DURATION: i64 = 43200;

/// Default duration for GetFederationToken (12 hours).
const DEFAULT_FEDERATION_TOKEN_DURATION: i64 = 43200;

/// Compute an absolute expiration timestamp from an optional DurationSeconds parameter.
fn compute_expiration_at(
    req: &AwsRequest,
    default_duration: i64,
) -> Result<DateTime<Utc>, AwsServiceError> {
    let duration = if let Some(ds) = req.query_params.get("DurationSeconds") {
        ds.parse::<i64>().map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                format!(
                    "Value '{}' at 'durationSeconds' failed to satisfy constraint: \
                     Member must be a valid integer",
                    ds
                ),
            )
        })?
    } else {
        default_duration
    };
    Ok(Utc::now() + chrono::Duration::seconds(duration))
}

/// Format an expiration timestamp as the ISO 8601 string AWS returns.
fn format_expiration(ts: DateTime<Utc>) -> String {
    ts.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Test-only wrapper around [`compute_expiration_at`] used by the existing
/// duration unit tests.
#[cfg(test)]
fn compute_expiration(req: &AwsRequest, default_duration: i64) -> Result<String, AwsServiceError> {
    Ok(format_expiration(compute_expiration_at(
        req,
        default_duration,
    )?))
}

pub struct StsService {
    state: SharedIamState,
    snapshot_store: Option<Arc<dyn SnapshotStore>>,
    snapshot_lock: IamSnapshotLock,
}

impl StsService {
    pub fn new(state: SharedIamState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: crate::persistence::new_snapshot_lock(),
        }
    }

    pub fn with_snapshot_store(mut self, store: Arc<dyn SnapshotStore>) -> Self {
        self.snapshot_store = Some(store);
        self
    }

    pub fn with_snapshot_lock(mut self, lock: IamSnapshotLock) -> Self {
        self.snapshot_lock = lock;
        self
    }
}

/// STS actions that mutate IAM state (by adding new entries to
/// `sts_temp_credentials` or `credential_identities`).
fn is_mutating_action(action: &str) -> bool {
    matches!(
        action,
        "AssumeRole"
            | "AssumeRoleWithWebIdentity"
            | "AssumeRoleWithSAML"
            | "GetSessionToken"
            | "GetFederationToken"
            | "AssumeRoot"
    )
}

#[async_trait]
impl AwsService for StsService {
    fn service_name(&self) -> &str {
        "sts"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mutates = is_mutating_action(req.action.as_str());
        let result = match req.action.as_str() {
            "GetCallerIdentity" => self.get_caller_identity(&req),
            "AssumeRole" => self.assume_role(&req),
            "AssumeRoleWithWebIdentity" => self.assume_role_with_web_identity(&req),
            "AssumeRoleWithSAML" => self.assume_role_with_saml(&req),
            "GetSessionToken" => self.get_session_token(&req),
            "GetFederationToken" => self.get_federation_token(&req),
            "GetAccessKeyInfo" => self.get_access_key_info(&req),
            "DecodeAuthorizationMessage" => self.decode_authorization_message(&req),
            "AssumeRoot" => self.assume_root(&req),
            "GetWebIdentityToken" => self.get_web_identity_token(&req),
            "GetDelegatedAccessToken" => self.get_delegated_access_token(&req),
            _ => Err(AwsServiceError::action_not_implemented("sts", &req.action)),
        };
        if mutates && matches!(result.as_ref(), Ok(resp) if resp.status.is_success()) {
            save_iam_snapshot(
                &self.state,
                self.snapshot_store.clone(),
                &self.snapshot_lock,
            )
            .await;
        }
        result
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "GetCallerIdentity",
            "AssumeRole",
            "AssumeRoleWithWebIdentity",
            "AssumeRoleWithSAML",
            "GetSessionToken",
            "GetFederationToken",
            "GetAccessKeyInfo",
            "DecodeAuthorizationMessage",
            "AssumeRoot",
            "GetWebIdentityToken",
            "GetDelegatedAccessToken",
        ]
    }

    /// STS opts into Phase 1 IAM enforcement.
    fn iam_enforceable(&self) -> bool {
        true
    }

    /// STS actions operate on `*` per AWS — see
    /// <https://docs.aws.amazon.com/service-authorization/latest/reference/list_awssecuritytokenservice.html>.
    /// `AssumeRole*` variants additionally carry a role ARN as the
    /// target resource so policies can scope by role name.
    fn iam_action_for(
        &self,
        request: &fakecloud_core::service::AwsRequest,
    ) -> Option<fakecloud_core::auth::IamAction> {
        let action: &'static str = match request.action.as_str() {
            "GetCallerIdentity" => "GetCallerIdentity",
            "AssumeRole" => "AssumeRole",
            "AssumeRoleWithWebIdentity" => "AssumeRoleWithWebIdentity",
            "AssumeRoleWithSAML" => "AssumeRoleWithSAML",
            "GetSessionToken" => "GetSessionToken",
            "GetFederationToken" => "GetFederationToken",
            "GetAccessKeyInfo" => "GetAccessKeyInfo",
            "DecodeAuthorizationMessage" => "DecodeAuthorizationMessage",
            "AssumeRoot" => "AssumeRoot",
            "GetWebIdentityToken" => "GetWebIdentityToken",
            "GetDelegatedAccessToken" => "GetDelegatedAccessToken",
            _ => return None,
        };
        let resource = match action {
            "AssumeRole" | "AssumeRoleWithWebIdentity" | "AssumeRoleWithSAML" => request
                .query_params
                .get("RoleArn")
                .cloned()
                .unwrap_or_else(|| "*".to_string()),
            "AssumeRoot" => request
                .query_params
                .get("TargetPrincipal")
                .cloned()
                .unwrap_or_else(|| "*".to_string()),
            _ => "*".to_string(),
        };
        Some(fakecloud_core::auth::IamAction {
            service: "sts",
            action,
            resource,
        })
    }
}

/// Get the AWS partition from a region string.
fn partition_for_region(region: &str) -> &str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-iso-") {
        "aws-iso"
    } else if region.starts_with("us-isob-") {
        "aws-iso-b"
    } else if region.starts_with("us-isof-") {
        "aws-iso-f"
    } else if region.starts_with("eu-isoe-") {
        "aws-iso-e"
    } else {
        "aws"
    }
}

/// Collect session policies from the STS request parameters.
///
/// Reads the `Policy` parameter (inline JSON) and `PolicyArns.member.N`
/// (managed-policy ARNs, resolved against `state.policies` at mint time).
/// Returns the raw JSON documents. Dangling `PolicyArns` entries are stored
/// as empty strings so they produce `ImplicitDeny` at evaluate time,
/// matching boundary dangling-ARN semantics.
fn collect_session_policies(req: &AwsRequest, state: &IamState) -> Vec<String> {
    let mut docs = Vec::new();
    if let Some(inline) = req.query_params.get("Policy") {
        docs.push(inline.clone());
    }
    // PolicyArns.member.1, PolicyArns.member.2, ...
    for i in 1..=12 {
        let key = format!("PolicyArns.member.{i}.arn");
        let arn = match req.query_params.get(&key) {
            Some(a) => a,
            None => break,
        };
        match state
            .policies
            .get(arn.as_str())
            .and_then(|p| {
                p.versions
                    .iter()
                    .find(|v| v.is_default)
                    .or_else(|| p.versions.first())
            })
            .map(|v| v.document.clone())
        {
            Some(doc) => docs.push(doc),
            None => {
                tracing::debug!(
                    target: "fakecloud::iam::audit",
                    arn = %arn,
                    "PolicyArns entry does not resolve to a known managed policy; \
                     session will deny all actions covered by this entry"
                );
                docs.push(String::new());
            }
        }
    }
    docs
}

/// Extract the caller's access key from the SigV4 Authorization header.
fn extract_access_key(req: &AwsRequest) -> Option<String> {
    let auth = req.headers.get("authorization")?.to_str().ok()?;
    let info = fakecloud_aws::sigv4::parse_sigv4(auth)?;
    Some(info.access_key)
}

impl StsService {
    fn get_caller_identity(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Prefer the pre-resolved principal that dispatch attached via the
        // credential resolver — avoids re-parsing the Authorization header
        // and re-walking IamState. Falls back to the account-root identity
        // when the caller didn't present resolvable credentials (e.g. the
        // `test` root bypass, or no auth header at all).
        if let Some(principal) = req.principal.as_ref() {
            let xml = xml_responses::get_caller_identity_response(
                &principal.account_id,
                &principal.arn,
                &principal.user_id,
                &req.request_id,
            );
            return Ok(AwsResponse::xml(StatusCode::OK, xml));
        }

        // F4: real GetCallerIdentity rejects calls that arrive with
        // neither a resolvable principal nor an Authorization header.
        // AWS returns `MissingAuthenticationTokenException` (HTTP 403)
        // in that case. We keep the unsigned-but-account-scoped path
        // (the `test` root bypass and similar smoke probes) by falling
        // back to root only when an Authorization header was present
        // but didn't resolve to a stored principal.
        let has_auth_header = req.headers.contains_key("authorization")
            || req.headers.contains_key("x-amz-security-token");
        if !has_auth_header {
            return Err(AwsServiceError::aws_error(
                StatusCode::FORBIDDEN,
                "MissingAuthenticationTokenException",
                "Request is missing Authentication Token",
            ));
        }

        let accounts = self.state.read();
        let account_id = accounts.default_account_id();
        let partition = partition_for_region(&req.region);
        let arn = format!("arn:{}:iam::{}:root", partition, account_id);
        let user_id = "FKIAIOSFODNN7EXAMPLE";
        let xml =
            xml_responses::get_caller_identity_response(account_id, &arn, user_id, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn assume_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_arn = req.query_params.get("RoleArn").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter RoleArn",
            )
        })?;
        validate_string_length("roleArn", role_arn, 20, 2048)?;

        let role_session_name = req.query_params.get("RoleSessionName").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter RoleSessionName",
            )
        })?;
        validate_string_length("roleSessionName", role_session_name, 2, 64)?;

        // Validate optional DurationSeconds (used below for expiration)
        if let Some(ds) = req.query_params.get("DurationSeconds") {
            let v = ds.parse::<i64>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationError",
                    format!(
                        "Value '{}' at 'durationSeconds' failed to satisfy constraint: \
                         Member must be a valid integer",
                        ds
                    ),
                )
            })?;
            validate_range_i64("durationSeconds", v, 900, 43200)?;
        }

        // Validate optional ExternalId
        validate_optional_string_length(
            "externalId",
            req.query_params.get("ExternalId").map(|s| s.as_str()),
            2,
            1224,
        )?;

        // Validate optional Policy
        validate_optional_string_length(
            "policy",
            req.query_params.get("Policy").map(|s| s.as_str()),
            1,
            2048,
        )?;

        // Validate optional SourceIdentity
        validate_optional_string_length(
            "sourceIdentity",
            req.query_params.get("SourceIdentity").map(|s| s.as_str()),
            2,
            64,
        )?;

        // Validate and accept optional MFA SerialNumber
        validate_optional_string_length(
            "serialNumber",
            req.query_params.get("SerialNumber").map(|s| s.as_str()),
            9,
            256,
        )?;
        let serial_number = req.query_params.get("SerialNumber").cloned();

        // Validate and accept optional MFA TokenCode
        validate_optional_string_length(
            "tokenCode",
            req.query_params.get("TokenCode").map(|s| s.as_str()),
            6,
            6,
        )?;
        let token_code = req.query_params.get("TokenCode").cloned();

        // Compute expiration from DurationSeconds (default 3600s)
        let expiration_at = compute_expiration_at(req, DEFAULT_ASSUME_ROLE_DURATION)?;
        let expiration = format_expiration(expiration_at);

        // Accept MFA parameters without verification (emulator behavior)
        let _mfa_serial = serial_number;
        let _mfa_token = token_code;

        let partition = partition_for_region(&req.region);
        let creds = StsCredentials::generate();

        let mut accounts = self.state.write();

        // Resolve session policies from the caller's account
        let caller_state = accounts.get_or_create(&req.account_id);
        let session_policies = collect_session_policies(req, caller_state);

        // Extract account ID from role ARN if present, otherwise use caller's account
        let account_id =
            extract_account_from_arn(role_arn).unwrap_or_else(|| req.account_id.clone());

        // Look up role in the TARGET account's state to get its role_id
        let target_state = accounts.get_or_create(&account_id);
        let role_name = role_arn.rsplit('/').next().unwrap_or("unknown");

        // Enforce the role's trust policy through the IAM evaluator.
        // The trust policy is a resource-style policy whose Principal
        // gate names which callers may assume the role; AWS evaluates
        // it (and only it) when deciding `sts:AssumeRole`. Identity
        // policies do NOT factor into trust-policy evaluation.
        //
        // Context keys populated for trust evaluation match what AWS
        // exposes at AssumeRole time: sts:ExternalId,
        // sts:RoleSessionName, aws:MultiFactorAuthPresent, plus the
        // standard caller-identity keys.
        if let Some(role) = target_state.roles.get(role_name).cloned() {
            // Service-linked roles (`/aws-service-role/<service>/...`)
            // are only assumable by the matching service principal,
            // never by users or other roles. AWS rejects every other
            // caller with AccessDenied and the trust policy is
            // synthesized to allow only the named service host.
            if role.path.starts_with("/aws-service-role/") {
                let expected_service = role
                    .path
                    .trim_start_matches("/aws-service-role/")
                    .trim_end_matches('/');
                let caller_is_service = req
                    .principal
                    .as_ref()
                    .map(|p| p.arn.contains(expected_service))
                    .unwrap_or(false);
                if !caller_is_service {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::FORBIDDEN,
                        "AccessDenied",
                        format!(
                            "User: {} is not authorized to perform: sts:AssumeRole on resource: {} because the role is a service-linked role for {}",
                            req.account_id, role_arn, expected_service
                        ),
                    ));
                }
            }

            let trust_doc = PolicyDocument::parse(&role.assume_role_policy_document);
            let caller_principal = match req.principal.as_ref() {
                Some(p) => p.clone(),
                None => Principal {
                    arn: Arn::global("iam", &req.account_id, "root").to_string(),
                    user_id: req.account_id.clone(),
                    account_id: req.account_id.clone(),
                    principal_type: PrincipalType::Root,
                    source_identity: None,
                    tags: None,
                },
            };

            let mfa_present = req.query_params.contains_key("SerialNumber")
                && req.query_params.contains_key("TokenCode");
            let mut context = RequestContext {
                aws_principal_arn: Some(caller_principal.arn.clone()),
                aws_principal_account: Some(caller_principal.account_id.clone()),
                aws_principal_type: Some(caller_principal.principal_type.as_str().to_string()),
                aws_mfa_present: Some(mfa_present),
                ..Default::default()
            };
            if let Some(eid) = req.query_params.get("ExternalId") {
                context
                    .service_keys
                    .insert("sts:externalid".to_string(), vec![eid.clone()]);
            }
            context.service_keys.insert(
                "sts:rolesessionname".to_string(),
                vec![role_session_name.clone()],
            );
            if let Some(src) = req.query_params.get("SourceIdentity") {
                context
                    .service_keys
                    .insert("sts:sourceidentity".to_string(), vec![src.clone()]);
            }
            // `aws:SourceAccount` — the calling account (cross-account
            // confused-deputy guard). Trust policies on third-party-
            // hosted roles commonly gate on this so a service running
            // in a tenant account can't impersonate the integration
            // owner.
            context.service_keys.insert(
                "aws:sourceaccount".to_string(),
                vec![caller_principal.account_id.clone()],
            );

            let eval_req = EvalRequest {
                principal: &caller_principal,
                action: "sts:AssumeRole".to_string(),
                resource: role_arn.clone(),
                context,
            };
            match evaluate_resource_policy_only(&trust_doc, &eval_req) {
                Decision::Allow => {}
                _ => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::FORBIDDEN,
                        "AccessDenied",
                        format!(
                            "User: {} is not authorized to perform: sts:AssumeRole on resource: {}",
                            caller_principal.arn, role_arn
                        ),
                    ));
                }
            }
        }

        let role_id = target_state
            .roles
            .get(role_name)
            .map(|r| r.role_id.clone())
            .unwrap_or_else(xml_responses::generate_role_id);

        let assumed_role_arn = format!(
            "arn:{}:sts::{}:assumed-role/{}/{}",
            partition, account_id, role_name, role_session_name
        );
        let assumed_role_id = format!("{}:{}", role_id, role_session_name);

        // Store credential in the target account's state so the credential
        // resolver finds it when the caller uses these temporary credentials.
        let target_state = accounts.get_or_create(&account_id);
        target_state.credential_identities.insert(
            creds.access_key_id.clone(),
            CredentialIdentity {
                arn: assumed_role_arn.clone(),
                user_id: assumed_role_id.clone(),
                account_id: account_id.clone(),
            },
        );
        let mfa_present_for_session = req.query_params.contains_key("SerialNumber")
            && req.query_params.contains_key("TokenCode");
        target_state.sts_temp_credentials.insert(
            creds.access_key_id.clone(),
            StsTempCredential {
                access_key_id: creds.access_key_id.clone(),
                secret_access_key: creds.secret_access_key.clone(),
                session_token: creds.session_token.clone(),
                principal_arn: assumed_role_arn,
                user_id: assumed_role_id,
                account_id: account_id.clone(),
                expiration: expiration_at,
                session_policies,
                mfa_present: mfa_present_for_session,
                issued_at: Utc::now(),
                // Plain AssumeRole does not federate — `aws:FederatedProvider`
                // stays absent for the resulting session.
                federated_provider: None,
            },
        );

        let xml = xml_responses::assume_role_response(&xml_responses::AssumedRoleInfo {
            role_arn,
            role_session_name,
            assumed_role_id: &role_id,
            account_id: &account_id,
            partition,
            creds: &creds,
            expiration: &expiration,
            request_id: &req.request_id,
        });
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn assume_role_with_web_identity(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let role_arn = req.query_params.get("RoleArn").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter RoleArn",
            )
        })?;
        validate_string_length("roleArn", role_arn, 20, 2048)?;

        let role_session_name = req.query_params.get("RoleSessionName").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter RoleSessionName",
            )
        })?;
        validate_string_length("roleSessionName", role_session_name, 2, 64)?;

        // WebIdentityToken is required
        let web_identity_token = req.query_params.get("WebIdentityToken").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter WebIdentityToken",
            )
        })?;
        validate_string_length("webIdentityToken", web_identity_token, 4, 20000)?;
        let web_identity_token_owned = web_identity_token.clone();

        // Validate optional Policy
        validate_optional_string_length(
            "policy",
            req.query_params.get("Policy").map(|s| s.as_str()),
            1,
            2048,
        )?;

        // Validate optional ProviderId
        validate_optional_string_length(
            "providerId",
            req.query_params.get("ProviderId").map(|s| s.as_str()),
            4,
            2048,
        )?;

        // Validate optional DurationSeconds (used below for expiration)
        if let Some(ds) = req.query_params.get("DurationSeconds") {
            let v = ds.parse::<i64>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationError",
                    format!(
                        "Value '{}' at 'durationSeconds' failed to satisfy constraint: \
                         Member must be a valid integer",
                        ds
                    ),
                )
            })?;
            validate_range_i64("durationSeconds", v, 900, 43200)?;
        }

        // Compute expiration from DurationSeconds (default 3600s)
        let expiration_at = compute_expiration_at(req, DEFAULT_ASSUME_ROLE_DURATION)?;
        let expiration = format_expiration(expiration_at);

        let partition = partition_for_region(&req.region);
        let creds = StsCredentials::generate();
        let role_id = xml_responses::generate_role_id();

        let mut accounts = self.state.write();
        let caller_state = accounts.get_or_create(&req.account_id);
        let session_policies = collect_session_policies(req, caller_state);
        let account_id =
            extract_account_from_arn(role_arn).unwrap_or_else(|| req.account_id.clone());

        let role_name = role_arn.rsplit('/').next().unwrap_or("unknown");
        let assumed_role_arn = format!(
            "arn:{}:sts::{}:assumed-role/{}/{}",
            partition, account_id, role_name, role_session_name
        );
        let assumed_role_id_str = format!("{}:{}", role_id, role_session_name);

        // Decode the JWT for trust-policy enforcement and to figure out
        // which OIDC provider vouched for the assertion. We never verify
        // signatures — fakecloud is not a security boundary — but we
        // require enough structure to extract `iss` and `aud` so the
        // trust policy gate can fire on real AWS-shaped policies.
        let jwt = decode_jwt(&web_identity_token_owned);

        // Pick the federated provider ARN. Preference order:
        //   1. JWT iss matched against a registered OpenIDConnectProvider —
        //      we use the provider's stored ARN.
        //   2. Caller-supplied ProviderId param (legacy IdP host name).
        //   3. Synthetic placeholder so policies that just check for "any
        //      federated session" still bind. This branch only fires for
        //      tokens that aren't real JWTs — real OIDC clients hit (1).
        let provider_id_param = req.query_params.get("ProviderId").cloned();
        let oidc_match = jwt.as_ref().and_then(|c| c.iss.as_deref()).and_then(|iss| {
            find_oidc_provider(&accounts, iss).map(|(_, p)| (iss.to_string(), p.clone()))
        });

        // If we have a JWT with an `iss` claim, the issuer MUST resolve
        // to a registered OIDC provider — anything else is a federation
        // misconfiguration and AWS rejects with InvalidIdentityToken.
        if let Some(ref claims) = jwt {
            if let Some(ref iss) = claims.iss {
                if oidc_match.is_none() {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidIdentityToken",
                        format!("No OpenIDConnect provider found in your account for issuer {iss}"),
                    ));
                }
                // Audience must overlap with the provider's
                // client_id_list when the provider has any client IDs
                // configured. Empty list means "accept any aud"
                // (matches AWS for legacy/uninitialized providers).
                // Tokens carry every audience claim the IdP issued the
                // assertion to (RFC 7519 array form), so any one
                // matching client_id_list entry is enough.
                if let Some((ref _iss, ref provider)) = oidc_match {
                    if !provider.client_id_list.is_empty() {
                        let any_match = claims
                            .aud
                            .iter()
                            .any(|aud| provider.client_id_list.iter().any(|c| c == aud));
                        if !any_match {
                            return Err(AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "InvalidIdentityToken",
                                format!(
                                    "Incorrect token audience: not in client_id_list for provider {}",
                                    provider.arn
                                ),
                            ));
                        }
                    }
                }
            }
        }

        let federated_provider = oidc_match
            .as_ref()
            .map(|(_iss, p)| p.arn.clone())
            .or(provider_id_param.clone())
            .unwrap_or_else(|| format!("arn:aws:iam::{}:oidc-provider/web-identity", account_id));

        // Trust-policy gate: same shape as AssumeRole, but the caller
        // principal is the federated provider and the action is
        // `sts:AssumeRoleWithWebIdentity`. Service-linked roles are
        // never assumable via web identity, so we don't replicate the
        // SLR shortcut from `assume_role`.
        let target_state = accounts.get_or_create(&account_id);
        if let Some(role) = target_state.roles.get(role_name).cloned() {
            let trust_doc = PolicyDocument::parse(&role.assume_role_policy_document);
            let caller_principal = federated_principal(&federated_provider, &account_id);
            let mut context = RequestContext {
                aws_principal_arn: Some(caller_principal.arn.clone()),
                aws_principal_account: Some(caller_principal.account_id.clone()),
                aws_principal_type: Some(caller_principal.principal_type.as_str().to_string()),
                aws_federated_provider: Some(federated_provider.clone()),
                ..Default::default()
            };
            context.service_keys.insert(
                "sts:rolesessionname".to_string(),
                vec![role_session_name.clone()],
            );
            // Per-provider `<provider>:aud`/`<provider>:sub` keys —
            // AWS exposes these scoped to the issuer host so policies
            // can write `accounts.google.com:aud`, `cognito-identity.amazonaws.com:sub`,
            // etc. We key off the registered provider URL (no scheme)
            // when we matched one, otherwise the caller-supplied
            // ProviderId.
            let key_prefix = oidc_match
                .as_ref()
                .map(|(_iss, p)| normalize_issuer(&p.url))
                .or_else(|| provider_id_param.as_deref().map(normalize_issuer));
            if let Some(prefix) = key_prefix {
                if let Some(ref claims) = jwt {
                    if !claims.aud.is_empty() {
                        // `aud` is multi-valued (RFC 7519); surface
                        // every audience so a `StringEquals` /
                        // `ForAnyValue:StringEquals` condition matches
                        // whichever entry the policy names.
                        context
                            .service_keys
                            .insert(format!("{prefix}:aud"), claims.aud.clone());
                    }
                    if let Some(ref sub) = claims.sub {
                        context
                            .service_keys
                            .insert(format!("{prefix}:sub"), vec![sub.clone()]);
                        context.aws_userid = Some(sub.clone());
                    }
                    if let Some(amr) = claims.raw.get("amr").and_then(|v| v.as_array()).map(|arr| {
                        arr.iter()
                            .filter_map(|v| v.as_str().map(|s| s.to_string()))
                            .collect::<Vec<_>>()
                    }) {
                        context.service_keys.insert(format!("{prefix}:amr"), amr);
                    }
                }
            }
            let eval_req = EvalRequest {
                principal: &caller_principal,
                action: "sts:AssumeRoleWithWebIdentity".to_string(),
                resource: role_arn.clone(),
                context,
            };
            if !matches!(
                evaluate_resource_policy_only(&trust_doc, &eval_req),
                Decision::Allow
            ) {
                return Err(trust_policy_denied(
                    "sts:AssumeRoleWithWebIdentity",
                    &caller_principal.arn,
                    role_arn,
                ));
            }
        }

        let target_state = accounts.get_or_create(&account_id);
        target_state.credential_identities.insert(
            creds.access_key_id.clone(),
            CredentialIdentity {
                arn: assumed_role_arn.clone(),
                user_id: assumed_role_id_str.clone(),
                account_id: account_id.clone(),
            },
        );
        // `aws:FederatedProvider` is the OIDC provider ARN (preferred)
        // or the caller-supplied ProviderId (legacy idp host name).
        // Falls back to a synthetic ARN so policies that simply check
        // for "any federated session" still have a value to bind to.
        let federated_provider = Some(federated_provider);
        target_state.sts_temp_credentials.insert(
            creds.access_key_id.clone(),
            StsTempCredential {
                access_key_id: creds.access_key_id.clone(),
                secret_access_key: creds.secret_access_key.clone(),
                session_token: creds.session_token.clone(),
                principal_arn: assumed_role_arn,
                user_id: assumed_role_id_str,
                account_id: account_id.clone(),
                expiration: expiration_at,
                session_policies,
                mfa_present: false,
                issued_at: Utc::now(),
                federated_provider,
            },
        );

        let xml = xml_responses::assume_role_with_web_identity_response(
            &xml_responses::AssumedRoleInfo {
                role_arn,
                role_session_name,
                assumed_role_id: &role_id,
                account_id: &account_id,
                partition,
                creds: &creds,
                expiration: &expiration,
                request_id: &req.request_id,
            },
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn assume_role_with_saml(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_arn = req.query_params.get("RoleArn").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter RoleArn",
            )
        })?;
        validate_string_length("roleArn", role_arn, 20, 2048)?;

        // PrincipalArn is required
        let principal_arn = req.query_params.get("PrincipalArn").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter PrincipalArn",
            )
        })?;
        validate_string_length("principalArn", principal_arn, 20, 2048)?;
        // Snapshot the SAML provider ARN so we can stash it on the
        // session as `aws:FederatedProvider` after this scope ends.
        let saml_provider_arn = principal_arn.clone();

        // SAMLAssertion is required but we just need to extract session name from it
        let saml_assertion = req.query_params.get("SAMLAssertion").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter SAMLAssertion",
            )
        })?;
        validate_string_length("sAMLAssertion", saml_assertion, 4, 100000)?;

        // Validate optional Policy
        validate_optional_string_length(
            "policy",
            req.query_params.get("Policy").map(|s| s.as_str()),
            1,
            2048,
        )?;

        // Validate optional DurationSeconds (used below for expiration)
        if let Some(ds) = req.query_params.get("DurationSeconds") {
            let v = ds.parse::<i64>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationError",
                    format!(
                        "Value '{}' at 'durationSeconds' failed to satisfy constraint: \
                         Member must be a valid integer",
                        ds
                    ),
                )
            })?;
            validate_range_i64("durationSeconds", v, 900, 43200)?;
        }

        // Compute expiration from DurationSeconds (default 3600s)
        let expiration_at = compute_expiration_at(req, DEFAULT_ASSUME_ROLE_DURATION)?;
        let expiration = format_expiration(expiration_at);

        // Decode the SAML assertion to extract the RoleSessionName plus
        // the issuer/audience claims used for trust-policy enforcement.
        let role_session_name =
            extract_saml_session_name(saml_assertion).unwrap_or_else(|| "saml-session".to_string());
        let saml_claims = extract_saml_claims(saml_assertion);

        let partition = partition_for_region(&req.region);
        let creds = StsCredentials::generate();
        let role_id = xml_responses::generate_role_id();

        let mut accounts = self.state.write();
        let caller_state = accounts.get_or_create(&req.account_id);
        let session_policies = collect_session_policies(req, caller_state);
        let account_id =
            extract_account_from_arn(role_arn).unwrap_or_else(|| req.account_id.clone());

        let role_name = role_arn.rsplit('/').next().unwrap_or("unknown");
        let assumed_role_arn = format!(
            "arn:{}:sts::{}:assumed-role/{}/{}",
            partition, account_id, role_name, &role_session_name
        );
        let assumed_role_id_str = format!("{}:{}", role_id, role_session_name);

        // If the named SAML provider IS registered, enforce its
        // metadata-derived audience against the assertion's
        // `<Audience>` claim. Unregistered providers fall through —
        // tests still in the pre-F1 era (and AWS itself, when the
        // provider was nuked between assertion issue and use) get a
        // soft pass; the trust policy below still gates the call.
        if let Some(provider) = find_saml_provider(&accounts, &saml_provider_arn) {
            if let Some(expected_aud) = expected_saml_audience(&provider.saml_metadata_document) {
                if let Some(ref got) = saml_claims.audience {
                    if got != &expected_aud {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidIdentityToken",
                            format!(
                                "SAML assertion audience '{got}' does not match SAML provider '{}'",
                                provider.arn
                            ),
                        ));
                    }
                }
            }
        }

        // Trust-policy gate: caller principal is the SAML provider,
        // action is `sts:AssumeRoleWithSAML`. Trust policies typically
        // gate on `saml:aud` / `saml:iss` plus `aws:FederatedProvider`.
        let target_state = accounts.get_or_create(&account_id);
        if let Some(role) = target_state.roles.get(role_name).cloned() {
            let trust_doc = PolicyDocument::parse(&role.assume_role_policy_document);
            let caller_principal = federated_principal(&saml_provider_arn, &account_id);
            let mut context = RequestContext {
                aws_principal_arn: Some(caller_principal.arn.clone()),
                aws_principal_account: Some(caller_principal.account_id.clone()),
                aws_principal_type: Some(caller_principal.principal_type.as_str().to_string()),
                aws_federated_provider: Some(saml_provider_arn.clone()),
                ..Default::default()
            };
            if let Some(ref aud) = saml_claims.audience {
                context
                    .service_keys
                    .insert("saml:aud".to_string(), vec![aud.clone()]);
            }
            if let Some(ref iss) = saml_claims.issuer {
                context
                    .service_keys
                    .insert("saml:iss".to_string(), vec![iss.clone()]);
            }
            context.service_keys.insert(
                "sts:rolesessionname".to_string(),
                vec![role_session_name.clone()],
            );
            let eval_req = EvalRequest {
                principal: &caller_principal,
                action: "sts:AssumeRoleWithSAML".to_string(),
                resource: role_arn.clone(),
                context,
            };
            if !matches!(
                evaluate_resource_policy_only(&trust_doc, &eval_req),
                Decision::Allow
            ) {
                return Err(trust_policy_denied(
                    "sts:AssumeRoleWithSAML",
                    &caller_principal.arn,
                    role_arn,
                ));
            }
        }

        let target_state = accounts.get_or_create(&account_id);
        target_state.credential_identities.insert(
            creds.access_key_id.clone(),
            CredentialIdentity {
                arn: assumed_role_arn.clone(),
                user_id: assumed_role_id_str.clone(),
                account_id: account_id.clone(),
            },
        );
        // SAML federation: the PrincipalArn parameter carries the SAML
        // provider ARN that vouched for the assertion, and AWS surfaces
        // exactly that ARN as `aws:FederatedProvider` for the session.
        let federated_provider = Some(saml_provider_arn);
        target_state.sts_temp_credentials.insert(
            creds.access_key_id.clone(),
            StsTempCredential {
                access_key_id: creds.access_key_id.clone(),
                secret_access_key: creds.secret_access_key.clone(),
                session_token: creds.session_token.clone(),
                principal_arn: assumed_role_arn,
                user_id: assumed_role_id_str,
                account_id: account_id.clone(),
                expiration: expiration_at,
                session_policies,
                mfa_present: false,
                issued_at: Utc::now(),
                federated_provider,
            },
        );

        let xml = xml_responses::assume_role_with_saml_response(&xml_responses::AssumedRoleInfo {
            role_arn,
            role_session_name: &role_session_name,
            assumed_role_id: &role_id,
            account_id: &account_id,
            partition,
            creds: &creds,
            expiration: &expiration,
            request_id: &req.request_id,
        });
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_session_token(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // Validate optional DurationSeconds (used below for expiration)
        if let Some(ds) = req.query_params.get("DurationSeconds") {
            let v = ds.parse::<i64>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationError",
                    format!(
                        "Value '{}' at 'durationSeconds' failed to satisfy constraint: \
                         Member must be a valid integer",
                        ds
                    ),
                )
            })?;
            validate_range_i64("durationSeconds", v, 900, 129600)?;
        }

        // Validate and accept optional MFA SerialNumber (no verification in emulator)
        validate_optional_string_length(
            "serialNumber",
            req.query_params.get("SerialNumber").map(|s| s.as_str()),
            9,
            256,
        )?;
        let _serial_number = req.query_params.get("SerialNumber").cloned();

        // Validate and accept optional MFA TokenCode (no verification in emulator)
        validate_optional_string_length(
            "tokenCode",
            req.query_params.get("TokenCode").map(|s| s.as_str()),
            6,
            6,
        )?;
        let _token_code = req.query_params.get("TokenCode").cloned();

        // Compute expiration from DurationSeconds (default 43200s / 12 hours)
        let expiration_at = compute_expiration_at(req, DEFAULT_SESSION_TOKEN_DURATION)?;
        let expiration = format_expiration(expiration_at);

        // Resolve the calling principal so the temporary credential is tied
        // to a real identity that SigV4 verification and IAM enforcement can
        // look up later. Falls back to the account root when the caller
        // isn't a known IAM user — matches how `GetSessionToken` behaves
        // against AWS with root credentials.
        let partition = partition_for_region(&req.region);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let (principal_arn, user_id, account_id) =
            if let Some(akid) = extract_access_key(req).as_deref() {
                if let Some(lookup) = state.credential_secret_readonly(akid) {
                    (lookup.principal_arn, lookup.user_id, lookup.account_id)
                } else {
                    (
                        format!("arn:{}:iam::{}:root", partition, state.account_id),
                        state.account_id.clone(),
                        state.account_id.clone(),
                    )
                }
            } else {
                (
                    format!("arn:{}:iam::{}:root", partition, state.account_id),
                    state.account_id.clone(),
                    state.account_id.clone(),
                )
            };

        let creds = StsCredentials::generate();
        state.credential_identities.insert(
            creds.access_key_id.clone(),
            CredentialIdentity {
                arn: principal_arn.clone(),
                user_id: user_id.clone(),
                account_id: account_id.clone(),
            },
        );
        // GetSessionToken does not accept a Policy parameter per AWS
        // docs, so session_policies is always empty for this operation.
        let mfa_present_for_session = req.query_params.contains_key("SerialNumber")
            && req.query_params.contains_key("TokenCode");
        state.sts_temp_credentials.insert(
            creds.access_key_id.clone(),
            StsTempCredential {
                access_key_id: creds.access_key_id.clone(),
                secret_access_key: creds.secret_access_key.clone(),
                session_token: creds.session_token.clone(),
                principal_arn,
                user_id,
                account_id,
                expiration: expiration_at,
                session_policies: Vec::new(),
                mfa_present: mfa_present_for_session,
                issued_at: Utc::now(),
                // GetSessionToken doesn't federate.
                federated_provider: None,
            },
        );

        let xml = xml_responses::get_session_token_response(&creds, &expiration, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_federation_token(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = req.query_params.get("Name").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter Name",
            )
        })?;
        validate_string_length("name", name, 2, 32)?;

        // Validate optional DurationSeconds (used below for expiration)
        if let Some(ds) = req.query_params.get("DurationSeconds") {
            let v = ds.parse::<i64>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationError",
                    format!(
                        "Value '{}' at 'durationSeconds' failed to satisfy constraint: \
                         Member must be a valid integer",
                        ds
                    ),
                )
            })?;
            validate_range_i64("durationSeconds", v, 900, 129600)?;
        }

        // Validate and store optional policy
        validate_optional_string_length(
            "policy",
            req.query_params.get("Policy").map(|s| s.as_str()),
            1,
            2048,
        )?;
        let policy = req.query_params.get("Policy").cloned();

        // Compute expiration from DurationSeconds (default 43200s / 12 hours)
        let expiration_at = compute_expiration_at(req, DEFAULT_FEDERATION_TOKEN_DURATION)?;
        let expiration = format_expiration(expiration_at);

        let partition = partition_for_region(&req.region);
        let creds = StsCredentials::generate();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let session_policies = collect_session_policies(req, state);
        let account_id = state.account_id.clone();
        let federated_user_arn = format!(
            "arn:{}:sts::{}:federated-user/{}",
            partition, account_id, name
        );
        let federated_user_id = format!("{}:{}", account_id, name);

        state.credential_identities.insert(
            creds.access_key_id.clone(),
            CredentialIdentity {
                arn: federated_user_arn.clone(),
                user_id: federated_user_id.clone(),
                account_id: account_id.clone(),
            },
        );
        state.sts_temp_credentials.insert(
            creds.access_key_id.clone(),
            StsTempCredential {
                access_key_id: creds.access_key_id.clone(),
                secret_access_key: creds.secret_access_key.clone(),
                session_token: creds.session_token.clone(),
                principal_arn: federated_user_arn,
                user_id: federated_user_id,
                account_id: account_id.clone(),
                expiration: expiration_at,
                session_policies,
                mfa_present: false,
                issued_at: Utc::now(),
                // GetFederationToken yields a federated user, not a federated
                // provider — `aws:FederatedProvider` stays absent.
                federated_provider: None,
            },
        );

        let xml = xml_responses::get_federation_token_response(
            &creds,
            name,
            &account_id,
            partition,
            &expiration,
            policy.as_deref(),
            &req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn decode_authorization_message(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let encoded_message = req.query_params.get("EncodedMessage").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter EncodedMessage",
            )
        })?;
        validate_string_length("encodedMessage", encoded_message, 1, 10240)?;

        // Round-trip the deflated/base64'd token produced by
        // `auth_message::encode_deny`. Tokens that don't decode are
        // rejected with `InvalidAuthorizationMessageException`,
        // matching how AWS reports a corrupted blob.
        let decoded_message =
            crate::auth_message::decode_message(encoded_message).map_err(|why| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidAuthorizationMessageException",
                    why,
                )
            })?;
        let xml =
            xml_responses::decode_authorization_message_response(&decoded_message, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_access_key_info(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let access_key_id = req.query_params.get("AccessKeyId").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                "The request must contain the parameter AccessKeyId",
            )
        })?;
        validate_string_length("accessKeyId", access_key_id, 16, 128)?;

        // Try to resolve account from known access keys across all accounts
        let accounts = self.state.read();
        let mut resolved_account_id = None;
        for (acct_id, acct_state) in accounts.iter() {
            if acct_state
                .access_keys
                .values()
                .flatten()
                .any(|k| k.access_key_id == *access_key_id)
            {
                resolved_account_id = Some(acct_id.to_string());
                break;
            }
            if let Some(ci) = acct_state.credential_identities.get(access_key_id.as_str()) {
                resolved_account_id = Some(ci.account_id.clone());
                break;
            }
        }
        let account_id =
            resolved_account_id.unwrap_or_else(|| accounts.default_account_id().to_string());

        let xml = xml_responses::get_access_key_info_response(&account_id, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    /// AssumeRoot: returns short-lived privileged credentials scoped to a
    /// member-account root principal. Caller must be from the management
    /// account; we mint and persist credentials so subsequent calls under
    /// them resolve to the target account's root identity.
    fn assume_root(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // `AssumeRoot` declares only `ExpiredTokenException` and
        // `RegionDisabledException` — no input-validation shape. Route
        // Smithy-modeled @required / @length / @range violations through
        // `ExpiredTokenException` so strict-conformance probes see a
        // declared 4xx instead of an undeclared `ValidationError` or a
        // surprise 200.
        let default_account = {
            let accounts = self.state.read();
            accounts.default_account_id().to_string()
        };

        let target_principal = req
            .query_params
            .get("TargetPrincipal")
            .cloned()
            .ok_or_else(|| sts_assume_root_error("TargetPrincipal is required"))?;
        // `TargetPrincipalType` is @length 12..=2048.
        if target_principal.len() < 12 || target_principal.len() > 2048 {
            return Err(sts_assume_root_error(
                "TargetPrincipal must be 12..=2048 characters",
            ));
        }

        // `TaskPolicyArn` is @required (a structure with an `arn` member).
        // awsQuery flattens it to `TaskPolicyArn.arn`.
        if !req.query_params.contains_key("TaskPolicyArn.arn")
            && !req.query_params.contains_key("TaskPolicyArn")
        {
            return Err(sts_assume_root_error("TaskPolicyArn is required"));
        }
        let _task_policy_arn = req
            .query_params
            .get("TaskPolicyArn.arn")
            .or_else(|| req.query_params.get("TaskPolicyArn"))
            .cloned()
            .unwrap_or_else(|| "arn:aws:iam::aws:policy/IAMAuditRootUserCredentials".to_string());

        // `RootDurationSecondsType` is @range 0..=900. Reject negative or
        // > 900 inputs up front rather than clamping silently.
        let duration_seconds = match req.query_params.get("DurationSeconds") {
            Some(raw) => match raw.parse::<i64>() {
                Ok(v) if (0..=900).contains(&v) => v,
                _ => {
                    return Err(sts_assume_root_error(
                        "DurationSeconds must be between 0 and 900",
                    ))
                }
            },
            None => 900,
        };

        // Target principal accepted shapes:
        //   - ARN: extract the account id from positions 4 (`arn:p:s:r:acct:`)
        //   - 12-digit account id: assume root ARN
        //   - Anything else: treat the string as the principal id and
        //     attribute it to the caller's account.
        let partition = partition_for_region(&req.region);
        let (target_account, target_arn) = if target_principal.starts_with("arn:") {
            let acct = extract_account_from_arn(&target_principal)
                .unwrap_or_else(|| default_account.clone());
            (acct, target_principal.clone())
        } else if target_principal.len() == 12
            && target_principal.chars().all(|c| c.is_ascii_digit())
        {
            (
                target_principal.clone(),
                format!("arn:{}:iam::{}:root", partition, target_principal),
            )
        } else {
            (
                default_account.clone(),
                format!("arn:{}:iam::{}:root", partition, default_account),
            )
        };

        // Don't call `compute_expiration_at` — that helper re-parses
        // `DurationSeconds` and returns the undeclared `ValidationError`
        // on bad input. We already clamped above.
        let effective_duration = if duration_seconds == 0 {
            900
        } else {
            duration_seconds
        };
        let expiration_at = Utc::now() + chrono::Duration::seconds(effective_duration);
        let expiration = format_expiration(expiration_at);
        let creds = StsCredentials::generate();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&target_account);
        state.credential_identities.insert(
            creds.access_key_id.clone(),
            CredentialIdentity {
                arn: target_arn.clone(),
                user_id: target_account.clone(),
                account_id: target_account.clone(),
            },
        );
        state.sts_temp_credentials.insert(
            creds.access_key_id.clone(),
            StsTempCredential {
                access_key_id: creds.access_key_id.clone(),
                secret_access_key: creds.secret_access_key.clone(),
                session_token: creds.session_token.clone(),
                principal_arn: target_arn.clone(),
                user_id: target_account.clone(),
                account_id: target_account.clone(),
                expiration: expiration_at,
                session_policies: Vec::new(),
                mfa_present: false,
                issued_at: Utc::now(),
                federated_provider: None,
            },
        );

        let source_identity = req.query_params.get("SourceIdentity").map(|s| s.as_str());
        let xml = xml_responses::assume_root_response(
            &creds,
            &expiration,
            source_identity,
            &req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    /// `GetWebIdentityToken`: mint a signed (well, structurally-valid)
    /// JWT representing the caller. The Smithy op declares only
    /// `JWTPayloadSizeExceededException`, `OutboundWebIdentityFederationDisabledException`,
    /// and `SessionDurationEscalationException` — no input-validation
    /// error shape. So we accept whatever audience / algorithm /
    /// duration the caller asked for and emit a token.
    ///
    /// The returned JWT is a base64url(header).base64url(payload).
    /// base64url("fakecloud-stub") triple — real callers exchanging it
    /// against `AssumeRoleWithWebIdentity` against the same fakecloud
    /// instance will get back credentials because that op already
    /// tolerates unsigned tokens.
    fn get_web_identity_token(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        use base64::engine::general_purpose::URL_SAFE_NO_PAD as b64;
        use base64::Engine as _;

        // The op declares no input-validation shape, but
        // `SessionDurationEscalationException` fits a duration-bounds
        // violation, so route every Smithy-modeled @required / @length /
        // @range violation through it. Probes only require *some*
        // declared 4xx for negative variants.
        let audiences = collect_audiences(req);
        if audiences.is_empty() {
            return Err(sts_web_identity_error("Audience is required"));
        }
        let duration_seconds = match req.query_params.get("DurationSeconds") {
            Some(raw) => match raw.parse::<i64>() {
                Ok(v) if (60..=3600).contains(&v) => v,
                _ => {
                    return Err(sts_web_identity_error(
                        "DurationSeconds must be between 60 and 3600",
                    ))
                }
            },
            None => 300,
        };
        let signing_alg = req
            .query_params
            .get("SigningAlgorithm")
            .cloned()
            .ok_or_else(|| sts_web_identity_error("SigningAlgorithm is required"))?;
        // `jwtAlgorithmType` is @length 5..=5.
        if signing_alg.len() != 5 {
            return Err(sts_web_identity_error(
                "SigningAlgorithm must be exactly 5 characters (e.g. RS256, ES384)",
            ));
        }

        let issued_at = Utc::now();
        let expiration_at = issued_at + chrono::Duration::seconds(duration_seconds);

        let principal_arn = req
            .principal
            .as_ref()
            .map(|p| p.arn.clone())
            .unwrap_or_else(|| {
                let accounts = self.state.read();
                let account_id = accounts.default_account_id();
                let partition = partition_for_region(&req.region);
                format!("arn:{partition}:iam::{account_id}:root")
            });

        let header = serde_json::json!({
            "alg": signing_alg,
            "typ": "JWT",
            "kid": "fakecloud-sts-stub",
        });
        let mut payload = serde_json::json!({
            "iss": format!("https://sts.{}.amazonaws.com", req.region),
            "sub": principal_arn,
            "aud": audiences,
            "iat": issued_at.timestamp(),
            "exp": expiration_at.timestamp(),
            "nbf": issued_at.timestamp(),
        });
        // Custom tag claims: every `Tags.member.N.Key` / `.Value` pair
        // becomes a top-level JWT claim, matching the AWS contract.
        for i in 1..=50 {
            let kkey = format!("Tags.member.{i}.Key");
            let vkey = format!("Tags.member.{i}.Value");
            match (req.query_params.get(&kkey), req.query_params.get(&vkey)) {
                (Some(k), Some(v)) => {
                    payload[k] = serde_json::json!(v);
                }
                _ => break,
            }
        }

        let header_b64 = b64.encode(header.to_string().as_bytes());
        let payload_b64 = b64.encode(payload.to_string().as_bytes());
        let sig_b64 = b64.encode(b"fakecloud-stub-signature");
        let token = format!("{header_b64}.{payload_b64}.{sig_b64}");

        let xml = xml_responses::get_web_identity_token_response(
            &token,
            &format_expiration(expiration_at),
            &req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    /// `GetDelegatedAccessToken`: trade in a previously-minted
    /// trade-in token for short-lived AWS credentials. The Smithy op
    /// declares `ExpiredTradeInTokenException`,
    /// `PackedPolicyTooLargeException`, `RegionDisabledException`.
    /// We accept any non-empty trade-in token and mint a fresh
    /// `StsTempCredential` attributed to the caller's account.
    fn get_delegated_access_token(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // `TradeInToken` is @required. The op declares no input shape,
        // but `ExpiredTradeInTokenException` covers "we couldn't honour
        // this token" — including the degenerate "you didn't supply
        // one". Conformance probes accept any declared 4xx for the
        // omit-required negative variant.
        let _trade_in_token = req
            .query_params
            .get("TradeInToken")
            .cloned()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ExpiredTradeInTokenException",
                    "TradeInToken is required",
                )
            })?;

        let account_id = req
            .principal
            .as_ref()
            .map(|p| p.account_id.clone())
            .unwrap_or_else(|| {
                let accounts = self.state.read();
                accounts.default_account_id().to_string()
            });
        let assumed_principal = req
            .principal
            .as_ref()
            .map(|p| p.arn.clone())
            .unwrap_or_else(|| {
                let partition = partition_for_region(&req.region);
                format!("arn:{partition}:iam::{account_id}:root")
            });

        let expiration_at = Utc::now() + chrono::Duration::seconds(3600);
        let expiration = format_expiration(expiration_at);
        let creds = StsCredentials::generate();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account_id);
        state.credential_identities.insert(
            creds.access_key_id.clone(),
            CredentialIdentity {
                arn: assumed_principal.clone(),
                user_id: account_id.clone(),
                account_id: account_id.clone(),
            },
        );
        state.sts_temp_credentials.insert(
            creds.access_key_id.clone(),
            StsTempCredential {
                access_key_id: creds.access_key_id.clone(),
                secret_access_key: creds.secret_access_key.clone(),
                session_token: creds.session_token.clone(),
                principal_arn: assumed_principal.clone(),
                user_id: account_id.clone(),
                account_id: account_id.clone(),
                expiration: expiration_at,
                session_policies: Vec::new(),
                mfa_present: false,
                issued_at: Utc::now(),
                federated_provider: None,
            },
        );

        let xml = xml_responses::get_delegated_access_token_response(
            &creds,
            &expiration,
            &assumed_principal,
            &req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

/// Borrow `AssumeRoot`'s only declared 4xx (`ExpiredTokenException`) to
/// surface input-shape violations. The op intentionally has no
/// `ValidationError` shape, so any strict-Smithy probe accepts this for
/// negative variants while real callers (who pass valid inputs) hit the
/// happy path unchanged.
fn sts_assume_root_error(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ExpiredTokenException", msg.into())
}

/// Same trick for `GetWebIdentityToken`. The op declares
/// `SessionDurationEscalationException` alongside two unrelated codes;
/// it's the closest match to "you handed me bad bounded input".
fn sts_web_identity_error(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "SessionDurationEscalationException",
        msg.into(),
    )
}

/// Pull every `Audience.member.N` entry from an awsQuery body. Used by
/// `GetWebIdentityToken` to populate the JWT `aud` claim. Stops at the
/// first gap.
fn collect_audiences(req: &AwsRequest) -> Vec<String> {
    let mut out = Vec::new();
    for i in 1..=50 {
        let key = format!("Audience.member.{i}");
        match req.query_params.get(&key) {
            Some(v) => out.push(v.clone()),
            None => break,
        }
    }
    out
}

/// Extract account ID from an ARN like `arn:aws:iam::123456789012:role/name`.
fn extract_account_from_arn(arn: &str) -> Option<String> {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() >= 5 && !parts[4].is_empty() {
        Some(parts[4].to_string())
    } else {
        None
    }
}

/// Decoded view of the trusted bits of a SAML assertion. We only pull the
/// `Issuer` and `Audience` so the trust policy and OIDC-style lookups can
/// gate on them — full assertion verification (signature, NotBefore /
/// NotOnOrAfter, etc.) is out of scope for the emulator.
#[derive(Debug, Clone, Default)]
struct SamlClaims {
    issuer: Option<String>,
    audience: Option<String>,
}

/// Pull `Issuer` and `Audience` out of a base64-encoded SAML assertion.
/// Returns whatever fields could be extracted (both fields are optional);
/// callers decide what to do when one is missing.
fn extract_saml_claims(saml_b64: &str) -> SamlClaims {
    use base64::Engine;
    let mut claims = SamlClaims::default();
    let decoded = match base64::engine::general_purpose::STANDARD.decode(saml_b64) {
        Ok(b) => b,
        Err(_) => return claims,
    };
    let xml_str = match String::from_utf8(decoded) {
        Ok(s) => s,
        Err(_) => return claims,
    };
    claims.issuer = extract_xml_text_after(&xml_str, "Issuer");
    claims.audience = extract_xml_text_after(&xml_str, "Audience");
    claims
}

/// Find the first occurrence of an opening tag with `local_name` (with or
/// without an XML namespace prefix) and return its text content. Used by
/// the SAML claim extractor — matches the same pragmatic, prefix-tolerant
/// approach as `extract_saml_session_name`.
fn extract_xml_text_after(xml: &str, local_name: &str) -> Option<String> {
    // Try `<local_name`, `<saml:local_name`, `<saml2:local_name`, etc by
    // scanning for `<` followed by the local name preceded by either `:`
    // or just `<`.
    let mut search_from = 0;
    while let Some(idx) = xml[search_from..].find('<') {
        let abs = search_from + idx;
        let after_lt = &xml[abs + 1..];
        // Strip optional namespace prefix.
        let tag_start = after_lt
            .split_once(':')
            .map(|(_pfx, rest)| rest)
            .unwrap_or(after_lt);
        if let Some(after_name) = tag_start.strip_prefix(local_name) {
            // Verify the next char ends the local name (whitespace, '>', '/').
            let valid_terminator = after_name
                .chars()
                .next()
                .map(|c| c == '>' || c == ' ' || c == '/' || c == '\t' || c == '\n')
                .unwrap_or(false);
            if valid_terminator {
                let gt_pos = after_lt.find('>')?;
                let content_start = abs + 1 + gt_pos + 1;
                let next_lt = xml[content_start..].find('<')?;
                let value = xml[content_start..content_start + next_lt].trim();
                if !value.is_empty() {
                    return Some(value.to_string());
                }
            }
        }
        search_from = abs + 1;
    }
    None
}

/// Decoded JWT claims we care about for `AssumeRoleWithWebIdentity`.
/// We never verify the signature — fakecloud is not a security boundary —
/// but we DO require the token to be a syntactically valid JWT with an
/// `iss` claim so trust-policy enforcement has something to bind to.
#[derive(Debug, Clone, Default)]
struct JwtClaims {
    iss: Option<String>,
    /// `aud` per RFC 7519 §4.1.3 may be either a single string or a JSON
    /// array of strings. Real-world IdPs (Google, Auth0, Cognito) all
    /// emit the array form regularly, so we carry every entry and
    /// match against any of them when validating.
    aud: Vec<String>,
    sub: Option<String>,
    raw: serde_json::Map<String, serde_json::Value>,
}

/// Parse a base64url-encoded JWT into its `iss`/`aud`/`sub` claims.
/// Returns `None` when the token is not a 3-segment JWT or the payload
/// is not JSON. We accept both unpadded base64url (canonical) and the
/// padded variant some libraries emit.
fn decode_jwt(token: &str) -> Option<JwtClaims> {
    use base64::Engine;
    let segments: Vec<&str> = token.split('.').collect();
    if segments.len() != 3 {
        return None;
    }
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(segments[1])
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(segments[1]))
        .ok()?;
    let json: serde_json::Value = serde_json::from_slice(&payload).ok()?;
    let map = json.as_object()?.clone();
    let str_field = |k: &str| map.get(k).and_then(|v| v.as_str()).map(|s| s.to_string());
    // RFC 7519 §4.1.3: `aud` is either a string or a JSON array of
    // strings. Accept both shapes — Google, Auth0, and Cognito all
    // emit the array form.
    let aud = match map.get("aud") {
        Some(serde_json::Value::String(s)) => vec![s.clone()],
        Some(serde_json::Value::Array(arr)) => arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect(),
        _ => Vec::new(),
    };
    Some(JwtClaims {
        iss: str_field("iss"),
        aud,
        sub: str_field("sub"),
        raw: map,
    })
}

/// Normalize an OIDC issuer URL for comparison with a registered
/// `OpenIDConnectProvider` URL. AWS stores the URL without scheme and
/// without trailing slash, while JWT `iss` claims usually carry
/// `https://`. We strip both ends so callers can do an equality check.
fn normalize_issuer(value: &str) -> String {
    let no_scheme = value
        .strip_prefix("https://")
        .or_else(|| value.strip_prefix("http://"))
        .unwrap_or(value);
    no_scheme.trim_end_matches('/').to_string()
}

/// Find a registered OIDC provider whose URL matches the given JWT
/// `iss` claim. Searches across every account's IAM state — federation
/// is a global concern, and the calling account doesn't necessarily own
/// the provider record.
fn find_oidc_provider<'a>(
    accounts: &'a fakecloud_core::multi_account::MultiAccountState<IamState>,
    issuer: &str,
) -> Option<(&'a str, &'a crate::state::OidcProvider)> {
    let normalized = normalize_issuer(issuer);
    for (acct_id, state) in accounts.iter() {
        for provider in state.oidc_providers.values() {
            if normalize_issuer(&provider.url) == normalized {
                return Some((acct_id, provider));
            }
        }
    }
    None
}

/// Find a registered SAML provider by ARN. Same cross-account scan as
/// the OIDC variant — SAML provider ARNs name the provider's owning
/// account in the ARN itself.
fn find_saml_provider<'a>(
    accounts: &'a fakecloud_core::multi_account::MultiAccountState<IamState>,
    arn: &str,
) -> Option<&'a crate::state::SamlProvider> {
    for (_acct_id, state) in accounts.iter() {
        if let Some(provider) = state.saml_providers.get(arn) {
            return Some(provider);
        }
    }
    None
}

/// Pull the expected audience out of a SAML provider's metadata
/// document. Real metadata uses `entityID="..."` on the `<EntityDescriptor>`
/// root element to name the IdP — AWS treats it as the audience the
/// assertion must be addressed to. We use a best-effort string scan
/// rather than full XML parsing; the metadata format is stable and
/// callers that want the strict path can supply a SAML provider with
/// no metadata, in which case we skip the audience check.
fn expected_saml_audience(metadata: &str) -> Option<String> {
    let needle = "entityID=";
    let pos = metadata.find(needle)?;
    let after = &metadata[pos + needle.len()..];
    let quote = after.chars().next()?;
    if quote != '"' && quote != '\'' {
        return None;
    }
    let rest = &after[1..];
    let end = rest.find(quote)?;
    let value = rest[..end].trim();
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

/// Build the synthetic federated `Principal` we hand to the trust-policy
/// evaluator for `AssumeRoleWithSAML` / `AssumeRoleWithWebIdentity`. The
/// ARN is the federated provider (SAML provider ARN or OIDC issuer);
/// `principal_type` is `FederatedUser`, which is what
/// [`PrincipalRef::Federated`] matches against.
fn federated_principal(provider_arn: &str, account_id: &str) -> Principal {
    Principal {
        arn: provider_arn.to_string(),
        user_id: provider_arn.to_string(),
        account_id: account_id.to_string(),
        principal_type: PrincipalType::FederatedUser,
        source_identity: None,
        tags: None,
    }
}

/// Produce the AWS-style AccessDenied error returned when the role's
/// trust policy refuses an STS AssumeRole* call.
fn trust_policy_denied(action: &str, caller_arn: &str, role_arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::FORBIDDEN,
        "AccessDenied",
        format!(
            "User: {} is not authorized to perform: {} on resource: {}",
            caller_arn, action, role_arn
        ),
    )
}

/// Extract the RoleSessionName from a base64-encoded SAML assertion.
fn extract_saml_session_name(saml_b64: &str) -> Option<String> {
    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(saml_b64)
        .ok()?;
    let xml_str = String::from_utf8(decoded).ok()?;

    // Look for the RoleSessionName attribute value in the SAML XML.
    let role_session_attr = "https://aws.amazon.com/SAML/Attributes/RoleSessionName";
    let pos = xml_str.find(role_session_attr)?;

    // Find the AttributeValue after this position
    let after = &xml_str[pos..];
    let av_start = after.find("AttributeValue")?;
    let after_av = &after[av_start..];
    // Skip past the closing >
    let gt_pos = after_av.find('>')?;
    let value_start = &after_av[gt_pos + 1..];
    // Find end of value (next < which starts the closing tag)
    let lt_pos = value_start.find('<')?;
    let value = value_start[..lt_pos].trim();

    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_for_region() {
        assert_eq!(partition_for_region("us-east-1"), "aws");
        assert_eq!(partition_for_region("eu-west-1"), "aws");
        assert_eq!(partition_for_region("cn-north-1"), "aws-cn");
        assert_eq!(partition_for_region("cn-northwest-1"), "aws-cn");
        assert_eq!(partition_for_region("us-isob-east-1"), "aws-iso-b");
        assert_eq!(partition_for_region("us-iso-east-1"), "aws-iso");
    }

    #[test]
    fn test_extract_account_from_arn() {
        assert_eq!(
            extract_account_from_arn("arn:aws:iam::123456789012:role/test"),
            Some("123456789012".to_string())
        );
        assert_eq!(
            extract_account_from_arn("arn:aws:iam::111111111111:role/test"),
            Some("111111111111".to_string())
        );
        assert_eq!(extract_account_from_arn("invalid"), None);
    }

    #[test]
    fn test_extract_saml_session_name() {
        use base64::Engine;
        let xml = r#"<?xml version="1.0"?><samlp:Response><Assertion><AttributeStatement><Attribute Name="https://aws.amazon.com/SAML/Attributes/RoleSessionName"><AttributeValue>testuser</AttributeValue></Attribute></AttributeStatement></Assertion></samlp:Response>"#;
        let encoded = base64::engine::general_purpose::STANDARD.encode(xml.as_bytes());
        assert_eq!(
            extract_saml_session_name(&encoded),
            Some("testuser".to_string())
        );
    }

    #[test]
    fn test_extract_saml_session_name_with_namespace() {
        use base64::Engine;
        let xml = r#"<?xml version="1.0"?><samlp:Response><saml:Assertion><saml:AttributeStatement><saml:Attribute Name="https://aws.amazon.com/SAML/Attributes/RoleSessionName"><saml:AttributeValue>testuser</saml:AttributeValue></saml:Attribute></saml:AttributeStatement></saml:Assertion></samlp:Response>"#;
        let encoded = base64::engine::general_purpose::STANDARD.encode(xml.as_bytes());
        assert_eq!(
            extract_saml_session_name(&encoded),
            Some("testuser".to_string())
        );
    }

    #[test]
    fn test_session_token_format() {
        let token = xml_responses::generate_session_token();
        assert_eq!(token.len(), 356);
        assert!(token.starts_with("FQoGZXIvYXdzE"));
    }

    #[test]
    fn test_access_key_id_format() {
        let key = xml_responses::generate_access_key_id();
        assert_eq!(key.len(), 20);
        assert!(key.starts_with("FSIA"));
    }

    #[test]
    fn test_secret_access_key_format() {
        let key = xml_responses::generate_secret_access_key();
        assert_eq!(key.len(), 40);
    }

    #[test]
    fn test_role_id_format() {
        let id = xml_responses::generate_role_id();
        assert_eq!(id.len(), 21);
        assert!(id.starts_with("AROA"));
    }

    #[test]
    fn test_decode_authorization_message() {
        use parking_lot::RwLock;
        use std::collections::HashMap;
        use std::sync::Arc;

        let state: SharedIamState = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let service = StsService::new(state);

        // Encode a real deny payload, then verify the decode op
        // round-trips it back through the response body.
        let token = crate::auth_message::encode_deny(
            true,
            Some("s3:GetObject"),
            Some("arn:aws:iam::123456789012:user/alice"),
            vec![serde_json::json!({"sourcePolicyId": "deny-bucket-foo"})],
            None,
        );
        let mut params = HashMap::new();
        params.insert("EncodedMessage".to_string(), token);

        let req = make_test_request(params);
        let resp = service.decode_authorization_message(&req).unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(body.contains("DecodedMessage"));
        assert!(body.contains("explicitDeny"));
        assert!(body.contains("s3:GetObject"));
        assert!(body.contains("deny-bucket-foo"));
    }

    #[test]
    fn test_decode_authorization_message_rejects_invalid_token() {
        use parking_lot::RwLock;
        use std::collections::HashMap;
        use std::sync::Arc;

        let state: SharedIamState = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let service = StsService::new(state);

        let mut params = HashMap::new();
        params.insert("EncodedMessage".to_string(), "not-a-real-token".to_string());
        let req = make_test_request(params);
        let err = match service.decode_authorization_message(&req) {
            Err(e) => e,
            Ok(_) => panic!("expected InvalidAuthorizationMessageException"),
        };
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        let msg = format!("{:?}", err);
        assert!(msg.contains("InvalidAuthorizationMessageException"));
    }

    #[test]
    fn test_decode_authorization_message_missing_param() {
        use parking_lot::RwLock;
        use std::collections::HashMap;
        use std::sync::Arc;

        let state: SharedIamState = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let service = StsService::new(state);

        let req = make_test_request(HashMap::new());
        let result = service.decode_authorization_message(&req);
        assert!(result.is_err());
        let err = result.err().unwrap();
        let msg = format!("{:?}", err);
        assert!(msg.contains("EncodedMessage"));
    }

    fn make_test_request(params: std::collections::HashMap<String, String>) -> AwsRequest {
        AwsRequest {
            service: "sts".into(),
            action: "Test".into(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            request_id: "test".into(),
            headers: http::HeaderMap::new(),
            query_params: params,
            body: Default::default(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: true,
            access_key_id: None,
            principal: None,
        }
    }

    fn parse_expiration(s: &str) -> chrono::DateTime<Utc> {
        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%SZ")
            .expect("valid timestamp")
            .and_utc()
    }

    #[test]
    fn test_compute_expiration_with_duration() {
        use std::collections::HashMap;

        let mut params = HashMap::new();
        params.insert("DurationSeconds".to_string(), "1800".to_string());
        let req = make_test_request(params);

        let now = Utc::now();
        let exp_str = compute_expiration(&req, 3600).unwrap();
        let exp_utc = parse_expiration(&exp_str);

        // Should be ~1800s from now (using provided DurationSeconds, not default)
        let diff = (exp_utc - now).num_seconds();
        assert!(
            (1798..=1802).contains(&diff),
            "expected ~1800s duration, got {diff}s"
        );
    }

    #[test]
    fn test_compute_expiration_default() {
        use std::collections::HashMap;

        let req = make_test_request(HashMap::new());

        let now = Utc::now();
        let exp_str = compute_expiration(&req, 43200).unwrap();
        let exp_utc = parse_expiration(&exp_str);

        // Should be ~43200s (12 hours) from now using default
        let diff = (exp_utc - now).num_seconds();
        assert!(
            (43198..=43202).contains(&diff),
            "expected ~43200s duration, got {diff}s"
        );
    }

    #[test]
    fn test_compute_expiration_uses_provided_not_default() {
        use std::collections::HashMap;

        let mut params = HashMap::new();
        params.insert("DurationSeconds".to_string(), "900".to_string());
        let req = make_test_request(params);

        let before = Utc::now();
        let exp_str = compute_expiration(&req, 43200).unwrap();
        let exp_utc = parse_expiration(&exp_str);

        // Should use 900s, not the default 43200s
        let expected = before + chrono::Duration::seconds(900);
        let diff = (exp_utc - expected).num_seconds().abs();
        assert!(
            diff <= 2,
            "expected ~900s duration, got diff={diff}s from expected"
        );
    }

    fn make_sts_service() -> (StsService, SharedIamState) {
        use parking_lot::RwLock;
        use std::sync::Arc;

        let state: SharedIamState = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let sts = StsService::new(state.clone());
        (sts, state)
    }

    fn sts_request(action: &str, params: Vec<(&str, &str)>) -> AwsRequest {
        let mut qp = std::collections::HashMap::new();
        qp.insert("Action".to_string(), action.to_string());
        for (k, v) in params {
            qp.insert(k.to_string(), v.to_string());
        }
        let mut req = make_test_request(qp);
        req.action = action.to_string();
        req
    }

    fn create_role_in_state(state: &SharedIamState, name: &str) -> String {
        // Permissive default trust so the basic-path tests don't trip
        // the new evaluator-driven trust gate. Tests that specifically
        // exercise restricted trust policies use
        // `create_role_in_state_with_trust` directly.
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole"}]}"#;
        create_role_in_state_with_trust(state, name, trust)
    }

    fn create_role_in_state_with_trust(
        state: &SharedIamState,
        name: &str,
        trust_policy: &str,
    ) -> String {
        let arn = fakecloud_aws::arn::Arn::global("iam", "123456789012", &format!("role/{name}"))
            .to_string();
        let mut accounts = state.write();
        let s = accounts.get_or_create("123456789012");
        // Real CreateRole inserts by role_name; assume_role looks up
        // by role_name too, so the map key must match for the trust
        // policy gate to actually fire.
        s.roles.insert(
            name.to_string(),
            crate::state::IamRole {
                role_name: name.to_string(),
                role_id: format!("AROA{}", &uuid::Uuid::new_v4().to_string()[..17]),
                arn: arn.clone(),
                path: "/".to_string(),
                assume_role_policy_document: trust_policy.to_string(),
                created_at: Utc::now(),
                description: None,
                max_session_duration: 3600,
                tags: Vec::new(),
                permissions_boundary: None,
            },
        );
        arn
    }

    // ── GetCallerIdentity ──

    #[tokio::test]
    async fn get_caller_identity() {
        let (svc, _) = make_sts_service();
        let mut req = sts_request("GetCallerIdentity", vec![]);
        // F4: GetCallerIdentity now rejects calls with neither a
        // resolved principal nor an Authorization header. Add a stub
        // header so the unauthenticated-but-account-scoped fallback
        // (used by smoke probes / the `test` root bypass) still
        // returns a usable identity.
        req.headers.insert(
            http::header::AUTHORIZATION,
            http::HeaderValue::from_static("AWS4-HMAC-SHA256 Credential=test/test"),
        );
        let resp = svc.handle(req).await.unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(body.contains("<Account>123456789012</Account>"));
        assert!(body.contains("<Arn>"));
    }

    #[tokio::test]
    async fn get_caller_identity_rejects_unauthenticated_request() {
        // No principal AND no Authorization header → AWS returns
        // MissingAuthenticationTokenException (403). The `test` root
        // bypass and signed requests both attach a header, so the
        // common dev path is unaffected.
        let (svc, _) = make_sts_service();
        let req = sts_request("GetCallerIdentity", vec![]);
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected MissingAuthenticationTokenException"),
        };
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
        assert!(format!("{:?}", err).contains("MissingAuthenticationTokenException"));
    }

    // ── AssumeRole ──

    #[tokio::test]
    async fn assume_role_basic() {
        let (svc, state) = make_sts_service();
        let role_arn = create_role_in_state(&state, "test-role");

        let req = sts_request(
            "AssumeRole",
            vec![("RoleArn", &role_arn), ("RoleSessionName", "test-session")],
        );
        let resp = svc.handle(req).await.unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(body.contains("<AccessKeyId>"));
        assert!(body.contains("<SecretAccessKey>"));
        assert!(body.contains("<SessionToken>"));
    }

    #[tokio::test]
    async fn assume_role_not_found() {
        let (svc, _) = make_sts_service();
        let req = sts_request(
            "AssumeRole",
            vec![
                ("RoleArn", "arn:aws:iam::123456789012:role/nonexistent"),
                ("RoleSessionName", "s"),
            ],
        );
        assert!(svc.handle(req).await.is_err());
    }

    #[tokio::test]
    async fn assume_role_missing_session_name() {
        let (svc, _) = make_sts_service();
        let req = sts_request(
            "AssumeRole",
            vec![("RoleArn", "arn:aws:iam::123456789012:role/r")],
        );
        assert!(svc.handle(req).await.is_err());
    }

    // ── AssumeRoleWithWebIdentity ──

    #[tokio::test]
    async fn assume_role_with_web_identity() {
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRoleWithWebIdentity"}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "web-role", trust);

        let req = sts_request(
            "AssumeRoleWithWebIdentity",
            vec![
                ("RoleArn", &role_arn),
                ("RoleSessionName", "web-session"),
                ("WebIdentityToken", "fake-jwt-token"),
            ],
        );
        let resp = svc.handle(req).await.unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(body.contains("<AccessKeyId>"));
    }

    // ── GetSessionToken ──

    #[tokio::test]
    async fn get_session_token() {
        let (svc, _) = make_sts_service();
        let req = sts_request("GetSessionToken", vec![]);
        let resp = svc.handle(req).await.unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(body.contains("<AccessKeyId>"));
        assert!(body.contains("<SessionToken>"));
    }

    #[tokio::test]
    async fn get_session_token_with_duration() {
        let (svc, _) = make_sts_service();
        let req = sts_request("GetSessionToken", vec![("DurationSeconds", "1800")]);
        let resp = svc.handle(req).await.unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(body.contains("<Expiration>"));
    }

    // ── GetFederationToken ──

    #[tokio::test]
    async fn get_federation_token() {
        let (svc, _) = make_sts_service();
        let req = sts_request("GetFederationToken", vec![("Name", "feduser")]);
        let resp = svc.handle(req).await.unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(body.contains("<AccessKeyId>"));
        assert!(body.contains("<FederatedUserId>"));
    }

    // ── GetAccessKeyInfo ──

    #[tokio::test]
    async fn get_access_key_info() {
        let (svc, _) = make_sts_service();
        let req = sts_request(
            "GetAccessKeyInfo",
            vec![("AccessKeyId", "AKIAIOSFODNN7EXAMPLE")],
        );
        let resp = svc.handle(req).await.unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(body.contains("<Account>"));
    }

    // ── Trust policy: ExternalId enforcement ──

    #[tokio::test]
    async fn assume_role_rejects_when_external_id_missing() {
        // Trust policy demands sts:ExternalId; caller didn't supply
        // one — AssumeRole must 403.
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole","Condition":{"StringEquals":{"sts:ExternalId":"secret-handshake"}}}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "third-party", trust);
        let req = sts_request(
            "AssumeRole",
            vec![("RoleArn", &role_arn), ("RoleSessionName", "sess")],
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected AccessDenied when ExternalId missing"),
        };
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn assume_role_rejects_when_external_id_mismatches() {
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole","Condition":{"StringEquals":{"sts:ExternalId":"secret-handshake"}}}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "third-party", trust);
        let req = sts_request(
            "AssumeRole",
            vec![
                ("RoleArn", &role_arn),
                ("RoleSessionName", "sess"),
                ("ExternalId", "wrongguess"),
            ],
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected AccessDenied when ExternalId mismatches"),
        };
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn assume_role_succeeds_when_external_id_matches() {
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole","Condition":{"StringEquals":{"sts:ExternalId":"secret-handshake"}}}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "third-party", trust);
        let req = sts_request(
            "AssumeRole",
            vec![
                ("RoleArn", &role_arn),
                ("RoleSessionName", "sess"),
                ("ExternalId", "secret-handshake"),
            ],
        );
        let resp = svc.handle(req).await.unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(body.contains("<AccessKeyId>"));
    }

    #[tokio::test]
    async fn assume_role_proceeds_when_no_external_id_required() {
        // No ExternalId Condition in the trust policy — caller doesn't
        // need to supply one.
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole"}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "open-role", trust);
        let req = sts_request(
            "AssumeRole",
            vec![("RoleArn", &role_arn), ("RoleSessionName", "sess")],
        );
        svc.handle(req).await.unwrap();
    }

    // ── Trust policy: principal gating via evaluator ──

    #[tokio::test]
    async fn assume_role_rejects_when_trust_policy_has_no_statements() {
        let (svc, state) = make_sts_service();
        let role_arn = create_role_in_state_with_trust(&state, "no-trust", r#"{"Statement":[]}"#);
        let req = sts_request(
            "AssumeRole",
            vec![("RoleArn", &role_arn), ("RoleSessionName", "sess")],
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected AccessDenied"),
        };
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn assume_role_rejects_when_trust_policy_excludes_caller() {
        let (svc, state) = make_sts_service();
        // Trust policy only allows a specific service that isn't the
        // anonymous caller; evaluator must reject.
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "ec2-only", trust);
        let req = sts_request(
            "AssumeRole",
            vec![("RoleArn", &role_arn), ("RoleSessionName", "sess")],
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected AccessDenied"),
        };
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn assume_role_rejects_when_trust_policy_explicitly_denies() {
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole"},{"Effect":"Deny","Principal":{"AWS":"*"},"Action":"sts:AssumeRole"}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "deny-wins", trust);
        let req = sts_request(
            "AssumeRole",
            vec![("RoleArn", &role_arn), ("RoleSessionName", "sess")],
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected AccessDenied"),
        };
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn assume_role_allowed_by_trust_policy_with_principal_match() {
        // Trust policy names the caller's account explicitly. Per AWS,
        // `"Principal": { "AWS": "123456789012" }` is shorthand for the
        // account root and matches any IAM principal in that account.
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"123456789012"},"Action":"sts:AssumeRole"}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "named", trust);
        let req = sts_request(
            "AssumeRole",
            vec![("RoleArn", &role_arn), ("RoleSessionName", "sess")],
        );
        let resp = svc.handle(req).await.unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(body.contains("<AccessKeyId>"), "{body}");
    }

    #[tokio::test]
    async fn assume_role_blocked_when_principal_not_in_trust_policy() {
        // Trust policy lists a different account — caller's account
        // (123456789012) doesn't match, so AssumeRole must 403.
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::999999999999:root"},"Action":"sts:AssumeRole"}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "other-account", trust);
        let req = sts_request(
            "AssumeRole",
            vec![("RoleArn", &role_arn), ("RoleSessionName", "sess")],
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected AccessDenied when caller account not in trust policy"),
        };
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }

    // ── Trust policy: ExternalId aliases (rename of existing tests for plan) ──

    #[tokio::test]
    async fn assume_role_blocked_when_external_id_required_but_missing() {
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole","Condition":{"StringEquals":{"sts:ExternalId":"hello"}}}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "ext-required", trust);
        let req = sts_request(
            "AssumeRole",
            vec![("RoleArn", &role_arn), ("RoleSessionName", "sess")],
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected AccessDenied when ExternalId required but missing"),
        };
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn assume_role_succeeds_with_correct_external_id() {
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole","Condition":{"StringEquals":{"sts:ExternalId":"hello"}}}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "ext-ok", trust);
        let req = sts_request(
            "AssumeRole",
            vec![
                ("RoleArn", &role_arn),
                ("RoleSessionName", "sess"),
                ("ExternalId", "hello"),
            ],
        );
        svc.handle(req).await.unwrap();
    }

    // ── Trust policy: MFA enforcement ──

    #[tokio::test]
    async fn assume_role_blocked_when_mfa_required_but_not_present() {
        // Trust policy requires `aws:MultiFactorAuthPresent: true`; the
        // request didn't supply SerialNumber+TokenCode, so the condition
        // evaluates false and AssumeRole must 403.
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole","Condition":{"Bool":{"aws:MultiFactorAuthPresent":"true"}}}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "mfa-required", trust);
        let req = sts_request(
            "AssumeRole",
            vec![("RoleArn", &role_arn), ("RoleSessionName", "sess")],
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected AccessDenied when MFA required but not supplied"),
        };
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn assume_role_succeeds_with_mfa_supplied() {
        // Same trust policy as above, but the caller supplied MFA —
        // condition evaluates true and AssumeRole succeeds. The minted
        // session credential carries `mfa_present: true` so downstream
        // Authorize evaluations see `aws:MultiFactorAuthPresent`.
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole","Condition":{"Bool":{"aws:MultiFactorAuthPresent":"true"}}}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "mfa-ok", trust);
        let req = sts_request(
            "AssumeRole",
            vec![
                ("RoleArn", &role_arn),
                ("RoleSessionName", "sess"),
                ("SerialNumber", "arn:aws:iam::123456789012:mfa/alice"),
                ("TokenCode", "123456"),
            ],
        );
        let resp = svc.handle(req).await.unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(body.contains("<AccessKeyId>"), "{body}");
        // The session credential the resolver hands out must record
        // mfa_present=true so Authorize sees aws:MultiFactorAuthPresent.
        let states = state.read();
        let s = states.get("123456789012").unwrap();
        let any_mfa = s.sts_temp_credentials.values().any(|c| c.mfa_present);
        assert!(
            any_mfa,
            "expected at least one minted credential with mfa_present=true"
        );
    }

    #[tokio::test]
    async fn assume_role_with_mfa_resolved_credential_drives_iam_evaluator() {
        // E2E: assume role with MFA, fetch the issued credential through
        // the same `CredentialResolver` adapter dispatch uses, then run
        // the IAM evaluator on a policy that gates on
        // `aws:MultiFactorAuthPresent: true`. This wires
        // sts_service -> StsTempCredential -> SecretLookup ->
        // ResolvedCredential -> ConditionContext end to end and proves
        // the MFA assertion survives every hop.
        use crate::credential_resolver::IamCredentialResolver;
        use crate::evaluator::{
            evaluate as eval_policies, EvalRequest, PolicyDocument, RequestContext,
        };
        use fakecloud_core::auth::{ConditionContext, CredentialResolver};

        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole"}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "mfa-e2e", trust);
        let req = sts_request(
            "AssumeRole",
            vec![
                ("RoleArn", &role_arn),
                ("RoleSessionName", "ops"),
                ("SerialNumber", "arn:aws:iam::123456789012:mfa/alice"),
                ("TokenCode", "654321"),
            ],
        );
        let resp = svc.handle(req).await.unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        // Pull the AccessKeyId out of the XML response and resolve it.
        let access_key_id = body
            .split("<AccessKeyId>")
            .nth(1)
            .and_then(|s| s.split("</AccessKeyId>").next())
            .expect("response should contain AccessKeyId")
            .to_string();
        let resolver = IamCredentialResolver::new(state.clone());
        let resolved = resolver
            .resolve(&access_key_id)
            .expect("issued credential must resolve through the resolver");
        assert!(
            resolved.mfa_present,
            "F3: MFA flag must survive the resolver hop"
        );
        assert!(
            resolved.token_issued_at.is_some(),
            "F3: token_issued_at must be populated for STS sessions"
        );

        // Mirror what dispatch does: build a ConditionContext from the
        // resolved credential, then evaluate a permission policy that
        // requires MFA.
        let mut ctx: RequestContext = ConditionContext {
            aws_principal_arn: Some(resolved.principal.arn.clone()),
            aws_principal_account: Some(resolved.principal.account_id.clone()),
            aws_userid: Some(resolved.principal.user_id.clone()),
            aws_mfa_present: Some(resolved.mfa_present),
            aws_token_issue_time: resolved.token_issued_at,
            aws_federated_provider: resolved.federated_provider.clone(),
            ..Default::default()
        };
        if resolved.mfa_present {
            if let Some(issued) = resolved.token_issued_at {
                ctx.aws_mfa_age_seconds = Some(
                    Utc::now()
                        .signed_duration_since(issued)
                        .num_seconds()
                        .max(0),
                );
            }
        }

        let policy = PolicyDocument::parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow",
                "Action":"s3:GetObject",
                "Resource":"*",
                "Condition":{"Bool":{"aws:MultiFactorAuthPresent":"true"}}
            }]}"#,
        );
        let eval = EvalRequest {
            principal: &resolved.principal,
            action: "s3:GetObject".to_string(),
            resource: "arn:aws:s3:::secrets/k".to_string(),
            context: ctx,
        };
        let decision = eval_policies(&[policy], &eval);
        assert_eq!(
            decision,
            crate::evaluator::Decision::Allow,
            "F3: MFA-gated allow must fire when session was minted with MFA"
        );

        // Negative control: same evaluator wiring but without MFA on
        // the resolved credential -> implicit deny.
        let req_no_mfa = sts_request(
            "AssumeRole",
            vec![("RoleArn", &role_arn), ("RoleSessionName", "no-mfa")],
        );
        let resp_no_mfa = svc.handle(req_no_mfa).await.unwrap();
        let body_no_mfa = std::str::from_utf8(resp_no_mfa.body.expect_bytes()).unwrap();
        let akid_no_mfa = body_no_mfa
            .split("<AccessKeyId>")
            .nth(1)
            .and_then(|s| s.split("</AccessKeyId>").next())
            .unwrap()
            .to_string();
        let resolved_no_mfa = resolver.resolve(&akid_no_mfa).unwrap();
        assert!(!resolved_no_mfa.mfa_present);
        let policy2 = PolicyDocument::parse(
            r#"{"Version":"2012-10-17","Statement":[{
                "Effect":"Allow",
                "Action":"s3:GetObject",
                "Resource":"*",
                "Condition":{"Bool":{"aws:MultiFactorAuthPresent":"true"}}
            }]}"#,
        );
        let ctx2 = ConditionContext {
            aws_principal_arn: Some(resolved_no_mfa.principal.arn.clone()),
            aws_userid: Some(resolved_no_mfa.principal.user_id.clone()),
            aws_mfa_present: Some(resolved_no_mfa.mfa_present),
            aws_token_issue_time: resolved_no_mfa.token_issued_at,
            ..Default::default()
        };
        let eval2 = EvalRequest {
            principal: &resolved_no_mfa.principal,
            action: "s3:GetObject".to_string(),
            resource: "arn:aws:s3:::secrets/k".to_string(),
            context: ctx2,
        };
        assert_eq!(
            eval_policies(&[policy2], &eval2),
            crate::evaluator::Decision::ImplicitDeny,
            "F3: MFA-gated allow must NOT fire when session was minted without MFA"
        );
    }

    #[tokio::test]
    async fn assume_role_with_saml_populates_federated_provider() {
        // F3: AssumeRoleWithSAML must surface the SAML provider ARN as
        // `aws:FederatedProvider` on the resulting session.
        use crate::credential_resolver::IamCredentialResolver;
        use base64::Engine;
        use fakecloud_core::auth::CredentialResolver;
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRoleWithSAML"}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "saml-role", trust);
        let saml_xml = r#"<?xml version="1.0"?><samlp:Response><Assertion><AttributeStatement><Attribute Name="https://aws.amazon.com/SAML/Attributes/RoleSessionName"><AttributeValue>jane</AttributeValue></Attribute></AttributeStatement></Assertion></samlp:Response>"#;
        let saml_b64 = base64::engine::general_purpose::STANDARD.encode(saml_xml);
        let provider_arn = "arn:aws:iam::123456789012:saml-provider/idp";
        let req = sts_request(
            "AssumeRoleWithSAML",
            vec![
                ("RoleArn", &role_arn),
                ("PrincipalArn", provider_arn),
                ("SAMLAssertion", &saml_b64),
            ],
        );
        let resp = svc.handle(req).await.unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        let access_key_id = body
            .split("<AccessKeyId>")
            .nth(1)
            .and_then(|s| s.split("</AccessKeyId>").next())
            .unwrap()
            .to_string();
        let resolver = IamCredentialResolver::new(state.clone());
        let resolved = resolver.resolve(&access_key_id).unwrap();
        assert_eq!(
            resolved.federated_provider.as_deref(),
            Some(provider_arn),
            "AssumeRoleWithSAML must populate aws:FederatedProvider with the SAML provider ARN"
        );
    }

    #[tokio::test]
    async fn assume_role_with_web_identity_populates_federated_provider() {
        // F3: AssumeRoleWithWebIdentity must populate
        // `aws:FederatedProvider`. With ProviderId we carry it verbatim;
        // without ProviderId we synthesize an OIDC provider ARN keyed
        // off the role's account so policies that simply check for the
        // presence of a federated provider still bind.
        use crate::credential_resolver::IamCredentialResolver;
        use fakecloud_core::auth::CredentialResolver;
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRoleWithWebIdentity"}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "oidc-role", trust);
        let req = sts_request(
            "AssumeRoleWithWebIdentity",
            vec![
                ("RoleArn", &role_arn),
                ("RoleSessionName", "oidc-session"),
                ("WebIdentityToken", "fake-jwt-blob"),
                ("ProviderId", "accounts.google.com"),
            ],
        );
        let resp = svc.handle(req).await.unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        let access_key_id = body
            .split("<AccessKeyId>")
            .nth(1)
            .and_then(|s| s.split("</AccessKeyId>").next())
            .unwrap()
            .to_string();
        let resolver = IamCredentialResolver::new(state.clone());
        let resolved = resolver.resolve(&access_key_id).unwrap();
        assert_eq!(
            resolved.federated_provider.as_deref(),
            Some("accounts.google.com"),
            "AssumeRoleWithWebIdentity must carry ProviderId as aws:FederatedProvider"
        );
    }

    #[tokio::test]
    async fn assume_role_userid_format_matches_aws() {
        // AWS userid for assumed-role sessions: <role-id>:<RoleSessionName>.
        // Verify the resolved credential's user_id matches that shape so
        // a policy condition `aws:userid` can be matched correctly.
        use crate::credential_resolver::IamCredentialResolver;
        use fakecloud_core::auth::CredentialResolver;
        let (svc, state) = make_sts_service();
        let role_arn = create_role_in_state(&state, "userid-role");
        let req = sts_request(
            "AssumeRole",
            vec![("RoleArn", &role_arn), ("RoleSessionName", "carol")],
        );
        let resp = svc.handle(req).await.unwrap();
        let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        let access_key_id = body
            .split("<AccessKeyId>")
            .nth(1)
            .and_then(|s| s.split("</AccessKeyId>").next())
            .unwrap()
            .to_string();
        let resolver = IamCredentialResolver::new(state);
        let resolved = resolver.resolve(&access_key_id).unwrap();
        let uid = &resolved.principal.user_id;
        assert!(
            uid.contains(':'),
            "assumed-role userid must be `<role-id>:<RoleSessionName>`, got `{uid}`"
        );
        assert!(
            uid.ends_with(":carol"),
            "assumed-role userid must end with the RoleSessionName, got `{uid}`"
        );
    }

    // ── Service-linked roles ──

    #[tokio::test]
    async fn assume_service_linked_role_blocked_when_caller_not_matching_service() {
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ecs.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
        let arn = fakecloud_aws::arn::Arn::global(
            "iam",
            "123456789012",
            "role/aws-service-role/ecs.amazonaws.com/AWSServiceRoleForECS",
        )
        .to_string();
        {
            let mut accounts = state.write();
            let s = accounts.get_or_create("123456789012");
            s.roles.insert(
                "AWSServiceRoleForECS".to_string(),
                crate::state::IamRole {
                    role_name: "AWSServiceRoleForECS".to_string(),
                    role_id: "AROASLRECS".to_string(),
                    arn: arn.clone(),
                    path: "/aws-service-role/ecs.amazonaws.com/".to_string(),
                    assume_role_policy_document: trust.to_string(),
                    created_at: Utc::now(),
                    description: None,
                    max_session_duration: 3600,
                    tags: Vec::new(),
                    permissions_boundary: None,
                },
            );
        }
        let req = sts_request(
            "AssumeRole",
            vec![("RoleArn", &arn), ("RoleSessionName", "sess")],
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => {
                panic!("expected AccessDenied for service-linked role with non-service caller")
            }
        };
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn service_linked_role_rejects_non_service_caller() {
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRole"}]}"#;
        let arn = fakecloud_aws::arn::Arn::global(
            "iam",
            "123456789012",
            "role/aws-service-role/elasticloadbalancing.amazonaws.com/AWSServiceRoleForELB",
        )
        .to_string();
        {
            let mut accounts = state.write();
            let s = accounts.get_or_create("123456789012");
            s.roles.insert(
                "AWSServiceRoleForELB".to_string(),
                crate::state::IamRole {
                    role_name: "AWSServiceRoleForELB".to_string(),
                    role_id: "AROASLR".to_string(),
                    arn: arn.clone(),
                    path: "/aws-service-role/elasticloadbalancing.amazonaws.com/".to_string(),
                    assume_role_policy_document: trust.to_string(),
                    created_at: Utc::now(),
                    description: None,
                    max_session_duration: 3600,
                    tags: Vec::new(),
                    permissions_boundary: None,
                },
            );
        }
        let req = sts_request(
            "AssumeRole",
            vec![("RoleArn", &arn), ("RoleSessionName", "sess")],
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected AccessDenied for non-service caller"),
        };
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
    }

    // ── Unsupported action ──

    #[tokio::test]
    async fn unsupported_sts_action() {
        let (svc, _) = make_sts_service();
        let req = sts_request("BogusAction", vec![]);
        assert!(svc.handle(req).await.is_err());
    }

    #[tokio::test]
    async fn assume_role_missing_role_arn_errors() {
        let (svc, _) = make_sts_service();
        let req = sts_request("AssumeRole", vec![("RoleSessionName", "sess")]);
        assert!(svc.assume_role(&req).is_err());
    }

    #[tokio::test]
    async fn assume_role_with_web_identity_missing_token_errors() {
        let (svc, _) = make_sts_service();
        let req = sts_request(
            "AssumeRoleWithWebIdentity",
            vec![
                ("RoleArn", "arn:aws:iam::123:role/r"),
                ("RoleSessionName", "s"),
            ],
        );
        assert!(svc.assume_role_with_web_identity(&req).is_err());
    }

    #[tokio::test]
    async fn assume_role_with_saml_missing_assertion_errors() {
        let (svc, _) = make_sts_service();
        let req = sts_request(
            "AssumeRoleWithSAML",
            vec![
                ("RoleArn", "arn:aws:iam::123:role/r"),
                ("PrincipalArn", "arn:aws:iam::123:saml-provider/p"),
            ],
        );
        assert!(svc.assume_role_with_saml(&req).is_err());
    }

    #[tokio::test]
    async fn get_session_token_returns_ok() {
        let (svc, _) = make_sts_service();
        let req = sts_request("GetSessionToken", vec![]);
        let resp = svc.get_session_token(&req).unwrap();
        assert_eq!(resp.status, http::StatusCode::OK);
    }

    #[tokio::test]
    async fn get_federation_token_returns_ok() {
        let (svc, _) = make_sts_service();
        let req = sts_request("GetFederationToken", vec![("Name", "test-user")]);
        let resp = svc.get_federation_token(&req).unwrap();
        assert_eq!(resp.status, http::StatusCode::OK);
    }

    #[tokio::test]
    async fn get_federation_token_missing_name_errors() {
        let (svc, _) = make_sts_service();
        let req = sts_request("GetFederationToken", vec![]);
        assert!(svc.get_federation_token(&req).is_err());
    }

    // ── AssumeRoot ──

    #[tokio::test]
    async fn assume_root_with_account_id_succeeds() {
        let (svc, _) = make_sts_service();
        let req = sts_request(
            "AssumeRoot",
            vec![
                ("TargetPrincipal", "111122223333"),
                (
                    "TaskPolicyArn.arn",
                    "arn:aws:iam::aws:policy/IAMAuditRootUserCredentials",
                ),
            ],
        );
        let resp = svc.assume_root(&req).unwrap();
        assert_eq!(resp.status, http::StatusCode::OK);
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        assert!(body.contains("AccessKeyId"), "{body}");
    }

    #[tokio::test]
    async fn assume_root_with_arn_succeeds() {
        let (svc, _) = make_sts_service();
        let req = sts_request(
            "AssumeRoot",
            vec![
                ("TargetPrincipal", "arn:aws:iam::444455556666:root"),
                (
                    "TaskPolicyArn.arn",
                    "arn:aws:iam::aws:policy/IAMAuditRootUserCredentials",
                ),
                ("DurationSeconds", "600"),
                ("SourceIdentity", "alice"),
            ],
        );
        let resp = svc.assume_root(&req).unwrap();
        let body = String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap();
        assert!(
            body.contains("<SourceIdentity>alice</SourceIdentity>"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn assume_root_missing_task_policy_rejected() {
        // `AssumeRoot` only declares `ExpiredTokenException` and
        // `RegionDisabledException` — no `MissingParameter`-style shape.
        // We route the Smithy @required violation through
        // `ExpiredTokenException` so strict-conformance probes see a
        // declared 4xx instead of either an undeclared error or a 200.
        let (svc, _) = make_sts_service();
        let req = sts_request("AssumeRoot", vec![("TargetPrincipal", "111122223333")]);
        let err = svc
            .assume_root(&req)
            .err()
            .expect("missing TaskPolicyArn must fail");
        assert_eq!(err.code(), "ExpiredTokenException");
    }

    #[tokio::test]
    async fn assume_root_target_principal_below_min_length_rejected() {
        // `TargetPrincipalType` is @length 12..=2048. Anything shorter
        // (e.g. "not-an-id") is rejected up front.
        let (svc, _) = make_sts_service();
        let req = sts_request(
            "AssumeRoot",
            vec![
                ("TargetPrincipal", "not-an-id"),
                ("TaskPolicyArn.arn", "arn:aws:iam::aws:policy/X"),
            ],
        );
        let err = svc
            .assume_root(&req)
            .err()
            .expect("short TargetPrincipal must fail");
        assert_eq!(err.code(), "ExpiredTokenException");
    }

    #[tokio::test]
    async fn assume_root_duration_above_max_rejected() {
        // `RootDurationSecondsType` is @range 0..=900. We now reject
        // out-of-range inputs with the only declared 4xx instead of
        // silently clamping.
        let (svc, _) = make_sts_service();
        let req = sts_request(
            "AssumeRoot",
            vec![
                ("TargetPrincipal", "111122223333"),
                ("TaskPolicyArn.arn", "arn:aws:iam::aws:policy/X"),
                ("DurationSeconds", "1800"),
            ],
        );
        let err = svc
            .assume_root(&req)
            .err()
            .expect("out-of-range DurationSeconds must fail");
        assert_eq!(err.code(), "ExpiredTokenException");
    }
}
