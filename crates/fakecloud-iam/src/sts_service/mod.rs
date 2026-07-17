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

/// STS speaks the awsQuery protocol, whose constraint violations surface as
/// `ValidationError` (no `Exception` suffix). The shared awsJson validators
/// emit `ValidationException`; these wrappers reuse their logic/messages but
/// re-stamp the code so STS matches the AWS wire shape (bug-hunt: STS length /
/// range checks were leaking `ValidationException`).
fn to_validation_error(e: AwsServiceError) -> AwsServiceError {
    if e.code() == "ValidationException" {
        AwsServiceError::aws_error(e.status(), "ValidationError", e.message())
    } else {
        e
    }
}

fn sts_validate_string_length(
    field: &str,
    value: &str,
    min: usize,
    max: usize,
) -> Result<(), AwsServiceError> {
    validate_string_length(field, value, min, max).map_err(to_validation_error)
}

fn sts_validate_range_i64(
    field: &str,
    value: i64,
    min: i64,
    max: i64,
) -> Result<(), AwsServiceError> {
    validate_range_i64(field, value, min, max).map_err(to_validation_error)
}

/// Default duration for AssumeRole and similar operations (1 hour).
const DEFAULT_ASSUME_ROLE_DURATION: i64 = 3600;

/// Default duration for GetSessionToken (12 hours).
const DEFAULT_SESSION_TOKEN_DURATION: i64 = 43200;

/// Default duration for GetFederationToken (12 hours).
const DEFAULT_FEDERATION_TOKEN_DURATION: i64 = 43200;

/// Validate an optional inline session `Policy` parameter is a well-formed JSON
/// policy document (a JSON object). AWS rejects a non-JSON / non-object policy
/// with `MalformedPolicyDocument` before minting credentials; length-only
/// validation let garbage through.
pub(crate) fn validate_session_policy_json(policy: Option<&str>) -> Result<(), AwsServiceError> {
    if let Some(doc) = policy {
        let malformed = || {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedPolicyDocument",
                "The policy is not in the valid JSON format.".to_string(),
            )
        };
        let value: serde_json::Value = serde_json::from_str(doc).map_err(|_| malformed())?;
        if !value.is_object() {
            return Err(malformed());
        }
    }
    Ok(())
}

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
pub(super) fn format_expiration(ts: DateTime<Utc>) -> String {
    ts.format("%Y-%m-%dT%H:%M:%SZ").to_string()
}

/// Extract the assumed-role name (the final `/`-separated segment) from a role
/// ARN, matching how AWS names the assumed-role principal (the role path is
/// dropped). Shared by every assumed-role mint path so the name cannot drift.
pub(super) fn assumed_role_name(role_arn: &str) -> &str {
    role_arn.rsplit('/').next().unwrap_or("unknown")
}

/// Format the assumed-role principal ARN
/// (`arn:<partition>:sts::<account>:assumed-role/<role_name>/<session_name>`).
/// Single source of truth shared by every path that mints assumed-role
/// credentials (AssumeRole / WebIdentity / SAML in `assume.rs`, and the
/// container-credential endpoint in `container_creds.rs`) so the shape cannot
/// drift between them.
pub(super) fn format_assumed_role_arn(
    partition: &str,
    account_id: &str,
    role_name: &str,
    session_name: &str,
) -> String {
    format!("arn:{partition}:sts::{account_id}:assumed-role/{role_name}/{session_name}")
}

/// Enforce the AWS `RoleSessionName` pattern `[\w+=,.@-]*`. Without this, a
/// name containing `<`, `>` or `&` is interpolated raw into the AssumedRoleId /
/// assumed-role ARN in the XML response, producing malformed (and, for the
/// attacker-controlled SAML session name, injectable) XML that SDK parsers
/// reject. AWS rejects such input up front with a ValidationError.
fn validate_session_name(name: &str) -> Result<(), AwsServiceError> {
    if name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '+' | '=' | ',' | '.' | '@' | '-'))
    {
        return Ok(());
    }
    Err(AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationError",
        format!(
            "1 validation error detected: Value '{name}' at 'roleSessionName' failed to satisfy \
             constraint: Member must satisfy regular expression pattern: [\\w+=,.@-]*"
        ),
    ))
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
    /// Organization-membership oracle used to gate cross-account `AssumeRoot`.
    /// `None` in single-account setups / unit tests, in which case
    /// cross-account AssumeRoot is denied (no org topology to authorize it).
    org_membership: Option<Arc<dyn fakecloud_core::auth::OrgMembershipResolver>>,
}

mod assume;
mod caller;
pub mod container_creds;
mod federation;
mod session;

impl StsService {
    pub fn new(state: SharedIamState) -> Self {
        Self {
            state,
            snapshot_store: None,
            snapshot_lock: crate::persistence::new_snapshot_lock(),
            org_membership: None,
        }
    }

    /// Wire the organization-membership resolver used to authorize cross-account
    /// `AssumeRoot`. Without it, cross-account AssumeRoot is always denied.
    pub fn with_org_membership(
        mut self,
        resolver: Arc<dyn fakecloud_core::auth::OrgMembershipResolver>,
    ) -> Self {
        self.org_membership = Some(resolver);
        self
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

/// Partition-aware STS issuer URL for JWT `iss` claims. Mirrors the
/// `*.amazonaws.com.cn` quirk in China and the regional STS
/// endpoints AWS publishes in the GovCloud and ISO partitions.
pub(super) fn sts_issuer_url(region: &str) -> String {
    let suffix = match partition_for_region(region) {
        "aws-cn" => "amazonaws.com.cn",
        // GovCloud + ISO partitions still use `.amazonaws.com` for
        // their public STS endpoints today.
        _ => "amazonaws.com",
    };
    format!("https://sts.{region}.{suffix}")
}

/// Get the AWS partition from a region string.
fn partition_for_region(region: &str) -> &str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
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
    fn validate_session_policy_json_accepts_object_rejects_garbage() {
        // Absent policy is fine.
        assert!(validate_session_policy_json(None).is_ok());
        // A well-formed JSON object passes.
        assert!(
            validate_session_policy_json(Some(r#"{"Version":"2012-10-17","Statement":[]}"#))
                .is_ok()
        );
        // Non-JSON and non-object JSON are rejected as MalformedPolicyDocument.
        for bad in ["not json", "[]", "\"a string\"", "42"] {
            let err = validate_session_policy_json(Some(bad)).unwrap_err();
            assert_eq!(err.code(), "MalformedPolicyDocument", "input {bad:?}");
        }
    }

    #[test]
    fn session_name_pattern_accepts_valid_and_rejects_xml_metacharacters() {
        // AWS-legal characters pass.
        for ok in ["testuser", "role.session-1", "a+b=c,d.e@f", "user_123"] {
            assert!(validate_session_name(ok).is_ok(), "should accept {ok}");
        }
        // XML metacharacters (and anything else off-pattern) are rejected with
        // ValidationError rather than injected raw into the response XML.
        for bad in ["a<b", "a>b", "a&b", "a\"b", "a b", "a/b"] {
            let err = validate_session_name(bad).unwrap_err();
            assert_eq!(err.code(), "ValidationError", "should reject {bad:?}");
        }
    }

    #[test]
    fn test_partition_for_region() {
        assert_eq!(partition_for_region("us-east-1"), "aws");
        assert_eq!(partition_for_region("eu-west-1"), "aws");
        assert_eq!(partition_for_region("cn-north-1"), "aws-cn");
        assert_eq!(partition_for_region("cn-northwest-1"), "aws-cn");
        assert_eq!(partition_for_region("us-gov-west-1"), "aws-us-gov");
        assert_eq!(partition_for_region("us-gov-east-1"), "aws-us-gov");
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

    /// Test double for the org-membership oracle. `can_assume_root_into` is true
    /// only for the exact (caller, target) pairs it was seeded with.
    struct MockOrgMembership {
        allowed: Vec<(String, String)>,
    }

    impl fakecloud_core::auth::OrgMembershipResolver for MockOrgMembership {
        fn can_assume_root_into(&self, caller: &str, target: &str) -> bool {
            self.allowed.iter().any(|(c, t)| c == caller && t == target)
        }
    }

    fn org_membership_allowing(
        pairs: &[(&str, &str)],
    ) -> std::sync::Arc<dyn fakecloud_core::auth::OrgMembershipResolver> {
        std::sync::Arc::new(MockOrgMembership {
            allowed: pairs
                .iter()
                .map(|(c, t)| (c.to_string(), t.to_string()))
                .collect(),
        })
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

    /// Register a SAML provider in state so `AssumeRoleWithSAML` accepts its
    /// assertion. The metadata deliberately carries no `entityID`, so
    /// `expected_saml_audience` returns `None` and the audience check is
    /// skipped (mirroring a provider whose metadata declares no audience).
    fn register_saml_provider_in_state(state: &SharedIamState, arn: &str) {
        let mut accounts = state.write();
        let s = accounts.get_or_create("123456789012");
        s.saml_providers.insert(
            arn.to_string(),
            crate::state::SamlProvider {
                arn: arn.to_string(),
                name: arn.rsplit('/').next().unwrap_or("idp").to_string(),
                saml_metadata_document: "fake-saml-metadata-no-entity-id".to_string(),
                created_at: Utc::now(),
                valid_until: Utc::now() + chrono::Duration::days(365),
                tags: Vec::new(),
            },
        );
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

    #[tokio::test]
    async fn assume_role_duration_exceeding_max_session_is_rejected() {
        // The role's MaxSessionDuration is 3600; a DurationSeconds of 7200 is
        // within the generic 900..43200 range but over the role cap, so AWS
        // rejects it with ValidationError.
        let (svc, state) = make_sts_service();
        let role_arn = create_role_in_state(&state, "capped-role");
        let req = sts_request(
            "AssumeRole",
            vec![
                ("RoleArn", &role_arn),
                ("RoleSessionName", "sess"),
                ("DurationSeconds", "7200"),
            ],
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected ValidationError for over-cap DurationSeconds"),
        };
        assert_eq!(err.code(), "ValidationError");
        assert!(format!("{err:?}").contains("MaxSessionDuration"));
    }

    #[tokio::test]
    async fn assume_role_within_max_session_duration_succeeds() {
        let (svc, state) = make_sts_service();
        let role_arn = create_role_in_state(&state, "capped-role-ok");
        let req = sts_request(
            "AssumeRole",
            vec![
                ("RoleArn", &role_arn),
                ("RoleSessionName", "sess"),
                ("DurationSeconds", "3000"),
            ],
        );
        assert!(svc.handle(req).await.is_ok());
    }

    #[tokio::test]
    async fn assume_role_unknown_account_arn_does_not_create_phantom_account() {
        // A RoleArn naming an account that doesn't exist must be denied
        // WITHOUT materializing that account (unbounded map growth guard).
        let (svc, state) = make_sts_service();
        let req = sts_request(
            "AssumeRole",
            vec![
                ("RoleArn", "arn:aws:iam::999999999999:role/whatever"),
                ("RoleSessionName", "sess"),
            ],
        );
        assert!(svc.handle(req).await.is_err());
        assert!(
            state.read().get("999999999999").is_none(),
            "attacker-controlled RoleArn must not insert a phantom account"
        );
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

    #[tokio::test]
    async fn assume_role_with_web_identity_rejects_expired_token() {
        // A JWT whose `exp` is in the past must be rejected with
        // ExpiredTokenException, not mint fresh credentials (bug-audit
        // 2026-06-20, 5.1).
        use base64::Engine;
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRoleWithWebIdentity"}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "web-role", trust);

        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(br#"{"alg":"none"}"#);
        let payload_json = format!(
            r#"{{"iss":"https://example.com","aud":"client","sub":"u","exp":{}}}"#,
            chrono::Utc::now().timestamp() - 3600
        );
        let payload =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload_json.as_bytes());
        let token = format!("{header}.{payload}.sig");

        let req = sts_request(
            "AssumeRoleWithWebIdentity",
            vec![
                ("RoleArn", &role_arn),
                ("RoleSessionName", "web-session"),
                ("WebIdentityToken", &token),
            ],
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected ExpiredTokenException for an expired token"),
        };
        assert_eq!(err.code(), "ExpiredTokenException");
    }

    #[tokio::test]
    async fn assume_role_with_web_identity_nonexistent_role_denied() {
        // §5.2: a missing role must NOT fall through to credential minting.
        // AWS returns AccessDenied for AssumeRoleWithWebIdentity on a role
        // it cannot resolve.
        let (svc, _state) = make_sts_service();
        let req = sts_request(
            "AssumeRoleWithWebIdentity",
            vec![
                ("RoleArn", "arn:aws:iam::123456789012:role/does-not-exist"),
                ("RoleSessionName", "web-session"),
                ("WebIdentityToken", "fake-jwt-token"),
            ],
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected AccessDenied for phantom role, got success"),
        };
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
        assert!(
            format!("{err:?}").contains("AccessDenied"),
            "expected AccessDenied, got {err:?}"
        );
    }

    #[tokio::test]
    async fn assume_role_with_web_identity_uses_stable_role_id_prefix() {
        // The AssumedRoleId prefix must be the role's stable RoleId, not a
        // fresh random AROA on every assume (so it's identical across calls).
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"sts:AssumeRoleWithWebIdentity"}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "web-role", trust);
        let role_id = state
            .read()
            .get("123456789012")
            .unwrap()
            .roles
            .get("web-role")
            .unwrap()
            .role_id
            .clone();
        let expected = format!("<AssumedRoleId>{role_id}:web-session</AssumedRoleId>");

        for _ in 0..2 {
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
            assert!(
                body.contains(&expected),
                "AssumedRoleId prefix must be the role's stable RoleId; body = {body}"
            );
        }
    }

    // ── AssumeRoleWithSAML ──

    fn make_saml_assertion() -> String {
        use base64::Engine;
        // Minimal SAML XML carrying a RoleSessionName attribute value.
        let xml = r#"<saml:Assertion><saml:AttributeStatement><saml:Attribute Name="https://aws.amazon.com/SAML/Attributes/RoleSessionName"><saml:AttributeValue>saml-user</saml:AttributeValue></saml:Attribute></saml:AttributeStatement></saml:Assertion>"#;
        base64::engine::general_purpose::STANDARD.encode(xml.as_bytes())
    }

    #[tokio::test]
    async fn assume_role_with_saml_existing_role_succeeds() {
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":"arn:aws:iam::123456789012:saml-provider/idp"},"Action":"sts:AssumeRoleWithSAML"}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "saml-role", trust);
        register_saml_provider_in_state(&state, "arn:aws:iam::123456789012:saml-provider/idp");
        let assertion = make_saml_assertion();
        let req = sts_request(
            "AssumeRoleWithSAML",
            vec![
                ("RoleArn", &role_arn),
                (
                    "PrincipalArn",
                    "arn:aws:iam::123456789012:saml-provider/idp",
                ),
                ("SAMLAssertion", &assertion),
            ],
        );
        let resp = svc.handle(req).await.expect("existing role should succeed");
        let body = std::str::from_utf8(resp.body.expect_bytes()).unwrap();
        assert!(body.contains("<AccessKeyId>"));
    }

    #[tokio::test]
    async fn assume_role_with_saml_nonexistent_role_denied() {
        // §5.2: a missing role must NOT fall through to credential minting.
        let (svc, state) = make_sts_service();
        register_saml_provider_in_state(&state, "arn:aws:iam::123456789012:saml-provider/idp");
        let assertion = make_saml_assertion();
        let req = sts_request(
            "AssumeRoleWithSAML",
            vec![
                ("RoleArn", "arn:aws:iam::123456789012:role/does-not-exist"),
                (
                    "PrincipalArn",
                    "arn:aws:iam::123456789012:saml-provider/idp",
                ),
                ("SAMLAssertion", &assertion),
            ],
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected AccessDenied for phantom role, got success"),
        };
        assert_eq!(err.status(), StatusCode::FORBIDDEN);
        assert!(
            format!("{err:?}").contains("AccessDenied"),
            "expected AccessDenied, got {err:?}"
        );
    }

    #[tokio::test]
    async fn assume_role_with_saml_missing_audience_rejected() {
        // §5.7: when the provider metadata declares an audience (`entityID`),
        // an assertion with no matching audience claim must be rejected rather
        // than skipping the binding.
        let (svc, state) = make_sts_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":"arn:aws:iam::123456789012:saml-provider/idp"},"Action":"sts:AssumeRoleWithSAML"}]}"#;
        let role_arn = create_role_in_state_with_trust(&state, "saml-role", trust);
        // Provider metadata declaring an entityID -> expected audience.
        {
            let mut accounts = state.write();
            let s = accounts.get_or_create("123456789012");
            s.saml_providers.insert(
                "arn:aws:iam::123456789012:saml-provider/idp".to_string(),
                crate::state::SamlProvider {
                    arn: "arn:aws:iam::123456789012:saml-provider/idp".to_string(),
                    name: "idp".to_string(),
                    saml_metadata_document:
                        r#"<EntityDescriptor entityID="https://sp.example.com/saml">"#.to_string(),
                    created_at: Utc::now(),
                    valid_until: Utc::now() + chrono::Duration::days(365),
                    tags: Vec::new(),
                },
            );
        }
        // `make_saml_assertion` carries no <Audience> claim.
        let assertion = make_saml_assertion();
        let req = sts_request(
            "AssumeRoleWithSAML",
            vec![
                ("RoleArn", &role_arn),
                (
                    "PrincipalArn",
                    "arn:aws:iam::123456789012:saml-provider/idp",
                ),
                ("SAMLAssertion", &assertion),
            ],
        );
        let err = match svc.handle(req).await {
            Err(e) => e,
            Ok(_) => panic!("expected InvalidIdentityToken for missing audience"),
        };
        assert_eq!(err.status(), StatusCode::BAD_REQUEST);
        assert!(
            format!("{err:?}").contains("audience"),
            "expected audience-mismatch error, got {err:?}"
        );
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
        let provider_arn = "arn:aws:iam::123456789012:saml-provider/idp";
        register_saml_provider_in_state(&state, provider_arn);
        let saml_xml = r#"<?xml version="1.0"?><samlp:Response><Assertion><AttributeStatement><Attribute Name="https://aws.amazon.com/SAML/Attributes/RoleSessionName"><AttributeValue>jane</AttributeValue></Attribute></AttributeStatement></Assertion></samlp:Response>"#;
        let saml_b64 = base64::engine::general_purpose::STANDARD.encode(saml_xml);
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
    async fn assume_role_length_violation_uses_query_protocol_error_code() {
        // STS is awsQuery: a length-constraint violation must surface as
        // `ValidationError`, not the awsJson `ValidationException` the shared
        // validator emits (bug-hunt LOW error-code).
        let (svc, _) = make_sts_service();
        // RoleArn shorter than the 20-char minimum.
        let req = sts_request(
            "AssumeRole",
            vec![("RoleArn", "arn:short"), ("RoleSessionName", "sess")],
        );
        let err = svc
            .assume_role(&req)
            .err()
            .expect("too-short RoleArn errors");
        assert_eq!(err.code(), "ValidationError");
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
        let (svc, state) = make_sts_service();
        // AssumeRoot requires the RootSessions feature enabled (5.1) AND, for a
        // cross-account target, that the org topology authorizes it (5.2).
        state
            .write()
            .get_or_create("123456789012")
            .organizations_root_sessions = true;
        let svc =
            svc.with_org_membership(org_membership_allowing(&[("123456789012", "111122223333")]));
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
        let (svc, state) = make_sts_service();
        // AssumeRoot requires the RootSessions feature enabled (5.1) AND, for a
        // cross-account target, org authorization (5.2).
        state
            .write()
            .get_or_create("123456789012")
            .organizations_root_sessions = true;
        let svc =
            svc.with_org_membership(org_membership_allowing(&[("123456789012", "444455556666")]));
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
    async fn assume_root_denied_without_root_sessions() {
        // Without the centralized root-access RootSessions feature enabled,
        // AssumeRoot must be denied — previously it minted :root credentials
        // unconditionally (5.1).
        let (svc, _) = make_sts_service();
        let req = sts_request(
            "AssumeRoot",
            vec![
                ("TargetPrincipal", "arn:aws:iam::444455556666:root"),
                (
                    "TaskPolicyArn.arn",
                    "arn:aws:iam::aws:policy/IAMAuditRootUserCredentials",
                ),
            ],
        );
        let err = svc
            .assume_root(&req)
            .err()
            .expect("AssumeRoot must be denied without RootSessions");
        assert_eq!(err.code(), "AccessDeniedException");
    }

    #[tokio::test]
    async fn assume_root_cross_account_denied_when_org_disallows() {
        // §5.2: even with RootSessions enabled, a cross-account target that the
        // organization does not authorize (target not a member, or caller not
        // the management account) must be denied. Here the mock authorizes only
        // 555566667777, not the 444455556666 being requested.
        let (svc, state) = make_sts_service();
        state
            .write()
            .get_or_create("123456789012")
            .organizations_root_sessions = true;
        let svc =
            svc.with_org_membership(org_membership_allowing(&[("123456789012", "555566667777")]));
        let req = sts_request(
            "AssumeRoot",
            vec![
                ("TargetPrincipal", "arn:aws:iam::444455556666:root"),
                (
                    "TaskPolicyArn.arn",
                    "arn:aws:iam::aws:policy/IAMAuditRootUserCredentials",
                ),
            ],
        );
        let err = svc
            .assume_root(&req)
            .err()
            .expect("cross-account AssumeRoot must be denied when the org disallows it");
        assert_eq!(err.code(), "AccessDeniedException");
    }

    #[tokio::test]
    async fn assume_root_cross_account_denied_without_org_resolver() {
        // §5.2: with no org resolver wired (single-account setup), there is no
        // topology to authorize cross-account AssumeRoot, so it is denied even
        // with the RootSessions flag set.
        let (svc, state) = make_sts_service();
        state
            .write()
            .get_or_create("123456789012")
            .organizations_root_sessions = true;
        let req = sts_request(
            "AssumeRoot",
            vec![
                ("TargetPrincipal", "arn:aws:iam::444455556666:root"),
                (
                    "TaskPolicyArn.arn",
                    "arn:aws:iam::aws:policy/IAMAuditRootUserCredentials",
                ),
            ],
        );
        let err = svc
            .assume_root(&req)
            .err()
            .expect("cross-account AssumeRoot must be denied without an org resolver");
        assert_eq!(err.code(), "AccessDeniedException");
    }

    #[tokio::test]
    async fn assume_root_same_account_succeeds_without_root_sessions() {
        // The member root managing its OWN account does not require the
        // centralized RootSessions feature; this is the recorded AWS baseline
        // (conformance sts_assume_root). Only cross-account targets are gated.
        let (svc, _) = make_sts_service();
        let req = sts_request(
            "AssumeRoot",
            vec![
                ("TargetPrincipal", "123456789012"),
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
