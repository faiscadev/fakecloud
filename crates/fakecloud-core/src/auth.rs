//! Authentication and authorization primitives shared across services.
//!
//! This module defines the opt-in modes for SigV4 signature verification and
//! IAM policy enforcement, plus the reserved "root bypass" identity that
//! short-circuits both checks when enabled.
//!
//! Neither feature is enforced at this layer — the types are plumbed through
//! [`crate::dispatch::DispatchConfig`] and consulted later by dispatch and
//! service handlers once the corresponding batches land. See
//! `/docs/reference/security` (added in a later batch) for the user-facing
//! contract.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Utc};

/// Kind of principal a set of credentials resolves to.
///
/// Used to drive IAM policy evaluation (Phase 2) and the `GetCallerIdentity`
/// response shape. Inferred from the credential's storage path in
/// [`IamState`] and — for STS temporary credentials — from the ARN form
/// `arn:aws:sts::<account>:assumed-role/...` or `federated-user/...`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum PrincipalType {
    /// An IAM user access key (AKID created via `CreateAccessKey`).
    User,
    /// An assumed role session issued by `AssumeRole` /
    /// `AssumeRoleWithWebIdentity` / `AssumeRoleWithSAML`.
    AssumedRole,
    /// Credentials issued by `GetFederationToken` — i.e. a federated user.
    FederatedUser,
    /// The account root identity. Reserved for explicit `...:root` ARNs
    /// only; do not return this from a generic fallback because root
    /// principals bypass IAM enforcement (see `Principal::is_root`).
    Root,
    /// The ARN didn't match any known shape. Treated as a non-root,
    /// non-bypassable principal so a malformed or unexpected ARN can never
    /// silently grant elevated permissions during IAM evaluation.
    Unknown,
}

impl PrincipalType {
    pub fn as_str(self) -> &'static str {
        match self {
            PrincipalType::User => "user",
            PrincipalType::AssumedRole => "assumed-role",
            PrincipalType::FederatedUser => "federated-user",
            PrincipalType::Root => "root",
            PrincipalType::Unknown => "unknown",
        }
    }

    /// Classify a principal from its ARN. Returns [`PrincipalType::Unknown`]
    /// for ARNs that don't match any of the well-known principal shapes —
    /// **never** [`PrincipalType::Root`] as a fallback, because root
    /// bypasses IAM enforcement and silently treating malformed ARNs as
    /// root would let unexpected inputs grant elevated permissions
    /// (identified by cubic in PR #391 review).
    pub fn from_arn(arn: &str) -> Self {
        if arn.ends_with(":root") {
            PrincipalType::Root
        } else if arn.contains(":user/") {
            PrincipalType::User
        } else if arn.contains(":assumed-role/") {
            PrincipalType::AssumedRole
        } else if arn.contains(":federated-user/") {
            PrincipalType::FederatedUser
        } else {
            PrincipalType::Unknown
        }
    }
}

/// Identity of the caller making a request, once its credentials have been
/// resolved. Attached to [`crate::service::AwsRequest::principal`] so
/// handlers can make identity-based decisions without re-parsing the
/// Authorization header.
///
/// `account_id` is always sourced from the credential itself (via
/// [`CredentialResolver`]), never from global config — #381 note.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub arn: String,
    pub user_id: String,
    pub account_id: String,
    pub principal_type: PrincipalType,
    /// Optional source identity string, carried through from
    /// `AssumeRole`'s `SourceIdentity` parameter. Reserved for later
    /// batches that wire session policies and auditing.
    pub source_identity: Option<String>,
    /// Tags on the calling principal (IAM user or assumed role).
    /// Populated at credential-resolution time from `IamState`.
    /// Used for `aws:PrincipalTag/<key>` condition evaluation.
    pub tags: Option<HashMap<String, String>>,
}

impl Principal {
    /// Is this caller the account's root identity? Root bypasses IAM
    /// evaluation, matching AWS.
    pub fn is_root(&self) -> bool {
        matches!(self.principal_type, PrincipalType::Root) || self.arn.ends_with(":root")
    }
}

/// Credentials resolved from an access key ID.
///
/// Returned by [`CredentialResolver::resolve`]. Holds both the secret access
/// key (needed for SigV4 verification) and the resolved [`Principal`]
/// (needed for IAM enforcement and `GetCallerIdentity` consolidation).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedCredential {
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub principal: Principal,
    /// Session policies passed to the STS call that minted this credential.
    /// Empty for IAM user access keys.
    pub session_policies: Vec<String>,
    /// True iff the underlying STS credential was minted with MFA. Drives
    /// `aws:MultiFactorAuthPresent` for downstream IAM evaluation. Always
    /// false for raw IAM user access keys.
    pub mfa_present: bool,
    /// Wall-clock time at which the underlying STS credential was issued.
    /// Drives `aws:TokenIssueTime` and `aws:MultiFactorAuthAge` (the latter
    /// computed at evaluation time as `now - token_issued_at` when
    /// [`Self::mfa_present`] is true). `None` for raw IAM user access keys
    /// — AWS does not expose `aws:TokenIssueTime` for long-lived credentials.
    pub token_issued_at: Option<DateTime<Utc>>,
    /// `aws:FederatedProvider` — SAML provider ARN for AssumeRoleWithSAML,
    /// OIDC provider ARN for AssumeRoleWithWebIdentity. `None` for raw IAM
    /// user keys, plain AssumeRole, GetSessionToken, GetFederationToken.
    pub federated_provider: Option<String>,
}

impl ResolvedCredential {
    /// Convenience accessors for the flat fields batch 3 callers use. Kept
    /// as methods rather than re-adding the fields to avoid making the
    /// shape inconsistent with [`Principal`] itself.
    pub fn principal_arn(&self) -> &str {
        &self.principal.arn
    }

    pub fn user_id(&self) -> &str {
        &self.principal.user_id
    }

    pub fn account_id(&self) -> &str {
        &self.principal.account_id
    }
}

/// Abstraction over "given an access key ID, return the secret and resolved
/// principal." Implemented by the IAM crate against `IamState`; the core
/// crate depends only on the trait so there's no circular dependency.
///
/// Implementations must be cheap to clone-share via `Arc` and must be
/// thread-safe — dispatch calls them from an axum handler under a tokio
/// worker.
pub trait CredentialResolver: Send + Sync {
    /// Resolve `access_key_id` to its secret access key and principal.
    /// Returns `None` when the AKID is unknown or its underlying credential
    /// has expired.
    fn resolve(&self, access_key_id: &str) -> Option<ResolvedCredential>;
}

/// One IAM action that the dispatch layer should evaluate against the
/// caller's effective policy set.
///
/// Produced by [`crate::service::AwsService::iam_action_for`] on services
/// that opt into enforcement. The `resource` is a fully-qualified AWS ARN
/// built from `request.principal.account_id` so multi-account isolation
/// (#381) becomes a state-partitioning change rather than a cross-cutting
/// rewrite.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IamAction {
    /// IAM service prefix, e.g. `"s3"`, `"sqs"`, `"iam"`.
    pub service: &'static str,
    /// AWS action name, e.g. `"GetObject"`, `"SendMessage"`.
    pub action: &'static str,
    /// Fully-qualified ARN of the target resource.
    pub resource: String,
}

impl IamAction {
    /// Compose the canonical `service:Action` string the evaluator
    /// matches against.
    pub fn action_string(&self) -> String {
        format!("{}:{}", self.service, self.action)
    }
}

/// Result of evaluating a request against an identity's effective policy
/// set. Abstract over the concrete evaluator [`Decision`] in
/// `fakecloud-iam::evaluator` so `fakecloud-core` can consume it without
/// depending on `fakecloud-iam`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IamDecision {
    Allow,
    ImplicitDeny,
    ExplicitDeny,
}

impl IamDecision {
    pub fn is_allow(self) -> bool {
        matches!(self, IamDecision::Allow)
    }
}

/// Request-time values consulted when a policy statement carries a
/// `Condition` block. Populated at dispatch time from the resolved
/// [`Principal`] and the incoming HTTP request, then handed to
/// [`IamPolicyEvaluator::evaluate`].
///
/// Lives in `fakecloud-core` (not `fakecloud-iam`) so the trait can
/// reference it without creating a circular crate dependency. All
/// fields are optional — a missing field means the key wasn't knowable
/// at dispatch time, and any operator that references it safe-fails to
/// `false` (unless the operator carries the `IfExists` suffix, in which
/// case it evaluates to `true`, matching AWS).
///
/// The `service_keys` map is reserved for service-specific condition
/// keys (`s3:prefix`, `sqs:MessageAttribute`, …) which Phase 2 ships
/// empty; service-specific support lands in a follow-up batch without
/// a signature change.
#[derive(Debug, Clone, Default)]
pub struct ConditionContext {
    /// `aws:username` — username segment of an IAM user ARN, or `None`
    /// for assumed roles / federated users where AWS does not set the key.
    pub aws_username: Option<String>,
    /// `aws:userid` — the unique `AIDA...`/`AROA...` identifier.
    pub aws_userid: Option<String>,
    /// `aws:PrincipalArn` — full principal ARN.
    pub aws_principal_arn: Option<String>,
    /// `aws:PrincipalAccount` — 12-digit account ID sourced from the
    /// credential, not global config (#381 multi-account alignment).
    pub aws_principal_account: Option<String>,
    /// `aws:PrincipalType` — `"User"`, `"AssumedRole"`, etc.
    pub aws_principal_type: Option<String>,
    /// `aws:SourceIp` — remote address of the HTTP connection.
    pub aws_source_ip: Option<IpAddr>,
    /// `aws:CurrentTime` — evaluation timestamp (UTC).
    pub aws_current_time: Option<DateTime<Utc>>,
    /// `aws:EpochTime` — same moment as `aws_current_time` in seconds
    /// since the Unix epoch.
    pub aws_epoch_time: Option<i64>,
    /// `aws:SecureTransport` — `true` iff the request came in over TLS.
    pub aws_secure_transport: Option<bool>,
    /// `aws:RequestedRegion` — region extracted from SigV4 / config.
    pub aws_requested_region: Option<String>,
    /// `aws:MultiFactorAuthPresent` — true iff the caller supplied an
    /// MFA credential when minting the session (AssumeRole with
    /// SerialNumber + TokenCode, or a long-lived user credential
    /// re-asserted via STS GetSessionToken with MFA).
    pub aws_mfa_present: Option<bool>,
    /// `aws:MultiFactorAuthAge` — seconds since MFA was asserted on
    /// the session.
    pub aws_mfa_age_seconds: Option<i64>,
    /// `aws:CalledVia` — the chain of service principals that have
    /// re-invoked downstream services on the caller's behalf
    /// (e.g. `["cloudformation.amazonaws.com"]`). Multi-value key.
    pub aws_called_via: Vec<String>,
    /// `aws:SourceVpce` — VPC endpoint id when the request transited
    /// a VPC interface endpoint.
    pub aws_source_vpce: Option<String>,
    /// `aws:SourceVpc` — VPC id when the request originated inside a
    /// VPC.
    pub aws_source_vpc: Option<String>,
    /// `aws:VpcSourceIp` — private source IP inside the VPC (distinct
    /// from `aws:SourceIp` which is the public NAT/Edge IP).
    pub aws_vpc_source_ip: Option<IpAddr>,
    /// `aws:FederatedProvider` — `cognito-identity.amazonaws.com`,
    /// `accounts.google.com`, or the SAML-provider ARN, depending on
    /// how the credential was minted.
    pub aws_federated_provider: Option<String>,
    /// `aws:TokenIssueTime` — when the temporary credential
    /// underlying this session was issued (UTC).
    pub aws_token_issue_time: Option<DateTime<Utc>>,
    /// Service-specific keys (`s3:prefix`, `sqs:MessageAttribute`, …).
    pub service_keys: BTreeMap<String, Vec<String>>,
    /// `aws:ResourceTag/<key>` — tags on the target resource.
    /// Populated by [`crate::service::AwsService::resource_tags_for`].
    /// `None` means the service doesn't expose resource tags for ABAC.
    pub resource_tags: Option<HashMap<String, String>>,
    /// `aws:RequestTag/<key>` — tags sent in the request body/headers.
    /// Populated by [`crate::service::AwsService::request_tags_from`].
    /// Also drives `aws:TagKeys` (the list of request tag keys).
    pub request_tags: Option<HashMap<String, String>>,
    /// `aws:PrincipalTag/<key>` — tags on the calling IAM user or role.
    /// Populated from [`Principal::tags`] at dispatch time.
    pub principal_tags: Option<HashMap<String, String>>,
}

impl ConditionContext {
    /// Resolve a condition key (e.g. `"aws:username"`) to the list of
    /// context values. Returns `None` if the key is not populated.
    /// Key names are matched case-insensitively — AWS treats
    /// `aws:username` and `AWS:UserName` as the same key.
    pub fn lookup(&self, key: &str) -> Option<Vec<String>> {
        let lower = key.to_ascii_lowercase();
        let one = |s: &str| Some(vec![s.to_string()]);

        // ABAC tag-based keys: case-insensitive prefix, case-sensitive
        // tag key (the part after the slash). AWS treats "Environment"
        // and "environment" as distinct tag keys.
        //
        // Prefix lengths: "aws:resourcetag/" = 16, "aws:requesttag/" = 15,
        //                 "aws:principaltag/" = 17
        if lower.starts_with("aws:resourcetag/") {
            let tag_key = &key[16..]; // preserve original case
            return self
                .resource_tags
                .as_ref()
                .and_then(|tags| tags.get(tag_key))
                .map(|v| vec![v.clone()]);
        }
        if lower.starts_with("aws:requesttag/") {
            let tag_key = &key[15..];
            return self
                .request_tags
                .as_ref()
                .and_then(|tags| tags.get(tag_key))
                .map(|v| vec![v.clone()]);
        }
        if lower.starts_with("aws:principaltag/") {
            let tag_key = &key[17..];
            return self
                .principal_tags
                .as_ref()
                .and_then(|tags| tags.get(tag_key))
                .map(|v| vec![v.clone()]);
        }
        if lower == "aws:tagkeys" {
            return self
                .request_tags
                .as_ref()
                .map(|tags| tags.keys().cloned().collect());
        }

        match lower.as_str() {
            "aws:username" => self.aws_username.as_deref().and_then(one),
            "aws:userid" => self.aws_userid.as_deref().and_then(one),
            "aws:principalarn" => self.aws_principal_arn.as_deref().and_then(one),
            "aws:principalaccount" => self.aws_principal_account.as_deref().and_then(one),
            "aws:principaltype" => self.aws_principal_type.as_deref().and_then(one),
            "aws:sourceip" => self.aws_source_ip.map(|ip| vec![ip.to_string()]),
            "aws:currenttime" => self
                .aws_current_time
                .map(|t| vec![t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)]),
            "aws:epochtime" => self.aws_epoch_time.map(|e| vec![e.to_string()]),
            "aws:securetransport" => self.aws_secure_transport.map(|b| vec![b.to_string()]),
            "aws:requestedregion" => self.aws_requested_region.as_deref().and_then(one),
            "aws:multifactorauthpresent" => self.aws_mfa_present.map(|b| vec![b.to_string()]),
            "aws:multifactorauthage" => self.aws_mfa_age_seconds.map(|s| vec![s.to_string()]),
            "aws:calledvia" => {
                if self.aws_called_via.is_empty() {
                    None
                } else {
                    Some(self.aws_called_via.clone())
                }
            }
            "aws:sourcevpce" => self.aws_source_vpce.as_deref().and_then(one),
            "aws:sourcevpc" => self.aws_source_vpc.as_deref().and_then(one),
            "aws:vpcsourceip" => self.aws_vpc_source_ip.map(|ip| vec![ip.to_string()]),
            "aws:federatedprovider" => self.aws_federated_provider.as_deref().and_then(one),
            "aws:tokenissuetime" => self
                .aws_token_issue_time
                .map(|t| vec![t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)]),
            _ => {
                if let Some(vs) = self.service_keys.get(&lower) {
                    if vs.is_empty() {
                        None
                    } else {
                        Some(vs.clone())
                    }
                } else {
                    self.service_keys
                        .iter()
                        .find(|(k, _)| k.eq_ignore_ascii_case(key))
                        .map(|(_, vs)| vs.clone())
                }
            }
        }
    }
}

/// Abstraction over "given a principal, an action, and request-time
/// condition keys, say Allow / Deny". Implemented by `fakecloud-iam`
/// against `IamState` + the evaluator. Dispatch calls this for every
/// request when `FAKECLOUD_IAM != off` and the target service opts in.
pub trait IamPolicyEvaluator: Send + Sync {
    /// Evaluate `action` against the identity policies attached to
    /// `principal`, using `context` for `Condition` block resolution.
    /// `session_policies` are the raw JSON session-policy documents
    /// from the STS call that minted the caller's credential (empty
    /// for IAM user access keys). `scps` are the inherited SCP
    /// documents (root-OU first, account-direct last) that form the
    /// top-of-chain allow-list ceiling; `None` means no org exists
    /// for this principal or the principal is exempt (management,
    /// service-linked role) and the layer is a pass-through.
    fn evaluate(
        &self,
        principal: &Principal,
        action: &IamAction,
        context: &ConditionContext,
        session_policies: &[String],
        scps: Option<&[String]>,
    ) -> IamDecision;

    /// Evaluate with resource-policy + session-policy intersection.
    /// `scps` follows the same semantics as in [`Self::evaluate`].
    #[allow(clippy::too_many_arguments)]
    fn evaluate_with_resource_policy(
        &self,
        principal: &Principal,
        action: &IamAction,
        context: &ConditionContext,
        resource_policy_json: Option<&str>,
        resource_account_id: &str,
        session_policies: &[String],
        scps: Option<&[String]>,
    ) -> IamDecision;

    /// Evaluate `action` for an **anonymous** (unsigned) caller against a
    /// resource-based policy in isolation. Anonymous requests carry no
    /// identity, so the resource policy is the sole authorization source:
    /// the request is allowed only if the policy explicitly grants the
    /// action to a wildcard principal (`Principal:"*"` / `{"AWS":"*"}`).
    ///
    /// `resource_policy_json` is the raw policy document (S3 bucket policy
    /// today); `None` or a non-public policy yields [`IamDecision::ImplicitDeny`].
    /// ACL-based public grants are evaluated separately by the dispatcher
    /// via [`ResourcePolicyProvider::public_acl_allows`].
    ///
    /// The default implementation returns [`IamDecision::ImplicitDeny`] so
    /// evaluators that don't support anonymous access never silently grant.
    fn evaluate_anonymous(
        &self,
        _action: &IamAction,
        _context: &ConditionContext,
        _resource_policy_json: Option<&str>,
    ) -> IamDecision {
        IamDecision::ImplicitDeny
    }
}

/// Abstraction over "given a principal, return the inherited SCP
/// documents that form the top-of-chain allow-list ceiling for the
/// principal's account". Implemented by `fakecloud-organizations`.
///
/// Returning `None` means SCPs do not apply (no org exists for this
/// fakecloud process, or the principal is the management account, or
/// the principal is a service-linked role, or the account is not
/// enrolled in the organization). Dispatch plumbs the returned slice
/// straight into [`IamPolicyEvaluator`].
///
/// The ordered list puts root-OU-attached policies first, then each
/// descendant OU down to the account's parent, and account-direct
/// attachments last — the evaluator treats each entry as a separate
/// gate that must allow (intersection), matching AWS SCP semantics.
pub trait ScpResolver: Send + Sync {
    fn scps_for(&self, principal: &Principal) -> Option<Vec<String>>;
}

/// Abstraction over "does the organization topology permit `caller_account` to
/// mint centralized-root (`sts:AssumeRoot`) credentials for `target_account`".
/// Implemented by `fakecloud-organizations`, which owns the membership graph
/// the IAM/STS crate has no visibility into.
///
/// Returns `true` only when an organization exists, `target_account` is a
/// member of it, and `caller_account` is that org's management account (or a
/// registered delegated administrator for centralized root access). Any other
/// case — no org, target not enrolled, caller not privileged — returns
/// `false`, so a bare `sts:AssumeRoot` grant can no longer escalate to root
/// over an arbitrary account. Same-account AssumeRoot is handled by the caller
/// and never consults this resolver.
pub trait OrgMembershipResolver: Send + Sync {
    fn can_assume_root_into(&self, caller_account: &str, target_account: &str) -> bool;
}

/// Abstraction over "given a service + a fully-qualified resource ARN,
/// return the resource-based policy attached to that resource, if any."
///
/// Implemented by resource-owning services (S3 for bucket policies in
/// the initial rollout; SNS topic policies, KMS key policies, and
/// Lambda resource policies are separate future wirings) and plumbed
/// through [`crate::dispatch::DispatchConfig`] alongside
/// [`IamPolicyEvaluator`]. Dispatch fetches the policy for the target
/// resource and hands it to the evaluator so cross-account Allow/Deny
/// semantics can be computed.
///
/// Implementations must be cheap to clone-share via `Arc` and must be
/// thread-safe — dispatch calls them on every enforced request.
///
/// Returning `None` means "no resource policy attached / resource
/// doesn't exist / this provider doesn't handle that service." Returning
/// `Some(json)` yields the raw JSON document as stored by the
/// resource's CRUD handlers; parsing happens inside the evaluator so a
/// malformed document logs a debug audit event and falls through to
/// "no resource policy" rather than silently allowing.
pub trait ResourcePolicyProvider: Send + Sync {
    /// Fetch the resource-based policy document attached to
    /// `resource_arn` on `service`. Both arguments are lowercase-ish
    /// (`"s3"`, `"arn:aws:s3:::my-bucket"`); implementations should
    /// match the service prefix they own and return `None` for
    /// anything else so providers can be composed safely.
    fn resource_policy(&self, service: &str, resource_arn: &str) -> Option<String>;

    /// Resolve the 12-digit account that owns `resource_arn` on `service`,
    /// when the ARN itself does not carry it. S3 ARNs have an empty account
    /// field (`arn:aws:s3:::bucket`), so without this the dispatcher would
    /// fall back to the caller's account and treat every S3 request as
    /// same-account — letting account A reach account B's bucket without B's
    /// bucket policy granting it (bug-audit 2026-05-28, 5.3). Providers whose
    /// ARNs already carry the account (SQS/SNS/Lambda/…) return `None` and let
    /// the dispatcher parse it from the ARN. Default `None`.
    fn resource_owner_account(&self, _service: &str, _resource_arn: &str) -> Option<String> {
        None
    }

    /// Whether a **public-read ACL** on `resource_arn` grants `action` to
    /// an anonymous (unsigned) caller. Distinct from a bucket policy: S3
    /// ACLs are a separate grant surface, so an object/bucket with an
    /// `AllUsers` group grant is publicly readable even without a bucket
    /// policy. `action` is the bare AWS action name (`"GetObject"`,
    /// `"ListBucket"`, …).
    ///
    /// Implementations must honor `PublicAccessBlock` (a bucket with
    /// `IgnorePublicAcls` set is not public via ACL). Default `false` so
    /// providers that don't model ACLs never grant anonymous access.
    fn public_acl_allows(&self, _service: &str, _resource_arn: &str, _action: &str) -> bool {
        false
    }
}

/// Failure mode for IAM PassRole trust-policy validation.
///
/// Exists in `fakecloud-core` so service crates (Lambda, ECS, …) can
/// surface a wire-shaped error without taking a dependency on
/// `fakecloud-iam`. The server crate wires the concrete validator that
/// reads the IAM state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PassRoleError {
    /// No role with this ARN exists in the IAM state.
    RoleNotFound(String),
    /// Role exists but its `AssumeRolePolicyDocument` does not allow the
    /// service principal to call `sts:AssumeRole`. Real AWS returns
    /// `InvalidParameterValueException` in this shape.
    TrustPolicyDenies {
        role_arn: String,
        service_principal: String,
    },
    /// Role's `AssumeRolePolicyDocument` could not be parsed as JSON.
    InvalidTrustPolicy(String),
}

impl std::fmt::Display for PassRoleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RoleNotFound(arn) => write!(f, "role not found: {arn}"),
            Self::TrustPolicyDenies {
                role_arn,
                service_principal,
            } => write!(
                f,
                "Role's trust policy does not allow {service_principal} to assume the role: {role_arn}"
            ),
            Self::InvalidTrustPolicy(arn) => {
                write!(f, "invalid trust policy on role {arn}")
            }
        }
    }
}

impl std::error::Error for PassRoleError {}

/// Validator that checks whether a role can be passed to a given
/// service. Used by Lambda / ECS / EC2 etc. to reject `CreateFunction`,
/// `RegisterTaskDefinition`, etc. when the supplied role's trust policy
/// doesn't allow the service principal — matching the `iam:PassRole`
/// trust-side behavior real AWS enforces unconditionally (separate from
/// identity-policy `iam:PassRole`, which sits behind the IAM evaluator).
pub trait RoleTrustValidator: Send + Sync {
    fn validate(
        &self,
        account_id: &str,
        role_arn: &str,
        service_principal: &str,
    ) -> Result<(), PassRoleError>;
}

/// Composite [`ResourcePolicyProvider`] that delegates to a list of
/// sub-providers in order, returning the first `Some` hit.
///
/// Each concrete provider (`S3ResourcePolicyProvider`,
/// `SnsResourcePolicyProvider`, `LambdaResourcePolicyProvider`, …)
/// already gates on its own service prefix and returns `None` for
/// anything it doesn't own, so composition is short-circuit and
/// order-independent. Server bootstrap builds one of these holding
/// every resource-owning service and passes it to
/// [`crate::dispatch::DispatchConfig::resource_policy_provider`].
///
/// This is the extension point for future resource-owning services:
/// adding KMS key policies (or anything else) is a one-line push at
/// bootstrap, never a core-crate refactor.
pub struct MultiResourcePolicyProvider {
    providers: Vec<Arc<dyn ResourcePolicyProvider>>,
}

impl MultiResourcePolicyProvider {
    /// Build a composite from a list of providers.
    pub fn new(providers: Vec<Arc<dyn ResourcePolicyProvider>>) -> Self {
        Self { providers }
    }

    /// Shared constructor returning the composite as an
    /// `Arc<dyn ResourcePolicyProvider>`, matching the signature of
    /// `DispatchConfig::resource_policy_provider`.
    pub fn shared(
        providers: Vec<Arc<dyn ResourcePolicyProvider>>,
    ) -> Arc<dyn ResourcePolicyProvider> {
        Arc::new(Self::new(providers))
    }

    /// Number of sub-providers held by this composite. Used by tests.
    pub fn len(&self) -> usize {
        self.providers.len()
    }

    /// True when no sub-providers are registered.
    pub fn is_empty(&self) -> bool {
        self.providers.is_empty()
    }
}

impl ResourcePolicyProvider for MultiResourcePolicyProvider {
    fn resource_policy(&self, service: &str, resource_arn: &str) -> Option<String> {
        self.providers
            .iter()
            .find_map(|p| p.resource_policy(service, resource_arn))
    }

    fn resource_owner_account(&self, service: &str, resource_arn: &str) -> Option<String> {
        self.providers
            .iter()
            .find_map(|p| p.resource_owner_account(service, resource_arn))
    }

    fn public_acl_allows(&self, service: &str, resource_arn: &str, action: &str) -> bool {
        self.providers
            .iter()
            .any(|p| p.public_acl_allows(service, resource_arn, action))
    }
}

/// How IAM identity policies are evaluated for incoming requests.
///
/// Default is [`IamMode::Off`] — existing behavior, policies are stored but
/// never consulted. [`IamMode::Soft`] evaluates and logs denied decisions via
/// the `fakecloud::iam::audit` tracing target without failing the request, and
/// [`IamMode::Strict`] returns an `AccessDeniedException` in the protocol-
/// correct shape.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum IamMode {
    /// Do not evaluate IAM policies.
    #[default]
    Off,
    /// Evaluate policies and log audit events for denied requests, but allow
    /// the request to proceed.
    Soft,
    /// Evaluate policies and reject denied requests with `AccessDeniedException`.
    Strict,
}

impl IamMode {
    /// Returns true when policy evaluation should occur at all.
    pub fn is_enabled(self) -> bool {
        !matches!(self, IamMode::Off)
    }

    /// Returns true when denied decisions should fail the request.
    pub fn is_strict(self) -> bool {
        matches!(self, IamMode::Strict)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            IamMode::Off => "off",
            IamMode::Soft => "soft",
            IamMode::Strict => "strict",
        }
    }
}

impl fmt::Display for IamMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Parse error for [`IamMode`] from string.
#[derive(Debug)]
pub struct ParseIamModeError(String);

impl fmt::Display for ParseIamModeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid IAM mode `{}`; expected one of: off, soft, strict",
            self.0
        )
    }
}

impl std::error::Error for ParseIamModeError {}

impl FromStr for IamMode {
    type Err = ParseIamModeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "off" | "none" | "disabled" => Ok(IamMode::Off),
            "soft" | "audit" | "warn" => Ok(IamMode::Soft),
            "strict" | "enforce" | "deny" => Ok(IamMode::Strict),
            other => Err(ParseIamModeError(other.to_string())),
        }
    }
}

/// Reserved root-identity convention.
///
/// Any access key whose ID begins with `test` (case-insensitive) is treated as
/// the de-facto root bypass. This matches the long-standing community
/// convention used by LocalStack and Floci: `test`/`test` credentials should
/// always "just work" for local development.
///
/// When SigV4 verification or IAM enforcement is enabled, callers using a
/// bypass AKID skip both checks. We emit a one-time startup WARN whenever
/// enforcement is turned on so users understand that unsigned `test` clients
/// will silently receive positive results.
pub fn is_root_bypass(access_key_id: &str) -> bool {
    access_key_id
        .trim()
        .get(..4)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("test"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iam_mode_default_is_off() {
        assert_eq!(IamMode::default(), IamMode::Off);
        assert!(!IamMode::default().is_enabled());
    }

    #[test]
    fn iam_mode_from_str_accepts_primary_values() {
        assert_eq!(IamMode::from_str("off").unwrap(), IamMode::Off);
        assert_eq!(IamMode::from_str("soft").unwrap(), IamMode::Soft);
        assert_eq!(IamMode::from_str("strict").unwrap(), IamMode::Strict);
    }

    #[test]
    fn iam_mode_from_str_is_case_insensitive_and_trimmed() {
        assert_eq!(IamMode::from_str(" OFF ").unwrap(), IamMode::Off);
        assert_eq!(IamMode::from_str("Soft").unwrap(), IamMode::Soft);
        assert_eq!(IamMode::from_str("STRICT").unwrap(), IamMode::Strict);
    }

    #[test]
    fn iam_mode_from_str_accepts_aliases() {
        assert_eq!(IamMode::from_str("disabled").unwrap(), IamMode::Off);
        assert_eq!(IamMode::from_str("audit").unwrap(), IamMode::Soft);
        assert_eq!(IamMode::from_str("enforce").unwrap(), IamMode::Strict);
    }

    #[test]
    fn iam_mode_from_str_rejects_garbage() {
        assert!(IamMode::from_str("").is_err());
        assert!(IamMode::from_str("allow").is_err());
        assert!(IamMode::from_str("yes").is_err());
    }

    #[test]
    fn iam_mode_display_roundtrips() {
        for mode in [IamMode::Off, IamMode::Soft, IamMode::Strict] {
            assert_eq!(IamMode::from_str(&mode.to_string()).unwrap(), mode);
        }
    }

    #[test]
    fn iam_mode_flags() {
        assert!(!IamMode::Off.is_enabled());
        assert!(!IamMode::Off.is_strict());
        assert!(IamMode::Soft.is_enabled());
        assert!(!IamMode::Soft.is_strict());
        assert!(IamMode::Strict.is_enabled());
        assert!(IamMode::Strict.is_strict());
    }

    #[test]
    fn root_bypass_matches_test_prefix() {
        assert!(is_root_bypass("test"));
        assert!(is_root_bypass("TEST"));
        assert!(is_root_bypass("Test"));
        assert!(is_root_bypass("testAccessKey"));
        assert!(is_root_bypass("TESTAKIAIOSFODNN7EXAMPLE"));
    }

    #[test]
    fn root_bypass_does_not_panic_on_multibyte_input() {
        // Byte index 4 falls inside a multi-byte UTF-8 character; must not panic.
        assert!(!is_root_bypass("té"));
        assert!(!is_root_bypass("日本語キー"));
        assert!(!is_root_bypass("🔑🔑"));
    }

    #[test]
    fn principal_type_from_arn_classifies_known_shapes() {
        assert_eq!(
            PrincipalType::from_arn("arn:aws:iam::123456789012:user/alice"),
            PrincipalType::User
        );
        assert_eq!(
            PrincipalType::from_arn("arn:aws:sts::123456789012:assumed-role/R/s"),
            PrincipalType::AssumedRole
        );
        assert_eq!(
            PrincipalType::from_arn("arn:aws:sts::123456789012:federated-user/bob"),
            PrincipalType::FederatedUser
        );
        assert_eq!(
            PrincipalType::from_arn("arn:aws:iam::123456789012:root"),
            PrincipalType::Root
        );
    }

    #[test]
    fn principal_type_unparseable_is_unknown_not_root() {
        // Identified by cubic on PR #391: falling back to Root would let
        // malformed or unexpected ARNs bypass IAM enforcement, since
        // Principal::is_root short-circuits evaluation. The fallback must
        // be the non-bypassable Unknown variant.
        assert_eq!(
            PrincipalType::from_arn("not-an-arn"),
            PrincipalType::Unknown
        );
        assert_eq!(PrincipalType::from_arn(""), PrincipalType::Unknown);
        assert_eq!(
            PrincipalType::from_arn("arn:aws:iam::123456789012:something-weird"),
            PrincipalType::Unknown
        );

        // And a Principal built from an Unknown ARN must not be treated
        // as root for enforcement decisions.
        let p = Principal {
            arn: "garbage".to_string(),
            user_id: "x".to_string(),
            account_id: "123456789012".to_string(),
            principal_type: PrincipalType::Unknown,
            source_identity: None,
            tags: None,
        };
        assert!(!p.is_root());
    }

    #[test]
    fn principal_is_root_covers_root_type_and_arn_suffix() {
        let p = Principal {
            arn: "arn:aws:iam::123456789012:root".to_string(),
            user_id: "AIDAROOT".to_string(),
            account_id: "123456789012".to_string(),
            principal_type: PrincipalType::Root,
            source_identity: None,
            tags: None,
        };
        assert!(p.is_root());

        let user = Principal {
            arn: "arn:aws:iam::123456789012:user/alice".to_string(),
            user_id: "AIDAALICE".to_string(),
            account_id: "123456789012".to_string(),
            principal_type: PrincipalType::User,
            source_identity: None,
            tags: None,
        };
        assert!(!user.is_root());
    }

    #[test]
    fn resolved_credential_accessors_forward_to_principal() {
        let rc = ResolvedCredential {
            secret_access_key: "s".into(),
            session_token: None,
            principal: Principal {
                arn: "arn:aws:iam::123456789012:user/alice".into(),
                user_id: "AIDAALICE".into(),
                account_id: "123456789012".into(),
                principal_type: PrincipalType::User,
                source_identity: None,
                tags: None,
            },
            session_policies: Vec::new(),
            mfa_present: false,
            token_issued_at: None,
            federated_provider: None,
        };
        assert_eq!(rc.principal_arn(), "arn:aws:iam::123456789012:user/alice");
        assert_eq!(rc.user_id(), "AIDAALICE");
        assert_eq!(rc.account_id(), "123456789012");
    }

    #[test]
    fn root_bypass_rejects_non_test_keys() {
        assert!(!is_root_bypass(""));
        assert!(!is_root_bypass("   "));
        assert!(!is_root_bypass("AKIAIOSFODNN7EXAMPLE"));
        assert!(!is_root_bypass("FKIA123456"));
        assert!(!is_root_bypass("tes"));
        assert!(!is_root_bypass("tst"));
    }

    // --- MultiResourcePolicyProvider composite -------------------------

    /// Test provider that returns a canned document for one
    /// (service, arn) pair and `None` for everything else.
    struct FakeProvider {
        service: &'static str,
        arn: &'static str,
        policy: &'static str,
    }

    impl ResourcePolicyProvider for FakeProvider {
        fn resource_policy(&self, service: &str, resource_arn: &str) -> Option<String> {
            if service.eq_ignore_ascii_case(self.service) && resource_arn == self.arn {
                Some(self.policy.to_string())
            } else {
                None
            }
        }
    }

    fn fake(
        service: &'static str,
        arn: &'static str,
        policy: &'static str,
    ) -> Arc<dyn ResourcePolicyProvider> {
        Arc::new(FakeProvider {
            service,
            arn,
            policy,
        })
    }

    #[test]
    fn multi_provider_empty_always_returns_none() {
        let m = MultiResourcePolicyProvider::new(vec![]);
        assert!(m.is_empty());
        assert_eq!(m.len(), 0);
        assert_eq!(m.resource_policy("s3", "arn:aws:s3:::x"), None);
    }

    #[test]
    fn multi_provider_delegates_to_single_child() {
        let m = MultiResourcePolicyProvider::new(vec![fake("s3", "arn:aws:s3:::b", r#"{"v":1}"#)]);
        assert_eq!(m.len(), 1);
        assert_eq!(
            m.resource_policy("s3", "arn:aws:s3:::b").as_deref(),
            Some(r#"{"v":1}"#)
        );
        assert_eq!(m.resource_policy("s3", "arn:aws:s3:::missing"), None);
        assert_eq!(m.resource_policy("sns", "arn:aws:s3:::b"), None);
    }

    #[test]
    fn multi_provider_hits_first_matching_child() {
        let m = MultiResourcePolicyProvider::new(vec![
            fake("s3", "arn:aws:s3:::b", r#"{"v":"s3"}"#),
            fake("sns", "arn:aws:sns:us-east-1:123:t", r#"{"v":"sns"}"#),
        ]);
        assert_eq!(
            m.resource_policy("s3", "arn:aws:s3:::b").as_deref(),
            Some(r#"{"v":"s3"}"#)
        );
        assert_eq!(
            m.resource_policy("sns", "arn:aws:sns:us-east-1:123:t")
                .as_deref(),
            Some(r#"{"v":"sns"}"#)
        );
    }

    #[test]
    fn multi_provider_is_order_independent_when_services_differ() {
        // Because each concrete provider gates on its own service
        // prefix, swapping the order must never change the result.
        let children: Vec<Arc<dyn ResourcePolicyProvider>> = vec![
            fake("s3", "arn:aws:s3:::b", "s3-doc"),
            fake("sns", "arn:aws:sns:us-east-1:123:t", "sns-doc"),
            fake(
                "lambda",
                "arn:aws:lambda:us-east-1:123:function:f",
                "lam-doc",
            ),
        ];
        let forward = MultiResourcePolicyProvider::new(children.clone());
        let reversed = MultiResourcePolicyProvider::new({
            let mut v = children.clone();
            v.reverse();
            v
        });
        for (svc, arn) in [
            ("s3", "arn:aws:s3:::b"),
            ("sns", "arn:aws:sns:us-east-1:123:t"),
            ("lambda", "arn:aws:lambda:us-east-1:123:function:f"),
        ] {
            assert_eq!(
                forward.resource_policy(svc, arn),
                reversed.resource_policy(svc, arn),
                "service {svc}"
            );
        }
    }

    #[test]
    fn multi_provider_returns_none_for_unhandled_service() {
        let m = MultiResourcePolicyProvider::new(vec![fake("s3", "arn:aws:s3:::b", "doc")]);
        assert_eq!(
            m.resource_policy("kms", "arn:aws:kms:us-east-1:123:key/k"),
            None
        );
        assert_eq!(m.resource_policy("iam", "arn:aws:iam::123:role/r"), None);
    }

    #[test]
    fn multi_provider_shared_wraps_in_arc() {
        let arc = MultiResourcePolicyProvider::shared(vec![fake("s3", "arn:aws:s3:::b", "doc")]);
        assert_eq!(
            arc.resource_policy("s3", "arn:aws:s3:::b").as_deref(),
            Some("doc")
        );
    }

    // --- ABAC tag condition key lookup ------------------------------------

    #[test]
    fn lookup_mfa_present_emits_bool_string() {
        let ctx = ConditionContext {
            aws_mfa_present: Some(true),
            ..Default::default()
        };
        assert_eq!(
            ctx.lookup("aws:MultiFactorAuthPresent"),
            Some(vec!["true".to_string()])
        );
        let ctx = ConditionContext {
            aws_mfa_present: Some(false),
            ..Default::default()
        };
        assert_eq!(
            ctx.lookup("aws:multifactorauthpresent"),
            Some(vec!["false".to_string()])
        );
    }

    #[test]
    fn lookup_mfa_age_emits_seconds() {
        let ctx = ConditionContext {
            aws_mfa_age_seconds: Some(900),
            ..Default::default()
        };
        assert_eq!(
            ctx.lookup("aws:MultiFactorAuthAge"),
            Some(vec!["900".to_string()])
        );
    }

    #[test]
    fn lookup_called_via_returns_full_chain() {
        let ctx = ConditionContext {
            aws_called_via: vec![
                "cloudformation.amazonaws.com".to_string(),
                "lambda.amazonaws.com".to_string(),
            ],
            ..Default::default()
        };
        assert_eq!(
            ctx.lookup("aws:CalledVia"),
            Some(vec![
                "cloudformation.amazonaws.com".to_string(),
                "lambda.amazonaws.com".to_string(),
            ])
        );
    }

    #[test]
    fn lookup_called_via_empty_returns_none() {
        let ctx = ConditionContext::default();
        assert_eq!(ctx.lookup("aws:CalledVia"), None);
    }

    #[test]
    fn lookup_source_vpc_keys() {
        let ctx = ConditionContext {
            aws_source_vpc: Some("vpc-123".to_string()),
            aws_source_vpce: Some("vpce-456".to_string()),
            aws_vpc_source_ip: Some("10.0.1.5".parse::<IpAddr>().unwrap()),
            ..Default::default()
        };
        assert_eq!(
            ctx.lookup("aws:SourceVpc"),
            Some(vec!["vpc-123".to_string()])
        );
        assert_eq!(
            ctx.lookup("aws:SourceVpce"),
            Some(vec!["vpce-456".to_string()])
        );
        assert_eq!(
            ctx.lookup("aws:VpcSourceIp"),
            Some(vec!["10.0.1.5".to_string()])
        );
    }

    #[test]
    fn lookup_federated_provider_and_token_issue_time() {
        use chrono::TimeZone;
        let ctx = ConditionContext {
            aws_federated_provider: Some("cognito-identity.amazonaws.com".to_string()),
            aws_token_issue_time: Some(
                chrono::Utc.with_ymd_and_hms(2026, 4, 30, 12, 0, 0).unwrap(),
            ),
            ..Default::default()
        };
        assert_eq!(
            ctx.lookup("aws:FederatedProvider"),
            Some(vec!["cognito-identity.amazonaws.com".to_string()])
        );
        assert_eq!(
            ctx.lookup("aws:TokenIssueTime"),
            Some(vec!["2026-04-30T12:00:00Z".to_string()])
        );
    }

    fn abac_context() -> ConditionContext {
        ConditionContext {
            resource_tags: Some(
                [("Environment", "prod"), ("CostCenter", "42")]
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
            request_tags: Some(
                [("Project", "web"), ("Team", "platform")]
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
            principal_tags: Some(
                [("Department", "eng"), ("Role", "developer")]
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ),
            ..Default::default()
        }
    }

    #[test]
    fn lookup_resource_tag_case_sensitive_key() {
        let ctx = abac_context();
        assert_eq!(
            ctx.lookup("aws:ResourceTag/Environment"),
            Some(vec!["prod".to_string()])
        );
        // Different case -> different tag key -> None
        assert_eq!(ctx.lookup("aws:ResourceTag/environment"), None);
    }

    #[test]
    fn lookup_resource_tag_prefix_case_insensitive() {
        let ctx = abac_context();
        // Prefix is case-insensitive per AWS
        assert_eq!(
            ctx.lookup("AWS:resourcetag/Environment"),
            Some(vec!["prod".to_string()])
        );
        assert_eq!(
            ctx.lookup("Aws:RESOURCETAG/CostCenter"),
            Some(vec!["42".to_string()])
        );
    }

    #[test]
    fn lookup_request_tag() {
        let ctx = abac_context();
        assert_eq!(
            ctx.lookup("aws:RequestTag/Project"),
            Some(vec!["web".to_string()])
        );
        assert_eq!(ctx.lookup("aws:RequestTag/project"), None);
    }

    #[test]
    fn lookup_principal_tag() {
        let ctx = abac_context();
        assert_eq!(
            ctx.lookup("aws:PrincipalTag/Department"),
            Some(vec!["eng".to_string()])
        );
        assert_eq!(ctx.lookup("aws:PrincipalTag/department"), None);
    }

    #[test]
    fn lookup_tag_keys_returns_all_request_tag_keys() {
        let ctx = abac_context();
        let mut keys = ctx.lookup("aws:TagKeys").unwrap();
        keys.sort();
        assert_eq!(keys, vec!["Project", "Team"]);
    }

    #[test]
    fn lookup_tag_keys_case_insensitive() {
        let ctx = abac_context();
        assert!(ctx.lookup("AWS:TAGKEYS").is_some());
        assert!(ctx.lookup("aws:tagkeys").is_some());
    }

    #[test]
    fn lookup_tag_none_when_field_not_set() {
        let ctx = ConditionContext::default();
        assert_eq!(ctx.lookup("aws:ResourceTag/Foo"), None);
        assert_eq!(ctx.lookup("aws:RequestTag/Foo"), None);
        assert_eq!(ctx.lookup("aws:PrincipalTag/Foo"), None);
        assert_eq!(ctx.lookup("aws:TagKeys"), None);
    }

    #[test]
    fn lookup_tag_missing_key_returns_none() {
        let ctx = abac_context();
        assert_eq!(ctx.lookup("aws:ResourceTag/NonExistent"), None);
        assert_eq!(ctx.lookup("aws:RequestTag/NonExistent"), None);
        assert_eq!(ctx.lookup("aws:PrincipalTag/NonExistent"), None);
    }
}
