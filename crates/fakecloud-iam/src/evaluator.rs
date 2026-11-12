//! Phase 1 IAM identity-policy evaluator.
//!
//! This module is a **pure function** over a set of policy documents and a
//! request: it does no I/O, no network, no state mutation, and never panics.
//! Dispatch (in batch 6) wires it up by collecting the principal's effective
//! policy set via [`collect_identity_policies`] and calling
//! [`evaluate`].
//!
//! # Phase 1 scope
//!
//! Implemented:
//! - `Effect: "Allow"` / `Effect: "Deny"` with **Deny precedence**: any
//!   matching `Deny` statement wins, regardless of how many `Allow`s match.
//! - `Action` / `NotAction` with `*` and `?` wildcards (case-insensitive
//!   service prefix match, case-sensitive action match — matches AWS).
//! - `Resource` / `NotResource` with `*` and `?` wildcards.
//! - Identity policies attached to users (inline + managed) and to groups
//!   the user belongs to.
//! - Identity policies attached to roles (inline + managed).
//! - Empty effective policy set → implicit deny.
//!
//! **Phase 2** — `Condition` block evaluation is now integrated via
//! [`crate::condition`]. A statement that carries a `Condition` is
//! evaluated against the [`RequestContext`] (populated at dispatch time);
//! the statement applies iff every operator entry matches. Unknown
//! operators / unknown keys / parse errors safe-fail to "statement does
//! not apply" with a `fakecloud::iam::audit` debug log, matching the
//! no-silent-accept rule from Phase 1.
//!
//! **Phase 3** — [`evaluate_with_gates`] and
//! [`evaluate_with_resource_policy_and_gates`] add intersection with
//! optional permission-boundary and session-policy layers. Each layer
//! is evaluated independently with the same matching logic; the final
//! decision requires every present layer to allow, and an explicit
//! `Deny` in any layer still wins.
//!
//! **Not** implemented (returns implicit deny rather than guessing — these
//! are tracked for future phases and documented on `/docs/reference/security`):
//! - Service control policies
//!
use std::collections::HashSet;

use fakecloud_core::auth::{Principal, PrincipalType};
use serde_json::Value;

use crate::condition::{CompiledCondition, ConditionContext};
use crate::state::IamState;

/// Request-time context keys used when evaluating `Condition` blocks.
///
/// This is a re-export of [`ConditionContext`] to keep the evaluator's
/// public API stable while centralizing the context definition in the
/// [`crate::condition`] module.
pub type RequestContext = ConditionContext;

/// The result of evaluating a request against a set of policies.
///
/// `Allow` requires at least one matching `Allow` statement and zero
/// matching `Deny` statements. `ExplicitDeny` indicates at least one
/// matching `Deny` statement (which takes precedence over any `Allow`).
/// `ImplicitDeny` is the catch-all for "no policy spoke to this request".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Allow,
    ImplicitDeny,
    ExplicitDeny,
}

impl Decision {
    /// Returns true if the request should be allowed.
    pub fn is_allow(self) -> bool {
        matches!(self, Decision::Allow)
    }
}

/// One IAM action to evaluate against a policy set.
///
/// `action` follows the canonical `service:Action` shape (e.g.
/// `s3:GetObject`, `sqs:SendMessage`). `resource` is a fully-qualified
/// AWS ARN; the per-service resource extractors in batches 6-8 produce
/// these.
///
/// `context` carries request-time condition keys (populated at dispatch)
/// used when evaluating statements with a `Condition` block.
#[derive(Debug, Clone)]
pub struct EvalRequest<'a> {
    pub principal: &'a Principal,
    pub action: String,
    pub resource: String,
    pub context: RequestContext,
}

/// Parsed view of a single statement within a policy document.
#[derive(Debug, Clone)]
pub(crate) struct ParsedStatement {
    pub effect: Effect,
    pub action: ActionMatch,
    pub resource: ResourceMatch,
    /// Compiled `Condition` block if the statement carried one. A
    /// statement with `Some(_)` only applies when the compiled block
    /// evaluates to `true` against the request's [`RequestContext`].
    pub condition: Option<CompiledCondition>,
    /// How this statement restricts which principals it applies to.
    /// Identity policies always parse as [`PrincipalPattern::None`];
    /// resource policies may carry a `Principal` or `NotPrincipal` key.
    pub principal: PrincipalPattern,
    /// Whether the statement's policy is `Version: 2012-10-17`, the only
    /// policy language that expands `${...}` policy variables.
    pub variables: bool,
}

/// `Principal` / `NotPrincipal` pattern on a parsed statement.
///
/// Identity policies never carry `Principal` — they inherit the
/// principal from the attaching identity. Resource policies (S3 bucket
/// policies in the initial Phase 2 rollout) use `Principal` to name
/// which users, accounts, or services the statement grants to.
#[derive(Debug, Clone)]
pub(crate) enum PrincipalPattern {
    /// Statement carried neither `Principal` nor `NotPrincipal`.
    /// Used by all identity-policy statements and by any resource-policy
    /// statement that forgets to name a principal (AWS rejects the
    /// latter at validation time, but the evaluator should not grant
    /// silently if it somehow makes it in).
    None,
    /// Statement carried `Principal` naming the accepted principals.
    /// A request is accepted iff it matches at least one entry.
    Principal(Vec<PrincipalRef>),
    /// Statement carried `NotPrincipal` naming the excluded principals.
    /// A statement with `NotPrincipal` applies to all callers **except**
    /// those matching any entry in the list — the inverse of `Principal`.
    /// If the caller matches ANY entry, the statement does NOT apply.
    /// If the caller matches NONE, the statement applies.
    ///
    /// An empty ref list (all entries were unrecognized principal types)
    /// causes the statement to be skipped with a debug log — we never
    /// silently grant by falling through to "matches everyone".
    NotPrincipal(Vec<PrincipalRef>),
}

/// A single principal reference parsed from a statement's `Principal`
/// key. AWS accepts several shapes; we implement the subset S3 bucket
/// policies actually use in practice.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PrincipalRef {
    /// `"Principal": "*"` or `"Principal": {"AWS": "*"}`. Matches any
    /// authenticated principal (including cross-account). The
    /// public-bucket idiom.
    AnyAws,
    /// `"Principal": {"AWS": "arn:aws:iam::ACCOUNT:root"}`. Matches any
    /// principal whose `account_id` equals `ACCOUNT`.
    AwsAccountRoot(String),
    /// `"Principal": {"AWS": "arn:aws:iam::ACCOUNT:user/name"}` (or
    /// `role/name`, `assumed-role/...`, etc). Matches a principal
    /// whose ARN equals this string exactly.
    AwsArn(String),
    /// `"Principal": {"Service": "lambda.amazonaws.com"}`. Matches a
    /// principal whose ARN was produced by the named service
    /// assuming a service-linked role (approximated by the role name
    /// including the service host, matching how AWS builds
    /// service-linked role ARNs).
    Service(String),
    /// `"Principal": {"Federated": "arn:aws:iam::ACCOUNT:saml-provider/Idp"}`
    /// or `{"Federated": "accounts.google.com"}` /
    /// `{"Federated": "cognito-identity.amazonaws.com"}`. Matches a
    /// federated principal whose ARN equals the named SAML/OIDC
    /// provider — STS sets the principal ARN to the provider when
    /// minting the trust-policy evaluation request for
    /// AssumeRoleWithSAML / AssumeRoleWithWebIdentity.
    Federated(String),
    /// `"Principal": {"CanonicalUser": "<id>"}` -- the S3 canonical user id
    /// form. The principals fakecloud gives a canonical user id to are
    /// CloudFront origin access identities (their `S3CanonicalUserId`,
    /// carried as the principal's `user_id`); see
    /// [`principal_is_canonical_user`].
    CanonicalUser(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Effect {
    Allow,
    Deny,
}

/// Action / NotAction patterns. `Allow` lists are positive matches;
/// `Deny` lists are negative matches (NotAction).
#[derive(Debug, Clone)]
pub(crate) enum ActionMatch {
    Action(Vec<String>),
    NotAction(Vec<String>),
}

/// Resource / NotResource patterns.
#[derive(Debug, Clone)]
pub(crate) enum ResourceMatch {
    Resource(Vec<String>),
    NotResource(Vec<String>),
    /// Statement omitted both `Resource` and `NotResource`. AWS treats
    /// this as "applies to all resources" only inside trust policies; for
    /// identity policies it's a validation error. We treat missing as
    /// wildcard-all to match how some Terraform-generated policies look
    /// in practice, but the evaluator never silently grants more than
    /// the policy text actually says — this maps to the same behavior
    /// as `Resource: ["*"]`.
    Implicit,
}

/// Parsed policy document — only the fields the evaluator needs. Any
/// statement that fails to parse (wrong shape, unknown effect, etc.) is
/// dropped with a warn-level log and the rest of the document is still
/// usable, matching how AWS behaves with invalid statements (the broken
/// statement is ignored, not the whole policy).
#[derive(Debug, Clone, Default)]
pub struct PolicyDocument {
    pub(crate) statements: Vec<ParsedStatement>,
}

impl PolicyDocument {
    /// Parse a policy document from its JSON string form. Returns an
    /// empty document on JSON errors so the caller can fall through to
    /// implicit-deny rather than panicking on malformed state.
    pub fn parse(json: &str) -> Self {
        let value: Value = match serde_json::from_str(json) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "failed to parse policy document JSON; ignoring");
                return Self::default();
            }
        };
        Self::from_value(&value)
    }

    /// Parse a policy document from a `serde_json::Value`. Used by both
    /// [`PolicyDocument::parse`] and tests that build inline `serde_json!`
    /// values.
    pub fn from_value(value: &Value) -> Self {
        let variables = value.get("Version").and_then(Value::as_str) == Some("2012-10-17");
        let statements = match value.get("Statement") {
            Some(Value::Array(arr)) => arr
                .iter()
                .filter_map(|s| parse_statement(s, variables))
                .collect::<Vec<_>>(),
            Some(obj @ Value::Object(_)) => parse_statement(obj, variables).into_iter().collect(),
            _ => Vec::new(),
        };
        Self { statements }
    }

    /// Number of parsed statements in this document. Used by tests as a
    /// proxy for "did this statement parse successfully?" without exposing
    /// the internal representation.
    pub fn statement_count(&self) -> usize {
        self.statements.len()
    }

    /// Count the identity-policy statements in this document that match the
    /// request's action + resource (and condition, if any) and carry the given
    /// effect (`allow` selects `Allow`, otherwise `Deny`). Used by policy
    /// simulation to attribute `MatchedStatements` provenance: a statement that
    /// contributed to the decision is reported with its source policy id.
    /// Statements carrying a `Principal`/`NotPrincipal` (resource-policy only)
    /// are never identity matches.
    pub fn matching_identity_statements(&self, request: &EvalRequest<'_>, allow: bool) -> usize {
        let want = if allow { Effect::Allow } else { Effect::Deny };
        self.statements
            .iter()
            .filter(|s| matches!(s.principal, PrincipalPattern::None))
            .filter(|s| s.effect == want)
            .filter(|s| action_matches(&s.action, &request.action))
            .filter(|s| resource_matches(s, &request.resource, &request.context))
            .filter(|s| {
                s.condition
                    .as_ref()
                    .is_none_or(|c| c.matches_for_effect(&request.context, !allow))
            })
            .count()
    }
}

fn parse_statement(value: &Value, variables: bool) -> Option<ParsedStatement> {
    let obj = value.as_object()?;
    let effect = match obj.get("Effect")?.as_str()? {
        "Allow" => Effect::Allow,
        "Deny" => Effect::Deny,
        other => {
            tracing::warn!(effect = other, "unknown Effect; ignoring statement");
            return None;
        }
    };
    let action = if let Some(a) = obj.get("Action") {
        ActionMatch::Action(coerce_string_list(a))
    } else if let Some(na) = obj.get("NotAction") {
        ActionMatch::NotAction(coerce_string_list(na))
    } else {
        tracing::warn!("statement has no Action or NotAction; ignoring");
        return None;
    };
    let resource = if let Some(r) = obj.get("Resource") {
        ResourceMatch::Resource(coerce_string_list(r))
    } else if let Some(nr) = obj.get("NotResource") {
        ResourceMatch::NotResource(coerce_string_list(nr))
    } else {
        ResourceMatch::Implicit
    };
    let condition = obj
        .get("Condition")
        .map(|c| CompiledCondition::parse(c).with_policy_variables(variables));
    let principal = if let Some(np) = obj.get("NotPrincipal") {
        PrincipalPattern::NotPrincipal(parse_principal(np))
    } else if let Some(p) = obj.get("Principal") {
        PrincipalPattern::Principal(parse_principal(p))
    } else {
        PrincipalPattern::None
    };
    Some(ParsedStatement {
        effect,
        action,
        resource,
        condition,
        principal,
        variables,
    })
}

/// Parse a `Principal` JSON value into the list of refs the evaluator
/// can match against a request principal.
///
/// AWS accepts any of:
/// - `"Principal": "*"`
/// - `"Principal": {"AWS": "*"}` or `{"AWS": ["..."]}`
/// - `"Principal": {"Service": "lambda.amazonaws.com"}` (string or array)
/// - `"Principal": {"Federated": "..."}` (matched via [`principal_is_federated`])
/// - `"Principal": {"CanonicalUser": "..."}` (matched via [`principal_is_canonical_user`])
///
/// Unknown shapes fall through to an empty ref list, which the matcher
/// treats as "doesn't match" — never silently grant. The drop is logged at
/// `warn` so callers can see when their policy uses an unsupported
/// principal type rather than discovering the silent skip in production.
fn parse_principal(value: &Value) -> Vec<PrincipalRef> {
    let mut out = Vec::new();
    match value {
        Value::String(s) if s == "*" => out.push(PrincipalRef::AnyAws),
        Value::String(other) => {
            tracing::warn!(
                target: "fakecloud::iam::audit",
                principal = %other,
                "Principal string other than \"*\" is not a recognized shape; statement will not match"
            );
        }
        Value::Object(map) => {
            for (key, v) in map {
                match key.as_str() {
                    "AWS" => {
                        for s in coerce_string_list(v) {
                            out.push(classify_aws_principal(&s));
                        }
                    }
                    "Service" => {
                        for s in coerce_string_list(v) {
                            out.push(PrincipalRef::Service(s));
                        }
                    }
                    "Federated" => {
                        for s in coerce_string_list(v) {
                            out.push(PrincipalRef::Federated(s));
                        }
                    }
                    "CanonicalUser" => {
                        for s in coerce_string_list(v) {
                            out.push(PrincipalRef::CanonicalUser(s));
                        }
                    }
                    other => {
                        tracing::warn!(
                            target: "fakecloud::iam::audit",
                            principal_type = %other,
                            "Principal type not recognized; entries dropped — statement \
                             will not match unless other Principal entries cover the caller"
                        );
                    }
                }
            }
        }
        _ => {
            tracing::warn!(
                target: "fakecloud::iam::audit",
                "Principal has an unexpected JSON shape; statement will not match"
            );
        }
    }
    out
}

fn classify_aws_principal(s: &str) -> PrincipalRef {
    if s == "*" {
        return PrincipalRef::AnyAws;
    }
    // `arn:<partition>:iam::<account>:root` → account root
    if let Some(rest) = s
        .strip_prefix("arn:")
        .and_then(|r| r.split_once(':'))
        .and_then(|(_partition, r)| r.strip_prefix("iam::"))
    {
        if let Some((account, tail)) = rest.split_once(':') {
            if tail == "root" && !account.is_empty() {
                return PrincipalRef::AwsAccountRoot(account.to_string());
            }
        }
    }
    // A bare 12-digit account ID is shorthand for `<account>:root`.
    if s.len() == 12 && s.chars().all(|c| c.is_ascii_digit()) {
        return PrincipalRef::AwsAccountRoot(s.to_string());
    }
    PrincipalRef::AwsArn(s.to_string())
}

/// Coerce a JSON value into a list of strings. AWS policy schema accepts
/// either a single string or an array of strings for `Action`/`Resource`.
/// Non-string entries are dropped.
fn coerce_string_list(value: &Value) -> Vec<String> {
    match value {
        Value::String(s) => vec![s.clone()],
        Value::Array(arr) => arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect(),
        _ => Vec::new(),
    }
}

/// Evaluate a request against a set of policy documents.
///
/// Implements AWS's standard identity-policy evaluation logic for Phase 1
/// features only. See the module-level docstring for the exhaustive list
/// of what is and isn't covered.
///
/// # Algorithm
///
/// 1. Walk every statement in every policy.
/// 2. For each statement that matches the request's action *and* resource:
///    - If the statement has a `Condition` block, evaluate it against
///      [`EvalRequest::context`]; skip the statement if the condition
///      does not match.
///    - If `Effect: Deny` → return [`Decision::ExplicitDeny`] immediately.
///    - If `Effect: Allow` → record that we saw an allow.
/// 3. After all statements are scanned: return [`Decision::Allow`] if any
///    allow matched, otherwise [`Decision::ImplicitDeny`].
pub fn evaluate(policies: &[PolicyDocument], request: &EvalRequest<'_>) -> Decision {
    evaluate_with_gates(policies, None, None, request)
}

/// Evaluate `request` against a single resource-style policy in
/// isolation — no identity-side gating. Use this for trust policies
/// (the only thing that gates `sts:AssumeRole`) and any other
/// scenario where the policy itself is the sole authorization source
/// and `Principal` matching is meaningful.
pub fn evaluate_resource_policy_only(
    policy: &PolicyDocument,
    request: &EvalRequest<'_>,
) -> Decision {
    evaluate_inner(std::slice::from_ref(policy), request, true)
}

/// Evaluate `request` against a principal's identity policies plus
/// optional permission-boundary, session-policy, and SCP layers.
///
/// Intersection semantics (applies identically to every gate):
///
/// - `boundary = None` / `session = None` / `scps = None` → the layer
///   is absent and does not gate the decision (pass-through).
/// - `Some(&[])` → the layer is present but empty, which evaluates to
///   `ImplicitDeny` and therefore denies the request. This is how
///   dangling boundary ARNs, empty session policies, and empty SCP
///   sets (e.g. every policy detached from a target) are represented.
/// - Any layer returning `ExplicitDeny` wins immediately (Deny
///   precedence applies across layers, not just within one).
/// - Otherwise the request is allowed iff **every present layer**
///   evaluates to `Allow`. A layer with `ImplicitDeny` caps the
///   intersection to `ImplicitDeny`.
///
/// When `scps` is `Some`, each document in the slice is treated as a
/// separate gate that must allow — the caller already assembled the
/// ordered list (root OU first, account-direct last) via
/// [`crate::scp_resolver`] or equivalent.
pub fn evaluate_with_gates(
    identity: &[PolicyDocument],
    boundary: Option<&[PolicyDocument]>,
    session: Option<&[PolicyDocument]>,
    request: &EvalRequest<'_>,
) -> Decision {
    evaluate_with_gates_and_scps(identity, boundary, session, None, request)
}

/// Full-chain variant of [`evaluate_with_gates`] that also applies an
/// SCP ceiling. See the top-of-module docs for the intersection
/// semantics. Batch 4 added this alongside the 4-arg form so existing
/// callers (and tests) don't have to thread an extra `None` through
/// every evaluation site.
pub fn evaluate_with_gates_and_scps(
    identity: &[PolicyDocument],
    boundary: Option<&[PolicyDocument]>,
    session: Option<&[PolicyDocument]>,
    scps: Option<&[PolicyDocument]>,
    request: &EvalRequest<'_>,
) -> Decision {
    let identity_decision = evaluate_inner(identity, request, false);
    intersect_layers(identity_decision, boundary, session, scps, request)
}

/// Evaluate only the permissions-boundary, session-policy and SCP ceilings
/// for `request`, as for a permission that does not come from an identity
/// policy (a KMS grant naming the caller): `Allow` when every present layer
/// allows (so with no layer the permission stands), `ExplicitDeny` when any
/// denies.
pub fn evaluate_ceiling(
    boundary: Option<&[PolicyDocument]>,
    session: Option<&[PolicyDocument]>,
    scps: Option<&[PolicyDocument]>,
    request: &EvalRequest<'_>,
) -> Decision {
    intersect_layers(Decision::Allow, boundary, session, scps, request)
}

/// Combine an already-computed identity-side decision with the optional
/// boundary, session-policy, and SCP layers. Factored out so the
/// resource-policy variant can apply the same intersection to the
/// identity side before OR/ANDing with the resource-policy side.
fn intersect_layers(
    identity_decision: Decision,
    boundary: Option<&[PolicyDocument]>,
    session: Option<&[PolicyDocument]>,
    scps: Option<&[PolicyDocument]>,
    request: &EvalRequest<'_>,
) -> Decision {
    if matches!(identity_decision, Decision::ExplicitDeny) {
        return Decision::ExplicitDeny;
    }
    // SCP gate sits at the top of the ceiling stack. Each SCP
    // document is a separate layer that must allow (AWS intersects
    // SCPs across the OU path). A single explicit Deny in any SCP
    // short-circuits the evaluation.
    let scp_decision = scps.map(|docs| evaluate_scp_chain(docs, request));
    if matches!(scp_decision, Some(Decision::ExplicitDeny)) {
        if let Some(scps_slice) = scps {
            tracing::debug!(
                target: "fakecloud::iam::audit",
                action = %request.action,
                principal_arn = %request.principal.arn,
                scp_count = scps_slice.len(),
                "SCP ceiling produced ExplicitDeny"
            );
        }
        return Decision::ExplicitDeny;
    }
    let boundary_decision = boundary.map(|policies| evaluate_inner(policies, request, false));
    if matches!(boundary_decision, Some(Decision::ExplicitDeny)) {
        return Decision::ExplicitDeny;
    }
    let session_decision = session.map(|policies| evaluate_inner(policies, request, false));
    if matches!(session_decision, Some(Decision::ExplicitDeny)) {
        return Decision::ExplicitDeny;
    }
    // Intersection: every present layer must allow.
    let identity_allows = matches!(identity_decision, Decision::Allow);
    let boundary_allows = boundary_decision
        .map(|d| matches!(d, Decision::Allow))
        .unwrap_or(true);
    let session_allows = session_decision
        .map(|d| matches!(d, Decision::Allow))
        .unwrap_or(true);
    let scp_allows = scp_decision
        .map(|d| matches!(d, Decision::Allow))
        .unwrap_or(true);
    if identity_allows && boundary_allows && session_allows && scp_allows {
        Decision::Allow
    } else {
        if scps.is_some() && !scp_allows {
            tracing::debug!(
                target: "fakecloud::iam::audit",
                action = %request.action,
                principal_arn = %request.principal.arn,
                "SCP ceiling did not allow action; capped to ImplicitDeny"
            );
        }
        Decision::ImplicitDeny
    }
}

/// Walk an ordered SCP chain (root OU -> descendant OUs -> account)
/// and intersect the per-document decisions. Each document is its own
/// gate: an explicit Deny anywhere wins, otherwise every document
/// must evaluate to Allow for the chain to allow.
fn evaluate_scp_chain(scps: &[PolicyDocument], request: &EvalRequest<'_>) -> Decision {
    if scps.is_empty() {
        // `Some(&[])` means the org exists and applies but no SCPs
        // are attached up the chain. Preserve AWS's deny-by-default
        // ceiling semantics: nothing allowed.
        return Decision::ImplicitDeny;
    }
    let mut all_allow = true;
    for doc in scps {
        match evaluate_inner(std::slice::from_ref(doc), request, false) {
            Decision::ExplicitDeny => return Decision::ExplicitDeny,
            Decision::Allow => {}
            Decision::ImplicitDeny => all_allow = false,
        }
    }
    if all_allow {
        Decision::Allow
    } else {
        Decision::ImplicitDeny
    }
}

/// Evaluate `request` against the principal's identity policies and an
/// optional resource-based policy, combining the two with AWS's
/// cross-account semantics.
///
/// - Either side returning an explicit `Deny` wins immediately.
/// - Same-account (`principal.account_id == resource_account_id`):
///   the request is allowed if identity OR resource grants it.
/// - Cross-account: the request is allowed only if identity AND
///   resource both grant it.
///
/// `resource_account_id` is the 12-digit account that owns the target
/// resource. For S3 bucket policies, dispatch parses this from the
/// resource ARN; S3 ARNs have an empty account field, so the caller
/// is expected to fall back to the server's configured account ID in
/// that case (#381 multi-account alignment).
pub fn evaluate_with_resource_policy(
    identity_policies: &[PolicyDocument],
    resource_policy: Option<&PolicyDocument>,
    request: &EvalRequest<'_>,
    resource_account_id: &str,
) -> Decision {
    evaluate_with_resource_policy_and_gates(
        identity_policies,
        None,
        None,
        resource_policy,
        request,
        resource_account_id,
    )
}

/// Resource-policy variant of [`evaluate_with_gates`].
///
/// The boundary and session policies gate the **identity side** only —
/// they never apply to the resource-policy branch. Rationale: the
/// resource policy is evaluated in the resource's account, and a
/// caller's permission boundary has no authority in another account
/// (this is also how AWS describes it). That shows up here as two
/// separate combinators:
///
/// - Same-account: `(identity ∩ boundary ∩ session) OR resource`.
///   Boundary/session cap the identity side, but a resource-policy
///   grant in the same account still allows the request on its own.
/// - Cross-account: `(identity ∩ boundary ∩ session) AND resource`.
///   Both sides must allow; boundary/session still cap the identity
///   side.
///
/// Explicit Deny from any layer — identity, boundary, session, or
/// resource — wins immediately.
pub fn evaluate_with_resource_policy_and_gates(
    identity_policies: &[PolicyDocument],
    boundary: Option<&[PolicyDocument]>,
    session: Option<&[PolicyDocument]>,
    resource_policy: Option<&PolicyDocument>,
    request: &EvalRequest<'_>,
    resource_account_id: &str,
) -> Decision {
    evaluate_with_resource_policy_and_gates_and_scps(
        identity_policies,
        boundary,
        session,
        None,
        resource_policy,
        request,
        resource_account_id,
    )
}

/// Full-chain variant of
/// [`evaluate_with_resource_policy_and_gates`] that also applies the
/// caller's SCP ceiling; see [`evaluate_with_resource_policy_for_role`].
pub fn evaluate_with_resource_policy_and_gates_and_scps(
    identity_policies: &[PolicyDocument],
    boundary: Option<&[PolicyDocument]>,
    session: Option<&[PolicyDocument]>,
    scps: Option<&[PolicyDocument]>,
    resource_policy: Option<&PolicyDocument>,
    request: &EvalRequest<'_>,
    resource_account_id: &str,
) -> Decision {
    evaluate_with_resource_policy_for_role(
        identity_policies,
        boundary,
        session,
        scps,
        resource_policy,
        None,
        request,
        resource_account_id,
    )
}

/// [`evaluate_with_resource_policy_and_gates_and_scps`] for a caller that
/// may be an assumed-role session of the role `role_arn` (its IAM ARN, path
/// included), which a resource policy can name to grant the role's sessions.
///
/// The SCPs cap every permission of a member-account principal, a resource
/// policy's included: AWS evaluates them before any resource-based policy,
/// and SCPs that do not allow the action are a final Deny (IAM user guide,
/// "How AWS enforcement code logic evaluates requests to allow or deny
/// access"). Within the same account, a resource-policy grant naming the
/// role ARN is also "limited by an implicit deny in a permissions boundary
/// or session policy", while one naming the IAM user or the role session
/// itself is not.
#[allow(clippy::too_many_arguments)]
pub fn evaluate_with_resource_policy_for_role(
    identity_policies: &[PolicyDocument],
    boundary: Option<&[PolicyDocument]>,
    session: Option<&[PolicyDocument]>,
    scps: Option<&[PolicyDocument]>,
    resource_policy: Option<&PolicyDocument>,
    role_arn: Option<&str>,
    request: &EvalRequest<'_>,
    resource_account_id: &str,
) -> Decision {
    let identity_raw = evaluate_inner(identity_policies, request, false);
    if matches!(identity_raw, Decision::ExplicitDeny) {
        return Decision::ExplicitDeny;
    }
    // Boundary, session and SCP gates on the identity side; an explicit
    // Deny in any of them is final.
    let identity_gated = intersect_layers(identity_raw, boundary, session, scps, request);
    if matches!(identity_gated, Decision::ExplicitDeny) {
        return Decision::ExplicitDeny;
    }
    let resource =
        resource_policy.map(|policy| ResourcePolicyDecision::new(policy, role_arn, request));
    if resource
        .as_ref()
        .is_some_and(|r| r.either_is(Decision::ExplicitDeny))
    {
        return Decision::ExplicitDeny;
    }
    // The SCP ceiling, past which no resource policy reaches.
    if scps.is_some_and(|docs| evaluate_scp_chain(docs, request) != Decision::Allow) {
        return Decision::ImplicitDeny;
    }
    // What a grant to the caller's role is limited by: the role's boundary
    // and the session's policies.
    let role_ceiling_allows =
        intersect_layers(Decision::Allow, boundary, session, None, request) == Decision::Allow;

    let same_account = request.principal.account_id == resource_account_id;
    // KMS keys are governed by their key policy. Unlike the generic
    // same-account `identity OR resource` rule, a KMS identity-policy grant
    // only takes effect when the key policy delegates to the account's IAM
    // (the default "Enable IAM permissions" root statement). A key policy that
    // neither names the principal directly nor delegates to the account root
    // makes identity grants powerless. bug-audit 2026-05-28, 5.5.
    // IAM actions are case-insensitive, so match the `kms:` service prefix
    // case-insensitively rather than letting a mixed-case `KMS:Decrypt` slip
    // past the key-policy delegation rule.
    let is_kms = request
        .action
        .split_once(':')
        .is_some_and(|(svc, _)| svc.eq_ignore_ascii_case("kms"));
    let Some(resource) = resource else {
        // No resource policy: identity alone decides in the same account; a
        // cross-account request needs the resource's policy.
        return if same_account {
            identity_gated
        } else {
            Decision::ImplicitDeny
        };
    };
    if same_account && is_kms {
        return evaluate_kms_same_account(&resource, identity_gated, role_ceiling_allows);
    }
    let identity_allows = matches!(identity_gated, Decision::Allow);
    let allowed = if same_account {
        identity_allows || resource.grants(role_ceiling_allows)
    } else {
        // The caller's boundary and session policies already cap the
        // identity side, which must allow as well.
        identity_allows && resource.grants(true)
    };
    if allowed {
        Decision::Allow
    } else {
        Decision::ImplicitDeny
    }
}

/// Evaluate a resource policy alone (as [`evaluate_resource_policy_only`])
/// for a caller that may be an assumed-role session of the role `role_arn`
/// (its IAM ARN, path included): a statement naming the role applies to the
/// session as one naming the session does. Every resource policy evaluated
/// for a calling identity goes through this, so a policy naming a role
/// grants (or denies) its sessions everywhere.
pub fn evaluate_resource_policy_for_role(
    policy: &PolicyDocument,
    role_arn: Option<&str>,
    request: &EvalRequest<'_>,
) -> Decision {
    let decision = ResourcePolicyDecision::new(policy, role_arn, request);
    if decision.either_is(Decision::ExplicitDeny) {
        Decision::ExplicitDeny
    } else if decision.either_is(Decision::Allow) {
        Decision::Allow
    } else {
        Decision::ImplicitDeny
    }
}

/// For an assumed-role session `principal` of an account whose IAM state is
/// `state`, the IAM ARN (path included) of the role it assumed: what a
/// resource policy names to grant the role's sessions, and the session's
/// `aws:PrincipalArn`.
pub fn session_role_arn(state: &IamState, principal: &Principal) -> Option<String> {
    if principal.principal_type != PrincipalType::AssumedRole {
        return None;
    }
    let name = role_name_from_assumed_role_arn(&principal.arn)?;
    state.roles.get(name).map(|role| role.arn.clone())
}

/// [`session_role_arn`] as a function over the shared IAM state, for
/// services that evaluate resource policies themselves (EventBridge bus
/// policies) without access to IAM's state.
pub type SessionRoleResolver = std::sync::Arc<dyn Fn(&Principal) -> Option<String> + Send + Sync>;

/// The [`SessionRoleResolver`] over `state`.
pub fn session_role_resolver(state: crate::state::SharedIamState) -> SessionRoleResolver {
    std::sync::Arc::new(move |principal: &Principal| {
        let accounts = state.read();
        session_role_arn(accounts.get(&principal.account_id)?, principal)
    })
}

/// A resource policy's decisions on a request, for the calling principal
/// and, for an assumed-role session, for the role it assumed (named by its
/// IAM ARN). The role is evaluated only when the policy can tell it apart
/// from the session: it names the role, or has a `NotPrincipal` statement.
/// Otherwise only account-wide entries could match the role, and those
/// match the session alike.
struct ResourcePolicyDecision<'p, 'r, 'a> {
    policy: &'p PolicyDocument,
    request: &'r EvalRequest<'a>,
    role: Option<Principal>,
    caller: Decision,
    role_decision: Decision,
}

impl<'p, 'r, 'a> ResourcePolicyDecision<'p, 'r, 'a> {
    fn new(
        policy: &'p PolicyDocument,
        role_arn: Option<&str>,
        request: &'r EvalRequest<'a>,
    ) -> Self {
        // The role, as the principal a statement naming it matches. Its
        // type is left unknown so that it matches no service or federated
        // principal.
        let role = role_arn
            .filter(|arn| policy_distinguishes_role(policy, arn))
            .map(|arn| Principal {
                arn: arn.to_string(),
                user_id: request.principal.user_id.clone(),
                account_id: request.principal.account_id.clone(),
                principal_type: PrincipalType::Unknown,
                source_identity: None,
                tags: request.principal.tags.clone(),
            });
        let mut decision = Self {
            policy,
            request,
            role,
            caller: Decision::ImplicitDeny,
            role_decision: Decision::ImplicitDeny,
        };
        decision.caller = decision.evaluate_caller(false);
        decision.role_decision = decision.evaluate_role(false);
        decision
    }

    fn evaluate_caller(&self, specific: bool) -> Decision {
        evaluate_inner_scoped(
            std::slice::from_ref(self.policy),
            self.request,
            true,
            specific,
        )
    }

    /// The decision for the role (`ImplicitDeny` when it is not evaluated);
    /// `specific` leaves out the account-wide entries (`"AWS": "*"`, the
    /// account root).
    fn evaluate_role(&self, specific: bool) -> Decision {
        let Some(role) = self.role.as_ref() else {
            return Decision::ImplicitDeny;
        };
        let as_role = EvalRequest {
            principal: role,
            action: self.request.action.clone(),
            resource: self.request.resource.clone(),
            context: self.request.context.clone(),
        };
        evaluate_inner_scoped(std::slice::from_ref(self.policy), &as_role, true, specific)
    }

    /// Whether the caller's or the role's decision is `decision`.
    fn either_is(&self, decision: Decision) -> bool {
        self.caller == decision || self.role_decision == decision
    }

    /// Whether the policy grants the request: to the caller itself, or to its
    /// role when what limits a grant to the role (`role_ceiling_allows`)
    /// allows.
    fn grants(&self, role_ceiling_allows: bool) -> bool {
        self.caller == Decision::Allow
            || (role_ceiling_allows && self.role_decision == Decision::Allow)
    }

    /// As [`Self::grants`], counting only entries naming the caller or its
    /// role, not its whole account (a KMS key policy's direct grant).
    fn grants_specifically(&self, role_ceiling_allows: bool) -> bool {
        self.evaluate_caller(true) == Decision::Allow
            || (role_ceiling_allows && self.evaluate_role(true) == Decision::Allow)
    }
}

/// Whether evaluating `policy` for the role `role_arn` can differ from
/// evaluating it for one of the role's sessions: a statement names the role,
/// or excludes principals with `NotPrincipal`.
fn policy_distinguishes_role(policy: &PolicyDocument, role_arn: &str) -> bool {
    policy.statements.iter().any(|s| match &s.principal {
        PrincipalPattern::NotPrincipal(_) => true,
        PrincipalPattern::Principal(refs) => refs
            .iter()
            .any(|r| matches!(r, PrincipalRef::AwsArn(arn) if arn == role_arn)),
        PrincipalPattern::None => false,
    })
}

/// Same-account KMS authorization. The key policy is the root of trust:
///
/// 1. An explicit `Deny` in the key policy denies (checked by the caller).
/// 2. A *direct* grant — the key policy names this specific principal (by ARN,
///    service, or federation, not the account-wide root entry) — allows on its
///    own. One naming the caller's role is limited by the role's permissions
///    boundary and the session's policies (`role_ceiling_allows`).
/// 3. Otherwise a key-policy `Allow` can only have come from the account-root /
///    `"AWS": "*"` delegation, which merely *enables* IAM: the request is
///    allowed only if an identity policy (already gated by boundary/session/SCP)
///    also allows it.
/// 4. A key policy that neither grants directly nor delegates denies, even if
///    identity policies allow.
///
/// The caller's SCPs were applied before: none of these reaches past them.
fn evaluate_kms_same_account(
    key_policy: &ResourcePolicyDecision<'_, '_, '_>,
    identity_gated: Decision,
    role_ceiling_allows: bool,
) -> Decision {
    if key_policy.grants_specifically(role_ceiling_allows) {
        return Decision::Allow;
    }
    // A non-direct key-policy Allow is the account-root delegation to IAM:
    // identity policies now decide. No delegation -> deny.
    if key_policy.either_is(Decision::Allow) && identity_gated == Decision::Allow {
        return Decision::Allow;
    }
    Decision::ImplicitDeny
}

fn evaluate_inner(
    policies: &[PolicyDocument],
    request: &EvalRequest<'_>,
    is_resource_policy: bool,
) -> Decision {
    evaluate_inner_scoped(policies, request, is_resource_policy, false)
}

/// As [`evaluate_inner`], but when `ignore_account_wide` is set, account-wide
/// resource-policy principals (`"AWS": "*"` / account root) do not match. KMS
/// evaluation uses this to isolate a direct grant of a specific principal from
/// the account-root delegation entry.
fn evaluate_inner_scoped(
    policies: &[PolicyDocument],
    request: &EvalRequest<'_>,
    is_resource_policy: bool,
    ignore_account_wide: bool,
) -> Decision {
    let mut allowed = false;
    for policy in policies {
        for statement in &policy.statements {
            // Principal / NotPrincipal gate. Identity policies never
            // carry these keys; resource policies must, and a
            // statement without a matching Principal does not apply.
            match &statement.principal {
                PrincipalPattern::None => {
                    if is_resource_policy {
                        // Resource-policy statement with no Principal
                        // does not apply — AWS treats this as a
                        // validation error and we will not silently
                        // grant.
                        tracing::debug!(
                            target: "fakecloud::iam::audit",
                            action = %request.action,
                            "resource policy statement has no Principal; skipping"
                        );
                        continue;
                    }
                }
                PrincipalPattern::Principal(refs) => {
                    if !principal_matches_scoped(refs, request.principal, ignore_account_wide) {
                        continue;
                    }
                }
                PrincipalPattern::NotPrincipal(refs) => {
                    if refs.is_empty() {
                        tracing::debug!(
                            target: "fakecloud::iam::audit",
                            action = %request.action,
                            "NotPrincipal has no recognized principal types; statement does not apply"
                        );
                        continue;
                    }
                    // NotPrincipal: statement applies when caller does NOT match any entry.
                    if principal_matches(refs, request.principal) {
                        continue;
                    }
                }
            }
            if !action_matches(&statement.action, &request.action) {
                continue;
            }
            if !resource_matches(statement, &request.resource, &request.context) {
                continue;
            }
            if let Some(condition) = &statement.condition {
                if !condition
                    .matches_for_effect(&request.context, matches!(statement.effect, Effect::Deny))
                {
                    tracing::debug!(
                        target: "fakecloud::iam::audit",
                        action = %request.action,
                        "condition did not match; statement does not apply"
                    );
                    continue;
                }
            }
            match statement.effect {
                Effect::Deny => return Decision::ExplicitDeny,
                Effect::Allow => allowed = true,
            }
        }
    }
    if allowed {
        Decision::Allow
    } else {
        Decision::ImplicitDeny
    }
}

/// Check whether any entry in a parsed `Principal` list matches the
/// calling principal. An empty list never matches — that's how we
/// keep unrecognized principal types from silently granting.
fn principal_matches(refs: &[PrincipalRef], principal: &Principal) -> bool {
    principal_matches_scoped(refs, principal, false)
}

/// As [`principal_matches`], but when `ignore_account_wide` is set the
/// account-wide entries (`"AWS": "*"` and `arn:aws:iam::<acct>:root`) do not
/// match. KMS evaluation uses this to tell a *direct* grant of a specific
/// principal apart from the default "Enable IAM" account-root delegation.
fn principal_matches_scoped(
    refs: &[PrincipalRef],
    principal: &Principal,
    ignore_account_wide: bool,
) -> bool {
    refs.iter().any(|r| match r {
        PrincipalRef::AnyAws => !ignore_account_wide,
        PrincipalRef::AwsAccountRoot(account) => {
            !ignore_account_wide && &principal.account_id == account
        }
        PrincipalRef::AwsArn(arn) => &principal.arn == arn,
        PrincipalRef::Service(service) => principal_is_service(principal, service),
        PrincipalRef::Federated(provider) => principal_is_federated(principal, provider),
        PrincipalRef::CanonicalUser(id) => principal_is_canonical_user(principal, id),
    })
}

/// Match a `"CanonicalUser"` principal. An S3 canonical user id is a
/// lowercase-hex string; the only principals fakecloud assigns one are
/// CloudFront origin access identities, which carry it as their `user_id`
/// (see `fakecloud_core::auth::InternalCaller::ServiceOwned`). IAM user and
/// role ids (`AIDA...`, `AROA...:session`) are never all-hex, so they can't
/// satisfy the comparison by accident.
fn principal_is_canonical_user(principal: &Principal, id: &str) -> bool {
    !id.is_empty()
        && id.bytes().all(|b| b.is_ascii_hexdigit())
        && principal.user_id.eq_ignore_ascii_case(id)
}

/// Match a `"Federated"` principal. STS injects the federated provider
/// (SAML provider ARN, OIDC issuer URL, or `cognito-identity.amazonaws.com`)
/// as the principal ARN when evaluating trust policies for
/// `AssumeRoleWithSAML` / `AssumeRoleWithWebIdentity`. We require the
/// principal to be of type `FederatedUser` and its ARN to equal the
/// provider — never silently grant.
fn principal_is_federated(principal: &Principal, provider: &str) -> bool {
    matches!(principal.principal_type, PrincipalType::FederatedUser) && principal.arn == provider
}

/// Approximate match for a `"Service"` principal. AWS represents a
/// request made by a service (e.g. Lambda invoking something via a
/// service-linked role) as an assumed-role principal whose role ARN
/// contains the service host. We match conservatively: the principal
/// must be an `AssumedRole` whose ARN contains the literal service
/// host string. False matches are avoided because unrelated role
/// names would have to happen to contain `lambda.amazonaws.com` —
/// unlikely in practice and never silently grant to user principals.
/// True when `arn` denotes the given AWS service principal — i.e. it is a
/// service-linked-role identity under the AWS-reserved `aws-service-role/
/// <service>/` path. The reserved path segment cannot be forged as an ordinary
/// role name, so this distinguishes a genuine service principal from a role a
/// user merely named after a service host.
pub(crate) fn arn_denotes_service(arn: &str, service: &str) -> bool {
    arn.contains(&format!("aws-service-role/{service}"))
}

fn principal_is_service(principal: &Principal, service: &str) -> bool {
    // Match the bare service host *anywhere* in the ARN would let a user create
    // a role literally named e.g. `lambda.amazonaws.com`, assume it, and have
    // its `assumed-role/lambda.amazonaws.com/...` ARN satisfy a
    // `Principal: { Service: lambda.amazonaws.com }` trust — a privilege
    // escalation. Require the reserved service-linked-role path instead.
    match principal.principal_type {
        // The service principal itself, acting on its own behalf (e.g.
        // CloudFront fetching an S3 origin through an origin access
        // control). Only fakecloud's in-process callers produce this type.
        PrincipalType::Service => principal.arn.eq_ignore_ascii_case(service),
        PrincipalType::AssumedRole => arn_denotes_service(&principal.arn, service),
        _ => false,
    }
}

fn action_matches(action: &ActionMatch, request_action: &str) -> bool {
    match action {
        ActionMatch::Action(patterns) => patterns
            .iter()
            .any(|p| iam_glob_match(p, request_action, true)),
        // An EMPTY NotAction must match NOTHING, not everything: `[].all(...)`
        // is vacuously true, so a degenerate `{Effect:Allow, NotAction:[],
        // Resource:*}` (which a resource policy with no put-time validation can
        // carry) became a public allow-all. AWS rejects such a policy; the safe
        // interpretation here is deny-by-default (bug-hunt 2026-06-24, 5.2).
        ActionMatch::NotAction(patterns) => {
            !patterns.is_empty()
                && patterns
                    .iter()
                    .all(|p| !iam_glob_match(p, request_action, true))
        }
    }
}

fn resource_matches(
    statement: &ParsedStatement,
    request_resource: &str,
    ctx: &fakecloud_core::auth::ConditionContext,
) -> bool {
    // A pattern with a policy variable that has no value matches no
    // resource -- in `Resource` and `NotResource` alike.
    let pattern_matches = |p: &str| {
        if statement.variables && crate::policy_variables::has_variables(p) {
            crate::policy_variables::resource_pattern(p, ctx).is_some_and(|pieces| {
                crate::policy_variables::glob(&pieces, request_resource, false)
            })
        } else {
            iam_glob_match(p, request_resource, false)
        }
    };
    match &statement.resource {
        ResourceMatch::Resource(patterns) => patterns.iter().any(|p| pattern_matches(p)),
        // Empty NotResource matches nothing (see action_matches, 5.2).
        ResourceMatch::NotResource(patterns) => {
            !patterns.is_empty() && patterns.iter().all(|p| !pattern_matches(p))
        }
        ResourceMatch::Implicit => true,
    }
}

/// IAM-style glob match supporting `*` (any sequence) and `?` (single
/// character). When `is_action` is true the pattern is an Action element
/// (`service:Action`), which AWS evaluates **case-insensitively** for BOTH the
/// service prefix and the action name — so `s3:deleteobject` and
/// `s3:DeleteObject` match. Resource (ARN) patterns pass `false` and are
/// matched case-sensitively.
fn iam_glob_match(pattern: &str, value: &str, is_action: bool) -> bool {
    if is_action {
        if let (Some((p_svc, p_act)), Some((v_svc, v_act))) =
            (pattern.split_once(':'), value.split_once(':'))
        {
            if !glob_match(&p_svc.to_ascii_lowercase(), &v_svc.to_ascii_lowercase()) {
                return false;
            }
            // AWS matches the action NAME case-insensitively too. Comparing it
            // verbatim let a mixed-case explicit Deny (e.g. `s3:deleteobject`)
            // silently miss `s3:DeleteObject` and become a no-op, so the
            // request fell through to an Allow.
            return glob_match(&p_act.to_ascii_lowercase(), &v_act.to_ascii_lowercase());
        }
    }
    glob_match(pattern, value)
}

/// Plain glob matcher with `*` (zero or more) and `?` (exactly one).
/// Iterative two-pointer implementation — runs in `O(pattern.len() *
/// value.len())` worst case, no backtracking explosions.
fn glob_match(pattern: &str, value: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let v: Vec<char> = value.chars().collect();
    let mut pi = 0usize;
    let mut vi = 0usize;
    let mut star: Option<usize> = None;
    let mut star_v: usize = 0;
    while vi < v.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == v[vi]) {
            pi += 1;
            vi += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            star_v = vi;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            star_v += 1;
            vi = star_v;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// Collect every identity policy that should be considered when
/// evaluating a request from `principal`.
///
/// Phase 1 walks identity policies only (user inline + managed, group
/// inline + managed via membership, role inline + managed). Resource
/// policies, permission boundaries, and SCPs are not consulted —
/// see the module-level scope notes.
///
/// The returned vector is the **deduplicated** set of policy documents,
/// parsed and ready to feed into [`evaluate`]. Unknown managed policy
/// ARNs are skipped with a debug log.
pub fn collect_identity_policies(state: &IamState, principal: &Principal) -> Vec<PolicyDocument> {
    let mut docs = Vec::new();
    let mut seen_managed: HashSet<String> = HashSet::new();
    match principal.principal_type {
        PrincipalType::User => {
            if let Some(user_name) = user_name_from_arn(&principal.arn) {
                collect_user_policies(state, user_name, &mut docs, &mut seen_managed);
            }
        }
        PrincipalType::AssumedRole => {
            if let Some(role_name) = role_name_from_assumed_role_arn(&principal.arn) {
                collect_role_policies(state, role_name, &mut docs, &mut seen_managed);
            }
        }
        PrincipalType::Root => {
            // Root bypasses evaluation; the caller (dispatch) should
            // short-circuit via `Principal::is_root` before reaching here.
            // Returning an empty vec means an explicit `Allow` is required,
            // which is the safe default if a caller forgets to bypass.
        }
        PrincipalType::FederatedUser | PrincipalType::Unknown | PrincipalType::Service => {
            // No identity-policy story for these in Phase 1.
        }
    }
    docs
}

fn collect_user_policies(
    state: &IamState,
    user_name: &str,
    docs: &mut Vec<PolicyDocument>,
    seen_managed: &mut HashSet<String>,
) {
    if let Some(inline) = state.user_inline_policies.get(user_name) {
        for doc in inline.values() {
            docs.push(PolicyDocument::parse(doc));
        }
    }
    if let Some(arns) = state.user_policies.get(user_name) {
        for arn in arns {
            if !seen_managed.insert(arn.clone()) {
                continue;
            }
            if let Some(doc) = managed_policy_default_document(state, arn) {
                docs.push(PolicyDocument::parse(&doc));
            }
        }
    }
    // Group memberships: walk every group whose members include the user.
    for (group_name, group) in &state.groups {
        if !group.members.iter().any(|m| m == user_name) {
            continue;
        }
        for doc in group.inline_policies.values() {
            docs.push(PolicyDocument::parse(doc));
        }
        for arn in &group.attached_policies {
            if !seen_managed.insert(arn.clone()) {
                continue;
            }
            if let Some(doc) = managed_policy_default_document(state, arn) {
                docs.push(PolicyDocument::parse(&doc));
            }
        }
        let _ = group_name;
    }
}

fn collect_role_policies(
    state: &IamState,
    role_name: &str,
    docs: &mut Vec<PolicyDocument>,
    seen_managed: &mut HashSet<String>,
) {
    if let Some(inline) = state.role_inline_policies.get(role_name) {
        for doc in inline.values() {
            docs.push(PolicyDocument::parse(doc));
        }
    }
    if let Some(arns) = state.role_policies.get(role_name) {
        for arn in arns {
            if !seen_managed.insert(arn.clone()) {
                continue;
            }
            if let Some(doc) = managed_policy_default_document(state, arn) {
                docs.push(PolicyDocument::parse(&doc));
            }
        }
    }
}

/// Look up the permission-boundary policy document attached to
/// `principal`, if any.
///
/// Returns:
/// - `None` — the principal has no boundary set, OR the principal is
///   exempt from boundary evaluation (account root, service-linked
///   role, or an unhandled principal type like a federated user). The
///   caller should treat this as "boundary layer absent"
///   (pass-through) when calling [`evaluate_with_gates`].
/// - `Some(vec![])` — a boundary ARN is set but does not resolve to
///   a known managed policy (dangling ARN, or the user/role was found
///   but its boundary points at a deleted policy). The caller must
///   treat this as **deny-all** — matching AWS's behavior when a
///   permission boundary is deleted, the principal can no longer
///   perform any action until the boundary is re-attached or removed.
///   Emits a `fakecloud::iam::audit` debug log.
/// - `Some(vec![doc])` — the boundary resolves to a policy document.
///
/// Service-linked roles are detected by the `AWSServiceRoleFor` name
/// prefix (AWS rejects attaching boundaries to SLRs at the API layer
/// anyway; this is defense-in-depth).
pub fn collect_boundary_policies(
    state: &IamState,
    principal: &Principal,
) -> Option<Vec<PolicyDocument>> {
    if principal.is_root() {
        return None;
    }
    let boundary_arn = match principal.principal_type {
        PrincipalType::User => {
            let user_name = user_name_from_arn(&principal.arn)?;
            let user = state.users.get(user_name)?;
            user.permissions_boundary.clone()?
        }
        PrincipalType::AssumedRole => {
            let role_name = role_name_from_assumed_role_arn(&principal.arn)?;
            if role_name.starts_with("AWSServiceRoleFor") {
                // Service-linked roles are exempt from boundary
                // evaluation — AWS rejects attaching one at the API
                // layer, but if state has been force-injected we
                // still bypass to match documented semantics.
                return None;
            }
            let role = state.roles.get(role_name)?;
            role.permissions_boundary.clone()?
        }
        // No boundary story for root / federated / unknown.
        _ => return None,
    };
    match managed_policy_default_document(state, &boundary_arn) {
        Some(doc) => Some(vec![PolicyDocument::parse(&doc)]),
        None => {
            tracing::debug!(
                target: "fakecloud::iam::audit",
                principal_arn = %principal.arn,
                boundary_arn = %boundary_arn,
                "permission boundary ARN does not resolve to a known managed policy; denying all actions"
            );
            Some(Vec::new())
        }
    }
}

fn managed_policy_default_document(state: &IamState, arn: &str) -> Option<String> {
    if let Some(policy) = state.policies.get(arn) {
        return policy
            .versions
            .iter()
            .find(|v| v.is_default)
            .or_else(|| policy.versions.first())
            .map(|v| v.document.clone());
    }
    // AWS-managed policies (arn:aws:iam::aws:policy/*) are not stored in account
    // state; resolve their default document from the seeded catalog so attached
    // managed policies actually grant permissions under --iam soft|strict.
    crate::managed_policies::default_document(arn).map(str::to_owned)
}

/// Extract the bare `user_name` component from an IAM user ARN.
///
/// IAM users can be created with a non-default path (e.g. `/engineering/`),
/// which produces ARNs of the form
/// `arn:aws:iam::123456789012:user/engineering/alice`. `IamState` indexes
/// users by the bare name (`alice`), so returning the full
/// `engineering/alice` would silently miss the user and make
/// `collect_user_policies` return an empty set — the evaluator would then
/// issue an incorrect implicit deny for every pathed user.
/// (Identified by cubic on PR #392.)
fn user_name_from_arn(arn: &str) -> Option<&str> {
    let after = arn.rsplit_once(":user/").map(|(_, name)| name)?;
    // Bare name is the last segment; the rest is the path.
    Some(after.rsplit('/').next().unwrap_or(after))
}

pub(crate) fn role_name_from_assumed_role_arn(arn: &str) -> Option<&str> {
    // `arn:aws:sts::<account>:assumed-role/<role-name>/<session>`
    let after = arn.rsplit_once(":assumed-role/")?.1;
    Some(after.split('/').next().unwrap_or(after))
}

#[cfg(test)]
#[allow(clippy::cloned_ref_to_slice_refs)]
#[path = "evaluator_tests.rs"]
mod tests;
