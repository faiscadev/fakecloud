//! Adapter that implements [`fakecloud_core::auth::IamPolicyEvaluator`]
//! over the shared IAM state + Phase 1 evaluator.
//!
//! Mirrors the shape of [`crate::credential_resolver`]: the trait lives in
//! `fakecloud-core`, the concrete implementation lives here, and dispatch
//! calls through the trait so the dependency edge points core -> iam only.

use std::sync::Arc;

use fakecloud_core::auth::{
    ConditionContext, IamAction, IamDecision, IamPolicyEvaluator, Principal, PrincipalType,
};

use crate::evaluator::{self, Decision, EvalRequest};
use crate::state::SharedIamState;

/// [`IamPolicyEvaluator`] backed by shared [`crate::state::IamState`].
#[derive(Clone)]
pub struct IamPolicyEvaluatorImpl {
    state: SharedIamState,
}

impl IamPolicyEvaluatorImpl {
    pub fn new(state: SharedIamState) -> Self {
        Self { state }
    }

    pub fn shared(state: SharedIamState) -> Arc<dyn IamPolicyEvaluator> {
        Arc::new(Self::new(state))
    }
}

impl IamPolicyEvaluator for IamPolicyEvaluatorImpl {
    fn evaluate(
        &self,
        principal: &Principal,
        action: &IamAction,
        context: &ConditionContext,
        session_policies: &[String],
        scps: Option<&[String]>,
    ) -> IamDecision {
        let states = self.state.read();
        let Some(state) = states.get(&principal.account_id) else {
            return IamDecision::ImplicitDeny;
        };
        let identity_policies = evaluator::collect_identity_policies(state, principal);
        let boundary = evaluator::collect_boundary_policies(state, principal);
        let session = parse_session_policies(session_policies);
        let scp_docs = parse_scp_policies(scps, principal);
        let request = EvalRequest {
            principal,
            action: action.action_string(),
            resource: action.resource.clone(),
            context: context.clone(),
        };
        decision_to_core(evaluator::evaluate_with_gates_and_scps(
            &identity_policies,
            boundary.as_deref(),
            session.as_deref(),
            scp_docs.as_deref(),
            &request,
        ))
    }

    fn evaluate_with_resource_policy(
        &self,
        principal: &Principal,
        action: &IamAction,
        context: &ConditionContext,
        resource_policy_json: Option<&str>,
        resource_account_id: &str,
        session_policies: &[String],
        scps: Option<&[String]>,
    ) -> IamDecision {
        let states = self.state.read();
        let Some(state) = states.get(&principal.account_id) else {
            return IamDecision::ImplicitDeny;
        };
        let identity_policies = evaluator::collect_identity_policies(state, principal);
        let boundary = evaluator::collect_boundary_policies(state, principal);
        let session = parse_session_policies(session_policies);
        let scp_docs = parse_scp_policies(scps, principal);
        let request = EvalRequest {
            principal,
            action: action.action_string(),
            resource: action.resource.clone(),
            context: context.clone(),
        };
        let resource_policy = resource_policy_json.map(evaluator::PolicyDocument::parse);
        decision_to_core(evaluator::evaluate_with_resource_policy_and_gates_and_scps(
            &identity_policies,
            boundary.as_deref(),
            session.as_deref(),
            scp_docs.as_deref(),
            resource_policy.as_ref(),
            &request,
            resource_account_id,
        ))
    }

    fn evaluate_anonymous(
        &self,
        action: &IamAction,
        context: &ConditionContext,
        resource_policy_json: Option<&str>,
    ) -> IamDecision {
        // Anonymous callers carry no identity; only the resource policy can
        // grant access, and only via a wildcard principal. Evaluate the
        // bucket policy in isolation against a synthetic anonymous principal
        // — `evaluate_resource_policy_only` matches `Principal:"*"`
        // (`PrincipalRef::AnyAws`) while an account-root or specific-ARN
        // grant fails to match the empty anonymous identity, exactly as AWS
        // treats unsigned requests.
        let Some(json) = resource_policy_json else {
            return IamDecision::ImplicitDeny;
        };
        let anonymous = anonymous_principal();
        let request = EvalRequest {
            principal: &anonymous,
            action: action.action_string(),
            resource: action.resource.clone(),
            context: context.clone(),
        };
        let policy = evaluator::PolicyDocument::parse(json);
        decision_to_core(evaluator::evaluate_resource_policy_only(&policy, &request))
    }
}

/// A synthetic principal standing in for an unsigned (anonymous) caller.
/// It has no account and an empty ARN, so only a wildcard `Principal:"*"`
/// resource-policy statement matches it — account-root and specific-ARN
/// grants do not, matching how real AWS authorizes anonymous requests.
fn anonymous_principal() -> Principal {
    Principal {
        arn: String::new(),
        user_id: String::new(),
        account_id: String::new(),
        principal_type: PrincipalType::Unknown,
        source_identity: None,
        tags: None,
    }
}

fn parse_session_policies(raw: &[String]) -> Option<Vec<evaluator::PolicyDocument>> {
    if raw.is_empty() {
        return None;
    }
    Some(
        raw.iter()
            .map(|doc| evaluator::PolicyDocument::parse(doc))
            .collect(),
    )
}

/// Parse the ordered SCP documents returned by the resolver into the
/// evaluator's `PolicyDocument` shape.
///
/// Returns `None` when the resolver indicated that SCPs don't apply
/// to this principal (no organization, management account, SLR, or
/// account not enrolled). Returns `Some(vec![])` when the resolver
/// returned an empty chain — the evaluator treats that as deny-all,
/// matching AWS's default when every SCP has been detached from every
/// target up the chain.
///
/// Individual JSON parse failures are skipped with a debug log on
/// `fakecloud::iam::audit` — the "no gaming" rule from the task brief
/// says we must never silently treat a broken SCP as "matches".
fn parse_scp_policies(
    raw: Option<&[String]>,
    principal: &Principal,
) -> Option<Vec<evaluator::PolicyDocument>> {
    let raw = raw?;
    let mut docs = Vec::with_capacity(raw.len());
    for (i, json) in raw.iter().enumerate() {
        // PolicyDocument::parse already logs a warn on malformed JSON
        // and returns an empty (implicit-deny) document. That makes
        // the evaluator deny the action rather than silently allow —
        // the safer direction for an SCP ceiling. Emit an audit
        // breadcrumb so operators can see which attachment is bad.
        if serde_json::from_str::<serde_json::Value>(json).is_err() {
            tracing::debug!(
                target: "fakecloud::iam::audit",
                principal_arn = %principal.arn,
                scp_index = i,
                "SCP JSON failed to parse; treating as implicit-deny document"
            );
        }
        docs.push(evaluator::PolicyDocument::parse(json));
    }
    Some(docs)
}

fn decision_to_core(decision: Decision) -> IamDecision {
    match decision {
        Decision::Allow => IamDecision::Allow,
        Decision::ImplicitDeny => IamDecision::ImplicitDeny,
        Decision::ExplicitDeny => IamDecision::ExplicitDeny,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{IamAccessKey, IamPolicy, IamState, IamUser, PolicyVersion, SharedIamState};
    use chrono::Utc;
    use fakecloud_core::auth::PrincipalType;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    fn principal() -> Principal {
        Principal {
            arn: "arn:aws:iam::123456789012:user/alice".to_string(),
            user_id: "AIDAALICE".to_string(),
            account_id: "123456789012".to_string(),
            principal_type: PrincipalType::User,
            source_identity: None,
            tags: None,
        }
    }

    fn setup() -> SharedIamState {
        let mut mas = MultiAccountState::<IamState>::new("123456789012", "us-east-1", "");
        let state = mas.get_or_create("123456789012");
        state.users.insert(
            "alice".into(),
            IamUser {
                user_name: "alice".into(),
                user_id: "AIDAALICE".into(),
                arn: "arn:aws:iam::123456789012:user/alice".into(),
                path: "/".into(),
                created_at: Utc::now(),
                tags: Vec::new(),
                permissions_boundary: None,
            },
        );
        state.access_keys.insert(
            "alice".into(),
            vec![IamAccessKey {
                access_key_id: "FKIAALICE".into(),
                secret_access_key: "s".into(),
                user_name: "alice".into(),
                status: "Active".into(),
                created_at: Utc::now(),
            }],
        );
        Arc::new(RwLock::new(mas))
    }

    #[test]
    fn allow_policy_produces_allow_decision() {
        let state = setup();
        state
            .write()
            .get_or_create("123456789012")
            .user_inline_policies
            .insert(
                "alice".into(),
                std::collections::BTreeMap::from([(
                    "AllowGet".into(),
                    r#"{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#
                        .into(),
                )]),
            );
        let eval = IamPolicyEvaluatorImpl::new(state);
        let action = IamAction {
            service: "s3",
            action: "GetObject",
            resource: "arn:aws:s3:::bucket/key".into(),
        };
        assert_eq!(
            eval.evaluate(
                &principal(),
                &action,
                &ConditionContext::default(),
                &[],
                None
            ),
            IamDecision::Allow
        );
    }

    #[test]
    fn explicit_deny_takes_precedence() {
        let state = setup();
        state.write().get_or_create("123456789012").user_inline_policies.insert(
            "alice".into(),
            std::collections::BTreeMap::from([
                (
                    "AllowAll".into(),
                    r#"{"Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#.into(),
                ),
                (
                    "DenyGet".into(),
                    r#"{"Statement":[{"Effect":"Deny","Action":"s3:GetObject","Resource":"*"}]}"#
                        .into(),
                ),
            ]),
        );
        let eval = IamPolicyEvaluatorImpl::new(state);
        let action = IamAction {
            service: "s3",
            action: "GetObject",
            resource: "arn:aws:s3:::bucket/key".into(),
        };
        assert_eq!(
            eval.evaluate(
                &principal(),
                &action,
                &ConditionContext::default(),
                &[],
                None
            ),
            IamDecision::ExplicitDeny
        );
    }

    fn insert_managed_policy(state: &mut IamState, arn: &str, document: &str) {
        state.policies.insert(
            arn.to_string(),
            IamPolicy {
                policy_name: arn.rsplit('/').next().unwrap_or("p").to_string(),
                policy_id: "ANPATEST".into(),
                arn: arn.to_string(),
                path: "/".into(),
                description: String::new(),
                created_at: Utc::now(),
                tags: Vec::new(),
                default_version_id: "v1".into(),
                versions: vec![PolicyVersion {
                    version_id: "v1".into(),
                    document: document.to_string(),
                    is_default: true,
                    created_at: Utc::now(),
                }],
                next_version_num: 2,
                attachment_count: 1,
            },
        );
    }

    fn s3_get_object_action() -> IamAction {
        IamAction {
            service: "s3",
            action: "GetObject",
            resource: "arn:aws:s3:::bucket/key".into(),
        }
    }

    fn s3_put_object_action() -> IamAction {
        IamAction {
            service: "s3",
            action: "PutObject",
            resource: "arn:aws:s3:::bucket/key".into(),
        }
    }

    #[test]
    fn boundary_caps_identity_allow_all() {
        // alice has a catch-all Allow inline policy but her boundary
        // only permits s3:GetObject → PutObject must be implicit-deny.
        let state = setup();
        {
            let mut mas = state.write();
            let s = mas.get_or_create("123456789012");
            s.user_inline_policies.insert(
                "alice".into(),
                std::collections::BTreeMap::from([(
                    "AllowAll".into(),
                    r#"{"Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#.into(),
                )]),
            );
            let boundary_arn = "arn:aws:iam::123456789012:policy/BoundaryReadOnly";
            insert_managed_policy(
                s,
                boundary_arn,
                r#"{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#,
            );
            s.users.get_mut("alice").unwrap().permissions_boundary = Some(boundary_arn.to_string());
        }
        let eval = IamPolicyEvaluatorImpl::new(state);
        assert_eq!(
            eval.evaluate(
                &principal(),
                &s3_get_object_action(),
                &ConditionContext::default(),
                &[],
                None,
            ),
            IamDecision::Allow
        );
        assert_eq!(
            eval.evaluate(
                &principal(),
                &s3_put_object_action(),
                &ConditionContext::default(),
                &[],
                None,
            ),
            IamDecision::ImplicitDeny
        );
    }

    #[test]
    fn boundary_explicit_deny_overrides_identity_allow() {
        let state = setup();
        {
            let mut mas = state.write();
            let s = mas.get_or_create("123456789012");
            s.user_inline_policies.insert(
                "alice".into(),
                std::collections::BTreeMap::from([(
                    "AllowAll".into(),
                    r#"{"Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#.into(),
                )]),
            );
            let boundary_arn = "arn:aws:iam::123456789012:policy/BoundaryDenyPut";
            insert_managed_policy(
                s,
                boundary_arn,
                r#"{"Statement":[{"Effect":"Deny","Action":"s3:PutObject","Resource":"*"}]}"#,
            );
            s.users.get_mut("alice").unwrap().permissions_boundary = Some(boundary_arn.to_string());
        }
        let eval = IamPolicyEvaluatorImpl::new(state);
        assert_eq!(
            eval.evaluate(
                &principal(),
                &s3_put_object_action(),
                &ConditionContext::default(),
                &[],
                None,
            ),
            IamDecision::ExplicitDeny
        );
    }

    #[test]
    fn dangling_boundary_arn_denies_all_actions() {
        // Boundary ARN set but the managed policy was deleted (or
        // never existed). Must deny every action, matching AWS.
        let state = setup();
        {
            let mut mas = state.write();
            let s = mas.get_or_create("123456789012");
            s.user_inline_policies.insert(
                "alice".into(),
                std::collections::BTreeMap::from([(
                    "AllowAll".into(),
                    r#"{"Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#.into(),
                )]),
            );
            s.users.get_mut("alice").unwrap().permissions_boundary =
                Some("arn:aws:iam::123456789012:policy/DoesNotExist".into());
        }
        let eval = IamPolicyEvaluatorImpl::new(state);
        assert_eq!(
            eval.evaluate(
                &principal(),
                &s3_get_object_action(),
                &ConditionContext::default(),
                &[],
                None,
            ),
            IamDecision::ImplicitDeny
        );
    }

    #[test]
    fn service_linked_role_bypasses_boundary() {
        // Even if state is force-injected with a boundary on an SLR,
        // the evaluator must not apply it — AWS exempts SLRs.
        let state = setup();
        {
            use crate::state::IamRole;
            let mut mas = state.write();
            let s = mas.get_or_create("123456789012");
            s.roles.insert(
                "AWSServiceRoleForLambda".into(),
                IamRole {
                    role_name: "AWSServiceRoleForLambda".into(),
                    role_id: "AROASLR".into(),
                    arn: "arn:aws:iam::123456789012:role/aws-service-role/lambda.amazonaws.com/AWSServiceRoleForLambda".into(),
                    path: "/aws-service-role/lambda.amazonaws.com/".into(),
                    assume_role_policy_document: "{}".into(),
                    description: None,
                    created_at: Utc::now(),
                    max_session_duration: 3600,
                    tags: Vec::new(),
                    permissions_boundary: Some(
                        "arn:aws:iam::123456789012:policy/ShouldBeIgnored".into(),
                    ),
                },
            );
            s.role_inline_policies.insert(
                "AWSServiceRoleForLambda".into(),
                std::collections::BTreeMap::from([(
                    "AllowAll".into(),
                    r#"{"Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#.into(),
                )]),
            );
        }
        let principal = Principal {
            arn: "arn:aws:sts::123456789012:assumed-role/AWSServiceRoleForLambda/session1"
                .to_string(),
            user_id: "AROASLR:session1".into(),
            account_id: "123456789012".into(),
            principal_type: PrincipalType::AssumedRole,
            source_identity: None,
            tags: None,
        };
        let eval = IamPolicyEvaluatorImpl::new(state);
        assert_eq!(
            eval.evaluate(
                &principal,
                &s3_get_object_action(),
                &ConditionContext::default(),
                &[],
                None,
            ),
            IamDecision::Allow
        );
    }

    #[test]
    fn anonymous_allowed_by_wildcard_resource_policy() {
        let eval = IamPolicyEvaluatorImpl::new(setup());
        let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#;
        assert_eq!(
            eval.evaluate_anonymous(
                &s3_get_object_action(),
                &ConditionContext::default(),
                Some(policy)
            ),
            IamDecision::Allow
        );
    }

    #[test]
    fn anonymous_denied_without_resource_policy() {
        let eval = IamPolicyEvaluatorImpl::new(setup());
        assert_eq!(
            eval.evaluate_anonymous(&s3_get_object_action(), &ConditionContext::default(), None),
            IamDecision::ImplicitDeny
        );
    }

    #[test]
    fn anonymous_denied_when_policy_grants_specific_account() {
        // A bucket policy that grants a named account root — not "*" — must not
        // let an anonymous (identity-less) caller through.
        let eval = IamPolicyEvaluatorImpl::new(setup());
        let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:root"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#;
        assert_eq!(
            eval.evaluate_anonymous(
                &s3_get_object_action(),
                &ConditionContext::default(),
                Some(policy)
            ),
            IamDecision::ImplicitDeny
        );
    }

    #[test]
    fn anonymous_explicit_deny_in_resource_policy() {
        let eval = IamPolicyEvaluatorImpl::new(setup());
        let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#;
        let decision = eval.evaluate_anonymous(
            &s3_get_object_action(),
            &ConditionContext::default(),
            Some(policy),
        );
        assert!(!decision.is_allow());
    }

    #[test]
    fn empty_policy_set_is_implicit_deny() {
        let state = setup();
        let eval = IamPolicyEvaluatorImpl::new(state);
        let action = IamAction {
            service: "s3",
            action: "GetObject",
            resource: "arn:aws:s3:::bucket/key".into(),
        };
        assert_eq!(
            eval.evaluate(
                &principal(),
                &action,
                &ConditionContext::default(),
                &[],
                None
            ),
            IamDecision::ImplicitDeny
        );
    }
}
