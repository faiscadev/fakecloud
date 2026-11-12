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
        decision_to_core(evaluator::evaluate_with_resource_policy_for_role(
            &identity_policies,
            boundary.as_deref(),
            session.as_deref(),
            scp_docs.as_deref(),
            resource_policy.as_ref(),
            evaluator::request_role_arn(state, principal, context).as_deref(),
            &request,
            resource_account_id,
        ))
    }

    fn evaluate_ceiling(
        &self,
        principal: &Principal,
        action: &IamAction,
        context: &ConditionContext,
        session_policies: &[String],
        scps: Option<&[String]>,
        with_boundary: bool,
    ) -> IamDecision {
        let session = parse_session_policies(session_policies);
        let scp_docs = parse_scp_policies(scps, principal);
        let boundary = with_boundary
            .then(|| {
                let states = self.state.read();
                states
                    .get(&principal.account_id)
                    .and_then(|state| evaluator::collect_boundary_policies(state, principal))
            })
            .flatten();
        let request = EvalRequest {
            principal,
            action: action.action_string(),
            resource: action.resource.clone(),
            context: context.clone(),
        };
        decision_to_core(evaluator::evaluate_ceiling(
            boundary.as_deref(),
            session.as_deref(),
            scp_docs.as_deref(),
            &request,
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

    fn evaluate_resource_policy_only(
        &self,
        principal: &Principal,
        action: &IamAction,
        context: &ConditionContext,
        resource_policy_json: Option<&str>,
    ) -> IamDecision {
        let Some(json) = resource_policy_json else {
            return IamDecision::ImplicitDeny;
        };
        let request = EvalRequest {
            principal,
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

    /// The ceiling a grant is capped by: the SCPs, session policies and (when
    /// asked) the permissions boundary alone, with no identity policy needed,
    /// and no layer at all leaving it be.
    #[test]
    fn ceiling_evaluates_only_scps_session_policies_and_boundary() {
        let state = setup();
        {
            let mut mas = state.write();
            let s = mas.get_or_create("123456789012");
            let boundary_arn = "arn:aws:iam::123456789012:policy/S3Only";
            insert_managed_policy(
                s,
                boundary_arn,
                r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#,
            );
            s.users.get_mut("alice").unwrap().permissions_boundary = Some(boundary_arn.to_string());
        }
        let eval = IamPolicyEvaluatorImpl::new(state);
        let action = IamAction {
            service: "kms",
            action: "Decrypt",
            resource: "arn:aws:kms:us-east-1:111111111111:key/k".into(),
        };
        let ceiling_with = |session: &[String], scps: Option<&[String]>, boundary: bool| {
            eval.evaluate_ceiling(
                &principal(),
                &action,
                &ConditionContext::default(),
                session,
                scps,
                boundary,
            )
        };
        // The boundary (s3 only) caps the grant only when asked.
        assert_eq!(ceiling_with(&[], None, true), IamDecision::ImplicitDeny);
        let ceiling =
            |session: &[String], scps: Option<&[String]>| ceiling_with(session, scps, false);
        let allow_kms =
            r#"{"Statement":[{"Effect":"Allow","Action":"kms:*","Resource":"*"}]}"#.to_string();
        let allow_s3 =
            r#"{"Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#.to_string();
        let deny_decrypt =
            r#"{"Statement":[{"Effect":"Deny","Action":"kms:Decrypt","Resource":"*"}]}"#
                .to_string();
        assert_eq!(ceiling(&[], None), IamDecision::Allow);
        assert_eq!(
            ceiling(&[], Some(std::slice::from_ref(&allow_kms))),
            IamDecision::Allow
        );
        assert_eq!(
            ceiling(&[], Some(std::slice::from_ref(&allow_s3))),
            IamDecision::ImplicitDeny
        );
        assert_eq!(
            ceiling(&[], Some(&[allow_kms.clone(), deny_decrypt.clone()])),
            IamDecision::ExplicitDeny
        );
        assert_eq!(
            ceiling(std::slice::from_ref(&allow_kms), None),
            IamDecision::Allow
        );
        assert_eq!(
            ceiling(std::slice::from_ref(&allow_s3), None),
            IamDecision::ImplicitDeny
        );
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

    const DIST_A: &str = "arn:aws:cloudfront::123456789012:distribution/EDFDVBD6EXAMPLE";
    const DIST_B: &str = "arn:aws:cloudfront::123456789012:distribution/EOTHERDIST0001";

    fn cloudfront_caller(source_arn: &str) -> fakecloud_core::auth::InternalCaller {
        fakecloud_core::auth::InternalCaller::Service {
            service: "cloudfront.amazonaws.com".into(),
            source_arn: source_arn.into(),
            source_account: "123456789012".into(),
        }
    }

    fn oai_caller(canonical: &str) -> fakecloud_core::auth::InternalCaller {
        fakecloud_core::auth::InternalCaller::ServiceOwned {
            arn: "arn:aws:iam::cloudfront:user/CloudFront Origin Access Identity E2QWRUHAPOMQZL"
                .into(),
            canonical_user_id: Some(canonical.into()),
            acting_account: "123456789012".into(),
        }
    }

    fn eval_caller(caller: &fakecloud_core::auth::InternalCaller, policy: &str) -> IamDecision {
        let context = ConditionContext {
            service_keys: caller.condition_keys(),
            ..Default::default()
        };
        IamPolicyEvaluatorImpl::new(setup()).evaluate_resource_policy_only(
            &caller.principal(),
            &s3_get_object_action(),
            &context,
            Some(policy),
        )
    }

    /// The bucket policy CDK's `S3BucketOrigin.withOriginAccessControl`
    /// writes: the CloudFront service principal, scoped to one distribution.
    fn oac_policy(source_arn: &str) -> String {
        format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"Service":"cloudfront.amazonaws.com"}},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{{"StringEquals":{{"AWS:SourceArn":"{source_arn}"}}}}}}]}}"#
        )
    }

    #[test]
    fn cloudfront_service_principal_allowed_for_its_own_distribution() {
        assert_eq!(
            eval_caller(&cloudfront_caller(DIST_A), &oac_policy(DIST_A)),
            IamDecision::Allow
        );
    }

    #[test]
    fn cloudfront_service_principal_denied_for_another_distribution() {
        assert_eq!(
            eval_caller(&cloudfront_caller(DIST_B), &oac_policy(DIST_A)),
            IamDecision::ImplicitDeny
        );
    }

    #[test]
    fn cloudfront_service_principal_matches_source_account_condition() {
        let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"cloudfront.amazonaws.com"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"aws:SourceAccount":"123456789012"},"Bool":{"aws:PrincipalIsAWSService":"true"}}}]}"#;
        assert_eq!(
            eval_caller(&cloudfront_caller(DIST_A), policy),
            IamDecision::Allow
        );
        let other_account = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"cloudfront.amazonaws.com"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*","Condition":{"StringEquals":{"aws:SourceAccount":"999999999999"}}}]}"#;
        assert_eq!(
            eval_caller(&cloudfront_caller(DIST_A), other_account),
            IamDecision::ImplicitDeny
        );
    }

    #[test]
    fn service_principal_is_not_matched_by_another_service_or_account_root() {
        let other_service = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"s3:GetObject","Resource":"*"}]}"#;
        assert_eq!(
            eval_caller(&cloudfront_caller(DIST_A), other_service),
            IamDecision::ImplicitDeny
        );
        // The source account's root grants that account's identities, not a
        // service acting for one of its resources.
        let account_root = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:root"},"Action":"s3:GetObject","Resource":"*"}]}"#;
        assert_eq!(
            eval_caller(&cloudfront_caller(DIST_A), account_root),
            IamDecision::ImplicitDeny
        );
    }

    #[test]
    fn service_principal_explicit_deny_wins() {
        let policy = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"Service":"cloudfront.amazonaws.com"}},"Action":"s3:GetObject","Resource":"*","Condition":{{"StringEquals":{{"AWS:SourceArn":"{DIST_A}"}}}}}},{{"Effect":"Deny","Principal":"*","Action":"s3:GetObject","Resource":"*"}}]}}"#
        );
        assert_eq!(
            eval_caller(&cloudfront_caller(DIST_A), &policy),
            IamDecision::ExplicitDeny
        );
    }

    #[test]
    fn origin_access_identity_matches_by_arn_or_canonical_user() {
        let canonical = "5f0b6c1a2e3d4c5b6a7f8e9d0c1b2a3f";
        let by_arn = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::cloudfront:user/CloudFront Origin Access Identity E2QWRUHAPOMQZL"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}]}"#;
        assert_eq!(
            eval_caller(&oai_caller(canonical), by_arn),
            IamDecision::Allow
        );
        let by_canonical = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"CanonicalUser":"{canonical}"}},"Action":"s3:GetObject","Resource":"arn:aws:s3:::bucket/*"}}]}}"#
        );
        assert_eq!(
            eval_caller(&oai_caller(canonical), &by_canonical),
            IamDecision::Allow
        );
        // A different OAI's canonical id does not match.
        assert_eq!(
            eval_caller(
                &oai_caller("0123456789abcdef0123456789abcdef"),
                &by_canonical
            ),
            IamDecision::ImplicitDeny
        );
        // Nor does the CloudFront service principal grant reach an OAI.
        assert_eq!(
            eval_caller(&oai_caller(canonical), &oac_policy(DIST_A)),
            IamDecision::ImplicitDeny
        );
    }

    #[test]
    fn canonical_user_never_matches_an_iam_user_id() {
        // IAM ids are not canonical ids, even when a policy names one.
        let eval = IamPolicyEvaluatorImpl::new(setup());
        let principal = Principal {
            arn: "arn:aws:iam::123456789012:user/alice".into(),
            user_id: "AIDAALICE".into(),
            account_id: "123456789012".into(),
            principal_type: PrincipalType::User,
            source_identity: None,
            tags: None,
        };
        let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"CanonicalUser":"AIDAALICE"},"Action":"s3:GetObject","Resource":"*"}]}"#;
        assert_eq!(
            eval.evaluate_resource_policy_only(
                &principal,
                &s3_get_object_action(),
                &ConditionContext::default(),
                Some(policy)
            ),
            IamDecision::ImplicitDeny
        );
    }

    #[test]
    fn service_principal_denied_without_resource_policy() {
        let caller = cloudfront_caller(DIST_A);
        assert_eq!(
            IamPolicyEvaluatorImpl::new(setup()).evaluate_resource_policy_only(
                &caller.principal(),
                &s3_get_object_action(),
                &ConditionContext::default(),
                None
            ),
            IamDecision::ImplicitDeny
        );
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

    /// A session whose role is not in IAM (deleted, or never created) still
    /// carries the role ARN recorded on its credential, which dispatch sets
    /// as `aws:PrincipalArn`: a Deny naming that role reaches the session.
    #[test]
    fn deny_naming_a_deleted_role_reaches_its_session() {
        const ROLE_ARN: &str = "arn:aws:iam::123456789012:role/gone";
        let session = Principal {
            arn: "arn:aws:sts::123456789012:assumed-role/gone/s1".into(),
            user_id: "AROAGONE:s1".into(),
            account_id: "123456789012".into(),
            principal_type: PrincipalType::AssumedRole,
            source_identity: None,
            tags: None,
        };
        let policy = format!(
            r#"{{"Statement":[{{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"*"}},{{"Effect":"Deny","Principal":{{"AWS":"{ROLE_ARN}"}},"Action":"s3:GetObject","Resource":"*"}}]}}"#
        );
        let eval = IamPolicyEvaluatorImpl::new(setup());
        let decide = |context: &ConditionContext| {
            eval.evaluate_with_resource_policy(
                &session,
                &s3_get_object_action(),
                context,
                Some(&policy),
                "123456789012",
                &[],
                None,
            )
        };
        let with_role = ConditionContext {
            aws_principal_arn: Some(ROLE_ARN.into()),
            ..Default::default()
        };
        assert_eq!(decide(&with_role), IamDecision::ExplicitDeny);
        // Without the recorded ARN the role is unknown, and only the
        // wildcard grant applies.
        assert_eq!(decide(&ConditionContext::default()), IamDecision::Allow);
    }

    /// A same-account bucket policy naming a role (by its ARN, path
    /// included) grants the role's sessions, capped by the role's
    /// permissions boundary; every grant is capped by the SCPs.
    #[test]
    fn resource_policy_naming_the_role_grants_its_sessions() {
        use crate::state::IamRole;
        const ROLE_ARN: &str = "arn:aws:iam::123456789012:role/team/app";
        let state = setup();
        state.write().get_or_create("123456789012").roles.insert(
            "app".into(),
            IamRole {
                role_name: "app".into(),
                role_id: "AROAAPP".into(),
                arn: ROLE_ARN.into(),
                path: "/team/".into(),
                assume_role_policy_document: "{}".into(),
                description: None,
                created_at: Utc::now(),
                max_session_duration: 3600,
                tags: Vec::new(),
                permissions_boundary: None,
            },
        );
        let session = Principal {
            arn: "arn:aws:sts::123456789012:assumed-role/app/s1".into(),
            user_id: "AROAAPP:s1".into(),
            account_id: "123456789012".into(),
            principal_type: PrincipalType::AssumedRole,
            source_identity: None,
            tags: None,
        };
        let grant = |principal: &str| {
            format!(
                r#"{{"Statement":[{{"Effect":"Allow","Principal":{{"AWS":"{principal}"}},"Action":"s3:GetObject","Resource":"*"}}]}}"#
            )
        };
        let eval = IamPolicyEvaluatorImpl::new(state.clone());
        let decide = |policy: &str, scps: Option<&[String]>| {
            eval.evaluate_with_resource_policy(
                &session,
                &s3_get_object_action(),
                &ConditionContext::default(),
                Some(policy),
                "123456789012",
                &[],
                scps,
            )
        };
        assert_eq!(decide(&grant(ROLE_ARN), None), IamDecision::Allow);
        // The role ARN without its path names another role.
        assert_eq!(
            decide(&grant("arn:aws:iam::123456789012:role/app"), None),
            IamDecision::ImplicitDeny
        );
        // SCPs that do not allow the action cap the grant.
        let s3_put_only =
            r#"{"Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"*"}]}"#
                .to_string();
        assert_eq!(
            decide(&grant(ROLE_ARN), Some(std::slice::from_ref(&s3_put_only))),
            IamDecision::ImplicitDeny
        );
        // So does the role's boundary, which a grant naming the session
        // itself escapes.
        {
            let mut mas = state.write();
            let s = mas.get_or_create("123456789012");
            let boundary_arn = "arn:aws:iam::123456789012:policy/PutOnly";
            insert_managed_policy(s, boundary_arn, &s3_put_only);
            s.roles.get_mut("app").unwrap().permissions_boundary = Some(boundary_arn.into());
        }
        assert_eq!(decide(&grant(ROLE_ARN), None), IamDecision::ImplicitDeny);
        assert_eq!(
            decide(
                &grant("arn:aws:sts::123456789012:assumed-role/app/s1"),
                None
            ),
            IamDecision::Allow
        );
    }
}
