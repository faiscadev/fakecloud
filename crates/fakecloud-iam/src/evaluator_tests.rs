use super::*;
use fakecloud_aws::arn::Arn;
use serde_json::json;

fn principal_user(arn: &str) -> Principal {
    Principal {
        arn: arn.to_string(),
        user_id: "AIDA".into(),
        account_id: "123456789012".into(),
        principal_type: PrincipalType::User,
        source_identity: None,
        tags: None,
    }
}

fn req<'a>(principal: &'a Principal, action: &str, resource: &str) -> EvalRequest<'a> {
    EvalRequest {
        principal,
        action: action.to_string(),
        resource: resource.to_string(),
        context: RequestContext::default(),
    }
}

fn doc(json: serde_json::Value) -> PolicyDocument {
    PolicyDocument::from_value(&json)
}

// --- glob_match -----------------------------------------------------

#[test]
fn glob_literal_match() {
    assert!(glob_match("foo", "foo"));
    assert!(!glob_match("foo", "bar"));
}

#[test]
fn glob_star_matches_any() {
    assert!(glob_match("*", "foo"));
    assert!(glob_match("*", ""));
    assert!(glob_match("foo*", "foobar"));
    assert!(glob_match("*bar", "foobar"));
    assert!(glob_match("f*r", "foobar"));
    assert!(!glob_match("foo*", "fo"));
}

#[test]
fn glob_question_mark_matches_one() {
    assert!(glob_match("f?o", "foo"));
    assert!(!glob_match("f?o", "fo"));
    assert!(!glob_match("f?o", "foo!"));
}

#[test]
fn glob_no_backtracking_explosion() {
    // Pattern that would blow up a naive recursive matcher.
    assert!(!glob_match("a*a*a*a*a*b", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
}

// --- iam_glob_match (action specifics) ------------------------------

#[test]
fn iam_action_service_prefix_is_case_insensitive() {
    assert!(iam_glob_match("S3:GetObject", "s3:GetObject", true));
    assert!(iam_glob_match("s3:GetObject", "S3:GetObject", true));
}

#[test]
fn iam_action_name_is_case_sensitive() {
    // Action name is case-sensitive in AWS.
    assert!(!iam_glob_match("s3:getobject", "s3:GetObject", true));
    assert!(iam_glob_match("s3:GetObject", "s3:GetObject", true));
}

#[test]
fn iam_action_supports_wildcards() {
    assert!(iam_glob_match("s3:Get*", "s3:GetObject", true));
    assert!(iam_glob_match("s3:*", "s3:DeleteObject", true));
    assert!(iam_glob_match("*", "s3:GetObject", true));
    assert!(!iam_glob_match("s3:Get*", "s3:PutObject", true));
}

// --- evaluate -------------------------------------------------------

#[test]
fn empty_policy_set_is_implicit_deny() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    assert_eq!(
        evaluate(&[], &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key")),
        Decision::ImplicitDeny
    );
}

#[test]
fn allow_with_matching_action_and_resource() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let policy = doc(json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::bucket/key"
        }]
    }));
    assert_eq!(
        evaluate(
            &[policy],
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::Allow
    );
}

#[test]
fn deny_takes_precedence_over_allow() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let allow = doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Action": "*",
            "Resource": "*"
        }]
    }));
    let deny = doc(json!({
        "Statement": [{
            "Effect": "Deny",
            "Action": "s3:DeleteObject",
            "Resource": "*"
        }]
    }));
    assert_eq!(
        evaluate(
            &[allow.clone(), deny.clone()],
            &req(&p, "s3:DeleteObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::ExplicitDeny
    );
    // Order doesn't matter — Deny still wins when listed first.
    assert_eq!(
        evaluate(
            &[deny, allow],
            &req(&p, "s3:DeleteObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::ExplicitDeny
    );
}

#[test]
fn allow_with_wrong_action_is_implicit_deny() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let policy = doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": "*"
        }]
    }));
    assert_eq!(
        evaluate(
            &[policy],
            &req(&p, "s3:DeleteObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn allow_with_wrong_resource_is_implicit_deny() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let policy = doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::other-bucket/*"
        }]
    }));
    assert_eq!(
        evaluate(
            &[policy],
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn resource_wildcard_matches_arn_path() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let policy = doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::bucket/*"
        }]
    }));
    assert_eq!(
        evaluate(
            &[policy],
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/path/to/key")
        ),
        Decision::Allow
    );
}

#[test]
fn not_action_excludes_listed_actions() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let policy = doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "NotAction": "s3:DeleteObject",
            "Resource": "*"
        }]
    }));
    // Allowed because GetObject is not in NotAction.
    assert_eq!(
        evaluate(
            &[policy.clone()],
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::Allow
    );
    // Implicit-denied because DeleteObject is in NotAction (no allow matches).
    assert_eq!(
        evaluate(
            &[policy],
            &req(&p, "s3:DeleteObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn not_resource_excludes_listed_resources() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let policy = doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "NotResource": "arn:aws:s3:::secret-bucket/*"
        }]
    }));
    assert_eq!(
        evaluate(
            &[policy.clone()],
            &req(&p, "s3:GetObject", "arn:aws:s3:::public-bucket/key")
        ),
        Decision::Allow
    );
    assert_eq!(
        evaluate(
            &[policy],
            &req(&p, "s3:GetObject", "arn:aws:s3:::secret-bucket/key")
        ),
        Decision::ImplicitDeny
    );
}

fn req_with_ctx<'a>(
    principal: &'a Principal,
    action: &str,
    resource: &str,
    context: RequestContext,
) -> EvalRequest<'a> {
    EvalRequest {
        principal,
        action: action.to_string(),
        resource: resource.to_string(),
        context,
    }
}

fn ctx_alice() -> RequestContext {
    RequestContext {
        aws_username: Some("alice".into()),
        aws_principal_arn: Some("arn:aws:iam::123456789012:user/alice".into()),
        aws_principal_account: Some("123456789012".into()),
        aws_principal_type: Some("User".into()),
        aws_userid: Some("AIDA".into()),
        ..Default::default()
    }
}

#[test]
fn condition_string_equals_username_allows_match() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let policy = doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Action": "*",
            "Resource": "*",
            "Condition": { "StringEquals": { "aws:username": "alice" } }
        }]
    }));
    assert_eq!(
        evaluate(
            &[policy],
            &req_with_ctx(&p, "s3:GetObject", "arn:aws:s3:::bucket/key", ctx_alice())
        ),
        Decision::Allow
    );
}

#[test]
fn condition_string_equals_username_denies_mismatch() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let policy = doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Action": "*",
            "Resource": "*",
            "Condition": { "StringEquals": { "aws:username": "bob" } }
        }]
    }));
    assert_eq!(
        evaluate(
            &[policy],
            &req_with_ctx(&p, "s3:GetObject", "arn:aws:s3:::bucket/key", ctx_alice())
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn deny_with_condition_fires_when_condition_matches() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    // Deny-MFA-absent + unconditional Allow => the Deny only fires
    // when the SecureTransport context value is false. Deny precedence
    // beats the unconditional Allow.
    let policy = doc(json!({
        "Statement": [
            {
                "Effect": "Deny",
                "Action": "*",
                "Resource": "*",
                "Condition": { "Bool": { "aws:SecureTransport": "false" } }
            },
            {
                "Effect": "Allow",
                "Action": "s3:GetObject",
                "Resource": "*"
            }
        ]
    }));
    let mut ctx = ctx_alice();
    ctx.aws_secure_transport = Some(false);
    assert_eq!(
        evaluate(
            &[policy.clone()],
            &req_with_ctx(&p, "s3:GetObject", "arn:aws:s3:::bucket/key", ctx)
        ),
        Decision::ExplicitDeny
    );
    // When the request IS secure, the conditional Deny should not
    // fire and the Allow wins.
    let mut ctx_secure = ctx_alice();
    ctx_secure.aws_secure_transport = Some(true);
    assert_eq!(
        evaluate(
            &[policy],
            &req_with_ctx(&p, "s3:GetObject", "arn:aws:s3:::bucket/key", ctx_secure)
        ),
        Decision::Allow
    );
}

#[test]
fn condition_ip_address_allows_within_cidr() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let policy = doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": "*",
            "Condition": { "IpAddress": { "aws:SourceIp": "10.0.0.0/24" } }
        }]
    }));
    let mut ctx = ctx_alice();
    ctx.aws_source_ip = Some("10.0.0.17".parse().unwrap());
    assert_eq!(
        evaluate(
            &[policy.clone()],
            &req_with_ctx(&p, "s3:GetObject", "arn:aws:s3:::bucket/key", ctx)
        ),
        Decision::Allow
    );
    let mut wrong = ctx_alice();
    wrong.aws_source_ip = Some("192.168.1.1".parse().unwrap());
    assert_eq!(
        evaluate(
            &[policy],
            &req_with_ctx(&p, "s3:GetObject", "arn:aws:s3:::bucket/key", wrong)
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn condition_date_less_than_blocks_expired() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let policy = doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": "*",
            "Condition": {
                "DateLessThan": { "aws:CurrentTime": "2020-01-01T00:00:00Z" }
            }
        }]
    }));
    let mut ctx = ctx_alice();
    ctx.aws_current_time = Some(
        chrono::DateTime::parse_from_rfc3339("2024-06-15T12:00:00Z")
            .unwrap()
            .with_timezone(&chrono::Utc),
    );
    assert_eq!(
        evaluate(
            &[policy],
            &req_with_ctx(&p, "s3:GetObject", "arn:aws:s3:::bucket/key", ctx)
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn condition_missing_key_without_if_exists_denies() {
    // Context has no SourceIp; the IpAddress operator should
    // safe-fail, making the statement not apply.
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let policy = doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Action": "*",
            "Resource": "*",
            "Condition": { "IpAddress": { "aws:SourceIp": "10.0.0.0/8" } }
        }]
    }));
    assert_eq!(
        evaluate(
            &[policy],
            &req_with_ctx(&p, "s3:GetObject", "arn:aws:s3:::bucket/key", ctx_alice())
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn condition_if_exists_passes_on_missing_key() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let policy = doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Action": "*",
            "Resource": "*",
            "Condition": {
                "IpAddressIfExists": { "aws:SourceIp": "10.0.0.0/8" }
            }
        }]
    }));
    // SourceIp not populated; IfExists => condition passes.
    assert_eq!(
        evaluate(
            &[policy],
            &req_with_ctx(&p, "s3:GetObject", "arn:aws:s3:::bucket/key", ctx_alice())
        ),
        Decision::Allow
    );
}

#[test]
fn condition_multiple_operators_all_must_match() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let policy = doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Action": "*",
            "Resource": "*",
            "Condition": {
                "StringEquals": { "aws:username": "alice" },
                "IpAddress":    { "aws:SourceIp": "10.0.0.0/24" }
            }
        }]
    }));
    let mut ctx = ctx_alice();
    ctx.aws_source_ip = Some("10.0.0.1".parse().unwrap());
    assert_eq!(
        evaluate(
            &[policy.clone()],
            &req_with_ctx(&p, "s3:GetObject", "arn:aws:s3:::bucket/key", ctx)
        ),
        Decision::Allow
    );
    let mut wrong_ip = ctx_alice();
    wrong_ip.aws_source_ip = Some("192.168.1.1".parse().unwrap());
    assert_eq!(
        evaluate(
            &[policy],
            &req_with_ctx(&p, "s3:GetObject", "arn:aws:s3:::bucket/key", wrong_ip)
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn condition_unknown_operator_fails_closed() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let policy = doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Action": "*",
            "Resource": "*",
            "Condition": { "NotARealOperator": { "aws:username": "alice" } }
        }]
    }));
    assert_eq!(
        evaluate(
            &[policy],
            &req_with_ctx(&p, "s3:GetObject", "arn:aws:s3:::bucket/key", ctx_alice())
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn array_action_matches_any_entry() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let policy = doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Action": ["s3:GetObject", "s3:PutObject"],
            "Resource": "*"
        }]
    }));
    assert_eq!(
        evaluate(
            &[policy.clone()],
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::Allow
    );
    assert_eq!(
        evaluate(
            &[policy],
            &req(&p, "s3:PutObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::Allow
    );
}

#[test]
fn statement_without_effect_is_dropped() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let policy = doc(json!({
        "Statement": [
            { "Action": "s3:GetObject", "Resource": "*" },
            { "Effect": "Allow", "Action": "s3:GetObject", "Resource": "*" }
        ]
    }));
    // The dropped statement doesn't contribute, but the second
    // valid one still grants the request.
    assert_eq!(policy.statement_count(), 1);
    assert_eq!(
        evaluate(
            &[policy],
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::Allow
    );
}

#[test]
fn statement_without_action_is_dropped() {
    let policy = doc(json!({
        "Statement": [{ "Effect": "Allow", "Resource": "*" }]
    }));
    assert_eq!(policy.statement_count(), 0);
}

#[test]
fn implicit_resource_acts_like_wildcard() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let policy = doc(json!({
        "Statement": [{ "Effect": "Allow", "Action": "s3:GetObject" }]
    }));
    assert_eq!(
        evaluate(
            &[policy],
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::Allow
    );
}

#[test]
fn malformed_policy_json_is_implicit_deny() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let policy = PolicyDocument::parse("{ this is not valid json");
    assert_eq!(policy.statement_count(), 0);
    assert_eq!(
        evaluate(
            &[policy],
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn deny_short_circuits_after_match() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let policy = doc(json!({
        "Statement": [
            { "Effect": "Deny", "Action": "*", "Resource": "*" },
            { "Effect": "Allow", "Action": "s3:GetObject", "Resource": "*" }
        ]
    }));
    assert_eq!(
        evaluate(
            &[policy],
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::ExplicitDeny
    );
}

#[test]
fn user_name_from_arn_strips_iam_path() {
    // Default path — bare user name.
    assert_eq!(
        user_name_from_arn("arn:aws:iam::123456789012:user/alice"),
        Some("alice")
    );
    // Non-default path — must return the bare name, not
    // `engineering/alice`. IamState indexes users by the bare name,
    // so returning the path would silently drop pathed users from
    // policy evaluation (identified by cubic on PR #392).
    assert_eq!(
        user_name_from_arn("arn:aws:iam::123456789012:user/engineering/alice"),
        Some("alice")
    );
    assert_eq!(
        user_name_from_arn("arn:aws:iam::123456789012:user/path/to/alice"),
        Some("alice")
    );
    assert_eq!(user_name_from_arn("arn:aws:iam::123456789012:role/r"), None);
}

#[test]
fn collect_identity_policies_resolves_pathed_user() {
    // Regression guard for the pathed-user bug: a user created under
    // `/engineering/` must still have their inline policies picked up
    // by the evaluator.
    use crate::state::IamUser;
    use chrono::Utc;
    let mut state = IamState::new("123456789012");
    state.users.insert(
        "alice".to_string(),
        IamUser {
            user_name: "alice".into(),
            user_id: "AIDAALICE".into(),
            arn: "arn:aws:iam::123456789012:user/engineering/alice".into(),
            path: "/engineering/".into(),
            created_at: Utc::now(),
            tags: Vec::new(),
            permissions_boundary: None,
        },
    );
    let mut inline = std::collections::BTreeMap::new();
    inline.insert(
        "AllowGet".to_string(),
        r#"{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#.to_string(),
    );
    state
        .user_inline_policies
        .insert("alice".to_string(), inline);

    let principal = Principal {
        arn: "arn:aws:iam::123456789012:user/engineering/alice".to_string(),
        user_id: "AIDAALICE".to_string(),
        account_id: "123456789012".to_string(),
        principal_type: PrincipalType::User,
        source_identity: None,
        tags: None,
    };
    let docs = collect_identity_policies(&state, &principal);
    assert_eq!(docs.len(), 1, "pathed user's inline policy was missed");
    assert_eq!(
        evaluate(
            &docs,
            &req(&principal, "s3:GetObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::Allow
    );
}

#[test]
fn role_name_from_assumed_role_arn_strips_session() {
    assert_eq!(
        role_name_from_assumed_role_arn("arn:aws:sts::123456789012:assumed-role/ops/session-1"),
        Some("ops")
    );
}

// --- collect_identity_policies --------------------------------------

#[test]
fn collect_identity_policies_picks_up_user_inline() {
    use crate::state::IamUser;
    use chrono::Utc;
    let mut state = IamState::new("123456789012");
    state.users.insert(
        "alice".to_string(),
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
    let mut inline = std::collections::BTreeMap::new();
    inline.insert(
        "AllowGet".to_string(),
        r#"{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#.to_string(),
    );
    state
        .user_inline_policies
        .insert("alice".to_string(), inline);

    let principal = principal_user("arn:aws:iam::123456789012:user/alice");
    let docs = collect_identity_policies(&state, &principal);
    assert_eq!(docs.len(), 1);
    assert_eq!(
        evaluate(
            &docs,
            &req(&principal, "s3:GetObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::Allow
    );
}

#[test]
fn collect_identity_policies_picks_up_managed_via_groups() {
    use crate::state::{IamGroup, IamPolicy, IamUser, PolicyVersion};
    use chrono::Utc;
    let mut state = IamState::new("123456789012");
    state.users.insert(
        "alice".to_string(),
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
    let policy_arn = "arn:aws:iam::123456789012:policy/AllowGet".to_string();
    state.policies.insert(
        policy_arn.clone(),
        IamPolicy {
            policy_name: "AllowGet".into(),
            policy_id: "ANPA1".into(),
            arn: policy_arn.clone(),
            path: "/".into(),
            description: "".into(),
            created_at: Utc::now(),
            tags: Vec::new(),
            default_version_id: "v1".into(),
            versions: vec![PolicyVersion {
                version_id: "v1".into(),
                document:
                    r#"{"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#
                        .into(),
                is_default: true,
                created_at: Utc::now(),
            }],
            next_version_num: 2,
            attachment_count: 1,
        },
    );
    state.groups.insert(
        "readers".to_string(),
        IamGroup {
            group_name: "readers".into(),
            group_id: "AGPA1".into(),
            arn: "arn:aws:iam::123456789012:group/readers".into(),
            path: "/".into(),
            created_at: Utc::now(),
            members: vec!["alice".into()],
            inline_policies: std::collections::BTreeMap::new(),
            attached_policies: vec![policy_arn],
        },
    );
    let principal = principal_user("arn:aws:iam::123456789012:user/alice");
    let docs = collect_identity_policies(&state, &principal);
    assert_eq!(docs.len(), 1);
    assert_eq!(
        evaluate(
            &docs,
            &req(&principal, "s3:GetObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::Allow
    );
}

#[test]
fn collect_identity_policies_for_root_returns_empty() {
    let state = IamState::new("123456789012");
    let principal = Principal {
        arn: "arn:aws:iam::123456789012:root".into(),
        user_id: "ROOT".into(),
        account_id: "123456789012".into(),
        principal_type: PrincipalType::Root,
        source_identity: None,
        tags: None,
    };
    // Root short-circuits via Principal::is_root in dispatch; here we
    // just assert collect_identity_policies doesn't synthesize a
    // wildcard allow on its behalf.
    assert!(collect_identity_policies(&state, &principal).is_empty());
}

// --- resource-policy cross-account evaluation -----------------------

const ACCT_A: &str = "111111111111";
const ACCT_B: &str = "222222222222";

fn principal_in(account: &str, user: &str) -> Principal {
    Principal {
        arn: Arn::global("iam", account, &format!("user/{user}")).to_string(),
        user_id: format!("AIDA{user}"),
        account_id: account.into(),
        principal_type: PrincipalType::User,
        source_identity: None,
        tags: None,
    }
}

fn assumed_role_principal(account: &str, role_arn_tail: &str) -> Principal {
    Principal {
        arn: Arn::global("sts", account, &format!("assumed-role/{role_arn_tail}")).to_string(),
        user_id: "AROAEXAMPLE".into(),
        account_id: account.into(),
        principal_type: PrincipalType::AssumedRole,
        source_identity: None,
        tags: None,
    }
}

fn eval_cross(
    identity: Option<serde_json::Value>,
    resource: Option<serde_json::Value>,
    principal: &Principal,
    resource_account_id: &str,
) -> Decision {
    let identity_docs: Vec<PolicyDocument> = identity.into_iter().map(doc).collect();
    let resource_doc = resource.map(doc);
    let request = req(principal, "s3:GetObject", "arn:aws:s3:::bucket/key");
    evaluate_with_resource_policy(
        &identity_docs,
        resource_doc.as_ref(),
        &request,
        resource_account_id,
    )
}

fn allow_get_wildcard() -> serde_json::Value {
    json!({"Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]})
}

fn deny_get_wildcard() -> serde_json::Value {
    json!({"Statement":[{"Effect":"Deny","Action":"s3:GetObject","Resource":"*"}]})
}

fn resource_allow_for(principal_arn: &str) -> serde_json::Value {
    json!({
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": principal_arn},
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::bucket/key"
        }]
    })
}

#[test]
fn same_account_identity_only_allow() {
    let p = principal_in(ACCT_A, "alice");
    assert_eq!(
        eval_cross(Some(allow_get_wildcard()), None, &p, ACCT_A),
        Decision::Allow
    );
}

#[test]
fn same_account_resource_only_allow_via_user_arn() {
    let p = principal_in(ACCT_A, "alice");
    let resource = resource_allow_for(&p.arn);
    assert_eq!(
        eval_cross(None, Some(resource), &p, ACCT_A),
        Decision::Allow
    );
}

#[test]
fn same_account_both_allow() {
    let p = principal_in(ACCT_A, "alice");
    assert_eq!(
        eval_cross(
            Some(allow_get_wildcard()),
            Some(resource_allow_for(&p.arn)),
            &p,
            ACCT_A,
        ),
        Decision::Allow
    );
}

#[test]
fn same_account_neither_allows_is_implicit_deny() {
    let p = principal_in(ACCT_A, "alice");
    assert_eq!(eval_cross(None, None, &p, ACCT_A), Decision::ImplicitDeny);
}

#[test]
fn identity_deny_blocks_resource_allow() {
    let p = principal_in(ACCT_A, "alice");
    let resource = resource_allow_for(&p.arn);
    assert_eq!(
        eval_cross(Some(deny_get_wildcard()), Some(resource), &p, ACCT_A),
        Decision::ExplicitDeny
    );
}

#[test]
fn resource_deny_blocks_identity_allow() {
    let p = principal_in(ACCT_A, "alice");
    let resource_deny = json!({
        "Statement": [{
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "*"
        }]
    });
    assert_eq!(
        eval_cross(Some(allow_get_wildcard()), Some(resource_deny), &p, ACCT_A,),
        Decision::ExplicitDeny
    );
}

#[test]
fn cross_account_identity_only_is_implicit_deny() {
    // Resource lives in B, principal in A. Identity grants, resource
    // policy silent -> cross-account semantics require both.
    let p = principal_in(ACCT_A, "alice");
    assert_eq!(
        eval_cross(Some(allow_get_wildcard()), None, &p, ACCT_B),
        Decision::ImplicitDeny
    );
}

#[test]
fn cross_account_resource_only_is_implicit_deny() {
    // Resource lives in B and grants via its policy; principal in A
    // has no identity policy → cross-account requires identity too.
    let p = principal_in(ACCT_A, "alice");
    let resource = resource_allow_for(&p.arn);
    assert_eq!(
        eval_cross(None, Some(resource), &p, ACCT_B),
        Decision::ImplicitDeny
    );
}

#[test]
fn cross_account_both_allow_succeeds() {
    let p = principal_in(ACCT_A, "alice");
    let resource = resource_allow_for(&p.arn);
    assert_eq!(
        eval_cross(Some(allow_get_wildcard()), Some(resource), &p, ACCT_B),
        Decision::Allow
    );
}

#[test]
fn principal_wildcard_star_matches_any_principal() {
    let p = principal_in(ACCT_A, "alice");
    let resource = json!({
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "*"
        }]
    });
    assert_eq!(
        eval_cross(None, Some(resource), &p, ACCT_A),
        Decision::Allow
    );
}

#[test]
fn principal_aws_star_matches_any_principal() {
    let p = principal_in(ACCT_A, "alice");
    let resource = json!({
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "*"},
            "Action": "s3:GetObject",
            "Resource": "*"
        }]
    });
    assert_eq!(
        eval_cross(None, Some(resource), &p, ACCT_A),
        Decision::Allow
    );
}

#[test]
fn principal_account_root_matches_any_user_in_account() {
    let p = principal_in(ACCT_A, "alice");
    let resource = resource_allow_for("arn:aws:iam::111111111111:root");
    assert_eq!(
        eval_cross(None, Some(resource), &p, ACCT_A),
        Decision::Allow
    );
}

#[test]
fn principal_account_root_does_not_match_other_account() {
    let p = principal_in(ACCT_A, "alice");
    let resource = resource_allow_for("arn:aws:iam::222222222222:root");
    assert_eq!(
        eval_cross(None, Some(resource), &p, ACCT_A),
        Decision::ImplicitDeny
    );
}

#[test]
fn principal_user_arn_exact_match() {
    let p = principal_in(ACCT_A, "alice");
    let resource = resource_allow_for("arn:aws:iam::111111111111:user/alice");
    assert_eq!(
        eval_cross(None, Some(resource), &p, ACCT_A),
        Decision::Allow
    );
}

#[test]
fn principal_user_arn_mismatch_is_deny() {
    let p = principal_in(ACCT_A, "alice");
    let resource = resource_allow_for("arn:aws:iam::111111111111:user/bob");
    assert_eq!(
        eval_cross(None, Some(resource), &p, ACCT_A),
        Decision::ImplicitDeny
    );
}

#[test]
fn principal_service_matches_assumed_role_containing_service_host() {
    let p = assumed_role_principal(
        ACCT_A,
        "AWSServiceRoleForLambda.lambda.amazonaws.com/session",
    );
    let resource = json!({
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "s3:GetObject",
            "Resource": "*"
        }]
    });
    assert_eq!(
        eval_cross(None, Some(resource), &p, ACCT_A),
        Decision::Allow
    );
}

#[test]
fn principal_service_does_not_match_unrelated_user() {
    let p = principal_in(ACCT_A, "alice");
    let resource = json!({
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "s3:GetObject",
            "Resource": "*"
        }]
    });
    assert_eq!(
        eval_cross(None, Some(resource), &p, ACCT_A),
        Decision::ImplicitDeny
    );
}

#[test]
fn not_principal_deny_excludes_named_user() {
    // NotPrincipal + Deny: deny everyone EXCEPT bob.
    // Alice is not bob -> deny applies -> ExplicitDeny.
    let alice = principal_in(ACCT_A, "alice");
    let resource = json!({
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "*"
            },
            {
                "Effect": "Deny",
                "NotPrincipal": {"AWS": Arn::global("iam", ACCT_A, "user/bob").to_string()},
                "Action": "s3:GetObject",
                "Resource": "*"
            }
        ]
    });
    assert_eq!(
        eval_cross(None, Some(resource.clone()), &alice, ACCT_A),
        Decision::ExplicitDeny
    );

    // Bob IS the named principal -> deny does NOT apply -> Allow from first statement.
    let bob = principal_in(ACCT_A, "bob");
    assert_eq!(
        eval_cross(None, Some(resource), &bob, ACCT_A),
        Decision::Allow
    );
}

#[test]
fn not_principal_allow_excludes_named_user() {
    // NotPrincipal + Allow: allow everyone EXCEPT bob.
    // Alice is not bob -> allow applies.
    let alice = principal_in(ACCT_A, "alice");
    let resource = json!({
        "Statement": [{
            "Effect": "Allow",
            "NotPrincipal": {"AWS": Arn::global("iam", ACCT_A, "user/bob").to_string()},
            "Action": "s3:GetObject",
            "Resource": "*"
        }]
    });
    assert_eq!(
        eval_cross(None, Some(resource.clone()), &alice, ACCT_A),
        Decision::Allow
    );

    // Bob IS the named principal -> allow does NOT apply -> ImplicitDeny.
    let bob = principal_in(ACCT_A, "bob");
    assert_eq!(
        eval_cross(None, Some(resource), &bob, ACCT_A),
        Decision::ImplicitDeny
    );
}

#[test]
fn not_principal_with_star_never_applies() {
    // NotPrincipal: "*" matches everyone, so the statement never applies.
    let alice = principal_in(ACCT_A, "alice");
    let resource = json!({
        "Statement": [{
            "Effect": "Allow",
            "NotPrincipal": "*",
            "Action": "s3:GetObject",
            "Resource": "*"
        }]
    });
    assert_eq!(
        eval_cross(None, Some(resource), &alice, ACCT_A),
        Decision::ImplicitDeny
    );
}

#[test]
fn not_principal_with_account_root() {
    // NotPrincipal names account root. AwsAccountRoot matches
    // any principal in that account, so alice (in ACCT_A) matches
    // the NotPrincipal list and the statement does NOT apply.
    let alice = principal_in(ACCT_A, "alice");
    let resource = json!({
        "Statement": [{
            "Effect": "Allow",
            "NotPrincipal": {"AWS": Arn::global("iam", ACCT_A, "root").to_string()},
            "Action": "s3:GetObject",
            "Resource": "*"
        }]
    });
    assert_eq!(
        eval_cross(None, Some(resource.clone()), &alice, ACCT_A),
        Decision::ImplicitDeny
    );

    // A user in a DIFFERENT account does NOT match ACCT_A root,
    // so the Deny statement applies. With a Deny+NotPrincipal pattern
    // this means the cross-account user gets denied.
    let eve = principal_in(ACCT_B, "eve");
    let resource_deny = json!({
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": "s3:GetObject",
                "Resource": "*"
            },
            {
                "Effect": "Deny",
                "NotPrincipal": {"AWS": Arn::global("iam", ACCT_A, "root").to_string()},
                "Action": "s3:GetObject",
                "Resource": "*"
            }
        ]
    });
    // Eve (ACCT_B) doesn't match ACCT_A root, so Deny applies.
    assert_eq!(
        eval_cross(None, Some(resource_deny.clone()), &eve, ACCT_A),
        Decision::ExplicitDeny
    );
    // Alice (ACCT_A) matches ACCT_A root, so Deny does NOT apply -> Allow.
    assert_eq!(
        eval_cross(None, Some(resource_deny), &alice, ACCT_A),
        Decision::Allow
    );
}

#[test]
fn not_principal_with_unrecognized_type_safe_skips() {
    // NotPrincipal with only CanonicalUser (still unrecognized) ->
    // empty refs list -> statement skipped safely. Federated is now
    // recognized (F1) so we use CanonicalUser to keep covering the
    // safe-skip path.
    let alice = principal_in(ACCT_A, "alice");
    let resource = json!({
        "Statement": [{
            "Effect": "Allow",
            "NotPrincipal": {"CanonicalUser": "abc123"},
            "Action": "s3:GetObject",
            "Resource": "*"
        }]
    });
    assert_eq!(
        eval_cross(None, Some(resource), &alice, ACCT_A),
        Decision::ImplicitDeny
    );
}

#[test]
fn not_principal_federated_excludes_federated_callers() {
    // F1: NotPrincipal with Federated now actually filters federated
    // callers. A non-federated caller (alice) doesn't match the
    // federated entry, so the statement applies and the Allow fires.
    let alice = principal_in(ACCT_A, "alice");
    let resource = json!({
        "Statement": [{
            "Effect": "Allow",
            "NotPrincipal": {"Federated": "cognito-identity.amazonaws.com"},
            "Action": "s3:GetObject",
            "Resource": "*"
        }]
    });
    assert_eq!(
        eval_cross(None, Some(resource), &alice, ACCT_A),
        Decision::Allow
    );
}

#[test]
fn not_principal_with_multiple_entries() {
    // NotPrincipal with multiple users. Statement applies only
    // to callers matching NONE of the entries.
    let alice = principal_in(ACCT_A, "alice");
    let bob = principal_in(ACCT_A, "bob");
    let charlie = principal_in(ACCT_A, "charlie");
    let resource = json!({
        "Statement": [{
            "Effect": "Deny",
            "NotPrincipal": {"AWS": [
                Arn::global("iam", ACCT_A, "user/alice").to_string(),
                Arn::global("iam", ACCT_A, "user/bob").to_string()
            ]},
            "Action": "s3:GetObject",
            "Resource": "*"
        }]
    });
    // Alice and bob are in the list -> deny does NOT apply
    assert_eq!(
        eval_cross(None, Some(resource.clone()), &alice, ACCT_A),
        Decision::ImplicitDeny
    );
    assert_eq!(
        eval_cross(None, Some(resource.clone()), &bob, ACCT_A),
        Decision::ImplicitDeny
    );
    // Charlie is NOT in the list -> deny applies
    assert_eq!(
        eval_cross(None, Some(resource), &charlie, ACCT_A),
        Decision::ExplicitDeny
    );
}

#[test]
fn resource_policy_statement_without_principal_is_skipped() {
    // Malformed resource policy (missing Principal entirely) must
    // not silently grant to everyone.
    let p = principal_in(ACCT_A, "alice");
    let resource = json!({
        "Statement": [{
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": "*"
        }]
    });
    assert_eq!(
        eval_cross(None, Some(resource), &p, ACCT_A),
        Decision::ImplicitDeny
    );
}

#[test]
fn resource_policy_condition_block_gates_access() {
    // Regression guard: Phase 2 condition evaluation still applies
    // to resource-policy statements.
    use crate::condition::ConditionContext;
    use std::net::IpAddr;

    let p = principal_in(ACCT_A, "alice");
    let resource = json!({
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "*",
            "Condition": {
                "IpAddress": {"aws:SourceIp": "10.0.0.0/8"}
            }
        }]
    });
    let resource_doc = doc(resource);

    let ctx_ok = ConditionContext {
        aws_source_ip: Some("10.1.2.3".parse::<IpAddr>().unwrap()),
        ..ConditionContext::default()
    };
    let req_ok = EvalRequest {
        principal: &p,
        action: "s3:GetObject".to_string(),
        resource: "arn:aws:s3:::bucket/key".to_string(),
        context: ctx_ok,
    };
    assert_eq!(
        evaluate_with_resource_policy(&[], Some(&resource_doc), &req_ok, ACCT_A),
        Decision::Allow
    );

    let ctx_bad = ConditionContext {
        aws_source_ip: Some("8.8.8.8".parse::<IpAddr>().unwrap()),
        ..ConditionContext::default()
    };
    let req_bad = EvalRequest {
        principal: &p,
        action: "s3:GetObject".to_string(),
        resource: "arn:aws:s3:::bucket/key".to_string(),
        context: ctx_bad,
    };
    assert_eq!(
        evaluate_with_resource_policy(&[], Some(&resource_doc), &req_bad, ACCT_A),
        Decision::ImplicitDeny
    );
}

#[test]
fn classify_aws_principal_recognizes_bare_account_id() {
    assert_eq!(
        classify_aws_principal("111111111111"),
        PrincipalRef::AwsAccountRoot("111111111111".to_string())
    );
}

#[test]
fn classify_aws_principal_recognizes_root_arn() {
    assert_eq!(
        classify_aws_principal("arn:aws:iam::111111111111:root"),
        PrincipalRef::AwsAccountRoot("111111111111".to_string())
    );
}

#[test]
fn classify_aws_principal_keeps_user_arn_as_arn() {
    assert_eq!(
        classify_aws_principal("arn:aws:iam::111111111111:user/alice"),
        PrincipalRef::AwsArn("arn:aws:iam::111111111111:user/alice".to_string())
    );
}

// --- evaluate_with_gates (Phase 3) ---------------------------------

fn allow_all() -> PolicyDocument {
    doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Action": "*",
            "Resource": "*"
        }]
    }))
}

fn allow_get_object() -> PolicyDocument {
    doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Action": "s3:GetObject",
            "Resource": "*"
        }]
    }))
}

fn deny_put_object() -> PolicyDocument {
    doc(json!({
        "Statement": [{
            "Effect": "Deny",
            "Action": "s3:PutObject",
            "Resource": "*"
        }]
    }))
}

#[test]
fn gates_absent_behaves_like_phase2_allow() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let identity = [allow_all()];
    assert_eq!(
        evaluate_with_gates(
            &identity,
            None,
            None,
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::Allow
    );
}

#[test]
fn gates_absent_behaves_like_phase2_implicit_deny() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    assert_eq!(
        evaluate_with_gates(
            &[],
            None,
            None,
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn boundary_caps_identity_allow() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let identity = [allow_all()];
    let boundary = [allow_get_object()];
    // Action covered by both identity and boundary → Allow.
    assert_eq!(
        evaluate_with_gates(
            &identity,
            Some(&boundary),
            None,
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::Allow
    );
    // Action covered by identity but not boundary → ImplicitDeny.
    assert_eq!(
        evaluate_with_gates(
            &identity,
            Some(&boundary),
            None,
            &req(&p, "s3:PutObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn empty_boundary_denies_everything() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let identity = [allow_all()];
    let boundary: [PolicyDocument; 0] = [];
    // Dangling / unresolved boundary ARN → caller passes Some(&[])
    // which must deny everything.
    assert_eq!(
        evaluate_with_gates(
            &identity,
            Some(&boundary),
            None,
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn explicit_deny_in_boundary_wins() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let identity = [allow_all()];
    let boundary = [deny_put_object()];
    assert_eq!(
        evaluate_with_gates(
            &identity,
            Some(&boundary),
            None,
            &req(&p, "s3:PutObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::ExplicitDeny
    );
}

#[test]
fn identity_implicit_with_boundary_allow_is_implicit_deny() {
    // Boundary doesn't grant — only caps. If identity is silent,
    // the request must still deny.
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let boundary = [allow_all()];
    assert_eq!(
        evaluate_with_gates(
            &[],
            Some(&boundary),
            None,
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn session_policy_caps_identity_allow() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let identity = [allow_all()];
    let session = [allow_get_object()];
    assert_eq!(
        evaluate_with_gates(
            &identity,
            None,
            Some(&session),
            &req(&p, "s3:PutObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::ImplicitDeny
    );
    assert_eq!(
        evaluate_with_gates(
            &identity,
            None,
            Some(&session),
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::Allow
    );
}

#[test]
fn session_policy_explicit_deny_wins() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let identity = [allow_all()];
    let session = [deny_put_object()];
    assert_eq!(
        evaluate_with_gates(
            &identity,
            None,
            Some(&session),
            &req(&p, "s3:PutObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::ExplicitDeny
    );
}

#[test]
fn boundary_and_session_must_both_allow() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let identity = [allow_all()];
    let boundary = [allow_all()];
    let session = [allow_get_object()];
    // Session caps to GetObject only.
    assert_eq!(
        evaluate_with_gates(
            &identity,
            Some(&boundary),
            Some(&session),
            &req(&p, "s3:PutObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::ImplicitDeny
    );
    assert_eq!(
        evaluate_with_gates(
            &identity,
            Some(&boundary),
            Some(&session),
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key")
        ),
        Decision::Allow
    );
}

// --- evaluate_with_resource_policy_and_gates -----------------------

#[test]
fn resource_policy_gated_same_account_resource_bypasses_boundary() {
    // Same-account grant via a resource policy does NOT need the
    // identity side (or the boundary/session gates) to allow —
    // resource policies in the same account stand on their own.
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let identity: [PolicyDocument; 0] = [];
    let boundary: [PolicyDocument; 0] = []; // deny-all boundary
    let resource = doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::123456789012:user/alice"},
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::bucket/key"
        }]
    }));
    assert_eq!(
        evaluate_with_resource_policy_and_gates(
            &identity,
            Some(&boundary),
            None,
            Some(&resource),
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key"),
            "123456789012"
        ),
        Decision::Allow
    );
}

#[test]
fn resource_policy_gated_cross_account_identity_must_allow() {
    // Cross-account: identity AND resource must both allow. Even
    // with a resource-policy grant, if identity is implicit-deny
    // the call is denied.
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let identity: [PolicyDocument; 0] = [];
    let resource = doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::bucket/key"
        }]
    }));
    assert_eq!(
        evaluate_with_resource_policy_and_gates(
            &identity,
            None,
            None,
            Some(&resource),
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key"),
            "999999999999"
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn resource_policy_gated_cross_account_boundary_caps_identity_side() {
    // Cross-account, identity allows, resource allows, but the
    // caller's boundary is empty (deny-all) → identity side is
    // gated to ImplicitDeny and the AND denies the call.
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let identity = [allow_all()];
    let boundary: [PolicyDocument; 0] = [];
    let resource = doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::bucket/key"
        }]
    }));
    assert_eq!(
        evaluate_with_resource_policy_and_gates(
            &identity,
            Some(&boundary),
            None,
            Some(&resource),
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key"),
            "999999999999"
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn resource_policy_gated_explicit_deny_in_session_wins() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let identity = [allow_all()];
    let session = [deny_put_object()];
    let resource = doc(json!({
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::bucket/*"
        }]
    }));
    assert_eq!(
        evaluate_with_resource_policy_and_gates(
            &identity,
            None,
            Some(&session),
            Some(&resource),
            &req(&p, "s3:PutObject", "arn:aws:s3:::bucket/key"),
            "123456789012"
        ),
        Decision::ExplicitDeny
    );
}

// --- Batch 4: SCP ceiling layer -------------------------------------

#[test]
fn scp_caps_identity_allow_all() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let identity = [allow_all()];
    let scps = [allow_get_object()];
    assert_eq!(
        evaluate_with_gates_and_scps(
            &identity,
            None,
            None,
            Some(&scps),
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key"),
        ),
        Decision::Allow
    );
    assert_eq!(
        evaluate_with_gates_and_scps(
            &identity,
            None,
            None,
            Some(&scps),
            &req(&p, "s3:PutObject", "arn:aws:s3:::bucket/key"),
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn scp_explicit_deny_wins() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let identity = [allow_all()];
    let scps = [deny_put_object()];
    assert_eq!(
        evaluate_with_gates_and_scps(
            &identity,
            None,
            None,
            Some(&scps),
            &req(&p, "s3:PutObject", "arn:aws:s3:::bucket/key"),
        ),
        Decision::ExplicitDeny
    );
}

#[test]
fn scp_empty_chain_denies_everything() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let identity = [allow_all()];
    let scps: [PolicyDocument; 0] = [];
    // Some(&[]) means the org applies but no SCP allow-all reaches
    // the account path (e.g. FullAWSAccess detached and nothing
    // else attached). Deny-by-default.
    assert_eq!(
        evaluate_with_gates_and_scps(
            &identity,
            None,
            None,
            Some(&scps),
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key"),
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn scp_none_preserves_identity_only_decision() {
    // None = off-by-default. Evaluation must match the no-SCP
    // path bit-for-bit, preserving the zero-behavior-change
    // contract when no organization exists.
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let identity = [allow_all()];
    let with_scps = evaluate_with_gates_and_scps(
        &identity,
        None,
        None,
        None,
        &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key"),
    );
    let without = evaluate_with_gates(
        &identity,
        None,
        None,
        &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key"),
    );
    assert_eq!(with_scps, without);
    assert_eq!(with_scps, Decision::Allow);
}

#[test]
fn scp_chain_intersects_across_ancestors() {
    // Two SCPs up the path: outer Allow *, inner Allow only
    // s3:GetObject. AWS intersects — action must be in every one.
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let identity = [allow_all()];
    let scps = [allow_all(), allow_get_object()];
    assert_eq!(
        evaluate_with_gates_and_scps(
            &identity,
            None,
            None,
            Some(&scps),
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key"),
        ),
        Decision::Allow
    );
    assert_eq!(
        evaluate_with_gates_and_scps(
            &identity,
            None,
            None,
            Some(&scps),
            &req(&p, "s3:PutObject", "arn:aws:s3:::bucket/key"),
        ),
        Decision::ImplicitDeny
    );
}

#[test]
fn scp_intersects_with_boundary_and_session() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let identity = [allow_all()];
    let boundary = [allow_all()];
    let session = [allow_all()];
    let scps = [allow_get_object()];
    assert_eq!(
        evaluate_with_gates_and_scps(
            &identity,
            Some(&boundary),
            Some(&session),
            Some(&scps),
            &req(&p, "s3:PutObject", "arn:aws:s3:::bucket/key"),
        ),
        Decision::ImplicitDeny
    );
    assert_eq!(
        evaluate_with_gates_and_scps(
            &identity,
            Some(&boundary),
            Some(&session),
            Some(&scps),
            &req(&p, "s3:GetObject", "arn:aws:s3:::bucket/key"),
        ),
        Decision::Allow
    );
}

#[test]
fn scp_caps_identity_side_of_resource_policy() {
    // Cross-account resource policy grants PutObject; caller's SCP
    // allows only GetObject. Identity side is gated by SCP →
    // cross-account AND means the whole thing denies.
    let p = principal_user("arn:aws:iam::111111111111:user/alice");
    let identity = [allow_all()];
    let resource = doc(serde_json::json!({
        "Statement": [{
            "Effect": "Allow",
            "Principal": "*",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::bucket/*"
        }]
    }));
    let scps = [allow_get_object()];
    assert_eq!(
        evaluate_with_resource_policy_and_gates_and_scps(
            &identity,
            None,
            None,
            Some(&scps),
            Some(&resource),
            &req(&p, "s3:PutObject", "arn:aws:s3:::bucket/key"),
            "222222222222",
        ),
        Decision::ImplicitDeny
    );
}

// --- KMS key-policy delegation (bug-audit 2026-05-28, 5.5) ----------

const KMS_ACCT: &str = "123456789012";
const KMS_KEY_ARN: &str = "arn:aws:kms:us-east-1:123456789012:key/k1";

fn kms_identity_allow() -> PolicyDocument {
    doc(json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": "kms:Decrypt", "Resource": "*"}]
    }))
}

#[test]
fn kms_nondelegating_key_policy_blocks_identity_grant() {
    // Key policy grants only some other principal; it neither names this
    // caller nor delegates to the account root. Identity allows kms:Decrypt,
    // but AWS denies because identity is powerless without key-policy
    // delegation.
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let key_policy = doc(json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::123456789012:user/bob"},
            "Action": "kms:*",
            "Resource": "*"
        }]
    }));
    let r = req(&p, "kms:Decrypt", KMS_KEY_ARN);
    assert_eq!(
        evaluate_with_resource_policy(&[kms_identity_allow()], Some(&key_policy), &r, KMS_ACCT),
        Decision::ImplicitDeny
    );
}

#[test]
fn kms_default_delegating_policy_plus_identity_allows() {
    // Default "Enable IAM permissions" key policy delegates to the account
    // root; the identity grant then takes effect.
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let key_policy = doc(json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
            "Action": "kms:*",
            "Resource": "*"
        }]
    }));
    let r = req(&p, "kms:Decrypt", KMS_KEY_ARN);
    assert_eq!(
        evaluate_with_resource_policy(&[kms_identity_allow()], Some(&key_policy), &r, KMS_ACCT),
        Decision::Allow
    );
}

#[test]
fn kms_delegating_policy_without_identity_denies() {
    // Delegation merely enables IAM; with no identity policy the request is
    // denied.
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let key_policy = doc(json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
            "Action": "kms:*",
            "Resource": "*"
        }]
    }));
    let r = req(&p, "kms:Decrypt", KMS_KEY_ARN);
    assert_eq!(
        evaluate_with_resource_policy(&[], Some(&key_policy), &r, KMS_ACCT),
        Decision::ImplicitDeny
    );
}

#[test]
fn kms_direct_key_policy_grant_allows_without_identity() {
    // Key policy names the principal directly -> allowed even with no identity
    // policy.
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let key_policy = doc(json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::123456789012:user/alice"},
            "Action": "kms:Decrypt",
            "Resource": "*"
        }]
    }));
    let r = req(&p, "kms:Decrypt", KMS_KEY_ARN);
    assert_eq!(
        evaluate_with_resource_policy(&[], Some(&key_policy), &r, KMS_ACCT),
        Decision::Allow
    );
}

#[test]
fn kms_explicit_deny_in_key_policy_wins() {
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let key_policy = doc(json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
                "Action": "kms:*",
                "Resource": "*"
            },
            {
                "Effect": "Deny",
                "Principal": {"AWS": "*"},
                "Action": "kms:Decrypt",
                "Resource": "*"
            }
        ]
    }));
    let r = req(&p, "kms:Decrypt", KMS_KEY_ARN);
    assert_eq!(
        evaluate_with_resource_policy(&[kms_identity_allow()], Some(&key_policy), &r, KMS_ACCT),
        Decision::ExplicitDeny
    );
}

#[test]
fn kms_delegation_rule_is_case_insensitive_on_action_prefix() {
    // IAM actions are case-insensitive; a mixed-case service prefix must still
    // route through the KMS key-policy delegation rule, not the generic
    // identity-OR-resource path (bug-audit 5.5 hardening).
    let p = principal_user("arn:aws:iam::123456789012:user/alice");
    let key_policy = doc(json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::123456789012:user/bob"},
            "Action": "kms:*",
            "Resource": "*"
        }]
    }));
    let r = req(&p, "KMS:Decrypt", KMS_KEY_ARN);
    assert_eq!(
        evaluate_with_resource_policy(&[kms_identity_allow()], Some(&key_policy), &r, KMS_ACCT),
        Decision::ImplicitDeny
    );
}
