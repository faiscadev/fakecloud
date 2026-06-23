//! End-to-end coverage for SimulateCustomPolicy + SimulatePrincipalPolicy.
//!
//! These tests drive the AWS Rust SDK against fakecloud's IAM simulator
//! and assert on the structured `EvaluationResult` decisions we return —
//! exercising the full evaluator pipeline (action match, explicit deny,
//! conditions, principal-policy resolution including group union).

mod helpers;

use aws_sdk_iam::types::{ContextEntry, ContextKeyTypeEnum};
use helpers::TestServer;

const ALLOW_S3_GET: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;

const ALLOW_S3_DENY_DELETE: &str = r#"{"Version":"2012-10-17","Statement":[
    {"Effect":"Allow","Action":"s3:*","Resource":"*"},
    {"Effect":"Deny","Action":"s3:DeleteObject","Resource":"*"}
]}"#;

const ALLOW_TAG_CONDITION: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"*","Condition":{"StringEquals":{"aws:RequestTag/team":"red"}}}]}"#;

#[tokio::test]
async fn simulate_custom_policy_allows_matching_action() {
    let server = TestServer::start().await;
    let iam = server.iam_client().await;

    let resp = iam
        .simulate_custom_policy()
        .policy_input_list(ALLOW_S3_GET)
        .action_names("s3:GetObject")
        .resource_arns("arn:aws:s3:::bucket/key")
        .send()
        .await
        .unwrap();

    let results = resp.evaluation_results();
    assert_eq!(results.len(), 1);
    let r = &results[0];
    assert_eq!(r.eval_action_name(), "s3:GetObject");
    assert_eq!(r.eval_resource_name(), Some("arn:aws:s3:::bucket/key"));
    assert_eq!(
        r.eval_decision().as_str(),
        "allowed",
        "expected allowed, got {:?}",
        r.eval_decision()
    );
}

#[tokio::test]
async fn simulate_custom_policy_explicit_deny_wins_over_allow() {
    let server = TestServer::start().await;
    let iam = server.iam_client().await;

    let resp = iam
        .simulate_custom_policy()
        .policy_input_list(ALLOW_S3_DENY_DELETE)
        .action_names("s3:DeleteObject")
        .resource_arns("arn:aws:s3:::bucket/key")
        .send()
        .await
        .unwrap();

    let r = &resp.evaluation_results()[0];
    assert_eq!(
        r.eval_decision().as_str(),
        "explicitDeny",
        "explicit Deny must trump matching Allow"
    );
}

#[tokio::test]
async fn simulate_custom_policy_implicit_deny_when_condition_mismatches() {
    let server = TestServer::start().await;
    let iam = server.iam_client().await;

    // Caller supplies aws:RequestTag/team=blue but the policy only
    // allows team=red. No Deny statement, no matching Allow -> implicit
    // deny. The supplied context key must NOT show up under
    // MissingContextValues.
    let resp = iam
        .simulate_custom_policy()
        .policy_input_list(ALLOW_TAG_CONDITION)
        .action_names("s3:PutObject")
        .resource_arns("arn:aws:s3:::bucket/key")
        .context_entries(
            ContextEntry::builder()
                .context_key_name("aws:RequestTag/team")
                .context_key_values("blue")
                .context_key_type(ContextKeyTypeEnum::String)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let r = &resp.evaluation_results()[0];
    assert_eq!(r.eval_decision().as_str(), "implicitDeny");
    assert!(
        r.missing_context_values().is_empty(),
        "supplied key must not be missing: {:?}",
        r.missing_context_values()
    );
}

#[tokio::test]
async fn simulate_custom_policy_lists_missing_context_values() {
    let server = TestServer::start().await;
    let iam = server.iam_client().await;

    // Caller didn't supply aws:RequestTag/team — simulator should
    // surface it under MissingContextValues so the AWS console UX
    // works the same against fakecloud.
    let resp = iam
        .simulate_custom_policy()
        .policy_input_list(ALLOW_TAG_CONDITION)
        .action_names("s3:PutObject")
        .resource_arns("arn:aws:s3:::bucket/key")
        .send()
        .await
        .unwrap();

    let r = &resp.evaluation_results()[0];
    assert_eq!(r.eval_decision().as_str(), "implicitDeny");
    assert!(
        r.missing_context_values()
            .iter()
            .any(|k| k == "aws:RequestTag/team"),
        "expected missing key surfaced, got {:?}",
        r.missing_context_values()
    );
}

#[tokio::test]
async fn simulate_principal_policy_via_attached_managed_policy() {
    let server = TestServer::start().await;
    let iam = server.iam_client().await;

    iam.create_user().user_name("simuser").send().await.unwrap();
    iam.create_policy()
        .policy_name("AllowS3Get")
        .policy_document(ALLOW_S3_GET)
        .send()
        .await
        .unwrap();
    let policy_arn = "arn:aws:iam::123456789012:policy/AllowS3Get";
    iam.attach_user_policy()
        .user_name("simuser")
        .policy_arn(policy_arn)
        .send()
        .await
        .unwrap();

    let resp = iam
        .simulate_principal_policy()
        .policy_source_arn("arn:aws:iam::123456789012:user/simuser")
        .action_names("s3:GetObject")
        .resource_arns("arn:aws:s3:::bucket/key")
        .send()
        .await
        .unwrap();

    let r = &resp.evaluation_results()[0];
    assert_eq!(r.eval_decision().as_str(), "allowed");
}

#[tokio::test]
async fn simulate_principal_policy_unions_group_attached_policies() {
    let server = TestServer::start().await;
    let iam = server.iam_client().await;

    // The user has zero identity-side policies; the only allow comes
    // from the group's attached managed policy. AWS unions group
    // policies into the user's effective policy set during simulation.
    iam.create_user()
        .user_name("groupuser")
        .send()
        .await
        .unwrap();
    iam.create_group()
        .group_name("readers")
        .send()
        .await
        .unwrap();
    iam.add_user_to_group()
        .group_name("readers")
        .user_name("groupuser")
        .send()
        .await
        .unwrap();

    iam.create_policy()
        .policy_name("GroupAllowGet")
        .policy_document(ALLOW_S3_GET)
        .send()
        .await
        .unwrap();
    iam.attach_group_policy()
        .group_name("readers")
        .policy_arn("arn:aws:iam::123456789012:policy/GroupAllowGet")
        .send()
        .await
        .unwrap();

    let resp = iam
        .simulate_principal_policy()
        .policy_source_arn("arn:aws:iam::123456789012:user/groupuser")
        .action_names("s3:GetObject")
        .resource_arns("arn:aws:s3:::bucket/key")
        .send()
        .await
        .unwrap();

    let r = &resp.evaluation_results()[0];
    assert_eq!(
        r.eval_decision().as_str(),
        "allowed",
        "user must inherit group's attached policy"
    );
}

#[tokio::test]
async fn simulate_principal_policy_via_attached_aws_managed_policy() {
    // SimulatePrincipalPolicy must resolve an attached AWS-managed policy from
    // the built-in catalog (it is never created in this account) and honor its
    // grants.
    let server = TestServer::start().await;
    let iam = server.iam_client().await;

    iam.create_user()
        .user_name("awsmanaged-simuser")
        .send()
        .await
        .unwrap();
    iam.attach_user_policy()
        .user_name("awsmanaged-simuser")
        .policy_arn("arn:aws:iam::aws:policy/AmazonS3FullAccess")
        .send()
        .await
        .unwrap();

    // s3:PutObject is granted by AmazonS3FullAccess (s3:*).
    let allowed = iam
        .simulate_principal_policy()
        .policy_source_arn("arn:aws:iam::123456789012:user/awsmanaged-simuser")
        .action_names("s3:PutObject")
        .resource_arns("arn:aws:s3:::bucket/key")
        .send()
        .await
        .unwrap();
    assert_eq!(
        allowed.evaluation_results()[0].eval_decision().as_str(),
        "allowed"
    );

    // An unrelated service is not granted.
    let denied = iam
        .simulate_principal_policy()
        .policy_source_arn("arn:aws:iam::123456789012:user/awsmanaged-simuser")
        .action_names("dynamodb:PutItem")
        .resource_arns("arn:aws:dynamodb:us-east-1:123456789012:table/t")
        .send()
        .await
        .unwrap();
    assert_eq!(
        denied.evaluation_results()[0].eval_decision().as_str(),
        "implicitDeny"
    );
}
