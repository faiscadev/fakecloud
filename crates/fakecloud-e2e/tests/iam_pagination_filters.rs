//! IAM pagination / filter fidelity:
//! - ListPolicies honors OnlyAttached.
//! - GetGroup honors Marker/MaxItems and reports IsTruncated.
//! - ListAccessKeys raises NoSuchEntity for a nonexistent user.

mod helpers;

use aws_sdk_iam::types::PolicyScopeType;
use helpers::TestServer;

const POLICY_DOC: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;

#[tokio::test]
async fn list_policies_only_attached_filters_unattached() {
    let server = TestServer::start().await;
    let iam = server.iam_client().await;

    let attached = iam
        .create_policy()
        .policy_name("attached-pol")
        .policy_document(POLICY_DOC)
        .send()
        .await
        .expect("create attached policy");
    let attached_arn = attached.policy().unwrap().arn().unwrap().to_string();

    iam.create_policy()
        .policy_name("lonely-pol")
        .policy_document(POLICY_DOC)
        .send()
        .await
        .expect("create unattached policy");

    iam.create_user()
        .user_name("pol-user")
        .send()
        .await
        .expect("create user");
    iam.attach_user_policy()
        .user_name("pol-user")
        .policy_arn(&attached_arn)
        .send()
        .await
        .expect("attach policy");

    let listed = iam
        .list_policies()
        .scope(PolicyScopeType::Local)
        .only_attached(true)
        .send()
        .await
        .expect("list_policies only_attached");
    let names: Vec<&str> = listed
        .policies()
        .iter()
        .filter_map(|p| p.policy_name())
        .collect();
    assert!(
        names.contains(&"attached-pol"),
        "attached policy must be listed: {names:?}"
    );
    assert!(
        !names.contains(&"lonely-pol"),
        "unattached policy must be filtered out: {names:?}"
    );
}

#[tokio::test]
async fn get_group_paginates_members() {
    let server = TestServer::start().await;
    let iam = server.iam_client().await;

    iam.create_group()
        .group_name("paged-group")
        .send()
        .await
        .expect("create group");
    for u in ["m-alice", "m-bob", "m-carol"] {
        iam.create_user().user_name(u).send().await.expect("user");
        iam.add_user_to_group()
            .group_name("paged-group")
            .user_name(u)
            .send()
            .await
            .expect("add to group");
    }

    let page1 = iam
        .get_group()
        .group_name("paged-group")
        .max_items(2)
        .send()
        .await
        .expect("get_group page1");
    assert_eq!(page1.users().len(), 2, "first page should hold MaxItems=2");
    assert!(page1.is_truncated(), "first page must be truncated");
    let marker = page1.marker().expect("truncated page returns a marker");

    let page2 = iam
        .get_group()
        .group_name("paged-group")
        .marker(marker)
        .send()
        .await
        .expect("get_group page2");
    assert_eq!(page2.users().len(), 1, "second page should hold remainder");
    assert!(!page2.is_truncated(), "second page must not be truncated");
}

#[tokio::test]
async fn list_access_keys_missing_user_is_no_such_entity() {
    let server = TestServer::start().await;
    let iam = server.iam_client().await;

    let err = iam
        .list_access_keys()
        .user_name("ghost-user")
        .send()
        .await
        .expect_err("missing user must error");
    assert!(
        err.into_service_error().is_no_such_entity_exception(),
        "expected NoSuchEntity for a nonexistent user"
    );
}
