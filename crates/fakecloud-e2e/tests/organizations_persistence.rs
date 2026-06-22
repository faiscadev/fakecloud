mod helpers;

use aws_sdk_organizations::types::PolicyType;
use helpers::TestServer;

const DENY_ALL: &str =
    r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"*","Resource":"*"}]}"#;

async fn wait_for_account_succeeded(
    orgs: &aws_sdk_organizations::Client,
    request_id: &str,
) -> String {
    for _ in 0..60 {
        let status = orgs
            .describe_create_account_status()
            .create_account_request_id(request_id)
            .send()
            .await
            .unwrap()
            .create_account_status
            .unwrap();
        if status.state().map(|s| s.as_str()) == Some("SUCCEEDED") {
            return status.account_id().unwrap().to_string();
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    panic!("create account never SUCCEEDED");
}

/// The organization, an OU, an SCP, and an asynchronously-created member
/// account all survive a restart in persistent mode. The marker OU created
/// after the account reaches SUCCEEDED forces a durable snapshot capturing the
/// async completion.
#[tokio::test]
async fn persistence_round_trip_org_ou_policy_account() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let orgs = server.organizations_client().await;

    orgs.create_organization().send().await.unwrap();
    let root_id = orgs.list_roots().send().await.unwrap().roots()[0]
        .id()
        .unwrap()
        .to_string();

    let ou_id = orgs
        .create_organizational_unit()
        .parent_id(&root_id)
        .name("Engineering")
        .send()
        .await
        .unwrap()
        .organizational_unit
        .unwrap()
        .id
        .unwrap();

    let policy_id = orgs
        .create_policy()
        .name("deny-all")
        .description("deny everything")
        .content(DENY_ALL)
        .r#type(PolicyType::ServiceControlPolicy)
        .send()
        .await
        .unwrap()
        .policy
        .unwrap()
        .policy_summary
        .unwrap()
        .id
        .unwrap();

    let req_id = orgs
        .create_account()
        .account_name("Dev")
        .email("dev@example.com")
        .send()
        .await
        .unwrap()
        .create_account_status
        .unwrap()
        .id
        .unwrap();
    let account_id = wait_for_account_succeeded(&orgs, &req_id).await;

    // Force a durable snapshot that captures the SUCCEEDED account.
    orgs.create_organizational_unit()
        .parent_id(&root_id)
        .name("marker")
        .send()
        .await
        .unwrap();

    server.restart().await;
    let orgs = server.organizations_client().await;

    // Org survives.
    assert!(orgs.describe_organization().send().await.is_ok());

    // OU survives.
    assert_eq!(
        orgs.describe_organizational_unit()
            .organizational_unit_id(&ou_id)
            .send()
            .await
            .unwrap()
            .organizational_unit()
            .unwrap()
            .name(),
        Some("Engineering")
    );

    // Policy survives.
    assert!(orgs
        .describe_policy()
        .policy_id(&policy_id)
        .send()
        .await
        .is_ok());

    // The async-created member account survives with its enrollment.
    let accounts = orgs.list_accounts().send().await.unwrap();
    assert!(accounts
        .accounts()
        .iter()
        .any(|a| a.id() == Some(account_id.as_str())));
}

/// A deleted OU stays gone after restart while the org persists.
#[tokio::test]
async fn persistence_delete_ou_survives_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let mut server = TestServer::start_persistent(tmp.path()).await;
    let orgs = server.organizations_client().await;

    orgs.create_organization().send().await.unwrap();
    let root_id = orgs.list_roots().send().await.unwrap().roots()[0]
        .id()
        .unwrap()
        .to_string();
    let ou_id = orgs
        .create_organizational_unit()
        .parent_id(&root_id)
        .name("ephemeral")
        .send()
        .await
        .unwrap()
        .organizational_unit
        .unwrap()
        .id
        .unwrap();
    orgs.delete_organizational_unit()
        .organizational_unit_id(&ou_id)
        .send()
        .await
        .unwrap();

    server.restart().await;
    let orgs = server.organizations_client().await;

    assert!(orgs.describe_organization().send().await.is_ok());
    assert!(orgs
        .describe_organizational_unit()
        .organizational_unit_id(&ou_id)
        .send()
        .await
        .is_err());
}
