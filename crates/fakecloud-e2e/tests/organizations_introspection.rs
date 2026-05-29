//! E2E test for the `GET /_fakecloud/organizations/accounts` admin
//! endpoint and the matching `FakeCloud::organizations().get_accounts()`
//! SDK helper. Exercises the full flow: bootstrap admin -> create org ->
//! create + tag + attach-SCP a member account -> hit the introspection
//! endpoint and assert on the curated shape.

mod helpers;

use aws_credential_types::Credentials;
use aws_sdk_organizations::types::CreateAccountState;
use aws_sdk_organizations::Client as OrgsClient;
use fakecloud_sdk::FakeCloud;
use helpers::TestServer;
use std::time::{Duration, Instant};

const MGMT_ACCOUNT: &str = "111111111111";

async fn start() -> TestServer {
    TestServer::start_with_env(&[
        ("FAKECLOUD_IAM", "strict"),
        ("FAKECLOUD_VERIFY_SIGV4", "true"),
        ("FAKECLOUD_CONTAINER_CLI", "false"),
    ])
    .await
}

async fn config_with(server: &TestServer, akid: &str, secret: &str) -> aws_config::SdkConfig {
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(akid, secret, None, None, "orgs-intro"))
        .load()
        .await
}

async fn wait_for_account(orgs: &OrgsClient, request_id: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let resp = orgs
            .describe_create_account_status()
            .create_account_request_id(request_id)
            .send()
            .await
            .unwrap();
        let status = resp.create_account_status().unwrap();
        if matches!(status.state(), Some(&CreateAccountState::Succeeded)) {
            return status.account_id().unwrap().to_string();
        }
        assert!(
            Instant::now() < deadline,
            "CreateAccount {request_id} did not succeed"
        );
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
}

#[tokio::test]
async fn introspection_empty_when_no_org_created() {
    let server = start().await;
    let fc = FakeCloud::new(server.endpoint());

    let snap = fc
        .organizations()
        .get_accounts()
        .await
        .expect("introspection should succeed even with no org");
    assert!(snap.accounts.is_empty());
    assert!(snap.management_account_id.is_none());
    assert!(snap.master_account_id.is_none());
}

#[tokio::test]
async fn introspection_returns_management_member_tags_and_scp() {
    let server = start().await;
    let (akid, secret) = server.create_admin(MGMT_ACCOUNT, "admin").await;
    let cfg = config_with(&server, &akid, &secret).await;
    let orgs = OrgsClient::new(&cfg);

    orgs.create_organization().send().await.unwrap();

    // Create a member account and wait for completion.
    let created = orgs
        .create_account()
        .email("dev@example.com")
        .account_name("Dev")
        .send()
        .await
        .unwrap();
    let request_id = created
        .create_account_status()
        .unwrap()
        .id()
        .unwrap()
        .to_string();
    let member_id = wait_for_account(&orgs, &request_id).await;

    // Tag the member account.
    orgs.tag_resource()
        .resource_id(&member_id)
        .tags(
            aws_sdk_organizations::types::Tag::builder()
                .key("env")
                .value("prod")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Create + attach a custom SCP directly to the member account.
    let policy = orgs
        .create_policy()
        .name("DenyDelete")
        .description("Deny S3 delete")
        .content(
            r#"{"Version":"2012-10-17","Statement":[{"Effect":"Deny","Action":"s3:DeleteBucket","Resource":"*"}]}"#,
        )
        .r#type(aws_sdk_organizations::types::PolicyType::ServiceControlPolicy)
        .send()
        .await
        .unwrap();
    let policy_id = policy
        .policy()
        .unwrap()
        .policy_summary()
        .unwrap()
        .id()
        .unwrap()
        .to_string();
    orgs.attach_policy()
        .policy_id(&policy_id)
        .target_id(&member_id)
        .send()
        .await
        .unwrap();

    // Hit introspection.
    let fc = FakeCloud::new(server.endpoint());
    let snap = fc
        .organizations()
        .get_accounts()
        .await
        .expect("introspection");

    assert_eq!(snap.management_account_id.as_deref(), Some(MGMT_ACCOUNT));
    assert_eq!(snap.master_account_id.as_deref(), Some(MGMT_ACCOUNT));
    assert_eq!(snap.accounts.len(), 2, "management + member");

    let mgmt = snap
        .accounts
        .iter()
        .find(|a| a.id == MGMT_ACCOUNT)
        .expect("management account present");
    assert_eq!(mgmt.status, "ACTIVE");
    assert!(mgmt.parent_ou_id.is_some());
    assert!(mgmt.scp_attached.is_empty());

    let member = snap
        .accounts
        .iter()
        .find(|a| a.id == member_id)
        .expect("member account present");
    assert_eq!(member.status, "ACTIVE");
    assert_eq!(member.email, "dev@example.com");
    assert_eq!(member.name, "Dev");
    assert_eq!(member.joined_method, "CREATED");
    assert_eq!(member.tags.len(), 1);
    assert_eq!(member.tags[0].key, "env");
    assert_eq!(member.tags[0].value, "prod");
    assert_eq!(member.scp_attached, vec![policy_id]);
}

#[tokio::test]
async fn responsibility_transfers_and_firehose_streams_introspection() {
    use aws_sdk_firehose::types::{
        DeliveryStreamEncryptionConfigurationInput, ExtendedS3DestinationConfiguration, KeyType,
    };
    use aws_sdk_organizations::types::{
        HandshakeParty, HandshakePartyType, ResponsibilityTransferType,
    };

    let server = start().await;
    let (akid, secret) = server.create_admin(MGMT_ACCOUNT, "admin").await;
    let cfg = config_with(&server, &akid, &secret).await;
    let orgs = OrgsClient::new(&cfg);

    orgs.create_organization().send().await.unwrap();

    // Invite an outbound BILLING responsibility transfer.
    orgs.invite_organization_to_transfer_responsibility()
        .r#type(ResponsibilityTransferType::Billing)
        .source_name("my-billing-transfer")
        .target(
            HandshakeParty::builder()
                .id("222222222222")
                .r#type(HandshakePartyType::Account)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("invite transfer");

    // Create a Firehose delivery stream and enable SSE on it.
    let s3 = aws_sdk_s3::Client::new(&cfg);
    let _ = s3.create_bucket().bucket("rt-fh-bucket").send().await;
    let firehose = aws_sdk_firehose::Client::new(&cfg);
    firehose
        .create_delivery_stream()
        .delivery_stream_name("fh-intro")
        .extended_s3_destination_configuration(
            ExtendedS3DestinationConfiguration::builder()
                .role_arn("arn:aws:iam::123456789012:role/firehose")
                .bucket_arn("arn:aws:s3:::rt-fh-bucket")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create stream");
    firehose
        .start_delivery_stream_encryption()
        .delivery_stream_name("fh-intro")
        .delivery_stream_encryption_configuration_input(
            DeliveryStreamEncryptionConfigurationInput::builder()
                .key_type(KeyType::AwsOwnedCmk)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("enable encryption");

    let fc = FakeCloud::new(server.endpoint());

    // Responsibility-transfers endpoint.
    let transfers = fc
        .organizations()
        .get_responsibility_transfers()
        .await
        .expect("transfers introspection");
    assert_eq!(transfers.responsibility_transfers.len(), 1);
    let t = &transfers.responsibility_transfers[0];
    assert_eq!(t.name, "my-billing-transfer");
    assert_eq!(t.transfer_type, "BILLING");
    assert_eq!(t.status, "REQUESTED");
    assert_eq!(t.direction, "OUTBOUND");
    assert_eq!(t.source_management_account_id, MGMT_ACCOUNT);
    assert_eq!(t.target_management_account_id, "222222222222");
    assert!(t.end_timestamp.is_none());
    assert!(t.active_handshake_id.is_some());

    // Firehose delivery-streams endpoint.
    let streams = fc
        .firehose()
        .get_delivery_streams()
        .await
        .expect("firehose introspection");
    assert_eq!(streams.delivery_streams.len(), 1);
    let s = &streams.delivery_streams[0];
    assert_eq!(s.name, "fh-intro");
    assert_eq!(s.account_id, MGMT_ACCOUNT);
    assert_eq!(s.stream_type, "DirectPut");
    assert_eq!(s.status, "ACTIVE");
    assert_eq!(s.encryption.status, "ENABLED");
    assert_eq!(s.destination_count, 1);
    assert!(s.last_update_timestamp.is_some());
}
