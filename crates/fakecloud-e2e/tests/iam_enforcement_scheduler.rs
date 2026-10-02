//! IAM enforcement tests for the Scheduler service.

mod helpers;

use aws_credential_types::Credentials;
use aws_sdk_iam::Client as IamClient;
use aws_sdk_scheduler::types::{FlexibleTimeWindow, FlexibleTimeWindowMode, Target};
use aws_sdk_scheduler::Client as SchedulerClient;
use helpers::TestServer;

async fn start_strict() -> TestServer {
    TestServer::start_with_env(&[
        ("FAKECLOUD_IAM", "strict"),
        ("FAKECLOUD_VERIFY_SIGV4", "true"),
    ])
    .await
}

async fn sdk_config_with(server: &TestServer, akid: &str, secret: &str) -> aws_config::SdkConfig {
    aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(
            akid,
            secret,
            None,
            None,
            "fakecloud-scheduler-iam",
        ))
        .load()
        .await
}

async fn bootstrap_user(server: &TestServer, name: &str) -> (String, String) {
    let boot = sdk_config_with(server, "test", "test").await;
    let iam_boot = IamClient::new(&boot);
    iam_boot.create_user().user_name(name).send().await.unwrap();
    let ak = iam_boot
        .create_access_key()
        .user_name(name)
        .send()
        .await
        .unwrap();
    let key = ak.access_key().unwrap();
    (
        key.access_key_id().to_string(),
        key.secret_access_key().to_string(),
    )
}

async fn attach_inline_policy(server: &TestServer, user: &str, name: &str, document: &str) {
    let boot = sdk_config_with(server, "test", "test").await;
    IamClient::new(&boot)
        .put_user_policy()
        .user_name(user)
        .policy_name(name)
        .policy_document(document)
        .send()
        .await
        .unwrap();
}

fn off_window() -> FlexibleTimeWindow {
    FlexibleTimeWindow::builder()
        .mode(FlexibleTimeWindowMode::Off)
        .build()
        .unwrap()
}

fn sqs_target() -> Target {
    Target::builder()
        .arn("arn:aws:sqs:us-east-1:000000000000:dest")
        // Same account as the caller: a role can only be passed within its
        // own account.
        .role_arn("arn:aws:iam::123456789012:role/s")
        .build()
        .unwrap()
}

#[tokio::test]
async fn deny_blocks_create_schedule() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "alice").await;
    attach_inline_policy(
        &server,
        "alice",
        "deny-sched",
        r#"{
            "Version":"2012-10-17",
            "Statement":[{"Effect":"Deny","Action":"scheduler:*","Resource":"*"}]
        }"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let client = SchedulerClient::new(&cfg);
    let err = client
        .create_schedule()
        .name("denied")
        .schedule_expression("rate(1 minute)")
        .flexible_time_window(off_window())
        .target(sqs_target())
        .send()
        .await
        .expect_err("Deny should block CreateSchedule");
    assert!(format!("{err:?}").contains("AccessDenied"));
}

#[tokio::test]
async fn allow_permits_create_schedule() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "bob").await;
    attach_inline_policy(
        &server,
        "bob",
        "allow-sched",
        r#"{
            "Version":"2012-10-17",
            "Statement":[{"Effect":"Allow","Action":"scheduler:*","Resource":"*"}]
        }"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let client = SchedulerClient::new(&cfg);
    // The target's RoleArn is handed to Scheduler, so AWS also requires
    // iam:PassRole on it, passed to scheduler.amazonaws.com.
    let err = client
        .create_schedule()
        .name("no-pass-role")
        .schedule_expression("rate(1 minute)")
        .flexible_time_window(off_window())
        .target(sqs_target())
        .send()
        .await
        .expect_err("CreateSchedule without iam:PassRole on the target role");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("AccessDenied") && msg.contains("iam:PassRole"),
        "{msg}"
    );
    attach_inline_policy(
        &server,
        "bob",
        "pass-role",
        r#"{
            "Version":"2012-10-17",
            "Statement":[{"Effect":"Allow","Action":"iam:PassRole",
                "Resource":"arn:aws:iam::123456789012:role/s",
                "Condition":{"StringEquals":{"iam:PassedToService":"scheduler.amazonaws.com"}}}]
        }"#,
    )
    .await;
    client
        .create_schedule()
        .name("allowed")
        .schedule_expression("rate(1 minute)")
        .flexible_time_window(off_window())
        .target(sqs_target())
        .send()
        .await
        .expect("Allow should permit CreateSchedule");
}

#[tokio::test]
async fn schedule_group_condition_key_gates_access() {
    let server = start_strict().await;
    let (akid, secret) = bootstrap_user(&server, "carol").await;
    // Only allow CreateSchedule when ScheduleGroup == "prod".
    attach_inline_policy(
        &server,
        "carol",
        "gate-group",
        r#"{
            "Version":"2012-10-17",
            "Statement":[{
                "Effect":"Allow",
                "Action":"scheduler:*",
                "Resource":"*",
                "Condition":{"StringEquals":{"scheduler:ScheduleGroup":"prod"}}
            },{
                "Effect":"Allow",
                "Action":"iam:PassRole",
                "Resource":"*"
            }]
        }"#,
    )
    .await;

    let cfg = sdk_config_with(&server, &akid, &secret).await;
    let client = SchedulerClient::new(&cfg);
    // Create the prod group using root-bypass so the condition can resolve.
    let root_cfg = sdk_config_with(&server, "test", "test").await;
    SchedulerClient::new(&root_cfg)
        .create_schedule_group()
        .name("prod")
        .send()
        .await
        .unwrap();

    // Wrong group → Deny.
    let err = client
        .create_schedule()
        .name("in-default")
        .schedule_expression("rate(1 minute)")
        .flexible_time_window(off_window())
        .target(sqs_target())
        .send()
        .await
        .expect_err("group 'default' should be blocked by condition");
    assert!(format!("{err:?}").contains("AccessDenied"));

    // Right group → allow.
    client
        .create_schedule()
        .name("in-prod")
        .group_name("prod")
        .schedule_expression("rate(1 minute)")
        .flexible_time_window(off_window())
        .target(sqs_target())
        .send()
        .await
        .expect("group 'prod' should satisfy the condition");
}
