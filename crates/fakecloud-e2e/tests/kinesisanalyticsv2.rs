//! Amazon Managed Service for Apache Flink (kinesisanalyticsv2) control-plane E2E.
//!
//! Exercises the full lifecycle against a spawned fakecloud server via the AWS
//! Rust SDK, which speaks the real awsJson1.1 wire format (x-amz-target
//! `KinesisAnalytics_20180523.<Op>`):
//!
//!   create -> describe -> start (settles RUNNING) -> snapshot -> stop
//!          -> update (version bump) -> delete
//!
//! This is a pure control-plane test — no real Flink container is spawned; the
//! lifecycle auto-settles in memory on describe. The Docker-backed Flink job
//! data plane is a separate later batch.

mod helpers;

use aws_sdk_kinesisanalyticsv2::types::RuntimeEnvironment;
use helpers::TestServer;

async fn ka2_client(server: &TestServer) -> aws_sdk_kinesisanalyticsv2::Client {
    aws_sdk_kinesisanalyticsv2::Client::new(&server.aws_config().await)
}

#[tokio::test]
async fn application_lifecycle_create_start_snapshot_stop_update_delete() {
    let server = TestServer::start().await;
    let ka2 = ka2_client(&server).await;

    let role = "arn:aws:iam::000000000000:role/service-role/kinesis-analytics";

    // CreateApplication -> READY, version 1.
    let created = ka2
        .create_application()
        .application_name("e2e-flink-app")
        .runtime_environment(RuntimeEnvironment::from("FLINK_1_20"))
        .service_execution_role(role)
        .application_description("e2e managed flink app")
        .send()
        .await
        .expect("create application");
    let detail = created.application_detail().expect("application detail");
    assert_eq!(detail.application_status().as_str(), "READY");
    assert_eq!(detail.application_version_id(), 1);
    let arn = detail.application_arn().to_string();
    assert!(
        arn.ends_with(":application/e2e-flink-app"),
        "unexpected ARN: {arn}"
    );
    let create_ts = *detail.create_timestamp().expect("create timestamp");

    // DescribeApplication echoes the description.
    let described = ka2
        .describe_application()
        .application_name("e2e-flink-app")
        .send()
        .await
        .expect("describe application");
    assert_eq!(
        described
            .application_detail()
            .and_then(|d| d.application_description()),
        Some("e2e managed flink app")
    );

    // StartApplication -> STARTING, settles RUNNING on the next describe.
    ka2.start_application()
        .application_name("e2e-flink-app")
        .send()
        .await
        .expect("start application");
    let after_start = ka2
        .describe_application()
        .application_name("e2e-flink-app")
        .send()
        .await
        .expect("describe after start");
    assert_eq!(
        after_start
            .application_detail()
            .map(|d| d.application_status().as_str()),
        Some("RUNNING")
    );

    // CreateApplicationSnapshot -> READY snapshot.
    ka2.create_application_snapshot()
        .application_name("e2e-flink-app")
        .snapshot_name("snap-1")
        .send()
        .await
        .expect("create snapshot");
    let snap = ka2
        .describe_application_snapshot()
        .application_name("e2e-flink-app")
        .snapshot_name("snap-1")
        .send()
        .await
        .expect("describe snapshot");
    assert_eq!(
        snap.snapshot_details()
            .map(|s| s.snapshot_status().as_str()),
        Some("READY")
    );

    // StopApplication -> STOPPING, settles READY on the next describe.
    ka2.stop_application()
        .application_name("e2e-flink-app")
        .send()
        .await
        .expect("stop application");
    let after_stop = ka2
        .describe_application()
        .application_name("e2e-flink-app")
        .send()
        .await
        .expect("describe after stop");
    assert_eq!(
        after_stop
            .application_detail()
            .map(|d| d.application_status().as_str()),
        Some("READY")
    );

    // UpdateApplication bumps the version.
    let updated = ka2
        .update_application()
        .application_name("e2e-flink-app")
        .service_execution_role_update(role)
        .send()
        .await
        .expect("update application");
    assert!(
        updated
            .application_detail()
            .map(|d| d.application_version_id())
            .unwrap_or(0)
            > 1,
        "version should bump past 1"
    );

    // ListApplications sees it.
    let listed = ka2.list_applications().send().await.expect("list");
    assert!(listed
        .application_summaries()
        .iter()
        .any(|s| s.application_name() == "e2e-flink-app"));

    // DeleteApplication (with the CreateTimestamp condition) removes it.
    ka2.delete_application()
        .application_name("e2e-flink-app")
        .create_timestamp(create_ts)
        .send()
        .await
        .expect("delete application");
    let err = ka2
        .describe_application()
        .application_name("e2e-flink-app")
        .send()
        .await;
    assert!(err.is_err(), "application should be gone after delete");
}
