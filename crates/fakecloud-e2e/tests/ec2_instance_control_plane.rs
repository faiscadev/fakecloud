//! EC2 instance control-plane correctness E2E (no container runtime needed —
//! these exercise metadata-only behavior). Covers bug-hunt 2026-06-15 findings:
//! 0.5/0.6 attribute + Modify*Options round-trip, 1.8 MaxCount, 1.9 existence +
//! illegal transitions, 1.16 filter wildcards, 1.17 pagination, and the async
//! `pending -> running` lifecycle (0.1/0.2) observed without Docker.

mod helpers;

use aws_sdk_ec2::types::{AttributeBooleanValue, BlobAttributeValue, InstanceAttributeName};
use helpers::TestServer;

/// Launch instances and return their ids (no AMI required in metadata mode).
async fn run(c: &aws_sdk_ec2::Client, min: i32, max: i32) -> Vec<String> {
    let resp = c
        .run_instances()
        .image_id("ami-12345678")
        .min_count(min)
        .max_count(max)
        .send()
        .await
        .unwrap();
    resp.instances()
        .iter()
        .filter_map(|i| i.instance_id().map(str::to_string))
        .collect()
}

#[tokio::test]
async fn run_instances_honors_max_count() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    // MaxCount=5 (>= MinCount) launches 5 instances, not MinCount.
    let ids = run(&c, 1, 5).await;
    assert_eq!(ids.len(), 5, "MaxCount should drive the launch count");
}

#[tokio::test]
async fn run_instances_rejects_min_gt_max() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let err = c
        .run_instances()
        .image_id("ami-12345678")
        .min_count(5)
        .max_count(2)
        .send()
        .await
        .expect_err("MinCount > MaxCount must be rejected");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("InvalidParameterValue") || msg.contains("maxCount"),
        "expected InvalidParameterValue, got {msg}"
    );
}

#[tokio::test]
async fn run_instances_rejects_count_over_limit_without_panic() {
    // Regression: MinCount/MaxCount above the per-request ceiling used to panic
    // the server (`clamp` with lo > hi), dropping the connection. AWS returns
    // `InstanceLimitExceeded`; the server must respond with that, not crash.
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    for (min, max) in [(100, 100), (65, 65), (1000, 1000), (65, 200)] {
        let err = c
            .run_instances()
            .image_id("ami-12345678")
            .min_count(min)
            .max_count(max)
            .send()
            .await
            .expect_err("over-limit count must return an EC2 error, not crash");
        let msg = format!("{err:?}");
        assert!(
            msg.contains("InstanceLimitExceeded"),
            "expected InstanceLimitExceeded for {min}/{max}, got {msg}"
        );
    }
    // The server is still alive after the rejected requests (no panic).
    let ids = run(&c, 1, 1).await;
    assert_eq!(ids.len(), 1, "server must survive over-limit requests");
}

#[tokio::test]
async fn run_instances_returns_pending_then_running() {
    // A tiny image keeps any backing-container boot fast; `tail -f /dev/null`
    // keeps it alive. When no container runtime is present the background task
    // flips the instance to `running` immediately.
    std::env::set_var("FAKECLOUD_EC2_DEFAULT_IMAGE", "alpine:3");
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let resp = c
        .run_instances()
        .image_id("ami-12345678")
        .min_count(1)
        .max_count(1)
        .send()
        .await
        .unwrap();
    // AWS returns the instance in `pending` immediately — RunInstances must not
    // block on the container coming up (findings 0.1/0.2).
    let inst = &resp.instances()[0];
    assert_eq!(
        inst.state().and_then(|st| st.name()).map(|n| n.as_str()),
        Some("pending"),
    );
    let id = inst.instance_id().unwrap().to_string();

    // The background task reconciles it to `running` once the container is up;
    // poll DescribeInstances to observe the transition.
    let mut running = false;
    for _ in 0..120 {
        let d = c
            .describe_instances()
            .instance_ids(&id)
            .send()
            .await
            .unwrap();
        if d.reservations()
            .iter()
            .flat_map(|r| r.instances())
            .any(|i| i.state().and_then(|st| st.name()).map(|n| n.as_str()) == Some("running"))
        {
            running = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    assert!(running, "instance never reconciled to running");
}

#[tokio::test]
async fn state_change_on_unknown_id_is_not_found() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let err = c
        .stop_instances()
        .instance_ids("i-00000000000000000")
        .send()
        .await
        .expect_err("unknown instance id must fail");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("InvalidInstanceID.NotFound"),
        "expected InvalidInstanceID.NotFound, got {msg}"
    );
}

#[tokio::test]
async fn terminated_instance_cannot_be_started() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let ids = run(&c, 1, 1).await;
    let id = &ids[0];
    c.terminate_instances()
        .instance_ids(id)
        .send()
        .await
        .unwrap();
    let err = c
        .start_instances()
        .instance_ids(id)
        .send()
        .await
        .expect_err("starting a terminated instance must fail");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("IncorrectInstanceState"),
        "expected IncorrectInstanceState, got {msg}"
    );
}

#[tokio::test]
async fn modify_instance_attribute_round_trips() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run(&c, 1, 1).await.remove(0);

    // disableApiTermination: set true, read back true.
    c.modify_instance_attribute()
        .instance_id(&id)
        .disable_api_termination(AttributeBooleanValue::builder().value(true).build())
        .send()
        .await
        .unwrap();
    let r = c
        .describe_instance_attribute()
        .instance_id(&id)
        .attribute(InstanceAttributeName::DisableApiTermination)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.disable_api_termination().and_then(|v| v.value()),
        Some(true),
        "disableApiTermination did not persist"
    );

    // sourceDestCheck defaults true; set false, read back false.
    c.modify_instance_attribute()
        .instance_id(&id)
        .source_dest_check(AttributeBooleanValue::builder().value(false).build())
        .send()
        .await
        .unwrap();
    let r = c
        .describe_instance_attribute()
        .instance_id(&id)
        .attribute(InstanceAttributeName::SourceDestCheck)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.source_dest_check().and_then(|v| v.value()),
        Some(false),
        "sourceDestCheck did not persist"
    );

    // ResetInstanceAttribute restores sourceDestCheck to the AWS default (true).
    c.reset_instance_attribute()
        .instance_id(&id)
        .attribute(InstanceAttributeName::SourceDestCheck)
        .send()
        .await
        .unwrap();
    let r = c
        .describe_instance_attribute()
        .instance_id(&id)
        .attribute(InstanceAttributeName::SourceDestCheck)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.source_dest_check().and_then(|v| v.value()),
        Some(true),
        "ResetInstanceAttribute should restore the default"
    );
}

#[tokio::test]
async fn disable_api_termination_blocks_terminate() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run(&c, 1, 1).await.remove(0);
    c.modify_instance_attribute()
        .instance_id(&id)
        .disable_api_termination(AttributeBooleanValue::builder().value(true).build())
        .send()
        .await
        .unwrap();
    let err = c
        .terminate_instances()
        .instance_ids(&id)
        .send()
        .await
        .expect_err("termination protection must block Terminate");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("OperationNotPermitted"),
        "expected OperationNotPermitted, got {msg}"
    );
}

#[tokio::test]
async fn modify_instance_metadata_options_round_trips() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run(&c, 1, 1).await.remove(0);
    // Harden IMDS to v2 (httpTokens=required).
    let resp = c
        .modify_instance_metadata_options()
        .instance_id(&id)
        .http_tokens(aws_sdk_ec2::types::HttpTokensState::Required)
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.instance_metadata_options()
            .and_then(|m| m.http_tokens())
            .map(|t| t.as_str()),
        Some("required"),
        "ModifyInstanceMetadataOptions did not echo the requested value"
    );

    // DescribeInstances must reflect the hardened setting (round-trip).
    let d = c
        .describe_instances()
        .instance_ids(&id)
        .send()
        .await
        .unwrap();
    let tokens = d
        .reservations()
        .iter()
        .flat_map(|r| r.instances())
        .find(|i| i.instance_id() == Some(id.as_str()))
        .and_then(|i| i.metadata_options())
        .and_then(|m| m.http_tokens())
        .map(|t| t.as_str().to_string());
    assert_eq!(
        tokens.as_deref(),
        Some("required"),
        "metadataOptions httpTokens did not round-trip through DescribeInstances"
    );
}

#[tokio::test]
async fn modify_instance_attribute_generic_form_persists_user_data() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run(&c, 1, 1).await.remove(0);
    // The SDK base64-encodes the blob on the wire; the server stores and
    // returns that base64 form. "echo hi" -> base64 "ZWNobyBoaQ==".
    c.modify_instance_attribute()
        .instance_id(&id)
        .user_data(
            BlobAttributeValue::builder()
                .value(aws_smithy_types::Blob::new("echo hi".as_bytes()))
                .build(),
        )
        .send()
        .await
        .unwrap();
    let r = c
        .describe_instance_attribute()
        .instance_id(&id)
        .attribute(InstanceAttributeName::UserData)
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.user_data().and_then(|v| v.value()),
        Some("ZWNobyBoaQ=="),
        "userData did not persist"
    );
}

#[tokio::test]
async fn describe_instances_filter_supports_wildcards() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let id = run(&c, 1, 1).await.remove(0);
    c.create_tags()
        .resources(&id)
        .tags(
            aws_sdk_ec2::types::Tag::builder()
                .key("Name")
                .value("web-prod-01")
                .build(),
        )
        .send()
        .await
        .unwrap();

    // tag:Name=web* must match via the trailing wildcard.
    let d = c
        .describe_instances()
        .filters(
            aws_sdk_ec2::types::Filter::builder()
                .name("tag:Name")
                .values("web*")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let matched: Vec<&str> = d
        .reservations()
        .iter()
        .flat_map(|r| r.instances())
        .filter_map(|i| i.instance_id())
        .collect();
    assert!(
        matched.contains(&id.as_str()),
        "web* should match web-prod-01"
    );

    // A non-matching wildcard returns nothing.
    let d = c
        .describe_instances()
        .filters(
            aws_sdk_ec2::types::Filter::builder()
                .name("tag:Name")
                .values("db*")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let count = d.reservations().iter().flat_map(|r| r.instances()).count();
    assert_eq!(count, 0, "db* must not match web-prod-01");
}

#[tokio::test]
async fn describe_instances_unknown_filter_matches_nothing() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let _ = run(&c, 1, 2).await;
    // An unknown filter name must NOT return all instances (was `return true`).
    let d = c
        .describe_instances()
        .filters(
            aws_sdk_ec2::types::Filter::builder()
                .name("totally-unknown-filter")
                .values("anything")
                .build(),
        )
        .send()
        .await
        .unwrap();
    let count = d.reservations().iter().flat_map(|r| r.instances()).count();
    assert_eq!(count, 0, "unknown filter must not match-all");
}

#[tokio::test]
async fn describe_instances_paginates_with_next_token() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let _ = run(&c, 1, 6).await;

    // Page size 5: first page returns 5 instances + a NextToken.
    let page1 = c.describe_instances().max_results(5).send().await.unwrap();
    let n1 = page1
        .reservations()
        .iter()
        .flat_map(|r| r.instances())
        .count();
    assert_eq!(n1, 5, "first page should hold MaxResults instances");
    let token = page1.next_token().expect("a NextToken when more remain");

    // Second page returns the remaining instance and no further token.
    let page2 = c
        .describe_instances()
        .max_results(5)
        .next_token(token)
        .send()
        .await
        .unwrap();
    let n2 = page2
        .reservations()
        .iter()
        .flat_map(|r| r.instances())
        .count();
    assert_eq!(n2, 1, "second page should hold the remainder");
    assert!(page2.next_token().is_none(), "no token past the last page");
}

#[tokio::test]
async fn describe_instances_rejects_out_of_range_max_results() {
    let s = TestServer::start().await;
    let c = s.ec2_client().await;
    let err = c
        .describe_instances()
        .max_results(1) // below the 5 minimum
        .send()
        .await
        .expect_err("MaxResults below minimum must be rejected");
    let msg = format!("{err:?}");
    assert!(
        msg.contains("InvalidParameterValue") || msg.contains("MaxResults"),
        "expected InvalidParameterValue, got {msg}"
    );
}
