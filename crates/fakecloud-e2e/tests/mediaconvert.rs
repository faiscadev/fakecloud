//! End-to-end tests for AWS Elemental MediaConvert, driven through the real
//! `aws-sdk-mediaconvert` client against a live fakecloud server. Exercises the
//! transcoding control plane (create queue -> get/list -> create preset ->
//! create job template -> create job -> get job settles COMPLETE -> list jobs ->
//! tag/list-tags -> delete queue), asserting resource state round-trips and the
//! job lifecycle settles `SUBMITTED` -> `COMPLETE` on read.
//!
//! MediaConvert normally uses account-specific endpoints discovered via
//! `DescribeEndpoints`; here the client is pinned to the fakecloud endpoint via
//! the shared `TestServer` config, so every call routes to the running server.

// `DescribeEndpoints` is deprecated in the AWS SDK (account-specific endpoints
// are no longer required), but fakecloud still implements it and we assert it
// returns a well-formed endpoint, so allow the deprecation for this test.
#![allow(deprecated)]

use aws_sdk_mediaconvert::types::{JobSettings, JobTemplateSettings, PresetSettings};
use fakecloud_testkit::TestServer;

async fn mediaconvert_client(server: &TestServer) -> aws_sdk_mediaconvert::Client {
    let conf = aws_sdk_mediaconvert::config::Builder::from(&server.aws_config().await).build();
    aws_sdk_mediaconvert::Client::from_conf(conf)
}

#[tokio::test]
async fn mediaconvert_full_lifecycle() {
    let server = TestServer::start().await;
    let client = mediaconvert_client(&server).await;

    // --- DescribeEndpoints echoes a well-formed endpoint URL ---
    let endpoints = client
        .describe_endpoints()
        .send()
        .await
        .expect("describe_endpoints")
        .endpoints
        .unwrap_or_default();
    assert!(!endpoints.is_empty());
    assert!(endpoints[0].url().unwrap_or_default().starts_with("http"));

    // --- Create queue ---
    let queue = client
        .create_queue()
        .name("e2e-queue")
        .description("end-to-end queue")
        .send()
        .await
        .expect("create_queue")
        .queue
        .expect("queue present");
    assert_eq!(queue.name(), Some("e2e-queue"));
    assert_eq!(
        queue.r#type(),
        Some(&aws_sdk_mediaconvert::types::Type::Custom)
    );
    assert!(queue
        .arn()
        .unwrap_or_default()
        .contains(":queues/e2e-queue"));

    // --- Get queue (round-trips the create inputs) ---
    let got = client
        .get_queue()
        .name("e2e-queue")
        .send()
        .await
        .expect("get_queue")
        .queue
        .expect("queue present");
    assert_eq!(got.description(), Some("end-to-end queue"));

    // --- List queues (includes the seeded Default queue) ---
    let queues = client
        .list_queues()
        .send()
        .await
        .expect("list_queues")
        .queues
        .unwrap_or_default();
    assert!(queues.iter().any(|q| q.name() == Some("e2e-queue")));
    assert!(queues.iter().any(|q| q.name() == Some("Default")));

    // --- Update queue ---
    client
        .update_queue()
        .name("e2e-queue")
        .description("updated queue")
        .status(aws_sdk_mediaconvert::types::QueueStatus::Paused)
        .send()
        .await
        .expect("update_queue");
    let got = client
        .get_queue()
        .name("e2e-queue")
        .send()
        .await
        .expect("get after update")
        .queue
        .expect("queue present");
    assert_eq!(got.description(), Some("updated queue"));
    assert_eq!(
        got.status(),
        Some(&aws_sdk_mediaconvert::types::QueueStatus::Paused)
    );

    // --- Create preset ---
    let preset = client
        .create_preset()
        .name("e2e-preset")
        .settings(PresetSettings::builder().build())
        .send()
        .await
        .expect("create_preset")
        .preset
        .expect("preset present");
    assert_eq!(preset.name(), Some("e2e-preset"));

    // --- Create job template ---
    let tpl = client
        .create_job_template()
        .name("e2e-template")
        .settings(JobTemplateSettings::builder().build())
        .send()
        .await
        .expect("create_job_template")
        .job_template
        .expect("job template present");
    assert_eq!(tpl.name(), Some("e2e-template"));

    // --- Create job (settles SUBMITTED -> COMPLETE on read) ---
    let job = client
        .create_job()
        .role("arn:aws:iam::000000000000:role/MediaConvertRole")
        .settings(JobSettings::builder().build())
        .send()
        .await
        .expect("create_job")
        .job
        .expect("job present");
    let job_id = job.id().expect("job id").to_string();
    assert_eq!(
        job.status(),
        Some(&aws_sdk_mediaconvert::types::JobStatus::Submitted)
    );

    let got = client
        .get_job()
        .id(&job_id)
        .send()
        .await
        .expect("get_job")
        .job
        .expect("job present");
    assert_eq!(
        got.status(),
        Some(&aws_sdk_mediaconvert::types::JobStatus::Complete)
    );
    assert_eq!(got.job_percent_complete(), Some(100));

    // --- List jobs ---
    let jobs = client
        .list_jobs()
        .send()
        .await
        .expect("list_jobs")
        .jobs
        .unwrap_or_default();
    assert!(jobs.iter().any(|j| j.id() == Some(job_id.as_str())));

    // --- Policy round-trip ---
    client
        .put_policy()
        .policy(
            aws_sdk_mediaconvert::types::Policy::builder()
                .s3_inputs(aws_sdk_mediaconvert::types::InputPolicy::Disallowed)
                .build(),
        )
        .send()
        .await
        .expect("put_policy");
    let policy = client
        .get_policy()
        .send()
        .await
        .expect("get_policy")
        .policy
        .expect("policy present");
    assert_eq!(
        policy.s3_inputs(),
        Some(&aws_sdk_mediaconvert::types::InputPolicy::Disallowed)
    );

    // --- Tagging (queue ARN) ---
    let queue_arn = queue.arn().unwrap().to_string();
    client
        .tag_resource()
        .arn(&queue_arn)
        .tags("team", "media")
        .tags("env", "prod")
        .send()
        .await
        .expect("tag_resource");
    let tags = client
        .list_tags_for_resource()
        .arn(&queue_arn)
        .send()
        .await
        .expect("list_tags")
        .resource_tags
        .and_then(|rt| rt.tags)
        .unwrap_or_default();
    assert_eq!(tags.get("team").map(String::as_str), Some("media"));
    assert_eq!(tags.get("env").map(String::as_str), Some("prod"));

    client
        .untag_resource()
        .arn(&queue_arn)
        .tag_keys("env")
        .send()
        .await
        .expect("untag_resource");
    let tags = client
        .list_tags_for_resource()
        .arn(&queue_arn)
        .send()
        .await
        .expect("list_tags after untag")
        .resource_tags
        .and_then(|rt| rt.tags)
        .unwrap_or_default();
    assert!(tags.contains_key("team"));
    assert!(!tags.contains_key("env"));

    // --- Delete queue ---
    client
        .delete_queue()
        .name("e2e-queue")
        .send()
        .await
        .expect("delete_queue");
    let err = client
        .get_queue()
        .name("e2e-queue")
        .send()
        .await
        .expect_err("get after delete should fail");
    assert!(err.into_service_error().is_not_found_exception());
}

#[tokio::test]
async fn mediaconvert_get_missing_queue_is_not_found() {
    let server = TestServer::start().await;
    let client = mediaconvert_client(&server).await;
    let err = client
        .get_queue()
        .name("nonexistent-queue")
        .send()
        .await
        .expect_err("missing queue");
    assert!(err.into_service_error().is_not_found_exception());
}

#[tokio::test]
async fn mediaconvert_default_queue_cannot_be_deleted() {
    let server = TestServer::start().await;
    let client = mediaconvert_client(&server).await;
    let err = client
        .delete_queue()
        .name("Default")
        .send()
        .await
        .expect_err("delete Default should fail");
    // Deleting the seeded Default queue is a conflict.
    assert!(err.into_service_error().is_conflict_exception());
}
