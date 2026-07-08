//! Amazon Comprehend (comprehend) control + inference plane E2E.
//!
//! Exercises the synchronous detection, async analysis-job, flywheel, and
//! tagging lifecycle against a spawned fakecloud server via the AWS Rust SDK,
//! which speaks the real awsJson1.1 wire format (x-amz-target
//! `Comprehend_20171127.<Op>`):
//!
//!   DetectSentiment (neutral default)
//!     -> StartSentimentDetectionJob -> DescribeSentimentDetectionJob (settles
//!        COMPLETED) -> ListSentimentDetectionJobs
//!     -> CreateFlywheel -> DescribeFlywheel (settles ACTIVE)
//!     -> TagResource / ListTagsForResource
//!
//! Honest NLP gap: fakecloud runs no natural-language inference, so
//! `DetectSentiment` returns the model's neutral default and detection lists
//! come back empty. Everything else -- job records, status lifecycle, flywheels,
//! tags, and persistence -- is real, persisted control-plane state.

mod helpers;

use aws_sdk_comprehend::types::{JobStatus, LanguageCode, SentimentType};
use helpers::TestServer;

async fn comprehend_client(server: &TestServer) -> aws_sdk_comprehend::Client {
    aws_sdk_comprehend::Client::new(&server.aws_config().await)
}

#[tokio::test]
async fn detection_job_and_flywheel_lifecycle() {
    let server = TestServer::start().await;
    let cp = comprehend_client(&server).await;

    // DetectSentiment returns the neutral default (no ML inference is run).
    let sentiment = cp
        .detect_sentiment()
        .text("fakecloud makes local AWS testing painless")
        .language_code(LanguageCode::En)
        .send()
        .await
        .expect("detect sentiment");
    assert_eq!(sentiment.sentiment(), Some(&SentimentType::Neutral));
    assert_eq!(
        sentiment.sentiment_score().and_then(|s| s.neutral()),
        Some(1.0)
    );

    // A missing required member is rejected before any business logic.
    let bad = cp.detect_sentiment().text("no language code").send().await;
    assert!(bad.is_err(), "missing LanguageCode should be rejected");

    // StartSentimentDetectionJob -> SUBMITTED job with a minted JobId + ARN.
    let input = aws_sdk_comprehend::types::InputDataConfig::builder()
        .s3_uri("s3://my-input-bucket/docs/")
        .build()
        .unwrap();
    let output = aws_sdk_comprehend::types::OutputDataConfig::builder()
        .s3_uri("s3://my-output-bucket/results/")
        .build()
        .unwrap();
    let started = cp
        .start_sentiment_detection_job()
        .job_name("e2e-sentiment")
        .input_data_config(input)
        .output_data_config(output)
        .data_access_role_arn("arn:aws:iam::000000000000:role/comprehend-role")
        .language_code(LanguageCode::En)
        .send()
        .await
        .expect("start sentiment detection job");
    let job_id = started.job_id().expect("job id").to_string();
    assert_eq!(started.job_status(), Some(&JobStatus::Submitted));
    assert!(
        started
            .job_arn()
            .unwrap()
            .contains("sentiment-detection-job"),
        "job arn should carry the family resource type"
    );

    // DescribeSentimentDetectionJob settles the job to COMPLETED.
    let described = cp
        .describe_sentiment_detection_job()
        .job_id(&job_id)
        .send()
        .await
        .expect("describe sentiment detection job");
    let props = described
        .sentiment_detection_job_properties()
        .expect("job properties");
    assert_eq!(props.job_status(), Some(&JobStatus::Completed));
    assert_eq!(props.job_name(), Some("e2e-sentiment"));

    // ListSentimentDetectionJobs sees it.
    let listed = cp
        .list_sentiment_detection_jobs()
        .send()
        .await
        .expect("list sentiment detection jobs");
    assert!(
        listed
            .sentiment_detection_job_properties_list()
            .iter()
            .any(|p| p.job_id() == Some(job_id.as_str())),
        "job should appear in ListSentimentDetectionJobs"
    );

    // CreateFlywheel -> CREATING; DescribeFlywheel settles it to ACTIVE.
    let flywheel = cp
        .create_flywheel()
        .flywheel_name("e2e-flywheel")
        .data_access_role_arn("arn:aws:iam::000000000000:role/comprehend-role")
        .data_lake_s3_uri("s3://my-data-lake/comprehend/")
        .send()
        .await
        .expect("create flywheel");
    let fw_arn = flywheel.flywheel_arn().expect("flywheel arn").to_string();
    let fw = cp
        .describe_flywheel()
        .flywheel_arn(&fw_arn)
        .send()
        .await
        .expect("describe flywheel");
    assert_eq!(
        fw.flywheel_properties().and_then(|p| p.status()),
        Some(&aws_sdk_comprehend::types::FlywheelStatus::Active)
    );

    // Tag the flywheel and read the tags back.
    cp.tag_resource()
        .resource_arn(&fw_arn)
        .tags(
            aws_sdk_comprehend::types::Tag::builder()
                .key("team")
                .value("nlp")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("tag resource");
    let tags = cp
        .list_tags_for_resource()
        .resource_arn(&fw_arn)
        .send()
        .await
        .expect("list tags");
    assert!(
        tags.tags()
            .iter()
            .any(|t| t.key() == "team" && t.value() == Some("nlp")),
        "tag should be present"
    );

    // Tagging an unknown resource is rejected.
    let bad_tag = cp
        .list_tags_for_resource()
        .resource_arn("arn:aws:comprehend:us-east-1:000000000000:flywheel/nope")
        .send()
        .await;
    assert!(bad_tag.is_err(), "unknown resource should not be found");
}
