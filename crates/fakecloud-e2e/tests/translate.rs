//! Amazon Translate (translate) control-plane E2E.
//!
//! Exercises the synchronous translate, custom-terminology, batch-job, and
//! tagging lifecycle against a spawned fakecloud server via the AWS Rust SDK,
//! which speaks the real awsJson1.1 wire format (x-amz-target
//! `AWSShineFrontendService_20170701.<Op>`):
//!
//!   TranslateText -> ImportTerminology -> GetTerminology -> ListTerminologies
//!     -> StartTextTranslationJob -> DescribeTextTranslationJob (COMPLETED)
//!     -> TagResource / ListTagsForResource -> DeleteTerminology
//!
//! Honest machine-translation gap: no MT model runs, so `TranslateText` echoes
//! the input verbatim as `TranslatedText` with the requested target language
//! applied. Everything else -- terminologies (languages/term-count parsed from
//! the imported CSV), batch-job records, status lifecycle, tags -- is real,
//! persisted control-plane state.

mod helpers;

use aws_sdk_translate::primitives::Blob;
use aws_sdk_translate::types::{
    InputDataConfig, JobStatus, MergeStrategy, OutputDataConfig, Tag, TerminologyData,
    TerminologyDataFormat,
};
use helpers::TestServer;

async fn translate_client(server: &TestServer) -> aws_sdk_translate::Client {
    aws_sdk_translate::Client::new(&server.aws_config().await)
}

#[tokio::test]
async fn translate_terminology_and_batch_job_lifecycle() {
    let server = TestServer::start().await;
    let tr = translate_client(&server).await;

    // TranslateText: honest passthrough echoes the input as the translation.
    let translated = tr
        .translate_text()
        .text("hello world")
        .source_language_code("en")
        .target_language_code("fr")
        .send()
        .await
        .expect("translate text");
    assert_eq!(translated.translated_text(), "hello world");
    assert_eq!(translated.source_language_code(), "en");
    assert_eq!(translated.target_language_code(), "fr");

    // ImportTerminology from a CSV: source/target languages + term count are
    // parsed out of the uploaded file.
    let csv = "en,fr\nhello,bonjour\ndog,chien\ncat,chat\n";
    tr.import_terminology()
        .name("e2e-term")
        .merge_strategy(MergeStrategy::Overwrite)
        .terminology_data(
            TerminologyData::builder()
                .file(Blob::new(csv.as_bytes()))
                .format(TerminologyDataFormat::Csv)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("import terminology");

    let got = tr
        .get_terminology()
        .name("e2e-term")
        .send()
        .await
        .expect("get terminology");
    let props = got.terminology_properties().expect("props");
    assert_eq!(props.name(), Some("e2e-term"));
    assert_eq!(props.source_language_code(), Some("en"));
    assert_eq!(props.target_language_codes(), ["fr".to_string()]);
    assert_eq!(props.term_count(), Some(3));
    assert!(
        got.terminology_data_location().is_some(),
        "terminology has a downloadable data location"
    );

    // ListTerminologies sees it.
    let listed = tr
        .list_terminologies()
        .send()
        .await
        .expect("list terminologies");
    assert!(
        listed
            .terminology_properties_list()
            .iter()
            .any(|t| t.name() == Some("e2e-term")),
        "terminology should appear in ListTerminologies"
    );

    // StartTextTranslationJob -> SUBMITTED; DescribeTextTranslationJob settles
    // it to COMPLETED on read.
    let started = tr
        .start_text_translation_job()
        .job_name("e2e-job")
        .input_data_config(
            InputDataConfig::builder()
                .s3_uri("s3://my-input-bucket/docs/")
                .content_type("text/plain")
                .build()
                .unwrap(),
        )
        .output_data_config(
            OutputDataConfig::builder()
                .s3_uri("s3://my-output-bucket/out/")
                .build()
                .unwrap(),
        )
        .data_access_role_arn("arn:aws:iam::000000000000:role/TranslateBatchRole")
        .source_language_code("en")
        .target_language_codes("fr")
        .client_token("e2e-token-1")
        .send()
        .await
        .expect("start text translation job");
    let job_id = started.job_id().expect("job id").to_string();
    assert_eq!(started.job_status(), Some(&JobStatus::Submitted));

    let described = tr
        .describe_text_translation_job()
        .job_id(&job_id)
        .send()
        .await
        .expect("describe job");
    let jprops = described
        .text_translation_job_properties()
        .expect("job props");
    assert_eq!(jprops.job_status(), Some(&JobStatus::Completed));
    assert_eq!(jprops.job_name(), Some("e2e-job"));

    // Tag the terminology and read the tags back.
    let arn = "arn:aws:translate:us-east-1:000000000000:terminology/e2e-term".to_string();
    tr.tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("team").value("loc").build().unwrap())
        .send()
        .await
        .expect("tag resource");
    let tags = tr
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .expect("list tags");
    assert!(
        tags.tags()
            .iter()
            .any(|t| t.key() == "team" && t.value() == "loc"),
        "tag should be present"
    );

    // DeleteTerminology -> subsequent GetTerminology 404s.
    tr.delete_terminology()
        .name("e2e-term")
        .send()
        .await
        .expect("delete terminology");
    let after = tr.get_terminology().name("e2e-term").send().await;
    assert!(after.is_err(), "deleted terminology should not be found");
}
