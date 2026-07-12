//! Amazon Textract E2E.
//!
//! Exercises the synchronous analysis path, the asynchronous document-text
//! detection job lifecycle, and the custom-adapter + tagging lifecycle against
//! a spawned fakecloud server via the AWS Rust SDK, which speaks the real
//! awsJson1.1 wire format (x-amz-target `Textract.<Op>`):
//!
//!   DetectDocumentText (inline bytes)
//!     -> StartDocumentTextDetection -> GetDocumentTextDetection (SUCCEEDED)
//!     -> CreateAdapter -> GetAdapter -> TagResource / ListTagsForResource
//!     -> DeleteAdapter
//!
//! Textract does not run a real OCR/ML model in fakecloud, so the analysis
//! responses are structural (correct `DocumentMetadata`, empty `Blocks`); this
//! test asserts the API surface, job lifecycle and adapter CRUD, not extracted
//! text.

mod helpers;

use aws_sdk_textract::primitives::Blob;
use aws_sdk_textract::types::{Document, DocumentLocation, FeatureType, JobStatus, S3Object};
use helpers::TestServer;

async fn textract_client(server: &TestServer) -> aws_sdk_textract::Client {
    aws_sdk_textract::Client::new(&server.aws_config().await)
}

#[tokio::test]
async fn textract_round_trip_detect_job_adapter_tags() {
    let server = TestServer::start().await;
    let tx = textract_client(&server).await;

    // ---- synchronous DetectDocumentText on inline bytes -------------------
    let detected = tx
        .detect_document_text()
        .document(
            Document::builder()
                .bytes(Blob::new(b"%PDF-1.4 fake document bytes".to_vec()))
                .build(),
        )
        .send()
        .await
        .expect("detect document text");
    assert!(
        detected
            .document_metadata()
            .and_then(|m| m.pages())
            .is_some(),
        "expected DocumentMetadata.Pages"
    );
    // Blocks is a well-formed (empty) collection, not fabricated text.
    assert!(
        detected.blocks().is_empty(),
        "expected no fabricated blocks"
    );

    // ---- asynchronous text-detection job ----------------------------------
    let started = tx
        .start_document_text_detection()
        .document_location(
            DocumentLocation::builder()
                .s3_object(
                    S3Object::builder()
                        .bucket("my-input-bucket")
                        .name("scans/doc.pdf")
                        .build(),
                )
                .build(),
        )
        .send()
        .await
        .expect("start document text detection");
    let job_id = started.job_id().expect("job id").to_string();
    assert_eq!(job_id.len(), 64, "unexpected job id: {job_id}");

    let got = tx
        .get_document_text_detection()
        .job_id(&job_id)
        .send()
        .await
        .expect("get document text detection");
    assert_eq!(got.job_status(), Some(&JobStatus::Succeeded));

    // An unknown job id yields InvalidJobIdException.
    let bad = tx
        .get_document_text_detection()
        .job_id("this-job-does-not-exist")
        .send()
        .await;
    assert!(bad.is_err(), "expected error for unknown job id");

    // ---- custom adapter lifecycle -----------------------------------------
    let created = tx
        .create_adapter()
        .adapter_name("e2e-adapter")
        .feature_types(FeatureType::Tables)
        .feature_types(FeatureType::Forms)
        .description("adapter created by the fakecloud E2E test")
        .send()
        .await
        .expect("create adapter");
    let adapter_id = created.adapter_id().expect("adapter id").to_string();
    assert!(
        adapter_id.len() >= 12,
        "unexpected adapter id: {adapter_id}"
    );

    let got_adapter = tx
        .get_adapter()
        .adapter_id(&adapter_id)
        .send()
        .await
        .expect("get adapter");
    assert_eq!(got_adapter.adapter_name(), Some("e2e-adapter"));
    assert!(
        got_adapter.feature_types().contains(&FeatureType::Tables),
        "expected TABLES feature type"
    );

    // ---- tagging via the adapter ARN --------------------------------------
    // The SDK signs with the default test credentials, which resolve to the
    // server's default account (123456789012) in us-east-1.
    let arn = format!("arn:aws:textract:us-east-1:123456789012:adapter/{adapter_id}");
    tx.tag_resource()
        .resource_arn(&arn)
        .tags("team", "docs")
        .send()
        .await
        .expect("tag resource");
    let listed = tx
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .expect("list tags");
    assert_eq!(
        listed
            .tags()
            .and_then(|t| t.get("team"))
            .map(String::as_str),
        Some("docs")
    );

    // ---- delete ------------------------------------------------------------
    tx.delete_adapter()
        .adapter_id(&adapter_id)
        .send()
        .await
        .expect("delete adapter");
    let after = tx.get_adapter().adapter_id(&adapter_id).send().await;
    assert!(after.is_err(), "expected ResourceNotFound after delete");
}
