//! Amazon Transcribe (transcribe) control-plane E2E.
//!
//! Exercises the transcription-job + vocabulary + tagging lifecycle against a
//! spawned fakecloud server via the AWS Rust SDK, which speaks the real
//! awsJson1.1 wire format (x-amz-target `Transcribe.<Op>`):
//!
//!   StartTranscriptionJob -> GetTranscriptionJob (settles COMPLETED)
//!     -> ListTranscriptionJobs -> CreateVocabulary -> GetVocabulary
//!     -> TagResource / ListTagsForResource -> DeleteVocabulary
//!     -> DeleteTranscriptionJob
//!
//! Honest ASR gap: a completed job carries a well-formed `Transcript` whose
//! `TranscriptFileUri` points at the requested output location, but no
//! transcript JSON is produced there (fakecloud does not run speech
//! recognition). Everything else is real, persisted control-plane state.

mod helpers;

use aws_sdk_transcribe::types::{Media, TranscriptionJobStatus, VocabularyState};
use helpers::TestServer;

async fn transcribe_client(server: &TestServer) -> aws_sdk_transcribe::Client {
    aws_sdk_transcribe::Client::new(&server.aws_config().await)
}

#[tokio::test]
async fn transcription_job_and_vocabulary_lifecycle() {
    let server = TestServer::start().await;
    let tx = transcribe_client(&server).await;

    // StartTranscriptionJob -> QUEUED job with the requested media echoed back.
    let media = Media::builder()
        .media_file_uri("s3://my-input-bucket/meeting.wav")
        .build();
    let started = tx
        .start_transcription_job()
        .transcription_job_name("e2e-job")
        .language_code(aws_sdk_transcribe::types::LanguageCode::EnUs)
        .media(media)
        .output_bucket_name("my-output-bucket")
        .send()
        .await
        .expect("start transcription job");
    let job = started.transcription_job().expect("job");
    assert_eq!(job.transcription_job_name(), Some("e2e-job"));
    assert_eq!(
        job.media().and_then(|m| m.media_file_uri()),
        Some("s3://my-input-bucket/meeting.wav")
    );

    // Duplicate name -> ConflictException.
    let dup = tx
        .start_transcription_job()
        .transcription_job_name("e2e-job")
        .media(Media::builder().media_file_uri("s3://b/a.wav").build())
        .send()
        .await;
    assert!(dup.is_err(), "duplicate job name should conflict");

    // GetTranscriptionJob settles the job to COMPLETED with a Transcript.
    let got = tx
        .get_transcription_job()
        .transcription_job_name("e2e-job")
        .send()
        .await
        .expect("get transcription job");
    let gjob = got.transcription_job().expect("job");
    assert_eq!(
        gjob.transcription_job_status(),
        Some(&TranscriptionJobStatus::Completed)
    );
    let uri = gjob
        .transcript()
        .and_then(|t| t.transcript_file_uri())
        .expect("transcript file uri");
    assert!(
        uri.contains("my-output-bucket"),
        "transcript uri should point at the output bucket: {uri}"
    );

    // ListTranscriptionJobs sees it.
    let listed = tx
        .list_transcription_jobs()
        .send()
        .await
        .expect("list transcription jobs");
    assert!(
        listed
            .transcription_job_summaries()
            .iter()
            .any(|s| s.transcription_job_name() == Some("e2e-job")),
        "job should appear in ListTranscriptionJobs"
    );

    // CreateVocabulary -> PENDING; GetVocabulary settles it to READY.
    tx.create_vocabulary()
        .vocabulary_name("e2e-vocab")
        .language_code(aws_sdk_transcribe::types::LanguageCode::EnUs)
        .phrases("Amazon")
        .phrases("Transcribe")
        .send()
        .await
        .expect("create vocabulary");
    let gv = tx
        .get_vocabulary()
        .vocabulary_name("e2e-vocab")
        .send()
        .await
        .expect("get vocabulary");
    assert_eq!(gv.vocabulary_state(), Some(&VocabularyState::Ready));
    assert!(gv.download_uri().is_some(), "vocabulary has a download URI");

    // Tag the vocabulary and read the tags back.
    let arn = "arn:aws:transcribe:us-east-1:000000000000:vocabulary/e2e-vocab".to_string();
    tx.tag_resource()
        .resource_arn(&arn)
        .tags(
            aws_sdk_transcribe::types::Tag::builder()
                .key("team")
                .value("asr")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("tag resource");
    let tags = tx
        .list_tags_for_resource()
        .resource_arn(&arn)
        .send()
        .await
        .expect("list tags");
    assert!(
        tags.tags()
            .iter()
            .any(|t| t.key() == "team" && t.value() == "asr"),
        "tag should be present"
    );

    // DeleteVocabulary -> subsequent GetVocabulary 404s.
    tx.delete_vocabulary()
        .vocabulary_name("e2e-vocab")
        .send()
        .await
        .expect("delete vocabulary");
    let after = tx
        .get_vocabulary()
        .vocabulary_name("e2e-vocab")
        .send()
        .await;
    assert!(after.is_err(), "deleted vocabulary should not be found");

    // DeleteTranscriptionJob is idempotent and removes the job.
    tx.delete_transcription_job()
        .transcription_job_name("e2e-job")
        .send()
        .await
        .expect("delete transcription job");
    let gone = tx
        .get_transcription_job()
        .transcription_job_name("e2e-job")
        .send()
        .await;
    assert!(gone.is_err(), "deleted job should not be found");
}
