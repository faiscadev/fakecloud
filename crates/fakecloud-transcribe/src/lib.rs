//! Amazon Transcribe (`transcribe`) awsJson1_1 service for fakecloud.
//!
//! The full 43-operation Amazon Transcribe Smithy model: batch transcription
//! jobs (`StartTranscriptionJob` / `GetTranscriptionJob` / `ListTranscriptionJobs`
//! / `DeleteTranscriptionJob`), medical transcription jobs, call-analytics jobs
//! and categories, medical-scribe jobs, custom vocabularies, medical
//! vocabularies, vocabulary filters, custom language models, and ARN-keyed
//! resource tagging (`TagResource` / `UntagResource` / `ListTagsForResource`).
//!
//! Requests carry `X-Amz-Target: Transcribe.<Operation>`; dispatch keys off
//! `req.action`. Every operation runs model-driven input validation first
//! (required / length / range / enum / pattern), then real, account-partitioned,
//! persisted CRUD. Each resource is stored as its already-output-valid wire
//! JSON object so a `Get*` echoes exactly what its `Start*` / `Create*`
//! persisted.
//!
//! The asynchronous lifecycles are modelled by advancing the stored status on
//! the next read (and reconciled again on restart so an interrupted transition
//! never wedges): transcription / medical-transcription / call-analytics /
//! medical-scribe jobs settle `QUEUED` -> `IN_PROGRESS` -> `COMPLETED`,
//! vocabularies and medical vocabularies settle `PENDING` -> `READY`, and custom
//! language models settle `IN_PROGRESS` -> `COMPLETED`.
//!
//! Honest ASR gap: Transcribe's value is the speech-recognition model that turns
//! audio into text. fakecloud does not run speech recognition -- a completed job
//! carries a well-formed `Transcript` whose `TranscriptFileUri` points at the
//! output location the caller requested (or a service-managed location), but no
//! transcript JSON is fabricated at that URI. Everything else -- job records,
//! settings, vocabularies, filters, models, categories, tags, status lifecycle,
//! and persistence -- is real.

pub mod persistence;
pub mod service;
pub mod shared;
pub mod state;
mod validate;

pub use service::{TranscribeService, TRANSCRIBE_ACTIONS};
pub use state::{
    SharedTranscribeState, TranscribeData, TranscribeSnapshot, TRANSCRIBE_SNAPSHOT_SCHEMA_VERSION,
};
