//! Amazon Translate (`translate`) awsJson1_1 service for fakecloud.
//!
//! The full 19-operation Amazon Translate Smithy model: synchronous text and
//! document translation (`TranslateText` / `TranslateDocument`), asynchronous
//! batch translation jobs (`StartTextTranslationJob` / `DescribeTextTranslationJob`
//! / `ListTextTranslationJobs` / `StopTextTranslationJob`), parallel data
//! (`CreateParallelData` / `GetParallelData` / `UpdateParallelData` /
//! `ListParallelData` / `DeleteParallelData`), custom terminologies
//! (`ImportTerminology` / `GetTerminology` / `ListTerminologies` /
//! `DeleteTerminology`), the supported-language catalogue (`ListLanguages`),
//! and ARN-keyed resource tagging (`TagResource` / `UntagResource` /
//! `ListTagsForResource`).
//!
//! Requests carry `X-Amz-Target: AWSShineFrontendService_20170701.<Operation>`;
//! dispatch keys off `req.action`. Every operation runs model-driven input
//! validation first (required / length / range / enum / pattern), then real,
//! account-partitioned, persisted CRUD. Each resource is stored as its
//! already-output-valid wire JSON object so a `Get*` echoes exactly what its
//! `Create*` / `Import*` / `Start*` persisted.
//!
//! The asynchronous lifecycles are modelled by advancing the stored status on
//! the next read (and reconciled again on restart so an interrupted transition
//! never wedges): batch translation jobs settle `SUBMITTED` -> `COMPLETED`
//! (`STOP_REQUESTED` -> `STOPPED`), parallel data settles `CREATING` ->
//! `ACTIVE` (and an update's `LatestUpdateAttemptStatus` `UPDATING` ->
//! `ACTIVE`).
//!
//! Honest machine-translation gap: Amazon Translate's value is the neural MT
//! model that rewrites text from one language into another. fakecloud runs no
//! MT model, so `TranslateText` / `TranslateDocument` return a structurally
//! correct response that echoes the input content verbatim as the
//! `TranslatedText` / `TranslatedDocument` with the requested target language
//! applied. No fabricated translation is presented as if a real model produced
//! it. Everything else -- terminology parsing (languages / term counts read
//! from the imported CSV/TSV file), parallel-data records, batch-job records,
//! tags, status lifecycle, and persistence -- is real.

pub mod persistence;
pub mod service;
pub mod shared;
pub mod state;
mod validate;

pub use service::{TranslateService, TRANSLATE_ACTIONS};
pub use state::{
    SharedTranslateState, TranslateData, TranslateSnapshot, TRANSLATE_SNAPSHOT_SCHEMA_VERSION,
};
