//! Amazon Comprehend (`comprehend`) awsJson1_1 service for fakecloud.
//!
//! The full 85-operation Amazon Comprehend Smithy model: synchronous
//! single-document detection (`DetectEntities` / `DetectSentiment` /
//! `DetectSyntax` / `DetectKeyPhrases` / `DetectDominantLanguage` /
//! `DetectPiiEntities` / `DetectTargetedSentiment` / `ContainsPiiEntities` /
//! `DetectToxicContent` / `ClassifyDocument`), their `BatchDetect*` list
//! variants, nine families of asynchronous analysis jobs
//! (dominant-language / entities / key-phrases / sentiment /
//! targeted-sentiment / PII / events / topics / document-classification), custom
//! document classifiers and entity recognizers (with versioning), real-time
//! endpoints, flywheels and flywheel iterations, datasets, resource policies,
//! `ImportModel`, and ARN-keyed resource tagging.
//!
//! Requests carry `X-Amz-Target: Comprehend_20171127.<Operation>`; dispatch
//! keys off `req.action`. Every operation runs model-driven input validation
//! first (required / length / range / enum / pattern), then real,
//! account-partitioned, persisted CRUD. Each resource is stored as its
//! already-output-valid wire JSON object so a `Describe*` echoes exactly what
//! its `Create*` / `Start*` persisted.
//!
//! The asynchronous lifecycles are modelled by advancing the stored status on
//! the next read (and reconciled again on restart so an interrupted transition
//! never wedges): analysis jobs settle `SUBMITTED` -> `COMPLETED`, custom
//! classifiers and recognizers settle `SUBMITTED` -> `TRAINED`, endpoints settle
//! `CREATING` -> `IN_SERVICE`, flywheels settle `CREATING` -> `ACTIVE`, datasets
//! settle `CREATING` -> `COMPLETED`, and flywheel iterations settle `TRAINING`
//! -> `COMPLETED`. A `Stop*` / `StopTraining*` on an in-flight resource settles
//! it to `STOPPED`.
//!
//! Honest NLP gap: Comprehend's value is the natural-language model that turns
//! text into entities, sentiment, syntax, key phrases, PII spans, and language
//! probabilities. fakecloud does not run any NLP inference. The synchronous
//! detection operations return well-formed, structurally-correct result shapes
//! with empty analysis lists (no fabricated entities / key phrases / syntax /
//! PII spans / languages); `DetectSentiment` returns the model's neutral default
//! (`NEUTRAL`, `SentimentScore` weighted to neutral) rather than an invented
//! judgement. Async jobs, classifiers, recognizers, endpoints, flywheels,
//! datasets, policies, tags, status lifecycle, and persistence are all real.

pub mod persistence;
pub mod service;
pub mod shared;
pub mod state;
mod validate;

pub use service::{ComprehendService, COMPREHEND_ACTIONS};
pub use state::{
    ComprehendData, ComprehendSnapshot, SharedComprehendState, COMPREHEND_SNAPSHOT_SCHEMA_VERSION,
};
