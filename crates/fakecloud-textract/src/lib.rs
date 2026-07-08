//! Amazon Textract (`textract`) awsJson1_1 service for fakecloud.
//!
//! Implements the full Textract API surface: the synchronous analysis
//! operations (`DetectDocumentText`, `AnalyzeDocument`, `AnalyzeExpense`,
//! `AnalyzeID`), the asynchronous document-processing job operations
//! (`Start*` / `Get*` for text detection, document analysis, expense analysis
//! and lending analysis) with real `JobId` minting and a deterministic
//! `IN_PROGRESS -> SUCCEEDED` lifecycle that settles on `Get`, the custom
//! adapter + adapter-version CRUD lifecycle, and resource tagging. Every
//! operation runs model-driven input validation (required / length / range /
//! enum) and reads/writes real, account-partitioned, persisted state.
//!
//! Honest gap: fakecloud does not run a real OCR / ML inference model, so the
//! analysis operations return a faithful, deterministic *structural* response
//! derived from the input (correct `DocumentMetadata`, well-formed but empty
//! `Blocks` / `ExpenseDocuments` / `IdentityDocuments` / lending `Results`
//! collections) rather than fabricated extracted text. The API surface,
//! validation, job lifecycle, adapters and persistence are all real.

pub mod persistence;
pub(crate) mod service;
pub(crate) mod state;
pub(crate) mod validate;

pub use service::TextractService;
pub use state::{
    SharedTextractState, TextractSnapshot, TextractState, TEXTRACT_SNAPSHOT_SCHEMA_VERSION,
};
