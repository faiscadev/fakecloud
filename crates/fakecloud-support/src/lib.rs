//! AWS Support (`support`) awsJson1_1 service for fakecloud.
//!
//! The full 16-operation AWS Support Smithy model: the Support Cases API
//! (`CreateCase` / `DescribeCases` / `DescribeCommunications` /
//! `AddCommunicationToCase` / `ResolveCase`), attachment sets
//! (`AddAttachmentsToSet` / `DescribeAttachment`), the Trusted Advisor API
//! (`DescribeTrustedAdvisorChecks` / `DescribeTrustedAdvisorCheckResult` /
//! `DescribeTrustedAdvisorCheckSummaries` /
//! `DescribeTrustedAdvisorCheckRefreshStatuses` / `RefreshTrustedAdvisorCheck`),
//! severity levels (`DescribeSeverityLevels`), and the case-creation
//! reference operations (`DescribeServices` / `DescribeCreateCaseOptions` /
//! `DescribeSupportedLanguages`).
//!
//! Requests carry `X-Amz-Target: AWSSupport_20130415.<Operation>`; dispatch
//! keys off `req.action`. Every operation runs model-driven input validation
//! first (required / length / range / enum / pattern), then real,
//! account-partitioned, persisted CRUD.
//!
//! Support cases are real: `CreateCase` mints an AWS-shaped case id
//! (`case-{account}-{year}-{hex}`) and a numeric `displayId`, opens the case
//! `status: "opened"`, and seeds its communication thread with the initial
//! `communicationBody`. `DescribeCases` filters by case-id list / display id /
//! time window / resolved inclusion / language and paginates with a
//! round-tripping `nextToken`. `AddCommunicationToCase` appends to the thread,
//! `DescribeCommunications` pages it, and `ResolveCase` flips the case to
//! `resolved`, returning the initial and final status. Attachment sets are real
//! (`AddAttachmentsToSet` mints/extends an `attachmentSetId` with an
//! `expiryTime`; `DescribeAttachment` returns a stored attachment by id).
//! Severity levels and the Trusted Advisor check catalogue are faithful static
//! AWS reference data; the Trusted Advisor refresh status is a real per-check
//! `none -> enqueued -> processing -> success` state machine.
//!
//! Honest gap: fakecloud does not run the real Trusted Advisor analysis engine
//! or attach a live AWS Support agent. `DescribeTrustedAdvisorCheckResult` and
//! `DescribeTrustedAdvisorCheckSummaries` return well-formed, structurally
//! correct result shapes reporting zero flagged resources (an all-clear
//! account) rather than fabricating findings, and no automated support-agent
//! reply is generated for a case. Cases, communications, attachment sets,
//! severity levels, the check catalogue, and the refresh state machine are all
//! real, account-partitioned, and persisted.

mod catalog;
pub mod persistence;
pub mod service;
pub mod shared;
pub mod state;
mod validate;

pub use service::{SupportService, SUPPORT_ACTIONS};
pub use state::{
    SharedSupportState, SupportData, SupportSnapshot, SUPPORT_SNAPSHOT_SCHEMA_VERSION,
};
