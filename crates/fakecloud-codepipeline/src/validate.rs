//! Enum sets and input-validation helpers for AWS CodePipeline.
//!
//! Enum value lists are transcribed verbatim from the CodePipeline Smithy
//! model (`aws-models/codepipeline.json`). A value outside the model set is
//! rejected with the operation's declared `ValidationException`, matching AWS.

pub const ACTION_CATEGORY: &[&str] = &[
    "Source", "Build", "Deploy", "Test", "Invoke", "Approval", "Compute",
];

pub const ACTION_OWNER: &[&str] = &["AWS", "ThirdParty", "Custom"];

/// `ActionTypeOwner` for `GetActionType` / `ListActionTypes` filters.
pub const ACTION_TYPE_OWNER: &[&str] = &["AWS", "ThirdParty", "Custom"];

pub const RULE_OWNER: &[&str] = &["AWS"];

pub const EXECUTION_MODE: &[&str] = &["QUEUED", "SUPERSEDED", "PARALLEL"];

pub const PIPELINE_TYPE: &[&str] = &["V1", "V2"];

pub const APPROVAL_STATUS: &[&str] = &["Approved", "Rejected"];

pub const STAGE_TRANSITION_TYPE: &[&str] = &["Inbound", "Outbound"];

pub const STAGE_RETRY_MODE: &[&str] = &["FAILED_ACTIONS", "ALL_ACTIONS"];

pub const CONDITION_TYPE: &[&str] = &["BEFORE_ENTRY", "ON_SUCCESS"];

pub const FAILURE_TYPE: &[&str] = &[
    "JobFailed",
    "ConfigurationError",
    "PermissionError",
    "RevisionOutOfSync",
    "RevisionUnavailable",
    "SystemUnavailable",
];

/// Whether `value` is a member of the given model enum set.
pub fn is_enum(set: &[&str], value: &str) -> bool {
    set.contains(&value)
}
