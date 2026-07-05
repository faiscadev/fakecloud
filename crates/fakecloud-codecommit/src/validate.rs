//! Enum sets and input-validation helpers for AWS CodeCommit.
//!
//! Enum value lists are transcribed verbatim from the CodeCommit Smithy model
//! (`aws-models/codecommit.json`). A value outside the model set is rejected
//! with the operation's declared `Invalid*Exception`, matching AWS.

pub const APPROVAL_STATE: &[&str] = &["APPROVE", "REVOKE"];

/// `ChangeTypeEnum`: the difference kind surfaced by `GetDifferences`. Emitted
/// as string literals in the handler; kept here for the model record.
#[allow(dead_code)]
pub const CHANGE_TYPE: &[&str] = &["A", "M", "D"];

pub const CONFLICT_DETAIL_LEVEL: &[&str] = &["FILE_LEVEL", "LINE_LEVEL"];

pub const CONFLICT_RESOLUTION_STRATEGY: &[&str] =
    &["NONE", "ACCEPT_SOURCE", "ACCEPT_DESTINATION", "AUTOMERGE"];

pub const FILE_MODE: &[&str] = &["EXECUTABLE", "NORMAL", "SYMLINK"];

pub const MERGE_OPTION: &[&str] = &["FAST_FORWARD_MERGE", "SQUASH_MERGE", "THREE_WAY_MERGE"];

/// `ObjectTypeEnum`: the kind of a folder entry. Recorded from the model.
#[allow(dead_code)]
pub const OBJECT_TYPE: &[&str] = &["FILE", "DIRECTORY", "GIT_LINK", "SYMBOLIC_LINK"];

pub const ORDER: &[&str] = &["ascending", "descending"];

pub const OVERRIDE_STATUS: &[&str] = &["OVERRIDE", "REVOKE"];

pub const PULL_REQUEST_EVENT_TYPE: &[&str] = &[
    "PULL_REQUEST_CREATED",
    "PULL_REQUEST_STATUS_CHANGED",
    "PULL_REQUEST_SOURCE_REFERENCE_UPDATED",
    "PULL_REQUEST_MERGE_STATE_CHANGED",
    "PULL_REQUEST_APPROVAL_RULE_CREATED",
    "PULL_REQUEST_APPROVAL_RULE_UPDATED",
    "PULL_REQUEST_APPROVAL_RULE_DELETED",
    "PULL_REQUEST_APPROVAL_RULE_OVERRIDDEN",
    "PULL_REQUEST_APPROVAL_STATE_CHANGED",
];

pub const PULL_REQUEST_STATUS: &[&str] = &["OPEN", "CLOSED"];

/// `RelativeFileVersionEnum`: which side of a compared commit a comment
/// location refers to. Recorded from the model.
#[allow(dead_code)]
pub const RELATIVE_FILE_VERSION: &[&str] = &["BEFORE", "AFTER"];

/// `ReplacementTypeEnum`: how a conflict resolution replaces a file. Recorded
/// from the model for the merge-resolution inputs.
#[allow(dead_code)]
pub const REPLACEMENT_TYPE: &[&str] = &[
    "KEEP_BASE",
    "KEEP_SOURCE",
    "KEEP_DESTINATION",
    "USE_NEW_CONTENT",
];

pub const REPOSITORY_TRIGGER_EVENT: &[&str] = &[
    "all",
    "updateReference",
    "createReference",
    "deleteReference",
];

pub const SORT_BY: &[&str] = &["repositoryName", "lastModifiedDate"];

pub const REACTION_EMOJIS: &[&str] = &[
    ":thumbsup:",
    ":thumbsdown:",
    ":smile:",
    ":confused:",
    ":heart:",
    ":hooray:",
    ":rocket:",
    ":eyes:",
];

/// Whether `value` is a member of the given model enum set.
pub fn is_enum(set: &[&str], value: &str) -> bool {
    set.contains(&value)
}

/// A `RepositoryName`: 1-100 chars matching `^[\w\.-]+$`.
pub fn valid_repository_name(v: &str) -> bool {
    let n = v.chars().count();
    (1..=100).contains(&n)
        && v.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'))
}

/// A `BranchName`: 1-256 chars.
pub fn valid_branch_name(v: &str) -> bool {
    let n = v.chars().count();
    (1..=256).contains(&n)
}

/// A `KmsKeyId`: matches `^[a-zA-Z0-9:/_-]+$`.
pub fn valid_kms_key_id(v: &str) -> bool {
    !v.is_empty()
        && v.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, ':' | '/' | '_' | '-'))
}
