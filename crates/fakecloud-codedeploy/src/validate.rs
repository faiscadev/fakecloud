//! Enum sets and input-validation helpers for AWS CodeDeploy.
//!
//! Enum value lists are transcribed verbatim from the CodeDeploy Smithy model
//! (`aws-models/codedeploy.json`). A value outside the model set is rejected
//! with the operation's declared `Invalid*Exception`, matching AWS.

pub const COMPUTE_PLATFORM: &[&str] = &["Server", "Lambda", "ECS"];

pub const FILE_EXISTS_BEHAVIOR: &[&str] = &["DISALLOW", "OVERWRITE", "RETAIN"];

pub const OUTDATED_INSTANCES_STRATEGY: &[&str] = &["UPDATE", "IGNORE"];

pub const SORT_ORDER: &[&str] = &["ascending", "descending"];

pub const APPLICATION_REVISION_SORT_BY: &[&str] =
    &["registerTime", "firstUsedTime", "lastUsedTime"];

pub const LIST_STATE_FILTER_ACTION: &[&str] = &["include", "exclude", "ignore"];

pub const REGISTRATION_STATUS: &[&str] = &["Registered", "Deregistered"];

pub const LIFECYCLE_EVENT_STATUS: &[&str] = &[
    "Pending",
    "InProgress",
    "Succeeded",
    "Failed",
    "Skipped",
    "Unknown",
];

pub const DEPLOYMENT_WAIT_TYPE: &[&str] = &["READY_WAIT", "TERMINATION_WAIT"];

pub const MINIMUM_HEALTHY_HOSTS_TYPE: &[&str] = &["HOST_COUNT", "FLEET_PERCENT"];

/// `EC2TagFilterType` and `TagFilterType` (on-premises) share the same member
/// set in the CodeDeploy model.
pub const TAG_FILTER_TYPE: &[&str] = &["KEY_ONLY", "VALUE_ONLY", "KEY_AND_VALUE"];

pub const TRIGGER_EVENT_TYPE: &[&str] = &[
    "DeploymentStart",
    "DeploymentSuccess",
    "DeploymentFailure",
    "DeploymentStop",
    "DeploymentRollback",
    "DeploymentReady",
    "InstanceStart",
    "InstanceSuccess",
    "InstanceFailure",
    "InstanceReady",
];

pub const AUTO_ROLLBACK_EVENT: &[&str] = &[
    "DEPLOYMENT_FAILURE",
    "DEPLOYMENT_STOP_ON_ALARM",
    "DEPLOYMENT_STOP_ON_REQUEST",
];

pub const REVISION_LOCATION_TYPE: &[&str] = &["S3", "GitHub", "String", "AppSpecContent"];

pub const DEPLOYMENT_STATUS: &[&str] = &[
    "Created",
    "Queued",
    "InProgress",
    "Baking",
    "Succeeded",
    "Failed",
    "Stopped",
    "Ready",
];

/// Whether `value` is a member of the given model enum set.
pub fn is_enum(set: &[&str], value: &str) -> bool {
    set.contains(&value)
}
