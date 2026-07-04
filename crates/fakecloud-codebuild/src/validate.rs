//! Enum sets and input validation helpers for AWS CodeBuild.
//!
//! Enum value lists are transcribed verbatim from the CodeBuild Smithy model
//! (`aws-models/codebuild.json`). A value outside the model set is rejected
//! with `InvalidInputException`, matching AWS.

pub const SOURCE_TYPE: &[&str] = &[
    "CODECOMMIT",
    "CODEPIPELINE",
    "GITHUB",
    "GITLAB",
    "GITLAB_SELF_MANAGED",
    "S3",
    "BITBUCKET",
    "GITHUB_ENTERPRISE",
    "NO_SOURCE",
];

pub const ARTIFACTS_TYPE: &[&str] = &["CODEPIPELINE", "S3", "NO_ARTIFACTS"];

pub const ENVIRONMENT_TYPE: &[&str] = &[
    "WINDOWS_CONTAINER",
    "LINUX_CONTAINER",
    "LINUX_GPU_CONTAINER",
    "ARM_CONTAINER",
    "WINDOWS_SERVER_2019_CONTAINER",
    "WINDOWS_SERVER_2022_CONTAINER",
    "LINUX_LAMBDA_CONTAINER",
    "ARM_LAMBDA_CONTAINER",
    "LINUX_EC2",
    "ARM_EC2",
    "WINDOWS_EC2",
    "MAC_ARM",
];

pub const COMPUTE_TYPE: &[&str] = &[
    "BUILD_GENERAL1_SMALL",
    "BUILD_GENERAL1_MEDIUM",
    "BUILD_GENERAL1_LARGE",
    "BUILD_GENERAL1_XLARGE",
    "BUILD_GENERAL1_2XLARGE",
    "BUILD_LAMBDA_1GB",
    "BUILD_LAMBDA_2GB",
    "BUILD_LAMBDA_4GB",
    "BUILD_LAMBDA_8GB",
    "BUILD_LAMBDA_10GB",
    "ATTRIBUTE_BASED_COMPUTE",
    "CUSTOM_INSTANCE_TYPE",
];

pub const REPORT_TYPE: &[&str] = &["TEST", "CODE_COVERAGE"];

/// Build / build-batch settled status values (the `StatusType` model enum),
/// used to validate the `BuildBatchFilter.status` filter.
pub const STATUS_TYPE: &[&str] = &[
    "SUCCEEDED",
    "FAILED",
    "FAULT",
    "TIMED_OUT",
    "IN_PROGRESS",
    "STOPPED",
];

pub const SERVER_TYPE: &[&str] = &[
    "GITHUB",
    "BITBUCKET",
    "GITHUB_ENTERPRISE",
    "GITLAB",
    "GITLAB_SELF_MANAGED",
];

pub const AUTH_TYPE: &[&str] = &[
    "OAUTH",
    "BASIC_AUTH",
    "PERSONAL_ACCESS_TOKEN",
    "CODECONNECTIONS",
    "SECRETS_MANAGER",
];

pub const PROJECT_VISIBILITY: &[&str] = &["PUBLIC_READ", "PRIVATE"];

pub const SORT_ORDER: &[&str] = &["ASCENDING", "DESCENDING"];

pub const PROJECT_SORT_BY: &[&str] = &["NAME", "CREATED_TIME", "LAST_MODIFIED_TIME"];

pub const REPORT_GROUP_SORT_BY: &[&str] = &["NAME", "CREATED_TIME", "LAST_MODIFIED_TIME"];

pub const FLEET_SORT_BY: &[&str] = &["NAME", "CREATED_TIME", "LAST_MODIFIED_TIME"];

pub const SHARED_RESOURCE_SORT_BY: &[&str] = &["ARN", "MODIFIED_TIME"];

pub const CODE_COVERAGE_SORT_BY: &[&str] = &["LINE_COVERAGE_PERCENTAGE", "FILE_PATH"];

pub const REPORT_EXPORT_CONFIG_TYPE: &[&str] = &["S3", "NO_EXPORT"];

pub const FLEET_OVERFLOW_BEHAVIOR: &[&str] = &["QUEUE", "ON_DEMAND"];

/// Source types that support webhooks (the provider-hosted Git backends).
pub const WEBHOOK_SOURCE_TYPE: &[&str] = &[
    "GITHUB",
    "GITHUB_ENTERPRISE",
    "BITBUCKET",
    "GITLAB",
    "GITLAB_SELF_MANAGED",
];

/// Whether `value` is a member of the given model enum set.
pub fn is_enum(set: &[&str], value: &str) -> bool {
    set.contains(&value)
}
