//! Enum sets and input-validation helpers for AWS CodeArtifact.
//!
//! Enum value lists are transcribed verbatim from the CodeArtifact Smithy
//! model (`aws-models/codeartifact.json`). A value outside the model set is
//! rejected with the operation's declared `ValidationException`, matching AWS.

/// `PackageFormat` -- the package manager an artifact belongs to.
pub const PACKAGE_FORMAT: &[&str] = &[
    "npm", "pypi", "maven", "nuget", "generic", "ruby", "swift", "cargo",
];

/// `PackageVersionStatus`.
pub const PACKAGE_VERSION_STATUS: &[&str] = &[
    "Published",
    "Unfinished",
    "Unlisted",
    "Archived",
    "Disposed",
    "Deleted",
];

/// `PackageVersionSortType` for `ListPackageVersions`.
pub const PACKAGE_VERSION_SORT: &[&str] = &["PUBLISHED_TIME"];

/// `PackageVersionOriginType`.
pub const PACKAGE_VERSION_ORIGIN_TYPE: &[&str] = &["INTERNAL", "EXTERNAL", "UNKNOWN"];

/// `EndpointType` for `GetRepositoryEndpoint`.
pub const ENDPOINT_TYPE: &[&str] = &["dualstack", "ipv4"];

/// `AllowPublish` / `AllowUpstream` package-origin restriction values.
pub const ALLOW_BLOCK: &[&str] = &["ALLOW", "BLOCK"];

/// `PackageGroupOriginRestrictionType`.
pub const ORIGIN_RESTRICTION_TYPE: &[&str] = &["EXTERNAL_UPSTREAM", "INTERNAL_UPSTREAM", "PUBLISH"];

/// `PackageGroupOriginRestrictionMode`.
pub const ORIGIN_RESTRICTION_MODE: &[&str] =
    &["ALLOW", "ALLOW_SPECIFIC_REPOSITORIES", "BLOCK", "INHERIT"];

/// Whether `value` is a member of the given model enum set.
pub fn is_enum(set: &[&str], value: &str) -> bool {
    set.contains(&value)
}

/// Validate a `DomainName` against the model `@length` (2..=50) and `@pattern`
/// (`^[a-z][a-z0-9\-]{0,48}[a-z0-9]$`).
pub fn is_domain_name(name: &str) -> bool {
    let len = name.chars().count();
    if !(2..=50).contains(&len) {
        return false;
    }
    let bytes = name.as_bytes();
    let first = bytes[0];
    let last = bytes[len - 1];
    if !first.is_ascii_lowercase() {
        return false;
    }
    if !(last.is_ascii_lowercase() || last.is_ascii_digit()) {
        return false;
    }
    name.bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// Validate a `RepositoryName` against the model `@length` (2..=100) and
/// `@pattern` (`^[A-Za-z0-9][A-Za-z0-9._\-]{1,99}$`).
pub fn is_repository_name(name: &str) -> bool {
    let len = name.chars().count();
    if !(2..=100).contains(&len) {
        return false;
    }
    let first = name.as_bytes()[0];
    if !(first.is_ascii_alphanumeric()) {
        return false;
    }
    name.bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-')
}

/// Validate a 12-digit `AccountId`.
pub fn is_account_id(id: &str) -> bool {
    id.len() == 12 && id.bytes().all(|b| b.is_ascii_digit())
}
