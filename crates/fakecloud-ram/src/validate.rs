//! Model-derived input validation for AWS Resource Access Manager.
//!
//! AWS rejects requests that violate the Smithy model's top-level constraints
//! (`@required`, `@length`, `@range`, `@enum`) before the operation runs. This
//! module encodes those per-operation constraints so omitted / out-of-range /
//! bad-enum inputs are rejected exactly as the real service does. Rules are
//! derived from `aws-models/ram.json`. `@pattern` is intentionally not enforced
//! here: RAM's ARN/name patterns are not exercised by the negative test suite
//! and enforcing them would reject otherwise-valid synthetic inputs.

use http::StatusCode;
use serde_json::Value;

use fakecloud_core::service::{AwsRequest, AwsServiceError};

/// A single input-constraint rule from the Smithy model. Every RAM input
/// member is bound to the JSON body (restJson1 with no `@httpLabel`/`@httpQuery`
/// members), so the source is implicit.
pub enum Rule {
    Required(&'static str),
    LenMin(&'static str, usize),
    LenMax(&'static str, usize),
    RangeMin(&'static str, f64),
    RangeMax(&'static str, f64),
    Enum(&'static str, &'static [&'static str]),
}

// Enum value sets, from the model.
const RESOURCE_OWNER: &[&str] = &["SELF", "OTHER_ACCOUNTS"];
const ASSOCIATION_TYPE: &[&str] = &["PRINCIPAL", "RESOURCE", "SOURCE"];
const ASSOCIATION_STATUS: &[&str] = &[
    "ASSOCIATING",
    "ASSOCIATED",
    "FAILED",
    "DISASSOCIATING",
    "DISASSOCIATED",
    "SUSPENDED",
    "SUSPENDING",
    "RESTORING",
];
const RESOURCE_SHARE_STATUS: &[&str] = &["PENDING", "ACTIVE", "FAILED", "DELETING", "DELETED"];
const REGION_SCOPE_FILTER: &[&str] = &["ALL", "REGIONAL", "GLOBAL"];
const PERMISSION_TYPE_FILTER: &[&str] = &["ALL", "AWS_MANAGED", "CUSTOMER_MANAGED"];
const PERMISSION_FEATURE_SET: &[&str] =
    &["CREATED_FROM_POLICY", "PROMOTING_TO_STANDARD", "STANDARD"];
const WORK_STATUS: &[&str] = &["IN_PROGRESS", "COMPLETED", "FAILED"];

fn bad(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidParameterException", msg)
}

fn value_len(v: &Value) -> Option<usize> {
    match v {
        Value::String(s) => Some(s.chars().count()),
        Value::Array(a) => Some(a.len()),
        Value::Object(o) => Some(o.len()),
        _ => None,
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    v.as_f64()
        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// Validate an operation's request body against the model constraints.
pub fn validate(action: &str, req: &AwsRequest) -> Result<(), AwsServiceError> {
    let rules = input_rules(action);
    if rules.is_empty() {
        return Ok(());
    }
    let body: Value = serde_json::from_slice(&req.body).unwrap_or(Value::Null);
    let raw = |field: &str| -> Option<Value> { body.get(field).cloned().filter(|v| !v.is_null()) };

    for rule in rules {
        match rule {
            Rule::Required(f) => {
                if raw(f).is_none() {
                    return Err(bad(format!("{f} is required.")));
                }
            }
            Rule::LenMin(f, n) => {
                if let Some(len) = raw(f).as_ref().and_then(value_len) {
                    if len < n {
                        return Err(bad(format!("{f} is shorter than the minimum length.")));
                    }
                }
            }
            Rule::LenMax(f, n) => {
                if let Some(len) = raw(f).as_ref().and_then(value_len) {
                    if len > n {
                        return Err(bad(format!("{f} exceeds the maximum length.")));
                    }
                }
            }
            Rule::RangeMin(f, n) => {
                if let Some(x) = raw(f).as_ref().and_then(as_f64) {
                    if x < n {
                        return Err(bad(format!("{f} is below the minimum value.")));
                    }
                }
            }
            Rule::RangeMax(f, n) => {
                if let Some(x) = raw(f).as_ref().and_then(as_f64) {
                    if x > n {
                        return Err(bad(format!("{f} exceeds the maximum value.")));
                    }
                }
            }
            Rule::Enum(f, vals) => {
                if let Some(Value::String(s)) = raw(f) {
                    if !vals.contains(&s.as_str()) {
                        return Err(bad(format!("{f} is not a valid value.")));
                    }
                }
            }
        }
    }
    Ok(())
}

/// Shared pagination range: `MaxResults` is 1..=500 across every list op.
fn max_results_rules() -> Vec<Rule> {
    vec![
        Rule::RangeMin("maxResults", 1.0),
        Rule::RangeMax("maxResults", 500.0),
    ]
}

#[allow(clippy::too_many_lines)]
fn input_rules(action: &str) -> Vec<Rule> {
    let mut r: Vec<Rule> = match action {
        "AcceptResourceShareInvitation" | "RejectResourceShareInvitation" => {
            vec![Rule::Required("resourceShareInvitationArn")]
        }
        "AssociateResourceShare"
        | "DisassociateResourceShare"
        | "UpdateResourceShare"
        | "ListResourceSharePermissions" => {
            vec![Rule::Required("resourceShareArn")]
        }
        // These operations bind their identifiers to `@httpQuery`, not the JSON
        // body; the handlers validate those from the query string. The `Delete*`
        // ops and `PromoteResourceShareCreatedFromPolicy` (single `@httpQuery`
        // `resourceShareArn`, no body members) all fall here.
        "DeletePermission"
        | "DeletePermissionVersion"
        | "DeleteResourceShare"
        | "PromoteResourceShareCreatedFromPolicy" => Vec::new(),
        "AssociateResourceSharePermission" | "DisassociateResourceSharePermission" => vec![
            Rule::Required("resourceShareArn"),
            Rule::Required("permissionArn"),
        ],
        "CreatePermission" => vec![
            Rule::Required("name"),
            Rule::LenMin("name", 1),
            Rule::LenMax("name", 36),
            Rule::Required("resourceType"),
            Rule::Required("policyTemplate"),
        ],
        "CreatePermissionVersion" => vec![
            Rule::Required("permissionArn"),
            Rule::Required("policyTemplate"),
        ],
        "CreateResourceShare" => vec![Rule::Required("name")],
        "GetPermission" | "ListPermissionVersions" => {
            vec![Rule::Required("permissionArn")]
        }
        "SetDefaultPermissionVersion" => vec![
            Rule::Required("permissionArn"),
            Rule::Required("permissionVersion"),
        ],
        "PromotePermissionCreatedFromPolicy" => {
            vec![Rule::Required("permissionArn"), Rule::Required("name")]
        }
        "ReplacePermissionAssociations" => vec![
            Rule::Required("fromPermissionArn"),
            Rule::Required("toPermissionArn"),
        ],
        "GetResourcePolicies" => {
            let mut v = vec![Rule::Required("resourceArns")];
            v.extend(max_results_rules());
            v
        }
        "GetResourceShareAssociations" => {
            let mut v = vec![
                Rule::Required("associationType"),
                Rule::Enum("associationType", ASSOCIATION_TYPE),
                Rule::Enum("associationStatus", ASSOCIATION_STATUS),
            ];
            v.extend(max_results_rules());
            v
        }
        "GetResourceShareInvitations" | "ListReplacePermissionAssociationsWork" => {
            let mut v = max_results_rules();
            if action == "ListReplacePermissionAssociationsWork" {
                v.push(Rule::Enum("status", WORK_STATUS));
            }
            v
        }
        "GetResourceShares" => {
            let mut v = vec![
                Rule::Required("resourceOwner"),
                Rule::Enum("resourceOwner", RESOURCE_OWNER),
                Rule::Enum("resourceShareStatus", RESOURCE_SHARE_STATUS),
            ];
            v.extend(max_results_rules());
            v
        }
        "ListPendingInvitationResources" => {
            let mut v = vec![
                Rule::Required("resourceShareInvitationArn"),
                Rule::Enum("resourceRegionScope", REGION_SCOPE_FILTER),
            ];
            v.extend(max_results_rules());
            v
        }
        "ListPermissionAssociations" => {
            let mut v = vec![
                Rule::Enum("associationStatus", ASSOCIATION_STATUS),
                Rule::Enum("featureSet", PERMISSION_FEATURE_SET),
            ];
            v.extend(max_results_rules());
            v
        }
        "ListPermissions" => {
            let mut v = vec![Rule::Enum("permissionType", PERMISSION_TYPE_FILTER)];
            v.extend(max_results_rules());
            v
        }
        "ListPrincipals" => {
            let mut v = vec![
                Rule::Required("resourceOwner"),
                Rule::Enum("resourceOwner", RESOURCE_OWNER),
            ];
            v.extend(max_results_rules());
            v
        }
        "ListResources" => {
            let mut v = vec![
                Rule::Required("resourceOwner"),
                Rule::Enum("resourceOwner", RESOURCE_OWNER),
                Rule::Enum("resourceRegionScope", REGION_SCOPE_FILTER),
            ];
            v.extend(max_results_rules());
            v
        }
        "ListResourceTypes" => {
            let mut v = vec![Rule::Enum("resourceRegionScope", REGION_SCOPE_FILTER)];
            v.extend(max_results_rules());
            v
        }
        "ListSourceAssociations" => {
            let mut v = vec![Rule::Enum("associationStatus", ASSOCIATION_STATUS)];
            v.extend(max_results_rules());
            v
        }
        "TagResource" => vec![Rule::Required("tags")],
        "UntagResource" => vec![Rule::Required("tagKeys")],
        _ => Vec::new(),
    };
    // `ListPermissionVersions` shares the pagination range with the other list
    // ops but its only required member is handled above.
    if action == "ListPermissionVersions" || action == "ListResourceSharePermissions" {
        r.extend(max_results_rules());
    }
    r
}
