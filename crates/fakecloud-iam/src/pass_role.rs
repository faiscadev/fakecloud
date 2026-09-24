//! IAM PassRole trust-policy validator.
//!
//! When a service like Lambda or ECS is given a role to assume, the
//! caller's `iam:PassRole` permission is one half of the check; the
//! other half is the role's own trust policy (the
//! `AssumeRolePolicyDocument`). Real AWS rejects the operation when the
//! role's trust policy does not include the relevant service principal
//! in an `Allow` statement, regardless of the caller's identity policy.
//!
//! This module implements the trust-policy half via
//! [`fakecloud_core::auth::RoleTrustValidator`]. The identity-policy
//! half lives in the existing IAM evaluator and is invoked separately
//! at the IAM evaluator boundary.

use std::sync::Arc;

use serde_json::Value;

use fakecloud_core::auth::{PassRoleError, RoleTrustValidator};

use crate::state::SharedIamState;

/// `RoleTrustValidator` impl backed by the in-process IAM state.
pub struct IamRoleTrustValidator {
    state: SharedIamState,
}

impl IamRoleTrustValidator {
    pub fn new(state: SharedIamState) -> Self {
        Self { state }
    }

    pub fn shared(state: SharedIamState) -> Arc<dyn RoleTrustValidator> {
        Arc::new(Self::new(state))
    }
}

/// Parse `arn:<partition>:iam::<account>:role[/<path>]/<name>` into role
/// name, in any partition. Returns `None` if the ARN is not an IAM role ARN.
fn role_name_from_arn(role_arn: &str) -> Option<&str> {
    // Extract the substring after the last "/".
    let (_partition, rest) = role_arn.strip_prefix("arn:")?.split_once(':')?;
    let role_part = rest.strip_prefix("iam::")?;
    let (_account, rest) = role_part.split_once(':')?;
    let role_path = rest.strip_prefix("role/")?;
    role_path.rsplit('/').next()
}

impl RoleTrustValidator for IamRoleTrustValidator {
    /// Validate a role's trust policy against a service principal.
    ///
    /// fakecloud is intentionally permissive when the role is not
    /// present in IAM state: real AWS requires the role to exist, but
    /// long-standing fakecloud tests pass arbitrary role ARNs without
    /// creating IAM roles first. To keep that test culture working we
    /// only reject when the role *does* exist *and* its trust policy
    /// explicitly excludes the calling service principal — the
    /// high-signal failure mode users actually hit.
    fn validate(
        &self,
        account_id: &str,
        role_arn: &str,
        service_principal: &str,
    ) -> Result<(), PassRoleError> {
        let Some(name) = role_name_from_arn(role_arn) else {
            return Ok(());
        };

        let mas = self.state.read();
        let Some(state) = mas.get(account_id) else {
            return Ok(());
        };
        let Some(role) = state.roles.get(name) else {
            return Ok(());
        };

        let parsed: Value = serde_json::from_str(&role.assume_role_policy_document)
            .map_err(|_| PassRoleError::InvalidTrustPolicy(role_arn.to_string()))?;

        if trust_policy_allows(&parsed, service_principal) {
            Ok(())
        } else {
            Err(PassRoleError::TrustPolicyDenies {
                role_arn: role_arn.to_string(),
                service_principal: service_principal.to_string(),
            })
        }
    }
}

/// Returns `true` when the trust policy contains an `Allow` statement
/// with `sts:AssumeRole` in its `Action` list and `Principal.Service`
/// listing the supplied service principal.
fn trust_policy_allows(policy: &Value, service_principal: &str) -> bool {
    let statements = match policy.get("Statement") {
        Some(Value::Array(arr)) => arr.clone(),
        Some(stmt) => vec![stmt.clone()],
        None => return false,
    };

    for stmt in &statements {
        let effect = stmt
            .get("Effect")
            .and_then(Value::as_str)
            .unwrap_or("Allow"); // AWS default is Allow when absent.
        if !effect.eq_ignore_ascii_case("Allow") {
            continue;
        }
        if !action_includes(stmt.get("Action"), "sts:AssumeRole") {
            continue;
        }
        let Some(principal) = stmt.get("Principal") else {
            continue;
        };
        if principal_service_includes(principal, service_principal) {
            return true;
        }
    }
    false
}

fn action_includes(action: Option<&Value>, target: &str) -> bool {
    match action {
        Some(Value::String(s)) => s == target || s == "*",
        Some(Value::Array(arr)) => arr.iter().any(|v| match v {
            Value::String(s) => s == target || s == "*",
            _ => false,
        }),
        _ => false,
    }
}

fn principal_service_includes(principal: &Value, service_principal: &str) -> bool {
    // `"Principal": "*"` (or the equivalent `{"AWS": "*"}`) trusts any
    // principal — service principals included.
    if let Value::String(s) = principal {
        return s == "*";
    }
    if let Some(aws) = principal.get("AWS") {
        match aws {
            Value::String(s) if s == "*" => return true,
            Value::Array(arr) if arr.iter().any(|v| v.as_str() == Some("*")) => return true,
            _ => {}
        }
    }
    let Some(svc) = principal.get("Service") else {
        return false;
    };
    match svc {
        Value::String(s) => s == "*" || s == service_principal,
        Value::Array(arr) => arr.iter().any(|v| match v.as_str() {
            Some("*") => true,
            Some(p) => p == service_principal,
            None => false,
        }),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allow(service: &str) -> Value {
        serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Action": "sts:AssumeRole",
                "Principal": {"Service": service}
            }]
        })
    }

    #[test]
    fn parses_role_name_from_arn() {
        assert_eq!(
            role_name_from_arn("arn:aws:iam::000000000000:role/MyRole"),
            Some("MyRole")
        );
        assert_eq!(
            role_name_from_arn("arn:aws:iam::000000000000:role/service-role/MyRole"),
            Some("MyRole")
        );
        assert_eq!(
            role_name_from_arn("arn:aws-cn:iam::000000000000:role/MyRole"),
            Some("MyRole")
        );
        assert_eq!(
            role_name_from_arn("arn:aws-us-gov:iam::000000000000:role/svc/MyRole"),
            Some("MyRole")
        );
        assert_eq!(
            role_name_from_arn("arn:aws-cn:sts::000000000000:role/MyRole"),
            None
        );
        assert_eq!(role_name_from_arn("not-an-arn"), None);
    }

    #[test]
    fn allows_matching_service_principal() {
        assert!(trust_policy_allows(
            &allow("lambda.amazonaws.com"),
            "lambda.amazonaws.com"
        ));
    }

    #[test]
    fn rejects_other_service_principal() {
        assert!(!trust_policy_allows(
            &allow("ec2.amazonaws.com"),
            "lambda.amazonaws.com"
        ));
    }

    #[test]
    fn allows_array_principal() {
        let policy = serde_json::json!({
            "Statement": [{
                "Effect": "Allow",
                "Action": ["sts:AssumeRole"],
                "Principal": {"Service": ["lambda.amazonaws.com", "ec2.amazonaws.com"]}
            }]
        });
        assert!(trust_policy_allows(&policy, "ec2.amazonaws.com"));
    }

    #[test]
    fn allows_wildcard_principal_string() {
        let policy = serde_json::json!({
            "Statement": [{
                "Effect": "Allow",
                "Action": "sts:AssumeRole",
                "Principal": "*"
            }]
        });
        assert!(trust_policy_allows(&policy, "lambda.amazonaws.com"));
    }

    #[test]
    fn allows_wildcard_principal_aws() {
        let policy = serde_json::json!({
            "Statement": [{
                "Effect": "Allow",
                "Action": "sts:AssumeRole",
                "Principal": {"AWS": "*"}
            }]
        });
        assert!(trust_policy_allows(&policy, "ecs-tasks.amazonaws.com"));
    }

    #[test]
    fn allows_wildcard_service() {
        let policy = serde_json::json!({
            "Statement": [{
                "Effect": "Allow",
                "Action": "sts:AssumeRole",
                "Principal": {"Service": "*"}
            }]
        });
        assert!(trust_policy_allows(&policy, "ec2.amazonaws.com"));
    }

    #[test]
    fn rejects_deny_statement() {
        let policy = serde_json::json!({
            "Statement": [{
                "Effect": "Deny",
                "Action": "sts:AssumeRole",
                "Principal": {"Service": "lambda.amazonaws.com"}
            }]
        });
        assert!(!trust_policy_allows(&policy, "lambda.amazonaws.com"));
    }
}
