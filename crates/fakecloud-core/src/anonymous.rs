//! How an unsigned (anonymous) request to an AWS API is treated.
//!
//! AWS answers most unsigned API requests with a missing-credentials error
//! before looking at them. A few services authorize the anonymous principal
//! against resource policies instead (S3, SQS), and the operations the Smithy
//! models mark unauthenticated (Cognito's user-facing auth flows, STS's
//! web-identity and SAML federation) are served without credentials.

#[path = "anonymous_gen.rs"]
mod generated;

/// What a service does with an unsigned request to one of its operations
/// ([`crate::service::AwsService::anonymous_access`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnonymousAccess {
    /// Refused for its missing credentials before anything else looks at it.
    Refused,
    /// Authorized as the anonymous principal against the resource's policy
    /// (and ACLs), like any other caller.
    Authorized,
    /// Needs no credentials: served without authorization.
    Unauthenticated,
}

impl AnonymousAccess {
    /// The model's verdict for `operation` of `service` (by signing name):
    /// [`AnonymousAccess::Unauthenticated`] for an operation marked
    /// `smithy.api#optionalAuth` or `smithy.api#auth: []`,
    /// [`AnonymousAccess::Refused`] otherwise.
    pub fn for_operation(service: &str, operation: &str) -> Self {
        if is_unauthenticated_operation(service, operation) {
            Self::Unauthenticated
        } else {
            Self::Refused
        }
    }
}

/// Whether the Smithy model marks `operation` of `service` (its SigV4 signing
/// name) as callable without credentials.
pub fn is_unauthenticated_operation(service: &str, operation: &str) -> bool {
    generated::UNAUTHENTICATED_OPERATIONS
        .binary_search(&(service, operation))
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Re-derives the table from the vendored models, so a model refresh
    /// that adds or drops an unauthenticated operation fails here until
    /// `scripts/generate-unauthenticated-operations.py` is rerun.
    #[test]
    fn table_matches_the_models() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../aws-models");
        let mut derived: Vec<(String, String)> = Vec::new();
        for entry in
            std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        {
            let path = entry.unwrap().path();
            if path.extension().is_none_or(|e| e != "json") {
                continue;
            }
            let text = std::fs::read_to_string(&path).unwrap();
            if !text.contains("smithy.api#optionalAuth") && !text.contains("\"smithy.api#auth\"") {
                continue;
            }
            let model: serde_json::Value = serde_json::from_str(&text).unwrap();
            let Some(shapes) = model["shapes"].as_object() else {
                continue;
            };
            let Some(service) = shapes.values().find(|s| s["type"] == "service") else {
                continue;
            };
            let Some(signing) = service["traits"]["aws.auth#sigv4"]["name"].as_str() else {
                continue;
            };
            let service_open = service["traits"]["smithy.api#auth"]
                .as_array()
                .is_some_and(|a| a.is_empty());
            for (id, shape) in shapes {
                if shape["type"] != "operation" {
                    continue;
                }
                let traits = &shape["traits"];
                let auth = traits.get("smithy.api#auth");
                let open = traits.get("smithy.api#optionalAuth").is_some()
                    || auth
                        .and_then(|a| a.as_array())
                        .is_some_and(|a| a.is_empty())
                    || (auth.is_none() && service_open);
                if open {
                    let op = id.split_once('#').map_or(id.as_str(), |(_, op)| op);
                    derived.push((signing.to_string(), op.to_string()));
                }
            }
        }
        derived.sort();
        derived.dedup();
        let table: Vec<(String, String)> = generated::UNAUTHENTICATED_OPERATIONS
            .iter()
            .map(|(s, o)| (s.to_string(), o.to_string()))
            .collect();
        assert_eq!(
            table, derived,
            "rerun scripts/generate-unauthenticated-operations.py"
        );
    }

    #[test]
    fn table_is_sorted_for_binary_search() {
        assert!(generated::UNAUTHENTICATED_OPERATIONS
            .windows(2)
            .all(|w| w[0] < w[1]));
    }

    #[test]
    fn model_marked_operations_are_unauthenticated() {
        for (service, op) in [
            ("cognito-idp", "InitiateAuth"),
            ("cognito-idp", "SignUp"),
            ("cognito-idp", "ConfirmSignUp"),
            ("cognito-identity", "GetId"),
            ("cognito-identity", "GetOpenIdToken"),
            ("cognito-identity", "GetCredentialsForIdentity"),
            ("sts", "AssumeRoleWithWebIdentity"),
            ("sts", "AssumeRoleWithSAML"),
        ] {
            assert_eq!(
                AnonymousAccess::for_operation(service, op),
                AnonymousAccess::Unauthenticated,
                "{service} {op}"
            );
        }
        for (service, op) in [
            ("sts", "GetCallerIdentity"),
            ("sts", "AssumeRoot"),
            ("cognito-idp", "AdminInitiateAuth"),
            ("cognito-identity", "DescribeIdentityPool"),
            ("kms", "ListKeys"),
        ] {
            assert_eq!(
                AnonymousAccess::for_operation(service, op),
                AnonymousAccess::Refused,
                "{service} {op}"
            );
        }
    }
}
