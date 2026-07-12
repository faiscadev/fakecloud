//! Model-derived input validation for AWS IoT Wireless operations.
//!
//! The rules are generated from the Smithy model (see `src/generated.rs`):
//! for every top-level input member the generator records its wire location
//! (`@httpLabel` / `@httpQuery` / `@httpHeader` / body), whether it is
//! `@required`, and its `@length` / `@range` / `@enum` constraints. This
//! validator enforces exactly those constraints — mirroring the model — so a
//! request that satisfies the model is accepted and one that violates a
//! declared constraint is rejected with the operation's declared client error
//! (`InvalidRequestException`, falling back to `ValidationException` or the
//! operation's first declared error when `InvalidRequestException` is not in
//! its Smithy `errors` list). `@pattern` is intentionally *not* enforced:
//! IoT Wireless patterns are not part of the constraint set exercised here and
//! enforcing them would reject otherwise-valid inputs.

use std::collections::HashMap;

use http::{HeaderMap, StatusCode};
use serde_json::{Map, Value};

use fakecloud_core::service::AwsServiceError;

use crate::generated::{OpMeta, Src, K};

/// Pick the client-error code this operation should use for an input-validation
/// failure: `InvalidRequestException` when declared, else `ValidationException`,
/// else the operation's first declared error, else `InvalidRequestException`.
pub fn validation_error_code(meta: &OpMeta) -> &'static str {
    if meta.errors.contains(&"InvalidRequestException") {
        "InvalidRequestException"
    } else if meta.errors.contains(&"ValidationException") {
        "ValidationException"
    } else if let Some(first) = meta.errors.first() {
        first
    } else {
        "InvalidRequestException"
    }
}

pub fn invalid(meta: &OpMeta, msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, validation_error_code(meta), msg)
}

/// The request inputs, already split by wire location.
pub struct Inputs<'a> {
    pub labels: &'a HashMap<String, String>,
    pub query: &'a [(String, String)],
    pub headers: &'a HeaderMap,
    pub body: &'a Map<String, Value>,
}

impl Inputs<'_> {
    fn query_get(&self, key: &str) -> Option<&str> {
        self.query
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
    }
    fn header_get(&self, key: &str) -> Option<&str> {
        self.headers.get(key).and_then(|v| v.to_str().ok())
    }
}

/// Validate a request against the operation's model-derived constraints.
pub fn validate(meta: &OpMeta, inputs: &Inputs) -> Result<(), AwsServiceError> {
    // A required `@httpPayload` member is the entire request body; enforce its
    // presence (an absent / empty body is a client error). Payload members are
    // not part of the per-member `rules`, so this is checked separately.
    if meta.req_payload && inputs.body.is_empty() {
        return Err(invalid(meta, "Missing required request payload."));
    }

    for rule in meta.rules {
        // Resolve the member's string value (label/query/header) or JSON value
        // (body), and whether it is present at all.
        let string_val: Option<String> = match rule.src {
            Src::Label => inputs.labels.get(rule.wire).cloned(),
            Src::Query => inputs.query_get(rule.wire).map(str::to_string),
            Src::Header => inputs.header_get(rule.wire).map(str::to_string),
            Src::Body => None,
        };
        let body_val: Option<&Value> = match rule.src {
            Src::Body => inputs.body.get(rule.wire),
            _ => None,
        };
        let present = string_val.is_some() || body_val.is_some();

        if rule.req && !present {
            return Err(invalid(
                meta,
                format!("Missing required request parameter: {}", rule.wire),
            ));
        }
        if !present {
            continue;
        }

        // String-shaped members: length + enum. For body members, extract the
        // string when the JSON value is a string (the wire form of a Smithy
        // string / enum). A non-string JSON value for a string member is a
        // client error too.
        if matches!(rule.kind, K::Str) {
            let s: Option<String> = match &string_val {
                Some(s) => Some(s.clone()),
                None => match body_val {
                    Some(Value::String(s)) => Some(s.clone()),
                    Some(Value::Null) => None,
                    Some(_other) => {
                        return Err(invalid(
                            meta,
                            format!("Member {} must be a string.", rule.wire),
                        ));
                    }
                    None => None,
                },
            };
            if let Some(s) = s {
                let len = s.chars().count() as u64;
                if let Some(min) = rule.min_len {
                    if len < min {
                        return Err(invalid(
                            meta,
                            format!(
                                "Member {} is shorter than the minimum length {min}.",
                                rule.wire
                            ),
                        ));
                    }
                }
                if let Some(max) = rule.max_len {
                    if len > max {
                        return Err(invalid(
                            meta,
                            format!(
                                "Member {} is longer than the maximum length {max}.",
                                rule.wire
                            ),
                        ));
                    }
                }
                if !rule.enums.is_empty() && !rule.enums.contains(&s.as_str()) {
                    return Err(invalid(
                        meta,
                        format!("Member {} has an invalid enum value.", rule.wire),
                    ));
                }
            }
        }

        // Numeric members: range.
        if matches!(rule.kind, K::Int) && (rule.min_val.is_some() || rule.max_val.is_some()) {
            let n: Option<i64> = match &string_val {
                Some(s) => s.parse::<i64>().ok(),
                None => body_val.and_then(Value::as_i64),
            };
            if let Some(n) = n {
                if let Some(min) = rule.min_val {
                    if n < min {
                        return Err(invalid(
                            meta,
                            format!("Member {} is below the minimum {min}.", rule.wire),
                        ));
                    }
                }
                if let Some(max) = rule.max_val {
                    if n > max {
                        return Err(invalid(
                            meta,
                            format!("Member {} is above the maximum {max}.", rule.wire),
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generated::OPS;

    fn meta(op: &str) -> &'static OpMeta {
        OPS.iter().find(|m| m.op == op).unwrap()
    }
    fn inputs<'a>(
        labels: &'a HashMap<String, String>,
        query: &'a [(String, String)],
        headers: &'a HeaderMap,
        body: &'a Map<String, Value>,
    ) -> Inputs<'a> {
        Inputs {
            labels,
            query,
            headers,
            body,
        }
    }

    #[test]
    fn rejects_missing_required_body_member() {
        // CreateDestination requires Name/ExpressionType/Expression/RoleArn.
        let m = meta("CreateDestination");
        let labels = HashMap::new();
        let body = Map::new();
        let hm = HeaderMap::new();
        let inp = inputs(&labels, &[], &hm, &body);
        assert!(validate(m, &inp).is_err());
    }

    #[test]
    fn accepts_valid_body() {
        let m = meta("CreateDestination");
        let labels = HashMap::new();
        let mut body = Map::new();
        body.insert("Name".to_string(), Value::String("dest".to_string()));
        body.insert("Expression".to_string(), Value::String("rule".to_string()));
        // RoleArn has a minimum length of 20.
        body.insert(
            "RoleArn".to_string(),
            Value::String("arn:aws:iam::000000000000:role/x".to_string()),
        );
        // ExpressionType is an enum {RuleName, MqttTopic}.
        body.insert(
            "ExpressionType".to_string(),
            Value::String("RuleName".to_string()),
        );
        let hm = HeaderMap::new();
        let inp = inputs(&labels, &[], &hm, &body);
        assert!(validate(m, &inp).is_ok());
    }

    #[test]
    fn rejects_missing_required_query() {
        // GetPartnerAccount requires the PartnerType query parameter.
        let m = meta("GetPartnerAccount");
        let mut labels = HashMap::new();
        labels.insert("PartnerAccountId".to_string(), "acct".to_string());
        let body = Map::new();
        let hm = HeaderMap::new();
        let inp = inputs(&labels, &[], &hm, &body);
        assert!(validate(m, &inp).is_err());
    }
}
