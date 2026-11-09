//! The flow an execution runs: resolving a `flowIdentifier` +
//! `flowAliasIdentifier` pair against Bedrock Agents state to the flow version
//! the alias routes to, and the definition, execution role and customer key
//! that version carries. An execution captures this at start, so its snapshot
//! stays what actually ran even if the flow is later edited.
//!
//! Executions are stored under [`execution_map_key`] (flow, alias, execution
//! id): an execution belongs to one flow alias, and an execution name only has
//! to be unique within that alias.

use http::StatusCode;
use serde_json::Value;

use fakecloud_bedrock_agent::SharedBedrockAgentState;
use fakecloud_core::service::AwsServiceError;

use crate::service::make_error;

/// The built-in alias every flow has, routing to its working draft.
pub(crate) const TEST_ALIAS_ID: &str = "TSTALIASID";

const DRAFT: &str = "DRAFT";

/// What an execution of a flow alias runs.
#[derive(Debug, Clone)]
pub(crate) struct ResolvedFlow {
    pub flow_id: String,
    pub alias_id: String,
    pub version: String,
    pub definition: Option<Value>,
    pub execution_role_arn: Option<String>,
    pub customer_encryption_key_arn: Option<String>,
}

pub(crate) fn not_found(message: impl Into<String>) -> AwsServiceError {
    make_error(
        StatusCode::NOT_FOUND,
        "ResourceNotFoundException",
        &message.into(),
    )
}

/// The `region:account:flow/...` part of a Bedrock ARN, or `None` when the
/// identifier is a bare id.
fn bedrock_arn_rest(identifier: &str) -> Option<&str> {
    fakecloud_aws::arn::arn_resource(identifier, "bedrock")
}

/// The account a Bedrock ARN names, `None` for a bare id.
fn arn_account(identifier: &str) -> Option<&str> {
    bedrock_arn_rest(identifier).and_then(|rest| rest.split(':').nth(1))
}

/// The bare flow id a `flowIdentifier` names: the identifier itself, or the id
/// inside a flow ARN (`arn:<partition>:bedrock:<region>:<account>:flow/<id>`).
pub(crate) fn bare_flow_id(identifier: &str) -> &str {
    bedrock_arn_rest(identifier)
        .and_then(|rest| rest.split_once(":flow/"))
        .map_or(identifier, |(_, id)| id.split('/').next().unwrap_or(id))
}

/// The bare alias id a `flowAliasIdentifier` names for `flow_id`: the
/// identifier itself, or the alias id inside an alias ARN
/// (`...:flow/<flowId>/alias/<aliasId>`) whose flow is `flow_id`.
pub(crate) fn bare_alias_id(identifier: &str, flow_id: &str) -> Option<String> {
    match bedrock_arn_rest(identifier) {
        None => Some(identifier.to_string()),
        Some(rest) => rest
            .split_once(":flow/")
            .and_then(|(_, path)| path.split_once("/alias/"))
            .filter(|(flow, alias)| *flow == flow_id && !alias.is_empty())
            .map(|(_, alias)| alias.to_string()),
    }
}

/// The flow id and alias id a `flowIdentifier` / `flowAliasIdentifier` pair
/// names in `account_id`. `None` when either is an ARN in another account, or
/// the alias ARN belongs to another flow.
pub(crate) fn flow_and_alias(
    account_id: &str,
    flow_identifier: &str,
    alias_identifier: &str,
) -> Option<(String, String)> {
    if [flow_identifier, alias_identifier]
        .iter()
        .any(|id| arn_account(id).is_some_and(|a| a != account_id))
    {
        return None;
    }
    let flow_id = bare_flow_id(flow_identifier).to_string();
    let alias_id = bare_alias_id(alias_identifier, &flow_id)?;
    Some((flow_id, alias_id))
}

/// The `flow_executions` map key of an execution.
pub(crate) fn execution_map_key(flow_id: &str, alias_id: &str, execution_id: &str) -> String {
    format!("{flow_id}/{alias_id}/{execution_id}")
}

/// The map key of the execution an `executionIdentifier` (an execution id, or
/// an execution ARN `...:flow/<f>/alias/<a>/execution/<e>`) names under the
/// labelled flow and alias. `None` when the labels or the ARN name another
/// account, flow or alias.
pub(crate) fn execution_key_for(
    account_id: &str,
    flow_identifier: &str,
    alias_identifier: &str,
    execution_identifier: &str,
) -> Option<String> {
    let (flow_id, alias_id) = flow_and_alias(account_id, flow_identifier, alias_identifier)?;
    let execution_id = match bedrock_arn_rest(execution_identifier) {
        None => execution_identifier.to_string(),
        Some(rest) => {
            if arn_account(execution_identifier) != Some(account_id) {
                return None;
            }
            let path = rest.split_once(":flow/")?.1;
            let (arn_flow, rest) = path.split_once("/alias/")?;
            let (arn_alias, exec) = rest.split_once("/execution/")?;
            if arn_flow != flow_id || arn_alias != alias_id || exec.is_empty() {
                return None;
            }
            exec.to_string()
        }
    };
    Some(execution_map_key(&flow_id, &alias_id, &execution_id))
}

/// Resolve the flow and alias an execution targets in `account_id`. Every
/// miss (unknown flow, an identifier ARN in another account, unknown alias or
/// one belonging to another flow, a routed version that no longer exists) is a
/// `ResourceNotFoundException`, as the real service reports. Running the
/// working draft requires the flow to be prepared since its last edit
/// (`ValidationException` otherwise).
pub(crate) fn resolve_flow(
    agent_state: Option<&SharedBedrockAgentState>,
    account_id: &str,
    flow_identifier: &str,
    alias_identifier: &str,
) -> Result<ResolvedFlow, AwsServiceError> {
    let flow_missing = || not_found(format!("Flow {flow_identifier} not found."));
    let alias_missing = || not_found(format!("Flow alias {alias_identifier} not found."));
    // A flow or alias ARN in another account (or an alias ARN of another
    // flow) names nothing here.
    let (flow_id, alias_id) = flow_and_alias(account_id, flow_identifier, alias_identifier)
        .ok_or_else(|| {
            not_found(format!(
                "Flow {flow_identifier} alias {alias_identifier} not found."
            ))
        })?;

    let agent_state = agent_state.ok_or_else(flow_missing)?;
    let accounts = agent_state.read();
    let state = accounts.get(account_id).ok_or_else(flow_missing)?;
    let flow = state.flows.get(&flow_id).ok_or_else(flow_missing)?;

    let version = if alias_id == TEST_ALIAS_ID {
        DRAFT.to_string()
    } else {
        let alias = state
            .flow_aliases
            .get(&alias_id)
            .filter(|a| a.flow_id == flow_id)
            .ok_or_else(alias_missing)?;
        alias
            .routing_configuration
            .iter()
            .find_map(|r| r.get("flowVersion").and_then(Value::as_str))
            .unwrap_or(DRAFT)
            .to_string()
    };

    let resolved = if version == DRAFT {
        if flow.status != "Prepared" {
            return Err(make_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                &format!("Flow {flow_id} is not prepared. Prepare the flow before running it."),
            ));
        }
        ResolvedFlow {
            flow_id,
            alias_id,
            version,
            definition: flow.definition.clone(),
            execution_role_arn: flow.execution_role_arn.clone(),
            customer_encryption_key_arn: flow.customer_encryption_key_arn.clone(),
        }
    } else {
        let fv = state
            .flow_versions
            .get(&flow_id)
            .and_then(|versions| versions.iter().find(|v| v.flow_version == version))
            .ok_or_else(|| not_found(format!("Flow version {version} not found.")))?;
        // Versions captured before their flow-level fields were stored fall
        // back to the parent flow, as the Bedrock Agents readers do.
        ResolvedFlow {
            flow_id,
            alias_id,
            version,
            definition: fv.definition.clone(),
            execution_role_arn: fv
                .execution_role_arn
                .clone()
                .or_else(|| flow.execution_role_arn.clone()),
            customer_encryption_key_arn: fv
                .customer_encryption_key_arn
                .clone()
                .or_else(|| flow.customer_encryption_key_arn.clone()),
        }
    };
    Ok(resolved)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_parse_to_bare_ids() {
        assert_eq!(bare_flow_id("ABCDEFGHIJ"), "ABCDEFGHIJ");
        assert_eq!(
            bare_flow_id("arn:aws-cn:bedrock:cn-north-1:123456789012:flow/ABCDEFGHIJ"),
            "ABCDEFGHIJ"
        );
        assert_eq!(
            bare_alias_id(
                "arn:aws:bedrock:us-east-1:123456789012:flow/ABCDEFGHIJ/alias/KLMNOPQRST",
                "ABCDEFGHIJ"
            )
            .as_deref(),
            Some("KLMNOPQRST")
        );
        // An alias ARN of another flow names no alias of this one.
        assert_eq!(
            bare_alias_id(
                "arn:aws:bedrock:us-east-1:123456789012:flow/ZZZZZZZZZZ/alias/KLMNOPQRST",
                "ABCDEFGHIJ"
            ),
            None
        );
        assert_eq!(
            arn_account("arn:aws:bedrock:us-east-1:111122223333:flow/ABCDEFGHIJ"),
            Some("111122223333")
        );
        assert_eq!(arn_account("ABCDEFGHIJ"), None);
    }

    #[test]
    fn execution_identifiers_resolve_under_their_flow_and_alias() {
        let acct = "123456789012";
        let arn =
            "arn:aws:bedrock:us-east-1:123456789012:flow/FFFFFFFFFF/alias/AAAAAAAAAA/execution/e-1";
        assert_eq!(
            execution_key_for(acct, "FFFFFFFFFF", "AAAAAAAAAA", arn).as_deref(),
            Some("FFFFFFFFFF/AAAAAAAAAA/e-1")
        );
        assert_eq!(
            execution_key_for(acct, "FFFFFFFFFF", "AAAAAAAAAA", "e-1").as_deref(),
            Some("FFFFFFFFFF/AAAAAAAAAA/e-1")
        );
        // The ARN of another alias, flow or account names nothing here.
        assert_eq!(
            execution_key_for(acct, "FFFFFFFFFF", "TSTALIASID", arn),
            None
        );
        assert_eq!(
            execution_key_for(acct, "GGGGGGGGGG", "AAAAAAAAAA", arn),
            None
        );
        assert_eq!(
            execution_key_for("999999999999", "FFFFFFFFFF", "AAAAAAAAAA", arn),
            None
        );
    }
}
