//! Real Cedar authorization for Verified Permissions.
//!
//! Translates the Verified Permissions wire shapes (`EntityIdentifier`,
//! `ActionIdentifier`, `ContextDefinition`, `EntitiesDefinition`,
//! `AttributeValue`, ...) into `cedar-policy` values, compiles a policy store's
//! static and template-linked policies into a Cedar `PolicySet`, and evaluates
//! the request. The decision (`ALLOW`/`DENY`), the determining policy ids and
//! any evaluation errors are returned exactly as the API models them.

use std::collections::HashMap;
use std::str::FromStr;

use cedar_policy::{
    Authorizer, Context, Decision, Entities, EntityId, EntityTypeName, EntityUid, Policy, PolicyId,
    PolicySet, Request, SlotId, Template,
};
use serde_json::{json, Map, Value};

use crate::state::StoredPolicyStore;

/// The outcome of a single Cedar authorization evaluation.
pub struct AuthzResult {
    /// `ALLOW` or `DENY`.
    pub decision: String,
    /// Policy ids that determined the decision (the `reason` set).
    pub determining_policies: Vec<String>,
    /// Human-readable evaluation errors.
    pub errors: Vec<String>,
}

/// Evaluate a request against a policy store's compiled Cedar policies.
///
/// `principal`, `action` and `resource` are the raw `EntityIdentifier` /
/// `ActionIdentifier` request members; `context` and `entities` are the raw
/// `ContextDefinition` / `EntitiesDefinition` members (or `None`).
pub fn evaluate(
    store: &StoredPolicyStore,
    principal: &Value,
    action: &Value,
    resource: &Value,
    context: Option<&Value>,
    entities: Option<&Value>,
) -> Result<AuthzResult, String> {
    let principal_uid = entity_uid(principal)?;
    let action_uid = action_uid(action)?;
    let resource_uid = entity_uid(resource)?;
    let ctx = build_context(context)?;
    let ents = build_entities(entities)?;

    let (policy_set, mut errors) = build_policy_set(store);

    let request = Request::new(principal_uid, action_uid, resource_uid, ctx, None)
        .map_err(|e| format!("invalid authorization request: {e}"))?;
    let response = Authorizer::new().is_authorized(&request, &policy_set, &ents);

    let decision = match response.decision() {
        Decision::Allow => "ALLOW",
        Decision::Deny => "DENY",
    }
    .to_string();
    let determining_policies = response
        .diagnostics()
        .reason()
        .map(|pid| pid.to_string())
        .collect();
    for err in response.diagnostics().errors() {
        errors.push(err.to_string());
    }

    Ok(AuthzResult {
        decision,
        determining_policies,
        errors,
    })
}

/// Build the Cedar `PolicySet` for a store, returning any policies that failed
/// to compile as evaluation errors (rather than aborting the whole request).
fn build_policy_set(store: &StoredPolicyStore) -> (PolicySet, Vec<String>) {
    let mut set = PolicySet::new();
    let mut errors = Vec::new();
    for (id, policy) in &store.policies {
        match policy.policy_type.as_str() {
            "TEMPLATE_LINKED" => {
                let Some(template_id) = policy.template_id.as_deref() else {
                    continue;
                };
                let Some(template) = store.templates.get(template_id) else {
                    errors.push(format!(
                        "policy {id} references missing template {template_id}"
                    ));
                    continue;
                };
                let tpid = PolicyId::new(template_id);
                if set.template(&tpid).is_none() {
                    match Template::parse(Some(tpid.clone()), &template.statement) {
                        Ok(t) => {
                            let _ = set.add_template(t);
                        }
                        Err(e) => {
                            errors.push(format!("template {template_id} failed to parse: {e}"));
                            continue;
                        }
                    }
                }
                let mut vals: HashMap<SlotId, EntityUid> = HashMap::new();
                if let Some(p) = &policy.principal {
                    if let Ok(u) = entity_uid(p) {
                        vals.insert(SlotId::principal(), u);
                    }
                }
                if let Some(r) = &policy.resource {
                    if let Ok(u) = entity_uid(r) {
                        vals.insert(SlotId::resource(), u);
                    }
                }
                if let Err(e) = set.link(tpid, PolicyId::new(id), vals) {
                    errors.push(format!("policy {id} failed to link: {e}"));
                }
            }
            _ => {
                let statement = policy.statement.as_deref().unwrap_or("");
                match Policy::parse(Some(PolicyId::new(id)), statement) {
                    Ok(p) => {
                        if let Err(e) = set.add(p) {
                            errors.push(format!("policy {id} failed to add: {e}"));
                        }
                    }
                    Err(e) => errors.push(format!("policy {id} failed to parse: {e}")),
                }
            }
        }
    }
    (set, errors)
}

/// Build a Cedar `EntityUid` from a Verified Permissions `EntityIdentifier`.
pub fn entity_uid(v: &Value) -> Result<EntityUid, String> {
    let entity_type = v
        .get("entityType")
        .and_then(Value::as_str)
        .ok_or("EntityIdentifier.entityType is required")?;
    let entity_id = v
        .get("entityId")
        .and_then(Value::as_str)
        .ok_or("EntityIdentifier.entityId is required")?;
    let type_name = EntityTypeName::from_str(entity_type)
        .map_err(|e| format!("invalid entity type `{entity_type}`: {e}"))?;
    Ok(EntityUid::from_type_name_and_id(
        type_name,
        EntityId::new(entity_id),
    ))
}

/// Build a Cedar action `EntityUid` from a Verified Permissions
/// `ActionIdentifier` (`actionType` is the entity type, `actionId` the id).
fn action_uid(v: &Value) -> Result<EntityUid, String> {
    let action_type = v
        .get("actionType")
        .and_then(Value::as_str)
        .ok_or("ActionIdentifier.actionType is required")?;
    let action_id = v
        .get("actionId")
        .and_then(Value::as_str)
        .ok_or("ActionIdentifier.actionId is required")?;
    let type_name = EntityTypeName::from_str(action_type)
        .map_err(|e| format!("invalid action type `{action_type}`: {e}"))?;
    Ok(EntityUid::from_type_name_and_id(
        type_name,
        EntityId::new(action_id),
    ))
}

fn build_context(cd: Option<&Value>) -> Result<Context, String> {
    let Some(cd) = cd else {
        return Ok(Context::empty());
    };
    if let Some(map) = cd.get("contextMap").and_then(Value::as_object) {
        let json = Value::Object(
            map.iter()
                .map(|(k, v)| (k.clone(), attribute_to_cedar(v)))
                .collect(),
        );
        return Context::from_json_value(json, None).map_err(|e| format!("invalid context: {e}"));
    }
    if let Some(cedar_json) = cd.get("cedarJson").and_then(Value::as_str) {
        let json: Value = serde_json::from_str(cedar_json)
            .map_err(|e| format!("invalid context cedarJson: {e}"))?;
        return Context::from_json_value(json, None).map_err(|e| format!("invalid context: {e}"));
    }
    Ok(Context::empty())
}

fn build_entities(ed: Option<&Value>) -> Result<Entities, String> {
    let Some(ed) = ed else {
        return Ok(Entities::empty());
    };
    if let Some(list) = ed.get("entityList").and_then(Value::as_array) {
        let json = Value::Array(list.iter().map(entity_item_to_cedar).collect());
        return Entities::from_json_value(json, None).map_err(|e| format!("invalid entities: {e}"));
    }
    if let Some(cedar_json) = ed.get("cedarJson").and_then(Value::as_str) {
        let json: Value = serde_json::from_str(cedar_json)
            .map_err(|e| format!("invalid entities cedarJson: {e}"))?;
        return Entities::from_json_value(json, None).map_err(|e| format!("invalid entities: {e}"));
    }
    Ok(Entities::empty())
}

/// Convert a Verified Permissions `EntityItem` into the Cedar entity JSON form
/// (`{uid, attrs, parents, tags}`).
fn entity_item_to_cedar(item: &Value) -> Value {
    let uid = item
        .get("identifier")
        .map(entity_ref_to_cedar)
        .unwrap_or(Value::Null);
    let attrs = item
        .get("attributes")
        .and_then(Value::as_object)
        .map(|m| {
            Value::Object(
                m.iter()
                    .map(|(k, v)| (k.clone(), attribute_to_cedar(v)))
                    .collect(),
            )
        })
        .unwrap_or_else(|| json!({}));
    let parents = item
        .get("parents")
        .and_then(Value::as_array)
        .map(|a| Value::Array(a.iter().map(entity_ref_to_cedar).collect()))
        .unwrap_or_else(|| json!([]));
    let mut out = json!({ "uid": uid, "attrs": attrs, "parents": parents });
    if let Some(tags) = item.get("tags").and_then(Value::as_object) {
        out["tags"] = Value::Object(
            tags.iter()
                .map(|(k, v)| (k.clone(), attribute_to_cedar(v)))
                .collect(),
        );
    }
    out
}

/// `EntityIdentifier` -> Cedar `{type, id}` object (as used inside `uid` /
/// `parents`).
fn entity_ref_to_cedar(v: &Value) -> Value {
    json!({
        "type": v.get("entityType").and_then(Value::as_str).unwrap_or(""),
        "id": v.get("entityId").and_then(Value::as_str).unwrap_or(""),
    })
}

/// Convert a Verified Permissions `AttributeValue` union into a Cedar JSON
/// attribute value.
fn attribute_to_cedar(v: &Value) -> Value {
    let Some(obj) = v.as_object() else {
        // Already a bare JSON value (e.g. from a `cedarJson` blob) — pass through.
        return v.clone();
    };
    if let Some(b) = obj.get("boolean") {
        return b.clone();
    }
    if let Some(l) = obj.get("long") {
        return l.clone();
    }
    if let Some(s) = obj.get("string") {
        return s.clone();
    }
    if let Some(e) = obj.get("entityIdentifier") {
        return json!({ "__entity": entity_ref_to_cedar(e) });
    }
    if let Some(set) = obj.get("set").and_then(Value::as_array) {
        return Value::Array(set.iter().map(attribute_to_cedar).collect());
    }
    if let Some(rec) = obj.get("record").and_then(Value::as_object) {
        return Value::Object(
            rec.iter()
                .map(|(k, val)| (k.clone(), attribute_to_cedar(val)))
                .collect(),
        );
    }
    for (key, cedar_fn) in [
        ("ipaddr", "ip"),
        ("decimal", "decimal"),
        ("datetime", "datetime"),
        ("duration", "duration"),
    ] {
        if let Some(s) = obj.get(key).and_then(Value::as_str) {
            return json!({ "__extn": { "fn": cedar_fn, "arg": s } });
        }
    }
    // Unknown/empty union: preserve the raw object.
    Value::Object(Map::clone(obj))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{StoredPolicy, StoredPolicyStore};
    use chrono::Utc;
    use std::collections::BTreeMap;

    fn store_with_policies(policies: Vec<(&str, &str)>) -> StoredPolicyStore {
        let now = Utc::now();
        let mut map = BTreeMap::new();
        for (id, statement) in policies {
            map.insert(
                id.to_string(),
                StoredPolicy {
                    policy_id: id.to_string(),
                    policy_type: "STATIC".into(),
                    name: None,
                    description: None,
                    statement: Some(statement.to_string()),
                    template_id: None,
                    principal: None,
                    resource: None,
                    created_at: now,
                    updated_at: now,
                },
            );
        }
        StoredPolicyStore {
            policy_store_id: "PS1".into(),
            arn: "arn".into(),
            validation_mode: "OFF".into(),
            description: None,
            deletion_protection: None,
            created_at: now,
            updated_at: now,
            schema: None,
            policies: map,
            templates: BTreeMap::new(),
            identity_sources: BTreeMap::new(),
            tags: BTreeMap::new(),
        }
    }

    #[test]
    fn permit_yields_allow_with_determining_policy() {
        let store = store_with_policies(vec![(
            "p1",
            r#"permit(principal == User::"alice", action == Action::"view", resource == Photo::"vacation");"#,
        )]);
        let r = evaluate(
            &store,
            &json!({ "entityType": "User", "entityId": "alice" }),
            &json!({ "actionType": "Action", "actionId": "view" }),
            &json!({ "entityType": "Photo", "entityId": "vacation" }),
            None,
            None,
        )
        .unwrap();
        assert_eq!(r.decision, "ALLOW");
        assert_eq!(r.determining_policies, vec!["p1".to_string()]);
        assert!(r.errors.is_empty());
    }

    #[test]
    fn no_matching_permit_yields_deny() {
        let store = store_with_policies(vec![(
            "p1",
            r#"permit(principal == User::"bob", action == Action::"view", resource == Photo::"vacation");"#,
        )]);
        let r = evaluate(
            &store,
            &json!({ "entityType": "User", "entityId": "alice" }),
            &json!({ "actionType": "Action", "actionId": "view" }),
            &json!({ "entityType": "Photo", "entityId": "vacation" }),
            None,
            None,
        )
        .unwrap();
        assert_eq!(r.decision, "DENY");
        assert!(r.determining_policies.is_empty());
    }

    #[test]
    fn forbid_overrides_permit() {
        let store = store_with_policies(vec![
            ("allow", r#"permit(principal, action, resource);"#),
            (
                "deny",
                r#"forbid(principal == User::"alice", action, resource);"#,
            ),
        ]);
        let r = evaluate(
            &store,
            &json!({ "entityType": "User", "entityId": "alice" }),
            &json!({ "actionType": "Action", "actionId": "view" }),
            &json!({ "entityType": "Photo", "entityId": "vacation" }),
            None,
            None,
        )
        .unwrap();
        assert_eq!(r.decision, "DENY");
        assert!(r.determining_policies.contains(&"deny".to_string()));
    }

    #[test]
    fn context_attribute_gates_decision() {
        let store = store_with_policies(vec![(
            "mfa",
            r#"permit(principal, action, resource) when { context.mfa == true };"#,
        )]);
        let with_mfa = evaluate(
            &store,
            &json!({ "entityType": "User", "entityId": "alice" }),
            &json!({ "actionType": "Action", "actionId": "view" }),
            &json!({ "entityType": "Photo", "entityId": "vacation" }),
            Some(&json!({ "contextMap": { "mfa": { "boolean": true } } })),
            None,
        )
        .unwrap();
        assert_eq!(with_mfa.decision, "ALLOW");
        let without = evaluate(
            &store,
            &json!({ "entityType": "User", "entityId": "alice" }),
            &json!({ "actionType": "Action", "actionId": "view" }),
            &json!({ "entityType": "Photo", "entityId": "vacation" }),
            Some(&json!({ "contextMap": { "mfa": { "boolean": false } } })),
            None,
        )
        .unwrap();
        assert_eq!(without.decision, "DENY");
    }
}
