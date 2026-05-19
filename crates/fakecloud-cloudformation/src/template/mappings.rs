//! `mappings` concerns from template.rs (audit-2026-05-19).

use super::*;

/// Parse the top-level `Mappings` block into a 2-level lookup table.
/// `Fn::FindInMap: [MapName, TopKey, SecondKey]` returns the leaf
/// value at that path.
pub(super) fn parse_mappings(template: &Value) -> Mappings {
    let mut out: Mappings = BTreeMap::new();
    let Some(maps) = template.get("Mappings").and_then(|v| v.as_object()) else {
        return out;
    };
    for (map_name, top) in maps {
        let Some(top_obj) = top.as_object() else {
            continue;
        };
        let mut top_out = BTreeMap::new();
        for (top_key, second) in top_obj {
            let Some(second_obj) = second.as_object() else {
                continue;
            };
            let mut second_out: BTreeMap<String, Value> = BTreeMap::new();
            for (k, v) in second_obj {
                second_out.insert(k.clone(), v.clone());
            }
            top_out.insert(top_key.clone(), second_out);
        }
        out.insert(map_name.clone(), top_out);
    }
    out
}

/// Walk `value`, replacing every `Fn::FindInMap` map ref with its
/// resolved leaf value. Args resolve `Ref` / nested `Fn::FindInMap`
/// against `parameters` + `mappings` first. Unresolvable lookups return
/// the optional `DefaultValue` from the 4-arg form, otherwise surface a
/// `ValidationError`-shaped string matching CloudFormation's error.
///
/// `Fn::If` short-circuits: only the branch picked by `conditions`
/// recurses, so a `Fn::FindInMap` sitting in an unused branch never
/// trips the strict miss-handling. Conditions that aren't yet known
/// (caller passed an empty map) recurse into both branches as before
/// to preserve behaviour.
pub(super) fn apply_mappings(
    value: &Value,
    parameters: &BTreeMap<String, String>,
    mappings: &Mappings,
    conditions: &BTreeMap<String, bool>,
) -> Result<Value, String> {
    match value {
        Value::Object(map) => {
            if let Some(arr) = map.get("Fn::If").and_then(|v| v.as_array()) {
                if arr.len() == 3 {
                    let cond_name = arr[0].as_str().unwrap_or("");
                    if let Some(picked_idx) =
                        conditions
                            .get(cond_name)
                            .copied()
                            .map(|b| if b { 1 } else { 2 })
                    {
                        // Resolve the picked branch eagerly; leave the
                        // unused branch verbatim so the downstream
                        // resolver (`resolve_refs_full`) still sees the
                        // same Fn::If shape and re-applies its own
                        // branch picking. Crucially, we never recurse
                        // into the unused branch, so a FindInMap that
                        // would fail there never executes.
                        let mut new_arr = arr.clone();
                        new_arr[picked_idx] =
                            apply_mappings(&arr[picked_idx], parameters, mappings, conditions)?;
                        let mut rewritten = serde_json::Map::new();
                        rewritten.insert("Fn::If".to_string(), Value::Array(new_arr));
                        return Ok(Value::Object(rewritten));
                    }
                }
            }
            if let Some(arr) = map.get("Fn::FindInMap").and_then(|v| v.as_array()) {
                return resolve_find_in_map(arr, parameters, mappings, conditions);
            }
            let mut new_map = serde_json::Map::new();
            for (k, v) in map {
                new_map.insert(
                    k.clone(),
                    apply_mappings(v, parameters, mappings, conditions)?,
                );
            }
            Ok(Value::Object(new_map))
        }
        Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                out.push(apply_mappings(v, parameters, mappings, conditions)?);
            }
            Ok(Value::Array(out))
        }
        other => Ok(other.clone()),
    }
}

/// Resolve a single `Fn::FindInMap` array. Supports the 3-arg form
/// `[MapName, TopKey, SecondKey]` and the 4-arg form
/// `[MapName, TopKey, SecondKey, { DefaultValue: <value> }]`. Args may
/// themselves be intrinsics (e.g. `{ "Ref": "AWS::Region" }` or a
/// nested `Fn::FindInMap`); those resolve before lookup.
pub(super) fn resolve_find_in_map(
    arr: &[Value],
    parameters: &BTreeMap<String, String>,
    mappings: &Mappings,
    conditions: &BTreeMap<String, bool>,
) -> Result<Value, String> {
    if arr.len() != 3 && arr.len() != 4 {
        return Err(format!(
            "Fn::FindInMap requires 3 or 4 arguments, got {}",
            arr.len()
        ));
    }
    let default_value: Option<Value> = if arr.len() == 4 {
        let opts = arr[3].as_object().ok_or_else(|| {
            "Fn::FindInMap fourth argument must be an object with a DefaultValue key".to_string()
        })?;
        let dv = opts.get("DefaultValue").ok_or_else(|| {
            "Fn::FindInMap fourth argument must contain a DefaultValue key".to_string()
        })?;
        Some(apply_mappings(dv, parameters, mappings, conditions)?)
    } else {
        None
    };

    let map_name = stringify_findinmap_arg(&arr[0], parameters, mappings, conditions)?;
    let top_key = stringify_findinmap_arg(&arr[1], parameters, mappings, conditions)?;
    let second_key = stringify_findinmap_arg(&arr[2], parameters, mappings, conditions)?;

    if let Some(top) = mappings.get(&map_name) {
        if let Some(second) = top.get(&top_key) {
            if let Some(leaf) = second.get(&second_key) {
                return Ok(leaf.clone());
            }
        }
    }

    if let Some(dv) = default_value {
        return Ok(dv);
    }

    Err(format!(
        "Template error: Unable to get mapping for {map_name}::{top_key}::{second_key}"
    ))
}

pub(super) fn stringify_findinmap_arg(
    value: &Value,
    parameters: &BTreeMap<String, String>,
    mappings: &Mappings,
    conditions: &BTreeMap<String, bool>,
) -> Result<String, String> {
    match value {
        Value::String(s) => Ok(s.clone()),
        Value::Object(m) => {
            if let Some(name) = m.get("Ref").and_then(|v| v.as_str()) {
                if let Some(p) = parameters.get(name) {
                    return Ok(p.clone());
                }
                // Pseudo refs that have a canonical default value
                // resolve so FindInMap keyed off `AWS::Region` etc.
                // works without the caller priming `parameters`.
                if let Some(Value::String(s)) = pseudo_value(name, parameters) {
                    return Ok(s);
                }
                return Ok(name.to_string());
            }
            // Nested Fn::FindInMap as a key — resolve it and stringify
            // the leaf, so e.g. `Fn::FindInMap: [Outer, !FindInMap [...], K]`
            // works.
            if let Some(arr) = m.get("Fn::FindInMap").and_then(|v| v.as_array()) {
                let resolved = resolve_find_in_map(arr, parameters, mappings, conditions)?;
                return Ok(match resolved {
                    Value::String(s) => s,
                    other => other.to_string(),
                });
            }
            Ok(value.to_string())
        }
        _ => Ok(value.to_string()),
    }
}
