//! dynamodb helpers `paths` concerns (audit-2026-05-19).

use super::*;

pub(crate) fn resolve_attr_name(name: &str, expr_attr_names: &HashMap<String, String>) -> String {
    if name.starts_with('#') {
        expr_attr_names
            .get(name)
            .cloned()
            .unwrap_or_else(|| name.to_string())
    } else {
        name.to_string()
    }
}

/// Resolve a (possibly dotted, possibly `#name`-containing) document path to
/// the leaf `AttributeValue` inside `item`. Single-segment paths (`foo`,
/// `#foo`) resolve to a top-level attribute. Dotted paths (`profile.email`,
/// `#p.#e`, `items[0].sku`) walk into `M`/`L` containers. Returns `None` if
/// any segment is missing or the intermediate value isn't a map/list.
pub(crate) fn resolve_path(
    path: &str,
    item: &HashMap<String, AttributeValue>,
    expr_attr_names: &HashMap<String, String>,
) -> Option<Value> {
    // Fast path: a single-segment expression (no `.` and no `[` in the raw
    // input) refers to a top-level attribute by its literal name, even if the
    // resolved alias contains a `.`. Without this, `#sw` -> `Safety.Warning`
    // would be misread as the nested path `Safety` -> `Warning`.
    if !path.contains('.') && !path.contains('[') {
        return item.get(&resolve_attr_name(path, expr_attr_names)).cloned();
    }
    let segs = resolve_projection_path_segments(path, expr_attr_names);
    resolve_nested_path_segments(item, &segs)
}

pub(crate) fn project_item(
    item: &HashMap<String, AttributeValue>,
    body: &Value,
) -> HashMap<String, AttributeValue> {
    let projection = body["ProjectionExpression"].as_str();
    match projection {
        Some(proj) if !proj.is_empty() => {
            let expr_attr_names = parse_expression_attribute_names(body);
            let mut result = HashMap::new();
            for raw in proj.split(',') {
                let raw = raw.trim();
                // Single-segment: treat as literal top-level attribute even if
                // the alias resolves to a name containing `.` (e.g. `#sw` ->
                // `Safety.Warning`).
                if !raw.contains('.') && !raw.contains('[') {
                    let key = resolve_attr_name(raw, &expr_attr_names);
                    if let Some(v) = item.get(&key) {
                        result.insert(key, v.clone());
                    }
                } else {
                    let segs = resolve_projection_path_segments(raw, &expr_attr_names);
                    if let Some(v) = resolve_nested_path_segments(item, &segs) {
                        insert_nested_value_segments(&mut result, &segs, v);
                    }
                }
            }
            result
        }
        _ => item.clone(),
    }
}

/// Resolve a projection path to logical `PathSegment`s, substituting
/// `#alias` references without re-splitting the resolved name. Use
/// this when the result will feed back into
/// [`resolve_nested_path_segments`] / [`insert_nested_value_segments`]
/// — otherwise an alias whose value contains `.` (e.g. `#sw` ->
/// `Safety.Warning`) produces extra spurious segments.
pub(crate) fn resolve_projection_path_segments(
    path: &str,
    expr_attr_names: &HashMap<String, String>,
) -> Vec<PathSegment> {
    let raw = parse_path_segments(path);
    raw.into_iter()
        .map(|seg| match seg {
            PathSegment::Key(k) => PathSegment::Key(resolve_attr_name(&k, expr_attr_names)),
            other => other,
        })
        .collect()
}

/// Like [`resolve_nested_path`] but operating on pre-resolved segments.
pub(crate) fn resolve_nested_path_segments(
    item: &HashMap<String, AttributeValue>,
    segments: &[PathSegment],
) -> Option<Value> {
    if segments.is_empty() {
        return None;
    }
    let top_key = match &segments[0] {
        PathSegment::Key(k) => k.as_str(),
        _ => return None,
    };
    let mut current = item.get(top_key)?.clone();
    for segment in &segments[1..] {
        match segment {
            PathSegment::Key(k) => {
                current = current.get("M")?.get(k)?.clone();
            }
            PathSegment::Index(idx) => {
                current = current.get("L")?.get(*idx)?.clone();
            }
        }
    }
    Some(current)
}

/// Insert a value into `result` at the given pre-resolved segment path.
pub(crate) fn insert_nested_value_segments(
    result: &mut HashMap<String, AttributeValue>,
    segments: &[PathSegment],
    value: Value,
) {
    if segments.is_empty() {
        return;
    }
    let top_key = match &segments[0] {
        PathSegment::Key(k) => k.clone(),
        _ => return,
    };
    if segments.len() == 1 {
        result.insert(top_key, value);
        return;
    }
    let wrapped = wrap_value_in_path(&segments[1..], value);
    let existing = result.remove(&top_key);
    let merged = match existing {
        Some(existing) => merge_attribute_values(existing, wrapped),
        None => wrapped,
    };
    result.insert(top_key, merged);
}

/// Resolve a potentially nested path like "a.b.c" or "a[0].b" from an item.
///
/// Kept for tests that exercise raw path parsing; production callers
/// should resolve aliases first via
/// [`resolve_projection_path_segments`] and then call
/// [`resolve_nested_path_segments`] directly.
#[cfg(test)]
pub(crate) fn resolve_nested_path(
    item: &HashMap<String, AttributeValue>,
    path: &str,
) -> Option<Value> {
    resolve_nested_path_segments(item, &parse_path_segments(path))
}

/// Parse a path like "a.b[0].c" into segments: [Key("a"), Key("b"), Index(0), Key("c")]
pub(crate) fn parse_path_segments(path: &str) -> Vec<PathSegment> {
    let mut segments = Vec::new();
    let mut current = String::new();

    let chars: Vec<char> = path.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '.' => {
                if !current.is_empty() {
                    segments.push(PathSegment::Key(current.clone()));
                    current.clear();
                }
            }
            '[' => {
                if !current.is_empty() {
                    segments.push(PathSegment::Key(current.clone()));
                    current.clear();
                }
                i += 1;
                let mut num = String::new();
                while i < chars.len() && chars[i] != ']' {
                    num.push(chars[i]);
                    i += 1;
                }
                if let Ok(idx) = num.parse::<usize>() {
                    segments.push(PathSegment::Index(idx));
                }
                // skip ']'
            }
            c => {
                current.push(c);
            }
        }
        i += 1;
    }
    if !current.is_empty() {
        segments.push(PathSegment::Key(current));
    }
    segments
}

/// Wrap a value in the nested path structure.
pub(crate) fn wrap_value_in_path(segments: &[PathSegment], value: Value) -> Value {
    if segments.is_empty() {
        return value;
    }
    let inner = wrap_value_in_path(&segments[1..], value);
    match &segments[0] {
        PathSegment::Key(k) => {
            json!({"M": {k.clone(): inner}})
        }
        PathSegment::Index(idx) => {
            let mut arr = vec![Value::Null; idx + 1];
            arr[*idx] = inner;
            json!({"L": arr})
        }
    }
}

/// Merge two attribute values (for overlapping projections).
///
/// Handles both `M` (map) and `L` (list) merging. Without the list
/// branch, two list-indexed projections (`list[0]` and `list[1]`)
/// would overwrite each other because the second projection's `L`
/// value replaced the first wholesale.
pub(crate) fn merge_attribute_values(a: Value, b: Value) -> Value {
    if let (Some(a_map), Some(b_map)) = (
        a.get("M").and_then(|v| v.as_object()),
        b.get("M").and_then(|v| v.as_object()),
    ) {
        let mut merged = a_map.clone();
        for (k, v) in b_map {
            if let Some(existing) = merged.get(k) {
                merged.insert(
                    k.clone(),
                    merge_attribute_values(existing.clone(), v.clone()),
                );
            } else {
                merged.insert(k.clone(), v.clone());
            }
        }
        return json!({"M": merged});
    }
    if let (Some(a_list), Some(b_list)) = (
        a.get("L").and_then(|v| v.as_array()),
        b.get("L").and_then(|v| v.as_array()),
    ) {
        let len = a_list.len().max(b_list.len());
        let mut out = Vec::with_capacity(len);
        for i in 0..len {
            let lhs = a_list.get(i).cloned().unwrap_or(Value::Null);
            let rhs = b_list.get(i).cloned().unwrap_or(Value::Null);
            // Null is the wrap_value_in_path placeholder for "no
            // projection touched this index" — prefer the non-null
            // side, recurse when both contributed a real value.
            let picked = if lhs.is_null() {
                rhs
            } else if rhs.is_null() {
                lhs
            } else {
                merge_attribute_values(lhs, rhs)
            };
            out.push(picked);
        }
        return json!({"L": out});
    }
    b
}
