//! dynamodb helpers `updates` concerns (audit-2026-05-19).

use super::*;

/// Write `value` at a dotted path inside an M-typed attribute.
///
/// Resolves each `#name` segment through `expr_attr_names`. The top-level
/// attribute and every intermediate segment must already exist as a Map —
/// DynamoDB rejects writes through missing parents with ValidationException.
pub(crate) fn assign_nested_path(
    item: &mut HashMap<String, AttributeValue>,
    path: &str,
    expr_attr_names: &HashMap<String, String>,
    value: Value,
) -> Result<(), AwsServiceError> {
    let mut segments: Vec<String> = path
        .split('.')
        .map(|seg| resolve_attr_name(seg.trim(), expr_attr_names))
        .collect();
    if segments.len() < 2 {
        return Err(invalid_document_path());
    }

    let leaf = segments.pop().expect("len >= 2");
    let top = segments.remove(0);

    let top_attr = item.get_mut(&top).ok_or_else(invalid_document_path)?;
    let mut current = top_attr
        .get_mut("M")
        .and_then(|m| m.as_object_mut())
        .ok_or_else(invalid_document_path)?;

    for seg in &segments {
        current = current
            .get_mut(seg)
            .and_then(|v| v.get_mut("M"))
            .and_then(|m| m.as_object_mut())
            .ok_or_else(invalid_document_path)?;
    }

    current.insert(leaf, value);
    Ok(())
}

/// One step of a document path: a map key or a list index.
enum PathSeg {
    Key(String),
    Index(usize),
}

/// Strip a trailing `[N]` list index off a single path segment, returning the
/// name and the index. `name` may itself be a `#placeholder`.
fn strip_trailing_index(part: &str) -> Option<(&str, usize)> {
    let part = part.trim();
    if !part.ends_with(']') {
        return None;
    }
    let open = part.rfind('[')?;
    let idx: usize = part[open + 1..part.len() - 1].parse().ok()?;
    let name = part[..open].trim();
    if name.is_empty() {
        return None;
    }
    Some((name, idx))
}

/// Parse a document path (`a`, `#a`, `a.b.c`, `a.b[2].c`, `tags[0]`) into
/// resolved segments. Returns `None` for an empty/invalid path.
fn parse_path_segments(
    path: &str,
    expr_attr_names: &HashMap<String, String>,
) -> Option<Vec<PathSeg>> {
    let mut segs = Vec::new();
    for part in path.split('.') {
        let part = part.trim();
        let (name, idx) = match strip_trailing_index(part) {
            Some((n, i)) => (n, Some(i)),
            None => (part, None),
        };
        if name.is_empty() {
            return None;
        }
        segs.push(PathSeg::Key(resolve_attr_name(name, expr_attr_names)));
        if let Some(i) = idx {
            segs.push(PathSeg::Index(i));
        }
    }
    if segs.is_empty() {
        None
    } else {
        Some(segs)
    }
}

/// Remove the attribute at `path`, supporting top-level names, `#name`
/// placeholders, dotted nested map paths (`a.b.c`), and list indices
/// (`tags[0]`, `a.b[2]`). A path through a missing parent is a silent no-op,
/// matching DynamoDB's behavior that `REMOVE` of an absent path succeeds.
pub(crate) fn remove_path(
    item: &mut HashMap<String, AttributeValue>,
    path: &str,
    expr_attr_names: &HashMap<String, String>,
) {
    let Some(segs) = parse_path_segments(path, expr_attr_names) else {
        return;
    };
    // Top-level attribute: remove the key directly.
    if segs.len() == 1 {
        if let PathSeg::Key(k) = &segs[0] {
            item.remove(k);
        }
        return;
    }
    let PathSeg::Key(top) = &segs[0] else {
        return;
    };
    let Some(mut cur) = item.get_mut(top) else {
        return;
    };
    // Descend to the parent of the leaf.
    for seg in &segs[1..segs.len() - 1] {
        cur = match seg {
            PathSeg::Key(k) => match cur.get_mut("M").and_then(|m| m.get_mut(k)) {
                Some(v) => v,
                None => return,
            },
            PathSeg::Index(i) => match cur.get_mut("L").and_then(|l| l.get_mut(*i)) {
                Some(v) => v,
                None => return,
            },
        };
    }
    // Remove the leaf from its parent container.
    match segs.last().expect("len >= 2") {
        PathSeg::Key(k) => {
            if let Some(map) = cur.get_mut("M").and_then(|m| m.as_object_mut()) {
                map.remove(k);
            }
        }
        PathSeg::Index(i) => {
            if let Some(list) = cur.get_mut("L").and_then(|l| l.as_array_mut()) {
                if *i < list.len() {
                    list.remove(*i);
                }
            }
        }
    }
}

pub(crate) fn extract_number(val: &Option<Value>) -> Option<f64> {
    val.as_ref()
        .and_then(|v| v.get("N"))
        .and_then(|n| n.as_str())
        .and_then(|s| s.parse().ok())
}

pub(crate) fn parse_arithmetic(expr: &str) -> Option<(&str, &str, bool)> {
    let mut depth = 0;
    for (i, c) in expr.char_indices() {
        match c {
            '(' => depth += 1,
            ')' => depth -= 1,
            '+' if depth == 0 && i > 0 => {
                return Some((&expr[..i], &expr[i + 1..], true));
            }
            '-' if depth == 0 && i > 0 => {
                return Some((&expr[..i], &expr[i + 1..], false));
            }
            _ => {}
        }
    }
    None
}

pub(crate) fn apply_add_assignment(
    item: &mut HashMap<String, AttributeValue>,
    assignment: &str,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Result<(), AwsServiceError> {
    let parts: Vec<&str> = assignment.splitn(2, ' ').collect();
    if parts.len() != 2 {
        return Ok(());
    }

    let attr = resolve_attr_name(parts[0].trim(), expr_attr_names);
    let val_ref = parts[1].trim();
    let add_val = expr_attr_values.get(val_ref);

    if let Some(add_val) = add_val {
        if let Some(existing) = item.get(&attr) {
            if let (Some(existing_num), Some(add_num)) = (
                extract_number(&Some(existing.clone())),
                extract_number(&Some(add_val.clone())),
            ) {
                let result = existing_num + add_num;
                let num_str = if result == result.trunc() {
                    format!("{}", result as i64)
                } else {
                    format!("{result}")
                };
                item.insert(attr, json!({"N": num_str}));
            } else if let Some(existing_set) = existing.get("SS").and_then(|v| v.as_array()) {
                if let Some(add_set) = add_val.get("SS").and_then(|v| v.as_array()) {
                    let mut merged: Vec<Value> = existing_set.clone();
                    for v in add_set {
                        if !merged.contains(v) {
                            merged.push(v.clone());
                        }
                    }
                    item.insert(attr, json!({"SS": merged}));
                }
            } else if let Some(existing_set) = existing.get("NS").and_then(|v| v.as_array()) {
                if let Some(add_set) = add_val.get("NS").and_then(|v| v.as_array()) {
                    let mut merged: Vec<Value> = existing_set.clone();
                    for v in add_set {
                        if !merged.contains(v) {
                            merged.push(v.clone());
                        }
                    }
                    item.insert(attr, json!({"NS": merged}));
                }
            } else if let Some(existing_set) = existing.get("BS").and_then(|v| v.as_array()) {
                if let Some(add_set) = add_val.get("BS").and_then(|v| v.as_array()) {
                    let mut merged: Vec<Value> = existing_set.clone();
                    for v in add_set {
                        if !merged.contains(v) {
                            merged.push(v.clone());
                        }
                    }
                    item.insert(attr, json!({"BS": merged}));
                }
            }
        } else {
            item.insert(attr, add_val.clone());
        }
    }

    Ok(())
}

pub(crate) fn apply_delete_assignment(
    item: &mut HashMap<String, AttributeValue>,
    assignment: &str,
    expr_attr_names: &HashMap<String, String>,
    expr_attr_values: &HashMap<String, Value>,
) -> Result<(), AwsServiceError> {
    let parts: Vec<&str> = assignment.splitn(2, ' ').collect();
    if parts.len() != 2 {
        return Ok(());
    }

    let attr = resolve_attr_name(parts[0].trim(), expr_attr_names);
    let val_ref = parts[1].trim();
    let del_val = expr_attr_values.get(val_ref);

    if let (Some(existing), Some(del_val)) = (item.get(&attr).cloned(), del_val) {
        if let (Some(existing_set), Some(del_set)) = (
            existing.get("SS").and_then(|v| v.as_array()),
            del_val.get("SS").and_then(|v| v.as_array()),
        ) {
            let filtered: Vec<Value> = existing_set
                .iter()
                .filter(|v| !del_set.contains(v))
                .cloned()
                .collect();
            if filtered.is_empty() {
                item.remove(&attr);
            } else {
                item.insert(attr, json!({"SS": filtered}));
            }
        } else if let (Some(existing_set), Some(del_set)) = (
            existing.get("NS").and_then(|v| v.as_array()),
            del_val.get("NS").and_then(|v| v.as_array()),
        ) {
            let filtered: Vec<Value> = existing_set
                .iter()
                .filter(|v| !del_set.contains(v))
                .cloned()
                .collect();
            if filtered.is_empty() {
                item.remove(&attr);
            } else {
                item.insert(attr, json!({"NS": filtered}));
            }
        } else if let (Some(existing_set), Some(del_set)) = (
            existing.get("BS").and_then(|v| v.as_array()),
            del_val.get("BS").and_then(|v| v.as_array()),
        ) {
            let filtered: Vec<Value> = existing_set
                .iter()
                .filter(|v| !del_set.contains(v))
                .cloned()
                .collect();
            if filtered.is_empty() {
                item.remove(&attr);
            } else {
                item.insert(attr, json!({"BS": filtered}));
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod remove_path_tests {
    use super::*;
    use serde_json::json;

    fn names() -> HashMap<String, String> {
        HashMap::new()
    }

    // bug-audit 2026-06-27, T1.3: REMOVE of a nested map path / list index used
    // to delete a literal top-level key (a no-op). It must reach into the path.
    #[test]
    fn removes_nested_map_path() {
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert(
            "profile".to_string(),
            json!({"M": {"first": {"S": "a"}, "middle": {"S": "b"}}}),
        );
        remove_path(&mut item, "profile.middle", &names());
        assert_eq!(
            item["profile"],
            json!({"M": {"first": {"S": "a"}}}),
            "nested map key removed, sibling kept"
        );
    }

    #[test]
    fn removes_list_index() {
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert(
            "tags".to_string(),
            json!({"L": [{"S": "x"}, {"S": "y"}, {"S": "z"}]}),
        );
        remove_path(&mut item, "tags[1]", &names());
        assert_eq!(item["tags"], json!({"L": [{"S": "x"}, {"S": "z"}]}));
    }

    #[test]
    fn removes_top_level_and_tolerates_missing() {
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert("a".to_string(), json!({"S": "1"}));
        remove_path(&mut item, "a", &names());
        assert!(!item.contains_key("a"));
        // Missing path is a silent no-op (no panic).
        remove_path(&mut item, "ghost.child", &names());
        remove_path(&mut item, "ghost[3]", &names());
    }
}
