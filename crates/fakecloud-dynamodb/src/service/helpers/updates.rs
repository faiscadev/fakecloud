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
        // ADD only supports Number (increment) and Set (union). Anything else,
        // or a value whose type doesn't match the existing attribute's type, is
        // a ValidationException on real DynamoDB — not a silent no-op (bug-hunt
        // 2026-07-01, DynamoDB ADD/DELETE type mismatch).
        let set_types = ["SS", "NS", "BS"];
        if let Some(existing) = item.get(&attr) {
            if let (Some(existing_num), Some(add_num)) = (
                existing.get("N").and_then(|n| n.as_str()),
                add_val.get("N").and_then(|n| n.as_str()),
            ) {
                // Arbitrary-precision decimal add — f64 rounds past 2^53.
                if let Some(num_str) = decimal_add_sub(existing_num, add_num, true) {
                    item.insert(attr, json!({"N": num_str}));
                }
            } else if let Some(set_type) = set_types
                .iter()
                .find(|t| existing.get(**t).and_then(|v| v.as_array()).is_some())
            {
                let existing_set = existing.get(*set_type).and_then(|v| v.as_array()).unwrap();
                let Some(add_set) = add_val.get(*set_type).and_then(|v| v.as_array()) else {
                    return Err(add_type_mismatch(&attr));
                };
                let mut merged: Vec<Value> = existing_set.clone();
                for v in add_set {
                    if !merged.contains(v) {
                        merged.push(v.clone());
                    }
                }
                let mut obj = serde_json::Map::new();
                obj.insert((*set_type).to_string(), json!(merged));
                item.insert(attr, Value::Object(obj));
            } else {
                // Existing attribute is neither Number nor Set, or the add value
                // is a different type than the existing Number/Set.
                return Err(add_type_mismatch(&attr));
            }
        } else {
            // New attribute: ADD only allows Number or Set values.
            let is_addable =
                add_val.get("N").is_some() || set_types.iter().any(|t| add_val.get(*t).is_some());
            if !is_addable {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    "An operand in the update expression has an incorrect data type",
                ));
            }
            item.insert(attr, add_val.clone());
        }
    }

    Ok(())
}

/// ADD requires the operand type to match the existing attribute (Number to a
/// Number, a set of the same element type to a set).
fn add_type_mismatch(attr: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ValidationException",
        format!("Type mismatch for attribute to update: {attr}"),
    )
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
        } else {
            // DELETE `attr :val` removes set elements; the existing attribute
            // and the operand must be sets of the same element type. Any other
            // pairing is a ValidationException, not a silent no-op (bug-hunt
            // 2026-07-01, DynamoDB ADD/DELETE type mismatch).
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!("Type mismatch for attribute to update: {attr}"),
            ));
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

    fn vals(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn add_number_to_existing_set_is_type_mismatch() {
        // ADD :n to an existing SS attribute is a ValidationException, not a
        // silent no-op (bug-hunt 2026-07-01, DynamoDB ADD/DELETE mismatch).
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert("tags".to_string(), json!({"SS": ["x"]}));
        let err = apply_add_assignment(
            &mut item,
            "tags :n",
            &names(),
            &vals(&[(":n", json!({"N": "1"}))]),
        )
        .unwrap_err();
        assert!(format!("{err:?}").contains("ValidationException"));
        // The set is unchanged.
        assert_eq!(item["tags"], json!({"SS": ["x"]}));
    }

    #[test]
    fn add_string_to_new_attribute_is_rejected() {
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        let err = apply_add_assignment(
            &mut item,
            "note :s",
            &names(),
            &vals(&[(":s", json!({"S": "hi"}))]),
        )
        .unwrap_err();
        assert!(format!("{err:?}").contains("ValidationException"));
    }

    #[test]
    fn add_number_increments_and_set_unions() {
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert("count".to_string(), json!({"N": "5"}));
        item.insert("tags".to_string(), json!({"SS": ["x"]}));
        apply_add_assignment(
            &mut item,
            "count :n",
            &names(),
            &vals(&[(":n", json!({"N": "3"}))]),
        )
        .unwrap();
        apply_add_assignment(
            &mut item,
            "tags :s",
            &names(),
            &vals(&[(":s", json!({"SS": ["y"]}))]),
        )
        .unwrap();
        assert_eq!(item["count"], json!({"N": "8"}));
        assert_eq!(item["tags"], json!({"SS": ["x", "y"]}));
    }

    #[test]
    fn delete_on_non_set_is_type_mismatch() {
        let mut item: HashMap<String, AttributeValue> = HashMap::new();
        item.insert("count".to_string(), json!({"N": "5"}));
        let err = apply_delete_assignment(
            &mut item,
            "count :s",
            &names(),
            &vals(&[(":s", json!({"SS": ["y"]}))]),
        )
        .unwrap_err();
        assert!(format!("{err:?}").contains("ValidationException"));
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
