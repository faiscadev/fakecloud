//! Minimal RFC 6902 JSON Patch applied to Cloud Control desired state.
//!
//! Cloud Control's `UpdateResource` takes a `PatchDocument` (a JSON Patch array)
//! describing the change from the resource's current properties to the desired
//! ones. We apply it to the stored desired state, then hand the resulting
//! properties to the CloudFormation update handler. Supports the full operation
//! set (`add`, `remove`, `replace`, `move`, `copy`, `test`) over JSON Pointer
//! paths (RFC 6901).

use serde_json::Value;

/// Apply a JSON Patch document (an array of operations) to `doc` in place.
pub fn apply_json_patch(doc: &mut Value, patch: &Value) -> Result<(), String> {
    let ops = patch
        .as_array()
        .ok_or_else(|| "patch document must be a JSON array".to_string())?;
    for op in ops {
        apply_one(doc, op)?;
    }
    Ok(())
}

fn apply_one(doc: &mut Value, op: &Value) -> Result<(), String> {
    let kind = op
        .get("op")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "patch operation missing 'op'".to_string())?;
    match kind {
        "add" => {
            let path = path_of(op)?;
            let value = require_value(op)?;
            add(doc, &path, value)
        }
        "remove" => {
            let path = path_of(op)?;
            remove(doc, &path).map(|_| ())
        }
        "replace" => {
            let path = path_of(op)?;
            let value = require_value(op)?;
            // replace requires the target to exist.
            get(doc, &path)
                .ok_or_else(|| format!("replace target does not exist: /{}", path.join("/")))?;
            remove(doc, &path)?;
            add(doc, &path, value)
        }
        "move" => {
            let from = pointer_of(op, "from")?;
            let path = path_of(op)?;
            let value = remove(doc, &from)?;
            add(doc, &path, value)
        }
        "copy" => {
            let from = pointer_of(op, "from")?;
            let path = path_of(op)?;
            let value = get(doc, &from)
                .cloned()
                .ok_or_else(|| format!("copy source does not exist: /{}", from.join("/")))?;
            add(doc, &path, value)
        }
        "test" => {
            let path = path_of(op)?;
            let expected = require_value(op)?;
            let actual = get(doc, &path)
                .ok_or_else(|| format!("test target does not exist: /{}", path.join("/")))?;
            if *actual != expected {
                return Err(format!("test failed at /{}", path.join("/")));
            }
            Ok(())
        }
        other => Err(format!("unsupported patch op: {other}")),
    }
}

/// Require the `value` member on `add`/`replace`/`test`. A missing member is a
/// malformed patch, not an implicit `null` (RFC 6902 s4.1/4.3/4.6).
fn require_value(op: &Value) -> Result<Value, String> {
    op.get("value")
        .cloned()
        .ok_or_else(|| "patch operation missing 'value'".to_string())
}

fn path_of(op: &Value) -> Result<Vec<String>, String> {
    pointer_of(op, "path")
}

fn pointer_of(op: &Value, field: &str) -> Result<Vec<String>, String> {
    let raw = op
        .get(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| format!("patch operation missing '{field}'"))?;
    parse_pointer(raw)
}

/// Parse an RFC 6901 JSON Pointer into decoded reference tokens. A non-empty
/// pointer that does not begin with `/` is malformed (RFC 6901 s3) and is
/// rejected rather than silently targeting the whole document; so is any token
/// containing a stray `~` not followed by `0` or `1`.
fn parse_pointer(pointer: &str) -> Result<Vec<String>, String> {
    if pointer.is_empty() {
        return Ok(Vec::new());
    }
    if !pointer.starts_with('/') {
        return Err(format!(
            "invalid JSON Pointer '{pointer}': must be empty or begin with '/'"
        ));
    }
    pointer
        .split('/')
        .skip(1) // leading empty segment before the first '/'
        .map(unescape_token)
        .collect()
}

/// Decode a single RFC 6901 reference token: `~1` -> `/`, `~0` -> `~`. A `~`
/// followed by anything else (or trailing) is an invalid escape.
fn unescape_token(token: &str) -> Result<String, String> {
    let mut out = String::with_capacity(token.len());
    let mut chars = token.chars();
    while let Some(c) = chars.next() {
        if c == '~' {
            match chars.next() {
                Some('0') => out.push('~'),
                Some('1') => out.push('/'),
                other => {
                    return Err(format!(
                        "invalid JSON Pointer escape '~{}'",
                        other.map(String::from).unwrap_or_default()
                    ));
                }
            }
        } else {
            out.push(c);
        }
    }
    Ok(out)
}

fn get<'a>(doc: &'a Value, path: &[String]) -> Option<&'a Value> {
    let mut cur = doc;
    for token in path {
        cur = match cur {
            Value::Object(map) => map.get(token)?,
            Value::Array(arr) => arr.get(token.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

fn add(doc: &mut Value, path: &[String], value: Value) -> Result<(), String> {
    if path.is_empty() {
        *doc = value;
        return Ok(());
    }
    let (last, parents) = path.split_last().unwrap();
    let target = get_mut(doc, parents)
        .ok_or_else(|| format!("add parent path does not exist: /{}", parents.join("/")))?;
    match target {
        Value::Object(map) => {
            map.insert(last.clone(), value);
            Ok(())
        }
        Value::Array(arr) => {
            if last == "-" {
                arr.push(value);
                return Ok(());
            }
            let idx = last
                .parse::<usize>()
                .map_err(|_| format!("invalid array index: {last}"))?;
            if idx > arr.len() {
                return Err(format!("array index out of bounds: {idx}"));
            }
            arr.insert(idx, value);
            Ok(())
        }
        _ => Err(format!(
            "cannot add into non-container at /{}",
            parents.join("/")
        )),
    }
}

fn remove(doc: &mut Value, path: &[String]) -> Result<Value, String> {
    let (last, parents) = path
        .split_last()
        .ok_or_else(|| "cannot remove the whole document".to_string())?;
    let target = get_mut(doc, parents)
        .ok_or_else(|| format!("remove parent path does not exist: /{}", parents.join("/")))?;
    match target {
        Value::Object(map) => map
            .remove(last)
            .ok_or_else(|| format!("remove target does not exist: {last}")),
        Value::Array(arr) => {
            let idx = last
                .parse::<usize>()
                .map_err(|_| format!("invalid array index: {last}"))?;
            if idx >= arr.len() {
                return Err(format!("array index out of bounds: {idx}"));
            }
            Ok(arr.remove(idx))
        }
        _ => Err(format!(
            "cannot remove from non-container at /{}",
            parents.join("/")
        )),
    }
}

fn get_mut<'a>(doc: &'a mut Value, path: &[String]) -> Option<&'a mut Value> {
    let mut cur = doc;
    for token in path {
        cur = match cur {
            Value::Object(map) => map.get_mut(token)?,
            Value::Array(arr) => arr.get_mut(token.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn replace_scalar() {
        let mut doc = json!({"BucketName": "old", "Tags": []});
        apply_json_patch(
            &mut doc,
            &json!([{"op":"replace","path":"/BucketName","value":"new"}]),
        )
        .unwrap();
        assert_eq!(doc["BucketName"], "new");
    }

    #[test]
    fn add_and_remove() {
        let mut doc = json!({"A": 1});
        apply_json_patch(&mut doc, &json!([{"op":"add","path":"/B","value":2}])).unwrap();
        assert_eq!(doc["B"], 2);
        apply_json_patch(&mut doc, &json!([{"op":"remove","path":"/A"}])).unwrap();
        assert!(doc.get("A").is_none());
    }

    #[test]
    fn array_append_and_index() {
        let mut doc = json!({"L": [1, 2]});
        apply_json_patch(&mut doc, &json!([{"op":"add","path":"/L/-","value":3}])).unwrap();
        assert_eq!(doc["L"], json!([1, 2, 3]));
        apply_json_patch(&mut doc, &json!([{"op":"remove","path":"/L/0"}])).unwrap();
        assert_eq!(doc["L"], json!([2, 3]));
    }

    #[test]
    fn move_and_copy() {
        let mut doc = json!({"A": {"X": 1}, "B": {}});
        apply_json_patch(
            &mut doc,
            &json!([{"op":"move","from":"/A/X","path":"/B/Y"}]),
        )
        .unwrap();
        assert_eq!(doc["B"]["Y"], 1);
        assert!(doc["A"].get("X").is_none());
        apply_json_patch(
            &mut doc,
            &json!([{"op":"copy","from":"/B/Y","path":"/B/Z"}]),
        )
        .unwrap();
        assert_eq!(doc["B"]["Z"], 1);
    }

    #[test]
    fn test_op_mismatch_errors() {
        let mut doc = json!({"A": 1});
        assert!(apply_json_patch(&mut doc, &json!([{"op":"test","path":"/A","value":2}])).is_err());
    }

    #[test]
    fn missing_value_is_rejected() {
        // `add`/`replace`/`test` must carry a `value` member; a missing one is a
        // malformed patch, not an implicit null that mutates the document.
        let mut doc = json!({"A": 1});
        assert!(apply_json_patch(&mut doc, &json!([{"op":"add","path":"/B"}])).is_err());
        assert!(apply_json_patch(&mut doc, &json!([{"op":"replace","path":"/A"}])).is_err());
        assert!(apply_json_patch(&mut doc, &json!([{"op":"test","path":"/A"}])).is_err());
        // Document is untouched by the rejected patches.
        assert_eq!(doc, json!({"A": 1}));
    }

    #[test]
    fn malformed_pointer_is_rejected() {
        // A non-empty pointer that does not start with '/' is invalid and must
        // not silently target the whole document.
        let mut doc = json!({"BucketName": "old"});
        assert!(apply_json_patch(
            &mut doc,
            &json!([{"op":"replace","path":"BucketName","value":"new"}])
        )
        .is_err());
        assert_eq!(doc, json!({"BucketName": "old"}));
    }

    #[test]
    fn escaped_pointer_tokens() {
        let mut doc = json!({"a/b": 1, "c~d": 2});
        apply_json_patch(
            &mut doc,
            &json!([{"op":"replace","path":"/a~1b","value":9}]),
        )
        .unwrap();
        assert_eq!(doc["a/b"], 9);
        apply_json_patch(
            &mut doc,
            &json!([{"op":"replace","path":"/c~0d","value":8}]),
        )
        .unwrap();
        assert_eq!(doc["c~d"], 8);
    }

    #[test]
    fn invalid_pointer_escape_is_rejected() {
        // `~` must be followed by `0` or `1`; `~2` (and a trailing `~`) are
        // invalid escapes and must not mutate the document.
        let mut doc = json!({"A": 1});
        assert!(
            apply_json_patch(&mut doc, &json!([{"op":"add","path":"/A~2B","value":9}])).is_err()
        );
        assert!(apply_json_patch(&mut doc, &json!([{"op":"add","path":"/A~","value":9}])).is_err());
        assert_eq!(doc, json!({"A": 1}));
    }
}
