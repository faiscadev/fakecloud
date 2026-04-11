use serde_json::Value;

/// Apply InputPath to extract a subset of the raw input.
/// - `None` or `Some("$")` → return input unchanged
/// - `Some("null")` handled at call site (pass `{}`)
/// - `Some("$.foo.bar")` → extract nested field
pub fn apply_input_path(input: &Value, path: Option<&str>) -> Value {
    match path {
        None | Some("$") => input.clone(),
        Some(p) => resolve_path(input, p),
    }
}

/// Apply OutputPath to extract a subset of the effective output.
/// Same semantics as InputPath.
pub fn apply_output_path(output: &Value, path: Option<&str>) -> Value {
    match path {
        None | Some("$") => output.clone(),
        Some(p) => resolve_path(output, p),
    }
}

/// Apply ResultPath to merge a state's result into the input.
/// - `None` or `Some("$")` → result replaces input entirely
/// - `Some("null")` → discard result, return original input
/// - `Some("$.foo")` → set result at that path within input
pub fn apply_result_path(input: &Value, result: &Value, path: Option<&str>) -> Value {
    match path {
        None | Some("$") => result.clone(),
        Some("null") => input.clone(),
        Some(p) => set_at_path(input, p, result),
    }
}

/// Resolve a simple JsonPath expression against a JSON value.
/// Supports: `$`, `$.field`, `$.field.nested`, `$.arr[0]`
pub fn resolve_path(root: &Value, path: &str) -> Value {
    if path == "$" {
        return root.clone();
    }

    let path = path.strip_prefix("$.").unwrap_or(path);
    let mut current = root;

    for segment in split_path_segments(path) {
        match segment {
            PathSegment::Field(name) => {
                current = match current.get(name) {
                    Some(v) => v,
                    None => return Value::Null,
                };
            }
            PathSegment::Index(name, idx) => {
                current = match current.get(name) {
                    Some(v) => match v.get(idx) {
                        Some(v) => v,
                        None => return Value::Null,
                    },
                    None => return Value::Null,
                };
            }
        }
    }

    current.clone()
}

/// Set a value at a simple JsonPath within a JSON structure.
fn set_at_path(root: &Value, path: &str, value: &Value) -> Value {
    let mut result = root.clone();
    let path = path.strip_prefix("$.").unwrap_or(path);
    let segments: Vec<&str> = path.split('.').collect();

    let mut current = &mut result;
    for (i, segment) in segments.iter().enumerate() {
        if i == segments.len() - 1 {
            // Last segment — set the value
            if let Some(obj) = current.as_object_mut() {
                obj.insert(segment.to_string(), value.clone());
            }
        } else {
            // Intermediate — ensure object exists
            if current.get(*segment).is_none() {
                if let Some(obj) = current.as_object_mut() {
                    obj.insert(segment.to_string(), serde_json::json!({}));
                }
            }
            current = current.get_mut(*segment).unwrap();
        }
    }

    result
}

enum PathSegment<'a> {
    Field(&'a str),
    Index(&'a str, usize),
}

fn split_path_segments(path: &str) -> Vec<PathSegment<'_>> {
    let mut segments = Vec::new();
    for part in path.split('.') {
        if let Some(bracket_pos) = part.find('[') {
            let name = &part[..bracket_pos];
            let idx_str = &part[bracket_pos + 1..part.len() - 1];
            if let Ok(idx) = idx_str.parse::<usize>() {
                segments.push(PathSegment::Index(name, idx));
            } else {
                segments.push(PathSegment::Field(part));
            }
        } else {
            segments.push(PathSegment::Field(part));
        }
    }
    segments
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_resolve_path_root() {
        let input = json!({"a": 1});
        assert_eq!(resolve_path(&input, "$"), input);
    }

    #[test]
    fn test_resolve_path_simple_field() {
        let input = json!({"name": "hello", "value": 42});
        assert_eq!(resolve_path(&input, "$.name"), json!("hello"));
        assert_eq!(resolve_path(&input, "$.value"), json!(42));
    }

    #[test]
    fn test_resolve_path_nested() {
        let input = json!({"a": {"b": {"c": 99}}});
        assert_eq!(resolve_path(&input, "$.a.b.c"), json!(99));
    }

    #[test]
    fn test_resolve_path_missing() {
        let input = json!({"a": 1});
        assert_eq!(resolve_path(&input, "$.missing"), Value::Null);
    }

    #[test]
    fn test_resolve_path_array_index() {
        let input = json!({"items": [10, 20, 30]});
        assert_eq!(resolve_path(&input, "$.items[0]"), json!(10));
        assert_eq!(resolve_path(&input, "$.items[2]"), json!(30));
    }

    #[test]
    fn test_apply_input_path_default() {
        let input = json!({"x": 1});
        assert_eq!(apply_input_path(&input, None), input);
        assert_eq!(apply_input_path(&input, Some("$")), input);
    }

    #[test]
    fn test_apply_result_path_default() {
        let input = json!({"x": 1});
        let result = json!({"y": 2});
        // Default: result replaces input
        assert_eq!(apply_result_path(&input, &result, None), result);
        assert_eq!(apply_result_path(&input, &result, Some("$")), result);
    }

    #[test]
    fn test_apply_result_path_null() {
        let input = json!({"x": 1});
        let result = json!({"y": 2});
        // null: discard result, keep input
        assert_eq!(apply_result_path(&input, &result, Some("null")), input);
    }

    #[test]
    fn test_apply_result_path_nested() {
        let input = json!({"x": 1});
        let result = json!("hello");
        let output = apply_result_path(&input, &result, Some("$.result"));
        assert_eq!(output, json!({"x": 1, "result": "hello"}));
    }

    #[test]
    fn test_apply_output_path() {
        let output = json!({"a": 1, "b": 2});
        assert_eq!(apply_output_path(&output, Some("$.a")), json!(1));
        assert_eq!(apply_output_path(&output, None), output);
    }
}
