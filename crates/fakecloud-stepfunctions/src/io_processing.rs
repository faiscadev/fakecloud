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

/// Set a value at a simple JsonPath within a JSON structure. Handles both plain
/// object fields (`$.a.b`) and array-index segments (`$.a[0].b`). An index
/// segment `name[idx]` descends into element `idx` of the array stored at
/// `name`, growing the array with nulls as needed.
fn set_at_path(root: &Value, path: &str, value: &Value) -> Value {
    let mut result = root.clone();
    let path = path.strip_prefix("$.").unwrap_or(path);
    let segments = split_path_segments(path);

    fn assign(current: &mut Value, segments: &[PathSegment<'_>], value: &Value) {
        let (segment, rest) = match segments.split_first() {
            Some(pair) => pair,
            None => return,
        };
        let is_last = rest.is_empty();
        match segment {
            PathSegment::Field(name) => {
                if let Some(obj) = current.as_object_mut() {
                    if is_last {
                        obj.insert(name.to_string(), value.clone());
                    } else {
                        let child = obj
                            .entry(name.to_string())
                            .or_insert_with(|| serde_json::json!({}));
                        assign(child, rest, value);
                    }
                }
            }
            PathSegment::Index(name, idx) => {
                // Ensure `name` holds an array long enough to index `idx`.
                if let Some(obj) = current.as_object_mut() {
                    let arr_val = obj
                        .entry(name.to_string())
                        .or_insert_with(|| Value::Array(Vec::new()));
                    if !arr_val.is_array() {
                        *arr_val = Value::Array(Vec::new());
                    }
                    if let Some(arr) = arr_val.as_array_mut() {
                        if arr.len() <= *idx {
                            arr.resize(idx + 1, Value::Null);
                        }
                        if is_last {
                            arr[*idx] = value.clone();
                        } else {
                            if !arr[*idx].is_object() && !arr[*idx].is_array() {
                                arr[*idx] = serde_json::json!({});
                            }
                            assign(&mut arr[*idx], rest, value);
                        }
                    }
                }
            }
        }
    }

    assign(&mut result, &segments, value);
    result
}

enum PathSegment<'a> {
    Field(&'a str),
    Index(&'a str, usize),
}

fn split_path_segments(path: &str) -> Vec<PathSegment<'_>> {
    let mut segments = Vec::new();
    for part in path.split('.') {
        match parse_index_segment(part) {
            Some(segment) => segments.push(segment),
            None => segments.push(PathSegment::Field(part)),
        }
    }
    segments
}

/// Attempt to parse a `name[idx]` segment. Returns `None` (so the caller treats
/// the part as a plain field) for any malformed bracket expression. This must
/// never panic on adversarial input such as `"arr["` (no trailing `]`) or
/// `"x[é"` (multibyte char where the close bracket would be), both of which are
/// accepted by `CreateStateMachine` today.
fn parse_index_segment(part: &str) -> Option<PathSegment<'_>> {
    let open = part.find('[')?;
    // Must actually be a closed `[...]` ending the segment.
    if !part.ends_with(']') {
        return None;
    }
    let close = part.len() - 1;
    // `close` lands on the `]` byte; the index content is between the brackets.
    // Guard against `[]` (empty) and ranges that would underflow.
    let inner_start = open + 1;
    if inner_start > close {
        return None;
    }
    // Both `inner_start` and `close` are at ASCII bracket boundaries, so slicing
    // here can never split a multibyte char. The slice content itself may still
    // be non-ASCII, but it only needs to parse as a usize.
    let idx_str = part.get(inner_start..close)?;
    let name = part.get(..open)?;
    let idx = idx_str.parse::<usize>().ok()?;
    Some(PathSegment::Index(name, idx))
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
    fn test_set_at_path_non_object_intermediate() {
        // When an intermediate path segment is a non-object (e.g., a number),
        // set_at_path should not panic — it should bail out gracefully.
        let input = json!({"x": 42});
        let result = json!("hello");
        let output = apply_result_path(&input, &result, Some("$.x.nested"));
        // x is a number, can't set nested on it — should return input unchanged
        assert_eq!(output, json!({"x": 42}));
    }

    // L8: ResultPath with an array-index segment must descend into the array,
    // not insert a literal key like "a[0]".
    #[test]
    fn test_set_at_path_array_index_leaf() {
        let input = json!({"a": [1, 2, 3]});
        let result = json!(99);
        let output = apply_result_path(&input, &result, Some("$.a[1]"));
        assert_eq!(output, json!({"a": [1, 99, 3]}));
        // No stray literal "a[1]" key was created.
        assert!(output.get("a[1]").is_none());
    }

    #[test]
    fn test_set_at_path_array_index_then_field() {
        let input = json!({"items": [{"v": 1}]});
        let result = json!("done");
        let output = apply_result_path(&input, &result, Some("$.items[0].status"));
        assert_eq!(output, json!({"items": [{"v": 1, "status": "done"}]}));
    }

    #[test]
    fn test_set_at_path_array_index_grows_array() {
        let input = json!({});
        let result = json!("x");
        let output = apply_result_path(&input, &result, Some("$.a[2]"));
        assert_eq!(output, json!({"a": [null, null, "x"]}));
    }

    #[test]
    fn test_apply_output_path() {
        let output = json!({"a": 1, "b": 2});
        assert_eq!(apply_output_path(&output, Some("$.a")), json!(1));
        assert_eq!(apply_output_path(&output, None), output);
    }

    #[test]
    fn test_resolve_path_unclosed_bracket_does_not_panic() {
        // Malformed JSONPath: `[` with no trailing `]`. Previously this sliced
        // `[bracket_pos + 1 .. part.len() - 1]` which underflows -> panic.
        let input = json!({"arr": [1, 2, 3]});
        // No field literally named "arr[" exists, so this resolves to Null,
        // but the key requirement is that it must NOT panic.
        assert_eq!(resolve_path(&input, "$.arr["), Value::Null);
    }

    #[test]
    fn test_resolve_path_multibyte_after_bracket_does_not_panic() {
        // Multibyte char where the close bracket would be. `part.len() - 1`
        // previously landed mid-char -> "byte index is not a char boundary".
        let input = json!({"x": [1, 2, 3]});
        assert_eq!(resolve_path(&input, "$.x[é"), Value::Null);
        // Also the closed-but-multibyte-inner case.
        assert_eq!(resolve_path(&input, "$.x[é]"), Value::Null);
    }

    #[test]
    fn test_resolve_path_empty_brackets_do_not_panic() {
        let input = json!({"x": [1, 2, 3]});
        assert_eq!(resolve_path(&input, "$.x[]"), Value::Null);
    }

    #[test]
    fn test_resolve_path_bracket_only_segment() {
        // A bare `[` as an entire segment.
        let input = json!({"a": 1});
        assert_eq!(resolve_path(&input, "$.["), Value::Null);
        assert_eq!(resolve_path(&input, "$.]"), Value::Null);
    }

    #[test]
    fn test_split_path_segments_well_formed_index_still_works() {
        // Ensure the hardening did not break the happy path.
        let input = json!({"items": [10, 20, 30]});
        assert_eq!(resolve_path(&input, "$.items[1]"), json!(20));
        let nested = json!({"a": {"b": [{"c": 7}]}});
        assert_eq!(resolve_path(&nested, "$.a.b[0].c"), json!(7));
    }

    #[test]
    fn test_apply_input_path_malformed_does_not_panic() {
        let input = json!({"arr": [1, 2, 3]});
        // Exercised via the public apply_* entrypoints used by the interpreter.
        let _ = apply_input_path(&input, Some("$.arr["));
        let _ = apply_output_path(&input, Some("$.x[é"));
    }
}
