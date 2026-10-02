use serde_json::Value;

use crate::jsonpath::{JsonPath, PathError, Step};

/// A Step Functions error: `(error, cause)`.
pub type StatesError = (String, String);

/// Apply InputPath to extract a subset of the raw input.
/// - `None` or `Some("$")` -> return input unchanged
/// - `Some("null")` handled at call site (pass `{}`)
/// - any other JSONPath -> the selected value; a definite path that matches
///   nothing fails the state with `States.Runtime`, as on AWS
pub fn apply_input_path(input: &Value, path: Option<&str>) -> Result<Value, StatesError> {
    match path {
        None | Some("$") => Ok(input.clone()),
        Some(p) => resolve_reference(input, p),
    }
}

/// Apply OutputPath to extract a subset of the effective output.
/// Same semantics as InputPath.
pub fn apply_output_path(output: &Value, path: Option<&str>) -> Result<Value, StatesError> {
    match path {
        None | Some("$") => Ok(output.clone()),
        Some(p) => resolve_reference(output, p),
    }
}

/// Apply ResultPath to merge a state's result into the input.
/// - `None` or `Some("$")` -> result replaces input entirely
/// - `Some("null")` -> discard result, return original input
/// - `Some("$.foo")` -> set result at that path within input
pub fn apply_result_path(input: &Value, result: &Value, path: Option<&str>) -> Value {
    match path {
        None | Some("$") => result.clone(),
        Some("null") => input.clone(),
        Some(p) => set_at_path(input, p, result),
    }
}

/// Evaluate a JSONPath against `root` (see [`crate::jsonpath`]).
pub fn resolve_path(root: &Value, path: &str) -> Result<Value, PathError> {
    crate::jsonpath::evaluate(root, path)
}

/// Resolve a path, mapping a miss to the `States.Runtime` error AWS raises
/// for InputPath / OutputPath / ItemsPath / SecondsPath and friends:
/// `Invalid path '$.x' : No results for path: $['x']`.
pub fn resolve_reference(root: &Value, path: &str) -> Result<Value, StatesError> {
    resolve_path(root, path).map_err(|e| runtime_error(path_error_cause(path, &e)))
}

/// [`resolve_reference`] for a path that may address the context object
/// (`$$...`), as `ItemsPath`, `SecondsPath` and friends may.
pub fn resolve_reference_with_context(
    input: &Value,
    context: Option<&Value>,
    path: &str,
) -> Result<Value, StatesError> {
    resolve_with_context(input, context, path)
        .map_err(|e| runtime_error(path_error_cause(path, &e)))
}

/// Resolve a path that may address the context object (`$$...`). Without a
/// context object, `$$` paths resolve to null.
pub fn resolve_with_context(
    input: &Value,
    context: Option<&Value>,
    path: &str,
) -> Result<Value, PathError> {
    match path.strip_prefix("$$") {
        Some(rest) => match context {
            Some(ctx) => resolve_path(ctx, &format!("${rest}")),
            None => Ok(Value::Null),
        },
        None => resolve_path(input, path),
    }
}

pub fn path_error_cause(path: &str, err: &PathError) -> String {
    match err {
        PathError::NoResults(norm) => {
            format!("Invalid path '{path}' : No results for path: {norm}")
        }
        PathError::Invalid(msg) => format!("Invalid path '{path}' : {msg}"),
    }
}

pub fn runtime_error(cause: String) -> StatesError {
    ("States.Runtime".to_string(), cause)
}

/// Set a value at a reference path (`$.a.b`, `$['a'][0]`) within a JSON
/// structure. An index step descends into the array stored at that point,
/// growing it with nulls as needed. A path that runs into a scalar leaves the
/// input unchanged.
fn set_at_path(root: &Value, path: &str, value: &Value) -> Value {
    let mut result = root.clone();
    let Some(steps) = JsonPath::parse(path).ok().and_then(|p| p.reference_steps()) else {
        return result;
    };

    // The empty container a missing intermediate node is created as.
    fn container_for(next: &Step) -> Value {
        match next {
            Step::Name(_) => serde_json::json!({}),
            Step::Index(_) => Value::Array(Vec::new()),
        }
    }

    fn assign(current: &mut Value, steps: &[Step], value: &Value) {
        let Some((step, rest)) = steps.split_first() else {
            *current = value.clone();
            return;
        };
        match step {
            Step::Name(name) => {
                if let Some(obj) = current.as_object_mut() {
                    match rest.first() {
                        None => {
                            obj.insert(name.clone(), value.clone());
                        }
                        Some(next) => {
                            let child = obj
                                .entry(name.clone())
                                .or_insert_with(|| container_for(next));
                            assign(child, rest, value);
                        }
                    }
                }
            }
            Step::Index(idx) => {
                if let Some(arr) = current.as_array_mut() {
                    let idx = if *idx < 0 {
                        match usize::try_from(arr.len() as i64 + idx) {
                            Ok(i) => i,
                            Err(_) => return,
                        }
                    } else {
                        *idx as usize
                    };
                    if arr.len() <= idx {
                        arr.resize(idx + 1, Value::Null);
                    }
                    if let Some(next) = rest.first() {
                        if arr[idx].is_null() {
                            arr[idx] = container_for(next);
                        }
                    }
                    assign(&mut arr[idx], rest, value);
                }
            }
        }
    }

    assign(&mut result, &steps, value);
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_resolve_path_root() {
        let input = json!({"a": 1});
        assert_eq!(resolve_path(&input, "$").unwrap(), input);
    }

    #[test]
    fn test_resolve_path_simple_field() {
        let input = json!({"name": "hello", "value": 42});
        assert_eq!(resolve_path(&input, "$.name").unwrap(), json!("hello"));
        assert_eq!(resolve_path(&input, "$.value").unwrap(), json!(42));
    }

    #[test]
    fn test_resolve_path_nested() {
        let input = json!({"a": {"b": {"c": 99}}});
        assert_eq!(resolve_path(&input, "$.a.b.c").unwrap(), json!(99));
    }

    #[test]
    fn test_resolve_path_missing() {
        let input = json!({"a": 1});
        assert_eq!(
            resolve_path(&input, "$.missing").unwrap_err(),
            PathError::NoResults("$['missing']".into())
        );
    }

    #[test]
    fn test_resolve_path_array_index() {
        let input = json!({"items": [10, 20, 30]});
        assert_eq!(resolve_path(&input, "$.items[0]").unwrap(), json!(10));
        assert_eq!(resolve_path(&input, "$.items[2]").unwrap(), json!(30));
    }

    #[test]
    fn test_apply_input_path_default() {
        let input = json!({"x": 1});
        assert_eq!(apply_input_path(&input, None).unwrap(), input);
        assert_eq!(apply_input_path(&input, Some("$")).unwrap(), input);
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
        assert_eq!(apply_output_path(&output, Some("$.a")).unwrap(), json!(1));
        assert_eq!(apply_output_path(&output, None).unwrap(), output);
    }

    #[test]
    fn test_resolve_path_unclosed_bracket_does_not_panic() {
        // Malformed JSONPath: `[` with no trailing `]`. Previously this sliced
        // `[bracket_pos + 1 .. part.len() - 1]` which underflows -> panic.
        let input = json!({"arr": [1, 2, 3]});
        // No field literally named "arr[" exists, so this resolves to Null,
        // but the key requirement is that it must NOT panic.
        assert!(matches!(
            resolve_path(&input, "$.arr["),
            Err(PathError::Invalid(_))
        ));
    }

    #[test]
    fn test_resolve_path_multibyte_after_bracket_does_not_panic() {
        // Multibyte char where the close bracket would be. `part.len() - 1`
        // previously landed mid-char -> "byte index is not a char boundary".
        let input = json!({"x": [1, 2, 3]});
        assert!(matches!(
            resolve_path(&input, "$.x[é"),
            Err(PathError::Invalid(_))
        ));
        // Also the closed-but-multibyte-inner case.
        assert!(matches!(
            resolve_path(&input, "$.x[é]"),
            Err(PathError::Invalid(_))
        ));
    }

    #[test]
    fn test_resolve_path_empty_brackets_do_not_panic() {
        let input = json!({"x": [1, 2, 3]});
        assert!(matches!(
            resolve_path(&input, "$.x[]"),
            Err(PathError::Invalid(_))
        ));
    }

    #[test]
    fn test_resolve_path_bracket_only_segment() {
        // A bare `[` as an entire segment.
        let input = json!({"a": 1});
        assert!(matches!(
            resolve_path(&input, "$.["),
            Err(PathError::Invalid(_))
        ));
        assert!(matches!(
            resolve_path(&input, "$.]"),
            Err(PathError::Invalid(_))
        ));
    }

    #[test]
    fn test_split_path_segments_well_formed_index_still_works() {
        // Ensure the hardening did not break the happy path.
        let input = json!({"items": [10, 20, 30]});
        assert_eq!(resolve_path(&input, "$.items[1]").unwrap(), json!(20));
        let nested = json!({"a": {"b": [{"c": 7}]}});
        assert_eq!(resolve_path(&nested, "$.a.b[0].c").unwrap(), json!(7));
    }

    #[test]
    fn test_apply_input_path_malformed_does_not_panic() {
        let input = json!({"arr": [1, 2, 3]});
        // Exercised via the public apply_* entrypoints used by the interpreter.
        let (err, _) = apply_input_path(&input, Some("$.arr[")).unwrap_err();
        assert_eq!(err, "States.Runtime");
        assert!(apply_output_path(&input, Some("$.x[é")).is_err());
    }
}

#[cfg(test)]
mod missing_path_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn missing_input_path_is_states_runtime() {
        let (err, cause) = apply_input_path(&json!({"a": 1}), Some("$.x")).unwrap_err();
        assert_eq!(err, "States.Runtime");
        assert_eq!(cause, "Invalid path '$.x' : No results for path: $['x']");
        let (err, _) = apply_output_path(&json!({"a": 1}), Some("$.a.b")).unwrap_err();
        assert_eq!(err, "States.Runtime");
        // A present null is fine.
        assert_eq!(
            apply_input_path(&json!({"a": null}), Some("$.a")).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn full_jsonpath_syntax_in_input_path() {
        let input =
            json!({"items": [{"id": 1, "ok": true}, {"id": 2, "ok": false}], "k": {"x y": 5}});
        assert_eq!(
            apply_input_path(&input, Some("$['k']['x y']")).unwrap(),
            json!(5)
        );
        assert_eq!(
            apply_input_path(&input, Some("$.items[-1].id")).unwrap(),
            json!(2)
        );
        assert_eq!(
            apply_input_path(&input, Some("$.items[*].id")).unwrap(),
            json!([1, 2])
        );
        assert_eq!(
            apply_input_path(&input, Some("$.items[?(@.ok == true)].id")).unwrap(),
            json!([1])
        );
        assert_eq!(
            apply_input_path(&input, Some("$.items[0:1]")).unwrap(),
            json!([{"id": 1, "ok": true}])
        );
    }

    #[test]
    fn result_path_bracket_notation() {
        let out = apply_result_path(&json!({"a": {}}), &json!(1), Some("$['a']['b c']"));
        assert_eq!(out, json!({"a": {"b c": 1}}));
        let out = apply_result_path(&json!({"a": [1, 2]}), &json!(9), Some("$.a[-1]"));
        assert_eq!(out, json!({"a": [1, 9]}));
    }

    #[test]
    fn context_paths() {
        let ctx = json!({"Execution": {"Id": "arn"}});
        assert_eq!(
            resolve_with_context(&json!({}), Some(&ctx), "$$.Execution.Id").unwrap(),
            json!("arn")
        );
        assert!(resolve_with_context(&json!({}), Some(&ctx), "$$.Nope").is_err());
        assert_eq!(
            resolve_with_context(&json!({"a": 1}), None, "$.a").unwrap(),
            json!(1)
        );
    }
}
