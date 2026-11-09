//! Flow node input expressions: the JsonPath subset the Flows user guide lists
//! (`$.data`, `.name`, `[int]`, `[int, int]`, `[int:int]` and `*`), anything else
//! being a malformed expression.

use serde_json::Value;

#[derive(Debug, PartialEq)]
enum Step {
    Field(String),
    Wildcard,
    Index(usize),
    Indices(Vec<usize>),
    Slice(Option<usize>, Option<usize>),
}

fn malformed(expression: &str) -> String {
    format!("The input expression '{expression}' is not valid.")
}

fn parse(expression: &str) -> Result<Vec<Step>, String> {
    let bad = || malformed(expression);
    let mut rest = expression.strip_prefix("$.data").ok_or_else(bad)?;
    let mut steps = Vec::new();
    while !rest.is_empty() {
        if let Some(after_dot) = rest.strip_prefix('.') {
            let end = after_dot.find(['.', '[']).unwrap_or(after_dot.len());
            let name = &after_dot[..end];
            if name.is_empty() || name.contains(']') {
                return Err(bad());
            }
            steps.push(if name == "*" {
                Step::Wildcard
            } else {
                Step::Field(name.to_string())
            });
            rest = &after_dot[end..];
        } else if let Some(after_bracket) = rest.strip_prefix('[') {
            let end = after_bracket.find(']').ok_or_else(bad)?;
            steps.push(parse_selector(after_bracket[..end].trim()).ok_or_else(bad)?);
            rest = &after_bracket[end + 1..];
        } else {
            return Err(bad());
        }
    }
    Ok(steps)
}

fn parse_index(text: &str) -> Option<usize> {
    let text = text.trim();
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    text.parse().ok()
}

fn parse_selector(selector: &str) -> Option<Step> {
    if selector == "*" {
        return Some(Step::Wildcard);
    }
    if let Some((start, end)) = selector.split_once(':') {
        let bound = |text: &str| match text.trim() {
            "" => Some(None),
            digits => parse_index(digits).map(Some),
        };
        return Some(Step::Slice(bound(start)?, bound(end)?));
    }
    if selector.contains(',') {
        let indices = selector
            .split(',')
            .map(parse_index)
            .collect::<Option<Vec<_>>>()?;
        return Some(Step::Indices(indices));
    }
    parse_index(selector).map(Step::Index)
}

/// Apply an expression to the whole input a connection delivers. A path that
/// selects nothing is an error, as a scalar reached with anything but `$.data`
/// is, rather than a silent null.
pub(crate) fn apply(expression: &str, whole: &Value) -> Result<Value, String> {
    let steps = parse(expression)?;
    let missing = || format!("The input expression '{expression}' selects nothing in the input.");
    let mut selected: Vec<&Value> = vec![whole];
    // Past a wildcard or a list of positions one selection can be many, and a
    // branch without the next step simply contributes nothing.
    let mut many = false;
    // An index list or a slice always answers an array; a wildcard does only when
    // it has several results.
    let mut always_array = false;
    for step in &steps {
        let mut next = Vec::new();
        for value in &selected {
            match step {
                Step::Field(name) => match value.get(name.as_str()) {
                    Some(found) if value.is_object() => next.push(found),
                    _ if many => {}
                    _ => return Err(missing()),
                },
                Step::Index(index) => match value.as_array().and_then(|items| items.get(*index)) {
                    Some(found) => next.push(found),
                    None if many => {}
                    None => return Err(missing()),
                },
                Step::Wildcard => match value {
                    Value::Object(members) => next.extend(members.values()),
                    Value::Array(items) => next.extend(items.iter()),
                    _ if many => {}
                    _ => return Err(missing()),
                },
                Step::Indices(indices) => {
                    let Some(items) = value.as_array() else {
                        if many {
                            continue;
                        }
                        return Err(missing());
                    };
                    next.extend(indices.iter().filter_map(|index| items.get(*index)));
                }
                Step::Slice(start, end) => {
                    let Some(items) = value.as_array() else {
                        if many {
                            continue;
                        }
                        return Err(missing());
                    };
                    let end = end.unwrap_or(items.len()).min(items.len());
                    let start = start.unwrap_or(0).min(end);
                    next.extend(&items[start..end]);
                }
            }
        }
        match step {
            Step::Wildcard => many = true,
            Step::Indices(_) | Step::Slice(..) => {
                many = true;
                always_array = true;
            }
            Step::Field(_) | Step::Index(_) => {}
        }
        selected = next;
    }
    if always_array {
        return Ok(Value::Array(selected.into_iter().cloned().collect()));
    }
    match selected.as_slice() {
        [] => Err(missing()),
        [single] => Ok((*single).clone()),
        several => Ok(Value::Array(several.iter().map(|v| (*v).clone()).collect())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The object the Flows user guide's expression table is written against.
    fn guide_input() -> Value {
        json!({
            "animals": {
                "mammals": ["cat", "dog"],
                "reptiles": ["snake", "turtle", "iguana"]
            },
            "organisms": {
                "mammals": ["rabbit", "horse", "mouse"],
                "flowers": ["lily", "daisy"]
            },
            "numbers": [1, 2, 3, 5, 8]
        })
    }

    #[test]
    fn the_guide_examples_give_the_guide_results() {
        let input = guide_input();
        assert_eq!(apply("$.data", &input).unwrap(), input);
        assert_eq!(
            apply("$.data.numbers", &input).unwrap(),
            json!([1, 2, 3, 5, 8])
        );
        assert_eq!(
            apply("$.data.animals.reptiles[2]", &input).unwrap(),
            json!("iguana")
        );
        assert_eq!(
            apply("$.data.numbers[0, 3]", &input).unwrap(),
            json!([1, 5])
        );
        assert_eq!(
            apply("$.data.organisms.mammals[1:]", &input).unwrap(),
            json!(["horse", "mouse"])
        );
        assert_eq!(
            apply("$.data.*.mammals", &input).unwrap(),
            json!([["cat", "dog"], ["rabbit", "horse", "mouse"]])
        );
    }

    #[test]
    fn slices_and_wildcards_handle_their_edges() {
        let input = guide_input();
        assert_eq!(apply("$.data.numbers[:2]", &input).unwrap(), json!([1, 2]));
        assert_eq!(apply("$.data.numbers[9:]", &input).unwrap(), json!([]));
        assert_eq!(apply("$.data.numbers[3:1]", &input).unwrap(), json!([]));
        assert_eq!(apply("$.data.numbers[0, 99]", &input).unwrap(), json!([1]));
        assert_eq!(
            apply("$.data.numbers[*]", &input).unwrap(),
            json!([1, 2, 3, 5, 8])
        );
        // One result from a wildcard is that result, not an array of it.
        assert_eq!(
            apply("$.data.*.flowers", &input).unwrap(),
            json!(["lily", "daisy"])
        );
    }

    #[test]
    fn malformed_expressions_are_errors() {
        for broken in [
            "data",
            "$.database",
            "$.data.",
            "$.data..a",
            "$.data[",
            "$.data[]",
            "$.data[-1]",
            "$.data[a]",
            "$.data[1,]",
            "$.data[1:2:3]",
            "$.data]",
            "$.data.a]",
        ] {
            let error = apply(broken, &guide_input()).unwrap_err();
            assert!(error.contains("not valid"), "{broken}: {error}");
        }
    }

    #[test]
    fn a_path_that_selects_nothing_is_an_error() {
        let input = guide_input();
        for path in [
            "$.data.nope",
            "$.data.numbers[99]",
            "$.data.numbers.x",
            "$.data.*.nope",
        ] {
            let error = apply(path, &input).unwrap_err();
            assert!(error.contains("selects nothing"), "{path}: {error}");
        }
        // The guide: a scalar's only expression is `$.data`.
        assert_eq!(apply("$.data", &json!("hi")).unwrap(), json!("hi"));
        assert!(apply("$.data.a", &json!("hi")).is_err());
        assert!(apply("$.data[0]", &json!(1)).is_err());
        assert!(apply("$.data.*", &json!(true)).is_err());
    }
}
