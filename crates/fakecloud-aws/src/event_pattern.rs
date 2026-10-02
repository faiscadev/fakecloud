//! AWS event-pattern matching, shared by EventBridge rules / archives,
//! EventBridge Pipes filters and Lambda event source mapping
//! `FilterCriteria`. All of them use the same pattern language (AWS's
//! event-ruler), so they share one evaluator here.
//!
//! Semantics implemented (event-ruler's `rulesForJSONEvent`):
//!
//! - Sibling keys are ANDed; `$or` is an alternation group ANDed with the
//!   other siblings.
//! - A nested pattern object descends into the event. When the event value is
//!   an array, it is "crushed out": the pattern matches when ANY element
//!   matches the whole sub-pattern (fields of one sub-pattern must come from
//!   the same array element).
//! - A pattern list (`"k": [...]`) matches when ANY of its entries matches ANY
//!   of the field's leaf values. Arrays (including nested arrays) are
//!   flattened into their scalar leaves; objects are intermediate nodes, not
//!   leaves.
//! - Every matcher except `{"exists": false}` requires the field to be
//!   present (have at least one leaf). `[null]` matches only a present JSON
//!   `null`, never an absent field, and `{"exists": true}` matches a present
//!   `null`.
//! - `anything-but` is evaluated per leaf like every other matcher, so against
//!   an array it matches when at least one element is not excluded.
//! - Numbers compare numerically (as IEEE 754 doubles), so `100` and `100.0`
//!   are equal for exact matches, `anything-but` and `numeric`.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

use serde_json::{Map, Value};

/// Match `event` against an event `pattern`. A pattern that is not a JSON
/// object never matches.
pub fn matches(pattern: &Value, event: &Value) -> bool {
    match pattern {
        Value::Object(obj) => match_object(obj, Some(event)),
        _ => false,
    }
}

/// Structural check AWS applies to a filter pattern: a JSON object whose
/// leaves are all lists of matchers (or nested objects); `$or` must be a
/// list of such objects. A bare scalar leaf such as `{"foo": "bar"}` is
/// rejected.
pub fn is_valid_structure(pattern: &Value) -> bool {
    match pattern {
        Value::Object(obj) => obj.iter().all(|(k, v)| match v {
            Value::Object(_) => is_valid_structure(v),
            Value::Array(alts) if k == "$or" => {
                alts.iter().all(|a| a.is_object() && is_valid_structure(a))
            }
            Value::Array(_) => true,
            _ => false,
        }),
        _ => false,
    }
}

/// Rewrite every scalar leaf (`{"foo": "bar"}`) as a one-element list
/// (`{"foo": ["bar"]}`). Patterns stored before scalar leaves were rejected
/// are normalized this way on load so they keep matching what they used to.
pub fn normalize_scalar_leaves(pattern: &Value) -> Value {
    match pattern {
        Value::Object(obj) => Value::Object(
            obj.iter()
                .map(|(k, v)| {
                    let v = match v {
                        Value::Object(_) => normalize_scalar_leaves(v),
                        Value::Array(alts) if k == "$or" => {
                            Value::Array(alts.iter().map(normalize_scalar_leaves).collect())
                        }
                        Value::Array(_) => v.clone(),
                        scalar => Value::Array(vec![scalar.clone()]),
                    };
                    (k.clone(), v)
                })
                .collect(),
        ),
        other => other.clone(),
    }
}

fn match_object(pattern: &Map<String, Value>, node: Option<&Value>) -> bool {
    // An array-valued node is crushed out: any element may satisfy the whole
    // sub-pattern.
    // An empty array holds no leaves, so every field under it is absent.
    if let Some(Value::Array(items)) = node {
        if items.is_empty() {
            return match_object(pattern, None);
        }
        return items.iter().any(|item| match_object(pattern, Some(item)));
    }
    let obj = match node {
        Some(Value::Object(o)) => Some(o),
        _ => None,
    };
    for (key, sub) in pattern {
        if key == "$or" {
            continue;
        }
        let field = obj.and_then(|o| o.get(key));
        let ok = match sub {
            Value::Object(sub_obj) => match_object(sub_obj, field),
            Value::Array(list) => match_list(list, field),
            // A scalar leaf is not a valid pattern; it constrains nothing it
            // could ever match.
            _ => false,
        };
        if !ok {
            return false;
        }
    }
    if let Some(Value::Array(alternatives)) = pattern.get("$or") {
        let any = alternatives.iter().any(|alt| match alt {
            Value::Object(alt_obj) => match_object(alt_obj, node),
            _ => false,
        });
        if !any {
            return false;
        }
    }
    true
}

/// Collect the scalar leaves of a field value: a scalar is its own leaf,
/// arrays are flattened recursively, objects contribute nothing.
fn collect_leaves<'a>(value: &'a Value, out: &mut Vec<&'a Value>) {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_leaves(item, out);
            }
        }
        Value::Object(_) => {}
        scalar => out.push(scalar),
    }
}

fn match_list(list: &[Value], field: Option<&Value>) -> bool {
    let mut leaves = Vec::new();
    if let Some(v) = field {
        collect_leaves(v, &mut leaves);
    }
    let present = !leaves.is_empty();
    list.iter().any(|entry| {
        if let Some(exists) = entry.as_object().and_then(|o| o.get("exists")) {
            return match exists.as_bool() {
                Some(true) => present,
                Some(false) => !present,
                None => false,
            };
        }
        leaves.iter().any(|leaf| match_leaf(entry, leaf))
    })
}

/// Evaluate one pattern-list entry against one scalar leaf value.
pub fn match_leaf(entry: &Value, leaf: &Value) -> bool {
    match entry {
        Value::Object(op) => match_operator(op, leaf),
        literal => scalars_equal(literal, leaf),
    }
}

/// Equality of two JSON scalars; numbers compare numerically.
pub fn scalars_equal(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => match (x.as_f64(), y.as_f64()) {
            (Some(x), Some(y)) => x == y,
            _ => x == y,
        },
        _ => a == b,
    }
}

fn match_operator(op: &Map<String, Value>, leaf: &Value) -> bool {
    let s = leaf.as_str();
    if let Some(arg) = op.get("prefix") {
        return string_op(arg, s, |p, v| v.starts_with(p), |p, v| starts_with_ic(v, p));
    }
    if let Some(arg) = op.get("suffix") {
        return string_op(arg, s, |p, v| v.ends_with(p), |p, v| ends_with_ic(v, p));
    }
    if let Some(arg) = op.get("equals-ignore-case") {
        return match (arg.as_str(), s) {
            (Some(p), Some(v)) => equals_ic(p, v),
            _ => false,
        };
    }
    if let Some(arg) = op.get("wildcard") {
        return match (arg.as_str(), s) {
            (Some(p), Some(v)) => wildcard_matches(p, v),
            _ => false,
        };
    }
    if let Some(arg) = op.get("cidr") {
        return match (arg.as_str(), s) {
            (Some(c), Some(v)) => cidr_matches(c, v),
            _ => false,
        };
    }
    if let Some(arg) = op.get("numeric") {
        return numeric_matches(arg, leaf);
    }
    if let Some(arg) = op.get("anything-but") {
        return anything_but_matches(arg, leaf);
    }
    false
}

/// `prefix` / `suffix` take a string, or `{"equals-ignore-case": "..."}`.
fn string_op(
    arg: &Value,
    leaf: Option<&str>,
    exact: impl Fn(&str, &str) -> bool,
    ignore_case: impl Fn(&str, &str) -> bool,
) -> bool {
    let Some(v) = leaf else {
        return false;
    };
    match arg {
        Value::String(p) => exact(p, v),
        Value::Object(o) => match o.get("equals-ignore-case").and_then(Value::as_str) {
            Some(p) => ignore_case(p, v),
            None => false,
        },
        _ => false,
    }
}

fn equals_ic(a: &str, b: &str) -> bool {
    a.to_lowercase() == b.to_lowercase()
}

fn starts_with_ic(value: &str, prefix: &str) -> bool {
    value.to_lowercase().starts_with(&prefix.to_lowercase())
}

fn ends_with_ic(value: &str, suffix: &str) -> bool {
    value.to_lowercase().ends_with(&suffix.to_lowercase())
}

fn anything_but_matches(arg: &Value, leaf: &Value) -> bool {
    match arg {
        Value::String(_) | Value::Number(_) => !scalars_equal(arg, leaf),
        Value::Array(list) => !list.iter().any(|v| scalars_equal(v, leaf)),
        Value::Object(nested) => {
            let s = leaf.as_str();
            // Each nested matcher accepts one string or a list of strings.
            // A non-string leaf can never satisfy a string matcher, so it is
            // never excluded by one.
            let hit = |arg: &Value, pred: &dyn Fn(&str, &str) -> bool| -> Option<bool> {
                let v = s?;
                match arg {
                    Value::String(p) => Some(pred(p, v)),
                    Value::Array(a) => Some(a.iter().filter_map(Value::as_str).any(|p| pred(p, v))),
                    _ => None,
                }
            };
            let excluded = if let Some(p) = nested.get("prefix") {
                hit(p, &|p, v| v.starts_with(p))
            } else if let Some(p) = nested.get("suffix") {
                hit(p, &|p, v| v.ends_with(p))
            } else if let Some(p) = nested.get("equals-ignore-case") {
                hit(p, &|p, v| equals_ic(p, v))
            } else if let Some(p) = nested.get("wildcard") {
                hit(p, &|p, v| wildcard_matches(p, v))
            } else if let Some(p) = nested.get("cidr") {
                hit(p, &|p, v| cidr_matches(p, v))
            } else {
                // Unknown nested matcher: cannot evaluate, never deliver.
                return false;
            };
            match excluded {
                Some(hit) => !hit,
                // Non-string leaf against a string matcher: not excluded.
                None if s.is_none() => true,
                // Malformed matcher argument.
                None => false,
            }
        }
        _ => false,
    }
}

/// `{"numeric": [op, n, op, n]}`: every operator/value pair must hold. Only
/// JSON numbers match. Malformed matchers match nothing.
pub fn numeric_matches(arg: &Value, leaf: &Value) -> bool {
    let Some(actual) = leaf.as_f64() else {
        return false;
    };
    let Some(arr) = arg.as_array() else {
        return false;
    };
    if arr.is_empty() || arr.len() % 2 != 0 {
        return false;
    }
    arr.chunks(2).all(|pair| {
        let Some(threshold) = pair[1].as_f64() else {
            return false;
        };
        match pair[0].as_str() {
            Some("=") => actual == threshold,
            Some("<") => actual < threshold,
            Some("<=") => actual <= threshold,
            Some(">") => actual > threshold,
            Some(">=") => actual >= threshold,
            _ => false,
        }
    })
}

/// `wildcard` matcher: `*` matches any run of characters (including empty);
/// `\*` is a literal asterisk and `\\` a literal backslash.
pub fn wildcard_matches(pattern: &str, actual: &str) -> bool {
    let mut segments: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut chars = pattern.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(next) = chars.next() {
                current.push(next);
            }
        } else if c == '*' {
            segments.push(std::mem::take(&mut current));
        } else {
            current.push(c);
        }
    }
    segments.push(current);

    if segments.len() == 1 {
        return segments[0] == actual;
    }
    let first = &segments[0];
    if !actual.starts_with(first.as_str()) {
        return false;
    }
    let mut pos = first.len();
    let last_idx = segments.len() - 1;
    for (i, seg) in segments.iter().enumerate().skip(1) {
        if i == last_idx {
            return actual.len().saturating_sub(pos) >= seg.len() && actual.ends_with(seg.as_str());
        }
        match actual[pos..].find(seg.as_str()) {
            Some(idx) => pos += idx + seg.len(),
            None => return false,
        }
    }
    true
}

/// IPv4 / IPv6 CIDR membership (a bare address is a /32 or /128).
pub fn cidr_matches(cidr: &str, actual: &str) -> bool {
    let (net_str, prefix) = match cidr.split_once('/') {
        Some((n, p)) => match p.parse::<u32>() {
            Ok(p) => (n, Some(p)),
            Err(_) => return false,
        },
        None => (cidr, None),
    };
    let (Ok(net), Ok(value)) = (net_str.parse::<IpAddr>(), actual.parse::<IpAddr>()) else {
        return false;
    };
    match (net, value) {
        (IpAddr::V4(n), IpAddr::V4(v)) => {
            let prefix = prefix.unwrap_or(32);
            prefix <= 32 && masked_v4(n, prefix) == masked_v4(v, prefix)
        }
        (IpAddr::V6(n), IpAddr::V6(v)) => {
            let prefix = prefix.unwrap_or(128);
            prefix <= 128 && masked_v6(n, prefix) == masked_v6(v, prefix)
        }
        _ => false,
    }
}

fn masked_v4(addr: Ipv4Addr, prefix: u32) -> u32 {
    let bits = u32::from(addr);
    if prefix == 0 {
        0
    } else {
        bits & (u32::MAX << (32 - prefix))
    }
}

fn masked_v6(addr: Ipv6Addr, prefix: u32) -> u128 {
    let bits = u128::from(addr);
    if prefix == 0 {
        0
    } else {
        bits & (u128::MAX << (128 - prefix))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn object_pattern_matches_array_of_objects() {
        let p = json!({"detail": {"items": {"sku": ["b"]}}});
        assert!(matches(
            &p,
            &json!({"detail": {"items": [{"sku": "a"}, {"sku": "b"}]}})
        ));
        assert!(!matches(&p, &json!({"detail": {"items": [{"sku": "a"}]}})));
        // Nested arrays are crushed out too.
        assert!(matches(
            &p,
            &json!({"detail": {"items": [[{"sku": "x"}], [{"sku": "b"}]]}})
        ));
    }

    #[test]
    fn array_of_objects_requires_same_element() {
        let p = json!({"employees": {"first": ["Anna"], "last": ["Jones"]}});
        let ev = json!({"employees": [
            {"first": "John", "last": "Doe"},
            {"first": "Anna", "last": "Smith"},
            {"first": "Peter", "last": "Jones"}
        ]});
        assert!(!matches(&p, &ev));
        let p = json!({"employees": {"first": ["Anna"], "last": ["Smith"]}});
        assert!(matches(&p, &ev));
    }

    #[test]
    fn exists_true_matches_present_null() {
        let p = json!({"a": [{"exists": true}]});
        assert!(matches(&p, &json!({"a": null})));
        assert!(matches(&p, &json!({"a": "x"})));
        assert!(!matches(&p, &json!({})));
        // Intermediate (object) nodes are not leaves.
        assert!(!matches(&p, &json!({"a": {"b": 1}})));
    }

    #[test]
    fn exists_false_matches_absent_only() {
        let p = json!({"a": [{"exists": false}]});
        assert!(matches(&p, &json!({})));
        assert!(!matches(&p, &json!({"a": null})));
        assert!(!matches(&p, &json!({"a": 0})));
        assert!(matches(&p, &json!({"a": {"b": 1}})));
        // Under a missing parent the leaf is absent too.
        let p = json!({"x": {"a": [{"exists": false}]}});
        assert!(matches(&p, &json!({})));
    }

    #[test]
    fn null_pattern_does_not_match_absent_field() {
        let p = json!({"a": [null]});
        assert!(matches(&p, &json!({"a": null})));
        assert!(!matches(&p, &json!({})));
        assert!(!matches(&p, &json!({"a": "x"})));
    }

    #[test]
    fn anything_but_requires_presence() {
        for ab in [
            json!("x"),
            json!(["x"]),
            json!(5),
            json!({"prefix": "x"}),
            json!({"suffix": "x"}),
        ] {
            let p = json!({"a": [{"anything-but": ab}]});
            assert!(!matches(&p, &json!({})), "{p}");
            assert!(matches(&p, &json!({"a": "zzz"})), "{p}");
        }
        // Other matchers likewise need the field.
        assert!(!matches(&json!({"a": [{"prefix": ""}]}), &json!({})));
        assert!(!matches(&json!({"a": [{"numeric": [">", 0]}]}), &json!({})));
    }

    #[test]
    fn anything_but_numeric_compares_numerically() {
        let p = json!({"n": [{"anything-but": 100}]});
        assert!(!matches(&p, &json!({"n": 100.0})));
        assert!(!matches(&p, &json!({"n": 100})));
        assert!(matches(&p, &json!({"n": 101})));
        let p = json!({"n": [{"anything-but": [100, 200]}]});
        assert!(!matches(&p, &json!({"n": 200.0})));
        assert!(matches(&p, &json!({"n": 300})));
        // Exact numeric match is numeric too.
        assert!(matches(&json!({"n": [300]}), &json!({"n": 300.0})));
        assert!(matches(&json!({"n": [3.018e2]}), &json!({"n": 301.8})));
    }

    #[test]
    fn anything_but_on_array_matches_if_any_element_not_excluded() {
        let p = json!({"c": [{"anything-but": ["rugby", "tennis"]}]});
        assert!(matches(&p, &json!({"c": ["rugby", "baseball"]})));
        assert!(!matches(&p, &json!({"c": ["rugby"]})));
        assert!(!matches(&p, &json!({"c": ["rugby", "tennis"]})));
        let p = json!({"c": [{"anything-but": {"wildcard": "*ball"}}]});
        assert!(matches(&p, &json!({"c": ["hockey", "rugby"]})));
        assert!(!matches(&p, &json!({"c": ["baseball", "basketball"]})));
    }

    #[test]
    fn scalar_pattern_matches_any_array_element() {
        let p = json!({"tags": ["b"]});
        assert!(matches(&p, &json!({"tags": ["a", "b"]})));
        assert!(!matches(&p, &json!({"tags": ["a"]})));
        assert!(matches(&json!({"n": [2]}), &json!({"n": [1, 2]})));
        assert!(matches(
            &json!({"n": [{"numeric": [">", 5]}]}),
            &json!({"n": [1, 10]})
        ));
    }

    #[test]
    fn empty_array_field_is_absent() {
        assert!(matches(
            &json!({"a": [{"exists": false}]}),
            &json!({"a": []})
        ));
        assert!(!matches(
            &json!({"a": [{"exists": true}]}),
            &json!({"a": []})
        ));
    }

    #[test]
    fn structure_validation_and_legacy_normalization() {
        assert!(is_valid_structure(&json!({"a": ["x"], "b": {"c": [1]}})));
        assert!(is_valid_structure(
            &json!({"$or": [{"a": ["1"]}, {"b": ["2"]}]})
        ));
        assert!(!is_valid_structure(&json!({"a": "x"})));
        assert!(!is_valid_structure(&json!({"a": {"b": 1}})));
        assert!(!is_valid_structure(&json!(["x"])));
        let legacy = json!({"a": "x", "b": {"c": 1, "d": [2]}, "$or": [{"e": true}]});
        let norm = normalize_scalar_leaves(&legacy);
        assert_eq!(
            norm,
            json!({"a": ["x"], "b": {"c": [1], "d": [2]}, "$or": [{"e": [true]}]})
        );
        assert!(is_valid_structure(&norm));
        assert!(matches(
            &norm,
            &json!({"a": "x", "b": {"c": 1, "d": 2}, "e": true})
        ));
    }

    #[test]
    fn empty_intermediate_array_is_absent() {
        let p = json!({"detail": {"items": {"sku": [{"exists": false}]}}});
        assert!(matches(&p, &json!({"detail": {"items": []}})));
        assert!(matches(&p, &json!({"detail": {"items": [[]]}})));
        assert!(!matches(&p, &json!({"detail": {"items": [{"sku": "x"}]}})));
        let p = json!({"detail": {"items": {"sku": [{"exists": true}]}}});
        assert!(!matches(&p, &json!({"detail": {"items": []}})));
        // A field pattern under an empty array never matches a value.
        let p = json!({"detail": {"items": {"sku": ["x"]}}});
        assert!(!matches(&p, &json!({"detail": {"items": []}})));
    }

    #[test]
    fn or_is_anded_with_siblings() {
        let p = json!({"source": ["s"], "$or": [{"a": ["1"]}, {"b": ["2"]}]});
        assert!(matches(&p, &json!({"source": "s", "b": "2"})));
        assert!(!matches(&p, &json!({"source": "t", "b": "2"})));
        assert!(!matches(&p, &json!({"source": "s", "c": "2"})));
    }

    #[test]
    fn string_operators() {
        assert!(matches(
            &json!({"a": [{"prefix": "pre"}]}),
            &json!({"a": "prefix"})
        ));
        assert!(matches(
            &json!({"a": [{"prefix": {"equals-ignore-case": "PRE"}}]}),
            &json!({"a": "prefix"})
        ));
        assert!(matches(
            &json!({"a": [{"suffix": {"equals-ignore-case": ".PNG"}}]}),
            &json!({"a": "x.png"})
        ));
        assert!(matches(
            &json!({"a": [{"equals-ignore-case": "ABC"}]}),
            &json!({"a": "abc"})
        ));
        assert!(matches(
            &json!({"a": [{"wildcard": "dir/*.png"}]}),
            &json!({"a": "dir/x.png"})
        ));
        assert!(!matches(
            &json!({"a": [{"wildcard": "dir/*.png"}]}),
            &json!({"a": "dir/x.jpg"})
        ));
    }

    #[test]
    fn cidr_v4_and_v6() {
        assert!(cidr_matches("10.0.0.0/24", "10.0.0.255"));
        assert!(!cidr_matches("10.0.0.0/24", "10.0.1.0"));
        assert!(cidr_matches("2001:db8::/32", "2001:db8:1::1"));
        assert!(!cidr_matches("2001:db8::/32", "2001:db9::1"));
        assert!(!cidr_matches("10.0.0.0/8", "2001:db8::1"));
        assert!(cidr_matches("0.0.0.0/0", "1.2.3.4"));
    }

    #[test]
    fn numeric_matcher() {
        let p = json!({"n": [{"numeric": [">", 0, "<=", 5]}]});
        assert!(matches(&p, &json!({"n": 5})));
        assert!(!matches(&p, &json!({"n": 0})));
        assert!(!matches(&p, &json!({"n": "3"})));
        assert!(!numeric_matches(&json!([]), &json!(1)));
        assert!(!numeric_matches(&json!([">"]), &json!(1)));
    }
}
