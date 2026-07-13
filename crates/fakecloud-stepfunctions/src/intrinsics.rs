//! Amazon States Language intrinsic functions.
//!
//! These appear in Parameters / ResultSelector / Arguments / Output
//! values when the JSON key uses the `.$` suffix and the value is a
//! string starting with `States.`. This module parses the call,
//! resolves arguments (JSONPath references vs JSON literals), and
//! returns the computed value.
//!
//! Reference: https://docs.aws.amazon.com/step-functions/latest/dg/intrinsic-functions.html

use base64::Engine;
use serde_json::{json, Value};

use crate::io_processing::resolve_path;

#[derive(Debug, Clone)]
pub struct IntrinsicError(pub String);

impl std::fmt::Display for IntrinsicError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "States.IntrinsicFailure: {}", self.0)
    }
}

/// Returns true if `value` is a string that should be evaluated as an
/// intrinsic (`States.Foo(...)`) rather than a JSONPath reference.
pub fn is_intrinsic_call(value: &str) -> bool {
    value.starts_with("States.") && value.contains('(')
}

/// Evaluate an ASL intrinsic call against `input`. Returns the
/// computed value or an error string suitable for surfacing as
/// `States.IntrinsicFailure`.
pub fn evaluate(call: &str, input: &Value) -> Result<Value, IntrinsicError> {
    let (name, args_str) = split_call(call)?;
    let args = parse_args(args_str, input)?;
    match name {
        "States.Format" => fn_format(&args),
        "States.JsonToString" => fn_json_to_string(&args),
        "States.StringToJson" => fn_string_to_json(&args),
        "States.Array" => Ok(Value::Array(args)),
        "States.ArrayPartition" => fn_array_partition(&args),
        "States.ArrayContains" => fn_array_contains(&args),
        "States.ArrayRange" => fn_array_range(&args),
        "States.ArrayGetItem" => fn_array_get_item(&args),
        "States.ArrayLength" => fn_array_length(&args),
        "States.ArrayUnique" => fn_array_unique(&args),
        "States.Base64Encode" => fn_base64_encode(&args),
        "States.Base64Decode" => fn_base64_decode(&args),
        "States.Hash" => fn_hash(&args),
        "States.JsonMerge" => fn_json_merge(&args),
        "States.MathRandom" => fn_math_random(&args),
        "States.MathAdd" => fn_math_add(&args),
        "States.UUID" => fn_uuid(&args),
        "States.StringSplit" => fn_string_split(&args),
        other => Err(IntrinsicError(format!("unknown intrinsic '{other}'"))),
    }
}

fn split_call(call: &str) -> Result<(&str, &str), IntrinsicError> {
    let open = call
        .find('(')
        .ok_or_else(|| IntrinsicError(format!("missing '(' in '{call}'")))?;
    if !call.ends_with(')') {
        return Err(IntrinsicError(format!("missing ')' in '{call}'")));
    }
    let name = &call[..open];
    let args_str = &call[open + 1..call.len() - 1];
    Ok((name, args_str))
}

fn parse_args(args_str: &str, input: &Value) -> Result<Vec<Value>, IntrinsicError> {
    let mut out = Vec::new();
    if args_str.trim().is_empty() {
        return Ok(out);
    }
    for raw in split_top_level_commas(args_str) {
        let arg = raw.trim();
        if arg.is_empty() {
            continue;
        }
        out.push(parse_arg(arg, input)?);
    }
    Ok(out)
}

/// Split a comma-separated argument list, ignoring commas that fall
/// inside quoted strings (`'...'` or `"..."`). Backslash escapes
/// inside single-quoted strings are honoured.
fn split_top_level_commas(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut in_single = false;
    let mut in_double = false;
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' if in_single => {
                if let Some(&next) = chars.peek() {
                    current.push('\\');
                    current.push(next);
                    chars.next();
                }
            }
            '\'' if !in_double => {
                in_single = !in_single;
                current.push(c);
            }
            '"' if !in_single => {
                in_double = !in_double;
                current.push(c);
            }
            ',' if !in_single && !in_double => {
                out.push(current.clone());
                current.clear();
            }
            _ => current.push(c),
        }
    }
    if !current.is_empty() || s.ends_with(',') {
        out.push(current);
    }
    out
}

fn parse_arg(arg: &str, input: &Value) -> Result<Value, IntrinsicError> {
    if arg.starts_with('$') {
        Ok(resolve_path(input, arg))
    } else if arg.starts_with('\'') && arg.ends_with('\'') && arg.len() >= 2 {
        // Single-quoted string literal with backslash escapes.
        let inner = &arg[1..arg.len() - 1];
        Ok(Value::String(unescape_single_quoted(inner)))
    } else {
        // Try JSON literal (number, bool, null, double-quoted string,
        // object/array). Fall back to bare string.
        serde_json::from_str(arg)
            .map_err(|e| IntrinsicError(format!("invalid argument '{arg}': {e}")))
    }
}

fn unescape_single_quoted(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('\\') => out.push('\\'),
                Some('\'') => out.push('\''),
                Some('n') => out.push('\n'),
                Some('t') => out.push('\t'),
                Some('{') => out.push('{'),
                Some('}') => out.push('}'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn arg_as_str(v: &Value) -> Result<String, IntrinsicError> {
    match v {
        Value::String(s) => Ok(s.clone()),
        other => Ok(serde_json::to_string(other).unwrap_or_default()),
    }
}

fn arg_as_array(v: &Value) -> Result<&Vec<Value>, IntrinsicError> {
    v.as_array()
        .ok_or_else(|| IntrinsicError(format!("expected array, got {v}")))
}

fn arg_as_i64(v: &Value) -> Result<i64, IntrinsicError> {
    v.as_i64()
        .or_else(|| v.as_f64().map(|f| f as i64))
        .ok_or_else(|| IntrinsicError(format!("expected integer, got {v}")))
}

fn arg_as_f64(v: &Value) -> Result<f64, IntrinsicError> {
    v.as_f64()
        .ok_or_else(|| IntrinsicError(format!("expected number, got {v}")))
}

fn need_args(args: &[Value], expected: usize, name: &str) -> Result<(), IntrinsicError> {
    if args.len() != expected {
        Err(IntrinsicError(format!(
            "{name} expected {expected} args, got {}",
            args.len()
        )))
    } else {
        Ok(())
    }
}

fn fn_format(args: &[Value]) -> Result<Value, IntrinsicError> {
    if args.is_empty() {
        return Err(IntrinsicError(
            "States.Format requires at least one argument".into(),
        ));
    }
    let template = args[0]
        .as_str()
        .ok_or_else(|| IntrinsicError("States.Format template must be a string".into()))?;
    let mut out = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();
    let mut idx = 1;
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                if let Some(&n) = chars.peek() {
                    out.push(n);
                    chars.next();
                }
            }
            '{' if matches!(chars.peek(), Some('}')) => {
                chars.next();
                let v = args.get(idx).ok_or_else(|| {
                    IntrinsicError("States.Format placeholder count exceeds args".into())
                })?;
                idx += 1;
                match v {
                    Value::String(s) => out.push_str(s),
                    Value::Null => out.push_str("null"),
                    other => out.push_str(&serde_json::to_string(other).unwrap_or_default()),
                }
            }
            _ => out.push(c),
        }
    }
    Ok(Value::String(out))
}

fn fn_json_to_string(args: &[Value]) -> Result<Value, IntrinsicError> {
    need_args(args, 1, "States.JsonToString")?;
    Ok(Value::String(
        serde_json::to_string(&args[0]).unwrap_or_default(),
    ))
}

fn fn_string_to_json(args: &[Value]) -> Result<Value, IntrinsicError> {
    need_args(args, 1, "States.StringToJson")?;
    let s = args[0]
        .as_str()
        .ok_or_else(|| IntrinsicError("States.StringToJson arg must be a string".into()))?;
    serde_json::from_str(s)
        .map_err(|e| IntrinsicError(format!("States.StringToJson parse failed: {e}")))
}

fn fn_array_partition(args: &[Value]) -> Result<Value, IntrinsicError> {
    need_args(args, 2, "States.ArrayPartition")?;
    let arr = arg_as_array(&args[0])?;
    let chunk = arg_as_i64(&args[1])?;
    if chunk <= 0 {
        return Err(IntrinsicError(
            "ArrayPartition chunk size must be > 0".into(),
        ));
    }
    let chunk = chunk as usize;
    let mut out: Vec<Value> = Vec::new();
    for slice in arr.chunks(chunk) {
        out.push(Value::Array(slice.to_vec()));
    }
    Ok(Value::Array(out))
}

fn fn_array_contains(args: &[Value]) -> Result<Value, IntrinsicError> {
    need_args(args, 2, "States.ArrayContains")?;
    let arr = arg_as_array(&args[0])?;
    Ok(Value::Bool(arr.iter().any(|v| v == &args[1])))
}

fn fn_array_range(args: &[Value]) -> Result<Value, IntrinsicError> {
    need_args(args, 3, "States.ArrayRange")?;
    let start = arg_as_i64(&args[0])?;
    let end = arg_as_i64(&args[1])?;
    let step = arg_as_i64(&args[2])?;
    if step == 0 {
        return Err(IntrinsicError("ArrayRange step must be != 0".into()));
    }
    let mut out = Vec::new();
    let mut i = start;
    if step > 0 {
        while i <= end {
            out.push(json!(i));
            i += step;
        }
    } else {
        while i >= end {
            out.push(json!(i));
            i += step;
        }
    }
    Ok(Value::Array(out))
}

fn fn_array_get_item(args: &[Value]) -> Result<Value, IntrinsicError> {
    need_args(args, 2, "States.ArrayGetItem")?;
    let arr = arg_as_array(&args[0])?;
    let idx = arg_as_i64(&args[1])?;
    if idx < 0 {
        return Err(IntrinsicError("ArrayGetItem index must be >= 0".into()));
    }
    Ok(arr.get(idx as usize).cloned().unwrap_or(Value::Null))
}

fn fn_array_length(args: &[Value]) -> Result<Value, IntrinsicError> {
    need_args(args, 1, "States.ArrayLength")?;
    let arr = arg_as_array(&args[0])?;
    Ok(json!(arr.len()))
}

fn fn_array_unique(args: &[Value]) -> Result<Value, IntrinsicError> {
    need_args(args, 1, "States.ArrayUnique")?;
    let arr = arg_as_array(&args[0])?;
    let mut seen: Vec<Value> = Vec::new();
    for v in arr {
        if !seen.contains(v) {
            seen.push(v.clone());
        }
    }
    Ok(Value::Array(seen))
}

fn fn_base64_encode(args: &[Value]) -> Result<Value, IntrinsicError> {
    need_args(args, 1, "States.Base64Encode")?;
    let s = arg_as_str(&args[0])?;
    Ok(Value::String(
        base64::engine::general_purpose::STANDARD.encode(s.as_bytes()),
    ))
}

fn fn_base64_decode(args: &[Value]) -> Result<Value, IntrinsicError> {
    need_args(args, 1, "States.Base64Decode")?;
    let s = arg_as_str(&args[0])?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(s.as_bytes())
        .map_err(|e| IntrinsicError(format!("Base64Decode failed: {e}")))?;
    let decoded = String::from_utf8(bytes)
        .map_err(|e| IntrinsicError(format!("Base64Decode utf8 failed: {e}")))?;
    Ok(Value::String(decoded))
}

fn fn_hash(args: &[Value]) -> Result<Value, IntrinsicError> {
    use md5::Digest;
    need_args(args, 2, "States.Hash")?;
    let input = arg_as_str(&args[0])?;
    let algo = arg_as_str(&args[1])?;
    let digest_hex = match algo.as_str() {
        "MD5" => {
            let mut h = md5::Md5::new();
            h.update(input.as_bytes());
            hex::encode(h.finalize())
        }
        "SHA-1" => {
            let mut h = sha1::Sha1::new();
            h.update(input.as_bytes());
            hex::encode(h.finalize())
        }
        "SHA-256" => {
            let mut h = sha2::Sha256::new();
            h.update(input.as_bytes());
            hex::encode(h.finalize())
        }
        "SHA-384" => {
            let mut h = sha2::Sha384::new();
            h.update(input.as_bytes());
            hex::encode(h.finalize())
        }
        "SHA-512" => {
            let mut h = sha2::Sha512::new();
            h.update(input.as_bytes());
            hex::encode(h.finalize())
        }
        other => {
            return Err(IntrinsicError(format!(
                "unsupported hash algorithm '{other}'"
            )))
        }
    };
    Ok(Value::String(digest_hex))
}

fn fn_json_merge(args: &[Value]) -> Result<Value, IntrinsicError> {
    need_args(args, 3, "States.JsonMerge")?;
    let a = args[0]
        .as_object()
        .ok_or_else(|| IntrinsicError("JsonMerge arg 1 must be object".into()))?;
    let b = args[1]
        .as_object()
        .ok_or_else(|| IntrinsicError("JsonMerge arg 2 must be object".into()))?;
    let deep = args[2]
        .as_bool()
        .ok_or_else(|| IntrinsicError("JsonMerge arg 3 must be bool".into()))?;
    let mut merged = a.clone();
    if deep {
        deep_merge(&mut merged, b);
    } else {
        for (k, v) in b {
            merged.insert(k.clone(), v.clone());
        }
    }
    Ok(Value::Object(merged))
}

fn deep_merge(a: &mut serde_json::Map<String, Value>, b: &serde_json::Map<String, Value>) {
    for (k, v) in b {
        match (a.get_mut(k), v) {
            (Some(Value::Object(am)), Value::Object(bm)) => deep_merge(am, bm),
            _ => {
                a.insert(k.clone(), v.clone());
            }
        }
    }
}

fn fn_math_random(args: &[Value]) -> Result<Value, IntrinsicError> {
    use rand::Rng;
    if args.len() < 2 || args.len() > 3 {
        return Err(IntrinsicError(
            "States.MathRandom expected 2 or 3 args".into(),
        ));
    }
    let start = arg_as_i64(&args[0])?;
    let end = arg_as_i64(&args[1])?;
    if end <= start {
        return Err(IntrinsicError("MathRandom end must be > start".into()));
    }
    // 3rd arg is an optional seed; we honour it for deterministic tests.
    let v: i64 = if let Some(seed_v) = args.get(2) {
        use rand::SeedableRng;
        let seed = arg_as_i64(seed_v)? as u64;
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
        rng.gen_range(start..end)
    } else {
        rand::thread_rng().gen_range(start..end)
    };
    Ok(json!(v))
}

fn fn_math_add(args: &[Value]) -> Result<Value, IntrinsicError> {
    need_args(args, 2, "States.MathAdd")?;
    // Integer operands add with 64-bit integer semantics, but a genuine
    // overflow errors instead of panicking (debug) or silently wrapping
    // (release).
    if let (Some(a), Some(b)) = (args[0].as_i64(), args[1].as_i64()) {
        return match a.checked_add(b) {
            Some(sum) => Ok(json!(sum)),
            None => Err(IntrinsicError(
                "States.MathAdd result overflows a 64-bit integer".into(),
            )),
        };
    }
    // Otherwise fall back to floating-point addition. This covers fractional
    // operands (which the old i64 coercion truncated) and integers too large
    // for i64. AWS treats numbers as given, so no truncation is applied.
    let a = arg_as_f64(&args[0])?;
    let b = arg_as_f64(&args[1])?;
    Ok(json!(a + b))
}

fn fn_uuid(args: &[Value]) -> Result<Value, IntrinsicError> {
    need_args(args, 0, "States.UUID")?;
    Ok(Value::String(uuid::Uuid::new_v4().to_string()))
}

fn fn_string_split(args: &[Value]) -> Result<Value, IntrinsicError> {
    need_args(args, 2, "States.StringSplit")?;
    let s = arg_as_str(&args[0])?;
    let splitter = arg_as_str(&args[1])?;
    if splitter.is_empty() {
        return Err(IntrinsicError(
            "StringSplit delimiter must be non-empty".into(),
        ));
    }
    // ASL StringSplit treats every char in the delimiter as a possible
    // separator (eg. delimiter "., " splits on either dot, comma, or
    // space) and drops empty tokens.
    let chars: Vec<char> = splitter.chars().collect();
    let parts: Vec<Value> = s
        .split(|c: char| chars.contains(&c))
        .filter(|p| !p.is_empty())
        .map(|p| Value::String(p.to_string()))
        .collect();
    Ok(Value::Array(parts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn format_substitutes_placeholders() {
        let out = evaluate("States.Format('Hello, {}!', 'Alice')", &Value::Null).unwrap();
        assert_eq!(out, json!("Hello, Alice!"));
    }

    #[test]
    fn format_resolves_jsonpath_args() {
        let input = json!({"name": "Bob", "n": 3});
        let out = evaluate("States.Format('{}={}', $.name, $.n)", &input).unwrap();
        assert_eq!(out, json!("Bob=3"));
    }

    #[test]
    fn array_intrinsics() {
        assert_eq!(
            evaluate("States.Array(1, 2, 3)", &Value::Null).unwrap(),
            json!([1, 2, 3])
        );
        assert_eq!(
            evaluate("States.ArrayLength($)", &json!([10, 20, 30])).unwrap(),
            json!(3)
        );
        assert_eq!(
            evaluate("States.ArrayContains($, 2)", &json!([1, 2, 3])).unwrap(),
            json!(true)
        );
        assert_eq!(
            evaluate("States.ArrayContains($, 9)", &json!([1, 2, 3])).unwrap(),
            json!(false)
        );
        assert_eq!(
            evaluate("States.ArrayRange(1, 9, 2)", &Value::Null).unwrap(),
            json!([1, 3, 5, 7, 9])
        );
        assert_eq!(
            evaluate("States.ArrayPartition($, 2)", &json!([1, 2, 3, 4, 5])).unwrap(),
            json!([[1, 2], [3, 4], [5]])
        );
        assert_eq!(
            evaluate("States.ArrayGetItem($, 1)", &json!(["a", "b", "c"])).unwrap(),
            json!("b")
        );
        assert_eq!(
            evaluate("States.ArrayUnique($)", &json!([1, 2, 1, 3, 2])).unwrap(),
            json!([1, 2, 3])
        );
    }

    #[test]
    fn json_intrinsics() {
        assert_eq!(
            evaluate("States.JsonToString($)", &json!({"x": 1})).unwrap(),
            json!(r#"{"x":1}"#)
        );
        assert_eq!(
            evaluate("States.StringToJson($)", &json!(r#"{"x":1}"#)).unwrap(),
            json!({"x": 1})
        );
        assert_eq!(
            evaluate(
                "States.JsonMerge($.a, $.b, false)",
                &json!({"a": {"x": 1, "y": 2}, "b": {"y": 9, "z": 3}})
            )
            .unwrap(),
            json!({"x": 1, "y": 9, "z": 3})
        );
    }

    #[test]
    fn base64_intrinsics() {
        let enc = evaluate("States.Base64Encode('hello')", &Value::Null).unwrap();
        assert_eq!(enc, json!("aGVsbG8="));
        let dec = evaluate("States.Base64Decode('aGVsbG8=')", &Value::Null).unwrap();
        assert_eq!(dec, json!("hello"));
    }

    #[test]
    fn hash_intrinsic() {
        let out = evaluate("States.Hash('hello', 'SHA-256')", &Value::Null).unwrap();
        assert_eq!(
            out,
            json!("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824")
        );
    }

    #[test]
    fn math_intrinsics() {
        assert_eq!(
            evaluate("States.MathAdd(2, 3)", &Value::Null).unwrap(),
            json!(5)
        );
        let r = evaluate("States.MathRandom(0, 10)", &Value::Null).unwrap();
        let n = r.as_i64().unwrap();
        assert!((0..10).contains(&n));
    }

    // L6: MathAdd must not panic (debug) or wrap (release) on i64 overflow, and
    // must not truncate fractional operands.
    #[test]
    fn math_add_overflow_and_floats() {
        // i64::MAX + 1 overflows → error rather than panic/wrap.
        let expr = format!("States.MathAdd({}, 1)", i64::MAX);
        assert!(evaluate(&expr, &Value::Null).is_err());

        // Fractional operands are preserved, not truncated to int.
        assert_eq!(
            fn_math_add(&[json!(1.5), json!(2.25)]).unwrap(),
            json!(3.75)
        );
        // Mixed int + float stays a float.
        assert_eq!(fn_math_add(&[json!(2), json!(0.5)]).unwrap(), json!(2.5));
        // Negative integers still work.
        assert_eq!(fn_math_add(&[json!(-4), json!(1)]).unwrap(), json!(-3));
    }

    #[test]
    fn uuid_intrinsic_is_v4() {
        let out = evaluate("States.UUID()", &Value::Null).unwrap();
        let s = out.as_str().unwrap();
        // 8-4-4-4-12 = 36 chars total
        assert_eq!(s.len(), 36);
        assert_eq!(s.chars().nth(14).unwrap(), '4');
    }

    #[test]
    fn string_split_intrinsic() {
        assert_eq!(
            evaluate("States.StringSplit('a,b,c', ',')", &Value::Null).unwrap(),
            json!(["a", "b", "c"])
        );
        // Multi-char delimiter splits on any contained char and drops
        // empties.
        assert_eq!(
            evaluate("States.StringSplit('a,b c', ', ')", &Value::Null).unwrap(),
            json!(["a", "b", "c"])
        );
    }

    #[test]
    fn detects_intrinsic_call() {
        assert!(is_intrinsic_call("States.UUID()"));
        assert!(is_intrinsic_call("States.Format('{}', $.x)"));
        assert!(!is_intrinsic_call("$.foo.bar"));
        assert!(!is_intrinsic_call("States.IntrinsicFailure"));
    }

    #[test]
    fn unknown_intrinsic_errors() {
        let err = evaluate("States.NoSuchFunction()", &Value::Null).unwrap_err();
        assert!(format!("{err}").contains("unknown"));
    }
}
