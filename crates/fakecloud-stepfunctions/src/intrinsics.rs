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

/// Maximum nesting depth of intrinsic calls within one field. AWS: "You can
/// nest up to 10 intrinsic functions within a field."
const MAX_NESTING: usize = 10;

/// Failure evaluating an intrinsic expression.
#[derive(Debug, Clone)]
pub enum CallError {
    /// The function itself failed (bad arguments, unknown function, ...):
    /// surfaced as `States.IntrinsicFailure`.
    Intrinsic(IntrinsicError),
    /// A JSONPath argument matched nothing: surfaced as `States.Runtime`, as
    /// for every other path that matches nothing.
    Path(String),
}

impl CallError {
    /// The `(error, cause)` pair the state fails with.
    pub fn into_states_error(self) -> (String, String) {
        match self {
            CallError::Intrinsic(e) => ("States.IntrinsicFailure".to_string(), e.0),
            CallError::Path(cause) => ("States.Runtime".to_string(), cause),
        }
    }
}

impl From<IntrinsicError> for CallError {
    fn from(e: IntrinsicError) -> Self {
        CallError::Intrinsic(e)
    }
}

/// Evaluate an ASL intrinsic call against `input`. Returns the
/// computed value or an error string suitable for surfacing as
/// `States.IntrinsicFailure`.
pub fn evaluate(call: &str, input: &Value) -> Result<Value, IntrinsicError> {
    evaluate_with_context(call, input, None).map_err(|e| match e {
        CallError::Intrinsic(e) => e,
        CallError::Path(cause) => IntrinsicError(cause),
    })
}

/// Evaluate an intrinsic call whose `$$` arguments read `context` (the
/// Step Functions context object) when one is supplied.
pub fn evaluate_with_context(
    call: &str,
    input: &Value,
    context: Option<&Value>,
) -> Result<Value, CallError> {
    let mut parser = CallParser {
        chars: call.trim().chars().collect(),
        pos: 0,
    };
    let expr = parser.call(1)?;
    parser.skip_ws();
    if parser.pos != parser.chars.len() {
        return Err(IntrinsicError(format!("unexpected trailing input in '{call}'")).into());
    }
    eval_expr(&expr, input, context)
}

/// A parsed intrinsic argument.
#[derive(Debug, Clone)]
enum Expr {
    Call {
        name: String,
        args: Vec<Expr>,
    },
    /// A single-quoted string literal: its unescaped value, and the raw text
    /// between the quotes (States.Format needs the escapes to tell `\{\}`
    /// apart from a `{}` placeholder).
    Str {
        value: String,
        raw: String,
    },
    Path(String),
    Literal(Value),
}

fn eval_expr(expr: &Expr, input: &Value, context: Option<&Value>) -> Result<Value, CallError> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Str { value, .. } => Ok(Value::String(value.clone())),
        Expr::Path(path) => crate::io_processing::resolve_with_context(input, context, path)
            .map_err(|e| CallError::Path(crate::io_processing::path_error_cause(path, &e))),
        Expr::Call { name, args } => {
            let values = args
                .iter()
                .map(|a| eval_expr(a, input, context))
                .collect::<Result<Vec<_>, _>>()?;
            if name == "States.Format" {
                let raw = match args.first() {
                    Some(Expr::Str { raw, .. }) => Some(raw.as_str()),
                    _ => None,
                };
                return Ok(fn_format(raw, &values)?);
            }
            Ok(call_function(name, &values)?)
        }
    }
}

fn call_function(name: &str, args: &[Value]) -> Result<Value, IntrinsicError> {
    match name {
        "States.JsonToString" => fn_json_to_string(args),
        "States.StringToJson" => fn_string_to_json(args),
        "States.Array" => Ok(Value::Array(args.to_vec())),
        "States.ArrayPartition" => fn_array_partition(args),
        "States.ArrayContains" => fn_array_contains(args),
        "States.ArrayRange" => fn_array_range(args),
        "States.ArrayGetItem" => fn_array_get_item(args),
        "States.ArrayLength" => fn_array_length(args),
        "States.ArrayUnique" => fn_array_unique(args),
        "States.Base64Encode" => fn_base64_encode(args),
        "States.Base64Decode" => fn_base64_decode(args),
        "States.Hash" => fn_hash(args),
        "States.JsonMerge" => fn_json_merge(args),
        "States.MathRandom" => fn_math_random(args),
        "States.MathAdd" => fn_math_add(args),
        "States.UUID" => fn_uuid(args),
        "States.StringSplit" => fn_string_split(args),
        other => Err(IntrinsicError(format!("unknown intrinsic '{other}'"))),
    }
}

/// Recursive-descent parser for `States.Fn(arg, ...)` expressions. Arguments
/// are nested calls, single-quoted strings (escapes `\'`, `\{`, `\}`, `\\`),
/// JSONPath references (`$...` / `$$...`, which may themselves contain
/// brackets, parentheses, commas and quotes inside filters) or JSON literals
/// (numbers, `true`, `false`, `null`).
struct CallParser {
    chars: Vec<char>,
    pos: usize,
}

impl CallParser {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn skip_ws(&mut self) {
        while self.peek().is_some_and(char::is_whitespace) {
            self.pos += 1;
        }
    }

    fn rest_starts_with(&self, s: &str) -> bool {
        let n = s.chars().count();
        self.chars
            .get(self.pos..self.pos + n)
            .is_some_and(|w| w.iter().copied().eq(s.chars()))
    }

    fn text(&self) -> String {
        self.chars.iter().collect()
    }

    fn err<T>(&self, msg: &str) -> Result<T, IntrinsicError> {
        Err(IntrinsicError(format!(
            "{msg} at position {} in '{}'",
            self.pos,
            self.text()
        )))
    }

    fn call(&mut self, depth: usize) -> Result<Expr, IntrinsicError> {
        if depth > MAX_NESTING {
            return Err(IntrinsicError(format!(
                "intrinsic functions can be nested at most {MAX_NESTING} levels deep in '{}'",
                self.text()
            )));
        }
        self.skip_ws();
        if !self.rest_starts_with("States.") {
            return self.err("expected an intrinsic function");
        }
        let start = self.pos;
        while self
            .peek()
            .is_some_and(|c| c.is_ascii_alphanumeric() || c == '.')
        {
            self.pos += 1;
        }
        let name: String = self.chars[start..self.pos].iter().collect();
        self.skip_ws();
        if self.peek() != Some('(') {
            return Err(IntrinsicError(format!("missing '(' in '{}'", self.text())));
        }
        self.pos += 1;
        let mut args = Vec::new();
        self.skip_ws();
        if self.peek() == Some(')') {
            self.pos += 1;
            return Ok(Expr::Call { name, args });
        }
        loop {
            args.push(self.arg(depth)?);
            self.skip_ws();
            match self.peek() {
                Some(',') => self.pos += 1,
                Some(')') => {
                    self.pos += 1;
                    return Ok(Expr::Call { name, args });
                }
                None => return Err(IntrinsicError(format!("missing ')' in '{}'", self.text()))),
                Some(_) => return self.err("expected ',' or ')'"),
            }
        }
    }

    fn arg(&mut self, depth: usize) -> Result<Expr, IntrinsicError> {
        self.skip_ws();
        match self.peek() {
            Some('S') if self.rest_starts_with("States.") => self.call(depth + 1),
            Some('\'') => self.string(),
            Some('$') => self.path(),
            Some(_) => self.literal(),
            None => self.err("expected an argument"),
        }
    }

    fn string(&mut self) -> Result<Expr, IntrinsicError> {
        self.pos += 1; // opening quote
        let mut value = String::new();
        let mut raw = String::new();
        loop {
            match self.peek() {
                None => return self.err("unterminated string literal"),
                Some('\\') => {
                    let Some(next) = self.chars.get(self.pos + 1).copied() else {
                        return self.err("unterminated escape sequence");
                    };
                    raw.push('\\');
                    raw.push(next);
                    value.push_str(&unescape_char(next));
                    self.pos += 2;
                }
                Some('\'') => {
                    self.pos += 1;
                    return Ok(Expr::Str { value, raw });
                }
                Some(c) => {
                    value.push(c);
                    raw.push(c);
                    self.pos += 1;
                }
            }
        }
    }

    /// A JSONPath runs to the next top-level `,` or `)`; brackets,
    /// parentheses and quotes inside it (filters, quoted names) are skipped.
    fn path(&mut self) -> Result<Expr, IntrinsicError> {
        let start = self.pos;
        let mut depth = 0usize;
        let mut quote: Option<char> = None;
        while let Some(c) = self.peek() {
            if let Some(q) = quote {
                if c == '\\' {
                    self.pos += 1;
                } else if c == q {
                    quote = None;
                }
            } else {
                match c {
                    '\'' | '"' if depth > 0 => quote = Some(c),
                    '[' | '(' => depth += 1,
                    ']' | ')' if depth > 0 => depth -= 1,
                    ',' | ')' if depth == 0 => break,
                    _ => {}
                }
            }
            self.pos += 1;
        }
        let path: String = self.chars[start..self.pos.min(self.chars.len())]
            .iter()
            .collect();
        Ok(Expr::Path(path.trim_end().to_string()))
    }

    fn literal(&mut self) -> Result<Expr, IntrinsicError> {
        let start = self.pos;
        while self.peek().is_some_and(|c| c != ',' && c != ')') {
            self.pos += 1;
        }
        let raw: String = self.chars[start..self.pos].iter().collect();
        let raw = raw.trim();
        serde_json::from_str::<Value>(raw)
            .map(Expr::Literal)
            .map_err(|e| IntrinsicError(format!("invalid argument '{raw}': {e}")))
    }
}

/// Value of the character following a backslash in a string literal.
fn unescape_char(c: char) -> String {
    match c {
        '\\' | '\'' | '{' | '}' => c.to_string(),
        'n' => "\n".to_string(),
        't' => "\t".to_string(),
        other => format!("\\{other}"),
    }
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

/// `States.Format(template, args...)`. Each `{}` in the template is
/// replaced by the next argument. In a string-literal template, `\{` and `\}`
/// are literal braces (so `'\{\}'` is not a placeholder); `raw` carries the
/// literal's escaped text for that. A template from a path or nested call is
/// used as-is.
fn fn_format(raw: Option<&str>, args: &[Value]) -> Result<Value, IntrinsicError> {
    if args.is_empty() {
        return Err(IntrinsicError(
            "States.Format requires at least one argument".into(),
        ));
    }
    let template = match raw {
        Some(raw) => raw.to_string(),
        None => args[0]
            .as_str()
            .ok_or_else(|| IntrinsicError("States.Format template must be a string".into()))?
            .to_string(),
    };
    let honour_escapes = raw.is_some();
    let mut out = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();
    let mut idx = 1;
    while let Some(c) = chars.next() {
        match c {
            '\\' if honour_escapes => {
                if let Some(n) = chars.next() {
                    out.push_str(&unescape_char(n));
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

    #[test]
    fn nested_intrinsics() {
        let input = json!({"s": "a,b,c", "x": 7});
        assert_eq!(
            evaluate(
                "States.ArrayGetItem(States.StringSplit($.s, ','), 1)",
                &input
            )
            .unwrap(),
            json!("b")
        );
        assert_eq!(
            evaluate(
                r#"States.StringToJson(States.Format('\{"a":\{\}, "n": {}\}', $.x))"#,
                &input
            )
            .unwrap(),
            json!({"a": {}, "n": 7})
        );
        assert_eq!(
            evaluate(
                "States.ArrayLength(States.Array(States.MathAdd($.x, 1), 'p,q', States.Array()))",
                &input
            )
            .unwrap(),
            json!(3)
        );
    }

    #[test]
    fn nesting_limit_is_ten() {
        let nest = |n: usize| {
            let mut e = "States.Array()".to_string();
            for _ in 1..n {
                e = format!("States.Array({e})");
            }
            e
        };
        assert!(evaluate(&nest(10), &Value::Null).is_ok());
        let err = evaluate(&nest(11), &Value::Null).unwrap_err();
        assert!(format!("{err}").contains("nested"), "{err}");
    }

    #[test]
    fn string_literal_escapes() {
        assert_eq!(
            evaluate(r"States.Format('it\'s \\ a \{\} {}', 'x')", &Value::Null).unwrap(),
            json!(r"it's \ a {} x")
        );
        // Commas and parens inside strings never split arguments.
        assert_eq!(
            evaluate("States.Array('a,b', '(c)')", &Value::Null).unwrap(),
            json!(["a,b", "(c)"])
        );
        // Escaped braces in a non-Format literal are plain braces.
        assert_eq!(
            evaluate(r"States.StringToJson('\{\}')", &Value::Null).unwrap(),
            json!({})
        );
    }

    #[test]
    fn path_arguments_with_brackets_and_filters() {
        let input = json!({"items": [{"id": 1, "n": "a"}, {"id": 2, "n": "b"}], "k": {"x,y": 3}});
        assert_eq!(
            evaluate("States.ArrayLength($.items[?(@.id > 1)])", &input).unwrap(),
            json!(1)
        );
        assert_eq!(
            evaluate("States.Array($['k']['x,y'], $.items[-1].n)", &input).unwrap(),
            json!([3, "b"])
        );
    }

    #[test]
    fn missing_path_argument_is_runtime_error() {
        let err = evaluate_with_context("States.Format('{}', $.nope)", &json!({}), None)
            .unwrap_err()
            .into_states_error();
        assert_eq!(err.0, "States.Runtime");
        let err = evaluate_with_context("States.ArrayGetItem($.a, -1)", &json!({"a": [1]}), None)
            .unwrap_err()
            .into_states_error();
        assert_eq!(err.0, "States.IntrinsicFailure");
    }

    #[test]
    fn context_path_arguments() {
        let ctx = json!({"Execution": {"Id": "arn:x"}});
        assert_eq!(
            evaluate_with_context(
                "States.Format('id={}', $$.Execution.Id)",
                &json!({}),
                Some(&ctx)
            )
            .unwrap(),
            json!("id=arn:x")
        );
    }

    #[test]
    fn malformed_calls_error() {
        for bad in [
            "States.Format('{}'",
            "States.Array(1,",
            "States.Array(1) trailing",
            "States.Array('unterminated)",
            "States.Array(bogus)",
        ] {
            assert!(evaluate(bad, &Value::Null).is_err(), "{bad}");
        }
    }
}
