//! Minimal Velocity Template Language (VTL) evaluator for API Gateway.
//!
//! Supports the subset API Gateway commonly uses:
//! `#set`, `#if`/`#else`/`#end`, `#foreach`/`#end`, `$input`, `$context`,
//! `$util`, `$method`, and property / method-call access.

use base64::Engine;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use serde_json::{json, Value};
use std::collections::HashMap;

/// Maximum nesting depth for recursive block rendering (`#if`/`#foreach`).
/// A pathological template (deeply nested or self-referential via
/// `#set`-driven templates) would otherwise recurse until the thread's
/// stack overflows and aborts the process; capping it turns that into a
/// recoverable render error.
pub const MAX_RENDER_DEPTH: usize = 64;

/// Render a VTL template string using the supplied variable context.
///
/// Infallible wrapper around [`try_render`]: on a render error (currently
/// only the recursion-depth cap) it yields the output accumulated so far
/// rather than propagating, preserving the historical `-> String` API.
pub fn render(template: &str, ctx: &mut Context) -> String {
    match try_render(template, ctx) {
        Ok(s) => s,
        Err((partial, _)) => partial,
    }
}

/// Render a VTL template, returning an error (with the partial output)
/// when the recursion-depth cap is exceeded.
pub fn try_render(template: &str, ctx: &mut Context) -> Result<String, (String, String)> {
    render_depth(template, ctx, 0)
}

fn render_depth(
    template: &str,
    ctx: &mut Context,
    depth: usize,
) -> Result<String, (String, String)> {
    if depth > MAX_RENDER_DEPTH {
        return Err((
            String::new(),
            format!("VTL render exceeded maximum nesting depth of {MAX_RENDER_DEPTH}"),
        ));
    }
    let mut out = String::new();
    let mut i = 0;
    while i < template.len() {
        let rest = &template[i..];
        if rest.starts_with("#set(") {
            i += 5;
            let (var_expr, consumed) = parse_expr(template, i);
            i = consumed;
            if template[i..].starts_with(")") {
                i += 1;
            }
            if let Some((name, value)) = var_expr.strip_prefix("$").and_then(|s| s.split_once('='))
            {
                let name = name.trim();
                let val = eval_expr(value.trim(), ctx);
                ctx.set(name, val);
            }
        } else if rest.starts_with("#if(") {
            i += 4; // skip "#if("
            let (cond, consumed) = parse_expr(template, i);
            i = consumed;
            if template[i..].starts_with(")") {
                i += 1;
            }
            let cond_val = eval_expr(&cond, ctx);
            let truthy = is_truthy(&cond_val);
            let (then_block, else_block, consumed) = parse_if_blocks(template, i);
            i = consumed;
            let block = if truthy { then_block } else { else_block };
            match render_depth(block, ctx, depth + 1) {
                Ok(s) => out.push_str(&s),
                Err((partial, msg)) => {
                    out.push_str(&partial);
                    return Err((out, msg));
                }
            }
        } else if rest.starts_with("#foreach(") {
            i += 9;
            let (header, consumed) = parse_expr(template, i);
            i = consumed;
            if template[i..].starts_with(")") {
                i += 1;
            }
            let (item_var, collection_expr) = parse_foreach_header(&header);
            let items = eval_expr(collection_expr, ctx);
            let (body, consumed) = parse_foreach_body(template, i);
            i = consumed;
            if let Some(arr) = items.as_array() {
                for item in arr {
                    ctx.push_scope();
                    ctx.set(item_var, item.clone());
                    let rendered = render_depth(body, ctx, depth + 1);
                    ctx.pop_scope();
                    match rendered {
                        Ok(s) => out.push_str(&s),
                        Err((partial, msg)) => {
                            out.push_str(&partial);
                            return Err((out, msg));
                        }
                    }
                }
            } else if let Some(obj) = items.as_object() {
                for (key, val) in obj {
                    ctx.push_scope();
                    ctx.set(item_var, Value::String(key.clone()));
                    ctx.set(&format!("{item_var}_value"), val.clone());
                    let rendered = render_depth(body, ctx, depth + 1);
                    ctx.pop_scope();
                    match rendered {
                        Ok(s) => out.push_str(&s),
                        Err((partial, msg)) => {
                            out.push_str(&partial);
                            return Err((out, msg));
                        }
                    }
                }
            }
        } else if rest.starts_with("##") {
            // Comment to end of line
            if let Some(nl) = template[i..].find('\n') {
                i += nl + 1;
            } else {
                break;
            }
        } else if rest.starts_with('#') {
            // Unknown directive — skip to next newline
            if let Some(nl) = template[i..].find('\n') {
                i += nl + 1;
            } else {
                break;
            }
        } else if rest.starts_with("$!") {
            // Silent reference — swallow on null
            let (name, consumed) = parse_ref(template, i + 2);
            let val = eval_ref(&name, ctx);
            if !val.is_null() {
                out.push_str(&json_to_string(&val));
            }
            i = consumed;
        } else if rest.starts_with('$') {
            let (name, consumed) = parse_ref(template, i + 1);
            let val = eval_ref(&name, ctx);
            out.push_str(&json_to_string(&val));
            i = consumed;
        } else {
            out.push(template[i..].chars().next().unwrap());
            i += template[i..].chars().next().unwrap().len_utf8();
        }
    }
    Ok(out)
}

// ── Context ──

#[derive(Debug, Clone)]
pub struct Context {
    scopes: Vec<HashMap<String, Value>>,
}

impl Default for Context {
    fn default() -> Self {
        Self {
            scopes: vec![HashMap::new()],
        }
    }
}

impl Context {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set(&mut self, name: &str, value: Value) {
        if let Some(last) = self.scopes.last_mut() {
            last.insert(name.to_string(), value);
        }
    }

    pub fn with_var(mut self, name: &str, value: Value) -> Self {
        self.set(name, value);
        self
    }

    pub fn push_scope(&mut self) {
        self.scopes.push(HashMap::new());
    }

    pub fn pop_scope(&mut self) {
        if self.scopes.len() > 1 {
            self.scopes.pop();
        }
    }

    fn get(&self, name: &str) -> Value {
        for scope in self.scopes.iter().rev() {
            if let Some(v) = scope.get(name) {
                return v.clone();
            }
        }
        Value::Null
    }
}

// ── Parser helpers ──

fn parse_expr(template: &str, start: usize) -> (String, usize) {
    let mut depth = 0;
    let mut in_string = false;
    let mut string_char = ' ';
    let mut escaped = false;
    let mut i = start;
    while i < template.len() {
        let c = template[i..].chars().next().unwrap();
        let char_len = c.len_utf8();
        if escaped {
            escaped = false;
            i += char_len;
            continue;
        }
        if in_string {
            if c == '\\' {
                escaped = true;
                i += char_len;
                continue;
            }
            if c == string_char {
                in_string = false;
            }
            i += char_len;
            continue;
        }
        if c == '\'' || c == '"' {
            in_string = true;
            string_char = c;
            i += char_len;
            continue;
        }
        if c == '(' {
            depth += 1;
        } else if c == ')' {
            if depth == 0 {
                break;
            }
            depth -= 1;
        }
        i += char_len;
    }
    (template[start..i].trim().to_string(), i)
}

fn parse_ref(template: &str, start: usize) -> (String, usize) {
    let mut i = start;
    let mut name = String::new();
    // Handle ${var} syntax
    if template[i..].starts_with('{') {
        i += 1;
        while i < template.len() {
            let c = template[i..].chars().next().unwrap();
            if c == '}' {
                i += 1;
                break;
            }
            name.push(c);
            i += c.len_utf8();
        }
        return (name, i);
    }
    // Normal $var.name.method('arg')...
    while i < template.len() {
        let c = template[i..].chars().next().unwrap();
        if c.is_alphanumeric() || c == '_' || c == '.' || c == '[' || c == ']' {
            name.push(c);
            i += c.len_utf8();
        } else if c == '(' {
            // Include method call args
            let (args, consumed) = parse_expr(template, i + 1);
            name.push('(');
            name.push_str(&args);
            name.push(')');
            i = consumed + 1; // skip closing )
        } else {
            break;
        }
    }
    (name, i)
}

fn parse_foreach_header(header: &str) -> (&str, &str) {
    let parts: Vec<&str> = header.split(" in ").collect();
    if parts.len() == 2 {
        let item = parts[0].trim().strip_prefix("$").unwrap_or(parts[0].trim());
        (item, parts[1].trim())
    } else {
        (header, "")
    }
}

fn parse_if_blocks(template: &str, start: usize) -> (&str, &str, usize) {
    let mut depth = 1;
    let mut i = start;
    let then_start = start;
    let mut else_start = None;
    while i < template.len() && depth > 0 {
        let rest = &template[i..];
        if rest.starts_with("#if(") {
            depth += 1;
            i += 4;
            continue;
        }
        if rest.starts_with("#foreach(") {
            depth += 1;
            i += 9;
            continue;
        }
        if rest.starts_with("#end") {
            depth -= 1;
            if depth == 0 {
                let then_block = match else_start {
                    Some(es) => &template[then_start..es],
                    None => &template[then_start..i],
                };
                let else_block = match else_start {
                    Some(es) => &template[(es + 5)..i],
                    None => "",
                };
                i += 4;
                return (then_block.trim(), else_block.trim(), i);
            }
            i += 4;
            continue;
        }
        if depth == 1 && rest.starts_with("#else") {
            else_start = Some(i);
            i += 5;
            continue;
        }
        i += template[i..].chars().next().unwrap().len_utf8();
    }
    (&template[then_start..i], "", i)
}

fn parse_foreach_body(template: &str, start: usize) -> (&str, usize) {
    let mut depth = 1;
    let mut i = start;
    let body_start = start;
    while i < template.len() && depth > 0 {
        let rest = &template[i..];
        if rest.starts_with("#foreach(") {
            depth += 1;
            i += 9;
            continue;
        }
        if rest.starts_with("#if(") {
            depth += 1;
            i += 4;
            continue;
        }
        if rest.starts_with("#end") {
            depth -= 1;
            if depth == 0 {
                let body = &template[body_start..i];
                i += 4;
                return (body.trim(), i);
            }
            i += 4;
            continue;
        }
        i += template[i..].chars().next().unwrap().len_utf8();
    }
    (&template[body_start..i], i)
}

// ── Evaluation ──

fn eval_expr(expr: &str, ctx: &mut Context) -> Value {
    let expr = expr.trim();
    if expr.is_empty() {
        return Value::Null;
    }
    // String literal. The `len() >= 2` guard is load-bearing: a lone `'` or `"`
    // satisfies both `starts_with` and `ends_with` (the single char is
    // simultaneously first and last), which would make `expr[1..expr.len() - 1]`
    // slice `[1..0]` and panic, dropping the connection.
    if expr.len() >= 2
        && ((expr.starts_with('\'') && expr.ends_with('\''))
            || (expr.starts_with('"') && expr.ends_with('"')))
    {
        return Value::String(expr[1..expr.len() - 1].to_string());
    }
    // Boolean literals
    if expr == "true" {
        return Value::Bool(true);
    }
    if expr == "false" {
        return Value::Bool(false);
    }
    // Number literal
    if let Ok(n) = expr.parse::<i64>() {
        return Value::Number(n.into());
    }
    if let Ok(f) = expr.parse::<f64>() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return Value::Number(n);
        }
    }
    // Binary ops: +, -, ==, !=, <, >, <=, >=, &&, ||
    if let Some((op, lhs, rhs)) = find_binary_op(expr) {
        let l = eval_expr(lhs, ctx);
        let r = eval_expr(rhs, ctx);
        return eval_binary_op(op, &l, &r);
    }
    // Reference
    if let Some(stripped) = expr.strip_prefix('$') {
        return eval_ref(stripped, ctx);
    }
    // JSON literal (starts with { or [)
    if expr.starts_with('{') || expr.starts_with('[') {
        return serde_json::from_str(expr).unwrap_or(Value::Null);
    }
    Value::String(expr.to_string())
}

fn eval_ref(name: &str, ctx: &mut Context) -> Value {
    let mut parts = split_dotted(name);
    if parts.is_empty() {
        return Value::Null;
    }
    let first = parts.remove(0);
    let mut val = ctx.get(&first);

    while !parts.is_empty() {
        let part = parts.remove(0);
        // Method call?
        if part.ends_with(')') {
            let (method, args) = parse_method_call(&part, ctx);
            val = eval_method(method, &args, &val, ctx);
        } else {
            val = access_property(&val, &part);
        }
    }
    val
}

fn parse_method_call<'a>(s: &'a str, ctx: &mut Context) -> (&'a str, Vec<Value>) {
    if let Some(open) = s.find('(') {
        let method = &s[..open];
        let args_str = &s[open + 1..s.len() - 1];
        let args = split_args(args_str)
            .into_iter()
            .map(|a| eval_expr(&a, ctx))
            .collect();
        (method, args)
    } else {
        (s, vec![])
    }
}

fn split_args(s: &str) -> Vec<String> {
    let mut out = vec![];
    let mut cur = String::new();
    let mut depth = 0;
    let mut in_string = false;
    let mut string_char = ' ';
    let mut escaped = false;
    for c in s.chars() {
        if escaped {
            cur.push(c);
            escaped = false;
            continue;
        }
        if in_string {
            if c == '\\' {
                escaped = true;
                cur.push(c);
                continue;
            }
            if c == string_char {
                in_string = false;
            }
            cur.push(c);
            continue;
        }
        if c == '\'' || c == '"' {
            in_string = true;
            string_char = c;
            cur.push(c);
            continue;
        }
        if c == '(' {
            depth += 1;
            cur.push(c);
            continue;
        }
        if c == ')' {
            depth -= 1;
            cur.push(c);
            continue;
        }
        if c == ',' && depth == 0 {
            out.push(cur.trim().to_string());
            cur.clear();
            continue;
        }
        cur.push(c);
    }
    if !cur.trim().is_empty() {
        out.push(cur.trim().to_string());
    }
    out
}

fn split_dotted(s: &str) -> Vec<String> {
    let mut out = vec![];
    let mut cur = String::new();
    let mut in_brackets = false;
    let mut paren_depth = 0;
    for c in s.chars() {
        if c == '[' {
            in_brackets = true;
            if !cur.is_empty() {
                out.push(cur.clone());
                cur.clear();
            }
            cur.push(c);
            continue;
        }
        if c == ']' {
            in_brackets = false;
            cur.push(c);
            out.push(cur.clone());
            cur.clear();
            continue;
        }
        if c == '(' {
            paren_depth += 1;
            cur.push(c);
            continue;
        }
        if c == ')' {
            paren_depth -= 1;
            cur.push(c);
            continue;
        }
        if c == '.' && !in_brackets && paren_depth == 0 {
            if !cur.is_empty() {
                out.push(cur.clone());
                cur.clear();
            }
            continue;
        }
        cur.push(c);
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

fn access_property(val: &Value, prop: &str) -> Value {
    let prop = if prop.starts_with('[') && prop.ends_with(']') {
        &prop[1..prop.len() - 1]
    } else {
        prop
    };
    let prop = if prop.len() >= 2
        && ((prop.starts_with('\'') && prop.ends_with('\''))
            || (prop.starts_with('"') && prop.ends_with('"')))
    {
        &prop[1..prop.len() - 1]
    } else {
        prop
    };
    match val {
        Value::Object(o) => o.get(prop).cloned().unwrap_or(Value::Null),
        Value::Array(a) => {
            if let Ok(idx) = prop.parse::<usize>() {
                a.get(idx).cloned().unwrap_or(Value::Null)
            } else {
                Value::Null
            }
        }
        _ => Value::Null,
    }
}

fn eval_method(name: &str, args: &[Value], receiver: &Value, _ctx: &mut Context) -> Value {
    match name {
        "json" => {
            // JSONPath against parsed body: $input.json('$.foo')
            if let Some(arg) = args.first() {
                let path = json_to_string(arg);
                let path = path.trim_matches('\'').trim_matches('"');
                let target = access_property(receiver, "json");
                let result = jsonpath_extract(&target, path);
                return serde_json::to_string(&result).unwrap_or_default().into();
            }
            Value::Null
        }
        "path" => {
            // JSONPath against parsed body returning raw value
            if let Some(arg) = args.first() {
                let path = json_to_string(arg);
                let path = path.trim_matches('\'').trim_matches('"');
                let target = access_property(receiver, "json");
                return jsonpath_extract(&target, path);
            }
            Value::Null
        }
        "parseJson" => {
            if let Some(arg) = args.first() {
                let s = json_to_string(arg);
                return serde_json::from_str(&s).unwrap_or(Value::Null);
            }
            Value::Null
        }
        "toJson" => {
            if let Some(arg) = args.first() {
                serde_json::to_string(arg).unwrap_or_default().into()
            } else {
                serde_json::to_string(receiver).unwrap_or_default().into()
            }
        }
        "escapeJavaScript" => {
            let src = args
                .first()
                .map(json_to_string)
                .unwrap_or_else(|| json_to_string(receiver));
            let json = serde_json::to_string(&src).unwrap_or_default();
            // Strip the JSON wrapping quotes; AWS returns only the escaped
            // content so templates can embed it inside their own quotes.
            json.strip_prefix('"')
                .and_then(|s| s.strip_suffix('"'))
                .unwrap_or(&json)
                .into()
        }
        "urlEncode" => {
            if let Some(arg) = args.first() {
                let s = json_to_string(arg);
                return utf8_percent_encode(&s, NON_ALPHANUMERIC).to_string().into();
            }
            utf8_percent_encode(&json_to_string(receiver), NON_ALPHANUMERIC)
                .to_string()
                .into()
        }
        "base64Encode" => {
            if let Some(arg) = args.first() {
                let s = json_to_string(arg);
                return base64::engine::general_purpose::STANDARD.encode(&s).into();
            }
            base64::engine::general_purpose::STANDARD
                .encode(json_to_string(receiver))
                .into()
        }
        _ => Value::Null,
    }
}

fn jsonpath_extract(val: &Value, path: &str) -> Value {
    let mut current = val;
    let path = path.strip_prefix("$").unwrap_or(path);
    for seg in path.split('.').filter(|s| !s.is_empty()) {
        if let Some(stripped) = seg.strip_prefix("[") {
            let idx_str = stripped.strip_suffix("]").unwrap_or(stripped);
            if let Ok(idx) = idx_str.parse::<usize>() {
                if let Value::Array(a) = current {
                    current = a.get(idx).unwrap_or(&Value::Null);
                } else {
                    return Value::Null;
                }
            } else {
                if let Value::Object(o) = current {
                    current = o.get(idx_str).unwrap_or(&Value::Null);
                } else {
                    return Value::Null;
                }
            }
        } else {
            match current {
                Value::Object(o) => current = o.get(seg).unwrap_or(&Value::Null),
                _ => return Value::Null,
            }
        }
    }
    current.clone()
}

fn json_to_string(val: &Value) -> String {
    match val {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(val).unwrap_or_default(),
    }
}

fn is_truthy(val: &Value) -> bool {
    match val {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                i != 0
            } else if let Some(f) = n.as_f64() {
                f != 0.0
            } else {
                false
            }
        }
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

fn find_binary_op(expr: &str) -> Option<(&str, &str, &str)> {
    // Lower number = lower precedence (binds looser, so split here first).
    let precedence = |op: &str| match op {
        "||" => 1,
        "&&" => 2,
        "==" | "!=" => 3,
        "<" | ">" | "<=" | ">=" => 4,
        "+" | "-" => 5,
        "*" | "/" => 6,
        _ => 100,
    };
    let ops = [
        "||", "&&", "==", "!=", "<=", ">=", "<", ">", "+", "-", "*", "/",
    ];
    let mut depth = 0;
    let mut in_string = false;
    let mut string_char = ' ';
    let mut escaped = false;
    let mut best: Option<(usize, &str)> = None;
    for (i, c) in expr.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if in_string {
            if c == '\\' {
                escaped = true;
                continue;
            }
            if c == string_char {
                in_string = false;
            }
            continue;
        }
        if c == '\'' || c == '"' {
            in_string = true;
            string_char = c;
            continue;
        }
        if c == '(' {
            depth += 1;
            continue;
        }
        if c == ')' {
            depth -= 1;
            continue;
        }
        if depth == 0 {
            for op in &ops {
                if expr[i..].starts_with(op) {
                    let p = precedence(op);
                    let replace = if let Some((bi, cur)) = best {
                        let cur_p = precedence(cur);
                        p < cur_p || (p == cur_p && i > bi)
                    } else {
                        true
                    };
                    if replace {
                        best = Some((i, *op));
                    }
                }
            }
        }
    }
    best.map(|(i, op)| {
        let lhs = expr[..i].trim();
        let rhs = expr[i + op.len()..].trim();
        (op, lhs, rhs)
    })
}

fn eval_binary_op(op: &str, lhs: &Value, rhs: &Value) -> Value {
    match op {
        "==" => Value::Bool(lhs == rhs),
        "!=" => Value::Bool(lhs != rhs),
        "&&" => Value::Bool(is_truthy(lhs) && is_truthy(rhs)),
        "||" => Value::Bool(is_truthy(lhs) || is_truthy(rhs)),
        "+" => {
            if let (Some(a), Some(b)) = (as_number(lhs), as_number(rhs)) {
                if let Some(n) = serde_json::Number::from_f64(a + b) {
                    return Value::Number(n);
                }
            }
            Value::String(format!("{}{}", json_to_string(lhs), json_to_string(rhs)))
        }
        "-" => {
            if let (Some(a), Some(b)) = (as_number(lhs), as_number(rhs)) {
                if let Some(n) = serde_json::Number::from_f64(a - b) {
                    return Value::Number(n);
                }
            }
            Value::Null
        }
        "*" => {
            if let (Some(a), Some(b)) = (as_number(lhs), as_number(rhs)) {
                if let Some(n) = serde_json::Number::from_f64(a * b) {
                    return Value::Number(n);
                }
            }
            Value::Null
        }
        "/" => {
            if let (Some(a), Some(b)) = (as_number(lhs), as_number(rhs)) {
                if b != 0.0 {
                    if let Some(n) = serde_json::Number::from_f64(a / b) {
                        return Value::Number(n);
                    }
                }
            }
            Value::Null
        }
        "<" => {
            if let (Some(a), Some(b)) = (as_number(lhs), as_number(rhs)) {
                Value::Bool(a < b)
            } else {
                Value::Bool(json_to_string(lhs) < json_to_string(rhs))
            }
        }
        ">" => {
            if let (Some(a), Some(b)) = (as_number(lhs), as_number(rhs)) {
                Value::Bool(a > b)
            } else {
                Value::Bool(json_to_string(lhs) > json_to_string(rhs))
            }
        }
        "<=" => {
            if let (Some(a), Some(b)) = (as_number(lhs), as_number(rhs)) {
                Value::Bool(a <= b)
            } else {
                Value::Bool(json_to_string(lhs) <= json_to_string(rhs))
            }
        }
        ">=" => {
            if let (Some(a), Some(b)) = (as_number(lhs), as_number(rhs)) {
                Value::Bool(a >= b)
            } else {
                Value::Bool(json_to_string(lhs) >= json_to_string(rhs))
            }
        }
        _ => Value::Null,
    }
}

fn as_number(val: &Value) -> Option<f64> {
    match val {
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

// ── Built-in context builders ──

/// Build a standard API Gateway VTL context for a request.
pub fn build_context(
    req: &fakecloud_core::service::AwsRequest,
    api_id: &str,
    stage_name: &str,
    resource_path: &str,
    _path_params: &std::collections::BTreeMap<String, String>,
    stage_vars: &std::collections::BTreeMap<String, String>,
) -> Context {
    let mut ctx = Context::new();

    // $input
    let body_str = String::from_utf8_lossy(&req.body).to_string();
    let body_json: Value = serde_json::from_str(&body_str).unwrap_or(Value::Null);
    let input = json!({
        "body": body_str,
        "json": body_json,
    });
    ctx.set("input", input);

    // $method
    ctx.set("method", req.method.as_str().into());

    // $context
    let context = json!({
        "apiId": api_id,
        "stage": stage_name,
        "resourcePath": resource_path,
        "httpMethod": req.method.as_str(),
        "identity": {
            "sourceIp": "127.0.0.1",
        },
        "requestId": req.request_id,
        "requestTime": chrono::Utc::now().to_rfc3339(),
        "requestTimeEpoch": chrono::Utc::now().timestamp_millis(),
    });
    ctx.set("context", context);

    // $util
    let util = json!({"_type": "util"});
    ctx.set("util", util);

    // $stageVariables
    let sv: serde_json::Map<String, serde_json::Value> = stage_vars
        .iter()
        .map(|(k, v)| (k.clone(), v.clone().into()))
        .collect();
    ctx.set("stageVariables", serde_json::Value::Object(sv));

    ctx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_variable_interpolation() {
        let mut ctx = Context::new().with_var("name", "world".into());
        assert_eq!(render("Hello $name!", &mut ctx), "Hello world!");
    }

    #[test]
    fn set_and_use() {
        let mut ctx = Context::new();
        let out = render("#set($x = 42)Value: $x", &mut ctx);
        assert_eq!(out, "Value: 42");
    }

    #[test]
    fn input_body() {
        let mut ctx = Context::new().with_var("input", json!({"body": r#"{"foo":"bar"}"#}));
        assert_eq!(render("$input.body", &mut ctx), r#"{"foo":"bar"}"#);
    }

    #[test]
    fn input_json_path() {
        let mut ctx = Context::new().with_var(
            "input",
            json!({"body": r#"{"foo":"bar"}"#, "json": {"foo":"bar"}}),
        );
        assert_eq!(render("$input.json('$.foo')", &mut ctx), "\"bar\"");
    }

    #[test]
    fn util_to_json() {
        let mut ctx = Context::new().with_var("util", json!({"_type":"util"}));
        assert_eq!(
            render("#set($obj = {\"a\":1})$util.toJson($obj)", &mut ctx),
            r#"{"a":1}"#,
        );
    }

    #[test]
    fn util_parse_json() {
        let mut ctx = Context::new().with_var("util", json!({"_type":"util"}));
        let out = render(
            "#set($parsed = $util.parseJson('{\"x\":9}'))$parsed.x",
            &mut ctx,
        );
        assert_eq!(out, "9");
    }

    #[test]
    fn if_else_true() {
        let mut ctx = Context::new().with_var("ok", true.into());
        let out = render("#if($ok)yes#else no#end", &mut ctx);
        assert_eq!(out, "yes");
    }

    #[test]
    fn if_else_false() {
        let mut ctx = Context::new().with_var("ok", false.into());
        let out = render("#if($ok)yes#else no#end", &mut ctx);
        assert_eq!(out, "no");
    }

    #[test]
    fn foreach_array() {
        let mut ctx = Context::new().with_var("items", json!([1, 2, 3]));
        let out = render("#foreach($i in $items)$i,#end", &mut ctx);
        assert_eq!(out, "1,2,3,");
    }

    #[test]
    fn foreach_object_keys() {
        let mut ctx = Context::new().with_var("items", json!({"a": 1, "b": 2}));
        let out = render("#foreach($k in $items)$k:#end", &mut ctx);
        assert_eq!(out, "a:b:");
    }

    #[test]
    fn json_object_literal() {
        let mut ctx = Context::new();
        let out = render("#set($x = {\"a\":1})$x.a", &mut ctx);
        assert_eq!(out, "1");
    }

    #[test]
    fn property_chain() {
        let mut ctx = Context::new().with_var("ctx", json!({"a":{"b":"val"}}));
        assert_eq!(render("$ctx.a.b", &mut ctx), "val");
    }

    #[test]
    fn braced_reference() {
        let mut ctx = Context::new().with_var("foo", "bar".into());
        assert_eq!(render("${foo}", &mut ctx), "bar");
    }

    #[test]
    fn silent_reference_null() {
        let mut ctx = Context::new();
        assert_eq!(render("$!missing", &mut ctx), "");
    }

    #[test]
    fn comparison_operators() {
        let mut ctx = Context::new()
            .with_var("x", 5.into())
            .with_var("y", 10.into());
        assert_eq!(render("#if($x < $y)less#end", &mut ctx), "less");
        assert_eq!(render("#if($x > $y)greater#end", &mut ctx), "");
        assert_eq!(render("#if($x == 5)eq#end", &mut ctx), "eq");
        assert_eq!(render("#if($x != 5)neq#end", &mut ctx), "");
    }

    #[test]
    fn util_escape_javascript() {
        let mut ctx = Context::new().with_var("util", json!({"_type":"util"}));
        let out = render(r#"$util.escapeJavaScript('a"b')"#, &mut ctx);
        assert_eq!(out, r#"a\"b"#);
    }

    #[test]
    fn util_url_encode() {
        let mut ctx = Context::new().with_var("util", json!({"_type":"util"}));
        let out = render(r#"$util.urlEncode('hello world')"#, &mut ctx);
        assert_eq!(out, "hello%20world");
    }

    #[test]
    fn recursion_depth_cap_returns_error() {
        // A template nested far deeper than the cap must surface a render
        // error instead of overflowing the stack.
        let depth = MAX_RENDER_DEPTH + 5;
        let mut tpl = String::new();
        for _ in 0..depth {
            tpl.push_str("#if(true)");
        }
        tpl.push('x');
        for _ in 0..depth {
            tpl.push_str("#end");
        }
        let mut ctx = Context::new();
        let err = try_render(&tpl, &mut ctx);
        assert!(err.is_err(), "deeply nested template must error");
        assert!(err.unwrap_err().1.contains("maximum nesting depth"));
    }

    #[test]
    fn shallow_nesting_still_renders() {
        // Nesting well within the cap keeps working.
        let mut ctx = Context::new().with_var("ok", true.into());
        let out = render("#if($ok)#if($ok)deep#end#end", &mut ctx);
        assert_eq!(out, "deep");
    }

    #[test]
    fn util_base64_encode() {
        let mut ctx = Context::new().with_var("util", json!({"_type":"util"}));
        let out = render(r#"$util.base64Encode('hello')"#, &mut ctx);
        assert_eq!(out, "aGVsbG8=");
    }

    #[test]
    fn eval_expr_lone_quote_does_not_panic() {
        // A lone `'` or `"` satisfies both starts_with and ends_with; without the
        // len>=2 guard `expr[1..expr.len()-1]` slices [1..0] and panics, dropping
        // the connection. Treat it as a plain one-char string instead.
        let mut ctx = Context::new();
        assert_eq!(eval_expr("'", &mut ctx), Value::String("'".to_string()));
        assert_eq!(eval_expr("\"", &mut ctx), Value::String("\"".to_string()));
        // Balanced literals still strip.
        assert_eq!(eval_expr("'hi'", &mut ctx), Value::String("hi".to_string()));
        assert_eq!(eval_expr("''", &mut ctx), Value::String(String::new()));
    }

    #[test]
    fn access_property_lone_quote_does_not_panic() {
        // Same guard on access_property's quote-strip: a bare-quote property
        // segment must not panic.
        let obj = json!({"'": "v", "k": "w"});
        assert_eq!(access_property(&obj, "'"), Value::String("v".to_string()));
        assert_eq!(access_property(&obj, "\""), Value::Null);
        // Balanced-quoted key still resolves.
        assert_eq!(access_property(&obj, "'k'"), Value::String("w".to_string()));
    }

    #[test]
    fn lone_quote_template_renders_without_crash() {
        // End-to-end: the exact template from the bug report must render without
        // unwinding (pre-fix it panicked and dropped the connection). The rendered
        // value itself is incidental; not crashing is the contract under test.
        let mut ctx = Context::new();
        let _ = render("#set($x = ')$x", &mut ctx);
        let mut ctx2 = Context::new().with_var("obj", json!({"a": 1}));
        let _ = render("$obj.'", &mut ctx2);
    }
}
