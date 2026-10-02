//! JSONPath evaluation for Amazon States Language paths.
//!
//! Step Functions evaluates `InputPath`, `OutputPath`, `ItemsPath`, the `.$`
//! fields of `Parameters` / `ResultSelector` / `ItemSelector`, Choice
//! `Variable`s and intrinsic arguments with Jayway JsonPath. This module
//! implements the same syntax and the same result shape:
//!
//! - dot and bracket child access: `$.a.b`, `$['a']`, `$["a"]`, `$['a','b']`
//! - array indexes, including negative ones and unions: `[0]`, `[-1]`, `[0,2]`
//! - wildcards `.*` / `[*]`, slices `[1:3]`, `[:2]`, `[-2:]`, deep scan `..x`
//! - filters `[?(@.price < 10 && @.tag == 'x')]`, `[?(@.isbn)]`
//!
//! A *definite* path (only single names and single indexes) yields one value
//! and fails when nothing is found, which Step Functions surfaces as a
//! `States.Runtime` error. Any other path is *indefinite* and yields a JSON
//! array of every match (possibly empty).

use serde_json::{Number, Value};

#[derive(Debug, Clone, PartialEq)]
enum Segment {
    /// One or more property names (`.a`, `['a']`, `['a','b']`).
    Names(Vec<String>),
    /// One or more indexes (`[0]`, `[-1]`, `[0,2]`).
    Indexes(Vec<i64>),
    Wildcard,
    Slice(Option<i64>, Option<i64>),
    /// `..name`, `..*` or `..[...]`: the inner segment applied at every depth.
    DeepScan(Box<Segment>),
    Filter(FilterExpr),
}

#[derive(Debug, Clone, PartialEq)]
enum FilterExpr {
    Or(Box<FilterExpr>, Box<FilterExpr>),
    And(Box<FilterExpr>, Box<FilterExpr>),
    Not(Box<FilterExpr>),
    Exists(Operand),
    Compare(Operand, CmpOp, Operand),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum CmpOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Debug, Clone, PartialEq)]
enum Operand {
    /// `@...` relative to the current item.
    Current(Vec<Segment>),
    /// `$...` relative to the document root.
    Root(Vec<Segment>),
    Literal(Value),
}

/// A parsed JSONPath.
#[derive(Debug, Clone, PartialEq)]
pub struct JsonPath {
    segments: Vec<Segment>,
}

/// One step of a reference path.
#[derive(Debug, Clone, PartialEq)]
pub enum Step {
    Name(String),
    Index(i64),
}

/// Why a path could not be evaluated.
#[derive(Debug, Clone, PartialEq)]
pub enum PathError {
    /// Syntactically invalid path.
    Invalid(String),
    /// A definite path matched nothing. Carries the normalized path
    /// (`$['a'][0]`) as Jayway reports it.
    NoResults(String),
}

impl JsonPath {
    /// Parse a path that must start with `$`.
    pub fn parse(path: &str) -> Result<Self, PathError> {
        let chars: Vec<char> = path.trim().chars().collect();
        if chars.first() != Some(&'$') {
            return Err(PathError::Invalid(format!(
                "Path must start with '$': {path}"
            )));
        }
        let mut p = Parser { chars, pos: 1 };
        let segments = p.segments(false)?;
        if p.pos != p.chars.len() {
            return Err(PathError::Invalid(format!(
                "Unexpected character at position {} in path {path}",
                p.pos
            )));
        }
        Ok(Self { segments })
    }

    /// True when the path can yield at most one value.
    pub fn is_definite(&self) -> bool {
        self.segments.iter().all(|s| match s {
            Segment::Names(n) => n.len() == 1,
            Segment::Indexes(i) => i.len() == 1,
            _ => false,
        })
    }

    /// For a definite (reference) path, the chain of property names and
    /// indexes it walks. `None` for an indefinite path.
    pub fn reference_steps(&self) -> Option<Vec<Step>> {
        if !self.is_definite() {
            return None;
        }
        Some(
            self.segments
                .iter()
                .map(|s| match s {
                    Segment::Names(n) => Step::Name(n[0].clone()),
                    Segment::Indexes(i) => Step::Index(i[0]),
                    _ => unreachable!("definite paths only hold names and indexes"),
                })
                .collect(),
        )
    }

    /// Jayway-style normalized rendering, used in error messages.
    pub fn normalized(&self) -> String {
        let mut out = String::from("$");
        for s in &self.segments {
            render_segment(s, &mut out);
        }
        out
    }

    /// Evaluate against `root`. Definite paths return the single match or
    /// [`PathError::NoResults`]; indefinite paths return an array of matches.
    pub fn evaluate(&self, root: &Value) -> Result<Value, PathError> {
        let results = select(&self.segments, root, root);
        if self.is_definite() {
            results
                .into_iter()
                .next()
                .cloned()
                .ok_or_else(|| PathError::NoResults(self.normalized()))
        } else {
            Ok(Value::Array(results.into_iter().cloned().collect()))
        }
    }
}

/// Parse and evaluate in one step.
pub fn evaluate(root: &Value, path: &str) -> Result<Value, PathError> {
    JsonPath::parse(path)?.evaluate(root)
}

fn render_segment(s: &Segment, out: &mut String) {
    match s {
        Segment::Names(names) => {
            let inner: Vec<String> = names.iter().map(|n| format!("'{n}'")).collect();
            out.push('[');
            out.push_str(&inner.join(","));
            out.push(']');
        }
        Segment::Indexes(idx) => {
            let inner: Vec<String> = idx.iter().map(|i| i.to_string()).collect();
            out.push('[');
            out.push_str(&inner.join(","));
            out.push(']');
        }
        Segment::Wildcard => out.push_str("[*]"),
        Segment::Slice(a, b) => {
            out.push('[');
            if let Some(a) = a {
                out.push_str(&a.to_string());
            }
            out.push(':');
            if let Some(b) = b {
                out.push_str(&b.to_string());
            }
            out.push(']');
        }
        Segment::DeepScan(inner) => {
            out.push_str("..");
            render_segment(inner, out);
        }
        Segment::Filter(_) => out.push_str("[?]"),
    }
}

fn select<'a>(segments: &[Segment], node: &'a Value, root: &'a Value) -> Vec<&'a Value> {
    let mut current = vec![node];
    for seg in segments {
        let mut next = Vec::new();
        for v in current {
            apply_segment(seg, v, root, &mut next);
        }
        current = next;
    }
    current
}

fn apply_segment<'a>(seg: &Segment, v: &'a Value, root: &'a Value, out: &mut Vec<&'a Value>) {
    match seg {
        Segment::Names(names) => {
            if let Value::Object(o) = v {
                for n in names {
                    if let Some(child) = o.get(n) {
                        out.push(child);
                    }
                }
            }
        }
        Segment::Indexes(idx) => {
            if let Value::Array(a) = v {
                for &i in idx {
                    if let Some(child) = resolve_index(a, i) {
                        out.push(child);
                    }
                }
            }
        }
        Segment::Wildcard => match v {
            Value::Array(a) => out.extend(a.iter()),
            Value::Object(o) => out.extend(o.values()),
            _ => {}
        },
        Segment::Slice(start, end) => {
            if let Value::Array(a) = v {
                let len = a.len() as i64;
                let norm = |x: i64| if x < 0 { (len + x).max(0) } else { x.min(len) };
                let s = start.map(norm).unwrap_or(0);
                let e = end.map(norm).unwrap_or(len);
                if s < e {
                    out.extend(a[s as usize..e as usize].iter());
                }
            }
        }
        Segment::DeepScan(inner) => deep_scan(inner, v, root, out),
        Segment::Filter(expr) => {
            let items: Vec<&Value> = match v {
                Value::Array(a) => a.iter().collect(),
                Value::Object(_) => vec![v],
                _ => Vec::new(),
            };
            for item in items {
                if eval_filter(expr, item, root) {
                    out.push(item);
                }
            }
        }
    }
}

fn deep_scan<'a>(inner: &Segment, v: &'a Value, root: &'a Value, out: &mut Vec<&'a Value>) {
    apply_segment(inner, v, root, out);
    match v {
        Value::Array(a) => a.iter().for_each(|c| deep_scan(inner, c, root, out)),
        Value::Object(o) => o.values().for_each(|c| deep_scan(inner, c, root, out)),
        _ => {}
    }
}

fn resolve_index(a: &[Value], i: i64) -> Option<&Value> {
    let idx = if i < 0 { a.len() as i64 + i } else { i };
    if idx < 0 {
        None
    } else {
        a.get(idx as usize)
    }
}

fn operand_value<'a>(op: &'a Operand, item: &'a Value, root: &'a Value) -> Option<Value> {
    match op {
        Operand::Literal(v) => Some(v.clone()),
        Operand::Current(segs) => select(segs, item, root).first().map(|v| (*v).clone()),
        Operand::Root(segs) => select(segs, root, root).first().map(|v| (*v).clone()),
    }
}

fn eval_filter(expr: &FilterExpr, item: &Value, root: &Value) -> bool {
    match expr {
        FilterExpr::Or(a, b) => eval_filter(a, item, root) || eval_filter(b, item, root),
        FilterExpr::And(a, b) => eval_filter(a, item, root) && eval_filter(b, item, root),
        FilterExpr::Not(a) => !eval_filter(a, item, root),
        FilterExpr::Exists(op) => operand_value(op, item, root).is_some(),
        FilterExpr::Compare(l, op, r) => {
            let (Some(l), Some(r)) = (operand_value(l, item, root), operand_value(r, item, root))
            else {
                return false;
            };
            compare(&l, *op, &r)
        }
    }
}

fn compare(l: &Value, op: CmpOp, r: &Value) -> bool {
    use std::cmp::Ordering;
    let ord: Option<Ordering> = match (l, r) {
        (Value::Number(a), Value::Number(b)) => a
            .as_f64()
            .zip(b.as_f64())
            .and_then(|(a, b)| a.partial_cmp(&b)),
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        _ => None,
    };
    match op {
        CmpOp::Eq => ord.map(|o| o == Ordering::Equal).unwrap_or(l == r),
        CmpOp::Ne => ord.map(|o| o != Ordering::Equal).unwrap_or(l != r),
        CmpOp::Lt => ord == Some(Ordering::Less),
        CmpOp::Le => matches!(ord, Some(Ordering::Less | Ordering::Equal)),
        CmpOp::Gt => ord == Some(Ordering::Greater),
        CmpOp::Ge => matches!(ord, Some(Ordering::Greater | Ordering::Equal)),
    }
}

struct Parser {
    chars: Vec<char>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<char> {
        self.chars.get(self.pos).copied()
    }

    fn peek_at(&self, off: usize) -> Option<char> {
        self.chars.get(self.pos + off).copied()
    }

    fn err<T>(&self, msg: &str) -> Result<T, PathError> {
        Err(PathError::Invalid(format!(
            "{msg} at position {} in path {}",
            self.pos,
            self.chars.iter().collect::<String>()
        )))
    }

    fn skip_ws(&mut self) {
        while self.peek().is_some_and(char::is_whitespace) {
            self.pos += 1;
        }
    }

    /// Parse segments until the end of input, or (inside a filter) until a
    /// character that cannot continue a path.
    fn segments(&mut self, in_filter: bool) -> Result<Vec<Segment>, PathError> {
        let mut out = Vec::new();
        loop {
            match self.peek() {
                Some('.') if self.peek_at(1) == Some('.') => {
                    self.pos += 2;
                    let inner = match self.peek() {
                        Some('[') => self.bracket()?,
                        Some('*') => {
                            self.pos += 1;
                            Segment::Wildcard
                        }
                        _ => Segment::Names(vec![self.dot_name()?]),
                    };
                    out.push(Segment::DeepScan(Box::new(inner)));
                }
                Some('.') => {
                    self.pos += 1;
                    if self.peek() == Some('*') {
                        self.pos += 1;
                        out.push(Segment::Wildcard);
                    } else {
                        out.push(Segment::Names(vec![self.dot_name()?]));
                    }
                }
                Some('[') => {
                    let seg = self.bracket()?;
                    out.push(seg);
                }
                None => break,
                Some(_) if in_filter => break,
                Some(_) => return self.err("Unexpected character"),
            }
        }
        Ok(out)
    }

    fn dot_name(&mut self) -> Result<String, PathError> {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c == '.' || c == '[' || c.is_whitespace() || "]()=!<>&|,'\"".contains(c) {
                break;
            }
            self.pos += 1;
        }
        if self.pos == start {
            return self.err("Expected a property name");
        }
        Ok(self.chars[start..self.pos].iter().collect())
    }

    fn quoted(&mut self) -> Result<String, PathError> {
        let quote = self.peek().unwrap_or('\'');
        self.pos += 1;
        let mut s = String::new();
        loop {
            match self.peek() {
                None => return self.err("Unterminated quoted string"),
                Some('\\') => {
                    self.pos += 1;
                    match self.peek() {
                        Some(c) => {
                            s.push(c);
                            self.pos += 1;
                        }
                        None => return self.err("Unterminated escape"),
                    }
                }
                Some(c) if c == quote => {
                    self.pos += 1;
                    return Ok(s);
                }
                Some(c) => {
                    s.push(c);
                    self.pos += 1;
                }
            }
        }
    }

    fn integer(&mut self) -> Option<i64> {
        let start = self.pos;
        if self.peek() == Some('-') {
            self.pos += 1;
        }
        while self.peek().is_some_and(|c| c.is_ascii_digit()) {
            self.pos += 1;
        }
        let s: String = self.chars[start..self.pos].iter().collect();
        match s.parse() {
            Ok(n) => Some(n),
            Err(_) => {
                self.pos = start;
                None
            }
        }
    }

    fn bracket(&mut self) -> Result<Segment, PathError> {
        self.pos += 1; // '['
        self.skip_ws();
        let seg = match self.peek() {
            Some('*') => {
                self.pos += 1;
                Segment::Wildcard
            }
            Some('?') => {
                self.pos += 1;
                self.skip_ws();
                if self.peek() != Some('(') {
                    return self.err("Expected '(' after '?'");
                }
                self.pos += 1;
                let expr = self.filter_or()?;
                self.skip_ws();
                if self.peek() != Some(')') {
                    return self.err("Expected ')' closing filter");
                }
                self.pos += 1;
                Segment::Filter(expr)
            }
            Some('\'') | Some('"') => {
                let mut names = vec![self.quoted()?];
                loop {
                    self.skip_ws();
                    if self.peek() != Some(',') {
                        break;
                    }
                    self.pos += 1;
                    self.skip_ws();
                    if !matches!(self.peek(), Some('\'') | Some('"')) {
                        return self.err("Expected a quoted property name");
                    }
                    names.push(self.quoted()?);
                }
                Segment::Names(names)
            }
            Some(':') => {
                self.pos += 1;
                self.skip_ws();
                let end = self.integer();
                Segment::Slice(None, end)
            }
            Some(c) if c == '-' || c.is_ascii_digit() => {
                let first = match self.integer() {
                    Some(n) => n,
                    None => return self.err("Invalid array index"),
                };
                self.skip_ws();
                if self.peek() == Some(':') {
                    self.pos += 1;
                    self.skip_ws();
                    let end = self.integer();
                    Segment::Slice(Some(first), end)
                } else {
                    let mut idx = vec![first];
                    loop {
                        self.skip_ws();
                        if self.peek() != Some(',') {
                            break;
                        }
                        self.pos += 1;
                        self.skip_ws();
                        match self.integer() {
                            Some(n) => idx.push(n),
                            None => return self.err("Invalid array index"),
                        }
                    }
                    Segment::Indexes(idx)
                }
            }
            _ => return self.err("Invalid bracket expression"),
        };
        self.skip_ws();
        if self.peek() != Some(']') {
            return self.err("Expected ']'");
        }
        self.pos += 1;
        Ok(seg)
    }

    fn filter_or(&mut self) -> Result<FilterExpr, PathError> {
        let mut left = self.filter_and()?;
        loop {
            self.skip_ws();
            if self.peek() == Some('|') && self.peek_at(1) == Some('|') {
                self.pos += 2;
                let right = self.filter_and()?;
                left = FilterExpr::Or(Box::new(left), Box::new(right));
            } else {
                return Ok(left);
            }
        }
    }

    fn filter_and(&mut self) -> Result<FilterExpr, PathError> {
        let mut left = self.filter_unary()?;
        loop {
            self.skip_ws();
            if self.peek() == Some('&') && self.peek_at(1) == Some('&') {
                self.pos += 2;
                let right = self.filter_unary()?;
                left = FilterExpr::And(Box::new(left), Box::new(right));
            } else {
                return Ok(left);
            }
        }
    }

    fn filter_unary(&mut self) -> Result<FilterExpr, PathError> {
        self.skip_ws();
        if self.peek() == Some('!') && self.peek_at(1) != Some('=') {
            self.pos += 1;
            return Ok(FilterExpr::Not(Box::new(self.filter_unary()?)));
        }
        if self.peek() == Some('(') {
            self.pos += 1;
            let inner = self.filter_or()?;
            self.skip_ws();
            if self.peek() != Some(')') {
                return self.err("Expected ')'");
            }
            self.pos += 1;
            return Ok(inner);
        }
        let left = self.operand()?;
        self.skip_ws();
        let op = match (self.peek(), self.peek_at(1)) {
            (Some('='), Some('=')) => Some((CmpOp::Eq, 2)),
            (Some('!'), Some('=')) => Some((CmpOp::Ne, 2)),
            (Some('<'), Some('=')) => Some((CmpOp::Le, 2)),
            (Some('>'), Some('=')) => Some((CmpOp::Ge, 2)),
            (Some('<'), _) => Some((CmpOp::Lt, 1)),
            (Some('>'), _) => Some((CmpOp::Gt, 1)),
            _ => None,
        };
        match op {
            Some((op, len)) => {
                self.pos += len;
                let right = self.operand()?;
                Ok(FilterExpr::Compare(left, op, right))
            }
            None => match left {
                Operand::Literal(_) => self.err("A filter needs a path or a comparison"),
                path => Ok(FilterExpr::Exists(path)),
            },
        }
    }

    fn operand(&mut self) -> Result<Operand, PathError> {
        self.skip_ws();
        match self.peek() {
            Some('@') => {
                self.pos += 1;
                Ok(Operand::Current(self.segments(true)?))
            }
            Some('$') => {
                self.pos += 1;
                Ok(Operand::Root(self.segments(true)?))
            }
            Some('\'') | Some('"') => Ok(Operand::Literal(Value::String(self.quoted()?))),
            Some(_) => {
                let start = self.pos;
                while self
                    .peek()
                    .is_some_and(|c| c.is_ascii_alphanumeric() || "+-.".contains(c))
                {
                    self.pos += 1;
                }
                let word: String = self.chars[start..self.pos].iter().collect();
                let lit = match word.as_str() {
                    "true" => Value::Bool(true),
                    "false" => Value::Bool(false),
                    "null" => Value::Null,
                    w => {
                        if let Ok(i) = w.parse::<i64>() {
                            Value::Number(Number::from(i))
                        } else if let Some(n) = w.parse::<f64>().ok().and_then(Number::from_f64) {
                            Value::Number(n)
                        } else {
                            return self.err("Invalid filter literal");
                        }
                    }
                };
                Ok(Operand::Literal(lit))
            }
            None => self.err("Unexpected end of filter"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doc() -> Value {
        json!({
            "store": {
                "book": [
                    {"category": "ref", "author": "Nigel", "price": 8.95},
                    {"category": "fiction", "author": "Evelyn", "price": 12.99, "isbn": "x"},
                    {"category": "fiction", "author": "Herman", "price": 8.99, "isbn": "y"}
                ],
                "bicycle": {"color": "red", "price": 19.95}
            },
            "weird key": 1,
            "n": null
        })
    }

    #[test]
    fn definite_paths() {
        let d = doc();
        assert_eq!(evaluate(&d, "$").unwrap(), d);
        assert_eq!(evaluate(&d, "$.store.bicycle.color").unwrap(), json!("red"));
        assert_eq!(
            evaluate(&d, "$['store']['bicycle']['color']").unwrap(),
            json!("red")
        );
        assert_eq!(evaluate(&d, "$[\"weird key\"]").unwrap(), json!(1));
        assert_eq!(
            evaluate(&d, "$.store.book[0].author").unwrap(),
            json!("Nigel")
        );
        assert_eq!(
            evaluate(&d, "$.store.book[-1].author").unwrap(),
            json!("Herman")
        );
        // A present null is a result, not a miss.
        assert_eq!(evaluate(&d, "$.n").unwrap(), Value::Null);
    }

    #[test]
    fn definite_miss_is_no_results() {
        let d = doc();
        assert_eq!(
            evaluate(&d, "$.store.missing").unwrap_err(),
            PathError::NoResults("$['store']['missing']".into())
        );
        assert!(matches!(
            evaluate(&d, "$.store.book[9]"),
            Err(PathError::NoResults(_))
        ));
        assert!(matches!(
            evaluate(&d, "$.store.bicycle.color.x"),
            Err(PathError::NoResults(_))
        ));
    }

    #[test]
    fn indefinite_paths_return_arrays() {
        let d = doc();
        assert_eq!(
            evaluate(&d, "$.store.book[*].author").unwrap(),
            json!(["Nigel", "Evelyn", "Herman"])
        );
        assert_eq!(
            evaluate(&d, "$.store.book[0,2].price").unwrap(),
            json!([8.95, 8.99])
        );
        assert_eq!(
            evaluate(&d, "$.store.book[1:].author").unwrap(),
            json!(["Evelyn", "Herman"])
        );
        assert_eq!(
            evaluate(&d, "$.store.book[:1].author").unwrap(),
            json!(["Nigel"])
        );
        assert_eq!(
            evaluate(&d, "$.store.book[-2:].author").unwrap(),
            json!(["Evelyn", "Herman"])
        );
        assert_eq!(evaluate(&d, "$..color").unwrap(), json!(["red"]));
        assert_eq!(
            evaluate(&d, "$.store..price")
                .unwrap()
                .as_array()
                .unwrap()
                .len(),
            4
        );
        // No match on an indefinite path is an empty array, not an error.
        assert_eq!(evaluate(&d, "$.store.book[*].nope").unwrap(), json!([]));
        assert_eq!(
            evaluate(&d, "$['store']['book','bicycle']")
                .unwrap()
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn filters() {
        let d = doc();
        assert_eq!(
            evaluate(&d, "$.store.book[?(@.price < 10)].author").unwrap(),
            json!(["Nigel", "Herman"])
        );
        assert_eq!(
            evaluate(&d, "$.store.book[?(@.isbn)].author").unwrap(),
            json!(["Evelyn", "Herman"])
        );
        assert_eq!(
            evaluate(
                &d,
                "$.store.book[?(@.category == 'fiction' && @.price > 10)].author"
            )
            .unwrap(),
            json!(["Evelyn"])
        );
        assert_eq!(
            evaluate(
                &d,
                "$.store.book[?(@.author == \"Nigel\" || @.price >= 12.99)].author"
            )
            .unwrap(),
            json!(["Nigel", "Evelyn"])
        );
        assert_eq!(
            evaluate(&d, "$.store.book[?(@.price > $.store.bicycle.price)]").unwrap(),
            json!([])
        );
        assert_eq!(
            evaluate(&d, "$.store.book[?(!(@.isbn))].author").unwrap(),
            json!(["Nigel"])
        );
    }

    #[test]
    fn invalid_paths() {
        for p in [
            "", "foo", "$.", "$[", "$.a[", "$.x[é]", "$[]", "$.a]", "$['a",
        ] {
            assert!(
                matches!(JsonPath::parse(p), Err(PathError::Invalid(_))),
                "{p}"
            );
        }
    }

    #[test]
    fn definiteness() {
        assert!(JsonPath::parse("$.a[0]['b']").unwrap().is_definite());
        assert!(JsonPath::parse("$.a[-1]").unwrap().is_definite());
        assert!(!JsonPath::parse("$.a[*]").unwrap().is_definite());
        assert!(!JsonPath::parse("$..a").unwrap().is_definite());
        assert!(!JsonPath::parse("$.a[0,1]").unwrap().is_definite());
        assert!(!JsonPath::parse("$.a[0:1]").unwrap().is_definite());
    }
}
