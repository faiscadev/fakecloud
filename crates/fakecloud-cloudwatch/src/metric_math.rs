//! A small, real (if reduced) CloudWatch metric-math evaluator.
//!
//! It supports the two patterns that show up in nearly every real
//! `GetMetricData` request that uses an `Expression`:
//!
//! - arithmetic over referenced metric ids and numeric literals
//!   (`m1+m2`, `m1/m2*100`, `(m1-m2)/m3`), aligned element-wise by timestamp;
//! - the array/aggregate functions `SUM`/`AVG`/`MIN`/`MAX`/`COUNT` applied
//!   either to a single metric id (collapsing its series to a scalar) or to an
//!   array of metrics (`SUM([m1,m2])`, element-wise across the array).
//!
//! It is intentionally not a full implementation of the metric-math grammar
//! (no `FILL`, `RATE`, `ANOMALY_DETECTION_BAND`, time functions, etc.) — those
//! return an `Err` so the caller can surface a real error instead of a
//! silently-empty result.

use std::collections::{BTreeMap, BTreeSet};

use chrono::{DateTime, Utc};

/// A time-aligned metric series: bucket timestamp -> value.
pub type Series = BTreeMap<DateTime<Utc>, f64>;

#[derive(Clone)]
enum Value {
    Scalar(f64),
    Series(Series),
    Array(Vec<Value>),
}

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Ident(String),
    Number(f64),
    Plus,
    Minus,
    Star,
    Slash,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Comma,
}

fn tokenize(expr: &str) -> Result<Vec<Token>, String> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = expr.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            ' ' | '\t' | '\n' | '\r' => {
                i += 1;
            }
            '+' => {
                tokens.push(Token::Plus);
                i += 1;
            }
            '-' => {
                tokens.push(Token::Minus);
                i += 1;
            }
            '*' => {
                tokens.push(Token::Star);
                i += 1;
            }
            '/' => {
                tokens.push(Token::Slash);
                i += 1;
            }
            '(' => {
                tokens.push(Token::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(Token::RParen);
                i += 1;
            }
            '[' => {
                tokens.push(Token::LBracket);
                i += 1;
            }
            ']' => {
                tokens.push(Token::RBracket);
                i += 1;
            }
            ',' => {
                tokens.push(Token::Comma);
                i += 1;
            }
            c if c.is_ascii_digit() || c == '.' => {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                let s: String = chars[start..i].iter().collect();
                let n = s
                    .parse::<f64>()
                    .map_err(|_| format!("invalid number '{s}'"))?;
                tokens.push(Token::Number(n));
            }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                tokens.push(Token::Ident(chars[start..i].iter().collect()));
            }
            other => return Err(format!("unexpected character '{other}' in expression")),
        }
    }
    Ok(tokens)
}

/// Maximum parser recursion depth. Guards against a stack overflow on a
/// pathologically deep expression (e.g. thousands of nested parentheses)
/// arriving from an untrusted `GetMetricData` request.
const MAX_DEPTH: usize = 256;

struct Parser<'a> {
    tokens: Vec<Token>,
    pos: usize,
    depth: usize,
    metrics: &'a BTreeMap<String, Series>,
}

impl Parser<'_> {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    /// Enter one level of recursion, erroring if the nesting limit is reached.
    fn enter(&mut self) -> Result<(), String> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err("expression nesting is too deep".to_string());
        }
        Ok(())
    }

    fn leave(&mut self) {
        self.depth -= 1;
    }

    fn next(&mut self) -> Option<Token> {
        let t = self.tokens.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn expect(&mut self, want: &Token) -> Result<(), String> {
        match self.next() {
            Some(ref t) if t == want => Ok(()),
            other => Err(format!("expected {want:?}, found {other:?}")),
        }
    }

    fn parse_expr(&mut self) -> Result<Value, String> {
        let mut left = self.parse_term()?;
        while let Some(op) = self.peek() {
            let op = match op {
                Token::Plus => '+',
                Token::Minus => '-',
                _ => break,
            };
            self.pos += 1;
            let right = self.parse_term()?;
            left = apply_op(op, left, right)?;
        }
        Ok(left)
    }

    fn parse_term(&mut self) -> Result<Value, String> {
        let mut left = self.parse_factor()?;
        while let Some(op) = self.peek() {
            let op = match op {
                Token::Star => '*',
                Token::Slash => '/',
                _ => break,
            };
            self.pos += 1;
            let right = self.parse_factor()?;
            left = apply_op(op, left, right)?;
        }
        Ok(left)
    }

    fn parse_factor(&mut self) -> Result<Value, String> {
        self.enter()?;
        let result = self.parse_factor_inner();
        self.leave();
        result
    }

    fn parse_factor_inner(&mut self) -> Result<Value, String> {
        match self.next() {
            Some(Token::Number(n)) => Ok(Value::Scalar(n)),
            Some(Token::Minus) => {
                // Unary minus.
                let v = self.parse_factor()?;
                apply_op('-', Value::Scalar(0.0), v)
            }
            Some(Token::LParen) => {
                let v = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(v)
            }
            Some(Token::LBracket) => {
                let mut items = Vec::new();
                if self.peek() != Some(&Token::RBracket) {
                    loop {
                        items.push(self.parse_expr()?);
                        match self.peek() {
                            Some(Token::Comma) => {
                                self.pos += 1;
                            }
                            _ => break,
                        }
                    }
                }
                self.expect(&Token::RBracket)?;
                Ok(Value::Array(items))
            }
            Some(Token::Ident(name)) => {
                // Function call when followed by `(`, otherwise a metric id.
                if self.peek() == Some(&Token::LParen) {
                    self.pos += 1;
                    let arg = self.parse_expr()?;
                    self.expect(&Token::RParen)?;
                    apply_func(&name, arg)
                } else {
                    match self.metrics.get(&name) {
                        Some(s) => Ok(Value::Series(s.clone())),
                        None => Err(format!("unknown metric id '{name}' in expression")),
                    }
                }
            }
            other => Err(format!("unexpected token {other:?} in expression")),
        }
    }
}

fn apply_op(op: char, left: Value, right: Value) -> Result<Value, String> {
    let f = |a: f64, b: f64| -> f64 {
        match op {
            '+' => a + b,
            '-' => a - b,
            '*' => a * b,
            '/' => {
                if b == 0.0 {
                    f64::NAN
                } else {
                    a / b
                }
            }
            _ => f64::NAN,
        }
    };
    match (left, right) {
        (Value::Scalar(a), Value::Scalar(b)) => Ok(Value::Scalar(f(a, b))),
        (Value::Series(s), Value::Scalar(b)) => Ok(Value::Series(
            s.into_iter().map(|(t, a)| (t, f(a, b))).collect(),
        )),
        (Value::Scalar(a), Value::Series(s)) => Ok(Value::Series(
            s.into_iter().map(|(t, b)| (t, f(a, b))).collect(),
        )),
        (Value::Series(a), Value::Series(b)) => {
            // Align on the timestamps present in both operands.
            let mut out = Series::new();
            for (t, av) in a.iter() {
                if let Some(bv) = b.get(t) {
                    out.insert(*t, f(*av, *bv));
                }
            }
            Ok(Value::Series(out))
        }
        _ => Err("arithmetic on metric arrays is not supported".to_string()),
    }
}

fn apply_func(name: &str, arg: Value) -> Result<Value, String> {
    let upper = name.to_ascii_uppercase();
    let reducer: fn(&[f64]) -> f64 = match upper.as_str() {
        "SUM" => |xs| xs.iter().sum(),
        "AVG" | "AVERAGE" => |xs| {
            if xs.is_empty() {
                f64::NAN
            } else {
                xs.iter().sum::<f64>() / xs.len() as f64
            }
        },
        "MIN" => |xs| xs.iter().cloned().fold(f64::INFINITY, f64::min),
        "MAX" => |xs| xs.iter().cloned().fold(f64::NEG_INFINITY, f64::max),
        "COUNT" => |xs| xs.len() as f64,
        other => return Err(format!("unsupported metric-math function '{other}'")),
    };

    match arg {
        // Aggregate over a single series collapses it to a scalar.
        Value::Series(s) => {
            let vals: Vec<f64> = s.values().cloned().collect();
            Ok(Value::Scalar(reducer(&vals)))
        }
        Value::Scalar(n) => Ok(Value::Scalar(reducer(&[n]))),
        // Aggregate across an array of series is element-wise per timestamp.
        Value::Array(items) => {
            let mut series: Vec<Series> = Vec::new();
            for item in items {
                match item {
                    Value::Series(s) => series.push(s),
                    Value::Scalar(_) => {
                        return Err("array aggregate over scalars is not supported".to_string())
                    }
                    Value::Array(_) => return Err("nested arrays are not supported".to_string()),
                }
            }
            let mut timestamps: BTreeSet<DateTime<Utc>> = BTreeSet::new();
            for s in &series {
                timestamps.extend(s.keys().cloned());
            }
            let mut out = Series::new();
            for t in timestamps {
                let vals: Vec<f64> = series.iter().filter_map(|s| s.get(&t).cloned()).collect();
                if !vals.is_empty() {
                    out.insert(t, reducer(&vals));
                }
            }
            Ok(Value::Series(out))
        }
    }
}

/// Evaluate `expr` against the referenced metric series. A scalar result (e.g.
/// `SUM(m1)`) is broadcast as a single datapoint at the earliest timestamp seen
/// across all referenced metrics, matching how CloudWatch plots a scalar
/// metric-math result.
pub fn evaluate(expr: &str, metrics: &BTreeMap<String, Series>) -> Result<Series, String> {
    let tokens = tokenize(expr)?;
    if tokens.is_empty() {
        return Err("empty expression".to_string());
    }
    let mut parser = Parser {
        tokens,
        pos: 0,
        depth: 0,
        metrics,
    };
    let value = parser.parse_expr()?;
    if parser.pos != parser.tokens.len() {
        return Err("unexpected trailing tokens in expression".to_string());
    }
    match value {
        Value::Series(s) => Ok(s),
        Value::Scalar(n) => {
            let earliest = metrics.values().flat_map(|s| s.keys()).min().cloned();
            let mut out = Series::new();
            if let Some(t) = earliest {
                out.insert(t, n);
            }
            Ok(out)
        }
        Value::Array(_) => Err("expression result is an array, not a single series".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(n: i64) -> DateTime<Utc> {
        DateTime::<Utc>::from_timestamp(n, 0).unwrap()
    }

    fn series(pairs: &[(i64, f64)]) -> Series {
        pairs.iter().map(|(t, v)| (ts(*t), *v)).collect()
    }

    #[test]
    fn adds_two_series() {
        let mut m = BTreeMap::new();
        m.insert("m1".to_string(), series(&[(0, 1.0), (60, 2.0)]));
        m.insert("m2".to_string(), series(&[(0, 10.0), (60, 20.0)]));
        let out = evaluate("m1+m2", &m).unwrap();
        assert_eq!(out.get(&ts(0)), Some(&11.0));
        assert_eq!(out.get(&ts(60)), Some(&22.0));
    }

    #[test]
    fn scalar_multiply() {
        let mut m = BTreeMap::new();
        m.insert("m1".to_string(), series(&[(0, 3.0)]));
        let out = evaluate("m1*100", &m).unwrap();
        assert_eq!(out.get(&ts(0)), Some(&300.0));
    }

    #[test]
    fn aggregate_single_series_to_scalar() {
        let mut m = BTreeMap::new();
        m.insert("m1".to_string(), series(&[(0, 1.0), (60, 2.0), (120, 3.0)]));
        let out = evaluate("SUM(m1)", &m).unwrap();
        // Broadcast at earliest timestamp.
        assert_eq!(out.get(&ts(0)), Some(&6.0));
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn aggregate_array_elementwise() {
        let mut m = BTreeMap::new();
        m.insert("m1".to_string(), series(&[(0, 1.0), (60, 2.0)]));
        m.insert("m2".to_string(), series(&[(0, 10.0), (60, 20.0)]));
        let out = evaluate("SUM([m1,m2])", &m).unwrap();
        assert_eq!(out.get(&ts(0)), Some(&11.0));
        assert_eq!(out.get(&ts(60)), Some(&22.0));
    }

    #[test]
    fn unknown_function_errors() {
        let m = BTreeMap::new();
        assert!(evaluate("RATE(m1)", &m).is_err());
    }

    #[test]
    fn unknown_id_errors() {
        let m = BTreeMap::new();
        assert!(evaluate("m1+m2", &m).is_err());
    }

    #[test]
    fn deeply_nested_parens_error_not_overflow() {
        // A pathologically deep expression must return an Err rather than
        // overflowing the stack.
        let m = BTreeMap::new();
        let expr = format!("{}1{}", "(".repeat(5000), ")".repeat(5000));
        let out = evaluate(&expr, &m);
        assert!(out.is_err());
    }
}
