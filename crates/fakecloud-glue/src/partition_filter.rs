//! Minimal evaluator for Glue `GetPartitions.Expression` filters.
//!
//! Supports the common SQL-style predicates real callers ship:
//! `col = 'v'`, `col != 'v'`, `col > 'v'`, `col >= 'v'`, `col < 'v'`,
//! `col <= 'v'`, `col LIKE 'pat%'`, joined by `AND`/`OR` with optional
//! parentheses. String comparisons are case-sensitive; numeric values
//! parsed as f64 fall back to string compare on parse failure.
//!
//! Anything we can't parse (function calls, IN lists, BETWEEN, NOT) makes
//! the filter return `true` for every partition — i.e. we degrade to
//! "no filter" rather than silently pruning real data.

use crate::state::Column;

pub fn matches(expression: &str, partition_keys: &[Column], values: &[String]) -> bool {
    let expr = expression.trim();
    if expr.is_empty() {
        return true;
    }
    match parse_or(expr) {
        Some(node) => eval(&node, partition_keys, values),
        None => true,
    }
}

#[derive(Debug)]
enum Node {
    Or(Box<Node>, Box<Node>),
    And(Box<Node>, Box<Node>),
    Cmp { col: String, op: Op, value: String },
}

#[derive(Debug, Clone, Copy)]
enum Op {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    Like,
}

fn parse_or(s: &str) -> Option<Node> {
    let s = strip_outer_parens(s.trim());
    if let Some((l, r)) = split_top_level(s, " OR ") {
        let left = parse_or(l)?;
        let right = parse_or(r)?;
        return Some(Node::Or(Box::new(left), Box::new(right)));
    }
    parse_and(s)
}

fn parse_and(s: &str) -> Option<Node> {
    let trimmed = s.trim();
    let stripped = strip_outer_parens(trimmed);
    // If outer parens were peeled, the inside may contain a top-level OR.
    if stripped != trimmed {
        return parse_or(stripped);
    }
    if let Some((l, r)) = split_top_level(stripped, " AND ") {
        let left = parse_and(l)?;
        let right = parse_and(r)?;
        return Some(Node::And(Box::new(left), Box::new(right)));
    }
    parse_cmp(stripped)
}

fn parse_cmp(s: &str) -> Option<Node> {
    let s = strip_outer_parens(s.trim());
    // Order matters: longer ops first.
    for (op_str, op) in [
        (" LIKE ", Op::Like),
        (">=", Op::Ge),
        ("<=", Op::Le),
        ("!=", Op::Ne),
        ("<>", Op::Ne),
        ("=", Op::Eq),
        (">", Op::Gt),
        ("<", Op::Lt),
    ] {
        if let Some(idx) = find_top_level(s, op_str) {
            let col = s[..idx].trim().trim_matches('`').to_string();
            let raw = s[idx + op_str.len()..].trim();
            let value = strip_quotes(raw).to_string();
            if col.is_empty() {
                return None;
            }
            return Some(Node::Cmp { col, op, value });
        }
    }
    None
}

fn eval(node: &Node, keys: &[Column], values: &[String]) -> bool {
    match node {
        Node::Or(l, r) => eval(l, keys, values) || eval(r, keys, values),
        Node::And(l, r) => eval(l, keys, values) && eval(r, keys, values),
        Node::Cmp { col, op, value } => {
            let Some(idx) = keys.iter().position(|k| k.name.eq_ignore_ascii_case(col)) else {
                return false;
            };
            let Some(actual) = values.get(idx) else {
                return false;
            };
            cmp(actual, *op, value)
        }
    }
}

fn cmp(actual: &str, op: Op, expected: &str) -> bool {
    if let Op::Like = op {
        return like_match(actual, expected);
    }
    if let (Ok(a), Ok(b)) = (actual.parse::<f64>(), expected.parse::<f64>()) {
        return match op {
            Op::Eq => (a - b).abs() < f64::EPSILON,
            Op::Ne => (a - b).abs() >= f64::EPSILON,
            Op::Gt => a > b,
            Op::Ge => a >= b,
            Op::Lt => a < b,
            Op::Le => a <= b,
            Op::Like => unreachable!(),
        };
    }
    match op {
        Op::Eq => actual == expected,
        Op::Ne => actual != expected,
        Op::Gt => actual > expected,
        Op::Ge => actual >= expected,
        Op::Lt => actual < expected,
        Op::Le => actual <= expected,
        Op::Like => unreachable!(),
    }
}

fn like_match(actual: &str, pattern: &str) -> bool {
    let mut regex = String::with_capacity(pattern.len() + 4);
    regex.push('^');
    for c in pattern.chars() {
        match c {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            ch if ch.is_alphanumeric() => regex.push(ch),
            ch => {
                regex.push('\\');
                regex.push(ch);
            }
        }
    }
    regex.push('$');
    regex::Regex::new(&regex)
        .map(|r| r.is_match(actual))
        .unwrap_or(false)
}

fn strip_outer_parens(s: &str) -> &str {
    let s = s.trim();
    if s.starts_with('(') && s.ends_with(')') {
        let inner = &s[1..s.len() - 1];
        // Only strip if the parens are matched at the boundary.
        let mut depth = 0i32;
        for (i, c) in inner.char_indices() {
            if c == '(' {
                depth += 1;
            } else if c == ')' {
                depth -= 1;
                if depth < 0 && i + 1 < inner.len() {
                    return s;
                }
            }
        }
        if depth == 0 {
            return inner.trim();
        }
    }
    s
}

fn strip_quotes(s: &str) -> &str {
    let s = s.trim();
    if (s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2)
        || (s.starts_with('"') && s.ends_with('"') && s.len() >= 2)
    {
        return &s[1..s.len() - 1];
    }
    s
}

fn split_top_level<'a>(s: &'a str, sep: &str) -> Option<(&'a str, &'a str)> {
    let idx = find_top_level(s, sep)?;
    Some((&s[..idx], &s[idx + sep.len()..]))
}

fn find_top_level(s: &str, sep: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    let sep_bytes = sep.as_bytes();
    let mut depth = 0i32;
    let mut in_quote: Option<u8> = None;
    let mut i = 0usize;
    while i < bytes.len() {
        let b = bytes[i];
        if let Some(q) = in_quote {
            if b == q {
                in_quote = None;
            }
            i += 1;
            continue;
        }
        if b == b'\'' || b == b'"' {
            in_quote = Some(b);
            i += 1;
            continue;
        }
        if b == b'(' {
            depth += 1;
            i += 1;
            continue;
        }
        if b == b')' {
            depth -= 1;
            i += 1;
            continue;
        }
        if depth == 0 && bytes[i..].len() >= sep_bytes.len() {
            // Case-insensitive match for keyword separators (" AND ", " OR ", " LIKE ").
            let chunk = &bytes[i..i + sep_bytes.len()];
            if chunk.eq_ignore_ascii_case(sep_bytes) {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys() -> Vec<Column> {
        vec![
            Column {
                name: "year".to_string(),
                column_type: "string".to_string(),
                comment: None,
                parameters: Default::default(),
            },
            Column {
                name: "month".to_string(),
                column_type: "string".to_string(),
                comment: None,
                parameters: Default::default(),
            },
        ]
    }

    fn vals(y: &str, m: &str) -> Vec<String> {
        vec![y.to_string(), m.to_string()]
    }

    #[test]
    fn empty_expression_matches_all() {
        assert!(matches("", &keys(), &vals("2024", "01")));
    }

    #[test]
    fn equality_match() {
        assert!(matches("year = '2024'", &keys(), &vals("2024", "01")));
        assert!(!matches("year = '2023'", &keys(), &vals("2024", "01")));
    }

    #[test]
    fn and_combination() {
        let e = "year = '2024' AND month = '01'";
        assert!(matches(e, &keys(), &vals("2024", "01")));
        assert!(!matches(e, &keys(), &vals("2024", "02")));
    }

    #[test]
    fn or_combination() {
        let e = "month = '01' OR month = '02'";
        assert!(matches(e, &keys(), &vals("2024", "01")));
        assert!(matches(e, &keys(), &vals("2024", "02")));
        assert!(!matches(e, &keys(), &vals("2024", "03")));
    }

    #[test]
    fn numeric_comparisons() {
        let e = "month >= '02' AND month < '06'";
        assert!(matches(e, &keys(), &vals("2024", "03")));
        assert!(!matches(e, &keys(), &vals("2024", "01")));
        assert!(!matches(e, &keys(), &vals("2024", "06")));
    }

    #[test]
    fn like_wildcard() {
        let e = "year LIKE '202%'";
        assert!(matches(e, &keys(), &vals("2024", "01")));
        assert!(!matches(e, &keys(), &vals("1999", "01")));
    }

    #[test]
    fn unknown_column_no_match() {
        assert!(!matches("nope = 'x'", &keys(), &vals("2024", "01")));
    }

    #[test]
    fn unparseable_expression_returns_all() {
        assert!(matches("year IN ('2024')", &keys(), &vals("2024", "01")));
    }

    #[test]
    fn parens_supported() {
        let e = "(year = '2024' OR year = '2025') AND month = '01'";
        assert!(matches(e, &keys(), &vals("2025", "01")));
        assert!(!matches(e, &keys(), &vals("2025", "02")));
    }
}
