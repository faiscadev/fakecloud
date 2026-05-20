//! dynamodb helpers `partiql` concerns (audit-2026-05-19).

use super::*;

pub(crate) fn compare_attribute_values(a: Option<&Value>, b: Option<&Value>) -> std::cmp::Ordering {
    match (a, b) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(a), Some(b)) => {
            let a_type = attribute_type_and_value(a);
            let b_type = attribute_type_and_value(b);
            match (a_type, b_type) {
                (Some(("S", a_val)), Some(("S", b_val))) => {
                    let a_str = a_val.as_str().unwrap_or("");
                    let b_str = b_val.as_str().unwrap_or("");
                    a_str.cmp(b_str)
                }
                (Some(("N", a_val)), Some(("N", b_val))) => {
                    let a_num: f64 = a_val.as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    let b_num: f64 = b_val.as_str().and_then(|s| s.parse().ok()).unwrap_or(0.0);
                    a_num
                        .partial_cmp(&b_num)
                        .unwrap_or(std::cmp::Ordering::Equal)
                }
                (Some(("B", a_val)), Some(("B", b_val))) => {
                    let a_str = a_val.as_str().unwrap_or("");
                    let b_str = b_val.as_str().unwrap_or("");
                    a_str.cmp(b_str)
                }
                _ => std::cmp::Ordering::Equal,
            }
        }
    }
}

pub(crate) fn execute_partiql_in_state(
    state: &mut crate::state::DynamoDbState,
    statement: &str,
    parameters: &[Value],
) -> Result<PartiqlOutcome, AwsServiceError> {
    let trimmed = statement.trim();
    let upper = trimmed.to_ascii_uppercase();

    if upper.starts_with("SELECT") {
        let from_pos = upper.find("FROM").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Invalid SELECT statement: missing FROM",
            )
        })?;
        let after_from = trimmed[from_pos + 4..].trim();
        let (table_name, rest) = parse_partiql_table_name(after_from);
        let table = get_table(&state.tables, &table_name)?;
        let rest_upper = rest.trim().to_ascii_uppercase();
        let items: Vec<Value> = if rest_upper.starts_with("WHERE") {
            let where_clause = rest.trim()[5..].trim();
            evaluate_partiql_where(table, where_clause, parameters)?
                .iter()
                .map(|item| json!(item))
                .collect()
        } else {
            table.items.iter().map(|item| json!(item)).collect()
        };
        Ok(PartiqlOutcome {
            response: json!({ "Items": items }),
            table_name: Some(table_name),
            event_name: None,
            keys: None,
            old_image: None,
            new_image: None,
        })
    } else if upper.starts_with("INSERT") {
        let into_pos = upper.find("INTO").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Invalid INSERT statement: missing INTO",
            )
        })?;
        let after_into = trimmed[into_pos + 4..].trim();
        let (table_name, rest) = parse_partiql_table_name(after_into);
        let rest_upper = rest.trim().to_ascii_uppercase();
        let value_pos = rest_upper.find("VALUE").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Invalid INSERT statement: missing VALUE",
            )
        })?;
        let value_str = rest.trim()[value_pos + 5..].trim();
        let item = parse_partiql_value_object(value_str, parameters)?;
        let table = get_table_mut(&mut state.tables, &table_name)?;
        validate_partiql_item_against_key_schema(table, &item)?;
        let key = extract_key(table, &item);
        if table.find_item_index(&key).is_some() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "DuplicateItemException",
                "Duplicate primary key exists in table",
            ));
        }
        table.items.push(item.clone());
        table.recalculate_stats();
        Ok(PartiqlOutcome {
            response: json!({}),
            table_name: Some(table_name),
            event_name: Some("INSERT".to_string()),
            keys: Some(key),
            old_image: None,
            new_image: Some(item),
        })
    } else if upper.starts_with("UPDATE") {
        let after_update = trimmed[6..].trim();
        let (table_name, rest) = parse_partiql_table_name(after_update);
        let rest_upper = rest.trim().to_ascii_uppercase();
        let set_pos = rest_upper.find("SET").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Invalid UPDATE statement: missing SET",
            )
        })?;
        let after_set = rest.trim()[set_pos + 3..].trim();
        let where_pos = after_set.to_ascii_uppercase().find("WHERE");
        let (set_clause, where_clause) = if let Some(wp) = where_pos {
            (&after_set[..wp], after_set[wp + 5..].trim())
        } else {
            (after_set, "")
        };
        let table = get_table_mut(&mut state.tables, &table_name)?;
        let matched_indices = if !where_clause.is_empty() {
            find_partiql_where_indices(table, where_clause, parameters)?
        } else {
            (0..table.items.len()).collect()
        };
        let param_offset = count_params_in_str(where_clause);
        let assignments: Vec<&str> = set_clause.split(',').collect();
        let mut last_key: Option<HashMap<String, AttributeValue>> = None;
        let mut last_old: Option<HashMap<String, AttributeValue>> = None;
        let mut last_new: Option<HashMap<String, AttributeValue>> = None;
        for idx in &matched_indices {
            last_old = Some(table.items[*idx].clone());
            let mut local_offset = param_offset;
            for assignment in &assignments {
                let assignment = assignment.trim();
                if let Some((attr, val_str)) = assignment.split_once('=') {
                    let attr = attr.trim().trim_matches('"');
                    let val_str = val_str.trim();
                    let value = parse_partiql_literal(val_str, parameters, &mut local_offset);
                    if let Some(v) = value {
                        table.items[*idx].insert(attr.to_string(), v);
                    }
                }
            }
            last_key = Some(extract_key(table, &table.items[*idx]));
            last_new = Some(table.items[*idx].clone());
        }
        table.recalculate_stats();
        Ok(PartiqlOutcome {
            response: json!({}),
            table_name: Some(table_name),
            event_name: last_old.as_ref().map(|_| "MODIFY".to_string()),
            keys: last_key,
            old_image: last_old,
            new_image: last_new,
        })
    } else if upper.starts_with("DELETE") {
        let from_pos = upper.find("FROM").ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Invalid DELETE statement: missing FROM",
            )
        })?;
        let after_from = trimmed[from_pos + 4..].trim();
        let (table_name, rest) = parse_partiql_table_name(after_from);
        let rest_upper = rest.trim().to_ascii_uppercase();
        if !rest_upper.starts_with("WHERE") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "DELETE requires a WHERE clause",
            ));
        }
        let where_clause = rest.trim()[5..].trim();
        let table = get_table_mut(&mut state.tables, &table_name)?;
        let mut indices = find_partiql_where_indices(table, where_clause, parameters)?;
        indices.sort_unstable();
        indices.reverse();
        let mut last_old: Option<HashMap<String, AttributeValue>> = None;
        let mut last_key: Option<HashMap<String, AttributeValue>> = None;
        for idx in indices {
            let removed = table.items.remove(idx);
            last_key = Some(extract_key(table, &removed));
            last_old = Some(removed);
        }
        table.recalculate_stats();
        Ok(PartiqlOutcome {
            response: json!({}),
            table_name: Some(table_name),
            event_name: last_old.as_ref().map(|_| "REMOVE".to_string()),
            keys: last_key,
            old_image: last_old,
            new_image: None,
        })
    } else {
        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            format!("Unsupported PartiQL statement: {trimmed}"),
        ))
    }
}

/// Parse a table name that may be quoted with double quotes.
/// Returns (table_name, rest_of_string).
pub(crate) fn parse_partiql_table_name(s: &str) -> (String, &str) {
    let s = s.trim();
    if let Some(stripped) = s.strip_prefix('"') {
        // Quoted name
        if let Some(end) = stripped.find('"') {
            let name = &stripped[..end];
            let rest = &stripped[end + 1..];
            (name.to_string(), rest)
        } else {
            let end = s.find(' ').unwrap_or(s.len());
            (s[..end].trim_matches('"').to_string(), &s[end..])
        }
    } else {
        let end = s.find(|c: char| c.is_whitespace()).unwrap_or(s.len());
        (s[..end].to_string(), &s[end..])
    }
}

/// Evaluate a simple WHERE clause: `col = 'value'` or `col = ?`
/// Returns matching items.
pub(crate) fn evaluate_partiql_where<'a>(
    table: &'a DynamoTable,
    where_clause: &str,
    parameters: &[Value],
) -> Result<Vec<&'a HashMap<String, AttributeValue>>, AwsServiceError> {
    let indices = find_partiql_where_indices(table, where_clause, parameters)?;
    Ok(indices.iter().map(|i| &table.items[*i]).collect())
}

pub(crate) fn find_partiql_where_indices(
    table: &DynamoTable,
    where_clause: &str,
    parameters: &[Value],
) -> Result<Vec<usize>, AwsServiceError> {
    // Try the full expression parser first — supports AND/OR/NOT and
    // parenthesized groups. If the clause doesn't parse cleanly we
    // fall back to the legacy AND-only path so older callers that
    // emit non-standard syntax keep matching zero rows instead of
    // 500-ing.
    let expr = parse_partiql_where_expr(where_clause, parameters);
    if let Some(expr) = expr {
        let mut indices = Vec::new();
        for (i, item) in table.items.iter().enumerate() {
            if evaluate_partiql_expr(&expr, item) {
                indices.push(i);
            }
        }
        return Ok(indices);
    }

    let conditions = split_partiql_and_clauses(where_clause);
    let parsed_conditions = parse_partiql_conditions(&conditions, parameters);
    // If any clause failed to parse, fall back to the structured parser
    // instead of running a `.all([])` -> match-all evaluation. AWS
    // rejects unparseable PartiQL with ValidationException; mirror that
    // rather than silently UPDATE/DELETE every row in the table.
    if parsed_conditions.len() != conditions.len() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationException",
            "Statement contains unsupported predicate(s); refusing match-all fallback",
        ));
    }

    let mut indices = Vec::new();
    for (i, item) in table.items.iter().enumerate() {
        let all_match = parsed_conditions
            .iter()
            .all(|c| evaluate_partiql_cond(c, item));
        if all_match {
            indices.push(i);
        }
    }

    Ok(indices)
}

pub(crate) fn evaluate_partiql_expr(
    expr: &PartiqlExpr,
    item: &HashMap<String, AttributeValue>,
) -> bool {
    match expr {
        PartiqlExpr::Cond(c) => evaluate_partiql_cond(c, item),
        PartiqlExpr::And(l, r) => evaluate_partiql_expr(l, item) && evaluate_partiql_expr(r, item),
        PartiqlExpr::Or(l, r) => evaluate_partiql_expr(l, item) || evaluate_partiql_expr(r, item),
        PartiqlExpr::Not(e) => !evaluate_partiql_expr(e, item),
    }
}

fn tokenize_partiql_where(where_clause: &str) -> Vec<WhereTok<'_>> {
    let bytes = where_clause.as_bytes();
    let upper = where_clause.to_ascii_uppercase();
    let upper_bytes = upper.as_bytes();
    let mut toks: Vec<WhereTok<'_>> = Vec::new();
    let mut i = 0usize;
    let mut atom_start: Option<usize> = None;
    let mut paren_depth: i32 = 0;
    let mut in_quote = false;
    let mut in_atom_paren = 0i32; // tracks `(...)` inside an atom

    fn push_atom<'a>(toks: &mut Vec<WhereTok<'a>>, src: &'a str, start: usize, end: usize) {
        let slice = src[start..end].trim();
        if !slice.is_empty() {
            toks.push(WhereTok::Atom(&src[start..end]));
        }
    }

    while i < bytes.len() {
        let c = bytes[i] as char;
        if in_quote {
            if c == '\'' {
                in_quote = false;
            }
            i += 1;
            continue;
        }
        if c == '\'' {
            if atom_start.is_none() {
                atom_start = Some(i);
            }
            in_quote = true;
            i += 1;
            continue;
        }

        // Inside an atom, track parens so begins_with(name, 'a') keeps
        // its inner `(` tied to the atom.
        if let Some(start) = atom_start {
            if c == '(' {
                in_atom_paren += 1;
                i += 1;
                continue;
            }
            if c == ')' {
                if in_atom_paren > 0 {
                    in_atom_paren -= 1;
                    i += 1;
                    continue;
                }
                // Top-level `)` closes a group — flush the atom first.
                push_atom(&mut toks, where_clause, start, i);
                atom_start = None;
                toks.push(WhereTok::RParen);
                paren_depth -= 1;
                i += 1;
                continue;
            }
            // Look for keyword boundaries: AND / OR / NOT surrounded
            // by whitespace.
            if c.is_whitespace() && in_atom_paren == 0 {
                if let Some((kw, len)) = match_where_keyword(upper_bytes, i) {
                    push_atom(&mut toks, where_clause, start, i);
                    atom_start = None;
                    toks.push(kw);
                    i += len;
                    continue;
                }
            }
            i += 1;
            continue;
        }

        // Not currently building an atom.
        if c.is_whitespace() {
            i += 1;
            continue;
        }
        if c == '(' {
            toks.push(WhereTok::LParen);
            paren_depth += 1;
            i += 1;
            continue;
        }
        if c == ')' {
            toks.push(WhereTok::RParen);
            paren_depth -= 1;
            i += 1;
            continue;
        }
        // Could be a leading NOT.
        if let Some((kw, len)) = match_where_keyword_at_start(upper_bytes, i) {
            toks.push(kw);
            i += len;
            continue;
        }
        atom_start = Some(i);
        i += 1;
    }

    if let Some(start) = atom_start {
        push_atom(&mut toks, where_clause, start, bytes.len());
    }

    if paren_depth != 0 || in_quote {
        return Vec::new();
    }
    toks
}

/// Match ` AND `, ` OR `, ` NOT ` starting at `i` (`i` is the leading
/// whitespace). Returns the token plus the consumed length so the
/// scanner can advance past the trailing whitespace too.
fn match_where_keyword(upper: &[u8], i: usize) -> Option<(WhereTok<'static>, usize)> {
    // Need: whitespace at i, then keyword, then whitespace or `(`.
    if i >= upper.len() || !(upper[i] as char).is_whitespace() {
        return None;
    }
    let after = i + 1;
    let try_kw = |kw: &[u8], tok: WhereTok<'static>| -> Option<(WhereTok<'static>, usize)> {
        if after + kw.len() > upper.len() {
            return None;
        }
        if &upper[after..after + kw.len()] != kw {
            return None;
        }
        let trail = after + kw.len();
        if trail >= upper.len() {
            return Some((tok, trail - i));
        }
        let next = upper[trail] as char;
        if next.is_whitespace() || next == '(' {
            Some((tok, trail - i))
        } else {
            None
        }
    };
    if let Some(r) = try_kw(b"AND", WhereTok::And) {
        return Some(r);
    }
    if let Some(r) = try_kw(b"OR", WhereTok::Or) {
        return Some(r);
    }
    if let Some(r) = try_kw(b"NOT", WhereTok::Not) {
        return Some(r);
    }
    None
}

/// Match a leading `NOT`/`AND`/`OR` (binary ops appear here when they
/// follow a `)` token without preceding whitespace). No leading
/// whitespace requirement — the caller has already skipped it.
fn match_where_keyword_at_start(upper: &[u8], i: usize) -> Option<(WhereTok<'static>, usize)> {
    let try_kw = |kw: &[u8], tok: WhereTok<'static>| -> Option<(WhereTok<'static>, usize)> {
        if i + kw.len() > upper.len() {
            return None;
        }
        if &upper[i..i + kw.len()] != kw {
            return None;
        }
        let trail = i + kw.len();
        if trail >= upper.len() {
            return Some((tok, kw.len()));
        }
        let next = upper[trail] as char;
        if next.is_whitespace() || next == '(' {
            Some((tok, kw.len()))
        } else {
            None
        }
    };
    if let Some(r) = try_kw(b"NOT", WhereTok::Not) {
        return Some(r);
    }
    if let Some(r) = try_kw(b"AND", WhereTok::And) {
        return Some(r);
    }
    if let Some(r) = try_kw(b"OR", WhereTok::Or) {
        return Some(r);
    }
    None
}

/// Parse a WHERE clause into [`PartiqlExpr`]. Returns `None` when the
/// clause has no logical operators OR fails to parse — callers fall
/// back to the legacy AND-only evaluator in that case.
pub(crate) fn parse_partiql_where_expr(
    where_clause: &str,
    parameters: &[Value],
) -> Option<PartiqlExpr> {
    let toks = tokenize_partiql_where(where_clause);
    if toks.is_empty() {
        return None;
    }
    let mut idx = 0usize;
    let mut param_idx = 0usize;
    let expr = parse_or(&toks, &mut idx, parameters, &mut param_idx)?;
    if idx != toks.len() {
        return None;
    }
    Some(expr)
}

fn parse_or(
    toks: &[WhereTok<'_>],
    i: &mut usize,
    params: &[Value],
    param_idx: &mut usize,
) -> Option<PartiqlExpr> {
    let mut left = parse_and(toks, i, params, param_idx)?;
    while matches!(toks.get(*i), Some(WhereTok::Or)) {
        *i += 1;
        let right = parse_and(toks, i, params, param_idx)?;
        left = PartiqlExpr::Or(Box::new(left), Box::new(right));
    }
    Some(left)
}

fn parse_and(
    toks: &[WhereTok<'_>],
    i: &mut usize,
    params: &[Value],
    param_idx: &mut usize,
) -> Option<PartiqlExpr> {
    let mut left = parse_not(toks, i, params, param_idx)?;
    while matches!(toks.get(*i), Some(WhereTok::And)) {
        *i += 1;
        let right = parse_not(toks, i, params, param_idx)?;
        left = PartiqlExpr::And(Box::new(left), Box::new(right));
    }
    Some(left)
}

fn parse_not(
    toks: &[WhereTok<'_>],
    i: &mut usize,
    params: &[Value],
    param_idx: &mut usize,
) -> Option<PartiqlExpr> {
    if matches!(toks.get(*i), Some(WhereTok::Not)) {
        *i += 1;
        let inner = parse_not(toks, i, params, param_idx)?;
        return Some(PartiqlExpr::Not(Box::new(inner)));
    }
    parse_primary(toks, i, params, param_idx)
}

fn parse_primary(
    toks: &[WhereTok<'_>],
    i: &mut usize,
    params: &[Value],
    param_idx: &mut usize,
) -> Option<PartiqlExpr> {
    match toks.get(*i)? {
        WhereTok::LParen => {
            *i += 1;
            let inner = parse_or(toks, i, params, param_idx)?;
            if !matches!(toks.get(*i), Some(WhereTok::RParen)) {
                return None;
            }
            *i += 1;
            Some(inner)
        }
        WhereTok::Atom(s) => {
            *i += 1;
            // Each atom may consume one or more `?` parameters; track
            // them globally so the order matches statement order.
            let cond = parse_one_partiql_condition(s.trim(), params, param_idx)?;
            Some(PartiqlExpr::Cond(cond))
        }
        _ => None,
    }
}

pub(crate) fn evaluate_partiql_cond(
    cond: &PartiqlCond,
    item: &HashMap<String, AttributeValue>,
) -> bool {
    match cond {
        PartiqlCond::Eq(a, v) => item.get(a) == Some(v),
        PartiqlCond::Ne(a, v) => item.get(a) != Some(v),
        PartiqlCond::Lt(a, v) => compare_attr(item.get(a), v).is_some_and(|c| c < 0),
        PartiqlCond::Le(a, v) => compare_attr(item.get(a), v).is_some_and(|c| c <= 0),
        PartiqlCond::Gt(a, v) => compare_attr(item.get(a), v).is_some_and(|c| c > 0),
        PartiqlCond::Ge(a, v) => compare_attr(item.get(a), v).is_some_and(|c| c >= 0),
        PartiqlCond::Between(a, lo, hi) => {
            let l = compare_attr(item.get(a), lo).is_some_and(|c| c >= 0);
            let r = compare_attr(item.get(a), hi).is_some_and(|c| c <= 0);
            l && r
        }
        PartiqlCond::In(a, vals) => match item.get(a) {
            Some(v) => vals.iter().any(|x| x == v),
            None => false,
        },
        PartiqlCond::Like(a, pattern) => {
            attr_string(item.get(a)).is_some_and(|s| match_like(&s, pattern))
        }
        PartiqlCond::BeginsWith(a, prefix) => attr_string(item.get(a))
            .zip(attr_string(Some(prefix)))
            .is_some_and(|(s, p)| s.starts_with(&p)),
        PartiqlCond::Contains(a, needle) => attr_string(item.get(a))
            .zip(attr_string(Some(needle)))
            .is_some_and(|(s, n)| s.contains(&n)),
        PartiqlCond::AttributeExists(a) => item.contains_key(a),
        PartiqlCond::AttributeNotExists(a) => !item.contains_key(a),
    }
}

/// Match a string against a PartiQL/SQL LIKE pattern. `%` matches any
/// run of characters (including empty), `_` matches exactly one
/// character. Both wildcards are anchored — `LIKE 'foo'` requires an
/// exact match, mirroring DDB PartiQL semantics.
pub(crate) fn match_like(s: &str, pattern: &str) -> bool {
    let s_chars: Vec<char> = s.chars().collect();
    let p_chars: Vec<char> = pattern.chars().collect();
    like_recurse(&s_chars, 0, &p_chars, 0)
}

fn like_recurse(s: &[char], si: usize, p: &[char], pi: usize) -> bool {
    if pi == p.len() {
        return si == s.len();
    }
    match p[pi] {
        '%' => {
            // Greedy backtracking: try matching 0..=remaining chars.
            for k in si..=s.len() {
                if like_recurse(s, k, p, pi + 1) {
                    return true;
                }
            }
            false
        }
        '_' => si < s.len() && like_recurse(s, si + 1, p, pi + 1),
        c => si < s.len() && s[si] == c && like_recurse(s, si + 1, p, pi + 1),
    }
}

/// Validate that every key-schema attribute is present in the item AND
/// that its AttributeValue carries the declared scalar type from
/// `attribute_definitions`. Real DDB rejects an INSERT or PutItem that
/// omits a key or supplies the wrong type with a `ValidationException`;
/// without the type check we'd silently accept e.g. `{'pk': 1}` for a
/// HASH key declared as `S`.
pub(crate) fn validate_partiql_item_against_key_schema(
    table: &DynamoTable,
    item: &HashMap<String, AttributeValue>,
) -> Result<(), AwsServiceError> {
    for key_attr in &table.key_schema {
        let Some(val) = item.get(&key_attr.attribute_name) else {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                format!(
                    "One or more parameter values were invalid: Missing the key {} in the item",
                    key_attr.attribute_name
                ),
            ));
        };
        // Type check against AttributeDefinitions. AWS only allows
        // S/N/B for key attribute types.
        let declared = table
            .attribute_definitions
            .iter()
            .find(|d| d.attribute_name == key_attr.attribute_name)
            .map(|d| d.attribute_type.as_str());
        if let Some(expected) = declared {
            let obj = val.as_object();
            let actual_tag = obj.and_then(|o| o.keys().next().map(|k| k.as_str()));
            if actual_tag != Some(expected) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationException",
                    format!(
                        "One or more parameter values were invalid: Type mismatch for key {} expected: {} actual: {}",
                        key_attr.attribute_name,
                        expected,
                        actual_tag.unwrap_or("?"),
                    ),
                ));
            }
        }
    }
    Ok(())
}

/// Three-way compare two AttributeValue payloads. Returns `Some(c)`
/// where the sign matches `lhs - rhs` (-1/0/+1), or `None` when the
/// comparison is undefined (mixed types, missing lhs, parse errors).
pub(crate) fn compare_attr(lhs: Option<&Value>, rhs: &Value) -> Option<i32> {
    let l = lhs?.as_object()?;
    let r = rhs.as_object()?;
    if let (Some(a), Some(b)) = (
        l.get("N").and_then(|v| v.as_str()),
        r.get("N").and_then(|v| v.as_str()),
    ) {
        let an: f64 = a.parse().ok()?;
        let bn: f64 = b.parse().ok()?;
        return Some(an.partial_cmp(&bn).map(|o| o as i32).unwrap_or(0));
    }
    if let (Some(a), Some(b)) = (
        l.get("S").and_then(|v| v.as_str()),
        r.get("S").and_then(|v| v.as_str()),
    ) {
        return Some(match a.cmp(b) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        });
    }
    None
}

/// Pull the underlying string out of a PartiQL string-typed
/// AttributeValue (`{"S": "..."}`).
pub(crate) fn attr_string(v: Option<&Value>) -> Option<String> {
    v?.as_object()?.get("S")?.as_str().map(|s| s.to_string())
}

/// Parse a list of `<expr>` clauses into [`PartiqlCond`] entries.
/// Conditions that don't parse are silently dropped — the WHERE
/// clause yields zero matches in that case rather than 500-ing.
pub(crate) fn parse_partiql_conditions(
    conditions: &[&str],
    parameters: &[Value],
) -> Vec<PartiqlCond> {
    let mut param_idx = 0usize;
    let mut parsed = Vec::new();
    for cond in conditions {
        if let Some(c) = parse_one_partiql_condition(cond.trim(), parameters, &mut param_idx) {
            parsed.push(c);
        }
    }
    parsed
}

fn parse_one_partiql_condition(
    cond: &str,
    parameters: &[Value],
    param_idx: &mut usize,
) -> Option<PartiqlCond> {
    let upper = cond.to_ascii_uppercase();

    // Function-style: begins_with(attr, val), contains(attr, val),
    // attribute_exists(attr), attribute_not_exists(attr).
    if let Some(arg) = strip_func(cond, &upper, "ATTRIBUTE_EXISTS") {
        return Some(PartiqlCond::AttributeExists(strip_attr(arg)));
    }
    if let Some(arg) = strip_func(cond, &upper, "ATTRIBUTE_NOT_EXISTS") {
        return Some(PartiqlCond::AttributeNotExists(strip_attr(arg)));
    }
    if let Some(args) = strip_func(cond, &upper, "BEGINS_WITH") {
        let (attr, val) = split_two_args(args, parameters, param_idx)?;
        return Some(PartiqlCond::BeginsWith(attr, val));
    }
    if let Some(args) = strip_func(cond, &upper, "CONTAINS") {
        let (attr, val) = split_two_args(args, parameters, param_idx)?;
        return Some(PartiqlCond::Contains(attr, val));
    }

    // BETWEEN: `attr BETWEEN lo AND hi`. The split-on-AND step
    // already preserved the inner AND, so we see the full clause.
    if let Some(b) = upper.find(" BETWEEN ") {
        let attr = cond[..b].trim().trim_matches('"').to_string();
        let rest = cond[b + 9..].trim();
        let rest_upper = rest.to_ascii_uppercase();
        if let Some(a) = rest_upper.find(" AND ") {
            let lo = parse_partiql_literal(rest[..a].trim(), parameters, param_idx)?;
            let hi = parse_partiql_literal(rest[a + 5..].trim(), parameters, param_idx)?;
            return Some(PartiqlCond::Between(attr, lo, hi));
        }
    }

    // IN: `attr IN (a, b, c)`.
    if let Some(i) = upper.find(" IN ") {
        let attr = cond[..i].trim().trim_matches('"').to_string();
        let after = cond[i + 4..].trim();
        let inner = after
            .strip_prefix('(')
            .and_then(|s| s.strip_suffix(')'))?
            .trim();
        let mut vals = Vec::new();
        for raw in inner.split(',') {
            if let Some(v) = parse_partiql_literal(raw.trim(), parameters, param_idx) {
                vals.push(v);
            }
        }
        return Some(PartiqlCond::In(attr, vals));
    }

    // LIKE: `attr LIKE 'pattern'` (with `%` and `_` wildcards). The
    // pattern is always a string; we unwrap the {"S": ...} payload at
    // parse time so the evaluator can stay scalar-only.
    if let Some(l) = upper.find(" LIKE ") {
        let attr = cond[..l].trim().trim_matches('"').to_string();
        let rhs = cond[l + 6..].trim();
        let pattern_val = parse_partiql_literal(rhs, parameters, param_idx)?;
        let pattern = pattern_val
            .as_object()
            .and_then(|o| o.get("S"))
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())?;
        return Some(PartiqlCond::Like(attr, pattern));
    }

    // Operator-style. Order matters: longest operator first so `<=`
    // doesn't get parsed as `<`.
    for op in ["<>", "<=", ">=", "<", ">", "="] {
        if let Some(idx) = cond.find(op) {
            let attr = cond[..idx].trim().trim_matches('"').to_string();
            let rhs = cond[idx + op.len()..].trim();

            // BETWEEN expressed as a chained `>=` AND `<=` is split on
            // " AND " upstream; the literal `BETWEEN x AND y` form is
            // handled below.
            let val = parse_partiql_literal(rhs, parameters, param_idx)?;
            return Some(match op {
                "=" => PartiqlCond::Eq(attr, val),
                "<>" => PartiqlCond::Ne(attr, val),
                "<=" => PartiqlCond::Le(attr, val),
                ">=" => PartiqlCond::Ge(attr, val),
                "<" => PartiqlCond::Lt(attr, val),
                ">" => PartiqlCond::Gt(attr, val),
                _ => return None,
            });
        }
    }

    None
}

fn strip_func<'a>(cond: &'a str, upper: &str, name: &str) -> Option<&'a str> {
    let prefix = format!("{name}(");
    if !upper.starts_with(&prefix) || !cond.ends_with(')') {
        return None;
    }
    Some(cond[prefix.len()..cond.len() - 1].trim())
}

fn strip_attr(s: &str) -> String {
    s.trim().trim_matches('"').to_string()
}

fn split_two_args(
    args: &str,
    parameters: &[Value],
    param_idx: &mut usize,
) -> Option<(String, Value)> {
    let (a, b) = args.split_once(',')?;
    let attr = strip_attr(a);
    let val = parse_partiql_literal(b.trim(), parameters, param_idx)?;
    Some((attr, val))
}

/// Split a PartiQL WHERE clause on case-insensitive ` AND ` boundaries.
/// Honors:
/// - `BETWEEN x AND y` — the inner AND must not split the clause
/// - `IN (a, b, c)` — internal commas/ANDs are inside parens, never matched
pub(crate) fn split_partiql_and_clauses(where_clause: &str) -> Vec<&str> {
    let upper = where_clause.to_uppercase();
    if !upper.contains(" AND ") {
        return vec![where_clause.trim()];
    }
    let mut parts = Vec::new();
    let mut last = 0usize;
    let mut paren_depth: i32 = 0;
    let mut in_quote = false;
    let bytes = where_clause.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i] as char;
        match c {
            '\'' => in_quote = !in_quote,
            '(' if !in_quote => paren_depth += 1,
            ')' if !in_quote => paren_depth -= 1,
            _ => {}
        }
        if !in_quote
            && paren_depth == 0
            && i + 5 <= bytes.len()
            && upper.as_bytes()[i..i + 5] == *b" AND "
        {
            // Suppress this AND when it's the inner AND of a
            // BETWEEN: search backward for the most recent BETWEEN
            // since the previous split point and require that no
            // sibling AND has appeared between them.
            let segment = &upper[last..i];
            let in_between = segment
                .rfind(" BETWEEN ")
                .is_some_and(|b| segment[b + 9..].find(" AND ").is_none());
            if !in_between {
                parts.push(where_clause[last..i].trim());
                last = i + 5;
                i += 5;
                continue;
            }
        }
        i += 1;
    }
    parts.push(where_clause[last..].trim());
    parts
}

/// Parse a PartiQL literal value. Supports:
/// - 'string' -> {"S": "string"}
/// - 123 -> {"N": "123"}
/// - ? -> parameter from list
pub(crate) fn parse_partiql_literal(
    s: &str,
    parameters: &[Value],
    param_idx: &mut usize,
) -> Option<Value> {
    let s = s.trim();
    if s == "?" {
        let idx = *param_idx;
        *param_idx += 1;
        parameters.get(idx).cloned()
    } else if s.starts_with('\'') && s.ends_with('\'') && s.len() >= 2 {
        let inner = &s[1..s.len() - 1];
        Some(json!({"S": inner}))
    } else if let Ok(n) = s.parse::<f64>() {
        let num_str = if n == n.trunc() {
            format!("{}", n as i64)
        } else {
            format!("{n}")
        };
        Some(json!({"N": num_str}))
    } else {
        None
    }
}

/// Parse a PartiQL VALUE object like `{'pk': 'val1', 'attr': 'val2'}` or with ? params.
pub(crate) fn parse_partiql_value_object(
    s: &str,
    parameters: &[Value],
) -> Result<HashMap<String, AttributeValue>, AwsServiceError> {
    let s = s.trim();
    let inner = s
        .strip_prefix('{')
        .and_then(|s| s.strip_suffix('}'))
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationException",
                "Invalid VALUE: expected object literal",
            )
        })?;

    let mut item = HashMap::new();
    let mut param_idx = 0usize;

    // Simple comma-separated key:value parsing
    for pair in split_partiql_pairs(inner) {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        if let Some((key_part, val_part)) = pair.split_once(':') {
            let key = key_part
                .trim()
                .trim_matches('\'')
                .trim_matches('"')
                .to_string();
            if let Some(val) = parse_partiql_literal(val_part.trim(), parameters, &mut param_idx) {
                item.insert(key, val);
            }
        }
    }

    Ok(item)
}

/// Split PartiQL object pairs on commas, respecting nested braces and quotes.
pub(crate) fn split_partiql_pairs(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut depth = 0;
    let mut in_quote = false;

    for (i, c) in s.char_indices() {
        match c {
            '\'' if !in_quote => in_quote = true,
            '\'' if in_quote => in_quote = false,
            '{' if !in_quote => depth += 1,
            '}' if !in_quote => depth -= 1,
            ',' if !in_quote && depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}
