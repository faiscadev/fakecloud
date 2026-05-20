//! `conditions` concerns from template.rs (audit-2026-05-19).

use super::*;

/// Walk the top-level `Conditions` block and evaluate each entry to a
/// boolean. Conditions can reference each other; we evaluate
/// recursively with memoization plus an `in_progress` set to surface a
/// clear error on cycles (`A` -> `B` -> `A`).
pub(super) fn evaluate_conditions(
    template: &Value,
    parameters: &BTreeMap<String, String>,
) -> Result<BTreeMap<String, bool>, String> {
    let mut memo: BTreeMap<String, bool> = BTreeMap::new();
    let Some(conds) = template.get("Conditions").and_then(|v| v.as_object()) else {
        return Ok(memo);
    };
    let mut in_progress: BTreeSet<String> = BTreeSet::new();
    let names: Vec<String> = conds.keys().cloned().collect();
    for name in names {
        evaluate_condition_named(&name, conds, parameters, &mut memo, &mut in_progress)?;
    }
    Ok(memo)
}

/// Resolve a single named condition, recursively walking its expression
/// tree. Memoizes into `memo`, tracks in-flight names in `in_progress`
/// to detect cycles. `Condition: <name>` references trigger recursion.
pub(super) fn evaluate_condition_named(
    name: &str,
    conds: &serde_json::Map<String, Value>,
    parameters: &BTreeMap<String, String>,
    memo: &mut BTreeMap<String, bool>,
    in_progress: &mut BTreeSet<String>,
) -> Result<bool, String> {
    if let Some(b) = memo.get(name) {
        return Ok(*b);
    }
    if !in_progress.insert(name.to_string()) {
        return Err(format!(
            "Circular reference in Conditions: '{name}' transitively references itself"
        ));
    }
    let expr = conds.get(name).ok_or_else(|| {
        format!("Condition '{name}' is referenced but not defined in Conditions block")
    })?;
    let result = eval_condition_expr(expr, conds, parameters, memo, in_progress)?;
    in_progress.remove(name);
    memo.insert(name.to_string(), result);
    Ok(result)
}

/// Evaluate a single condition expression node. Operators short-circuit
/// where it matters (`Fn::And` stops on first false, `Fn::Or` stops on
/// first true). Named-condition references recurse via
/// `evaluate_condition_named` so cycles are caught at the named layer.
pub(super) fn eval_condition_expr(
    expr: &Value,
    conds: &serde_json::Map<String, Value>,
    parameters: &BTreeMap<String, String>,
    memo: &mut BTreeMap<String, bool>,
    in_progress: &mut BTreeSet<String>,
) -> Result<bool, String> {
    if let Some(b) = expr.as_bool() {
        return Ok(b);
    }
    let map = expr
        .as_object()
        .ok_or_else(|| format!("Invalid condition expression: {expr}"))?;
    if let Some(args) = map.get("Fn::Equals").and_then(|v| v.as_array()) {
        if args.len() != 2 {
            return Err("Fn::Equals requires exactly 2 arguments".to_string());
        }
        let a = stringify_value(&args[0], parameters);
        let b = stringify_value(&args[1], parameters);
        return Ok(a == b);
    }
    if let Some(args) = map.get("Fn::And").and_then(|v| v.as_array()) {
        if !(1..=10).contains(&args.len()) {
            return Err("Fn::And requires between 1 and 10 conditions".to_string());
        }
        for a in args {
            if !eval_condition_expr(a, conds, parameters, memo, in_progress)? {
                return Ok(false);
            }
        }
        return Ok(true);
    }
    if let Some(args) = map.get("Fn::Or").and_then(|v| v.as_array()) {
        if !(1..=10).contains(&args.len()) {
            return Err("Fn::Or requires between 1 and 10 conditions".to_string());
        }
        for a in args {
            if eval_condition_expr(a, conds, parameters, memo, in_progress)? {
                return Ok(true);
            }
        }
        return Ok(false);
    }
    if let Some(arr) = map.get("Fn::Not").and_then(|v| v.as_array()) {
        if arr.len() != 1 {
            return Err("Fn::Not requires exactly 1 argument".to_string());
        }
        return Ok(!eval_condition_expr(
            &arr[0],
            conds,
            parameters,
            memo,
            in_progress,
        )?);
    }
    if let Some(name) = map.get("Condition").and_then(|v| v.as_str()) {
        return evaluate_condition_named(name, conds, parameters, memo, in_progress);
    }
    Err(format!("Unknown condition operator in expression: {expr}"))
}
