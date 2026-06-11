//! Corpus-driven tests for DynamoDB expression grammars.
//!
//! Cases are seeded from two reference sources: `aws-sdk-go-v2`'s expression
//! builder (every compound shape real SDK callers send on the wire) and
//! `moto`'s parser (canonical grammar documented via its state machine and
//! error messages). One `#[test]` per dimension of the grammar so a failure
//! points at exactly one shape. New shapes discovered in the wild should be
//! added here first, then fixed.

#![cfg(test)]

use std::collections::HashMap;

use serde_json::{json, Value};

use super::{
    apply_update_expression, evaluate_condition, evaluate_filter_expression,
    evaluate_key_condition, AttributeValue,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn s(v: &str) -> Value {
    json!({ "S": v })
}
fn n(v: &str) -> Value {
    json!({ "N": v })
}
fn b(v: bool) -> Value {
    json!({ "BOOL": v })
}
fn ss(vals: &[&str]) -> Value {
    json!({ "SS": vals })
}
fn l(vals: Vec<Value>) -> Value {
    json!({ "L": vals })
}
fn m(pairs: &[(&str, Value)]) -> Value {
    let mut obj = serde_json::Map::new();
    for (k, v) in pairs {
        obj.insert((*k).to_string(), v.clone());
    }
    json!({ "M": obj })
}

fn item(pairs: &[(&str, Value)]) -> HashMap<String, AttributeValue> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

fn names(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

fn values(pairs: &[(&str, Value)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), v.clone()))
        .collect()
}

// ---------------------------------------------------------------------------
// KeyConditionExpression — operators & paren shapes
// ---------------------------------------------------------------------------

#[test]
fn key_condition_comparison_operators_bare() {
    // Each of the five legal KeyCondition comparison operators against a
    // range-key value of "orderN". The store has order_id = "order5".
    let it = item(&[("store_id", s("s1")), ("order_id", s("order5"))]);
    let names = HashMap::new();
    let values = values(&[
        (":s", s("s1")),
        (":v", s("order5")),
        (":lt", s("order6")),
        (":gt", s("order4")),
    ]);

    let cases = [
        ("equal", "store_id = :s AND order_id = :v", true),
        ("less-than", "store_id = :s AND order_id < :lt", true),
        ("greater-than", "store_id = :s AND order_id > :gt", true),
        ("less-or-equal", "store_id = :s AND order_id <= :v", true),
        ("greater-or-equal", "store_id = :s AND order_id >= :v", true),
    ];
    for (label, expr, expected) in cases {
        assert_eq!(
            evaluate_key_condition(expr, &it, &names, &values),
            expected,
            "key cond operator '{label}' failed for `{expr}`"
        );
    }
}

#[test]
fn key_condition_comparison_operators_parenthesized() {
    // Same as above but each clause wrapped in parens — the SDK builder shape.
    let it = item(&[("store_id", s("s1")), ("order_id", s("order5"))]);
    let names = HashMap::new();
    let values = values(&[
        (":s", s("s1")),
        (":v", s("order5")),
        (":lt", s("order6")),
        (":gt", s("order4")),
    ]);

    let cases = [
        ("equal", "(store_id = :s) AND (order_id = :v)", true),
        ("less-than", "(store_id = :s) AND (order_id < :lt)", true),
        ("greater-than", "(store_id = :s) AND (order_id > :gt)", true),
        (
            "less-or-equal",
            "(store_id = :s) AND (order_id <= :v)",
            true,
        ),
        (
            "greater-or-equal",
            "(store_id = :s) AND (order_id >= :v)",
            true,
        ),
    ];
    for (label, expr, expected) in cases {
        assert_eq!(
            evaluate_key_condition(expr, &it, &names, &values),
            expected,
            "parenthesized key cond operator '{label}' failed for `{expr}`"
        );
    }
}

#[test]
fn key_condition_sdk_builder_placeholder_shape() {
    // The literal shape aws-sdk-go-v2's KeyConditionBuilder emits:
    //   "(#0 = :0) AND (#1 < :1)"
    let it = item(&[("store_id", s("s1")), ("order_id", s("order5"))]);
    let names = names(&[("#0", "store_id"), ("#1", "order_id")]);
    let values = values(&[(":0", s("s1")), (":1", s("order6"))]);

    assert!(evaluate_key_condition(
        "(#0 = :0) AND (#1 < :1)",
        &it,
        &names,
        &values,
    ));
}

#[test]
fn key_condition_begins_with_both_spacings() {
    // aws-sdk-go-v2 emits "begins_with ($c, $c)" — space before paren.
    // Hand-crafted expressions use "begins_with($c, $c)" — no space.
    // Both must work.
    let it = item(&[("store_id", s("s1")), ("order_id", s("order5"))]);
    let names = HashMap::new();
    let values = values(&[(":s", s("s1")), (":p", s("order"))]);

    for expr in [
        "store_id = :s AND begins_with(order_id, :p)",
        "store_id = :s AND begins_with (order_id, :p)",
        "(store_id = :s) AND (begins_with(order_id, :p))",
        "(store_id = :s) AND (begins_with (order_id, :p))",
    ] {
        assert!(
            evaluate_key_condition(expr, &it, &names, &values),
            "begins_with shape failed: `{expr}`"
        );
    }
}

#[test]
fn key_condition_between() {
    let it = item(&[("store_id", s("s1")), ("order_id", n("50"))]);
    let names = HashMap::new();
    let values = values(&[(":s", s("s1")), (":lo", n("10")), (":hi", n("99"))]);

    for expr in [
        "store_id = :s AND order_id BETWEEN :lo AND :hi",
        "(store_id = :s) AND (order_id BETWEEN :lo AND :hi)",
        "store_id = :s AND (order_id BETWEEN :lo AND :hi)",
    ] {
        assert!(
            evaluate_key_condition(expr, &it, &names, &values),
            "BETWEEN shape failed: `{expr}`"
        );
    }
}

#[test]
fn key_condition_whitespace_variations() {
    let it = item(&[("store_id", s("s1")), ("order_id", s("o1"))]);
    let names = HashMap::new();
    let values = values(&[(":s", s("s1")), (":o", s("o1"))]);

    // Extra whitespace in various places should not matter.
    for expr in [
        "store_id=:s AND order_id=:o",
        "store_id =:s AND order_id= :o",
        "  store_id = :s   AND   order_id = :o  ",
        "store_id\t=\t:s\tAND\torder_id\t=\t:o",
        "store_id = :s\nAND\norder_id = :o",
    ] {
        assert!(
            evaluate_key_condition(expr, &it, &names, &values),
            "whitespace variation failed: `{expr:?}`"
        );
    }
}

#[test]
fn key_condition_hash_only_table() {
    // Tables without a range key should work fine with a single clause.
    let it = item(&[("pk", s("x"))]);
    let names = HashMap::new();
    let values = values(&[(":pk", s("x"))]);

    for expr in ["pk = :pk", "(pk = :pk)", "((pk = :pk))"] {
        assert!(
            evaluate_key_condition(expr, &it, &names, &values),
            "hash-only failed: `{expr}`"
        );
    }
}

// ---------------------------------------------------------------------------
// FilterExpression — the full DynamoDB comparison/function surface
// ---------------------------------------------------------------------------

#[test]
fn filter_all_comparison_operators() {
    let it = item(&[("x", n("10"))]);
    let names = HashMap::new();
    let values = values(&[
        (":eq", n("10")),
        (":ne", n("11")),
        (":lt", n("20")),
        (":le", n("10")),
        (":gt", n("5")),
        (":ge", n("10")),
    ]);

    let cases = [
        ("eq", "x = :eq", true),
        ("ne", "x <> :ne", true),
        ("lt", "x < :lt", true),
        ("le", "x <= :le", true),
        ("gt", "x > :gt", true),
        ("ge", "x >= :ge", true),
    ];
    for (label, expr, expected) in cases {
        assert_eq!(
            evaluate_filter_expression(expr, &it, &names, &values),
            expected,
            "filter op '{label}' failed for `{expr}`"
        );
    }
}

#[test]
fn filter_sdk_compound_shapes() {
    // aws-sdk-go-v2 condition builder emits:
    //   ($c) AND ($c)
    //   ($c) AND ($c) AND ($c)
    //   ($c) OR ($c)
    //   ($c) OR ($c) OR ($c) OR ($c)
    //   NOT ($c)
    let it = item(&[("a", n("1")), ("b", n("2")), ("c", n("3")), ("d", n("4"))]);
    let names = HashMap::new();
    let values = values(&[
        (":1", n("1")),
        (":2", n("2")),
        (":3", n("3")),
        (":4", n("4")),
        (":99", n("99")),
    ]);

    let cases = [
        ("2-and", "(a = :1) AND (b = :2)", true),
        ("3-and", "(a = :1) AND (b = :2) AND (c = :3)", true),
        (
            "4-and",
            "(a = :1) AND (b = :2) AND (c = :3) AND (d = :4)",
            true,
        ),
        ("2-or-hit-first", "(a = :1) OR (b = :99)", true),
        ("2-or-hit-second", "(a = :99) OR (b = :2)", true),
        ("2-or-miss", "(a = :99) OR (b = :99)", false),
        (
            "4-or",
            "(a = :99) OR (b = :99) OR (c = :99) OR (d = :4)",
            true,
        ),
        ("not-true", "NOT (a = :99)", true),
        ("not-false", "NOT (a = :1)", false),
        // No-space `NOT(` form (python_dynamodb_lock / hand-written).
        ("not-true-no-space", "NOT(a = :99)", true),
        ("not-false-no-space", "NOT(a = :1)", false),
        (
            "and-of-or",
            "((a = :1) OR (a = :99)) AND ((b = :2) OR (b = :99))",
            true,
        ),
        (
            "or-of-and",
            "((a = :99) AND (b = :2)) OR ((a = :1) AND (b = :99))",
            false,
        ),
    ];
    for (label, expr, expected) in cases {
        assert_eq!(
            evaluate_filter_expression(expr, &it, &names, &values),
            expected,
            "filter SDK-shape '{label}' failed for `{expr}`"
        );
    }
}

#[test]
fn filter_functions_with_sdk_space_before_paren() {
    // aws-sdk-go-v2 emits a space before the open-paren for these 6 functions:
    //   attribute_exists (X), attribute_not_exists (X), attribute_type (X, Y),
    //   begins_with (X, Y), contains (X, Y), size (X).
    // Hand-crafted callers usually omit the space. Both must work.
    //
    // `count` is a length-10 string so size() returns 10; size() on an N-type
    // attribute is invalid per AWS and must evaluate to false (see
    // filter_size_on_numeric_is_invalid).
    let it = item(&[
        ("name", s("widget-42")),
        ("tags", ss(&["red", "blue"])),
        ("count", s("1234567890")),
    ]);
    let names = HashMap::new();
    let values = values(&[
        (":prefix", s("widget")),
        (":tag", s("red")),
        (":ten", n("10")),
        (":t", s("S")),
    ]);

    let cases = [
        ("exists-no-space", "attribute_exists(name)", true),
        ("exists-space", "attribute_exists (name)", true),
        ("not-exists-no-space", "attribute_not_exists(missing)", true),
        ("not-exists-space", "attribute_not_exists (missing)", true),
        ("begins_with-no-space", "begins_with(name, :prefix)", true),
        ("begins_with-space", "begins_with (name, :prefix)", true),
        ("contains-string-no-space", "contains(name, :prefix)", true),
        ("contains-string-space", "contains (name, :prefix)", true),
        ("contains-set-no-space", "contains(tags, :tag)", true),
        ("contains-set-space", "contains (tags, :tag)", true),
        ("attribute_type-no-space", "attribute_type(name, :t)", true),
        ("attribute_type-space", "attribute_type (name, :t)", true),
        ("size-no-space", "size(count) = :ten", true),
        ("size-space", "size (count) = :ten", true),
    ];
    for (label, expr, expected) in cases {
        assert_eq!(
            evaluate_filter_expression(expr, &it, &names, &values),
            expected,
            "filter fn-shape '{label}' failed for `{expr}`"
        );
    }
}

#[test]
fn filter_size_on_numeric_is_invalid() {
    // size() is defined for S, B, SS, NS, BS, L, M per AWS docs; calling it on
    // N, BOOL, or NULL is a type error that evaluates to false (matching AWS's
    // silent filter-out semantics in FilterExpression context).
    let it = item(&[("count", n("10")), ("active", b(true))]);
    let names = HashMap::new();
    let values = values(&[(":ten", n("10")), (":one", n("1"))]);

    let cases = [
        ("size-on-n-eq", "size(count) = :ten", false),
        ("size-on-n-gt", "size(count) > :one", false),
        ("size-on-bool", "size(active) = :one", false),
        ("size-on-missing", "size(ghost) = :one", false),
    ];
    for (label, expr, expected) in cases {
        assert_eq!(
            evaluate_filter_expression(expr, &it, &names, &values),
            expected,
            "size-invalid '{label}' failed for `{expr}`"
        );
    }
}

#[test]
fn filter_in_operator() {
    // IN accepts 1–100 values per real DynamoDB. Builder emits: "$c IN ($c, $c)".
    let it = item(&[("status", s("active"))]);
    let names = HashMap::new();
    let values = values(&[
        (":a", s("active")),
        (":i", s("inactive")),
        (":p", s("pending")),
    ]);

    let cases = [
        ("in-2-hit", "status IN (:a, :i)", true),
        ("in-2-miss", "status IN (:i, :p)", false),
        ("in-3-hit-last", "status IN (:p, :i, :a)", true),
        ("in-single", "status IN (:a)", true),
        // Hand-crafted no-space-after-comma form.
        ("in-no-space-hit", "status IN (:i,:a)", true),
        // Parenthesized IN in a compound expression.
        (
            "paren-in-compound",
            "(status IN (:a, :i)) AND (status = :a)",
            true,
        ),
    ];
    for (label, expr, expected) in cases {
        assert_eq!(
            evaluate_filter_expression(expr, &it, &names, &values),
            expected,
            "filter IN '{label}' failed for `{expr}`"
        );
    }
}

#[test]
fn filter_between() {
    let it = item(&[("x", n("50"))]);
    let names = HashMap::new();
    let values = values(&[(":lo", n("10")), (":hi", n("100"))]);

    let cases = [
        ("between-inside", "x BETWEEN :lo AND :hi", true),
        ("between-lower-edge", "x BETWEEN :lo AND :lo", false),
        ("between-wrapped", "(x BETWEEN :lo AND :hi)", true),
        (
            "between-in-compound",
            "(x BETWEEN :lo AND :hi) AND (x = :lo)",
            false,
        ),
    ];
    for (label, expr, expected) in cases {
        assert_eq!(
            evaluate_filter_expression(expr, &it, &names, &values),
            expected,
            "filter BETWEEN '{label}' failed for `{expr}`"
        );
    }
}

#[test]
fn filter_sdk_builder_claim_lease_shape() {
    // Representative compound-condition pattern with nested parens, OR
    // inside AND, and the SDK's space-before-paren function syntax:
    //   (attribute_exists (#0)) AND ((attribute_not_exists (#1)) OR (#1 = :0))
    let names = names(&[("#0", "store_id"), ("#1", "lease_holder")]);
    let values = values(&[(":0", s("tab-A"))]);
    let expr = "(attribute_exists (#0)) AND ((attribute_not_exists (#1)) OR (#1 = :0))";

    let free = item(&[("store_id", s("s1"))]);
    assert!(
        evaluate_filter_expression(expr, &free, &names, &values),
        "claim-lease: free slot should match"
    );

    let self_held = item(&[("store_id", s("s1")), ("lease_holder", s("tab-A"))]);
    assert!(
        evaluate_filter_expression(expr, &self_held, &names, &values),
        "claim-lease: self-held should match"
    );

    let other_held = item(&[("store_id", s("s1")), ("lease_holder", s("tab-B"))]);
    assert!(
        !evaluate_filter_expression(expr, &other_held, &names, &values),
        "claim-lease: other-held must NOT match"
    );

    let missing_store = item(&[("lease_holder", s("tab-A"))]);
    assert!(
        !evaluate_filter_expression(expr, &missing_store, &names, &values),
        "claim-lease: missing store_id must NOT match"
    );
}

#[test]
fn filter_deep_nested_parentheses() {
    let it = item(&[("a", n("1"))]);
    let names = HashMap::new();
    let values = values(&[(":1", n("1"))]);

    for expr in ["(a = :1)", "((a = :1))", "(((a = :1)))", "((((a = :1))))"] {
        assert!(
            evaluate_filter_expression(expr, &it, &names, &values),
            "nested-paren failed: `{expr}`"
        );
    }
}

#[test]
fn filter_nested_map_path() {
    // Projection-style nested paths inside filter — dot notation, including
    // multi-level traversal, missing intermediates, placeholder segments, and
    // dotted paths inside function arguments.
    let it = item(&[(
        "profile",
        m(&[
            ("email", s("a@b.com")),
            ("verified", b(true)),
            ("address", m(&[("city", s("Lisbon")), ("zip", s("1000"))])),
        ]),
    )]);
    let names = names(&[("#p", "profile"), ("#e", "email")]);
    let values = values(&[
        (":e", s("a@b.com")),
        (":wrong", s("x@y.com")),
        (":city", s("Lisbon")),
    ]);

    let cases = [
        ("dot-equal", "profile.email = :e", true),
        ("dot-equal-miss", "profile.email = :wrong", false),
        (
            "dot-attribute-exists",
            "attribute_exists(profile.email)",
            true,
        ),
        (
            "dot-attribute-not-exists-leaf",
            "attribute_not_exists(profile.missing)",
            true,
        ),
        (
            "dot-attribute-exists-missing-parent",
            "attribute_exists(ghost.email)",
            false,
        ),
        ("three-level-equal", "profile.address.city = :city", true),
        (
            "three-level-attribute-exists",
            "attribute_exists(profile.address.city)",
            true,
        ),
        (
            "three-level-attribute-not-exists",
            "attribute_not_exists(profile.address.country)",
            true,
        ),
        ("placeholder-dotted", "#p.#e = :e", true),
        ("begins-with-nested", "begins_with(profile.email, :e)", true),
    ];
    for (label, expr, expected) in cases {
        assert_eq!(
            evaluate_filter_expression(expr, &it, &names, &values),
            expected,
            "filter nested-path '{label}' failed for `{expr}`"
        );
    }
}

// ---------------------------------------------------------------------------
// ConditionExpression — share the filter evaluator but exercise missing-item
// semantics
// ---------------------------------------------------------------------------

#[test]
fn condition_item_missing_semantics() {
    // ConditionExpression is evaluated against an item that may or may not
    // exist. These are the semantics the evaluator must get right:
    //
    //   exists → attribute_exists → true, attribute_not_exists → false
    //   missing → attribute_exists → false, attribute_not_exists → true
    let names = HashMap::new();
    let values = values(&[(":v", s("x"))]);

    let existing = item(&[("id", s("x"))]);
    assert!(evaluate_condition("attribute_exists(id)", Some(&existing), &names, &values).is_ok());
    assert!(
        evaluate_condition("attribute_not_exists(id)", Some(&existing), &names, &values).is_err()
    );

    assert!(evaluate_condition("attribute_exists(id)", None, &names, &values).is_err());
    assert!(evaluate_condition("attribute_not_exists(id)", None, &names, &values).is_ok());
}

#[test]
fn condition_sdk_builder_compound_shape() {
    // Compound `(#s <> :c) AND (#s <> :f)` — common "not-in-terminal-state"
    // guard the SDK builder emits.
    let names = names(&[("#s", "state")]);
    let values = values(&[(":c", s("complete")), (":f", s("failed"))]);
    let existing = item(&[("state", s("active"))]);

    // Should pass: state is neither complete nor failed.
    assert!(
        evaluate_condition(
            "(#s <> :c) AND (#s <> :f)",
            Some(&existing),
            &names,
            &values
        )
        .is_ok(),
        "compound <> AND <> must pass when both clauses are true"
    );
}

// ---------------------------------------------------------------------------
// UpdateExpression — the SET/REMOVE/ADD/DELETE grammar
// ---------------------------------------------------------------------------

fn apply(
    expr: &str,
    it: &mut HashMap<String, AttributeValue>,
    nmap: &HashMap<String, String>,
    vmap: &HashMap<String, Value>,
) {
    apply_update_expression(it, expr, nmap, vmap).expect("update apply failed");
}

#[test]
fn update_single_clause_forms() {
    // SET, REMOVE, ADD, DELETE each on their own.
    let names = HashMap::new();
    let values = values(&[(":v", n("42")), (":one", n("1")), (":tag", ss(&["red"]))]);

    // SET
    let mut it = HashMap::new();
    apply("SET x = :v", &mut it, &names, &values);
    assert_eq!(it.get("x"), Some(&n("42")));

    // REMOVE
    let mut it = item(&[("x", n("1")), ("y", n("2"))]);
    apply("REMOVE y", &mut it, &names, &values);
    assert!(!it.contains_key("y"));
    assert_eq!(it.get("x"), Some(&n("1")));

    // ADD (numeric delta)
    let mut it = item(&[("counter", n("10"))]);
    apply("ADD counter :one", &mut it, &names, &values);
    assert_eq!(it.get("counter"), Some(&n("11")));

    // DELETE (set element)
    let mut it = item(&[("tags", ss(&["red", "blue"]))]);
    apply("DELETE tags :tag", &mut it, &names, &values);
    assert_eq!(it.get("tags"), Some(&ss(&["blue"])));
}

#[test]
fn update_sdk_compound_newline_separated() {
    // SDK-emitted compound update: newline-separated clauses in fixed
    // ADD/DELETE/REMOVE/SET ordering, each clause trailed by `\n`.
    let mut it = item(&[
        ("counter", n("5")),
        ("tags", ss(&["a", "b"])),
        ("old", s("gone")),
    ]);
    let names = HashMap::new();
    let values = values(&[(":inc", n("2")), (":t", ss(&["a"])), (":new", s("fresh"))]);

    apply(
        "ADD counter :inc\nDELETE tags :t\nREMOVE old\nSET name = :new\n",
        &mut it,
        &names,
        &values,
    );

    assert_eq!(it.get("counter"), Some(&n("7")));
    assert_eq!(it.get("tags"), Some(&ss(&["b"])));
    assert!(!it.contains_key("old"));
    assert_eq!(it.get("name"), Some(&s("fresh")));
}

#[test]
fn update_sdk_compound_with_placeholders() {
    // Same as above but using #0/:0 placeholders as the SDK emits.
    let mut it = item(&[("counter", n("5"))]);
    let names = names(&[("#0", "counter")]);
    let values = values(&[(":0", n("3"))]);

    apply("ADD #0 :0\n", &mut it, &names, &values);
    assert_eq!(it.get("counter"), Some(&n("8")));
}

#[test]
fn update_set_functions() {
    let names = HashMap::new();

    // if_not_exists: keep existing
    let mut it = item(&[("x", n("100"))]);
    let ifne = values(&[(":v", n("5"))]);
    apply("SET x = if_not_exists(x, :v)", &mut it, &names, &ifne);
    assert_eq!(it.get("x"), Some(&n("100")));

    // if_not_exists: install default
    let mut it = HashMap::new();
    apply("SET x = if_not_exists(x, :v)", &mut it, &names, &ifne);
    assert_eq!(it.get("x"), Some(&n("5")));

    // list_append on existing list
    let mut it = item(&[("xs", l(vec![n("1"), n("2")]))]);
    let la = values(&[(":tail", l(vec![n("3")]))]);
    apply("SET xs = list_append(xs, :tail)", &mut it, &names, &la);
    assert_eq!(it.get("xs"), Some(&l(vec![n("1"), n("2"), n("3")])));

    // list_append on empty existing list
    let mut it = item(&[("xs", l(vec![]))]);
    apply("SET xs = list_append(xs, :tail)", &mut it, &names, &la);
    assert_eq!(it.get("xs"), Some(&l(vec![n("3")])));
}

#[test]
fn update_set_arithmetic() {
    let mut it = item(&[("counter", n("10"))]);
    let names = HashMap::new();
    let values = values(&[(":d", n("3"))]);

    apply("SET counter = counter + :d", &mut it, &names, &values);
    assert_eq!(it.get("counter"), Some(&n("13")));

    apply("SET counter = counter - :d", &mut it, &names, &values);
    assert_eq!(it.get("counter"), Some(&n("10")));
}

#[test]
fn update_set_list_index_target() {
    // SET into a specific list slot: #list[N] = :v.
    let mut it = item(&[("xs", l(vec![s("a"), s("b"), s("c")]))]);
    let names = names(&[("#xs", "xs")]);
    let values = values(&[(":v", s("B"))]);

    apply("SET #xs[1] = :v", &mut it, &names, &values);
    assert_eq!(it.get("xs"), Some(&l(vec![s("a"), s("B"), s("c")])));
}

#[test]
fn update_set_multiple_with_list_append() {
    // Two SET assignments separated by a top-level comma, where one of them
    // contains a `list_append(a, b)` call — the inner comma must not split
    // the clause list.
    let mut it = item(&[("xs", l(vec![s("a")])), ("name", s("old"))]);
    let names = HashMap::new();
    let values = values(&[(":tail", l(vec![s("b")])), (":n", s("new"))]);

    apply(
        "SET xs = list_append(xs, :tail), name = :n",
        &mut it,
        &names,
        &values,
    );
    assert_eq!(it.get("xs"), Some(&l(vec![s("a"), s("b")])));
    assert_eq!(it.get("name"), Some(&s("new")));
}

#[test]
fn update_set_nested_map_path_target() {
    // Dotted path into a nested M-type attribute. The parent map must already
    // exist; the SET updates just the child keys, leaving sibling keys alone.
    let mut it = item(&[(
        "web",
        m(&[
            ("tab_id", s("old-tab")),
            ("session_id", s("old-session")),
            ("keep_me", s("untouched")),
        ]),
    )]);
    let nmap = names(&[
        ("#web", "web"),
        ("#tab_id", "tab_id"),
        ("#session_id", "session_id"),
    ]);
    let vmap = values(&[(":tab", s("new-tab")), (":sess", s("new-session"))]);

    apply(
        "SET #web.#tab_id = :tab, #web.#session_id = :sess",
        &mut it,
        &nmap,
        &vmap,
    );

    let expected = m(&[
        ("tab_id", s("new-tab")),
        ("session_id", s("new-session")),
        ("keep_me", s("untouched")),
    ]);
    assert_eq!(
        it.get("web"),
        Some(&expected),
        "nested-path SET must update child keys in place, not create a top-level '#web.#tab_id' attribute"
    );
}

#[test]
fn update_set_nested_bool_single_descent() {
    // Single-level nested SET into a child key.
    let mut it = item(&[(
        "uploader",
        m(&[("rcv_blocked", b(false)), ("other", s("keep"))]),
    )]);
    let nmap = names(&[("#u", "uploader"), ("#rb", "rcv_blocked")]);
    let vmap = values(&[(":val", b(true))]);

    apply("SET #u.#rb = :val", &mut it, &nmap, &vmap);

    let expected = m(&[("rcv_blocked", b(true)), ("other", s("keep"))]);
    assert_eq!(it.get("uploader"), Some(&expected));
}

#[test]
fn update_set_nested_path_missing_parent_errors() {
    // Per DynamoDB semantics, SETting a path through a missing parent is a
    // ValidationException, not a silent no-op.
    let mut it: HashMap<String, AttributeValue> = HashMap::new();
    let nmap = names(&[("#web", "web"), ("#tab_id", "tab_id")]);
    let vmap = values(&[(":tab", s("t"))]);

    let err = apply_update_expression(&mut it, "SET #web.#tab_id = :tab", &nmap, &vmap);
    assert!(
        err.is_err(),
        "SET into a missing parent map must be an error"
    );
    assert!(
        !it.contains_key("#web.#tab_id"),
        "must not leak a literal dotted-name top-level key"
    );
}

#[test]
fn update_set_nested_path_complex_rhs() {
    // Real DynamoDB allows any SET RHS against any SET LHS, including dotted
    // paths: arithmetic, list_append, and if_not_exists must all be valid
    // RHS shapes even when the LHS is a nested M-typed path.

    // Arithmetic into a nested N: web.count = web.count + :d
    let mut it = item(&[("web", m(&[("count", n("5"))]))]);
    let nmap = names(&[("#web", "web"), ("#count", "count")]);
    let vmap = values(&[(":d", n("3"))]);
    apply_update_expression(&mut it, "SET #web.#count = #web.#count + :d", &nmap, &vmap)
        .expect("arithmetic RHS into nested path should succeed");
    assert_eq!(it.get("web"), Some(&m(&[("count", n("8"))])));

    // Subtraction variant with plain dotted path.
    let mut it = item(&[("web", m(&[("count", n("10"))]))]);
    let nmap = HashMap::new();
    let vmap = values(&[(":d", n("3"))]);
    apply_update_expression(&mut it, "SET web.count = web.count - :d", &nmap, &vmap)
        .expect("subtraction RHS into nested path should succeed");
    assert_eq!(it.get("web"), Some(&m(&[("count", n("7"))])));

    // list_append into a nested L.
    let mut it = item(&[("web", m(&[("items", l(vec![s("a"), s("b")]))]))]);
    let nmap = HashMap::new();
    let vmap = values(&[(":new", l(vec![s("c")]))]);
    apply_update_expression(
        &mut it,
        "SET web.items = list_append(web.items, :new)",
        &nmap,
        &vmap,
    )
    .expect("list_append RHS into nested path should succeed");
    assert_eq!(
        it.get("web"),
        Some(&m(&[("items", l(vec![s("a"), s("b"), s("c")]))]))
    );

    // if_not_exists: existing leaf → RHS is a no-op, original value preserved.
    let mut it = item(&[("web", m(&[("count", n("42"))]))]);
    let nmap = HashMap::new();
    let vmap = values(&[(":default", n("0"))]);
    apply_update_expression(
        &mut it,
        "SET web.count = if_not_exists(web.count, :default)",
        &nmap,
        &vmap,
    )
    .expect("if_not_exists should skip when the leaf already has a value");
    assert_eq!(it.get("web"), Some(&m(&[("count", n("42"))])));

    // if_not_exists: missing leaf → default is written through the nested path.
    let mut it = item(&[("web", m(&[("other", s("keep"))]))]);
    let nmap = HashMap::new();
    let vmap = values(&[(":default", n("7"))]);
    apply_update_expression(
        &mut it,
        "SET web.count = if_not_exists(web.count, :default)",
        &nmap,
        &vmap,
    )
    .expect("if_not_exists should write default when leaf missing");
    assert_eq!(
        it.get("web"),
        Some(&m(&[("other", s("keep")), ("count", n("7"))]))
    );
}

#[test]
fn update_clause_order_independence() {
    // Real DynamoDB accepts SET/REMOVE/ADD/DELETE in any order. Fakecloud
    // should too.
    let expected_x = n("9");
    let expected_y = None;

    let exprs = [
        "SET x = :v REMOVE y",
        "REMOVE y SET x = :v",
        "SET x = :v\nREMOVE y",
        "REMOVE y\nSET x = :v",
    ];
    for expr in exprs {
        let mut it = item(&[("x", n("0")), ("y", n("1"))]);
        let names = HashMap::new();
        let values = values(&[(":v", n("9"))]);
        apply(expr, &mut it, &names, &values);
        assert_eq!(it.get("x"), Some(&expected_x), "clause-order `{expr}` x");
        assert_eq!(it.get("y").cloned(), expected_y, "clause-order `{expr}` y");
    }
}
