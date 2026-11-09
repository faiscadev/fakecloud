//! JSONata states: `QueryLanguage: JSONata` definitions, whose fields hold
//! `{% expression %}` strings evaluated with `$states` (`input`, `context`,
//! `result`, `errorOutput`), workflow variables from `Assign`, and the functions
//! Step Functions adds to the language.
//!
//! A JSONata state runs on the same machinery as a JSONPath state: its
//! expressions are evaluated up front into a literal state definition, the state
//! runs, and its `Assign` and `Output` then apply to what it produced.

use jsonata_core::evaluator::{Context, Evaluator};
use jsonata_core::value::JValue;
use md5::{Digest, Md5};
use serde_json::{json, Map, Value};

type StateError = (String, String);

const QUERY_ERROR: &str = "States.QueryEvaluationError";

fn query_error(cause: impl Into<String>) -> StateError {
    (QUERY_ERROR.to_string(), cause.into())
}

/// Give every state of a definition its effective query language, so nested
/// Parallel branches and Map processors inherit the machine's.
pub(crate) fn propagate_query_language(def: &mut Value, inherited: Option<&str>) {
    let language = def["QueryLanguage"]
        .as_str()
        .map(String::from)
        .or_else(|| inherited.map(String::from));
    let Some(states) = def.get_mut("States").and_then(Value::as_object_mut) else {
        return;
    };
    for state in states.values_mut() {
        // Refused at creation; a stored definition must still not panic here.
        if !state.is_object() {
            continue;
        }
        if state.get("QueryLanguage").is_none() {
            if let Some(language) = &language {
                state["QueryLanguage"] = json!(language);
            }
        }
        let own = state["QueryLanguage"].as_str().map(String::from);
        for key in ["ItemProcessor", "Iterator"] {
            if let Some(nested) = state.get_mut(key) {
                propagate_query_language(nested, own.as_deref());
            }
        }
        if let Some(branches) = state.get_mut("Branches").and_then(Value::as_array_mut) {
            for branch in branches {
                propagate_query_language(branch, own.as_deref());
            }
        }
    }
}

pub(crate) fn is_jsonata(state_def: &Value) -> bool {
    state_def["QueryLanguage"].as_str() == Some("JSONata")
}

/// What an expression can see.
pub(crate) struct Scope<'a> {
    pub(crate) input: &'a Value,
    pub(crate) context: &'a Value,
    pub(crate) result: Option<&'a Value>,
    pub(crate) error_output: Option<&'a Value>,
}

fn expression_of(text: &str) -> Option<&str> {
    text.trim()
        .strip_prefix("{%")
        .and_then(|rest| rest.strip_suffix("%}"))
}

fn host_error(message: String) -> jsonata_core::evaluator::EvaluatorError {
    jsonata_core::evaluator::EvaluatorError::EvaluationError(message)
}

/// `$random(seed)`: as recorded, what `new java.util.Random(seed).nextDouble()`
/// returns, the same value for the same seed.
fn seeded_random(seed: i64) -> f64 {
    const MULTIPLIER: i64 = 0x5_DEEC_E66D;
    const MASK: i64 = (1 << 48) - 1;
    let mut state = (seed ^ MULTIPLIER) & MASK;
    let mut next = |bits: u32| {
        state = state.wrapping_mul(MULTIPLIER).wrapping_add(0xB) & MASK;
        state >> (48 - bits)
    };
    let high = next(26);
    let low = next(27);
    ((high << 27) + low) as f64 / (1_i64 << 53) as f64
}

fn register_step_functions_functions(evaluator: &mut Evaluator) -> Result<(), StateError> {
    let bad = |e: jsonata_core::evaluator::EvaluatorError| query_error(e.to_string());
    evaluator
        .register_fn("uuid", |_: &[JValue]| {
            Ok(JValue::from(uuid::Uuid::new_v4().to_string()))
        })
        .map_err(bad)?;
    evaluator
        .register_fn("parse", |args: &[JValue]| {
            let text = args
                .first()
                .and_then(|v| v.as_str())
                .ok_or_else(|| host_error("$parse needs a JSON string".to_string()))?;
            JValue::from_json_str(text).map_err(|e| host_error(format!("$parse: {e}")))
        })
        .map_err(bad)?;
    evaluator
        .register_fn("partition", |args: &[JValue]| {
            let items = args
                .first()
                .and_then(|v| v.as_array())
                .ok_or_else(|| host_error("$partition needs an array".to_string()))?;
            let size = args
                .get(1)
                .and_then(|v| v.as_f64())
                .filter(|size| *size >= 1.0)
                .ok_or_else(|| host_error("$partition needs a positive size".to_string()))?;
            let chunks: Vec<JValue> = items
                .chunks(size as usize)
                .map(|chunk| JValue::array(chunk.to_vec()))
                .collect();
            Ok(JValue::array(chunks))
        })
        .map_err(bad)?;
    evaluator
        .register_fn("range", |args: &[JValue]| {
            let number = |index: usize| args.get(index).and_then(|v| v.as_f64());
            let (start, end, step) = match (number(0), number(1), number(2)) {
                (Some(start), Some(end), Some(step)) if step != 0.0 => (start, end, step),
                _ => return Err(host_error("$range needs a start, end and step".to_string())),
            };
            let mut out = Vec::new();
            let mut at = start;
            while (step > 0.0 && at <= end) || (step < 0.0 && at >= end) {
                out.push(JValue::from(at));
                at += step;
            }
            Ok(JValue::array(out))
        })
        .map_err(bad)?;
    evaluator
        .register_fn("hash", |args: &[JValue]| {
            let data = args
                .first()
                .and_then(|v| v.as_str())
                .ok_or_else(|| host_error("$hash needs a string".to_string()))?;
            let digest = match args.get(1).and_then(|v| v.as_str()).unwrap_or("SHA-256") {
                "MD5" => hex::encode(Md5::digest(data)),
                "SHA-1" => hex::encode(sha1::Sha1::digest(data)),
                "SHA-256" => hex::encode(sha2::Sha256::digest(data)),
                "SHA-384" => hex::encode(sha2::Sha384::digest(data)),
                "SHA-512" => hex::encode(sha2::Sha512::digest(data)),
                other => return Err(host_error(format!("$hash: unknown algorithm {other}"))),
            };
            Ok(JValue::from(digest))
        })
        .map_err(bad)?;
    // Step Functions' `$random` takes an optional seed the language's does not.
    evaluator
        .register_fn_override("random", |args: &[JValue]| {
            Ok(JValue::from(match args.first().and_then(|v| v.as_f64()) {
                Some(seed) => seeded_random(seed as i64),
                None => rand::random::<f64>(),
            }))
        })
        .map_err(bad)?;
    Ok(())
}

fn evaluate(expression: &str, scope: &Scope<'_>) -> Result<Option<Value>, StateError> {
    let ast = jsonata_core::parser::parse(expression)
        .map_err(|e| query_error(format!("Failed to parse '{expression}': {e}")))?;

    let mut states = json!({ "input": scope.input, "context": scope.context });
    if let Some(result) = scope.result {
        states["result"] = result.clone();
    }
    if let Some(error_output) = scope.error_output {
        states["errorOutput"] = error_output.clone();
    }
    let mut context = Context::new();
    context.bind("states".to_string(), JValue::from(states));
    for (name, value) in crate::variables::visible() {
        context.bind(name, JValue::from(value));
    }

    let mut evaluator = Evaluator::with_context(context);
    register_step_functions_functions(&mut evaluator)?;
    let value = evaluator
        .evaluate(&ast, &JValue::Null)
        .map_err(|e| query_error(format!("Failed to evaluate '{expression}': {e}")))?;
    if value.is_undefined() {
        return Ok(None);
    }
    let text = value
        .to_json_string()
        .map_err(|e| query_error(format!("'{expression}' has no JSON value: {e}")))?;
    serde_json::from_str(&text)
        .map(Some)
        .map_err(|e| query_error(format!("'{expression}' has no JSON value: {e}")))
}

/// Evaluate every `{% %}` string inside the value of a state's `field`.
pub(crate) fn eval_template(
    field: &str,
    template: &Value,
    scope: &Scope<'_>,
) -> Result<Value, StateError> {
    match template {
        Value::String(text) => match expression_of(text) {
            Some(expression) => {
                let expression = expression.trim();
                evaluate(expression, scope)?.ok_or_else(|| {
                    query_error(format!(
                        "The JSONata expression '{expression}' specified for the field \
                         '{field}' returned nothing (undefined)."
                    ))
                })
            }
            None => Ok(template.clone()),
        },
        Value::Object(object) => object
            .iter()
            .map(|(key, value)| Ok((key.clone(), eval_template(field, value, scope)?)))
            .collect::<Result<Map<_, _>, StateError>>()
            .map(Value::Object),
        Value::Array(items) => items
            .iter()
            .map(|item| eval_template(field, item, scope))
            .collect::<Result<Vec<_>, StateError>>()
            .map(Value::Array),
        other => Ok(other.clone()),
    }
}

fn eval_fields(object: &mut Value, fields: &[&str], scope: &Scope<'_>) -> Result<(), StateError> {
    for field in fields {
        if let Some(template) = object.get(*field) {
            let value = eval_template(field, template, scope)?;
            object[*field] = value;
        }
    }
    Ok(())
}

/// The cause of a query failure as AWS words it, naming the state and when it
/// was entered.
pub(crate) fn in_state_cause(state: &str, entered_event_id: i64, cause: &str) -> String {
    format!(
        "An error occurred while executing the state '{state}' (entered at the event id \
         #{entered_event_id}). {cause}"
    )
}

/// The field a query failure's cause names, for its `EvaluationFailed` event.
pub(crate) fn failed_field(cause: &str) -> Option<&str> {
    if let Some((_, rest)) = cause.split_once("for the field '") {
        return rest.split('\'').next();
    }
    if cause.starts_with("Seconds ") {
        return Some("Seconds");
    }
    if cause.starts_with("The Timestamp field") {
        return Some("Timestamp");
    }
    None
}

fn json_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// A Wait state's expressions, checked as recorded.
fn prepare_wait(
    state_def: &Value,
    prepared: &mut Value,
    scope: &Scope<'_>,
) -> Result<(), StateError> {
    if let Some(template) = state_def.get("Seconds") {
        let seconds = eval_template("Seconds", template, scope)?;
        if !seconds.is_number() {
            let expression = template
                .as_str()
                .and_then(expression_of)
                .unwrap_or_default()
                .trim();
            return Err(query_error(format!(
                "The JSONata expression '{expression}' specified for the field 'Seconds' \
                 returned an unexpected result type. Expected 'number', but was '{}' for \
                 value: {seconds}",
                json_type_name(&seconds)
            )));
        }
        if seconds.as_f64().is_some_and(|n| n < 0.0) {
            return Err(query_error(format!(
                "Seconds cannot be negative but was: {seconds}"
            )));
        }
        prepared["Seconds"] = seconds;
    }
    if let Some(template) = state_def.get("Timestamp") {
        let timestamp = eval_template("Timestamp", template, scope)?;
        let text = match &timestamp {
            Value::String(text) => text.clone(),
            other => other.to_string(),
        };
        if chrono::DateTime::parse_from_rfc3339(&text).is_err() {
            return Err(query_error(format!(
                "The Timestamp field cannot be parsed as an ISO-8601 extended offset date-time \
                 format string: {text}"
            )));
        }
        prepared["Timestamp"] = timestamp;
    }
    Ok(())
}

/// A JSONata state evaluated into the literal definition the machinery runs,
/// and the input it runs on.
pub(crate) fn prepare_state(
    state_def: &Value,
    input: &Value,
    context: &Value,
) -> Result<(Value, Value), StateError> {
    let scope = Scope {
        input,
        context,
        result: None,
        error_output: None,
    };
    let mut prepared = state_def.clone();
    let mut run_input = input.clone();
    match state_def["Type"].as_str() {
        Some("Wait") => prepare_wait(state_def, &mut prepared, &scope)?,
        Some("Fail") => eval_fields(&mut prepared, &["Error", "Cause"], &scope)?,
        Some("Task") => eval_fields(
            &mut prepared,
            &["TimeoutSeconds", "HeartbeatSeconds"],
            &scope,
        )?,
        Some("Parallel") => {
            if let Some(arguments) = state_def.get("Arguments") {
                run_input = eval_template("Arguments", arguments, &scope)?;
            }
        }
        Some("Map") => {
            eval_fields(
                &mut prepared,
                &[
                    "MaxConcurrency",
                    "ToleratedFailureCount",
                    "ToleratedFailurePercentage",
                ],
                &scope,
            )?;
            // `Items` stays apart from the input: `$states.input` in `ItemSelector`
            // is still the state's input.
            eval_fields(&mut prepared, &["Items", "ItemBatcher"], &scope)?;
            for field in ["ItemReader", "ResultWriter"] {
                if let Some(part) = prepared.get_mut(field) {
                    if let Some(arguments) = part.get("Arguments").cloned() {
                        part["Parameters"] = eval_template("Arguments", &arguments, &scope)?;
                    }
                    if let Some(config) = part.get_mut("ReaderConfig") {
                        eval_fields(config, &["MaxItems"], &scope)?;
                    }
                }
            }
        }
        _ => {}
    }
    Ok((prepared, run_input))
}

fn apply_assign(assign: &Value, scope: &Scope<'_>) -> Result<(), StateError> {
    let Some(pairs) = assign.as_object() else {
        return Ok(());
    };
    // References read the values from state entry; new ones apply to the next
    // state. All of them evaluate before any is staged.
    let evaluated = pairs
        .iter()
        .map(|(name, template)| Ok((name.clone(), eval_template("Assign", template, scope)?)))
        .collect::<Result<Vec<_>, StateError>>()?;
    for (name, value) in evaluated {
        crate::variables::stage(name, value);
    }
    Ok(())
}

/// A state's output: its `Assign` applied, then its `Output`, by default
/// `produced` (the task result, or the input of a state that produces nothing).
pub(crate) fn finish_state(
    holder: &Value,
    input: &Value,
    context: &Value,
    produced: Value,
    error_output: bool,
) -> Result<Value, StateError> {
    let scope = Scope {
        input,
        context,
        result: (!error_output).then_some(&produced),
        error_output: error_output.then_some(&produced),
    };
    // Nothing is staged unless both fields evaluate: a state failing here keeps
    // none of its assignments.
    let output = match holder.get("Output") {
        Some(output) => Some(eval_template("Output", output, &scope)?),
        None => None,
    };
    if let Some(assign) = holder.get("Assign") {
        apply_assign(assign, &scope)?;
    }
    Ok(output.unwrap_or(produced))
}

/// Run a JSONata Choice state's rules: the rule or default taken and its output.
pub(crate) fn choose(
    state_def: &Value,
    input: &Value,
    context: &Value,
) -> Result<Option<(String, Value)>, StateError> {
    let scope = Scope {
        input,
        context,
        result: None,
        error_output: None,
    };
    for rule in state_def["Choices"].as_array().into_iter().flatten() {
        let condition = rule.get("Condition").unwrap_or(&Value::Null);
        match eval_template("Condition", condition, &scope)? {
            Value::Bool(true) => {
                let Some(next) = rule["Next"].as_str() else {
                    continue;
                };
                let output = finish_state(rule, input, context, input.clone(), false)?;
                return Ok(Some((next.to_string(), output)));
            }
            Value::Bool(false) => {}
            other => {
                return Err(query_error(format!(
                    "A Choice Condition must be a boolean, got {other}"
                )))
            }
        }
    }
    match state_def["Default"].as_str() {
        Some(next) => {
            let output = finish_state(state_def, input, context, input.clone(), false)?;
            Ok(Some((next.to_string(), output)))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope<'a>(input: &'a Value, context: &'a Value, _arn: &'a str) -> Scope<'a> {
        Scope {
            input,
            context,
            result: None,
            error_output: None,
        }
    }

    #[test]
    fn expressions_evaluate_at_any_depth_and_literals_stay() {
        let input = json!({"n": 2});
        let context = json!({"Execution": {"Name": "e"}});
        let out = eval_template(
            "Output",
            &json!({
                "a": "{% 1 + 1 %}",
                "b": {"c": ["{% $states.input.n * 2 %}", "plain"]},
                "d": "{% $states.context.Execution.Name %}",
                "e": 5
            }),
            &scope(&input, &context, "arn:t1"),
        )
        .unwrap();
        assert_eq!(
            out,
            json!({"a": 2, "b": {"c": [4, "plain"]}, "d": "e", "e": 5})
        );
    }

    #[test]
    fn undefined_and_broken_expressions_are_query_errors() {
        let input = json!({});
        let context = json!({});
        for template in ["{% $states.input.missing %}", "{% 1 + %}"] {
            let (error, _) = eval_template(
                "Output",
                &json!(template),
                &scope(&input, &context, "arn:t2"),
            )
            .unwrap_err();
            assert_eq!(error, QUERY_ERROR);
        }
    }

    #[test]
    fn step_functions_functions_are_available() {
        let input = json!({});
        let context = json!({});
        let eval = |text: &str| {
            eval_template("Output", &json!(text), &scope(&input, &context, "arn:t3")).unwrap()
        };
        assert_eq!(eval("{% $parse('{\"a\":1}') %}"), json!({"a": 1}));
        assert_eq!(eval("{% $partition([1,2,3], 2) %}"), json!([[1, 2], [3]]));
        assert_eq!(eval("{% $range(0, 4, 2) %}"), json!([0, 2, 4]));
        assert_eq!(
            eval("{% $hash('abc', 'MD5') %}"),
            "900150983cd24fb0d6963f7d28e17f72"
        );
        assert_eq!(eval("{% $uuid() %}").as_str().unwrap().len(), 36);
        assert!(eval("{% $random() %}").is_number());
    }

    #[test]
    fn output_and_assign_read_the_values_from_state_entry() {
        let input = json!({});
        let context = json!({});
        let run = async {
            finish_state(
                &json!({"Assign": {"x": 1}}),
                &input,
                &context,
                json!({}),
                false,
            )
            .unwrap();
            crate::variables::commit();
            // The swap of the docs' example: both sides read the old values.
            let out = finish_state(
                &json!({
                    "Assign": {"x": "{% $x + 1 %}", "y": "{% $x %}"},
                    "Output": "{% [$x, $states.result] %}"
                }),
                &input,
                &context,
                json!("r"),
                false,
            )
            .unwrap();
            assert_eq!(out, json!([1, "r"]));
            crate::variables::commit();
            let after = eval_template(
                "Output",
                &json!("{% [$x, $y] %}"),
                &scope(&input, &context, ""),
            )
            .unwrap();
            assert_eq!(after, json!([2, 1]));
        };
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(crate::variables::in_scope(
                crate::variables::VariableScope::root(),
                run,
            ));
    }

    #[test]
    fn query_language_reaches_nested_states() {
        let mut def = json!({
            "QueryLanguage": "JSONata",
            "States": {
                "M": {"Type": "Map", "ItemProcessor": {"States": {"P": {"Type": "Pass"}}}},
                "J": {"Type": "Pass", "QueryLanguage": "JSONPath"},
                "B": {"Type": "Parallel", "Branches": [{"States": {"Q": {"Type": "Pass"}}}]}
            }
        });
        propagate_query_language(&mut def, None);
        assert_eq!(
            def["States"]["M"]["ItemProcessor"]["States"]["P"]["QueryLanguage"],
            "JSONata"
        );
        assert_eq!(def["States"]["J"]["QueryLanguage"], "JSONPath");
        assert_eq!(
            def["States"]["B"]["Branches"][0]["States"]["Q"]["QueryLanguage"],
            "JSONata"
        );
    }

    #[test]
    fn choices_take_the_first_true_condition_or_the_default() {
        let input = json!({"n": 3});
        let context = json!({});
        let state = json!({
            "Choices": [
                {"Condition": "{% $states.input.n > 5 %}", "Next": "Big"},
                {"Condition": "{% $states.input.n > 1 %}", "Next": "Mid", "Output": "{% 'mid' %}"}
            ],
            "Default": "Small"
        });
        assert_eq!(
            choose(&state, &input, &context).unwrap(),
            Some(("Mid".to_string(), json!("mid")))
        );
        let small = json!({"n": 0});
        assert_eq!(
            choose(&state, &small, &context).unwrap(),
            Some(("Small".to_string(), small.clone()))
        );
    }

    #[test]
    fn non_object_states_are_left_alone() {
        let mut def = json!({"QueryLanguage": "JSONata", "States": {"Bad": 1, "Worse": [2]}});
        propagate_query_language(&mut def, None);
        assert_eq!(def["States"]["Bad"], 1);
    }

    #[test]
    fn item_batcher_expressions_evaluate() {
        let input = json!({"size": 2});
        let context = json!({});
        let (prepared, _) = prepare_state(
            &json!({
                "Type": "Map",
                "ItemBatcher": {
                    "MaxItemsPerBatch": "{% $states.input.size %}",
                    "BatchInput": {"from": "{% $states.input.size + 1 %}"}
                }
            }),
            &input,
            &context,
        )
        .unwrap();
        assert_eq!(prepared["ItemBatcher"]["MaxItemsPerBatch"], 2);
        assert_eq!(prepared["ItemBatcher"]["BatchInput"]["from"], 3);
    }

    /// Recorded: `$random(42)` from `java.util.Random(42).nextDouble()`.
    #[test]
    fn seeded_random_matches_the_recording() {
        assert_eq!(seeded_random(42), 0.7275636800328681);
        let input = json!({});
        let context = json!({});
        let out = eval_template(
            "Output",
            &json!("{% [$random(42) = $random(42), $random(42)] %}"),
            &scope(&input, &context, ""),
        )
        .unwrap();
        assert_eq!(out, json!([true, 0.7275636800328681]));
    }

    fn wait_error(state: Value) -> String {
        let input = json!({});
        let context = json!({});
        let (error, cause) = prepare_state(&state, &input, &context).unwrap_err();
        assert_eq!(error, QUERY_ERROR);
        cause
    }

    #[test]
    fn wait_values_fail_as_recorded() {
        assert_eq!(
            wait_error(json!({"Type": "Wait", "Seconds": "{% '5' %}"})),
            "The JSONata expression ''5'' specified for the field 'Seconds' returned an \
             unexpected result type. Expected 'number', but was 'string' for value: \"5\""
        );
        assert_eq!(
            wait_error(json!({"Type": "Wait", "Seconds": "{% -1 %}"})),
            "Seconds cannot be negative but was: -1"
        );
        assert_eq!(
            wait_error(json!({"Type": "Wait", "Timestamp": "{% 'not-a-date' %}"})),
            "The Timestamp field cannot be parsed as an ISO-8601 extended offset date-time \
             format string: not-a-date"
        );
        let input = json!({});
        let context = json!({});
        let (prepared, _) = prepare_state(
            &json!({"Type": "Wait", "Seconds": "{% 0 %}"}),
            &input,
            &context,
        )
        .unwrap();
        assert_eq!(prepared["Seconds"], 0);
    }

    #[test]
    fn undefined_output_names_its_field() {
        let input = json!({});
        let context = json!({});
        let (_, cause) = finish_state(
            &json!({"Output": "{% $states.input.missing %}"}),
            &input,
            &context,
            json!({}),
            false,
        )
        .unwrap_err();
        assert_eq!(
            cause,
            "The JSONata expression '$states.input.missing' specified for the field 'Output' \
             returned nothing (undefined)."
        );
        assert_eq!(failed_field(&cause), Some("Output"));
        assert_eq!(
            in_state_cause("A", 2, "x"),
            "An error occurred while executing the state 'A' (entered at the event id #2). x"
        );
    }
}
