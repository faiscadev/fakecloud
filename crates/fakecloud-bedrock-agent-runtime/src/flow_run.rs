//! Walking a flow definition's graph for an `InvokeFlow`: from the input node
//! along its connections to the output nodes the run reaches, with the trace
//! events that walk produces. Condition nodes really evaluate their conditions;
//! a node that would call a model or another service produces a synthetic reply.

use std::collections::{HashMap, HashSet, VecDeque};

use chrono::Utc;
use serde_json::{json, Value};

/// The name output events carry when a definition has no output node of its own.
const DEFAULT_OUTPUT_NODE: &str = "FlowOutputNode";

/// What one run of a flow produces.
#[derive(Debug, Default)]
pub(crate) struct FlowRun {
    /// `FlowTrace` union values, in the order they happened.
    pub traces: Vec<Value>,
    /// Each output node reached and the document it received.
    pub outputs: Vec<(String, Value)>,
}

/// A field's `FlowNodeIODataType` as a trace reports it. A `String` field was
/// recorded as `STRING` although the Smithy enum spells it `String`, as with
/// `nodeType` on `flowOutputEvent`; the other types are extrapolated from it.
fn trace_data_type(declared: Option<&str>) -> String {
    declared.unwrap_or("String").to_ascii_uppercase()
}

fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true)
}

fn text_of(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

/// What a node that would call a model or service replies.
fn synthetic_reply(input: &Value) -> Value {
    json!(format!("Flow output for input: {}", text_of(input)))
}

/// An input expression (`$.data`, `$.data.a.b`) applied to what a connection
/// delivers.
fn apply_expression(expression: &str, value: &Value) -> Value {
    let Some(path) = expression.strip_prefix("$.data") else {
        return value.clone();
    };
    let mut current = value;
    for step in path.split('.').filter(|step| !step.is_empty()) {
        current = &current[step];
    }
    current.clone()
}

/// Check a run's input against the definition.
pub(crate) fn validate_input(
    definition: Option<&Value>,
    node: &str,
    output: &str,
) -> Result<(), String> {
    let Some(nodes) = definition
        .and_then(|d| d["nodes"].as_array())
        .filter(|n| !n.is_empty())
    else {
        return Ok(());
    };
    let found = nodes
        .iter()
        .find(|candidate| candidate["name"] == node && candidate["type"] == "Input")
        .ok_or_else(|| format!("The flow has no input node named {node}."))?;
    if let Some(outputs) = found["outputs"].as_array() {
        if !outputs.iter().any(|candidate| candidate["name"] == output) {
            return Err(format!("Node {node} has no output named {output}."));
        }
    }
    Ok(())
}

/// Run a definition from its input node.
pub(crate) fn run(
    definition: Option<&Value>,
    start_node: &str,
    start_output: &str,
    document: &Value,
) -> Result<FlowRun, String> {
    let empty = Vec::new();
    let nodes = definition
        .and_then(|d| d["nodes"].as_array())
        .unwrap_or(&empty);
    let connections = definition
        .and_then(|d| d["connections"].as_array())
        .unwrap_or(&empty);

    // A definition with nowhere for output to go still answers the caller.
    if !nodes.iter().any(|node| node["type"] == "Output") {
        return Ok(FlowRun {
            traces: Vec::new(),
            outputs: vec![(DEFAULT_OUTPUT_NODE.to_string(), synthetic_reply(document))],
        });
    }

    let node_named = |name: &str| nodes.iter().find(|node| node["name"] == name);
    let data_edges = |source: &str, output: &str| -> Vec<&Value> {
        connections
            .iter()
            .filter(|c| {
                c["type"] == "Data"
                    && c["source"] == source
                    && c["configuration"]["data"]["sourceOutput"] == output
            })
            .collect()
    };
    let has_conditional_entry = |name: &str| {
        connections
            .iter()
            .any(|c| c["type"] == "Conditional" && c["target"] == name)
    };

    let mut run = FlowRun::default();
    // Values each finished node's outputs hold.
    let mut produced: HashMap<(String, String), Value> = HashMap::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<String> = VecDeque::new();

    produced.insert(
        (start_node.to_string(), start_output.to_string()),
        document.clone(),
    );
    visited.insert(start_node.to_string());
    push_output_trace(
        &mut run,
        start_node,
        start_output,
        document,
        node_named(start_node),
        &data_edges(start_node, start_output),
        nodes,
    );
    for edge in data_edges(start_node, start_output) {
        let target = edge["target"].as_str().unwrap_or_default();
        if !has_conditional_entry(target) {
            queue.push_back(target.to_string());
        }
    }

    while let Some(name) = queue.pop_front() {
        if !visited.insert(name.clone()) {
            continue;
        }
        let Some(node) = node_named(&name) else {
            continue;
        };

        // Gather this node's inputs from the connections that have delivered.
        let mut inputs: Vec<(String, Value)> = Vec::new();
        let mut fields: Vec<Value> = Vec::new();
        for edge in connections
            .iter()
            .filter(|c| c["type"] == "Data" && c["target"] == name.as_str())
        {
            let source = edge["source"].as_str().unwrap_or_default();
            let source_output = edge["configuration"]["data"]["sourceOutput"]
                .as_str()
                .unwrap_or("document");
            let target_input = edge["configuration"]["data"]["targetInput"]
                .as_str()
                .unwrap_or("document");
            let Some(delivered) = produced.get(&(source.to_string(), source_output.to_string()))
            else {
                continue;
            };
            let declared = node["inputs"]
                .as_array()
                .and_then(|inputs| inputs.iter().find(|i| i["name"] == target_input));
            let expression = declared
                .and_then(|i| i["expression"].as_str())
                .unwrap_or("$.data");
            let value = apply_expression(expression, delivered);
            fields.push(json!({
                "nodeInputName": target_input,
                "content": { "document": value },
                "source": {
                    "nodeName": source,
                    "outputFieldName": source_output,
                    "expression": expression,
                },
                "type": trace_data_type(declared.and_then(|i| i["type"].as_str())),
            }));
            inputs.push((target_input.to_string(), value));
        }
        run.traces.push(json!({
            "nodeInputTrace": {
                "nodeName": name,
                "timestamp": timestamp(),
                "fields": fields,
            }
        }));

        match node["type"].as_str() {
            Some("Output") => {
                let document = inputs
                    .first()
                    .map(|(_, value)| value.clone())
                    .unwrap_or(Value::Null);
                run.outputs.push((name.clone(), document));
            }
            Some("Condition") => {
                let taken = first_satisfied_condition(node, &inputs)?;
                run.traces.push(json!({
                    "conditionNodeResultTrace": {
                        "nodeName": name,
                        "timestamp": timestamp(),
                        "satisfiedConditions": taken
                            .iter()
                            .map(|condition| json!({ "conditionName": condition }))
                            .collect::<Vec<_>>(),
                    }
                }));
                let Some(condition) = taken else {
                    continue;
                };
                for edge in connections.iter().filter(|c| {
                    c["type"] == "Conditional"
                        && c["source"] == name.as_str()
                        && c["configuration"]["conditional"]["condition"] == condition.as_str()
                }) {
                    queue.push_back(edge["target"].as_str().unwrap_or_default().to_string());
                }
            }
            _ => {
                let input = inputs
                    .first()
                    .map(|(_, value)| value.clone())
                    .unwrap_or(Value::Null);
                let reply = synthetic_reply(&input);
                for output in node["outputs"].as_array().into_iter().flatten() {
                    let output_name = output["name"].as_str().unwrap_or("document");
                    produced.insert((name.clone(), output_name.to_string()), reply.clone());
                    let edges = data_edges(&name, output_name);
                    push_output_trace(
                        &mut run,
                        &name,
                        output_name,
                        &reply,
                        Some(node),
                        &edges,
                        nodes,
                    );
                    for edge in edges {
                        let target = edge["target"].as_str().unwrap_or_default();
                        if !has_conditional_entry(target) {
                            queue.push_back(target.to_string());
                        }
                    }
                }
            }
        }
    }
    Ok(run)
}

fn push_output_trace(
    run: &mut FlowRun,
    node: &str,
    output: &str,
    value: &Value,
    definition: Option<&Value>,
    edges: &[&Value],
    nodes: &[Value],
) {
    let declared = definition
        .and_then(|n| n["outputs"].as_array())
        .and_then(|outputs| outputs.iter().find(|o| o["name"] == output));
    let next: Vec<Value> = edges
        .iter()
        .filter(|edge| {
            nodes
                .iter()
                .any(|candidate| candidate["name"] == edge["target"])
        })
        .map(|edge| {
            json!({
                "nodeName": edge["target"],
                "inputFieldName": edge["configuration"]["data"]["targetInput"],
            })
        })
        .collect();
    run.traces.push(json!({
        "nodeOutputTrace": {
            "nodeName": node,
            "timestamp": timestamp(),
            "fields": [{
                "nodeOutputName": output,
                "content": { "document": value },
                "next": next,
                "type": trace_data_type(declared.and_then(|o| o["type"].as_str())),
            }],
        }
    }));
}

/// The first condition that holds, or the `default` condition (one with no
/// expression) when none does.
fn first_satisfied_condition(
    node: &Value,
    inputs: &[(String, Value)],
) -> Result<Option<String>, String> {
    let conditions = node["configuration"]["condition"]["conditions"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut fallback = None;
    for condition in &conditions {
        let name = condition["name"].as_str().unwrap_or_default();
        match condition["expression"].as_str() {
            Some(expression) => {
                if ConditionParser::new(expression, inputs)?.evaluate()? {
                    return Ok(Some(name.to_string()));
                }
            }
            None => fallback = Some(name.to_string()),
        }
    }
    Ok(fallback)
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Operand(Value),
    Compare(String),
    And,
    Or,
    Not,
    Open,
    Close,
}

/// Condition expressions: `==`, `!=`, `>`, `>=`, `<`, `<=` over input names and
/// string, number and boolean literals, with `and`, `or`, `not` and parentheses.
struct ConditionParser {
    tokens: Vec<Token>,
    at: usize,
}

impl ConditionParser {
    fn new(expression: &str, inputs: &[(String, Value)]) -> Result<Self, String> {
        let bad = || format!("The condition expression '{expression}' is not valid.");
        let chars: Vec<char> = expression.chars().collect();
        let mut tokens = Vec::new();
        let mut i = 0;
        while i < chars.len() {
            let c = chars[i];
            match c {
                c if c.is_whitespace() => i += 1,
                '(' => {
                    tokens.push(Token::Open);
                    i += 1;
                }
                ')' => {
                    tokens.push(Token::Close);
                    i += 1;
                }
                '"' | '\'' => {
                    let end = chars[i + 1..]
                        .iter()
                        .position(|other| *other == c)
                        .ok_or_else(bad)?;
                    let text: String = chars[i + 1..i + 1 + end].iter().collect();
                    tokens.push(Token::Operand(json!(text)));
                    i += end + 2;
                }
                '=' | '!' | '>' | '<' => {
                    let double = chars.get(i + 1) == Some(&'=');
                    let operator: String = if double {
                        [c, '='].iter().collect()
                    } else {
                        c.to_string()
                    };
                    if !matches!(operator.as_str(), "==" | "!=" | ">" | ">=" | "<" | "<=") {
                        return Err(bad());
                    }
                    tokens.push(Token::Compare(operator));
                    i += if double { 2 } else { 1 };
                }
                c if c.is_ascii_digit() || c == '-' => {
                    let start = i;
                    i += 1;
                    while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                        i += 1;
                    }
                    let text: String = chars[start..i].iter().collect();
                    let number: f64 = text.parse().map_err(|_| bad())?;
                    tokens.push(Token::Operand(json!(number)));
                }
                c if c.is_alphabetic() || c == '_' => {
                    let start = i;
                    while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                        i += 1;
                    }
                    let word: String = chars[start..i].iter().collect();
                    tokens.push(match word.as_str() {
                        "and" => Token::And,
                        "or" => Token::Or,
                        "not" => Token::Not,
                        "true" => Token::Operand(json!(true)),
                        "false" => Token::Operand(json!(false)),
                        name => Token::Operand(
                            inputs
                                .iter()
                                .find(|(input, _)| input == name)
                                .map(|(_, value)| value.clone())
                                .unwrap_or(Value::Null),
                        ),
                    });
                }
                _ => return Err(bad()),
            }
        }
        Ok(Self { tokens, at: 0 })
    }

    fn evaluate(mut self) -> Result<bool, String> {
        let result = self.or()?;
        if self.at != self.tokens.len() {
            return Err("The condition expression has unexpected trailing text.".to_string());
        }
        Ok(result)
    }

    fn or(&mut self) -> Result<bool, String> {
        let mut result = self.and()?;
        while self.tokens.get(self.at) == Some(&Token::Or) {
            self.at += 1;
            let right = self.and()?;
            result = result || right;
        }
        Ok(result)
    }

    fn and(&mut self) -> Result<bool, String> {
        let mut result = self.not()?;
        while self.tokens.get(self.at) == Some(&Token::And) {
            self.at += 1;
            let right = self.not()?;
            result = result && right;
        }
        Ok(result)
    }

    fn not(&mut self) -> Result<bool, String> {
        if self.tokens.get(self.at) == Some(&Token::Not) {
            self.at += 1;
            return Ok(!self.not()?);
        }
        if self.tokens.get(self.at) == Some(&Token::Open) {
            self.at += 1;
            let result = self.or()?;
            if self.tokens.get(self.at) != Some(&Token::Close) {
                return Err("The condition expression has an unclosed parenthesis.".to_string());
            }
            self.at += 1;
            return Ok(result);
        }
        self.comparison()
    }

    fn operand(&mut self) -> Result<Value, String> {
        match self.tokens.get(self.at) {
            Some(Token::Operand(value)) => {
                let value = value.clone();
                self.at += 1;
                Ok(value)
            }
            _ => Err("The condition expression is missing an operand.".to_string()),
        }
    }

    fn comparison(&mut self) -> Result<bool, String> {
        let left = self.operand()?;
        let Some(Token::Compare(operator)) = self.tokens.get(self.at).cloned() else {
            return Ok(left == json!(true));
        };
        self.at += 1;
        let right = self.operand()?;
        let ordering = match (left.as_f64(), right.as_f64(), left.as_str(), right.as_str()) {
            (Some(a), Some(b), _, _) => a.partial_cmp(&b),
            (_, _, Some(a), Some(b)) => Some(a.cmp(b)),
            _ => None,
        };
        use std::cmp::Ordering::{Equal, Greater, Less};
        Ok(match operator.as_str() {
            "==" => ordering == Some(Equal) || (ordering.is_none() && left == right),
            "!=" => !(ordering == Some(Equal) || (ordering.is_none() && left == right)),
            ">" => ordering == Some(Greater),
            ">=" => matches!(ordering, Some(Greater | Equal)),
            "<" => ordering == Some(Less),
            _ => matches!(ordering, Some(Less | Equal)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn holds(expression: &str, inputs: &[(&str, Value)]) -> bool {
        let inputs: Vec<(String, Value)> = inputs
            .iter()
            .map(|(name, value)| (name.to_string(), value.clone()))
            .collect();
        ConditionParser::new(expression, &inputs)
            .unwrap()
            .evaluate()
            .unwrap()
    }

    #[test]
    fn conditions_compare_and_combine() {
        let inputs = [("A", json!(5)), ("B", json!("x"))];
        assert!(holds("A > 3", &inputs));
        assert!(!holds("A < 3", &inputs));
        assert!(holds("A >= 5 and B == \"x\"", &inputs));
        assert!(holds("A == 1 or B == 'x'", &inputs));
        assert!(holds("not (A == 1)", &inputs));
        assert!(holds("B != 'y'", &inputs));
        assert!(!holds("Missing > 1", &inputs));
    }

    #[test]
    fn broken_conditions_are_errors() {
        for broken in ["A >", "(A > 1", "A = 1", "A > 1 junk 2", "'open"] {
            let error = ConditionParser::new(broken, &[("A".to_string(), json!(1))])
                .and_then(ConditionParser::evaluate);
            assert!(error.is_err(), "{broken}");
        }
    }

    fn data_edge(source: &str, target: &str) -> Value {
        json!({
            "name": format!("{source}{target}"),
            "type": "Data",
            "source": source,
            "target": target,
            "configuration": {"data": {"sourceOutput": "document", "targetInput": "document"}}
        })
    }

    fn input_node() -> Value {
        json!({"name": "Start", "type": "Input", "outputs": [{"name": "document", "type": "Number"}]})
    }

    fn output_node(name: &str) -> Value {
        json!({"name": name, "type": "Output", "inputs": [{"name": "document", "type": "Number", "expression": "$.data"}]})
    }

    fn trace_kinds(run: &FlowRun) -> Vec<String> {
        run.traces
            .iter()
            .map(|trace| {
                let (kind, body) = trace.as_object().unwrap().iter().next().unwrap();
                format!("{kind}:{}", body["nodeName"].as_str().unwrap())
            })
            .collect()
    }

    #[test]
    fn a_condition_runs_only_the_branch_it_takes() {
        let definition = json!({
            "nodes": [
                input_node(),
                {
                    "name": "Check",
                    "type": "Condition",
                    "configuration": {"condition": {"conditions": [
                        {"name": "big", "expression": "value > 5"},
                        {"name": "default"}
                    ]}},
                    "inputs": [{"name": "value", "type": "Number", "expression": "$.data"}]
                },
                output_node("Big"),
                output_node("Small"),
                output_node("Unconnected")
            ],
            "connections": [
                {
                    "name": "toCheck", "type": "Data", "source": "Start", "target": "Check",
                    "configuration": {"data": {"sourceOutput": "document", "targetInput": "value"}}
                },
                {"name": "b", "type": "Conditional", "source": "Check", "target": "Big",
                 "configuration": {"conditional": {"condition": "big"}}},
                {"name": "s", "type": "Conditional", "source": "Check", "target": "Small",
                 "configuration": {"conditional": {"condition": "default"}}},
                data_edge("Start", "Big"),
                data_edge("Start", "Small")
            ]
        });

        let big = run(Some(&definition), "Start", "document", &json!(9)).unwrap();
        assert_eq!(big.outputs, vec![("Big".to_string(), json!(9))]);
        let small = run(Some(&definition), "Start", "document", &json!(1)).unwrap();
        assert_eq!(small.outputs, vec![("Small".to_string(), json!(1))]);
        assert_eq!(
            big.traces[2]["conditionNodeResultTrace"]["satisfiedConditions"],
            json!([{"conditionName": "big"}])
        );
    }

    #[test]
    fn the_trace_follows_the_real_graph_through_intermediate_nodes() {
        let definition = json!({
            "nodes": [
                input_node(),
                {
                    "name": "Model",
                    "type": "Prompt",
                    "inputs": [{"name": "document", "type": "String", "expression": "$.data"}],
                    "outputs": [{"name": "document", "type": "String"}]
                },
                output_node("End")
            ],
            "connections": [data_edge("Start", "Model"), data_edge("Model", "End")]
        });
        let result = run(Some(&definition), "Start", "document", &json!("hi")).unwrap();
        assert_eq!(
            trace_kinds(&result),
            [
                "nodeOutputTrace:Start",
                "nodeInputTrace:Model",
                "nodeOutputTrace:Model",
                "nodeInputTrace:End"
            ]
        );
        assert_eq!(
            result.traces[3]["nodeInputTrace"]["fields"][0]["source"]["nodeName"],
            "Model"
        );
        assert_eq!(
            result.outputs,
            vec![("End".to_string(), json!("Flow output for input: hi"))]
        );
    }

    #[test]
    fn a_non_string_document_stays_a_value() {
        let definition = json!({
            "nodes": [input_node(), output_node("End")],
            "connections": [data_edge("Start", "End")]
        });
        let document = json!({"a": [1, 2]});
        let result = run(Some(&definition), "Start", "document", &document).unwrap();
        assert_eq!(result.outputs, vec![("End".to_string(), document.clone())]);
        assert_eq!(
            result.traces[0]["nodeOutputTrace"]["fields"][0]["content"]["document"],
            document
        );
    }

    #[test]
    fn expressions_select_within_the_delivered_value() {
        assert_eq!(
            apply_expression("$.data.a.b", &json!({"a": {"b": 1}})),
            json!(1)
        );
        assert_eq!(apply_expression("$.data", &json!(2)), json!(2));
    }

    #[test]
    fn input_nodes_and_outputs_are_validated() {
        let definition = json!({"nodes": [input_node(), output_node("End")]});
        assert!(validate_input(Some(&definition), "Start", "document").is_ok());
        assert!(validate_input(Some(&definition), "End", "document").is_err());
        assert!(validate_input(Some(&definition), "Nope", "document").is_err());
        assert!(validate_input(Some(&definition), "Start", "other").is_err());
        assert!(validate_input(None, "Anything", "x").is_ok());
    }
}
