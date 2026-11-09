//! Walking a flow definition's graph for an `InvokeFlow`: from the input node
//! along its connections to the output nodes the run reaches, with the trace
//! events that walk produces. Condition nodes really evaluate their conditions and
//! iterators and collectors really iterate and collect; a node that would call a
//! model or another service produces a synthetic reply.

use std::collections::{HashMap, HashSet, VecDeque};

use chrono::Utc;
use serde_json::{json, Value};

/// The name output events carry when a definition has no output node of its own.
const DEFAULT_OUTPUT_NODE: &str = "FlowOutputNode";

/// The node types whose real work is a call to a model or another service.
const SYNTHETIC_NODE_TYPES: [&str; 8] = [
    "Prompt",
    "KnowledgeBase",
    "Lex",
    "LambdaFunction",
    "Storage",
    "Agent",
    "Retrieval",
    "InlineCode",
];

const ITERATOR_ITEM_OUTPUT: &str = "arrayItem";
const ITERATOR_SIZE_OUTPUT: &str = "arraySize";
const COLLECTOR_OUTPUT: &str = "collectedArray";

/// How deep iterators can nest and how many nodes one invocation can run. Items
/// multiply through nested iterators, and a request's array is the caller's word,
/// not a workload to run unbounded on a request thread.
const MAX_ITERATOR_NESTING: usize = 8;
const MAX_NODE_RUNS: usize = 100_000;

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

/// A flow definition's connections, indexed by the nodes they join.
struct Graph<'a> {
    nodes: HashMap<&'a str, &'a Value>,
    /// Node names in declaration order, which is the order ties run in.
    order: Vec<&'a str>,
    incoming: HashMap<&'a str, Vec<&'a Value>>,
    outgoing: HashMap<&'a str, Vec<&'a Value>>,
}

impl<'a> Graph<'a> {
    fn new(nodes: &'a [Value], connections: &'a [Value]) -> Self {
        let mut graph = Graph {
            nodes: HashMap::new(),
            order: Vec::new(),
            incoming: HashMap::new(),
            outgoing: HashMap::new(),
        };
        for node in nodes {
            let name = node["name"].as_str().unwrap_or_default();
            if graph.nodes.insert(name, node).is_none() {
                graph.order.push(name);
            }
        }
        for connection in connections {
            let source = connection["source"].as_str().unwrap_or_default();
            let target = connection["target"].as_str().unwrap_or_default();
            graph.outgoing.entry(source).or_default().push(connection);
            graph.incoming.entry(target).or_default().push(connection);
        }
        graph
    }

    fn incoming(&self, name: &str, kind: &str) -> Vec<&'a Value> {
        let edges = self.incoming.get(name).map(Vec::as_slice).unwrap_or(&[]);
        edges
            .iter()
            .copied()
            .filter(|c| c["type"] == kind)
            .collect()
    }

    fn data_edges(&self, source: &str, output: &str) -> Vec<&'a Value> {
        let edges = self.outgoing.get(source).map(Vec::as_slice).unwrap_or(&[]);
        edges
            .iter()
            .copied()
            .filter(|c| c["type"] == "Data" && source_output(c) == output)
            .collect()
    }

    fn node_type(&self, name: &str) -> Option<&'a str> {
        self.nodes.get(name).and_then(|node| node["type"].as_str())
    }

    /// What one iterator repeats per item: everything `arrayItem` reaches, up to
    /// the collector that closes this iterator. `arraySize` is a single value, so
    /// what only it feeds runs once, outside.
    fn iteration_body(&self, iterator: &'a str) -> Result<HashSet<&'a str>, String> {
        let mut body = HashSet::new();
        let mut seen = HashSet::new();
        let mut pending = vec![(iterator, 0usize)];
        while let Some((name, depth)) = pending.pop() {
            for edge in self.outgoing.get(name).into_iter().flatten() {
                if name == iterator && source_output(edge) == ITERATOR_SIZE_OUTPUT {
                    continue;
                }
                let target = edge["target"].as_str().unwrap_or_default();
                let target_depth = match self.node_type(target) {
                    None => continue,
                    Some("Collector") if depth == 0 => continue,
                    Some("Collector") => depth - 1,
                    Some("Iterator") if target == iterator => {
                        return Err(cycle_error());
                    }
                    Some("Iterator") if depth == MAX_ITERATOR_NESTING => {
                        return Err(format!(
                            "Iterators are nested more than {MAX_ITERATOR_NESTING} deep under node {iterator}."
                        ));
                    }
                    Some("Iterator") => depth + 1,
                    Some(_) => depth,
                };
                body.insert(target);
                if seen.insert((target, target_depth)) {
                    pending.push((target, target_depth));
                }
            }
        }
        Ok(body)
    }
}

fn cycle_error() -> String {
    "The flow's connections form a cycle, so a node would wait on its own output.".to_string()
}

fn source_output(edge: &Value) -> &str {
    edge["configuration"]["data"]["sourceOutput"]
        .as_str()
        .unwrap_or("document")
}

fn target_input(edge: &Value) -> &str {
    edge["configuration"]["data"]["targetInput"]
        .as_str()
        .unwrap_or("document")
}

/// A node's inputs by name, and their trace fields.
type GatheredInputs = (Vec<(String, Value)>, Vec<Value>);

/// One run's progress through a [`Graph`].
struct Walk<'a> {
    graph: &'a Graph<'a>,
    run: FlowRun,
    /// Values each finished node's outputs hold.
    produced: HashMap<(&'a str, &'a str), Value>,
    /// Conditional connections taken, as (condition node, target).
    taken: HashSet<(&'a str, &'a str)>,
    /// What each collector has gathered so far.
    collected: HashMap<&'a str, Vec<Value>>,
    node_runs: usize,
}

impl<'a> Walk<'a> {
    /// Run `members`, each only once everything that could feed it has settled,
    /// so a node joining several branches sees all of them. An iterator stands in
    /// for its whole body here: nothing in the body starts before everything the
    /// body needs from outside it has settled, and whatever follows the body
    /// waits for the last item.
    fn settle(&mut self, members: &[&'a str]) -> Result<(), String> {
        let graph = self.graph;
        let member_set: HashSet<&str> = members.iter().copied().collect();

        let mut bodies: HashMap<&'a str, HashSet<&'a str>> = HashMap::new();
        for name in members {
            if graph.node_type(name) == Some("Iterator") {
                bodies.insert(name, graph.iteration_body(name)?);
            }
        }
        let mut stands_in_for: HashMap<&'a str, &'a str> = HashMap::new();
        for (iterator, body) in &bodies {
            // An iterator inside another belongs to that one's pass.
            if !bodies
                .iter()
                .any(|(other, others)| other != iterator && others.contains(iterator))
            {
                stands_in_for.extend(body.iter().map(|node| (*node, *iterator)));
            }
        }
        let unit = |name: &'a str| stands_in_for.get(name).copied().unwrap_or(name);

        let mut waiting: HashMap<&str, usize> = HashMap::new();
        let mut followers: HashMap<&str, Vec<&'a str>> = HashMap::new();
        for name in members {
            for edge in graph.incoming.get(name).into_iter().flatten() {
                let Some(source) = edge["source"].as_str() else {
                    continue;
                };
                if !member_set.contains(source) || unit(source) == unit(name) {
                    continue;
                }
                *waiting.entry(unit(name)).or_default() += 1;
                followers.entry(unit(source)).or_default().push(unit(name));
            }
        }

        let units: Vec<&'a str> = members
            .iter()
            .copied()
            .filter(|name| unit(name) == *name)
            .collect();
        let mut ready: VecDeque<&'a str> = units
            .iter()
            .copied()
            .filter(|name| !waiting.contains_key(name))
            .collect();
        let mut settled = 0;
        while let Some(name) = ready.pop_front() {
            settled += 1;
            self.run_node(name, bodies.get(name))?;
            for follower in followers.remove(name).unwrap_or_default() {
                let count = waiting.entry(follower).or_default();
                *count -= 1;
                if *count == 0 {
                    ready.push_back(follower);
                }
            }
        }
        if settled < units.len() {
            return Err(cycle_error());
        }
        Ok(())
    }

    /// The inputs a node has received, and their trace fields.
    fn gather_inputs(&self, name: &'a str) -> Result<GatheredInputs, String> {
        let node = self.graph.nodes[name];
        let mut inputs = Vec::new();
        let mut fields = Vec::new();
        for edge in self.graph.incoming(name, "Data") {
            let source = edge["source"].as_str().unwrap_or_default();
            let Some(delivered) = self.produced.get(&(source, source_output(edge))) else {
                continue;
            };
            let (input, field) = deliver(node, edge, delivered)?;
            inputs.push(input);
            fields.push(field);
        }
        Ok((inputs, fields))
    }

    /// Whether a settled node runs at all: a condition's target only when that
    /// condition was taken, anything else only when something reached it.
    fn is_reached(&self, name: &'a str) -> Result<bool, String> {
        let conditional = self.graph.incoming(name, "Conditional");
        if !conditional.is_empty() {
            return Ok(conditional.iter().any(|edge| {
                let source = edge["source"].as_str().unwrap_or_default();
                self.taken.contains(&(source, name))
            }));
        }
        let delivered = self.graph.incoming(name, "Data").iter().any(|edge| {
            let source = edge["source"].as_str().unwrap_or_default();
            self.produced.contains_key(&(source, source_output(edge)))
        });
        if self.graph.node_type(name) == Some("Collector") && !self.collected.contains_key(name) {
            if delivered {
                return Err(format!(
                    "Collector node {name} isn't downstream of an iterator node."
                ));
            }
            return Ok(false);
        }
        Ok(delivered || self.collected.contains_key(name))
    }

    fn run_node(
        &mut self,
        name: &'a str,
        iteration_body: Option<&HashSet<&'a str>>,
    ) -> Result<(), String> {
        if !self.is_reached(name)? {
            return Ok(());
        }
        self.node_runs += 1;
        if self.node_runs > MAX_NODE_RUNS {
            return Err(format!(
                "The flow ran more than {MAX_NODE_RUNS} nodes in one invocation."
            ));
        }
        let node = self.graph.nodes[name];
        let kind = node["type"].as_str().unwrap_or_default();
        let (inputs, fields) = self.gather_inputs(name)?;
        if inputs.is_empty() && kind != "Collector" {
            return Err(format!("Node {name} received none of its inputs."));
        }
        self.run.traces.push(json!({
            "nodeInputTrace": {
                "nodeName": name,
                "timestamp": timestamp(),
                "fields": fields,
            }
        }));

        match kind {
            "Output" => {
                self.run
                    .outputs
                    .push((name.to_string(), inputs[0].1.clone()));
            }
            "Condition" => self.run_condition(name, node, &inputs)?,
            "Iterator" => {
                let body = iteration_body
                    .ok_or_else(|| format!("Iterator node {name} has no iteration scope."))?;
                self.run_iterator(name, &inputs, body)?;
            }
            "Collector" => {
                let items = self.collected.remove(name).unwrap_or_default();
                self.emit(name, COLLECTOR_OUTPUT, Value::Array(items));
            }
            kind if SYNTHETIC_NODE_TYPES.contains(&kind) => {
                let reply = synthetic_reply(&combined_input(&inputs));
                for output in node["outputs"].as_array().into_iter().flatten() {
                    let output = output["name"].as_str().unwrap_or("document");
                    self.emit(name, output, reply.clone());
                }
            }
            other => {
                // A DoWhile loop's data contract (the controller's inputs, and
                // whether its condition continues or exits) isn't documented
                // consistently enough to run without guessing.
                return Err(format!(
                    "Node {name} has type {other}, which fakecloud's InvokeFlow does not run."
                ));
            }
        }
        Ok(())
    }

    fn emit(&mut self, name: &'a str, output: &'a str, value: Value) {
        let edges = self.graph.data_edges(name, output);
        push_output_trace(
            &mut self.run,
            name,
            output,
            &value,
            self.graph.nodes.get(name).copied(),
            &edges,
            &self.graph.nodes,
        );
        self.produced.insert((name, output), value);
    }

    fn run_condition(
        &mut self,
        name: &'a str,
        node: &Value,
        inputs: &[(String, Value)],
    ) -> Result<(), String> {
        let taken = first_satisfied_condition(node, inputs)?;
        self.run.traces.push(json!({
            "conditionNodeResultTrace": {
                "nodeName": name,
                "timestamp": timestamp(),
                "satisfiedConditions": taken
                    .iter()
                    .map(|condition| json!({ "conditionName": condition }))
                    .collect::<Vec<_>>(),
            }
        }));
        // A definition without a default condition fails validation
        // (`MissingDefaultCondition`), as does a condition with no connection
        // (`UnsatisfiedConnectionConditions`): neither has a branch to continue on.
        let Some(condition) = taken else {
            return Err(format!(
                "No condition of node {name} was satisfied and it has no default condition."
            ));
        };
        let edges = self
            .graph
            .outgoing
            .get(name)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let mut any = false;
        for edge in edges.iter().filter(|c| {
            c["type"] == "Conditional"
                && c["configuration"]["conditional"]["condition"] == condition.as_str()
        }) {
            self.taken
                .insert((name, edge["target"].as_str().unwrap_or_default()));
            any = true;
        }
        if !any {
            return Err(format!(
                "Condition {condition} of node {name} has no connection to follow."
            ));
        }
        Ok(())
    }

    /// Send each item through the body in turn, as the guide says, one at a time.
    /// An output node in the body answers once per item; a collector gathers what
    /// the body delivers it and runs after the last one.
    fn run_iterator(
        &mut self,
        name: &'a str,
        inputs: &[(String, Value)],
        body: &HashSet<&'a str>,
    ) -> Result<(), String> {
        let graph = self.graph;
        let array = inputs
            .iter()
            .find(|(input, _)| input == "array")
            .unwrap_or(&inputs[0]);
        let Value::Array(items) = &array.1 else {
            return Err(format!("Iterator node {name} needs an array input."));
        };
        self.emit(name, ITERATOR_SIZE_OUTPUT, json!(items.len()));

        // What the iterator and its body hand to the collectors that close it.
        let handoffs: Vec<&'a Value> = body
            .iter()
            .copied()
            .chain([name])
            .flat_map(|source| graph.outgoing.get(source).into_iter().flatten().copied())
            .filter(|edge| {
                let target = edge["target"].as_str().unwrap_or_default();
                edge["type"] == "Data"
                    && !body.contains(target)
                    && graph.node_type(target) == Some("Collector")
                    && source_output(edge) != ITERATOR_SIZE_OUTPUT
            })
            .collect();
        for edge in &handoffs {
            let target = edge["target"].as_str().unwrap_or_default();
            self.collected.entry(target).or_default();
        }

        let members: Vec<&'a str> = graph
            .order
            .iter()
            .copied()
            .filter(|node| body.contains(node))
            .collect();
        for item in items {
            self.emit(name, ITERATOR_ITEM_OUTPUT, item.clone());
            self.settle(&members)?;
            for edge in &handoffs {
                let source = edge["source"].as_str().unwrap_or_default();
                let target = edge["target"].as_str().unwrap_or_default();
                let Some(delivered) = self.produced.get(&(source, source_output(edge))) else {
                    continue;
                };
                let ((_, value), field) = deliver(graph.nodes[target], edge, delivered)?;
                self.run.traces.push(json!({
                    "nodeInputTrace": {
                        "nodeName": target,
                        "timestamp": timestamp(),
                        "fields": [field],
                    }
                }));
                self.collected.entry(target).or_default().push(value);
            }
            self.produced.retain(|(node, output), _| {
                !(body.contains(node) || (*node == name && *output == ITERATOR_ITEM_OUTPUT))
            });
            self.taken.retain(|(_, target)| !body.contains(target));
        }
        Ok(())
    }
}

/// One connection's value as the input it feeds: the value after the input's
/// expression, and its trace field.
fn deliver(
    node: &Value,
    edge: &Value,
    delivered: &Value,
) -> Result<((String, Value), Value), String> {
    let input = target_input(edge);
    let declared = node["inputs"]
        .as_array()
        .and_then(|inputs| inputs.iter().find(|i| i["name"] == input));
    let expression = declared
        .and_then(|i| i["expression"].as_str())
        .unwrap_or("$.data");
    let value = crate::flow_expression::apply(expression, delivered)?;
    let field = json!({
        "nodeInputName": input,
        "content": { "document": value },
        "source": {
            "nodeName": edge["source"],
            "outputFieldName": source_output(edge),
            "expression": expression,
        },
        "type": trace_data_type(declared.and_then(|i| i["type"].as_str())),
    });
    Ok(((input.to_string(), value), field))
}

/// What a node with several inputs works from: all of them, by name.
fn combined_input(inputs: &[(String, Value)]) -> Value {
    match inputs {
        [(_, single)] => single.clone(),
        several => Value::Object(several.iter().cloned().collect()),
    }
}

/// The output a request means when it names none (`nodeOutputName` is optional):
/// the input node's own, which the guide fixes as `document`.
pub(crate) fn default_input_output(definition: Option<&Value>, node: &str) -> String {
    definition
        .and_then(|d| d["nodes"].as_array())
        .and_then(|nodes| nodes.iter().find(|candidate| candidate["name"] == node))
        .and_then(|found| found["outputs"][0]["name"].as_str())
        .unwrap_or("document")
        .to_string()
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

    let graph = Graph::new(nodes, connections);
    let mut walk = Walk {
        graph: &graph,
        run: FlowRun::default(),
        produced: HashMap::new(),
        taken: HashSet::new(),
        collected: HashMap::new(),
        node_runs: 0,
    };
    walk.emit(start_node, start_output, document.clone());
    let members: Vec<&str> = graph
        .order
        .iter()
        .copied()
        .filter(|name| *name != start_node)
        .collect();
    walk.settle(&members)?;
    Ok(walk.run)
}

fn push_output_trace(
    run: &mut FlowRun,
    node: &str,
    output: &str,
    value: &Value,
    definition: Option<&Value>,
    edges: &[&Value],
    nodes: &HashMap<&str, &Value>,
) {
    let declared = definition
        .and_then(|n| n["outputs"].as_array())
        .and_then(|outputs| outputs.iter().find(|o| o["name"] == output));
    let next: Vec<Value> = edges
        .iter()
        .filter(|edge| nodes.contains_key(edge["target"].as_str().unwrap_or_default()))
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
    // Parse them all first: a broken later condition is broken whichever earlier
    // one holds.
    let declared: Option<HashSet<&str>> = node["inputs"]
        .as_array()
        .map(|declared| declared.iter().filter_map(|i| i["name"].as_str()).collect());
    let mut parsed = Vec::new();
    let mut fallback = None;
    for condition in &conditions {
        let name = condition["name"].as_str().unwrap_or_default();
        match condition["expression"].as_str() {
            Some(expression) => parsed.push((
                name,
                ConditionParser::new(expression, inputs, declared.as_ref())?,
            )),
            None => fallback = Some(name.to_string()),
        }
    }
    for (name, condition) in parsed {
        if condition.evaluate()? {
            return Ok(Some(name.to_string()));
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
    /// `declared` is the node's input names: a name outside them is no input, and
    /// a declared one no connection delivered has no value to compare.
    fn new(
        expression: &str,
        inputs: &[(String, Value)],
        declared: Option<&HashSet<&str>>,
    ) -> Result<Self, String> {
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
                        name => {
                            if declared.is_some_and(|declared| !declared.contains(name)) {
                                return Err(format!(
                                    "The condition expression '{expression}' refers to {name}, which isn't an input of the node."
                                ));
                            }
                            let value = inputs.iter().find(|(input, _)| input == name);
                            let (_, value) = value.ok_or_else(|| {
                                format!(
                                    "The condition expression '{expression}' refers to input {name}, which received no value."
                                )
                            })?;
                            Token::Operand(value.clone())
                        }
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
        let ordering = match (left.as_f64(), right.as_f64()) {
            (Some(a), Some(b)) => a.partial_cmp(&b),
            _ => None,
        };
        use std::cmp::Ordering::{Equal, Greater, Less};
        // The guide lists `>`, `>=`, `<`, `<=` for numbers only.
        if !matches!(operator.as_str(), "==" | "!=") && !(left.is_number() && right.is_number()) {
            return Err(format!(
                "{operator} compares numbers, not {left} and {right}."
            ));
        }
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
        ConditionParser::new(expression, &inputs, None)
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
    }

    #[test]
    fn broken_conditions_are_errors() {
        for broken in ["A >", "(A > 1", "A = 1", "A > 1 junk 2", "'open"] {
            let error = ConditionParser::new(broken, &[("A".to_string(), json!(1))], None)
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

    fn node(name: &str, kind: &str, inputs: &[&str], outputs: &[&str]) -> Value {
        json!({
            "name": name,
            "type": kind,
            "inputs": inputs
                .iter()
                .map(|input| json!({"name": input, "type": "String", "expression": "$.data"}))
                .collect::<Vec<_>>(),
            "outputs": outputs
                .iter()
                .map(|output| json!({"name": output, "type": "String"}))
                .collect::<Vec<_>>(),
        })
    }

    fn edge(source: (&str, &str), target: (&str, &str)) -> Value {
        json!({
            "name": format!("{}{}", source.0, target.0),
            "type": "Data",
            "source": source.0,
            "target": target.0,
            "configuration": {"data": {"sourceOutput": source.1, "targetInput": target.1}}
        })
    }

    fn prompt(name: &str, inputs: &[&str]) -> Value {
        node(name, "Prompt", inputs, &["modelCompletion"])
    }

    fn outputs_of(run: &FlowRun) -> Vec<(&str, &Value)> {
        run.outputs
            .iter()
            .map(|(name, value)| (name.as_str(), value))
            .collect()
    }

    #[test]
    fn a_node_joining_branches_waits_for_all_of_them() {
        // Declared and connected so the short branch reaches the join first.
        let definition = json!({
            "nodes": [
                input_node(),
                prompt("Join", &["short", "long"]),
                prompt("Long1", &["document"]),
                prompt("Long2", &["document"]),
                output_node("End")
            ],
            "connections": [
                edge(("Start", "document"), ("Join", "short")),
                edge(("Start", "document"), ("Long1", "document")),
                edge(("Long1", "modelCompletion"), ("Long2", "document")),
                edge(("Long2", "modelCompletion"), ("Join", "long")),
                edge(("Join", "modelCompletion"), ("End", "document"))
            ]
        });
        let result = run(Some(&definition), "Start", "document", &json!(1)).unwrap();
        let join = result
            .traces
            .iter()
            .find(|trace| trace["nodeInputTrace"]["nodeName"] == "Join")
            .unwrap();
        assert_eq!(
            join["nodeInputTrace"]["fields"].as_array().unwrap().len(),
            2
        );
        let reply = result.outputs[0].1.as_str().unwrap();
        assert!(reply.contains("\"short\":1"), "{reply}");
        assert!(reply.contains("Flow output for input"), "{reply}");
        assert!(reply.contains("\"long\":"), "{reply}");
        assert_eq!(
            result
                .traces
                .iter()
                .filter(|trace| trace["nodeInputTrace"]["nodeName"] == "Join")
                .count(),
            1
        );
    }

    #[test]
    fn a_join_runs_on_what_the_taken_branch_delivers() {
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
                prompt("Taken", &["document"]),
                prompt("NotTaken", &["document"]),
                prompt("Join", &["a", "b"]),
                output_node("End")
            ],
            "connections": [
                edge(("Start", "document"), ("Check", "value")),
                edge(("Start", "document"), ("Taken", "document")),
                edge(("Start", "document"), ("NotTaken", "document")),
                {"name": "t", "type": "Conditional", "source": "Check", "target": "Taken",
                 "configuration": {"conditional": {"condition": "big"}}},
                {"name": "n", "type": "Conditional", "source": "Check", "target": "NotTaken",
                 "configuration": {"conditional": {"condition": "default"}}},
                edge(("Taken", "modelCompletion"), ("Join", "a")),
                edge(("NotTaken", "modelCompletion"), ("Join", "b")),
                edge(("Join", "modelCompletion"), ("End", "document"))
            ]
        });
        let result = run(Some(&definition), "Start", "document", &json!(9)).unwrap();
        assert_eq!(result.outputs.len(), 1);
        assert!(trace_kinds(&result).contains(&"nodeInputTrace:Taken".to_string()));
        assert!(!trace_kinds(&result).contains(&"nodeInputTrace:NotTaken".to_string()));
    }

    #[test]
    fn a_cycle_is_an_error_not_a_silent_stop() {
        let definition = json!({
            "nodes": [
                input_node(),
                prompt("A", &["first", "back"]),
                prompt("B", &["document"]),
                output_node("End")
            ],
            "connections": [
                edge(("Start", "document"), ("A", "first")),
                edge(("A", "modelCompletion"), ("B", "document")),
                edge(("B", "modelCompletion"), ("A", "back")),
                edge(("B", "modelCompletion"), ("End", "document"))
            ]
        });
        let error = run(Some(&definition), "Start", "document", &json!(1)).unwrap_err();
        assert!(error.contains("cycle"), "{error}");
    }

    fn iterator_definition(with_collector: bool) -> Value {
        let mut nodes = vec![
            input_node(),
            node("Each", "Iterator", &["array"], &["arrayItem", "arraySize"]),
        ];
        let mut connections = vec![edge(("Start", "document"), ("Each", "array"))];
        if with_collector {
            nodes.push(prompt("Model", &["document"]));
            nodes.push(node(
                "Gather",
                "Collector",
                &["arrayItem", "arraySize"],
                &["collectedArray"],
            ));
            nodes.push(output_node("End"));
            connections.extend([
                edge(("Each", "arrayItem"), ("Model", "document")),
                edge(("Model", "modelCompletion"), ("Gather", "arrayItem")),
                edge(("Each", "arraySize"), ("Gather", "arraySize")),
                edge(("Gather", "collectedArray"), ("End", "document")),
            ]);
        } else {
            nodes.push(output_node("End"));
            connections.push(edge(("Each", "arrayItem"), ("End", "document")));
        }
        json!({"nodes": nodes, "connections": connections})
    }

    #[test]
    fn an_iterator_gives_an_output_node_one_response_per_item() {
        let definition = iterator_definition(false);
        let result = run(Some(&definition), "Start", "document", &json!([1, "b"])).unwrap();
        assert_eq!(
            outputs_of(&result),
            [("End", &json!(1)), ("End", &json!("b"))]
        );
        let empty = run(Some(&definition), "Start", "document", &json!([])).unwrap();
        assert!(empty.outputs.is_empty());
    }

    #[test]
    fn a_collector_gathers_what_the_body_produced_for_each_item() {
        let definition = iterator_definition(true);
        let result = run(Some(&definition), "Start", "document", &json!(["a", "b"])).unwrap();
        assert_eq!(
            outputs_of(&result),
            [(
                "End",
                &json!(["Flow output for input: a", "Flow output for input: b"])
            )]
        );
        assert_eq!(
            trace_kinds(&result)
                .iter()
                .filter(|kind| *kind == "nodeInputTrace:Model")
                .count(),
            2
        );
        let empty = run(Some(&definition), "Start", "document", &json!([])).unwrap();
        assert_eq!(outputs_of(&empty), [("End", &json!([]))]);
    }

    #[test]
    fn an_iterator_needs_an_array_and_a_collector_an_iterator() {
        let definition = iterator_definition(false);
        let error = run(Some(&definition), "Start", "document", &json!("nope")).unwrap_err();
        assert!(error.contains("array"), "{error}");

        let orphan = json!({
            "nodes": [
                input_node(),
                node("Gather", "Collector", &["arrayItem"], &["collectedArray"]),
                output_node("End")
            ],
            "connections": [
                edge(("Start", "document"), ("Gather", "arrayItem")),
                edge(("Gather", "collectedArray"), ("End", "document"))
            ]
        });
        let error = run(Some(&orphan), "Start", "document", &json!(1)).unwrap_err();
        assert!(error.contains("iterator"), "{error}");
    }

    #[test]
    fn nested_iterators_multiply_and_their_collectors_nest() {
        let definition = json!({
            "nodes": [
                input_node(),
                node("Outer", "Iterator", &["array"], &["arrayItem", "arraySize"]),
                node("Inner", "Iterator", &["array"], &["arrayItem", "arraySize"]),
                node("GatherInner", "Collector", &["arrayItem"], &["collectedArray"]),
                node("GatherOuter", "Collector", &["arrayItem"], &["collectedArray"]),
                output_node("End")
            ],
            "connections": [
                edge(("Start", "document"), ("Outer", "array")),
                edge(("Outer", "arrayItem"), ("Inner", "array")),
                edge(("Inner", "arrayItem"), ("GatherInner", "arrayItem")),
                edge(("GatherInner", "collectedArray"), ("GatherOuter", "arrayItem")),
                edge(("GatherOuter", "collectedArray"), ("End", "document"))
            ]
        });
        let document = json!([[1, 2], [3]]);
        let result = run(Some(&definition), "Start", "document", &document).unwrap();
        assert_eq!(outputs_of(&result), [("End", &document)]);
    }

    #[test]
    fn runaway_iteration_is_refused() {
        let definition = iterator_definition(false);
        let document = Value::Array(vec![json!(1); MAX_NODE_RUNS + 1]);
        let error = run(Some(&definition), "Start", "document", &document).unwrap_err();
        assert!(error.contains("more than"), "{error}");
    }

    #[test]
    fn node_types_that_are_not_run_are_refused_when_reached() {
        for kind in ["Loop", "LoopInput", "LoopController", "Mystery"] {
            let definition = json!({
                "nodes": [
                    input_node(),
                    node("Odd", kind, &["document"], &["document"]),
                    output_node("End")
                ],
                "connections": [
                    data_edge("Start", "Odd"),
                    data_edge("Odd", "End")
                ]
            });
            let error = run(Some(&definition), "Start", "document", &json!(1)).unwrap_err();
            assert!(error.contains(kind), "{error}");
        }
    }

    fn condition_definition(conditions: Value, connected: &[&str]) -> Value {
        let mut connections = vec![
            edge(("Start", "document"), ("Check", "value")),
            data_edge("Start", "End"),
        ];
        connections.extend(connected.iter().map(|condition| {
            json!({"name": condition, "type": "Conditional", "source": "Check", "target": "End",
                   "configuration": {"conditional": {"condition": condition}}})
        }));
        json!({
            "nodes": [
                input_node(),
                {
                    "name": "Check",
                    "type": "Condition",
                    "configuration": {"condition": {"conditions": conditions}},
                    "inputs": [{"name": "value", "type": "Number", "expression": "$.data"}]
                },
                output_node("End")
            ],
            "connections": connections
        })
    }

    #[test]
    fn a_condition_naming_no_input_is_an_error() {
        // Previously `Missing != 1` was true because Missing read as null, and a
        // broken later condition went unnoticed when an earlier one held.
        for conditions in [
            json!([{"name": "c", "expression": "Missing != 1"}, {"name": "default"}]),
            json!([
                {"name": "c", "expression": "value == 1"},
                {"name": "d", "expression": "Missing != 1"},
                {"name": "default"}
            ]),
        ] {
            let definition = condition_definition(conditions, &["c", "default"]);
            let error = run(Some(&definition), "Start", "document", &json!(1)).unwrap_err();
            assert!(error.contains("Missing"), "{error}");
        }
    }

    #[test]
    fn a_condition_with_no_branch_to_take_is_an_error() {
        let no_default =
            condition_definition(json!([{"name": "c", "expression": "value > 5"}]), &["c"]);
        let error = run(Some(&no_default), "Start", "document", &json!(1)).unwrap_err();
        assert!(error.contains("no default"), "{error}");

        let unconnected = condition_definition(
            json!([{"name": "c", "expression": "value > 5"}, {"name": "default"}]),
            &["c"],
        );
        let error = run(Some(&unconnected), "Start", "document", &json!(1)).unwrap_err();
        assert!(error.contains("no connection"), "{error}");
    }

    #[test]
    fn ordering_operators_compare_numbers_only() {
        let inputs = [("A".to_string(), json!("x"))];
        let error = ConditionParser::new("A > 'a'", &inputs, None)
            .and_then(ConditionParser::evaluate)
            .unwrap_err();
        assert!(error.contains("compares numbers"), "{error}");
        // `==` is type-strict rather than an error.
        assert!(!holds("A == 1", &[("A", json!("1"))]));
    }

    #[test]
    fn expressions_apply_to_what_a_connection_delivers() {
        let definition = json!({
            "nodes": [
                input_node(),
                {"name": "End", "type": "Output",
                 "inputs": [{"name": "document", "type": "Number", "expression": "$.data.a[1]"}]}
            ],
            "connections": [data_edge("Start", "End")]
        });
        let result = run(
            Some(&definition),
            "Start",
            "document",
            &json!({"a": [1, 2]}),
        )
        .unwrap();
        assert_eq!(outputs_of(&result), [("End", &json!(2))]);
        assert!(run(Some(&definition), "Start", "document", &json!("scalar")).is_err());
    }

    #[test]
    fn an_omitted_output_name_means_the_input_nodes_own() {
        let definition = json!({"nodes": [input_node(), output_node("End")]});
        assert_eq!(default_input_output(Some(&definition), "Start"), "document");
        assert_eq!(default_input_output(None, "Start"), "document");
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
