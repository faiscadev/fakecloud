//! Walking a flow definition's graph for an `InvokeFlow`: from the input node
//! along its connections to the output nodes the run reaches, with the trace
//! events that walk produces. Condition nodes really evaluate their conditions and
//! iterators and collectors really iterate and collect; a node that would call a
//! model or another service produces a synthetic reply.

use std::collections::{HashMap, HashSet, VecDeque};
use std::rc::Rc;

use chrono::Utc;
use fakecloud_bedrock_agent::flow_graph::{
    ambiguous_iteration, cycle_error, source_output, target_input, Graph, IterationBody,
    ITERATOR_ITEM_OUTPUT, ITERATOR_SIZE_OUTPUT,
};
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

const COLLECTOR_OUTPUT: &str = "collectedArray";

/// How many nodes one invocation can run. Items multiply through nested
/// iterators, and a request's array is the caller's word, not a workload to run
/// unbounded on a request thread.
const MAX_NODE_RUNS: usize = 100_000;

/// What one run of a flow produces.
#[derive(Debug, Default)]
pub(crate) struct FlowRun {
    /// `FlowTrace` union values, in the order they happened.
    pub traces: Vec<Value>,
    /// Each output node reached and the document it received.
    pub outputs: Vec<(String, Value)>,
    /// Why the run stopped before finishing, when a node failed. What ran before
    /// the failure stays in `traces` and `outputs`: the stream had already sent it.
    pub failure: Option<String>,
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

/// An iterator's scope: its body, or why it has none. A broken scope is only an
/// error once the iterator is reached, so a branch the run never takes can't fail
/// it.
type Scope<'a> = Rc<Result<IterationBody<'a>, String>>;

/// How one set of nodes is scheduled, worked out once and reused for every pass
/// over the set (every item of the iterator whose body it is).
struct Plan<'a> {
    /// What runs, in declaration order: the nodes outside any iteration, and
    /// each outermost iterator standing in for its whole body.
    units: Vec<&'a str>,
    /// The scope of each iterator among `units`.
    scopes: HashMap<&'a str, Scope<'a>>,
    /// How many units each unit waits on, and which units wait on it.
    waiting: HashMap<&'a str, usize>,
    followers: HashMap<&'a str, Vec<&'a str>>,
}

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
    /// Each iterator's scope, computed the first time a plan needs it.
    scopes: HashMap<&'a str, Scope<'a>>,
    node_runs: usize,
}

impl<'a> Walk<'a> {
    fn scope(&mut self, iterator: &'a str) -> Scope<'a> {
        let graph = self.graph;
        self.scopes
            .entry(iterator)
            .or_insert_with(|| Rc::new(graph.iteration_body(iterator)))
            .clone()
    }

    /// Schedule `members`, each to run only once everything that could feed it
    /// has settled, so a node joining several branches sees all of them. An
    /// iterator stands in for its whole body here: nothing in the body starts
    /// before everything the body needs from outside it has settled, and
    /// whatever follows the body waits for the last item.
    fn plan(&mut self, members: &[&'a str]) -> Plan<'a> {
        let graph = self.graph;
        let member_set: HashSet<&str> = members.iter().copied().collect();
        let iterators: Vec<(&'a str, Scope<'a>)> = members
            .iter()
            .copied()
            .filter(|name| graph.node_type(name) == Some("Iterator"))
            .map(|name| (name, self.scope(name)))
            .collect();
        let inside = |name: &str, others: &[(&'a str, Scope<'a>)]| {
            others.iter().any(|(other, scope)| {
                *other != name
                    && scope
                        .as_ref()
                        .as_ref()
                        .is_ok_and(|body| body.contains(name))
            })
        };
        // An iterator inside another belongs to that one's pass.
        let outermost: Vec<(&'a str, Scope<'a>)> = iterators
            .iter()
            .filter(|(name, _)| !inside(name, &iterators))
            .cloned()
            .collect();

        let mut scopes: HashMap<&'a str, Scope<'a>> = HashMap::new();
        for (i, (first, first_scope)) in outermost.iter().enumerate() {
            let Ok(first_body) = first_scope.as_ref() else {
                continue;
            };
            for (second, second_scope) in &outermost[i + 1..] {
                let Ok(second_body) = second_scope.as_ref() else {
                    continue;
                };
                if let Some(node) = first_body
                    .members
                    .iter()
                    .find(|node| second_body.contains(node))
                {
                    let error = Rc::new(Err(ambiguous_iteration(node, first, second)));
                    scopes.entry(*first).or_insert_with(|| error.clone());
                    scopes.entry(*second).or_insert(error);
                }
            }
        }
        let mut stands_in_for: HashMap<&'a str, &'a str> = HashMap::new();
        for (iterator, scope) in outermost {
            let scope = scopes.entry(iterator).or_insert(scope);
            if let Ok(body) = scope.as_ref() {
                stands_in_for.extend(body.members.iter().map(|node| (*node, iterator)));
            }
        }
        let unit = |name: &'a str| stands_in_for.get(name).copied().unwrap_or(name);

        let mut waiting: HashMap<&'a str, usize> = HashMap::new();
        let mut followers: HashMap<&'a str, Vec<&'a str>> = HashMap::new();
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
        let units = members
            .iter()
            .copied()
            .filter(|name| unit(name) == *name)
            .collect();
        Plan {
            units,
            scopes,
            waiting,
            followers,
        }
    }

    /// Run one pass over a planned set of nodes.
    fn settle(&mut self, plan: &Plan<'a>) -> Result<(), String> {
        let mut waiting = plan.waiting.clone();
        let mut followers = plan.followers.clone();
        let mut ready: VecDeque<&'a str> = plan
            .units
            .iter()
            .copied()
            .filter(|name| !waiting.contains_key(name))
            .collect();
        while let Some(name) = ready.pop_front() {
            self.run_node(name, plan.scopes.get(name).cloned())?;
            for follower in followers.remove(name).unwrap_or_default() {
                let count = waiting.entry(follower).or_default();
                *count -= 1;
                if *count == 0 {
                    waiting.remove(follower);
                    ready.push_back(follower);
                }
            }
        }
        // Units still waiting sit on a cycle (or behind one). That only stops
        // the run when the cycle is reached: one no branch leads into never runs.
        for name in plan
            .units
            .iter()
            .copied()
            .filter(|name| waiting.contains_key(name))
        {
            if self.is_reached(name)? {
                return Err(cycle_error());
            }
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

    fn run_node(&mut self, name: &'a str, scope: Option<Scope<'a>>) -> Result<(), String> {
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
                let scope =
                    scope.ok_or_else(|| format!("Iterator node {name} has no iteration scope."))?;
                match scope.as_ref() {
                    Ok(body) => self.run_iterator(name, &inputs, body)?,
                    Err(error) => return Err(error.clone()),
                }
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
        body: &IterationBody<'a>,
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
            .members
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

        // The body's graph doesn't change from one item to the next.
        let plan = self.plan(&body.members);
        for item in items {
            self.emit(name, ITERATOR_ITEM_OUTPUT, item.clone());
            self.settle(&plan)?;
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
            // Forget what this item ran, so the next one starts clean. A branch
            // a condition outside the body took into it stays taken: that
            // condition ran once, for every item.
            self.produced.retain(|(node, output), _| {
                !(body.contains(node) || (*node == name && *output == ITERATOR_ITEM_OUTPUT))
            });
            self.taken.retain(|(source, _)| !body.contains(source));
        }
        Ok(())
    }
}

/// A node's inputs by name, and their trace fields.
type GatheredInputs = (Vec<(String, Value)>, Vec<Value>);

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

/// Run a definition from its input node. A node that fails ends the run there,
/// with what ran before it kept and the reason in [`FlowRun::failure`].
pub(crate) fn run(
    definition: Option<&Value>,
    start_node: &str,
    start_output: &str,
    document: &Value,
) -> FlowRun {
    let empty = Vec::new();
    let nodes = definition
        .and_then(|d| d["nodes"].as_array())
        .unwrap_or(&empty);
    let connections = definition
        .and_then(|d| d["connections"].as_array())
        .unwrap_or(&empty);

    // A definition with nowhere for output to go still answers the caller.
    if !nodes.iter().any(|node| node["type"] == "Output") {
        return FlowRun {
            outputs: vec![(DEFAULT_OUTPUT_NODE.to_string(), synthetic_reply(document))],
            ..FlowRun::default()
        };
    }

    let graph = Graph::new(nodes, connections);
    let mut walk = Walk {
        graph: &graph,
        run: FlowRun::default(),
        produced: HashMap::new(),
        taken: HashSet::new(),
        collected: HashMap::new(),
        scopes: HashMap::new(),
        node_runs: 0,
    };
    walk.emit(start_node, start_output, document.clone());
    let members: Vec<&str> = graph
        .order
        .iter()
        .copied()
        .filter(|name| *name != start_node)
        .collect();
    let plan = walk.plan(&members);
    if let Err(failure) = walk.settle(&plan) {
        walk.run.failure = Some(failure);
    }
    walk.run
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

/// The first condition that holds, in the order they are declared ("If more
/// than one condition is satisfied, the earlier condition takes precedence"), or
/// the `default` condition (one with no expression) when none does. Conditions
/// are evaluated one at a time and only until one holds, so a later condition
/// never runs once an earlier one is satisfied. One that does run and refers to
/// an input nothing delivered a value to (its branch wasn't taken) isn't
/// satisfied, and evaluation moves on to the next; one naming something that
/// isn't an input of the node fails it.
fn first_satisfied_condition(
    node: &Value,
    inputs: &[(String, Value)],
) -> Result<Option<String>, String> {
    let empty = Vec::new();
    let conditions = node["configuration"]["condition"]["conditions"]
        .as_array()
        .unwrap_or(&empty);
    let declared: Option<HashSet<&str>> = node["inputs"]
        .as_array()
        .map(|declared| declared.iter().filter_map(|i| i["name"].as_str()).collect());
    let mut fallback = None;
    for condition in conditions {
        let name = condition["name"].as_str().unwrap_or_default();
        match condition["expression"].as_str() {
            Some(expression) => {
                let parser = ConditionParser::new(expression, inputs, declared.as_ref())?;
                if !parser.lacks_input && parser.evaluate()? {
                    return Ok(Some(name.to_string()));
                }
            }
            None => fallback = fallback.or_else(|| Some(name.to_string())),
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
    /// Whether the expression names an input no connection delivered a value
    /// to. Comparisons are defined over values only, so such a condition can't
    /// be satisfied.
    lacks_input: bool,
}

impl ConditionParser {
    /// `declared` is the node's input names: a name outside them is no input (an
    /// error), and a declared one no connection delivered leaves the condition
    /// unsatisfiable.
    fn new(
        expression: &str,
        inputs: &[(String, Value)],
        declared: Option<&HashSet<&str>>,
    ) -> Result<Self, String> {
        let bad = || format!("The condition expression '{expression}' is not valid.");
        let chars: Vec<char> = expression.chars().collect();
        let mut tokens = Vec::new();
        let mut lacks_input = false;
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
                            match inputs.iter().find(|(input, _)| input == name) {
                                Some((_, value)) => Token::Operand(value.clone()),
                                None => {
                                    lacks_input = true;
                                    Token::Operand(Value::Null)
                                }
                            }
                        }
                    });
                }
                _ => return Err(bad()),
            }
        }
        Ok(Self {
            tokens,
            at: 0,
            lacks_input,
        })
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

    /// [`super::run`], with a failed run as its failure.
    fn run(
        definition: Option<&Value>,
        start_node: &str,
        start_output: &str,
        document: &Value,
    ) -> Result<FlowRun, String> {
        let mut result = super::run(definition, start_node, start_output, document);
        match result.failure.take() {
            Some(failure) => Err(failure),
            None => Ok(result),
        }
    }

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
    fn a_condition_outside_an_iterator_gates_its_body_for_every_item() {
        let definition = json!({
            "nodes": [
                input_node(),
                {
                    "name": "Check", "type": "Condition",
                    "configuration": {"condition": {"conditions": [
                        {"name": "some", "expression": "count > 0"},
                        {"name": "default"}
                    ]}},
                    "inputs": [{"name": "count", "type": "Number", "expression": "$.data.count"}]
                },
                node("Each", "Iterator", &["array"], &["arrayItem", "arraySize"]),
                prompt("Model", &["item"]),
                output_node("End"),
                output_node("None")
            ],
            "connections": [
                edge(("Start", "document"), ("Check", "count")),
                edge(("Start", "document"), ("Each", "array")),
                edge(("Each", "arrayItem"), ("Model", "item")),
                {"name": "s", "type": "Conditional", "source": "Check", "target": "Model",
                 "configuration": {"conditional": {"condition": "some"}}},
                {"name": "n", "type": "Conditional", "source": "Check", "target": "None",
                 "configuration": {"conditional": {"condition": "default"}}},
                edge(("Model", "modelCompletion"), ("End", "document")),
                data_edge("Start", "None")
            ]
        });
        // The iterator's array is the expression's whole input here, so take
        // the items from `items`.
        let mut definition = definition;
        definition["nodes"][2]["inputs"][0]["expression"] = json!("$.data.items");
        let result = run(
            Some(&definition),
            "Start",
            "document",
            &json!({"count": 3, "items": ["a", "b", "c"]}),
        )
        .unwrap();
        assert_eq!(
            outputs_of(&result),
            [
                ("End", &json!("Flow output for input: a")),
                ("End", &json!("Flow output for input: b")),
                ("End", &json!("Flow output for input: c"))
            ]
        );
    }

    /// Two iterators off one input, and a third node they both feed.
    fn two_iterators_sharing(shared: bool) -> Value {
        let mut connections = vec![
            edge(("Start", "document"), ("Gate", "value")),
            json!({"name": "a", "type": "Conditional", "source": "Gate", "target": "One",
                   "configuration": {"conditional": {"condition": "one"}}}),
            json!({"name": "b", "type": "Conditional", "source": "Gate", "target": "Two",
                   "configuration": {"conditional": {"condition": "default"}}}),
            edge(("Start", "document"), ("One", "array")),
            edge(("Start", "document"), ("Two", "array")),
            edge(("One", "arrayItem"), ("Shared", "a")),
            edge(("Shared", "modelCompletion"), ("End", "document")),
        ];
        if shared {
            connections.push(edge(("Two", "arrayItem"), ("Shared", "b")));
        } else {
            connections.push(edge(("Two", "arrayItem"), ("Other", "document")));
        }
        json!({
            "nodes": [
                input_node(),
                {
                    "name": "Gate", "type": "Condition",
                    "configuration": {"condition": {"conditions": [
                        {"name": "one", "expression": "value == 'never'"},
                        {"name": "default"}
                    ]}},
                    "inputs": [{"name": "value", "type": "Array", "expression": "$.data"}]
                },
                node("One", "Iterator", &["array"], &["arrayItem", "arraySize"]),
                node("Two", "Iterator", &["array"], &["arrayItem", "arraySize"]),
                prompt("Shared", &["a", "b"]),
                output_node("Other"),
                output_node("End")
            ],
            "connections": connections
        })
    }

    #[test]
    fn a_node_two_unnested_iterators_repeat_fails_the_iterator_that_reaches_it() {
        let definition = two_iterators_sharing(true);
        for _ in 0..8 {
            let error = run(Some(&definition), "Start", "document", &json!([1])).unwrap_err();
            assert!(error.contains("ambiguous"), "{error}");
            assert!(
                error.contains("Shared") && error.contains("One") && error.contains("Two"),
                "{error}"
            );
        }
    }

    #[test]
    fn a_broken_iterator_the_run_never_reaches_fails_nothing() {
        // `One` would nest past the limit, but the gate only ever takes `Two`.
        let mut definition = two_iterators_sharing(false);
        let nodes = definition["nodes"].as_array_mut().unwrap();
        let mut connections = Vec::new();
        let mut previous = ("One".to_string(), "arrayItem");
        for depth in 0..=fakecloud_bedrock_agent::flow_graph::MAX_ITERATOR_NESTING {
            let name = format!("Deep{depth}");
            nodes.push(node(
                &name,
                "Iterator",
                &["array"],
                &["arrayItem", "arraySize"],
            ));
            connections.push(edge((&previous.0, previous.1), (&name, "array")));
            previous = (name, "arrayItem");
        }
        definition["connections"]
            .as_array_mut()
            .unwrap()
            .append(&mut connections);
        let result = run(Some(&definition), "Start", "document", &json!([1, 2])).unwrap();
        assert_eq!(
            outputs_of(&result),
            [("Other", &json!(1)), ("Other", &json!(2))]
        );
        // Reached, the same iterator fails the run.
        definition["nodes"][1]["configuration"]["condition"]["conditions"][0]["expression"] =
            json!("value != 'never'");
        let error = run(Some(&definition), "Start", "document", &json!([1])).unwrap_err();
        assert!(error.contains("nested more than"), "{error}");
    }

    #[test]
    fn a_failed_run_keeps_what_ran_before_the_failure() {
        let definition = json!({
            "nodes": [
                input_node(),
                output_node("First"),
                node("Each", "Iterator", &["array"], &["arrayItem", "arraySize"]),
                output_node("End")
            ],
            "connections": [
                data_edge("Start", "First"),
                edge(("Start", "document"), ("Each", "array")),
                edge(("Each", "arrayItem"), ("End", "document"))
            ]
        });
        let result = super::run(Some(&definition), "Start", "document", &json!("nope"));
        assert_eq!(outputs_of(&result), [("First", &json!("nope"))]);
        assert!(result.failure.as_deref().unwrap().contains("array"));
        assert!(trace_kinds(&result).contains(&"nodeInputTrace:Each".to_string()));
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
        // Previously `Missing != 1` was true because Missing read as null.
        let definition = condition_definition(
            json!([{"name": "c", "expression": "Missing != 1"}, {"name": "default"}]),
            &["c", "default"],
        );
        let error = run(Some(&definition), "Start", "document", &json!(1)).unwrap_err();
        assert!(error.contains("Missing"), "{error}");
    }

    #[test]
    fn conditions_are_evaluated_in_order_and_only_until_one_holds() {
        // A broken condition after the one that holds never runs.
        let definition = condition_definition(
            json!([
                {"name": "c", "expression": "value == 1"},
                {"name": "d", "expression": "Missing != 1"},
                {"name": "default"}
            ]),
            &["c", "default"],
        );
        let result = run(Some(&definition), "Start", "document", &json!(1)).unwrap();
        assert_eq!(
            result.traces[2]["conditionNodeResultTrace"]["satisfiedConditions"],
            json!([{"conditionName": "c"}])
        );
    }

    #[test]
    fn a_condition_on_an_input_no_branch_delivered_is_not_satisfied() {
        // `Gate` sends the number to `Upper` only when it's big; `Check` reads
        // both, so for a small number `upper` never gets a value.
        let definition = json!({
            "nodes": [
                input_node(),
                {
                    "name": "Gate", "type": "Condition",
                    "configuration": {"condition": {"conditions": [
                        {"name": "big", "expression": "value > 5"},
                        {"name": "default"}
                    ]}},
                    "inputs": [{"name": "value", "type": "Number", "expression": "$.data"}]
                },
                prompt("Upper", &["document"]),
                {
                    "name": "Check", "type": "Condition",
                    "configuration": {"condition": {"conditions": [
                        {"name": "fromUpper", "expression": "upper == 'x'"},
                        {"name": "small", "expression": "raw < 5"},
                        {"name": "default"}
                    ]}},
                    "inputs": [
                        {"name": "upper", "type": "String", "expression": "$.data"},
                        {"name": "raw", "type": "Number", "expression": "$.data"}
                    ]
                },
                output_node("Small"),
                output_node("Other"),
                output_node("Nowhere")
            ],
            "connections": [
                edge(("Start", "document"), ("Gate", "value")),
                edge(("Start", "document"), ("Upper", "document")),
                {"name": "g", "type": "Conditional", "source": "Gate", "target": "Upper",
                 "configuration": {"conditional": {"condition": "big"}}},
                {"name": "gd", "type": "Conditional", "source": "Gate", "target": "Nowhere",
                 "configuration": {"conditional": {"condition": "default"}}},
                edge(("Upper", "modelCompletion"), ("Check", "upper")),
                edge(("Start", "document"), ("Check", "raw")),
                {"name": "u", "type": "Conditional", "source": "Check", "target": "Other",
                 "configuration": {"conditional": {"condition": "fromUpper"}}},
                {"name": "s", "type": "Conditional", "source": "Check", "target": "Small",
                 "configuration": {"conditional": {"condition": "small"}}},
                {"name": "d", "type": "Conditional", "source": "Check", "target": "Other",
                 "configuration": {"conditional": {"condition": "default"}}},
                data_edge("Start", "Small"),
                data_edge("Start", "Other"),
                data_edge("Start", "Nowhere")
            ]
        });
        let small = run(Some(&definition), "Start", "document", &json!(2)).unwrap();
        assert_eq!(
            outputs_of(&small),
            [("Nowhere", &json!(2)), ("Small", &json!(2))]
        );
        // A big number gives `upper` a value, which isn't 'x', and isn't small.
        let big = run(Some(&definition), "Start", "document", &json!(9)).unwrap();
        assert_eq!(outputs_of(&big), [("Other", &json!(9))]);
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
