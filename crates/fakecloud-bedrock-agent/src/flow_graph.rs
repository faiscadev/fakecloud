//! A flow definition's graph: its nodes and the connections joining them, and
//! the iteration scopes its iterator nodes open. `ValidateFlowDefinition`
//! checks a definition with it and `InvokeFlow` walks one.

use std::collections::{HashMap, HashSet};

use serde_json::Value;

pub const ITERATOR_ITEM_OUTPUT: &str = "arrayItem";
pub const ITERATOR_SIZE_OUTPUT: &str = "arraySize";

/// How deep iterators can nest. Items multiply through nested iterators, and a
/// request's array is the caller's word, not a workload to run unbounded on a
/// request thread.
pub const MAX_ITERATOR_NESTING: usize = 8;

/// A data connection's source output, `document` when it names none.
pub fn source_output(edge: &Value) -> &str {
    edge["configuration"]["data"]["sourceOutput"]
        .as_str()
        .unwrap_or("document")
}

/// A data connection's target input, `document` when it names none.
pub fn target_input(edge: &Value) -> &str {
    edge["configuration"]["data"]["targetInput"]
        .as_str()
        .unwrap_or("document")
}

pub fn cycle_error() -> String {
    "The flow's connections form a cycle, so a node would wait on its own output.".to_string()
}

/// What one iterator repeats per item.
#[derive(Debug, Default)]
pub struct IterationBody<'a> {
    /// The body's nodes in declaration order, which is the order ties run in.
    pub members: Vec<&'a str>,
    set: HashSet<&'a str>,
}

impl IterationBody<'_> {
    pub fn contains(&self, name: &str) -> bool {
        self.set.contains(name)
    }
}

/// A flow definition's connections, indexed by the nodes they join.
pub struct Graph<'a> {
    pub nodes: HashMap<&'a str, &'a Value>,
    /// Node names in declaration order, which is the order ties run in.
    pub order: Vec<&'a str>,
    pub incoming: HashMap<&'a str, Vec<&'a Value>>,
    pub outgoing: HashMap<&'a str, Vec<&'a Value>>,
}

impl<'a> Graph<'a> {
    pub fn new(nodes: &'a [Value], connections: &'a [Value]) -> Self {
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

    /// A node's incoming connections of one type (`Data` or `Conditional`).
    pub fn incoming(&self, name: &str, kind: &str) -> Vec<&'a Value> {
        let edges = self.incoming.get(name).map(Vec::as_slice).unwrap_or(&[]);
        edges
            .iter()
            .copied()
            .filter(|c| c["type"] == kind)
            .collect()
    }

    /// The data connections leaving one output of a node.
    pub fn data_edges(&self, source: &str, output: &str) -> Vec<&'a Value> {
        let edges = self.outgoing.get(source).map(Vec::as_slice).unwrap_or(&[]);
        edges
            .iter()
            .copied()
            .filter(|c| c["type"] == "Data" && source_output(c) == output)
            .collect()
    }

    pub fn node_type(&self, name: &str) -> Option<&'a str> {
        self.nodes.get(name).and_then(|node| node["type"].as_str())
    }

    /// The iterator nodes, in declaration order.
    pub fn iterators(&self) -> impl Iterator<Item = &'a str> + '_ {
        self.order
            .iter()
            .copied()
            .filter(|name| self.node_type(name) == Some("Iterator"))
    }

    /// What one iterator repeats per item: everything `arrayItem` reaches, up to
    /// the collector that closes this iterator. `arraySize` is a single value, so
    /// what only it feeds runs once, outside.
    pub fn iteration_body(&self, iterator: &str) -> Result<IterationBody<'a>, String> {
        let mut set = HashSet::new();
        let mut seen = HashSet::new();
        let mut pending: Vec<(&str, usize)> = vec![(iterator, 0usize)];
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
                set.insert(target);
                if seen.insert((target, target_depth)) {
                    pending.push((target, target_depth));
                }
            }
        }
        let members = self
            .order
            .iter()
            .copied()
            .filter(|name| set.contains(name))
            .collect();
        Ok(IterationBody { members, set })
    }
}

/// Why a node two iterators repeat can't run: neither iterator is inside the
/// other, so nothing says whose items it runs once per.
pub fn ambiguous_iteration(node: &str, first: &str, second: &str) -> String {
    format!(
        "Node {node} is downstream of both iterator {first} and iterator {second}, and neither iterator is inside the other, so which one's items it runs for is ambiguous."
    )
}

/// The nodes two unnested iterators both repeat, as (node, first iterator,
/// second iterator) in declaration order. Iterators whose scope is broken (a
/// cycle, or nesting past [`MAX_ITERATOR_NESTING`]) are left out: that is its
/// own error.
pub fn ambiguous_iterations<'a>(graph: &Graph<'a>) -> Vec<(&'a str, &'a str, &'a str)> {
    let bodies: Vec<(&'a str, IterationBody<'a>)> = graph
        .iterators()
        .filter_map(|name| graph.iteration_body(name).ok().map(|body| (name, body)))
        .collect();
    let mut found = Vec::new();
    for (i, (first, first_body)) in bodies.iter().enumerate() {
        for (second, second_body) in &bodies[i + 1..] {
            if first_body.contains(second) || second_body.contains(first) {
                continue;
            }
            if let Some(node) = first_body
                .members
                .iter()
                .find(|node| second_body.contains(node))
            {
                found.push((*node, *first, *second));
            }
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn node(name: &str, kind: &str) -> Value {
        json!({"name": name, "type": kind})
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

    #[test]
    fn a_body_lists_its_nodes_in_declaration_order() {
        let nodes = [
            node("Each", "Iterator"),
            node("B", "Prompt"),
            node("A", "Prompt"),
            node("Gather", "Collector"),
            node("After", "Output"),
        ];
        let connections = [
            edge(("Each", "arrayItem"), ("A", "document")),
            edge(("A", "modelCompletion"), ("B", "document")),
            edge(("B", "modelCompletion"), ("Gather", "arrayItem")),
            edge(("Each", "arraySize"), ("Gather", "arraySize")),
            edge(("Gather", "collectedArray"), ("After", "document")),
        ];
        let graph = Graph::new(&nodes, &connections);
        let body = graph.iteration_body("Each").unwrap();
        assert_eq!(body.members, ["B", "A"]);
        assert!(!body.contains("Gather") && !body.contains("After"));
    }

    #[test]
    fn two_unnested_iterators_reaching_one_node_are_ambiguous() {
        let nodes = [
            node("One", "Iterator"),
            node("Two", "Iterator"),
            node("Shared", "Prompt"),
            node("End", "Output"),
        ];
        let connections = [
            edge(("One", "arrayItem"), ("Shared", "a")),
            edge(("Two", "arrayItem"), ("Shared", "b")),
            edge(("Shared", "modelCompletion"), ("End", "document")),
        ];
        let graph = Graph::new(&nodes, &connections);
        assert_eq!(ambiguous_iterations(&graph), [("Shared", "One", "Two")]);

        // Nested iterators share their inner body without ambiguity.
        let nested = [
            edge(("One", "arrayItem"), ("Two", "array")),
            edge(("Two", "arrayItem"), ("Shared", "a")),
            edge(("Shared", "modelCompletion"), ("End", "document")),
        ];
        let graph = Graph::new(&nodes, &nested);
        assert!(ambiguous_iterations(&graph).is_empty());
    }
}
