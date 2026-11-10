//! `BedrockAgentService` `flows` family — extracted from service.rs by audit-2026-05-19.

use super::*;

/// The flow ID a `flowIdentifier` names: the bare flow ID, or the ARN of a
/// flow in the caller's account and region (`arn:...:flow/<id>`). An ARN of
/// anything else names no flow here.
fn flow_id_of(req: &AwsRequest, identifier: &str) -> Result<String, AwsServiceError> {
    match parse_identifier(req, identifier) {
        Some(Identifier::Id(id)) => Ok(id.to_string()),
        Some(Identifier::Resource(resource)) => resource
            .strip_prefix("flow/")
            .filter(|id| !id.is_empty() && !id.contains('/'))
            .map(str::to_string)
            .ok_or_else(|| flow_not_found(identifier)),
        None => Err(flow_not_found(identifier)),
    }
}

/// The alias ID an `aliasIdentifier` names: the bare alias ID, or the ARN of
/// an alias of `flow_id` in the caller's account and region
/// (`arn:...:flow/<flow-id>/alias/<alias-id>`).
fn alias_id_of(
    req: &AwsRequest,
    flow_id: &str,
    identifier: &str,
) -> Result<String, AwsServiceError> {
    let not_found_alias = || not_found(format!("Flow alias {identifier} not found"));
    match parse_identifier(req, identifier) {
        Some(Identifier::Id(id)) => Ok(id.to_string()),
        Some(Identifier::Resource(resource)) => resource
            .strip_prefix("flow/")
            .and_then(|rest| rest.split_once("/alias/"))
            .filter(|(flow, alias)| *flow == flow_id && !alias.is_empty())
            .map(|(_, alias)| alias.to_string())
            .ok_or_else(not_found_alias),
        None => Err(not_found_alias()),
    }
}

/// An alias routes to a numbered version, never the working draft (the
/// built-in test alias `TSTALIASID` is what runs the draft).
fn check_alias_routing(body: &Value) -> Result<(), AwsServiceError> {
    let routes_to_draft = body["routingConfiguration"]
        .as_array()
        .into_iter()
        .flatten()
        .any(|r| r["flowVersion"] == "DRAFT");
    if routes_to_draft {
        return Err(validation(
            "Alias routing configuration must point to a non draft version. Retry your request with a valid version.",
        ));
    }
    Ok(())
}

/// A request's flow `definition`; an explicit `null` is no definition.
fn definition_of(body: &Value) -> Option<Value> {
    body.get("definition").filter(|d| !d.is_null()).cloned()
}

fn flow_not_found(id: &str) -> AwsServiceError {
    not_found(format!("Flow {id} not found"))
}

fn flow_validation(severity: &str, kind: &str, message: String, details: Value) -> Value {
    json!({
        "severity": severity,
        "type": kind,
        "message": message,
        "details": details,
    })
}

/// The one data slot Input and Output nodes have: an Input node's only output
/// and an Output node's only input are both named `document`.
const DOCUMENT: &str = "document";

/// A flow definition that can't be stored or validated at all, as opposed to
/// one that validates with findings. Node names address nodes, so a name two
/// nodes share makes the whole definition ambiguous.
fn check_definition(definition: &Value) -> Result<(), AwsServiceError> {
    let mut names = std::collections::BTreeSet::new();
    for node in definition["nodes"].as_array().into_iter().flatten() {
        if let Some(name) = node["name"].as_str().filter(|n| !n.is_empty()) {
            if !names.insert(name) {
                return Err(validation(format!("Node name {name} must be unique")));
            }
        }
    }
    Ok(())
}

/// What a stored flow definition has to satisfy on CreateFlow / UpdateFlow:
/// a well-formed definition, and no more Input nodes than the account's
/// `max-number-flow-input-nodes` limit (1) allows.
fn check_stored_definition(definition: &Value, account_id: &str) -> Result<(), AwsServiceError> {
    check_definition(definition)?;
    let inputs = definition["nodes"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|n| n["type"] == "Input")
        .count();
    if inputs > 1 {
        return Err(validation(format!(
            "The limit for max-number-flow-input-nodes is 1, but the requested value is {inputs} for account {account_id}. Retry your request with a value less than or equal to 1."
        )));
    }
    Ok(())
}

/// One node of a definition, as validation sees it.
struct NodeInfo<'a> {
    kind: &'a str,
    /// (name, type, expression) in declaration order.
    inputs: Vec<(&'a str, &'a str, &'a str)>,
    /// (name, type) in declaration order.
    outputs: Vec<(&'a str, &'a str)>,
}

impl NodeInfo<'_> {
    fn input(&self, name: &str) -> Option<(&str, &str)> {
        self.inputs
            .iter()
            .find(|(n, _, _)| *n == name)
            .map(|(_, t, e)| (*t, *e))
    }

    fn output(&self, name: &str) -> Option<&str> {
        self.outputs
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, t)| *t)
    }
}

/// Whether a data connection's source output can feed its target input. The
/// types must match, unless the input's expression selects into an `Object`
/// or `Array` (anything but the whole value, `$.data`): what it selects has
/// no static type.
fn compatible_types(source_type: &str, target_type: &str, expression: &str) -> bool {
    source_type == target_type
        || (matches!(source_type, "Object" | "Array") && expression != "$.data")
}

/// Validation of a flow definition, as `ValidateFlowDefinition` reports it
/// and `PrepareFlow` records it. A flow with any `Error` finding fails to
/// prepare; `Warning`s alone leave it prepared.
///
/// Findings come in this order: the missing starting (`Input`, an error) and
/// ending (`Output`, a warning) nodes; then, node by node in declaration
/// order, missing and unknown inputs/outputs of Input and Output nodes and
/// each input fed by no data connection or by more than one; then,
/// connection by connection, unknown ends, a missing configuration, unknown
/// source outputs / target inputs (only when both ends exist), incompatible
/// data types and duplicates;
/// then cycles; then nodes no starting node reaches (warnings). A node-level
/// finding on a node no starting node reaches is only a warning.
fn validate_definition(definition: &Value) -> Vec<Value> {
    use std::collections::{BTreeMap, BTreeSet};

    let empty = Vec::new();
    let nodes = definition["nodes"].as_array().unwrap_or(&empty);
    let connections = definition["connections"].as_array().unwrap_or(&empty);
    fn s(v: &Value) -> &str {
        v.as_str().unwrap_or_default()
    }

    let mut out = Vec::new();
    // Nodes by name, and their names in declaration order. A node with no
    // name can't be addressed by a connection; it is reported (the model has
    // no dedicated validation type for it, so as `Unspecified`).
    let mut by_name: BTreeMap<&str, NodeInfo> = BTreeMap::new();
    let mut order: Vec<&str> = Vec::new();
    for (index, n) in nodes.iter().enumerate() {
        let name = s(&n["name"]);
        if name.is_empty() {
            out.push(flow_validation(
                "Error",
                "Unspecified",
                format!("Node at index {index} has no name."),
                json!({ "unspecified": {} }),
            ));
            continue;
        }
        if by_name.contains_key(name) {
            // Rejected up front by `check_definition`.
            continue;
        }
        let inputs = n["inputs"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|i| (s(&i["name"]), s(&i["type"]), s(&i["expression"])))
            .collect();
        let outputs = n["outputs"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|o| (s(&o["name"]), s(&o["type"])))
            .collect();
        by_name.insert(
            name,
            NodeInfo {
                kind: s(&n["type"]),
                inputs,
                outputs,
            },
        );
        order.push(name);
    }

    // A connection with no configuration joins nothing: it neither feeds an
    // input nor makes its target reachable.
    let configured = |c: &Value| {
        c["configuration"].get("data").is_some() || c["configuration"].get("conditional").is_some()
    };
    // (target node, target input) -> data connections feeding it. The
    // source need not exist or have the named output: the input is still
    // spoken for (those are the connection's own findings).
    let mut feeds: BTreeMap<(&str, &str), usize> = BTreeMap::new();
    // Connections joining two existing nodes, by source, in declaration order.
    let mut edges: BTreeMap<&str, Vec<(&str, &str)>> = BTreeMap::new();
    for c in connections.iter().filter(|c| configured(c)) {
        let (source, target) = (s(&c["source"]), s(&c["target"]));
        let Some(target_node) = by_name.get(target) else {
            continue;
        };
        if let Some(data) = c["configuration"].get("data") {
            let input = s(&data["targetInput"]);
            if target_node.input(input).is_some() {
                *feeds.entry((target, input)).or_default() += 1;
            }
        }
        if by_name.contains_key(source) {
            edges
                .entry(source)
                .or_default()
                .push((target, s(&c["name"])));
        }
    }

    let starts: Vec<&str> = order
        .iter()
        .copied()
        .filter(|n| by_name[n].kind == "Input")
        .collect();
    let mut reached: BTreeSet<&str> = starts.iter().copied().collect();
    let mut frontier = starts.clone();
    while let Some(node) = frontier.pop() {
        for (target, _) in edges.get(node).into_iter().flatten() {
            if reached.insert(target) {
                frontier.push(target);
            }
        }
    }

    if starts.is_empty() {
        out.push(flow_validation(
            "Error",
            "MissingStartingNodes",
            "The flow is missing a required Flow Input node. Add an Input node to define the flow's starting point and input parameters.".to_string(),
            json!({ "missingStartingNodes": {} }),
        ));
    }
    if !by_name.values().any(|n| n.kind == "Output") {
        out.push(flow_validation(
            "Warning",
            "MissingEndingNodes",
            "The flow is missing a required Flow Output node. Add an Output node to define what the flow returns when it completes.".to_string(),
            json!({ "missingEndingNodes": {} }),
        ));
    }

    for name in &order {
        let node = &by_name[name];
        let severity = if reached.contains(name) {
            "Error"
        } else {
            "Warning"
        };
        match node.kind {
            "Input" => {
                if node.output(DOCUMENT).is_none() {
                    out.push(flow_validation(
                        severity,
                        "MissingNodeOutput",
                        format!("The node '{name}' is missing the required output '{DOCUMENT}'. Add this output to the node definition."),
                        json!({ "missingNodeOutput": { "node": name, "output": DOCUMENT } }),
                    ));
                }
                for (output, _) in node.outputs.iter().filter(|(o, _)| *o != DOCUMENT) {
                    out.push(flow_validation(
                        severity,
                        "UnknownNodeOutput",
                        format!("The node '{name}' has an unknown output '{output}' that is not supported by this node type. Remove the output or use a valid output name for this node type."),
                        json!({ "unknownNodeOutput": { "node": name, "output": output } }),
                    ));
                }
            }
            "Output" => {
                if node.input(DOCUMENT).is_none() {
                    out.push(flow_validation(
                        severity,
                        "MissingNodeInput",
                        format!("The node '{name}' is missing the required input '{DOCUMENT}'. Add this input to the node definition."),
                        json!({ "missingNodeInput": { "node": name, "input": DOCUMENT } }),
                    ));
                }
                for (input, _, _) in node.inputs.iter().filter(|(i, _, _)| *i != DOCUMENT) {
                    out.push(flow_validation(
                        severity,
                        "UnknownNodeInput",
                        format!("The node '{name}' has an unknown input '{input}' that is not supported by this node type. Remove the input or use a valid input name for this node type."),
                        json!({ "unknownNodeInput": { "node": name, "input": input } }),
                    ));
                }
            }
            _ => {}
        }
        for (input, _, _) in &node.inputs {
            match feeds.get(&(*name, *input)).copied().unwrap_or(0) {
                0 => out.push(flow_validation(
                    severity,
                    "UnfulfilledNodeInput",
                    format!("The input '{input}' in node '{name}' is undefined. Connect a source value to this input."),
                    json!({ "unfulfilledNodeInput": { "node": name, "input": input } }),
                )),
                1 => {}
                _ => out.push(flow_validation(
                    severity,
                    "MultipleNodeInputConnections",
                    format!("The input '{input}' in node '{name}' has multiple incoming connections. Remove all but one connection to this input, as each input can only receive data from a single source."),
                    json!({ "multipleNodeInputConnections": { "node": name, "input": input } }),
                )),
            }
        }
    }

    let mut seen_transfers = BTreeSet::new();
    for c in connections {
        let name = s(&c["name"]);
        let (source, target) = (s(&c["source"]), s(&c["target"]));
        let between = format!("The connection between the nodes '{source}' and '{target}'");
        let source_node = by_name.get(source);
        let target_node = by_name.get(target);
        if source_node.is_none() {
            out.push(flow_validation(
                "Error",
                "UnknownConnectionSource",
                format!("{between} references an unknown source node '{source}'. Update the connection to reference a valid node that exists in the flow, or add the missing node to the flow."),
                json!({ "unknownConnectionSource": { "connection": name } }),
            ));
        }
        if target_node.is_none() {
            out.push(flow_validation(
                "Error",
                "UnknownConnectionTarget",
                format!("{between} references an unknown target node '{target}'. Update the connection to reference a valid node that exists in the flow, or add the missing node to the flow."),
                json!({ "unknownConnectionTarget": { "connection": name } }),
            ));
        }
        if !configured(c) {
            out.push(flow_validation(
                "Error",
                "MissingConnectionConfiguration",
                format!("{between} is missing its configuration. Add configuration to specify how data should flow between these nodes."),
                json!({ "missingConnectionConfiguration": { "connection": name } }),
            ));
            continue;
        }
        // Two connections duplicate each other when they make the same
        // transfer: the same output into the same input for a data
        // connection, the same condition for a conditional one. One node
        // feeding two different inputs of another is two distinct transfers.
        let transfer = if let Some(data) = c["configuration"].get("data") {
            let source_output = s(&data["sourceOutput"]);
            let target_input = s(&data["targetInput"]);
            let output_type = source_node.and_then(|n| n.output(source_output));
            let input = target_node.and_then(|n| n.input(target_input));
            // A connection with an unknown end is reported for that alone:
            // its outputs and inputs aren't checked.
            let both_ends = source_node.is_some() && target_node.is_some();
            if both_ends && output_type.is_none() {
                out.push(flow_validation(
                    "Error",
                    "UnknownConnectionSourceOutput",
                    format!("{between} references an unknown source output '{source_output}'. Update the connection to use a valid output that exists on the source node '{source}'."),
                    json!({ "unknownConnectionSourceOutput": { "connection": name } }),
                ));
            }
            if both_ends && input.is_none() {
                out.push(flow_validation(
                    "Error",
                    "UnknownConnectionTargetInput",
                    format!("{between} references an unknown target input '{target_input}'. Update the connection to use a valid input that exists on the target node '{target}'."),
                    json!({ "unknownConnectionTargetInput": { "connection": name } }),
                ));
            }
            if let (Some(from), Some((to, expression))) = (output_type, input) {
                if !from.is_empty() && !to.is_empty() && !compatible_types(from, to, expression) {
                    out.push(flow_validation(
                        "Error",
                        "IncompatibleConnectionDataType",
                        format!("{between} has incompatible data types: type '{from} -> {to}'. Make sure that the input and output data types for this connection match each other."),
                        json!({ "incompatibleConnectionDataType": { "connection": name } }),
                    ));
                }
            }
            format!("data:{source_output}:{target_input}")
        } else {
            format!(
                "conditional:{}",
                s(&c["configuration"]["conditional"]["condition"])
            )
        };
        if !seen_transfers.insert((source, target, transfer)) {
            out.push(flow_validation(
                "Error",
                "DuplicateConnections",
                format!("The flow has duplicate connections between node '{source}' and node '{target}' that perform the same data transfer. Remove the redundant connections."),
                json!({ "duplicateConnections": { "source": source, "target": target } }),
            ));
        }
    }

    // A depth-first walk from the starting nodes (then from any node they
    // don't reach), following connections in declaration order. A connection
    // back to a node still on the walk closes a cycle; the cycle is reported
    // by its first connection, the one leaving that node along the walk.
    // The walk keeps its own stack: a definition can chain far more nodes
    // than a request thread's stack has frames for.
    let mut cyclic: Vec<(&str, &str, &str)> = Vec::new();
    {
        let no_edges: Vec<(&str, &str)> = Vec::new();
        let edges_of = |node: &str| edges.get(node).unwrap_or(&no_edges);
        // Nodes the walk has finished with.
        let mut done: BTreeSet<&str> = BTreeSet::new();
        // Nodes on the walk -> their position in `stack`.
        let mut on_walk: BTreeMap<&str, usize> = BTreeMap::new();
        // (node, connections of it taken so far). For every frame but the
        // top, the last one taken is the connection the walk followed out.
        let mut stack: Vec<(&str, usize)> = Vec::new();
        for &root in starts.iter().chain(order.iter()) {
            if done.contains(root) || on_walk.contains_key(root) {
                continue;
            }
            on_walk.insert(root, 0);
            stack.push((root, 0));
            while let Some(&(node, taken)) = stack.last() {
                let Some(&(target, connection)) = edges_of(node).get(taken) else {
                    on_walk.remove(node);
                    done.insert(node);
                    stack.pop();
                    continue;
                };
                let top = stack.len() - 1;
                stack[top].1 += 1;
                if let Some(&i) = on_walk.get(target) {
                    let (from, from_taken) = stack[i];
                    let via = if i == top {
                        connection
                    } else {
                        edges_of(from)[from_taken - 1].1
                    };
                    let to = stack.get(i + 1).map_or(target, |(n, _)| *n);
                    if !cyclic.iter().any(|(_, _, c)| *c == via) {
                        cyclic.push((from, to, via));
                    }
                } else if !done.contains(target) {
                    on_walk.insert(target, stack.len());
                    stack.push((target, 0));
                }
            }
        }
    }
    for (source, target, connection) in cyclic {
        out.push(flow_validation(
            "Error",
            "CyclicConnection",
            format!("The connection between the nodes '{source}' and '{target}' creates a circular loop in the flow. Restructure your flow to avoid circular loops between nodes."),
            json!({ "cyclicConnection": { "connection": connection } }),
        ));
    }

    // A node two unnested iterators both repeat has no single iteration to run
    // in. The model has no dedicated validation type for it, so it is reported
    // as `Unspecified`, as unaddressable nodes are.
    let graph = crate::flow_graph::Graph::new(nodes, connections);
    for (node, first, second) in crate::flow_graph::ambiguous_iterations(&graph) {
        out.push(flow_validation(
            "Error",
            "Unspecified",
            crate::flow_graph::ambiguous_iteration(node, first, second),
            json!({ "unspecified": {} }),
        ));
    }

    for node in order.iter().filter(|n| !reached.contains(*n)) {
        out.push(flow_validation(
            "Warning",
            "UnreachableNode",
            format!("The node '{node}' isn't connected to any other nodes. Attach this node to the flow or delete it."),
            json!({ "unreachableNode": { "node": node } }),
        ));
    }
    out
}

impl BedrockAgentService {
    pub(super) fn create_flow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "name")?;
        if let Some(definition) = body.get("definition") {
            check_stored_definition(definition, &req.account_id)?;
        }
        let id = short_id();
        let now_dt = now();
        // executionRoleArn is required by the Smithy model; synthesize a
        // plausible value when the caller omits one so the response still
        // satisfies the required shape.
        let role_arn = opt_str(&body, "executionRoleArn").unwrap_or_else(|| {
            crate::arns::default_flow_execution_role_arn(&req.region, &req.account_id, &id)
        });
        let flow = Flow {
            flow_id: id.clone(),
            name,
            description: opt_str(&body, "description"),
            execution_role_arn: Some(role_arn),
            status: "NotPrepared".to_string(),
            created_at: now_dt,
            updated_at: now_dt,
            version: "DRAFT".to_string(),
            definition: definition_of(&body),
            arn: flow_arn(&req.region, &req.account_id, &id),
            customer_encryption_key_arn: opt_str(&body, "customerEncryptionKeyArn"),
            latest_version: 0,
            validations: None,
        };
        let out = flow_json(&flow);
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        state.flows.insert(id, flow);
        Ok(AwsResponse::json_value(StatusCode::CREATED, out))
    }

    pub(super) fn get_flow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = flow_id_of(req, &req_str(&body, "flowIdentifier")?)?;
        let accts = self.state.read();
        let f = accts
            .get(&req.account_id)
            .and_then(|s| s.flows.get(&id))
            .ok_or_else(|| flow_not_found(&id))?;
        Ok(AwsResponse::ok_json(flow_json(f)))
    }

    pub(super) fn list_flows(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accts = self.state.read();
        let list: Vec<Value> = accts
            .get(&req.account_id)
            .map(|s| s.flows.values().map(flow_summary_json).collect())
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({ "flowSummaries": list })))
    }

    pub(super) fn update_flow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = flow_id_of(req, &req_str(&body, "flowIdentifier")?)?;
        if let Some(definition) = body.get("definition") {
            check_stored_definition(definition, &req.account_id)?;
        }
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let f = state
            .flows
            .get_mut(&id)
            .ok_or_else(|| flow_not_found(&id))?;
        f.updated_at = now();
        if let Some(n) = opt_str(&body, "name") {
            f.name = n;
        }
        if let Some(d) = opt_str(&body, "description") {
            f.description = Some(d);
        }
        if let Some(r) = opt_str(&body, "executionRoleArn") {
            f.execution_role_arn = Some(r);
        }
        if let Some(k) = opt_str(&body, "customerEncryptionKeyArn") {
            f.customer_encryption_key_arn = Some(k);
        }
        if body.get("definition").is_some() {
            f.definition = definition_of(&body);
        }
        // An updated draft has to be prepared again before it can run, and
        // the last preparation's findings no longer describe it.
        f.status = "NotPrepared".to_string();
        f.validations = None;
        Ok(AwsResponse::ok_json(flow_json(f)))
    }

    pub(super) fn delete_flow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = flow_id_of(req, &req_str(&body, "flowIdentifier")?)?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        state.flows.remove(&id).ok_or_else(|| flow_not_found(&id))?;
        state.flow_versions.remove(&id);
        state.flow_aliases.retain(|_, a| a.flow_id != id);
        Ok(AwsResponse::ok_json(json!({ "id": id })))
    }

    pub(super) fn prepare_flow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let id = flow_id_of(req, &req_str(&body, "flowIdentifier")?)?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let f = state
            .flows
            .get_mut(&id)
            .ok_or_else(|| flow_not_found(&id))?;
        // A null definition (stored before nulls were dropped) is no
        // definition either.
        let definition = f.definition.as_ref().filter(|d| !d.is_null()).ok_or_else(|| {
            validation(format!(
                "You can\u{2019}t prepare flow {id} because its definition is empty. Update your flow with a definition and retry your request."
            ))
        })?;
        // A definition stored before node names had to be unique can't be
        // validated node by node; it is refused as CreateFlow refuses it.
        check_definition(definition)?;
        // Preparing validates the definition: any error fails the flow, and
        // GetFlow reports every finding until the draft changes again.
        let validations = validate_definition(definition);
        let failed = validations.iter().any(|v| v["severity"] == "Error");
        f.status = if failed { "Failed" } else { "Prepared" }.to_string();
        f.validations = Some(validations);
        // PrepareFlow answers while the preparation is under way; it has
        // settled by the time anyone reads the flow back.
        Ok(AwsResponse::json_value(
            StatusCode::ACCEPTED,
            json!({
                "id": id,
                "status": "Preparing",
            }),
        ))
    }

    pub(super) fn create_flow_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = flow_id_of(req, &req_str(&body, "flowIdentifier")?)?;
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        // `flows` and `flow_versions` are disjoint fields, so the flow can be
        // borrowed while its version list is extended.
        let flow = state
            .flows
            .get_mut(&flow_id)
            .ok_or_else(|| flow_not_found(&flow_id))?;
        // Only a prepared draft can be snapshotted.
        if flow.status != "Prepared" {
            return Err(conflict(format!(
                "Couldn't perform CreateFlowVersion operation. The resource that you specified is currently in status {}. Retry your request after the resource changes to a valid status.",
                flow.status
            )));
        }
        let versions = state.flow_versions.entry(flow_id.clone()).or_default();
        let version_num = next_version(
            &mut flow.latest_version,
            versions.iter().map(|v| v.flow_version.as_str()),
        );
        let fv = FlowVersion {
            flow_version: version_num,
            flow_id,
            description: opt_str(&body, "description"),
            created_at: now_dt,
            updated_at: now_dt,
            definition: flow.definition.clone(),
            name: Some(flow.name.clone()),
            execution_role_arn: flow.execution_role_arn.clone(),
            customer_encryption_key_arn: flow.customer_encryption_key_arn.clone(),
            status: Some(flow.status.clone()),
        };
        let out = flow_version_json(flow, &fv);
        versions.push(fv);
        Ok(AwsResponse::json_value(StatusCode::CREATED, out))
    }

    pub(super) fn get_flow_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = flow_id_of(req, &req_str(&body, "flowIdentifier")?)?;
        let version = req_str(&body, "flowVersion")?;
        let accts = self.state.read();
        let version_not_found = || not_found(format!("Flow version {version} not found"));
        let state = accts.get(&req.account_id).ok_or_else(version_not_found)?;
        let flow = state.flows.get(&flow_id).ok_or_else(version_not_found)?;
        let v = state
            .flow_versions
            .get(&flow_id)
            .and_then(|vec| vec.iter().find(|v| v.flow_version == version))
            .ok_or_else(version_not_found)?;
        Ok(AwsResponse::ok_json(flow_version_json(flow, v)))
    }

    pub(super) fn list_flow_versions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = flow_id_of(req, &req_str(&body, "flowIdentifier")?)?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| flow_not_found(&flow_id))?;
        let flow = state
            .flows
            .get(&flow_id)
            .ok_or_else(|| flow_not_found(&flow_id))?;
        // The working draft is listed first, as version `DRAFT`.
        let draft = json!({
            "id": flow.flow_id,
            "arn": flow.arn,
            "status": flow.status,
            "createdAt": flow.created_at.to_rfc3339(),
            "version": "DRAFT",
        });
        let list: Vec<Value> = std::iter::once(draft)
            .chain(
                state
                    .flow_versions
                    .get(&flow_id)
                    .into_iter()
                    .flat_map(|vec| {
                        vec.iter().map(|v| {
                            json!({
                                "id": flow.flow_id,
                                "arn": flow.arn,
                                "status": v.status.as_deref().unwrap_or(&flow.status),
                                "createdAt": v.created_at.to_rfc3339(),
                                "version": v.flow_version,
                            })
                        })
                    }),
            )
            .collect();
        Ok(AwsResponse::ok_json(
            json!({ "flowVersionSummaries": list }),
        ))
    }

    pub(super) fn delete_flow_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = flow_id_of(req, &req_str(&body, "flowIdentifier")?)?;
        let version = req_str(&body, "flowVersion")?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let vec = state
            .flow_versions
            .get_mut(&flow_id)
            .ok_or_else(|| not_found(format!("Flow version {version} not found")))?;
        let pos = vec
            .iter()
            .position(|v| v.flow_version == version)
            .ok_or_else(|| not_found(format!("Flow version {version} not found")))?;
        vec.remove(pos);
        Ok(AwsResponse::ok_json(json!({
            "id": flow_id,
            "version": version,
        })))
    }

    pub(super) fn validate_flow_definition(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let definition = body
            .get("definition")
            .filter(|d| d.is_object())
            .ok_or_else(|| missing("definition"))?;
        check_definition(definition)?;
        Ok(AwsResponse::ok_json(json!({
            "validations": validate_definition(definition),
        })))
    }

    pub(super) fn create_flow_alias(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = flow_id_of(req, &req_str(&body, "flowIdentifier")?)?;
        let name = req_str(&body, "name")?;
        check_alias_routing(&body)?;
        let alias_id = short_id();
        let now_dt = now();
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let flow_arn = state
            .flows
            .get(&flow_id)
            .ok_or_else(|| flow_not_found(&flow_id))?
            .arn
            .clone();
        let alias = FlowAlias {
            alias_id: alias_id.clone(),
            alias_name: name,
            flow_id,
            routing_configuration: opt_array(&body, "routingConfiguration"),
            description: opt_str(&body, "description"),
            created_at: now_dt,
            updated_at: now_dt,
            concurrency_configuration: opt_json(&body, "concurrencyConfiguration"),
        };
        let out = flow_alias_json(&flow_arn, &alias);
        state.flow_aliases.insert(alias_id, alias);
        Ok(AwsResponse::json_value(StatusCode::CREATED, out))
    }

    pub(super) fn get_flow_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = flow_id_of(req, &req_str(&body, "flowIdentifier")?)?;
        let alias_id = alias_id_of(req, &flow_id, &req_str(&body, "aliasIdentifier")?)?;
        let accts = self.state.read();
        let alias_not_found = || not_found(format!("Flow alias {alias_id} not found"));
        let state = accts.get(&req.account_id).ok_or_else(alias_not_found)?;
        let flow = state.flows.get(&flow_id).ok_or_else(alias_not_found)?;
        let a = state
            .flow_aliases
            .get(&alias_id)
            .filter(|a| a.flow_id == flow_id)
            .ok_or_else(alias_not_found)?;
        Ok(AwsResponse::ok_json(flow_alias_json(&flow.arn, a)))
    }

    pub(super) fn list_flow_aliases(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = flow_id_of(req, &req_str(&body, "flowIdentifier")?)?;
        let accts = self.state.read();
        let state = accts
            .get(&req.account_id)
            .ok_or_else(|| flow_not_found(&flow_id))?;
        let flow = state
            .flows
            .get(&flow_id)
            .ok_or_else(|| flow_not_found(&flow_id))?;
        let list: Vec<Value> = state
            .flow_aliases
            .values()
            .filter(|a| a.flow_id == flow_id)
            .map(|a| flow_alias_json(&flow.arn, a))
            .collect();
        Ok(AwsResponse::ok_json(json!({ "flowAliasSummaries": list })))
    }

    pub(super) fn update_flow_alias(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = flow_id_of(req, &req_str(&body, "flowIdentifier")?)?;
        let alias_id = alias_id_of(req, &flow_id, &req_str(&body, "aliasIdentifier")?)?;
        check_alias_routing(&body)?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        let alias_not_found = || not_found(format!("Flow alias {alias_id} not found"));
        let flow_arn = state
            .flows
            .get(&flow_id)
            .ok_or_else(alias_not_found)?
            .arn
            .clone();
        let a = state
            .flow_aliases
            .get_mut(&alias_id)
            .filter(|a| a.flow_id == flow_id)
            .ok_or_else(alias_not_found)?;
        a.updated_at = now();
        if let Some(n) = opt_str(&body, "name") {
            a.alias_name = n;
        }
        if let Some(d) = opt_str(&body, "description") {
            a.description = Some(d);
        }
        if body.get("routingConfiguration").is_some() {
            a.routing_configuration = opt_array(&body, "routingConfiguration");
        }
        if body.get("concurrencyConfiguration").is_some() {
            a.concurrency_configuration = opt_json(&body, "concurrencyConfiguration");
        }
        Ok(AwsResponse::ok_json(flow_alias_json(&flow_arn, a)))
    }

    pub(super) fn delete_flow_alias(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let flow_id = flow_id_of(req, &req_str(&body, "flowIdentifier")?)?;
        let alias_id = alias_id_of(req, &flow_id, &req_str(&body, "aliasIdentifier")?)?;
        let mut accts = self.state.write();
        let state = accts.get_or_create(&req.account_id, &req.region);
        match state.flow_aliases.get(&alias_id) {
            Some(a) if a.flow_id == flow_id => {
                state.flow_aliases.remove(&alias_id);
            }
            _ => return Err(not_found(format!("Flow alias {alias_id} not found"))),
        }
        Ok(AwsResponse::ok_json(json!({
            "flowId": flow_id,
            "id": alias_id,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Definitions sent to the real `ValidateFlowDefinition`, with what it
    /// answered (`validations`, or the `error` refusing the definition).
    const RECORDED: &str = include_str!("testdata/flow_validations.json");

    #[test]
    fn validation_matches_the_recorded_findings() {
        let cases: Value = serde_json::from_str(RECORDED).unwrap();
        assert!(cases.as_object().unwrap().len() > 30);
        for (name, case) in cases.as_object().unwrap() {
            let definition = &case["definition"];
            if let Some(error) = case.get("error") {
                let err = check_definition(definition).expect_err(name);
                assert_eq!(err.code(), error["code"], "{name}");
                assert_eq!(err.message(), error["message"], "{name}");
                continue;
            }
            check_definition(definition).unwrap_or_else(|e| panic!("{name}: {e:?}"));
            // Which conditional paths can never run is not modelled.
            let expected: Vec<Value> = case["validations"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|v| v["type"] != "UnsatisfiedConnectionConditions")
                .cloned()
                .collect();
            assert_eq!(validate_definition(definition), expected, "{name}");
        }
    }

    #[test]
    fn a_stored_definition_has_at_most_one_input_node() {
        let input = |name: &str| {
            json!({"name": name, "type": "Input",
                "outputs": [{"name": "document", "type": "String"}]})
        };
        let one = json!({"nodes": [input("FlowInputNode")]});
        check_stored_definition(&one, "123456789012").unwrap();
        let two = json!({"nodes": [input("FlowInputNode"), input("Other")]});
        let err = check_stored_definition(&two, "123456789012").unwrap_err();
        assert_eq!(err.code(), "ValidationException");
        assert_eq!(
            err.message(),
            "The limit for max-number-flow-input-nodes is 1, but the requested value is 2 for account 123456789012. Retry your request with a value less than or equal to 1."
        );
        // ValidateFlowDefinition only checks the definition's shape.
        check_definition(&two).unwrap();
    }

    #[test]
    fn a_very_long_chain_validates_without_exhausting_the_stack() {
        // 50,000 nodes in a line, then a connection from the last back to
        // the first: the cycle walk is as deep as the chain. Run on a thread
        // with the 2 MiB stack a tokio worker has.
        const LEN: usize = 50_000;
        let link = |name: String, source: &str, target: &str| {
            json!({"name": name, "type": "Data", "source": source, "target": target,
                "configuration": {"data": {"sourceOutput": "out", "targetInput": "in"}}})
        };
        let mut nodes = vec![json!({"name": "FlowInputNode", "type": "Input",
            "outputs": [{"name": "document", "type": "String"}]})];
        let mut connections = vec![json!({"name": "Start", "type": "Data",
            "source": "FlowInputNode", "target": "Step0",
            "configuration": {"data": {"sourceOutput": "document", "targetInput": "in"}}})];
        for i in 0..LEN {
            nodes.push(json!({"name": format!("Step{i}"), "type": "Prompt",
                "inputs": [{"name": "in", "type": "String", "expression": "$.data"},
                           {"name": "back", "type": "String", "expression": "$.data"}],
                "outputs": [{"name": "out", "type": "String"}]}));
            if i + 1 < LEN {
                connections.push(link(
                    format!("Link{i}"),
                    &format!("Step{i}"),
                    &format!("Step{}", i + 1),
                ));
            }
        }
        let last = format!("Step{}", LEN - 1);
        connections.push(
            json!({"name": "Back", "type": "Data", "source": last, "target": "Step0",
            "configuration": {"data": {"sourceOutput": "out", "targetInput": "back"}}}),
        );
        let definition = json!({"nodes": nodes, "connections": connections});

        let validations = std::thread::Builder::new()
            .stack_size(2 * 1024 * 1024)
            .spawn(move || validate_definition(&definition))
            .unwrap()
            .join()
            .expect("validation overflowed its stack");
        let cyclic: Vec<&Value> = validations
            .iter()
            .filter(|v| v["type"] == "CyclicConnection")
            .collect();
        assert_eq!(cyclic.len(), 1, "{cyclic:?}");
        // Reported by the cycle's first connection along the walk.
        assert_eq!(
            cyclic[0]["details"],
            json!({"cyclicConnection": {"connection": "Link0"}})
        );
        // Every input after the first step's `back` is unfed, nothing else
        // but the missing Output node is wrong, and every step is reached.
        assert!(validations.iter().all(|v| {
            v["type"] == "CyclicConnection"
                || v["type"] == "MissingEndingNodes"
                || v["type"] == "UnfulfilledNodeInput"
        }));
        assert!(!validations.iter().any(|v| v["type"] == "UnreachableNode"));
    }

    #[test]
    fn a_self_connection_is_its_own_cycle() {
        let definition = json!({
            "nodes": [
                {"name": "FlowInputNode", "type": "Input",
                    "outputs": [{"name": "document", "type": "String"}]},
                {"name": "Loop", "type": "Condition",
                    "inputs": [{"name": "xval", "type": "String", "expression": "$.data"}]}
            ],
            "connections": [
                {"name": "Feed", "type": "Data", "source": "FlowInputNode", "target": "Loop",
                    "configuration": {"data": {"sourceOutput": "document", "targetInput": "xval"}}},
                {"name": "Again", "type": "Conditional", "source": "Loop", "target": "Loop",
                    "configuration": {"conditional": {"condition": "default"}}}
            ]
        });
        let cyclic: Vec<Value> = validate_definition(&definition)
            .into_iter()
            .filter(|v| v["type"] == "CyclicConnection")
            .collect();
        assert_eq!(cyclic.len(), 1, "{cyclic:?}");
        assert_eq!(
            cyclic[0]["details"],
            json!({"cyclicConnection": {"connection": "Again"}})
        );
        assert_eq!(
            cyclic[0]["message"],
            "The connection between the nodes 'Loop' and 'Loop' creates a circular loop in the flow. Restructure your flow to avoid circular loops between nodes."
        );
    }
}
