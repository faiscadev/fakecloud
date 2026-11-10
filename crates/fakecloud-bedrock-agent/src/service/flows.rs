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

/// Structural validation of a flow definition, the checks `ValidateFlowDefinition`
/// reports: the flow needs a starting (`Input`) and an ending (`Output`) node;
/// every connection must join existing nodes (and, for data connections, an
/// existing output of the source to an existing input of the target); no two
/// connections may join the same ends; node names must be present and unique;
/// no node input may be fed by
/// more than one data connection or by none; connections may not form a cycle;
/// and every node should be reachable from a starting node.
fn validate_definition(definition: &Value) -> Vec<Value> {
    use std::collections::{BTreeMap, BTreeSet};

    let empty = Vec::new();
    let nodes = definition["nodes"].as_array().unwrap_or(&empty);
    let connections = definition["connections"].as_array().unwrap_or(&empty);
    let str_of = |v: &Value, k: &str| v[k].as_str().unwrap_or_default().to_string();
    let names_of = |v: &Value, k: &str| -> BTreeSet<String> {
        v[k].as_array()
            .map(|a| a.iter().map(|x| str_of(x, "name")).collect())
            .unwrap_or_default()
    };

    // node name -> (type, input names, output names). A node with no name,
    // or a name another node already took, can't be addressed by a
    // connection; it is reported (the model has no dedicated validation type
    // for either, so as `Unspecified`) rather than silently merged away.
    let mut out = Vec::new();
    let mut by_name: BTreeMap<String, (String, BTreeSet<String>, BTreeSet<String>)> =
        BTreeMap::new();
    for (index, n) in nodes.iter().enumerate() {
        let name = str_of(n, "name");
        if name.is_empty() {
            out.push(flow_validation(
                "Error",
                "Unspecified",
                format!("Node at index {index} has no name."),
                json!({ "unspecified": {} }),
            ));
            continue;
        }
        if by_name.contains_key(&name) {
            out.push(flow_validation(
                "Error",
                "Unspecified",
                format!("Node name {name} is used by more than one node."),
                json!({ "unspecified": {} }),
            ));
            continue;
        }
        by_name.insert(
            name,
            (
                str_of(n, "type"),
                names_of(n, "inputs"),
                names_of(n, "outputs"),
            ),
        );
    }

    if !by_name.values().any(|(t, _, _)| t == "Input") {
        out.push(flow_validation(
            "Error",
            "MissingStartingNodes",
            "The flow has no starting node. Add an Input node.".to_string(),
            json!({ "missingStartingNodes": {} }),
        ));
    }
    if !by_name.values().any(|(t, _, _)| t == "Output") {
        out.push(flow_validation(
            "Error",
            "MissingEndingNodes",
            "The flow has no ending node. Add an Output node.".to_string(),
            json!({ "missingEndingNodes": {} }),
        ));
    }

    let mut seen_pairs = BTreeSet::new();
    let mut input_feeds: BTreeMap<(String, String), usize> = BTreeMap::new();
    let mut edges: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    for c in connections {
        let name = str_of(c, "name");
        let source = str_of(c, "source");
        let target = str_of(c, "target");
        let source_node = by_name.get(&source);
        let target_node = by_name.get(&target);
        if source_node.is_none() {
            out.push(flow_validation(
                "Error",
                "UnknownConnectionSource",
                format!("Connection {name} has an unknown source node {source}."),
                json!({ "unknownConnectionSource": { "connection": name } }),
            ));
        }
        if target_node.is_none() {
            out.push(flow_validation(
                "Error",
                "UnknownConnectionTarget",
                format!("Connection {name} has an unknown target node {target}."),
                json!({ "unknownConnectionTarget": { "connection": name } }),
            ));
        }
        // Two connections duplicate each other when they join the same ends:
        // the same output to the same input for a data connection, the same
        // condition for a conditional one. One node feeding two different
        // inputs of another is two distinct connections.
        let ends = match (
            c["configuration"].get("data"),
            c["configuration"].get("conditional"),
        ) {
            (Some(data), _) => format!(
                "data:{}:{}",
                str_of(data, "sourceOutput"),
                str_of(data, "targetInput")
            ),
            (None, Some(cond)) => format!("conditional:{}", str_of(cond, "condition")),
            (None, None) => String::new(),
        };
        if !seen_pairs.insert((source.clone(), target.clone(), ends)) {
            out.push(flow_validation(
                "Error",
                "DuplicateConnections",
                format!("Nodes {source} and {target} are joined by more than one connection."),
                json!({ "duplicateConnections": { "source": source, "target": target } }),
            ));
        }
        if let Some(data) = c["configuration"].get("data") {
            let source_output = str_of(data, "sourceOutput");
            let target_input = str_of(data, "targetInput");
            if source_node.is_some_and(|(_, _, outputs)| !outputs.contains(&source_output)) {
                out.push(flow_validation(
                    "Error",
                    "UnknownConnectionSourceOutput",
                    format!(
                        "Connection {name} references unknown output {source_output} of node {source}."
                    ),
                    json!({ "unknownConnectionSourceOutput": { "connection": name } }),
                ));
            }
            if target_node.is_some_and(|(_, inputs, _)| !inputs.contains(&target_input)) {
                out.push(flow_validation(
                    "Error",
                    "UnknownConnectionTargetInput",
                    format!(
                        "Connection {name} references unknown input {target_input} of node {target}."
                    ),
                    json!({ "unknownConnectionTargetInput": { "connection": name } }),
                ));
            } else if target_node.is_some() {
                *input_feeds
                    .entry((target.clone(), target_input))
                    .or_default() += 1;
            }
        }
        if source_node.is_some() && target_node.is_some() {
            edges.entry(source).or_default().push((target, name));
        }
    }

    for (node, (_, inputs, _)) in &by_name {
        for input in inputs {
            match input_feeds.get(&(node.clone(), input.clone())).copied() {
                None | Some(0) => out.push(flow_validation(
                    "Error",
                    "UnfulfilledNodeInput",
                    format!("Input {input} of node {node} is not connected."),
                    json!({ "unfulfilledNodeInput": { "node": node, "input": input } }),
                )),
                Some(1) => {}
                Some(_) => out.push(flow_validation(
                    "Error",
                    "MultipleNodeInputConnections",
                    format!("Input {input} of node {node} has more than one connection."),
                    json!({ "multipleNodeInputConnections": { "node": node, "input": input } }),
                )),
            }
        }
    }

    // A connection that leads back to a node still on the DFS stack closes a
    // cycle.
    fn visit<'a>(
        node: &'a str,
        edges: &'a BTreeMap<String, Vec<(String, String)>>,
        state: &mut BTreeMap<&'a str, bool>,
        cyclic: &mut Vec<String>,
    ) {
        state.insert(node, true);
        for (target, connection) in edges.get(node).into_iter().flatten() {
            match state.get(target.as_str()) {
                Some(true) => cyclic.push(connection.clone()),
                Some(false) => {}
                None => visit(target, edges, state, cyclic),
            }
        }
        state.insert(node, false);
    }
    let mut dfs_state = BTreeMap::new();
    let mut cyclic = Vec::new();
    for node in by_name.keys() {
        if !dfs_state.contains_key(node.as_str()) {
            visit(node, &edges, &mut dfs_state, &mut cyclic);
        }
    }
    for connection in cyclic {
        out.push(flow_validation(
            "Error",
            "CyclicConnection",
            format!("Connection {connection} creates a cycle."),
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

    let mut reached: BTreeSet<&str> = by_name
        .iter()
        .filter(|(_, (t, _, _))| t == "Input")
        .map(|(n, _)| n.as_str())
        .collect();
    let mut frontier: Vec<&str> = reached.iter().copied().collect();
    while let Some(node) = frontier.pop() {
        for (target, _) in edges.get(node).into_iter().flatten() {
            if reached.insert(target.as_str()) {
                frontier.push(target.as_str());
            }
        }
    }
    for node in by_name.keys().filter(|n| !reached.contains(n.as_str())) {
        out.push(flow_validation(
            "Warning",
            "UnreachableNode",
            format!("Node {node} cannot be reached from a starting node."),
            json!({ "unreachableNode": { "node": node } }),
        ));
    }
    out
}

impl BedrockAgentService {
    pub(super) fn create_flow(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = req_str(&body, "name")?;
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
            definition: opt_json(&body, "definition"),
            arn: flow_arn(&req.region, &req.account_id, &id),
            customer_encryption_key_arn: opt_str(&body, "customerEncryptionKeyArn"),
            latest_version: 0,
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
            f.definition = opt_json(&body, "definition");
        }
        // An updated draft has to be prepared again before it can run.
        f.status = "NotPrepared".to_string();
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
        f.status = "Prepared".to_string();
        f.updated_at = now();
        Ok(AwsResponse::json_value(
            StatusCode::ACCEPTED,
            json!({
                "id": id,
                "status": f.status,
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
        let list: Vec<Value> = state
            .flow_versions
            .get(&flow_id)
            .map(|vec| {
                vec.iter()
                    .map(|v| {
                        json!({
                            "id": flow.flow_id,
                            "arn": flow.arn,
                            "status": v.status.as_deref().unwrap_or(&flow.status),
                            "createdAt": v.created_at.to_rfc3339(),
                            "version": v.flow_version,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
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
