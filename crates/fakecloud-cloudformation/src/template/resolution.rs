//! `resolution` concerns from template.rs (audit-2026-05-19).

use super::*;

/// Re-resolve a single resource definition's properties with updated physical IDs.
pub fn resolve_resource_properties(
    resource: &ResourceDefinition,
    template_body: &str,
    parameters: &BTreeMap<String, String>,
    resource_physical_ids: &BTreeMap<String, String>,
) -> Result<ResourceDefinition, String> {
    resolve_resource_properties_with_attrs(
        resource,
        template_body,
        parameters,
        resource_physical_ids,
        &BTreeMap::new(),
        &BTreeMap::new(),
    )
}

/// Re-resolve a single resource definition's properties with updated physical
/// IDs and attribute values for `Fn::GetAtt`.
pub fn resolve_resource_properties_with_attrs(
    resource: &ResourceDefinition,
    template_body: &str,
    parameters: &BTreeMap<String, String>,
    resource_physical_ids: &BTreeMap<String, String>,
    resource_attributes: &BTreeMap<String, BTreeMap<String, String>>,
    imports: &BTreeMap<String, String>,
) -> Result<ResourceDefinition, String> {
    let value: Value = if template_body.trim_start().starts_with('{') {
        serde_json::from_str(template_body).map_err(|e| format!("Invalid JSON template: {e}"))?
    } else {
        serde_yaml::from_str(template_body).map_err(|e| format!("Invalid YAML template: {e}"))?
    };
    // Re-expand ForEach so the resource we look up matches the post-
    // expansion logical IDs from the original parse.
    let value = expand_for_each(&value, &BTreeMap::new(), parameters)?;
    // Re-apply the SAM transform too: the original parse rewrote SAM
    // resources (e.g. AWS::Serverless::StateMachine -> the native
    // AWS::StepFunctions::StateMachine, Role -> RoleArn). Without this the
    // properties get re-read from the raw SAM resource and the native
    // provisioner sees SAM property names it doesn't understand.
    let value = expand_sam(&value);

    let resources_obj = value
        .get("Resources")
        .and_then(|v| v.as_object())
        .ok_or("Template must contain a Resources section")?;

    let raw_props = resources_obj
        .get(&resource.logical_id)
        .and_then(|r| r.get("Properties"))
        .cloned()
        .unwrap_or(Value::Object(serde_json::Map::new()));

    // Re-evaluate Conditions / Mappings on every resolve so Fn::If picks
    // the right branch and AWS::NoValue still strips at incremental
    // provisioning time. Without this, the sentinel would leak into the
    // provisioned property map.
    let conditions = evaluate_conditions(&value, parameters)?;
    let mappings = parse_mappings(&value);
    let raw_props = apply_mappings(&raw_props, parameters, &mappings, &conditions)?;

    let resolved = resolve_refs_full(
        &raw_props,
        parameters,
        resources_obj,
        resource_physical_ids,
        resource_attributes,
        imports,
        &conditions,
    );
    let resolved = strip_no_value(resolved);

    Ok(ResourceDefinition {
        logical_id: resource.logical_id.clone(),
        resource_type: resource.resource_type.clone(),
        properties: resolved,
        deletion_policy: resource.deletion_policy.clone(),
        update_replace_policy: resource.update_replace_policy.clone(),
    })
}

/// Compute a provisioning order for `resource_defs` such that every resource is
/// provisioned *after* the resources it references — via an explicit
/// `DependsOn` or an implicit `Ref` / `Fn::GetAtt` / `Fn::Sub` to another
/// resource's logical id.
///
/// Returns a permutation of indices into `resource_defs`. CloudFormation
/// provisions in dependency order; FakeCloud previously provisioned in template
/// order, so a `Ref` to a not-yet-created resource resolved to the bare logical
/// id (the "not yet provisioned" fallback in `resolve_refs_full`) and got baked
/// into derived state — e.g. a Step Functions ASL whose `DefinitionSubstitutions`
/// reference a Lambda declared later in the template, leaving the logical id in
/// the definition and failing every invoke with `Lambda.ResourceNotFoundException`.
///
/// The graph is built from the post-`ForEach`/post-SAM template so the logical
/// ids and reference shapes match what provisioning actually sees. Falls back to
/// the original order on a parse failure or a dependency cycle (which real CFN
/// rejects outright), and breaks ties by original index so the order is
/// deterministic.
pub fn dependency_order(
    template_body: &str,
    parameters: &BTreeMap<String, String>,
    resource_defs: &[ResourceDefinition],
) -> Vec<usize> {
    let n = resource_defs.len();
    let identity = || (0..n).collect::<Vec<usize>>();
    if n < 2 {
        return identity();
    }

    let parse = |body: &str| -> Result<Value, ()> {
        if body.trim_start().starts_with('{') {
            serde_json::from_str(body).map_err(|_| ())
        } else {
            serde_yaml::from_str(body).map_err(|_| ())
        }
    };
    let Ok(value) = parse(template_body) else {
        return identity();
    };
    // Match the logical ids the rest of provisioning sees (ForEach + SAM).
    let Ok(value) = expand_for_each(&value, &BTreeMap::new(), parameters) else {
        return identity();
    };
    let value = expand_sam(&value);
    let Some(resources_obj) = value.get("Resources").and_then(|v| v.as_object()) else {
        return identity();
    };

    let index_of: BTreeMap<&str, usize> = resource_defs
        .iter()
        .enumerate()
        .map(|(i, r)| (r.logical_id.as_str(), i))
        .collect();
    let known: BTreeSet<&str> = index_of.keys().copied().collect();

    // deps[i] = indices that resource i must be provisioned after.
    let mut deps: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); n];
    for (i, r) in resource_defs.iter().enumerate() {
        let Some(raw) = resources_obj.get(&r.logical_id) else {
            continue;
        };
        let mut refs: BTreeSet<String> = BTreeSet::new();
        match raw.get("DependsOn") {
            Some(Value::String(s)) => {
                refs.insert(s.clone());
            }
            Some(Value::Array(a)) => {
                for x in a {
                    if let Some(s) = x.as_str() {
                        refs.insert(s.to_string());
                    }
                }
            }
            _ => {}
        }
        if let Some(props) = raw.get("Properties") {
            collect_resource_refs(props, &known, &mut refs);
        }
        for name in refs {
            if let Some(&j) = index_of.get(name.as_str()) {
                if j != i {
                    deps[i].insert(j);
                }
            }
        }
    }

    // Kahn's algorithm; ready set ordered by original index for determinism.
    let mut indeg: Vec<usize> = deps.iter().map(|d| d.len()).collect();
    let mut dependents: Vec<Vec<usize>> = vec![Vec::new(); n];
    for (i, d) in deps.iter().enumerate() {
        for &j in d {
            dependents[j].push(i);
        }
    }
    let mut ready: BTreeSet<usize> = (0..n).filter(|&i| indeg[i] == 0).collect();
    let mut order: Vec<usize> = Vec::with_capacity(n);
    while let Some(&i) = ready.iter().next() {
        ready.remove(&i);
        order.push(i);
        for &j in &dependents[i] {
            indeg[j] -= 1;
            if indeg[j] == 0 {
                ready.insert(j);
            }
        }
    }
    if order.len() != n {
        // Dependency cycle — append the unresolved remainder in original order.
        let placed: BTreeSet<usize> = order.iter().copied().collect();
        for i in 0..n {
            if !placed.contains(&i) {
                order.push(i);
            }
        }
    }
    order
}

/// Recursively collect the logical ids in `known` that `value` references via
/// `Ref`, `Fn::GetAtt`, or `Fn::Sub`.
fn collect_resource_refs(value: &Value, known: &BTreeSet<&str>, out: &mut BTreeSet<String>) {
    match value {
        Value::Object(map) => {
            if let Some(Value::String(name)) = map.get("Ref") {
                if known.contains(name.as_str()) {
                    out.insert(name.clone());
                }
            }
            if let Some(g) = map.get("Fn::GetAtt") {
                let logical = match g {
                    Value::Array(a) => a.first().and_then(|x| x.as_str()),
                    Value::String(s) => s.split('.').next(),
                    _ => None,
                };
                if let Some(l) = logical {
                    if known.contains(l) {
                        out.insert(l.to_string());
                    }
                }
            }
            if let Some(s) = map.get("Fn::Sub") {
                collect_sub_refs(s, known, out);
            }
            // Recurse into every value to catch nested references.
            for v in map.values() {
                collect_resource_refs(v, known, out);
            }
        }
        Value::Array(a) => {
            for v in a {
                collect_resource_refs(v, known, out);
            }
        }
        _ => {}
    }
}

/// Extract `${LogicalId}` / `${LogicalId.Attr}` references from an `Fn::Sub`
/// (string form or `[template, {vars}]` form), skipping the locally-defined
/// variables and `${!Literal}` escapes.
fn collect_sub_refs(sub: &Value, known: &BTreeSet<&str>, out: &mut BTreeSet<String>) {
    let (template, local_vars): (Option<&str>, BTreeSet<&str>) = match sub {
        Value::String(t) => (Some(t.as_str()), BTreeSet::new()),
        Value::Array(a) => {
            let t = a.first().and_then(|x| x.as_str());
            let vars = a
                .get(1)
                .and_then(|x| x.as_object())
                .map(|m| m.keys().map(|k| k.as_str()).collect())
                .unwrap_or_default();
            (t, vars)
        }
        _ => (None, BTreeSet::new()),
    };
    let Some(t) = template else {
        return;
    };
    let mut i = 0;
    while let Some(start) = t[i..].find("${") {
        let abs = i + start + 2;
        let Some(end_rel) = t[abs..].find('}') else {
            break;
        };
        let token = &t[abs..abs + end_rel];
        // `${!X}` is a literal `${X}`, not a reference.
        if !token.starts_with('!') {
            let logical = token.split('.').next().unwrap_or(token).trim();
            if !local_vars.contains(logical) && known.contains(logical) {
                out.insert(logical.to_string());
            }
        }
        i = abs + end_rel + 1;
    }
}

#[cfg(test)]
mod dependency_order_tests {
    use super::*;

    fn rd(logical: &str, resource_type: &str) -> ResourceDefinition {
        ResourceDefinition {
            logical_id: logical.to_string(),
            resource_type: resource_type.to_string(),
            properties: serde_json::json!({}),
            deletion_policy: None,
            update_replace_policy: None,
        }
    }

    // The Gap #3 scenario: a StateMachine declared (and sorting) before the
    // Lambda it references via a DefinitionSubstitutions `Ref` must provision
    // after that Lambda, so the substitution resolves to the function name.
    #[test]
    fn referenced_resource_is_ordered_first() {
        let template = r#"{
            "Resources": {
                "Machine": {
                    "Type": "AWS::StepFunctions::StateMachine",
                    "Properties": {
                        "DefinitionString": "${fn}",
                        "DefinitionSubstitutions": {"fn": {"Ref": "WorkerFn"}}
                    }
                },
                "WorkerFn": {"Type": "AWS::Lambda::Function", "Properties": {}}
            }
        }"#;
        // Template order (alphabetical, as the parser yields): Machine, WorkerFn.
        let defs = vec![
            rd("Machine", "AWS::StepFunctions::StateMachine"),
            rd("WorkerFn", "AWS::Lambda::Function"),
        ];
        let order = dependency_order(template, &BTreeMap::new(), &defs);
        assert_eq!(
            order,
            vec![1, 0],
            "WorkerFn must be provisioned before Machine"
        );
    }

    #[test]
    fn get_att_sub_and_depends_on_all_create_edges() {
        // A: referenced by B via GetAtt, by C via Fn::Sub, by D via DependsOn.
        let template = r#"{
            "Resources": {
                "B": {"Type": "X", "Properties": {"V": {"Fn::GetAtt": ["A", "Arn"]}}},
                "C": {"Type": "X", "Properties": {"V": {"Fn::Sub": "p-${A}-s"}}},
                "D": {"Type": "X", "DependsOn": "A", "Properties": {}},
                "A": {"Type": "X", "Properties": {}}
            }
        }"#;
        let defs = vec![rd("B", "X"), rd("C", "X"), rd("D", "X"), rd("A", "X")];
        let order = dependency_order(template, &BTreeMap::new(), &defs);
        let pos = |name: &str| {
            order
                .iter()
                .position(|&i| defs[i].logical_id == name)
                .unwrap()
        };
        assert!(pos("A") < pos("B"), "GetAtt edge");
        assert!(pos("A") < pos("C"), "Fn::Sub edge");
        assert!(pos("A") < pos("D"), "DependsOn edge");
    }

    #[test]
    fn import_value_in_property_resolves_from_imports() {
        // §1.5: Fn::ImportValue inside a resource property must resolve to the
        // real exported value, not the empty string the old code baked.
        let template = r#"{
            "Resources": {
                "Q": {"Type": "AWS::SQS::Queue", "Properties": {"QueueName": {"Fn::ImportValue": "SharedQueueName"}}}
            }
        }"#;
        let mut imports = BTreeMap::new();
        imports.insert("SharedQueueName".to_string(), "shared-q".to_string());
        let resolved = resolve_resource_properties_with_attrs(
            &rd("Q", "AWS::SQS::Queue"),
            template,
            &BTreeMap::new(),
            &BTreeMap::new(),
            &BTreeMap::new(),
            &imports,
        )
        .unwrap();
        assert_eq!(
            resolved.properties["QueueName"],
            serde_json::json!("shared-q")
        );
    }

    #[test]
    fn independent_resources_keep_original_order() {
        let template =
            r#"{"Resources": {"A": {"Type": "X"}, "B": {"Type": "X"}, "C": {"Type": "X"}}}"#;
        let defs = vec![rd("A", "X"), rd("B", "X"), rd("C", "X")];
        assert_eq!(
            dependency_order(template, &BTreeMap::new(), &defs),
            vec![0, 1, 2]
        );
    }

    #[test]
    fn sub_escape_and_local_vars_are_not_edges() {
        // `${!A}` is a literal; `${V}` is a local Sub variable — neither is a
        // dependency on resource A.
        let template = r#"{
            "Resources": {
                "A": {"Type": "X", "Properties": {}},
                "B": {"Type": "X", "Properties": {
                    "V": {"Fn::Sub": ["${!A}-${V}", {"V": "literal"}]}
                }}
            }
        }"#;
        let defs = vec![rd("A", "X"), rd("B", "X")];
        // No real edge -> original order preserved.
        assert_eq!(
            dependency_order(template, &BTreeMap::new(), &defs),
            vec![0, 1]
        );
    }

    #[test]
    fn dependency_cycle_falls_back_to_original_order() {
        let template = r#"{
            "Resources": {
                "A": {"Type": "X", "Properties": {"V": {"Ref": "B"}}},
                "B": {"Type": "X", "Properties": {"V": {"Ref": "A"}}}
            }
        }"#;
        let defs = vec![rd("A", "X"), rd("B", "X")];
        assert_eq!(
            dependency_order(template, &BTreeMap::new(), &defs),
            vec![0, 1]
        );
    }
}
