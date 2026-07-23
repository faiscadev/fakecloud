use regex::Regex;
use serde_json::Value;
use std::collections::BTreeMap;

use crate::state::{ContainerInstance, EcsState};

/// Pick a container instance for a task using ECS placement constraints
/// and strategies.  Returns the ARN of the chosen instance.
///
/// This is best-effort: fakecloud does not model real resource
/// exhaustion, so `binpack` is approximated by task count and `spread`
/// by bucket count.  Fargate launch types skip placement entirely.
pub(crate) fn select_container_instance(
    state: &EcsState,
    cluster_name: &str,
    constraints: &[Value],
    strategies: &[Value],
    task_group: Option<&str>,
    task_definition_arn: &str,
    launch_type: &str,
) -> Option<String> {
    if launch_type == "FARGATE" {
        return None;
    }

    let mut candidates: Vec<&ContainerInstance> = state
        .container_instances
        .values()
        .filter(|ci| ci.cluster_name == cluster_name && ci.status == "ACTIVE")
        .collect();

    if candidates.is_empty() {
        return None;
    }

    // Apply constraints (filtering).
    for c in constraints {
        let ctype = c.get("type").and_then(|v| v.as_str()).unwrap_or("");
        match ctype {
            "memberOf" => {
                if let Some(expr) = c.get("expression").and_then(|v| v.as_str()) {
                    candidates.retain(|ci| {
                        evaluate_expression(state, ci, expr, task_group, task_definition_arn)
                    });
                }
            }
            "distinctInstance" => {
                candidates.retain(|ci| {
                    let has_conflict = state.tasks.values().any(|t| {
                        t.container_instance_arn.as_deref() == Some(&ci.container_instance_arn)
                            && t.last_status != "STOPPED"
                            && match task_group {
                                Some(g) => t.group.as_deref() == Some(g),
                                None => t.task_definition_arn == task_definition_arn,
                            }
                    });
                    !has_conflict
                });
            }
            _ => {}
        }
    }

    if candidates.is_empty() {
        return None;
    }

    if candidates.len() == 1 {
        return Some(candidates[0].container_instance_arn.clone());
    }

    // Apply strategies (ranking).
    // Build scores: lower = better.
    let mut scored: Vec<(&ContainerInstance, i64)> =
        candidates.into_iter().map(|ci| (ci, 0i64)).collect();

    for s in strategies {
        let stype = s.get("type").and_then(|v| v.as_str()).unwrap_or("");
        let field = s.get("field").and_then(|v| v.as_str()).unwrap_or("");
        match stype {
            "spread" => {
                let buckets = bucket_by_field(state, &scored, field);
                let max_per_bucket = buckets.values().map(|v| v.len()).max().unwrap_or(0);
                for (ci, score) in &mut scored {
                    let bucket_val = field_value(state, ci, field);
                    let bucket_count = buckets.get(&bucket_val).map(|v| v.len()).unwrap_or(0);
                    // Prefer emptier buckets.
                    *score += (bucket_count as i64) * 1000;
                    // Tie-break by instance ARN for determinism.
                    *score += string_hash(&ci.container_instance_arn);
                }
                // Also penalise if bucket already at max (strongly).
                for (ci, score) in &mut scored {
                    let bucket_val = field_value(state, ci, field);
                    let bucket_count = buckets.get(&bucket_val).map(|v| v.len()).unwrap_or(0);
                    if bucket_count >= max_per_bucket {
                        *score += 10_000;
                    }
                }
            }
            "binpack" => {
                // Pack onto instances that already have the most tasks.
                let counts = task_counts_per_instance(state, cluster_name);
                for (ci, score) in &mut scored {
                    let cnt = counts.get(&ci.container_instance_arn).copied().unwrap_or(0);
                    // Higher count = better = lower score.
                    *score -= (cnt as i64) * 1000;
                    *score += string_hash(&ci.container_instance_arn);
                }
            }
            "random" => {
                // Use a deterministic hash of the instance ARN so tests are stable.
                for (ci, score) in &mut scored {
                    *score += string_hash(&ci.container_instance_arn);
                }
            }
            _ => {}
        }
    }

    scored.sort_by_key(|a| a.1);
    Some(scored[0].0.container_instance_arn.clone())
}

fn evaluate_expression(
    state: &EcsState,
    ci: &ContainerInstance,
    expr: &str,
    task_group: Option<&str>,
    task_definition_arn: &str,
) -> bool {
    let expr = expr.trim();

    // Supported prefixes: attribute:, task:group, task:definition
    if let Some(rest) = expr.strip_prefix("task:group ") {
        return evaluate_simple(task_group.unwrap_or(""), rest);
    }
    if let Some(rest) = expr.strip_prefix("task:definition ") {
        return evaluate_simple(task_definition_arn, rest);
    }
    if let Some(rest) = expr.strip_prefix("attribute:") {
        return evaluate_attribute(state, ci, rest);
    }
    // Fallback: try to parse as generic expression.
    evaluate_simple(expr, "== true")
}

fn evaluate_attribute(state: &EcsState, ci: &ContainerInstance, rest: &str) -> bool {
    let key_end = rest.find(' ').unwrap_or(rest.len());
    let key = &rest[..key_end];
    let op_val = rest[key_end..].trim_start();

    let value = resolve_attribute_value(state, ci, key);
    match value {
        Some(v) => evaluate_simple(&v, op_val),
        None => {
            // If attribute is missing, treat != and !~ as true, everything else false.
            op_val.starts_with("!=") || op_val.starts_with("!~")
        }
    }
}

fn resolve_attribute_value(state: &EcsState, ci: &ContainerInstance, key: &str) -> Option<String> {
    // Check instance-level attributes first.
    for attr in &ci.attributes {
        if attr.name == key {
            return attr.value.clone();
        }
    }
    // Then cluster-level custom attributes keyed by target.
    let target_id = ci
        .container_instance_arn
        .rsplit_once('/')
        .map(|(_, id)| id)
        .unwrap_or("");
    let attr_key = format!("{}/{}/{}", ci.cluster_name, target_id, key);
    state
        .attributes
        .get(&attr_key)
        .and_then(|a| a.value.clone())
}

fn evaluate_simple(left: &str, rest: &str) -> bool {
    let rest = rest.trim_start();
    if let Some(right) = rest.strip_prefix("== ") {
        return left.trim() == right.trim();
    }
    if let Some(right) = rest.strip_prefix("!= ") {
        return left.trim() != right.trim();
    }
    if let Some(right) = rest.strip_prefix("=~ ") {
        let pattern = right.trim();
        return Regex::new(pattern)
            .map(|re| re.is_match(left))
            .unwrap_or(false);
    }
    if let Some(right) = rest.strip_prefix("!~ ") {
        let pattern = right.trim();
        return Regex::new(pattern)
            .map(|re| !re.is_match(left))
            .unwrap_or(true);
    }
    false
}

fn field_value(state: &EcsState, ci: &ContainerInstance, field: &str) -> String {
    if let Some(key) = field.strip_prefix("attribute:") {
        return resolve_attribute_value(state, ci, key).unwrap_or_default();
    }
    if field == "instanceId" {
        return ci
            .container_instance_arn
            .rsplit_once('/')
            .map(|(_, id)| id.to_string())
            .unwrap_or_default();
    }
    String::new()
}

fn bucket_by_field<'a>(
    state: &EcsState,
    scored: &[(&'a ContainerInstance, i64)],
    field: &str,
) -> BTreeMap<String, Vec<&'a ContainerInstance>> {
    let mut buckets: BTreeMap<String, Vec<&ContainerInstance>> = BTreeMap::new();
    for (ci, _) in scored {
        let val = field_value(state, ci, field);
        buckets.entry(val).or_default().push(*ci);
    }
    buckets
}

fn task_counts_per_instance(state: &EcsState, cluster_name: &str) -> BTreeMap<String, usize> {
    let mut counts: BTreeMap<String, usize> = BTreeMap::new();
    for t in state.tasks.values() {
        if t.cluster_name == cluster_name && t.last_status != "STOPPED" {
            if let Some(ref arn) = t.container_instance_arn {
                *counts.entry(arn.clone()).or_insert(0) += 1;
            }
        }
    }
    counts
}

fn string_hash(s: &str) -> i64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    s.hash(&mut h);
    (h.finish() as i64).abs() % 1000
}

#[cfg(test)]
mod tests {
    use super::select_container_instance;
    use crate::state::{AttributeRef, Cluster, ContainerInstance, EcsState, Task};
    use chrono::Utc;
    use serde_json::json;

    fn make_state() -> EcsState {
        let mut s = EcsState::new("000000000000", "us-east-1");
        let arn = s.cluster_arn("us-east-1", "default");
        s.clusters
            .insert("default".into(), Cluster::new("default", arn));
        s
    }

    fn add_instance(state: &mut EcsState, id: &str, attrs: Vec<(&str, &str)>) {
        let ci_arn = state.container_instance_arn("us-east-1", "default", id);
        let key = format!("default/{}", id);
        let attributes = attrs
            .into_iter()
            .map(|(name, value)| AttributeRef {
                name: name.to_string(),
                value: Some(value.to_string()),
                target_type: Some("container-instance".to_string()),
                target_id: Some(id.to_string()),
            })
            .collect();
        let ci = ContainerInstance {
            container_instance_arn: ci_arn.clone(),
            ec2_instance_id: Some(id.to_string()),
            cluster_name: "default".into(),
            cluster_arn: state.cluster_arn("us-east-1", "default"),
            status: "ACTIVE".into(),
            version: 1,
            version_info: None,
            agent_connected: true,
            agent_update_status: None,
            remaining_resources: Vec::new(),
            registered_resources: Vec::new(),
            running_tasks_count: 0,
            pending_tasks_count: 0,
            registered_at: Utc::now(),
            attributes,
            tags: Vec::new(),
            capacity_provider_name: None,
            health_status: None,
        };
        state.container_instances.insert(key, ci);
    }

    #[test]
    fn fargate_skips_placement() {
        let s = make_state();
        let arn = select_container_instance(&s, "default", &[], &[], None, "td", "FARGATE");
        assert!(arn.is_none());
    }

    #[test]
    fn member_of_equality_match() {
        let mut s = make_state();
        add_instance(&mut s, "i-1", vec![("ecs.availability-zone", "us-east-1a")]);
        let c = vec![
            json!({"type": "memberOf", "expression": "attribute:ecs.availability-zone == us-east-1a"}),
        ];
        let arn = select_container_instance(&s, "default", &c, &[], None, "td", "EC2");
        assert!(arn.is_some());
    }

    #[test]
    fn member_of_equality_no_match() {
        let mut s = make_state();
        add_instance(&mut s, "i-1", vec![("ecs.availability-zone", "us-east-1b")]);
        let c = vec![
            json!({"type": "memberOf", "expression": "attribute:ecs.availability-zone == us-east-1a"}),
        ];
        let arn = select_container_instance(&s, "default", &c, &[], None, "td", "EC2");
        assert!(arn.is_none());
    }

    #[test]
    fn member_of_regex_match() {
        let mut s = make_state();
        add_instance(&mut s, "i-1", vec![("ecs.instance-type", "t2.micro")]);
        let c =
            vec![json!({"type": "memberOf", "expression": "attribute:ecs.instance-type =~ t2.*"})];
        let arn = select_container_instance(&s, "default", &c, &[], None, "td", "EC2");
        assert!(arn.is_some());
    }

    #[test]
    fn distinct_instance_no_conflict() {
        let mut s = make_state();
        add_instance(&mut s, "i-1", vec![]);
        let c = vec![json!({"type": "distinctInstance"})];
        let arn =
            select_container_instance(&s, "default", &c, &[], Some("service:web"), "td", "EC2");
        assert!(arn.is_some());
    }

    #[test]
    fn distinct_instance_with_conflict() {
        let mut s = make_state();
        add_instance(&mut s, "i-1", vec![]);
        s.tasks.insert(
            "t1".into(),
            Task {
                task_arn: "a".into(),
                task_id: "t1".into(),
                cluster_arn: s.cluster_arn("us-east-1", "default"),
                cluster_name: "default".into(),
                task_definition_arn: "td".into(),
                family: "f".into(),
                revision: 1,
                container_instance_arn: Some(s.container_instance_arn(
                    "us-east-1",
                    "default",
                    "i-1",
                )),
                capacity_provider_name: None,
                last_status: "RUNNING".into(),
                desired_status: "RUNNING".into(),
                launch_type: "EC2".into(),
                platform_version: None,
                cpu: None,
                memory: None,
                containers: Vec::new(),
                overrides: json!({}),
                started_by: None,
                group: Some("service:web".into()),
                connectivity: "CONNECTED".into(),
                stop_code: None,
                stopped_reason: None,
                created_at: Utc::now(),
                started_at: None,
                stopping_at: None,
                stopped_at: None,
                pull_started_at: None,
                pull_stopped_at: None,
                connectivity_at: None,
                started_by_ref_id: None,
                execution_role_arn: None,
                task_role_arn: None,
                tags: Vec::new(),
                awslogs: None,
                captured_logs: String::new(),
                protection: None,
                enable_execute_command: false,
                attachments: Vec::new(),
                volume_configurations: Vec::new(),
                task_set_arn: None,
            },
        );
        let c = vec![json!({"type": "distinctInstance"})];
        let arn =
            select_container_instance(&s, "default", &c, &[], Some("service:web"), "td", "EC2");
        assert!(arn.is_none());
    }

    #[test]
    fn spread_prefers_emptier_bucket() {
        let mut s = make_state();
        add_instance(&mut s, "i-1", vec![("ecs.availability-zone", "us-east-1a")]);
        add_instance(&mut s, "i-2", vec![("ecs.availability-zone", "us-east-1a")]);
        add_instance(&mut s, "i-3", vec![("ecs.availability-zone", "us-east-1b")]);
        // Put a running task on i-1 in zone 1a.
        s.tasks.insert(
            "t1".into(),
            Task {
                task_arn: "a".into(),
                task_id: "t1".into(),
                cluster_arn: s.cluster_arn("us-east-1", "default"),
                cluster_name: "default".into(),
                task_definition_arn: "td".into(),
                family: "f".into(),
                revision: 1,
                container_instance_arn: Some(s.container_instance_arn(
                    "us-east-1",
                    "default",
                    "i-1",
                )),
                capacity_provider_name: None,
                last_status: "RUNNING".into(),
                desired_status: "RUNNING".into(),
                launch_type: "EC2".into(),
                platform_version: None,
                cpu: None,
                memory: None,
                containers: Vec::new(),
                overrides: json!({}),
                started_by: None,
                group: None,
                connectivity: "CONNECTED".into(),
                stop_code: None,
                stopped_reason: None,
                created_at: Utc::now(),
                started_at: None,
                stopping_at: None,
                stopped_at: None,
                pull_started_at: None,
                pull_stopped_at: None,
                connectivity_at: None,
                started_by_ref_id: None,
                execution_role_arn: None,
                task_role_arn: None,
                tags: Vec::new(),
                awslogs: None,
                captured_logs: String::new(),
                protection: None,
                enable_execute_command: false,
                attachments: Vec::new(),
                volume_configurations: Vec::new(),
                task_set_arn: None,
            },
        );
        let strat = vec![json!({"type": "spread", "field": "attribute:ecs.availability-zone"})];
        let arn = select_container_instance(&s, "default", &[], &strat, None, "td", "EC2");
        // Should pick i-3 because zone 1b has fewer tasks.
        assert_eq!(
            arn,
            Some(s.container_instance_arn("us-east-1", "default", "i-3"))
        );
    }

    #[test]
    fn binpack_prefers_busiest_instance() {
        let mut s = make_state();
        add_instance(&mut s, "i-1", vec![]);
        add_instance(&mut s, "i-2", vec![]);
        s.tasks.insert(
            "t1".into(),
            Task {
                task_arn: "a".into(),
                task_id: "t1".into(),
                cluster_arn: s.cluster_arn("us-east-1", "default"),
                cluster_name: "default".into(),
                task_definition_arn: "td".into(),
                family: "f".into(),
                revision: 1,
                container_instance_arn: Some(s.container_instance_arn(
                    "us-east-1",
                    "default",
                    "i-1",
                )),
                capacity_provider_name: None,
                last_status: "RUNNING".into(),
                desired_status: "RUNNING".into(),
                launch_type: "EC2".into(),
                platform_version: None,
                cpu: None,
                memory: None,
                containers: Vec::new(),
                overrides: json!({}),
                started_by: None,
                group: None,
                connectivity: "CONNECTED".into(),
                stop_code: None,
                stopped_reason: None,
                created_at: Utc::now(),
                started_at: None,
                stopping_at: None,
                stopped_at: None,
                pull_started_at: None,
                pull_stopped_at: None,
                connectivity_at: None,
                started_by_ref_id: None,
                execution_role_arn: None,
                task_role_arn: None,
                tags: Vec::new(),
                awslogs: None,
                captured_logs: String::new(),
                protection: None,
                enable_execute_command: false,
                attachments: Vec::new(),
                volume_configurations: Vec::new(),
                task_set_arn: None,
            },
        );
        let strat = vec![json!({"type": "binpack", "field": "instanceId"})];
        let arn = select_container_instance(&s, "default", &[], &strat, None, "td", "EC2");
        assert_eq!(
            arn,
            Some(s.container_instance_arn("us-east-1", "default", "i-1"))
        );
    }

    #[test]
    fn random_returns_some_when_candidates_exist() {
        let mut s = make_state();
        add_instance(&mut s, "i-1", vec![]);
        let strat = vec![json!({"type": "random"})];
        let arn = select_container_instance(&s, "default", &[], &strat, None, "td", "EC2");
        assert_eq!(
            arn,
            Some(s.container_instance_arn("us-east-1", "default", "i-1"))
        );
    }
}
