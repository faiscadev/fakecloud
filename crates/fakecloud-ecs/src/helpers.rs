use super::*;

pub(crate) fn is_mutating(action: &str) -> bool {
    matches!(
        action,
        "CreateCluster"
            | "DeleteCluster"
            | "UpdateCluster"
            | "UpdateClusterSettings"
            | "PutClusterCapacityProviders"
            | "RegisterTaskDefinition"
            | "DeregisterTaskDefinition"
            | "DeleteTaskDefinitions"
            | "TagResource"
            | "UntagResource"
            | "PutAccountSetting"
            | "PutAccountSettingDefault"
            | "DeleteAccountSetting"
            | "RunTask"
            | "StartTask"
            | "StopTask"
            | "CreateService"
            | "UpdateService"
            | "DeleteService"
            | "RegisterContainerInstance"
            | "DeregisterContainerInstance"
            | "UpdateContainerAgent"
            | "UpdateContainerInstancesState"
            | "PutAttributes"
            | "DeleteAttributes"
            | "CreateCapacityProvider"
            | "DeleteCapacityProvider"
            | "UpdateCapacityProvider"
            | "UpdateTaskProtection"
            | "CreateTaskSet"
            | "UpdateTaskSet"
            | "DeleteTaskSet"
            | "UpdateServicePrimaryTaskSet"
            | "SubmitContainerStateChange"
            | "SubmitTaskStateChange"
            | "SubmitAttachmentStateChanges"
            | "StopServiceDeployment"
            | "RegisterDaemonTaskDefinition"
            | "DeleteDaemonTaskDefinition"
            | "CreateDaemon"
            | "UpdateDaemon"
            | "DeleteDaemon"
            | "CreateExpressGatewayService"
            | "UpdateExpressGatewayService"
            | "DeleteExpressGatewayService"
    )
}

pub(crate) fn req_str<'a>(body: &'a Value, field: &str) -> Result<&'a str, AwsServiceError> {
    body.get(field)
        .and_then(|v| v.as_str())
        .ok_or_else(|| client_exception(format!("Missing required field: {field}")))
}

pub(crate) fn req_array<'a>(
    body: &'a Value,
    field: &str,
) -> Result<&'a Vec<Value>, AwsServiceError> {
    body.get(field)
        .and_then(|v| v.as_array())
        .ok_or_else(|| client_exception(format!("Missing required field: {field}")))
}

pub(crate) fn req_bool(body: &Value, field: &str) -> Result<bool, AwsServiceError> {
    body.get(field)
        .and_then(|v| v.as_bool())
        .ok_or_else(|| client_exception(format!("Missing required field: {field}")))
}

pub(crate) fn opt_str<'a>(body: &'a Value, field: &str) -> Option<&'a str> {
    body.get(field).and_then(|v| v.as_str())
}

/// Paginate a list of ARN-like strings with a key-based cursor instead of a
/// numeric offset. The list is sorted for a stable order (HashMap/BTreeMap
/// value order is not a contract callers can rely on), and the returned
/// `nextToken` is an opaque base64 of the last-emitted key. Resuming finds the
/// first key strictly greater than the cursor, so pages stay correct even when
/// items are added or removed between calls — a numeric offset would skip or
/// duplicate rows there. A non-empty token that doesn't base64-decode (a stale
/// or corrupted cursor we never emitted) is treated as past the end and yields
/// an empty page — restarting at the beginning would silently re-list page 1
/// and risk an infinite pagination loop for the client.
pub(crate) fn paginate_arns(
    arns: Vec<String>,
    next_token: &str,
    max_results: usize,
) -> (Vec<String>, Option<String>) {
    paginate_arns_dir(arns, next_token, max_results, false)
}

/// Like [`paginate_arns`] but honors a sort direction. `descending` reverses the
/// canonical (ascending, deduped) order and adjusts the resume cursor
/// comparison so DESC pages advance correctly instead of being silently
/// re-sorted back to ASC.
pub(crate) fn paginate_arns_dir(
    mut arns: Vec<String>,
    next_token: &str,
    max_results: usize,
    descending: bool,
) -> (Vec<String>, Option<String>) {
    use base64::Engine;
    arns.sort();
    arns.dedup();
    if descending {
        arns.reverse();
    }
    let cursor = if next_token.is_empty() {
        None
    } else {
        base64::engine::general_purpose::STANDARD
            .decode(next_token)
            .ok()
            .and_then(|b| String::from_utf8(b).ok())
    };
    let start = if next_token.is_empty() {
        0
    } else {
        match &cursor {
            // Resume just past the cursor. For ASC the next element is the first
            // greater than the cursor; for DESC it's the first less than it.
            Some(c) => arns
                .iter()
                .position(|a| {
                    if descending {
                        a.as_str() < c.as_str()
                    } else {
                        a.as_str() > c.as_str()
                    }
                })
                .unwrap_or(arns.len()),
            // Non-empty but undecodable token: a cursor we never emitted.
            // Treat as past the end (empty page) rather than restarting at 0.
            None => arns.len(),
        }
    };
    let end = (start + max_results).min(arns.len());
    let page = arns[start..end].to_vec();
    let next = if end < arns.len() {
        Some(base64::engine::general_purpose::STANDARD.encode(arns[end - 1].as_bytes()))
    } else {
        None
    };
    (page, next)
}

/// Validate that an optional string field, if present, is one of the allowed
/// enum values. Returns InvalidParameterException if the field is set to a
/// value outside `allowed`. Absent or null fields are accepted.
pub(crate) fn validate_enum_opt(
    body: &Value,
    field: &str,
    allowed: &[&str],
) -> Result<(), AwsServiceError> {
    if let Some(v) = body.get(field).and_then(|v| v.as_str()) {
        if !allowed.contains(&v) {
            return Err(invalid_parameter(format!(
                "Invalid value '{v}' for field '{field}'. Valid values: {}",
                allowed.join(", ")
            )));
        }
    }
    Ok(())
}

pub(crate) fn client_exception(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ClientException", message)
}

pub(crate) fn invalid_parameter(message: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidParameterException",
        message,
    )
}

pub(crate) fn cluster_not_found(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ClusterNotFoundException",
        format!("The referenced cluster was inactive: {name}"),
    )
}

pub(crate) fn cluster_contains_services() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ClusterContainsServicesException",
        "The specified cluster still contains active services",
    )
}

pub(crate) fn cluster_contains_tasks() -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ClusterContainsTasksException",
        "The specified cluster still contains active tasks",
    )
}

pub(crate) fn task_definition_not_found(family_rev: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ClientException",
        format!("Unable to describe task definition: {family_rev}"),
    )
}

pub(crate) fn parse_tags(body: &Value) -> Vec<TagEntry> {
    body.get("tags")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|t| {
                    let k = t.get("key").and_then(|v| v.as_str())?;
                    let v = t.get("value").and_then(|v| v.as_str()).unwrap_or("");
                    Some(TagEntry {
                        key: k.to_string(),
                        value: v.to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

pub(crate) fn tags_json(tags: &[TagEntry]) -> Value {
    Value::Array(
        tags.iter()
            .map(|t| json!({"key": t.key, "value": t.value}))
            .collect(),
    )
}

pub(crate) fn merge_tags(current: &mut Vec<TagEntry>, incoming: Vec<TagEntry>) {
    for new_tag in incoming {
        if let Some(existing) = current.iter_mut().find(|t| t.key == new_tag.key) {
            existing.value = new_tag.value;
        } else {
            current.push(new_tag);
        }
    }
}

pub(crate) fn cluster_to_json(cluster: &Cluster) -> Value {
    json!({
        "clusterArn": cluster.cluster_arn,
        "clusterName": cluster.cluster_name,
        "status": cluster.status,
        "registeredContainerInstancesCount": cluster.registered_container_instances_count,
        "runningTasksCount": cluster.running_tasks_count,
        "pendingTasksCount": cluster.pending_tasks_count,
        "activeServicesCount": cluster.active_services_count,
        "statistics": cluster.statistics,
        "tags": tags_json(&cluster.tags),
        "settings": cluster.settings,
        "configuration": cluster.configuration,
        "capacityProviders": cluster.capacity_providers,
        "defaultCapacityProviderStrategy": cluster.default_capacity_provider_strategy,
        "attachments": cluster.attachments,
        "attachmentsStatus": cluster.attachments_status,
        "serviceConnectDefaults": cluster.service_connect_defaults,
    })
}

pub(crate) fn task_definition_to_json(td: &TaskDefinition) -> Value {
    let mut map = Map::new();
    map.insert("taskDefinitionArn".into(), json!(td.task_definition_arn));
    map.insert("family".into(), json!(td.family));
    map.insert("revision".into(), json!(td.revision));
    map.insert("status".into(), json!(td.status));
    map.insert(
        "containerDefinitions".into(),
        Value::Array(
            td.container_definitions
                .iter()
                .map(normalize_container_definition)
                .collect(),
        ),
    );
    map.insert("compatibilities".into(), json!(td.compatibilities));
    map.insert(
        "requiresCompatibilities".into(),
        json!(td.requires_compatibilities),
    );
    map.insert("volumes".into(), Value::Array(td.volumes.clone()));
    map.insert(
        "placementConstraints".into(),
        Value::Array(td.placement_constraints.clone()),
    );
    map.insert(
        "requiresAttributes".into(),
        Value::Array(td.requires_attributes.clone()),
    );
    map.insert(
        "inferenceAccelerators".into(),
        Value::Array(td.inference_accelerators.clone()),
    );
    if let Some(ref x) = td.network_mode {
        map.insert("networkMode".into(), json!(x));
    }
    if let Some(ref x) = td.cpu {
        map.insert("cpu".into(), json!(x));
    }
    if let Some(ref x) = td.memory {
        map.insert("memory".into(), json!(x));
    }
    if let Some(ref x) = td.task_role_arn {
        map.insert("taskRoleArn".into(), json!(x));
    }
    if let Some(ref x) = td.execution_role_arn {
        map.insert("executionRoleArn".into(), json!(x));
    }
    if let Some(ref x) = td.pid_mode {
        map.insert("pidMode".into(), json!(x));
    }
    if let Some(ref x) = td.ipc_mode {
        map.insert("ipcMode".into(), json!(x));
    }
    if let Some(ref x) = td.proxy_configuration {
        map.insert("proxyConfiguration".into(), x.clone());
    }
    if let Some(ref x) = td.ephemeral_storage {
        map.insert("ephemeralStorage".into(), x.clone());
    }
    if let Some(ref x) = td.runtime_platform {
        map.insert("runtimePlatform".into(), x.clone());
    }
    if let Some(ref x) = td.registered_by {
        map.insert("registeredBy".into(), json!(x));
    }
    map.insert("registeredAt".into(), json!(td.registered_at.timestamp()));
    if let Some(ts) = td.deregistered_at {
        map.insert("deregisteredAt".into(), json!(ts.timestamp()));
    }
    if let Some(enabled) = td.enable_fault_injection {
        map.insert("enableFaultInjection".into(), json!(enabled));
    }
    Value::Object(map)
}

/// Resolve a service ARN tail to its storage key in `state.services`.
/// Long-form ARNs carry `cluster/service` directly; short-form
/// (pre-Nov-2018, still emitted when `serviceLongArnFormat=disabled`)
/// only carry the service name, so we scan for any cluster whose stored
/// key ends with `/<name>`.
pub(crate) fn resolve_service_key(state: &EcsState, tail: &str) -> Option<String> {
    if tail.contains('/') {
        return state.services.contains_key(tail).then(|| tail.to_string());
    }
    let suffix = format!("/{tail}");
    state
        .services
        .keys()
        .find(|k| k.ends_with(&suffix))
        .cloned()
}

/// Same idea for container instances. Storage keys are
/// `cluster/instance-id`; short ARN tails are just `instance-id`.
pub(crate) fn resolve_container_instance_key(state: &EcsState, tail: &str) -> Option<String> {
    if tail.contains('/') {
        return state
            .container_instances
            .contains_key(tail)
            .then(|| tail.to_string());
    }
    let suffix = format!("/{tail}");
    state
        .container_instances
        .keys()
        .find(|k| k.ends_with(&suffix))
        .cloned()
}

/// Decode an `arn:aws:ecs:<region>:<account>:<type>/<name>[:<rev>]` ARN
/// into `(account, resource_type, tail)`. For task definitions `tail` is
/// `family:revision`; for clusters it's `cluster_name`.
pub(crate) fn decode_ecs_arn(arn: &str) -> Result<(String, String, String), AwsServiceError> {
    let rest = arn
        .strip_prefix("arn:aws:ecs:")
        .ok_or_else(|| invalid_parameter(format!("Malformed ECS ARN: {arn}")))?;
    // Resource portion may itself contain a trailing `:<revision>`, so we
    // split at most three ways then treat the remainder as the resource.
    let mut parts = rest.splitn(3, ':');
    let _region = parts
        .next()
        .ok_or_else(|| invalid_parameter("Malformed ECS ARN"))?;
    let account = parts
        .next()
        .ok_or_else(|| invalid_parameter("Malformed ECS ARN"))?;
    let resource = parts
        .next()
        .ok_or_else(|| invalid_parameter("Malformed ECS ARN"))?;
    let (resource_type, tail) = resource
        .split_once('/')
        .ok_or_else(|| invalid_parameter("Malformed ECS ARN"))?;
    Ok((
        account.to_string(),
        resource_type.to_string(),
        tail.to_string(),
    ))
}

/// Parse a `family[:revision]` reference. Returns `(family, Some(rev))`
/// when a specific revision is requested, or `(family, None)` for the
/// latest-active shorthand.
pub(crate) fn parse_family_revision(input: &str) -> (String, Option<i32>) {
    if let Some((family, rev)) = input.rsplit_once(':') {
        if let Ok(n) = rev.parse::<i32>() {
            return (family.to_string(), Some(n));
        }
    }
    (input.to_string(), None)
}

/// Task-definition ARNs may appear as the full ARN or just `family:rev`.
/// Returns `(account, family, Some(rev))` where `account` is `None` for
/// the bare shorthand form.
pub(crate) fn resolve_task_definition_ref(
    input: &str,
) -> Result<(Option<String>, String, Option<i32>), AwsServiceError> {
    if input.starts_with("arn:aws:ecs:") {
        let (account, resource_type, tail) = decode_ecs_arn(input)?;
        if resource_type != "task-definition" {
            return Err(invalid_parameter(format!(
                "Expected task-definition ARN: {input}"
            )));
        }
        let (family, rev) = parse_family_revision(&tail);
        Ok((Some(account), family, rev))
    } else {
        let (family, rev) = parse_family_revision(input);
        Ok((None, family, rev))
    }
}

pub(crate) fn target_account_for_task_definition(request: &AwsRequest, td_ref: &str) -> String {
    if let Ok((Some(account), _, _)) = resolve_task_definition_ref(td_ref) {
        account
    } else {
        request.account_id.clone()
    }
}

pub(crate) fn target_account_for_cluster(
    request: &AwsRequest,
    cluster_ref: Option<&str>,
) -> String {
    if let Some(input) = cluster_ref {
        if input.starts_with("arn:aws:ecs:") {
            if let Ok((account, _, _)) = decode_ecs_arn(input) {
                return account;
            }
        }
    }
    request.account_id.clone()
}

pub(crate) fn latest_active_revision(
    revisions: &std::collections::BTreeMap<i32, TaskDefinition>,
) -> Option<&TaskDefinition> {
    revisions.values().rev().find(|td| td.status == "ACTIVE")
}

pub(crate) fn validate_family_name(family: &str) -> Result<(), AwsServiceError> {
    if family.is_empty() || family.len() > 255 {
        return Err(invalid_parameter(
            "Task definition family must be 1-255 characters",
        ));
    }
    let ok = family
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if !ok {
        return Err(invalid_parameter(
            "Task definition family may only contain letters, numbers, hyphens, and underscores",
        ));
    }
    Ok(())
}

pub(crate) fn resource_not_found(arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ClientException",
        format!("The referenced resource was not found: {arn}"),
    )
}

pub(crate) fn matches_filter(filter: Option<&str>, value: &str) -> bool {
    filter.is_none_or(|f| f == value)
}

pub(crate) fn task_not_found(task_ref: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ClientException",
        format!("Task not found: {task_ref}"),
    )
}

/// Strip cluster prefix + optional ARN prefix to recover the task UUID.
pub(crate) fn task_id_from_ref(input: &str) -> String {
    if let Some(rest) = input.rsplit('/').next() {
        return rest.to_string();
    }
    input.to_string()
}

pub(crate) fn resolve_task_id(state: &EcsState, task_ref: &str) -> Result<String, AwsServiceError> {
    let id = task_id_from_ref(task_ref);
    if state.tasks.contains_key(&id) {
        Ok(id)
    } else {
        Err(task_not_found(task_ref))
    }
}

pub(crate) fn task_to_json(task: &Task) -> Value {
    let mut map = serde_json::Map::new();
    map.insert("taskArn".into(), json!(task.task_arn));
    map.insert("clusterArn".into(), json!(task.cluster_arn));
    map.insert("taskDefinitionArn".into(), json!(task.task_definition_arn));
    map.insert("lastStatus".into(), json!(task.last_status));
    map.insert("desiredStatus".into(), json!(task.desired_status));
    map.insert("launchType".into(), json!(task.launch_type));
    if let Some(ref v) = task.platform_version {
        map.insert("platformVersion".into(), json!(v));
    }
    if let Some(ref v) = task.cpu {
        map.insert("cpu".into(), json!(v));
    }
    if let Some(ref v) = task.memory {
        map.insert("memory".into(), json!(v));
    }
    map.insert(
        "containers".into(),
        Value::Array(task.containers.iter().map(container_to_json).collect()),
    );
    map.insert(
        "overrides".into(),
        normalize_task_overrides(&task.overrides),
    );
    if let Some(ref v) = task.started_by {
        map.insert("startedBy".into(), json!(v));
    }
    if let Some(ref v) = task.group {
        map.insert("group".into(), json!(v));
    }
    if let Some(ref v) = task.task_set_arn {
        map.insert("taskSetArn".into(), json!(v));
    }
    map.insert("connectivity".into(), json!(task.connectivity));
    if let Some(ref v) = task.stop_code {
        map.insert("stopCode".into(), json!(v));
    }
    if let Some(ref v) = task.stopped_reason {
        map.insert("stoppedReason".into(), json!(v));
    }
    // `taskRoleArn` and `executionRoleArn` are task-definition fields,
    // not Task runtime fields. Intentionally not echoed on the Task shape
    // to match AWS — they appear on the corresponding TaskDefinition.
    let _ = (&task.task_role_arn, &task.execution_role_arn);
    map.insert("createdAt".into(), json!(task.created_at.timestamp()));
    if let Some(ts) = task.started_at {
        map.insert("startedAt".into(), json!(ts.timestamp()));
    }
    if let Some(ts) = task.stopping_at {
        map.insert("stoppingAt".into(), json!(ts.timestamp()));
    }
    if let Some(ts) = task.stopped_at {
        map.insert("stoppedAt".into(), json!(ts.timestamp()));
    }
    if let Some(ts) = task.pull_started_at {
        map.insert("pullStartedAt".into(), json!(ts.timestamp()));
    }
    if let Some(ts) = task.pull_stopped_at {
        map.insert("pullStoppedAt".into(), json!(ts.timestamp()));
    }
    if let Some(ts) = task.connectivity_at {
        map.insert("connectivityAt".into(), json!(ts.timestamp()));
    }
    // Always-present fields AWS returns. SDKs deserialize these
    // unconditionally; without them, terraform/cdk plan diffs and
    // observability hooks see missing keys.
    map.insert(
        "attachments".into(),
        Value::Array(
            task.attachments
                .iter()
                .map(|a| {
                    let mut obj = serde_json::Map::new();
                    obj.insert("id".into(), json!(a.id));
                    obj.insert("type".into(), json!(a.attachment_type));
                    obj.insert("status".into(), json!(a.status));
                    obj.insert(
                        "details".into(),
                        Value::Array(
                            a.details
                                .iter()
                                .map(|d| {
                                    serde_json::json!({
                                        "name": d.name,
                                        "value": d.value,
                                    })
                                })
                                .collect(),
                        ),
                    );
                    Value::Object(obj)
                })
                .collect(),
        ),
    );
    map.insert("attributes".into(), json!([]));
    map.insert("availabilityZone".into(), json!("us-east-1a"));
    // Always emit `containerInstanceArn` as a string. AWS returns a
    // synthetic placeholder for Fargate tasks rather than null — matching
    // that here keeps the response shape stable for SDKs that deserialize
    // into a non-nullable String.
    map.insert(
        "containerInstanceArn".into(),
        if let Some(ref arn) = task.container_instance_arn {
            json!(arn)
        } else {
            json!(format!(
                "{}/container-instance/i-fakecloud-1",
                task.cluster_arn
            ))
        },
    );
    map.insert(
        "enableExecuteCommand".into(),
        json!(task.enable_execute_command),
    );
    map.insert("ephemeralStorage".into(), json!({ "sizeInGiB": 20 }));
    map.insert("healthStatus".into(), json!(aggregate_task_health(task)));
    map.insert("version".into(), json!(1));
    if let Some(ref cp) = task.capacity_provider_name {
        map.insert("capacityProviderName".into(), json!(cp));
    }
    map.insert(
        "platformFamily".into(),
        match task.launch_type.as_str() {
            "FARGATE" => json!("Linux"),
            _ => Value::Null,
        },
    );
    if let Some(ts) = task.stopped_at {
        map.insert("executionStoppedAt".into(), json!(ts.timestamp()));
    }
    if !task.tags.is_empty() {
        map.insert(
            "tags".into(),
            json!(task
                .tags
                .iter()
                .map(|t| json!({ "key": t.key, "value": t.value }))
                .collect::<Vec<_>>()),
        );
    }
    if !task.volume_configurations.is_empty() {
        map.insert(
            "volumeConfigurations".into(),
            Value::Array(task.volume_configurations.clone()),
        );
    }
    Value::Object(map)
}

/// Aggregate per-container `healthStatus` into the task-level
/// `healthStatus` AWS surfaces on DescribeTasks. Rules mirror real ECS:
/// - `UNHEALTHY` if any essential container is `UNHEALTHY`
/// - `HEALTHY` if every essential container is `HEALTHY`
/// - `UNKNOWN` otherwise (anything still warming up / no probe defined)
pub(crate) fn aggregate_task_health(task: &Task) -> &'static str {
    let essentials: Vec<&Container> = task.containers.iter().filter(|c| c.essential).collect();
    if essentials.is_empty() {
        return "UNKNOWN";
    }
    let any_unhealthy = essentials
        .iter()
        .any(|c| c.health_status.as_deref() == Some("UNHEALTHY"));
    if any_unhealthy {
        return "UNHEALTHY";
    }
    let all_healthy = essentials
        .iter()
        .all(|c| c.health_status.as_deref() == Some("HEALTHY"));
    if all_healthy {
        return "HEALTHY";
    }
    "UNKNOWN"
}

pub(crate) fn container_to_json(container: &Container) -> Value {
    let mut map = serde_json::Map::new();
    map.insert("containerArn".into(), json!(container.container_arn));
    map.insert("taskArn".into(), json!(container.task_arn));
    map.insert("name".into(), json!(container.name));
    map.insert("image".into(), json!(container.image));
    if let Some(ref digest) = container.image_digest {
        map.insert("imageDigest".into(), json!(digest));
    }
    map.insert("lastStatus".into(), json!(container.last_status));
    // `essential` is a task-definition-level container field, not part of
    // the `Container` runtime shape; intentionally omitted to match AWS.
    let _ = container.essential;
    if let Some(code) = container.exit_code {
        map.insert("exitCode".into(), json!(code));
    }
    if let Some(ref r) = container.reason {
        map.insert("reason".into(), json!(r));
    }
    if let Some(ref id) = container.runtime_id {
        map.insert("runtimeId".into(), json!(id));
    }
    if let Some(ref v) = container.cpu {
        map.insert("cpu".into(), json!(v));
    }
    if let Some(ref v) = container.memory {
        map.insert("memory".into(), json!(v));
    }
    if let Some(ref v) = container.memory_reservation {
        map.insert("memoryReservation".into(), json!(v));
    }
    map.insert("networkBindings".into(), json!(container.network_bindings));
    map.insert(
        "networkInterfaces".into(),
        json!(container.network_interfaces),
    );
    if let Some(ref v) = container.health_status {
        map.insert("healthStatus".into(), json!(v));
    }
    Value::Object(map)
}

pub(crate) fn validate_service_name(name: &str) -> Result<(), AwsServiceError> {
    if name.is_empty() || name.len() > 255 {
        return Err(invalid_parameter("Service name must be 1-255 characters"));
    }
    let ok = name
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
    if !ok {
        return Err(invalid_parameter(
            "Service name may only contain letters, numbers, hyphens, and underscores",
        ));
    }
    Ok(())
}

pub(crate) fn service_name_from_ref(input: &str) -> String {
    if let Some(rest) = input.rsplit('/').next() {
        return rest.to_string();
    }
    input.to_string()
}

pub(crate) fn service_not_found(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ServiceNotFoundException",
        format!("The service could not be found: {name}"),
    )
}

pub(crate) fn service_already_exists(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ServiceNotActiveException",
        format!("The service {name} already exists"),
    )
}

/// Spawn N tasks for a service by cloning the task-definition containers
/// and inserting `Task` rows in the shared state. The task IDs are
/// returned so the caller can hand them to `EcsRuntime::run_task` after
/// releasing the state write lock.
pub(crate) fn spawn_service_tasks(
    state: &mut EcsState,
    service: &Service,
    count: i32,
    principal_arn: &str,
    launch_type: &str,
    task_set_arn: Option<String>,
) -> Vec<String> {
    if count <= 0 {
        return Vec::new();
    }
    let Some(revisions) = state.task_definitions.get(&service.family) else {
        return Vec::new();
    };
    let Some(td) = revisions.get(&service.revision) else {
        return Vec::new();
    };
    let container_defs = td.container_definitions.clone();
    let cpu = td.cpu.clone();
    let memory = td.memory.clone();
    let task_role = td.task_role_arn.clone();
    let exec_role = td.execution_role_arn.clone();
    let cluster_name = service.cluster_name.clone();
    let cluster_arn = service.cluster_arn.clone();
    let td_arn = service.task_definition_arn.clone();
    let family = service.family.clone();
    let revision = service.revision;
    let service_tag = format!("ecs-svc/{}", service.service_name);
    // Resolve task tags from the propagateTags strategy. AWS copies
    // TaskDefinition.tags or Service.tags onto each spawned Task; the
    // default ("NONE") leaves the task untagged.
    let propagated_tags: Vec<TagEntry> = match service.propagate_tags.as_deref() {
        Some("TASK_DEFINITION") => td.tags.clone(),
        Some("SERVICE") => service.tags.clone(),
        _ => Vec::new(),
    };
    let service_exec = service.enable_execute_command;

    let mut ids = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let task_id = uuid::Uuid::new_v4().to_string().replace('-', "");
        let task_arn = state.task_arn(&cluster_name, &task_id);
        let containers: Vec<Container> = container_defs
            .iter()
            .map(|def| Container {
                container_arn: format!(
                    "arn:aws:ecs:{}:{}:container/{}/{}/{}",
                    state.region,
                    state.account_id,
                    cluster_name,
                    task_id,
                    def.get("name").and_then(|v| v.as_str()).unwrap_or("app")
                ),
                name: def
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("app")
                    .to_string(),
                image: def
                    .get("image")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                task_arn: task_arn.clone(),
                last_status: "PENDING".into(),
                exit_code: None,
                reason: None,
                runtime_id: None,
                essential: def
                    .get("essential")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true),
                cpu: def
                    .get("cpu")
                    .and_then(|v| v.as_i64())
                    .map(|n| n.to_string()),
                memory: def
                    .get("memory")
                    .and_then(|v| v.as_i64())
                    .map(|n| n.to_string()),
                memory_reservation: def
                    .get("memoryReservation")
                    .and_then(|v| v.as_i64())
                    .map(|n| n.to_string()),
                network_bindings: Vec::new(),
                network_interfaces: Vec::new(),
                health_status: Some("UNKNOWN".into()),
                managed_agents: None,
                image_digest: None,
            })
            .collect();
        let awslogs = container_defs.iter().find_map(|def| {
            let name = def.get("name").and_then(|v| v.as_str())?.to_string();
            let log_cfg = def.get("logConfiguration")?;
            if log_cfg.get("logDriver").and_then(|v| v.as_str()) != Some("awslogs") {
                return None;
            }
            let opts = log_cfg.get("options").and_then(|v| v.as_object())?;
            Some(AwsLogsConfig {
                group: opts.get("awslogs-group").and_then(|v| v.as_str())?.into(),
                stream_prefix: opts
                    .get("awslogs-stream-prefix")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                region: opts
                    .get("awslogs-region")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&state.region)
                    .to_string(),
                container_name: name,
            })
        });
        let mut task = Task {
            task_arn: task_arn.clone(),
            task_id: task_id.clone(),
            cluster_arn: cluster_arn.clone(),
            cluster_name: cluster_name.clone(),
            task_definition_arn: td_arn.clone(),
            family: family.clone(),
            revision,
            container_instance_arn: None,
            capacity_provider_name: None,
            last_status: "PENDING".into(),
            desired_status: "RUNNING".into(),
            launch_type: launch_type.into(),
            platform_version: Some("1.4.0".into()),
            cpu: cpu.clone(),
            memory: memory.clone(),
            containers,
            overrides: json!({}),
            started_by: Some(service_tag.clone()),
            group: Some(format!("service:{}", service.service_name)),
            connectivity: "CONNECTING".into(),
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
            execution_role_arn: exec_role.clone(),
            task_role_arn: task_role.clone(),
            tags: propagated_tags.clone(),
            awslogs,
            captured_logs: String::new(),
            protection: None,
            enable_execute_command: service_exec,
            attachments: Vec::new(),
            volume_configurations: Vec::new(),
            task_set_arn: task_set_arn.clone(),
        };
        if launch_type != "FARGATE" {
            if let Some(arn) = crate::placement::select_container_instance(
                state,
                &cluster_name,
                &service.placement_constraints,
                &service.placement_strategy,
                task.group.as_deref(),
                &td_arn,
                launch_type,
            ) {
                task.container_instance_arn = Some(arn.clone());
                if let Some(ci) = state
                    .container_instances
                    .values_mut()
                    .find(|ci| ci.container_instance_arn == arn)
                {
                    ci.pending_tasks_count += 1;
                }
            }
        }
        state.tasks.insert(task_id.clone(), task);
        if let Some(cluster) = state.clusters.get_mut(&cluster_name) {
            cluster.pending_tasks_count += 1;
        }
        ids.push(task_id);
    }
    let _ = principal_arn;
    ids
}

/// Spawn one task per capacity provider ARN for a daemon by cloning the
/// daemon task-definition containers and inserting `Task` rows in the
/// shared state. The task IDs are returned so the caller can hand them
/// to `EcsRuntime::run_task` after releasing the state write lock.
pub(crate) fn spawn_daemon_tasks(
    state: &mut EcsState,
    daemon: &crate::state::Daemon,
    principal_arn: &str,
    launch_type: &str,
) -> Vec<String> {
    if daemon.capacity_provider_arns.is_empty() {
        return Vec::new();
    }
    let Some((family, revision)) = parse_daemon_td_arn(&daemon.daemon_task_definition_arn) else {
        return Vec::new();
    };
    let Some(revisions) = state.daemon_task_definitions.get(&family) else {
        return Vec::new();
    };
    let Some(td) = revisions.get(&revision) else {
        return Vec::new();
    };
    let container_defs = td.container_definitions.clone();
    let cpu = td.cpu.clone();
    let memory = td.memory.clone();
    let task_role = td.task_role_arn.clone();
    let exec_role = td.execution_role_arn.clone();
    let cluster_name = daemon.cluster_name.clone();
    let cluster_arn = daemon.cluster_arn.clone();
    let td_arn = daemon.daemon_task_definition_arn.clone();
    let daemon_tag = format!("ecs-daemon/{}", daemon.daemon_name);
    let daemon_exec = daemon.enable_execute_command;
    let propagated_tags: Vec<TagEntry> = match daemon.propagate_tags.as_deref() {
        Some("TASK_DEFINITION") => td.tags.clone(),
        Some("DAEMON") => daemon.tags.clone(),
        _ => Vec::new(),
    };

    let mut ids = Vec::with_capacity(daemon.capacity_provider_arns.len());
    for cap_arn in &daemon.capacity_provider_arns {
        let cap_name = cap_arn
            .rsplit_once('/')
            .map(|(_, n)| n.to_string())
            .unwrap_or_else(|| cap_arn.clone());
        let task_id = uuid::Uuid::new_v4().to_string().replace('-', "");
        let task_arn = state.task_arn(&cluster_name, &task_id);
        let containers: Vec<Container> = container_defs
            .iter()
            .map(|def| Container {
                container_arn: format!(
                    "arn:aws:ecs:{}:{}:container/{}/{}/{}",
                    state.region,
                    state.account_id,
                    cluster_name,
                    task_id,
                    def.get("name").and_then(|v| v.as_str()).unwrap_or("app")
                ),
                name: def
                    .get("name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("app")
                    .to_string(),
                image: def
                    .get("image")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                task_arn: task_arn.clone(),
                last_status: "PENDING".into(),
                exit_code: None,
                reason: None,
                runtime_id: None,
                essential: def
                    .get("essential")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(true),
                cpu: def
                    .get("cpu")
                    .and_then(|v| v.as_i64())
                    .map(|n| n.to_string()),
                memory: def
                    .get("memory")
                    .and_then(|v| v.as_i64())
                    .map(|n| n.to_string()),
                memory_reservation: def
                    .get("memoryReservation")
                    .and_then(|v| v.as_i64())
                    .map(|n| n.to_string()),
                network_bindings: Vec::new(),
                network_interfaces: Vec::new(),
                health_status: Some("UNKNOWN".into()),
                managed_agents: None,
                image_digest: None,
            })
            .collect();
        let awslogs = container_defs.iter().find_map(|def| {
            let name = def.get("name").and_then(|v| v.as_str())?.to_string();
            let log_cfg = def.get("logConfiguration")?;
            if log_cfg.get("logDriver").and_then(|v| v.as_str()) != Some("awslogs") {
                return None;
            }
            let opts = log_cfg.get("options").and_then(|v| v.as_object())?;
            Some(AwsLogsConfig {
                group: opts.get("awslogs-group").and_then(|v| v.as_str())?.into(),
                stream_prefix: opts
                    .get("awslogs-stream-prefix")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                region: opts
                    .get("awslogs-region")
                    .and_then(|v| v.as_str())
                    .unwrap_or(&state.region)
                    .to_string(),
                container_name: name,
            })
        });
        let task = Task {
            task_arn: task_arn.clone(),
            task_id: task_id.clone(),
            cluster_arn: cluster_arn.clone(),
            cluster_name: cluster_name.clone(),
            task_definition_arn: td_arn.clone(),
            family: family.clone(),
            revision,
            container_instance_arn: None,
            capacity_provider_name: Some(cap_name),
            last_status: "PENDING".into(),
            desired_status: "RUNNING".into(),
            launch_type: launch_type.into(),
            platform_version: Some("1.4.0".into()),
            cpu: cpu.clone(),
            memory: memory.clone(),
            containers,
            overrides: json!({}),
            started_by: Some(daemon_tag.clone()),
            group: Some(format!("daemon:{}", daemon.daemon_name)),
            connectivity: "CONNECTING".into(),
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
            execution_role_arn: exec_role.clone(),
            task_role_arn: task_role.clone(),
            tags: propagated_tags.clone(),
            awslogs,
            captured_logs: String::new(),
            protection: None,
            enable_execute_command: daemon_exec,
            attachments: Vec::new(),
            volume_configurations: Vec::new(),
            task_set_arn: None,
        };
        state.tasks.insert(task_id.clone(), task);
        if let Some(cluster) = state.clusters.get_mut(&cluster_name) {
            cluster.pending_tasks_count += 1;
        }
        ids.push(task_id);
    }
    let _ = principal_arn;
    ids
}

fn parse_daemon_td_arn(arn: &str) -> Option<(String, i32)> {
    let after_slash = arn.rsplit_once('/')?.1;
    let (family, rev_str) = after_slash.rsplit_once(':')?;
    let revision: i32 = rev_str.parse().ok()?;
    Some((family.to_string(), revision))
}

pub(crate) fn recompute_service_counts(
    state: &EcsState,
    service_name: &str,
    cluster_name: &str,
    service_json: &mut Value,
) {
    let service_tag = format!("ecs-svc/{}", service_name);
    let mut running = 0i32;
    let mut pending = 0i32;
    for t in state.tasks.values() {
        if t.started_by.as_deref() == Some(service_tag.as_str()) && t.cluster_name == cluster_name {
            match t.last_status.as_str() {
                "RUNNING" => running += 1,
                "PENDING" | "PROVISIONING" => pending += 1,
                _ => {}
            }
        }
    }
    if let Some(map) = service_json.as_object_mut() {
        map.insert("runningCount".into(), json!(running));
        map.insert("pendingCount".into(), json!(pending));
    }
}

/// True while a service still owns any task that has not reached STOPPED
/// (RUNNING / PENDING / PROVISIONING). Used to decide when a DRAINING
/// (deleted) service has fully drained and may transition to INACTIVE.
pub(crate) fn service_has_live_tasks(
    state: &EcsState,
    service_name: &str,
    cluster_name: &str,
) -> bool {
    let service_tag = format!("ecs-svc/{}", service_name);
    state.tasks.values().any(|t| {
        t.started_by.as_deref() == Some(service_tag.as_str())
            && t.cluster_name == cluster_name
            && matches!(
                t.last_status.as_str(),
                "RUNNING" | "PENDING" | "PROVISIONING"
            )
    })
}

/// Count tasks in a cluster that have not yet reached STOPPED. Authoritative
/// for DeleteCluster's ClusterContainsTasksException check: the cached
/// running/pending counters on the cluster can drift under rapid task churn
/// (e.g. a container that exits immediately may transition PENDING -> STOPPED
/// without a clean RUNNING decrement), so the gate scans live task records
/// instead. STOPPED tasks linger as history and never block cluster deletion.
pub(crate) fn cluster_active_task_count(state: &EcsState, cluster_name: &str) -> usize {
    state
        .tasks
        .values()
        .filter(|t| {
            t.cluster_name == cluster_name
                && matches!(
                    t.last_status.as_str(),
                    "RUNNING" | "PENDING" | "PROVISIONING"
                )
        })
        .count()
}

pub(crate) fn inject_service_task_sets(
    state: &EcsState,
    service_name: &str,
    cluster_name: &str,
    service_json: &mut Value,
) {
    let sets: Vec<Value> = state
        .task_sets
        .values()
        .filter(|ts| ts.service_name == service_name && ts.cluster_name == cluster_name)
        .map(task_set_to_json)
        .collect();
    if let Some(map) = service_json.as_object_mut() {
        map.insert("taskSets".into(), Value::Array(sets));
    }
}

pub(crate) fn service_to_json(svc: &Service) -> Value {
    let mut map = serde_json::Map::new();
    map.insert("serviceArn".into(), json!(svc.service_arn));
    map.insert("serviceName".into(), json!(svc.service_name));
    map.insert("clusterArn".into(), json!(svc.cluster_arn));
    map.insert("status".into(), json!(svc.status));
    map.insert("desiredCount".into(), json!(svc.desired_count));
    map.insert("runningCount".into(), json!(svc.running_count));
    map.insert("pendingCount".into(), json!(svc.pending_count));
    map.insert("launchType".into(), json!(svc.launch_type));
    map.insert("schedulingStrategy".into(), json!(svc.scheduling_strategy));
    map.insert("taskDefinition".into(), json!(svc.task_definition_arn));
    map.insert(
        "deploymentController".into(),
        json!({"type": svc.deployment_controller}),
    );
    let mut deployment_cfg = serde_json::Map::new();
    if let Some(n) = svc.minimum_healthy_percent {
        deployment_cfg.insert("minimumHealthyPercent".into(), json!(n));
    }
    if let Some(n) = svc.maximum_percent {
        deployment_cfg.insert("maximumPercent".into(), json!(n));
    }
    if let Some(ref cb) = svc.circuit_breaker {
        deployment_cfg.insert(
            "deploymentCircuitBreaker".into(),
            json!({"enable": cb.enable, "rollback": cb.rollback}),
        );
    }
    if !deployment_cfg.is_empty() {
        map.insert(
            "deploymentConfiguration".into(),
            Value::Object(deployment_cfg),
        );
    }
    map.insert(
        "deployments".into(),
        Value::Array(svc.deployments.iter().map(deployment_to_json).collect()),
    );
    map.insert(
        "loadBalancers".into(),
        Value::Array(svc.load_balancers.clone()),
    );
    map.insert(
        "serviceRegistries".into(),
        Value::Array(svc.service_registries.clone()),
    );
    map.insert(
        "placementConstraints".into(),
        Value::Array(svc.placement_constraints.clone()),
    );
    map.insert(
        "placementStrategy".into(),
        Value::Array(svc.placement_strategy.clone()),
    );
    if let Some(ref v) = svc.network_configuration {
        map.insert("networkConfiguration".into(), v.clone());
    }
    if let Some(ref v) = svc.role_arn {
        map.insert("roleArn".into(), json!(v));
    }
    if let Some(ref v) = svc.created_by {
        map.insert("createdBy".into(), json!(v));
    }
    map.insert("createdAt".into(), json!(svc.created_at.timestamp()));
    // Always-present fields AWS returns; SDKs deserialize unconditionally.
    map.insert(
        "enableExecuteCommand".into(),
        json!(svc.enable_execute_command),
    );
    map.insert(
        "enableECSManagedTags".into(),
        json!(svc.enable_ecs_managed_tags),
    );
    map.insert(
        "propagateTags".into(),
        json!(svc.propagate_tags.as_deref().unwrap_or("NONE")),
    );
    map.insert(
        "healthCheckGracePeriodSeconds".into(),
        json!(svc.health_check_grace_period_seconds.unwrap_or(0)),
    );
    map.insert(
        "platformVersion".into(),
        json!(svc.platform_version.as_deref().unwrap_or("LATEST")),
    );
    map.insert(
        "platformFamily".into(),
        match svc.launch_type.as_str() {
            "FARGATE" => json!("Linux"),
            _ => Value::Null,
        },
    );
    map.insert(
        "availabilityZoneRebalancing".into(),
        json!(svc
            .availability_zone_rebalancing
            .as_deref()
            .unwrap_or("DISABLED")),
    );
    map.insert(
        "volumeConfigurations".into(),
        Value::Array(svc.volume_configurations.clone()),
    );
    map.insert("taskSets".into(), json!([]));
    map.insert("events".into(), json!([]));
    map.insert(
        "capacityProviderStrategy".into(),
        json!(svc.capacity_provider_strategy),
    );
    if !svc.tags.is_empty() {
        map.insert(
            "tags".into(),
            json!(svc
                .tags
                .iter()
                .map(|t| json!({ "key": t.key, "value": t.value }))
                .collect::<Vec<_>>()),
        );
    }
    Value::Object(map)
}

pub(crate) fn deployment_to_json(d: &Deployment) -> Value {
    json!({
        "id": d.deployment_id,
        "status": d.status,
        "taskDefinition": d.task_definition_arn,
        "desiredCount": d.desired_count,
        "pendingCount": d.pending_count,
        "runningCount": d.running_count,
        "failedTasks": d.failed_tasks,
        "createdAt": d.created_at.timestamp(),
        "updatedAt": d.updated_at.timestamp(),
        "launchType": d.launch_type,
        "rolloutState": d.rollout_state,
        "rolloutStateReason": d.rollout_state_reason,
    })
}

/// Build the `lifecycleHookDetails` array for a service deployment. Surfaces
/// the PAUSE hook the deployment is currently waiting on (if any) so callers
/// can discover the `hookId` to pass to `ContinueServiceDeployment`.
pub(crate) fn lifecycle_hook_details_json(d: &Deployment) -> Value {
    let Some(hook_id) = d.pending_hook_id.as_ref() else {
        return json!([]);
    };
    let pause_hook = d
        .lifecycle_hooks
        .iter()
        .find(|h| h.get("targetType").and_then(|v| v.as_str()) == Some("PAUSE"));
    let target_arn = pause_hook
        .and_then(|h| h.get("hookTargetArn"))
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let timeout_action = pause_hook
        .and_then(|h| h.get("timeoutConfiguration"))
        .and_then(|t| t.get("timeoutAction"))
        .and_then(|v| v.as_str());
    json!([{
        "hookId": hook_id,
        "targetType": "PAUSE",
        "targetArn": target_arn,
        "status": "AWAITING_ACTION",
        "timeoutAction": timeout_action,
    }])
}

/// Error raised when a `serviceDeploymentArn` does not resolve to a known
/// service deployment.
pub(crate) fn service_deployment_not_found(arn: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ServiceDeploymentNotFoundException",
        format!("The service deployment could not be found: {arn}"),
    )
}

pub(crate) fn container_instance_id_from_ref(input: &str) -> String {
    input.rsplit('/').next().unwrap_or(input).to_string()
}

pub(crate) fn container_instance_not_found(input: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ClientException",
        format!("Container instance not found: {input}"),
    )
}

pub(crate) fn capacity_provider_name_from_ref(input: &str) -> String {
    input.rsplit('/').next().unwrap_or(input).to_string()
}

pub(crate) fn capacity_provider_not_found(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ClientException",
        format!("Capacity provider not found: {name}"),
    )
}

pub(crate) fn task_set_id_from_ref(input: &str) -> String {
    input.rsplit('/').next().unwrap_or(input).to_string()
}

pub(crate) fn container_instance_to_json(ci: &ContainerInstance) -> Value {
    json!({
        "containerInstanceArn": ci.container_instance_arn,
        "ec2InstanceId": ci.ec2_instance_id,
        "status": ci.status,
        "version": ci.version,
        "versionInfo": ci.version_info,
        "agentConnected": ci.agent_connected,
        "agentUpdateStatus": ci.agent_update_status,
        "remainingResources": ci.remaining_resources,
        "registeredResources": ci.registered_resources,
        "runningTasksCount": ci.running_tasks_count,
        "pendingTasksCount": ci.pending_tasks_count,
        "registeredAt": ci.registered_at.timestamp(),
        "attributes": ci.attributes.iter().map(|a| json!({
            "name": a.name,
            "value": a.value,
            "targetType": a.target_type,
            "targetId": a.target_id,
        })).collect::<Vec<_>>(),
        "tags": tags_json(&ci.tags),
        "capacityProviderName": ci.capacity_provider_name,
        "healthStatus": ci.health_status,
    })
}

pub(crate) fn capacity_provider_to_json(cp: &CapacityProvider) -> Value {
    json!({
        "name": cp.name,
        "capacityProviderArn": cp.arn,
        "status": cp.status,
        "autoScalingGroupProvider": cp
            .auto_scaling_group_provider
            .as_ref()
            .map(normalize_auto_scaling_group_provider),
        "updateStatus": cp.update_status,
        "updateStatusReason": cp.update_status_reason,
        "tags": tags_json(&cp.tags),
    })
}

/// AWS always echoes the list-valued container-definition fields on the
/// response shape, even when the caller didn't pass them. The Smithy
/// `@examples` for RegisterTaskDefinition rely on this — without the
/// empty arrays the documented-output diff flags the response as a shape
/// mismatch.
/// AWS always echoes `containerOverrides` on a Task's `overrides`, even
/// when the caller didn't pass any — the documented `@examples` for
/// RunTask depend on this shape. Backfill empty arrays when absent so
/// SDKs deserialize a stable structure.
fn normalize_task_overrides(overrides: &Value) -> Value {
    let mut out = match overrides {
        Value::Object(map) => map.clone(),
        _ => serde_json::Map::new(),
    };
    out.entry("containerOverrides".to_string())
        .or_insert_with(|| json!([]));
    out.entry("inferenceAcceleratorOverrides".to_string())
        .or_insert_with(|| json!([]));
    Value::Object(out)
}

fn normalize_container_definition(cd: &Value) -> Value {
    let mut out = match cd {
        Value::Object(map) => map.clone(),
        _ => return cd.clone(),
    };
    for key in [
        "portMappings",
        "environment",
        "environmentFiles",
        "mountPoints",
        "volumesFrom",
        "secrets",
        "dependsOn",
        "extraHosts",
        "ulimits",
        "systemControls",
        "resourceRequirements",
    ] {
        out.entry(key.to_string()).or_insert_with(|| json!([]));
    }
    Value::Object(out)
}

/// Ensure the response shape always includes the documented
/// `autoScalingGroupArn`, `managedScaling`, `managedTerminationProtection`,
/// and `managedDraining` fields with their real-AWS defaults so callers
/// (and the conformance probe) see a stable shape regardless of what the
/// CreateCapacityProvider input omitted.
fn normalize_auto_scaling_group_provider(asg: &Value) -> Value {
    let mut out = match asg {
        Value::Object(map) => map.clone(),
        _ => serde_json::Map::new(),
    };
    out.entry("autoScalingGroupArn".to_string())
        .or_insert_with(|| json!(""));
    let mut ms = match out.get("managedScaling") {
        Some(Value::Object(m)) => m.clone(),
        _ => serde_json::Map::new(),
    };
    ms.entry("status".to_string())
        .or_insert_with(|| json!("DISABLED"));
    ms.entry("targetCapacity".to_string())
        .or_insert_with(|| json!(100));
    ms.entry("minimumScalingStepSize".to_string())
        .or_insert_with(|| json!(1));
    ms.entry("maximumScalingStepSize".to_string())
        .or_insert_with(|| json!(10000));
    ms.entry("instanceWarmupPeriod".to_string())
        .or_insert_with(|| json!(300));
    out.insert("managedScaling".to_string(), Value::Object(ms));
    out.entry("managedTerminationProtection".to_string())
        .or_insert_with(|| json!("DISABLED"));
    out.entry("managedDraining".to_string())
        .or_insert_with(|| json!("DISABLED"));
    Value::Object(out)
}

pub(crate) fn task_set_to_json(ts: &TaskSet) -> Value {
    json!({
        "id": ts.task_set_id,
        "taskSetArn": ts.task_set_arn,
        "serviceArn": ts.service_arn,
        "clusterArn": ts.cluster_arn,
        "externalId": ts.external_id,
        "status": ts.status,
        "taskDefinition": ts.task_definition,
        "computedDesiredCount": ts.computed_desired_count,
        "pendingCount": ts.pending_count,
        "runningCount": ts.running_count,
        "launchType": ts.launch_type,
        "platformVersion": ts.platform_version,
        "scale": ts.scale,
        "stabilityStatus": ts.stability_status,
        "createdAt": ts.created_at.timestamp(),
        "updatedAt": ts.updated_at.timestamp(),
        "loadBalancers": ts.load_balancers,
        "serviceRegistries": ts.service_registries,
        "capacityProviderStrategy": ts.capacity_provider_strategy,
        "tags": tags_json(&ts.tags),
    })
}

pub(crate) fn task_protection_json(task: &Task) -> Value {
    let p = task.protection.as_ref();
    json!({
        "taskArn": task.task_arn,
        "protectionEnabled": p.map(|p| p.enabled).unwrap_or(false),
        "expirationDate": p.and_then(|p| p.expiration).map(|e| e.timestamp()),
    })
}

#[cfg(test)]
mod pagination_tests {
    use super::{paginate_arns, paginate_arns_dir};

    #[test]
    fn paginate_arns_dir_descending_pages_in_reverse() {
        let all: Vec<String> = (0..5).map(|i| format!("arn:{i}")).collect();
        // DESC page 1: highest first.
        let (p1, next) = paginate_arns_dir(all.clone(), "", 2, true);
        assert_eq!(p1, vec!["arn:4", "arn:3"]);
        let token = next.expect("nextToken after DESC page 1");
        // DESC page 2 continues downward, not silently re-sorted to ASC.
        let (p2, next2) = paginate_arns_dir(all.clone(), &token, 2, true);
        assert_eq!(p2, vec!["arn:2", "arn:1"]);
        let (p3, _) = paginate_arns_dir(all, &next2.unwrap(), 2, true);
        assert_eq!(p3, vec!["arn:0"]);
    }

    #[test]
    fn paginate_arns_key_cursor_is_stable_across_delete() {
        let all: Vec<String> = (0..5).map(|i| format!("arn:{i}")).collect();
        let (p1, next) = paginate_arns(all.clone(), "", 2);
        assert_eq!(p1, vec!["arn:0", "arn:1"]);
        let token = next.expect("nextToken after page 1");

        // Delete the first item before fetching page 2. A numeric offset (2)
        // would skip "arn:2"; the key cursor resumes correctly after "arn:1".
        let after: Vec<String> = all.iter().filter(|a| *a != "arn:0").cloned().collect();
        let (p2, next2) = paginate_arns(after, &token, 2);
        assert_eq!(p2, vec!["arn:2", "arn:3"]);
        assert!(next2.is_some());
    }

    #[test]
    fn paginate_arns_sorts_and_dedups_and_tolerates_garbage_token() {
        let dupes = vec![
            "b".to_string(),
            "a".to_string(),
            "a".to_string(),
            "c".to_string(),
        ];
        let (page, _) = paginate_arns(dupes, "", 10);
        assert_eq!(page, vec!["a", "b", "c"]);
        // A non-empty, undecodable token is treated as past the end: empty
        // page + no nextToken, never a silent restart at page 1.
        let (page, next) = paginate_arns(vec!["a".into(), "b".into()], "!!!not-base64", 1);
        assert!(page.is_empty());
        assert!(next.is_none());
        // An empty token still starts at the beginning.
        let (page, _) = paginate_arns(vec!["a".into(), "b".into()], "", 1);
        assert_eq!(page, vec!["a"]);
    }
}
