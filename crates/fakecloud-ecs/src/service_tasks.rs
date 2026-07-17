// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use chrono::Utc;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

/// Extract the region field (index 3) from a standard ARN
/// (`arn:partition:service:region:account:resource`). Returns `None` when
/// the string isn't an ARN or the region segment is empty.
fn region_from_arn(arn: &str) -> Option<&str> {
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    if parts.len() >= 4 && parts[0] == "arn" && !parts[3].is_empty() {
        Some(parts[3])
    } else {
        None
    }
}

impl EcsService {
    /// Spawn a task from a cross-service caller (EventBridge Scheduler /
    /// EventBridge Rules) without going through the AwsRequest dispatch
    /// path. Builds the JSON body and reuses [`Self::run_task`] so all
    /// the existing validation / runtime spawn logic runs identically.
    /// Returns Err with a human-readable message on validation failures —
    /// the caller decides whether to surface the failure (e.g. DLQ).
    pub fn run_task_external(
        &self,
        account_id: &str,
        cluster: &str,
        task_definition: &str,
        launch_type: Option<&str>,
        count: usize,
    ) -> Result<(), String> {
        use bytes::Bytes;
        use http::{HeaderMap, Method};
        use std::collections::HashMap;
        let body = serde_json::json!({
            "cluster": cluster,
            "taskDefinition": task_definition,
            "launchType": launch_type.unwrap_or("FARGATE"),
            "count": count.max(1) as i64,
        });
        let body_bytes =
            Bytes::from(serde_json::to_vec(&body).map_err(|e| format!("encode body: {e}"))?);
        // Derive the region from the task-definition or cluster ARN (EventBridge
        // targets pass full ARNs) so the spawned task's ARNs carry the caller's
        // region instead of a hardcoded us-east-1.
        let region = region_from_arn(task_definition)
            .or_else(|| region_from_arn(cluster))
            .unwrap_or("us-east-1")
            .to_string();
        let req = AwsRequest {
            service: "ecs".into(),
            action: "RunTask".into(),
            region,
            account_id: account_id.to_string(),
            request_id: uuid::Uuid::new_v4().to_string(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: body_bytes,
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".into(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        };
        self.run_task(&req)
            .map(|_| ())
            .map_err(|e| format!("{e:?}"))
    }

    pub fn run_task(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let td_ref = req_str(&body, "taskDefinition")?;
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let launch_type = opt_str(&body, "launchType")
            .unwrap_or("FARGATE")
            .to_string();
        let placement_constraints: Vec<Value> = body
            .get("placementConstraints")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let placement_strategy: Vec<Value> = body
            .get("placementStrategy")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        // AWS caps RunTask at 1..=10 tasks per call and rejects anything else
        // with InvalidParameterException — it does NOT silently clamp to 1.
        let count = match body.get("count").and_then(|v| v.as_i64()) {
            Some(n) if (1..=10).contains(&n) => n as usize,
            Some(n) => {
                return Err(invalid_parameter(format!(
                    "count should be between 1 and 10, inclusive. count={n}"
                )))
            }
            None => 1,
        };
        // Defaulted to `family:<taskDefinitionFamily>` below once the task
        // definition is resolved, mirroring how real AWS labels a bare RunTask.
        let group = opt_str(&body, "group").map(String::from);
        let started_by = opt_str(&body, "startedBy").map(String::from);
        let enable_execute_command = body
            .get("enableExecuteCommand")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let propagate_tags = opt_str(&body, "propagateTags").map(String::from);
        let _enable_ecs_managed_tags = body
            .get("enableECSManagedTags")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let _capacity_provider_strategy: Vec<Value> = body
            .get("capacityProviderStrategy")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let volume_configurations: Vec<Value> = body
            .get("volumeConfigurations")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let _availability_zone_rebalancing =
            opt_str(&body, "availabilityZoneRebalancing").map(String::from);
        let mut tags = parse_tags(&body);

        // PassRole trust check on any role overrides supplied via the
        // overrides.taskRoleArn / overrides.executionRoleArn fields.
        // The base task definition was already checked at Register time,
        // but RunTask can override either role and AWS re-validates the
        // trust policy on every call.
        if let Some(overrides) = body.get("overrides") {
            if let Some(role_arn) = opt_str(overrides, "taskRoleArn") {
                self.check_pass_role(&request.account_id, role_arn)?;
            }
            if let Some(role_arn) = opt_str(overrides, "executionRoleArn") {
                self.check_pass_role(&request.account_id, role_arn)?;
            }
        }

        let account = request.account_id.clone();
        let runtime = self.runtime.clone();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        // AWS rejects RunTask against a cluster that does not exist rather than
        // synthesizing one; a phantom cluster returns ClusterNotFoundException.
        let cluster_arn = state
            .clusters
            .get(&cluster_name)
            .map(|c| c.cluster_arn.clone())
            .ok_or_else(|| cluster_not_found(&cluster_name))?;
        let (_, family, rev) = resolve_task_definition_ref(td_ref)?;
        let revisions = state
            .task_definitions
            .get(&family)
            .ok_or_else(|| task_definition_not_found(td_ref))?;
        let td = match rev {
            Some(n) => revisions
                .get(&n)
                .ok_or_else(|| task_definition_not_found(td_ref))?,
            None => latest_active_revision(revisions)
                .ok_or_else(|| task_definition_not_found(td_ref))?,
        };
        if td.status != "ACTIVE" {
            return Err(client_exception(format!(
                "Task definition {} is not ACTIVE",
                td.task_definition_arn
            )));
        }
        let td_arn = td.task_definition_arn.clone();
        let td_family = td.family.clone();
        // Real AWS assigns a bare RunTask (no explicit `group`) the default
        // group `family:<taskDefinitionFamily>`. Service/daemon tasks set their
        // own group upstream, so only default it when the caller omitted one.
        let group = group.or_else(|| Some(format!("family:{td_family}")));
        let td_revision = td.revision;
        let td_cpu = td.cpu.clone();
        let td_memory = td.memory.clone();
        let td_task_role = td.task_role_arn.clone();
        let td_exec_role = td.execution_role_arn.clone();
        let td_containers = td.container_definitions.clone();
        // RunTask supports propagateTags=TASK_DEFINITION to copy the
        // TaskDefinition's tags onto each spawned task, in addition to
        // any tags supplied directly in the request body. Real AWS
        // unions the two sets; explicit tags win on key conflicts.
        if propagate_tags.as_deref() == Some("TASK_DEFINITION") {
            let mut td_tags = td.tags.clone();
            td_tags.retain(|t| !tags.iter().any(|x| x.key == t.key));
            tags.extend(td_tags);
        }

        let mut spawned_tasks: Vec<String> = Vec::new();
        let mut task_jsons: Vec<Value> = Vec::new();
        for _ in 0..count {
            let task_id = uuid::Uuid::new_v4().to_string().replace('-', "");
            let task_arn = state.task_arn(&cluster_name, &task_id);
            let containers: Vec<Container> = td_containers
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
                    health_status: Some("UNKNOWN".to_string()),
                    managed_agents: None,
                    image_digest: None,
                })
                .collect();
            let awslogs = td_containers.iter().find_map(|def| {
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
            let capacity_provider_name = body
                .get("capacityProviderStrategy")
                .and_then(|v| v.as_array())
                .and_then(|arr| arr.first())
                .and_then(|item| item.get("capacityProvider"))
                .and_then(|v| v.as_str())
                .map(String::from);
            let mut task = Task {
                task_arn: task_arn.clone(),
                task_id: task_id.clone(),
                cluster_arn: cluster_arn.clone(),
                cluster_name: cluster_name.clone(),
                task_definition_arn: td_arn.clone(),
                family: td_family.clone(),
                revision: td_revision,
                container_instance_arn: None,
                capacity_provider_name,
                last_status: "PROVISIONING".into(),
                desired_status: "RUNNING".into(),
                launch_type: launch_type.clone(),
                platform_version: Some("1.4.0".into()),
                cpu: body
                    .get("overrides")
                    .and_then(|v| v.get("cpu"))
                    .and_then(|v| v.as_str())
                    .map(String::from)
                    .or_else(|| td_cpu.clone()),
                memory: body
                    .get("overrides")
                    .and_then(|v| v.get("memory"))
                    .and_then(|v| v.as_str())
                    .map(String::from)
                    .or_else(|| td_memory.clone()),
                containers,
                overrides: body.get("overrides").cloned().unwrap_or_else(|| json!({})),
                started_by: started_by.clone(),
                group: group.clone(),
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
                execution_role_arn: td_exec_role.clone(),
                task_role_arn: td_task_role.clone(),
                tags: tags.clone(),
                awslogs,
                captured_logs: String::new(),
                protection: None,
                enable_execute_command,
                attachments: Vec::new(),
                volume_configurations: volume_configurations.clone(),
                task_set_arn: None,
            };
            // Best-effort placement for EC2 / EXTERNAL launch types.
            if launch_type != "FARGATE" {
                if let Some(arn) = crate::placement::select_container_instance(
                    state,
                    &cluster_name,
                    &placement_constraints,
                    &placement_strategy,
                    task.group.as_deref(),
                    &td_arn,
                    &launch_type,
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
            state.tasks.insert(task_id.clone(), task.clone());
            if let Some(cluster) = state.clusters.get_mut(&cluster_name) {
                cluster.pending_tasks_count += 1;
            }
            // Snapshot-in-progress: transition to PENDING synchronously so
            // callers that immediately DescribeTasks see movement. RUNNING /
            // STOPPED come later from the background runtime task.
            if let Some(t) = state.tasks.get_mut(&task_id) {
                t.last_status = "PENDING".into();
            }
            task_jsons.push(task_to_json(&task));
            spawned_tasks.push(task_id.clone());
        }
        drop(accounts);

        // Launch container execution outside the state lock.
        if let Some(rt) = runtime {
            for id in &spawned_tasks {
                rt.clone()
                    .run_task(self.state.clone(), id.clone(), account.clone());
            }
        } else {
            // No runtime available — fail fast so the task doesn't stay
            // PENDING forever. We incremented pending_tasks_count above;
            // decrement it here so the cluster counter doesn't drift and
            // block later DeleteCluster calls.
            let mut accounts = self.state.write();
            if let Some(state) = accounts.get_mut(&account) {
                // (cluster_name, container_instance_arn) for each failed task so
                // BOTH pending counters that RunTask incremented get drained —
                // previously only the cluster counter was decremented, leaking
                // the container-instance pendingTasksCount for EC2/EXTERNAL tasks.
                let mut drains: Vec<(String, Option<String>)> = Vec::new();
                for id in &spawned_tasks {
                    if let Some(t) = state.tasks.get_mut(id) {
                        t.last_status = "STOPPED".into();
                        // desired_status stays RUNNING: the task was requested
                        // to run but failed to start. AWS keeps desired_status
                        // as RUNNING for failed standalone RunTask until the
                        // caller explicitly stops it. This also ensures
                        // list_tasks (default filter=RUNNING) finds it.
                        t.stop_code = Some("TaskFailedToStart".into());
                        t.stopped_reason = Some(format!(
                            "No container runtime available (docker/podman not installed). {}",
                            fakecloud_core::container_net::CONTAINER_RUNTIME_HINT
                        ));
                        t.stopped_at = Some(Utc::now());
                        for c in t.containers.iter_mut() {
                            c.last_status = "STOPPED".into();
                        }
                        drains.push((t.cluster_name.clone(), t.container_instance_arn.clone()));
                    }
                }
                for (name, ci_arn) in drains {
                    if let Some(cluster) = state.clusters.get_mut(&name) {
                        if cluster.pending_tasks_count > 0 {
                            cluster.pending_tasks_count -= 1;
                        }
                    }
                    if let Some(arn) = ci_arn {
                        if let Some(ci) = state
                            .container_instances
                            .values_mut()
                            .find(|ci| ci.container_instance_arn == arn)
                        {
                            if ci.pending_tasks_count > 0 {
                                ci.pending_tasks_count -= 1;
                            }
                        }
                    }
                }
            }
        }

        Ok(AwsResponse::ok_json(json!({
            "tasks": task_jsons,
            "failures": [],
        })))
    }

    pub(super) fn start_task(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // StartTask targets explicit container instances. Our ECS emulator
        // has no concept of registered container instances yet (Batch 4);
        // fall through to the same semantics as RunTask so the API is
        // usable while the container-instance surface is pending.
        self.run_task(request)
    }

    pub(super) async fn stop_task(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let task_ref = req_str(&body, "task")?;
        let reason = opt_str(&body, "reason")
            .unwrap_or("UserInitiated")
            .to_string();
        let cluster_ref = opt_str(&body, "cluster");
        let _cluster_name = EcsState::resolve_cluster_name(cluster_ref);

        let (task_id, account, task_snapshot) = {
            let account = request.account_id.clone();
            let mut accounts = self.state.write();
            let state = accounts
                .get_mut(&account)
                .ok_or_else(|| task_not_found(task_ref))?;
            let task_id = resolve_task_id(state, task_ref)?;
            let task = state
                .tasks
                .get_mut(&task_id)
                .ok_or_else(|| task_not_found(task_ref))?;
            task.desired_status = "STOPPED".into();
            task.stopping_at = Some(Utc::now());
            task.stopped_reason = Some(reason.clone());
            task.stop_code = Some("UserInitiated".into());
            (task_id, account, task.clone())
        };
        if let Some(rt) = &self.runtime {
            rt.stop_task(&task_id, &reason).await;
        }
        let _ = account;
        Ok(AwsResponse::ok_json(json!({
            "task": task_to_json(&task_snapshot),
        })))
    }

    pub(super) fn describe_tasks(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let refs: Vec<String> = req_array(&body, "tasks")?
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        let include_tags = body
            .get("include")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().any(|v| v.as_str() == Some("TAGS")))
            .unwrap_or(false);
        // AWS scopes DescribeTasks to the given cluster (default "default"):
        // a task that exists in another cluster is reported MISSING, not found.
        let cluster_name = EcsState::resolve_cluster_name(opt_str(&body, "cluster"));

        let account = request.account_id.clone();
        let accounts = self.state.read();
        let Some(state) = accounts.get(&account) else {
            return Ok(AwsResponse::ok_json(json!({
                "tasks": [],
                "failures": refs.iter().map(|r| json!({"arn": r, "reason": "MISSING"})).collect::<Vec<_>>(),
            })));
        };
        let mut found = Vec::new();
        let mut failures = Vec::new();
        for input in &refs {
            let task_id = task_id_from_ref(input);
            match state.tasks.get(&task_id) {
                Some(t) if t.cluster_name == cluster_name => {
                    let mut v = task_to_json(t);
                    if include_tags {
                        v.as_object_mut()
                            .unwrap()
                            .insert("tags".into(), tags_json(&t.tags));
                    }
                    found.push(v);
                }
                _ => {
                    failures.push(json!({
                        "arn": input,
                        "reason": "MISSING",
                    }));
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "tasks": found,
            "failures": failures,
        })))
    }

    pub(super) fn list_tasks(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_enum_opt(&body, "desiredStatus", &["RUNNING", "PENDING", "STOPPED"])?;
        validate_enum_opt(
            &body,
            "launchType",
            &["EC2", "FARGATE", "EXTERNAL", "MANAGED_INSTANCES"],
        )?;
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let family = opt_str(&body, "family");
        let status_filter = opt_str(&body, "desiredStatus").or(Some("RUNNING"));
        let started_by = opt_str(&body, "startedBy");
        // A service's tasks carry group `service:<name>`. The serviceName
        // filter may arrive as a bare name or a full service ARN.
        let service_group =
            opt_str(&body, "serviceName").map(|s| format!("service:{}", service_name_from_ref(s)));
        // containerInstance may be a full ARN or a bare instance ID; compare on
        // the trailing ID segment so both forms match.
        let container_instance = opt_str(&body, "containerInstance").map(id_from_ref);
        let max_results = body
            .get("maxResults")
            .and_then(|v| v.as_i64())
            .filter(|n| (1..=100).contains(n))
            .map(|n| n as usize)
            .unwrap_or(100);
        let next_token = opt_str(&body, "nextToken").unwrap_or("");

        let account = request.account_id.clone();
        let accounts = self.state.read();
        let mut arns: Vec<String> = match accounts.get(&account) {
            Some(state) => state
                .tasks
                .values()
                .filter(|t| t.cluster_name == cluster_name)
                .filter(|t| family.is_none_or(|f| t.family == f))
                .filter(|t| status_filter.is_none_or(|s| t.desired_status == s))
                .filter(|t| started_by.is_none_or(|s| t.started_by.as_deref() == Some(s)))
                .filter(|t| {
                    service_group
                        .as_deref()
                        .is_none_or(|g| t.group.as_deref() == Some(g))
                })
                .filter(|t| {
                    container_instance.as_deref().is_none_or(|ci| {
                        t.container_instance_arn
                            .as_deref()
                            .map(id_from_ref)
                            .as_deref()
                            == Some(ci)
                    })
                })
                .map(|t| t.task_arn.clone())
                .collect(),
            None => Vec::new(),
        };
        arns.sort();
        let (page, next) = paginate_arns(arns, next_token, max_results);
        let mut out = json!({"taskArns": page});
        if let Some(n) = next {
            out.as_object_mut()
                .unwrap()
                .insert("nextToken".into(), json!(n));
        }
        Ok(AwsResponse::ok_json(out))
    }
}

#[cfg(test)]
mod multi_container_tests {
    use super::*;
    use crate::EcsService;
    use bytes::Bytes;
    use fakecloud_core::multi_account::MultiAccountState;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    pub(super) fn fresh_service() -> EcsService {
        let accounts: MultiAccountState<EcsState> =
            MultiAccountState::new("000000000000", "us-east-1", "http://localhost:4566");
        let state = Arc::new(RwLock::new(accounts));
        let svc = EcsService::new(state.clone());
        // Pre-create the cluster so RunTask doesn't trip on a missing one.
        let mut accounts = state.write();
        let s = accounts.get_or_create("000000000000");
        let arn = s.cluster_arn("default");
        s.clusters
            .insert("default".into(), Cluster::new("default", arn));
        drop(accounts);
        svc
    }

    pub(super) fn make_request(action: &str, body: Value) -> AwsRequest {
        let body_bytes = Bytes::from(serde_json::to_vec(&body).unwrap());
        AwsRequest {
            service: "ecs".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "000000000000".into(),
            request_id: uuid::Uuid::new_v4().to_string(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: body_bytes,
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".into(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    #[test]
    fn run_task_count_over_ten_is_invalid_parameter() {
        let svc = fresh_service();
        // count is validated before the cluster/task-def are dereferenced.
        let err = match svc.run_task(&make_request(
            "RunTask",
            json!({"taskDefinition": "x", "count": 50}),
        )) {
            Ok(_) => panic!("expected InvalidParameterException for count > 10"),
            Err(e) => e,
        };
        assert_eq!(err.code(), "InvalidParameterException");
    }

    #[test]
    fn create_service_negative_desired_count_is_invalid_parameter() {
        let svc = fresh_service();
        // desiredCount is validated before the cluster/task-def are resolved.
        let err = match svc.create_service(&make_request(
            "CreateService",
            json!({"serviceName": "s", "taskDefinition": "x", "desiredCount": -3}),
        )) {
            Ok(_) => panic!("expected InvalidParameterException for negative desiredCount"),
            Err(e) => e,
        };
        assert_eq!(err.code(), "InvalidParameterException");
    }

    #[test]
    fn run_task_on_missing_cluster_is_cluster_not_found() {
        let svc = fresh_service();
        let err = match svc.run_task(&make_request(
            "RunTask",
            json!({"taskDefinition": "x", "cluster": "ghost", "count": 1}),
        )) {
            Ok(_) => panic!("expected ClusterNotFoundException for a phantom cluster"),
            Err(e) => e,
        };
        assert_eq!(err.code(), "ClusterNotFoundException");
    }

    #[test]
    fn region_from_arn_extracts_region_or_none() {
        assert_eq!(
            region_from_arn("arn:aws:ecs:eu-west-1:000000000000:task-definition/web:3"),
            Some("eu-west-1")
        );
        assert_eq!(
            region_from_arn("arn:aws:ecs:ap-southeast-2:000000000000:cluster/prod"),
            Some("ap-southeast-2")
        );
        // Bare names (not ARNs) and empty-region ARNs yield None.
        assert_eq!(region_from_arn("web:3"), None);
        assert_eq!(
            region_from_arn("arn:aws:ecs::000000000000:cluster/prod"),
            None
        );
    }

    pub(super) fn insert_task(svc: &EcsService, id: &str, cluster: &str) {
        let state = svc.state.clone();
        let mut accounts = state.write();
        let acct = accounts.get_or_create("000000000000");
        let task = Task {
            task_arn: format!("arn:aws:ecs:us-east-1:000000000000:task/{cluster}/{id}"),
            task_id: id.into(),
            cluster_arn: format!("arn:aws:ecs:us-east-1:000000000000:cluster/{cluster}"),
            cluster_name: cluster.into(),
            task_definition_arn: "arn:aws:ecs:us-east-1:000000000000:task-definition/web:1".into(),
            family: "web".into(),
            revision: 1,
            container_instance_arn: None,
            capacity_provider_name: None,
            last_status: "RUNNING".into(),
            desired_status: "RUNNING".into(),
            launch_type: "FARGATE".into(),
            platform_version: None,
            cpu: None,
            memory: None,
            containers: Vec::new(),
            overrides: serde_json::json!({}),
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
        };
        acct.tasks.insert(id.into(), task);
    }

    #[test]
    fn describe_tasks_scopes_to_requested_cluster() {
        let svc = fresh_service();
        insert_task(&svc, "abc", "other");
        let task_arn = "arn:aws:ecs:us-east-1:000000000000:task/other/abc";

        // Wrong cluster (default) -> MISSING, not found.
        let resp = svc
            .describe_tasks(&make_request(
                "DescribeTasks",
                json!({"tasks": [task_arn], "cluster": "default"}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["tasks"].as_array().unwrap().len(), 0);
        assert_eq!(body["failures"][0]["reason"], "MISSING");

        // Correct cluster -> found.
        let resp = svc
            .describe_tasks(&make_request(
                "DescribeTasks",
                json!({"tasks": [task_arn], "cluster": "other"}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["tasks"].as_array().unwrap().len(), 1);
        assert_eq!(body["failures"].as_array().unwrap().len(), 0);
    }

    #[test]
    fn describe_service_revisions_returns_stored_revision_or_missing() {
        let svc = fresh_service();
        svc.register_task_definition(&make_request(
            "RegisterTaskDefinition",
            json!({"family": "web", "containerDefinitions": [{"name": "app", "image": "alpine"}]}),
        ))
        .unwrap();
        let created = svc
            .create_service(&make_request(
                "CreateService",
                json!({"serviceName": "s", "taskDefinition": "web", "desiredCount": 0}),
            ))
            .unwrap();
        let cbody: Value = serde_json::from_slice(created.body.expect_bytes()).unwrap();
        let service_arn = cbody["service"]["serviceArn"].as_str().unwrap().to_string();

        // CreateService minted revision 1.
        let rev1 = format!("{service_arn}:1");
        let d = svc
            .describe_service_revisions(&make_request(
                "DescribeServiceRevisions",
                json!({"serviceRevisionArns": [rev1.clone()]}),
            ))
            .unwrap();
        let dbody: Value = serde_json::from_slice(d.body.expect_bytes()).unwrap();
        assert_eq!(dbody["serviceRevisions"].as_array().unwrap().len(), 1);
        assert_eq!(
            dbody["serviceRevisions"][0]["serviceRevisionArn"],
            json!(rev1)
        );
        assert!(dbody["serviceRevisions"][0]["taskDefinition"]
            .as_str()
            .unwrap()
            .ends_with("web:1"));
        assert_eq!(dbody["failures"].as_array().unwrap().len(), 0);

        // Unknown revision -> MISSING.
        let d2 = svc
            .describe_service_revisions(&make_request(
                "DescribeServiceRevisions",
                json!({"serviceRevisionArns": [format!("{service_arn}:99")]}),
            ))
            .unwrap();
        let d2body: Value = serde_json::from_slice(d2.body.expect_bytes()).unwrap();
        assert_eq!(d2body["serviceRevisions"].as_array().unwrap().len(), 0);
        assert_eq!(d2body["failures"][0]["reason"], "MISSING");
    }

    #[test]
    fn register_task_def_with_two_containers_then_run_task_starts_both() {
        let svc = fresh_service();
        let reg = make_request(
            "RegisterTaskDefinition",
            json!({
                "family": "multi",
                "containerDefinitions": [
                    {"name": "app", "image": "alpine"},
                    {"name": "sidecar", "image": "alpine"}
                ]
            }),
        );
        svc.register_task_definition(&reg)
            .expect("register should succeed");

        let run = make_request(
            "RunTask",
            json!({
                "cluster": "default",
                "taskDefinition": "multi",
            }),
        );
        let resp = svc.run_task(&run).expect("run_task should succeed");
        let body: Value =
            serde_json::from_slice(resp.body.expect_bytes()).expect("body should be valid JSON");
        let tasks = body
            .get("tasks")
            .and_then(|v| v.as_array())
            .expect("tasks array");
        assert_eq!(tasks.len(), 1);
        let task = &tasks[0];
        let containers = task
            .get("containers")
            .and_then(|v| v.as_array())
            .expect("containers array on task");
        assert_eq!(containers.len(), 2, "expected both containers in task");
        let names: Vec<&str> = containers
            .iter()
            .filter_map(|c| c.get("name").and_then(|v| v.as_str()))
            .collect();
        assert!(names.contains(&"app"));
        assert!(names.contains(&"sidecar"));

        // Per-container ARNs must be distinct so DescribeTasks can address
        // each container independently.
        let arns: std::collections::HashSet<&str> = containers
            .iter()
            .filter_map(|c| c.get("containerArn").and_then(|v| v.as_str()))
            .collect();
        assert_eq!(arns.len(), 2);
    }

    #[test]
    fn run_task_defaults_group_to_family() {
        // A bare RunTask (no `group`) gets `family:<taskDefinitionFamily>`,
        // matching real AWS. eventing/consumers filter tasks by group, so a
        // null group hides RunTask-spawned tasks from them.
        let svc = fresh_service();
        svc.register_task_definition(&make_request(
            "RegisterTaskDefinition",
            json!({"family": "web", "containerDefinitions": [{"name": "app", "image": "alpine"}]}),
        ))
        .unwrap();

        let resp = svc
            .run_task(&make_request(
                "RunTask",
                json!({"cluster": "default", "taskDefinition": "web"}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["tasks"][0]["group"], "family:web");
    }

    #[test]
    fn run_task_preserves_explicit_group() {
        let svc = fresh_service();
        svc.register_task_definition(&make_request(
            "RegisterTaskDefinition",
            json!({"family": "web", "containerDefinitions": [{"name": "app", "image": "alpine"}]}),
        ))
        .unwrap();

        let resp = svc
            .run_task(&make_request(
                "RunTask",
                json!({"cluster": "default", "taskDefinition": "web", "group": "my-group"}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(body["tasks"][0]["group"], "my-group");
    }

    #[test]
    fn register_task_def_defaults_essential_true() {
        let svc = fresh_service();
        let reg = make_request(
            "RegisterTaskDefinition",
            json!({
                "family": "default-essential",
                // No `essential` declared on either container.
                "containerDefinitions": [
                    {"name": "main", "image": "alpine"},
                    {"name": "extra", "image": "alpine"}
                ]
            }),
        );
        svc.register_task_definition(&reg).unwrap();

        let run = make_request(
            "RunTask",
            json!({
                "cluster": "default",
                "taskDefinition": "default-essential",
            }),
        );
        let resp = svc.run_task(&run).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let containers = body["tasks"][0]["containers"].as_array().unwrap();
        // The `essential` flag is a TaskDefinition.ContainerDefinition
        // field, not part of the runtime `Container` response shape.
        // Real ECS doesn't echo it back on RunTask, and the conformance
        // probe rejects responses that do.
        for c in containers {
            assert!(
                c.get("essential").is_none(),
                "container {:?} must not carry `essential` on the runtime shape",
                c.get("name")
            );
        }
    }

    #[test]
    fn task_to_json_emits_full_container_array() {
        // Build a Task with two containers directly and confirm helper
        // emits both entries in the response shape.
        let mut task = Task {
            task_arn: "arn:aws:ecs:us-east-1:000000000000:task/default/abc".into(),
            task_id: "abc".into(),
            cluster_arn: "arn:aws:ecs:us-east-1:000000000000:cluster/default".into(),
            cluster_name: "default".into(),
            task_definition_arn: "arn:aws:ecs:us-east-1:000000000000:task-definition/multi:1"
                .into(),
            family: "multi".into(),
            revision: 1,
            container_instance_arn: None,
            capacity_provider_name: None,
            last_status: "RUNNING".into(),
            desired_status: "RUNNING".into(),
            launch_type: "FARGATE".into(),
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
            created_at: chrono::Utc::now(),
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
        };
        for name in ["app", "sidecar"] {
            task.containers.push(Container {
                container_arn: format!(
                    "arn:aws:ecs:us-east-1:000000000000:container/default/abc/{name}"
                ),
                name: name.into(),
                image: "alpine".into(),
                task_arn: task.task_arn.clone(),
                last_status: "RUNNING".into(),
                exit_code: None,
                reason: None,
                runtime_id: Some(format!("docker-{name}")),
                essential: true,
                cpu: None,
                memory: None,
                memory_reservation: None,
                network_bindings: Vec::new(),
                network_interfaces: Vec::new(),
                health_status: None,
                managed_agents: None,
                image_digest: None,
            });
        }

        let v = task_to_json(&task);
        let containers = v
            .get("containers")
            .and_then(|v| v.as_array())
            .expect("containers array");
        assert_eq!(containers.len(), 2);
        let names: Vec<&str> = containers
            .iter()
            .filter_map(|c| c.get("name").and_then(|v| v.as_str()))
            .collect();
        assert_eq!(names, vec!["app", "sidecar"]);
        for c in containers {
            assert!(c.get("containerArn").is_some());
            assert!(c.get("name").is_some());
            assert!(c.get("lastStatus").is_some());
            assert!(c.get("runtimeId").is_some());
            assert!(c.get("essential").is_none());
        }
    }
}

#[cfg(test)]
mod port_mapping_tests {
    //! Cover the `portMappings` -> `docker run --publish` translation
    //! plus the per-container `networkBindings` projected onto the task.
    //! The argv builder is exercised directly so we don't need a real
    //! container CLI on the test host.
    use super::*;
    use crate::runtime::{
        build_run_argv, mark_running_multi, ContainerPlan, PortMapping, RunningContainer,
    };
    use crate::state::{Container, EcsState};
    use crate::SharedEcsState;
    use chrono::Utc;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::sync::Arc;

    fn plan_with_ports(
        port_mappings: Vec<PortMapping>,
        network_mode: Option<&str>,
    ) -> ContainerPlan {
        ContainerPlan {
            container_name: "app".into(),
            image: "alpine:latest".into(),
            env: Vec::new(),
            entry_point: Vec::new(),
            command: Vec::new(),
            secrets_refs: Vec::new(),
            essential: true,
            has_task_role: false,
            port_mappings,
            network_mode: network_mode.map(String::from),
            depends_on: Vec::new(),
            health_check: None,
            volume_mounts: Vec::new(),
            ulimits: Vec::new(),
            linux_parameters: None,
            stop_timeout: None,
            user: None,
            working_directory: None,
            tty: false,
            interactive: false,
            readonly_rootfs: false,
        }
    }

    fn argv_string(plan: &ContainerPlan) -> Vec<String> {
        build_run_argv(
            plan,
            &[],
            "task-1",
            "host.docker.internal",
            None,
            "alpine:latest",
            true,
        )
    }

    /// Helper for asserting a `--publish <spec>` pair is present in argv.
    /// Returns true when the flag/value pair appears as adjacent entries.
    fn argv_has_publish(argv: &[String], spec: &str) -> bool {
        argv.windows(2).any(|w| w[0] == "--publish" && w[1] == spec)
    }

    #[test]
    fn port_mappings_translate_to_publish_flags() {
        let plan = plan_with_ports(
            vec![PortMapping {
                container_port: 80,
                host_port: 8080,
                protocol: "tcp".into(),
            }],
            None,
        );
        let argv = argv_string(&plan);
        assert!(
            argv_has_publish(&argv, "80:8080/tcp"),
            "expected --publish 80:8080/tcp in argv: {argv:?}"
        );
    }

    #[test]
    fn port_mappings_default_host_port_to_container_port() {
        // host_port=0 in the parsed mapping means "AWS host-mode default";
        // parse_port_mapping rewrites that to containerPort, so by the time
        // we reach build_run_argv the host_port should already equal 80.
        // Drive the same path through the JSON parser to lock in the
        // default behaviour end to end.
        let parsed =
            crate::runtime::__test_parse_port_mapping(&serde_json::json!({"containerPort": 80}))
                .expect("containerPort should parse");
        assert_eq!(
            parsed.host_port, 80,
            "default hostPort should mirror containerPort"
        );
        let argv = argv_string(&plan_with_ports(vec![parsed], None));
        assert!(
            argv_has_publish(&argv, "80:80/tcp"),
            "expected --publish 80:80/tcp when hostPort omitted: {argv:?}"
        );
    }

    #[test]
    fn port_mappings_default_protocol_tcp() {
        let parsed = crate::runtime::__test_parse_port_mapping(
            &serde_json::json!({"containerPort": 443, "hostPort": 443}),
        )
        .expect("containerPort should parse");
        assert_eq!(parsed.protocol, "tcp");
        let argv = argv_string(&plan_with_ports(vec![parsed], None));
        assert!(
            argv_has_publish(&argv, "443:443/tcp"),
            "expected default protocol tcp: {argv:?}"
        );
    }

    #[test]
    fn awsvpc_network_mode_skips_publish() {
        let plan = plan_with_ports(
            vec![PortMapping {
                container_port: 80,
                host_port: 8080,
                protocol: "tcp".into(),
            }],
            Some("awsvpc"),
        );
        let argv = argv_string(&plan);
        assert!(
            !argv.iter().any(|s| s == "--publish"),
            "awsvpc must not emit --publish: {argv:?}"
        );
    }

    #[test]
    fn awsvpc_network_mode_includes_network_flag() {
        let plan = plan_with_ports(Vec::new(), Some("awsvpc"));
        let argv = argv_string(&plan);
        let network_idx = argv.iter().position(|s| s == "--network");
        assert!(
            network_idx.is_some(),
            "awsvpc must emit --network: {argv:?}"
        );
        let network_name = argv.get(network_idx.unwrap() + 1);
        assert!(
            network_name
                .map(|n| n.starts_with("fakecloud-ecs-"))
                .unwrap_or(false),
            "awsvpc must reference fakecloud-ecs network: {argv:?}"
        );
    }

    #[test]
    fn network_bindings_populated_on_task() {
        // Build a task in state, run mark_running_multi with a started
        // container that has network_bindings populated, and verify
        // task_to_json emits them under containers[0].networkBindings.
        let mut accounts: MultiAccountState<EcsState> =
            MultiAccountState::new("000000000000", "us-east-1", "http://localhost:4566");
        let acct = accounts.get_or_create("000000000000");
        let arn = acct.cluster_arn("default");
        acct.clusters
            .insert("default".into(), Cluster::new("default", arn));
        let mut task = Task {
            task_arn: "arn:aws:ecs:us-east-1:000000000000:task/default/abc".into(),
            task_id: "abc".into(),
            cluster_arn: "arn:aws:ecs:us-east-1:000000000000:cluster/default".into(),
            cluster_name: "default".into(),
            task_definition_arn: "arn:aws:ecs:us-east-1:000000000000:task-definition/web:1".into(),
            family: "web".into(),
            revision: 1,
            container_instance_arn: None,
            capacity_provider_name: None,
            last_status: "PENDING".into(),
            desired_status: "RUNNING".into(),
            launch_type: "FARGATE".into(),
            platform_version: None,
            cpu: None,
            memory: None,
            containers: vec![Container {
                container_arn: "arn:aws:ecs:us-east-1:000000000000:container/default/abc/web"
                    .into(),
                name: "web".into(),
                image: "alpine".into(),
                task_arn: "arn:aws:ecs:us-east-1:000000000000:task/default/abc".into(),
                last_status: "PENDING".into(),
                exit_code: None,
                reason: None,
                runtime_id: None,
                essential: true,
                cpu: None,
                memory: None,
                memory_reservation: None,
                network_bindings: Vec::new(),
                network_interfaces: Vec::new(),
                health_status: None,
                managed_agents: None,
                image_digest: None,
            }],
            overrides: serde_json::json!({}),
            started_by: None,
            group: None,
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
        };
        task.last_status = "PENDING".into();
        acct.tasks.insert("abc".into(), task);
        let state: SharedEcsState = Arc::new(RwLock::new(accounts));

        let bindings = vec![serde_json::json!({
            "bindIP": "0.0.0.0",
            "containerPort": 80,
            "hostPort": 8080,
            "protocol": "tcp",
        })];
        let started = vec![RunningContainer {
            name: "web".into(),
            container_id: "docker-id".into(),
            essential: true,
            exit_code: None,
            network_bindings: bindings.clone(),
            image_digest: None,
        }];
        mark_running_multi(&state, "000000000000", "abc", &started);

        let accounts = state.read();
        let task = accounts
            .get("000000000000")
            .unwrap()
            .tasks
            .get("abc")
            .unwrap();
        let json = task_to_json(task);
        let nb = &json["containers"][0]["networkBindings"];
        assert_eq!(nb, &serde_json::Value::Array(bindings));
    }
}

#[cfg(test)]
mod service_validation_tests {
    use super::multi_container_tests::*;
    use super::*;
    use serde_json::{json, Value};

    /// Register a task def + create an ACTIVE service named `web` with
    /// desiredCount 0, so the update/create validation tests have a target.
    fn service_ready(svc: &EcsService) {
        svc.register_task_definition(&make_request(
            "RegisterTaskDefinition",
            json!({"family": "web", "containerDefinitions": [{"name": "app", "image": "alpine"}]}),
        ))
        .unwrap();
        svc.create_service(&make_request(
            "CreateService",
            json!({"serviceName": "web", "taskDefinition": "web", "desiredCount": 0}),
        ))
        .unwrap();
    }

    #[test]
    fn update_service_negative_desired_count_is_invalid_parameter() {
        let svc = fresh_service();
        service_ready(&svc);
        // A negative desiredCount previously clamped to 0 and silently scaled
        // the service down. AWS rejects it instead.
        let err = match svc.update_service(&make_request(
            "UpdateService",
            json!({"service": "web", "desiredCount": -1}),
        )) {
            Ok(_) => panic!("expected InvalidParameterException for negative desiredCount"),
            Err(e) => e,
        };
        assert_eq!(err.code(), "InvalidParameterException");
    }

    #[test]
    fn update_service_on_inactive_service_is_service_not_active() {
        let svc = fresh_service();
        service_ready(&svc);
        // Simulate a deleted service left at INACTIVE (still describable).
        {
            let mut accounts = svc.state.write();
            let state = accounts.get_mut("000000000000").unwrap();
            let key = EcsState::service_key("default", "web");
            state.services.get_mut(&key).unwrap().status = "INACTIVE".into();
        }
        let err = match svc.update_service(&make_request(
            "UpdateService",
            json!({"service": "web", "desiredCount": 2}),
        )) {
            Ok(_) => panic!("expected ServiceNotActiveException for a non-ACTIVE service"),
            Err(e) => e,
        };
        assert_eq!(err.code(), "ServiceNotActiveException");
    }

    #[test]
    fn create_service_duplicate_name_is_invalid_parameter_not_idempotent() {
        let svc = fresh_service();
        service_ready(&svc);
        let err = match svc.create_service(&make_request(
            "CreateService",
            json!({"serviceName": "web", "taskDefinition": "web", "desiredCount": 0}),
        )) {
            Ok(_) => panic!("expected InvalidParameterException for a duplicate service name"),
            Err(e) => e,
        };
        assert_eq!(err.code(), "InvalidParameterException");
        assert_eq!(err.message(), "Creation of service was not idempotent.");
    }

    #[test]
    fn list_tasks_filters_by_service_name_and_container_instance() {
        let svc = fresh_service();
        // Two service-owned tasks (group service:web) on distinct container
        // instances, plus one unrelated task (group service:api).
        insert_task(&svc, "web1", "default");
        insert_task(&svc, "web2", "default");
        insert_task(&svc, "api1", "default");
        {
            let mut accounts = svc.state.write();
            let state = accounts.get_mut("000000000000").unwrap();
            let ci_a = "arn:aws:ecs:us-east-1:000000000000:container-instance/default/aaaaaaaa"
                .to_string();
            let ci_b = "arn:aws:ecs:us-east-1:000000000000:container-instance/default/bbbbbbbb"
                .to_string();
            let t = state.tasks.get_mut("web1").unwrap();
            t.group = Some("service:web".into());
            t.container_instance_arn = Some(ci_a);
            let t = state.tasks.get_mut("web2").unwrap();
            t.group = Some("service:web".into());
            t.container_instance_arn = Some(ci_b);
            let t = state.tasks.get_mut("api1").unwrap();
            t.group = Some("service:api".into());
        }

        // serviceName filter returns only the two web tasks.
        let resp = svc
            .list_tasks(&make_request("ListTasks", json!({"serviceName": "web"})))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arns = body["taskArns"].as_array().unwrap();
        assert_eq!(arns.len(), 2);
        assert!(arns.iter().all(|a| a.as_str().unwrap().contains("/web")));

        // containerInstance filter (bare ID) narrows to the single task on it.
        let resp = svc
            .list_tasks(&make_request(
                "ListTasks",
                json!({"containerInstance": "aaaaaaaa"}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arns = body["taskArns"].as_array().unwrap();
        assert_eq!(arns.len(), 1);
        assert!(arns[0].as_str().unwrap().ends_with("/web1"));

        // containerInstance filter also accepts a full ARN.
        let resp = svc
            .list_tasks(&make_request(
                "ListTasks",
                json!({"containerInstance": "arn:aws:ecs:us-east-1:000000000000:container-instance/default/bbbbbbbb"}),
            ))
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let arns = body["taskArns"].as_array().unwrap();
        assert_eq!(arns.len(), 1);
        assert!(arns[0].as_str().unwrap().ends_with("/web2"));
    }
}
