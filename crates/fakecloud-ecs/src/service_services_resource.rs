// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use chrono::Utc;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl EcsService {
    pub(super) fn create_service(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let service_name = req_str(&body, "serviceName")?.to_string();
        validate_service_name(&service_name)?;
        let td_ref = req_str(&body, "taskDefinition")?;
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let desired_count = body
            .get("desiredCount")
            .and_then(|v| v.as_i64())
            .filter(|n| *n >= 0)
            .unwrap_or(1) as i32;
        let launch_type = opt_str(&body, "launchType")
            .unwrap_or("FARGATE")
            .to_string();
        let scheduling = opt_str(&body, "schedulingStrategy")
            .unwrap_or("REPLICA")
            .to_string();
        let deployment_controller = body
            .get("deploymentController")
            .and_then(|v| v.get("type"))
            .and_then(|v| v.as_str())
            .unwrap_or("ECS")
            .to_string();
        let deployment_config = body.get("deploymentConfiguration");
        let min_healthy = deployment_config
            .and_then(|d| d.get("minimumHealthyPercent"))
            .and_then(|v| v.as_i64())
            .map(|n| n as i32);
        let max_percent = deployment_config
            .and_then(|d| d.get("maximumPercent"))
            .and_then(|v| v.as_i64())
            .map(|n| n as i32);
        let circuit = deployment_config.and_then(|d| d.get("deploymentCircuitBreaker"));
        let circuit_breaker = circuit.map(|c| CircuitBreakerConfig {
            enable: c.get("enable").and_then(|v| v.as_bool()).unwrap_or(false),
            rollback: c.get("rollback").and_then(|v| v.as_bool()).unwrap_or(false),
        });
        let lifecycle_hooks: Vec<Value> = deployment_config
            .and_then(|d| d.get("lifecycleHooks"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let tags = parse_tags(&body);
        let role_arn = opt_str(&body, "role").map(String::from);
        let load_balancers: Vec<Value> = body
            .get("loadBalancers")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let service_registries: Vec<Value> = body
            .get("serviceRegistries")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
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
        let network_configuration = body.get("networkConfiguration").cloned();
        let platform_version = opt_str(&body, "platformVersion").map(String::from);
        let health_check_grace_period_seconds = body
            .get("healthCheckGracePeriodSeconds")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32);
        let enable_execute_command = body
            .get("enableExecuteCommand")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let enable_ecs_managed_tags = body
            .get("enableECSManagedTags")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let propagate_tags = opt_str(&body, "propagateTags").map(String::from);
        let capacity_provider_strategy: Vec<Value> = body
            .get("capacityProviderStrategy")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let availability_zone_rebalancing =
            opt_str(&body, "availabilityZoneRebalancing").map(String::from);
        let volume_configurations: Vec<Value> = body
            .get("volumeConfigurations")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let runtime = self.runtime.clone();
        let account = request.account_id.clone();
        let principal_arn = request
            .principal
            .as_ref()
            .map(|p| p.arn.clone())
            .unwrap_or_else(|| Arn::global("iam", &request.account_id, "root").to_string());

        let (service_json, spawn_task_ids) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&account);
            // Resolve task definition to get family/revision.
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
            let td_arn = td.task_definition_arn.clone();
            let td_family = td.family.clone();
            let td_revision = td.revision;
            let cluster_arn = state
                .clusters
                .get(&cluster_name)
                .map(|c| c.cluster_arn.clone())
                .unwrap_or_else(|| state.cluster_arn(&cluster_name));
            let service_arn = state.service_arn(&cluster_name, &service_name);
            let key = EcsState::service_key(&cluster_name, &service_name);
            if let Some(existing) = state.services.get(&key) {
                if existing.status != "INACTIVE" {
                    return Err(service_already_exists(&service_name));
                }
            }
            let is_code_deploy = deployment_controller == "CODE_DEPLOY";
            let deployments = if is_code_deploy {
                Vec::new()
            } else {
                // A PAUSE lifecycle hook holds the deployment at its stage until
                // ContinueServiceDeployment resolves the hook.
                let pause_hook = lifecycle_hooks
                    .iter()
                    .find(|h| h.get("targetType").and_then(|v| v.as_str()) == Some("PAUSE"));
                let (pending_hook_id, lifecycle_stage, rollout_reason) = match pause_hook {
                    Some(hook) => {
                        let hook_id = format!("hook-{}", uuid::Uuid::new_v4().simple());
                        let stage = hook
                            .get("lifecycleStages")
                            .and_then(|v| v.as_array())
                            .and_then(|a| a.first())
                            .and_then(|v| v.as_str())
                            .map(String::from);
                        (
                            Some(hook_id),
                            stage,
                            "Deployment paused at a lifecycle hook awaiting ContinueServiceDeployment.".to_string(),
                        )
                    }
                    None => (None, None, "ECS deployment in progress.".to_string()),
                };
                vec![Deployment {
                    deployment_id: format!(
                        "ecs-svc/{}",
                        uuid::Uuid::new_v4().as_u128() & 0xffff_ffff_ffff_ffff
                    ),
                    status: "PRIMARY".into(),
                    task_definition_arn: td_arn.clone(),
                    desired_count,
                    pending_count: 0,
                    running_count: 0,
                    failed_tasks: 0,
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    launch_type: launch_type.clone(),
                    rollout_state: "IN_PROGRESS".into(),
                    rollout_state_reason: Some(rollout_reason),
                    lifecycle_hooks: lifecycle_hooks.clone(),
                    pending_hook_id,
                    lifecycle_stage,
                }]
            };
            let service = Service {
                service_name: service_name.clone(),
                service_arn: service_arn.clone(),
                cluster_name: cluster_name.clone(),
                cluster_arn: cluster_arn.clone(),
                task_definition_arn: td_arn,
                family: td_family,
                revision: td_revision,
                desired_count,
                running_count: 0,
                pending_count: 0,
                launch_type: launch_type.clone(),
                status: "ACTIVE".into(),
                scheduling_strategy: scheduling,
                deployment_controller: deployment_controller.clone(),
                minimum_healthy_percent: min_healthy,
                maximum_percent: max_percent,
                circuit_breaker,
                deployments,
                load_balancers: load_balancers.clone(),
                service_registries: service_registries.clone(),
                placement_constraints,
                placement_strategy,
                network_configuration,
                tags: tags.clone(),
                created_at: Utc::now(),
                created_by: Some(principal_arn.clone()),
                role_arn,
                platform_version: platform_version.clone(),
                health_check_grace_period_seconds,
                enable_execute_command,
                enable_ecs_managed_tags,
                propagate_tags: propagate_tags.clone(),
                capacity_provider_strategy: capacity_provider_strategy.clone(),
                availability_zone_rebalancing: availability_zone_rebalancing.clone(),
                volume_configurations: volume_configurations.clone(),
            };
            state.services.insert(key.clone(), service.clone());
            if let Some(cluster) = state.clusters.get_mut(&cluster_name) {
                cluster.active_services_count += 1;
            }
            state.push_event(crate::state::LifecycleEvent {
                at: Utc::now(),
                event_type: "ServiceCreated".into(),
                task_arn: None,
                cluster_arn: Some(cluster_arn),
                last_status: Some("ACTIVE".into()),
                detail: json!({"serviceArn": service_arn, "desiredCount": desired_count}),
            });
            let ids = spawn_service_tasks(
                state,
                &service,
                desired_count,
                &principal_arn,
                &launch_type,
                None,
            );
            if is_code_deploy {
                let ts_id = uuid::Uuid::new_v4().to_string().replace('-', "");
                let ts_arn = format!(
                    "arn:aws:ecs:{}:{}:task-set/{}/{}/{}",
                    state.region, state.account_id, cluster_name, service_name, ts_id
                );
                let task_set = TaskSet {
                    task_set_id: ts_id,
                    task_set_arn: ts_arn.clone(),
                    service_arn: service_arn.clone(),
                    cluster_arn: service.cluster_arn.clone(),
                    service_name: service_name.clone(),
                    cluster_name: cluster_name.clone(),
                    external_id: None,
                    status: "PRIMARY".into(),
                    task_definition: service.task_definition_arn.clone(),
                    computed_desired_count: desired_count,
                    pending_count: ids.len() as i32,
                    running_count: 0,
                    launch_type: Some(launch_type.clone()),
                    platform_version: Some("1.4.0".into()),
                    scale: None,
                    stability_status: "STABILIZING".into(),
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    load_balancers: service.load_balancers.clone(),
                    service_registries: service.service_registries.clone(),
                    capacity_provider_strategy: service.capacity_provider_strategy.clone(),
                    tags: Vec::new(),
                };
                state.task_sets.insert(
                    format!("{}/{}/{}", cluster_name, service_name, task_set.task_set_id),
                    task_set,
                );
            }
            let mut service_json = service_to_json(state.services.get(&key).unwrap());
            if is_code_deploy {
                inject_service_task_sets(state, &service_name, &cluster_name, &mut service_json);
            }
            (service_json, ids)
        };

        if let Some(rt) = runtime {
            for id in spawn_task_ids {
                rt.clone().run_task(self.state.clone(), id, account.clone());
            }
        } else {
            let mut accounts = self.state.write();
            if let Some(state) = accounts.get_mut(&account) {
                let mut cluster_drains: Vec<String> = Vec::new();
                for id in &spawn_task_ids {
                    if let Some(t) = state.tasks.get_mut(id) {
                        t.last_status = "STOPPED".into();
                        t.stop_code = Some("TaskFailedToStart".into());
                        t.stopped_reason = Some(
                            "No container runtime available (docker/podman not installed)".into(),
                        );
                        t.stopped_at = Some(Utc::now());
                        for c in t.containers.iter_mut() {
                            c.last_status = "STOPPED".into();
                        }
                        cluster_drains.push(t.cluster_name.clone());
                    }
                }
                for name in cluster_drains {
                    if let Some(cluster) = state.clusters.get_mut(&name) {
                        if cluster.pending_tasks_count > 0 {
                            cluster.pending_tasks_count -= 1;
                        }
                    }
                }
            }
        }

        Ok(AwsResponse::ok_json(json!({ "service": service_json })))
    }

    pub(super) fn update_service(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let service_ref = req_str(&body, "service")?;
        let service_name = service_name_from_ref(service_ref);
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let new_desired = body.get("desiredCount").and_then(|v| v.as_i64());
        let new_td_ref = opt_str(&body, "taskDefinition");
        let update_lifecycle_hooks: Vec<Value> = body
            .get("deploymentConfiguration")
            .and_then(|d| d.get("lifecycleHooks"))
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let account = request.account_id.clone();
        let principal_arn = request
            .principal
            .as_ref()
            .map(|p| p.arn.clone())
            .unwrap_or_else(|| Arn::global("iam", &request.account_id, "root").to_string());
        let runtime = self.runtime.clone();

        let (service_json, spawn_ids, stop_ids) = {
            let mut accounts = self.state.write();
            let state = accounts
                .get_mut(&account)
                .ok_or_else(|| service_not_found(&service_name))?;
            let key = EcsState::service_key(&cluster_name, &service_name);
            if !state.services.contains_key(&key) {
                return Err(service_not_found(&service_name));
            }

            // Resolve new task definition (may stay on current one).
            let (new_td_arn, new_family, new_revision) = if let Some(td_ref) = new_td_ref {
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
                (
                    Some(td.task_definition_arn.clone()),
                    td.family.clone(),
                    td.revision,
                )
            } else {
                let svc = state.services.get(&key).unwrap();
                (None, svc.family.clone(), svc.revision)
            };

            let service_cluster_arn;
            let launch_type_clone;
            let effective_desired;
            let old_desired;
            let mut old_deployments_drained: Vec<String> = Vec::new();
            let mut new_deployment_triggered = false;

            {
                let svc = state.services.get_mut(&key).unwrap();
                old_desired = svc.desired_count;
                service_cluster_arn = svc.cluster_arn.clone();
                launch_type_clone = svc.launch_type.clone();

                // Apply the remaining mutable service fields UpdateService
                // accepts. AWS updates each only when present in the request;
                // omitting one leaves it unchanged. The old handler read only
                // desiredCount / taskDefinition / lifecycleHooks and silently
                // dropped everything else, so ECS Exec enablement, subnet/SG
                // changes, deployment tuning, load-balancer swaps, etc. all
                // no-op'd (bug-audit 2026-06-20, 1.15).
                if let Some(dc) = body.get("deploymentConfiguration") {
                    if let Some(n) = dc.get("minimumHealthyPercent").and_then(|v| v.as_i64()) {
                        svc.minimum_healthy_percent = Some(n as i32);
                    }
                    if let Some(n) = dc.get("maximumPercent").and_then(|v| v.as_i64()) {
                        svc.maximum_percent = Some(n as i32);
                    }
                    if let Some(c) = dc.get("deploymentCircuitBreaker") {
                        svc.circuit_breaker = Some(CircuitBreakerConfig {
                            enable: c.get("enable").and_then(|v| v.as_bool()).unwrap_or(false),
                            rollback: c.get("rollback").and_then(|v| v.as_bool()).unwrap_or(false),
                        });
                    }
                }
                if let Some(v) = body.get("networkConfiguration") {
                    svc.network_configuration = Some(v.clone());
                }
                if let Some(s) = opt_str(&body, "platformVersion") {
                    svc.platform_version = Some(s.to_string());
                }
                if let Some(n) = body
                    .get("healthCheckGracePeriodSeconds")
                    .and_then(|v| v.as_i64())
                {
                    svc.health_check_grace_period_seconds = Some(n as i32);
                }
                if let Some(b) = body.get("enableExecuteCommand").and_then(|v| v.as_bool()) {
                    svc.enable_execute_command = b;
                }
                if let Some(b) = body.get("enableECSManagedTags").and_then(|v| v.as_bool()) {
                    svc.enable_ecs_managed_tags = b;
                }
                if let Some(s) = opt_str(&body, "propagateTags") {
                    svc.propagate_tags = Some(s.to_string());
                }
                if let Some(a) = body
                    .get("capacityProviderStrategy")
                    .and_then(|v| v.as_array())
                {
                    svc.capacity_provider_strategy = a.clone();
                }
                if let Some(a) = body.get("placementConstraints").and_then(|v| v.as_array()) {
                    svc.placement_constraints = a.clone();
                }
                if let Some(a) = body.get("placementStrategy").and_then(|v| v.as_array()) {
                    svc.placement_strategy = a.clone();
                }
                if let Some(a) = body.get("loadBalancers").and_then(|v| v.as_array()) {
                    svc.load_balancers = a.clone();
                }
                if let Some(a) = body.get("serviceRegistries").and_then(|v| v.as_array()) {
                    svc.service_registries = a.clone();
                }

                if let Some(n) = new_desired {
                    let n = n.max(0) as i32;
                    svc.desired_count = n;
                    if svc.deployment_controller != "CODE_DEPLOY" {
                        if let Some(d) = svc.deployments.iter_mut().find(|d| d.status == "PRIMARY")
                        {
                            d.desired_count = n;
                            d.updated_at = Utc::now();
                        }
                    }
                }

                if let Some(arn) = new_td_arn.clone() {
                    svc.task_definition_arn = arn;
                    svc.family = new_family;
                    svc.revision = new_revision;
                    new_deployment_triggered = true;
                    if svc.deployment_controller != "CODE_DEPLOY" {
                        // Roll a new PRIMARY deployment; mark the previous one ACTIVE
                        // so it's eligible for drain once the new deployment ramps.
                        for d in svc.deployments.iter_mut() {
                            if d.status == "PRIMARY" {
                                d.status = "ACTIVE".into();
                                old_deployments_drained.push(d.deployment_id.clone());
                            }
                        }
                        let pause_hook = update_lifecycle_hooks.iter().find(|h| {
                            h.get("targetType").and_then(|v| v.as_str()) == Some("PAUSE")
                        });
                        let (pending_hook_id, lifecycle_stage, rollout_reason) = match pause_hook {
                            Some(hook) => {
                                let hook_id = format!("hook-{}", uuid::Uuid::new_v4().simple());
                                let stage = hook
                                    .get("lifecycleStages")
                                    .and_then(|v| v.as_array())
                                    .and_then(|a| a.first())
                                    .and_then(|v| v.as_str())
                                    .map(String::from);
                                (
                                    Some(hook_id),
                                    stage,
                                    "Deployment paused at a lifecycle hook awaiting ContinueServiceDeployment.".to_string(),
                                )
                            }
                            None => (None, None, "ECS deployment in progress.".to_string()),
                        };
                        svc.deployments.insert(
                            0,
                            Deployment {
                                deployment_id: format!(
                                    "ecs-svc/{}",
                                    uuid::Uuid::new_v4().as_u128() & 0xffff_ffff_ffff_ffff
                                ),
                                status: "PRIMARY".into(),
                                task_definition_arn: svc.task_definition_arn.clone(),
                                desired_count: svc.desired_count,
                                pending_count: 0,
                                running_count: 0,
                                failed_tasks: 0,
                                created_at: Utc::now(),
                                updated_at: Utc::now(),
                                launch_type: svc.launch_type.clone(),
                                rollout_state: "IN_PROGRESS".into(),
                                rollout_state_reason: Some(rollout_reason),
                                lifecycle_hooks: update_lifecycle_hooks.clone(),
                                pending_hook_id,
                                lifecycle_stage,
                            },
                        );
                    }
                }

                effective_desired = svc.desired_count;
            }

            let controller = state
                .services
                .get(&key)
                .unwrap()
                .deployment_controller
                .clone();
            let primary_ts_arn = if controller == "CODE_DEPLOY" {
                state
                    .task_sets
                    .values()
                    .find(|ts| {
                        ts.service_name == service_name
                            && ts.cluster_name == cluster_name
                            && ts.status == "PRIMARY"
                    })
                    .map(|ts| ts.task_set_arn.clone())
            } else {
                None
            };

            // Compute spawn + stop plan.
            let mut spawn: Vec<String> = Vec::new();
            let mut stop: Vec<String> = Vec::new();

            let service_tag = format!("ecs-svc/{}", service_name);
            let now = Utc::now();

            if new_deployment_triggered && controller == "CODE_DEPLOY" {
                let new_td_arn_match = state
                    .services
                    .get(&key)
                    .unwrap()
                    .task_definition_arn
                    .clone();

                // Create new ACTIVE task set with the new TD
                let ts_id = uuid::Uuid::new_v4().to_string().replace('-', "");
                let ts_arn = format!(
                    "arn:aws:ecs:{}:{}:task-set/{}/{}/{}",
                    state.region, state.account_id, cluster_name, service_name, ts_id
                );
                let svc_snapshot = state.services.get(&key).unwrap().clone();
                let task_set = TaskSet {
                    task_set_id: ts_id,
                    task_set_arn: ts_arn.clone(),
                    service_arn: svc_snapshot.service_arn.clone(),
                    cluster_arn: svc_snapshot.cluster_arn.clone(),
                    service_name: service_name.clone(),
                    cluster_name: cluster_name.clone(),
                    external_id: None,
                    status: "ACTIVE".into(),
                    task_definition: new_td_arn_match.clone(),
                    computed_desired_count: 0,
                    pending_count: 0,
                    running_count: 0,
                    launch_type: Some(launch_type_clone.clone()),
                    platform_version: Some("1.4.0".into()),
                    scale: None,
                    stability_status: "STABILIZING".into(),
                    created_at: Utc::now(),
                    updated_at: Utc::now(),
                    load_balancers: svc_snapshot.load_balancers.clone(),
                    service_registries: svc_snapshot.service_registries.clone(),
                    capacity_provider_strategy: svc_snapshot.capacity_provider_strategy.clone(),
                    tags: Vec::new(),
                };
                state.task_sets.insert(
                    format!("{}/{}/{}", cluster_name, service_name, task_set.task_set_id),
                    task_set.clone(),
                );

                // Spawn tasks for the new task set
                let mut new_ids = spawn_service_tasks(
                    state,
                    &svc_snapshot,
                    effective_desired,
                    &principal_arn,
                    &launch_type_clone,
                    Some(ts_arn.clone()),
                );
                spawn.append(&mut new_ids);

                // Flip PRIMARY: demote old PRIMARY to ACTIVE with computed_desired_count=0
                if let Some(old_ts) = state.task_sets.values_mut().find(|ts| {
                    ts.service_name == service_name
                        && ts.cluster_name == cluster_name
                        && ts.status == "PRIMARY"
                }) {
                    old_ts.status = "ACTIVE".into();
                    old_ts.computed_desired_count = 0;
                    old_ts.updated_at = Utc::now();
                }

                // Promote new task set to PRIMARY
                if let Some(new_ts) = state.task_sets.get_mut(&format!(
                    "{}/{}/{}",
                    cluster_name, service_name, task_set.task_set_id
                )) {
                    new_ts.status = "PRIMARY".into();
                    new_ts.computed_desired_count = effective_desired;
                    new_ts.updated_at = Utc::now();
                }

                // Stop ALL old tasks
                for (id, task) in state.tasks.iter() {
                    if task.started_by.as_deref() == Some(service_tag.as_str())
                        && task.cluster_name == cluster_name
                        && task.task_definition_arn != new_td_arn_match
                        && !stop.contains(id)
                    {
                        stop.push(id.clone());
                    }
                }
            } else {
                // Tasks belonging to this service (by startedBy convention).
                // Track per-task protection so scale-down skips protected ones
                // (UpdateTaskProtection). Real AWS only terminates a protected
                // task when nothing else is eligible.
                let current_tasks: Vec<(String, String, bool)> = state
                    .tasks
                    .iter()
                    .filter(|(_, t)| {
                        t.started_by.as_deref() == Some(service_tag.as_str())
                            && t.cluster_name == cluster_name
                            && t.last_status != "STOPPED"
                    })
                    .map(|(id, t)| {
                        let protected = t
                            .protection
                            .as_ref()
                            .filter(|p| p.enabled && p.expiration.is_none_or(|exp| exp > now))
                            .is_some();
                        (id.clone(), t.task_definition_arn.clone(), protected)
                    })
                    .collect();

                let current_count = current_tasks.len() as i32;
                if effective_desired > current_count {
                    let add = (effective_desired - current_count) as usize;
                    let svc_snapshot = state.services.get(&key).unwrap().clone();
                    let mut new_ids = spawn_service_tasks(
                        state,
                        &svc_snapshot,
                        add as i32,
                        &principal_arn,
                        &launch_type_clone,
                        primary_ts_arn.clone(),
                    );
                    spawn.append(&mut new_ids);
                } else if effective_desired < current_count {
                    let mut remove = (current_count - effective_desired) as usize;
                    // First pass: drain unprotected tasks. Only fall back to
                    // protected ones when there's nothing else left to stop.
                    for (id, _, protected) in current_tasks.iter() {
                        if remove == 0 {
                            break;
                        }
                        if !*protected {
                            stop.push(id.clone());
                            remove -= 1;
                        }
                    }
                    if remove > 0 {
                        for (id, _, protected) in current_tasks.iter() {
                            if remove == 0 {
                                break;
                            }
                            if *protected && !stop.contains(id) {
                                stop.push(id.clone());
                                remove -= 1;
                            }
                        }
                    }
                }

                // If a new deployment was triggered, also stop tasks still on
                // the old task definition so the new deployment can ramp up,
                // then spawn replacements — but only enough to hit the
                // effective desired count (not `stop.len()`, which conflates
                // scale-down drain with TD-drain and would over-spawn).
                if new_deployment_triggered {
                    let new_td_arn_match = state
                        .services
                        .get(&key)
                        .unwrap()
                        .task_definition_arn
                        .clone();
                    // Tasks already on the new task definition that we're NOT
                    // stopping (scale-down may have picked the first N; those
                    // are skipped here via `stop.contains(id)`).
                    let kept_on_new_td: i32 = current_tasks
                        .iter()
                        .filter(|(id, t_arn, _)| *t_arn == new_td_arn_match && !stop.contains(id))
                        .count() as i32;
                    for (id, t_arn, protected) in &current_tasks {
                        if *t_arn != new_td_arn_match && !stop.contains(id) && !*protected {
                            stop.push(id.clone());
                        }
                    }
                    let already_spawned = spawn.len() as i32;
                    let need = (effective_desired - kept_on_new_td - already_spawned).max(0);
                    if need > 0 {
                        let svc_snapshot = state.services.get(&key).unwrap().clone();
                        let mut more = spawn_service_tasks(
                            state,
                            &svc_snapshot,
                            need,
                            &principal_arn,
                            &launch_type_clone,
                            primary_ts_arn.clone(),
                        );
                        spawn.append(&mut more);
                    }
                }
            }

            if controller == "CODE_DEPLOY" && !new_deployment_triggered {
                if let Some(ts) = state.task_sets.values_mut().find(|ts| {
                    ts.service_name == service_name
                        && ts.cluster_name == cluster_name
                        && ts.status == "PRIMARY"
                }) {
                    ts.computed_desired_count = effective_desired;
                    ts.updated_at = Utc::now();
                }
            }

            state.push_event(crate::state::LifecycleEvent {
                at: Utc::now(),
                event_type: "ServiceUpdated".into(),
                task_arn: None,
                cluster_arn: Some(service_cluster_arn),
                last_status: Some("ACTIVE".into()),
                detail: json!({
                    "serviceArn": state.services.get(&key).unwrap().service_arn,
                    "desiredCount": effective_desired,
                    "previousDesiredCount": old_desired,
                    "newDeployment": new_deployment_triggered,
                    "drainedDeployments": old_deployments_drained,
                }),
            });

            let svc = state.services.get(&key).unwrap();
            let mut service_json = service_to_json(svc);
            if controller == "CODE_DEPLOY" {
                inject_service_task_sets(state, &service_name, &cluster_name, &mut service_json);
            }
            (service_json, spawn, stop)
        };

        // Always flip desired_status for tasks we plan to stop.
        for id in &stop_ids {
            let mut accounts = self.state.write();
            if let Some(state) = accounts.get_mut(&account) {
                if let Some(task) = state.tasks.get_mut(id) {
                    task.desired_status = "STOPPED".into();
                    task.stopping_at = Some(Utc::now());
                    if runtime.is_none() {
                        task.last_status = "STOPPED".into();
                        task.stopped_at = Some(Utc::now());
                        task.stop_code = Some("UserInitiated".into());
                        for c in task.containers.iter_mut() {
                            c.last_status = "STOPPED".into();
                        }
                        if let Some(cluster) = state.clusters.get_mut(&task.cluster_name) {
                            if cluster.pending_tasks_count > 0 {
                                cluster.pending_tasks_count -= 1;
                            }
                        }
                    }
                }
            }
        }

        if let Some(rt) = runtime {
            for id in spawn_ids {
                rt.clone().run_task(self.state.clone(), id, account.clone());
            }
            for id in stop_ids {
                let rt2 = rt.clone();
                let id_clone = id.clone();
                tokio::spawn(async move {
                    rt2.stop_task(&id_clone, "ECS service scale-down").await;
                });
            }
        } else {
            let mut accounts = self.state.write();
            if let Some(state) = accounts.get_mut(&account) {
                let mut cluster_drains: Vec<String> = Vec::new();
                for id in &spawn_ids {
                    if let Some(t) = state.tasks.get_mut(id) {
                        t.last_status = "STOPPED".into();
                        t.stop_code = Some("TaskFailedToStart".into());
                        t.stopped_reason = Some(
                            "No container runtime available (docker/podman not installed)".into(),
                        );
                        t.stopped_at = Some(Utc::now());
                        for c in t.containers.iter_mut() {
                            c.last_status = "STOPPED".into();
                        }
                        cluster_drains.push(t.cluster_name.clone());
                    }
                }
                for name in cluster_drains {
                    if let Some(cluster) = state.clusters.get_mut(&name) {
                        if cluster.pending_tasks_count > 0 {
                            cluster.pending_tasks_count -= 1;
                        }
                    }
                }
            }
        }

        Ok(AwsResponse::ok_json(json!({ "service": service_json })))
    }

    pub(super) async fn delete_service(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let service_ref = req_str(&body, "service")?;
        let service_name = service_name_from_ref(service_ref);
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let force = body.get("force").and_then(|v| v.as_bool()).unwrap_or(false);

        let (snapshot, task_ids_to_stop, task_sets_for_response) = {
            let mut accounts = self.state.write();
            let state = accounts
                .get_mut(&request.account_id)
                .ok_or_else(|| service_not_found(&service_name))?;
            let key = EcsState::service_key(&cluster_name, &service_name);
            let svc = state
                .services
                .get_mut(&key)
                .ok_or_else(|| service_not_found(&service_name))?;
            if !force && svc.desired_count > 0 {
                return Err(client_exception(
                    "The service cannot be stopped while it is scaled above 0. \
                     Either set desiredCount to 0 first, or pass force=true.",
                ));
            }
            svc.desired_count = 0;
            svc.status = "DRAINING".into();
            let service_tag = format!("ecs-svc/{}", service_name);
            let stop_ids: Vec<String> = state
                .tasks
                .iter()
                .filter(|(_, t)| {
                    t.started_by.as_deref() == Some(service_tag.as_str())
                        && t.cluster_name == cluster_name
                        && t.last_status != "STOPPED"
                })
                .map(|(id, _)| id.clone())
                .collect();
            if let Some(cluster) = state.clusters.get_mut(&cluster_name) {
                if cluster.active_services_count > 0 {
                    cluster.active_services_count -= 1;
                }
            }
            let svc_snapshot = state.services.get(&key).unwrap().clone();
            let mut task_sets_for_response = Vec::new();
            if svc_snapshot.deployment_controller == "CODE_DEPLOY" {
                task_sets_for_response = state
                    .task_sets
                    .values()
                    .filter(|ts| ts.service_name == service_name && ts.cluster_name == cluster_name)
                    .map(task_set_to_json)
                    .collect();
                state.task_sets.retain(|_, ts| {
                    ts.service_name != service_name || ts.cluster_name != cluster_name
                });
            }
            state.services.remove(&key);
            state.push_event(crate::state::LifecycleEvent {
                at: Utc::now(),
                event_type: "ServiceDeleted".into(),
                task_arn: None,
                cluster_arn: Some(svc_snapshot.cluster_arn.clone()),
                last_status: Some("DRAINING".into()),
                detail: json!({"serviceArn": svc_snapshot.service_arn}),
            });
            (svc_snapshot, stop_ids, task_sets_for_response)
        };

        for id in &task_ids_to_stop {
            {
                let mut accounts = self.state.write();
                if let Some(state) = accounts.get_mut(&request.account_id) {
                    if let Some(task) = state.tasks.get_mut(id) {
                        task.desired_status = "STOPPED".into();
                        task.stopping_at = Some(Utc::now());
                        if self.runtime.is_none() {
                            task.last_status = "STOPPED".into();
                            task.stopped_at = Some(Utc::now());
                            task.stop_code = Some("UserInitiated".into());
                            for c in task.containers.iter_mut() {
                                c.last_status = "STOPPED".into();
                            }
                            if let Some(cluster) = state.clusters.get_mut(&task.cluster_name) {
                                if cluster.pending_tasks_count > 0 {
                                    cluster.pending_tasks_count -= 1;
                                }
                            }
                        }
                    }
                }
            }
            if let Some(rt) = &self.runtime {
                rt.stop_task(id, "ECS service deletion").await;
            }
        }

        let mut resp = json!({ "service": service_to_json(&snapshot) });
        if !task_sets_for_response.is_empty() {
            resp.as_object_mut()
                .unwrap()
                .insert("taskSets".into(), json!(task_sets_for_response));
        }
        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn describe_services(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let refs: Vec<String> = req_array(&body, "services")?
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();

        let account = request.account_id.clone();
        let accounts = self.state.read();
        let mut found = Vec::new();
        let mut failures = Vec::new();
        let Some(state) = accounts.get(&account) else {
            for r in &refs {
                failures.push(json!({"arn": r, "reason": "MISSING"}));
            }
            return Ok(AwsResponse::ok_json(
                json!({"services": found, "failures": failures}),
            ));
        };
        for r in &refs {
            let name = service_name_from_ref(r);
            let key = EcsState::service_key(&cluster_name, &name);
            match state.services.get(&key) {
                Some(svc) => {
                    let mut v = service_to_json(svc);
                    // Update derived running/pending counts from current tasks.
                    recompute_service_counts(state, &name, &cluster_name, &mut v);
                    inject_service_task_sets(state, &name, &cluster_name, &mut v);
                    found.push(v);
                }
                None => failures.push(json!({"arn": r, "reason": "MISSING"})),
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "services": found,
            "failures": failures,
        })))
    }

    pub(super) fn list_services(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_enum_opt(
            &body,
            "launchType",
            &["EC2", "FARGATE", "EXTERNAL", "MANAGED_INSTANCES"],
        )?;
        validate_enum_opt(&body, "schedulingStrategy", &["REPLICA", "DAEMON"])?;
        validate_enum_opt(&body, "resourceManagementType", &["CUSTOMER", "ECS"])?;
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let launch_type = opt_str(&body, "launchType");
        let scheduling = opt_str(&body, "schedulingStrategy");
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
                .services
                .values()
                .filter(|s| s.cluster_name == cluster_name)
                .filter(|s| launch_type.is_none_or(|lt| s.launch_type == lt))
                .filter(|s| scheduling.is_none_or(|sc| s.scheduling_strategy == sc))
                .map(|s| s.service_arn.clone())
                .collect(),
            None => Vec::new(),
        };
        arns.sort();
        let start = next_token.parse::<usize>().unwrap_or(0).min(arns.len());
        let end = (start + max_results).min(arns.len());
        let page = arns[start..end].to_vec();
        let mut out = json!({"serviceArns": page});
        if end < arns.len() {
            out.as_object_mut()
                .unwrap()
                .insert("nextToken".into(), json!(end.to_string()));
        }
        Ok(AwsResponse::ok_json(out))
    }

    pub(super) fn list_services_by_namespace(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // fakecloud doesn't model Cloud Map namespaces yet — return all
        // services attached to services with registries pointing at the
        // given namespace ARN. Filter is loose: treat the namespace as a
        // hint and return every service when none match to mirror AWS's
        // "loose match when ambiguous" response shape.
        let body = request.json_body();
        let namespace = req_str(&body, "namespace")?.to_string();
        let account = request.account_id.clone();
        let accounts = self.state.read();
        let mut arns: Vec<String> = match accounts.get(&account) {
            Some(state) => state
                .services
                .values()
                .filter(|s| {
                    s.service_registries.iter().any(|r| {
                        r.get("registryArn")
                            .and_then(|v| v.as_str())
                            .is_some_and(|arn| arn.contains(&namespace))
                    })
                })
                .map(|s| s.service_arn.clone())
                .collect(),
            None => Vec::new(),
        };
        arns.sort();
        Ok(AwsResponse::ok_json(json!({"serviceArns": arns})))
    }
}
