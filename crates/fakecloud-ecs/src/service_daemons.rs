// Daemon and DaemonTaskDefinition operations.
//
// AWS shipped Daemon services (DaemonSet-style scheduling, one task per
// matching capacity provider) and a dedicated DaemonTaskDefinition
// shape in late 2025. Implemented here against the same dispatcher
// pattern as Service / TaskDefinition.

#![allow(clippy::too_many_arguments)]

use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::pagination::paginate_checked;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;
use crate::state::{Daemon, DaemonDeployment, DaemonTaskDefinition};

impl EcsService {
    pub(super) fn register_daemon_task_definition(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let family = req_str(&body, "family")?.to_string();
        let containers = body
            .get("containerDefinitions")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ClientException",
                    "containerDefinitions is required",
                )
            })?
            .clone();
        let task_role_arn = body
            .get("taskRoleArn")
            .and_then(|v| v.as_str())
            .map(String::from);
        let execution_role_arn = body
            .get("executionRoleArn")
            .and_then(|v| v.as_str())
            .map(String::from);
        let cpu = body.get("cpu").and_then(|v| v.as_str()).map(String::from);
        let memory = body
            .get("memory")
            .and_then(|v| v.as_str())
            .map(String::from);
        let volumes = body
            .get("volumes")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let tags = parse_tags(&body);

        let mut accounts = self.state.write();
        let account_id = request.account_id.clone();
        let s = accounts.get_or_create(&account_id);
        let revision = s.allocate_daemon_revision(&family);
        let arn = s.daemon_task_definition_arn(&family, revision);

        let def = DaemonTaskDefinition {
            family: family.clone(),
            revision,
            task_definition_arn: arn.clone(),
            status: "ACTIVE".to_string(),
            container_definitions: containers,
            task_role_arn,
            execution_role_arn,
            cpu,
            memory,
            volumes,
            registered_at: Utc::now(),
            deregistered_at: None,
            tags,
        };

        s.daemon_task_definitions
            .entry(family.clone())
            .or_default()
            .insert(revision, def.clone());

        let arn_clone = def.task_definition_arn.clone();
        Ok(AwsResponse::ok_json(json!({
            "daemonTaskDefinitionArn": arn_clone,
        })))
    }

    pub(super) fn describe_daemon_task_definition(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let identifier = req_str(&body, "daemonTaskDefinition")?.to_string();
        let accounts = self.state.read();
        let s = accounts
            .get(&request.account_id)
            .cloned()
            .unwrap_or_else(|| accounts.default_ref().clone());
        let def = lookup_daemon_task_definition(&s, &identifier).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ClientException",
                format!("Daemon task definition {} not found", identifier),
            )
        })?;
        Ok(AwsResponse::ok_json(json!({
            "daemonTaskDefinition": daemon_task_definition_json(&def),
        })))
    }

    pub(super) fn delete_daemon_task_definition(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let identifier = req_str(&body, "daemonTaskDefinition")?.to_string();
        let mut accounts = self.state.write();
        let s = accounts.get_or_create(&request.account_id);
        let arn = if let Some(def) = lookup_daemon_task_definition_mut(s, &identifier) {
            def.status = "DELETE_IN_PROGRESS".to_string();
            def.deregistered_at = Some(Utc::now());
            def.task_definition_arn.clone()
        } else {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ClientException",
                format!("Daemon task definition {} not found", identifier),
            ));
        };
        Ok(AwsResponse::ok_json(json!({
            "daemonTaskDefinitionArn": arn,
        })))
    }

    pub(super) fn list_daemon_task_definitions(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_enum_opt(&body, "status", &["ACTIVE", "DELETE_IN_PROGRESS", "ALL"])?;
        validate_enum_opt(&body, "revision", &["LAST_REGISTERED"])?;
        validate_enum_opt(&body, "sort", &["ASC", "DESC"])?;
        let family_prefix = body
            .get("familyPrefix")
            .and_then(|v| v.as_str())
            .map(String::from);
        let status_filter = body
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("ACTIVE")
            .to_string();
        let max_results = body
            .get("maxResults")
            .and_then(|v| v.as_i64())
            .unwrap_or(100) as usize;
        let next_token = body
            .get("nextToken")
            .and_then(|v| v.as_str())
            .map(String::from);

        let accounts = self.state.read();
        let s = accounts
            .get(&request.account_id)
            .cloned()
            .unwrap_or_else(|| accounts.default_ref().clone());
        let mut summaries: Vec<DaemonTaskDefinition> = Vec::new();
        for (family, revisions) in &s.daemon_task_definitions {
            if let Some(prefix) = &family_prefix {
                if !family.starts_with(prefix) {
                    continue;
                }
            }
            for def in revisions.values() {
                if def.status == status_filter {
                    summaries.push(def.clone());
                }
            }
        }
        summaries.sort_by(|a, b| a.task_definition_arn.cmp(&b.task_definition_arn));

        let (page, token) = paginate_checked(&summaries, next_token.as_deref(), max_results)
            .map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "Invalid nextToken".to_string(),
                )
            })?;
        let json_page: Vec<Value> = page
            .iter()
            .map(daemon_task_definition_summary_json)
            .collect();
        Ok(AwsResponse::ok_json(json!({
            "daemonTaskDefinitions": json_page,
            "nextToken": token,
        })))
    }

    pub(super) fn create_daemon(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let daemon_name = req_str(&body, "daemonName")?.to_string();
        let task_definition_arn = req_str(&body, "daemonTaskDefinitionArn")?.to_string();
        let cluster_input = body.get("clusterArn").and_then(|v| v.as_str());
        let capacity_provider_arns: Vec<String> = body
            .get("capacityProviderArns")
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ClientException",
                    "capacityProviderArns is required",
                )
            })?
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();

        let deployment_configuration = body.get("deploymentConfiguration").cloned();
        let propagate_tags = body
            .get("propagateTags")
            .and_then(|v| v.as_str())
            .map(String::from);
        let enable_ecs_managed_tags = body
            .get("enableECSManagedTags")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let enable_execute_command = body
            .get("enableExecuteCommand")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let client_token = body
            .get("clientToken")
            .and_then(|v| v.as_str())
            .map(String::from);
        let tags = parse_tags(&body);

        let now = Utc::now();
        let runtime = self.runtime.clone();
        let account = request.account_id.clone();
        let principal_arn = request
            .principal
            .as_ref()
            .map(|p| p.arn.clone())
            .unwrap_or_default();

        let (daemon_arn, deployment_arn, daemon_json, spawn_ids) = {
            let mut accounts = self.state.write();
            let s = accounts.get_or_create(&account);
            let cluster_name = cluster_arn_to_name(cluster_input.unwrap_or("default"));
            let cluster_arn = s.cluster_arn(&cluster_name);

            if !s.clusters.contains_key(&cluster_name) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ClusterNotFoundException",
                    format!("Cluster {} not found", cluster_name),
                ));
            }

            let key = EcsState::daemon_key(&cluster_name, &daemon_name);
            if s.daemons.contains_key(&key) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ClientException",
                    format!(
                        "Daemon {} already exists in cluster {}",
                        daemon_name, cluster_name
                    ),
                ));
            }

            let daemon_arn = s.daemon_arn(&cluster_name, &daemon_name);
            let deployment_id = uuid::Uuid::new_v4().to_string();
            let deployment_arn = s.daemon_deployment_arn(&daemon_name, &deployment_id);

            let deployment = DaemonDeployment {
                deployment_arn: deployment_arn.clone(),
                daemon_arn: daemon_arn.clone(),
                daemon_name: daemon_name.clone(),
                cluster_arn: cluster_arn.clone(),
                task_definition_arn: task_definition_arn.clone(),
                status: "PRIMARY".to_string(),
                revision: 1,
                created_at: now,
                updated_at: now,
            };

            let daemon = Daemon {
                daemon_name: daemon_name.clone(),
                daemon_arn: daemon_arn.clone(),
                cluster_arn,
                cluster_name: cluster_name.clone(),
                daemon_task_definition_arn: task_definition_arn.clone(),
                status: "ACTIVE".to_string(),
                deployment_arn: deployment_arn.clone(),
                created_at: now,
                updated_at: now,
                capacity_provider_arns,
                deployment_configuration,
                propagate_tags,
                enable_ecs_managed_tags,
                enable_execute_command,
                client_token,
                tags,
                deployment_history: vec![deployment_arn.clone()],
                task_arns: Vec::new(),
            };

            s.daemons.insert(key.clone(), daemon.clone());
            s.daemon_deployments
                .insert(deployment_arn.clone(), deployment);
            let ids = spawn_daemon_tasks(s, &daemon, &principal_arn, "EC2");
            if let Some(d) = s.daemons.get_mut(&key) {
                d.task_arns = ids.clone();
            }
            let json = daemon_json(s.daemons.get(&key).unwrap());
            (daemon_arn, deployment_arn, json, ids)
        };

        if let Some(rt) = runtime {
            for id in &spawn_ids {
                rt.clone()
                    .run_task(self.state.clone(), id.clone(), account.clone());
            }
        } else {
            let mut accounts = self.state.write();
            if let Some(s) = accounts.get_mut(&account) {
                for id in &spawn_ids {
                    if let Some(t) = s.tasks.get_mut(id) {
                        t.last_status = "STOPPED".into();
                        t.desired_status = "STOPPED".into();
                        t.stop_code = Some("TaskFailedToStart".into());
                        t.stopped_reason = Some(format!(
                            "No container runtime available (docker/podman not installed). {}",
                            fakecloud_core::container_net::CONTAINER_RUNTIME_HINT
                        ));
                        t.stopped_at = Some(Utc::now());
                        for c in t.containers.iter_mut() {
                            c.last_status = "STOPPED".into();
                        }
                        if let Some(cluster) = s.clusters.get_mut(&t.cluster_name) {
                            if cluster.pending_tasks_count > 0 {
                                cluster.pending_tasks_count -= 1;
                            }
                        }
                    }
                }
            }
        }

        Ok(AwsResponse::ok_json(json!({
            "daemonArn": daemon_json.get("daemonArn").cloned().unwrap_or(json!(daemon_arn)),
            "status": "ACTIVE",
            "createdAt": now.timestamp() as f64 + now.timestamp_subsec_micros() as f64 / 1_000_000.0,
            "deploymentArn": deployment_arn,
        })))
    }

    pub(super) fn describe_daemon(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let daemon_arn = req_str(&body, "daemonArn")?.to_string();

        let accounts = self.state.read();
        let s = accounts
            .get(&request.account_id)
            .cloned()
            .unwrap_or_else(|| accounts.default_ref().clone());

        let daemon = lookup_daemon_by_arn(&s, &daemon_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ClientException",
                format!("Daemon {} not found", daemon_arn),
            )
        })?;

        Ok(AwsResponse::ok_json(json!({
            "daemon": daemon_json(&daemon),
        })))
    }

    pub(super) fn update_daemon(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let daemon_arn = req_str(&body, "daemonArn")?.to_string();
        let new_task_def = Some(req_str(&body, "daemonTaskDefinitionArn")?.to_string());
        let new_caps: Option<Vec<String>> = body
            .get("capacityProviderArns")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str().map(String::from))
                    .collect()
            });

        let now = Utc::now();
        let runtime = self.runtime.clone();
        let account = request.account_id.clone();
        let principal_arn = request
            .principal
            .as_ref()
            .map(|p| p.arn.clone())
            .unwrap_or_default();

        let (snapshot, stop_ids, spawn_ids) = {
            let mut accounts = self.state.write();
            let s = accounts.get_or_create(&account);

            let key = lookup_daemon_key_by_arn(s, &daemon_arn).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ClientException",
                    format!("Daemon {} not found", daemon_arn),
                )
            })?;

            // Mint a new deployment if task definition changed.
            let mut new_deployment_arn: Option<String> = None;
            if let Some(td_arn) = new_task_def.clone() {
                let daemon_name = s
                    .daemons
                    .get(&key)
                    .map(|d| d.daemon_name.clone())
                    .unwrap_or_default();
                let cluster_arn = s
                    .daemons
                    .get(&key)
                    .map(|d| d.cluster_arn.clone())
                    .unwrap_or_default();
                let daemon_arn = s
                    .daemons
                    .get(&key)
                    .map(|d| d.daemon_arn.clone())
                    .unwrap_or_default();
                let deployment_id = uuid::Uuid::new_v4().to_string();
                let deployment_arn = s.daemon_deployment_arn(&daemon_name, &deployment_id);
                let revision = s
                    .daemons
                    .get(&key)
                    .map(|d| d.deployment_history.len() as i64 + 1)
                    .unwrap_or(1);
                let deployment = DaemonDeployment {
                    deployment_arn: deployment_arn.clone(),
                    daemon_arn,
                    daemon_name,
                    cluster_arn,
                    task_definition_arn: td_arn,
                    status: "PRIMARY".to_string(),
                    revision,
                    created_at: now,
                    updated_at: now,
                };
                s.daemon_deployments
                    .insert(deployment_arn.clone(), deployment);
                new_deployment_arn = Some(deployment_arn);
            }

            let old_task_ids: Vec<String> = s
                .daemons
                .get(&key)
                .map(|d| d.task_arns.clone())
                .unwrap_or_default();

            {
                let daemon = s.daemons.get_mut(&key).unwrap();
                if let Some(td) = new_task_def {
                    daemon.daemon_task_definition_arn = td;
                }
                if let Some(caps) = new_caps {
                    daemon.capacity_provider_arns = caps;
                }
                if let Some(dep_arn) = new_deployment_arn {
                    daemon.deployment_arn = dep_arn.clone();
                    daemon.deployment_history.push(dep_arn);
                }
                daemon.updated_at = now;
            }

            // Re-spawn tasks when definition or providers changed.
            let ids = {
                let daemon = s.daemons.get(&key).unwrap().clone();
                spawn_daemon_tasks(s, &daemon, &principal_arn, "EC2")
            };
            if let Some(d) = s.daemons.get_mut(&key) {
                d.task_arns = ids.clone();
            }
            let snap = s.daemons.get(&key).unwrap().clone();
            (snap, old_task_ids, ids)
        };

        // Stop old tasks.
        for id in &stop_ids {
            {
                let mut accounts = self.state.write();
                if let Some(s) = accounts.get_mut(&account) {
                    if let Some(t) = s.tasks.get_mut(id) {
                        t.desired_status = "STOPPED".into();
                        t.stopping_at = Some(Utc::now());
                        if runtime.is_none() {
                            t.last_status = "STOPPED".into();
                            t.stopped_at = Some(Utc::now());
                            t.stop_code = Some("UserInitiated".into());
                            for c in t.containers.iter_mut() {
                                c.last_status = "STOPPED".into();
                            }
                            if let Some(cluster) = s.clusters.get_mut(&t.cluster_name) {
                                if cluster.pending_tasks_count > 0 {
                                    cluster.pending_tasks_count -= 1;
                                }
                            }
                        }
                    }
                }
            }
            if let Some(rt) = &runtime {
                let rt2 = rt.clone();
                let id_clone = id.clone();
                tokio::spawn(async move {
                    rt2.stop_task(&id_clone, "ECS daemon update").await;
                });
            }
        }

        if let Some(rt) = runtime {
            for id in &spawn_ids {
                rt.clone()
                    .run_task(self.state.clone(), id.clone(), account.clone());
            }
        } else {
            let mut accounts = self.state.write();
            if let Some(s) = accounts.get_mut(&account) {
                for id in &spawn_ids {
                    if let Some(t) = s.tasks.get_mut(id) {
                        t.last_status = "STOPPED".into();
                        t.desired_status = "STOPPED".into();
                        t.stop_code = Some("TaskFailedToStart".into());
                        t.stopped_reason = Some(format!(
                            "No container runtime available (docker/podman not installed). {}",
                            fakecloud_core::container_net::CONTAINER_RUNTIME_HINT
                        ));
                        t.stopped_at = Some(Utc::now());
                        for c in t.containers.iter_mut() {
                            c.last_status = "STOPPED".into();
                        }
                        if let Some(cluster) = s.clusters.get_mut(&t.cluster_name) {
                            if cluster.pending_tasks_count > 0 {
                                cluster.pending_tasks_count -= 1;
                            }
                        }
                    }
                }
            }
        }

        Ok(AwsResponse::ok_json(json!({
            "daemonArn": snapshot.daemon_arn,
            "status": snapshot.status,
            "createdAt": snapshot.created_at.timestamp() as f64
                + snapshot.created_at.timestamp_subsec_micros() as f64 / 1_000_000.0,
            "updatedAt": snapshot.updated_at.timestamp() as f64
                + snapshot.updated_at.timestamp_subsec_micros() as f64 / 1_000_000.0,
            "deploymentArn": snapshot.deployment_arn,
        })))
    }

    pub(super) fn delete_daemon(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let daemon_arn = req_str(&body, "daemonArn")?.to_string();

        let runtime = self.runtime.clone();
        let account = request.account_id.clone();
        let (snapshot, stop_ids) = {
            let mut accounts = self.state.write();
            let s = accounts.get_or_create(&account);

            let key = lookup_daemon_key_by_arn(s, &daemon_arn).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ClientException",
                    format!("Daemon {} not found", daemon_arn),
                )
            })?;
            let mut daemon = s.daemons.remove(&key).unwrap();
            daemon.status = "DRAINING".to_string();
            daemon.updated_at = Utc::now();
            let ids = daemon.task_arns.clone();
            (daemon, ids)
        };

        for id in &stop_ids {
            {
                let mut accounts = self.state.write();
                if let Some(s) = accounts.get_mut(&account) {
                    if let Some(t) = s.tasks.get_mut(id) {
                        t.desired_status = "STOPPED".into();
                        t.stopping_at = Some(Utc::now());
                        if runtime.is_none() {
                            t.last_status = "STOPPED".into();
                            t.stopped_at = Some(Utc::now());
                            t.stop_code = Some("UserInitiated".into());
                            for c in t.containers.iter_mut() {
                                c.last_status = "STOPPED".into();
                            }
                            if let Some(cluster) = s.clusters.get_mut(&t.cluster_name) {
                                if cluster.pending_tasks_count > 0 {
                                    cluster.pending_tasks_count -= 1;
                                }
                            }
                        }
                    }
                }
            }
            if let Some(rt) = &runtime {
                let rt2 = rt.clone();
                let id_clone = id.clone();
                tokio::spawn(async move {
                    rt2.stop_task(&id_clone, "ECS daemon deletion").await;
                });
            }
        }

        Ok(AwsResponse::ok_json(json!({
            "daemonArn": snapshot.daemon_arn,
            "status": snapshot.status,
            "createdAt": snapshot.created_at.timestamp() as f64
                + snapshot.created_at.timestamp_subsec_micros() as f64 / 1_000_000.0,
            "updatedAt": snapshot.updated_at.timestamp() as f64
                + snapshot.updated_at.timestamp_subsec_micros() as f64 / 1_000_000.0,
            "deploymentArn": snapshot.deployment_arn,
        })))
    }

    pub(super) fn list_daemons(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let cluster_input = body.get("clusterArn").and_then(|v| v.as_str());
        let max_results = body
            .get("maxResults")
            .and_then(|v| v.as_i64())
            .unwrap_or(100) as usize;
        let next_token = body
            .get("nextToken")
            .and_then(|v| v.as_str())
            .map(String::from);

        let accounts = self.state.read();
        let s = accounts
            .get(&request.account_id)
            .cloned()
            .unwrap_or_else(|| accounts.default_ref().clone());
        let cluster_name = cluster_input.map(cluster_arn_to_name);

        let mut summaries: Vec<Value> = Vec::new();
        for daemon in s.daemons.values() {
            if let Some(target) = &cluster_name {
                if &daemon.cluster_name != target {
                    continue;
                }
            }
            summaries.push(json!({
                "daemonArn": daemon.daemon_arn,
                "status": daemon.status,
                "createdAt": daemon.created_at.timestamp() as f64
                    + daemon.created_at.timestamp_subsec_micros() as f64 / 1_000_000.0,
                "updatedAt": daemon.updated_at.timestamp() as f64
                    + daemon.updated_at.timestamp_subsec_micros() as f64 / 1_000_000.0,
            }));
        }
        let (page, token) = paginate_checked(&summaries, next_token.as_deref(), max_results)
            .map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "Invalid nextToken".to_string(),
                )
            })?;
        Ok(AwsResponse::ok_json(json!({
            "daemonSummariesList": page,
            "nextToken": token,
        })))
    }

    pub(super) fn describe_daemon_deployments(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let arns: Vec<String> = req_array(&body, "daemonDeploymentArns")?
            .iter()
            .filter_map(|x| x.as_str())
            .map(String::from)
            .collect();
        let accounts = self.state.read();
        let s = accounts
            .get(&request.account_id)
            .cloned()
            .unwrap_or_else(|| accounts.default_ref().clone());
        let mut found = Vec::new();
        let mut failures = Vec::new();
        for arn in arns {
            if let Some(d) = s.daemon_deployments.get(&arn) {
                found.push(daemon_deployment_json(d));
            } else {
                failures.push(json!({
                    "arn": arn,
                    "reason": "MISSING",
                }));
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "daemonDeployments": found,
            "failures": failures,
        })))
    }

    pub(super) fn list_daemon_deployments(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let daemon_filter = Some(req_str(&body, "daemonArn")?.to_string());
        let cluster_input: Option<&str> = None;
        let max_results = body
            .get("maxResults")
            .and_then(|v| v.as_i64())
            .unwrap_or(100) as usize;
        let next_token = body
            .get("nextToken")
            .and_then(|v| v.as_str())
            .map(String::from);

        let accounts = self.state.read();
        let s = accounts
            .get(&request.account_id)
            .cloned()
            .unwrap_or_else(|| accounts.default_ref().clone());
        let cluster_name = cluster_input.map(cluster_arn_to_name);

        let mut summaries: Vec<Value> = Vec::new();
        for d in s.daemon_deployments.values() {
            if let Some(target) = &cluster_name {
                let dc = cluster_arn_to_name(&d.cluster_arn);
                if &dc != target {
                    continue;
                }
            }
            if let Some(filter) = &daemon_filter {
                if &d.daemon_name != filter && &d.daemon_arn != filter {
                    continue;
                }
            }
            summaries.push(daemon_deployment_summary_json(d));
        }
        let (page, token) = paginate_checked(&summaries, next_token.as_deref(), max_results)
            .map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterException",
                    "Invalid nextToken".to_string(),
                )
            })?;
        Ok(AwsResponse::ok_json(json!({
            "daemonDeployments": page,
            "nextToken": token,
        })))
    }

    pub(super) fn describe_daemon_revisions(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let arns: Vec<String> = req_array(&body, "daemonRevisionArns")?
            .iter()
            .filter_map(|x| x.as_str())
            .map(String::from)
            .collect();
        let accounts = self.state.read();
        let s = accounts
            .get(&request.account_id)
            .cloned()
            .unwrap_or_else(|| accounts.default_ref().clone());

        let mut revisions = Vec::new();
        let mut failures = Vec::new();
        for arn in arns {
            if let Some(d) = s.daemon_deployments.get(&arn) {
                revisions.push(daemon_deployment_json(d));
            } else {
                failures.push(json!({"arn": arn, "reason": "MISSING"}));
            }
        }

        Ok(AwsResponse::ok_json(json!({
            "daemonRevisions": revisions,
            "failures": failures,
        })))
    }
}

// ── Internal helpers ────────────────────────────────────────────────────

fn cluster_arn_to_name(arn_or_name: &str) -> String {
    if let Some(idx) = arn_or_name.rfind('/') {
        arn_or_name[idx + 1..].to_string()
    } else {
        arn_or_name.to_string()
    }
}

fn lookup_daemon_by_arn(s: &crate::state::EcsState, arn: &str) -> Option<Daemon> {
    s.daemons.values().find(|d| d.daemon_arn == arn).cloned()
}

fn lookup_daemon_key_by_arn(s: &crate::state::EcsState, arn: &str) -> Option<String> {
    for (k, d) in &s.daemons {
        if d.daemon_arn == arn {
            return Some(k.clone());
        }
    }
    None
}

fn lookup_daemon_task_definition(
    s: &crate::state::EcsState,
    identifier: &str,
) -> Option<DaemonTaskDefinition> {
    if let Some((family, revision)) = parse_family_revision(identifier) {
        return s
            .daemon_task_definitions
            .get(&family)
            .and_then(|m| m.get(&revision))
            .cloned();
    }
    s.daemon_task_definitions
        .values()
        .flat_map(|m| m.values())
        .find(|d| d.task_definition_arn == identifier)
        .cloned()
}

fn lookup_daemon_task_definition_mut<'a>(
    s: &'a mut crate::state::EcsState,
    identifier: &str,
) -> Option<&'a mut DaemonTaskDefinition> {
    if let Some((family, revision)) = parse_family_revision(identifier) {
        return s
            .daemon_task_definitions
            .get_mut(&family)
            .and_then(|m| m.get_mut(&revision));
    }
    for m in s.daemon_task_definitions.values_mut() {
        for d in m.values_mut() {
            if d.task_definition_arn == identifier {
                return Some(d);
            }
        }
    }
    None
}

/// Parse `family:revision` (or its trailing form within an ARN) into
/// `(family, revision)`. Returns None if the input is just a family
/// or otherwise malformed.
fn parse_family_revision(s: &str) -> Option<(String, i32)> {
    let (head, rev_str) = s.rsplit_once(':')?;
    let rev: i32 = rev_str.parse().ok()?;
    let family = match head.rsplit_once('/') {
        Some((_, f)) => f,
        None => head,
    };
    Some((family.to_string(), rev))
}

fn daemon_task_definition_json(d: &DaemonTaskDefinition) -> Value {
    json!({
        "family": d.family,
        "revision": d.revision,
        "taskDefinitionArn": d.task_definition_arn,
        "status": d.status,
        "containerDefinitions": d.container_definitions,
        "taskRoleArn": d.task_role_arn,
        "executionRoleArn": d.execution_role_arn,
        "cpu": d.cpu,
        "memory": d.memory,
        "volumes": d.volumes,
        "registeredAt": d.registered_at.timestamp() as f64
            + d.registered_at.timestamp_subsec_micros() as f64 / 1_000_000.0,
        "deregisteredAt": d.deregistered_at.map(|t|
            t.timestamp() as f64 + t.timestamp_subsec_micros() as f64 / 1_000_000.0),
        "tags": d.tags.iter().map(|t| json!({"key": t.key, "value": t.value})).collect::<Vec<_>>(),
    })
}

fn daemon_task_definition_summary_json(d: &DaemonTaskDefinition) -> Value {
    json!({
        "arn": d.task_definition_arn,
        "registeredAt": d.registered_at.timestamp() as f64
            + d.registered_at.timestamp_subsec_micros() as f64 / 1_000_000.0,
        "deleteRequestedAt": d.deregistered_at.map(|t|
            t.timestamp() as f64 + t.timestamp_subsec_micros() as f64 / 1_000_000.0),
        "status": d.status,
    })
}

fn daemon_json(d: &Daemon) -> Value {
    json!({
        "daemonName": d.daemon_name,
        "daemonArn": d.daemon_arn,
        "clusterArn": d.cluster_arn,
        "daemonTaskDefinitionArn": d.daemon_task_definition_arn,
        "status": d.status,
        "deploymentArn": d.deployment_arn,
        "createdAt": d.created_at.timestamp() as f64
            + d.created_at.timestamp_subsec_micros() as f64 / 1_000_000.0,
        "updatedAt": d.updated_at.timestamp() as f64
            + d.updated_at.timestamp_subsec_micros() as f64 / 1_000_000.0,
        "capacityProviderArns": d.capacity_provider_arns,
        "deploymentConfiguration": d.deployment_configuration,
        "propagateTags": d.propagate_tags,
        "enableECSManagedTags": d.enable_ecs_managed_tags,
        "enableExecuteCommand": d.enable_execute_command,
        "tags": d.tags.iter().map(|t| json!({"key": t.key, "value": t.value})).collect::<Vec<_>>(),
    })
}

fn daemon_deployment_json(d: &DaemonDeployment) -> Value {
    let created_at = d.created_at.timestamp() as f64
        + d.created_at.timestamp_subsec_micros() as f64 / 1_000_000.0;
    // Matches the `DaemonDeployment` Smithy shape used by
    // Describe{Daemon,DaemonDeployment,DaemonRevisions}. Implementation-only
    // fields (daemonArn, daemonName, taskDefinitionArn, revision, updatedAt)
    // are intentionally omitted to keep the response wire-compatible.
    json!({
        "daemonDeploymentArn": d.deployment_arn,
        "clusterArn": d.cluster_arn,
        "status": d.status,
        "createdAt": created_at,
        "startedAt": created_at,
    })
}

/// Shape used by ListDaemonDeployments — matches `DaemonDeploymentSummary` in
/// the ECS Smithy model. The richer `daemon_deployment_json` (used by
/// Describe*) corresponds to `DaemonDeployment` and includes additional
/// implementation-specific fields that aren't part of the summary contract.
fn daemon_deployment_summary_json(d: &DaemonDeployment) -> Value {
    let created_at = d.created_at.timestamp() as f64
        + d.created_at.timestamp_subsec_micros() as f64 / 1_000_000.0;
    json!({
        "daemonDeploymentArn": d.deployment_arn,
        "daemonArn": d.daemon_arn,
        "clusterArn": d.cluster_arn,
        "status": d.status,
        "targetDaemonRevisionArn": d.task_definition_arn,
        "createdAt": created_at,
        "startedAt": created_at,
    })
}
