// Auto-extracted from service.rs as part of carryover service.rs split.

#![allow(clippy::too_many_arguments)]

use chrono::Utc;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use super::*;

impl EcsService {
    pub(super) fn register_container_instance(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let ec2_id = opt_str(&body, "instanceIdentityDocument")
            .and_then(|s| serde_json::from_str::<Value>(s).ok())
            .and_then(|v| {
                v.get("instanceId")
                    .and_then(|x| x.as_str())
                    .map(String::from)
            });
        let tags = parse_tags(&body);

        let account = request.account_id.clone();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&account);
        let cluster_arn = state
            .clusters
            .get(&cluster_name)
            .map(|c| c.cluster_arn.clone())
            .unwrap_or_else(|| state.cluster_arn(&cluster_name));
        let uuid = uuid::Uuid::new_v4().to_string();
        let ci_arn = state.container_instance_arn(&cluster_name, &uuid);
        let key = format!("{}/{}", cluster_name, uuid);
        let ci = ContainerInstance {
            container_instance_arn: ci_arn.clone(),
            ec2_instance_id: ec2_id,
            cluster_name: cluster_name.clone(),
            cluster_arn,
            status: "ACTIVE".into(),
            version: 1,
            version_info: body.get("versionInfo").cloned(),
            agent_connected: true,
            agent_update_status: None,
            remaining_resources: body
                .get("totalResources")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default(),
            registered_resources: body
                .get("totalResources")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default(),
            running_tasks_count: 0,
            pending_tasks_count: 0,
            registered_at: Utc::now(),
            attributes: body
                .get("attributes")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|a| {
                            let name = a.get("name").and_then(|v| v.as_str())?;
                            Some(AttributeRef {
                                name: name.to_string(),
                                value: a.get("value").and_then(|v| v.as_str()).map(String::from),
                                target_type: a
                                    .get("targetType")
                                    .and_then(|v| v.as_str())
                                    .map(String::from),
                                target_id: a
                                    .get("targetId")
                                    .and_then(|v| v.as_str())
                                    .map(String::from),
                            })
                        })
                        .collect()
                })
                .unwrap_or_default(),
            tags,
            capacity_provider_name: None,
            health_status: None,
        };
        state.container_instances.insert(key, ci.clone());
        if let Some(cluster) = state.clusters.get_mut(&cluster_name) {
            cluster.registered_container_instances_count += 1;
        }
        Ok(AwsResponse::ok_json(json!({
            "containerInstance": container_instance_to_json(&ci),
        })))
    }

    pub(super) fn deregister_container_instance(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let ci_ref = req_str(&body, "containerInstance")?.to_string();
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let id = container_instance_id_from_ref(&ci_ref);
        let key = format!("{}/{}", cluster_name, id);

        let account = request.account_id.clone();
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&account)
            .ok_or_else(|| container_instance_not_found(&ci_ref))?;
        let mut ci = state
            .container_instances
            .remove(&key)
            .ok_or_else(|| container_instance_not_found(&ci_ref))?;
        ci.status = "INACTIVE".into();
        if let Some(cluster) = state.clusters.get_mut(&cluster_name) {
            if cluster.registered_container_instances_count > 0 {
                cluster.registered_container_instances_count -= 1;
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "containerInstance": container_instance_to_json(&ci),
        })))
    }

    pub(super) fn describe_container_instances(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let refs: Vec<String> = req_array(&body, "containerInstances")?
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();

        let accounts = self.state.read();
        let mut found = Vec::new();
        let mut failures = Vec::new();
        if let Some(state) = accounts.get(&request.account_id) {
            for r in &refs {
                let id = container_instance_id_from_ref(r);
                let key = format!("{}/{}", cluster_name, id);
                match state.container_instances.get(&key) {
                    Some(ci) => found.push(container_instance_to_json(ci)),
                    None => failures.push(json!({"arn": r, "reason": "MISSING"})),
                }
            }
        } else {
            for r in &refs {
                failures.push(json!({"arn": r, "reason": "MISSING"}));
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "containerInstances": found,
            "failures": failures,
        })))
    }

    pub(super) fn list_container_instances(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_enum_opt(
            &body,
            "status",
            &[
                "ACTIVE",
                "DRAINING",
                "REGISTERING",
                "DEREGISTERING",
                "REGISTRATION_FAILED",
            ],
        )?;
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let status_filter = opt_str(&body, "status");
        let max_results = body
            .get("maxResults")
            .and_then(|v| v.as_i64())
            .filter(|n| (1..=100).contains(n))
            .map(|n| n as usize)
            .unwrap_or(100);
        let next_token = opt_str(&body, "nextToken").unwrap_or("");

        let accounts = self.state.read();
        let mut arns: Vec<String> = match accounts.get(&request.account_id) {
            Some(state) => state
                .container_instances
                .values()
                .filter(|ci| ci.cluster_name == cluster_name)
                .filter(|ci| status_filter.is_none_or(|s| ci.status == s))
                .map(|ci| ci.container_instance_arn.clone())
                .collect(),
            None => Vec::new(),
        };
        arns.sort();
        let (page, next) = paginate_arns(arns, next_token, max_results);
        let mut out = json!({"containerInstanceArns": page});
        if let Some(n) = next {
            out.as_object_mut()
                .unwrap()
                .insert("nextToken".into(), json!(n));
        }
        Ok(AwsResponse::ok_json(out))
    }

    pub(super) fn update_container_agent(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let ci_ref = req_str(&body, "containerInstance")?.to_string();
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let id = container_instance_id_from_ref(&ci_ref);
        let key = format!("{}/{}", cluster_name, id);
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&request.account_id)
            .ok_or_else(|| container_instance_not_found(&ci_ref))?;
        let ci = state
            .container_instances
            .get_mut(&key)
            .ok_or_else(|| container_instance_not_found(&ci_ref))?;
        ci.agent_update_status = Some("UPDATED".into());
        Ok(AwsResponse::ok_json(json!({
            "containerInstance": container_instance_to_json(ci),
        })))
    }

    pub(super) fn update_container_instances_state(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        validate_enum_opt(
            &body,
            "status",
            &[
                "ACTIVE",
                "DRAINING",
                "REGISTERING",
                "DEREGISTERING",
                "REGISTRATION_FAILED",
            ],
        )?;
        let status = req_str(&body, "status")?.to_string();
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let refs: Vec<String> = req_array(&body, "containerInstances")?
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&request.account_id)
            .ok_or_else(|| client_exception("account not found"))?;
        let mut found = Vec::new();
        let mut failures = Vec::new();
        for r in &refs {
            let id = container_instance_id_from_ref(r);
            let key = format!("{}/{}", cluster_name, id);
            match state.container_instances.get_mut(&key) {
                Some(ci) => {
                    ci.status = status.clone();
                    found.push(container_instance_to_json(ci));
                }
                None => failures.push(json!({"arn": r, "reason": "MISSING"})),
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "containerInstances": found,
            "failures": failures,
        })))
    }

    pub(super) fn put_attributes(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let attrs = req_array(&body, "attributes")?.clone();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let mut stored = Vec::new();
        for a in &attrs {
            let Some(name) = a.get("name").and_then(|v| v.as_str()) else {
                continue;
            };
            let target_type = a
                .get("targetType")
                .and_then(|v| v.as_str())
                .unwrap_or("container-instance")
                .to_string();
            let target_id = a
                .get("targetId")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let value = a.get("value").and_then(|v| v.as_str()).map(String::from);
            let key = format!("{}/{}/{}", cluster_name, target_id, name);
            let attr = Attribute {
                cluster_name: cluster_name.clone(),
                target_type: target_type.clone(),
                target_id: target_id.clone(),
                name: name.to_string(),
                value: value.clone(),
            };
            state.attributes.insert(key, attr);
            // Container-instance attributes must also surface in
            // DescribeContainerInstances, which reads ContainerInstance.attributes.
            if target_type == "container-instance" {
                let ci_key = format!(
                    "{}/{}",
                    cluster_name,
                    container_instance_id_from_ref(&target_id)
                );
                if let Some(ci) = state.container_instances.get_mut(&ci_key) {
                    if let Some(existing) = ci.attributes.iter_mut().find(|r| r.name == name) {
                        existing.value = value.clone();
                        existing.target_type = Some(target_type.clone());
                        existing.target_id = Some(target_id.clone());
                    } else {
                        ci.attributes.push(AttributeRef {
                            name: name.to_string(),
                            value: value.clone(),
                            target_type: Some(target_type.clone()),
                            target_id: Some(target_id.clone()),
                        });
                    }
                }
            }
            stored.push(json!({
                "name": name,
                "value": value,
                "targetType": target_type,
                "targetId": target_id,
            }));
        }
        Ok(AwsResponse::ok_json(json!({"attributes": stored})))
    }

    pub(super) fn delete_attributes(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let attrs = req_array(&body, "attributes")?.clone();
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let mut deleted = Vec::new();
        for a in &attrs {
            let Some(name) = a.get("name").and_then(|v| v.as_str()) else {
                continue;
            };
            let target_id = a.get("targetId").and_then(|v| v.as_str()).unwrap_or("");
            let key = format!("{}/{}/{}", cluster_name, target_id, name);
            if let Some(attr) = state.attributes.remove(&key) {
                if attr.target_type == "container-instance" {
                    let ci_key = format!(
                        "{}/{}",
                        cluster_name,
                        container_instance_id_from_ref(target_id)
                    );
                    if let Some(ci) = state.container_instances.get_mut(&ci_key) {
                        ci.attributes.retain(|r| r.name != name);
                    }
                }
                deleted.push(json!({
                    "name": attr.name,
                    "value": attr.value,
                    "targetType": attr.target_type,
                    "targetId": attr.target_id,
                }));
            }
        }
        Ok(AwsResponse::ok_json(json!({"attributes": deleted})))
    }

    pub(super) fn list_attributes(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        validate_enum_opt(&body, "targetType", &["container-instance"])?;
        let target_type = req_str(&body, "targetType")?.to_string();
        let attr_name = opt_str(&body, "attributeName");
        let attr_value = opt_str(&body, "attributeValue");

        let accounts = self.state.read();
        let attrs: Vec<Value> = match accounts.get(&request.account_id) {
            Some(state) => state
                .attributes
                .values()
                .filter(|a| a.cluster_name == cluster_name)
                .filter(|a| a.target_type == target_type)
                .filter(|a| attr_name.is_none_or(|n| a.name == n))
                .filter(|a| attr_value.is_none_or(|v| a.value.as_deref() == Some(v)))
                .map(|a| {
                    json!({
                        "name": a.name,
                        "value": a.value,
                        "targetType": a.target_type,
                        "targetId": a.target_id,
                    })
                })
                .collect(),
            None => Vec::new(),
        };
        Ok(AwsResponse::ok_json(json!({"attributes": attrs})))
    }

    pub(super) fn create_capacity_provider(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let name = req_str(&body, "name")?.to_string();
        if name.starts_with("aws") || name.starts_with("ecs") {
            return Err(invalid_parameter(format!(
                "Capacity provider name cannot begin with 'aws' or 'ecs': {name}"
            )));
        }
        let auto_scaling_group_provider = body.get("autoScalingGroupProvider").cloned();
        let tags = parse_tags(&body);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        if state.capacity_providers.contains_key(&name) {
            return Err(client_exception(format!(
                "Capacity provider already exists: {name}"
            )));
        }
        let arn = format!(
            "arn:aws:ecs:{}:{}:capacity-provider/{}",
            state.region, state.account_id, name
        );
        let cp = CapacityProvider {
            name: name.clone(),
            arn,
            status: "ACTIVE".into(),
            auto_scaling_group_provider,
            update_status: None,
            update_status_reason: None,
            created_at: Utc::now(),
            tags,
        };
        state.capacity_providers.insert(name.clone(), cp.clone());
        Ok(AwsResponse::ok_json(json!({
            "capacityProvider": capacity_provider_to_json(&cp),
        })))
    }

    pub(super) fn delete_capacity_provider(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let input = req_str(&body, "capacityProvider")?.to_string();
        let name = capacity_provider_name_from_ref(&input);
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&request.account_id)
            .ok_or_else(|| capacity_provider_not_found(&name))?;
        let mut cp = state
            .capacity_providers
            .remove(&name)
            .ok_or_else(|| capacity_provider_not_found(&name))?;
        cp.status = "INACTIVE".into();
        Ok(AwsResponse::ok_json(json!({
            "capacityProvider": capacity_provider_to_json(&cp),
        })))
    }

    pub(super) fn describe_capacity_providers(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let names: Vec<String> = body
            .get("capacityProviders")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(capacity_provider_name_from_ref))
                    .collect()
            })
            .unwrap_or_default();
        let accounts = self.state.read();
        let mut found = Vec::new();
        let mut failures = Vec::new();
        if let Some(state) = accounts.get(&request.account_id) {
            if names.is_empty() {
                for cp in state.capacity_providers.values() {
                    found.push(capacity_provider_to_json(cp));
                }
            } else {
                for n in &names {
                    match state.capacity_providers.get(n) {
                        Some(cp) => found.push(capacity_provider_to_json(cp)),
                        None => failures.push(json!({"arn": n, "reason": "MISSING"})),
                    }
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "capacityProviders": found,
            "failures": failures,
        })))
    }

    pub(super) fn update_capacity_provider(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let input = req_str(&body, "name")?.to_string();
        let name = capacity_provider_name_from_ref(&input);
        let asg = body.get("autoScalingGroupProvider").cloned();
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&request.account_id)
            .ok_or_else(|| capacity_provider_not_found(&name))?;
        let cp = state
            .capacity_providers
            .get_mut(&name)
            .ok_or_else(|| capacity_provider_not_found(&name))?;
        if let Some(v) = asg {
            cp.auto_scaling_group_provider = Some(v);
        }
        cp.update_status = Some("UPDATE_COMPLETE".into());
        Ok(AwsResponse::ok_json(json!({
            "capacityProvider": capacity_provider_to_json(cp),
        })))
    }

    pub(super) fn get_task_protection(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let _cluster = req_str(&body, "cluster")?;
        let refs: Vec<String> = body
            .get("tasks")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let accounts = self.state.read();
        let mut protections = Vec::new();
        let mut failures = Vec::new();
        if let Some(state) = accounts.get(&request.account_id) {
            for r in &refs {
                let id = task_id_from_ref(r);
                match state.tasks.get(&id) {
                    Some(t) => protections.push(task_protection_json(t)),
                    None => failures.push(json!({"arn": r, "reason": "MISSING"})),
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "protectedTasks": protections,
            "failures": failures,
        })))
    }

    pub(super) fn update_task_protection(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let _cluster = req_str(&body, "cluster")?;
        let refs: Vec<String> = req_array(&body, "tasks")?
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        let protect = req_bool(&body, "protectionEnabled")?;
        let expires_in_minutes = body
            .get("expiresInMinutes")
            .and_then(|v| v.as_i64())
            .unwrap_or(2880);
        let expiration = if protect {
            Some(Utc::now() + chrono::Duration::minutes(expires_in_minutes))
        } else {
            None
        };

        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&request.account_id)
            .ok_or_else(|| client_exception("account not found"))?;
        let mut protections = Vec::new();
        let mut failures = Vec::new();
        for r in &refs {
            let id = task_id_from_ref(r);
            match state.tasks.get_mut(&id) {
                Some(t) => {
                    t.protection = Some(crate::state::TaskProtection {
                        enabled: protect,
                        expiration,
                    });
                    protections.push(task_protection_json(t));
                }
                None => failures.push(json!({"arn": r, "reason": "MISSING"})),
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "protectedTasks": protections,
            "failures": failures,
        })))
    }

    pub(super) fn create_task_set(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let service_ref = req_str(&body, "service")?;
        let service_name = service_name_from_ref(service_ref);
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let task_definition = req_str(&body, "taskDefinition")?.to_string();
        let external_id = opt_str(&body, "externalId").map(String::from);
        let launch_type = opt_str(&body, "launchType").map(String::from);
        let platform_version = opt_str(&body, "platformVersion").map(String::from);
        let scale = body.get("scale").cloned();
        let tags = parse_tags(&body);
        let load_balancers = body
            .get("loadBalancers")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let service_registries = body
            .get("serviceRegistries")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let capacity_provider_strategy = body
            .get("capacityProviderStrategy")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&request.account_id)
            .ok_or_else(|| service_not_found(&service_name))?;
        let service_key = EcsState::service_key(&cluster_name, &service_name);
        let svc = state
            .services
            .get(&service_key)
            .ok_or_else(|| service_not_found(&service_name))?;
        if !matches!(
            svc.deployment_controller.as_str(),
            "EXTERNAL" | "CODE_DEPLOY"
        ) {
            return Err(client_exception(
                "CreateTaskSet requires the service to be created with \
                 deploymentController.type = EXTERNAL or CODE_DEPLOY",
            ));
        }
        let ts_id = format!("ecs-svc-{}", uuid::Uuid::new_v4().simple());
        let task_set = TaskSet {
            task_set_id: ts_id.clone(),
            task_set_arn: format!(
                "arn:aws:ecs:{}:{}:task-set/{}/{}/{}",
                state.region, state.account_id, cluster_name, service_name, ts_id
            ),
            service_arn: svc.service_arn.clone(),
            cluster_arn: svc.cluster_arn.clone(),
            service_name: service_name.clone(),
            cluster_name: cluster_name.clone(),
            external_id,
            status: "ACTIVE".into(),
            task_definition,
            computed_desired_count: 0,
            pending_count: 0,
            running_count: 0,
            launch_type,
            platform_version,
            scale,
            stability_status: "STABILIZING".into(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            load_balancers,
            service_registries,
            capacity_provider_strategy,
            tags,
        };
        let key = format!("{}/{}/{}", cluster_name, service_name, ts_id);
        state.task_sets.insert(key, task_set.clone());
        Ok(AwsResponse::ok_json(json!({
            "taskSet": task_set_to_json(&task_set),
        })))
    }

    pub(super) fn update_task_set(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let ts_ref = req_str(&body, "taskSet")?.to_string();
        let service_ref = req_str(&body, "service")?;
        let service_name = service_name_from_ref(service_ref);
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let scale = body.get("scale").cloned();

        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&request.account_id)
            .ok_or_else(|| client_exception("task set not found"))?;
        let ts_id = task_set_id_from_ref(&ts_ref);
        let key = format!("{}/{}/{}", cluster_name, service_name, ts_id);
        let ts = state
            .task_sets
            .get_mut(&key)
            .ok_or_else(|| client_exception(format!("task set not found: {}", ts_ref)))?;
        if let Some(v) = scale {
            ts.scale = Some(v);
        }
        ts.updated_at = Utc::now();
        Ok(AwsResponse::ok_json(json!({
            "taskSet": task_set_to_json(ts),
        })))
    }

    pub(super) fn delete_task_set(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let ts_ref = req_str(&body, "taskSet")?.to_string();
        let service_ref = req_str(&body, "service")?;
        let service_name = service_name_from_ref(service_ref);
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let ts_id = task_set_id_from_ref(&ts_ref);
        let key = format!("{}/{}/{}", cluster_name, service_name, ts_id);

        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&request.account_id)
            .ok_or_else(|| client_exception("task set not found"))?;
        let mut ts = state
            .task_sets
            .remove(&key)
            .ok_or_else(|| client_exception(format!("task set not found: {}", ts_ref)))?;
        ts.status = "DRAINING".into();
        Ok(AwsResponse::ok_json(json!({
            "taskSet": task_set_to_json(&ts),
        })))
    }

    pub(super) fn describe_task_sets(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let service_ref = req_str(&body, "service")?;
        let service_name = service_name_from_ref(service_ref);
        let cluster_ref = req_str(&body, "cluster")?;
        let cluster_name = EcsState::resolve_cluster_name(Some(cluster_ref));
        let filter_refs: Vec<String> = body
            .get("taskSets")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();

        let accounts = self.state.read();
        let mut found = Vec::new();
        let mut failures = Vec::new();
        {
            // AWS validates the referenced cluster + service exist, returning
            // ClusterNotFoundException / ServiceNotFoundException rather than an
            // empty task-set list.
            let state = accounts
                .get(&request.account_id)
                .ok_or_else(|| service_not_found(&service_name))?;
            if !state.clusters.contains_key(&cluster_name) {
                return Err(cluster_not_found(&cluster_name));
            }
            let svc_key = EcsState::service_key(&cluster_name, &service_name);
            if !state.services.contains_key(&svc_key) {
                return Err(service_not_found(&service_name));
            }
        }
        if let Some(state) = accounts.get(&request.account_id) {
            if filter_refs.is_empty() {
                for ts in state.task_sets.values() {
                    if ts.cluster_name == cluster_name && ts.service_name == service_name {
                        found.push(task_set_to_json(ts));
                    }
                }
            } else {
                for r in &filter_refs {
                    let id = task_set_id_from_ref(r);
                    let key = format!("{}/{}/{}", cluster_name, service_name, id);
                    match state.task_sets.get(&key) {
                        Some(ts) => found.push(task_set_to_json(ts)),
                        None => failures.push(json!({"arn": r, "reason": "MISSING"})),
                    }
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "taskSets": found,
            "failures": failures,
        })))
    }

    pub(super) fn update_service_primary_task_set(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let ts_ref = req_str(&body, "primaryTaskSet")?.to_string();
        let service_ref = req_str(&body, "service")?;
        let service_name = service_name_from_ref(service_ref);
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let ts_id = task_set_id_from_ref(&ts_ref);
        let key = format!("{}/{}/{}", cluster_name, service_name, ts_id);
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&request.account_id)
            .ok_or_else(|| client_exception("task set not found"))?;
        if !state.task_sets.contains_key(&key) {
            return Err(client_exception(format!("task set not found: {}", ts_ref)));
        }
        // Demote any existing PRIMARY on this service to ACTIVE before
        // promoting the new one. Otherwise the service would be left with
        // two PRIMARY task sets, which AWS forbids.
        for ts in state.task_sets.values_mut() {
            if ts.service_name == service_name
                && ts.cluster_name == cluster_name
                && ts.status == "PRIMARY"
                && ts.task_set_id != ts_id
            {
                ts.status = "ACTIVE".into();
                ts.updated_at = Utc::now();
            }
        }
        let ts = state.task_sets.get_mut(&key).unwrap();
        ts.status = "PRIMARY".into();
        ts.updated_at = Utc::now();
        Ok(AwsResponse::ok_json(json!({
            "taskSet": task_set_to_json(ts),
        })))
    }

    pub(super) async fn execute_command(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let task_ref = req_str(&body, "task")?.to_string();
        let command = req_str(&body, "command")?.to_string();
        let interactive = body
            .get("interactive")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        // Resolve runtime container id and gate on the task's
        // enableExecuteCommand flag. AWS rejects ExecuteCommand on
        // tasks that didn't opt in (whether launched directly via
        // RunTask or spawned by a service with enableExecuteCommand=false)
        // with `InvalidParameterException`.
        let container_id = {
            let accounts = self.state.read();
            let state = accounts
                .get(&request.account_id)
                .ok_or_else(|| task_not_found(&task_ref))?;
            let id = task_id_from_ref(&task_ref);
            let task = state
                .tasks
                .get(&id)
                .ok_or_else(|| task_not_found(&task_ref))?;
            if !task.enable_execute_command {
                return Err(invalid_parameter(format!(
                    "The execute command failed because execute command was not enabled when the task ({}) was run or the task is not running.",
                    task.task_arn
                )));
            }
            task.containers.first().and_then(|c| c.runtime_id.clone())
        };

        let session_id = format!("ecs-execute-command-{}", uuid::Uuid::new_v4());
        if let (Some(id), Some(rt)) = (container_id.clone(), self.runtime.as_ref()) {
            // Best-effort proxy: shell the command through `<cli> exec`. We
            // don't stream back stdout/stderr in this ExecuteCommand response
            // (real AWS returns a Session token for the SSM sidecar), so log
            // the result server-side for visibility. Use the runtime's
            // detected CLI (docker or podman) rather than hardcoding docker
            // so podman-only hosts work too (issue #1539 family, bug 0.5).
            let out = tokio::process::Command::new(rt.cli_name())
                .args(["exec", &id, "sh", "-c", &command])
                .output()
                .await
                .map_err(|e| client_exception(format!("docker exec failed: {e}")))?;
            tracing::info!(
                task = %task_ref,
                exit = out.status.code().unwrap_or(-1),
                "ExecuteCommand via docker exec"
            );
        }

        Ok(AwsResponse::ok_json(json!({
            "clusterArn": opt_str(&body, "cluster").unwrap_or(""),
            "containerArn": container_id.unwrap_or_default(),
            "containerName": opt_str(&body, "container").unwrap_or(""),
            "interactive": interactive,
            "session": {
                "sessionId": session_id,
                "streamUrl": "",
                "tokenValue": "",
            },
            "taskArn": task_ref,
        })))
    }

    pub(super) fn submit_container_state_change(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Agent-side API: record whatever the agent tells us about the
        // container. We already drive state internally via the runtime,
        // so this is an idempotent ack that updates the in-memory copy
        // when the named task+container exist.
        let body = request.json_body();
        let task_ref = opt_str(&body, "task").unwrap_or("");
        let container_name = opt_str(&body, "containerName").unwrap_or("");
        let status = opt_str(&body, "status").map(String::from);
        let exit_code = body.get("exitCode").and_then(|v| v.as_i64());
        let reason = opt_str(&body, "reason").map(String::from);

        if !task_ref.is_empty() {
            let mut accounts = self.state.write();
            if let Some(state) = accounts.get_mut(&request.account_id) {
                let id = task_id_from_ref(task_ref);
                if let Some(task) = state.tasks.get_mut(&id) {
                    if let Some(container) = task
                        .containers
                        .iter_mut()
                        .find(|c| c.name == container_name)
                    {
                        if let Some(s) = status {
                            container.last_status = s;
                        }
                        if let Some(code) = exit_code {
                            container.exit_code = Some(code);
                        }
                        if let Some(r) = reason {
                            container.reason = Some(r);
                        }
                    }
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({"acknowledgment": "OK"})))
    }

    pub(super) fn submit_task_state_change(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let task_ref = opt_str(&body, "task").unwrap_or("");
        let status = opt_str(&body, "status").map(String::from);
        if !task_ref.is_empty() {
            let mut accounts = self.state.write();
            if let Some(state) = accounts.get_mut(&request.account_id) {
                let id = task_id_from_ref(task_ref);
                if let Some(task) = state.tasks.get_mut(&id) {
                    if let Some(s) = status {
                        task.last_status = s;
                    }
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({"acknowledgment": "OK"})))
    }

    pub(super) fn submit_attachment_state_changes(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let _attachments = req_array(&body, "attachments")?;
        // Attachments (ENIs) are beyond what fakecloud models today.
        // Ack the report so agents don't retry in a tight loop.
        Ok(AwsResponse::ok_json(json!({"acknowledgment": "OK"})))
    }

    pub(super) fn discover_poll_endpoint(
        &self,
        _request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // ECS agents use this to discover the long-poll + telemetry
        // endpoints. Point both at fakecloud's local listener; real
        // agent polling isn't wired, but the shape is correct.
        let accounts = self.state.read();
        let endpoint = format!("https://ecs.{}.amazonaws.com/", accounts.region());
        Ok(AwsResponse::ok_json(json!({
            "endpoint": endpoint,
            "telemetryEndpoint": endpoint,
            "serviceConnectEndpoint": endpoint,
        })))
    }

    pub(super) fn stop_service_deployment(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let deployment_ref = req_str(&body, "serviceDeploymentArn")?.to_string();
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&request.account_id)
            .ok_or_else(|| client_exception("service deployment not found"))?;
        for svc in state.services.values_mut() {
            for d in svc.deployments.iter_mut() {
                if deployment_ref.contains(&d.deployment_id) {
                    d.status = "STOPPED".into();
                    d.rollout_state = "FAILED".into();
                    d.rollout_state_reason = Some("StopServiceDeployment requested".into());
                    d.updated_at = Utc::now();
                    return Ok(AwsResponse::ok_json(json!({
                        "serviceDeployment": deployment_to_json(d),
                    })));
                }
            }
        }
        Err(client_exception(format!(
            "service deployment not found: {deployment_ref}"
        )))
    }

    /// Continue or roll back a service deployment that is paused at a PAUSE
    /// lifecycle hook. `CONTINUE` resolves the hook and lets the deployment
    /// complete; `ROLLBACK` reverts the deployment.
    pub(super) fn continue_service_deployment(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let deployment_ref = req_str(&body, "serviceDeploymentArn")?.to_string();
        let hook_id = req_str(&body, "hookId")?.to_string();
        let action = req_str(&body, "action")?.to_string();
        if action != "CONTINUE" && action != "ROLLBACK" {
            return Err(invalid_parameter(format!(
                "Invalid action '{action}': must be CONTINUE or ROLLBACK"
            )));
        }
        let mut accounts = self.state.write();
        let state = accounts
            .get_mut(&request.account_id)
            .ok_or_else(|| service_deployment_not_found(&deployment_ref))?;
        for svc in state.services.values_mut() {
            for d in svc.deployments.iter_mut() {
                if deployment_ref.contains(&d.deployment_id) {
                    match d.pending_hook_id.as_deref() {
                        Some(pending) if pending == hook_id => {}
                        _ => {
                            return Err(invalid_parameter(format!(
                                "Service deployment {deployment_ref} has no paused hook {hook_id}"
                            )));
                        }
                    }
                    d.pending_hook_id = None;
                    d.lifecycle_stage = None;
                    d.updated_at = Utc::now();
                    if action == "CONTINUE" {
                        d.rollout_state = "COMPLETED".into();
                        d.rollout_state_reason =
                            Some("Continued via ContinueServiceDeployment.".into());
                    } else {
                        d.status = "STOPPED".into();
                        d.rollout_state = "ROLLBACK_COMPLETED".into();
                        d.rollout_state_reason =
                            Some("Rolled back via ContinueServiceDeployment.".into());
                    }
                    return Ok(AwsResponse::ok_json(json!({
                        "serviceDeploymentArn": deployment_ref,
                    })));
                }
            }
        }
        Err(service_deployment_not_found(&deployment_ref))
    }

    pub(super) fn list_service_deployments(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let service_ref = req_str(&body, "service")?;
        let service_name = service_name_from_ref(service_ref);
        let cluster_ref = opt_str(&body, "cluster");
        let cluster_name = EcsState::resolve_cluster_name(cluster_ref);
        let accounts = self.state.read();
        let mut deployments: Vec<Value> = Vec::new();
        if let Some(state) = accounts.get(&request.account_id) {
            let key = EcsState::service_key(&cluster_name, &service_name);
            if let Some(svc) = state.services.get(&key) {
                for d in &svc.deployments {
                    deployments.push(json!({
                        "serviceDeploymentArn": format!("{}/{}", svc.service_arn, d.deployment_id),
                        "serviceArn": svc.service_arn,
                        "clusterArn": svc.cluster_arn,
                        "status": d.status,
                        "createdAt": d.created_at.timestamp(),
                        "startedAt": d.created_at.timestamp(),
                        "finishedAt": null,
                    }));
                }
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "serviceDeployments": deployments,
        })))
    }

    pub(super) fn describe_service_deployments(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let refs: Vec<String> = req_array(&body, "serviceDeploymentArns")?
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        let accounts = self.state.read();
        let mut found = Vec::new();
        let mut failures = Vec::new();
        if let Some(state) = accounts.get(&request.account_id) {
            'next_ref: for r in &refs {
                for svc in state.services.values() {
                    for d in &svc.deployments {
                        if r.contains(&d.deployment_id) {
                            found.push(json!({
                                "serviceDeploymentArn": r,
                                "serviceArn": svc.service_arn,
                                "clusterArn": svc.cluster_arn,
                                "status": d.status,
                                "createdAt": d.created_at.timestamp(),
                                "startedAt": d.created_at.timestamp(),
                                "finishedAt": null,
                                "deploymentConfiguration": {
                                    "minimumHealthyPercent": svc.minimum_healthy_percent,
                                    "maximumPercent": svc.maximum_percent,
                                },
                                "sourceServiceRevisions": [],
                                "targetServiceRevision": {
                                    "arn": d.task_definition_arn,
                                    "requestedTaskCount": d.desired_count,
                                    "runningTaskCount": d.running_count,
                                    "pendingTaskCount": d.pending_count,
                                    "failedTasks": d.failed_tasks,
                                },
                                "lifecycleStage": d.lifecycle_stage,
                                "lifecycleHookDetails": lifecycle_hook_details_json(d),
                            }));
                            continue 'next_ref;
                        }
                    }
                }
                failures.push(json!({"arn": r, "reason": "MISSING"}));
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "serviceDeployments": found,
            "failures": failures,
        })))
    }

    pub(super) fn describe_service_revisions(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = request.json_body();
        let refs: Vec<String> = req_array(&body, "serviceRevisionArns")?
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect();
        let account = request.account_id.clone();
        let accounts = self.state.read();
        let state = accounts.get(&account);
        let mut found = Vec::new();
        let mut failures = Vec::new();
        for r in &refs {
            match state.and_then(|s| s.service_revisions.get(r)) {
                Some(rev) => found.push(json!({
                    "serviceRevisionArn": rev.service_revision_arn,
                    "serviceArn": rev.service_arn,
                    "clusterArn": rev.cluster_arn,
                    "taskDefinition": rev.task_definition_arn,
                    "launchType": rev.launch_type,
                    "createdAt": rev.created_at.timestamp(),
                })),
                None => failures.push(json!({"arn": r, "reason": "MISSING"})),
            }
        }
        Ok(AwsResponse::ok_json(json!({
            "serviceRevisions": found,
            "failures": failures,
        })))
    }
}
