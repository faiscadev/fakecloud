//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `ecs`.

use super::*;

impl ResourceProvisioner {
    // --- ECS ---

    pub(super) fn create_ecs_cluster(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let generated_name = self.physical_name(resource);
        let cluster_name = props
            .get("ClusterName")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name)
            .to_string();
        let cluster_arn = self
            .ecs_state
            .write()
            .get_or_create(&self.account_id)
            .cluster_arn(&self.region, &cluster_name);
        let mut cluster = EcsCluster::new(&cluster_name, cluster_arn.clone());
        cluster.tags = parse_ecs_tags(props.get("Tags"));
        cluster.capacity_providers = props
            .get("CapacityProviders")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        if let Some(strategy) = props
            .get("DefaultCapacityProviderStrategy")
            .and_then(|v| v.as_array())
        {
            cluster.default_capacity_provider_strategy =
                strategy.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(cfg) = props.get("Configuration") {
            cluster.configuration = Some(lowercase_first_keys(cfg.clone()));
        }
        if let Some(settings) = props.get("ClusterSettings").and_then(|v| v.as_array()) {
            cluster.settings = settings.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(scd) = props.get("ServiceConnectDefaults") {
            cluster.service_connect_defaults = Some(lowercase_first_keys(scd.clone()));
        }
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.clusters.insert(cluster_name.clone(), cluster);
        Ok(ProvisionResult::new(cluster_name).with("Arn", cluster_arn))
    }

    pub(super) fn delete_ecs_cluster(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.clusters.remove(physical_id);
        // Cascade: drop services + tasks tied to this cluster.
        state.services.retain(|_, s| s.cluster_name != physical_id);
        state
            .tasks
            .retain(|_, t| t.cluster_arn.split('/').next_back() != Some(physical_id));
        Ok(())
    }

    /// The same role checks `RegisterTaskDefinition` runs: a role whose trust
    /// policy lets ECS tasks assume it, in the stack's account under IAM
    /// enforcement.
    fn validate_ecs_task_role(&self, role_arn: &str) -> Result<(), String> {
        let validator =
            fakecloud_iam::pass_role::IamRoleTrustValidator::new(self.iam_state.clone());
        fakecloud_ecs::validate_task_role(
            &self.account_id,
            role_arn,
            Some(&validator),
            self.iam_mode,
        )
        .map_err(|e| e.to_string())
    }

    pub(super) fn create_ecs_task_definition(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let generated_name = self.physical_name(resource);
        let family = props
            .get("Family")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name)
            .to_string();
        // ECS DescribeTaskDefinition emits camelCase keys; CFN ships PascalCase.
        // Recursively lower the leading char so SDKs deserialize cleanly.
        let container_definitions: Vec<serde_json::Value> = props
            .get("ContainerDefinitions")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(lowercase_first_keys)
            .collect();
        let task_role_arn = props
            .get("TaskRoleArn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let execution_role_arn = props
            .get("ExecutionRoleArn")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        for role_arn in [&task_role_arn, &execution_role_arn].into_iter().flatten() {
            self.validate_ecs_task_role(role_arn)?;
        }
        let network_mode = props
            .get("NetworkMode")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let requires_compatibilities: Vec<String> = props
            .get("RequiresCompatibilities")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let cpu = props
            .get("Cpu")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let memory = props
            .get("Memory")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let volumes: Vec<serde_json::Value> = props
            .get("Volumes")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(lowercase_first_keys)
            .collect();
        let placement_constraints: Vec<serde_json::Value> = props
            .get("PlacementConstraints")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(lowercase_first_keys)
            .collect();
        let proxy_configuration = props
            .get("ProxyConfiguration")
            .cloned()
            .map(lowercase_first_keys);
        let ephemeral_storage = props
            .get("EphemeralStorage")
            .cloned()
            .map(lowercase_first_keys);
        let runtime_platform = props
            .get("RuntimePlatform")
            .cloned()
            .map(lowercase_first_keys);
        let tags = parse_ecs_tags(props.get("Tags"));

        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let revision = state
            .next_revision
            .entry(family.clone())
            .and_modify(|n| *n += 1)
            .or_insert(1);
        let revision = *revision;
        let arn = state.task_definition_arn(&self.region, &family, revision);
        let td = EcsTaskDefinition {
            family: family.clone(),
            revision,
            task_definition_arn: arn.clone(),
            container_definitions,
            status: "ACTIVE".to_string(),
            task_role_arn,
            execution_role_arn,
            network_mode,
            requires_compatibilities: requires_compatibilities.clone(),
            compatibilities: requires_compatibilities,
            cpu,
            memory,
            pid_mode: None,
            ipc_mode: None,
            volumes,
            placement_constraints,
            proxy_configuration,
            inference_accelerators: Vec::new(),
            ephemeral_storage,
            runtime_platform,
            requires_attributes: Vec::new(),
            registered_at: Utc::now(),
            registered_by: None,
            deregistered_at: None,
            tags,
            enable_fault_injection: props.get("EnableFaultInjection").and_then(|v| v.as_bool()),
        };
        state
            .task_definitions
            .entry(family.clone())
            .or_default()
            .insert(revision, td);
        Ok(ProvisionResult::new(arn.clone()).with("TaskDefinitionArn", arn))
    }

    pub(super) fn delete_ecs_task_definition(&self, physical_id: &str) -> Result<(), String> {
        // physical_id is the full task-definition ARN; family + revision
        // sit at the trailing segment after `/`.
        let Some(suffix) = physical_id.rsplit('/').next() else {
            return Ok(());
        };
        let Some((family, rev)) = suffix.split_once(':') else {
            return Ok(());
        };
        let Ok(revision) = rev.parse::<i32>() else {
            return Ok(());
        };
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(revs) = state.task_definitions.get_mut(family) {
            if let Some(td) = revs.get_mut(&revision) {
                td.status = "INACTIVE".to_string();
                td.deregistered_at = Some(Utc::now());
            }
        }
        Ok(())
    }

    pub(super) fn create_ecs_service(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let generated_name = self.physical_name(resource);
        let service_name = props
            .get("ServiceName")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name)
            .to_string();
        // Cluster: default to "default" if missing; accept name or ARN.
        let cluster_name = props
            .get("Cluster")
            .and_then(|v| v.as_str())
            .map(parse_ecs_cluster_name)
            .unwrap_or_else(|| "default".to_string());
        let task_definition_arn = props
            .get("TaskDefinition")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "TaskDefinition is required".to_string())?
            .to_string();
        let desired_count = props
            .get("DesiredCount")
            .and_then(cfn_as_i64)
            .map(cfn_desired_count)
            .transpose()?
            .unwrap_or(1);
        let launch_type = props
            .get("LaunchType")
            .and_then(|v| v.as_str())
            .unwrap_or("FARGATE")
            .to_string();
        let scheduling_strategy = props
            .get("SchedulingStrategy")
            .and_then(|v| v.as_str())
            .unwrap_or("REPLICA")
            .to_string();
        let deployment_controller = props
            .get("DeploymentController")
            .and_then(|v| v.get("Type"))
            .and_then(|v| v.as_str())
            .unwrap_or("ECS")
            .to_string();
        let load_balancers: Vec<serde_json::Value> = props
            .get("LoadBalancers")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(lowercase_first_keys)
            .collect();
        let service_registries: Vec<serde_json::Value> = props
            .get("ServiceRegistries")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(lowercase_first_keys)
            .collect();
        let placement_constraints: Vec<serde_json::Value> = props
            .get("PlacementConstraints")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(lowercase_first_keys)
            .collect();
        let placement_strategy: Vec<serde_json::Value> = props
            .get("PlacementStrategies")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(lowercase_first_keys)
            .collect();
        let network_configuration = props
            .get("NetworkConfiguration")
            .cloned()
            .map(lowercase_first_keys);
        let role_arn = props
            .get("Role")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let platform_version = props
            .get("PlatformVersion")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let health_check_grace_period_seconds = props
            .get("HealthCheckGracePeriodSeconds")
            .and_then(cfn_as_i64)
            .map(|n| n as i32);
        let enable_ecs_managed_tags = props
            .get("EnableECSManagedTags")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let enable_execute_command = props
            .get("EnableExecuteCommand")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let propagate_tags = props
            .get("PropagateTags")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let capacity_provider_strategy: Vec<serde_json::Value> = props
            .get("CapacityProviderStrategy")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(lowercase_first_keys)
            .collect();
        let availability_zone_rebalancing = props
            .get("AvailabilityZoneRebalancing")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let tags = parse_ecs_tags(props.get("Tags"));

        // Family + revision parsed from the task definition ARN tail.
        let (family, revision) = parse_td_arn(&task_definition_arn);

        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.clusters.contains_key(&cluster_name) {
            return Err(format!(
                "Cluster {cluster_name} does not exist yet — retry once it has been provisioned"
            ));
        }
        let cluster_arn = state.clusters[&cluster_name].cluster_arn.clone();
        let service_arn = state.service_arn(&self.region, &cluster_name, &service_name);
        let key = format!("{cluster_name}/{service_name}");
        // CreateService starts the service's first PRIMARY deployment (none
        // for an external CODE_DEPLOY controller, which uses task sets).
        let deployments = if deployment_controller == "CODE_DEPLOY" {
            Vec::new()
        } else {
            vec![fakecloud_ecs::Deployment::new_primary(
                &task_definition_arn,
                desired_count,
                &launch_type,
                &cfn_ecs_lifecycle_hooks(props),
            )]
        };
        let service = EcsService {
            service_name: service_name.clone(),
            service_arn: service_arn.clone(),
            cluster_name: cluster_name.clone(),
            cluster_arn,
            task_definition_arn,
            family,
            revision,
            desired_count,
            running_count: 0,
            pending_count: 0,
            launch_type,
            status: "ACTIVE".to_string(),
            scheduling_strategy,
            deployment_controller,
            minimum_healthy_percent: props
                .get("DeploymentConfiguration")
                .and_then(|v| v.get("MinimumHealthyPercent"))
                .and_then(cfn_as_i64)
                .map(|n| n as i32),
            maximum_percent: props
                .get("DeploymentConfiguration")
                .and_then(|v| v.get("MaximumPercent"))
                .and_then(cfn_as_i64)
                .map(|n| n as i32),
            circuit_breaker: cfn_ecs_circuit_breaker(props),
            deployments,
            load_balancers,
            service_registries,
            placement_constraints,
            placement_strategy,
            network_configuration,
            tags,
            created_at: Utc::now(),
            created_by: None,
            role_arn,
            platform_version,
            health_check_grace_period_seconds,
            enable_execute_command,
            enable_ecs_managed_tags,
            propagate_tags,
            capacity_provider_strategy,
            availability_zone_rebalancing,
            volume_configurations: Vec::new(),
            service_connect_configuration: props
                .get("ServiceConnectConfiguration")
                .filter(|v| !v.is_null())
                .cloned(),
            vpc_lattice_configurations: props
                .get("VpcLatticeConfigurations")
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .map(lowercase_first_keys)
                .collect(),
            // AWS::ECS::Service has no gateway connector property.
            gateway_connectors: Vec::new(),
        };
        state.record_service_revision(&service);
        state.services.insert(key.clone(), service);
        if let Some(c) = state.clusters.get_mut(&cluster_name) {
            c.active_services_count += 1;
        }
        drop(accounts);

        // Back the inserted service with REAL running tasks when a container
        // runtime is wired. With no runtime (CI / metadata-only) we skip the
        // intent, leaving running_count 0 with no tasks — the same metadata-only
        // outcome the provisioner had before, and consistent with the direct
        // CreateService no-runtime path.
        if self.ecs_runtime.is_some() {
            self.pending_container_spawns.lock().push(
                super::ContainerSpawnIntent::EcsServiceTasks {
                    cluster_name: cluster_name.clone(),
                    service_name: service_name.clone(),
                },
            );
        }

        Ok(ProvisionResult::new(service_arn.clone())
            .with("Name", service_name)
            .with("ServiceArn", service_arn))
    }

    pub(super) fn delete_ecs_service(&self, physical_id: &str) -> Result<(), String> {
        // physical_id is full Service ARN: .../service/<cluster>/<service>
        let Some((cluster, service)) = parse_service_arn(physical_id) else {
            return Ok(());
        };
        let key = format!("{cluster}/{service}");
        let removed = {
            let mut accounts = self.ecs_state.write();
            let state = accounts.get_or_create(&self.account_id);
            if state.services.remove(&key).is_some() {
                if let Some(c) = state.clusters.get_mut(&cluster) {
                    if c.active_services_count > 0 {
                        c.active_services_count -= 1;
                    }
                }
                true
            } else {
                false
            }
        };
        // Queue stopping the REAL tasks (containers) the service was running so
        // the stack delete drain reaps them instead of leaking them. The task
        // records live separately from the service record removed above.
        if removed && self.ecs_runtime.is_some() {
            self.pending_container_teardowns.lock().push(
                super::ContainerTeardownIntent::EcsService {
                    cluster_name: cluster,
                    service_name: service,
                },
            );
        }
        Ok(())
    }

    pub(super) fn create_ecs_capacity_provider(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let generated_name = self.physical_name(resource);
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name)
            .to_string();
        let arn = fakecloud_ecs::ecs_arn(
            &self.region,
            &self.account_id,
            &format!("capacity-provider/{name}"),
        );
        // Stored in the camelCase API shape so Describe reads it back the
        // same as a direct CreateCapacityProvider.
        let present = |key: &str| {
            props
                .get(key)
                .filter(|v| !v.is_null())
                .map(|v| cfn_props_to_camel(v, &[]))
        };
        let auto_scaling_group_provider = present("AutoScalingGroupProvider");
        let managed_instances_provider = present("ManagedInstancesProvider");
        // Same rule as CreateCapacityProvider: exactly one kind of backing
        // capacity, and only Managed Instances providers are cluster-scoped.
        let cluster = match (&auto_scaling_group_provider, &managed_instances_provider) {
            (None, None) => return Err(
                "Either AutoScalingGroupProvider or ManagedInstancesProvider must be specified."
                    .to_string(),
            ),
            (Some(_), Some(_)) => return Err(
                "AutoScalingGroupProvider and ManagedInstancesProvider can't both be specified."
                    .to_string(),
            ),
            (Some(_), None) => None,
            (None, Some(_)) => {
                let cluster_ref = props
                    .get("ClusterName")
                    .and_then(|v| v.as_str())
                    .filter(|c| !c.is_empty())
                    .ok_or_else(|| {
                        "ClusterName is required for a Managed Instances capacity provider."
                            .to_string()
                    })?;
                Some(fakecloud_ecs::EcsState::resolve_cluster_name(Some(
                    cluster_ref,
                )))
            }
        };
        let cp = EcsCapacityProvider {
            name: name.clone(),
            arn: arn.clone(),
            status: "ACTIVE".to_string(),
            auto_scaling_group_provider,
            managed_instances_provider,
            cluster,
            update_status: None,
            update_status_reason: None,
            created_at: Utc::now(),
            tags: parse_ecs_tags(props.get("Tags")),
        };
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(c) = &cp.cluster {
            if !state.clusters.contains_key(c) {
                return Err(format!("The referenced cluster was inactive: {c}"));
            }
        }
        state.capacity_providers.insert(name.clone(), cp);
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    pub(super) fn delete_ecs_capacity_provider(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.capacity_providers.remove(physical_id);
        Ok(())
    }

    /// In-place update for AWS::ECS::Cluster. ClusterName is immutable —
    /// CFN replaces the resource if it changes — so we only refresh
    /// settings/configuration/capacity-provider/tags here. The physical
    /// id is kept stable.
    pub(super) fn update_ecs_cluster(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let cluster_name = existing.physical_id.clone();
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let cluster = state
            .clusters
            .get_mut(&cluster_name)
            .ok_or_else(|| format!("Cluster {cluster_name} no longer exists"))?;
        if let Some(settings) = props.get("ClusterSettings").and_then(|v| v.as_array()) {
            cluster.settings = settings.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(cfg) = props.get("Configuration") {
            cluster.configuration = Some(lowercase_first_keys(cfg.clone()));
        }
        if let Some(cps) = props.get("CapacityProviders").and_then(|v| v.as_array()) {
            cluster.capacity_providers = cps
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect();
        }
        if let Some(strategy) = props
            .get("DefaultCapacityProviderStrategy")
            .and_then(|v| v.as_array())
        {
            cluster.default_capacity_provider_strategy =
                strategy.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(scd) = props.get("ServiceConnectDefaults") {
            cluster.service_connect_defaults = Some(lowercase_first_keys(scd.clone()));
        }
        if props.get("Tags").is_some() {
            cluster.tags = parse_ecs_tags(props.get("Tags"));
        }
        let cluster_arn = cluster.cluster_arn.clone();
        Ok(ProvisionResult::new(cluster_name).with("Arn", cluster_arn))
    }

    /// In-place update for AWS::ECS::Service. Service ARN is keyed by
    /// cluster + service name, both immutable. CFN replaces the resource
    /// if either changes, so we only mutate task definition / desired
    /// count / deployment-related fields here.
    pub(super) fn update_ecs_service(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let service_arn = existing.physical_id.clone();
        let Some((cluster_name, service_name)) = parse_service_arn(&service_arn) else {
            return Err(format!("Cannot parse service ARN: {service_arn}"));
        };
        let new_desired = props
            .get("DesiredCount")
            .and_then(cfn_as_i64)
            .map(cfn_desired_count)
            .transpose()?;
        let key = format!("{cluster_name}/{service_name}");
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let svc = state
            .services
            .get_mut(&key)
            .ok_or_else(|| format!("Service {service_arn} no longer exists"))?;
        if let Some(n) = new_desired {
            svc.desired_count = n;
            if let Some(d) = svc.deployments.iter_mut().find(|d| d.status == "PRIMARY") {
                d.desired_count = n;
                d.updated_at = Utc::now();
            }
        }
        let mut new_task_definition = false;
        if let Some(td) = props
            .get("TaskDefinition")
            .and_then(|v| v.as_str())
            .filter(|td| *td != svc.task_definition_arn)
        {
            svc.task_definition_arn = td.to_string();
            let (family, revision) = parse_td_arn(td);
            svc.family = family;
            svc.revision = revision;
            new_task_definition = true;
            // A new task definition rolls a new PRIMARY deployment and demotes
            // the previous one to ACTIVE, as UpdateService does.
            if svc.deployment_controller != "CODE_DEPLOY" {
                for d in svc.deployments.iter_mut().filter(|d| d.status == "PRIMARY") {
                    d.status = "ACTIVE".to_string();
                    d.updated_at = Utc::now();
                }
                let primary = fakecloud_ecs::Deployment::new_primary(
                    &svc.task_definition_arn,
                    svc.desired_count,
                    &svc.launch_type,
                    &cfn_ecs_lifecycle_hooks(props),
                );
                svc.deployments.insert(0, primary);
            }
        }
        if let Some(s) = props.get("LaunchType").and_then(|v| v.as_str()) {
            svc.launch_type = s.to_string();
        }
        if let Some(s) = props.get("PlatformVersion").and_then(|v| v.as_str()) {
            svc.platform_version = Some(s.to_string());
        }
        if let Some(n) = props
            .get("HealthCheckGracePeriodSeconds")
            .and_then(cfn_as_i64)
        {
            svc.health_check_grace_period_seconds = Some(n as i32);
        }
        if let Some(b) = props.get("EnableExecuteCommand").and_then(|v| v.as_bool()) {
            svc.enable_execute_command = b;
        }
        if let Some(b) = props.get("EnableECSManagedTags").and_then(|v| v.as_bool()) {
            svc.enable_ecs_managed_tags = b;
        }
        if let Some(s) = props.get("PropagateTags").and_then(|v| v.as_str()) {
            svc.propagate_tags = Some(s.to_string());
        }
        if let Some(s) = props
            .get("AvailabilityZoneRebalancing")
            .and_then(|v| v.as_str())
        {
            svc.availability_zone_rebalancing = Some(s.to_string());
        }
        if let Some(arr) = props
            .get("CapacityProviderStrategy")
            .and_then(|v| v.as_array())
        {
            svc.capacity_provider_strategy =
                arr.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(dc) = props.get("DeploymentConfiguration") {
            if let Some(n) = dc.get("MinimumHealthyPercent").and_then(cfn_as_i64) {
                svc.minimum_healthy_percent = Some(n as i32);
            }
            if let Some(n) = dc.get("MaximumPercent").and_then(cfn_as_i64) {
                svc.maximum_percent = Some(n as i32);
            }
        }
        // The circuit breaker is desired state: removing it turns it off.
        svc.circuit_breaker = cfn_ecs_circuit_breaker(props);
        if let Some(arr) = props.get("LoadBalancers").and_then(|v| v.as_array()) {
            svc.load_balancers = arr.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(arr) = props.get("ServiceRegistries").and_then(|v| v.as_array()) {
            svc.service_registries = arr.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(arr) = props.get("PlacementConstraints").and_then(|v| v.as_array()) {
            svc.placement_constraints = arr.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(arr) = props.get("PlacementStrategies").and_then(|v| v.as_array()) {
            svc.placement_strategy = arr.iter().cloned().map(lowercase_first_keys).collect();
        }
        if let Some(nc) = props.get("NetworkConfiguration") {
            svc.network_configuration = Some(lowercase_first_keys(nc.clone()));
        }
        if let Some(arr) = props
            .get("VpcLatticeConfigurations")
            .and_then(|v| v.as_array())
        {
            svc.vpc_lattice_configurations =
                arr.iter().cloned().map(lowercase_first_keys).collect();
        }
        if props.get("Tags").is_some() {
            svc.tags = parse_ecs_tags(props.get("Tags"));
        }
        let name = svc.service_name.clone();
        if new_task_definition {
            let snapshot = svc.clone();
            state.record_service_revision(&snapshot);
        }
        Ok(ProvisionResult::new(service_arn.clone())
            .with("Name", name)
            .with("ServiceArn", service_arn))
    }

    /// In-place update for AWS::ECS::TaskDefinition. ECS treats every
    /// register call as a new revision, so a CFN update produces a fresh
    /// revision and the physical id (the full ARN) shifts. Returning a
    /// new ARN tells CFN's update path that the resource was replaced.
    pub(super) fn update_ecs_task_definition(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        // Delegating to create_ecs_task_definition is safe because each
        // call bumps the revision counter — semantically the right
        // behaviour for ECS. An unnamed family keeps its generated name, so
        // the update registers the family's next revision.
        self.with_existing_name(existing, || self.create_ecs_task_definition(resource))
    }

    /// In-place update for AWS::ECS::CapacityProvider. Name is immutable;
    /// only the underlying ASG provider config and tags can change.
    pub(super) fn update_ecs_capacity_provider(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = existing.physical_id.clone();
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let cp = state
            .capacity_providers
            .get_mut(&name)
            .ok_or_else(|| format!("CapacityProvider {name} no longer exists"))?;
        // An in-place update keeps the provider's kind of backing capacity.
        let managed = cp.managed_instances_provider.is_some();
        if let Some(asg) = props
            .get("AutoScalingGroupProvider")
            .filter(|v| !v.is_null())
        {
            if managed {
                return Err(format!(
                    "CapacityProvider {name} is a Managed Instances provider; AutoScalingGroupProvider can't be set"
                ));
            }
            cp.auto_scaling_group_provider = Some(cfn_props_to_camel(asg, &[]));
        }
        if let Some(mip) = props
            .get("ManagedInstancesProvider")
            .filter(|v| !v.is_null())
        {
            if !managed {
                return Err(format!(
                    "CapacityProvider {name} is an Auto Scaling group provider; ManagedInstancesProvider can't be set"
                ));
            }
            cp.managed_instances_provider = Some(cfn_props_to_camel(mip, &[]));
        }
        if props.get("Tags").is_some() {
            cp.tags = parse_ecs_tags(props.get("Tags"));
        }
        let arn = cp.arn.clone();
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    pub(super) fn get_att_ecs_cluster(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let cluster = state.clusters.get(physical_id)?;
        match attribute {
            "Arn" => Some(cluster.cluster_arn.clone()),
            _ => None,
        }
    }

    pub(super) fn get_att_ecs_service(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let (cluster, service) = parse_service_arn(physical_id)?;
        let key = format!("{cluster}/{service}");
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let svc = state.services.get(&key)?;
        match attribute {
            "Name" => Some(svc.service_name.clone()),
            "ServiceArn" => Some(svc.service_arn.clone()),
            _ => None,
        }
    }

    pub(super) fn get_att_ecs_capacity_provider(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.ecs_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let cp = state.capacity_providers.get(physical_id)?;
        match attribute {
            "Arn" => Some(cp.arn.clone()),
            _ => None,
        }
    }
}

/// `DesiredCount` must be within 0..=the ECS tasks-per-service quota, as the
/// underlying CreateService / UpdateService calls enforce.
fn cfn_desired_count(n: i64) -> Result<i32, String> {
    let max = fakecloud_ecs::MAX_TASKS_PER_SERVICE;
    if (0..=i64::from(max)).contains(&n) {
        Ok(n as i32)
    } else {
        Err(format!(
            "Invalid request provided: CreateService error: DesiredCount {n} must be between 0 and {max}"
        ))
    }
}

/// `DeploymentConfiguration.DeploymentCircuitBreaker` as the service's
/// circuit-breaker config.
fn cfn_ecs_circuit_breaker(
    props: &serde_json::Value,
) -> Option<fakecloud_ecs::CircuitBreakerConfig> {
    let cb = props
        .get("DeploymentConfiguration")?
        .get("DeploymentCircuitBreaker")
        .filter(|v| v.is_object())?;
    let flag = |k: &str| {
        cb.get(k)
            .map(|v| v.as_bool().unwrap_or_else(|| v.as_str() == Some("true")))
            .unwrap_or(false)
    };
    Some(fakecloud_ecs::CircuitBreakerConfig {
        enable: flag("Enable"),
        rollback: flag("Rollback"),
    })
}

/// `DeploymentConfiguration.LifecycleHooks` in the ECS API's key casing.
fn cfn_ecs_lifecycle_hooks(props: &serde_json::Value) -> Vec<serde_json::Value> {
    props
        .get("DeploymentConfiguration")
        .and_then(|d| d.get("LifecycleHooks"))
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(lowercase_first_keys)
        .collect()
}
