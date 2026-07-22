//! `AWS::ELASTICACHE::*` CloudFormation provisioning (extracted from the provisioner's core module).

#![allow(clippy::too_many_lines)]

use super::*;

impl ResourceProvisioner {
    pub(crate) fn create_ec_parameter_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("CacheParameterGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let family = props
            .get("CacheParameterGroupFamily")
            .and_then(|v| v.as_str())
            .unwrap_or("redis7")
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let arn = format!(
            "arn:aws:elasticache:{}:{}:parametergroup:{}",
            self.region, self.account_id, name
        );
        let group = CacheParameterGroup {
            cache_parameter_group_name: name.clone(),
            cache_parameter_group_family: family,
            description,
            is_global: false,
            arn: arn.clone(),
        };
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        // ParameterGroups stored as Vec — replace any existing entry.
        state
            .parameter_groups
            .retain(|p| p.cache_parameter_group_name != name);
        state.parameter_groups.push(group);
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    pub(crate) fn delete_ec_parameter_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state
            .parameter_groups
            .retain(|p| p.cache_parameter_group_name != physical_id);
        Ok(())
    }

    pub(crate) fn create_ec_subnet_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("CacheSubnetGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let subnet_ids: Vec<String> = props
            .get("SubnetIds")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let arn = format!(
            "arn:aws:elasticache:{}:{}:subnetgroup:{}",
            self.region, self.account_id, name
        );
        let group = CacheSubnetGroup {
            cache_subnet_group_name: name.clone(),
            cache_subnet_group_description: description,
            vpc_id: String::new(),
            subnet_ids,
            arn: arn.clone(),
        };
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.subnet_groups.insert(name.clone(), group);
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    pub(crate) fn delete_ec_subnet_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.subnet_groups.remove(physical_id);
        Ok(())
    }

    pub(crate) fn create_ec_security_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("CacheSecurityGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let arn = format!(
            "arn:aws:elasticache:{}:{}:securitygroup:{}",
            self.region, self.account_id, name
        );
        let group = CacheSecurityGroup {
            cache_security_group_name: name.clone(),
            description,
            owner_id: self.account_id.clone(),
            arn: arn.clone(),
            ec2_security_groups: Vec::new(),
        };
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.security_groups.insert(name.clone(), group);
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    pub(crate) fn delete_ec_security_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.security_groups.remove(physical_id);
        Ok(())
    }

    pub(crate) fn create_ec_user(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let user_id = props
            .get("UserId")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let user_name = props
            .get("UserName")
            .and_then(|v| v.as_str())
            .unwrap_or(&user_id)
            .to_string();
        let engine = props
            .get("Engine")
            .and_then(|v| v.as_str())
            .unwrap_or("redis")
            .to_string();
        let access_string = props
            .get("AccessString")
            .and_then(|v| v.as_str())
            .unwrap_or("on ~* +@all")
            .to_string();
        let authentication_type = props
            .get("AuthenticationMode")
            .and_then(|v| v.get("Type"))
            .and_then(|v| v.as_str())
            .unwrap_or("no-password-required")
            .to_string();
        let arn = format!(
            "arn:aws:elasticache:{}:{}:user:{}",
            self.region, self.account_id, user_id
        );
        let user = EcUser {
            user_id: user_id.clone(),
            user_name,
            engine,
            access_string,
            status: "active".to_string(),
            authentication_type,
            password_count: 0,
            arn: arn.clone(),
            minimum_engine_version: "6.0".to_string(),
            user_group_ids: Vec::new(),
        };
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.users.insert(user_id.clone(), user);
        Ok(ProvisionResult::new(user_id).with("Arn", arn))
    }

    pub(crate) fn delete_ec_user(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.users.remove(physical_id);
        Ok(())
    }

    pub(crate) fn create_ec_user_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let user_group_id = props
            .get("UserGroupId")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let engine = props
            .get("Engine")
            .and_then(|v| v.as_str())
            .unwrap_or("redis")
            .to_string();
        let user_ids: Vec<String> = props
            .get("UserIds")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let arn = format!(
            "arn:aws:elasticache:{}:{}:usergroup:{}",
            self.region, self.account_id, user_group_id
        );
        let group = EcUserGroup {
            user_group_id: user_group_id.clone(),
            engine,
            status: "active".to_string(),
            user_ids,
            arn: arn.clone(),
            minimum_engine_version: "6.0".to_string(),
            pending_changes: None,
            replication_groups: Vec::new(),
        };
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.user_groups.insert(user_group_id.clone(), group);
        Ok(ProvisionResult::new(user_group_id).with("Arn", arn))
    }

    pub(crate) fn delete_ec_user_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.user_groups.remove(physical_id);
        Ok(())
    }

    pub(crate) fn create_ec_cache_cluster(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = props
            .get("ClusterName")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-cc-{}", resource.logical_id.to_lowercase()));
        let cache_node_type = props
            .get("CacheNodeType")
            .and_then(|v| v.as_str())
            .unwrap_or("cache.t4g.micro")
            .to_string();
        let engine = props
            .get("Engine")
            .and_then(|v| v.as_str())
            .unwrap_or("redis")
            .to_string();
        let engine_version = props
            .get("EngineVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("7.1")
            .to_string();
        let num_cache_nodes = props
            .get("NumCacheNodes")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32)
            .unwrap_or(1);
        let preferred_az = props
            .get("PreferredAvailabilityZone")
            .and_then(|v| v.as_str())
            .unwrap_or("us-east-1a")
            .to_string();
        let cache_subnet_group_name = props
            .get("CacheSubnetGroupName")
            .and_then(|v| v.as_str())
            .map(String::from);
        let auto_minor_version_upgrade = props
            .get("AutoMinorVersionUpgrade")
            .and_then(|v| v.as_bool())
            .unwrap_or(true);
        let default_port = if engine == "memcached" { 11211 } else { 6379 };
        let port = props
            .get("Port")
            .and_then(|v| v.as_i64())
            .map(|n| n as u16)
            .unwrap_or(default_port);
        let cache_parameter_group_name = props
            .get("CacheParameterGroupName")
            .and_then(|v| v.as_str())
            .map(String::from);
        let security_group_ids: Vec<String> = props
            .get("VpcSecurityGroupIds")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let cache_security_group_names: Vec<String> = props
            .get("CacheSecurityGroupNames")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let preferred_availability_zones: Vec<String> = props
            .get("PreferredAvailabilityZones")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let snapshot_arns: Vec<String> = props
            .get("SnapshotArns")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let snapshot_name = props
            .get("SnapshotName")
            .and_then(|v| v.as_str())
            .map(String::from);
        let snapshot_retention_limit = props
            .get("SnapshotRetentionLimit")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32)
            .unwrap_or(0);
        let snapshot_window = props
            .get("SnapshotWindow")
            .and_then(|v| v.as_str())
            .map(String::from);
        let preferred_maintenance_window = props
            .get("PreferredMaintenanceWindow")
            .and_then(|v| v.as_str())
            .map(String::from);
        let notification_topic_arn = props
            .get("NotificationTopicArn")
            .and_then(|v| v.as_str())
            .map(String::from);
        let transit_encryption_enabled = props
            .get("TransitEncryptionEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let auth_token = props
            .get("AuthToken")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(String::from);
        let auth_token_enabled = auth_token.is_some();
        let network_type = props
            .get("NetworkType")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or_else(|| Some("ipv4".to_string()));
        let ip_discovery = props
            .get("IpDiscovery")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or_else(|| Some("ipv4".to_string()));
        let az_mode = props
            .get("AZMode")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or_else(|| Some("single-az".to_string()));
        let outpost_mode = props
            .get("OutpostMode")
            .and_then(|v| v.as_str())
            .map(String::from);
        let preferred_outpost_arn = props
            .get("PreferredOutpostArn")
            .and_then(|v| v.as_str())
            .map(String::from);

        // When an ElastiCache container runtime is configured, the record is
        // inserted as "creating" and `CreateStack` drains a spawn intent that
        // backs it with a real Redis/Memcached container (flipping it to
        // "available" once up), matching the direct `CreateCacheCluster` path.
        // Without a runtime (CI / metadata-only) it stays "available" with no
        // container, as before.
        let back_with_container = self.elasticache_runtime.is_some();
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let arn = format!(
            "arn:aws:elasticache:{}:{}:cluster:{}",
            self.region, state.account_id, id
        );
        let endpoint_address = format!("{id}.fakecloud.{}.cache.amazonaws.com", self.region);
        let cluster = EcCacheCluster {
            cache_cluster_id: id.clone(),
            cache_node_type,
            engine,
            engine_version,
            cache_cluster_status: if back_with_container {
                "creating".to_string()
            } else {
                "available".to_string()
            },
            num_cache_nodes,
            preferred_availability_zone: preferred_az,
            cache_subnet_group_name,
            auto_minor_version_upgrade,
            arn: arn.clone(),
            created_at: Utc::now().to_rfc3339(),
            endpoint_address: endpoint_address.clone(),
            endpoint_port: port,
            container_id: String::new(),
            host_port: 0,
            replication_group_id: None,
            cache_parameter_group_name,
            security_group_ids,
            log_delivery_configurations: Vec::new(),
            transit_encryption_enabled,
            at_rest_encryption_enabled: false,
            auth_token_enabled,
            port,
            preferred_maintenance_window,
            preferred_availability_zones,
            notification_topic_arn,
            cache_security_group_names,
            snapshot_arns,
            snapshot_name,
            snapshot_retention_limit,
            snapshot_window,
            outpost_mode,
            preferred_outpost_arn,
            network_type,
            ip_discovery,
            az_mode,
            auth_token,
            kms_key_id: None,
            transit_encryption_mode: None,
            data_tiering_enabled: None,
            cluster_mode: None,
            preferred_outpost_arns: Vec::new(),
        };
        state.cache_clusters.insert(id.clone(), cluster);
        drop(accounts);

        if back_with_container {
            self.pending_container_spawns
                .lock()
                .push(ContainerSpawnIntent::ElastiCacheCluster {
                    cache_cluster_id: id.clone(),
                });
        }

        Ok(ProvisionResult::new(id.clone())
            .with("Arn", arn)
            .with("RedisEndpoint.Address", endpoint_address.clone())
            .with("RedisEndpoint.Port", port.to_string())
            .with("ConfigurationEndpoint.Address", endpoint_address)
            .with("ConfigurationEndpoint.Port", port.to_string()))
    }

    pub(crate) fn delete_ec_cache_cluster(&self, physical_id: &str) -> Result<(), String> {
        {
            let mut accounts = self.elasticache_state.write();
            let state = accounts.get_or_create(&self.account_id);
            state.cache_clusters.remove(physical_id);
        }
        if self.elasticache_runtime.is_some() {
            self.pending_container_teardowns.lock().push(
                ContainerTeardownIntent::ElastiCacheCluster {
                    cache_cluster_id: physical_id.to_string(),
                },
            );
        }
        Ok(())
    }

    pub(crate) fn create_ec_replication_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let id = props
            .get("ReplicationGroupId")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| format!("cfn-rg-{}", resource.logical_id.to_lowercase()));
        let description = props
            .get("ReplicationGroupDescription")
            .and_then(|v| v.as_str())
            .unwrap_or("CFN-provisioned replication group")
            .to_string();
        let cache_node_type = props
            .get("CacheNodeType")
            .and_then(|v| v.as_str())
            .unwrap_or("cache.t4g.micro")
            .to_string();
        let engine = props
            .get("Engine")
            .and_then(|v| v.as_str())
            .unwrap_or("redis")
            .to_string();
        let engine_version = props
            .get("EngineVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("7.1")
            .to_string();
        let num_cache_clusters = props
            .get("NumCacheClusters")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32)
            .unwrap_or(1);
        let num_node_groups = props
            .get("NumNodeGroups")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32)
            .unwrap_or(0);
        let replicas_per_node_group = props
            .get("ReplicasPerNodeGroup")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32);
        let automatic_failover_enabled = props
            .get("AutomaticFailoverEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let multi_az_enabled = props
            .get("MultiAZEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let transit_encryption_enabled = props
            .get("TransitEncryptionEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let at_rest_encryption_enabled = props
            .get("AtRestEncryptionEnabled")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let kms_key_id = props
            .get("KmsKeyId")
            .and_then(|v| v.as_str())
            .map(String::from);
        let auth_token_enabled = props
            .get("AuthToken")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .is_some();
        let user_group_ids: Vec<String> = props
            .get("UserGroupIds")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let snapshot_retention_limit = props
            .get("SnapshotRetentionLimit")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32)
            .unwrap_or(0);
        let snapshot_window = props
            .get("SnapshotWindow")
            .and_then(|v| v.as_str())
            .unwrap_or("00:00-01:00")
            .to_string();
        let port = props
            .get("Port")
            .and_then(|v| v.as_i64())
            .map(|n| n as u16)
            .unwrap_or(6379);
        let cluster_enabled = num_node_groups > 1
            || props
                .get("ClusterEnabled")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);

        // As with CacheCluster: with a runtime configured the group is inserted
        // as "creating" and `CreateStack` drains a spawn intent that backs it
        // with a real Redis container, matching the direct
        // `CreateReplicationGroup` path. Without a runtime it stays "available"
        // with no container.
        let back_with_container = self.elasticache_runtime.is_some();
        let mut accounts = self.elasticache_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let arn = format!(
            "arn:aws:elasticache:{}:{}:replicationgroup:{}",
            self.region, state.account_id, id
        );
        let endpoint_address =
            format!("{id}.fakecloud.ng.0001.{}.cache.amazonaws.com", self.region);
        let configuration_endpoint = if cluster_enabled {
            Some(format!(
                "{id}.fakecloud.cfg.{}.cache.amazonaws.com",
                self.region
            ))
        } else {
            None
        };

        let group = EcReplicationGroup {
            replication_group_id: id.clone(),
            description,
            global_replication_group_id: None,
            global_replication_group_role: None,
            status: if back_with_container {
                "creating".to_string()
            } else {
                "available".to_string()
            },
            cache_node_type,
            engine,
            engine_version,
            num_cache_clusters,
            automatic_failover_enabled,
            endpoint_address: endpoint_address.clone(),
            endpoint_port: port,
            arn: arn.clone(),
            created_at: Utc::now().to_rfc3339(),
            container_id: String::new(),
            host_port: 0,
            member_clusters: Vec::new(),
            snapshot_retention_limit,
            snapshot_window,
            transit_encryption_enabled,
            at_rest_encryption_enabled,
            cluster_enabled,
            kms_key_id,
            auth_token_enabled,
            user_group_ids,
            multi_az_enabled,
            log_delivery_configurations: Vec::new(),
            data_tiering: props
                .get("DataTieringEnabled")
                .and_then(|v| v.as_bool())
                .map(|b| if b { "enabled" } else { "disabled" }.to_string()),
            ip_discovery: props
                .get("IpDiscovery")
                .and_then(|v| v.as_str())
                .map(String::from),
            network_type: props
                .get("NetworkType")
                .and_then(|v| v.as_str())
                .map(String::from),
            transit_encryption_mode: props
                .get("TransitEncryptionMode")
                .and_then(|v| v.as_str())
                .map(String::from),
            num_node_groups,
            configuration_endpoint_address: configuration_endpoint.clone(),
            configuration_endpoint_port: configuration_endpoint.as_ref().map(|_| port),
            replicas_per_node_group,
            auth_token: props
                .get("AuthToken")
                .and_then(|v| v.as_str())
                .filter(|s| !s.is_empty())
                .map(String::from),
            port,
            notification_topic_arn: props
                .get("NotificationTopicArn")
                .and_then(|v| v.as_str())
                .map(String::from),
            cluster_mode: props
                .get("ClusterMode")
                .and_then(|v| v.as_str())
                .map(String::from),
            data_tiering_enabled: props.get("DataTieringEnabled").and_then(|v| v.as_bool()),
            notification_topic_status: None,
            cache_parameter_group_name: props
                .get("CacheParameterGroupName")
                .and_then(|v| v.as_str())
                .map(String::from),
            cache_subnet_group_name: props
                .get("CacheSubnetGroupName")
                .and_then(|v| v.as_str())
                .map(String::from),
            security_group_ids: props
                .get("SecurityGroupIds")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            preferred_maintenance_window: props
                .get("PreferredMaintenanceWindow")
                .and_then(|v| v.as_str())
                .map(String::from),
            snapshot_name: props
                .get("SnapshotName")
                .and_then(|v| v.as_str())
                .map(String::from),
            snapshot_arns: props
                .get("SnapshotArns")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            auto_minor_version_upgrade: props
                .get("AutoMinorVersionUpgrade")
                .and_then(|v| v.as_bool())
                .unwrap_or(true),
        };
        state.replication_groups.insert(id.clone(), group);
        drop(accounts);

        if back_with_container {
            self.pending_container_spawns.lock().push(
                ContainerSpawnIntent::ElastiCacheReplicationGroup {
                    replication_group_id: id.clone(),
                },
            );
        }

        let mut result = ProvisionResult::new(id.clone())
            .with("Arn", arn)
            .with("PrimaryEndPoint.Address", endpoint_address.clone())
            .with("PrimaryEndPoint.Port", port.to_string())
            .with("ReadEndPoint.Addresses", endpoint_address.clone())
            .with("ReadEndPoint.Ports", port.to_string());
        if let Some(cfg) = configuration_endpoint {
            result = result
                .with("ConfigurationEndPoint.Address", cfg)
                .with("ConfigurationEndPoint.Port", port.to_string());
        }
        Ok(result)
    }

    pub(crate) fn delete_ec_replication_group(&self, physical_id: &str) -> Result<(), String> {
        {
            let mut accounts = self.elasticache_state.write();
            let state = accounts.get_or_create(&self.account_id);
            state.replication_groups.remove(physical_id);
        }
        if self.elasticache_runtime.is_some() {
            self.pending_container_teardowns.lock().push(
                ContainerTeardownIntent::ElastiCacheReplicationGroup {
                    replication_group_id: physical_id.to_string(),
                },
            );
        }
        Ok(())
    }
}
