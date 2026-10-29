//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `rds`.

use super::*;

impl ResourceProvisioner {
    // --- RDS ---

    pub(super) fn create_rds_subnet_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let generated_name = self.physical_name(resource);
        let name = props
            .get("DBSubnetGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name)
            .to_string();
        let description = props
            .get("DBSubnetGroupDescription")
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
        let tags = parse_rds_tags(props.get("Tags"));
        // The subnets must exist in one VPC across two or more Availability
        // Zones, as the direct CreateDBSubnetGroup requires.
        let placed = fakecloud_ec2::vpc_lookup::resolve_subnet_group(
            &self.ec2_state,
            &self.account_id,
            &self.region,
            &subnet_ids,
        )
        .map_err(|e| e.message())?;
        if placed.distinct_availability_zones() < 2 {
            return Err(
                "DB Subnet Group must contain at least 2 subnets in different Availability Zones."
                    .to_string(),
            );
        }
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let arn = state.db_subnet_group_arn(&self.region, &name);
        let group = DbSubnetGroup {
            db_subnet_group_name: name.clone(),
            db_subnet_group_arn: arn.clone(),
            db_subnet_group_description: description,
            vpc_id: placed.vpc_id.clone(),
            subnet_ids,
            subnet_availability_zones: placed.availability_zones(),
            tags,
        };
        state.subnet_groups.insert(name.clone(), group);
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    pub(super) fn delete_rds_subnet_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.subnet_groups.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_rds_parameter_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let generated_name = self.physical_name(resource);
        let name = props
            .get("DBParameterGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name)
            .to_string();
        let family = props
            .get("Family")
            .and_then(|v| v.as_str())
            .unwrap_or("postgres16")
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let parameters: std::collections::BTreeMap<String, String> = props
            .get("Parameters")
            .and_then(|v| v.as_object())
            .map(|m| {
                m.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect()
            })
            .unwrap_or_default();
        let tags = parse_rds_tags(props.get("Tags"));

        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let arn = state.db_parameter_group_arn(&self.region, &name);
        let group = DbParameterGroup {
            db_parameter_group_name: name.clone(),
            db_parameter_group_arn: arn.clone(),
            db_parameter_group_family: family,
            description,
            parameters,
            parameter_apply_methods: std::collections::BTreeMap::new(),
            tags,
        };
        state.parameter_groups.insert(name.clone(), group);
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    pub(super) fn delete_rds_parameter_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.parameter_groups.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_rds_cluster_parameter_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let generated_name = self.physical_name(resource);
        let name = props
            .get("DBClusterParameterGroupName")
            .or_else(|| props.get("Name"))
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name)
            .to_string();
        let family = props
            .get("Family")
            .and_then(|v| v.as_str())
            .unwrap_or("aurora-postgresql15")
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let arn = fakecloud_rds::rds_arn(&self.region, &self.account_id, "cluster-pg", &name);
        let entry = serde_json::json!({
            "DBClusterParameterGroupName": name,
            "DBClusterParameterGroupArn": arn,
            "DBParameterGroupFamily": family,
            "Description": description,
        });
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        rds_extras_mut(state, "cluster_param_groups").insert(name.clone(), entry);
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    pub(super) fn delete_rds_cluster_parameter_group(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(m) = state.extras.get_mut("cluster_param_groups") {
            m.remove(physical_id);
        }
        Ok(())
    }

    pub(super) fn create_rds_option_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let generated_name = self.physical_name(resource);
        let name = props
            .get("OptionGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name)
            .to_string();
        let engine_name = props
            .get("EngineName")
            .and_then(|v| v.as_str())
            .unwrap_or("mysql")
            .to_string();
        let major_engine_version = props
            .get("MajorEngineVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("8.0")
            .to_string();
        let description = props
            .get("OptionGroupDescription")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let arn = fakecloud_rds::rds_arn(&self.region, &self.account_id, "og", &name);
        let entry = serde_json::json!({
            "OptionGroupName": name,
            "OptionGroupArn": arn,
            "EngineName": engine_name,
            "MajorEngineVersion": major_engine_version,
            "OptionGroupDescription": description,
        });
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        rds_extras_mut(state, "option_groups").insert(name.clone(), entry);
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    pub(super) fn delete_rds_option_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(m) = state.extras.get_mut("option_groups") {
            m.remove(physical_id);
        }
        Ok(())
    }

    pub(super) fn create_rds_event_subscription(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let generated_name = self.physical_name(resource);
        let name = props
            .get("SubscriptionName")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name)
            .to_string();
        let sns_topic_arn = props
            .get("SnsTopicArn")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let entry = serde_json::json!({
            "CustSubscriptionId": name,
            "SnsTopicArn": sns_topic_arn,
            "Status": "active",
            "Enabled": props.get("Enabled").and_then(|v| v.as_bool()).unwrap_or(true),
        });
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        rds_extras_mut(state, "event_subscriptions").insert(name.clone(), entry);
        Ok(ProvisionResult::new(name))
    }

    pub(super) fn delete_rds_event_subscription(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(m) = state.extras.get_mut("event_subscriptions") {
            m.remove(physical_id);
        }
        Ok(())
    }

    pub(super) fn create_rds_security_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let generated_name = self.physical_name(resource);
        let name = props
            .get("DBSecurityGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name)
            .to_string();
        let description = props
            .get("GroupDescription")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let entry = serde_json::json!({
            "DBSecurityGroupName": name,
            "DBSecurityGroupDescription": description,
            "OwnerId": self.account_id,
        });
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        rds_extras_mut(state, "security_groups").insert(name.clone(), entry);
        Ok(ProvisionResult::new(name))
    }

    pub(super) fn delete_rds_security_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(m) = state.extras.get_mut("security_groups") {
            m.remove(physical_id);
        }
        Ok(())
    }

    pub(super) fn create_rds_db_proxy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let generated_name = self.physical_name(resource);
        let name = props
            .get("DBProxyName")
            .and_then(|v| v.as_str())
            .unwrap_or(&generated_name)
            .to_string();
        let engine_family = props
            .get("EngineFamily")
            .and_then(|v| v.as_str())
            .unwrap_or("POSTGRESQL")
            .to_string();
        let arn = fakecloud_rds::rds_arn(&self.region, &self.account_id, "db-proxy", &name);
        let endpoint = format!("{name}.proxy-default.{}.rds.amazonaws.com", self.region);
        let entry = serde_json::json!({
            "DBProxyName": name,
            "DBProxyArn": arn,
            "Status": "available",
            "EngineFamily": engine_family,
            "Endpoint": endpoint,
        });
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        rds_extras_mut(state, "proxies").insert(name.clone(), entry);
        Ok(ProvisionResult::new(name)
            .with("DBProxyArn", arn)
            .with("Endpoint", endpoint))
    }

    pub(super) fn delete_rds_db_proxy(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(m) = state.extras.get_mut("proxies") {
            m.remove(physical_id);
        }
        Ok(())
    }

    /// Live `GetAtt` for `AWS::RDS::DBInstance`: the endpoint moves once a
    /// backing container is up (and on a port change), so it is read from
    /// the instance rather than the value captured at create time.
    pub(super) fn get_att_rds_db_instance(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let accounts = self.rds_state.read();
        let inst = accounts.get(&self.account_id)?.instances.get(physical_id)?;
        match attribute {
            "Endpoint.Address" => Some(inst.endpoint_address.clone()),
            "Endpoint.Port" => Some(inst.port.to_string()),
            "DBInstanceArn" => Some(inst.db_instance_arn.clone()),
            "DbiResourceId" => Some(inst.dbi_resource_id.clone()),
            _ => None,
        }
    }

    /// Live `GetAtt` for `AWS::RDS::DBCluster` endpoint attributes.
    pub(super) fn get_att_rds_db_cluster(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let accounts = self.rds_state.read();
        let cluster = accounts
            .get(&self.account_id)?
            .extras
            .get("clusters")?
            .get(physical_id)?;
        let field = |k: &str| cluster.get(k).and_then(|v| v.as_str()).map(String::from);
        match attribute {
            "Endpoint.Address" => field("Endpoint"),
            "ReadEndpoint.Address" => field("ReaderEndpoint"),
            "Endpoint.Port" => cluster
                .get("Port")
                .and_then(|v| v.as_i64())
                .map(|p| p.to_string()),
            "DBClusterArn" => field("DBClusterArn"),
            "DBClusterResourceId" => field("DbClusterResourceId"),
            _ => None,
        }
    }

    pub(super) fn create_rds_db_instance(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let identifier = props
            .get("DBInstanceIdentifier")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| self.physical_name(resource));
        let class = props
            .get("DBInstanceClass")
            .and_then(|v| v.as_str())
            .unwrap_or("db.t4g.micro")
            .to_string();
        // An Aurora member takes its engine, version, credentials and port
        // from its cluster (the template usually omits them on the
        // instance), as CreateDBInstance does for a cluster member.
        let cluster_id = props
            .get("DBClusterIdentifier")
            .and_then(|v| v.as_str())
            .map(String::from);
        let cluster = cluster_id.as_deref().and_then(|cid| {
            self.rds_state
                .read()
                .get(&self.account_id)?
                .extras
                .get("clusters")?
                .get(cid)
                .cloned()
        });
        let from_cluster = |key: &str| {
            cluster
                .as_ref()
                .and_then(|c| c.get(key))
                .and_then(|v| v.as_str())
                .map(String::from)
        };
        let engine = props
            .get("Engine")
            .and_then(|v| v.as_str())
            .map(String::from)
            .or_else(|| from_cluster("Engine"))
            .unwrap_or_else(|| "postgres".to_string());
        let engine_version = from_cluster("EngineVersion")
            .or_else(|| {
                props
                    .get("EngineVersion")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .unwrap_or_else(|| fakecloud_rds::default_engine_version(&engine).to_string());
        let master_username = from_cluster("MasterUsername")
            .or_else(|| {
                props
                    .get("MasterUsername")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .unwrap_or_else(|| "admin".to_string());
        let master_user_password = from_cluster("MasterUserPassword")
            .or_else(|| {
                props
                    .get("MasterUserPassword")
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
            .unwrap_or_default();
        let db_name = props
            .get("DBName")
            .and_then(|v| v.as_str())
            .map(String::from);
        // Default the port from the engine (MySQL/MariaDB -> 3306, Oracle ->
        // 1521, SQL Server -> 1433, Db2 -> 50000, Postgres -> 5432) instead of
        // hardcoding 5432 for every engine; a cluster member listens on its
        // cluster's port.
        let cluster_port = cluster
            .as_ref()
            .and_then(|c| c.get("Port"))
            .and_then(|v| v.as_i64())
            .map(|n| n as i32);
        let port = cluster_port
            .or_else(|| props.get("Port").and_then(|v| v.as_i64()).map(|n| n as i32))
            .unwrap_or_else(|| fakecloud_rds::default_port_for_engine(&engine));
        let allocated_storage = props
            .get("AllocatedStorage")
            .and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
            })
            .map(|n| n as i32)
            .unwrap_or(20);
        let publicly_accessible = props
            .get("PubliclyAccessible")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let deletion_protection = props
            .get("DeletionProtection")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let backup_retention_period = props
            .get("BackupRetentionPeriod")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32)
            .unwrap_or(0);
        let multi_az = props
            .get("MultiAZ")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let availability_zone = props
            .get("AvailabilityZone")
            .and_then(|v| v.as_str())
            .map(String::from);
        let storage_type = props
            .get("StorageType")
            .and_then(|v| v.as_str())
            .map(String::from);
        let (storage_encrypted, kms_key_id) = self.rds_instance_storage_encryption(props);
        let iam_database_authentication_enabled = props
            .get("EnableIAMDatabaseAuthentication")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let db_parameter_group_name = props
            .get("DBParameterGroupName")
            .and_then(|v| v.as_str())
            .map(String::from);
        let option_group_name = props
            .get("OptionGroupName")
            .and_then(|v| v.as_str())
            .map(String::from);
        let vpc_security_group_ids: Vec<String> = props
            .get("VPCSecurityGroups")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let enabled_cloudwatch_logs_exports: Vec<String> = props
            .get("EnableCloudwatchLogsExports")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let tags = parse_rds_tags(props.get("Tags"));

        // When an RDS container runtime is configured, the record is inserted as
        // "creating" and `CreateStack` drains a spawn intent that backs it with
        // a real Postgres/MySQL container (flipping it to "available" once up),
        // matching the direct `CreateDBInstance` path. Without a runtime (CI /
        // metadata-only) it stays "available" with no container, as before.
        let back_with_container = self.rds_runtime.is_some();
        let initial_status = if back_with_container {
            "creating"
        } else {
            "available"
        };

        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        // Build the instance ARN from the stack's request region (self.region),
        // not the RDS sub-service state's frozen startup region.
        let arn = fakecloud_rds::rds_arn(&self.region, &state.account_id, "db", &identifier);
        let endpoint_address =
            fakecloud_rds::instance_endpoint(&identifier, &self.account_id, &self.region);
        let dbi_resource_id = format!("db-{}", Uuid::new_v4().simple());
        let dbi_resource_id_attr = dbi_resource_id.clone();
        let inst = DbInstance {
            associated_roles: Vec::new(),
            db_instance_identifier: identifier.clone(),
            db_instance_arn: arn.clone(),
            db_instance_class: class,
            engine,
            engine_version,
            db_instance_status: initial_status.to_string(),
            master_username,
            db_name,
            endpoint_address,
            port,
            allocated_storage,
            publicly_accessible,
            deletion_protection,
            db_subnet_group_name: props
                .get("DBSubnetGroupName")
                .and_then(|v| v.as_str())
                .map(str::to_string),
            created_at: Utc::now(),
            dbi_resource_id,
            master_user_password,
            container_id: String::new(),
            host_port: 0,
            data_volume: Some(fakecloud_core::data_volume::DataVolumeBinding::Scoped),
            tags,
            read_replica_source_db_instance_identifier: None,
            read_replica_db_instance_identifiers: Vec::new(),
            vpc_security_group_ids,
            db_parameter_group_name,
            backup_retention_period,
            preferred_backup_window: props
                .get("PreferredBackupWindow")
                .and_then(|v| v.as_str())
                .unwrap_or("03:00-04:00")
                .to_string(),
            preferred_maintenance_window: props
                .get("PreferredMaintenanceWindow")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            latest_restorable_time: None,
            option_group_name,
            multi_az,
            pending_modified_values: None,
            availability_zone,
            storage_type,
            storage_encrypted,
            kms_key_id,
            iam_database_authentication_enabled,
            iops: props.get("Iops").and_then(|v| v.as_i64()).map(|n| n as i32),
            monitoring_interval: props
                .get("MonitoringInterval")
                .and_then(|v| v.as_i64())
                .map(|n| n as i32),
            monitoring_role_arn: props
                .get("MonitoringRoleArn")
                .and_then(|v| v.as_str())
                .map(String::from),
            performance_insights_enabled: props
                .get("EnablePerformanceInsights")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            performance_insights_kms_key_id: props
                .get("PerformanceInsightsKMSKeyId")
                .and_then(|v| v.as_str())
                .map(String::from),
            performance_insights_retention_period: props
                .get("PerformanceInsightsRetentionPeriod")
                .and_then(|v| v.as_i64())
                .map(|n| n as i32),
            enabled_cloudwatch_logs_exports,
            ca_certificate_identifier: props
                .get("CACertificateIdentifier")
                .and_then(|v| v.as_str())
                .map(String::from),
            network_type: props
                .get("NetworkType")
                .and_then(|v| v.as_str())
                .map(String::from),
            character_set_name: props
                .get("CharacterSetName")
                .and_then(|v| v.as_str())
                .map(String::from),
            auto_minor_version_upgrade: props
                .get("AutoMinorVersionUpgrade")
                .and_then(|v| v.as_bool()),
            copy_tags_to_snapshot: props.get("CopyTagsToSnapshot").and_then(|v| v.as_bool()),
            master_user_secret_arn: None,
            master_user_secret_kms_key_id: props
                .get("MasterUserSecret")
                .and_then(|v| v.get("KmsKeyId"))
                .and_then(|v| v.as_str())
                .map(String::from),
            license_model: props
                .get("LicenseModel")
                .and_then(|v| v.as_str())
                .map(String::from),
            max_allocated_storage: props
                .get("MaxAllocatedStorage")
                .and_then(|v| v.as_i64())
                .map(|n| n as i32),
            multi_tenant: props.get("MultiTenant").and_then(|v| v.as_bool()),
            storage_throughput: props
                .get("StorageThroughput")
                .and_then(|v| v.as_i64())
                .map(|n| n as i32),
            tde_credential_arn: props
                .get("TdeCredentialArn")
                .and_then(|v| v.as_str())
                .map(String::from),
            delete_automated_backups: props
                .get("DeleteAutomatedBackups")
                .and_then(|v| v.as_bool()),
            db_security_groups: props
                .get("DBSecurityGroups")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            domain: props
                .get("Domain")
                .and_then(|v| v.as_str())
                .map(String::from),
            domain_fqdn: props
                .get("DomainFqdn")
                .and_then(|v| v.as_str())
                .map(String::from),
            domain_ou: props
                .get("DomainOu")
                .and_then(|v| v.as_str())
                .map(String::from),
            domain_iam_role_name: props
                .get("DomainIAMRoleName")
                .and_then(|v| v.as_str())
                .map(String::from),
            domain_auth_secret_arn: props
                .get("DomainAuthSecretArn")
                .and_then(|v| v.as_str())
                .map(String::from),
            domain_dns_ips: props
                .get("DomainDnsIps")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
            db_cluster_identifier: cluster_id.clone(),
            activity_stream: None,
        };
        let endpoint = inst.endpoint_address.clone();
        let endpoint_port = inst.port;
        state.instances.insert(identifier.clone(), inst);
        // Register the member on its cluster now, so DescribeDBClusters lists
        // it (and names a writer) while the instance is still creating.
        if let Some(cid) = &cluster_id {
            fakecloud_rds::attach_cluster_member(state, cid, &identifier);
        }
        drop(accounts);

        if back_with_container {
            self.pending_container_spawns
                .lock()
                .push(super::ContainerSpawnIntent::RdsInstance {
                    identifier: identifier.clone(),
                });
        }

        Ok(ProvisionResult::new(identifier.clone())
            .with("DBInstanceArn", arn)
            .with("Endpoint.Address", endpoint)
            .with("Endpoint.Port", endpoint_port.to_string())
            .with("DbiResourceId", dbi_resource_id_attr))
    }

    /// Apply a CFN property update to an existing RDS DB instance in place.
    /// Mirrors the property extraction in `create_rds_db_instance` for the
    /// fields that `ModifyDBInstance` mutates without replacement (instance
    /// class, allocated storage, engine version, master password, backup
    /// retention, multi-AZ, deletion protection, IOPS, storage type, etc.) so a
    /// stack update reaches the instance and `DescribeDBInstances` reflects the
    /// new config instead of the stale one. The identifier, ARN, endpoint,
    /// backing container and creation time are preserved.
    pub(super) fn update_rds_db_instance(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let identifier = &existing.physical_id;

        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let inst = state
            .instances
            .get_mut(identifier)
            .ok_or_else(|| format!("DB instance {identifier} not yet provisioned"))?;

        if let Some(class) = props.get("DBInstanceClass").and_then(|v| v.as_str()) {
            inst.db_instance_class = class.to_string();
        }
        if let Some(w) = props.get("PreferredBackupWindow").and_then(|v| v.as_str()) {
            inst.preferred_backup_window = w.to_string();
        }
        if let Some(w) = props
            .get("PreferredMaintenanceWindow")
            .and_then(|v| v.as_str())
        {
            inst.preferred_maintenance_window = Some(w.to_string());
        }
        if let Some(ev) = props.get("EngineVersion").and_then(|v| v.as_str()) {
            inst.engine_version = ev.to_string();
        }
        if let Some(pw) = props.get("MasterUserPassword").and_then(|v| v.as_str()) {
            inst.master_user_password = pw.to_string();
        }
        if let Some(storage) = props.get("AllocatedStorage").and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
        }) {
            inst.allocated_storage = storage as i32;
        }
        if let Some(b) = props.get("PubliclyAccessible").and_then(|v| v.as_bool()) {
            inst.publicly_accessible = b;
        }
        if let Some(b) = props.get("DeletionProtection").and_then(|v| v.as_bool()) {
            inst.deletion_protection = b;
        }
        if let Some(n) = props.get("BackupRetentionPeriod").and_then(|v| v.as_i64()) {
            inst.backup_retention_period = n as i32;
        }
        if let Some(b) = props.get("MultiAZ").and_then(|v| v.as_bool()) {
            inst.multi_az = b;
        }
        if let Some(st) = props.get("StorageType").and_then(|v| v.as_str()) {
            inst.storage_type = Some(st.to_string());
        }
        if let Some(n) = props.get("Iops").and_then(|v| v.as_i64()) {
            inst.iops = Some(n as i32);
        }
        if let Some(pg) = props.get("DBParameterGroupName").and_then(|v| v.as_str()) {
            inst.db_parameter_group_name = Some(pg.to_string());
        }
        if let Some(og) = props.get("OptionGroupName").and_then(|v| v.as_str()) {
            inst.option_group_name = Some(og.to_string());
        }
        if let Some(n) = props.get("MaxAllocatedStorage").and_then(|v| v.as_i64()) {
            inst.max_allocated_storage = Some(n as i32);
        }
        if let Some(b) = props
            .get("AutoMinorVersionUpgrade")
            .and_then(|v| v.as_bool())
        {
            inst.auto_minor_version_upgrade = Some(b);
        }
        if let Some(b) = props.get("CopyTagsToSnapshot").and_then(|v| v.as_bool()) {
            inst.copy_tags_to_snapshot = Some(b);
        }
        if let Some(n) = props.get("MonitoringInterval").and_then(|v| v.as_i64()) {
            inst.monitoring_interval = Some(n as i32);
        }
        if let Some(vsgs) = props.get("VPCSecurityGroups").and_then(|v| v.as_array()) {
            inst.vpc_security_group_ids = vsgs
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
        }
        if let Some(exports) = props
            .get("EnableCloudwatchLogsExports")
            .and_then(|v| v.as_array())
        {
            inst.enabled_cloudwatch_logs_exports = exports
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
        }
        if let Some(ca) = props
            .get("CACertificateIdentifier")
            .and_then(|v| v.as_str())
        {
            inst.ca_certificate_identifier = Some(ca.to_string());
        }

        let arn = inst.db_instance_arn.clone();
        let endpoint = inst.endpoint_address.clone();
        let port = inst.port;
        let dbi_resource_id = inst.dbi_resource_id.clone();
        Ok(ProvisionResult::new(identifier.clone())
            .with("DBInstanceArn", arn)
            .with("Endpoint.Address", endpoint)
            .with("Endpoint.Port", port.to_string())
            .with("DbiResourceId", dbi_resource_id))
    }

    pub(super) fn delete_rds_db_instance(&self, physical_id: &str) -> Result<(), String> {
        let removed = {
            let mut accounts = self.rds_state.write();
            let state = accounts.get_or_create(&self.account_id);
            let removed = state.instances.remove(physical_id);
            if let Some(cid) = removed
                .as_ref()
                .and_then(|i| i.db_cluster_identifier.clone())
            {
                fakecloud_rds::detach_cluster_member(state, &cid, physical_id);
            }
            removed
        };
        // Queue the REAL container teardown when a runtime is wired, so the stack
        // delete drain stops + removes the Postgres/MySQL container and its data
        // volume instead of leaking it (the create-side #2031 hardening for the
        // delete path). The deleted row's resource id and volume travel with
        // the intent, so the teardown never reaches a replacement instance
        // that reuses the identifier.
        if let (Some(_), Some(inst)) = (self.rds_runtime.as_ref(), removed) {
            self.pending_container_teardowns.lock().push(
                super::ContainerTeardownIntent::RdsInstance {
                    incarnation: inst.dbi_resource_id.clone(),
                    data_volume: Some(inst.data_volume_name(
                        fakecloud_core::data_volume::current_scope().tag(),
                        &self.account_id,
                    )),
                },
            );
        }
        Ok(())
    }

    /// An `AWS::RDS::DBInstance`'s storage encryption and key, as the
    /// `CreateDBInstance` API path reports them: an Aurora cluster member
    /// takes its cluster's; otherwise encrypted storage uses the named key's
    /// ARN or the AWS-managed `aws/rds` key, and unencrypted storage keeps any
    /// named key as given. Resolved before the RDS state lock is taken.
    fn rds_instance_storage_encryption(&self, props: &serde_json::Value) -> (bool, Option<String>) {
        let cluster = props
            .get("DBClusterIdentifier")
            .and_then(|v| v.as_str())
            .and_then(|cluster_id| {
                let accounts = self.rds_state.read();
                let cluster = accounts
                    .get(&self.account_id)?
                    .extras
                    .get("clusters")?
                    .get(cluster_id)?;
                Some((
                    cluster["StorageEncrypted"].as_bool().unwrap_or(false),
                    cluster["KmsKeyId"].as_str().map(String::from),
                ))
            });
        if let Some(inherited) = cluster {
            return inherited;
        }
        let named = props.get("KmsKeyId").and_then(|v| v.as_str());
        if props.get("StorageEncrypted").and_then(|v| v.as_bool()) == Some(true) {
            (true, self.kms_key_arn_or_aws_managed(named, "rds"))
        } else {
            (false, named.map(String::from))
        }
    }

    /// An `AWS::RDS::DBCluster`'s storage key: the named key's ARN or the
    /// AWS-managed `aws/rds` key when storage is encrypted, else any named
    /// key as given.
    fn rds_cluster_storage_key(&self, props: &serde_json::Value) -> Option<String> {
        let named = props.get("KmsKeyId").and_then(|v| v.as_str());
        if props.get("StorageEncrypted").and_then(|v| v.as_bool()) == Some(true) {
            self.kms_key_arn_or_aws_managed(named, "rds")
        } else {
            named.map(String::from)
        }
    }

    pub(super) fn create_rds_db_cluster(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let identifier = props
            .get("DBClusterIdentifier")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| self.physical_name(resource));
        let engine = props
            .get("Engine")
            .and_then(|v| v.as_str())
            .unwrap_or("aurora-postgresql")
            .to_string();
        let engine_version = props
            .get("EngineVersion")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| fakecloud_rds::default_engine_version(&engine).to_string());
        let master_username = props
            .get("MasterUsername")
            .and_then(|v| v.as_str())
            .map(String::from);
        // Stored (never rendered) like CreateDBCluster does, so members and
        // snapshots of the cluster start with the cluster's credentials.
        let master_user_password = props
            .get("MasterUserPassword")
            .and_then(|v| v.as_str())
            .unwrap_or(fakecloud_rds::extras::DEFAULT_CLUSTER_MASTER_PASSWORD)
            .to_string();
        let port = props
            .get("Port")
            .and_then(|v| {
                v.as_i64()
                    .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
            })
            .unwrap_or_else(|| i64::from(fakecloud_rds::default_port_for_engine(&engine)));
        let kms_key_id = self.rds_cluster_storage_key(props);
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let arn = fakecloud_rds::rds_arn(&self.region, &self.account_id, "cluster", &identifier);
        let cluster_resource_id = format!("cluster-{}", Uuid::new_v4().simple());
        let endpoint = fakecloud_rds::cluster_endpoint(&identifier, &self.account_id, &self.region);
        let reader_endpoint =
            fakecloud_rds::cluster_reader_endpoint(&identifier, &self.account_id, &self.region);
        let body = serde_json::json!({
            "DBClusterIdentifier": identifier,
            "DBClusterArn": arn,
            "Engine": engine,
            "EngineVersion": engine_version,
            "MasterUsername": master_username,
            "MasterUserPassword": master_user_password,
            "Status": "available",
            "DbClusterResourceId": cluster_resource_id,
            "Endpoint": endpoint,
            "ReaderEndpoint": reader_endpoint,
            "Port": port,
            "AllocatedStorage": props.get("AllocatedStorage").and_then(|v| v.as_i64()).unwrap_or(1),
            "BackupRetentionPeriod": props.get("BackupRetentionPeriod").and_then(|v| v.as_i64()).unwrap_or(1),
            "DatabaseName": props.get("DatabaseName").and_then(|v| v.as_str()),
            "DBSubnetGroup": props.get("DBSubnetGroupName").and_then(|v| v.as_str()),
            "VpcSecurityGroupIds": props.get("VpcSecurityGroupIds").cloned().unwrap_or(serde_json::json!([])),
            "StorageEncrypted": props.get("StorageEncrypted").and_then(|v| v.as_bool()).unwrap_or(false),
            "KmsKeyId": kms_key_id,
            "DeletionProtection": props.get("DeletionProtection").and_then(|v| v.as_bool()).unwrap_or(false),
            "ClusterCreateTime": Utc::now().to_rfc3339(),
            "EnabledCloudwatchLogsExports": props.get("EnableCloudwatchLogsExports").cloned().unwrap_or(serde_json::json!([])),
            "MultiAZ": false,
            "DBClusterMembers": [],
        });
        state
            .extras
            .entry("clusters".to_string())
            .or_default()
            .insert(identifier.clone(), body);
        Ok(ProvisionResult::new(identifier.clone())
            .with("DBClusterArn", arn)
            .with("Endpoint.Address", endpoint)
            .with("ReadEndpoint.Address", reader_endpoint)
            .with("Endpoint.Port", port.to_string())
            .with("DBClusterResourceId", cluster_resource_id))
    }

    /// In-place stack update for `AWS::RDS::DBCluster`. Aurora clusters are
    /// stateful (they can be container-backed and hold real data), so a benign
    /// property or tag change must NOT delete+recreate the cluster the way the
    /// reprovision fallback would -- that would wipe the data. This mutates the
    /// stored cluster record in place for the properties `ModifyDBCluster`
    /// applies without replacement (engine version, backup retention, port,
    /// deletion protection, security groups, storage, log exports, ...) while
    /// preserving the identifier, ARN, endpoints, resource id, and create time.
    pub(super) fn update_rds_db_cluster(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let identifier = existing.physical_id.clone();
        // Resolved before the RDS state lock; applied below only when the
        // update turns encryption on for a cluster without a key.
        let encrypt_key = (props.get("StorageEncrypted").and_then(|v| v.as_bool()) == Some(true))
            .then(|| self.rds_cluster_storage_key(props))
            .flatten();

        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let body = state
            .extras
            .get_mut("clusters")
            .and_then(|m| m.get_mut(&identifier))
            .ok_or_else(|| format!("DB cluster {identifier} not yet provisioned"))?;
        let obj = body
            .as_object_mut()
            .ok_or_else(|| format!("DB cluster {identifier} record is malformed"))?;

        // Mutable-without-replacement properties. Immutable ones (identifier,
        // ARN, endpoints, resource id, create time) are deliberately left
        // untouched so the cluster -- and its backing data -- survives.
        if let Some(v) = props.get("EngineVersion").filter(|v| !v.is_null()) {
            obj.insert("EngineVersion".to_string(), v.clone());
        }
        if let Some(v) = props.get("BackupRetentionPeriod").and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
        }) {
            obj.insert("BackupRetentionPeriod".to_string(), serde_json::json!(v));
        }
        if let Some(v) = props.get("Port").and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
        }) {
            obj.insert("Port".to_string(), serde_json::json!(v));
        }
        if let Some(v) = props.get("DeletionProtection").and_then(|v| v.as_bool()) {
            obj.insert("DeletionProtection".to_string(), serde_json::json!(v));
        }
        if let Some(v) = props.get("MasterUserPassword").and_then(|v| v.as_str()) {
            obj.insert("MasterUserPassword".to_string(), serde_json::json!(v));
        }
        if let Some(v) = props.get("StorageEncrypted").and_then(|v| v.as_bool()) {
            obj.insert("StorageEncrypted".to_string(), serde_json::json!(v));
            if !v {
                obj.remove("KmsKeyId");
            } else if obj.get("KmsKeyId").is_none_or(|k| k.is_null()) {
                if let Some(key) = encrypt_key {
                    obj.insert("KmsKeyId".to_string(), serde_json::json!(key));
                }
            }
        }
        if let Some(v) = props.get("AllocatedStorage").and_then(|v| v.as_i64()) {
            obj.insert("AllocatedStorage".to_string(), serde_json::json!(v));
        }
        if let Some(v) = props.get("VpcSecurityGroupIds").filter(|v| v.is_array()) {
            obj.insert("VpcSecurityGroupIds".to_string(), v.clone());
        }
        if let Some(v) = props
            .get("EnableCloudwatchLogsExports")
            .filter(|v| v.is_array())
        {
            obj.insert("EnabledCloudwatchLogsExports".to_string(), v.clone());
        }
        if let Some(v) = props.get("PreferredBackupWindow").filter(|v| !v.is_null()) {
            obj.insert("PreferredBackupWindow".to_string(), v.clone());
        }
        if let Some(v) = props
            .get("PreferredMaintenanceWindow")
            .filter(|v| !v.is_null())
        {
            obj.insert("PreferredMaintenanceWindow".to_string(), v.clone());
        }

        // Read back the (unchanged) identity attributes for Ref/GetAtt.
        let arn = obj
            .get("DBClusterArn")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let endpoint = obj
            .get("Endpoint")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let reader_endpoint = obj
            .get("ReaderEndpoint")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        let port = obj.get("Port").and_then(|v| v.as_i64()).unwrap_or_else(|| {
            i64::from(fakecloud_rds::default_port_for_engine(
                obj.get("Engine")
                    .and_then(|v| v.as_str())
                    .unwrap_or_default(),
            ))
        });
        let resource_id = obj
            .get("DbClusterResourceId")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();

        Ok(ProvisionResult::new(identifier)
            .with("DBClusterArn", arn)
            .with("Endpoint.Address", endpoint)
            .with("ReadEndpoint.Address", reader_endpoint)
            .with("Endpoint.Port", port.to_string())
            .with("DBClusterResourceId", resource_id))
    }

    pub(super) fn delete_rds_db_cluster(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(m) = state.extras.get_mut("clusters") {
            m.remove(physical_id);
        }
        Ok(())
    }
}
