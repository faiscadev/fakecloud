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
        let name = props
            .get("DBSubnetGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
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
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let arn = state.db_subnet_group_arn(&name);
        let group = DbSubnetGroup {
            db_subnet_group_name: name.clone(),
            db_subnet_group_arn: arn.clone(),
            db_subnet_group_description: description,
            vpc_id: String::new(),
            subnet_ids,
            subnet_availability_zones: Vec::new(),
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
        let name = props
            .get("DBParameterGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
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
        let arn = state.db_parameter_group_arn(&name);
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
        let name = props
            .get("DBClusterParameterGroupName")
            .or_else(|| props.get("Name"))
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
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
        let arn = format!(
            "arn:aws:rds:{}:{}:cluster-pg:{}",
            self.region, self.account_id, name
        );
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
        let name = props
            .get("OptionGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
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
        let arn = format!(
            "arn:aws:rds:{}:{}:og:{}",
            self.region, self.account_id, name
        );
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
        let name = props
            .get("SubscriptionName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
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
        let name = props
            .get("DBSecurityGroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
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
        let name = props
            .get("DBProxyName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let engine_family = props
            .get("EngineFamily")
            .and_then(|v| v.as_str())
            .unwrap_or("POSTGRESQL")
            .to_string();
        let arn = format!(
            "arn:aws:rds:{}:{}:db-proxy:{}",
            self.region, self.account_id, name
        );
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

    pub(super) fn create_rds_db_instance(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let identifier = props
            .get("DBInstanceIdentifier")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| {
                format!(
                    "cfn-{}-{}",
                    resource.logical_id.to_lowercase(),
                    Uuid::new_v4().simple().to_string()[..8].to_lowercase()
                )
            });
        let class = props
            .get("DBInstanceClass")
            .and_then(|v| v.as_str())
            .unwrap_or("db.t4g.micro")
            .to_string();
        let engine = props
            .get("Engine")
            .and_then(|v| v.as_str())
            .unwrap_or("postgres")
            .to_string();
        let engine_version = props
            .get("EngineVersion")
            .and_then(|v| v.as_str())
            .unwrap_or("16.0")
            .to_string();
        let master_username = props
            .get("MasterUsername")
            .and_then(|v| v.as_str())
            .unwrap_or("admin")
            .to_string();
        let master_user_password = props
            .get("MasterUserPassword")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let db_name = props
            .get("DBName")
            .and_then(|v| v.as_str())
            .map(String::from);
        let port = props
            .get("Port")
            .and_then(|v| v.as_i64())
            .map(|n| n as i32)
            .unwrap_or(5432);
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
        let storage_encrypted = props
            .get("StorageEncrypted")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let kms_key_id = props
            .get("KmsKeyId")
            .and_then(|v| v.as_str())
            .map(String::from);
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
        let arn = state.db_instance_arn(&identifier);
        let endpoint_address = format!(
            "{identifier}.cluster-fakecloud.{}.rds.amazonaws.com",
            state.region
        );
        let dbi_resource_id = format!("db-{}", Uuid::new_v4().simple());
        let inst = DbInstance {
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
            created_at: Utc::now(),
            dbi_resource_id,
            master_user_password,
            container_id: String::new(),
            host_port: 0,
            tags,
            read_replica_source_db_instance_identifier: None,
            read_replica_db_instance_identifiers: Vec::new(),
            vpc_security_group_ids,
            db_parameter_group_name,
            backup_retention_period,
            preferred_backup_window: "03:00-04:00".to_string(),
            preferred_maintenance_window: None,
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
            db_cluster_identifier: props
                .get("DBClusterIdentifier")
                .and_then(|v| v.as_str())
                .map(String::from),
        };
        let endpoint = inst.endpoint_address.clone();
        let endpoint_port = inst.port;
        state.instances.insert(identifier.clone(), inst);
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
            .with("DbiResourceId", format!("db-{identifier}")))
    }

    pub(super) fn delete_rds_db_instance(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.instances.remove(physical_id);
        Ok(())
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
            .unwrap_or_else(|| {
                format!(
                    "cfn-cluster-{}-{}",
                    resource.logical_id.to_lowercase(),
                    Uuid::new_v4().simple().to_string()[..8].to_lowercase()
                )
            });
        let engine = props
            .get("Engine")
            .and_then(|v| v.as_str())
            .unwrap_or("aurora-postgresql")
            .to_string();
        let engine_version = props
            .get("EngineVersion")
            .and_then(|v| v.as_str())
            .map(String::from);
        let master_username = props
            .get("MasterUsername")
            .and_then(|v| v.as_str())
            .map(String::from);
        let port = props.get("Port").and_then(|v| v.as_i64()).unwrap_or(5432);
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let arn = format!(
            "arn:aws:rds:{}:{}:cluster:{}",
            state.region, state.account_id, identifier
        );
        let cluster_resource_id = format!("cluster-{}", Uuid::new_v4().simple());
        let endpoint = format!(
            "{identifier}.cluster-fakecloud.{}.rds.amazonaws.com",
            state.region
        );
        let reader_endpoint = format!(
            "{identifier}.cluster-ro-fakecloud.{}.rds.amazonaws.com",
            state.region
        );
        let body = serde_json::json!({
            "DBClusterIdentifier": identifier,
            "DBClusterArn": arn,
            "Engine": engine,
            "EngineVersion": engine_version,
            "MasterUsername": master_username,
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
            "KmsKeyId": props.get("KmsKeyId").and_then(|v| v.as_str()),
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

    pub(super) fn delete_rds_db_cluster(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.rds_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some(m) = state.extras.get_mut("clusters") {
            m.remove(physical_id);
        }
        Ok(())
    }
}
