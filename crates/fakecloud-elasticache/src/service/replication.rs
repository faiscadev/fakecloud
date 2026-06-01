//! ElastiCache `replication` family handlers extracted from service.rs
//! by audit-2026-05-19 file-split.

use super::*;

impl ElastiCacheService {
    pub(super) async fn create_replication_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let replication_group_id = required_query_param(request, "ReplicationGroupId")?;
        let description = required_query_param(request, "ReplicationGroupDescription")?;
        let engine =
            optional_query_param(request, "Engine").unwrap_or_else(|| ENGINE_REDIS.to_string());
        validate_engine(&engine)?;
        reject_memcached_for(&engine, "Replication groups")?;
        let default_version = if engine == ENGINE_VALKEY {
            "8.0"
        } else {
            "7.1"
        };
        let engine_version = optional_query_param(request, "EngineVersion")
            .unwrap_or_else(|| default_version.to_string());
        let cache_node_type = optional_query_param(request, "CacheNodeType")
            .unwrap_or_else(|| "cache.t3.micro".to_string());
        let num_cache_clusters = match optional_query_param(request, "NumCacheClusters") {
            Some(v) => {
                let n = v.parse::<i32>().map_err(|_| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!("Invalid value for NumCacheClusters: '{v}'"),
                    )
                })?;
                if n < 1 {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!("NumCacheClusters must be a positive integer, got {n}"),
                    ));
                }
                n
            }
            None => 1,
        };
        let automatic_failover = parse_optional_bool(
            optional_query_param(request, "AutomaticFailoverEnabled").as_deref(),
        )?
        .unwrap_or(false);
        let transit_encryption_enabled = parse_optional_bool(
            optional_query_param(request, "TransitEncryptionEnabled").as_deref(),
        )?
        .unwrap_or(false);
        let at_rest_encryption_enabled = parse_optional_bool(
            optional_query_param(request, "AtRestEncryptionEnabled").as_deref(),
        )?
        .unwrap_or(false);
        let multi_az_enabled =
            parse_optional_bool(optional_query_param(request, "MultiAZEnabled").as_deref())?
                .unwrap_or(false);
        let auth_token = optional_query_param(request, "AuthToken");
        let auth_token_enabled = auth_token.is_some();
        let kms_key_id = optional_query_param(request, "KmsKeyId");
        let user_group_ids = parse_query_list_param(request, "UserGroupIds", "UserGroupId");
        let max_node_groups = max_node_groups_for(&engine, &engine_version);
        let num_node_groups = match optional_query_param(request, "NumNodeGroups") {
            Some(v) => {
                let n = v.parse::<i32>().map_err(|_| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!("Invalid value for NumNodeGroups: '{v}'"),
                    )
                })?;
                // AWS shard cap depends on engine + version: pre-5.0.6 Redis is
                // 90, 5.0.6+ / Valkey is 500. Reject anything outside the engine
                // ceiling to prevent unbounded server-side allocation.
                if !(1..=max_node_groups).contains(&n) {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!(
                            "NumNodeGroups must be between 1 and {max_node_groups} for {engine} {engine_version}, got {n}"
                        ),
                    ));
                }
                n
            }
            None => 1,
        };
        let replicas_per_node_group = optional_query_param(request, "ReplicasPerNodeGroup")
            .and_then(|v| v.parse::<i32>().ok());
        let data_tiering_enabled =
            parse_optional_bool(optional_query_param(request, "DataTieringEnabled").as_deref())?;
        let data_tiering =
            data_tiering_enabled.map(|b| if b { "enabled" } else { "disabled" }.to_string());
        let ip_discovery = optional_query_param(request, "IpDiscovery");
        // Default to ipv4 when unspecified, matching AWS's default network stack.
        let network_type =
            Some(optional_query_param(request, "NetworkType").unwrap_or_else(|| "ipv4".into()));
        let transit_encryption_mode = optional_query_param(request, "TransitEncryptionMode");
        let log_delivery_configurations = parse_log_delivery_configs(request);
        let notification_topic_arn = optional_query_param(request, "NotificationTopicArn");
        let cluster_mode = optional_query_param(request, "ClusterMode");
        let cluster_enabled = num_node_groups > 1
            || cluster_mode.as_deref() == Some("enabled")
            || cluster_mode.as_deref() == Some("compatible");
        // ElastiCache Redis defaults to 6379 when Port is omitted.
        let port = optional_query_param(request, "Port")
            .and_then(|v| v.parse::<u16>().ok())
            .unwrap_or(6379);
        let cache_parameter_group_name = optional_query_param(request, "CacheParameterGroupName");
        let cache_subnet_group_name = optional_query_param(request, "CacheSubnetGroupName");
        let security_group_ids =
            parse_query_list_param(request, "SecurityGroupIds", "SecurityGroupId");
        let preferred_maintenance_window =
            optional_query_param(request, "PreferredMaintenanceWindow");
        let snapshot_name = optional_query_param(request, "SnapshotName");
        let snapshot_arns = parse_query_list_param(request, "SnapshotArns", "SnapshotArn");
        let snapshot_retention_limit =
            optional_non_negative_i32_param(request, "SnapshotRetentionLimit")?.unwrap_or(0);
        let snapshot_window = optional_query_param(request, "SnapshotWindow")
            .unwrap_or_else(|| "05:00-09:00".to_string());
        let auto_minor_version_upgrade = parse_optional_bool(
            optional_query_param(request, "AutoMinorVersionUpgrade").as_deref(),
        )?
        .unwrap_or(true);
        let tags = parse_tags(request)?;
        // Reserve the ID under a write lock before starting the container.
        let rdb_path = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            if !state.begin_replication_group_creation(&replication_group_id) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ReplicationGroupAlreadyExists",
                    format!("ReplicationGroup {replication_group_id} already exists."),
                ));
            }
            if let Some(ref subnet_group_name) = cache_subnet_group_name {
                if !state.subnet_groups.contains_key(subnet_group_name) {
                    state.cancel_replication_group_creation(&replication_group_id);
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "CacheSubnetGroupNotFoundFault",
                        format!("Cache subnet group {subnet_group_name} not found."),
                    ));
                }
            }

            // SnapshotNotFoundFault is not declared on CreateReplicationGroup
            // either — same reasoning as CreateCacheCluster above.
            snapshot_name
                .as_ref()
                .and_then(|snap_name| state.snapshots.get(snap_name))
                .and_then(|snap| snap.rdb_path.clone())
        };

        let running = if let Some(runtime) = self.runtime.as_ref() {
            match runtime
                .ensure_redis(&replication_group_id, rdb_path.as_deref())
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    self.state
                        .write()
                        .get_or_create(&request.account_id)
                        .cancel_replication_group_creation(&replication_group_id);
                    return Err(runtime_error_to_service_error(e));
                }
            }
        } else {
            // No container runtime: degrade to metadata-only replication
            // group. The control-plane response shape is unaffected and
            // describe/modify/delete still work; only the data plane
            // (connecting to the endpoint) is unavailable.
            crate::runtime::RunningCacheContainer {
                container_id: String::new(),
                host_port: 0,
            }
        };

        let member_clusters: Vec<String> = (1..=num_cache_clusters)
            .map(|i| format!("{replication_group_id}-{i:03}"))
            .collect();

        let (arn, region) = {
            let accounts = self.state.read();
            let empty = ElastiCacheState::new(&request.account_id, &request.region);
            let state = accounts.get(&request.account_id).unwrap_or(&empty);
            let arn = format!(
                "arn:aws:elasticache:{}:{}:replicationgroup:{}",
                state.region, state.account_id, replication_group_id
            );
            (arn, state.region.clone())
        };

        let group = ReplicationGroup {
            replication_group_id: replication_group_id.clone(),
            description,
            global_replication_group_id: None,
            global_replication_group_role: None,
            status: "available".to_string(),
            cache_node_type,
            engine,
            engine_version,
            num_cache_clusters,
            automatic_failover_enabled: automatic_failover,
            endpoint_address: "127.0.0.1".to_string(),
            endpoint_port: running.host_port,
            arn: arn.clone(),
            created_at: chrono::Utc::now().to_rfc3339(),
            container_id: running.container_id,
            host_port: running.host_port,
            member_clusters,
            snapshot_retention_limit,
            snapshot_window,
            transit_encryption_enabled,
            at_rest_encryption_enabled,
            cluster_enabled,
            kms_key_id,
            auth_token_enabled,
            user_group_ids: user_group_ids.clone(),
            multi_az_enabled,
            log_delivery_configurations,
            data_tiering,
            ip_discovery,
            network_type,
            transit_encryption_mode,
            num_node_groups,
            configuration_endpoint_address: if cluster_enabled {
                Some("127.0.0.1".to_string())
            } else {
                None
            },
            configuration_endpoint_port: if cluster_enabled {
                Some(running.host_port)
            } else {
                None
            },
            replicas_per_node_group,
            auth_token,
            port,
            notification_topic_arn,
            cluster_mode,
            data_tiering_enabled,
            notification_topic_status: None,
            cache_parameter_group_name: cache_parameter_group_name.clone(),
            cache_subnet_group_name,
            security_group_ids,
            preferred_maintenance_window,
            snapshot_name,
            snapshot_arns,
            auto_minor_version_upgrade,
        };

        let xml = replication_group_xml(&group, &region);
        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            state.finish_replication_group_creation(group);
            if !tags.is_empty() {
                merge_tags(state.tags.entry(arn).or_default(), &tags);
            }
        }
        if !user_group_ids.is_empty() {
            self.apply_acls_for_replication_group(&request.account_id, &replication_group_id)
                .await;
        }
        if let Some(ref param_group) = cache_parameter_group_name {
            self.apply_parameters_for_group(&request.account_id, param_group)
                .await;
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CreateReplicationGroup",
                ELASTICACHE_NS,
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn create_global_replication_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let suffix = required_query_param(request, "GlobalReplicationGroupIdSuffix")?;
        let primary_replication_group_id =
            required_query_param(request, "PrimaryReplicationGroupId")?;
        let description =
            optional_query_param(request, "GlobalReplicationGroupDescription").unwrap_or_default();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let region = state.region.clone();
        let account_id = state.account_id.clone();
        let global_replication_group_id = global_replication_group_id(&region, suffix.as_str());
        if state
            .global_replication_groups
            .contains_key(&global_replication_group_id)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "GlobalReplicationGroupAlreadyExistsFault",
                format!("GlobalReplicationGroup {global_replication_group_id} already exists."),
            ));
        }

        let primary_group = state
            .replication_groups
            .get_mut(&primary_replication_group_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ReplicationGroupNotFoundFault",
                    format!("ReplicationGroup {primary_replication_group_id} not found."),
                )
            })?;

        if primary_group.global_replication_group_id.is_some() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidReplicationGroupState",
                format!(
                    "ReplicationGroup {primary_replication_group_id} is already associated with a GlobalReplicationGroup."
                ),
            ));
        }

        primary_group.global_replication_group_id = Some(global_replication_group_id.clone());
        primary_group.global_replication_group_role = Some("primary".to_string());

        let group = GlobalReplicationGroup {
            global_replication_group_id: global_replication_group_id.clone(),
            global_replication_group_description: description,
            status: "available".to_string(),
            cache_node_type: primary_group.cache_node_type.clone(),
            engine: primary_group.engine.clone(),
            engine_version: primary_group.engine_version.clone(),
            members: vec![GlobalReplicationGroupMember {
                replication_group_id: primary_group.replication_group_id.clone(),
                replication_group_region: region.clone(),
                role: "primary".to_string(),
                automatic_failover: primary_group.automatic_failover_enabled,
                status: "associated".to_string(),
            }],
            cluster_enabled: false,
            arn: format!(
                "arn:aws:elasticache:{}:{}:globalreplicationgroup:{}",
                region, account_id, global_replication_group_id
            ),
        };

        let xml = global_replication_group_xml(&group, true);
        state
            .global_replication_groups
            .insert(global_replication_group_id, group);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CreateGlobalReplicationGroup",
                ELASTICACHE_NS,
                &format!("<GlobalReplicationGroup>{xml}</GlobalReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn describe_global_replication_groups(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let global_replication_group_id = optional_query_param(request, "GlobalReplicationGroupId");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_query_param(request, "Marker");
        let show_member_info =
            parse_optional_bool(optional_query_param(request, "ShowMemberInfo").as_deref())?
                .unwrap_or(false);

        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let groups: Vec<&GlobalReplicationGroup> = if let Some(ref global_replication_group_id) =
            global_replication_group_id
        {
            match state
                .global_replication_groups
                .get(global_replication_group_id)
            {
                Some(group) => vec![group],
                None => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "GlobalReplicationGroupNotFoundFault",
                        format!("GlobalReplicationGroup {global_replication_group_id} not found."),
                    ));
                }
            }
        } else {
            let mut groups: Vec<&GlobalReplicationGroup> =
                state.global_replication_groups.values().collect();
            groups.sort_by(|a, b| {
                a.global_replication_group_id
                    .cmp(&b.global_replication_group_id)
            });
            groups
        };

        let (page, next_marker) = paginate(&groups, marker.as_deref(), max_records)?;
        let groups_xml: String = page
            .iter()
            .map(|group| {
                format!(
                    "<GlobalReplicationGroup>{}</GlobalReplicationGroup>",
                    global_replication_group_xml(group, show_member_info)
                )
            })
            .collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeGlobalReplicationGroups",
                ELASTICACHE_NS,
                &format!(
                    "<GlobalReplicationGroups>{groups_xml}</GlobalReplicationGroups>{marker_xml}"
                ),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn describe_replication_groups(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group_id = optional_query_param(request, "ReplicationGroupId");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_query_param(request, "Marker");

        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let region = state.region.clone();

        let groups: Vec<&ReplicationGroup> = if let Some(ref id) = group_id {
            match state.replication_groups.get(id) {
                Some(g) => vec![g],
                None => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "ReplicationGroupNotFoundFault",
                        format!("ReplicationGroup {id} not found."),
                    ));
                }
            }
        } else {
            let mut groups: Vec<&ReplicationGroup> = state.replication_groups.values().collect();
            groups.sort_by(|a, b| a.replication_group_id.cmp(&b.replication_group_id));
            groups
        };

        let (page, next_marker) = paginate(&groups, marker.as_deref(), max_records)?;

        let members_xml: String = page
            .iter()
            .map(|g| {
                format!(
                    "<ReplicationGroup>{}</ReplicationGroup>",
                    replication_group_xml(g, &region)
                )
            })
            .collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeReplicationGroups",
                ELASTICACHE_NS,
                &format!("<ReplicationGroups>{members_xml}</ReplicationGroups>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn delete_global_replication_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let global_replication_group_id =
            required_query_param(request, "GlobalReplicationGroupId")?;
        let retain_primary = parse_required_bool(request, "RetainPrimaryReplicationGroup")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let mut group = state
            .global_replication_groups
            .remove(&global_replication_group_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "GlobalReplicationGroupNotFoundFault",
                    format!("GlobalReplicationGroup {global_replication_group_id} not found."),
                )
            })?;

        // Stop runtime containers for any primary we're about to drop —
        // otherwise the Docker containers + their port bindings leak
        // until process exit and a later CreateReplicationGroup hits
        // port conflicts. Collect first because container teardown
        // releases the state lock asynchronously.
        let mut to_drop: Vec<String> = Vec::new();
        for member in &group.members {
            if !retain_primary && member.role == "primary" {
                if let Some(rg) = state
                    .replication_groups
                    .remove(&member.replication_group_id)
                {
                    state.tags.remove(&rg.arn);
                    to_drop.push(member.replication_group_id.clone());
                }
            } else if let Some(replication_group) = state
                .replication_groups
                .get_mut(&member.replication_group_id)
            {
                replication_group.global_replication_group_id = None;
                replication_group.global_replication_group_role = None;
            }
        }
        if !to_drop.is_empty() {
            if let Some(runtime) = self.runtime.clone() {
                let to_drop = to_drop.clone();
                tokio::spawn(async move {
                    for id in to_drop {
                        runtime.stop_container(&id).await;
                    }
                });
            }
        }

        group.status = "deleting".to_string();
        let xml = global_replication_group_xml(&group, true);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DeleteGlobalReplicationGroup",
                ELASTICACHE_NS,
                &format!("<GlobalReplicationGroup>{xml}</GlobalReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) async fn delete_replication_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let replication_group_id = required_query_param(request, "ReplicationGroupId")?;

        let group = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            let g = state
                .replication_groups
                .remove(&replication_group_id)
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "ReplicationGroupNotFoundFault",
                        format!("ReplicationGroup {replication_group_id} not found."),
                    )
                })?;
            state.tags.remove(&g.arn);
            g
        };

        if let Some(ref runtime) = self.runtime {
            runtime.stop_container(&replication_group_id).await;
        }

        let region = self.state.read().region().to_string();
        let mut deleted_group = group;
        deleted_group.status = "deleting".to_string();
        let xml = replication_group_xml(&deleted_group, &region);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DeleteReplicationGroup",
                ELASTICACHE_NS,
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) async fn modify_replication_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let replication_group_id = required_query_param(request, "ReplicationGroupId")?;

        let new_description = optional_query_param(request, "ReplicationGroupDescription");
        let new_cache_node_type = optional_query_param(request, "CacheNodeType");
        let new_engine_version = optional_query_param(request, "EngineVersion");
        let new_automatic_failover = parse_optional_bool(
            optional_query_param(request, "AutomaticFailoverEnabled").as_deref(),
        )?;
        let new_snapshot_retention_limit = optional_query_param(request, "SnapshotRetentionLimit")
            .map(|v| {
                let val = v.parse::<i32>().map_err(|_| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!("Invalid value for SnapshotRetentionLimit: '{v}'"),
                    )
                })?;
                if val < 0 {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!("SnapshotRetentionLimit must be non-negative, got {val}"),
                    ));
                }
                Ok(val)
            })
            .transpose()?;
        let new_snapshot_window = optional_query_param(request, "SnapshotWindow");
        let user_group_ids_to_add =
            parse_member_list(&request.query_params, "UserGroupIdsToAdd", "member");
        let user_group_ids_to_remove =
            parse_member_list(&request.query_params, "UserGroupIdsToRemove", "member");
        let new_auth_token = optional_query_param(request, "AuthToken");
        let new_auth_token_strategy = optional_query_param(request, "AuthTokenUpdateStrategy");
        let new_transit_encryption_enabled = parse_optional_bool(
            optional_query_param(request, "TransitEncryptionEnabled").as_deref(),
        )?;
        let new_transit_encryption_mode = optional_query_param(request, "TransitEncryptionMode");
        if let Some(ref mode) = new_transit_encryption_mode {
            if mode != "preferred" && mode != "required" {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!(
                        "Invalid value for TransitEncryptionMode: '{mode}'. Valid values: preferred, required."
                    ),
                ));
            }
        }
        let new_at_rest_encryption_enabled = parse_optional_bool(
            optional_query_param(request, "AtRestEncryptionEnabled").as_deref(),
        )?;
        let new_kms_key_id = optional_query_param(request, "KmsKeyId");
        let new_multi_az_enabled =
            parse_optional_bool(optional_query_param(request, "MultiAZEnabled").as_deref())?;
        let remove_user_groups =
            parse_optional_bool(optional_query_param(request, "RemoveUserGroups").as_deref())?;
        let new_log_delivery_configurations = parse_log_delivery_configs(request);
        let has_log_delivery_input = !new_log_delivery_configurations.is_empty()
            || request.query_params.keys().any(|k| {
                k.starts_with("LogDeliveryConfigurations.LogDeliveryConfigurationRequest.")
            });
        let new_ip_discovery = optional_query_param(request, "IpDiscovery");
        if let Some(ref ip) = new_ip_discovery {
            if ip != "ipv4" && ip != "ipv6" {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("Invalid value for IpDiscovery: '{ip}'. Valid values: ipv4, ipv6."),
                ));
            }
        }
        let new_network_type = optional_query_param(request, "NetworkType");
        if let Some(ref nt) = new_network_type {
            if nt != "ipv4" && nt != "ipv6" && nt != "dual_stack" {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!(
                        "Invalid value for NetworkType: '{nt}'. Valid values: ipv4, ipv6, dual_stack."
                    ),
                ));
            }
        }
        let new_cluster_mode = optional_query_param(request, "ClusterMode");
        if let Some(ref cm) = new_cluster_mode {
            if cm != "compatible" && cm != "enabled" && cm != "disabled" {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!(
                        "Invalid value for ClusterMode: '{cm}'. Valid values: compatible, enabled, disabled."
                    ),
                ));
            }
        }
        let new_preferred_maintenance_window =
            optional_query_param(request, "PreferredMaintenanceWindow");
        let new_cache_parameter_group_name =
            optional_query_param(request, "CacheParameterGroupName");
        let new_auto_minor_version_upgrade = parse_optional_bool(
            optional_query_param(request, "AutoMinorVersionUpgrade").as_deref(),
        )?;
        // ApplyImmediately is parsed for wire compatibility but fakecloud always
        // applies modifications synchronously since there's no reboot window.
        let _apply_immediately =
            parse_optional_bool(optional_query_param(request, "ApplyImmediately").as_deref())?;
        let new_notification_topic_arn = optional_query_param(request, "NotificationTopicArn");
        let new_notification_topic_status =
            optional_query_param(request, "NotificationTopicStatus");

        let (group, region, rg_id, had_user_group_changes) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);

            let group = state
                .replication_groups
                .get_mut(&replication_group_id)
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "ReplicationGroupNotFoundFault",
                        format!("ReplicationGroup {replication_group_id} not found."),
                    )
                })?;

            if let Some(desc) = new_description {
                group.description = desc;
            }
            if let Some(node_type) = new_cache_node_type {
                group.cache_node_type = node_type;
            }
            if let Some(version) = new_engine_version {
                group.engine_version = version;
            }
            if let Some(af) = new_automatic_failover {
                group.automatic_failover_enabled = af;
            }
            if let Some(limit) = new_snapshot_retention_limit {
                group.snapshot_retention_limit = limit;
            }
            if let Some(window) = new_snapshot_window {
                group.snapshot_window = window;
            }
            if let Some(transit) = new_transit_encryption_enabled {
                group.transit_encryption_enabled = transit;
            }
            if let Some(mode) = new_transit_encryption_mode {
                group.transit_encryption_mode = Some(mode);
            }
            if let Some(at_rest) = new_at_rest_encryption_enabled {
                group.at_rest_encryption_enabled = at_rest;
            }
            if let Some(kms) = new_kms_key_id {
                group.kms_key_id = Some(kms);
            }
            if let Some(multi_az) = new_multi_az_enabled {
                group.multi_az_enabled = multi_az;
            }
            if let Some(ip_discovery) = new_ip_discovery {
                group.ip_discovery = Some(ip_discovery);
            }
            if let Some(network_type) = new_network_type {
                group.network_type = Some(network_type);
            }
            if let Some(cluster_mode) = new_cluster_mode {
                // ClusterMode "enabled"/"compatible" both flip cluster_enabled true;
                // "disabled" turns it off. Mirrors create_replication_group semantics.
                let enabled = cluster_mode == "enabled" || cluster_mode == "compatible";
                group.cluster_enabled = enabled;
                group.cluster_mode = Some(cluster_mode);
            }
            if let Some(window) = new_preferred_maintenance_window {
                group.preferred_maintenance_window = Some(window);
            }
            if let Some(name) = new_cache_parameter_group_name {
                group.cache_parameter_group_name = Some(name);
            }
            if let Some(amvu) = new_auto_minor_version_upgrade {
                group.auto_minor_version_upgrade = amvu;
            }
            if let Some(arn) = new_notification_topic_arn {
                group.notification_topic_arn = Some(arn);
            }
            if let Some(status) = new_notification_topic_status {
                group.notification_topic_status = Some(status);
            }
            if has_log_delivery_input {
                // AWS replaces the full log-delivery configuration set per call;
                // disabled entries (Enabled=false) are dropped from state.
                group.log_delivery_configurations = new_log_delivery_configurations;
            }
            if remove_user_groups == Some(true) {
                let cleared = std::mem::take(&mut group.user_group_ids);
                // Drop the reverse pointer too — otherwise
                // DescribeUserGroups keeps reporting this replication
                // group as a member after it was detached.
                for ug_id in &cleared {
                    if let Some(ug) = state.user_groups.get_mut(ug_id) {
                        ug.replication_groups.retain(|r| r != &replication_group_id);
                    }
                }
            }
            // Add / remove user-group ids on the replication group itself.
            // Skipped when RemoveUserGroups already cleared the list above only
            // for removes; adds still apply because AWS lets a single call clear
            // and then re-attach. Dedup on add to match AWS idempotency.
            for ug_id in &user_group_ids_to_add {
                if !group.user_group_ids.contains(ug_id) {
                    group.user_group_ids.push(ug_id.clone());
                }
            }
            for ug_id in &user_group_ids_to_remove {
                group.user_group_ids.retain(|id| id != ug_id);
            }
            // AuthToken rotation: SET / ROTATE store the new token, DELETE clears
            // it. Default strategy is SET when AuthToken is supplied without one.
            match new_auth_token_strategy.as_deref() {
                Some("DELETE") => {
                    group.auth_token = None;
                    group.auth_token_enabled = false;
                }
                Some("SET") | Some("ROTATE") => {
                    if let Some(token) = new_auth_token {
                        group.auth_token = Some(token);
                        group.auth_token_enabled = true;
                    }
                }
                Some(other) => {
                    return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!(
                        "Invalid value for AuthTokenUpdateStrategy: '{other}'. Valid values: SET, ROTATE, DELETE."
                    ),
                ));
                }
                None => {
                    if let Some(token) = new_auth_token {
                        group.auth_token = Some(token);
                        group.auth_token_enabled = true;
                    }
                }
            }

            // Mirror the membership change onto the user-group side so
            // DescribeUserGroups reflects which RGs each group covers.
            for ug_id in &user_group_ids_to_add {
                if let Some(ug) = state.user_groups.get_mut(ug_id) {
                    if !ug.replication_groups.contains(&replication_group_id) {
                        ug.replication_groups.push(replication_group_id.clone());
                    }
                }
            }
            for ug_id in &user_group_ids_to_remove {
                if let Some(ug) = state.user_groups.get_mut(ug_id) {
                    ug.replication_groups
                        .retain(|id| id != &replication_group_id);
                }
            }

            let group = state.replication_groups[&replication_group_id].clone();
            let region = state.region.clone();
            let rg_id = replication_group_id.clone();
            let had_user_group_changes = !user_group_ids_to_add.is_empty()
                || remove_user_groups == Some(true)
                || !user_group_ids_to_remove.is_empty();
            (group, region, rg_id, had_user_group_changes)
        };
        let xml = replication_group_xml(&group, &region);
        if had_user_group_changes {
            self.apply_acls_for_replication_group(&request.account_id, &rg_id)
                .await;
        }
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "ModifyReplicationGroup",
                ELASTICACHE_NS,
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn modify_global_replication_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let global_replication_group_id =
            required_query_param(request, "GlobalReplicationGroupId")?;
        let _apply_immediately = parse_required_bool(request, "ApplyImmediately")?;
        let new_description = optional_query_param(request, "GlobalReplicationGroupDescription");
        let new_cache_node_type = optional_query_param(request, "CacheNodeType");
        let new_engine = optional_query_param(request, "Engine");
        let new_engine_version = optional_query_param(request, "EngineVersion");
        let new_automatic_failover = parse_optional_bool(
            optional_query_param(request, "AutomaticFailoverEnabled").as_deref(),
        )?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let primary_replication_group_id = state
            .global_replication_groups
            .get(&global_replication_group_id)
            .and_then(primary_global_member)
            .map(|member| member.replication_group_id.clone())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "GlobalReplicationGroupNotFoundFault",
                    format!("GlobalReplicationGroup {global_replication_group_id} not found."),
                )
            })?;

        if let Some(ref engine) = new_engine {
            validate_serverless_engine(engine)?;
            let current_engine =
                &state.global_replication_groups[&global_replication_group_id].engine;
            if engine != current_engine {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!(
                        "Engine changes are not supported for GlobalReplicationGroup {global_replication_group_id}."
                    ),
                ));
            }
        }

        if let Some(primary_group) = state
            .replication_groups
            .get_mut(&primary_replication_group_id)
        {
            if let Some(cache_node_type) = new_cache_node_type.clone() {
                primary_group.cache_node_type = cache_node_type;
            }
            if let Some(engine_version) = new_engine_version.clone() {
                primary_group.engine_version = engine_version;
            }
            if let Some(automatic_failover) = new_automatic_failover {
                primary_group.automatic_failover_enabled = automatic_failover;
            }
        }

        let primary_group = state
            .replication_groups
            .get(&primary_replication_group_id)
            .cloned()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ReplicationGroupNotFoundFault",
                    format!(
                        "ReplicationGroup {primary_replication_group_id} not found for GlobalReplicationGroup {global_replication_group_id}."
                    ),
                )
            })?;
        let group = state
            .global_replication_groups
            .get_mut(&global_replication_group_id)
            .expect("global replication group exists");
        if let Some(description) = new_description {
            group.global_replication_group_description = description;
        }
        group.cache_node_type = primary_group.cache_node_type.clone();
        group.engine = primary_group.engine.clone();
        group.engine_version = primary_group.engine_version.clone();
        if let Some(member) = group
            .members
            .iter_mut()
            .find(|member| member.role == "primary")
        {
            member.automatic_failover = primary_group.automatic_failover_enabled;
        }

        let xml = global_replication_group_xml(group, true);
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "ModifyGlobalReplicationGroup",
                ELASTICACHE_NS,
                &format!("<GlobalReplicationGroup>{xml}</GlobalReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn disassociate_global_replication_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let global_replication_group_id =
            required_query_param(request, "GlobalReplicationGroupId")?;
        let replication_group_id = required_query_param(request, "ReplicationGroupId")?;
        let replication_group_region = required_query_param(request, "ReplicationGroupRegion")?;

        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let group = state
            .global_replication_groups
            .get(&global_replication_group_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "GlobalReplicationGroupNotFoundFault",
                    format!("GlobalReplicationGroup {global_replication_group_id} not found."),
                )
            })?;

        let primary_member = primary_global_member(group).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidGlobalReplicationGroupState",
                format!(
                    "GlobalReplicationGroup {global_replication_group_id} does not have a primary member."
                ),
            )
        })?;
        if primary_member.replication_group_id != replication_group_id
            || primary_member.replication_group_region != replication_group_region
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "ReplicationGroup {replication_group_id} in region {replication_group_region} is not associated with GlobalReplicationGroup {global_replication_group_id}."
                ),
            ));
        }

        let xml = global_replication_group_xml(group, true);
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DisassociateGlobalReplicationGroup",
                ELASTICACHE_NS,
                &format!("<GlobalReplicationGroup>{xml}</GlobalReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn failover_global_replication_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let global_replication_group_id =
            required_query_param(request, "GlobalReplicationGroupId")?;
        let primary_region = required_query_param(request, "PrimaryRegion")?;
        let primary_replication_group_id =
            required_query_param(request, "PrimaryReplicationGroupId")?;

        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let group = state
            .global_replication_groups
            .get(&global_replication_group_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "GlobalReplicationGroupNotFoundFault",
                    format!("GlobalReplicationGroup {global_replication_group_id} not found."),
                )
            })?;

        let primary_member = primary_global_member(group).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidGlobalReplicationGroupState",
                format!(
                    "GlobalReplicationGroup {global_replication_group_id} does not have a primary member."
                ),
            )
        })?;
        if primary_member.replication_group_id != primary_replication_group_id
            || primary_member.replication_group_region != primary_region
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "PrimaryReplicationGroupId and PrimaryRegion do not match the current primary for GlobalReplicationGroup {global_replication_group_id}."
                ),
            ));
        }

        let xml = global_replication_group_xml(group, true);
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "FailoverGlobalReplicationGroup",
                ELASTICACHE_NS,
                &format!("<GlobalReplicationGroup>{xml}</GlobalReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    // ── Replication group + global replication group ──

    pub(super) fn modify_replication_group_shard_configuration(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(request, "ReplicationGroupId")?;
        let node_group_count_str = required_query_param(request, "NodeGroupCount")?;
        let node_group_count: i32 = node_group_count_str.parse().map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("Invalid value for NodeGroupCount: '{node_group_count_str}'"),
            )
        })?;
        if node_group_count < 1 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("NodeGroupCount must be a positive integer, got {node_group_count}"),
            ));
        }
        let apply_str = required_query_param(request, "ApplyImmediately")?;
        let _apply = parse_optional_bool(Some(&apply_str))?.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("Invalid boolean value for ApplyImmediately: '{apply_str}'"),
            )
        })?;
        // Optional NodeGroupsToRemove / NodeGroupsToRetain — we accept and
        // validate them but the actual data placement is opaque (single
        // backing redis container), so they only affect bookkeeping.
        let node_groups_to_remove =
            parse_query_list_param(request, "NodeGroupsToRemove", "NodeGroupToRemove");
        let node_groups_to_retain =
            parse_query_list_param(request, "NodeGroupsToRetain", "NodeGroupToRetain");

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let group = state.replication_groups.get_mut(&id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "ReplicationGroupNotFoundFault",
                format!("ReplicationGroup {id} not found."),
            )
        })?;

        if !group.cluster_enabled && node_group_count != 1 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterCombination",
                "NodeGroupCount can only be modified for cluster mode replication groups."
                    .to_string(),
            ));
        }

        let max_shards = max_node_groups_for(&group.engine, &group.engine_version);
        if node_group_count > max_shards {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "NodeGroupCount must be between 1 and {max_shards} for {} {}, got {node_group_count}",
                    group.engine, group.engine_version
                ),
            ));
        }

        let current_shards = group.num_node_groups.max(1);
        if node_group_count < current_shards {
            // Decrease: AWS requires either NodeGroupsToRemove or
            // NodeGroupsToRetain whose length matches the delta or the new
            // shape respectively.
            let to_drop = (current_shards - node_group_count) as usize;
            let supplied_remove = node_groups_to_remove.len();
            let supplied_retain = node_groups_to_retain.len();
            if supplied_remove == 0 && supplied_retain == 0 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterCombination",
                    "Decreasing NodeGroupCount requires NodeGroupsToRemove or NodeGroupsToRetain."
                        .to_string(),
                ));
            }
            if supplied_remove > 0 && supplied_remove != to_drop {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!(
                        "NodeGroupsToRemove must contain exactly {to_drop} entries, got {supplied_remove}"
                    ),
                ));
            }
            if supplied_retain > 0 && supplied_retain != node_group_count as usize {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!(
                        "NodeGroupsToRetain must contain exactly {node_group_count} entries, got {supplied_retain}"
                    ),
                ));
            }
        }

        let replicas_per_shard = current_replicas_per_shard(group);
        let new_total = node_group_count
            .checked_mul(replicas_per_shard + 1)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!(
                        "Shard count {node_group_count} with {replicas_per_shard} replicas per shard overflows."
                    ),
                )
            })?;

        group.num_node_groups = node_group_count;
        group.num_cache_clusters = new_total;
        group.member_clusters = build_member_clusters(&id, new_total);
        // Cluster-mode flag tracks shard count > 1 unless the user already
        // opted into cluster_mode=enabled at create time.
        if node_group_count > 1 {
            group.cluster_enabled = true;
        }

        let group = group.clone();
        let region = state.region.clone();
        let xml = replication_group_xml(&group, &region);
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "ModifyReplicationGroupShardConfiguration",
                ELASTICACHE_NS,
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn decrease_node_groups_in_global_replication_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.modify_global_node_groups(request, "DecreaseNodeGroupsInGlobalReplicationGroup")
    }

    pub(super) fn increase_node_groups_in_global_replication_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.modify_global_node_groups(request, "IncreaseNodeGroupsInGlobalReplicationGroup")
    }

    pub(super) fn rebalance_slots_in_global_replication_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        self.modify_global_node_groups(request, "RebalanceSlotsInGlobalReplicationGroup")
    }

    /// Best-effort ACL application: push every user from every user group
    /// attached to the replication group into the running Redis container
    /// via `ACL SETUSER`, and remove any users that are no longer attached.
    pub(super) async fn apply_acls_for_replication_group(&self, account_id: &str, rg_id: &str) {
        let Some(runtime) = self.runtime.as_ref() else {
            return;
        };
        let (users, all_known_user_ids) = {
            let accounts = self.state.read();
            let state = accounts.get(account_id);
            let Some(state) = state else { return };
            let Some(rg) = state.replication_groups.get(rg_id) else {
                return;
            };
            let mut users = Vec::new();
            let mut all_known_user_ids: std::collections::HashSet<String> =
                std::collections::HashSet::new();
            for u in state.users.values() {
                all_known_user_ids.insert(u.user_id.clone());
            }
            for ug_id in &rg.user_group_ids {
                if let Some(ug) = state.user_groups.get(ug_id) {
                    for uid in &ug.user_ids {
                        if let Some(u) = state.users.get(uid) {
                            users.push(u.clone());
                        }
                    }
                }
            }
            (users, all_known_user_ids)
        };

        // Remove users that are no longer attached to this replication group.
        // We query the current ACL list from Redis and delete any custom user
        // (other than "default") that is not in the desired set.
        let desired: std::collections::HashSet<String> =
            users.iter().map(|u| u.user_id.clone()).collect();
        match runtime
            .exec_redis(rg_id, &["ACL".to_string(), "USERS".to_string()])
            .await
        {
            Ok(output) if output.status.success() => {
                let stdout = String::from_utf8_lossy(&output.stdout);
                for line in stdout.lines() {
                    let username = line.trim().trim_matches('"');
                    if username == "default" || username.is_empty() {
                        continue;
                    }
                    // Only delete users that exist in our fakecloud state but
                    // are no longer attached to this RG.  This avoids
                    // accidentally deleting users that belong to another
                    // service sharing the same Redis container.
                    if all_known_user_ids.contains(username) && !desired.contains(username) {
                        let del_args = vec![
                            "ACL".to_string(),
                            "DELUSER".to_string(),
                            username.to_string(),
                        ];
                        if let Ok(del_out) = runtime.exec_redis(rg_id, &del_args).await {
                            if !del_out.status.success() {
                                tracing::warn!(
                                    rg_id = %rg_id,
                                    user_id = %username,
                                    stderr = %String::from_utf8_lossy(&del_out.stderr),
                                    "ACL DELUSER failed"
                                );
                            }
                        }
                    }
                }
            }
            Ok(output) => {
                tracing::warn!(
                    rg_id = %rg_id,
                    stderr = %String::from_utf8_lossy(&output.stderr),
                    "ACL USERS failed"
                );
            }
            Err(e) => {
                tracing::warn!(
                    rg_id = %rg_id,
                    %e,
                    "ACL USERS exec failed"
                );
            }
        }

        for user in &users {
            let mut args = vec![
                "ACL".to_string(),
                "SETUSER".to_string(),
                user.user_id.clone(),
                "reset".to_string(),
            ];
            args.extend(user.access_string.split_whitespace().map(|s| s.to_string()));
            match runtime.exec_redis(rg_id, &args).await {
                Ok(output) if !output.status.success() => {
                    tracing::warn!(
                        rg_id = %rg_id,
                        user_id = %user.user_id,
                        stderr = %String::from_utf8_lossy(&output.stderr),
                        "ACL SETUSER failed"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        rg_id = %rg_id,
                        user_id = %user.user_id,
                        %e,
                        "ACL SETUSER exec failed"
                    );
                }
                _ => {}
            }
        }
    }
}
