//! ElastiCache `serverless` family handlers extracted from service.rs
//! by audit-2026-05-19 file-split.

use super::*;

impl ElastiCacheService {
    pub(super) async fn create_serverless_cache(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let serverless_cache_name = required_query_param(request, "ServerlessCacheName")?;
        let engine = required_query_param(request, "Engine")?;
        validate_serverless_engine(&engine)?;

        let description = optional_query_param(request, "Description").unwrap_or_default();
        let major_engine_version = optional_query_param(request, "MajorEngineVersion")
            .unwrap_or_else(|| default_major_engine_version(&engine).to_string());
        let full_engine_version = default_full_engine_version(&engine, &major_engine_version)?;
        let cache_usage_limits = parse_cache_usage_limits(request)?;
        let security_group_ids =
            parse_query_list_param(request, "SecurityGroupIds", "SecurityGroupId");
        let subnet_ids = parse_query_list_param(request, "SubnetIds", "SubnetId");
        let kms_key_id = optional_query_param(request, "KmsKeyId");
        let user_group_id = optional_query_param(request, "UserGroupId");
        let snapshot_retention_limit =
            optional_non_negative_i32_param(request, "SnapshotRetentionLimit")?;
        let daily_snapshot_time = optional_query_param(request, "DailySnapshotTime");
        let network_type = optional_query_param(request, "NetworkType");
        if let Some(ref nt) = network_type {
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
        let connection_type = optional_query_param(request, "ConnectionType");
        validate_serverless_connection_type(
            connection_type.as_deref(),
            &engine,
            &major_engine_version,
        )?;
        let tags = parse_tags(request)?;

        let (arn, endpoint_address) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            if !state.begin_serverless_cache_creation(&serverless_cache_name) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ServerlessCacheAlreadyExistsFault",
                    format!("ServerlessCache {serverless_cache_name} already exists."),
                ));
            }

            if let Some(ref group_id) = user_group_id {
                let user_group_status = match state.user_groups.get(group_id) {
                    Some(user_group) => user_group.status.clone(),
                    None => {
                        state.cancel_serverless_cache_creation(&serverless_cache_name);
                        return Err(AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "UserGroupNotFound",
                            format!("User group {group_id} not found."),
                        ));
                    }
                };
                if user_group_status != "active" {
                    state.cancel_serverless_cache_creation(&serverless_cache_name);
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidUserGroupState",
                        format!("User group {group_id} is not in active state."),
                    ));
                }
            }

            let arn = elasticache_arn(
                request.region.as_str(),
                &state.account_id,
                "serverlesscache",
                &serverless_cache_name,
            );
            (arn, "127.0.0.1".to_string())
        };

        // The backing container is started off the request path (below); with a
        // runtime the cache begins with no endpoint port and is filled in once
        // ready, so the response doesn't block on the cold image pull +
        // readiness (which made the AWS CLI hit its 60s read timeout).
        // bug-audit 2026-05-28, 3.2.
        //
        // Without a runtime (the default in local/CI setups) the cache is
        // created directly as "available", so it must advertise a real port
        // immediately — the Docker-tick backfill never runs, so leaving it 0
        // makes every connection string end in `:0` (unconnectable). Use the
        // conventional engine port (6379 redis/valkey, 11211 memcached).
        // bug-hunt 2026-07-16, 1.7.
        let initial_port = if self.runtime.is_some() {
            0
        } else {
            engine_default_port(&engine)
        };
        let endpoint = ServerlessCacheEndpoint {
            address: endpoint_address.clone(),
            port: initial_port,
        };
        let reader_endpoint = ServerlessCacheEndpoint {
            address: endpoint_address,
            port: initial_port,
        };
        let cache = ServerlessCache {
            serverless_cache_name: serverless_cache_name.clone(),
            description,
            engine,
            major_engine_version,
            full_engine_version,
            status: if self.runtime.is_some() {
                "creating".to_string()
            } else {
                "available".to_string()
            },
            endpoint,
            reader_endpoint,
            arn: arn.clone(),
            created_at: chrono::Utc::now().to_rfc3339(),
            cache_usage_limits,
            security_group_ids,
            subnet_ids,
            kms_key_id,
            user_group_id,
            snapshot_retention_limit,
            daily_snapshot_time,
            network_type: Some(network_type.unwrap_or_else(|| "ipv4".to_string())),
            connection_type: Some(connection_type.unwrap_or_else(|| "vpc".to_string())),
            container_id: String::new(),
            host_port: 0,
            data_volume: Some(fakecloud_core::data_volume::DataVolumeBinding::Scoped),
            migration: None,
        };

        let xml = serverless_cache_xml(&cache);
        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            state.finish_serverless_cache_creation(cache.clone());
            if !tags.is_empty() {
                merge_tags(state.tags.entry(arn).or_default(), &tags);
            }
        }

        // Start the backing container off the request path and flip the cache
        // to "available" (filling in the endpoint ports) once it is ready.
        if let Some(runtime) = self.runtime.clone() {
            let state = self.state.clone();
            let snapshot_store = self.snapshot_store.clone();
            let snapshot_lock = self.snapshot_lock.clone();
            let account_id = request.account_id.clone();
            let name = serverless_cache_name.clone();
            // Reserved `fakecloud-k8s/*` scheduling tags from the create
            // request (ignored on the Docker backend).
            let pod_tags: std::collections::BTreeMap<String, String> =
                tags.iter().cloned().collect();
            // This incarnation's id and volume, pinned now (see
            // CreateCacheCluster).
            let (incarnation, volume) = {
                let accounts = self.state.read();
                match accounts
                    .get(&account_id)
                    .and_then(|s| s.serverless_caches.get(&name))
                {
                    Some(c) => (
                        c.incarnation(),
                        c.data_volume_name(
                            fakecloud_core::data_volume::current_scope().tag(),
                            &account_id,
                        ),
                    ),
                    None => (String::new(), String::new()),
                }
            };
            tokio::spawn(async move {
                if incarnation.is_empty() {
                    return;
                }
                let result = runtime
                    .ensure_redis(&incarnation, &account_id, &name, &volume, None, &pod_tags)
                    .await;
                let mut stop_container = false;
                {
                    let mut accounts = state.write();
                    if let Some(s) = accounts.get_mut(&account_id) {
                        match &result {
                            Ok(running) => {
                                if let Some(c) = s
                                    .serverless_caches
                                    .get_mut(&name)
                                    .filter(|c| c.incarnation() == incarnation)
                                {
                                    c.status = "available".to_string();
                                    c.endpoint.address = running.endpoint_address.clone();
                                    c.endpoint.port = running.endpoint_port;
                                    c.reader_endpoint.address = running.endpoint_address.clone();
                                    c.reader_endpoint.port = running.endpoint_port;
                                    c.host_port = running.host_port;
                                    c.container_id = running.container_id.clone();
                                } else {
                                    // Deleted during startup: the container came
                                    // up but the cache is gone — reap it after
                                    // the lock is released so it isn't orphaned.
                                    stop_container = true;
                                }
                            }
                            Err(error) => {
                                tracing::error!(
                                    %error,
                                    serverless_cache_name = %name,
                                    "failed to start elasticache serverless cache container",
                                );
                                match s
                                    .serverless_caches
                                    .get_mut(&name)
                                    .filter(|c| c.incarnation() == incarnation)
                                {
                                    Some(c) => c.status = "create-failed".to_string(),
                                    // Deleted while starting: the failed start may still
                                    // have created this incarnation's volume.
                                    None => stop_container = true,
                                }
                            }
                        }
                    } else {
                        stop_container = true;
                    }
                }
                if stop_container {
                    // Deleted (or reset) while it was starting: reap this
                    // incarnation's container and its volume.
                    super::reap_gone_start(&runtime, &incarnation, Some(&volume)).await;
                }
                save_snapshot_static(state, snapshot_store, snapshot_lock).await;
            });
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CreateServerlessCache",
                ELASTICACHE_NS,
                &format!("<ServerlessCache>{xml}</ServerlessCache>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn describe_serverless_caches(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let serverless_cache_name = optional_query_param(request, "ServerlessCacheName");
        let max_results = optional_usize_param(request, "MaxResults")?;
        let next_token = optional_query_param(request, "NextToken");

        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let caches: Vec<&ServerlessCache> = if let Some(ref name) = serverless_cache_name {
            match state.serverless_caches.get(name) {
                Some(cache) => vec![cache],
                None => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "ServerlessCacheNotFoundFault",
                        format!("ServerlessCache {name} not found."),
                    ));
                }
            }
        } else {
            let mut caches: Vec<&ServerlessCache> = state.serverless_caches.values().collect();
            caches.sort_by(|a, b| a.serverless_cache_name.cmp(&b.serverless_cache_name));
            caches
        };

        let (page, next_token) = paginate(&caches, next_token.as_deref(), max_results)?;
        let members_xml: String = page
            .iter()
            .map(|cache| format!("<member>{}</member>", serverless_cache_xml(cache)))
            .collect();
        let next_token_xml = next_token
            .map(|token| format!("<NextToken>{}</NextToken>", xml_escape(&token)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeServerlessCaches",
                ELASTICACHE_NS,
                &format!("<ServerlessCaches>{members_xml}</ServerlessCaches>{next_token_xml}"),
                &request.request_id,
            ),
        ))
    }

    pub(super) async fn delete_serverless_cache(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let serverless_cache_name = required_query_param(request, "ServerlessCacheName")?;

        let cache = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            if let Some(m) = state
                .serverless_caches
                .get(&serverless_cache_name)
                .and_then(|c| c.migration.as_ref())
                .filter(|m| m.is_active())
            {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidServerlessCacheStateFault",
                    format!(
                        "ServerlessCache {serverless_cache_name} has a migration in progress ({}); complete it before deleting the cache.",
                        m.status
                    ),
                ));
            }
            let cache = state
                .serverless_caches
                .remove(&serverless_cache_name)
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "ServerlessCacheNotFoundFault",
                        format!("ServerlessCache {serverless_cache_name} not found."),
                    )
                })?;
            state.tags.remove(&cache.arn);
            cache
        };

        if let Some(ref runtime) = self.runtime {
            // Serverless caches run on a durable Redis volume too: stop this
            // incarnation and drop its volume so a later cache reusing this
            // name starts clean instead of reloading the deleted one's data.
            runtime.stop(&cache.incarnation()).await;
            runtime
                .remove_data_volume_named(&cache.data_volume_name(
                    fakecloud_core::data_volume::current_scope().tag(),
                    &request.account_id,
                ))
                .await;
        }

        let mut deleted = cache;
        deleted.status = "deleting".to_string();
        let xml = serverless_cache_xml(&deleted);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DeleteServerlessCache",
                ELASTICACHE_NS,
                &format!("<ServerlessCache>{xml}</ServerlessCache>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn modify_serverless_cache(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let serverless_cache_name = required_query_param(request, "ServerlessCacheName")?;
        let description = optional_query_param(request, "Description");
        let cache_usage_limits = parse_cache_usage_limits(request)?;
        let security_group_ids =
            parse_query_list_param(request, "SecurityGroupIds", "SecurityGroupId");
        let user_group_id = optional_query_param(request, "UserGroupId");
        let snapshot_retention_limit =
            optional_non_negative_i32_param(request, "SnapshotRetentionLimit")?;
        let daily_snapshot_time = optional_query_param(request, "DailySnapshotTime");

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

        if let Some(ref group_id) = user_group_id {
            let user_group = state.user_groups.get(group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "UserGroupNotFound",
                    format!("User group {group_id} not found."),
                )
            })?;
            if user_group.status != "active" {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidUserGroupState",
                    format!("User group {group_id} is not in active state."),
                ));
            }
        }

        let cache = state
            .serverless_caches
            .get_mut(&serverless_cache_name)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ServerlessCacheNotFoundFault",
                    format!("ServerlessCache {serverless_cache_name} not found."),
                )
            })?;

        if let Some(description) = description {
            cache.description = description;
        }
        if cache_usage_limits.is_some() {
            cache.cache_usage_limits = cache_usage_limits;
        }
        if !security_group_ids.is_empty() {
            cache.security_group_ids = security_group_ids;
        }
        if let Some(user_group_id) = user_group_id {
            cache.user_group_id = Some(user_group_id);
        }
        if let Some(snapshot_retention_limit) = snapshot_retention_limit {
            cache.snapshot_retention_limit = Some(snapshot_retention_limit);
        }
        if let Some(daily_snapshot_time) = daily_snapshot_time {
            cache.daily_snapshot_time = Some(daily_snapshot_time);
        }

        let xml = serverless_cache_xml(cache);
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "ModifyServerlessCache",
                ELASTICACHE_NS,
                &format!("<ServerlessCache>{xml}</ServerlessCache>"),
                &request.request_id,
            ),
        ))
    }
}
