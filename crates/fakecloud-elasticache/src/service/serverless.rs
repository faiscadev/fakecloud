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

            let arn = format!(
                "arn:aws:elasticache:{}:{}:serverlesscache:{}",
                state.region, state.account_id, serverless_cache_name
            );
            (arn, "127.0.0.1".to_string())
        };

        let running = if let Some(runtime) = self.runtime.as_ref() {
            match runtime.ensure_redis(&serverless_cache_name, None).await {
                Ok(r) => r,
                Err(e) => {
                    self.state
                        .write()
                        .get_or_create(&request.account_id)
                        .cancel_serverless_cache_creation(&serverless_cache_name);
                    return Err(runtime_error_to_service_error(e));
                }
            }
        } else {
            // No container runtime: metadata-only serverless cache. Matches
            // the CreateCacheCluster / CreateReplicationGroup degradation.
            crate::runtime::RunningCacheContainer {
                container_id: String::new(),
                host_port: 0,
            }
        };

        let endpoint = ServerlessCacheEndpoint {
            address: endpoint_address.clone(),
            port: running.host_port,
        };
        let reader_endpoint = ServerlessCacheEndpoint {
            address: endpoint_address,
            port: running.host_port,
        };
        let cache = ServerlessCache {
            serverless_cache_name: serverless_cache_name.clone(),
            description,
            engine,
            major_engine_version,
            full_engine_version,
            status: "available".to_string(),
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
            container_id: running.container_id,
            host_port: running.host_port,
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

        let (page, next_token) = paginate(&caches, next_token.as_deref(), max_results);
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
            runtime.stop_container(&serverless_cache_name).await;
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
