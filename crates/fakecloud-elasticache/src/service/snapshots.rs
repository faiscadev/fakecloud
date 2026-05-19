//! ElastiCache `snapshots` family handlers extracted from service.rs
//! by audit-2026-05-19 file-split.

use super::*;

impl ElastiCacheService {
    pub(super) fn create_serverless_cache_snapshot(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let serverless_cache_name = required_query_param(request, "ServerlessCacheName")?;
        let serverless_cache_snapshot_name =
            required_query_param(request, "ServerlessCacheSnapshotName")?;
        let kms_key_id = optional_query_param(request, "KmsKeyId");
        let tags = parse_tags(request)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        if state
            .serverless_cache_snapshots
            .contains_key(&serverless_cache_snapshot_name)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ServerlessCacheSnapshotAlreadyExistsFault",
                format!("ServerlessCacheSnapshot {serverless_cache_snapshot_name} already exists."),
            ));
        }

        let cache = state
            .serverless_caches
            .get(&serverless_cache_name)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ServerlessCacheNotFoundFault",
                    format!("ServerlessCache {serverless_cache_name} not found."),
                )
            })?;

        let arn = format!(
            "arn:aws:elasticache:{}:{}:serverlesssnapshot:{}",
            state.region, state.account_id, serverless_cache_snapshot_name
        );
        let snapshot = ServerlessCacheSnapshot {
            serverless_cache_snapshot_name: serverless_cache_snapshot_name.clone(),
            arn: arn.clone(),
            kms_key_id: kms_key_id.or_else(|| cache.kms_key_id.clone()),
            snapshot_type: "manual".to_string(),
            status: "available".to_string(),
            create_time: chrono::Utc::now().to_rfc3339(),
            expiry_time: None,
            bytes_used_for_cache: None,
            serverless_cache_name: cache.serverless_cache_name.clone(),
            engine: cache.engine.clone(),
            major_engine_version: cache.major_engine_version.clone(),
        };

        let xml = serverless_cache_snapshot_xml(&snapshot);
        state.tags.insert(arn.clone(), Vec::new());
        if !tags.is_empty() {
            merge_tags(state.tags.entry(arn).or_default(), &tags);
        }
        state
            .serverless_cache_snapshots
            .insert(serverless_cache_snapshot_name, snapshot);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CreateServerlessCacheSnapshot",
                ELASTICACHE_NS,
                &format!("<ServerlessCacheSnapshot>{xml}</ServerlessCacheSnapshot>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn describe_serverless_cache_snapshots(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let serverless_cache_name = optional_query_param(request, "ServerlessCacheName");
        let serverless_cache_snapshot_name =
            optional_query_param(request, "ServerlessCacheSnapshotName");
        let snapshot_type = optional_query_param(request, "SnapshotType");
        let max_results = optional_usize_param(request, "MaxResults")?;
        let next_token = optional_query_param(request, "NextToken");

        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let snapshots: Vec<&ServerlessCacheSnapshot> =
            if let Some(ref snapshot_name) = serverless_cache_snapshot_name {
                match state.serverless_cache_snapshots.get(snapshot_name) {
                    Some(snapshot) => vec![snapshot],
                    None => {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "ServerlessCacheSnapshotNotFoundFault",
                            format!("ServerlessCacheSnapshot {snapshot_name} not found."),
                        ));
                    }
                }
            } else {
                if let Some(ref cache_name) = serverless_cache_name {
                    if !state.serverless_caches.contains_key(cache_name) {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::NOT_FOUND,
                            "ServerlessCacheNotFoundFault",
                            format!("ServerlessCache {cache_name} not found."),
                        ));
                    }
                }

                let mut snapshots: Vec<&ServerlessCacheSnapshot> = state
                    .serverless_cache_snapshots
                    .values()
                    .filter(|snapshot| {
                        serverless_cache_name
                            .as_ref()
                            .is_none_or(|name| snapshot.serverless_cache_name == *name)
                    })
                    .filter(|snapshot| {
                        snapshot_type
                            .as_ref()
                            .is_none_or(|value| snapshot.snapshot_type == *value)
                    })
                    .collect();
                snapshots.sort_by(|a, b| {
                    a.serverless_cache_snapshot_name
                        .cmp(&b.serverless_cache_snapshot_name)
                });
                snapshots
            };

        let (page, next_token) = paginate(&snapshots, next_token.as_deref(), max_results);
        let members_xml: String = page
            .iter()
            .map(|snapshot| {
                format!(
                    "<ServerlessCacheSnapshot>{}</ServerlessCacheSnapshot>",
                    serverless_cache_snapshot_xml(snapshot)
                )
            })
            .collect();
        let next_token_xml = next_token
            .map(|token| format!("<NextToken>{}</NextToken>", xml_escape(&token)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeServerlessCacheSnapshots", ELASTICACHE_NS,
                &format!(
                    "<ServerlessCacheSnapshots>{members_xml}</ServerlessCacheSnapshots>{next_token_xml}"
                ),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn delete_serverless_cache_snapshot(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let serverless_cache_snapshot_name =
            required_query_param(request, "ServerlessCacheSnapshotName")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let mut snapshot = state
            .serverless_cache_snapshots
            .remove(&serverless_cache_snapshot_name)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ServerlessCacheSnapshotNotFoundFault",
                    format!("ServerlessCacheSnapshot {serverless_cache_snapshot_name} not found."),
                )
            })?;
        state.tags.remove(&snapshot.arn);

        snapshot.status = "deleting".to_string();
        let xml = serverless_cache_snapshot_xml(&snapshot);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DeleteServerlessCacheSnapshot",
                ELASTICACHE_NS,
                &format!("<ServerlessCacheSnapshot>{xml}</ServerlessCacheSnapshot>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) async fn create_snapshot(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let snapshot_name = required_query_param(request, "SnapshotName")?;
        let replication_group_id = optional_query_param(request, "ReplicationGroupId");
        let cache_cluster_id = optional_query_param(request, "CacheClusterId");

        if replication_group_id.is_none() && cache_cluster_id.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterCombination",
                "At least one of ReplicationGroupId or CacheClusterId must be specified."
                    .to_string(),
            ));
        }

        let (mut snapshot, arn, group_id) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);

            if state.snapshots.contains_key(&snapshot_name) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "SnapshotAlreadyExistsFault",
                    format!("Snapshot {snapshot_name} already exists."),
                ));
            }

            // Resolve the replication group: either directly by ID or via CacheClusterId
            let group_id = if let Some(ref rg_id) = replication_group_id {
                rg_id.clone()
            } else {
                let cluster_id = cache_cluster_id.as_ref().unwrap();
                if let Some(cluster) = state.cache_clusters.get(cluster_id) {
                    if let Some(group_id) = cluster.replication_group_id.clone() {
                        group_id
                    } else {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidParameterCombination",
                            format!(
                                "CacheCluster {cluster_id} is not associated with a replication group."
                            ),
                        ));
                    }
                } else {
                    // CacheClusterId may also map to a member cluster like "rg-001", find parent group
                    state
                        .replication_groups
                        .values()
                        .find(|g| g.member_clusters.contains(cluster_id))
                        .map(|g| g.replication_group_id.clone())
                        .ok_or_else(|| {
                            AwsServiceError::aws_error(
                                StatusCode::NOT_FOUND,
                                "CacheClusterNotFound",
                                format!("CacheCluster {cluster_id} not found."),
                            )
                        })?
                }
            };

            let group = state.replication_groups.get(&group_id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ReplicationGroupNotFoundFault",
                    format!("ReplicationGroup {group_id} not found."),
                )
            })?;

            let arn = format!(
                "arn:aws:elasticache:{}:{}:snapshot:{}",
                state.region, state.account_id, snapshot_name
            );

            let snapshot = CacheSnapshot {
                snapshot_name: snapshot_name.clone(),
                replication_group_id: group.replication_group_id.clone(),
                replication_group_description: group.description.clone(),
                snapshot_status: "available".to_string(),
                cache_node_type: group.cache_node_type.clone(),
                engine: group.engine.clone(),
                engine_version: group.engine_version.clone(),
                num_cache_clusters: group.num_cache_clusters,
                arn: arn.clone(),
                created_at: chrono::Utc::now().to_rfc3339(),
                snapshot_source: "manual".to_string(),
                rdb_path: None,
            };
            (snapshot, arn, group_id)
        };

        if let Some(ref runtime) = self.runtime {
            let tmp_path = format!(
                "/tmp/fakecloud-ec-{}-{}-{}.rdb",
                request.account_id,
                snapshot_name,
                std::process::id()
            );
            match runtime.dump_rdb(&group_id, &tmp_path).await {
                Ok(()) => {
                    snapshot.rdb_path = Some(tmp_path);
                }
                Err(err) => {
                    tracing::warn!("Failed to dump RDB for snapshot {}: {}", snapshot_name, err);
                }
            }
        }

        let xml = snapshot_xml(&snapshot);
        {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            if state.snapshots.contains_key(&snapshot_name) {
                if let Some(ref path) = snapshot.rdb_path {
                    let _ = std::fs::remove_file(path);
                }
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "SnapshotAlreadyExistsFault",
                    format!("Snapshot {snapshot_name} already exists."),
                ));
            }
            state.tags.insert(arn, Vec::new());
            state.snapshots.insert(snapshot_name, snapshot);
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CreateSnapshot",
                ELASTICACHE_NS,
                &format!("<Snapshot>{xml}</Snapshot>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn describe_snapshots(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let snapshot_name = optional_query_param(request, "SnapshotName");
        let replication_group_id = optional_query_param(request, "ReplicationGroupId");
        let cache_cluster_id = optional_query_param(request, "CacheClusterId");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_query_param(request, "Marker");

        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);

        let snapshots: Vec<&CacheSnapshot> = if let Some(ref name) = snapshot_name {
            match state.snapshots.get(name) {
                Some(s) => vec![s],
                None => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "SnapshotNotFoundFault",
                        format!("Snapshot {name} not found."),
                    ));
                }
            }
        } else {
            let mut snaps: Vec<&CacheSnapshot> = state
                .snapshots
                .values()
                .filter(|s| {
                    replication_group_id
                        .as_ref()
                        .is_none_or(|id| s.replication_group_id == *id)
                })
                .filter(|s| {
                    cache_cluster_id.as_ref().is_none_or(|cluster_id| {
                        state.cache_clusters.get(cluster_id).is_some_and(|cluster| {
                            cluster.replication_group_id.as_deref() == Some(&s.replication_group_id)
                        }) || state
                            .replication_groups
                            .get(&s.replication_group_id)
                            .is_some_and(|g| g.member_clusters.contains(cluster_id))
                    })
                })
                .collect();
            snaps.sort_by(|a, b| a.snapshot_name.cmp(&b.snapshot_name));
            snaps
        };

        let (page, next_marker) = paginate(&snapshots, marker.as_deref(), max_records);

        let members_xml: String = page
            .iter()
            .map(|s| format!("<Snapshot>{}</Snapshot>", snapshot_xml(s)))
            .collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeSnapshots",
                ELASTICACHE_NS,
                &format!("<Snapshots>{members_xml}</Snapshots>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn delete_snapshot(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let snapshot_name = required_query_param(request, "SnapshotName")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let mut snapshot = state.snapshots.remove(&snapshot_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "SnapshotNotFoundFault",
                format!("Snapshot {snapshot_name} not found."),
            )
        })?;
        state.tags.remove(&snapshot.arn);

        snapshot.snapshot_status = "deleting".to_string();
        let xml = snapshot_xml(&snapshot);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DeleteSnapshot",
                ELASTICACHE_NS,
                &format!("<Snapshot>{xml}</Snapshot>"),
                &request.request_id,
            ),
        ))
    }

    // ── Snapshots ──

    pub(super) fn copy_snapshot(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let source = required_query_param(request, "SourceSnapshotName")?;
        let target = required_query_param(request, "TargetSnapshotName")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        let mut snap = state.snapshots.get(&source).cloned().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "SnapshotNotFoundFault",
                format!("Snapshot {source} not found."),
            )
        })?;
        if state.snapshots.contains_key(&target) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "SnapshotAlreadyExistsFault",
                format!("Snapshot {target} already exists."),
            ));
        }
        snap.snapshot_name = target.clone();
        snap.arn = format!(
            "arn:aws:elasticache:{}:{}:snapshot:{}",
            state.region, state.account_id, target
        );
        snap.snapshot_status = "creating".to_string();
        snap.snapshot_source = "manual".to_string();
        let xml = snapshot_xml(&snap);
        state.snapshots.insert(target, snap);
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CopySnapshot",
                ELASTICACHE_NS,
                &format!("<Snapshot>{xml}</Snapshot>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn copy_serverless_cache_snapshot(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let source = required_query_param(request, "SourceServerlessCacheSnapshotName")?;
        let target = required_query_param(request, "TargetServerlessCacheSnapshotName")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);
        if state.serverless_cache_snapshots.contains_key(&target) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ServerlessCacheSnapshotAlreadyExistsFault",
                format!("ServerlessCacheSnapshot {target} already exists."),
            ));
        }
        let mut snap = state
            .serverless_cache_snapshots
            .get(&source)
            .cloned()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ServerlessCacheSnapshotNotFoundFault",
                    format!("ServerlessCacheSnapshot {source} not found."),
                )
            })?;
        snap.serverless_cache_snapshot_name = target.clone();
        snap.arn = format!(
            "arn:aws:elasticache:{}:{}:serverlesssnapshot:{}",
            state.region, state.account_id, target
        );
        snap.status = "creating".to_string();
        let xml = serverless_cache_snapshot_xml(&snap);
        state.serverless_cache_snapshots.insert(target, snap);
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "CopyServerlessCacheSnapshot",
                ELASTICACHE_NS,
                &format!("<ServerlessCacheSnapshot>{xml}</ServerlessCacheSnapshot>"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn export_serverless_cache_snapshot(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let snap_name = required_query_param(request, "ServerlessCacheSnapshotName")?;
        let bucket = required_query_param(request, "S3BucketName")?;
        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);
        let snap = state
            .serverless_cache_snapshots
            .get(&snap_name)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ServerlessCacheSnapshotNotFoundFault",
                    format!("ServerlessCacheSnapshot {snap_name} not found."),
                )
            })?;
        let xml = serverless_cache_snapshot_xml(snap);
        let _ = bucket;
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "ExportServerlessCacheSnapshot",
                ELASTICACHE_NS,
                &format!("<ServerlessCacheSnapshot>{xml}</ServerlessCacheSnapshot>"),
                &request.request_id,
            ),
        ))
    }
}
