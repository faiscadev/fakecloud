use std::convert::TryFrom;
use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;

use fakecloud_aws::xml::xml_escape;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::runtime::{ElastiCacheRuntime, RuntimeError};
use crate::state::{
    default_engine_versions, default_parameters_for_family, CacheCluster, CacheEngineVersion,
    CacheParameterGroup, CacheSnapshot, CacheSubnetGroup, ElastiCacheState, ElastiCacheUser,
    ElastiCacheUserGroup, EngineDefaultParameter, ReplicationGroup, ServerlessCache,
    ServerlessCacheDataStorage, ServerlessCacheEcpuPerSecond, ServerlessCacheEndpoint,
    ServerlessCacheSnapshot, ServerlessCacheUsageLimits, SharedElastiCacheState,
};

const ELASTICACHE_NS: &str = "http://elasticache.amazonaws.com/doc/2015-02-02/";
const SUPPORTED_ACTIONS: &[&str] = &[
    "AddTagsToResource",
    "CreateCacheCluster",
    "CreateCacheSubnetGroup",
    "CreateReplicationGroup",
    "CreateServerlessCache",
    "CreateServerlessCacheSnapshot",
    "CreateSnapshot",
    "CreateUser",
    "CreateUserGroup",
    "DecreaseReplicaCount",
    "DeleteCacheCluster",
    "DeleteCacheSubnetGroup",
    "DeleteReplicationGroup",
    "DeleteServerlessCache",
    "DeleteServerlessCacheSnapshot",
    "DeleteSnapshot",
    "DeleteUser",
    "DeleteUserGroup",
    "DescribeCacheClusters",
    "DescribeCacheEngineVersions",
    "DescribeCacheParameterGroups",
    "DescribeCacheSubnetGroups",
    "DescribeEngineDefaultParameters",
    "DescribeReplicationGroups",
    "DescribeServerlessCaches",
    "DescribeServerlessCacheSnapshots",
    "DescribeSnapshots",
    "DescribeUserGroups",
    "DescribeUsers",
    "IncreaseReplicaCount",
    "ListTagsForResource",
    "ModifyCacheSubnetGroup",
    "ModifyReplicationGroup",
    "ModifyServerlessCache",
    "RemoveTagsFromResource",
    "TestFailover",
];

pub struct ElastiCacheService {
    state: SharedElastiCacheState,
    runtime: Option<Arc<ElastiCacheRuntime>>,
}

impl ElastiCacheService {
    pub fn new(state: SharedElastiCacheState) -> Self {
        Self {
            state,
            runtime: None,
        }
    }

    pub fn with_runtime(mut self, runtime: Arc<ElastiCacheRuntime>) -> Self {
        self.runtime = Some(runtime);
        self
    }
}

#[async_trait]
impl AwsService for ElastiCacheService {
    fn service_name(&self) -> &str {
        "elasticache"
    }

    async fn handle(&self, request: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match request.action.as_str() {
            "AddTagsToResource" => self.add_tags_to_resource(&request),
            "CreateCacheCluster" => self.create_cache_cluster(&request).await,
            "CreateCacheSubnetGroup" => self.create_cache_subnet_group(&request),
            "CreateReplicationGroup" => self.create_replication_group(&request).await,
            "CreateServerlessCache" => self.create_serverless_cache(&request).await,
            "CreateServerlessCacheSnapshot" => self.create_serverless_cache_snapshot(&request),
            "CreateSnapshot" => self.create_snapshot(&request),
            "CreateUser" => self.create_user(&request),
            "CreateUserGroup" => self.create_user_group(&request),
            "DecreaseReplicaCount" => self.decrease_replica_count(&request),
            "DeleteCacheCluster" => self.delete_cache_cluster(&request).await,
            "DeleteCacheSubnetGroup" => self.delete_cache_subnet_group(&request),
            "DeleteReplicationGroup" => self.delete_replication_group(&request).await,
            "DeleteServerlessCache" => self.delete_serverless_cache(&request).await,
            "DeleteServerlessCacheSnapshot" => self.delete_serverless_cache_snapshot(&request),
            "DeleteSnapshot" => self.delete_snapshot(&request),
            "DeleteUser" => self.delete_user(&request),
            "DeleteUserGroup" => self.delete_user_group(&request),
            "DescribeCacheClusters" => self.describe_cache_clusters(&request),
            "DescribeCacheEngineVersions" => self.describe_cache_engine_versions(&request),
            "DescribeCacheParameterGroups" => self.describe_cache_parameter_groups(&request),
            "DescribeCacheSubnetGroups" => self.describe_cache_subnet_groups(&request),
            "DescribeEngineDefaultParameters" => self.describe_engine_default_parameters(&request),
            "DescribeReplicationGroups" => self.describe_replication_groups(&request),
            "DescribeServerlessCaches" => self.describe_serverless_caches(&request),
            "DescribeServerlessCacheSnapshots" => {
                self.describe_serverless_cache_snapshots(&request)
            }
            "DescribeSnapshots" => self.describe_snapshots(&request),
            "DescribeUserGroups" => self.describe_user_groups(&request),
            "DescribeUsers" => self.describe_users(&request),
            "IncreaseReplicaCount" => self.increase_replica_count(&request),
            "ListTagsForResource" => self.list_tags_for_resource(&request),
            "ModifyCacheSubnetGroup" => self.modify_cache_subnet_group(&request),
            "ModifyReplicationGroup" => self.modify_replication_group(&request),
            "ModifyServerlessCache" => self.modify_serverless_cache(&request),
            "RemoveTagsFromResource" => self.remove_tags_from_resource(&request),
            "TestFailover" => self.test_failover(&request),
            _ => Err(AwsServiceError::action_not_implemented(
                self.service_name(),
                &request.action,
            )),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        SUPPORTED_ACTIONS
    }
}

impl ElastiCacheService {
    fn describe_cache_engine_versions(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let engine = optional_param(request, "Engine");
        let engine_version = optional_param(request, "EngineVersion");
        let family = optional_param(request, "CacheParameterGroupFamily");
        let default_only = parse_optional_bool(optional_param(request, "DefaultOnly").as_deref())?;
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_param(request, "Marker");

        let mut versions = filter_engine_versions(
            &default_engine_versions(),
            &engine,
            &engine_version,
            &family,
        );

        if default_only.unwrap_or(false) {
            // Keep only one version per engine (the latest)
            let mut seen_engines = std::collections::HashSet::new();
            versions.retain(|v| seen_engines.insert(v.engine.clone()));
        }

        let (page, next_marker) = paginate(&versions, marker.as_deref(), max_records);

        let members_xml: String = page.iter().map(engine_version_xml).collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DescribeCacheEngineVersions",
                &format!("<CacheEngineVersions>{members_xml}</CacheEngineVersions>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    fn describe_cache_parameter_groups(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group_name = optional_param(request, "CacheParameterGroupName");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_param(request, "Marker");

        let state = self.state.read();

        let groups: Vec<&CacheParameterGroup> = state
            .parameter_groups
            .iter()
            .filter(|g| {
                group_name
                    .as_ref()
                    .is_none_or(|name| g.cache_parameter_group_name == *name)
            })
            .collect();

        if let Some(ref name) = group_name {
            if groups.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "CacheParameterGroupNotFound",
                    format!("CacheParameterGroup {name} not found."),
                ));
            }
        }

        let (page, next_marker) = paginate(&groups, marker.as_deref(), max_records);

        let members_xml: String = page.iter().map(|g| cache_parameter_group_xml(g)).collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DescribeCacheParameterGroups",
                &format!("<CacheParameterGroups>{members_xml}</CacheParameterGroups>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    fn describe_engine_default_parameters(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let family = required_param(request, "CacheParameterGroupFamily")?;
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_param(request, "Marker");

        let params = default_parameters_for_family(&family);
        let (page, next_marker) = paginate(&params, marker.as_deref(), max_records);

        let params_xml: String = page.iter().map(parameter_xml).collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DescribeEngineDefaultParameters",
                &format!(
                    "<EngineDefaults>\
                     <CacheParameterGroupFamily>{}</CacheParameterGroupFamily>\
                     <Parameters>{params_xml}</Parameters>\
                     {marker_xml}\
                     </EngineDefaults>",
                    xml_escape(&family),
                ),
                &request.request_id,
            ),
        ))
    }

    fn create_cache_subnet_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(request, "CacheSubnetGroupName")?;
        let description = required_param(request, "CacheSubnetGroupDescription")?;
        let subnet_ids = parse_member_list(&request.query_params, "SubnetIds", "SubnetIdentifier");

        if subnet_ids.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                "At least one subnet ID must be specified.".to_string(),
            ));
        }

        let mut state = self.state.write();

        if state.subnet_groups.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CacheSubnetGroupAlreadyExists",
                format!("Cache subnet group {name} already exists."),
            ));
        }

        let arn = format!(
            "arn:aws:elasticache:{}:{}:subnetgroup:{}",
            state.region, state.account_id, name
        );
        let vpc_id = format!(
            "vpc-{:08x}",
            name.as_bytes()
                .iter()
                .fold(0u32, |acc, &b| acc.wrapping_add(b as u32))
        );

        let group = CacheSubnetGroup {
            cache_subnet_group_name: name.clone(),
            cache_subnet_group_description: description,
            vpc_id,
            subnet_ids,
            arn,
        };

        let xml = cache_subnet_group_xml(&group, &state.region);
        state.register_arn(&group.arn);
        state.subnet_groups.insert(name, group);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "CreateCacheSubnetGroup",
                &format!("<CacheSubnetGroup>{xml}</CacheSubnetGroup>"),
                &request.request_id,
            ),
        ))
    }

    fn describe_cache_subnet_groups(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group_name = optional_param(request, "CacheSubnetGroupName");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_param(request, "Marker");

        let state = self.state.read();

        let groups: Vec<&CacheSubnetGroup> = if let Some(ref name) = group_name {
            match state.subnet_groups.get(name) {
                Some(g) => vec![g],
                None => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "CacheSubnetGroupNotFoundFault",
                        format!("Cache subnet group {name} not found."),
                    ));
                }
            }
        } else {
            let mut groups: Vec<&CacheSubnetGroup> = state.subnet_groups.values().collect();
            groups.sort_by(|a, b| a.cache_subnet_group_name.cmp(&b.cache_subnet_group_name));
            groups
        };

        let (page, next_marker) = paginate(&groups, marker.as_deref(), max_records);

        let members_xml: String = page
            .iter()
            .map(|g| {
                format!(
                    "<CacheSubnetGroup>{}</CacheSubnetGroup>",
                    cache_subnet_group_xml(g, &state.region)
                )
            })
            .collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DescribeCacheSubnetGroups",
                &format!("<CacheSubnetGroups>{members_xml}</CacheSubnetGroups>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    fn delete_cache_subnet_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(request, "CacheSubnetGroupName")?;

        let mut state = self.state.write();

        if name == "default" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "CacheSubnetGroupInUse",
                "Cannot delete default cache subnet group.".to_string(),
            ));
        }

        if let Some(group) = state.subnet_groups.remove(&name) {
            state.tags.remove(&group.arn);
        } else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "CacheSubnetGroupNotFoundFault",
                format!("Cache subnet group {name} not found."),
            ));
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap("DeleteCacheSubnetGroup", "", &request.request_id),
        ))
    }

    fn modify_cache_subnet_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(request, "CacheSubnetGroupName")?;
        let description = optional_param(request, "CacheSubnetGroupDescription");
        let subnet_ids = parse_member_list(&request.query_params, "SubnetIds", "SubnetIdentifier");

        let mut state = self.state.write();
        let region = state.region.clone();

        let group = state.subnet_groups.get_mut(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "CacheSubnetGroupNotFoundFault",
                format!("Cache subnet group {name} not found."),
            )
        })?;

        if let Some(desc) = description {
            group.cache_subnet_group_description = desc;
        }
        if !subnet_ids.is_empty() {
            group.subnet_ids = subnet_ids;
        }

        let xml = cache_subnet_group_xml(group, &region);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "ModifyCacheSubnetGroup",
                &format!("<CacheSubnetGroup>{xml}</CacheSubnetGroup>"),
                &request.request_id,
            ),
        ))
    }

    async fn create_cache_cluster(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cache_cluster_id = required_param(request, "CacheClusterId")?;
        let engine = optional_param(request, "Engine").unwrap_or_else(|| "redis".to_string());
        if engine != "redis" && engine != "valkey" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("Invalid value for Engine: {engine}. Supported engines: redis, valkey"),
            ));
        }

        let default_version = if engine == "valkey" { "8.0" } else { "7.1" };
        let engine_version =
            optional_param(request, "EngineVersion").unwrap_or_else(|| default_version.to_string());
        let cache_node_type = optional_param(request, "CacheNodeType")
            .unwrap_or_else(|| "cache.t3.micro".to_string());
        let num_cache_nodes = match optional_param(request, "NumCacheNodes") {
            Some(v) => {
                let n = v.parse::<i32>().map_err(|_| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!("Invalid value for NumCacheNodes: '{v}'"),
                    )
                })?;
                if n < 1 {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!("NumCacheNodes must be a positive integer, got {n}"),
                    ));
                }
                n
            }
            None => 1,
        };
        let cache_subnet_group_name =
            optional_param(request, "CacheSubnetGroupName").or_else(|| Some("default".to_string()));
        let replication_group_id = optional_param(request, "ReplicationGroupId");
        let auto_minor_version_upgrade =
            parse_optional_bool(optional_param(request, "AutoMinorVersionUpgrade").as_deref())?
                .unwrap_or(true);

        let (preferred_availability_zone, arn) = {
            let mut state = self.state.write();
            if !state.begin_cache_cluster_creation(&cache_cluster_id) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "CacheClusterAlreadyExists",
                    format!("CacheCluster {cache_cluster_id} already exists."),
                ));
            }

            if let Some(ref subnet_group_name) = cache_subnet_group_name {
                if !state.subnet_groups.contains_key(subnet_group_name) {
                    state.cancel_cache_cluster_creation(&cache_cluster_id);
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "CacheSubnetGroupNotFoundFault",
                        format!("Cache subnet group {subnet_group_name} not found."),
                    ));
                }
            }

            if let Some(ref group_id) = replication_group_id {
                if !state.replication_groups.contains_key(group_id) {
                    state.cancel_cache_cluster_creation(&cache_cluster_id);
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "ReplicationGroupNotFoundFault",
                        format!("ReplicationGroup {group_id} not found."),
                    ));
                }
            }

            let preferred_availability_zone = optional_param(request, "PreferredAvailabilityZone")
                .unwrap_or_else(|| format!("{}a", state.region));
            let arn = format!(
                "arn:aws:elasticache:{}:{}:cluster:{}",
                state.region, state.account_id, cache_cluster_id
            );
            (preferred_availability_zone, arn)
        };

        let runtime = self.runtime.as_ref().ok_or_else(|| {
            self.state
                .write()
                .cancel_cache_cluster_creation(&cache_cluster_id);
            AwsServiceError::aws_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "InvalidParameterValue",
                "Docker/Podman is required for ElastiCache cache clusters but is not available"
                    .to_string(),
            )
        })?;

        let running = match runtime.ensure_redis(&cache_cluster_id).await {
            Ok(r) => r,
            Err(e) => {
                self.state
                    .write()
                    .cancel_cache_cluster_creation(&cache_cluster_id);
                return Err(runtime_error_to_service_error(e));
            }
        };

        let cluster = CacheCluster {
            cache_cluster_id: cache_cluster_id.clone(),
            cache_node_type,
            engine,
            engine_version,
            cache_cluster_status: "available".to_string(),
            num_cache_nodes,
            preferred_availability_zone,
            cache_subnet_group_name,
            auto_minor_version_upgrade,
            arn,
            created_at: chrono::Utc::now().to_rfc3339(),
            endpoint_address: "127.0.0.1".to_string(),
            endpoint_port: running.host_port,
            container_id: running.container_id,
            host_port: running.host_port,
            replication_group_id,
        };

        let xml = cache_cluster_xml(&cluster, true);
        {
            let mut state = self.state.write();
            state.finish_cache_cluster_creation(cluster.clone());
            if let Some(ref group_id) = cluster.replication_group_id {
                add_cluster_to_replication_group(&mut state, group_id, &cluster.cache_cluster_id);
            }
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "CreateCacheCluster",
                &format!("<CacheCluster>{xml}</CacheCluster>"),
                &request.request_id,
            ),
        ))
    }

    fn describe_cache_clusters(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cache_cluster_id = optional_param(request, "CacheClusterId");
        let show_cache_node_info =
            parse_optional_bool(optional_param(request, "ShowCacheNodeInfo").as_deref())?
                .unwrap_or(false);
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_param(request, "Marker");

        let state = self.state.read();
        let clusters: Vec<&CacheCluster> = if let Some(ref cluster_id) = cache_cluster_id {
            match state.cache_clusters.get(cluster_id) {
                Some(cluster) => vec![cluster],
                None => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "CacheClusterNotFound",
                        format!("CacheCluster {cluster_id} not found."),
                    ));
                }
            }
        } else {
            let mut clusters: Vec<&CacheCluster> = state.cache_clusters.values().collect();
            clusters.sort_by(|a, b| a.cache_cluster_id.cmp(&b.cache_cluster_id));
            clusters
        };

        let (page, next_marker) = paginate(&clusters, marker.as_deref(), max_records);
        let members_xml: String = page
            .iter()
            .map(|cluster| {
                format!(
                    "<CacheCluster>{}</CacheCluster>",
                    cache_cluster_xml(cluster, show_cache_node_info)
                )
            })
            .collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DescribeCacheClusters",
                &format!("<CacheClusters>{members_xml}</CacheClusters>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    async fn delete_cache_cluster(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let cache_cluster_id = required_param(request, "CacheClusterId")?;

        let cluster = {
            let mut state = self.state.write();
            let cluster = state
                .cache_clusters
                .remove(&cache_cluster_id)
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "CacheClusterNotFound",
                        format!("CacheCluster {cache_cluster_id} not found."),
                    )
                })?;
            if let Some(ref group_id) = cluster.replication_group_id {
                remove_cluster_from_replication_group(
                    &mut state,
                    group_id,
                    &cluster.cache_cluster_id,
                );
            }
            state.tags.remove(&cluster.arn);
            cluster
        };

        if let Some(ref runtime) = self.runtime {
            runtime.stop_container(&cache_cluster_id).await;
        }

        let mut deleted_cluster = cluster;
        deleted_cluster.cache_cluster_status = "deleting".to_string();
        let xml = cache_cluster_xml(&deleted_cluster, true);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DeleteCacheCluster",
                &format!("<CacheCluster>{xml}</CacheCluster>"),
                &request.request_id,
            ),
        ))
    }

    async fn create_replication_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let replication_group_id = required_param(request, "ReplicationGroupId")?;
        let description = required_param(request, "ReplicationGroupDescription")?;
        let engine = optional_param(request, "Engine").unwrap_or_else(|| "redis".to_string());
        if engine != "redis" && engine != "valkey" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("Invalid value for Engine: {engine}. Supported engines: redis, valkey"),
            ));
        }
        let default_version = if engine == "valkey" { "8.0" } else { "7.1" };
        let engine_version =
            optional_param(request, "EngineVersion").unwrap_or_else(|| default_version.to_string());
        let cache_node_type = optional_param(request, "CacheNodeType")
            .unwrap_or_else(|| "cache.t3.micro".to_string());
        let num_cache_clusters = match optional_param(request, "NumCacheClusters") {
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
        let automatic_failover =
            parse_optional_bool(optional_param(request, "AutomaticFailoverEnabled").as_deref())?
                .unwrap_or(false);
        // Reserve the ID under a write lock before starting the container.
        {
            let mut state = self.state.write();
            if !state.begin_replication_group_creation(&replication_group_id) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ReplicationGroupAlreadyExistsFault",
                    format!("ReplicationGroup {replication_group_id} already exists."),
                ));
            }
        }

        let runtime = self.runtime.as_ref().ok_or_else(|| {
            self.state
                .write()
                .cancel_replication_group_creation(&replication_group_id);
            AwsServiceError::aws_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "InvalidParameterValue",
                "Docker/Podman is required for ElastiCache replication groups but is not available"
                    .to_string(),
            )
        })?;

        let running = match runtime.ensure_redis(&replication_group_id).await {
            Ok(r) => r,
            Err(e) => {
                self.state
                    .write()
                    .cancel_replication_group_creation(&replication_group_id);
                return Err(runtime_error_to_service_error(e));
            }
        };

        let member_clusters: Vec<String> = (1..=num_cache_clusters)
            .map(|i| format!("{replication_group_id}-{i:03}"))
            .collect();

        let (arn, region) = {
            let state = self.state.read();
            let arn = format!(
                "arn:aws:elasticache:{}:{}:replicationgroup:{}",
                state.region, state.account_id, replication_group_id
            );
            (arn, state.region.clone())
        };

        let group = ReplicationGroup {
            replication_group_id: replication_group_id.clone(),
            description,
            status: "available".to_string(),
            cache_node_type,
            engine,
            engine_version,
            num_cache_clusters,
            automatic_failover_enabled: automatic_failover,
            endpoint_address: "127.0.0.1".to_string(),
            endpoint_port: running.host_port,
            arn,
            created_at: chrono::Utc::now().to_rfc3339(),
            container_id: running.container_id,
            host_port: running.host_port,
            member_clusters,
            snapshot_retention_limit: 0,
            snapshot_window: "05:00-09:00".to_string(),
        };

        let xml = replication_group_xml(&group, &region);
        self.state.write().finish_replication_group_creation(group);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "CreateReplicationGroup",
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    fn describe_replication_groups(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group_id = optional_param(request, "ReplicationGroupId");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_param(request, "Marker");

        let state = self.state.read();
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

        let (page, next_marker) = paginate(&groups, marker.as_deref(), max_records);

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
            xml_wrap(
                "DescribeReplicationGroups",
                &format!("<ReplicationGroups>{members_xml}</ReplicationGroups>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    async fn delete_replication_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let replication_group_id = required_param(request, "ReplicationGroupId")?;

        let group = {
            let mut state = self.state.write();
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

        let region = self.state.read().region.clone();
        let mut deleted_group = group;
        deleted_group.status = "deleting".to_string();
        let xml = replication_group_xml(&deleted_group, &region);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DeleteReplicationGroup",
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    async fn create_serverless_cache(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let serverless_cache_name = required_param(request, "ServerlessCacheName")?;
        let engine = required_param(request, "Engine")?;
        validate_serverless_engine(&engine)?;

        let description = optional_param(request, "Description").unwrap_or_default();
        let major_engine_version = optional_param(request, "MajorEngineVersion")
            .unwrap_or_else(|| default_major_engine_version(&engine).to_string());
        let full_engine_version = default_full_engine_version(&engine, &major_engine_version)?;
        let cache_usage_limits = parse_cache_usage_limits(request)?;
        let security_group_ids =
            parse_query_list_param(request, "SecurityGroupIds", "SecurityGroupId");
        let subnet_ids = parse_member_list(&request.query_params, "SubnetIds", "SubnetId");
        let kms_key_id = optional_param(request, "KmsKeyId");
        let user_group_id = optional_param(request, "UserGroupId");
        let snapshot_retention_limit =
            optional_non_negative_i32_param(request, "SnapshotRetentionLimit")?;
        let daily_snapshot_time = optional_param(request, "DailySnapshotTime");
        let tags = parse_tags(request)?;

        let (arn, endpoint_address) = {
            let mut state = self.state.write();
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

        let runtime = self.runtime.as_ref().ok_or_else(|| {
            self.state
                .write()
                .cancel_serverless_cache_creation(&serverless_cache_name);
            AwsServiceError::aws_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "InvalidParameterValue",
                "Docker/Podman is required for ElastiCache serverless caches but is not available"
                    .to_string(),
            )
        })?;

        let running = match runtime.ensure_redis(&serverless_cache_name).await {
            Ok(r) => r,
            Err(e) => {
                self.state
                    .write()
                    .cancel_serverless_cache_creation(&serverless_cache_name);
                return Err(runtime_error_to_service_error(e));
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
            let mut state = self.state.write();
            state.finish_serverless_cache_creation(cache.clone());
            if !tags.is_empty() {
                merge_tags(state.tags.entry(arn).or_default(), &tags);
            }
        }

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "CreateServerlessCache",
                &format!("<ServerlessCache>{xml}</ServerlessCache>"),
                &request.request_id,
            ),
        ))
    }

    fn describe_serverless_caches(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let serverless_cache_name = optional_param(request, "ServerlessCacheName");
        let max_results = optional_usize_param(request, "MaxResults")?;
        let next_token = optional_param(request, "NextToken");

        let state = self.state.read();
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
            xml_wrap(
                "DescribeServerlessCaches",
                &format!("<ServerlessCaches>{members_xml}</ServerlessCaches>{next_token_xml}"),
                &request.request_id,
            ),
        ))
    }

    async fn delete_serverless_cache(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let serverless_cache_name = required_param(request, "ServerlessCacheName")?;

        let cache = {
            let mut state = self.state.write();
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
            xml_wrap(
                "DeleteServerlessCache",
                &format!("<ServerlessCache>{xml}</ServerlessCache>"),
                &request.request_id,
            ),
        ))
    }

    fn modify_serverless_cache(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let serverless_cache_name = required_param(request, "ServerlessCacheName")?;
        let description = optional_param(request, "Description");
        let cache_usage_limits = parse_cache_usage_limits(request)?;
        let security_group_ids =
            parse_query_list_param(request, "SecurityGroupIds", "SecurityGroupId");
        let user_group_id = optional_param(request, "UserGroupId");
        let snapshot_retention_limit =
            optional_non_negative_i32_param(request, "SnapshotRetentionLimit")?;
        let daily_snapshot_time = optional_param(request, "DailySnapshotTime");

        let mut state = self.state.write();

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
            xml_wrap(
                "ModifyServerlessCache",
                &format!("<ServerlessCache>{xml}</ServerlessCache>"),
                &request.request_id,
            ),
        ))
    }

    fn create_serverless_cache_snapshot(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let serverless_cache_name = required_param(request, "ServerlessCacheName")?;
        let serverless_cache_snapshot_name =
            required_param(request, "ServerlessCacheSnapshotName")?;
        let kms_key_id = optional_param(request, "KmsKeyId");
        let tags = parse_tags(request)?;

        let mut state = self.state.write();
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
            xml_wrap(
                "CreateServerlessCacheSnapshot",
                &format!("<ServerlessCacheSnapshot>{xml}</ServerlessCacheSnapshot>"),
                &request.request_id,
            ),
        ))
    }

    fn describe_serverless_cache_snapshots(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let serverless_cache_name = optional_param(request, "ServerlessCacheName");
        let serverless_cache_snapshot_name = optional_param(request, "ServerlessCacheSnapshotName");
        let snapshot_type = optional_param(request, "SnapshotType");
        let max_results = optional_usize_param(request, "MaxResults")?;
        let next_token = optional_param(request, "NextToken");

        let state = self.state.read();
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
            xml_wrap(
                "DescribeServerlessCacheSnapshots",
                &format!(
                    "<ServerlessCacheSnapshots>{members_xml}</ServerlessCacheSnapshots>{next_token_xml}"
                ),
                &request.request_id,
            ),
        ))
    }

    fn delete_serverless_cache_snapshot(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let serverless_cache_snapshot_name =
            required_param(request, "ServerlessCacheSnapshotName")?;

        let mut state = self.state.write();
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
            xml_wrap(
                "DeleteServerlessCacheSnapshot",
                &format!("<ServerlessCacheSnapshot>{xml}</ServerlessCacheSnapshot>"),
                &request.request_id,
            ),
        ))
    }

    fn create_snapshot(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let snapshot_name = required_param(request, "SnapshotName")?;
        let replication_group_id = optional_param(request, "ReplicationGroupId");
        let cache_cluster_id = optional_param(request, "CacheClusterId");

        if replication_group_id.is_none() && cache_cluster_id.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterCombination",
                "At least one of ReplicationGroupId or CacheClusterId must be specified."
                    .to_string(),
            ));
        }

        let mut state = self.state.write();

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
        };

        let xml = snapshot_xml(&snapshot);
        state.tags.insert(arn, Vec::new());
        state.snapshots.insert(snapshot_name, snapshot);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "CreateSnapshot",
                &format!("<Snapshot>{xml}</Snapshot>"),
                &request.request_id,
            ),
        ))
    }

    fn describe_snapshots(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let snapshot_name = optional_param(request, "SnapshotName");
        let replication_group_id = optional_param(request, "ReplicationGroupId");
        let cache_cluster_id = optional_param(request, "CacheClusterId");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_param(request, "Marker");

        let state = self.state.read();

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
            xml_wrap(
                "DescribeSnapshots",
                &format!("<Snapshots>{members_xml}</Snapshots>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    fn delete_snapshot(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let snapshot_name = required_param(request, "SnapshotName")?;

        let mut state = self.state.write();
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
            xml_wrap(
                "DeleteSnapshot",
                &format!("<Snapshot>{xml}</Snapshot>"),
                &request.request_id,
            ),
        ))
    }

    fn modify_replication_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let replication_group_id = required_param(request, "ReplicationGroupId")?;

        let new_description = optional_param(request, "ReplicationGroupDescription");
        let new_cache_node_type = optional_param(request, "CacheNodeType");
        let new_engine_version = optional_param(request, "EngineVersion");
        let new_automatic_failover =
            parse_optional_bool(optional_param(request, "AutomaticFailoverEnabled").as_deref())?;
        let new_snapshot_retention_limit = optional_param(request, "SnapshotRetentionLimit")
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
        let new_snapshot_window = optional_param(request, "SnapshotWindow");
        let user_group_ids_to_add =
            parse_member_list(&request.query_params, "UserGroupIdsToAdd", "member");
        let user_group_ids_to_remove =
            parse_member_list(&request.query_params, "UserGroupIdsToRemove", "member");

        let mut state = self.state.write();

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

        // Associate/disassociate user groups
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
        let xml = replication_group_xml(&group, &region);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "ModifyReplicationGroup",
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    fn increase_replica_count(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let replication_group_id = required_param(request, "ReplicationGroupId")?;
        let apply_str = required_param(request, "ApplyImmediately")?;
        let _apply_immediately = parse_optional_bool(Some(&apply_str))?.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "Invalid boolean value for ApplyImmediately: '{}'",
                    apply_str
                ),
            )
        })?;

        let new_replica_count = optional_param(request, "NewReplicaCount")
            .map(|v| {
                v.parse::<i32>().map_err(|_| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!("Invalid value for NewReplicaCount: '{v}'"),
                    )
                })
            })
            .transpose()?
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "MissingParameter",
                    "The request must contain the parameter NewReplicaCount.".to_string(),
                )
            })?;

        if new_replica_count < 1 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("NewReplicaCount must be a positive integer, got {new_replica_count}"),
            ));
        }

        let mut state = self.state.write();

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

        // new_replica_count is number of replicas (excluding primary), so total clusters = replicas + 1
        let new_total = new_replica_count.checked_add(1).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("NewReplicaCount value {new_replica_count} is too large"),
            )
        })?;
        if new_total <= group.num_cache_clusters {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "NewReplicaCount ({new_replica_count}) must result in more clusters than current count ({}).",
                    group.num_cache_clusters
                ),
            ));
        }

        group.num_cache_clusters = new_total;
        group.member_clusters = (1..=new_total)
            .map(|i| format!("{replication_group_id}-{i:03}"))
            .collect();

        let group = group.clone();
        let region = state.region.clone();
        let xml = replication_group_xml(&group, &region);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "IncreaseReplicaCount",
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    fn decrease_replica_count(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let replication_group_id = required_param(request, "ReplicationGroupId")?;
        let apply_str = required_param(request, "ApplyImmediately")?;
        let _apply_immediately = parse_optional_bool(Some(&apply_str))?.ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "Invalid boolean value for ApplyImmediately: '{}'",
                    apply_str
                ),
            )
        })?;

        let new_replica_count = optional_param(request, "NewReplicaCount")
            .map(|v| {
                v.parse::<i32>().map_err(|_| {
                    AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!("Invalid value for NewReplicaCount: '{v}'"),
                    )
                })
            })
            .transpose()?
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "MissingParameter",
                    "The request must contain the parameter NewReplicaCount.".to_string(),
                )
            })?;

        if new_replica_count < 0 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("NewReplicaCount must be non-negative, got {new_replica_count}"),
            ));
        }

        let mut state = self.state.write();

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

        // new_replica_count is number of replicas (excluding primary), so total clusters = replicas + 1
        let new_total = new_replica_count.checked_add(1).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("NewReplicaCount value {new_replica_count} is too large"),
            )
        })?;
        if new_total >= group.num_cache_clusters {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "NewReplicaCount ({new_replica_count}) must result in fewer clusters than current count ({}).",
                    group.num_cache_clusters
                ),
            ));
        }

        group.num_cache_clusters = new_total;
        group.member_clusters = (1..=new_total)
            .map(|i| format!("{replication_group_id}-{i:03}"))
            .collect();

        let group = group.clone();
        let region = state.region.clone();
        let xml = replication_group_xml(&group, &region);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DecreaseReplicaCount",
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    fn test_failover(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let replication_group_id = required_param(request, "ReplicationGroupId")?;
        let node_group_id = required_param(request, "NodeGroupId")?;

        let state = self.state.read();

        let group = state
            .replication_groups
            .get(&replication_group_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "ReplicationGroupNotFoundFault",
                    format!("ReplicationGroup {replication_group_id} not found."),
                )
            })?;

        // Our replication groups always have a single node group with ID "0001"
        if node_group_id != "0001" {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NodeGroupNotFoundFault",
                format!("NodeGroup {node_group_id} not found in ReplicationGroup {replication_group_id}."),
            ));
        }

        let region = state.region.clone();
        let xml = replication_group_xml(group, &region);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "TestFailover",
                &format!("<ReplicationGroup>{xml}</ReplicationGroup>"),
                &request.request_id,
            ),
        ))
    }

    fn create_user(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_id = required_param(request, "UserId")?;
        let user_name = required_param(request, "UserName")?;
        let engine = required_param(request, "Engine")?;
        let access_string = required_param(request, "AccessString")?;

        if engine != "redis" && engine != "valkey" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("Invalid value for Engine: {engine}. Supported engines: redis, valkey"),
            ));
        }

        let no_password_required =
            parse_optional_bool(optional_param(request, "NoPasswordRequired").as_deref())?
                .unwrap_or(false);
        let passwords = parse_member_list(&request.query_params, "Passwords", "member");
        let auth_mode_type = optional_param(request, "AuthenticationMode.Type");

        let (authentication_type, password_count) = if no_password_required {
            if !passwords.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterCombination",
                    "Passwords cannot be provided when NoPasswordRequired is true.".to_string(),
                ));
            }
            ("no-password".to_string(), 0)
        } else if let Some(ref mode) = auth_mode_type {
            let mode_passwords = parse_member_list(
                &request.query_params,
                "AuthenticationMode.Passwords",
                "member",
            );
            match mode.as_str() {
                "password" => {
                    if mode_passwords.is_empty() {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidParameterValue",
                            "At least one password is required when AuthenticationMode.Type is password.".to_string(),
                        ));
                    }
                    ("password".to_string(), mode_passwords.len() as i32)
                }
                "no-password-required" | "iam" => {
                    if !mode_passwords.is_empty() {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidParameterValue",
                            format!("Passwords cannot be provided when AuthenticationMode.Type is {mode}."),
                        ));
                    }
                    (mode.clone(), 0)
                }
                _ => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!("Invalid value for AuthenticationMode.Type: {mode}. Supported values: password, iam, no-password-required"),
                    ));
                }
            }
        } else if !passwords.is_empty() {
            ("password".to_string(), passwords.len() as i32)
        } else {
            ("no-password".to_string(), 0)
        };

        let minimum_engine_version = if engine == "valkey" {
            "8.0".to_string()
        } else {
            "6.0".to_string()
        };

        let mut state = self.state.write();

        if state.users.contains_key(&user_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UserAlreadyExistsFault",
                format!("User {user_id} already exists."),
            ));
        }

        let arn = format!(
            "arn:aws:elasticache:{}:{}:user:{}",
            state.region, state.account_id, user_id
        );

        let user = ElastiCacheUser {
            user_id: user_id.clone(),
            user_name,
            engine,
            access_string,
            status: "active".to_string(),
            authentication_type,
            password_count,
            arn,
            minimum_engine_version,
            user_group_ids: Vec::new(),
        };

        let xml = user_xml(&user);
        state.register_arn(&user.arn);
        state.users.insert(user_id, user);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap("CreateUser", &xml, &request.request_id),
        ))
    }

    fn describe_users(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_id = optional_param(request, "UserId");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_param(request, "Marker");

        let state = self.state.read();

        let users: Vec<&ElastiCacheUser> = if let Some(ref id) = user_id {
            match state.users.get(id) {
                Some(u) => vec![u],
                None => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "UserNotFoundFault",
                        format!("User {id} not found."),
                    ));
                }
            }
        } else {
            let mut users: Vec<&ElastiCacheUser> = state.users.values().collect();
            users.sort_by(|a, b| a.user_id.cmp(&b.user_id));
            users
        };

        let (page, next_marker) = paginate(&users, marker.as_deref(), max_records);

        let members_xml: String = page
            .iter()
            .map(|u| format!("<member>{}</member>", user_xml(u)))
            .collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DescribeUsers",
                &format!("<Users>{members_xml}</Users>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    fn delete_user(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_id = required_param(request, "UserId")?;

        if user_id == "default" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                "Cannot delete the default user.".to_string(),
            ));
        }

        let mut state = self.state.write();

        let user = state.users.remove(&user_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UserNotFoundFault",
                format!("User {user_id} not found."),
            )
        })?;

        state.tags.remove(&user.arn);

        // Remove user from any user groups
        for group in state.user_groups.values_mut() {
            group.user_ids.retain(|id| id != &user_id);
        }

        let mut deleted_user = user;
        deleted_user.status = "deleting".to_string();
        let xml = user_xml(&deleted_user);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap("DeleteUser", &xml, &request.request_id),
        ))
    }

    fn create_user_group(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_group_id = required_param(request, "UserGroupId")?;
        let engine = required_param(request, "Engine")?;

        if engine != "redis" && engine != "valkey" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("Invalid value for Engine: {engine}. Supported engines: redis, valkey"),
            ));
        }

        let user_ids = parse_member_list(&request.query_params, "UserIds", "member");

        let minimum_engine_version = if engine == "valkey" {
            "8.0".to_string()
        } else {
            "6.0".to_string()
        };

        let mut state = self.state.write();

        if state.user_groups.contains_key(&user_group_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UserGroupAlreadyExistsFault",
                format!("User Group {user_group_id} already exists."),
            ));
        }

        // Validate all referenced users exist and have a matching engine
        for uid in &user_ids {
            match state.users.get(uid) {
                None => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "UserNotFoundFault",
                        format!("User {uid} not found."),
                    ));
                }
                Some(user) if user.engine != engine => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!(
                            "User {uid} has engine {} which does not match the user group engine {engine}.",
                            user.engine
                        ),
                    ));
                }
                _ => {}
            }
        }

        let arn = format!(
            "arn:aws:elasticache:{}:{}:usergroup:{}",
            state.region, state.account_id, user_group_id
        );

        let group = ElastiCacheUserGroup {
            user_group_id: user_group_id.clone(),
            engine,
            status: "active".to_string(),
            user_ids: user_ids.clone(),
            arn,
            minimum_engine_version,
            pending_changes: None,
            replication_groups: Vec::new(),
        };

        // Update user_group_ids on referenced users
        for uid in &user_ids {
            if let Some(user) = state.users.get_mut(uid) {
                user.user_group_ids.push(user_group_id.clone());
            }
        }

        let xml = user_group_xml(&group);
        state.register_arn(&group.arn);
        state.user_groups.insert(user_group_id, group);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap("CreateUserGroup", &xml, &request.request_id),
        ))
    }

    fn describe_user_groups(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_group_id = optional_param(request, "UserGroupId");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_param(request, "Marker");

        let state = self.state.read();

        let groups: Vec<&ElastiCacheUserGroup> = if let Some(ref id) = user_group_id {
            match state.user_groups.get(id) {
                Some(g) => vec![g],
                None => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "UserGroupNotFoundFault",
                        format!("User Group {id} not found."),
                    ));
                }
            }
        } else {
            let mut groups: Vec<&ElastiCacheUserGroup> = state.user_groups.values().collect();
            groups.sort_by(|a, b| a.user_group_id.cmp(&b.user_group_id));
            groups
        };

        let (page, next_marker) = paginate(&groups, marker.as_deref(), max_records);

        let members_xml: String = page
            .iter()
            .map(|g| format!("<member>{}</member>", user_group_xml(g)))
            .collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "DescribeUserGroups",
                &format!("<UserGroups>{members_xml}</UserGroups>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    fn delete_user_group(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_group_id = required_param(request, "UserGroupId")?;

        let mut state = self.state.write();

        let group = state.user_groups.remove(&user_group_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UserGroupNotFoundFault",
                format!("User Group {user_group_id} not found."),
            )
        })?;

        state.tags.remove(&group.arn);

        // Remove this group from users' user_group_ids
        for uid in &group.user_ids {
            if let Some(user) = state.users.get_mut(uid) {
                user.user_group_ids.retain(|gid| gid != &user_group_id);
            }
        }

        let mut deleted_group = group;
        deleted_group.status = "deleting".to_string();
        let xml = user_group_xml(&deleted_group);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap("DeleteUserGroup", &xml, &request.request_id),
        ))
    }

    fn add_tags_to_resource(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let resource_name = required_param(request, "ResourceName")?;
        let tags = parse_tags(request)?;

        let mut state = self.state.write();
        let tag_list = state.tags.get_mut(&resource_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "CacheClusterNotFound",
                format!("The resource {resource_name} could not be found."),
            )
        })?;

        merge_tags(tag_list, &tags);

        let tag_xml: String = tag_list.iter().map(tag_xml).collect();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "AddTagsToResource",
                &format!("<TagList>{tag_xml}</TagList>"),
                &request.request_id,
            ),
        ))
    }

    fn list_tags_for_resource(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let resource_name = required_param(request, "ResourceName")?;

        let state = self.state.read();
        let tag_list = state.tags.get(&resource_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "CacheClusterNotFound",
                format!("The resource {resource_name} could not be found."),
            )
        })?;

        let tag_xml: String = tag_list.iter().map(tag_xml).collect();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap(
                "ListTagsForResource",
                &format!("<TagList>{tag_xml}</TagList>"),
                &request.request_id,
            ),
        ))
    }

    fn remove_tags_from_resource(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let resource_name = required_param(request, "ResourceName")?;
        let tag_keys = parse_tag_keys(request)?;

        let mut state = self.state.write();
        let tag_list = state.tags.get_mut(&resource_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "CacheClusterNotFound",
                format!("The resource {resource_name} could not be found."),
            )
        })?;

        tag_list.retain(|(key, _)| !tag_keys.contains(key));

        Ok(AwsResponse::xml(
            StatusCode::OK,
            xml_wrap("RemoveTagsFromResource", "", &request.request_id),
        ))
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn optional_param(req: &AwsRequest, name: &str) -> Option<String> {
    req.query_params
        .get(name)
        .cloned()
        .filter(|value| !value.is_empty())
}

fn required_param(req: &AwsRequest, name: &str) -> Result<String, AwsServiceError> {
    optional_param(req, name).ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "MissingParameter",
            format!("The request must contain the parameter {name}."),
        )
    })
}

fn validate_serverless_engine(engine: &str) -> Result<(), AwsServiceError> {
    if engine == "redis" || engine == "valkey" {
        Ok(())
    } else {
        Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            format!("Invalid value for Engine: {engine}. Supported engines: redis, valkey"),
        ))
    }
}

fn default_major_engine_version(engine: &str) -> &'static str {
    if engine == "valkey" {
        "8.0"
    } else {
        "7.1"
    }
}

fn default_full_engine_version(
    engine: &str,
    major_engine_version: &str,
) -> Result<String, AwsServiceError> {
    if major_engine_version.is_empty() {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            "MajorEngineVersion must not be empty.".to_string(),
        ));
    }

    if (engine == "redis" && !major_engine_version.starts_with('7'))
        || (engine == "valkey" && !major_engine_version.starts_with('8'))
    {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            format!(
                "MajorEngineVersion {major_engine_version} is not supported for engine {engine}."
            ),
        ));
    }

    Ok(major_engine_version.to_string())
}

fn parse_optional_bool(value: Option<&str>) -> Result<Option<bool>, AwsServiceError> {
    value
        .map(|raw| match raw {
            "true" | "True" | "TRUE" => Ok(true),
            "false" | "False" | "FALSE" => Ok(false),
            _ => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("Boolean parameter value '{raw}' is invalid."),
            )),
        })
        .transpose()
}

fn optional_non_negative_i32_param(
    req: &AwsRequest,
    name: &str,
) -> Result<Option<i32>, AwsServiceError> {
    optional_param(req, name)
        .map(|v| {
            let parsed = v.parse::<i32>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("Invalid value for {name}: '{v}'"),
                )
            })?;
            if parsed < 0 {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("{name} must be non-negative, got {parsed}"),
                ));
            }
            Ok(parsed)
        })
        .transpose()
}

fn parse_cache_usage_limits(
    req: &AwsRequest,
) -> Result<Option<ServerlessCacheUsageLimits>, AwsServiceError> {
    let data_storage_maximum =
        optional_non_negative_i32_param(req, "CacheUsageLimits.DataStorage.Maximum")?;
    let data_storage_minimum =
        optional_non_negative_i32_param(req, "CacheUsageLimits.DataStorage.Minimum")?;
    let data_storage_unit = optional_param(req, "CacheUsageLimits.DataStorage.Unit");
    let ecpu_maximum =
        optional_non_negative_i32_param(req, "CacheUsageLimits.ECPUPerSecond.Maximum")?;
    let ecpu_minimum =
        optional_non_negative_i32_param(req, "CacheUsageLimits.ECPUPerSecond.Minimum")?;

    if let (Some(minimum), Some(maximum)) = (data_storage_minimum, data_storage_maximum) {
        if minimum > maximum {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "CacheUsageLimits.DataStorage.Minimum ({minimum}) must be less than or equal to Maximum ({maximum})."
                ),
            ));
        }
    }
    if let (Some(minimum), Some(maximum)) = (ecpu_minimum, ecpu_maximum) {
        if minimum > maximum {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "CacheUsageLimits.ECPUPerSecond.Minimum ({minimum}) must be less than or equal to Maximum ({maximum})."
                ),
            ));
        }
    }

    let data_storage = if data_storage_maximum.is_some()
        || data_storage_minimum.is_some()
        || data_storage_unit.is_some()
    {
        Some(ServerlessCacheDataStorage {
            maximum: data_storage_maximum,
            minimum: data_storage_minimum,
            unit: data_storage_unit,
        })
    } else {
        None
    };
    let ecpu_per_second = if ecpu_maximum.is_some() || ecpu_minimum.is_some() {
        Some(ServerlessCacheEcpuPerSecond {
            maximum: ecpu_maximum,
            minimum: ecpu_minimum,
        })
    } else {
        None
    };

    if data_storage.is_none() && ecpu_per_second.is_none() {
        Ok(None)
    } else {
        Ok(Some(ServerlessCacheUsageLimits {
            data_storage,
            ecpu_per_second,
        }))
    }
}

/// Parse an AWS Query protocol member list.
///
/// AWS encodes lists in the query string as:
///   `{param}.{member_name}.1=val1&{param}.{member_name}.2=val2`
///
/// Returns the values sorted by index.
fn parse_member_list(
    params: &std::collections::HashMap<String, String>,
    param: &str,
    member_name: &str,
) -> Vec<String> {
    let prefix = format!("{param}.{member_name}.");
    let mut indexed: Vec<(usize, String)> = params
        .iter()
        .filter_map(|(k, v)| {
            k.strip_prefix(&prefix)
                .and_then(|idx| idx.parse::<usize>().ok())
                .map(|idx| (idx, v.clone()))
        })
        .collect();
    indexed.sort_by_key(|(idx, _)| *idx);
    indexed.into_iter().map(|(_, v)| v).collect()
}

fn parse_query_list_param(req: &AwsRequest, param: &str, member_name: &str) -> Vec<String> {
    let mut indexed = parse_member_list(&req.query_params, param, member_name);
    if indexed.is_empty() {
        indexed = parse_member_list(&req.query_params, param, "member");
    }
    if indexed.is_empty() {
        indexed = req.query_params.get(param).into_iter().cloned().collect();
    }
    indexed
}

fn optional_usize_param(req: &AwsRequest, name: &str) -> Result<Option<usize>, AwsServiceError> {
    optional_param(req, name)
        .map(|v| {
            v.parse::<usize>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("Value '{v}' for parameter {name} is not a valid integer."),
                )
            })
        })
        .transpose()
}

/// Simple index-based pagination. Returns the current page and an optional next marker.
fn paginate<T: Clone>(
    items: &[T],
    marker: Option<&str>,
    max_records: Option<usize>,
) -> (Vec<T>, Option<String>) {
    let start = marker.and_then(|m| m.parse::<usize>().ok()).unwrap_or(0);
    let limit = max_records.unwrap_or(100).min(100);

    if start >= items.len() {
        return (Vec::new(), None);
    }

    let end = start.saturating_add(limit).min(items.len());
    let page = items[start..end].to_vec();
    let next_marker = if end < items.len() {
        Some(end.to_string())
    } else {
        None
    };
    (page, next_marker)
}

// ---------------------------------------------------------------------------
// Tag helpers
// ---------------------------------------------------------------------------

fn parse_tags(req: &AwsRequest) -> Result<Vec<(String, String)>, AwsServiceError> {
    let mut tags = Vec::new();
    for index in 1.. {
        let key_name = format!("Tags.Tag.{index}.Key");
        let value_name = format!("Tags.Tag.{index}.Value");
        let key = optional_param(req, &key_name);
        let value = optional_param(req, &value_name);
        match (key, value) {
            (Some(k), Some(v)) => tags.push((k, v)),
            (None, None) => break,
            _ => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    "Each tag must include both Key and Value.",
                ));
            }
        }
    }
    Ok(tags)
}

fn parse_tag_keys(req: &AwsRequest) -> Result<Vec<String>, AwsServiceError> {
    let mut keys = Vec::new();
    for index in 1.. {
        let key_name = format!("TagKeys.member.{index}");
        match optional_param(req, &key_name) {
            Some(key) => keys.push(key),
            None => break,
        }
    }
    Ok(keys)
}

fn merge_tags(existing: &mut Vec<(String, String)>, incoming: &[(String, String)]) {
    for (key, value) in incoming {
        if let Some(existing_tag) = existing.iter_mut().find(|(k, _)| k == key) {
            existing_tag.1 = value.clone();
        } else {
            existing.push((key.clone(), value.clone()));
        }
    }
}

fn tag_xml(tag: &(String, String)) -> String {
    format!(
        "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
        xml_escape(&tag.0),
        xml_escape(&tag.1),
    )
}

// ---------------------------------------------------------------------------
// Filtering
// ---------------------------------------------------------------------------

fn filter_engine_versions(
    versions: &[CacheEngineVersion],
    engine: &Option<String>,
    engine_version: &Option<String>,
    family: &Option<String>,
) -> Vec<CacheEngineVersion> {
    versions
        .iter()
        .filter(|v| engine.as_ref().is_none_or(|expected| v.engine == *expected))
        .filter(|v| {
            engine_version
                .as_ref()
                .is_none_or(|expected| v.engine_version == *expected)
        })
        .filter(|v| {
            family
                .as_ref()
                .is_none_or(|expected| v.cache_parameter_group_family == *expected)
        })
        .cloned()
        .collect()
}

// ---------------------------------------------------------------------------
// XML formatting
// ---------------------------------------------------------------------------

fn xml_wrap(action: &str, inner: &str, request_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
         <{action}Response xmlns=\"{ELASTICACHE_NS}\">\
         <{action}Result>{inner}</{action}Result>\
         <ResponseMetadata><RequestId>{request_id}</RequestId></ResponseMetadata>\
         </{action}Response>"
    )
}

fn engine_version_xml(v: &CacheEngineVersion) -> String {
    format!(
        "<CacheEngineVersion>\
         <Engine>{}</Engine>\
         <EngineVersion>{}</EngineVersion>\
         <CacheParameterGroupFamily>{}</CacheParameterGroupFamily>\
         <CacheEngineDescription>{}</CacheEngineDescription>\
         <CacheEngineVersionDescription>{}</CacheEngineVersionDescription>\
         </CacheEngineVersion>",
        xml_escape(&v.engine),
        xml_escape(&v.engine_version),
        xml_escape(&v.cache_parameter_group_family),
        xml_escape(&v.cache_engine_description),
        xml_escape(&v.cache_engine_version_description),
    )
}

fn cache_parameter_group_xml(g: &CacheParameterGroup) -> String {
    format!(
        "<CacheParameterGroup>\
         <CacheParameterGroupName>{}</CacheParameterGroupName>\
         <CacheParameterGroupFamily>{}</CacheParameterGroupFamily>\
         <Description>{}</Description>\
         <IsGlobal>{}</IsGlobal>\
         <ARN>{}</ARN>\
         </CacheParameterGroup>",
        xml_escape(&g.cache_parameter_group_name),
        xml_escape(&g.cache_parameter_group_family),
        xml_escape(&g.description),
        g.is_global,
        xml_escape(&g.arn),
    )
}

fn cache_subnet_group_xml(g: &CacheSubnetGroup, region: &str) -> String {
    let subnets_xml: String = g
        .subnet_ids
        .iter()
        .enumerate()
        .map(|(i, id)| {
            let az = format!("{}{}", region, (b'a' + (i % 6) as u8) as char);
            format!(
                "<Subnet>\
                 <SubnetIdentifier>{}</SubnetIdentifier>\
                 <SubnetAvailabilityZone><Name>{}</Name></SubnetAvailabilityZone>\
                 </Subnet>",
                xml_escape(id),
                xml_escape(&az),
            )
        })
        .collect();
    format!(
        "<CacheSubnetGroupName>{}</CacheSubnetGroupName>\
         <CacheSubnetGroupDescription>{}</CacheSubnetGroupDescription>\
         <VpcId>{}</VpcId>\
         <Subnets>{subnets_xml}</Subnets>\
         <ARN>{}</ARN>",
        xml_escape(&g.cache_subnet_group_name),
        xml_escape(&g.cache_subnet_group_description),
        xml_escape(&g.vpc_id),
        xml_escape(&g.arn),
    )
}

fn cache_cluster_xml(cluster: &CacheCluster, show_cache_node_info: bool) -> String {
    let cache_subnet_group_name_xml = cluster
        .cache_subnet_group_name
        .as_ref()
        .map(|name| {
            format!(
                "<CacheSubnetGroupName>{}</CacheSubnetGroupName>",
                xml_escape(name)
            )
        })
        .unwrap_or_default();
    let replication_group_id_xml = cluster
        .replication_group_id
        .as_ref()
        .map(|group_id| {
            format!(
                "<ReplicationGroupId>{}</ReplicationGroupId>",
                xml_escape(group_id)
            )
        })
        .unwrap_or_default();
    let cache_nodes_xml = if show_cache_node_info {
        match usize::try_from(cluster.num_cache_nodes) {
            Ok(node_count) => {
                let members: String = (0..node_count)
                    .filter_map(|index| {
                        let node_id = index.checked_add(1)?;
                        Some(cache_node_xml(cluster, node_id))
                    })
                    .collect();
                format!("<CacheNodes>{members}</CacheNodes>")
            }
            Err(_) => String::new(),
        }
    } else {
        String::new()
    };

    format!(
        "<CacheClusterId>{}</CacheClusterId>\
         <CacheNodeType>{}</CacheNodeType>\
         <Engine>{}</Engine>\
         <EngineVersion>{}</EngineVersion>\
         <CacheClusterStatus>{}</CacheClusterStatus>\
         <NumCacheNodes>{}</NumCacheNodes>\
         <PreferredAvailabilityZone>{}</PreferredAvailabilityZone>\
         <CacheClusterCreateTime>{}</CacheClusterCreateTime>\
         {cache_subnet_group_name_xml}\
         {cache_nodes_xml}\
         <AutoMinorVersionUpgrade>{}</AutoMinorVersionUpgrade>\
         {replication_group_id_xml}\
         <ARN>{}</ARN>",
        xml_escape(&cluster.cache_cluster_id),
        xml_escape(&cluster.cache_node_type),
        xml_escape(&cluster.engine),
        xml_escape(&cluster.engine_version),
        xml_escape(&cluster.cache_cluster_status),
        cluster.num_cache_nodes,
        xml_escape(&cluster.preferred_availability_zone),
        xml_escape(&cluster.created_at),
        cluster.auto_minor_version_upgrade,
        xml_escape(&cluster.arn),
    )
}

fn cache_node_xml(cluster: &CacheCluster, node_id: usize) -> String {
    format!(
        "<CacheNode>\
         <CacheNodeId>{node_id:04}</CacheNodeId>\
         <CacheNodeStatus>{}</CacheNodeStatus>\
         <CacheNodeCreateTime>{}</CacheNodeCreateTime>\
         <Endpoint><Address>{}</Address><Port>{}</Port></Endpoint>\
         <ParameterGroupStatus>in-sync</ParameterGroupStatus>\
         <CustomerAvailabilityZone>{}</CustomerAvailabilityZone>\
         </CacheNode>",
        xml_escape(&cluster.cache_cluster_status),
        xml_escape(&cluster.created_at),
        xml_escape(&cluster.endpoint_address),
        cluster.endpoint_port,
        xml_escape(&cluster.preferred_availability_zone),
    )
}

fn replication_group_xml(g: &ReplicationGroup, region: &str) -> String {
    let member_clusters_xml: String = g
        .member_clusters
        .iter()
        .map(|c| format!("<ClusterId>{}</ClusterId>", xml_escape(c)))
        .collect();

    let primary_az = format!("{region}a");

    format!(
        "<ReplicationGroupId>{}</ReplicationGroupId>\
         <Description>{}</Description>\
         <Status>{}</Status>\
         <MemberClusters>{member_clusters_xml}</MemberClusters>\
         <NodeGroups>\
         <NodeGroup>\
         <NodeGroupId>0001</NodeGroupId>\
         <Status>available</Status>\
         <PrimaryEndpoint>\
         <Address>{}</Address>\
         <Port>{}</Port>\
         </PrimaryEndpoint>\
         <ReaderEndpoint>\
         <Address>{}</Address>\
         <Port>{}</Port>\
         </ReaderEndpoint>\
         <NodeGroupMembers>\
         <NodeGroupMember>\
         <CacheClusterId>{}</CacheClusterId>\
         <CacheNodeId>0001</CacheNodeId>\
         <PreferredAvailabilityZone>{}</PreferredAvailabilityZone>\
         <CurrentRole>primary</CurrentRole>\
         </NodeGroupMember>\
         </NodeGroupMembers>\
         </NodeGroup>\
         </NodeGroups>\
         <AutomaticFailover>{}</AutomaticFailover>\
         <SnapshotRetentionLimit>{}</SnapshotRetentionLimit>\
         <SnapshotWindow>{}</SnapshotWindow>\
         <ClusterEnabled>false</ClusterEnabled>\
         <CacheNodeType>{}</CacheNodeType>\
         <TransitEncryptionEnabled>false</TransitEncryptionEnabled>\
         <AtRestEncryptionEnabled>false</AtRestEncryptionEnabled>\
         <ARN>{}</ARN>",
        xml_escape(&g.replication_group_id),
        xml_escape(&g.description),
        xml_escape(&g.status),
        xml_escape(&g.endpoint_address),
        g.endpoint_port,
        xml_escape(&g.endpoint_address),
        g.endpoint_port,
        xml_escape(g.member_clusters.first().map(|s| s.as_str()).unwrap_or("")),
        xml_escape(&primary_az),
        if g.automatic_failover_enabled {
            "enabled"
        } else {
            "disabled"
        },
        g.snapshot_retention_limit,
        xml_escape(&g.snapshot_window),
        xml_escape(&g.cache_node_type),
        xml_escape(&g.arn),
    )
}

fn user_xml(u: &ElastiCacheUser) -> String {
    let user_group_ids_xml: String = u
        .user_group_ids
        .iter()
        .map(|id| format!("<member>{}</member>", xml_escape(id)))
        .collect();
    format!(
        "<UserId>{}</UserId>\
         <UserName>{}</UserName>\
         <Status>{}</Status>\
         <Engine>{}</Engine>\
         <MinimumEngineVersion>{}</MinimumEngineVersion>\
         <AccessString>{}</AccessString>\
         <UserGroupIds>{user_group_ids_xml}</UserGroupIds>\
         <Authentication>\
         <Type>{}</Type>\
         <PasswordCount>{}</PasswordCount>\
         </Authentication>\
         <ARN>{}</ARN>",
        xml_escape(&u.user_id),
        xml_escape(&u.user_name),
        xml_escape(&u.status),
        xml_escape(&u.engine),
        xml_escape(&u.minimum_engine_version),
        xml_escape(&u.access_string),
        xml_escape(&u.authentication_type),
        u.password_count,
        xml_escape(&u.arn),
    )
}

fn user_group_xml(g: &ElastiCacheUserGroup) -> String {
    let user_ids_xml: String = g
        .user_ids
        .iter()
        .map(|id| format!("<member>{}</member>", xml_escape(id)))
        .collect();
    let replication_groups_xml: String = g
        .replication_groups
        .iter()
        .map(|id| format!("<member>{}</member>", xml_escape(id)))
        .collect();
    let pending_xml = if let Some(ref pc) = g.pending_changes {
        let to_add: String = pc
            .user_ids_to_add
            .iter()
            .map(|id| format!("<member>{}</member>", xml_escape(id)))
            .collect();
        let to_remove: String = pc
            .user_ids_to_remove
            .iter()
            .map(|id| format!("<member>{}</member>", xml_escape(id)))
            .collect();
        format!(
            "<PendingChanges>\
             <UserIdsToAdd>{to_add}</UserIdsToAdd>\
             <UserIdsToRemove>{to_remove}</UserIdsToRemove>\
             </PendingChanges>"
        )
    } else {
        String::new()
    };
    format!(
        "<UserGroupId>{}</UserGroupId>\
         <Status>{}</Status>\
         <Engine>{}</Engine>\
         <MinimumEngineVersion>{}</MinimumEngineVersion>\
         <UserIds>{user_ids_xml}</UserIds>\
         <ReplicationGroups>{replication_groups_xml}</ReplicationGroups>\
         {pending_xml}\
         <ARN>{}</ARN>",
        xml_escape(&g.user_group_id),
        xml_escape(&g.status),
        xml_escape(&g.engine),
        xml_escape(&g.minimum_engine_version),
        xml_escape(&g.arn),
    )
}

fn runtime_error_to_service_error(error: RuntimeError) -> AwsServiceError {
    match error {
        RuntimeError::Unavailable => AwsServiceError::aws_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "InvalidParameterValue",
            "Docker/Podman is required for ElastiCache replication groups but is not available"
                .to_string(),
        ),
        RuntimeError::ContainerStartFailed(msg) => AwsServiceError::aws_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InvalidParameterValue",
            format!("Failed to start Redis container: {msg}"),
        ),
    }
}

fn add_cluster_to_replication_group(
    state: &mut ElastiCacheState,
    replication_group_id: &str,
    cache_cluster_id: &str,
) {
    if let Some(group) = state.replication_groups.get_mut(replication_group_id) {
        if !group
            .member_clusters
            .iter()
            .any(|id| id == cache_cluster_id)
        {
            group.member_clusters.push(cache_cluster_id.to_string());
            group.num_cache_clusters = group.member_clusters.len() as i32;
        }
    }
}

fn remove_cluster_from_replication_group(
    state: &mut ElastiCacheState,
    replication_group_id: &str,
    cache_cluster_id: &str,
) {
    if let Some(group) = state.replication_groups.get_mut(replication_group_id) {
        let original_len = group.member_clusters.len();
        group.member_clusters.retain(|id| id != cache_cluster_id);
        if group.member_clusters.len() != original_len {
            group.num_cache_clusters = group.member_clusters.len() as i32;
        }
    }
}

fn snapshot_xml(s: &CacheSnapshot) -> String {
    format!(
        "<SnapshotName>{}</SnapshotName>\
         <ReplicationGroupId>{}</ReplicationGroupId>\
         <ReplicationGroupDescription>{}</ReplicationGroupDescription>\
         <SnapshotStatus>{}</SnapshotStatus>\
         <SnapshotSource>{}</SnapshotSource>\
         <CacheNodeType>{}</CacheNodeType>\
         <Engine>{}</Engine>\
         <EngineVersion>{}</EngineVersion>\
         <NumCacheClusters>{}</NumCacheClusters>\
         <ARN>{}</ARN>",
        xml_escape(&s.snapshot_name),
        xml_escape(&s.replication_group_id),
        xml_escape(&s.replication_group_description),
        xml_escape(&s.snapshot_status),
        xml_escape(&s.snapshot_source),
        xml_escape(&s.cache_node_type),
        xml_escape(&s.engine),
        xml_escape(&s.engine_version),
        s.num_cache_clusters,
        xml_escape(&s.arn),
    )
}

fn serverless_cache_xml(cache: &ServerlessCache) -> String {
    let cache_usage_limits_xml = cache
        .cache_usage_limits
        .as_ref()
        .map(serverless_cache_usage_limits_xml)
        .unwrap_or_default();
    let kms_key_id_xml = cache
        .kms_key_id
        .as_ref()
        .map(|value| format!("<KmsKeyId>{}</KmsKeyId>", xml_escape(value)))
        .unwrap_or_default();
    let security_group_ids_xml = if cache.security_group_ids.is_empty() {
        String::new()
    } else {
        let members: String = cache
            .security_group_ids
            .iter()
            .map(|id| format!("<SecurityGroupId>{}</SecurityGroupId>", xml_escape(id)))
            .collect();
        format!("<SecurityGroupIds>{members}</SecurityGroupIds>")
    };
    let subnet_ids_xml = if cache.subnet_ids.is_empty() {
        String::new()
    } else {
        let members: String = cache
            .subnet_ids
            .iter()
            .map(|id| format!("<member>{}</member>", xml_escape(id)))
            .collect();
        format!("<SubnetIds>{members}</SubnetIds>")
    };
    let user_group_id_xml = cache
        .user_group_id
        .as_ref()
        .map(|value| format!("<UserGroupId>{}</UserGroupId>", xml_escape(value)))
        .unwrap_or_default();
    let snapshot_retention_limit_xml = cache
        .snapshot_retention_limit
        .map(|value| format!("<SnapshotRetentionLimit>{value}</SnapshotRetentionLimit>"))
        .unwrap_or_default();
    let daily_snapshot_time_xml = cache
        .daily_snapshot_time
        .as_ref()
        .map(|value| {
            format!(
                "<DailySnapshotTime>{}</DailySnapshotTime>",
                xml_escape(value)
            )
        })
        .unwrap_or_default();

    format!(
        "<ServerlessCacheName>{}</ServerlessCacheName>\
         <Description>{}</Description>\
         <CreateTime>{}</CreateTime>\
         <Status>{}</Status>\
         <Engine>{}</Engine>\
         <MajorEngineVersion>{}</MajorEngineVersion>\
         <FullEngineVersion>{}</FullEngineVersion>\
         {cache_usage_limits_xml}\
         {kms_key_id_xml}\
         {security_group_ids_xml}\
         <Endpoint>{}</Endpoint>\
         <ReaderEndpoint>{}</ReaderEndpoint>\
         <ARN>{}</ARN>\
         {user_group_id_xml}\
         {subnet_ids_xml}\
         {snapshot_retention_limit_xml}\
         {daily_snapshot_time_xml}",
        xml_escape(&cache.serverless_cache_name),
        xml_escape(&cache.description),
        xml_escape(&cache.created_at),
        xml_escape(&cache.status),
        xml_escape(&cache.engine),
        xml_escape(&cache.major_engine_version),
        xml_escape(&cache.full_engine_version),
        serverless_cache_endpoint_xml(&cache.endpoint),
        serverless_cache_endpoint_xml(&cache.reader_endpoint),
        xml_escape(&cache.arn),
    )
}

fn serverless_cache_usage_limits_xml(limits: &ServerlessCacheUsageLimits) -> String {
    let data_storage_xml = limits
        .data_storage
        .as_ref()
        .map(|data_storage| {
            let maximum_xml = data_storage
                .maximum
                .map(|value| format!("<Maximum>{value}</Maximum>"))
                .unwrap_or_default();
            let minimum_xml = data_storage
                .minimum
                .map(|value| format!("<Minimum>{value}</Minimum>"))
                .unwrap_or_default();
            let unit_xml = data_storage
                .unit
                .as_ref()
                .map(|value| format!("<Unit>{}</Unit>", xml_escape(value)))
                .unwrap_or_default();
            format!("<DataStorage>{maximum_xml}{minimum_xml}{unit_xml}</DataStorage>")
        })
        .unwrap_or_default();
    let ecpu_per_second_xml = limits
        .ecpu_per_second
        .as_ref()
        .map(|ecpu| {
            let maximum_xml = ecpu
                .maximum
                .map(|value| format!("<Maximum>{value}</Maximum>"))
                .unwrap_or_default();
            let minimum_xml = ecpu
                .minimum
                .map(|value| format!("<Minimum>{value}</Minimum>"))
                .unwrap_or_default();
            format!("<ECPUPerSecond>{maximum_xml}{minimum_xml}</ECPUPerSecond>")
        })
        .unwrap_or_default();

    format!("<CacheUsageLimits>{data_storage_xml}{ecpu_per_second_xml}</CacheUsageLimits>")
}

fn serverless_cache_endpoint_xml(endpoint: &ServerlessCacheEndpoint) -> String {
    format!(
        "<Address>{}</Address><Port>{}</Port>",
        xml_escape(&endpoint.address),
        endpoint.port,
    )
}

fn serverless_cache_snapshot_xml(snapshot: &ServerlessCacheSnapshot) -> String {
    let kms_key_id_xml = snapshot
        .kms_key_id
        .as_ref()
        .map(|value| format!("<KmsKeyId>{}</KmsKeyId>", xml_escape(value)))
        .unwrap_or_default();
    let expiry_time_xml = snapshot
        .expiry_time
        .as_ref()
        .map(|value| format!("<ExpiryTime>{}</ExpiryTime>", xml_escape(value)))
        .unwrap_or_default();
    let bytes_used_for_cache_xml = snapshot
        .bytes_used_for_cache
        .as_ref()
        .map(|value| {
            format!(
                "<BytesUsedForCache>{}</BytesUsedForCache>",
                xml_escape(value)
            )
        })
        .unwrap_or_default();

    format!(
        "<ServerlessCacheSnapshotName>{}</ServerlessCacheSnapshotName>\
         <ARN>{}</ARN>\
         {kms_key_id_xml}\
         <SnapshotType>{}</SnapshotType>\
         <Status>{}</Status>\
         <CreateTime>{}</CreateTime>\
         {expiry_time_xml}\
         {bytes_used_for_cache_xml}\
         <ServerlessCacheConfiguration>\
         <ServerlessCacheName>{}</ServerlessCacheName>\
         <Engine>{}</Engine>\
         <MajorEngineVersion>{}</MajorEngineVersion>\
         </ServerlessCacheConfiguration>",
        xml_escape(&snapshot.serverless_cache_snapshot_name),
        xml_escape(&snapshot.arn),
        xml_escape(&snapshot.snapshot_type),
        xml_escape(&snapshot.status),
        xml_escape(&snapshot.create_time),
        xml_escape(&snapshot.serverless_cache_name),
        xml_escape(&snapshot.engine),
        xml_escape(&snapshot.major_engine_version),
    )
}

fn parameter_xml(p: &EngineDefaultParameter) -> String {
    format!(
        "<Parameter>\
         <ParameterName>{}</ParameterName>\
         <ParameterValue>{}</ParameterValue>\
         <Description>{}</Description>\
         <Source>{}</Source>\
         <DataType>{}</DataType>\
         <AllowedValues>{}</AllowedValues>\
         <IsModifiable>{}</IsModifiable>\
         <MinimumEngineVersion>{}</MinimumEngineVersion>\
         </Parameter>",
        xml_escape(&p.parameter_name),
        xml_escape(&p.parameter_value),
        xml_escape(&p.description),
        xml_escape(&p.source),
        xml_escape(&p.data_type),
        xml_escape(&p.allowed_values),
        p.is_modifiable,
        xml_escape(&p.minimum_engine_version),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::default_engine_versions;
    use bytes::Bytes;
    use http::{HeaderMap, Method};
    use std::collections::HashMap;

    fn request(action: &str, params: &[(&str, &str)]) -> AwsRequest {
        let mut query_params = HashMap::from([("Action".to_string(), action.to_string())]);
        for (key, value) in params {
            query_params.insert((*key).to_string(), (*value).to_string());
        }

        AwsRequest {
            service: "elasticache".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-request-id".to_string(),
            headers: HeaderMap::new(),
            query_params,
            body: Bytes::new(),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: true,
            access_key_id: None,
        }
    }

    #[test]
    fn parse_member_list_extracts_indexed_values() {
        let mut params = HashMap::new();
        params.insert(
            "SubnetIds.SubnetIdentifier.1".to_string(),
            "subnet-aaa".to_string(),
        );
        params.insert(
            "SubnetIds.SubnetIdentifier.2".to_string(),
            "subnet-bbb".to_string(),
        );
        params.insert(
            "SubnetIds.SubnetIdentifier.3".to_string(),
            "subnet-ccc".to_string(),
        );
        params.insert("OtherParam".to_string(), "ignored".to_string());

        let result = parse_member_list(&params, "SubnetIds", "SubnetIdentifier");
        assert_eq!(result, vec!["subnet-aaa", "subnet-bbb", "subnet-ccc"]);
    }

    #[test]
    fn parse_member_list_returns_sorted_by_index() {
        let mut params = HashMap::new();
        params.insert(
            "SubnetIds.SubnetIdentifier.3".to_string(),
            "subnet-ccc".to_string(),
        );
        params.insert(
            "SubnetIds.SubnetIdentifier.1".to_string(),
            "subnet-aaa".to_string(),
        );

        let result = parse_member_list(&params, "SubnetIds", "SubnetIdentifier");
        assert_eq!(result, vec!["subnet-aaa", "subnet-ccc"]);
    }

    #[test]
    fn parse_member_list_returns_empty_for_no_matches() {
        let params = HashMap::new();
        let result = parse_member_list(&params, "SubnetIds", "SubnetIdentifier");
        assert!(result.is_empty());
    }

    #[test]
    fn cache_subnet_group_xml_contains_all_fields() {
        let group = CacheSubnetGroup {
            cache_subnet_group_name: "my-group".to_string(),
            cache_subnet_group_description: "My description".to_string(),
            vpc_id: "vpc-123".to_string(),
            subnet_ids: vec!["subnet-aaa".to_string(), "subnet-bbb".to_string()],
            arn: "arn:aws:elasticache:us-east-1:123:subnetgroup:my-group".to_string(),
        };
        let xml = cache_subnet_group_xml(&group, "us-east-1");
        assert!(xml.contains("<CacheSubnetGroupName>my-group</CacheSubnetGroupName>"));
        assert!(xml
            .contains("<CacheSubnetGroupDescription>My description</CacheSubnetGroupDescription>"));
        assert!(xml.contains("<VpcId>vpc-123</VpcId>"));
        assert!(xml.contains("<SubnetIdentifier>subnet-aaa</SubnetIdentifier>"));
        assert!(xml.contains("<SubnetIdentifier>subnet-bbb</SubnetIdentifier>"));
        assert!(xml.contains("<Name>us-east-1a</Name>"));
        assert!(xml.contains("<Name>us-east-1b</Name>"));
        assert!(xml.contains("<ARN>arn:aws:elasticache:us-east-1:123:subnetgroup:my-group</ARN>"));
    }

    #[test]
    fn cache_cluster_xml_contains_expected_fields() {
        let cluster = CacheCluster {
            cache_cluster_id: "classic-1".to_string(),
            cache_node_type: "cache.t3.micro".to_string(),
            engine: "redis".to_string(),
            engine_version: "7.1".to_string(),
            cache_cluster_status: "available".to_string(),
            num_cache_nodes: 2,
            preferred_availability_zone: "us-east-1a".to_string(),
            cache_subnet_group_name: Some("default".to_string()),
            auto_minor_version_upgrade: true,
            arn: "arn:aws:elasticache:us-east-1:123:cluster:classic-1".to_string(),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            endpoint_address: "127.0.0.1".to_string(),
            endpoint_port: 6379,
            container_id: "abc123".to_string(),
            host_port: 6379,
            replication_group_id: Some("rg-1".to_string()),
        };

        let xml = cache_cluster_xml(&cluster, true);
        assert!(xml.contains("<CacheClusterId>classic-1</CacheClusterId>"));
        assert!(xml.contains("<CacheNodeType>cache.t3.micro</CacheNodeType>"));
        assert!(xml.contains("<Engine>redis</Engine>"));
        assert!(xml.contains("<NumCacheNodes>2</NumCacheNodes>"));
        assert!(xml.contains("<PreferredAvailabilityZone>us-east-1a</PreferredAvailabilityZone>"));
        assert!(xml.contains("<CacheSubnetGroupName>default</CacheSubnetGroupName>"));
        assert!(xml.contains("<CacheNodes>"));
        assert!(xml.contains("<CacheNodeId>0001</CacheNodeId>"));
        assert!(xml.contains("<ReplicationGroupId>rg-1</ReplicationGroupId>"));
        assert!(xml.contains("<ARN>arn:aws:elasticache:us-east-1:123:cluster:classic-1</ARN>"));
    }

    #[test]
    fn filter_engine_versions_by_engine() {
        let versions = default_engine_versions();
        let filtered = filter_engine_versions(&versions, &Some("redis".to_string()), &None, &None);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].engine, "redis");
    }

    #[test]
    fn filter_engine_versions_by_family() {
        let versions = default_engine_versions();
        let filtered =
            filter_engine_versions(&versions, &None, &None, &Some("valkey8".to_string()));
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].engine, "valkey");
    }

    #[test]
    fn filter_engine_versions_no_match() {
        let versions = default_engine_versions();
        let filtered =
            filter_engine_versions(&versions, &Some("memcached".to_string()), &None, &None);
        assert!(filtered.is_empty());
    }

    #[test]
    fn paginate_returns_all_when_within_limit() {
        let items = vec![1, 2, 3];
        let (page, marker) = paginate(&items, None, None);
        assert_eq!(page, vec![1, 2, 3]);
        assert!(marker.is_none());
    }

    #[test]
    fn paginate_respects_max_records() {
        let items = vec![1, 2, 3, 4, 5];
        let (page, marker) = paginate(&items, None, Some(2));
        assert_eq!(page, vec![1, 2]);
        assert_eq!(marker, Some("2".to_string()));

        let (page2, marker2) = paginate(&items, Some("2"), Some(2));
        assert_eq!(page2, vec![3, 4]);
        assert_eq!(marker2, Some("4".to_string()));

        let (page3, marker3) = paginate(&items, Some("4"), Some(2));
        assert_eq!(page3, vec![5]);
        assert!(marker3.is_none());
    }

    #[test]
    fn xml_wrap_produces_valid_response() {
        let xml = xml_wrap("TestAction", "<Data>ok</Data>", "req-123");
        assert!(xml.contains("<TestActionResponse"));
        assert!(xml.contains("<TestActionResult>"));
        assert!(xml.contains("<RequestId>req-123</RequestId>"));
        assert!(xml.contains(ELASTICACHE_NS));
    }

    #[test]
    fn parse_tags_reads_query_shape() {
        let req = request(
            "AddTagsToResource",
            &[
                ("Tags.Tag.1.Key", "env"),
                ("Tags.Tag.1.Value", "prod"),
                ("Tags.Tag.2.Key", "team"),
                ("Tags.Tag.2.Value", "backend"),
            ],
        );

        let tags = parse_tags(&req).expect("tags");
        assert_eq!(
            tags,
            vec![
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "backend".to_string()),
            ]
        );
    }

    #[test]
    fn parse_tags_returns_empty_for_no_tags() {
        let req = request("AddTagsToResource", &[]);
        let tags = parse_tags(&req).expect("tags");
        assert!(tags.is_empty());
    }

    #[test]
    fn parse_tag_keys_reads_member_shape() {
        let req = request(
            "RemoveTagsFromResource",
            &[("TagKeys.member.1", "env"), ("TagKeys.member.2", "team")],
        );

        let keys = parse_tag_keys(&req).expect("tag keys");
        assert_eq!(keys, vec!["env".to_string(), "team".to_string()]);
    }

    #[test]
    fn merge_tags_adds_new_and_updates_existing() {
        let mut tags = vec![("env".to_string(), "dev".to_string())];

        merge_tags(
            &mut tags,
            &[
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "core".to_string()),
            ],
        );

        assert_eq!(
            tags,
            vec![
                ("env".to_string(), "prod".to_string()),
                ("team".to_string(), "core".to_string()),
            ]
        );
    }

    #[test]
    fn tag_xml_produces_valid_element() {
        let xml = tag_xml(&("env".to_string(), "prod".to_string()));
        assert_eq!(xml, "<Tag><Key>env</Key><Value>prod</Value></Tag>");
    }

    #[test]
    fn user_xml_contains_all_fields() {
        let user = ElastiCacheUser {
            user_id: "myuser".to_string(),
            user_name: "myuser".to_string(),
            engine: "redis".to_string(),
            access_string: "on ~* +@all".to_string(),
            status: "active".to_string(),
            authentication_type: "password".to_string(),
            password_count: 1,
            arn: "arn:aws:elasticache:us-east-1:123:user:myuser".to_string(),
            minimum_engine_version: "6.0".to_string(),
            user_group_ids: vec!["group1".to_string()],
        };
        let xml = user_xml(&user);
        assert!(xml.contains("<UserId>myuser</UserId>"));
        assert!(xml.contains("<UserName>myuser</UserName>"));
        assert!(xml.contains("<Engine>redis</Engine>"));
        assert!(xml.contains("<AccessString>on ~* +@all</AccessString>"));
        assert!(xml.contains("<Status>active</Status>"));
        assert!(xml.contains("<Type>password</Type>"));
        assert!(xml.contains("<PasswordCount>1</PasswordCount>"));
        assert!(xml.contains("<member>group1</member>"));
        assert!(xml.contains("<ARN>arn:aws:elasticache:us-east-1:123:user:myuser</ARN>"));
    }

    #[test]
    fn user_group_xml_contains_all_fields() {
        let group = ElastiCacheUserGroup {
            user_group_id: "mygroup".to_string(),
            engine: "redis".to_string(),
            status: "active".to_string(),
            user_ids: vec!["default".to_string(), "myuser".to_string()],
            arn: "arn:aws:elasticache:us-east-1:123:usergroup:mygroup".to_string(),
            minimum_engine_version: "6.0".to_string(),
            pending_changes: None,
            replication_groups: Vec::new(),
        };
        let xml = user_group_xml(&group);
        assert!(xml.contains("<UserGroupId>mygroup</UserGroupId>"));
        assert!(xml.contains("<Engine>redis</Engine>"));
        assert!(xml.contains("<Status>active</Status>"));
        assert!(xml.contains("<member>default</member>"));
        assert!(xml.contains("<member>myuser</member>"));
        assert!(xml.contains("<ARN>arn:aws:elasticache:us-east-1:123:usergroup:mygroup</ARN>"));
    }

    #[test]
    fn create_user_returns_user_xml() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);

        let req = request(
            "CreateUser",
            &[
                ("UserId", "testuser"),
                ("UserName", "testuser"),
                ("Engine", "redis"),
                ("AccessString", "on ~* +@all"),
            ],
        );
        let resp = service.create_user(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<UserId>testuser</UserId>"));
        assert!(body.contains("<Status>active</Status>"));
        assert!(body.contains("<CreateUserResponse"));
    }

    #[test]
    fn create_user_rejects_duplicate() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);

        let req = request(
            "CreateUser",
            &[
                ("UserId", "default"),
                ("UserName", "default"),
                ("Engine", "redis"),
                ("AccessString", "on ~* +@all"),
            ],
        );
        assert!(service.create_user(&req).is_err());
    }

    #[test]
    fn delete_user_rejects_default() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);

        let req = request("DeleteUser", &[("UserId", "default")]);
        assert!(service.delete_user(&req).is_err());
    }

    #[test]
    fn describe_users_returns_default_user() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);

        let req = request("DescribeUsers", &[]);
        let resp = service.describe_users(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<UserId>default</UserId>"));
    }

    #[test]
    fn create_and_describe_user_group() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);

        let req = request(
            "CreateUserGroup",
            &[
                ("UserGroupId", "mygroup"),
                ("Engine", "redis"),
                ("UserIds.member.1", "default"),
            ],
        );
        let resp = service.create_user_group(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<UserGroupId>mygroup</UserGroupId>"));
        assert!(body.contains("<member>default</member>"));

        let req = request("DescribeUserGroups", &[]);
        let resp = service.describe_user_groups(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<UserGroupId>mygroup</UserGroupId>"));
    }

    #[test]
    fn create_user_group_rejects_unknown_user() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);

        let req = request(
            "CreateUserGroup",
            &[
                ("UserGroupId", "mygroup"),
                ("Engine", "redis"),
                ("UserIds.member.1", "nonexistent"),
            ],
        );
        assert!(service.create_user_group(&req).is_err());
    }

    #[test]
    fn delete_user_group_removes_from_state() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);

        let req = request(
            "CreateUserGroup",
            &[("UserGroupId", "delgroup"), ("Engine", "redis")],
        );
        service.create_user_group(&req).unwrap();

        let req = request("DeleteUserGroup", &[("UserGroupId", "delgroup")]);
        let resp = service.delete_user_group(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<Status>deleting</Status>"));

        let req = request("DescribeUserGroups", &[("UserGroupId", "delgroup")]);
        assert!(service.describe_user_groups(&req).is_err());
    }

    fn service_with_cache_cluster(cluster_id: &str) -> ElastiCacheService {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        {
            let mut s = shared.write();
            let arn = format!("arn:aws:elasticache:us-east-1:123456789012:cluster:{cluster_id}");
            s.tags.insert(arn.clone(), Vec::new());
            s.cache_clusters.insert(
                cluster_id.to_string(),
                CacheCluster {
                    cache_cluster_id: cluster_id.to_string(),
                    cache_node_type: "cache.t3.micro".to_string(),
                    engine: "redis".to_string(),
                    engine_version: "7.1".to_string(),
                    cache_cluster_status: "available".to_string(),
                    num_cache_nodes: 1,
                    preferred_availability_zone: "us-east-1a".to_string(),
                    cache_subnet_group_name: Some("default".to_string()),
                    auto_minor_version_upgrade: true,
                    arn,
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                    endpoint_address: "127.0.0.1".to_string(),
                    endpoint_port: 6379,
                    container_id: "abc123".to_string(),
                    host_port: 6379,
                    replication_group_id: None,
                },
            );
        }
        ElastiCacheService::new(shared)
    }

    #[test]
    fn describe_cache_clusters_returns_all() {
        let service = service_with_cache_cluster("cluster-a");
        {
            let mut state = service.state.write();
            let arn = "arn:aws:elasticache:us-east-1:123456789012:cluster:cluster-b".to_string();
            state.tags.insert(arn.clone(), Vec::new());
            state.cache_clusters.insert(
                "cluster-b".to_string(),
                CacheCluster {
                    cache_cluster_id: "cluster-b".to_string(),
                    cache_node_type: "cache.t3.micro".to_string(),
                    engine: "valkey".to_string(),
                    engine_version: "8.0".to_string(),
                    cache_cluster_status: "available".to_string(),
                    num_cache_nodes: 2,
                    preferred_availability_zone: "us-east-1b".to_string(),
                    cache_subnet_group_name: Some("default".to_string()),
                    auto_minor_version_upgrade: false,
                    arn,
                    created_at: "2024-01-02T00:00:00Z".to_string(),
                    endpoint_address: "127.0.0.1".to_string(),
                    endpoint_port: 6380,
                    container_id: "def456".to_string(),
                    host_port: 6380,
                    replication_group_id: None,
                },
            );
        }

        let req = request("DescribeCacheClusters", &[]);
        let resp = service.describe_cache_clusters(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<CacheClusterId>cluster-a</CacheClusterId>"));
        assert!(body.contains("<CacheClusterId>cluster-b</CacheClusterId>"));
        assert!(body.contains("<DescribeCacheClustersResponse"));
    }

    #[tokio::test]
    async fn create_cache_cluster_validates_engine_before_runtime() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);

        let req = request(
            "CreateCacheCluster",
            &[("CacheClusterId", "bad-engine"), ("Engine", "memcached")],
        );
        assert!(service.create_cache_cluster(&req).await.is_err());
    }

    #[tokio::test]
    async fn create_cache_cluster_without_runtime_cancels_reservation() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared.clone());

        let req = request("CreateCacheCluster", &[("CacheClusterId", "no-runtime")]);
        assert!(service.create_cache_cluster(&req).await.is_err());

        let mut state = shared.write();
        assert!(state.begin_cache_cluster_creation("no-runtime"));
    }

    #[test]
    fn describe_cache_clusters_filters_by_id_and_shows_node_info() {
        let service = service_with_cache_cluster("nodeful-cluster");
        let req = request(
            "DescribeCacheClusters",
            &[
                ("CacheClusterId", "nodeful-cluster"),
                ("ShowCacheNodeInfo", "true"),
            ],
        );
        let resp = service.describe_cache_clusters(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<CacheClusterId>nodeful-cluster</CacheClusterId>"));
        assert!(body.contains("<CacheNodes>"));
        assert!(body.contains("<CacheNodeId>0001</CacheNodeId>"));
        assert!(body.contains("<ParameterGroupStatus>in-sync</ParameterGroupStatus>"));
    }

    #[test]
    fn describe_cache_clusters_not_found() {
        let service = service_with_cache_cluster("cluster-a");
        let req = request("DescribeCacheClusters", &[("CacheClusterId", "missing")]);
        assert!(service.describe_cache_clusters(&req).is_err());
    }

    #[tokio::test]
    async fn delete_cache_cluster_removes_state_and_tags() {
        let service = service_with_cache_cluster("delete-me");
        let arn = "arn:aws:elasticache:us-east-1:123456789012:cluster:delete-me".to_string();

        let req = request("DeleteCacheCluster", &[("CacheClusterId", "delete-me")]);
        let resp = service.delete_cache_cluster(&req).await.unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<CacheClusterStatus>deleting</CacheClusterStatus>"));
        assert!(body.contains("<DeleteCacheClusterResponse"));
        assert!(!service
            .state
            .read()
            .cache_clusters
            .contains_key("delete-me"));
        assert!(!service.state.read().tags.contains_key(&arn));
    }

    #[test]
    fn add_cluster_to_replication_group_updates_members_and_count() {
        let mut state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        state.replication_groups.insert(
            "rg-1".to_string(),
            ReplicationGroup {
                replication_group_id: "rg-1".to_string(),
                description: "test group".to_string(),
                status: "available".to_string(),
                cache_node_type: "cache.t3.micro".to_string(),
                engine: "redis".to_string(),
                engine_version: "7.1".to_string(),
                num_cache_clusters: 1,
                automatic_failover_enabled: false,
                endpoint_address: "127.0.0.1".to_string(),
                endpoint_port: 6379,
                arn: "arn:aws:elasticache:us-east-1:123456789012:replicationgroup:rg-1".to_string(),
                created_at: "2024-01-01T00:00:00Z".to_string(),
                container_id: "abc123".to_string(),
                host_port: 6379,
                member_clusters: vec!["rg-1-001".to_string()],
                snapshot_retention_limit: 0,
                snapshot_window: "05:00-09:00".to_string(),
            },
        );

        add_cluster_to_replication_group(&mut state, "rg-1", "manual-cluster");

        let group = state.replication_groups.get("rg-1").unwrap();
        assert_eq!(group.member_clusters, vec!["rg-1-001", "manual-cluster"]);
        assert_eq!(group.num_cache_clusters, 2);
    }

    #[tokio::test]
    async fn delete_cache_cluster_removes_cluster_from_replication_group() {
        let service = service_with_cache_cluster("delete-rg-cluster");
        {
            let mut state = service.state.write();
            state
                .cache_clusters
                .get_mut("delete-rg-cluster")
                .unwrap()
                .replication_group_id = Some("delete-rg".to_string());
            state.replication_groups.insert(
                "delete-rg".to_string(),
                ReplicationGroup {
                    replication_group_id: "delete-rg".to_string(),
                    description: "test group".to_string(),
                    status: "available".to_string(),
                    cache_node_type: "cache.t3.micro".to_string(),
                    engine: "redis".to_string(),
                    engine_version: "7.1".to_string(),
                    num_cache_clusters: 2,
                    automatic_failover_enabled: false,
                    endpoint_address: "127.0.0.1".to_string(),
                    endpoint_port: 6379,
                    arn: "arn:aws:elasticache:us-east-1:123456789012:replicationgroup:delete-rg"
                        .to_string(),
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                    container_id: "abc123".to_string(),
                    host_port: 6379,
                    member_clusters: vec![
                        "delete-rg-001".to_string(),
                        "delete-rg-cluster".to_string(),
                    ],
                    snapshot_retention_limit: 0,
                    snapshot_window: "05:00-09:00".to_string(),
                },
            );
        }

        let req = request(
            "DeleteCacheCluster",
            &[("CacheClusterId", "delete-rg-cluster")],
        );
        service.delete_cache_cluster(&req).await.unwrap();

        let group = service
            .state
            .read()
            .replication_groups
            .get("delete-rg")
            .unwrap()
            .clone();
        assert_eq!(group.member_clusters, vec!["delete-rg-001"]);
        assert_eq!(group.num_cache_clusters, 1);
    }

    #[test]
    fn create_snapshot_rejects_standalone_cache_cluster_id() {
        let service = service_with_cache_cluster("standalone");
        let req = request(
            "CreateSnapshot",
            &[
                ("SnapshotName", "standalone-snap"),
                ("CacheClusterId", "standalone"),
            ],
        );
        assert!(service.create_snapshot(&req).is_err());
    }

    fn service_with_replication_group(group_id: &str, num_clusters: i32) -> ElastiCacheService {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        {
            let mut s = shared.write();
            let member_clusters: Vec<String> = (1..=num_clusters)
                .map(|i| format!("{group_id}-{i:03}"))
                .collect();
            let arn =
                format!("arn:aws:elasticache:us-east-1:123456789012:replicationgroup:{group_id}");
            s.tags.insert(arn.clone(), Vec::new());
            s.replication_groups.insert(
                group_id.to_string(),
                ReplicationGroup {
                    replication_group_id: group_id.to_string(),
                    description: "test group".to_string(),
                    status: "available".to_string(),
                    cache_node_type: "cache.t3.micro".to_string(),
                    engine: "redis".to_string(),
                    engine_version: "7.1".to_string(),
                    num_cache_clusters: num_clusters,
                    automatic_failover_enabled: false,
                    endpoint_address: "127.0.0.1".to_string(),
                    endpoint_port: 6379,
                    arn,
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                    container_id: "abc123".to_string(),
                    host_port: 6379,
                    member_clusters,
                    snapshot_retention_limit: 0,
                    snapshot_window: "05:00-09:00".to_string(),
                },
            );
        }
        ElastiCacheService::new(shared)
    }

    fn service_with_serverless_cache(cache_name: &str) -> ElastiCacheService {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        {
            let mut s = shared.write();
            let arn =
                format!("arn:aws:elasticache:us-east-1:123456789012:serverlesscache:{cache_name}");
            s.tags.insert(arn.clone(), Vec::new());
            s.serverless_caches.insert(
                cache_name.to_string(),
                ServerlessCache {
                    serverless_cache_name: cache_name.to_string(),
                    description: "serverless cache".to_string(),
                    engine: "redis".to_string(),
                    major_engine_version: "7.1".to_string(),
                    full_engine_version: "7.1".to_string(),
                    status: "available".to_string(),
                    endpoint: ServerlessCacheEndpoint {
                        address: "127.0.0.1".to_string(),
                        port: 6379,
                    },
                    reader_endpoint: ServerlessCacheEndpoint {
                        address: "127.0.0.1".to_string(),
                        port: 6379,
                    },
                    arn,
                    created_at: "2024-01-01T00:00:00Z".to_string(),
                    cache_usage_limits: Some(ServerlessCacheUsageLimits {
                        data_storage: Some(ServerlessCacheDataStorage {
                            maximum: Some(10),
                            minimum: Some(1),
                            unit: Some("GB".to_string()),
                        }),
                        ecpu_per_second: Some(ServerlessCacheEcpuPerSecond {
                            maximum: Some(5000),
                            minimum: Some(1000),
                        }),
                    }),
                    security_group_ids: vec!["sg-123".to_string()],
                    subnet_ids: vec!["subnet-123".to_string()],
                    kms_key_id: Some("kms-123".to_string()),
                    user_group_id: None,
                    snapshot_retention_limit: Some(1),
                    daily_snapshot_time: Some("03:00".to_string()),
                    container_id: "cid".to_string(),
                    host_port: 6379,
                },
            );
        }
        ElastiCacheService::new(shared)
    }

    #[test]
    fn modify_replication_group_updates_description() {
        let service = service_with_replication_group("my-rg", 1);
        let req = request(
            "ModifyReplicationGroup",
            &[
                ("ReplicationGroupId", "my-rg"),
                ("ReplicationGroupDescription", "Updated description"),
            ],
        );
        let resp = service.modify_replication_group(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<Description>Updated description</Description>"));
        assert!(body.contains("<ModifyReplicationGroupResponse"));
    }

    #[test]
    fn modify_replication_group_updates_multiple_fields() {
        let service = service_with_replication_group("my-rg", 1);
        let req = request(
            "ModifyReplicationGroup",
            &[
                ("ReplicationGroupId", "my-rg"),
                ("CacheNodeType", "cache.m5.large"),
                ("AutomaticFailoverEnabled", "true"),
                ("SnapshotRetentionLimit", "5"),
                ("SnapshotWindow", "02:00-06:00"),
            ],
        );
        let resp = service.modify_replication_group(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<CacheNodeType>cache.m5.large</CacheNodeType>"));
        assert!(body.contains("<AutomaticFailover>enabled</AutomaticFailover>"));
        assert!(body.contains("<SnapshotRetentionLimit>5</SnapshotRetentionLimit>"));
        assert!(body.contains("<SnapshotWindow>02:00-06:00</SnapshotWindow>"));
    }

    #[test]
    fn modify_replication_group_not_found() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);
        let req = request(
            "ModifyReplicationGroup",
            &[("ReplicationGroupId", "nonexistent")],
        );
        assert!(service.modify_replication_group(&req).is_err());
    }

    #[test]
    fn parse_cache_usage_limits_reads_nested_query_shape() {
        let req = request(
            "CreateServerlessCache",
            &[
                ("CacheUsageLimits.DataStorage.Maximum", "10"),
                ("CacheUsageLimits.DataStorage.Minimum", "2"),
                ("CacheUsageLimits.DataStorage.Unit", "GB"),
                ("CacheUsageLimits.ECPUPerSecond.Maximum", "5000"),
                ("CacheUsageLimits.ECPUPerSecond.Minimum", "1000"),
            ],
        );

        let limits = parse_cache_usage_limits(&req).unwrap().unwrap();
        let data_storage = limits.data_storage.unwrap();
        assert_eq!(data_storage.maximum, Some(10));
        assert_eq!(data_storage.minimum, Some(2));
        assert_eq!(data_storage.unit.as_deref(), Some("GB"));

        let ecpu = limits.ecpu_per_second.unwrap();
        assert_eq!(ecpu.maximum, Some(5000));
        assert_eq!(ecpu.minimum, Some(1000));
    }

    #[test]
    fn serverless_cache_xml_contains_expected_fields() {
        let cache = service_with_serverless_cache("cache-a")
            .state
            .read()
            .serverless_caches["cache-a"]
            .clone();

        let xml = serverless_cache_xml(&cache);
        assert!(xml.contains("<ServerlessCacheName>cache-a</ServerlessCacheName>"));
        assert!(xml.contains("<Engine>redis</Engine>"));
        assert!(xml.contains("<MajorEngineVersion>7.1</MajorEngineVersion>"));
        assert!(xml.contains("<Endpoint><Address>127.0.0.1</Address><Port>6379</Port></Endpoint>"));
        assert!(xml.contains(
            "<ReaderEndpoint><Address>127.0.0.1</Address><Port>6379</Port></ReaderEndpoint>"
        ));
        assert!(xml.contains(
            "<SecurityGroupIds><SecurityGroupId>sg-123</SecurityGroupId></SecurityGroupIds>"
        ));
        assert!(xml.contains("<SubnetIds><member>subnet-123</member></SubnetIds>"));
        assert!(xml.contains("<CacheUsageLimits>"));
    }

    #[test]
    fn serverless_cache_snapshot_xml_contains_expected_fields() {
        let snapshot = ServerlessCacheSnapshot {
            serverless_cache_snapshot_name: "snap-a".to_string(),
            arn: "arn:aws:elasticache:us-east-1:123456789012:serverlesssnapshot:snap-a".to_string(),
            kms_key_id: Some("kms-123".to_string()),
            snapshot_type: "manual".to_string(),
            status: "available".to_string(),
            create_time: "2024-01-01T00:00:00Z".to_string(),
            expiry_time: None,
            bytes_used_for_cache: Some("0".to_string()),
            serverless_cache_name: "cache-a".to_string(),
            engine: "redis".to_string(),
            major_engine_version: "7.1".to_string(),
        };

        let xml = serverless_cache_snapshot_xml(&snapshot);
        assert!(xml.contains("<ServerlessCacheSnapshotName>snap-a</ServerlessCacheSnapshotName>"));
        assert!(xml.contains("<KmsKeyId>kms-123</KmsKeyId>"));
        assert!(xml.contains("<SnapshotType>manual</SnapshotType>"));
        assert!(xml.contains("<ServerlessCacheConfiguration>"));
        assert!(xml.contains("<ServerlessCacheName>cache-a</ServerlessCacheName>"));
    }

    #[test]
    fn describe_serverless_caches_returns_all() {
        let service = service_with_serverless_cache("cache-a");
        {
            let mut state = service.state.write();
            state.serverless_caches.insert(
                "cache-b".to_string(),
                ServerlessCache {
                    serverless_cache_name: "cache-b".to_string(),
                    description: "serverless cache".to_string(),
                    engine: "valkey".to_string(),
                    major_engine_version: "8.0".to_string(),
                    full_engine_version: "8.0".to_string(),
                    status: "available".to_string(),
                    endpoint: ServerlessCacheEndpoint {
                        address: "127.0.0.1".to_string(),
                        port: 6380,
                    },
                    reader_endpoint: ServerlessCacheEndpoint {
                        address: "127.0.0.1".to_string(),
                        port: 6380,
                    },
                    arn: "arn:aws:elasticache:us-east-1:123456789012:serverlesscache:cache-b"
                        .to_string(),
                    created_at: "2024-01-02T00:00:00Z".to_string(),
                    cache_usage_limits: None,
                    security_group_ids: Vec::new(),
                    subnet_ids: Vec::new(),
                    kms_key_id: None,
                    user_group_id: None,
                    snapshot_retention_limit: None,
                    daily_snapshot_time: None,
                    container_id: "cid".to_string(),
                    host_port: 6380,
                },
            );
        }

        let resp = service
            .describe_serverless_caches(&request("DescribeServerlessCaches", &[]))
            .unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<ServerlessCacheName>cache-a</ServerlessCacheName>"));
        assert!(body.contains("<ServerlessCacheName>cache-b</ServerlessCacheName>"));
    }

    #[test]
    fn modify_serverless_cache_updates_fields() {
        let service = service_with_serverless_cache("cache-a");
        let req = request(
            "ModifyServerlessCache",
            &[
                ("ServerlessCacheName", "cache-a"),
                ("Description", "updated"),
                ("SecurityGroupIds.SecurityGroupId.1", "sg-999"),
                ("SnapshotRetentionLimit", "7"),
                ("DailySnapshotTime", "05:00"),
            ],
        );

        let resp = service.modify_serverless_cache(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<Description>updated</Description>"));
        assert!(body.contains(
            "<SecurityGroupIds><SecurityGroupId>sg-999</SecurityGroupId></SecurityGroupIds>"
        ));
        assert!(body.contains("<SnapshotRetentionLimit>7</SnapshotRetentionLimit>"));
        assert!(body.contains("<DailySnapshotTime>05:00</DailySnapshotTime>"));
    }

    #[test]
    fn parse_query_list_param_reads_indexed_and_flat_query_values() {
        let req = AwsRequest {
            service: "elasticache".to_string(),
            action: "ModifyServerlessCache".to_string(),
            region: "us-east-1".to_string(),
            account_id: "000000000000".to_string(),
            request_id: "req-1".to_string(),
            headers: HeaderMap::new(),
            query_params: HashMap::from([
                ("SecurityGroupIds.member.1".to_string(), "sg-a".to_string()),
                ("SecurityGroupIds.member.2".to_string(), "sg-b".to_string()),
            ]),
            body: Bytes::new(),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: true,
            access_key_id: None,
        };
        assert_eq!(
            parse_query_list_param(&req, "SecurityGroupIds", "SecurityGroupId"),
            vec!["sg-a".to_string(), "sg-b".to_string()]
        );

        let req = AwsRequest {
            query_params: HashMap::from([("SecurityGroupIds".to_string(), "sg-flat".to_string())]),
            ..req
        };
        assert_eq!(
            parse_query_list_param(&req, "SecurityGroupIds", "SecurityGroupId"),
            vec!["sg-flat".to_string()]
        );
    }

    #[test]
    fn describe_serverless_cache_snapshots_filters_by_cache_name() {
        let service = service_with_serverless_cache("cache-a");
        {
            let mut state = service.state.write();
            state.serverless_cache_snapshots.insert(
                "snap-a".to_string(),
                ServerlessCacheSnapshot {
                    serverless_cache_snapshot_name: "snap-a".to_string(),
                    arn: "arn:aws:elasticache:us-east-1:123456789012:serverlesssnapshot:snap-a"
                        .to_string(),
                    kms_key_id: None,
                    snapshot_type: "manual".to_string(),
                    status: "available".to_string(),
                    create_time: "2024-01-01T00:00:00Z".to_string(),
                    expiry_time: None,
                    bytes_used_for_cache: None,
                    serverless_cache_name: "cache-a".to_string(),
                    engine: "redis".to_string(),
                    major_engine_version: "7.1".to_string(),
                },
            );
        }

        let resp = service
            .describe_serverless_cache_snapshots(&request(
                "DescribeServerlessCacheSnapshots",
                &[("ServerlessCacheName", "cache-a")],
            ))
            .unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<ServerlessCacheSnapshotName>snap-a</ServerlessCacheSnapshotName>"));
    }

    #[test]
    fn delete_serverless_cache_snapshot_removes_tags() {
        let service = service_with_serverless_cache("cache-a");
        {
            let mut state = service.state.write();
            let arn =
                "arn:aws:elasticache:us-east-1:123456789012:serverlesssnapshot:snap-a".to_string();
            state.tags.insert(arn.clone(), Vec::new());
            state.serverless_cache_snapshots.insert(
                "snap-a".to_string(),
                ServerlessCacheSnapshot {
                    serverless_cache_snapshot_name: "snap-a".to_string(),
                    arn,
                    kms_key_id: None,
                    snapshot_type: "manual".to_string(),
                    status: "available".to_string(),
                    create_time: "2024-01-01T00:00:00Z".to_string(),
                    expiry_time: None,
                    bytes_used_for_cache: None,
                    serverless_cache_name: "cache-a".to_string(),
                    engine: "redis".to_string(),
                    major_engine_version: "7.1".to_string(),
                },
            );
        }

        let resp = service
            .delete_serverless_cache_snapshot(&request(
                "DeleteServerlessCacheSnapshot",
                &[("ServerlessCacheSnapshotName", "snap-a")],
            ))
            .unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<Status>deleting</Status>"));
        assert!(!service
            .state
            .read()
            .tags
            .contains_key("arn:aws:elasticache:us-east-1:123456789012:serverlesssnapshot:snap-a"));
    }

    #[test]
    fn increase_replica_count_updates_member_clusters() {
        let service = service_with_replication_group("my-rg", 1);
        let req = request(
            "IncreaseReplicaCount",
            &[
                ("ReplicationGroupId", "my-rg"),
                ("ApplyImmediately", "true"),
                ("NewReplicaCount", "2"),
            ],
        );
        let resp = service.increase_replica_count(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<ClusterId>my-rg-001</ClusterId>"));
        assert!(body.contains("<ClusterId>my-rg-002</ClusterId>"));
        assert!(body.contains("<ClusterId>my-rg-003</ClusterId>"));
        assert!(body.contains("<IncreaseReplicaCountResponse"));
    }

    #[test]
    fn increase_replica_count_rejects_same_or_lower() {
        let service = service_with_replication_group("my-rg", 3);
        let req = request(
            "IncreaseReplicaCount",
            &[
                ("ReplicationGroupId", "my-rg"),
                ("ApplyImmediately", "true"),
                ("NewReplicaCount", "2"),
            ],
        );
        assert!(service.increase_replica_count(&req).is_err());
    }

    #[test]
    fn increase_replica_count_not_found() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);
        let req = request(
            "IncreaseReplicaCount",
            &[
                ("ReplicationGroupId", "nonexistent"),
                ("ApplyImmediately", "true"),
                ("NewReplicaCount", "2"),
            ],
        );
        assert!(service.increase_replica_count(&req).is_err());
    }

    #[test]
    fn decrease_replica_count_updates_member_clusters() {
        let service = service_with_replication_group("my-rg", 3);
        let req = request(
            "DecreaseReplicaCount",
            &[
                ("ReplicationGroupId", "my-rg"),
                ("ApplyImmediately", "true"),
                ("NewReplicaCount", "1"),
            ],
        );
        let resp = service.decrease_replica_count(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<ClusterId>my-rg-001</ClusterId>"));
        assert!(body.contains("<ClusterId>my-rg-002</ClusterId>"));
        assert!(!body.contains("<ClusterId>my-rg-003</ClusterId>"));
        assert!(body.contains("<DecreaseReplicaCountResponse"));
    }

    #[test]
    fn decrease_replica_count_validates_minimum() {
        let service = service_with_replication_group("my-rg", 1);
        // NewReplicaCount=0 means total=1, which is not fewer than current 1
        let req = request(
            "DecreaseReplicaCount",
            &[
                ("ReplicationGroupId", "my-rg"),
                ("ApplyImmediately", "true"),
                ("NewReplicaCount", "0"),
            ],
        );
        assert!(service.decrease_replica_count(&req).is_err());
    }

    #[test]
    fn decrease_replica_count_rejects_negative() {
        let service = service_with_replication_group("my-rg", 2);
        let req = request(
            "DecreaseReplicaCount",
            &[
                ("ReplicationGroupId", "my-rg"),
                ("ApplyImmediately", "true"),
                ("NewReplicaCount", "-1"),
            ],
        );
        assert!(service.decrease_replica_count(&req).is_err());
    }

    #[test]
    fn test_failover_validates_node_group() {
        let service = service_with_replication_group("my-rg", 1);
        let req = request(
            "TestFailover",
            &[("ReplicationGroupId", "my-rg"), ("NodeGroupId", "0001")],
        );
        let resp = service.test_failover(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<Status>available</Status>"));
        assert!(body.contains("<TestFailoverResponse"));
    }

    #[test]
    fn test_failover_rejects_invalid_node_group() {
        let service = service_with_replication_group("my-rg", 1);
        let req = request(
            "TestFailover",
            &[("ReplicationGroupId", "my-rg"), ("NodeGroupId", "9999")],
        );
        assert!(service.test_failover(&req).is_err());
    }

    #[test]
    fn test_failover_not_found() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);
        let req = request(
            "TestFailover",
            &[
                ("ReplicationGroupId", "nonexistent"),
                ("NodeGroupId", "0001"),
            ],
        );
        assert!(service.test_failover(&req).is_err());
    }

    // -----------------------------------------------------------------------
    // Snapshot tests
    // -----------------------------------------------------------------------

    #[test]
    fn create_snapshot_returns_snapshot_xml() {
        let service = service_with_replication_group("snap-rg", 1);
        let req = request(
            "CreateSnapshot",
            &[
                ("SnapshotName", "my-snap"),
                ("ReplicationGroupId", "snap-rg"),
            ],
        );
        let resp = service.create_snapshot(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<SnapshotName>my-snap</SnapshotName>"));
        assert!(body.contains("<ReplicationGroupId>snap-rg</ReplicationGroupId>"));
        assert!(body.contains("<SnapshotStatus>available</SnapshotStatus>"));
        assert!(body.contains("<SnapshotSource>manual</SnapshotSource>"));
        assert!(body.contains("<Engine>redis</Engine>"));
        assert!(body.contains("<CreateSnapshotResponse"));
    }

    #[test]
    fn create_snapshot_via_cache_cluster_id() {
        let service = service_with_replication_group("cc-rg", 2);
        let req = request(
            "CreateSnapshot",
            &[
                ("SnapshotName", "cluster-snap"),
                ("CacheClusterId", "cc-rg-001"),
            ],
        );
        let resp = service.create_snapshot(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<ReplicationGroupId>cc-rg</ReplicationGroupId>"));
    }

    #[test]
    fn create_snapshot_rejects_missing_group_and_cluster() {
        let service = service_with_replication_group("rg", 1);
        let req = request("CreateSnapshot", &[("SnapshotName", "bad-snap")]);
        assert!(service.create_snapshot(&req).is_err());
    }

    #[test]
    fn create_snapshot_rejects_duplicate_name() {
        let service = service_with_replication_group("dup-rg", 1);
        let req = request(
            "CreateSnapshot",
            &[
                ("SnapshotName", "dup-snap"),
                ("ReplicationGroupId", "dup-rg"),
            ],
        );
        service.create_snapshot(&req).unwrap();
        assert!(service.create_snapshot(&req).is_err());
    }

    #[test]
    fn create_snapshot_rejects_nonexistent_group() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);
        let req = request(
            "CreateSnapshot",
            &[
                ("SnapshotName", "orphan"),
                ("ReplicationGroupId", "no-such-rg"),
            ],
        );
        assert!(service.create_snapshot(&req).is_err());
    }

    #[test]
    fn create_snapshot_rejects_missing_name() {
        let service = service_with_replication_group("rg", 1);
        let req = request("CreateSnapshot", &[("ReplicationGroupId", "rg")]);
        assert!(service.create_snapshot(&req).is_err());
    }

    #[test]
    fn create_snapshot_registers_arn_for_tags() {
        let service = service_with_replication_group("tag-rg", 1);
        let req = request(
            "CreateSnapshot",
            &[
                ("SnapshotName", "tag-snap"),
                ("ReplicationGroupId", "tag-rg"),
            ],
        );
        service.create_snapshot(&req).unwrap();

        let state = service.state.read();
        let arn = "arn:aws:elasticache:us-east-1:123456789012:snapshot:tag-snap".to_string();
        assert!(state.tags.contains_key(&arn));
    }

    #[test]
    fn describe_snapshots_returns_all() {
        let service = service_with_replication_group("desc-rg", 1);
        for name in &["snap-a", "snap-b"] {
            let req = request(
                "CreateSnapshot",
                &[("SnapshotName", name), ("ReplicationGroupId", "desc-rg")],
            );
            service.create_snapshot(&req).unwrap();
        }
        let req = request("DescribeSnapshots", &[]);
        let resp = service.describe_snapshots(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<SnapshotName>snap-a</SnapshotName>"));
        assert!(body.contains("<SnapshotName>snap-b</SnapshotName>"));
        assert!(body.contains("<DescribeSnapshotsResponse"));
    }

    #[test]
    fn describe_snapshots_filters_by_name() {
        let service = service_with_replication_group("filt-rg", 1);
        for name in &["snap-1", "snap-2"] {
            let req = request(
                "CreateSnapshot",
                &[("SnapshotName", name), ("ReplicationGroupId", "filt-rg")],
            );
            service.create_snapshot(&req).unwrap();
        }
        let req = request("DescribeSnapshots", &[("SnapshotName", "snap-1")]);
        let resp = service.describe_snapshots(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<SnapshotName>snap-1</SnapshotName>"));
        assert!(!body.contains("<SnapshotName>snap-2</SnapshotName>"));
    }

    #[test]
    fn describe_snapshots_filters_by_replication_group() {
        let service = service_with_replication_group("rg-a", 1);
        let req = request(
            "CreateSnapshot",
            &[
                ("SnapshotName", "rg-a-snap"),
                ("ReplicationGroupId", "rg-a"),
            ],
        );
        service.create_snapshot(&req).unwrap();

        let req = request("DescribeSnapshots", &[("ReplicationGroupId", "rg-a")]);
        let resp = service.describe_snapshots(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<SnapshotName>rg-a-snap</SnapshotName>"));

        // Filter by non-matching group returns empty
        let req = request("DescribeSnapshots", &[("ReplicationGroupId", "rg-b")]);
        let resp = service.describe_snapshots(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(!body.contains("<SnapshotName>"));
    }

    #[test]
    fn describe_snapshots_not_found_by_name() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);
        let req = request("DescribeSnapshots", &[("SnapshotName", "nope")]);
        assert!(service.describe_snapshots(&req).is_err());
    }

    #[test]
    fn delete_snapshot_removes_and_returns_deleting() {
        let service = service_with_replication_group("del-rg", 1);
        let req = request(
            "CreateSnapshot",
            &[
                ("SnapshotName", "del-snap"),
                ("ReplicationGroupId", "del-rg"),
            ],
        );
        service.create_snapshot(&req).unwrap();

        let req = request("DeleteSnapshot", &[("SnapshotName", "del-snap")]);
        let resp = service.delete_snapshot(&req).unwrap();
        let body = String::from_utf8(resp.body.to_vec()).unwrap();
        assert!(body.contains("<SnapshotStatus>deleting</SnapshotStatus>"));
        assert!(body.contains("<DeleteSnapshotResponse"));

        // Verify it's gone
        assert!(!service.state.read().snapshots.contains_key("del-snap"));
    }

    #[test]
    fn delete_snapshot_cleans_up_tags() {
        let service = service_with_replication_group("tag-del-rg", 1);
        let req = request(
            "CreateSnapshot",
            &[
                ("SnapshotName", "tag-del-snap"),
                ("ReplicationGroupId", "tag-del-rg"),
            ],
        );
        service.create_snapshot(&req).unwrap();

        let arn = "arn:aws:elasticache:us-east-1:123456789012:snapshot:tag-del-snap".to_string();
        assert!(service.state.read().tags.contains_key(&arn));

        let req = request("DeleteSnapshot", &[("SnapshotName", "tag-del-snap")]);
        service.delete_snapshot(&req).unwrap();
        assert!(!service.state.read().tags.contains_key(&arn));
    }

    #[test]
    fn delete_snapshot_not_found() {
        let state = crate::state::ElastiCacheState::new("123456789012", "us-east-1");
        let shared = std::sync::Arc::new(parking_lot::RwLock::new(state));
        let service = ElastiCacheService::new(shared);
        let req = request("DeleteSnapshot", &[("SnapshotName", "nope")]);
        assert!(service.delete_snapshot(&req).is_err());
    }

    #[test]
    fn snapshot_xml_contains_all_fields() {
        let snap = CacheSnapshot {
            snapshot_name: "test-snap".to_string(),
            replication_group_id: "rg-1".to_string(),
            replication_group_description: "desc".to_string(),
            snapshot_status: "available".to_string(),
            cache_node_type: "cache.t3.micro".to_string(),
            engine: "redis".to_string(),
            engine_version: "7.1".to_string(),
            num_cache_clusters: 2,
            arn: "arn:aws:elasticache:us-east-1:123:snapshot:test-snap".to_string(),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            snapshot_source: "manual".to_string(),
        };
        let xml = snapshot_xml(&snap);
        assert!(xml.contains("<SnapshotName>test-snap</SnapshotName>"));
        assert!(xml.contains("<ReplicationGroupId>rg-1</ReplicationGroupId>"));
        assert!(xml.contains("<SnapshotStatus>available</SnapshotStatus>"));
        assert!(xml.contains("<SnapshotSource>manual</SnapshotSource>"));
        assert!(xml.contains("<CacheNodeType>cache.t3.micro</CacheNodeType>"));
        assert!(xml.contains("<Engine>redis</Engine>"));
        assert!(xml.contains("<EngineVersion>7.1</EngineVersion>"));
        assert!(xml.contains("<NumCacheClusters>2</NumCacheClusters>"));
        assert!(xml.contains("<ARN>arn:aws:elasticache:us-east-1:123:snapshot:test-snap</ARN>"));
    }
}
