use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;

use fakecloud_aws::xml::xml_escape;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::runtime::{ElastiCacheRuntime, RuntimeError};
use crate::state::{
    default_engine_versions, default_parameters_for_family, CacheEngineVersion,
    CacheParameterGroup, CacheSubnetGroup, EngineDefaultParameter, ReplicationGroup,
    SharedElastiCacheState,
};

const ELASTICACHE_NS: &str = "http://elasticache.amazonaws.com/doc/2015-02-02/";
const SUPPORTED_ACTIONS: &[&str] = &[
    "AddTagsToResource",
    "CreateCacheSubnetGroup",
    "CreateReplicationGroup",
    "DeleteCacheSubnetGroup",
    "DeleteReplicationGroup",
    "DescribeCacheEngineVersions",
    "DescribeCacheParameterGroups",
    "DescribeCacheSubnetGroups",
    "DescribeEngineDefaultParameters",
    "DescribeReplicationGroups",
    "ListTagsForResource",
    "ModifyCacheSubnetGroup",
    "RemoveTagsFromResource",
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
            "CreateCacheSubnetGroup" => self.create_cache_subnet_group(&request),
            "CreateReplicationGroup" => self.create_replication_group(&request).await,
            "DeleteCacheSubnetGroup" => self.delete_cache_subnet_group(&request),
            "DeleteReplicationGroup" => self.delete_replication_group(&request).await,
            "DescribeCacheEngineVersions" => self.describe_cache_engine_versions(&request),
            "DescribeCacheParameterGroups" => self.describe_cache_parameter_groups(&request),
            "DescribeCacheSubnetGroups" => self.describe_cache_subnet_groups(&request),
            "DescribeEngineDefaultParameters" => self.describe_engine_default_parameters(&request),
            "DescribeReplicationGroups" => self.describe_replication_groups(&request),
            "ListTagsForResource" => self.list_tags_for_resource(&request),
            "ModifyCacheSubnetGroup" => self.modify_cache_subnet_group(&request),
            "RemoveTagsFromResource" => self.remove_tags_from_resource(&request),
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

        if state.subnet_groups.remove(&name).is_none() {
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
            state
                .replication_groups
                .remove(&replication_group_id)
                .ok_or_else(|| {
                    AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "ReplicationGroupNotFoundFault",
                        format!("ReplicationGroup {replication_group_id} not found."),
                    )
                })?
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

    let end = (start + limit).min(items.len());
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
         <SnapshotRetentionLimit>0</SnapshotRetentionLimit>\
         <SnapshotWindow>05:00-09:00</SnapshotWindow>\
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
        xml_escape(&g.cache_node_type),
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
    use http::HeaderMap;
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
}
