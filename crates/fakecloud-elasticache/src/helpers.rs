use super::*;

/// Engine default listening port: 11211 for memcached, 6379 for
/// redis/valkey. Used so describe responses never emit `Port=0` when no
/// container runtime assigned a host port.
pub(crate) fn engine_default_port(engine: &str) -> u16 {
    if engine.eq_ignore_ascii_case("memcached") {
        11211
    } else {
        6379
    }
}

/// Resolve the port to serialize for an endpoint. Prefer the live container
/// port, then the configured `Port` from the create request, then the engine
/// default — so the endpoint is never `0` even when no container runtime is
/// available to assign a host port.
pub(crate) fn effective_endpoint_port(live: u16, configured: u16, engine: &str) -> u16 {
    if live != 0 {
        live
    } else if configured != 0 {
        configured
    } else {
        engine_default_port(engine)
    }
}

/// Match the Smithy constraints on `com.amazonaws.elasticache#UserId`:
/// `@length(min=1)` and `@pattern("^[a-zA-Z][a-zA-Z0-9\\-]*$")`. Returns
/// false for an empty value or one whose shape violates the pattern.
pub(crate) fn is_valid_user_id(value: &str) -> bool {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !first.is_ascii_alphabetic() {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '-')
}

/// Match the Smithy `SourceType` enum on `DescribeEvents`. Returns false
/// when the value is not one of the documented wire forms.
pub(crate) fn is_valid_source_type(value: &str) -> bool {
    matches!(
        value,
        "cache-cluster"
            | "cache-parameter-group"
            | "cache-security-group"
            | "cache-subnet-group"
            | "replication-group"
            | "serverless-cache"
            | "serverless-cache-snapshot"
            | "user"
            | "user-group"
    )
}

pub(crate) fn parse_required_bool(req: &AwsRequest, name: &str) -> Result<bool, AwsServiceError> {
    parse_optional_bool(Some(&required_query_param(req, name)?))?.ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            format!("Boolean parameter {name} is invalid."),
        )
    })
}

pub(crate) fn validate_serverless_engine(engine: &str) -> Result<(), AwsServiceError> {
    validate_engine(engine)?;
    if engine == ENGINE_MEMCACHED {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            "Serverless caches are not supported for the memcached engine.".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn default_major_engine_version(engine: &str) -> &'static str {
    if engine == ENGINE_VALKEY {
        "8.0"
    } else {
        "7.1"
    }
}

pub(crate) fn default_full_engine_version(
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

    if (engine == ENGINE_REDIS && !major_engine_version.starts_with('7'))
        || (engine == ENGINE_VALKEY && !major_engine_version.starts_with('8'))
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

pub(crate) fn collect_indexed_strings(req: &AwsRequest, prefix: &str) -> Vec<String> {
    let mut out = Vec::new();
    for i in 1..=64 {
        let key = format!("{prefix}.{i}");
        match req.query_params.get(&key) {
            Some(v) => out.push(v.clone()),
            None => break,
        }
    }
    out
}

/// Pull values out of an AWS Query protocol indexed list of structures,
/// where each entry has a named field (e.g. `member.1.Address`,
/// `member.2.Address`). Returns values in index order.
pub(crate) fn collect_member_field(req: &AwsRequest, prefix: &str, field: &str) -> Vec<String> {
    let mut out = Vec::new();
    for i in 1..=64 {
        let key = format!("{prefix}.{i}.{field}");
        match req.query_params.get(&key) {
            Some(v) => out.push(v.clone()),
            None => break,
        }
    }
    out
}

pub(crate) fn collect_indexed_pairs(
    req: &AwsRequest,
    prefix: &str,
    a: &str,
    b: &str,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for i in 1..=64 {
        let key_a = format!("{prefix}.{i}.{a}");
        let key_b = format!("{prefix}.{i}.{b}");
        match (req.query_params.get(&key_a), req.query_params.get(&key_b)) {
            (Some(av), Some(bv)) => out.push((av.clone(), bv.clone())),
            _ => break,
        }
    }
    out
}

pub(crate) fn cache_security_group_xml(g: &crate::state::CacheSecurityGroup) -> String {
    let ec2_xml: String = g
        .ec2_security_groups
        .iter()
        .map(|e| {
            format!(
                "<EC2SecurityGroup><Status>{}</Status><EC2SecurityGroupName>{}</EC2SecurityGroupName><EC2SecurityGroupOwnerId>{}</EC2SecurityGroupOwnerId></EC2SecurityGroup>",
                xml_escape(&e.status),
                xml_escape(&e.ec2_security_group_name),
                xml_escape(&e.ec2_security_group_owner_id),
            )
        })
        .collect();
    format!(
        "<CacheSecurityGroupName>{}</CacheSecurityGroupName>\
         <Description>{}</Description>\
         <OwnerId>{}</OwnerId>\
         <ARN>{}</ARN>\
         <EC2SecurityGroups>{}</EC2SecurityGroups>",
        xml_escape(&g.cache_security_group_name),
        xml_escape(&g.description),
        xml_escape(&g.owner_id),
        xml_escape(&g.arn),
        ec2_xml,
    )
}

pub(crate) fn cache_parameter_xml(p: &crate::state::CacheParameter) -> String {
    format!(
        "<Parameter><ParameterName>{}</ParameterName><ParameterValue>{}</ParameterValue><Description>{}</Description><Source>{}</Source><DataType>{}</DataType><AllowedValues>{}</AllowedValues><IsModifiable>{}</IsModifiable><MinimumEngineVersion>{}</MinimumEngineVersion></Parameter>",
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

pub(crate) fn parse_optional_bool(value: Option<&str>) -> Result<Option<bool>, AwsServiceError> {
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

pub(crate) fn optional_non_negative_i32_param(
    req: &AwsRequest,
    name: &str,
) -> Result<Option<i32>, AwsServiceError> {
    optional_query_param(req, name)
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

pub(crate) fn parse_cache_usage_limits(
    req: &AwsRequest,
) -> Result<Option<ServerlessCacheUsageLimits>, AwsServiceError> {
    let data_storage_maximum =
        optional_non_negative_i32_param(req, "CacheUsageLimits.DataStorage.Maximum")?;
    let data_storage_minimum =
        optional_non_negative_i32_param(req, "CacheUsageLimits.DataStorage.Minimum")?;
    let data_storage_unit = optional_query_param(req, "CacheUsageLimits.DataStorage.Unit");
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
/// Parse `LogDeliveryConfigurations.LogDeliveryConfigurationRequest.N.*`
/// into structured records. ElastiCache emits two destination types:
/// `cloudwatch-logs` and `kinesis-firehose`; details vary by type but
/// we keep the raw payload string.
pub(crate) fn parse_log_delivery_configs(req: &AwsRequest) -> Vec<LogDeliveryConfiguration> {
    let mut out = Vec::new();
    for i in 1.. {
        let base = format!("LogDeliveryConfigurations.LogDeliveryConfigurationRequest.{i}");
        let log_type = optional_query_param(req, &format!("{base}.LogType"));
        let dest_type = optional_query_param(req, &format!("{base}.DestinationType"));
        let enabled = optional_query_param(req, &format!("{base}.Enabled"));
        if log_type.is_none() && dest_type.is_none() && enabled.is_none() {
            break;
        }
        // AWS deletes a log delivery destination when Enabled=false. Skip it
        // here so the modify handler ends up with only the active set.
        if matches!(
            enabled.as_deref(),
            Some("false") | Some("False") | Some("FALSE")
        ) {
            continue;
        }
        let log_format = optional_query_param(req, &format!("{base}.LogFormat"))
            .unwrap_or_else(|| "json".into());
        let cw_group = optional_query_param(
            req,
            &format!("{base}.DestinationDetails.CloudWatchLogsDetails.LogGroup"),
        );
        let kinesis = optional_query_param(
            req,
            &format!("{base}.DestinationDetails.KinesisFirehoseDetails.DeliveryStream"),
        );
        let destination_details = cw_group.or(kinesis);
        out.push(LogDeliveryConfiguration {
            log_type: log_type.unwrap_or_default(),
            destination_type: dest_type.unwrap_or_default(),
            destination_details,
            log_format,
            status: "active".to_string(),
        });
    }
    out
}

pub(crate) fn parse_member_list(
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

pub(crate) fn parse_query_list_param(
    req: &AwsRequest,
    param: &str,
    member_name: &str,
) -> Vec<String> {
    let mut indexed = parse_member_list(&req.query_params, param, member_name);
    if indexed.is_empty() {
        indexed = parse_member_list(&req.query_params, param, "member");
    }
    if indexed.is_empty() {
        indexed = req.query_params.get(param).into_iter().cloned().collect();
    }
    indexed
}

/// Per-shard replica config supplied via
/// `ReplicaConfiguration.ConfigureShard.N.{NodeGroupId,NewReplicaCount}`.
/// Used by IncreaseReplicaCount + DecreaseReplicaCount.
pub(crate) struct ConfigureShard {
    pub new_replica_count: i32,
}

pub(crate) fn parse_replica_configuration(
    req: &AwsRequest,
) -> Result<Vec<ConfigureShard>, AwsServiceError> {
    let prefix = "ReplicaConfiguration.ConfigureShard.";
    let mut indices: Vec<usize> = req
        .query_params
        .keys()
        .filter_map(|k| {
            k.strip_prefix(prefix).and_then(|tail| {
                tail.split_once('.')
                    .and_then(|(idx, _)| idx.parse::<usize>().ok())
            })
        })
        .collect();
    indices.sort_unstable();
    indices.dedup();
    let mut out = Vec::with_capacity(indices.len());
    for idx in indices {
        let id_key = format!("{prefix}{idx}.NodeGroupId");
        let count_key = format!("{prefix}{idx}.NewReplicaCount");
        req.query_params.get(&id_key).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                format!("ReplicaConfiguration entry {idx} is missing NodeGroupId."),
            )
        })?;
        let count_raw = req.query_params.get(&count_key).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MissingParameter",
                format!("ReplicaConfiguration entry {idx} is missing NewReplicaCount."),
            )
        })?;
        let new_replica_count: i32 = count_raw.parse().map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "ReplicaConfiguration entry {idx} has invalid NewReplicaCount '{count_raw}'."
                ),
            )
        })?;
        out.push(ConfigureShard { new_replica_count });
    }
    Ok(out)
}

/// Returns the current per-shard replica count, derived from
/// `replicas_per_node_group` when set or back-computed from
/// `num_cache_clusters / num_node_groups - 1` for legacy state created
/// before that field was populated.
pub(crate) fn current_replicas_per_shard(group: &ReplicationGroup) -> i32 {
    if let Some(r) = group.replicas_per_node_group {
        return r;
    }
    let shards = group.num_node_groups.max(1);
    (group.num_cache_clusters / shards - 1).max(0)
}

/// Rebuild the flat `member_clusters` list as `<rg_id>-NNN` for the given
/// total cluster count. Used by Increase/DecreaseReplicaCount and
/// ModifyReplicationGroupShardConfiguration so DescribeReplicationGroups
/// reflects the new shape.
pub(crate) fn build_member_clusters(replication_group_id: &str, total: i32) -> Vec<String> {
    (1..=total.max(0))
        .map(|i| format!("{replication_group_id}-{i:03}"))
        .collect()
}

pub(crate) fn optional_usize_param(
    req: &AwsRequest,
    name: &str,
) -> Result<Option<usize>, AwsServiceError> {
    optional_query_param(req, name)
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

pub(crate) fn parse_reserved_duration_filter(
    value: Option<String>,
) -> Result<Option<i32>, AwsServiceError> {
    value
        .map(|raw| match raw.as_str() {
            "1" => Ok(31_536_000),
            "3" => Ok(94_608_000),
            "31536000" => Ok(31_536_000),
            "94608000" => Ok(94_608_000),
            _ => Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!(
                    "Invalid value for Duration: {raw}. Valid values are 1, 3, 31536000, or 94608000."
                ),
            )),
        })
        .transpose()
}

/// ElastiCache wraps the core paginate helper to carry its `Option<usize>`
/// max_records convention (default 100, hard cap 100, matching real AWS).
///
/// A `marker` that does not parse as a valid offset is rejected with
/// `InvalidParameterValue` rather than being silently treated as the first
/// page (which can drive an infinite client pagination loop). bug-audit
/// 2026-05-28, 1.7.
pub(crate) fn paginate<T: Clone>(
    items: &[T],
    marker: Option<&str>,
    max_records: Option<usize>,
) -> Result<(Vec<T>, Option<String>), AwsServiceError> {
    let limit = max_records.unwrap_or(100).min(100);
    fakecloud_core::pagination::paginate_checked(items, marker, limit).map_err(|_| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterValue",
            "Invalid value for parameter Marker.",
        )
    })
}

// Tag helpers

pub(crate) fn parse_tags(req: &AwsRequest) -> Result<Vec<(String, String)>, AwsServiceError> {
    let mut tags = Vec::new();
    for index in 1.. {
        let key_name = format!("Tags.Tag.{index}.Key");
        let value_name = format!("Tags.Tag.{index}.Value");
        let key = optional_query_param(req, &key_name);
        let value = optional_query_param(req, &value_name);
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

pub(crate) fn parse_tag_keys(req: &AwsRequest) -> Result<Vec<String>, AwsServiceError> {
    let mut keys = Vec::new();
    for index in 1.. {
        let key_name = format!("TagKeys.member.{index}");
        match optional_query_param(req, &key_name) {
            Some(key) => keys.push(key),
            None => break,
        }
    }
    Ok(keys)
}

pub(crate) fn merge_tags(existing: &mut Vec<(String, String)>, incoming: &[(String, String)]) {
    for (key, value) in incoming {
        if let Some(existing_tag) = existing.iter_mut().find(|(k, _)| k == key) {
            existing_tag.1 = value.clone();
        } else {
            existing.push((key.clone(), value.clone()));
        }
    }
}

pub(crate) fn tag_xml(tag: &(String, String)) -> String {
    format!(
        "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
        xml_escape(&tag.0),
        xml_escape(&tag.1),
    )
}

// Filtering

pub(crate) fn filter_engine_versions(
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

// XML formatting

pub(crate) fn engine_version_xml(v: &CacheEngineVersion) -> String {
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

pub(crate) fn recurring_charge_xml(charge: &RecurringCharge) -> String {
    format!(
        "<RecurringCharge>\
         <RecurringChargeAmount>{}</RecurringChargeAmount>\
         <RecurringChargeFrequency>{}</RecurringChargeFrequency>\
         </RecurringCharge>",
        charge.recurring_charge_amount,
        xml_escape(&charge.recurring_charge_frequency),
    )
}

pub(crate) fn reserved_cache_node_xml(node: &ReservedCacheNode) -> String {
    let recurring_charges_xml: String = node
        .recurring_charges
        .iter()
        .map(recurring_charge_xml)
        .collect();

    format!(
        "<ReservedCacheNode>\
         <ReservedCacheNodeId>{}</ReservedCacheNodeId>\
         <ReservedCacheNodesOfferingId>{}</ReservedCacheNodesOfferingId>\
         <CacheNodeType>{}</CacheNodeType>\
         <StartTime>{}</StartTime>\
         <Duration>{}</Duration>\
         <FixedPrice>{}</FixedPrice>\
         <UsagePrice>{}</UsagePrice>\
         <CacheNodeCount>{}</CacheNodeCount>\
         <ProductDescription>{}</ProductDescription>\
         <OfferingType>{}</OfferingType>\
         <State>{}</State>\
         <RecurringCharges>{}</RecurringCharges>\
         <ReservationARN>{}</ReservationARN>\
         </ReservedCacheNode>",
        xml_escape(&node.reserved_cache_node_id),
        xml_escape(&node.reserved_cache_nodes_offering_id),
        xml_escape(&node.cache_node_type),
        xml_escape(&node.start_time),
        node.duration,
        node.fixed_price,
        node.usage_price,
        node.cache_node_count,
        xml_escape(&node.product_description),
        xml_escape(&node.offering_type),
        xml_escape(&node.state),
        recurring_charges_xml,
        xml_escape(&node.reservation_arn),
    )
}

pub(crate) fn reserved_cache_nodes_offering_xml(offering: &ReservedCacheNodesOffering) -> String {
    let recurring_charges_xml: String = offering
        .recurring_charges
        .iter()
        .map(recurring_charge_xml)
        .collect();

    format!(
        "<ReservedCacheNodesOffering>\
         <ReservedCacheNodesOfferingId>{}</ReservedCacheNodesOfferingId>\
         <CacheNodeType>{}</CacheNodeType>\
         <Duration>{}</Duration>\
         <FixedPrice>{}</FixedPrice>\
         <UsagePrice>{}</UsagePrice>\
         <ProductDescription>{}</ProductDescription>\
         <OfferingType>{}</OfferingType>\
         <RecurringCharges>{}</RecurringCharges>\
         </ReservedCacheNodesOffering>",
        xml_escape(&offering.reserved_cache_nodes_offering_id),
        xml_escape(&offering.cache_node_type),
        offering.duration,
        offering.fixed_price,
        offering.usage_price,
        xml_escape(&offering.product_description),
        xml_escape(&offering.offering_type),
        recurring_charges_xml,
    )
}

pub(crate) fn cache_parameter_group_xml(g: &CacheParameterGroup) -> String {
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

pub(crate) fn cache_subnet_group_xml(g: &CacheSubnetGroup, region: &str) -> String {
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

pub(crate) fn cache_cluster_xml(cluster: &CacheCluster, show_cache_node_info: bool) -> String {
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

    let cache_parameter_group_xml = match &cluster.cache_parameter_group_name {
        Some(name) => format!(
            "<CacheParameterGroup>\
             <CacheParameterGroupName>{}</CacheParameterGroupName>\
             <ParameterApplyStatus>in-sync</ParameterApplyStatus>\
             </CacheParameterGroup>",
            xml_escape(name)
        ),
        None => String::new(),
    };
    // SecurityGroups is a list of SecurityGroupMembership keyed on the
    // generic `member` wrapper element — not `<SecurityGroupMembership>`
    // — per the Smithy `SecurityGroupMembershipList$member` shape. The
    // SDK XML deserializer drops anything else, so each entry must be
    // wrapped in `<member>`.
    let security_groups_xml = if cluster.security_group_ids.is_empty() {
        "<SecurityGroups/>".to_string()
    } else {
        // SecurityGroupMembershipList's member has no xmlName trait, so
        // awsQuery serializes each entry under the default `<member>`
        // tag — not `<SecurityGroupMembership>`. Real AWS responses use
        // `<member>`, and the AWS SDK Rust deserializer treats anything
        // else as an unknown element and drops the entry, leaving
        // `cluster.security_groups()` empty on the client side.
        format!(
            "<SecurityGroups>{}</SecurityGroups>",
            cluster
                .security_group_ids
                .iter()
                .map(|sg| format!(
                    "<member>\
                     <SecurityGroupId>{}</SecurityGroupId>\
                     <Status>active</Status>\
                     </member>",
                    xml_escape(sg)
                ))
                .collect::<String>()
        )
    };
    let log_delivery_configurations_xml = if cluster.log_delivery_configurations.is_empty() {
        "<LogDeliveryConfigurations/>".to_string()
    } else {
        let entries: String = cluster
            .log_delivery_configurations
            .iter()
            .map(log_delivery_configuration_xml)
            .collect();
        format!("<LogDeliveryConfigurations>{entries}</LogDeliveryConfigurations>")
    };
    let configuration_endpoint_xml = if (cluster.replication_group_id.is_some()
        || cluster.engine == "memcached")
        && !cluster.endpoint_address.is_empty()
    {
        format!(
            "<ConfigurationEndpoint><Address>{}</Address><Port>{}</Port></ConfigurationEndpoint>",
            xml_escape(&cluster.endpoint_address),
            effective_endpoint_port(cluster.endpoint_port, cluster.port, &cluster.engine)
        )
    } else {
        String::new()
    };

    let preferred_maintenance_window_xml = cluster
        .preferred_maintenance_window
        .as_ref()
        .map(|w| {
            format!(
                "<PreferredMaintenanceWindow>{}</PreferredMaintenanceWindow>",
                xml_escape(w)
            )
        })
        .unwrap_or_default();
    let preferred_outpost_arn_xml = cluster
        .preferred_outpost_arn
        .as_ref()
        .map(|a| {
            format!(
                "<PreferredOutpostArn>{}</PreferredOutpostArn>",
                xml_escape(a)
            )
        })
        .unwrap_or_default();
    let outpost_mode_xml = cluster
        .outpost_mode
        .as_ref()
        .map(|m| format!("<OutpostMode>{}</OutpostMode>", xml_escape(m)))
        .unwrap_or_default();
    let network_type_xml = cluster
        .network_type
        .as_ref()
        .map(|n| format!("<NetworkType>{}</NetworkType>", xml_escape(n)))
        .unwrap_or_default();
    let ip_discovery_xml = cluster
        .ip_discovery
        .as_ref()
        .map(|n| format!("<IpDiscovery>{}</IpDiscovery>", xml_escape(n)))
        .unwrap_or_default();
    let transit_encryption_mode_xml = cluster
        .transit_encryption_mode
        .as_ref()
        .map(|m| {
            format!(
                "<TransitEncryptionMode>{}</TransitEncryptionMode>",
                xml_escape(m)
            )
        })
        .unwrap_or_default();
    let notification_topic_xml = cluster
        .notification_topic_arn
        .as_ref()
        .map(|t| {
            format!(
                "<NotificationConfiguration><TopicArn>{}</TopicArn><TopicStatus>active</TopicStatus></NotificationConfiguration>",
                xml_escape(t)
            )
        })
        .unwrap_or_default();
    let snapshot_window_xml = cluster
        .snapshot_window
        .as_ref()
        .map(|w| format!("<SnapshotWindow>{}</SnapshotWindow>", xml_escape(w)))
        .unwrap_or_default();
    let snapshot_retention_limit_xml = format!(
        "<SnapshotRetentionLimit>{}</SnapshotRetentionLimit>",
        cluster.snapshot_retention_limit
    );
    let preferred_azs_xml = if cluster.preferred_availability_zones.is_empty() {
        String::new()
    } else {
        format!(
            "<PreferredAvailabilityZones>{}</PreferredAvailabilityZones>",
            cluster
                .preferred_availability_zones
                .iter()
                .map(|az| format!("<AvailabilityZone>{}</AvailabilityZone>", xml_escape(az)))
                .collect::<String>()
        )
    };
    // Defaults to single-az even if not set; emitted only when known so
    // older snapshots without the field don't gain a fake value.
    let az_mode_xml = cluster
        .az_mode
        .as_ref()
        .map(|m| format!("<AZMode>{}</AZMode>", xml_escape(m)))
        .unwrap_or_default();
    let cache_security_groups_xml = if cluster.cache_security_group_names.is_empty() {
        String::new()
    } else {
        format!(
            "<CacheSecurityGroups>{}</CacheSecurityGroups>",
            cluster
                .cache_security_group_names
                .iter()
                .map(|n| format!(
                    "<CacheSecurityGroup>\
                     <CacheSecurityGroupName>{}</CacheSecurityGroupName>\
                     <Status>active</Status>\
                     </CacheSecurityGroup>",
                    xml_escape(n)
                ))
                .collect::<String>()
        )
    };

    format!(
        "<CacheClusterId>{}</CacheClusterId>\
         <CacheNodeType>{}</CacheNodeType>\
         <Engine>{}</Engine>\
         <EngineVersion>{}</EngineVersion>\
         <CacheClusterStatus>{}</CacheClusterStatus>\
         <NumCacheNodes>{}</NumCacheNodes>\
         <PreferredAvailabilityZone>{}</PreferredAvailabilityZone>\
         {preferred_azs_xml}\
         {az_mode_xml}\
         <CacheClusterCreateTime>{}</CacheClusterCreateTime>\
         {preferred_maintenance_window_xml}\
         {cache_subnet_group_name_xml}\
         {cache_nodes_xml}\
         {cache_parameter_group_xml}\
         {cache_security_groups_xml}\
         {security_groups_xml}\
         {log_delivery_configurations_xml}\
         {configuration_endpoint_xml}\
         <ClientDownloadLandingPage></ClientDownloadLandingPage>\
         {notification_topic_xml}\
         {snapshot_retention_limit_xml}\
         {snapshot_window_xml}\
         {outpost_mode_xml}\
         {preferred_outpost_arn_xml}\
         {network_type_xml}\
         {ip_discovery_xml}\
         {transit_encryption_mode_xml}\
         <TransitEncryptionEnabled>{}</TransitEncryptionEnabled>\
         <AtRestEncryptionEnabled>{}</AtRestEncryptionEnabled>\
         <AuthTokenEnabled>{}</AuthTokenEnabled>\
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
        cluster.transit_encryption_enabled,
        cluster.at_rest_encryption_enabled,
        cluster.auth_token_enabled,
        cluster.auto_minor_version_upgrade,
        xml_escape(&cluster.arn),
    )
}

pub(crate) fn cache_node_xml(cluster: &CacheCluster, node_id: usize) -> String {
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
        effective_endpoint_port(cluster.endpoint_port, cluster.port, &cluster.engine),
        xml_escape(&cluster.preferred_availability_zone),
    )
}

pub(crate) fn replication_group_xml(g: &ReplicationGroup, region: &str) -> String {
    let member_clusters_xml: String = g
        .member_clusters
        .iter()
        .map(|c| format!("<ClusterId>{}</ClusterId>", xml_escape(c)))
        .collect();
    let global_replication_group_info_xml = g
        .global_replication_group_id
        .as_ref()
        .map(|global_replication_group_id| {
            format!(
                "<GlobalReplicationGroupInfo>\
                 <GlobalReplicationGroupId>{}</GlobalReplicationGroupId>\
                 <GlobalReplicationGroupMemberRole>{}</GlobalReplicationGroupMemberRole>\
                 </GlobalReplicationGroupInfo>",
                xml_escape(global_replication_group_id),
                xml_escape(
                    g.global_replication_group_role
                        .as_deref()
                        .unwrap_or("primary")
                ),
            )
        })
        .unwrap_or_default();

    let primary_az = format!("{region}a");
    let kms_xml = g
        .kms_key_id
        .as_ref()
        .map(|k| format!("<KmsKeyId>{}</KmsKeyId>", xml_escape(k)))
        .unwrap_or_default();
    let user_groups_xml = if g.user_group_ids.is_empty() {
        "<UserGroupIds/>".to_string()
    } else {
        format!(
            "<UserGroupIds>{}</UserGroupIds>",
            g.user_group_ids
                .iter()
                .map(|u| format!("<member>{}</member>", xml_escape(u)))
                .collect::<String>()
        )
    };
    let log_delivery_xml = if g.log_delivery_configurations.is_empty() {
        "<LogDeliveryConfigurations/>".to_string()
    } else {
        let entries: String = g
            .log_delivery_configurations
            .iter()
            .map(log_delivery_configuration_xml)
            .collect();
        format!("<LogDeliveryConfigurations>{entries}</LogDeliveryConfigurations>")
    };
    let data_tiering_xml = g
        .data_tiering
        .as_ref()
        .map(|d| format!("<DataTiering>{}</DataTiering>", xml_escape(d)))
        .unwrap_or_default();
    let ip_discovery_xml = g
        .ip_discovery
        .as_ref()
        .map(|v| format!("<IpDiscovery>{}</IpDiscovery>", xml_escape(v)))
        .unwrap_or_default();
    let network_type_xml = g
        .network_type
        .as_ref()
        .map(|v| format!("<NetworkType>{}</NetworkType>", xml_escape(v)))
        .unwrap_or_default();
    let transit_encryption_mode_xml = g
        .transit_encryption_mode
        .as_ref()
        .map(|v| {
            format!(
                "<TransitEncryptionMode>{}</TransitEncryptionMode>",
                xml_escape(v)
            )
        })
        .unwrap_or_default();
    let configuration_endpoint_xml = match (
        g.configuration_endpoint_address.as_ref(),
        g.configuration_endpoint_port,
    ) {
        (Some(addr), Some(port)) => format!(
            "<ConfigurationEndpoint><Address>{}</Address><Port>{}</Port></ConfigurationEndpoint>",
            xml_escape(addr),
            effective_endpoint_port(port, 0, &g.engine)
        ),
        _ => String::new(),
    };
    let replication_group_create_time_xml = format!(
        "<ReplicationGroupCreateTime>{}</ReplicationGroupCreateTime>",
        xml_escape(&g.created_at)
    );
    let notification_topic_xml = g
        .notification_topic_arn
        .as_ref()
        .map(|t| {
            let status = g
                .notification_topic_status
                .as_deref()
                .unwrap_or("active");
            format!(
                "<NotificationConfiguration><TopicArn>{}</TopicArn><TopicStatus>{}</TopicStatus></NotificationConfiguration>",
                xml_escape(t),
                xml_escape(status),
            )
        })
        .unwrap_or_default();
    let cluster_mode_xml = g
        .cluster_mode
        .as_ref()
        .map(|m| format!("<ClusterMode>{}</ClusterMode>", xml_escape(m)))
        .unwrap_or_default();
    let cache_parameter_group_xml = g
        .cache_parameter_group_name
        .as_ref()
        .map(|n| {
            format!(
                "<CacheParameterGroup><CacheParameterGroupName>{}</CacheParameterGroupName></CacheParameterGroup>",
                xml_escape(n)
            )
        })
        .unwrap_or_default();
    let preferred_maintenance_window_xml = g
        .preferred_maintenance_window
        .as_ref()
        .map(|w| {
            format!(
                "<PreferredMaintenanceWindow>{}</PreferredMaintenanceWindow>",
                xml_escape(w)
            )
        })
        .unwrap_or_default();

    let id = xml_escape(&g.replication_group_id);
    let description = xml_escape(&g.description);
    let status = xml_escape(&g.status);
    let endpoint_address = xml_escape(&g.endpoint_address);
    let endpoint_port = effective_endpoint_port(g.endpoint_port, 0, &g.engine);
    let primary_az_xml = xml_escape(&primary_az);
    let automatic_failover = if g.automatic_failover_enabled {
        "enabled"
    } else {
        "disabled"
    };
    let multi_az = if g.multi_az_enabled {
        "enabled"
    } else {
        "disabled"
    };
    let snapshot_retention = g.snapshot_retention_limit;
    let snapshot_window = xml_escape(&g.snapshot_window);
    let cache_node_type = xml_escape(&g.cache_node_type);
    let cluster_enabled = if g.cluster_enabled { "true" } else { "false" };
    let auth_token_enabled = if g.auth_token_enabled {
        "true"
    } else {
        "false"
    };
    let transit_enc = if g.transit_encryption_enabled {
        "true"
    } else {
        "false"
    };
    let at_rest_enc = if g.at_rest_encryption_enabled {
        "true"
    } else {
        "false"
    };
    let auto_minor_version_upgrade = if g.auto_minor_version_upgrade {
        "true"
    } else {
        "false"
    };
    let engine = xml_escape(&g.engine);
    let arn = xml_escape(&g.arn);

    // Emit one NodeGroup per shard. AWS numbers them 0001..N in
    // padded form regardless of cluster_enabled. The same primary
    // endpoint is reused since fakecloud runs a single backing
    // container per replication group. Clamp at AWS's documented
    // 500-shard ceiling so a corrupt stored value can't cause an
    // unbounded XML allocation here.
    const MAX_SHARDS: i32 = 500;
    // AWS caps a Redis/Valkey node group at 5 read replicas. Clamp so a
    // corrupt stored value can't drive an unbounded member allocation.
    const MAX_REPLICAS_PER_SHARD: i32 = 5;
    let shard_count = g.num_node_groups.clamp(1, MAX_SHARDS);
    // Emit one primary NodeGroupMember plus one member per configured replica.
    // The Terraform provider derives `replicas_per_node_group` from
    // `len(NodeGroupMembers) - 1`; emitting only the primary reported 0
    // replicas and produced a perpetual plan diff. bug-hunt 2026-07-16, 1.8.
    let replicas_per_shard = current_replicas_per_shard(g).clamp(0, MAX_REPLICAS_PER_SHARD);
    let members_per_shard = replicas_per_shard + 1;
    let node_groups_inner: String = (1..=shard_count)
        .map(|shard| {
            // Members for this shard: 1 primary + N replicas. AWS names the
            // per-node clusters `<rg>-001` (primary), `-002..` (replicas);
            // reuse the stored member_clusters slice when it lines up so
            // multi-shard describe responses round-trip the requested shape.
            let base = ((shard - 1) * members_per_shard) as usize;
            let members_xml: String = (0..members_per_shard)
                .map(|i| {
                    let global_idx = base + i as usize;
                    let cluster_id =
                        g.member_clusters
                            .get(global_idx)
                            .cloned()
                            .unwrap_or_else(|| {
                                format!("{}-{:03}", g.replication_group_id, global_idx + 1)
                            });
                    let role = if i == 0 { "primary" } else { "replica" };
                    format!(
                        "<NodeGroupMember>\
                         <CacheClusterId>{cluster_id}</CacheClusterId>\
                         <CacheNodeId>0001</CacheNodeId>\
                         <PreferredAvailabilityZone>{primary_az_xml}</PreferredAvailabilityZone>\
                         <CurrentRole>{role}</CurrentRole>\
                         </NodeGroupMember>",
                        cluster_id = xml_escape(&cluster_id),
                    )
                })
                .collect();
            format!(
                "<NodeGroup>\
                 <NodeGroupId>{shard:04}</NodeGroupId>\
                 <Status>available</Status>\
                 <PrimaryEndpoint>\
                 <Address>{endpoint_address}</Address>\
                 <Port>{endpoint_port}</Port>\
                 </PrimaryEndpoint>\
                 <ReaderEndpoint>\
                 <Address>{endpoint_address}</Address>\
                 <Port>{endpoint_port}</Port>\
                 </ReaderEndpoint>\
                 <NodeGroupMembers>{members_xml}</NodeGroupMembers>\
                 </NodeGroup>",
            )
        })
        .collect();

    format!(
        "<ReplicationGroupId>{id}</ReplicationGroupId>\
         <Description>{description}</Description>\
         {global_replication_group_info_xml}\
         <Status>{status}</Status>\
         {replication_group_create_time_xml}\
         <MemberClusters>{member_clusters_xml}</MemberClusters>\
         <NodeGroups>{node_groups_inner}</NodeGroups>\
         <AutomaticFailover>{automatic_failover}</AutomaticFailover>\
         <MultiAZ>{multi_az}</MultiAZ>\
         <SnapshotRetentionLimit>{snapshot_retention}</SnapshotRetentionLimit>\
         <SnapshotWindow>{snapshot_window}</SnapshotWindow>\
         <ClusterEnabled>{cluster_enabled}</ClusterEnabled>\
         <CacheNodeType>{cache_node_type}</CacheNodeType>\
         <AuthTokenEnabled>{auth_token_enabled}</AuthTokenEnabled>\
         <TransitEncryptionEnabled>{transit_enc}</TransitEncryptionEnabled>\
         <AtRestEncryptionEnabled>{at_rest_enc}</AtRestEncryptionEnabled>\
         <AutoMinorVersionUpgrade>{auto_minor_version_upgrade}</AutoMinorVersionUpgrade>\
         <Engine>{engine}</Engine>\
         {kms_xml}\
         {user_groups_xml}\
         {log_delivery_xml}\
         {data_tiering_xml}\
         {ip_discovery_xml}\
         {network_type_xml}\
         {transit_encryption_mode_xml}\
         {configuration_endpoint_xml}\
         {notification_topic_xml}\
         {cluster_mode_xml}\
         {cache_parameter_group_xml}\
         {preferred_maintenance_window_xml}\
         <PendingModifiedValues/>\
         <ARN>{arn}</ARN>",
    )
}

pub(crate) fn log_delivery_configuration_xml(c: &LogDeliveryConfiguration) -> String {
    let detail = c
        .destination_details
        .as_deref()
        .map(|d| {
            if c.destination_type == "cloudwatch-logs" {
                format!(
                    "<DestinationDetails><CloudWatchLogsDetails><LogGroup>{}</LogGroup></CloudWatchLogsDetails></DestinationDetails>",
                    xml_escape(d)
                )
            } else if c.destination_type == "kinesis-firehose" {
                format!(
                    "<DestinationDetails><KinesisFirehoseDetails><DeliveryStream>{}</DeliveryStream></KinesisFirehoseDetails></DestinationDetails>",
                    xml_escape(d)
                )
            } else {
                String::new()
            }
        })
        .unwrap_or_default();
    format!(
        "<LogDeliveryConfiguration><LogType>{}</LogType><DestinationType>{}</DestinationType>{detail}<LogFormat>{}</LogFormat><Status>{}</Status></LogDeliveryConfiguration>",
        xml_escape(&c.log_type),
        xml_escape(&c.destination_type),
        xml_escape(&c.log_format),
        xml_escape(&c.status),
    )
}

pub(crate) fn global_replication_group_id(region: &str, suffix: &str) -> String {
    format!("fc-{}-{}", region, suffix)
}

pub(crate) fn primary_global_member(
    group: &GlobalReplicationGroup,
) -> Option<&GlobalReplicationGroupMember> {
    group.members.iter().find(|member| member.role == "primary")
}

pub(crate) fn global_replication_group_xml(
    group: &GlobalReplicationGroup,
    show_member_info: bool,
) -> String {
    let members_xml = if show_member_info {
        let members_xml: String = group
            .members
            .iter()
            .map(global_replication_group_member_xml)
            .collect();
        format!("<Members>{members_xml}</Members>")
    } else {
        String::new()
    };
    let global_node_groups_xml = if group.cluster_enabled {
        let count = group.num_node_groups.max(1);
        // Partition the 16384-slot keyspace evenly across the node groups
        // so the rendered slot ranges reflect the actual shard count.
        let total_slots = 16384i32;
        let mut members = String::new();
        let mut next_slot = 0i32;
        for i in 0..count {
            let remaining = count - i;
            let chunk = (total_slots - next_slot) / remaining;
            let end = next_slot + chunk - 1;
            members.push_str(&format!(
                "<GlobalNodeGroup><GlobalNodeGroupId>{:04}</GlobalNodeGroupId><Slots>{}-{}</Slots></GlobalNodeGroup>",
                i + 1,
                next_slot,
                end
            ));
            next_slot = end + 1;
        }
        format!("<GlobalNodeGroups>{members}</GlobalNodeGroups>")
    } else {
        String::new()
    };

    format!(
        "<GlobalReplicationGroupId>{}</GlobalReplicationGroupId>\
         <GlobalReplicationGroupDescription>{}</GlobalReplicationGroupDescription>\
         <Status>{}</Status>\
         <CacheNodeType>{}</CacheNodeType>\
         <Engine>{}</Engine>\
         <EngineVersion>{}</EngineVersion>\
         {members_xml}\
         <ClusterEnabled>{}</ClusterEnabled>\
         {global_node_groups_xml}\
         <AuthTokenEnabled>false</AuthTokenEnabled>\
         <TransitEncryptionEnabled>false</TransitEncryptionEnabled>\
         <AtRestEncryptionEnabled>false</AtRestEncryptionEnabled>\
         <ARN>{}</ARN>",
        xml_escape(&group.global_replication_group_id),
        xml_escape(&group.global_replication_group_description),
        xml_escape(&group.status),
        xml_escape(&group.cache_node_type),
        xml_escape(&group.engine),
        xml_escape(&group.engine_version),
        group.cluster_enabled,
        xml_escape(&group.arn),
    )
}

pub(crate) fn global_replication_group_member_xml(member: &GlobalReplicationGroupMember) -> String {
    format!(
        "<GlobalReplicationGroupMember>\
         <ReplicationGroupId>{}</ReplicationGroupId>\
         <ReplicationGroupRegion>{}</ReplicationGroupRegion>\
         <Role>{}</Role>\
         <AutomaticFailover>{}</AutomaticFailover>\
         <Status>{}</Status>\
         </GlobalReplicationGroupMember>",
        xml_escape(&member.replication_group_id),
        xml_escape(&member.replication_group_region),
        xml_escape(&member.role),
        if member.automatic_failover {
            "enabled"
        } else {
            "disabled"
        },
        xml_escape(&member.status),
    )
}

pub(crate) fn user_xml(u: &ElastiCacheUser) -> String {
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

pub(crate) fn user_group_xml(g: &ElastiCacheUserGroup) -> String {
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

pub(crate) fn add_cluster_to_replication_group(
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

pub(crate) fn remove_cluster_from_replication_group(
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

pub(crate) fn snapshot_xml(s: &CacheSnapshot) -> String {
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

pub(crate) fn serverless_cache_xml(cache: &ServerlessCache) -> String {
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

pub(crate) fn serverless_cache_usage_limits_xml(limits: &ServerlessCacheUsageLimits) -> String {
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

pub(crate) fn serverless_cache_endpoint_xml(endpoint: &ServerlessCacheEndpoint) -> String {
    format!(
        "<Address>{}</Address><Port>{}</Port>",
        xml_escape(&endpoint.address),
        endpoint.port,
    )
}

pub(crate) fn serverless_cache_snapshot_xml(snapshot: &ServerlessCacheSnapshot) -> String {
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

pub(crate) fn parameter_xml(p: &EngineDefaultParameter) -> String {
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
mod cluster_xml_tests {
    use super::*;
    use crate::state::{CacheCluster, LogDeliveryConfiguration};

    fn fixture() -> CacheCluster {
        CacheCluster {
            cache_cluster_id: "c1".into(),
            cache_node_type: "cache.t3.micro".into(),
            engine: "redis".into(),
            engine_version: "7.1".into(),
            cache_cluster_status: "available".into(),
            num_cache_nodes: 1,
            preferred_availability_zone: "us-east-1a".into(),
            cache_subnet_group_name: None,
            auto_minor_version_upgrade: true,
            arn: "arn:aws:elasticache:us-east-1:000000000000:cluster:c1".into(),
            created_at: "2026-05-02T00:00:00Z".into(),
            endpoint_address: "127.0.0.1".into(),
            endpoint_port: 6379,
            container_id: String::new(),
            host_port: 6379,
            replication_group_id: None,
            cache_parameter_group_name: None,
            security_group_ids: Vec::new(),
            log_delivery_configurations: Vec::new(),
            transit_encryption_enabled: false,
            at_rest_encryption_enabled: false,
            auth_token_enabled: false,
            port: 6379,
            preferred_maintenance_window: None,
            preferred_availability_zones: Vec::new(),
            notification_topic_arn: None,
            cache_security_group_names: Vec::new(),
            snapshot_arns: Vec::new(),
            snapshot_name: None,
            snapshot_retention_limit: 0,
            snapshot_window: None,
            outpost_mode: None,
            preferred_outpost_arn: None,
            network_type: None,
            ip_discovery: None,
            az_mode: None,
            auth_token: None,
            kms_key_id: None,
            transit_encryption_mode: None,
            data_tiering_enabled: None,
            cluster_mode: None,
            preferred_outpost_arns: Vec::new(),
        }
    }

    #[test]
    fn defaults_emit_canonical_flags() {
        let c = fixture();
        let xml = cache_cluster_xml(&c, false);
        assert!(xml.contains("<TransitEncryptionEnabled>false</TransitEncryptionEnabled>"));
        assert!(xml.contains("<AtRestEncryptionEnabled>false</AtRestEncryptionEnabled>"));
        assert!(xml.contains("<AuthTokenEnabled>false</AuthTokenEnabled>"));
        // No parameter group / security groups / log destinations set.
        assert!(!xml.contains("<CacheParameterGroup>"));
        assert!(xml.contains("<SecurityGroups/>"));
        assert!(xml.contains("<LogDeliveryConfigurations/>"));
        // No replication group => no ConfigurationEndpoint emitted.
        assert!(!xml.contains("<ConfigurationEndpoint>"));
    }

    #[test]
    fn populated_fields_round_trip() {
        let mut c = fixture();
        c.cache_parameter_group_name = Some("default.redis7".into());
        c.security_group_ids = vec!["sg-abc".into(), "sg-def".into()];
        c.transit_encryption_enabled = true;
        c.at_rest_encryption_enabled = true;
        c.auth_token_enabled = true;
        c.log_delivery_configurations = vec![LogDeliveryConfiguration {
            log_type: "slow-log".into(),
            destination_type: "cloudwatch-logs".into(),
            destination_details: Some("my-log-group".into()),
            log_format: "json".into(),
            status: "active".into(),
        }];
        c.replication_group_id = Some("rg1".into());
        let xml = cache_cluster_xml(&c, false);
        assert!(xml.contains("<CacheParameterGroupName>default.redis7</CacheParameterGroupName>"));
        assert!(xml.contains("<SecurityGroupId>sg-abc</SecurityGroupId>"));
        assert!(xml.contains("<SecurityGroupId>sg-def</SecurityGroupId>"));
        assert!(xml.contains("<TransitEncryptionEnabled>true</TransitEncryptionEnabled>"));
        assert!(xml.contains("<AtRestEncryptionEnabled>true</AtRestEncryptionEnabled>"));
        assert!(xml.contains("<AuthTokenEnabled>true</AuthTokenEnabled>"));
        assert!(xml.contains("<LogType>slow-log</LogType>"));
        assert!(xml.contains("<ConfigurationEndpoint>"));
    }

    #[test]
    fn no_runtime_port_falls_back_to_configured_then_engine_default() {
        // No container runtime => endpoint_port stays 0. The node endpoint
        // must still emit the configured Port (1.4, 2026-06-20) instead of 0.
        let mut c = fixture();
        c.endpoint_port = 0;
        c.host_port = 0;
        c.port = 6380;
        let xml = cache_cluster_xml(&c, true);
        assert!(xml.contains("<Port>6380</Port>"));
        assert!(!xml.contains("<Port>0</Port>"));

        // No configured port either => fall back to the engine default.
        c.port = 0;
        let xml = cache_cluster_xml(&c, true);
        assert!(xml.contains("<Port>6379</Port>"));

        // memcached emits its ConfigurationEndpoint on 11211 by default.
        c.engine = "memcached".into();
        c.port = 0;
        let xml = cache_cluster_xml(&c, false);
        assert!(xml.contains("<ConfigurationEndpoint>"));
        assert!(xml.contains("<Port>11211</Port>"));
        assert!(!xml.contains("<Port>0</Port>"));
    }

    #[test]
    fn engine_default_port_maps_engines() {
        assert_eq!(engine_default_port("redis"), 6379);
        assert_eq!(engine_default_port("valkey"), 6379);
        assert_eq!(engine_default_port("Memcached"), 11211);
    }
}

#[cfg(test)]
mod replication_group_xml_tests {
    use super::*;
    use crate::state::{LogDeliveryConfiguration, ReplicationGroup};

    fn fixture() -> ReplicationGroup {
        ReplicationGroup {
            replication_group_id: "rg1".into(),
            description: "fixture".into(),
            global_replication_group_id: None,
            global_replication_group_role: None,
            status: "available".into(),
            cache_node_type: "cache.t3.micro".into(),
            engine: "redis".into(),
            engine_version: "7.1".into(),
            num_cache_clusters: 1,
            automatic_failover_enabled: false,
            endpoint_address: "127.0.0.1".into(),
            endpoint_port: 6379,
            arn: "arn:aws:elasticache:us-east-1:000000000000:replicationgroup:rg1".into(),
            created_at: "2026-05-02T00:00:00Z".into(),
            container_id: String::new(),
            host_port: 6379,
            member_clusters: vec!["rg1-001".into()],
            snapshot_retention_limit: 0,
            snapshot_window: "05:00-09:00".into(),
            transit_encryption_enabled: false,
            at_rest_encryption_enabled: false,
            cluster_enabled: false,
            kms_key_id: None,
            auth_token_enabled: false,
            user_group_ids: Vec::new(),
            multi_az_enabled: false,
            log_delivery_configurations: Vec::new(),
            data_tiering: None,
            ip_discovery: None,
            network_type: None,
            transit_encryption_mode: None,
            num_node_groups: 1,
            configuration_endpoint_address: None,
            configuration_endpoint_port: None,
            replicas_per_node_group: None,
            auth_token: None,
            port: 6379,
            notification_topic_arn: None,
            cluster_mode: None,
            data_tiering_enabled: None,
            notification_topic_status: None,
            cache_parameter_group_name: None,
            cache_subnet_group_name: None,
            security_group_ids: Vec::new(),
            preferred_maintenance_window: None,
            snapshot_name: None,
            snapshot_arns: Vec::new(),
            auto_minor_version_upgrade: true,
        }
    }

    #[test]
    fn modify_kitchen_sink_fields_appear_in_xml() {
        let mut g = fixture();
        g.transit_encryption_enabled = true;
        g.transit_encryption_mode = Some("required".into());
        g.at_rest_encryption_enabled = true;
        g.kms_key_id = Some("alias/k".into());
        g.multi_az_enabled = true;
        g.automatic_failover_enabled = true;
        g.user_group_ids = vec!["ug-a".into(), "ug-b".into()];
        g.log_delivery_configurations = vec![LogDeliveryConfiguration {
            log_type: "slow-log".into(),
            destination_type: "cloudwatch-logs".into(),
            destination_details: Some("/aws/elasticache/x".into()),
            log_format: "json".into(),
            status: "active".into(),
        }];
        g.ip_discovery = Some("ipv6".into());
        g.network_type = Some("dual_stack".into());
        g.cluster_mode = Some("compatible".into());
        g.cluster_enabled = true;
        g.notification_topic_arn = Some("arn:aws:sns:us-east-1:000:t".into());
        g.notification_topic_status = Some("active".into());
        g.cache_parameter_group_name = Some("default.redis7".into());
        g.preferred_maintenance_window = Some("mon:02:00-mon:03:00".into());
        g.auto_minor_version_upgrade = false;

        let xml = replication_group_xml(&g, "us-east-1");
        assert!(xml.contains("<TransitEncryptionEnabled>true</TransitEncryptionEnabled>"));
        assert!(xml.contains("<TransitEncryptionMode>required</TransitEncryptionMode>"));
        assert!(xml.contains("<AtRestEncryptionEnabled>true</AtRestEncryptionEnabled>"));
        assert!(xml.contains("<KmsKeyId>alias/k</KmsKeyId>"));
        assert!(xml.contains("<MultiAZ>enabled</MultiAZ>"));
        assert!(xml.contains("<AutomaticFailover>enabled</AutomaticFailover>"));
        assert!(xml.contains("<member>ug-a</member>"));
        assert!(xml.contains("<member>ug-b</member>"));
        assert!(xml.contains("<LogType>slow-log</LogType>"));
        assert!(xml.contains("<IpDiscovery>ipv6</IpDiscovery>"));
        assert!(xml.contains("<NetworkType>dual_stack</NetworkType>"));
        assert!(xml.contains("<ClusterMode>compatible</ClusterMode>"));
        assert!(xml.contains("<ClusterEnabled>true</ClusterEnabled>"));
        assert!(xml.contains("<TopicArn>arn:aws:sns:us-east-1:000:t</TopicArn>"));
        assert!(xml.contains("<TopicStatus>active</TopicStatus>"));
        assert!(xml.contains("<CacheParameterGroupName>default.redis7</CacheParameterGroupName>"));
        assert!(xml.contains(
            "<PreferredMaintenanceWindow>mon:02:00-mon:03:00</PreferredMaintenanceWindow>"
        ));
        assert!(xml.contains("<AutoMinorVersionUpgrade>false</AutoMinorVersionUpgrade>"));
    }

    #[test]
    fn defaults_emit_canonical_state() {
        let g = fixture();
        let xml = replication_group_xml(&g, "us-east-1");
        // Defaults: AMVU true, encryption flags false, no optional sections.
        assert!(xml.contains("<AutoMinorVersionUpgrade>true</AutoMinorVersionUpgrade>"));
        assert!(xml.contains("<TransitEncryptionEnabled>false</TransitEncryptionEnabled>"));
        assert!(xml.contains("<AtRestEncryptionEnabled>false</AtRestEncryptionEnabled>"));
        assert!(xml.contains("<MultiAZ>disabled</MultiAZ>"));
        assert!(xml.contains("<AutomaticFailover>disabled</AutomaticFailover>"));
        assert!(xml.contains("<UserGroupIds/>"));
        assert!(xml.contains("<LogDeliveryConfigurations/>"));
        // No CacheParameterGroup / NotificationConfiguration when unset.
        assert!(!xml.contains("<CacheParameterGroup>"));
        assert!(!xml.contains("<NotificationConfiguration>"));
        assert!(!xml.contains("<PreferredMaintenanceWindow>"));
    }
}
