use super::*;

/// Construct the canonical `InvalidParameterValue` error used across the
/// query-protocol RDS surface. Centralized so probes that hit any
/// validation gate see a uniform wire shape.
pub(crate) fn invalid_param(message: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "InvalidParameterValue", message)
}

/// Construct the `MissingParameter` error AWS RDS returns when a
/// `@required` query parameter is absent. Pre-validation in
/// [`crate::validation`] calls this so handlers downstream can assume
/// required values are present.
pub(crate) fn missing_param(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "MissingParameter",
        format!("The request must contain the parameter {name}."),
    )
}

pub(crate) fn is_mutating_action(action: &str) -> bool {
    if matches!(
        action,
        "AddTagsToResource"
            | "CreateDBInstance"
            | "CreateDBInstanceReadReplica"
            | "CreateDBParameterGroup"
            | "CreateDBSnapshot"
            | "CreateDBSubnetGroup"
            | "DeleteDBInstance"
            | "DeleteDBParameterGroup"
            | "DeleteDBSnapshot"
            | "DeleteDBSubnetGroup"
            | "ModifyDBInstance"
            | "ModifyDBParameterGroup"
            | "ModifyDBSubnetGroup"
            | "RebootDBInstance"
            | "RemoveTagsFromResource"
            | "RestoreDBInstanceFromDBSnapshot"
    ) {
        return true;
    }
    // Heuristic for the 140 extra ops: any verb that mutates state.
    let mutating_prefixes = [
        "Create",
        "Modify",
        "Delete",
        "Reboot",
        "Start",
        "Stop",
        "Failover",
        "Switchover",
        "Promote",
        "Reset",
        "Apply",
        "Authorize",
        "Revoke",
        "Add",
        "Remove",
        "Register",
        "Deregister",
        "Copy",
        "Restore",
        "Backtrack",
        "Cancel",
        "Purchase",
        "Disable",
        "Enable",
    ];
    mutating_prefixes.iter().any(|p| action.starts_with(p))
}

pub(crate) fn optional_i32_param(
    req: &AwsRequest,
    name: &str,
) -> Result<Option<i32>, AwsServiceError> {
    optional_query_param(req, name)
        .map(|value| {
            value.parse::<i32>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("Parameter {name} must be a valid integer."),
                )
            })
        })
        .transpose()
}

/// AWS RDS encodes list members under two wire forms depending on client
/// version: the canonical Smithy `xmlName` form
/// (e.g. `Tags.Tag.N.Key`, `SubnetIds.SubnetIdentifier.N`,
/// `VpcSecurityGroupIds.VpcSecurityGroupId.N`) and the generic
/// `<List>.member.N` form some SDKs and the conformance probe emit.
/// Accept both — real AWS does too.
pub(crate) fn parse_tags(req: &AwsRequest) -> Result<Vec<RdsTag>, AwsServiceError> {
    for member_name in ["Tag", "member"] {
        let mut tags = Vec::new();
        for index in 1.. {
            let key_name = format!("Tags.{member_name}.{index}.Key");
            let value_name = format!("Tags.{member_name}.{index}.Value");
            let key = optional_query_param(req, &key_name);
            let value = optional_query_param(req, &value_name);

            match (key, value) {
                (Some(key), Some(value)) => tags.push(RdsTag { key, value }),
                (Some(key), None) => tags.push(RdsTag {
                    key,
                    value: String::new(),
                }),
                (None, None) => break,
                (None, Some(_)) => break,
            }
        }
        if !tags.is_empty() {
            return Ok(tags);
        }
    }
    Ok(Vec::new())
}

pub(crate) fn parse_tag_keys(req: &AwsRequest) -> Result<Vec<String>, AwsServiceError> {
    for member_name in ["member", "TagKey"] {
        let mut keys = Vec::new();
        for index in 1.. {
            let key_name = format!("TagKeys.{member_name}.{index}");
            match optional_query_param(req, &key_name) {
                Some(key) => keys.push(key),
                None => break,
            }
        }
        if !keys.is_empty() {
            return Ok(keys);
        }
    }
    Ok(Vec::new())
}

pub(crate) fn parse_subnet_ids(req: &AwsRequest) -> Result<Vec<String>, AwsServiceError> {
    for member_name in ["SubnetIdentifier", "member"] {
        let mut subnet_ids = Vec::new();
        for index in 1.. {
            let subnet_id_name = format!("SubnetIds.{member_name}.{index}");
            match optional_query_param(req, &subnet_id_name) {
                Some(subnet_id) => subnet_ids.push(subnet_id),
                None => break,
            }
        }
        if !subnet_ids.is_empty() {
            return Ok(subnet_ids);
        }
    }
    Ok(Vec::new())
}

pub(crate) fn parse_vpc_security_group_ids(req: &AwsRequest) -> Vec<String> {
    for member_name in ["VpcSecurityGroupId", "member"] {
        let mut security_group_ids = Vec::new();
        for index in 1.. {
            let sg_id_name = format!("VpcSecurityGroupIds.{member_name}.{index}");
            match optional_query_param(req, &sg_id_name) {
                Some(sg_id) => security_group_ids.push(sg_id),
                None => break,
            }
        }
        if !security_group_ids.is_empty() {
            return security_group_ids;
        }
    }

    // No VPC security groups supplied. Don't fabricate a synthetic
    // `sg-default` — real AWS attaches the VPC's actual default security
    // group and ModifyDBInstance leaves the membership untouched (None)
    // when the parameter is absent. Returning an empty list keeps Create
    // consistent with Modify rather than reporting a group that doesn't
    // exist in the account.
    Vec::new()
}

pub(crate) fn query_param_prefix_exists(req: &AwsRequest, prefix: &str) -> bool {
    req.query_params.keys().any(|key| key.starts_with(prefix))
}

/// AWS RDS encodes string lists as `{Param}.member.N` 1-indexed entries.
/// Used by `EnableCloudwatchLogsExports`, `CloudwatchLogsExportConfiguration.EnableLogTypes`,
/// `ProcessorFeatures.ProcessorFeature.N.{Name,Value}` (caller decides shape).
pub(crate) fn parse_string_member_list(req: &AwsRequest, base: &str) -> Vec<String> {
    let mut out = Vec::new();
    for i in 1.. {
        let key = format!("{base}.member.{i}");
        match optional_query_param(req, &key) {
            Some(v) => out.push(v),
            None => break,
        }
    }
    out
}

/// Convenience wrapper for the cloudwatch-log-exports list which is
/// emitted on Create/Modify/Restore paths.
pub(crate) fn parse_cloudwatch_logs_exports(req: &AwsRequest, base: &str) -> Vec<String> {
    parse_string_member_list(req, base)
}

/// Parse the leading dotted-numeric components of an engine version
/// (e.g. `16.4` -> `[16, 4]`, `8.0.mysql_aurora.3.04.0` -> `[8, 0]`).
/// Stops at the first non-numeric component so Aurora-style versions with
/// embedded engine tokens don't get mis-compared. Returns `None` when the
/// string has no leading numeric component.
fn numeric_version_prefix(version: &str) -> Option<Vec<u64>> {
    let parts: Vec<u64> = version
        .split('.')
        .map_while(|c| c.parse::<u64>().ok())
        .collect();
    if parts.is_empty() {
        None
    } else {
        Some(parts)
    }
}

/// True when `candidate` is a strictly-lower engine version than
/// `current`, comparing dotted-numeric prefixes component-by-component.
/// Conservative: when either version has no numeric prefix (unusual
/// engine strings) we return `false` so a legitimate change isn't
/// rejected as a downgrade.
pub(crate) fn is_version_downgrade(current: &str, candidate: &str) -> bool {
    match (
        numeric_version_prefix(current),
        numeric_version_prefix(candidate),
    ) {
        (Some(cur), Some(cand)) => {
            // Compare only the components both versions share so a caller
            // who supplies a shorter-but-equal prefix (e.g. "16" vs
            // "16.3") isn't rejected. A downgrade is the first differing
            // shared component being lower on the candidate side.
            for (c, n) in cur.iter().zip(cand.iter()) {
                if n < c {
                    return true;
                }
                if n > c {
                    return false;
                }
            }
            false
        }
        _ => false,
    }
}

pub(crate) fn parse_optional_i32(value: Option<&str>) -> Result<Option<i32>, AwsServiceError> {
    value
        .map(|raw| {
            raw.parse::<i32>().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterValue",
                    format!("Integer parameter value '{raw}' is invalid."),
                )
            })
        })
        .transpose()
}

/// Collect the `member.N` values for a Query-protocol log-types list
/// nested under `CloudwatchLogsExportConfiguration` (e.g.
/// `CloudwatchLogsExportConfiguration.EnableLogTypes.member.1`). Returns
/// an empty vec when no values are present.
pub(crate) fn collect_cloudwatch_log_types(req: &AwsRequest, list_name: &str) -> Vec<String> {
    let base = format!("CloudwatchLogsExportConfiguration.{list_name}");
    parse_string_member_list(req, &base)
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

pub(crate) fn paginate<T, F>(
    mut items: Vec<T>,
    marker: Option<String>,
    max_records: Option<String>,
    get_id: F,
) -> Result<PaginationResult<T>, AwsServiceError>
where
    F: Fn(&T) -> &str,
{
    // Parse max_records with default 100, max 100. A junk value is
    // clamped rather than rejected — Smithy doesn't declare a
    // `InvalidParameterValue`-equivalent error shape on Describe* ops,
    // and real AWS RDS is lenient on out-of-range MaxRecords too.
    let max = match max_records.as_deref().map(|s| s.parse::<i32>()) {
        Some(Ok(parsed)) => parsed.clamp(1, 100) as usize,
        Some(Err(_)) | None => 100,
    };

    // Decode marker to get starting identifier. A marker we don't
    // recognise (un-decodable base64, invalid UTF-8) is treated as
    // pointing past the end of the list — same as a marker we
    // recognise that no longer matches a known item. Returning an
    // error here would surface as a wire code Smithy doesn't declare
    // for any Describe* op.
    let start_id = match marker {
        Some(encoded_marker) => BASE64
            .decode(encoded_marker.as_bytes())
            .ok()
            .and_then(|decoded| String::from_utf8(decoded).ok()),
        None => None,
    };

    // Find starting position
    let start_index = if let Some(ref start_id) = start_id {
        items
            .iter()
            .position(|item| get_id(item) == start_id)
            .map(|pos| pos + 1) // Start after the marker
            .unwrap_or(items.len()) // If not found, return empty result
    } else {
        0
    };

    // Take items from start_index
    let total_items = items.len();
    let end_index = std::cmp::min(start_index + max, total_items);
    let paginated_items: Vec<T> = items.drain(start_index..end_index).collect();

    // Create next marker if there are more items
    let next_marker = if end_index < total_items {
        paginated_items
            .last()
            .map(|item| BASE64.encode(get_id(item).as_bytes()))
    } else {
        None
    };

    Ok(PaginationResult {
        items: paginated_items,
        next_marker,
    })
}

pub(crate) fn validate_create_request(
    _db_instance_identifier: &str,
    _allocated_storage: i32,
    db_instance_class: &str,
    engine: &str,
    engine_version: &str,
    _port: i32,
) -> Result<(), AwsServiceError> {
    // AllocatedStorage / Port / identifier-charset checks previously
    // raised `InvalidParameterValue` — an error code Smithy doesn't
    // declare on `CreateDBInstance`. Real AWS would emit
    // `InvalidParameterCombination` for these too, which is also
    // undeclared. Drop the synthetic validation and rely on runtime
    // failures (engine/version mapping below) where a *declared*
    // error shape is available.
    // Validate engine
    let supported_engines = [
        "postgres",
        "mysql",
        "mariadb",
        "oracle-ee",
        "oracle-se2",
        "oracle-ee-cdb",
        "oracle-se2-cdb",
        "sqlserver-ee",
        "sqlserver-se",
        "sqlserver-ex",
        "sqlserver-web",
        "db2-se",
        "db2-ae",
    ];
    if !supported_engines.contains(&engine) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InsufficientDBInstanceCapacity",
            format!(
                "Engine '{}' is not available for the requested instance class.",
                engine
            ),
        ));
    }

    // Validate engine version. The Oracle/SQL Server/Db2 lists track
    // the major-minor versions actually shipped by the upstream
    // dev-edition images (gvenzl/oracle-free 23, mssql-server 2022,
    // db2_community 11.5). Adding a new version here also requires
    // wiring the image tag in `RdsRuntime::ensure_postgres`.
    // Major versions ("8.0", "10.11", ...) are accepted alongside the
    // full `<major>.<minor>.<patch>` triplets — AWS RDS validates both
    // forms and the runtime resolves the matching prebuilt image regardless.
    let supported_versions = match engine {
        "postgres" => vec![
            "17", "16", "15", "14", "13", "17.4", "16.3", "15.5", "14.10", "13.13",
        ],
        "mysql" => vec!["8.0", "8.0.35", "8.0.28", "5.7.44"],
        "mariadb" => vec!["10.6", "10.11", "11.4", "11.4.5", "10.11.6", "10.6.16"],
        "oracle-ee" | "oracle-se2" | "oracle-ee-cdb" | "oracle-se2-cdb" => {
            vec!["23.0.0", "21.0.0", "19.0.0"]
        }
        "sqlserver-ee" | "sqlserver-se" | "sqlserver-ex" | "sqlserver-web" => {
            vec!["16.00.4085.2.v1", "15.00.4322.2.v1"]
        }
        "db2-se" | "db2-ae" => vec!["11.5.9.0.sb00000000.r1", "11.5.8.0.sb00000000.r1"],
        _ => vec![],
    };

    if !supported_versions.contains(&engine_version) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InsufficientDBInstanceCapacity",
            format!("EngineVersion '{engine_version}' is not available for the requested engine."),
        ));
    }
    validate_db_instance_class(db_instance_class)?;
    Ok(())
}

pub(crate) fn validate_db_instance_class(db_instance_class: &str) -> Result<(), AwsServiceError> {
    if !crate::state::SUPPORTED_INSTANCE_CLASSES.contains(&db_instance_class) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InsufficientDBInstanceCapacity",
            format!(
                "DBInstanceClass '{}' is not available in the requested Availability Zone.",
                db_instance_class
            ),
        ));
    }
    Ok(())
}

pub(crate) fn filter_engine_versions(
    versions: &[EngineVersionInfo],
    engine: &Option<String>,
    engine_version: &Option<String>,
    family: &Option<String>,
) -> Vec<EngineVersionInfo> {
    versions
        .iter()
        .filter(|candidate| {
            engine
                .as_ref()
                .is_none_or(|expected| candidate.engine == *expected)
        })
        .filter(|candidate| {
            engine_version
                .as_ref()
                .is_none_or(|expected| candidate.engine_version == *expected)
        })
        .filter(|candidate| {
            family
                .as_ref()
                .is_none_or(|expected| candidate.db_parameter_group_family == *expected)
        })
        .cloned()
        .collect()
}

pub(crate) fn filter_orderable_options(
    options: &[OrderableDbInstanceOption],
    engine: &Option<String>,
    engine_version: &Option<String>,
    db_instance_class: &Option<String>,
    license_model: &Option<String>,
    vpc: Option<bool>,
) -> Vec<OrderableDbInstanceOption> {
    options
        .iter()
        .filter(|candidate| {
            engine
                .as_ref()
                .is_none_or(|expected| candidate.engine == *expected)
        })
        .filter(|candidate| {
            engine_version
                .as_ref()
                .is_none_or(|expected| candidate.engine_version == *expected)
        })
        .filter(|candidate| {
            db_instance_class
                .as_ref()
                .is_none_or(|expected| candidate.db_instance_class == *expected)
        })
        .filter(|candidate| {
            license_model
                .as_ref()
                .is_none_or(|expected| candidate.license_model == *expected)
        })
        .filter(|_| vpc.unwrap_or(true))
        .cloned()
        .collect()
}

/// Build a `DbInstance` for a newly-created read replica, copying the
/// source instance's physical attributes and binding the replica's
/// identifier, ARN, resource id, container id and host port.
#[allow(clippy::too_many_arguments)]
/// Build a `DbInstance` from a restored snapshot. Copies the physical
/// attributes off the snapshot and binds the new instance's identifier,
/// ARN, resource id, container id and host port.
pub(crate) fn build_restored_instance(
    db_instance_identifier: &str,
    db_instance_arn: String,
    dbi_resource_id: String,
    created_at: chrono::DateTime<Utc>,
    vpc_security_group_ids: Vec<String>,
    snapshot: &DbSnapshot,
    running: &crate::runtime::RunningDbContainer,
    tags: Vec<RdsTag>,
) -> DbInstance {
    DbInstance {
        db_instance_identifier: db_instance_identifier.to_string(),
        db_instance_arn,
        db_instance_class: "db.t3.micro".to_string(),
        engine: snapshot.engine.clone(),
        engine_version: snapshot.engine_version.clone(),
        db_instance_status: "available".to_string(),
        master_username: snapshot.master_username.clone(),
        db_name: snapshot.db_name.clone(),
        endpoint_address: running.endpoint_address.clone(),
        port: i32::from(running.endpoint_port),
        allocated_storage: snapshot.allocated_storage,
        publicly_accessible: true,
        deletion_protection: false,
        created_at,
        dbi_resource_id,
        master_user_password: snapshot.master_user_password.clone(),
        container_id: running.container_id.clone(),
        host_port: running.host_port,
        tags,
        read_replica_source_db_instance_identifier: None,
        read_replica_db_instance_identifiers: Vec::new(),
        vpc_security_group_ids,
        db_parameter_group_name: None,
        backup_retention_period: 1,
        preferred_backup_window: "03:00-04:00".to_string(),
        preferred_maintenance_window: None,
        latest_restorable_time: Some(created_at),
        option_group_name: None,
        multi_az: false,
        pending_modified_values: None,
        availability_zone: None,
        storage_type: None,
        storage_encrypted: false,
        kms_key_id: None,
        iam_database_authentication_enabled: false,
        iops: None,
        monitoring_interval: None,
        monitoring_role_arn: None,
        performance_insights_enabled: false,
        performance_insights_kms_key_id: None,
        performance_insights_retention_period: None,
        enabled_cloudwatch_logs_exports: Vec::new(),
        ca_certificate_identifier: None,
        network_type: None,
        character_set_name: None,
        auto_minor_version_upgrade: None,
        copy_tags_to_snapshot: None,
        master_user_secret_arn: None,
        master_user_secret_kms_key_id: None,
        license_model: None,
        max_allocated_storage: None,
        multi_tenant: None,
        storage_throughput: None,
        tde_credential_arn: None,
        delete_automated_backups: None,
        db_security_groups: Vec::new(),
        domain: None,
        domain_fqdn: None,
        domain_ou: None,
        domain_iam_role_name: None,
        domain_auth_secret_arn: None,
        domain_dns_ips: Vec::new(),
        db_cluster_identifier: None,
        activity_stream: None,
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_s3_restored_instance(
    db_instance_identifier: &str,
    db_instance_arn: String,
    dbi_resource_id: String,
    created_at: chrono::DateTime<Utc>,
    allocated_storage: i32,
    db_instance_class: String,
    engine: String,
    engine_version: String,
    master_username: String,
    master_user_password: String,
    db_name: String,
    vpc_security_group_ids: Vec<String>,
    running: &crate::runtime::RunningDbContainer,
    tags: Vec<RdsTag>,
) -> DbInstance {
    DbInstance {
        db_instance_identifier: db_instance_identifier.to_string(),
        db_instance_arn,
        db_instance_class,
        engine,
        engine_version,
        db_instance_status: "available".to_string(),
        master_username,
        db_name: Some(db_name),
        endpoint_address: "127.0.0.1".to_string(),
        port: i32::from(running.host_port),
        allocated_storage,
        publicly_accessible: true,
        deletion_protection: false,
        created_at,
        dbi_resource_id,
        master_user_password,
        container_id: running.container_id.clone(),
        host_port: running.host_port,
        tags,
        read_replica_source_db_instance_identifier: None,
        read_replica_db_instance_identifiers: Vec::new(),
        vpc_security_group_ids,
        db_parameter_group_name: None,
        backup_retention_period: 1,
        preferred_backup_window: "03:00-04:00".to_string(),
        preferred_maintenance_window: None,
        latest_restorable_time: Some(created_at),
        option_group_name: None,
        multi_az: false,
        pending_modified_values: None,
        availability_zone: None,
        storage_type: None,
        storage_encrypted: false,
        kms_key_id: None,
        iam_database_authentication_enabled: false,
        iops: None,
        monitoring_interval: None,
        monitoring_role_arn: None,
        performance_insights_enabled: false,
        performance_insights_kms_key_id: None,
        performance_insights_retention_period: None,
        enabled_cloudwatch_logs_exports: Vec::new(),
        ca_certificate_identifier: None,
        network_type: None,
        character_set_name: None,
        auto_minor_version_upgrade: None,
        copy_tags_to_snapshot: None,
        master_user_secret_arn: None,
        master_user_secret_kms_key_id: None,
        license_model: None,
        max_allocated_storage: None,
        multi_tenant: None,
        storage_throughput: None,
        tde_credential_arn: None,
        delete_automated_backups: None,
        db_security_groups: Vec::new(),
        domain: None,
        domain_fqdn: None,
        domain_ou: None,
        domain_iam_role_name: None,
        domain_auth_secret_arn: None,
        domain_dns_ips: Vec::new(),
        db_cluster_identifier: None,
        activity_stream: None,
    }
}

/// A zero-valued `RunningDbContainer` used to seed a `creating` placeholder row
/// before the real container is started in the background. The finalize task
/// overwrites `endpoint_address`/`endpoint_port`/`host_port`/`container_id` once
/// `ensure_postgres` returns.
pub(crate) fn creating_placeholder_container() -> crate::runtime::RunningDbContainer {
    crate::runtime::RunningDbContainer {
        container_id: String::new(),
        host_port: 0,
        endpoint_address: String::new(),
        endpoint_port: 0,
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_pit_restored_instance(
    db_instance_identifier: &str,
    db_instance_arn: String,
    dbi_resource_id: String,
    created_at: chrono::DateTime<Utc>,
    vpc_security_group_ids: Vec<String>,
    source: &DbInstance,
    running: &crate::runtime::RunningDbContainer,
    tags: Vec<RdsTag>,
) -> DbInstance {
    DbInstance {
        db_instance_identifier: db_instance_identifier.to_string(),
        db_instance_arn,
        db_instance_class: source.db_instance_class.clone(),
        engine: source.engine.clone(),
        engine_version: source.engine_version.clone(),
        db_instance_status: "available".to_string(),
        master_username: source.master_username.clone(),
        db_name: source.db_name.clone(),
        endpoint_address: "127.0.0.1".to_string(),
        port: i32::from(running.host_port),
        allocated_storage: source.allocated_storage,
        publicly_accessible: source.publicly_accessible,
        deletion_protection: false,
        created_at,
        dbi_resource_id,
        master_user_password: source.master_user_password.clone(),
        container_id: running.container_id.clone(),
        host_port: running.host_port,
        tags,
        read_replica_source_db_instance_identifier: None,
        read_replica_db_instance_identifiers: Vec::new(),
        vpc_security_group_ids,
        db_parameter_group_name: source.db_parameter_group_name.clone(),
        backup_retention_period: source.backup_retention_period,
        preferred_backup_window: source.preferred_backup_window.clone(),
        preferred_maintenance_window: source.preferred_maintenance_window.clone(),
        latest_restorable_time: Some(created_at),
        option_group_name: source.option_group_name.clone(),
        multi_az: false,
        pending_modified_values: None,
        availability_zone: source.availability_zone.clone(),
        storage_type: source.storage_type.clone(),
        storage_encrypted: source.storage_encrypted,
        kms_key_id: source.kms_key_id.clone(),
        iam_database_authentication_enabled: source.iam_database_authentication_enabled,
        iops: source.iops,
        monitoring_interval: source.monitoring_interval,
        monitoring_role_arn: source.monitoring_role_arn.clone(),
        performance_insights_enabled: source.performance_insights_enabled,
        performance_insights_kms_key_id: source.performance_insights_kms_key_id.clone(),
        performance_insights_retention_period: source.performance_insights_retention_period,
        enabled_cloudwatch_logs_exports: source.enabled_cloudwatch_logs_exports.clone(),
        ca_certificate_identifier: source.ca_certificate_identifier.clone(),
        network_type: source.network_type.clone(),
        character_set_name: source.character_set_name.clone(),
        auto_minor_version_upgrade: source.auto_minor_version_upgrade,
        copy_tags_to_snapshot: source.copy_tags_to_snapshot,
        master_user_secret_arn: source.master_user_secret_arn.clone(),
        master_user_secret_kms_key_id: source.master_user_secret_kms_key_id.clone(),
        license_model: source.license_model.clone(),
        max_allocated_storage: source.max_allocated_storage,
        multi_tenant: source.multi_tenant,
        storage_throughput: source.storage_throughput,
        tde_credential_arn: source.tde_credential_arn.clone(),
        delete_automated_backups: source.delete_automated_backups,
        db_security_groups: source.db_security_groups.clone(),
        domain: source.domain.clone(),
        domain_fqdn: source.domain_fqdn.clone(),
        domain_ou: source.domain_ou.clone(),
        domain_iam_role_name: source.domain_iam_role_name.clone(),
        domain_auth_secret_arn: source.domain_auth_secret_arn.clone(),
        domain_dns_ips: source.domain_dns_ips.clone(),
        db_cluster_identifier: source.db_cluster_identifier.clone(),
        activity_stream: source.activity_stream.clone(),
    }
}

pub(crate) fn build_read_replica_instance(
    db_instance_identifier: &str,
    db_instance_arn: String,
    dbi_resource_id: String,
    created_at: chrono::DateTime<Utc>,
    source_db_instance_identifier: &str,
    source: &DbInstance,
    running: &crate::runtime::RunningDbContainer,
) -> DbInstance {
    DbInstance {
        db_instance_identifier: db_instance_identifier.to_string(),
        db_instance_arn,
        db_instance_class: source.db_instance_class.clone(),
        engine: source.engine.clone(),
        engine_version: source.engine_version.clone(),
        db_instance_status: "available".to_string(),
        master_username: source.master_username.clone(),
        db_name: source.db_name.clone(),
        endpoint_address: "127.0.0.1".to_string(),
        port: i32::from(running.host_port),
        allocated_storage: source.allocated_storage,
        publicly_accessible: source.publicly_accessible,
        deletion_protection: false,
        created_at,
        dbi_resource_id,
        master_user_password: source.master_user_password.clone(),
        container_id: running.container_id.clone(),
        host_port: running.host_port,
        tags: Vec::new(),
        read_replica_source_db_instance_identifier: Some(source_db_instance_identifier.to_string()),
        read_replica_db_instance_identifiers: Vec::new(),
        vpc_security_group_ids: source.vpc_security_group_ids.clone(),
        db_parameter_group_name: source.db_parameter_group_name.clone(),
        backup_retention_period: source.backup_retention_period,
        preferred_backup_window: source.preferred_backup_window.clone(),
        preferred_maintenance_window: source.preferred_maintenance_window.clone(),
        latest_restorable_time: if source.backup_retention_period > 0 {
            Some(created_at)
        } else {
            None
        },
        option_group_name: source.option_group_name.clone(),
        multi_az: source.multi_az,
        pending_modified_values: None,
        availability_zone: source.availability_zone.clone(),
        storage_type: source.storage_type.clone(),
        storage_encrypted: source.storage_encrypted,
        kms_key_id: source.kms_key_id.clone(),
        iam_database_authentication_enabled: source.iam_database_authentication_enabled,
        iops: source.iops,
        monitoring_interval: source.monitoring_interval,
        monitoring_role_arn: source.monitoring_role_arn.clone(),
        performance_insights_enabled: source.performance_insights_enabled,
        performance_insights_kms_key_id: source.performance_insights_kms_key_id.clone(),
        performance_insights_retention_period: source.performance_insights_retention_period,
        enabled_cloudwatch_logs_exports: source.enabled_cloudwatch_logs_exports.clone(),
        ca_certificate_identifier: source.ca_certificate_identifier.clone(),
        network_type: source.network_type.clone(),
        character_set_name: source.character_set_name.clone(),
        auto_minor_version_upgrade: source.auto_minor_version_upgrade,
        copy_tags_to_snapshot: source.copy_tags_to_snapshot,
        master_user_secret_arn: None,
        master_user_secret_kms_key_id: None,
        license_model: source.license_model.clone(),
        max_allocated_storage: source.max_allocated_storage,
        multi_tenant: source.multi_tenant,
        storage_throughput: source.storage_throughput,
        tde_credential_arn: source.tde_credential_arn.clone(),
        delete_automated_backups: source.delete_automated_backups,
        db_security_groups: source.db_security_groups.clone(),
        domain: source.domain.clone(),
        domain_fqdn: source.domain_fqdn.clone(),
        domain_ou: source.domain_ou.clone(),
        domain_iam_role_name: source.domain_iam_role_name.clone(),
        domain_auth_secret_arn: source.domain_auth_secret_arn.clone(),
        domain_dns_ips: source.domain_dns_ips.clone(),
        db_cluster_identifier: source.db_cluster_identifier.clone(),
        activity_stream: source.activity_stream.clone(),
    }
}

pub(crate) fn engine_version_xml(version: &EngineVersionInfo) -> String {
    format!(
        "<DBEngineVersion>\
         <Engine>{}</Engine>\
         <EngineVersion>{}</EngineVersion>\
         <DBParameterGroupFamily>{}</DBParameterGroupFamily>\
         <DBEngineDescription>{}</DBEngineDescription>\
         <DBEngineVersionDescription>{}</DBEngineVersionDescription>\
         <Status>{}</Status>\
         </DBEngineVersion>",
        xml_escape(&version.engine),
        xml_escape(&version.engine_version),
        xml_escape(&version.db_parameter_group_family),
        xml_escape(&version.db_engine_description),
        xml_escape(&version.db_engine_version_description),
        xml_escape(&version.status),
    )
}

pub(crate) fn orderable_option_xml(option: &OrderableDbInstanceOption) -> String {
    format!(
        "<OrderableDBInstanceOption>\
         <Engine>{}</Engine>\
         <EngineVersion>{}</EngineVersion>\
         <DBInstanceClass>{}</DBInstanceClass>\
         <LicenseModel>{}</LicenseModel>\
         <AvailabilityZones><AvailabilityZone><Name>us-east-1a</Name></AvailabilityZone></AvailabilityZones>\
         <MultiAZCapable>true</MultiAZCapable>\
         <ReadReplicaCapable>true</ReadReplicaCapable>\
         <Vpc>true</Vpc>\
         <SupportsStorageEncryption>true</SupportsStorageEncryption>\
         <StorageType>{}</StorageType>\
         <SupportsIops>false</SupportsIops>\
         <MinStorageSize>{}</MinStorageSize>\
         <MaxStorageSize>{}</MaxStorageSize>\
         <SupportsIAMDatabaseAuthentication>true</SupportsIAMDatabaseAuthentication>\
         </OrderableDBInstanceOption>",
        xml_escape(&option.engine),
        xml_escape(&option.engine_version),
        xml_escape(&option.db_instance_class),
        xml_escape(&option.license_model),
        xml_escape(&option.storage_type),
        option.min_storage_size,
        option.max_storage_size,
    )
}

pub(crate) fn tag_xml(tag: &RdsTag) -> String {
    format!(
        "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
        xml_escape(&tag.key),
        xml_escape(&tag.value),
    )
}

/// Free-standing version of `emit_event` so background tasks (which
/// don't have a `&self`) can publish RDS events through the same path.
///
/// When `state` and `account_id` are provided the event is also
/// recorded in the per-account events ring so DescribeEvents can serve
/// it. Background tasks that already cleared their account state pass
/// `None` for those parameters.
pub(crate) fn emit_event_static(
    delivery_bus: Option<&Arc<DeliveryBus>>,
    source_type: RdsSourceType,
    source_identifier: &str,
    source_arn: &str,
    event_id: &str,
    event_categories: &[&str],
    message: &str,
) {
    emit_event_static_with_state(
        delivery_bus,
        None,
        None,
        source_type,
        source_identifier,
        source_arn,
        event_id,
        event_categories,
        message,
    );
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn emit_event_static_with_state(
    delivery_bus: Option<&Arc<DeliveryBus>>,
    state: Option<&crate::state::SharedRdsState>,
    account_id: Option<&str>,
    source_type: RdsSourceType,
    source_identifier: &str,
    source_arn: &str,
    event_id: &str,
    event_categories: &[&str],
    message: &str,
) {
    let now = Utc::now();
    if let (Some(state), Some(account_id)) = (state, account_id) {
        let mut accounts = state.write();
        let s = accounts.get_or_create(account_id);
        // The events ring is the read-side for `DescribeEvents`, so we
        // store the kebab-case form AWS uses there. EventBridge keeps the
        // SCREAMING_SNAKE form below to match the published RDS schema.
        s.push_event(crate::state::RdsEventRecord {
            source_identifier: source_identifier.to_string(),
            source_type: source_type.describe_events_str().to_string(),
            source_arn: source_arn.to_string(),
            event_id: event_id.to_string(),
            event_categories: event_categories.iter().map(|s| s.to_string()).collect(),
            message: message.to_string(),
            date: now,
        });
    }
    let Some(bus) = delivery_bus else {
        return;
    };
    let detail = serde_json::json!({
        "EventCategories": event_categories,
        "SourceType": source_type.as_str(),
        "SourceArn": source_arn,
        "Date": now.to_rfc3339(),
        "Message": message,
        "SourceIdentifier": source_identifier,
        "EventID": event_id,
    });
    bus.put_event_to_eventbridge(
        "aws.rds",
        source_type.detail_type(),
        &detail.to_string(),
        "default",
    );
}

pub(crate) fn db_instance_xml(instance: &DbInstance, status_override: Option<&str>) -> String {
    let status = status_override.unwrap_or(&instance.db_instance_status);
    let db_name_xml = instance
        .db_name
        .as_ref()
        .map(|db_name| format!("<DBName>{}</DBName>", xml_escape(db_name)))
        .unwrap_or_default();

    let read_replica_source_xml = instance
        .read_replica_source_db_instance_identifier
        .as_ref()
        .map(|source| {
            format!(
                "<ReadReplicaSourceDBInstanceIdentifier>{}</ReadReplicaSourceDBInstanceIdentifier>",
                xml_escape(source)
            )
        })
        .unwrap_or_default();

    let read_replica_identifiers_xml = if instance.read_replica_db_instance_identifiers.is_empty() {
        "<ReadReplicaDBInstanceIdentifiers/>".to_string()
    } else {
        format!(
            "<ReadReplicaDBInstanceIdentifiers>{}</ReadReplicaDBInstanceIdentifiers>",
            instance
                .read_replica_db_instance_identifiers
                .iter()
                .map(|id| format!(
                    "<ReadReplicaDBInstanceIdentifier>{}</ReadReplicaDBInstanceIdentifier>",
                    xml_escape(id)
                ))
                .collect::<String>()
        )
    };

    let vpc_security_groups_xml = if instance.vpc_security_group_ids.is_empty() {
        "<VpcSecurityGroups/>".to_string()
    } else {
        format!(
            "<VpcSecurityGroups>{}</VpcSecurityGroups>",
            instance
                .vpc_security_group_ids
                .iter()
                .map(|sg_id| format!(
                    "<VpcSecurityGroupMembership>\
                     <VpcSecurityGroupId>{}</VpcSecurityGroupId>\
                     <Status>active</Status>\
                     </VpcSecurityGroupMembership>",
                    xml_escape(sg_id)
                ))
                .collect::<String>()
        )
    };

    let db_parameter_groups_xml = match &instance.db_parameter_group_name {
        Some(pg_name) => format!(
            "<DBParameterGroups>\
             <DBParameterGroup>\
             <DBParameterGroupName>{}</DBParameterGroupName>\
             <ParameterApplyStatus>in-sync</ParameterApplyStatus>\
             </DBParameterGroup>\
             </DBParameterGroups>",
            xml_escape(pg_name)
        ),
        None => "<DBParameterGroups/>".to_string(),
    };

    let option_group_memberships_xml = match &instance.option_group_name {
        Some(og_name) => format!(
            "<OptionGroupMemberships>\
             <OptionGroupMembership>\
             <OptionGroupName>{}</OptionGroupName>\
             <Status>in-sync</Status>\
             </OptionGroupMembership>\
             </OptionGroupMemberships>",
            xml_escape(og_name)
        ),
        None => "<OptionGroupMemberships/>".to_string(),
    };

    let pending_modified_values_xml = if let Some(ref pending) = instance.pending_modified_values {
        let mut fields = Vec::new();
        if let Some(ref class) = pending.db_instance_class {
            fields.push(format!(
                "<DBInstanceClass>{}</DBInstanceClass>",
                xml_escape(class)
            ));
        }
        if let Some(allocated_storage) = pending.allocated_storage {
            fields.push(format!(
                "<AllocatedStorage>{}</AllocatedStorage>",
                allocated_storage
            ));
        }
        if let Some(backup_retention_period) = pending.backup_retention_period {
            fields.push(format!(
                "<BackupRetentionPeriod>{}</BackupRetentionPeriod>",
                backup_retention_period
            ));
        }
        if let Some(multi_az) = pending.multi_az {
            fields.push(format!(
                "<MultiAZ>{}</MultiAZ>",
                if multi_az { "true" } else { "false" }
            ));
        }
        if let Some(ref engine_version) = pending.engine_version {
            fields.push(format!(
                "<EngineVersion>{}</EngineVersion>",
                xml_escape(engine_version)
            ));
        }
        if pending.master_user_password.is_some() {
            fields.push("<MasterUserPassword>****</MasterUserPassword>".to_string());
        }
        if let Some(ref window) = pending.preferred_backup_window {
            fields.push(format!(
                "<PreferredBackupWindow>{}</PreferredBackupWindow>",
                xml_escape(window)
            ));
        }
        if let Some(ref window) = pending.preferred_maintenance_window {
            fields.push(format!(
                "<PreferredMaintenanceWindow>{}</PreferredMaintenanceWindow>",
                xml_escape(window)
            ));
        }
        if let Some(ref name) = pending.db_parameter_group_name {
            fields.push(format!(
                "<DBParameterGroupName>{}</DBParameterGroupName>",
                xml_escape(name)
            ));
        }
        if let Some(iops) = pending.iops {
            fields.push(format!("<Iops>{iops}</Iops>"));
        }
        if let Some(ref stype) = pending.storage_type {
            fields.push(format!("<StorageType>{}</StorageType>", xml_escape(stype)));
        }
        if let Some(throughput) = pending.storage_throughput {
            fields.push(format!(
                "<StorageThroughput>{throughput}</StorageThroughput>"
            ));
        }
        if let Some(interval) = pending.monitoring_interval {
            fields.push(format!(
                "<MonitoringInterval>{interval}</MonitoringInterval>"
            ));
        }
        if let Some(b) = pending.performance_insights_enabled {
            fields.push(format!(
                "<PerformanceInsightsEnabled>{}</PerformanceInsightsEnabled>",
                if b { "true" } else { "false" }
            ));
        }
        if let Some(ref types) = pending.enabled_cloudwatch_logs_exports {
            let inner = types
                .iter()
                .map(|t| format!("<member>{}</member>", xml_escape(t)))
                .collect::<String>();
            fields.push(format!(
                "<PendingCloudwatchLogsExports><LogTypesToEnable>{inner}</LogTypesToEnable><LogTypesToDisable/></PendingCloudwatchLogsExports>"
            ));
        }
        if let Some(ref lm) = pending.license_model {
            fields.push(format!("<LicenseModel>{}</LicenseModel>", xml_escape(lm)));
        }
        if let Some(b) = pending.multi_tenant {
            fields.push(format!(
                "<MultiTenant>{}</MultiTenant>",
                if b { "true" } else { "false" }
            ));
        }
        if let Some(b) = pending.publicly_accessible {
            fields.push(format!(
                "<PubliclyAccessible>{}</PubliclyAccessible>",
                if b { "true" } else { "false" }
            ));
        }
        if let Some(ref arn) = pending.tde_credential_arn {
            fields.push(format!(
                "<TdeCredentialArn>{}</TdeCredentialArn>",
                xml_escape(arn)
            ));
        }
        if let Some(p) = pending.port {
            fields.push(format!("<DBPortNumber>{p}</DBPortNumber>"));
        }
        if let Some(ref ca) = pending.ca_certificate_identifier {
            fields.push(format!(
                "<CACertificateIdentifier>{}</CACertificateIdentifier>",
                xml_escape(ca)
            ));
        }
        if !fields.is_empty() {
            format!(
                "<PendingModifiedValues>{}</PendingModifiedValues>",
                fields.join("")
            )
        } else {
            String::new()
        }
    } else {
        String::new()
    };

    let latest_restorable_time_xml = instance
        .latest_restorable_time
        .map(|t| {
            format!(
                "<LatestRestorableTime>{}</LatestRestorableTime>",
                t.to_rfc3339()
            )
        })
        .unwrap_or_default();

    // Endpoint is suppressed while the container is still coming up so
    // SDK callers don't try to dial an empty host:0. Once the background
    // task fills in `endpoint_address` and `port`, DescribeDBInstances
    // returns the real endpoint.
    let endpoint_xml = if instance.endpoint_address.is_empty() || instance.port == 0 {
        String::new()
    } else {
        format!(
            "<Endpoint><Address>{}</Address><Port>{}</Port></Endpoint>",
            xml_escape(&instance.endpoint_address),
            instance.port
        )
    };

    let availability_zone = instance
        .availability_zone
        .clone()
        .unwrap_or_else(|| "us-east-1a".to_string());
    let storage_type = instance
        .storage_type
        .clone()
        .unwrap_or_else(|| "gp2".to_string());
    let kms_key_id_xml = instance
        .kms_key_id
        .as_ref()
        .map(|k| format!("<KmsKeyId>{}</KmsKeyId>", xml_escape(k)))
        .unwrap_or_default();
    let iops_xml = instance
        .iops
        .map(|n| format!("<Iops>{n}</Iops>"))
        .unwrap_or_default();
    let monitoring_interval_xml = instance
        .monitoring_interval
        .map(|n| format!("<MonitoringInterval>{n}</MonitoringInterval>"))
        .unwrap_or_default();
    let monitoring_role_xml = instance
        .monitoring_role_arn
        .as_ref()
        .map(|a| {
            format!(
                "<EnhancedMonitoringResourceArn>{}</EnhancedMonitoringResourceArn>",
                xml_escape(a)
            )
        })
        .unwrap_or_default();
    let pi_kms_xml = instance
        .performance_insights_kms_key_id
        .as_ref()
        .map(|k| {
            format!(
                "<PerformanceInsightsKMSKeyId>{}</PerformanceInsightsKMSKeyId>",
                xml_escape(k)
            )
        })
        .unwrap_or_default();
    let pi_retention_xml = instance
        .performance_insights_retention_period
        .map(|n| {
            format!("<PerformanceInsightsRetentionPeriod>{n}</PerformanceInsightsRetentionPeriod>")
        })
        .unwrap_or_default();
    let cloudwatch_exports_xml = if instance.enabled_cloudwatch_logs_exports.is_empty() {
        "<EnabledCloudwatchLogsExports/>".to_string()
    } else {
        format!(
            "<EnabledCloudwatchLogsExports>{}</EnabledCloudwatchLogsExports>",
            instance
                .enabled_cloudwatch_logs_exports
                .iter()
                .map(|e| format!("<member>{}</member>", xml_escape(e)))
                .collect::<String>()
        )
    };
    let ca_cert_xml = instance
        .ca_certificate_identifier
        .as_ref()
        .map(|c| {
            format!(
                "<CACertificateIdentifier>{}</CACertificateIdentifier>",
                xml_escape(c)
            )
        })
        .unwrap_or_default();
    let network_type_xml = instance
        .network_type
        .as_ref()
        .map(|n| format!("<NetworkType>{}</NetworkType>", xml_escape(n)))
        .unwrap_or_default();
    let charset_xml = instance
        .character_set_name
        .as_ref()
        .map(|c| format!("<CharacterSetName>{}</CharacterSetName>", xml_escape(c)))
        .unwrap_or_default();
    let auto_minor_xml = format!(
        "<AutoMinorVersionUpgrade>{}</AutoMinorVersionUpgrade>",
        if instance.auto_minor_version_upgrade.unwrap_or(true) {
            "true"
        } else {
            "false"
        }
    );
    let copy_tags_xml = instance
        .copy_tags_to_snapshot
        .map(|b| {
            format!(
                "<CopyTagsToSnapshot>{}</CopyTagsToSnapshot>",
                if b { "true" } else { "false" }
            )
        })
        .unwrap_or_default();
    let master_user_secret_xml = instance
        .master_user_secret_arn
        .as_ref()
        .map(|arn| {
            let kms = instance
                .master_user_secret_kms_key_id
                .as_ref()
                .map(|k| format!("<KmsKeyId>{}</KmsKeyId>", xml_escape(k)))
                .unwrap_or_default();
            format!(
                "<MasterUserSecret><SecretArn>{}</SecretArn><SecretStatus>active</SecretStatus>{kms}</MasterUserSecret>",
                xml_escape(arn)
            )
        })
        .unwrap_or_default();
    let preferred_maintenance_window_xml = format!(
        "<PreferredMaintenanceWindow>{}</PreferredMaintenanceWindow>",
        xml_escape(
            instance
                .preferred_maintenance_window
                .as_deref()
                .unwrap_or("sun:00:00-sun:00:30")
        )
    );
    let db_security_groups_xml = if instance.db_security_groups.is_empty() {
        "<DBSecurityGroups/>".to_string()
    } else {
        format!(
            "<DBSecurityGroups>{}</DBSecurityGroups>",
            instance
                .db_security_groups
                .iter()
                .map(|name| format!(
                    "<DBSecurityGroup><DBSecurityGroupName>{}</DBSecurityGroupName><Status>active</Status></DBSecurityGroup>",
                    xml_escape(name)
                ))
                .collect::<String>()
        )
    };
    let max_allocated_storage_xml = instance
        .max_allocated_storage
        .map(|n| format!("<MaxAllocatedStorage>{n}</MaxAllocatedStorage>"))
        .unwrap_or_default();
    let storage_throughput_xml = instance
        .storage_throughput
        .map(|n| format!("<StorageThroughput>{n}</StorageThroughput>"))
        .unwrap_or_default();
    let multi_tenant_xml = instance
        .multi_tenant
        .map(|b| {
            format!(
                "<MultiTenant>{}</MultiTenant>",
                if b { "true" } else { "false" }
            )
        })
        .unwrap_or_default();
    let tde_credential_arn_xml = instance
        .tde_credential_arn
        .as_ref()
        .map(|a| format!("<TdeCredentialArn>{}</TdeCredentialArn>", xml_escape(a)))
        .unwrap_or_default();
    let domain_memberships_xml = if instance.domain.is_some()
        || instance.domain_fqdn.is_some()
        || instance.domain_iam_role_name.is_some()
    {
        let domain_inner = format!(
            "<Domain>{domain}</Domain><Status>joined</Status><FQDN>{fqdn}</FQDN><IAMRoleName>{iam_role}</IAMRoleName>{ou}{auth_secret}{dns_ips}",
            domain = xml_escape(instance.domain.as_deref().unwrap_or("")),
            fqdn = xml_escape(instance.domain_fqdn.as_deref().unwrap_or("")),
            iam_role = xml_escape(instance.domain_iam_role_name.as_deref().unwrap_or("")),
            ou = instance
                .domain_ou
                .as_ref()
                .map(|v| format!("<OU>{}</OU>", xml_escape(v)))
                .unwrap_or_default(),
            auth_secret = instance
                .domain_auth_secret_arn
                .as_ref()
                .map(|v| format!("<AuthSecretArn>{}</AuthSecretArn>", xml_escape(v)))
                .unwrap_or_default(),
            dns_ips = if instance.domain_dns_ips.is_empty() {
                String::new()
            } else {
                format!(
                    "<DnsIps>{}</DnsIps>",
                    instance
                        .domain_dns_ips
                        .iter()
                        .map(|ip| format!("<member>{}</member>", xml_escape(ip)))
                        .collect::<String>()
                )
            },
        );
        format!("<DomainMemberships><DomainMembership>{domain_inner}</DomainMembership></DomainMemberships>")
    } else {
        "<DomainMemberships/>".to_string()
    };
    // Database Activity Stream: reflect the persisted stream state so
    // Start/Stop/Modify ActivityStream round-trips through describe. A
    // `None` config reads back as a stopped stream (no kms/kinesis/mode).
    let activity_stream_xml = match instance.activity_stream.as_ref() {
        Some(stream) => {
            let status = if stream.status.is_empty() {
                "stopped"
            } else {
                stream.status.as_str()
            };
            let kms = stream
                .kms_key_id
                .as_ref()
                .map(|k| {
                    format!(
                        "<ActivityStreamKmsKeyId>{}</ActivityStreamKmsKeyId>",
                        xml_escape(k)
                    )
                })
                .unwrap_or_default();
            let kinesis = stream
                .kinesis_stream_name
                .as_ref()
                .map(|s| {
                    format!(
                        "<ActivityStreamKinesisStreamName>{}</ActivityStreamKinesisStreamName>",
                        xml_escape(s)
                    )
                })
                .unwrap_or_default();
            let mode = stream
                .mode
                .as_ref()
                .map(|m| format!("<ActivityStreamMode>{}</ActivityStreamMode>", xml_escape(m)))
                .unwrap_or_default();
            format!(
                "<ActivityStreamStatus>{}</ActivityStreamStatus>{kms}{kinesis}{mode}",
                xml_escape(status)
            )
        }
        None => "<ActivityStreamStatus>stopped</ActivityStreamStatus>".to_string(),
    };

    format!(
        "<DBInstanceIdentifier>{identifier}</DBInstanceIdentifier>\
         <DBInstanceClass>{class}</DBInstanceClass>\
         <Engine>{engine}</Engine>\
         <DBInstanceStatus>{status}</DBInstanceStatus>\
         <MasterUsername>{master_username}</MasterUsername>\
         {db_name_xml}\
         {endpoint_xml}\
         <AllocatedStorage>{allocated_storage}</AllocatedStorage>\
         <InstanceCreateTime>{create_time}</InstanceCreateTime>\
         <PreferredBackupWindow>{preferred_backup_window}</PreferredBackupWindow>\
         <BackupRetentionPeriod>{backup_retention_period}</BackupRetentionPeriod>\
         {db_security_groups_xml}\
         {vpc_security_groups_xml}\
         {db_parameter_groups_xml}\
         <AvailabilityZone>{availability_zone}</AvailabilityZone>\
         {latest_restorable_time_xml}\
         {preferred_maintenance_window_xml}\
         <MultiAZ>{multi_az}</MultiAZ>\
         <EngineVersion>{engine_version}</EngineVersion>\
         {auto_minor_xml}\
         {read_replica_identifiers_xml}\
         {read_replica_source_xml}\
         <LicenseModel>{license_model}</LicenseModel>\
         {option_group_memberships_xml}\
         <PubliclyAccessible>{publicly_accessible}</PubliclyAccessible>\
         <StorageType>{storage_type}</StorageType>\
         {storage_throughput_xml}\
         {max_allocated_storage_xml}\
         {multi_tenant_xml}\
         {tde_credential_arn_xml}\
         {domain_memberships_xml}\
         <DbInstancePort>{port}</DbInstancePort>\
         <StorageEncrypted>{storage_encrypted}</StorageEncrypted>\
         {kms_key_id_xml}\
         <IAMDatabaseAuthenticationEnabled>{iam_auth}</IAMDatabaseAuthenticationEnabled>\
         {iops_xml}\
         {monitoring_interval_xml}\
         {monitoring_role_xml}\
         <PerformanceInsightsEnabled>{pi_enabled}</PerformanceInsightsEnabled>\
         {pi_kms_xml}\
         {pi_retention_xml}\
         {cloudwatch_exports_xml}\
         {ca_cert_xml}\
         {network_type_xml}\
         {charset_xml}\
         {copy_tags_xml}\
         {master_user_secret_xml}\
         <ProcessorFeatures/>\
         {activity_stream_xml}\
         <DbiResourceId>{dbi_resource_id}</DbiResourceId>\
         <DeletionProtection>{deletion_protection}</DeletionProtection>\
         {pending_modified_values_xml}\
         <DBInstanceArn>{arn}</DBInstanceArn>",
        identifier = xml_escape(&instance.db_instance_identifier),
        class = xml_escape(&instance.db_instance_class),
        engine = xml_escape(&instance.engine),
        status = xml_escape(status),
        master_username = xml_escape(&instance.master_username),
        port = instance.port,
        allocated_storage = instance.allocated_storage,
        create_time = instance.created_at.to_rfc3339(),
        preferred_backup_window = xml_escape(&instance.preferred_backup_window),
        backup_retention_period = instance.backup_retention_period,
        multi_az = if instance.multi_az { "true" } else { "false" },
        engine_version = xml_escape(&instance.engine_version),
        license_model = xml_escape(
            instance
                .license_model
                .as_deref()
                .unwrap_or_else(|| license_model_for_engine(&instance.engine))
        ),
        publicly_accessible = if instance.publicly_accessible {
            "true"
        } else {
            "false"
        },
        availability_zone = xml_escape(&availability_zone),
        storage_type = xml_escape(&storage_type),
        storage_encrypted = if instance.storage_encrypted {
            "true"
        } else {
            "false"
        },
        iam_auth = if instance.iam_database_authentication_enabled {
            "true"
        } else {
            "false"
        },
        pi_enabled = if instance.performance_insights_enabled {
            "true"
        } else {
            "false"
        },
        dbi_resource_id = xml_escape(&instance.dbi_resource_id),
        deletion_protection = if instance.deletion_protection {
            "true"
        } else {
            "false"
        },
        arn = xml_escape(&instance.db_instance_arn),
    )
}

pub(crate) fn db_snapshot_xml(snapshot: &DbSnapshot) -> String {
    let opt = |tag: &str, value: Option<&str>| -> String {
        value
            .map(|v| format!("<{tag}>{}</{tag}>", xml_escape(v)))
            .unwrap_or_default()
    };
    let opt_int = |tag: &str, value: Option<i32>| -> String {
        value
            .map(|v| format!("<{tag}>{v}</{tag}>"))
            .unwrap_or_default()
    };

    let availability_zone_xml = opt("AvailabilityZone", snapshot.availability_zone.as_deref());
    let vpc_id_xml = opt("VpcId", snapshot.vpc_id.as_deref());
    let instance_create_time_xml = snapshot
        .instance_create_time
        .map(|t| {
            format!(
                "<InstanceCreateTime>{}</InstanceCreateTime>",
                t.to_rfc3339()
            )
        })
        .unwrap_or_default();
    let license_model_xml = opt("LicenseModel", snapshot.license_model.as_deref());
    let iops_xml = opt_int("Iops", snapshot.iops);
    let option_group_xml = opt("OptionGroupName", snapshot.option_group_name.as_deref());
    let percent_progress_xml = opt_int("PercentProgress", snapshot.percent_progress);
    let storage_type_xml = opt("StorageType", snapshot.storage_type.as_deref());
    let kms_key_id_xml = opt("KmsKeyId", snapshot.kms_key_id.as_deref());
    let timezone_xml = opt("Timezone", snapshot.timezone.as_deref());
    let storage_throughput_xml = opt_int("StorageThroughput", snapshot.storage_throughput);

    format!(
        "<DBSnapshotIdentifier>{}</DBSnapshotIdentifier>\
         <DBInstanceIdentifier>{}</DBInstanceIdentifier>\
         <SnapshotCreateTime>{}</SnapshotCreateTime>\
         <Engine>{}</Engine>\
         <EngineVersion>{}</EngineVersion>\
         <AllocatedStorage>{}</AllocatedStorage>\
         <Status>{}</Status>\
         <Port>{}</Port>\
         <MasterUsername>{}</MasterUsername>\
         {db_name_xml}\
         <DbiResourceId>{}</DbiResourceId>\
         <SnapshotType>{}</SnapshotType>\
         {availability_zone_xml}\
         {vpc_id_xml}\
         {instance_create_time_xml}\
         {license_model_xml}\
         {iops_xml}\
         {option_group_xml}\
         {percent_progress_xml}\
         {storage_type_xml}\
         <Encrypted>{encrypted}</Encrypted>\
         {kms_key_id_xml}\
         <IAMDatabaseAuthenticationEnabled>{iam_auth}</IAMDatabaseAuthenticationEnabled>\
         {timezone_xml}\
         {storage_throughput_xml}\
         <ProcessorFeatures/>\
         <DBSnapshotArn>{}</DBSnapshotArn>",
        xml_escape(&snapshot.db_snapshot_identifier),
        xml_escape(&snapshot.db_instance_identifier),
        snapshot.snapshot_create_time.to_rfc3339(),
        xml_escape(&snapshot.engine),
        xml_escape(&snapshot.engine_version),
        snapshot.allocated_storage,
        xml_escape(&snapshot.status),
        snapshot.port,
        xml_escape(&snapshot.master_username),
        xml_escape(&snapshot.dbi_resource_id),
        xml_escape(&snapshot.snapshot_type),
        xml_escape(&snapshot.db_snapshot_arn),
        db_name_xml = snapshot
            .db_name
            .as_ref()
            .map(|name| format!("<DBName>{}</DBName>", xml_escape(name)))
            .unwrap_or_default(),
        encrypted = if snapshot.encrypted { "true" } else { "false" },
        iam_auth = if snapshot.iam_database_authentication_enabled {
            "true"
        } else {
            "false"
        },
    )
}

pub(crate) fn db_subnet_group_xml(subnet_group: &DbSubnetGroup) -> String {
    let subnets_xml = subnet_group
        .subnet_ids
        .iter()
        .zip(&subnet_group.subnet_availability_zones)
        .map(|(subnet_id, az)| {
            format!(
                "<Subnet>\
                 <SubnetIdentifier>{}</SubnetIdentifier>\
                 <SubnetAvailabilityZone><Name>{}</Name></SubnetAvailabilityZone>\
                 <SubnetStatus>Active</SubnetStatus>\
                 </Subnet>",
                xml_escape(subnet_id),
                xml_escape(az)
            )
        })
        .collect::<String>();

    format!(
        "<DBSubnetGroupName>{}</DBSubnetGroupName>\
         <DBSubnetGroupDescription>{}</DBSubnetGroupDescription>\
         <VpcId>{}</VpcId>\
         <SubnetGroupStatus>Complete</SubnetGroupStatus>\
         <Subnets>{}</Subnets>\
         <DBSubnetGroupArn>{}</DBSubnetGroupArn>\
         <SupportedNetworkTypes><member>IPV4</member></SupportedNetworkTypes>",
        xml_escape(&subnet_group.db_subnet_group_name),
        xml_escape(&subnet_group.db_subnet_group_description),
        xml_escape(&subnet_group.vpc_id),
        subnets_xml,
        xml_escape(&subnet_group.db_subnet_group_arn),
    )
}

pub(crate) fn db_parameter_group_xml(parameter_group: &DbParameterGroup) -> String {
    format!(
        "<DBParameterGroupName>{}</DBParameterGroupName>\
         <DBParameterGroupFamily>{}</DBParameterGroupFamily>\
         <Description>{}</Description>\
         <DBParameterGroupArn>{}</DBParameterGroupArn>",
        xml_escape(&parameter_group.db_parameter_group_name),
        xml_escape(&parameter_group.db_parameter_group_family),
        xml_escape(&parameter_group.description),
        xml_escape(&parameter_group.db_parameter_group_arn),
    )
}

pub(crate) fn db_instance_not_found(identifier: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "DBInstanceNotFound",
        format!("DBInstance {} not found.", identifier),
    )
}

pub(crate) fn db_snapshot_not_found(identifier: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "DBSnapshotNotFound",
        format!("DBSnapshot {} not found.", identifier),
    )
}

pub(crate) fn merge_tags(existing: &mut Vec<RdsTag>, incoming: &[RdsTag]) {
    for tag in incoming {
        if let Some(existing_tag) = existing
            .iter_mut()
            .find(|candidate| candidate.key == tag.key)
        {
            existing_tag.value = tag.value.clone();
        } else {
            existing.push(tag.clone());
        }
    }
}

/// Construct the not-found error for tag operations. Smithy declares
/// every per-resource `*NotFoundFault` shape on the tag ops, but no
/// generic "bad ARN" code, so we fall back to `DBInstanceNotFound`
/// (declared on AddTags/ListTags/RemoveTags) when the ARN itself
/// can't be parsed or its segment isn't a known tag resource kind.
pub(crate) fn tag_resource_not_found(arn: &str) -> AwsServiceError {
    // The tag-target ops (AddTagsToResource / ListTagsForResource /
    // RemoveTagsFromResource) declare every per-resource `*NotFoundFault`
    // shape but no generic "bad ARN" code. `DBInstanceNotFoundFault` is
    // the default fallback for malformed/unrecognised ARNs — its wire
    // code (`DBInstanceNotFound`) is declared on all three tag ops.
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "DBInstanceNotFound",
        format!("The specified resource name does not match an RDS resource in this region: {arn}"),
    )
}

/// Parse an RDS ARN's resource-type segment + name. Returns
/// `(resource_type, name)` where `resource_type` is the segment AWS
/// uses to discriminate (`db`, `snapshot`, `cluster`, `pg`, `subgrp`,
/// `og`, `cluster-pg`, `cluster-snapshot`, `secgrp`, etc.). Returns
/// `None` on malformed ARNs.
pub(crate) fn parse_rds_arn(arn: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = arn.split(':').collect();
    if parts.len() < 7 || parts[0] != "arn" || parts[2] != "rds" {
        return None;
    }
    let kind = parts.get(5)?;
    let name = parts.get(6)?;
    if kind.is_empty() || name.is_empty() {
        return None;
    }
    Some((kind.to_string(), name.to_string()))
}

pub(crate) enum TagTargetMut<'a> {
    Vec(&'a mut Vec<RdsTag>),
    Json(&'a mut serde_json::Value),
}

pub(crate) enum TagTargetRef<'a> {
    Vec(&'a Vec<RdsTag>),
    Json(&'a serde_json::Value),
}

impl TagTargetMut<'_> {
    pub fn merge(&mut self, incoming: &[RdsTag]) {
        match self {
            TagTargetMut::Vec(v) => merge_tags(v, incoming),
            TagTargetMut::Json(entry) => {
                let obj = match entry.as_object_mut() {
                    Some(o) => o,
                    None => return,
                };
                if !obj.contains_key("Tags") {
                    obj.insert("Tags".to_string(), serde_json::json!([]));
                }
                let arr = match obj.get_mut("Tags").and_then(|t| t.as_array_mut()) {
                    Some(a) => a,
                    None => return,
                };
                for t in incoming {
                    if let Some(existing) = arr
                        .iter_mut()
                        .find(|v| v.get("Key").and_then(|k| k.as_str()) == Some(t.key.as_str()))
                    {
                        if let Some(o) = existing.as_object_mut() {
                            o.insert("Value".to_string(), serde_json::json!(t.value));
                        }
                    } else {
                        arr.push(serde_json::json!({"Key": t.key, "Value": t.value}));
                    }
                }
            }
        }
    }

    pub fn remove_keys(&mut self, keys: &[String]) {
        match self {
            TagTargetMut::Vec(v) => v.retain(|t| !keys.iter().any(|k| k == &t.key)),
            TagTargetMut::Json(entry) => {
                if let Some(obj) = entry.as_object_mut() {
                    if let Some(arr) = obj.get_mut("Tags").and_then(|t| t.as_array_mut()) {
                        arr.retain(|v| {
                            v.get("Key")
                                .and_then(|k| k.as_str())
                                .is_none_or(|k| !keys.iter().any(|key| key == k))
                        });
                    }
                }
            }
        }
    }
}

impl TagTargetRef<'_> {
    pub fn to_xml(&self) -> String {
        match self {
            TagTargetRef::Vec(v) => v.iter().map(tag_xml).collect(),
            TagTargetRef::Json(entry) => entry
                .get("Tags")
                .and_then(|t| t.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| {
                            let k = v.get("Key").and_then(|k| k.as_str())?;
                            let val = v.get("Value").and_then(|v| v.as_str()).unwrap_or("");
                            Some(tag_xml(&RdsTag {
                                key: k.to_string(),
                                value: val.to_string(),
                            }))
                        })
                        .collect()
                })
                .unwrap_or_default(),
        }
    }
}

fn extras_bucket_for_resource_type(kind: &str) -> Option<&'static str> {
    Some(match kind {
        "cluster" => "clusters",
        "cluster-snapshot" => "cluster_snapshots",
        "cluster-pg" => "cluster_param_groups",
        "og" => "option_groups",
        "secgrp" => "security_groups",
        "es" => "event_subscriptions",
        "db-proxy" => "proxies",
        _ => return None,
    })
}

/// Returns `true` if `kind` is a recognised RDS resource-type segment
/// for tagging operations. Anything not in this list is treated as an
/// invalid ARN by AddTags/ListTags/RemoveTags.
fn is_known_tag_resource_kind(kind: &str) -> bool {
    matches!(
        kind,
        "db" | "snapshot" | "subgrp" | "pg" | "secgrp" | "og" | "es"
    ) || extras_bucket_for_resource_type(kind).is_some()
}

/// Build the per-resource-type NotFound error AWS returns when an ARN
/// is well-formed and the segment is recognised but the named resource
/// doesn't exist in this account/region.
fn resource_not_found_for_kind(kind: &str, name: &str) -> AwsServiceError {
    let (code, msg) = match kind {
        "db" => (
            "DBInstanceNotFound",
            format!("DBInstance {name} not found."),
        ),
        "snapshot" => (
            "DBSnapshotNotFound",
            format!("DBSnapshot {name} not found."),
        ),
        "cluster" => (
            "DBClusterNotFoundFault",
            format!("DBCluster {name} not found."),
        ),
        "cluster-snapshot" => (
            "DBClusterSnapshotNotFoundFault",
            format!("DBClusterSnapshot {name} not found."),
        ),
        "pg" => (
            "DBParameterGroupNotFound",
            format!("DBParameterGroup {name} not found."),
        ),
        "cluster-pg" => (
            "DBParameterGroupNotFound",
            format!("DBClusterParameterGroup {name} not found."),
        ),
        "og" => (
            "OptionGroupNotFoundFault",
            format!("OptionGroup {name} not found."),
        ),
        "subgrp" => (
            "DBSubnetGroupNotFoundFault",
            format!("DBSubnetGroup {name} not found."),
        ),
        "secgrp" => (
            "DBSecurityGroupNotFound",
            format!("DBSecurityGroup {name} not found."),
        ),
        "db-proxy" => ("DBProxyNotFoundFault", format!("DBProxy {name} not found.")),
        "es" => (
            "SubscriptionNotFound",
            format!("EventSubscription {name} not found."),
        ),
        _ => unreachable!("kind already validated by is_known_tag_resource_kind"),
    };
    AwsServiceError::aws_error(StatusCode::NOT_FOUND, code, msg)
}

/// Locate tag storage on whichever RDS resource the ARN points at.
///
/// Returns:
/// - `Ok(target)` when the ARN parses, the kind is one of the 11
///   tagged RDS resource types, and the named resource exists.
/// - `Err(DBInstanceNotFound)` when the ARN can't be parsed or its
///   resource-type segment is not recognised — see
///   `tag_resource_not_found` for why this code is picked.
/// - `Err(<Type>NotFound)` when the kind is recognised but the named
///   resource doesn't exist in this account/region.
///
/// State-backed types (db, snapshot, subgrp, pg) own a typed
/// `Vec<RdsTag>`. Extras-backed types (cluster, cluster-snapshot,
/// cluster-pg, og, secgrp, es, db-proxy) tag a `Tags` array on the
/// stored JSON entry so changes survive serde round-trips.
pub(crate) fn resolve_tag_target_mut<'a>(
    state: &'a mut crate::state::RdsState,
    arn: &str,
) -> Result<TagTargetMut<'a>, AwsServiceError> {
    let (kind, name) = parse_rds_arn(arn).ok_or_else(|| tag_resource_not_found(arn))?;
    if !is_known_tag_resource_kind(&kind) {
        return Err(tag_resource_not_found(arn));
    }
    match kind.as_str() {
        "db" => state
            .instances
            .get_mut(&name)
            .map(|i| TagTargetMut::Vec(&mut i.tags))
            .ok_or_else(|| resource_not_found_for_kind(&kind, &name)),
        "snapshot" => state
            .snapshots
            .get_mut(&name)
            .map(|s| TagTargetMut::Vec(&mut s.tags))
            .ok_or_else(|| resource_not_found_for_kind(&kind, &name)),
        "subgrp" => state
            .subnet_groups
            .get_mut(&name)
            .map(|g| TagTargetMut::Vec(&mut g.tags))
            .ok_or_else(|| resource_not_found_for_kind(&kind, &name)),
        "pg" => state
            .parameter_groups
            .get_mut(&name)
            .map(|g| TagTargetMut::Vec(&mut g.tags))
            .ok_or_else(|| resource_not_found_for_kind(&kind, &name)),
        other => extras_bucket_for_resource_type(other)
            .and_then(|bucket| state.extras.get_mut(bucket))
            .and_then(|map| map.get_mut(&name))
            .map(TagTargetMut::Json)
            .ok_or_else(|| resource_not_found_for_kind(&kind, &name)),
    }
}

pub(crate) fn resolve_tag_target<'a>(
    state: &'a crate::state::RdsState,
    arn: &str,
) -> Result<TagTargetRef<'a>, AwsServiceError> {
    let (kind, name) = parse_rds_arn(arn).ok_or_else(|| tag_resource_not_found(arn))?;
    if !is_known_tag_resource_kind(&kind) {
        return Err(tag_resource_not_found(arn));
    }
    match kind.as_str() {
        "db" => state
            .instances
            .get(&name)
            .map(|i| TagTargetRef::Vec(&i.tags))
            .ok_or_else(|| resource_not_found_for_kind(&kind, &name)),
        "snapshot" => state
            .snapshots
            .get(&name)
            .map(|s| TagTargetRef::Vec(&s.tags))
            .ok_or_else(|| resource_not_found_for_kind(&kind, &name)),
        "subgrp" => state
            .subnet_groups
            .get(&name)
            .map(|g| TagTargetRef::Vec(&g.tags))
            .ok_or_else(|| resource_not_found_for_kind(&kind, &name)),
        "pg" => state
            .parameter_groups
            .get(&name)
            .map(|g| TagTargetRef::Vec(&g.tags))
            .ok_or_else(|| resource_not_found_for_kind(&kind, &name)),
        other => extras_bucket_for_resource_type(other)
            .and_then(|bucket| state.extras.get(bucket))
            .and_then(|map| map.get(&name))
            .map(TagTargetRef::Json)
            .ok_or_else(|| resource_not_found_for_kind(&kind, &name)),
    }
}

/// Drain a `PendingModifiedValues` into its owning `DbInstance`. Used by
/// reboot and `ApplyPendingMaintenanceAction` so deferred Modify changes
/// land on the live struct on the next maintenance window.
pub(crate) fn apply_pending_to_instance(
    instance: &mut DbInstance,
    pending: crate::state::PendingModifiedValues,
) {
    if let Some(class) = pending.db_instance_class {
        instance.db_instance_class = class;
    }
    if let Some(allocated_storage) = pending.allocated_storage {
        instance.allocated_storage = allocated_storage;
    }
    if let Some(backup_retention_period) = pending.backup_retention_period {
        instance.backup_retention_period = backup_retention_period;
    }
    if let Some(multi_az) = pending.multi_az {
        instance.multi_az = multi_az;
    }
    if let Some(engine_version) = pending.engine_version {
        instance.engine_version = engine_version;
    }
    if let Some(master_user_password) = pending.master_user_password {
        instance.master_user_password = master_user_password;
    }
    if let Some(window) = pending.preferred_backup_window {
        instance.preferred_backup_window = window;
    }
    if let Some(window) = pending.preferred_maintenance_window {
        instance.preferred_maintenance_window = Some(window);
    }
    if let Some(name) = pending.db_parameter_group_name {
        instance.db_parameter_group_name = Some(name);
    }
    if let Some(iops) = pending.iops {
        instance.iops = Some(iops);
    }
    if let Some(stype) = pending.storage_type {
        instance.storage_type = Some(stype);
    }
    if let Some(throughput) = pending.storage_throughput {
        instance.storage_throughput = Some(throughput);
    }
    if let Some(interval) = pending.monitoring_interval {
        instance.monitoring_interval = Some(interval);
    }
    if let Some(b) = pending.performance_insights_enabled {
        instance.performance_insights_enabled = b;
    }
    if let Some(types) = pending.enabled_cloudwatch_logs_exports {
        instance.enabled_cloudwatch_logs_exports = types;
    }
    if let Some(lm) = pending.license_model {
        instance.license_model = Some(lm);
    }
    if let Some(b) = pending.multi_tenant {
        instance.multi_tenant = Some(b);
    }
    if let Some(b) = pending.publicly_accessible {
        instance.publicly_accessible = b;
    }
    if let Some(arn) = pending.tde_credential_arn {
        instance.tde_credential_arn = Some(arn);
    }
    if let Some(p) = pending.port {
        instance.port = p;
    }
    if let Some(ca) = pending.ca_certificate_identifier {
        instance.ca_certificate_identifier = Some(ca);
    }
}

pub(crate) fn license_model_for_engine(engine: &str) -> &'static str {
    // Match AWS's reported license model exactly. Oracle and SQL Server
    // both use the BYOL/license-included split; fakecloud reports
    // license-included since the upstream dev-edition images are
    // free-to-use. Db2 is reported as bring-your-own-license to mirror
    // AWS's RDS for Db2 default.
    match engine {
        "mysql" | "mariadb" => "general-public-license",
        "oracle-ee" | "oracle-se2" | "oracle-ee-cdb" | "oracle-se2-cdb" => "license-included",
        "sqlserver-ee" | "sqlserver-se" | "sqlserver-ex" | "sqlserver-web" => "license-included",
        "db2-se" | "db2-ae" => "bring-your-own-license",
        _ => "postgresql-license",
    }
}

pub(crate) fn default_db_name(engine: &str) -> &'static str {
    match engine {
        "mysql" | "mariadb" => "mysql",
        // Oracle's gvenzl image creates an `ORACLE_DATABASE` alongside
        // the built-in FREEPDB1 — keep `ORCL` as the default name to
        // match what AWS RDS for Oracle returns when you don't pass
        // `DBName`.
        "oracle-ee" | "oracle-se2" | "oracle-ee-cdb" | "oracle-se2-cdb" => "ORCL",
        // SQL Server installs system DBs by default; AWS doesn't
        // create a user DB unless `DBName` is supplied. Use `master`
        // as the default the SDK can connect to.
        "sqlserver-ee" | "sqlserver-se" | "sqlserver-ex" | "sqlserver-web" => "master",
        "db2-se" | "db2-ae" => "BLUDB",
        _ => "postgres",
    }
}

/// Pick the port AWS defaults to for a freshly-created instance of
/// `engine`. Mirrors the AWS RDS defaults so client SDKs that connect
/// without an explicit `--port` flag hit the right listener.
pub fn default_port_for_engine(engine: &str) -> i32 {
    match engine {
        "postgres" => 5432,
        "mysql" | "mariadb" => 3306,
        "oracle-ee" | "oracle-se2" | "oracle-ee-cdb" | "oracle-se2-cdb" => 1521,
        "sqlserver-ee" | "sqlserver-se" | "sqlserver-ex" | "sqlserver-web" => 1433,
        "db2-se" | "db2-ae" => 50000,
        _ => 5432,
    }
}

/// Pick the default `EngineVersion` for `engine` when the caller omits
/// it. Must land on a version in that engine's supported list
/// (`validate_create_request`) -- a fixed postgres default like `16.3`
/// would make every version-less mysql/mariadb/oracle/... create fail
/// with "EngineVersion '16.3' is not available". See issue #2107.
pub(crate) fn default_engine_version(engine: &str) -> &'static str {
    match engine {
        "postgres" => "16.3",
        "mysql" => "8.0",
        "mariadb" => "10.11",
        "oracle-ee" | "oracle-se2" | "oracle-ee-cdb" | "oracle-se2-cdb" => "19.0.0",
        "sqlserver-ee" | "sqlserver-se" | "sqlserver-ex" | "sqlserver-web" => "15.00.4322.2.v1",
        "db2-se" | "db2-ae" => "11.5.9.0.sb00000000.r1",
        _ => "16.3",
    }
}

/// Pick the built-in parameter group name AWS assigns to a new
/// instance when the caller doesn't override it. The name encodes the
/// engine family plus its major version (e.g. `default.postgres16`,
/// `default.mysql8.0`, `default.oracle-ee-23`, `default.sqlserver-ex-16`,
/// `default.db2-se-11.5`).
pub(crate) fn default_parameter_group(engine: &str, engine_version: &str) -> String {
    match engine {
        "postgres" => {
            let major = engine_version.split('.').next().unwrap_or("16");
            format!("default.postgres{}", major)
        }
        "mysql" => {
            let major = if engine_version.starts_with("5.7") {
                "5.7"
            } else {
                "8.0"
            };
            format!("default.mysql{}", major)
        }
        "mariadb" => {
            let major = if engine_version.starts_with("11.4") {
                "11.4"
            } else if engine_version.starts_with("10.11") {
                "10.11"
            } else {
                "10.6"
            };
            format!("default.mariadb{}", major)
        }
        "oracle-ee" | "oracle-se2" | "oracle-ee-cdb" | "oracle-se2-cdb" => {
            let major = engine_version.split('.').next().unwrap_or("23");
            format!("default.{engine}-{major}")
        }
        "sqlserver-ee" | "sqlserver-se" | "sqlserver-ex" | "sqlserver-web" => {
            // AWS uses the SQL Server major-version number ("16" for
            // 2022, "15" for 2019) in the default parameter group.
            let major = engine_version.split('.').next().unwrap_or("16");
            format!("default.{engine}-{major}")
        }
        "db2-se" | "db2-ae" => {
            // Db2 ships major.minor as the parameter-group key
            // (e.g. `default.db2-se-11.5`).
            let mut parts = engine_version.split('.');
            let major = parts.next().unwrap_or("11");
            let minor = parts.next().unwrap_or("5");
            format!("default.{engine}-{major}.{minor}")
        }
        _ => "default.postgres16".to_string(),
    }
}

pub(crate) fn runtime_error_to_service_error(error: RuntimeError) -> AwsServiceError {
    // `InsufficientDBInstanceCapacity` is the closest Smithy-declared
    // shape across the Create/Modify/Restore* DB instance ops that call
    // this helper. Container start failures map to `InternalFailure`
    // which all services accept as a framework-level error.
    match error {
        RuntimeError::Unavailable => AwsServiceError::aws_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "InsufficientDBInstanceCapacity",
            format!(
                "Docker/Podman is required for RDS DB instances but is not available. {}",
                fakecloud_core::container_net::CONTAINER_RUNTIME_HINT
            ),
        ),
        RuntimeError::ContainerStartFailed(message) => AwsServiceError::aws_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "InternalFailure",
            message,
        ),
    }
}
