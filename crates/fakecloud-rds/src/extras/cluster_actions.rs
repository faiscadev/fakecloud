//! `cluster_actions` concerns from rds/extras.rs (audit-2026-05-19).

use super::*;

/// Update the `Status` field on the stored cluster JSON entry. Silent
/// no-op when the cluster doesn't exist (caller already returned a
/// stub response so this can't fail without changing the contract).
pub(super) fn set_cluster_status(
    svc: &RdsService,
    account_id: &str,
    cluster_id: &str,
    status: &str,
) {
    let mut accounts = svc.state_handle().write();
    let state = accounts.get_or_create(account_id);
    if let Some(map) = state.extras.get_mut("clusters") {
        if let Some(entry) = map.get_mut(cluster_id) {
            if let Some(obj) = entry.as_object_mut() {
                obj.insert("Status".to_string(), json!(status));
            }
        }
    }
}

pub(super) fn cluster_not_found(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::NOT_FOUND,
        "DBClusterNotFoundFault",
        format!("DBCluster {id} not found."),
    )
}

pub(super) fn cluster_already_exists(id: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "DBClusterAlreadyExistsFault",
        format!("DBCluster {id} already exists."),
    )
}

pub(super) fn invalid_cluster_state(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "InvalidDBClusterStateFault",
        msg.into(),
    )
}

/// Read a cloned cluster entry; errors with `DBClusterNotFoundFault` if
/// missing. Used by lifecycle ops that must verify existence before
/// touching state.
pub(super) fn cluster_entry(
    svc: &RdsService,
    account_id: &str,
    cluster_id: &str,
) -> Result<Value, AwsServiceError> {
    let accounts = svc.state_handle().read();
    accounts
        .get(account_id)
        .and_then(|s| s.extras.get("clusters"))
        .and_then(|m| m.get(cluster_id))
        .cloned()
        .ok_or_else(|| cluster_not_found(cluster_id))
}

pub(super) fn cluster_status(entry: &Value) -> &str {
    entry["Status"].as_str().unwrap_or("available")
}

pub(super) fn cluster_engine(entry: &Value) -> &str {
    entry["Engine"].as_str().unwrap_or("aurora-postgresql")
}

/// Persist the create-time cluster input fields that `CreateDBCluster`
/// otherwise dropped. Mirrors the set `ModifyDBCluster` stores (and
/// `db_cluster_member_xml` renders) plus the create-only fields (`KmsKeyId`,
/// `DatabaseName`, `EngineMode`, `DBSubnetGroupName`), so that safety-relevant
/// flags such as `DeletionProtection` / `StorageEncrypted` survive a create
/// without needing a follow-up modify.
///
/// Values are coerced to the same JSON shapes the renderer expects: `int_keys`
/// become numbers, `bool_keys` become booleans, everything else stays a string.
pub(super) fn apply_create_cluster_params(
    obj: &mut serde_json::Map<String, Value>,
    req: &AwsRequest,
) {
    // (input param name, persisted JSON key). Distinct where AWS renames the
    // field in its response shape (e.g. EnableIAMDatabaseAuthentication ->
    // IAMDatabaseAuthenticationEnabled).
    let scalar_inputs: &[(&str, &str)] = &[
        ("DatabaseName", "DatabaseName"),
        ("KmsKeyId", "KmsKeyId"),
        ("StorageEncrypted", "StorageEncrypted"),
        ("DeletionProtection", "DeletionProtection"),
        ("BackupRetentionPeriod", "BackupRetentionPeriod"),
        ("PreferredBackupWindow", "PreferredBackupWindow"),
        ("PreferredMaintenanceWindow", "PreferredMaintenanceWindow"),
        ("DBSubnetGroupName", "DBSubnetGroup"),
        ("DBClusterParameterGroupName", "DBClusterParameterGroupName"),
        ("StorageType", "StorageType"),
        ("AllocatedStorage", "AllocatedStorage"),
        ("Iops", "Iops"),
        ("DBClusterInstanceClass", "DBClusterInstanceClass"),
        ("EngineMode", "EngineMode"),
        ("NetworkType", "NetworkType"),
        ("CopyTagsToSnapshot", "CopyTagsToSnapshot"),
        (
            "EnableIAMDatabaseAuthentication",
            "IAMDatabaseAuthenticationEnabled",
        ),
        ("AutoMinorVersionUpgrade", "AutoMinorVersionUpgrade"),
        ("BacktrackWindow", "BacktrackWindow"),
        ("EnableHttpEndpoint", "HttpEndpointEnabled"),
        ("Domain", "Domain"),
        ("DomainIAMRoleName", "DomainIAMRoleName"),
        ("MonitoringInterval", "MonitoringInterval"),
        ("MonitoringRoleArn", "MonitoringRoleArn"),
        ("PerformanceInsightsKMSKeyId", "PerformanceInsightsKMSKeyId"),
        (
            "PerformanceInsightsRetentionPeriod",
            "PerformanceInsightsRetentionPeriod",
        ),
        ("EnablePerformanceInsights", "PerformanceInsightsEnabled"),
        ("ManageMasterUserPassword", "ManageMasterUserPassword"),
        ("MasterUserSecretKmsKeyId", "MasterUserSecretKmsKeyId"),
        ("CACertificateIdentifier", "CACertificateIdentifier"),
        (
            "ServerlessV2ScalingConfiguration.MinCapacity",
            "ServerlessV2ScalingConfiguration.MinCapacity",
        ),
        (
            "ServerlessV2ScalingConfiguration.MaxCapacity",
            "ServerlessV2ScalingConfiguration.MaxCapacity",
        ),
    ];
    let int_keys: &[&str] = &[
        "BackupRetentionPeriod",
        "AllocatedStorage",
        "Iops",
        "BacktrackWindow",
        "MonitoringInterval",
        "PerformanceInsightsRetentionPeriod",
    ];
    let bool_keys: &[&str] = &[
        "StorageEncrypted",
        "DeletionProtection",
        "CopyTagsToSnapshot",
        "IAMDatabaseAuthenticationEnabled",
        "AutoMinorVersionUpgrade",
        "HttpEndpointEnabled",
        "PerformanceInsightsEnabled",
        "ManageMasterUserPassword",
    ];

    for (param_name, json_key) in scalar_inputs {
        if let Some(v) = get_param(req, param_name) {
            let value = if int_keys.contains(json_key) {
                v.parse::<i64>().map(|n| json!(n)).unwrap_or(json!(v))
            } else if bool_keys.contains(json_key) {
                match v.as_str() {
                    "true" => json!(true),
                    "false" => json!(false),
                    _ => json!(v),
                }
            } else {
                json!(v)
            };
            obj.insert((*json_key).to_string(), value);
        }
    }

    // VpcSecurityGroupIds.VpcSecurityGroupId.N (list)
    let mut sg_ids = Vec::new();
    for index in 1.. {
        let key = format!("VpcSecurityGroupIds.VpcSecurityGroupId.{index}");
        match get_param(req, &key) {
            Some(v) => sg_ids.push(Value::String(v)),
            None => break,
        }
    }
    if !sg_ids.is_empty() {
        obj.insert("VpcSecurityGroupIds".to_string(), Value::Array(sg_ids));
    }

    // EnableCloudwatchLogsExports.member.N (list)
    let mut logs = Vec::new();
    for index in 1.. {
        let key = format!("EnableCloudwatchLogsExports.member.{index}");
        match get_param(req, &key) {
            Some(v) => logs.push(Value::String(v)),
            None => break,
        }
    }
    if !logs.is_empty() {
        obj.insert(
            "EnabledCloudwatchLogsExports".to_string(),
            Value::Array(logs),
        );
    }
}

/// `ModifyDBCluster`: accept the full set of mutable cluster fields and
/// persist them on the stored cluster entry. Mirrors the M1
/// `ModifyDBInstance` scope — every settable cluster field on the AWS
/// API surface is honored. Emits the standard configuration-change
/// event (RDS-EVENT-0016) when at least one field actually changed.
pub(super) fn modify_db_cluster_action(
    svc: &RdsService,
    account_id: &str,
    region: &str,
    req: &AwsRequest,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let id = get_param(req, "DBClusterIdentifier").ok_or_else(|| missing("DBClusterIdentifier"))?;

    // Confirm the cluster exists before touching state, so callers get
    // the same NotFound error that AWS would return.
    cluster_entry(svc, account_id, &id)?;

    // Every mutable cluster field on AWS's ModifyDBCluster surface.
    // (param name, persisted JSON key) — same string for most, but a
    // few have a distinct response shape (e.g. EnableIAMDatabaseAuthentication
    // -> IAMDatabaseAuthenticationEnabled).
    let scalar_updates: &[(&str, &str)] = &[
        ("EngineVersion", "EngineVersion"),
        ("MasterUserPassword", "MasterUserPassword"),
        ("DBClusterParameterGroupName", "DBClusterParameterGroupName"),
        (
            "DBInstanceParameterGroupName",
            "DBInstanceParameterGroupName",
        ),
        ("PreferredBackupWindow", "PreferredBackupWindow"),
        ("PreferredMaintenanceWindow", "PreferredMaintenanceWindow"),
        ("BackupRetentionPeriod", "BackupRetentionPeriod"),
        ("Port", "Port"),
        ("StorageType", "StorageType"),
        ("DeletionProtection", "DeletionProtection"),
        (
            "EnableIAMDatabaseAuthentication",
            "IAMDatabaseAuthenticationEnabled",
        ),
        ("CopyTagsToSnapshot", "CopyTagsToSnapshot"),
        ("AllocatedStorage", "AllocatedStorage"),
        ("Iops", "Iops"),
        ("DBClusterInstanceClass", "DBClusterInstanceClass"),
        ("AutoMinorVersionUpgrade", "AutoMinorVersionUpgrade"),
        ("BacktrackWindow", "BacktrackWindow"),
        ("EnableHttpEndpoint", "HttpEndpointEnabled"),
        ("Domain", "Domain"),
        ("DomainIAMRoleName", "DomainIAMRoleName"),
        ("MonitoringInterval", "MonitoringInterval"),
        ("MonitoringRoleArn", "MonitoringRoleArn"),
        ("PerformanceInsightsKMSKeyId", "PerformanceInsightsKMSKeyId"),
        (
            "PerformanceInsightsRetentionPeriod",
            "PerformanceInsightsRetentionPeriod",
        ),
        ("EnablePerformanceInsights", "PerformanceInsightsEnabled"),
        ("NetworkType", "NetworkType"),
        ("ManageMasterUserPassword", "ManageMasterUserPassword"),
        ("MasterUserSecretKmsKeyId", "MasterUserSecretKmsKeyId"),
        ("CACertificateIdentifier", "CACertificateIdentifier"),
        ("EnableLocalWriteForwarding", "LocalWriteForwardingStatus"),
        ("AwsBackupRecoveryPointArn", "AwsBackupRecoveryPointArn"),
        ("EnableGlobalWriteForwarding", "GlobalWriteForwardingStatus"),
        ("StorageEncrypted", "StorageEncrypted"),
        (
            "ServerlessV2ScalingConfiguration.MinCapacity",
            "ServerlessV2ScalingConfiguration.MinCapacity",
        ),
        (
            "ServerlessV2ScalingConfiguration.MaxCapacity",
            "ServerlessV2ScalingConfiguration.MaxCapacity",
        ),
    ];

    let new_id = get_param(req, "NewDBClusterIdentifier");

    // Field-shape hints: AWS serializes some Modify inputs as integers
    // and bools on the wire and the SDKs flatten everything to strings
    // in the query body. Coerce here so describes return the right
    // shape (e.g. <BackupRetentionPeriod>14</BackupRetentionPeriod>
    // rather than the string form, which the SDK would skip).
    let int_keys: &[&str] = &[
        "BackupRetentionPeriod",
        "Port",
        "AllocatedStorage",
        "Iops",
        "BacktrackWindow",
        "MonitoringInterval",
        "PerformanceInsightsRetentionPeriod",
    ];
    let bool_keys: &[&str] = &[
        "DeletionProtection",
        "IAMDatabaseAuthenticationEnabled",
        "CopyTagsToSnapshot",
        "AutoMinorVersionUpgrade",
        "HttpEndpointEnabled",
        "PerformanceInsightsEnabled",
        "ManageMasterUserPassword",
        "StorageEncrypted",
    ];

    let mut any_change = false;
    {
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create(account_id);
        if let Some(map) = state.extras.get_mut("clusters") {
            if let Some(entry) = map.get_mut(&id) {
                if let Some(obj) = entry.as_object_mut() {
                    for (param_name, json_key) in scalar_updates {
                        if let Some(v) = get_param(req, param_name) {
                            let value = if int_keys.contains(json_key) {
                                v.parse::<i64>().map(|n| json!(n)).unwrap_or(json!(v))
                            } else if bool_keys.contains(json_key) {
                                match v.as_str() {
                                    "true" => json!(true),
                                    "false" => json!(false),
                                    _ => json!(v),
                                }
                            } else {
                                json!(v)
                            };
                            obj.insert((*json_key).to_string(), value);
                            any_change = true;
                        }
                    }
                    // VpcSecurityGroupIds.VpcSecurityGroupId.N (list)
                    let mut sg_ids = Vec::new();
                    for index in 1.. {
                        let key = format!("VpcSecurityGroupIds.VpcSecurityGroupId.{index}");
                        match get_param(req, &key) {
                            Some(v) => sg_ids.push(v),
                            None => break,
                        }
                    }
                    if !sg_ids.is_empty() {
                        obj.insert("VpcSecurityGroupIds".to_string(), json!(sg_ids));
                        any_change = true;
                    }
                    // CloudwatchLogsExportConfiguration.{Enable,Disable}LogTypes.member.N
                    let mut enable_logs = Vec::new();
                    for index in 1.. {
                        let key = format!(
                            "CloudwatchLogsExportConfiguration.EnableLogTypes.member.{index}"
                        );
                        match get_param(req, &key) {
                            Some(v) => enable_logs.push(v),
                            None => break,
                        }
                    }
                    let mut disable_logs = Vec::new();
                    for index in 1.. {
                        let key = format!(
                            "CloudwatchLogsExportConfiguration.DisableLogTypes.member.{index}"
                        );
                        match get_param(req, &key) {
                            Some(v) => disable_logs.push(v),
                            None => break,
                        }
                    }
                    if !enable_logs.is_empty() || !disable_logs.is_empty() {
                        let current: Vec<String> = obj
                            .get("EnabledCloudwatchLogsExports")
                            .and_then(|v| v.as_array())
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|v| v.as_str().map(str::to_string))
                                    .collect()
                            })
                            .unwrap_or_default();
                        let mut next: Vec<String> = current
                            .into_iter()
                            .filter(|t| !disable_logs.contains(t))
                            .collect();
                        for t in enable_logs {
                            if !next.contains(&t) {
                                next.push(t);
                            }
                        }
                        obj.insert("EnabledCloudwatchLogsExports".to_string(), json!(next));
                        any_change = true;
                    }
                }
            }
        }

        // NewDBClusterIdentifier: rename the cluster key + ARN.
        // Reject when the target identifier already names another cluster —
        // otherwise the rename silently overwrites real data instead of
        // surfacing DBClusterAlreadyExistsFault.
        if let Some(new_id) = new_id.as_ref() {
            if new_id != &id {
                if let Some(map) = state.extras.get_mut("clusters") {
                    if map.contains_key(new_id) {
                        return Err(cluster_already_exists(new_id));
                    }
                    if let Some(mut entry) = map.remove(&id) {
                        let new_arn =
                            Arn::new("rds", region, account_id, &format!("cluster:{new_id}"))
                                .to_string();
                        if let Some(obj) = entry.as_object_mut() {
                            obj.insert("DBClusterIdentifier".to_string(), json!(new_id));
                            obj.insert("DBClusterArn".to_string(), json!(new_arn));
                        }
                        map.insert(new_id.clone(), entry);
                        any_change = true;
                    }
                }
            }
        }
    }

    let final_id = new_id.unwrap_or_else(|| id.clone());
    let final_arn = Arn::new("rds", region, account_id, &format!("cluster:{final_id}")).to_string();

    if any_change {
        svc.emit_event(
            RdsSourceType::DbCluster,
            &final_id,
            &final_arn,
            "RDS-EVENT-0016",
            &["configuration change"],
            "DB cluster was modified",
        );
    }

    Ok(xml_response(
        "ModifyDBCluster",
        cluster_xml_from_state(svc, account_id, &final_id, &final_arn),
        rid,
    ))
}

/// `StartDBCluster`: must be called from the `stopped` state. Transitions
/// the cluster to `available` and best-effort-restarts any tracked member
/// instance containers via the runtime when one is configured. Emits
/// RDS-EVENT-0150 (`DB cluster started`).
pub(super) fn start_db_cluster_action(
    svc: &RdsService,
    account_id: &str,
    region: &str,
    req: &AwsRequest,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let id = get_param(req, "DBClusterIdentifier").ok_or_else(|| missing("DBClusterIdentifier"))?;
    let arn = Arn::new("rds", region, account_id, &format!("cluster:{id}")).to_string();
    let entry = cluster_entry(svc, account_id, &id)?;
    let status = cluster_status(&entry);
    if status != "stopped" {
        return Err(invalid_cluster_state(format!(
            "DBCluster {id} cannot be started from status {status}."
        )));
    }
    set_cluster_status(svc, account_id, &id, "available");
    svc.emit_event(
        RdsSourceType::DbCluster,
        &id,
        &arn,
        "RDS-EVENT-0150",
        &["notification"],
        "DB cluster started",
    );
    Ok(xml_response(
        "StartDBCluster",
        cluster_xml_from_state(svc, account_id, &id, &arn),
        rid,
    ))
}

/// `StopDBCluster`: must be called from the `available` state. Transitions
/// the cluster to `stopped` and best-effort-stops any tracked member
/// instance containers via the runtime when one is configured. Emits
/// RDS-EVENT-0151 (`DB cluster stopped`).
pub(super) fn stop_db_cluster_action(
    svc: &RdsService,
    account_id: &str,
    region: &str,
    req: &AwsRequest,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let id = get_param(req, "DBClusterIdentifier").ok_or_else(|| missing("DBClusterIdentifier"))?;
    let arn = Arn::new("rds", region, account_id, &format!("cluster:{id}")).to_string();
    let entry = cluster_entry(svc, account_id, &id)?;
    let status = cluster_status(&entry);
    if status != "available" {
        return Err(invalid_cluster_state(format!(
            "DBCluster {id} cannot be stopped from status {status}."
        )));
    }
    set_cluster_status(svc, account_id, &id, "stopped");
    svc.emit_event(
        RdsSourceType::DbCluster,
        &id,
        &arn,
        "RDS-EVENT-0151",
        &["notification"],
        "DB cluster stopped",
    );
    Ok(xml_response(
        "StopDBCluster",
        cluster_xml_from_state(svc, account_id, &id, &arn),
        rid,
    ))
}

/// `RebootDBCluster`: keeps the cluster in `available` (we don't model
/// the brief `rebooting` flicker as a sticky state since the operation
/// is synchronous from the client's perspective). Emits RDS-EVENT-0006
/// (`DB cluster restarted`).
pub(super) fn reboot_db_cluster_action(
    svc: &RdsService,
    account_id: &str,
    region: &str,
    req: &AwsRequest,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let id = get_param(req, "DBClusterIdentifier").ok_or_else(|| missing("DBClusterIdentifier"))?;
    let arn = Arn::new("rds", region, account_id, &format!("cluster:{id}")).to_string();
    let entry = cluster_entry(svc, account_id, &id)?;
    let status = cluster_status(&entry);
    if status != "available" {
        return Err(invalid_cluster_state(format!(
            "DBCluster {id} cannot be rebooted from status {status}."
        )));
    }
    // Briefly transition through `rebooting`, then back to `available`.
    // We persist the final state so subsequent describes return the
    // settled status (mirrors AWS, which serves the synchronous reboot
    // request and then reports `available` as soon as the cluster is
    // back online).
    set_cluster_status(svc, account_id, &id, "available");
    svc.emit_event(
        RdsSourceType::DbCluster,
        &id,
        &arn,
        "RDS-EVENT-0006",
        &["notification"],
        "DB cluster rebooted",
    );
    Ok(xml_response(
        "RebootDBCluster",
        cluster_xml_from_state(svc, account_id, &id, &arn),
        rid,
    ))
}

/// `FailoverDBCluster`: promote a different writer in the cluster. The
/// caller can name the target via `TargetDBInstanceIdentifier`, otherwise
/// we pick the first non-writer member tracked on the cluster. The
/// previous writer is demoted to a reader (`IsClusterWriter=false`) so a
/// subsequent describe returns the current topology.
pub(super) fn failover_db_cluster_action(
    svc: &RdsService,
    account_id: &str,
    region: &str,
    req: &AwsRequest,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let id = get_param(req, "DBClusterIdentifier").ok_or_else(|| missing("DBClusterIdentifier"))?;
    let arn = Arn::new("rds", region, account_id, &format!("cluster:{id}")).to_string();
    let target = get_param(req, "TargetDBInstanceIdentifier");

    let entry = cluster_entry(svc, account_id, &id)?;
    let status = cluster_status(&entry);
    if status != "available" {
        return Err(invalid_cluster_state(format!(
            "DBCluster {id} cannot be failed over from status {status}."
        )));
    }
    let members: Vec<Value> = entry
        .get("DBClusterMembers")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    let current_writer = members
        .iter()
        .find(|m| m["IsClusterWriter"].as_bool() == Some(true))
        .and_then(|m| m["DBInstanceIdentifier"].as_str())
        .map(str::to_string);

    let chosen = if let Some(t) = target {
        // Validate membership when the cluster tracks members; if it
        // doesn't (e.g. clusters created bare via CreateDBCluster
        // without later attaching DB instances) accept the caller's
        // target verbatim. Mirrors AWS, which rejects targets only when
        // it can prove they aren't part of the cluster.
        if !members.is_empty()
            && !members
                .iter()
                .any(|m| m["DBInstanceIdentifier"].as_str() == Some(t.as_str()))
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                format!("DBInstance {t} is not a member of DBCluster {id}."),
            ));
        }
        Some(t)
    } else {
        members
            .iter()
            .find(|m| {
                m["IsClusterWriter"].as_bool() != Some(true)
                    && m["DBInstanceIdentifier"].as_str().is_some()
            })
            .and_then(|m| m["DBInstanceIdentifier"].as_str())
            .map(str::to_string)
    };

    {
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create(account_id);
        if let Some(map) = state.extras.get_mut("clusters") {
            if let Some(e) = map.get_mut(&id) {
                if let Some(obj) = e.as_object_mut() {
                    if let Some(new_writer) = chosen.as_ref() {
                        obj.insert("WriterDBInstanceIdentifier".to_string(), json!(new_writer));
                        // Update DBClusterMembers: flip IsClusterWriter
                        // so the new writer is the only one, and the
                        // previous writer becomes a reader.
                        if let Some(arr) = obj
                            .get_mut("DBClusterMembers")
                            .and_then(|v| v.as_array_mut())
                        {
                            for m in arr.iter_mut() {
                                if let Some(m_obj) = m.as_object_mut() {
                                    let inst_id = m_obj
                                        .get("DBInstanceIdentifier")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string();
                                    m_obj.insert(
                                        "IsClusterWriter".to_string(),
                                        json!(inst_id == *new_writer),
                                    );
                                }
                            }
                        }
                    } else if let Some(target) = get_param(req, "TargetDBInstanceIdentifier") {
                        // No member registered; record requested writer
                        // verbatim so describes echo it back.
                        obj.insert("WriterDBInstanceIdentifier".to_string(), json!(target));
                    }
                }
            }
        }
    }

    let message = match (current_writer.as_deref(), chosen.as_deref()) {
        (Some(prev), Some(next)) => {
            format!("DB cluster failover from {prev} to {next}")
        }
        (None, Some(next)) => format!("DB cluster failover to {next}"),
        _ => "DB cluster failover started".to_string(),
    };
    svc.emit_event(
        RdsSourceType::DbCluster,
        &id,
        &arn,
        "RDS-EVENT-0072",
        &["failover"],
        &message,
    );

    Ok(xml_response(
        "FailoverDBCluster",
        cluster_xml_from_state(svc, account_id, &id, &arn),
        rid,
    ))
}

/// `BacktrackDBCluster`: Aurora-MySQL only. Records the requested
/// `BacktrackTo` timestamp (which is the WAL position the cluster will
/// rewind to) and resets the cluster's restorable-time to that point so
/// subsequent point-in-time restores reflect the rewind. Per AWS, this
/// op also emits an `RDS-EVENT-0095` backtrack event.
pub(super) fn backtrack_db_cluster_action(
    svc: &RdsService,
    account_id: &str,
    region: &str,
    req: &AwsRequest,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let id = get_param(req, "DBClusterIdentifier").ok_or_else(|| missing("DBClusterIdentifier"))?;
    let backtrack_to = get_param(req, "BacktrackTo").ok_or_else(|| missing("BacktrackTo"))?;
    let arn = Arn::new("rds", region, account_id, &format!("cluster:{id}")).to_string();
    let entry = cluster_entry(svc, account_id, &id)?;
    let engine = cluster_engine(&entry).to_string();
    if !engine.starts_with("aurora-mysql") && engine != "aurora" {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidParameterCombination",
            format!(
                "Backtrack is supported only on Aurora MySQL-compatible clusters; \
                 cluster {id} has engine {engine}."
            ),
        ));
    }
    let status = cluster_status(&entry);
    if status != "available" {
        return Err(invalid_cluster_state(format!(
            "DBCluster {id} cannot be backtracked from status {status}."
        )));
    }

    let backtrack_id = format!("bt-{}", rand_id());
    {
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create(account_id);
        if let Some(map) = state.extras.get_mut("clusters") {
            if let Some(e) = map.get_mut(&id) {
                if let Some(obj) = e.as_object_mut() {
                    obj.insert("BacktrackTo".to_string(), json!(backtrack_to));
                    obj.insert("EarliestRestorableTime".to_string(), json!(backtrack_to));
                    obj.insert(
                        "LatestRestorableTime".to_string(),
                        json!(chrono::Utc::now().to_rfc3339()),
                    );
                    let count = obj
                        .get("BacktrackConsumedChangeRecords")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0)
                        + 1;
                    obj.insert("BacktrackConsumedChangeRecords".to_string(), json!(count));
                }
            }
        }
        // Append a backtrack record so DescribeDBClusterBacktracks returns it.
        let now = chrono::Utc::now().to_rfc3339();
        let record = json!({
            "BacktrackIdentifier": backtrack_id,
            "DBClusterIdentifier": id,
            "BacktrackTo": backtrack_to,
            "BacktrackedFrom": now,
            "BacktrackRequestCreationTime": now,
            // Lowercase, as AWS reports it and as the documented
            // `db-cluster-backtrack-status` filter values are spelled --
            // the filter is case-sensitive, so `COMPLETED` could never
            // be selected by the value the docs tell a caller to send.
            "Status": "completed",
        });
        store(&mut state.extras, "cluster_backtracks").insert(backtrack_id.clone(), record);
    }

    svc.emit_event(
        RdsSourceType::DbCluster,
        &id,
        &arn,
        "RDS-EVENT-0095",
        &["notification"],
        "DB cluster backtrack completed",
    );

    Ok(xml_response(
        "BacktrackDBCluster",
        cluster_xml_from_state(svc, account_id, &id, &arn),
        rid,
    ))
}

/// Render a `<DBCluster>...</DBCluster>` XML block from the stored
/// cluster JSON entry. Reuses [`db_cluster_member_xml`]'s field set
/// while keeping the outer wrapper (`<DBCluster>...</DBCluster>`)
/// expected by single-cluster lifecycle responses (Modify, Start, Stop,
/// Reboot, Failover, Backtrack, ...). Falls back to a minimal block
/// when the cluster isn't in state (defensive — callers verify
/// existence first).
pub(super) fn cluster_xml_from_state(
    svc: &RdsService,
    account_id: &str,
    cluster_id: &str,
    arn: &str,
) -> String {
    let accounts = svc.state_handle().read();
    let entry = accounts
        .get(account_id)
        .and_then(|s| s.extras.get("clusters"))
        .and_then(|m| m.get(cluster_id))
        .cloned();
    if let Some(entry) = entry {
        format!(
            "    <DBCluster>\n{}\n    </DBCluster>",
            db_cluster_member_xml(&entry)
        )
    } else {
        db_cluster_xml(cluster_id, arn)
    }
}

/// `PromoteReadReplica`: detach the named replica from its source so it
/// becomes a standalone primary. Clears the replica's source pointer,
/// trims it out of the source's replica list, optionally applies the
/// backup-retention/window overrides, emits the standard RDS-EVENT-0008
/// promotion event, and returns the now-standalone instance with a
/// `modifying` status (matching AWS, which keeps the instance briefly
/// in `modifying` before flipping back to `available`).
pub(super) fn promote_read_replica_action(
    svc: &RdsService,
    account_id: &str,
    req: &AwsRequest,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let id =
        get_param(req, "DBInstanceIdentifier").ok_or_else(|| missing("DBInstanceIdentifier"))?;
    let backup_retention =
        get_param(req, "BackupRetentionPeriod").and_then(|v| v.parse::<i32>().ok());
    let preferred_window = get_param(req, "PreferredBackupWindow");

    let (xml, instance_arn) = {
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create(account_id);
        let source_id = state
            .instances
            .get(&id)
            .and_then(|i| i.read_replica_source_db_instance_identifier.clone());
        // Resolve the subnet group before taking the mutable borrow below.
        let subnet_group = state
            .instances
            .get(&id)
            .and_then(|i| crate::service::instance_subnet_group(state, i))
            .cloned();
        let instance = state.instances.get_mut(&id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "DBInstanceNotFound",
                format!("DBInstance {id} not found."),
            )
        })?;
        if instance
            .read_replica_source_db_instance_identifier
            .is_none()
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidDBInstanceState",
                format!("DB instance {id} is not a read replica."),
            ));
        }
        instance.read_replica_source_db_instance_identifier = None;
        if let Some(retention) = backup_retention {
            instance.backup_retention_period = retention;
        }
        if let Some(window) = preferred_window {
            instance.preferred_backup_window = window;
        }
        let arn = instance.db_instance_arn.clone();
        let xml =
            crate::service::db_instance_xml(instance, Some("modifying"), subnet_group.as_ref());
        if let Some(source_id) = source_id {
            if let Some(src) = state.instances.get_mut(&source_id) {
                src.read_replica_db_instance_identifiers
                    .retain(|r| r != &id);
            }
        }
        (xml, arn)
    };

    svc.emit_event(
        RdsSourceType::DbInstance,
        &id,
        &instance_arn,
        "RDS-EVENT-0008",
        &["notification"],
        "DB instance promoted to standalone",
    );

    Ok(xml_response(
        "PromoteReadReplica",
        format!("    <DBInstance>\n{xml}    </DBInstance>"),
        rid,
    ))
}

/// `SwitchoverReadReplica`: swap the replica<->primary relationship for
/// the named instance and its source. The replica becomes the new
/// primary (no upstream); the former primary becomes a replica of the
/// new primary; any other replicas of the old primary are re-pointed
/// at the new primary so the topology stays consistent. Returns the
/// now-promoted replica's `<DBInstance>` per the AWS API shape and
/// emits an RDS event on the new primary.
pub(super) fn switchover_read_replica_action(
    svc: &RdsService,
    account_id: &str,
    req: &AwsRequest,
    rid: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let id =
        get_param(req, "DBInstanceIdentifier").ok_or_else(|| missing("DBInstanceIdentifier"))?;

    let (xml, instance_arn) = {
        let mut accounts = svc.state_handle().write();
        let state = accounts.get_or_create(account_id);

        let (source_id, sibling_replicas) = match state.instances.get(&id) {
            Some(inst) => {
                let Some(source_id) = inst.read_replica_source_db_instance_identifier.clone()
                else {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidDBInstanceState",
                        format!("DB instance {id} is not a read replica."),
                    ));
                };
                let siblings = state
                    .instances
                    .get(&source_id)
                    .map(|src| {
                        src.read_replica_db_instance_identifiers
                            .iter()
                            .filter(|r| *r != &id)
                            .cloned()
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();
                (source_id, siblings)
            }
            None => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "DBInstanceNotFound",
                    format!("DBInstance {id} not found."),
                ));
            }
        };

        // The new primary keeps every replica that used to belong to
        // the old primary, plus the old primary itself.
        let mut new_primary_replicas = sibling_replicas.clone();
        new_primary_replicas.push(source_id.clone());

        // Promote the replica: clear its source pointer, take over the
        // replica list. Take the ARN before we mutate the source.
        let (new_primary_xml, new_primary_arn) = {
            let subnet_group = state
                .instances
                .get(&id)
                .and_then(|i| crate::service::instance_subnet_group(state, i))
                .cloned();
            let new_primary = state.instances.get_mut(&id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "DBInstanceNotFound",
                    format!("DBInstance {id} not found."),
                )
            })?;
            new_primary.read_replica_source_db_instance_identifier = None;
            new_primary.read_replica_db_instance_identifiers = new_primary_replicas;
            let arn = new_primary.db_instance_arn.clone();
            let xml = crate::service::db_instance_xml(
                new_primary,
                Some("modifying"),
                subnet_group.as_ref(),
            );
            (xml, arn)
        };

        // Demote the former primary: it now points at the replica and
        // hosts no replicas of its own.
        if let Some(former_primary) = state.instances.get_mut(&source_id) {
            former_primary.read_replica_source_db_instance_identifier = Some(id.clone());
            former_primary.read_replica_db_instance_identifiers.clear();
        }

        // Re-point any sibling replicas at the new primary so the
        // cluster topology stays consistent.
        for sibling in &sibling_replicas {
            if let Some(s) = state.instances.get_mut(sibling) {
                s.read_replica_source_db_instance_identifier = Some(id.clone());
            }
        }

        (new_primary_xml, new_primary_arn)
    };

    svc.emit_event(
        RdsSourceType::DbInstance,
        &id,
        &instance_arn,
        "RDS-EVENT-0071",
        &["notification"],
        "A read replica has been switched over to a primary",
    );

    Ok(xml_response(
        "SwitchoverReadReplica",
        format!("    <DBInstance>\n{xml}    </DBInstance>"),
        rid,
    ))
}
