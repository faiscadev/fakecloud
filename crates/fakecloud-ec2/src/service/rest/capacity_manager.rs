//! EC2 capacity manager operations (extracted from the rest long-tail module).

#![allow(clippy::too_many_lines)]

use super::*;

fn cm_status(enabled: bool) -> &'static str {
    if enabled {
        "ENABLED"
    } else {
        "DISABLED"
    }
}

fn data_export_xml(e: &CapacityManagerDataExport) -> String {
    format!(
        "{}{}{}{}{}<latestDeliveryStatus>PENDING</latestDeliveryStatus>",
        ec2_elem("capacityManagerDataExportId", &e.id),
        ec2_elem("s3BucketName", &e.s3_bucket_name),
        ec2_elem("s3BucketPrefix", &e.s3_bucket_prefix),
        ec2_elem("schedule", &e.schedule),
        ec2_elem("outputFormat", &e.output_format),
    )
}

pub(crate) fn create_capacity_manager_data_export(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let s3_bucket_name = require(&req.query_params, "S3BucketName")?;
    let schedule = require(&req.query_params, "Schedule")?;
    let output_format = require(&req.query_params, "OutputFormat")?;
    validate_enum(&req.query_params, "Schedule", &["hourly"])?;
    validate_enum(&req.query_params, "OutputFormat", &["csv", "parquet"])?;
    let id = gen_id("cmde");
    let export = CapacityManagerDataExport {
        id: id.clone(),
        s3_bucket_name,
        s3_bucket_prefix: req
            .query_params
            .get("S3BucketPrefix")
            .cloned()
            .unwrap_or_default(),
        schedule,
        output_format,
    };
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .capacity_manager_data_exports
            .insert(id.clone(), export);
    }
    Ok(Ec2Service::respond(
        "CreateCapacityManagerDataExport",
        &req.request_id,
        &ec2_elem("capacityManagerDataExportId", &id),
    ))
}

pub(crate) fn delete_capacity_manager_data_export(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "CapacityManagerDataExportId")?;
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .capacity_manager_data_exports
            .remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteCapacityManagerDataExport",
        &req.request_id,
        &ec2_elem("capacityManagerDataExportId", &id),
    ))
}

pub(crate) fn describe_capacity_manager_data_exports(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .capacity_manager_data_exports
        .values()
        .map(data_export_xml)
        .collect();
    Ok(Ec2Service::respond(
        "DescribeCapacityManagerDataExports",
        &req.request_id,
        &ec2_list("capacityManagerDataExportSet", &items),
    ))
}

/// Render `<capacityManagerStatus>` + `<organizationsAccess>`, shared by the
/// enable/disable/update-org-access ops.
fn cm_status_body(enabled: bool, org_access: &str) -> String {
    // `OrganizationsAccess` is modeled as `Boolean` (com.amazonaws.ec2#Boolean),
    // so it must render as `true`/`false` — emitting a status string like
    // "disabled" makes the SDK's XML decoder reject the response.
    let org_bool = matches!(
        org_access.trim().to_ascii_lowercase().as_str(),
        "enabled" | "true"
    );
    format!(
        "{}{}",
        ec2_elem("capacityManagerStatus", cm_status(enabled)),
        ec2_elem(
            "organizationsAccess",
            if org_bool { "true" } else { "false" }
        ),
    )
}

pub(crate) fn disable_capacity_manager(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let org_access = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.capacity_manager_enabled = false;
        state
            .capacity_manager_org_access
            .clone()
            .unwrap_or_else(|| "disabled".to_string())
    };
    Ok(Ec2Service::respond(
        "DisableCapacityManager",
        &req.request_id,
        &cm_status_body(false, &org_access),
    ))
}

pub(crate) fn enable_capacity_manager(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let org_access = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.capacity_manager_enabled = true;
        state
            .capacity_manager_org_access
            .clone()
            .unwrap_or_else(|| "disabled".to_string())
    };
    Ok(Ec2Service::respond(
        "EnableCapacityManager",
        &req.request_id,
        &cm_status_body(true, &org_access),
    ))
}

pub(crate) fn get_capacity_manager_attributes(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let org_access = state
        .capacity_manager_org_access
        .clone()
        .unwrap_or_else(|| "disabled".to_string());
    let body = format!(
        "{}{}",
        cm_status_body(state.capacity_manager_enabled, &org_access),
        ec2_elem(
            "dataExportCount",
            &state.capacity_manager_data_exports.len().to_string()
        ),
    );
    Ok(Ec2Service::respond(
        "GetCapacityManagerAttributes",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn get_capacity_manager_metric_data(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_int_range(&req.query_params, "Period", 3600, i64::MAX)?;
    require(&req.query_params, "StartTime")?;
    require(&req.query_params, "EndTime")?;
    require(&req.query_params, "Period")?;
    Ok(Ec2Service::respond(
        "GetCapacityManagerMetricData",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_capacity_manager_metric_dimensions(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "StartTime")?;
    require(&req.query_params, "EndTime")?;
    Ok(Ec2Service::respond(
        "GetCapacityManagerMetricDimensions",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_capacity_manager_monitored_tag_keys(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items = state.capacity_manager_monitored_tag_keys.clone();
    Ok(Ec2Service::respond(
        "GetCapacityManagerMonitoredTagKeys",
        &req.request_id,
        &ec2_list("capacityManagerTagKeySet", &items),
    ))
}

pub(crate) fn update_capacity_manager_monitored_tag_keys(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let activate = indexed_list(&req.query_params, "ActivateTagKey");
    let deactivate = indexed_list(&req.query_params, "DeactivateTagKey");
    let keys = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for k in &activate {
            if !state.capacity_manager_monitored_tag_keys.contains(k) {
                state.capacity_manager_monitored_tag_keys.push(k.clone());
            }
        }
        state
            .capacity_manager_monitored_tag_keys
            .retain(|k| !deactivate.contains(k));
        state.capacity_manager_monitored_tag_keys.clone()
    };
    Ok(Ec2Service::respond(
        "UpdateCapacityManagerMonitoredTagKeys",
        &req.request_id,
        &ec2_list("capacityManagerTagKeySet", &keys),
    ))
}

pub(crate) fn update_capacity_manager_organizations_access(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let org_access = require(&req.query_params, "OrganizationsAccess")?;
    let enabled = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.capacity_manager_org_access = Some(org_access.clone());
        state.capacity_manager_enabled
    };
    Ok(Ec2Service::respond(
        "UpdateCapacityManagerOrganizationsAccess",
        &req.request_id,
        &cm_status_body(enabled, &org_access),
    ))
}
