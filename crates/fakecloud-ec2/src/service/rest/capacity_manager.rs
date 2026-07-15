//! EC2 capacity manager operations (extracted from the rest long-tail module).

#![allow(clippy::too_many_lines)]

use super::*;

pub(crate) fn create_capacity_manager_data_export(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "S3BucketName")?;
    require(&req.query_params, "Schedule")?;
    require(&req.query_params, "OutputFormat")?;
    validate_enum(&req.query_params, "Schedule", &["hourly"])?;
    validate_enum(&req.query_params, "OutputFormat", &["csv", "parquet"])?;
    Ok(Ec2Service::respond(
        "CreateCapacityManagerDataExport",
        &req.request_id,
        "",
    ))
}

pub(crate) fn delete_capacity_manager_data_export(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "CapacityManagerDataExportId")?;
    Ok(Ec2Service::respond(
        "DeleteCapacityManagerDataExport",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_capacity_manager_data_exports(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeCapacityManagerDataExports",
        &req.request_id,
        "",
    ))
}

pub(crate) fn disable_capacity_manager(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "DisableCapacityManager",
        &req.request_id,
        "",
    ))
}

pub(crate) fn enable_capacity_manager(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "EnableCapacityManager",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_capacity_manager_attributes(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "GetCapacityManagerAttributes",
        &req.request_id,
        "",
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
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    Ok(Ec2Service::respond(
        "GetCapacityManagerMonitoredTagKeys",
        &req.request_id,
        "",
    ))
}

pub(crate) fn update_capacity_manager_monitored_tag_keys(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "UpdateCapacityManagerMonitoredTagKeys",
        &req.request_id,
        "",
    ))
}

pub(crate) fn update_capacity_manager_organizations_access(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "OrganizationsAccess")?;
    Ok(Ec2Service::respond(
        "UpdateCapacityManagerOrganizationsAccess",
        &req.request_id,
        "",
    ))
}
