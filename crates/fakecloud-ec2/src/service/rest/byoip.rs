//! EC2 byoip operations (extracted from the rest long-tail module).

#![allow(clippy::too_many_lines)]

use super::*;

pub(crate) fn advertise_byoip_cidr(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Cidr")?;
    Ok(Ec2Service::respond(
        "AdvertiseByoipCidr",
        &req.request_id,
        "",
    ))
}

pub(crate) fn deprovision_byoip_cidr(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Cidr")?;
    Ok(Ec2Service::respond(
        "DeprovisionByoipCidr",
        &req.request_id,
        "",
    ))
}

pub(crate) fn describe_byoip_cidrs(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "MaxResults")?;
    validate_max_results(&req.query_params, 1, 100)?;
    Ok(Ec2Service::respond(
        "DescribeByoipCidrs",
        &req.request_id,
        "",
    ))
}

pub(crate) fn provision_byoip_cidr(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Cidr")?;
    Ok(Ec2Service::respond(
        "ProvisionByoipCidr",
        &req.request_id,
        "",
    ))
}

pub(crate) fn withdraw_byoip_cidr(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Cidr")?;
    Ok(Ec2Service::respond(
        "WithdrawByoipCidr",
        &req.request_id,
        "",
    ))
}
