//! EC2 Instance Connect endpoints, fast-launch images, serial-console access,
//! and console/password retrieval.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{gen_id, require, validate_enum, validate_max_results};
use crate::state::{Ec2State, InstanceConnectEndpoint, Tag};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";
const IP_TYPES: &[&str] = &["ipv4", "dualstack", "ipv6"];

// ---- instance connect endpoints ----

fn ice_xml(e: &InstanceConnectEndpoint, tags: &[Tag], owner: &str, region: &str) -> String {
    format!(
        "{}{}{}<state>create-complete</state>{}<createdAt>{}</createdAt><preserveClientIp>true</preserveClientIp>{}",
        ec2_elem("instanceConnectEndpointId", &e.id),
        ec2_elem("instanceConnectEndpointArn", &format!("arn:aws:ec2:{region}:{owner}:instance-connect-endpoint/{}", e.id)),
        ec2_elem("ownerId", owner),
        ec2_elem("subnetId", &e.subnet_id),
        FIXED_TIME,
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_instance_connect_endpoint(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let subnet = require(&req.query_params, "SubnetId")?;
    validate_enum(&req.query_params, "IpAddressType", IP_TYPES)?;
    let id = gen_id("eice");
    let e = InstanceConnectEndpoint {
        id: id.clone(),
        subnet_id: subnet,
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "instance-connect-endpoint",
        );
        let t = state.tags_for(&id).to_vec();
        state
            .instance_connect_endpoints
            .insert(id.clone(), e.clone());
        t
    };
    let token = req
        .query_params
        .get("ClientToken")
        .cloned()
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "CreateInstanceConnectEndpoint",
        &req.request_id,
        &format!(
            "<instanceConnectEndpoint>{}</instanceConnectEndpoint>{}",
            ice_xml(&e, &tags, &owner, &req.region),
            ec2_elem("clientToken", &token)
        ),
    ))
}

pub(crate) fn delete_instance_connect_endpoint(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceConnectEndpointId")?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let e = state
        .instance_connect_endpoints
        .remove(&id)
        .unwrap_or(InstanceConnectEndpoint {
            id: id.clone(),
            subnet_id: "subnet-0".to_string(),
        });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteInstanceConnectEndpoint",
        &req.request_id,
        &format!(
            "<instanceConnectEndpoint>{}</instanceConnectEndpoint>",
            ice_xml(&e, &tags, &owner, &req.region)
        ),
    ))
}

pub(crate) fn describe_instance_connect_endpoints(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 50)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .instance_connect_endpoints
        .values()
        .map(|e| ice_xml(e, state.tags_for(&e.id), &owner, &req.region))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeInstanceConnectEndpoints",
        &req.request_id,
        &ec2_list("instanceConnectEndpointSet", &items),
    ))
}

pub(crate) fn modify_instance_connect_endpoint(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceConnectEndpointId")?;
    validate_enum(&req.query_params, "IpAddressType", IP_TYPES)?;
    Ok(Ec2Service::respond(
        "ModifyInstanceConnectEndpoint",
        &req.request_id,
        &ec2_return(true),
    ))
}

// ---- fast launch ----

fn fast_launch_xml(image: &str, owner: &str, state: &str) -> String {
    format!(
        "{}<resourceType>snapshot</resourceType><maxParallelLaunches>6</maxParallelLaunches>{}<state>{}</state><stateTransitionTime>{}</stateTransitionTime>",
        ec2_elem("imageId", image),
        ec2_elem("ownerId", owner),
        state,
        FIXED_TIME,
    )
}

pub(crate) fn enable_fast_launch(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let image = require(&req.query_params, "ImageId")?;
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .fast_launch_images
            .insert(image.clone());
    }
    Ok(Ec2Service::respond(
        "EnableFastLaunch",
        &req.request_id,
        &fast_launch_xml(&image, &req.account_id, "enabled"),
    ))
}

pub(crate) fn disable_fast_launch(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let image = require(&req.query_params, "ImageId")?;
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .fast_launch_images
            .remove(&image);
    }
    Ok(Ec2Service::respond(
        "DisableFastLaunch",
        &req.request_id,
        &fast_launch_xml(&image, &req.account_id, "disabled"),
    ))
}

// ---- serial console access ----

pub(crate) fn enable_serial_console_access(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .serial_console_access = true;
    }
    Ok(Ec2Service::respond(
        "EnableSerialConsoleAccess",
        &req.request_id,
        "<serialConsoleAccessEnabled>true</serialConsoleAccessEnabled>",
    ))
}

pub(crate) fn disable_serial_console_access(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .serial_console_access = false;
    }
    Ok(Ec2Service::respond(
        "DisableSerialConsoleAccess",
        &req.request_id,
        "<serialConsoleAccessEnabled>false</serialConsoleAccessEnabled>",
    ))
}

pub(crate) fn get_serial_console_access_status(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let enabled = {
        let accounts = svc.state.read();
        accounts
            .get(&req.account_id)
            .map(|s| s.serial_console_access)
            .unwrap_or(false)
    };
    Ok(Ec2Service::respond(
        "GetSerialConsoleAccessStatus",
        &req.request_id,
        &format!("<serialConsoleAccessEnabled>{enabled}</serialConsoleAccessEnabled>"),
    ))
}

// ---- console + password ----

pub(crate) async fn get_console_output(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    use base64::Engine;
    let id = require(&req.query_params, "InstanceId")?;
    // Real GetConsoleOutput returns the console log base64-encoded. Back it
    // with the instance container's logs (which include any user-data boot
    // output); empty when the instance has no backing container.
    let output = match &svc.runtime {
        Some(rt) => rt.console_output(&id).await.unwrap_or_default(),
        None => Vec::new(),
    };
    let encoded = base64::engine::general_purpose::STANDARD.encode(output);
    let body = format!(
        "{}<timestamp>{}</timestamp><output>{}</output>",
        ec2_elem("instanceId", &id),
        FIXED_TIME,
        encoded
    );
    Ok(Ec2Service::respond(
        "GetConsoleOutput",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn get_console_screenshot(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceId")?;
    let body = format!("{}<imageData></imageData>", ec2_elem("instanceId", &id));
    Ok(Ec2Service::respond(
        "GetConsoleScreenshot",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn get_password_data(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceId")?;
    let body = format!(
        "{}<timestamp>{}</timestamp><passwordData></passwordData>",
        ec2_elem("instanceId", &id),
        FIXED_TIME
    );
    Ok(Ec2Service::respond(
        "GetPasswordData",
        &req.request_id,
        &body,
    ))
}
