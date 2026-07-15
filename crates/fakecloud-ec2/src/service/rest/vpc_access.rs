//! EC2 vpc access operations (extracted from the rest long-tail module).

#![allow(clippy::too_many_lines)]

use super::*;

fn vpc_bpa_exclusion_xml(e: &VpcBpaExclusion, tags: &[Tag]) -> String {
    format!(
        "{}{}{}{}{}",
        ec2_elem("exclusionId", &e.id),
        ec2_elem(
            "internetGatewayExclusionMode",
            &e.internet_gateway_exclusion_mode
        ),
        ec2_elem_opt("resourceArn", e.resource_arn.as_deref()),
        ec2_elem("state", &e.state),
        super::super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_vpc_block_public_access_exclusion(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let mode = require(&req.query_params, "InternetGatewayExclusionMode")?;
    validate_enum(
        &req.query_params,
        "InternetGatewayExclusionMode",
        &["allow-bidirectional", "allow-egress"],
    )?;
    let region = region_of(req);
    // The exclusion targets a subnet or a VPC; record its ARN.
    let resource_arn = req
        .query_params
        .get("SubnetId")
        .filter(|v| !v.is_empty())
        .map(|s| format!("arn:aws:ec2:{region}:{}:subnet/{s}", req.account_id))
        .or_else(|| {
            req.query_params
                .get("VpcId")
                .filter(|v| !v.is_empty())
                .map(|v| format!("arn:aws:ec2:{region}:{}:vpc/{v}", req.account_id))
        });
    let id = gen_id("vpcbpa-exclude");
    let e = VpcBpaExclusion {
        id: id.clone(),
        internet_gateway_exclusion_mode: mode,
        resource_arn,
        state: "create-complete".to_string(),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "vpc-block-public-access-exclusion",
        );
        let t = state.tags_for(&id).to_vec();
        state.vpc_bpa_exclusions.insert(id.clone(), e.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateVpcBlockPublicAccessExclusion",
        &req.request_id,
        &format!(
            "<vpcBlockPublicAccessExclusion>{}</vpcBlockPublicAccessExclusion>",
            vpc_bpa_exclusion_xml(&e, &tags)
        ),
    ))
}

pub(crate) fn delete_vpc_block_public_access_exclusion(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ExclusionId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let mut e = state
        .vpc_bpa_exclusions
        .remove(&id)
        .unwrap_or_else(|| VpcBpaExclusion {
            id: id.clone(),
            internet_gateway_exclusion_mode: "allow-bidirectional".to_string(),
            resource_arn: None,
            state: String::new(),
        });
    e.state = "delete-complete".to_string();
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteVpcBlockPublicAccessExclusion",
        &req.request_id,
        &format!(
            "<vpcBlockPublicAccessExclusion>{}</vpcBlockPublicAccessExclusion>",
            vpc_bpa_exclusion_xml(&e, &tags)
        ),
    ))
}

pub(crate) fn describe_vpc_block_public_access_exclusions(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "ExclusionId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .vpc_bpa_exclusions
        .values()
        .filter(|e| wanted.is_empty() || wanted.contains(&e.id))
        .map(|e| vpc_bpa_exclusion_xml(e, state.tags_for(&e.id)))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeVpcBlockPublicAccessExclusions",
        &req.request_id,
        &ec2_list("vpcBlockPublicAccessExclusionSet", &items),
    ))
}

fn vpc_bpa_options_xml(account: &str, region: &str, mode: &str) -> String {
    let state = if mode == "off" {
        "default-state"
    } else {
        "update-complete"
    };
    format!(
        "<vpcBlockPublicAccessOptions>{}{}{}{}{}</vpcBlockPublicAccessOptions>",
        ec2_elem("awsAccountId", account),
        ec2_elem("awsRegion", region),
        ec2_elem("state", state),
        ec2_elem("internetGatewayBlockMode", mode),
        ec2_elem("exclusionsAllowed", "allowed"),
    )
}

pub(crate) fn describe_vpc_block_public_access_options(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let region = region_of(req);
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mode = state
        .vpc_bpa_internet_gateway_block_mode
        .clone()
        .unwrap_or_else(|| "off".to_string());
    Ok(Ec2Service::respond(
        "DescribeVpcBlockPublicAccessOptions",
        &req.request_id,
        &vpc_bpa_options_xml(&req.account_id, &region, &mode),
    ))
}

pub(crate) fn modify_vpc_block_public_access_exclusion(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "ExclusionId")?;
    let mode = require(&req.query_params, "InternetGatewayExclusionMode")?;
    validate_enum(
        &req.query_params,
        "InternetGatewayExclusionMode",
        &["allow-bidirectional", "allow-egress"],
    )?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    // Mutate when present; synthesize for a probe-only synthetic id.
    let mut synth = VpcBpaExclusion {
        id: id.clone(),
        internet_gateway_exclusion_mode: mode.clone(),
        resource_arn: None,
        state: "update-complete".to_string(),
    };
    let entry = state.vpc_bpa_exclusions.get_mut(&id).unwrap_or(&mut synth);
    entry.internet_gateway_exclusion_mode = mode;
    entry.state = "update-complete".to_string();
    let e = entry.clone();
    let tags = state.tags_for(&id).to_vec();
    Ok(Ec2Service::respond(
        "ModifyVpcBlockPublicAccessExclusion",
        &req.request_id,
        &format!(
            "<vpcBlockPublicAccessExclusion>{}</vpcBlockPublicAccessExclusion>",
            vpc_bpa_exclusion_xml(&e, &tags)
        ),
    ))
}

pub(crate) fn modify_vpc_block_public_access_options(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let mode = require(&req.query_params, "InternetGatewayBlockMode")?;
    validate_enum(
        &req.query_params,
        "InternetGatewayBlockMode",
        &["off", "block-bidirectional", "block-ingress"],
    )?;
    let region = region_of(req);
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.vpc_bpa_internet_gateway_block_mode = Some(mode.clone());
    }
    Ok(Ec2Service::respond(
        "ModifyVpcBlockPublicAccessOptions",
        &req.request_id,
        &vpc_bpa_options_xml(&req.account_id, &region, &mode),
    ))
}
