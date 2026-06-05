//! Subnet operations: lifecycle, attributes, CIDR association, CIDR
//! reservations, and the secondary-subnet sub-family.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    gen_id, indexed_list, parse_filters, require, validate_enum, validate_max_results, Filter,
};
use crate::state::{Ec2State, Subnet, SubnetCidrReservation, Tag};

/// Render the inner XML of a `<subnet>` element (lowerCamel wire names).
pub(crate) fn subnet_xml(s: &Subnet, tags: &[Tag], owner: &str, region: &str) -> String {
    format!(
        "{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
        ec2_elem("subnetId", &s.subnet_id),
        ec2_elem("state", &s.state),
        ec2_elem("vpcId", &s.vpc_id),
        ec2_elem("cidrBlock", &s.cidr_block),
        format_args!(
            "<availableIpAddressCount>{}</availableIpAddressCount>",
            s.available_ip_address_count
        ),
        ec2_elem("availabilityZone", &s.availability_zone),
        ec2_elem("availabilityZoneId", &s.availability_zone_id),
        format_args!("<defaultForAz>{}</defaultForAz>", s.default_for_az),
        format_args!(
            "<mapPublicIpOnLaunch>{}</mapPublicIpOnLaunch>",
            s.map_public_ip_on_launch
        ),
        format_args!(
            "<mapCustomerOwnedIpOnLaunch>{}</mapCustomerOwnedIpOnLaunch>",
            s.map_customer_owned_ip_on_launch
        ),
        ec2_elem("ownerId", owner),
        ec2_elem(
            "subnetArn",
            &format!("arn:aws:ec2:{region}:{owner}:subnet/{}", s.subnet_id),
        ),
        format_args!("<enableDns64>{}</enableDns64>", s.enable_dns64),
        super::tags::tag_set_xml(tags),
    )
}

fn reservation_xml(r: &SubnetCidrReservation, owner: &str) -> String {
    format!(
        "{}{}{}{}{}{}",
        ec2_elem("subnetCidrReservationId", &r.subnet_cidr_reservation_id),
        ec2_elem("subnetId", &r.subnet_id),
        ec2_elem("cidr", &r.cidr),
        ec2_elem("reservationType", &r.reservation_type),
        ec2_elem("ownerId", owner),
        ec2_elem("description", &r.description),
    )
}

fn build_subnet(vpc_id: String, cidr: String, az: &str, default_for_az: bool) -> Subnet {
    Subnet {
        subnet_id: gen_id("subnet"),
        vpc_id,
        cidr_block: cidr,
        availability_zone: az.to_string(),
        availability_zone_id: format!("use1-az{}", (az.bytes().last().unwrap_or(b'a') % 6) + 1),
        state: "available".to_string(),
        available_ip_address_count: 251,
        default_for_az,
        map_public_ip_on_launch: false,
        assign_ipv6_address_on_creation: false,
        map_customer_owned_ip_on_launch: false,
        enable_dns64: false,
        private_dns_hostname_type: "ip-name".to_string(),
    }
}

fn default_az(req: &AwsRequest) -> String {
    req.query_params
        .get("AvailabilityZone")
        .cloned()
        .unwrap_or_else(|| {
            format!(
                "{}a",
                if req.region.is_empty() {
                    "us-east-1"
                } else {
                    &req.region
                }
            )
        })
}

pub(crate) fn create_subnet(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let vpc_id = require(&req.query_params, "VpcId")?;
    let cidr = req
        .query_params
        .get("CidrBlock")
        .cloned()
        .unwrap_or_else(|| "10.0.0.0/24".to_string());
    let az = default_az(req);
    let subnet = build_subnet(vpc_id, cidr, &az, false);
    let id = subnet.subnet_id.clone();
    let owner = req.account_id.clone();
    let region = req.region.clone();
    let body = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(state, &req.query_params, &id, "subnet");
        let tags = state.tags_for(&id).to_vec();
        state.subnets.insert(id.clone(), subnet.clone());
        format!(
            "<subnet>{}</subnet>",
            subnet_xml(&subnet, &tags, &owner, &region)
        )
    };
    Ok(Ec2Service::respond("CreateSubnet", &req.request_id, &body))
}

pub(crate) fn create_default_subnet(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let az = default_az(req);
    let subnet = build_subnet(
        "vpc-default".to_string(),
        "172.31.0.0/20".to_string(),
        &az,
        true,
    );
    let id = subnet.subnet_id.clone();
    let owner = req.account_id.clone();
    let region = req.region.clone();
    let body = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.subnets.insert(id.clone(), subnet.clone());
        format!(
            "<subnet>{}</subnet>",
            subnet_xml(&subnet, &[], &owner, &region)
        )
    };
    Ok(Ec2Service::respond(
        "CreateDefaultSubnet",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn create_secondary_subnet(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let _cidr = require(&req.query_params, "Ipv4CidrBlock")?;
    let network = require(&req.query_params, "SecondaryNetworkId")?;
    let az = default_az(req);
    let owner = &req.account_id;
    let id = gen_id("subnet");
    let body = format!(
        "<secondarySubnet>{}{}{}{}{}{}{}</secondarySubnet><clientToken>{}</clientToken>",
        ec2_elem("secondarySubnetId", &id),
        ec2_elem(
            "secondarySubnetArn",
            &format!("arn:aws:ec2:{}:{owner}:subnet/{id}", req.region)
        ),
        ec2_elem("secondaryNetworkId", &network),
        ec2_elem("ownerId", owner),
        ec2_elem("availabilityZone", &az),
        ec2_elem("availabilityZoneId", "use1-az1"),
        ec2_elem("state", "available"),
        gen_id("token"),
    );
    Ok(Ec2Service::respond(
        "CreateSecondarySubnet",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_subnet(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "SubnetId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.subnets.remove(&id);
        state.tags.remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteSubnet",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn delete_secondary_subnet(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "SecondarySubnetId")?;
    Ok(Ec2Service::respond(
        "DeleteSecondarySubnet",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_subnets(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let filters = parse_filters(&req.query_params);
    let wanted = indexed_list(&req.query_params, "SubnetId");
    let owner = req.account_id.clone();
    let region = req.region.clone();

    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);

    let mut items: Vec<String> = state
        .subnets
        .values()
        .filter(|s| wanted.is_empty() || wanted.contains(&s.subnet_id))
        .filter(|s| subnet_matches(s, state.tags_for(&s.subnet_id), &filters))
        .map(|s| subnet_xml(s, state.tags_for(&s.subnet_id), &owner, &region))
        .collect();
    items.sort();

    let body = ec2_list("subnetSet", &items);
    Ok(Ec2Service::respond(
        "DescribeSubnets",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn describe_secondary_subnets(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeSecondarySubnets",
        &req.request_id,
        &ec2_list("secondarySubnetSet", &[]),
    ))
}

fn subnet_matches(s: &Subnet, tags: &[Tag], filters: &[Filter]) -> bool {
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "subnet-id" => vec![s.subnet_id.clone()],
            "vpc-id" => vec![s.vpc_id.clone()],
            "cidr" | "cidr-block" => vec![s.cidr_block.clone()],
            "availability-zone" => vec![s.availability_zone.clone()],
            "state" => vec![s.state.clone()],
            "default-for-az" => vec![s.default_for_az.to_string()],
            "tag-key" => tags.iter().map(|t| t.key.clone()).collect(),
            name => {
                if let Some(key) = name.strip_prefix("tag:") {
                    tags.iter()
                        .filter(|t| t.key == key)
                        .map(|t| t.value.clone())
                        .collect()
                } else {
                    return true;
                }
            }
        };
        f.values.iter().any(|v| candidates.iter().any(|c| c == v))
    })
}

pub(crate) fn modify_subnet_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let subnet_id = require(&req.query_params, "SubnetId")?;
    validate_enum(
        &req.query_params,
        "PrivateDnsHostnameTypeOnLaunch",
        &["ip-name", "resource-name"],
    )?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(s) = state.subnets.get_mut(&subnet_id) {
            if let Some(v) = req.query_params.get("MapPublicIpOnLaunch.Value") {
                s.map_public_ip_on_launch = v == "true";
            }
            if let Some(v) = req.query_params.get("AssignIpv6AddressOnCreation.Value") {
                s.assign_ipv6_address_on_creation = v == "true";
            }
            if let Some(v) = req.query_params.get("EnableDns64.Value") {
                s.enable_dns64 = v == "true";
            }
            if let Some(v) = req.query_params.get("PrivateDnsHostnameTypeOnLaunch") {
                s.private_dns_hostname_type = v.clone();
            }
        }
    }
    Ok(Ec2Service::respond(
        "ModifySubnetAttribute",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn associate_subnet_cidr_block(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let subnet_id = require(&req.query_params, "SubnetId")?;
    let ipv6 = req
        .query_params
        .get("Ipv6CidrBlock")
        .cloned()
        .unwrap_or_else(|| "2600:1f00::/64".to_string());
    let assoc_id = gen_id("subnet-cidr-assoc");
    let _ = svc;
    let body = format!(
        "{}<ipv6CidrBlockAssociation>{}{}<ipv6CidrBlockState><state>associated</state></ipv6CidrBlockState></ipv6CidrBlockAssociation>",
        ec2_elem("subnetId", &subnet_id),
        ec2_elem("associationId", &assoc_id),
        ec2_elem("ipv6CidrBlock", &ipv6),
    );
    Ok(Ec2Service::respond(
        "AssociateSubnetCidrBlock",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn disassociate_subnet_cidr_block(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let assoc_id = require(&req.query_params, "AssociationId")?;
    let body = format!(
        "{}<ipv6CidrBlockAssociation>{}<ipv6CidrBlockState><state>disassociating</state></ipv6CidrBlockState></ipv6CidrBlockAssociation>",
        ec2_elem("subnetId", "subnet-00000000000000000"),
        ec2_elem("associationId", &assoc_id),
    );
    Ok(Ec2Service::respond(
        "DisassociateSubnetCidrBlock",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn create_subnet_cidr_reservation(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let subnet_id = require(&req.query_params, "SubnetId")?;
    let cidr = require(&req.query_params, "Cidr")?;
    let reservation_type = require(&req.query_params, "ReservationType")?;
    validate_enum(
        &req.query_params,
        "ReservationType",
        &["prefix", "explicit"],
    )?;

    let res = SubnetCidrReservation {
        subnet_cidr_reservation_id: gen_id("scr"),
        subnet_id,
        cidr,
        reservation_type,
        description: req
            .query_params
            .get("Description")
            .cloned()
            .unwrap_or_default(),
    };
    let id = res.subnet_cidr_reservation_id.clone();
    let owner = req.account_id.clone();
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.subnet_cidr_reservations.insert(id, res.clone());
    }
    let body = format!(
        "<subnetCidrReservation>{}</subnetCidrReservation>",
        reservation_xml(&res, &owner)
    );
    Ok(Ec2Service::respond(
        "CreateSubnetCidrReservation",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_subnet_cidr_reservation(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "SubnetCidrReservationId")?;
    let owner = req.account_id.clone();
    let removed = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.subnet_cidr_reservations.remove(&id)
    };
    // Response echoes the deleted reservation; synthesize if unknown (lenient).
    let res = removed.unwrap_or(SubnetCidrReservation {
        subnet_cidr_reservation_id: id,
        subnet_id: "subnet-00000000000000000".to_string(),
        cidr: "10.0.0.0/28".to_string(),
        reservation_type: "prefix".to_string(),
        description: String::new(),
    });
    let body = format!(
        "<deletedSubnetCidrReservation>{}</deletedSubnetCidrReservation>",
        reservation_xml(&res, &owner)
    );
    Ok(Ec2Service::respond(
        "DeleteSubnetCidrReservation",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn get_subnet_cidr_reservations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let subnet_id = require(&req.query_params, "SubnetId")?;
    let owner = req.account_id.clone();

    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .subnet_cidr_reservations
        .values()
        .filter(|r| r.subnet_id == subnet_id)
        .map(|r| reservation_xml(r, &owner))
        .collect();

    let body = format!(
        "{}{}",
        ec2_list("subnetIpv4CidrReservationSet", &items),
        ec2_list("subnetIpv6CidrReservationSet", &[]),
    );
    Ok(Ec2Service::respond(
        "GetSubnetCidrReservations",
        &req.request_id,
        &body,
    ))
}
