//! Subnet operations: lifecycle, attributes, CIDR association, CIDR
//! reservations, and the secondary-subnet sub-family.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    filter_value_matches, gen_id, indexed_list, not_found, paginate, parse_filters, require,
    validate_enum, validate_max_results, Filter,
};
use crate::state::{Ec2State, Subnet, SubnetCidrReservation, Tag};

/// Render the inner XML of a `<subnet>` element (lowerCamel wire names).
pub(crate) fn subnet_xml(s: &Subnet, tags: &[Tag], owner: &str, region: &str) -> String {
    let ipv6_set = match &s.ipv6_cidr_block {
        Some(cidr) => {
            let item = format!(
                "{}{}<ipv6CidrBlockState><state>associated</state></ipv6CidrBlockState>",
                ec2_elem("associationId", &subnet_ipv6_assoc_id(&s.subnet_id)),
                ec2_elem("ipv6CidrBlock", cidr),
            );
            ec2_list("ipv6CidrBlockAssociationSet", &[item])
        }
        None => String::new(),
    };
    format!(
        "{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
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
        format_args!(
            "<assignIpv6AddressOnCreation>{}</assignIpv6AddressOnCreation>",
            s.assign_ipv6_address_on_creation
        ),
        ec2_elem("ownerId", owner),
        ec2_elem(
            "subnetArn",
            &format!("arn:aws:ec2:{region}:{owner}:subnet/{}", s.subnet_id),
        ),
        format_args!("<enableDns64>{}</enableDns64>", s.enable_dns64),
        // The `aws_subnet` resource reads `private_dns_hostname_type_on_launch`
        // (and the DNS-record toggles) from this block; AWS defaults the
        // hostname type to `ip-name`.
        format_args!(
            "<privateDnsNameOptionsOnLaunch><hostnameType>{}</hostnameType><enableResourceNameDnsARecord>{}</enableResourceNameDnsARecord><enableResourceNameDnsAAAARecord>{}</enableResourceNameDnsAAAARecord></privateDnsNameOptionsOnLaunch>",
            s.private_dns_hostname_type,
            s.enable_resource_name_dns_a_record_on_launch,
            s.enable_resource_name_dns_aaaa_record_on_launch
        ),
        ipv6_set,
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
        enable_resource_name_dns_a_record_on_launch: false,
        enable_resource_name_dns_aaaa_record_on_launch: false,
        private_dns_hostname_type: "ip-name".to_string(),
        ipv6_cidr_block: None,
    }
}

/// Deterministic association id for a subnet's IPv6 CIDR.
fn subnet_ipv6_assoc_id(subnet_id: &str) -> String {
    // Probe variants send arbitrary (often short) synthetic ids; strip the
    // `subnet-` prefix when present rather than slicing by byte offset, which
    // would panic on an id shorter than 7 bytes or off a UTF-8 boundary.
    let suffix = subnet_id.strip_prefix("subnet-").unwrap_or(subnet_id);
    format!("subnet-cidr-assoc-ipv6-{suffix}")
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
    let mut subnet = build_subnet(vpc_id, cidr, &az, false);
    // CreateSubnet may carry an IPv6 CIDR; the resource then waits for the
    // association to appear in DescribeSubnets.
    if let Some(ipv6) = req.query_params.get("Ipv6CidrBlock") {
        if !ipv6.is_empty() {
            subnet.ipv6_cidr_block = Some(ipv6.clone());
        }
    }
    if req
        .query_params
        .get("AssignIpv6AddressOnCreation")
        .map(|v| v == "true")
        .unwrap_or(false)
    {
        subnet.assign_ipv6_address_on_creation = true;
    }
    let id = subnet.subnet_id.clone();
    let owner = req.account_id.clone();
    let region = req.region.clone();
    let body = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(state, &req.query_params, &id, "subnet");
        let tags = state.tags_for(&id).to_vec();
        // AWS auto-associates every new subnet with its VPC's default network
        // ACL. The aws_network_acl_association resource resolves the subnet's
        // current association via DescribeNetworkAcls before replacing it, so
        // the default association must exist.
        let vpc_of_subnet = subnet.vpc_id.clone();
        if let Some(default_acl) = state
            .network_acls
            .values_mut()
            .find(|a| a.vpc_id == vpc_of_subnet && a.is_default)
        {
            default_acl
                .associations
                .push(crate::state::NetworkAclAssoc {
                    association_id: gen_id("aclassoc"),
                    subnet_id: id.clone(),
                });
        }
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
    let owner = req.account_id.clone();
    let region = req.region.clone();
    let body = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // Attach to the account's real default VPC, not the literal
        // `vpc-default` (which matches no VPC and orphaned the subnet —
        // bug-hunt 2026-06-18 finding 1.2). If a default subnet already exists
        // for this AZ (the bootstrap seeds one per AZ), return it instead of
        // minting a duplicate, matching CreateDefaultSubnet's idempotency.
        let default_vpc = state
            .vpcs
            .values()
            .find(|v| v.is_default)
            .map(|v| v.vpc_id.clone())
            .unwrap_or_else(|| crate::defaults::default_vpc_id(&req.account_id));
        let subnet = if let Some(existing) = state
            .subnets
            .values()
            .find(|s| s.vpc_id == default_vpc && s.default_for_az && s.availability_zone == az)
            .cloned()
        {
            existing
        } else {
            let s = build_subnet(default_vpc, "172.31.0.0/20".to_string(), &az, true);
            state.subnets.insert(s.subnet_id.clone(), s.clone());
            s
        };
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

    // An explicitly-requested SubnetId that does not exist is a hard error on
    // AWS (InvalidSubnetID.NotFound), not a silently-empty result.
    for id in &wanted {
        if !state.subnets.contains_key(id) {
            return Err(not_found("InvalidSubnetID.NotFound", id));
        }
    }

    let mut items: Vec<String> = state
        .subnets
        .values()
        .filter(|s| wanted.is_empty() || wanted.contains(&s.subnet_id))
        .filter(|s| subnet_matches(s, state.tags_for(&s.subnet_id), &filters))
        .map(|s| subnet_xml(s, state.tags_for(&s.subnet_id), &owner, &region))
        .collect();
    items.sort();

    // DescribeSubnets is a `paginated` operation in the model; honor MaxResults
    // + NextToken instead of always returning the full set.
    let max_results = req
        .query_params
        .get("MaxResults")
        .filter(|v| !v.is_empty())
        .and_then(|v| v.parse::<usize>().ok());
    let next_token = req.query_params.get("NextToken").map(String::as_str);
    let (page, token) = paginate(&items, next_token, max_results);
    let body = format!(
        "{}{}",
        ec2_list("subnetSet", &page),
        token.map(|t| ec2_elem("nextToken", &t)).unwrap_or_default(),
    );
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
            "tag-value" => tags.iter().map(|t| t.value.clone()).collect(),
            "ipv6-cidr-block-association.association-id" => s
                .ipv6_cidr_block
                .as_ref()
                .map(|_| vec![subnet_ipv6_assoc_id(&s.subnet_id)])
                .unwrap_or_default(),
            "ipv6-cidr-block-association.ipv6-cidr-block" => {
                s.ipv6_cidr_block.clone().into_iter().collect()
            }
            "tag-key" => tags.iter().map(|t| t.key.clone()).collect(),
            name => {
                if let Some(key) = name.strip_prefix("tag:") {
                    tags.iter()
                        .filter(|t| t.key == key)
                        .map(|t| t.value.clone())
                        .collect()
                } else {
                    return false;
                }
            }
        };
        f.values
            .iter()
            .any(|v| candidates.iter().any(|c| filter_value_matches(v, c)))
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
            if let Some(v) = req
                .query_params
                .get("EnableResourceNameDnsARecordOnLaunch.Value")
            {
                s.enable_resource_name_dns_a_record_on_launch = v == "true";
            }
            if let Some(v) = req
                .query_params
                .get("EnableResourceNameDnsAAAARecordOnLaunch.Value")
            {
                s.enable_resource_name_dns_aaaa_record_on_launch = v == "true";
            }
            if let Some(v) = req.query_params.get("MapCustomerOwnedIpOnLaunch.Value") {
                s.map_customer_owned_ip_on_launch = v == "true";
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
    let assoc_id = subnet_ipv6_assoc_id(&subnet_id);
    // Persist on the subnet so DescribeSubnets reports the association; the
    // resource waits for it via the returned association id.
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(s) = state.subnets.get_mut(&subnet_id) {
            s.ipv6_cidr_block = Some(ipv6.clone());
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{ec2_request as req, err_of};

    fn body_of(resp: AwsResponse) -> String {
        String::from_utf8(resp.body.expect_bytes().to_vec()).unwrap()
    }

    /// Create a subnet in `vpc_id` with `cidr` and return its id.
    fn make_subnet(svc: &Ec2Service, vpc_id: &str, cidr: &str) -> String {
        let body = body_of(
            create_subnet(
                svc,
                &req("CreateSubnet", &[("VpcId", vpc_id), ("CidrBlock", cidr)]),
            )
            .unwrap(),
        );
        body.split("<subnetId>")
            .nth(1)
            .and_then(|s| s.split("</subnetId>").next())
            .unwrap()
            .to_string()
    }

    #[test]
    fn modify_subnet_attribute_persists_dns_record_and_customer_owned_ip() {
        // bug-audit 2026-07-27 (cycle 6): these attrs were ignored on modify and
        // the render hardcoded the DNS-record toggles to false -> perpetual drift.
        let svc = Ec2Service::new();
        let id = make_subnet(&svc, "vpc-test", "10.0.7.0/24");
        for (k, v) in [
            ("EnableResourceNameDnsARecordOnLaunch.Value", "true"),
            ("EnableResourceNameDnsAAAARecordOnLaunch.Value", "true"),
            ("MapCustomerOwnedIpOnLaunch.Value", "true"),
        ] {
            modify_subnet_attribute(
                &svc,
                &req("ModifySubnetAttribute", &[("SubnetId", &id), (k, v)]),
            )
            .unwrap();
        }
        let body = body_of(
            describe_subnets(&svc, &req("DescribeSubnets", &[("SubnetId.1", &id)])).unwrap(),
        );
        assert!(
            body.contains("<enableResourceNameDnsARecord>true</enableResourceNameDnsARecord>"),
            "{body}"
        );
        assert!(
            body.contains(
                "<enableResourceNameDnsAAAARecord>true</enableResourceNameDnsAAAARecord>"
            ),
            "{body}"
        );
        assert!(
            body.contains("<mapCustomerOwnedIpOnLaunch>true</mapCustomerOwnedIpOnLaunch>"),
            "{body}"
        );
    }

    #[test]
    fn describe_subnets_explicit_missing_id_errors() {
        let svc = Ec2Service::new();
        make_subnet(&svc, "vpc-test", "10.0.1.0/24");
        let err = err_of(describe_subnets(
            &svc,
            &req("DescribeSubnets", &[("SubnetId.1", "subnet-missing")]),
        ));
        assert_eq!(err.code(), "InvalidSubnetID.NotFound");
    }

    #[test]
    fn describe_subnets_paginates() {
        let svc = Ec2Service::new();
        // Seed enough subnets (plus the default subnets) to force a second page
        // at the minimum MaxResults of 5.
        for i in 0..6 {
            make_subnet(&svc, "vpc-test", &format!("10.0.{i}.0/24"));
        }
        let body = body_of(
            describe_subnets(&svc, &req("DescribeSubnets", &[("MaxResults", "5")])).unwrap(),
        );
        assert!(body.contains("<nextToken>"), "expected a NextToken: {body}");
    }

    #[test]
    fn describe_subnets_tag_value_filter() {
        let svc = Ec2Service::new();
        let id = make_subnet(&svc, "vpc-test", "10.0.9.0/24");
        {
            let mut accounts = svc.state.write();
            let state = accounts.get_or_create("000000000000");
            state.tags.insert(
                id.clone(),
                vec![Tag {
                    key: "tier".into(),
                    value: "public".into(),
                }],
            );
        }
        let body = body_of(
            describe_subnets(
                &svc,
                &req(
                    "DescribeSubnets",
                    &[
                        ("Filter.1.Name", "tag-value"),
                        ("Filter.1.Value.1", "public"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(
            body.contains(&format!("<subnetId>{id}</subnetId>")),
            "{body}"
        );
    }
}
