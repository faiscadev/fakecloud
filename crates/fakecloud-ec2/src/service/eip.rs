//! Elastic IPs, key pairs, and placement groups.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    filter_value_matches, gen_id, indexed_list, not_found, parse_filters, require, validate_enum,
    validate_max_results, Filter,
};
use crate::state::{Ec2State, ElasticIp, KeyPair, PlacementGroup, Tag};

// ---- Elastic IPs ----

fn address_xml(a: &ElasticIp, tags: &[Tag]) -> String {
    let mut out = format!(
        "{}{}{}{}{}",
        ec2_elem("publicIp", &a.public_ip),
        ec2_elem("allocationId", &a.allocation_id),
        ec2_elem("domain", &a.domain),
        ec2_elem("publicIpv4Pool", &a.public_ipv4_pool),
        ec2_elem("networkBorderGroup", &a.network_border_group),
    );
    if let Some(v) = &a.association_id {
        out.push_str(&ec2_elem("associationId", v));
    }
    if let Some(v) = &a.instance_id {
        out.push_str(&ec2_elem("instanceId", v));
    }
    if let Some(v) = &a.network_interface_id {
        out.push_str(&ec2_elem("networkInterfaceId", v));
    }
    if let Some(v) = &a.private_ip_address {
        out.push_str(&ec2_elem("privateIpAddress", v));
    }
    out.push_str(&super::tags::tag_set_xml(tags));
    out
}

pub(crate) fn allocate_address(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(&req.query_params, "Domain", &["vpc", "standard"])?;
    let alloc_id = gen_id("eipalloc");
    let public_ip = format!(
        "52.0.{}.{}",
        alloc_id.len() % 250,
        alloc_id.as_bytes()[9] as u16 % 250
    );
    let domain = req
        .query_params
        .get("Domain")
        .cloned()
        .unwrap_or_else(|| "vpc".to_string());
    let public_ipv4_pool = req
        .query_params
        .get("PublicIpv4Pool")
        .cloned()
        .unwrap_or_else(|| "amazon".to_string());
    let network_border_group = req
        .query_params
        .get("NetworkBorderGroup")
        .cloned()
        .unwrap_or_else(|| {
            if req.region.is_empty() {
                "us-east-1".to_string()
            } else {
                req.region.clone()
            }
        });
    let eip = ElasticIp {
        allocation_id: alloc_id.clone(),
        public_ip: public_ip.clone(),
        domain: domain.clone(),
        association_id: None,
        instance_id: None,
        network_interface_id: None,
        private_ip_address: None,
        public_ipv4_pool: public_ipv4_pool.clone(),
        network_border_group: network_border_group.clone(),
    };
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &alloc_id,
            "elastic-ip",
        );
        state.elastic_ips.insert(alloc_id.clone(), eip);
    }
    let body = format!(
        "{}{}{}{}{}",
        ec2_elem("allocationId", &alloc_id),
        ec2_elem("publicIp", &public_ip),
        ec2_elem("domain", &domain),
        ec2_elem("networkBorderGroup", &network_border_group),
        ec2_elem("publicIpv4Pool", &public_ipv4_pool),
    );
    Ok(Ec2Service::respond(
        "AllocateAddress",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn release_address(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    if let Some(id) = req.query_params.get("AllocationId") {
        if state.elastic_ips.remove(id).is_none() {
            return Err(not_found("InvalidAllocationID.NotFound", id));
        }
        state.tags.remove(id);
    } else if let Some(ip) = req.query_params.get("PublicIp") {
        let ids: Vec<String> = state
            .elastic_ips
            .values()
            .filter(|e| &e.public_ip == ip)
            .map(|e| e.allocation_id.clone())
            .collect();
        if ids.is_empty() {
            return Err(AwsServiceError::aws_error(
                http::StatusCode::BAD_REQUEST,
                "InvalidAddress.NotFound",
                format!("Address '{ip}' not found."),
            ));
        }
        for id in ids {
            state.elastic_ips.remove(&id);
        }
    }
    Ok(Ec2Service::respond(
        "ReleaseAddress",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_addresses(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let filters = parse_filters(&req.query_params);
    let wanted = indexed_list(&req.query_params, "AllocationId");
    let wanted_ips = indexed_list(&req.query_params, "PublicIp");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .elastic_ips
        .values()
        .filter(|e| wanted.is_empty() || wanted.contains(&e.allocation_id))
        .filter(|e| wanted_ips.is_empty() || wanted_ips.contains(&e.public_ip))
        .filter(|e| addr_match(e, state.tags_for(&e.allocation_id), &filters))
        .map(|e| address_xml(e, state.tags_for(&e.allocation_id)))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeAddresses",
        &req.request_id,
        &ec2_list("addressesSet", &items),
    ))
}

fn addr_match(e: &ElasticIp, tags: &[Tag], filters: &[Filter]) -> bool {
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "allocation-id" => vec![e.allocation_id.clone()],
            "public-ip" => vec![e.public_ip.clone()],
            "domain" => vec![e.domain.clone()],
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

pub(crate) fn associate_address(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let assoc_id = gen_id("eipassoc");
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    // Resolve the target address by AllocationId (VPC) or PublicIp (Classic).
    // An unknown identifier must error rather than fabricate a phantom
    // association that no Describe would ever reflect.
    let target_alloc = if let Some(alloc) = req.query_params.get("AllocationId") {
        if !state.elastic_ips.contains_key(alloc) {
            return Err(not_found("InvalidAllocationID.NotFound", alloc));
        }
        alloc.clone()
    } else if let Some(ip) = req.query_params.get("PublicIp") {
        match state
            .elastic_ips
            .values()
            .find(|e| &e.public_ip == ip)
            .map(|e| e.allocation_id.clone())
        {
            Some(id) => id,
            None => {
                return Err(AwsServiceError::aws_error(
                    http::StatusCode::BAD_REQUEST,
                    "InvalidAddress.NotFound",
                    format!("Address '{ip}' not found."),
                ));
            }
        }
    } else {
        return Err(AwsServiceError::aws_error(
            http::StatusCode::BAD_REQUEST,
            "MissingParameter",
            "The request must contain either AllocationId or PublicIp.".to_string(),
        ));
    };
    if let Some(e) = state.elastic_ips.get_mut(&target_alloc) {
        e.association_id = Some(assoc_id.clone());
        e.instance_id = req.query_params.get("InstanceId").cloned();
        e.network_interface_id = req.query_params.get("NetworkInterfaceId").cloned();
    }
    Ok(Ec2Service::respond(
        "AssociateAddress",
        &req.request_id,
        &ec2_elem("associationId", &assoc_id),
    ))
}

pub(crate) fn disassociate_address(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    if let Some(assoc) = req.query_params.get("AssociationId") {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for e in state.elastic_ips.values_mut() {
            if e.association_id.as_deref() == Some(assoc.as_str()) {
                e.association_id = None;
                e.instance_id = None;
                e.network_interface_id = None;
            }
        }
    }
    Ok(Ec2Service::respond(
        "DisassociateAddress",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_addresses_attribute(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    validate_enum(&req.query_params, "Attribute", &["domain-name"])?;
    Ok(Ec2Service::respond(
        "DescribeAddressesAttribute",
        &req.request_id,
        &ec2_list("addressSet", &[]),
    ))
}

fn address_attribute_body(req: &AwsRequest) -> String {
    let alloc = req
        .query_params
        .get("AllocationId")
        .cloned()
        .unwrap_or_default();
    format!(
        "<address>{}{}</address>",
        ec2_elem("publicIp", "52.0.0.1"),
        ec2_elem("allocationId", &alloc),
    )
}

pub(crate) fn modify_address_attribute(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "AllocationId")?;
    Ok(Ec2Service::respond(
        "ModifyAddressAttribute",
        &req.request_id,
        &address_attribute_body(req),
    ))
}

pub(crate) fn reset_address_attribute(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "AllocationId")?;
    require(&req.query_params, "Attribute")?;
    validate_enum(&req.query_params, "Attribute", &["domain-name"])?;
    Ok(Ec2Service::respond(
        "ResetAddressAttribute",
        &req.request_id,
        &address_attribute_body(req),
    ))
}

pub(crate) fn move_address_to_vpc(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "PublicIp")?;
    let body = format!(
        "{}{}",
        ec2_elem("allocationId", &gen_id("eipalloc")),
        ec2_elem("status", "MoveInProgress"),
    );
    Ok(Ec2Service::respond(
        "MoveAddressToVpc",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn restore_address_to_classic(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ip = require(&req.query_params, "PublicIp")?;
    let body = format!(
        "{}{}",
        ec2_elem("publicIp", &ip),
        ec2_elem("status", "InClassic")
    );
    Ok(Ec2Service::respond(
        "RestoreAddressToClassic",
        &req.request_id,
        &body,
    ))
}

fn address_transfer_xml(req: &AwsRequest, status: &str) -> String {
    format!(
        "<addressTransfer>{}{}{}{}</addressTransfer>",
        ec2_elem(
            "publicIp",
            req.query_params
                .get("Address")
                .map(|s| s.as_str())
                .unwrap_or("52.0.0.1")
        ),
        ec2_elem(
            "allocationId",
            req.query_params
                .get("AllocationId")
                .map(|s| s.as_str())
                .unwrap_or("eipalloc-0")
        ),
        ec2_elem(
            "transferAccountId",
            req.query_params
                .get("TransferAccountId")
                .map(|s| s.as_str())
                .unwrap_or("123456789012")
        ),
        ec2_elem("addressTransferStatus", status),
    )
}

pub(crate) fn accept_address_transfer(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Address")?;
    Ok(Ec2Service::respond(
        "AcceptAddressTransfer",
        &req.request_id,
        &address_transfer_xml(req, "accepted"),
    ))
}

pub(crate) fn enable_address_transfer(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "AllocationId")?;
    require(&req.query_params, "TransferAccountId")?;
    Ok(Ec2Service::respond(
        "EnableAddressTransfer",
        &req.request_id,
        &address_transfer_xml(req, "pending"),
    ))
}

pub(crate) fn disable_address_transfer(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "AllocationId")?;
    Ok(Ec2Service::respond(
        "DisableAddressTransfer",
        &req.request_id,
        &address_transfer_xml(req, "disabled"),
    ))
}

pub(crate) fn describe_address_transfers(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeAddressTransfers",
        &req.request_id,
        &ec2_list("addressTransferSet", &[]),
    ))
}

pub(crate) fn describe_moving_addresses(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "DescribeMovingAddresses",
        &req.request_id,
        &ec2_list("movingAddressStatusSet", &[]),
    ))
}

// ---- Key pairs ----

/// `InvalidKeyPair.Duplicate` — a key pair with this name already exists.
fn duplicate_key_pair(name: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        http::StatusCode::BAD_REQUEST,
        "InvalidKeyPair.Duplicate",
        format!("The keypair '{name}' already exists."),
    )
}

const FAKE_KEY_MATERIAL: &str =
    "-----BEGIN RSA PRIVATE KEY-----\nMIIfakefakefakefakefake\n-----END RSA PRIVATE KEY-----";

pub(crate) fn create_key_pair(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let key_name = require(&req.query_params, "KeyName")?;
    validate_enum(&req.query_params, "KeyType", &["rsa", "ed25519"])?;
    validate_enum(&req.query_params, "KeyFormat", &["pem", "ppk"])?;
    let key_pair_id = gen_id("key");
    let fingerprint = "1a:2b:3c:4d:5e:6f:00:11:22:33:44:55:66:77:88:99".to_string();
    let kp = KeyPair {
        key_pair_id: key_pair_id.clone(),
        key_name: key_name.clone(),
        key_type: req
            .query_params
            .get("KeyType")
            .cloned()
            .unwrap_or_else(|| "rsa".to_string()),
        key_fingerprint: fingerprint.clone(),
    };
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state.key_pairs.contains_key(&key_name) {
            return Err(duplicate_key_pair(&key_name));
        }
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &key_pair_id,
            "key-pair",
        );
        state.key_pairs.insert(key_name.clone(), kp);
    }
    let body = format!(
        "{}{}{}{}",
        ec2_elem("keyName", &key_name),
        ec2_elem("keyPairId", &key_pair_id),
        ec2_elem("keyFingerprint", &fingerprint),
        ec2_elem("keyMaterial", FAKE_KEY_MATERIAL),
    );
    Ok(Ec2Service::respond("CreateKeyPair", &req.request_id, &body))
}

pub(crate) fn import_key_pair(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let key_name = require(&req.query_params, "KeyName")?;
    require(&req.query_params, "PublicKeyMaterial")?;
    let key_pair_id = gen_id("key");
    let fingerprint = "aa:bb:cc:dd:ee:ff:00:11:22:33:44:55:66:77:88:99".to_string();
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state.key_pairs.contains_key(&key_name) {
            return Err(duplicate_key_pair(&key_name));
        }
        state.key_pairs.insert(
            key_name.clone(),
            KeyPair {
                key_pair_id: key_pair_id.clone(),
                key_name: key_name.clone(),
                key_type: "rsa".to_string(),
                key_fingerprint: fingerprint.clone(),
            },
        );
    }
    let body = format!(
        "{}{}{}",
        ec2_elem("keyName", &key_name),
        ec2_elem("keyPairId", &key_pair_id),
        ec2_elem("keyFingerprint", &fingerprint),
    );
    Ok(Ec2Service::respond("ImportKeyPair", &req.request_id, &body))
}

pub(crate) fn delete_key_pair(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    if let Some(name) = req.query_params.get("KeyName") {
        state.key_pairs.remove(name);
    } else if let Some(id) = req.query_params.get("KeyPairId") {
        let names: Vec<String> = state
            .key_pairs
            .values()
            .filter(|k| &k.key_pair_id == id)
            .map(|k| k.key_name.clone())
            .collect();
        for n in names {
            state.key_pairs.remove(&n);
        }
    }
    Ok(Ec2Service::respond(
        "DeleteKeyPair",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_key_pairs(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let wanted = indexed_list(&req.query_params, "KeyName");
    let wanted_ids = indexed_list(&req.query_params, "KeyPairId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .key_pairs
        .values()
        .filter(|k| wanted.is_empty() || wanted.contains(&k.key_name))
        .filter(|k| wanted_ids.is_empty() || wanted_ids.contains(&k.key_pair_id))
        .map(|k| {
            format!(
                "{}{}{}{}",
                ec2_elem("keyName", &k.key_name),
                ec2_elem("keyPairId", &k.key_pair_id),
                ec2_elem("keyType", &k.key_type),
                ec2_elem("keyFingerprint", &k.key_fingerprint),
            )
        })
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeKeyPairs",
        &req.request_id,
        &ec2_list("keySet", &items),
    ))
}

// ---- Placement groups ----

fn pg_xml(p: &PlacementGroup, tags: &[Tag], owner: &str, region: &str) -> String {
    let mut out = format!(
        "{}{}{}{}{}",
        ec2_elem("groupName", &p.group_name),
        ec2_elem("groupId", &p.group_id),
        ec2_elem("state", &p.state),
        ec2_elem("strategy", &p.strategy),
        ec2_elem(
            "groupArn",
            &format!(
                "arn:aws:ec2:{region}:{owner}:placement-group/{}",
                p.group_name
            )
        ),
    );
    if let Some(n) = p.partition_count {
        out.push_str(&format!("<partitionCount>{n}</partitionCount>"));
    }
    if let Some(sl) = &p.spread_level {
        out.push_str(&ec2_elem("spreadLevel", sl));
    }
    out.push_str(&super::tags::tag_set_xml(tags));
    out
}

pub(crate) fn create_placement_group(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(
        &req.query_params,
        "Strategy",
        &["cluster", "spread", "partition"],
    )?;
    validate_enum(&req.query_params, "SpreadLevel", &["host", "rack"])?;
    let name = req
        .query_params
        .get("GroupName")
        .cloned()
        .unwrap_or_else(|| gen_id("pg"));
    let pg = PlacementGroup {
        group_id: gen_id("pg"),
        group_name: name.clone(),
        strategy: req
            .query_params
            .get("Strategy")
            .cloned()
            .unwrap_or_else(|| "cluster".to_string()),
        state: "available".to_string(),
        partition_count: req
            .query_params
            .get("PartitionCount")
            .and_then(|v| v.parse().ok()),
        spread_level: req.query_params.get("SpreadLevel").cloned(),
    };
    let owner = req.account_id.clone();
    let region = req.region.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &pg.group_id,
            "placement-group",
        );
        let t = state.tags_for(&pg.group_id).to_vec();
        state.placement_groups.insert(name, pg.clone());
        t
    };
    let body = format!(
        "<placementGroup>{}</placementGroup>",
        pg_xml(&pg, &tags, &owner, &region)
    );
    Ok(Ec2Service::respond(
        "CreatePlacementGroup",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_placement_group(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = require(&req.query_params, "GroupName")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.placement_groups.remove(&name);
    }
    Ok(Ec2Service::respond(
        "DeletePlacementGroup",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_placement_groups(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let wanted = indexed_list(&req.query_params, "GroupName");
    let owner = req.account_id.clone();
    let region = req.region.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .placement_groups
        .values()
        .filter(|p| wanted.is_empty() || wanted.contains(&p.group_name))
        .map(|p| pg_xml(p, state.tags_for(&p.group_id), &owner, &region))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribePlacementGroups",
        &req.request_id,
        &ec2_list("placementGroupSet", &items),
    ))
}

pub(crate) fn get_groups_for_capacity_reservation(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    require(&req.query_params, "CapacityReservationId")?;
    Ok(Ec2Service::respond(
        "GetGroupsForCapacityReservation",
        &req.request_id,
        &ec2_list("capacityReservationGroupSet", &[]),
    ))
}

#[cfg(test)]
mod eip_tests {
    use super::*;
    use crate::test_support::ec2_request as req;
    use fakecloud_core::service::AwsResponse;

    fn body(resp: AwsResponse) -> String {
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    #[test]
    fn describe_addresses_reports_pool_and_border_group() {
        let svc = Ec2Service::new();
        allocate_address(&svc, &req("AllocateAddress", &[("Domain", "vpc")])).unwrap();
        let out = body(describe_addresses(&svc, &req("DescribeAddresses", &[])).unwrap());
        assert!(
            out.contains("<publicIpv4Pool>amazon</publicIpv4Pool>"),
            "got: {out}"
        );
        assert!(
            out.contains("<networkBorderGroup>us-east-1</networkBorderGroup>"),
            "got: {out}"
        );
    }

    #[test]
    fn allocate_address_honors_pool_and_border_group_params() {
        let svc = Ec2Service::new();
        let out = body(
            allocate_address(
                &svc,
                &req(
                    "AllocateAddress",
                    &[
                        ("Domain", "vpc"),
                        ("NetworkBorderGroup", "us-east-1-atl-1"),
                        ("PublicIpv4Pool", "ipv4pool-ec2-0abc"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert!(
            out.contains("<networkBorderGroup>us-east-1-atl-1</networkBorderGroup>"),
            "got: {out}"
        );
        assert!(
            out.contains("<publicIpv4Pool>ipv4pool-ec2-0abc</publicIpv4Pool>"),
            "got: {out}"
        );
        let out = body(describe_addresses(&svc, &req("DescribeAddresses", &[])).unwrap());
        assert!(
            out.contains("<networkBorderGroup>us-east-1-atl-1</networkBorderGroup>"),
            "got: {out}"
        );
        assert!(
            out.contains("<publicIpv4Pool>ipv4pool-ec2-0abc</publicIpv4Pool>"),
            "got: {out}"
        );
    }

    #[test]
    fn elastic_ip_deserializes_legacy_state_without_pool_fields() {
        let json = r#"{"allocation_id":"eipalloc-1","public_ip":"52.0.0.1",
            "domain":"vpc","association_id":null,"instance_id":null,
            "network_interface_id":null,"private_ip_address":null}"#;
        let eip: ElasticIp = serde_json::from_str(json).unwrap();
        assert_eq!(eip.public_ipv4_pool, "amazon");
        assert_eq!(eip.network_border_group, "us-east-1");
    }
}

#[cfg(test)]
mod keypair_tests {
    use super::*;
    use crate::test_support::{ec2_request as req, err_of};

    #[test]
    fn create_key_pair_rejects_duplicate_name() {
        let svc = Ec2Service::new();
        create_key_pair(&svc, &req("CreateKeyPair", &[("KeyName", "kp")])).unwrap();
        let err = err_of(create_key_pair(
            &svc,
            &req("CreateKeyPair", &[("KeyName", "kp")]),
        ));
        assert_eq!(err.code(), "InvalidKeyPair.Duplicate");
    }

    #[test]
    fn import_key_pair_rejects_duplicate_name() {
        let svc = Ec2Service::new();
        create_key_pair(&svc, &req("CreateKeyPair", &[("KeyName", "kp")])).unwrap();
        let err = err_of(import_key_pair(
            &svc,
            &req(
                "ImportKeyPair",
                &[("KeyName", "kp"), ("PublicKeyMaterial", "c3NoLXJzYQ==")],
            ),
        ));
        assert_eq!(err.code(), "InvalidKeyPair.Duplicate");
    }
}
