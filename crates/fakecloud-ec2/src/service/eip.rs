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
        "{}{}{}",
        ec2_elem("publicIp", &a.public_ip),
        ec2_elem("allocationId", &a.allocation_id),
        ec2_elem("domain", &a.domain),
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
    // publicIpv4Pool and networkBorderGroup trail tagSet, matching the
    // member order AWS emits for the Address type.
    out.push_str(&ec2_elem("publicIpv4Pool", &a.public_ipv4_pool));
    out.push_str(&ec2_elem("networkBorderGroup", &a.network_border_group));
    out
}

pub(crate) fn allocate_address(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(&req.query_params, "Domain", &["vpc", "standard"])?;
    let alloc_id = gen_id("eipalloc");
    // Derive the public IP from three hex bytes of the allocation id so
    // distinct allocations get distinct addresses. The previous form keyed
    // only on `alloc_id.len()` (a constant 26) and a single hex nibble,
    // collapsing every EIP onto ~16 IPs and letting ReleaseAddress-by-PublicIp
    // target the wrong address.
    let hex = &alloc_id["eipalloc-".len()..];
    let octet = |s: &str| u16::from_str_radix(s, 16).unwrap_or(0) % 256;
    let public_ip = format!(
        "52.{}.{}.{}",
        octet(&hex[0..2]),
        octet(&hex[2..4]),
        octet(&hex[4..6]),
    );
    let domain = req
        .query_params
        .get("Domain")
        .cloned()
        .unwrap_or_else(|| "vpc".to_string());
    // Treat an empty query-param value as absent so `PublicIpv4Pool=` still
    // falls back to the default rather than persisting an empty string.
    let public_ipv4_pool = req
        .query_params
        .get("PublicIpv4Pool")
        .filter(|v| !v.is_empty())
        .cloned()
        .unwrap_or_else(|| "amazon".to_string());
    let network_border_group = req
        .query_params
        .get("NetworkBorderGroup")
        .filter(|v| !v.is_empty())
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
        domain_name: None,
    };
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        // The AWS-managed "amazon" pool is always valid; any BYOIP pool id must
        // have been provisioned first, else AWS returns
        // InvalidPublicIpv4PoolID.NotFound.
        if public_ipv4_pool != "amazon" && !state.public_ipv4_pools.contains_key(&public_ipv4_pool)
        {
            return Err(not_found(
                "InvalidPublicIpv4PoolID.NotFound",
                &public_ipv4_pool,
            ));
        }
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
        ec2_elem("publicIpv4Pool", &public_ipv4_pool),
        ec2_elem("networkBorderGroup", &network_border_group),
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
            "network-border-group" => vec![e.network_border_group.clone()],
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
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 1000)?;
    validate_enum(&req.query_params, "Attribute", &["domain-name"])?;
    let wanted = indexed_list(&req.query_params, "AllocationId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    // AWS only returns addresses that have a domain-name attribute set.
    let mut items: Vec<String> = state
        .elastic_ips
        .values()
        .filter(|e| wanted.is_empty() || wanted.contains(&e.allocation_id))
        .filter(|e| e.domain_name.is_some())
        .map(address_attribute_xml)
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeAddressesAttribute",
        &req.request_id,
        &ec2_list("addressSet", &items),
    ))
}

fn address_attribute_xml(eip: &ElasticIp) -> String {
    let ptr = match &eip.domain_name {
        Some(name) => ec2_elem("ptrRecord", name),
        None => String::new(),
    };
    format!(
        "{}{}{}",
        ec2_elem("publicIp", &eip.public_ip),
        ec2_elem("allocationId", &eip.allocation_id),
        ptr,
    )
}

pub(crate) fn modify_address_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let alloc = require(&req.query_params, "AllocationId")?;
    let domain_name = req.query_params.get("DomainName").cloned();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let eip = state
        .elastic_ips
        .get_mut(&alloc)
        .ok_or_else(|| not_found("InvalidAllocationID.NotFound", &alloc))?;
    if let Some(name) = domain_name {
        eip.domain_name = Some(name);
    }
    let body = format!("<address>{}</address>", address_attribute_xml(eip));
    Ok(Ec2Service::respond(
        "ModifyAddressAttribute",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn reset_address_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let alloc = require(&req.query_params, "AllocationId")?;
    require(&req.query_params, "Attribute")?;
    validate_enum(&req.query_params, "Attribute", &["domain-name"])?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let eip = state
        .elastic_ips
        .get_mut(&alloc)
        .ok_or_else(|| not_found("InvalidAllocationID.NotFound", &alloc))?;
    eip.domain_name = None;
    let body = format!("<address>{}</address>", address_attribute_xml(eip));
    Ok(Ec2Service::respond(
        "ResetAddressAttribute",
        &req.request_id,
        &body,
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
    fn modify_address_attribute_persists_ptr_record() {
        let svc = Ec2Service::new();
        let alloc = {
            let out = body(
                allocate_address(&svc, &req("AllocateAddress", &[("Domain", "vpc")])).unwrap(),
            );
            out.split("<allocationId>")
                .nth(1)
                .and_then(|s| s.split("</allocationId>").next())
                .unwrap()
                .to_string()
        };

        // Before any modify, the address has no domain-name attribute so it is
        // omitted from DescribeAddressesAttribute.
        let empty = body(
            describe_addresses_attribute(
                &svc,
                &req(
                    "DescribeAddressesAttribute",
                    &[("Attribute", "domain-name")],
                ),
            )
            .unwrap(),
        );
        assert!(
            !empty.contains("ptrRecord"),
            "no PTR before modify: {empty}"
        );

        let modified = body(
            modify_address_attribute(
                &svc,
                &req(
                    "ModifyAddressAttribute",
                    &[("AllocationId", &alloc), ("DomainName", "host.example.com")],
                ),
            )
            .unwrap(),
        );
        assert!(
            modified.contains("<ptrRecord>host.example.com</ptrRecord>"),
            "{modified}"
        );

        // DescribeAddressesAttribute now reflects it.
        let desc = body(
            describe_addresses_attribute(
                &svc,
                &req(
                    "DescribeAddressesAttribute",
                    &[("Attribute", "domain-name"), ("AllocationId.1", &alloc)],
                ),
            )
            .unwrap(),
        );
        assert!(
            desc.contains("<ptrRecord>host.example.com</ptrRecord>"),
            "{desc}"
        );

        // ResetAddressAttribute clears it.
        reset_address_attribute(
            &svc,
            &req(
                "ResetAddressAttribute",
                &[("AllocationId", &alloc), ("Attribute", "domain-name")],
            ),
        )
        .unwrap();
        let after = body(
            describe_addresses_attribute(
                &svc,
                &req(
                    "DescribeAddressesAttribute",
                    &[("Attribute", "domain-name")],
                ),
            )
            .unwrap(),
        );
        assert!(
            !after.contains("ptrRecord"),
            "PTR cleared after reset: {after}"
        );
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
        {
            let mut accounts = svc.state.write();
            let state = accounts.get_or_create("000000000000");
            state.public_ipv4_pools.insert(
                "ipv4pool-ec2-0abc".to_string(),
                crate::state::PublicIpv4Pool {
                    pool_id: "ipv4pool-ec2-0abc".to_string(),
                    description: String::new(),
                    network_border_group: String::new(),
                    cidrs: Vec::new(),
                },
            );
        }
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
    fn describe_addresses_filters_by_network_border_group() {
        let svc = Ec2Service::new();
        allocate_address(&svc, &req("AllocateAddress", &[("Domain", "vpc")])).unwrap();
        allocate_address(
            &svc,
            &req(
                "AllocateAddress",
                &[("Domain", "vpc"), ("NetworkBorderGroup", "us-east-1-atl-1")],
            ),
        )
        .unwrap();
        let out = body(
            describe_addresses(
                &svc,
                &req(
                    "DescribeAddresses",
                    &[
                        ("Filter.1.Name", "network-border-group"),
                        ("Filter.1.Value.1", "us-east-1-atl-1"),
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
            !out.contains("<networkBorderGroup>us-east-1</networkBorderGroup>"),
            "default border group should be filtered out, got: {out}"
        );
    }

    #[test]
    fn allocate_address_rejects_unprovisioned_pool() {
        let svc = Ec2Service::new();
        let res = allocate_address(
            &svc,
            &req(
                "AllocateAddress",
                &[
                    ("Domain", "vpc"),
                    ("PublicIpv4Pool", "ipv4pool-ec2-missing"),
                ],
            ),
        );
        let err = match res {
            Ok(_) => panic!("expected InvalidPublicIpv4PoolID.NotFound"),
            Err(e) => e,
        };
        assert_eq!(err.code(), "InvalidPublicIpv4PoolID.NotFound");
    }

    #[test]
    fn allocate_address_accepts_provisioned_pool() {
        let svc = Ec2Service::new();
        {
            let mut accounts = svc.state.write();
            let state = accounts.get_or_create("000000000000");
            state.public_ipv4_pools.insert(
                "ipv4pool-ec2-0abc".to_string(),
                crate::state::PublicIpv4Pool {
                    pool_id: "ipv4pool-ec2-0abc".to_string(),
                    description: String::new(),
                    network_border_group: String::new(),
                    cidrs: Vec::new(),
                },
            );
        }
        let out = body(
            allocate_address(
                &svc,
                &req(
                    "AllocateAddress",
                    &[("Domain", "vpc"), ("PublicIpv4Pool", "ipv4pool-ec2-0abc")],
                ),
            )
            .unwrap(),
        );
        assert!(
            out.contains("<publicIpv4Pool>ipv4pool-ec2-0abc</publicIpv4Pool>"),
            "got: {out}"
        );
    }

    #[test]
    fn allocate_address_empty_pool_param_falls_back_to_default() {
        let svc = Ec2Service::new();
        let out = body(
            allocate_address(
                &svc,
                &req(
                    "AllocateAddress",
                    &[
                        ("Domain", "vpc"),
                        ("PublicIpv4Pool", ""),
                        ("NetworkBorderGroup", ""),
                    ],
                ),
            )
            .unwrap(),
        );
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
    fn allocate_address_assigns_distinct_public_ips() {
        let svc = Ec2Service::new();
        allocate_address(&svc, &req("AllocateAddress", &[("Domain", "vpc")])).unwrap();
        allocate_address(&svc, &req("AllocateAddress", &[("Domain", "vpc")])).unwrap();
        let accounts = svc.state.read();
        let state = accounts.get("000000000000").unwrap();
        let ips: std::collections::HashSet<_> =
            state.elastic_ips.values().map(|e| &e.public_ip).collect();
        assert_eq!(ips.len(), 2, "each allocation should get a distinct IP");
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
