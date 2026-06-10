//! IPAM resource discovery, discovered-resource getters, BYOASN, BYOIP-to-IPAM,
//! and external resource verification tokens.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{gen_id, require, require_struct, validate_max_results};
use crate::state::{Ec2State, IpamResourceDiscovery, Tag};

fn region_of(req: &AwsRequest) -> String {
    if req.region.is_empty() {
        "us-east-1".to_string()
    } else {
        req.region.clone()
    }
}

// ---- resource discovery ----

fn rd_xml(d: &IpamResourceDiscovery, tags: &[Tag], owner: &str, region: &str) -> String {
    format!(
        "{}{}{}{}<isDefault>false</isDefault><state>create-complete</state>{}{}",
        ec2_elem("ipamResourceDiscoveryId", &d.id),
        ec2_elem(
            "ipamResourceDiscoveryArn",
            &format!("arn:aws:ec2::{owner}:ipam-resource-discovery/{}", d.id)
        ),
        ec2_elem("ipamResourceDiscoveryRegion", region),
        ec2_elem("ownerId", owner),
        ec2_list(
            "operatingRegionSet",
            &[format!("<regionName>{region}</regionName>")]
        ),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_ipam_resource_discovery(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = gen_id("ipam-res-disco");
    let d = IpamResourceDiscovery { id: id.clone() };
    let owner = req.account_id.clone();
    let region = region_of(req);
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "ipam-resource-discovery",
        );
        let t = state.tags_for(&id).to_vec();
        state
            .ipam_resource_discoveries
            .insert(id.clone(), d.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateIpamResourceDiscovery",
        &req.request_id,
        &format!(
            "<ipamResourceDiscovery>{}</ipamResourceDiscovery>",
            rd_xml(&d, &tags, &owner, &region)
        ),
    ))
}

pub(crate) fn delete_ipam_resource_discovery(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "IpamResourceDiscoveryId")?;
    let owner = req.account_id.clone();
    let region = region_of(req);
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let d = state
        .ipam_resource_discoveries
        .remove(&id)
        .unwrap_or(IpamResourceDiscovery { id: id.clone() });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteIpamResourceDiscovery",
        &req.request_id,
        &format!(
            "<ipamResourceDiscovery>{}</ipamResourceDiscovery>",
            rd_xml(&d, &tags, &owner, &region)
        ),
    ))
}

pub(crate) fn describe_ipam_resource_discoveries(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let owner = req.account_id.clone();
    let region = region_of(req);
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .ipam_resource_discoveries
        .values()
        .map(|d| rd_xml(d, state.tags_for(&d.id), &owner, &region))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeIpamResourceDiscoveries",
        &req.request_id,
        &ec2_list("ipamResourceDiscoverySet", &items),
    ))
}

pub(crate) fn modify_ipam_resource_discovery(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "IpamResourceDiscoveryId")?;
    let owner = req.account_id.clone();
    let region = region_of(req);
    let accounts = svc.state.read();
    let d = accounts
        .get(&req.account_id)
        .and_then(|s| s.ipam_resource_discoveries.get(&id).cloned())
        .unwrap_or(IpamResourceDiscovery { id: id.clone() });
    let tags = accounts
        .get(&req.account_id)
        .map(|s| s.tags_for(&id).to_vec())
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "ModifyIpamResourceDiscovery",
        &req.request_id,
        &format!(
            "<ipamResourceDiscovery>{}</ipamResourceDiscovery>",
            rd_xml(&d, &tags, &owner, &region)
        ),
    ))
}

fn rd_assoc_xml(
    assoc_id: &str,
    disco_id: &str,
    ipam_id: &str,
    owner: &str,
    region: &str,
) -> String {
    format!(
        "{}{}{}{}<isDefault>false</isDefault><resourceDiscoveryStatus>active</resourceDiscoveryStatus><state>associate-complete</state>",
        ec2_elem("ipamResourceDiscoveryAssociationId", assoc_id),
        ec2_elem("ipamResourceDiscoveryId", disco_id),
        ec2_elem("ipamId", ipam_id),
        ec2_elem("ownerId", owner),
    ) + &ec2_elem("ipamRegion", region)
}

pub(crate) fn associate_ipam_resource_discovery(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ipam = require(&req.query_params, "IpamId")?;
    let disco = require(&req.query_params, "IpamResourceDiscoveryId")?;
    let assoc = gen_id("ipam-res-disco-assoc");
    let region = region_of(req);
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .ipam_rd_associations
            .insert(assoc.clone(), (disco.clone(), ipam.clone()));
    }
    Ok(Ec2Service::respond(
        "AssociateIpamResourceDiscovery",
        &req.request_id,
        &format!(
            "<ipamResourceDiscoveryAssociation>{}</ipamResourceDiscoveryAssociation>",
            rd_assoc_xml(&assoc, &disco, &ipam, &req.account_id, &region)
        ),
    ))
}

pub(crate) fn disassociate_ipam_resource_discovery(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let assoc = require(&req.query_params, "IpamResourceDiscoveryAssociationId")?;
    let region = region_of(req);
    let (disco, ipam) = {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .ipam_rd_associations
            .remove(&assoc)
            .unwrap_or_default()
    };
    Ok(Ec2Service::respond(
        "DisassociateIpamResourceDiscovery",
        &req.request_id,
        &format!(
            "<ipamResourceDiscoveryAssociation>{}</ipamResourceDiscoveryAssociation>",
            rd_assoc_xml(&assoc, &disco, &ipam, &req.account_id, &region)
        ),
    ))
}

pub(crate) fn describe_ipam_resource_discovery_associations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let region = region_of(req);
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .ipam_rd_associations
        .iter()
        .map(|(a, (d, i))| rd_assoc_xml(a, d, i, &owner, &region))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeIpamResourceDiscoveryAssociations",
        &req.request_id,
        &ec2_list("ipamResourceDiscoveryAssociationSet", &items),
    ))
}

// ---- discovered getters (synthesized empty sets) ----

pub(crate) fn get_ipam_discovered_accounts(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "IpamResourceDiscoveryId")?;
    require(&req.query_params, "DiscoveryRegion")?;
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "GetIpamDiscoveredAccounts",
        &req.request_id,
        &ec2_list("ipamDiscoveredAccountSet", &[]),
    ))
}

pub(crate) fn get_ipam_discovered_public_addresses(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "IpamResourceDiscoveryId")?;
    require(&req.query_params, "AddressRegion")?;
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "GetIpamDiscoveredPublicAddresses",
        &req.request_id,
        &ec2_list("ipamDiscoveredPublicAddressSet", &[]),
    ))
}

pub(crate) fn get_ipam_discovered_resource_cidrs(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "IpamResourceDiscoveryId")?;
    require(&req.query_params, "ResourceRegion")?;
    validate_max_results(&req.query_params, 5, 1000)?;
    Ok(Ec2Service::respond(
        "GetIpamDiscoveredResourceCidrs",
        &req.request_id,
        &ec2_list("ipamDiscoveredResourceCidrSet", &[]),
    ))
}

// ---- BYOASN ----

fn asn_assoc_xml(asn: &str, cidr: &str) -> String {
    format!(
        "{}{}<statusMessage/><state>associated</state>",
        ec2_elem("asn", asn),
        ec2_elem("cidr", cidr)
    )
}

fn byoasn_xml(asn: &str, ipam: &str) -> String {
    format!(
        "{}{}<statusMessage/><state>provisioned</state>",
        ec2_elem("asn", asn),
        ec2_elem("ipamId", ipam)
    )
}

pub(crate) fn associate_ipam_byoasn(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let asn = require(&req.query_params, "Asn")?;
    let cidr = require(&req.query_params, "Cidr")?;
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .ipam_byoasns
            .insert(asn.clone(), cidr.clone());
    }
    Ok(Ec2Service::respond(
        "AssociateIpamByoasn",
        &req.request_id,
        &format!(
            "<asnAssociation>{}</asnAssociation>",
            asn_assoc_xml(&asn, &cidr)
        ),
    ))
}

pub(crate) fn disassociate_ipam_byoasn(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let asn = require(&req.query_params, "Asn")?;
    let cidr = require(&req.query_params, "Cidr")?;
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .ipam_byoasns
            .remove(&asn);
    }
    Ok(Ec2Service::respond(
        "DisassociateIpamByoasn",
        &req.request_id,
        &format!(
            "<asnAssociation>{}</asnAssociation>",
            asn_assoc_xml(&asn, &cidr)
        ),
    ))
}

pub(crate) fn provision_ipam_byoasn(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ipam = require(&req.query_params, "IpamId")?;
    let asn = require(&req.query_params, "Asn")?;
    require_struct(&req.query_params, "AsnAuthorizationContext")?;
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .ipam_byoasns
            .entry(asn.clone())
            .or_default();
    }
    Ok(Ec2Service::respond(
        "ProvisionIpamByoasn",
        &req.request_id,
        &format!("<byoasn>{}</byoasn>", byoasn_xml(&asn, &ipam)),
    ))
}

pub(crate) fn deprovision_ipam_byoasn(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ipam = require(&req.query_params, "IpamId")?;
    let asn = require(&req.query_params, "Asn")?;
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .ipam_byoasns
            .remove(&asn);
    }
    Ok(Ec2Service::respond(
        "DeprovisionIpamByoasn",
        &req.request_id,
        &format!("<byoasn>{}</byoasn>", byoasn_xml(&asn, &ipam)),
    ))
}

pub(crate) fn describe_ipam_byoasn(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 100)?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .ipam_byoasns
        .keys()
        .map(|asn| {
            format!(
                "{}<statusMessage/><state>provisioned</state>",
                ec2_elem("asn", asn)
            )
        })
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeIpamByoasn",
        &req.request_id,
        &ec2_list("byoasnSet", &items),
    ))
}

pub(crate) fn move_byoip_cidr_to_ipam(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let cidr = require(&req.query_params, "Cidr")?;
    require(&req.query_params, "IpamPoolId")?;
    require(&req.query_params, "IpamPoolOwner")?;
    Ok(Ec2Service::respond(
        "MoveByoipCidrToIpam",
        &req.request_id,
        &format!(
            "<byoipCidr>{}<state>provisioned</state></byoipCidr>",
            ec2_elem("cidr", &cidr)
        ),
    ))
}

// ---- external resource verification tokens ----

fn token_xml(id: &str, ipam: &str, owner: &str, region: &str) -> String {
    format!(
        "{}{}{}{}<tokenValue>{}</tokenValue><tokenName>token</tokenName><status>valid</status><state>create-complete</state>",
        ec2_elem("ipamExternalResourceVerificationTokenId", id),
        ec2_elem("ipamExternalResourceVerificationTokenArn", &format!("arn:aws:ec2::{owner}:ipam-external-resource-verification-token/{id}")),
        ec2_elem("ipamId", ipam),
        ec2_elem("ipamRegion", region),
        id,
    )
}

pub(crate) fn create_ipam_external_resource_verification_token(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ipam = require(&req.query_params, "IpamId")?;
    let id = gen_id("ipam-ext-token");
    let owner = req.account_id.clone();
    let region = region_of(req);
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "ipam-external-resource-verification-token",
        );
        state.ipam_ext_tokens.insert(id.clone(), ipam.clone());
    }
    Ok(Ec2Service::respond(
        "CreateIpamExternalResourceVerificationToken",
        &req.request_id,
        &format!(
            "<ipamExternalResourceVerificationToken>{}</ipamExternalResourceVerificationToken>",
            token_xml(&id, &ipam, &owner, &region)
        ),
    ))
}

pub(crate) fn delete_ipam_external_resource_verification_token(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "IpamExternalResourceVerificationTokenId")?;
    let owner = req.account_id.clone();
    let region = region_of(req);
    let ipam = {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .ipam_ext_tokens
            .remove(&id)
            .unwrap_or_else(|| "ipam-0".to_string())
    };
    Ok(Ec2Service::respond(
        "DeleteIpamExternalResourceVerificationToken",
        &req.request_id,
        &format!(
            "<ipamExternalResourceVerificationToken>{}</ipamExternalResourceVerificationToken>",
            token_xml(&id, &ipam, &owner, &region)
        ),
    ))
}

pub(crate) fn describe_ipam_external_resource_verification_tokens(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let owner = req.account_id.clone();
    let region = region_of(req);
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .ipam_ext_tokens
        .iter()
        .map(|(id, ipam)| token_xml(id, ipam, &owner, &region))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeIpamExternalResourceVerificationTokens",
        &req.request_id,
        &ec2_list("ipamExternalResourceVerificationTokenSet", &items),
    ))
}
