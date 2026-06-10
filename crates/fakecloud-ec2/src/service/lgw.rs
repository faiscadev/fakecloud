//! Outpost / local-gateway family: carrier gateways, CoIP pools + CIDRs, local
//! gateway route tables, routes, virtual interfaces, groups, and associations.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{gen_id, require, validate_enum, validate_max_results};
use crate::state::{
    CarrierGateway, CoipPool, Ec2State, LocalGatewayRouteTable, LocalGatewayRouteTableVifgAssoc,
    LocalGatewayRouteTableVpcAssoc, LocalGatewayVif, LocalGatewayVifGroup, Tag,
};

fn mr(req: &AwsRequest) -> Result<(), AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)
}

fn owner_arn(owner: &str, kind: &str, id: &str) -> String {
    format!("arn:aws:ec2:us-east-1:{owner}:{kind}/{id}")
}

// ---- carrier gateways ----

fn carrier_xml(c: &CarrierGateway, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}<state>available</state>{}{}",
        ec2_elem("carrierGatewayId", &c.id),
        ec2_elem("vpcId", &c.vpc_id),
        ec2_elem("ownerId", owner),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_carrier_gateway(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let vpc = require(&req.query_params, "VpcId")?;
    let id = gen_id("cagw");
    let c = CarrierGateway {
        id: id.clone(),
        vpc_id: vpc,
    };
    let owner = req.account_id.clone();
    let tags = ins(svc, req, &id, "carrier-gateway", |s| {
        s.carrier_gateways.insert(id.clone(), c.clone());
    });
    Ok(Ec2Service::respond(
        "CreateCarrierGateway",
        &req.request_id,
        &format!(
            "<carrierGateway>{}</carrierGateway>",
            carrier_xml(&c, &tags, &owner)
        ),
    ))
}

pub(crate) fn delete_carrier_gateway(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "CarrierGatewayId")?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let c = state
        .carrier_gateways
        .remove(&id)
        .unwrap_or(CarrierGateway {
            id: id.clone(),
            vpc_id: "vpc-0".to_string(),
        });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteCarrierGateway",
        &req.request_id,
        &format!(
            "<carrierGateway>{}</carrierGateway>",
            carrier_xml(&c, &tags, &owner)
        ),
    ))
}

pub(crate) fn describe_carrier_gateways(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .carrier_gateways
        .values()
        .map(|c| carrier_xml(c, state.tags_for(&c.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeCarrierGateways",
        &req.request_id,
        &ec2_list("carrierGatewaySet", &items),
    ))
}

// shared insert-with-tags helper
fn ins(
    svc: &Ec2Service,
    req: &AwsRequest,
    id: &str,
    resource: &str,
    f: impl FnOnce(&mut Ec2State),
) -> Vec<Tag> {
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    crate::service::tags::apply_tag_specifications(state, &req.query_params, id, resource);
    f(state);
    state.tags_for(id).to_vec()
}

// ---- CoIP pools + CIDRs ----

fn coip_pool_xml(p: &CoipPool, cidrs: &[String], tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}{}",
        ec2_elem("poolId", &p.id),
        ec2_list(
            "poolCidrSet",
            &cidrs
                .iter()
                .map(|c| format!("<item>{c}</item>"))
                .collect::<Vec<_>>()
        ),
        ec2_elem("localGatewayRouteTableId", &p.route_table_id)
            + &ec2_elem("poolArn", &owner_arn(owner, "coip-pool", &p.id)),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_coip_pool(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rt = require(&req.query_params, "LocalGatewayRouteTableId")?;
    let id = gen_id("ipv4pool-coip");
    let p = CoipPool {
        id: id.clone(),
        route_table_id: rt,
    };
    let owner = req.account_id.clone();
    let tags = ins(svc, req, &id, "coip-pool", |s| {
        s.coip_pools.insert(id.clone(), p.clone());
    });
    Ok(Ec2Service::respond(
        "CreateCoipPool",
        &req.request_id,
        &format!(
            "<coipPool>{}</coipPool>",
            coip_pool_xml(&p, &[], &tags, &owner)
        ),
    ))
}

pub(crate) fn delete_coip_pool(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "CoipPoolId")?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let p = state.coip_pools.remove(&id).unwrap_or(CoipPool {
        id: id.clone(),
        route_table_id: "lgw-rtb-0".to_string(),
    });
    let cidrs = state.coip_pool_cidrs.remove(&id).unwrap_or_default();
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteCoipPool",
        &req.request_id,
        &format!(
            "<coipPool>{}</coipPool>",
            coip_pool_xml(&p, &cidrs, &tags, &owner)
        ),
    ))
}

pub(crate) fn describe_coip_pools(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .coip_pools
        .values()
        .map(|p| {
            let cidrs = state
                .coip_pool_cidrs
                .get(&p.id)
                .cloned()
                .unwrap_or_default();
            coip_pool_xml(p, &cidrs, state.tags_for(&p.id), &owner)
        })
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeCoipPools",
        &req.request_id,
        &ec2_list("coipPoolSet", &items),
    ))
}

fn coip_cidr_xml(cidr: &str, pool: &str, rt: &str) -> String {
    format!(
        "{}{}{}",
        ec2_elem("cidr", cidr),
        ec2_elem("coipPoolId", pool),
        ec2_elem("localGatewayRouteTableId", rt)
    )
}

pub(crate) fn create_coip_cidr(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let cidr = require(&req.query_params, "Cidr")?;
    let pool = require(&req.query_params, "CoipPoolId")?;
    let rt = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .coip_pool_cidrs
            .entry(pool.clone())
            .or_default()
            .push(cidr.clone());
        state
            .coip_pools
            .get(&pool)
            .map(|p| p.route_table_id.clone())
            .unwrap_or_else(|| "lgw-rtb-0".to_string())
    };
    Ok(Ec2Service::respond(
        "CreateCoipCidr",
        &req.request_id,
        &format!("<coipCidr>{}</coipCidr>", coip_cidr_xml(&cidr, &pool, &rt)),
    ))
}

pub(crate) fn delete_coip_cidr(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let cidr = require(&req.query_params, "Cidr")?;
    let pool = require(&req.query_params, "CoipPoolId")?;
    let rt = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(cs) = state.coip_pool_cidrs.get_mut(&pool) {
            cs.retain(|c| c != &cidr);
        }
        state
            .coip_pools
            .get(&pool)
            .map(|p| p.route_table_id.clone())
            .unwrap_or_else(|| "lgw-rtb-0".to_string())
    };
    Ok(Ec2Service::respond(
        "DeleteCoipCidr",
        &req.request_id,
        &format!("<coipCidr>{}</coipCidr>", coip_cidr_xml(&cidr, &pool, &rt)),
    ))
}

pub(crate) fn get_coip_pool_usage(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let pool = require(&req.query_params, "PoolId")?;
    mr(req)?;
    let body = format!(
        "{}{}<localGatewayRouteTableId>lgw-rtb-0</localGatewayRouteTableId>",
        ec2_elem("coipPoolId", &pool),
        ec2_list("coipAddressUsageSet", &[])
    );
    Ok(Ec2Service::respond(
        "GetCoipPoolUsage",
        &req.request_id,
        &body,
    ))
}

// ---- local gateway route tables ----

fn lgrt_xml(rt: &LocalGatewayRouteTable, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}<outpostArn>arn:aws:outposts:us-east-1:{owner}:outpost/op-0</outpostArn>{}<state>available</state><mode>{}</mode>{}",
        ec2_elem("localGatewayRouteTableId", &rt.id),
        ec2_elem("localGatewayRouteTableArn", &owner_arn(owner, "local-gateway-route-table", &rt.id)),
        ec2_elem("localGatewayId", &rt.local_gateway_id),
        ec2_elem("ownerId", owner),
        rt.mode,
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_local_gateway_route_table(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let lgw = require(&req.query_params, "LocalGatewayId")?;
    validate_enum(&req.query_params, "Mode", &["direct-vpc-routing", "coip"])?;
    let id = gen_id("lgw-rtb");
    let rt = LocalGatewayRouteTable {
        id: id.clone(),
        local_gateway_id: lgw,
        mode: req
            .query_params
            .get("Mode")
            .cloned()
            .unwrap_or_else(|| "direct-vpc-routing".to_string()),
    };
    let owner = req.account_id.clone();
    let tags = ins(svc, req, &id, "local-gateway-route-table", |s| {
        s.lg_route_tables.insert(id.clone(), rt.clone());
    });
    Ok(Ec2Service::respond(
        "CreateLocalGatewayRouteTable",
        &req.request_id,
        &format!(
            "<localGatewayRouteTable>{}</localGatewayRouteTable>",
            lgrt_xml(&rt, &tags, &owner)
        ),
    ))
}

pub(crate) fn delete_local_gateway_route_table(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "LocalGatewayRouteTableId")?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let rt = state
        .lg_route_tables
        .remove(&id)
        .unwrap_or(LocalGatewayRouteTable {
            id: id.clone(),
            local_gateway_id: "lgw-0".to_string(),
            mode: "direct-vpc-routing".to_string(),
        });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    state.lg_routes.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteLocalGatewayRouteTable",
        &req.request_id,
        &format!(
            "<localGatewayRouteTable>{}</localGatewayRouteTable>",
            lgrt_xml(&rt, &tags, &owner)
        ),
    ))
}

pub(crate) fn describe_local_gateway_route_tables(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .lg_route_tables
        .values()
        .map(|rt| lgrt_xml(rt, state.tags_for(&rt.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeLocalGatewayRouteTables",
        &req.request_id,
        &ec2_list("localGatewayRouteTableSet", &items),
    ))
}

// ---- local gateway routes ----

fn route_xml(cidr: &str, rt: &str, owner: &str) -> String {
    format!(
        "{}<type>static</type><state>active</state>{}{}{}",
        ec2_elem("destinationCidrBlock", cidr),
        ec2_elem("localGatewayRouteTableId", rt),
        ec2_elem(
            "localGatewayRouteTableArn",
            &owner_arn(owner, "local-gateway-route-table", rt)
        ),
        ec2_elem("ownerId", owner),
    )
}

pub(crate) fn create_local_gateway_route(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rt = require(&req.query_params, "LocalGatewayRouteTableId")?;
    let cidr = req
        .query_params
        .get("DestinationCidrBlock")
        .cloned()
        .unwrap_or_else(|| "0.0.0.0/0".to_string());
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .lg_routes
            .entry(rt.clone())
            .or_default()
            .push(cidr.clone());
    }
    Ok(Ec2Service::respond(
        "CreateLocalGatewayRoute",
        &req.request_id,
        &format!("<route>{}</route>", route_xml(&cidr, &rt, &req.account_id)),
    ))
}

pub(crate) fn delete_local_gateway_route(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rt = require(&req.query_params, "LocalGatewayRouteTableId")?;
    let cidr = req
        .query_params
        .get("DestinationCidrBlock")
        .cloned()
        .unwrap_or_default();
    {
        let mut accounts = svc.state.write();
        if let Some(rs) = accounts
            .get_or_create(&req.account_id)
            .lg_routes
            .get_mut(&rt)
        {
            rs.retain(|c| c != &cidr);
        }
    }
    Ok(Ec2Service::respond(
        "DeleteLocalGatewayRoute",
        &req.request_id,
        &format!("<route>{}</route>", route_xml(&cidr, &rt, &req.account_id)),
    ))
}

pub(crate) fn modify_local_gateway_route(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rt = require(&req.query_params, "LocalGatewayRouteTableId")?;
    let cidr = req
        .query_params
        .get("DestinationCidrBlock")
        .cloned()
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "ModifyLocalGatewayRoute",
        &req.request_id,
        &format!("<route>{}</route>", route_xml(&cidr, &rt, &req.account_id)),
    ))
}

pub(crate) fn search_local_gateway_routes(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rt = require(&req.query_params, "LocalGatewayRouteTableId")?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let items: Vec<String> = accounts
        .get(&req.account_id)
        .and_then(|s| s.lg_routes.get(&rt))
        .map(|rs| rs.iter().map(|c| route_xml(c, &rt, &owner)).collect())
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "SearchLocalGatewayRoutes",
        &req.request_id,
        &ec2_list("routeSet", &items),
    ))
}

// ---- VPC associations ----

fn vpc_assoc_xml(a: &LocalGatewayRouteTableVpcAssoc, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}{}<state>associated</state>{}",
        ec2_elem("localGatewayRouteTableVpcAssociationId", &a.id),
        ec2_elem("localGatewayRouteTableId", &a.route_table_id),
        ec2_elem("vpcId", &a.vpc_id),
        ec2_elem("ownerId", owner),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_local_gateway_route_table_vpc_association(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rt = require(&req.query_params, "LocalGatewayRouteTableId")?;
    let vpc = require(&req.query_params, "VpcId")?;
    let id = gen_id("lgw-vpc-assoc");
    let a = LocalGatewayRouteTableVpcAssoc {
        id: id.clone(),
        route_table_id: rt,
        vpc_id: vpc,
    };
    let owner = req.account_id.clone();
    let tags = ins(
        svc,
        req,
        &id,
        "local-gateway-route-table-vpc-association",
        |s| {
            s.lg_rt_vpc_assocs.insert(id.clone(), a.clone());
        },
    );
    Ok(Ec2Service::respond(
        "CreateLocalGatewayRouteTableVpcAssociation",
        &req.request_id,
        &format!(
            "<localGatewayRouteTableVpcAssociation>{}</localGatewayRouteTableVpcAssociation>",
            vpc_assoc_xml(&a, &tags, &owner)
        ),
    ))
}

pub(crate) fn delete_local_gateway_route_table_vpc_association(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "LocalGatewayRouteTableVpcAssociationId")?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let a = state
        .lg_rt_vpc_assocs
        .remove(&id)
        .unwrap_or(LocalGatewayRouteTableVpcAssoc {
            id: id.clone(),
            route_table_id: "lgw-rtb-0".to_string(),
            vpc_id: "vpc-0".to_string(),
        });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteLocalGatewayRouteTableVpcAssociation",
        &req.request_id,
        &format!(
            "<localGatewayRouteTableVpcAssociation>{}</localGatewayRouteTableVpcAssociation>",
            vpc_assoc_xml(&a, &tags, &owner)
        ),
    ))
}

pub(crate) fn describe_local_gateway_route_table_vpc_associations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .lg_rt_vpc_assocs
        .values()
        .map(|a| vpc_assoc_xml(a, state.tags_for(&a.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeLocalGatewayRouteTableVpcAssociations",
        &req.request_id,
        &ec2_list("localGatewayRouteTableVpcAssociationSet", &items),
    ))
}

// ---- virtual interfaces ----

fn vif_xml(v: &LocalGatewayVif, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}<vlan>{}</vlan>{}{}{}{}",
        ec2_elem("localGatewayVirtualInterfaceId", &v.id),
        ec2_elem("localGatewayVirtualInterfaceGroupId", &v.group_id),
        v.vlan,
        ec2_elem("localAddress", &v.local_address),
        ec2_elem("peerAddress", &v.peer_address),
        ec2_elem("ownerId", owner),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_local_gateway_virtual_interface(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let group = require(&req.query_params, "LocalGatewayVirtualInterfaceGroupId")?;
    require(&req.query_params, "OutpostLagId")?;
    let vlan = require(&req.query_params, "Vlan")?;
    let local = require(&req.query_params, "LocalAddress")?;
    let peer = require(&req.query_params, "PeerAddress")?;
    let id = gen_id("lgw-vif");
    let v = LocalGatewayVif {
        id: id.clone(),
        group_id: group,
        vlan,
        local_address: local,
        peer_address: peer,
    };
    let owner = req.account_id.clone();
    let tags = ins(svc, req, &id, "local-gateway-virtual-interface", |s| {
        s.lg_virtual_interfaces.insert(id.clone(), v.clone());
    });
    Ok(Ec2Service::respond(
        "CreateLocalGatewayVirtualInterface",
        &req.request_id,
        &format!(
            "<localGatewayVirtualInterface>{}</localGatewayVirtualInterface>",
            vif_xml(&v, &tags, &owner)
        ),
    ))
}

pub(crate) fn delete_local_gateway_virtual_interface(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "LocalGatewayVirtualInterfaceId")?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let v = state
        .lg_virtual_interfaces
        .remove(&id)
        .unwrap_or(LocalGatewayVif {
            id: id.clone(),
            group_id: "lgw-vif-grp-0".to_string(),
            vlan: "0".to_string(),
            local_address: "10.0.0.1".to_string(),
            peer_address: "10.0.0.2".to_string(),
        });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteLocalGatewayVirtualInterface",
        &req.request_id,
        &format!(
            "<localGatewayVirtualInterface>{}</localGatewayVirtualInterface>",
            vif_xml(&v, &tags, &owner)
        ),
    ))
}

pub(crate) fn describe_local_gateway_virtual_interfaces(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .lg_virtual_interfaces
        .values()
        .map(|v| vif_xml(v, state.tags_for(&v.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeLocalGatewayVirtualInterfaces",
        &req.request_id,
        &ec2_list("localGatewayVirtualInterfaceSet", &items),
    ))
}

fn vifg_xml(g: &LocalGatewayVifGroup, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}{}",
        ec2_elem("localGatewayVirtualInterfaceGroupId", &g.id),
        ec2_elem("localGatewayId", &g.local_gateway_id),
        ec2_elem("ownerId", owner),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_local_gateway_virtual_interface_group(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let lgw = require(&req.query_params, "LocalGatewayId")?;
    let id = gen_id("lgw-vif-grp");
    let g = LocalGatewayVifGroup {
        id: id.clone(),
        local_gateway_id: lgw,
    };
    let owner = req.account_id.clone();
    let tags = ins(
        svc,
        req,
        &id,
        "local-gateway-virtual-interface-group",
        |s| {
            s.lg_vif_groups.insert(id.clone(), g.clone());
        },
    );
    Ok(Ec2Service::respond(
        "CreateLocalGatewayVirtualInterfaceGroup",
        &req.request_id,
        &format!(
            "<localGatewayVirtualInterfaceGroup>{}</localGatewayVirtualInterfaceGroup>",
            vifg_xml(&g, &tags, &owner)
        ),
    ))
}

pub(crate) fn delete_local_gateway_virtual_interface_group(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "LocalGatewayVirtualInterfaceGroupId")?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let g = state
        .lg_vif_groups
        .remove(&id)
        .unwrap_or(LocalGatewayVifGroup {
            id: id.clone(),
            local_gateway_id: "lgw-0".to_string(),
        });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteLocalGatewayVirtualInterfaceGroup",
        &req.request_id,
        &format!(
            "<localGatewayVirtualInterfaceGroup>{}</localGatewayVirtualInterfaceGroup>",
            vifg_xml(&g, &tags, &owner)
        ),
    ))
}

pub(crate) fn describe_local_gateway_virtual_interface_groups(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .lg_vif_groups
        .values()
        .map(|g| vifg_xml(g, state.tags_for(&g.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeLocalGatewayVirtualInterfaceGroups",
        &req.request_id,
        &ec2_list("localGatewayVirtualInterfaceGroupSet", &items),
    ))
}

// ---- route-table <-> vif-group associations ----

fn vifg_assoc_xml(a: &LocalGatewayRouteTableVifgAssoc, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}<state>associated</state>{}{}",
        ec2_elem(
            "localGatewayRouteTableVirtualInterfaceGroupAssociationId",
            &a.id
        ),
        ec2_elem("localGatewayVirtualInterfaceGroupId", &a.vif_group_id),
        ec2_elem("localGatewayRouteTableId", &a.route_table_id),
        ec2_elem("ownerId", owner),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_local_gateway_route_table_virtual_interface_group_association(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let rt = require(&req.query_params, "LocalGatewayRouteTableId")?;
    let grp = require(&req.query_params, "LocalGatewayVirtualInterfaceGroupId")?;
    let id = gen_id("lgw-vif-grp-assoc");
    let a = LocalGatewayRouteTableVifgAssoc {
        id: id.clone(),
        route_table_id: rt,
        vif_group_id: grp,
    };
    let owner = req.account_id.clone();
    let tags = ins(
        svc,
        req,
        &id,
        "local-gateway-route-table-virtual-interface-group-association",
        |s| {
            s.lg_rt_vifg_assocs.insert(id.clone(), a.clone());
        },
    );
    Ok(Ec2Service::respond("CreateLocalGatewayRouteTableVirtualInterfaceGroupAssociation", &req.request_id, &format!("<localGatewayRouteTableVirtualInterfaceGroupAssociation>{}</localGatewayRouteTableVirtualInterfaceGroupAssociation>", vifg_assoc_xml(&a, &tags, &owner))))
}

pub(crate) fn delete_local_gateway_route_table_virtual_interface_group_association(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(
        &req.query_params,
        "LocalGatewayRouteTableVirtualInterfaceGroupAssociationId",
    )?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let a = state
        .lg_rt_vifg_assocs
        .remove(&id)
        .unwrap_or(LocalGatewayRouteTableVifgAssoc {
            id: id.clone(),
            route_table_id: "lgw-rtb-0".to_string(),
            vif_group_id: "lgw-vif-grp-0".to_string(),
        });
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond("DeleteLocalGatewayRouteTableVirtualInterfaceGroupAssociation", &req.request_id, &format!("<localGatewayRouteTableVirtualInterfaceGroupAssociation>{}</localGatewayRouteTableVirtualInterfaceGroupAssociation>", vifg_assoc_xml(&a, &tags, &owner))))
}

pub(crate) fn describe_local_gateway_route_table_virtual_interface_group_associations(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .lg_rt_vifg_assocs
        .values()
        .map(|a| vifg_assoc_xml(a, state.tags_for(&a.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeLocalGatewayRouteTableVirtualInterfaceGroupAssociations",
        &req.request_id,
        &ec2_list(
            "localGatewayRouteTableVirtualInterfaceGroupAssociationSet",
            &items,
        ),
    ))
}

// ---- local gateways (AWS-managed; synthesize empty unless present) ----

pub(crate) fn describe_local_gateways(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    mr(req)?;
    Ok(Ec2Service::respond(
        "DescribeLocalGateways",
        &req.request_id,
        &ec2_list("localGatewaySet", &[]),
    ))
}
