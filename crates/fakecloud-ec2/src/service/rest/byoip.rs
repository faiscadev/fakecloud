//! EC2 byoip operations (extracted from the rest long-tail module).

#![allow(clippy::too_many_lines)]

use super::*;

fn byoip_cidr_xml(c: &ByoipCidr) -> String {
    format!(
        "{}{}{}",
        ec2_elem("cidr", &c.cidr),
        ec2_elem("description", &c.description),
        ec2_elem("state", &c.state),
    )
}

/// Update the persisted BYOIP CIDR's state (inserting it when unknown), then
/// render it. Shared by advertise/withdraw so each op round-trips.
fn set_byoip_state(svc: &Ec2Service, req: &AwsRequest, cidr: &str, state: &str) -> ByoipCidr {
    let mut accounts = svc.state.write();
    let account = accounts.get_or_create(&req.account_id);
    let entry = account
        .byoip_cidrs
        .entry(cidr.to_string())
        .or_insert_with(|| ByoipCidr {
            cidr: cidr.to_string(),
            description: String::new(),
            state: state.to_string(),
        });
    entry.state = state.to_string();
    entry.clone()
}

pub(crate) fn advertise_byoip_cidr(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let cidr = require(&req.query_params, "Cidr")?;
    let c = set_byoip_state(svc, req, &cidr, "advertised");
    Ok(Ec2Service::respond(
        "AdvertiseByoipCidr",
        &req.request_id,
        &format!("<byoipCidr>{}</byoipCidr>", byoip_cidr_xml(&c)),
    ))
}

pub(crate) fn deprovision_byoip_cidr(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let cidr = require(&req.query_params, "Cidr")?;
    // Deprovision removes the CIDR from the pool; report the transitional state.
    let mut c = set_byoip_state(svc, req, &cidr, "pending-deprovision");
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .byoip_cidrs
            .remove(&cidr);
    }
    c.state = "pending-deprovision".to_string();
    Ok(Ec2Service::respond(
        "DeprovisionByoipCidr",
        &req.request_id,
        &format!("<byoipCidr>{}</byoipCidr>", byoip_cidr_xml(&c)),
    ))
}

pub(crate) fn describe_byoip_cidrs(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "MaxResults")?;
    validate_max_results(&req.query_params, 1, 100)?;
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state.byoip_cidrs.values().map(byoip_cidr_xml).collect();
    Ok(Ec2Service::respond(
        "DescribeByoipCidrs",
        &req.request_id,
        &ec2_list("byoipCidrSet", &items),
    ))
}

pub(crate) fn provision_byoip_cidr(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let cidr = require(&req.query_params, "Cidr")?;
    let c = ByoipCidr {
        cidr: cidr.clone(),
        description: req
            .query_params
            .get("Description")
            .cloned()
            .unwrap_or_default(),
        state: "provisioned".to_string(),
    };
    {
        let mut accounts = svc.state.write();
        accounts
            .get_or_create(&req.account_id)
            .byoip_cidrs
            .insert(cidr, c.clone());
    }
    Ok(Ec2Service::respond(
        "ProvisionByoipCidr",
        &req.request_id,
        &format!("<byoipCidr>{}</byoipCidr>", byoip_cidr_xml(&c)),
    ))
}

pub(crate) fn withdraw_byoip_cidr(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let cidr = require(&req.query_params, "Cidr")?;
    let c = set_byoip_state(svc, req, &cidr, "provisioned");
    Ok(Ec2Service::respond(
        "WithdrawByoipCidr",
        &req.request_id,
        &format!("<byoipCidr>{}</byoipCidr>", byoip_cidr_xml(&c)),
    ))
}
