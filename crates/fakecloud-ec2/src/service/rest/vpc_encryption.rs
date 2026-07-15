//! EC2 vpc encryption operations (extracted from the rest long-tail module).

#![allow(clippy::too_many_lines)]

use super::*;

fn vpc_encryption_control_xml(c: &VpcEncryptionControl, tags: &[Tag]) -> String {
    let exclusions: String = VPC_ENC_EXCLUSIONS
        .iter()
        .map(|(name, _)| {
            let st = c
                .exclusions
                .get(*name)
                .map(String::as_str)
                .unwrap_or("disabled");
            format!("<{name}><state>{st}</state></{name}>")
        })
        .collect();
    format!(
        "{}{}{}{}<resourceExclusions>{}</resourceExclusions>{}",
        ec2_elem("vpcId", &c.vpc_id),
        ec2_elem("vpcEncryptionControlId", &c.id),
        ec2_elem("mode", &c.mode),
        ec2_elem("state", &c.state),
        exclusions,
        super::super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_vpc_encryption_control(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let vpc_id = require(&req.query_params, "VpcId")?;
    let id = gen_id("vpc-enc");
    let c = VpcEncryptionControl {
        id: id.clone(),
        vpc_id,
        mode: req
            .query_params
            .get("Mode")
            .cloned()
            .unwrap_or_else(|| "monitor".to_string()),
        state: "available".to_string(),
        exclusions: std::collections::BTreeMap::new(),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "vpc-encryption-control",
        );
        let tg = state.tags_for(&id).to_vec();
        state.vpc_encryption_controls.insert(id.clone(), c.clone());
        tg
    };
    Ok(Ec2Service::respond(
        "CreateVpcEncryptionControl",
        &req.request_id,
        &format!(
            "<vpcEncryptionControl>{}</vpcEncryptionControl>",
            vpc_encryption_control_xml(&c, &tags)
        ),
    ))
}

pub(crate) fn delete_vpc_encryption_control(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VpcEncryptionControlId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let mut c = state
        .vpc_encryption_controls
        .remove(&id)
        .unwrap_or_else(|| VpcEncryptionControl {
            id: id.clone(),
            vpc_id: String::new(),
            mode: "monitor".to_string(),
            state: String::new(),
            exclusions: std::collections::BTreeMap::new(),
        });
    c.state = "deleting".to_string();
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteVpcEncryptionControl",
        &req.request_id,
        &format!(
            "<vpcEncryptionControl>{}</vpcEncryptionControl>",
            vpc_encryption_control_xml(&c, &tags)
        ),
    ))
}

pub(crate) fn describe_vpc_encryption_controls(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "VpcEncryptionControlId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .vpc_encryption_controls
        .values()
        .filter(|c| wanted.is_empty() || wanted.contains(&c.id))
        .map(|c| vpc_encryption_control_xml(c, state.tags_for(&c.id)))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeVpcEncryptionControls",
        &req.request_id,
        &ec2_list("vpcEncryptionControlSet", &items),
    ))
}

pub(crate) fn modify_vpc_encryption_control(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "VpcEncryptionControlId")?;
    validate_enum(&req.query_params, "Mode", &["monitor", "enforce"])?;
    validate_enum(
        &req.query_params,
        "InternetGatewayExclusion",
        &["enable", "disable"],
    )?;
    validate_enum(
        &req.query_params,
        "EgressOnlyInternetGatewayExclusion",
        &["enable", "disable"],
    )?;
    validate_enum(
        &req.query_params,
        "NatGatewayExclusion",
        &["enable", "disable"],
    )?;
    validate_enum(
        &req.query_params,
        "VirtualPrivateGatewayExclusion",
        &["enable", "disable"],
    )?;
    validate_enum(
        &req.query_params,
        "VpcPeeringExclusion",
        &["enable", "disable"],
    )?;
    validate_enum(&req.query_params, "LambdaExclusion", &["enable", "disable"])?;
    validate_enum(
        &req.query_params,
        "VpcLatticeExclusion",
        &["enable", "disable"],
    )?;
    validate_enum(
        &req.query_params,
        "ElasticFileSystemExclusion",
        &["enable", "disable"],
    )?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    // Mutate when present; synthesize for a probe-only synthetic id.
    let mut synth = VpcEncryptionControl {
        id: id.clone(),
        vpc_id: String::new(),
        mode: "monitor".to_string(),
        state: "available".to_string(),
        exclusions: std::collections::BTreeMap::new(),
    };
    let entry = state
        .vpc_encryption_controls
        .get_mut(&id)
        .unwrap_or(&mut synth);
    if let Some(m) = req.query_params.get("Mode") {
        entry.mode = m.clone();
    }
    for (name, param) in VPC_ENC_EXCLUSIONS {
        if let Some(v) = req.query_params.get(*param).filter(|v| !v.is_empty()) {
            let st = if v == "enable" { "enabled" } else { "disabled" };
            entry.exclusions.insert((*name).to_string(), st.to_string());
        }
    }
    let c = entry.clone();
    let tags = state.tags_for(&id).to_vec();
    Ok(Ec2Service::respond(
        "ModifyVpcEncryptionControl",
        &req.request_id,
        &format!(
            "<vpcEncryptionControl>{}</vpcEncryptionControl>",
            vpc_encryption_control_xml(&c, &tags)
        ),
    ))
}

fn account_vpc_encryption_control_xml(c: &AccountVpcEncryptionControl) -> String {
    let exclusions: String = ACCOUNT_VPC_ENC_EXCLUSIONS
        .iter()
        .map(|(name, _)| {
            let st = c
                .exclusions
                .get(*name)
                .map(String::as_str)
                .unwrap_or("disabled");
            format!("<{name}><state>{st}</state></{name}>")
        })
        .collect();
    format!(
        "{}{}<exclusions>{}</exclusions>{}{}",
        ec2_elem("state", &c.state),
        ec2_elem("mode", &c.mode),
        exclusions,
        ec2_elem("managedBy", "account"),
        ec2_elem("lastUpdateTimestamp", "2024-01-01T00:00:00.000Z"),
    )
}

pub(crate) fn describe_account_vpc_encryption_control(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = svc.state.read();
    let default = AccountVpcEncryptionControl::default();
    let c = accounts
        .get(&req.account_id)
        .and_then(|s| s.account_vpc_encryption_control.as_ref())
        .unwrap_or(&default);
    Ok(Ec2Service::respond(
        "DescribeAccountVpcEncryptionControl",
        &req.request_id,
        &format!(
            "<accountVpcEncryptionControl>{}</accountVpcEncryptionControl>",
            account_vpc_encryption_control_xml(c)
        ),
    ))
}

pub(crate) fn modify_account_vpc_encryption_control(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(
        &req.query_params,
        "Mode",
        &["unmanaged", "attempt-monitor", "attempt-enforce"],
    )?;
    for (_, param) in ACCOUNT_VPC_ENC_EXCLUSIONS {
        validate_enum(&req.query_params, param, &["enable", "disable"])?;
    }
    let c = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let entry = state
            .account_vpc_encryption_control
            .get_or_insert_with(AccountVpcEncryptionControl::default);
        if let Some(m) = req.query_params.get("Mode").filter(|v| !v.is_empty()) {
            entry.mode = m.clone();
        }
        for (name, param) in ACCOUNT_VPC_ENC_EXCLUSIONS {
            if let Some(v) = req.query_params.get(*param).filter(|v| !v.is_empty()) {
                let st = if v == "enable" { "enabled" } else { "disabled" };
                entry.exclusions.insert((*name).to_string(), st.to_string());
            }
        }
        // A completed modification leaves the control in a successful state.
        entry.state = "transitions-successful".to_string();
        entry.clone()
    };
    Ok(Ec2Service::respond(
        "ModifyAccountVpcEncryptionControl",
        &req.request_id,
        &format!(
            "<accountVpcEncryptionControl>{}</accountVpcEncryptionControl>",
            account_vpc_encryption_control_xml(&c)
        ),
    ))
}
