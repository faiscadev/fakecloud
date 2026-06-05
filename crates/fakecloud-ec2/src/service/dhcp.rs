//! DHCP options set operations.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    gen_id, indexed_list, parse_filters, require, validate_max_results, Filter,
};
use crate::state::{DhcpConfig, DhcpOptions, Ec2State, Tag};

/// Render the inner XML of a `<dhcpOptions>` element.
pub(crate) fn dhcp_xml(opts: &DhcpOptions, tags: &[Tag], owner_id: &str) -> String {
    let configs: Vec<String> = opts
        .configurations
        .iter()
        .map(|c| {
            let values: Vec<String> = c
                .values
                .iter()
                .map(|v| format!("<value>{}</value>", fakecloud_aws::xml::xml_escape(v)))
                .collect();
            format!(
                "{}{}",
                ec2_elem("key", &c.key),
                ec2_list("valueSet", &values)
            )
        })
        .collect();

    format!(
        "{}{}{}{}",
        ec2_elem("dhcpOptionsId", &opts.dhcp_options_id),
        ec2_list("dhcpConfigurationSet", &configs),
        ec2_elem("ownerId", owner_id),
        super::tags::tag_set_xml(tags),
    )
}

/// Parse `DhcpConfiguration.N.Key` + `DhcpConfiguration.N.Value.M`.
fn parse_configs(params: &std::collections::HashMap<String, String>) -> Vec<DhcpConfig> {
    let mut out = Vec::new();
    let mut i = 1usize;
    loop {
        let key = format!("DhcpConfiguration.{i}.Key");
        let Some(k) = params.get(&key).filter(|v| !v.is_empty()) else {
            break;
        };
        let values = indexed_list(params, &format!("DhcpConfiguration.{i}.Value"));
        out.push(DhcpConfig {
            key: k.clone(),
            values,
        });
        i += 1;
    }
    out
}

pub(crate) fn create_dhcp_options(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let opts = DhcpOptions {
        dhcp_options_id: gen_id("dopt"),
        configurations: parse_configs(&req.query_params),
    };
    let id = opts.dhcp_options_id.clone();
    let owner = req.account_id.clone();
    let body = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "dhcp-options",
        );
        let tags = state.tags_for(&id).to_vec();
        state.dhcp_options.insert(id.clone(), opts.clone());
        format!(
            "<dhcpOptions>{}</dhcpOptions>",
            dhcp_xml(&opts, &tags, &owner)
        )
    };
    Ok(Ec2Service::respond(
        "CreateDhcpOptions",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_dhcp_options(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "DhcpOptionsId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.dhcp_options.remove(&id);
        state.tags.remove(&id);
    }
    Ok(Ec2Service::respond(
        "DeleteDhcpOptions",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_dhcp_options(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 5, 1000)?;
    let filters = parse_filters(&req.query_params);
    let wanted = indexed_list(&req.query_params, "DhcpOptionsId");
    let owner = req.account_id.clone();

    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);

    let mut items: Vec<String> = state
        .dhcp_options
        .values()
        .filter(|o| wanted.is_empty() || wanted.contains(&o.dhcp_options_id))
        .filter(|o| dhcp_matches(o, state.tags_for(&o.dhcp_options_id), &filters))
        .map(|o| dhcp_xml(o, state.tags_for(&o.dhcp_options_id), &owner))
        .collect();
    items.sort();

    let body = ec2_list("dhcpOptionsSet", &items);
    Ok(Ec2Service::respond(
        "DescribeDhcpOptions",
        &req.request_id,
        &body,
    ))
}

fn dhcp_matches(opts: &DhcpOptions, tags: &[Tag], filters: &[Filter]) -> bool {
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "dhcp-options-id" => vec![opts.dhcp_options_id.clone()],
            "key" => opts.configurations.iter().map(|c| c.key.clone()).collect(),
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

pub(crate) fn associate_dhcp_options(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let dopt = require(&req.query_params, "DhcpOptionsId")?;
    let vpc_id = require(&req.query_params, "VpcId")?;
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(vpc) = state.vpcs.get_mut(&vpc_id) {
            vpc.dhcp_options_id = dopt;
        }
    }
    Ok(Ec2Service::respond(
        "AssociateDhcpOptions",
        &req.request_id,
        &ec2_return(true),
    ))
}
