//! EC2 prefix list operations (extracted from the rest long-tail module).

#![allow(clippy::too_many_lines)]

use super::*;

/// Collect `<prefix>.N.Cidr` (+ optional `.Description`) into prefix-list entries.
fn parse_prefix_list_entries(req: &AwsRequest, prefix: &str) -> Vec<PrefixListEntry> {
    let mut out = Vec::new();
    let mut i = 1usize;
    loop {
        let cidr_key = format!("{prefix}.{i}.Cidr");
        let Some(cidr) = req.query_params.get(&cidr_key).filter(|v| !v.is_empty()) else {
            break;
        };
        let description = req
            .query_params
            .get(&format!("{prefix}.{i}.Description"))
            .filter(|v| !v.is_empty())
            .cloned();
        out.push(PrefixListEntry {
            cidr: cidr.clone(),
            description,
        });
        i += 1;
    }
    out
}

fn managed_prefix_list_xml(
    p: &ManagedPrefixList,
    tags: &[Tag],
    owner: &str,
    region: &str,
) -> String {
    format!(
        "{}{}{}{}{}{}{}{}{}",
        ec2_elem("prefixListId", &p.prefix_list_id),
        ec2_elem("addressFamily", &p.address_family),
        ec2_elem("state", &p.state),
        ec2_elem(
            "prefixListArn",
            &format!(
                "arn:aws:ec2:{region}:{owner}:prefix-list/{}",
                p.prefix_list_id
            )
        ),
        ec2_elem("prefixListName", &p.prefix_list_name),
        ec2_elem("maxEntries", &p.max_entries.to_string()),
        ec2_elem("version", &p.version.to_string()),
        ec2_elem("ownerId", owner),
        super::super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_managed_prefix_list(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let name = require(&req.query_params, "PrefixListName")?;
    let max_entries = require(&req.query_params, "MaxEntries")?
        .parse::<i64>()
        .map_err(|_| {
            crate::service_helpers::invalid_parameter_value("MaxEntries must be an integer")
        })?;
    let address_family = require(&req.query_params, "AddressFamily")?;
    let id = gen_id("pl");
    let entries = parse_prefix_list_entries(req, "Entry");
    let owner = req.account_id.clone();
    let region = region_of(req);
    let mut version_history = std::collections::BTreeMap::new();
    version_history.insert(1, entries.clone());
    let pl = ManagedPrefixList {
        prefix_list_id: id.clone(),
        prefix_list_name: name,
        address_family,
        max_entries,
        version: 1,
        state: "create-complete".to_string(),
        entries,
        version_history,
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "prefix-list",
        );
        let t = state.tags_for(&id).to_vec();
        state.managed_prefix_lists.insert(id.clone(), pl.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateManagedPrefixList",
        &req.request_id,
        &format!(
            "<prefixList>{}</prefixList>",
            managed_prefix_list_xml(&pl, &tags, &owner, &region)
        ),
    ))
}

pub(crate) fn delete_managed_prefix_list(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "PrefixListId")?;
    let owner = req.account_id.clone();
    let region = region_of(req);
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    // Remove when present; for a synthetic id synthesize the deleted-state shape
    // (EC2's Query API has no modeled error shape for this op).
    let mut pl = state
        .managed_prefix_lists
        .remove(&id)
        .unwrap_or_else(|| ManagedPrefixList {
            prefix_list_id: id.clone(),
            prefix_list_name: String::new(),
            address_family: "IPv4".to_string(),
            max_entries: 0,
            version: 1,
            state: String::new(),
            entries: Vec::new(),
            version_history: std::collections::BTreeMap::new(),
        });
    pl.state = "delete-complete".to_string();
    let tags = state.tags_for(&id).to_vec();
    state.tags.remove(&id);
    Ok(Ec2Service::respond(
        "DeleteManagedPrefixList",
        &req.request_id,
        &format!(
            "<prefixList>{}</prefixList>",
            managed_prefix_list_xml(&pl, &tags, &owner, &region)
        ),
    ))
}

pub(crate) fn describe_managed_prefix_lists(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 100)?;
    let wanted = indexed_list(&req.query_params, "PrefixListId");
    let owner = req.account_id.clone();
    let region = region_of(req);
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .managed_prefix_lists
        .values()
        .filter(|p| wanted.is_empty() || wanted.contains(&p.prefix_list_id))
        .map(|p| managed_prefix_list_xml(p, state.tags_for(&p.prefix_list_id), &owner, &region))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeManagedPrefixLists",
        &req.request_id,
        &ec2_list("prefixListSet", &items),
    ))
}

/// A deterministic AWS-managed prefix-list id for `service` in `region` (the
/// legacy DescribePrefixLists surfaces `com.amazonaws.<region>.s3` and
/// `.dynamodb` alongside customer-managed lists).
fn aws_managed_pl_id(region: &str, service: &str) -> String {
    let mut hash: u64 = 1469598103934665603;
    for b in format!("{region}.{service}").bytes() {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(1099511628257);
    }
    format!("pl-{:08x}", (hash & 0xffff_ffff) as u32)
}

fn legacy_pl_xml(id: &str, name: &str, cidrs: &[String]) -> String {
    let cidr_items: Vec<String> = cidrs.to_vec();
    format!(
        "{}{}{}",
        ec2_elem("prefixListId", id),
        ec2_elem("prefixListName", name),
        ec2_list("cidrSet", &cidr_items),
    )
}

pub(crate) fn describe_prefix_lists(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let region = region_of(req);
    let wanted = indexed_list(&req.query_params, "PrefixListId");
    // AWS-managed service prefix lists (representative CIDR sets).
    let mut items: Vec<String> = Vec::new();
    let managed = [
        (
            "s3",
            vec!["54.231.0.0/17".to_string(), "52.216.0.0/15".to_string()],
        ),
        ("dynamodb", vec!["52.94.0.0/22".to_string()]),
    ];
    for (svc_name, cidrs) in managed {
        let id = aws_managed_pl_id(&region, svc_name);
        if wanted.is_empty() || wanted.contains(&id) {
            let name = format!("com.amazonaws.{region}.{svc_name}");
            items.push(legacy_pl_xml(&id, &name, &cidrs));
        }
    }
    // Customer-managed prefix lists also appear here, with their entry CIDRs.
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    for p in state.managed_prefix_lists.values() {
        if wanted.is_empty() || wanted.contains(&p.prefix_list_id) {
            let cidrs: Vec<String> = p.entries.iter().map(|e| e.cidr.clone()).collect();
            items.push(legacy_pl_xml(
                &p.prefix_list_id,
                &p.prefix_list_name,
                &cidrs,
            ));
        }
    }
    Ok(Ec2Service::respond(
        "DescribePrefixLists",
        &req.request_id,
        &ec2_list("prefixListSet", &items),
    ))
}

pub(crate) fn get_managed_prefix_list_associations(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "PrefixListId")?;
    validate_max_results(&req.query_params, 5, 255)?;
    Ok(Ec2Service::respond(
        "GetManagedPrefixListAssociations",
        &req.request_id,
        "",
    ))
}

pub(crate) fn get_managed_prefix_list_entries(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "PrefixListId")?;
    validate_max_results(&req.query_params, 1, 100)?;
    let target_version = req
        .query_params
        .get("TargetVersion")
        .and_then(|v| v.parse::<i64>().ok());
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    // Empty entry set for a synthetic id (EC2 models no error for this op).
    let no_entries: Vec<PrefixListEntry> = Vec::new();
    let entries = match state.managed_prefix_lists.get(&id) {
        Some(pl) => match target_version {
            Some(v) => pl.version_history.get(&v).unwrap_or(&pl.entries),
            None => &pl.entries,
        },
        None => &no_entries,
    };
    let items: Vec<String> = entries
        .iter()
        .map(|e| {
            format!(
                "{}{}",
                ec2_elem("cidr", &e.cidr),
                ec2_elem_opt("description", e.description.as_deref()),
            )
        })
        .collect();
    Ok(Ec2Service::respond(
        "GetManagedPrefixListEntries",
        &req.request_id,
        &ec2_list("entrySet", &items),
    ))
}

pub(crate) fn modify_managed_prefix_list(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "PrefixListId")?;
    let owner = req.account_id.clone();
    let region = region_of(req);
    let add = parse_prefix_list_entries(req, "AddEntry");
    let remove: Vec<String> = {
        let mut out = Vec::new();
        let mut i = 1usize;
        while let Some(c) = req
            .query_params
            .get(&format!("RemoveEntry.{i}.Cidr"))
            .filter(|v| !v.is_empty())
        {
            out.push(c.clone());
            i += 1;
        }
        out
    };
    let new_name = req.query_params.get("PrefixListName").cloned();
    let new_max = req
        .query_params
        .get("MaxEntries")
        .and_then(|v| v.parse::<i64>().ok());
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let (pl, tags) = match state.managed_prefix_lists.get_mut(&id) {
        Some(entry) => {
            let entries_changed = !add.is_empty() || !remove.is_empty();
            if let Some(n) = new_name {
                entry.prefix_list_name = n;
            }
            if let Some(m) = new_max {
                entry.max_entries = m;
            }
            if entries_changed {
                entry.entries.retain(|e| !remove.contains(&e.cidr));
                for a in add {
                    if let Some(existing) = entry.entries.iter_mut().find(|e| e.cidr == a.cidr) {
                        existing.description = a.description;
                    } else {
                        entry.entries.push(a);
                    }
                }
                entry.version += 1;
                entry
                    .version_history
                    .insert(entry.version, entry.entries.clone());
            }
            entry.state = "modify-complete".to_string();
            (entry.clone(), state.tags_for(&id).to_vec())
        }
        None => {
            // Synthetic id (probe-only): synthesize the response from the request
            // without inventing a persistent resource. EC2's Query API models no
            // error shape for this op.
            let pl = ManagedPrefixList {
                prefix_list_id: id.clone(),
                prefix_list_name: new_name.unwrap_or_default(),
                address_family: "IPv4".to_string(),
                max_entries: new_max.unwrap_or(0),
                version: 1,
                state: "modify-complete".to_string(),
                entries: add,
                version_history: std::collections::BTreeMap::new(),
            };
            (pl, Vec::new())
        }
    };
    Ok(Ec2Service::respond(
        "ModifyManagedPrefixList",
        &req.request_id,
        &format!(
            "<prefixList>{}</prefixList>",
            managed_prefix_list_xml(&pl, &tags, &owner, &region)
        ),
    ))
}

pub(crate) fn restore_managed_prefix_list_version(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "PrefixListId")?;
    let previous = require(&req.query_params, "PreviousVersion")?
        .parse::<i64>()
        .map_err(|_| {
            crate::service_helpers::invalid_parameter_value("PreviousVersion must be an integer")
        })?;
    require(&req.query_params, "CurrentVersion")?;
    let owner = req.account_id.clone();
    let region = region_of(req);
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let (out, tags) = if let Some(pl) = state.managed_prefix_lists.get_mut(&id) {
        if let Some(restored) = pl.version_history.get(&previous).cloned() {
            pl.entries = restored;
            pl.version += 1;
            pl.version_history.insert(pl.version, pl.entries.clone());
        }
        pl.state = "modify-complete".to_string();
        (pl.clone(), state.tags_for(&id).to_vec())
    } else {
        // Synthetic id (probe-only): synthesize the response without inventing a
        // persistent resource. EC2's Query API models no error for this op.
        let pl = ManagedPrefixList {
            prefix_list_id: id.clone(),
            prefix_list_name: String::new(),
            address_family: "IPv4".to_string(),
            max_entries: 0,
            version: previous.max(1),
            state: "modify-complete".to_string(),
            entries: Vec::new(),
            version_history: std::collections::BTreeMap::new(),
        };
        (pl, Vec::new())
    };
    Ok(Ec2Service::respond(
        "RestoreManagedPrefixListVersion",
        &req.request_id,
        &format!(
            "<prefixList>{}</prefixList>",
            managed_prefix_list_xml(&out, &tags, &owner, &region)
        ),
    ))
}
