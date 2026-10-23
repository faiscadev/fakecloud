//! EC2 prefix list operations (extracted from the rest long-tail module).

#![allow(clippy::too_many_lines)]

use super::*;
use crate::service::aws_prefix_lists::{self, GATEWAY_ENDPOINT_SERVICES};
use crate::service::quota::RuleWeights;
use crate::service_helpers::ec2_arn;

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
        "{}{}{}{}{}{}{}{}{}{}",
        ec2_elem("prefixListId", &p.prefix_list_id),
        ec2_elem("addressFamily", &p.address_family),
        ec2_elem("state", &p.state),
        p.state_message
            .as_deref()
            .map(|m| ec2_elem("stateMessage", m))
            .unwrap_or_default(),
        ec2_elem(
            "prefixListArn",
            &ec2_arn(region, owner, &format!("prefix-list/{}", p.prefix_list_id))
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
        state_message: None,
        entries,
        version_history,
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut(&req.account_id, &req.region);
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
    let state = accounts.regional_mut(&req.account_id, &req.region);
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
            state_message: None,
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
    let filters = parse_filters(&req.query_params);
    // `prefix-list-id`, `prefix-list-name`, `owner-id`, `tag:<key>`,
    // `tag-key` and `tag-value` select lists. An unknown filter name matches
    // nothing, as in the other EC2 describes.
    let selected = |id: &str, name: &str, owner_id: &str, tags: &[Tag]| {
        (wanted.is_empty() || wanted.iter().any(|w| w == id))
            && filters.iter().all(|f| {
                let candidates: Vec<&str> = match f.name.as_str() {
                    "prefix-list-id" => vec![id],
                    "prefix-list-name" => vec![name],
                    "owner-id" => vec![owner_id],
                    "tag-key" => tags.iter().map(|t| t.key.as_str()).collect(),
                    "tag-value" => tags.iter().map(|t| t.value.as_str()).collect(),
                    other => match other.strip_prefix("tag:") {
                        Some(key) => tags
                            .iter()
                            .filter(|t| t.key == key)
                            .map(|t| t.value.as_str())
                            .collect(),
                        None => Vec::new(),
                    },
                };
                f.values.iter().any(|v| {
                    candidates
                        .iter()
                        .any(|c| crate::service_helpers::filter_value_matches(v, c))
                })
            })
    };
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    // The AWS-managed lists every account can reference (owner `AWS`). They
    // report no MaxEntries or version: a reference weighs the list's
    // published weight instead.
    let mut items: Vec<String> = aws_prefix_lists::AWS_MANAGED_PREFIX_LISTS
        .iter()
        .map(|l| (l.id_in(&region), l.name_in(&region), l))
        .filter(|(id, name, _)| selected(id, name, "AWS", &[]))
        .map(|(id, name, l)| {
            format!(
                "{}{}{}{}{}{}{}",
                ec2_elem("prefixListId", &id),
                ec2_elem("addressFamily", l.address_family),
                ec2_elem("state", "create-complete"),
                ec2_elem(
                    "prefixListArn",
                    &ec2_arn(&region, "aws", &format!("prefix-list/{id}"))
                ),
                ec2_elem("prefixListName", &name),
                ec2_elem("ownerId", "AWS"),
                super::super::tags::tag_set_xml(&[]),
            )
        })
        .collect();
    items.extend(
        state
            .managed_prefix_lists
            .values()
            .filter(|p| {
                selected(
                    &p.prefix_list_id,
                    &p.prefix_list_name,
                    &owner,
                    state.tags_for(&p.prefix_list_id),
                )
            })
            .map(|p| {
                managed_prefix_list_xml(p, state.tags_for(&p.prefix_list_id), &owner, &region)
            }),
    );
    Ok(Ec2Service::respond(
        "DescribeManagedPrefixLists",
        &req.request_id,
        &ec2_list("prefixListSet", &items),
    ))
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
    let filters = parse_filters(&req.query_params);
    let selected = |id: &str, name: &str| {
        (wanted.is_empty() || wanted.iter().any(|w| w == id))
            && filters.iter().all(|f| {
                let candidates: Vec<&str> = match f.name.as_str() {
                    "prefix-list-id" => vec![id],
                    "prefix-list-name" => vec![name],
                    _ => Vec::new(),
                };
                f.values.iter().any(|v| {
                    candidates
                        .iter()
                        .any(|c| crate::service_helpers::filter_value_matches(v, c))
                })
            })
    };
    // The gateway-endpoint AWS-managed lists, with their published ranges.
    let mut items: Vec<String> = Vec::new();
    for list in GATEWAY_ENDPOINT_SERVICES
        .iter()
        .filter_map(|s| aws_prefix_lists::gateway_endpoint_list(s))
    {
        let id = list.id_in(&region);
        if selected(&id, &list.name_in(&region)) {
            items.push(legacy_pl_xml(
                &id,
                &list.name_in(&region),
                &list.cidrs_in(&region),
            ));
        }
    }
    // Customer-managed prefix lists also appear here, with their entry CIDRs.
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    for p in state.managed_prefix_lists.values() {
        if selected(&p.prefix_list_id, &p.prefix_list_name) {
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
    let state = accounts
        .regional(&req.account_id, &req.region)
        .unwrap_or(&empty);
    // An AWS-managed list reports its published address ranges (from AWS's
    // ip-ranges.json); any other unknown id an empty entry set (EC2 models no
    // error for this op).
    let region = region_of(req);
    let aws_entries: Vec<PrefixListEntry> = aws_prefix_lists::by_id(&region, &id)
        .map(|l| {
            l.cidrs_in(&region)
                .into_iter()
                .map(|cidr| PrefixListEntry {
                    cidr,
                    description: None,
                })
                .collect()
        })
        .unwrap_or_default();
    let entries = match state.managed_prefix_lists.get(&id) {
        Some(pl) => match target_version {
            Some(v) => pl.version_history.get(&v).unwrap_or(&pl.entries),
            None => &pl.entries,
        },
        None => &aws_entries,
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
    // Resolved before the EC2 lock is taken: Service Quotas answers it.
    let rule_limit = svc.enforced_rules_per_security_group(&req.account_id, &req.region);
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut(&req.account_id, &req.region);
    // A larger MaxEntries weighs more in every security group that references
    // the list. AWS fails the resize (the list goes to `modify-failed`, naming
    // up to ten resources in its state message) when a referencing group
    // could not take the new size.
    let blocked = match (new_max, state.managed_prefix_lists.get(&id), rule_limit) {
        (Some(m), Some(current), Some(limit)) if m > current.max_entries => {
            groups_over_quota_at(state, &region, &id, m, limit)
        }
        _ => Vec::new(),
    };
    let (pl, tags) = match state.managed_prefix_lists.get_mut(&id) {
        Some(entry) => {
            let entries_changed = !add.is_empty() || !remove.is_empty();
            if let Some(n) = new_name {
                entry.prefix_list_name = n;
            }
            if !blocked.is_empty() {
                entry.state = "modify-failed".to_string();
                entry.state_message = Some(format!(
                    "The following resources do not support the new maximum size: {}",
                    blocked.join(", ")
                ));
                let failed = entry.clone();
                let tags = state.tags_for(&id).to_vec();
                return Ok(Ec2Service::respond(
                    "ModifyManagedPrefixList",
                    &req.request_id,
                    &format!(
                        "<prefixList>{}</prefixList>",
                        managed_prefix_list_xml(&failed, &tags, &owner, &region)
                    ),
                ));
            }
            entry.state_message = None;
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
                state_message: None,
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

/// Security groups (at most ten, as AWS reports them) whose rules would exceed
/// `limit` if prefix list `id` had `max_entries` entries.
fn groups_over_quota_at(
    state: &Ec2State,
    region: &str,
    id: &str,
    max_entries: i64,
    limit: usize,
) -> Vec<String> {
    let mut lists = state.managed_prefix_lists.clone();
    if let Some(pl) = lists.get_mut(id) {
        pl.max_entries = max_entries;
    }
    let before = RuleWeights::new(&state.managed_prefix_lists, region);
    let after = RuleWeights::new(&lists, region);
    state
        .security_groups
        .values()
        .filter(|g| {
            g.rules
                .iter()
                .any(|r| r.prefix_list_id.as_deref() == Some(id))
        })
        // A group already over the limit at the current size is not one the
        // resize breaks; only a group the larger size pushes over is.
        .filter(|g| {
            let n = after.group_rule_count(&g.rules);
            n > limit && n > before.group_rule_count(&g.rules)
        })
        .map(|g| g.group_id.clone())
        .take(10)
        .collect()
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
    let state = accounts.regional_mut(&req.account_id, &req.region);
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
            state_message: None,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{SecurityGroup, SecurityGroupRule};
    use crate::test_support::ec2_request;

    fn body(r: AwsResponse) -> String {
        String::from_utf8(r.body.expect_bytes().to_vec()).unwrap()
    }

    fn cidr_rule(i: usize) -> SecurityGroupRule {
        SecurityGroupRule {
            rule_id: format!("sgr-{i}"),
            group_id: "sg-a".into(),
            is_egress: false,
            ip_protocol: "tcp".into(),
            from_port: i as i64,
            to_port: i as i64,
            cidr_ipv4: Some("10.0.0.0/8".into()),
            cidr_ipv6: None,
            prefix_list_id: None,
            referenced_group_id: None,
            referenced_group_name: None,
            referenced_user_id: None,
            description: String::new(),
        }
    }

    fn describe(params: &[(&str, &str)]) -> String {
        body(
            describe_managed_prefix_lists(
                &Ec2Service::new(),
                &ec2_request("DescribeManagedPrefixLists", params),
            )
            .unwrap(),
        )
    }

    #[test]
    fn aws_managed_lists_are_described_and_filtered() {
        let all = describe(&[]);
        assert!(all.contains("com.amazonaws.global.cloudfront.origin-facing"));
        assert!(all.contains("<ownerId>AWS</ownerId>"));

        let cf = describe(&[
            ("Filter.1.Name", "prefix-list-name"),
            (
                "Filter.1.Value.1",
                "com.amazonaws.global.cloudfront.origin-facing",
            ),
        ]);
        assert_eq!(cf.matches("<prefixListId>").count(), 1, "{cf}");

        let wildcard = describe(&[
            ("Filter.1.Name", "prefix-list-name"),
            ("Filter.1.Value.1", "*cloudfront*"),
        ]);
        assert_eq!(wildcard.matches("<prefixListId>").count(), 2, "{wildcard}");

        // An unknown filter name matches nothing, as in the other describes.
        let unknown = describe(&[
            ("Filter.1.Name", "no-such-filter"),
            ("Filter.1.Value.1", "x"),
        ]);
        assert_eq!(unknown.matches("<prefixListId>").count(), 0, "{unknown}");
    }

    #[test]
    fn aws_managed_list_entries_are_its_published_ranges() {
        let svc = Ec2Service::new();
        let cf = aws_prefix_lists::AWS_MANAGED_PREFIX_LISTS
            .iter()
            .find(|l| l.name_in("us-east-1") == "com.amazonaws.global.cloudfront.origin-facing")
            .unwrap();
        let id = cf.id_in("us-east-1");
        let entries = body(
            get_managed_prefix_list_entries(
                &svc,
                &ec2_request(
                    "GetManagedPrefixListEntries",
                    &[("PrefixListId", id.as_str())],
                ),
            )
            .unwrap(),
        );
        let n = entries.matches("<cidr>").count();
        assert!(n > 0 && n <= cf.weight, "{entries}");
        assert_eq!(n, cf.cidrs_in("us-east-1").len());
    }

    #[test]
    fn describe_prefix_lists_filters_by_name() {
        let svc = Ec2Service::new();
        let all = body(
            super::describe_prefix_lists(&svc, &ec2_request("DescribePrefixLists", &[])).unwrap(),
        );
        assert!(
            all.matches("<prefixListId>").count() > 1,
            "unfiltered should return multiple lists: {all}"
        );

        let filtered = body(
            super::describe_prefix_lists(
                &svc,
                &ec2_request(
                    "DescribePrefixLists",
                    &[
                        ("Filter.1.Name", "prefix-list-name"),
                        ("Filter.1.Value.1", "com.amazonaws.us-east-1.s3"),
                    ],
                ),
            )
            .unwrap(),
        );
        assert_eq!(
            filtered.matches("<prefixListId>").count(),
            1,
            "filter by name should return exactly one list: {filtered}"
        );
        assert!(
            filtered.contains("com.amazonaws.us-east-1.s3"),
            "filtered result should contain the S3 prefix list: {filtered}"
        );
    }

    #[test]
    fn resizing_a_referenced_list_past_the_rules_quota_fails() {
        let svc = crate::test_support::svc_enforcing_sg_quotas();
        let created = body(
            create_managed_prefix_list(
                &svc,
                &ec2_request(
                    "CreateManagedPrefixList",
                    &[
                        ("PrefixListName", "corp"),
                        ("MaxEntries", "1"),
                        ("AddressFamily", "IPv4"),
                    ],
                ),
            )
            .unwrap(),
        );
        let pl_id = created
            .split("<prefixListId>")
            .nth(1)
            .and_then(|r| r.split('<').next())
            .unwrap()
            .to_string();
        // 59 CIDR rules + one rule on the 1-entry list = 60, the default quota.
        let mut rules: Vec<SecurityGroupRule> = (0..59).map(cidr_rule).collect();
        let mut pl_rule = cidr_rule(59);
        pl_rule.cidr_ipv4 = None;
        pl_rule.prefix_list_id = Some(pl_id.clone());
        rules.push(pl_rule);
        svc.state
            .write()
            .regional_mut("000000000000", "us-east-1")
            .security_groups
            .insert(
                "sg-a".into(),
                SecurityGroup {
                    group_id: "sg-a".into(),
                    group_name: "a".into(),
                    description: "d".into(),
                    vpc_id: "vpc-1".into(),
                    rules,
                },
            );

        let resize = |max: &str| {
            body(
                modify_managed_prefix_list(
                    &svc,
                    &ec2_request(
                        "ModifyManagedPrefixList",
                        &[("PrefixListId", pl_id.as_str()), ("MaxEntries", max)],
                    ),
                )
                .unwrap(),
            )
        };
        let failed = resize("1000");
        assert!(failed.contains("<state>modify-failed</state>"), "{failed}");
        assert!(failed.contains("<stateMessage>"), "{failed}");
        assert!(failed.contains("sg-a"), "{failed}");
        assert!(failed.contains("<maxEntries>1</maxEntries>"), "{failed}");
        assert_eq!(
            svc.state
                .read()
                .regional("000000000000", "us-east-1")
                .unwrap()
                .managed_prefix_lists[&pl_id]
                .max_entries,
            1
        );

        // Once the group has room, the same resize goes through.
        svc.state
            .write()
            .regional_mut("000000000000", "us-east-1")
            .security_groups
            .get_mut("sg-a")
            .unwrap()
            .rules
            .retain(|r| r.prefix_list_id.is_some() || r.from_port < 9);
        let ok = resize("50");
        assert!(ok.contains("<state>modify-complete</state>"), "{ok}");
        assert!(!ok.contains("<stateMessage>"), "{ok}");
        assert!(ok.contains("<maxEntries>50</maxEntries>"), "{ok}");
    }
}
