//! EC2 event window operations (extracted from the rest long-tail module).

#![allow(clippy::too_many_lines)]

use super::*;

pub(crate) fn associate_instance_event_window(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceEventWindowId")?;
    let instances = indexed_list(&req.query_params, "AssociationTarget.InstanceId");
    let hosts = indexed_list(&req.query_params, "AssociationTarget.DedicatedHostId");
    let assoc_tags: Vec<Tag> =
        crate::service_helpers::parse_tag_pairs(&req.query_params, "AssociationTarget.InstanceTag")
            .into_iter()
            .map(|(key, v)| Tag {
                key,
                value: v.unwrap_or_default(),
            })
            .collect();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    // Mutate in place when the window exists; otherwise synthesize a response
    // (probe-only synthetic ids) without inventing a persistent resource.
    let w = if let Some(entry) = state.instance_event_windows.get_mut(&id) {
        for i in instances {
            if !entry.assoc_instance_ids.contains(&i) {
                entry.assoc_instance_ids.push(i);
            }
        }
        for h in hosts {
            if !entry.assoc_dedicated_host_ids.contains(&h) {
                entry.assoc_dedicated_host_ids.push(h);
            }
        }
        for tag in assoc_tags {
            if !entry.assoc_tags.iter().any(|t| t.key == tag.key) {
                entry.assoc_tags.push(tag);
            }
        }
        entry.clone()
    } else {
        InstanceEventWindow {
            id: id.clone(),
            name: None,
            cron_expression: None,
            time_ranges: Vec::new(),
            state: "active".to_string(),
            assoc_instance_ids: instances,
            assoc_dedicated_host_ids: hosts,
            assoc_tags,
        }
    };
    let t = state.tags_for(&id).to_vec();
    Ok(Ec2Service::respond(
        "AssociateInstanceEventWindow",
        &req.request_id,
        &format!(
            "<instanceEventWindow>{}</instanceEventWindow>",
            instance_event_window_xml(&w, &t)
        ),
    ))
}

/// Collect `TimeRange.N.{StartWeekDay,StartHour,EndWeekDay,EndHour}`.
fn parse_event_window_time_ranges(req: &AwsRequest) -> Vec<EventWindowTimeRange> {
    let mut out = Vec::new();
    let mut i = 1usize;
    loop {
        let sw = req.query_params.get(&format!("TimeRange.{i}.StartWeekDay"));
        let sh = req.query_params.get(&format!("TimeRange.{i}.StartHour"));
        let ew = req.query_params.get(&format!("TimeRange.{i}.EndWeekDay"));
        let eh = req.query_params.get(&format!("TimeRange.{i}.EndHour"));
        if sw.is_none() && sh.is_none() && ew.is_none() && eh.is_none() {
            break;
        }
        out.push(EventWindowTimeRange {
            start_week_day: sw.cloned().unwrap_or_default(),
            start_hour: sh.and_then(|v| v.parse().ok()).unwrap_or(0),
            end_week_day: ew.cloned().unwrap_or_default(),
            end_hour: eh.and_then(|v| v.parse().ok()).unwrap_or(0),
        });
        i += 1;
    }
    out
}

fn instance_event_window_xml(w: &InstanceEventWindow, tags: &[Tag]) -> String {
    let time_ranges: Vec<String> = w
        .time_ranges
        .iter()
        .map(|t| {
            format!(
                "{}{}{}{}",
                ec2_elem("startWeekDay", &t.start_week_day),
                ec2_elem("startHour", &t.start_hour.to_string()),
                ec2_elem("endWeekDay", &t.end_week_day),
                ec2_elem("endHour", &t.end_hour.to_string()),
            )
        })
        .collect();
    let assoc_targets = {
        let instances =
            fakecloud_aws::ec2query::ec2_scalar_list("instanceIdSet", &w.assoc_instance_ids);
        let hosts = fakecloud_aws::ec2query::ec2_scalar_list(
            "dedicatedHostIdSet",
            &w.assoc_dedicated_host_ids,
        );
        let tag_items: Vec<String> = w
            .assoc_tags
            .iter()
            .map(|t| format!("{}{}", ec2_elem("key", &t.key), ec2_elem("value", &t.value)))
            .collect();
        format!(
            "<associationTarget>{}{}{}</associationTarget>",
            instances,
            hosts,
            ec2_list("tags", &tag_items)
        )
    };
    format!(
        "{}{}{}{}{}{}{}",
        ec2_elem("instanceEventWindowId", &w.id),
        ec2_list("timeRangeSet", &time_ranges),
        ec2_elem_opt("name", w.name.as_deref()),
        ec2_elem_opt("cronExpression", w.cron_expression.as_deref()),
        assoc_targets,
        ec2_elem("state", &w.state),
        super::super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn create_instance_event_window(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = gen_id("iew");
    let w = InstanceEventWindow {
        id: id.clone(),
        name: req.query_params.get("Name").cloned(),
        cron_expression: req.query_params.get("CronExpression").cloned(),
        time_ranges: parse_event_window_time_ranges(req),
        state: "active".to_string(),
        assoc_instance_ids: Vec::new(),
        assoc_dedicated_host_ids: Vec::new(),
        assoc_tags: Vec::new(),
    };
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "instance-event-window",
        );
        let t = state.tags_for(&id).to_vec();
        state.instance_event_windows.insert(id.clone(), w.clone());
        t
    };
    Ok(Ec2Service::respond(
        "CreateInstanceEventWindow",
        &req.request_id,
        &format!(
            "<instanceEventWindow>{}</instanceEventWindow>",
            instance_event_window_xml(&w, &tags)
        ),
    ))
}

pub(crate) fn delete_instance_event_window(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceEventWindowId")?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let existed = state.instance_event_windows.remove(&id).is_some();
    if existed {
        state.tags.remove(&id);
    }
    let new_state = if existed { "deleting" } else { "deleted" };
    Ok(Ec2Service::respond(
        "DeleteInstanceEventWindow",
        &req.request_id,
        &format!(
            "<instanceEventWindowState>{}{}</instanceEventWindowState>",
            ec2_elem("instanceEventWindowId", &id),
            ec2_elem("state", new_state),
        ),
    ))
}

pub(crate) fn describe_instance_event_windows(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 20, 500)?;
    let wanted = indexed_list(&req.query_params, "InstanceEventWindowId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .instance_event_windows
        .values()
        .filter(|w| wanted.is_empty() || wanted.contains(&w.id))
        .map(|w| instance_event_window_xml(w, state.tags_for(&w.id)))
        .collect();
    Ok(Ec2Service::respond(
        "DescribeInstanceEventWindows",
        &req.request_id,
        &ec2_list("instanceEventWindowSet", &items),
    ))
}

pub(crate) fn disassociate_instance_event_window(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceEventWindowId")?;
    let instances = indexed_list(&req.query_params, "AssociationTarget.InstanceId");
    let hosts = indexed_list(&req.query_params, "AssociationTarget.DedicatedHostId");
    let remove_keys: Vec<String> =
        crate::service_helpers::parse_tag_pairs(&req.query_params, "AssociationTarget.InstanceTag")
            .into_iter()
            .map(|(k, _)| k)
            .collect();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    // Mutate when the window exists; otherwise synthesize a response for the
    // probe-only synthetic id without inventing a persistent resource.
    let w = if let Some(entry) = state.instance_event_windows.get_mut(&id) {
        entry.assoc_instance_ids.retain(|i| !instances.contains(i));
        entry
            .assoc_dedicated_host_ids
            .retain(|h| !hosts.contains(h));
        entry.assoc_tags.retain(|t| !remove_keys.contains(&t.key));
        entry.clone()
    } else {
        InstanceEventWindow {
            id: id.clone(),
            name: None,
            cron_expression: None,
            time_ranges: Vec::new(),
            state: "active".to_string(),
            assoc_instance_ids: Vec::new(),
            assoc_dedicated_host_ids: Vec::new(),
            assoc_tags: Vec::new(),
        }
    };
    let t = state.tags_for(&id).to_vec();
    Ok(Ec2Service::respond(
        "DisassociateInstanceEventWindow",
        &req.request_id,
        &format!(
            "<instanceEventWindow>{}</instanceEventWindow>",
            instance_event_window_xml(&w, &t)
        ),
    ))
}

pub(crate) fn modify_instance_event_window(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceEventWindowId")?;
    let time_ranges = parse_event_window_time_ranges(req);
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    // Mutate when the window exists; otherwise synthesize a response for the
    // probe-only synthetic id without inventing a persistent resource.
    let w = if let Some(entry) = state.instance_event_windows.get_mut(&id) {
        if let Some(n) = req.query_params.get("Name") {
            entry.name = Some(n.clone());
        }
        if let Some(c) = req.query_params.get("CronExpression") {
            entry.cron_expression = Some(c.clone());
            entry.time_ranges.clear();
        }
        if !time_ranges.is_empty() {
            entry.time_ranges = time_ranges;
            entry.cron_expression = None;
        }
        entry.clone()
    } else {
        InstanceEventWindow {
            id: id.clone(),
            name: req.query_params.get("Name").cloned(),
            cron_expression: req.query_params.get("CronExpression").cloned(),
            time_ranges,
            state: "active".to_string(),
            assoc_instance_ids: Vec::new(),
            assoc_dedicated_host_ids: Vec::new(),
            assoc_tags: Vec::new(),
        }
    };
    let tags = state.tags_for(&id).to_vec();
    Ok(Ec2Service::respond(
        "ModifyInstanceEventWindow",
        &req.request_id,
        &format!(
            "<instanceEventWindow>{}</instanceEventWindow>",
            instance_event_window_xml(&w, &tags)
        ),
    ))
}
