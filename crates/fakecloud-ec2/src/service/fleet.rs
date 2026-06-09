//! Launch templates (+ versions), Spot instance/fleet requests, EC2 fleets,
//! and the spot datafeed subscription.

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    gen_id, indexed_list, require, require_struct, validate_enum, validate_int_range,
    validate_length, validate_max_results,
};
use crate::state::{Ec2State, Fleet, LaunchTemplate, SpotFleet, SpotRequest, Tag};

const FIXED_TIME: &str = "2024-01-01T00:00:00.000Z";

/// Shared `LaunchTemplateName` (3..128) + `VersionDescription` (0..255) checks.
fn validate_lt_strings(req: &AwsRequest) -> Result<(), AwsServiceError> {
    validate_length(&req.query_params, "LaunchTemplateName", 3, 128)?;
    validate_length(&req.query_params, "VersionDescription", 0, 255)?;
    Ok(())
}

// ---- launch templates ----

fn lt_xml(t: &LaunchTemplate, tags: &[Tag], owner: &str) -> String {
    format!(
        "{}{}{}{}<defaultVersionNumber>{}</defaultVersionNumber><latestVersionNumber>{}</latestVersionNumber>{}",
        ec2_elem("launchTemplateId", &t.id),
        ec2_elem("launchTemplateName", &t.name),
        ec2_elem("createTime", FIXED_TIME),
        ec2_elem("createdBy", &format!("arn:aws:iam::{owner}:root")),
        t.default_version,
        t.latest_version,
        super::tags::tag_set_xml(tags),
    )
}

fn lt_version_xml(t: &LaunchTemplate, version: i64, owner: &str) -> String {
    format!(
        "{}{}<versionNumber>{}</versionNumber>{}{}<defaultVersion>{}</defaultVersion><launchTemplateData/>",
        ec2_elem("launchTemplateId", &t.id),
        ec2_elem("launchTemplateName", &t.name),
        version,
        ec2_elem("createTime", FIXED_TIME),
        ec2_elem("createdBy", &format!("arn:aws:iam::{owner}:root")),
        version == t.default_version,
    )
}

pub(crate) fn create_launch_template(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    // CreateLaunchTemplate's LaunchTemplateName is unconstrained (unlike the
    // other launch-template ops); only VersionDescription is length-bounded.
    validate_length(&req.query_params, "VersionDescription", 0, 255)?;
    let name = require(&req.query_params, "LaunchTemplateName")?;
    let id = gen_id("lt");
    let t = LaunchTemplate {
        id: id.clone(),
        name,
        default_version: 1,
        latest_version: 1,
    };
    let owner = req.account_id.clone();
    let tags = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        crate::service::tags::apply_tag_specifications(
            state,
            &req.query_params,
            &id,
            "launch-template",
        );
        let tg = state.tags_for(&id).to_vec();
        state.launch_templates.insert(id.clone(), t.clone());
        tg
    };
    Ok(Ec2Service::respond(
        "CreateLaunchTemplate",
        &req.request_id,
        &format!(
            "<launchTemplate>{}</launchTemplate>",
            lt_xml(&t, &tags, &owner)
        ),
    ))
}

pub(crate) fn create_launch_template_version(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_lt_strings(req)?;
    require_struct(&req.query_params, "LaunchTemplateData").ok();
    let owner = req.account_id.clone();
    let id = req.query_params.get("LaunchTemplateId").cloned();
    let name = req.query_params.get("LaunchTemplateName").cloned();
    let (t, version) = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let key = id
            .clone()
            .or_else(|| {
                name.as_ref().and_then(|n| {
                    state
                        .launch_templates
                        .values()
                        .find(|t| &t.name == n)
                        .map(|t| t.id.clone())
                })
            })
            .unwrap_or_default();
        if let Some(t) = state.launch_templates.get_mut(&key) {
            t.latest_version += 1;
            (t.clone(), t.latest_version)
        } else {
            let synthetic = LaunchTemplate {
                id: id.unwrap_or_else(|| gen_id("lt")),
                name: name.unwrap_or_default(),
                default_version: 1,
                latest_version: 2,
            };
            (synthetic, 2)
        }
    };
    Ok(Ec2Service::respond(
        "CreateLaunchTemplateVersion",
        &req.request_id,
        &format!(
            "<launchTemplateVersion>{}</launchTemplateVersion>",
            lt_version_xml(&t, version, &owner)
        ),
    ))
}

fn resolve_lt(state: &Ec2State, req: &AwsRequest) -> Option<LaunchTemplate> {
    if let Some(id) = req.query_params.get("LaunchTemplateId") {
        return state.launch_templates.get(id).cloned();
    }
    if let Some(name) = req.query_params.get("LaunchTemplateName") {
        return state
            .launch_templates
            .values()
            .find(|t| &t.name == name)
            .cloned();
    }
    None
}

pub(crate) fn delete_launch_template(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_lt_strings(req)?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let t = resolve_lt(state, req);
    let tags = t
        .as_ref()
        .map(|t| state.tags_for(&t.id).to_vec())
        .unwrap_or_default();
    let body = if let Some(t) = t {
        state.launch_templates.remove(&t.id);
        state.tags.remove(&t.id);
        format!(
            "<launchTemplate>{}</launchTemplate>",
            lt_xml(&t, &tags, &owner)
        )
    } else {
        String::new()
    };
    Ok(Ec2Service::respond(
        "DeleteLaunchTemplate",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn delete_launch_template_versions(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_lt_strings(req)?;
    let id = req
        .query_params
        .get("LaunchTemplateId")
        .cloned()
        .unwrap_or_default();
    let versions = indexed_list(&req.query_params, "LaunchTemplateVersion");
    let items: Vec<String> = versions
        .iter()
        .map(|v| {
            format!(
                "{}<versionNumber>{}</versionNumber>",
                ec2_elem("launchTemplateId", &id),
                v
            )
        })
        .collect();
    let body = format!(
        "{}{}",
        ec2_list("successfullyDeletedLaunchTemplateVersionSet", &items),
        ec2_list("unsuccessfullyDeletedLaunchTemplateVersionSet", &[])
    );
    Ok(Ec2Service::respond(
        "DeleteLaunchTemplateVersions",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn describe_launch_templates(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_max_results(&req.query_params, 1, 200)?;
    let wanted = indexed_list(&req.query_params, "LaunchTemplateId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .launch_templates
        .values()
        .filter(|t| wanted.is_empty() || wanted.contains(&t.id))
        .map(|t| lt_xml(t, state.tags_for(&t.id), &owner))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeLaunchTemplates",
        &req.request_id,
        &ec2_list("launchTemplates", &items),
    ))
}

pub(crate) fn describe_launch_template_versions(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_lt_strings(req)?;
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = resolve_lt(state, req)
        .map(|t| {
            (1..=t.latest_version)
                .map(|v| lt_version_xml(&t, v, &owner))
                .collect()
        })
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "DescribeLaunchTemplateVersions",
        &req.request_id,
        &ec2_list("launchTemplateVersionSet", &items),
    ))
}

pub(crate) fn get_launch_template_data(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    Ok(Ec2Service::respond(
        "GetLaunchTemplateData",
        &req.request_id,
        "<launchTemplateData><instanceType>t3.micro</instanceType></launchTemplateData>",
    ))
}

pub(crate) fn modify_launch_template(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_lt_strings(req)?;
    let owner = req.account_id.clone();
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    if let (Some(t), Some(v)) = (
        resolve_lt(state, req),
        req.query_params
            .get("DefaultVersion")
            .and_then(|v| v.parse::<i64>().ok()),
    ) {
        if let Some(t) = state.launch_templates.get_mut(&t.id) {
            t.default_version = v;
        }
    }
    let t = resolve_lt(state, req);
    let tags = t
        .as_ref()
        .map(|t| state.tags_for(&t.id).to_vec())
        .unwrap_or_default();
    let body = t
        .map(|t| {
            format!(
                "<launchTemplate>{}</launchTemplate>",
                lt_xml(&t, &tags, &owner)
            )
        })
        .unwrap_or_default();
    Ok(Ec2Service::respond(
        "ModifyLaunchTemplate",
        &req.request_id,
        &body,
    ))
}

// ---- spot instance requests ----

fn spot_request_xml(r: &SpotRequest, tags: &[Tag]) -> String {
    format!(
        "{}{}{}{}<status><code>{}</code><message>request fulfilled</message></status>{}{}{}",
        ec2_elem("spotInstanceRequestId", &r.id),
        ec2_elem("state", &r.state),
        ec2_elem("type", &r.request_type),
        ec2_elem("spotPrice", &r.spot_price),
        r.state,
        ec2_elem("productDescription", "Linux/UNIX"),
        ec2_elem("createTime", FIXED_TIME),
        super::tags::tag_set_xml(tags),
    )
}

pub(crate) fn request_spot_instances(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(
        &req.query_params,
        "InstanceInterruptionBehavior",
        &["hibernate", "stop", "terminate"],
    )?;
    validate_enum(&req.query_params, "Type", &["one-time", "persistent"])?;
    let count: usize = req
        .query_params
        .get("InstanceCount")
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let price = req
        .query_params
        .get("SpotPrice")
        .cloned()
        .unwrap_or_else(|| "0.05".to_string());
    let rtype = req
        .query_params
        .get("Type")
        .cloned()
        .unwrap_or_else(|| "one-time".to_string());
    let mut rendered = Vec::new();
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for _ in 0..count.max(1) {
            let id = gen_id("sir");
            let r = SpotRequest {
                id: id.clone(),
                state: "active".to_string(),
                request_type: rtype.clone(),
                spot_price: price.clone(),
            };
            rendered.push(spot_request_xml(&r, &[]));
            state.spot_requests.insert(id, r);
        }
    }
    Ok(Ec2Service::respond(
        "RequestSpotInstances",
        &req.request_id,
        &ec2_list("spotInstanceRequestSet", &rendered),
    ))
}

pub(crate) fn describe_spot_instance_requests(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let wanted = indexed_list(&req.query_params, "SpotInstanceRequestId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .spot_requests
        .values()
        .filter(|r| wanted.is_empty() || wanted.contains(&r.id))
        .map(|r| spot_request_xml(r, state.tags_for(&r.id)))
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeSpotInstanceRequests",
        &req.request_id,
        &ec2_list("spotInstanceRequestSet", &items),
    ))
}

pub(crate) fn cancel_spot_instance_requests(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ids = indexed_list(&req.query_params, "SpotInstanceRequestId");
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for id in &ids {
            if let Some(r) = state.spot_requests.get_mut(id) {
                r.state = "cancelled".to_string();
            }
        }
    }
    let items: Vec<String> = ids
        .iter()
        .map(|id| {
            format!(
                "{}{}",
                ec2_elem("spotInstanceRequestId", id),
                ec2_elem("state", "cancelled")
            )
        })
        .collect();
    Ok(Ec2Service::respond(
        "CancelSpotInstanceRequests",
        &req.request_id,
        &ec2_list("spotInstanceRequestSet", &items),
    ))
}

// ---- spot fleet ----

pub(crate) fn request_spot_fleet(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require_struct(&req.query_params, "SpotFleetRequestConfig")?;
    let id = gen_id("sfr");
    {
        let mut accounts = svc.state.write();
        accounts.get_or_create(&req.account_id).spot_fleets.insert(
            id.clone(),
            SpotFleet {
                id: id.clone(),
                state: "active".to_string(),
            },
        );
    }
    Ok(Ec2Service::respond(
        "RequestSpotFleet",
        &req.request_id,
        &ec2_elem("spotFleetRequestId", &id),
    ))
}

pub(crate) fn describe_spot_fleet_requests(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let wanted = indexed_list(&req.query_params, "SpotFleetRequestId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .spot_fleets
        .values()
        .filter(|f| wanted.is_empty() || wanted.contains(&f.id))
        .map(|f| {
            format!(
                "{}{}<spotFleetRequestConfig><targetCapacity>1</targetCapacity></spotFleetRequestConfig>{}",
                ec2_elem("spotFleetRequestId", &f.id),
                ec2_elem("spotFleetRequestState", &f.state),
                ec2_elem("createTime", FIXED_TIME),
            )
        })
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeSpotFleetRequests",
        &req.request_id,
        &ec2_list("spotFleetRequestConfigSet", &items),
    ))
}

pub(crate) fn cancel_spot_fleet_requests(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "TerminateInstances")?;
    let ids = indexed_list(&req.query_params, "SpotFleetRequestId");
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for id in &ids {
            if let Some(f) = state.spot_fleets.get_mut(id) {
                f.state = "cancelled_running".to_string();
            }
        }
    }
    let items: Vec<String> = ids
        .iter()
        .map(|id| {
            format!(
                "{}<currentSpotFleetRequestState>cancelled_running</currentSpotFleetRequestState><previousSpotFleetRequestState>active</previousSpotFleetRequestState>",
                ec2_elem("spotFleetRequestId", id)
            )
        })
        .collect();
    let body = format!(
        "{}{}",
        ec2_list("successfulFleetRequestSet", &items),
        ec2_list("unsuccessfulFleetRequestSet", &[])
    );
    Ok(Ec2Service::respond(
        "CancelSpotFleetRequests",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn modify_spot_fleet_request(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "SpotFleetRequestId")?;
    validate_enum(
        &req.query_params,
        "ExcessCapacityTerminationPolicy",
        &["noTermination", "default"],
    )?;
    let _ = svc;
    Ok(Ec2Service::respond(
        "ModifySpotFleetRequest",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_spot_fleet_instances(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "SpotFleetRequestId")?;
    validate_max_results(&req.query_params, 1, 1000)?;
    let body = format!(
        "{}{}",
        ec2_elem("spotFleetRequestId", &id),
        ec2_list("activeInstanceSet", &[])
    );
    Ok(Ec2Service::respond(
        "DescribeSpotFleetInstances",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn describe_spot_fleet_request_history(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "SpotFleetRequestId")?;
    let start = require(&req.query_params, "StartTime")?;
    validate_enum(
        &req.query_params,
        "EventType",
        &[
            "instanceChange",
            "fleetRequestChange",
            "error",
            "information",
        ],
    )?;
    validate_max_results(&req.query_params, 1, 1000)?;
    let body = format!(
        "{}{}{}{}",
        ec2_elem("spotFleetRequestId", &id),
        ec2_elem("startTime", &start),
        ec2_elem("lastEvaluatedTime", FIXED_TIME),
        ec2_list("historyRecordSet", &[])
    );
    Ok(Ec2Service::respond(
        "DescribeSpotFleetRequestHistory",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn describe_spot_price_history(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let az = req
        .query_params
        .get("AvailabilityZone")
        .cloned()
        .unwrap_or_else(|| {
            format!(
                "{}a",
                if req.region.is_empty() {
                    "us-east-1"
                } else {
                    &req.region
                }
            )
        });
    let item = format!(
        "{}{}{}{}{}",
        ec2_elem("instanceType", "t3.micro"),
        ec2_elem("productDescription", "Linux/UNIX"),
        ec2_elem("spotPrice", "0.0035"),
        ec2_elem("timestamp", FIXED_TIME),
        ec2_elem("availabilityZone", &az),
    );
    Ok(Ec2Service::respond(
        "DescribeSpotPriceHistory",
        &req.request_id,
        &ec2_list("spotPriceHistorySet", &[item]),
    ))
}

pub(crate) fn get_spot_placement_scores(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "TargetCapacity")?;
    validate_int_range(&req.query_params, "TargetCapacity", 1, 2_000_000_000)?;
    validate_enum(
        &req.query_params,
        "TargetCapacityUnitType",
        &["vcpu", "memory-mib", "units"],
    )?;
    validate_max_results(&req.query_params, 10, 1000)?;
    let region = if req.region.is_empty() {
        "us-east-1"
    } else {
        &req.region
    };
    let item = format!("{}<score>9</score>", ec2_elem("region", region));
    Ok(Ec2Service::respond(
        "GetSpotPlacementScores",
        &req.request_id,
        &ec2_list("spotPlacementScoreSet", &[item]),
    ))
}

// ---- spot datafeed subscription ----

fn datafeed_xml(bucket: &str, prefix: &str, owner: &str) -> String {
    format!(
        "{}{}{}<state>Active</state>",
        ec2_elem("ownerId", owner),
        ec2_elem("bucket", bucket),
        ec2_elem("prefix", prefix),
    )
}

pub(crate) fn create_spot_datafeed_subscription(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let bucket = require(&req.query_params, "Bucket")?;
    let prefix = req.query_params.get("Prefix").cloned().unwrap_or_default();
    {
        let mut accounts = svc.state.write();
        accounts.get_or_create(&req.account_id).spot_datafeed =
            Some((bucket.clone(), prefix.clone()));
    }
    Ok(Ec2Service::respond(
        "CreateSpotDatafeedSubscription",
        &req.request_id,
        &format!(
            "<spotDatafeedSubscription>{}</spotDatafeedSubscription>",
            datafeed_xml(&bucket, &prefix, &req.account_id)
        ),
    ))
}

pub(crate) fn delete_spot_datafeed_subscription(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    {
        let mut accounts = svc.state.write();
        accounts.get_or_create(&req.account_id).spot_datafeed = None;
    }
    Ok(Ec2Service::respond(
        "DeleteSpotDatafeedSubscription",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_spot_datafeed_subscription(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let sub = accounts
        .get(&req.account_id)
        .and_then(|s| s.spot_datafeed.clone());
    let (bucket, prefix) = sub.unwrap_or_else(|| ("spot-datafeed".to_string(), String::new()));
    Ok(Ec2Service::respond(
        "DescribeSpotDatafeedSubscription",
        &req.request_id,
        &format!(
            "<spotDatafeedSubscription>{}</spotDatafeedSubscription>",
            datafeed_xml(&bucket, &prefix, &owner)
        ),
    ))
}

// ---- EC2 fleets ----

pub(crate) fn create_fleet(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require_struct(&req.query_params, "TargetCapacitySpecification")?;
    validate_enum(
        &req.query_params,
        "ExcessCapacityTerminationPolicy",
        &["no-termination", "termination"],
    )?;
    validate_enum(
        &req.query_params,
        "Type",
        &["request", "maintain", "instant"],
    )?;
    let id = gen_id("fleet");
    let ftype = req
        .query_params
        .get("Type")
        .cloned()
        .unwrap_or_else(|| "maintain".to_string());
    {
        let mut accounts = svc.state.write();
        accounts.get_or_create(&req.account_id).fleets.insert(
            id.clone(),
            Fleet {
                id: id.clone(),
                state: "active".to_string(),
                fleet_type: ftype,
            },
        );
    }
    let body = format!(
        "{}{}{}",
        ec2_elem("fleetId", &id),
        ec2_list("errorSet", &[]),
        ec2_list("fleetInstanceSet", &[])
    );
    Ok(Ec2Service::respond("CreateFleet", &req.request_id, &body))
}

pub(crate) fn delete_fleets(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "TerminateInstances")?;
    let ids = indexed_list(&req.query_params, "FleetId");
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for id in &ids {
            state.fleets.remove(id);
        }
    }
    let items: Vec<String> = ids
        .iter()
        .map(|id| format!("{}<currentFleetState>deleted</currentFleetState><previousFleetState>active</previousFleetState>", ec2_elem("fleetId", id)))
        .collect();
    let body = format!(
        "{}{}",
        ec2_list("successfulFleetDeletionSet", &items),
        ec2_list("unsuccessfulFleetDeletionSet", &[])
    );
    Ok(Ec2Service::respond("DeleteFleets", &req.request_id, &body))
}

pub(crate) fn describe_fleets(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let wanted = indexed_list(&req.query_params, "FleetId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .fleets
        .values()
        .filter(|f| wanted.is_empty() || wanted.contains(&f.id))
        .map(|f| {
            format!(
                "{}{}{}<targetCapacitySpecification><totalTargetCapacity>1</totalTargetCapacity></targetCapacitySpecification>",
                ec2_elem("fleetId", &f.id),
                ec2_elem("fleetState", &f.state),
                ec2_elem("type", &f.fleet_type),
            )
        })
        .collect();
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeFleets",
        &req.request_id,
        &ec2_list("fleetSet", &items),
    ))
}

pub(crate) fn modify_fleet(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "FleetId")?;
    validate_enum(
        &req.query_params,
        "ExcessCapacityTerminationPolicy",
        &["no-termination", "termination"],
    )?;
    let _ = svc;
    Ok(Ec2Service::respond(
        "ModifyFleet",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_fleet_history(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "FleetId")?;
    let start = require(&req.query_params, "StartTime")?;
    validate_enum(
        &req.query_params,
        "EventType",
        &["instance-change", "fleet-change", "service-error"],
    )?;
    let body = format!(
        "{}{}{}{}",
        ec2_elem("fleetId", &id),
        ec2_elem("startTime", &start),
        ec2_elem("lastEvaluatedTime", FIXED_TIME),
        ec2_list("historyRecordSet", &[])
    );
    Ok(Ec2Service::respond(
        "DescribeFleetHistory",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn describe_fleet_instances(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "FleetId")?;
    let body = format!(
        "{}{}",
        ec2_elem("fleetId", &id),
        ec2_list("activeInstanceSet", &[])
    );
    Ok(Ec2Service::respond(
        "DescribeFleetInstances",
        &req.request_id,
        &body,
    ))
}
