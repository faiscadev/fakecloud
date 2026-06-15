//! EC2 instance control plane (metadata-faithful). A Docker-backed runtime
//! layers real container execution on top of this in a follow-up; the API
//! surface and conformance live here.

use std::collections::HashMap;

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    gen_id, indexed_list, invalid_parameter_value, parse_filters, require, require_struct,
    validate_enum, Filter,
};
use crate::state::{Ec2State, Instance, Tag};

const LAUNCH_TIME: &str = "2024-01-01T00:00:00.000Z";

const INSTANCE_TYPES: &[&str] = &[
    "t3.micro",
    "t3.small",
    "t3.medium",
    "t3.large",
    "m5.large",
    "m5.xlarge",
    "c5.large",
    "r5.large",
    "t2.micro",
];

fn state_xml(tag: &str, code: i64, name: &str) -> String {
    format!("<{tag}><code>{code}</code><name>{name}</name></{tag}>")
}

fn instance_xml(i: &Instance, tags: &[Tag], owner: &str) -> String {
    let groups: Vec<String> = i
        .security_group_ids
        .iter()
        .map(|g| format!("{}{}", ec2_elem("groupId", g), ec2_elem("groupName", g)))
        .collect();
    let public = i
        .public_ip
        .as_ref()
        .map(|ip| {
            format!(
                "{}{}",
                ec2_elem("ipAddress", ip),
                ec2_elem(
                    "dnsName",
                    &format!("ec2-{}.compute.amazonaws.com", ip.replace('.', "-"))
                )
            )
        })
        .unwrap_or_default();
    let m = &i.metadata_options;
    let metadata_options = format!(
        "<metadataOptions><state>applied</state><httpTokens>{}</httpTokens>\
         <httpPutResponseHopLimit>{}</httpPutResponseHopLimit><httpEndpoint>{}</httpEndpoint>\
         <httpProtocolIpv6>{}</httpProtocolIpv6><instanceMetadataTags>{}</instanceMetadataTags></metadataOptions>",
        m.http_tokens,
        m.http_put_response_hop_limit,
        m.http_endpoint,
        m.http_protocol_ipv6,
        m.instance_metadata_tags,
    );
    let cpu_options = i
        .cpu_options
        .as_ref()
        .map(|c| {
            format!(
                "<cpuOptions><coreCount>{}</coreCount><threadsPerCore>{}</threadsPerCore></cpuOptions>",
                c.core_count, c.threads_per_core
            )
        })
        .unwrap_or_default();
    let tenancy = i.placement_tenancy.as_deref().unwrap_or("default");
    let placement_group = i
        .placement_group_name
        .as_ref()
        .map(|g| ec2_elem("groupName", g))
        .unwrap_or_default();
    format!(
        "{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
        ec2_elem("instanceId", &i.instance_id),
        ec2_elem("imageId", &i.image_id),
        state_xml("instanceState", i.state_code, &i.state_name),
        ec2_elem(
            "privateDnsName",
            &format!("ip-{}.ec2.internal", i.private_ip.replace('.', "-"))
        ),
        ec2_elem("privateIpAddress", &i.private_ip),
        public,
        ec2_elem("instanceType", &i.instance_type),
        ec2_elem("launchTime", &i.launch_time),
        ec2_elem("amiLaunchIndex", &i.ami_launch_index.to_string()),
        ec2_elem("architecture", "x86_64"),
        ec2_elem("rootDeviceType", "ebs"),
        ec2_elem("rootDeviceName", "/dev/xvda"),
        ec2_elem("virtualizationType", "hvm"),
        ec2_elem("hypervisor", "xen"),
        format_args!(
            "<ebsOptimized>{}</ebsOptimized><sourceDestCheck>{}</sourceDestCheck>",
            i.ebs_optimized, i.source_dest_check
        ),
        format_args!(
            "<placement><availabilityZone>{}</availabilityZone>{}<tenancy>{}</tenancy></placement>",
            i.az, placement_group, tenancy
        ),
        format_args!(
            "<monitoring><state>{}</state></monitoring>",
            if i.monitoring { "enabled" } else { "disabled" }
        ),
        format_args!(
            "{}{}",
            i.subnet_id
                .as_ref()
                .map(|s| ec2_elem("subnetId", s))
                .unwrap_or_default(),
            i.vpc_id
                .as_ref()
                .map(|s| ec2_elem("vpcId", s))
                .unwrap_or_default(),
        ),
        i.key_name
            .as_ref()
            .map(|k| ec2_elem("keyName", k))
            .unwrap_or_default(),
        format_args!(
            "{}{}",
            ec2_list("groupSet", &groups),
            ec2_elem("ownerId", owner)
        ),
        metadata_options,
        cpu_options,
        super::tags::tag_set_xml(tags),
    )
}

fn reservation_xml(reservation_id: &str, owner: &str, instances: &[String]) -> String {
    format!(
        "{}{}{}{}",
        ec2_elem("reservationId", reservation_id),
        ec2_elem("ownerId", owner),
        ec2_list("groupSet", &[]),
        ec2_list("instancesSet", instances),
    )
}

pub(crate) async fn run_instances(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let min: usize = require(&req.query_params, "MinCount")?
        .parse()
        .map_err(|_| invalid_parameter_value("MinCount must be an integer"))?;
    let max: usize = require(&req.query_params, "MaxCount")?
        .parse()
        .map_err(|_| invalid_parameter_value("MaxCount must be an integer"))?;
    if min == 0 {
        return Err(invalid_parameter_value("MinCount must be at least 1"));
    }
    if min > max {
        return Err(invalid_parameter_value(format!(
            "Invalid value '{max}' for parameter maxCount is invalid. The maxCount must be equal to or greater than the minCount."
        )));
    }
    validate_enum_instance_type(req)?;
    validate_enum(
        &req.query_params,
        "InstanceInitiatedShutdownBehavior",
        &["stop", "terminate"],
    )?;
    // AWS best-effort launches MaxCount instances (>= MinCount). With no real
    // capacity ceiling here we launch the full MaxCount, clamped for safety.
    let count = max.clamp(min.max(1), 64);
    let reservation_id = gen_id("r");
    let image_id = req
        .query_params
        .get("ImageId")
        .cloned()
        .unwrap_or_else(|| "ami-00000000000000000".to_string());
    let instance_type = req
        .query_params
        .get("InstanceType")
        .cloned()
        .unwrap_or_else(|| "t3.micro".to_string());
    let key_name = req.query_params.get("KeyName").cloned();
    let subnet_id = req.query_params.get("SubnetId").cloned();
    let sg_ids = indexed_list(&req.query_params, "SecurityGroupId");
    let user_data = req.query_params.get("UserData").cloned();
    let owner = req.account_id.clone();
    let az = format!(
        "{}a",
        if req.region.is_empty() {
            "us-east-1"
        } else {
            &req.region
        }
    );

    // Reserved `fakecloud-k8s/*` scheduling tags are read from the request's
    // TagSpecification here, before the backing Pod is built.
    let instance_tags = crate::service::tags::tag_specifications_for(&req.query_params, "instance");

    // Resolve the VPC from the requested subnet (so the `vpc-id` filter and
    // describe output reflect reality), and decide whether a public IP is
    // assigned per AWS rules: explicit `AssociatePublicIpAddress=true`, else
    // the subnet's `map_public_ip_on_launch` / default-subnet behavior. With
    // no subnet, an instance launched into the (implicit) default VPC gets a
    // public IP, matching the EC2-default-VPC contract.
    let assoc_public = req
        .query_params
        .get("NetworkInterface.1.AssociatePublicIpAddress")
        .or_else(|| req.query_params.get("AssociatePublicIpAddress"))
        .map(|v| v == "true");
    let (vpc_id, subnet_auto_public) = {
        let accounts = svc.state.read();
        match (accounts.get(&req.account_id), subnet_id.as_ref()) {
            (Some(state), Some(sid)) => state
                .subnets
                .get(sid)
                .map(|s| {
                    (
                        Some(s.vpc_id.clone()),
                        s.map_public_ip_on_launch || s.default_for_az,
                    )
                })
                .unwrap_or((None, false)),
            // No subnet specified: treat as a launch into the default VPC,
            // which assigns public IPs by default.
            _ => (None, true),
        }
    };
    let assign_public = assoc_public.unwrap_or(subnet_auto_public);

    // Generate instance ids; insert each instance synchronously in `pending`
    // state (code 0), then boot the backing container in a background task
    // that reconciles the instance to `running` (code 16) when it's up, or to
    // `stopped` (code 80) on failure. RunInstances returns immediately so a
    // cold image pull / k8s Pod readiness never blocks the client (mirrors
    // RDS CreateDBInstance). With no runtime configured the instance is
    // flipped to `running` immediately in a spawned task.
    let ids: Vec<String> = (0..count).map(|_| gen_id("i")).collect();
    let mut rendered = Vec::new();
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for (idx, id) in ids.iter().enumerate() {
            let inst = Instance {
                instance_id: id.clone(),
                image_id: image_id.clone(),
                instance_type: instance_type.clone(),
                state_code: 0,
                state_name: "pending".to_string(),
                private_ip: format!("10.0.0.{}", 10 + idx),
                public_ip: if assign_public {
                    Some(format!("52.0.0.{}", 10 + idx))
                } else {
                    None
                },
                subnet_id: subnet_id.clone(),
                vpc_id: vpc_id.clone(),
                key_name: key_name.clone(),
                security_group_ids: sg_ids.clone(),
                reservation_id: reservation_id.clone(),
                ami_launch_index: idx as i64,
                monitoring: false,
                az: az.clone(),
                launch_time: LAUNCH_TIME.to_string(),
                container_id: None,
                disable_api_termination: false,
                disable_api_stop: false,
                source_dest_check: true,
                ebs_optimized: false,
                instance_initiated_shutdown_behavior: req
                    .query_params
                    .get("InstanceInitiatedShutdownBehavior")
                    .cloned()
                    .unwrap_or_else(|| "stop".to_string()),
                user_data: user_data.clone().filter(|s| !s.is_empty()),
                metadata_options: crate::state::MetadataOptions::default(),
                cpu_options: None,
                bandwidth_weighting: None,
                maintenance_options: crate::state::MaintenanceOptions::default(),
                placement_tenancy: None,
                placement_affinity: None,
                placement_group_name: req.query_params.get("Placement.GroupName").cloned(),
            };
            crate::service::tags::apply_tag_specifications(
                state,
                &req.query_params,
                id,
                "instance",
            );
            let tags = state.tags_for(id).to_vec();
            rendered.push(instance_xml(&inst, &tags, &owner));
            state.instances.insert(id.clone(), inst);
        }
    }

    // Background boot: bring up each backing container and reconcile state.
    {
        let svc_state = svc.state.clone();
        let runtime = svc.runtime.clone();
        let account_id = req.account_id.clone();
        let ids = ids.clone();
        tokio::spawn(async move {
            for id in &ids {
                let running = if let Some(rt) = &runtime {
                    match rt
                        .run_instance(id, user_data.as_deref(), &instance_tags)
                        .await
                    {
                        Ok(r) => Some(r),
                        Err(e) => {
                            tracing::warn!(instance_id = %id, error = %e, "EC2 instance container failed to start; serving metadata-only");
                            None
                        }
                    }
                } else {
                    None
                };
                reconcile_started(&svc_state, &account_id, id, running);
            }
        });
    }

    let body = reservation_xml(&reservation_id, &owner, &rendered);
    Ok(Ec2Service::respond("RunInstances", &req.request_id, &body))
}

/// Flip a `pending`/`stopped` instance to `running` after its backing
/// container is up. Re-acquires the lock and re-checks the instance still
/// exists and hasn't been terminated by a concurrent op before writing the
/// container handle / IP (bug-hunt 2026-06-15 finding 0.4).
fn reconcile_started(
    state: &crate::state::SharedEc2State,
    account_id: &str,
    id: &str,
    running: Option<crate::runtime::RunningInstance>,
) {
    let mut accounts = state.write();
    let Some(s) = accounts.get_mut(account_id) else {
        return;
    };
    let Some(inst) = s.instances.get_mut(id) else {
        return;
    };
    // A concurrent Terminate (code 48) or Stop wins: don't resurrect it.
    if inst.state_code == 48 || inst.state_code == 80 {
        return;
    }
    inst.state_code = 16;
    inst.state_name = "running".to_string();
    if let Some(r) = running {
        inst.private_ip = r.private_ip;
        inst.container_id = Some(r.container_id);
    }
}

fn validate_enum_instance_type(req: &AwsRequest) -> Result<(), AwsServiceError> {
    // InstanceType has ~850 enum members; accept any non-empty value that looks
    // like a `family.size` token rather than enumerating them all.
    if let Some(v) = req
        .query_params
        .get("InstanceType")
        .filter(|v| !v.is_empty())
    {
        if !v.contains('.') {
            return Err(invalid_parameter_value(format!(
                "Invalid instance type '{v}'"
            )));
        }
    }
    Ok(())
}

/// Is a transition to `new_code` legal from the instance's `current` state?
/// Terminated (48) is terminal — no transition out of it is allowed. Stop/Start
/// from any non-terminal state is accepted (AWS is lenient on no-op
/// transitions, e.g. stopping an already-stopped instance).
fn transition_allowed(current: i64, new_code: i64) -> bool {
    if current == 48 {
        // A terminated instance can only be (re-)terminated (a no-op).
        return new_code == 48;
    }
    true
}

async fn change_state(
    svc: &Ec2Service,
    req: &AwsRequest,
    action: &str,
    new_code: i64,
    new_name: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let ids = indexed_list(&req.query_params, "InstanceId");

    // Validate existence + legal transitions BEFORE mutating anything: AWS
    // fails the whole call (no partial application) on a bad id or illegal
    // transition (bug-hunt 2026-06-15 findings 1.9 / 0.4).
    {
        let accounts = svc.state.read();
        let empty = Ec2State::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        for id in &ids {
            let inst = state
                .instances
                .get(id)
                .ok_or_else(|| crate::service_helpers::instance_not_found(id))?;
            if !transition_allowed(inst.state_code, new_code) {
                return Err(crate::service_helpers::incorrect_instance_state(
                    id,
                    &inst.state_name,
                ));
            }
            // Termination / stop protection.
            if new_code == 48 && inst.disable_api_termination {
                return Err(AwsServiceError::aws_error(
                    http::StatusCode::BAD_REQUEST,
                    "OperationNotPermitted",
                    format!(
                        "The instance '{id}' may not be terminated. Modify its 'disableApiTermination' instance attribute and try again."
                    ),
                ));
            }
            if new_code == 80 && inst.disable_api_stop {
                return Err(AwsServiceError::aws_error(
                    http::StatusCode::BAD_REQUEST,
                    "OperationNotPermitted",
                    format!(
                        "The instance '{id}' may not be stopped. Modify its 'disableApiStop' instance attribute and try again."
                    ),
                ));
            }
        }
    }

    // For StartInstances, AWS returns the instances in `pending` (not yet
    // `running`); the container comes up in the background. Stop/Terminate
    // apply their target state immediately (and AWS reports stopping/
    // shutting-down transitionally, but the durable target is what callers
    // poll for).
    let applied_code = if new_code == 16 { 0 } else { new_code };
    let applied_name = if new_code == 16 { "pending" } else { new_name };

    let mut changes = Vec::new();
    let mut affected: Vec<String> = Vec::new();
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for id in &ids {
            let (prev_code, prev_name) = state
                .instances
                .get(id)
                .map(|i| (i.state_code, i.state_name.clone()))
                .unwrap_or((16, "running".to_string()));
            if let Some(inst) = state.instances.get_mut(id) {
                inst.state_code = applied_code;
                inst.state_name = applied_name.to_string();
                if new_code == 80 || new_code == 48 {
                    inst.public_ip = None;
                }
                affected.push(id.clone());
                // A terminated instance no longer has a backing container.
                if new_code == 48 {
                    inst.container_id = None;
                }
            }
            changes.push(format!(
                "{}{}{}",
                ec2_elem("instanceId", id),
                state_xml("currentState", applied_code, applied_name),
                state_xml("previousState", prev_code, &prev_name),
            ));
        }
    }

    // Drive the backing container's lifecycle in the background so the response
    // returns immediately (bug-hunt 2026-06-15 findings 0.2 / 0.4). Each op
    // re-checks the instance's state after its await before persisting.
    {
        let svc_state = svc.state.clone();
        let runtime = svc.runtime.clone();
        let account_id = req.account_id.clone();
        tokio::spawn(async move {
            for id in &affected {
                match new_code {
                    16 => {
                        let running = match &runtime {
                            Some(rt) => rt.start_instance(id).await,
                            None => None,
                        };
                        reconcile_started(&svc_state, &account_id, id, running);
                    }
                    80 => {
                        if let Some(rt) = &runtime {
                            rt.stop_instance(id).await;
                        }
                    }
                    48 => {
                        if let Some(rt) = &runtime {
                            rt.terminate_instance(id).await;
                        }
                    }
                    _ => {}
                }
            }
        });
    }

    Ok(Ec2Service::respond(
        action,
        &req.request_id,
        &ec2_list("instancesSet", &changes),
    ))
}

pub(crate) async fn start_instances(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    change_state(svc, req, "StartInstances", 16, "running").await
}
pub(crate) async fn stop_instances(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    change_state(svc, req, "StopInstances", 80, "stopped").await
}
pub(crate) async fn terminate_instances(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    change_state(svc, req, "TerminateInstances", 48, "terminated").await
}

pub(crate) async fn reboot_instances(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let ids = indexed_list(&req.query_params, "InstanceId");
    // Validate existence + reject rebooting a terminated instance before doing
    // any work (findings 1.9). AWS rejects RebootInstances on a bad id.
    let backed: Vec<String> = {
        let accounts = svc.state.read();
        let empty = Ec2State::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let mut backed = Vec::new();
        for id in &ids {
            let inst = state
                .instances
                .get(id)
                .ok_or_else(|| crate::service_helpers::instance_not_found(id))?;
            if inst.state_code == 48 {
                return Err(crate::service_helpers::incorrect_instance_state(
                    id,
                    &inst.state_name,
                ));
            }
            if inst.container_id.is_some() {
                backed.push(id.clone());
            }
        }
        backed
    };
    // Reboot the backing containers in the background so the API returns
    // immediately (k8s Pod recreate can take up to 90s) — finding 0.2.
    {
        let svc_state = svc.state.clone();
        let runtime = svc.runtime.clone();
        let account_id = req.account_id.clone();
        tokio::spawn(async move {
            let Some(rt) = runtime else {
                return;
            };
            for id in &backed {
                // k8s reboot recreates the Pod under a new name/IP; persist them
                // so describe/introspection stay accurate (Docker returns None).
                if let Some(running) = rt.reboot_instance(id).await {
                    let mut accounts = svc_state.write();
                    if let Some(state) = accounts.get_mut(&account_id) {
                        if let Some(inst) = state.instances.get_mut(id) {
                            // Don't clobber an instance a concurrent op
                            // terminated mid-reboot.
                            if inst.state_code != 48 {
                                inst.private_ip = running.private_ip;
                                inst.container_id = Some(running.container_id);
                            }
                        }
                    }
                }
            }
        });
    }
    Ok(Ec2Service::respond(
        "RebootInstances",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn monitor_instances(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    monitor(svc, req, "MonitorInstances", true)
}
pub(crate) fn unmonitor_instances(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    monitor(svc, req, "UnmonitorInstances", false)
}

fn monitor(
    svc: &Ec2Service,
    req: &AwsRequest,
    action: &str,
    enable: bool,
) -> Result<AwsResponse, AwsServiceError> {
    let ids = indexed_list(&req.query_params, "InstanceId");
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for id in &ids {
            if let Some(i) = state.instances.get_mut(id) {
                i.monitoring = enable;
            }
        }
    }
    let items: Vec<String> = ids
        .iter()
        .map(|id| {
            format!(
                "{}<monitoring><state>{}</state></monitoring>",
                ec2_elem("instanceId", id),
                if enable { "pending" } else { "disabling" }
            )
        })
        .collect();
    Ok(Ec2Service::respond(
        action,
        &req.request_id,
        &ec2_list("instancesSet", &items),
    ))
}

pub(crate) fn describe_instances(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    crate::service_helpers::validate_max_results(&req.query_params, 5, 1000)?;
    let max_results = parse_max_results(&req.query_params);
    let next_token = req.query_params.get("NextToken").map(String::as_str);
    let filters = parse_filters(&req.query_params);
    let wanted = indexed_list(&req.query_params, "InstanceId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);

    // Flatten matching instances into a stable order (by reservation, then id),
    // then paginate over the flat instance list — AWS counts instances, not
    // reservations, against MaxResults.
    let mut matching: Vec<&Instance> = state
        .instances
        .values()
        .filter(|i| wanted.is_empty() || wanted.contains(&i.instance_id))
        .filter(|i| inst_match(i, state.tags_for(&i.instance_id), &filters))
        .collect();
    matching.sort_by(|a, b| {
        a.reservation_id
            .cmp(&b.reservation_id)
            .then(a.instance_id.cmp(&b.instance_id))
    });
    let (page, token) = crate::service_helpers::paginate(&matching, next_token, max_results);

    // Group the page back into reservations, preserving the sorted order.
    let mut by_res: HashMap<String, Vec<String>> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for i in page {
        if !by_res.contains_key(&i.reservation_id) {
            order.push(i.reservation_id.clone());
        }
        by_res
            .entry(i.reservation_id.clone())
            .or_default()
            .push(instance_xml(i, state.tags_for(&i.instance_id), &owner));
    }
    let reservations: Vec<String> = order
        .iter()
        .map(|rid| {
            let insts = by_res.remove(rid).unwrap_or_default();
            reservation_xml(rid, &owner, &insts)
        })
        .collect();
    let body = format!(
        "{}{}",
        ec2_list("reservationSet", &reservations),
        token.map(|t| ec2_elem("nextToken", &t)).unwrap_or_default(),
    );
    Ok(Ec2Service::respond(
        "DescribeInstances",
        &req.request_id,
        &body,
    ))
}

/// Parse `MaxResults` into an optional usize (already range-validated by the
/// caller via `validate_max_results`).
fn parse_max_results(params: &HashMap<String, String>) -> Option<usize> {
    params
        .get("MaxResults")
        .filter(|v| !v.is_empty())
        .and_then(|v| v.parse::<usize>().ok())
}

fn inst_match(i: &Instance, tags: &[Tag], filters: &[Filter]) -> bool {
    use crate::service_helpers::filter_value_matches;
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "instance-id" => vec![i.instance_id.clone()],
            "instance-type" => vec![i.instance_type.clone()],
            "image-id" => vec![i.image_id.clone()],
            "instance-state-name" => vec![i.state_name.clone()],
            "instance-state-code" => vec![i.state_code.to_string()],
            "vpc-id" => i.vpc_id.clone().into_iter().collect(),
            "subnet-id" => i.subnet_id.clone().into_iter().collect(),
            "availability-zone" => vec![i.az.clone()],
            "private-ip-address" => vec![i.private_ip.clone()],
            "ip-address" => i.public_ip.clone().into_iter().collect(),
            "key-name" => i.key_name.clone().into_iter().collect(),
            "architecture" => vec!["x86_64".to_string()],
            "tag-key" => tags.iter().map(|t| t.key.clone()).collect(),
            name => {
                if let Some(key) = name.strip_prefix("tag:") {
                    tags.iter()
                        .filter(|t| t.key == key)
                        .map(|t| t.value.clone())
                        .collect()
                } else {
                    // Unknown filter name: AWS rejects with InvalidParameterValue.
                    // Returning `false` (match nothing) is the safe approximation
                    // rather than `return true` (match everything) — finding 1.16.
                    return false;
                }
            }
        };
        f.values
            .iter()
            .any(|v| candidates.iter().any(|c| filter_value_matches(v, c)))
    })
}

pub(crate) fn describe_instance_status(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    crate::service_helpers::validate_max_results(&req.query_params, 5, 1000)?;
    let max_results = parse_max_results(&req.query_params);
    let next_token = req.query_params.get("NextToken").map(String::as_str);
    let filters = parse_filters(&req.query_params);
    let wanted = indexed_list(&req.query_params, "InstanceId");
    let include_all = req
        .query_params
        .get("IncludeAllInstances")
        .map(|v| v == "true")
        .unwrap_or(false);
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut matching: Vec<&Instance> = state
        .instances
        .values()
        .filter(|i| wanted.is_empty() || wanted.contains(&i.instance_id))
        .filter(|i| include_all || i.state_name == "running")
        .filter(|i| inst_match(i, state.tags_for(&i.instance_id), &filters))
        .collect();
    matching.sort_by(|a, b| a.instance_id.cmp(&b.instance_id));
    let (page, token) = crate::service_helpers::paginate(&matching, next_token, max_results);
    let items: Vec<String> = page
        .iter()
        .map(|i| {
            format!(
                "{}{}{}{}{}{}",
                ec2_elem("instanceId", &i.instance_id),
                ec2_elem("availabilityZone", &i.az),
                state_xml("instanceState", i.state_code, &i.state_name),
                "<instanceStatus><status>ok</status></instanceStatus>",
                "<systemStatus><status>ok</status></systemStatus>",
                ec2_list("eventsSet", &[]),
            )
        })
        .collect();
    let body = format!(
        "{}{}",
        ec2_list("instanceStatusSet", &items),
        token.map(|t| ec2_elem("nextToken", &t)).unwrap_or_default(),
    );
    Ok(Ec2Service::respond(
        "DescribeInstanceStatus",
        &req.request_id,
        &body,
    ))
}

fn instance_type_items(req: &AwsRequest) -> Vec<String> {
    let wanted = indexed_list(&req.query_params, "InstanceType");
    INSTANCE_TYPES
        .iter()
        .filter(|t| wanted.is_empty() || wanted.iter().any(|w| w == *t))
        .map(|t| {
            format!(
                "{}<currentGeneration>true</currentGeneration><bareMetal>false</bareMetal>\
                 <hypervisor>nitro</hypervisor><instanceStorageSupported>false</instanceStorageSupported>\
                 <processorInfo><supportedArchitectures><item>x86_64</item></supportedArchitectures></processorInfo>\
                 <vCpuInfo><defaultVCpus>2</defaultVCpus></vCpuInfo>\
                 <memoryInfo><sizeInMiB>1024</sizeInMiB></memoryInfo>\
                 <supportedVirtualizationTypes><item>hvm</item></supportedVirtualizationTypes>",
                ec2_elem("instanceType", t),
            )
        })
        .collect()
}

pub(crate) fn describe_instance_types(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    crate::service_helpers::validate_max_results(&req.query_params, 5, 100)?;
    Ok(Ec2Service::respond(
        "DescribeInstanceTypes",
        &req.request_id,
        &ec2_list("instanceTypeSet", &instance_type_items(req)),
    ))
}

pub(crate) fn get_instance_types_from_requirements(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require_struct(&req.query_params, "InstanceRequirements")?;
    let items: Vec<String> = INSTANCE_TYPES
        .iter()
        .map(|t| format!("<instanceType>{t}</instanceType>"))
        .collect();
    Ok(Ec2Service::respond(
        "GetInstanceTypesFromInstanceRequirements",
        &req.request_id,
        &ec2_list("instanceTypeSet", &items),
    ))
}

// ---- attributes ----

pub(crate) fn describe_instance_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceId")?;
    let attribute = require(&req.query_params, "Attribute")?;
    validate_enum(&req.query_params, "Attribute", ATTRIBUTE_VALUES)?;
    let accounts = svc.state.read();
    let inst = accounts
        .get(&req.account_id)
        .and_then(|s| s.instances.get(&id))
        .ok_or_else(|| crate::service_helpers::instance_not_found(&id))?;
    let attr_xml = match attribute.as_str() {
        "instanceType" => format!(
            "<instanceType><value>{}</value></instanceType>",
            inst.instance_type
        ),
        "disableApiTermination" => format!(
            "<disableApiTermination><value>{}</value></disableApiTermination>",
            inst.disable_api_termination
        ),
        "disableApiStop" => format!(
            "<disableApiStop><value>{}</value></disableApiStop>",
            inst.disable_api_stop
        ),
        "ebsOptimized" => format!(
            "<ebsOptimized><value>{}</value></ebsOptimized>",
            inst.ebs_optimized
        ),
        "sourceDestCheck" => format!(
            "<sourceDestCheck><value>{}</value></sourceDestCheck>",
            inst.source_dest_check
        ),
        "instanceInitiatedShutdownBehavior" => format!(
            "<instanceInitiatedShutdownBehavior><value>{}</value></instanceInitiatedShutdownBehavior>",
            inst.instance_initiated_shutdown_behavior
        ),
        "userData" => match &inst.user_data {
            Some(d) => format!("<userData><value>{d}</value></userData>"),
            None => "<userData/>".to_string(),
        },
        "groupSet" => {
            let groups: Vec<String> = inst
                .security_group_ids
                .iter()
                .map(|g| format!("{}{}", ec2_elem("groupId", g), ec2_elem("groupName", g)))
                .collect();
            ec2_list("groupSet", &groups)
        }
        _ => String::new(),
    };
    let body = format!("{}{}", ec2_elem("instanceId", &id), attr_xml);
    Ok(Ec2Service::respond(
        "DescribeInstanceAttribute",
        &req.request_id,
        &body,
    ))
}

const ATTRIBUTE_VALUES: &[&str] = &[
    "instanceType",
    "kernel",
    "ramdisk",
    "userData",
    "disableApiTermination",
    "instanceInitiatedShutdownBehavior",
    "rootDeviceName",
    "blockDeviceMapping",
    "productCodes",
    "sourceDestCheck",
    "groupSet",
    "ebsOptimized",
    "sriovNetSupport",
    "enaSupport",
    "enclaveOptions",
    "disableApiStop",
];

/// Read an attribute value from either the flat `<Attr>.Value=` form (e.g.
/// `DisableApiTermination.Value=true`) or the bare `<Attr>=` form the CLI
/// sends for some attrs (`SourceDestCheck.Value`, `Attribute`+`Value`).
fn attr_bool(params: &HashMap<String, String>, key: &str) -> Option<bool> {
    params
        .get(&format!("{key}.Value"))
        .or_else(|| params.get(key))
        .map(|v| v == "true")
}

fn attr_str<'a>(params: &'a HashMap<String, String>, key: &str) -> Option<&'a String> {
    params
        .get(&format!("{key}.Value"))
        .or_else(|| params.get(key))
}

pub(crate) fn modify_instance_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceId")?;
    // `Attribute` is only present when called in the generic form; the
    // convenience form passes the attribute as its own member (e.g.
    // `DisableApiTermination.Value`). Validate it only when present.
    validate_enum(&req.query_params, "Attribute", ATTRIBUTE_VALUES)?;
    let p = &req.query_params;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let inst = state
        .instances
        .get_mut(&id)
        .ok_or_else(|| crate::service_helpers::instance_not_found(&id))?;

    // Generic form: Attribute=<name> Value=<value>.
    if let Some(attr) = p.get("Attribute").filter(|v| !v.is_empty()) {
        let value = p.get("Value").cloned();
        match attr.as_str() {
            "instanceType" => {
                if let Some(v) = value {
                    inst.instance_type = v;
                }
            }
            "userData" => inst.user_data = value.filter(|s| !s.is_empty()),
            "disableApiTermination" => {
                inst.disable_api_termination = value.as_deref() == Some("true")
            }
            "disableApiStop" => inst.disable_api_stop = value.as_deref() == Some("true"),
            "sourceDestCheck" => inst.source_dest_check = value.as_deref() == Some("true"),
            "ebsOptimized" => inst.ebs_optimized = value.as_deref() == Some("true"),
            "instanceInitiatedShutdownBehavior" => {
                if let Some(v) = value {
                    inst.instance_initiated_shutdown_behavior = v;
                }
            }
            _ => {}
        }
    }
    // Convenience form: each modifiable attribute as its own member.
    if let Some(v) = attr_bool(p, "DisableApiTermination") {
        inst.disable_api_termination = v;
    }
    if let Some(v) = attr_bool(p, "DisableApiStop") {
        inst.disable_api_stop = v;
    }
    if let Some(v) = attr_bool(p, "SourceDestCheck") {
        inst.source_dest_check = v;
    }
    if let Some(v) = attr_bool(p, "EbsOptimized") {
        inst.ebs_optimized = v;
    }
    if let Some(v) = attr_str(p, "InstanceType") {
        inst.instance_type = v.clone();
    }
    if let Some(v) = attr_str(p, "InstanceInitiatedShutdownBehavior") {
        inst.instance_initiated_shutdown_behavior = v.clone();
    }
    if let Some(v) = attr_str(p, "UserData") {
        inst.user_data = Some(v.clone()).filter(|s| !s.is_empty());
    }
    let new_groups = indexed_list(p, "GroupId");
    if !new_groups.is_empty() {
        inst.security_group_ids = new_groups;
    }

    Ok(Ec2Service::respond(
        "ModifyInstanceAttribute",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn reset_instance_attribute(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceId")?;
    let attribute = require(&req.query_params, "Attribute")?;
    validate_enum(&req.query_params, "Attribute", ATTRIBUTE_VALUES)?;
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(&req.account_id);
    let inst = state
        .instances
        .get_mut(&id)
        .ok_or_else(|| crate::service_helpers::instance_not_found(&id))?;
    // Reset to AWS defaults. AWS only supports resetting kernel/ramdisk/
    // sourceDestCheck, but we reset the corresponding field for any attr.
    match attribute.as_str() {
        "sourceDestCheck" => inst.source_dest_check = true,
        "disableApiTermination" => inst.disable_api_termination = false,
        "disableApiStop" => inst.disable_api_stop = false,
        "ebsOptimized" => inst.ebs_optimized = false,
        "userData" => inst.user_data = None,
        "instanceInitiatedShutdownBehavior" => {
            inst.instance_initiated_shutdown_behavior = "stop".to_string()
        }
        _ => {}
    }
    Ok(Ec2Service::respond(
        "ResetInstanceAttribute",
        &req.request_id,
        &ec2_return(true),
    ))
}

// ---- modify-* and misc ----

/// Look up an instance for mutation, erroring with `InvalidInstanceID.NotFound`
/// when absent. Returns a write guard so the caller can mutate in place.
fn with_instance_mut<R>(
    svc: &Ec2Service,
    account_id: &str,
    id: &str,
    f: impl FnOnce(&mut Instance) -> R,
) -> Result<R, AwsServiceError> {
    let mut accounts = svc.state.write();
    let state = accounts.get_or_create(account_id);
    let inst = state
        .instances
        .get_mut(id)
        .ok_or_else(|| crate::service_helpers::instance_not_found(id))?;
    Ok(f(inst))
}

pub(crate) fn modify_instance_placement(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceId")?;
    validate_enum(
        &req.query_params,
        "Tenancy",
        &["default", "dedicated", "host"],
    )?;
    validate_enum(&req.query_params, "Affinity", &["default", "host"])?;
    let p = req.query_params.clone();
    with_instance_mut(svc, &req.account_id, &id, |inst| {
        if let Some(t) = p.get("Tenancy").filter(|v| !v.is_empty()) {
            inst.placement_tenancy = Some(t.clone());
        }
        if let Some(a) = p.get("Affinity").filter(|v| !v.is_empty()) {
            inst.placement_affinity = Some(a.clone());
        }
        if let Some(g) = p.get("GroupName") {
            inst.placement_group_name = Some(g.clone()).filter(|s| !s.is_empty());
        }
    })?;
    Ok(Ec2Service::respond(
        "ModifyInstancePlacement",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn modify_instance_metadata_options(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceId")?;
    validate_enum(&req.query_params, "HttpTokens", &["optional", "required"])?;
    validate_enum(&req.query_params, "HttpEndpoint", &["disabled", "enabled"])?;
    validate_enum(
        &req.query_params,
        "HttpProtocolIpv6",
        &["disabled", "enabled"],
    )?;
    validate_enum(
        &req.query_params,
        "InstanceMetadataTags",
        &["disabled", "enabled"],
    )?;
    let p = req.query_params.clone();
    let opts = with_instance_mut(svc, &req.account_id, &id, |inst| {
        let m = &mut inst.metadata_options;
        if let Some(v) = p.get("HttpTokens").filter(|v| !v.is_empty()) {
            m.http_tokens = v.clone();
        }
        if let Some(v) = p.get("HttpEndpoint").filter(|v| !v.is_empty()) {
            m.http_endpoint = v.clone();
        }
        if let Some(v) = p.get("HttpProtocolIpv6").filter(|v| !v.is_empty()) {
            m.http_protocol_ipv6 = v.clone();
        }
        if let Some(v) = p.get("InstanceMetadataTags").filter(|v| !v.is_empty()) {
            m.instance_metadata_tags = v.clone();
        }
        if let Some(n) = p
            .get("HttpPutResponseHopLimit")
            .and_then(|v| v.parse::<i64>().ok())
        {
            m.http_put_response_hop_limit = n;
        }
        m.clone()
    })?;
    let body = format!(
        "{}<instanceMetadataOptions><state>applied</state><httpTokens>{}</httpTokens>\
         <httpPutResponseHopLimit>{}</httpPutResponseHopLimit><httpEndpoint>{}</httpEndpoint>\
         <httpProtocolIpv6>{}</httpProtocolIpv6><instanceMetadataTags>{}</instanceMetadataTags></instanceMetadataOptions>",
        ec2_elem("instanceId", &id),
        opts.http_tokens,
        opts.http_put_response_hop_limit,
        opts.http_endpoint,
        opts.http_protocol_ipv6,
        opts.instance_metadata_tags,
    );
    Ok(Ec2Service::respond(
        "ModifyInstanceMetadataOptions",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn modify_instance_maintenance_options(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceId")?;
    validate_enum(&req.query_params, "AutoRecovery", &["disabled", "default"])?;
    validate_enum(
        &req.query_params,
        "RebootMigration",
        &["disabled", "default"],
    )?;
    let p = req.query_params.clone();
    let opts = with_instance_mut(svc, &req.account_id, &id, |inst| {
        let m = &mut inst.maintenance_options;
        if let Some(v) = p.get("AutoRecovery").filter(|v| !v.is_empty()) {
            m.auto_recovery = v.clone();
        }
        if let Some(v) = p.get("RebootMigration").filter(|v| !v.is_empty()) {
            m.reboot_migration = v.clone();
        }
        m.clone()
    })?;
    let body = format!(
        "{}<maintenanceOptions><autoRecovery>{}</autoRecovery><rebootMigration>{}</rebootMigration></maintenanceOptions>",
        ec2_elem("instanceId", &id),
        opts.auto_recovery,
        opts.reboot_migration,
    );
    Ok(Ec2Service::respond(
        "ModifyInstanceMaintenanceOptions",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn modify_instance_cpu_options(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceId")?;
    validate_enum(
        &req.query_params,
        "NestedVirtualization",
        &["disabled", "enabled"],
    )?;
    let core_count = req
        .query_params
        .get("CoreCount")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(2);
    let threads_per_core = req
        .query_params
        .get("ThreadsPerCore")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or(1);
    with_instance_mut(svc, &req.account_id, &id, |inst| {
        inst.cpu_options = Some(crate::state::CpuOptions {
            core_count,
            threads_per_core,
        });
    })?;
    let body = format!(
        "{}<coreCount>{core_count}</coreCount><threadsPerCore>{threads_per_core}</threadsPerCore>",
        ec2_elem("instanceId", &id)
    );
    Ok(Ec2Service::respond(
        "ModifyInstanceCpuOptions",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn modify_instance_network_performance_options(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceId")?;
    let weighting = require(&req.query_params, "BandwidthWeighting")?;
    validate_enum(
        &req.query_params,
        "BandwidthWeighting",
        &["default", "vpc-1", "ebs-1"],
    )?;
    with_instance_mut(svc, &req.account_id, &id, |inst| {
        inst.bandwidth_weighting = Some(weighting.clone());
    })?;
    let body = format!(
        "{}<bandwidthWeighting>{weighting}</bandwidthWeighting>",
        ec2_elem("instanceId", &id)
    );
    Ok(Ec2Service::respond(
        "ModifyInstanceNetworkPerformanceOptions",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn modify_instance_event_start_time(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    let event_id = require(&req.query_params, "InstanceEventId")?;
    require(&req.query_params, "NotBefore")?;
    let body = format!(
        "<event>{}<code>system-reboot</code><description>scheduled</description></event>",
        ec2_elem("instanceEventId", &event_id)
    );
    Ok(Ec2Service::respond(
        "ModifyInstanceEventStartTime",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn describe_instance_credit_specifications(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    crate::service_helpers::validate_max_results(&req.query_params, 5, 1000)?;
    let wanted = indexed_list(&req.query_params, "InstanceId");
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let items: Vec<String> = state
        .instances
        .values()
        .filter(|i| wanted.is_empty() || wanted.contains(&i.instance_id))
        .map(|i| {
            format!(
                "{}<cpuCredits>standard</cpuCredits>",
                ec2_elem("instanceId", &i.instance_id)
            )
        })
        .collect();
    Ok(Ec2Service::respond(
        "DescribeInstanceCreditSpecifications",
        &req.request_id,
        &ec2_list("instanceCreditSpecificationSet", &items),
    ))
}

pub(crate) fn modify_instance_credit_specification(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let body = format!(
        "{}{}",
        ec2_list("successfulInstanceCreditSpecificationSet", &[]),
        ec2_list("unsuccessfulInstanceCreditSpecificationSet", &[]),
    );
    Ok(Ec2Service::respond(
        "ModifyInstanceCreditSpecification",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn get_instance_metadata_defaults(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "GetInstanceMetadataDefaults",
        &req.request_id,
        "<accountLevel><httpTokens>optional</httpTokens><httpEndpoint>enabled</httpEndpoint></accountLevel>",
    ))
}

pub(crate) fn modify_instance_metadata_defaults(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    validate_enum(
        &req.query_params,
        "HttpTokens",
        &["optional", "required", "no-preference"],
    )?;
    validate_enum(
        &req.query_params,
        "HttpEndpoint",
        &["disabled", "enabled", "no-preference"],
    )?;
    validate_enum(
        &req.query_params,
        "InstanceMetadataTags",
        &["disabled", "enabled", "no-preference"],
    )?;
    validate_enum(
        &req.query_params,
        "HttpTokensEnforced",
        &["disabled", "enabled", "no-preference"],
    )?;
    Ok(Ec2Service::respond(
        "ModifyInstanceMetadataDefaults",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn register_event_notification_attributes(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    // InstanceTagAttribute is a required struct with no required members, so an
    // empty one is wire-invisible (== omission) — not validated here.
    Ok(Ec2Service::respond(
        "RegisterInstanceEventNotificationAttributes",
        &req.request_id,
        &event_tag_attribute(),
    ))
}

pub(crate) fn deregister_event_notification_attributes(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "DeregisterInstanceEventNotificationAttributes",
        &req.request_id,
        &event_tag_attribute(),
    ))
}

pub(crate) fn describe_event_notification_attributes(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    Ok(Ec2Service::respond(
        "DescribeInstanceEventNotificationAttributes",
        &req.request_id,
        &event_tag_attribute(),
    ))
}

fn event_tag_attribute() -> String {
    format!(
        "<instanceTagAttribute><includeAllTagsOfInstance>false</includeAllTagsOfInstance>{}</instanceTagAttribute>",
        ec2_list("instanceTagKeySet", &[])
    )
}

pub(crate) fn report_instance_status(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "Status")?;
    validate_enum(&req.query_params, "Status", &["ok", "impaired"])?;
    Ok(Ec2Service::respond(
        "ReportInstanceStatus",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn describe_instance_topology(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    crate::service_helpers::validate_max_results(&req.query_params, 1, 100)?;
    Ok(Ec2Service::respond(
        "DescribeInstanceTopology",
        &req.request_id,
        &ec2_list("instanceSet", &[]),
    ))
}
