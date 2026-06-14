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
    format!(
        "{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
        ec2_elem("instanceId", &i.instance_id),
        ec2_elem("imageId", &i.image_id),
        state_xml("instanceState", i.state_code, &i.state_name),
        ec2_elem("privateDnsName", &format!("ip-{}.ec2.internal", i.private_ip.replace('.', "-"))),
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
        format_args!("<ebsOptimized>false</ebsOptimized><sourceDestCheck>true</sourceDestCheck>"),
        format_args!(
            "<placement><availabilityZone>{}</availabilityZone><tenancy>default</tenancy></placement>",
            i.az
        ),
        format_args!("<monitoring><state>{}</state></monitoring>", if i.monitoring { "enabled" } else { "disabled" }),
        format_args!(
            "{}{}",
            i.subnet_id.as_ref().map(|s| ec2_elem("subnetId", s)).unwrap_or_default(),
            i.vpc_id.as_ref().map(|s| ec2_elem("vpcId", s)).unwrap_or_default(),
        ),
        i.key_name.as_ref().map(|k| ec2_elem("keyName", k)).unwrap_or_default(),
        format_args!("{}{}", ec2_list("groupSet", &groups), ec2_elem("ownerId", owner)),
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
    require(&req.query_params, "MaxCount")?;
    validate_enum_instance_type(req)?;
    validate_enum(
        &req.query_params,
        "InstanceInitiatedShutdownBehavior",
        &["stop", "terminate"],
    )?;
    let count = min.clamp(1, 64);
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
    let user_data = req.query_params.get("UserData").map(String::as_str);
    let owner = req.account_id.clone();
    let az = format!(
        "{}a",
        if req.region.is_empty() {
            "us-east-1"
        } else {
            &req.region
        }
    );

    // Generate instance ids first, then boot a backing container per id
    // (without holding the state lock across the awaits). When no runtime is
    // configured, or a container fails to start, the instance falls back to a
    // synthesized private IP and no container handle — the API still succeeds.
    let ids: Vec<String> = (0..count).map(|_| gen_id("i")).collect();
    let mut backing: HashMap<String, crate::runtime::RunningInstance> = HashMap::new();
    if let Some(rt) = &svc.runtime {
        for id in &ids {
            match rt.run_instance(id, user_data).await {
                Ok(running) => {
                    backing.insert(id.clone(), running);
                }
                Err(e) => {
                    tracing::warn!(instance_id = %id, error = %e, "EC2 instance container failed to start; serving metadata-only");
                }
            }
        }
    }

    let mut rendered = Vec::new();
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for (idx, id) in ids.iter().enumerate() {
            let running = backing.remove(id);
            let private_ip = running
                .as_ref()
                .map(|r| r.private_ip.clone())
                .unwrap_or_else(|| format!("10.0.0.{}", 10 + idx));
            let inst = Instance {
                instance_id: id.clone(),
                image_id: image_id.clone(),
                instance_type: instance_type.clone(),
                state_code: 16,
                state_name: "running".to_string(),
                private_ip,
                public_ip: Some(format!("52.0.0.{}", 10 + idx)),
                subnet_id: subnet_id.clone(),
                vpc_id: None,
                key_name: key_name.clone(),
                security_group_ids: sg_ids.clone(),
                reservation_id: reservation_id.clone(),
                ami_launch_index: idx as i64,
                monitoring: false,
                az: az.clone(),
                launch_time: LAUNCH_TIME.to_string(),
                container_id: running.map(|r| r.container_id),
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
    let body = reservation_xml(&reservation_id, &owner, &rendered);
    Ok(Ec2Service::respond("RunInstances", &req.request_id, &body))
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

async fn change_state(
    svc: &Ec2Service,
    req: &AwsRequest,
    action: &str,
    new_code: i64,
    new_name: &str,
) -> Result<AwsResponse, AwsServiceError> {
    let ids = indexed_list(&req.query_params, "InstanceId");
    let mut changes = Vec::new();
    // Container handles of affected instances, collected under the lock and
    // acted on afterwards so no await happens while the state lock is held.
    let mut affected: Vec<(String, String)> = Vec::new();
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
                inst.state_code = new_code;
                inst.state_name = new_name.to_string();
                if new_code == 80 {
                    inst.public_ip = None;
                }
                if let Some(cid) = &inst.container_id {
                    affected.push((id.clone(), cid.clone()));
                }
                // A terminated instance no longer has a backing container.
                if new_code == 48 {
                    inst.container_id = None;
                }
            }
            changes.push(format!(
                "{}{}{}",
                ec2_elem("instanceId", id),
                state_xml("currentState", new_code, new_name),
                state_xml("previousState", prev_code, &prev_name),
            ));
        }
    }

    // Map the state transition onto the backing container's lifecycle.
    if let Some(rt) = &svc.runtime {
        for (id, cid) in &affected {
            match new_code {
                16 => {
                    // StartInstances: the container may come back with a new
                    // address; reflect it in describe output.
                    if let Some(new_ip) = rt.start_instance(cid).await {
                        let mut accounts = svc.state.write();
                        let state = accounts.get_or_create(&req.account_id);
                        if let Some(inst) = state.instances.get_mut(id) {
                            inst.private_ip = new_ip;
                        }
                    }
                }
                80 => rt.stop_instance(cid).await,
                48 => rt.terminate_instance(cid).await,
                _ => {}
            }
        }
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
    let container_ids: Vec<String> = {
        let accounts = svc.state.read();
        match accounts.get(&req.account_id) {
            Some(state) => ids
                .iter()
                .filter_map(|id| state.instances.get(id))
                .filter_map(|i| i.container_id.clone())
                .collect(),
            None => Vec::new(),
        }
    };
    if let Some(rt) = &svc.runtime {
        for cid in &container_ids {
            rt.reboot_instance(cid).await;
        }
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
    let filters = parse_filters(&req.query_params);
    let wanted = indexed_list(&req.query_params, "InstanceId");
    let owner = req.account_id.clone();
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);

    // Group matching instances by reservation.
    let mut by_res: HashMap<String, Vec<String>> = HashMap::new();
    let mut order: Vec<String> = Vec::new();
    for i in state.instances.values() {
        if !wanted.is_empty() && !wanted.contains(&i.instance_id) {
            continue;
        }
        if !inst_match(i, state.tags_for(&i.instance_id), &filters) {
            continue;
        }
        if !by_res.contains_key(&i.reservation_id) {
            order.push(i.reservation_id.clone());
        }
        by_res
            .entry(i.reservation_id.clone())
            .or_default()
            .push(instance_xml(i, state.tags_for(&i.instance_id), &owner));
    }
    order.sort();
    let reservations: Vec<String> = order
        .iter()
        .map(|rid| {
            let mut insts = by_res.remove(rid).unwrap_or_default();
            insts.sort();
            reservation_xml(rid, &owner, &insts)
        })
        .collect();
    Ok(Ec2Service::respond(
        "DescribeInstances",
        &req.request_id,
        &ec2_list("reservationSet", &reservations),
    ))
}

fn inst_match(i: &Instance, tags: &[Tag], filters: &[Filter]) -> bool {
    filters.iter().all(|f| {
        let candidates: Vec<String> = match f.name.as_str() {
            "instance-id" => vec![i.instance_id.clone()],
            "instance-type" => vec![i.instance_type.clone()],
            "image-id" => vec![i.image_id.clone()],
            "instance-state-name" => vec![i.state_name.clone()],
            "instance-state-code" => vec![i.state_code.to_string()],
            "vpc-id" => i.vpc_id.clone().into_iter().collect(),
            "subnet-id" => i.subnet_id.clone().into_iter().collect(),
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

pub(crate) fn describe_instance_status(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let wanted = indexed_list(&req.query_params, "InstanceId");
    let include_all = req
        .query_params
        .get("IncludeAllInstances")
        .map(|v| v == "true")
        .unwrap_or(false);
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    let mut items: Vec<String> = state
        .instances
        .values()
        .filter(|i| wanted.is_empty() || wanted.contains(&i.instance_id))
        .filter(|i| include_all || i.state_name == "running")
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
    items.sort();
    Ok(Ec2Service::respond(
        "DescribeInstanceStatus",
        &req.request_id,
        &ec2_list("instanceStatusSet", &items),
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
    let it = {
        let accounts = svc.state.read();
        accounts
            .get(&req.account_id)
            .and_then(|s| s.instances.get(&id).map(|i| i.instance_type.clone()))
            .unwrap_or_else(|| "t3.micro".to_string())
    };
    let attr_xml = match attribute.as_str() {
        "instanceType" => format!("<instanceType><value>{it}</value></instanceType>"),
        "disableApiTermination" => "<disableApiTermination><value>false</value></disableApiTermination>".to_string(),
        "disableApiStop" => "<disableApiStop><value>false</value></disableApiStop>".to_string(),
        "ebsOptimized" => "<ebsOptimized><value>false</value></ebsOptimized>".to_string(),
        "sourceDestCheck" => "<sourceDestCheck><value>true</value></sourceDestCheck>".to_string(),
        "instanceInitiatedShutdownBehavior" => "<instanceInitiatedShutdownBehavior><value>stop</value></instanceInitiatedShutdownBehavior>".to_string(),
        "userData" => "<userData/>".to_string(),
        "groupSet" => ec2_list("groupSet", &[]),
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

pub(crate) fn modify_instance_attribute(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    validate_enum(&req.query_params, "Attribute", ATTRIBUTE_VALUES)?;
    Ok(Ec2Service::respond(
        "ModifyInstanceAttribute",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn reset_instance_attribute(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    require(&req.query_params, "Attribute")?;
    validate_enum(&req.query_params, "Attribute", ATTRIBUTE_VALUES)?;
    Ok(Ec2Service::respond(
        "ResetInstanceAttribute",
        &req.request_id,
        &ec2_return(true),
    ))
}

// ---- modify-* and misc ----

pub(crate) fn modify_instance_placement(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    require(&req.query_params, "InstanceId")?;
    validate_enum(
        &req.query_params,
        "Tenancy",
        &["default", "dedicated", "host"],
    )?;
    validate_enum(&req.query_params, "Affinity", &["default", "host"])?;
    Ok(Ec2Service::respond(
        "ModifyInstancePlacement",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn modify_instance_metadata_options(
    _svc: &Ec2Service,
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
    let body = format!(
        "{}<instanceMetadataOptions><state>applied</state><httpTokens>optional</httpTokens>\
         <httpPutResponseHopLimit>1</httpPutResponseHopLimit><httpEndpoint>enabled</httpEndpoint></instanceMetadataOptions>",
        ec2_elem("instanceId", &id)
    );
    Ok(Ec2Service::respond(
        "ModifyInstanceMetadataOptions",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn modify_instance_maintenance_options(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceId")?;
    validate_enum(&req.query_params, "AutoRecovery", &["disabled", "default"])?;
    validate_enum(
        &req.query_params,
        "RebootMigration",
        &["disabled", "default"],
    )?;
    Ok(Ec2Service::respond(
        "ModifyInstanceMaintenanceOptions",
        &req.request_id,
        &ec2_elem("instanceId", &id),
    ))
}

pub(crate) fn modify_instance_cpu_options(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceId")?;
    validate_enum(
        &req.query_params,
        "NestedVirtualization",
        &["disabled", "enabled"],
    )?;
    let body = format!(
        "{}<coreCount>2</coreCount><threadsPerCore>1</threadsPerCore>",
        ec2_elem("instanceId", &id)
    );
    Ok(Ec2Service::respond(
        "ModifyInstanceCpuOptions",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn modify_instance_network_performance_options(
    _svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let id = require(&req.query_params, "InstanceId")?;
    require(&req.query_params, "BandwidthWeighting")?;
    validate_enum(
        &req.query_params,
        "BandwidthWeighting",
        &["default", "vpc-1", "ebs-1"],
    )?;
    let body = format!(
        "{}<bandwidthWeighting>default</bandwidthWeighting>",
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
