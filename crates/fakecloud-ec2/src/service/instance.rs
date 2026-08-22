//! EC2 instance control plane (metadata-faithful). A Docker-backed runtime
//! layers real container execution on top of this in a follow-up; the API
//! surface and conformance live here.

use std::collections::HashMap;

use fakecloud_aws::ec2query::{ec2_elem, ec2_list, ec2_return};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::Ec2Service;
use crate::service_helpers::{
    gen_id, indexed_list, instance_limit_exceeded, invalid_parameter_value, missing_parameter,
    parse_filters, require, require_struct, validate_enum, Filter,
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

/// First three octets of a subnet CIDR's network address, e.g.
/// `172.31.16.0/20 -> "172.31.16"`, so a synthesized private IP lands inside
/// the subnet. Falls back to `10.0.0` for a non-IPv4-CIDR input.
fn subnet_ip_prefix(cidr: &str) -> String {
    let addr = cidr.split('/').next().unwrap_or(cidr);
    let octets: Vec<&str> = addr.split('.').collect();
    if octets.len() == 4 && octets.iter().all(|o| o.parse::<u8>().is_ok()) {
        format!("{}.{}.{}", octets[0], octets[1], octets[2])
    } else {
        "10.0.0".to_string()
    }
}

/// Map of `security-group-id -> group-name` for the whole state, so
/// DescribeInstances can render the real `groupName` (AWS returns the name, not
/// the id) instead of echoing the id back.
fn sg_name_map(state: &Ec2State) -> HashMap<String, String> {
    state
        .security_groups
        .values()
        .map(|g| (g.group_id.clone(), g.group_name.clone()))
        .collect()
}

/// Resolve an instance's CPU architecture from its AMI in the seeded/owned
/// catalogue (arm64 for Graviton images), defaulting to x86_64 when the AMI
/// isn't known.
fn arch_for(state: &Ec2State, image_id: &str) -> String {
    state
        .images
        .get(image_id)
        .map(|img| img.architecture.clone())
        .unwrap_or_else(|| "x86_64".to_string())
}

/// Resolve an instance's `platformDetails` from its AMI in the seeded/owned
/// catalogue (`Windows` for Windows images), defaulting to `Linux/UNIX` when
/// the AMI isn't known — the same contract as [`image_xml`]'s platform fields.
fn platform_for(state: &Ec2State, image_id: &str) -> String {
    let raw = state
        .images
        .get(image_id)
        .and_then(|img| img.platform.as_deref());
    super::image::platform_details_label(raw)
}

fn instance_xml(
    i: &Instance,
    tags: &[Tag],
    owner: &str,
    sg_names: &HashMap<String, String>,
    architecture: &str,
    platform_details: &str,
) -> String {
    let groups: Vec<String> = i
        .security_group_ids
        .iter()
        .map(|g| {
            let name = sg_names.get(g).map(String::as_str).unwrap_or(g.as_str());
            format!("{}{}", ec2_elem("groupId", g), ec2_elem("groupName", name))
        })
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
    let private_dns_name_options = format!(
        "<privateDnsNameOptions><hostnameType>{}</hostnameType>\
         <enableResourceNameDnsARecord>{}</enableResourceNameDnsARecord>\
         <enableResourceNameDnsAAAARecord>{}</enableResourceNameDnsAAAARecord></privateDnsNameOptions>",
        i.private_dns_hostname_type.as_deref().unwrap_or("ip-name"),
        i.enable_resource_name_dns_a_record,
        i.enable_resource_name_dns_aaaa_record,
    );
    // Windows instances additionally report `<platform>windows</platform>` and
    // a Windows usage operation, mirroring `image_xml`.
    let platform_xml = if platform_details.eq_ignore_ascii_case("windows") {
        format!(
            "{}{}{}",
            ec2_elem("platform", "windows"),
            ec2_elem("platformDetails", platform_details),
            ec2_elem("usageOperation", "RunInstances:0002"),
        )
    } else {
        format!(
            "{}{}",
            ec2_elem("platformDetails", platform_details),
            ec2_elem("usageOperation", "RunInstances"),
        )
    };
    let tenancy = i.placement_tenancy.as_deref().unwrap_or("default");
    let placement_group = i
        .placement_group_name
        .as_ref()
        .map(|g| ec2_elem("groupName", g))
        .unwrap_or_default();
    // `placement_affinity` was written by ModifyInstancePlacement/RunInstances
    // but never rendered here (a dead write) -> the value never reflected on
    // DescribeInstances. Emit it when set.
    let affinity = i
        .placement_affinity
        .as_ref()
        .map(|a| ec2_elem("affinity", a))
        .unwrap_or_default();
    format!(
        "{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}{}",
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
        ec2_elem("architecture", architecture),
        platform_xml,
        ec2_elem("rootDeviceType", "ebs"),
        ec2_elem("rootDeviceName", "/dev/xvda"),
        ec2_elem("virtualizationType", "hvm"),
        ec2_elem("hypervisor", "xen"),
        format_args!(
            "<ebsOptimized>{}</ebsOptimized><sourceDestCheck>{}</sourceDestCheck>",
            i.ebs_optimized, i.source_dest_check
        ),
        format_args!(
            "<placement><availabilityZone>{}</availabilityZone>{}{}<tenancy>{}</tenancy></placement>",
            i.az, affinity, placement_group, tenancy
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
        private_dns_name_options,
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

/// Launch-time instance options that `RunInstances` must persist. These were
/// previously hardcoded to defaults in the instance record even when the
/// request specified them (only the `Modify*` handlers set the struct fields),
/// so `aws_instance` / a launch template read them back as defaults and drifted
/// -- and they are ForceNew, so the drift never self-healed. Parsed once from
/// the flattened request params.
#[derive(Default)]
struct LaunchOpts {
    cpu_options: Option<crate::state::CpuOptions>,
    placement_tenancy: Option<String>,
    placement_affinity: Option<String>,
    private_dns_hostname_type: Option<String>,
    enable_a_record: bool,
    enable_aaaa_record: bool,
}

fn parse_launch_opts(params: &HashMap<String, String>) -> LaunchOpts {
    let cc = params
        .get("CpuOptions.CoreCount")
        .and_then(|v| v.parse::<i64>().ok());
    let tpc = params
        .get("CpuOptions.ThreadsPerCore")
        .and_then(|v| v.parse::<i64>().ok());
    LaunchOpts {
        cpu_options: (cc.is_some() || tpc.is_some()).then(|| crate::state::CpuOptions {
            core_count: cc.unwrap_or(1),
            threads_per_core: tpc.unwrap_or(1),
        }),
        placement_tenancy: params.get("Placement.Tenancy").cloned(),
        placement_affinity: params.get("Placement.Affinity").cloned(),
        private_dns_hostname_type: params.get("PrivateDnsNameOptions.HostnameType").cloned(),
        enable_a_record: params
            .get("PrivateDnsNameOptions.EnableResourceNameDnsARecord")
            .is_some_and(|v| v == "true"),
        enable_aaaa_record: params
            .get("PrivateDnsNameOptions.EnableResourceNameDnsAAAARecord")
            .is_some_and(|v| v == "true"),
    }
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
    // Reject requests for more instances than this fake will launch. Real AWS
    // returns `InstanceLimitExceeded` once MaxCount exceeds the per-request /
    // account ceiling; we apply the same error rather than silently clamping
    // (which would launch fewer than MinCount and panic for min > ceiling).
    const MAX_INSTANCES_PER_REQUEST: usize = 64;
    if min > MAX_INSTANCES_PER_REQUEST {
        return Err(instance_limit_exceeded(format!(
            "You have requested more instances ({min}) than your current instance limit of {MAX_INSTANCES_PER_REQUEST} allows for this launch."
        )));
    }
    validate_enum_instance_type(req)?;
    validate_enum(
        &req.query_params,
        "InstanceInitiatedShutdownBehavior",
        &["stop", "terminate"],
    )?;
    // AWS best-effort launches MaxCount instances (>= MinCount). `min` is
    // already validated to be in 1..=MAX_INSTANCES_PER_REQUEST above, so this
    // only caps an oversized MaxCount down to the ceiling (never below MinCount,
    // and never with `lo > hi`, which would panic).
    let count = max.min(MAX_INSTANCES_PER_REQUEST).max(min);
    let reservation_id = gen_id("r");
    // ImageId is required unless a launch template (which can carry the image)
    // is referenced. AWS returns MissingParameter rather than silently
    // launching from a placeholder AMI.
    let uses_launch_template = req.query_params.keys().any(|k| {
        k.starts_with("LaunchTemplate.LaunchTemplateId")
            || k.starts_with("LaunchTemplate.LaunchTemplateName")
    });
    let image_id = match req.query_params.get("ImageId").filter(|v| !v.is_empty()) {
        Some(v) => v.clone(),
        None if uses_launch_template => "ami-00000000000000000".to_string(),
        None => return Err(missing_parameter("ImageId")),
    };
    let instance_type = req
        .query_params
        .get("InstanceType")
        .cloned()
        .unwrap_or_else(|| "t3.micro".to_string());
    let key_name = req.query_params.get("KeyName").cloned();
    let mut subnet_id = req.query_params.get("SubnetId").cloned();
    let mut sg_ids = indexed_list(&req.query_params, "SecurityGroupId");
    let user_data = req.query_params.get("UserData").cloned();
    let owner = req.account_id.clone();
    // Honor launch-time `Monitoring.Enabled`, `EbsOptimized`, and
    // `MetadataOptions.*` so DescribeInstances reflects them, matching CFN's
    // `AWS::EC2::Instance` provisioner (which shares the same instance model).
    let monitoring = req
        .query_params
        .get("Monitoring.Enabled")
        .map(|v| v == "true")
        .unwrap_or(false);
    let ebs_optimized = req
        .query_params
        .get("EbsOptimized")
        .map(|v| v == "true")
        .unwrap_or(false);
    let metadata_options = parse_metadata_options(&req.query_params);
    // `IamInstanceProfile.Arn` / `.Name` (either half is accepted; synthesize
    // the ARN from the name when only the name is given).
    let iam_profile_arn = req
        .query_params
        .get("IamInstanceProfile.Arn")
        .cloned()
        .or_else(|| {
            req.query_params
                .get("IamInstanceProfile.Name")
                .map(|n| format!("arn:aws:iam::{}:instance-profile/{n}", req.account_id))
        });
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
    let (vpc_id, subnet_auto_public, instance_network, ip_prefix) = {
        let accounts = svc.state.read();
        let empty = Ec2State::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        // No explicit subnet: land in the default VPC's default subnet for the
        // target AZ (falling back to any default subnet), exactly as AWS does.
        // This fills `subnet_id`/`vpc_id` so DescribeInstances reports a real
        // subnet/VPC and phase-2 per-subnet networking has something to key on.
        if subnet_id.is_none() {
            if let Some(s) = state
                .subnets
                .values()
                .filter(|s| s.default_for_az)
                .find(|s| s.availability_zone == az)
                .or_else(|| state.subnets.values().find(|s| s.default_for_az))
            {
                subnet_id = Some(s.subnet_id.clone());
            }
        }
        // When the caller named no security group, AWS attaches the VPC's
        // `default` group. Resolve it from the subnet's VPC (or the default VPC).
        let resolved_vpc = subnet_id
            .as_ref()
            .and_then(|sid| state.subnets.get(sid))
            .map(|s| s.vpc_id.clone());
        // Honor `SecurityGroup.N` (group *names*, EC2-Classic/default-VPC form)
        // by resolving each to its id, alongside the modern `SecurityGroupId.N`.
        let sg_names = indexed_list(&req.query_params, "SecurityGroup");
        if !sg_names.is_empty() {
            let target_vpc = resolved_vpc
                .clone()
                .unwrap_or_else(|| crate::defaults::default_vpc_id(&req.account_id));
            for name in &sg_names {
                if let Some(sg) = state
                    .security_groups
                    .values()
                    .find(|g| g.vpc_id == target_vpc && &g.group_name == name)
                {
                    if !sg_ids.contains(&sg.group_id) {
                        sg_ids.push(sg.group_id.clone());
                    }
                }
            }
        }
        if sg_ids.is_empty() {
            let vpc = resolved_vpc
                .clone()
                .unwrap_or_else(|| crate::defaults::default_vpc_id(&req.account_id));
            if let Some(sg) = state
                .security_groups
                .values()
                .find(|g| g.vpc_id == vpc && g.group_name == "default")
            {
                sg_ids = vec![sg.group_id.clone()];
            }
        }
        // The backing per-subnet network for phase-2 L3 isolation. A subnet
        // with no `0.0.0.0/0 -> igw` route is private -> `internal` network.
        let instance_network = subnet_id
            .as_ref()
            .map(|sid| crate::runtime::InstanceNetwork {
                subnet_id: sid.clone(),
                internal: !crate::defaults::subnet_is_public(state, sid),
            });
        let (vpc, auto_public) = match subnet_id.as_ref() {
            Some(sid) => state
                .subnets
                .get(sid)
                .map(|s| {
                    (
                        Some(s.vpc_id.clone()),
                        s.map_public_ip_on_launch || s.default_for_az,
                    )
                })
                .unwrap_or((None, false)),
            // No subnet (and no default subnet found): still a default-VPC
            // launch, which assigns public IPs by default.
            None => (Some(crate::defaults::default_vpc_id(&req.account_id)), true),
        };
        // Metadata-only private IP base, derived from the resolved subnet's
        // CIDR so DescribeInstances reports an IP inside the subnet (was a
        // hard-coded 10.0.0.x outside the subnet — bug-hunt finding 1.7). A
        // real container-backed instance overwrites this with its true bridge
        // IP once running.
        let ip_prefix = subnet_id
            .as_ref()
            .and_then(|sid| state.subnets.get(sid))
            .map(|s| subnet_ip_prefix(&s.cidr_block))
            .unwrap_or_else(|| "10.0.0".to_string());
        (vpc, auto_public, instance_network, ip_prefix)
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
        let sg_names = sg_name_map(state);
        let launch_opts = parse_launch_opts(&req.query_params);
        for (idx, id) in ids.iter().enumerate() {
            let inst = Instance {
                instance_id: id.clone(),
                image_id: image_id.clone(),
                instance_type: instance_type.clone(),
                state_code: 0,
                state_name: "pending".to_string(),
                private_ip: format!("{ip_prefix}.{}", 10 + idx),
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
                monitoring,
                az: az.clone(),
                launch_time: LAUNCH_TIME.to_string(),
                container_id: None,
                disable_api_termination: false,
                disable_api_stop: false,
                source_dest_check: true,
                ebs_optimized,
                instance_initiated_shutdown_behavior: req
                    .query_params
                    .get("InstanceInitiatedShutdownBehavior")
                    .cloned()
                    .unwrap_or_else(|| "stop".to_string()),
                user_data: user_data.clone().filter(|s| !s.is_empty()),
                metadata_options: metadata_options.clone(),
                // Launch-time CpuOptions / Placement.Tenancy+Affinity /
                // PrivateDnsNameOptions were dropped at RunInstances (only the
                // Modify* handlers set them), so a launch template / aws_instance
                // that specified them at creation read back defaults and drifted
                // -- and they are ForceNew, so the drift never self-healed.
                cpu_options: launch_opts.cpu_options.clone(),
                bandwidth_weighting: None,
                maintenance_options: crate::state::MaintenanceOptions::default(),
                placement_tenancy: launch_opts.placement_tenancy.clone(),
                placement_affinity: launch_opts.placement_affinity.clone(),
                placement_group_name: req.query_params.get("Placement.GroupName").cloned(),
                private_dns_hostname_type: launch_opts.private_dns_hostname_type.clone(),
                enable_resource_name_dns_a_record: launch_opts.enable_a_record,
                enable_resource_name_dns_aaaa_record: launch_opts.enable_aaaa_record,
            };
            crate::service::tags::apply_tag_specifications(
                state,
                &req.query_params,
                id,
                "instance",
            );
            let tags = state.tags_for(id).to_vec();
            let architecture = arch_for(state, &inst.image_id);
            let platform_details = platform_for(state, &inst.image_id);
            rendered.push(instance_xml(
                &inst,
                &tags,
                &owner,
                &sg_names,
                &architecture,
                &platform_details,
            ));
            state.instances.insert(id.clone(), inst);

            // Record an IAM instance-profile association when the request
            // supplies `IamInstanceProfile`, matching a direct
            // AssociateIamInstanceProfile so
            // DescribeIamInstanceProfileAssociations reflects it.
            if let Some(profile_arn) = iam_profile_arn.clone() {
                let assoc = crate::state::IamInstanceProfileAssociation {
                    association_id: gen_id("iip-assoc"),
                    instance_id: id.clone(),
                    iam_instance_profile_arn: profile_arn,
                    iam_instance_profile_id: gen_id("AIPA"),
                    state: "associated".to_string(),
                };
                state
                    .iam_instance_profile_associations
                    .insert(assoc.association_id.clone(), assoc);
            }
        }
    }

    // Background boot: bring up each backing container and reconcile state.
    {
        let svc_state = svc.state.clone();
        let runtime = svc.runtime.clone();
        let account_id = req.account_id.clone();
        let ids = ids.clone();
        let instance_network = instance_network.clone();
        // Capture the persistence hook so the pending->running flip is written
        // through to disk; RunInstances' own dispatch-path snapshot ran while
        // the instance was still `pending` (M12).
        let snapshot_hook = svc.snapshot_hook();
        tokio::spawn(async move {
            for id in &ids {
                let running = if let Some(rt) = &runtime {
                    match rt
                        .run_instance(
                            &account_id,
                            id,
                            user_data.as_deref(),
                            &instance_tags,
                            instance_network.as_ref(),
                        )
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
            // Persist the reconciled (`running`) state so a restart restores
            // running instances rather than resurrecting them as pending.
            if let Some(hook) = &snapshot_hook {
                hook().await;
            }
            // All instances are up with their real IPs: (re)apply the
            // security-group firewall so the new instances are filtered
            // (#1745 phase 3). No-op when enforcement is disabled.
            if let Some(rt) = &runtime {
                if rt.network_isolation_enforced() {
                    super::firewall_model::reconcile(&svc_state, rt).await;
                }
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

/// The inputs [`crate::runtime::Ec2Runtime::run_instance`] needs to boot a
/// backing container, reconstituted from a persisted instance: base64
/// user-data, the tag map (carries `fakecloud-k8s/*` Pod scheduling tags), and
/// the subnet network placement.
type RunInstanceInputs = (
    Option<String>,
    std::collections::BTreeMap<String, String>,
    Option<crate::runtime::InstanceNetwork>,
);

/// Gather the inputs needed to boot a fresh backing container for an already
/// persisted instance (`user_data`, the tag map, and the subnet network
/// placement), reading them from the persisted instance metadata. Mirrors the
/// per-instance derivation in
/// [`recover_persisted_containers`](crate::service::Ec2Service::recover_persisted_containers)
/// so a container reconstituted here rejoins the same VPC/subnet (and re-runs
/// the same user-data) as one recovered on restart. Returns `None` if the
/// instance no longer exists.
fn run_instance_inputs(
    state: &crate::state::SharedEc2State,
    account_id: &str,
    id: &str,
) -> Option<RunInstanceInputs> {
    let accounts = state.read();
    let s = accounts.get(account_id)?;
    let inst = s.instances.get(id)?;
    let user_data = inst.user_data.clone();
    // A subnet with no `0.0.0.0/0 -> igw` route is private -> `internal`
    // network, matching RunInstances / recover_persisted_containers.
    let network = inst
        .subnet_id
        .clone()
        .map(|sid| crate::runtime::InstanceNetwork {
            internal: !crate::defaults::subnet_is_public(s, &sid),
            subnet_id: sid,
        });
    let tags = s
        .tags_for(id)
        .iter()
        .map(|t| (t.key.clone(), t.value.clone()))
        .collect();
    Some((user_data, tags, network))
}

/// Build a [`MetadataOptions`](crate::state::MetadataOptions) from launch-time
/// `MetadataOptions.*` request params, defaulting each unset field to the AWS
/// default. Shared by RunInstances (the CFN provisioner parses its own JSON
/// property shape).
fn parse_metadata_options(
    params: &std::collections::HashMap<String, String>,
) -> crate::state::MetadataOptions {
    let default = crate::state::MetadataOptions::default();
    crate::state::MetadataOptions {
        http_tokens: params
            .get("MetadataOptions.HttpTokens")
            .cloned()
            .unwrap_or(default.http_tokens),
        http_endpoint: params
            .get("MetadataOptions.HttpEndpoint")
            .cloned()
            .unwrap_or(default.http_endpoint),
        http_put_response_hop_limit: params
            .get("MetadataOptions.HttpPutResponseHopLimit")
            .and_then(|v| v.parse().ok())
            .unwrap_or(default.http_put_response_hop_limit),
        http_protocol_ipv6: params
            .get("MetadataOptions.HttpProtocolIpv6")
            .cloned()
            .unwrap_or(default.http_protocol_ipv6),
        instance_metadata_tags: params
            .get("MetadataOptions.InstanceMetadataTags")
            .cloned()
            .unwrap_or(default.instance_metadata_tags),
    }
}

/// Inputs for a CloudFormation-driven `AWS::EC2::Instance` launch.
#[derive(Debug, Clone, Default)]
pub struct CfnInstanceSpec {
    pub image_id: Option<String>,
    pub instance_type: Option<String>,
    pub subnet_id: Option<String>,
    pub availability_zone: Option<String>,
    pub security_group_ids: Vec<String>,
    pub key_name: Option<String>,
    pub user_data: Option<String>,
    pub private_ip: Option<String>,
    /// Instance metadata service options (`MetadataOptions`). When present the
    /// created instance reports these in DescribeInstances, matching a direct
    /// RunInstances launch with the same options.
    pub metadata_options: Option<crate::state::MetadataOptions>,
    /// `EbsOptimized` — reflected in DescribeInstances `<ebsOptimized>`.
    pub ebs_optimized: bool,
    /// `Monitoring` — reflected in DescribeInstances `<monitoring><state>`.
    pub monitoring: bool,
    /// `IamInstanceProfile` (arn, name). When present, an
    /// `IamInstanceProfileAssociation` is recorded so
    /// DescribeIamInstanceProfileAssociations reflects it, mirroring a direct
    /// AssociateIamInstanceProfile after launch.
    pub iam_instance_profile_arn: Option<String>,
    pub iam_instance_profile_name: Option<String>,
}

/// The Ref / GetAtt-resolvable attributes of a CFN-launched instance.
#[derive(Debug, Clone)]
pub struct CfnInstanceAttrs {
    pub instance_id: String,
    pub private_ip: String,
    pub public_ip: Option<String>,
    pub availability_zone: String,
}

/// Synchronously insert a control-plane `AWS::EC2::Instance` record (status
/// `pending`) and return its Ref/GetAtt attributes. Mirrors the control-plane
/// half of [`run_instances`] for a single instance so a CFN-provisioned
/// instance resolves `Ref` to a real `i-...` id and `GetAtt`
/// PrivateIp/PublicIp/AvailabilityZone immediately. The backing container is
/// booted afterwards by [`cfn_boot_instance`] (drained off the request path).
pub(crate) fn cfn_create_instance(
    svc: &Ec2Service,
    account_id: &str,
    region: &str,
    spec: &CfnInstanceSpec,
) -> CfnInstanceAttrs {
    let image_id = spec
        .image_id
        .clone()
        .unwrap_or_else(|| "ami-00000000000000000".to_string());
    let instance_type = spec
        .instance_type
        .clone()
        .unwrap_or_else(|| "t3.micro".to_string());
    let region = if region.is_empty() {
        "us-east-1"
    } else {
        region
    };
    let mut subnet_id = spec.subnet_id.clone();
    let mut sg_ids = spec.security_group_ids.clone();

    let (vpc_id, subnet_auto_public, ip_prefix, az) = {
        let accounts = svc.state.read();
        let empty = Ec2State::new(account_id, region);
        let state = accounts.get(account_id).unwrap_or(&empty);
        // Resolve a default subnet when none is given, preferring the requested
        // AZ, exactly like `run_instances`.
        if subnet_id.is_none() {
            let want_az = spec.availability_zone.clone();
            if let Some(s) = state
                .subnets
                .values()
                .filter(|s| s.default_for_az)
                .find(|s| want_az.as_deref().is_none_or(|a| s.availability_zone == a))
                .or_else(|| state.subnets.values().find(|s| s.default_for_az))
            {
                subnet_id = Some(s.subnet_id.clone());
            }
        }
        let resolved_vpc = subnet_id
            .as_ref()
            .and_then(|sid| state.subnets.get(sid))
            .map(|s| s.vpc_id.clone());
        if sg_ids.is_empty() {
            let vpc = resolved_vpc
                .clone()
                .unwrap_or_else(|| crate::defaults::default_vpc_id(account_id));
            if let Some(sg) = state
                .security_groups
                .values()
                .find(|g| g.vpc_id == vpc && g.group_name == "default")
            {
                sg_ids = vec![sg.group_id.clone()];
            }
        }
        let (vpc, auto_public, az) = match subnet_id.as_ref() {
            Some(sid) => state
                .subnets
                .get(sid)
                .map(|s| {
                    (
                        Some(s.vpc_id.clone()),
                        s.map_public_ip_on_launch || s.default_for_az,
                        s.availability_zone.clone(),
                    )
                })
                .unwrap_or((None, false, format!("{region}a"))),
            None => (
                Some(crate::defaults::default_vpc_id(account_id)),
                true,
                format!("{region}a"),
            ),
        };
        let az = spec.availability_zone.clone().unwrap_or(az);
        let ip_prefix = subnet_id
            .as_ref()
            .and_then(|sid| state.subnets.get(sid))
            .map(|s| subnet_ip_prefix(&s.cidr_block))
            .unwrap_or_else(|| "10.0.0".to_string());
        (vpc, auto_public, ip_prefix, az)
    };

    let assign_public = subnet_auto_public;
    let id = gen_id("i");
    let private_ip = spec
        .private_ip
        .clone()
        .unwrap_or_else(|| format!("{ip_prefix}.10"));
    let public_ip = if assign_public {
        Some("52.0.0.10".to_string())
    } else {
        None
    };

    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(account_id);
        let inst = Instance {
            instance_id: id.clone(),
            image_id,
            instance_type,
            state_code: 0,
            state_name: "pending".to_string(),
            private_ip: private_ip.clone(),
            public_ip: public_ip.clone(),
            subnet_id: subnet_id.clone(),
            vpc_id: vpc_id.clone(),
            key_name: spec.key_name.clone(),
            security_group_ids: sg_ids,
            reservation_id: gen_id("r"),
            ami_launch_index: 0,
            monitoring: spec.monitoring,
            az: az.clone(),
            launch_time: LAUNCH_TIME.to_string(),
            container_id: None,
            disable_api_termination: false,
            disable_api_stop: false,
            source_dest_check: true,
            ebs_optimized: spec.ebs_optimized,
            instance_initiated_shutdown_behavior: "stop".to_string(),
            user_data: spec.user_data.clone().filter(|s| !s.is_empty()),
            metadata_options: spec.metadata_options.clone().unwrap_or_default(),
            cpu_options: None,
            bandwidth_weighting: None,
            maintenance_options: crate::state::MaintenanceOptions::default(),
            placement_tenancy: None,
            placement_affinity: None,
            placement_group_name: None,
            private_dns_hostname_type: None,
            enable_resource_name_dns_a_record: false,
            enable_resource_name_dns_aaaa_record: false,
        };
        state.instances.insert(id.clone(), inst);

        // Record an IAM instance-profile association when the template supplies
        // `IamInstanceProfile`, mirroring a direct AssociateIamInstanceProfile
        // so DescribeIamInstanceProfileAssociations reflects it. AWS accepts
        // either Arn or Name; synthesize the missing half so it round-trips.
        if spec.iam_instance_profile_arn.is_some() || spec.iam_instance_profile_name.is_some() {
            let arn = spec.iam_instance_profile_arn.clone().unwrap_or_else(|| {
                let name = spec.iam_instance_profile_name.clone().unwrap_or_default();
                format!("arn:aws:iam::{account_id}:instance-profile/{name}")
            });
            let assoc = crate::state::IamInstanceProfileAssociation {
                association_id: gen_id("iip-assoc"),
                instance_id: id.clone(),
                iam_instance_profile_arn: arn,
                iam_instance_profile_id: gen_id("AIPA"),
                state: "associated".to_string(),
            };
            state
                .iam_instance_profile_associations
                .insert(assoc.association_id.clone(), assoc);
        }
    }

    CfnInstanceAttrs {
        instance_id: id,
        private_ip,
        public_ip,
        availability_zone: az,
    }
}

/// Boot the backing container for a CFN-created instance and reconcile it to
/// `running` (or metadata-only `running` when no runtime is wired). Mirrors the
/// background-boot half of [`run_instances`]. Intended to be `tokio::spawn`ed by
/// the CloudFormation create drain so stack creation never blocks on a cold
/// image pull / Pod readiness.
pub(crate) async fn cfn_boot_instance(svc: &Ec2Service, account_id: &str, id: &str) {
    let (user_data, instance_network) = {
        let accounts = svc.state.read();
        let Some(state) = accounts.get(account_id) else {
            return;
        };
        let Some(inst) = state.instances.get(id) else {
            return;
        };
        let network = inst
            .subnet_id
            .as_ref()
            .map(|sid| crate::runtime::InstanceNetwork {
                subnet_id: sid.clone(),
                internal: !crate::defaults::subnet_is_public(state, sid),
            });
        (inst.user_data.clone(), network)
    };

    let empty_tags = std::collections::BTreeMap::new();
    let running = if let Some(rt) = &svc.runtime {
        match rt
            .run_instance(
                account_id,
                id,
                user_data.as_deref(),
                &empty_tags,
                instance_network.as_ref(),
            )
            .await
        {
            Ok(r) => Some(r),
            Err(e) => {
                tracing::warn!(instance_id = %id, error = %e, "CFN EC2 instance container failed to start; serving metadata-only");
                None
            }
        }
    } else {
        None
    };
    reconcile_started(&svc.state, account_id, id, running);

    if let Some(rt) = &svc.runtime {
        if rt.network_isolation_enforced() {
            super::firewall_model::reconcile(&svc.state, rt).await;
        }
    }
}

/// Terminate a CFN-created instance (reaping its real backing container) when
/// its stack is deleted. Routes through the real `TerminateInstances` handler so
/// the container/Pod is stopped and the firewall re-reconciled, instead of
/// leaking a running EC2 container. No-op if the instance is already gone.
pub(crate) async fn cfn_terminate_instance(
    svc: &Ec2Service,
    account_id: &str,
    region: &str,
    instance_id: &str,
) {
    let mut query_params = std::collections::HashMap::new();
    query_params.insert("InstanceId.1".to_string(), instance_id.to_string());
    let req = AwsRequest {
        service: "ec2".to_string(),
        action: "TerminateInstances".to_string(),
        region: region.to_string(),
        account_id: account_id.to_string(),
        request_id: gen_id("req"),
        headers: http::HeaderMap::new(),
        query_params,
        body: bytes::Bytes::new(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: Vec::new(),
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: true,
        access_key_id: None,
        principal: None,
    };
    let _ = terminate_instances(svc, &req).await;
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

/// Validate a single instance's state transition + stop/termination protection,
/// returning the exact error AWS returns when it is illegal. Shared by the
/// pre-flight read check and the re-check under the write lock so both critical
/// sections enforce identical rules (TOCTOU-safe: a concurrent Terminate/Stop
/// landing between the read and the write must fail this call, not be silently
/// clobbered — bug-hunt 2026-07 finding 4.1).
fn check_transition(inst: &Instance, id: &str, new_code: i64) -> Result<(), AwsServiceError> {
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
            check_transition(inst, id, new_code)?;
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
        // Re-run existence + transition/protection checks against the freshly
        // re-read state INSIDE the write lock before mutating anything. The
        // read-phase check above ran under a different (dropped) lock, so a
        // concurrent Terminate/Stop could have landed in between; without this
        // re-check a StartInstances that passed the read check would overwrite
        // a terminated instance's state code, resurrecting it past the boot
        // task's terminal guard (bug-hunt 2026-07 finding 4.1). AWS applies the
        // whole call atomically, so any id now failing fails the entire call
        // with nothing mutated (the guard is still held, so no partial writes).
        for id in &ids {
            let inst = state
                .instances
                .get(id)
                .ok_or_else(|| crate::service_helpers::instance_not_found(id))?;
            check_transition(inst, id, new_code)?;
        }
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
                            // The runtime holds a backing record for this
                            // instance: reattach/restart the existing container.
                            Some(rt) if rt.is_registered(id) => rt.start_instance(id).await,
                            // No backing record: the instance persisted as
                            // `stopped` and fakecloud restarted, so its runtime
                            // registry entry was never rebuilt (recovery only
                            // reconstitutes running/pending instances). `start_instance`
                            // would return `None` and `reconcile_started` would
                            // flip the instance to `running` with no live
                            // container — a phantom-running instance where every
                            // later container-backed op silently no-ops (the EC2
                            // analogue of the RDS restart-recovery bug). Boot a
                            // fresh container from the persisted metadata instead,
                            // mirroring `recover_persisted_containers`.
                            Some(rt) => match run_instance_inputs(&svc_state, &account_id, id) {
                                Some((user_data, tags, network)) => match rt
                                    .run_instance(
                                        &account_id,
                                        id,
                                        user_data.as_deref(),
                                        &tags,
                                        network.as_ref(),
                                    )
                                    .await
                                {
                                    Ok(r) => Some(r),
                                    Err(e) => {
                                        tracing::warn!(instance_id = %id, error = %e, "EC2 instance container failed to start after restart; serving metadata-only");
                                        None
                                    }
                                },
                                None => None,
                            },
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
            // Lifecycle changes move/remove instances (new IP on start, gone on
            // terminate): re-apply the security-group firewall (#1745 ph3).
            if let Some(rt) = &runtime {
                if rt.network_isolation_enforced() {
                    super::firewall_model::reconcile(&svc_state, rt).await;
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
            // A reboot can change the instance's IP (k8s Pod recreate), which
            // leaves a stale /32 in every peer's security-group rules until an
            // unrelated reconcile fires. Re-apply the firewall now (#1745;
            // bug-hunt 2026-06-18 finding 4.2). No-op when enforcement is off.
            if rt.network_isolation_enforced() {
                super::firewall_model::reconcile(&svc_state, &rt).await;
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
        // A nonexistent instance id is a hard error, not a silent no-op.
        for id in &ids {
            if !state.instances.contains_key(id) {
                return Err(crate::service_helpers::instance_not_found(id));
            }
        }
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

    // An explicitly-listed InstanceId that does not exist is a hard error on
    // AWS (`InvalidInstanceID.NotFound`), not an empty result set.
    for id in &wanted {
        if !state.instances.contains_key(id) {
            return Err(crate::service_helpers::instance_not_found(id));
        }
    }

    // Resolve group id -> name up front so both the `group-name` filter and the
    // response rendering can use it. Build the per-instance block-device
    // mappings (from attached volumes) so `block-device-mapping.*` filters work.
    let sg_names = sg_name_map(state);
    let mut bdm_by_instance: HashMap<String, Vec<&crate::state::VolumeAttachment>> = HashMap::new();
    for vol in state.volumes.values() {
        for att in &vol.attachments {
            bdm_by_instance
                .entry(att.instance_id.clone())
                .or_default()
                .push(att);
        }
    }
    let no_bdm: Vec<&crate::state::VolumeAttachment> = Vec::new();

    // Flatten matching instances into a stable order (by reservation, then id),
    // then paginate over the flat instance list — AWS counts instances, not
    // reservations, against MaxResults.
    let mut matching: Vec<&Instance> = state
        .instances
        .values()
        .filter(|i| wanted.is_empty() || wanted.contains(&i.instance_id))
        .filter(|i| {
            inst_match(
                i,
                state.tags_for(&i.instance_id),
                &filters,
                &arch_for(state, &i.image_id),
                &sg_names,
                bdm_by_instance.get(&i.instance_id).unwrap_or(&no_bdm),
            )
        })
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
            .push(instance_xml(
                i,
                state.tags_for(&i.instance_id),
                &owner,
                &sg_names,
                &arch_for(state, &i.image_id),
                &platform_for(state, &i.image_id),
            ));
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

fn inst_match(
    i: &Instance,
    tags: &[Tag],
    filters: &[Filter],
    architecture: &str,
    sg_names: &HashMap<String, String>,
    block_devices: &[&crate::state::VolumeAttachment],
) -> bool {
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
            "architecture" => vec![architecture.to_string()],
            "launch-time" => vec![i.launch_time.clone()],
            // The instance's security groups. AWS exposes them under `group-id`
            // (default-VPC form), `instance.group-id`, and `group-name`; the
            // primary ENI mirrors them for `network-interface.group-id`.
            "group-id" | "instance.group-id" | "network-interface.group-id" => {
                i.security_group_ids.clone()
            }
            "group-name" | "instance.group-name" => i
                .security_group_ids
                .iter()
                .map(|g| sg_names.get(g).cloned().unwrap_or_else(|| g.clone()))
                .collect(),
            // Public DNS name derived from the public IP (see `instance_xml`).
            "dns-name" => i
                .public_ip
                .as_ref()
                .map(|ip| {
                    vec![format!(
                        "ec2-{}.compute.amazonaws.com",
                        ip.replace('.', "-")
                    )]
                })
                .unwrap_or_default(),
            "private-dns-name" => {
                vec![format!(
                    "ip-{}.ec2.internal",
                    i.private_ip.replace('.', "-")
                )]
            }
            // The primary network interface mirrors the instance's placement.
            "network-interface.subnet-id" => i.subnet_id.clone().into_iter().collect(),
            "network-interface.vpc-id" => i.vpc_id.clone().into_iter().collect(),
            "network-interface.availability-zone" => vec![i.az.clone()],
            "network-interface.addresses.private-ip-address" => vec![i.private_ip.clone()],
            "block-device-mapping.volume-id" => {
                block_devices.iter().map(|a| a.volume_id.clone()).collect()
            }
            "block-device-mapping.device-name" => {
                block_devices.iter().map(|a| a.device.clone()).collect()
            }
            "block-device-mapping.status" => {
                block_devices.iter().map(|a| a.status.clone()).collect()
            }
            "block-device-mapping.delete-on-termination" => block_devices
                .iter()
                .map(|a| a.delete_on_termination.to_string())
                .collect(),
            "tag-key" => tags.iter().map(|t| t.key.clone()).collect(),
            "tag-value" => tags.iter().map(|t| t.value.clone()).collect(),
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
    let sg_names = sg_name_map(state);
    let mut bdm_by_instance: HashMap<String, Vec<&crate::state::VolumeAttachment>> = HashMap::new();
    for vol in state.volumes.values() {
        for att in &vol.attachments {
            bdm_by_instance
                .entry(att.instance_id.clone())
                .or_default()
                .push(att);
        }
    }
    let no_bdm: Vec<&crate::state::VolumeAttachment> = Vec::new();
    let mut matching: Vec<&Instance> = state
        .instances
        .values()
        .filter(|i| wanted.is_empty() || wanted.contains(&i.instance_id))
        .filter(|i| include_all || i.state_name == "running")
        .filter(|i| {
            inst_match(
                i,
                state.tags_for(&i.instance_id),
                &filters,
                &arch_for(state, &i.image_id),
                &sg_names,
                bdm_by_instance.get(&i.instance_id).unwrap_or(&no_bdm),
            )
        })
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
    let acct_state = accounts.get(&req.account_id);
    let inst = acct_state
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
            Some(d) => format!(
                "<userData><value>{}</value></userData>",
                fakecloud_aws::xml::xml_escape(d)
            ),
            None => "<userData/>".to_string(),
        },
        "groupSet" => {
            let sg_names = acct_state.map(sg_name_map).unwrap_or_default();
            let groups: Vec<String> = inst
                .security_group_ids
                .iter()
                .map(|g| {
                    let name = sg_names.get(g).map(String::as_str).unwrap_or(g.as_str());
                    format!("{}{}", ec2_elem("groupId", g), ec2_elem("groupName", name))
                })
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
            // Burstable families (t2/t3/t3a/t4g) default to `unlimited` on AWS
            // except t2 (`standard`); non-burstable have no credit spec. We only
            // track the value once explicitly set, else report `standard`.
            let credits = state
                .instance_credit_specs
                .get(&i.instance_id)
                .cloned()
                .unwrap_or_else(|| "standard".to_string());
            format!(
                "{}{}",
                ec2_elem("instanceId", &i.instance_id),
                ec2_elem("cpuCredits", &credits)
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
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let p = &req.query_params;
    let mut successful = Vec::new();
    let mut unsuccessful = Vec::new();
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let mut n = 1usize;
        loop {
            let id_key = format!("InstanceCreditSpecification.{n}.InstanceId");
            let Some(instance_id) = p.get(&id_key).cloned() else {
                break;
            };
            let credits = p
                .get(&format!("InstanceCreditSpecification.{n}.CpuCredits"))
                .cloned()
                .unwrap_or_else(|| "standard".to_string());
            if state.instances.contains_key(&instance_id) {
                state
                    .instance_credit_specs
                    .insert(instance_id.clone(), credits);
                successful.push(ec2_elem("instanceId", &instance_id));
            } else {
                unsuccessful.push(format!(
                    "{}<error><code>InvalidInstanceID.NotFound</code><message>The instance ID '{instance_id}' does not exist</message></error>",
                    ec2_elem("instanceId", &instance_id)
                ));
            }
            n += 1;
        }
    }
    let body = format!(
        "{}{}",
        ec2_list("successfulInstanceCreditSpecificationSet", &successful),
        ec2_list("unsuccessfulInstanceCreditSpecificationSet", &unsuccessful),
    );
    Ok(Ec2Service::respond(
        "ModifyInstanceCreditSpecification",
        &req.request_id,
        &body,
    ))
}

pub(crate) fn get_instance_metadata_defaults(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = svc.state.read();
    let d = accounts
        .get(&req.account_id)
        .and_then(|s| s.instance_metadata_defaults.clone())
        .unwrap_or_default();
    let mut inner = String::new();
    if let Some(v) = &d.http_tokens {
        inner.push_str(&ec2_elem("httpTokens", v));
    }
    if let Some(v) = &d.http_endpoint {
        inner.push_str(&ec2_elem("httpEndpoint", v));
    }
    if let Some(v) = d.http_put_response_hop_limit {
        inner.push_str(&ec2_elem("httpPutResponseHopLimit", &v.to_string()));
    }
    if let Some(v) = &d.instance_metadata_tags {
        inner.push_str(&ec2_elem("instanceMetadataTags", v));
    }
    if let Some(v) = &d.http_tokens_enforced {
        inner.push_str(&ec2_elem("httpTokensEnforced", v));
    }
    Ok(Ec2Service::respond(
        "GetInstanceMetadataDefaults",
        &req.request_id,
        &format!("<accountLevel>{inner}</accountLevel>"),
    ))
}

pub(crate) fn modify_instance_metadata_defaults(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let p = &req.query_params;
    validate_enum(p, "HttpTokens", &["optional", "required", "no-preference"])?;
    validate_enum(p, "HttpEndpoint", &["disabled", "enabled", "no-preference"])?;
    validate_enum(
        p,
        "InstanceMetadataTags",
        &["disabled", "enabled", "no-preference"],
    )?;
    validate_enum(
        p,
        "HttpTokensEnforced",
        &["disabled", "enabled", "no-preference"],
    )?;
    // `no-preference` resets that setting to the account default (drop it).
    let apply = |cur: &mut Option<String>, key: &str| {
        if let Some(v) = p.get(key) {
            *cur = if v == "no-preference" {
                None
            } else {
                Some(v.clone())
            };
        }
    };
    {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let d = state
            .instance_metadata_defaults
            .get_or_insert_with(Default::default);
        apply(&mut d.http_tokens, "HttpTokens");
        apply(&mut d.http_endpoint, "HttpEndpoint");
        apply(&mut d.instance_metadata_tags, "InstanceMetadataTags");
        apply(&mut d.http_tokens_enforced, "HttpTokensEnforced");
        if let Some(v) = p
            .get("HttpPutResponseHopLimit")
            .and_then(|v| v.parse::<i64>().ok())
        {
            // -1 clears the account default per the EC2 API.
            d.http_put_response_hop_limit = if v < 0 { None } else { Some(v) };
        }
    }
    Ok(Ec2Service::respond(
        "ModifyInstanceMetadataDefaults",
        &req.request_id,
        &ec2_return(true),
    ))
}

pub(crate) fn register_event_notification_attributes(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let keys = indexed_sub_keys(&req.query_params);
    let include_all = req
        .query_params
        .get("InstanceTagAttribute.IncludeAllTagsOfInstance")
        .map(|v| v == "true");
    let xml = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        for k in keys {
            if !state.event_notification_tag_keys.contains(&k) {
                state.event_notification_tag_keys.push(k);
            }
        }
        if let Some(v) = include_all {
            state.event_notification_include_all_tags = v;
        }
        event_tag_attribute(state)
    };
    Ok(Ec2Service::respond(
        "RegisterInstanceEventNotificationAttributes",
        &req.request_id,
        &xml,
    ))
}

pub(crate) fn deregister_event_notification_attributes(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let keys = indexed_sub_keys(&req.query_params);
    let include_all = req
        .query_params
        .get("InstanceTagAttribute.IncludeAllTagsOfInstance")
        .map(|v| v == "true");
    let xml = {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .event_notification_tag_keys
            .retain(|k| !keys.contains(k));
        // Deregistering with IncludeAllTagsOfInstance=true clears the flag.
        if include_all == Some(true) {
            state.event_notification_include_all_tags = false;
        }
        event_tag_attribute(state)
    };
    Ok(Ec2Service::respond(
        "DeregisterInstanceEventNotificationAttributes",
        &req.request_id,
        &xml,
    ))
}

pub(crate) fn describe_event_notification_attributes(
    svc: &Ec2Service,
    req: &AwsRequest,
) -> Result<AwsResponse, AwsServiceError> {
    let accounts = svc.state.read();
    let empty = Ec2State::new(&req.account_id, &req.region);
    let state = accounts.get(&req.account_id).unwrap_or(&empty);
    Ok(Ec2Service::respond(
        "DescribeInstanceEventNotificationAttributes",
        &req.request_id,
        &event_tag_attribute(state),
    ))
}

/// Collect `InstanceTagAttribute.InstanceTagKey.N` values.
fn indexed_sub_keys(params: &HashMap<String, String>) -> Vec<String> {
    let mut out = Vec::new();
    let mut n = 1usize;
    while let Some(v) = params.get(&format!("InstanceTagAttribute.InstanceTagKey.{n}")) {
        out.push(v.clone());
        n += 1;
    }
    out
}

fn event_tag_attribute(state: &Ec2State) -> String {
    // `ec2_list` already wraps each element in <item>; pass the (escaped) key
    // strings directly so the shape is <item>key</item>, not a nested pair.
    let keys: Vec<String> = state
        .event_notification_tag_keys
        .iter()
        .map(|k| fakecloud_aws::xml::xml_escape(k))
        .collect();
    format!(
        "<instanceTagAttribute><includeAllTagsOfInstance>{}</includeAllTagsOfInstance>{}</instanceTagAttribute>",
        state.event_notification_include_all_tags,
        ec2_list("instanceTagKeySet", &keys)
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

#[cfg(test)]
mod tests {
    use super::subnet_ip_prefix;

    #[test]
    fn subnet_ip_prefix_uses_subnet_network() {
        // The synthesized metadata IP must land inside the subnet (finding 1.7).
        assert_eq!(subnet_ip_prefix("172.31.16.0/20"), "172.31.16");
        assert_eq!(subnet_ip_prefix("10.0.5.0/24"), "10.0.5");
        // bare address (no mask) still works
        assert_eq!(subnet_ip_prefix("192.168.1.0"), "192.168.1");
    }

    #[test]
    fn subnet_ip_prefix_falls_back_on_garbage() {
        assert_eq!(subnet_ip_prefix(""), "10.0.0");
        assert_eq!(subnet_ip_prefix("not-a-cidr"), "10.0.0");
        // IPv6 / non-dotted-quad falls back rather than producing nonsense
        assert_eq!(subnet_ip_prefix("fd00::/8"), "10.0.0");
    }
}

#[cfg(test)]
mod modify_tests {
    use super::*;

    fn req(action: &str, query: &[(&str, &str)]) -> AwsRequest {
        AwsRequest {
            service: "ec2".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "000000000000".into(),
            request_id: "rid".into(),
            headers: http::HeaderMap::new(),
            query_params: query
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: bytes::Bytes::new(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: Vec::new(),
            raw_path: "/".into(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: true,
            access_key_id: None,
            principal: None,
        }
    }

    fn body(resp: AwsResponse) -> String {
        String::from_utf8_lossy(resp.body.expect_bytes()).to_string()
    }

    #[test]
    fn modify_instance_placement_renders_affinity() {
        // bug-audit 2026-07-29 (cycle 8) E2-7: placement_affinity was written but
        // the DescribeInstances <placement> render omitted <affinity> (dead
        // write) -> the value never reflected.
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-aff");
        modify_instance_placement(
            &svc,
            &req(
                "ModifyInstancePlacement",
                &[("InstanceId", "i-aff"), ("Affinity", "host")],
            ),
        )
        .unwrap();
        let desc = body(describe_instances(&svc, &req("DescribeInstances", &[])).unwrap());
        assert!(desc.contains("<affinity>host</affinity>"), "{desc}");
    }

    #[test]
    fn parse_launch_opts_reads_cpu_tenancy_and_dns() {
        // bug-audit 2026-07-28 (cycle 7) E4: RunInstances hardcoded these to
        // defaults even when the request set them (only Modify* handlers did),
        // so aws_instance drifted on ForceNew attributes. The launch path must
        // parse them.
        let mut p = std::collections::HashMap::new();
        p.insert("CpuOptions.CoreCount".to_string(), "2".to_string());
        p.insert("CpuOptions.ThreadsPerCore".to_string(), "1".to_string());
        p.insert("Placement.Tenancy".to_string(), "dedicated".to_string());
        p.insert(
            "PrivateDnsNameOptions.HostnameType".to_string(),
            "resource-name".to_string(),
        );
        p.insert(
            "PrivateDnsNameOptions.EnableResourceNameDnsARecord".to_string(),
            "true".to_string(),
        );
        let lo = super::parse_launch_opts(&p);
        let cpu = lo.cpu_options.expect("cpu options parsed");
        assert_eq!(cpu.core_count, 2);
        assert_eq!(cpu.threads_per_core, 1);
        assert_eq!(lo.placement_tenancy.as_deref(), Some("dedicated"));
        assert_eq!(
            lo.private_dns_hostname_type.as_deref(),
            Some("resource-name")
        );
        assert!(lo.enable_a_record);
        assert!(!lo.enable_aaaa_record);

        // Absent -> all defaults (no phantom cpu_options struct).
        let empty = super::parse_launch_opts(&std::collections::HashMap::new());
        assert!(empty.cpu_options.is_none());
        assert!(empty.placement_tenancy.is_none());
    }

    #[test]
    fn describe_instance_attribute_escapes_user_data_xml() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-esc");
        {
            let mut accounts = svc.state.write();
            let state = accounts.get_or_create("000000000000");
            state.instances.get_mut("i-esc").unwrap().user_data = Some("echo a && b <c>".into());
        }
        let resp = describe_instance_attribute(
            &svc,
            &req(
                "DescribeInstanceAttribute",
                &[("InstanceId", "i-esc"), ("Attribute", "userData")],
            ),
        )
        .unwrap();
        let xml = String::from_utf8_lossy(resp.body.expect_bytes()).to_string();
        assert!(
            xml.contains("echo a &amp;&amp; b &lt;c&gt;"),
            "userData must be XML-escaped: {xml}"
        );
        assert!(!xml.contains("b <c>"), "raw < must not appear: {xml}");
    }

    fn seed_instance(svc: &Ec2Service, id: &str) {
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create("000000000000");
        let inst = Instance {
            instance_id: id.into(),
            image_id: "ami-1".into(),
            instance_type: "t3.micro".into(),
            state_code: 16,
            state_name: "running".into(),
            private_ip: "10.0.0.5".into(),
            public_ip: None,
            subnet_id: Some("subnet-1".into()),
            vpc_id: Some("vpc-1".into()),
            key_name: None,
            security_group_ids: vec![],
            reservation_id: "r-1".into(),
            ami_launch_index: 0,
            monitoring: false,
            az: "us-east-1a".into(),
            launch_time: "2024-01-01T00:00:00.000Z".into(),
            container_id: None,
            disable_api_termination: false,
            disable_api_stop: false,
            source_dest_check: true,
            ebs_optimized: false,
            instance_initiated_shutdown_behavior: "stop".into(),
            user_data: None,
            metadata_options: Default::default(),
            cpu_options: None,
            bandwidth_weighting: None,
            maintenance_options: Default::default(),
            placement_tenancy: None,
            placement_affinity: None,
            placement_group_name: None,
            private_dns_hostname_type: None,
            enable_resource_name_dns_a_record: false,
            enable_resource_name_dns_aaaa_record: false,
        };
        state.instances.insert(id.to_string(), inst);
    }

    // Docker-free coverage of the metadata threading the StartInstances
    // stopped-then-restart fallback relies on: after a restart the runtime
    // registry has no handle for a `stopped` instance, so the boot task must
    // reconstitute a fresh container via `run_instance`, feeding it the same
    // user-data / tags / subnet placement as `recover_persisted_containers`
    // would. This asserts `run_instance_inputs` extracts exactly those from the
    // persisted instance (the container spawn itself needs Docker and is not
    // exercised here).
    #[test]
    fn run_instance_inputs_mirror_persisted_metadata() {
        let svc = Ec2Service::new();
        {
            let mut accounts = svc.state.write();
            let state = accounts.get_or_create("000000000000");
            let inst = crate::state::Instance {
                instance_id: "i-1".into(),
                image_id: "ami-1".into(),
                instance_type: "t3.micro".into(),
                state_code: 80,
                state_name: "stopped".into(),
                private_ip: "10.0.0.5".into(),
                public_ip: None,
                subnet_id: Some("subnet-1".into()),
                vpc_id: Some("vpc-1".into()),
                key_name: None,
                security_group_ids: vec![],
                reservation_id: "r-1".into(),
                ami_launch_index: 0,
                monitoring: false,
                az: "us-east-1a".into(),
                launch_time: "2024-01-01T00:00:00.000Z".into(),
                container_id: None,
                disable_api_termination: false,
                disable_api_stop: false,
                source_dest_check: true,
                ebs_optimized: false,
                instance_initiated_shutdown_behavior: "stop".into(),
                user_data: Some("Zm9v".into()),
                metadata_options: Default::default(),
                cpu_options: None,
                bandwidth_weighting: None,
                maintenance_options: Default::default(),
                placement_tenancy: None,
                placement_affinity: None,
                placement_group_name: None,
                private_dns_hostname_type: None,
                enable_resource_name_dns_a_record: false,
                enable_resource_name_dns_aaaa_record: false,
            };
            state.instances.insert("i-1".into(), inst);
            state.upsert_tags(
                "i-1",
                &[Tag {
                    key: "Name".into(),
                    value: "web".into(),
                }],
            );
        }

        let (user_data, tags, network) =
            run_instance_inputs(&svc.state, "000000000000", "i-1").expect("instance exists");
        assert_eq!(user_data.as_deref(), Some("Zm9v"), "user-data is threaded");
        assert_eq!(
            tags.get("Name").map(String::as_str),
            Some("web"),
            "tags threaded"
        );
        let net = network.expect("subnet-bound instance gets a network placement");
        assert_eq!(net.subnet_id, "subnet-1", "rejoins the same subnet");

        // A missing instance yields None (mirrors a concurrent terminate).
        assert!(run_instance_inputs(&svc.state, "000000000000", "i-missing").is_none());
    }

    #[test]
    fn cfn_instance_honors_metadata_monitoring_ebs_and_iam_profile() {
        // Fix #4: the CFN AWS::EC2::Instance provisioner must carry through
        // MetadataOptions, Monitoring, EbsOptimized, and IamInstanceProfile so
        // DescribeInstances (and the IAM-profile association list) reflect them,
        // matching a direct RunInstances launch.
        let svc = Ec2Service::new();
        let spec = CfnInstanceSpec {
            image_id: Some("ami-123".into()),
            instance_type: Some("t3.small".into()),
            metadata_options: Some(crate::state::MetadataOptions {
                http_tokens: "required".into(),
                http_put_response_hop_limit: 3,
                ..Default::default()
            }),
            ebs_optimized: true,
            monitoring: true,
            iam_instance_profile_name: Some("my-profile".into()),
            ..Default::default()
        };
        let attrs = cfn_create_instance(&svc, "000000000000", "us-east-1", &spec);

        let accounts = svc.state.read();
        let state = accounts.get("000000000000").unwrap();
        let inst = state.instances.get(&attrs.instance_id).unwrap();
        assert_eq!(inst.metadata_options.http_tokens, "required");
        assert_eq!(inst.metadata_options.http_put_response_hop_limit, 3);
        assert!(inst.ebs_optimized);
        assert!(inst.monitoring);
        // An IAM instance-profile association was recorded for the instance.
        let assoc = state
            .iam_instance_profile_associations
            .values()
            .find(|a| a.instance_id == attrs.instance_id)
            .expect("iam instance profile association should be recorded");
        assert!(assoc.iam_instance_profile_arn.contains("my-profile"));
    }

    #[test]
    fn modify_instance_credit_specification_round_trips() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-1");
        modify_instance_credit_specification(
            &svc,
            &req(
                "ModifyInstanceCreditSpecification",
                &[
                    ("InstanceCreditSpecification.1.InstanceId", "i-1"),
                    ("InstanceCreditSpecification.1.CpuCredits", "unlimited"),
                ],
            ),
        )
        .unwrap();
        let out = body(
            describe_instance_credit_specifications(
                &svc,
                &req(
                    "DescribeInstanceCreditSpecifications",
                    &[("InstanceId.1", "i-1")],
                ),
            )
            .unwrap(),
        );
        assert!(
            out.contains("<cpuCredits>unlimited</cpuCredits>"),
            "got: {out}"
        );
    }

    #[test]
    fn modify_instance_credit_specification_unknown_is_unsuccessful() {
        let svc = Ec2Service::new();
        let out = body(
            modify_instance_credit_specification(
                &svc,
                &req(
                    "ModifyInstanceCreditSpecification",
                    &[("InstanceCreditSpecification.1.InstanceId", "i-missing")],
                ),
            )
            .unwrap(),
        );
        assert!(out.contains("InvalidInstanceID.NotFound"), "got: {out}");
    }

    #[test]
    fn instance_metadata_defaults_round_trip_and_reset() {
        let svc = Ec2Service::new();
        modify_instance_metadata_defaults(
            &svc,
            &req(
                "ModifyInstanceMetadataDefaults",
                &[
                    ("HttpTokens", "required"),
                    ("HttpEndpoint", "enabled"),
                    ("HttpTokensEnforced", "enabled"),
                ],
            ),
        )
        .unwrap();
        let out = body(
            get_instance_metadata_defaults(&svc, &req("GetInstanceMetadataDefaults", &[])).unwrap(),
        );
        assert!(
            out.contains("<httpTokens>required</httpTokens>"),
            "got: {out}"
        );
        assert!(out.contains("<httpEndpoint>enabled</httpEndpoint>"));
        // HttpTokensEnforced must persist too (Cubic P2 on #2057).
        assert!(out.contains("<httpTokensEnforced>enabled</httpTokensEnforced>"));

        // `no-preference` drops the stored default.
        modify_instance_metadata_defaults(
            &svc,
            &req(
                "ModifyInstanceMetadataDefaults",
                &[("HttpTokens", "no-preference")],
            ),
        )
        .unwrap();
        let out = body(
            get_instance_metadata_defaults(&svc, &req("GetInstanceMetadataDefaults", &[])).unwrap(),
        );
        assert!(
            !out.contains("<httpTokens>"),
            "no-preference should clear it: {out}"
        );
    }

    #[test]
    fn event_notification_attributes_persist_keys() {
        let svc = Ec2Service::new();
        register_event_notification_attributes(
            &svc,
            &req(
                "RegisterInstanceEventNotificationAttributes",
                &[
                    ("InstanceTagAttribute.InstanceTagKey.1", "Name"),
                    ("InstanceTagAttribute.InstanceTagKey.2", "env"),
                ],
            ),
        )
        .unwrap();
        let out = body(
            describe_event_notification_attributes(
                &svc,
                &req("DescribeInstanceEventNotificationAttributes", &[]),
            )
            .unwrap(),
        );
        assert!(out.contains("<item>Name</item>"), "got: {out}");
        assert!(out.contains("<item>env</item>"));
        // Keys must not be double-wrapped as <item><item>key</item></item>
        // (Cubic P2 on #2057).
        assert!(!out.contains("<item><item>"), "got: {out}");

        deregister_event_notification_attributes(
            &svc,
            &req(
                "DeregisterInstanceEventNotificationAttributes",
                &[("InstanceTagAttribute.InstanceTagKey.1", "Name")],
            ),
        )
        .unwrap();
        let out = body(
            describe_event_notification_attributes(
                &svc,
                &req("DescribeInstanceEventNotificationAttributes", &[]),
            )
            .unwrap(),
        );
        assert!(!out.contains("<item>Name</item>"), "got: {out}");
        assert!(out.contains("<item>env</item>"));
    }

    #[test]
    fn describe_instances_explicit_missing_id_errors() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-1");
        let err = crate::test_support::err_of(describe_instances(
            &svc,
            &req("DescribeInstances", &[("InstanceId.1", "i-missing")]),
        ));
        assert_eq!(err.code(), "InvalidInstanceID.NotFound");
    }

    #[test]
    fn describe_instances_existing_id_ok() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-1");
        let out = body(
            describe_instances(&svc, &req("DescribeInstances", &[("InstanceId.1", "i-1")]))
                .unwrap(),
        );
        assert!(out.contains("<instanceId>i-1</instanceId>"), "got: {out}");
    }

    #[test]
    fn describe_instances_group_and_attachment_filters() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-1");
        {
            let mut accounts = svc.state.write();
            let state = accounts.get_or_create("000000000000");
            state.instances.get_mut("i-1").unwrap().security_group_ids = vec!["sg-web".into()];
            // A security group so `group-name` can resolve.
            state.security_groups.insert(
                "sg-web".into(),
                crate::state::SecurityGroup {
                    group_id: "sg-web".into(),
                    group_name: "web".into(),
                    description: "d".into(),
                    vpc_id: "vpc-1".into(),
                    rules: vec![],
                },
            );
            // A volume attached to the instance for block-device-mapping filters.
            state.volumes.insert(
                "vol-1".into(),
                crate::state::Volume {
                    volume_id: "vol-1".into(),
                    size: 8,
                    snapshot_id: None,
                    availability_zone: "us-east-1a".into(),
                    state: "in-use".into(),
                    volume_type: "gp3".into(),
                    iops: None,
                    throughput: None,
                    encrypted: false,
                    kms_key_id: None,
                    multi_attach_enabled: false,
                    auto_enable_io: false,
                    attachments: vec![crate::state::VolumeAttachment {
                        volume_id: "vol-1".into(),
                        instance_id: "i-1".into(),
                        device: "/dev/sdf".into(),
                        status: "attached".into(),
                        delete_on_termination: true,
                    }],
                    in_recycle_bin: false,
                    modification: None,
                },
            );
        }
        let matches = |name: &str, value: &str| -> bool {
            body(
                describe_instances(
                    &svc,
                    &req(
                        "DescribeInstances",
                        &[("Filter.1.Name", name), ("Filter.1.Value.1", value)],
                    ),
                )
                .unwrap(),
            )
            .contains("<instanceId>i-1</instanceId>")
        };
        assert!(matches("instance.group-id", "sg-web"));
        assert!(matches("group-id", "sg-web"));
        assert!(matches("group-name", "web"));
        assert!(matches("network-interface.group-id", "sg-web"));
        assert!(matches("launch-time", "2024-01-01T00:00:00.000Z"));
        assert!(matches("network-interface.subnet-id", "subnet-1"));
        assert!(matches("block-device-mapping.volume-id", "vol-1"));
        assert!(matches("block-device-mapping.device-name", "/dev/sdf"));
        // A group that the instance is not in does not match.
        assert!(!matches("instance.group-id", "sg-other"));
        // A genuinely-unknown filter still matches nothing.
        assert!(!matches("bogus-filter-name", "sg-web"));
    }

    #[test]
    fn describe_instances_reports_linux_platform_details() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-1");
        // "ami-1" is not in the catalogue, so platformDetails falls back to
        // Linux/UNIX — the same default as image_xml.
        let out = body(
            describe_instances(&svc, &req("DescribeInstances", &[("InstanceId.1", "i-1")]))
                .unwrap(),
        );
        assert!(
            out.contains("<platformDetails>Linux/UNIX</platformDetails>"),
            "got: {out}"
        );
        assert!(
            out.contains("<usageOperation>RunInstances</usageOperation>"),
            "got: {out}"
        );
        assert!(!out.contains("<platform>windows</platform>"), "got: {out}");
    }

    #[test]
    fn describe_instances_reports_windows_platform() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-1");
        {
            let mut accounts = svc.state.write();
            let inst = accounts
                .get_mut("000000000000")
                .unwrap()
                .instances
                .get_mut("i-1")
                .unwrap();
            // Seeded Windows Server AMI (platform = windows).
            inst.image_id = "ami-0a1b2c3d4e5f60006".into();
        }
        let out = body(
            describe_instances(&svc, &req("DescribeInstances", &[("InstanceId.1", "i-1")]))
                .unwrap(),
        );
        assert!(out.contains("<platform>windows</platform>"), "got: {out}");
        // platformDetails is the capitalized billing label, distinct from the
        // lowercase `<platform>` wire element.
        assert!(
            out.contains("<platformDetails>Windows</platformDetails>"),
            "got: {out}"
        );
        assert!(
            out.contains("<usageOperation>RunInstances:0002</usageOperation>"),
            "got: {out}"
        );
    }

    #[tokio::test]
    async fn run_instances_reports_platform_details_on_reservation() {
        let svc = Ec2Service::new();
        // Seeded Windows Server AMI -> the RunInstances reservation body carries
        // the Windows billing label + RunInstances:0002 (covers the second
        // render path, distinct from DescribeInstances).
        let out = body(
            run_instances(
                &svc,
                &req(
                    "RunInstances",
                    &[
                        ("ImageId", "ami-0a1b2c3d4e5f60006"),
                        ("MinCount", "1"),
                        ("MaxCount", "1"),
                    ],
                ),
            )
            .await
            .unwrap(),
        );
        assert!(out.contains("<platform>windows</platform>"), "got: {out}");
        assert!(
            out.contains("<platformDetails>Windows</platformDetails>"),
            "got: {out}"
        );
        assert!(
            out.contains("<usageOperation>RunInstances:0002</usageOperation>"),
            "got: {out}"
        );
    }

    #[tokio::test]
    async fn reconcile_pending_metadata_only_flips_to_running() {
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-1");
        // Simulate a persisted mid-boot instance.
        {
            let mut accounts = svc.state.write();
            let inst = accounts
                .get_mut("000000000000")
                .unwrap()
                .instances
                .get_mut("i-1")
                .unwrap();
            inst.state_code = 0;
            inst.state_name = "pending".into();
        }
        svc.reconcile_pending_metadata_only().await;
        let accounts = svc.state.read();
        let inst = &accounts.get("000000000000").unwrap().instances["i-1"];
        assert_eq!(inst.state_code, 16);
        assert_eq!(inst.state_name, "running");
    }

    fn state_of(svc: &Ec2Service, id: &str) -> (i64, Option<String>) {
        let accounts = svc.state.read();
        let inst = &accounts.get("000000000000").unwrap().instances[id];
        (inst.state_code, inst.container_id.clone())
    }

    #[test]
    fn check_transition_enforces_terminal_and_protection() {
        // Direct unit test of the shared re-check helper the write-lock TOCTOU
        // guard relies on (bug-hunt finding 4.1).
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-1");
        let mut accounts = svc.state.write();
        let state = accounts.get_or_create("000000000000");
        let inst = state.instances.get_mut("i-1").unwrap();

        // Running -> start/stop/terminate all legal.
        assert!(check_transition(inst, "i-1", 16).is_ok());
        assert!(check_transition(inst, "i-1", 80).is_ok());
        assert!(check_transition(inst, "i-1", 48).is_ok());

        // Terminated is terminal: no Start (16) allowed, only re-terminate.
        inst.state_code = 48;
        inst.state_name = "terminated".into();
        assert_eq!(
            check_transition(inst, "i-1", 16).unwrap_err().code(),
            "IncorrectInstanceState"
        );
        assert!(check_transition(inst, "i-1", 48).is_ok());

        // Protection flags map to OperationNotPermitted.
        inst.state_code = 16;
        inst.state_name = "running".into();
        inst.disable_api_termination = true;
        assert_eq!(
            check_transition(inst, "i-1", 48).unwrap_err().code(),
            "OperationNotPermitted"
        );
        inst.disable_api_termination = false;
        inst.disable_api_stop = true;
        assert_eq!(
            check_transition(inst, "i-1", 80).unwrap_err().code(),
            "OperationNotPermitted"
        );
    }

    #[tokio::test]
    async fn start_after_terminate_does_not_resurrect() {
        // End-to-end: once terminated, a StartInstances must be rejected rather
        // than overwriting code 48 with pending (the resurrection the write-lock
        // re-check closes). Metadata-only mode has no runtime, so the terminal
        // state is fully determined by the control-plane path.
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-1");

        terminate_instances(&svc, &req("TerminateInstances", &[("InstanceId.1", "i-1")]))
            .await
            .unwrap();
        let (code, container) = state_of(&svc, "i-1");
        assert_eq!(code, 48, "terminate must set code 48");
        assert_eq!(container, None, "terminate must drop the container handle");

        let err = start_instances(&svc, &req("StartInstances", &[("InstanceId.1", "i-1")]))
            .await
            .err()
            .expect("starting a terminated instance must fail");
        assert_eq!(err.code(), "IncorrectInstanceState");

        // State is untouched: still terminated, no partial resurrection.
        let (code, _) = state_of(&svc, "i-1");
        assert_eq!(code, 48, "instance must stay terminated");
    }

    #[tokio::test]
    async fn change_state_is_atomic_on_mixed_ids() {
        // AWS applies the whole call or none: a batch where one id is illegal
        // (terminated) must fail entirely and leave the other id untouched.
        let svc = Ec2Service::new();
        seed_instance(&svc, "i-ok");
        seed_instance(&svc, "i-dead");
        terminate_instances(
            &svc,
            &req("TerminateInstances", &[("InstanceId.1", "i-dead")]),
        )
        .await
        .unwrap();

        let err = start_instances(
            &svc,
            &req(
                "StartInstances",
                &[("InstanceId.1", "i-ok"), ("InstanceId.2", "i-dead")],
            ),
        )
        .await
        .err()
        .expect("batch with a terminated id must fail");
        assert_eq!(err.code(), "IncorrectInstanceState");

        // i-ok must NOT have been flipped to pending by a partial write.
        let (code, _) = state_of(&svc, "i-ok");
        assert_eq!(
            code, 16,
            "healthy instance must be untouched on atomic fail"
        );
    }
}
