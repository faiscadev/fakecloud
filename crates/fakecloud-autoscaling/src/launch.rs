//! What an Auto Scaling group launches its instances from, and how that maps
//! onto EC2 `RunInstances`.
//!
//! A group launches through the same `RunInstances` path a direct EC2 call
//! takes: a launch configuration is mapped to the equivalent `RunInstances`
//! parameters (image, type, key, security groups, user data, IAM instance
//! profile, monitoring, block-device mappings, metadata options, ...), and a
//! launch template (directly, or through a mixed-instances policy override) is
//! passed as `LaunchTemplate.*` so EC2 resolves the version and merges its data
//! exactly as it does for any launch.

use std::collections::HashMap;

use fakecloud_core::service::AwsRequest;

use crate::state::{
    BlockDeviceMapping, Ebs, InstanceMetadataOptions, InstancesDistribution, LaunchConfiguration,
    LaunchTemplateOverride, LaunchTemplateSpec, MixedInstancesPolicy,
};

/// A non-empty query parameter.
fn param(req: &AwsRequest, key: &str) -> Option<String> {
    req.query_params.get(key).filter(|v| !v.is_empty()).cloned()
}

/// Indexes `N` present under `prefix.member.N.` in a request.
fn member_indexes(req: &AwsRequest, prefix: &str) -> Vec<usize> {
    let lead = format!("{prefix}.member.");
    let mut out: Vec<usize> = req
        .query_params
        .keys()
        .filter_map(|k| k.strip_prefix(&lead))
        .filter_map(|rest| rest.split('.').next()?.parse().ok())
        .collect();
    out.sort_unstable();
    out.dedup();
    out
}

/// `LaunchTemplateSpecification` fields under `prefix` (`prefix.LaunchTemplateId`, ...).
pub(crate) fn parse_launch_template_spec(
    req: &AwsRequest,
    prefix: &str,
) -> Option<LaunchTemplateSpec> {
    let id = param(req, &format!("{prefix}.LaunchTemplateId"));
    let name = param(req, &format!("{prefix}.LaunchTemplateName"));
    if id.is_none() && name.is_none() {
        return None;
    }
    Some(LaunchTemplateSpec {
        launch_template_id: id,
        launch_template_name: name,
        version: param(req, &format!("{prefix}.Version")),
    })
}

/// `BlockDeviceMappings.member.N.*` of a CreateLaunchConfiguration request.
pub(crate) fn parse_block_device_mappings(req: &AwsRequest) -> Vec<BlockDeviceMapping> {
    member_indexes(req, "BlockDeviceMappings")
        .into_iter()
        .filter_map(|n| {
            let p = format!("BlockDeviceMappings.member.{n}");
            let get = |f: &str| param(req, &format!("{p}.{f}"));
            let has_ebs = req
                .query_params
                .keys()
                .any(|k| k.starts_with(&format!("{p}.Ebs.")));
            let ebs = has_ebs.then(|| Ebs {
                snapshot_id: get("Ebs.SnapshotId"),
                volume_size: get("Ebs.VolumeSize").and_then(|v| v.parse().ok()),
                volume_type: get("Ebs.VolumeType"),
                delete_on_termination: get("Ebs.DeleteOnTermination").map(|v| v == "true"),
                iops: get("Ebs.Iops").and_then(|v| v.parse().ok()),
                encrypted: get("Ebs.Encrypted").map(|v| v == "true"),
                throughput: get("Ebs.Throughput").and_then(|v| v.parse().ok()),
            });
            Some(BlockDeviceMapping {
                device_name: get("DeviceName")?,
                virtual_name: get("VirtualName"),
                no_device: get("NoDevice").is_some_and(|v| v == "true"),
                ebs,
            })
        })
        .collect()
}

/// `MetadataOptions.*` of a CreateLaunchConfiguration request.
pub(crate) fn parse_metadata_options(req: &AwsRequest) -> Option<InstanceMetadataOptions> {
    let m = InstanceMetadataOptions {
        http_tokens: param(req, "MetadataOptions.HttpTokens"),
        http_put_response_hop_limit: param(req, "MetadataOptions.HttpPutResponseHopLimit")
            .and_then(|v| v.parse().ok()),
        http_endpoint: param(req, "MetadataOptions.HttpEndpoint"),
    };
    (m != InstanceMetadataOptions::default()).then_some(m)
}

/// `MixedInstancesPolicy.*` of a Create/UpdateAutoScalingGroup request.
pub(crate) fn parse_mixed_instances_policy(req: &AwsRequest) -> Option<MixedInstancesPolicy> {
    let base = "MixedInstancesPolicy.LaunchTemplate";
    let launch_template =
        parse_launch_template_spec(req, &format!("{base}.LaunchTemplateSpecification"))?;
    let overrides = member_indexes(req, &format!("{base}.Overrides"))
        .into_iter()
        .map(|n| {
            let p = format!("{base}.Overrides.member.{n}");
            LaunchTemplateOverride {
                instance_type: param(req, &format!("{p}.InstanceType")),
                weighted_capacity: param(req, &format!("{p}.WeightedCapacity")),
                launch_template_specification: parse_launch_template_spec(
                    req,
                    &format!("{p}.LaunchTemplateSpecification"),
                ),
            }
        })
        .collect();
    let d = "MixedInstancesPolicy.InstancesDistribution";
    let get = |f: &str| param(req, &format!("{d}.{f}"));
    let distribution = InstancesDistribution {
        on_demand_allocation_strategy: get("OnDemandAllocationStrategy"),
        on_demand_base_capacity: get("OnDemandBaseCapacity").and_then(|v| v.parse().ok()),
        on_demand_percentage_above_base_capacity: get("OnDemandPercentageAboveBaseCapacity")
            .and_then(|v| v.parse().ok()),
        spot_allocation_strategy: get("SpotAllocationStrategy"),
        spot_instance_pools: get("SpotInstancePools").and_then(|v| v.parse().ok()),
        spot_max_price: get("SpotMaxPrice"),
    };
    let has_distribution = req.query_params.keys().any(|k| k.starts_with(d));
    Some(MixedInstancesPolicy {
        launch_template,
        overrides,
        instances_distribution: has_distribution.then_some(distribution),
    })
}

/// Resolve a group's launch templates (the direct one, a mixed-instances
/// policy's, and each override's) the way CreateAutoScalingGroup validates
/// them: each must name exactly one of id / name and resolve to an existing
/// version. Each is then recorded with both its id and name, and `$Default`
/// when no version was given, as DescribeAutoScalingGroups reports them.
/// `ec2_state` is `None` without an EC2 backend (unit tests), where there is
/// nothing to resolve against. `Err` is the ValidationError message.
pub fn resolve_launch_template_specs(
    ec2_state: Option<&fakecloud_ec2::SharedEc2State>,
    account_id: &str,
    region: &str,
    launch_template: Option<&mut LaunchTemplateSpec>,
    mixed: Option<&mut MixedInstancesPolicy>,
) -> Result<(), String> {
    let mut specs: Vec<&mut LaunchTemplateSpec> = Vec::new();
    if let Some(lt) = launch_template {
        specs.push(lt);
    }
    if let Some(policy) = mixed {
        specs.push(&mut policy.launch_template);
        for o in policy.overrides.iter_mut() {
            if let Some(lt) = o.launch_template_specification.as_mut() {
                specs.push(lt);
            }
        }
    }
    for spec in specs {
        if spec.launch_template_id.is_some() && spec.launch_template_name.is_some() {
            return Err(
                "You must use a valid fully-formed launch template. You can specify \
                        either a launch template ID or a launch template name, but not both."
                    .to_string(),
            );
        }
        if spec.version.is_none() {
            spec.version = Some("$Default".to_string());
        }
        let Some(ec2_state) = ec2_state else {
            continue;
        };
        let resolved = fakecloud_ec2::service::launch_template::resolve_launch_template_in(
            ec2_state,
            account_id,
            region,
            spec.launch_template_id.as_deref(),
            spec.launch_template_name.as_deref(),
            spec.version.as_deref(),
        )
        .map_err(|e| {
            format!(
                "You must use a valid fully-formed launch template. {}",
                e.message()
            )
        })?;
        spec.launch_template_id = Some(resolved.id);
        spec.launch_template_name = Some(resolved.name);
    }
    Ok(())
}

/// The launch configuration AWS creates for a group given an `InstanceId`:
/// named after the group, with the instance's AMI, type, key pair, security
/// groups, user data, IAM instance profile, monitoring, EBS optimization,
/// tenancy and EBS volumes. Also returns the instance's subnet (the group's
/// `VPCZoneIdentifier` when none is given) and availability zone.
pub fn launch_configuration_from_instance(
    ec2_state: &fakecloud_ec2::SharedEc2State,
    account_id: &str,
    region: &str,
    instance_id: &str,
    name: &str,
) -> Result<(LaunchConfiguration, Option<String>, String), String> {
    let accounts = ec2_state.read();
    let st = accounts
        .regional(account_id, region)
        .ok_or_else(|| format!("Invalid instance id {instance_id}"))?;
    let inst = st
        .instances
        .get(instance_id)
        .filter(|i| i.state_name != "terminated")
        .ok_or_else(|| format!("Invalid instance id {instance_id}"))?;
    let mut volumes: Vec<(
        &fakecloud_ec2::state::Volume,
        &fakecloud_ec2::state::VolumeAttachment,
    )> = st
        .volumes
        .values()
        .filter_map(|v| {
            v.attachments
                .iter()
                .find(|a| a.instance_id == instance_id)
                .map(|a| (v, a))
        })
        .collect();
    volumes.sort_by(|a, b| a.1.device.cmp(&b.1.device));
    let block_device_mappings = volumes
        .into_iter()
        .map(|(v, a)| BlockDeviceMapping {
            device_name: a.device.clone(),
            virtual_name: None,
            no_device: false,
            ebs: Some(Ebs {
                snapshot_id: v.snapshot_id.clone(),
                volume_size: Some(v.size),
                volume_type: Some(v.volume_type.clone()),
                delete_on_termination: Some(a.delete_on_termination),
                iops: v.iops,
                encrypted: Some(v.encrypted),
                throughput: v.throughput,
            }),
        })
        .collect();
    let iam_instance_profile = st
        .iam_instance_profile_associations
        .values()
        .find(|a| a.instance_id == instance_id && a.state != "disassociated")
        .map(|a| a.iam_instance_profile_arn.clone());
    let m = &inst.metadata_options;
    let lc = LaunchConfiguration {
        name: name.to_string(),
        arn: crate::service::autoscaling_arn(
            region,
            account_id,
            "launchConfiguration",
            &uuid::Uuid::new_v4().to_string(),
            name,
        ),
        image_id: inst.image_id.clone(),
        instance_type: inst.instance_type.clone(),
        key_name: inst.key_name.clone(),
        security_groups: inst.security_group_ids.clone(),
        user_data: inst.user_data.clone(),
        iam_instance_profile,
        // Replacements get a public IP exactly when the running instance has
        // one; a stopped instance has released its public IP, so it says
        // nothing and the subnet's default applies.
        associate_public_ip_address: (inst.state_code != 80).then_some(inst.public_ip.is_some()),
        instance_monitoring: inst.monitoring,
        ebs_optimized: inst.ebs_optimized,
        spot_price: None,
        placement_tenancy: inst.placement_tenancy.clone(),
        source_instance_id: Some(instance_id.to_string()),
        block_device_mappings,
        metadata_options: Some(InstanceMetadataOptions {
            http_tokens: Some(m.http_tokens.clone()),
            http_put_response_hop_limit: Some(m.http_put_response_hop_limit),
            http_endpoint: Some(m.http_endpoint.clone()),
        }),
        created_time: chrono::Utc::now(),
    };
    Ok((lc, inst.subnet_id.clone(), inst.az.clone()))
}

/// The `RunInstances` parameters equivalent to a launch configuration.
pub(crate) fn launch_configuration_params(lc: &LaunchConfiguration) -> HashMap<String, String> {
    let mut p = HashMap::new();
    p.insert("ImageId".to_string(), lc.image_id.clone());
    p.insert("InstanceType".to_string(), lc.instance_type.clone());
    if let Some(k) = lc.key_name.as_ref().filter(|k| !k.is_empty()) {
        p.insert("KeyName".to_string(), k.clone());
    }
    // Launch configurations take security groups by id or (default VPC) name.
    let (mut ids, mut names) = (0, 0);
    for sg in &lc.security_groups {
        if sg.starts_with("sg-") {
            ids += 1;
            p.insert(format!("SecurityGroupId.{ids}"), sg.clone());
        } else {
            names += 1;
            p.insert(format!("SecurityGroup.{names}"), sg.clone());
        }
    }
    if let Some(u) = lc.user_data.as_ref().filter(|u| !u.is_empty()) {
        p.insert("UserData".to_string(), u.clone());
    }
    // `IamInstanceProfile` is the profile name or its ARN.
    if let Some(profile) = lc.iam_instance_profile.as_ref().filter(|v| !v.is_empty()) {
        let key = if profile.starts_with("arn:") {
            "IamInstanceProfile.Arn"
        } else {
            "IamInstanceProfile.Name"
        };
        p.insert(key.to_string(), profile.clone());
    }
    if let Some(public) = lc.associate_public_ip_address {
        p.insert("AssociatePublicIpAddress".to_string(), public.to_string());
    }
    p.insert(
        "Monitoring.Enabled".to_string(),
        lc.instance_monitoring.to_string(),
    );
    p.insert("EbsOptimized".to_string(), lc.ebs_optimized.to_string());
    if let Some(t) = lc.placement_tenancy.as_ref().filter(|t| !t.is_empty()) {
        p.insert("Placement.Tenancy".to_string(), t.clone());
    }
    // A `SpotPrice` launches Spot instances at that maximum price.
    if let Some(price) = lc.spot_price.as_ref().filter(|v| !v.is_empty()) {
        p.insert(
            "InstanceMarketOptions.MarketType".to_string(),
            "spot".to_string(),
        );
        p.insert(
            "InstanceMarketOptions.SpotOptions.MaxPrice".to_string(),
            price.clone(),
        );
    }
    for (i, m) in lc.block_device_mappings.iter().enumerate() {
        let b = format!("BlockDeviceMapping.{}", i + 1);
        p.insert(format!("{b}.DeviceName"), m.device_name.clone());
        if let Some(v) = &m.virtual_name {
            p.insert(format!("{b}.VirtualName"), v.clone());
        }
        if m.no_device {
            p.insert(format!("{b}.NoDevice"), String::new());
        }
        if let Some(ebs) = &m.ebs {
            let mut put = |f: &str, v: Option<String>| {
                if let Some(v) = v {
                    p.insert(format!("{b}.Ebs.{f}"), v);
                }
            };
            put("SnapshotId", ebs.snapshot_id.clone());
            put("VolumeSize", ebs.volume_size.map(|v| v.to_string()));
            put("VolumeType", ebs.volume_type.clone());
            put(
                "DeleteOnTermination",
                ebs.delete_on_termination.map(|v| v.to_string()),
            );
            put("Iops", ebs.iops.map(|v| v.to_string()));
            put("Encrypted", ebs.encrypted.map(|v| v.to_string()));
            put("Throughput", ebs.throughput.map(|v| v.to_string()));
            // A mapping with an empty `Ebs` block still creates a volume.
            p.entry(format!("{b}.Ebs.DeleteOnTermination"))
                .or_insert_with(|| "true".to_string());
        }
    }
    if let Some(m) = &lc.metadata_options {
        if let Some(v) = &m.http_tokens {
            p.insert("MetadataOptions.HttpTokens".to_string(), v.clone());
        }
        if let Some(v) = m.http_put_response_hop_limit {
            p.insert(
                "MetadataOptions.HttpPutResponseHopLimit".to_string(),
                v.to_string(),
            );
        }
        if let Some(v) = &m.http_endpoint {
            p.insert("MetadataOptions.HttpEndpoint".to_string(), v.clone());
        }
    }
    p
}

/// What a group launches from.
#[derive(Debug, Clone)]
pub(crate) enum LaunchSource {
    /// A launch configuration.
    Configuration(Box<LaunchConfiguration>),
    /// A launch configuration the group names that no longer exists.
    MissingConfiguration(String),
    /// A launch template (directly or through a mixed-instances policy),
    /// with the override's instance type and weight.
    Template {
        spec: LaunchTemplateSpec,
        instance_type: Option<String>,
        weighted_capacity: Option<String>,
    },
    /// A mixed-instances policy: each launch picks an override and a market
    /// per the policy's distribution (see [`choose_mixed_launch`]).
    Mixed(Box<MixedInstancesPolicy>),
    /// No launch source recorded (a group record inserted without one):
    /// launches a seeded public AMI.
    Default,
}

impl LaunchSource {
    /// The launch source of a group.
    pub(crate) fn for_group(
        g: &crate::state::AutoScalingGroup,
        lcs: &std::collections::BTreeMap<String, LaunchConfiguration>,
    ) -> Self {
        if let Some(policy) = &g.mixed_instances_policy {
            return LaunchSource::Mixed(Box::new(policy.clone()));
        }
        if let Some(lt) = &g.launch_template {
            return LaunchSource::Template {
                spec: lt.clone(),
                instance_type: None,
                weighted_capacity: None,
            };
        }
        match &g.launch_configuration_name {
            Some(name) => match lcs.get(name) {
                Some(lc) => LaunchSource::Configuration(Box::new(lc.clone())),
                None => LaunchSource::MissingConfiguration(name.clone()),
            },
            None => LaunchSource::Default,
        }
    }
}

/// One instance a mixed-instances group already runs (or just launched):
/// which override it came from (`None` when it matches none), whether it is
/// Spot, and its capacity weight.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MixedTally {
    pub(crate) override_index: Option<usize>,
    pub(crate) spot: bool,
    pub(crate) weight: i64,
}

/// Pick the override and market of a mixed-instances group's next launch,
/// per its `InstancesDistribution`:
/// - On-demand until `OnDemandBaseCapacity` is met, then on-demand for
///   `OnDemandPercentageAboveBaseCapacity` percent of the capacity above it
///   (rounded up, as AWS rounds in favor of on-demand), Spot for the rest.
/// - On-demand launches take the highest-priority override (`prioritized`).
///   `lowest-price` orders by price, and with no instance pricing data here
///   the override order stands in for it, so both pick the first override.
/// - Spot launches diversify: `lowest-price` spreads across the first
///   `SpotInstancePools` (default 2) overrides, the capacity strategies
///   (`capacity-optimized`, `price-capacity-optimized`,
///   `capacity-optimized-prioritized`) across all of them; each launch goes
///   to the pool with the least Spot capacity, ties to the higher priority.
///
/// `weights` is each override's capacity weight; the result is
/// `(override index, spot)`.
pub(crate) fn choose_mixed_launch(
    policy: &MixedInstancesPolicy,
    existing: &[MixedTally],
    weights: &[i64],
) -> (usize, bool) {
    let d = policy.instances_distribution.clone().unwrap_or_default();
    let base = d.on_demand_base_capacity.unwrap_or(0).max(0);
    let pct = d
        .on_demand_percentage_above_base_capacity
        .unwrap_or(100)
        .clamp(0, 100);
    let on_demand: i64 = existing.iter().filter(|t| !t.spot).map(|t| t.weight).sum();
    let total: i64 = existing.iter().map(|t| t.weight).sum();
    let pools = weights.len().max(1);
    let first_weight = weights.first().copied().unwrap_or(1);
    let spot = if on_demand < base {
        false
    } else {
        let above_after = (total - base).max(0) + first_weight;
        let want_on_demand = (pct * above_after + 99) / 100;
        (on_demand - base) >= want_on_demand
    };
    if !spot {
        return (0, false);
    }
    let candidates = match d.spot_allocation_strategy.as_deref() {
        None | Some("lowest-price") => {
            (d.spot_instance_pools.unwrap_or(2).max(1) as usize).min(pools)
        }
        _ => pools,
    };
    let spot_capacity = |i: usize| -> i64 {
        existing
            .iter()
            .filter(|t| t.spot && t.override_index == Some(i))
            .map(|t| t.weight)
            .sum()
    };
    let pick = (0..candidates)
        .min_by_key(|i| (spot_capacity(*i), *i))
        .unwrap_or(0);
    (pick, true)
}

/// A `LaunchTemplateSpecification` rendered as its XML members.
pub(crate) fn launch_template_spec_xml(lt: &LaunchTemplateSpec) -> String {
    let el = |n: &str, v: &Option<String>| {
        v.as_deref()
            .map(|v| crate::service::el(n, v))
            .unwrap_or_default()
    };
    format!(
        "{}{}{}",
        el("LaunchTemplateId", &lt.launch_template_id),
        el("LaunchTemplateName", &lt.launch_template_name),
        el("Version", &lt.version),
    )
}

/// `<MixedInstancesPolicy>` of DescribeAutoScalingGroups, with the
/// distribution's AWS defaults filled in.
pub(crate) fn mixed_instances_policy_xml(p: &MixedInstancesPolicy) -> String {
    use crate::service::el;
    let overrides: String = p
        .overrides
        .iter()
        .map(|o| {
            format!(
                "<member>{}{}{}</member>",
                o.instance_type
                    .as_deref()
                    .map(|v| el("InstanceType", v))
                    .unwrap_or_default(),
                o.launch_template_specification
                    .as_ref()
                    .map(|s| format!(
                        "<LaunchTemplateSpecification>{}</LaunchTemplateSpecification>",
                        launch_template_spec_xml(s)
                    ))
                    .unwrap_or_default(),
                o.weighted_capacity
                    .as_deref()
                    .map(|v| el("WeightedCapacity", v))
                    .unwrap_or_default(),
            )
        })
        .collect();
    let d = p.instances_distribution.clone().unwrap_or_default();
    format!(
        "<MixedInstancesPolicy><LaunchTemplate><LaunchTemplateSpecification>{}</LaunchTemplateSpecification>\
         <Overrides>{overrides}</Overrides></LaunchTemplate><InstancesDistribution>{}{}{}{}{}{}</InstancesDistribution></MixedInstancesPolicy>",
        launch_template_spec_xml(&p.launch_template),
        el(
            "OnDemandAllocationStrategy",
            d.on_demand_allocation_strategy.as_deref().unwrap_or("prioritized"),
        ),
        el(
            "OnDemandBaseCapacity",
            &d.on_demand_base_capacity.unwrap_or(0).to_string(),
        ),
        el(
            "OnDemandPercentageAboveBaseCapacity",
            &d.on_demand_percentage_above_base_capacity
                .unwrap_or(100)
                .to_string(),
        ),
        el(
            "SpotAllocationStrategy",
            d.spot_allocation_strategy.as_deref().unwrap_or("lowest-price"),
        ),
        el(
            "SpotInstancePools",
            &d.spot_instance_pools.unwrap_or(2).to_string(),
        ),
        d.spot_max_price
            .as_deref()
            .map(|v| el("SpotMaxPrice", v))
            .unwrap_or_default(),
    )
}

/// `<BlockDeviceMappings>` of DescribeLaunchConfigurations.
pub(crate) fn block_device_mappings_xml(mappings: &[BlockDeviceMapping]) -> String {
    use crate::service::el;
    let items: String = mappings
        .iter()
        .map(|m| {
            let ebs = m
                .ebs
                .as_ref()
                .map(|e| {
                    let opt = |n: &str, v: Option<String>| v.map(|v| el(n, &v)).unwrap_or_default();
                    format!(
                        "<Ebs>{}{}{}{}{}{}{}</Ebs>",
                        opt("SnapshotId", e.snapshot_id.clone()),
                        opt("VolumeSize", e.volume_size.map(|v| v.to_string())),
                        opt("VolumeType", e.volume_type.clone()),
                        opt(
                            "DeleteOnTermination",
                            e.delete_on_termination.map(|v| v.to_string())
                        ),
                        opt("Iops", e.iops.map(|v| v.to_string())),
                        opt("Encrypted", e.encrypted.map(|v| v.to_string())),
                        opt("Throughput", e.throughput.map(|v| v.to_string())),
                    )
                })
                .unwrap_or_default();
            format!(
                "<member>{}{}{}{ebs}</member>",
                m.virtual_name
                    .as_deref()
                    .map(|v| el("VirtualName", v))
                    .unwrap_or_default(),
                el("DeviceName", &m.device_name),
                if m.no_device {
                    el("NoDevice", "true")
                } else {
                    String::new()
                },
            )
        })
        .collect();
    format!("<BlockDeviceMappings>{items}</BlockDeviceMappings>")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lc() -> LaunchConfiguration {
        LaunchConfiguration {
            name: "lc".into(),
            arn: "arn".into(),
            image_id: "ami-1".into(),
            instance_type: "t3.small".into(),
            key_name: Some("kp".into()),
            security_groups: vec!["sg-123".into(), "web".into()],
            user_data: Some("ZWNobw==".into()),
            iam_instance_profile: Some("profile".into()),
            associate_public_ip_address: Some(false),
            instance_monitoring: false,
            ebs_optimized: true,
            spot_price: Some("0.05".into()),
            placement_tenancy: Some("dedicated".into()),
            source_instance_id: None,
            block_device_mappings: vec![BlockDeviceMapping {
                device_name: "/dev/xvda".into(),
                ebs: Some(Ebs {
                    volume_size: Some(25),
                    encrypted: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            metadata_options: Some(InstanceMetadataOptions {
                http_tokens: Some("required".into()),
                ..Default::default()
            }),
            created_time: chrono::Utc::now(),
        }
    }

    #[test]
    fn launch_configuration_maps_onto_run_instances() {
        let p = launch_configuration_params(&lc());
        let want = [
            ("ImageId", "ami-1"),
            ("InstanceType", "t3.small"),
            ("KeyName", "kp"),
            ("SecurityGroupId.1", "sg-123"),
            ("SecurityGroup.1", "web"),
            ("UserData", "ZWNobw=="),
            ("IamInstanceProfile.Name", "profile"),
            ("AssociatePublicIpAddress", "false"),
            ("Monitoring.Enabled", "false"),
            ("EbsOptimized", "true"),
            ("Placement.Tenancy", "dedicated"),
            ("InstanceMarketOptions.MarketType", "spot"),
            ("InstanceMarketOptions.SpotOptions.MaxPrice", "0.05"),
            ("BlockDeviceMapping.1.DeviceName", "/dev/xvda"),
            ("BlockDeviceMapping.1.Ebs.VolumeSize", "25"),
            ("BlockDeviceMapping.1.Ebs.Encrypted", "true"),
            ("BlockDeviceMapping.1.Ebs.DeleteOnTermination", "true"),
            ("MetadataOptions.HttpTokens", "required"),
        ];
        for (k, v) in want {
            assert_eq!(p.get(k).map(String::as_str), Some(v), "{k}");
        }
        let mut arn_lc = lc();
        arn_lc.iam_instance_profile = Some("arn:aws:iam::1:instance-profile/p".into());
        assert!(launch_configuration_params(&arn_lc).contains_key("IamInstanceProfile.Arn"));
    }

    #[test]
    fn mixed_policy_is_its_own_launch_source() {
        let mut g = crate::state::AutoScalingGroup {
            name: "g".into(),
            arn: "a".into(),
            launch_configuration_name: None,
            launch_template: None,
            min_size: 0,
            max_size: 1,
            desired_capacity: 1,
            default_cooldown: 300,
            availability_zones: vec![],
            vpc_zone_identifier: None,
            health_check_type: "EC2".into(),
            health_check_grace_period: 0,
            target_group_arns: vec![],
            load_balancer_names: vec![],
            new_instances_protected_from_scale_in: false,
            created_time: chrono::Utc::now(),
            instances: vec![],
            tags: vec![],
            status: None,
            service_linked_role_arn: String::new(),
            mixed_instances_policy: Some(MixedInstancesPolicy {
                launch_template: LaunchTemplateSpec {
                    launch_template_name: Some("base".into()),
                    ..Default::default()
                },
                overrides: vec![
                    LaunchTemplateOverride {
                        instance_type: Some("c5.large".into()),
                        weighted_capacity: Some("2".into()),
                        launch_template_specification: Some(LaunchTemplateSpec {
                            launch_template_name: Some("other".into()),
                            ..Default::default()
                        }),
                    },
                    LaunchTemplateOverride {
                        instance_type: Some("m5.large".into()),
                        ..Default::default()
                    },
                ],
                instances_distribution: None,
            }),
        };
        assert!(matches!(
            LaunchSource::for_group(&g, &Default::default()),
            LaunchSource::Mixed(_)
        ));
        let policy = g.mixed_instances_policy.clone().unwrap();
        // Default distribution: all on-demand, highest priority.
        assert_eq!(choose_mixed_launch(&policy, &[], &[2, 1]), (0, false));
        g.mixed_instances_policy = None;
        g.launch_configuration_name = Some("gone".into());
        assert!(matches!(
            LaunchSource::for_group(&g, &Default::default()),
            LaunchSource::MissingConfiguration(n) if n == "gone"
        ));
    }

    fn policy(dist: InstancesDistribution, overrides: usize) -> MixedInstancesPolicy {
        MixedInstancesPolicy {
            launch_template: LaunchTemplateSpec::default(),
            overrides: (0..overrides)
                .map(|i| LaunchTemplateOverride {
                    instance_type: Some(format!("t{i}.large")),
                    ..Default::default()
                })
                .collect(),
            instances_distribution: Some(dist),
        }
    }

    /// Launch `n` instances one at a time, as a reconcile does.
    fn launch_n(policy: &MixedInstancesPolicy, n: usize, weights: &[i64]) -> Vec<MixedTally> {
        let mut out: Vec<MixedTally> = Vec::new();
        for _ in 0..n {
            let (i, spot) = choose_mixed_launch(policy, &out, weights);
            out.push(MixedTally {
                override_index: Some(i),
                spot,
                weight: weights[i],
            });
        }
        out
    }

    #[test]
    fn mixed_distribution_honors_base_and_percentage() {
        let p = policy(
            InstancesDistribution {
                on_demand_base_capacity: Some(2),
                on_demand_percentage_above_base_capacity: Some(50),
                ..Default::default()
            },
            3,
        );
        let got = launch_n(&p, 6, &[1, 1, 1]);
        let on_demand = got.iter().filter(|t| !t.spot).count();
        // 2 base + half of the 4 above it.
        assert_eq!(on_demand, 4, "{got:?}");
        assert!(got[..2].iter().all(|t| !t.spot), "base is on-demand first");
        assert!(got
            .iter()
            .filter(|t| !t.spot)
            .all(|t| t.override_index == Some(0)));
    }

    #[test]
    fn spot_lowest_price_spreads_over_its_pools() {
        let p = policy(
            InstancesDistribution {
                on_demand_percentage_above_base_capacity: Some(0),
                spot_allocation_strategy: Some("lowest-price".into()),
                spot_instance_pools: Some(2),
                ..Default::default()
            },
            3,
        );
        let got = launch_n(&p, 4, &[1, 1, 1]);
        assert!(got.iter().all(|t| t.spot));
        let per = |i| got.iter().filter(|t| t.override_index == Some(i)).count();
        assert_eq!((per(0), per(1), per(2)), (2, 2, 0));
    }

    #[test]
    fn spot_capacity_optimized_spreads_over_every_override() {
        let p = policy(
            InstancesDistribution {
                on_demand_percentage_above_base_capacity: Some(0),
                spot_allocation_strategy: Some("capacity-optimized".into()),
                ..Default::default()
            },
            3,
        );
        let got = launch_n(&p, 3, &[1, 1, 1]);
        let mut idx: Vec<_> = got.iter().map(|t| t.override_index.unwrap()).collect();
        idx.sort();
        assert_eq!(idx, vec![0, 1, 2]);
    }
}
