//! `AWS::AutoScaling::*` CloudFormation provisioning. Creates Launch
//! Configurations and Auto Scaling Groups as real records in the `autoscaling`
//! service state (metadata-only: the group is reconciled to its desired
//! capacity with placeholder instances, mirroring the service's control plane;
//! real container instances are a runtime concern, not CFN-time). cycle 4.

use chrono::Utc;
use fakecloud_autoscaling::state::{
    AsgInstance, AsgTag, AutoScalingGroup, BlockDeviceMapping, Ebs, InstanceMetadataOptions,
    InstancesDistribution, LaunchConfiguration, LaunchTemplateOverride, LaunchTemplateSpec,
    MixedInstancesPolicy,
};
use serde_json::Value;
use uuid::Uuid;

use super::{ProvisionResult, ResourceDefinition, ResourceProvisioner, StackResource};

/// Parse a CFN `LaunchTemplate` / `LaunchTemplateSpecification` block
/// (`{LaunchTemplateId|LaunchTemplateName, Version}`) into a [`LaunchTemplateSpec`].
fn parse_cfn_launch_template(v: Option<&Value>) -> Option<LaunchTemplateSpec> {
    let obj = v?;
    let id = obj.get("LaunchTemplateId").and_then(|x| x.as_str());
    let name = obj.get("LaunchTemplateName").and_then(|x| x.as_str());
    if id.is_none() && name.is_none() {
        return None;
    }
    Some(LaunchTemplateSpec {
        launch_template_id: id.map(String::from),
        launch_template_name: name.map(String::from),
        version: obj.get("Version").and_then(scalar),
    })
}

/// A CFN scalar (string, number or bool) as a string.
fn scalar(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// A CFN boolean, given as a JSON bool or (after `Ref` resolution) a string.
fn prop_bool(p: &Value, k: &str) -> Option<bool> {
    p.get(k).and_then(|v| {
        v.as_bool()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    })
}

/// Recorded on a real account for `LaunchTemplate` at creation: the API takes these
/// versions, CloudFormation does not. Applied the same to the mixed policy's
/// specifications and to updates, which name the same shape.
fn refuse_floating_launch_template_versions(props: &Value) -> Result<(), String> {
    let lt = &props["MixedInstancesPolicy"]["LaunchTemplate"];
    let overrides = lt["Overrides"].as_array().into_iter().flatten();
    let versions = std::iter::once(&props["LaunchTemplate"]["Version"])
        .chain(std::iter::once(
            &lt["LaunchTemplateSpecification"]["Version"],
        ))
        .chain(overrides.map(|o| &o["LaunchTemplateSpecification"]["Version"]));
    for version in versions {
        if matches!(version.as_str(), Some("$Latest" | "$Default")) {
            return Err(
                "CloudFormation does not support using $Latest or $Default for LaunchTemplate version.  Please specify a version number or name."
                    .to_string(),
            );
        }
    }
    Ok(())
}

/// CFN `MixedInstancesPolicy`.
fn parse_cfn_mixed_instances_policy(props: &Value) -> Option<MixedInstancesPolicy> {
    let policy = props.get("MixedInstancesPolicy")?;
    let lt = policy.get("LaunchTemplate")?;
    let launch_template = parse_cfn_launch_template(lt.get("LaunchTemplateSpecification"))?;
    let overrides = lt
        .get("Overrides")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .map(|o| LaunchTemplateOverride {
                    instance_type: o.get("InstanceType").and_then(scalar),
                    weighted_capacity: o.get("WeightedCapacity").and_then(scalar),
                    launch_template_specification: parse_cfn_launch_template(
                        o.get("LaunchTemplateSpecification"),
                    ),
                })
                .collect()
        })
        .unwrap_or_default();
    let instances_distribution = policy.get("InstancesDistribution").map(|d| {
        let s = |k: &str| d.get(k).and_then(scalar);
        let n = |k: &str| s(k).and_then(|v| v.parse().ok());
        InstancesDistribution {
            on_demand_allocation_strategy: s("OnDemandAllocationStrategy"),
            on_demand_base_capacity: n("OnDemandBaseCapacity"),
            on_demand_percentage_above_base_capacity: n("OnDemandPercentageAboveBaseCapacity"),
            spot_allocation_strategy: s("SpotAllocationStrategy"),
            spot_instance_pools: n("SpotInstancePools"),
            spot_max_price: s("SpotMaxPrice"),
        }
    });
    Some(MixedInstancesPolicy {
        launch_template,
        overrides,
        instances_distribution,
    })
}

/// CFN `BlockDeviceMappings` of an `AWS::AutoScaling::LaunchConfiguration`.
fn parse_cfn_block_device_mappings(props: &Value) -> Vec<BlockDeviceMapping> {
    props
        .get("BlockDeviceMappings")
        .and_then(|v| v.as_array())
        .map(|items| {
            items
                .iter()
                .filter_map(|m| {
                    let ebs = m.get("Ebs").map(|e| {
                        let s = |k: &str| e.get(k).and_then(scalar);
                        Ebs {
                            snapshot_id: s("SnapshotId"),
                            volume_size: s("VolumeSize").and_then(|v| v.parse().ok()),
                            volume_type: s("VolumeType"),
                            delete_on_termination: prop_bool(e, "DeleteOnTermination"),
                            iops: s("Iops").and_then(|v| v.parse().ok()),
                            encrypted: prop_bool(e, "Encrypted"),
                            throughput: s("Throughput").and_then(|v| v.parse().ok()),
                        }
                    });
                    Some(BlockDeviceMapping {
                        device_name: m.get("DeviceName").and_then(scalar)?,
                        virtual_name: m.get("VirtualName").and_then(scalar),
                        no_device: prop_bool(m, "NoDevice").unwrap_or(false),
                        ebs,
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// CFN `MetadataOptions` of an `AWS::AutoScaling::LaunchConfiguration`.
fn parse_cfn_metadata_options(props: &Value) -> Option<InstanceMetadataOptions> {
    let m = props.get("MetadataOptions")?;
    Some(InstanceMetadataOptions {
        http_tokens: m.get("HttpTokens").and_then(scalar),
        http_put_response_hop_limit: m
            .get("HttpPutResponseHopLimit")
            .and_then(scalar)
            .and_then(|v| v.parse().ok()),
        http_endpoint: m.get("HttpEndpoint").and_then(scalar),
    })
}

/// CFN `Tags` of an `AWS::AutoScaling::AutoScalingGroup`.
fn parse_cfn_asg_tags(props: &Value) -> Vec<AsgTag> {
    props
        .get("Tags")
        .and_then(|v| v.as_array())
        .map(|tags| {
            tags.iter()
                .filter_map(|t| {
                    Some(AsgTag {
                        key: t.get("Key").and_then(scalar)?,
                        value: t.get("Value").and_then(scalar).unwrap_or_default(),
                        propagate_at_launch: prop_bool(t, "PropagateAtLaunch").unwrap_or(false),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn prop_str<'a>(p: &'a Value, k: &str) -> Option<&'a str> {
    p.get(k).and_then(|v| v.as_str())
}

fn prop_i64(p: &Value, k: &str) -> Option<i64> {
    p.get(k).and_then(|v| {
        v.as_i64()
            .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
    })
}

fn str_list(p: &Value, k: &str) -> Vec<String> {
    p.get(k)
        .and_then(|v| v.as_array())
        .map(|a| {
            a.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

impl ResourceProvisioner {
    fn asg_arn(&self, kind: &str, name: &str) -> String {
        fakecloud_autoscaling::autoscaling_arn(
            &self.region,
            &self.account_id,
            kind,
            &Uuid::new_v4().to_string(),
            name,
        )
    }

    /// Derive the launch configuration an `InstanceId`-sourced group launches
    /// from (named after the group) and store it, returning its name and the
    /// instance's subnet / AZ. `replacing` names the configuration the group
    /// already owns under that name (an update), which may be overwritten;
    /// any other configuration of that name is never replaced.
    fn install_instance_launch_configuration(
        &self,
        instance_id: &str,
        group: &str,
        replacing: Option<&str>,
    ) -> Result<(String, Option<String>, String), String> {
        let (lc, subnet, az) = fakecloud_autoscaling::launch::launch_configuration_from_instance(
            &self.ec2_state,
            &self.account_id,
            &self.region,
            instance_id,
            group,
        )?;
        let mut st = self.autoscaling_state.write();
        let acct = st.get_or_create(&self.account_id);
        // Only a configuration this group itself derived from an instance may
        // be replaced; an independently defined one of that name never is.
        let replaceable = replacing == Some(lc.name.as_str())
            && acct
                .launch_configurations
                .get(&lc.name)
                .is_some_and(|existing| existing.source_instance_id.is_some());
        if acct.launch_configurations.contains_key(&lc.name) && !replaceable {
            return Err(format!(
                "Launch Configuration by this name already exists - A launch configuration already exists with the name {}",
                lc.name
            ));
        }
        let name = lc.name.clone();
        acct.launch_configurations.insert(name.clone(), lc);
        Ok((name, subnet, az))
    }

    /// Validate an `AWS::AutoScaling::AutoScalingGroup`'s launch source as
    /// CreateAutoScalingGroup does: exactly one of an instance id, launch
    /// configuration, launch template or mixed-instances policy; a named launch configuration
    /// exists; every launch template resolves (recorded with id + name and
    /// `$Default` when no version is given).
    fn validate_asg_launch_source(
        &self,
        instance_id: Option<&str>,
        launch_configuration: Option<&str>,
        launch_template: Option<&mut LaunchTemplateSpec>,
        mixed: Option<&mut MixedInstancesPolicy>,
    ) -> Result<(), String> {
        let sources = usize::from(launch_configuration.is_some())
            + usize::from(launch_template.is_some())
            + usize::from(mixed.is_some())
            + usize::from(instance_id.is_some());
        if sources != 1 {
            return Err("Valid requests must contain either LaunchTemplate, \
                        LaunchConfigurationName, InstanceId or MixedInstancesPolicy parameter."
                .to_string());
        }
        if let Some(lc) = launch_configuration {
            let exists = self
                .autoscaling_state
                .read()
                .accounts
                .get(&self.account_id)
                .is_some_and(|st| st.launch_configurations.contains_key(lc));
            if !exists {
                return Err(format!(
                    "Launch configuration name not found - Launch configuration {lc} not found"
                ));
            }
        }
        fakecloud_autoscaling::launch::resolve_launch_template_specs(
            Some(&self.ec2_state),
            &self.account_id,
            &self.region,
            launch_template,
            mixed,
        )
    }

    pub(super) fn create_autoscaling_launch_configuration(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = prop_str(props, "LaunchConfigurationName")
            .map(String::from)
            .unwrap_or_else(|| self.physical_name(resource));
        let image_id = prop_str(props, "ImageId")
            .ok_or("AWS::AutoScaling::LaunchConfiguration requires ImageId")?
            .to_string();
        let instance_type = prop_str(props, "InstanceType")
            .ok_or("AWS::AutoScaling::LaunchConfiguration requires InstanceType")?
            .to_string();
        let lc = LaunchConfiguration {
            arn: self.asg_arn("launchConfiguration", &name),
            name: name.clone(),
            image_id,
            instance_type,
            key_name: prop_str(props, "KeyName").map(String::from),
            security_groups: str_list(props, "SecurityGroups"),
            user_data: prop_str(props, "UserData").map(String::from),
            iam_instance_profile: prop_str(props, "IamInstanceProfile").map(String::from),
            associate_public_ip_address: prop_bool(props, "AssociatePublicIpAddress"),
            instance_monitoring: prop_bool(props, "InstanceMonitoring").unwrap_or(true),
            ebs_optimized: prop_bool(props, "EbsOptimized").unwrap_or(false),
            spot_price: prop_str(props, "SpotPrice").map(String::from),
            placement_tenancy: prop_str(props, "PlacementTenancy").map(String::from),
            source_instance_id: None,
            block_device_mappings: parse_cfn_block_device_mappings(props),
            metadata_options: parse_cfn_metadata_options(props),
            created_time: Utc::now(),
        };
        self.autoscaling_state
            .write()
            .get_or_create(&self.account_id)
            .launch_configurations
            .insert(name.clone(), lc);
        Ok(ProvisionResult::new(name))
    }

    pub(super) fn create_autoscaling_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = prop_str(props, "AutoScalingGroupName")
            .map(String::from)
            .unwrap_or_else(|| self.physical_name(resource));
        let min_size = prop_i64(props, "MinSize").unwrap_or(0);
        let max_size = prop_i64(props, "MaxSize").unwrap_or(min_size);
        let desired = prop_i64(props, "DesiredCapacity").unwrap_or(min_size);
        let mut azs = str_list(props, "AvailabilityZones");
        if azs.is_empty() {
            azs.push(format!("{}a", self.region));
        }
        let mut lcn = prop_str(props, "LaunchConfigurationName").map(String::from);
        // Modern templates use LaunchTemplate (or MixedInstancesPolicy) instead
        // of the legacy LaunchConfigurationName; honor every form so the
        // launch spec isn't silently dropped.
        refuse_floating_launch_template_versions(props)?;
        let mut launch_template = parse_cfn_launch_template(props.get("LaunchTemplate"));
        let mut mixed_instances_policy = parse_cfn_mixed_instances_policy(props);
        self.validate_asg_launch_source(
            prop_str(props, "InstanceId"),
            lcn.as_deref(),
            launch_template.as_mut(),
            mixed_instances_policy.as_mut(),
        )?;
        let mut vpc_zone_identifier = props
            .get("VPCZoneIdentifier")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .or_else(|| prop_str(props, "VPCZoneIdentifier").map(String::from));
        // `InstanceId`: a launch configuration named after the group, derived
        // from the instance (as CreateAutoScalingGroup does).
        if let Some(iid) = prop_str(props, "InstanceId") {
            let (lc, subnet, az) = self.install_instance_launch_configuration(iid, &name, None)?;
            if vpc_zone_identifier.is_none() && props.get("AvailabilityZones").is_none() {
                match subnet {
                    Some(s) => vpc_zone_identifier = Some(s),
                    None => azs = vec![az],
                }
            }
            lcn = Some(lc);
        }

        // Insert the group as control-plane only (no instances). After
        // provisioning, `CreateStack` drains an `AsgInstances` spawn intent that
        // reconciles the group to its desired capacity by launching REAL
        // container-backed EC2 instances via the EC2 runtime — the same
        // instances the direct `CreateAutoScalingGroup` path spawns — instead of
        // the phantom placeholder metadata this used to insert at CFN time.
        let instances: Vec<AsgInstance> = Vec::new();

        let arn = self.asg_arn("autoScalingGroup", &name);
        let group = AutoScalingGroup {
            arn: arn.clone(),
            name: name.clone(),
            launch_configuration_name: lcn,
            launch_template,
            min_size,
            max_size,
            desired_capacity: desired,
            default_cooldown: prop_i64(props, "Cooldown").unwrap_or(300),
            availability_zones: azs,
            vpc_zone_identifier,
            health_check_type: prop_str(props, "HealthCheckType")
                .unwrap_or("EC2")
                .to_string(),
            health_check_grace_period: prop_i64(props, "HealthCheckGracePeriod").unwrap_or(0),
            target_group_arns: str_list(props, "TargetGroupARNs"),
            load_balancer_names: str_list(props, "LoadBalancerNames"),
            new_instances_protected_from_scale_in: props
                .get("NewInstancesProtectedFromScaleIn")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            created_time: Utc::now(),
            instances,
            tags: parse_cfn_asg_tags(props),
            status: None,
            service_linked_role_arn: fakecloud_autoscaling::service_linked_role_arn(
                &self.region,
                &self.account_id,
            ),
            mixed_instances_policy,
        };
        self.autoscaling_state
            .write()
            .get_or_create(&self.account_id)
            .groups
            .insert(name.clone(), group);
        self.pending_container_spawns
            .lock()
            .push(super::ContainerSpawnIntent::AsgInstances {
                group_name: name.clone(),
            });
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    /// In-place `UpdateStack` for an `AWS::AutoScaling::AutoScalingGroup`.
    ///
    /// The reprovision fallback would call `delete_autoscaling` (which queues an
    /// `AsgInstances` teardown for EVERY running instance) then
    /// `create_autoscaling_group` (which re-queues a full `AsgInstances` spawn) —
    /// so a benign `DesiredCapacity`/`MinSize`/`Cooldown`/`HealthCheck*`/`Tags`/
    /// `TargetGroupARNs` change would TERMINATE every instance and launch a brand
    /// new set, churning instance ids/IPs and breaking ELB target registrations.
    /// AWS applies all of these in place with no interruption.
    ///
    /// This mutates the stored group record in place (applying only the mutable
    /// properties, preserving `arn`/`created_time`/`instances` and thus the
    /// existing instances' ids) and then queues the SAME `AsgInstances` spawn
    /// intent `create` uses. The drain routes it through the owning
    /// autoscaling service's `reconcile_group`/`apply_capacity`, which
    /// reconciles the instance set to the (possibly-new) desired capacity BY THE
    /// DELTA: scale-up launches only the shortfall, scale-down terminates only
    /// the excess (the newest ids), and an unchanged capacity touches no
    /// instances. Existing instance ids are preserved.
    pub(super) fn update_autoscaling_group(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        refuse_floating_launch_template_versions(props)?;
        let name = existing.physical_id.clone();
        let mut new_lc = prop_str(props, "LaunchConfigurationName").map(String::from);
        let mut new_lt = parse_cfn_launch_template(props.get("LaunchTemplate"));
        let mut new_mixed = parse_cfn_mixed_instances_policy(props);
        self.validate_asg_launch_source(
            prop_str(props, "InstanceId"),
            new_lc.as_deref(),
            new_lt.as_mut(),
            new_mixed.as_mut(),
        )?;
        // An `InstanceId` source re-derives the group's launch configuration
        // from the (possibly new) instance.
        if let Some(iid) = prop_str(props, "InstanceId") {
            // The configuration the group owns, and the instance it came from.
            let (owned, source) = {
                let st = self.autoscaling_state.read();
                let acct = st.accounts.get(&self.account_id);
                let owned = acct
                    .and_then(|a| a.groups.get(&name))
                    .and_then(|g| g.launch_configuration_name.clone());
                let source = owned
                    .as_ref()
                    .and_then(|lc| acct.and_then(|a| a.launch_configurations.get(lc)))
                    .and_then(|lc| lc.source_instance_id.clone());
                (owned, source)
            };
            // Re-derive only for a different instance: the original may since
            // have stopped or terminated, which must not fail an unrelated
            // update (or change what the group launches).
            new_lc = if source.as_deref() == Some(iid) {
                owned
            } else {
                Some(
                    self.install_instance_launch_configuration(iid, &name, owned.as_deref())?
                        .0,
                )
            };
        }

        let arn = {
            let mut st = self.autoscaling_state.write();
            let acct = st.get_or_create(&self.account_id);
            let group = acct
                .groups
                .get_mut(&name)
                .ok_or_else(|| format!("Auto Scaling group {name} not yet provisioned"))?;

            // Mutable-without-replacement properties. Immutable identity (arn,
            // name, created_time) and the live `instances` list are deliberately
            // left untouched so the group -- and its running instances' ids --
            // survive; the instance set is reconciled by delta below.
            if let Some(v) = prop_i64(props, "MinSize") {
                group.min_size = v;
            }
            if let Some(v) = prop_i64(props, "MaxSize") {
                group.max_size = v;
            }
            if let Some(v) = prop_i64(props, "DesiredCapacity") {
                group.desired_capacity = v;
            }
            if let Some(v) = prop_i64(props, "Cooldown") {
                group.default_cooldown = v;
            }
            if let Some(v) = prop_str(props, "HealthCheckType") {
                group.health_check_type = v.to_string();
            }
            if let Some(v) = prop_i64(props, "HealthCheckGracePeriod") {
                group.health_check_grace_period = v;
            }
            if props.get("TargetGroupARNs").is_some() {
                group.target_group_arns = str_list(props, "TargetGroupARNs");
            }
            if props.get("LoadBalancerNames").is_some() {
                group.load_balancer_names = str_list(props, "LoadBalancerNames");
            }
            if let Some(v) = props
                .get("NewInstancesProtectedFromScaleIn")
                .and_then(|v| v.as_bool())
            {
                group.new_instances_protected_from_scale_in = v;
            }
            if props.get("AvailabilityZones").is_some() {
                let azs = str_list(props, "AvailabilityZones");
                if !azs.is_empty() {
                    group.availability_zones = azs;
                }
            }
            if let Some(vzi) = props
                .get("VPCZoneIdentifier")
                .and_then(|v| v.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str())
                        .collect::<Vec<_>>()
                        .join(",")
                })
                .or_else(|| prop_str(props, "VPCZoneIdentifier").map(String::from))
            {
                group.vpc_zone_identifier = Some(vzi);
            }
            // The template's launch source replaces the group's (a group has
            // exactly one).
            if new_lc.is_some() || new_lt.is_some() || new_mixed.is_some() {
                group.launch_configuration_name = new_lc;
                group.launch_template = new_lt;
                group.mixed_instances_policy = new_mixed;
            }
            if props.get("Tags").is_some() {
                group.tags = parse_cfn_asg_tags(props);
            }
            group.arn.clone()
        };

        // Reconcile the instance set to the (possibly-new) desired capacity BY
        // DELTA through the owning service, exactly as `create` does. Preserves
        // existing instance ids; only the delta is spawned/torn down.
        self.pending_container_spawns
            .lock()
            .push(super::ContainerSpawnIntent::AsgInstances {
                group_name: name.clone(),
            });
        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    /// Delete an AutoScaling LaunchConfiguration / Group by physical id (name).
    pub(super) fn delete_autoscaling(&self, resource_type: &str, name: &str) {
        let removed_instance_ids = {
            let mut st = self.autoscaling_state.write();
            let acct = st.get_or_create(&self.account_id);
            match resource_type {
                "AWS::AutoScaling::LaunchConfiguration" => {
                    acct.launch_configurations.remove(name);
                    Vec::new()
                }
                "AWS::AutoScaling::AutoScalingGroup" => acct
                    .groups
                    .remove(name)
                    .map(|g| {
                        g.instances
                            .iter()
                            .map(|i| i.instance_id.clone())
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default(),
                _ => Vec::new(),
            }
        };
        // Queue terminating the REAL EC2 instances the group launched so the
        // stack delete drain reaps them instead of leaking real EC2 containers.
        // Captured before the group record was removed above.
        if !removed_instance_ids.is_empty() {
            self.pending_container_teardowns.lock().push(
                super::ContainerTeardownIntent::AsgInstances {
                    instance_ids: removed_instance_ids,
                },
            );
        }
    }
}
