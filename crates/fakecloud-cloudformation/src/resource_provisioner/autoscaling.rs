//! `AWS::AutoScaling::*` CloudFormation provisioning. Creates Launch
//! Configurations and Auto Scaling Groups as real records in the `autoscaling`
//! service state (metadata-only: the group is reconciled to its desired
//! capacity with placeholder instances, mirroring the service's control plane;
//! real container instances are a runtime concern, not CFN-time). cycle 4.

use chrono::Utc;
use fakecloud_autoscaling::state::{
    AsgInstance, AutoScalingGroup, LaunchConfiguration, LaunchTemplateSpec,
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
        version: obj
            .get("Version")
            .and_then(|x| x.as_str())
            .map(String::from),
    })
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
        format!(
            "arn:aws:autoscaling:{}:{}:{kind}:{}:{kind}Name/{name}",
            self.region,
            self.account_id,
            Uuid::new_v4()
        )
    }

    pub(super) fn create_autoscaling_launch_configuration(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = prop_str(props, "LaunchConfigurationName")
            .map(String::from)
            .unwrap_or_else(|| resource.logical_id.clone());
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
            associate_public_ip_address: props
                .get("AssociatePublicIpAddress")
                .and_then(|v| v.as_bool()),
            instance_monitoring: props
                .get("InstanceMonitoring")
                .and_then(|v| v.as_bool())
                .unwrap_or(true),
            ebs_optimized: props
                .get("EbsOptimized")
                .and_then(|v| v.as_bool())
                .unwrap_or(false),
            spot_price: prop_str(props, "SpotPrice").map(String::from),
            placement_tenancy: prop_str(props, "PlacementTenancy").map(String::from),
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
            .unwrap_or_else(|| resource.logical_id.clone());
        let min_size = prop_i64(props, "MinSize").unwrap_or(0);
        let max_size = prop_i64(props, "MaxSize").unwrap_or(min_size);
        let desired = prop_i64(props, "DesiredCapacity").unwrap_or(min_size);
        let mut azs = str_list(props, "AvailabilityZones");
        if azs.is_empty() {
            azs.push(format!("{}a", self.region));
        }
        let lcn = prop_str(props, "LaunchConfigurationName").map(String::from);
        // Modern templates use LaunchTemplate (or MixedInstancesPolicy) instead
        // of the legacy LaunchConfigurationName; honor both so the launch spec
        // isn't silently dropped.
        let launch_template =
            parse_cfn_launch_template(props.get("LaunchTemplate")).or_else(|| {
                props
                    .get("MixedInstancesPolicy")
                    .and_then(|m| m.get("LaunchTemplate"))
                    .and_then(|lt| lt.get("LaunchTemplateSpecification"))
                    .and_then(|spec| parse_cfn_launch_template(Some(spec)))
            });
        let vpc_zone_identifier = props
            .get("VPCZoneIdentifier")
            .and_then(|v| v.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|x| x.as_str())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .or_else(|| prop_str(props, "VPCZoneIdentifier").map(String::from));

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
            tags: Vec::new(),
            status: None,
            service_linked_role_arn: format!(
                "arn:aws:iam::{}:role/aws-service-role/autoscaling.amazonaws.com/AWSServiceRoleForAutoScaling",
                self.account_id
            ),
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
        let name = existing.physical_id.clone();

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
            if props.get("LaunchConfigurationName").is_some() {
                group.launch_configuration_name =
                    prop_str(props, "LaunchConfigurationName").map(String::from);
            }
            if let Some(lt) = parse_cfn_launch_template(props.get("LaunchTemplate")).or_else(|| {
                props
                    .get("MixedInstancesPolicy")
                    .and_then(|m| m.get("LaunchTemplate"))
                    .and_then(|lt| lt.get("LaunchTemplateSpecification"))
                    .and_then(|spec| parse_cfn_launch_template(Some(spec)))
            }) {
                group.launch_template = Some(lt);
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
