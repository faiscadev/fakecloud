//! The Auto Scaling group behind a managed node group.
//!
//! EKS runs every managed node group as an EC2 Auto Scaling group named
//! `eks-<nodegroup>-<id>` and reports it under `resources.autoScalingGroups`.
//! Tooling tags and reads that group through the Auto Scaling API (the
//! `aws_autoscaling_group_tag` resource the cluster-autoscaler setup relies
//! on), so it has to be a real Auto Scaling record: created with the node
//! group, resized with its scaling config, and deleted with it.

use chrono::Utc;
use fakecloud_autoscaling::state::{AsgTag, AutoScalingGroup};
use fakecloud_autoscaling::SharedAutoScalingState;
use fakecloud_ec2::SharedEc2State;
use serde_json::Value;

use crate::state::Nodegroup;

fn scaling(ng: &Nodegroup, key: &str, default: i64) -> i64 {
    ng.scaling_config
        .get(key)
        .and_then(Value::as_i64)
        .unwrap_or(default)
}

fn subnet_ids(ng: &Nodegroup) -> Vec<String> {
    ng.subnets
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// The tags EKS puts on a node group's Auto Scaling group.
fn eks_asg_tags(cluster: &str, nodegroup: &str) -> Vec<AsgTag> {
    let tag = |key: String, value: &str| AsgTag {
        key,
        value: value.to_string(),
        propagate_at_launch: true,
    };
    vec![
        tag("eks:cluster-name".to_string(), cluster),
        tag("eks:nodegroup-name".to_string(), nodegroup),
        tag("k8s.io/cluster-autoscaler/enabled".to_string(), "true"),
        tag(format!("k8s.io/cluster-autoscaler/{cluster}"), "owned"),
        tag(format!("kubernetes.io/cluster/{cluster}"), "owned"),
    ]
}

/// Create the Auto Scaling group for `ng` (no-op when it already exists).
/// Its Availability Zones come from the node group's subnets in EC2.
pub fn create_nodegroup_asg(
    asg: &SharedAutoScalingState,
    ec2: Option<&SharedEc2State>,
    account_id: &str,
    region: &str,
    ng: &Nodegroup,
) {
    let subnets = subnet_ids(ng);
    let mut zones: Vec<String> = match ec2 {
        Some(ec2) => {
            let mut accounts = ec2.write();
            let state = accounts.regional_mut(account_id, region);
            subnets
                .iter()
                .filter_map(|id| state.subnets.get(id).map(|s| s.availability_zone.clone()))
                .collect()
        }
        None => Vec::new(),
    };
    zones.sort();
    zones.dedup();
    let mut guard = asg.write();
    let st = guard.get_or_create(account_id);
    if st.groups.contains_key(&ng.asg_name) {
        return;
    }
    let id = uuid::Uuid::new_v4().to_string();
    st.groups.insert(
        ng.asg_name.clone(),
        AutoScalingGroup {
            name: ng.asg_name.clone(),
            arn: fakecloud_autoscaling::autoscaling_arn(
                region,
                account_id,
                "autoScalingGroup",
                &id,
                &ng.asg_name,
            ),
            launch_configuration_name: None,
            launch_template: None,
            min_size: scaling(ng, "minSize", 1),
            max_size: scaling(ng, "maxSize", 2),
            desired_capacity: scaling(ng, "desiredSize", 2),
            default_cooldown: 300,
            availability_zones: zones,
            vpc_zone_identifier: (!subnets.is_empty()).then(|| subnets.join(",")),
            health_check_type: "EC2".to_string(),
            health_check_grace_period: 15,
            target_group_arns: Vec::new(),
            load_balancer_names: Vec::new(),
            new_instances_protected_from_scale_in: false,
            created_time: Utc::now(),
            instances: Vec::new(),
            tags: eks_asg_tags(&ng.cluster_name, &ng.name),
            status: None,
            service_linked_role_arn: fakecloud_autoscaling::service_linked_role_arn(
                region, account_id,
            ),
            mixed_instances_policy: None,
        },
    );
}

/// Apply the node group's scaling config to its Auto Scaling group.
pub fn sync_nodegroup_asg_scaling(asg: &SharedAutoScalingState, account_id: &str, ng: &Nodegroup) {
    let mut guard = asg.write();
    let st = guard.get_or_create(account_id);
    if let Some(group) = st.groups.get_mut(&ng.asg_name) {
        group.min_size = scaling(ng, "minSize", group.min_size);
        group.max_size = scaling(ng, "maxSize", group.max_size);
        group.desired_capacity = scaling(ng, "desiredSize", group.desired_capacity);
    }
}

/// Delete a node group's Auto Scaling group. Only a group still tagged as
/// that node group's is removed.
pub fn delete_nodegroup_asg(asg: &SharedAutoScalingState, account_id: &str, ng: &Nodegroup) {
    let mut guard = asg.write();
    let st = guard.get_or_create(account_id);
    let owned = st.groups.get(&ng.asg_name).is_some_and(|g| {
        g.tags
            .iter()
            .any(|t| t.key == "eks:nodegroup-name" && t.value == ng.name)
    });
    if owned {
        st.groups.remove(&ng.asg_name);
    }
}

/// Back every restored node group with its Auto Scaling group. Node groups
/// persisted before EKS created the group carry a name with nothing behind
/// it. Returns how many groups were created.
pub fn restore_nodegroup_asgs(
    eks: &crate::SharedEksState,
    asg: &SharedAutoScalingState,
    ec2: Option<&SharedEc2State>,
) -> usize {
    let eks = eks.read();
    let mut restored = 0;
    for (account_id, eks_state) in eks.iter() {
        for ng in eks_state.nodegroups.values().flat_map(|m| m.values()) {
            // The node group's ARN carries the region it was created in.
            let region = ng.arn.split(':').nth(3).unwrap_or("us-east-1").to_string();
            let exists = asg
                .read()
                .accounts
                .get(account_id)
                .is_some_and(|st| st.groups.contains_key(&ng.asg_name));
            if !exists {
                create_nodegroup_asg(asg, ec2, account_id, &region, ng);
                restored += 1;
            }
        }
    }
    restored
}
