//! The EKS-managed cluster security group.
//!
//! `CreateCluster` creates a security group in the cluster's VPC and returns it
//! as `resourcesVpcConfig.clusterSecurityGroupId`; `DeleteCluster` deletes it.
//! Tooling (the `terraform-aws-modules/eks` module, Karpenter discovery tags,
//! `aws_security_group_rule` against the primary group) reads and tags that
//! group through EC2, so it has to be a real EC2 record, not just an id.

use fakecloud_ec2::state::{SecurityGroup, SecurityGroupRule, Tag};
use fakecloud_ec2::SharedEc2State;

/// The description AWS gives every EKS cluster security group.
pub const CLUSTER_SECURITY_GROUP_DESCRIPTION: &str = "EKS created security group applied to ENI that is attached to EKS Control Plane master nodes, as well as any managed workloads.";

/// Where a new cluster's control-plane networking lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterNetwork {
    pub vpc_id: String,
    pub cluster_security_group_id: String,
}

/// Create the cluster security group for `cluster_name` in EC2 and return its
/// id with the cluster's VPC.
///
/// The VPC is the one owning the first of `subnet_ids` EC2 knows about, as on
/// AWS. When none of the subnets exist in EC2 (callers passing placeholder
/// subnet ids), the group is created in `fallback_vpc_id` so the returned id
/// still resolves through `DescribeSecurityGroups` / `CreateTags`.
///
/// The group mirrors what EKS creates: named `eks-cluster-sg-<cluster>-<id>`,
/// tagged `Name`, `aws:eks:cluster-name` and `kubernetes.io/cluster/<cluster>=owned`,
/// with an all-traffic ingress rule from itself and an all-traffic egress rule.
pub fn create_cluster_security_group(
    ec2: &SharedEc2State,
    account_id: &str,
    region: &str,
    cluster_name: &str,
    subnet_ids: &[String],
    fallback_vpc_id: &str,
) -> ClusterNetwork {
    let mut accounts = ec2.write();
    let state = accounts.regional_mut(account_id, region);
    let vpc_id = subnet_ids
        .iter()
        .find_map(|id| state.subnets.get(id).map(|s| s.vpc_id.clone()))
        .unwrap_or_else(|| fallback_vpc_id.to_string());

    let group_id = fakecloud_ec2::service_helpers::gen_id("sg");
    insert_cluster_security_group(state, account_id, cluster_name, &group_id, &vpc_id);
    ClusterNetwork {
        vpc_id,
        cluster_security_group_id: group_id,
    }
}

/// Record the cluster security group `group_id` for `cluster_name` in one
/// account's EC2 state: named `eks-cluster-sg-<cluster>-<id>` and tagged
/// `Name`, `aws:eks:cluster-name` and `kubernetes.io/cluster/<cluster>=owned`,
/// with an all-traffic ingress rule from itself and an all-traffic egress rule.
fn insert_cluster_security_group(
    state: &mut fakecloud_ec2::state::Ec2State,
    account_id: &str,
    cluster_name: &str,
    group_id: &str,
    vpc_id: &str,
) {
    let suffix = uuid::Uuid::new_v4().as_u128() % 10_000_000_000;
    let group_name = format!("eks-cluster-sg-{cluster_name}-{suffix:010}");
    let all_traffic = |is_egress: bool| SecurityGroupRule {
        rule_id: fakecloud_ec2::service_helpers::gen_id("sgr"),
        group_id: group_id.to_string(),
        is_egress,
        ip_protocol: "-1".to_string(),
        from_port: -1,
        to_port: -1,
        cidr_ipv4: is_egress.then(|| "0.0.0.0/0".to_string()),
        cidr_ipv6: None,
        prefix_list_id: None,
        referenced_group_id: (!is_egress).then(|| group_id.to_string()),
        referenced_group_name: None,
        referenced_user_id: (!is_egress).then(|| account_id.to_string()),
        description: String::new(),
    };
    let rules = vec![all_traffic(false), all_traffic(true)];
    state.security_groups.insert(
        group_id.to_string(),
        SecurityGroup {
            group_id: group_id.to_string(),
            group_name: group_name.clone(),
            description: CLUSTER_SECURITY_GROUP_DESCRIPTION.to_string(),
            vpc_id: vpc_id.to_string(),
            rules,
        },
    );
    state.tags.insert(
        group_id.to_string(),
        vec![
            Tag {
                key: "Name".to_string(),
                value: group_name,
            },
            Tag {
                key: "aws:eks:cluster-name".to_string(),
                value: cluster_name.to_string(),
            },
            Tag {
                key: format!("kubernetes.io/cluster/{cluster_name}"),
                value: "owned".to_string(),
            },
        ],
    );
}

/// Back the cluster security group of every restored cluster with an EC2
/// record. Clusters persisted before EKS created the group in EC2 carry a
/// group id with nothing behind it; recreate the group under that same id so
/// `DescribeSecurityGroups` / `CreateTags` resolve it. Returns how many groups
/// were recreated.
pub fn restore_cluster_security_groups(eks: &crate::SharedEksState, ec2: &SharedEc2State) -> usize {
    let eks = eks.read();
    let mut accounts = ec2.write();
    let mut restored = 0;
    for (account_id, eks_state) in eks.iter() {
        for cluster in eks_state.clusters.values() {
            if cluster.connector_config.is_some() {
                continue;
            }
            let vpc = &cluster.resources_vpc_config;
            let (Some(group_id), Some(vpc_id)) = (
                vpc["clusterSecurityGroupId"].as_str(),
                vpc["vpcId"].as_str(),
            ) else {
                continue;
            };
            // A cluster ARN names its region; a record without one belongs to
            // the server's configured region.
            let region = fakecloud_aws::arn::region_of(&cluster.arn)
                .map(str::to_string)
                .unwrap_or_else(|| accounts.region().to_string());
            let state = accounts.regional_mut(account_id, &region);
            if state.security_groups.contains_key(group_id) {
                continue;
            }
            insert_cluster_security_group(state, account_id, &cluster.name, group_id, vpc_id);
            restored += 1;
        }
    }
    restored
}

/// Delete the cluster security group EKS created for `cluster_name`. Only a
/// group still tagged as that cluster's is removed, so an id that was never
/// EKS-managed (or was re-used) is left alone.
pub fn delete_cluster_security_group(
    ec2: &SharedEc2State,
    account_id: &str,
    region: &str,
    cluster_name: &str,
    group_id: &str,
) {
    let mut accounts = ec2.write();
    let state = accounts.regional_mut(account_id, region);
    let owned = state.tags.get(group_id).is_some_and(|tags| {
        tags.iter()
            .any(|t| t.key == "aws:eks:cluster-name" && t.value == cluster_name)
    });
    if owned {
        state.security_groups.remove(group_id);
        state.tags.remove(group_id);
    }
}
