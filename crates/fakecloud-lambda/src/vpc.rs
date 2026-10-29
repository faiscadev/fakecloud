//! A function's `VpcConfig` against the real EC2 VPC.
//!
//! On AWS, `CreateFunction` / `UpdateFunctionConfiguration` look the subnets
//! and security groups up in EC2, reject ones that do not exist or that span
//! VPCs, and report the VPC they share as `VpcConfig.VpcId`. Shared by the API
//! handlers and the CloudFormation provisioner.

use fakecloud_ec2::vpc_lookup;
use fakecloud_ec2::SharedEc2State;
use serde_json::Value;

fn string_list(v: &Value) -> Vec<String> {
    v.as_array()
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Validate `cfg`'s `SubnetIds` / `SecurityGroupIds` against EC2 and return it
/// with `VpcId` set to the VPC they belong to (empty when the function is
/// detached from any VPC). `Err` carries the `InvalidParameterValueException`
/// message Lambda returns.
pub fn resolve_vpc_config(
    ec2: &SharedEc2State,
    account_id: &str,
    region: &str,
    cfg: &Value,
) -> Result<Value, String> {
    let subnet_ids = string_list(&cfg["SubnetIds"]);
    let group_ids = string_list(&cfg["SecurityGroupIds"]);
    let mut out = cfg.clone();
    if !out.is_object() {
        out = Value::Object(serde_json::Map::new());
    }
    out["SubnetIds"] = Value::from(subnet_ids.clone());
    out["SecurityGroupIds"] = Value::from(group_ids.clone());
    if subnet_ids.is_empty() && group_ids.is_empty() {
        out["VpcId"] = Value::String(String::new());
        return Ok(out);
    }
    let subnets =
        vpc_lookup::resolve_subnets(ec2, account_id, region, &subnet_ids).map_err(|id| {
            format!(
                "Error occurred while DescribeSubnets. EC2 Error Code: InvalidSubnetID.NotFound. \
             EC2 Error Message: The subnet ID '{id}' does not exist"
            )
        })?;
    let groups =
        vpc_lookup::security_group_vpcs(ec2, account_id, region, &group_ids).map_err(|id| {
            format!(
            "Error occurred while DescribeSecurityGroups. EC2 Error Code: InvalidGroup.NotFound. \
             EC2 Error Message: The security group '{id}' does not exist"
        )
        })?;
    let vpc_id = subnets
        .first()
        .map(|s| s.vpc_id.clone())
        .or_else(|| groups.first().map(|(_, v)| v.clone()))
        .unwrap_or_default();
    if subnets.iter().any(|s| s.vpc_id != vpc_id) || groups.iter().any(|(_, v)| *v != vpc_id) {
        return Err("Security groups and subnets must all belong to the same VPC.".to_string());
    }
    out["VpcId"] = Value::String(vpc_id);
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiRegionState;
    use parking_lot::RwLock;
    use serde_json::json;
    use std::sync::Arc;

    #[test]
    fn vpc_config_reports_the_subnets_vpc_and_rejects_unknown_ids() {
        let ec2: SharedEc2State = Arc::new(RwLock::new(MultiRegionState::new(
            "123456789012",
            "us-east-1",
            "",
        )));
        let subnet = vpc_lookup::default_vpc_subnets(&ec2, "123456789012", "us-east-1").remove(0);
        let sg = vpc_lookup::default_security_group_id(
            &ec2,
            "123456789012",
            "us-east-1",
            &subnet.vpc_id,
        )
        .unwrap();
        let out = resolve_vpc_config(
            &ec2,
            "123456789012",
            "us-east-1",
            &json!({ "SubnetIds": [subnet.subnet_id], "SecurityGroupIds": [sg] }),
        )
        .unwrap();
        assert_eq!(out["VpcId"], subnet.vpc_id.as_str());

        let err = resolve_vpc_config(
            &ec2,
            "123456789012",
            "us-east-1",
            &json!({ "SubnetIds": ["subnet-0000dead"], "SecurityGroupIds": [] }),
        )
        .unwrap_err();
        assert!(err.contains("InvalidSubnetID.NotFound"), "{err}");
        let err = resolve_vpc_config(
            &ec2,
            "123456789012",
            "us-east-1",
            &json!({ "SubnetIds": [subnet.subnet_id], "SecurityGroupIds": ["sg-0000dead"] }),
        )
        .unwrap_err();
        assert!(err.contains("InvalidGroup.NotFound"), "{err}");

        // Detaching from the VPC reports an empty VpcId.
        let out = resolve_vpc_config(
            &ec2,
            "123456789012",
            "us-east-1",
            &json!({ "SubnetIds": [], "SecurityGroupIds": [] }),
        )
        .unwrap();
        assert_eq!(out["VpcId"], "");
    }
}
