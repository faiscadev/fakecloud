//! A domain's (or VPC endpoint's) `VPCOptions` against the real EC2 VPC.
//!
//! On AWS the request carries `SubnetIds` / `SecurityGroupIds` and the
//! response's `VPCDerivedInfo` adds the `VPCId` and `AvailabilityZones` those
//! subnets resolve to; omitted security groups become the VPC's `default`
//! group. Shared by the API handlers and the CloudFormation provisioner.

use fakecloud_ec2::vpc_lookup;
use fakecloud_ec2::SharedEc2State;
use serde_json::{json, Value};

fn string_list(v: Option<&Value>) -> Vec<String> {
    v.and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|s| s.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

/// Resolve `opts` (`VPCOptions` / `VpcOptions`) into `VPCDerivedInfo`. `Err`
/// carries the `ValidationException` message.
pub fn derive_vpc_options(
    ec2: &SharedEc2State,
    account_id: &str,
    region: &str,
    opts: &Value,
) -> Result<Value, String> {
    let subnet_ids = string_list(opts.get("SubnetIds"));
    if subnet_ids.is_empty() {
        return Ok(opts.clone());
    }
    let subnets = vpc_lookup::resolve_subnets(ec2, account_id, region, &subnet_ids)
        .map_err(|id| format!("The subnet '{id}' does not exist."))?;
    let vpc_id = subnets[0].vpc_id.clone();
    if subnets.iter().any(|s| s.vpc_id != vpc_id) {
        return Err("The subnets you specified must all be in the same VPC.".to_string());
    }
    let requested_groups = string_list(opts.get("SecurityGroupIds"));
    let groups = if requested_groups.is_empty() {
        vpc_lookup::default_security_group_id(ec2, account_id, region, &vpc_id)
            .into_iter()
            .collect::<Vec<_>>()
    } else {
        let resolved = vpc_lookup::security_group_vpcs(ec2, account_id, region, &requested_groups)
            .map_err(|id| format!("The security group '{id}' does not exist."))?;
        if let Some((id, _)) = resolved.iter().find(|(_, v)| *v != vpc_id) {
            return Err(format!(
                "The security group '{id}' is not in VPC '{vpc_id}' of the specified subnets."
            ));
        }
        requested_groups
    };
    let mut zones: Vec<String> = subnets
        .iter()
        .map(|s| s.availability_zone.clone())
        .collect();
    zones.sort();
    zones.dedup();
    Ok(json!({
        "VPCId": vpc_id,
        "SubnetIds": subnet_ids,
        "AvailabilityZones": zones,
        "SecurityGroupIds": groups,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;
    use std::sync::Arc;

    #[test]
    fn derives_vpc_and_zones_from_subnets() {
        let ec2: SharedEc2State = Arc::new(RwLock::new(MultiAccountState::new(
            "123456789012",
            "us-east-1",
            "",
        )));
        let subnets = vpc_lookup::default_vpc_subnets(&ec2, "123456789012", "us-east-1");
        let sg = vpc_lookup::default_security_group_id(
            &ec2,
            "123456789012",
            "us-east-1",
            &subnets[0].vpc_id,
        )
        .unwrap();
        let out = derive_vpc_options(
            &ec2,
            "123456789012",
            "us-east-1",
            &json!({ "SubnetIds": [subnets[0].subnet_id, subnets[1].subnet_id] }),
        )
        .unwrap();
        assert_eq!(out["VPCId"], subnets[0].vpc_id.as_str());
        assert_eq!(
            out["AvailabilityZones"],
            json!([subnets[0].availability_zone, subnets[1].availability_zone])
        );
        assert_eq!(out["SecurityGroupIds"], json!([sg]));
        assert!(derive_vpc_options(
            &ec2,
            "123456789012",
            "us-east-1",
            &json!({ "SubnetIds": ["subnet-0000dead"] })
        )
        .is_err());
    }
}
