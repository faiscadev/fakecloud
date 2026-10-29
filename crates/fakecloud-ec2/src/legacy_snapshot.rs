//! Migration of pre-regional (v1) EC2 snapshots.
//!
//! A v1 snapshot holds one account-wide [`Ec2State`] per account. Requests
//! were served from it whatever their region, so one state can hold resources
//! of several regions: a subnet, instance or volume created through a region's
//! endpoint carries that region's Availability Zone. On load each resource is
//! moved to the region it belongs to:
//!
//! - a subnet, instance, network interface or volume to the region of its
//!   zone (a subnet follows its VPC, an instance or interface its subnet, an
//!   attached volume its instance, so a VPC's resources stay together);
//! - a VPC to the region most of its subnets are in, and every VPC-scoped
//!   resource (security groups, route tables, network ACLs, internet and
//!   egress-only gateways, NAT gateways, endpoints, flow logs, peerings) with
//!   its VPC;
//! - snapshots with their volume, Elastic IPs with what they are associated
//!   with (else their network border group), tags with their resource, and
//!   the per-region EBS encryption settings to the region they name.
//!
//! The account's default VPC and everything that names no region stays in
//! the server's configured region, whose state is the legacy state itself (so
//! it keeps the default network it already has). Every other region starts
//! from a fresh regional state, with that region's own default network.

use std::collections::{BTreeMap, HashMap};

use fakecloud_core::multi_account::RegionalState;

use crate::state::Ec2State;

/// The region an Availability Zone name belongs to (`eu-west-1b ->
/// eu-west-1`), `None` for anything not zone-shaped.
fn zone_region(zone: &str) -> Option<&str> {
    let region = zone.strip_suffix(|c: char| c.is_ascii_lowercase())?;
    (region.contains('-') && region.ends_with(|c: char| c.is_ascii_digit())).then_some(region)
}

/// Resource id -> region, for the resources that leave the home region.
struct Placement {
    home: String,
    regions: HashMap<String, String>,
}

impl Placement {
    fn of(&self, id: &str) -> Option<&str> {
        self.regions.get(id).map(String::as_str)
    }

    /// The region of `id` if it was placed, else `fallback`, else home.
    fn follow(&self, id: Option<&str>, fallback: Option<&str>) -> String {
        id.and_then(|i| self.of(i))
            .or(fallback)
            .unwrap_or(&self.home)
            .to_string()
    }

    fn place(&mut self, id: &str, region: String) {
        if region != self.home {
            self.regions.insert(id.to_string(), region);
        }
    }
}

/// The region most of a VPC's subnets are in; the home region on a tie with
/// it, or when no subnet names one.
fn vpc_regions(legacy: &Ec2State, home: &str) -> HashMap<String, String> {
    let mut votes: HashMap<&str, BTreeMap<&str, usize>> = HashMap::new();
    for s in legacy.subnets.values() {
        if let Some(r) = zone_region(&s.availability_zone) {
            *votes
                .entry(s.vpc_id.as_str())
                .or_default()
                .entry(r)
                .or_default() += 1;
        }
    }
    legacy
        .vpcs
        .values()
        .map(|vpc| {
            let region = if vpc.is_default {
                home.to_string()
            } else {
                votes
                    .get(vpc.vpc_id.as_str())
                    .and_then(|v| {
                        let max = *v.values().max()?;
                        if v.get(home) == Some(&max) {
                            return Some(home);
                        }
                        v.iter().find(|(_, n)| **n == max).map(|(r, _)| *r)
                    })
                    .unwrap_or(home)
                    .to_string()
            };
            (vpc.vpc_id.clone(), region)
        })
        .collect()
}

/// Move every entry of `$field` the placement sends to another region.
macro_rules! move_placed {
    ($legacy:ident, $into:ident, $placement:ident, $($field:ident),+ $(,)?) => {
        $(
            let ids: Vec<(String, String)> = $legacy
                .$field
                .keys()
                .filter_map(|id| $placement.of(id).map(|r| (id.clone(), r.to_string())))
                .collect();
            for (id, region) in ids {
                if let Some(v) = $legacy.$field.remove(&id) {
                    $into.region_mut(&region).$field.insert(id, v);
                }
            }
        )+
    };
}

/// Split one account's legacy, account-wide state into `into`'s regions.
pub(crate) fn split_legacy(mut legacy: Ec2State, into: &mut RegionalState<Ec2State>) {
    let home = into.default_region().to_string();
    let mut p = Placement {
        home: home.clone(),
        regions: HashMap::new(),
    };

    let vpc_region = vpc_regions(&legacy, &home);
    for (vpc, region) in &vpc_region {
        p.place(vpc, region.clone());
    }
    let by_vpc = |vpc: &str| vpc_region.get(vpc).cloned();

    for s in legacy.subnets.values() {
        let region = by_vpc(&s.vpc_id)
            .or_else(|| zone_region(&s.availability_zone).map(str::to_string))
            .unwrap_or_else(|| home.clone());
        p.place(&s.subnet_id, region);
    }
    for i in legacy.instances.values() {
        let region = p.follow(
            i.subnet_id.as_deref(),
            i.vpc_id
                .as_deref()
                .and_then(|v| vpc_region.get(v).map(String::as_str))
                .or_else(|| zone_region(&i.az)),
        );
        p.place(&i.instance_id, region);
    }
    for e in legacy.network_interfaces.values() {
        let region = p.follow(
            Some(&e.subnet_id),
            vpc_region
                .get(&e.vpc_id)
                .map(String::as_str)
                .or_else(|| zone_region(&e.availability_zone)),
        );
        p.place(&e.network_interface_id, region);
    }
    for v in legacy.volumes.values() {
        let attached = v.attachments.first().map(|a| a.instance_id.as_str());
        let region = p.follow(attached, zone_region(&v.availability_zone));
        p.place(&v.volume_id, region);
    }

    // Children follow their parent.
    let vpc_child = |p: &Placement, vpc: &str| p.follow(Some(vpc), None);
    let mut placed: Vec<(String, String)> = Vec::new();
    placed.extend(
        legacy
            .security_groups
            .iter()
            .map(|(id, g)| (id.clone(), vpc_child(&p, &g.vpc_id))),
    );
    placed.extend(
        legacy
            .route_tables
            .iter()
            .map(|(id, t)| (id.clone(), vpc_child(&p, &t.vpc_id))),
    );
    placed.extend(
        legacy
            .network_acls
            .iter()
            .map(|(id, a)| (id.clone(), vpc_child(&p, &a.vpc_id))),
    );
    placed.extend(
        legacy
            .vpc_endpoints
            .iter()
            .map(|(id, e)| (id.clone(), vpc_child(&p, &e.vpc_id))),
    );
    placed.extend(
        legacy
            .nat_gateways
            .iter()
            .map(|(id, n)| (id.clone(), p.follow(Some(&n.subnet_id), p.of(&n.vpc_id)))),
    );
    placed.extend(
        legacy
            .vpc_peerings
            .iter()
            .map(|(id, x)| (id.clone(), vpc_child(&p, &x.requester_vpc_id))),
    );
    for igws in [&legacy.internet_gateways, &legacy.egress_only_igws] {
        placed.extend(igws.iter().map(|(id, g)| {
            let vpc = g.attachments.first().map(|(v, _)| v.as_str());
            (id.clone(), p.follow(vpc, None))
        }));
    }
    placed.extend(
        legacy
            .subnet_cidr_reservations
            .iter()
            .map(|(id, r)| (id.clone(), p.follow(Some(&r.subnet_id), None))),
    );
    placed.extend(
        legacy
            .flow_logs
            .iter()
            .map(|(id, f)| (id.clone(), p.follow(Some(&f.resource_id), None))),
    );
    placed.extend(
        legacy
            .snapshots
            .iter()
            .map(|(id, s)| (id.clone(), p.follow(Some(&s.volume_id), None))),
    );
    placed.extend(legacy.elastic_ips.iter().map(|(id, e)| {
        let owner = e
            .instance_id
            .as_deref()
            .filter(|i| p.of(i).is_some())
            .or(e.network_interface_id.as_deref());
        let region = p.follow(
            owner,
            Some(e.network_border_group.as_str()).filter(|g| !g.is_empty()),
        );
        (id.clone(), region)
    }));
    placed.extend(
        legacy
            .eni_permissions
            .iter()
            .map(|(id, x)| (id.clone(), p.follow(Some(&x.network_interface_id), None))),
    );
    for (id, region) in placed {
        p.place(&id, region);
    }

    move_placed!(
        legacy,
        into,
        p,
        vpcs,
        subnets,
        instances,
        network_interfaces,
        volumes,
        security_groups,
        route_tables,
        network_acls,
        vpc_endpoints,
        nat_gateways,
        vpc_peerings,
        internet_gateways,
        egress_only_igws,
        subnet_cidr_reservations,
        flow_logs,
        snapshots,
        elastic_ips,
        eni_permissions,
        tags,
    );

    // Per-region EBS encryption settings live in the region they name.
    let other_regions: Vec<String> = legacy
        .ebs_encryption_default_by_region
        .keys()
        .chain(legacy.ebs_default_kms_key_id_by_region.keys())
        .filter(|r| **r != home)
        .cloned()
        .collect();
    for region in other_regions {
        let enc = legacy.ebs_encryption_default_by_region.remove(&region);
        let key = legacy.ebs_default_kms_key_id_by_region.remove(&region);
        let target = into.region_mut(&region);
        if let Some(enc) = enc {
            target
                .ebs_encryption_default_by_region
                .insert(region.clone(), enc);
        }
        if let Some(key) = key {
            target
                .ebs_default_kms_key_id_by_region
                .insert(region.clone(), key);
        }
    }

    legacy.region = home.clone();
    into.insert_region(&home, legacy);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{Instance, Subnet, Tag, Volume, Vpc};
    use fakecloud_core::multi_account::MultiAccountState;

    const ACCOUNT: &str = "123456789012";

    fn vpc(id: &str) -> Vpc {
        serde_json::from_value(serde_json::json!({
            "vpc_id": id, "cidr_block": "10.0.0.0/16", "state": "available",
            "dhcp_options_id": "default", "instance_tenancy": "default",
            "is_default": false, "enable_dns_support": true,
            "enable_dns_hostnames": false,
        }))
        .unwrap()
    }

    fn subnet(id: &str, vpc: &str, az: &str) -> Subnet {
        serde_json::from_value(serde_json::json!({
            "subnet_id": id, "vpc_id": vpc, "cidr_block": "10.0.1.0/24",
            "availability_zone": az, "availability_zone_id": "x", "state": "available",
            "available_ip_address_count": 251, "default_for_az": false,
            "map_public_ip_on_launch": false, "assign_ipv6_address_on_creation": false,
            "map_customer_owned_ip_on_launch": false, "enable_dns64": false,
            "private_dns_hostname_type": "ip-name",
        }))
        .unwrap()
    }

    fn instance(id: &str, subnet: &str, vpc: &str, az: &str) -> Instance {
        serde_json::from_value(serde_json::json!({
            "instance_id": id, "image_id": "ami-1", "instance_type": "t3.micro",
            "state_code": 16, "state_name": "running", "private_ip": "10.0.1.5",
            "subnet_id": subnet, "vpc_id": vpc, "reservation_id": "r-1",
            "ami_launch_index": 0, "monitoring": false, "az": az,
            "launch_time": "2024-01-01T00:00:00.000Z",
        }))
        .unwrap()
    }

    fn volume(id: &str, az: &str) -> Volume {
        serde_json::from_value(serde_json::json!({
            "volume_id": id, "size": 8, "availability_zone": az, "state": "available",
            "volume_type": "gp2", "encrypted": false, "multi_attach_enabled": false,
            "auto_enable_io": false,
        }))
        .unwrap()
    }

    /// A v1 snapshot with resources created through two regions' endpoints
    /// lands each in its region; nothing is dropped.
    #[test]
    fn v1_snapshot_splits_resources_by_region() {
        // The legacy, account-wide state (what a v1 snapshot holds).
        let mut legacy = Ec2State::new(ACCOUNT, "us-east-1");
        let legacy_default_vpc = legacy
            .vpcs
            .values()
            .find(|v| v.is_default)
            .unwrap()
            .vpc_id
            .clone();
        legacy.vpcs.insert("vpc-east".into(), vpc("vpc-east"));
        legacy.vpcs.insert("vpc-west".into(), vpc("vpc-west"));
        legacy.vpcs.insert("vpc-bare".into(), vpc("vpc-bare"));
        legacy.subnets.insert(
            "subnet-east".into(),
            subnet("subnet-east", "vpc-east", "us-east-1b"),
        );
        legacy.subnets.insert(
            "subnet-west".into(),
            subnet("subnet-west", "vpc-west", "eu-west-1a"),
        );
        crate::defaults::create_vpc_default_resources(&mut legacy, "vpc-west", "10.0.0.0/16");
        let west_sgs: Vec<String> = legacy
            .security_groups
            .values()
            .filter(|g| g.vpc_id == "vpc-west")
            .map(|g| g.group_id.clone())
            .collect();
        assert!(!west_sgs.is_empty());
        legacy.instances.insert(
            "i-west".into(),
            instance("i-west", "subnet-west", "vpc-west", "eu-west-1a"),
        );
        legacy.instances.insert(
            "i-east".into(),
            instance("i-east", "subnet-east", "vpc-east", "us-east-1b"),
        );
        legacy
            .volumes
            .insert("vol-west".into(), volume("vol-west", "eu-west-1c"));
        legacy
            .volumes
            .insert("vol-east".into(), volume("vol-east", "us-east-1a"));
        legacy.upsert_tags(
            "i-west",
            &[Tag {
                key: "team".into(),
                value: "w".into(),
            }],
        );
        legacy.set_ebs_encryption_by_default("eu-west-1", true);
        legacy.key_pairs.insert(
            "kp".into(),
            crate::state::KeyPair {
                key_pair_id: "key-1".into(),
                key_name: "kp".into(),
                key_type: "rsa".into(),
                key_fingerprint: "f".into(),
            },
        );
        let total = |s: &Ec2State| {
            s.vpcs.len()
                + s.subnets.len()
                + s.instances.len()
                + s.volumes.len()
                + s.security_groups.len()
                + s.route_tables.len()
                + s.network_acls.len()
                + s.internet_gateways.len()
                + s.key_pairs.len()
        };
        let legacy_total = total(&legacy);

        let mut accounts: MultiAccountState<Ec2State> =
            MultiAccountState::new("000000000000", "us-east-1", "");
        *accounts.get_or_create(ACCOUNT) = legacy;
        let bytes = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "accounts": accounts,
        }))
        .unwrap();
        let snap =
            crate::state::parse_ec2_snapshot(&bytes, "000000000000", "us-east-1", "").unwrap();
        assert_eq!(
            snap.schema_version,
            crate::state::EC2_SNAPSHOT_SCHEMA_VERSION
        );
        let migrated = snap.accounts.unwrap();
        let east = migrated.regional(ACCOUNT, "us-east-1").unwrap();
        let west = migrated.regional(ACCOUNT, "eu-west-1").unwrap();
        assert_eq!(east.region, "us-east-1");
        assert_eq!(west.region, "eu-west-1");

        // eu-west-1 resources moved, with their VPC's children, tags and
        // the region's EBS setting.
        assert!(west.vpcs.contains_key("vpc-west") && !east.vpcs.contains_key("vpc-west"));
        assert!(west.subnets.contains_key("subnet-west"));
        assert!(west.instances.contains_key("i-west"));
        assert!(west.volumes.contains_key("vol-west"));
        for sg in &west_sgs {
            assert!(west.security_groups.contains_key(sg), "{sg}");
            assert!(!east.security_groups.contains_key(sg), "{sg}");
        }
        assert_eq!(west.tags_for("i-west").len(), 1);
        assert!(west.ebs_encryption_by_default("eu-west-1"));

        // us-east-1 keeps its own resources, the legacy default VPC and
        // everything region-less.
        assert!(east.vpcs.contains_key("vpc-east"));
        assert!(east.vpcs.contains_key("vpc-bare"));
        assert!(east.vpcs.contains_key(&legacy_default_vpc));
        assert!(east.subnets.contains_key("subnet-east"));
        assert!(east.instances.contains_key("i-east"));
        assert!(east.volumes.contains_key("vol-east"));
        assert!(east.key_pairs.contains_key("kp"));
        assert!(!east.instances.contains_key("i-west"));

        // Nothing dropped: everything the legacy state held is in one of the
        // two regions; eu-west-1 additionally has its own fresh default
        // network.
        let fresh_west = total(&Ec2State::new(ACCOUNT, "eu-west-1"));
        assert_eq!(total(east) + total(west) - fresh_west, legacy_total);
        // eu-west-1's own default VPC is not the legacy one.
        assert!(!west.vpcs.contains_key(&legacy_default_vpc));
        assert_eq!(west.vpcs.values().filter(|v| v.is_default).count(), 1);
    }

    /// Region-less legacy resources go to the server's configured region,
    /// not us-east-1, and the container is routed with the server's account
    /// and region.
    #[test]
    fn v1_snapshot_falls_back_to_configured_region() {
        let mut legacy = Ec2State::new(ACCOUNT, "us-east-1");
        legacy.vpcs.insert("vpc-bare".into(), vpc("vpc-bare"));
        let mut accounts: MultiAccountState<Ec2State> =
            MultiAccountState::new("000000000000", "us-east-1", "");
        *accounts.get_or_create(ACCOUNT) = legacy;
        let bytes = serde_json::to_vec(&serde_json::json!({
            "schema_version": 1,
            "accounts": accounts,
        }))
        .unwrap();
        let snap = crate::state::parse_ec2_snapshot(&bytes, ACCOUNT, "ap-south-1", "").unwrap();
        let migrated = snap.accounts.unwrap();
        assert_eq!(migrated.region(), "ap-south-1");
        assert_eq!(migrated.default_account_id(), ACCOUNT);
        let south = migrated.regional(ACCOUNT, "ap-south-1").unwrap();
        assert!(south.vpcs.contains_key("vpc-bare"));
        assert_eq!(south.region, "ap-south-1");
        assert!(migrated.regional(ACCOUNT, "us-east-1").is_none());
        // The legacy container's other account is kept too.
        assert!(migrated.get("000000000000").is_some());
    }

    #[test]
    fn zone_region_parses_zone_names_only() {
        assert_eq!(zone_region("eu-west-1b"), Some("eu-west-1"));
        assert_eq!(zone_region("us-gov-west-1a"), Some("us-gov-west-1"));
        assert_eq!(zone_region("us-east-1"), None);
        assert_eq!(zone_region(""), None);
        assert_eq!(zone_region("x"), None);
    }
}
