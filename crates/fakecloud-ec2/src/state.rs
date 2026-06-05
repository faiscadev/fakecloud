//! EC2 service state.
//!
//! Partitioned per account+region via [`fakecloud_core::multi_account`]. The
//! `tags` map is keyed by EC2 resource id (e.g. `vpc-…`, `i-…`, `sg-…`) and is
//! the backing store for `CreateTags`/`DeleteTags`/`DescribeTags` plus the
//! `tag:`/`tag-key` describe filters shared across every resource family.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// Shared, account-partitioned EC2 state handle.
pub type SharedEc2State = Arc<RwLock<fakecloud_core::multi_account::MultiAccountState<Ec2State>>>;

impl fakecloud_core::multi_account::AccountState for Ec2State {
    fn new_for_account(account_id: &str, region: &str, _endpoint: &str) -> Self {
        Self::new(account_id, region)
    }
}

/// A single EC2 resource tag.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tag {
    pub key: String,
    pub value: String,
}

/// A secondary CIDR-block association on a VPC.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VpcCidrAssoc {
    pub association_id: String,
    pub cidr_block: String,
    /// `associated` | `disassociated`.
    pub state: String,
}

/// A Virtual Private Cloud.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Vpc {
    pub vpc_id: String,
    pub cidr_block: String,
    /// `pending` | `available`.
    pub state: String,
    pub dhcp_options_id: String,
    /// `default` | `dedicated` | `host`.
    pub instance_tenancy: String,
    pub is_default: bool,
    pub enable_dns_support: bool,
    pub enable_dns_hostnames: bool,
    #[serde(default)]
    pub cidr_associations: Vec<VpcCidrAssoc>,
}

/// One `key -> values` entry in a DHCP options set.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DhcpConfig {
    pub key: String,
    pub values: Vec<String>,
}

/// A DHCP options set.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DhcpOptions {
    pub dhcp_options_id: String,
    pub configurations: Vec<DhcpConfig>,
}

/// Per-account, per-region EC2 state. Resource families are added to this
/// struct as their batches land.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Ec2State {
    pub account_id: String,
    pub region: String,
    /// resource-id -> tags. Shared by every Describe* `tag:` filter.
    #[serde(default)]
    pub tags: HashMap<String, Vec<Tag>>,
    #[serde(default)]
    pub vpcs: HashMap<String, Vpc>,
    #[serde(default)]
    pub dhcp_options: HashMap<String, DhcpOptions>,
}

impl Ec2State {
    pub fn new(account_id: &str, region: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            region: region.to_string(),
            tags: HashMap::new(),
            vpcs: HashMap::new(),
            dhcp_options: HashMap::new(),
        }
    }

    /// Replace the tag set for `resource_id` with `tags` merged over any
    /// existing tags (CreateTags is upsert-by-key, matching AWS).
    pub fn upsert_tags(&mut self, resource_id: &str, new_tags: &[Tag]) {
        let entry = self.tags.entry(resource_id.to_string()).or_default();
        for t in new_tags {
            if let Some(existing) = entry.iter_mut().find(|e| e.key == t.key) {
                existing.value = t.value.clone();
            } else {
                entry.push(t.clone());
            }
        }
    }

    /// Remove tags for `resource_id`. When a tag's value is `None`, the key is
    /// removed regardless of value; when `Some`, only a key+value match is
    /// removed (AWS DeleteTags semantics).
    pub fn remove_tags(&mut self, resource_id: &str, to_remove: &[(String, Option<String>)]) {
        if let Some(entry) = self.tags.get_mut(resource_id) {
            for (key, value) in to_remove {
                entry.retain(|e| {
                    if &e.key != key {
                        return true;
                    }
                    match value {
                        Some(v) => &e.value != v,
                        None => false,
                    }
                });
            }
            if entry.is_empty() {
                self.tags.remove(resource_id);
            }
        }
    }

    /// Tags for `resource_id`, or an empty slice when none.
    pub fn tags_for(&self, resource_id: &str) -> &[Tag] {
        self.tags.get(resource_id).map(Vec::as_slice).unwrap_or(&[])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(k: &str, v: &str) -> Tag {
        Tag {
            key: k.to_string(),
            value: v.to_string(),
        }
    }

    #[test]
    fn upsert_tags_inserts_then_overwrites_by_key() {
        let mut s = Ec2State::new("123456789012", "us-east-1");
        s.upsert_tags("vpc-1", &[tag("Name", "a"), tag("env", "dev")]);
        s.upsert_tags("vpc-1", &[tag("Name", "b")]);
        let tags = s.tags_for("vpc-1");
        assert_eq!(tags.len(), 2);
        assert_eq!(tags.iter().find(|t| t.key == "Name").unwrap().value, "b");
    }

    #[test]
    fn remove_tags_by_key_and_by_key_value() {
        let mut s = Ec2State::new("123456789012", "us-east-1");
        s.upsert_tags(
            "i-1",
            &[tag("Name", "x"), tag("env", "prod"), tag("team", "a")],
        );
        // key-only removal
        s.remove_tags("i-1", &[("Name".to_string(), None)]);
        // key+value removal that does NOT match -> kept
        s.remove_tags("i-1", &[("env".to_string(), Some("dev".to_string()))]);
        // key+value removal that matches -> removed
        s.remove_tags("i-1", &[("team".to_string(), Some("a".to_string()))]);
        let tags = s.tags_for("i-1");
        assert_eq!(tags.len(), 1);
        assert_eq!(tags[0].key, "env");
    }

    #[test]
    fn empty_tag_set_drops_resource_entry() {
        let mut s = Ec2State::new("123456789012", "us-east-1");
        s.upsert_tags("sg-1", &[tag("Name", "x")]);
        s.remove_tags("sg-1", &[("Name".to_string(), None)]);
        assert!(!s.tags.contains_key("sg-1"));
    }
}
