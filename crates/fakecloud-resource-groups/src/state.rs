//! Account-partitioned, serializable state for AWS Resource Groups.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use fakecloud_core::multi_account::{AccountState, MultiAccountState};

pub const RESOURCE_GROUPS_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

/// A resource query attached to a group: either a tag filter or a
/// CloudFormation-stack membership query. `query` is the raw JSON string AWS
/// carries in the `Query` member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceQuery {
    /// `TAG_FILTERS_1_0` or `CLOUDFORMATION_STACK_1_0`.
    pub type_: String,
    pub query: String,
}

/// A resource group. `resources` holds ARNs explicitly associated via
/// `GroupResources` (for configuration/service-linked groups without a query);
/// query-based groups compute membership from their `query`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceGroup {
    pub name: String,
    pub arn: String,
    pub description: Option<String>,
    pub query: Option<ResourceQuery>,
    /// `GroupConfigurationItem` list, stored as raw JSON values.
    pub configuration: Vec<serde_json::Value>,
    pub tags: BTreeMap<String, String>,
    pub criticality: Option<i32>,
    pub owner: Option<String>,
    pub display_name: Option<String>,
    pub application_tag: BTreeMap<String, String>,
    /// ARNs explicitly grouped via `GroupResources`.
    pub resources: BTreeSet<String>,
    pub created_at: DateTime<Utc>,
}

/// A tag-sync task binding a tag key/value to a group so matching resources are
/// auto-associated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TagSyncTask {
    pub task_arn: String,
    pub group_arn: String,
    pub group_name: String,
    pub tag_key: Option<String>,
    pub tag_value: Option<String>,
    pub resource_query: Option<ResourceQuery>,
    pub role_arn: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AccountSettings {
    /// `GroupLifecycleEventsDesiredStatus`: ACTIVE | INACTIVE (default INACTIVE).
    pub desired_status: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResourceGroupsState {
    /// Groups keyed by name.
    pub groups: BTreeMap<String, ResourceGroup>,
    /// Tag-sync tasks keyed by task ARN.
    pub tag_sync_tasks: BTreeMap<String, TagSyncTask>,
    pub account_settings: AccountSettings,
}

impl ResourceGroupsState {
    /// Resolve a group by name or ARN (the `GroupStringV2` member accepts both).
    pub fn resolve_name<'a>(&'a self, name_or_arn: &'a str) -> Option<&'a str> {
        if self.groups.contains_key(name_or_arn) {
            return Some(name_or_arn);
        }
        // Exact stored ARN: arn:aws:resource-groups:region:acct:group/<name>/<uid>
        if let Some(g) = self.groups.values().find(|g| g.arn == name_or_arn) {
            return Some(g.name.as_str());
        }
        // ARN whose `group/` resource part names an existing group, with or without
        // the generated id suffix (`group/<name>` or `group/<name>/<uid>`). Match by
        // the leading name segment so a client-reconstructed ARN still resolves.
        if let Some(resource) = name_or_arn
            .starts_with("arn:")
            .then(|| name_or_arn.split(":group/").nth(1))
            .flatten()
        {
            let group_name = resource.split('/').next().unwrap_or(resource);
            if let Some((stored, _)) = self.groups.get_key_value(group_name) {
                return Some(stored.as_str());
            }
        }
        None
    }
}

impl AccountState for ResourceGroupsState {
    fn new_for_account(_account_id: &str, _region: &str, _endpoint: &str) -> Self {
        Self::default()
    }
}

pub type SharedResourceGroupsState = Arc<RwLock<MultiAccountState<ResourceGroupsState>>>;

#[derive(Debug, Serialize, Deserialize)]
pub struct ResourceGroupsSnapshot {
    pub schema_version: u32,
    pub accounts: MultiAccountState<ResourceGroupsState>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_by_name_and_arn() {
        let mut st = ResourceGroupsState::default();
        st.groups.insert(
            "g1".into(),
            ResourceGroup {
                name: "g1".into(),
                arn: "arn:aws:resource-groups:us-east-1:123456789012:group/g1/abcd".into(),
                description: None,
                query: None,
                configuration: vec![],
                tags: BTreeMap::new(),
                criticality: None,
                owner: None,
                display_name: None,
                application_tag: BTreeMap::new(),
                resources: BTreeSet::new(),
                created_at: Utc::now(),
            },
        );
        assert_eq!(st.resolve_name("g1"), Some("g1"));
        assert_eq!(
            st.resolve_name("arn:aws:resource-groups:us-east-1:123456789012:group/g1/abcd"),
            Some("g1")
        );
        // Client-reconstructed ARN without the generated id suffix still resolves.
        assert_eq!(
            st.resolve_name("arn:aws:resource-groups:us-east-1:123456789012:group/g1"),
            Some("g1")
        );
        // ARN with a different (wrong) id suffix but the right name still resolves.
        assert_eq!(
            st.resolve_name("arn:aws:resource-groups:us-west-2:123456789012:group/g1/zzzz"),
            Some("g1")
        );
        assert_eq!(st.resolve_name("missing"), None);
        assert_eq!(
            st.resolve_name("arn:aws:resource-groups:us-east-1:123456789012:group/missing"),
            None
        );
    }
}
