//! In-memory state for EC2 Auto Scaling.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub type SharedAutoScalingState = Arc<RwLock<AutoScalingAccounts>>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AutoScalingAccounts {
    pub accounts: BTreeMap<String, AccountState>,
}

impl AutoScalingAccounts {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_create(&mut self, account_id: &str) -> &mut AccountState {
        self.accounts.entry(account_id.to_string()).or_default()
    }
}

/// Versioned on-disk persistence snapshot for EC2 Auto Scaling.
#[derive(Debug, Serialize, Deserialize)]
pub struct AutoScalingSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<AutoScalingAccounts>,
}

pub const AUTOSCALING_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AccountState {
    /// Launch configurations keyed by name.
    pub launch_configurations: BTreeMap<String, LaunchConfiguration>,
    /// Auto Scaling groups keyed by name.
    pub groups: BTreeMap<String, AutoScalingGroup>,
    /// Scaling activities, newest first.
    pub activities: Vec<ScalingActivity>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchConfiguration {
    pub name: String,
    pub arn: String,
    pub image_id: String,
    pub instance_type: String,
    #[serde(default)]
    pub key_name: Option<String>,
    #[serde(default)]
    pub security_groups: Vec<String>,
    #[serde(default)]
    pub user_data: Option<String>,
    #[serde(default)]
    pub iam_instance_profile: Option<String>,
    #[serde(default)]
    pub associate_public_ip_address: Option<bool>,
    pub created_time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoScalingGroup {
    pub name: String,
    pub arn: String,
    #[serde(default)]
    pub launch_configuration_name: Option<String>,
    /// (LaunchTemplateId, LaunchTemplateName, Version) when launched from a
    /// launch template instead of a launch configuration.
    #[serde(default)]
    pub launch_template: Option<LaunchTemplateSpec>,
    pub min_size: i64,
    pub max_size: i64,
    pub desired_capacity: i64,
    pub default_cooldown: i64,
    #[serde(default)]
    pub availability_zones: Vec<String>,
    /// `VPCZoneIdentifier` — comma-separated subnet ids.
    #[serde(default)]
    pub vpc_zone_identifier: Option<String>,
    pub health_check_type: String,
    pub health_check_grace_period: i64,
    #[serde(default)]
    pub target_group_arns: Vec<String>,
    #[serde(default)]
    pub load_balancer_names: Vec<String>,
    #[serde(default)]
    pub new_instances_protected_from_scale_in: bool,
    pub created_time: DateTime<Utc>,
    #[serde(default)]
    pub instances: Vec<AsgInstance>,
    /// ASG tags (propagate-at-launch tracked per tag).
    #[serde(default)]
    pub tags: Vec<AsgTag>,
    /// Set during a DeleteAutoScalingGroup that is draining instances.
    #[serde(default)]
    pub status: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LaunchTemplateSpec {
    #[serde(default)]
    pub launch_template_id: Option<String>,
    #[serde(default)]
    pub launch_template_name: Option<String>,
    #[serde(default)]
    pub version: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsgInstance {
    pub instance_id: String,
    pub availability_zone: String,
    /// `Pending` | `InService` | `Terminating` | `Terminated`.
    pub lifecycle_state: String,
    /// `Healthy` | `Unhealthy`.
    pub health_status: String,
    #[serde(default)]
    pub launch_configuration_name: Option<String>,
    #[serde(default)]
    pub protected_from_scale_in: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsgTag {
    pub key: String,
    pub value: String,
    #[serde(default)]
    pub propagate_at_launch: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingActivity {
    pub activity_id: String,
    pub auto_scaling_group_name: String,
    pub description: String,
    pub cause: String,
    pub start_time: DateTime<Utc>,
    #[serde(default)]
    pub end_time: Option<DateTime<Utc>>,
    /// `Successful` | `InProgress` | `Failed`.
    pub status_code: String,
    pub progress: i64,
    #[serde(default)]
    pub details: String,
}
