//! In-memory state for Application Auto Scaling.

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

pub type SharedApplicationAutoScalingState = Arc<RwLock<ApplicationAutoScalingAccounts>>;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ApplicationAutoScalingAccounts {
    pub accounts: BTreeMap<String, AccountState>,
}

impl ApplicationAutoScalingAccounts {
    pub fn new() -> Self {
        Self::default()
    }
}

/// Versioned on-disk persistence snapshot for Application Auto Scaling.
#[derive(Debug, Serialize, Deserialize)]
pub struct ApplicationAutoScalingSnapshot {
    pub schema_version: u32,
    #[serde(default)]
    pub accounts: Option<ApplicationAutoScalingAccounts>,
}

pub const APPLICATION_AUTOSCALING_SNAPSHOT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct AccountState {
    /// Keyed by (ServiceNamespace, ResourceId, ScalableDimension).
    #[serde(with = "map_as_pairs")]
    pub scalable_targets: BTreeMap<TargetKey, ScalableTarget>,
    /// Keyed by (ServiceNamespace, ResourceId, ScalableDimension, PolicyName).
    #[serde(with = "map_as_pairs")]
    pub scaling_policies: BTreeMap<PolicyKey, ScalingPolicy>,
    /// Keyed by (ServiceNamespace, ResourceId, ScalableDimension, ScheduledActionName).
    #[serde(with = "map_as_pairs")]
    pub scheduled_actions: BTreeMap<ScheduledKey, ScheduledAction>,
    /// Scaling activities, newest first.
    pub scaling_activities: Vec<ScalingActivity>,
    /// Tags keyed by ARN.
    pub tags: BTreeMap<String, BTreeMap<String, String>>,
}

/// Serialize a `BTreeMap` with a non-string (tuple) key as a sequence of
/// `[key, value]` pairs. JSON object keys must be strings, so the tuple-keyed
/// maps above can't serialize directly — this round-trips them via a list,
/// which is what the persistence snapshot needs.
mod map_as_pairs {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;

    pub fn serialize<K, V, S>(map: &BTreeMap<K, V>, ser: S) -> Result<S::Ok, S::Error>
    where
        K: Serialize + Ord,
        V: Serialize,
        S: Serializer,
    {
        ser.collect_seq(map.iter())
    }

    pub fn deserialize<'de, K, V, D>(de: D) -> Result<BTreeMap<K, V>, D::Error>
    where
        K: Deserialize<'de> + Ord,
        V: Deserialize<'de>,
        D: Deserializer<'de>,
    {
        let pairs: Vec<(K, V)> = Vec::deserialize(de)?;
        Ok(pairs.into_iter().collect())
    }
}

pub type TargetKey = (String, String, String);
pub type PolicyKey = (String, String, String, String);
pub type ScheduledKey = (String, String, String, String);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalableTarget {
    pub arn: String,
    pub service_namespace: String,
    pub resource_id: String,
    pub scalable_dimension: String,
    pub min_capacity: i32,
    pub max_capacity: i32,
    pub role_arn: String,
    pub creation_time: DateTime<Utc>,
    pub suspended_state: Option<SuspendedState>,
    pub predicted_capacity: Option<i32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct SuspendedState {
    pub dynamic_scaling_in_suspended: Option<bool>,
    pub dynamic_scaling_out_suspended: Option<bool>,
    pub scheduled_scaling_suspended: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingPolicy {
    pub arn: String,
    pub policy_name: String,
    pub service_namespace: String,
    pub resource_id: String,
    pub scalable_dimension: String,
    pub policy_type: String,
    pub creation_time: DateTime<Utc>,
    pub step_scaling_policy_configuration: Option<serde_json::Value>,
    pub target_tracking_scaling_policy_configuration: Option<serde_json::Value>,
    pub predictive_scaling_policy_configuration: Option<serde_json::Value>,
    pub alarms: Vec<Alarm>,
    /// Last time the watcher applied a scale action through this policy.
    /// Honours the policy's cooldown so we don't thrash capacity.
    #[serde(default)]
    pub last_applied_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Alarm {
    pub alarm_name: String,
    pub alarm_arn: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledAction {
    pub arn: String,
    pub scheduled_action_name: String,
    pub service_namespace: String,
    pub resource_id: String,
    pub scalable_dimension: Option<String>,
    pub schedule: String,
    pub timezone: Option<String>,
    pub start_time: Option<DateTime<Utc>>,
    pub end_time: Option<DateTime<Utc>>,
    pub scalable_target_action: Option<ScalableTargetAction>,
    pub creation_time: DateTime<Utc>,
    /// Last time the executor fired this scheduled action. Used to
    /// dedupe within the same minute so a cron-every-minute schedule
    /// doesn't double-fire when the ticker runs more than once during
    /// the same wall-clock minute.
    #[serde(default)]
    pub last_fired_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalableTargetAction {
    pub min_capacity: Option<i32>,
    pub max_capacity: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingActivity {
    pub activity_id: String,
    pub service_namespace: String,
    pub resource_id: String,
    pub scalable_dimension: String,
    pub description: String,
    pub cause: String,
    pub start_time: DateTime<Utc>,
    pub end_time: Option<DateTime<Utc>>,
    pub status_code: String,
    pub status_message: Option<String>,
    pub details: Option<String>,
    pub not_scaled_reasons: Vec<NotScaledReason>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotScaledReason {
    pub code: String,
    pub max_capacity: Option<i32>,
    pub min_capacity: Option<i32>,
    pub current_capacity: Option<i32>,
}
