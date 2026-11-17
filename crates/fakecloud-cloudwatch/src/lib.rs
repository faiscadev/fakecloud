pub mod alarm_actions;
pub(crate) mod alarm_eval;
pub mod alarm_mutes;
pub(crate) mod anomaly_detectors;
pub(crate) mod composite_alarms;
pub(crate) mod datasets;
pub mod delivery;
pub mod flat_members;
pub(crate) mod insight_rules;
pub mod introspection;
pub(crate) mod json_protocol;
pub(crate) mod log_alarms;
pub(crate) mod metric_math;
pub(crate) mod metric_streams;
pub(crate) mod mute_rules;
pub(crate) mod mute_schedule;
pub(crate) mod otel;
pub(crate) mod resource_metrics;
pub(crate) mod series_index;
pub(crate) mod service;
pub(crate) mod state;
pub(crate) mod tagging;

#[cfg(test)]
mod tests;

pub use alarm_actions::{
    AlarmOpsItem, Ec2AlarmActionHook, Ec2InstanceAction, OpsItemAlarmActionHook, OPS_ITEM_SOURCE,
};
pub use alarm_mutes::alarm_muted;
pub use delivery::CloudwatchDeliveryImpl;
pub use service::CloudWatchService;
pub use state::{
    alarm_arn, AlarmExit, AlarmMetricQuery, AlarmMetricStat, AlarmState, CloudWatchAccounts,
    CloudWatchSnapshot, CloudWatchState, Dashboard, MetricAlarm, MetricDatum,
    SharedCloudWatchState, CLOUDWATCH_SNAPSHOT_SCHEMA_VERSION,
};
