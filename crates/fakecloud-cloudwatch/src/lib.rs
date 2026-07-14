pub(crate) mod alarm_eval;
pub(crate) mod anomaly_detectors;
pub(crate) mod composite_alarms;
pub(crate) mod datasets;
pub mod delivery;
pub(crate) mod insight_rules;
pub mod introspection;
pub(crate) mod json_protocol;
pub(crate) mod metric_math;
pub(crate) mod metric_streams;
pub(crate) mod mute_rules;
pub(crate) mod otel;
pub(crate) mod service;
pub(crate) mod state;
pub(crate) mod tagging;

#[cfg(test)]
mod tests;

pub use delivery::CloudwatchDeliveryImpl;
pub use service::CloudWatchService;
pub use state::{
    AlarmMetricQuery, AlarmMetricStat, AlarmState, CloudWatchAccounts, CloudWatchSnapshot,
    CloudWatchState, Dashboard, MetricAlarm, MetricDatum, SharedCloudWatchState,
    CLOUDWATCH_SNAPSHOT_SCHEMA_VERSION,
};
