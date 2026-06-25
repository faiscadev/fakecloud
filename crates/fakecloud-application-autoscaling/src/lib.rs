pub mod hooks;
pub mod scheduled_executor;
pub(crate) mod service;
pub(crate) mod state;
pub mod ticker;

pub use hooks::{DynamoDbCapacityHook, EcsServiceHook, MetricReader};
pub use scheduled_executor::ScheduledActionExecutor;
pub use service::{save_application_autoscaling_snapshot, ApplicationAutoScalingService};
pub use state::{
    AccountState, Alarm, ApplicationAutoScalingAccounts, ApplicationAutoScalingSnapshot,
    NotScaledReason, PolicyKey, ScalableTarget, ScalableTargetAction, ScalingActivity,
    ScalingPolicy, ScheduledAction, ScheduledKey, SharedApplicationAutoScalingState,
    SuspendedState, TargetKey, APPLICATION_AUTOSCALING_SNAPSHOT_SCHEMA_VERSION,
};
pub use ticker::ScalingWatcher;
