//! EC2 Auto Scaling (the `autoscaling` service) — Auto Scaling Groups, Launch
//! Configurations, and scaling activities.
//!
//! Distinct from Application Auto Scaling (`fakecloud-application-autoscaling`,
//! the `application-autoscaling` service), which scales DynamoDB/ECS/etc.
//! targets. This service manages EC2 fleets: an ASG with a desired capacity
//! launches real container-backed EC2 instances (batch 2), closing the gap
//! every rival has where an ASG scales to a *mock* instance.

pub mod cfn_provision;
pub mod service;
pub mod state;

pub use service::AutoScalingService;
pub use state::{
    AutoScalingAccounts, AutoScalingSnapshot, SharedAutoScalingState,
    AUTOSCALING_SNAPSHOT_SCHEMA_VERSION,
};
