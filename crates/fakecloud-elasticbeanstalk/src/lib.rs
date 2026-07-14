//! AWS Elastic Beanstalk (`elasticbeanstalk`) - awsQuery-protocol control
//! plane.
//!
//! Elastic Beanstalk is an orchestration facade over EC2 / Auto Scaling /
//! ELB / CloudFormation / S3 / CloudWatch, the same way `fakecloud-batch`
//! drives ECS. This crate implements the full control plane with real
//! persisted state - Applications, ApplicationVersions (source bundle in S3),
//! Environments (with a real `Launching -> Ready / Updating / Terminating`
//! lifecycle and a `Green/Yellow/Red/Grey` health rollup derived from that
//! state), ConfigurationTemplates, configuration option settings, Events
//! emitted on every transition, and platform / solution-stack listing.
//!
//! Environment settle transitions are asynchronous (like RDS / Batch):
//! `CreateEnvironment` returns immediately with `Status=Launching`, then a
//! spawned task settles it to `Ready`. Pending transitions are reconciled on
//! restart so no environment is left stuck. The actual application data-plane
//! (spawning a container that serves the deployed version) is deferred; health
//! and status are derived from the real modeled state, never faked - the same
//! control-plane-complete posture ELBv2 shipped with.

pub(crate) mod service;
pub(crate) mod state;

pub use service::ElasticBeanstalkService;
pub use state::{
    EbAccounts, ElasticBeanstalkSnapshot, SharedEbState, ELASTICBEANSTALK_SNAPSHOT_SCHEMA_VERSION,
};
// Re-exported for the CloudFormation resource_provisioner; `state` is pub(crate).
pub use state::{
    environment_status, Application, ApplicationVersion, ConfigurationTemplate, Environment, Event,
    MaxAgeRule, MaxCountRule, OptionSetting, ResourceLifecycleConfig, ResourceTag,
    SourceBuildInformation,
};
