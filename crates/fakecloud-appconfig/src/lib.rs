//! AWS AppConfig (`appconfig`) + AppConfig Data (`appconfigdata`) implementation
//! for FakeCloud.
//!
//! One crate serves both AWS model-services. They share the SigV4 signing name
//! `appconfig`; the single service handler splits control-plane traffic
//! (`/applications/...`, `/deploymentstrategies/...`, `/extensions/...`) from
//! data-plane traffic (`/configurationsessions`, `/configuration`) on the URL
//! path, mirroring the OpenSearch (es + opensearch) dual-model crate.

pub mod persistence;
pub mod service;
pub mod state;
pub mod validation;

pub use service::{AppConfigService, APPCONFIG_ACTIONS};
pub use state::{
    AppConfigSnapshot, AppConfigState, SharedAppConfigState, APPCONFIG_SNAPSHOT_SCHEMA_VERSION,
};
