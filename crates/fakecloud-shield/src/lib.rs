//! AWS Shield / Shield Advanced (`shield`) awsJson1_1 service for fakecloud.
//!
//! Full control plane for AWS Shield Advanced: protections and protection
//! groups over supported resource types (CloudFront distributions, Route 53
//! hosted zones, Elastic IPs, Classic/Application load balancers, Global
//! Accelerator), the annual auto-renewing Shield Advanced subscription and its
//! subscription-state / limits, emergency-contact settings, DDoS Response Team
//! (DRT) access (role + log buckets), proactive engagement, application-layer
//! automatic response, health-check association, and resource tagging -- all
//! model-driven-validated, account-partitioned and persisted (snapshot +
//! restore).
//!
//! Attack surfacing is honest: with no real DDoS traffic to observe,
//! `ListAttacks` returns an empty attack list and `DescribeAttackStatistics`
//! returns a zeroed time-series over the requested window. No synthetic attacks
//! are fabricated. Every other operation is real, persisted CRUD -- no stubbed
//! success responses.

pub mod persistence;
pub(crate) mod service;
pub(crate) mod state;
pub(crate) mod validate;

pub use service::ShieldService;
pub use state::{SharedShieldState, ShieldSnapshot, ShieldState, SHIELD_SNAPSHOT_SCHEMA_VERSION};
