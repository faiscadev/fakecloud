//! AWS Route 53 emulation for FakeCloud.
//!
//! Wire protocol: REST-XML. Requests are routed by HTTP method + URI
//! beneath the `/2013-04-01/` API version prefix. SigV4 service name is
//! `route53`; the service is global so callers always sign for
//! `us-east-1`.

pub mod dnssec;
pub mod model;
pub mod router;
pub(crate) mod service;
pub(crate) mod state;
pub mod xml_io;

pub const API_VERSION: &str = "2013-04-01";
pub const API_PREFIX: &str = "/2013-04-01";
pub const NAMESPACE: &str = "https://route53.amazonaws.com/doc/2013-04-01/";

pub use service::{DnssecSignature, Route53Service};
pub use state::{
    AccountState, HealthCheckStatus, Route53Accounts, SharedRoute53State, StoredHealthCheck,
    StoredHostedZone, StoredKeySigningKey,
};
