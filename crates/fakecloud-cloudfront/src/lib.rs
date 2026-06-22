//! AWS CloudFront emulation for FakeCloud.
//!
//! Wire protocol: REST-XML. Requests are routed by HTTP method + URI
//! beneath the `/2020-05-31/` API version prefix. SigV4 service name is
//! `cloudfront`; the service is global so callers always sign for
//! `us-east-1`.

pub mod cfunctions;
pub mod cfunctions_service;
pub mod extras;
pub mod extras2;
pub mod extras2_service;
pub mod extras_service;
pub mod fle;
pub mod fle_service;
pub mod functions;
pub mod functions_service;
pub(crate) mod js_runtime;
pub mod model;
pub mod policies;
pub mod policies_service;
pub mod router;
pub(crate) mod service;
pub(crate) mod state;
pub mod streaming;
pub mod streaming_service;
pub mod tenants;
pub mod tenants_service;
pub mod xml_io;

pub const API_VERSION: &str = "2020-05-31";
pub const API_PREFIX: &str = "/2020-05-31";
pub const NAMESPACE: &str = "http://cloudfront.amazonaws.com/doc/2020-05-31/";

pub use service::CloudFrontService;
pub use state::{
    CloudFrontAccounts, CloudFrontSnapshot, SharedCloudFrontState, StoredDistribution,
    CLOUDFRONT_SNAPSHOT_SCHEMA_VERSION,
};
