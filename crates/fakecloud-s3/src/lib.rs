pub mod cfn;
pub mod delivery;
pub mod eventstream;
pub mod inventory;
pub mod lifecycle;
pub mod logging;
pub mod persistence;
pub mod resource_policy;
pub(crate) mod select;
pub(crate) mod service;
pub mod simulation;
pub(crate) mod state;
mod xml_util;

pub use cfn::apply_cfn_bucket_properties;
pub use delivery::S3DeliveryImpl;
pub use service::S3Service;
pub use state::{memory_body, S3AccessPoint, S3Bucket, S3Object, S3State, SharedS3State};
