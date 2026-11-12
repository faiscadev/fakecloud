pub mod api;
pub mod aws_managed;
pub mod blob;
pub mod hook;
pub mod quota;
pub mod resource_policy;
pub(crate) mod service;
pub(crate) mod state;
#[cfg(any(test, feature = "test-util"))]
pub mod test_support;

pub use service::provisioner;
pub use service::KmsService;
pub use state::{
    aws_managed_key_slot, kms_alias_arn, kms_key_arn, parse_kms_arn, KmsAlias, KmsKey, KmsSnapshot,
    KmsState, SharedKmsState, KMS_SNAPSHOT_SCHEMA_VERSION,
};
