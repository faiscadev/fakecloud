//! AWS IAM Identity Center SSO Admin (`sso-admin`) awsJson1.1 service for
//! fakecloud.
//!
//! The full 79-operation control plane from the AWS Smithy model: IAM
//! Identity Center instances, permission sets and their inline / managed /
//! customer-managed / boundary policies, account assignments (async status
//! settle), permission-set provisioning, applications with their assignments,
//! access scopes, authentication methods, grants and session configuration,
//! the fixed application-provider catalogue, trusted token issuers, instance
//! regions, access-control attribute configuration, and resource tagging.
//!
//! Every operation is backed by real, account-partitioned, persisted state.
//! Nested configuration objects (PortalOptions, TrustedTokenIssuerConfiguration,
//! Grant, AuthenticationMethod, ...) are stored as the raw request `Value` so
//! they round-trip verbatim.

pub mod persistence;
pub mod service;
pub mod state;

pub use service::SsoAdminService;
pub use state::{SharedSsoAdminState, SsoAdminData, SSOADMIN_SNAPSHOT_SCHEMA_VERSION};
