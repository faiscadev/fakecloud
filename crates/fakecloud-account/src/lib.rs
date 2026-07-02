//! AWS Account Management (`account`) restJson1 service for fakecloud.
//!
//! The full 15-operation control plane from the AWS Smithy model: alternate
//! contacts (Get/Put/DeleteAlternateContact), primary contact information
//! (Get/PutContactInformation), account information (GetAccountInformation,
//! PutAccountName, GetGovCloudAccountInformation), primary-email management
//! (GetPrimaryEmail, Start/AcceptPrimaryEmailUpdate), and Region opt-in control
//! (ListRegions, GetRegionOptStatus, Enable/DisableRegion). Every operation is
//! backed by real, account-partitioned, persisted state and honors the optional
//! `AccountId` member so an organization's management account can read/write a
//! member account's settings.

pub mod persistence;
pub mod service;
pub mod state;

pub use service::AccountService;
pub use state::{AccountData, SharedAccountState, ACCOUNT_SNAPSHOT_SCHEMA_VERSION};
