//! AWS Transfer Family (`transfer`) awsJson1.1 control plane for fakecloud.
//!
//! The full 71-operation Transfer Family control plane from the AWS Smithy
//! model: SFTP/FTPS/FTP/AS2 servers, users, SSH public keys, host keys,
//! service-managed accesses, workflows and executions, AS2 agreements,
//! connectors (SFTP + AS2, with `TestConnection` / file-transfer / directory
//! listing / remote delete+move actions), profiles, certificates, security
//! policies, web apps (+ customization), identity-provider testing, and
//! resource tagging.
//!
//! Transfer is modelled as a state CRUD control plane. There is no real SFTP
//! daemon or AS2 data movement engine (LocalStack Community treats Transfer the
//! same way — the actual file transport is out of scope). Every resource is
//! real, account-partitioned, persisted state: every `Create`/`Import` is
//! reflected by its `Describe` and `List`, every `Update` persists, every
//! `Delete` deletes, `StartServer` / `StopServer` transition the server `State`
//! and settle to steady state (`ONLINE` / `OFFLINE`) synchronously so waiters
//! complete, and `TestConnection` / `TestIdentityProvider` return successful
//! results against a real, existing resource. Nested configuration objects
//! (endpoint details, identity-provider details, AS2/SFTP connector config,
//! workflow steps, ...) are stored as the raw request `Value` so they
//! round-trip verbatim through `Describe`.

pub mod persistence;
pub mod service;
pub mod state;
mod validate;

pub use service::TransferService;
pub use state::{SharedTransferState, TransferData, TRANSFER_SNAPSHOT_SCHEMA_VERSION};
