//! AWS Certificate Manager Private CA (ACM PCA) implementation for FakeCloud.
//!
//! Real X.509 issuance: every CA has a genuine key pair and certificate, and
//! `IssueCertificate` signs real end-entity certificates that verify against
//! the CA's certificate.

pub(crate) mod persistence;
pub(crate) mod service;
pub(crate) mod state;
pub mod validate;

pub use persistence::save_acmpca_snapshot;
pub use service::AcmPcaService;
pub use state::{
    AccountState, AcmPcaAccounts, AcmPcaSnapshot, AuditReport, CertificateAuthority,
    IssuedCertificate, Permission, RevokedCertificate, SharedAcmPcaState, TagEntry,
    ACM_PCA_SNAPSHOT_SCHEMA_VERSION,
};
pub use validate::{
    generate_ca_csr, generate_key_pair, generate_root_ca, issue_certificate, load_key_pair,
    resolve_validity,
};
