//! AWS Certificate Manager Private CA (ACM PCA) implementation for FakeCloud.
//!
//! Real X.509 issuance: every CA has a genuine key pair and certificate, and
//! `IssueCertificate` signs real end-entity certificates that verify against
//! the CA's certificate.

pub(crate) mod persistence;
pub mod provision;
pub(crate) mod service;
pub(crate) mod state;
pub mod validate;

pub use persistence::save_acmpca_snapshot;
pub use provision::{
    build_pending_ca, default_revocation_configuration, fill_keygen, generate_ca_material,
    subject_of, CaCreateParams, DEFAULT_KEY_STORAGE_STANDARD,
};
pub use service::AcmPcaService;
pub use state::{
    AccountState, AcmPcaAccounts, AcmPcaSnapshot, AuditReport, CertificateAuthority,
    IssuedCertificate, Permission, RevokedCertificate, SharedAcmPcaState, TagEntry,
    ACM_PCA_SNAPSHOT_SCHEMA_VERSION,
};
pub use validate::{
    generate_ca_csr, generate_key_pair, issue_certificate, issuer_from_ca_cert, load_key_pair,
    load_signing_key, resolve_validity, self_issuer, verify_imported_cert, ImportCheck,
    SUPPORTED_CA_KEY_ALGORITHMS,
};
