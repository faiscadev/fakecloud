pub mod dkim;
pub mod fanout;
pub mod mime;
pub(crate) mod service;
pub mod smtp_relay;
pub(crate) mod state;
pub mod v1;

pub use service::SesV2Service;
pub use state::{
    BouncedRecipientInfo, ConfigurationSet, ContactList, DedicatedIpPool, EmailIdentity,
    EmailTemplate, EventDestination, EventDestinationDispatch, IpFilter, ReceiptAction,
    ReceiptFilter, ReceiptRule, ReceiptRuleSet, SentBounce, SentEmail, SesSnapshot, SesState,
    SharedSesState, SmtpSubmission, SES_SNAPSHOT_SCHEMA_VERSION,
};
