pub mod auth_message;
pub mod condition;
pub mod credential_resolver;
pub mod evaluator;
pub mod iam_service;
pub mod managed_policies;
pub mod pass_role;
pub mod persistence;
pub mod policy_evaluator;
pub mod policy_validation;
pub mod resource_policy;
pub(crate) mod state;
pub mod sts_service;
pub mod xml_responses;

pub use state::{
    CredentialIdentity, IamAccessKey, IamGroup, IamInstanceProfile, IamPolicy, IamRole,
    IamSnapshot, IamState, IamUser, OidcProvider, PolicyVersion, SamlProvider, SharedIamState,
    StsTempCredential, Tag, VirtualMfaDevice, IAM_SNAPSHOT_SCHEMA_VERSION,
};
