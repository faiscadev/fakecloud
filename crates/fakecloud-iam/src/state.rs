use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct IamUser {
    pub user_name: String,
    pub user_id: String,
    pub arn: String,
    pub path: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct IamAccessKey {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub user_name: String,
    pub status: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct IamRole {
    pub role_name: String,
    pub role_id: String,
    pub arn: String,
    pub path: String,
    pub assume_role_policy_document: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct IamPolicy {
    pub policy_name: String,
    pub policy_id: String,
    pub arn: String,
    pub path: String,
    pub policy_document: String,
    pub created_at: DateTime<Utc>,
}

/// Identity associated with a set of credentials, for GetCallerIdentity resolution.
#[derive(Debug, Clone)]
pub struct CredentialIdentity {
    pub arn: String,
    pub user_id: String,
    pub account_id: String,
}

pub struct IamState {
    pub account_id: String,
    pub users: HashMap<String, IamUser>,
    pub access_keys: HashMap<String, Vec<IamAccessKey>>, // username -> keys
    pub roles: HashMap<String, IamRole>,
    pub policies: HashMap<String, IamPolicy>, // arn -> policy
    pub role_policies: HashMap<String, Vec<String>>, // role_name -> policy arns
    /// Maps access key ID to the identity that should be returned by GetCallerIdentity.
    pub credential_identities: HashMap<String, CredentialIdentity>,
    /// Override ARN for GetCallerIdentity when no user/role matches.
    pub default_caller_arn: Option<String>,
}

impl IamState {
    pub fn new(account_id: &str) -> Self {
        Self {
            account_id: account_id.to_string(),
            users: HashMap::new(),
            access_keys: HashMap::new(),
            roles: HashMap::new(),
            policies: HashMap::new(),
            role_policies: HashMap::new(),
            credential_identities: HashMap::new(),
            default_caller_arn: None,
        }
    }

    /// Reset all state, preserving account_id.
    pub fn reset(&mut self) {
        self.users.clear();
        self.access_keys.clear();
        self.roles.clear();
        self.policies.clear();
        self.role_policies.clear();
        self.credential_identities.clear();
    }
}

pub type SharedIamState = std::sync::Arc<RwLock<IamState>>;
