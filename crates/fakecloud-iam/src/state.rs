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

pub struct IamState {
    pub account_id: String,
    pub users: HashMap<String, IamUser>,
    pub access_keys: HashMap<String, Vec<IamAccessKey>>, // username -> keys
    pub roles: HashMap<String, IamRole>,
    pub policies: HashMap<String, IamPolicy>, // arn -> policy
    pub role_policies: HashMap<String, Vec<String>>, // role_name -> policy arns
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
        }
    }
}

pub type SharedIamState = std::sync::Arc<RwLock<IamState>>;
