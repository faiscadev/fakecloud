use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{IamAccessKey, IamPolicy, IamRole, IamUser, SharedIamState};
use crate::xml_responses;

pub struct IamService {
    state: SharedIamState,
}

impl IamService {
    pub fn new(state: SharedIamState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl AwsService for IamService {
    fn service_name(&self) -> &str {
        "iam"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        match req.action.as_str() {
            "CreateUser" => self.create_user(&req),
            "GetUser" => self.get_user(&req),
            "DeleteUser" => self.delete_user(&req),
            "ListUsers" => self.list_users(&req),
            "CreateAccessKey" => self.create_access_key(&req),
            "DeleteAccessKey" => self.delete_access_key(&req),
            "ListAccessKeys" => self.list_access_keys(&req),
            "CreateRole" => self.create_role(&req),
            "GetRole" => self.get_role(&req),
            "DeleteRole" => self.delete_role(&req),
            "ListRoles" => self.list_roles(&req),
            "CreatePolicy" => self.create_policy(&req),
            "AttachRolePolicy" => self.attach_role_policy(&req),
            _ => Err(AwsServiceError::action_not_implemented("iam", &req.action)),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "CreateUser",
            "GetUser",
            "DeleteUser",
            "ListUsers",
            "CreateAccessKey",
            "DeleteAccessKey",
            "ListAccessKeys",
            "CreateRole",
            "GetRole",
            "DeleteRole",
            "ListRoles",
            "CreatePolicy",
            "AttachRolePolicy",
        ]
    }
}

impl IamService {
    fn create_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let path = req
            .query_params
            .get("Path")
            .cloned()
            .unwrap_or_else(|| "/".to_string());

        let mut state = self.state.write();

        if state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                format!("User with name {user_name} already exists."),
            ));
        }

        let user = IamUser {
            user_id: format!("AIDA{}", generate_id()),
            arn: format!(
                "arn:aws:iam::{}:user{}{}",
                state.account_id,
                if path == "/" { "/" } else { &path },
                user_name
            ),
            user_name: user_name.clone(),
            path,
            created_at: Utc::now(),
        };

        let xml = xml_responses::create_user_response(&user, &req.request_id);
        state.users.insert(user_name, user);

        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let state = self.state.read();

        let user = state.users.get(&user_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            )
        })?;

        let xml = xml_responses::get_user_response(user, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let mut state = self.state.write();

        if state.users.remove(&user_name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        state.access_keys.remove(&user_name);
        let xml = xml_responses::delete_user_response(&req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_users(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let users: Vec<IamUser> = state.users.values().cloned().collect();
        let xml = xml_responses::list_users_response(&users, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn create_access_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let mut state = self.state.write();

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        let key = IamAccessKey {
            access_key_id: format!("AKIA{}", generate_id()),
            secret_access_key: format!("fake{}", generate_id()),
            user_name: user_name.clone(),
            status: "Active".to_string(),
            created_at: Utc::now(),
        };

        let xml = xml_responses::create_access_key_response(&key, &req.request_id);
        state.access_keys.entry(user_name).or_default().push(key);

        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_access_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let access_key_id = required_param(&req.query_params, "AccessKeyId")?;
        let mut state = self.state.write();

        if let Some(keys) = state.access_keys.get_mut(&user_name) {
            let len_before = keys.len();
            keys.retain(|k| k.access_key_id != access_key_id);
            if keys.len() == len_before {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchEntity",
                    format!("The Access Key with id {access_key_id} cannot be found."),
                ));
            }
        } else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The Access Key with id {access_key_id} cannot be found."),
            ));
        }

        let xml = xml_responses::delete_access_key_response(&req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_access_keys(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let state = self.state.read();
        let keys = state
            .access_keys
            .get(&user_name)
            .cloned()
            .unwrap_or_default();
        let xml = xml_responses::list_access_keys_response(&keys, &user_name, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn create_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let assume_role_policy = required_param(&req.query_params, "AssumeRolePolicyDocument")?;
        let path = req
            .query_params
            .get("Path")
            .cloned()
            .unwrap_or_else(|| "/".to_string());

        let mut state = self.state.write();

        if state.roles.contains_key(&role_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                format!("Role with name {role_name} already exists."),
            ));
        }

        let role = IamRole {
            role_id: format!("AROA{}", generate_id()),
            arn: format!(
                "arn:aws:iam::{}:role{}{}",
                state.account_id,
                if path == "/" { "/" } else { &path },
                role_name
            ),
            role_name: role_name.clone(),
            path,
            assume_role_policy_document: assume_role_policy,
            created_at: Utc::now(),
        };

        let xml = xml_responses::create_role_response(&role, &req.request_id);
        state.roles.insert(role_name, role);

        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let state = self.state.read();

        let role = state.roles.get(&role_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The role with name {role_name} cannot be found."),
            )
        })?;

        let xml = xml_responses::get_role_response(role, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let mut state = self.state.write();

        if state.roles.remove(&role_name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The role with name {role_name} cannot be found."),
            ));
        }

        state.role_policies.remove(&role_name);
        let xml = xml_responses::delete_role_response(&req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_roles(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let roles: Vec<IamRole> = state.roles.values().cloned().collect();
        let xml = xml_responses::list_roles_response(&roles, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn create_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let policy_name = required_param(&req.query_params, "PolicyName")?;
        let policy_document = required_param(&req.query_params, "PolicyDocument")?;
        let path = req
            .query_params
            .get("Path")
            .cloned()
            .unwrap_or_else(|| "/".to_string());

        let mut state = self.state.write();

        let policy = IamPolicy {
            policy_id: format!("ANPA{}", generate_id()),
            arn: format!(
                "arn:aws:iam::{}:policy{}{}",
                state.account_id, path, policy_name
            ),
            policy_name,
            path,
            policy_document,
            created_at: Utc::now(),
        };

        let xml = xml_responses::create_policy_response(&policy, &req.request_id);
        state.policies.insert(policy.arn.clone(), policy);

        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn attach_role_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;

        let mut state = self.state.write();

        if !state.roles.contains_key(&role_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The role with name {role_name} cannot be found."),
            ));
        }

        state
            .role_policies
            .entry(role_name)
            .or_default()
            .push(policy_arn);

        let xml = xml_responses::attach_role_policy_response(&req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

fn required_param(
    params: &std::collections::HashMap<String, String>,
    name: &str,
) -> Result<String, AwsServiceError> {
    params.get(name).cloned().ok_or_else(|| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "MissingParameter",
            format!("The request must contain the parameter {name}"),
        )
    })
}

fn generate_id() -> String {
    uuid::Uuid::new_v4()
        .to_string()
        .replace('-', "")
        .to_uppercase()[..16]
        .to_string()
}
