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
            "GetPolicy" => self.get_policy(&req),
            "DeletePolicy" => self.delete_policy(&req),
            "ListPolicies" => self.list_policies(&req),
            "AttachRolePolicy" => self.attach_role_policy(&req),
            "DetachRolePolicy" => self.detach_role_policy(&req),
            "ListRolePolicies" => self.list_role_policies(&req),
            "ListAttachedRolePolicies" => self.list_attached_role_policies(&req),
            "GetPolicyVersion" => self.get_policy_version(&req),
            "ListPolicyVersions" => self.list_policy_versions(&req),
            "ListInstanceProfilesForRole" => self.list_instance_profiles_for_role(&req),
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
            "GetPolicy",
            "DeletePolicy",
            "ListPolicies",
            "AttachRolePolicy",
            "DetachRolePolicy",
            "ListRolePolicies",
            "ListAttachedRolePolicies",
            "GetPolicyVersion",
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

    fn list_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let policies: Vec<IamPolicy> = state.policies.values().cloned().collect();
        let xml = xml_responses::list_policies_response(&policies, &req.request_id);
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

    fn get_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let state = self.state.read();

        let policy = state.policies.get(&policy_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} does not exist."),
            )
        })?;

        let xml = xml_responses::get_policy_response(policy, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let mut state = self.state.write();

        if state.policies.remove(&policy_arn).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} does not exist."),
            ));
        }

        // Also remove from any role attachments
        for arns in state.role_policies.values_mut() {
            arns.retain(|a| a != &policy_arn);
        }

        let xml = xml_responses::delete_policy_response(&req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_role_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let state = self.state.read();

        if !state.roles.contains_key(&role_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The role with name {role_name} cannot be found."),
            ));
        }

        // ListRolePolicies returns inline policy names, not managed policies.
        // We only support managed policies, so return an empty list.
        let empty: Vec<String> = Vec::new();
        let xml = xml_responses::list_role_policies_response(&empty, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn detach_role_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

        if let Some(arns) = state.role_policies.get_mut(&role_name) {
            arns.retain(|a| a != &policy_arn);
        }

        let xml = xml_responses::detach_role_policy_response(&req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_attached_role_policies(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let state = self.state.read();

        if !state.roles.contains_key(&role_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The role with name {role_name} cannot be found."),
            ));
        }

        let policy_arns = state
            .role_policies
            .get(&role_name)
            .cloned()
            .unwrap_or_default();

        let members: String = policy_arns
            .iter()
            .filter_map(|arn| {
                state.policies.get(arn).map(|p| {
                    format!(
                        "      <member>\n        <PolicyName>{}</PolicyName>\n        <PolicyArn>{}</PolicyArn>\n      </member>",
                        p.policy_name, p.arn
                    )
                })
            })
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAttachedRolePoliciesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListAttachedRolePoliciesResult>
    <IsTruncated>false</IsTruncated>
    <AttachedPolicies>
{members}
    </AttachedPolicies>
  </ListAttachedRolePoliciesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListAttachedRolePoliciesResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_policy_versions(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let state = self.state.read();

        let policy = state.policies.get(&policy_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} not found."),
            )
        })?;

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListPolicyVersionsResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListPolicyVersionsResult>
    <IsTruncated>false</IsTruncated>
    <Versions>
      <member>
        <VersionId>v1</VersionId>
        <IsDefaultVersion>true</IsDefaultVersion>
        <CreateDate>{}</CreateDate>
      </member>
    </Versions>
  </ListPolicyVersionsResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListPolicyVersionsResponse>"#,
            policy.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_instance_profiles_for_role(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let _role_name = required_param(&req.query_params, "RoleName")?;
        // Return empty list — we don't support instance profiles yet
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListInstanceProfilesForRoleResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListInstanceProfilesForRoleResult>
    <IsTruncated>false</IsTruncated>
    <InstanceProfiles/>
  </ListInstanceProfilesForRoleResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListInstanceProfilesForRoleResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_policy_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let _version_id = required_param(&req.query_params, "VersionId")?;
        let state = self.state.read();

        let policy = state.policies.get(&policy_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} not found."),
            )
        })?;

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetPolicyVersionResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetPolicyVersionResult>
    <PolicyVersion>
      <Document>{}</Document>
      <VersionId>v1</VersionId>
      <IsDefaultVersion>true</IsDefaultVersion>
      <CreateDate>{}</CreateDate>
    </PolicyVersion>
  </GetPolicyVersionResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetPolicyVersionResponse>"#,
            xml_escape(&policy.policy_document),
            policy.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
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
