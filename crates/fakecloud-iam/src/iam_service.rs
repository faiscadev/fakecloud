use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{
    AccountPasswordPolicy, IamAccessKey, IamGroup, IamInstanceProfile, IamPolicy, IamRole, IamUser,
    LoginProfile, OidcProvider, PolicyVersion, SamlProvider, ServerCertificate,
    ServiceLinkedRoleDeletion, SharedIamState, SigningCertificate, Tag, VirtualMfaDevice,
};
use crate::xml_responses;

/// Get the AWS partition from a region string.
fn partition_for_region(region: &str) -> &str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-iso-") {
        "aws-iso"
    } else if region.starts_with("us-isob-") {
        "aws-iso-b"
    } else if region.starts_with("us-isof-") {
        "aws-iso-f"
    } else if region.starts_with("eu-isoe-") {
        "aws-iso-e"
    } else {
        "aws"
    }
}

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
            // Users
            "CreateUser" => self.create_user(&req),
            "GetUser" => self.get_user(&req),
            "DeleteUser" => self.delete_user(&req),
            "ListUsers" => self.list_users(&req),
            "UpdateUser" => self.update_user(&req),
            "TagUser" => self.tag_user(&req),
            "UntagUser" => self.untag_user(&req),
            "ListUserTags" => self.list_user_tags(&req),

            // Access Keys
            "CreateAccessKey" => self.create_access_key(&req),
            "DeleteAccessKey" => self.delete_access_key(&req),
            "ListAccessKeys" => self.list_access_keys(&req),
            "UpdateAccessKey" => self.update_access_key(&req),

            // Roles
            "CreateRole" => self.create_role(&req),
            "GetRole" => self.get_role(&req),
            "DeleteRole" => self.delete_role(&req),
            "ListRoles" => self.list_roles(&req),
            "UpdateRole" => self.update_role(&req),
            "UpdateRoleDescription" => self.update_role_description(&req),
            "UpdateAssumeRolePolicy" => self.update_assume_role_policy(&req),
            "TagRole" => self.tag_role(&req),
            "UntagRole" => self.untag_role(&req),
            "ListRoleTags" => self.list_role_tags(&req),
            "PutRolePermissionsBoundary" => self.put_role_permissions_boundary(&req),
            "DeleteRolePermissionsBoundary" => self.delete_role_permissions_boundary(&req),

            // Policies (managed)
            "CreatePolicy" => self.create_policy(&req),
            "GetPolicy" => self.get_policy(&req),
            "DeletePolicy" => self.delete_policy(&req),
            "ListPolicies" => self.list_policies(&req),
            "TagPolicy" => self.tag_policy(&req),
            "UntagPolicy" => self.untag_policy(&req),
            "ListPolicyTags" => self.list_policy_tags(&req),

            // Policy Versions
            "CreatePolicyVersion" => self.create_policy_version(&req),
            "GetPolicyVersion" => self.get_policy_version(&req),
            "ListPolicyVersions" => self.list_policy_versions(&req),
            "DeletePolicyVersion" => self.delete_policy_version(&req),
            "SetDefaultPolicyVersion" => self.set_default_policy_version(&req),

            // Role policy attachments (managed)
            "AttachRolePolicy" => self.attach_role_policy(&req),
            "DetachRolePolicy" => self.detach_role_policy(&req),
            "ListAttachedRolePolicies" => self.list_attached_role_policies(&req),

            // Role inline policies
            "PutRolePolicy" => self.put_role_policy(&req),
            "GetRolePolicy" => self.get_role_policy(&req),
            "DeleteRolePolicy" => self.delete_role_policy(&req),
            "ListRolePolicies" => self.list_role_policies(&req),

            // User policy attachments (managed)
            "AttachUserPolicy" => self.attach_user_policy(&req),
            "DetachUserPolicy" => self.detach_user_policy(&req),
            "ListAttachedUserPolicies" => self.list_attached_user_policies(&req),

            // User inline policies
            "PutUserPolicy" => self.put_user_policy(&req),
            "GetUserPolicy" => self.get_user_policy(&req),
            "DeleteUserPolicy" => self.delete_user_policy(&req),
            "ListUserPolicies" => self.list_user_policies(&req),

            // Groups
            "CreateGroup" => self.create_group(&req),
            "GetGroup" => self.get_group(&req),
            "DeleteGroup" => self.delete_group(&req),
            "ListGroups" => self.list_groups(&req),
            "UpdateGroup" => self.update_group(&req),
            "AddUserToGroup" => self.add_user_to_group(&req),
            "RemoveUserFromGroup" => self.remove_user_from_group(&req),
            "ListGroupsForUser" => self.list_groups_for_user(&req),

            // Group policies
            "PutGroupPolicy" => self.put_group_policy(&req),
            "GetGroupPolicy" => self.get_group_policy(&req),
            "DeleteGroupPolicy" => self.delete_group_policy(&req),
            "ListGroupPolicies" => self.list_group_policies(&req),
            "AttachGroupPolicy" => self.attach_group_policy(&req),
            "DetachGroupPolicy" => self.detach_group_policy(&req),
            "ListAttachedGroupPolicies" => self.list_attached_group_policies(&req),

            // Instance Profiles
            "CreateInstanceProfile" => self.create_instance_profile(&req),
            "GetInstanceProfile" => self.get_instance_profile(&req),
            "DeleteInstanceProfile" => self.delete_instance_profile(&req),
            "ListInstanceProfiles" => self.list_instance_profiles(&req),
            "AddRoleToInstanceProfile" => self.add_role_to_instance_profile(&req),
            "RemoveRoleFromInstanceProfile" => self.remove_role_from_instance_profile(&req),
            "ListInstanceProfilesForRole" => self.list_instance_profiles_for_role(&req),
            "TagInstanceProfile" => self.tag_instance_profile(&req),
            "UntagInstanceProfile" => self.untag_instance_profile(&req),
            "ListInstanceProfileTags" => self.list_instance_profile_tags(&req),

            // Login Profiles
            "CreateLoginProfile" => self.create_login_profile(&req),
            "GetLoginProfile" => self.get_login_profile(&req),
            "UpdateLoginProfile" => self.update_login_profile(&req),
            "DeleteLoginProfile" => self.delete_login_profile(&req),

            // SAML Providers
            "CreateSAMLProvider" => self.create_saml_provider(&req),
            "GetSAMLProvider" => self.get_saml_provider(&req),
            "DeleteSAMLProvider" => self.delete_saml_provider(&req),
            "ListSAMLProviders" => self.list_saml_providers(&req),
            "UpdateSAMLProvider" => self.update_saml_provider(&req),

            // OIDC Providers
            "CreateOpenIDConnectProvider" => self.create_oidc_provider(&req),
            "GetOpenIDConnectProvider" => self.get_oidc_provider(&req),
            "DeleteOpenIDConnectProvider" => self.delete_oidc_provider(&req),
            "ListOpenIDConnectProviders" => self.list_oidc_providers(&req),
            "UpdateOpenIDConnectProviderThumbprint" => self.update_oidc_thumbprint(&req),
            "AddClientIDToOpenIDConnectProvider" => self.add_client_id_to_oidc(&req),
            "RemoveClientIDFromOpenIDConnectProvider" => self.remove_client_id_from_oidc(&req),
            "TagOpenIDConnectProvider" => self.tag_oidc_provider(&req),
            "UntagOpenIDConnectProvider" => self.untag_oidc_provider(&req),
            "ListOpenIDConnectProviderTags" => self.list_oidc_provider_tags(&req),

            // Server Certificates
            "UploadServerCertificate" => self.upload_server_certificate(&req),
            "GetServerCertificate" => self.get_server_certificate(&req),
            "DeleteServerCertificate" => self.delete_server_certificate(&req),
            "ListServerCertificates" => self.list_server_certificates(&req),

            // Signing Certificates
            "UploadSigningCertificate" => self.upload_signing_certificate(&req),
            "ListSigningCertificates" => self.list_signing_certificates(&req),
            "UpdateSigningCertificate" => self.update_signing_certificate(&req),
            "DeleteSigningCertificate" => self.delete_signing_certificate(&req),

            // Service Linked Roles
            "CreateServiceLinkedRole" => self.create_service_linked_role(&req),
            "DeleteServiceLinkedRole" => self.delete_service_linked_role(&req),
            "GetServiceLinkedRoleDeletionStatus" => {
                self.get_service_linked_role_deletion_status(&req)
            }

            // Account
            "GetAccountSummary" => self.get_account_summary(&req),
            "GetAccountAuthorizationDetails" => self.get_account_authorization_details(&req),
            "CreateAccountAlias" => self.create_account_alias(&req),
            "DeleteAccountAlias" => self.delete_account_alias(&req),
            "ListAccountAliases" => self.list_account_aliases(&req),
            "UpdateAccountPasswordPolicy" => self.update_account_password_policy(&req),
            "GetAccountPasswordPolicy" => self.get_account_password_policy(&req),
            "DeleteAccountPasswordPolicy" => self.delete_account_password_policy(&req),

            // Credential Report
            "GenerateCredentialReport" => self.generate_credential_report(&req),
            "GetCredentialReport" => self.get_credential_report(&req),

            // Virtual MFA Devices
            "CreateVirtualMFADevice" => self.create_virtual_mfa_device(&req),
            "DeleteVirtualMFADevice" => self.delete_virtual_mfa_device(&req),
            "ListVirtualMFADevices" => self.list_virtual_mfa_devices(&req),
            "EnableMFADevice" => self.enable_mfa_device(&req),
            "DeactivateMFADevice" => self.deactivate_mfa_device(&req),
            "ListMFADevices" => self.list_mfa_devices(&req),

            // Entities for policy
            "ListEntitiesForPolicy" => self.list_entities_for_policy(&req),

            _ => Err(AwsServiceError::action_not_implemented("iam", &req.action)),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "CreateUser",
            "GetUser",
            "DeleteUser",
            "ListUsers",
            "UpdateUser",
            "TagUser",
            "UntagUser",
            "ListUserTags",
            "CreateAccessKey",
            "DeleteAccessKey",
            "ListAccessKeys",
            "UpdateAccessKey",
            "CreateRole",
            "GetRole",
            "DeleteRole",
            "ListRoles",
            "UpdateRole",
            "UpdateRoleDescription",
            "UpdateAssumeRolePolicy",
            "TagRole",
            "UntagRole",
            "ListRoleTags",
            "PutRolePermissionsBoundary",
            "DeleteRolePermissionsBoundary",
            "CreatePolicy",
            "GetPolicy",
            "DeletePolicy",
            "ListPolicies",
            "TagPolicy",
            "UntagPolicy",
            "ListPolicyTags",
            "CreatePolicyVersion",
            "GetPolicyVersion",
            "ListPolicyVersions",
            "DeletePolicyVersion",
            "SetDefaultPolicyVersion",
            "AttachRolePolicy",
            "DetachRolePolicy",
            "ListAttachedRolePolicies",
            "PutRolePolicy",
            "GetRolePolicy",
            "DeleteRolePolicy",
            "ListRolePolicies",
            "AttachUserPolicy",
            "DetachUserPolicy",
            "ListAttachedUserPolicies",
            "PutUserPolicy",
            "GetUserPolicy",
            "DeleteUserPolicy",
            "ListUserPolicies",
            "CreateGroup",
            "GetGroup",
            "DeleteGroup",
            "ListGroups",
            "UpdateGroup",
            "AddUserToGroup",
            "RemoveUserFromGroup",
            "ListGroupsForUser",
            "PutGroupPolicy",
            "GetGroupPolicy",
            "DeleteGroupPolicy",
            "ListGroupPolicies",
            "AttachGroupPolicy",
            "DetachGroupPolicy",
            "ListAttachedGroupPolicies",
            "CreateInstanceProfile",
            "GetInstanceProfile",
            "DeleteInstanceProfile",
            "ListInstanceProfiles",
            "AddRoleToInstanceProfile",
            "RemoveRoleFromInstanceProfile",
            "ListInstanceProfilesForRole",
            "TagInstanceProfile",
            "UntagInstanceProfile",
            "ListInstanceProfileTags",
            "CreateLoginProfile",
            "GetLoginProfile",
            "UpdateLoginProfile",
            "DeleteLoginProfile",
            "CreateSAMLProvider",
            "GetSAMLProvider",
            "DeleteSAMLProvider",
            "ListSAMLProviders",
            "UpdateSAMLProvider",
            "CreateOpenIDConnectProvider",
            "GetOpenIDConnectProvider",
            "DeleteOpenIDConnectProvider",
            "ListOpenIDConnectProviders",
            "UpdateOpenIDConnectProviderThumbprint",
            "AddClientIDToOpenIDConnectProvider",
            "RemoveClientIDFromOpenIDConnectProvider",
            "TagOpenIDConnectProvider",
            "UntagOpenIDConnectProvider",
            "ListOpenIDConnectProviderTags",
            "UploadServerCertificate",
            "GetServerCertificate",
            "DeleteServerCertificate",
            "ListServerCertificates",
            "UploadSigningCertificate",
            "ListSigningCertificates",
            "UpdateSigningCertificate",
            "DeleteSigningCertificate",
            "CreateServiceLinkedRole",
            "DeleteServiceLinkedRole",
            "GetServiceLinkedRoleDeletionStatus",
            "GetAccountSummary",
            "GetAccountAuthorizationDetails",
            "CreateAccountAlias",
            "DeleteAccountAlias",
            "ListAccountAliases",
            "UpdateAccountPasswordPolicy",
            "GetAccountPasswordPolicy",
            "DeleteAccountPasswordPolicy",
            "GenerateCredentialReport",
            "GetCredentialReport",
            "CreateVirtualMFADevice",
            "DeleteVirtualMFADevice",
            "ListVirtualMFADevices",
            "EnableMFADevice",
            "DeactivateMFADevice",
            "ListMFADevices",
            "ListEntitiesForPolicy",
        ]
    }
}

/// Extract the caller's access key from the request's Authorization header.
fn extract_access_key(req: &AwsRequest) -> Option<String> {
    let auth = req.headers.get("authorization")?.to_str().ok()?;
    let info = fakecloud_aws::sigv4::parse_sigv4(auth)?;
    Some(info.access_key)
}

// ========= Helper functions =========

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn url_encode(s: &str) -> String {
    use std::fmt::Write;
    let mut result = String::new();
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(byte as char);
            }
            _ => {
                write!(result, "%{:02X}", byte).unwrap();
            }
        }
    }
    result
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

/// Resolve the calling user when UserName is not provided.
/// Returns the first user found or a default "default" name.
fn resolve_calling_user(state: &crate::state::IamState, _account_id: &str) -> String {
    // In a real implementation, we'd look up the user from the access key.
    // For simplicity, return the first user or "default".
    state
        .users
        .keys()
        .next()
        .cloned()
        .unwrap_or_else(|| "default".to_string())
}

fn generate_id() -> String {
    uuid::Uuid::new_v4()
        .to_string()
        .replace('-', "")
        .to_uppercase()[..16]
        .to_string()
}

fn parse_tags(params: &std::collections::HashMap<String, String>) -> Vec<Tag> {
    let mut tags = Vec::new();
    let mut i = 1;
    loop {
        let key_param = format!("Tags.member.{i}.Key");
        let value_param = format!("Tags.member.{i}.Value");
        match params.get(&key_param) {
            Some(key) => {
                let value = params.get(&value_param).cloned().unwrap_or_default();
                tags.push(Tag {
                    key: key.clone(),
                    value,
                });
                i += 1;
            }
            None => break,
        }
    }
    tags
}

fn parse_tag_keys(params: &std::collections::HashMap<String, String>) -> Vec<String> {
    let mut keys = Vec::new();
    let mut i = 1;
    loop {
        let key_param = format!("TagKeys.member.{i}");
        match params.get(&key_param) {
            Some(key) => {
                keys.push(key.clone());
                i += 1;
            }
            None => break,
        }
    }
    keys
}

fn tags_xml(tags: &[Tag]) -> String {
    tags.iter()
        .map(|t| {
            format!(
                "        <member>\n          <Key>{}</Key>\n          <Value>{}</Value>\n        </member>",
                xml_escape(&t.key),
                xml_escape(&t.value)
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn paginated_tags_response(action: &str, tags: &[Tag], req: &AwsRequest) -> String {
    let max_items: usize = req
        .query_params
        .get("MaxItems")
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let offset: usize = req
        .query_params
        .get("Marker")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let offset = offset.min(tags.len());
    let page = &tags[offset..tags.len().min(offset + max_items)];
    let is_truncated = offset + max_items < tags.len();
    let members = tags_xml(page);
    let marker = if is_truncated {
        format!("<Marker>{}</Marker>", offset + max_items)
    } else {
        String::new()
    };

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<{action}Response xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <{action}Result>
    <IsTruncated>{is_truncated}</IsTruncated>
    <Tags>
{members}
    </Tags>
    {marker}
  </{action}Result>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</{action}Response>"#,
        req.request_id
    )
}

fn validate_tags(tags: &[Tag], existing_count: usize) -> Result<(), AwsServiceError> {
    // Check total tag count
    if tags.len() + existing_count > 50 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidInput",
            "1 validation error detected: Value at 'tags' failed to satisfy constraint: Member must have length less than or equal to 50.".to_string(),
        ));
    }

    // Check for duplicate keys
    let mut seen_keys = std::collections::HashSet::new();
    for tag in tags {
        let lower = tag.key.to_lowercase();
        if !seen_keys.insert(lower) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                "Duplicate tag keys found. Please note that Tag keys are case insensitive."
                    .to_string(),
            ));
        }

        // Key length
        if tag.key.len() > 128 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                format!(
                    "1 validation error detected: Value at 'tags.{}.member.key' failed to satisfy constraint: Member must have length less than or equal to 128.",
                    seen_keys.len()
                ),
            ));
        }

        // Value length
        if tag.value.len() > 256 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                format!(
                    "1 validation error detected: Value at 'tags.{}.member.value' failed to satisfy constraint: Member must have length less than or equal to 256.",
                    seen_keys.len()
                ),
            ));
        }

        // Invalid characters in key
        if !tag.key.chars().all(|c| {
            c.is_alphanumeric()
                || c == ' '
                || c == '+'
                || c == '-'
                || c == '='
                || c == '.'
                || c == '_'
                || c == ':'
                || c == '/'
                || c == '@'
        }) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                format!(
                    "1 validation error detected: Value at 'tags.{}.member.key' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\p{{L}}\\p{{Z}}\\p{{N}}_.:/=+\\-@]+",
                    seen_keys.len()
                ),
            ));
        }
    }

    Ok(())
}

fn validate_untag_keys(keys: &[String]) -> Result<(), AwsServiceError> {
    if keys.len() > 50 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationError",
            "1 validation error detected: Value at 'tagKeys' failed to satisfy constraint: Member must have length less than or equal to 50.".to_string(),
        ));
    }
    for key in keys {
        if key.len() > 128 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                "1 validation error detected: Value at 'tagKeys' failed to satisfy constraint: Member must have length less than or equal to 128.".to_string(),
            ));
        }
        if !key.chars().all(|c| {
            c.is_alphanumeric()
                || c == ' '
                || c == '+'
                || c == '-'
                || c == '='
                || c == '.'
                || c == '_'
                || c == ':'
                || c == '/'
                || c == '@'
        }) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                "1 validation error detected: Value at 'tagKeys' failed to satisfy constraint: Member must satisfy regular expression pattern: [\\p{L}\\p{Z}\\p{N}_.:/=+\\-@]+".to_string(),
            ));
        }
    }
    Ok(())
}

fn empty_response(action: &str, request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<{action}Response xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <{action}Result/>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</{action}Response>"#,
    )
}

// ========= User operations =========

impl IamService {
    /// Determine the effective account ID for this request.
    /// If the caller has assumed a role into a different account, use that account ID.
    /// MUST be called before acquiring a write lock on self.state.
    fn effective_account_id(&self, req: &AwsRequest) -> String {
        if let Some(access_key) = extract_access_key(req) {
            let state = self.state.read();
            if let Some(identity) = state.credential_identities.get(&access_key) {
                return identity.account_id.clone();
            }
        }
        self.state.read().account_id.clone()
    }

    fn create_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let path = req
            .query_params
            .get("Path")
            .cloned()
            .unwrap_or_else(|| "/".to_string());
        let tags = parse_tags(&req.query_params);
        let permissions_boundary = req.query_params.get("PermissionsBoundary").cloned();

        let partition = partition_for_region(&req.region);
        let effective_account = self.effective_account_id(req);

        let mut state = self.state.write();

        if state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                format!("User {user_name} already exists"),
            ));
        }
        let user = IamUser {
            user_id: format!("AIDA{}", generate_id()),
            arn: format!(
                "arn:{}:iam::{}:user{}{}",
                partition,
                effective_account,
                if path == "/" { "/" } else { &path },
                user_name
            ),
            user_name: user_name.clone(),
            path,
            created_at: Utc::now(),
            tags,
            permissions_boundary,
        };

        let xml = xml_responses::create_user_response(&user, &req.request_id);
        state.users.insert(user_name, user);

        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        // If no UserName specified, return current/default user
        let user_name = match req.query_params.get("UserName") {
            Some(name) => name.clone(),
            None => {
                let default_user = IamUser {
                    user_id: format!("AIDA{}", generate_id()),
                    arn: format!("arn:aws:iam::{}:user/default_user", state.account_id),
                    user_name: "default_user".to_string(),
                    path: "/".to_string(),
                    created_at: Utc::now(),
                    tags: Vec::new(),
                    permissions_boundary: None,
                };
                let xml = xml_responses::get_user_response(&default_user, &req.request_id);
                return Ok(AwsResponse::xml(StatusCode::OK, xml));
            }
        };

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

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        // Check for access keys
        if state
            .access_keys
            .get(&user_name)
            .map(|k| !k.is_empty())
            .unwrap_or(false)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "DeleteConflict",
                "Cannot delete entity, must delete access keys first.".to_string(),
            ));
        }

        // Check for group membership
        let in_groups = state
            .groups
            .values()
            .any(|g| g.members.contains(&user_name));
        if in_groups {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "DeleteConflict",
                "Cannot delete entity, must remove user from group first.".to_string(),
            ));
        }

        // Check for attached managed policies
        if state
            .user_policies
            .get(&user_name)
            .map(|p| !p.is_empty())
            .unwrap_or(false)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "DeleteConflict",
                "Cannot delete entity, must detach all policies first.".to_string(),
            ));
        }

        // Check for inline policies
        if state
            .user_inline_policies
            .get(&user_name)
            .map(|p| !p.is_empty())
            .unwrap_or(false)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "DeleteConflict",
                "Cannot delete entity, must delete policies first.".to_string(),
            ));
        }

        state.users.remove(&user_name);
        state.access_keys.remove(&user_name);
        state.user_policies.remove(&user_name);
        state.user_inline_policies.remove(&user_name);
        state.login_profiles.remove(&user_name);
        state.signing_certificates.remove(&user_name);

        let xml = empty_response("DeleteUser", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_users(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let path_prefix = req.query_params.get("PathPrefix").cloned();
        let mut users: Vec<IamUser> = state.users.values().cloned().collect();
        if let Some(prefix) = path_prefix {
            users.retain(|u| u.path.starts_with(&prefix));
        }
        let xml = xml_responses::list_users_response(&users, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn update_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let new_path = req.query_params.get("NewPath").cloned();
        let new_user_name = req.query_params.get("NewUserName").cloned();

        let mut state = self.state.write();

        let user = state.users.get(&user_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            )
        })?;
        let mut user = user.clone();

        if let Some(ref new_name) = new_user_name {
            if new_name != &user_name && state.users.contains_key(new_name) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::CONFLICT,
                    "EntityAlreadyExists",
                    format!("User with name {new_name} already exists."),
                ));
            }
        }

        if let Some(ref path) = new_path {
            user.path = path.clone();
        }

        let actual_new_name = new_user_name.unwrap_or_else(|| user_name.clone());
        user.user_name = actual_new_name.clone();
        user.arn = format!(
            "arn:aws:iam::{}:user{}{}",
            state.account_id,
            if user.path == "/" { "/" } else { &user.path },
            actual_new_name
        );

        state.users.remove(&user_name);
        state.users.insert(actual_new_name.clone(), user);

        // Update references
        if actual_new_name != user_name {
            if let Some(keys) = state.access_keys.remove(&user_name) {
                state.access_keys.insert(actual_new_name.clone(), keys);
            }
            if let Some(policies) = state.user_policies.remove(&user_name) {
                state
                    .user_policies
                    .insert(actual_new_name.clone(), policies);
            }
            if let Some(policies) = state.user_inline_policies.remove(&user_name) {
                state
                    .user_inline_policies
                    .insert(actual_new_name.clone(), policies);
            }
            if let Some(profile) = state.login_profiles.remove(&user_name) {
                state
                    .login_profiles
                    .insert(actual_new_name.clone(), profile);
            }
            for group in state.groups.values_mut() {
                for member in group.members.iter_mut() {
                    if member == &user_name {
                        *member = actual_new_name.clone();
                    }
                }
            }
        }

        let xml = empty_response("UpdateUser", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn tag_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let new_tags = parse_tags(&req.query_params);
        let mut state = self.state.write();

        let user = state.users.get_mut(&user_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            )
        })?;

        for new_tag in new_tags {
            if let Some(existing) = user.tags.iter_mut().find(|t| t.key == new_tag.key) {
                existing.value = new_tag.value;
            } else {
                user.tags.push(new_tag);
            }
        }

        let xml = empty_response("TagUser", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn untag_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let tag_keys = parse_tag_keys(&req.query_params);
        let mut state = self.state.write();

        let user = state.users.get_mut(&user_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            )
        })?;

        user.tags.retain(|t| !tag_keys.contains(&t.key));

        let xml = empty_response("UntagUser", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_user_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let state = self.state.read();

        let user = state.users.get(&user_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            )
        })?;

        let members = tags_xml(&user.tags);
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListUserTagsResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListUserTagsResult>
    <IsTruncated>false</IsTruncated>
    <Tags>
{members}
    </Tags>
  </ListUserTagsResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListUserTagsResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= Access Key operations =========

impl IamService {
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
            access_key_id: format!("FKIA{}", generate_id()),
            secret_access_key: format!("fake{}{}fake", generate_id(), generate_id()),
            user_name: user_name.clone(),
            status: "Active".to_string(),
            created_at: Utc::now(),
        };

        let xml = xml_responses::create_access_key_response(&key, &req.request_id);
        state.access_keys.entry(user_name).or_default().push(key);

        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_access_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = req
            .query_params
            .get("UserName")
            .cloned()
            .unwrap_or_else(|| resolve_calling_user(&self.state.read(), &req.account_id));
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

        let xml = empty_response("DeleteAccessKey", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_access_keys(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = req
            .query_params
            .get("UserName")
            .cloned()
            .unwrap_or_else(|| resolve_calling_user(&self.state.read(), &req.account_id));
        let state = self.state.read();
        let keys = state
            .access_keys
            .get(&user_name)
            .cloned()
            .unwrap_or_default();
        let xml = xml_responses::list_access_keys_response(&keys, &user_name, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn update_access_key(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = req
            .query_params
            .get("UserName")
            .cloned()
            .unwrap_or_else(|| resolve_calling_user(&self.state.read(), &req.account_id));
        let access_key_id = required_param(&req.query_params, "AccessKeyId")?;
        let status = required_param(&req.query_params, "Status")?;
        let mut state = self.state.write();

        if let Some(keys) = state.access_keys.get_mut(&user_name) {
            if let Some(key) = keys.iter_mut().find(|k| k.access_key_id == access_key_id) {
                key.status = status;
            } else {
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

        let xml = empty_response("UpdateAccessKey", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= Role operations =========

impl IamService {
    fn create_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let assume_role_policy = required_param(&req.query_params, "AssumeRolePolicyDocument")?;
        let path = req
            .query_params
            .get("Path")
            .cloned()
            .unwrap_or_else(|| "/".to_string());
        let description = req
            .query_params
            .get("Description")
            .cloned()
            .unwrap_or_default();
        let max_session_duration = req
            .query_params
            .get("MaxSessionDuration")
            .and_then(|v| v.parse().ok())
            .unwrap_or(3600);
        let tags = parse_tags(&req.query_params);
        validate_tags(&tags, 0)?;
        let permissions_boundary = req.query_params.get("PermissionsBoundary").cloned();

        // Validate permissions boundary ARN format
        if let Some(ref boundary) = permissions_boundary {
            if !boundary.contains(":policy/") {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationError",
                    format!("Value ({boundary}) for parameter PermissionsBoundary is invalid."),
                ));
            }
        }

        let mut state = self.state.write();

        if state.roles.contains_key(&role_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                format!("Role with name {role_name} already exists."),
            ));
        }

        let partition = partition_for_region(&req.region);

        // Note: AWS does not validate the assume role policy document format
        // during CreateRole, only during UpdateAssumeRolePolicy.

        let role = IamRole {
            role_id: crate::xml_responses::generate_role_id(),
            arn: format!(
                "arn:{}:iam::{}:role{}{}",
                partition,
                state.account_id,
                if path == "/" { "/" } else { &path },
                role_name
            ),
            role_name: role_name.clone(),
            path,
            assume_role_policy_document: assume_role_policy,
            created_at: Utc::now(),
            description,
            max_session_duration,
            tags,
            permissions_boundary,
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
                format!("Role {role_name} not found"),
            )
        })?;

        let xml = xml_responses::get_role_response(role, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let mut state = self.state.write();

        if !state.roles.contains_key(&role_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            ));
        }

        // Check if role is in any instance profiles
        let in_profiles: Vec<String> = state
            .instance_profiles
            .values()
            .filter(|ip| ip.roles.contains(&role_name))
            .map(|ip| ip.instance_profile_name.clone())
            .collect();

        if !in_profiles.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "DeleteConflict",
                "Cannot delete entity, must remove roles from instance profile first.".to_string(),
            ));
        }

        // Check if role has attached managed policies
        if state
            .role_policies
            .get(&role_name)
            .map(|p| !p.is_empty())
            .unwrap_or(false)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "DeleteConflict",
                "Cannot delete entity, must detach all policies first.".to_string(),
            ));
        }

        // Check if role has inline policies
        if state
            .role_inline_policies
            .get(&role_name)
            .map(|p| !p.is_empty())
            .unwrap_or(false)
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "DeleteConflict",
                "Cannot delete entity, must delete policies first.".to_string(),
            ));
        }

        state.roles.remove(&role_name);
        state.role_policies.remove(&role_name);
        state.role_inline_policies.remove(&role_name);

        let xml = empty_response("DeleteRole", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_roles(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let path_prefix = req.query_params.get("PathPrefix").cloned();
        let mut roles: Vec<IamRole> = state.roles.values().cloned().collect();
        if let Some(prefix) = path_prefix {
            roles.retain(|r| r.path.starts_with(&prefix));
        }
        let xml = xml_responses::list_roles_response(&roles, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn update_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let mut state = self.state.write();

        let role = state.roles.get_mut(&role_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            )
        })?;

        // UpdateRole clears description if not provided
        role.description = req
            .query_params
            .get("Description")
            .cloned()
            .unwrap_or_default();
        if let Some(dur) = req
            .query_params
            .get("MaxSessionDuration")
            .and_then(|v| v.parse().ok())
        {
            role.max_session_duration = dur;
        }

        let xml = empty_response("UpdateRole", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn update_role_description(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let mut state = self.state.write();

        let role = state.roles.get_mut(&role_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            )
        })?;

        if let Some(desc) = req.query_params.get("Description") {
            role.description = desc.clone();
        }

        let role_clone = role.clone();
        let xml = xml_responses::get_role_response(&role_clone, &req.request_id)
            .replace("GetRoleResponse", "UpdateRoleDescriptionResponse")
            .replace("GetRoleResult", "UpdateRoleDescriptionResult");
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn update_assume_role_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let policy_document = required_param(&req.query_params, "PolicyDocument")?;

        // Validate policy document is valid JSON
        let doc: serde_json::Value = match serde_json::from_str(&policy_document) {
            Ok(v) => v,
            Err(_) => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "MalformedPolicyDocument",
                    "Syntax errors in policy.".to_string(),
                ));
            }
        };

        // Validate trust policy constraints
        if let Some(statements) = doc.get("Statement").and_then(|s| s.as_array()) {
            for stmt in statements {
                // Check for prohibited Resource field
                if stmt.get("Resource").is_some() {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "MalformedPolicyDocument",
                        "Has prohibited field Resource.".to_string(),
                    ));
                }
                // Validate actions are valid trust policy actions
                let allowed = [
                    "sts:AssumeRole",
                    "sts:AssumeRoleWithSAML",
                    "sts:AssumeRoleWithWebIdentity",
                ];
                let actions: Vec<&str> = match stmt.get("Action") {
                    Some(serde_json::Value::String(s)) => vec![s.as_str()],
                    Some(serde_json::Value::Array(arr)) => {
                        arr.iter().filter_map(|v| v.as_str()).collect()
                    }
                    _ => vec![],
                };
                for action in &actions {
                    if !allowed.contains(action) {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "MalformedPolicyDocument",
                            "Trust Policy statement actions can only be sts:AssumeRole, sts:AssumeRoleWithSAML,  and sts:AssumeRoleWithWebIdentity".to_string(),
                        ));
                    }
                }
            }
        }

        let mut state = self.state.write();

        let role = state.roles.get_mut(&role_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            )
        })?;

        role.assume_role_policy_document = policy_document;

        let xml = empty_response("UpdateAssumeRolePolicy", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn tag_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let new_tags = parse_tags(&req.query_params);
        let mut state = self.state.write();

        let role = state.roles.get_mut(&role_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            )
        })?;

        // Count existing tags that won't be overwritten by new tags
        let existing_count = role
            .tags
            .iter()
            .filter(|t| !new_tags.iter().any(|nt| nt.key == t.key))
            .count();
        validate_tags(&new_tags, existing_count)?;

        for new_tag in new_tags {
            if let Some(existing) = role.tags.iter_mut().find(|t| t.key == new_tag.key) {
                existing.value = new_tag.value;
            } else {
                role.tags.push(new_tag);
            }
        }

        let xml = empty_response("TagRole", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn untag_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let tag_keys = parse_tag_keys(&req.query_params);
        validate_untag_keys(&tag_keys)?;
        let mut state = self.state.write();

        let role = state.roles.get_mut(&role_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            )
        })?;

        role.tags.retain(|t| !tag_keys.contains(&t.key));

        let xml = empty_response("UntagRole", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_role_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let state = self.state.read();

        let role = state.roles.get(&role_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            )
        })?;

        let xml = paginated_tags_response("ListRoleTags", &role.tags, req);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn put_role_permissions_boundary(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let boundary = required_param(&req.query_params, "PermissionsBoundary")?;

        // Validate boundary ARN format
        if !boundary.contains(":policy/") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                format!("Value ({boundary}) for parameter PermissionsBoundary is invalid."),
            ));
        }

        let mut state = self.state.write();
        let role = state.roles.get_mut(&role_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            )
        })?;

        role.permissions_boundary = Some(boundary);
        let xml = empty_response("PutRolePermissionsBoundary", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_role_permissions_boundary(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let mut state = self.state.write();

        let role = state.roles.get_mut(&role_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            )
        })?;

        role.permissions_boundary = None;
        let xml = empty_response("DeleteRolePermissionsBoundary", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= Policy operations =========

impl IamService {
    fn create_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let policy_name = required_param(&req.query_params, "PolicyName")?;
        let policy_document = required_param(&req.query_params, "PolicyDocument")?;
        let path = req
            .query_params
            .get("Path")
            .cloned()
            .unwrap_or_else(|| "/".to_string());
        let description = req
            .query_params
            .get("Description")
            .cloned()
            .unwrap_or_default();
        let tags = parse_tags(&req.query_params);
        validate_tags(&tags, 0)?;

        let mut state = self.state.write();

        let partition = partition_for_region(&req.region);
        let arn = format!(
            "arn:{}:iam::{}:policy{}{}",
            partition, state.account_id, path, policy_name
        );

        // Check for duplicate policy ARN
        if state.policies.contains_key(&arn) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                format!("A policy called {policy_name} already exists. Duplicate names are not allowed."),
            ));
        }

        let now = Utc::now();
        let version = PolicyVersion {
            version_id: "v1".to_string(),
            document: policy_document,
            is_default: true,
            created_at: now,
        };

        let policy = IamPolicy {
            policy_id: format!("ANPA{}", generate_id()),
            arn: arn.clone(),
            policy_name,
            path,
            description,
            created_at: now,
            tags,
            default_version_id: "v1".to_string(),
            versions: vec![version],
            next_version_num: 2,
            attachment_count: 0,
        };

        let xml = xml_responses::create_policy_response(&policy, &req.request_id);
        state.policies.insert(arn, policy);

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

        // Remove from any attachments
        for arns in state.role_policies.values_mut() {
            arns.retain(|a| a != &policy_arn);
        }
        for arns in state.user_policies.values_mut() {
            arns.retain(|a| a != &policy_arn);
        }
        for group in state.groups.values_mut() {
            group.attached_policies.retain(|a| a != &policy_arn);
        }

        let xml = empty_response("DeletePolicy", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let path_prefix = req.query_params.get("PathPrefix").cloned();
        let scope = req.query_params.get("Scope").cloned();
        let mut policies: Vec<IamPolicy> = state.policies.values().cloned().collect();
        if let Some(prefix) = path_prefix {
            policies.retain(|p| p.path.starts_with(&prefix));
        }
        // If scope is "Local", show only customer-managed
        // If scope is "AWS", show only AWS-managed (we have none)
        if let Some(s) = scope {
            if s == "AWS" {
                policies.clear();
            }
        }
        let xml = xml_responses::list_policies_response(&policies, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn tag_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let new_tags = parse_tags(&req.query_params);
        let mut state = self.state.write();

        let policy = state.policies.get_mut(&policy_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} does not exist."),
            )
        })?;

        validate_tags(&new_tags, policy.tags.len())?;

        for new_tag in new_tags {
            if let Some(existing) = policy.tags.iter_mut().find(|t| t.key == new_tag.key) {
                existing.value = new_tag.value;
            } else {
                policy.tags.push(new_tag);
            }
        }

        let xml = empty_response("TagPolicy", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn untag_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let tag_keys = parse_tag_keys(&req.query_params);
        validate_untag_keys(&tag_keys)?;
        let mut state = self.state.write();

        let policy = state.policies.get_mut(&policy_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} does not exist."),
            )
        })?;

        policy.tags.retain(|t| !tag_keys.contains(&t.key));

        let xml = empty_response("UntagPolicy", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_policy_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let state = self.state.read();

        let policy = state.policies.get(&policy_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} does not exist."),
            )
        })?;

        let xml = paginated_tags_response("ListPolicyTags", &policy.tags, req);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= Policy Version operations =========

impl IamService {
    fn create_policy_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let policy_document = required_param(&req.query_params, "PolicyDocument")?;
        let set_as_default = req
            .query_params
            .get("SetAsDefault")
            .map(|v| v == "true")
            .unwrap_or(false);

        let mut state = self.state.write();

        let policy = state.policies.get_mut(&policy_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} not found"),
            )
        })?;

        if policy.versions.len() >= 5 {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "LimitExceeded",
                "A managed policy can have up to 5 versions.".to_string(),
            ));
        }

        let next_version = policy.next_version_num;
        policy.next_version_num += 1;
        let version_id = format!("v{next_version}");

        if set_as_default {
            for v in &mut policy.versions {
                v.is_default = false;
            }
            policy.default_version_id = version_id.clone();
        }

        let version = PolicyVersion {
            version_id: version_id.clone(),
            document: policy_document,
            is_default: set_as_default,
            created_at: Utc::now(),
        };

        policy.versions.push(version.clone());

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<CreatePolicyVersionResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <CreatePolicyVersionResult>
    <PolicyVersion>
      <VersionId>{}</VersionId>
      <IsDefaultVersion>{}</IsDefaultVersion>
      <Document>{}</Document>
      <CreateDate>{}</CreateDate>
    </PolicyVersion>
  </CreatePolicyVersionResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CreatePolicyVersionResponse>"#,
            version.version_id,
            version.is_default,
            xml_escape(&version.document),
            version.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_policy_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let version_id = required_param(&req.query_params, "VersionId")?;
        let state = self.state.read();

        let policy = state.policies.get(&policy_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} not found"),
            )
        })?;

        let version = policy
            .versions
            .iter()
            .find(|v| v.version_id == version_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchEntity",
                    format!(
                        "Policy {policy_arn} version {version_id} does not exist or is not attachable."
                    ),
                )
            })?;

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetPolicyVersionResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetPolicyVersionResult>
    <PolicyVersion>
      <Document>{}</Document>
      <VersionId>{}</VersionId>
      <IsDefaultVersion>{}</IsDefaultVersion>
      <CreateDate>{}</CreateDate>
    </PolicyVersion>
  </GetPolicyVersionResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetPolicyVersionResponse>"#,
            url_encode(&version.document),
            version.version_id,
            version.is_default,
            version.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
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
                format!("Policy {policy_arn} not found"),
            )
        })?;

        let members: String = policy
            .versions
            .iter()
            .map(|v| {
                format!(
                    "      <member>\n        <VersionId>{}</VersionId>\n        <IsDefaultVersion>{}</IsDefaultVersion>\n        <Document>{}</Document>\n        <CreateDate>{}</CreateDate>\n      </member>",
                    v.version_id,
                    v.is_default,
                    url_encode(&v.document),
                    v.created_at.format("%Y-%m-%dT%H:%M:%SZ")
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListPolicyVersionsResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListPolicyVersionsResult>
    <IsTruncated>false</IsTruncated>
    <Versions>
{members}
    </Versions>
  </ListPolicyVersionsResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListPolicyVersionsResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_policy_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let version_id = required_param(&req.query_params, "VersionId")?;

        let mut state = self.state.write();

        let policy = state.policies.get_mut(&policy_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} not found"),
            )
        })?;

        // Can't delete the default version
        if let Some(v) = policy.versions.iter().find(|v| v.version_id == version_id) {
            if v.is_default {
                return Err(AwsServiceError::aws_error(
                    StatusCode::CONFLICT,
                    "DeleteConflict",
                    "Cannot delete the default version of a policy.".to_string(),
                ));
            }
        } else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy version {version_id} not found"),
            ));
        }

        policy.versions.retain(|v| v.version_id != version_id);

        let xml = empty_response("DeletePolicyVersion", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn set_default_policy_version(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let version_id = required_param(&req.query_params, "VersionId")?;

        // Validate version ID format: must match v[1-9][0-9]*(\.[A-Za-z0-9-]*)?
        let valid_format = version_id.starts_with('v')
            && version_id.len() > 1
            && version_id[1..2]
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_digit() && c != '0')
            && version_id[1..]
                .split_once('.')
                .map(|(num, ext)| {
                    num.chars().all(|c| c.is_ascii_digit())
                        && ext.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
                })
                .unwrap_or_else(|| version_id[1..].chars().all(|c| c.is_ascii_digit()));

        if !valid_format {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                format!(
                    "Value '{}' at 'versionId' failed to satisfy constraint: Member must satisfy regular expression pattern: v[1-9][0-9]*(\\.[A-Za-z0-9-]*)?",
                    version_id
                ),
            ));
        }

        let mut state = self.state.write();

        let policy = state.policies.get_mut(&policy_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} not found"),
            )
        })?;

        if !policy.versions.iter().any(|v| v.version_id == version_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!(
                    "Policy {policy_arn} version {version_id} does not exist or is not attachable."
                ),
            ));
        }

        for v in &mut policy.versions {
            v.is_default = v.version_id == version_id;
        }
        policy.default_version_id = version_id;

        let xml = empty_response("SetDefaultPolicyVersion", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= Role policy (managed) operations =========

impl IamService {
    fn attach_role_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;

        let mut state = self.state.write();

        if !state.roles.contains_key(&role_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            ));
        }

        // Check policy exists (allow AWS managed policies)
        if !policy_arn.contains(":aws:policy/") && !state.policies.contains_key(&policy_arn) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} does not exist or is not attachable."),
            ));
        }

        let arns = state.role_policies.entry(role_name).or_default();
        if !arns.contains(&policy_arn) {
            arns.push(policy_arn.clone());
            // Increment attachment count
            if let Some(p) = state.policies.get_mut(&policy_arn) {
                p.attachment_count += 1;
            }
        }

        let xml = empty_response("AttachRolePolicy", &req.request_id);
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
                format!("Role {role_name} not found"),
            ));
        }

        let attached = state
            .role_policies
            .get(&role_name)
            .map(|arns| arns.contains(&policy_arn))
            .unwrap_or(false);

        if !attached {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} was not found."),
            ));
        }

        if let Some(arns) = state.role_policies.get_mut(&role_name) {
            arns.retain(|a| a != &policy_arn);
            if let Some(p) = state.policies.get_mut(&policy_arn) {
                p.attachment_count = p.attachment_count.saturating_sub(1);
            }
        }

        let xml = empty_response("DetachRolePolicy", &req.request_id);
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
                format!("Role {role_name} not found"),
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
}

// ========= Role inline policy operations =========

impl IamService {
    fn put_role_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let policy_name = required_param(&req.query_params, "PolicyName")?;
        let policy_document = required_param(&req.query_params, "PolicyDocument")?;

        let mut state = self.state.write();

        if !state.roles.contains_key(&role_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            ));
        }

        state
            .role_inline_policies
            .entry(role_name)
            .or_default()
            .insert(policy_name, policy_document);

        let xml = empty_response("PutRolePolicy", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_role_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let policy_name = required_param(&req.query_params, "PolicyName")?;
        let state = self.state.read();

        if !state.roles.contains_key(&role_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            ));
        }

        let doc = state
            .role_inline_policies
            .get(&role_name)
            .and_then(|policies| policies.get(&policy_name))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchEntity",
                    format!("The role policy with name {policy_name} cannot be found."),
                )
            })?;

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetRolePolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetRolePolicyResult>
    <RoleName>{}</RoleName>
    <PolicyName>{}</PolicyName>
    <PolicyDocument>{}</PolicyDocument>
  </GetRolePolicyResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetRolePolicyResponse>"#,
            xml_escape(&role_name),
            xml_escape(&policy_name),
            url_encode(doc),
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_role_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let policy_name = required_param(&req.query_params, "PolicyName")?;

        let mut state = self.state.write();

        if !state.roles.contains_key(&role_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            ));
        }

        let policy_exists = state
            .role_inline_policies
            .get(&role_name)
            .is_some_and(|p| p.contains_key(&policy_name));

        if !policy_exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The role policy with name {policy_name} cannot be found."),
            ));
        }

        if let Some(policies) = state.role_inline_policies.get_mut(&role_name) {
            policies.remove(&policy_name);
        }

        let xml = empty_response("DeleteRolePolicy", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_role_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let state = self.state.read();

        if !state.roles.contains_key(&role_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            ));
        }

        let policy_names: Vec<String> = state
            .role_inline_policies
            .get(&role_name)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();

        let xml = xml_responses::list_role_policies_response(&policy_names, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= User policy operations =========

impl IamService {
    fn attach_user_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;

        let mut state = self.state.write();

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        // Check policy exists (allow AWS managed policies)
        if !policy_arn.contains(":aws:policy/") && !state.policies.contains_key(&policy_arn) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} does not exist or is not attachable."),
            ));
        }

        let arns = state.user_policies.entry(user_name).or_default();
        if !arns.contains(&policy_arn) {
            arns.push(policy_arn.clone());
            if let Some(p) = state.policies.get_mut(&policy_arn) {
                p.attachment_count += 1;
            }
        }

        let xml = empty_response("AttachUserPolicy", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn detach_user_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;

        let mut state = self.state.write();

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        let attached = state
            .user_policies
            .get(&user_name)
            .map(|arns| arns.contains(&policy_arn))
            .unwrap_or(false);

        if !attached {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} was not found."),
            ));
        }

        if let Some(arns) = state.user_policies.get_mut(&user_name) {
            arns.retain(|a| a != &policy_arn);
            if let Some(p) = state.policies.get_mut(&policy_arn) {
                p.attachment_count = p.attachment_count.saturating_sub(1);
            }
        }

        let xml = empty_response("DetachUserPolicy", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_attached_user_policies(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let state = self.state.read();

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        let policy_arns = state
            .user_policies
            .get(&user_name)
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
<ListAttachedUserPoliciesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListAttachedUserPoliciesResult>
    <IsTruncated>false</IsTruncated>
    <AttachedPolicies>
{members}
    </AttachedPolicies>
  </ListAttachedUserPoliciesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListAttachedUserPoliciesResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn put_user_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let policy_name = required_param(&req.query_params, "PolicyName")?;
        let policy_document = required_param(&req.query_params, "PolicyDocument")?;

        let mut state = self.state.write();

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        state
            .user_inline_policies
            .entry(user_name)
            .or_default()
            .insert(policy_name, policy_document);

        let xml = empty_response("PutUserPolicy", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_user_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let policy_name = required_param(&req.query_params, "PolicyName")?;
        let state = self.state.read();

        let doc = state
            .user_inline_policies
            .get(&user_name)
            .and_then(|policies| policies.get(&policy_name))
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchEntity",
                    format!("The user policy with name {policy_name} cannot be found."),
                )
            })?;

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetUserPolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetUserPolicyResult>
    <UserName>{}</UserName>
    <PolicyName>{}</PolicyName>
    <PolicyDocument>{}</PolicyDocument>
  </GetUserPolicyResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetUserPolicyResponse>"#,
            xml_escape(&user_name),
            xml_escape(&policy_name),
            url_encode(doc),
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_user_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let policy_name = required_param(&req.query_params, "PolicyName")?;

        let mut state = self.state.write();

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        if let Some(policies) = state.user_inline_policies.get_mut(&user_name) {
            policies.remove(&policy_name);
        }

        let xml = empty_response("DeleteUserPolicy", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_user_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let state = self.state.read();

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        let policy_names: Vec<String> = state
            .user_inline_policies
            .get(&user_name)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();

        let members: String = policy_names
            .iter()
            .map(|name| format!("      <member>{name}</member>"))
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListUserPoliciesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListUserPoliciesResult>
    <IsTruncated>false</IsTruncated>
    <PolicyNames>
{members}
    </PolicyNames>
  </ListUserPoliciesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListUserPoliciesResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= Group operations =========

impl IamService {
    fn create_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let path = req
            .query_params
            .get("Path")
            .cloned()
            .unwrap_or_else(|| "/".to_string());

        let mut state = self.state.write();

        if state.groups.contains_key(&group_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                format!("Group {group_name} already exists"),
            ));
        }

        let group = IamGroup {
            group_id: format!("AGPA{}", generate_id()),
            arn: format!(
                "arn:aws:iam::{}:group{}{}",
                state.account_id,
                if path == "/" { "/" } else { &path },
                group_name
            ),
            group_name: group_name.clone(),
            path,
            created_at: Utc::now(),
            members: Vec::new(),
            inline_policies: std::collections::HashMap::new(),
            attached_policies: Vec::new(),
        };

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<CreateGroupResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <CreateGroupResult>
    <Group>
      <Path>{}</Path>
      <GroupName>{}</GroupName>
      <GroupId>{}</GroupId>
      <Arn>{}</Arn>
      <CreateDate>{}</CreateDate>
    </Group>
  </CreateGroupResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CreateGroupResponse>"#,
            group.path,
            group.group_name,
            group.group_id,
            group.arn,
            group.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
            req.request_id
        );

        state.groups.insert(group_name, group);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let state = self.state.read();

        let group = state.groups.get(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Group {group_name} not found"),
            )
        })?;

        let user_members: String = group
            .members
            .iter()
            .filter_map(|uname| {
                state.users.get(uname).map(|u| {
                    format!(
                        "      <member>\n        <Path>{}</Path>\n        <UserName>{}</UserName>\n        <UserId>{}</UserId>\n        <Arn>{}</Arn>\n        <CreateDate>{}</CreateDate>\n      </member>",
                        u.path, u.user_name, u.user_id, u.arn, u.created_at.format("%Y-%m-%dT%H:%M:%SZ")
                    )
                })
            })
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetGroupResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetGroupResult>
    <Group>
      <Path>{}</Path>
      <GroupName>{}</GroupName>
      <GroupId>{}</GroupId>
      <Arn>{}</Arn>
      <CreateDate>{}</CreateDate>
    </Group>
    <IsTruncated>false</IsTruncated>
    <Users>
{user_members}
    </Users>
  </GetGroupResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetGroupResponse>"#,
            group.path,
            group.group_name,
            group.group_id,
            group.arn,
            group.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let mut state = self.state.write();

        if state.groups.remove(&group_name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The group with name {group_name} cannot be found."),
            ));
        }

        // Decrement attachment counts for policies
        // (No need as we're removing the group)

        let xml = empty_response("DeleteGroup", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let path_prefix = req.query_params.get("PathPrefix").cloned();
        let mut groups: Vec<&IamGroup> = state.groups.values().collect();
        if let Some(prefix) = &path_prefix {
            groups.retain(|g| g.path.starts_with(prefix));
        }

        let members: String = groups
            .iter()
            .map(|g| {
                format!(
                    "      <member>\n        <Path>{}</Path>\n        <GroupName>{}</GroupName>\n        <GroupId>{}</GroupId>\n        <Arn>{}</Arn>\n        <CreateDate>{}</CreateDate>\n      </member>",
                    g.path, g.group_name, g.group_id, g.arn, g.created_at.format("%Y-%m-%dT%H:%M:%SZ")
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListGroupsResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListGroupsResult>
    <IsTruncated>false</IsTruncated>
    <Groups>
{members}
    </Groups>
  </ListGroupsResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListGroupsResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn update_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let new_group_name = req.query_params.get("NewGroupName").cloned();
        let new_path = req.query_params.get("NewPath").cloned();

        let mut state = self.state.write();

        let group = state.groups.get(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The group with name {group_name} cannot be found."),
            )
        })?;
        let mut group = group.clone();

        if let Some(ref new_name) = new_group_name {
            if new_name != &group_name && state.groups.contains_key(new_name) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::CONFLICT,
                    "EntityAlreadyExists",
                    format!("Group {new_name} already exists"),
                ));
            }
        }

        if let Some(ref path) = new_path {
            group.path = path.clone();
        }

        let actual_new_name = new_group_name.unwrap_or_else(|| group_name.clone());
        group.group_name = actual_new_name.clone();
        group.arn = format!(
            "arn:aws:iam::{}:group{}{}",
            state.account_id,
            if group.path == "/" { "/" } else { &group.path },
            actual_new_name
        );

        state.groups.remove(&group_name);
        state.groups.insert(actual_new_name, group);

        let xml = empty_response("UpdateGroup", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn add_user_to_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let user_name = required_param(&req.query_params, "UserName")?;

        let mut state = self.state.write();

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        let group = state.groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Group {group_name} not found"),
            )
        })?;

        if !group.members.contains(&user_name) {
            group.members.push(user_name);
        }

        let xml = empty_response("AddUserToGroup", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn remove_user_from_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let user_name = required_param(&req.query_params, "UserName")?;

        let mut state = self.state.write();

        let group = state.groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Group {group_name} not found"),
            )
        })?;

        let before = group.members.len();
        group.members.retain(|m| m != &user_name);
        if group.members.len() == before {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("User {user_name} not in group {group_name}"),
            ));
        }

        let xml = empty_response("RemoveUserFromGroup", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_groups_for_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let state = self.state.read();

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        let groups: Vec<&IamGroup> = state
            .groups
            .values()
            .filter(|g| g.members.contains(&user_name))
            .collect();

        let members: String = groups
            .iter()
            .map(|g| {
                format!(
                    "      <member>\n        <Path>{}</Path>\n        <GroupName>{}</GroupName>\n        <GroupId>{}</GroupId>\n        <Arn>{}</Arn>\n        <CreateDate>{}</CreateDate>\n      </member>",
                    g.path, g.group_name, g.group_id, g.arn, g.created_at.format("%Y-%m-%dT%H:%M:%SZ")
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListGroupsForUserResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListGroupsForUserResult>
    <IsTruncated>false</IsTruncated>
    <Groups>
{members}
    </Groups>
  </ListGroupsForUserResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListGroupsForUserResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= Group policy operations =========

impl IamService {
    fn put_group_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let policy_name = required_param(&req.query_params, "PolicyName")?;
        let policy_document = required_param(&req.query_params, "PolicyDocument")?;

        let mut state = self.state.write();

        let group = state.groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The group with name {group_name} cannot be found."),
            )
        })?;

        group.inline_policies.insert(policy_name, policy_document);

        let xml = empty_response("PutGroupPolicy", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_group_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let policy_name = required_param(&req.query_params, "PolicyName")?;
        let state = self.state.read();

        let group = state.groups.get(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The group with name {group_name} cannot be found."),
            )
        })?;

        let doc = group.inline_policies.get(&policy_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_name} not found"),
            )
        })?;

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetGroupPolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetGroupPolicyResult>
    <GroupName>{}</GroupName>
    <PolicyName>{}</PolicyName>
    <PolicyDocument>{}</PolicyDocument>
  </GetGroupPolicyResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetGroupPolicyResponse>"#,
            xml_escape(&group_name),
            xml_escape(&policy_name),
            url_encode(doc),
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_group_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let policy_name = required_param(&req.query_params, "PolicyName")?;

        let mut state = self.state.write();

        let group = state.groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The group with name {group_name} cannot be found."),
            )
        })?;

        group.inline_policies.remove(&policy_name);

        let xml = empty_response("DeleteGroupPolicy", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_group_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let state = self.state.read();

        let group = state.groups.get(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The group with name {group_name} cannot be found."),
            )
        })?;

        let policy_names: Vec<String> = group.inline_policies.keys().cloned().collect();
        let members: String = policy_names
            .iter()
            .map(|name| format!("      <member>{name}</member>"))
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListGroupPoliciesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListGroupPoliciesResult>
    <IsTruncated>false</IsTruncated>
    <PolicyNames>
{members}
    </PolicyNames>
  </ListGroupPoliciesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListGroupPoliciesResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn attach_group_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;

        let mut state = self.state.write();

        let group = state.groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The group with name {group_name} cannot be found."),
            )
        })?;

        if !group.attached_policies.contains(&policy_arn) {
            group.attached_policies.push(policy_arn.clone());
            if let Some(p) = state.policies.get_mut(&policy_arn) {
                p.attachment_count += 1;
            }
        }

        let xml = empty_response("AttachGroupPolicy", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn detach_group_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;

        let mut state = self.state.write();

        let group = state.groups.get_mut(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The group with name {group_name} cannot be found."),
            )
        })?;

        if !group.attached_policies.contains(&policy_arn) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} was not found."),
            ));
        }

        let before = group.attached_policies.len();
        group.attached_policies.retain(|a| a != &policy_arn);
        if group.attached_policies.len() < before {
            if let Some(p) = state.policies.get_mut(&policy_arn) {
                p.attachment_count = p.attachment_count.saturating_sub(1);
            }
        }

        let xml = empty_response("DetachGroupPolicy", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_attached_group_policies(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let state = self.state.read();

        let group = state.groups.get(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The group with name {group_name} cannot be found."),
            )
        })?;

        let members: String = group
            .attached_policies
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
<ListAttachedGroupPoliciesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListAttachedGroupPoliciesResult>
    <IsTruncated>false</IsTruncated>
    <AttachedPolicies>
{members}
    </AttachedPolicies>
  </ListAttachedGroupPoliciesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListAttachedGroupPoliciesResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= Instance Profile operations =========

impl IamService {
    fn create_instance_profile(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "InstanceProfileName")?;
        let path = req
            .query_params
            .get("Path")
            .cloned()
            .unwrap_or_else(|| "/".to_string());
        let tags = parse_tags(&req.query_params);

        let mut state = self.state.write();

        if state.instance_profiles.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                format!("Instance Profile {name} already exists."),
            ));
        }

        let ip = IamInstanceProfile {
            instance_profile_id: format!("AIPA{}", generate_id()),
            arn: format!(
                "arn:aws:iam::{}:instance-profile{}{}",
                state.account_id,
                if path == "/" { "/" } else { &path },
                name
            ),
            instance_profile_name: name.clone(),
            path,
            created_at: Utc::now(),
            roles: Vec::new(),
            tags,
        };

        let xml = self.instance_profile_xml("CreateInstanceProfile", &ip, &state, &req.request_id);
        state.instance_profiles.insert(name, ip);

        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_instance_profile(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "InstanceProfileName")?;
        let state = self.state.read();

        let ip = state.instance_profiles.get(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Instance Profile {name} not found"),
            )
        })?;

        let xml = self.instance_profile_xml("GetInstanceProfile", ip, &state, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_instance_profile(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "InstanceProfileName")?;
        let mut state = self.state.write();

        let ip = state.instance_profiles.get(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Instance Profile {name} not found"),
            )
        })?;

        if !ip.roles.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "DeleteConflict",
                "Cannot delete entity, must remove roles from instance profile first.".to_string(),
            ));
        }

        state.instance_profiles.remove(&name);

        let xml = empty_response("DeleteInstanceProfile", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_instance_profiles(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let path_prefix = req.query_params.get("PathPrefix").cloned();

        let profiles: Vec<&IamInstanceProfile> = state
            .instance_profiles
            .values()
            .filter(|ip| {
                path_prefix
                    .as_ref()
                    .map(|p| ip.path.starts_with(p))
                    .unwrap_or(true)
            })
            .collect();

        let members: String = profiles
            .iter()
            .map(|ip| self.instance_profile_member_xml(ip, &state))
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListInstanceProfilesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListInstanceProfilesResult>
    <IsTruncated>false</IsTruncated>
    <InstanceProfiles>
{members}
    </InstanceProfiles>
  </ListInstanceProfilesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListInstanceProfilesResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn add_role_to_instance_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let profile_name = required_param(&req.query_params, "InstanceProfileName")?;
        let role_name = required_param(&req.query_params, "RoleName")?;

        let mut state = self.state.write();

        if !state.roles.contains_key(&role_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            ));
        }

        let ip = state
            .instance_profiles
            .get_mut(&profile_name)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchEntity",
                    format!("Instance Profile {profile_name} not found"),
                )
            })?;

        if !ip.roles.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "LimitExceeded",
                "Cannot exceed quota for InstanceSessionsPerInstanceProfile: 1".to_string(),
            ));
        }

        ip.roles.push(role_name);

        let xml = empty_response("AddRoleToInstanceProfile", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn remove_role_from_instance_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let profile_name = required_param(&req.query_params, "InstanceProfileName")?;
        let role_name = required_param(&req.query_params, "RoleName")?;

        let mut state = self.state.write();

        let ip = state
            .instance_profiles
            .get_mut(&profile_name)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchEntity",
                    format!("Instance Profile {profile_name} not found"),
                )
            })?;

        ip.roles.retain(|r| r != &role_name);

        let xml = empty_response("RemoveRoleFromInstanceProfile", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_instance_profiles_for_role(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let state = self.state.read();

        if !state.roles.contains_key(&role_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            ));
        }

        let profiles: Vec<&IamInstanceProfile> = state
            .instance_profiles
            .values()
            .filter(|ip| ip.roles.contains(&role_name))
            .collect();

        let members: String = profiles
            .iter()
            .map(|ip| self.instance_profile_member_xml(ip, &state))
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListInstanceProfilesForRoleResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListInstanceProfilesForRoleResult>
    <IsTruncated>false</IsTruncated>
    <InstanceProfiles>
{members}
    </InstanceProfiles>
  </ListInstanceProfilesForRoleResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListInstanceProfilesForRoleResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn tag_instance_profile(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "InstanceProfileName")?;
        let new_tags = parse_tags(&req.query_params);
        let mut state = self.state.write();

        let ip = state.instance_profiles.get_mut(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Instance Profile {name} not found"),
            )
        })?;

        for new_tag in new_tags {
            if let Some(existing) = ip.tags.iter_mut().find(|t| t.key == new_tag.key) {
                existing.value = new_tag.value;
            } else {
                ip.tags.push(new_tag);
            }
        }

        let xml = empty_response("TagInstanceProfile", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn untag_instance_profile(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "InstanceProfileName")?;
        let tag_keys = parse_tag_keys(&req.query_params);
        let mut state = self.state.write();

        let ip = state.instance_profiles.get_mut(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Instance Profile {name} not found"),
            )
        })?;

        ip.tags.retain(|t| !tag_keys.contains(&t.key));

        let xml = empty_response("UntagInstanceProfile", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_instance_profile_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "InstanceProfileName")?;
        let state = self.state.read();

        let ip = state.instance_profiles.get(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Instance Profile {name} not found"),
            )
        })?;

        let members = tags_xml(&ip.tags);
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListInstanceProfileTagsResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListInstanceProfileTagsResult>
    <IsTruncated>false</IsTruncated>
    <Tags>
{members}
    </Tags>
  </ListInstanceProfileTagsResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListInstanceProfileTagsResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    // Helper for instance profile XML
    fn instance_profile_xml(
        &self,
        action: &str,
        ip: &IamInstanceProfile,
        state: &crate::state::IamState,
        request_id: &str,
    ) -> String {
        let roles_xml = self.roles_xml_for_instance_profile(ip, state);
        let tags_members = tags_xml(&ip.tags);

        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<{action}Response xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <{action}Result>
    <InstanceProfile>
      <InstanceProfileName>{}</InstanceProfileName>
      <InstanceProfileId>{}</InstanceProfileId>
      <Arn>{}</Arn>
      <Path>{}</Path>
      <Roles>
{roles_xml}
      </Roles>
      <Tags>
{tags_members}
      </Tags>
      <CreateDate>{}</CreateDate>
    </InstanceProfile>
  </{action}Result>
  <ResponseMetadata>
    <RequestId>{request_id}</RequestId>
  </ResponseMetadata>
</{action}Response>"#,
            ip.instance_profile_name,
            ip.instance_profile_id,
            ip.arn,
            ip.path,
            ip.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
        )
    }

    fn instance_profile_member_xml(
        &self,
        ip: &IamInstanceProfile,
        state: &crate::state::IamState,
    ) -> String {
        let roles_xml = self.roles_xml_for_instance_profile(ip, state);
        let tags_members = tags_xml(&ip.tags);

        format!(
            "      <member>\n        <InstanceProfileName>{}</InstanceProfileName>\n        <InstanceProfileId>{}</InstanceProfileId>\n        <Arn>{}</Arn>\n        <Path>{}</Path>\n        <Roles>\n{roles_xml}\n        </Roles>\n        <Tags>\n{tags_members}\n        </Tags>\n        <CreateDate>{}</CreateDate>\n      </member>",
            ip.instance_profile_name,
            ip.instance_profile_id,
            ip.arn,
            ip.path,
            ip.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
        )
    }

    fn roles_xml_for_instance_profile(
        &self,
        ip: &IamInstanceProfile,
        state: &crate::state::IamState,
    ) -> String {
        ip.roles
            .iter()
            .filter_map(|rn| {
                state.roles.get(rn).map(|r| {
                    format!(
                        "        <member>\n          <Path>{}</Path>\n          <RoleName>{}</RoleName>\n          <RoleId>{}</RoleId>\n          <Arn>{}</Arn>\n          <CreateDate>{}</CreateDate>\n          <AssumeRolePolicyDocument>{}</AssumeRolePolicyDocument>\n        </member>",
                        r.path, r.role_name, r.role_id, r.arn, r.created_at.format("%Y-%m-%dT%H:%M:%SZ"), url_encode(&r.assume_role_policy_document)
                    )
                })
            })
            .collect::<Vec<_>>()
            .join("\n")
    }
}

// ========= Login Profile operations =========

impl IamService {
    fn create_login_profile(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let _password = required_param(&req.query_params, "Password")?;
        let password_reset_required = req
            .query_params
            .get("PasswordResetRequired")
            .map(|v| v == "true")
            .unwrap_or(false);

        let mut state = self.state.write();

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        if state.login_profiles.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                format!("User {user_name} already has password"),
            ));
        }

        let profile = LoginProfile {
            user_name: user_name.clone(),
            created_at: Utc::now(),
            password_reset_required,
        };

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<CreateLoginProfileResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <CreateLoginProfileResult>
    <LoginProfile>
      <UserName>{}</UserName>
      <CreateDate>{}</CreateDate>
      <PasswordResetRequired>{}</PasswordResetRequired>
    </LoginProfile>
  </CreateLoginProfileResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CreateLoginProfileResponse>"#,
            profile.user_name,
            profile.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
            profile.password_reset_required,
            req.request_id
        );

        state.login_profiles.insert(user_name, profile);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_login_profile(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let state = self.state.read();

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        let profile = state.login_profiles.get(&user_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Login Profile for user {user_name} cannot be found."),
            )
        })?;

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetLoginProfileResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetLoginProfileResult>
    <LoginProfile>
      <UserName>{}</UserName>
      <CreateDate>{}</CreateDate>
      <PasswordResetRequired>{}</PasswordResetRequired>
    </LoginProfile>
  </GetLoginProfileResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetLoginProfileResponse>"#,
            profile.user_name,
            profile.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
            profile.password_reset_required,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn update_login_profile(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;

        let mut state = self.state.write();

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        let profile = state.login_profiles.get_mut(&user_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Login Profile for user {user_name} cannot be found."),
            )
        })?;

        if let Some(v) = req.query_params.get("PasswordResetRequired") {
            profile.password_reset_required = v == "true";
        }

        let xml = empty_response("UpdateLoginProfile", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_login_profile(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let mut state = self.state.write();

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        if state.login_profiles.remove(&user_name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Login profile for {user_name} not found"),
            ));
        }

        let xml = empty_response("DeleteLoginProfile", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= SAML Provider operations =========

impl IamService {
    fn create_saml_provider(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "Name")?;
        let saml_metadata_document = required_param(&req.query_params, "SAMLMetadataDocument")?;
        let tags = parse_tags(&req.query_params);

        let mut state = self.state.write();

        let arn = format!("arn:aws:iam::{}:saml-provider/{}", state.account_id, name);

        let provider = SamlProvider {
            arn: arn.clone(),
            name,
            saml_metadata_document,
            created_at: Utc::now(),
            valid_until: Utc::now() + chrono::Duration::days(365),
            tags,
        };

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<CreateSAMLProviderResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <CreateSAMLProviderResult>
    <SAMLProviderArn>{}</SAMLProviderArn>
  </CreateSAMLProviderResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CreateSAMLProviderResponse>"#,
            arn, req.request_id
        );

        state.saml_providers.insert(arn, provider);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_saml_provider(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "SAMLProviderArn")?;
        let state = self.state.read();

        let provider = state.saml_providers.get(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("SAML provider {arn} not found"),
            )
        })?;

        let tags_members = tags_xml(&provider.tags);
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetSAMLProviderResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetSAMLProviderResult>
    <SAMLMetadataDocument>{}</SAMLMetadataDocument>
    <CreateDate>{}</CreateDate>
    <ValidUntil>{}</ValidUntil>
    <Tags>
{tags_members}
    </Tags>
  </GetSAMLProviderResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetSAMLProviderResponse>"#,
            xml_escape(&provider.saml_metadata_document),
            provider.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
            provider.valid_until.format("%Y-%m-%dT%H:%M:%SZ"),
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_saml_provider(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "SAMLProviderArn")?;
        let mut state = self.state.write();

        if state.saml_providers.remove(&arn).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("SAML provider {arn} not found"),
            ));
        }

        let xml = empty_response("DeleteSAMLProvider", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_saml_providers(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        let members: String = state
            .saml_providers
            .values()
            .map(|p| {
                format!(
                    "      <member>\n        <Arn>{}</Arn>\n        <ValidUntil>{}</ValidUntil>\n        <CreateDate>{}</CreateDate>\n      </member>",
                    p.arn,
                    p.valid_until.format("%Y-%m-%dT%H:%M:%SZ"),
                    p.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListSAMLProvidersResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListSAMLProvidersResult>
    <SAMLProviderList>
{members}
    </SAMLProviderList>
  </ListSAMLProvidersResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListSAMLProvidersResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn update_saml_provider(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "SAMLProviderArn")?;
        let saml_metadata_document = required_param(&req.query_params, "SAMLMetadataDocument")?;

        let mut state = self.state.write();

        let provider = state.saml_providers.get_mut(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("SAML provider {arn} not found"),
            )
        })?;

        provider.saml_metadata_document = saml_metadata_document;

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<UpdateSAMLProviderResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <UpdateSAMLProviderResult>
    <SAMLProviderArn>{}</SAMLProviderArn>
  </UpdateSAMLProviderResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</UpdateSAMLProviderResponse>"#,
            arn, req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= OIDC Provider operations =========

impl IamService {
    fn create_oidc_provider(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let url = required_param(&req.query_params, "Url")?;
        let tags = parse_tags(&req.query_params);

        let mut client_ids = Vec::new();
        let mut i = 1;
        while let Some(id) = req.query_params.get(&format!("ClientIDList.member.{i}")) {
            client_ids.push(id.clone());
            i += 1;
        }

        let mut thumbprints = Vec::new();
        i = 1;
        while let Some(tp) = req.query_params.get(&format!("ThumbprintList.member.{i}")) {
            thumbprints.push(tp.clone());
            i += 1;
        }

        let mut state = self.state.write();

        // Validate URL: must start with http:// or https://
        if !url.starts_with("https://") && !url.starts_with("http://") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                "Invalid Open ID Connect Provider URL".to_string(),
            ));
        }

        // Store URL without scheme for responses (AWS behavior)
        let url_without_scheme = url
            .strip_prefix("https://")
            .or_else(|| url.strip_prefix("http://"))
            .unwrap_or(&url)
            .to_string();

        // ARN uses URL path without query string
        let url_for_arn = url_without_scheme
            .split('?')
            .next()
            .unwrap_or(&url_without_scheme);
        let arn = format!(
            "arn:aws:iam::{}:oidc-provider/{}",
            state.account_id, url_for_arn
        );

        if state.oidc_providers.contains_key(&arn) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                "Unknown".to_string(),
            ));
        }

        let provider = OidcProvider {
            arn: arn.clone(),
            url: url_without_scheme,
            client_id_list: client_ids,
            thumbprint_list: thumbprints,
            created_at: Utc::now(),
            tags,
        };

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<CreateOpenIDConnectProviderResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <CreateOpenIDConnectProviderResult>
    <OpenIDConnectProviderArn>{}</OpenIDConnectProviderArn>
  </CreateOpenIDConnectProviderResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CreateOpenIDConnectProviderResponse>"#,
            arn, req.request_id
        );

        state.oidc_providers.insert(arn, provider);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_oidc_provider(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "OpenIDConnectProviderArn")?;
        let state = self.state.read();

        let provider = state.oidc_providers.get(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("OpenIDConnect provider not found for arn {arn}"),
            )
        })?;

        let client_ids: String = provider
            .client_id_list
            .iter()
            .map(|id| format!("      <member>{id}</member>"))
            .collect::<Vec<_>>()
            .join("\n");

        let thumbprints: String = provider
            .thumbprint_list
            .iter()
            .map(|tp| format!("      <member>{tp}</member>"))
            .collect::<Vec<_>>()
            .join("\n");

        let tags_members = tags_xml(&provider.tags);

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetOpenIDConnectProviderResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetOpenIDConnectProviderResult>
    <Url>{}</Url>
    <CreateDate>{}</CreateDate>
    <ClientIDList>
{client_ids}
    </ClientIDList>
    <ThumbprintList>
{thumbprints}
    </ThumbprintList>
    <Tags>
{tags_members}
    </Tags>
  </GetOpenIDConnectProviderResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetOpenIDConnectProviderResponse>"#,
            xml_escape(&provider.url),
            provider.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_oidc_provider(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "OpenIDConnectProviderArn")?;
        let mut state = self.state.write();

        if state.oidc_providers.remove(&arn).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("OpenIDConnect provider not found for arn {arn}"),
            ));
        }

        let xml = empty_response("DeleteOpenIDConnectProvider", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_oidc_providers(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        let members: String = state
            .oidc_providers
            .values()
            .map(|p| {
                format!(
                    "      <member>\n        <Arn>{}</Arn>\n      </member>",
                    p.arn
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListOpenIDConnectProvidersResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListOpenIDConnectProvidersResult>
    <OpenIDConnectProviderList>
{members}
    </OpenIDConnectProviderList>
  </ListOpenIDConnectProvidersResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListOpenIDConnectProvidersResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn update_oidc_thumbprint(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "OpenIDConnectProviderArn")?;

        let mut thumbprints = Vec::new();
        let mut i = 1;
        while let Some(tp) = req.query_params.get(&format!("ThumbprintList.member.{i}")) {
            thumbprints.push(tp.clone());
            i += 1;
        }

        let mut state = self.state.write();

        let provider = state.oidc_providers.get_mut(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("OpenIDConnect provider not found for arn {arn}"),
            )
        })?;

        provider.thumbprint_list = thumbprints;

        let xml = empty_response("UpdateOpenIDConnectProviderThumbprint", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn add_client_id_to_oidc(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "OpenIDConnectProviderArn")?;
        let client_id = required_param(&req.query_params, "ClientID")?;

        let mut state = self.state.write();

        let provider = state.oidc_providers.get_mut(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("OpenIDConnect provider not found for arn {arn}"),
            )
        })?;

        if !provider.client_id_list.contains(&client_id) {
            provider.client_id_list.push(client_id);
        }

        let xml = empty_response("AddClientIDToOpenIDConnectProvider", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn remove_client_id_from_oidc(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "OpenIDConnectProviderArn")?;
        let client_id = required_param(&req.query_params, "ClientID")?;

        let mut state = self.state.write();

        let provider = state.oidc_providers.get_mut(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("OpenIDConnect provider not found for arn {arn}"),
            )
        })?;

        provider.client_id_list.retain(|id| id != &client_id);

        let xml = empty_response("RemoveClientIDFromOpenIDConnectProvider", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn tag_oidc_provider(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "OpenIDConnectProviderArn")?;
        let new_tags = parse_tags(&req.query_params);
        let mut state = self.state.write();

        let provider = state.oidc_providers.get_mut(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("OpenIDConnect provider not found for arn {arn}"),
            )
        })?;

        for new_tag in new_tags {
            if let Some(existing) = provider.tags.iter_mut().find(|t| t.key == new_tag.key) {
                existing.value = new_tag.value;
            } else {
                provider.tags.push(new_tag);
            }
        }

        let xml = empty_response("TagOpenIDConnectProvider", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn untag_oidc_provider(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "OpenIDConnectProviderArn")?;
        let tag_keys = parse_tag_keys(&req.query_params);
        let mut state = self.state.write();

        let provider = state.oidc_providers.get_mut(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("OpenIDConnect provider not found for arn {arn}"),
            )
        })?;

        provider.tags.retain(|t| !tag_keys.contains(&t.key));

        let xml = empty_response("UntagOpenIDConnectProvider", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_oidc_provider_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let arn = required_param(&req.query_params, "OpenIDConnectProviderArn")?;
        let state = self.state.read();

        let provider = state.oidc_providers.get(&arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("OpenIDConnect provider not found for arn {arn}"),
            )
        })?;

        let members = tags_xml(&provider.tags);
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListOpenIDConnectProviderTagsResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListOpenIDConnectProviderTagsResult>
    <IsTruncated>false</IsTruncated>
    <Tags>
{members}
    </Tags>
  </ListOpenIDConnectProviderTagsResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListOpenIDConnectProviderTagsResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= Server Certificate operations =========

impl IamService {
    fn upload_server_certificate(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "ServerCertificateName")?;
        let certificate_body = required_param(&req.query_params, "CertificateBody")?;
        let _private_key = required_param(&req.query_params, "PrivateKey")?;
        let path = req
            .query_params
            .get("Path")
            .cloned()
            .unwrap_or_else(|| "/".to_string());
        let certificate_chain = req.query_params.get("CertificateChain").cloned();
        let tags = parse_tags(&req.query_params);

        let mut state = self.state.write();

        if state.server_certificates.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                format!("Server certificate {name} already exists."),
            ));
        }

        let cert = ServerCertificate {
            server_certificate_id: format!("ASCA{}", generate_id()),
            arn: format!(
                "arn:aws:iam::{}:server-certificate{}{}",
                state.account_id,
                if path == "/" { "/" } else { &path },
                name
            ),
            server_certificate_name: name.clone(),
            path,
            certificate_body,
            certificate_chain,
            upload_date: Utc::now(),
            expiration: Utc::now() + chrono::Duration::days(365),
            tags,
        };

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<UploadServerCertificateResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <UploadServerCertificateResult>
    <ServerCertificateMetadata>
      <ServerCertificateName>{}</ServerCertificateName>
      <ServerCertificateId>{}</ServerCertificateId>
      <Arn>{}</Arn>
      <Path>{}</Path>
      <UploadDate>{}</UploadDate>
      <Expiration>{}</Expiration>
    </ServerCertificateMetadata>
  </UploadServerCertificateResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</UploadServerCertificateResponse>"#,
            cert.server_certificate_name,
            cert.server_certificate_id,
            cert.arn,
            cert.path,
            cert.upload_date.format("%Y-%m-%dT%H:%M:%SZ"),
            cert.expiration.format("%Y-%m-%dT%H:%M:%SZ"),
            req.request_id
        );

        state.server_certificates.insert(name, cert);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_server_certificate(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "ServerCertificateName")?;
        let state = self.state.read();

        let cert = state.server_certificates.get(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The Server Certificate with name {name} cannot be found."),
            )
        })?;

        let chain_xml = cert
            .certificate_chain
            .as_ref()
            .map(|c| {
                format!(
                    "      <CertificateChain>{}</CertificateChain>",
                    xml_escape(c)
                )
            })
            .unwrap_or_default();

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetServerCertificateResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetServerCertificateResult>
    <ServerCertificate>
      <ServerCertificateMetadata>
        <ServerCertificateName>{}</ServerCertificateName>
        <ServerCertificateId>{}</ServerCertificateId>
        <Arn>{}</Arn>
        <Path>{}</Path>
        <UploadDate>{}</UploadDate>
        <Expiration>{}</Expiration>
      </ServerCertificateMetadata>
      <CertificateBody>{}</CertificateBody>
{chain_xml}
    </ServerCertificate>
  </GetServerCertificateResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetServerCertificateResponse>"#,
            cert.server_certificate_name,
            cert.server_certificate_id,
            cert.arn,
            cert.path,
            cert.upload_date.format("%Y-%m-%dT%H:%M:%SZ"),
            cert.expiration.format("%Y-%m-%dT%H:%M:%SZ"),
            xml_escape(&cert.certificate_body),
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_server_certificate(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "ServerCertificateName")?;
        let mut state = self.state.write();

        if state.server_certificates.remove(&name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The Server Certificate with name {name} cannot be found."),
            ));
        }

        let xml = empty_response("DeleteServerCertificate", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_server_certificates(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        let members: String = state
            .server_certificates
            .values()
            .map(|cert| {
                format!(
                    "      <member>\n        <ServerCertificateName>{}</ServerCertificateName>\n        <ServerCertificateId>{}</ServerCertificateId>\n        <Arn>{}</Arn>\n        <Path>{}</Path>\n        <UploadDate>{}</UploadDate>\n        <Expiration>{}</Expiration>\n      </member>",
                    cert.server_certificate_name,
                    cert.server_certificate_id,
                    cert.arn,
                    cert.path,
                    cert.upload_date.format("%Y-%m-%dT%H:%M:%SZ"),
                    cert.expiration.format("%Y-%m-%dT%H:%M:%SZ"),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListServerCertificatesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListServerCertificatesResult>
    <IsTruncated>false</IsTruncated>
    <ServerCertificateMetadataList>
{members}
    </ServerCertificateMetadataList>
  </ListServerCertificatesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListServerCertificatesResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= Signing Certificate operations =========

impl IamService {
    fn upload_signing_certificate(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let certificate_body = required_param(&req.query_params, "CertificateBody")?;

        let mut state = self.state.write();

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        let certs = state
            .signing_certificates
            .entry(user_name.clone())
            .or_default();
        if certs.len() >= 2 {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "LimitExceeded",
                "Cannot exceed quota for SigningCertificatesPerUser: 2".to_string(),
            ));
        }

        let cert = SigningCertificate {
            certificate_id: format!("ASC{}", generate_id()),
            user_name: user_name.clone(),
            certificate_body,
            status: "Active".to_string(),
            upload_date: Utc::now(),
        };

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<UploadSigningCertificateResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <UploadSigningCertificateResult>
    <Certificate>
      <CertificateId>{}</CertificateId>
      <UserName>{}</UserName>
      <CertificateBody>{}</CertificateBody>
      <Status>{}</Status>
      <UploadDate>{}</UploadDate>
    </Certificate>
  </UploadSigningCertificateResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</UploadSigningCertificateResponse>"#,
            cert.certificate_id,
            cert.user_name,
            xml_escape(&cert.certificate_body),
            cert.status,
            cert.upload_date.format("%Y-%m-%dT%H:%M:%SZ"),
            req.request_id
        );

        certs.push(cert);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_signing_certificates(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let state = self.state.read();

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        let certs = state
            .signing_certificates
            .get(&user_name)
            .cloned()
            .unwrap_or_default();

        let members: String = certs
            .iter()
            .map(|c| {
                format!(
                    "      <member>\n        <CertificateId>{}</CertificateId>\n        <UserName>{}</UserName>\n        <CertificateBody>{}</CertificateBody>\n        <Status>{}</Status>\n        <UploadDate>{}</UploadDate>\n      </member>",
                    c.certificate_id,
                    c.user_name,
                    xml_escape(&c.certificate_body),
                    c.status,
                    c.upload_date.format("%Y-%m-%dT%H:%M:%SZ"),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListSigningCertificatesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListSigningCertificatesResult>
    <IsTruncated>false</IsTruncated>
    <Certificates>
{members}
    </Certificates>
  </ListSigningCertificatesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListSigningCertificatesResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn update_signing_certificate(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let certificate_id = required_param(&req.query_params, "CertificateId")?;
        let status = required_param(&req.query_params, "Status")?;

        let mut state = self.state.write();

        // Check user exists first
        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        let certs = state
            .signing_certificates
            .get_mut(&user_name)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchEntity",
                    format!("The Certificate with id {certificate_id} cannot be found."),
                )
            })?;

        let cert = certs
            .iter_mut()
            .find(|c| c.certificate_id == certificate_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchEntity",
                    format!("The Certificate with id {certificate_id} cannot be found."),
                )
            })?;

        cert.status = status;

        let xml = empty_response("UpdateSigningCertificate", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_signing_certificate(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let certificate_id = required_param(&req.query_params, "CertificateId")?;

        let mut state = self.state.write();

        if let Some(certs) = state.signing_certificates.get_mut(&user_name) {
            certs.retain(|c| c.certificate_id != certificate_id);
        }

        let xml = empty_response("DeleteSigningCertificate", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= Service Linked Role operations =========

impl IamService {
    fn create_service_linked_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let aws_service_name = required_param(&req.query_params, "AWSServiceName")?;
        let description = req
            .query_params
            .get("Description")
            .cloned()
            .unwrap_or_default();
        let custom_suffix = req.query_params.get("CustomSuffix").cloned();

        let mut state = self.state.write();

        // Derive role name from service name using AWS naming conventions
        // The service name before .amazonaws.com determines the role suffix
        let service_part = aws_service_name
            .strip_suffix(".amazonaws.com")
            .unwrap_or(&aws_service_name);

        // Known service name mappings (AWS has specific casing rules)
        let role_suffix = match service_part {
            "autoscaling" => "AutoScaling".to_string(),
            "elasticbeanstalk" => "ElasticBeanstalk".to_string(),
            "elasticloadbalancing" => "ElasticLoadBalancing".to_string(),
            "elasticmapreduce" => "ElasticMapReduce".to_string(),
            s if s.contains('.') => {
                // e.g. "custom-resource.application-autoscaling"
                // -> suffix is from the part after the dot: "ApplicationAutoScaling"
                // -> role name has "_CustomResource" appended for the prefix
                let parts: Vec<&str> = s.splitn(2, '.').collect();
                let prefix = parts[0]; // "custom-resource"
                let service = parts[1]; // "application-autoscaling"

                let service_cased = service
                    .split('-')
                    .map(|w| {
                        let mut c = w.chars();
                        match c.next() {
                            None => String::new(),
                            Some(ch) => ch.to_uppercase().to_string() + c.as_str(),
                        }
                    })
                    .collect::<String>();

                let prefix_cased = prefix
                    .split('-')
                    .map(|w| {
                        let mut c = w.chars();
                        match c.next() {
                            None => String::new(),
                            Some(ch) => ch.to_uppercase().to_string() + c.as_str(),
                        }
                    })
                    .collect::<String>();

                format!("{}_{}", service_cased, prefix_cased)
            }
            other => other.to_string(), // Use as-is for unknown services
        };

        let role_name = if let Some(suffix) = &custom_suffix {
            format!("AWSServiceRoleFor{}_{}", role_suffix, suffix)
        } else {
            format!("AWSServiceRoleFor{}", role_suffix)
        };

        let path = format!("/aws-service-role/{}/", aws_service_name);

        // AWS uses arrays for Action and Service in SLR trust policies
        let assume_role_policy = format!(
            r#"{{"Version":"2012-10-17","Statement":[{{"Effect":"Allow","Principal":{{"Service":["{}"]}},"Action":["sts:AssumeRole"]}}]}}"#,
            aws_service_name
        );

        if state.roles.contains_key(&role_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "InvalidInput",
                format!(
                    "Service role name {role_name} has been taken in this account, please try a different suffix."
                ),
            ));
        }

        let role = IamRole {
            role_id: format!("AROA{}", generate_id()),
            arn: format!(
                "arn:aws:iam::{}:role{}{}",
                state.account_id, path, role_name
            ),
            role_name: role_name.clone(),
            path,
            assume_role_policy_document: assume_role_policy,
            created_at: Utc::now(),
            description,
            max_session_duration: 3600,
            tags: Vec::new(),
            permissions_boundary: None,
        };

        let xml = xml_responses::create_role_response(&role, &req.request_id)
            .replace("CreateRoleResponse", "CreateServiceLinkedRoleResponse")
            .replace("CreateRoleResult", "CreateServiceLinkedRoleResult");

        state.roles.insert(role_name, role);

        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_service_linked_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let mut state = self.state.write();

        if !state.roles.contains_key(&role_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            ));
        }

        // Don't actually delete yet -- return a deletion task ID
        let task_id = format!("task/{}", uuid::Uuid::new_v4());

        // Actually delete the role
        state.roles.remove(&role_name);
        state.role_policies.remove(&role_name);
        state.role_inline_policies.remove(&role_name);

        state.service_linked_role_deletions.insert(
            task_id.clone(),
            ServiceLinkedRoleDeletion {
                deletion_task_id: task_id.clone(),
                status: "SUCCEEDED".to_string(),
            },
        );

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<DeleteServiceLinkedRoleResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <DeleteServiceLinkedRoleResult>
    <DeletionTaskId>{task_id}</DeletionTaskId>
  </DeleteServiceLinkedRoleResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</DeleteServiceLinkedRoleResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_service_linked_role_deletion_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let task_id = required_param(&req.query_params, "DeletionTaskId")?;
        let state = self.state.read();

        let task = state
            .service_linked_role_deletions
            .get(&task_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchEntity",
                    format!("Deletion task {task_id} not found"),
                )
            })?;

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetServiceLinkedRoleDeletionStatusResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetServiceLinkedRoleDeletionStatusResult>
    <Status>{}</Status>
  </GetServiceLinkedRoleDeletionStatusResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetServiceLinkedRoleDeletionStatusResponse>"#,
            task.status, req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= Account operations =========

impl IamService {
    fn get_account_summary(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetAccountSummaryResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetAccountSummaryResult>
    <SummaryMap>
      <entry><key>Users</key><value>{}</value></entry>
      <entry><key>UsersQuota</key><value>5000</value></entry>
      <entry><key>Groups</key><value>{}</value></entry>
      <entry><key>GroupsQuota</key><value>300</value></entry>
      <entry><key>ServerCertificates</key><value>{}</value></entry>
      <entry><key>ServerCertificatesQuota</key><value>20</value></entry>
      <entry><key>UserPolicySizeQuota</key><value>2048</value></entry>
      <entry><key>GroupPolicySizeQuota</key><value>5120</value></entry>
      <entry><key>GroupsPerUserQuota</key><value>10</value></entry>
      <entry><key>SigningCertificatesPerUserQuota</key><value>2</value></entry>
      <entry><key>AccessKeysPerUserQuota</key><value>2</value></entry>
      <entry><key>MFADevices</key><value>{}</value></entry>
      <entry><key>MFADevicesInUse</key><value>0</value></entry>
      <entry><key>AccountMFAEnabled</key><value>0</value></entry>
      <entry><key>AccountAccessKeysPresent</key><value>0</value></entry>
      <entry><key>AccountSigningCertificatesPresent</key><value>0</value></entry>
      <entry><key>Policies</key><value>{}</value></entry>
      <entry><key>PoliciesQuota</key><value>1500</value></entry>
      <entry><key>PolicySizeQuota</key><value>6144</value></entry>
      <entry><key>PolicyVersionsInUse</key><value>{}</value></entry>
      <entry><key>PolicyVersionsInUseQuota</key><value>10000</value></entry>
      <entry><key>VersionsPerPolicyQuota</key><value>5</value></entry>
      <entry><key>Roles</key><value>{}</value></entry>
      <entry><key>RolesQuota</key><value>1000</value></entry>
      <entry><key>RolePolicySizeQuota</key><value>10240</value></entry>
      <entry><key>InstanceProfiles</key><value>{}</value></entry>
      <entry><key>InstanceProfilesQuota</key><value>1000</value></entry>
      <entry><key>Providers</key><value>{}</value></entry>
      <entry><key>AttachedPoliciesPerGroupQuota</key><value>10</value></entry>
      <entry><key>AttachedPoliciesPerRoleQuota</key><value>10</value></entry>
      <entry><key>AttachedPoliciesPerUserQuota</key><value>10</value></entry>
      <entry><key>GlobalEndpointTokenVersion</key><value>1</value></entry>
    </SummaryMap>
  </GetAccountSummaryResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetAccountSummaryResponse>"#,
            state.users.len(),
            state.groups.len(),
            state.server_certificates.len(),
            state.virtual_mfa_devices.len(),
            state.policies.len(),
            state
                .policies
                .values()
                .map(|p| p.versions.len())
                .sum::<usize>(),
            state.roles.len(),
            state.instance_profiles.len(),
            state.saml_providers.len() + state.oidc_providers.len(),
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_account_authorization_details(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        // Build user details
        let user_details: String = state
            .users
            .values()
            .map(|u| {
                let inline_policies: String = state
                    .user_inline_policies
                    .get(&u.user_name)
                    .map(|policies| {
                        policies
                            .iter()
                            .map(|(name, doc)| {
                                format!(
                                    "          <member>\n            <PolicyName>{}</PolicyName>\n            <PolicyDocument>{}</PolicyDocument>\n          </member>",
                                    xml_escape(name),
                                    url_encode(doc)
                                )
                            })
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default();

                let attached: String = state
                    .user_policies
                    .get(&u.user_name)
                    .map(|arns| {
                        arns.iter()
                            .filter_map(|arn| {
                                state.policies.get(arn).map(|p| {
                                    format!(
                                        "          <member>\n            <PolicyName>{}</PolicyName>\n            <PolicyArn>{}</PolicyArn>\n          </member>",
                                        p.policy_name, p.arn
                                    )
                                })
                            })
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default();

                let group_list: String = state
                    .groups
                    .values()
                    .filter(|g| g.members.contains(&u.user_name))
                    .map(|g| format!("          <member>{}</member>", g.group_name))
                    .collect::<Vec<_>>()
                    .join("\n");

                let tags_members = tags_xml(&u.tags);

                format!(
                    "      <member>\n        <Path>{}</Path>\n        <UserName>{}</UserName>\n        <UserId>{}</UserId>\n        <Arn>{}</Arn>\n        <CreateDate>{}</CreateDate>\n        <UserPolicyList>\n{inline_policies}\n        </UserPolicyList>\n        <GroupList>\n{group_list}\n        </GroupList>\n        <AttachedManagedPolicies>\n{attached}\n        </AttachedManagedPolicies>\n        <Tags>\n{tags_members}\n        </Tags>\n      </member>",
                    u.path, u.user_name, u.user_id, u.arn, u.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        // Build role details
        let role_details: String = state
            .roles
            .values()
            .map(|r| {
                let inline_policies: String = state
                    .role_inline_policies
                    .get(&r.role_name)
                    .map(|policies| {
                        policies
                            .iter()
                            .map(|(name, doc)| {
                                format!(
                                    "          <member>\n            <PolicyName>{}</PolicyName>\n            <PolicyDocument>{}</PolicyDocument>\n          </member>",
                                    xml_escape(name),
                                    url_encode(doc)
                                )
                            })
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default();

                let attached: String = state
                    .role_policies
                    .get(&r.role_name)
                    .map(|arns| {
                        arns.iter()
                            .filter_map(|arn| {
                                state.policies.get(arn).map(|p| {
                                    format!(
                                        "          <member>\n            <PolicyName>{}</PolicyName>\n            <PolicyArn>{}</PolicyArn>\n          </member>",
                                        p.policy_name, p.arn
                                    )
                                })
                            })
                            .collect::<Vec<_>>()
                            .join("\n")
                    })
                    .unwrap_or_default();

                let instance_profiles: String = state
                    .instance_profiles
                    .values()
                    .filter(|ip| ip.roles.contains(&r.role_name))
                    .map(|ip| {
                        format!(
                            "          <member>\n            <InstanceProfileName>{}</InstanceProfileName>\n            <InstanceProfileId>{}</InstanceProfileId>\n            <Arn>{}</Arn>\n            <Path>{}</Path>\n            <CreateDate>{}</CreateDate>\n          </member>",
                            ip.instance_profile_name, ip.instance_profile_id, ip.arn, ip.path, ip.created_at.format("%Y-%m-%dT%H:%M:%SZ")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                let tags_members = tags_xml(&r.tags);

                format!(
                    "      <member>\n        <Path>{}</Path>\n        <RoleName>{}</RoleName>\n        <RoleId>{}</RoleId>\n        <Arn>{}</Arn>\n        <CreateDate>{}</CreateDate>\n        <AssumeRolePolicyDocument>{}</AssumeRolePolicyDocument>\n        <RolePolicyList>\n{inline_policies}\n        </RolePolicyList>\n        <AttachedManagedPolicies>\n{attached}\n        </AttachedManagedPolicies>\n        <InstanceProfileList>\n{instance_profiles}\n        </InstanceProfileList>\n        <Tags>\n{tags_members}\n        </Tags>\n      </member>",
                    r.path, r.role_name, r.role_id, r.arn, r.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
                    url_encode(&r.assume_role_policy_document),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        // Build group details
        let group_details: String = state
            .groups
            .values()
            .map(|g| {
                let inline_policies: String = g
                    .inline_policies
                    .iter()
                    .map(|(name, doc)| {
                        format!(
                            "          <member>\n            <PolicyName>{}</PolicyName>\n            <PolicyDocument>{}</PolicyDocument>\n          </member>",
                            xml_escape(name),
                            url_encode(doc)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                let attached: String = g
                    .attached_policies
                    .iter()
                    .filter_map(|arn| {
                        state.policies.get(arn).map(|p| {
                            format!(
                                "          <member>\n            <PolicyName>{}</PolicyName>\n            <PolicyArn>{}</PolicyArn>\n          </member>",
                                p.policy_name, p.arn
                            )
                        })
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                format!(
                    "      <member>\n        <Path>{}</Path>\n        <GroupName>{}</GroupName>\n        <GroupId>{}</GroupId>\n        <Arn>{}</Arn>\n        <CreateDate>{}</CreateDate>\n        <GroupPolicyList>\n{inline_policies}\n        </GroupPolicyList>\n        <AttachedManagedPolicies>\n{attached}\n        </AttachedManagedPolicies>\n      </member>",
                    g.path, g.group_name, g.group_id, g.arn, g.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        // Build policy details
        let policy_details: String = state
            .policies
            .values()
            .map(|p| {
                let versions: String = p
                    .versions
                    .iter()
                    .map(|v| {
                        format!(
                            "            <member>\n              <VersionId>{}</VersionId>\n              <IsDefaultVersion>{}</IsDefaultVersion>\n              <Document>{}</Document>\n              <CreateDate>{}</CreateDate>\n            </member>",
                            v.version_id, v.is_default, url_encode(&v.document), v.created_at.format("%Y-%m-%dT%H:%M:%SZ")
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                format!(
                    "      <member>\n        <PolicyName>{}</PolicyName>\n        <PolicyId>{}</PolicyId>\n        <Arn>{}</Arn>\n        <Path>{}</Path>\n        <DefaultVersionId>{}</DefaultVersionId>\n        <AttachmentCount>{}</AttachmentCount>\n        <IsAttachable>true</IsAttachable>\n        <CreateDate>{}</CreateDate>\n        <PolicyVersionList>\n{versions}\n        </PolicyVersionList>\n      </member>",
                    p.policy_name, p.policy_id, p.arn, p.path, p.default_version_id,
                    p.attachment_count, p.created_at.format("%Y-%m-%dT%H:%M:%SZ"),
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetAccountAuthorizationDetailsResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetAccountAuthorizationDetailsResult>
    <IsTruncated>false</IsTruncated>
    <UserDetailList>
{user_details}
    </UserDetailList>
    <RoleDetailList>
{role_details}
    </RoleDetailList>
    <GroupDetailList>
{group_details}
    </GroupDetailList>
    <Policies>
{policy_details}
    </Policies>
  </GetAccountAuthorizationDetailsResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetAccountAuthorizationDetailsResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn create_account_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let alias = required_param(&req.query_params, "AccountAlias")?;
        let mut state = self.state.write();

        if !state.account_aliases.contains(&alias) {
            state.account_aliases.push(alias);
        }

        let xml = empty_response("CreateAccountAlias", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_account_alias(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let alias = required_param(&req.query_params, "AccountAlias")?;
        let mut state = self.state.write();
        state.account_aliases.retain(|a| a != &alias);

        let xml = empty_response("DeleteAccountAlias", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_account_aliases(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let members: String = state
            .account_aliases
            .iter()
            .map(|a| format!("      <member>{a}</member>"))
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAccountAliasesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListAccountAliasesResult>
    <IsTruncated>false</IsTruncated>
    <AccountAliases>
{members}
    </AccountAliases>
  </ListAccountAliasesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListAccountAliasesResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn update_account_password_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Validate constraints
        let min_len: Option<i64> = req
            .query_params
            .get("MinimumPasswordLength")
            .and_then(|v| v.parse().ok());
        let max_age: Option<i64> = req
            .query_params
            .get("MaxPasswordAge")
            .and_then(|v| v.parse().ok());
        let reuse_prevention: Option<i64> = req
            .query_params
            .get("PasswordReusePrevention")
            .and_then(|v| v.parse().ok());

        let mut errors = Vec::new();
        if let Some(v) = min_len {
            if v > 128 {
                errors.push(format!("Value \"{v}\" at \"minimumPasswordLength\" failed to satisfy constraint: Member must have value less than or equal to 128"));
            }
        }
        if let Some(v) = reuse_prevention {
            if v > 24 {
                errors.push(format!("Value \"{v}\" at \"passwordReusePrevention\" failed to satisfy constraint: Member must have value less than or equal to 24"));
            }
        }
        if let Some(v) = max_age {
            if v > 1095 {
                errors.push(format!("Value \"{v}\" at \"maxPasswordAge\" failed to satisfy constraint: Member must have value less than or equal to 1095"));
            }
        }
        if !errors.is_empty() {
            let n = errors.len();
            let msg = format!(
                "{n} validation error{} detected: {}",
                if n > 1 { "s" } else { "" },
                errors.join("; ")
            );
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ValidationError",
                msg,
            ));
        }

        let mut state = self.state.write();

        let policy = state
            .account_password_policy
            .get_or_insert(AccountPasswordPolicy::default());

        if let Some(v) = req
            .query_params
            .get("MinimumPasswordLength")
            .and_then(|v| v.parse().ok())
        {
            policy.minimum_password_length = v;
        }
        if let Some(v) = req.query_params.get("RequireSymbols") {
            policy.require_symbols = v == "true";
        }
        if let Some(v) = req.query_params.get("RequireNumbers") {
            policy.require_numbers = v == "true";
        }
        if let Some(v) = req.query_params.get("RequireUppercaseCharacters") {
            policy.require_uppercase_characters = v == "true";
        }
        if let Some(v) = req.query_params.get("RequireLowercaseCharacters") {
            policy.require_lowercase_characters = v == "true";
        }
        if let Some(v) = req.query_params.get("AllowUsersToChangePassword") {
            policy.allow_users_to_change_password = v == "true";
        }
        if let Some(v) = req
            .query_params
            .get("MaxPasswordAge")
            .and_then(|v| v.parse().ok())
        {
            policy.max_password_age = v;
        }
        if let Some(v) = req
            .query_params
            .get("PasswordReusePrevention")
            .and_then(|v| v.parse().ok())
        {
            policy.password_reuse_prevention = v;
        }
        if let Some(v) = req.query_params.get("HardExpiry") {
            policy.hard_expiry = v == "true";
        }

        let xml = empty_response("UpdateAccountPasswordPolicy", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_account_password_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

        let policy = state.account_password_policy.as_ref().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!(
                    "The Password Policy with domain name {} cannot be found.",
                    state.account_id
                ),
            )
        })?;

        let max_age_xml = format!(
            "\n      <MaxPasswordAge>{}</MaxPasswordAge>",
            policy.max_password_age
        );

        let reuse_prevention_xml = if policy.password_reuse_prevention > 0 {
            format!(
                "\n      <PasswordReusePrevention>{}</PasswordReusePrevention>",
                policy.password_reuse_prevention
            )
        } else {
            String::new()
        };

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetAccountPasswordPolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetAccountPasswordPolicyResult>
    <PasswordPolicy>
      <MinimumPasswordLength>{}</MinimumPasswordLength>
      <RequireSymbols>{}</RequireSymbols>
      <RequireNumbers>{}</RequireNumbers>
      <RequireUppercaseCharacters>{}</RequireUppercaseCharacters>
      <RequireLowercaseCharacters>{}</RequireLowercaseCharacters>
      <AllowUsersToChangePassword>{}</AllowUsersToChangePassword>{max_age_xml}{reuse_prevention_xml}
      <HardExpiry>{}</HardExpiry>
      <ExpirePasswords>{}</ExpirePasswords>
    </PasswordPolicy>
  </GetAccountPasswordPolicyResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetAccountPasswordPolicyResponse>"#,
            policy.minimum_password_length,
            policy.require_symbols,
            policy.require_numbers,
            policy.require_uppercase_characters,
            policy.require_lowercase_characters,
            policy.allow_users_to_change_password,
            policy.hard_expiry,
            policy.max_password_age > 0,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_account_password_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

        if state.account_password_policy.is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                "The account policy with name PasswordPolicy cannot be found.".to_string(),
            ));
        }

        state.account_password_policy = None;

        let xml = empty_response("DeleteAccountPasswordPolicy", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= Credential Report =========

impl IamService {
    fn generate_credential_report(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
        let (report_state, description) = if state.credential_report_generated {
            ("COMPLETE", "Report generated")
        } else {
            state.credential_report_generated = true;
            (
                "STARTED",
                "No report exists. Starting a new report generation.",
            )
        };
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GenerateCredentialReportResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GenerateCredentialReportResult>
    <State>{report_state}</State>
    <Description>{description}</Description>
  </GenerateCredentialReportResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GenerateCredentialReportResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn get_credential_report(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        use base64::Engine;
        let state = self.state.read();

        let mut csv = String::from(
            "user,arn,user_creation_time,password_enabled,password_last_used,password_last_changed,password_next_rotation,mfa_active,access_key_1_active,access_key_1_last_rotated,access_key_1_last_used_date,access_key_1_last_used_region,access_key_1_last_used_service,access_key_2_active,access_key_2_last_rotated,access_key_2_last_used_date,access_key_2_last_used_region,access_key_2_last_used_service,cert_1_active,cert_1_last_rotated,cert_2_active,cert_2_last_rotated\n"
        );

        // Root account
        csv.push_str(&format!(
            "<root_account>,arn:aws:iam::{}:root,{},not_supported,not_supported,not_supported,not_supported,false,false,N/A,N/A,N/A,N/A,false,N/A,N/A,N/A,N/A,false,N/A,false,N/A\n",
            state.account_id,
            Utc::now().format("%Y-%m-%dT%H:%M:%S+00:00")
        ));

        for user in state.users.values() {
            let has_password = state.login_profiles.contains_key(&user.user_name);
            let keys = state
                .access_keys
                .get(&user.user_name)
                .cloned()
                .unwrap_or_default();
            let key1_active = keys.first().map(|k| k.status == "Active").unwrap_or(false);
            let key2_active = keys.get(1).map(|k| k.status == "Active").unwrap_or(false);

            let certs = state
                .signing_certificates
                .get(&user.user_name)
                .cloned()
                .unwrap_or_default();
            let cert1_active = certs.first().map(|c| c.status == "Active").unwrap_or(false);
            let cert2_active = certs.get(1).map(|c| c.status == "Active").unwrap_or(false);

            csv.push_str(&format!(
                "{},{},{},{},not_supported,N/A,N/A,false,{},N/A,N/A,N/A,N/A,{},N/A,N/A,N/A,N/A,{},N/A,{},N/A\n",
                user.user_name,
                user.arn,
                user.created_at.format("%Y-%m-%dT%H:%M:%S+00:00"),
                has_password,
                key1_active,
                key2_active,
                cert1_active,
                cert2_active,
            ));
        }

        let encoded = base64::engine::general_purpose::STANDARD.encode(csv.as_bytes());

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetCredentialReportResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetCredentialReportResult>
    <Content>{encoded}</Content>
    <GeneratedTime>{}</GeneratedTime>
    <ReportFormat>text/csv</ReportFormat>
  </GetCredentialReportResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetCredentialReportResponse>"#,
            Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= Virtual MFA Device operations =========

impl IamService {
    fn create_virtual_mfa_device(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let virtual_mfa_device_name = required_param(&req.query_params, "VirtualMFADeviceName")?;
        let path = req
            .query_params
            .get("Path")
            .cloned()
            .unwrap_or_else(|| "/".to_string());
        let tags = parse_tags(&req.query_params);

        let mut state = self.state.write();

        let serial_number = format!(
            "arn:aws:iam::{}:mfa/{}",
            state.account_id, virtual_mfa_device_name
        );

        if state.virtual_mfa_devices.contains_key(&serial_number) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                "MFADevice entity at the same path and name already exists.".to_string(),
            ));
        }

        use base64::Engine;
        let seed = uuid::Uuid::new_v4().to_string();
        let seed_b32 = base64::engine::general_purpose::STANDARD.encode(seed.as_bytes());
        let qr_png = base64::engine::general_purpose::STANDARD
            .encode(format!("fake-qr-{}", virtual_mfa_device_name).as_bytes());

        let device = VirtualMfaDevice {
            serial_number: serial_number.clone(),
            base32_string_seed: seed_b32.clone(),
            qr_code_png: qr_png.clone(),
            enable_date: None,
            user: None,
            tags,
        };

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<CreateVirtualMFADeviceResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <CreateVirtualMFADeviceResult>
    <VirtualMFADevice>
      <SerialNumber>{serial_number}</SerialNumber>
      <Base32StringSeed>{seed_b32}</Base32StringSeed>
      <QRCodePNG>{qr_png}</QRCodePNG>
    </VirtualMFADevice>
  </CreateVirtualMFADeviceResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</CreateVirtualMFADeviceResponse>"#,
            req.request_id
        );

        state.virtual_mfa_devices.insert(serial_number, device);
        let _ = path; // path is used in serial_number for MFA devices

        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn delete_virtual_mfa_device(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let serial_number = required_param(&req.query_params, "SerialNumber")?;
        let mut state = self.state.write();

        if state.virtual_mfa_devices.remove(&serial_number).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("VirtualMFADevice with serial number {serial_number} not found"),
            ));
        }

        let xml = empty_response("DeleteVirtualMFADevice", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_virtual_mfa_devices(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let assignment_status = req.query_params.get("AssignmentStatus").cloned();

        let devices: Vec<&VirtualMfaDevice> = state
            .virtual_mfa_devices
            .values()
            .filter(|d| match assignment_status.as_deref() {
                Some("Assigned") => d.user.is_some(),
                Some("Unassigned") => d.user.is_none(),
                _ => true,
            })
            .collect();

        let members: String = devices
            .iter()
            .map(|d| {
                let user_xml = d
                    .user
                    .as_ref()
                    .and_then(|uname| {
                        state.users.get(uname).map(|u| {
                            format!(
                                "\n        <User>\n          <Path>{}</Path>\n          <UserName>{}</UserName>\n          <UserId>{}</UserId>\n          <Arn>{}</Arn>\n          <CreateDate>{}</CreateDate>\n        </User>",
                                u.path, u.user_name, u.user_id, u.arn, u.created_at.format("%Y-%m-%dT%H:%M:%SZ")
                            )
                        })
                    })
                    .unwrap_or_default();
                let enable_date = d
                    .enable_date
                    .map(|dt| {
                        format!(
                            "\n        <EnableDate>{}</EnableDate>",
                            dt.format("%Y-%m-%dT%H:%M:%SZ")
                        )
                    })
                    .unwrap_or_default();
                format!(
                    "      <member>\n        <SerialNumber>{}</SerialNumber>{user_xml}{enable_date}\n      </member>",
                    d.serial_number,
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListVirtualMFADevicesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListVirtualMFADevicesResult>
    <IsTruncated>false</IsTruncated>
    <VirtualMFADevices>
{members}
    </VirtualMFADevices>
  </ListVirtualMFADevicesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListVirtualMFADevicesResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn enable_mfa_device(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let serial_number = required_param(&req.query_params, "SerialNumber")?;
        let _code1 = required_param(&req.query_params, "AuthenticationCode1")?;
        let _code2 = required_param(&req.query_params, "AuthenticationCode2")?;

        let mut state = self.state.write();

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        let device = state
            .virtual_mfa_devices
            .get_mut(&serial_number)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchEntity",
                    format!("VirtualMFADevice with serial number {serial_number} not found"),
                )
            })?;

        device.user = Some(user_name);
        device.enable_date = Some(Utc::now());

        let xml = empty_response("EnableMFADevice", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn deactivate_mfa_device(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let _user_name = required_param(&req.query_params, "UserName")?;
        let serial_number = required_param(&req.query_params, "SerialNumber")?;

        let mut state = self.state.write();

        if let Some(device) = state.virtual_mfa_devices.get_mut(&serial_number) {
            device.user = None;
            device.enable_date = None;
        }

        let xml = empty_response("DeactivateMFADevice", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    fn list_mfa_devices(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let state = self.state.read();

        let devices: Vec<&VirtualMfaDevice> = state
            .virtual_mfa_devices
            .values()
            .filter(|d| d.user.as_deref() == Some(&user_name))
            .collect();

        let members: String = devices
            .iter()
            .map(|d| {
                let enable_date = d
                    .enable_date
                    .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                    .unwrap_or_default();
                format!(
                    "      <member>\n        <SerialNumber>{}</SerialNumber>\n        <UserName>{}</UserName>\n        <EnableDate>{}</EnableDate>\n      </member>",
                    d.serial_number, user_name, enable_date,
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListMFADevicesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListMFADevicesResult>
    <IsTruncated>false</IsTruncated>
    <MFADevices>
{members}
    </MFADevices>
  </ListMFADevicesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListMFADevicesResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= ListEntitiesForPolicy =========

impl IamService {
    fn list_entities_for_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let state = self.state.read();

        if !state.policies.contains_key(&policy_arn) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} does not exist."),
            ));
        }

        // Find roles attached to this policy
        let role_members: String = state
            .role_policies
            .iter()
            .filter(|(_, arns)| arns.contains(&policy_arn))
            .filter_map(|(role_name, _)| {
                state.roles.get(role_name).map(|r| {
                    format!(
                        "      <member>\n        <RoleName>{}</RoleName>\n        <RoleId>{}</RoleId>\n      </member>",
                        r.role_name, r.role_id
                    )
                })
            })
            .collect::<Vec<_>>()
            .join("\n");

        // Find users attached to this policy
        let user_members: String = state
            .user_policies
            .iter()
            .filter(|(_, arns)| arns.contains(&policy_arn))
            .filter_map(|(user_name, _)| {
                state.users.get(user_name).map(|u| {
                    format!(
                        "      <member>\n        <UserName>{}</UserName>\n        <UserId>{}</UserId>\n      </member>",
                        u.user_name, u.user_id
                    )
                })
            })
            .collect::<Vec<_>>()
            .join("\n");

        // Find groups attached to this policy
        let group_members: String = state
            .groups
            .values()
            .filter(|g| g.attached_policies.contains(&policy_arn))
            .map(|g| {
                format!(
                    "      <member>\n        <GroupName>{}</GroupName>\n        <GroupId>{}</GroupId>\n      </member>",
                    g.group_name, g.group_id
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListEntitiesForPolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListEntitiesForPolicyResult>
    <IsTruncated>false</IsTruncated>
    <PolicyRoles>
{role_members}
    </PolicyRoles>
    <PolicyUsers>
{user_members}
    </PolicyUsers>
    <PolicyGroups>
{group_members}
    </PolicyGroups>
  </ListEntitiesForPolicyResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListEntitiesForPolicyResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}
