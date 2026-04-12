mod account;
mod groups;
mod instance_profiles;
mod oidc;
mod policies;
mod roles;
mod users;

use async_trait::async_trait;
use chrono::Utc;
use http::StatusCode;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};
// NOTE: The shared validation helpers use ValidationException error codes, but real IAM
// typically returns InvalidInput or ValidationError for input validation failures. This is
// a known simplification — the validators are reused across services for consistency.
use fakecloud_core::validation::*;

use crate::state::{AccessKeyLastUsed, SharedIamState, Tag};

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
        // Track access key usage for GetAccessKeyLastUsed
        if let Some(ref key_id) = req.access_key_id {
            let mut state = self.state.write();
            let is_known = state
                .access_keys
                .values()
                .any(|keys| keys.iter().any(|k| k.access_key_id == *key_id));
            if is_known {
                state.access_key_last_used.insert(
                    key_id.clone(),
                    AccessKeyLastUsed {
                        last_used_date: Utc::now(),
                        service_name: "iam".to_string(),
                        region: req.region.clone(),
                    },
                );
            }
            drop(state);
        }

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
            "GetAccessKeyLastUsed" => self.get_access_key_last_used(&req),

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

            // SSH Public Keys
            "UploadSSHPublicKey" => self.upload_ssh_public_key(&req),
            "GetSSHPublicKey" => self.get_ssh_public_key(&req),
            "ListSSHPublicKeys" => self.list_ssh_public_keys(&req),
            "UpdateSSHPublicKey" => self.update_ssh_public_key(&req),
            "DeleteSSHPublicKey" => self.delete_ssh_public_key(&req),

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
            "GetAccessKeyLastUsed",
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
            "UploadSSHPublicKey",
            "GetSSHPublicKey",
            "ListSSHPublicKeys",
            "UpdateSSHPublicKey",
            "DeleteSSHPublicKey",
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

/// Convert a hyphenated service name to title case, handling known abbreviations.
fn title_case_service(s: &str) -> String {
    s.split('-')
        .map(|w| {
            // Known abbreviation mappings
            match w {
                "autoscaling" => "AutoScaling".to_string(),
                "loadbalancing" => "LoadBalancing".to_string(),
                "mapreduce" => "MapReduce".to_string(),
                "beanstalk" => "Beanstalk".to_string(),
                _ => {
                    let mut c = w.chars();
                    match c.next() {
                        None => String::new(),
                        Some(ch) => ch.to_uppercase().to_string() + c.as_str(),
                    }
                }
            }
        })
        .collect::<String>()
}

use fakecloud_aws::xml::xml_escape;

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
    // Generate 16 uppercase hex chars (used with 4-char prefixes like FKIA, AIDA = 20 chars)
    uuid::Uuid::new_v4()
        .to_string()
        .replace('-', "")
        .to_uppercase()[..16]
        .to_string()
}

fn generate_long_id() -> String {
    // Generate 21 uppercase hex chars (used with 3-char prefixes like ASC = 24 chars).
    // CertificateId requires minimum 24 characters.
    uuid::Uuid::new_v4()
        .to_string()
        .replace('-', "")
        .to_uppercase()[..21]
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

fn paginated_tags_response(
    action: &str,
    tags: &[Tag],
    req: &AwsRequest,
) -> Result<String, AwsServiceError> {
    let max_items_i64 = parse_optional_i64_param(
        "maxItems",
        req.query_params.get("MaxItems").map(|s| s.as_str()),
    )?;
    validate_optional_range_i64("maxItems", max_items_i64, 1, 1000)?;
    let max_items: usize = max_items_i64.unwrap_or(100) as usize;
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

    Ok(format!(
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
    ))
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

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn make_service() -> IamService {
        let state: SharedIamState =
            Arc::new(RwLock::new(crate::state::IamState::new("123456789012")));
        IamService::new(state)
    }

    fn make_request(action: &str, params: Vec<(&str, &str)>) -> AwsRequest {
        let mut query_params = HashMap::new();
        query_params.insert("Action".to_string(), action.to_string());
        for (k, v) in params {
            query_params.insert(k.to_string(), v.to_string());
        }
        AwsRequest {
            service: "iam".to_string(),
            action: action.to_string(),
            region: "us-east-1".to_string(),
            account_id: "123456789012".to_string(),
            request_id: "test-id".to_string(),
            headers: http::HeaderMap::new(),
            query_params,
            body: bytes::Bytes::new(),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: http::Method::POST,
            is_query_protocol: true,
            access_key_id: None,
        }
    }

    #[test]
    fn list_access_keys_max_items_zero_returns_error() {
        let svc = make_service();

        // Create a user first
        let req = make_request("CreateUser", vec![("UserName", "testuser")]);
        svc.create_user(&req).unwrap();

        // Try listing access keys with MaxItems=0
        let req = make_request(
            "ListAccessKeys",
            vec![("UserName", "testuser"), ("MaxItems", "0")],
        );
        let result = svc.list_access_keys(&req);
        assert!(result.is_err(), "MaxItems=0 should return an error");
    }

    #[test]
    fn list_users_rejects_non_numeric_max_items() {
        let svc = make_service();
        let req = make_request("ListUsers", vec![("MaxItems", "abc")]);
        let result = svc.list_users(&req);
        assert!(
            result.is_err(),
            "non-numeric MaxItems should return an error"
        );
    }

    #[test]
    fn list_roles_rejects_non_numeric_max_items() {
        let svc = make_service();
        let req = make_request("ListRoles", vec![("MaxItems", "xyz")]);
        let result = svc.list_roles(&req);
        assert!(
            result.is_err(),
            "non-numeric MaxItems should return an error"
        );
    }

    #[test]
    fn list_policies_rejects_non_numeric_max_items() {
        let svc = make_service();
        let req = make_request("ListPolicies", vec![("MaxItems", "notanumber")]);
        let result = svc.list_policies(&req);
        assert!(
            result.is_err(),
            "non-numeric MaxItems should return an error"
        );
    }

    // ---- Group inline policy tests ----

    #[test]
    fn put_and_get_group_policy() {
        let svc = make_service();
        let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;

        // Create group
        svc.handle_sync("CreateGroup", vec![("GroupName", "devs")]);

        // Put inline policy
        svc.handle_sync(
            "PutGroupPolicy",
            vec![
                ("GroupName", "devs"),
                ("PolicyName", "s3-access"),
                ("PolicyDocument", policy_doc),
            ],
        );

        // Get inline policy
        let resp = svc.handle_sync(
            "GetGroupPolicy",
            vec![("GroupName", "devs"), ("PolicyName", "s3-access")],
        );
        assert!(resp.contains("s3-access"));
        assert!(resp.contains("devs"));
    }

    #[test]
    fn list_group_policies() {
        let svc = make_service();
        let doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#;

        svc.handle_sync("CreateGroup", vec![("GroupName", "ops")]);
        svc.handle_sync(
            "PutGroupPolicy",
            vec![
                ("GroupName", "ops"),
                ("PolicyName", "pol-a"),
                ("PolicyDocument", doc),
            ],
        );
        svc.handle_sync(
            "PutGroupPolicy",
            vec![
                ("GroupName", "ops"),
                ("PolicyName", "pol-b"),
                ("PolicyDocument", doc),
            ],
        );

        let resp = svc.handle_sync("ListGroupPolicies", vec![("GroupName", "ops")]);
        assert!(resp.contains("pol-a"));
        assert!(resp.contains("pol-b"));
    }

    #[test]
    fn delete_group_policy() {
        let svc = make_service();
        let doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#;

        svc.handle_sync("CreateGroup", vec![("GroupName", "team")]);
        svc.handle_sync(
            "PutGroupPolicy",
            vec![
                ("GroupName", "team"),
                ("PolicyName", "temp"),
                ("PolicyDocument", doc),
            ],
        );

        // Delete
        svc.handle_sync(
            "DeleteGroupPolicy",
            vec![("GroupName", "team"), ("PolicyName", "temp")],
        );

        // List should be empty
        let resp = svc.handle_sync("ListGroupPolicies", vec![("GroupName", "team")]);
        assert!(!resp.contains("temp"));
    }

    #[test]
    fn get_group_policy_not_found() {
        let svc = make_service();
        svc.handle_sync("CreateGroup", vec![("GroupName", "g1")]);

        let req = make_request(
            "GetGroupPolicy",
            vec![("GroupName", "g1"), ("PolicyName", "nope")],
        );
        let result = svc.get_group_policy(&req);
        assert!(result.is_err());
    }

    // ---- Group managed policy attachment tests ----

    #[test]
    fn attach_and_list_group_policies_managed() {
        let svc = make_service();
        let doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#;

        svc.handle_sync("CreateGroup", vec![("GroupName", "eng")]);
        svc.handle_sync(
            "CreatePolicy",
            vec![("PolicyName", "read-policy"), ("PolicyDocument", doc)],
        );

        let create_resp = svc.handle_sync(
            "CreatePolicy",
            vec![("PolicyName", "write-policy"), ("PolicyDocument", doc)],
        );
        // Extract the ARN from the second policy
        let arn_start = create_resp.find("<Arn>").unwrap() + 5;
        let arn_end = create_resp.find("</Arn>").unwrap();
        let write_arn = &create_resp[arn_start..arn_end];

        // Attach both policies - for the first one, extract its ARN too
        // Just use the write_arn which we already have
        svc.handle_sync(
            "AttachGroupPolicy",
            vec![("GroupName", "eng"), ("PolicyArn", write_arn)],
        );

        let list = svc.handle_sync("ListAttachedGroupPolicies", vec![("GroupName", "eng")]);
        assert!(list.contains("write-policy"));
    }

    #[test]
    fn detach_group_policy() {
        let svc = make_service();
        let doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#;

        svc.handle_sync("CreateGroup", vec![("GroupName", "detach-grp")]);
        let resp = svc.handle_sync(
            "CreatePolicy",
            vec![("PolicyName", "detach-pol"), ("PolicyDocument", doc)],
        );
        let arn = extract_xml_value(&resp, "Arn");

        svc.handle_sync(
            "AttachGroupPolicy",
            vec![("GroupName", "detach-grp"), ("PolicyArn", &arn)],
        );

        // Detach
        svc.handle_sync(
            "DetachGroupPolicy",
            vec![("GroupName", "detach-grp"), ("PolicyArn", &arn)],
        );

        let list = svc.handle_sync(
            "ListAttachedGroupPolicies",
            vec![("GroupName", "detach-grp")],
        );
        assert!(!list.contains("detach-pol"));
    }

    #[test]
    fn detach_group_policy_not_attached_fails() {
        let svc = make_service();
        svc.handle_sync("CreateGroup", vec![("GroupName", "grp-err")]);

        let req = make_request(
            "DetachGroupPolicy",
            vec![
                ("GroupName", "grp-err"),
                ("PolicyArn", "arn:aws:iam::123456789012:policy/nope"),
            ],
        );
        let result = svc.detach_group_policy(&req);
        assert!(result.is_err());
    }

    // ---- User inline policy tests ----

    #[test]
    fn put_get_delete_user_inline_policy() {
        let svc = make_service();
        let doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"sqs:*","Resource":"*"}]}"#;

        svc.handle_sync("CreateUser", vec![("UserName", "alice")]);

        // Put
        svc.handle_sync(
            "PutUserPolicy",
            vec![
                ("UserName", "alice"),
                ("PolicyName", "sqs-access"),
                ("PolicyDocument", doc),
            ],
        );

        // Get
        let resp = svc.handle_sync(
            "GetUserPolicy",
            vec![("UserName", "alice"), ("PolicyName", "sqs-access")],
        );
        assert!(resp.contains("sqs-access"));
        assert!(resp.contains("alice"));

        // List
        let list = svc.handle_sync("ListUserPolicies", vec![("UserName", "alice")]);
        assert!(list.contains("sqs-access"));

        // Delete
        svc.handle_sync(
            "DeleteUserPolicy",
            vec![("UserName", "alice"), ("PolicyName", "sqs-access")],
        );

        let list = svc.handle_sync("ListUserPolicies", vec![("UserName", "alice")]);
        assert!(!list.contains("sqs-access"));
    }

    #[test]
    fn get_user_policy_not_found() {
        let svc = make_service();
        svc.handle_sync("CreateUser", vec![("UserName", "bob")]);

        let req = make_request(
            "GetUserPolicy",
            vec![("UserName", "bob"), ("PolicyName", "ghost")],
        );
        let result = svc.get_user_policy(&req);
        assert!(result.is_err());
    }

    // ---- User managed policy attachment tests ----

    #[test]
    fn attach_detach_list_user_policies_managed() {
        let svc = make_service();
        let doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}"#;

        svc.handle_sync("CreateUser", vec![("UserName", "carol")]);
        let resp = svc.handle_sync(
            "CreatePolicy",
            vec![("PolicyName", "user-pol"), ("PolicyDocument", doc)],
        );
        let arn = extract_xml_value(&resp, "Arn");

        // Attach
        svc.handle_sync(
            "AttachUserPolicy",
            vec![("UserName", "carol"), ("PolicyArn", &arn)],
        );

        // List attached
        let list = svc.handle_sync("ListAttachedUserPolicies", vec![("UserName", "carol")]);
        assert!(list.contains("user-pol"));

        // Detach
        svc.handle_sync(
            "DetachUserPolicy",
            vec![("UserName", "carol"), ("PolicyArn", &arn)],
        );

        let list = svc.handle_sync("ListAttachedUserPolicies", vec![("UserName", "carol")]);
        assert!(!list.contains("user-pol"));
    }

    #[test]
    fn attach_user_policy_nonexistent_user_fails() {
        let svc = make_service();
        let req = make_request(
            "AttachUserPolicy",
            vec![
                ("UserName", "nobody"),
                ("PolicyArn", "arn:aws:iam::123456789012:policy/x"),
            ],
        );
        let result = svc.attach_user_policy(&req);
        assert!(result.is_err());
    }

    #[test]
    fn detach_user_policy_not_attached_fails() {
        let svc = make_service();
        svc.handle_sync("CreateUser", vec![("UserName", "dave")]);

        let req = make_request(
            "DetachUserPolicy",
            vec![
                ("UserName", "dave"),
                ("PolicyArn", "arn:aws:iam::123456789012:policy/nope"),
            ],
        );
        let result = svc.detach_user_policy(&req);
        assert!(result.is_err());
    }

    // ---- Login profile tests ----

    #[test]
    fn login_profile_lifecycle() {
        let svc = make_service();
        svc.handle_sync("CreateUser", vec![("UserName", "loginuser")]);

        // Create login profile
        let resp = svc.handle_sync(
            "CreateLoginProfile",
            vec![
                ("UserName", "loginuser"),
                ("Password", "S3cureP@ss!"),
                ("PasswordResetRequired", "true"),
            ],
        );
        assert!(resp.contains("loginuser"));
        assert!(resp.contains("<PasswordResetRequired>true</PasswordResetRequired>"));

        // Get login profile
        let resp = svc.handle_sync("GetLoginProfile", vec![("UserName", "loginuser")]);
        assert!(resp.contains("loginuser"));
        assert!(resp.contains("<PasswordResetRequired>true</PasswordResetRequired>"));

        // Update login profile
        svc.handle_sync(
            "UpdateLoginProfile",
            vec![
                ("UserName", "loginuser"),
                ("PasswordResetRequired", "false"),
            ],
        );

        let resp = svc.handle_sync("GetLoginProfile", vec![("UserName", "loginuser")]);
        assert!(resp.contains("<PasswordResetRequired>false</PasswordResetRequired>"));

        // Delete login profile
        svc.handle_sync("DeleteLoginProfile", vec![("UserName", "loginuser")]);

        // Should fail now
        let req = make_request("GetLoginProfile", vec![("UserName", "loginuser")]);
        assert!(svc.get_login_profile(&req).is_err());
    }

    #[test]
    fn create_login_profile_duplicate_fails() {
        let svc = make_service();
        svc.handle_sync("CreateUser", vec![("UserName", "dupuser")]);
        svc.handle_sync(
            "CreateLoginProfile",
            vec![("UserName", "dupuser"), ("Password", "pass1")],
        );

        let req = make_request(
            "CreateLoginProfile",
            vec![("UserName", "dupuser"), ("Password", "pass2")],
        );
        assert!(svc.create_login_profile(&req).is_err());
    }

    #[test]
    fn delete_login_profile_nonexistent_fails() {
        let svc = make_service();
        svc.handle_sync("CreateUser", vec![("UserName", "nologin")]);

        let req = make_request("DeleteLoginProfile", vec![("UserName", "nologin")]);
        assert!(svc.delete_login_profile(&req).is_err());
    }

    // ---- MFA tests ----

    #[test]
    fn virtual_mfa_device_lifecycle() {
        let svc = make_service();

        // Create virtual MFA device
        let resp = svc.handle_sync(
            "CreateVirtualMFADevice",
            vec![("VirtualMFADeviceName", "my-mfa")],
        );
        assert!(resp.contains("my-mfa"));
        assert!(resp.contains("<Base32StringSeed>"));
        assert!(resp.contains("<QRCodePNG>"));
        let serial = extract_xml_value(&resp, "SerialNumber");

        // List should include it
        let list = svc.handle_sync("ListVirtualMFADevices", vec![]);
        assert!(list.contains("my-mfa"));

        // Delete
        svc.handle_sync("DeleteVirtualMFADevice", vec![("SerialNumber", &serial)]);

        // List should be empty
        let list = svc.handle_sync("ListVirtualMFADevices", vec![]);
        assert!(!list.contains("my-mfa"));
    }

    #[test]
    fn delete_virtual_mfa_device_not_found() {
        let svc = make_service();
        let req = make_request(
            "DeleteVirtualMFADevice",
            vec![("SerialNumber", "arn:aws:iam::123456789012:mfa/ghost")],
        );
        assert!(svc.delete_virtual_mfa_device(&req).is_err());
    }

    #[test]
    fn enable_and_list_mfa_devices() {
        let svc = make_service();
        svc.handle_sync("CreateUser", vec![("UserName", "mfauser")]);

        // Create virtual MFA
        let resp = svc.handle_sync(
            "CreateVirtualMFADevice",
            vec![("VirtualMFADeviceName", "dev-mfa")],
        );
        let serial = extract_xml_value(&resp, "SerialNumber");

        // Enable MFA device for user
        svc.handle_sync(
            "EnableMFADevice",
            vec![
                ("UserName", "mfauser"),
                ("SerialNumber", &serial),
                ("AuthenticationCode1", "123456"),
                ("AuthenticationCode2", "654321"),
            ],
        );

        // List MFA devices for user
        let list = svc.handle_sync("ListMFADevices", vec![("UserName", "mfauser")]);
        assert!(list.contains(&serial));
        assert!(list.contains("mfauser"));
    }

    #[test]
    fn deactivate_mfa_device() {
        let svc = make_service();
        svc.handle_sync("CreateUser", vec![("UserName", "deactuser")]);

        let resp = svc.handle_sync(
            "CreateVirtualMFADevice",
            vec![("VirtualMFADeviceName", "deact-mfa")],
        );
        let serial = extract_xml_value(&resp, "SerialNumber");

        svc.handle_sync(
            "EnableMFADevice",
            vec![
                ("UserName", "deactuser"),
                ("SerialNumber", &serial),
                ("AuthenticationCode1", "111111"),
                ("AuthenticationCode2", "222222"),
            ],
        );

        // Deactivate
        svc.handle_sync(
            "DeactivateMFADevice",
            vec![("UserName", "deactuser"), ("SerialNumber", &serial)],
        );

        // Should no longer appear in user's MFA device list
        let list = svc.handle_sync("ListMFADevices", vec![("UserName", "deactuser")]);
        assert!(!list.contains(&serial));
    }

    #[test]
    fn list_virtual_mfa_devices_assignment_filter() {
        let svc = make_service();
        svc.handle_sync("CreateUser", vec![("UserName", "filteruser")]);

        // Create two MFA devices with distinct names
        let resp1 = svc.handle_sync(
            "CreateVirtualMFADevice",
            vec![("VirtualMFADeviceName", "enabled-device")],
        );
        let serial1 = extract_xml_value(&resp1, "SerialNumber");
        svc.handle_sync(
            "CreateVirtualMFADevice",
            vec![("VirtualMFADeviceName", "spare-device")],
        );

        // Enable only the first
        svc.handle_sync(
            "EnableMFADevice",
            vec![
                ("UserName", "filteruser"),
                ("SerialNumber", &serial1),
                ("AuthenticationCode1", "123456"),
                ("AuthenticationCode2", "654321"),
            ],
        );

        // Filter by Assigned
        let assigned = svc.handle_sync(
            "ListVirtualMFADevices",
            vec![("AssignmentStatus", "Assigned")],
        );
        assert!(assigned.contains("enabled-device"));
        assert!(!assigned.contains("spare-device"));

        // Filter by Unassigned
        let unassigned = svc.handle_sync(
            "ListVirtualMFADevices",
            vec![("AssignmentStatus", "Unassigned")],
        );
        assert!(!unassigned.contains("enabled-device"));
        assert!(unassigned.contains("spare-device"));
    }

    // ---- Account tests ----

    #[test]
    fn get_account_summary() {
        let svc = make_service();

        // Create some resources to verify counts
        svc.handle_sync("CreateUser", vec![("UserName", "u1")]);
        svc.handle_sync("CreateUser", vec![("UserName", "u2")]);
        svc.handle_sync("CreateGroup", vec![("GroupName", "g1")]);

        let resp = svc.handle_sync("GetAccountSummary", vec![]);
        assert!(resp.contains("<key>Users</key><value>2</value>"));
        assert!(resp.contains("<key>Groups</key><value>1</value>"));
        assert!(resp.contains("<key>UsersQuota</key><value>5000</value>"));
    }

    #[test]
    fn account_alias_lifecycle() {
        let svc = make_service();

        // Create alias
        svc.handle_sync("CreateAccountAlias", vec![("AccountAlias", "my-org")]);

        // List aliases
        let list = svc.handle_sync("ListAccountAliases", vec![]);
        assert!(list.contains("my-org"));

        // Delete alias
        svc.handle_sync("DeleteAccountAlias", vec![("AccountAlias", "my-org")]);

        let list = svc.handle_sync("ListAccountAliases", vec![]);
        assert!(!list.contains("my-org"));
    }

    #[test]
    fn create_account_alias_idempotent() {
        let svc = make_service();
        svc.handle_sync("CreateAccountAlias", vec![("AccountAlias", "test-alias")]);
        svc.handle_sync("CreateAccountAlias", vec![("AccountAlias", "test-alias")]);

        let list = svc.handle_sync("ListAccountAliases", vec![]);
        // Should only appear once
        let count = list.matches("test-alias").count();
        assert_eq!(count, 1, "alias should appear exactly once");
    }

    // ---- Helper methods for tests ----

    fn extract_xml_value(xml: &str, tag: &str) -> String {
        let open = format!("<{tag}>");
        let close = format!("</{tag}>");
        let start = xml.find(&open).unwrap() + open.len();
        let end = xml.find(&close).unwrap();
        xml[start..end].to_string()
    }

    impl IamService {
        /// Synchronous helper for unit tests: dispatches to the correct method
        fn handle_sync(&self, action: &str, params: Vec<(&str, &str)>) -> String {
            let req = make_request(action, params);
            let resp = match action {
                "CreateUser" => self.create_user(&req),
                "CreateGroup" => self.create_group(&req),
                "CreatePolicy" => self.create_policy(&req),
                "ListPolicies" => self.list_policies(&req),
                "PutGroupPolicy" => self.put_group_policy(&req),
                "GetGroupPolicy" => self.get_group_policy(&req),
                "DeleteGroupPolicy" => self.delete_group_policy(&req),
                "ListGroupPolicies" => self.list_group_policies(&req),
                "AttachGroupPolicy" => self.attach_group_policy(&req),
                "DetachGroupPolicy" => self.detach_group_policy(&req),
                "ListAttachedGroupPolicies" => self.list_attached_group_policies(&req),
                "PutUserPolicy" => self.put_user_policy(&req),
                "GetUserPolicy" => self.get_user_policy(&req),
                "DeleteUserPolicy" => self.delete_user_policy(&req),
                "ListUserPolicies" => self.list_user_policies(&req),
                "AttachUserPolicy" => self.attach_user_policy(&req),
                "DetachUserPolicy" => self.detach_user_policy(&req),
                "ListAttachedUserPolicies" => self.list_attached_user_policies(&req),
                "CreateLoginProfile" => self.create_login_profile(&req),
                "GetLoginProfile" => self.get_login_profile(&req),
                "UpdateLoginProfile" => self.update_login_profile(&req),
                "DeleteLoginProfile" => self.delete_login_profile(&req),
                "CreateVirtualMFADevice" => self.create_virtual_mfa_device(&req),
                "DeleteVirtualMFADevice" => self.delete_virtual_mfa_device(&req),
                "ListVirtualMFADevices" => self.list_virtual_mfa_devices(&req),
                "EnableMFADevice" => self.enable_mfa_device(&req),
                "DeactivateMFADevice" => self.deactivate_mfa_device(&req),
                "ListMFADevices" => self.list_mfa_devices(&req),
                "GetAccountSummary" => self.get_account_summary(&req),
                "CreateAccountAlias" => self.create_account_alias(&req),
                "DeleteAccountAlias" => self.delete_account_alias(&req),
                "ListAccountAliases" => self.list_account_aliases(&req),
                other => panic!("handle_sync: unhandled action {other}"),
            }
            .unwrap();
            String::from_utf8(resp.body.to_vec()).unwrap()
        }
    }

    // ---- Policy Version Tests ----

    #[test]
    fn create_and_get_policy_version() {
        let svc = make_service();
        let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}"#;
        let req = make_request(
            "CreatePolicy",
            vec![("PolicyName", "test-pol"), ("PolicyDocument", policy_doc)],
        );
        let resp = svc.create_policy(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        // Extract policy ARN
        let arn_start = body.find("<Arn>").unwrap() + 5;
        let arn_end = body.find("</Arn>").unwrap();
        let policy_arn = &body[arn_start..arn_end];

        // Create v2
        let new_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetObject","s3:PutObject"],"Resource":"*"}]}"#;
        let req = make_request(
            "CreatePolicyVersion",
            vec![
                ("PolicyArn", policy_arn),
                ("PolicyDocument", new_doc),
                ("SetAsDefault", "true"),
            ],
        );
        let resp = svc.create_policy_version(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<VersionId>v2</VersionId>"));
        assert!(body.contains("<IsDefaultVersion>true</IsDefaultVersion>"));

        // Get v2
        let req = make_request(
            "GetPolicyVersion",
            vec![("PolicyArn", policy_arn), ("VersionId", "v2")],
        );
        let resp = svc.get_policy_version(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<VersionId>v2</VersionId>"));
        assert!(body.contains("<IsDefaultVersion>true</IsDefaultVersion>"));
    }

    #[test]
    fn list_policy_versions() {
        let svc = make_service();
        let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;
        let req = make_request(
            "CreatePolicy",
            vec![("PolicyName", "ver-pol"), ("PolicyDocument", policy_doc)],
        );
        let resp = svc.create_policy(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        let arn_start = body.find("<Arn>").unwrap() + 5;
        let arn_end = body.find("</Arn>").unwrap();
        let policy_arn = &body[arn_start..arn_end];

        // Create v2
        let req = make_request(
            "CreatePolicyVersion",
            vec![("PolicyArn", policy_arn), ("PolicyDocument", policy_doc)],
        );
        svc.create_policy_version(&req).unwrap();

        let req = make_request("ListPolicyVersions", vec![("PolicyArn", policy_arn)]);
        let resp = svc.list_policy_versions(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        // Should have v1 and v2
        assert!(body.contains("<VersionId>v1</VersionId>"));
        assert!(body.contains("<VersionId>v2</VersionId>"));
    }

    #[test]
    fn delete_default_policy_version_fails() {
        let svc = make_service();
        let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;
        let req = make_request(
            "CreatePolicy",
            vec![("PolicyName", "def-pol"), ("PolicyDocument", policy_doc)],
        );
        let resp = svc.create_policy(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        let arn_start = body.find("<Arn>").unwrap() + 5;
        let arn_end = body.find("</Arn>").unwrap();
        let policy_arn = &body[arn_start..arn_end];

        // v1 is the default; deleting it should fail
        let req = make_request(
            "DeletePolicyVersion",
            vec![("PolicyArn", policy_arn), ("VersionId", "v1")],
        );
        let result = svc.delete_policy_version(&req);
        assert!(result.is_err(), "deleting default version should fail");
    }

    #[test]
    fn set_default_policy_version() {
        let svc = make_service();
        let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;
        let req = make_request(
            "CreatePolicy",
            vec![("PolicyName", "sd-pol"), ("PolicyDocument", policy_doc)],
        );
        let resp = svc.create_policy(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        let arn_start = body.find("<Arn>").unwrap() + 5;
        let arn_end = body.find("</Arn>").unwrap();
        let policy_arn = &body[arn_start..arn_end];

        // Create v2
        let req = make_request(
            "CreatePolicyVersion",
            vec![("PolicyArn", policy_arn), ("PolicyDocument", policy_doc)],
        );
        svc.create_policy_version(&req).unwrap();

        // Set v2 as default
        let req = make_request(
            "SetDefaultPolicyVersion",
            vec![("PolicyArn", policy_arn), ("VersionId", "v2")],
        );
        svc.set_default_policy_version(&req).unwrap();

        // Verify v2 is now default
        let req = make_request(
            "GetPolicyVersion",
            vec![("PolicyArn", policy_arn), ("VersionId", "v2")],
        );
        let resp = svc.get_policy_version(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<IsDefaultVersion>true</IsDefaultVersion>"));

        // v1 should no longer be default
        let req = make_request(
            "GetPolicyVersion",
            vec![("PolicyArn", policy_arn), ("VersionId", "v1")],
        );
        let resp = svc.get_policy_version(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<IsDefaultVersion>false</IsDefaultVersion>"));
    }

    // ---- Server Certificate Tests ----

    #[test]
    fn server_certificate_lifecycle() {
        let svc = make_service();

        // Upload
        let req = make_request(
            "UploadServerCertificate",
            vec![
                ("ServerCertificateName", "my-cert"),
                (
                    "CertificateBody",
                    "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----",
                ),
                (
                    "PrivateKey",
                    "-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----",
                ),
            ],
        );
        let resp = svc.upload_server_certificate(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<ServerCertificateName>my-cert</ServerCertificateName>"));
        assert!(body.contains("ASCA"));

        // Get
        let req = make_request(
            "GetServerCertificate",
            vec![("ServerCertificateName", "my-cert")],
        );
        let resp = svc.get_server_certificate(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<ServerCertificateName>my-cert</ServerCertificateName>"));

        // List
        let req = make_request("ListServerCertificates", vec![]);
        let resp = svc.list_server_certificates(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("my-cert"));

        // Delete
        let req = make_request(
            "DeleteServerCertificate",
            vec![("ServerCertificateName", "my-cert")],
        );
        svc.delete_server_certificate(&req).unwrap();

        // Should be gone
        let req = make_request(
            "GetServerCertificate",
            vec![("ServerCertificateName", "my-cert")],
        );
        assert!(svc.get_server_certificate(&req).is_err());
    }

    #[test]
    fn server_certificate_duplicate_fails() {
        let svc = make_service();
        let req = make_request(
            "UploadServerCertificate",
            vec![
                ("ServerCertificateName", "dup-cert"),
                ("CertificateBody", "cert-body"),
                ("PrivateKey", "key-body"),
            ],
        );
        svc.upload_server_certificate(&req).unwrap();

        let req = make_request(
            "UploadServerCertificate",
            vec![
                ("ServerCertificateName", "dup-cert"),
                ("CertificateBody", "cert-body"),
                ("PrivateKey", "key-body"),
            ],
        );
        assert!(svc.upload_server_certificate(&req).is_err());
    }

    // ---- SSH Public Key Tests ----

    #[test]
    fn ssh_public_key_lifecycle() {
        let svc = make_service();
        svc.create_user(&make_request("CreateUser", vec![("UserName", "sshuser")]))
            .unwrap();

        // Upload
        let req = make_request(
            "UploadSSHPublicKey",
            vec![
                ("UserName", "sshuser"),
                (
                    "SSHPublicKeyBody",
                    "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQ test@example",
                ),
            ],
        );
        let resp = svc.upload_ssh_public_key(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Status>Active</Status>"));
        assert!(body.contains("APKA"));
        // Extract key ID
        let kid_start = body.find("<SSHPublicKeyId>").unwrap() + 16;
        let kid_end = body.find("</SSHPublicKeyId>").unwrap();
        let key_id = &body[kid_start..kid_end];

        // Get
        let req = make_request(
            "GetSSHPublicKey",
            vec![("UserName", "sshuser"), ("SSHPublicKeyId", key_id)],
        );
        let resp = svc.get_ssh_public_key(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains(key_id));
        assert!(body.contains("<Status>Active</Status>"));

        // List
        let req = make_request("ListSSHPublicKeys", vec![("UserName", "sshuser")]);
        let resp = svc.list_ssh_public_keys(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains(key_id));

        // Update status to Inactive
        let req = make_request(
            "UpdateSSHPublicKey",
            vec![
                ("UserName", "sshuser"),
                ("SSHPublicKeyId", key_id),
                ("Status", "Inactive"),
            ],
        );
        svc.update_ssh_public_key(&req).unwrap();

        // Verify status changed
        let req = make_request(
            "GetSSHPublicKey",
            vec![("UserName", "sshuser"), ("SSHPublicKeyId", key_id)],
        );
        let resp = svc.get_ssh_public_key(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Status>Inactive</Status>"));

        // Delete
        let req = make_request(
            "DeleteSSHPublicKey",
            vec![("UserName", "sshuser"), ("SSHPublicKeyId", key_id)],
        );
        svc.delete_ssh_public_key(&req).unwrap();

        // Should be empty now
        let req = make_request("ListSSHPublicKeys", vec![("UserName", "sshuser")]);
        let resp = svc.list_ssh_public_keys(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(!body.contains(key_id));
    }

    // ---- Signing Certificate Tests ----

    #[test]
    fn signing_certificate_lifecycle() {
        let svc = make_service();
        svc.create_user(&make_request("CreateUser", vec![("UserName", "certuser")]))
            .unwrap();

        let pem = "-----BEGIN CERTIFICATE-----\nMIIBxTCCAW4=\n-----END CERTIFICATE-----";

        // Upload
        let req = make_request(
            "UploadSigningCertificate",
            vec![("UserName", "certuser"), ("CertificateBody", pem)],
        );
        let resp = svc.upload_signing_certificate(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Status>Active</Status>"));
        assert!(body.contains("<UserName>certuser</UserName>"));
        let cid_start = body.find("<CertificateId>").unwrap() + 15;
        let cid_end = body.find("</CertificateId>").unwrap();
        let cert_id = &body[cid_start..cid_end];

        // List
        let req = make_request("ListSigningCertificates", vec![("UserName", "certuser")]);
        let resp = svc.list_signing_certificates(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains(cert_id));

        // Update to Inactive
        let req = make_request(
            "UpdateSigningCertificate",
            vec![
                ("UserName", "certuser"),
                ("CertificateId", cert_id),
                ("Status", "Inactive"),
            ],
        );
        svc.update_signing_certificate(&req).unwrap();

        // Delete
        let req = make_request(
            "DeleteSigningCertificate",
            vec![("UserName", "certuser"), ("CertificateId", cert_id)],
        );
        svc.delete_signing_certificate(&req).unwrap();

        // Should be empty
        let req = make_request("ListSigningCertificates", vec![("UserName", "certuser")]);
        let resp = svc.list_signing_certificates(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(!body.contains(cert_id));
    }

    #[test]
    fn signing_certificate_malformed_pem_fails() {
        let svc = make_service();
        svc.create_user(&make_request("CreateUser", vec![("UserName", "badcert")]))
            .unwrap();

        let req = make_request(
            "UploadSigningCertificate",
            vec![
                ("UserName", "badcert"),
                ("CertificateBody", "not-a-pem-cert"),
            ],
        );
        assert!(svc.upload_signing_certificate(&req).is_err());
    }

    // ---- Credential Report Tests ----

    #[test]
    fn credential_report_lifecycle() {
        let svc = make_service();

        // GetCredentialReport without generating first should fail
        let req = make_request("GetCredentialReport", vec![]);
        assert!(svc.get_credential_report(&req).is_err());

        // Generate
        let req = make_request("GenerateCredentialReport", vec![]);
        let resp = svc.generate_credential_report(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<State>STARTED</State>"));

        // Generate again returns COMPLETE
        let req = make_request("GenerateCredentialReport", vec![]);
        let resp = svc.generate_credential_report(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<State>COMPLETE</State>"));

        // Get credential report
        let req = make_request("GetCredentialReport", vec![]);
        let resp = svc.get_credential_report(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<ReportFormat>text/csv</ReportFormat>"));
        assert!(body.contains("<Content>"));
    }

    // ---- Service Linked Role Tests ----

    #[test]
    fn service_linked_role_lifecycle() {
        let svc = make_service();

        // Create
        let req = make_request(
            "CreateServiceLinkedRole",
            vec![("AWSServiceName", "elasticloadbalancing.amazonaws.com")],
        );
        let resp = svc.create_service_linked_role(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("AWSServiceRoleForElasticLoadBalancing"));
        assert!(body.contains("/aws-service-role/elasticloadbalancing.amazonaws.com/"));

        // Delete
        let req = make_request(
            "DeleteServiceLinkedRole",
            vec![("RoleName", "AWSServiceRoleForElasticLoadBalancing")],
        );
        let resp = svc.delete_service_linked_role(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<DeletionTaskId>"));
        let tid_start = body.find("<DeletionTaskId>").unwrap() + 16;
        let tid_end = body.find("</DeletionTaskId>").unwrap();
        let task_id = &body[tid_start..tid_end];

        // Check deletion status
        let req = make_request(
            "GetServiceLinkedRoleDeletionStatus",
            vec![("DeletionTaskId", task_id)],
        );
        let resp = svc.get_service_linked_role_deletion_status(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Status>SUCCEEDED</Status>"));
    }

    // ---- Permission Boundary Tests ----

    #[test]
    fn role_permissions_boundary() {
        let svc = make_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
        svc.create_role(&make_request(
            "CreateRole",
            vec![
                ("RoleName", "bound-role"),
                ("AssumeRolePolicyDocument", trust),
            ],
        ))
        .unwrap();

        // Put boundary
        let boundary_arn = "arn:aws:iam::123456789012:policy/boundary-policy";
        let req = make_request(
            "PutRolePermissionsBoundary",
            vec![
                ("RoleName", "bound-role"),
                ("PermissionsBoundary", boundary_arn),
            ],
        );
        svc.put_role_permissions_boundary(&req).unwrap();

        // Verify via GetRole
        let req = make_request("GetRole", vec![("RoleName", "bound-role")]);
        let resp = svc.get_role(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains(boundary_arn));

        // Delete boundary
        let req = make_request(
            "DeleteRolePermissionsBoundary",
            vec![("RoleName", "bound-role")],
        );
        svc.delete_role_permissions_boundary(&req).unwrap();

        // Verify removed
        let req = make_request("GetRole", vec![("RoleName", "bound-role")]);
        let resp = svc.get_role(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(!body.contains(boundary_arn));
    }

    // ---- Tag Role Tests ----

    #[test]
    fn tag_untag_role() {
        let svc = make_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
        svc.create_role(&make_request(
            "CreateRole",
            vec![
                ("RoleName", "tag-role"),
                ("AssumeRolePolicyDocument", trust),
            ],
        ))
        .unwrap();

        // Tag
        let req = make_request(
            "TagRole",
            vec![
                ("RoleName", "tag-role"),
                ("Tags.member.1.Key", "env"),
                ("Tags.member.1.Value", "prod"),
            ],
        );
        svc.tag_role(&req).unwrap();

        // List tags
        let req = make_request("ListRoleTags", vec![("RoleName", "tag-role")]);
        let resp = svc.list_role_tags(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Key>env</Key>"));
        assert!(body.contains("<Value>prod</Value>"));

        // Untag
        let req = make_request(
            "UntagRole",
            vec![("RoleName", "tag-role"), ("TagKeys.member.1", "env")],
        );
        svc.untag_role(&req).unwrap();

        let req = make_request("ListRoleTags", vec![("RoleName", "tag-role")]);
        let resp = svc.list_role_tags(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(!body.contains("<Key>env</Key>"));
    }

    // ---- Tag Policy Tests ----

    #[test]
    fn tag_untag_policy() {
        let svc = make_service();
        let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;
        let req = make_request(
            "CreatePolicy",
            vec![("PolicyName", "tag-pol"), ("PolicyDocument", policy_doc)],
        );
        let resp = svc.create_policy(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        let arn_start = body.find("<Arn>").unwrap() + 5;
        let arn_end = body.find("</Arn>").unwrap();
        let policy_arn = body[arn_start..arn_end].to_string();

        let req = make_request(
            "TagPolicy",
            vec![
                ("PolicyArn", &policy_arn),
                ("Tags.member.1.Key", "team"),
                ("Tags.member.1.Value", "platform"),
            ],
        );
        svc.tag_policy(&req).unwrap();

        let req = make_request("ListPolicyTags", vec![("PolicyArn", &policy_arn)]);
        let resp = svc.list_policy_tags(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Key>team</Key>"));

        let req = make_request(
            "UntagPolicy",
            vec![("PolicyArn", &policy_arn), ("TagKeys.member.1", "team")],
        );
        svc.untag_policy(&req).unwrap();

        let req = make_request("ListPolicyTags", vec![("PolicyArn", &policy_arn)]);
        let resp = svc.list_policy_tags(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(!body.contains("<Key>team</Key>"));
    }

    // ---- Tag Instance Profile Tests ----

    #[test]
    fn tag_untag_instance_profile() {
        let svc = make_service();
        svc.create_instance_profile(&make_request(
            "CreateInstanceProfile",
            vec![("InstanceProfileName", "tag-ip")],
        ))
        .unwrap();

        let req = make_request(
            "TagInstanceProfile",
            vec![
                ("InstanceProfileName", "tag-ip"),
                ("Tags.member.1.Key", "dept"),
                ("Tags.member.1.Value", "eng"),
            ],
        );
        svc.tag_instance_profile(&req).unwrap();

        let req = make_request(
            "ListInstanceProfileTags",
            vec![("InstanceProfileName", "tag-ip")],
        );
        let resp = svc.list_instance_profile_tags(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Key>dept</Key>"));

        let req = make_request(
            "UntagInstanceProfile",
            vec![
                ("InstanceProfileName", "tag-ip"),
                ("TagKeys.member.1", "dept"),
            ],
        );
        svc.untag_instance_profile(&req).unwrap();

        let req = make_request(
            "ListInstanceProfileTags",
            vec![("InstanceProfileName", "tag-ip")],
        );
        let resp = svc.list_instance_profile_tags(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(!body.contains("<Key>dept</Key>"));
    }

    // ---- Tag OIDC Provider Tests ----

    #[test]
    fn tag_untag_oidc_provider() {
        let svc = make_service();
        let req = make_request(
            "CreateOpenIDConnectProvider",
            vec![
                ("Url", "https://oidc.example.com"),
                (
                    "ThumbprintList.member.1",
                    "abcdef1234567890abcdef1234567890abcdef12",
                ),
            ],
        );
        let resp = svc.create_oidc_provider(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        let arn_start =
            body.find("<OpenIDConnectProviderArn>").unwrap() + "<OpenIDConnectProviderArn>".len();
        let arn_end = body.find("</OpenIDConnectProviderArn>").unwrap();
        let oidc_arn = body[arn_start..arn_end].to_string();

        let req = make_request(
            "TagOpenIDConnectProvider",
            vec![
                ("OpenIDConnectProviderArn", &oidc_arn),
                ("Tags.member.1.Key", "stage"),
                ("Tags.member.1.Value", "dev"),
            ],
        );
        svc.tag_oidc_provider(&req).unwrap();

        let req = make_request(
            "ListOpenIDConnectProviderTags",
            vec![("OpenIDConnectProviderArn", &oidc_arn)],
        );
        let resp = svc.list_oidc_provider_tags(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Key>stage</Key>"));

        let req = make_request(
            "UntagOpenIDConnectProvider",
            vec![
                ("OpenIDConnectProviderArn", &oidc_arn),
                ("TagKeys.member.1", "stage"),
            ],
        );
        svc.untag_oidc_provider(&req).unwrap();

        let req = make_request(
            "ListOpenIDConnectProviderTags",
            vec![("OpenIDConnectProviderArn", &oidc_arn)],
        );
        let resp = svc.list_oidc_provider_tags(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(!body.contains("<Key>stage</Key>"));
    }

    // ---- Update Role Tests ----

    #[test]
    fn update_role_description_and_max_session() {
        let svc = make_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
        svc.create_role(&make_request(
            "CreateRole",
            vec![
                ("RoleName", "upd-role"),
                ("AssumeRolePolicyDocument", trust),
            ],
        ))
        .unwrap();

        // UpdateRole with Description and MaxSessionDuration
        let req = make_request(
            "UpdateRole",
            vec![
                ("RoleName", "upd-role"),
                ("Description", "new description"),
                ("MaxSessionDuration", "7200"),
            ],
        );
        svc.update_role(&req).unwrap();

        // UpdateRoleDescription
        let req = make_request(
            "UpdateRoleDescription",
            vec![("RoleName", "upd-role"), ("Description", "updated desc")],
        );
        let resp = svc.update_role_description(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<Description>updated desc</Description>"));
    }

    // ---- UpdateAssumeRolePolicy Tests ----

    #[test]
    fn update_assume_role_policy() {
        let svc = make_service();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
        svc.create_role(&make_request(
            "CreateRole",
            vec![
                ("RoleName", "arp-role"),
                ("AssumeRolePolicyDocument", trust),
            ],
        ))
        .unwrap();

        let new_trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
        let req = make_request(
            "UpdateAssumeRolePolicy",
            vec![("RoleName", "arp-role"), ("PolicyDocument", new_trust)],
        );
        svc.update_assume_role_policy(&req).unwrap();

        // Verify by GetRole
        let req = make_request("GetRole", vec![("RoleName", "arp-role")]);
        let resp = svc.get_role(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("lambda.amazonaws.com"));
    }

    // ---- UpdateGroup Tests ----

    #[test]
    fn update_group_rename() {
        let svc = make_service();
        svc.create_group(&make_request("CreateGroup", vec![("GroupName", "old-grp")]))
            .unwrap();

        let req = make_request(
            "UpdateGroup",
            vec![("GroupName", "old-grp"), ("NewGroupName", "new-grp")],
        );
        svc.update_group(&req).unwrap();

        // Old name should not exist
        assert!(svc
            .get_group(&make_request("GetGroup", vec![("GroupName", "old-grp")]))
            .is_err());

        // New name should exist
        let resp = svc
            .get_group(&make_request("GetGroup", vec![("GroupName", "new-grp")]))
            .unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<GroupName>new-grp</GroupName>"));
    }

    // ---- UpdateUser Tests ----

    #[test]
    fn update_user_rename() {
        let svc = make_service();
        svc.create_user(&make_request("CreateUser", vec![("UserName", "old-user")]))
            .unwrap();

        let req = make_request(
            "UpdateUser",
            vec![("UserName", "old-user"), ("NewUserName", "new-user")],
        );
        svc.update_user(&req).unwrap();

        assert!(svc
            .get_user(&make_request("GetUser", vec![("UserName", "old-user")]))
            .is_err());

        let resp = svc
            .get_user(&make_request("GetUser", vec![("UserName", "new-user")]))
            .unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<UserName>new-user</UserName>"));
    }

    // ---- Account Password Policy Tests ----

    #[test]
    fn account_password_policy_lifecycle() {
        let svc = make_service();

        // Get before setting returns error
        let req = make_request("GetAccountPasswordPolicy", vec![]);
        assert!(svc.get_account_password_policy(&req).is_err());

        // Update (creates the policy)
        let req = make_request(
            "UpdateAccountPasswordPolicy",
            vec![
                ("MinimumPasswordLength", "12"),
                ("RequireSymbols", "true"),
                ("RequireNumbers", "true"),
            ],
        );
        svc.update_account_password_policy(&req).unwrap();

        // Get
        let req = make_request("GetAccountPasswordPolicy", vec![]);
        let resp = svc.get_account_password_policy(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<MinimumPasswordLength>12</MinimumPasswordLength>"));
        assert!(body.contains("<RequireSymbols>true</RequireSymbols>"));

        // Delete
        let req = make_request("DeleteAccountPasswordPolicy", vec![]);
        svc.delete_account_password_policy(&req).unwrap();

        // Should be gone
        let req = make_request("GetAccountPasswordPolicy", vec![]);
        assert!(svc.get_account_password_policy(&req).is_err());
    }

    // ---- GetAccountAuthorizationDetails Tests ----

    #[test]
    fn get_account_authorization_details() {
        let svc = make_service();

        // Create a user and role
        svc.create_user(&make_request("CreateUser", vec![("UserName", "auth-user")]))
            .unwrap();
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
        svc.create_role(&make_request(
            "CreateRole",
            vec![
                ("RoleName", "auth-role"),
                ("AssumeRolePolicyDocument", trust),
            ],
        ))
        .unwrap();

        let req = make_request("GetAccountAuthorizationDetails", vec![]);
        let resp = svc.get_account_authorization_details(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<UserName>auth-user</UserName>"));
        assert!(body.contains("<RoleName>auth-role</RoleName>"));
    }

    // ---- ListEntitiesForPolicy Tests ----

    #[test]
    fn list_entities_for_policy() {
        let svc = make_service();

        // Create policy
        let policy_doc = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}"#;
        let req = make_request(
            "CreatePolicy",
            vec![("PolicyName", "ent-pol"), ("PolicyDocument", policy_doc)],
        );
        let resp = svc.create_policy(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        let arn_start = body.find("<Arn>").unwrap() + 5;
        let arn_end = body.find("</Arn>").unwrap();
        let policy_arn = body[arn_start..arn_end].to_string();

        // Create role and attach policy
        let trust = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"ec2.amazonaws.com"},"Action":"sts:AssumeRole"}]}"#;
        svc.create_role(&make_request(
            "CreateRole",
            vec![
                ("RoleName", "ent-role"),
                ("AssumeRolePolicyDocument", trust),
            ],
        ))
        .unwrap();
        svc.attach_role_policy(&make_request(
            "AttachRolePolicy",
            vec![("RoleName", "ent-role"), ("PolicyArn", &policy_arn)],
        ))
        .unwrap();

        // Create user and attach policy
        svc.create_user(&make_request("CreateUser", vec![("UserName", "ent-user")]))
            .unwrap();
        svc.attach_user_policy(&make_request(
            "AttachUserPolicy",
            vec![("UserName", "ent-user"), ("PolicyArn", &policy_arn)],
        ))
        .unwrap();

        let req = make_request("ListEntitiesForPolicy", vec![("PolicyArn", &policy_arn)]);
        let resp = svc.list_entities_for_policy(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<RoleName>ent-role</RoleName>"));
        assert!(body.contains("<UserName>ent-user</UserName>"));
    }

    // ---- GetAccessKeyLastUsed Tests ----

    #[test]
    fn get_access_key_last_used() {
        let svc = make_service();
        svc.create_user(&make_request("CreateUser", vec![("UserName", "keyuser")]))
            .unwrap();

        let req = make_request("CreateAccessKey", vec![("UserName", "keyuser")]);
        let resp = svc.create_access_key(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        let kid_start = body.find("<AccessKeyId>").unwrap() + 13;
        let kid_end = body.find("</AccessKeyId>").unwrap();
        let key_id = body[kid_start..kid_end].to_string();

        let req = make_request("GetAccessKeyLastUsed", vec![("AccessKeyId", &key_id)]);
        let resp = svc.get_access_key_last_used(&req).unwrap();
        let body = String::from_utf8_lossy(&resp.body);
        assert!(body.contains("<UserName>keyuser</UserName>"));
        // No last used info yet -- should show N/A
        assert!(body.contains("<ServiceName>N/A</ServiceName>"));
    }
}
