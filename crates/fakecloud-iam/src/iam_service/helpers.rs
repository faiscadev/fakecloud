use super::*;

/// Get the AWS partition from a region string.
pub(crate) fn partition_for_region(region: &str) -> &str {
    if region.starts_with("cn-") {
        "aws-cn"
    } else if region.starts_with("us-gov-") {
        "aws-us-gov"
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

/// Actions on the IAM service that mutate state. Kept in sync with the
/// dispatch table in `handle`.
pub(crate) fn is_mutating_action(action: &str) -> bool {
    matches!(
        action,
        "CreateUser"
            | "DeleteUser"
            | "UpdateUser"
            | "TagUser"
            | "UntagUser"
            | "CreateAccessKey"
            | "DeleteAccessKey"
            | "UpdateAccessKey"
            | "CreateRole"
            | "DeleteRole"
            | "UpdateRole"
            | "UpdateRoleDescription"
            | "UpdateAssumeRolePolicy"
            | "TagRole"
            | "UntagRole"
            | "PutRolePermissionsBoundary"
            | "DeleteRolePermissionsBoundary"
            | "CreatePolicy"
            | "DeletePolicy"
            | "TagPolicy"
            | "UntagPolicy"
            | "CreatePolicyVersion"
            | "DeletePolicyVersion"
            | "SetDefaultPolicyVersion"
            | "AttachRolePolicy"
            | "DetachRolePolicy"
            | "PutRolePolicy"
            | "DeleteRolePolicy"
            | "AttachUserPolicy"
            | "DetachUserPolicy"
            | "PutUserPolicy"
            | "DeleteUserPolicy"
            | "PutUserPermissionsBoundary"
            | "DeleteUserPermissionsBoundary"
            | "CreateGroup"
            | "DeleteGroup"
            | "UpdateGroup"
            | "AddUserToGroup"
            | "RemoveUserFromGroup"
            | "PutGroupPolicy"
            | "DeleteGroupPolicy"
            | "AttachGroupPolicy"
            | "DetachGroupPolicy"
            | "CreateInstanceProfile"
            | "DeleteInstanceProfile"
            | "AddRoleToInstanceProfile"
            | "RemoveRoleFromInstanceProfile"
            | "TagInstanceProfile"
            | "UntagInstanceProfile"
            | "CreateLoginProfile"
            | "UpdateLoginProfile"
            | "DeleteLoginProfile"
            | "CreateSAMLProvider"
            | "DeleteSAMLProvider"
            | "UpdateSAMLProvider"
            | "CreateOpenIDConnectProvider"
            | "DeleteOpenIDConnectProvider"
            | "UpdateOpenIDConnectProviderThumbprint"
            | "AddClientIDToOpenIDConnectProvider"
            | "RemoveClientIDFromOpenIDConnectProvider"
            | "TagOpenIDConnectProvider"
            | "UntagOpenIDConnectProvider"
            | "UploadServerCertificate"
            | "DeleteServerCertificate"
            | "UploadSigningCertificate"
            | "UpdateSigningCertificate"
            | "DeleteSigningCertificate"
            | "UploadSSHPublicKey"
            | "UpdateSSHPublicKey"
            | "DeleteSSHPublicKey"
            | "CreateServiceLinkedRole"
            | "DeleteServiceLinkedRole"
            | "CreateAccountAlias"
            | "DeleteAccountAlias"
            | "UpdateAccountPasswordPolicy"
            | "DeleteAccountPasswordPolicy"
            | "GenerateCredentialReport"
            | "CreateVirtualMFADevice"
            | "DeleteVirtualMFADevice"
            | "EnableMFADevice"
            | "DeactivateMFADevice"
            | "UpdateServerCertificate"
            | "ChangePassword"
            | "ResyncMFADevice"
            | "SetSecurityTokenServicePreferences"
            | "TagSAMLProvider"
            | "UntagSAMLProvider"
            | "TagServerCertificate"
            | "UntagServerCertificate"
            | "TagMFADevice"
            | "UntagMFADevice"
            | "CreateServiceSpecificCredential"
            | "DeleteServiceSpecificCredential"
            | "ResetServiceSpecificCredential"
            | "UpdateServiceSpecificCredential"
            | "EnableOrganizationsRootCredentialsManagement"
            | "DisableOrganizationsRootCredentialsManagement"
            | "EnableOrganizationsRootSessions"
            | "DisableOrganizationsRootSessions"
            | "GenerateOrganizationsAccessReport"
            | "GenerateServiceLastAccessedDetails"
            | "CreateDelegationRequest"
            | "AcceptDelegationRequest"
            | "RejectDelegationRequest"
            | "UpdateDelegationRequest"
            | "AssociateDelegationRequest"
            | "SendDelegationToken"
            | "EnableOutboundWebIdentityFederation"
            | "DisableOutboundWebIdentityFederation"
    )
}

/// Look up resource tags for an IAM ARN.
///
/// IAM ARN formats:
/// - `arn:{p}:iam::{account}:user/{path/}name`
/// - `arn:{p}:iam::{account}:role/{path/}name`
/// - `arn:{p}:iam::{account}:policy/{path/}name`
/// - `arn:{p}:iam::{account}:instance-profile/{path/}name`
/// - `*` for account-level operations
pub(crate) fn iam_resource_tags(
    state: &SharedIamState,
    resource_arn: &str,
) -> Option<std::collections::HashMap<String, String>> {
    if resource_arn == "*" {
        return Some(std::collections::HashMap::new());
    }
    // Parse the resource segment after the 6th colon
    let parts: Vec<&str> = resource_arn.split(':').collect();
    if parts.len() < 6 {
        return None;
    }
    let resource = parts[5];
    let account_id = parts.get(4).copied().unwrap_or("");
    let accounts = state.read();
    let state = accounts.get(account_id)?;
    if let Some(rest) = resource.strip_prefix("user/") {
        let name = rest.rsplit('/').next().unwrap_or(rest);
        state
            .users
            .get(name)
            .map(|u| tags_to_hashmap_or_empty(&u.tags))
    } else if let Some(rest) = resource.strip_prefix("role/") {
        let name = rest.rsplit('/').next().unwrap_or(rest);
        state
            .roles
            .get(name)
            .map(|r| tags_to_hashmap_or_empty(&r.tags))
    } else if resource.starts_with("policy/") {
        // Policies are keyed by full ARN
        state
            .policies
            .get(resource_arn)
            .map(|p| tags_to_hashmap_or_empty(&p.tags))
    } else if let Some(rest) = resource.strip_prefix("instance-profile/") {
        let name = rest.rsplit('/').next().unwrap_or(rest);
        state
            .instance_profiles
            .get(name)
            .map(|ip| tags_to_hashmap_or_empty(&ip.tags))
    } else {
        Some(std::collections::HashMap::new())
    }
}

pub(crate) fn tags_to_hashmap_or_empty(tags: &[Tag]) -> std::collections::HashMap<String, String> {
    tags.iter()
        .map(|t| (t.key.clone(), t.value.clone()))
        .collect()
}

/// Extract request tags from IAM operations that accept tags.
pub(crate) fn iam_request_tags(
    request: &AwsRequest,
    action: &str,
) -> Option<std::collections::HashMap<String, String>> {
    const TAG_ACTIONS: &[&str] = &[
        "CreateUser",
        "TagUser",
        "CreateRole",
        "TagRole",
        "CreatePolicy",
        "TagPolicy",
        "CreateInstanceProfile",
        "TagInstanceProfile",
        "CreateOpenIDConnectProvider",
        "TagOpenIDConnectProvider",
        "CreateSAMLProvider",
        "TagSAMLProvider",
        "UploadServerCertificate",
        "TagServerCertificate",
    ];
    if TAG_ACTIONS.contains(&action) {
        let tags = parse_tags(&request.query_params);
        Some(tags.into_iter().map(|t| (t.key, t.value)).collect())
    } else {
        Some(std::collections::HashMap::new())
    }
}

/// Derive the fully-qualified IAM resource ARN for a given action +
/// request. Reads the relevant parameter (UserName, RoleName, PolicyArn,
/// etc.) from the request's query params or body. Falls back to `*` when
/// the action targets no specific resource (e.g. `ListUsers`,
/// `GetAccountSummary`) or the parameter is missing.
///
/// IAM resources follow a small set of shapes
/// (`user/`, `role/`, `group/`, `policy/`, `instance-profile/`,
/// `mfa/`, `server-certificate/`, `saml-provider/`, `oidc-provider/`).
/// Each action is classified by which shape it targets.
pub(crate) fn iam_action_resource(
    action: &str,
    partition: &str,
    account: &str,
    request: &AwsRequest,
) -> String {
    let params = &request.query_params;
    let wildcard = || "*".to_string();
    let user_arn = |name: &str| format!("arn:{}:iam::{}:user/{}", partition, account, name);
    let role_arn = |name: &str| format!("arn:{}:iam::{}:role/{}", partition, account, name);
    let group_arn = |name: &str| format!("arn:{}:iam::{}:group/{}", partition, account, name);
    let policy_arn = |name: &str| format!("arn:{}:iam::{}:policy/{}", partition, account, name);
    let profile_arn = |name: &str| {
        format!(
            "arn:{}:iam::{}:instance-profile/{}",
            partition, account, name
        )
    };
    let mfa_arn = |name: &str| format!("arn:{}:iam::{}:mfa/{}", partition, account, name);
    let server_cert_arn = |name: &str| {
        format!(
            "arn:{}:iam::{}:server-certificate/{}",
            partition, account, name
        )
    };

    // User-scoped actions: read `UserName` from params. Missing -> wildcard.
    let user_scoped: &[&str] = &[
        "CreateUser",
        "GetUser",
        "DeleteUser",
        "UpdateUser",
        "TagUser",
        "UntagUser",
        "ListUserTags",
        "CreateAccessKey",
        "DeleteAccessKey",
        "ListAccessKeys",
        "UpdateAccessKey",
        "GetAccessKeyLastUsed",
        "CreateLoginProfile",
        "GetLoginProfile",
        "DeleteLoginProfile",
        "UpdateLoginProfile",
        "AttachUserPolicy",
        "DetachUserPolicy",
        "ListAttachedUserPolicies",
        "PutUserPolicy",
        "GetUserPolicy",
        "DeleteUserPolicy",
        "ListUserPolicies",
        "PutUserPermissionsBoundary",
        "DeleteUserPermissionsBoundary",
        "AddUserToGroup",
        "RemoveUserFromGroup",
        "ListGroupsForUser",
        "EnableMFADevice",
        "DeactivateMFADevice",
        "ListMFADevices",
        "UploadSSHPublicKey",
        "GetSSHPublicKey",
        "UpdateSSHPublicKey",
        "DeleteSSHPublicKey",
        "ListSSHPublicKeys",
        "UploadSigningCertificate",
        "UpdateSigningCertificate",
        "DeleteSigningCertificate",
        "ListSigningCertificates",
    ];

    // Role-scoped actions: read `RoleName` from params.
    let role_scoped: &[&str] = &[
        "CreateRole",
        "GetRole",
        "DeleteRole",
        "UpdateRole",
        "UpdateRoleDescription",
        "UpdateAssumeRolePolicy",
        "TagRole",
        "UntagRole",
        "ListRoleTags",
        "PutRolePermissionsBoundary",
        "DeleteRolePermissionsBoundary",
        "AttachRolePolicy",
        "DetachRolePolicy",
        "ListAttachedRolePolicies",
        "PutRolePolicy",
        "GetRolePolicy",
        "DeleteRolePolicy",
        "ListRolePolicies",
        // NOTE: CreateServiceLinkedRole excluded — its target resource is
        // built from `AWSServiceName`, not `RoleName`, and is handled in
        // the match block below (identified by cubic on PR #395).
        // GetServiceLinkedRoleDeletionStatus is also excluded — it
        // operates on a deletion-task id rather than a role name; it
        // falls through to the wildcard branch below.
        "DeleteServiceLinkedRole",
        "ListInstanceProfilesForRole",
    ];

    // Group-scoped actions: read `GroupName` from params.
    let group_scoped: &[&str] = &[
        "CreateGroup",
        "GetGroup",
        "DeleteGroup",
        "UpdateGroup",
        "PutGroupPolicy",
        "GetGroupPolicy",
        "DeleteGroupPolicy",
        "ListGroupPolicies",
        "AttachGroupPolicy",
        "DetachGroupPolicy",
        "ListAttachedGroupPolicies",
    ];

    // Policy-scoped actions: read `PolicyArn` from params (ARN in place).
    let policy_scoped_arn: &[&str] = &[
        "GetPolicy",
        "DeletePolicy",
        "TagPolicy",
        "UntagPolicy",
        "ListPolicyTags",
        "CreatePolicyVersion",
        "GetPolicyVersion",
        "ListPolicyVersions",
        "DeletePolicyVersion",
        "SetDefaultPolicyVersion",
        "ListEntitiesForPolicy",
    ];

    // Instance-profile-scoped actions: read `InstanceProfileName`.
    let profile_scoped: &[&str] = &[
        "CreateInstanceProfile",
        "GetInstanceProfile",
        "DeleteInstanceProfile",
        "TagInstanceProfile",
        "UntagInstanceProfile",
        "ListInstanceProfileTags",
        "AddRoleToInstanceProfile",
        "RemoveRoleFromInstanceProfile",
    ];

    if user_scoped.contains(&action) {
        return params
            .get("UserName")
            .map(|n| user_arn(n))
            .unwrap_or_else(wildcard);
    }
    if role_scoped.contains(&action) {
        return params
            .get("RoleName")
            .map(|n| role_arn(n))
            .unwrap_or_else(wildcard);
    }
    if group_scoped.contains(&action) {
        return params
            .get("GroupName")
            .map(|n| group_arn(n))
            .unwrap_or_else(wildcard);
    }
    if policy_scoped_arn.contains(&action) {
        return params.get("PolicyArn").cloned().unwrap_or_else(wildcard);
    }
    if profile_scoped.contains(&action) {
        return params
            .get("InstanceProfileName")
            .map(|n| profile_arn(n))
            .unwrap_or_else(wildcard);
    }

    match action {
        // CreatePolicy: target ARN is the to-be-created policy.
        "CreatePolicy" => params
            .get("PolicyName")
            .map(|n| policy_arn(n))
            .unwrap_or_else(wildcard),
        // Service-linked roles are created by AWS service name, not
        // role name. Their ARNs follow the `role/aws-service-role/<svc>`
        // convention (identified by cubic on PR #395).
        "CreateServiceLinkedRole" => params
            .get("AWSServiceName")
            .map(|svc| {
                format!(
                    "arn:{}:iam::{}:role/aws-service-role/{}",
                    partition, account, svc
                )
            })
            .unwrap_or_else(wildcard),
        // MFA actions keyed by SerialNumber (which is the mfa ARN itself
        // for virtual devices, a plain string for hardware devices).
        "CreateVirtualMFADevice" => params
            .get("VirtualMFADeviceName")
            .map(|n| mfa_arn(n))
            .unwrap_or_else(wildcard),
        "DeleteVirtualMFADevice" => params.get("SerialNumber").cloned().unwrap_or_else(wildcard),
        // Note: ResyncMFADevice is not in SUPPORTED_ACTIONS and so
        // never reaches this match — if it's added later, handle
        // `SerialNumber` here.
        // Server certificates keyed by ServerCertificateName.
        "UploadServerCertificate" | "GetServerCertificate" | "DeleteServerCertificate" => params
            .get("ServerCertificateName")
            .map(|n| server_cert_arn(n))
            .unwrap_or_else(wildcard),
        // SAML / OIDC providers reference their ARN directly.
        "CreateSAMLProvider" => params
            .get("Name")
            .map(|n| format!("arn:{}:iam::{}:saml-provider/{}", partition, account, n))
            .unwrap_or_else(wildcard),
        "UpdateSAMLProvider" | "DeleteSAMLProvider" | "GetSAMLProvider" => params
            .get("SAMLProviderArn")
            .cloned()
            .unwrap_or_else(wildcard),
        "CreateOpenIDConnectProvider" => params
            .get("Url")
            .map(|u| {
                format!(
                    "arn:{}:iam::{}:oidc-provider/{}",
                    partition,
                    account,
                    u.trim_start_matches("https://")
                )
            })
            .unwrap_or_else(wildcard),
        "GetOpenIDConnectProvider"
        | "DeleteOpenIDConnectProvider"
        | "AddClientIDToOpenIDConnectProvider"
        | "RemoveClientIDFromOpenIDConnectProvider"
        | "UpdateOpenIDConnectProviderThumbprint"
        | "TagOpenIDConnectProvider"
        | "UntagOpenIDConnectProvider"
        | "ListOpenIDConnectProviderTags" => params
            .get("OpenIDConnectProviderArn")
            .cloned()
            .unwrap_or_else(wildcard),
        // Account-scoped / listing actions have no per-resource target.
        "ListUsers"
        | "ListRoles"
        | "ListGroups"
        | "ListPolicies"
        | "ListInstanceProfiles"
        | "ListVirtualMFADevices"
        | "ListServerCertificates"
        | "ListSAMLProviders"
        | "ListOpenIDConnectProviders"
        | "ListAccountAliases"
        | "CreateAccountAlias"
        | "DeleteAccountAlias"
        | "GetAccountSummary"
        | "GetAccountAuthorizationDetails"
        | "GenerateCredentialReport"
        | "GetCredentialReport"
        | "GetAccountPasswordPolicy"
        | "UpdateAccountPasswordPolicy"
        | "DeleteAccountPasswordPolicy" => wildcard(),
        // Anything we didn't classify above — be conservative.
        _ => wildcard(),
    }
}

/// Extract the caller's access key from the request's Authorization header.
pub(crate) fn extract_access_key(req: &AwsRequest) -> Option<String> {
    let auth = req.headers.get("authorization")?.to_str().ok()?;
    let info = fakecloud_aws::sigv4::parse_sigv4(auth)?;
    Some(info.access_key)
}

/// Convert a hyphenated service name to title case, handling known abbreviations.
pub(crate) fn title_case_service(s: &str) -> String {
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

pub(crate) fn url_encode(s: &str) -> String {
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

/// Resolve the calling user when UserName is not provided.
/// Returns the first user found or a default "default" name.
pub(crate) fn resolve_calling_user(state: &crate::state::IamState, _account_id: &str) -> String {
    // In a real implementation, we'd look up the user from the access key.
    // For simplicity, return the first user or "default".
    state
        .users
        .keys()
        .next()
        .cloned()
        .unwrap_or_else(|| "default".to_string())
}

pub(crate) fn generate_id() -> String {
    // AWS unique IDs (AIDA/AROA/AGPA/ANPA/AIPA/ASCA...) are a 4-char prefix
    // followed by 17 alphanumerics for 21 characters total, so this returns 17
    // chars. Access-key-style IDs (AKIA/APKA) are 20 chars total and slice this
    // down to 16 at their call sites.
    uuid::Uuid::new_v4()
        .to_string()
        .replace('-', "")
        .to_uppercase()[..17]
        .to_string()
}

pub(crate) fn generate_long_id() -> String {
    // Generate 21 uppercase hex chars (used with 3-char prefixes like ASC = 24 chars).
    // CertificateId requires minimum 24 characters.
    uuid::Uuid::new_v4()
        .to_string()
        .replace('-', "")
        .to_uppercase()[..21]
        .to_string()
}

pub(crate) fn parse_tags(params: &std::collections::HashMap<String, String>) -> Vec<Tag> {
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

pub(crate) fn parse_tag_keys(params: &std::collections::HashMap<String, String>) -> Vec<String> {
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

pub(crate) fn tags_xml(tags: &[Tag]) -> String {
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

pub(crate) fn paginated_tags_response(
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

    let next_token = req.query_params.get("Marker").map(|s| s.as_str());
    // A Marker that does not parse as a valid offset is rejected with the same
    // "Invalid Marker." ValidationError the other IAM list ops emit, rather than
    // being silently treated as the first page (bug-audit 2026-05-28, 1.7).
    let (page, next_marker) = paginate_checked(tags, next_token, max_items).map_err(|_| {
        AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationError",
            "Invalid Marker.",
        )
    })?;

    let is_truncated = next_marker.is_some();
    let members = tags_xml(&page);
    let marker = match &next_marker {
        Some(m) => format!("<Marker>{m}</Marker>"),
        None => String::new(),
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

pub(crate) fn validate_tags(tags: &[Tag], existing_count: usize) -> Result<(), AwsServiceError> {
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

pub(crate) fn validate_untag_keys(keys: &[String]) -> Result<(), AwsServiceError> {
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

/// Resolve the `PolicyName` to render for an attached managed policy ARN.
///
/// Customer-managed policies live in `state.policies` with their stored name.
/// AWS-managed policies (`arn:aws:iam::aws:policy/...`) are accepted by the
/// `Attach*Policy` calls without being inserted into state, so we derive the
/// name from the ARN's last path segment to match what AWS itself returns.
pub(crate) fn attached_policy_name(state: &crate::state::IamState, arn: &str) -> String {
    state
        .policies
        .get(arn)
        .map(|p| p.policy_name.clone())
        .unwrap_or_else(|| arn.rsplit('/').next().unwrap_or(arn).to_string())
}

/// Filter a list of inline-policy names by the IAM `Marker`/`MaxItems` triad
/// (cursor = policy name, sorted) and render the `<member>` block. Shared by
/// ListRolePolicies/ListUserPolicies/ListGroupPolicies, which all hardcoded
/// IsTruncated=false and ignored pagination.
pub(crate) fn paginate_policy_names(
    mut names: Vec<String>,
    req: &AwsRequest,
) -> (String, bool, Option<String>) {
    let max_items: usize = req
        .query_params
        .get("MaxItems")
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let marker = req.query_params.get("Marker").cloned();
    names.sort();
    // Resume at the first name strictly greater than the marker (the last name of
    // the previous page). A deleted marker then still advances instead of falling
    // back to 0 and restarting the listing.
    let start = marker
        .as_ref()
        .map(|m| {
            names
                .iter()
                .position(|n| n.as_str() > m.as_str())
                .unwrap_or(names.len())
        })
        .unwrap_or(0);
    let rest: Vec<String> = names.into_iter().skip(start).collect();
    let is_truncated = rest.len() > max_items;
    let page = if is_truncated {
        &rest[..max_items]
    } else {
        &rest[..]
    };
    let next_marker = if is_truncated {
        page.last().cloned()
    } else {
        None
    };
    let members = page
        .iter()
        .map(|n| format!("      <member>{}</member>", xml_escape(n)))
        .collect::<Vec<_>>()
        .join("\n");
    (members, is_truncated, next_marker)
}

/// The IAM path embedded in a policy ARN: everything between `:policy` and the
/// final `/` before the name, e.g. `…:policy/foo/bar/Name` -> `/foo/bar/`,
/// `…:policy/Name` -> `/`. Used to filter ListAttached*Policies by `PathPrefix`.
pub(crate) fn policy_path_from_arn(arn: &str) -> String {
    arn.split(":policy")
        .nth(1)
        .and_then(|rest| rest.rfind('/').map(|i| rest[..=i].to_string()))
        .unwrap_or_else(|| "/".to_string())
}

/// Filter a list of attached-policy ARNs by `PathPrefix` and paginate with the
/// IAM `Marker`/`MaxItems` triad (cursor = policy ARN). Returns the rendered
/// `<member>` block, the truncation flag, and the next marker. Shared by
/// ListAttached{Role,User,Group}Policies, which all hardcoded IsTruncated=false
/// and ignored PathPrefix.
pub(crate) fn paginate_attached_policies(
    state: &crate::state::IamState,
    arns: &[String],
    req: &AwsRequest,
) -> (String, bool, Option<String>) {
    let path_prefix = req.query_params.get("PathPrefix").cloned();
    let max_items: usize = req
        .query_params
        .get("MaxItems")
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let marker = req.query_params.get("Marker").cloned();

    let mut items: Vec<(String, String)> = arns
        .iter()
        .filter(|arn| {
            path_prefix
                .as_ref()
                .is_none_or(|p| policy_path_from_arn(arn).starts_with(p))
        })
        .map(|arn| (attached_policy_name(state, arn), arn.clone()))
        .collect();
    items.sort_by(|a, b| a.1.cmp(&b.1));

    // Resume at the first ARN strictly greater than the marker (the last ARN of
    // the previous page). A deleted marker then still advances instead of falling
    // back to 0 and restarting the listing.
    let start = marker
        .as_ref()
        .map(|m| {
            items
                .iter()
                .position(|(_, a)| a.as_str() > m.as_str())
                .unwrap_or(items.len())
        })
        .unwrap_or(0);
    let rest: Vec<(String, String)> = items.into_iter().skip(start).collect();
    let is_truncated = rest.len() > max_items;
    let page = if is_truncated {
        &rest[..max_items]
    } else {
        &rest[..]
    };
    let next_marker = if is_truncated {
        page.last().map(|(_, a)| a.clone())
    } else {
        None
    };

    let members = page
        .iter()
        .map(|(name, arn)| {
            format!(
                "      <member>\n        <PolicyName>{name}</PolicyName>\n        <PolicyArn>{arn}</PolicyArn>\n      </member>"
            )
        })
        .collect::<Vec<_>>()
        .join("\n");
    (members, is_truncated, next_marker)
}

/// Extract a required query parameter, returning the supplied IAM-specific
/// wire error code when missing. The shared `required_param` helper hard-codes
/// `MissingParameter`, which isn't in any IAM operation's Smithy error list.
/// IAM ops must surface one of their declared errors (usually
/// `InvalidInputException` -> `InvalidInput`, or `NoSuchEntity` when the
/// missing field identifies an entity).
pub(crate) fn required_param_with_code(
    params: &std::collections::HashMap<String, String>,
    name: &str,
    code: &str,
) -> Result<String, AwsServiceError> {
    params
        .get(name)
        .cloned()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                code,
                format!("The request must contain the parameter {name}."),
            )
        })
}

/// Length-validate a string, emitting the supplied IAM-declared wire error
/// code. Mirrors `fakecloud_core::validation::validate_string_length` but
/// avoids its hard-coded `ValidationException` (not declared on any IAM op).
pub(crate) fn validate_string_length_with_code(
    field: &str,
    value: &str,
    min: usize,
    max: usize,
    code: &str,
) -> Result<(), AwsServiceError> {
    let len = value.len();
    if len < min || len > max {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            code,
            format!(
                "Value at '{field}' failed to satisfy constraint: \
                 Member must have length between {min} and {max}",
            ),
        ));
    }
    Ok(())
}

/// Length-validate an optional string with an IAM-declared wire error code.
pub(crate) fn validate_optional_string_length_with_code(
    field: &str,
    value: Option<&str>,
    min: usize,
    max: usize,
    code: &str,
) -> Result<(), AwsServiceError> {
    if let Some(v) = value {
        validate_string_length_with_code(field, v, min, max, code)?;
    }
    Ok(())
}

/// Validate the standard IAM pagination triad (`Marker`, `MaxItems`,
/// `PathPrefix`) shared by virtually every IAM `List*` op. Smithy bounds
/// per the IAM model: `markerType` 1..=320, `maxItemsType` 1..=1000,
/// `pathPrefixType` 1..=512. Returns the validated `MaxItems` (defaulting
/// to 100, AWS' implicit cap) so callers don't re-parse it.
pub(crate) fn validate_list_pagination(req: &AwsRequest) -> Result<i64, AwsServiceError> {
    validate_optional_string_length_with_code(
        "Marker",
        req.query_params.get("Marker").map(|s| s.as_str()),
        1,
        320,
        "InvalidInput",
    )?;
    validate_optional_string_length_with_code(
        "PathPrefix",
        req.query_params.get("PathPrefix").map(|s| s.as_str()),
        1,
        512,
        "InvalidInput",
    )?;
    let raw = req.query_params.get("MaxItems").map(|s| s.as_str());
    let max_items_i64: Option<i64> = match raw {
        Some(s) => Some(s.parse().map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                format!("Value '{s}' at 'MaxItems' is not a valid integer"),
            )
        })?),
        None => None,
    };
    if let Some(v) = max_items_i64 {
        if !(1..=1000).contains(&v) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                format!("Value '{v}' at 'MaxItems' must be between 1 and 1000"),
            ));
        }
    }
    Ok(max_items_i64.unwrap_or(100))
}

/// Validate an IAM `Path` value. IAM paths must begin and end with `/`,
/// contain no empty segments (`//`), stay within 512 chars, and use only the
/// permitted character set. Equivalent to the Smithy `pathType` pattern
/// `^/(.*/)?$` plus the per-character constraint. Shared by CreateRole,
/// CreatePolicy, and CreateVirtualMFADevice so a malformed path is rejected
/// up front rather than baked into a broken ARN.
pub(crate) fn is_valid_iam_path(path: &str) -> bool {
    if !path.starts_with('/') || !path.ends_with('/') {
        return false;
    }
    if path.contains("//") {
        return false;
    }
    if path.len() > 512 {
        return false;
    }
    path.chars().all(|c| {
        c.is_alphanumeric()
            || c == '/'
            || c == '-'
            || c == '_'
            || c == '.'
            || c == '+'
            || c == '='
            || c == '@'
            || c == ','
    })
}

/// Validate an IAM `Path` and return the IAM-declared `InvalidInput` error on
/// failure, using the exact message AWS emits.
pub(crate) fn validate_iam_path(path: &str) -> Result<(), AwsServiceError> {
    if !is_valid_iam_path(path) {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "InvalidInput",
            "The specified value for path is invalid. It must begin and end with / and contain only alphanumeric characters and/or / characters.".to_string(),
        ));
    }
    Ok(())
}

/// Range-check an IAM `MaxSessionDuration` value (seconds). AWS enforces the
/// Smithy `@range(min: 3600, max: 43200)` constraint and surfaces violations
/// as a `ValidationError` on CreateRole/UpdateRole.
pub(crate) fn validate_max_session_duration(seconds: i32) -> Result<(), AwsServiceError> {
    if seconds < 3600 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationError",
            format!(
                "1 validation error detected: Value '{seconds}' at 'maxSessionDuration' failed to satisfy constraint: Member must have value greater than or equal to 3600"
            ),
        ));
    }
    if seconds > 43200 {
        return Err(AwsServiceError::aws_error(
            StatusCode::BAD_REQUEST,
            "ValidationError",
            format!(
                "1 validation error detected: Value '{seconds}' at 'maxSessionDuration' failed to satisfy constraint: Member must have value less than or equal to 43200"
            ),
        ));
    }
    Ok(())
}

/// Parse and range-check an optional `MaxSessionDuration` query parameter.
/// Returns `None` when the parameter is absent (caller keeps the existing or
/// default value), `Some(seconds)` when present and valid, or a
/// `ValidationError` when it is not a valid integer or out of range.
pub(crate) fn parse_max_session_duration(
    params: &std::collections::HashMap<String, String>,
) -> Result<Option<i32>, AwsServiceError> {
    match params.get("MaxSessionDuration") {
        Some(raw) => {
            let seconds: i32 = raw.parse().map_err(|_| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "ValidationError",
                    format!(
                        "1 validation error detected: Value '{raw}' at 'maxSessionDuration' failed to satisfy constraint: Member must be a valid integer"
                    ),
                )
            })?;
            validate_max_session_duration(seconds)?;
            Ok(Some(seconds))
        }
        None => Ok(None),
    }
}

/// Normalize a policy document's `Statement` element into a slice of statement
/// values. AWS (and boto3/aws-cli) accept `Statement` as either a JSON array or
/// a single JSON object; validators that only inspect `as_array()` silently skip
/// the single-object form. Returns an empty vec when `Statement` is absent or is
/// neither an array nor an object.
pub(crate) fn statements_as_slice(doc: &serde_json::Value) -> Vec<&serde_json::Value> {
    match doc.get("Statement") {
        Some(serde_json::Value::Array(arr)) => arr.iter().collect(),
        Some(obj @ serde_json::Value::Object(_)) => vec![obj],
        _ => Vec::new(),
    }
}

pub(crate) fn empty_response(action: &str, request_id: &str) -> String {
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
mod id_tests {
    use super::*;

    // AWS principal unique IDs are a 4-char prefix + 17 alphanumerics = 21 chars.
    #[test]
    fn generate_id_yields_21_char_unique_ids() {
        let id = generate_id();
        assert_eq!(id.len(), 17, "generate_id suffix must be 17 chars");
        for prefix in ["AIDA", "AROA", "AGPA", "ANPA", "AIPA"] {
            let unique = format!("{prefix}{}", generate_id());
            assert_eq!(unique.len(), 21, "{unique} must be 21 chars total");
            assert!(unique.starts_with(prefix));
        }
        // Access-key-style IDs stay 20 chars total (prefix + 16).
        let access_key = format!("FKIA{}", &generate_id()[..16]);
        assert_eq!(access_key.len(), 20);
    }
}
