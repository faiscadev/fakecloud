use chrono::Utc;
use http::StatusCode;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{IamPolicy, PolicyVersion};
use crate::xml_responses;

use super::{
    empty_response, generate_id, paginated_tags_response, parse_tag_keys, parse_tags,
    partition_for_region, url_encode, validate_tags, validate_untag_keys, IamService,
};
use fakecloud_core::query::required_param;

use fakecloud_aws::xml::xml_escape;

use crate::policy_validation::validate_policy_document;

/// IAM ``ListPolicies`` ``Scope`` filter.
///
/// ``All`` (the default) returns both AWS-managed and customer-managed policies,
/// ``Aws`` returns only AWS-managed, and ``Local`` returns only
/// customer-managed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PolicyScope {
    All,
    Aws,
    Local,
}

impl PolicyScope {
    fn from_query(value: Option<&str>) -> Self {
        match value {
            Some("AWS") => Self::Aws,
            Some("Local") => Self::Local,
            _ => Self::All,
        }
    }
}

impl IamService {
    pub(super) fn create_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let input = CreatePolicyInput::from_query(&req.query_params)?;

        let partition = partition_for_region(&req.region);
        let effective_account = self.effective_account_id(req);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let arn = format!(
            "arn:{}:iam::{}:policy{}{}",
            partition, effective_account, input.path, input.policy_name
        );

        if state.policies.contains_key(&arn) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                format!(
                    "A policy called {} already exists. Duplicate names are not allowed.",
                    input.policy_name
                ),
            ));
        }

        let now = Utc::now();
        let version = PolicyVersion {
            version_id: "v1".to_string(),
            document: input.policy_document,
            is_default: true,
            created_at: now,
        };

        let policy = IamPolicy {
            policy_id: format!("ANPA{}", generate_id()),
            arn: arn.clone(),
            policy_name: input.policy_name,
            path: input.path,
            description: input.description,
            created_at: now,
            tags: input.tags,
            default_version_id: "v1".to_string(),
            versions: vec![version],
            next_version_num: 2,
            attachment_count: 0,
        };

        let xml = xml_responses::create_policy_response(&policy, &req.request_id);
        state.policies.insert(arn, policy);

        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn get_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let policy = resolve_policy(state, &policy_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} does not exist."),
            )
        })?;

        let xml = xml_responses::get_policy_response(&policy, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn delete_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

    pub(super) fn list_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_optional_string_length(
            "pathPrefix",
            req.query_params.get("PathPrefix").map(|s| s.as_str()),
            1,
            512,
        )?;
        validate_optional_string_length(
            "marker",
            req.query_params.get("Marker").map(|s| s.as_str()),
            1,
            320,
        )?;
        validate_optional_range_i64(
            "maxItems",
            parse_optional_i64_param(
                "maxItems",
                req.query_params.get("MaxItems").map(|s| s.as_str()),
            )?,
            1,
            1000,
        )?;
        validate_optional_enum(
            "scope",
            req.query_params.get("Scope").map(|s| s.as_str()),
            &["All", "AWS", "Local"],
        )?;
        validate_optional_enum(
            "policyUsageFilter",
            req.query_params
                .get("PolicyUsageFilter")
                .map(|s| s.as_str()),
            &["PermissionsPolicy", "PermissionsBoundary"],
        )?;

        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let path_prefix = req.query_params.get("PathPrefix").cloned();
        let scope = PolicyScope::from_query(req.query_params.get("Scope").map(|s| s.as_str()));
        let max_items: usize = req
            .query_params
            .get("MaxItems")
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);
        let marker = req.query_params.get("Marker").cloned();

        // Customer-managed (``Local``) policies live in state; AWS-managed
        // policies come from the seeded catalog. ``Scope`` selects which sets
        // to include.
        let mut policies: Vec<IamPolicy> = if matches!(scope, PolicyScope::Aws) {
            Vec::new()
        } else {
            state.policies.values().cloned().collect()
        };
        if matches!(scope, PolicyScope::All | PolicyScope::Aws) {
            for mp in crate::managed_policies::all() {
                policies.push(mp.to_iam_policy(count_managed_attachments(state, &mp.arn)));
            }
        }
        if let Some(prefix) = path_prefix {
            policies.retain(|p| p.path.starts_with(&prefix));
        }
        policies.sort_by(|a, b| a.policy_name.cmp(&b.policy_name));

        // Marker-based pagination: resume after the marked item (by ARN, the
        // stable cursor AWS uses for ListPolicies).
        let start_idx = marker
            .as_ref()
            .and_then(|m| policies.iter().position(|p| p.arn == *m).map(|p| p + 1))
            .unwrap_or(0);
        let page = policies.get(start_idx..).unwrap_or(&[]);
        let is_truncated = page.len() > max_items;
        let page = if is_truncated {
            &page[..max_items]
        } else {
            page
        };
        let next_marker = if is_truncated {
            page.last().map(|p| p.arn.clone())
        } else {
            None
        };

        let xml = xml_responses::list_policies_response(
            page,
            is_truncated,
            next_marker.as_deref(),
            &req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn tag_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let new_tags = parse_tags(&req.query_params);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

    pub(super) fn untag_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let tag_keys = parse_tag_keys(&req.query_params);
        validate_untag_keys(&tag_keys)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

    pub(super) fn list_policy_tags(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let policy = state.policies.get(&policy_arn).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} does not exist."),
            )
        })?;

        let xml = paginated_tags_response("ListPolicyTags", &policy.tags, req)?;
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

impl IamService {
    pub(super) fn create_policy_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let policy_document = required_param(&req.query_params, "PolicyDocument")?;
        let set_as_default = req
            .query_params
            .get("SetAsDefault")
            .map(|v| v == "true")
            .unwrap_or(false);

        // Validate policy document
        if let Err(msg) = validate_policy_document(&policy_document) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedPolicyDocument",
                msg,
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

    pub(super) fn get_policy_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let version_id = required_param(&req.query_params, "VersionId")?;
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let policy = resolve_policy(state, &policy_arn).ok_or_else(|| {
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

    pub(super) fn list_policy_versions(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let policy = resolve_policy(state, &policy_arn).ok_or_else(|| {
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

    pub(super) fn delete_policy_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let version_id = required_param(&req.query_params, "VersionId")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

    pub(super) fn set_default_policy_version(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Declared: InvalidInputException, LimitExceeded, NoSuchEntity,
        // ServiceFailure -> InvalidInput for all generic input validation.
        let policy_arn =
            super::required_param_with_code(&req.query_params, "PolicyArn", "InvalidInput")?;
        super::validate_string_length_with_code(
            "policyArn",
            &policy_arn,
            20,
            2048,
            "InvalidInput",
        )?;
        let version_id =
            super::required_param_with_code(&req.query_params, "VersionId", "InvalidInput")?;

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
                "InvalidInput",
                format!(
                    "Value '{}' at 'versionId' failed to satisfy constraint: Member must satisfy regular expression pattern: v[1-9][0-9]*(\\.[A-Za-z0-9-]*)?",
                    version_id
                ),
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

impl IamService {
    pub(super) fn list_entities_for_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;
        let entity_filter = req.query_params.get("EntityFilter").cloned();
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        if !state.policies.contains_key(&policy_arn)
            && crate::managed_policies::lookup(&policy_arn).is_none()
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} does not exist."),
            ));
        }

        let include_roles = matches!(
            entity_filter.as_deref(),
            None | Some("Role") | Some("LocalManagedPolicy") | Some("AWSManagedPolicy")
        );
        let include_users = matches!(
            entity_filter.as_deref(),
            None | Some("User") | Some("LocalManagedPolicy") | Some("AWSManagedPolicy")
        );
        let include_groups = matches!(
            entity_filter.as_deref(),
            None | Some("Group") | Some("LocalManagedPolicy") | Some("AWSManagedPolicy")
        );

        // Find roles attached to this policy
        let role_members: String = if !include_roles {
            String::new()
        } else {
            state
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
            .join("\n")
        };

        // Find users attached to this policy
        let user_members: String = if !include_users {
            String::new()
        } else {
            state
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
            .join("\n")
        };

        // Find groups attached to this policy
        let group_members: String = if !include_groups {
            String::new()
        } else {
            state
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
            .join("\n")
        };

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

/// Parsed + validated inputs for `CreatePolicy`.
struct CreatePolicyInput {
    policy_name: String,
    policy_document: String,
    path: String,
    description: String,
    tags: Vec<crate::state::Tag>,
}

impl CreatePolicyInput {
    fn from_query(
        params: &std::collections::HashMap<String, String>,
    ) -> Result<Self, AwsServiceError> {
        let policy_name = required_param(params, "PolicyName")?;
        validate_string_length("policyName", &policy_name, 1, 128)?;
        let policy_document = required_param(params, "PolicyDocument")?;
        let path = params
            .get("Path")
            .cloned()
            .unwrap_or_else(|| "/".to_string());
        let description = params.get("Description").cloned().unwrap_or_default();
        let tags = parse_tags(params);
        validate_tags(&tags, 0)?;
        if let Err(msg) = validate_policy_document(&policy_document) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedPolicyDocument",
                msg,
            ));
        }
        Ok(Self {
            policy_name,
            policy_document,
            path,
            description,
            tags,
        })
    }
}

/// Resolve a policy ARN to an owned [`IamPolicy`], looking first in the
/// account's customer-managed policies and then in the seeded AWS-managed
/// catalog. Returns `None` if neither holds it.
fn resolve_policy(state: &crate::state::IamState, arn: &str) -> Option<IamPolicy> {
    if let Some(p) = state.policies.get(arn) {
        return Some(p.clone());
    }
    crate::managed_policies::lookup(arn)
        .map(|mp| mp.to_iam_policy(count_managed_attachments(state, arn)))
}

/// Count how many roles, users, and groups in this account reference the given
/// policy ARN. AWS-managed policies are not stored in state, so their
/// `AttachmentCount` is derived on demand from the attachment maps.
fn count_managed_attachments(state: &crate::state::IamState, arn: &str) -> u32 {
    let mut count = 0u32;
    for arns in state.role_policies.values() {
        if arns.iter().any(|a| a == arn) {
            count += 1;
        }
    }
    for arns in state.user_policies.values() {
        if arns.iter().any(|a| a == arn) {
            count += 1;
        }
    }
    for group in state.groups.values() {
        if group.attached_policies.iter().any(|a| a == arn) {
            count += 1;
        }
    }
    count
}
