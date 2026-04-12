use chrono::Utc;
use http::StatusCode;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{IamRole, ServiceLinkedRoleDeletion};
use crate::xml_responses;

use super::{
    empty_response, generate_id, paginated_tags_response, parse_tag_keys, parse_tags,
    partition_for_region, required_param, title_case_service, url_encode, validate_tags,
    validate_untag_keys, IamService,
};

use fakecloud_aws::xml::xml_escape;

use crate::policy_validation::validate_policy_document;

impl IamService {
    pub(super) fn create_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        validate_string_length("roleName", &role_name, 1, 64)?;
        let assume_role_policy = required_param(&req.query_params, "AssumeRolePolicyDocument")?;
        let path = req
            .query_params
            .get("Path")
            .cloned()
            .unwrap_or_else(|| "/".to_string());
        let description = req.query_params.get("Description").cloned();
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

    pub(super) fn get_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        validate_string_length("roleName", &role_name, 1, 64)?;
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

    pub(super) fn delete_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        validate_string_length("roleName", &role_name, 1, 64)?;
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

    pub(super) fn list_roles(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_optional_string_length(
            "marker",
            req.query_params.get("Marker").map(|s| s.as_str()),
            1,
            320,
        )?;
        validate_optional_string_length(
            "pathPrefix",
            req.query_params.get("PathPrefix").map(|s| s.as_str()),
            1,
            512,
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
        let state = self.state.read();
        let path_prefix = req.query_params.get("PathPrefix").cloned();
        let max_items: usize = req
            .query_params
            .get("MaxItems")
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);
        let marker = req.query_params.get("Marker").cloned();

        let mut roles: Vec<IamRole> = state.roles.values().cloned().collect();
        if let Some(prefix) = path_prefix {
            roles.retain(|r| r.path.starts_with(&prefix));
        }
        roles.sort_by(|a, b| a.role_name.cmp(&b.role_name));

        // Apply marker-based pagination (start after the marker item)
        let start_idx = if let Some(ref m) = marker {
            roles
                .iter()
                .position(|r| r.role_name == *m)
                .map(|pos| pos + 1)
                .unwrap_or(0)
        } else {
            0
        };

        let page = &roles[start_idx..];
        let is_truncated = page.len() > max_items;
        let page = if is_truncated {
            &page[..max_items]
        } else {
            page
        };
        let next_marker = if is_truncated {
            Some(page.last().map(|r| r.role_name.clone()).unwrap_or_default())
        } else {
            None
        };

        let xml = xml_responses::list_roles_response_paginated(
            page,
            is_truncated,
            next_marker.as_deref(),
            &req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn update_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        validate_string_length("roleName", &role_name, 1, 64)?;
        let mut state = self.state.write();

        let role = state.roles.get_mut(&role_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            )
        })?;

        // UpdateRole: if Description is provided, set it; if absent, clear it
        if let Some(desc) = req.query_params.get("Description") {
            role.description = Some(desc.clone());
        } else {
            role.description = None;
        }
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

    pub(super) fn update_role_description(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        validate_string_length("roleName", &role_name, 1, 64)?;
        let mut state = self.state.write();

        let role = state.roles.get_mut(&role_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            )
        })?;

        if let Some(desc) = req.query_params.get("Description") {
            role.description = Some(desc.clone());
        }

        let role_clone = role.clone();
        let xml = xml_responses::get_role_response(&role_clone, &req.request_id)
            .replace("GetRoleResponse", "UpdateRoleDescriptionResponse")
            .replace("GetRoleResult", "UpdateRoleDescriptionResult");
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn update_assume_role_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        validate_string_length("roleName", &role_name, 1, 64)?;
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

    pub(super) fn tag_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        validate_string_length("roleName", &role_name, 1, 64)?;
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

    pub(super) fn untag_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        validate_string_length("roleName", &role_name, 1, 64)?;
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

    pub(super) fn list_role_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        validate_string_length("roleName", &role_name, 1, 64)?;
        let state = self.state.read();

        let role = state.roles.get(&role_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            )
        })?;

        let xml = paginated_tags_response("ListRoleTags", &role.tags, req)?;
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn put_role_permissions_boundary(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        validate_string_length("roleName", &role_name, 1, 64)?;
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

    pub(super) fn delete_role_permissions_boundary(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        validate_string_length("roleName", &role_name, 1, 64)?;
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

impl IamService {
    pub(super) fn attach_role_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn detach_role_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn list_attached_role_policies(
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
    pub(super) fn put_role_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        let policy_name = required_param(&req.query_params, "PolicyName")?;
        let policy_document = required_param(&req.query_params, "PolicyDocument")?;

        // Validate policy document
        if let Err(msg) = validate_policy_document(&policy_document) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedPolicyDocument",
                msg,
            ));
        }

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

    pub(super) fn get_role_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn delete_role_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn list_role_policies(
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

        let policy_names: Vec<String> = state
            .role_inline_policies
            .get(&role_name)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();

        let xml = xml_responses::list_role_policies_response(&policy_names, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

impl IamService {
    pub(super) fn create_service_linked_role(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let aws_service_name = required_param(&req.query_params, "AWSServiceName")?;
        let description = req.query_params.get("Description").cloned();
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

                let service_cased = title_case_service(service);
                let prefix_cased = title_case_service(prefix);

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

    pub(super) fn delete_service_linked_role(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn get_service_linked_role_deletion_status(
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
