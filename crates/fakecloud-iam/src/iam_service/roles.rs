use chrono::Utc;
use http::StatusCode;

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{IamRole, IamState, ServiceLinkedRoleDeletion};
use crate::xml_responses;

use super::{
    empty_response, generate_id, paginated_tags_response, parse_tag_keys, parse_tags,
    partition_for_region, title_case_service, url_encode, validate_tags, validate_untag_keys,
    IamService,
};
use fakecloud_core::query::required_param;

use fakecloud_aws::xml::xml_escape;

use crate::policy_validation::validate_policy_document;

/// Reject deletion of a role that's still attached to instance profiles or
/// has any kind of policy on it. Mirrors AWS's per-dependency
/// `DeleteConflict` messages.
fn ensure_role_can_be_deleted(state: &IamState, role_name: &str) -> Result<(), AwsServiceError> {
    if state
        .instance_profiles
        .values()
        .any(|ip| ip.roles.contains(&role_name.to_string()))
    {
        return Err(AwsServiceError::aws_error(
            StatusCode::CONFLICT,
            "DeleteConflict",
            "Cannot delete entity, must remove roles from instance profile first.".to_string(),
        ));
    }

    if state
        .role_policies
        .get(role_name)
        .is_some_and(|p| !p.is_empty())
    {
        return Err(AwsServiceError::aws_error(
            StatusCode::CONFLICT,
            "DeleteConflict",
            "Cannot delete entity, must detach all policies first.".to_string(),
        ));
    }

    if state
        .role_inline_policies
        .get(role_name)
        .is_some_and(|p| !p.is_empty())
    {
        return Err(AwsServiceError::aws_error(
            StatusCode::CONFLICT,
            "DeleteConflict",
            "Cannot delete entity, must delete policies first.".to_string(),
        ));
    }

    Ok(())
}

/// Compute the service-linked-role name for a given service principal.
///
/// AWS has a fixed naming scheme: `AWSServiceRoleFor{Suffix}` (with an
/// optional `_{custom}` tail) where the suffix is derived from the part
/// of the service principal before `.amazonaws.com`. A handful of
/// services have non-derivable casing that we hard-code, and the
/// `<prefix>.<service>` form (e.g. `custom-resource.application-autoscaling`)
/// produces a `{Service}_{Prefix}` suffix.
fn derive_service_linked_role_name(aws_service_name: &str, custom_suffix: Option<&str>) -> String {
    let service_part = aws_service_name
        .strip_suffix(".amazonaws.com")
        .unwrap_or(aws_service_name);

    let role_suffix = match service_part {
        "autoscaling" => "AutoScaling".to_string(),
        "elasticbeanstalk" => "ElasticBeanstalk".to_string(),
        "elasticloadbalancing" => "ElasticLoadBalancing".to_string(),
        "elasticmapreduce" => "ElasticMapReduce".to_string(),
        // Documented service-linked-role suffixes that don't follow the
        // title-case-the-principal rule. AWS mints `AWSServiceRoleFor<Suffix>`
        // with these exact spellings; deriving them mechanically would produce
        // wrong names (e.g. `inspector` -> `AmazonInspector`).
        "inspector" => "AmazonInspector".to_string(),
        "inspector2" => "AmazonInspector2".to_string(),
        "ec2" | "ec2-instance-connect" => "EC2InstanceConnect".to_string(),
        "ssm" => "AmazonSSM".to_string(),
        "rds" => "RDS".to_string(),
        "elasticache" => "ElastiCache".to_string(),
        "ecs" => "ECS".to_string(),
        "eks" => "AmazonEKS".to_string(),
        " eks-nodegroup" | "eks-nodegroup" => "AmazonEKSNodegroup".to_string(),
        "opensearchservice" => "AmazonOpenSearchService".to_string(),
        "es" => "AmazonElasticsearchService".to_string(),
        "guardduty" => "AmazonGuardDuty".to_string(),
        "macie" => "AmazonMacie".to_string(),
        "config" => "Config".to_string(),
        "cloudtrail" => "CloudTrail".to_string(),
        "organizations" => "Organizations".to_string(),
        "sso" => "SSO".to_string(),
        "kafka" => "Kafka".to_string(),
        "globalaccelerator" => "GlobalAccelerator".to_string(),
        "dynamodb" => "DynamoDBReplication".to_string(),
        "lakeformation" => "LakeFormationDataAccess".to_string(),
        "s3" => "S3StorageLens".to_string(),
        s if s.contains('.') => {
            let parts: Vec<&str> = s.splitn(2, '.').collect();
            let prefix = parts[0];
            let service = parts[1];
            let service_cased = title_case_service(service);
            let prefix_cased = title_case_service(prefix);
            format!("{}_{}", service_cased, prefix_cased)
        }
        other => other.to_string(),
    };

    match custom_suffix {
        Some(suffix) => format!("AWSServiceRoleFor{}_{}", role_suffix, suffix),
        None => format!("AWSServiceRoleFor{}", role_suffix),
    }
}

impl IamService {
    pub(super) fn create_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let input = CreateRoleInput::from_query(&req.query_params)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if state.roles.contains_key(&input.role_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                format!("Role with name {} already exists.", input.role_name),
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
                if input.path == "/" { "/" } else { &input.path },
                input.role_name
            ),
            role_name: input.role_name.clone(),
            path: input.path,
            assume_role_policy_document: input.assume_role_policy,
            created_at: Utc::now(),
            description: input.description,
            max_session_duration: input.max_session_duration,
            tags: input.tags,
            permissions_boundary: input.permissions_boundary,
        };

        let xml = xml_responses::create_role_response(&role, &req.request_id);
        state.roles.insert(input.role_name, role);

        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn get_role(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let role_name = required_param(&req.query_params, "RoleName")?;
        validate_string_length("roleName", &role_name, 1, 64)?;
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

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
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if !state.roles.contains_key(&role_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            ));
        }

        ensure_role_can_be_deleted(state, &role_name)?;

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
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
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
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let role = state.roles.get_mut(&role_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Role {role_name} not found"),
            )
        })?;

        // UpdateRole: Description and MaxSessionDuration are independent
        // optionals. Only update Description when it's actually supplied;
        // omitting it leaves the existing description unchanged (AWS does not
        // clear it just because another field was updated).
        if let Some(desc) = req.query_params.get("Description") {
            role.description = Some(desc.clone());
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
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

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
        // Declared: InvalidInputException, NoSuchEntityException,
        // PolicyNotAttachableException, ServiceFailureException,
        // UnmodifiableEntityException -> InvalidInput for bad input shape.
        let role_name =
            super::required_param_with_code(&req.query_params, "RoleName", "InvalidInput")?;
        super::validate_string_length_with_code("roleName", &role_name, 1, 64, "InvalidInput")?;
        let boundary = super::required_param_with_code(
            &req.query_params,
            "PermissionsBoundary",
            "InvalidInput",
        )?;
        super::validate_string_length_with_code(
            "permissionsBoundary",
            &boundary,
            20,
            2048,
            "InvalidInput",
        )?;

        if boundary
            .parse::<Arn>()
            .ok()
            .filter(|arn| arn.service == "iam" && arn.resource.starts_with("policy/"))
            .is_none()
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidInput",
                format!("Value ({boundary}) for parameter PermissionsBoundary is invalid."),
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

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

        let (members, is_truncated, next_marker) =
            super::paginate_attached_policies(state, &policy_arns, req);
        let marker_section = next_marker
            .map(|m| format!("\n    <Marker>{m}</Marker>"))
            .unwrap_or_default();

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAttachedRolePoliciesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListAttachedRolePoliciesResult>
    <IsTruncated>{is_truncated}</IsTruncated>{marker_section}
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

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

        let (members, is_truncated, next_marker) = super::paginate_policy_names(policy_names, req);
        let marker_section = next_marker
            .map(|m| format!("\n    <Marker>{m}</Marker>"))
            .unwrap_or_default();
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListRolePoliciesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListRolePoliciesResult>
    <IsTruncated>{is_truncated}</IsTruncated>{marker_section}
    <PolicyNames>
{members}
    </PolicyNames>
  </ListRolePoliciesResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListRolePoliciesResponse>"#,
            req.request_id
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

impl IamService {
    pub(super) fn create_service_linked_role(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let aws_service_name = required_param(&req.query_params, "AWSServiceName")?;
        super::validate_string_length_with_code(
            "AWSServiceName",
            &aws_service_name,
            1,
            128,
            "InvalidInput",
        )?;
        let description = req.query_params.get("Description").cloned();
        let custom_suffix = req.query_params.get("CustomSuffix").cloned();
        super::validate_optional_string_length_with_code(
            "CustomSuffix",
            custom_suffix.as_deref(),
            1,
            64,
            "InvalidInput",
        )?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let role_name =
            derive_service_linked_role_name(&aws_service_name, custom_suffix.as_deref());
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
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

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

/// Parsed + validated inputs for `CreateRole`.
struct CreateRoleInput {
    role_name: String,
    assume_role_policy: String,
    path: String,
    description: Option<String>,
    max_session_duration: i32,
    tags: Vec<crate::state::Tag>,
    permissions_boundary: Option<String>,
}

impl CreateRoleInput {
    fn from_query(
        params: &std::collections::HashMap<String, String>,
    ) -> Result<Self, AwsServiceError> {
        // CreateRole's Smithy-declared errors: ConcurrentModification,
        // EntityAlreadyExists, InvalidInputException, LimitExceeded,
        // MalformedPolicyDocument, ServiceFailure -> InvalidInput for all
        // generic input validation; MalformedPolicyDocument is reserved for
        // policy-document parse failures handled downstream.
        let role_name = super::required_param_with_code(params, "RoleName", "InvalidInput")?;
        super::validate_string_length_with_code("roleName", &role_name, 1, 64, "InvalidInput")?;
        let assume_role_policy =
            super::required_param_with_code(params, "AssumeRolePolicyDocument", "InvalidInput")?;
        let path = params
            .get("Path")
            .cloned()
            .unwrap_or_else(|| "/".to_string());
        let description = params.get("Description").cloned();
        let max_session_duration = params
            .get("MaxSessionDuration")
            .and_then(|v| v.parse().ok())
            .unwrap_or(3600);
        let tags = parse_tags(params);
        validate_tags(&tags, 0)?;
        let permissions_boundary = params.get("PermissionsBoundary").cloned();
        if let Some(ref boundary) = permissions_boundary {
            super::validate_string_length_with_code(
                "permissionsBoundary",
                boundary,
                20,
                2048,
                "InvalidInput",
            )?;
            if !boundary.contains(":policy/") {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidInput",
                    format!("Value ({boundary}) for parameter PermissionsBoundary is invalid."),
                ));
            }
        }
        Ok(Self {
            role_name,
            assume_role_policy,
            path,
            description,
            max_session_duration,
            tags,
            permissions_boundary,
        })
    }
}
