use chrono::Utc;
use http::StatusCode;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::IamGroup;

use super::{empty_response, generate_id, url_encode, validate_list_pagination, IamService};
use fakecloud_core::query::required_param;

use fakecloud_aws::xml::xml_escape;

// ========= Group operations =========

impl IamService {
    pub(super) fn create_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        validate_string_length("groupName", &group_name, 1, 128)?;
        let path = req
            .query_params
            .get("Path")
            .cloned()
            .unwrap_or_else(|| "/".to_string());

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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
            inline_policies: std::collections::BTreeMap::new(),
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

    pub(super) fn get_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        validate_string_length("groupName", &group_name, 1, 128)?;
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let group = state.groups.get(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Group {group_name} not found"),
            )
        })?;

        // Marker/MaxItems pagination over the group's members (the cursor is
        // the member user name). Previously both were ignored and IsTruncated
        // was hardcoded false, so large groups never paged.
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
        let max_items: usize = req
            .query_params
            .get("MaxItems")
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);
        let marker = req.query_params.get("Marker").cloned();

        // Resolve members (skipping any whose user record vanished) preserving
        // membership order, then page after the marker.
        let resolved: Vec<&crate::state::IamUser> = group
            .members
            .iter()
            .filter_map(|uname| state.users.get(uname))
            .collect();
        let start_idx = marker
            .as_ref()
            .and_then(|m| {
                resolved
                    .iter()
                    .position(|u| u.user_name == *m)
                    .map(|p| p + 1)
            })
            .unwrap_or(0);
        let rest = resolved.get(start_idx..).unwrap_or(&[]);
        let is_truncated = rest.len() > max_items;
        let page = if is_truncated {
            &rest[..max_items]
        } else {
            rest
        };
        let next_marker = if is_truncated {
            page.last().map(|u| u.user_name.clone())
        } else {
            None
        };

        let user_members: String = page
            .iter()
            .map(|u| {
                format!(
                    "      <member>\n        <Path>{}</Path>\n        <UserName>{}</UserName>\n        <UserId>{}</UserId>\n        <Arn>{}</Arn>\n        <CreateDate>{}</CreateDate>\n      </member>",
                    u.path, u.user_name, u.user_id, u.arn, u.created_at.format("%Y-%m-%dT%H:%M:%SZ")
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let marker_xml = next_marker
            .as_deref()
            .map(|m| format!("    <Marker>{m}</Marker>\n"))
            .unwrap_or_default();

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
    <IsTruncated>{is_truncated}</IsTruncated>
{marker_xml}    <Users>
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

    pub(super) fn delete_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        validate_string_length("groupName", &group_name, 1, 128)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        // AWS rejects DeleteGroup with a 409 DeleteConflict while the group
        // still has members or attached/inline policies — it does NOT silently
        // destroy the group and its membership. Guard before removing.
        let (has_members, has_attached, has_inline) = {
            let group = state.groups.get(&group_name).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchEntity",
                    format!("The group with name {group_name} cannot be found."),
                )
            })?;
            (
                !group.members.is_empty(),
                !group.attached_policies.is_empty(),
                !group.inline_policies.is_empty(),
            )
        };
        if has_members {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "DeleteConflict",
                "Cannot delete entity, must remove users from the group first.".to_string(),
            ));
        }
        if has_attached {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "DeleteConflict",
                "Cannot delete entity, must detach all policies first.".to_string(),
            ));
        }
        if has_inline {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "DeleteConflict",
                "Cannot delete entity, must delete policies first.".to_string(),
            ));
        }

        state.groups.remove(&group_name);
        let xml = empty_response("DeleteGroup", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn list_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let max_items = validate_list_pagination(req)? as usize;
        let marker = req.query_params.get("Marker").cloned();
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let path_prefix = req.query_params.get("PathPrefix").cloned();
        let mut groups: Vec<&IamGroup> = state.groups.values().collect();
        if let Some(prefix) = &path_prefix {
            groups.retain(|g| g.path.starts_with(prefix));
        }
        groups.sort_by(|a, b| a.group_name.cmp(&b.group_name));

        // Marker-based pagination: resume after the marked item.
        let start_idx = marker
            .as_ref()
            .and_then(|m| {
                groups
                    .iter()
                    .position(|g| g.group_name == *m)
                    .map(|p| p + 1)
            })
            .unwrap_or(0);
        let page = groups.get(start_idx..).unwrap_or(&[]);
        let is_truncated = page.len() > max_items;
        let page = if is_truncated {
            &page[..max_items]
        } else {
            page
        };
        let next_marker = if is_truncated {
            page.last().map(|g| g.group_name.clone())
        } else {
            None
        };

        let members: String = page
            .iter()
            .map(|g| {
                format!(
                    "      <member>\n        <Path>{}</Path>\n        <GroupName>{}</GroupName>\n        <GroupId>{}</GroupId>\n        <Arn>{}</Arn>\n        <CreateDate>{}</CreateDate>\n      </member>",
                    g.path, g.group_name, g.group_id, g.arn, g.created_at.format("%Y-%m-%dT%H:%M:%SZ")
                )
            })
            .collect::<Vec<_>>()
            .join("\n");

        let marker_section = match next_marker {
            Some(m) => format!("\n    <Marker>{m}</Marker>"),
            None => String::new(),
        };

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListGroupsResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListGroupsResult>
    <IsTruncated>{is_truncated}</IsTruncated>{marker_section}
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

    pub(super) fn update_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let new_group_name = req.query_params.get("NewGroupName").cloned();
        let new_path = req.query_params.get("NewPath").cloned();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

    pub(super) fn add_user_to_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let user_name = required_param(&req.query_params, "UserName")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

    pub(super) fn remove_user_from_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let user_name = required_param(&req.query_params, "UserName")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

    pub(super) fn list_groups_for_user(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

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
    pub(super) fn put_group_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let policy_name = required_param(&req.query_params, "PolicyName")?;
        let policy_document = required_param(&req.query_params, "PolicyDocument")?;

        // Validate policy document
        if let Err(msg) = crate::policy_validation::validate_policy_document(&policy_document) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedPolicyDocument",
                msg,
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

    pub(super) fn get_group_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let policy_name = required_param(&req.query_params, "PolicyName")?;
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

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

    pub(super) fn delete_group_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let policy_name = required_param(&req.query_params, "PolicyName")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

    pub(super) fn list_group_policies(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let group = state.groups.get(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The group with name {group_name} cannot be found."),
            )
        })?;

        let policy_names: Vec<String> = group.inline_policies.keys().cloned().collect();
        let (members, is_truncated, next_marker) = super::paginate_policy_names(policy_names, req);
        let marker_section = next_marker
            .map(|m| format!("\n    <Marker>{m}</Marker>"))
            .unwrap_or_default();
        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListGroupPoliciesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListGroupPoliciesResult>
    <IsTruncated>{is_truncated}</IsTruncated>{marker_section}
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

    pub(super) fn attach_group_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if !state.groups.contains_key(&group_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The group with name {group_name} cannot be found."),
            ));
        }

        // Check the policy exists. Mirrors attach_role_policy / attach_user_policy:
        // an AWS-managed ARN must resolve in the managed-policy catalog (a bogus
        // `arn:aws:iam::aws:policy/DoesNotExist` is NoSuchEntity, not silently
        // accepted); a customer-managed ARN must exist in state.
        let policy_exists = if policy_arn.contains(":aws:policy/") {
            crate::managed_policies::lookup(&policy_arn).is_some()
        } else {
            state.policies.contains_key(&policy_arn)
        };
        if !policy_exists {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Policy {policy_arn} does not exist or is not attachable."),
            ));
        }

        let group = state
            .groups
            .get_mut(&group_name)
            .expect("group presence checked above");
        if !group.attached_policies.contains(&policy_arn) {
            group.attached_policies.push(policy_arn.clone());
            if let Some(p) = state.policies.get_mut(&policy_arn) {
                p.attachment_count += 1;
            }
        }

        let xml = empty_response("AttachGroupPolicy", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn detach_group_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let policy_arn = required_param(&req.query_params, "PolicyArn")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

    pub(super) fn list_attached_group_policies(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let group = state.groups.get(&group_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The group with name {group_name} cannot be found."),
            )
        })?;

        let (members, is_truncated, next_marker) =
            super::paginate_attached_policies(state, &group.attached_policies, req);
        let marker_section = next_marker
            .map(|m| format!("\n    <Marker>{m}</Marker>"))
            .unwrap_or_default();

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAttachedGroupPoliciesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListAttachedGroupPoliciesResult>
    <IsTruncated>{is_truncated}</IsTruncated>{marker_section}
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
