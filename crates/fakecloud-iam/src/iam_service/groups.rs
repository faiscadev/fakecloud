use chrono::Utc;
use http::StatusCode;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::IamGroup;

use super::{empty_response, generate_id, required_param, url_encode, IamService};

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

    pub(super) fn get_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        validate_string_length("groupName", &group_name, 1, 128)?;
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

    pub(super) fn delete_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let group_name = required_param(&req.query_params, "GroupName")?;
        validate_string_length("groupName", &group_name, 1, 128)?;
        let mut state = self.state.write();

        if state.groups.remove(&group_name).is_none() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The group with name {group_name} cannot be found."),
            ));
        }

        let xml = empty_response("DeleteGroup", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn list_groups(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn update_group(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn add_user_to_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn remove_user_from_group(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn list_groups_for_user(
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

    pub(super) fn get_group_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn delete_group_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn list_group_policies(
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

    pub(super) fn attach_group_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn detach_group_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn list_attached_group_policies(
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
