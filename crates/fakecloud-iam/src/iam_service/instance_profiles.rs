use chrono::Utc;
use http::StatusCode;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::IamInstanceProfile;

use super::{
    empty_response, generate_id, parse_tag_keys, parse_tags, required_param, tags_xml, url_encode,
    IamService,
};

impl IamService {
    pub(super) fn create_instance_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "InstanceProfileName")?;
        validate_string_length("instanceProfileName", &name, 1, 128)?;
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

    pub(super) fn get_instance_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "InstanceProfileName")?;
        validate_string_length("instanceProfileName", &name, 1, 128)?;
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

    pub(super) fn delete_instance_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "InstanceProfileName")?;
        validate_string_length("instanceProfileName", &name, 1, 128)?;
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

    pub(super) fn list_instance_profiles(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn add_role_to_instance_profile(
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

    pub(super) fn remove_role_from_instance_profile(
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

    pub(super) fn list_instance_profiles_for_role(
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

    pub(super) fn tag_instance_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn untag_instance_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn list_instance_profile_tags(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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
    pub(super) fn instance_profile_xml(
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

    pub(super) fn instance_profile_member_xml(
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

    pub(super) fn roles_xml_for_instance_profile(
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
