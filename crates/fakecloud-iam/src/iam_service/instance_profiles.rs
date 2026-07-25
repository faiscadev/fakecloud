use chrono::Utc;
use http::StatusCode;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::IamInstanceProfile;

use super::{
    empty_response, generate_id, paginated_tags_response, parse_tag_keys, parse_tags,
    partition_for_region, tags_xml, url_encode, validate_tags, validate_untag_keys, IamService,
};
use fakecloud_core::query::required_param;

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
        validate_tags(&tags, 0)?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if state.instance_profiles.contains_key(&name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "EntityAlreadyExists",
                format!("Instance Profile {name} already exists."),
            ));
        }

        let partition = partition_for_region(&req.region);
        let ip = IamInstanceProfile {
            instance_profile_id: format!("AIPA{}", generate_id()),
            arn: format!(
                "arn:{}:iam::{}:instance-profile{}{}",
                partition,
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

        let xml = self.instance_profile_xml("CreateInstanceProfile", &ip, state, &req.request_id);
        state.instance_profiles.insert(name, ip);

        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn get_instance_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "InstanceProfileName")?;
        validate_string_length("instanceProfileName", &name, 1, 128)?;
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let ip = state.instance_profiles.get(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Instance Profile {name} not found"),
            )
        })?;

        let xml = self.instance_profile_xml("GetInstanceProfile", ip, state, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn delete_instance_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let name = required_param(&req.query_params, "InstanceProfileName")?;
        validate_string_length("instanceProfileName", &name, 1, 128)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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
        let max_items = super::validate_list_pagination(req)? as usize;
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let path_prefix = req.query_params.get("PathPrefix").cloned();

        let mut profiles: Vec<&IamInstanceProfile> = state
            .instance_profiles
            .values()
            .filter(|ip| {
                path_prefix
                    .as_ref()
                    .map(|p| ip.path.starts_with(p))
                    .unwrap_or(true)
            })
            .collect();
        profiles.sort_by(|a, b| a.instance_profile_name.cmp(&b.instance_profile_name));

        let (page, is_truncated, next_marker) =
            paginate_by_name(&profiles, req, |ip| &ip.instance_profile_name, max_items);

        let members: String = page
            .iter()
            .map(|ip| self.instance_profile_member_xml(ip, state))
            .collect::<Vec<_>>()
            .join("\n");
        let marker_section = match &next_marker {
            Some(m) => format!("\n    <Marker>{m}</Marker>"),
            None => String::new(),
        };

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListInstanceProfilesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListInstanceProfilesResult>
    <IsTruncated>{is_truncated}</IsTruncated>{marker_section}
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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
        let max_items = super::validate_list_pagination(req)? as usize;
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

        let mut profiles: Vec<&IamInstanceProfile> = state
            .instance_profiles
            .values()
            .filter(|ip| ip.roles.contains(&role_name))
            .collect();
        profiles.sort_by(|a, b| a.instance_profile_name.cmp(&b.instance_profile_name));

        let (page, is_truncated, next_marker) =
            paginate_by_name(&profiles, req, |ip| &ip.instance_profile_name, max_items);

        let members: String = page
            .iter()
            .map(|ip| self.instance_profile_member_xml(ip, state))
            .collect::<Vec<_>>()
            .join("\n");
        let marker_section = match &next_marker {
            Some(m) => format!("\n    <Marker>{m}</Marker>"),
            None => String::new(),
        };

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListInstanceProfilesForRoleResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListInstanceProfilesForRoleResult>
    <IsTruncated>{is_truncated}</IsTruncated>{marker_section}
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
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let ip = state.instance_profiles.get_mut(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Instance Profile {name} not found"),
            )
        })?;

        // Enforce the 50-tag ceiling against the post-merge total (mirrors
        // TagRole/TagPolicy).
        let existing_count = ip
            .tags
            .iter()
            .filter(|t| !new_tags.iter().any(|nt| nt.key == t.key))
            .count();
        validate_tags(&new_tags, existing_count)?;

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
        validate_untag_keys(&tag_keys)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

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
        let accounts = self.state.read();
        let empty = crate::state::IamState::new(&req.account_id);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let ip = state.instance_profiles.get(&name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("Instance Profile {name} not found"),
            )
        })?;

        let xml = paginated_tags_response("ListInstanceProfileTags", &ip.tags, req)?;
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

/// Apply the IAM `Marker`/`MaxItems` pagination triad to an already-sorted
/// slice of items, using each item's name (via `name_of`) as the opaque
/// cursor. Returns the page, the truncation flag, and the next `Marker`.
/// Shared by the ListInstanceProfiles family, which previously returned every
/// item with a hardcoded `IsTruncated=false`.
fn paginate_by_name<'a, T, F>(
    items: &[&'a T],
    req: &AwsRequest,
    name_of: F,
    max_items: usize,
) -> (Vec<&'a T>, bool, Option<String>)
where
    F: Fn(&T) -> &str,
{
    let marker = req.query_params.get("Marker").cloned();
    // Resume at the first item whose name is strictly greater than the marker (the
    // last name of the previous page). items is already sorted by name, so a
    // deleted marker still advances instead of falling back to 0 and restarting.
    let start = marker
        .as_ref()
        .map(|m| {
            items
                .iter()
                .position(|it| name_of(it) > m.as_str())
                .unwrap_or(items.len())
        })
        .unwrap_or(0)
        .min(items.len());
    let rest = &items[start..];
    let is_truncated = rest.len() > max_items;
    let page: Vec<&'a T> = if is_truncated {
        rest[..max_items].to_vec()
    } else {
        rest.to_vec()
    };
    let next_marker = if is_truncated {
        page.last().map(|it| name_of(it).to_string())
    } else {
        None
    };
    (page, is_truncated, next_marker)
}
