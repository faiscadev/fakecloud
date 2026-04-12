use chrono::Utc;
use http::StatusCode;

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{IamAccessKey, IamUser, SigningCertificate, SshPublicKey};
use crate::xml_responses;

use super::{
    empty_response, extract_access_key, generate_id, generate_long_id, parse_tag_keys, parse_tags,
    partition_for_region, required_param, resolve_calling_user, tags_xml, url_encode, IamService,
};

use fakecloud_aws::xml::xml_escape;

// ========= User operations =========

impl IamService {
    /// Determine the effective account ID for this request.
    /// If the caller has assumed a role into a different account, use that account ID.
    /// MUST be called before acquiring a write lock on self.state.
    pub(super) fn effective_account_id(&self, req: &AwsRequest) -> String {
        if let Some(access_key) = extract_access_key(req) {
            let state = self.state.read();
            if let Some(identity) = state.credential_identities.get(&access_key) {
                return identity.account_id.clone();
            }
        }
        self.state.read().account_id.clone()
    }

    pub(super) fn create_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        validate_string_length("userName", &user_name, 1, 64)?;
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

    pub(super) fn get_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_optional_string_length(
            "userName",
            req.query_params.get("UserName").map(|s| s.as_str()),
            1,
            128,
        )?;
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

    pub(super) fn delete_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        validate_string_length("userName", &user_name, 1, 64)?;
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

    pub(super) fn list_users(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

        let state = self.state.read();
        let path_prefix = req.query_params.get("PathPrefix").cloned();
        let mut users: Vec<IamUser> = state.users.values().cloned().collect();
        if let Some(prefix) = path_prefix {
            users.retain(|u| u.path.starts_with(&prefix));
        }
        let xml = xml_responses::list_users_response(&users, &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn update_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        validate_string_length("userName", &user_name, 1, 64)?;
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

    pub(super) fn tag_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        validate_string_length("userName", &user_name, 1, 64)?;
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

    pub(super) fn untag_user(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        validate_string_length("userName", &user_name, 1, 64)?;
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

    pub(super) fn list_user_tags(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        validate_string_length("userName", &user_name, 1, 64)?;
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
    pub(super) fn create_access_key(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_optional_string_length(
            "userName",
            req.query_params.get("UserName").map(|s| s.as_str()),
            1,
            128,
        )?;
        let mut state = self.state.write();

        // UserName is optional; if not specified, infer from the caller's access key
        let user_name = match req.query_params.get("UserName") {
            Some(name) => name.clone(),
            None => {
                // Look up user by access key ID from the request credentials
                let access_key_id = req.access_key_id.as_deref().unwrap_or("");
                state
                    .access_keys
                    .iter()
                    .find_map(|(user, keys)| {
                        if keys.iter().any(|k| k.access_key_id == access_key_id) {
                            Some(user.clone())
                        } else {
                            None
                        }
                    })
                    .ok_or_else(|| {
                        AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "MissingParameter",
                            "The request must contain the parameter UserName".to_string(),
                        )
                    })?
            }
        };

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        // Check access key limit (max 2 per user)
        let existing_count = state
            .access_keys
            .get(&user_name)
            .map(|keys| keys.len())
            .unwrap_or(0);
        if existing_count >= 2 {
            return Err(AwsServiceError::aws_error(
                StatusCode::CONFLICT,
                "LimitExceeded",
                "Cannot exceed quota for AccessKeysPerUser: 2".to_string(),
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

    pub(super) fn delete_access_key(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_optional_string_length(
            "userName",
            req.query_params.get("UserName").map(|s| s.as_str()),
            1,
            128,
        )?;
        let user_name = req
            .query_params
            .get("UserName")
            .cloned()
            .unwrap_or_else(|| resolve_calling_user(&self.state.read(), &req.account_id));
        let access_key_id = required_param(&req.query_params, "AccessKeyId")?;
        validate_string_length("accessKeyId", &access_key_id, 16, 128)?;
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

    pub(super) fn list_access_keys(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_optional_string_length(
            "userName",
            req.query_params.get("UserName").map(|s| s.as_str()),
            1,
            128,
        )?;
        validate_optional_string_length(
            "marker",
            req.query_params.get("Marker").map(|s| s.as_str()),
            1,
            320,
        )?;
        let user_name = req
            .query_params
            .get("UserName")
            .cloned()
            .unwrap_or_else(|| resolve_calling_user(&self.state.read(), &req.account_id));
        let marker = req.query_params.get("Marker").cloned();
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

        let state = self.state.read();
        let mut keys = state
            .access_keys
            .get(&user_name)
            .cloned()
            .unwrap_or_default();
        keys.sort_by(|a, b| a.access_key_id.cmp(&b.access_key_id));

        // Apply marker-based pagination (start after the marker item)
        let start_idx = if let Some(ref m) = marker {
            keys.iter()
                .position(|k| k.access_key_id == *m)
                .map(|pos| pos + 1)
                .unwrap_or(0)
        } else {
            0
        };

        let page = &keys[start_idx..];
        let is_truncated = page.len() > max_items;
        let page = if is_truncated {
            &page[..max_items]
        } else {
            page
        };
        let next_marker = if is_truncated {
            page.last().map(|k| k.access_key_id.clone())
        } else {
            None
        };

        let xml = xml_responses::list_access_keys_response(
            page,
            &user_name,
            is_truncated,
            next_marker.as_deref(),
            &req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn update_access_key(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

// ========= Login Profile operations =========

impl IamService {
    pub(super) fn create_login_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

        let profile = crate::state::LoginProfile {
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

    pub(super) fn get_login_profile(
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

    pub(super) fn update_login_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn delete_login_profile(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

// ========= Signing Certificate operations =========

impl IamService {
    pub(super) fn upload_signing_certificate(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let certificate_body = required_param(&req.query_params, "CertificateBody")?;

        // Validate certificate body looks like a PEM certificate
        if !certificate_body.contains("-----BEGIN CERTIFICATE-----")
            || !certificate_body.contains("-----END CERTIFICATE-----")
        {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "MalformedCertificate",
                "Certificate body is malformed.".to_string(),
            ));
        }

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
                "Cannot exceed quota for CertificatesPerUser: 2".to_string(),
            ));
        }

        let cert = SigningCertificate {
            certificate_id: format!("ASC{}", generate_long_id()),
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

    pub(super) fn list_signing_certificates(
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

    pub(super) fn update_signing_certificate(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn delete_signing_certificate(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let certificate_id = required_param(&req.query_params, "CertificateId")?;

        let mut state = self.state.write();

        let found = if let Some(certs) = state.signing_certificates.get_mut(&user_name) {
            let before = certs.len();
            certs.retain(|c| c.certificate_id != certificate_id);
            certs.len() < before
        } else {
            false
        };

        if !found {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The Certificate with id {certificate_id} cannot be found."),
            ));
        }

        let xml = empty_response("DeleteSigningCertificate", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= SSH Public Key operations =========

impl IamService {
    pub(super) fn upload_ssh_public_key(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let ssh_public_key_body = required_param(&req.query_params, "SSHPublicKeyBody")?;

        let mut state = self.state.write();

        if !state.users.contains_key(&user_name) {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        let key_id = format!("APKA{}", generate_id());
        // Generate a simple fingerprint from the body
        let fingerprint = format!(
            "{}:{}:{}:{}:{}",
            &generate_id()[..2],
            &generate_id()[..2],
            &generate_id()[..2],
            &generate_id()[..2],
            &generate_id()[..2]
        );

        let key = SshPublicKey {
            ssh_public_key_id: key_id.clone(),
            user_name: user_name.clone(),
            ssh_public_key_body: ssh_public_key_body.clone(),
            status: "Active".to_string(),
            upload_date: Utc::now(),
            fingerprint: fingerprint.clone(),
        };

        let upload_date = key.upload_date.format("%Y-%m-%dT%H:%M:%SZ").to_string();
        state
            .ssh_public_keys
            .entry(user_name.clone())
            .or_default()
            .push(key);

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<UploadSSHPublicKeyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <UploadSSHPublicKeyResult>
    <SSHPublicKey>
      <UserName>{user_name}</UserName>
      <SSHPublicKeyId>{key_id}</SSHPublicKeyId>
      <Fingerprint>{fingerprint}</Fingerprint>
      <SSHPublicKeyBody>{}</SSHPublicKeyBody>
      <Status>Active</Status>
      <UploadDate>{upload_date}</UploadDate>
    </SSHPublicKey>
  </UploadSSHPublicKeyResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</UploadSSHPublicKeyResponse>"#,
            xml_escape(&ssh_public_key_body),
            req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn get_ssh_public_key(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let ssh_public_key_id = required_param(&req.query_params, "SSHPublicKeyId")?;

        let state = self.state.read();

        let key = state
            .ssh_public_keys
            .get(&user_name)
            .and_then(|keys| {
                keys.iter()
                    .find(|k| k.ssh_public_key_id == ssh_public_key_id)
            })
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchEntity",
                    format!("The SSH public key with id {ssh_public_key_id} cannot be found."),
                )
            })?;

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetSSHPublicKeyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetSSHPublicKeyResult>
    <SSHPublicKey>
      <UserName>{}</UserName>
      <SSHPublicKeyId>{}</SSHPublicKeyId>
      <Fingerprint>{}</Fingerprint>
      <SSHPublicKeyBody>{}</SSHPublicKeyBody>
      <Status>{}</Status>
      <UploadDate>{}</UploadDate>
    </SSHPublicKey>
  </GetSSHPublicKeyResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetSSHPublicKeyResponse>"#,
            key.user_name,
            key.ssh_public_key_id,
            key.fingerprint,
            xml_escape(&key.ssh_public_key_body),
            key.status,
            key.upload_date.format("%Y-%m-%dT%H:%M:%SZ"),
            req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn list_ssh_public_keys(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let state = self.state.read();

        let keys = state.ssh_public_keys.get(&user_name);
        let members: String = keys
            .map(|ks| {
                ks.iter()
                    .map(|k| {
                        format!(
                            r#"      <member>
        <UserName>{}</UserName>
        <SSHPublicKeyId>{}</SSHPublicKeyId>
        <Status>{}</Status>
        <UploadDate>{}</UploadDate>
      </member>"#,
                            k.user_name,
                            k.ssh_public_key_id,
                            k.status,
                            k.upload_date.format("%Y-%m-%dT%H:%M:%SZ"),
                        )
                    })
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default();

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ListSSHPublicKeysResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <ListSSHPublicKeysResult>
    <SSHPublicKeys>
{members}
    </SSHPublicKeys>
    <IsTruncated>false</IsTruncated>
  </ListSSHPublicKeysResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</ListSSHPublicKeysResponse>"#,
            req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn update_ssh_public_key(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let ssh_public_key_id = required_param(&req.query_params, "SSHPublicKeyId")?;
        let status = required_param(&req.query_params, "Status")?;

        let mut state = self.state.write();

        let key = state
            .ssh_public_keys
            .get_mut(&user_name)
            .and_then(|keys| {
                keys.iter_mut()
                    .find(|k| k.ssh_public_key_id == ssh_public_key_id)
            })
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchEntity",
                    format!("The SSH public key with id {ssh_public_key_id} cannot be found."),
                )
            })?;

        key.status = status;

        let xml = empty_response("UpdateSSHPublicKey", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }

    pub(super) fn delete_ssh_public_key(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
        let ssh_public_key_id = required_param(&req.query_params, "SSHPublicKeyId")?;

        let mut state = self.state.write();

        if let Some(keys) = state.ssh_public_keys.get_mut(&user_name) {
            let len_before = keys.len();
            keys.retain(|k| k.ssh_public_key_id != ssh_public_key_id);
            if keys.len() == len_before {
                return Err(AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NoSuchEntity",
                    format!("The SSH Public Key with id {ssh_public_key_id} cannot be found."),
                ));
            }
        } else {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The user with name {user_name} cannot be found."),
            ));
        }

        let xml = empty_response("DeleteSSHPublicKey", &req.request_id);
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= GetAccessKeyLastUsed =========

impl IamService {
    pub(super) fn get_access_key_last_used(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let access_key_id = required_param(&req.query_params, "AccessKeyId")?;
        let state = self.state.read();

        // Find the user that owns this access key
        let mut user_name = String::new();
        for (uname, keys) in &state.access_keys {
            if keys.iter().any(|k| k.access_key_id == access_key_id) {
                user_name = uname.clone();
                break;
            }
        }

        if user_name.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NoSuchEntity",
                format!("The Access Key with id {access_key_id} cannot be found."),
            ));
        }

        let last_used_xml = if let Some(usage) = state.access_key_last_used.get(&access_key_id) {
            format!(
                r#"    <AccessKeyLastUsed>
      <LastUsedDate>{}</LastUsedDate>
      <Region>{}</Region>
      <ServiceName>{}</ServiceName>
    </AccessKeyLastUsed>"#,
                usage.last_used_date.format("%Y-%m-%dT%H:%M:%SZ"),
                usage.region,
                usage.service_name,
            )
        } else {
            r#"    <AccessKeyLastUsed>
      <Region>N/A</Region>
      <ServiceName>N/A</ServiceName>
    </AccessKeyLastUsed>"#
                .to_string()
        };

        let xml = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<GetAccessKeyLastUsedResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/">
  <GetAccessKeyLastUsedResult>
    <UserName>{user_name}</UserName>
{last_used_xml}
  </GetAccessKeyLastUsedResult>
  <ResponseMetadata>
    <RequestId>{}</RequestId>
  </ResponseMetadata>
</GetAccessKeyLastUsedResponse>"#,
            req.request_id,
        );
        Ok(AwsResponse::xml(StatusCode::OK, xml))
    }
}

// ========= User policy operations =========

impl IamService {
    pub(super) fn attach_user_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn detach_user_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn list_attached_user_policies(
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

    pub(super) fn put_user_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_name = required_param(&req.query_params, "UserName")?;
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

    pub(super) fn get_user_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn delete_user_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
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

    pub(super) fn list_user_policies(
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
