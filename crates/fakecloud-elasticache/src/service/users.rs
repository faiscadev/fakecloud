//! ElastiCache `users` family handlers extracted from service.rs
//! by audit-2026-05-19 file-split.

use super::*;

impl ElastiCacheService {
    pub(super) fn create_user(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_id = required_query_param(request, "UserId")?;
        let user_name = required_query_param(request, "UserName")?;
        // Engine is case-insensitive on the wire (the provider sends "REDIS");
        // AWS normalises and stores it lowercase, so do the same.
        let engine = required_query_param(request, "Engine")?.to_lowercase();
        let access_string = required_query_param(request, "AccessString")?;

        validate_engine(&engine)?;

        let no_password_required =
            parse_optional_bool(optional_query_param(request, "NoPasswordRequired").as_deref())?
                .unwrap_or(false);
        let passwords = parse_member_list(&request.query_params, "Passwords", "member");
        let auth_mode_type = optional_query_param(request, "AuthenticationMode.Type");

        let (authentication_type, password_count) = if no_password_required {
            if !passwords.is_empty() {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterCombination",
                    "Passwords cannot be provided when NoPasswordRequired is true.".to_string(),
                ));
            }
            ("no-password".to_string(), 0)
        } else if let Some(ref mode) = auth_mode_type {
            let mode_passwords = parse_member_list(
                &request.query_params,
                "AuthenticationMode.Passwords",
                "member",
            );
            match mode.as_str() {
                "password" => {
                    if mode_passwords.is_empty() {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidParameterValue",
                            "At least one password is required when AuthenticationMode.Type is password.".to_string(),
                        ));
                    }
                    ("password".to_string(), mode_passwords.len() as i32)
                }
                "no-password-required" | "iam" => {
                    if !mode_passwords.is_empty() {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "InvalidParameterValue",
                            format!("Passwords cannot be provided when AuthenticationMode.Type is {mode}."),
                        ));
                    }
                    (mode.clone(), 0)
                }
                _ => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!("Invalid value for AuthenticationMode.Type: {mode}. Supported values: password, iam, no-password-required"),
                    ));
                }
            }
        } else if !passwords.is_empty() {
            ("password".to_string(), passwords.len() as i32)
        } else {
            ("no-password".to_string(), 0)
        };

        let minimum_engine_version = if engine == ENGINE_VALKEY {
            "8.0".to_string()
        } else {
            "6.0".to_string()
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

        if state.users.contains_key(&user_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UserAlreadyExists",
                format!("User {user_id} already exists."),
            ));
        }

        let arn = format!(
            "arn:aws:elasticache:{}:{}:user:{}",
            state.region, state.account_id, user_id
        );

        let user = ElastiCacheUser {
            user_id: user_id.clone(),
            user_name,
            engine,
            access_string,
            status: "active".to_string(),
            authentication_type,
            password_count,
            arn,
            minimum_engine_version,
            user_group_ids: Vec::new(),
        };

        let xml = user_xml(&user);
        state.register_arn(&user.arn);
        state.users.insert(user_id, user);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml("CreateUser", ELASTICACHE_NS, &xml, &request.request_id),
        ))
    }

    pub(super) fn describe_users(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Match the Smithy `@pattern` + `@length(min=1)` on UserId. A
        // zero-length value, or one that doesn't begin with an ASCII
        // letter followed by alphanumeric/hyphen characters, isn't a
        // valid UserId per the model. Empty values are already filtered
        // to None by `optional_param`; reach the raw map to detect the
        // explicit-but-empty case the conformance probe sends.
        if let Some(raw) = request.query_params.get("UserId") {
            if !is_valid_user_id(raw) {
                // DescribeUsers declares InvalidParameterCombinationException
                // but not InvalidParameterValueException, so the wire code
                // for a bad UserId value must be `InvalidParameterCombination`.
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "InvalidParameterCombination",
                    format!("Invalid value for UserId: '{raw}'"),
                ));
            }
        }
        let user_id = optional_query_param(request, "UserId");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_query_param(request, "Marker");

        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);

        let users: Vec<&ElastiCacheUser> = if let Some(ref id) = user_id {
            match state.users.get(id) {
                Some(u) => vec![u],
                None => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "UserNotFound",
                        format!("User {id} not found."),
                    ));
                }
            }
        } else {
            let mut users: Vec<&ElastiCacheUser> = state.users.values().collect();
            users.sort_by(|a, b| a.user_id.cmp(&b.user_id));
            users
        };

        let (page, next_marker) = paginate(&users, marker.as_deref(), max_records)?;

        let members_xml: String = page
            .iter()
            .map(|u| format!("<member>{}</member>", user_xml(u)))
            .collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeUsers",
                ELASTICACHE_NS,
                &format!("<Users>{members_xml}</Users>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn delete_user(&self, request: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let user_id = required_query_param(request, "UserId")?;

        if user_id == "default" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidParameterValue",
                "Cannot delete the default user.".to_string(),
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

        let user = state.users.remove(&user_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UserNotFound",
                format!("User {user_id} not found."),
            )
        })?;

        state.tags.remove(&user.arn);

        // Remove user from any user groups
        for group in state.user_groups.values_mut() {
            group.user_ids.retain(|id| id != &user_id);
        }

        let mut deleted_user = user;
        deleted_user.status = "deleting".to_string();
        let xml = user_xml(&deleted_user);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml("DeleteUser", ELASTICACHE_NS, &xml, &request.request_id),
        ))
    }

    pub(super) fn create_user_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let user_group_id = required_query_param(request, "UserGroupId")?;
        // Engine is case-insensitive on the wire (the provider sends "REDIS");
        // AWS normalises and stores it lowercase, matching the user records.
        let engine = required_query_param(request, "Engine")?.to_lowercase();

        validate_engine(&engine)?;

        let user_ids = parse_member_list(&request.query_params, "UserIds", "member");

        let minimum_engine_version = if engine == ENGINE_VALKEY {
            "8.0".to_string()
        } else {
            "6.0".to_string()
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

        if state.user_groups.contains_key(&user_group_id) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "UserGroupAlreadyExists",
                format!("User Group {user_group_id} already exists."),
            ));
        }

        // Validate all referenced users exist and have a matching engine
        for uid in &user_ids {
            match state.users.get(uid) {
                None => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "UserNotFound",
                        format!("User {uid} not found."),
                    ));
                }
                Some(user) if user.engine != engine => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::BAD_REQUEST,
                        "InvalidParameterValue",
                        format!(
                            "User {uid} has engine {} which does not match the user group engine {engine}.",
                            user.engine
                        ),
                    ));
                }
                _ => {}
            }
        }

        let arn = format!(
            "arn:aws:elasticache:{}:{}:usergroup:{}",
            state.region, state.account_id, user_group_id
        );

        let group = ElastiCacheUserGroup {
            user_group_id: user_group_id.clone(),
            engine,
            status: "active".to_string(),
            user_ids: user_ids.clone(),
            arn,
            minimum_engine_version,
            pending_changes: None,
            replication_groups: Vec::new(),
        };

        // Update user_group_ids on referenced users
        for uid in &user_ids {
            if let Some(user) = state.users.get_mut(uid) {
                user.user_group_ids.push(user_group_id.clone());
            }
        }

        let xml = user_group_xml(&group);
        state.register_arn(&group.arn);
        state.user_groups.insert(user_group_id, group);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml("CreateUserGroup", ELASTICACHE_NS, &xml, &request.request_id),
        ))
    }

    pub(super) fn describe_user_groups(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let user_group_id = optional_query_param(request, "UserGroupId");
        let max_records = optional_usize_param(request, "MaxRecords")?;
        let marker = optional_query_param(request, "Marker");

        let accounts = self.state.read();
        let empty = ElastiCacheState::new(&request.account_id, &request.region);
        let state = accounts.get(&request.account_id).unwrap_or(&empty);

        let groups: Vec<&ElastiCacheUserGroup> = if let Some(ref id) = user_group_id {
            match state.user_groups.get(id) {
                Some(g) => vec![g],
                None => {
                    return Err(AwsServiceError::aws_error(
                        StatusCode::NOT_FOUND,
                        "UserGroupNotFound",
                        format!("User Group {id} not found."),
                    ));
                }
            }
        } else {
            let mut groups: Vec<&ElastiCacheUserGroup> = state.user_groups.values().collect();
            groups.sort_by(|a, b| a.user_group_id.cmp(&b.user_group_id));
            groups
        };

        let (page, next_marker) = paginate(&groups, marker.as_deref(), max_records)?;

        let members_xml: String = page
            .iter()
            .map(|g| format!("<member>{}</member>", user_group_xml(g)))
            .collect();
        let marker_xml = next_marker
            .map(|m| format!("<Marker>{}</Marker>", xml_escape(&m)))
            .unwrap_or_default();

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml(
                "DescribeUserGroups",
                ELASTICACHE_NS,
                &format!("<UserGroups>{members_xml}</UserGroups>{marker_xml}"),
                &request.request_id,
            ),
        ))
    }

    pub(super) fn delete_user_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let user_group_id = required_query_param(request, "UserGroupId")?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&request.account_id);

        let group = state.user_groups.remove(&user_group_id).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "UserGroupNotFound",
                format!("User Group {user_group_id} not found."),
            )
        })?;

        state.tags.remove(&group.arn);

        // Remove this group from users' user_group_ids
        for uid in &group.user_ids {
            if let Some(user) = state.users.get_mut(uid) {
                user.user_group_ids.retain(|gid| gid != &user_group_id);
            }
        }

        let mut deleted_group = group;
        deleted_group.status = "deleting".to_string();
        let xml = user_group_xml(&deleted_group);

        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml("DeleteUserGroup", ELASTICACHE_NS, &xml, &request.request_id),
        ))
    }

    // ── Users / User groups (modify) ──

    pub(super) async fn modify_user(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(request, "UserId")?;
        let access_string = optional_query_param(request, "AccessString");
        let (xml, affected_rg_ids) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            let user = state.users.get_mut(&id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "UserNotFound",
                    format!("User {id} not found."),
                )
            })?;
            if let Some(a) = access_string {
                user.access_string = a;
            }
            user.status = "modifying".to_string();
            let rg_ids: Vec<String> = state
                .replication_groups
                .values()
                .filter(|rg| {
                    rg.user_group_ids
                        .iter()
                        .any(|ug| user.user_group_ids.contains(ug))
                })
                .map(|rg| rg.replication_group_id.clone())
                .collect();
            (user_xml(user), rg_ids)
        };
        for rg_id in affected_rg_ids {
            self.apply_acls_for_replication_group(&request.account_id, &rg_id)
                .await;
        }
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml("ModifyUser", ELASTICACHE_NS, &xml, &request.request_id),
        ))
    }

    pub(super) async fn modify_user_group(
        &self,
        request: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let id = required_query_param(request, "UserGroupId")?;
        let to_add = collect_indexed_strings(request, "UserIdsToAdd.member");
        let to_remove = collect_indexed_strings(request, "UserIdsToRemove.member");
        let (xml, affected_rg_ids) = {
            let mut accounts = self.state.write();
            let state = accounts.get_or_create(&request.account_id);
            let group = state.user_groups.get_mut(&id).ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "UserGroupNotFound",
                    format!("UserGroup {id} not found."),
                )
            })?;
            for u in &to_add {
                if !group.user_ids.contains(u) {
                    group.user_ids.push(u.clone());
                }
            }
            group.user_ids.retain(|u| !to_remove.contains(u));
            // The membership change is applied synchronously, so the group is
            // immediately back to `active`. Leaving it `modifying` would hang
            // the provider's update waiter (Pending: modifying, Target: active)
            // forever, since fakecloud has no async tick to flip it.
            group.status = "active".to_string();
            // Keep the reverse mapping on each User in sync so that
            // ModifyUser can resolve the correct replication groups.
            for u in &to_add {
                if let Some(user) = state.users.get_mut(u) {
                    if !user.user_group_ids.contains(&id) {
                        user.user_group_ids.push(id.clone());
                    }
                }
            }
            for u in &to_remove {
                if let Some(user) = state.users.get_mut(u) {
                    user.user_group_ids.retain(|ug| ug != &id);
                }
            }
            let rg_ids: Vec<String> = state
                .replication_groups
                .values()
                .filter(|rg| rg.user_group_ids.contains(&id))
                .map(|rg| rg.replication_group_id.clone())
                .collect();
            (user_group_xml(group), rg_ids)
        };
        for rg_id in affected_rg_ids {
            self.apply_acls_for_replication_group(&request.account_id, &rg_id)
                .await;
        }
        Ok(AwsResponse::xml(
            StatusCode::OK,
            query_response_xml("ModifyUserGroup", ELASTICACHE_NS, &xml, &request.request_id),
        ))
    }
}
