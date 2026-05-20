//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `iam`.

use super::*;

impl ResourceProvisioner {
    pub(super) fn get_att_iam_role(&self, physical_id: &str, attribute: &str) -> Option<String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        // physical_id may be the role ARN or the role name depending on how
        // provisioning resolved Ref. Try both shapes.
        let role = state
            .roles
            .values()
            .find(|r| r.arn == physical_id || r.role_name == physical_id)?;
        match attribute {
            "Arn" => Some(role.arn.clone()),
            "RoleId" => Some(role.role_id.clone()),
            _ => None,
        }
    }

    // --- IAM Role ---

    pub(super) fn create_iam_role(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let role_name = props
            .get("RoleName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let assume_role_policy = props
            .get("AssumeRolePolicyDocument")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap().to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .unwrap_or_default();

        let path = props.get("Path").and_then(|v| v.as_str()).unwrap_or("/");

        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let role_id = format!(
            "FKIA{}",
            &Uuid::new_v4().to_string().replace('-', "").to_uppercase()[..16]
        );
        let arn = format!(
            "arn:aws:iam::{}:role{}{}",
            state.account_id,
            if path == "/" { "/" } else { path },
            role_name
        );

        let role = IamRole {
            role_name: role_name.to_string(),
            role_id: role_id.clone(),
            arn: arn.clone(),
            path: path.to_string(),
            assume_role_policy_document: assume_role_policy,
            created_at: Utc::now(),
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            max_session_duration: 3600,
            tags: Vec::new(),
            permissions_boundary: None,
        };

        state.roles.insert(role_name.to_string(), role);
        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn)
            .with("RoleId", role_id))
    }

    pub(super) fn delete_iam_role(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        // physical_id is the ARN; find the role name
        let role_name = state
            .roles
            .iter()
            .find(|(_, r)| r.arn == physical_id)
            .map(|(name, _)| name.clone());
        if let Some(name) = role_name {
            state.roles.remove(&name);
            state.role_policies.remove(&name);
            state.role_inline_policies.remove(&name);
        }
        Ok(())
    }

    // --- IAM Policy ---

    pub(super) fn create_iam_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let policy_name = props
            .get("PolicyName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id);

        let policy_document = props
            .get("PolicyDocument")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap().to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .unwrap_or_default();

        let path = props.get("Path").and_then(|v| v.as_str()).unwrap_or("/");

        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let policy_id = format!(
            "FSIA{}",
            &Uuid::new_v4().to_string().replace('-', "").to_uppercase()[..16]
        );
        let arn = format!(
            "arn:aws:iam::{}:policy{}{}",
            state.account_id,
            if path == "/" { "/" } else { path },
            policy_name
        );

        let now = Utc::now();
        let policy = IamPolicy {
            policy_name: policy_name.to_string(),
            policy_id,
            arn: arn.clone(),
            path: path.to_string(),
            description: props
                .get("Description")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            created_at: now,
            tags: Vec::new(),
            default_version_id: "v1".to_string(),
            versions: vec![PolicyVersion {
                version_id: "v1".to_string(),
                document: policy_document,
                is_default: true,
                created_at: now,
            }],
            next_version_num: 2,
            attachment_count: 0,
        };

        state.policies.insert(arn.clone(), policy);
        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    pub(super) fn delete_iam_policy(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.policies.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_iam_user(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let user_name = props
            .get("UserName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let path = props
            .get("Path")
            .and_then(|v| v.as_str())
            .unwrap_or("/")
            .to_string();
        let permissions_boundary = props
            .get("PermissionsBoundary")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let tags = parse_iam_tags(props.get("Tags"));

        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if state.users.contains_key(&user_name) {
            return Err(format!("User {user_name} already exists"));
        }
        let arn = format!(
            "arn:aws:iam::{}:user{}{}",
            state.account_id, path, user_name
        );
        let user_id = format!(
            "AIDA{}",
            &Uuid::new_v4().to_string().replace('-', "").to_uppercase()[..16]
        );
        let user = IamUser {
            user_name: user_name.clone(),
            user_id: user_id.clone(),
            arn: arn.clone(),
            path,
            created_at: Utc::now(),
            tags,
            permissions_boundary,
        };
        state.users.insert(user_name.clone(), user);

        // Inline + managed policies declared inline on the user.
        if let Some(policies) = props.get("Policies").and_then(|v| v.as_array()) {
            let inline = state
                .user_inline_policies
                .entry(user_name.clone())
                .or_default();
            for p in policies {
                if let (Some(n), Some(doc)) = (
                    p.get("PolicyName").and_then(|v| v.as_str()),
                    p.get("PolicyDocument"),
                ) {
                    let document = if doc.is_string() {
                        doc.as_str().unwrap_or("").to_string()
                    } else {
                        serde_json::to_string(doc).unwrap_or_default()
                    };
                    inline.insert(n.to_string(), document);
                }
            }
        }
        if let Some(arns) = props.get("ManagedPolicyArns").and_then(|v| v.as_array()) {
            let attached = state.user_policies.entry(user_name.clone()).or_default();
            for a in arns {
                if let Some(s) = a.as_str() {
                    if !attached.contains(&s.to_string()) {
                        attached.push(s.to_string());
                    }
                }
            }
        }
        if let Some(groups) = props.get("Groups").and_then(|v| v.as_array()) {
            for g in groups {
                if let Some(g_name) = g.as_str() {
                    if let Some(group) = state.groups.get_mut(g_name) {
                        if !group.members.iter().any(|m| m == &user_name) {
                            group.members.push(user_name.clone());
                        }
                    }
                }
            }
        }

        Ok(ProvisionResult::new(user_name).with("Arn", arn))
    }

    pub(super) fn delete_iam_user(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.users.remove(physical_id);
        state.user_inline_policies.remove(physical_id);
        state.user_policies.remove(physical_id);
        state.access_keys.remove(physical_id);
        for group in state.groups.values_mut() {
            group.members.retain(|m| m != physical_id);
        }
        Ok(())
    }

    pub(super) fn create_iam_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let group_name = props
            .get("GroupName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let path = props
            .get("Path")
            .and_then(|v| v.as_str())
            .unwrap_or("/")
            .to_string();

        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if state.groups.contains_key(&group_name) {
            return Err(format!("Group {group_name} already exists"));
        }
        let arn = format!(
            "arn:aws:iam::{}:group{}{}",
            state.account_id, path, group_name
        );
        let group_id = format!(
            "AGPA{}",
            &Uuid::new_v4().to_string().replace('-', "").to_uppercase()[..16]
        );
        let mut inline_policies: BTreeMap<String, String> = BTreeMap::new();
        if let Some(policies) = props.get("Policies").and_then(|v| v.as_array()) {
            for p in policies {
                if let (Some(n), Some(doc)) = (
                    p.get("PolicyName").and_then(|v| v.as_str()),
                    p.get("PolicyDocument"),
                ) {
                    let document = if doc.is_string() {
                        doc.as_str().unwrap_or("").to_string()
                    } else {
                        serde_json::to_string(doc).unwrap_or_default()
                    };
                    inline_policies.insert(n.to_string(), document);
                }
            }
        }
        let mut attached_policies: Vec<String> = Vec::new();
        if let Some(arns) = props.get("ManagedPolicyArns").and_then(|v| v.as_array()) {
            for a in arns {
                if let Some(s) = a.as_str() {
                    attached_policies.push(s.to_string());
                }
            }
        }
        state.groups.insert(
            group_name.clone(),
            IamGroup {
                group_name: group_name.clone(),
                group_id,
                arn: arn.clone(),
                path,
                created_at: Utc::now(),
                members: Vec::new(),
                inline_policies,
                attached_policies,
            },
        );

        Ok(ProvisionResult::new(group_name).with("Arn", arn))
    }

    pub(super) fn delete_iam_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.groups.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_iam_managed_policy(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        // Same shape as AWS::IAM::Policy minus the inline-attach knobs;
        // ManagedPolicy is a standalone policy, attached separately.
        let props = &resource.properties;
        let policy_name = props
            .get("ManagedPolicyName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let policy_document = props
            .get("PolicyDocument")
            .map(|v| {
                if v.is_string() {
                    v.as_str().unwrap_or("").to_string()
                } else {
                    serde_json::to_string(v).unwrap_or_default()
                }
            })
            .unwrap_or_default();
        let path = props
            .get("Path")
            .and_then(|v| v.as_str())
            .unwrap_or("/")
            .to_string();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let arn = format!(
            "arn:aws:iam::{}:policy{}{}",
            state.account_id,
            if path == "/" { "/" } else { path.as_str() },
            policy_name
        );
        if state.policies.contains_key(&arn) {
            return Err(format!("Managed policy {policy_name} already exists"));
        }
        let policy_id = format!(
            "ANPA{}",
            &Uuid::new_v4().to_string().replace('-', "").to_uppercase()[..16]
        );
        let now = Utc::now();
        state.policies.insert(
            arn.clone(),
            IamPolicy {
                policy_name,
                policy_id,
                arn: arn.clone(),
                path,
                description,
                created_at: now,
                tags: Vec::new(),
                default_version_id: "v1".to_string(),
                versions: vec![PolicyVersion {
                    version_id: "v1".to_string(),
                    document: policy_document,
                    is_default: true,
                    created_at: now,
                }],
                next_version_num: 2,
                attachment_count: 0,
            },
        );

        // Attach to declared users/groups/roles.
        if let Some(users) = props.get("Users").and_then(|v| v.as_array()) {
            for u in users {
                if let Some(name) = u.as_str() {
                    let attached = state.user_policies.entry(name.to_string()).or_default();
                    if !attached.contains(&arn) {
                        attached.push(arn.clone());
                    }
                }
            }
        }
        if let Some(groups) = props.get("Groups").and_then(|v| v.as_array()) {
            for g in groups {
                if let Some(name) = g.as_str() {
                    if let Some(group) = state.groups.get_mut(name) {
                        if !group.attached_policies.contains(&arn) {
                            group.attached_policies.push(arn.clone());
                        }
                    }
                }
            }
        }
        if let Some(roles) = props.get("Roles").and_then(|v| v.as_array()) {
            for r in roles {
                if let Some(name) = r.as_str() {
                    let attached = state.role_policies.entry(name.to_string()).or_default();
                    if !attached.contains(&arn) {
                        attached.push(arn.clone());
                    }
                }
            }
        }

        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    pub(super) fn delete_iam_managed_policy(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.policies.remove(physical_id);
        for arns in state.user_policies.values_mut() {
            arns.retain(|a| a != physical_id);
        }
        for arns in state.role_policies.values_mut() {
            arns.retain(|a| a != physical_id);
        }
        for group in state.groups.values_mut() {
            group.attached_policies.retain(|a| a != physical_id);
        }
        Ok(())
    }

    pub(super) fn create_iam_user_to_group_addition(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let group_name = props
            .get("GroupName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "GroupName is required".to_string())?
            .to_string();
        let users: Vec<String> = props
            .get("Users")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|u| u.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        let group = state
            .groups
            .get_mut(&group_name)
            .ok_or_else(|| format!("Group {group_name} does not exist"))?;
        for u in &users {
            if !group.members.iter().any(|m| m == u) {
                group.members.push(u.clone());
            }
        }

        // Encode group + users so delete can revert exactly this addition.
        let physical_id = format!("{group_name}|{}", users.join(","));
        Ok(ProvisionResult::new(physical_id))
    }

    pub(super) fn delete_iam_user_to_group_addition(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if let Some((group_name, users)) = physical_id.split_once('|') {
            if let Some(group) = state.groups.get_mut(group_name) {
                let to_remove: Vec<&str> = users.split(',').filter(|s| !s.is_empty()).collect();
                group.members.retain(|m| !to_remove.iter().any(|u| u == m));
            }
        }
        Ok(())
    }

    pub(super) fn create_iam_access_key(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let user_name = props
            .get("UserName")
            .and_then(|v| v.as_str())
            .ok_or_else(|| "UserName is required".to_string())?
            .to_string();
        let status = props
            .get("Status")
            .and_then(|v| v.as_str())
            .unwrap_or("Active")
            .to_string();

        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if !state.users.contains_key(&user_name) {
            return Err(format!("User {user_name} does not exist"));
        }
        let access_key_id = format!(
            "AKIA{}",
            &Uuid::new_v4().to_string().replace('-', "").to_uppercase()[..16]
        );
        let secret_access_key: String = Uuid::new_v4()
            .to_string()
            .replace('-', "")
            .chars()
            .take(40)
            .collect();
        state
            .access_keys
            .entry(user_name.clone())
            .or_default()
            .push(IamAccessKey {
                access_key_id: access_key_id.clone(),
                secret_access_key: secret_access_key.clone(),
                user_name: user_name.clone(),
                status,
                created_at: Utc::now(),
            });

        Ok(ProvisionResult::new(access_key_id.clone()).with("SecretAccessKey", secret_access_key))
    }

    pub(super) fn delete_iam_access_key(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        for keys in state.access_keys.values_mut() {
            keys.retain(|k| k.access_key_id != physical_id);
        }
        Ok(())
    }

    pub(super) fn create_iam_instance_profile(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("InstanceProfileName")
            .and_then(|v| v.as_str())
            .unwrap_or(&resource.logical_id)
            .to_string();
        let path = props
            .get("Path")
            .and_then(|v| v.as_str())
            .unwrap_or("/")
            .to_string();
        // Roles[] entries can be plain role names or `Ref`-resolved role
        // ARNs (which the IAM Role provisioner emits as physical_id);
        // extract the trailing name segment so DescribeInstanceProfile
        // round-trips a name list.
        let roles: Vec<String> = props
            .get("Roles")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|r| r.as_str())
                    .map(|s| {
                        if let Some(rest) = s.strip_prefix("arn:aws:iam::") {
                            rest.split(":role/")
                                .nth(1)
                                .map(|name| name.to_string())
                                .unwrap_or_else(|| s.to_string())
                        } else {
                            s.to_string()
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        if state.instance_profiles.contains_key(&name) {
            return Err(format!("InstanceProfile {name} already exists"));
        }
        // Force a retry pass when role refs haven't been resolved yet: a
        // logical-id placeholder won't match any real role, and silently
        // storing it would leave DescribeInstanceProfile returning an
        // empty Roles array.
        for role_name in &roles {
            if !state.roles.contains_key(role_name) {
                return Err(format!(
                    "InstanceProfile {name}: referenced role {role_name} not yet provisioned"
                ));
            }
        }
        let arn = format!(
            "arn:aws:iam::{}:instance-profile{}{}",
            state.account_id, path, name
        );
        let id = format!(
            "AIPA{}",
            &Uuid::new_v4().to_string().replace('-', "").to_uppercase()[..16]
        );
        state.instance_profiles.insert(
            name.clone(),
            IamInstanceProfile {
                instance_profile_name: name.clone(),
                instance_profile_id: id,
                arn: arn.clone(),
                path,
                created_at: Utc::now(),
                roles,
                tags: Vec::new(),
            },
        );

        Ok(ProvisionResult::new(name).with("Arn", arn))
    }

    pub(super) fn delete_iam_instance_profile(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.instance_profiles.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_iam_oidc_provider(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let url = props
            .get("Url")
            .and_then(|v| v.as_str())
            .ok_or("Url is required")?
            .to_string();
        let client_id_list: Vec<String> = props
            .get("ClientIdList")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let thumbprint_list: Vec<String> = props
            .get("ThumbprintList")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        // Real AWS strips the scheme to form the resource path component.
        let url_path = url
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .to_string();
        let arn = format!(
            "arn:aws:iam::{}:oidc-provider/{}",
            self.account_id, url_path
        );
        let provider = OidcProvider {
            arn: arn.clone(),
            url,
            client_id_list,
            thumbprint_list,
            created_at: Utc::now(),
            tags: Vec::new(),
        };
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.oidc_providers.insert(arn.clone(), provider);
        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    pub(super) fn delete_iam_oidc_provider(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.oidc_providers.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_iam_saml_provider(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .map(String::from)
            .unwrap_or_else(|| {
                let suffix = Uuid::new_v4().simple().to_string();
                format!("{}-{}", resource.logical_id, &suffix[..8])
            });
        let saml_metadata_document = props
            .get("SamlMetadataDocument")
            .and_then(|v| v.as_str())
            .ok_or("SamlMetadataDocument is required")?
            .to_string();
        let arn =
            Arn::global("iam", &self.account_id, &format!("saml-provider/{name}")).to_string();
        let now = Utc::now();
        let valid_until = now + chrono::Duration::days(365 * 10);
        let provider = SamlProvider {
            arn: arn.clone(),
            name,
            saml_metadata_document,
            created_at: now,
            valid_until,
            tags: Vec::new(),
        };
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.saml_providers.insert(arn.clone(), provider);
        Ok(ProvisionResult::new(arn.clone()).with("Arn", arn))
    }

    pub(super) fn delete_iam_saml_provider(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.saml_providers.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_iam_service_linked_role(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let aws_service_name = props
            .get("AWSServiceName")
            .and_then(|v| v.as_str())
            .ok_or("AWSServiceName is required")?
            .to_string();
        let custom_suffix = props
            .get("CustomSuffix")
            .and_then(|v| v.as_str())
            .map(String::from);
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        // AWS service-linked role naming convention.
        let service_short = aws_service_name.split('.').next().unwrap_or("Service");
        let role_name = match &custom_suffix {
            Some(s) => format!("AWSServiceRoleFor{service_short}_{s}"),
            None => format!("AWSServiceRoleFor{service_short}"),
        };
        let path = format!("/aws-service-role/{aws_service_name}/");
        let arn =
            Arn::global("iam", &self.account_id, &format!("role{path}{role_name}")).to_string();
        // Service-linked roles get a trust policy specific to the service.
        let assume_role_policy_document = serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Effect": "Allow",
                "Principal": {"Service": aws_service_name.clone()},
                "Action": "sts:AssumeRole"
            }]
        })
        .to_string();
        let role_id_suffix = Uuid::new_v4().simple().to_string();
        let role = IamRole {
            role_name: role_name.clone(),
            role_id: format!("AROA{}", role_id_suffix[..16].to_uppercase()),
            arn: arn.clone(),
            path,
            assume_role_policy_document,
            created_at: Utc::now(),
            description: Some(description),
            max_session_duration: 3600,
            tags: Vec::new(),
            permissions_boundary: None,
        };
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.roles.insert(role_name.clone(), role);
        Ok(ProvisionResult::new(role_name)
            .with("Arn", arn)
            .with("RoleId", String::new()))
    }

    pub(super) fn delete_iam_service_linked_role(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.roles.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_iam_virtual_mfa_device(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("VirtualMfaDeviceName")
            .and_then(|v| v.as_str())
            .ok_or("VirtualMfaDeviceName is required")?
            .to_string();
        let path = props
            .get("Path")
            .and_then(|v| v.as_str())
            .unwrap_or("/")
            .to_string();
        let serial_number =
            Arn::global("iam", &self.account_id, &format!("mfa{path}{name}")).to_string();
        // Real AWS returns a base32 seed + a PNG QR code; we synthesize
        // deterministic placeholders so callers can read them back.
        let seed = format!("BASE32SEED{}", Uuid::new_v4().simple());
        let user = props
            .get("Users")
            .and_then(|v| v.as_array())
            .and_then(|arr| arr.first())
            .and_then(|v| v.as_str())
            .map(String::from);
        let device = VirtualMfaDevice {
            serial_number: serial_number.clone(),
            base32_string_seed: seed,
            qr_code_png: String::new(),
            enable_date: user.as_ref().map(|_| Utc::now()),
            user,
            tags: Vec::new(),
        };
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state
            .virtual_mfa_devices
            .insert(serial_number.clone(), device);
        Ok(ProvisionResult::new(serial_number.clone()).with("SerialNumber", serial_number))
    }

    pub(super) fn delete_iam_virtual_mfa_device(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.iam_state.write();
        let state = accounts.get_or_create(&self.account_id);
        state.virtual_mfa_devices.remove(physical_id);
        Ok(())
    }
}
