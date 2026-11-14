//! Auto-extracted from resource_provisioner/mod.rs by the
//! audit-2026-05-19 file-split. All methods here continue
//! the `impl ResourceProvisioner` block; the family slug is
//! `wafv2`.

use super::*;

impl ResourceProvisioner {
    /// The region a WAFv2 resource lives in: the one its ARN names (a
    /// CLOUDFRONT-scope ARN names the partition's global region), else the
    /// stack's region.
    fn wafv2_arn_region(&self, arn: &str) -> String {
        fakecloud_aws::arn::region_of(arn)
            .unwrap_or(&self.region)
            .to_string()
    }

    /// AWS only creates CLOUDFRONT-scope WAFv2 resources in the partition's
    /// global region (`us-east-1` for `aws`); a stack elsewhere fails.
    fn check_wafv2_scope_region(&self, scope: &str) -> Result<(), String> {
        if scope == "CLOUDFRONT"
            && self.region
                != fakecloud_aws::arn::implicit_global_region(fakecloud_aws::arn::partition_for(
                    &self.region,
                ))
        {
            return Err(
                "Error reason: The scope is not valid., field: SCOPE_VALUE, parameter: CLOUDFRONT"
                    .to_string(),
            );
        }
        Ok(())
    }

    pub(super) fn get_att_wafv2_web_acl(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts.region_mut(&self.account_id, &self.wafv2_arn_region(physical_id));
        let acl = state.web_acls.values().find(|a| a.arn == physical_id)?;
        match attribute {
            "Arn" => Some(acl.arn.clone()),
            "Id" => Some(acl.id.clone()),
            "Name" => Some(acl.name.clone()),
            "LabelNamespace" => Some(acl.label_namespace.clone()),
            "Capacity" => Some(acl.capacity.to_string()),
            _ => None,
        }
    }

    pub(super) fn get_att_wafv2_ip_set(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts.region_mut(&self.account_id, &self.wafv2_arn_region(physical_id));
        let ip_set = state.ip_sets.values().find(|i| i.arn == physical_id)?;
        match attribute {
            "Arn" => Some(ip_set.arn.clone()),
            "Id" => Some(ip_set.id.clone()),
            "Name" => Some(ip_set.name.clone()),
            _ => None,
        }
    }

    pub(super) fn get_att_wafv2_regex_pattern_set(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts.region_mut(&self.account_id, &self.wafv2_arn_region(physical_id));
        let set = state
            .regex_pattern_sets
            .values()
            .find(|r| r.arn == physical_id)?;
        match attribute {
            "Arn" => Some(set.arn.clone()),
            "Id" => Some(set.id.clone()),
            "Name" => Some(set.name.clone()),
            _ => None,
        }
    }

    pub(super) fn get_att_wafv2_rule_group(
        &self,
        physical_id: &str,
        attribute: &str,
    ) -> Option<String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts.region_mut(&self.account_id, &self.wafv2_arn_region(physical_id));
        let rg = state.rule_groups.values().find(|r| r.arn == physical_id)?;
        match attribute {
            "Arn" => Some(rg.arn.clone()),
            "Id" => Some(rg.id.clone()),
            "Name" => Some(rg.name.clone()),
            _ => None,
        }
    }

    // --- WAFv2 ---
    //
    // CFN exclusively writes WAFv2 resources at the global scope
    // (`CLOUDFRONT`) for global resources or `REGIONAL` for everything
    // else. We honor whatever the template specifies via the `Scope`
    // property and store under (scope, name).

    pub(super) fn create_wafv2_web_acl(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let scope = props
            .get("Scope")
            .and_then(|v| v.as_str())
            .ok_or("Scope is required")?
            .to_string();
        self.check_wafv2_scope_region(&scope)?;
        let default_action = props
            .get("DefaultAction")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({"Allow": {}}));
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);
        let rules = props
            .get("Rules")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let visibility_config = props
            .get("VisibilityConfig")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));
        let capacity = props.get("Capacity").and_then(|v| v.as_i64()).unwrap_or(0);

        let id = Uuid::new_v4().to_string();
        let arn = fakecloud_wafv2::synth_arn(
            &self.account_id,
            &self.region,
            &scope,
            "webacl",
            &name,
            &id,
        );
        let acl = WebAcl {
            id: id.clone(),
            name: name.clone(),
            arn: arn.clone(),
            scope: scope.clone(),
            default_action,
            description,
            rules,
            visibility_config,
            capacity,
            lock_token: Uuid::new_v4().simple().to_string(),
            label_namespace: format!("awswaf:{}:webacl:{}:", self.account_id, name),
            custom_response_bodies: BTreeMap::new(),
            captcha_config: None,
            challenge_config: None,
            token_domains: Vec::new(),
            association_config: None,
            data_protection_config: None,
            on_source_d_do_s_protection_config: None,
            application_config: None,
            monetization_config: None,
            retrofitted_by_firewall_manager: false,
            pre_process_firewall_manager_rule_groups: Vec::new(),
            post_process_firewall_manager_rule_groups: Vec::new(),
            managed_by_firewall_manager: false,
            created_time: Utc::now(),
        };

        let mut accounts = self.wafv2_state.write();
        let state = accounts.region_mut(&self.account_id, &self.wafv2_arn_region(&arn));
        state.web_acls.insert((scope.clone(), name.clone()), acl);

        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn)
            .with("Id", id)
            .with("Name", name)
            .with("Capacity", capacity.to_string()))
    }

    pub(super) fn delete_wafv2_web_acl(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts.region_mut(&self.account_id, &self.wafv2_arn_region(physical_id));
        state.web_acls.retain(|_, v| v.arn != physical_id);
        Ok(())
    }

    pub(super) fn create_wafv2_ip_set(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let scope = props
            .get("Scope")
            .and_then(|v| v.as_str())
            .ok_or("Scope is required")?
            .to_string();
        self.check_wafv2_scope_region(&scope)?;
        let ip_address_version = props
            .get("IPAddressVersion")
            .and_then(|v| v.as_str())
            .ok_or("IPAddressVersion is required")?
            .to_string();
        let addresses: Vec<String> = props
            .get("Addresses")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);

        let id = Uuid::new_v4().to_string();
        let arn =
            fakecloud_wafv2::synth_arn(&self.account_id, &self.region, &scope, "ipset", &name, &id);
        let ip_set = IpSet {
            id: id.clone(),
            name: name.clone(),
            arn: arn.clone(),
            scope: scope.clone(),
            description,
            ip_address_version,
            addresses,
            lock_token: Uuid::new_v4().simple().to_string(),
            created_time: Utc::now(),
        };

        let mut accounts = self.wafv2_state.write();
        let state = accounts.region_mut(&self.account_id, &self.wafv2_arn_region(&arn));
        state.ip_sets.insert((scope, name.clone()), ip_set);

        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn)
            .with("Id", id)
            .with("Name", name))
    }

    pub(super) fn delete_wafv2_ip_set(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts.region_mut(&self.account_id, &self.wafv2_arn_region(physical_id));
        state.ip_sets.retain(|_, v| v.arn != physical_id);
        Ok(())
    }

    pub(super) fn create_wafv2_regex_pattern_set(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let scope = props
            .get("Scope")
            .and_then(|v| v.as_str())
            .ok_or("Scope is required")?
            .to_string();
        self.check_wafv2_scope_region(&scope)?;
        let regular_expressions: Vec<serde_json::Value> = props
            .get("RegularExpressionList")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .map(|s| {
                        if let Some(s) = s.as_str() {
                            serde_json::json!({"RegexString": s})
                        } else {
                            s.clone()
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);

        let id = Uuid::new_v4().to_string();
        let arn = fakecloud_wafv2::synth_arn(
            &self.account_id,
            &self.region,
            &scope,
            "regexpatternset",
            &name,
            &id,
        );
        let set = RegexPatternSet {
            id: id.clone(),
            name: name.clone(),
            arn: arn.clone(),
            scope: scope.clone(),
            description,
            regular_expressions,
            lock_token: Uuid::new_v4().simple().to_string(),
            created_time: Utc::now(),
        };

        let mut accounts = self.wafv2_state.write();
        let state = accounts.region_mut(&self.account_id, &self.wafv2_arn_region(&arn));
        state.regex_pattern_sets.insert((scope, name.clone()), set);

        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn)
            .with("Id", id)
            .with("Name", name))
    }

    pub(super) fn delete_wafv2_regex_pattern_set(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts.region_mut(&self.account_id, &self.wafv2_arn_region(physical_id));
        state.regex_pattern_sets.retain(|_, v| v.arn != physical_id);
        Ok(())
    }

    pub(super) fn create_wafv2_rule_group(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let scope = props
            .get("Scope")
            .and_then(|v| v.as_str())
            .ok_or("Scope is required")?
            .to_string();
        self.check_wafv2_scope_region(&scope)?;
        let capacity = props
            .get("Capacity")
            .and_then(|v| v.as_i64())
            .ok_or("Capacity is required")?;
        let description = props
            .get("Description")
            .and_then(|v| v.as_str())
            .map(String::from);
        let rules = props
            .get("Rules")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let visibility_config = props
            .get("VisibilityConfig")
            .cloned()
            .unwrap_or_else(|| serde_json::json!({}));

        let id = Uuid::new_v4().to_string();
        let arn = fakecloud_wafv2::synth_arn(
            &self.account_id,
            &self.region,
            &scope,
            "rulegroup",
            &name,
            &id,
        );
        let rg = RuleGroup {
            id: id.clone(),
            name: name.clone(),
            arn: arn.clone(),
            scope: scope.clone(),
            capacity,
            description,
            rules,
            visibility_config,
            lock_token: Uuid::new_v4().simple().to_string(),
            label_namespace: format!("awswaf:{}:rulegroup:{}:", self.account_id, name),
            custom_response_bodies: BTreeMap::new(),
            available_labels: Vec::new(),
            consumed_labels: Vec::new(),
            created_time: Utc::now(),
            monetization_config: None,
        };

        let mut accounts = self.wafv2_state.write();
        let state = accounts.region_mut(&self.account_id, &self.wafv2_arn_region(&arn));
        state.rule_groups.insert((scope, name.clone()), rg);

        Ok(ProvisionResult::new(arn.clone())
            .with("Arn", arn)
            .with("Id", id)
            .with("Name", name)
            .with("Capacity", capacity.to_string()))
    }

    pub(super) fn delete_wafv2_rule_group(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts.region_mut(&self.account_id, &self.wafv2_arn_region(physical_id));
        state.rule_groups.retain(|_, v| v.arn != physical_id);
        Ok(())
    }

    pub(super) fn create_wafv2_logging_configuration(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let resource_arn = props
            .get("ResourceArn")
            .and_then(|v| v.as_str())
            .ok_or("ResourceArn is required")?
            .to_string();
        let cfg = serde_json::json!({
            "ResourceArn": resource_arn,
            "LogDestinationConfigs": props.get("LogDestinationConfigs").cloned().unwrap_or_else(|| serde_json::json!([])),
            "RedactedFields": props.get("RedactedFields").cloned().unwrap_or_else(|| serde_json::json!([])),
            "LoggingFilter": props.get("LoggingFilter").cloned(),
        });

        let mut accounts = self.wafv2_state.write();
        let state = accounts.region_mut(&self.account_id, &self.wafv2_arn_region(&resource_arn));
        state.logging_configs.insert(resource_arn.clone(), cfg);

        Ok(ProvisionResult::new(resource_arn))
    }

    pub(super) fn delete_wafv2_logging_configuration(
        &self,
        physical_id: &str,
    ) -> Result<(), String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts.region_mut(&self.account_id, &self.wafv2_arn_region(physical_id));
        state.logging_configs.remove(physical_id);
        Ok(())
    }

    pub(super) fn create_wafv2_web_acl_association(
        &self,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let resource_arn = props
            .get("ResourceArn")
            .and_then(|v| v.as_str())
            .ok_or("ResourceArn is required")?
            .to_string();
        let web_acl_arn = props
            .get("WebACLArn")
            .and_then(|v| v.as_str())
            .ok_or("WebACLArn is required")?
            .to_string();

        // As AssociateWebACL (which CloudFormation calls in the stack's
        // region): the resource must be in that region, and so must the web
        // ACL; the association lives with the resource.
        let region = self.wafv2_arn_region(&resource_arn);
        if region != self.region {
            return Err(format!(
                "WAFNonexistentItemException: resource {resource_arn} is not in {}",
                self.region
            ));
        }
        let mut accounts = self.wafv2_state.write();
        let state = accounts.region_mut(&self.account_id, &region);
        if !state.web_acls.values().any(|a| a.arn == web_acl_arn) {
            return Err(format!(
                "WAFNonexistentItemException: web ACL {web_acl_arn} does not exist in {region}"
            ));
        }
        state.associations.insert(resource_arn.clone(), web_acl_arn);

        // Physical id encodes the resource arn so delete can find it.
        Ok(ProvisionResult::new(resource_arn))
    }

    pub(super) fn delete_wafv2_web_acl_association(&self, physical_id: &str) -> Result<(), String> {
        let mut accounts = self.wafv2_state.write();
        let state = accounts.region_mut(&self.account_id, &self.wafv2_arn_region(physical_id));
        state.associations.remove(physical_id);
        Ok(())
    }

    // --- In-place updates ---
    //
    // WAFv2 resources are keyed by (scope, name) and their physical id is the
    // ARN (with a random uuid), also referenced as Id/Arn by WebACL rules,
    // WebACLAssociation and CloudFront `WebACLId`. Reprovision (delete + create)
    // on a rules/addresses/patterns edit mints a NEW ARN/Id, dangling every
    // reference. Name/Scope are immutable, so a change to either still forces
    // replacement. These arms mutate the mutable config in place and bump the
    // lock token, preserving the ARN/Id.

    pub(super) fn update_wafv2_web_acl(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let scope = props
            .get("Scope")
            .and_then(|v| v.as_str())
            .ok_or("Scope is required")?
            .to_string();
        let updated = {
            let mut accounts = self.wafv2_state.write();
            let state = accounts.region_mut(
                &self.account_id,
                &self.wafv2_arn_region(&existing.physical_id),
            );
            match state.web_acls.get_mut(&(scope.clone(), name.clone())) {
                Some(acl) if acl.arn == existing.physical_id => {
                    acl.default_action = props
                        .get("DefaultAction")
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({"Allow": {}}));
                    acl.description = props
                        .get("Description")
                        .and_then(|v| v.as_str())
                        .map(String::from);
                    acl.rules = props
                        .get("Rules")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    acl.visibility_config = props
                        .get("VisibilityConfig")
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({}));
                    acl.lock_token = Uuid::new_v4().simple().to_string();
                    Some(
                        ProvisionResult::new(acl.arn.clone())
                            .with("Arn", acl.arn.clone())
                            .with("Id", acl.id.clone())
                            .with("Name", name.clone())
                            .with("Capacity", acl.capacity.to_string()),
                    )
                }
                _ => None,
            }
        };
        match updated {
            Some(result) => Ok(result),
            // Name/Scope changed (immutable) -> replacement.
            None => self
                .reprovision_resource(existing, resource)
                .map(|o| o.unwrap_or_else(|| ProvisionResult::new(existing.physical_id.clone()))),
        }
    }

    pub(super) fn update_wafv2_ip_set(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let scope = props
            .get("Scope")
            .and_then(|v| v.as_str())
            .ok_or("Scope is required")?
            .to_string();
        let updated = {
            let mut accounts = self.wafv2_state.write();
            let state = accounts.region_mut(
                &self.account_id,
                &self.wafv2_arn_region(&existing.physical_id),
            );
            match state.ip_sets.get_mut(&(scope.clone(), name.clone())) {
                Some(set) if set.arn == existing.physical_id => {
                    set.addresses = props
                        .get("Addresses")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|v| v.as_str().map(String::from))
                                .collect()
                        })
                        .unwrap_or_default();
                    set.description = props
                        .get("Description")
                        .and_then(|v| v.as_str())
                        .map(String::from);
                    set.lock_token = Uuid::new_v4().simple().to_string();
                    Some(
                        ProvisionResult::new(set.arn.clone())
                            .with("Arn", set.arn.clone())
                            .with("Id", set.id.clone())
                            .with("Name", name.clone()),
                    )
                }
                _ => None,
            }
        };
        match updated {
            Some(result) => Ok(result),
            None => self
                .reprovision_resource(existing, resource)
                .map(|o| o.unwrap_or_else(|| ProvisionResult::new(existing.physical_id.clone()))),
        }
    }

    pub(super) fn update_wafv2_regex_pattern_set(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let scope = props
            .get("Scope")
            .and_then(|v| v.as_str())
            .ok_or("Scope is required")?
            .to_string();
        let updated = {
            let mut accounts = self.wafv2_state.write();
            let state = accounts.region_mut(
                &self.account_id,
                &self.wafv2_arn_region(&existing.physical_id),
            );
            match state
                .regex_pattern_sets
                .get_mut(&(scope.clone(), name.clone()))
            {
                Some(set) if set.arn == existing.physical_id => {
                    set.regular_expressions = props
                        .get("RegularExpressionList")
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .map(|s| {
                                    if let Some(s) = s.as_str() {
                                        serde_json::json!({"RegexString": s})
                                    } else {
                                        s.clone()
                                    }
                                })
                                .collect()
                        })
                        .unwrap_or_default();
                    set.description = props
                        .get("Description")
                        .and_then(|v| v.as_str())
                        .map(String::from);
                    set.lock_token = Uuid::new_v4().simple().to_string();
                    Some(
                        ProvisionResult::new(set.arn.clone())
                            .with("Arn", set.arn.clone())
                            .with("Id", set.id.clone())
                            .with("Name", name.clone()),
                    )
                }
                _ => None,
            }
        };
        match updated {
            Some(result) => Ok(result),
            None => self
                .reprovision_resource(existing, resource)
                .map(|o| o.unwrap_or_else(|| ProvisionResult::new(existing.physical_id.clone()))),
        }
    }

    pub(super) fn update_wafv2_rule_group(
        &self,
        existing: &StackResource,
        resource: &ResourceDefinition,
    ) -> Result<ProvisionResult, String> {
        let props = &resource.properties;
        let name = props
            .get("Name")
            .and_then(|v| v.as_str())
            .ok_or("Name is required")?
            .to_string();
        let scope = props
            .get("Scope")
            .and_then(|v| v.as_str())
            .ok_or("Scope is required")?
            .to_string();
        let updated = {
            let mut accounts = self.wafv2_state.write();
            let state = accounts.region_mut(
                &self.account_id,
                &self.wafv2_arn_region(&existing.physical_id),
            );
            match state.rule_groups.get_mut(&(scope.clone(), name.clone())) {
                Some(rg) if rg.arn == existing.physical_id => {
                    rg.rules = props
                        .get("Rules")
                        .and_then(|v| v.as_array())
                        .cloned()
                        .unwrap_or_default();
                    rg.description = props
                        .get("Description")
                        .and_then(|v| v.as_str())
                        .map(String::from);
                    rg.visibility_config = props
                        .get("VisibilityConfig")
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!({}));
                    rg.lock_token = Uuid::new_v4().simple().to_string();
                    Some(
                        ProvisionResult::new(rg.arn.clone())
                            .with("Arn", rg.arn.clone())
                            .with("Id", rg.id.clone())
                            .with("Name", name.clone())
                            .with("Capacity", rg.capacity.to_string()),
                    )
                }
                _ => None,
            }
        };
        match updated {
            Some(result) => Ok(result),
            None => self
                .reprovision_resource(existing, resource)
                .map(|o| o.unwrap_or_else(|| ProvisionResult::new(existing.physical_id.clone()))),
        }
    }
}
