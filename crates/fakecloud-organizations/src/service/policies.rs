//! `OrganizationsService` `policies` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl OrganizationsService {
    pub(super) fn create_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = required_str(&body, "Name")?;
        let policy_type = required_str(&body, "Type")?;
        let content = required_str(&body, "Content")?;
        // An out-of-enum Type is a malformed request; a valid enum value that
        // fakecloud doesn't manage isn't enabled for the org. Both are the
        // AWS-documented responses (CreatePolicy declares neither
        // PolicyTypeNotSupportedException — that isn't a real Organizations
        // error code).
        if !is_valid_policy_type(policy_type) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidInputException",
                format!("You specified an invalid value for the Type parameter: {policy_type}"),
            ));
        }
        if !is_known_policy_type(policy_type) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "PolicyTypeNotAvailableForOrganizationException",
                format!("The {policy_type} policy type is not available for this organization."),
            ));
        }
        let description = body
            .get("Description")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let mut guard = self.state.write();
        self.require_member_management(&guard, &req.account_id)?;
        let org = guard.as_mut().unwrap();
        let policy = org
            .create_policy(name, description, content, policy_type)
            .map_err(org_error_to_aws)?;
        // Apply create-time Tags so ListTagsForResource reflects them without a
        // follow-up TagResource (bug-audit 2026-06-20, 1.24).
        let tags = parse_tags(body.get("Tags"));
        if !tags.is_empty() {
            org.set_resource_tags(&policy.id, &tags);
        }
        Ok(AwsResponse::ok_json(
            json!({ "Policy": policy_with_content(&policy) }),
        ))
    }

    pub(super) fn update_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let policy_id = required_str(&body, "PolicyId")?;
        let name = body.get("Name").and_then(|v| v.as_str());
        let description = body.get("Description").and_then(|v| v.as_str());
        let content = body.get("Content").and_then(|v| v.as_str());
        let mut guard = self.state.write();
        self.require_member_management(&guard, &req.account_id)?;
        let org = guard.as_mut().unwrap();
        let policy = org
            .update_policy(policy_id, name, description, content)
            .map_err(org_error_to_aws)?;
        Ok(AwsResponse::ok_json(
            json!({ "Policy": policy_with_content(&policy) }),
        ))
    }

    pub(super) fn delete_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let policy_id = required_str(&body, "PolicyId")?;
        let mut guard = self.state.write();
        self.require_member_management(&guard, &req.account_id)?;
        let org = guard.as_mut().unwrap();
        org.delete_policy(policy_id).map_err(org_error_to_aws)?;
        Ok(AwsResponse::ok_json(Value::Null))
    }

    pub(super) fn describe_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let policy_id = required_str(&body, "PolicyId")?;
        let guard = self.state.read();
        let org = self.require_member(&guard, &req.account_id)?;
        let policy = org
            .policies
            .get(policy_id)
            .ok_or_else(|| org_error_to_aws(OrgError::PolicyNotFound(policy_id.to_string())))?;
        Ok(AwsResponse::ok_json(
            json!({ "Policy": policy_with_content(policy) }),
        ))
    }

    pub(super) fn list_policies(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Filter is a required parameter on the AWS API. Reject missing
        // filter so the SDK wire format matches and callers learn about
        // their typo rather than getting an implicit SCP default.
        let filter = required_str(&body, "Filter")?;
        if !is_valid_policy_type(filter) {
            return Err(invalid_policy_filter(filter));
        }
        let guard = self.state.read();
        let org = self.require_member(&guard, &req.account_id)?;
        let mut policies: Vec<&Policy> = org
            .policies
            .values()
            .filter(|p| p.policy_type == filter)
            .collect();
        policies.sort_by(|a, b| a.name.cmp(&b.name));
        let summaries: Vec<Value> = policies.iter().map(|p| policy_summary(p)).collect();
        Ok(AwsResponse::ok_json(json!({ "Policies": summaries })))
    }

    pub(super) fn attach_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let policy_id = required_str(&body, "PolicyId")?;
        let target_id = required_str(&body, "TargetId")?;
        let mut guard = self.state.write();
        self.require_member_management(&guard, &req.account_id)?;
        let org = guard.as_mut().unwrap();
        org.attach_policy(policy_id, target_id)
            .map_err(org_error_to_aws)?;
        Ok(AwsResponse::ok_json(Value::Null))
    }

    pub(super) fn detach_policy(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let policy_id = required_str(&body, "PolicyId")?;
        let target_id = required_str(&body, "TargetId")?;
        let mut guard = self.state.write();
        self.require_member_management(&guard, &req.account_id)?;
        let org = guard.as_mut().unwrap();
        org.detach_policy(policy_id, target_id)
            .map_err(org_error_to_aws)?;
        Ok(AwsResponse::ok_json(Value::Null))
    }

    pub(super) fn list_policies_for_target(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let target_id = required_str(&body, "TargetId")?;
        let filter = required_str(&body, "Filter")?;
        if !is_valid_policy_type(filter) {
            return Err(invalid_policy_filter(filter));
        }
        let guard = self.state.read();
        let org = self.require_member(&guard, &req.account_id)?;
        let mut policies = org
            .policies_for_target(target_id)
            .map_err(org_error_to_aws)?;
        policies.sort_by(|a, b| a.name.cmp(&b.name));
        let summaries: Vec<Value> = policies.iter().map(|p| policy_summary(p)).collect();
        Ok(AwsResponse::ok_json(json!({ "Policies": summaries })))
    }

    pub(super) fn list_targets_for_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let policy_id = required_str(&body, "PolicyId")?;
        let guard = self.state.read();
        let org = self.require_member(&guard, &req.account_id)?;
        let targets = org
            .targets_for_policy(policy_id)
            .map_err(org_error_to_aws)?;
        let payload: Vec<Value> = targets
            .iter()
            .map(|(id, name, ttype)| {
                json!({
                    "TargetId": id,
                    "Name": name,
                    "Type": ttype,
                    "Arn": target_arn(org, id, ttype),
                })
            })
            .collect();
        Ok(AwsResponse::ok_json(json!({ "Targets": payload })))
    }

    pub(super) fn enable_policy_type(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let policy_type = required_str(&body, "PolicyType")?.to_string();
        let mut guard = self.state.write();
        self.require_member_management(&guard, &req.account_id)?;
        let org = guard.as_mut().expect("management gate proved Some");
        org.enable_policy_type(&policy_type);
        let policy_types: Vec<Value> = org
            .list_policy_type_statuses()
            .into_iter()
            .filter(|(_, status)| status == "ENABLED")
            .map(|(t, status)| json!({"Type": t, "Status": status}))
            .collect();
        Ok(AwsResponse::ok_json(json!({
            "Root": {
                "Id": org.root_id,
                "Arn": org.root_arn,
                "Name": org.root_name,
                "PolicyTypes": policy_types,
            }
        })))
    }

    pub(super) fn disable_policy_type(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let policy_type = required_str(&body, "PolicyType")?.to_string();
        let mut guard = self.state.write();
        self.require_member_management(&guard, &req.account_id)?;
        let org = guard.as_mut().expect("management gate proved Some");
        org.disable_policy_type(&policy_type)
            .map_err(org_error_to_aws)?;
        let policy_types: Vec<Value> = org
            .list_policy_type_statuses()
            .into_iter()
            .filter(|(_, status)| status == "ENABLED")
            .map(|(t, status)| json!({"Type": t, "Status": status}))
            .collect();
        Ok(AwsResponse::ok_json(json!({
            "Root": {
                "Id": org.root_id,
                "Arn": org.root_arn,
                "Name": org.root_name,
                "PolicyTypes": policy_types,
            }
        })))
    }

    pub(super) fn describe_effective_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let policy_type = required_str(&body, "PolicyType")?.to_string();
        let target_id = body
            .get("TargetId")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| req.account_id.clone());
        let guard = self.state.read();
        let org = guard.as_ref().ok_or_else(organizations_not_in_use)?;
        // The effective policy is the union of every policy of `policy_type`
        // attached up the org hierarchy from `target_id` to root. We
        // present it as a single Statement[] union so callers can audit.
        let mut statements: Vec<Value> = Vec::new();
        for ancestor in ancestors_for(org, &target_id) {
            if let Some(policy_ids) = org.attachments.get(&ancestor) {
                for pid in policy_ids {
                    if let Some(policy) = org.policies.get(pid) {
                        if policy.policy_type == policy_type {
                            if let Ok(content) = serde_json::from_str::<Value>(&policy.content) {
                                if let Some(arr) =
                                    content.get("Statement").and_then(|v| v.as_array())
                                {
                                    statements.extend(arr.iter().cloned());
                                }
                            }
                        }
                    }
                }
            }
        }
        let merged = json!({"Version": "2012-10-17", "Statement": statements});
        let payload = json!({
            "EffectivePolicy": {
                "PolicyType": policy_type,
                "TargetId": target_id,
                "PolicyContent": merged.to_string(),
                "LastUpdatedTimestamp": Utc::now().timestamp() as f64,
            }
        });
        Ok(AwsResponse::ok_json(payload))
    }

    pub(super) fn put_resource_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let content = required_str(&body, "Content")?.to_string();
        // Reject malformed JSON up front.
        serde_json::from_str::<Value>(&content).map_err(|_| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "InvalidInputException",
                "Content must be valid JSON",
            )
        })?;
        let mut guard = self.state.write();
        self.require_member_management(&guard, &req.account_id)?;
        let org = guard.as_mut().expect("management gate proved Some");
        org.resource_policy = Some(content);
        let payload = json!({
            "ResourcePolicy": {
                "ResourcePolicySummary": {
                    "Id": "rp-fakecloud",
                    "Arn": format!(
                        "arn:aws:organizations::{}:resourcepolicy/{}/rp-fakecloud",
                        org.management_account_id, org.org_id
                    ),
                },
                "Content": org.resource_policy.clone(),
            }
        });
        Ok(AwsResponse::ok_json(payload))
    }

    pub(super) fn delete_resource_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut guard = self.state.write();
        self.require_member_management(&guard, &req.account_id)?;
        let org = guard.as_mut().expect("management gate proved Some");
        org.resource_policy = None;
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn describe_resource_policy(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let guard = self.state.read();
        let org = guard.as_ref().ok_or_else(organizations_not_in_use)?;
        let content = org.resource_policy.clone().ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "ResourcePolicyNotFoundException",
                "No resource policy is attached to this organization.",
            )
        })?;
        Ok(AwsResponse::ok_json(json!({
            "ResourcePolicy": {
                "ResourcePolicySummary": {
                    "Id": "rp-fakecloud",
                    "Arn": format!(
                        "arn:aws:organizations::{}:resourcepolicy/{}/rp-fakecloud",
                        org.management_account_id, org.org_id
                    ),
                },
                "Content": content,
            }
        })))
    }

    /// `ListAccountsWithInvalidEffectivePolicy` reports member accounts
    /// whose effective policy of `PolicyType` failed validation. fakecloud
    /// stores well-formed policies only, so no account ever has an invalid
    /// effective policy — the honest answer is an empty list. The
    /// `PolicyType` is echoed back per the AWS response shape.
    pub(super) fn list_accounts_with_invalid_effective_policy(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let policy_type = required_str(&body, "PolicyType")?.to_string();
        let guard = self.state.read();
        self.require_member(&guard, &req.account_id)?;
        Ok(AwsResponse::ok_json(json!({
            "Accounts": [],
            "PolicyType": policy_type,
        })))
    }

    /// `ListEffectivePolicyValidationErrors` reports the validation errors
    /// for one account's effective policy of `PolicyType`. With only
    /// well-formed policies stored, there are no errors — return the
    /// account/type echo and an empty error list.
    pub(super) fn list_effective_policy_validation_errors(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let account_id = required_str(&body, "AccountId")?.to_string();
        let policy_type = required_str(&body, "PolicyType")?.to_string();
        let guard = self.state.read();
        self.require_member(&guard, &req.account_id)?;
        Ok(AwsResponse::ok_json(json!({
            "AccountId": account_id,
            "PolicyType": policy_type,
            "EffectivePolicyValidationErrors": [],
        })))
    }
}
