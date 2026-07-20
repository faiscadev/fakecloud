//! `Wafv2Service` `web_acls` family — extracted from service.rs by audit-2026-05-19.

use super::*;

impl Wafv2Service {
    pub(super) fn create_web_acl(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str_len(&body, "Name", 1, 128)?;
        let scope = require_scope(&body)?;
        let default_action = body
            .get("DefaultAction")
            .cloned()
            .ok_or_else(|| invalid_param("DefaultAction is required"))?;
        let visibility_config = body
            .get("VisibilityConfig")
            .cloned()
            .ok_or_else(|| invalid_param("VisibilityConfig is required"))?;
        let rules = body
            .get("Rules")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let custom_response_bodies = parse_custom_response_bodies(body.get("CustomResponseBodies"));
        let captcha_config = body.get("CaptchaConfig").cloned();
        let challenge_config = body.get("ChallengeConfig").cloned();
        let token_domains = parse_string_list(body.get("TokenDomains"));
        let association_config = body.get("AssociationConfig").cloned();
        let data_protection_config = body.get("DataProtectionConfig").cloned();
        let on_source_d_do_s_protection_config = body.get("OnSourceDDoSProtectionConfig").cloned();
        let application_config = body.get("ApplicationConfig").cloned();
        let tags = parse_tags(body.get("Tags"))?;

        let key = (scope.clone(), name.clone());
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if account.web_acls.contains_key(&key) {
            return Err(already_exists(&format!("WebACL {name} already exists")));
        }
        let id = synth_uuid();
        let arn = synth_arn(&req.account_id, &req.region, &scope, "webacl", &name, &id);
        let lock_token = synth_uuid();
        let capacity = compute_capacity(&rules);
        let label_namespace = format!("awswaf:{}:webacl:{name}:", req.account_id);
        let summary = web_acl_summary_json(&id, &name, &arn, description.as_deref(), &lock_token);
        let acl = WebAcl {
            id,
            name,
            arn: arn.clone(),
            scope: scope.clone(),
            default_action,
            description,
            rules,
            visibility_config,
            capacity,
            lock_token,
            label_namespace,
            custom_response_bodies,
            captcha_config,
            challenge_config,
            token_domains,
            association_config,
            data_protection_config,
            on_source_d_do_s_protection_config,
            application_config,
            retrofitted_by_firewall_manager: false,
            pre_process_firewall_manager_rule_groups: Vec::new(),
            post_process_firewall_manager_rule_groups: Vec::new(),
            managed_by_firewall_manager: false,
            created_time: Utc::now(),
        };
        account.web_acls.insert(key, acl);
        if !tags.is_empty() {
            account.tags.insert(arn, tags);
        }
        Ok(AwsResponse::ok_json(json!({ "Summary": summary })))
    }

    pub(super) fn get_web_acl(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let arn_in = body.get("ARN").and_then(Value::as_str).map(str::to_owned);
        let state = self.state.read();
        let account = state
            .accounts
            .get(&req.account_id)
            .ok_or_else(|| not_found("WebACL"))?;
        let acl = if let Some(arn) = arn_in.as_deref() {
            account
                .web_acls
                .values()
                .find(|a| a.arn == arn)
                .ok_or_else(|| not_found("WebACL"))?
        } else {
            let name = require_str(&body, "Name")?;
            let scope = require_scope(&body)?;
            account
                .web_acls
                .get(&(scope, name))
                .ok_or_else(|| not_found("WebACL"))?
        };
        let mut response = json!({
            "WebACL": web_acl_detail_json(acl),
            "LockToken": acl.lock_token,
        });
        // AWS only surfaces ApplicationIntegrationURL (the CAPTCHA/Challenge
        // integration endpoint) for a web ACL that declares token domains. The
        // provider always sends a default CaptchaConfig (immunity time) even for
        // a basic ACL, so gating on token domains — not the CAPTCHA config — is
        // what matches AWS; the Terraform resource asserts the URL is empty when
        // no token domains are configured.
        if !acl.token_domains.is_empty() {
            response.as_object_mut().unwrap().insert(
                "ApplicationIntegrationURL".to_string(),
                Value::String(format!(
                    "https://{}.{}.amazonaws.com/captcha",
                    acl.id, req.region
                )),
            );
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn list_web_acls(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let scope = require_scope(&body)?;
        validate_opt_limit(&body)?;
        validate_opt_next_marker(&body)?;
        let limit = body.get("Limit").and_then(Value::as_u64).unwrap_or(100) as usize;
        let next_marker = body
            .get("NextMarker")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let state = self.state.read();
        let mut all: Vec<WebAcl> = state
            .accounts
            .get(&req.account_id)
            .map(|a| {
                a.web_acls
                    .values()
                    .filter(|x| x.scope == scope)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        all.sort_by(|a, b| a.name.cmp(&b.name));
        let (page, next) = paginate_checked(&all, next_marker.as_deref(), limit)
            .map_err(|_| invalid_param("The specified NextMarker is not valid."))?;
        let summaries: Vec<Value> = page
            .iter()
            .map(|a| {
                web_acl_summary_json(
                    &a.id,
                    &a.name,
                    &a.arn,
                    a.description.as_deref(),
                    &a.lock_token,
                )
            })
            .collect();
        let mut response = json!({ "WebACLs": summaries });
        if let Some(t) = next {
            response
                .as_object_mut()
                .unwrap()
                .insert("NextMarker".to_string(), Value::String(t));
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn update_web_acl(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let scope = require_scope(&body)?;
        let id_in = require_str(&body, "Id")?;
        let lock_token_in = require_str(&body, "LockToken")?;
        let default_action = body
            .get("DefaultAction")
            .cloned()
            .ok_or_else(|| invalid_param("DefaultAction is required"))?;
        let visibility_config = body
            .get("VisibilityConfig")
            .cloned()
            .ok_or_else(|| invalid_param("VisibilityConfig is required"))?;
        let rules = body
            .get("Rules")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();
        let description = body
            .get("Description")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let acl = account
            .web_acls
            .get_mut(&(scope, name.clone()))
            .ok_or_else(|| not_found("WebACL"))?;
        if acl.id != id_in {
            return Err(invalid_param("Id does not match the named WebACL"));
        }
        if acl.lock_token != lock_token_in {
            return Err(stale_lock_token());
        }
        acl.default_action = default_action;
        acl.visibility_config = visibility_config;
        acl.capacity = compute_capacity(&rules);
        acl.rules = rules;
        acl.description = description;
        // AWS Update ops are full-replace: every optional member omitted from
        // the request is cleared (not left at its previous value).
        acl.custom_response_bodies = parse_custom_response_bodies(body.get("CustomResponseBodies"));
        acl.captcha_config = body.get("CaptchaConfig").cloned();
        acl.challenge_config = body.get("ChallengeConfig").cloned();
        acl.token_domains = parse_string_list(body.get("TokenDomains"));
        acl.association_config = body.get("AssociationConfig").cloned();
        acl.data_protection_config = body.get("DataProtectionConfig").cloned();
        acl.on_source_d_do_s_protection_config = body.get("OnSourceDDoSProtectionConfig").cloned();
        acl.application_config = body.get("ApplicationConfig").cloned();
        acl.lock_token = synth_uuid();
        Ok(AwsResponse::ok_json(
            json!({ "NextLockToken": acl.lock_token }),
        ))
    }

    pub(super) fn delete_web_acl(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let name = require_str(&body, "Name")?;
        let scope = require_scope(&body)?;
        let id_in = require_str(&body, "Id")?;
        let lock_token_in = require_str(&body, "LockToken")?;
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        let key = (scope, name);
        let acl = account
            .web_acls
            .get(&key)
            .ok_or_else(|| not_found("WebACL"))?;
        if acl.id != id_in {
            return Err(invalid_param("Id does not match the named WebACL"));
        }
        if acl.lock_token != lock_token_in {
            return Err(stale_lock_token());
        }
        let arn = acl.arn.clone();
        // Reject if any resource still associated (matches WAFAssociatedItemException).
        if account.associations.values().any(|v| *v == arn) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "WAFAssociatedItemException",
                "WebACL is still associated with resources",
            ));
        }
        account.web_acls.remove(&key);
        account.tags.remove(&arn);
        account.logging_configs.remove(&arn);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn associate_web_acl(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        // Enforce the documented ARN length bounds (20..2048) on both
        // sides — otherwise bad/empty inputs land directly in the
        // associations map and surface as stale entries.
        let acl_arn = require_str_len(&body, "WebACLArn", 20, 2048)?;
        let resource_arn =
            normalize_resource_arn(&require_str_len(&body, "ResourceArn", 20, 2048)?);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        if !account.web_acls.values().any(|a| a.arn == acl_arn) {
            return Err(not_found("WebACL"));
        }
        account.associations.insert(resource_arn, acl_arn);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn disassociate_web_acl(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_arn =
            normalize_resource_arn(&require_str_len(&body, "ResourceArn", 20, 2048)?);
        let mut state = self.state.write();
        let account = account_mut(&mut state, &req.account_id);
        account.associations.remove(&resource_arn);
        Ok(AwsResponse::ok_json(json!({})))
    }

    pub(super) fn get_web_acl_for_resource(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let resource_arn =
            normalize_resource_arn(&require_str_len(&body, "ResourceArn", 20, 2048)?);
        let state = self.state.read();
        let account = state.accounts.get(&req.account_id);
        let acl_arn = account.and_then(|a| a.associations.get(&resource_arn).cloned());
        let mut response = json!({});
        if let Some(arn) = acl_arn {
            if let Some(acl) = account.and_then(|a| a.web_acls.values().find(|x| x.arn == arn)) {
                response
                    .as_object_mut()
                    .unwrap()
                    .insert("WebACL".to_string(), web_acl_detail_json(acl));
            }
        }
        Ok(AwsResponse::ok_json(response))
    }

    pub(super) fn list_resources_for_web_acl(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let acl_arn = require_str_len(&body, "WebACLArn", 20, 2048)?;
        if let Some(rt) = body.get("ResourceType").and_then(Value::as_str) {
            validate_enum(
                rt,
                &[
                    "APPLICATION_LOAD_BALANCER",
                    "API_GATEWAY",
                    "APPSYNC",
                    "COGNITO_USER_POOL",
                    "APP_RUNNER_SERVICE",
                    "VERIFIED_ACCESS_INSTANCE",
                    "AMPLIFY",
                ],
                "ResourceType",
            )?;
        }
        let resource_type = body
            .get("ResourceType")
            .and_then(Value::as_str)
            .map(str::to_string);
        let state = self.state.read();
        let resources: Vec<String> = state
            .accounts
            .get(&req.account_id)
            .map(|a| {
                a.associations
                    .iter()
                    .filter(|(_, v)| **v == acl_arn)
                    .filter(|(k, _)| {
                        // When ResourceType is set, restrict the
                        // returned ARNs to that family. Without this
                        // filter, listing ALB resources also returns
                        // associated APIGW/AppSync/Amplify ARNs.
                        // AWS defaults ResourceType to
                        // APPLICATION_LOAD_BALANCER when omitted —
                        // returning every resource type would surface
                        // ARNs callers didn't ask for.
                        let rt = resource_type
                            .as_deref()
                            .unwrap_or("APPLICATION_LOAD_BALANCER");
                        resource_arn_matches_type(k, rt)
                    })
                    .map(|(k, _)| k.clone())
                    .collect()
            })
            .unwrap_or_default();
        Ok(AwsResponse::ok_json(json!({
            "ResourceArns": resources,
        })))
    }
}

/// Map a `ResourceType` filter to the ARN segment that identifies that
/// resource family. Mirrors the documented mapping in the WAFv2
/// API reference.
fn resource_arn_matches_type(arn: &str, ty: &str) -> bool {
    match ty {
        "APPLICATION_LOAD_BALANCER" => {
            arn.contains(":elasticloadbalancing:") && arn.contains(":loadbalancer/app/")
        }
        "API_GATEWAY" => arn.contains(":apigateway:") && arn.contains("/restapis/"),
        "APPSYNC" => arn.contains(":appsync:"),
        "COGNITO_USER_POOL" => arn.contains(":cognito-idp:") && arn.contains(":userpool/"),
        "APP_RUNNER_SERVICE" => arn.contains(":apprunner:"),
        "VERIFIED_ACCESS_INSTANCE" => {
            arn.contains(":ec2:") && arn.contains(":verified-access-instance/")
        }
        "AMPLIFY" => arn.contains(":amplify:"),
        _ => true,
    }
}
