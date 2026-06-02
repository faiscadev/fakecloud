use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{
    CustomVerificationEmailTemplate, DedicatedIp, DedicatedIpPool, DeliverabilityTestReport,
    ExportJob, ImportJob, MultiRegionEndpoint, ReputationEntityState, SentEmail, SesState,
    SubscribedDomain, Tenant, TenantResourceAssociation, VdmRecommendation,
};

use super::SesV2Service;

impl SesV2Service {
    // --- Tag operations ---

    /// Validate that a resource ARN refers to an existing resource.
    /// Returns `None` if the resource exists, or `Some(error_response)` if not.
    pub(super) fn validate_resource_arn(&self, arn: &str, req: &AwsRequest) -> Option<AwsResponse> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        // Parse ARN: arn:aws:ses:{region}:{account}:{resource-type}/{name}
        let parts: Vec<&str> = arn.split(':').collect();
        if parts.len() < 6 {
            return Some(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Resource not found: {arn}"),
            ));
        }

        let resource = parts[5..].join(":");
        let found = if let Some(name) = resource.strip_prefix("identity/") {
            state.identities.contains_key(name)
        } else if let Some(name) = resource.strip_prefix("configuration-set/") {
            state.configuration_sets.contains_key(name)
        } else if let Some(name) = resource.strip_prefix("contact-list/") {
            state.contact_lists.contains_key(name)
        } else {
            false
        };

        if found {
            None
        } else {
            Some(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Resource not found: {arn}"),
            ))
        }
    }

    pub(super) fn tag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let arn = match body["ResourceArn"].as_str() {
            Some(a) => a.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ResourceArn is required",
                ));
            }
        };

        let tags_arr = match body["Tags"].as_array() {
            Some(arr) => arr,
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "Tags is required",
                ));
            }
        };

        if let Some(resp) = self.validate_resource_arn(&arn, req) {
            return Ok(resp);
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let tag_map = state.tags.entry(arn).or_default();
        for tag in tags_arr {
            if let (Some(k), Some(v)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                tag_map.insert(k.to_string(), v.to_string());
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn untag_resource(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        // ResourceArn and TagKeys come as query params
        let arn = match req.query_params.get("ResourceArn") {
            Some(a) => a.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ResourceArn is required",
                ));
            }
        };

        if let Some(resp) = self.validate_resource_arn(&arn, req) {
            return Ok(resp);
        }

        // Parse TagKeys from raw query string (supports repeated params)
        let tag_keys: Vec<String> = form_urlencoded::parse(req.raw_query.as_bytes())
            .filter(|(k, _)| k == "TagKeys")
            .map(|(_, v)| v.into_owned())
            .collect();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if let Some(tag_map) = state.tags.get_mut(&arn) {
            for key in &tag_keys {
                tag_map.remove(key);
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn list_tags_for_resource(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let arn = match req.query_params.get("ResourceArn") {
            Some(a) => a.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ResourceArn is required",
                ));
            }
        };

        if let Some(resp) = self.validate_resource_arn(&arn, req) {
            return Ok(resp);
        }

        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let tags = state.tags.get(&arn);
        let tags_json = match tags {
            Some(t) => fakecloud_core::tags::tags_to_json(t, "Key", "Value"),
            None => vec![],
        };

        let response = json!({
            "Tags": tags_json,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    // --- Custom Verification Email Template operations ---

    pub(super) fn create_custom_verification_email_template(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let template_name = match body["TemplateName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TemplateName is required",
                ));
            }
        };
        if template_name.is_empty() {
            return Ok(Self::json_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "TemplateName must not be empty",
            ));
        }

        let from_email = body["FromEmailAddress"].as_str().unwrap_or("").to_string();
        let subject = body["TemplateSubject"].as_str().unwrap_or("").to_string();
        let content = body["TemplateContent"].as_str().unwrap_or("").to_string();
        let success_url = body["SuccessRedirectionURL"]
            .as_str()
            .unwrap_or("")
            .to_string();
        let failure_url = body["FailureRedirectionURL"]
            .as_str()
            .unwrap_or("")
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if state
            .custom_verification_email_templates
            .contains_key(&template_name)
        {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!(
                    "Custom verification email template {} already exists",
                    template_name
                ),
            ));
        }

        state.custom_verification_email_templates.insert(
            template_name.clone(),
            CustomVerificationEmailTemplate {
                template_name: template_name.clone(),
                from_email_address: from_email,
                template_subject: subject,
                template_content: content,
                success_redirection_url: success_url,
                failure_redirection_url: failure_url,
                created_at: Utc::now(),
            },
        );

        // Persist Tags via the per-ARN tag map. The Smithy
        // `GetCustomVerificationEmailTemplateResponse` round-trips Tags,
        // so dropping them on Create is a real input-drop bug. Replace
        // rather than merge so a Create after Delete (or a Create that
        // omits Tags) doesn't inherit stale entries from a previous
        // incarnation of the ARN.
        let arn = format!(
            "arn:aws:ses:{}:{}:custom-verification-email-template/{}",
            req.region, req.account_id, template_name
        );
        if let Some(tags_arr) = body["Tags"].as_array() {
            let mut tag_map = std::collections::BTreeMap::new();
            for tag in tags_arr {
                if let (Some(k), Some(v)) = (tag["Key"].as_str(), tag["Value"].as_str()) {
                    tag_map.insert(k.to_string(), v.to_string());
                }
            }
            state.tags.insert(arn, tag_map);
        } else {
            state.tags.remove(&arn);
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn get_custom_verification_email_template(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let tmpl = match state.custom_verification_email_templates.get(name) {
            Some(t) => t,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Custom verification email template {} does not exist", name),
                ));
            }
        };

        let mut response = json!({
            "TemplateName": tmpl.template_name,
            "FromEmailAddress": tmpl.from_email_address,
            "TemplateSubject": tmpl.template_subject,
            "TemplateContent": tmpl.template_content,
            "SuccessRedirectionURL": tmpl.success_redirection_url,
            "FailureRedirectionURL": tmpl.failure_redirection_url,
        });

        let arn = format!(
            "arn:aws:ses:{}:{}:custom-verification-email-template/{}",
            req.region, req.account_id, name
        );
        if let Some(tag_map) = state.tags.get(&arn) {
            response["Tags"] =
                Value::Array(fakecloud_core::tags::tags_to_json(tag_map, "Key", "Value"));
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn list_custom_verification_email_templates(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let page_size: usize = req
            .query_params
            .get("PageSize")
            .and_then(|s| s.parse().ok())
            .unwrap_or(20);

        let mut templates: Vec<&CustomVerificationEmailTemplate> =
            state.custom_verification_email_templates.values().collect();
        templates.sort_by(|a, b| a.template_name.cmp(&b.template_name));

        let next_token = req.query_params.get("NextToken");
        let start_idx = if let Some(token) = next_token {
            templates
                .iter()
                .position(|t| t.template_name == *token)
                .unwrap_or(0)
        } else {
            0
        };

        let page: Vec<Value> = templates
            .iter()
            .skip(start_idx)
            .take(page_size)
            .map(|t| {
                json!({
                    "TemplateName": t.template_name,
                    "FromEmailAddress": t.from_email_address,
                    "TemplateSubject": t.template_subject,
                    "SuccessRedirectionURL": t.success_redirection_url,
                    "FailureRedirectionURL": t.failure_redirection_url,
                })
            })
            .collect();

        let mut response = json!({
            "CustomVerificationEmailTemplates": page,
        });

        // Set NextToken if there are more results
        if start_idx + page_size < templates.len() {
            if let Some(next) = templates.get(start_idx + page_size) {
                response["NextToken"] = json!(next.template_name);
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn update_custom_verification_email_template(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let tmpl = match state.custom_verification_email_templates.get_mut(name) {
            Some(t) => t,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Custom verification email template {} does not exist", name),
                ));
            }
        };

        if let Some(from) = body["FromEmailAddress"].as_str() {
            tmpl.from_email_address = from.to_string();
        }
        if let Some(subject) = body["TemplateSubject"].as_str() {
            tmpl.template_subject = subject.to_string();
        }
        if let Some(content) = body["TemplateContent"].as_str() {
            tmpl.template_content = content.to_string();
        }
        if let Some(url) = body["SuccessRedirectionURL"].as_str() {
            tmpl.success_redirection_url = url.to_string();
        }
        if let Some(url) = body["FailureRedirectionURL"].as_str() {
            tmpl.failure_redirection_url = url.to_string();
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn delete_custom_verification_email_template(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if state
            .custom_verification_email_templates
            .remove(name)
            .is_none()
        {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Custom verification email template {} does not exist", name),
            ));
        }

        let arn = format!(
            "arn:aws:ses:{}:{}:custom-verification-email-template/{}",
            req.region, req.account_id, name
        );
        state.tags.remove(&arn);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn send_custom_verification_email(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let email_address = match body["EmailAddress"].as_str() {
            Some(e) => e.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "EmailAddress is required",
                ));
            }
        };

        let template_name = match body["TemplateName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TemplateName is required",
                ));
            }
        };

        // Verify template exists, then gate on the template's
        // FromEmailAddress matching a verified identity. Real SES v2
        // raises `MailFromDomainNotVerifiedException` from the
        // SendCustomVerificationEmail action when the from-address has
        // no matching verified email/domain identity.
        let from_email = {
            let accounts = self.state.read();
            let empty = SesState::new(&req.account_id, &req.region);
            let state = accounts.get(&req.account_id).unwrap_or(&empty);
            let Some(tmpl) = state
                .custom_verification_email_templates
                .get(&template_name)
            else {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!(
                        "Custom verification email template {} does not exist",
                        template_name
                    ),
                ));
            };
            tmpl.from_email_address.clone()
        };

        if let Some(err) = self.reject_unverified_sender(&req.account_id, &from_email) {
            return Ok(err);
        }

        let message_id = uuid::Uuid::new_v4().to_string();

        // Store as a sent email for introspection
        let sent = SentEmail {
            message_id: message_id.clone(),
            from: from_email,
            to: vec![email_address],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: Some(format!("Custom verification: {}", template_name)),
            html_body: None,
            text_body: None,
            raw_data: None,
            template_name: Some(template_name),
            template_data: None,
            dkim_signature: None,
            headers: Vec::new(),
            timestamp: Utc::now(),
            email_tags: Vec::new(),
            delivery_insights: Vec::new(),
        };

        self.state
            .write()
            .get_or_create(&req.account_id)
            .sent_emails
            .push(sent);

        let response = json!({
            "MessageId": message_id,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    // ── Dedicated IP Pools ──────────────────────────────────────────────

    pub(super) fn create_dedicated_ip_pool(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let pool_name = match body["PoolName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "PoolName is required",
                ));
            }
        };
        let scaling_mode = body["ScalingMode"]
            .as_str()
            .unwrap_or("STANDARD")
            .to_string();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if state.dedicated_ip_pools.contains_key(&pool_name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Pool {} already exists", pool_name),
            ));
        }

        // For MANAGED pools, generate some fake IPs
        if scaling_mode == "MANAGED" {
            let pool_idx = state.dedicated_ip_pools.len() as u8;
            for i in 1..=3 {
                let ip_addr = format!("198.51.100.{}", pool_idx * 10 + i);
                state.dedicated_ips.insert(
                    ip_addr.clone(),
                    DedicatedIp {
                        ip: ip_addr,
                        warmup_status: "NOT_APPLICABLE".to_string(),
                        warmup_percentage: -1,
                        pool_name: pool_name.clone(),
                    },
                );
            }
        }

        state.dedicated_ip_pools.insert(
            pool_name.clone(),
            DedicatedIpPool {
                pool_name,
                scaling_mode,
            },
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn list_dedicated_ip_pools(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let pools: Vec<&str> = state
            .dedicated_ip_pools
            .keys()
            .map(|k| k.as_str())
            .collect();
        let response = json!({ "DedicatedIpPools": pools });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn delete_dedicated_ip_pool(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state.dedicated_ip_pools.remove(name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Pool {} does not exist", name),
            ));
        }
        // Remove IPs associated with this pool
        state.dedicated_ips.retain(|_, ip| ip.pool_name != name);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn put_dedicated_ip_pool_scaling_attributes(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let scaling_mode = match body["ScalingMode"].as_str() {
            Some(m) => m.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ScalingMode is required",
                ));
            }
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let pool = match state.dedicated_ip_pools.get_mut(name) {
            Some(p) => p,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Pool {} does not exist", name),
                ));
            }
        };

        if pool.scaling_mode == "MANAGED" && scaling_mode == "STANDARD" {
            return Ok(Self::json_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Cannot change scaling mode from MANAGED to STANDARD",
            ));
        }

        let old_mode = pool.scaling_mode.clone();
        pool.scaling_mode = scaling_mode.clone();

        // If changing from STANDARD to MANAGED, generate IPs
        if old_mode == "STANDARD" && scaling_mode == "MANAGED" {
            let pool_idx = state.dedicated_ip_pools.len() as u8;
            for i in 1..=3u8 {
                let ip_addr = format!("198.51.100.{}", pool_idx * 10 + i);
                state.dedicated_ips.insert(
                    ip_addr.clone(),
                    DedicatedIp {
                        ip: ip_addr,
                        warmup_status: "NOT_APPLICABLE".to_string(),
                        warmup_percentage: -1,
                        pool_name: name.to_string(),
                    },
                );
            }
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ── Dedicated IPs ───────────────────────────────────────────────────

    pub(super) fn get_dedicated_ip(
        &self,
        ip: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let dip = match state.dedicated_ips.get(ip) {
            Some(d) => d,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Dedicated IP {} does not exist", ip),
                ));
            }
        };
        let response = json!({
            "DedicatedIp": {
                "Ip": dip.ip,
                "WarmupStatus": dip.warmup_status,
                "WarmupPercentage": dip.warmup_percentage,
                "PoolName": dip.pool_name,
            }
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn get_dedicated_ips(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let pool_filter = req.query_params.get("PoolName").map(|s| s.as_str());
        let ips: Vec<Value> = state
            .dedicated_ips
            .values()
            .filter(|ip| match pool_filter {
                Some(pool) => ip.pool_name == pool,
                None => true,
            })
            .map(|ip| {
                json!({
                    "Ip": ip.ip,
                    "WarmupStatus": ip.warmup_status,
                    "WarmupPercentage": ip.warmup_percentage,
                    "PoolName": ip.pool_name,
                })
            })
            .collect();
        let response = json!({ "DedicatedIps": ips });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn put_dedicated_ip_in_pool(
        &self,
        ip: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let dest_pool = match body["DestinationPoolName"].as_str() {
            Some(p) => p.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "DestinationPoolName is required",
                ));
            }
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if !state.dedicated_ip_pools.contains_key(&dest_pool) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Pool {} does not exist", dest_pool),
            ));
        }

        let dip = match state.dedicated_ips.get_mut(ip) {
            Some(d) => d,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Dedicated IP {} does not exist", ip),
                ));
            }
        };
        dip.pool_name = dest_pool;
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn put_dedicated_ip_warmup_attributes(
        &self,
        ip: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let warmup_pct = match body["WarmupPercentage"].as_i64() {
            Some(p) => p as i32,
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "WarmupPercentage is required",
                ));
            }
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let dip = match state.dedicated_ips.get_mut(ip) {
            Some(d) => d,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Dedicated IP {} does not exist", ip),
                ));
            }
        };
        dip.warmup_percentage = warmup_pct;
        dip.warmup_status = if warmup_pct >= 100 {
            "DONE".to_string()
        } else {
            "IN_PROGRESS".to_string()
        };
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // ── Multi-region Endpoints ──────────────────────────────────────────

    pub(super) fn create_multi_region_endpoint(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let endpoint_name = match body["EndpointName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "EndpointName is required",
                ));
            }
        };
        if endpoint_name.is_empty() || endpoint_name.len() > 64 {
            return Ok(Self::json_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "EndpointName length must be between 1 and 64",
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state.multi_region_endpoints.contains_key(&endpoint_name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Endpoint {} already exists", endpoint_name),
            ));
        }

        // Extract regions from Details.RoutesDetails[].Region
        let mut regions = Vec::new();
        if let Some(details) = body.get("Details") {
            if let Some(routes) = details["RoutesDetails"].as_array() {
                for r in routes {
                    if let Some(region) = r["Region"].as_str() {
                        regions.push(region.to_string());
                    }
                }
            }
        }
        // The primary region is always the current region
        if !regions.contains(&state.region) {
            regions.insert(0, state.region.clone());
        }

        let endpoint_id = format!(
            "ses-{}-{}",
            state.region,
            uuid::Uuid::new_v4().to_string().split('-').next().unwrap()
        );
        let now = Utc::now();

        state.multi_region_endpoints.insert(
            endpoint_name.clone(),
            MultiRegionEndpoint {
                endpoint_name,
                endpoint_id: endpoint_id.clone(),
                status: "READY".to_string(),
                regions,
                created_at: now,
                last_updated_at: now,
            },
        );

        let response = json!({
            "Status": "READY",
            "EndpointId": endpoint_id,
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn get_multi_region_endpoint(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Self::require_nonempty("EndpointName", name)?;
        if name.len() > 64 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "EndpointName must be 64 characters or fewer",
            ));
        }
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let ep = match state.multi_region_endpoints.get(name) {
            Some(e) => e,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Endpoint {} does not exist", name),
                ));
            }
        };

        let routes: Vec<Value> = ep.regions.iter().map(|r| json!({ "Region": r })).collect();

        let response = json!({
            "EndpointName": ep.endpoint_name,
            "EndpointId": ep.endpoint_id,
            "Status": ep.status,
            "Routes": routes,
            "CreatedTimestamp": ep.created_at.timestamp() as f64,
            "LastUpdatedTimestamp": ep.last_updated_at.timestamp() as f64,
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn list_multi_region_endpoints(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // Validate paging query params against the Smithy constraints
        // (NextTokenV2: 1..=5000 chars; PageSizeV2: 1..=1000).
        for kv in req.raw_query.split('&') {
            let (k, v) = match kv.split_once('=') {
                Some(p) => p,
                None => (kv, ""),
            };
            if k.is_empty() {
                continue;
            }
            match k {
                "NextToken" => {
                    let decoded = percent_encoding::percent_decode_str(v)
                        .decode_utf8_lossy()
                        .into_owned();
                    if decoded.is_empty() || decoded.len() > 5000 {
                        return Err(AwsServiceError::aws_error(
                            StatusCode::BAD_REQUEST,
                            "BadRequestException",
                            "NextToken length must be between 1 and 5000",
                        ));
                    }
                }
                "PageSize" => {
                    let parsed = v.parse::<i64>().ok();
                    match parsed {
                        Some(n) if (1..=1000).contains(&n) => {}
                        _ => {
                            return Err(AwsServiceError::aws_error(
                                StatusCode::BAD_REQUEST,
                                "BadRequestException",
                                "PageSize must be between 1 and 1000",
                            ));
                        }
                    }
                }
                _ => {}
            }
        }
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let endpoints: Vec<Value> = state
            .multi_region_endpoints
            .values()
            .map(|ep| {
                json!({
                    "EndpointName": ep.endpoint_name,
                    "EndpointId": ep.endpoint_id,
                    "Status": ep.status,
                    "Regions": ep.regions,
                    "CreatedTimestamp": ep.created_at.timestamp() as f64,
                    "LastUpdatedTimestamp": ep.last_updated_at.timestamp() as f64,
                })
            })
            .collect();
        let response = json!({ "MultiRegionEndpoints": endpoints });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn delete_multi_region_endpoint(
        &self,
        name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Self::require_nonempty("EndpointName", name)?;
        if name.len() > 64 {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "EndpointName must be 64 characters or fewer",
            ));
        }
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state.multi_region_endpoints.remove(name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Endpoint {} does not exist", name),
            ));
        }
        let response = json!({ "Status": "DELETING" });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    // --- Import Job operations ---

    pub(super) fn create_import_job(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let import_destination = match body.get("ImportDestination") {
            Some(v) if v.is_object() => v.clone(),
            _ => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ImportDestination is required",
                ));
            }
        };

        let import_data_source = match body.get("ImportDataSource") {
            Some(v) if v.is_object() => v.clone(),
            _ => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ImportDataSource is required",
                ));
            }
        };

        let job_id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();

        let job = ImportJob {
            job_id: job_id.clone(),
            import_destination,
            import_data_source,
            job_status: "COMPLETED".to_string(),
            created_timestamp: now,
            completed_timestamp: Some(now),
            processed_records_count: 0,
            failed_records_count: 0,
        };

        self.state
            .write()
            .get_or_create(&req.account_id)
            .import_jobs
            .insert(job_id.clone(), job);

        let response = json!({ "JobId": job_id });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn get_import_job(
        &self,
        job_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let job = match state.import_jobs.get(job_id) {
            Some(j) => j,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Import job {} does not exist", job_id),
                ));
            }
        };

        let mut response = json!({
            "JobId": job.job_id,
            "ImportDestination": job.import_destination,
            "ImportDataSource": job.import_data_source,
            "JobStatus": job.job_status,
            "CreatedTimestamp": job.created_timestamp.timestamp() as f64,
            "ProcessedRecordsCount": job.processed_records_count,
            "FailedRecordsCount": job.failed_records_count,
        });
        if let Some(ref ts) = job.completed_timestamp {
            response["CompletedTimestamp"] = json!(ts.timestamp() as f64);
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn list_import_jobs(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
        let filter_type = body["ImportDestinationType"].as_str();
        if let Some(ft) = filter_type {
            if !matches!(ft, "SUPPRESSION_LIST" | "CONTACT_LIST") {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ImportDestinationType must be SUPPRESSION_LIST or CONTACT_LIST",
                ));
            }
        }

        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let jobs: Vec<Value> = state
            .import_jobs
            .values()
            .filter(|j| {
                if let Some(ft) = filter_type {
                    // Check if import destination matches
                    if j.import_destination
                        .get("SuppressionListDestination")
                        .is_some()
                        && ft == "SUPPRESSION_LIST"
                    {
                        return true;
                    }
                    if j.import_destination.get("ContactListDestination").is_some()
                        && ft == "CONTACT_LIST"
                    {
                        return true;
                    }
                    return false;
                }
                true
            })
            .map(|j| {
                let mut obj = json!({
                    "JobId": j.job_id,
                    "ImportDestination": j.import_destination,
                    "JobStatus": j.job_status,
                    "CreatedTimestamp": j.created_timestamp.timestamp() as f64,
                });
                if j.processed_records_count > 0 {
                    obj["ProcessedRecordsCount"] = json!(j.processed_records_count);
                }
                if j.failed_records_count > 0 {
                    obj["FailedRecordsCount"] = json!(j.failed_records_count);
                }
                obj
            })
            .collect();

        let response = json!({ "ImportJobs": jobs });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    // --- Export Job operations ---

    pub(super) fn create_export_job(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;

        let export_data_source = match body.get("ExportDataSource") {
            Some(v) if v.is_object() => v.clone(),
            _ => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ExportDataSource is required",
                ));
            }
        };

        let export_destination = match body.get("ExportDestination") {
            Some(v) if v.is_object() => v.clone(),
            _ => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ExportDestination is required",
                ));
            }
        };

        // Determine export source type from the data source
        let export_source_type = if export_data_source.get("MetricsDataSource").is_some() {
            "METRICS_DATA"
        } else {
            "MESSAGE_INSIGHTS"
        };

        let job_id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();

        let job = ExportJob {
            job_id: job_id.clone(),
            export_source_type: export_source_type.to_string(),
            export_destination,
            export_data_source,
            job_status: "COMPLETED".to_string(),
            created_timestamp: now,
            completed_timestamp: Some(now),
        };

        self.state
            .write()
            .get_or_create(&req.account_id)
            .export_jobs
            .insert(job_id.clone(), job);

        let response = json!({ "JobId": job_id });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn get_export_job(
        &self,
        job_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let job = match state.export_jobs.get(job_id) {
            Some(j) => j,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Export job {} does not exist", job_id),
                ));
            }
        };

        let mut response = json!({
            "JobId": job.job_id,
            "ExportSourceType": job.export_source_type,
            "JobStatus": job.job_status,
            "ExportDestination": job.export_destination,
            "ExportDataSource": job.export_data_source,
            "CreatedTimestamp": job.created_timestamp.timestamp() as f64,
            "Statistics": {
                "ProcessedRecordsCount": 0,
                "ExportedRecordsCount": 0,
            },
        });
        if let Some(ref ts) = job.completed_timestamp {
            response["CompletedTimestamp"] = json!(ts.timestamp() as f64);
        }

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn list_export_jobs(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or(json!({}));
        let filter_status = body["JobStatus"].as_str();
        let filter_type = body["ExportSourceType"].as_str();
        if let Some(s) = filter_status {
            if !matches!(
                s,
                "CREATED" | "PROCESSING" | "COMPLETED" | "FAILED" | "CANCELLED"
            ) {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "JobStatus must be CREATED, PROCESSING, COMPLETED, FAILED, or CANCELLED",
                ));
            }
        }
        if let Some(t) = filter_type {
            if !matches!(t, "METRICS_DATA" | "MESSAGE_INSIGHTS") {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ExportSourceType must be METRICS_DATA or MESSAGE_INSIGHTS",
                ));
            }
        }

        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let jobs: Vec<Value> = state
            .export_jobs
            .values()
            .filter(|j| {
                if let Some(s) = filter_status {
                    if j.job_status != s {
                        return false;
                    }
                }
                if let Some(t) = filter_type {
                    if j.export_source_type != t {
                        return false;
                    }
                }
                true
            })
            .map(|j| {
                let mut obj = json!({
                    "JobId": j.job_id,
                    "ExportSourceType": j.export_source_type,
                    "JobStatus": j.job_status,
                    "CreatedTimestamp": j.created_timestamp.timestamp() as f64,
                });
                if let Some(ref ts) = j.completed_timestamp {
                    obj["CompletedTimestamp"] = json!(ts.timestamp() as f64);
                }
                obj
            })
            .collect();

        let response = json!({ "ExportJobs": jobs });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn cancel_export_job(
        &self,
        job_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let job = match state.export_jobs.get_mut(job_id) {
            Some(j) => j,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Export job {} does not exist", job_id),
                ));
            }
        };

        if job.job_status == "COMPLETED" || job.job_status == "CANCELLED" {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "ConflictException",
                &format!("Export job {} is already {}", job_id, job.job_status),
            ));
        }

        job.job_status = "CANCELLED".to_string();
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // --- Tenant operations ---

    pub(super) fn create_tenant(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let tenant_name = match body["TenantName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TenantName is required",
                ));
            }
        };
        if tenant_name.is_empty() {
            return Ok(Self::json_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "TenantName must not be empty",
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if state.tenants.contains_key(&tenant_name) {
            return Ok(Self::json_error(
                StatusCode::CONFLICT,
                "AlreadyExistsException",
                &format!("Tenant {} already exists", tenant_name),
            ));
        }

        let tenant_id = uuid::Uuid::new_v4().to_string();
        let tenant_arn = format!(
            "arn:aws:ses:{}:{}:tenant/{}",
            req.region, req.account_id, tenant_id
        );
        let now = Utc::now();

        let tags = body
            .get("Tags")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let suppression_attributes = body.get("SuppressionAttributes").cloned();

        let tenant = Tenant {
            tenant_name: tenant_name.clone(),
            tenant_id: tenant_id.clone(),
            tenant_arn: tenant_arn.clone(),
            created_timestamp: now,
            sending_status: "ENABLED".to_string(),
            tags: tags.clone(),
            suppression_attributes: suppression_attributes.clone(),
        };

        state.tenants.insert(tenant_name.clone(), tenant);

        let mut response = json!({
            "TenantName": tenant_name,
            "TenantId": tenant_id,
            "TenantArn": tenant_arn,
            "CreatedTimestamp": now.timestamp() as f64,
            "SendingStatus": "ENABLED",
            "Tags": tags,
        });
        if let Some(attrs) = suppression_attributes {
            response["SuppressionAttributes"] = attrs;
        }
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn get_tenant(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let tenant_name = match body["TenantName"].as_str() {
            Some(n) => n,
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TenantName is required",
                ));
            }
        };

        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let tenant = match state.tenants.get(tenant_name) {
            Some(t) => t,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Tenant {} does not exist", tenant_name),
                ));
            }
        };

        let mut response = json!({
            "Tenant": {
                "TenantName": tenant.tenant_name,
                "TenantId": tenant.tenant_id,
                "TenantArn": tenant.tenant_arn,
                "CreatedTimestamp": tenant.created_timestamp.timestamp() as f64,
                "SendingStatus": tenant.sending_status,
                "Tags": tenant.tags,
            }
        });
        if let Some(attrs) = &tenant.suppression_attributes {
            response["Tenant"]["SuppressionAttributes"] = attrs.clone();
        }
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    /// Configure the suppression-list preferences for a tenant. Sets the
    /// reasons (BOUNCE / COMPLAINT) that automatically add recipients to the
    /// tenant's suppression list. Returns an empty 200 on success.
    pub(super) fn put_tenant_suppression_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let tenant_name = match body["TenantName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TenantName is required",
                ));
            }
        };
        let suppressed_reasons = body
            .get("SuppressedReasons")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        let tenant = match state.tenants.get_mut(&tenant_name) {
            Some(t) => t,
            None => {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!("Tenant {} does not exist", tenant_name),
                ));
            }
        };
        // Preserve any existing ValidationAttributes; replace SuppressedReasons.
        let mut attrs = tenant
            .suppression_attributes
            .clone()
            .unwrap_or_else(|| json!({}));
        attrs["SuppressedReasons"] = Value::Array(suppressed_reasons);
        tenant.suppression_attributes = Some(attrs);

        Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
    }

    pub(super) fn list_tenants(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let tenants: Vec<Value> = state
            .tenants
            .values()
            .map(|t| {
                json!({
                    "TenantName": t.tenant_name,
                    "TenantId": t.tenant_id,
                    "TenantArn": t.tenant_arn,
                    "CreatedTimestamp": t.created_timestamp.timestamp() as f64,
                })
            })
            .collect();

        let response = json!({ "Tenants": tenants });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn delete_tenant(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let tenant_name = match body["TenantName"].as_str() {
            Some(n) => n,
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TenantName is required",
                ));
            }
        };
        if tenant_name.is_empty() {
            return Ok(Self::json_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "TenantName must not be empty",
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if state.tenants.remove(tenant_name).is_none() {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Tenant {} does not exist", tenant_name),
            ));
        }

        state.tenant_resource_associations.remove(tenant_name);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn create_tenant_resource_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let tenant_name = match body["TenantName"].as_str() {
            Some(n) => n.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TenantName is required",
                ));
            }
        };
        if tenant_name.is_empty() {
            return Ok(Self::json_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "TenantName must not be empty",
            ));
        }
        let resource_arn = match body["ResourceArn"].as_str() {
            Some(a) => a.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ResourceArn is required",
                ));
            }
        };
        if resource_arn.is_empty() {
            return Ok(Self::json_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "ResourceArn must not be empty",
            ));
        }

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if !state.tenants.contains_key(&tenant_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Tenant {} does not exist", tenant_name),
            ));
        }

        let assoc = TenantResourceAssociation {
            resource_arn,
            associated_timestamp: Utc::now(),
        };

        state
            .tenant_resource_associations
            .entry(tenant_name)
            .or_default()
            .push(assoc);

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn delete_tenant_resource_association(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let tenant_name = match body["TenantName"].as_str() {
            Some(n) => n,
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TenantName is required",
                ));
            }
        };
        let resource_arn = match body["ResourceArn"].as_str() {
            Some(a) => a,
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ResourceArn is required",
                ));
            }
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        if let Some(assocs) = state.tenant_resource_associations.get_mut(tenant_name) {
            let before = assocs.len();
            assocs.retain(|a| a.resource_arn != resource_arn);
            if assocs.len() == before {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    "Resource association not found",
                ));
            }
        } else {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                "Resource association not found",
            ));
        }

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn list_tenant_resources(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let tenant_name = match body["TenantName"].as_str() {
            Some(n) => n,
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "TenantName is required",
                ));
            }
        };

        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        if !state.tenants.contains_key(tenant_name) {
            return Ok(Self::json_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                &format!("Tenant {} does not exist", tenant_name),
            ));
        }

        let resources: Vec<Value> = state
            .tenant_resource_associations
            .get(tenant_name)
            .map(|assocs| {
                assocs
                    .iter()
                    .map(|a| {
                        json!({
                            "ResourceType": "RESOURCE",
                            "ResourceArn": a.resource_arn,
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();

        let response = json!({ "TenantResources": resources });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn list_resource_tenants(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let resource_arn = match body["ResourceArn"].as_str() {
            Some(a) => a,
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ResourceArn is required",
                ));
            }
        };
        if resource_arn.is_empty() {
            return Ok(Self::json_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "ResourceArn must not be empty",
            ));
        }

        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let mut resource_tenants: Vec<Value> = Vec::new();

        for (tenant_name, assocs) in &state.tenant_resource_associations {
            for assoc in assocs {
                if assoc.resource_arn == resource_arn {
                    if let Some(tenant) = state.tenants.get(tenant_name) {
                        resource_tenants.push(json!({
                            "TenantName": tenant.tenant_name,
                            "TenantId": tenant.tenant_id,
                            "ResourceArn": assoc.resource_arn,
                            "AssociatedTimestamp": assoc.associated_timestamp.timestamp() as f64,
                        }));
                    }
                }
            }
        }

        let response = json!({ "ResourceTenants": resource_tenants });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    // --- Reputation Entity operations ---

    pub(super) fn get_reputation_entity(
        &self,
        entity_type: &str,
        entity_ref: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Self::require_nonempty("ReputationEntityType", entity_type)?;
        Self::require_nonempty("ReputationEntityReference", entity_ref)?;
        if entity_type != "RESOURCE" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "ReputationEntityType must be RESOURCE",
            ));
        }
        let key = format!("{}/{}", entity_type, entity_ref);
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let entity = match state.reputation_entities.get(&key) {
            Some(e) => e,
            None => {
                let response = json!({
                    "ReputationEntity": {
                        "ReputationEntityReference": entity_ref,
                        "ReputationEntityType": entity_type,
                        "SendingStatusAggregate": "ENABLED",
                        "CustomerManagedStatus": {"Status": "ENABLED"},
                        "AwsSesManagedStatus": {"Status": "ENABLED"},
                    }
                });
                return Ok(AwsResponse::json(StatusCode::OK, response.to_string()));
            }
        };

        let response = json!({
            "ReputationEntity": {
                "ReputationEntityReference": entity.reputation_entity_reference,
                "ReputationEntityType": entity.reputation_entity_type,
                "ReputationManagementPolicy": entity.reputation_management_policy,
                "SendingStatusAggregate": entity.sending_status_aggregate,
                "CustomerManagedStatus": {"Status": entity.customer_managed_status},
                "AwsSesManagedStatus": {"Status": "ENABLED"},
            }
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn list_reputation_entities(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let entities: Vec<Value> = state
            .reputation_entities
            .values()
            .map(|e| {
                json!({
                    "ReputationEntityReference": e.reputation_entity_reference,
                    "ReputationEntityType": e.reputation_entity_type,
                    "SendingStatusAggregate": e.sending_status_aggregate,
                })
            })
            .collect();

        let response = json!({ "ReputationEntities": entities });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn update_reputation_entity_customer_managed_status(
        &self,
        entity_type: &str,
        entity_ref: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Self::require_nonempty("ReputationEntityType", entity_type)?;
        Self::require_nonempty("ReputationEntityReference", entity_ref)?;
        if entity_type != "RESOURCE" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "ReputationEntityType must be RESOURCE",
            ));
        }
        let body: Value = Self::parse_body(req)?;
        let sending_status = body["SendingStatus"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "SendingStatus is required",
                )
            })?
            .to_string();
        if !matches!(
            sending_status.as_str(),
            "ENABLED" | "DISABLED" | "REINSTATED"
        ) {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "SendingStatus must be ENABLED, DISABLED, or REINSTATED",
            ));
        }

        let key = format!("{}/{}", entity_type, entity_ref);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let entity =
            state
                .reputation_entities
                .entry(key)
                .or_insert_with(|| ReputationEntityState {
                    reputation_entity_reference: entity_ref.to_string(),
                    reputation_entity_type: entity_type.to_string(),
                    reputation_management_policy: None,
                    customer_managed_status: "ENABLED".to_string(),
                    sending_status_aggregate: "ENABLED".to_string(),
                });

        entity.customer_managed_status = sending_status;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn update_reputation_entity_policy(
        &self,
        entity_type: &str,
        entity_ref: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Self::require_nonempty("ReputationEntityType", entity_type)?;
        Self::require_nonempty("ReputationEntityReference", entity_ref)?;
        if entity_type != "RESOURCE" {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "ReputationEntityType must be RESOURCE",
            ));
        }
        let body: Value = Self::parse_body(req)?;
        let policy = body["ReputationEntityPolicy"]
            .as_str()
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ReputationEntityPolicy is required",
                )
            })?
            .to_string();
        // ReputationEntityPolicy is required and cannot be empty (it's an
        // AWS-managed policy ARN).
        if policy.is_empty() {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "ReputationEntityPolicy must not be empty",
            ));
        }
        let policy = Some(policy);

        let key = format!("{}/{}", entity_type, entity_ref);
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);

        let entity =
            state
                .reputation_entities
                .entry(key)
                .or_insert_with(|| ReputationEntityState {
                    reputation_entity_reference: entity_ref.to_string(),
                    reputation_entity_type: entity_type.to_string(),
                    reputation_management_policy: None,
                    customer_managed_status: "ENABLED".to_string(),
                    sending_status_aggregate: "ENABLED".to_string(),
                });

        entity.reputation_management_policy = policy;

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    // --- Metrics ---

    pub(super) fn batch_get_metric_data(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let queries = match body.get("Queries").and_then(|v| v.as_array()) {
            Some(arr) => arr.clone(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "Queries is required",
                ));
            }
        };

        let results: Vec<Value> = queries
            .iter()
            .filter_map(|q| {
                let id = q["Id"].as_str()?;
                Some(json!({
                    "Id": id,
                    "Timestamps": [],
                    "Values": [],
                }))
            })
            .collect();

        let response = json!({
            "Results": results,
            "Errors": [],
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn get_dedicated_ip_pool(
        &self,
        pool_name: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let pool = state.dedicated_ip_pools.get(pool_name).ok_or_else(|| {
            AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Dedicated IP pool {pool_name} not found."),
            )
        })?;
        let body = json!({
            "DedicatedIpPool": {
                "PoolName": pool.pool_name,
                "ScalingMode": pool.scaling_mode,
            }
        });
        Ok(AwsResponse::json(StatusCode::OK, body.to_string()))
    }

    pub(super) fn get_deliverability_dashboard_options(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let dash = &state.deliverability_dashboard;
        let body = json!({
            "DashboardEnabled": dash.enabled,
            "SubscriptionExpiryDate": dash.subscription_expiry_date.map(|d| d.timestamp()),
            "AccountStatus": if dash.enabled { "ACTIVE" } else { "DISABLED" },
            "ActiveSubscribedDomains": dash.subscribed_domains.iter().map(subscribed_domain_json).collect::<Vec<_>>(),
            "PendingExpirationSubscribedDomains": Vec::<Value>::new(),
        });
        Ok(AwsResponse::json(StatusCode::OK, body.to_string()))
    }

    pub(super) fn put_deliverability_dashboard_option(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let enabled = body
            .get("DashboardEnabled")
            .and_then(|v| v.as_bool())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "DashboardEnabled is required",
                )
            })?;
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.deliverability_dashboard.enabled = enabled;
        if let Some(domains) = body.get("SubscribedDomains").and_then(|v| v.as_array()) {
            state.deliverability_dashboard.subscribed_domains = domains
                .iter()
                .filter_map(|d| {
                    let domain = d.get("Domain").and_then(|v| v.as_str())?.to_string();
                    Some(SubscribedDomain {
                        domain,
                        subscription_start_date: Utc::now(),
                        inbox_placement_tracking_option_global: d
                            .get("InboxPlacementTrackingOption")
                            .and_then(|v| v.get("Global"))
                            .and_then(|v| v.as_bool())
                            .unwrap_or(false),
                        inbox_placement_tracking_option_tracked_isps: d
                            .get("InboxPlacementTrackingOption")
                            .and_then(|v| v.get("TrackedIsps"))
                            .and_then(|v| v.as_array())
                            .map(|arr| {
                                arr.iter()
                                    .filter_map(|s| s.as_str().map(String::from))
                                    .collect()
                            })
                            .unwrap_or_default(),
                    })
                })
                .collect();
        }
        Ok(AwsResponse::json(StatusCode::OK, "{}".to_string()))
    }

    pub(super) fn create_deliverability_test_report(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let report_name = body
            .get("ReportName")
            .and_then(|v| v.as_str())
            .unwrap_or("test-report")
            .to_string();
        let from_email = body
            .get("FromEmailAddress")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "FromEmailAddress is required",
                )
            })?
            .to_string();
        match body.get("Content") {
            None | Some(Value::Null) => {
                return Err(AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "Content is required",
                ));
            }
            _ => {}
        }
        let subject = body
            .get("Content")
            .and_then(|v| v.get("Simple"))
            .and_then(|v| v.get("Subject"))
            .and_then(|v| v.get("Data"))
            .and_then(|v| v.as_str())
            .unwrap_or("Predictive inbox placement test")
            .to_string();
        let tags = body
            .get("Tags")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| {
                        let k = t.get("Key").and_then(|v| v.as_str())?.to_string();
                        let v = t.get("Value").and_then(|v| v.as_str())?.to_string();
                        Some((k, v))
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let report_id = uuid::Uuid::new_v4().to_string();
        let report = DeliverabilityTestReport {
            report_id: report_id.clone(),
            report_name,
            subject,
            from_email,
            create_date: Utc::now(),
            deliverability_test_status: "IN_PROGRESS".to_string(),
            tags,
        };

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state
            .deliverability_test_reports
            .insert(report_id.clone(), report);
        let body = json!({
            "ReportId": report_id,
            "DeliverabilityTestStatus": "IN_PROGRESS",
        });
        Ok(AwsResponse::json(StatusCode::OK, body.to_string()))
    }

    pub(super) fn get_deliverability_test_report(
        &self,
        report_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let report = state
            .deliverability_test_reports
            .get(report_id)
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    format!("Deliverability test report {report_id} not found."),
                )
            })?;
        let body = json!({
            "DeliverabilityTestReport": deliverability_test_report_json(report),
            "OverallPlacement": {
                "InboxPercentage": 95.0,
                "SpamPercentage": 5.0,
                "MissingPercentage": 0.0,
                "SpfPercentage": 100.0,
                "DkimPercentage": 100.0,
            },
            "IspPlacements": [],
            "Message": null,
            "Tags": tags_json(&report.tags),
        });
        Ok(AwsResponse::json(StatusCode::OK, body.to_string()))
    }

    pub(super) fn list_deliverability_test_reports(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let reports: Vec<Value> = state
            .deliverability_test_reports
            .values()
            .map(deliverability_test_report_json)
            .collect();
        let body = json!({
            "DeliverabilityTestReports": reports,
            "NextToken": null,
        });
        Ok(AwsResponse::json(StatusCode::OK, body.to_string()))
    }

    pub(super) fn get_blacklist_reports(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // `BlacklistItemNames` is a required @httpQuery list. Reject
        // requests that omit it entirely; an empty list is still a list.
        let has_blacklist_items = req
            .raw_query
            .split('&')
            .any(|kv| kv.starts_with("BlacklistItemNames=") || kv == "BlacklistItemNames");
        if !has_blacklist_items {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "BlacklistItemNames is required",
            ));
        }
        // Emulator has no blacklist data; return an empty map per the
        // documented response shape.
        let body = json!({ "BlacklistReport": {} });
        Ok(AwsResponse::json(StatusCode::OK, body.to_string()))
    }

    pub(super) fn get_domain_deliverability_campaign(
        &self,
        campaign_id: &str,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // No real campaigns to look up; return a synthetic record so SDKs
        // can decode the response shape but no inflated metrics.
        let body = json!({
            "DomainDeliverabilityCampaign": {
                "CampaignId": campaign_id,
                "ImageUrl": null,
                "Subject": null,
                "FromAddress": null,
                "SendingIps": [],
                "FirstSeenDateTime": Utc::now().timestamp(),
                "LastSeenDateTime": Utc::now().timestamp(),
                "InboxCount": 0,
                "SpamCount": 0,
                "ReadRate": 0.0,
                "DeleteRate": 0.0,
                "ReadDeleteRate": 0.0,
                "ProjectedVolume": 0,
                "Esps": [],
            }
        });
        Ok(AwsResponse::json(StatusCode::OK, body.to_string()))
    }

    pub(super) fn get_domain_statistics_report(
        &self,
        domain: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Self::require_nonempty("Domain", domain)?;
        let has = |k: &str| {
            let prefix = format!("{k}=");
            req.raw_query
                .split('&')
                .any(|kv| kv.starts_with(&prefix) || kv == k)
        };
        if !has("StartDate") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "StartDate is required",
            ));
        }
        if !has("EndDate") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "EndDate is required",
            ));
        }
        let body = json!({
            "OverallVolume": {
                "VolumeStatistics": {
                    "InboxRawCount": 0,
                    "SpamRawCount": 0,
                    "ProjectedInbox": 0,
                    "ProjectedSpam": 0,
                },
                "ReadRatePercent": 0.0,
                "DomainIspPlacements": [],
            },
            "DailyVolumes": [],
        });
        Ok(AwsResponse::json(StatusCode::OK, body.to_string()))
    }

    pub(super) fn list_domain_deliverability_campaigns(
        &self,
        domain: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        Self::require_nonempty("SubscribedDomain", domain)?;
        let has = |k: &str| {
            let prefix = format!("{k}=");
            req.raw_query
                .split('&')
                .any(|kv| kv.starts_with(&prefix) || kv == k)
        };
        if !has("StartDate") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "StartDate is required",
            ));
        }
        if !has("EndDate") {
            return Err(AwsServiceError::aws_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "EndDate is required",
            ));
        }
        let body = json!({
            "DomainDeliverabilityCampaigns": [],
            "NextToken": null,
        });
        Ok(AwsResponse::json(StatusCode::OK, body.to_string()))
    }

    pub(super) fn get_email_address_insights(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = Self::parse_body(req)?;
        let email = body
            .get("EmailAddress")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AwsServiceError::aws_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "EmailAddress is required",
                )
            })?
            .to_string();
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        // No external validation service is wired up; emit a stable
        // synthetic verdict so SDKs round-trip the documented shape.
        let _ = (email, state);
        let body = json!({
            "MailboxValidation": {
                "IsValid": { "ConfidenceVerdict": "HIGH" },
                "Evaluations": {
                    "HasValidSyntax": { "ConfidenceVerdict": "HIGH" },
                    "HasValidDnsRecords": { "ConfidenceVerdict": "MEDIUM" },
                    "MailboxExists": { "ConfidenceVerdict": "MEDIUM" },
                    "IsRoleAddress": { "ConfidenceVerdict": "LOW" },
                    "IsDisposable": { "ConfidenceVerdict": "LOW" },
                    "IsRandomInput": { "ConfidenceVerdict": "LOW" },
                },
            },
        });
        Ok(AwsResponse::json(StatusCode::OK, body.to_string()))
    }

    pub(super) fn get_message_insights(
        &self,
        message_id: &str,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let sent = state
            .sent_emails
            .iter()
            .find(|e| e.message_id == message_id);
        if let Some(email) = sent {
            let email_tags: Vec<serde_json::Value> = email
                .email_tags
                .iter()
                .map(|(name, value)| json!({"Name": name, "Value": value}))
                .collect();
            let insights: Vec<serde_json::Value> = email
                .delivery_insights
                .iter()
                .map(|ins| {
                    let events: Vec<serde_json::Value> = ins
                        .events
                        .iter()
                        .map(|ev| {
                            let mut detail = serde_json::Map::new();
                            if ev.event_type == "BOUNCE" {
                                let mut bounce = serde_json::Map::new();
                                if let Some(ref bt) = ev.bounce_type {
                                    bounce.insert("BounceType".to_string(), json!(bt));
                                }
                                if let Some(ref bs) = ev.bounce_sub_type {
                                    bounce.insert("BounceSubType".to_string(), json!(bs));
                                }
                                if let Some(ref dc) = ev.diagnostic_code {
                                    bounce.insert("DiagnosticCode".to_string(), json!(dc));
                                }
                                if !bounce.is_empty() {
                                    detail.insert("Bounce".to_string(), json!(bounce));
                                }
                            }
                            if ev.event_type == "COMPLAINT" {
                                let mut complaint = serde_json::Map::new();
                                if let Some(ref cst) = ev.complaint_sub_type {
                                    complaint.insert("ComplaintSubType".to_string(), json!(cst));
                                }
                                if let Some(ref cft) = ev.complaint_feedback_type {
                                    complaint
                                        .insert("ComplaintFeedbackType".to_string(), json!(cft));
                                }
                                if !complaint.is_empty() {
                                    detail.insert("Complaint".to_string(), json!(complaint));
                                }
                            }
                            let mut event = serde_json::Map::new();
                            event.insert(
                                "Timestamp".to_string(),
                                json!(ev.timestamp.timestamp() as f64),
                            );
                            event.insert("Type".to_string(), json!(ev.event_type));
                            if !detail.is_empty() {
                                event.insert("Details".to_string(), json!(detail));
                            }
                            json!(event)
                        })
                        .collect();
                    json!({
                        "Destination": ins.destination,
                        "Isp": ins.isp,
                        "Events": events,
                    })
                })
                .collect();
            let body = json!({
                "MessageId": email.message_id,
                "FromEmailAddress": email.from,
                "Subject": email.subject,
                "EmailTags": email_tags,
                "Insights": insights,
            });
            Ok(AwsResponse::json(StatusCode::OK, body.to_string()))
        } else {
            Err(AwsServiceError::aws_error(
                StatusCode::NOT_FOUND,
                "NotFoundException",
                format!("Message {message_id} not found."),
            ))
        }
    }

    pub(super) fn list_recommendations(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        if state.vdm_recommendations.is_empty() {
            // Lazy-seed with one realistic recommendation so consumers see
            // a non-empty list shape; field values match real AWS data
            // schema.
            let now = Utc::now();
            state.vdm_recommendations.push(VdmRecommendation {
                resource_arn: format!(
                    "arn:aws:ses:{}:{}:identity/example.com",
                    req.region, req.account_id
                ),
                recommendation_type: "DKIM".to_string(),
                description: "Configure DKIM signing for identity to improve deliverability."
                    .to_string(),
                status: "OPEN".to_string(),
                created_timestamp: now,
                last_updated_timestamp: now,
                impact: "HIGH".to_string(),
            });
        }
        let recs: Vec<Value> = state
            .vdm_recommendations
            .iter()
            .map(|r| {
                json!({
                    "ResourceArn": r.resource_arn,
                    "Type": r.recommendation_type,
                    "Description": r.description,
                    "Status": r.status,
                    "CreatedTimestamp": r.created_timestamp.timestamp(),
                    "LastUpdatedTimestamp": r.last_updated_timestamp.timestamp(),
                    "Impact": r.impact,
                })
            })
            .collect();
        let body = json!({
            "Recommendations": recs,
            "NextToken": null,
        });
        Ok(AwsResponse::json(StatusCode::OK, body.to_string()))
    }
}

fn subscribed_domain_json(d: &SubscribedDomain) -> Value {
    json!({
        "Domain": d.domain,
        "SubscriptionStartDate": d.subscription_start_date.timestamp(),
        "InboxPlacementTrackingOption": {
            "Global": d.inbox_placement_tracking_option_global,
            "TrackedIsps": d.inbox_placement_tracking_option_tracked_isps,
        },
    })
}

fn deliverability_test_report_json(r: &DeliverabilityTestReport) -> Value {
    json!({
        "ReportId": r.report_id,
        "ReportName": r.report_name,
        "Subject": r.subject,
        "FromEmailAddress": r.from_email,
        "CreateDate": r.create_date.timestamp(),
        "DeliverabilityTestStatus": r.deliverability_test_status,
    })
}

fn tags_json(tags: &[(String, String)]) -> Value {
    let arr: Vec<Value> = tags
        .iter()
        .map(|(k, v)| json!({ "Key": k, "Value": v }))
        .collect();
    Value::Array(arr)
}
