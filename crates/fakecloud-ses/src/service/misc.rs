use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::{
    CustomVerificationEmailTemplate, DedicatedIp, DedicatedIpPool, ExportJob, ImportJob,
    MultiRegionEndpoint, ReputationEntityState, SentEmail, Tenant, TenantResourceAssociation,
};

use super::SesV2Service;

impl SesV2Service {
    // --- Tag operations ---

    /// Validate that a resource ARN refers to an existing resource.
    /// Returns `None` if the resource exists, or `Some(error_response)` if not.
    pub(super) fn validate_resource_arn(&self, arn: &str) -> Option<AwsResponse> {
        let state = self.state.read();

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

        if let Some(resp) = self.validate_resource_arn(&arn) {
            return Ok(resp);
        }

        let mut state = self.state.write();
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

        if let Some(resp) = self.validate_resource_arn(&arn) {
            return Ok(resp);
        }

        // Parse TagKeys from raw query string (supports repeated params)
        let tag_keys: Vec<String> = form_urlencoded::parse(req.raw_query.as_bytes())
            .filter(|(k, _)| k == "TagKeys")
            .map(|(_, v)| v.into_owned())
            .collect();

        let mut state = self.state.write();
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

        if let Some(resp) = self.validate_resource_arn(&arn) {
            return Ok(resp);
        }

        let state = self.state.read();
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

        let mut state = self.state.write();

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
                template_name,
                from_email_address: from_email,
                template_subject: subject,
                template_content: content,
                success_redirection_url: success_url,
                failure_redirection_url: failure_url,
                created_at: Utc::now(),
            },
        );

        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn get_custom_verification_email_template(
        &self,
        name: &str,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
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

        let response = json!({
            "TemplateName": tmpl.template_name,
            "FromEmailAddress": tmpl.from_email_address,
            "TemplateSubject": tmpl.template_subject,
            "TemplateContent": tmpl.template_content,
            "SuccessRedirectionURL": tmpl.success_redirection_url,
            "FailureRedirectionURL": tmpl.failure_redirection_url,
        });

        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn list_custom_verification_email_templates(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();

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
        let mut state = self.state.write();

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
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();

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

        // Verify template exists
        {
            let state = self.state.read();
            if !state
                .custom_verification_email_templates
                .contains_key(&template_name)
            {
                return Ok(Self::json_error(
                    StatusCode::NOT_FOUND,
                    "NotFoundException",
                    &format!(
                        "Custom verification email template {} does not exist",
                        template_name
                    ),
                ));
            }
        }

        let message_id = uuid::Uuid::new_v4().to_string();

        // Store as a sent email for introspection
        let sent = SentEmail {
            message_id: message_id.clone(),
            from: String::new(),
            to: vec![email_address],
            cc: Vec::new(),
            bcc: Vec::new(),
            subject: Some(format!("Custom verification: {}", template_name)),
            html_body: None,
            text_body: None,
            raw_data: None,
            template_name: Some(template_name),
            template_data: None,
            timestamp: Utc::now(),
        };

        self.state.write().sent_emails.push(sent);

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

        let mut state = self.state.write();

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

    pub(super) fn list_dedicated_ip_pools(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
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
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
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

        let mut state = self.state.write();
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

    pub(super) fn get_dedicated_ip(&self, ip: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
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
        let state = self.state.read();
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

        let mut state = self.state.write();

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

        let mut state = self.state.write();
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

        let mut state = self.state.write();
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
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
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

    pub(super) fn list_multi_region_endpoints(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
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
    ) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
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

        self.state.write().import_jobs.insert(job_id.clone(), job);

        let response = json!({ "JobId": job_id });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn get_import_job(&self, job_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
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

        let state = self.state.read();
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

        self.state.write().export_jobs.insert(job_id.clone(), job);

        let response = json!({ "JobId": job_id });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn get_export_job(&self, job_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
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

        let state = self.state.read();
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

    pub(super) fn cancel_export_job(&self, job_id: &str) -> Result<AwsResponse, AwsServiceError> {
        let mut state = self.state.write();
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

        let mut state = self.state.write();

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

        let tenant = Tenant {
            tenant_name: tenant_name.clone(),
            tenant_id: tenant_id.clone(),
            tenant_arn: tenant_arn.clone(),
            created_timestamp: now,
            sending_status: "ENABLED".to_string(),
            tags: tags.clone(),
        };

        state.tenants.insert(tenant_name.clone(), tenant);

        let response = json!({
            "TenantName": tenant_name,
            "TenantId": tenant_id,
            "TenantArn": tenant_arn,
            "CreatedTimestamp": now.timestamp() as f64,
            "SendingStatus": "ENABLED",
            "Tags": tags,
        });
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

        let state = self.state.read();
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

        let response = json!({
            "Tenant": {
                "TenantName": tenant.tenant_name,
                "TenantId": tenant.tenant_id,
                "TenantArn": tenant.tenant_arn,
                "CreatedTimestamp": tenant.created_timestamp.timestamp() as f64,
                "SendingStatus": tenant.sending_status,
                "Tags": tenant.tags,
            }
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn list_tenants(&self, _req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
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

        let mut state = self.state.write();

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

        let mut state = self.state.write();

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

        let mut state = self.state.write();

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

        let state = self.state.read();

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

        let state = self.state.read();
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
    ) -> Result<AwsResponse, AwsServiceError> {
        let key = format!("{}/{}", entity_type, entity_ref);
        let state = self.state.read();

        let entity = match state.reputation_entities.get(&key) {
            Some(e) => e,
            None => {
                // Return a default entity for any reference
                let response = json!({
                    "ReputationEntity": {
                        "ReputationEntityReference": entity_ref,
                        "ReputationEntityType": entity_type,
                        "SendingStatusAggregate": "ENABLED",
                        "CustomerManagedStatus": {
                            "SendingStatus": "ENABLED",
                        },
                        "AwsSesManagedStatus": {
                            "SendingStatus": "ENABLED",
                        },
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
                "CustomerManagedStatus": {
                    "SendingStatus": entity.customer_managed_status,
                },
                "AwsSesManagedStatus": {
                    "SendingStatus": "ENABLED",
                },
            }
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn list_reputation_entities(
        &self,
        _req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
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
        let body: Value = Self::parse_body(req)?;
        let sending_status = body["SendingStatus"]
            .as_str()
            .unwrap_or("ENABLED")
            .to_string();

        let key = format!("{}/{}", entity_type, entity_ref);
        let mut state = self.state.write();

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
        let body: Value = Self::parse_body(req)?;
        let policy = body["ReputationEntityPolicy"]
            .as_str()
            .map(|s| s.to_string());

        let key = format!("{}/{}", entity_type, entity_ref);
        let mut state = self.state.write();

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
        let queries = body["Queries"].as_array().cloned().unwrap_or_default();

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
}
