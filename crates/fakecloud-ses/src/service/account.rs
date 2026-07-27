use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::AccountDetails;
use crate::state::SesState;

use super::SesV2Service;

impl SesV2Service {
    pub(super) fn get_account(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let accounts = self.state.read();
        let empty = SesState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        let acct = &state.account_settings;
        let production_access = acct.production_access_enabled;
        let mut response = json!({
            "DedicatedIpAutoWarmupEnabled": acct.dedicated_ip_auto_warmup_enabled,
            "EnforcementStatus": "HEALTHY",
            "ProductionAccessEnabled": production_access,
            "SendQuota": {
                "Max24HourSend": 50000.0,
                "MaxSendRate": 14.0,
                "SentLast24Hours": state.sent_emails.iter()
                    .filter(|e| e.timestamp > Utc::now() - chrono::Duration::hours(24))
                    .count() as f64,
            },
            "SendingEnabled": acct.sending_enabled,
            "SuppressionAttributes": {
                "SuppressedReasons": acct.suppressed_reasons,
            },
        });
        if let Some(ref details) = acct.details {
            let mut d = json!({});
            if let Some(ref mt) = details.mail_type {
                d["MailType"] = json!(mt);
            }
            if let Some(ref url) = details.website_url {
                d["WebsiteURL"] = json!(url);
            }
            if let Some(ref lang) = details.contact_language {
                d["ContactLanguage"] = json!(lang);
            }
            if let Some(ref desc) = details.use_case_description {
                d["UseCaseDescription"] = json!(desc);
            }
            if !details.additional_contact_email_addresses.is_empty() {
                d["AdditionalContactEmailAddresses"] =
                    json!(details.additional_contact_email_addresses);
            }
            d["ReviewDetails"] = json!({
                "Status": "GRANTED",
                "CaseId": "fakecloud-case-001",
            });
            response["Details"] = d;
        }
        if let Some(ref vdm) = acct.vdm_attributes {
            response["VdmAttributes"] = vdm.clone();
        }
        // PricingAttributes reports the plan set via PutAccountPricingAttributes.
        // An unset plan reads back as NONE; the change applies immediately so
        // CurrentPlan and NextPlan match.
        let plan = acct.pricing_plan.as_deref().unwrap_or("NONE");
        response["PricingAttributes"] = json!({
            "CurrentPlan": plan,
            "NextPlan": plan,
        });
        Ok(AwsResponse::json(StatusCode::OK, response.to_string()))
    }

    pub(super) fn put_account_details(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let mail_type = match body["MailType"].as_str() {
            Some(m) => m.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "MailType is required",
                ));
            }
        };
        // MailType enum: MARKETING | TRANSACTIONAL.
        if !matches!(mail_type.as_str(), "MARKETING" | "TRANSACTIONAL") {
            return Ok(Self::json_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "MailType must be MARKETING or TRANSACTIONAL",
            ));
        }
        let website_url = match body["WebsiteURL"].as_str() {
            Some(u) => u.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "WebsiteURL is required",
                ));
            }
        };
        // WebsiteURL has length 1..=1000 per the SES v2 Smithy model.
        // Smithy `@length` counts UTF-8 code points, not bytes.
        let website_url_len = website_url.chars().count();
        if website_url_len == 0 || website_url_len > 1000 {
            return Ok(Self::json_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "WebsiteURL length must be between 1 and 1000",
            ));
        }
        let contact_language = body["ContactLanguage"].as_str().map(|s| s.to_string());
        if let Some(ref cl) = contact_language {
            // ContactLanguage enum: EN | JA.
            if !matches!(cl.as_str(), "EN" | "JA") {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "ContactLanguage must be EN or JA",
                ));
            }
        }
        let use_case_description = body["UseCaseDescription"].as_str().map(|s| s.to_string());
        if let Some(ref desc) = use_case_description {
            // UseCaseDescription has length 1..=5000 code points.
            let desc_len = desc.chars().count();
            if desc_len == 0 || desc_len > 5000 {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "UseCaseDescription length must be between 1 and 5000",
                ));
            }
        }
        let additional = body["AdditionalContactEmailAddresses"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let production_access = body["ProductionAccessEnabled"].as_bool();

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.account_settings.details = Some(AccountDetails {
            mail_type: Some(mail_type),
            website_url: Some(website_url),
            contact_language,
            use_case_description,
            additional_contact_email_addresses: additional,
            production_access_enabled: production_access,
        });
        // Mirror onto the top-level flag — it's the source of truth for
        // the SendEmail sandbox-recipient gate.
        if let Some(flag) = production_access {
            state.account_settings.production_access_enabled = flag;
        }
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn put_account_sending_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let enabled = body["SendingEnabled"].as_bool().unwrap_or(false);
        self.state
            .write()
            .get_or_create(&req.account_id)
            .account_settings
            .sending_enabled = enabled;
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn put_account_suppression_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let reasons = body["SuppressedReasons"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        self.state
            .write()
            .get_or_create(&req.account_id)
            .account_settings
            .suppressed_reasons = reasons;
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn put_account_vdm_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let vdm = match body.get("VdmAttributes") {
            Some(v) => v.clone(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "VdmAttributes is required",
                ));
            }
        };
        self.state
            .write()
            .get_or_create(&req.account_id)
            .account_settings
            .vdm_attributes = Some(vdm);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn put_account_pricing_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let plan = match body["Plan"].as_str() {
            Some(p) => p.to_string(),
            None => {
                return Ok(Self::json_error(
                    StatusCode::BAD_REQUEST,
                    "BadRequestException",
                    "Plan is required",
                ));
            }
        };
        // PricingPlan enum: NONE | ESSENTIALS | PRO | ENTERPRISE.
        if !matches!(plan.as_str(), "NONE" | "ESSENTIALS" | "PRO" | "ENTERPRISE") {
            return Ok(Self::json_error(
                StatusCode::BAD_REQUEST,
                "BadRequestException",
                "Plan must be one of NONE, ESSENTIALS, PRO, ENTERPRISE",
            ));
        }
        self.state
            .write()
            .get_or_create(&req.account_id)
            .account_settings
            .pricing_plan = Some(plan);
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn put_account_dedicated_ip_warmup_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let enabled = body["AutoWarmupEnabled"].as_bool().unwrap_or(false);
        self.state
            .write()
            .get_or_create(&req.account_id)
            .account_settings
            .dedicated_ip_auto_warmup_enabled = enabled;
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }
}
