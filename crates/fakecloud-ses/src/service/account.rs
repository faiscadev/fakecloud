use chrono::Utc;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::state::AccountDetails;

use super::SesV2Service;

impl SesV2Service {
    pub(super) fn get_account(&self) -> Result<AwsResponse, AwsServiceError> {
        let state = self.state.read();
        let acct = &state.account_settings;
        let production_access = acct
            .details
            .as_ref()
            .and_then(|d| d.production_access_enabled)
            .unwrap_or(true);
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
        let contact_language = body["ContactLanguage"].as_str().map(|s| s.to_string());
        let use_case_description = body["UseCaseDescription"].as_str().map(|s| s.to_string());
        let additional = body["AdditionalContactEmailAddresses"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();
        let production_access = body["ProductionAccessEnabled"].as_bool();

        let mut state = self.state.write();
        state.account_settings.details = Some(AccountDetails {
            mail_type: Some(mail_type),
            website_url: Some(website_url),
            contact_language,
            use_case_description,
            additional_contact_email_addresses: additional,
            production_access_enabled: production_access,
        });
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }

    pub(super) fn put_account_sending_attributes(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = Self::parse_body(req)?;
        let enabled = body["SendingEnabled"].as_bool().unwrap_or(false);
        self.state.write().account_settings.sending_enabled = enabled;
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
        self.state.write().account_settings.suppressed_reasons = reasons;
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
        self.state.write().account_settings.vdm_attributes = Some(vdm);
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
            .account_settings
            .dedicated_ip_auto_warmup_enabled = enabled;
        Ok(AwsResponse::json(StatusCode::OK, "{}"))
    }
}
