use chrono::Utc;
use serde_json::json;

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::SsmServiceSetting;

use super::{json_resp, missing, parse_body, SsmService};

impl SsmService {
    pub(super) fn get_connection_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("Target", body["Target"].as_str(), 1, 400)?;
        let target = body["Target"].as_str().ok_or_else(|| missing("Target"))?;
        Ok(json_resp(json!({
            "Target": target,
            "Status": "connected",
        })))
    }

    pub(super) fn get_calendar_state(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        let _calendar_names = body["CalendarNames"]
            .as_array()
            .ok_or_else(|| missing("CalendarNames"))?;
        Ok(json_resp(json!({
            "State": "OPEN",
            "AtTime": Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        })))
    }

    pub(super) fn get_service_setting(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("SettingId", body["SettingId"].as_str(), 1, 1000)?;
        let setting_id = body["SettingId"]
            .as_str()
            .ok_or_else(|| missing("SettingId"))?;

        let state = self.state.read();
        if let Some(setting) = state.service_settings.get(setting_id) {
            Ok(json_resp(json!({
                "ServiceSetting": {
                    "SettingId": setting.setting_id,
                    "SettingValue": setting.setting_value,
                    "LastModifiedDate": setting.last_modified_date.timestamp_millis() as f64 / 1000.0,
                    "LastModifiedUser": setting.last_modified_user,
                    "ARN": Arn::new("ssm", &state.region, &state.account_id, &format!("servicesetting/{}", setting.setting_id)).to_string(),
                    "Status": setting.status,
                }
            })))
        } else {
            // Return sensible default for known settings
            Ok(json_resp(json!({
                "ServiceSetting": {
                    "SettingId": setting_id,
                    "SettingValue": get_default_service_setting(setting_id),
                    "LastModifiedDate": Utc::now().timestamp_millis() as f64 / 1000.0,
                    "LastModifiedUser": "System",
                    "ARN": Arn::new("ssm", &state.region, &state.account_id, &format!("servicesetting/{setting_id}")).to_string(),
                    "Status": "Default",
                }
            })))
        }
    }

    pub(super) fn reset_service_setting(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("SettingId", body["SettingId"].as_str(), 1, 1000)?;
        let setting_id = body["SettingId"]
            .as_str()
            .ok_or_else(|| missing("SettingId"))?;

        let mut state = self.state.write();
        state.service_settings.remove(setting_id);

        let default_value = get_default_service_setting(setting_id);
        Ok(json_resp(json!({
            "ServiceSetting": {
                "SettingId": setting_id,
                "SettingValue": default_value,
                "LastModifiedDate": Utc::now().timestamp_millis() as f64 / 1000.0,
                "LastModifiedUser": "System",
                "ARN": Arn::new("ssm", &state.region, &state.account_id, &format!("servicesetting/{setting_id}")).to_string(),
                "Status": "Default",
            }
        })))
    }

    pub(super) fn update_service_setting(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = parse_body(req);
        validate_optional_string_length("SettingId", body["SettingId"].as_str(), 1, 1000)?;
        validate_optional_string_length("SettingValue", body["SettingValue"].as_str(), 1, 4096)?;
        let setting_id = body["SettingId"]
            .as_str()
            .ok_or_else(|| missing("SettingId"))?
            .to_string();
        let setting_value = body["SettingValue"]
            .as_str()
            .ok_or_else(|| missing("SettingValue"))?
            .to_string();

        let mut state = self.state.write();
        let now = Utc::now();
        let account_id = state.account_id.clone();
        state.service_settings.insert(
            setting_id.clone(),
            SsmServiceSetting {
                setting_id,
                setting_value,
                last_modified_date: now,
                last_modified_user: Arn::global("iam", &account_id, "root").to_string(),
                status: "Customized".to_string(),
            },
        );

        Ok(json_resp(json!({})))
    }

    // ── Inventory ─────────────────────────────────────────────────
}

pub(super) fn get_default_service_setting(setting_id: &str) -> String {
    match setting_id {
        s if s.contains("parameter-store") && s.contains("high-throughput") => "false".to_string(),
        s if s.contains("parameter-store") && s.contains("throughput") => "standard".to_string(),
        s if s.contains("session-manager") => "".to_string(),
        s if s.contains("managed-instance") => "".to_string(),
        _ => "".to_string(),
    }
}
