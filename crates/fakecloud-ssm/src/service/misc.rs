use chrono::{DateTime, NaiveDate, NaiveDateTime, TimeZone, Utc};
use serde_json::json;

use fakecloud_aws::arn::Arn;
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};
use fakecloud_core::validation::*;

use crate::state::{SsmServiceSetting, SsmState};

use super::documents::doc_not_found;
use super::helpers::aws_400;
use super::{missing, SsmService};

impl SsmService {
    pub(super) fn get_connection_status(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("Target", body["Target"].as_str(), 1, 400)?;
        let target = body["Target"].as_str().ok_or_else(|| missing("Target"))?;
        Ok(AwsResponse::ok_json(json!({
            "Target": target,
            "Status": "connected",
        })))
    }

    /// Evaluate one or more Change Calendar documents at a point in time.
    ///
    /// A Change Calendar document holds an iCalendar (RFC 5545) body whose
    /// `X-CALENDAR-TYPE` property sets the default state (`DEFAULT_OPEN` or
    /// `DEFAULT_CLOSED`); each `VEVENT` marks a window during which the state is
    /// the opposite of the default. When several calendars are supplied the
    /// aggregate is `CLOSED` if any one of them is `CLOSED` at `AtTime`.
    pub(super) fn get_calendar_state(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        let calendar_names = body["CalendarNames"]
            .as_array()
            .ok_or_else(|| missing("CalendarNames"))?;
        if calendar_names.is_empty() {
            return Err(missing("CalendarNames"));
        }

        // Requested evaluation instant; echoed back verbatim in the response.
        let at_time = match body["AtTime"].as_str() {
            Some(s) => parse_iso8601(s).ok_or_else(|| {
                aws_400(
                    "ValidationException",
                    "AtTime is not a valid ISO 8601 timestamp",
                )
            })?,
            None => Utc::now(),
        };
        let at_time_str = at_time.format("%Y-%m-%dT%H:%M:%SZ").to_string();

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);

        let mut aggregate_closed = false;
        let mut next_transition: Option<DateTime<Utc>> = None;

        for entry in calendar_names {
            let raw = entry.as_str().ok_or_else(|| {
                aws_400(
                    "ValidationException",
                    "CalendarNames entries must be strings",
                )
            })?;
            let name = calendar_document_key(raw);
            let doc = state
                .documents
                .get(&name)
                .ok_or_else(|| doc_not_found(&name))?;
            if doc.document_type != "ChangeCalendar" {
                return Err(aws_400(
                    "InvalidDocumentType",
                    format!("Document {name} is not a Change Calendar"),
                ));
            }

            let calendar = parse_change_calendar(&doc.content);
            if calendar.is_closed_at(at_time) {
                aggregate_closed = true;
            }
            if let Some(t) = calendar.next_transition_after(at_time) {
                next_transition = Some(match next_transition {
                    Some(cur) => cur.min(t),
                    None => t,
                });
            }
        }

        let mut resp = json!({
            "State": if aggregate_closed { "CLOSED" } else { "OPEN" },
            "AtTime": at_time_str,
        });
        if let Some(t) = next_transition {
            resp["NextTransitionTime"] = json!(t.format("%Y-%m-%dT%H:%M:%SZ").to_string());
        }
        Ok(AwsResponse::ok_json(resp))
    }

    pub(super) fn get_service_setting(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("SettingId", body["SettingId"].as_str(), 1, 1000)?;
        let setting_id = body["SettingId"]
            .as_str()
            .ok_or_else(|| missing("SettingId"))?;

        let accounts = self.state.read();
        let empty = SsmState::new(&req.account_id, &req.region);
        let state = accounts.get(&req.account_id).unwrap_or(&empty);
        if let Some(setting) = state.service_settings.get(setting_id) {
            Ok(AwsResponse::ok_json(json!({
                "ServiceSetting": {
                    "SettingId": setting.setting_id,
                    "SettingValue": setting.setting_value,
                    "LastModifiedDate": setting.last_modified_date.timestamp_millis() as f64 / 1000.0,
                    "LastModifiedUser": setting.last_modified_user,
                    "ARN": Arn::new("ssm", req.region.as_str(), &state.account_id, &format!("servicesetting/{}", setting.setting_id)).to_string(),
                    "Status": setting.status,
                }
            })))
        } else {
            // Return sensible default for known settings
            Ok(AwsResponse::ok_json(json!({
                "ServiceSetting": {
                    "SettingId": setting_id,
                    "SettingValue": get_default_service_setting(setting_id),
                    "LastModifiedDate": Utc::now().timestamp_millis() as f64 / 1000.0,
                    "LastModifiedUser": "System",
                    "ARN": Arn::new("ssm", req.region.as_str(), &state.account_id, &format!("servicesetting/{setting_id}")).to_string(),
                    "Status": "Default",
                }
            })))
        }
    }

    pub(super) fn reset_service_setting(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
        validate_optional_string_length("SettingId", body["SettingId"].as_str(), 1, 1000)?;
        let setting_id = body["SettingId"]
            .as_str()
            .ok_or_else(|| missing("SettingId"))?;

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
        state.service_settings.remove(setting_id);

        let default_value = get_default_service_setting(setting_id);
        Ok(AwsResponse::ok_json(json!({
            "ServiceSetting": {
                "SettingId": setting_id,
                "SettingValue": default_value,
                "LastModifiedDate": Utc::now().timestamp_millis() as f64 / 1000.0,
                "LastModifiedUser": "System",
                "ARN": Arn::new("ssm", req.region.as_str(), &state.account_id, &format!("servicesetting/{setting_id}")).to_string(),
                "Status": "Default",
            }
        })))
    }

    pub(super) fn update_service_setting(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        let body = req.json_body();
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

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(&req.account_id);
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

        Ok(AwsResponse::ok_json(json!({})))
    }

    // ── Inventory ─────────────────────────────────────────────────
}

/// Resolve a `CalendarNames` entry (either a bare document name or a
/// `arn:aws:ssm:...:document/<name>` ARN) to the key used in `state.documents`.
fn calendar_document_key(raw: &str) -> String {
    if raw.starts_with("arn:") {
        if let Ok(arn) = raw.parse::<Arn>() {
            // resource looks like `document/my-cal`
            if let Some(name) = arn.resource.strip_prefix("document/") {
                return name.to_string();
            }
            return arn.resource;
        }
    }
    raw.to_string()
}

/// Parse an ISO 8601 timestamp. Accepts RFC 3339 (`2022-11-30T23:00:00Z`,
/// with optional fractional seconds/offset) and the compact iCalendar form
/// (`20221130T230000Z`).
fn parse_iso8601(s: &str) -> Option<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    parse_ical_datetime(s)
}

/// A parsed Change Calendar: its default state plus the event windows during
/// which the state is the opposite of the default.
struct ChangeCalendar {
    /// `true` when `X-CALENDAR-TYPE` is `DEFAULT_OPEN` (the AWS default).
    default_open: bool,
    /// `(start, end)` windows; state is flipped from the default while inside.
    windows: Vec<(DateTime<Utc>, DateTime<Utc>)>,
}

impl ChangeCalendar {
    fn is_closed_at(&self, at: DateTime<Utc>) -> bool {
        let inside = self.windows.iter().any(|(s, e)| at >= *s && at < *e);
        // Inside a window the state is the inverse of the default.
        if inside {
            self.default_open
        } else {
            !self.default_open
        }
    }

    /// The earliest window boundary strictly after `at`, i.e. the next instant
    /// the calendar flips state.
    fn next_transition_after(&self, at: DateTime<Utc>) -> Option<DateTime<Utc>> {
        self.windows
            .iter()
            .flat_map(|(s, e)| [*s, *e])
            .filter(|t| *t > at)
            .min()
    }
}

/// Parse a Change Calendar iCalendar (RFC 5545) body into its default state and
/// event windows. Unparseable bodies degrade to an all-OPEN calendar.
fn parse_change_calendar(content: &str) -> ChangeCalendar {
    let mut default_open = true;
    let mut windows = Vec::new();
    let mut cur_start: Option<DateTime<Utc>> = None;
    let mut cur_end: Option<DateTime<Utc>> = None;
    let mut in_event = false;

    for line in content.lines() {
        let line = line.trim();
        // Split off any property parameters: `NAME;PARAM=x:VALUE`.
        let (prop, value) = match line.split_once(':') {
            Some((p, v)) => (p, v.trim()),
            None => continue,
        };
        let key = prop.split(';').next().unwrap_or(prop).to_ascii_uppercase();
        match key.as_str() {
            "X-CALENDAR-TYPE" => {
                default_open = !value.eq_ignore_ascii_case("DEFAULT_CLOSED");
            }
            "BEGIN" if value.eq_ignore_ascii_case("VEVENT") => {
                in_event = true;
                cur_start = None;
                cur_end = None;
            }
            "END" if value.eq_ignore_ascii_case("VEVENT") => {
                if let (Some(s), Some(e)) = (cur_start, cur_end) {
                    windows.push((s, e));
                }
                in_event = false;
            }
            "DTSTART" if in_event => cur_start = parse_ical_datetime(value),
            "DTEND" if in_event => cur_end = parse_ical_datetime(value),
            _ => {}
        }
    }

    ChangeCalendar {
        default_open,
        windows,
    }
}

/// Parse an iCalendar DATE-TIME (`20221130T230000Z`) or DATE (`20221130`)
/// value into a UTC instant. Any zone qualifier is treated as UTC.
fn parse_ical_datetime(value: &str) -> Option<DateTime<Utc>> {
    let v = value.trim().trim_end_matches('Z');
    if let Some((date, time)) = v.split_once('T') {
        let d = NaiveDate::parse_from_str(date, "%Y%m%d").ok()?;
        let t = NaiveDateTime::parse_from_str(&format!("{date}T{time}"), "%Y%m%dT%H%M%S")
            .ok()
            .map(|dt| dt.time())
            .unwrap_or_else(|| d.and_hms_opt(0, 0, 0).unwrap().time());
        return Some(Utc.from_utc_datetime(&d.and_time(t)));
    }
    let d = NaiveDate::parse_from_str(v, "%Y%m%d").ok()?;
    Some(Utc.from_utc_datetime(&d.and_hms_opt(0, 0, 0).unwrap()))
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
