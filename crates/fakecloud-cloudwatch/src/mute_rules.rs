//! Alarm mute rules: Put/Get/List/Delete.
//!
//! PutAlarmMuteRule declares only `LimitExceededFault` and
//! DeleteAlarmMuteRule declares no errors. The conformance probe still
//! expects 4xx on negative variants (omitted required fields, out-of-range
//! lengths) — accepted as `AnyError` — while well-formed (Success-class)
//! requests must never 4xx. A delete of a non-existent rule is idempotent.

use chrono::{DateTime, Utc};

use fakecloud_core::query::{optional_query_param, required_query_param};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::{
    collect_member_values, empty_metadata_response, missing_param, not_found,
    parse_input_timestamp, validate_len, validate_range_i64, xml_escape, xml_response,
    CloudWatchService,
};
use crate::state::AlarmMuteRule;

fn parse_ts(req: &AwsRequest, name: &str) -> Option<DateTime<Utc>> {
    optional_query_param(req, name).and_then(|s| parse_input_timestamp(&s))
}

/// Compute the wire status of a mute rule from its start/expire dates.
fn rule_status(rule: &AlarmMuteRule, now: DateTime<Utc>) -> &'static str {
    if let Some(expire) = rule.expire_date {
        if now >= expire {
            return "EXPIRED";
        }
    }
    if let Some(start) = rule.start_date {
        if now < start {
            return "SCHEDULED";
        }
    }
    "ACTIVE"
}

impl CloudWatchService {
    pub(crate) fn put_alarm_mute_rule(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // `Name` is required; `Rule` is a nested Schedule structure on the
        // wire (Rule.Schedule.Expression / .Duration / .Timezone). The op
        // declares only LimitExceededFault, so it never rejects with an
        // undeclared error — the omitted-required negatives still 4xx via the
        // missing Name / missing Rule.Schedule checks (accepted as AnyError).
        validate_len(req, "Name", 1, 255)?;
        validate_len(req, "Description", 0, 1024)?;
        validate_len(req, "Rule.Schedule.Expression", 1, 256)?;
        validate_len(req, "Rule.Schedule.Duration", 1, 50)?;
        let name = required_query_param(req, "Name")?;
        if optional_query_param(req, "Rule.Schedule.Expression").is_none() {
            return Err(missing_param("Rule"));
        }
        let description = optional_query_param(req, "Description");
        let schedule_expression = optional_query_param(req, "Rule.Schedule.Expression");
        let schedule_duration = optional_query_param(req, "Rule.Schedule.Duration");
        let schedule_timezone = optional_query_param(req, "Rule.Schedule.Timezone");
        let mute_target_alarm_names = collect_member_values(req, "MuteTargets.AlarmNames");
        let start_date = parse_ts(req, "StartDate");
        let expire_date = parse_ts(req, "ExpireDate");

        let arn = format!(
            "arn:aws:cloudwatch:{}:{}:alarm-mute-rule/{name}",
            req.region, req.account_id
        );
        let mute_rule = AlarmMuteRule {
            name: name.clone(),
            arn,
            description,
            schedule_expression,
            schedule_duration,
            schedule_timezone,
            mute_target_alarm_names,
            start_date,
            expire_date,
            last_updated_timestamp: Utc::now(),
        };
        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        acct.mute_rules_in_mut(&req.region).insert(name, mute_rule);
        Ok(empty_metadata_response("PutAlarmMuteRule", &req.request_id))
    }

    pub(crate) fn get_alarm_mute_rule(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // A required name that is omitted is a validation error; a present but
        // unknown name is the declared ResourceNotFoundException below.
        let name = optional_query_param(req, "AlarmMuteRuleName")
            .ok_or_else(|| missing_param("AlarmMuteRuleName"))?;
        let state = self.state.read();
        let rule = state
            .get(&req.account_id)
            .and_then(|a| a.mute_rules_in(&req.region))
            .and_then(|m| m.get(&name))
            .cloned()
            .ok_or_else(|| not_found(format!("Mute rule {name} does not exist")))?;

        let now = Utc::now();
        let status = rule_status(&rule, now);
        let mut inner = format!("<Name>{}</Name>", xml_escape(&rule.name));
        inner.push_str(&format!(
            "<AlarmMuteRuleArn>{}</AlarmMuteRuleArn>",
            xml_escape(&rule.arn)
        ));
        if let Some(d) = &rule.description {
            inner.push_str(&format!("<Description>{}</Description>", xml_escape(d)));
        }
        inner.push_str("<Rule><Schedule>");
        if let Some(e) = &rule.schedule_expression {
            inner.push_str(&format!("<Expression>{}</Expression>", xml_escape(e)));
        }
        if let Some(d) = &rule.schedule_duration {
            inner.push_str(&format!("<Duration>{}</Duration>", xml_escape(d)));
        }
        if let Some(tz) = &rule.schedule_timezone {
            inner.push_str(&format!("<Timezone>{}</Timezone>", xml_escape(tz)));
        }
        inner.push_str("</Schedule></Rule>");
        inner.push_str("<MuteTargets><AlarmNames>");
        for n in &rule.mute_target_alarm_names {
            inner.push_str(&format!("<member>{}</member>", xml_escape(n)));
        }
        inner.push_str("</AlarmNames></MuteTargets>");
        if let Some(d) = rule.start_date {
            inner.push_str(&format!(
                "<StartDate>{}</StartDate>",
                d.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
            ));
        }
        if let Some(d) = rule.expire_date {
            inner.push_str(&format!(
                "<ExpireDate>{}</ExpireDate>",
                d.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
            ));
        }
        inner.push_str(&format!("<Status>{status}</Status>"));
        inner.push_str(&format!(
            "<LastUpdatedTimestamp>{}</LastUpdatedTimestamp>",
            rule.last_updated_timestamp
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
        ));
        inner.push_str("<MuteType>TARGET_ALARMS</MuteType>");
        Ok(xml_response("GetAlarmMuteRule", &inner, &req.request_id))
    }

    pub(crate) fn list_alarm_mute_rules(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_len(req, "AlarmName", 1, 255)?;
        validate_range_i64(req, "MaxRecords", 1, 100)?;
        let now = Utc::now();
        let state = self.state.read();
        let mut inner = String::from("<AlarmMuteRuleSummaries>");
        if let Some(acct) = state.get(&req.account_id) {
            if let Some(rules) = acct.mute_rules_in(&req.region) {
                for rule in rules.values() {
                    inner.push_str("<member>");
                    inner.push_str(&format!(
                        "<AlarmMuteRuleArn>{}</AlarmMuteRuleArn>",
                        xml_escape(&rule.arn)
                    ));
                    if let Some(d) = rule.expire_date {
                        inner.push_str(&format!(
                            "<ExpireDate>{}</ExpireDate>",
                            d.to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                        ));
                    }
                    inner.push_str(&format!("<Status>{}</Status>", rule_status(rule, now)));
                    inner.push_str("<MuteType>TARGET_ALARMS</MuteType>");
                    inner.push_str(&format!(
                        "<LastUpdatedTimestamp>{}</LastUpdatedTimestamp>",
                        rule.last_updated_timestamp
                            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
                    ));
                    inner.push_str("</member>");
                }
            }
        }
        inner.push_str("</AlarmMuteRuleSummaries>");
        Ok(xml_response("ListAlarmMuteRules", &inner, &req.request_id))
    }

    pub(crate) fn delete_alarm_mute_rule(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        // No declared errors, but negative variants (omit / out-of-range
        // length) must 4xx; a valid-but-absent rule deletes idempotently.
        validate_len(req, "AlarmMuteRuleName", 1, 255)?;
        let name = optional_query_param(req, "AlarmMuteRuleName")
            .ok_or_else(|| missing_param("AlarmMuteRuleName"))?;
        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        acct.mute_rules_in_mut(&req.region).remove(&name);
        Ok(empty_metadata_response(
            "DeleteAlarmMuteRule",
            &req.request_id,
        ))
    }
}
