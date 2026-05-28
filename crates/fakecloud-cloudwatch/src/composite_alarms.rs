//! Composite alarms: PutCompositeAlarm stores into the alarm store so
//! DescribeAlarms returns them under `<CompositeAlarms>`. DeleteAlarms (in
//! service.rs) already removes them.
//!
//! PutCompositeAlarm declares only `LimitExceededFault`, but the conformance
//! probe still expects 4xx on the negative variants (omitted required fields,
//! out-of-range lengths). Those are accepted as `AnyError`, so it validates
//! AlarmName/AlarmRule presence and field lengths while never returning an
//! undeclared error on a well-formed (Success-class) request.

use chrono::Utc;

use fakecloud_core::query::{optional_query_param, required_query_param};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::{
    collect_member_values, empty_metadata_response, validate_len, xml_escape, CloudWatchService,
};
use crate::state::{AlarmState, CompositeAlarm};

impl CloudWatchService {
    pub(crate) fn put_composite_alarm(
        &self,
        req: &AwsRequest,
    ) -> Result<AwsResponse, AwsServiceError> {
        validate_len(req, "AlarmName", 1, 255)?;
        validate_len(req, "AlarmRule", 1, 10240)?;
        validate_len(req, "AlarmDescription", 0, 1024)?;
        validate_len(req, "ActionsSuppressor", 1, 1600)?;
        let alarm_name = required_query_param(req, "AlarmName")?;
        let alarm_rule = required_query_param(req, "AlarmRule")?;
        let alarm_description = optional_query_param(req, "AlarmDescription");
        let actions_enabled = optional_query_param(req, "ActionsEnabled")
            .map(|s| s.eq_ignore_ascii_case("true"))
            .unwrap_or(true);
        let ok_actions = collect_member_values(req, "OKActions");
        let alarm_actions = collect_member_values(req, "AlarmActions");
        let insufficient_data_actions = collect_member_values(req, "InsufficientDataActions");
        let actions_suppressor = optional_query_param(req, "ActionsSuppressor");
        let actions_suppressor_wait_period =
            optional_query_param(req, "ActionsSuppressorWaitPeriod").and_then(|s| s.parse().ok());
        let actions_suppressor_extension_period =
            optional_query_param(req, "ActionsSuppressorExtensionPeriod")
                .and_then(|s| s.parse().ok());

        let arn = format!(
            "arn:aws:cloudwatch:{}:{}:alarm:{}",
            req.region, req.account_id, alarm_name
        );
        let now = Utc::now();

        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        let composites = acct.composite_alarms_in_mut(&req.region);
        let existing = composites.get(&alarm_name).cloned();
        let alarm = CompositeAlarm {
            alarm_name: alarm_name.clone(),
            alarm_arn: arn,
            alarm_description,
            alarm_rule,
            actions_enabled,
            ok_actions,
            alarm_actions,
            insufficient_data_actions,
            actions_suppressor,
            actions_suppressor_wait_period,
            actions_suppressor_extension_period,
            state_value: existing
                .as_ref()
                .map(|a| a.state_value)
                .unwrap_or(AlarmState::InsufficientData),
            state_reason: existing
                .as_ref()
                .map(|a| a.state_reason.clone())
                .unwrap_or_else(|| "Unchecked: Initial alarm creation".to_string()),
            state_updated_timestamp: existing
                .as_ref()
                .map(|a| a.state_updated_timestamp)
                .unwrap_or(now),
            alarm_configuration_updated_timestamp: now,
        };
        composites.insert(alarm_name, alarm);

        Ok(empty_metadata_response(
            "PutCompositeAlarm",
            &req.request_id,
        ))
    }
}

pub(crate) fn render_composite_alarm(alarm: &CompositeAlarm) -> String {
    let mut s = String::from("<member>");
    s.push_str(&format!(
        "<AlarmName>{}</AlarmName>",
        xml_escape(&alarm.alarm_name)
    ));
    s.push_str(&format!(
        "<AlarmArn>{}</AlarmArn>",
        xml_escape(&alarm.alarm_arn)
    ));
    if let Some(d) = &alarm.alarm_description {
        s.push_str(&format!(
            "<AlarmDescription>{}</AlarmDescription>",
            xml_escape(d)
        ));
    }
    s.push_str(&format!(
        "<ActionsEnabled>{}</ActionsEnabled>",
        alarm.actions_enabled
    ));
    push_actions(&mut s, "OKActions", &alarm.ok_actions);
    push_actions(&mut s, "AlarmActions", &alarm.alarm_actions);
    push_actions(
        &mut s,
        "InsufficientDataActions",
        &alarm.insufficient_data_actions,
    );
    s.push_str(&format!(
        "<AlarmRule>{}</AlarmRule>",
        xml_escape(&alarm.alarm_rule)
    ));
    s.push_str(&format!(
        "<StateValue>{}</StateValue>",
        alarm.state_value.as_str()
    ));
    s.push_str(&format!(
        "<StateReason>{}</StateReason>",
        xml_escape(&alarm.state_reason)
    ));
    s.push_str(&format!(
        "<StateUpdatedTimestamp>{}</StateUpdatedTimestamp>",
        alarm
            .state_updated_timestamp
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    ));
    if let Some(a) = &alarm.actions_suppressor {
        s.push_str(&format!(
            "<ActionsSuppressor>{}</ActionsSuppressor>",
            xml_escape(a)
        ));
    }
    if let Some(w) = alarm.actions_suppressor_wait_period {
        s.push_str(&format!(
            "<ActionsSuppressorWaitPeriod>{w}</ActionsSuppressorWaitPeriod>"
        ));
    }
    if let Some(e) = alarm.actions_suppressor_extension_period {
        s.push_str(&format!(
            "<ActionsSuppressorExtensionPeriod>{e}</ActionsSuppressorExtensionPeriod>"
        ));
    }
    s.push_str(&format!(
        "<AlarmConfigurationUpdatedTimestamp>{}</AlarmConfigurationUpdatedTimestamp>",
        alarm
            .alarm_configuration_updated_timestamp
            .to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
    ));
    s.push_str("</member>");
    s
}

fn push_actions(s: &mut String, name: &str, actions: &[String]) {
    s.push_str(&format!("<{name}>"));
    for action in actions {
        s.push_str(&format!("<member>{}</member>", xml_escape(action)));
    }
    s.push_str(&format!("</{name}>"));
}
