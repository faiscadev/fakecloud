//! Log alarms: `PutLogAlarm` stores into the alarm store so `DescribeAlarms`
//! returns them under `<LogAlarms>` when `AlarmTypes` asks for `LogAlarm`, and
//! `DeleteAlarms` removes them alongside metric and composite alarms.
//!
//! A log alarm evaluates the results of a service-managed CloudWatch Logs
//! scheduled query against a threshold. fakecloud does not run the query on a
//! schedule, so a fresh alarm sits in `INSUFFICIENT_DATA` — the same state a
//! real alarm holds until its first evaluation lands — and `SetAlarmState`
//! drives it from there.

use chrono::Utc;
use http::StatusCode;

use fakecloud_core::query::{optional_query_param, required_query_param};
use fakecloud_core::service::{AwsRequest, AwsResponse, AwsServiceError};

use crate::service::{
    collect_member_values, empty_metadata_response, validate_len, xml_escape, CloudWatchService,
};
use crate::state::{AlarmState, LogAlarm};

/// `ComparisonOperator` as modeled for log alarms — the metric-alarm set plus
/// the three range operators.
pub(crate) const COMPARISON_OPERATORS: &[&str] = &[
    "GreaterThanOrEqualToThreshold",
    "GreaterThanThreshold",
    "LessThanThreshold",
    "LessThanOrEqualToThreshold",
    "LessThanLowerOrGreaterThanUpperThreshold",
    "LessThanLowerThreshold",
    "GreaterThanUpperThreshold",
];

fn validation(msg: impl Into<String>) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationError", msg)
}

/// Read a required positive integer member, rejecting a missing, non-numeric
/// or below-minimum value. `QueryResultsToEvaluate` and `QueryResultsToAlarm`
/// are both modeled with `min: 1`.
fn required_positive_i64(req: &AwsRequest, key: &str) -> Result<i64, AwsServiceError> {
    let raw = required_query_param(req, key)?;
    let n = raw
        .parse::<i64>()
        .map_err(|_| validation(format!("{key} must be an integer")))?;
    if n < 1 {
        return Err(validation(format!("{key} must be at least 1")));
    }
    Ok(n)
}

impl CloudWatchService {
    pub(crate) fn put_log_alarm(&self, req: &AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        validate_len(req, "AlarmName", 1, 255)?;
        validate_len(req, "AlarmDescription", 0, 1024)?;
        let alarm_name = required_query_param(req, "AlarmName")?;

        // ScheduledQueryConfiguration's required members arrive flattened.
        let query_string = required_query_param(req, "ScheduledQueryConfiguration.QueryString")?;
        let scheduled_query_role_arn =
            required_query_param(req, "ScheduledQueryConfiguration.ScheduledQueryRoleARN")?;
        let aggregation_expression =
            required_query_param(req, "ScheduledQueryConfiguration.AggregationExpression")?;
        // ScheduleConfiguration itself is required; its ScheduleExpression is
        // the member that actually drives the cadence.
        let schedule_expression = optional_query_param(
            req,
            "ScheduledQueryConfiguration.ScheduleConfiguration.ScheduleExpression",
        );
        if schedule_expression.is_none() {
            return Err(validation(
                "ScheduledQueryConfiguration.ScheduleConfiguration is required",
            ));
        }

        let query_results_to_evaluate = required_positive_i64(req, "QueryResultsToEvaluate")?;
        let query_results_to_alarm = required_positive_i64(req, "QueryResultsToAlarm")?;
        // A log alarm cannot need more datapoints to alarm than it evaluates.
        if query_results_to_alarm > query_results_to_evaluate {
            return Err(validation(
                "QueryResultsToAlarm cannot exceed QueryResultsToEvaluate",
            ));
        }

        let threshold = required_query_param(req, "Threshold")?
            .parse::<f64>()
            .map_err(|_| validation("Threshold must be a number"))?;
        let comparison_operator = required_query_param(req, "ComparisonOperator")?;
        if !COMPARISON_OPERATORS.contains(&comparison_operator.as_str()) {
            return Err(validation(format!(
                "ComparisonOperator has an invalid value '{comparison_operator}'"
            )));
        }
        let treat_missing_data = optional_query_param(req, "TreatMissingData");
        if let Some(t) = &treat_missing_data {
            if !matches!(
                t.as_str(),
                "breaching" | "notBreaching" | "ignore" | "missing"
            ) {
                return Err(validation(format!(
                    "TreatMissingData has an invalid value '{t}'"
                )));
            }
        }

        let arn = format!(
            "arn:aws:cloudwatch:{}:{}:alarm:{}",
            req.region, req.account_id, alarm_name
        );
        let now = Utc::now();

        let mut state = self.state.write();
        let acct = state.get_or_create(&req.account_id);
        let alarms = acct.log_alarms_in_mut(&req.region);
        let existing = alarms.get(&alarm_name).cloned();
        let alarm = LogAlarm {
            alarm_name: alarm_name.clone(),
            alarm_arn: arn,
            alarm_description: optional_query_param(req, "AlarmDescription"),
            query_string,
            scheduled_query_role_arn,
            schedule_expression,
            schedule_start_time_offset: optional_query_param(
                req,
                "ScheduledQueryConfiguration.ScheduleConfiguration.StartTimeOffset",
            )
            .and_then(|s| s.parse().ok()),
            schedule_end_time_offset: optional_query_param(
                req,
                "ScheduledQueryConfiguration.ScheduleConfiguration.EndTimeOffset",
            )
            .and_then(|s| s.parse().ok()),
            aggregation_expression,
            log_group_identifiers: collect_member_values(
                req,
                "ScheduledQueryConfiguration.LogGroupIdentifiers",
            ),
            query_arn: optional_query_param(req, "ScheduledQueryConfiguration.QueryARN"),
            query_results_to_evaluate,
            query_results_to_alarm,
            threshold,
            comparison_operator,
            treat_missing_data,
            action_log_line_count: optional_query_param(req, "ActionLogLineCount")
                .and_then(|s| s.parse().ok()),
            action_log_line_role_arn: optional_query_param(req, "ActionLogLineRoleArn"),
            actions_enabled: optional_query_param(req, "ActionsEnabled")
                .map(|s| s.eq_ignore_ascii_case("true"))
                .unwrap_or(true),
            ok_actions: collect_member_values(req, "OKActions"),
            alarm_actions: collect_member_values(req, "AlarmActions"),
            insufficient_data_actions: collect_member_values(req, "InsufficientDataActions"),
            // An update keeps the alarm's current state; a new alarm starts
            // unevaluated, exactly as PutCompositeAlarm does.
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
        alarms.insert(alarm_name, alarm);

        Ok(empty_metadata_response("PutLogAlarm", &req.request_id))
    }
}

pub(crate) fn render_log_alarm(alarm: &LogAlarm) -> String {
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
        "<AlarmConfigurationUpdatedTimestamp>{}</AlarmConfigurationUpdatedTimestamp>",
        alarm
            .alarm_configuration_updated_timestamp
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
    ));
    s.push_str(&format!(
        "<ActionsEnabled>{}</ActionsEnabled>",
        alarm.actions_enabled
    ));
    for (tag, actions) in [
        ("OKActions", &alarm.ok_actions),
        ("AlarmActions", &alarm.alarm_actions),
        ("InsufficientDataActions", &alarm.insufficient_data_actions),
    ] {
        s.push_str(&format!("<{tag}>"));
        for a in actions {
            s.push_str(&format!("<member>{}</member>", xml_escape(a)));
        }
        s.push_str(&format!("</{tag}>"));
    }
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
            .format("%Y-%m-%dT%H:%M:%S%.3fZ")
    ));

    s.push_str("<ScheduledQueryConfiguration>");
    s.push_str(&format!(
        "<QueryString>{}</QueryString>",
        xml_escape(&alarm.query_string)
    ));
    s.push_str(&format!(
        "<ScheduledQueryRoleARN>{}</ScheduledQueryRoleARN>",
        xml_escape(&alarm.scheduled_query_role_arn)
    ));
    s.push_str(&format!(
        "<AggregationExpression>{}</AggregationExpression>",
        xml_escape(&alarm.aggregation_expression)
    ));
    if let Some(q) = &alarm.query_arn {
        s.push_str(&format!("<QueryARN>{}</QueryARN>", xml_escape(q)));
    }
    if !alarm.log_group_identifiers.is_empty() {
        s.push_str("<LogGroupIdentifiers>");
        for g in &alarm.log_group_identifiers {
            s.push_str(&format!("<member>{}</member>", xml_escape(g)));
        }
        s.push_str("</LogGroupIdentifiers>");
    }
    s.push_str("<ScheduleConfiguration>");
    if let Some(e) = &alarm.schedule_expression {
        s.push_str(&format!(
            "<ScheduleExpression>{}</ScheduleExpression>",
            xml_escape(e)
        ));
    }
    if let Some(o) = alarm.schedule_start_time_offset {
        s.push_str(&format!("<StartTimeOffset>{o}</StartTimeOffset>"));
    }
    if let Some(o) = alarm.schedule_end_time_offset {
        s.push_str(&format!("<EndTimeOffset>{o}</EndTimeOffset>"));
    }
    s.push_str("</ScheduleConfiguration>");
    s.push_str("</ScheduledQueryConfiguration>");

    s.push_str(&format!(
        "<QueryResultsToEvaluate>{}</QueryResultsToEvaluate>",
        alarm.query_results_to_evaluate
    ));
    s.push_str(&format!(
        "<QueryResultsToAlarm>{}</QueryResultsToAlarm>",
        alarm.query_results_to_alarm
    ));
    s.push_str(&format!("<Threshold>{}</Threshold>", alarm.threshold));
    s.push_str(&format!(
        "<ComparisonOperator>{}</ComparisonOperator>",
        xml_escape(&alarm.comparison_operator)
    ));
    if let Some(t) = &alarm.treat_missing_data {
        s.push_str(&format!(
            "<TreatMissingData>{}</TreatMissingData>",
            xml_escape(t)
        ));
    }
    if let Some(c) = alarm.action_log_line_count {
        s.push_str(&format!("<ActionLogLineCount>{c}</ActionLogLineCount>"));
    }
    if let Some(r) = &alarm.action_log_line_role_arn {
        s.push_str(&format!(
            "<ActionLogLineRoleArn>{}</ActionLogLineRoleArn>",
            xml_escape(r)
        ));
    }
    s.push_str("</member>");
    s
}
