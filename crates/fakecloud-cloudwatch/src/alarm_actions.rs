//! Alarm actions: what an alarm does when its state changes.
//!
//! When an alarm with `ActionsEnabled` changes `StateValue`, CloudWatch runs
//! the actions listed for the new state (`AlarmActions`, `OKActions`,
//! `InsufficientDataActions`):
//!
//! - an SNS topic ARN gets the alarm notification JSON (Subject
//!   `ALARM: "<name>" in <Region long name>`), published through the shared
//!   SNS delivery so every subscription protocol receives it;
//! - a Lambda function ARN is invoked asynchronously with the alarm event;
//! - an EC2 action (`arn:aws:automate:<region>:ec2:stop|terminate|reboot|recover`
//!   or `arn:aws:swf:<region>:<account>:action/actions/AWS_EC2.InstanceId.<Op>/1.0`)
//!   acts on the instance named by the alarm's `InstanceId` dimension through
//!   the EC2 service;
//! - Application Auto Scaling policy ARNs are acted on by the scaling
//!   watcher, which reads the alarm state itself.
//!
//! Each executed action is recorded in the alarm history as an `Action` item.
//! A composite alarm whose actions its `ActionsSuppressor` holds back performs
//! them once the suppression ends, if it is still in ALARM.

use std::future::Future;
use std::pin::Pin;

use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};

use fakecloud_core::delivery::DeliveryBus;

use crate::alarm_eval::reason_timestamp;
use crate::state::{
    AlarmHistoryItem, AlarmState, CompositeAlarm, LogAlarm, MetricAlarm, PreviousState,
};

/// An EC2 instance action an alarm can take.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ec2InstanceAction {
    Stop,
    Terminate,
    Reboot,
    /// Recover the instance onto healthy hardware: the instance keeps its ID
    /// and is restarted.
    Recover,
}

impl Ec2InstanceAction {
    /// The EC2 API operation that performs the action.
    pub fn operation(&self) -> &'static str {
        match self {
            Ec2InstanceAction::Stop => "StopInstances",
            Ec2InstanceAction::Terminate => "TerminateInstances",
            Ec2InstanceAction::Reboot | Ec2InstanceAction::Recover => "RebootInstances",
        }
    }
}

/// Performs EC2 alarm actions. The server implements it on top of the EC2
/// service so this crate needn't depend on it.
pub trait Ec2AlarmActionHook: Send + Sync {
    fn perform(
        &self,
        account_id: &str,
        region: &str,
        instance_id: &str,
        action: Ec2InstanceAction,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send>>;
}

/// Who may perform an alarm action on its target, as the target's resource
/// policy names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantPrincipal<'a> {
    /// An AWS service principal (`lambda.alarms.cloudwatch.amazonaws.com`).
    Service(&'static str),
    /// An account (its root), for a cross-account target.
    Account(&'a str),
}

/// A resource-policy check for an alarm action: may `principal` perform
/// `service:action` on `resource_arn` for the alarm `source_arn` of
/// `source_account`?
#[derive(Debug, Clone, Copy)]
pub struct ActionGrant<'a> {
    pub service: &'static str,
    pub action: &'static str,
    pub resource_arn: &'a str,
    pub principal: GrantPrincipal<'a>,
    pub source_arn: &'a str,
    pub source_account: &'a str,
}

/// Evaluates the resource policies of alarm action targets (a Lambda
/// function's policy, an SNS topic's policy). The server implements it over
/// the shared resource-policy providers and IAM evaluator.
pub trait AlarmActionAuthorizer: Send + Sync {
    fn allows(&self, grant: &ActionGrant<'_>) -> bool;
}

/// The service principal CloudWatch invokes Lambda alarm actions as; the
/// function's resource policy must grant it `lambda:InvokeFunction`.
pub const LAMBDA_ALARMS_PRINCIPAL: &str = "lambda.alarms.cloudwatch.amazonaws.com";

/// The kind of target an action ARN names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ActionTarget {
    Sns,
    Lambda,
    Ec2(Ec2InstanceAction),
    /// Application Auto Scaling policy, performed by the scaling watcher.
    AutoScaling,
    /// A valid action fakecloud doesn't perform (EC2 Auto Scaling group
    /// policies, Systems Manager OpsItems, Incident Manager response plans,
    /// Amazon Q investigations).
    NotPerformed,
    Unsupported,
}

/// Which kind of alarm an action belongs to (each accepts different action
/// types).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AlarmKind {
    Metric,
    Composite,
    Log,
}

/// Validate an action ARN as PutMetricAlarm / PutCompositeAlarm /
/// PutLogAlarm do: it must be one of the documented action forms for that
/// kind of alarm, and an EC2 action must name the alarm's own region.
pub(crate) fn validate_action(arn: &str, region: &str, kind: AlarmKind) -> Result<(), String> {
    let target = classify(arn);
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    let service = parts.get(2).copied().unwrap_or_default();
    let allowed = match (target, kind) {
        (ActionTarget::Sns | ActionTarget::Lambda, _) => true,
        (ActionTarget::Ec2(_) | ActionTarget::AutoScaling, AlarmKind::Metric) => true,
        (ActionTarget::NotPerformed, AlarmKind::Metric) => true,
        (ActionTarget::NotPerformed, AlarmKind::Composite) => {
            matches!(service, "ssm" | "aiops")
        }
        (ActionTarget::NotPerformed, AlarmKind::Log) => service == "ssm",
        _ => false,
    };
    if !allowed {
        return Err(format!("Invalid alarm action {arn}"));
    }
    if let ActionTarget::Ec2(_) = target {
        let action_region = parts.get(3).copied().unwrap_or_default();
        if action_region != region {
            return Err(format!(
                "Invalid region {action_region} specified. Only {region} is supported."
            ));
        }
    }
    Ok(())
}

pub(crate) fn classify(arn: &str) -> ActionTarget {
    let parts: Vec<&str> = arn.splitn(6, ':').collect();
    if parts.len() < 6 || parts[0] != "arn" || parts[5].is_empty() {
        return ActionTarget::Unsupported;
    }
    match parts[2] {
        // arn:aws:ssm:<region>:<account>:opsitem:<severity>[#CATEGORY=<c>]
        "ssm" if parts[5].starts_with("opsitem:") => return ActionTarget::NotPerformed,
        // arn:aws:ssm-incidents::<account>:responseplan/<name>
        "ssm-incidents" if parts[5].starts_with("responseplan/") => {
            return ActionTarget::NotPerformed
        }
        // arn:aws:aiops:<region>:<account>:investigation-group:<id>
        "aiops" if parts[5].starts_with("investigation-group:") => {
            return ActionTarget::NotPerformed
        }
        "autoscaling"
            if parts[5].starts_with("scalingPolicy:")
                && parts[5].contains(":autoScalingGroupName/") =>
        {
            return ActionTarget::NotPerformed
        }
        _ => {}
    }
    let ec2 = |op: &str| match op.to_ascii_lowercase().as_str() {
        "stop" => Some(Ec2InstanceAction::Stop),
        "terminate" => Some(Ec2InstanceAction::Terminate),
        "reboot" => Some(Ec2InstanceAction::Reboot),
        "recover" => Some(Ec2InstanceAction::Recover),
        _ => None,
    };
    match parts[2] {
        "sns" => ActionTarget::Sns,
        "lambda" if parts[5].starts_with("function:") => ActionTarget::Lambda,
        // Application Auto Scaling policies name their scalable target with
        // `:resource/`; EC2 Auto Scaling group policies aren't run from alarms.
        "autoscaling"
            if parts[5].starts_with("scalingPolicy:") && parts[5].contains(":resource/") =>
        {
            ActionTarget::AutoScaling
        }
        // arn:aws:automate:<region>:ec2:<op>
        "automate" if parts[4] == "ec2" => ec2(parts[5])
            .map(ActionTarget::Ec2)
            .unwrap_or(ActionTarget::Unsupported),
        // arn:aws:swf:<region>:<account>:action/actions/AWS_EC2.InstanceId.<Op>/1.0
        "swf" => parts[5]
            .strip_prefix("action/actions/AWS_EC2.InstanceId.")
            .and_then(|rest| rest.split('/').next())
            .and_then(ec2)
            .map(ActionTarget::Ec2)
            .unwrap_or(ActionTarget::Unsupported),
        _ => ActionTarget::Unsupported,
    }
}

/// The actions an alarm owes for one state change, with the payloads each
/// target type receives.
#[derive(Debug, Clone)]
pub(crate) struct AlarmNotification {
    pub account_id: String,
    pub region: String,
    pub alarm_name: String,
    pub alarm_type: &'static str,
    pub actions: Vec<String>,
    pub state_change_time: DateTime<Utc>,
    pub subject: String,
    pub sns_message: String,
    pub lambda_payload: String,
    /// The alarm's `InstanceId` dimension (target of EC2 actions).
    pub instance_id: Option<String>,
    /// The [`crate::state::CloudWatchState::epoch`] the state change happened
    /// in; the notification is dropped if the account was reset since.
    pub epoch: u64,
    /// The alarm's ARN (the `aws:SourceArn` its actions are authorized as).
    pub alarm_arn: String,
}

fn actions_for(
    state: AlarmState,
    ok: &[String],
    alarm: &[String],
    insufficient: &[String],
) -> Vec<String> {
    match state {
        AlarmState::Ok => ok.to_vec(),
        AlarmState::Alarm => alarm.to_vec(),
        AlarmState::InsufficientData => insufficient.to_vec(),
    }
}

/// `ALARM: "name" in US East (N. Virginia)`, cut to SNS's 100-character
/// subject limit.
fn subject(state: AlarmState, name: &str, region_name: &str) -> String {
    let s = format!("{}: \"{name}\" in {region_name}", state.as_str());
    s.chars().take(100).collect()
}

fn region_name(region: &str) -> String {
    fakecloud_aws::regions::long_name(region)
        .map(str::to_string)
        .unwrap_or_else(|| region.to_string())
}

/// The members every alarm notification carries.
#[allow(clippy::too_many_arguments)]
fn base_message(
    account_id: &str,
    region: &str,
    name: &str,
    arn: &str,
    description: Option<&str>,
    configuration_updated: DateTime<Utc>,
    new: (AlarmState, &str, DateTime<Utc>),
    old: AlarmState,
    actions: [&[String]; 3],
) -> Map<String, Value> {
    let mut m = Map::new();
    m.insert("AlarmName".into(), json!(name));
    m.insert("AlarmDescription".into(), json!(description));
    m.insert("AWSAccountId".into(), json!(account_id));
    m.insert(
        "AlarmConfigurationUpdatedTimestamp".into(),
        json!(reason_timestamp(configuration_updated)),
    );
    m.insert("NewStateValue".into(), json!(new.0.as_str()));
    m.insert("NewStateReason".into(), json!(new.1));
    m.insert("StateChangeTime".into(), json!(reason_timestamp(new.2)));
    m.insert("Region".into(), json!(region_name(region)));
    m.insert("AlarmArn".into(), json!(arn));
    m.insert("OldStateValue".into(), json!(old.as_str()));
    m.insert("OKActions".into(), json!(actions[0]));
    m.insert("AlarmActions".into(), json!(actions[1]));
    m.insert("InsufficientDataActions".into(), json!(actions[2]));
    m
}

/// `{"value","reason","reasonData","timestamp"}` of the Lambda alarm event.
fn lambda_state(
    value: AlarmState,
    reason: &str,
    reason_data: Option<&str>,
    ts: DateTime<Utc>,
) -> Value {
    let mut m = Map::new();
    m.insert("value".into(), json!(value.as_str()));
    m.insert("reason".into(), json!(reason));
    if let Some(d) = reason_data {
        m.insert("reasonData".into(), json!(d));
    }
    m.insert("timestamp".into(), json!(reason_timestamp(ts)));
    Value::Object(m)
}

/// The alarm a Lambda alarm event is about: (account, region, ARN, name).
type AlarmIdentity<'a> = (&'a str, &'a str, &'a str, &'a str);

fn lambda_payload(
    (account_id, region, arn, name): AlarmIdentity<'_>,
    at: DateTime<Utc>,
    state: Value,
    previous: Value,
    configuration: Value,
) -> String {
    json!({
        "source": "aws.cloudwatch",
        "alarmArn": arn,
        "accountId": account_id,
        "time": reason_timestamp(at),
        "region": region,
        "alarmData": {
            "alarmName": name,
            "state": state,
            "previousState": previous,
            "configuration": configuration,
        },
    })
    .to_string()
}

fn dimensions_list(dims: &std::collections::BTreeMap<String, String>) -> Value {
    Value::Array(
        dims.iter()
            .map(|(k, v)| json!({"value": v, "name": k}))
            .collect(),
    )
}

/// The `Trigger` of a metric alarm's notification.
fn metric_trigger(alarm: &MetricAlarm) -> Value {
    let mut t = Map::new();
    if alarm.metrics.is_empty() {
        t.insert("MetricName".into(), json!(alarm.metric_name));
        t.insert("Namespace".into(), json!(alarm.namespace));
        match (&alarm.extended_statistic, &alarm.statistic) {
            (Some(ext), _) => {
                t.insert("StatisticType".into(), json!("ExtendedStatistic"));
                t.insert("ExtendedStatistic".into(), json!(ext));
            }
            (None, stat) => {
                t.insert("StatisticType".into(), json!("Statistic"));
                t.insert(
                    "Statistic".into(),
                    json!(stat.as_deref().map(str::to_ascii_uppercase)),
                );
            }
        }
        t.insert("Unit".into(), json!(alarm.unit));
        t.insert("Dimensions".into(), dimensions_list(&alarm.dimensions));
        t.insert("Period".into(), json!(alarm.period));
    } else {
        let metrics: Vec<Value> = alarm
            .metrics
            .iter()
            .map(|q| {
                let mut m = Map::new();
                m.insert("Id".into(), json!(q.id));
                if let Some(e) = &q.expression {
                    m.insert("Expression".into(), json!(e));
                }
                if let Some(l) = &q.label {
                    m.insert("Label".into(), json!(l));
                }
                if let Some(ms) = &q.metric_stat {
                    m.insert(
                        "MetricStat".into(),
                        json!({
                            "Metric": {
                                "Dimensions": dimensions_list(&ms.dimensions),
                                "MetricName": ms.metric_name,
                                "Namespace": ms.namespace,
                            },
                            "Period": ms.period,
                            "Stat": ms.stat,
                        }),
                    );
                }
                m.insert("ReturnData".into(), json!(q.return_data.unwrap_or(true)));
                Value::Object(m)
            })
            .collect();
        t.insert("Metrics".into(), Value::Array(metrics));
        if let Some(tid) = &alarm.threshold_metric_id {
            t.insert("ThresholdMetricId".into(), json!(tid));
        }
    }
    t.insert("EvaluationPeriods".into(), json!(alarm.evaluation_periods));
    if let Some(d) = alarm.datapoints_to_alarm {
        t.insert("DatapointsToAlarm".into(), json!(d));
    }
    t.insert(
        "ComparisonOperator".into(),
        json!(alarm.comparison_operator),
    );
    if let Some(th) = alarm.threshold {
        t.insert("Threshold".into(), json!(th));
    }
    t.insert(
        "TreatMissingData".into(),
        json!(alarm.treat_missing_data.as_deref().unwrap_or("missing")),
    );
    t.insert(
        "EvaluateLowSampleCountPercentile".into(),
        json!(alarm
            .evaluate_low_sample_count_percentile
            .as_deref()
            .unwrap_or("")),
    );
    Value::Object(t)
}

/// The `configuration.metrics` of a metric alarm's Lambda event.
fn metric_lambda_configuration(alarm: &MetricAlarm) -> Value {
    let metrics: Vec<Value> = if alarm.metrics.is_empty() {
        vec![json!({
            "id": uuid::Uuid::new_v4().to_string(),
            "metricStat": {
                "metric": {
                    "namespace": alarm.namespace,
                    "name": alarm.metric_name,
                    "dimensions": alarm.dimensions,
                },
                "period": alarm.period,
                "stat": alarm.extended_statistic.clone().or_else(|| alarm.statistic.clone()),
            },
            "returnData": true,
        })]
    } else {
        alarm
            .metrics
            .iter()
            .map(|q| {
                let mut m = Map::new();
                m.insert("id".into(), json!(q.id));
                if let Some(e) = &q.expression {
                    m.insert("expression".into(), json!(e));
                }
                if let Some(l) = &q.label {
                    m.insert("label".into(), json!(l));
                }
                if let Some(ms) = &q.metric_stat {
                    m.insert(
                        "metricStat".into(),
                        json!({
                            "metric": {
                                "namespace": ms.namespace,
                                "name": ms.metric_name,
                                "dimensions": ms.dimensions,
                            },
                            "period": ms.period,
                            "stat": ms.stat,
                        }),
                    );
                }
                m.insert("returnData".into(), json!(q.return_data.unwrap_or(true)));
                Value::Object(m)
            })
            .collect()
    };
    let mut c = Map::new();
    if let Some(d) = &alarm.alarm_description {
        c.insert("description".into(), json!(d));
    }
    c.insert("metrics".into(), Value::Array(metrics));
    Value::Object(c)
}

/// The notification a metric alarm owes for its change from `prev` to its
/// current state, or `None` when it has no actions to run for it.
pub(crate) fn metric_alarm_notification(
    account_id: &str,
    region: &str,
    alarm: &MetricAlarm,
    prev: &PreviousState,
) -> Option<AlarmNotification> {
    if !alarm.actions_enabled || prev.value == alarm.state_value {
        return None;
    }
    let actions = actions_for(
        alarm.state_value,
        &alarm.ok_actions,
        &alarm.alarm_actions,
        &alarm.insufficient_data_actions,
    );
    if actions.is_empty() {
        return None;
    }
    let at = alarm.state_updated_timestamp;
    let mut msg = base_message(
        account_id,
        region,
        &alarm.alarm_name,
        &alarm.alarm_arn,
        alarm.alarm_description.as_deref(),
        alarm.alarm_configuration_updated_timestamp,
        (alarm.state_value, &alarm.state_reason, at),
        prev.value,
        [
            &alarm.ok_actions,
            &alarm.alarm_actions,
            &alarm.insufficient_data_actions,
        ],
    );
    msg.insert("Trigger".into(), metric_trigger(alarm));
    Some(AlarmNotification {
        account_id: account_id.to_string(),
        region: region.to_string(),
        alarm_name: alarm.alarm_name.clone(),
        alarm_type: "MetricAlarm",
        actions,
        state_change_time: at,
        subject: subject(alarm.state_value, &alarm.alarm_name, &region_name(region)),
        sns_message: Value::Object(msg).to_string(),
        lambda_payload: lambda_payload(
            (account_id, region, &alarm.alarm_arn, &alarm.alarm_name),
            at,
            lambda_state(
                alarm.state_value,
                &alarm.state_reason,
                alarm.state_reason_data.as_deref(),
                at,
            ),
            lambda_state(
                prev.value,
                &prev.reason,
                prev.reason_data.as_deref(),
                prev.timestamp,
            ),
            metric_lambda_configuration(alarm),
        ),
        instance_id: alarm.dimensions.get("InstanceId").cloned(),
        epoch: 0,
        alarm_arn: alarm.alarm_arn.clone(),
    })
}

/// The notification a composite alarm owes for its change from `prev`.
pub(crate) fn composite_alarm_notification(
    account_id: &str,
    region: &str,
    alarm: &CompositeAlarm,
    prev: &PreviousState,
) -> Option<AlarmNotification> {
    if !alarm.actions_enabled || prev.value == alarm.state_value {
        return None;
    }
    let actions = actions_for(
        alarm.state_value,
        &alarm.ok_actions,
        &alarm.alarm_actions,
        &alarm.insufficient_data_actions,
    );
    if actions.is_empty() {
        return None;
    }
    let at = alarm
        .state_transitioned_timestamp
        .unwrap_or(alarm.state_updated_timestamp);
    let mut msg = base_message(
        account_id,
        region,
        &alarm.alarm_name,
        &alarm.alarm_arn,
        alarm.alarm_description.as_deref(),
        alarm.alarm_configuration_updated_timestamp,
        (alarm.state_value, &alarm.state_reason, at),
        prev.value,
        [
            &alarm.ok_actions,
            &alarm.alarm_actions,
            &alarm.insufficient_data_actions,
        ],
    );
    msg.insert("AlarmRule".into(), json!(alarm.alarm_rule));
    let children: Vec<Value> = alarm
        .state_reason_data
        .as_deref()
        .and_then(|d| serde_json::from_str::<Value>(d).ok())
        .and_then(|v| v["triggeringAlarms"].as_array().cloned())
        .unwrap_or_default()
        .into_iter()
        .map(|c| {
            json!({
                "Arn": c["arn"],
                "State": {"Value": c["state"]["value"], "Timestamp": c["state"]["timestamp"]},
            })
        })
        .collect();
    msg.insert("TriggeringChildren".into(), Value::Array(children));
    let mut configuration = Map::new();
    if let Some(d) = &alarm.alarm_description {
        configuration.insert("description".into(), json!(d));
    }
    configuration.insert("alarmRule".into(), json!(alarm.alarm_rule));
    if let Some(s) = &alarm.actions_suppressor {
        configuration.insert("actionsSuppressor".into(), json!(s));
        configuration.insert(
            "actionsSuppressorWaitPeriod".into(),
            json!(alarm.actions_suppressor_wait_period),
        );
        configuration.insert(
            "actionsSuppressorExtensionPeriod".into(),
            json!(alarm.actions_suppressor_extension_period),
        );
    }
    Some(AlarmNotification {
        account_id: account_id.to_string(),
        region: region.to_string(),
        alarm_name: alarm.alarm_name.clone(),
        alarm_type: "CompositeAlarm",
        actions,
        state_change_time: at,
        subject: subject(alarm.state_value, &alarm.alarm_name, &region_name(region)),
        sns_message: Value::Object(msg).to_string(),
        lambda_payload: lambda_payload(
            (account_id, region, &alarm.alarm_arn, &alarm.alarm_name),
            at,
            lambda_state(
                alarm.state_value,
                &alarm.state_reason,
                alarm.state_reason_data.as_deref(),
                at,
            ),
            lambda_state(
                prev.value,
                &prev.reason,
                prev.reason_data.as_deref(),
                prev.timestamp,
            ),
            Value::Object(configuration),
        ),
        instance_id: None,
        epoch: 0,
        alarm_arn: alarm.alarm_arn.clone(),
    })
}

/// The notification a log alarm owes for its change from `prev`.
pub(crate) fn log_alarm_notification(
    account_id: &str,
    region: &str,
    alarm: &LogAlarm,
    prev: &PreviousState,
) -> Option<AlarmNotification> {
    if !alarm.actions_enabled || prev.value == alarm.state_value {
        return None;
    }
    let actions = actions_for(
        alarm.state_value,
        &alarm.ok_actions,
        &alarm.alarm_actions,
        &alarm.insufficient_data_actions,
    );
    if actions.is_empty() {
        return None;
    }
    let at = alarm.state_updated_timestamp;
    let mut msg = base_message(
        account_id,
        region,
        &alarm.alarm_name,
        &alarm.alarm_arn,
        alarm.alarm_description.as_deref(),
        alarm.alarm_configuration_updated_timestamp,
        (alarm.state_value, &alarm.state_reason, at),
        prev.value,
        [
            &alarm.ok_actions,
            &alarm.alarm_actions,
            &alarm.insufficient_data_actions,
        ],
    );
    msg.insert(
        "Trigger".into(),
        json!({
            "QueryString": alarm.query_string,
            "AggregationExpression": alarm.aggregation_expression,
            "LogGroupIdentifiers": alarm.log_group_identifiers,
            "QueryResultsToEvaluate": alarm.query_results_to_evaluate,
            "QueryResultsToAlarm": alarm.query_results_to_alarm,
            "ComparisonOperator": alarm.comparison_operator,
            "Threshold": alarm.threshold,
            "TreatMissingData": alarm.treat_missing_data.as_deref().unwrap_or("missing"),
        }),
    );
    let mut configuration = Map::new();
    if let Some(d) = &alarm.alarm_description {
        configuration.insert("description".into(), json!(d));
    }
    configuration.insert("queryString".into(), json!(alarm.query_string));
    Some(AlarmNotification {
        account_id: account_id.to_string(),
        region: region.to_string(),
        alarm_name: alarm.alarm_name.clone(),
        alarm_type: "LogAlarm",
        actions,
        state_change_time: at,
        subject: subject(alarm.state_value, &alarm.alarm_name, &region_name(region)),
        sns_message: Value::Object(msg).to_string(),
        lambda_payload: lambda_payload(
            (account_id, region, &alarm.alarm_arn, &alarm.alarm_name),
            at,
            lambda_state(
                alarm.state_value,
                &alarm.state_reason,
                alarm.state_reason_data.as_deref(),
                at,
            ),
            lambda_state(
                prev.value,
                &prev.reason,
                prev.reason_data.as_deref(),
                prev.timestamp,
            ),
            Value::Object(configuration),
        ),
        instance_id: None,
        epoch: 0,
        alarm_arn: alarm.alarm_arn.clone(),
    })
}

/// Run every action of `n` and return the `Action` history items recording
/// the outcomes.
/// Where alarm actions go.
#[derive(Clone, Copy, Default)]
pub(crate) struct ActionTargets<'a> {
    pub delivery: Option<&'a DeliveryBus>,
    pub ec2: Option<&'a dyn Ec2AlarmActionHook>,
    /// Checks targets' resource policies; without it they aren't checked.
    pub authorizer: Option<&'a dyn AlarmActionAuthorizer>,
}

/// Whether the resource policy of the action target lets the alarm act on
/// it. A Lambda function must grant `lambda:InvokeFunction` to
/// [`LAMBDA_ALARMS_PRINCIPAL`] (same account too); a topic in another account
/// must let CloudWatch (or the alarm's account) publish to it.
fn authorize(
    n: &AlarmNotification,
    arn: &str,
    target: ActionTarget,
    authorizer: Option<&dyn AlarmActionAuthorizer>,
) -> Result<(), String> {
    let Some(auth) = authorizer else {
        return Ok(());
    };
    let grant = |service, action, principal| ActionGrant {
        service,
        action,
        resource_arn: arn,
        principal,
        source_arn: &n.alarm_arn,
        source_account: &n.account_id,
    };
    match target {
        ActionTarget::Lambda => {
            if auth.allows(&grant(
                "lambda",
                "InvokeFunction",
                GrantPrincipal::Service(LAMBDA_ALARMS_PRINCIPAL),
            )) {
                Ok(())
            } else {
                Err(format!(
                    "CloudWatch Alarms is not authorized to invoke {arn}: the function's resource \
                     policy must grant lambda:InvokeFunction to {LAMBDA_ALARMS_PRINCIPAL}"
                ))
            }
        }
        ActionTarget::Sns => {
            let topic_account = fakecloud_aws::arn::account_of(arn).unwrap_or_default();
            if topic_account == n.account_id
                || auth.allows(&grant(
                    "sns",
                    "Publish",
                    GrantPrincipal::Service("cloudwatch.amazonaws.com"),
                ))
                || auth.allows(&grant(
                    "sns",
                    "Publish",
                    GrantPrincipal::Account(&n.account_id),
                ))
            {
                Ok(())
            } else {
                Err(format!(
                    "CloudWatch Alarms is not authorized to publish to {arn}: the topic policy \
                     doesn't allow it"
                ))
            }
        }
        _ => Ok(()),
    }
}

pub(crate) async fn dispatch(
    n: &AlarmNotification,
    targets: ActionTargets<'_>,
) -> Vec<AlarmHistoryItem> {
    let ActionTargets { delivery, ec2, .. } = targets;
    let mut items = Vec::new();
    for arn in &n.actions {
        let mut data = Map::new();
        let target = classify(arn);
        let allowed = authorize(n, arn, target, targets.authorizer);
        let outcome: Result<(), String> = match (target, allowed) {
            (_, Err(e)) => Err(e),
            (ActionTarget::Sns, _) => match delivery {
                Some(bus) => bus
                    .try_publish_to_sns(arn, &n.sns_message, Some(&n.subject))
                    .map(|_| {
                        data.insert("notificationResource".into(), json!(arn));
                        data.insert("publishedMessage".into(), json!(n.sns_message));
                    }),
                None => Err("SNS delivery is not available".to_string()),
            },
            (ActionTarget::Lambda, _) => {
                match delivery.and_then(|bus| bus.invoke_lambda_async(arn, &n.lambda_payload)) {
                    Some(r) => r,
                    None => Err("Lambda delivery is not available".to_string()),
                }
            }
            (ActionTarget::Ec2(action), _) => match (&n.instance_id, ec2) {
                (None, _) => Err("The alarm has no InstanceId dimension".to_string()),
                (Some(_), None) => Err("EC2 is not available".to_string()),
                (Some(id), Some(hook)) => hook.perform(&n.account_id, &n.region, id, action).await,
            },
            // The scaling watcher acts on the alarm state itself.
            (ActionTarget::AutoScaling, _) => continue,
            (ActionTarget::NotPerformed, _) => {
                Err(format!("fakecloud does not perform the alarm action {arn}"))
            }
            (ActionTarget::Unsupported, _) => Err(format!("Unsupported alarm action {arn}")),
        };
        if let Err(e) = &outcome {
            tracing::warn!(alarm = %n.alarm_name, action = %arn, error = %e, "alarm action failed");
        }
        let (state, summary) = match &outcome {
            Ok(()) => ("Succeeded", format!("Successfully executed action {arn}")),
            Err(_) => ("Failed", format!("Failed to execute action {arn}")),
        };
        let mut history = Map::new();
        history.insert("actionState".into(), json!(state));
        history.insert(
            "stateUpdateTimestamp".into(),
            json!(n.state_change_time.timestamp_millis()),
        );
        history.extend(data);
        if let Err(e) = outcome {
            history.insert("error".into(), json!(e));
        }
        items.push(AlarmHistoryItem {
            alarm_name: n.alarm_name.clone(),
            alarm_type: n.alarm_type.to_string(),
            timestamp: Utc::now(),
            history_item_type: "Action".to_string(),
            history_summary: summary,
            history_data: Value::Object(history).to_string(),
        });
    }
    items
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_action_arns() {
        assert_eq!(
            classify("arn:aws:sns:us-east-1:123456789012:ops"),
            ActionTarget::Sns
        );
        assert_eq!(
            classify("arn:aws:lambda:us-east-1:123456789012:function:fn:prod"),
            ActionTarget::Lambda
        );
        assert_eq!(
            classify("arn:aws:automate:us-east-1:ec2:stop"),
            ActionTarget::Ec2(Ec2InstanceAction::Stop)
        );
        assert_eq!(
            classify("arn:aws:automate:us-east-1:ec2:recover"),
            ActionTarget::Ec2(Ec2InstanceAction::Recover)
        );
        assert_eq!(
            classify("arn:aws:swf:us-east-1:123456789012:action/actions/AWS_EC2.InstanceId.Terminate/1.0"),
            ActionTarget::Ec2(Ec2InstanceAction::Terminate)
        );
        assert_eq!(
            classify("arn:aws-cn:swf:cn-north-1:123456789012:action/actions/AWS_EC2.InstanceId.Reboot/1.0"),
            ActionTarget::Ec2(Ec2InstanceAction::Reboot)
        );
        assert_eq!(
            classify("arn:aws:autoscaling:us-east-1:123456789012:scalingPolicy:x:resource/dynamodb/table/t:policyName/p"),
            ActionTarget::AutoScaling
        );
        assert_eq!(
            classify("arn:aws:autoscaling:us-east-1:123456789012:scalingPolicy:x:autoScalingGroupName/g:policyName/p"),
            ActionTarget::NotPerformed
        );
        assert_eq!(
            classify("arn:aws:ssm:us-east-1:123456789012:opsitem:3#CATEGORY=Security"),
            ActionTarget::NotPerformed
        );
        assert_eq!(
            classify("arn:aws:automate:us-east-1:ec2:explode"),
            ActionTarget::Unsupported
        );
        assert_eq!(classify("not-an-arn"), ActionTarget::Unsupported);
    }

    #[test]
    fn subject_uses_region_long_name_and_is_capped() {
        assert_eq!(
            subject(AlarmState::Alarm, "cpu", &region_name("us-east-1")),
            "ALARM: \"cpu\" in US East (N. Virginia)"
        );
        let long = "x".repeat(200);
        assert_eq!(subject(AlarmState::Ok, &long, "r").chars().count(), 100);
    }
}
