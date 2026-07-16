//! Target delivery + DLQ routing for fired schedules.
//!
//! Scheduler's firing contract: every enabled schedule whose expression
//! matches the current tick has its `Target.Input` body delivered to
//! `Target.Arn`. If delivery fails (target queue missing, etc.) and
//! `Target.DeadLetterConfig.Arn` is set, we route the original input +
//! failure metadata headers to the DLQ so tests can observe the
//! failure. No retry policy is modeled in Batch 2 — every fire is a
//! single attempt.

use std::collections::HashMap;
use std::sync::Arc;

use fakecloud_core::delivery::{DeliveryBus, SqsDeliveryError, SqsMessageAttribute};

use crate::state::Schedule;

/// Attempt to deliver a fired schedule's target. Returns `Ok(())` when
/// the target accepted the message (or the target type is a fire-and-
/// forget SNS/Lambda/SFN that we don't currently validate). Returns
/// `Err` only for SQS-shaped targets where the queue doesn't exist —
/// that's the specific signal Scheduler's DLQ routing needs.
pub fn deliver_target(bus: &Arc<DeliveryBus>, schedule: &Schedule) -> Result<(), SqsDeliveryError> {
    let arn = &schedule.target.arn;
    let body = schedule.target.input.as_deref().unwrap_or("{}");

    if arn.contains(":sqs:") {
        let attrs = HashMap::new();
        let group_id = schedule
            .target
            .sqs_parameters
            .as_ref()
            .and_then(|s| s.message_group_id.as_deref());
        // FIFO queues without content-based dedup reject messages
        // lacking a dedup ID; AWS Scheduler synthesizes one per fire
        // so the message survives regardless of the queue's dedup
        // policy. Use the schedule ARN + fire timestamp so the same
        // schedule can fire repeatedly without being deduplicated.
        let dedup_id = fifo_dedup_id(arn, &schedule.arn);
        return bus.try_send_to_sqs_with_attrs(arn, body, &attrs, group_id, dedup_id.as_deref());
    }

    if arn.contains(":sns:") {
        bus.publish_to_sns(arn, body, None);
        return Ok(());
    }

    if arn.contains(":lambda:") {
        // Fire-and-forget async invocation. A proper implementation
        // would await the future, but DeliveryBus::invoke_lambda is
        // async and we're called from the synchronous tick loop.
        let bus = bus.clone();
        let arn = arn.clone();
        let body = body.to_string();
        tokio::spawn(async move {
            let _ = bus.invoke_lambda(&arn, &body).await;
        });
        return Ok(());
    }

    if arn.contains(":states:") {
        bus.start_stepfunctions_execution(arn, body);
        return Ok(());
    }

    if arn.contains(":events:") {
        let bus_name = event_bus_name_from_arn(arn);
        let target_account = account_id_from_arn(arn);
        // EventBridgeParameters.{Source, DetailType} are REQUIRED by AWS for an
        // EventBridge bus target and populate the emitted event's source /
        // detail-type; falling back to the scheduler defaults only when absent.
        let eb_params = schedule.target.eventbridge_parameters.as_ref();
        let source = eb_params
            .and_then(|p| p.get("Source"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("aws.scheduler");
        let detail_type = eb_params
            .and_then(|p| p.get("DetailType"))
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("Scheduled Event");
        match target_account {
            Some(account) => bus.put_event_to_eventbridge_for_account(
                source,
                detail_type,
                body,
                &bus_name,
                &account,
            ),
            None => bus.put_event_to_eventbridge(source, detail_type, body, &bus_name),
        }
        return Ok(());
    }

    if arn.contains(":kinesis:") {
        // KinesisParameters.PartitionKey carries the per-record key per
        // AWS contract; fall back to the schedule name so the put never
        // fails on a missing key.
        let partition_key = schedule
            .target
            .kinesis_parameters
            .as_ref()
            .and_then(|p| p.get("PartitionKey").and_then(|v| v.as_str()))
            .unwrap_or(&schedule.name)
            .to_string();
        bus.send_to_kinesis(arn, body, &partition_key);
        return Ok(());
    }

    // Templated SageMaker pipeline target: ARN names the pipeline,
    // SageMakerPipelineParameters carries the PipelineParameterList.
    if arn.contains(":sagemaker:") && arn.contains(":pipeline/") {
        let params = schedule
            .target
            .sagemaker_pipeline_parameters
            .as_ref()
            .and_then(|p| p.get("PipelineParameterList"))
            .cloned()
            .unwrap_or_else(|| serde_json::Value::Array(vec![]));
        bus.start_sagemaker_pipeline(arn, &params);
        return Ok(());
    }

    // Templated ECS target: ARN names the cluster, EcsParameters carries
    // TaskDefinitionArn / LaunchType / TaskCount.
    if arn.contains(":ecs:") && arn.contains(":cluster/") {
        return deliver_ecs_templated(bus, schedule, arn);
    }

    // Universal SDK target (`arn:aws:scheduler:::aws-sdk:<service>:<api>`)
    // is the AWS-defined shape for arbitrary AWS APIs; we cover SES
    // SendEmail / SendEmailV2 and ECS RunTask explicitly here, with
    // remaining services documented as a roadmap item.
    if let Some((service, action)) = parse_aws_sdk_universal_arn(arn) {
        return deliver_aws_sdk_universal(bus, schedule, &service, &action, body);
    }

    // Unsupported target type — log and succeed so we don't push it to
    // DLQ (DLQ is for deliverable-target failures, not unknown targets).
    tracing::warn!(target_arn = %arn, schedule = %schedule.name, "scheduler: unsupported target type, skipping");
    Ok(())
}

/// Deliver to an ECS cluster ARN target. The cluster name is taken from
/// the ARN segment after `cluster/`; the task definition + launch type +
/// count come from `Target.EcsParameters`. Missing TaskDefinitionArn is
/// the failure case — surfaces as `InvalidParameter` so the schedule
/// routes to its DLQ.
fn deliver_ecs_templated(
    bus: &Arc<DeliveryBus>,
    schedule: &Schedule,
    cluster_arn: &str,
) -> Result<(), SqsDeliveryError> {
    let account_id = account_id_from_arn(cluster_arn).unwrap_or_else(|| "000000000000".to_string());
    let cluster = cluster_arn
        .split(":cluster/")
        .nth(1)
        .unwrap_or("default")
        .to_string();
    let params = match schedule.target.ecs_parameters.as_ref() {
        Some(p) => p,
        None => {
            return Err(SqsDeliveryError::InvalidParameter(
                "ECS target requires EcsParameters".to_string(),
            ));
        }
    };
    let task_definition = match params.get("TaskDefinitionArn").and_then(|v| v.as_str()) {
        Some(td) => td,
        None => {
            return Err(SqsDeliveryError::InvalidParameter(
                "ECS target requires TaskDefinitionArn".to_string(),
            ));
        }
    };
    let launch_type = params.get("LaunchType").and_then(|v| v.as_str());
    let count = params
        .get("TaskCount")
        .and_then(|v| v.as_u64())
        .unwrap_or(1)
        .max(1) as usize;
    bus.run_ecs_task(&account_id, &cluster, task_definition, launch_type, count)
        .map_err(SqsDeliveryError::InvalidParameter)
}

/// Parse `arn:aws:scheduler:::aws-sdk:<service>:<action>` (or the
/// equivalent with a region/account in slot 3/4) into `(service, action)`.
fn parse_aws_sdk_universal_arn(arn: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = arn.split(':').collect();
    // Expected layout for the canonical universal target:
    //   ["arn","aws","scheduler","","","aws-sdk","<service>","<action>"]
    // AWS also accepts a region+account variant; either way the
    // resource segment is `aws-sdk:<service>:<action>` and lives in the
    // tail — find it by name.
    let aws_sdk_idx = parts.iter().position(|p| *p == "aws-sdk")?;
    let service = parts.get(aws_sdk_idx + 1)?;
    let action = parts.get(aws_sdk_idx + 2)?;
    if service.is_empty() || action.is_empty() {
        return None;
    }
    Some((service.to_lowercase(), action.to_string()))
}

/// Dispatch a recognized aws-sdk universal target. Unknown service/api
/// combinations log a warning and succeed (matching the previous
/// behavior for unsupported targets — DLQ is for deliverable-target
/// failures, not for unrecognized API combinations).
fn deliver_aws_sdk_universal(
    bus: &Arc<DeliveryBus>,
    schedule: &Schedule,
    service: &str,
    action: &str,
    body: &str,
) -> Result<(), SqsDeliveryError> {
    let action_lower = action.to_lowercase();
    match (service, action_lower.as_str()) {
        ("sesv2", "sendemail") | ("ses", "sendemail") => {
            deliver_ses_send_email(bus, schedule, body, service == "sesv2")
        }
        ("ecs", "runtask") => deliver_ecs_universal(bus, schedule, body),
        _ => {
            tracing::warn!(
                schedule = %schedule.name,
                service = %service,
                action = %action,
                "scheduler: aws-sdk universal target not implemented for this API"
            );
            Ok(())
        }
    }
}

fn deliver_ses_send_email(
    bus: &Arc<DeliveryBus>,
    schedule: &Schedule,
    body: &str,
    v2: bool,
) -> Result<(), SqsDeliveryError> {
    let req: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| SqsDeliveryError::InvalidParameter(format!("invalid JSON Input: {e}")))?;
    let account_id =
        account_id_from_arn(&schedule.arn).unwrap_or_else(|| "000000000000".to_string());

    // SESv2 SendEmail uses FromEmailAddress/Destination/Content; v1 uses
    // Source/Destination/Message. Parse both shapes.
    let from = if v2 {
        req.get("FromEmailAddress").and_then(|v| v.as_str())
    } else {
        req.get("Source").and_then(|v| v.as_str())
    }
    .ok_or_else(|| SqsDeliveryError::InvalidParameter("missing source/from".to_string()))?
    .to_string();

    let dest = req
        .get("Destination")
        .cloned()
        .unwrap_or(serde_json::json!({}));
    let to = string_array(&dest, "ToAddresses");
    let cc = string_array(&dest, "CcAddresses");
    let bcc = string_array(&dest, "BccAddresses");
    if to.is_empty() && cc.is_empty() && bcc.is_empty() {
        return Err(SqsDeliveryError::InvalidParameter(
            "SES SendEmail requires at least one recipient".to_string(),
        ));
    }

    // SESv2 message lives at Content.Simple.{Subject,Body}; v1 at Message.
    let (subject, text, html) = if v2 {
        let simple = req
            .get("Content")
            .and_then(|c| c.get("Simple"))
            .cloned()
            .unwrap_or(serde_json::json!({}));
        (
            simple
                .get("Subject")
                .and_then(|s| s.get("Data"))
                .and_then(|d| d.as_str())
                .map(String::from),
            simple
                .get("Body")
                .and_then(|b| b.get("Text"))
                .and_then(|t| t.get("Data"))
                .and_then(|d| d.as_str())
                .map(String::from),
            simple
                .get("Body")
                .and_then(|b| b.get("Html"))
                .and_then(|h| h.get("Data"))
                .and_then(|d| d.as_str())
                .map(String::from),
        )
    } else {
        let msg = req.get("Message").cloned().unwrap_or(serde_json::json!({}));
        (
            msg.get("Subject")
                .and_then(|s| s.get("Data"))
                .and_then(|d| d.as_str())
                .map(String::from),
            msg.get("Body")
                .and_then(|b| b.get("Text"))
                .and_then(|t| t.get("Data"))
                .and_then(|d| d.as_str())
                .map(String::from),
            msg.get("Body")
                .and_then(|b| b.get("Html"))
                .and_then(|h| h.get("Data"))
                .and_then(|d| d.as_str())
                .map(String::from),
        )
    };

    bus.send_ses_email(
        &account_id,
        &from,
        to,
        cc,
        bcc,
        subject.as_deref(),
        text.as_deref(),
        html.as_deref(),
    )
    .map_err(SqsDeliveryError::InvalidParameter)
}

fn deliver_ecs_universal(
    bus: &Arc<DeliveryBus>,
    schedule: &Schedule,
    body: &str,
) -> Result<(), SqsDeliveryError> {
    let req: serde_json::Value = serde_json::from_str(body)
        .map_err(|e| SqsDeliveryError::InvalidParameter(format!("invalid JSON Input: {e}")))?;
    let account_id =
        account_id_from_arn(&schedule.arn).unwrap_or_else(|| "000000000000".to_string());
    let cluster = req
        .get("Cluster")
        .and_then(|v| v.as_str())
        .unwrap_or("default")
        .to_string();
    let task_definition = req
        .get("TaskDefinition")
        .and_then(|v| v.as_str())
        .ok_or_else(|| {
            SqsDeliveryError::InvalidParameter("RunTask requires TaskDefinition".to_string())
        })?
        .to_string();
    let launch_type = req.get("LaunchType").and_then(|v| v.as_str());
    let count = req
        .get("Count")
        .and_then(|v| v.as_u64())
        .unwrap_or(1)
        .max(1) as usize;
    bus.run_ecs_task(&account_id, &cluster, &task_definition, launch_type, count)
        .map_err(SqsDeliveryError::InvalidParameter)
}

fn string_array(value: &serde_json::Value, key: &str) -> Vec<String> {
    value
        .get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

/// Route a failed delivery to the schedule's DLQ, if one is
/// configured. Metadata headers mirror AWS's format so tests can
/// assert on `X-Amz-Scheduler-Attempt` etc.
pub fn route_to_dlq(
    bus: &Arc<DeliveryBus>,
    schedule: &Schedule,
    error_code: &str,
    error_message: &str,
) {
    let dlq_arn = match schedule
        .target
        .dead_letter_config
        .as_ref()
        .and_then(|d| d.arn.as_ref())
    {
        Some(arn) => arn.clone(),
        None => return,
    };

    let mut attrs: HashMap<String, SqsMessageAttribute> = HashMap::new();
    attrs.insert("X-Amz-Scheduler-Attempt".to_string(), string_attr("1"));
    attrs.insert(
        "X-Amz-Scheduler-Schedule-Arn".to_string(),
        string_attr(&schedule.arn),
    );
    attrs.insert(
        "X-Amz-Scheduler-Target-Arn".to_string(),
        string_attr(&schedule.target.arn),
    );
    attrs.insert(
        "X-Amz-Scheduler-Error-Code".to_string(),
        string_attr(error_code),
    );
    attrs.insert(
        "X-Amz-Scheduler-Error-Message".to_string(),
        string_attr(error_message),
    );
    attrs.insert(
        "X-Amz-Scheduler-Group".to_string(),
        string_attr(&schedule.group_name),
    );

    let body = schedule.target.input.as_deref().unwrap_or("{}");
    let dedup_id = fifo_dedup_id(&dlq_arn, &schedule.arn);
    // FIFO DLQs need a MessageGroupId; use the schedule name so each
    // schedule's DLQ messages fall in its own ordered stream.
    let group_id = dedup_id.as_ref().map(|_| schedule.name.clone());
    if let Err(err) = bus.try_send_to_sqs_with_attrs(
        &dlq_arn,
        body,
        &attrs,
        group_id.as_deref(),
        dedup_id.as_deref(),
    ) {
        tracing::error!(
            schedule = %schedule.name,
            dlq = %dlq_arn,
            %err,
            "scheduler: DLQ delivery failed — message dropped"
        );
    } else {
        tracing::info!(
            schedule = %schedule.name,
            dlq = %dlq_arn,
            %error_code,
            "scheduler: delivery failed, routed to DLQ"
        );
    }
}

/// Build a dedup ID for FIFO queues. Returns `Some(...)` only when
/// the target queue name ends in `.fifo`; standard queues ignore
/// dedup IDs so we skip the allocation. SQS caps MessageDeduplicationId
/// at 128 chars, so we use a bare UUID (36 chars) rather than
/// concatenating the schedule ARN — schedule ARNs can themselves reach
/// ~150 chars and would push combined IDs over the limit.
fn fifo_dedup_id(queue_arn: &str, _schedule_arn: &str) -> Option<String> {
    if !queue_arn.ends_with(".fifo") {
        return None;
    }
    Some(uuid::Uuid::new_v4().to_string())
}

/// Extract the account-id segment from any AWS ARN. Returns `None` when
/// the ARN is malformed or omits the account (some service ARNs do).
fn account_id_from_arn(arn: &str) -> Option<String> {
    let account = arn.split(':').nth(4)?;
    if account.is_empty() {
        None
    } else {
        Some(account.to_string())
    }
}

/// Extract the EventBridge bus name from a `:events:` target ARN.
/// Two shapes are accepted:
/// - `arn:aws:events:<region>:<account>:event-bus/<name>`
/// - `arn:aws:events:<region>:<account>:bus/<name>` (legacy / alias)
///
/// Returns `"default"` only when the ARN names the default bus or is
/// malformed; never as a silent fallback for a custom bus name.
fn event_bus_name_from_arn(arn: &str) -> String {
    let resource = match arn.split(':').nth(5) {
        Some(r) => r,
        None => return "default".to_string(),
    };
    let name = resource
        .strip_prefix("event-bus/")
        .or_else(|| resource.strip_prefix("bus/"))
        .unwrap_or("default");
    if name.is_empty() {
        "default".to_string()
    } else {
        name.to_string()
    }
}

fn string_attr(v: &str) -> SqsMessageAttribute {
    SqsMessageAttribute {
        data_type: "String".to_string(),
        string_value: Some(v.to_string()),
        binary_value: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{DeadLetterConfig, FlexibleTimeWindow, Schedule, Target};
    use chrono::Utc;
    use std::collections::HashMap;
    use std::sync::Mutex;

    type RecordedCall = (String, String, HashMap<String, SqsMessageAttribute>);

    struct Recorder {
        calls: Mutex<Vec<RecordedCall>>,
        fail_arn: Option<String>,
    }

    impl fakecloud_core::delivery::SqsDelivery for Recorder {
        fn deliver_to_queue(&self, _arn: &str, _body: &str, _attrs: &HashMap<String, String>) {}

        fn try_deliver_to_queue_with_attrs(
            &self,
            queue_arn: &str,
            message_body: &str,
            message_attributes: &HashMap<String, SqsMessageAttribute>,
            _group: Option<&str>,
            _dedup: Option<&str>,
        ) -> Result<(), SqsDeliveryError> {
            if Some(queue_arn.to_string()) == self.fail_arn {
                return Err(SqsDeliveryError::QueueNotFound(queue_arn.to_string()));
            }
            self.calls.lock().unwrap().push((
                queue_arn.to_string(),
                message_body.to_string(),
                message_attributes.clone(),
            ));
            Ok(())
        }
    }

    fn make_schedule(target_arn: &str, dlq: Option<&str>, input: Option<&str>) -> Schedule {
        Schedule {
            arn: "arn:aws:scheduler:us-east-1:1:schedule/default/t".to_string(),
            name: "t".to_string(),
            group_name: "default".to_string(),
            schedule_expression: "rate(1 minute)".to_string(),
            schedule_expression_timezone: None,
            start_date: None,
            end_date: None,
            description: None,
            state: "ENABLED".to_string(),
            kms_key_arn: None,
            action_after_completion: "NONE".to_string(),
            flexible_time_window: FlexibleTimeWindow::default(),
            target: Target {
                arn: target_arn.to_string(),
                role_arn: "arn:aws:iam::1:role/s".to_string(),
                input: input.map(String::from),
                dead_letter_config: dlq.map(|arn| DeadLetterConfig {
                    arn: Some(arn.to_string()),
                }),
                retry_policy: None,
                sqs_parameters: None,
                ecs_parameters: None,
                eventbridge_parameters: None,
                kinesis_parameters: None,
                sagemaker_pipeline_parameters: None,
            },
            creation_date: Utc::now(),
            last_modification_date: Utc::now(),
            last_fired: None,
        }
    }

    #[test]
    fn deliver_target_sqs_success() {
        let recorder = Arc::new(Recorder {
            calls: Mutex::new(Vec::new()),
            fail_arn: None,
        });
        let bus = Arc::new(DeliveryBus::new().with_sqs(recorder.clone()));
        let sched = make_schedule("arn:aws:sqs:us-east-1:1:dest", None, Some(r#"{"v":1}"#));
        let result = deliver_target(&bus, &sched);
        assert!(result.is_ok());
        let calls = recorder.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1, r#"{"v":1}"#);
    }

    #[test]
    fn deliver_target_kinesis_uses_kinesis_parameters_partition_key() {
        struct KinesisRec(Mutex<Vec<(String, String, String)>>);
        impl fakecloud_core::delivery::KinesisDelivery for KinesisRec {
            fn put_record(&self, stream_arn: &str, data: &str, partition_key: &str) {
                self.0.lock().unwrap().push((
                    stream_arn.to_string(),
                    data.to_string(),
                    partition_key.to_string(),
                ));
            }
        }
        let kinesis = Arc::new(KinesisRec(Mutex::new(Vec::new())));
        let bus = Arc::new(DeliveryBus::new().with_kinesis(kinesis.clone()));
        let mut sched = make_schedule(
            "arn:aws:kinesis:us-east-1:1:stream/orders",
            None,
            Some(r#"{"order":42}"#),
        );
        sched.target.kinesis_parameters = Some(serde_json::json!({"PartitionKey": "tenant-7"}));
        let result = deliver_target(&bus, &sched);
        assert!(result.is_ok());
        let calls = kinesis.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "arn:aws:kinesis:us-east-1:1:stream/orders");
        assert_eq!(calls[0].1, r#"{"order":42}"#);
        assert_eq!(calls[0].2, "tenant-7");
    }

    #[test]
    fn deliver_target_eventbridge_uses_source_and_detail_type_params() {
        struct EbRec(Mutex<Vec<(String, String, String, String)>>);
        impl fakecloud_core::delivery::EventBridgeDelivery for EbRec {
            fn put_event(&self, source: &str, detail_type: &str, detail: &str, bus_name: &str) {
                self.0.lock().unwrap().push((
                    source.to_string(),
                    detail_type.to_string(),
                    detail.to_string(),
                    bus_name.to_string(),
                ));
            }
        }
        let eb = Arc::new(EbRec(Mutex::new(Vec::new())));
        let bus = Arc::new(DeliveryBus::new().with_eventbridge(eb.clone()));
        let mut sched = make_schedule(
            "arn:aws:events:us-east-1:1:event-bus/app-bus",
            None,
            Some(r#"{"k":"v"}"#),
        );
        sched.target.eventbridge_parameters =
            Some(serde_json::json!({"Source": "my.app", "DetailType": "OrderPlaced"}));
        let result = deliver_target(&bus, &sched);
        assert!(result.is_ok());
        let calls = eb.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "my.app", "Source must come from params");
        assert_eq!(
            calls[0].1, "OrderPlaced",
            "DetailType must come from params"
        );
        assert_eq!(calls[0].3, "app-bus");
    }

    #[test]
    fn deliver_target_sagemaker_pipeline_starts_execution() {
        struct SmRec(Mutex<Vec<(String, serde_json::Value)>>);
        impl fakecloud_core::delivery::SageMakerPipelineDelivery for SmRec {
            fn start_pipeline_execution(&self, pipeline_arn: &str, parameters: &serde_json::Value) {
                self.0
                    .lock()
                    .unwrap()
                    .push((pipeline_arn.to_string(), parameters.clone()));
            }
        }
        let sm = Arc::new(SmRec(Mutex::new(Vec::new())));
        let bus = Arc::new(DeliveryBus::new().with_sagemaker_pipeline(sm.clone()));
        let mut sched = make_schedule(
            "arn:aws:sagemaker:us-east-1:1:pipeline/my-pipeline",
            None,
            Some("{}"),
        );
        sched.target.sagemaker_pipeline_parameters = Some(serde_json::json!({
            "PipelineParameterList": [{"Name": "p1", "Value": "v1"}]
        }));
        let result = deliver_target(&bus, &sched);
        assert!(result.is_ok());
        let calls = sm.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(
            calls[0].0,
            "arn:aws:sagemaker:us-east-1:1:pipeline/my-pipeline"
        );
        assert_eq!(calls[0].1[0]["Name"], "p1");
    }

    #[test]
    fn deliver_target_kinesis_falls_back_to_schedule_name_for_partition_key() {
        struct KinesisRec(Mutex<Vec<(String, String, String)>>);
        impl fakecloud_core::delivery::KinesisDelivery for KinesisRec {
            fn put_record(&self, stream_arn: &str, data: &str, partition_key: &str) {
                self.0.lock().unwrap().push((
                    stream_arn.to_string(),
                    data.to_string(),
                    partition_key.to_string(),
                ));
            }
        }
        let kinesis = Arc::new(KinesisRec(Mutex::new(Vec::new())));
        let bus = Arc::new(DeliveryBus::new().with_kinesis(kinesis.clone()));
        let sched = make_schedule(
            "arn:aws:kinesis:us-east-1:1:stream/orders",
            None,
            Some(r#"{"v":1}"#),
        );
        let result = deliver_target(&bus, &sched);
        assert!(result.is_ok());
        let calls = kinesis.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].2, "t");
    }

    #[test]
    fn deliver_target_sqs_missing_queue_returns_err() {
        let recorder = Arc::new(Recorder {
            calls: Mutex::new(Vec::new()),
            fail_arn: Some("arn:aws:sqs:us-east-1:1:missing".to_string()),
        });
        let bus = Arc::new(DeliveryBus::new().with_sqs(recorder.clone()));
        let sched = make_schedule("arn:aws:sqs:us-east-1:1:missing", None, None);
        let result = deliver_target(&bus, &sched);
        assert!(matches!(result, Err(SqsDeliveryError::QueueNotFound(_))));
    }

    #[test]
    fn route_to_dlq_includes_metadata_headers() {
        let recorder = Arc::new(Recorder {
            calls: Mutex::new(Vec::new()),
            fail_arn: None,
        });
        let bus = Arc::new(DeliveryBus::new().with_sqs(recorder.clone()));
        let sched = make_schedule(
            "arn:aws:sqs:us-east-1:1:main",
            Some("arn:aws:sqs:us-east-1:1:dlq"),
            Some(r#"{"original":true}"#),
        );
        route_to_dlq(&bus, &sched, "QueueNotFound", "queue missing");
        let calls = recorder.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        let (arn, body, attrs) = &calls[0];
        assert_eq!(arn, "arn:aws:sqs:us-east-1:1:dlq");
        assert_eq!(body, r#"{"original":true}"#);
        assert_eq!(
            attrs
                .get("X-Amz-Scheduler-Attempt")
                .and_then(|a| a.string_value.as_deref()),
            Some("1")
        );
        assert_eq!(
            attrs
                .get("X-Amz-Scheduler-Error-Code")
                .and_then(|a| a.string_value.as_deref()),
            Some("QueueNotFound")
        );
        assert_eq!(
            attrs
                .get("X-Amz-Scheduler-Schedule-Arn")
                .and_then(|a| a.string_value.as_deref()),
            Some("arn:aws:scheduler:us-east-1:1:schedule/default/t")
        );
    }

    #[test]
    fn route_to_dlq_without_config_is_noop() {
        let recorder = Arc::new(Recorder {
            calls: Mutex::new(Vec::new()),
            fail_arn: None,
        });
        let bus = Arc::new(DeliveryBus::new().with_sqs(recorder.clone()));
        let sched = make_schedule("arn:aws:sqs:us-east-1:1:main", None, None);
        route_to_dlq(&bus, &sched, "X", "y");
        assert!(recorder.calls.lock().unwrap().is_empty());
    }

    #[test]
    fn fifo_dedup_id_generated_for_fifo_queue() {
        let id = fifo_dedup_id("arn:aws:sqs:us-east-1:1:q.fifo", "arn:sched/a");
        assert!(id.is_some());
        let s = id.unwrap();
        // SQS caps MessageDeduplicationId at 128 chars.
        assert!(s.len() <= 128);
        // Two fires must not collide.
        let id2 = fifo_dedup_id("arn:aws:sqs:us-east-1:1:q.fifo", "arn:sched/a").unwrap();
        assert_ne!(s, id2);
    }

    #[test]
    fn fifo_dedup_id_stays_under_128_char_limit_for_long_arns() {
        let long_arn = format!(
            "arn:aws:scheduler:us-east-1:123456789012:schedule/{}/{}",
            "g".repeat(64),
            "s".repeat(64)
        );
        let id = fifo_dedup_id("arn:aws:sqs:us-east-1:1:q.fifo", &long_arn).unwrap();
        assert!(id.len() <= 128, "got {} chars", id.len());
    }

    #[test]
    fn fifo_dedup_id_skipped_for_standard_queue() {
        let id = fifo_dedup_id("arn:aws:sqs:us-east-1:1:standard", "arn:sched/a");
        assert!(id.is_none());
    }

    #[test]
    fn event_bus_name_from_arn_custom() {
        assert_eq!(
            event_bus_name_from_arn("arn:aws:events:us-east-1:1:event-bus/my-bus"),
            "my-bus"
        );
    }

    #[test]
    fn event_bus_name_from_arn_default_fallback() {
        assert_eq!(
            event_bus_name_from_arn("arn:aws:events:us-east-1:1:event-bus/default"),
            "default"
        );
        assert_eq!(event_bus_name_from_arn("arn:malformed"), "default");
    }

    #[test]
    fn deliver_target_sqs_cross_account_routes_by_arn_account() {
        // The SQS target points to account 999988887777 even though the
        // schedule lives in account 1. Account-aware SQS routing is the
        // SqsDelivery impl's responsibility — at this layer we just verify
        // the ARN is forwarded unchanged to the bus, so a real impl can
        // route it to the right account's state.
        let recorder = Arc::new(Recorder {
            calls: Mutex::new(Vec::new()),
            fail_arn: None,
        });
        let bus = Arc::new(DeliveryBus::new().with_sqs(recorder.clone()));
        let sched = make_schedule(
            "arn:aws:sqs:us-east-1:999988887777:cross-q",
            None,
            Some(r#"{"v":1}"#),
        );
        assert!(deliver_target(&bus, &sched).is_ok());
        let calls = recorder.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "arn:aws:sqs:us-east-1:999988887777:cross-q");
    }

    #[test]
    fn account_id_from_arn_extracts_account_segment() {
        assert_eq!(
            account_id_from_arn("arn:aws:events:us-east-1:111122223333:event-bus/main"),
            Some("111122223333".to_string())
        );
        assert_eq!(
            account_id_from_arn("arn:aws:events:us-east-1::event-bus/x"),
            None
        );
        assert_eq!(account_id_from_arn("arn:malformed"), None);
    }

    type EcsRunCall = (String, String, String, Option<String>, usize);
    struct EcsRunRecorder(Mutex<Vec<EcsRunCall>>);
    impl fakecloud_core::delivery::EcsTaskRunner for EcsRunRecorder {
        fn run_task(
            &self,
            account_id: &str,
            cluster: &str,
            task_definition: &str,
            launch_type: Option<&str>,
            count: usize,
        ) -> Result<(), String> {
            self.0.lock().unwrap().push((
                account_id.to_string(),
                cluster.to_string(),
                task_definition.to_string(),
                launch_type.map(String::from),
                count,
            ));
            Ok(())
        }
    }

    type SesCall = (
        String,
        String,
        Vec<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    struct SesRecorder(Mutex<Vec<SesCall>>);
    impl fakecloud_core::delivery::SesSendEmailDispatcher for SesRecorder {
        #[allow(clippy::too_many_arguments)]
        fn send_email(
            &self,
            account_id: &str,
            from: &str,
            to: Vec<String>,
            _cc: Vec<String>,
            _bcc: Vec<String>,
            subject: Option<&str>,
            text_body: Option<&str>,
            html_body: Option<&str>,
        ) -> Result<(), String> {
            self.0.lock().unwrap().push((
                account_id.to_string(),
                from.to_string(),
                to,
                subject.map(String::from),
                text_body.map(String::from),
                html_body.map(String::from),
            ));
            Ok(())
        }
    }

    #[test]
    fn deliver_target_ecs_templated_uses_cluster_arn_and_ecs_parameters() {
        let runner = Arc::new(EcsRunRecorder(Mutex::new(Vec::new())));
        let bus = Arc::new(DeliveryBus::new().with_ecs_task_runner(runner.clone()));
        let mut sched = make_schedule(
            "arn:aws:ecs:us-east-1:111122223333:cluster/prod",
            None,
            Some(r#"{}"#),
        );
        sched.target.ecs_parameters = Some(serde_json::json!({
            "TaskDefinitionArn": "arn:aws:ecs:us-east-1:111122223333:task-definition/web:7",
            "LaunchType": "FARGATE",
            "TaskCount": 2u64,
        }));
        let result = deliver_target(&bus, &sched);
        assert!(result.is_ok());
        let calls = runner.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "111122223333");
        assert_eq!(calls[0].1, "prod");
        assert_eq!(
            calls[0].2,
            "arn:aws:ecs:us-east-1:111122223333:task-definition/web:7"
        );
        assert_eq!(calls[0].3.as_deref(), Some("FARGATE"));
        assert_eq!(calls[0].4, 2);
    }

    #[test]
    fn deliver_target_ecs_templated_without_task_definition_returns_invalid_parameter() {
        let runner = Arc::new(EcsRunRecorder(Mutex::new(Vec::new())));
        let bus = Arc::new(DeliveryBus::new().with_ecs_task_runner(runner));
        let mut sched = make_schedule("arn:aws:ecs:us-east-1:1:cluster/prod", None, Some(r#"{}"#));
        sched.target.ecs_parameters = Some(serde_json::json!({"LaunchType": "FARGATE"}));
        let result = deliver_target(&bus, &sched);
        assert!(matches!(result, Err(SqsDeliveryError::InvalidParameter(_))));
    }

    #[test]
    fn aws_sdk_universal_target_dispatches_ses_v2_send_email() {
        let ses = Arc::new(SesRecorder(Mutex::new(Vec::new())));
        let bus = Arc::new(DeliveryBus::new().with_ses_dispatcher(ses.clone()));
        let body = serde_json::json!({
            "FromEmailAddress": "no-reply@example.com",
            "Destination": {"ToAddresses": ["a@example.com", "b@example.com"]},
            "Content": {"Simple": {
                "Subject": {"Data": "hello"},
                "Body": {"Text": {"Data": "world"}, "Html": {"Data": "<b>hi</b>"}}
            }}
        });
        let sched = make_schedule(
            "arn:aws:scheduler:::aws-sdk:sesv2:sendEmail",
            None,
            Some(&body.to_string()),
        );
        let result = deliver_target(&bus, &sched);
        assert!(result.is_ok(), "got {result:?}");
        let calls = ses.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1, "no-reply@example.com");
        assert_eq!(
            calls[0].2,
            vec!["a@example.com".to_string(), "b@example.com".to_string()]
        );
        assert_eq!(calls[0].3.as_deref(), Some("hello"));
        assert_eq!(calls[0].4.as_deref(), Some("world"));
        assert_eq!(calls[0].5.as_deref(), Some("<b>hi</b>"));
    }

    #[test]
    fn aws_sdk_universal_target_dispatches_ses_v1_send_email() {
        let ses = Arc::new(SesRecorder(Mutex::new(Vec::new())));
        let bus = Arc::new(DeliveryBus::new().with_ses_dispatcher(ses.clone()));
        let body = serde_json::json!({
            "Source": "from@example.com",
            "Destination": {"ToAddresses": ["to@example.com"]},
            "Message": {"Subject": {"Data": "s"}, "Body": {"Text": {"Data": "t"}}}
        });
        let sched = make_schedule(
            "arn:aws:scheduler:::aws-sdk:ses:sendEmail",
            None,
            Some(&body.to_string()),
        );
        assert!(deliver_target(&bus, &sched).is_ok());
        let calls = ses.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1, "from@example.com");
        assert_eq!(calls[0].3.as_deref(), Some("s"));
        assert_eq!(calls[0].4.as_deref(), Some("t"));
    }

    #[test]
    fn aws_sdk_universal_target_dispatches_ecs_run_task() {
        let runner = Arc::new(EcsRunRecorder(Mutex::new(Vec::new())));
        let bus = Arc::new(DeliveryBus::new().with_ecs_task_runner(runner.clone()));
        let body = serde_json::json!({
            "Cluster": "prod",
            "TaskDefinition": "web:9",
            "LaunchType": "EC2",
            "Count": 3u64,
        });
        let sched = make_schedule(
            "arn:aws:scheduler:::aws-sdk:ecs:runTask",
            None,
            Some(&body.to_string()),
        );
        assert!(deliver_target(&bus, &sched).is_ok());
        let calls = runner.0.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].1, "prod");
        assert_eq!(calls[0].2, "web:9");
        assert_eq!(calls[0].3.as_deref(), Some("EC2"));
        assert_eq!(calls[0].4, 3);
    }

    #[test]
    fn aws_sdk_universal_target_unknown_api_is_ok_no_dlq() {
        let bus = Arc::new(DeliveryBus::new());
        let sched = make_schedule("arn:aws:scheduler:::aws-sdk:s3:putObject", None, Some("{}"));
        // Unknown API → log + succeed (no DLQ; DLQ is for deliverable
        // failures, not unimplemented surfaces).
        assert!(deliver_target(&bus, &sched).is_ok());
    }

    #[test]
    fn parse_aws_sdk_universal_arn_extracts_service_and_action() {
        assert_eq!(
            parse_aws_sdk_universal_arn("arn:aws:scheduler:::aws-sdk:sesv2:sendEmail"),
            Some(("sesv2".to_string(), "sendEmail".to_string()))
        );
        assert_eq!(
            parse_aws_sdk_universal_arn("arn:aws:scheduler:us-east-1:1:aws-sdk:ecs:runTask"),
            Some(("ecs".to_string(), "runTask".to_string()))
        );
        assert_eq!(
            parse_aws_sdk_universal_arn("arn:aws:sqs:us-east-1:1:q"),
            None
        );
    }

    #[test]
    fn deliver_target_unknown_arn_is_ok() {
        let recorder = Arc::new(Recorder {
            calls: Mutex::new(Vec::new()),
            fail_arn: None,
        });
        let bus = Arc::new(DeliveryBus::new().with_sqs(recorder.clone()));
        let sched = make_schedule("arn:aws:ec2:us-east-1:1:instance/foo", None, None);
        assert!(deliver_target(&bus, &sched).is_ok());
        assert!(recorder.calls.lock().unwrap().is_empty());
    }
}
