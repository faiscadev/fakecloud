//! Asynchronous (`Event`) invocation: Lambda's error handling for queued
//! events. A failed attempt (a function error, or an invocation Lambda could
//! not run) is retried up to `MaximumRetryAttempts` times, one minute after
//! the first attempt and two minutes after the second, unless the event
//! would outlive `MaximumEventAge`. The final outcome is recorded to the
//! configured `OnSuccess` / `OnFailure` destination, and a discarded event is
//! also sent as-is to the function's dead-letter queue.
//!
//! See <https://docs.aws.amazon.com/lambda/latest/dg/invocation-async-error-handling.html>
//! and <https://docs.aws.amazon.com/lambda/latest/dg/invocation-async-retain-records.html>.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use fakecloud_core::delivery::{DeliveryBus, LambdaInvocationOutput, SqsMessageAttribute};

use crate::runtime::ContainerRuntime;
use crate::state::{LambdaFunction, LambdaState};
use fakecloud_core::multi_account::MultiRegionState;

/// Default `MaximumRetryAttempts` when the function has no event invoke
/// config.
const DEFAULT_MAX_RETRIES: u32 = 2;
/// Default `MaximumEventAge`: six hours.
const DEFAULT_MAX_EVENT_AGE: Duration = Duration::from_secs(6 * 60 * 60);
/// Wait before the first retry and before the second.
pub(crate) const RETRY_DELAYS: [Duration; 2] = [Duration::from_secs(60), Duration::from_secs(120)];
/// `ErrorMessage` on a dead-letter message is the first 1 KB of the error.
const DLQ_ERROR_MESSAGE_LIMIT: usize = 1024;

/// A function's async error-handling configuration, resolved for the
/// qualifier being invoked.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AsyncSettings {
    pub max_retries: u32,
    pub max_event_age: Duration,
    pub destination_config: Option<serde_json::Value>,
    /// `DeadLetterConfig.TargetArn`, which lives on the function ($LATEST)
    /// and is shared by every version.
    pub dead_letter_arn: Option<String>,
}

impl Default for AsyncSettings {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            max_event_age: DEFAULT_MAX_EVENT_AGE,
            destination_config: None,
            dead_letter_arn: None,
        }
    }
}

/// Resolve the async settings for `function_name` invoked at `qualifier`.
/// Event invoke config is per qualifier; an unqualified invoke reads
/// `$LATEST`, and a qualifier without its own config falls back to
/// `$LATEST`'s.
pub(crate) fn resolve_settings(
    accounts: &MultiRegionState<LambdaState>,
    account_id: &str,
    region: &str,
    function_name: &str,
    qualifier: Option<&str>,
) -> AsyncSettings {
    let mut settings = AsyncSettings::default();
    let Some(state) = accounts.regional(account_id, region) else {
        return settings;
    };
    let candidates = [qualifier.unwrap_or("$LATEST"), "$LATEST"];
    if let Some(cfg) = candidates.iter().find_map(|q| {
        state
            .event_invoke_configs
            .get(&format!("{function_name}:{q}"))
    }) {
        settings.max_retries = u32::try_from(cfg.maximum_retry_attempts).unwrap_or(0);
        // `0` means the config leaves MaximumEventAgeInSeconds unset, which
        // is the six-hour default.
        settings.max_event_age = u64::try_from(cfg.maximum_event_age)
            .ok()
            .filter(|secs| *secs > 0)
            .map_or(DEFAULT_MAX_EVENT_AGE, Duration::from_secs);
        settings.destination_config = cfg
            .destination_config
            .clone()
            .filter(|dc| !dc.is_null() && dc.as_object().is_none_or(|o| !o.is_empty()));
    }
    settings.dead_letter_arn = state
        .functions
        .get(function_name)
        .and_then(|f| f.dead_letter_config_arn.clone())
        .filter(|arn| !arn.is_empty());
    settings
}

/// Why the invocation's processing ended, as the destination record's
/// `requestContext.condition` reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Condition {
    Success,
    RetriesExhausted,
    EventAgeExceeded,
}

impl Condition {
    fn as_str(self) -> &'static str {
        match self {
            Condition::Success => "Success",
            Condition::RetriesExhausted => "RetriesExhausted",
            Condition::EventAgeExceeded => "EventAgeExceeded",
        }
    }
}

/// The result of processing one queued event.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct AsyncOutcome {
    /// The last attempt's result.
    pub result: Result<LambdaInvocationOutput, String>,
    /// How many times the function was invoked.
    pub invoke_count: u32,
    pub condition: Condition,
}

impl AsyncOutcome {
    fn succeeded(&self) -> bool {
        self.condition == Condition::Success
    }
}

/// Run `attempt` with Lambda's async retry policy. An attempt fails when it
/// produced a function error or could not run.
pub(crate) async fn run_attempts<F, Fut>(
    settings: &AsyncSettings,
    delays: [Duration; 2],
    mut attempt: F,
) -> AsyncOutcome
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<LambdaInvocationOutput, String>>,
{
    let start = Instant::now();
    let mut invoke_count = 0u32;
    loop {
        let result = attempt().await;
        invoke_count += 1;
        let failed = match &result {
            Ok(output) => output.is_function_error(),
            Err(_) => true,
        };
        if !failed {
            return AsyncOutcome {
                result,
                invoke_count,
                condition: Condition::Success,
            };
        }
        let retries_done = invoke_count - 1;
        if retries_done >= settings.max_retries {
            return AsyncOutcome {
                result,
                invoke_count,
                condition: Condition::RetriesExhausted,
            };
        }
        let delay = delays[(retries_done as usize).min(delays.len() - 1)];
        if start.elapsed() + delay > settings.max_event_age {
            return AsyncOutcome {
                result,
                invoke_count,
                condition: Condition::EventAgeExceeded,
            };
        }
        tokio::time::sleep(delay).await;
    }
}

/// A queued `Event` invocation.
pub(crate) struct AsyncInvocation {
    pub runtime: Arc<ContainerRuntime>,
    pub bus: Option<Arc<DeliveryBus>>,
    pub settings: AsyncSettings,
    pub func: LambdaFunction,
    pub layer_zips: Vec<Vec<u8>>,
    pub payload: Vec<u8>,
    pub executed_version: String,
    /// Held until processing finishes (the reserved-concurrency counter).
    pub guard: Option<Box<dyn Send>>,
}

/// Process `inv` in the background, on the current Tokio runtime.
pub(crate) fn spawn(inv: AsyncInvocation) {
    let Ok(handle) = tokio::runtime::Handle::try_current() else {
        tracing::error!(
            function = %inv.func.function_name,
            "async Lambda invocation dropped: no async runtime on this thread"
        );
        return;
    };
    handle.spawn(async move {
        let AsyncInvocation {
            runtime,
            bus,
            settings,
            func,
            layer_zips,
            payload,
            executed_version,
            guard,
        } = inv;
        // The reserved-concurrency slot covers the first attempt only: an
        // event waiting for a retry uses no concurrency on AWS.
        let guard = std::sync::Mutex::new(guard);
        let outcome = run_attempts(&settings, RETRY_DELAYS, || {
            let runtime = runtime.clone();
            let func = &func;
            let payload = &payload;
            let layer_zips = &layer_zips;
            let slot = guard.lock().ok().and_then(|mut g| g.take());
            async move {
                let result = runtime
                    .invoke(func, payload, layer_zips)
                    .await
                    .map_err(|e| e.to_string());
                drop(slot);
                result
            }
        })
        .await;
        if !outcome.succeeded() {
            tracing::warn!(
                function = %func.function_name,
                attempts = outcome.invoke_count,
                condition = outcome.condition.as_str(),
                "async Lambda invocation failed all processing attempts"
            );
        }
        if let Some(bus) = bus {
            let request_id = uuid::Uuid::new_v4().to_string();
            record_outcome(
                &bus,
                &func,
                &executed_version,
                &request_id,
                &payload,
                &outcome,
                &settings,
            );
        }
    });
}

/// Queue an `Event` invocation of the function `function_arn` names, as an
/// event source (S3, SNS, EventBridge, Scheduler, ...) does. Resolves the
/// function and its async settings; the outcome is recorded through the
/// runtime's bound destinations bus
/// ([`ContainerRuntime::set_async_destinations`]). `Err` when the function
/// does not exist or has no deployment package, which the caller treats as
/// a delivery failure.
pub fn queue_async_invocation(
    state: &crate::SharedLambdaState,
    runtime: Arc<ContainerRuntime>,
    function_arn: &str,
    payload: &[u8],
) -> Result<(), String> {
    let (func, layer_zips, settings, executed_version) = {
        let accounts = state.read();
        let (account, region, name) = crate::function_location(
            function_arn,
            accounts.default_account_id(),
            accounts.region(),
        );
        let (func, layer_zips) = crate::resolve_invocable(&accounts, function_arn, account, region)
            .ok_or_else(|| format!("Function not found: {function_arn}"))?;
        // Cross-service invokes run the function's current code ($LATEST);
        // a qualifier on the ARN still selects its event invoke config.
        let settings = resolve_settings(
            &accounts,
            account,
            region,
            name,
            arn_qualifier(function_arn),
        );
        (func, layer_zips, settings, "$LATEST".to_string())
    };
    if func.code_zip.is_none() && func.package_type != "Image" {
        return Err(format!(
            "Function {} has no deployment package",
            func.function_name
        ));
    }
    if tokio::runtime::Handle::try_current().is_err() {
        return Err("no async runtime is available to queue the invocation".to_string());
    }
    spawn(AsyncInvocation {
        bus: runtime.async_destinations(),
        runtime,
        settings,
        func,
        layer_zips,
        payload: payload.to_vec(),
        executed_version,
        guard: None,
    });
    Ok(())
}

/// The invocation record Lambda sends to a destination.
fn invocation_record(
    func: &LambdaFunction,
    executed_version: &str,
    request_id: &str,
    request_payload: &[u8],
    outcome: &AsyncOutcome,
) -> serde_json::Value {
    let request_payload: serde_json::Value =
        serde_json::from_slice(request_payload).unwrap_or(serde_json::Value::Null);
    let mut response_context = serde_json::json!({
        "statusCode": 200,
        "executedVersion": executed_version,
    });
    let response_payload = match &outcome.result {
        Ok(output) => {
            if let Some(fe) = &output.function_error {
                response_context["functionError"] = serde_json::Value::String(fe.clone());
            }
            parse_payload(&output.payload)
        }
        Err(err) => serde_json::json!({ "errorMessage": err }),
    };
    serde_json::json!({
        "version": "1.0",
        "timestamp": chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string(),
        "requestContext": {
            "requestId": request_id,
            "functionArn": format!("{}:{executed_version}", unqualified_arn(&func.function_arn)),
            "condition": outcome.condition.as_str(),
            "approximateInvokeCount": outcome.invoke_count,
        },
        "requestPayload": request_payload,
        "responseContext": response_context,
        "responsePayload": response_payload,
    })
}

fn parse_payload(bytes: &[u8]) -> serde_json::Value {
    if bytes.is_empty() {
        return serde_json::Value::Null;
    }
    serde_json::from_slice(bytes)
        .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(bytes).into_owned()))
}

/// The `:qualifier` suffix of a function ARN (`...:function:name:prod`) or
/// partial reference (`name:prod`), if any.
fn arn_qualifier(function_ref: &str) -> Option<&str> {
    let parts: Vec<&str> = function_ref.split(':').collect();
    let q = if function_ref.starts_with("arn:") {
        parts.get(7).copied()
    } else if parts.get(1) == Some(&"function") {
        parts.get(3).copied()
    } else {
        parts.get(1).copied()
    };
    q.filter(|q| !q.is_empty())
}

/// `arn:...:function:name` without any `:qualifier` suffix.
fn unqualified_arn(arn: &str) -> &str {
    let parts: Vec<&str> = arn.splitn(8, ':').collect();
    if parts.len() >= 8 {
        let qualified_len = arn.len() - parts[7].len() - 1;
        &arn[..qualified_len]
    } else {
        arn
    }
}

/// The error message an outcome reports, for the dead-letter `ErrorMessage`
/// attribute.
fn error_message(outcome: &AsyncOutcome) -> String {
    match &outcome.result {
        Ok(output) => output
            .error_message()
            .unwrap_or_else(|| String::from_utf8_lossy(&output.payload).into_owned()),
        Err(err) => err.clone(),
    }
}

fn truncate_utf8(s: &str, max: usize) -> &str {
    if s.len() <= max {
        return s;
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

/// Send the outcome to the configured destination and, for a discarded
/// event, the dead-letter queue.
fn record_outcome(
    bus: &Arc<DeliveryBus>,
    func: &LambdaFunction,
    executed_version: &str,
    request_id: &str,
    request_payload: &[u8],
    outcome: &AsyncOutcome,
    settings: &AsyncSettings,
) {
    let key = if outcome.succeeded() {
        "OnSuccess"
    } else {
        "OnFailure"
    };
    if let Some(dest) = settings
        .destination_config
        .as_ref()
        .and_then(|cfg| cfg.get(key))
        .and_then(|v| v.get("Destination"))
        .and_then(|v| v.as_str())
        .filter(|d| !d.is_empty())
    {
        let record =
            invocation_record(func, executed_version, request_id, request_payload, outcome);
        send_record(bus, func, dest, &record, outcome.succeeded());
    }
    if !outcome.succeeded() {
        if let Some(dlq) = settings.dead_letter_arn.as_deref() {
            send_to_dead_letter(bus, dlq, request_id, request_payload, outcome);
        }
    }
}

fn send_record(
    bus: &Arc<DeliveryBus>,
    func: &LambdaFunction,
    dest: &str,
    record: &serde_json::Value,
    succeeded: bool,
) {
    let body = record.to_string();
    let function_arn = unqualified_arn(&func.function_arn);
    if dest.contains(":sqs:") {
        bus.send_to_sqs(dest, &body, &HashMap::new());
    } else if dest.contains(":sns:") {
        bus.publish_to_sns(dest, &body, None);
    } else if dest.contains(":lambda:") {
        // Lambda invokes a destination function asynchronously, with its
        // own async error handling.
        if let Some(Err(err)) = bus.invoke_lambda_async(dest, &body) {
            tracing::warn!(
                function_arn,
                destination = dest,
                %err,
                "Lambda destination: function could not be invoked"
            );
        }
    } else if dest.starts_with("arn:") && dest.split(':').nth(2) == Some("s3") {
        // On-failure only (AWS rejects an S3 OnSuccess destination).
        let bucket = dest.rsplit(':').next().unwrap_or("");
        let now = chrono::Utc::now();
        let object_key = format!(
            "aws/lambda/async/{}/{}/{}-{}",
            func.function_name,
            now.format("%Y/%m/%d"),
            now.format("%Y-%m-%dT%H.%M.%S"),
            uuid::Uuid::new_v4()
        );
        let account_id = function_arn.split(':').nth(4).unwrap_or("");
        if let Err(err) = bus.put_object_to_s3(
            account_id,
            bucket,
            &object_key,
            body.into_bytes(),
            Some("application/json"),
        ) {
            tracing::warn!(
                function_arn,
                destination = dest,
                %err,
                "Lambda destination: S3 bucket refused the invocation record"
            );
        }
    } else if dest.contains(":events:") || dest.contains(":eventbridge:") {
        let detail_type = if succeeded {
            "Lambda Function Invocation Result - Success"
        } else {
            "Lambda Function Invocation Result - Failure"
        };
        // The destination names the bus by ARN (possibly another account's);
        // the event originates in the function's account and region.
        let mut arn_parts = function_arn.split(':');
        let region = arn_parts.nth(3).unwrap_or("");
        let account_id = arn_parts.next().unwrap_or("");
        // Lambda puts the record as the function's execution role; a bus in
        // another account must allow it, else the destination delivery fails.
        if let Err(err) =
            bus.put_event_to_eventbridge(&fakecloud_core::delivery::CrossServiceEvent {
                source: "lambda",
                detail_type,
                detail: &body,
                event_bus: dest,
                account_id,
                region,
                resources: &[function_arn.to_string(), dest.to_string()],
                principal_arn: (!func.role.is_empty()).then_some(func.role.as_str()),
            })
        {
            tracing::warn!(
                function_arn,
                destination = dest,
                %err,
                "Lambda destination: EventBridge bus refused the invocation record"
            );
        }
    }
}

/// Dead-letter message: the event as-is, with `RequestID`, `ErrorCode` and
/// `ErrorMessage` attributes.
fn send_to_dead_letter(
    bus: &Arc<DeliveryBus>,
    dlq: &str,
    request_id: &str,
    request_payload: &[u8],
    outcome: &AsyncOutcome,
) {
    let body = String::from_utf8_lossy(request_payload).into_owned();
    let message = error_message(outcome);
    let error_code = match &outcome.result {
        Ok(_) => "200",
        Err(_) => "500",
    };
    let attr = |data_type: &str, value: &str| SqsMessageAttribute {
        data_type: data_type.to_string(),
        string_value: Some(value.to_string()),
        binary_value: None,
    };
    let attributes = HashMap::from([
        ("RequestID".to_string(), attr("String", request_id)),
        ("ErrorCode".to_string(), attr("Number", error_code)),
        (
            "ErrorMessage".to_string(),
            attr("String", truncate_utf8(&message, DLQ_ERROR_MESSAGE_LIMIT)),
        ),
    ]);
    if dlq.contains(":sqs:") {
        bus.send_to_sqs_with_attrs(dlq, &body, &attributes, None, None);
    } else if dlq.contains(":sns:") {
        bus.publish_to_sns_with_attributes(dlq, &body, None, &attributes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    fn ok() -> Result<LambdaInvocationOutput, String> {
        Ok(LambdaInvocationOutput::success(br#"{"ok":true}"#.to_vec()))
    }

    fn function_error() -> Result<LambdaInvocationOutput, String> {
        Ok(LambdaInvocationOutput::unhandled(
            br#"{"errorType":"ValueError","errorMessage":"boom"}"#.to_vec(),
        ))
    }

    async fn run_with(
        settings: AsyncSettings,
        results: Vec<Result<LambdaInvocationOutput, String>>,
    ) -> (AsyncOutcome, u32) {
        let calls = AtomicU32::new(0);
        let outcome = run_attempts(&settings, [Duration::ZERO; 2], || {
            let i = calls.fetch_add(1, Ordering::SeqCst) as usize;
            let r = results[i.min(results.len() - 1)].clone();
            async move { r }
        })
        .await;
        (outcome, calls.load(Ordering::SeqCst))
    }

    #[tokio::test]
    async fn success_is_not_retried() {
        let (outcome, calls) = run_with(AsyncSettings::default(), vec![ok()]).await;
        assert_eq!(calls, 1);
        assert_eq!(outcome.condition, Condition::Success);
    }

    #[tokio::test]
    async fn function_error_is_retried_twice_by_default() {
        let (outcome, calls) = run_with(AsyncSettings::default(), vec![function_error()]).await;
        assert_eq!(calls, 3);
        assert_eq!(outcome.invoke_count, 3);
        assert_eq!(outcome.condition, Condition::RetriesExhausted);
    }

    #[tokio::test]
    async fn an_error_shaped_success_is_not_retried() {
        // Only the function-error flag decides; the payload shape does not.
        let shaped = Ok(LambdaInvocationOutput::success(
            br#"{"errorType":"X","errorMessage":"not an error"}"#.to_vec(),
        ));
        let (outcome, calls) = run_with(AsyncSettings::default(), vec![shaped]).await;
        assert_eq!(calls, 1);
        assert_eq!(outcome.condition, Condition::Success);
    }

    #[tokio::test]
    async fn retry_that_succeeds_ends_in_success() {
        let (outcome, calls) =
            run_with(AsyncSettings::default(), vec![function_error(), ok()]).await;
        assert_eq!(calls, 2);
        assert_eq!(outcome.condition, Condition::Success);
        assert_eq!(outcome.invoke_count, 2);
    }

    #[tokio::test]
    async fn zero_retries_fails_after_one_attempt() {
        let settings = AsyncSettings {
            max_retries: 0,
            ..AsyncSettings::default()
        };
        let (outcome, calls) = run_with(settings, vec![Err("no container".into())]).await;
        assert_eq!(calls, 1);
        assert_eq!(outcome.condition, Condition::RetriesExhausted);
    }

    #[tokio::test(start_paused = true)]
    async fn retries_wait_one_then_two_minutes() {
        let calls = std::sync::Mutex::new(Vec::new());
        let start = tokio::time::Instant::now();
        run_attempts(&AsyncSettings::default(), RETRY_DELAYS, || {
            calls.lock().unwrap().push(start.elapsed());
            async { function_error() }
        })
        .await;
        let at = calls.into_inner().unwrap();
        assert_eq!(
            at,
            vec![
                Duration::ZERO,
                Duration::from_secs(60),
                Duration::from_secs(180)
            ]
        );
    }

    #[tokio::test]
    async fn event_older_than_max_age_is_discarded() {
        let settings = AsyncSettings {
            max_event_age: Duration::from_secs(30),
            ..AsyncSettings::default()
        };
        let calls = AtomicU32::new(0);
        let outcome = run_attempts(&settings, RETRY_DELAYS, || {
            calls.fetch_add(1, Ordering::SeqCst);
            async { function_error() }
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(outcome.condition, Condition::EventAgeExceeded);
    }

    fn test_func() -> LambdaFunction {
        LambdaFunction {
            function_arn: "arn:aws:lambda:us-east-1:123456789012:function:f".into(),
            function_name: "f".into(),
            ..LambdaFunction::default()
        }
    }

    fn state_with_config(
        maximum_event_age: i64,
        maximum_retry_attempts: i64,
    ) -> MultiRegionState<LambdaState> {
        let mut accounts: MultiRegionState<LambdaState> =
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", "");
        accounts
            .regional_mut("123456789012", "us-east-1")
            .event_invoke_configs
            .insert(
                "f:$LATEST".to_string(),
                crate::state::EventInvokeConfig {
                    function_arn: "arn:aws:lambda:us-east-1:123456789012:function:f".into(),
                    maximum_event_age,
                    maximum_retry_attempts,
                    destination_config: Some(serde_json::json!({
                        "OnFailure": {"Destination": "arn:aws:sqs:us-east-1:123456789012:q"}
                    })),
                    last_modified: chrono::Utc::now(),
                },
            );
        accounts
    }

    #[test]
    fn unset_event_age_keeps_the_six_hour_default() {
        // A config that sets only a destination stores MaximumEventAge 0.
        let accounts = state_with_config(0, 2);
        let s = resolve_settings(&accounts, "123456789012", "us-east-1", "f", None);
        assert_eq!(s.max_event_age, DEFAULT_MAX_EVENT_AGE);
        assert_eq!(s.max_retries, 2);
        assert!(s.destination_config.is_some());

        let accounts = state_with_config(120, 1);
        let s = resolve_settings(&accounts, "123456789012", "us-east-1", "f", Some("prod"));
        assert_eq!(s.max_event_age, Duration::from_secs(120));
        assert_eq!(
            s.max_retries, 1,
            "an unconfigured qualifier falls back to $LATEST"
        );
    }

    #[test]
    fn failure_record_carries_function_error_and_error_payload() {
        let outcome = AsyncOutcome {
            result: function_error(),
            invoke_count: 3,
            condition: Condition::RetriesExhausted,
        };
        let record = invocation_record(&test_func(), "$LATEST", "req-1", br#"{"a":1}"#, &outcome);
        assert_eq!(record["requestContext"]["condition"], "RetriesExhausted");
        assert_eq!(record["requestContext"]["approximateInvokeCount"], 3);
        assert_eq!(
            record["requestContext"]["functionArn"],
            "arn:aws:lambda:us-east-1:123456789012:function:f:$LATEST"
        );
        assert_eq!(record["requestPayload"], serde_json::json!({"a": 1}));
        assert_eq!(record["responseContext"]["statusCode"], 200);
        assert_eq!(record["responseContext"]["functionError"], "Unhandled");
        assert_eq!(record["responsePayload"]["errorType"], "ValueError");
        assert_eq!(record["responsePayload"]["errorMessage"], "boom");
    }

    #[test]
    fn success_record_has_no_function_error() {
        let outcome = AsyncOutcome {
            result: ok(),
            invoke_count: 1,
            condition: Condition::Success,
        };
        let record = invocation_record(&test_func(), "3", "req-1", b"{}", &outcome);
        assert_eq!(record["requestContext"]["condition"], "Success");
        assert!(record["responseContext"].get("functionError").is_none());
        assert_eq!(record["responseContext"]["executedVersion"], "3");
        assert_eq!(
            record["requestContext"]["functionArn"],
            "arn:aws:lambda:us-east-1:123456789012:function:f:3"
        );
        assert_eq!(record["responsePayload"], serde_json::json!({"ok": true}));
    }

    #[test]
    fn arn_qualifier_reads_every_reference_form() {
        assert_eq!(
            arn_qualifier("arn:aws:lambda:us-east-1:1:function:f:prod"),
            Some("prod")
        );
        assert_eq!(arn_qualifier("arn:aws:lambda:us-east-1:1:function:f"), None);
        assert_eq!(arn_qualifier("f:2"), Some("2"));
        assert_eq!(arn_qualifier("123456789012:function:f:live"), Some("live"));
        assert_eq!(arn_qualifier("f"), None);
    }

    #[test]
    fn unqualified_arn_strips_the_qualifier() {
        assert_eq!(
            unqualified_arn("arn:aws:lambda:us-east-1:1:function:f:prod"),
            "arn:aws:lambda:us-east-1:1:function:f"
        );
        assert_eq!(
            unqualified_arn("arn:aws:lambda:us-east-1:1:function:f"),
            "arn:aws:lambda:us-east-1:1:function:f"
        );
    }

    #[test]
    fn dead_letter_error_message_is_truncated_to_1kb() {
        let long = "é".repeat(700);
        let t = truncate_utf8(&long, DLQ_ERROR_MESSAGE_LIMIT);
        assert!(t.len() <= DLQ_ERROR_MESSAGE_LIMIT);
        assert!(long.starts_with(t));
    }
}
