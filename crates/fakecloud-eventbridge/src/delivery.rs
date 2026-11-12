use std::sync::Arc;

use chrono::Utc;

use fakecloud_core::delivery::{
    CrossServiceEvent, DeliveryBus, EventBridgeDelivery, EventBridgeDeliveryError,
};
use fakecloud_lambda::runtime::ContainerRuntime;
use fakecloud_lambda::SharedLambdaState;
use fakecloud_logs::SharedLogsState;

use crate::service::helpers::archive_matching_event;
use crate::service::{
    account_root_principal, cross_account_put_denial, dispatch_event_target, matches_pattern,
    EventDispatchContext,
};
use crate::state::{PutEvent, SharedEventBridgeState};

/// The non-bus plumbing a rule target dispatch needs beyond the
/// [`DeliveryBus`]: Lambda state + container runtime (Lambda targets are
/// recorded in Lambda's invocation log and executed directly, not via the
/// bus), and CloudWatch Logs state + its persist hook (Logs targets write the
/// log group directly).
///
/// PutEvents (`EventBridgeService`), the rule `Scheduler` and every
/// cross-service [`EventBridgeDeliveryImpl`] take the same value, so an event
/// another service publishes (S3, SES, ECS, RDS, Step Functions, ...) reaches
/// the same target types as one sent with PutEvents.
///
/// The delivery dependencies and the persistence hook behave differently
/// when absent:
/// - `lambda_state` / `logs_state` missing: that target type cannot be
///   delivered; dispatch records the attempt in EventBridge's own log and
///   emits a `warn!` naming the skipped target.
/// - `container_runtime` missing (no Docker/Podman/Kubernetes) while
///   `lambda_state` is wired: Lambda targets are still recorded in Lambda's
///   invocation log but not executed, with a `warn!`. Without `lambda_state`
///   the previous case applies and nothing is recorded in Lambda's log.
/// - `logs_persist` missing (memory mode): Logs targets are still delivered
///   to the log group as normal; there is just no snapshot to write through,
///   so nothing is skipped and nothing is warned.
#[derive(Clone, Default)]
pub struct EventTargetWiring {
    pub lambda_state: Option<SharedLambdaState>,
    pub logs_state: Option<SharedLogsState>,
    /// Optional persistence hook, not a delivery dependency (see above).
    pub logs_persist: Option<fakecloud_persistence::SnapshotHook>,
    pub container_runtime: Option<Arc<ContainerRuntime>>,
}

/// Implements EventBridgeDelivery so other services (S3, SES, ECS, ...) can
/// put events on an EventBridge bus with full rule matching and target
/// delivery.
pub struct EventBridgeDeliveryImpl {
    state: SharedEventBridgeState,
    delivery: Arc<DeliveryBus>,
    wiring: EventTargetWiring,
}

impl EventBridgeDeliveryImpl {
    pub fn new(state: SharedEventBridgeState, delivery: Arc<DeliveryBus>) -> Self {
        Self {
            state,
            delivery,
            wiring: EventTargetWiring::default(),
        }
    }

    /// Wire every non-bus target dependency at once (see [`EventTargetWiring`]).
    pub fn with_target_wiring(mut self, wiring: EventTargetWiring) -> Self {
        self.wiring = wiring;
        self
    }

    pub fn with_lambda(mut self, lambda_state: SharedLambdaState) -> Self {
        self.wiring.lambda_state = Some(lambda_state);
        self
    }

    pub fn with_logs(mut self, logs_state: SharedLogsState) -> Self {
        self.wiring.logs_state = Some(logs_state);
        self
    }

    /// Wire the CloudWatch Logs persist hook so events this bus delivers to a
    /// Logs target are written through to the Logs snapshot (see
    /// `EventDispatchContext::logs_persist`).
    pub fn with_logs_persist(mut self, hook: fakecloud_persistence::SnapshotHook) -> Self {
        self.wiring.logs_persist = Some(hook);
        self
    }

    pub fn with_runtime(mut self, runtime: Arc<ContainerRuntime>) -> Self {
        self.wiring.container_runtime = Some(runtime);
        self
    }
}

/// An [`EventBridgeDelivery`] whose real implementation is supplied after
/// construction. Breaks the construction cycle where the bus EventBridge
/// targets deliver through (which starts Step Functions executions) must
/// itself hold an EventBridge sender (for the interpreter's
/// `events:putEvents` task). Events put before [`Self::set`] is called are
/// dropped with a warning.
#[derive(Clone, Default)]
pub struct DeferredEventBridgeDelivery {
    inner: Arc<std::sync::OnceLock<Arc<dyn EventBridgeDelivery>>>,
}

impl DeferredEventBridgeDelivery {
    pub fn new() -> Self {
        Self::default()
    }

    /// Install the real sender. Only the first call takes effect.
    pub fn set(&self, delivery: Arc<dyn EventBridgeDelivery>) {
        if self.inner.set(delivery).is_err() {
            tracing::warn!("deferred EventBridge delivery already set; ignoring");
        }
    }

    fn get(&self, source: &str, detail_type: &str) -> Option<&Arc<dyn EventBridgeDelivery>> {
        let inner = self.inner.get();
        if inner.is_none() {
            tracing::warn!(
                source,
                detail_type,
                "EventBridge delivery not wired yet; dropping event"
            );
        }
        inner
    }
}

impl EventBridgeDelivery for DeferredEventBridgeDelivery {
    fn put_event(&self, event: &CrossServiceEvent<'_>) -> Result<String, EventBridgeDeliveryError> {
        match self.get(event.source, event.detail_type) {
            Some(inner) => inner.put_event(event),
            // Not wired yet (startup): the event is dropped (with the warning
            // `get` logs), which the source must record as a failed delivery.
            None => Err(EventBridgeDeliveryError::Unavailable(
                "EventBridge delivery is not wired yet".to_string(),
            )),
        }
    }
}

/// The account that owns the bus `event_bus` names: the ARN's account for a
/// full event-bus ARN, otherwise the event's originating account.
fn bus_owner_account<'a>(event_bus: &'a str, origin_account: &'a str) -> &'a str {
    if event_bus.starts_with("arn:") {
        event_bus
            .split(':')
            .nth(4)
            .filter(|a| !a.is_empty())
            .unwrap_or(origin_account)
    } else {
        origin_account
    }
}

/// The region of the bus `event_bus` names: the ARN's region for a full
/// event-bus ARN, otherwise the event's originating region.
fn bus_region<'a>(event_bus: &'a str, origin_region: &'a str) -> &'a str {
    if event_bus.starts_with("arn:") {
        event_bus
            .split(':')
            .nth(3)
            .filter(|r| !r.is_empty())
            .unwrap_or(origin_region)
    } else {
        origin_region
    }
}

/// The principal a cross-service event is put as: the source's role when it
/// names one, otherwise the originating account's root.
fn event_principal(event: &CrossServiceEvent<'_>) -> fakecloud_core::auth::Principal {
    match event.principal_arn {
        Some(arn) if !arn.is_empty() => fakecloud_core::auth::Principal {
            arn: arn.to_string(),
            user_id: arn.to_string(),
            account_id: event.account_id.to_string(),
            principal_type: fakecloud_core::auth::PrincipalType::AssumedRole,
            source_identity: None,
            tags: None,
        },
        _ => account_root_principal(event.account_id, event.region),
    }
}

impl EventBridgeDelivery for EventBridgeDeliveryImpl {
    fn put_event(&self, event: &CrossServiceEvent<'_>) -> Result<String, EventBridgeDeliveryError> {
        let CrossServiceEvent {
            source,
            detail_type,
            detail,
            event_bus,
            account_id,
            region,
            resources,
            principal_arn: _,
        } = *event;
        let event_id = uuid::Uuid::new_v4().to_string();
        let now = Utc::now();
        let owner_account = bus_owner_account(event_bus, account_id);
        // The bus (its policy, ARN and rule targets) lives in its own region,
        // which a bus ARN names; the event keeps its originating `region`.
        let target_region = bus_region(event_bus, region);

        let mut accounts = self.state.write();
        let state = accounts.get_or_create(owner_account);
        let event_bus_name = state.resolve_bus_name(event_bus);

        // A bus in another account takes the event only when its resource
        // policy allows the source's principal, exactly as PutEvents gates
        // cross-account callers. A refused event is not stored, archived or
        // delivered.
        if owner_account != account_id {
            if let Some(message) = cross_account_put_denial(
                state,
                &event_bus_name,
                target_region,
                &event_principal(event),
            ) {
                return Err(EventBridgeDeliveryError::AccessDenied(message));
            }
        }

        let stored = PutEvent {
            event_id: event_id.clone(),
            source: source.to_string(),
            detail_type: detail_type.to_string(),
            detail: detail.to_string(),
            event_bus_name: event_bus_name.clone(),
            time: now,
            resources: resources.to_vec(),
        };
        // Archives on the bus capture service events exactly like PutEvents
        // entries.
        archive_matching_event(
            state,
            &stored,
            &event_bus_name,
            source,
            detail_type,
            detail,
            account_id,
            region,
            resources,
        );
        state.events.push(stored);

        // Find matching rules and their targets. Patterns match the event's
        // originating account and region, as on AWS.
        let matching_targets: Vec<(String, crate::state::EventTarget)> = state
            .rules
            .values()
            .filter(|r| {
                r.event_bus_name == event_bus_name
                    && r.state == "ENABLED"
                    && matches_pattern(
                        r.event_pattern.as_deref(),
                        source,
                        detail_type,
                        detail,
                        account_id,
                        region,
                        resources,
                        &event_id,
                        &now.to_rfc3339(),
                    )
            })
            .flat_map(|r| r.targets.iter().map(|t| (r.arn.clone(), t.clone())))
            .collect();

        // Drop the lock before delivering
        drop(accounts);

        if matching_targets.is_empty() {
            return Ok(event_id);
        }

        // Build the EventBridge event envelope
        let detail_value: serde_json::Value =
            serde_json::from_str(detail).unwrap_or(serde_json::json!({}));
        let event_json = serde_json::json!({
            "version": "0",
            "id": event_id,
            "source": source,
            "account": account_id,
            "detail-type": detail_type,
            "detail": detail_value,
            "time": now.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
            "region": region,
            "resources": resources,
        });
        // Targets belong to the rule, which lives in the bus owner's account.
        let ctx = EventDispatchContext {
            state: &self.state,
            delivery: &self.delivery,
            lambda_state: self.wiring.lambda_state.as_ref(),
            logs_state: self.wiring.logs_state.as_ref(),
            logs_persist: self.wiring.logs_persist.as_ref(),
            container_runtime: &self.wiring.container_runtime,
            account_id: owner_account,
            region: target_region,
        };
        for (rule_arn, target) in matching_targets {
            dispatch_event_target(
                &ctx,
                &target,
                &event_json,
                &event_id,
                detail_type,
                Some(&rule_arn),
            );
        }
        Ok(event_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{EventRule, EventTarget as EbTarget, SharedEventBridgeState};
    use fakecloud_aws::arn::Arn;
    use fakecloud_core::delivery::{SnsDelivery, SqsDelivery};
    use parking_lot::RwLock;
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Mutex;

    #[derive(Default)]
    struct Recorder {
        sqs: Mutex<Vec<(String, String)>>,
        sns: Mutex<Vec<(String, String, Option<String>)>>,
    }

    impl SqsDelivery for Recorder {
        fn deliver_to_queue(
            &self,
            _sender: fakecloud_core::delivery::DeliverySender<'_>,
            arn: &str,
            body: &str,
            _: &HashMap<String, String>,
        ) {
            self.sqs
                .lock()
                .unwrap()
                .push((arn.to_string(), body.to_string()));
        }
        fn deliver_to_queue_with_attrs(
            &self,
            _sender: fakecloud_core::delivery::DeliverySender<'_>,
            arn: &str,
            body: &str,
            _: &HashMap<String, fakecloud_core::delivery::SqsMessageAttribute>,
            _: Option<&str>,
            _: Option<&str>,
        ) {
            self.sqs
                .lock()
                .unwrap()
                .push((arn.to_string(), body.to_string()));
        }
    }

    impl SnsDelivery for Recorder {
        fn publish_to_topic(&self, arn: &str, msg: &str, subject: Option<&str>) {
            self.sns.lock().unwrap().push((
                arn.to_string(),
                msg.to_string(),
                subject.map(|s| s.to_string()),
            ));
        }
    }

    /// An event originating in the default test account and region.
    fn ev<'a>(
        source: &'a str,
        detail_type: &'a str,
        detail: &'a str,
        bus: &'a str,
    ) -> CrossServiceEvent<'a> {
        ev_in(source, detail_type, detail, bus, "123456789012")
    }

    /// An event originating in `account_id` (us-east-1).
    fn ev_in<'a>(
        source: &'a str,
        detail_type: &'a str,
        detail: &'a str,
        bus: &'a str,
        account_id: &'a str,
    ) -> CrossServiceEvent<'a> {
        CrossServiceEvent {
            source,
            detail_type,
            detail,
            event_bus: bus,
            account_id,
            region: "us-east-1",
            resources: &[],
            principal_arn: None,
        }
    }

    fn make_shared() -> SharedEventBridgeState {
        Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ))
    }

    fn make_rule(name: &str, pattern: Option<&str>, target_arn: &str) -> EventRule {
        EventRule {
            name: name.to_string(),
            arn: Arn::new(
                "events",
                "us-east-1",
                "123456789012",
                &format!("rule/{name}"),
            )
            .to_string(),
            event_bus_name: "default".to_string(),
            event_pattern: pattern.map(|s| s.to_string()),
            schedule_expression: None,
            state: "ENABLED".to_string(),
            description: None,
            role_arn: None,
            managed_by: None,
            created_by: None,
            targets: vec![EbTarget {
                id: "t1".to_string(),
                arn: target_arn.to_string(),
                input: None,
                input_path: None,
                input_transformer: None,
                sqs_parameters: None,
                ..Default::default()
            }],
            tags: BTreeMap::new(),
            last_fired: None,
        }
    }

    #[test]
    fn put_event_appends_to_events_log() {
        let state = make_shared();
        let bus = Arc::new(DeliveryBus::new());
        let delivery = EventBridgeDeliveryImpl::new(state.clone(), bus);
        delivery
            .put_event(&ev("my.source", "MyType", r#"{"k":"v"}"#, "default"))
            .unwrap();
        let guard = state.read();
        let default = guard.default_ref();
        assert_eq!(default.events.len(), 1);
        assert_eq!(default.events[0].source, "my.source");
        assert_eq!(default.events[0].detail_type, "MyType");
    }

    #[test]
    fn put_event_dispatches_matching_sqs_target() {
        let state = make_shared();
        let q_arn = "arn:aws:sqs:us-east-1:123456789012:q".to_string();
        {
            let mut s_accounts = state.write();
            let s = s_accounts.default_mut();
            let rule = make_rule("r", None, &q_arn);
            s.rules
                .insert(("default".to_string(), "r".to_string()), rule);
        }
        let recorder = Arc::new(Recorder::default());
        let bus = Arc::new(DeliveryBus::new().with_sqs(recorder.clone()));
        let delivery = EventBridgeDeliveryImpl::new(state, bus);
        delivery
            .put_event(&ev("app", "Changed", r#"{"x":1}"#, "default"))
            .unwrap();
        let calls = recorder.sqs.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, q_arn);
        let env: serde_json::Value = serde_json::from_str(&calls[0].1).unwrap();
        assert_eq!(env["detail-type"], "Changed");
        assert_eq!(env["source"], "app");
    }

    #[test]
    fn put_event_dispatches_to_sns_target() {
        let state = make_shared();
        let topic_arn = "arn:aws:sns:us-east-1:123456789012:t".to_string();
        {
            let mut s_accounts = state.write();
            let s = s_accounts.default_mut();
            let rule = make_rule("r", None, &topic_arn);
            s.rules
                .insert(("default".to_string(), "r".to_string()), rule);
        }
        let recorder = Arc::new(Recorder::default());
        let bus = Arc::new(DeliveryBus::new().with_sns(recorder.clone()));
        let delivery = EventBridgeDeliveryImpl::new(state, bus);
        delivery
            .put_event(&ev("app", "Changed", r#"{}"#, "default"))
            .unwrap();
        let calls = recorder.sns.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, topic_arn);
        assert_eq!(calls[0].2.as_deref(), Some("Changed"));
    }

    /// A metric put through the delivery bus: name, value and dimensions.
    type Metric = (String, f64, BTreeMap<String, String>);

    /// Records the metrics put through the delivery bus.
    #[derive(Default)]
    struct MetricSink(Mutex<Vec<Metric>>);
    impl fakecloud_core::delivery::CloudwatchDelivery for MetricSink {
        fn put_metric(
            &self,
            _account_id: &str,
            _region: &str,
            namespace: &str,
            metric_name: &str,
            value: f64,
            _unit: Option<&str>,
            dimensions: BTreeMap<String, String>,
            _timestamp_ms: i64,
        ) {
            assert_eq!(namespace, "AWS/Events");
            self.0
                .lock()
                .unwrap()
                .push((metric_name.to_string(), value, dimensions));
        }
    }

    /// Every target a rule invokes counts once in the rule's `Invocations`,
    /// not only SQS targets (AWS counts successful and failed invocations of
    /// any target): an SNS, a Step Functions and a Kinesis target each count
    /// one, dimensioned by the rule.
    #[test]
    fn every_target_type_counts_in_invocations() {
        for target_arn in [
            "arn:aws:sns:us-east-1:123456789012:t",
            "arn:aws:states:us-east-1:123456789012:stateMachine:m",
            "arn:aws:kinesis:us-east-1:123456789012:stream/s",
        ] {
            let state = make_shared();
            {
                let mut s_accounts = state.write();
                let s = s_accounts.default_mut();
                s.rules.insert(
                    ("default".to_string(), "r".to_string()),
                    make_rule("r", None, target_arn),
                );
            }
            let sink = Arc::new(MetricSink::default());
            let bus = Arc::new(DeliveryBus::new().with_cloudwatch_metrics(sink.clone()));
            EventBridgeDeliveryImpl::new(state, bus)
                .put_event(&ev("app", "Changed", r#"{}"#, "default"))
                .unwrap();
            let metrics = sink.0.lock().unwrap();
            assert_eq!(
                *metrics,
                vec![(
                    "Invocations".to_string(),
                    1.0,
                    BTreeMap::from([("RuleName".to_string(), "r".to_string())])
                )],
                "{target_arn}"
            );
        }
    }

    #[test]
    fn put_event_skips_disabled_rule() {
        let state = make_shared();
        let q_arn = "arn:aws:sqs:us-east-1:123456789012:q".to_string();
        {
            let mut s_accounts = state.write();
            let s = s_accounts.default_mut();
            let mut rule = make_rule("r", None, &q_arn);
            rule.state = "DISABLED".to_string();
            s.rules
                .insert(("default".to_string(), "r".to_string()), rule);
        }
        let recorder = Arc::new(Recorder::default());
        let bus = Arc::new(DeliveryBus::new().with_sqs(recorder.clone()));
        let delivery = EventBridgeDeliveryImpl::new(state, bus);
        delivery
            .put_event(&ev("app", "Changed", r#"{}"#, "default"))
            .unwrap();
        assert!(recorder.sqs.lock().unwrap().is_empty());
    }

    #[test]
    fn put_event_skips_other_bus_rule() {
        let state = make_shared();
        let q_arn = "arn:aws:sqs:us-east-1:123456789012:q".to_string();
        {
            let mut s_accounts = state.write();
            let s = s_accounts.default_mut();
            let mut rule = make_rule("r", None, &q_arn);
            rule.event_bus_name = "custom-bus".to_string();
            s.rules
                .insert(("custom-bus".to_string(), "r".to_string()), rule);
        }
        let recorder = Arc::new(Recorder::default());
        let bus = Arc::new(DeliveryBus::new().with_sqs(recorder.clone()));
        let delivery = EventBridgeDeliveryImpl::new(state, bus);
        delivery
            .put_event(&ev("app", "Changed", r#"{}"#, "default"))
            .unwrap();
        assert!(recorder.sqs.lock().unwrap().is_empty());
    }

    #[test]
    fn put_event_handles_invalid_detail_json_gracefully() {
        let state = make_shared();
        let q_arn = "arn:aws:sqs:us-east-1:123456789012:q".to_string();
        {
            let mut s_accounts = state.write();
            let s = s_accounts.default_mut();
            let rule = make_rule("r", None, &q_arn);
            s.rules
                .insert(("default".to_string(), "r".to_string()), rule);
        }
        let recorder = Arc::new(Recorder::default());
        let bus = Arc::new(DeliveryBus::new().with_sqs(recorder.clone()));
        let delivery = EventBridgeDeliveryImpl::new(state, bus);
        delivery
            .put_event(&ev("app", "Type", "not-json", "default"))
            .unwrap();
        let calls = recorder.sqs.lock().unwrap();
        assert_eq!(calls.len(), 1);
        let env: serde_json::Value = serde_json::from_str(&calls[0].1).unwrap();
        assert_eq!(env["detail"], serde_json::json!({}));
    }

    #[test]
    fn put_event_to_account_writes_to_target_account_bus() {
        let state = make_shared();
        let bus = Arc::new(DeliveryBus::new());
        let delivery = EventBridgeDeliveryImpl::new(state.clone(), bus);
        delivery
            .put_event(&ev_in(
                "scheduler",
                "Fired",
                r#"{}"#,
                "default",
                "999988887777",
            ))
            .unwrap();

        let guard = state.read();
        let target = guard
            .get("999988887777")
            .expect("target account should be created on demand");
        assert_eq!(target.events.len(), 1);
        assert_eq!(target.events[0].source, "scheduler");
        // The default account's bus should be untouched.
        assert!(guard.default_ref().events.is_empty());
    }

    #[test]
    fn put_event_to_account_dispatches_to_rules_in_target_account() {
        let state = make_shared();
        let q_arn = "arn:aws:sqs:us-east-1:999988887777:cross-q".to_string();
        {
            let mut s_accounts = state.write();
            let s = s_accounts.get_or_create("999988887777");
            let rule = make_rule("xacct-rule", None, &q_arn);
            s.rules
                .insert(("default".to_string(), "xacct-rule".to_string()), rule);
        }
        let recorder = Arc::new(Recorder::default());
        let bus = Arc::new(DeliveryBus::new().with_sqs(recorder.clone()));
        let delivery = EventBridgeDeliveryImpl::new(state, bus);
        delivery
            .put_event(&ev_in(
                "scheduler",
                "Cross",
                r#"{"hi":1}"#,
                "default",
                "999988887777",
            ))
            .unwrap();
        let calls = recorder.sqs.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, q_arn);
    }

    /// A service event is stamped with, and rule patterns match, the
    /// originating account and region rather than the server's startup
    /// account/region.
    #[test]
    fn put_event_stamps_origin_account_and_region() {
        let state = make_shared();
        let q_arn = "arn:aws:sqs:eu-west-2:111111111111:q".to_string();
        {
            let mut accounts = state.write();
            let s = accounts.get_or_create("111111111111");
            let rule = make_rule(
                "regional",
                Some(r#"{"region":["eu-west-2"],"account":["111111111111"]}"#),
                &q_arn,
            );
            s.rules
                .insert(("default".to_string(), "regional".to_string()), rule);
        }
        let recorder = Arc::new(Recorder::default());
        let bus = Arc::new(DeliveryBus::new().with_sqs(recorder.clone()));
        let delivery = EventBridgeDeliveryImpl::new(state.clone(), bus);
        let resources = vec!["arn:aws:s3:::b".to_string()];
        delivery
            .put_event(&CrossServiceEvent {
                source: "aws.s3",
                detail_type: "Object Created",
                detail: "{}",
                event_bus: "default",
                account_id: "111111111111",
                region: "eu-west-2",
                resources: &resources,
                principal_arn: None,
            })
            .unwrap();
        let calls = recorder.sqs.lock().unwrap();
        assert_eq!(calls.len(), 1, "region/account pattern must match");
        let env: serde_json::Value = serde_json::from_str(&calls[0].1).unwrap();
        assert_eq!(env["account"], "111111111111");
        assert_eq!(env["region"], "eu-west-2");
        assert_eq!(env["resources"], serde_json::json!(["arn:aws:s3:::b"]));
        let accounts = state.read();
        assert!(accounts.default_ref().events.is_empty());
        assert_eq!(accounts.get("111111111111").unwrap().events.len(), 1);
    }

    /// Create bus `custom` in 999988887777 with `policy` and a rule on it
    /// targeting an SQS queue; returns the SQS recorder's delivery.
    fn cross_account_bus(
        policy: Option<serde_json::Value>,
    ) -> (
        SharedEventBridgeState,
        Arc<Recorder>,
        EventBridgeDeliveryImpl,
    ) {
        let state = make_shared();
        let q_arn = "arn:aws:sqs:us-east-1:999988887777:q".to_string();
        {
            let mut accounts = state.write();
            let s = accounts.get_or_create("999988887777");
            s.buses.insert(
                "custom".to_string(),
                crate::state::EventBus {
                    name: "custom".to_string(),
                    arn: "arn:aws:events:us-east-1:999988887777:event-bus/custom".to_string(),
                    tags: BTreeMap::new(),
                    policy,
                    description: None,
                    kms_key_identifier: None,
                    dead_letter_config: None,
                    creation_time: Utc::now(),
                    last_modified_time: Utc::now(),
                },
            );
            let mut rule = make_rule("on-custom", None, &q_arn);
            rule.event_bus_name = "custom".to_string();
            s.rules
                .insert(("custom".to_string(), "on-custom".to_string()), rule);
        }
        let recorder = Arc::new(Recorder::default());
        let bus = Arc::new(DeliveryBus::new().with_sqs(recorder.clone()));
        let delivery = EventBridgeDeliveryImpl::new(state.clone(), bus);
        (state, recorder, delivery)
    }

    fn put_events_policy(principal: &str) -> serde_json::Value {
        serde_json::json!({
            "Version": "2012-10-17",
            "Statement": [{
                "Sid": "allow",
                "Effect": "Allow",
                "Principal": {"AWS": principal},
                "Action": "events:PutEvents",
                "Resource": "arn:aws:events:us-east-1:999988887777:event-bus/custom"
            }]
        })
    }

    const CUSTOM_BUS_ARN: &str = "arn:aws:events:us-east-1:999988887777:event-bus/custom";

    /// An event-bus ARN routes the event to the bus owner's account (when its
    /// policy allows the source account) while the event keeps its
    /// originating account.
    #[test]
    fn put_event_bus_arn_routes_to_bus_owner_account() {
        let (state, recorder, delivery) =
            cross_account_bus(Some(put_events_policy("arn:aws:iam::111111111111:root")));
        delivery
            .put_event(&ev_in("app", "T", "{}", CUSTOM_BUS_ARN, "111111111111"))
            .expect("policy allows the source account");
        let calls = recorder.sqs.lock().unwrap();
        assert_eq!(calls.len(), 1);
        let env: serde_json::Value = serde_json::from_str(&calls[0].1).unwrap();
        assert_eq!(env["account"], "111111111111");
        let accounts = state.read();
        let owner = accounts.get("999988887777").unwrap();
        assert_eq!(owner.events.len(), 1);
        assert_eq!(owner.events[0].event_bus_name, "custom");
        assert!(accounts.get("111111111111").is_none());
    }

    /// A bus ARN in another account and another region is authorized against
    /// that bus's own ARN (its region), not the source's region: a policy
    /// scoped to the eu-west-1 bus ARN allows a us-east-1 source.
    #[test]
    fn put_event_cross_account_bus_in_other_region_uses_bus_region() {
        let bus_arn = "arn:aws:events:eu-west-1:999988887777:event-bus/custom";
        let mut policy = put_events_policy("arn:aws:iam::111111111111:root");
        policy["Statement"][0]["Resource"] = serde_json::json!(bus_arn);
        let (state, recorder, delivery) = cross_account_bus(Some(policy));
        let event_id = delivery
            .put_event(&ev_in("app", "T", "{}", bus_arn, "111111111111"))
            .expect("policy allows the source on the eu-west-1 bus");
        assert!(!event_id.is_empty());
        let calls = recorder.sqs.lock().unwrap();
        assert_eq!(calls.len(), 1);
        let env: serde_json::Value = serde_json::from_str(&calls[0].1).unwrap();
        // The event keeps its originating region.
        assert_eq!(env["region"], "us-east-1");
        assert_eq!(state.read().get("999988887777").unwrap().events.len(), 1);
    }

    /// A bus in another account with no resource policy refuses the event:
    /// nothing is stored or delivered and the source gets AccessDenied.
    #[test]
    fn put_event_cross_account_bus_without_policy_is_denied() {
        let (state, recorder, delivery) = cross_account_bus(None);
        let err = delivery
            .put_event(&ev_in("app", "T", "{}", CUSTOM_BUS_ARN, "111111111111"))
            .unwrap_err();
        assert!(matches!(err, EventBridgeDeliveryError::AccessDenied(_)));
        assert!(recorder.sqs.lock().unwrap().is_empty());
        assert!(state.read().get("999988887777").unwrap().events.is_empty());
    }

    /// The policy is evaluated against the source's role: a policy naming a
    /// different principal refuses it, one naming the role allows it.
    #[test]
    fn put_event_cross_account_policy_is_evaluated_against_source_role() {
        let role = "arn:aws:iam::111111111111:role/scheduler-role";
        let (_, _, delivery) = cross_account_bus(Some(put_events_policy(
            "arn:aws:iam::111111111111:role/other",
        )));
        let mut event = ev_in("app", "T", "{}", CUSTOM_BUS_ARN, "111111111111");
        event.principal_arn = Some(role);
        assert!(matches!(
            delivery.put_event(&event),
            Err(EventBridgeDeliveryError::AccessDenied(_))
        ));

        let (state, recorder, delivery) = cross_account_bus(Some(put_events_policy(role)));
        delivery.put_event(&event).expect("policy names the role");
        assert_eq!(recorder.sqs.lock().unwrap().len(), 1);
        assert_eq!(state.read().get("999988887777").unwrap().events.len(), 1);
    }

    /// Archives on the bus capture service events like PutEvents entries.
    #[test]
    fn put_event_is_archived() {
        let state = make_shared();
        {
            let mut accounts = state.write();
            let s = accounts.default_mut();
            s.archives.insert(
                "arch".to_string(),
                crate::state::Archive {
                    name: "arch".to_string(),
                    arn: "arn:aws:events:us-east-1:123456789012:archive/arch".to_string(),
                    event_source_arn: "arn:aws:events:us-east-1:123456789012:event-bus/default"
                        .to_string(),
                    description: None,
                    event_pattern: Some(r#"{"source":["aws.s3"]}"#.to_string()),
                    retention_days: 0,
                    state: "ENABLED".to_string(),
                    creation_time: Utc::now(),
                    event_count: 0,
                    size_bytes: 0,
                    events: Vec::new(),
                },
            );
        }
        let delivery = EventBridgeDeliveryImpl::new(state.clone(), Arc::new(DeliveryBus::new()));
        delivery
            .put_event(&ev("aws.s3", "Object Created", "{}", "default"))
            .unwrap();
        delivery
            .put_event(&ev("other", "T", "{}", "default"))
            .unwrap();
        let accounts = state.read();
        let archive = &accounts.default_ref().archives["arch"];
        assert_eq!(archive.event_count, 1);
        assert_eq!(archive.events[0].source, "aws.s3");
    }

    fn insert_rule(state: &SharedEventBridgeState, rule: EventRule) {
        let mut accounts = state.write();
        accounts
            .default_mut()
            .rules
            .insert(("default".to_string(), rule.name.clone()), rule);
    }

    /// Regression for #2628: an event another service (S3) publishes through
    /// the cross-service delivery must reach a Lambda target exactly like
    /// PutEvents -- recorded in Lambda's invocation log (what
    /// `/_fakecloud/lambda/invocations` serves) rather than dropped because
    /// the delivery impl had no Lambda wiring.
    #[test]
    fn put_event_with_target_wiring_records_lambda_invocation() {
        let state = make_shared();
        let fn_arn = "arn:aws:lambda:us-east-1:123456789012:function:my-fn";
        insert_rule(
            &state,
            make_rule(
                "s3-to-fn",
                Some(r#"{"source":["aws.s3"],"detail-type":["Object Created"]}"#),
                fn_arn,
            ),
        );
        let lambda_state: SharedLambdaState = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let delivery = EventBridgeDeliveryImpl::new(state.clone(), Arc::new(DeliveryBus::new()))
            .with_target_wiring(EventTargetWiring {
                lambda_state: Some(lambda_state.clone()),
                ..Default::default()
            });

        delivery
            .put_event(&ev(
                "aws.s3",
                "Object Created",
                r#"{"bucket":{"name":"eb-bucket"},"object":{"key":"anything"}}"#,
                "default",
            ))
            .unwrap();

        let accounts = lambda_state.read();
        let invocations = &accounts
            .regional("123456789012", "us-east-1")
            .unwrap()
            .invocations;
        assert_eq!(invocations.len(), 1);
        assert_eq!(invocations[0].function_arn, fn_arn);
        assert_eq!(invocations[0].source, "aws:events");
        let payload: serde_json::Value = serde_json::from_str(&invocations[0].payload).unwrap();
        assert_eq!(payload["source"], "aws.s3");
        assert_eq!(payload["detail-type"], "Object Created");
        assert_eq!(payload["detail"]["bucket"]["name"], "eb-bucket");
        // EventBridge's own delivery record is kept too.
        assert_eq!(state.read().default_ref().lambda_invocations.len(), 1);
    }

    /// A Lambda target ARN naming another known account is recorded against
    /// that account, never the default one (which may own a same-named
    /// function).
    #[test]
    fn put_event_records_lambda_invocation_in_arn_account() {
        let state = make_shared();
        let fn_arn = "arn:aws:lambda:us-east-1:999988887777:function:foo";
        insert_rule(&state, make_rule("xacct", None, fn_arn));
        let lambda_state: SharedLambdaState = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        lambda_state
            .write()
            .regional_mut("999988887777", "us-east-1");
        let delivery = EventBridgeDeliveryImpl::new(state, Arc::new(DeliveryBus::new()))
            .with_target_wiring(EventTargetWiring {
                lambda_state: Some(lambda_state.clone()),
                ..Default::default()
            });

        delivery
            .put_event(&ev("app", "T", "{}", "default"))
            .unwrap();

        let accounts = lambda_state.read();
        assert!(accounts.regional("123456789012", "us-east-1").is_none());
        let target = accounts
            .regional("999988887777", "us-east-1")
            .expect("target account");
        assert_eq!(target.invocations.len(), 1);
        assert_eq!(target.invocations[0].function_arn, fn_arn);
    }

    /// An ARN naming an account fakecloud has never seen does not conjure a
    /// Lambda account for it; the record stays with the bus's account.
    #[test]
    fn put_event_does_not_create_lambda_account_for_unknown_arn_account() {
        let state = make_shared();
        let fn_arn = "arn:aws:lambda:us-east-1:555555555555:function:foo";
        insert_rule(&state, make_rule("unknown", None, fn_arn));
        let lambda_state: SharedLambdaState = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let delivery = EventBridgeDeliveryImpl::new(state, Arc::new(DeliveryBus::new()))
            .with_target_wiring(EventTargetWiring {
                lambda_state: Some(lambda_state.clone()),
                ..Default::default()
            });

        delivery
            .put_event(&ev("app", "T", "{}", "default"))
            .unwrap();

        let accounts = lambda_state.read();
        assert!(accounts.get("555555555555").is_none());
        assert_eq!(
            accounts
                .regional("123456789012", "us-east-1")
                .unwrap()
                .invocations
                .len(),
            1
        );
    }

    /// Lambda backend double: records which function each launch was for and
    /// runs an in-process runtime that records each event's payload and
    /// answers `{}`.
    struct RecordingBackend {
        bodies: Arc<Mutex<Vec<String>>>,
        launched: std::sync::Mutex<Vec<String>>,
    }

    #[async_trait::async_trait]
    impl fakecloud_lambda::runtime::LambdaBackend for RecordingBackend {
        fn name(&self) -> &str {
            "recording"
        }
        async fn launch(
            &self,
            func: &fakecloud_lambda::LambdaFunction,
            _code_zip: Option<&[u8]>,
            _layers: &[Vec<u8>],
            _deploy_id: &str,
            _credentials: Option<&fakecloud_core::auth::SessionCredentials>,
            runtime_api: &fakecloud_lambda::runtime::RuntimeApiAddress,
        ) -> Result<fakecloud_lambda::runtime::WarmInstance, fakecloud_lambda::runtime::RuntimeError>
        {
            use fakecloud_lambda::runtime::api::{spawn_in_process_runtime, InProcessReply};
            self.launched
                .lock()
                .unwrap()
                .push(func.function_arn.clone());
            let bodies = self.bodies.clone();
            spawn_in_process_runtime(runtime_api.sandbox.clone(), move |event| {
                bodies
                    .lock()
                    .unwrap()
                    .push(String::from_utf8_lossy(&event.payload).to_string());
                std::future::ready(InProcessReply::Response(b"{}".to_vec()))
            });
            let (_, exit) = tokio::sync::watch::channel(None);
            Ok(fakecloud_lambda::runtime::WarmInstance {
                handle: fakecloud_lambda::runtime::BackendHandle::Container {
                    id: "c0".to_string(),
                },
                exit,
                launcher: false,
            })
        }
        async fn terminate(&self, _handle: &fakecloud_lambda::runtime::BackendHandle) {}
    }

    fn lambda_function(arn: &str) -> fakecloud_lambda::LambdaFunction {
        fakecloud_lambda::LambdaFunction {
            function_name: crate::service::helpers::function_name_from_arn(arn).to_string(),
            function_arn: arn.to_string(),
            runtime: "python3.12".to_string(),
            handler: "index.handler".to_string(),
            timeout: 5,
            package_type: "Zip".to_string(),
            code_zip: Some(vec![1, 2, 3]),
            ..Default::default()
        }
    }

    /// #2628's real symptom was that the function never *ran* (no container,
    /// no Pod), not just a missing record. A cross-service event matched to a
    /// Lambda target must reach the container runtime: the function the ARN
    /// names (in the ARN's account, not a same-named default-account one) is
    /// launched and receives the EventBridge envelope as its payload.
    #[tokio::test]
    async fn put_event_executes_lambda_target_in_container_runtime() {
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let backend = Arc::new(RecordingBackend {
            bodies: bodies.clone(),
            launched: std::sync::Mutex::new(Vec::new()),
        });
        let runtime = Arc::new(ContainerRuntime::from_backend(backend.clone()));

        let fn_arn = "arn:aws:lambda:us-east-1:999988887777:function:my-fn";
        let default_fn_arn = "arn:aws:lambda:us-east-1:123456789012:function:my-fn";
        let lambda_state: SharedLambdaState = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        {
            let mut accounts = lambda_state.write();
            accounts
                .regional_mut("123456789012", "us-east-1")
                .functions
                .insert("my-fn".to_string(), lambda_function(default_fn_arn));
            accounts
                .regional_mut("999988887777", "us-east-1")
                .functions
                .insert("my-fn".to_string(), lambda_function(fn_arn));
        }

        let state = make_shared();
        insert_rule(
            &state,
            make_rule("s3-to-fn", Some(r#"{"source":["aws.s3"]}"#), fn_arn),
        );
        let delivery = EventBridgeDeliveryImpl::new(state, Arc::new(DeliveryBus::new()))
            .with_target_wiring(EventTargetWiring {
                lambda_state: Some(lambda_state),
                container_runtime: Some(runtime),
                ..Default::default()
            });

        delivery
            .put_event(&ev(
                "aws.s3",
                "Object Created",
                r#"{"bucket":{"name":"eb-bucket"},"object":{"key":"anything"}}"#,
                "default",
            ))
            .unwrap();

        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
        while bodies.lock().unwrap().is_empty() {
            assert!(
                tokio::time::Instant::now() < deadline,
                "Lambda target was never executed by the container runtime"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }

        assert_eq!(*backend.launched.lock().unwrap(), vec![fn_arn.to_string()]);
        let bodies = bodies.lock().unwrap();
        assert_eq!(bodies.len(), 1);
        let payload: serde_json::Value = serde_json::from_str(&bodies[0]).unwrap();
        assert_eq!(payload["source"], "aws.s3");
        assert_eq!(payload["detail-type"], "Object Created");
        assert_eq!(payload["detail"]["object"]["key"], "anything");
    }

    #[test]
    fn lambda_arn_account_only_for_full_arns() {
        use crate::service::helpers::lambda_arn_account;
        assert_eq!(
            lambda_arn_account("arn:aws:lambda:us-east-1:999988887777:function:foo:live"),
            Some("999988887777")
        );
        assert_eq!(
            lambda_arn_account("arn:aws:lambda:us-east-1:999988887777:function:foo"),
            Some("999988887777")
        );
        assert_eq!(
            lambda_arn_account("arn:aws-cn:lambda:cn-north-1:999988887777:function:foo"),
            Some("999988887777")
        );
        // Bare names and anything that isn't a Lambda *function* ARN.
        assert_eq!(lambda_arn_account("foo"), None);
        assert_eq!(lambda_arn_account(""), None);
        assert_eq!(
            lambda_arn_account("arn:aws:sqs:us-east-1:999988887777:function:foo"),
            None,
            "wrong service"
        );
        assert_eq!(
            lambda_arn_account("arn:aws:lambda:us-east-1:999988887777:layer:foo:1"),
            None,
            "layer, not function"
        );
        assert_eq!(
            lambda_arn_account("arn:aws:lambda:us-east-1:999988887777:function"),
            None,
            "missing function name"
        );
        assert_eq!(
            lambda_arn_account("arn:aws:lambda:us-east-1::function:foo"),
            None,
            "empty account"
        );
        assert_eq!(
            lambda_arn_account("xrn:aws:lambda:us-east-1:999988887777:function:foo"),
            None,
            "not an arn"
        );
        assert_eq!(
            lambda_arn_account("arn::lambda:us-east-1:999988887777:function:foo"),
            None,
            "empty partition"
        );
        assert_eq!(
            lambda_arn_account("arn:aws:lambda::999988887777:function:foo"),
            None,
            "empty region"
        );
    }

    /// A cross-service event matched by a rule with a CloudWatch Logs target
    /// lands in the log group, as with PutEvents.
    #[test]
    fn put_event_with_target_wiring_writes_logs_target() {
        let state = make_shared();
        let group_arn = "arn:aws:logs:us-east-1:123456789012:log-group:/aws/events/s3";
        insert_rule(
            &state,
            make_rule("s3-to-logs", Some(r#"{"source":["aws.s3"]}"#), group_arn),
        );
        let logs_state: SharedLogsState = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ));
        let delivery = EventBridgeDeliveryImpl::new(state, Arc::new(DeliveryBus::new()))
            .with_target_wiring(EventTargetWiring {
                logs_state: Some(logs_state.clone()),
                ..Default::default()
            });

        delivery
            .put_event(&ev("aws.s3", "Object Created", r#"{"k":1}"#, "default"))
            .unwrap();

        let accounts = logs_state.read();
        let group = accounts
            .default_regional()
            .expect("default region")
            .log_groups
            .get("/aws/events/s3")
            .expect("log group auto-created by the Logs target");
        let events = &group.log_streams["events"].events;
        assert_eq!(events.len(), 1);
        let payload: serde_json::Value = serde_json::from_str(&events[0].message).unwrap();
        assert_eq!(payload["source"], "aws.s3");
    }

    #[test]
    fn deferred_delivery_forwards_once_set() {
        let state = make_shared();
        let deferred = DeferredEventBridgeDelivery::new();
        // Before the real sender is bound the event is dropped, not panicked
        // on, and reported as a failed delivery so the source records it.
        assert!(matches!(
            deferred.put_event(&ev("early", "T", "{}", "default")),
            Err(EventBridgeDeliveryError::Unavailable(_))
        ));
        assert!(state.read().default_ref().events.is_empty());

        deferred.set(Arc::new(EventBridgeDeliveryImpl::new(
            state.clone(),
            Arc::new(DeliveryBus::new()),
        )));
        deferred
            .put_event(&ev("app", "T", "{}", "default"))
            .unwrap();
        deferred
            .put_event(&ev_in("app", "T", "{}", "default", "999988887777"))
            .unwrap();

        let accounts = state.read();
        assert_eq!(accounts.default_ref().events.len(), 1);
        assert_eq!(accounts.default_ref().events[0].source, "app");
        assert_eq!(accounts.get("999988887777").unwrap().events.len(), 1);
    }
}
