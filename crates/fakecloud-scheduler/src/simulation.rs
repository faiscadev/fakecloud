//! Test-only helpers that drive the Scheduler firing pipeline from
//! introspection endpoints. Lets integration tests bypass the wall
//! clock by poking a specific schedule and observing delivery.

use std::sync::Arc;

use chrono::Utc;

use fakecloud_core::delivery::DeliveryBus;

use crate::delivery::{deliver_target, route_to_dlq};
use crate::state::SharedSchedulerState;

/// Fire a specific schedule right now, regardless of its expression.
/// Applies the same post-fire handling as the ticker — `last_fired`
/// is bumped and `ActionAfterCompletion=DELETE` removes the schedule.
/// Returns the target ARN on success (so the endpoint can echo it
/// back to the caller).
pub fn fire_once(
    state: &SharedSchedulerState,
    delivery: &Arc<DeliveryBus>,
    account_id: &str,
    region: &str,
    group_name: &str,
    schedule_name: &str,
) -> Result<String, String> {
    let (sched_snapshot, should_delete) = {
        let mut accounts = state.write();
        let account_state = accounts
            .regional_get_mut(account_id, region)
            .ok_or_else(|| format!("account not found: {account_id} in {region}"))?;
        let key = (group_name.to_string(), schedule_name.to_string());
        let sched = account_state
            .schedules
            .get_mut(&key)
            .ok_or_else(|| format!("schedule not found: {group_name}/{schedule_name}"))?;
        sched.last_fired = Some(Utc::now());
        let snap = sched.clone();
        let should_delete = sched.schedule_expression.starts_with("at(")
            && sched.action_after_completion == "DELETE";
        (snap, should_delete)
    };

    match deliver_target(delivery, &sched_snapshot) {
        Ok(()) => {}
        Err(err) => {
            route_to_dlq(
                delivery,
                &sched_snapshot,
                "TargetDeliveryFailed",
                &err.to_string(),
            );
        }
    }

    if should_delete {
        let mut accounts = state.write();
        if let Some(account_state) = accounts.regional_get_mut(account_id, region) {
            account_state
                .schedules
                .remove(&(group_name.to_string(), schedule_name.to_string()));
        }
    }

    Ok(sched_snapshot.target.arn)
}

/// Snapshot of every schedule across every account, for the
/// `GET /_fakecloud/scheduler/schedules` introspection endpoint.
#[derive(Debug, Clone)]
pub struct ScheduleRow {
    pub account_id: String,
    pub region: String,
    pub group_name: String,
    pub name: String,
    pub arn: String,
    pub state: String,
    pub schedule_expression: String,
    pub target_arn: String,
    pub last_fired: Option<chrono::DateTime<chrono::Utc>>,
}

/// Ready-to-serialize response for the fire-once introspection endpoint.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FireScheduleResponse {
    #[serde(rename = "scheduleArn")]
    pub schedule_arn: String,
    #[serde(rename = "targetArn")]
    pub target_arn: String,
}

/// Fire a schedule and assemble the response body the introspection
/// endpoint echoes back. Extracted so the HTTP route closure is a
/// one-liner and the interesting logic has unit coverage.
pub fn fire_schedule_response(
    state: &SharedSchedulerState,
    delivery: &Arc<DeliveryBus>,
    region: &str,
    account_id: &str,
    group: &str,
    name: &str,
) -> Result<FireScheduleResponse, String> {
    let target_arn = fire_once(state, delivery, account_id, region, group, name)?;
    Ok(FireScheduleResponse {
        schedule_arn: crate::state::schedule_arn(region, account_id, group, name),
        target_arn,
    })
}

pub fn list_all_schedules(state: &SharedSchedulerState) -> Vec<ScheduleRow> {
    let accounts = state.read();
    let mut rows: Vec<ScheduleRow> = accounts
        .iter_regional()
        .flat_map(|(account_id, region, s)| {
            let account_id = account_id.to_string();
            let region = region.to_string();
            s.schedules.values().map(move |sched| ScheduleRow {
                account_id: account_id.clone(),
                region: region.clone(),
                group_name: sched.group_name.clone(),
                name: sched.name.clone(),
                arn: sched.arn.clone(),
                state: sched.state.clone(),
                schedule_expression: sched.schedule_expression.clone(),
                target_arn: sched.target.arn.clone(),
                last_fired: sched.last_fired,
            })
        })
        .collect();
    rows.sort_by(|a, b| {
        a.account_id
            .cmp(&b.account_id)
            .then_with(|| a.group_name.cmp(&b.group_name))
            .then_with(|| a.name.cmp(&b.name))
    });
    rows
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{FlexibleTimeWindow, Schedule, SharedSchedulerState, Target};
    use fakecloud_aws::arn::Arn;
    use fakecloud_core::delivery::{SqsDelivery, SqsDeliveryError, SqsMessageAttribute};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Mutex;

    fn make_state() -> SharedSchedulerState {
        Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiRegionState::new("000000000000", "us-east-1", ""),
        ))
    }

    fn seed(state: &SharedSchedulerState, name: &str, expr: &str, action: &str) {
        let now = Utc::now();
        let mut accounts = state.write();
        let s = accounts.regional_mut("000000000000", "us-east-1");
        s.schedules.insert(
            ("default".to_string(), name.to_string()),
            Schedule {
                arn: Arn::new(
                    "scheduler",
                    "us-east-1",
                    "000000000000",
                    &format!("schedule/default/{name}"),
                )
                .to_string(),
                name: name.to_string(),
                group_name: "default".to_string(),
                schedule_expression: expr.to_string(),
                schedule_expression_timezone: None,
                start_date: None,
                end_date: None,
                description: None,
                state: "ENABLED".to_string(),
                kms_key_arn: None,
                action_after_completion: action.to_string(),
                flexible_time_window: FlexibleTimeWindow::default(),
                target: Target {
                    arn: "arn:aws:sqs:us-east-1:000000000000:q".to_string(),
                    role_arn: "arn:aws:iam::000000000000:role/s".to_string(),
                    input: Some("{\"x\":1}".to_string()),
                    dead_letter_config: None,
                    retry_policy: None,
                    sqs_parameters: None,
                    ecs_parameters: None,
                    eventbridge_parameters: None,
                    kinesis_parameters: None,
                    sagemaker_pipeline_parameters: None,
                },
                creation_date: now,
                last_modification_date: now,
                last_fired: None,
            },
        );
    }

    #[derive(Default)]
    struct Recorder {
        calls: Mutex<Vec<(String, String)>>,
    }
    impl SqsDelivery for Recorder {
        fn deliver_to_queue(
            &self,
            _sender: fakecloud_core::delivery::DeliverySender<'_>,
            _a: &str,
            _b: &str,
            _c: &HashMap<String, String>,
        ) {
        }

        fn try_deliver_to_queue_with_attrs(
            &self,
            _sender: fakecloud_core::delivery::DeliverySender<'_>,
            arn: &str,
            body: &str,
            _a: &HashMap<String, SqsMessageAttribute>,
            _g: Option<&str>,
            _d: Option<&str>,
        ) -> Result<(), SqsDeliveryError> {
            self.calls
                .lock()
                .unwrap()
                .push((arn.to_string(), body.to_string()));
            Ok(())
        }
    }

    #[test]
    fn fire_once_delivers_and_updates_last_fired() {
        let state = make_state();
        seed(&state, "s", "rate(1 day)", "NONE");
        let rec = Arc::new(Recorder::default());
        let bus = Arc::new(DeliveryBus::new().with_sqs(rec.clone()));
        fire_once(&state, &bus, "000000000000", "us-east-1", "default", "s").unwrap();
        assert_eq!(rec.calls.lock().unwrap().len(), 1);
        let accounts = state.read();
        let sched = accounts
            .regional("000000000000", "us-east-1")
            .unwrap()
            .schedules
            .get(&("default".to_string(), "s".to_string()))
            .unwrap();
        assert!(sched.last_fired.is_some());
    }

    #[test]
    fn fire_once_deletes_at_schedule_with_delete_action() {
        let state = make_state();
        seed(&state, "once", "at(2020-01-01T00:00:00)", "DELETE");
        let rec = Arc::new(Recorder::default());
        let bus = Arc::new(DeliveryBus::new().with_sqs(rec.clone()));
        fire_once(&state, &bus, "000000000000", "us-east-1", "default", "once").unwrap();
        let accounts = state.read();
        assert!(!accounts
            .regional("000000000000", "us-east-1")
            .unwrap()
            .schedules
            .contains_key(&("default".to_string(), "once".to_string())));
    }

    #[test]
    fn fire_once_reports_missing_schedule() {
        let state = make_state();
        let bus = Arc::new(DeliveryBus::new());
        let err = fire_once(&state, &bus, "000000000000", "us-east-1", "default", "nope")
            .err()
            .unwrap();
        assert!(err.contains("not found"));
    }

    #[test]
    fn fire_schedule_response_builds_arn_and_target() {
        let state = make_state();
        seed(&state, "s", "rate(1 day)", "NONE");
        let rec = Arc::new(Recorder::default());
        let bus = Arc::new(DeliveryBus::new().with_sqs(rec.clone()));
        let resp =
            fire_schedule_response(&state, &bus, "us-east-1", "000000000000", "default", "s")
                .unwrap();
        assert_eq!(
            resp.schedule_arn,
            "arn:aws:scheduler:us-east-1:000000000000:schedule/default/s"
        );
        assert_eq!(resp.target_arn, "arn:aws:sqs:us-east-1:000000000000:q");
    }

    #[test]
    fn fire_schedule_response_propagates_missing_error() {
        let state = make_state();
        let bus = Arc::new(DeliveryBus::new());
        let err = fire_schedule_response(&state, &bus, "us-east-1", "000000000000", "g", "none")
            .err()
            .unwrap();
        assert!(err.contains("not found"));
    }

    #[test]
    fn list_all_schedules_returns_sorted_rows() {
        let state = make_state();
        seed(&state, "b", "rate(1 day)", "NONE");
        seed(&state, "a", "rate(1 day)", "NONE");
        let rows = list_all_schedules(&state);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].name, "a");
        assert_eq!(rows[1].name, "b");
    }
}
