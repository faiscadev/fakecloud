//! End-to-end tests for EventBridge Scheduler (`scheduler.amazonaws.com`).

mod helpers;

use std::time::Duration;

use aws_sdk_scheduler::types::{
    ActionAfterCompletion, DeadLetterConfig, FlexibleTimeWindow, FlexibleTimeWindowMode,
    RetryPolicy, ScheduleState, Target,
};
use aws_sdk_sqs::types::QueueAttributeName;
use fakecloud_testkit::TestServer;

async fn wait_for_message(
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
    timeout: Duration,
) -> Option<aws_sdk_sqs::types::Message> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        let resp = sqs
            .receive_message()
            .queue_url(queue_url)
            .wait_time_seconds(1)
            .max_number_of_messages(1)
            .message_attribute_names("All")
            .send()
            .await
            .unwrap();
        if let Some(msgs) = resp.messages {
            if let Some(m) = msgs.into_iter().next() {
                return Some(m);
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    None
}

async fn queue_url(sqs: &aws_sdk_sqs::Client, name: &str) -> String {
    sqs.create_queue()
        .queue_name(name)
        .send()
        .await
        .unwrap()
        .queue_url
        .unwrap()
}

async fn queue_arn(sqs: &aws_sdk_sqs::Client, url: &str) -> String {
    sqs.get_queue_attributes()
        .queue_url(url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap()
        .attributes
        .unwrap()
        .get(&QueueAttributeName::QueueArn)
        .unwrap()
        .clone()
}

fn sqs_target() -> Target {
    Target::builder()
        .arn("arn:aws:sqs:us-east-1:000000000000:scheduler-dest")
        .role_arn("arn:aws:iam::000000000000:role/scheduler")
        .input("{\"hello\":\"world\"}")
        .build()
        .unwrap()
}

fn off_window() -> FlexibleTimeWindow {
    FlexibleTimeWindow::builder()
        .mode(FlexibleTimeWindowMode::Off)
        .build()
        .unwrap()
}

#[tokio::test]
async fn scheduler_create_get_delete_schedule() {
    let server = TestServer::start().await;
    let client = server.scheduler_client().await;

    let created = client
        .create_schedule()
        .name("crud-s1")
        .schedule_expression("rate(5 minutes)")
        .flexible_time_window(off_window())
        .target(sqs_target())
        .send()
        .await
        .expect("create_schedule");
    assert!(created.schedule_arn().contains("schedule/default/crud-s1"));

    let got = client
        .get_schedule()
        .name("crud-s1")
        .send()
        .await
        .expect("get_schedule");
    assert_eq!(got.name().unwrap(), "crud-s1");
    assert_eq!(got.group_name().unwrap(), "default");
    assert_eq!(got.schedule_expression().unwrap(), "rate(5 minutes)");
    assert_eq!(got.state().unwrap(), &ScheduleState::Enabled);
    let target = got.target().unwrap();
    assert_eq!(target.input().unwrap(), "{\"hello\":\"world\"}");

    client
        .delete_schedule()
        .name("crud-s1")
        .send()
        .await
        .expect("delete_schedule");

    let err = client
        .get_schedule()
        .name("crud-s1")
        .send()
        .await
        .expect_err("schedule should be gone");
    assert!(format!("{err:?}").contains("ResourceNotFound"));
}

#[tokio::test]
async fn scheduler_target_reports_default_retry_policy() {
    // A target created without a RetryPolicy is reported with AWS's defaults:
    // MaximumEventAgeInSeconds=86400, MaximumRetryAttempts=185. The Terraform
    // resource reads these unconditionally, so they must always be present.
    let server = TestServer::start().await;
    let client = server.scheduler_client().await;

    client
        .create_schedule()
        .name("retry-default")
        .schedule_expression("rate(1 hour)")
        .flexible_time_window(off_window())
        .target(sqs_target())
        .send()
        .await
        .expect("create_schedule");

    let got = client
        .get_schedule()
        .name("retry-default")
        .send()
        .await
        .expect("get_schedule");
    let rp = got.target().unwrap().retry_policy().expect("retry_policy");
    assert_eq!(rp.maximum_event_age_in_seconds(), Some(86400));
    assert_eq!(rp.maximum_retry_attempts(), Some(185));
}

#[tokio::test]
async fn scheduler_update_is_idempotent_upsert() {
    let server = TestServer::start().await;
    let client = server.scheduler_client().await;

    client
        .create_schedule()
        .name("up-s1")
        .schedule_expression("rate(1 minute)")
        .flexible_time_window(off_window())
        .target(sqs_target())
        .send()
        .await
        .unwrap();

    let new_target = Target::builder()
        .arn("arn:aws:sqs:us-east-1:000000000000:updated-dest")
        .role_arn("arn:aws:iam::000000000000:role/scheduler")
        .input("{\"v\":2}")
        .build()
        .unwrap();

    client
        .update_schedule()
        .name("up-s1")
        .schedule_expression("rate(10 minutes)")
        .flexible_time_window(off_window())
        .target(new_target)
        .state(ScheduleState::Disabled)
        .send()
        .await
        .expect("update_schedule");

    let got = client.get_schedule().name("up-s1").send().await.unwrap();
    assert_eq!(got.schedule_expression().unwrap(), "rate(10 minutes)");
    assert_eq!(got.state().unwrap(), &ScheduleState::Disabled);
    assert_eq!(got.target().unwrap().input().unwrap(), "{\"v\":2}");
}

#[tokio::test]
async fn scheduler_at_one_shot_action_after_completion_persists() {
    // Round-trip the one-shot configuration; firing semantics land in Batch 2.
    let server = TestServer::start().await;
    let client = server.scheduler_client().await;

    client
        .create_schedule()
        .name("once")
        .schedule_expression("at(2099-01-01T12:00:00)")
        .flexible_time_window(off_window())
        .target(sqs_target())
        .action_after_completion(ActionAfterCompletion::Delete)
        .send()
        .await
        .expect("create at-schedule");

    let got = client.get_schedule().name("once").send().await.unwrap();
    assert_eq!(
        got.action_after_completion().unwrap(),
        &ActionAfterCompletion::Delete
    );
    assert_eq!(
        got.schedule_expression().unwrap(),
        "at(2099-01-01T12:00:00)"
    );
}

#[tokio::test]
async fn scheduler_list_schedules_filters() {
    let server = TestServer::start().await;
    let client = server.scheduler_client().await;

    client
        .create_schedule_group()
        .name("groupX")
        .send()
        .await
        .unwrap();

    for (name, group) in [
        ("alpha-1", "default"),
        ("alpha-2", "groupX"),
        ("beta-1", "groupX"),
    ] {
        client
            .create_schedule()
            .name(name)
            .group_name(group)
            .schedule_expression("rate(1 hour)")
            .flexible_time_window(off_window())
            .target(sqs_target())
            .send()
            .await
            .unwrap();
    }

    let resp = client
        .list_schedules()
        .group_name("groupX")
        .name_prefix("alpha")
        .send()
        .await
        .unwrap();
    let names: Vec<&str> = resp.schedules().iter().map(|s| s.name().unwrap()).collect();
    assert_eq!(names, ["alpha-2"]);
}

#[tokio::test]
async fn scheduler_group_lifecycle() {
    let server = TestServer::start().await;
    let client = server.scheduler_client().await;

    let created = client
        .create_schedule_group()
        .name("life-grp")
        .send()
        .await
        .unwrap();
    assert!(created
        .schedule_group_arn()
        .contains("schedule-group/life-grp"));

    let got = client
        .get_schedule_group()
        .name("life-grp")
        .send()
        .await
        .unwrap();
    assert_eq!(got.name().unwrap(), "life-grp");

    client
        .delete_schedule_group()
        .name("life-grp")
        .send()
        .await
        .unwrap();

    let err = client
        .get_schedule_group()
        .name("life-grp")
        .send()
        .await
        .expect_err("group should be gone");
    assert!(format!("{err:?}").contains("ResourceNotFound"));
}

// ---------------------------------------------------------------------------
// Firing semantics (Batch 2)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn scheduler_rate_expression_delivers_input_to_sqs() {
    let server = TestServer::start().await;
    let sched = server.scheduler_client().await;
    let sqs = server.sqs_client().await;

    let q_url = queue_url(&sqs, "fire-dest").await;
    let q_arn = queue_arn(&sqs, &q_url).await;

    let target = Target::builder()
        .arn(q_arn.clone())
        .role_arn("arn:aws:iam::000000000000:role/scheduler")
        .input("{\"msg\":\"hello\"}")
        .build()
        .unwrap();

    sched
        .create_schedule()
        .name("fire-rate")
        .schedule_expression("rate(1 minute)")
        .flexible_time_window(off_window())
        .target(target)
        .send()
        .await
        .expect("create_schedule");

    let msg = wait_for_message(&sqs, &q_url, Duration::from_secs(10))
        .await
        .expect("scheduler should deliver within 10s");
    assert_eq!(msg.body.unwrap(), "{\"msg\":\"hello\"}");
}

#[tokio::test]
async fn scheduler_at_one_shot_delete_removes_schedule_after_fire() {
    let server = TestServer::start().await;
    let sched = server.scheduler_client().await;
    let sqs = server.sqs_client().await;

    let q_url = queue_url(&sqs, "once-dest").await;
    let q_arn = queue_arn(&sqs, &q_url).await;

    let target = Target::builder()
        .arn(q_arn)
        .role_arn("arn:aws:iam::000000000000:role/scheduler")
        .input("{\"one\":\"shot\"}")
        .build()
        .unwrap();

    // Past-dated so it fires on the next tick.
    sched
        .create_schedule()
        .name("once-delete")
        .schedule_expression("at(2020-01-01T00:00:00)")
        .flexible_time_window(off_window())
        .target(target)
        .action_after_completion(ActionAfterCompletion::Delete)
        .send()
        .await
        .unwrap();

    let msg = wait_for_message(&sqs, &q_url, Duration::from_secs(10))
        .await
        .expect("one-shot should fire within 10s");
    assert_eq!(msg.body.unwrap(), "{\"one\":\"shot\"}");

    // Poll for the DELETE post-fire action to take effect rather than
    // sleeping a fixed duration. Still bounded by a 10s deadline.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let err = loop {
        let result = sched.get_schedule().name("once-delete").send().await;
        if let Err(e) = result {
            break e;
        }
        if std::time::Instant::now() >= deadline {
            panic!("schedule was not deleted within 10s after firing");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert!(format!("{err:?}").contains("ResourceNotFound"));
}

#[tokio::test]
async fn scheduler_dlq_routing_on_missing_target_queue() {
    let server = TestServer::start().await;
    let sched = server.scheduler_client().await;
    let sqs = server.sqs_client().await;

    // Create ONLY the DLQ — the target queue ARN is bogus on purpose.
    let dlq_url = queue_url(&sqs, "dlq-dest").await;
    let dlq_arn = queue_arn(&sqs, &dlq_url).await;

    let target = Target::builder()
        .arn("arn:aws:sqs:us-east-1:000000000000:no-such-queue")
        .role_arn("arn:aws:iam::000000000000:role/scheduler")
        .input("{\"original\":true}")
        .dead_letter_config(DeadLetterConfig::builder().arn(dlq_arn.clone()).build())
        .build()
        .unwrap();

    sched
        .create_schedule()
        .name("dlq-test")
        .schedule_expression("rate(1 minute)")
        .flexible_time_window(off_window())
        .target(target)
        .send()
        .await
        .unwrap();

    let msg = wait_for_message(&sqs, &dlq_url, Duration::from_secs(10))
        .await
        .expect("DLQ should receive failed delivery");
    assert_eq!(msg.body.unwrap(), "{\"original\":true}");
    let attrs = msg
        .message_attributes
        .expect("DLQ message should have attrs");
    assert!(attrs.contains_key("X-Amz-Scheduler-Attempt"));
    assert!(attrs.contains_key("X-Amz-Scheduler-Schedule-Arn"));
    assert!(attrs.contains_key("X-Amz-Scheduler-Error-Code"));
}

#[tokio::test]
async fn cross_account_sqs_target_delivers_to_target_account_queue() {
    use aws_credential_types::Credentials;

    const ACCOUNT_A: &str = "111111111111";
    const ACCOUNT_B: &str = "222222222222";

    let server = TestServer::start_with_env(&[("FAKECLOUD_IAM", "soft")]).await;

    // Bootstrap admins in both accounts so we can sign as either.
    let (a_akid, a_secret) = server.create_admin(ACCOUNT_A, "admin-a").await;
    let (b_akid, b_secret) = server.create_admin(ACCOUNT_B, "admin-b").await;

    let cfg_a = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(&a_akid, &a_secret, None, None, "a"))
        .load()
        .await;
    let cfg_b = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .endpoint_url(server.endpoint())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(Credentials::new(&b_akid, &b_secret, None, None, "b"))
        .load()
        .await;

    // Account B owns the destination queue.
    let sqs_b = aws_sdk_sqs::Client::new(&cfg_b);
    let dest_url = queue_url(&sqs_b, "xacct-dest").await;
    let dest_arn = queue_arn(&sqs_b, &dest_url).await;
    assert!(
        dest_arn.contains(ACCOUNT_B),
        "queue ARN should include account B"
    );

    // Account A creates a schedule whose target points at account B's queue.
    let scheduler_a = aws_sdk_scheduler::Client::new(&cfg_a);
    let target = Target::builder()
        .arn(&dest_arn)
        .role_arn(format!("arn:aws:iam::{ACCOUNT_A}:role/scheduler"))
        .input("{\"crossAccount\":true}")
        .build()
        .unwrap();
    scheduler_a
        .create_schedule()
        .name("xacct-sched")
        .schedule_expression("rate(1 minute)")
        .flexible_time_window(off_window())
        .target(target)
        .send()
        .await
        .unwrap();

    // Account B receives the message via the same queue.
    let msg = wait_for_message(&sqs_b, &dest_url, Duration::from_secs(10))
        .await
        .expect("cross-account schedule should deliver to account B queue");
    assert_eq!(msg.body.unwrap(), "{\"crossAccount\":true}");
}

// ---------------------------------------------------------------------------
// K11: FlexibleTimeWindow + RetryPolicy + ScheduleExpressionTimezone
// ---------------------------------------------------------------------------

#[tokio::test]
async fn scheduler_flexible_time_window_off_fires_immediately() {
    // Mode=OFF (default) must fire on the next tick after due, with no
    // window-driven deferral. This locks the OFF semantics so the
    // FLEXIBLE test below has a meaningful baseline.
    let server = TestServer::start().await;
    let sched = server.scheduler_client().await;
    let sqs = server.sqs_client().await;

    let q_url = queue_url(&sqs, "flex-off-dest").await;
    let q_arn = queue_arn(&sqs, &q_url).await;

    let target = Target::builder()
        .arn(q_arn)
        .role_arn("arn:aws:iam::000000000000:role/scheduler")
        .input("{\"k\":\"off\"}")
        .build()
        .unwrap();

    sched
        .create_schedule()
        .name("flex-off")
        .schedule_expression("rate(1 minute)")
        .flexible_time_window(off_window())
        .target(target)
        .send()
        .await
        .unwrap();

    let msg = wait_for_message(&sqs, &q_url, Duration::from_secs(10))
        .await
        .expect("OFF mode must fire promptly on next tick");
    assert_eq!(msg.body.unwrap(), "{\"k\":\"off\"}");
}

#[tokio::test]
async fn scheduler_flexible_time_window_flexible_fires_within_window() {
    // Mode=FLEXIBLE with MaximumWindowInMinutes=1 must fire within
    // [0, 60]s of due. We use a 1-minute window (the smallest AWS
    // accepts) and a 90s deadline so the window can complete with
    // headroom. The RNG is seeded per (schedule_arn, fire_minute) so
    // the offset is bounded but non-deterministic across schedules.
    let server = TestServer::start().await;
    let sched = server.scheduler_client().await;
    let sqs = server.sqs_client().await;

    let q_url = queue_url(&sqs, "flex-on-dest").await;
    let q_arn = queue_arn(&sqs, &q_url).await;

    let target = Target::builder()
        .arn(q_arn)
        .role_arn("arn:aws:iam::000000000000:role/scheduler")
        .input("{\"k\":\"flex\"}")
        .build()
        .unwrap();

    let window = FlexibleTimeWindow::builder()
        .mode(FlexibleTimeWindowMode::Flexible)
        .maximum_window_in_minutes(1)
        .build()
        .unwrap();

    sched
        .create_schedule()
        .name("flex-on")
        .schedule_expression("rate(1 minute)")
        .flexible_time_window(window)
        .target(target)
        .send()
        .await
        .unwrap();

    let msg = wait_for_message(&sqs, &q_url, Duration::from_secs(90))
        .await
        .expect("FLEXIBLE schedule must fire within MaximumWindowInMinutes");
    assert_eq!(msg.body.unwrap(), "{\"k\":\"flex\"}");
}

#[tokio::test]
async fn scheduler_retry_policy_routes_to_dlq_after_budget_exhausted() {
    // Schedule with MaximumRetryAttempts=2 against a non-existent queue.
    // Expected: initial attempt + 2 retries all fail -> DLQ receives
    // exactly one message after the budget is exhausted.
    let server = TestServer::start().await;
    let sched = server.scheduler_client().await;
    let sqs = server.sqs_client().await;

    let dlq_url = queue_url(&sqs, "retry-dlq").await;
    let dlq_arn = queue_arn(&sqs, &dlq_url).await;

    let target = Target::builder()
        .arn("arn:aws:sqs:us-east-1:000000000000:retry-missing")
        .role_arn("arn:aws:iam::000000000000:role/scheduler")
        .input("{\"v\":\"retry-fail\"}")
        .dead_letter_config(DeadLetterConfig::builder().arn(dlq_arn.clone()).build())
        .retry_policy(
            RetryPolicy::builder()
                .maximum_event_age_in_seconds(86400)
                .maximum_retry_attempts(2)
                .build(),
        )
        .build()
        .unwrap();

    sched
        .create_schedule()
        .name("retry-fail")
        .schedule_expression("rate(1 minute)")
        .flexible_time_window(off_window())
        .target(target)
        .send()
        .await
        .unwrap();

    let msg = wait_for_message(&sqs, &dlq_url, Duration::from_secs(30))
        .await
        .expect("DLQ must receive failed delivery once retry budget is exhausted");
    assert_eq!(msg.body.unwrap(), "{\"v\":\"retry-fail\"}");
    let attrs = msg.message_attributes.unwrap();
    assert!(attrs.contains_key("X-Amz-Scheduler-Error-Code"));
}

#[tokio::test]
async fn scheduler_retry_policy_does_not_dlq_when_target_recovers() {
    // Schedule against a queue that DOES exist. Initial attempt
    // succeeds -> no DLQ traffic. This pins the success path so we
    // know retries don't double-deliver to the DLQ when the target
    // accepts the first attempt.
    let server = TestServer::start().await;
    let sched = server.scheduler_client().await;
    let sqs = server.sqs_client().await;

    let dest_url = queue_url(&sqs, "retry-ok-dest").await;
    let dest_arn = queue_arn(&sqs, &dest_url).await;
    let dlq_url = queue_url(&sqs, "retry-ok-dlq").await;
    let dlq_arn = queue_arn(&sqs, &dlq_url).await;

    let target = Target::builder()
        .arn(dest_arn)
        .role_arn("arn:aws:iam::000000000000:role/scheduler")
        .input("{\"v\":\"recovers\"}")
        .dead_letter_config(DeadLetterConfig::builder().arn(dlq_arn).build())
        .retry_policy(
            RetryPolicy::builder()
                .maximum_event_age_in_seconds(86400)
                .maximum_retry_attempts(3)
                .build(),
        )
        .build()
        .unwrap();

    sched
        .create_schedule()
        .name("retry-ok")
        .schedule_expression("rate(1 minute)")
        .flexible_time_window(off_window())
        .target(target)
        .send()
        .await
        .unwrap();

    // Target receives the message normally.
    let msg = wait_for_message(&sqs, &dest_url, Duration::from_secs(15))
        .await
        .expect("target must receive on first attempt when reachable");
    assert_eq!(msg.body.unwrap(), "{\"v\":\"recovers\"}");

    // DLQ stays empty (allow a brief window in case retries fired
    // late, then poll once).
    let dlq_msg = wait_for_message(&sqs, &dlq_url, Duration::from_secs(2)).await;
    assert!(
        dlq_msg.is_none(),
        "DLQ must remain empty when target accepts delivery"
    );
}

#[tokio::test]
async fn scheduler_schedule_expression_timezone_round_trips() {
    // Wall-clock testing of cron-in-tz is fragile; the firing logic
    // for tz lives in unit tests on `expr::matches_cron_in_tz`. This
    // e2e pins the contract that ScheduleExpressionTimezone is
    // accepted, persisted, and returned via GetSchedule unchanged.
    let server = TestServer::start().await;
    let sched = server.scheduler_client().await;

    sched
        .create_schedule()
        .name("tz-sched")
        .schedule_expression("cron(0 12 * * ? *)")
        .schedule_expression_timezone("America/New_York")
        .flexible_time_window(off_window())
        .target(sqs_target())
        .send()
        .await
        .unwrap();

    let got = sched.get_schedule().name("tz-sched").send().await.unwrap();
    assert_eq!(
        got.schedule_expression_timezone().unwrap(),
        "America/New_York"
    );
    assert_eq!(got.schedule_expression().unwrap(), "cron(0 12 * * ? *)");
}
