//! Same-process log ingestion for cross-service callers.
//!
//! ECS's `awslogs` driver and other in-tree producers (future Lambda
//! CloudWatch-Logs forwarding) need to write events into fakecloud-logs
//! without going through HTTP PutLogEvents. This module exposes a thin,
//! well-typed API that creates the log group/stream on demand and
//! appends events, mirroring the same validation shape as
//! `service::streams::put_log_events` but skipping `AwsRequest` parsing.

use chrono::Utc;

use crate::state::{LogEvent, LogGroup, LogStream, SharedLogsState};

/// An event destined for a CloudWatch Log stream.
#[derive(Debug, Clone)]
pub struct IngestEvent {
    pub timestamp_ms: i64,
    pub message: String,
}

/// Ensure a log group and stream exist for `account_id`, then append
/// `events` to the stream. Creates the group with no retention and the
/// stream with a fresh upload sequence token if either is missing —
/// matching what an `awslogs-create-group=true` driver would do.
pub fn append_events(
    state: &SharedLogsState,
    account_id: &str,
    region: &str,
    group_name: &str,
    stream_name: &str,
    events: &[IngestEvent],
) {
    if events.is_empty() {
        return;
    }
    let now = Utc::now().timestamp_millis();
    let mut accounts = state.write();
    let logs = accounts.regional_mut(account_id, region);
    let group = logs
        .log_groups
        .entry(group_name.to_string())
        .or_insert_with(|| LogGroup {
            name: group_name.to_string(),
            arn: crate::state::log_group_stored_arn(region, account_id, group_name),
            creation_time: now,
            retention_in_days: None,
            kms_key_id: None,
            tags: Default::default(),
            log_streams: Default::default(),
            stored_bytes: 0,
            subscription_filters: Vec::new(),
            data_protection_policy: None,
            index_policies: Vec::new(),
            transformer: None,
            deletion_protection: false,
            log_group_class: Some("STANDARD".into()),
            data_protection_deleted: false,
        });
    // CloudWatch Logs rejects events older than the group's effective
    // retention rather than storing them only for the next retention sweep to
    // delete. An account-level retention policy overrides the group's own.
    let retention_cutoff = crate::retention::effective_retention(&logs.account_policies, group)
        .map(|days| now.saturating_sub(i64::from(days).saturating_mul(86_400_000)));
    let stream_arn = group.stream_arn(stream_name);
    let stream = group
        .log_streams
        .entry(stream_name.to_string())
        .or_insert_with(|| LogStream {
            persistence_id: uuid::Uuid::new_v4().to_string(),
            last_sequence: 0,
            name: stream_name.to_string(),
            arn: stream_arn,
            creation_time: now,
            first_event_timestamp: None,
            last_event_timestamp: None,
            last_ingestion_time: None,
            upload_sequence_token: uuid::Uuid::new_v4().simple().to_string(),
            events: Vec::new(),
        });

    let base_seq = stream
        .events
        .iter()
        .map(|e| e.seq)
        .max()
        .unwrap_or(0)
        .max(stream.last_sequence);
    let accepted = events
        .iter()
        .filter(|e| retention_cutoff.is_none_or(|cutoff| e.timestamp_ms >= cutoff));
    for (i, e) in accepted.enumerate() {
        if stream.first_event_timestamp.is_none() {
            stream.first_event_timestamp = Some(e.timestamp_ms);
        }
        stream.last_event_timestamp = Some(
            stream
                .last_event_timestamp
                .map(|t| t.max(e.timestamp_ms))
                .unwrap_or(e.timestamp_ms),
        );
        stream.last_ingestion_time = Some(now);
        group.stored_bytes += e.message.len() as i64 + crate::persistence::EVENT_OVERHEAD_BYTES;
        stream.events.push(LogEvent {
            timestamp: e.timestamp_ms,
            message: e.message.clone(),
            ingestion_time: now,
            // Stable, monotonically-increasing per-stream sequence number.
            seq: base_seq + 1 + i as u64,
        });
    }
}

#[cfg(test)]
mod tests {
    use parking_lot::RwLock;
    use std::sync::Arc;

    use super::*;

    #[test]
    fn append_creates_group_and_stream_then_appends() {
        let state = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiRegionState::<crate::state::LogsState>::new(
                "123456789012",
                "us-east-1",
                "http://localhost:4566",
            ),
        ));
        append_events(
            &state,
            "123456789012",
            "us-east-1",
            "/ecs/svc",
            "app/123",
            &[
                IngestEvent {
                    timestamp_ms: 1_000,
                    message: "hello".into(),
                },
                IngestEvent {
                    timestamp_ms: 2_000,
                    message: "world".into(),
                },
            ],
        );
        let s = state.read();
        let logs = s.regional("123456789012", "us-east-1").unwrap();
        let group = logs.log_groups.get("/ecs/svc").unwrap();
        let stream = group.log_streams.get("app/123").unwrap();
        assert_eq!(stream.events.len(), 2);
        assert_eq!(stream.events[0].message, "hello");
        assert_eq!(stream.first_event_timestamp, Some(1_000));
        assert_eq!(stream.last_event_timestamp, Some(2_000));
        assert!(group.stored_bytes >= "hello".len() as i64 + "world".len() as i64);
    }

    #[test]
    fn append_no_op_for_empty() {
        let state = Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiRegionState::<crate::state::LogsState>::new(
                "123456789012",
                "us-east-1",
                "http://localhost:4566",
            ),
        ));
        append_events(&state, "123456789012", "us-east-1", "g", "s", &[]);
        let s = state.read();
        assert!(s
            .regional("123456789012", "us-east-1")
            .is_none_or(|a| a.log_groups.is_empty()));
    }
}
