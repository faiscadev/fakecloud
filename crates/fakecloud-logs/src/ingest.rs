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
    let logs = accounts.get_or_create(account_id);
    let group = logs
        .log_groups
        .entry(group_name.to_string())
        .or_insert_with(|| LogGroup {
            name: group_name.to_string(),
            arn: format!(
                "arn:aws:logs:{region}:{account_id}:log-group:{group_name}",
                region = region,
                account_id = account_id,
            ),
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
        });
    let stream = group
        .log_streams
        .entry(stream_name.to_string())
        .or_insert_with(|| LogStream {
            name: stream_name.to_string(),
            arn: format!("{}:log-stream:{}", group.arn, stream_name),
            creation_time: now,
            first_event_timestamp: None,
            last_event_timestamp: None,
            last_ingestion_time: None,
            upload_sequence_token: uuid::Uuid::new_v4().simple().to_string(),
            events: Vec::new(),
        });

    let base_seq = stream.events.iter().map(|e| e.seq).max().unwrap_or(0);
    for (i, e) in events.iter().enumerate() {
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
        group.stored_bytes += e.message.len() as i64;
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
    use std::sync::Arc;

    use fakecloud_core::multi_account::MultiAccountState;
    use parking_lot::RwLock;

    use super::*;

    #[test]
    fn append_creates_group_and_stream_then_appends() {
        let state = Arc::new(RwLock::new(
            MultiAccountState::<crate::state::LogsState>::new(
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
        let logs = s.get("123456789012").unwrap();
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
            MultiAccountState::<crate::state::LogsState>::new(
                "123456789012",
                "us-east-1",
                "http://localhost:4566",
            ),
        ));
        append_events(&state, "123456789012", "us-east-1", "g", "s", &[]);
        let s = state.read();
        assert!(s
            .get("123456789012")
            .is_none_or(|a| a.log_groups.is_empty()));
    }
}
