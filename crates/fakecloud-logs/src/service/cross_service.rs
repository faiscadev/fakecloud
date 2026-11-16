//! Cross-service log delivery: Step Functions execution logging and API
//! Gateway access logs write through the same path as `PutLogEvents`, so the
//! group's transformer, subscription filters, metric filters and delivery
//! pipelines see their events, and the write is persisted like an API call.

use std::collections::HashMap;

use bytes::Bytes;
use http::{HeaderMap, Method};
use serde_json::json;

use fakecloud_core::delivery::CloudwatchLogsDelivery;
use fakecloud_core::service::AwsRequest;

use super::LogsService;

/// The largest batch PutLogEvents takes.
const MAX_BATCH_EVENTS: usize = 10_000;

fn internal_request(
    action: &str,
    account_id: &str,
    region: &str,
    body: serde_json::Value,
) -> AwsRequest {
    AwsRequest {
        service: "logs".to_string(),
        action: action.to_string(),
        region: region.to_string(),
        account_id: account_id.to_string(),
        request_id: uuid::Uuid::new_v4().to_string(),
        headers: HeaderMap::new(),
        query_params: HashMap::new(),
        body: Bytes::from(serde_json::to_vec(&body).expect("JSON body serializes")),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

impl LogsService {
    fn persist_in_background(&self) {
        if self.snapshot_store.is_none() {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let state = self.state.clone();
        let store = self.snapshot_store.clone();
        let lock = self.snapshot_lock.clone();
        handle.spawn(async move {
            if let Err(error) = super::save_logs_state(&state, store, &lock).await {
                tracing::error!(%error, "failed to persist Logs state");
            }
        });
    }
}

impl CloudwatchLogsDelivery for LogsService {
    fn put_log_events(
        &self,
        account_id: &str,
        region: &str,
        log_group_name: &str,
        log_stream_name: &str,
        events: &[(i64, String)],
    ) {
        if events.is_empty() {
            return;
        }
        if !self.log_group_exists(account_id, region, log_group_name) {
            tracing::debug!(
                log_group_name,
                "log group does not exist; dropping delivered events"
            );
            return;
        }
        // The stream is created on first delivery; one that exists is fine.
        let _ = self.create_log_stream(&internal_request(
            "CreateLogStream",
            account_id,
            region,
            json!({ "logGroupName": log_group_name, "logStreamName": log_stream_name }),
        ));
        // PutLogEvents takes a chronological batch.
        let mut ordered = events.to_vec();
        ordered.sort_by_key(|(timestamp, _)| *timestamp);
        for batch in ordered.chunks(MAX_BATCH_EVENTS) {
            let log_events: Vec<serde_json::Value> = batch
                .iter()
                .map(|(timestamp, message)| json!({ "timestamp": timestamp, "message": message }))
                .collect();
            if let Err(error) = self.put_log_events(&internal_request(
                "PutLogEvents",
                account_id,
                region,
                json!({
                    "logGroupName": log_group_name,
                    "logStreamName": log_stream_name,
                    "logEvents": log_events,
                }),
            )) {
                tracing::warn!(log_group_name, log_stream_name, %error, "log delivery failed");
            }
        }
        self.persist_in_background();
    }

    fn log_group_exists(&self, account_id: &str, region: &str, log_group_name: &str) -> bool {
        self.state
            .read()
            .regional(account_id, region)
            .is_some_and(|logs| logs.log_groups.contains_key(log_group_name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::test_helpers::{create_group, make_service};

    #[test]
    fn delivery_creates_the_stream_and_appends_in_order() {
        let svc = make_service();
        create_group(&svc, "/sfn/logs");
        let now = chrono::Utc::now().timestamp_millis();
        CloudwatchLogsDelivery::put_log_events(
            &svc,
            "123456789012",
            "us-east-1",
            "/sfn/logs",
            "states/sm/2026-11-16-21/00000000",
            &[(now, "second".into()), (now - 1_000, "first".into())],
        );
        let accounts = svc.state.read();
        let logs = accounts.regional("123456789012", "us-east-1").unwrap();
        let stream = &logs.log_groups["/sfn/logs"].log_streams["states/sm/2026-11-16-21/00000000"];
        let messages: Vec<&str> = stream.events.iter().map(|e| e.message.as_str()).collect();
        assert_eq!(messages, ["first", "second"]);
    }

    #[test]
    fn delivery_to_a_missing_group_takes_nothing() {
        let svc = make_service();
        assert!(!svc.log_group_exists("123456789012", "us-east-1", "/missing"));
        CloudwatchLogsDelivery::put_log_events(
            &svc,
            "123456789012",
            "us-east-1",
            "/missing",
            "s",
            &[(1_000, "x".into())],
        );
        let accounts = svc.state.read();
        assert!(accounts
            .regional("123456789012", "us-east-1")
            .is_none_or(|logs| logs.log_groups.is_empty()));
    }
}
