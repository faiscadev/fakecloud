//! Background poller that bridges DynamoDB Streams -> Lambda event source mappings.
//!
//! Periodically checks Lambda state for enabled event source mappings
//! pointing to DynamoDB streams, reads stream records, and invokes Lambda
//! functions with batches of records.

use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;

use fakecloud_core::delivery::LambdaDelivery;
use fakecloud_dynamodb::{cmp_seq, SharedDynamoDbState};
use fakecloud_lambda::filter::FilterSet;
use fakecloud_lambda::{LambdaInvocation, SharedLambdaState};
use fakecloud_persistence::SnapshotHook;

/// DynamoDB Streams -> Lambda event source mapping poller.
pub struct DynamoDbStreamsLambdaPoller {
    dynamodb_state: SharedDynamoDbState,
    lambda_state: SharedLambdaState,
    lambda_delivery: Option<Arc<dyn LambdaDelivery>>,
    /// Persists DynamoDB state after the poller advances a stream checkpoint.
    /// Without it the checkpoint only reached disk on the next unrelated
    /// DynamoDB API write, so a restart in the window re-delivered records the
    /// mapping had already consumed (bug-audit 4.5).
    snapshot_hook: Option<SnapshotHook>,
}

impl DynamoDbStreamsLambdaPoller {
    pub fn new(dynamodb_state: SharedDynamoDbState, lambda_state: SharedLambdaState) -> Self {
        Self {
            dynamodb_state,
            lambda_state,
            lambda_delivery: None,
            snapshot_hook: None,
        }
    }

    pub fn with_lambda_delivery(mut self, delivery: Arc<dyn LambdaDelivery>) -> Self {
        self.lambda_delivery = Some(delivery);
        self
    }

    pub fn with_snapshot_hook(mut self, hook: SnapshotHook) -> Self {
        self.snapshot_hook = Some(hook);
        self
    }

    /// Persist DynamoDB state through the same snapshot path a mutating
    /// DynamoDB API call uses. Called only after the write guard is released
    /// so the blocking snapshot IO never runs under the lock.
    async fn persist(&self) {
        if let Some(hook) = &self.snapshot_hook {
            hook().await;
        }
    }

    pub async fn run(self: Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_millis(500));
        loop {
            interval.tick().await;
            self.poll().await;
        }
    }

    async fn poll(&self) {
        struct DdbMapping {
            uuid: String,
            stream_arn: String,
            function_arn: String,
            batch_size: i64,
            filter: FilterSet,
            starting_position: Option<String>,
            starting_position_timestamp: Option<f64>,
        }

        // Collect enabled mappings that point to DynamoDB streams across
        // ALL accounts — a non-default/cross-account ESM must fire too, the
        // same way the Kinesis poller iterates every account.
        let mappings: Vec<DdbMapping> = {
            let lambda_accounts = self.lambda_state.read();
            lambda_accounts
                .iter()
                .flat_map(|(_, lambda)| {
                    lambda
                        .event_source_mappings
                        .values()
                        .filter(|m| {
                            m.enabled
                                && m.event_source_arn.contains(":dynamodb:")
                                && m.event_source_arn.contains("/stream/")
                        })
                        .map(|m| DdbMapping {
                            uuid: m.uuid.clone(),
                            stream_arn: m.event_source_arn.clone(),
                            function_arn: m.function_arn.clone(),
                            batch_size: m.batch_size,
                            filter: FilterSet::from_strings(m.filter_patterns.iter()),
                            starting_position: m.starting_position.clone(),
                            starting_position_timestamp: m.starting_position_timestamp,
                        })
                        .collect::<Vec<_>>()
                })
                .collect()
        };

        if mappings.is_empty() {
            return;
        }

        for DdbMapping {
            uuid: mapping_id,
            stream_arn,
            function_arn,
            batch_size,
            filter,
            starting_position,
            starting_position_timestamp,
        } in mappings
        {
            // Extract table name from stream ARN
            // Format: arn:aws:dynamodb:region:account:table/TableName/stream/timestamp
            let table_name = if let Some(table_part) = stream_arn.split(":table/").nth(1) {
                table_part.split('/').next().unwrap_or("")
            } else {
                continue;
            };
            // The DynamoDB state holding the table lives in the stream ARN's
            // own account, which is not necessarily the default account.
            let ddb_account = stream_arn.split(':').nth(4).unwrap_or("").to_string();

            // AT_TIMESTAMP isn't valid for DDB streams in real AWS;
            // suppress the field if the user supplied it.
            let _ = starting_position_timestamp;

            // Initialize the checkpoint based on StartingPosition the
            // first time we see this mapping. For DDB streams AWS only
            // accepts TRIM_HORIZON (default — replays existing records)
            // and LATEST (skip whatever is already in the stream).
            //
            // The checkpoint lives in the (persisted) DynamoDB state so it
            // survives a restart: on reload the key is already present, so
            // we resume from the last delivered sequence number instead of
            // re-seeding TRIM_HORIZON and re-invoking the target Lambda
            // with the whole retained backlog (duplicate side effects).
            let checkpoint = {
                let mut ddb_accounts = self.dynamodb_state.write();
                let dynamodb = match ddb_accounts.get_mut(&ddb_account) {
                    Some(d) => d,
                    None => continue,
                };
                if dynamodb.lambda_stream_checkpoint(&mapping_id).is_none() {
                    if let Some(table) = dynamodb.tables.get(table_name) {
                        let stream_records = table.stream_records.read();
                        let init = match starting_position.as_deref().unwrap_or("TRIM_HORIZON") {
                            "LATEST" => stream_records
                                .iter()
                                .map(|r| r.dynamodb.sequence_number.clone())
                                .max_by(|a, b| cmp_seq(a, b))
                                .unwrap_or_default(),
                            _ => String::new(),
                        };
                        drop(stream_records);
                        dynamodb.set_lambda_stream_checkpoint(&mapping_id, init);
                    }
                }
                dynamodb.lambda_stream_checkpoint(&mapping_id)
            };

            // Read stream records from DynamoDB
            let records = {
                let ddb_accounts = self.dynamodb_state.read();
                let dynamodb = match ddb_accounts.get(&ddb_account) {
                    Some(d) => d,
                    None => continue,
                };
                let table = match dynamodb.tables.get(table_name) {
                    Some(t) => t,
                    None => continue,
                };

                if !table.stream_enabled {
                    continue;
                }

                let stream_records = table.stream_records.read();

                // Filter records after checkpoint
                let mut filtered: Vec<_> = stream_records
                    .iter()
                    .filter(|r| match checkpoint.as_deref() {
                        Some(cp) if !cp.is_empty() => {
                            cmp_seq(&r.dynamodb.sequence_number, cp) == std::cmp::Ordering::Greater
                        }
                        _ => true,
                    })
                    .take(batch_size.max(0) as usize)
                    .cloned()
                    .collect();

                // Sort by sequence number (numeric) to ensure delivery order.
                filtered.sort_by(|a, b| {
                    cmp_seq(&a.dynamodb.sequence_number, &b.dynamodb.sequence_number)
                });

                filtered
            };

            if records.is_empty() {
                continue;
            }

            // Build per-record Lambda event JSON, then drop any record
            // whose JSON doesn't match FilterCriteria. AWS treats
            // filtered-out records as consumed — the checkpoint always
            // advances past them.
            let last_seq = records.last().map(|r| r.dynamodb.sequence_number.clone());
            let event_records: Vec<serde_json::Value> = records
                .iter()
                .filter_map(|record| {
                    let mut event_record = json!({
                        "eventID": record.event_id,
                        "eventName": record.event_name,
                        "eventVersion": record.event_version,
                        "eventSource": record.event_source,
                        "awsRegion": record.aws_region,
                        "dynamodb": {
                            "Keys": record.dynamodb.keys,
                            "SequenceNumber": record.dynamodb.sequence_number,
                            "SizeBytes": record.dynamodb.size_bytes,
                            "StreamViewType": record.dynamodb.stream_view_type,
                        },
                        "eventSourceARN": record.event_source_arn,
                    });

                    if let Some(ref new_img) = record.dynamodb.new_image {
                        event_record["dynamodb"]["NewImage"] = json!(new_img);
                    }
                    if let Some(ref old_img) = record.dynamodb.old_image {
                        event_record["dynamodb"]["OldImage"] = json!(old_img);
                    }

                    if filter.matches(&event_record) {
                        Some(event_record)
                    } else {
                        None
                    }
                })
                .collect();

            // If the filter dropped every record, advance the
            // checkpoint past them — AWS treats filtered records as
            // consumed. Otherwise, hold the checkpoint until after a
            // successful invoke so failures retry on the next poll.
            if event_records.is_empty() {
                if let Some(seq) = last_seq.clone() {
                    self.advance_checkpoint(&ddb_account, &mapping_id, seq);
                }
                continue;
            }

            let event = json!({ "Records": &event_records });
            let payload = serde_json::to_string(&event).unwrap_or_default();

            let invoke_succeeded = match &self.lambda_delivery {
                Some(delivery) => match delivery.invoke_lambda(&function_arn, &payload).await {
                    Ok(_) => {
                        tracing::info!(
                            function_arn = %function_arn,
                            record_count = event_records.len(),
                            "DynamoDB Streams->Lambda invocation succeeded"
                        );
                        true
                    }
                    Err(e) => {
                        tracing::error!(
                            function_arn = %function_arn,
                            error = %e,
                            "DynamoDB Streams->Lambda invocation failed"
                        );
                        false
                    }
                },
                None => true,
            };

            if !invoke_succeeded {
                continue;
            }

            // Successful invoke — advance the checkpoint and record
            // the invocation when no real Lambda runtime is wired.
            if let Some(seq) = last_seq.clone() {
                self.advance_checkpoint(&ddb_account, &mapping_id, seq);
                // Persist the advanced checkpoint so a restart resumes past the
                // records this batch already delivered.
                self.persist().await;
            }

            if self.lambda_delivery.is_none() {
                let fn_account = function_arn.split(':').nth(4).unwrap_or("");
                let mut lambda_accounts = self.lambda_state.write();
                let lambda = lambda_accounts.get_or_create(fn_account);
                lambda.invocations.push(LambdaInvocation {
                    function_arn: function_arn.clone(),
                    payload: payload.clone(),
                    timestamp: Utc::now(),
                    source: "dynamodb:streams".to_string(),
                });
            }
        }
    }

    /// Persist the last-delivered sequence number for a mapping into the
    /// DynamoDB state. The value rides along with the next DynamoDB
    /// snapshot save, so it survives a restart (mirrors how Kinesis lambda
    /// checkpoints persist through their snapshot).
    fn advance_checkpoint(&self, ddb_account: &str, mapping_id: &str, sequence_number: String) {
        let mut ddb_accounts = self.dynamodb_state.write();
        if let Some(dynamodb) = ddb_accounts.get_mut(ddb_account) {
            dynamodb.set_lambda_stream_checkpoint(mapping_id, sequence_number);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use fakecloud_core::multi_account::MultiAccountState;
    use fakecloud_core::service::{AwsRequest, AwsService};
    use fakecloud_dynamodb::{DynamoDbService, DynamoDbState};
    use fakecloud_lambda::{EventSourceMapping, LambdaState};
    use parking_lot::RwLock;
    use serde_json::Value;

    const DEFAULT_ACCOUNT: &str = "123456789012";
    const REGION: &str = "us-east-1";
    const ENDPOINT: &str = "http://localhost:4566";

    fn make_request(account: &str, action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "dynamodb".to_string(),
            action: action.to_string(),
            region: REGION.to_string(),
            account_id: account.to_string(),
            request_id: "test-id".to_string(),
            headers: axum::http::HeaderMap::new(),
            query_params: std::collections::HashMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: axum::http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    async fn create_streamed_table(svc: &DynamoDbService, account: &str, table: &str) {
        svc.handle(make_request(
            account,
            "CreateTable",
            serde_json::json!({
                "TableName": table,
                "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
                "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
                "BillingMode": "PAY_PER_REQUEST",
                "StreamSpecification": {
                    "StreamEnabled": true,
                    "StreamViewType": "NEW_AND_OLD_IMAGES"
                }
            }),
        ))
        .await
        .expect("CreateTable should succeed");
    }

    async fn put_item(svc: &DynamoDbService, account: &str, table: &str, pk: &str) {
        svc.handle(make_request(
            account,
            "PutItem",
            serde_json::json!({
                "TableName": table,
                "Item": { "pk": { "S": pk } }
            }),
        ))
        .await
        .expect("PutItem should succeed");
    }

    fn stream_arn(state: &SharedDynamoDbState, account: &str, table: &str) -> String {
        state
            .read()
            .get(account)
            .expect("account exists")
            .tables
            .get(table)
            .expect("table exists")
            .stream_arn
            .clone()
            .expect("stream enabled")
    }

    fn esm(uuid: &str, account: &str, stream_arn: &str) -> EventSourceMapping {
        EventSourceMapping {
            uuid: uuid.to_string(),
            function_arn: format!("arn:aws:lambda:{REGION}:{account}:function:streamed-fn"),
            event_source_arn: stream_arn.to_string(),
            batch_size: 10,
            enabled: true,
            state: "Enabled".to_string(),
            last_modified: Utc::now(),
            filter_patterns: Vec::new(),
            maximum_batching_window_in_seconds: None,
            starting_position: Some("TRIM_HORIZON".to_string()),
            starting_position_timestamp: None,
            parallelization_factor: None,
            function_response_types: Vec::new(),
            kms_key_arn: None,
            metrics_config: None,
            destination_config: None,
            maximum_retry_attempts: None,
            maximum_record_age_in_seconds: None,
            bisect_batch_on_function_error: None,
            tumbling_window_in_seconds: None,
            topics: Vec::new(),
            queues: Vec::new(),
            source_access_configurations: Vec::new(),
        }
    }

    fn lambda_state_with(account: &str, mapping: EventSourceMapping) -> SharedLambdaState {
        let mut lambda: MultiAccountState<LambdaState> =
            MultiAccountState::new(DEFAULT_ACCOUNT, REGION, ENDPOINT);
        lambda
            .get_or_create(account)
            .event_source_mappings
            .insert(mapping.uuid.clone(), mapping);
        Arc::new(RwLock::new(lambda))
    }

    fn invocation_count(lambda_state: &SharedLambdaState, account: &str) -> usize {
        lambda_state
            .read()
            .get(account)
            .map(|l| l.invocations.len())
            .unwrap_or(0)
    }

    /// Deliver a batch, then "restart" by round-tripping the DynamoDB state
    /// through serde and rebuilding the poller against the restored state +
    /// a fresh Lambda state. The durable checkpoint must prevent the whole
    /// retained backlog from being re-replayed to the target Lambda.
    #[tokio::test]
    async fn checkpoint_survives_restart_no_replay() {
        let dynamodb_state: SharedDynamoDbState = Arc::new(RwLock::new(MultiAccountState::new(
            DEFAULT_ACCOUNT,
            REGION,
            ENDPOINT,
        )));
        let svc = DynamoDbService::new(dynamodb_state.clone());
        create_streamed_table(&svc, DEFAULT_ACCOUNT, "t").await;
        put_item(&svc, DEFAULT_ACCOUNT, "t", "a").await;
        put_item(&svc, DEFAULT_ACCOUNT, "t", "b").await;

        let arn = stream_arn(&dynamodb_state, DEFAULT_ACCOUNT, "t");
        let lambda_state = lambda_state_with(DEFAULT_ACCOUNT, esm("esm-1", DEFAULT_ACCOUNT, &arn));

        let poller = DynamoDbStreamsLambdaPoller::new(dynamodb_state.clone(), lambda_state.clone());
        poller.poll().await;
        assert_eq!(
            invocation_count(&lambda_state, DEFAULT_ACCOUNT),
            1,
            "first poll delivers the backlog as one batch"
        );
        // A second poll on the same process must not re-deliver.
        poller.poll().await;
        assert_eq!(
            invocation_count(&lambda_state, DEFAULT_ACCOUNT),
            1,
            "no re-delivery while checkpoint is current"
        );

        // "Restart": persist + reload the DynamoDB state, fresh Lambda state.
        let serialized = serde_json::to_string(&*dynamodb_state.read()).unwrap();
        let restored: MultiAccountState<DynamoDbState> = serde_json::from_str(&serialized).unwrap();
        let restored_state: SharedDynamoDbState = Arc::new(RwLock::new(restored));
        // The checkpoint must have survived the round-trip.
        assert!(
            restored_state
                .read()
                .get(DEFAULT_ACCOUNT)
                .unwrap()
                .lambda_stream_checkpoint("esm-1")
                .is_some(),
            "checkpoint persisted through snapshot"
        );

        let lambda_state2 = lambda_state_with(DEFAULT_ACCOUNT, esm("esm-1", DEFAULT_ACCOUNT, &arn));
        let poller2 = DynamoDbStreamsLambdaPoller::new(restored_state, lambda_state2.clone());
        poller2.poll().await;
        assert_eq!(
            invocation_count(&lambda_state2, DEFAULT_ACCOUNT),
            0,
            "restart must resume from the durable checkpoint, not re-replay TRIM_HORIZON"
        );
    }

    /// An event source mapping on a non-default / cross-account stream must
    /// still fire — the poller used to serve only the default account.
    #[tokio::test]
    async fn non_default_account_mapping_fires() {
        const OTHER: &str = "999999999999";
        let dynamodb_state: SharedDynamoDbState = Arc::new(RwLock::new(MultiAccountState::new(
            DEFAULT_ACCOUNT,
            REGION,
            ENDPOINT,
        )));
        let svc = DynamoDbService::new(dynamodb_state.clone());
        create_streamed_table(&svc, OTHER, "t").await;
        put_item(&svc, OTHER, "t", "a").await;

        let arn = stream_arn(&dynamodb_state, OTHER, "t");
        assert!(arn.contains(OTHER), "stream arn carries the other account");
        let lambda_state = lambda_state_with(OTHER, esm("esm-x", OTHER, &arn));

        let poller = DynamoDbStreamsLambdaPoller::new(dynamodb_state.clone(), lambda_state.clone());
        poller.poll().await;
        assert_eq!(
            invocation_count(&lambda_state, OTHER),
            1,
            "cross-account mapping must fire"
        );
    }
}
