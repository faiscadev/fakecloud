//! DynamoDB Streams data plane (`DynamoDBStreams_20120810`).
//!
//! Lambda event source mappings against `arn:aws:dynamodb:.../stream/...`
//! depend on `ListStreams`, `DescribeStream`, `GetShardIterator`, and
//! `GetRecords`. The control plane's `EnableStream` / `DescribeTable`
//! already populate `DynamoTable::stream_records` on every mutation;
//! this module is the consumer side that surfaces those records.

use std::sync::Arc;

use async_trait::async_trait;
use http::StatusCode;
use serde_json::{json, Value};

use fakecloud_core::service::{AwsRequest, AwsResponse, AwsService, AwsServiceError};

use crate::state::{DynamoTable, SharedDynamoDbState};

pub struct DynamoDbStreamsService {
    state: SharedDynamoDbState,
}

impl DynamoDbStreamsService {
    pub fn new(state: SharedDynamoDbState) -> Self {
        Self { state }
    }
}

#[async_trait]
impl AwsService for DynamoDbStreamsService {
    fn service_name(&self) -> &str {
        "dynamodbstreams"
    }

    async fn handle(&self, req: AwsRequest) -> Result<AwsResponse, AwsServiceError> {
        let body: Value = serde_json::from_slice(&req.body).unwrap_or_default();
        match req.action.as_str() {
            "ListStreams" => self.list_streams(&req, &body),
            "DescribeStream" => self.describe_stream(&req, &body),
            "GetShardIterator" => self.get_shard_iterator(&req, &body),
            "GetRecords" => self.get_records(&req, &body),
            _ => Err(AwsServiceError::action_not_implemented(
                "dynamodbstreams",
                &req.action,
            )),
        }
    }

    fn supported_actions(&self) -> &[&str] {
        &[
            "ListStreams",
            "DescribeStream",
            "GetShardIterator",
            "GetRecords",
        ]
    }
}

impl DynamoDbStreamsService {
    fn list_streams(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let table_filter = body["TableName"].as_str();
        let accounts = self.state.read();
        let state = match accounts.get(&req.account_id) {
            Some(s) => s,
            None => return Ok(AwsResponse::ok_json(json!({ "Streams": [] }))),
        };
        let mut streams = Vec::new();
        for table in state.tables.values() {
            if let Some(name) = table_filter {
                if table.name != name {
                    continue;
                }
            }
            if !table.stream_enabled {
                continue;
            }
            let Some(arn) = table.stream_arn.as_ref() else {
                continue;
            };
            let label = stream_label(arn);
            streams.push(json!({
                "StreamArn": arn,
                "TableName": table.name,
                "StreamLabel": label,
            }));
        }
        Ok(AwsResponse::ok_json(json!({ "Streams": streams })))
    }

    fn describe_stream(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let stream_arn = require_string(body, "StreamArn")?;
        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(|| not_found("Stream", &stream_arn))?;
        let table = state
            .tables
            .values()
            .find(|t| t.stream_arn.as_deref() == Some(stream_arn.as_str()))
            .ok_or_else(|| not_found("Stream", &stream_arn))?;

        let view_type = table
            .stream_view_type
            .clone()
            .unwrap_or_else(|| "NEW_AND_OLD_IMAGES".to_string());
        let label = stream_label(&stream_arn);
        let key_schema: Vec<Value> = table
            .key_schema
            .iter()
            .map(|k| {
                json!({
                    "AttributeName": k.attribute_name,
                    "KeyType": k.key_type,
                })
            })
            .collect();

        let body = json!({
            "StreamDescription": {
                "StreamArn": stream_arn,
                "StreamLabel": label,
                "StreamStatus": "ENABLED",
                "StreamViewType": view_type,
                "CreationRequestDateTime": table.created_at.timestamp() as f64,
                "TableName": table.name,
                "KeySchema": key_schema,
                "Shards": [{
                    "ShardId": "shardId-00000000000000000000-00000001",
                    "SequenceNumberRange": {
                        "StartingSequenceNumber": "0",
                    },
                }],
            }
        });
        Ok(AwsResponse::ok_json(body))
    }

    fn get_shard_iterator(
        &self,
        req: &AwsRequest,
        body: &Value,
    ) -> Result<AwsResponse, AwsServiceError> {
        let stream_arn = require_string(body, "StreamArn")?;
        let shard_id = require_string(body, "ShardId")?;
        let iterator_type = require_string(body, "ShardIteratorType")?;

        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(|| not_found("Stream", &stream_arn))?;
        let table = state
            .tables
            .values()
            .find(|t| t.stream_arn.as_deref() == Some(stream_arn.as_str()))
            .ok_or_else(|| not_found("Stream", &stream_arn))?;

        // The iterator is anchored to an *exclusive start sequence number*,
        // not a positional index into the records Vec. `add_stream_record`
        // physically trims expired records off the front (`records.retain`),
        // which would shift every index; a sequence-number anchor is stable
        // across trims (like native Kinesis advancing by stored seq, not by
        // Vec position). GetRecords returns records whose sequence_number is
        // strictly greater than this anchor. bug-audit 2026-06-15, 4.4.
        let records = table.stream_records.read();
        let after_seq: String = match iterator_type.as_str() {
            // Oldest retained record: anchor below every sequence number so the
            // first GetRecords returns the whole retained window.
            "TRIM_HORIZON" => "0".to_string(),
            // After the newest record: anchor at the max current sequence so
            // only records added later are returned.
            "LATEST" => records
                .iter()
                .map(|r| r.dynamodb.sequence_number.clone())
                .max_by(|a, b| cmp_seq(a, b))
                .unwrap_or_else(|| "0".to_string()),
            // Inclusive of the named record: anchor just below it.
            "AT_SEQUENCE_NUMBER" => {
                let seq = require_string(body, "SequenceNumber")?;
                if !records.iter().any(|r| r.dynamodb.sequence_number == seq) {
                    return Err(invalid_argument("SequenceNumber not found"));
                }
                exclusive_before(&seq)
            }
            // Exclusive of the named record: anchor exactly at it.
            "AFTER_SEQUENCE_NUMBER" => {
                let seq = require_string(body, "SequenceNumber")?;
                if !records.iter().any(|r| r.dynamodb.sequence_number == seq) {
                    return Err(invalid_argument("SequenceNumber not found"));
                }
                seq
            }
            other => {
                return Err(invalid_argument(&format!(
                    "Unsupported ShardIteratorType: {other}",
                )))
            }
        };

        let token = format!("{stream_arn}|{shard_id}|{after_seq}");
        Ok(AwsResponse::ok_json(json!({ "ShardIterator": token })))
    }

    fn get_records(&self, req: &AwsRequest, body: &Value) -> Result<AwsResponse, AwsServiceError> {
        let iterator = require_string(body, "ShardIterator")?;
        let limit = body["Limit"].as_u64().unwrap_or(1000) as usize;

        let parts: Vec<&str> = iterator.splitn(3, '|').collect();
        if parts.len() != 3 {
            return Err(invalid_argument("ShardIterator is invalid"));
        }
        let stream_arn = parts[0].to_string();
        let shard_id = parts[1].to_string();
        // Exclusive start sequence number (see get_shard_iterator). Index-based
        // tokens minted by older builds would parse as a number too, but they
        // are positions; since we now compare by sequence number this is
        // self-correcting after one GetShardIterator. bug-audit 2026-06-15, 4.4.
        let after_seq = parts[2].to_string();

        let accounts = self.state.read();
        let state = accounts
            .get(&req.account_id)
            .ok_or_else(|| not_found("Stream", &stream_arn))?;
        let table = state
            .tables
            .values()
            .find(|t| t.stream_arn.as_deref() == Some(stream_arn.as_str()))
            .ok_or_else(|| not_found("Stream", &stream_arn))?;

        // Records whose sequence number is strictly greater than the anchor,
        // in stored (arrival) order. Front-trimming by `records.retain` cannot
        // make us skip or replay: the anchor moves only by what we actually
        // returned, never by physical position.
        let records = table.stream_records.read();
        let selected: Vec<&crate::state::StreamRecord> = records
            .iter()
            .filter(|r| {
                cmp_seq(&r.dynamodb.sequence_number, &after_seq) == std::cmp::Ordering::Greater
            })
            .take(limit)
            .collect();

        let next_seq = selected
            .last()
            .map(|r| r.dynamodb.sequence_number.clone())
            .unwrap_or(after_seq);
        let records_json: Vec<Value> = selected
            .iter()
            .map(|r| stream_record_to_json(r, table))
            .collect();

        let next_token = format!("{stream_arn}|{shard_id}|{next_seq}");
        Ok(AwsResponse::ok_json(json!({
            "Records": records_json,
            "NextShardIterator": next_token,
        })))
    }
}

fn stream_record_to_json(r: &crate::state::StreamRecord, table: &DynamoTable) -> Value {
    let mut dynamodb = json!({
        "ApproximateCreationDateTime": r.timestamp.timestamp() as f64,
        "Keys": &r.dynamodb.keys,
        "SequenceNumber": r.dynamodb.sequence_number,
        "SizeBytes": r.dynamodb.size_bytes,
        "StreamViewType": r.dynamodb.stream_view_type,
    });
    if let Some(ni) = r.dynamodb.new_image.as_ref() {
        dynamodb["NewImage"] = json!(ni);
    }
    if let Some(oi) = r.dynamodb.old_image.as_ref() {
        dynamodb["OldImage"] = json!(oi);
    }
    json!({
        "eventID": r.event_id,
        "eventName": r.event_name,
        "eventVersion": r.event_version,
        "eventSource": r.event_source,
        "awsRegion": r.aws_region,
        "eventSourceARN": table.stream_arn.clone().unwrap_or_default(),
        "dynamodb": dynamodb,
    })
}

fn stream_label(stream_arn: &str) -> String {
    stream_arn.rsplit('/').next().unwrap_or("").to_string()
}

/// Compare two DynamoDB stream sequence numbers numerically. Sequence numbers
/// are minted by an atomic counter and zero-padded to a fixed width, so they
/// are also lexicographically ordered; we still parse to `u128` so that an
/// un-padded legacy value (or one of a different width) compares correctly.
fn cmp_seq(a: &str, b: &str) -> std::cmp::Ordering {
    match (a.parse::<u128>(), b.parse::<u128>()) {
        (Ok(x), Ok(y)) => x.cmp(&y),
        // Non-numeric values fall back to byte order (deterministic, total).
        _ => a.cmp(b),
    }
}

/// The largest sequence number strictly less than `seq`, used to build an
/// exclusive anchor for `AT_SEQUENCE_NUMBER` (which is inclusive of the named
/// record). For the numeric counter this is `seq - 1`; if `seq` is `0` or
/// non-numeric we anchor at `"0"` so nothing earlier is skipped.
fn exclusive_before(seq: &str) -> String {
    match seq.parse::<u128>() {
        Ok(n) if n > 0 => {
            // Preserve the original zero-padded width so lexicographic order
            // continues to match numeric order for downstream string compares.
            format!("{:0width$}", n - 1, width = seq.len())
        }
        _ => "0".to_string(),
    }
}

fn require_string(body: &Value, field: &str) -> Result<String, AwsServiceError> {
    body[field]
        .as_str()
        .map(|s| s.to_string())
        .ok_or_else(|| invalid_argument(&format!("{field} is required")))
}

fn invalid_argument(msg: &str) -> AwsServiceError {
    AwsServiceError::aws_error(StatusCode::BAD_REQUEST, "ValidationException", msg)
}

fn not_found(kind: &str, target: &str) -> AwsServiceError {
    AwsServiceError::aws_error(
        StatusCode::BAD_REQUEST,
        "ResourceNotFoundException",
        format!("{kind} not found: {target}"),
    )
}

pub fn shared(state: SharedDynamoDbState) -> Arc<dyn AwsService> {
    Arc::new(DynamoDbStreamsService::new(state))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{DynamoDbStreamRecord, DynamoTable, ProvisionedThroughput, StreamRecord};
    use bytes::Bytes;
    use chrono::Utc;
    use http::{HeaderMap, Method};
    use parking_lot::RwLock;
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;

    fn make_state() -> SharedDynamoDbState {
        Arc::new(RwLock::new(
            fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
        ))
    }

    fn req(action: &str, body: Value) -> AwsRequest {
        AwsRequest {
            service: "dynamodbstreams".into(),
            action: action.into(),
            region: "us-east-1".into(),
            account_id: "123456789012".into(),
            request_id: "r".into(),
            headers: HeaderMap::new(),
            query_params: HashMap::new(),
            body: Bytes::from(serde_json::to_vec(&body).unwrap()),
            body_stream: parking_lot::Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".into(),
            raw_query: String::new(),
            method: Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        }
    }

    fn seed_table(state: &SharedDynamoDbState) -> String {
        let mut accts = state.write();
        let s = accts.get_or_create("123456789012");
        let arn =
            "arn:aws:dynamodb:us-east-1:123456789012:table/widgets/stream/2026-05-03T00:00:00.000"
                .to_string();
        let table = DynamoTable {
            name: "widgets".to_string(),
            arn: "arn:aws:dynamodb:us-east-1:123456789012:table/widgets".to_string(),
            table_id: "id".to_string(),
            key_schema: Vec::new(),
            attribute_definitions: Vec::new(),
            provisioned_throughput: ProvisionedThroughput {
                read_capacity_units: 0,
                write_capacity_units: 0,
            },
            items: Vec::new(),
            gsi: Vec::new(),
            lsi: Vec::new(),
            tags: BTreeMap::new(),
            created_at: Utc::now(),
            status: "ACTIVE".to_string(),
            item_count: 0,
            size_bytes: 0,
            billing_mode: "PAY_PER_REQUEST".to_string(),
            ttl_attribute: None,
            ttl_enabled: false,
            resource_policy: None,
            pitr_enabled: false,
            kinesis_destinations: Vec::new(),
            contributor_insights_status: "DISABLED".to_string(),
            contributor_insights_counters: BTreeMap::new(),
            stream_enabled: true,
            stream_view_type: Some("NEW_AND_OLD_IMAGES".to_string()),
            stream_arn: Some(arn.clone()),
            stream_records: Arc::new(RwLock::new(Vec::new())),
            sse_type: None,
            sse_kms_key_arn: None,
            deletion_protection_enabled: false,
            on_demand_throughput: None,
        };
        let rec = StreamRecord {
            event_id: "e1".into(),
            event_name: "INSERT".into(),
            event_version: "1.1".into(),
            event_source: "aws:dynamodb".into(),
            aws_region: "us-east-1".into(),
            event_source_arn: arn.clone(),
            timestamp: Utc::now(),
            dynamodb: DynamoDbStreamRecord {
                keys: HashMap::new(),
                new_image: Some(HashMap::new()),
                old_image: None,
                sequence_number: "1".into(),
                size_bytes: 16,
                stream_view_type: "NEW_AND_OLD_IMAGES".into(),
            },
        };
        table.stream_records.write().push(rec);
        s.tables.insert("widgets".to_string(), table);
        arn
    }

    #[tokio::test]
    async fn list_streams_returns_enabled_streams() {
        let state = make_state();
        let arn = seed_table(&state);
        let svc = DynamoDbStreamsService::new(state);
        let resp = svc.handle(req("ListStreams", json!({}))).await.unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let streams = body["Streams"].as_array().unwrap();
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0]["StreamArn"].as_str().unwrap(), arn);
    }

    #[tokio::test]
    async fn describe_stream_returns_shard() {
        let state = make_state();
        let arn = seed_table(&state);
        let svc = DynamoDbStreamsService::new(state);
        let resp = svc
            .handle(req("DescribeStream", json!({"StreamArn": arn})))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let desc = &body["StreamDescription"];
        assert_eq!(desc["StreamStatus"].as_str().unwrap(), "ENABLED");
        assert_eq!(desc["Shards"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn get_records_round_trip_via_shard_iterator() {
        let state = make_state();
        let arn = seed_table(&state);
        let svc = DynamoDbStreamsService::new(state);
        let it_resp = svc
            .handle(req(
                "GetShardIterator",
                json!({
                    "StreamArn": arn,
                    "ShardId": "shardId-00000000000000000000-00000001",
                    "ShardIteratorType": "TRIM_HORIZON",
                }),
            ))
            .await
            .unwrap();
        let it_body: Value = serde_json::from_slice(it_resp.body.expect_bytes()).unwrap();
        let iterator = it_body["ShardIterator"].as_str().unwrap().to_string();
        let resp = svc
            .handle(req("GetRecords", json!({"ShardIterator": iterator})))
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let recs = body["Records"].as_array().unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(recs[0]["eventName"].as_str().unwrap(), "INSERT");
    }

    fn push_record(state: &SharedDynamoDbState, seq: &str, age_hours: i64, event_id: &str) {
        let mut accts = state.write();
        let s = accts.get_or_create("123456789012");
        let table = s.tables.get_mut("widgets").unwrap();
        let rec = StreamRecord {
            event_id: event_id.into(),
            event_name: "INSERT".into(),
            event_version: "1.1".into(),
            event_source: "aws:dynamodb".into(),
            aws_region: "us-east-1".into(),
            event_source_arn: table.stream_arn.clone().unwrap(),
            timestamp: Utc::now() - chrono::Duration::hours(age_hours),
            dynamodb: DynamoDbStreamRecord {
                keys: HashMap::new(),
                new_image: Some(HashMap::new()),
                old_image: None,
                sequence_number: seq.into(),
                size_bytes: 16,
                stream_view_type: "NEW_AND_OLD_IMAGES".into(),
            },
        };
        table.stream_records.write().push(rec);
    }

    fn trim_front(state: &SharedDynamoDbState, n: usize) {
        let accts = state.read();
        let s = accts.get("123456789012").unwrap();
        let table = s.tables.get("widgets").unwrap();
        let mut recs = table.stream_records.write();
        for _ in 0..n {
            if !recs.is_empty() {
                recs.remove(0);
            }
        }
    }

    // bug-audit 2026-06-15, 4.4: the iterator is anchored to a sequence number,
    // not a Vec index. A consumer that has read up to record N must, after the
    // front of the records Vec is physically trimmed (24h retention), continue
    // exactly where it left off — no skipped or replayed records.
    #[tokio::test]
    async fn iterator_survives_front_trim_without_skip_or_replay() {
        let state = make_state();
        let arn = seed_table(&state); // seeds one record seq "1"
                                      // Replace the seeded record set with a clean, ordered set seq 1..=5.
        {
            let accts = state.read();
            let s = accts.get("123456789012").unwrap();
            s.tables
                .get("widgets")
                .unwrap()
                .stream_records
                .write()
                .clear();
        }
        for i in 1..=5u64 {
            // First two records are aged so a later trim removes them.
            let age = if i <= 2 { 30 } else { 0 };
            push_record(&state, &format!("{i:021}"), age, &format!("e{i}"));
        }
        let svc = DynamoDbStreamsService::new(state.clone());

        // Start at TRIM_HORIZON, read 3 records (seq 1,2,3).
        let it_resp = svc
            .handle(req(
                "GetShardIterator",
                json!({
                    "StreamArn": arn,
                    "ShardId": "shardId-00000000000000000000-00000001",
                    "ShardIteratorType": "TRIM_HORIZON",
                }),
            ))
            .await
            .unwrap();
        let it: Value = serde_json::from_slice(it_resp.body.expect_bytes()).unwrap();
        let iterator = it["ShardIterator"].as_str().unwrap().to_string();

        let r1 = svc
            .handle(req(
                "GetRecords",
                json!({"ShardIterator": iterator, "Limit": 3}),
            ))
            .await
            .unwrap();
        let b1: Value = serde_json::from_slice(r1.body.expect_bytes()).unwrap();
        let recs1 = b1["Records"].as_array().unwrap();
        assert_eq!(recs1.len(), 3);
        assert_eq!(recs1[0]["eventID"].as_str().unwrap(), "e1");
        assert_eq!(recs1[2]["eventID"].as_str().unwrap(), "e3");
        let next = b1["NextShardIterator"].as_str().unwrap().to_string();

        // Now retention trims the two aged front records (seq 1,2) off the Vec.
        // An index-based iterator would now mis-resolve and skip/replay; a
        // sequence-anchored one continues correctly with seq 4,5.
        trim_front(&state, 2);

        let r2 = svc
            .handle(req("GetRecords", json!({"ShardIterator": next})))
            .await
            .unwrap();
        let b2: Value = serde_json::from_slice(r2.body.expect_bytes()).unwrap();
        let recs2 = b2["Records"].as_array().unwrap();
        assert_eq!(
            recs2.len(),
            2,
            "must return exactly the un-consumed records after a front trim"
        );
        assert_eq!(recs2[0]["eventID"].as_str().unwrap(), "e4");
        assert_eq!(recs2[1]["eventID"].as_str().unwrap(), "e5");
    }

    #[tokio::test]
    async fn describe_stream_unknown_arn_404s() {
        let state = make_state();
        let _ = seed_table(&state);
        let svc = DynamoDbStreamsService::new(state);
        let err = svc
            .handle(req(
                "DescribeStream",
                json!({"StreamArn": "arn:aws:dynamodb:us-east-1:123456789012:table/missing/stream/x"}),
            ))
            .await
            .err()
            .expect("expected ResourceNotFound");
        assert!(format!("{:?}", err).contains("ResourceNotFoundException"));
    }
}
