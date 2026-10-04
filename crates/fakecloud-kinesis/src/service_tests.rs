use std::sync::Arc;

use bytes::Bytes;
use http::{HeaderMap, Method};
use parking_lot::RwLock;

use super::*;

fn request(action: &str, body: Value) -> AwsRequest {
    AwsRequest {
        service: "kinesis".to_string(),
        action: action.to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "req-1".to_string(),
        headers: HeaderMap::new(),
        query_params: std::collections::HashMap::new(),
        body: Bytes::from(serde_json::to_vec(&body).unwrap()),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: Vec::new(),
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

/// The same request with a different credential-scope region, for the paths
/// that must resolve ARNs region-tolerantly.
fn request_in_region(action: &str, region: &str, body: Value) -> AwsRequest {
    AwsRequest {
        region: region.to_string(),
        ..request(action, body)
    }
}

fn test_stream(name: &str) -> KinesisStream {
    KinesisStream {
        stream_name: name.to_string(),
        stream_arn: format!("arn:aws:kinesis:us-east-1:123456789012:stream/{name}"),
        stream_status: "ACTIVE".to_string(),
        stream_creation_timestamp: Utc::now(),
        retention_period_hours: 24,
        stream_mode: "PROVISIONED".to_string(),
        encryption_type: "NONE".to_string(),
        key_id: None,
        shard_count: 0,
        open_shard_count: 0,
        tags: Default::default(),
        shards: Vec::new(),
        next_shard_index: 0,
        enhanced_metrics: Vec::new(),
        warm_throughput_mibps: None,
        max_record_size_kib: None,
        record_distribution_strategy: crate::state::default_record_distribution_strategy(),
        auto_distribution_cursor: 0,
    }
}

fn test_shard() -> KinesisShard {
    KinesisShard {
        shard_id: "shardId-000000000000".to_string(),
        starting_hash_key: "0".to_string(),
        ending_hash_key: MAX_HASH_KEY.to_string(),
        parent_shard_id: None,
        adjacent_parent_shard_id: None,
        is_open: true,
        next_sequence_number: 1,
        records: Vec::new(),
    }
}

#[test]
fn create_stream_stores_metadata() {
    let state = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiRegionState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        ),
    ));
    let service = KinesisService::new(state.clone());

    service
        .create_stream(&request(
            "CreateStream",
            json!({ "StreamName": "orders", "ShardCount": 2 }),
        ))
        .unwrap();

    let _accts = state.read();
    let st = _accts.default_regional().unwrap();
    let stream = st.streams.get("orders").unwrap();
    assert_eq!(stream.stream_status, "ACTIVE");
    assert_eq!(stream.shard_count, 2);
    assert_eq!(stream.retention_period_hours, 24);
    assert!(stream.stream_arn.ends_with(":stream/orders"));
}

#[test]
fn create_stream_rejects_duplicate_names() {
    let state = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiRegionState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        ),
    ));
    let service = KinesisService::new(state.clone());

    service
        .create_stream(&request(
            "CreateStream",
            json!({ "StreamName": "orders", "ShardCount": 1 }),
        ))
        .unwrap();

    let error = service
        .create_stream(&request(
            "CreateStream",
            json!({ "StreamName": "orders", "ShardCount": 1 }),
        ))
        .err()
        .expect("duplicate stream should fail");
    assert_eq!(error.code(), "ResourceInUseException");
}

#[test]
fn update_retention_period_validates_direction() {
    let state = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiRegionState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        ),
    ));
    let service = KinesisService::new(state.clone());

    service
        .create_stream(&request(
            "CreateStream",
            json!({ "StreamName": "orders", "ShardCount": 1 }),
        ))
        .unwrap();

    let error = service
        .decrease_stream_retention_period(&request(
            "DecreaseStreamRetentionPeriod",
            json!({ "StreamName": "orders", "RetentionPeriodHours": 48 }),
        ))
        .err()
        .expect("invalid retention decrease should fail");
    assert_eq!(error.code(), "InvalidArgumentException");
}

#[test]
fn partition_keys_route_deterministically() {
    // The same partition key always yields the same 128-bit hash.
    let hash_a = partition_key_hash("customer-1");
    let hash_b = partition_key_hash("customer-1");
    assert_eq!(hash_a, hash_b);
}

#[test]
fn partition_key_routes_into_containing_hash_range() {
    // Build a 4-shard stream and verify every partition key lands in the
    // open shard whose [start, end] range contains MD5(partitionKey).
    let shards = build_stream_shards(4);
    let stream = KinesisStream {
        shards,
        ..test_stream("orders")
    };
    for key in ["customer-1", "customer-2", "alpha", "beta", "gamma", "zeta"] {
        let hash = partition_key_hash(key);
        let idx = select_shard_index_for_hash(&stream, hash);
        let (start, end) = shard_hash_range(&stream.shards[idx]);
        assert!(
            hash >= start && hash <= end,
            "key {key} hash {hash} not in shard {idx} range [{start}, {end}]"
        );
        assert!(stream.shards[idx].is_open);
    }
}

#[test]
fn explicit_hash_key_overrides_partition_key() {
    let shards = build_stream_shards(4);
    let mut stream = KinesisStream {
        shards,
        ..test_stream("orders")
    };
    // ExplicitHashKey points at the very top of the keyspace -> last shard.
    let top = MAX_HASH_KEY.to_string();
    let shard = select_shard_mut(&mut stream, "ignored-partition-key", Some(&top)).unwrap();
    assert_eq!(shard.shard_id, "shardId-000000000003");

    // ExplicitHashKey of 0 -> first shard regardless of partition key.
    let shard = select_shard_mut(&mut stream, "ignored-partition-key", Some("0")).unwrap();
    assert_eq!(shard.shard_id, "shardId-000000000000");
}

#[test]
fn routing_skips_closed_shards() {
    let mut shards = build_stream_shards(2);
    // Close the first shard; everything must route to the open one.
    shards[0].is_open = false;
    let mut stream = KinesisStream {
        shards,
        ..test_stream("orders")
    };
    // A hash that falls in the (now closed) first shard's range still routes
    // to an open shard.
    let shard = select_shard_mut(&mut stream, "x", Some("0")).unwrap();
    assert!(shard.is_open);
    assert_eq!(shard.shard_id, "shardId-000000000001");
}

#[test]
fn append_record_advances_sequence_numbers() {
    let mut shard = test_shard();

    let first = append_record(&mut shard, "key", b"first".to_vec());
    let second = append_record(&mut shard, "key", b"second".to_vec());

    // Real Kinesis emits 56-digit decimal sequence numbers; SDKs that
    // bind them as opaque strings rely on the width.
    assert_eq!(first.len(), 56);
    assert_eq!(second.len(), 56);
    assert!(first.ends_with("1"));
    assert!(second.ends_with("2"));
    assert_eq!(shard.records.len(), 2);
}

#[test]
fn trim_horizon_iterator_starts_at_zero() {
    let mut shard = test_shard();
    append_record(&mut shard, "key", b"first".to_vec());

    let index = shard_iterator_start_index(&shard, "TRIM_HORIZON", &json!({})).unwrap();
    assert_eq!(index, 0);
}

#[test]
fn latest_iterator_starts_after_existing_records() {
    let mut shard = test_shard();
    append_record(&mut shard, "key", b"first".to_vec());
    append_record(&mut shard, "key", b"second".to_vec());

    let index = shard_iterator_start_index(&shard, "LATEST", &json!({})).unwrap();
    assert_eq!(index, 2);
}

#[test]
fn at_timestamp_iterator_finds_first_record_at_or_after() {
    let mut shard = test_shard();
    append_record(&mut shard, "key", b"first".to_vec());
    append_record(&mut shard, "key", b"second".to_vec());

    // Stamp the second record well after the first so we can target it.
    let early = chrono::Utc::now() - chrono::Duration::hours(2);
    let later = chrono::Utc::now() - chrono::Duration::minutes(30);
    shard.records[0].approximate_arrival_timestamp = early;
    shard.records[1].approximate_arrival_timestamp = later;

    // Pick a timestamp between the two — must land on record index 1.
    let between = (early + chrono::Duration::hours(1)).timestamp() as f64;
    let index =
        shard_iterator_start_index(&shard, "AT_TIMESTAMP", &json!({"Timestamp": between})).unwrap();
    assert_eq!(index, 1);

    // Timestamp before everything — index 0.
    let before = (early - chrono::Duration::minutes(1)).timestamp() as f64;
    let index =
        shard_iterator_start_index(&shard, "AT_TIMESTAMP", &json!({"Timestamp": before})).unwrap();
    assert_eq!(index, 0);

    // Timestamp after everything — points past the end (empty page).
    let after = (later + chrono::Duration::hours(1)).timestamp() as f64;
    let index =
        shard_iterator_start_index(&shard, "AT_TIMESTAMP", &json!({"Timestamp": after})).unwrap();
    assert_eq!(index, 2);
}

#[test]
fn at_timestamp_iterator_rejects_missing_field() {
    let shard = test_shard();
    let err = shard_iterator_start_index(&shard, "AT_TIMESTAMP", &json!({})).unwrap_err();
    assert_eq!(err.code(), "InvalidArgumentException");
}

// ── Helpers for the expanded test suite ─────────────────────────

fn make_service() -> (KinesisService, SharedKinesisState) {
    let state = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiRegionState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        ),
    ));
    let svc = KinesisService::new(state.clone());
    (svc, state)
}

fn create_stream_action(svc: &KinesisService, name: &str, shards: i64) {
    svc.create_stream(&request(
        "CreateStream",
        json!({ "StreamName": name, "ShardCount": shards }),
    ))
    .unwrap();
}

fn json_response(resp: AwsResponse) -> Value {
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

fn assert_code_kinesis<T>(result: Result<T, AwsServiceError>, expected: &str) -> AwsServiceError {
    match result {
        Ok(_) => panic!("expected error {expected}, got Ok"),
        Err(e) => {
            assert_eq!(e.code(), expected, "wrong error code");
            e
        }
    }
}

// ── DescribeStream / DescribeStreamSummary / ListStreams / DeleteStream ──

#[test]
fn describe_stream_returns_shard_descriptions() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 2);
    let resp = svc
        .describe_stream(&request(
            "DescribeStream",
            json!({ "StreamName": "orders" }),
        ))
        .unwrap();
    let body = json_response(resp);
    let desc = &body["StreamDescription"];
    assert_eq!(desc["StreamName"], json!("orders"));
    assert_eq!(desc["StreamStatus"], json!("ACTIVE"));
    assert_eq!(desc["Shards"].as_array().unwrap().len(), 2);
    assert_eq!(
        desc["StreamModeDetails"]["StreamMode"],
        json!("PROVISIONED")
    );
    assert!(desc["EnhancedMonitoring"].is_array());
    assert_eq!(
        desc["EnhancedMonitoring"][0]["ShardLevelMetrics"],
        json!(Vec::<String>::new())
    );
    assert!(desc.get("KeyId").is_some());
}

#[test]
fn describe_stream_paginates_with_limit_and_exclusive_start() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 5);

    // First page: cap at Limit=2 and report HasMoreShards.
    let resp = svc
        .describe_stream(&request(
            "DescribeStream",
            json!({ "StreamName": "orders", "Limit": 2 }),
        ))
        .unwrap();
    let body = json_response(resp);
    let desc = &body["StreamDescription"];
    let page1 = desc["Shards"].as_array().unwrap();
    assert_eq!(page1.len(), 2, "Limit honored");
    assert_eq!(desc["HasMoreShards"], json!(true), "more shards remain");
    let last_id = page1.last().unwrap()["ShardId"]
        .as_str()
        .unwrap()
        .to_string();

    // Second page: resume after the last returned shard id.
    let resp = svc
        .describe_stream(&request(
            "DescribeStream",
            json!({ "StreamName": "orders", "ExclusiveStartShardId": last_id }),
        ))
        .unwrap();
    let body = json_response(resp);
    let desc = &body["StreamDescription"];
    let page2 = desc["Shards"].as_array().unwrap();
    assert_eq!(page2.len(), 3, "remaining shards returned");
    assert_eq!(desc["HasMoreShards"], json!(false), "no more shards");
}

#[test]
fn list_shards_honors_shard_filter() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 3);

    // AFTER_SHARD_ID drops the named shard and everything before it.
    let resp = svc
        .list_shards(&request(
            "ListShards",
            json!({
                "StreamName": "orders",
                "ShardFilter": { "Type": "AFTER_SHARD_ID", "ShardId": "shardId-000000000000" }
            }),
        ))
        .unwrap();
    let body = json_response(resp);
    let shards = body["Shards"].as_array().unwrap();
    assert_eq!(
        shards.len(),
        2,
        "two shards sort after shardId-000000000000"
    );
    assert!(
        shards
            .iter()
            .all(|s| s["ShardId"].as_str().unwrap() != "shardId-000000000000"),
        "the filtered shard is excluded"
    );

    // AT_LATEST returns the currently-open shards (all of them here).
    let resp = svc
        .list_shards(&request(
            "ListShards",
            json!({ "StreamName": "orders", "ShardFilter": { "Type": "AT_LATEST" } }),
        ))
        .unwrap();
    let body = json_response(resp);
    assert_eq!(body["Shards"].as_array().unwrap().len(), 3);

    // A ShardFilter that requires ShardId but omits it is rejected.
    assert_code_kinesis(
        svc.list_shards(&request(
            "ListShards",
            json!({ "StreamName": "orders", "ShardFilter": { "Type": "AFTER_SHARD_ID" } }),
        )),
        "InvalidArgumentException",
    );
}

#[test]
fn describe_stream_unknown_errors() {
    let (svc, _) = make_service();
    assert_code_kinesis(
        svc.describe_stream(&request("DescribeStream", json!({ "StreamName": "ghost" }))),
        "ResourceNotFoundException",
    );
}

#[test]
fn describe_stream_summary_counts_consumers() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    let resp = svc
        .describe_stream_summary(&request(
            "DescribeStreamSummary",
            json!({ "StreamName": "orders" }),
        ))
        .unwrap();
    let body = json_response(resp);
    assert_eq!(body["StreamDescriptionSummary"]["ConsumerCount"], json!(0));
    assert_eq!(body["StreamDescriptionSummary"]["OpenShardCount"], json!(1));
    // EnhancedMonitoring must be present so the aws_kinesis_stream data source
    // (which reads DescribeStreamSummary) can populate shard_level_metrics.
    assert_eq!(
        body["StreamDescriptionSummary"]["EnhancedMonitoring"][0]["ShardLevelMetrics"],
        json!([])
    );
}

#[test]
fn list_streams_sorts_and_paginates() {
    let (svc, _) = make_service();
    for name in ["charlie", "alpha", "bravo"] {
        create_stream_action(&svc, name, 1);
    }

    // Ask for 2 and expect names in sorted order.
    let resp = svc
        .list_streams(&request("ListStreams", json!({ "Limit": 2 })))
        .unwrap();
    let body = json_response(resp);
    let names: Vec<String> = serde_json::from_value(body["StreamNames"].clone()).unwrap();
    assert_eq!(names, vec!["alpha", "bravo"]);
    assert_eq!(body["HasMoreStreams"], json!(true));

    // Continue after "bravo".
    let resp = svc
        .list_streams(&request(
            "ListStreams",
            json!({ "ExclusiveStartStreamName": "bravo" }),
        ))
        .unwrap();
    let body = json_response(resp);
    let names: Vec<String> = serde_json::from_value(body["StreamNames"].clone()).unwrap();
    assert_eq!(names, vec!["charlie"]);
    assert_eq!(body["HasMoreStreams"], json!(false));
    let summaries = body["StreamSummaries"].as_array().expect("array");
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0]["StreamName"], json!("charlie"));
    assert_eq!(summaries[0]["StreamStatus"], json!("ACTIVE"));
    assert_eq!(
        summaries[0]["StreamModeDetails"]["StreamMode"],
        json!("PROVISIONED")
    );
    assert!(summaries[0]["StreamARN"].is_string());
}

#[test]
fn list_streams_nexttoken_round_trips() {
    // NextToken was validated but never emitted or honored (bug-audit
    // 2026-06-20, 1.14): a token-paging client looped on page one.
    let (svc, _) = make_service();
    for name in ["charlie", "alpha", "bravo"] {
        create_stream_action(&svc, name, 1);
    }

    // First page returns a NextToken alongside HasMoreStreams.
    let resp = svc
        .list_streams(&request("ListStreams", json!({ "Limit": 2 })))
        .unwrap();
    let body = json_response(resp);
    let names: Vec<String> = serde_json::from_value(body["StreamNames"].clone()).unwrap();
    assert_eq!(names, vec!["alpha", "bravo"]);
    assert_eq!(body["HasMoreStreams"], json!(true));
    let token = body["NextToken"]
        .as_str()
        .expect("NextToken present")
        .to_string();

    // Resuming with the token (not ExclusiveStartStreamName) yields the rest
    // and drops NextToken on the final page.
    let resp = svc
        .list_streams(&request("ListStreams", json!({ "NextToken": token })))
        .unwrap();
    let body = json_response(resp);
    let names: Vec<String> = serde_json::from_value(body["StreamNames"].clone()).unwrap();
    assert_eq!(names, vec!["charlie"]);
    assert_eq!(body["HasMoreStreams"], json!(false));
    assert!(body.get("NextToken").is_none());
}

#[test]
fn delete_stream_unknown_errors() {
    let (svc, _) = make_service();
    assert_code_kinesis(
        svc.delete_stream(&request("DeleteStream", json!({ "StreamName": "ghost" }))),
        "ResourceNotFoundException",
    );
}

#[test]
fn delete_stream_removes_entry_and_consumers() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);
    // Register a consumer on the stream.
    let stream_arn = state
        .read()
        .default_regional()
        .unwrap()
        .stream_arn("us-east-1", "orders");
    svc.register_stream_consumer(&request(
        "RegisterStreamConsumer",
        json!({ "StreamARN": stream_arn, "ConsumerName": "c1" }),
    ))
    .unwrap();

    svc.delete_stream(&request("DeleteStream", json!({ "StreamName": "orders" })))
        .unwrap();

    let _accts = state.read();
    let s = _accts.default_regional().unwrap();
    assert!(!s.streams.contains_key("orders"));
    assert!(s.consumers.is_empty());
}

// ── PutRecord / PutRecords / GetRecords ─────────────────────────

#[test]
fn put_record_requires_partition_key_and_data() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    let resp = svc
        .put_record(&request(
            "PutRecord",
            json!({
                "StreamName": "orders",
                "Data": base64::engine::general_purpose::STANDARD.encode(b"hello"),
                "PartitionKey": "k1",
            }),
        ))
        .unwrap();
    let body = json_response(resp);
    assert!(body["ShardId"].as_str().unwrap().starts_with("shardId-"));
    assert!(body["SequenceNumber"].is_string());
}

#[test]
fn put_records_delivers_each_entry_to_a_shard() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 2);
    let records = json!({
        "StreamName": "orders",
        "Records": [
            { "Data": base64::engine::general_purpose::STANDARD.encode(b"a"), "PartitionKey": "k1" },
            { "Data": base64::engine::general_purpose::STANDARD.encode(b"b"), "PartitionKey": "k2" },
        ]
    });
    let resp = svc.put_records(&request("PutRecords", records)).unwrap();
    let body = json_response(resp);
    assert_eq!(body["FailedRecordCount"], json!(0));
    assert_eq!(body["Records"].as_array().unwrap().len(), 2);

    // Verify records landed somewhere.
    let _accts = state.read();
    let s = _accts.default_regional().unwrap();
    let stream = s.streams.get("orders").unwrap();
    let total: usize = stream.shards.iter().map(|sh| sh.records.len()).sum();
    assert_eq!(total, 2);
}

#[test]
fn get_shard_iterator_and_records_happy_path() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);
    // Put a record.
    svc.put_record(&request(
        "PutRecord",
        json!({
            "StreamName": "orders",
            "Data": base64::engine::general_purpose::STANDARD.encode(b"hi"),
            "PartitionKey": "k1",
        }),
    ))
    .unwrap();
    let shard_id = state
        .read()
        .default_regional()
        .unwrap()
        .streams
        .get("orders")
        .unwrap()
        .shards[0]
        .shard_id
        .clone();

    let iter_resp = svc
        .get_shard_iterator(&request(
            "GetShardIterator",
            json!({
                "StreamName": "orders",
                "ShardId": shard_id,
                "ShardIteratorType": "TRIM_HORIZON",
            }),
        ))
        .unwrap();
    let iterator = json_response(iter_resp)["ShardIterator"]
        .as_str()
        .unwrap()
        .to_string();

    let rec_resp = svc
        .get_records(&request("GetRecords", json!({ "ShardIterator": iterator })))
        .unwrap();
    let body = json_response(rec_resp);
    let records = body["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["PartitionKey"], json!("k1"));
    assert!(body["NextShardIterator"].is_string());
}

#[test]
fn get_records_returns_null_iterator_for_closed_drained_shard() {
    // After SplitShard/MergeShards closes a shard and the consumer has read it
    // to the end, GetRecords must return NextShardIterator: null so the consumer
    // advances to the child shard(s). Returning a live iterator forever traps
    // KCL-style consumers on the parent (bug-audit 2026-06-20, 1.7).
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);
    svc.put_record(&request(
        "PutRecord",
        json!({
            "StreamName": "orders",
            "Data": base64::engine::general_purpose::STANDARD.encode(b"hi"),
            "PartitionKey": "k1",
        }),
    ))
    .unwrap();

    // Close the shard, as SplitShard/MergeShards would.
    let shard_id = {
        let mut g = state.write();
        let stream = g.default_regional_mut().streams.get_mut("orders").unwrap();
        stream.shards[0].is_open = false;
        stream.shards[0].shard_id.clone()
    };

    let iter = json_response(
        svc.get_shard_iterator(&request(
            "GetShardIterator",
            json!({
                "StreamName": "orders",
                "ShardId": shard_id,
                "ShardIteratorType": "TRIM_HORIZON",
            }),
        ))
        .unwrap(),
    )["ShardIterator"]
        .as_str()
        .unwrap()
        .to_string();

    // The single record is drained in this call; the shard is closed and fully
    // read, so NextShardIterator must be null.
    let body = json_response(
        svc.get_records(&request("GetRecords", json!({ "ShardIterator": iter })))
            .unwrap(),
    );
    assert_eq!(body["Records"].as_array().unwrap().len(), 1);
    assert!(
        body["NextShardIterator"].is_null(),
        "closed drained shard must return null NextShardIterator, got {:?}",
        body["NextShardIterator"]
    );
}

#[test]
fn get_records_requires_shard_iterator() {
    let (svc, _) = make_service();
    assert_code_kinesis(
        svc.get_records(&request("GetRecords", json!({}))),
        "InvalidArgumentException",
    );
}

#[test]
fn get_records_rejects_unknown_iterator() {
    let (svc, _) = make_service();
    assert_code_kinesis(
        svc.get_records(&request(
            "GetRecords",
            json!({ "ShardIterator": "not-a-real-iterator" }),
        )),
        "ExpiredIteratorException",
    );
}

#[test]
fn get_records_rejects_limit_zero() {
    // Limit < 1 is InvalidArgumentException (validated before iterator
    // resolution) — 1.14.
    let (svc, _) = make_service();
    assert_code_kinesis(
        svc.get_records(&request(
            "GetRecords",
            json!({ "ShardIterator": "any", "Limit": 0 }),
        )),
        "InvalidArgumentException",
    );
}

#[test]
fn get_records_rejects_limit_over_10000() {
    let (svc, _) = make_service();
    assert_code_kinesis(
        svc.get_records(&request(
            "GetRecords",
            json!({ "ShardIterator": "any", "Limit": 20000 }),
        )),
        "InvalidArgumentException",
    );
}

// ── Tags ─────────────────────────────────────────────────────────

#[test]
fn add_list_remove_tags_for_stream() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);

    svc.add_tags_to_stream(&request(
        "AddTagsToStream",
        json!({ "StreamName": "orders", "Tags": { "env": "prod", "team": "core" } }),
    ))
    .unwrap();

    let resp = svc
        .list_tags_for_stream(&request(
            "ListTagsForStream",
            json!({ "StreamName": "orders" }),
        ))
        .unwrap();
    let body = json_response(resp);
    let tags = body["Tags"].as_array().unwrap();
    assert_eq!(tags.len(), 2);

    svc.remove_tags_from_stream(&request(
        "RemoveTagsFromStream",
        json!({ "StreamName": "orders", "TagKeys": ["env"] }),
    ))
    .unwrap();
    let resp = svc
        .list_tags_for_stream(&request(
            "ListTagsForStream",
            json!({ "StreamName": "orders" }),
        ))
        .unwrap();
    let body = json_response(resp);
    let tags = body["Tags"].as_array().unwrap();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0]["Key"], json!("team"));
}

// ── Retention period ────────────────────────────────────────────

#[test]
fn increase_retention_period_bumps_value() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);
    svc.increase_stream_retention_period(&request(
        "IncreaseStreamRetentionPeriod",
        json!({ "StreamName": "orders", "RetentionPeriodHours": 72 }),
    ))
    .unwrap();
    assert_eq!(
        state
            .read()
            .default_regional()
            .unwrap()
            .streams
            .get("orders")
            .unwrap()
            .retention_period_hours,
        72
    );
}

#[test]
fn decrease_retention_period_after_increase() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);
    svc.increase_stream_retention_period(&request(
        "IncreaseStreamRetentionPeriod",
        json!({ "StreamName": "orders", "RetentionPeriodHours": 72 }),
    ))
    .unwrap();
    svc.decrease_stream_retention_period(&request(
        "DecreaseStreamRetentionPeriod",
        json!({ "StreamName": "orders", "RetentionPeriodHours": 48 }),
    ))
    .unwrap();
    assert_eq!(
        state
            .read()
            .default_regional()
            .unwrap()
            .streams
            .get("orders")
            .unwrap()
            .retention_period_hours,
        48
    );
}

#[test]
fn increase_retention_below_current_errors() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    assert_code_kinesis(
        svc.increase_stream_retention_period(&request(
            "IncreaseStreamRetentionPeriod",
            json!({ "StreamName": "orders", "RetentionPeriodHours": 12 }),
        )),
        "InvalidArgumentException",
    );
}

// ── Encryption / monitoring / stream mode ───────────────────────

#[test]
fn start_and_stop_stream_encryption() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);
    svc.start_stream_encryption(&request(
        "StartStreamEncryption",
        json!({
            "StreamName": "orders",
            "EncryptionType": "KMS",
            "KeyId": "alias/aws/kinesis"
        }),
    ))
    .unwrap();
    assert_eq!(
        state
            .read()
            .default_regional()
            .unwrap()
            .streams
            .get("orders")
            .unwrap()
            .encryption_type,
        "KMS"
    );
    svc.stop_stream_encryption(&request(
        "StopStreamEncryption",
        json!({
            "StreamName": "orders",
            "EncryptionType": "KMS",
            "KeyId": "alias/aws/kinesis"
        }),
    ))
    .unwrap();
    assert_eq!(
        state
            .read()
            .default_regional()
            .unwrap()
            .streams
            .get("orders")
            .unwrap()
            .encryption_type,
        "NONE"
    );
}

#[test]
fn enable_and_disable_enhanced_monitoring() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);
    svc.enable_enhanced_monitoring(&request(
        "EnableEnhancedMonitoring",
        json!({
            "StreamName": "orders",
            "ShardLevelMetrics": ["IncomingBytes", "OutgoingBytes"]
        }),
    ))
    .unwrap();
    assert_eq!(
        state
            .read()
            .default_regional()
            .unwrap()
            .streams
            .get("orders")
            .unwrap()
            .enhanced_metrics
            .len(),
        2
    );
    svc.disable_enhanced_monitoring(&request(
        "DisableEnhancedMonitoring",
        json!({
            "StreamName": "orders",
            "ShardLevelMetrics": ["IncomingBytes"]
        }),
    ))
    .unwrap();
    let _accts = state.read();
    let s = _accts.default_regional().unwrap();
    let metrics = &s.streams.get("orders").unwrap().enhanced_metrics;
    assert_eq!(metrics, &vec!["OutgoingBytes".to_string()]);
}

#[test]
fn update_stream_mode_writes_new_mode() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);
    let stream_arn = state
        .read()
        .default_regional()
        .unwrap()
        .stream_arn("us-east-1", "orders");
    svc.update_stream_mode(&request(
        "UpdateStreamMode",
        json!({
            "StreamARN": stream_arn,
            "StreamModeDetails": { "StreamMode": "ON_DEMAND" }
        }),
    ))
    .unwrap();
    assert_eq!(
        state
            .read()
            .default_regional()
            .unwrap()
            .streams
            .get("orders")
            .unwrap()
            .stream_mode,
        "ON_DEMAND"
    );
}

// ── Consumers ────────────────────────────────────────────────────

#[test]
fn register_describe_deregister_consumer() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);
    let stream_arn = state
        .read()
        .default_regional()
        .unwrap()
        .stream_arn("us-east-1", "orders");
    svc.register_stream_consumer(&request(
        "RegisterStreamConsumer",
        json!({ "StreamARN": stream_arn, "ConsumerName": "c1" }),
    ))
    .unwrap();

    let desc = svc
        .describe_stream_consumer(&request(
            "DescribeStreamConsumer",
            json!({ "StreamARN": stream_arn, "ConsumerName": "c1" }),
        ))
        .unwrap();
    let body = json_response(desc);
    assert_eq!(body["ConsumerDescription"]["ConsumerName"], json!("c1"));

    svc.deregister_stream_consumer(&request(
        "DeregisterStreamConsumer",
        json!({ "StreamARN": stream_arn, "ConsumerName": "c1" }),
    ))
    .unwrap();
    assert!(state
        .read()
        .default_regional()
        .unwrap()
        .consumers
        .is_empty());
}

#[test]
fn register_consumer_duplicate_errors() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);
    let stream_arn = state
        .read()
        .default_regional()
        .unwrap()
        .stream_arn("us-east-1", "orders");
    svc.register_stream_consumer(&request(
        "RegisterStreamConsumer",
        json!({ "StreamARN": stream_arn, "ConsumerName": "c1" }),
    ))
    .unwrap();
    assert_code_kinesis(
        svc.register_stream_consumer(&request(
            "RegisterStreamConsumer",
            json!({ "StreamARN": stream_arn, "ConsumerName": "c1" }),
        )),
        "ResourceInUseException",
    );
}

#[test]
fn list_stream_consumers_returns_registered_consumer() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);
    let stream_arn = state
        .read()
        .default_regional()
        .unwrap()
        .stream_arn("us-east-1", "orders");
    svc.register_stream_consumer(&request(
        "RegisterStreamConsumer",
        json!({ "StreamARN": stream_arn, "ConsumerName": "c1" }),
    ))
    .unwrap();
    let resp = svc
        .list_stream_consumers(&request(
            "ListStreamConsumers",
            json!({ "StreamARN": stream_arn }),
        ))
        .unwrap();
    let body = json_response(resp);
    let consumers = body["Consumers"].as_array().unwrap();
    assert_eq!(consumers.len(), 1);
    assert_eq!(consumers[0]["ConsumerName"], json!("c1"));
}

// ── Resource policy ─────────────────────────────────────────────

#[test]
fn put_get_delete_resource_policy() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);
    let stream_arn = state
        .read()
        .default_regional()
        .unwrap()
        .stream_arn("us-east-1", "orders");
    let policy_body = json!({"Version":"2012-10-17","Statement":[]}).to_string();

    svc.put_resource_policy(&request(
        "PutResourcePolicy",
        json!({ "ResourceARN": stream_arn, "Policy": policy_body }),
    ))
    .unwrap();

    let get = svc
        .get_resource_policy(&request(
            "GetResourcePolicy",
            json!({ "ResourceARN": stream_arn }),
        ))
        .unwrap();
    let body = json_response(get);
    assert_eq!(body["Policy"], json!(policy_body));

    svc.delete_resource_policy(&request(
        "DeleteResourcePolicy",
        json!({ "ResourceARN": stream_arn }),
    ))
    .unwrap();
    // After delete, the stream still exists so GetResourcePolicy succeeds
    // with an empty policy string rather than erroring.
    let get = svc
        .get_resource_policy(&request(
            "GetResourcePolicy",
            json!({ "ResourceARN": stream_arn }),
        ))
        .unwrap();
    assert_eq!(json_response(get)["Policy"], json!(""));
}

#[test]
fn get_resource_policy_unknown_stream_errors() {
    let (svc, _) = make_service();
    let bogus = "arn:aws:kinesis:us-east-1:123456789012:stream/ghost";
    assert_code_kinesis(
        svc.get_resource_policy(&request(
            "GetResourcePolicy",
            json!({ "ResourceARN": bogus }),
        )),
        "ResourceNotFoundException",
    );
}

// ── Account settings ────────────────────────────────────────────

#[test]
fn update_account_settings_toggles_billing_commitment() {
    let (svc, state) = make_service();
    svc.update_account_settings(&request(
        "UpdateAccountSettings",
        json!({ "MinimumThroughputBillingCommitment": { "Status": "ENABLED" } }),
    ))
    .unwrap();
    assert_eq!(
        state
            .read()
            .default_regional()
            .unwrap()
            .billing_commitment_status,
        "ENABLED"
    );

    svc.update_account_settings(&request(
        "UpdateAccountSettings",
        json!({ "MinimumThroughputBillingCommitment": { "Status": "DISABLED" } }),
    ))
    .unwrap();
    assert_eq!(
        state
            .read()
            .default_regional()
            .unwrap()
            .billing_commitment_status,
        "DISABLED"
    );
}

#[test]
fn update_account_settings_rejects_invalid_status() {
    let (svc, _) = make_service();
    assert_code_kinesis(
        svc.update_account_settings(&request(
            "UpdateAccountSettings",
            json!({ "MinimumThroughputBillingCommitment": { "Status": "NOPE" } }),
        )),
        "InvalidArgumentException",
    );
}

#[test]
fn insert_iterator_purges_expired_leases() {
    let mut state = crate::state::KinesisState::new("123456789012", "us-east-1");
    state.iterators.insert(
        "expired".to_string(),
        crate::state::ShardIteratorLease {
            iterator_token: "expired".to_string(),
            stream_name: "stream".to_string(),
            shard_id: "shardId-000000000000".to_string(),
            next_record_index: 0,
            expires_at: Utc::now() - chrono::Duration::minutes(1),
        },
    );

    let token = state.insert_iterator("stream", "shardId-000000000000", 0);

    assert!(state.iterators.contains_key(&token));
    assert!(!state.iterators.contains_key("expired"));
}

fn expect_err(result: Result<AwsResponse, AwsServiceError>, code: &str) {
    match result {
        Err(e) => assert!(e.to_string().contains(code), "expected {code}, got: {e}"),
        Ok(_) => panic!("expected error {code}, got Ok"),
    }
}

// ── Error branch tests ──

#[test]
fn describe_stream_not_found() {
    let (svc, _) = make_service();
    expect_err(
        svc.describe_stream(&request("DescribeStream", json!({"StreamName": "ghost"}))),
        "ResourceNotFoundException",
    );
}

#[test]
fn delete_stream_not_found() {
    let (svc, _) = make_service();
    expect_err(
        svc.delete_stream(&request("DeleteStream", json!({"StreamName": "ghost"}))),
        "ResourceNotFoundException",
    );
}

#[test]
fn put_record_stream_not_found() {
    let (svc, _) = make_service();
    expect_err(
        svc.put_record(&request(
            "PutRecord",
            json!({
                "StreamName": "ghost",
                "Data": "aGVsbG8=",
                "PartitionKey": "pk",
            }),
        )),
        "ResourceNotFoundException",
    );
}

#[test]
fn put_records_stream_not_found() {
    let (svc, _) = make_service();
    expect_err(
        svc.put_records(&request(
            "PutRecords",
            json!({
                "StreamName": "ghost",
                "Records": [{"Data": "aGVsbG8=", "PartitionKey": "pk"}],
            }),
        )),
        "ResourceNotFoundException",
    );
}

#[test]
fn get_shard_iterator_stream_not_found() {
    let (svc, _) = make_service();
    expect_err(
        svc.get_shard_iterator(&request(
            "GetShardIterator",
            json!({
                "StreamName": "ghost",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "TRIM_HORIZON",
            }),
        )),
        "ResourceNotFoundException",
    );
}

#[test]
fn add_tags_stream_not_found() {
    let (svc, _) = make_service();
    expect_err(
        svc.add_tags_to_stream(&request(
            "AddTagsToStream",
            json!({
                "StreamName": "ghost",
                "Tags": {"env": "prod"},
            }),
        )),
        "ResourceNotFoundException",
    );
}

#[test]
fn remove_tags_stream_not_found() {
    let (svc, _) = make_service();
    expect_err(
        svc.remove_tags_from_stream(&request(
            "RemoveTagsFromStream",
            json!({
                "StreamName": "ghost",
                "TagKeys": ["env"],
            }),
        )),
        "ResourceNotFoundException",
    );
}

#[test]
fn list_tags_stream_not_found() {
    let (svc, _) = make_service();
    expect_err(
        svc.list_tags_for_stream(&request(
            "ListTagsForStream",
            json!({
                "StreamName": "ghost",
            }),
        )),
        "ResourceNotFoundException",
    );
}

#[test]
fn increase_retention_stream_not_found() {
    let (svc, _) = make_service();
    expect_err(
        svc.increase_stream_retention_period(&request(
            "IncreaseStreamRetentionPeriod",
            json!({
                "StreamName": "ghost",
                "RetentionPeriodHours": 48,
            }),
        )),
        "ResourceNotFoundException",
    );
}

#[test]
fn decrease_retention_stream_not_found() {
    let (svc, _) = make_service();
    expect_err(
        svc.decrease_stream_retention_period(&request(
            "DecreaseStreamRetentionPeriod",
            json!({
                "StreamName": "ghost",
                "RetentionPeriodHours": 24,
            }),
        )),
        "ResourceNotFoundException",
    );
}

#[test]
fn create_stream_duplicate() {
    let (svc, _) = make_service();
    svc.create_stream(&request(
        "CreateStream",
        json!({"StreamName": "dup", "ShardCount": 1}),
    ))
    .unwrap();
    expect_err(
        svc.create_stream(&request(
            "CreateStream",
            json!({"StreamName": "dup", "ShardCount": 1}),
        )),
        "ResourceInUseException",
    );
}

#[test]
fn describe_stream_summary_not_found() {
    let (svc, _) = make_service();
    expect_err(
        svc.describe_stream_summary(&request(
            "DescribeStreamSummary",
            json!({"StreamName": "ghost"}),
        )),
        "ResourceNotFoundException",
    );
}

#[test]
fn get_records_invalid_iterator() {
    let (svc, _) = make_service();
    expect_err(
        svc.get_records(&request(
            "GetRecords",
            json!({"ShardIterator": "invalid-token"}),
        )),
        "ExpiredIteratorException",
    );
}

// ── missing params ──

#[test]
fn describe_stream_missing_name_errors() {
    let (svc, _) = make_service();
    assert!(svc
        .describe_stream(&request("DescribeStream", json!({})))
        .is_err());
}

#[test]
fn describe_stream_summary_missing_name_errors() {
    let (svc, _) = make_service();
    assert!(svc
        .describe_stream_summary(&request("DescribeStreamSummary", json!({})))
        .is_err());
}

#[test]
fn delete_stream_missing_name_errors() {
    let (svc, _) = make_service();
    assert!(svc
        .delete_stream(&request("DeleteStream", json!({})))
        .is_err());
}

#[test]
fn get_shard_iterator_missing_stream_errors() {
    let (svc, _) = make_service();
    assert!(svc
        .get_shard_iterator(&request(
            "GetShardIterator",
            json!({"ShardId": "shardId-000000000000", "ShardIteratorType": "TRIM_HORIZON"})
        ))
        .is_err());
}

#[test]
fn put_record_missing_stream_errors() {
    let (svc, _) = make_service();
    assert!(svc
        .put_record(&request(
            "PutRecord",
            json!({"Data": "aGVsbG8=", "PartitionKey": "k"})
        ))
        .is_err());
}

#[test]
fn start_stream_encryption_missing_stream_errors() {
    let (svc, _) = make_service();
    assert!(svc
        .start_stream_encryption(&request(
            "StartStreamEncryption",
            json!({"EncryptionType": "KMS", "KeyId": "alias/aws/kinesis"})
        ))
        .is_err());
}

#[test]
fn stop_stream_encryption_missing_stream_errors() {
    let (svc, _) = make_service();
    assert!(svc
        .stop_stream_encryption(&request(
            "StopStreamEncryption",
            json!({"EncryptionType": "KMS", "KeyId": "alias/aws/kinesis"})
        ))
        .is_err());
}

#[test]
fn start_stream_encryption_unknown_stream_errors() {
    let (svc, _) = make_service();
    assert!(svc
        .start_stream_encryption(&request(
            "StartStreamEncryption",
            json!({
                "StreamName": "ghost",
                "EncryptionType": "KMS",
                "KeyId": "alias/aws/kinesis"
            })
        ))
        .is_err());
}

#[test]
fn enable_enhanced_monitoring_unknown_stream_errors() {
    let (svc, _) = make_service();
    assert!(svc
        .enable_enhanced_monitoring(&request(
            "EnableEnhancedMonitoring",
            json!({"StreamName": "ghost", "ShardLevelMetrics": ["IncomingBytes"]})
        ))
        .is_err());
}

#[test]
fn disable_enhanced_monitoring_unknown_stream_errors() {
    let (svc, _) = make_service();
    assert!(svc
        .disable_enhanced_monitoring(&request(
            "DisableEnhancedMonitoring",
            json!({"StreamName": "ghost", "ShardLevelMetrics": ["IncomingBytes"]})
        ))
        .is_err());
}

#[test]
fn put_resource_policy_missing_errors() {
    let (svc, _) = make_service();
    assert!(svc
        .put_resource_policy(&request("PutResourcePolicy", json!({})))
        .is_err());
}

#[test]
fn delete_resource_policy_missing_errors() {
    let (svc, _) = make_service();
    assert!(svc
        .delete_resource_policy(&request("DeleteResourcePolicy", json!({})))
        .is_err());
}

#[test]
fn update_retention_below_minimum_errors() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "retlow", 1);
    assert!(svc
        .increase_stream_retention_period(&request(
            "IncreaseStreamRetentionPeriod",
            json!({"StreamName": "retlow", "RetentionPeriodHours": 10})
        ))
        .is_err());
}

#[test]
fn list_streams_empty_returns_zero() {
    let (svc, _) = make_service();
    let resp = svc
        .list_streams(&request("ListStreams", json!({})))
        .unwrap();
    let body = json_response(resp);
    assert!(body["StreamNames"].as_array().unwrap().is_empty());
    assert_eq!(body["HasMoreStreams"], false);
}

#[test]
fn create_stream_missing_name_errors() {
    let (svc, _) = make_service();
    assert!(svc
        .create_stream(&request("CreateStream", json!({})))
        .is_err());
}

#[test]
fn assert_code_kinesis_ok_panics_test() {
    assert_code_kinesis::<()>(
        Err(AwsServiceError::aws_error(
            http::StatusCode::BAD_REQUEST,
            "X",
            "msg",
        )),
        "X",
    );
}

// ── consumer operations ──

#[test]
fn register_consumer_unknown_stream_errors() {
    let (svc, _) = make_service();
    let req = request(
        "RegisterStreamConsumer",
        json!({"StreamARN": "arn:aws:kinesis:us-east-1:123:stream/ghost", "ConsumerName": "c1"}),
    );
    assert!(svc.register_stream_consumer(&req).is_err());
}

#[test]
fn describe_consumer_missing_errors() {
    let (svc, _) = make_service();
    let req = request("DescribeStreamConsumer", json!({}));
    assert!(svc.describe_stream_consumer(&req).is_err());
}

// ── shard operations ──

#[test]
fn list_shards_missing_stream_errors() {
    let (svc, _) = make_service();
    let req = request("ListShards", json!({}));
    assert!(svc.list_shards(&req).is_err());
}

#[test]
fn list_shards_unknown_stream_errors() {
    let (svc, _) = make_service();
    let req = request("ListShards", json!({"StreamName": "ghost"}));
    assert!(svc.list_shards(&req).is_err());
}

#[test]
fn list_shards_next_token_paginates_through_all_shards() {
    let (svc, _) = make_service();
    svc.create_stream(&request(
        "CreateStream",
        json!({ "StreamName": "paged", "ShardCount": 5 }),
    ))
    .unwrap();

    // Page 1: MaxResults=2 -> 2 shards + a NextToken.
    let resp = svc
        .list_shards(&request(
            "ListShards",
            json!({ "StreamName": "paged", "MaxResults": 2 }),
        ))
        .unwrap();
    let v = json_response(resp);
    let page1: Vec<String> = v["Shards"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["ShardId"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(page1.len(), 2);
    let token = v["NextToken"].as_str().expect("NextToken on page 1");

    // Page 2: feed the token back -> next 2 distinct shards.
    let resp = svc
        .list_shards(&request(
            "ListShards",
            json!({ "StreamName": "paged", "MaxResults": 2, "NextToken": token }),
        ))
        .unwrap();
    let v = json_response(resp);
    let page2: Vec<String> = v["Shards"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["ShardId"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(page2.len(), 2);
    let token = v["NextToken"].as_str().expect("NextToken on page 2");
    // Pages must advance, not loop on page 1 (the bug).
    assert!(page2.iter().all(|s| !page1.contains(s)), "pages overlap");

    // Page 3: final shard, no NextToken.
    let resp = svc
        .list_shards(&request(
            "ListShards",
            json!({ "StreamName": "paged", "MaxResults": 2, "NextToken": token }),
        ))
        .unwrap();
    let v = json_response(resp);
    let page3: Vec<String> = v["Shards"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["ShardId"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(page3.len(), 1);
    assert!(v.get("NextToken").is_none() || v["NextToken"].is_null());

    // All 5 shards seen exactly once across the three pages.
    let mut all: Vec<String> = page1;
    all.extend(page2);
    all.extend(page3);
    all.sort();
    all.dedup();
    assert_eq!(all.len(), 5);
}

#[test]
fn list_shards_rejects_garbage_next_token() {
    let (svc, _) = make_service();
    svc.create_stream(&request(
        "CreateStream",
        json!({ "StreamName": "paged", "ShardCount": 2 }),
    ))
    .unwrap();
    let err = svc
        .list_shards(&request(
            "ListShards",
            json!({ "StreamName": "paged", "NextToken": "not-a-real-token" }),
        ))
        .err()
        .expect("garbage NextToken should fail");
    assert_eq!(err.code(), "InvalidArgumentException");
}

#[test]
fn put_record_routes_into_shard_whose_range_contains_the_hash() {
    let (svc, state) = make_service();
    svc.create_stream(&request(
        "CreateStream",
        json!({ "StreamName": "routed", "ShardCount": 4 }),
    ))
    .unwrap();

    for key in ["alpha", "beta", "gamma", "delta", "omega"] {
        let resp = svc
            .put_record(&request(
                "PutRecord",
                json!({
                    "StreamName": "routed",
                    "PartitionKey": key,
                    "Data": base64::engine::general_purpose::STANDARD.encode(b"x"),
                }),
            ))
            .unwrap();
        let v = json_response(resp);
        let shard_id = v["ShardId"].as_str().unwrap();

        // Verify the chosen shard's hash range actually contains MD5(key).
        let hash = partition_key_hash(key);
        let accts = state.read();
        let st = accts.default_regional().unwrap();
        let stream = st.streams.get("routed").unwrap();
        let shard = stream
            .shards
            .iter()
            .find(|s| s.shard_id == shard_id)
            .unwrap();
        let (start, end) = shard_hash_range(shard);
        assert!(
            hash >= start && hash <= end,
            "key {key} hash {hash} routed to shard {shard_id} range [{start},{end}]"
        );
    }
}

#[test]
fn put_record_explicit_hash_key_overrides_partition_key() {
    let (svc, _state) = make_service();
    svc.create_stream(&request(
        "CreateStream",
        json!({ "StreamName": "ehk", "ShardCount": 4 }),
    ))
    .unwrap();

    // ExplicitHashKey=0 must land in the first shard regardless of key.
    let resp = svc
        .put_record(&request(
            "PutRecord",
            json!({
                "StreamName": "ehk",
                "PartitionKey": "any-key",
                "ExplicitHashKey": "0",
                "Data": base64::engine::general_purpose::STANDARD.encode(b"x"),
            }),
        ))
        .unwrap();
    let v = json_response(resp);
    assert_eq!(v["ShardId"].as_str().unwrap(), "shardId-000000000000");
}

#[test]
fn split_shard_unknown_stream_errors() {
    let (svc, _) = make_service();
    let req = request(
        "SplitShard",
        json!({
            "StreamName": "ghost",
            "ShardToSplit": "shardId-000000000000",
            "NewStartingHashKey": "1"
        }),
    );
    assert!(svc.split_shard(&req).is_err());
}

#[test]
fn merge_shards_unknown_stream_errors() {
    let (svc, _) = make_service();
    let req = request(
        "MergeShards",
        json!({
            "StreamName": "ghost",
            "ShardToMerge": "shardId-000000000000",
            "AdjacentShardToMerge": "shardId-000000000001"
        }),
    );
    assert!(svc.merge_shards(&req).is_err());
}

#[test]
fn update_shard_count_unknown_stream_errors() {
    let (svc, _) = make_service();
    let req = request(
        "UpdateShardCount",
        json!({
            "StreamName": "ghost",
            "TargetShardCount": 4,
            "ScalingType": "UNIFORM_SCALING"
        }),
    );
    assert!(svc.update_shard_count(&req).is_err());
}

// ── tags ──

#[test]
fn add_tags_missing_stream_errors() {
    let (svc, _) = make_service();
    let req = request("AddTagsToStream", json!({"Tags": {"env": "prod"}}));
    assert!(svc.add_tags_to_stream(&req).is_err());
}

#[test]
fn remove_tags_missing_stream_errors() {
    let (svc, _) = make_service();
    let req = request("RemoveTagsFromStream", json!({"TagKeys": ["env"]}));
    assert!(svc.remove_tags_from_stream(&req).is_err());
}

#[test]
fn list_tags_missing_stream_errors() {
    let (svc, _) = make_service();
    let req = request("ListTagsForStream", json!({}));
    assert!(svc.list_tags_for_stream(&req).is_err());
}

// ── resource policy ──

#[test]
fn get_resource_policy_missing_arn_errors() {
    let (svc, _) = make_service();
    let req = request("GetResourcePolicy", json!({}));
    assert!(svc.get_resource_policy(&req).is_err());
}

// ── describe_limits + account ──

#[test]
fn describe_limits_returns_ok() {
    let (svc, _) = make_service();
    let req = request("DescribeLimits", json!({}));
    let resp = svc.describe_limits(&req).unwrap();
    let body = json_response(resp);
    assert!(body["ShardLimit"].is_i64() || body["ShardLimit"].is_u64());
}

#[test]
fn describe_account_settings_returns_ok() {
    let (svc, _) = make_service();
    let req = request("DescribeAccountSettings", json!({}));
    let resp = svc.describe_account_settings(&req).unwrap();
    let body = json_response(resp);
    assert!(body.is_object());
}

#[test]
fn update_stream_mode_unknown_stream_errors() {
    let (svc, _) = make_service();
    let req = request(
        "UpdateStreamMode",
        json!({
            "StreamARN": "arn:aws:kinesis:us-east-1:123:stream/ghost",
            "StreamModeDetails": {"StreamMode": "ON_DEMAND"}
        }),
    );
    assert!(svc.update_stream_mode(&req).is_err());
}

#[test]
fn list_streams_with_limit() {
    let (svc, _) = make_service();
    for i in 0..5 {
        create_stream_action(&svc, &format!("s{i}"), 1);
    }
    let req = request("ListStreams", json!({"Limit": 2}));
    let resp = svc.list_streams(&req).unwrap();
    let body = json_response(resp);
    assert_eq!(body["StreamNames"].as_array().unwrap().len(), 2);
}

#[test]
fn list_streams_with_exclusive_start_stream_name() {
    let (svc, _) = make_service();
    for i in 0..3 {
        create_stream_action(&svc, &format!("s{i}"), 1);
    }
    let req = request("ListStreams", json!({"ExclusiveStartStreamName": "s0"}));
    let resp = svc.list_streams(&req).unwrap();
    let body = json_response(resp);
    let names: Vec<String> = body["StreamNames"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert!(!names.contains(&"s0".to_string()));
}

#[test]
fn put_records_missing_records_errors() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "prs", 1);
    let req = request("PutRecords", json!({"StreamName": "prs"}));
    assert!(svc.put_records(&req).is_err());
}

#[test]
fn put_record_missing_data_errors() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "pmd", 1);
    let req = request(
        "PutRecord",
        json!({"StreamName": "pmd", "PartitionKey": "k"}),
    );
    assert!(svc.put_record(&req).is_err());
}

#[test]
fn decrease_retention_unknown_stream_errors() {
    let (svc, _) = make_service();
    let req = request(
        "DecreaseStreamRetentionPeriod",
        json!({"StreamName": "ghost", "RetentionPeriodHours": 24}),
    );
    assert!(svc.decrease_stream_retention_period(&req).is_err());
}

#[test]
fn stop_stream_encryption_unknown_stream_errors() {
    let (svc, _) = make_service();
    let req = request(
        "StopStreamEncryption",
        json!({
            "StreamName": "ghost",
            "EncryptionType": "KMS",
            "KeyId": "alias/aws/kinesis"
        }),
    );
    assert!(svc.stop_stream_encryption(&req).is_err());
}

// ── K13: StreamModeDetails + retention pruning ──

#[test]
fn create_stream_honors_on_demand_mode() {
    let (svc, state) = make_service();
    svc.create_stream(&request(
        "CreateStream",
        json!({
            "StreamName": "demand",
            "StreamModeDetails": {"StreamMode": "ON_DEMAND"}
        }),
    ))
    .unwrap();
    let _accts = state.read();
    let st = _accts.default_regional().unwrap();
    let stream = st.streams.get("demand").unwrap();
    assert_eq!(stream.stream_mode, "ON_DEMAND");
    // ON_DEMAND ignores ShardCount; we seed a small fixed count.
    assert!(stream.shard_count >= 1);
}

#[test]
fn create_stream_rejects_unknown_stream_mode() {
    let (svc, _) = make_service();
    let err = svc
        .create_stream(&request(
            "CreateStream",
            json!({
                "StreamName": "bogus",
                "StreamModeDetails": {"StreamMode": "TURBO"}
            }),
        ))
        .err()
        .expect("expected invalid argument");
    assert!(format!("{:?}", err).contains("StreamMode"));
}

#[test]
fn create_stream_defaults_to_provisioned_mode() {
    let (svc, state) = make_service();
    svc.create_stream(&request(
        "CreateStream",
        json!({"StreamName": "default-mode", "ShardCount": 1}),
    ))
    .unwrap();
    let _accts = state.read();
    let st = _accts.default_regional().unwrap();
    let stream = st.streams.get("default-mode").unwrap();
    assert_eq!(stream.stream_mode, "PROVISIONED");
}

#[test]
fn get_records_skips_records_past_retention() {
    let (svc, state) = make_service();
    svc.create_stream(&request(
        "CreateStream",
        json!({"StreamName": "ret", "ShardCount": 1}),
    ))
    .unwrap();

    // Push two records: one stale (well past retention), one fresh.
    {
        let mut accts = state.write();
        let st = accts.default_regional_mut();
        let stream = st.streams.get_mut("ret").unwrap();
        let shard = &mut stream.shards[0];
        let stale_ts = chrono::Utc::now() - chrono::Duration::hours(48);
        shard.records.push(KinesisRecord {
            sequence_number: format!("{:056}", 1),
            partition_key: "p".to_string(),
            data: b"stale".to_vec(),
            approximate_arrival_timestamp: stale_ts,
        });
        shard.records.push(KinesisRecord {
            sequence_number: format!("{:056}", 2),
            partition_key: "p".to_string(),
            data: b"fresh".to_vec(),
            approximate_arrival_timestamp: chrono::Utc::now(),
        });
    }

    let it_resp = svc
        .get_shard_iterator(&request(
            "GetShardIterator",
            json!({
                "StreamName": "ret",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "TRIM_HORIZON"
            }),
        ))
        .unwrap();
    let it_body: Value = serde_json::from_slice(it_resp.body.expect_bytes()).unwrap();
    let iterator = it_body["ShardIterator"].as_str().unwrap().to_string();

    let resp = svc
        .get_records(&request("GetRecords", json!({"ShardIterator": iterator})))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let records = body["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1);
    let data_b64 = records[0]["Data"].as_str().unwrap();
    let data = base64::engine::general_purpose::STANDARD
        .decode(data_b64)
        .unwrap();
    assert_eq!(data, b"fresh");
}

/// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
#[test]
fn snapshot_hook_is_none_without_store() {
    let (svc, _state) = make_service();
    assert!(svc.snapshot_hook().is_none());
}

/// With a store, the hook is present and invoking it runs the whole-state
/// persist path the CloudFormation provisioner uses after mutating Kinesis
/// state directly.
#[tokio::test]
async fn snapshot_hook_fires_with_store() {
    let store: Arc<dyn fakecloud_persistence::SnapshotStore> =
        Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
    let (svc, _state) = make_service();
    let svc = svc.with_snapshot_store(store);
    let hook = svc
        .snapshot_hook()
        .expect("hook present when a store is set");
    // Must not panic; exercises the closure and the snapshot save path.
    hook().await;
}

#[test]
fn create_stream_persists_initial_tags() {
    // CreateStream accepts an initial Tags map; Terraform's aws_kinesis_stream
    // sets tags at create and treats a missing tag on the next read as drift.
    let (svc, _) = make_service();
    svc.create_stream(&request(
        "CreateStream",
        json!({ "StreamName": "tagged", "ShardCount": 1, "Tags": { "Name": "tagged" } }),
    ))
    .unwrap();
    let resp = svc
        .list_tags_for_stream(&request(
            "ListTagsForStream",
            json!({ "StreamName": "tagged" }),
        ))
        .unwrap();
    let body = json_response(resp);
    let tags = body["Tags"].as_array().unwrap();
    assert!(
        tags.iter()
            .any(|t| t["Key"] == "Name" && t["Value"] == "tagged"),
        "initial CreateStream tags should persist, got {body}"
    );
}

// ── H1: record-size / batch-count / batch-size limits ──

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[test]
fn put_record_rejects_payload_over_one_mib() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    // Data alone is 1 MiB + 1 byte, over the 1 MiB (Data + PartitionKey) limit.
    let oversized = vec![b'x'; 1024 * 1024 + 1];
    let res = svc.put_record(&request(
        "PutRecord",
        json!({
            "StreamName": "orders",
            "Data": b64(&oversized),
            "PartitionKey": "k1",
        }),
    ));
    assert_code_kinesis(res, "ValidationException");
}

#[test]
fn put_record_within_limit_is_accepted() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    // 512 KiB is comfortably under the 1 MiB ceiling.
    let ok = vec![b'x'; 512 * 1024];
    svc.put_record(&request(
        "PutRecord",
        json!({
            "StreamName": "orders",
            "Data": b64(&ok),
            "PartitionKey": "k1",
        }),
    ))
    .unwrap();
}

#[test]
fn put_records_rejects_more_than_500_records() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    let entries: Vec<Value> = (0..501)
        .map(|i| json!({ "Data": b64(b"a"), "PartitionKey": format!("k{i}") }))
        .collect();
    let res = svc.put_records(&request(
        "PutRecords",
        json!({ "StreamName": "orders", "Records": entries }),
    ));
    assert_code_kinesis(res, "ValidationException");
}

#[test]
fn put_records_rejects_aggregate_over_five_mib() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    // 6 records of ~1 MiB each = ~6 MiB aggregate, over the 5 MiB batch limit
    // while each individual record stays within the 1 MiB per-record ceiling.
    let chunk = vec![b'x'; 1000 * 1024];
    let entries: Vec<Value> = (0..6)
        .map(|i| json!({ "Data": b64(&chunk), "PartitionKey": format!("k{i}") }))
        .collect();
    let res = svc.put_records(&request(
        "PutRecords",
        json!({ "StreamName": "orders", "Records": entries }),
    ));
    assert_code_kinesis(res, "ValidationException");
}

#[test]
fn put_record_honors_configured_max_record_size() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);
    // Raise the per-record ceiling to 2 MiB for this stream.
    state
        .write()
        .default_regional_mut()
        .streams
        .get_mut("orders")
        .unwrap()
        .max_record_size_kib = Some(2048);
    // 1.5 MiB is over the 1 MiB default but under the configured 2 MiB.
    let payload = vec![b'x'; 1536 * 1024];
    svc.put_record(&request(
        "PutRecord",
        json!({
            "StreamName": "orders",
            "Data": b64(&payload),
            "PartitionKey": "k1",
        }),
    ))
    .unwrap();
}

// ── M3: PartitionKey length ──

#[test]
fn put_record_rejects_partition_key_over_256_chars() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    let long_key = "p".repeat(257);
    let res = svc.put_record(&request(
        "PutRecord",
        json!({
            "StreamName": "orders",
            "Data": b64(b"a"),
            "PartitionKey": long_key,
        }),
    ));
    assert_code_kinesis(res, "ValidationException");
}

#[test]
fn put_records_reports_long_partition_key_as_per_record_failure() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    let long_key = "p".repeat(257);
    let resp = svc
        .put_records(&request(
            "PutRecords",
            json!({
                "StreamName": "orders",
                "Records": [
                    { "Data": b64(b"a"), "PartitionKey": "ok" },
                    { "Data": b64(b"b"), "PartitionKey": long_key },
                ]
            }),
        ))
        .unwrap();
    let body = json_response(resp);
    assert_eq!(body["FailedRecordCount"], json!(1));
    let records = body["Records"].as_array().unwrap();
    assert!(records[0].get("SequenceNumber").is_some());
    assert!(records[1].get("ErrorCode").is_some());
}

// ── M2: read ops are not durable-mutating ──

#[test]
fn read_ops_are_not_mutating_actions() {
    // GetRecords / GetShardIterator only touch the ephemeral (serde-skipped)
    // iterator lease map, so they must not trigger a full-state snapshot save.
    assert!(!is_mutating_action("GetRecords"));
    assert!(!is_mutating_action("GetShardIterator"));
    // Writes still are.
    assert!(is_mutating_action("PutRecord"));
    assert!(is_mutating_action("PutRecords"));
}

// ── M1: UpdateShardCount lineage ──

#[test]
fn update_shard_count_preserves_parent_lineage() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 2);

    // Capture the pre-scale open shard ids and force a record onto shard 0
    // (ExplicitHashKey 0 always routes to the shard covering hash 0).
    let original_ids: Vec<String> = {
        let g = state.read();
        g.default_regional()
            .unwrap()
            .streams
            .get("orders")
            .unwrap()
            .shards
            .iter()
            .map(|s| s.shard_id.clone())
            .collect()
    };
    svc.put_record(&request(
        "PutRecord",
        json!({
            "StreamName": "orders",
            "Data": b64(b"hi"),
            "PartitionKey": "k1",
            "ExplicitHashKey": "0",
        }),
    ))
    .unwrap();

    svc.update_shard_count(&request(
        "UpdateShardCount",
        json!({
            "StreamName": "orders",
            "TargetShardCount": 4,
            "ScalingType": "UNIFORM_SCALING",
        }),
    ))
    .unwrap();

    // Every original shard must now be a parent of at least one new shard, so
    // consumers can discover the post-scale shards from the closed originals.
    {
        let g = state.read();
        let stream = g.default_regional().unwrap().streams.get("orders").unwrap();
        for original in &original_ids {
            let is_parent = stream.shards.iter().any(|s| {
                s.parent_shard_id.as_deref() == Some(original.as_str())
                    || s.adjacent_parent_shard_id.as_deref() == Some(original.as_str())
            });
            assert!(
                is_parent,
                "original shard {original} has no child after scaling"
            );
        }
    }

    // GetRecords draining a closed original returns non-empty ChildShards.
    let closed_original = &original_ids[0];
    let iter = json_response(
        svc.get_shard_iterator(&request(
            "GetShardIterator",
            json!({
                "StreamName": "orders",
                "ShardId": closed_original,
                "ShardIteratorType": "TRIM_HORIZON",
            }),
        ))
        .unwrap(),
    )["ShardIterator"]
        .as_str()
        .unwrap()
        .to_string();
    let body = json_response(
        svc.get_records(&request("GetRecords", json!({ "ShardIterator": iter })))
            .unwrap(),
    );
    let children = body["ChildShards"].as_array();
    assert!(
        children.map(|c| !c.is_empty()).unwrap_or(false),
        "closed original shard should report ChildShards, got {body}"
    );
}

// ── L1: below-horizon sequence number resolves to trim horizon ──

#[test]
fn at_sequence_number_below_trim_horizon_resolves_to_earliest() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);

    // Two records; capture the first sequence number, then simulate retention
    // trimming it away so only the second remains.
    let first_seq = json_response(
        svc.put_record(&request(
            "PutRecord",
            json!({ "StreamName": "orders", "Data": b64(b"one"), "PartitionKey": "k" }),
        ))
        .unwrap(),
    )["SequenceNumber"]
        .as_str()
        .unwrap()
        .to_string();
    svc.put_record(&request(
        "PutRecord",
        json!({ "StreamName": "orders", "Data": b64(b"two"), "PartitionKey": "k" }),
    ))
    .unwrap();
    {
        let mut g = state.write();
        let stream = g.default_regional_mut().streams.get_mut("orders").unwrap();
        stream.shards[0].records.remove(0); // drop the trimmed record
    }

    // AT_SEQUENCE_NUMBER on the trimmed seq must resolve to the earliest
    // available record instead of raising InvalidArgumentException.
    let iter = json_response(
        svc.get_shard_iterator(&request(
            "GetShardIterator",
            json!({
                "StreamName": "orders",
                "ShardId": "shardId-000000000000",
                "ShardIteratorType": "AT_SEQUENCE_NUMBER",
                "StartingSequenceNumber": first_seq,
            }),
        ))
        .unwrap(),
    )["ShardIterator"]
        .as_str()
        .unwrap()
        .to_string();
    let body = json_response(
        svc.get_records(&request("GetRecords", json!({ "ShardIterator": iter })))
            .unwrap(),
    );
    let records = body["Records"].as_array().unwrap();
    assert_eq!(records.len(), 1, "resolves to the surviving record");
}

// A sequence number minted by a *different* shard (its packed discriminator
// doesn't match) must raise InvalidArgumentException even when it sorts below
// this shard's earliest record, instead of silently resolving to trim horizon.
#[test]
fn at_sequence_number_from_another_shard_is_invalid() {
    let (svc, state) = make_service();
    svc.create_stream(&request(
        "CreateStream",
        json!({"StreamName": "xshard", "ShardCount": 2}),
    ))
    .unwrap();

    // Seed shard 1 (discriminator 1) with a record directly.
    {
        let mut g = state.write();
        let stream = g.default_regional_mut().streams.get_mut("xshard").unwrap();
        stream.shards[1].records.push(KinesisRecord {
            sequence_number: format!("{:05}{:051}", 1, 10),
            partition_key: "p".to_string(),
            data: b"one".to_vec(),
            approximate_arrival_timestamp: chrono::Utc::now(),
        });
    }

    // A token with shard-0's discriminator sorts below shard 1's record but was
    // never minted by shard 1.
    let foreign_seq = format!("{:05}{:051}", 0, 5);
    let err = match svc.get_shard_iterator(&request(
        "GetShardIterator",
        json!({
            "StreamName": "xshard",
            "ShardId": "shardId-000000000001",
            "ShardIteratorType": "AT_SEQUENCE_NUMBER",
            "StartingSequenceNumber": foreign_seq,
        }),
    )) {
        Ok(_) => panic!("expected InvalidArgumentException"),
        Err(e) => e,
    };
    assert_eq!(err.code(), "InvalidArgumentException");
}

// ── L2: shard-iterator tokens are unique per insert ──

#[test]
fn insert_iterator_tokens_are_distinct_after_eviction() {
    let mut st = KinesisState::new("123456789012", "us-east-1");
    let t1 = st.insert_iterator("s", "shardId-000000000000", 0);
    // Simulate the lease being evicted so the map size returns to its prior
    // value — the exact case where the old `iterators.len()` tie-breaker
    // produced a duplicate token within the same millisecond.
    st.iterators.clear();
    let t2 = st.insert_iterator("s", "shardId-000000000000", 0);
    assert_ne!(t1, t2, "same-ms iterator tokens must not collide");
}

// ── L3: ListStreams resumes correctly after the cursor stream is deleted ──

#[test]
fn list_streams_resumes_after_deleted_cursor() {
    let (svc, _) = make_service();
    for name in ["a", "b", "c", "d"] {
        create_stream_action(&svc, name, 1);
    }
    // First page of two returns [a, b] with a NextToken keyed on "b".
    let page1 = json_response(
        svc.list_streams(&request("ListStreams", json!({ "Limit": 2 })))
            .unwrap(),
    );
    assert_eq!(page1["StreamNames"], json!(["a", "b"]));
    let token = page1["NextToken"].as_str().unwrap().to_string();

    // Delete the cursor stream "b" before resuming.
    svc.delete_stream(&request("DeleteStream", json!({ "StreamName": "b" })))
        .unwrap();

    let page2 = json_response(
        svc.list_streams(&request("ListStreams", json!({ "NextToken": token })))
            .unwrap(),
    );
    assert_eq!(
        page2["StreamNames"],
        json!(["c", "d"]),
        "resume must continue past the deleted cursor, not restart"
    );
}

// ── Cheap guard: routing on a shard-less stream errors instead of panicking ──

#[test]
fn put_record_on_shardless_stream_errors_without_panic() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);
    // Force the (unreachable-via-API) shard-less state.
    state
        .write()
        .default_regional_mut()
        .streams
        .get_mut("orders")
        .unwrap()
        .shards
        .clear();
    let res = svc.put_record(&request(
        "PutRecord",
        json!({ "StreamName": "orders", "Data": b64(b"a"), "PartitionKey": "k" }),
    ));
    assert_code_kinesis(res, "InvalidArgumentException");
}

// ── channel operations ──

fn stream_arn_for(name: &str) -> String {
    format!("arn:aws:kinesis:us-east-1:123456789012:stream/{name}")
}

/// A minimal-but-valid CreateChannel body with a general purpose S3
/// destination: one source stream, no dead-letter queue, no logging and no
/// encryption, so the defaults are the thing under test.
fn s3_channel_body(name: &str, stream_name: &str) -> Value {
    json!({
        "ChannelName": name,
        "ServiceExecutionRoleARN": "arn:aws:iam::123456789012:role/channel",
        "StreamConfigurationList": [{
            "StreamARN": stream_arn_for(stream_name),
            "RecordConfiguration": { "RecordFormatType": "JSON" },
        }],
        "S3DestinationConfiguration": {
            "StorageConfiguration": {
                "BucketARN": "arn:aws:s3:::channel-bucket",
                "ExpectedBucketOwner": "123456789012",
                "CompressionType": "ZSTD",
            }
        },
    })
}

fn create_channel_action(svc: &KinesisService, name: &str, stream_name: &str) -> Value {
    json_response(
        svc.create_channel(&request(
            "CreateChannel",
            s3_channel_body(name, stream_name),
        ))
        .unwrap(),
    )
}

#[test]
fn create_channel_returns_active_description_with_defaults() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);

    let created = create_channel_action(&svc, "deliveries", "orders");
    let description = &created["ChannelDescription"];

    assert_eq!(description["ChannelName"], "deliveries");
    let channel_id = description["ChannelId"].as_str().unwrap();
    assert!(!channel_id.is_empty());
    // AWS keys the channel ARN off the channel id, not the channel name.
    assert_eq!(
        description["ChannelARN"],
        format!("arn:aws:kinesis:us-east-1:123456789012:channel/{channel_id}")
    );
    // Fakecloud provisions synchronously, so the channel never sits in CREATING.
    assert_eq!(description["ChannelStatus"], "ACTIVE");
    assert!(description["ChannelCreationTimestamp"].as_f64().unwrap() > 0.0);
    assert_eq!(
        description["StreamConfigurationList"][0]["StreamARN"],
        stream_arn_for("orders")
    );
    assert_eq!(
        description["StreamConfigurationList"][0]["RecordConfiguration"]["RecordFormatType"],
        "JSON"
    );

    let s3 = &description["S3DestinationConfiguration"];
    assert_eq!(s3["DataFreshnessInSeconds"], 300);
    assert_eq!(s3["StorageConfiguration"]["StorageClass"], "STANDARD");
    assert_eq!(
        s3["StorageConfiguration"]["OutputKeyTemplate"],
        DEFAULT_CHANNEL_OUTPUT_KEY_TEMPLATE
    );
    // An omitted dead-letter queue defaults to the destination bucket under
    // the channel's error prefix.
    let channel_id = description["ChannelId"].as_str().unwrap();
    assert_eq!(
        s3["DeadLetterQueueS3Configuration"]["BucketARN"],
        "arn:aws:s3:::channel-bucket"
    );
    assert_eq!(
        s3["DeadLetterQueueS3Configuration"]["ErrorOutputPrefix"],
        format!("kinesis-channel/errors/deliveries/{channel_id}/")
    );

    let logs = &description["LoggingConfiguration"]["CloudWatchLogs"];
    assert_eq!(logs["Enabled"], false);
    assert_eq!(
        logs["LogGroupName"],
        format!("/aws/kinesis/deliveries/{channel_id}")
    );
    assert_eq!(logs["LogStreamName"], "DestinationDelivery");
    assert!(description["EncryptionConfiguration"].is_null());
}

#[test]
fn create_channel_stores_tags_and_encryption() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);

    let mut body = s3_channel_body("deliveries", "orders");
    body["Tags"] = json!({ "env": "prod" });
    body["EncryptionConfiguration"] = json!({
        "EncryptionType": "KMS",
        "KeyId": "arn:aws:kms:us-east-1:123456789012:key/abc",
    });
    body["LoggingConfiguration"] = json!({
        "CloudWatchLogs": { "Enabled": true, "LogGroupName": "/aws/kinesis/custom" }
    });
    let created = json_response(svc.create_channel(&request("CreateChannel", body)).unwrap());

    assert_eq!(
        created["ChannelDescription"]["EncryptionConfiguration"]["KeyId"],
        "arn:aws:kms:us-east-1:123456789012:key/abc"
    );
    let logs = &created["ChannelDescription"]["LoggingConfiguration"]["CloudWatchLogs"];
    assert_eq!(logs["Enabled"], true);
    assert_eq!(logs["LogGroupName"], "/aws/kinesis/custom");
    assert_eq!(logs["LogStreamName"], "DestinationDelivery");

    let guard = state.read();
    let stored = &guard.default_regional().unwrap().channels["deliveries"];
    assert_eq!(stored.tags["env"], "prod");
}

#[test]
fn create_channel_accepts_s3_tables_destination() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);

    let body = json!({
        "ChannelName": "iceberg",
        "ServiceExecutionRoleARN": "arn:aws:iam::123456789012:role/channel",
        "StreamConfigurationList": [{
            "StreamARN": stream_arn_for("orders"),
            "RecordConfiguration": {
                "RecordFormatType": "GSR_JSON",
                "GSRSchemaARN": "arn:aws:glue:us-east-1:123456789012:schema/s",
            },
        }],
        "S3TablesDestinationConfiguration": {
            "DataFreshnessInSeconds": 600,
            "DeadLetterQueueS3Configuration": {
                "BucketARN": "arn:aws:s3:::dlq-bucket",
                "ExpectedBucketOwner": "123456789012",
            },
            "S3TablesConfigurationList": [{
                "TableBucketARN": "arn:aws:s3tables:us-east-1:123456789012:bucket/tables",
                "Namespace": "analytics",
                "TableName": "events",
                "CompressionType": "SNAPPY",
                "PartitionSpec": {
                    "PartitionFields": [{ "Transform": "TIME_HOUR", "SourceName": "ts" }]
                },
            }],
        },
    });
    let created = json_response(svc.create_channel(&request("CreateChannel", body)).unwrap());
    let description = &created["ChannelDescription"];

    let tables = &description["S3TablesDestinationConfiguration"];
    assert_eq!(tables["DataFreshnessInSeconds"], 600);
    assert_eq!(
        tables["S3TablesConfigurationList"][0]["TableName"],
        "events"
    );
    assert_eq!(
        tables["S3TablesConfigurationList"][0]["PartitionSpec"]["PartitionFields"][0]["Transform"],
        "TIME_HOUR"
    );
    assert!(description["S3DestinationConfiguration"].is_null());
    assert_eq!(
        description["StreamConfigurationList"][0]["RecordConfiguration"]["GSRSchemaARN"],
        "arn:aws:glue:us-east-1:123456789012:schema/s"
    );
}

#[test]
fn create_channel_rejects_duplicate_name() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    create_channel_action(&svc, "deliveries", "orders");

    let res = svc.create_channel(&request(
        "CreateChannel",
        s3_channel_body("deliveries", "orders"),
    ));
    assert_code_kinesis(res, "ResourceInUseException");
}

#[test]
fn create_channel_requires_an_existing_source_stream() {
    let (svc, _) = make_service();
    let res = svc.create_channel(&request(
        "CreateChannel",
        s3_channel_body("deliveries", "ghost"),
    ));
    assert_code_kinesis(res, "ResourceNotFoundException");
}

#[test]
fn create_channel_requires_required_members() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);

    let mut missing_name = s3_channel_body("deliveries", "orders");
    missing_name["ChannelName"] = Value::Null;
    assert_code_kinesis(
        svc.create_channel(&request("CreateChannel", missing_name)),
        "InvalidArgumentException",
    );

    let mut missing_role = s3_channel_body("deliveries", "orders");
    missing_role["ServiceExecutionRoleARN"] = Value::Null;
    assert_code_kinesis(
        svc.create_channel(&request("CreateChannel", missing_role)),
        "InvalidArgumentException",
    );

    let mut missing_streams = s3_channel_body("deliveries", "orders");
    missing_streams["StreamConfigurationList"] = Value::Null;
    assert_code_kinesis(
        svc.create_channel(&request("CreateChannel", missing_streams)),
        "InvalidArgumentException",
    );

    let mut empty_streams = s3_channel_body("deliveries", "orders");
    empty_streams["StreamConfigurationList"] = json!([]);
    assert_code_kinesis(
        svc.create_channel(&request("CreateChannel", empty_streams)),
        "ValidationException",
    );

    // ChannelName is ^[a-zA-Z0-9_.-]+$, so a space is a pattern violation.
    let bad_name = s3_channel_body("deliver ies", "orders");
    assert_code_kinesis(
        svc.create_channel(&request("CreateChannel", bad_name)),
        "ValidationException",
    );
}

#[test]
fn create_channel_requires_exactly_one_destination() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);

    let mut none = s3_channel_body("deliveries", "orders");
    none["S3DestinationConfiguration"] = Value::Null;
    assert_code_kinesis(
        svc.create_channel(&request("CreateChannel", none)),
        "InvalidArgumentException",
    );

    let mut both = s3_channel_body("deliveries", "orders");
    both["S3TablesDestinationConfiguration"] = json!({
        "DeadLetterQueueS3Configuration": {
            "BucketARN": "arn:aws:s3:::dlq-bucket",
            "ExpectedBucketOwner": "123456789012",
        },
        "S3TablesConfigurationList": [{
            "TableBucketARN": "arn:aws:s3tables:us-east-1:123456789012:bucket/tables",
            "Namespace": "analytics",
            "TableName": "events",
            "CompressionType": "ZSTD",
        }],
    });
    assert_code_kinesis(
        svc.create_channel(&request("CreateChannel", both)),
        "InvalidArgumentException",
    );
}

#[test]
fn create_channel_validates_destination_members() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);

    let mut out_of_range = s3_channel_body("deliveries", "orders");
    out_of_range["S3DestinationConfiguration"]["DataFreshnessInSeconds"] = json!(60);
    assert_code_kinesis(
        svc.create_channel(&request("CreateChannel", out_of_range)),
        "ValidationException",
    );

    let mut missing_compression = s3_channel_body("deliveries", "orders");
    missing_compression["S3DestinationConfiguration"]["StorageConfiguration"]["CompressionType"] =
        Value::Null;
    assert_code_kinesis(
        svc.create_channel(&request("CreateChannel", missing_compression)),
        "InvalidArgumentException",
    );

    let mut bad_storage_class = s3_channel_body("deliveries", "orders");
    bad_storage_class["S3DestinationConfiguration"]["StorageConfiguration"]["StorageClass"] =
        json!("DEEP_ARCHIVE");
    assert_code_kinesis(
        svc.create_channel(&request("CreateChannel", bad_storage_class)),
        "InvalidArgumentException",
    );

    // A streaming table destination has no destination bucket to fall back
    // to, so its dead-letter queue is required.
    let mut tables_without_dlq = s3_channel_body("deliveries", "orders");
    tables_without_dlq["S3DestinationConfiguration"] = Value::Null;
    tables_without_dlq["S3TablesDestinationConfiguration"] = json!({
        "S3TablesConfigurationList": [{
            "TableBucketARN": "arn:aws:s3tables:us-east-1:123456789012:bucket/tables",
            "Namespace": "analytics",
            "TableName": "events",
            "CompressionType": "ZSTD",
        }],
    });
    assert_code_kinesis(
        svc.create_channel(&request("CreateChannel", tables_without_dlq)),
        "InvalidArgumentException",
    );
}

#[test]
fn describe_channel_returns_stored_description() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    let created = create_channel_action(&svc, "deliveries", "orders");
    let arn = created["ChannelDescription"]["ChannelARN"]
        .as_str()
        .unwrap();

    let described = json_response(
        svc.describe_channel(&request("DescribeChannel", json!({ "ChannelARN": arn })))
            .unwrap(),
    );
    assert_eq!(
        described["ChannelDescription"],
        created["ChannelDescription"]
    );
}

#[test]
fn describe_channel_unknown_arn_errors() {
    let (svc, _) = make_service();
    assert_code_kinesis(
        svc.describe_channel(&request(
            "DescribeChannel",
            json!({ "ChannelARN": "arn:aws:kinesis:us-east-1:123456789012:channel/ghost" }),
        )),
        "ResourceNotFoundException",
    );
    assert_code_kinesis(
        svc.describe_channel(&request("DescribeChannel", json!({}))),
        "InvalidArgumentException",
    );
}

#[test]
fn update_channel_changes_freshness_and_logging() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    let created = create_channel_action(&svc, "deliveries", "orders");
    let arn = created["ChannelDescription"]["ChannelARN"]
        .as_str()
        .unwrap();

    let response = json_response(
        svc.update_channel(&request(
            "UpdateChannel",
            json!({
                "ChannelARN": arn,
                "S3DestinationConfiguration": { "DataFreshnessInSeconds": 900 },
                "LoggingConfiguration": {
                    "CloudWatchLogs": {
                        "Enabled": true,
                        "LogGroupName": "/aws/kinesis/updated",
                        "LogStreamName": "updated-stream",
                    }
                },
            }),
        ))
        .unwrap(),
    );
    let updated = &response["ChannelDescription"];

    assert_eq!(
        updated["S3DestinationConfiguration"]["DataFreshnessInSeconds"],
        900
    );
    let logs = &updated["LoggingConfiguration"]["CloudWatchLogs"];
    assert_eq!(logs["Enabled"], true);
    assert_eq!(logs["LogGroupName"], "/aws/kinesis/updated");
    assert_eq!(logs["LogStreamName"], "updated-stream");
    // The destination itself is untouched by the update.
    assert_eq!(
        updated["S3DestinationConfiguration"]["StorageConfiguration"]["BucketARN"],
        "arn:aws:s3:::channel-bucket"
    );

    // The change is persisted, not just echoed.
    let described = json_response(
        svc.describe_channel(&request(
            "DescribeChannel",
            json!({ "ChannelARN": updated["ChannelARN"].as_str().unwrap() }),
        ))
        .unwrap(),
    );
    assert_eq!(
        described["ChannelDescription"]["S3DestinationConfiguration"]["DataFreshnessInSeconds"],
        900
    );
}

#[test]
fn update_channel_rejects_mismatched_destination_and_bad_values() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    let created = create_channel_action(&svc, "deliveries", "orders");
    let arn = created["ChannelDescription"]["ChannelARN"]
        .as_str()
        .unwrap();

    assert_code_kinesis(
        svc.update_channel(&request(
            "UpdateChannel",
            json!({
                "ChannelARN": arn,
                "S3TablesDestinationConfiguration": { "DataFreshnessInSeconds": 600 },
            }),
        )),
        "InvalidArgumentException",
    );
    assert_code_kinesis(
        svc.update_channel(&request(
            "UpdateChannel",
            json!({
                "ChannelARN": arn,
                "S3DestinationConfiguration": { "DataFreshnessInSeconds": 60 },
            }),
        )),
        "ValidationException",
    );
    assert_code_kinesis(
        svc.update_channel(&request(
            "UpdateChannel",
            json!({ "ChannelARN": arn, "S3DestinationConfiguration": {} }),
        )),
        "InvalidArgumentException",
    );
    assert_code_kinesis(
        svc.update_channel(&request(
            "UpdateChannel",
            json!({ "ChannelARN": "arn:aws:kinesis:us-east-1:123456789012:channel/ghost" }),
        )),
        "ResourceNotFoundException",
    );
}

#[test]
fn delete_channel_removes_it() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);
    let created = create_channel_action(&svc, "deliveries", "orders");
    let arn = created["ChannelDescription"]["ChannelARN"]
        .as_str()
        .unwrap();

    svc.delete_channel(&request("DeleteChannel", json!({ "ChannelARN": arn })))
        .unwrap();
    assert!(state.read().default_regional().unwrap().channels.is_empty());

    assert_code_kinesis(
        svc.delete_channel(&request("DeleteChannel", json!({ "ChannelARN": arn }))),
        "ResourceNotFoundException",
    );
}

#[test]
fn list_channels_filters_by_stream() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    create_stream_action(&svc, "payments", 1);
    create_channel_action(&svc, "orders-channel", "orders");
    create_channel_action(&svc, "payments-channel", "payments");

    let all = json_response(
        svc.list_channels(&request("ListChannels", json!({})))
            .unwrap(),
    );
    assert_eq!(all["ChannelSummaries"].as_array().unwrap().len(), 2);
    assert_eq!(all["ChannelSummaries"][0]["ChannelName"], "orders-channel");
    assert_eq!(all["ChannelSummaries"][0]["ChannelDestinationType"], "S3");
    assert_eq!(
        all["ChannelSummaries"][0]["Streams"][0]["StreamARN"],
        stream_arn_for("orders")
    );

    let filtered = json_response(
        svc.list_channels(&request(
            "ListChannels",
            json!({ "StreamFilter": [{ "StreamARN": stream_arn_for("payments") }] }),
        ))
        .unwrap(),
    );
    let summaries = filtered["ChannelSummaries"].as_array().unwrap();
    assert_eq!(summaries.len(), 1);
    assert_eq!(summaries[0]["ChannelName"], "payments-channel");

    // A filter that names no source stream of any channel matches nothing.
    let unmatched = json_response(
        svc.list_channels(&request(
            "ListChannels",
            json!({ "StreamFilter": [{ "StreamARN": stream_arn_for("ghost") }] }),
        ))
        .unwrap(),
    );
    assert!(unmatched["ChannelSummaries"].as_array().unwrap().is_empty());

    // StreamCreationTimestamp, when supplied, must also match.
    let creation = all["ChannelSummaries"][0]["Streams"][0]["StreamCreationTimestamp"]
        .as_f64()
        .unwrap();
    let with_timestamp = json_response(
        svc.list_channels(&request(
            "ListChannels",
            json!({
                "StreamFilter": [{
                    "StreamARN": stream_arn_for("orders"),
                    "StreamCreationTimestamp": creation,
                }]
            }),
        ))
        .unwrap(),
    );
    assert_eq!(
        with_timestamp["ChannelSummaries"].as_array().unwrap().len(),
        1
    );
    let wrong_timestamp = json_response(
        svc.list_channels(&request(
            "ListChannels",
            json!({
                "StreamFilter": [{
                    "StreamARN": stream_arn_for("orders"),
                    "StreamCreationTimestamp": creation + 3600.0,
                }]
            }),
        ))
        .unwrap(),
    );
    assert!(wrong_timestamp["ChannelSummaries"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[test]
fn list_channels_paginates() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    for name in ["a", "b", "c"] {
        create_channel_action(&svc, name, "orders");
    }

    let page1 = json_response(
        svc.list_channels(&request("ListChannels", json!({ "MaxResults": 2 })))
            .unwrap(),
    );
    let names: Vec<&str> = page1["ChannelSummaries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["ChannelName"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["a", "b"]);
    let token = page1["NextToken"].as_str().unwrap().to_string();

    let page2 = json_response(
        svc.list_channels(&request("ListChannels", json!({ "NextToken": token })))
            .unwrap(),
    );
    assert_eq!(page2["ChannelSummaries"][0]["ChannelName"], "c");
    assert_eq!(page2["ChannelSummaries"].as_array().unwrap().len(), 1);
    assert!(
        page2["NextToken"].is_null(),
        "last page must not carry a cursor"
    );
}

#[test]
fn list_channels_rejects_bad_input() {
    let (svc, _) = make_service();
    assert_code_kinesis(
        svc.list_channels(&request("ListChannels", json!({ "MaxResults": 0 }))),
        "ValidationException",
    );
    assert_code_kinesis(
        svc.list_channels(&request("ListChannels", json!({ "StreamFilter": [{}] }))),
        "InvalidArgumentException",
    );
    assert_code_kinesis(
        svc.list_channels(&request(
            "ListChannels",
            json!({ "NextToken": "not base64 !!" }),
        )),
        "InvalidArgumentException",
    );
}

#[test]
fn delete_stream_is_blocked_while_a_channel_is_attached() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    let created = create_channel_action(&svc, "deliveries", "orders");
    let arn = created["ChannelDescription"]["ChannelARN"]
        .as_str()
        .unwrap();

    assert_code_kinesis(
        svc.delete_stream(&request("DeleteStream", json!({ "StreamName": "orders" }))),
        "ResourceInUseException",
    );

    svc.delete_channel(&request("DeleteChannel", json!({ "ChannelARN": arn })))
        .unwrap();
    svc.delete_stream(&request("DeleteStream", json!({ "StreamName": "orders" })))
        .unwrap();
}

#[test]
fn channel_actions_are_supported_and_mutating() {
    let svc = KinesisService::new(Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiRegionState::new(
            "123456789012",
            "us-east-1",
            "http://localhost:4566",
        ),
    )));
    for action in [
        "CreateChannel",
        "DescribeChannel",
        "ListChannels",
        "UpdateChannel",
        "DeleteChannel",
    ] {
        assert!(
            svc.supported_actions().contains(&action),
            "{action} missing from SUPPORTED_ACTIONS"
        );
    }
    for action in ["CreateChannel", "UpdateChannel", "DeleteChannel"] {
        assert!(is_mutating_action(action), "{action} must be snapshotted");
    }
    assert!(!is_mutating_action("DescribeChannel"));
    assert!(!is_mutating_action("ListChannels"));
}

// ── Tags v2 against streams and channels ──

#[test]
fn tag_resource_round_trips_stream_tags() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    let arn = stream_arn_for("orders");

    svc.tag_resource(&request(
        "TagResource",
        json!({ "ResourceARN": arn, "Tags": { "env": "test" } }),
    ))
    .unwrap();

    let listed = json_response(
        svc.list_tags_for_resource(&request(
            "ListTagsForResource",
            json!({ "ResourceARN": arn }),
        ))
        .unwrap(),
    );
    assert_eq!(listed["Tags"], json!([{ "Key": "env", "Value": "test" }]));

    svc.untag_resource(&request(
        "UntagResource",
        json!({ "ResourceARN": arn, "TagKeys": ["env"] }),
    ))
    .unwrap();
    let after = json_response(
        svc.list_tags_for_resource(&request(
            "ListTagsForResource",
            json!({ "ResourceARN": arn }),
        ))
        .unwrap(),
    );
    assert_eq!(after["Tags"], json!([]));
}

#[test]
fn tag_operations_reach_channels_by_arn() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    let mut body = s3_channel_body("deliveries", "orders");
    body["Tags"] = json!({ "team": "data" });
    let created = json_response(svc.create_channel(&request("CreateChannel", body)).unwrap());
    let channel_arn = created["ChannelDescription"]["ChannelARN"]
        .as_str()
        .unwrap()
        .to_string();

    // The tags supplied to CreateChannel are readable, not write-only.
    let listed = json_response(
        svc.list_tags_for_resource(&request(
            "ListTagsForResource",
            json!({ "ResourceARN": channel_arn }),
        ))
        .unwrap(),
    );
    assert_eq!(listed["Tags"], json!([{ "Key": "team", "Value": "data" }]));

    svc.tag_resource(&request(
        "TagResource",
        json!({ "ResourceARN": channel_arn, "Tags": { "env": "test" } }),
    ))
    .unwrap();
    svc.untag_resource(&request(
        "UntagResource",
        json!({ "ResourceARN": channel_arn, "TagKeys": ["team"] }),
    ))
    .unwrap();

    let after = json_response(
        svc.list_tags_for_resource(&request(
            "ListTagsForResource",
            json!({ "ResourceARN": channel_arn }),
        ))
        .unwrap(),
    );
    assert_eq!(after["Tags"], json!([{ "Key": "env", "Value": "test" }]));

    // Tagging the channel left the source stream's own tags alone.
    let stream_tags = json_response(
        svc.list_tags_for_resource(&request(
            "ListTagsForResource",
            json!({ "ResourceARN": stream_arn_for("orders") }),
        ))
        .unwrap(),
    );
    assert_eq!(stream_tags["Tags"], json!([]));
}

#[test]
fn tag_operations_do_not_resolve_another_regions_channel() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    let created = create_channel_action(&svc, "deliveries", "orders");
    let channel_id = created["ChannelDescription"]["ChannelId"].as_str().unwrap();
    let foreign_arn = format!("arn:aws:kinesis:eu-west-1:123456789012:channel/{channel_id}");

    // The channel lives in us-east-1; eu-west-1 has no such channel.
    assert_code_kinesis(
        svc.tag_resource(&request_in_region(
            "TagResource",
            "eu-west-1",
            json!({ "ResourceARN": foreign_arn, "Tags": { "env": "test" } }),
        )),
        "ResourceNotFoundException",
    );
}

#[test]
fn tag_operations_reject_unknown_resources() {
    let (svc, _) = make_service();
    for arn in [
        "arn:aws:kinesis:us-east-1:123456789012:channel/ghost",
        "arn:aws:kinesis:us-east-1:123456789012:stream/ghost",
    ] {
        let body = json!({
            "ResourceARN": arn,
            "Tags": { "env": "test" },
            "TagKeys": ["env"],
        });
        assert_code_kinesis(
            svc.tag_resource(&request("TagResource", body.clone())),
            "ResourceNotFoundException",
        );
        assert_code_kinesis(
            svc.untag_resource(&request("UntagResource", body.clone())),
            "ResourceNotFoundException",
        );
        assert_code_kinesis(
            svc.list_tags_for_resource(&request("ListTagsForResource", body)),
            "ResourceNotFoundException",
        );
    }
}

// ── channel model bounds and region-tolerant resolution ──

#[test]
fn create_channel_enforces_the_models_arn_lengths() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);

    // An ARN of exactly `len` bytes, padded in the resource segment.
    let padded = |prefix: &str, len: usize| format!("{prefix}{}", "a".repeat(len - prefix.len()));
    let role = |len: usize| padded("arn:aws:iam::123456789012:role/", len);
    let schema = |len: usize| padded("arn:aws:glue:us-east-1:123456789012:schema/registry/", len);

    // ServiceExecutionRoleARN is a RoleARN: 512, not the 2048 other ARN
    // members share.
    let mut long_role = s3_channel_body("deliveries", "orders");
    long_role["ServiceExecutionRoleARN"] = json!(role(513));
    assert_code_kinesis(
        svc.create_channel(&request("CreateChannel", long_role)),
        "ValidationException",
    );
    let mut max_role = s3_channel_body("deliveries", "orders");
    max_role["ServiceExecutionRoleARN"] = json!(role(512));
    svc.create_channel(&request("CreateChannel", max_role))
        .unwrap();

    // GSRSchemaARN is capped at 512 too.
    let mut long_schema = s3_channel_body("schemas", "orders");
    long_schema["StreamConfigurationList"][0]["RecordConfiguration"]["GSRSchemaARN"] =
        json!(schema(513));
    assert_code_kinesis(
        svc.create_channel(&request("CreateChannel", long_schema)),
        "ValidationException",
    );
    let mut max_schema = s3_channel_body("schemas", "orders");
    max_schema["StreamConfigurationList"][0]["RecordConfiguration"]["GSRSchemaARN"] =
        json!(schema(512));
    svc.create_channel(&request("CreateChannel", max_schema))
        .unwrap();
}

fn create_stream_in(svc: &KinesisService, region: &str, name: &str) {
    svc.create_stream(&request_in_region(
        "CreateStream",
        region,
        json!({ "StreamName": name, "ShardCount": 1 }),
    ))
    .unwrap();
}

fn list_stream_names(svc: &KinesisService, region: &str) -> Vec<String> {
    let body = json_response(
        svc.list_streams(&request_in_region("ListStreams", region, json!({})))
            .unwrap(),
    );
    body["StreamNames"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect()
}

#[test]
fn same_stream_name_coexists_in_two_regions() {
    let (svc, state) = make_service();
    create_stream_in(&svc, "us-east-1", "orders");
    create_stream_in(&svc, "eu-west-1", "orders");
    create_stream_in(&svc, "eu-west-1", "west-only");

    assert_eq!(list_stream_names(&svc, "us-east-1"), vec!["orders"]);
    assert_eq!(
        list_stream_names(&svc, "eu-west-1"),
        vec!["orders", "west-only"]
    );
    assert!(list_stream_names(&svc, "ap-south-1").is_empty());

    let west = json_response(
        svc.describe_stream_summary(&request_in_region(
            "DescribeStreamSummary",
            "eu-west-1",
            json!({ "StreamName": "orders" }),
        ))
        .unwrap(),
    );
    assert_eq!(
        west["StreamDescriptionSummary"]["StreamARN"],
        "arn:aws:kinesis:eu-west-1:123456789012:stream/orders"
    );
    // Reading a region that was never written leaves no empty state behind.
    assert!(state
        .read()
        .regional("123456789012", "ap-south-1")
        .is_none());

    // Deleting one region's stream leaves the other's alone.
    svc.delete_stream(&request_in_region(
        "DeleteStream",
        "eu-west-1",
        json!({ "StreamName": "orders" }),
    ))
    .unwrap();
    assert_eq!(list_stream_names(&svc, "us-east-1"), vec!["orders"]);
    assert_eq!(list_stream_names(&svc, "eu-west-1"), vec!["west-only"]);
}

#[test]
fn records_are_isolated_per_region() {
    let (svc, _) = make_service();
    create_stream_in(&svc, "us-east-1", "orders");
    create_stream_in(&svc, "eu-west-1", "orders");
    svc.put_record(&request_in_region(
        "PutRecord",
        "eu-west-1",
        json!({ "StreamName": "orders", "PartitionKey": "pk", "Data": "aGVsbG8=" }),
    ))
    .unwrap();

    let read = |region: &str| -> usize {
        let it = json_response(
            svc.get_shard_iterator(&request_in_region(
                "GetShardIterator",
                region,
                json!({
                    "StreamName": "orders",
                    "ShardId": "shardId-000000000000",
                    "ShardIteratorType": "TRIM_HORIZON",
                }),
            ))
            .unwrap(),
        );
        let records = json_response(
            svc.get_records(&request_in_region(
                "GetRecords",
                region,
                json!({ "ShardIterator": it["ShardIterator"] }),
            ))
            .unwrap(),
        );
        records["Records"].as_array().unwrap().len()
    };
    assert_eq!(read("eu-west-1"), 1);
    assert_eq!(read("us-east-1"), 0);
}

#[test]
fn stream_arn_of_another_region_is_not_found() {
    let (svc, _) = make_service();
    create_stream_action(&svc, "orders", 1);
    // The us-east-1 stream's ARN, sent to eu-west-1 where no stream exists.
    assert_code_kinesis(
        svc.describe_stream_summary(&request_in_region(
            "DescribeStreamSummary",
            "eu-west-1",
            json!({ "StreamARN": stream_arn_for("orders") }),
        )),
        "ResourceNotFoundException",
    );
    // A same-named eu-west-1 stream does not answer for the us-east-1 ARN.
    create_stream_in(&svc, "eu-west-1", "orders");
    assert_code_kinesis(
        svc.describe_stream_summary(&request_in_region(
            "DescribeStreamSummary",
            "eu-west-1",
            json!({ "StreamARN": stream_arn_for("orders") }),
        )),
        "ResourceNotFoundException",
    );
}

#[test]
fn cross_service_delivery_targets_the_arns_region() {
    use fakecloud_core::delivery::KinesisDelivery;
    let (svc, state) = make_service();
    create_stream_in(&svc, "us-east-1", "orders");
    create_stream_in(&svc, "eu-west-1", "orders");
    let delivery = crate::delivery::KinesisDeliveryImpl::new(state.clone());
    delivery.put_record(
        "arn:aws:kinesis:eu-west-1:123456789012:stream/orders",
        "aGVsbG8=",
        "pk",
    );
    let count = |region: &str| -> usize {
        state
            .read()
            .regional("123456789012", region)
            .unwrap()
            .streams["orders"]
            .shards
            .iter()
            .map(|s| s.records.len())
            .sum()
    };
    assert_eq!(count("eu-west-1"), 1);
    assert_eq!(count("us-east-1"), 0);

    // A region with no state is not created by a delivery that misses.
    delivery.put_record(
        "arn:aws:kinesis:ap-south-1:123456789012:stream/orders",
        "aGVsbG8=",
        "pk",
    );
    assert!(state
        .read()
        .regional("123456789012", "ap-south-1")
        .is_none());
}

#[test]
fn china_region_stream_arn_uses_the_aws_cn_partition() {
    let (svc, _) = make_service();
    svc.create_stream(&request_in_region(
        "CreateStream",
        "cn-north-1",
        json!({ "StreamName": "cn-s", "ShardCount": 1 }),
    ))
    .unwrap();
    let resp = svc
        .describe_stream_summary(&request_in_region(
            "DescribeStreamSummary",
            "cn-north-1",
            json!({ "StreamName": "cn-s" }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let arn = "arn:aws-cn:kinesis:cn-north-1:123456789012:stream/cn-s";
    assert_eq!(body["StreamDescriptionSummary"]["StreamARN"], arn);

    let resp = svc
        .describe_stream_summary(&request_in_region(
            "DescribeStreamSummary",
            "cn-north-1",
            json!({ "StreamARN": arn }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["StreamDescriptionSummary"]["StreamName"], "cn-s");
}

// ── Record distribution strategy ─────────────────────────────────

fn create_on_demand_stream(svc: &KinesisService, name: &str, strategy: Option<&str>) {
    let mut body = json!({
        "StreamName": name,
        "StreamModeDetails": { "StreamMode": "ON_DEMAND" }
    });
    if let Some(strategy) = strategy {
        body["RecordDistributionStrategy"] = json!(strategy);
    }
    svc.create_stream(&request("CreateStream", body)).unwrap();
}

fn summary(svc: &KinesisService, name: &str) -> Value {
    json_response(
        svc.describe_stream_summary(&request(
            "DescribeStreamSummary",
            json!({ "StreamName": name }),
        ))
        .unwrap(),
    )["StreamDescriptionSummary"]
        .clone()
}

#[test]
fn record_distribution_strategy_defaults_and_is_on_demand_only_in_summary() {
    let (svc, _) = make_service();
    create_on_demand_stream(&svc, "od", None);
    create_stream_action(&svc, "prov", 1);
    assert_eq!(
        summary(&svc, "od")["RecordDistributionStrategy"],
        "USER_PARTITION_KEY"
    );
    assert!(summary(&svc, "prov")
        .get("RecordDistributionStrategy")
        .is_none());

    create_on_demand_stream(&svc, "auto", Some("AUTO"));
    assert_eq!(summary(&svc, "auto")["RecordDistributionStrategy"], "AUTO");
}

#[test]
fn create_stream_rejects_auto_on_provisioned_and_bad_enum() {
    let (svc, _) = make_service();
    let err = svc
        .create_stream(&request(
            "CreateStream",
            json!({ "StreamName": "p", "ShardCount": 1, "RecordDistributionStrategy": "AUTO" }),
        ))
        .err()
        .expect("request should fail");
    assert_eq!(err.code(), "InvalidArgumentException");
    let err = svc
        .create_stream(&request(
            "CreateStream",
            json!({
                "StreamName": "p",
                "StreamModeDetails": { "StreamMode": "ON_DEMAND" },
                "RecordDistributionStrategy": "RANDOM"
            }),
        ))
        .err()
        .expect("request should fail");
    assert_eq!(err.code(), "ValidationException");
}

#[test]
fn update_record_distribution_strategy_round_trips_and_validates() {
    let (svc, state) = make_service();
    create_on_demand_stream(&svc, "od", None);
    create_stream_action(&svc, "prov", 1);
    let od_arn = state
        .read()
        .default_regional()
        .unwrap()
        .stream_arn("us-east-1", "od");
    let prov_arn = state
        .read()
        .default_regional()
        .unwrap()
        .stream_arn("us-east-1", "prov");

    svc.update_stream_record_distribution_strategy(&request(
        "UpdateStreamRecordDistributionStrategy",
        json!({ "StreamARN": od_arn, "RecordDistributionStrategy": "AUTO" }),
    ))
    .unwrap();
    assert_eq!(summary(&svc, "od")["RecordDistributionStrategy"], "AUTO");

    // AUTO on a provisioned stream is rejected.
    let err = svc
        .update_stream_record_distribution_strategy(&request(
            "UpdateStreamRecordDistributionStrategy",
            json!({ "StreamARN": prov_arn, "RecordDistributionStrategy": "AUTO" }),
        ))
        .err()
        .expect("request should fail");
    assert_eq!(err.code(), "InvalidArgumentException");

    // Unknown stream.
    let err = svc
        .update_stream_record_distribution_strategy(&request(
            "UpdateStreamRecordDistributionStrategy",
            json!({
                "StreamARN": "arn:aws:kinesis:us-east-1:123456789012:stream/ghost",
                "RecordDistributionStrategy": "AUTO"
            }),
        ))
        .err()
        .expect("request should fail");
    assert_eq!(err.code(), "ResourceNotFoundException");

    // An AUTO stream cannot leave on-demand mode.
    let err = svc
        .update_stream_mode(&request(
            "UpdateStreamMode",
            json!({ "StreamARN": od_arn, "StreamModeDetails": { "StreamMode": "PROVISIONED" } }),
        ))
        .err()
        .expect("request should fail");
    assert_eq!(err.code(), "InvalidArgumentException");

    svc.update_stream_record_distribution_strategy(&request(
        "UpdateStreamRecordDistributionStrategy",
        json!({ "StreamARN": od_arn, "RecordDistributionStrategy": "USER_PARTITION_KEY" }),
    ))
    .unwrap();
    assert_eq!(
        summary(&svc, "od")["RecordDistributionStrategy"],
        "USER_PARTITION_KEY"
    );
}

#[test]
fn auto_strategy_ignores_partition_key_and_spreads_records() {
    let (svc, _) = make_service();
    create_on_demand_stream(&svc, "auto", Some("AUTO"));
    create_on_demand_stream(&svc, "keyed", None);

    // Under AUTO the same partition key (and even an explicit hash key) no
    // longer pins every record to one shard.
    let mut shards = std::collections::BTreeSet::new();
    for _ in 0..8 {
        let resp = json_response(
            svc.put_record(&request(
                "PutRecord",
                json!({
                    "StreamName": "auto",
                    "Data": "aGk=",
                    "PartitionKey": "same",
                    "ExplicitHashKey": "0"
                }),
            ))
            .unwrap(),
        );
        shards.insert(resp["ShardId"].as_str().unwrap().to_string());
    }
    assert_eq!(shards.len(), 4, "AUTO spreads across every open shard");

    // PartitionKey is optional under AUTO...
    let resp = json_response(
        svc.put_records(&request(
            "PutRecords",
            json!({ "StreamName": "auto", "Records": [{ "Data": "aGk=" }] }),
        ))
        .unwrap(),
    );
    assert_eq!(resp["FailedRecordCount"], 0);
    svc.put_record(&request(
        "PutRecord",
        json!({ "StreamName": "auto", "Data": "aGk=" }),
    ))
    .unwrap();

    // ...but still required under USER_PARTITION_KEY.
    let err = svc
        .put_record(&request(
            "PutRecord",
            json!({ "StreamName": "keyed", "Data": "aGk=" }),
        ))
        .err()
        .expect("request should fail");
    assert_eq!(err.code(), "InvalidArgumentException");
    let resp = json_response(
        svc.put_records(&request(
            "PutRecords",
            json!({ "StreamName": "keyed", "Records": [{ "Data": "aGk=" }] }),
        ))
        .unwrap(),
    );
    assert_eq!(resp["FailedRecordCount"], 1);
}

#[test]
fn auto_strategy_still_validates_supplied_keys() {
    let (svc, _) = make_service();
    create_on_demand_stream(&svc, "auto", Some("AUTO"));
    // Optional means omitted: an explicitly empty key still breaks min length.
    let err = svc
        .put_record(&request(
            "PutRecord",
            json!({ "StreamName": "auto", "Data": "aGk=", "PartitionKey": "" }),
        ))
        .err()
        .expect("empty key rejected");
    assert_eq!(err.code(), "ValidationException");
    // A malformed ExplicitHashKey is rejected even though AUTO ignores it.
    let err = svc
        .put_record(&request(
            "PutRecord",
            json!({ "StreamName": "auto", "Data": "aGk=", "ExplicitHashKey": "abc" }),
        ))
        .err()
        .expect("bad hash key rejected");
    assert_eq!(err.code(), "InvalidArgumentException");
    let resp = json_response(
        svc.put_records(&request(
            "PutRecords",
            json!({ "StreamName": "auto", "Records": [
                { "Data": "aGk=", "PartitionKey": "" },
                { "Data": "aGk=", "ExplicitHashKey": "abc" },
            ] }),
        ))
        .unwrap(),
    );
    assert_eq!(resp["FailedRecordCount"], 2);
}

#[test]
fn record_without_partition_key_omits_it_on_read() {
    let (svc, _) = make_service();
    create_on_demand_stream(&svc, "auto", Some("AUTO"));
    let put = json_response(
        svc.put_record(&request(
            "PutRecord",
            json!({ "StreamName": "auto", "Data": "aGk=" }),
        ))
        .unwrap(),
    );
    let iterator = json_response(
        svc.get_shard_iterator(&request(
            "GetShardIterator",
            json!({
                "StreamName": "auto",
                "ShardId": put["ShardId"],
                "ShardIteratorType": "TRIM_HORIZON"
            }),
        ))
        .unwrap(),
    );
    let records = json_response(
        svc.get_records(&request(
            "GetRecords",
            json!({ "ShardIterator": iterator["ShardIterator"] }),
        ))
        .unwrap(),
    );
    let record = &records["Records"][0];
    assert_eq!(record["Data"], "aGk=");
    assert!(record.get("PartitionKey").is_none());
}

#[test]
fn create_stream_honors_warm_throughput_and_max_record_size() {
    let (svc, _) = make_service();
    svc.create_stream(&request(
        "CreateStream",
        json!({
            "StreamName": "big",
            "StreamModeDetails": {"StreamMode": "ON_DEMAND"},
            "WarmThroughputMiBps": 50,
            "MaxRecordSizeInKiB": 2048,
        }),
    ))
    .unwrap();
    let summary = json_response(
        svc.describe_stream_summary(&request(
            "DescribeStreamSummary",
            json!({"StreamName": "big"}),
        ))
        .unwrap(),
    );
    let s = &summary["StreamDescriptionSummary"];
    assert_eq!(s["MaxRecordSizeInKiB"], 2048);
    assert_eq!(s["WarmThroughput"]["TargetMiBps"], 50);

    // The create-time ceiling is enforced: 1.5 MiB fits under 2 MiB...
    svc.put_record(&request(
        "PutRecord",
        json!({"StreamName": "big", "Data": b64(&vec![b'x'; 1536 * 1024]), "PartitionKey": "k"}),
    ))
    .unwrap();
    // ...but 2 MiB + 1 byte does not.
    let res = svc.put_record(&request(
        "PutRecord",
        json!({"StreamName": "big", "Data": b64(&vec![b'x'; 2048 * 1024 + 1]), "PartitionKey": "k"}),
    ));
    assert_code_kinesis(res, "ValidationException");
}

#[test]
fn create_stream_rejects_out_of_range_max_record_size() {
    let (svc, _) = make_service();
    let res = svc.create_stream(&request(
        "CreateStream",
        json!({"StreamName": "s", "ShardCount": 1, "MaxRecordSizeInKiB": 512}),
    ));
    assert_code_kinesis(res, "ValidationException");
}

#[test]
fn consumer_tags_round_trip_through_tags_v2() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);
    let stream_arn = state
        .read()
        .default_regional()
        .unwrap()
        .stream_arn("us-east-1", "orders");
    let reg = json_response(
        svc.register_stream_consumer(&request(
            "RegisterStreamConsumer",
            json!({"StreamARN": stream_arn, "ConsumerName": "c1", "Tags": {"env": "dev"}}),
        ))
        .unwrap(),
    );
    let consumer_arn = reg["Consumer"]["ConsumerARN"].as_str().unwrap().to_string();
    let list = |svc: &KinesisService| {
        json_response(
            svc.list_tags_for_resource(&request(
                "ListTagsForResource",
                json!({"ResourceARN": consumer_arn}),
            ))
            .unwrap(),
        )["Tags"]
            .clone()
    };
    assert_eq!(list(&svc), json!([{"Key": "env", "Value": "dev"}]));

    svc.tag_resource(&request(
        "TagResource",
        json!({"ResourceARN": consumer_arn, "Tags": {"team": "core"}}),
    ))
    .unwrap();
    svc.untag_resource(&request(
        "UntagResource",
        json!({"ResourceARN": consumer_arn, "TagKeys": ["env"]}),
    ))
    .unwrap();
    assert_eq!(list(&svc), json!([{"Key": "team", "Value": "core"}]));
    // The stream's own tags are untouched.
    assert!(state.read().default_regional().unwrap().streams["orders"]
        .tags
        .is_empty());

    svc.deregister_stream_consumer(&request(
        "DeregisterStreamConsumer",
        json!({"ConsumerARN": consumer_arn}),
    ))
    .unwrap();
    let res = svc.list_tags_for_resource(&request(
        "ListTagsForResource",
        json!({"ResourceARN": consumer_arn}),
    ));
    assert_code_kinesis(res, "ResourceNotFoundException");
}
