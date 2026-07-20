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
        fakecloud_core::multi_account::MultiAccountState::new(
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
    let st = _accts.default_ref();
    let stream = st.streams.get("orders").unwrap();
    assert_eq!(stream.stream_status, "ACTIVE");
    assert_eq!(stream.shard_count, 2);
    assert_eq!(stream.retention_period_hours, 24);
    assert!(stream.stream_arn.ends_with(":stream/orders"));
}

#[test]
fn create_stream_rejects_duplicate_names() {
    let state = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new(
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
        fakecloud_core::multi_account::MultiAccountState::new(
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
        fakecloud_core::multi_account::MultiAccountState::new(
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
    let stream_arn = state.read().default_ref().stream_arn("orders");
    svc.register_stream_consumer(&request(
        "RegisterStreamConsumer",
        json!({ "StreamARN": stream_arn, "ConsumerName": "c1" }),
    ))
    .unwrap();

    svc.delete_stream(&request("DeleteStream", json!({ "StreamName": "orders" })))
        .unwrap();

    let _accts = state.read();
    let s = _accts.default_ref();
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
    let s = _accts.default_ref();
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
        .default_ref()
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
        let stream = g.default_mut().streams.get_mut("orders").unwrap();
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
            .default_ref()
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
            .default_ref()
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
            .default_ref()
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
            .default_ref()
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
            .default_ref()
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
    let s = _accts.default_ref();
    let metrics = &s.streams.get("orders").unwrap().enhanced_metrics;
    assert_eq!(metrics, &vec!["OutgoingBytes".to_string()]);
}

#[test]
fn update_stream_mode_writes_new_mode() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);
    let stream_arn = state.read().default_ref().stream_arn("orders");
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
            .default_ref()
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
    let stream_arn = state.read().default_ref().stream_arn("orders");
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
    assert!(state.read().default_ref().consumers.is_empty());
}

#[test]
fn register_consumer_duplicate_errors() {
    let (svc, state) = make_service();
    create_stream_action(&svc, "orders", 1);
    let stream_arn = state.read().default_ref().stream_arn("orders");
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
    let stream_arn = state.read().default_ref().stream_arn("orders");
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
    let stream_arn = state.read().default_ref().stream_arn("orders");
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
        state.read().default_ref().billing_commitment_status,
        "ENABLED"
    );

    svc.update_account_settings(&request(
        "UpdateAccountSettings",
        json!({ "MinimumThroughputBillingCommitment": { "Status": "DISABLED" } }),
    ))
    .unwrap();
    assert_eq!(
        state.read().default_ref().billing_commitment_status,
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
        let st = accts.default_ref();
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
    let st = _accts.default_ref();
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
    let st = _accts.default_ref();
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
        let st = accts.default_mut();
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
        .default_mut()
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
        g.default_ref()
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
        let stream = g.default_ref().streams.get("orders").unwrap();
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
        let stream = g.default_mut().streams.get_mut("orders").unwrap();
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
        let stream = g.default_mut().streams.get_mut("xshard").unwrap();
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
        .default_mut()
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
