use super::*;
use fakecloud_persistence::SnapshotStore;
use serde_json::json;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Default)]
struct RecordingSnapshotStore {
    bytes: parking_lot::Mutex<Option<Vec<u8>>>,
}

impl SnapshotStore for RecordingSnapshotStore {
    fn load(&self) -> io::Result<Option<Vec<u8>>> {
        Ok(self.bytes.lock().clone())
    }

    fn save(&self, bytes: &[u8]) -> io::Result<()> {
        *self.bytes.lock() = Some(bytes.to_vec());
        Ok(())
    }
}

struct TempSnapshotDir {
    path: PathBuf,
}

impl TempSnapshotDir {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("fakecloud-dynamodb-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&path).unwrap();
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempSnapshotDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

async fn call_dynamodb(service: &DynamoDbService, action: &str, body: Value) -> Value {
    let resp = service.handle(make_request(action, body)).await.unwrap();
    let status = resp.status;
    let body = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(status, StatusCode::OK, "{action} failed: {body}");
    body
}

fn load_recorded_snapshot(store: &RecordingSnapshotStore) -> Vec<u8> {
    SnapshotStore::load(store).unwrap().unwrap()
}

fn service_from_snapshot_bytes(bytes: &[u8]) -> DynamoDbService {
    let snapshot: DynamoDbSnapshot = serde_json::from_slice(bytes).unwrap();
    assert_eq!(snapshot.schema_version, DYNAMODB_SNAPSHOT_SCHEMA_VERSION);

    let state: SharedDynamoDbState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    if let Some(accounts) = snapshot.accounts {
        *state.write() = accounts;
    } else if let Some(single_state) = snapshot.state {
        let account_id = single_state.account_id.clone();
        *state.write().get_or_create(&account_id) = single_state;
    } else {
        panic!("snapshot must contain either multi-account or legacy state");
    }

    DynamoDbService::new(state)
}

async fn populate_snapshot_round_trip_fixture(service: &DynamoDbService) {
    call_dynamodb(
        service,
        "CreateTable",
        json!({
            "TableName": "orders",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gsi_pk", "AttributeType": "S"},
                {"AttributeName": "gsi_sk", "AttributeType": "N"},
                {"AttributeName": "lsi_sk", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": [{
                "IndexName": "by-status",
                "KeySchema": [
                    {"AttributeName": "gsi_pk", "KeyType": "HASH"},
                    {"AttributeName": "gsi_sk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }],
            "LocalSecondaryIndexes": [{
                "IndexName": "by-due",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lsi_sk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }]
        }),
    )
    .await;

    call_dynamodb(
        service,
        "CreateTable",
        json!({
            "TableName": "typed",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    )
    .await;

    call_dynamodb(
        service,
        "PutItem",
        json!({
            "TableName": "orders",
            "Item": {
                "pk": {"S": "acct#1"},
                "sk": {"S": "order#1"},
                "gsi_pk": {"S": "open"},
                "gsi_sk": {"N": "10"},
                "lsi_sk": {"S": "due#2026-07-01"},
                "payload": {"S": "first order"}
            }
        }),
    )
    .await;
    call_dynamodb(
        service,
        "PutItem",
        json!({
            "TableName": "orders",
            "Item": {
                "pk": {"S": "acct#1"},
                "sk": {"S": "order#2"},
                "gsi_pk": {"S": "open"},
                "gsi_sk": {"N": "20"},
                "lsi_sk": {"S": "due#2026-07-02"},
                "payload": {"S": "second order"}
            }
        }),
    )
    .await;
    call_dynamodb(
        service,
        "PutItem",
        json!({
            "TableName": "typed",
            "Item": {
                "pk": {"S": "types#1"},
                "s": {"S": "hello"},
                "n": {"N": "42.5"},
                "flag": {"BOOL": true},
                "none": {"NULL": true},
                "ss": {"SS": ["red", "blue"]},
                "ns": {"NS": ["1", "2"]},
                "b": {"B": "aGVsbG8="},
                "bs": {"BS": ["YQ==", "Yg=="]},
                "list": {"L": [{"S": "nested"}, {"N": "7"}, {"BOOL": false}]},
                "map": {"M": {"inner": {"S": "value"}, "count": {"N": "3"}}}
            }
        }),
    )
    .await;
}

async fn assert_snapshot_round_trip_fixture(service: &DynamoDbService) {
    let tables = call_dynamodb(service, "ListTables", json!({})).await;
    let table_names = tables["TableNames"].as_array().unwrap();
    assert!(table_names.iter().any(|name| name == "orders"));
    assert!(table_names.iter().any(|name| name == "typed"));

    let orders = call_dynamodb(service, "DescribeTable", json!({"TableName": "orders"})).await;
    let order_table = &orders["Table"];
    assert_eq!(
        order_table["GlobalSecondaryIndexes"][0]["IndexName"],
        "by-status"
    );
    assert_eq!(
        order_table["LocalSecondaryIndexes"][0]["IndexName"],
        "by-due"
    );

    let item = call_dynamodb(
        service,
        "GetItem",
        json!({
            "TableName": "orders",
            "Key": {"pk": {"S": "acct#1"}, "sk": {"S": "order#1"}}
        }),
    )
    .await;
    assert_eq!(item["Item"]["payload"], json!({"S": "first order"}));

    let gsi = call_dynamodb(
        service,
        "Query",
        json!({
            "TableName": "orders",
            "IndexName": "by-status",
            "KeyConditionExpression": "gsi_pk = :status",
            "ExpressionAttributeValues": {":status": {"S": "open"}}
        }),
    )
    .await;
    assert_eq!(gsi["Count"], 2);

    let lsi = call_dynamodb(
        service,
        "Query",
        json!({
            "TableName": "orders",
            "IndexName": "by-due",
            "KeyConditionExpression": "pk = :pk AND begins_with(lsi_sk, :prefix)",
            "ExpressionAttributeValues": {
                ":pk": {"S": "acct#1"},
                ":prefix": {"S": "due#"}
            }
        }),
    )
    .await;
    assert_eq!(lsi["Count"], 2);

    let typed = call_dynamodb(
        service,
        "GetItem",
        json!({
            "TableName": "typed",
            "Key": {"pk": {"S": "types#1"}}
        }),
    )
    .await;
    let item = &typed["Item"];
    assert_eq!(item["s"], json!({"S": "hello"}));
    assert_eq!(item["n"], json!({"N": "42.5"}));
    assert_eq!(item["flag"], json!({"BOOL": true}));
    assert_eq!(item["none"], json!({"NULL": true}));
    assert_eq!(item["b"], json!({"B": "aGVsbG8="}));
    // Lists and maps are ordered/deterministic, so assert full equality: a
    // regression that drops later list elements or extra map fields fails here.
    assert_eq!(
        item["list"],
        json!({"L": [{"S": "nested"}, {"N": "7"}, {"BOOL": false}]})
    );
    assert_eq!(
        item["map"],
        json!({"M": {"inner": {"S": "value"}, "count": {"N": "3"}}})
    );
    // Sets are unordered: check the exact length plus every expected member so
    // a dropped or duplicated element is caught regardless of ordering.
    let ss = item["ss"]["SS"].as_array().unwrap();
    assert_eq!(ss.len(), 2);
    assert!(ss.contains(&json!("red")));
    assert!(ss.contains(&json!("blue")));
    let ns = item["ns"]["NS"].as_array().unwrap();
    assert_eq!(ns.len(), 2);
    assert!(ns.contains(&json!("1")));
    assert!(ns.contains(&json!("2")));
    let bs = item["bs"]["BS"].as_array().unwrap();
    assert_eq!(bs.len(), 2);
    assert!(bs.contains(&json!("YQ==")));
    assert!(bs.contains(&json!("Yg==")));
}

#[tokio::test]
async fn save_snapshot_round_trips_empty_state() {
    let store = Arc::new(RecordingSnapshotStore::default());
    let service = make_service().with_snapshot_store(store.clone());

    assert!(service.save_snapshot().await.unwrap());

    let bytes = load_recorded_snapshot(store.as_ref());
    let restored = service_from_snapshot_bytes(&bytes);
    let tables = call_dynamodb(&restored, "ListTables", json!({})).await;
    assert_eq!(tables["TableNames"].as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn save_snapshot_round_trips_tables_items_and_indexes() {
    let store = Arc::new(RecordingSnapshotStore::default());
    let service = make_service().with_snapshot_store(store.clone());
    populate_snapshot_round_trip_fixture(&service).await;

    assert!(service.save_snapshot().await.unwrap());

    let bytes = load_recorded_snapshot(store.as_ref());
    let restored = service_from_snapshot_bytes(&bytes);
    assert_snapshot_round_trip_fixture(&restored).await;
}

#[tokio::test]
async fn save_snapshot_to_store_round_trips_tables_items_and_indexes() {
    let tmp = TempSnapshotDir::new();
    let snapshot_path = tmp.path().join("dynamodb").join("snapshot.json");
    let store = Arc::new(fakecloud_persistence::DiskSnapshotStore::new(
        snapshot_path.clone(),
    ));
    let service = make_service();
    populate_snapshot_round_trip_fixture(&service).await;

    service.save_snapshot_to_store(store).await.unwrap();

    let bytes = std::fs::read(snapshot_path).unwrap();
    let restored = service_from_snapshot_bytes(&bytes);
    assert_snapshot_round_trip_fixture(&restored).await;
}

#[tokio::test]
async fn save_snapshot_reports_missing_store() {
    let service = make_service();

    assert!(!service.save_snapshot().await.unwrap());
}

#[test]
fn test_parse_update_clauses_set() {
    let clauses = parse_update_clauses("SET #a = :val1, #b = :val2");
    assert_eq!(clauses.len(), 1);
    assert_eq!(clauses[0].0, UpdateAction::Set);
    assert_eq!(clauses[0].1.len(), 2);
}

#[test]
fn test_parse_update_clauses_set_and_remove() {
    let clauses = parse_update_clauses("SET #a = :val1 REMOVE #b");
    assert_eq!(clauses.len(), 2);
    assert_eq!(clauses[0].0, UpdateAction::Set);
    assert_eq!(clauses[1].0, UpdateAction::Remove);
}

#[test]
fn test_parse_update_clauses_list_append_single_assignment() {
    // Before fix: naive comma split tore list_append(#0, :0) at the
    // inner comma, producing two bogus assignments instead of one.
    let clauses = parse_update_clauses("SET #0 = list_append(#0, :0)");
    assert_eq!(clauses.len(), 1);
    assert_eq!(clauses[0].0, UpdateAction::Set);
    assert_eq!(
        clauses[0].1.len(),
        1,
        "list_append(a, b) must be kept as a single assignment, not split at the inner comma"
    );
}

#[test]
fn test_parse_update_clauses_list_append_mixed_with_plain_set() {
    // list_append assignment followed by a plain SET — the comma between
    // the two assignments must still split them, while the comma inside
    // the list_append call must not.
    let clauses = parse_update_clauses("SET #0 = list_append(#0, :new), #1 = :other");
    assert_eq!(clauses.len(), 1);
    assert_eq!(clauses[0].0, UpdateAction::Set);
    assert_eq!(
        clauses[0].1.len(),
        2,
        "two SET assignments: one list_append and one plain"
    );
}

#[test]
fn test_evaluate_key_condition_simple() {
    let mut item = HashMap::new();
    item.insert("pk".to_string(), json!({"S": "user1"}));
    item.insert("sk".to_string(), json!({"S": "order1"}));

    let mut expr_values = HashMap::new();
    expr_values.insert(":pk".to_string(), json!({"S": "user1"}));

    assert!(evaluate_key_condition(
        "pk = :pk",
        &item,
        &HashMap::new(),
        &expr_values,
    ));
}

#[test]
fn test_compare_attribute_values_numbers() {
    let a = json!({"N": "10"});
    let b = json!({"N": "20"});
    assert_eq!(
        compare_attribute_values(Some(&a), Some(&b)),
        std::cmp::Ordering::Less
    );
}

#[test]
fn test_compare_attribute_values_strings() {
    let a = json!({"S": "apple"});
    let b = json!({"S": "banana"});
    assert_eq!(
        compare_attribute_values(Some(&a), Some(&b)),
        std::cmp::Ordering::Less
    );
}

#[test]
fn test_split_on_and() {
    let parts = split_on_and("pk = :pk AND sk > :sk");
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0].trim(), "pk = :pk");
    assert_eq!(parts[1].trim(), "sk > :sk");
}

#[test]
fn test_split_on_and_respects_parentheses() {
    // Before fix: split_on_and would split inside the parens
    let parts = split_on_and("(a = :a AND b = :b) OR c = :c");
    // Should NOT split on the AND inside parentheses
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].trim(), "(a = :a AND b = :b) OR c = :c");
}

#[test]
fn test_evaluate_filter_expression_parenthesized_and_with_or() {
    // (a AND b) OR c — should match when c is true but a is false
    let mut item = HashMap::new();
    item.insert("x".to_string(), json!({"S": "no"}));
    item.insert("y".to_string(), json!({"S": "no"}));
    item.insert("z".to_string(), json!({"S": "yes"}));

    let mut expr_values = HashMap::new();
    expr_values.insert(":yes".to_string(), json!({"S": "yes"}));

    // x=yes AND y=yes => false, but z=yes => true => overall true
    let result = evaluate_filter_expression(
        "(x = :yes AND y = :yes) OR z = :yes",
        &item,
        &HashMap::new(),
        &expr_values,
    );
    assert!(result, "should match because z = :yes is true");

    // x=yes AND y=yes => false, z=yes => false => overall false
    let mut item2 = HashMap::new();
    item2.insert("x".to_string(), json!({"S": "no"}));
    item2.insert("y".to_string(), json!({"S": "no"}));
    item2.insert("z".to_string(), json!({"S": "no"}));

    let result2 = evaluate_filter_expression(
        "(x = :yes AND y = :yes) OR z = :yes",
        &item2,
        &HashMap::new(),
        &expr_values,
    );
    assert!(!result2, "should not match because nothing is true");
}

#[test]
fn test_project_item_nested_path() {
    // Item with a list attribute containing maps
    let mut item = HashMap::new();
    item.insert("pk".to_string(), json!({"S": "key1"}));
    item.insert(
        "data".to_string(),
        json!({"L": [{"M": {"name": {"S": "Alice"}, "age": {"N": "30"}}}, {"M": {"name": {"S": "Bob"}}}]}),
    );

    let body = json!({
        "ProjectionExpression": "data[0].name"
    });

    let projected = project_item(&item, &body);
    // Should contain data[0].name = "Alice", not the entire data[0] element
    let name = projected
        .get("data")
        .and_then(|v| v.get("L"))
        .and_then(|v| v.get(0))
        .and_then(|v| v.get("M"))
        .and_then(|v| v.get("name"))
        .and_then(|v| v.get("S"))
        .and_then(|v| v.as_str());
    assert_eq!(name, Some("Alice"));

    // Should NOT contain the "age" field
    let age = projected
        .get("data")
        .and_then(|v| v.get("L"))
        .and_then(|v| v.get(0))
        .and_then(|v| v.get("M"))
        .and_then(|v| v.get("age"));
    assert!(age.is_none(), "age should not be present in projection");
}

#[test]
fn test_resolve_nested_path_map() {
    let mut item = HashMap::new();
    item.insert(
        "info".to_string(),
        json!({"M": {"address": {"M": {"city": {"S": "NYC"}}}}}),
    );

    let result = resolve_nested_path(&item, "info.address.city");
    assert_eq!(result, Some(json!({"S": "NYC"})));
}

#[test]
fn test_resolve_nested_path_list_then_map() {
    let mut item = HashMap::new();
    item.insert(
        "items".to_string(),
        json!({"L": [{"M": {"sku": {"S": "ABC"}}}]}),
    );

    let result = resolve_nested_path(&item, "items[0].sku");
    assert_eq!(result, Some(json!({"S": "ABC"})));
}

#[test]
fn test_resolve_path_alias_with_dot_is_top_level_attr() {
    // Top-level attribute name literally contains a dot; user aliases it
    // via ExpressionAttributeNames and references the alias. Must resolve
    // to the top-level attribute, NOT be walked as a nested path.
    let mut item = HashMap::new();
    item.insert("Safety.Warning".to_string(), json!({"S": "high"}));
    let mut names = HashMap::new();
    names.insert("#sw".to_string(), "Safety.Warning".to_string());

    let result = resolve_path("#sw", &item, &names);
    assert_eq!(result, Some(json!({"S": "high"})));
}

#[test]
fn test_resolve_path_dotted_expression_still_walks_nested() {
    // When the expression itself contains `.`, we still walk the nested
    // path (the dot is a path separator, not part of an attribute name).
    let mut item = HashMap::new();
    item.insert("profile".to_string(), json!({"M": {"email": {"S": "x@y"}}}));
    let names = HashMap::new();

    let result = resolve_path("profile.email", &item, &names);
    assert_eq!(result, Some(json!({"S": "x@y"})));
}

#[test]
fn test_project_item_alias_with_dot_is_top_level_attr() {
    // Same invariant must hold for ProjectionExpression.
    let mut item = HashMap::new();
    item.insert("Safety.Warning".to_string(), json!({"S": "high"}));
    item.insert("other".to_string(), json!({"S": "ignored"}));
    let body = json!({
        "ProjectionExpression": "#sw",
        "ExpressionAttributeNames": {"#sw": "Safety.Warning"},
    });

    let projected = project_item(&item, &body);
    assert_eq!(projected.get("Safety.Warning"), Some(&json!({"S": "high"})));
    assert!(!projected.contains_key("other"));
}

// -- Integration-style tests using DynamoDbService --

use crate::state::SharedDynamoDbState;
use parking_lot::RwLock;

fn make_service() -> DynamoDbService {
    let state: SharedDynamoDbState = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    DynamoDbService::new(state)
}

fn make_request(action: &str, body: Value) -> AwsRequest {
    AwsRequest {
        service: "dynamodb".to_string(),
        action: action.to_string(),
        region: "us-east-1".to_string(),
        account_id: "123456789012".to_string(),
        request_id: "test-id".to_string(),
        headers: http::HeaderMap::new(),
        query_params: HashMap::new(),
        body: serde_json::to_vec(&body).unwrap().into(),
        body_stream: parking_lot::Mutex::new(None),
        path_segments: vec![],
        raw_path: "/".to_string(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

fn create_test_table(svc: &DynamoDbService) {
    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "test-table",
            "KeySchema": [
                { "AttributeName": "pk", "KeyType": "HASH" }
            ],
            "AttributeDefinitions": [
                { "AttributeName": "pk", "AttributeType": "S" }
            ],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    );
    svc.create_table(&req).unwrap();
}

#[test]
fn describe_table_returns_stable_table_id_and_active_warm_throughput() {
    let svc = make_service();
    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "warm-throughput-table",
            "KeySchema": [
                { "AttributeName": "pk", "KeyType": "HASH" }
            ],
            "AttributeDefinitions": [
                { "AttributeName": "pk", "AttributeType": "S" }
            ],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    );
    let create_resp = svc.create_table(&req).unwrap();
    let create_body: Value = serde_json::from_slice(create_resp.body.expect_bytes()).unwrap();
    let create_table = &create_body["TableDescription"];

    assert_eq!(create_table["TableStatus"], "ACTIVE");
    assert_eq!(create_table["WarmThroughput"]["Status"], "ACTIVE");
    let table_id = create_table["TableId"].as_str().unwrap().to_string();
    assert!(!table_id.is_empty());

    let describe_req = make_request(
        "DescribeTable",
        json!({ "TableName": "warm-throughput-table" }),
    );
    let describe_resp = svc.describe_table(&describe_req).unwrap();
    let describe_body: Value = serde_json::from_slice(describe_resp.body.expect_bytes()).unwrap();
    let described_table = &describe_body["Table"];

    assert_eq!(described_table["TableStatus"], "ACTIVE");
    assert_eq!(described_table["WarmThroughput"]["Status"], "ACTIVE");
    assert_eq!(described_table["TableId"], table_id);

    let describe_resp_again = svc.describe_table(&describe_req).unwrap();
    let describe_body_again: Value =
        serde_json::from_slice(describe_resp_again.body.expect_bytes()).unwrap();
    assert_eq!(describe_body_again["Table"]["TableId"], table_id);
}

#[test]
fn delete_item_return_values_all_old() {
    let svc = make_service();
    create_test_table(&svc);

    // Put an item
    let req = make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": {
                "pk": { "S": "key1" },
                "name": { "S": "Alice" },
                "age": { "N": "30" }
            }
        }),
    );
    svc.put_item(&req).unwrap();

    // Delete with ReturnValues=ALL_OLD
    let req = make_request(
        "DeleteItem",
        json!({
            "TableName": "test-table",
            "Key": { "pk": { "S": "key1" } },
            "ReturnValues": "ALL_OLD"
        }),
    );
    let resp = svc.delete_item(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();

    // Verify the old item is returned
    let attrs = &body["Attributes"];
    assert_eq!(attrs["pk"]["S"].as_str().unwrap(), "key1");
    assert_eq!(attrs["name"]["S"].as_str().unwrap(), "Alice");
    assert_eq!(attrs["age"]["N"].as_str().unwrap(), "30");

    // Verify the item is actually deleted
    let req = make_request(
        "GetItem",
        json!({
            "TableName": "test-table",
            "Key": { "pk": { "S": "key1" } }
        }),
    );
    let resp = svc.get_item(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body.get("Item").is_none(), "item should be deleted");
}

#[test]
fn transact_get_items_returns_existing_and_missing() {
    let svc = make_service();
    create_test_table(&svc);

    // Put one item
    let req = make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": {
                "pk": { "S": "exists" },
                "val": { "S": "hello" }
            }
        }),
    );
    svc.put_item(&req).unwrap();

    let req = make_request(
        "TransactGetItems",
        json!({
            "TransactItems": [
                { "Get": { "TableName": "test-table", "Key": { "pk": { "S": "exists" } } } },
                { "Get": { "TableName": "test-table", "Key": { "pk": { "S": "missing" } } } }
            ]
        }),
    );
    let resp = svc.transact_get_items(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let responses = body["Responses"].as_array().unwrap();
    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0]["Item"]["pk"]["S"].as_str().unwrap(), "exists");
    assert!(responses[1].get("Item").is_none());
}

#[test]
fn transact_write_items_put_and_delete() {
    let svc = make_service();
    create_test_table(&svc);

    // Put initial item
    let req = make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": {
                "pk": { "S": "to-delete" },
                "val": { "S": "bye" }
            }
        }),
    );
    svc.put_item(&req).unwrap();

    // TransactWrite: put new + delete existing
    let req = make_request(
        "TransactWriteItems",
        json!({
            "TransactItems": [
                {
                    "Put": {
                        "TableName": "test-table",
                        "Item": {
                            "pk": { "S": "new-item" },
                            "val": { "S": "hi" }
                        }
                    }
                },
                {
                    "Delete": {
                        "TableName": "test-table",
                        "Key": { "pk": { "S": "to-delete" } }
                    }
                }
            ]
        }),
    );
    let resp = svc.transact_write_items(&req).unwrap();
    assert_eq!(resp.status, StatusCode::OK);

    // Verify new item exists
    let req = make_request(
        "GetItem",
        json!({
            "TableName": "test-table",
            "Key": { "pk": { "S": "new-item" } }
        }),
    );
    let resp = svc.get_item(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Item"]["val"]["S"].as_str().unwrap(), "hi");

    // Verify deleted item is gone
    let req = make_request(
        "GetItem",
        json!({
            "TableName": "test-table",
            "Key": { "pk": { "S": "to-delete" } }
        }),
    );
    let resp = svc.get_item(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body.get("Item").is_none());
}

#[test]
fn transact_write_items_condition_check_failure() {
    let svc = make_service();
    create_test_table(&svc);

    // TransactWrite with a ConditionCheck that fails (item doesn't exist)
    let req = make_request(
        "TransactWriteItems",
        json!({
            "TransactItems": [
                {
                    "ConditionCheck": {
                        "TableName": "test-table",
                        "Key": { "pk": { "S": "nonexistent" } },
                        "ConditionExpression": "attribute_exists(pk)"
                    }
                }
            ]
        }),
    );
    let resp = svc.transact_write_items(&req).unwrap();
    // Should be a 400 error response
    assert_eq!(resp.status, StatusCode::BAD_REQUEST);
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["__type"].as_str().unwrap(),
        "TransactionCanceledException"
    );
    assert!(body["CancellationReasons"].as_array().is_some());
}

#[test]
fn update_and_describe_time_to_live() {
    let svc = make_service();
    create_test_table(&svc);

    // Enable TTL
    let req = make_request(
        "UpdateTimeToLive",
        json!({
            "TableName": "test-table",
            "TimeToLiveSpecification": {
                "AttributeName": "ttl",
                "Enabled": true
            }
        }),
    );
    let resp = svc.update_time_to_live(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["TimeToLiveSpecification"]["AttributeName"]
            .as_str()
            .unwrap(),
        "ttl"
    );
    assert!(body["TimeToLiveSpecification"]["Enabled"]
        .as_bool()
        .unwrap());

    // Describe TTL
    let req = make_request("DescribeTimeToLive", json!({ "TableName": "test-table" }));
    let resp = svc.describe_time_to_live(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["TimeToLiveDescription"]["TimeToLiveStatus"]
            .as_str()
            .unwrap(),
        "ENABLED"
    );
    assert_eq!(
        body["TimeToLiveDescription"]["AttributeName"]
            .as_str()
            .unwrap(),
        "ttl"
    );

    // Disable TTL
    let req = make_request(
        "UpdateTimeToLive",
        json!({
            "TableName": "test-table",
            "TimeToLiveSpecification": {
                "AttributeName": "ttl",
                "Enabled": false
            }
        }),
    );
    svc.update_time_to_live(&req).unwrap();

    let req = make_request("DescribeTimeToLive", json!({ "TableName": "test-table" }));
    let resp = svc.describe_time_to_live(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["TimeToLiveDescription"]["TimeToLiveStatus"]
            .as_str()
            .unwrap(),
        "DISABLED"
    );
}

#[test]
fn resource_policy_lifecycle() {
    let svc = make_service();
    create_test_table(&svc);

    let table_arn = {
        let __mas = svc.state.read();
        let state = __mas.default_ref();
        state.tables.get("test-table").unwrap().arn.clone()
    };

    // Put policy
    let policy_doc = r#"{"Version":"2012-10-17","Statement":[]}"#;
    let req = make_request(
        "PutResourcePolicy",
        json!({
            "ResourceArn": table_arn,
            "Policy": policy_doc
        }),
    );
    let resp = svc.put_resource_policy(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["RevisionId"].as_str().is_some());

    // Get policy
    let req = make_request("GetResourcePolicy", json!({ "ResourceArn": table_arn }));
    let resp = svc.get_resource_policy(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Policy"].as_str().unwrap(), policy_doc);

    // Delete policy
    let req = make_request("DeleteResourcePolicy", json!({ "ResourceArn": table_arn }));
    svc.delete_resource_policy(&req).unwrap();

    // Get should now return PolicyNotFoundException, matching real DynamoDB.
    let req = make_request("GetResourcePolicy", json!({ "ResourceArn": table_arn }));
    match svc.get_resource_policy(&req) {
        Err(e) => assert_eq!(e.code(), "PolicyNotFoundException"),
        Ok(_) => panic!("GetResourcePolicy after delete must error"),
    }
}

#[test]
fn describe_endpoints_uses_request_region() {
    let svc = make_service();
    let mut req = make_request("DescribeEndpoints", json!({}));
    req.region = "eu-west-1".to_string();
    let resp = svc.describe_endpoints(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["Endpoints"][0]["Address"],
        "dynamodb.eu-west-1.amazonaws.com"
    );
    assert_eq!(body["Endpoints"][0]["CachePeriodInMinutes"], 1440);
}

#[test]
fn describe_limits_default_account_cap() {
    let svc = make_service();
    let req = make_request("DescribeLimits", json!({}));
    let resp = svc.describe_limits(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["AccountMaxReadCapacityUnits"], 80_000);
    assert_eq!(body["TableMaxReadCapacityUnits"], 40_000);
}

#[test]
fn describe_limits_smaller_region_lower_cap() {
    let svc = make_service();
    let mut req = make_request("DescribeLimits", json!({}));
    req.region = "ap-south-1".to_string();
    let resp = svc.describe_limits(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["AccountMaxReadCapacityUnits"], 40_000);
    assert_eq!(body["AccountMaxWriteCapacityUnits"], 40_000);
}

#[test]
fn backup_lifecycle() {
    let svc = make_service();
    create_test_table(&svc);

    // Create backup
    let req = make_request(
        "CreateBackup",
        json!({ "TableName": "test-table", "BackupName": "my-backup" }),
    );
    let resp = svc.create_backup(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let backup_arn = body["BackupDetails"]["BackupArn"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(body["BackupDetails"]["BackupStatus"], "AVAILABLE");

    // Describe backup
    let req = make_request("DescribeBackup", json!({ "BackupArn": backup_arn }));
    let resp = svc.describe_backup(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["BackupDescription"]["BackupDetails"]["BackupName"],
        "my-backup"
    );

    // List backups
    let req = make_request("ListBackups", json!({}));
    let resp = svc.list_backups(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["BackupSummaries"].as_array().unwrap().len(), 1);

    // Restore from backup
    let req = make_request(
        "RestoreTableFromBackup",
        json!({ "BackupArn": backup_arn, "TargetTableName": "restored-table" }),
    );
    svc.restore_table_from_backup(&req).unwrap();

    // Verify restored table exists
    let req = make_request("DescribeTable", json!({ "TableName": "restored-table" }));
    let resp = svc.describe_table(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Table"]["TableStatus"], "ACTIVE");

    // Delete backup
    let req = make_request("DeleteBackup", json!({ "BackupArn": backup_arn }));
    svc.delete_backup(&req).unwrap();

    // List should be empty
    let req = make_request("ListBackups", json!({}));
    let resp = svc.list_backups(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["BackupSummaries"].as_array().unwrap().len(), 0);
}

#[test]
fn continuous_backups() {
    let svc = make_service();
    create_test_table(&svc);

    // Initially disabled
    let req = make_request(
        "DescribeContinuousBackups",
        json!({ "TableName": "test-table" }),
    );
    let resp = svc.describe_continuous_backups(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]
            ["PointInTimeRecoveryStatus"],
        "DISABLED"
    );

    // Enable
    let req = make_request(
        "UpdateContinuousBackups",
        json!({
            "TableName": "test-table",
            "PointInTimeRecoverySpecification": {
                "PointInTimeRecoveryEnabled": true
            }
        }),
    );
    svc.update_continuous_backups(&req).unwrap();

    // Verify
    let req = make_request(
        "DescribeContinuousBackups",
        json!({ "TableName": "test-table" }),
    );
    let resp = svc.describe_continuous_backups(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]
            ["PointInTimeRecoveryStatus"],
        "ENABLED"
    );
}

#[test]
fn restore_table_to_point_in_time() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "RestoreTableToPointInTime",
        json!({
            "SourceTableName": "test-table",
            "TargetTableName": "pitr-restored"
        }),
    );
    svc.restore_table_to_point_in_time(&req).unwrap();

    let req = make_request("DescribeTable", json!({ "TableName": "pitr-restored" }));
    let resp = svc.describe_table(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Table"]["TableStatus"], "ACTIVE");
}

#[test]
fn global_table_lifecycle() {
    let svc = make_service();

    // Create global table
    let req = make_request(
        "CreateGlobalTable",
        json!({
            "GlobalTableName": "my-global",
            "ReplicationGroup": [
                { "RegionName": "us-east-1" },
                { "RegionName": "eu-west-1" }
            ]
        }),
    );
    let resp = svc.create_global_table(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["GlobalTableDescription"]["GlobalTableStatus"],
        "ACTIVE"
    );

    // Describe
    let req = make_request(
        "DescribeGlobalTable",
        json!({ "GlobalTableName": "my-global" }),
    );
    let resp = svc.describe_global_table(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["GlobalTableDescription"]["ReplicationGroup"]
            .as_array()
            .unwrap()
            .len(),
        2
    );

    // List
    let req = make_request("ListGlobalTables", json!({}));
    let resp = svc.list_global_tables(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["GlobalTables"].as_array().unwrap().len(), 1);

    // Update - add a region
    let req = make_request(
        "UpdateGlobalTable",
        json!({
            "GlobalTableName": "my-global",
            "ReplicaUpdates": [
                { "Create": { "RegionName": "ap-southeast-1" } }
            ]
        }),
    );
    let resp = svc.update_global_table(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["GlobalTableDescription"]["ReplicationGroup"]
            .as_array()
            .unwrap()
            .len(),
        3
    );

    // Describe settings
    let req = make_request(
        "DescribeGlobalTableSettings",
        json!({ "GlobalTableName": "my-global" }),
    );
    let resp = svc.describe_global_table_settings(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ReplicaSettings"].as_array().unwrap().len(), 3);

    // Update settings (no-op, just verify no error)
    let req = make_request(
        "UpdateGlobalTableSettings",
        json!({ "GlobalTableName": "my-global" }),
    );
    svc.update_global_table_settings(&req).unwrap();
}

#[test]
fn table_replica_auto_scaling() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "DescribeTableReplicaAutoScaling",
        json!({ "TableName": "test-table" }),
    );
    let resp = svc.describe_table_replica_auto_scaling(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["TableAutoScalingDescription"]["TableName"],
        "test-table"
    );

    let req = make_request(
        "UpdateTableReplicaAutoScaling",
        json!({ "TableName": "test-table" }),
    );
    svc.update_table_replica_auto_scaling(&req).unwrap();
}

#[test]
fn kinesis_streaming_lifecycle() {
    let svc = make_service();
    create_test_table(&svc);

    // Enable
    let req = make_request(
        "EnableKinesisStreamingDestination",
        json!({
            "TableName": "test-table",
            "StreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream"
        }),
    );
    let resp = svc.enable_kinesis_streaming_destination(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["DestinationStatus"], "ACTIVE");

    // Describe
    let req = make_request(
        "DescribeKinesisStreamingDestination",
        json!({ "TableName": "test-table" }),
    );
    let resp = svc.describe_kinesis_streaming_destination(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["KinesisDataStreamDestinations"]
            .as_array()
            .unwrap()
            .len(),
        1
    );

    // Update
    let req = make_request(
        "UpdateKinesisStreamingDestination",
        json!({
            "TableName": "test-table",
            "StreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream",
            "UpdateKinesisStreamingConfiguration": {
                "ApproximateCreationDateTimePrecision": "MICROSECOND"
            }
        }),
    );
    svc.update_kinesis_streaming_destination(&req).unwrap();

    // Disable
    let req = make_request(
        "DisableKinesisStreamingDestination",
        json!({
            "TableName": "test-table",
            "StreamArn": "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream"
        }),
    );
    let resp = svc.disable_kinesis_streaming_destination(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["DestinationStatus"], "DISABLED");
}

#[test]
fn contributor_insights_lifecycle() {
    let svc = make_service();
    create_test_table(&svc);

    // Initially disabled
    let req = make_request(
        "DescribeContributorInsights",
        json!({ "TableName": "test-table" }),
    );
    let resp = svc.describe_contributor_insights(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ContributorInsightsStatus"], "DISABLED");

    // Enable
    let req = make_request(
        "UpdateContributorInsights",
        json!({
            "TableName": "test-table",
            "ContributorInsightsAction": "ENABLE"
        }),
    );
    let resp = svc.update_contributor_insights(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ContributorInsightsStatus"], "ENABLED");

    // List
    let req = make_request("ListContributorInsights", json!({}));
    let resp = svc.list_contributor_insights(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["ContributorInsightsSummaries"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn export_lifecycle() {
    let svc = make_service();
    create_test_table(&svc);

    let table_arn = "arn:aws:dynamodb:us-east-1:123456789012:table/test-table".to_string();

    // Export
    let req = make_request(
        "ExportTableToPointInTime",
        json!({
            "TableArn": table_arn,
            "S3Bucket": "my-bucket"
        }),
    );
    let resp = svc.export_table_to_point_in_time(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let export_arn = body["ExportDescription"]["ExportArn"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(body["ExportDescription"]["ExportStatus"], "COMPLETED");

    // Describe
    let req = make_request("DescribeExport", json!({ "ExportArn": export_arn }));
    let resp = svc.describe_export(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ExportDescription"]["S3Bucket"], "my-bucket");

    // List
    let req = make_request("ListExports", json!({}));
    let resp = svc.list_exports(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ExportSummaries"].as_array().unwrap().len(), 1);
}

#[test]
fn import_lifecycle() {
    let svc = make_service();

    let req = make_request(
        "ImportTable",
        json!({
            "InputFormat": "DYNAMODB_JSON",
            "S3BucketSource": { "S3Bucket": "import-bucket" },
            "TableCreationParameters": {
                "TableName": "imported-table",
                "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
                "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }]
            }
        }),
    );
    let resp = svc.import_table(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let import_arn = body["ImportTableDescription"]["ImportArn"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(body["ImportTableDescription"]["ImportStatus"], "COMPLETED");

    // Describe import
    let req = make_request("DescribeImport", json!({ "ImportArn": import_arn }));
    let resp = svc.describe_import(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ImportTableDescription"]["ImportStatus"], "COMPLETED");

    // List imports
    let req = make_request("ListImports", json!({}));
    let resp = svc.list_imports(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ImportSummaryList"].as_array().unwrap().len(), 1);

    // Verify the table was created
    let req = make_request("DescribeTable", json!({ "TableName": "imported-table" }));
    let resp = svc.describe_table(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Table"]["TableStatus"], "ACTIVE");
}

#[test]
fn backup_restore_preserves_items() {
    let svc = make_service();
    create_test_table(&svc);

    // Put 3 items
    for i in 1..=3 {
        let req = make_request(
            "PutItem",
            json!({
                "TableName": "test-table",
                "Item": {
                    "pk": { "S": format!("key{i}") },
                    "data": { "S": format!("value{i}") }
                }
            }),
        );
        svc.put_item(&req).unwrap();
    }

    // Create backup
    let req = make_request(
        "CreateBackup",
        json!({
            "TableName": "test-table",
            "BackupName": "my-backup"
        }),
    );
    let resp = svc.create_backup(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let backup_arn = body["BackupDetails"]["BackupArn"]
        .as_str()
        .unwrap()
        .to_string();

    // Delete all items from the original table
    for i in 1..=3 {
        let req = make_request(
            "DeleteItem",
            json!({
                "TableName": "test-table",
                "Key": { "pk": { "S": format!("key{i}") } }
            }),
        );
        svc.delete_item(&req).unwrap();
    }

    // Verify original table is empty
    let req = make_request("Scan", json!({ "TableName": "test-table" }));
    let resp = svc.scan(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Count"], 0);

    // Restore from backup
    let req = make_request(
        "RestoreTableFromBackup",
        json!({
            "BackupArn": backup_arn,
            "TargetTableName": "restored-table"
        }),
    );
    svc.restore_table_from_backup(&req).unwrap();

    // Scan restored table — should have 3 items
    let req = make_request("Scan", json!({ "TableName": "restored-table" }));
    let resp = svc.scan(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Count"], 3);
    assert_eq!(body["Items"].as_array().unwrap().len(), 3);
}

#[test]
fn global_table_replicates_writes() {
    let svc = make_service();
    create_test_table(&svc);

    // Create global table with replicas
    let req = make_request(
        "CreateGlobalTable",
        json!({
            "GlobalTableName": "test-table",
            "ReplicationGroup": [
                { "RegionName": "us-east-1" },
                { "RegionName": "eu-west-1" }
            ]
        }),
    );
    let resp = svc.create_global_table(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["GlobalTableDescription"]["GlobalTableStatus"],
        "ACTIVE"
    );

    // Put an item
    let req = make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": {
                "pk": { "S": "replicated-key" },
                "data": { "S": "replicated-value" }
            }
        }),
    );
    svc.put_item(&req).unwrap();

    // Verify the item is readable (since all replicas share the same table)
    let req = make_request(
        "GetItem",
        json!({
            "TableName": "test-table",
            "Key": { "pk": { "S": "replicated-key" } }
        }),
    );
    let resp = svc.get_item(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Item"]["pk"]["S"], "replicated-key");
    assert_eq!(body["Item"]["data"]["S"], "replicated-value");
}

#[test]
fn contributor_insights_tracks_access() {
    let svc = make_service();
    create_test_table(&svc);

    // Enable contributor insights
    let req = make_request(
        "UpdateContributorInsights",
        json!({
            "TableName": "test-table",
            "ContributorInsightsAction": "ENABLE"
        }),
    );
    svc.update_contributor_insights(&req).unwrap();

    // Put items with different partition keys
    for key in &["alpha", "beta", "alpha", "alpha", "beta"] {
        let req = make_request(
            "PutItem",
            json!({
                "TableName": "test-table",
                "Item": {
                    "pk": { "S": key },
                    "data": { "S": "value" }
                }
            }),
        );
        svc.put_item(&req).unwrap();
    }

    // Get items (to also track read access)
    for _ in 0..3 {
        let req = make_request(
            "GetItem",
            json!({
                "TableName": "test-table",
                "Key": { "pk": { "S": "alpha" } }
            }),
        );
        svc.get_item(&req).unwrap();
    }

    // Describe contributor insights — should show top contributors
    let req = make_request(
        "DescribeContributorInsights",
        json!({ "TableName": "test-table" }),
    );
    let resp = svc.describe_contributor_insights(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ContributorInsightsStatus"], "ENABLED");

    let contributors = body["TopContributors"].as_array().unwrap();
    assert!(
        !contributors.is_empty(),
        "TopContributors should not be empty"
    );

    // alpha was accessed 3 (put) + 3 (get) = 6 times, beta 2 times
    // alpha should be the top contributor
    let top = &contributors[0];
    assert!(top["Count"].as_u64().unwrap() > 0);

    // Verify the rule list is populated
    let rules = body["ContributorInsightsRuleList"].as_array().unwrap();
    assert!(!rules.is_empty());
}

#[test]
fn contributor_insights_not_tracked_when_disabled() {
    let svc = make_service();
    create_test_table(&svc);

    // Put items without enabling insights
    let req = make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": {
                "pk": { "S": "key1" },
                "data": { "S": "value" }
            }
        }),
    );
    svc.put_item(&req).unwrap();

    // Describe — should show empty contributors
    let req = make_request(
        "DescribeContributorInsights",
        json!({ "TableName": "test-table" }),
    );
    let resp = svc.describe_contributor_insights(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ContributorInsightsStatus"], "DISABLED");

    let contributors = body["TopContributors"].as_array().unwrap();
    assert!(contributors.is_empty());
}

#[test]
fn contributor_insights_disabled_table_no_counters_after_scan() {
    let svc = make_service();
    create_test_table(&svc);

    // Put items
    for key in &["alpha", "beta"] {
        let req = make_request(
            "PutItem",
            json!({
                "TableName": "test-table",
                "Item": { "pk": { "S": key } }
            }),
        );
        svc.put_item(&req).unwrap();
    }

    // Enable insights, then scan, then disable, then check counters are cleared
    let req = make_request(
        "UpdateContributorInsights",
        json!({
            "TableName": "test-table",
            "ContributorInsightsAction": "ENABLE"
        }),
    );
    svc.update_contributor_insights(&req).unwrap();

    // Scan to trigger counter collection
    let req = make_request("Scan", json!({ "TableName": "test-table" }));
    svc.scan(&req).unwrap();

    // Verify counters were collected
    let req = make_request(
        "DescribeContributorInsights",
        json!({ "TableName": "test-table" }),
    );
    let resp = svc.describe_contributor_insights(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let contributors = body["TopContributors"].as_array().unwrap();
    assert!(
        !contributors.is_empty(),
        "counters should be non-empty while enabled"
    );

    // Disable insights (this clears counters)
    let req = make_request(
        "UpdateContributorInsights",
        json!({
            "TableName": "test-table",
            "ContributorInsightsAction": "DISABLE"
        }),
    );
    svc.update_contributor_insights(&req).unwrap();

    // Scan again -- should NOT accumulate counters since insights is disabled
    let req = make_request("Scan", json!({ "TableName": "test-table" }));
    svc.scan(&req).unwrap();

    // Verify counters are still empty
    let req = make_request(
        "DescribeContributorInsights",
        json!({ "TableName": "test-table" }),
    );
    let resp = svc.describe_contributor_insights(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let contributors = body["TopContributors"].as_array().unwrap();
    assert!(
        contributors.is_empty(),
        "counters should be empty after disabling insights"
    );
}

#[test]
fn scan_pagination_with_limit() {
    let svc = make_service();
    create_test_table(&svc);

    // Insert 5 items
    for i in 0..5 {
        let req = make_request(
            "PutItem",
            json!({
                "TableName": "test-table",
                "Item": {
                    "pk": { "S": format!("item{i}") },
                    "data": { "S": format!("value{i}") }
                }
            }),
        );
        svc.put_item(&req).unwrap();
    }

    // Scan with limit=2
    let req = make_request("Scan", json!({ "TableName": "test-table", "Limit": 2 }));
    let resp = svc.scan(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Count"], 2);
    assert!(
        body["LastEvaluatedKey"].is_object(),
        "should have LastEvaluatedKey when limit < total items"
    );
    assert!(body["LastEvaluatedKey"]["pk"].is_object());

    // Page through all items
    let mut all_items: Vec<Value> = body["Items"].as_array().unwrap().clone();
    let mut lek = body["LastEvaluatedKey"].clone();

    while lek.is_object() {
        let req = make_request(
            "Scan",
            json!({
                "TableName": "test-table",
                "Limit": 2,
                "ExclusiveStartKey": lek
            }),
        );
        let resp = svc.scan(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        all_items.extend(body["Items"].as_array().unwrap().iter().cloned());
        lek = body["LastEvaluatedKey"].clone();
    }

    assert_eq!(
        all_items.len(),
        5,
        "should retrieve all 5 items via pagination"
    );
}

#[test]
fn scan_no_pagination_when_all_fit() {
    let svc = make_service();
    create_test_table(&svc);

    for i in 0..3 {
        let req = make_request(
            "PutItem",
            json!({
                "TableName": "test-table",
                "Item": {
                    "pk": { "S": format!("item{i}") }
                }
            }),
        );
        svc.put_item(&req).unwrap();
    }

    // Scan with limit > item count
    let req = make_request("Scan", json!({ "TableName": "test-table", "Limit": 10 }));
    let resp = svc.scan(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Count"], 3);
    assert!(
        body["LastEvaluatedKey"].is_null(),
        "should not have LastEvaluatedKey when all items fit"
    );

    // Scan without limit
    let req = make_request("Scan", json!({ "TableName": "test-table" }));
    let resp = svc.scan(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Count"], 3);
    assert!(body["LastEvaluatedKey"].is_null());
}

fn create_composite_table(svc: &DynamoDbService) {
    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "composite-table",
            "KeySchema": [
                { "AttributeName": "pk", "KeyType": "HASH" },
                { "AttributeName": "sk", "KeyType": "RANGE" }
            ],
            "AttributeDefinitions": [
                { "AttributeName": "pk", "AttributeType": "S" },
                { "AttributeName": "sk", "AttributeType": "S" }
            ],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    );
    svc.create_table(&req).unwrap();
}

#[test]
fn query_pagination_with_composite_key() {
    let svc = make_service();
    create_composite_table(&svc);

    // Insert 5 items under the same partition key
    for i in 0..5 {
        let req = make_request(
            "PutItem",
            json!({
                "TableName": "composite-table",
                "Item": {
                    "pk": { "S": "user1" },
                    "sk": { "S": format!("item{i:03}") },
                    "data": { "S": format!("value{i}") }
                }
            }),
        );
        svc.put_item(&req).unwrap();
    }

    // Query with limit=2
    let req = make_request(
        "Query",
        json!({
            "TableName": "composite-table",
            "KeyConditionExpression": "pk = :pk",
            "ExpressionAttributeValues": { ":pk": { "S": "user1" } },
            "Limit": 2
        }),
    );
    let resp = svc.query(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Count"], 2);
    assert!(body["LastEvaluatedKey"].is_object());
    assert!(body["LastEvaluatedKey"]["pk"].is_object());
    assert!(body["LastEvaluatedKey"]["sk"].is_object());

    // Page through all items
    let mut all_items: Vec<Value> = body["Items"].as_array().unwrap().clone();
    let mut lek = body["LastEvaluatedKey"].clone();

    while lek.is_object() {
        let req = make_request(
            "Query",
            json!({
                "TableName": "composite-table",
                "KeyConditionExpression": "pk = :pk",
                "ExpressionAttributeValues": { ":pk": { "S": "user1" } },
                "Limit": 2,
                "ExclusiveStartKey": lek
            }),
        );
        let resp = svc.query(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        all_items.extend(body["Items"].as_array().unwrap().iter().cloned());
        lek = body["LastEvaluatedKey"].clone();
    }

    assert_eq!(
        all_items.len(),
        5,
        "should retrieve all 5 items via pagination"
    );

    // Verify items came back sorted by sort key
    let sks: Vec<String> = all_items
        .iter()
        .map(|item| item["sk"]["S"].as_str().unwrap().to_string())
        .collect();
    let mut sorted = sks.clone();
    sorted.sort();
    assert_eq!(sks, sorted, "items should be sorted by sort key");
}

#[test]
fn query_no_pagination_when_all_fit() {
    let svc = make_service();
    create_composite_table(&svc);

    for i in 0..2 {
        let req = make_request(
            "PutItem",
            json!({
                "TableName": "composite-table",
                "Item": {
                    "pk": { "S": "user1" },
                    "sk": { "S": format!("item{i}") }
                }
            }),
        );
        svc.put_item(&req).unwrap();
    }

    let req = make_request(
        "Query",
        json!({
            "TableName": "composite-table",
            "KeyConditionExpression": "pk = :pk",
            "ExpressionAttributeValues": { ":pk": { "S": "user1" } },
            "Limit": 10
        }),
    );
    let resp = svc.query(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Count"], 2);
    assert!(
        body["LastEvaluatedKey"].is_null(),
        "should not have LastEvaluatedKey when all items fit"
    );
}

fn create_gsi_table(svc: &DynamoDbService) {
    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "gsi-table",
            "KeySchema": [
                { "AttributeName": "pk", "KeyType": "HASH" }
            ],
            "AttributeDefinitions": [
                { "AttributeName": "pk", "AttributeType": "S" },
                { "AttributeName": "gsi_pk", "AttributeType": "S" },
                { "AttributeName": "gsi_sk", "AttributeType": "S" }
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": [
                {
                    "IndexName": "gsi-index",
                    "KeySchema": [
                        { "AttributeName": "gsi_pk", "KeyType": "HASH" },
                        { "AttributeName": "gsi_sk", "KeyType": "RANGE" }
                    ],
                    "Projection": { "ProjectionType": "ALL" }
                }
            ]
        }),
    );
    svc.create_table(&req).unwrap();
}

#[test]
fn gsi_query_last_evaluated_key_includes_table_pk() {
    let svc = make_service();
    create_gsi_table(&svc);

    // Insert 3 items with the SAME GSI key but different table PKs
    for i in 0..3 {
        let req = make_request(
            "PutItem",
            json!({
                "TableName": "gsi-table",
                "Item": {
                    "pk": { "S": format!("item{i}") },
                    "gsi_pk": { "S": "shared" },
                    "gsi_sk": { "S": "sort" }
                }
            }),
        );
        svc.put_item(&req).unwrap();
    }

    // Query GSI with Limit=1 to trigger pagination
    let req = make_request(
        "Query",
        json!({
            "TableName": "gsi-table",
            "IndexName": "gsi-index",
            "KeyConditionExpression": "gsi_pk = :v",
            "ExpressionAttributeValues": { ":v": { "S": "shared" } },
            "Limit": 1
        }),
    );
    let resp = svc.query(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Count"], 1);
    let lek = &body["LastEvaluatedKey"];
    assert!(lek.is_object(), "should have LastEvaluatedKey");
    // Must contain the index keys
    assert!(lek["gsi_pk"].is_object(), "LEK must contain gsi_pk");
    assert!(lek["gsi_sk"].is_object(), "LEK must contain gsi_sk");
    // Must also contain the table PK
    assert!(
        lek["pk"].is_object(),
        "LEK must contain table PK for GSI queries"
    );
}

#[test]
fn gsi_query_pagination_returns_all_items() {
    let svc = make_service();
    create_gsi_table(&svc);

    // Insert 4 items with the SAME GSI key but different table PKs
    for i in 0..4 {
        let req = make_request(
            "PutItem",
            json!({
                "TableName": "gsi-table",
                "Item": {
                    "pk": { "S": format!("item{i:03}") },
                    "gsi_pk": { "S": "shared" },
                    "gsi_sk": { "S": "sort" }
                }
            }),
        );
        svc.put_item(&req).unwrap();
    }

    // Paginate through all items with Limit=2
    let mut all_pks = Vec::new();
    let mut lek: Option<Value> = None;

    loop {
        let mut query = json!({
            "TableName": "gsi-table",
            "IndexName": "gsi-index",
            "KeyConditionExpression": "gsi_pk = :v",
            "ExpressionAttributeValues": { ":v": { "S": "shared" } },
            "Limit": 2
        });
        if let Some(ref start_key) = lek {
            query["ExclusiveStartKey"] = start_key.clone();
        }

        let req = make_request("Query", query);
        let resp = svc.query(&req).unwrap();
        let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();

        for item in body["Items"].as_array().unwrap() {
            let pk = item["pk"]["S"].as_str().unwrap().to_string();
            all_pks.push(pk);
        }

        if body["LastEvaluatedKey"].is_object() {
            lek = Some(body["LastEvaluatedKey"].clone());
        } else {
            break;
        }
    }

    all_pks.sort();
    assert_eq!(
        all_pks,
        vec!["item000", "item001", "item002", "item003"],
        "pagination should return all items without duplicates"
    );
}

fn cond_item(pairs: &[(&str, &str)]) -> HashMap<String, AttributeValue> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), json!({"S": v})))
        .collect()
}

fn cond_names(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn cond_values(pairs: &[(&str, &str)]) -> HashMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), json!({"S": v})))
        .collect()
}

#[test]
fn test_evaluate_condition_bare_not_equal() {
    let item = cond_item(&[("state", "active")]);
    let names = cond_names(&[("#s", "state")]);
    let values = cond_values(&[(":c", "complete")]);

    assert!(evaluate_condition("#s <> :c", Some(&item), &names, &values).is_ok());

    let item2 = cond_item(&[("state", "complete")]);
    assert!(evaluate_condition("#s <> :c", Some(&item2), &names, &values).is_err());
}

#[test]
fn test_evaluate_condition_parenthesized_not_equal() {
    let item = cond_item(&[("state", "active")]);
    let names = cond_names(&[("#s", "state")]);
    let values = cond_values(&[(":c", "complete")]);

    assert!(evaluate_condition("(#s <> :c)", Some(&item), &names, &values).is_ok());
}

#[test]
fn test_evaluate_condition_parenthesized_equal_mismatch() {
    let item = cond_item(&[("state", "active")]);
    let names = cond_names(&[("#s", "state")]);
    let values = cond_values(&[(":c", "complete")]);

    assert!(evaluate_condition("(#s = :c)", Some(&item), &names, &values).is_err());
}

#[test]
fn test_evaluate_condition_compound_and() {
    let item = cond_item(&[("state", "active")]);
    let names = cond_names(&[("#s", "state")]);
    let values = cond_values(&[(":c", "complete"), (":f", "failed")]);

    // active <> complete AND active <> failed => true
    assert!(evaluate_condition("(#s <> :c) AND (#s <> :f)", Some(&item), &names, &values).is_ok());
}

#[test]
fn test_evaluate_condition_compound_and_mismatch() {
    let item = cond_item(&[("state", "inactive")]);
    let names = cond_names(&[("#s", "state")]);
    let values = cond_values(&[(":a", "active"), (":b", "active")]);

    // inactive = active AND inactive = active => false
    assert!(evaluate_condition("(#s = :a) AND (#s = :b)", Some(&item), &names, &values).is_err());
}

#[test]
fn test_evaluate_condition_compound_or() {
    let item = cond_item(&[("state", "running")]);
    let names = cond_names(&[("#s", "state")]);
    let values = cond_values(&[(":a", "active"), (":b", "idle")]);

    // running = active OR running = idle => false
    assert!(evaluate_condition("(#s = :a) OR (#s = :b)", Some(&item), &names, &values).is_err());

    // running = active OR running = running => true
    let values2 = cond_values(&[(":a", "active"), (":b", "running")]);
    assert!(evaluate_condition("(#s = :a) OR (#s = :b)", Some(&item), &names, &values2).is_ok());
}

#[test]
fn test_evaluate_condition_not_operator() {
    let item = cond_item(&[("state", "active")]);
    let names = cond_names(&[("#s", "state")]);
    let values = cond_values(&[(":c", "complete")]);

    // NOT (active = complete) => NOT false => true
    assert!(evaluate_condition("NOT (#s = :c)", Some(&item), &names, &values).is_ok());

    // NOT (active <> complete) => NOT true => false
    assert!(evaluate_condition("NOT (#s <> :c)", Some(&item), &names, &values).is_err());

    // NOT attribute_exists(#s) on existing item => NOT true => false
    assert!(evaluate_condition("NOT attribute_exists(#s)", Some(&item), &names, &values).is_err());

    // NOT attribute_exists(#s) on missing item => NOT false => true
    assert!(evaluate_condition("NOT attribute_exists(#s)", None, &names, &values).is_ok());
}

#[test]
fn test_evaluate_condition_not_no_space() {
    // `NOT(` with no space between the keyword and `(` is what
    // python_dynamodb_lock and most hand-written expressions emit. It must be
    // tokenized identically to `NOT (...)`.
    let item = cond_item(&[("state", "active")]);
    let names = cond_names(&[("#s", "state"), ("#pk", "pk"), ("#sk", "sk")]);
    let values = cond_values(&[]);

    // NOT(attribute_exists(#s)) on a missing item => NOT false => true
    assert!(evaluate_condition("NOT(attribute_exists(#s))", None, &names, &values).is_ok());
    // ...on an existing item => NOT true => false
    assert!(evaluate_condition("NOT(attribute_exists(#s))", Some(&item), &names, &values).is_err());

    // The python_dynamodb_lock acquire form on a missing item => passes.
    assert!(evaluate_condition(
        "NOT(attribute_exists(#pk) AND attribute_exists(#sk))",
        None,
        &names,
        &values
    )
    .is_ok());

    // Regression guard: the spaced form still behaves as before.
    assert!(evaluate_condition("NOT (attribute_exists(#s))", None, &names, &values).is_ok());
    assert!(evaluate_condition("NOT attribute_exists(#s)", Some(&item), &names, &values).is_err());
}

#[test]
fn test_evaluate_condition_begins_with() {
    // After unification, conditions support begins_with via
    // evaluate_single_filter_condition (previously only filters had it).
    let item = cond_item(&[("name", "fakecloud-dynamodb")]);
    let names = cond_names(&[("#n", "name")]);
    let values = cond_values(&[(":p", "fakecloud")]);

    assert!(evaluate_condition("begins_with(#n, :p)", Some(&item), &names, &values).is_ok());

    let values2 = cond_values(&[(":p", "realcloud")]);
    assert!(evaluate_condition("begins_with(#n, :p)", Some(&item), &names, &values2).is_err());
}

#[test]
fn test_evaluate_condition_contains() {
    let item = cond_item(&[("tags", "alpha,beta,gamma")]);
    let names = cond_names(&[("#t", "tags")]);
    let values = cond_values(&[(":v", "beta")]);

    assert!(evaluate_condition("contains(#t, :v)", Some(&item), &names, &values).is_ok());

    let values2 = cond_values(&[(":v", "delta")]);
    assert!(evaluate_condition("contains(#t, :v)", Some(&item), &names, &values2).is_err());
}

#[test]
fn test_evaluate_condition_no_existing_item() {
    // When no item exists (PutItem with condition), attribute_not_exists
    // should succeed and attribute_exists should fail.
    let names = cond_names(&[("#s", "state")]);
    let values = cond_values(&[(":v", "active")]);

    assert!(evaluate_condition("attribute_not_exists(#s)", None, &names, &values).is_ok());
    assert!(evaluate_condition("attribute_exists(#s)", None, &names, &values).is_err());
    // A comparison against a missing attribute is false for EVERY operator in
    // DynamoDB (presence is tested with attribute_exists/not_exists), so both
    // `<>` and `=` fail the condition here (bug-audit 2026-06-26, 1.5).
    assert!(evaluate_condition("#s <> :v", None, &names, &values).is_err());
    assert!(evaluate_condition("#s = :v", None, &names, &values).is_err());
}

#[test]
fn test_evaluate_filter_not_operator() {
    let item = cond_item(&[("status", "pending")]);
    let names = cond_names(&[("#s", "status")]);
    let values = cond_values(&[(":v", "pending")]);

    assert!(!evaluate_filter_expression(
        "NOT (#s = :v)",
        &item,
        &names,
        &values
    ));
    assert!(evaluate_filter_expression(
        "NOT (#s <> :v)",
        &item,
        &names,
        &values
    ));
}

#[test]
fn test_evaluate_filter_expression_in_match() {
    // aws-sdk-go v2's expression.Name("state").In(Value("active"), Value("pending"))
    // emits "#0 IN (:0, :1)". Before fix: neither evaluate_single_filter_condition
    // nor evaluate_single_key_condition handled IN, so the filter leaf fell through
    // to the simple-comparison loop, hit no operators, and returned `true` — meaning
    // every item matched every IN filter regardless of value.
    let item = cond_item(&[("state", "active")]);
    let names = cond_names(&[("#s", "state")]);
    let values = cond_values(&[(":a", "active"), (":p", "pending")]);

    assert!(
        evaluate_filter_expression("#s IN (:a, :p)", &item, &names, &values),
        "state=active should match IN (active, pending)"
    );
}

#[test]
fn test_evaluate_filter_expression_in_no_match() {
    let item = cond_item(&[("state", "complete")]);
    let names = cond_names(&[("#s", "state")]);
    let values = cond_values(&[(":a", "active"), (":p", "pending")]);

    assert!(
        !evaluate_filter_expression("#s IN (:a, :p)", &item, &names, &values),
        "state=complete should not match IN (active, pending)"
    );
}

#[test]
fn test_evaluate_filter_expression_in_no_spaces() {
    // orderbot emits the raw form
    //     "#status IN (" + strings.Join(keys, ",") + ")"
    // which produces "IN (:v0,:v1,:v2)" — no spaces after commas. Must parse.
    let item = cond_item(&[("status", "shipped")]);
    let names = cond_names(&[("#s", "status")]);
    let values = cond_values(&[(":a", "pending"), (":b", "shipped"), (":c", "delivered")]);

    assert!(
        evaluate_filter_expression("#s IN (:a,:b,:c)", &item, &names, &values),
        "no-space IN list should still parse"
    );
}

#[test]
fn test_evaluate_filter_expression_in_missing_attribute() {
    // A missing attribute must not match any IN list — the silent-true
    // fallthrough would wrongly accept these items.
    let item: HashMap<String, AttributeValue> = HashMap::new();
    let names = cond_names(&[("#s", "state")]);
    let values = cond_values(&[(":a", "active")]);

    assert!(
        !evaluate_filter_expression("#s IN (:a)", &item, &names, &values),
        "missing attribute should not match any IN list"
    );
}

#[test]
fn test_evaluate_filter_expression_compound_in_and_eq() {
    // Shape emitted by `Name("state").In(...).And(Name("priority").Equal(...))`:
    //     "(#0 IN (:0, :1)) AND (#1 = :2)"
    // split_on_and handles the outer parens, but the IN leaf had the
    // silent-true fallthrough, so any item with priority=high would match
    // regardless of state.
    let item = cond_item(&[("state", "active"), ("priority", "high")]);
    let names = cond_names(&[("#s", "state"), ("#p", "priority")]);
    let values = cond_values(&[(":a", "active"), (":pe", "pending"), (":h", "high")]);

    assert!(
        evaluate_filter_expression("(#s IN (:a, :pe)) AND (#p = :h)", &item, &names, &values,),
        "(active IN (active, pending)) AND (high = high) should match"
    );

    let item2 = cond_item(&[("state", "complete"), ("priority", "high")]);
    assert!(
        !evaluate_filter_expression("(#s IN (:a, :pe)) AND (#p = :h)", &item2, &names, &values,),
        "(complete IN (active, pending)) AND (high = high) should not match"
    );
}

#[test]
fn test_evaluate_condition_attribute_exists_with_space() {
    // aws-sdk-go v2's expression.NewBuilder emits function calls with a
    // space between the name and the opening paren:
    //     "(attribute_exists (#0)) AND ((attribute_not_exists (#1)) OR (#1 = :0))"
    // Before fix: extract_function_arg used strip_prefix("attribute_exists(")
    // with no space, so these fell through the filter leaf entirely and
    // hit evaluate_single_key_condition's silent-true fallthrough —
    // every conditional write was silently accepted.
    let item = cond_item(&[("store_id", "s-1")]);
    let names = cond_names(&[("#0", "store_id"), ("#1", "active_viewer_tab_id")]);
    let values = cond_values(&[(":0", "tab-A")]);

    // On an existing item without active_viewer_tab_id: exists(store_id)
    // is true, not_exists(active_viewer_tab_id) is true → OK.
    assert!(
        evaluate_condition(
            "(attribute_exists (#0)) AND ((attribute_not_exists (#1)) OR (#1 = :0))",
            Some(&item),
            &names,
            &values,
        )
        .is_ok(),
        "claim-lease compound on free item should succeed"
    );

    // On a missing item: exists(store_id) is false → whole AND false → Err.
    assert!(
        evaluate_condition(
            "(attribute_exists (#0)) AND ((attribute_not_exists (#1)) OR (#1 = :0))",
            None,
            &names,
            &values,
        )
        .is_err(),
        "claim-lease compound on missing item must fail attribute_exists branch"
    );

    // On an item already held by tab-B: exists ✓, not_exists ✗, #1 = :0 ✗
    // → (✓) AND ((✗) OR (✗)) → false → Err.
    let held = cond_item(&[("store_id", "s-1"), ("active_viewer_tab_id", "tab-B")]);
    assert!(
        evaluate_condition(
            "(attribute_exists (#0)) AND ((attribute_not_exists (#1)) OR (#1 = :0))",
            Some(&held),
            &names,
            &values,
        )
        .is_err(),
        "claim-lease compound on item held by another tab must fail"
    );

    // Same tab re-claiming: exists ✓, not_exists ✗, #1 = :0 ✓
    // → (✓) AND ((✗) OR (✓)) → true → Ok.
    let self_held = cond_item(&[("store_id", "s-1"), ("active_viewer_tab_id", "tab-A")]);
    assert!(
        evaluate_condition(
            "(attribute_exists (#0)) AND ((attribute_not_exists (#1)) OR (#1 = :0))",
            Some(&self_held),
            &names,
            &values,
        )
        .is_ok(),
        "same-tab re-claim must succeed"
    );
}

#[test]
fn test_evaluate_condition_in_match() {
    // evaluate_condition delegates to evaluate_filter_expression, so this
    // also proves the ConditionExpression path. Before fix: silently Ok.
    let item = cond_item(&[("state", "active")]);
    let names = cond_names(&[("#s", "state")]);
    let values = cond_values(&[(":a", "active"), (":p", "pending")]);

    assert!(
        evaluate_condition("#s IN (:a, :p)", Some(&item), &names, &values).is_ok(),
        "IN should succeed when actual value is in the list"
    );
}

#[test]
fn test_evaluate_condition_in_no_match() {
    // Before fix: evaluate_condition silently returned Ok(()) for IN — any
    // conditional write was accepted regardless of actual state, the
    // opposite of what the caller asked for.
    let item = cond_item(&[("state", "complete")]);
    let names = cond_names(&[("#s", "state")]);
    let values = cond_values(&[(":a", "active"), (":p", "pending")]);

    assert!(
        evaluate_condition("#s IN (:a, :p)", Some(&item), &names, &values).is_err(),
        "IN should fail when actual value is not in the list"
    );
}

#[test]
fn test_apply_update_set_list_index_replaces_existing() {
    // Shape emitted by orderbot's order-item update retry loop:
    //     UpdateExpression: fmt.Sprintf("SET #items[%d] = :item", index)
    // Before fix: apply_set_assignment called resolve_attr_name on the
    // whole "#items[0]" token, which misses the name map, and then
    // item.insert("#items[0]", :item), producing a top-level key
    // literally named "#items[0]" rather than mutating the list.
    let mut item = HashMap::new();
    item.insert(
        "items".to_string(),
        json!({"L": [
            {"M": {"sku": {"S": "OLD-A"}}},
            {"M": {"sku": {"S": "OLD-B"}}},
        ]}),
    );

    let names = cond_names(&[("#items", "items")]);
    let mut values = HashMap::new();
    values.insert(":item".to_string(), json!({"M": {"sku": {"S": "NEW-A"}}}));

    apply_update_expression(&mut item, "SET #items[0] = :item", &names, &values).unwrap();

    let items_list = item
        .get("items")
        .and_then(|v| v.get("L"))
        .and_then(|v| v.as_array())
        .expect("items should still be a list");
    assert_eq!(items_list.len(), 2, "list length should be unchanged");
    let sku0 = items_list[0]
        .get("M")
        .and_then(|m| m.get("sku"))
        .and_then(|s| s.get("S"))
        .and_then(|s| s.as_str());
    assert_eq!(sku0, Some("NEW-A"), "index 0 should be replaced");
    let sku1 = items_list[1]
        .get("M")
        .and_then(|m| m.get("sku"))
        .and_then(|s| s.get("S"))
        .and_then(|s| s.as_str());
    assert_eq!(sku1, Some("OLD-B"), "index 1 should be untouched");

    assert!(!item.contains_key("items[0]"));
    assert!(!item.contains_key("#items[0]"));
}

#[test]
fn test_apply_update_set_list_index_second_slot() {
    let mut item = HashMap::new();
    item.insert(
        "items".to_string(),
        json!({"L": [
            {"M": {"sku": {"S": "A"}}},
            {"M": {"sku": {"S": "B"}}},
            {"M": {"sku": {"S": "C"}}},
        ]}),
    );

    let names = cond_names(&[("#items", "items")]);
    let mut values = HashMap::new();
    values.insert(":item".to_string(), json!({"M": {"sku": {"S": "B-PRIME"}}}));

    apply_update_expression(&mut item, "SET #items[1] = :item", &names, &values).unwrap();

    let items_list = item
        .get("items")
        .and_then(|v| v.get("L"))
        .and_then(|v| v.as_array())
        .unwrap();
    let skus: Vec<&str> = items_list
        .iter()
        .map(|v| {
            v.get("M")
                .and_then(|m| m.get("sku"))
                .and_then(|s| s.get("S"))
                .and_then(|s| s.as_str())
                .unwrap()
        })
        .collect();
    assert_eq!(skus, vec!["A", "B-PRIME", "C"]);
}

#[test]
fn test_apply_update_set_list_index_without_name_ref() {
    // Same fix must also work when the LHS is a literal attribute name,
    // not an expression attribute name ref.
    let mut item = HashMap::new();
    item.insert(
        "tags".to_string(),
        json!({"L": [{"S": "red"}, {"S": "blue"}]}),
    );

    let names: HashMap<String, String> = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":t".to_string(), json!({"S": "green"}));

    apply_update_expression(&mut item, "SET tags[1] = :t", &names, &values).unwrap();

    let tags = item
        .get("tags")
        .and_then(|v| v.get("L"))
        .and_then(|v| v.as_array())
        .unwrap();
    assert_eq!(tags[0].get("S").and_then(|s| s.as_str()), Some("red"));
    assert_eq!(tags[1].get("S").and_then(|s| s.as_str()), Some("green"));
}

#[test]
fn test_list_append_into_empty_list() {
    // Regression: UpdateItem with `SET #0 = list_append(#0, :0)` where
    // the attribute already exists as an empty list silently no-oped.
    // Root cause: parse_update_clauses split `list_append(#0, :0)` at
    // the inner comma, so apply_set_list_append received a truncated
    // `rest` with no closing ')' and returned early without writing.
    let mut item = HashMap::new();
    item.insert("files".to_string(), json!({"L": []}));

    let names = cond_names(&[("#0", "files")]);
    let mut values = HashMap::new();
    values.insert(
        ":0".to_string(),
        json!({"L": [{"M": {"field": {"S": "value"}}}]}),
    );

    apply_update_expression(&mut item, "SET #0 = list_append(#0, :0)", &names, &values).unwrap();

    let list = item
        .get("files")
        .and_then(|v| v.get("L"))
        .and_then(|v| v.as_array())
        .expect("files should be an L-typed attribute");
    assert_eq!(list.len(), 1, "one element should have been appended");
}

#[test]
fn test_list_append_into_nonempty_list() {
    // Verifies the same fix works when the existing list already has elements.
    let mut item = HashMap::new();
    item.insert(
        "files".to_string(),
        json!({"L": [{"M": {"field": {"S": "existing"}}}]}),
    );

    let names = cond_names(&[("#0", "files")]);
    let mut values = HashMap::new();
    values.insert(
        ":0".to_string(),
        json!({"L": [{"M": {"field": {"S": "new"}}}]}),
    );

    apply_update_expression(&mut item, "SET #0 = list_append(#0, :0)", &names, &values).unwrap();

    let list = item
        .get("files")
        .and_then(|v| v.get("L"))
        .and_then(|v| v.as_array())
        .expect("files should be an L-typed attribute");
    assert_eq!(list.len(), 2, "existing element plus one new element");
}

#[test]
fn test_list_append_combined_with_plain_set() {
    // Verifies that a mixed expression like
    // `SET #a = list_append(#a, :v), #b = :other` correctly applies
    // both assignments after the paren-aware comma split fix.
    let mut item = HashMap::new();
    item.insert("logs".to_string(), json!({"L": []}));
    item.insert("count".to_string(), json!({"N": "0"}));

    let names = cond_names(&[("#a", "logs"), ("#b", "count")]);
    let mut values = HashMap::new();
    values.insert(":v".to_string(), json!({"L": [{"S": "entry"}]}));
    values.insert(":other".to_string(), json!({"N": "1"}));

    apply_update_expression(
        &mut item,
        "SET #a = list_append(#a, :v), #b = :other",
        &names,
        &values,
    )
    .unwrap();

    let list = item
        .get("logs")
        .and_then(|v| v.get("L"))
        .and_then(|v| v.as_array())
        .expect("logs should be an L-typed attribute");
    assert_eq!(list.len(), 1, "one log entry appended");

    let count = item
        .get("count")
        .and_then(|v| v.get("N"))
        .and_then(|v| v.as_str())
        .expect("count should be an N-typed attribute");
    assert_eq!(count, "1", "count updated to 1");
}

#[test]
fn test_unrecognized_expression_returns_false() {
    // evaluate_single_key_condition must fail-closed: an expression shape
    // it doesn't recognize should return false (reject), not true (accept).
    let item = cond_item(&[("x", "1")]);
    let names: HashMap<String, String> = HashMap::new();
    let values: HashMap<String, Value> = HashMap::new();

    assert!(
        !evaluate_single_key_condition("GARBAGE NONSENSE", &item, &names, &values),
        "unrecognized expression must return false"
    );
}

#[test]
fn test_set_list_index_out_of_range_returns_error() {
    // SET list[N] where N > len must return a ValidationException,
    // not silently no-op.
    let mut item = HashMap::new();
    item.insert("items".to_string(), json!({"L": [{"S": "a"}, {"S": "b"}]}));

    let names: HashMap<String, String> = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":v".to_string(), json!({"S": "z"}));

    let result = apply_update_expression(&mut item, "SET items[5] = :v", &names, &values);
    assert!(
        result.is_err(),
        "out-of-range list index must return an error"
    );

    // List should be unchanged
    let list = item
        .get("items")
        .and_then(|v| v.get("L"))
        .and_then(|v| v.as_array())
        .unwrap();
    assert_eq!(list.len(), 2);
}

#[test]
fn test_set_list_index_on_non_list_returns_error() {
    // SET attr[0] = :v where attr is a string (not a list) must return
    // a ValidationException.
    let mut item = HashMap::new();
    item.insert("name".to_string(), json!({"S": "hello"}));

    let names: HashMap<String, String> = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":v".to_string(), json!({"S": "z"}));

    let result = apply_update_expression(&mut item, "SET name[0] = :v", &names, &values);
    assert!(
        result.is_err(),
        "list index on non-list attribute must return an error"
    );
}

#[test]
fn test_unrecognized_update_action_returns_error() {
    let mut item = HashMap::new();
    item.insert("name".to_string(), json!({"S": "hello"}));

    let names: HashMap<String, String> = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":bar".to_string(), json!({"S": "baz"}));

    let result = apply_update_expression(&mut item, "INVALID foo = :bar", &names, &values);
    assert!(
        result.is_err(),
        "unrecognized UpdateExpression action must return an error"
    );
    let err_msg = format!("{}", result.unwrap_err());
    assert!(
        err_msg.contains("Invalid UpdateExpression") || err_msg.contains("Syntax error"),
        "error should mention Invalid UpdateExpression, got: {err_msg}"
    );
}

// ── size() function tests ──────────────────────────────────────────

#[test]
fn test_size_string() {
    let mut item = HashMap::new();
    item.insert("name".to_string(), json!({"S": "hello"}));
    let names = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":limit".to_string(), json!({"N": "5"}));

    assert!(evaluate_single_filter_condition(
        "size(name) = :limit",
        &item,
        &names,
        &values,
    ));
    values.insert(":limit".to_string(), json!({"N": "4"}));
    assert!(evaluate_single_filter_condition(
        "size(name) > :limit",
        &item,
        &names,
        &values,
    ));
}

#[test]
fn test_size_list() {
    let mut item = HashMap::new();
    item.insert(
        "items".to_string(),
        json!({"L": [{"S": "a"}, {"S": "b"}, {"S": "c"}]}),
    );
    let names = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":limit".to_string(), json!({"N": "3"}));

    assert!(evaluate_single_filter_condition(
        "size(items) = :limit",
        &item,
        &names,
        &values,
    ));
}

#[test]
fn test_size_map() {
    let mut item = HashMap::new();
    item.insert(
        "data".to_string(),
        json!({"M": {"a": {"S": "1"}, "b": {"S": "2"}}}),
    );
    let names = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":limit".to_string(), json!({"N": "2"}));

    assert!(evaluate_single_filter_condition(
        "size(data) = :limit",
        &item,
        &names,
        &values,
    ));
}

#[test]
fn test_size_set() {
    let mut item = HashMap::new();
    item.insert("tags".to_string(), json!({"SS": ["a", "b", "c", "d"]}));
    let names = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":limit".to_string(), json!({"N": "3"}));

    assert!(evaluate_single_filter_condition(
        "size(tags) > :limit",
        &item,
        &names,
        &values,
    ));
}

// ── attribute_type() function tests ────────────────────────────────

#[test]
fn test_attribute_type_string() {
    let mut item = HashMap::new();
    item.insert("name".to_string(), json!({"S": "hello"}));
    let names = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":t".to_string(), json!({"S": "S"}));

    assert!(evaluate_single_filter_condition(
        "attribute_type(name, :t)",
        &item,
        &names,
        &values,
    ));

    values.insert(":t".to_string(), json!({"S": "N"}));
    assert!(!evaluate_single_filter_condition(
        "attribute_type(name, :t)",
        &item,
        &names,
        &values,
    ));
}

#[test]
fn test_attribute_type_number() {
    let mut item = HashMap::new();
    item.insert("age".to_string(), json!({"N": "42"}));
    let names = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":t".to_string(), json!({"S": "N"}));

    assert!(evaluate_single_filter_condition(
        "attribute_type(age, :t)",
        &item,
        &names,
        &values,
    ));
}

#[test]
fn test_attribute_type_list() {
    let mut item = HashMap::new();
    item.insert("items".to_string(), json!({"L": [{"S": "a"}]}));
    let names = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":t".to_string(), json!({"S": "L"}));

    assert!(evaluate_single_filter_condition(
        "attribute_type(items, :t)",
        &item,
        &names,
        &values,
    ));
}

#[test]
fn test_attribute_type_map() {
    let mut item = HashMap::new();
    item.insert("data".to_string(), json!({"M": {"key": {"S": "val"}}}));
    let names = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":t".to_string(), json!({"S": "M"}));

    assert!(evaluate_single_filter_condition(
        "attribute_type(data, :t)",
        &item,
        &names,
        &values,
    ));
}

#[test]
fn test_attribute_type_bool() {
    let mut item = HashMap::new();
    item.insert("active".to_string(), json!({"BOOL": true}));
    let names = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":t".to_string(), json!({"S": "BOOL"}));

    assert!(evaluate_single_filter_condition(
        "attribute_type(active, :t)",
        &item,
        &names,
        &values,
    ));
}

// ── begins_with rejects non-string types ───────────────────────────

#[test]
fn test_begins_with_rejects_number_type() {
    let mut item = HashMap::new();
    item.insert("code".to_string(), json!({"N": "12345"}));
    let names = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":prefix".to_string(), json!({"S": "123"}));

    assert!(
        !evaluate_single_filter_condition("begins_with(code, :prefix)", &item, &names, &values,),
        "begins_with must return false for N-type attributes"
    );
}

#[test]
fn test_begins_with_works_on_string_type() {
    let mut item = HashMap::new();
    item.insert("code".to_string(), json!({"S": "abc123"}));
    let names = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":prefix".to_string(), json!({"S": "abc"}));

    assert!(evaluate_single_filter_condition(
        "begins_with(code, :prefix)",
        &item,
        &names,
        &values,
    ));
}

// ── contains on sets ───────────────────────────────────────────────

#[test]
fn test_contains_string_set() {
    let mut item = HashMap::new();
    item.insert("tags".to_string(), json!({"SS": ["red", "blue", "green"]}));
    let names = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":val".to_string(), json!({"S": "blue"}));

    assert!(evaluate_single_filter_condition(
        "contains(tags, :val)",
        &item,
        &names,
        &values,
    ));

    values.insert(":val".to_string(), json!({"S": "yellow"}));
    assert!(!evaluate_single_filter_condition(
        "contains(tags, :val)",
        &item,
        &names,
        &values,
    ));
}

#[test]
fn test_contains_number_set() {
    let mut item = HashMap::new();
    item.insert("scores".to_string(), json!({"NS": ["1", "2", "3"]}));
    let names = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":val".to_string(), json!({"N": "2"}));

    assert!(evaluate_single_filter_condition(
        "contains(scores, :val)",
        &item,
        &names,
        &values,
    ));
}

// ── SET arithmetic type validation ─────────────────────────────────

#[test]
fn test_set_arithmetic_rejects_string_operand() {
    let mut item = HashMap::new();
    item.insert("name".to_string(), json!({"S": "hello"}));
    let names = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":val".to_string(), json!({"N": "1"}));

    let result = apply_update_expression(&mut item, "SET name = name + :val", &names, &values);
    assert!(
        result.is_err(),
        "arithmetic on S-type attribute must return a ValidationException"
    );
}

#[test]
fn test_set_arithmetic_rejects_string_value() {
    let mut item = HashMap::new();
    item.insert("count".to_string(), json!({"N": "5"}));
    let names = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":val".to_string(), json!({"S": "notanumber"}));

    let result = apply_update_expression(&mut item, "SET count = count + :val", &names, &values);
    assert!(
        result.is_err(),
        "arithmetic with S-type value must return a ValidationException"
    );
}

#[test]
fn test_set_arithmetic_valid_numbers() {
    let mut item = HashMap::new();
    item.insert("count".to_string(), json!({"N": "10"}));
    let names = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":val".to_string(), json!({"N": "3"}));

    let result = apply_update_expression(&mut item, "SET count = count + :val", &names, &values);
    assert!(result.is_ok());
    assert_eq!(item["count"], json!({"N": "13"}));
}

// ── Binary Set (BS) support in ADD/DELETE ──────────────────────────

#[test]
fn test_add_binary_set() {
    let mut item = HashMap::new();
    item.insert("data".to_string(), json!({"BS": ["YQ==", "Yg=="]}));
    let names = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":val".to_string(), json!({"BS": ["Yw==", "YQ=="]}));

    let result = apply_update_expression(&mut item, "ADD data :val", &names, &values);
    assert!(result.is_ok());
    let bs = item["data"]["BS"].as_array().unwrap();
    assert_eq!(bs.len(), 3, "should merge sets without duplicates");
    assert!(bs.contains(&json!("YQ==")));
    assert!(bs.contains(&json!("Yg==")));
    assert!(bs.contains(&json!("Yw==")));
}

#[test]
fn test_delete_binary_set() {
    let mut item = HashMap::new();
    item.insert("data".to_string(), json!({"BS": ["YQ==", "Yg==", "Yw=="]}));
    let names = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":val".to_string(), json!({"BS": ["Yg=="]}));

    let result = apply_update_expression(&mut item, "DELETE data :val", &names, &values);
    assert!(result.is_ok());
    let bs = item["data"]["BS"].as_array().unwrap();
    assert_eq!(bs.len(), 2);
    assert!(!bs.contains(&json!("Yg==")));
}

#[test]
fn test_delete_binary_set_removes_attr_when_empty() {
    let mut item = HashMap::new();
    item.insert("data".to_string(), json!({"BS": ["YQ=="]}));
    let names = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":val".to_string(), json!({"BS": ["YQ=="]}));

    let result = apply_update_expression(&mut item, "DELETE data :val", &names, &values);
    assert!(result.is_ok());
    assert!(
        !item.contains_key("data"),
        "attribute should be removed when set becomes empty"
    );
}

fn body_json(resp: &AwsResponse) -> Value {
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

fn expect_err(result: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
    match result {
        Err(e) => e,
        Ok(_) => panic!("expected error, got Ok"),
    }
}

// ── CreateTable ──

#[test]
fn create_table_basic() {
    let svc = make_service();
    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "my-table",
            "KeySchema": [{"AttributeName": "id", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "id", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST",
        }),
    );
    let resp = svc.create_table(&req).unwrap();
    let b = body_json(&resp);
    assert_eq!(b["TableDescription"]["TableName"], "my-table");
    assert_eq!(b["TableDescription"]["TableStatus"], "ACTIVE");
    assert!(b["TableDescription"]["TableArn"].as_str().is_some());
}

#[test]
fn create_table_with_sort_key_and_gsi() {
    let svc = make_service();
    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "gsi-table",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gsi_key", "AttributeType": "N"},
            ],
            "GlobalSecondaryIndexes": [{
                "IndexName": "gsi1",
                "KeySchema": [{"AttributeName": "gsi_key", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }],
            "BillingMode": "PAY_PER_REQUEST",
        }),
    );
    let resp = svc.create_table(&req).unwrap();
    let b = body_json(&resp);
    let gsi = b["TableDescription"]["GlobalSecondaryIndexes"]
        .as_array()
        .unwrap();
    assert_eq!(gsi.len(), 1);
    assert_eq!(gsi[0]["IndexName"], "gsi1");
}

#[test]
fn create_table_duplicate_fails() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "test-table",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST",
        }),
    );
    let err = expect_err(svc.create_table(&req));
    assert!(err.to_string().contains("ResourceInUseException"));
}

#[test]
fn create_table_missing_key_attr_in_definitions() {
    let svc = make_service();
    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "bad",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "other", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST",
        }),
    );
    let err = expect_err(svc.create_table(&req));
    // CreateTable doesn't declare ValidationException; we remap to
    // LimitExceededException to stay Smithy-compliant.
    assert!(err.to_string().contains("LimitExceededException"));
}

// ── DescribeTable ──

#[test]
fn describe_table_found() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request("DescribeTable", json!({"TableName": "test-table"}));
    let resp = svc.describe_table(&req).unwrap();
    let b = body_json(&resp);
    assert_eq!(b["Table"]["TableName"], "test-table");
    assert_eq!(b["Table"]["TableStatus"], "ACTIVE");
}

#[test]
fn describe_table_not_found() {
    let svc = make_service();
    let req = make_request("DescribeTable", json!({"TableName": "nope"}));
    let err = expect_err(svc.describe_table(&req));
    assert!(err.to_string().contains("ResourceNotFoundException"));
}

// ── DeleteTable ──

#[test]
fn delete_table_removes_table() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request("DeleteTable", json!({"TableName": "test-table"}));
    let resp = svc.delete_table(&req).unwrap();
    let b = body_json(&resp);
    assert_eq!(b["TableDescription"]["TableName"], "test-table");

    // Should be gone
    let req = make_request("DescribeTable", json!({"TableName": "test-table"}));
    assert!(svc.describe_table(&req).is_err());
}

// ── ListTables ──

#[test]
fn list_tables_returns_names() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request("ListTables", json!({}));
    let resp = svc.list_tables(&req).unwrap();
    let b = body_json(&resp);
    let names = b["TableNames"].as_array().unwrap();
    assert!(names.iter().any(|n| n == "test-table"));
}

// ── PutItem / GetItem / DeleteItem ──

#[test]
fn put_and_get_item() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": {
                "pk": {"S": "key1"},
                "name": {"S": "Alice"},
                "age": {"N": "30"},
            },
        }),
    );
    svc.put_item(&req).unwrap();

    let req = make_request(
        "GetItem",
        json!({
            "TableName": "test-table",
            "Key": {"pk": {"S": "key1"}},
        }),
    );
    let resp = svc.get_item(&req).unwrap();
    let b = body_json(&resp);
    assert_eq!(b["Item"]["name"]["S"], "Alice");
    assert_eq!(b["Item"]["age"]["N"], "30");
}

#[test]
fn get_item_not_found() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "GetItem",
        json!({
            "TableName": "test-table",
            "Key": {"pk": {"S": "nonexistent"}},
        }),
    );
    let resp = svc.get_item(&req).unwrap();
    let b = body_json(&resp);
    assert!(b.get("Item").is_none() || b["Item"].is_null());
}

#[test]
fn delete_item_removes_item() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": {"pk": {"S": "del-me"}},
        }),
    );
    svc.put_item(&req).unwrap();

    let req = make_request(
        "DeleteItem",
        json!({
            "TableName": "test-table",
            "Key": {"pk": {"S": "del-me"}},
        }),
    );
    svc.delete_item(&req).unwrap();

    let req = make_request(
        "GetItem",
        json!({
            "TableName": "test-table",
            "Key": {"pk": {"S": "del-me"}},
        }),
    );
    let resp = svc.get_item(&req).unwrap();
    let b = body_json(&resp);
    assert!(b.get("Item").is_none() || b["Item"].is_null());
}

#[test]
fn put_item_returns_old_item() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": {"pk": {"S": "overwrite"}, "v": {"N": "1"}},
        }),
    );
    svc.put_item(&req).unwrap();

    let req = make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": {"pk": {"S": "overwrite"}, "v": {"N": "2"}},
            "ReturnValues": "ALL_OLD",
        }),
    );
    let resp = svc.put_item(&req).unwrap();
    let b = body_json(&resp);
    assert_eq!(b["Attributes"]["v"]["N"], "1");
}

#[test]
fn put_item_emits_consumed_capacity_when_requested() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": {"pk": {"S": "cc"}, "v": {"N": "1"}},
            "ReturnConsumedCapacity": "TOTAL",
        }),
    );
    let resp = svc.put_item(&req).unwrap();
    let b = body_json(&resp);
    assert_eq!(b["ConsumedCapacity"]["TableName"], "test-table");
    assert_eq!(b["ConsumedCapacity"]["CapacityUnits"], 1.0);
    assert_eq!(b["ConsumedCapacity"]["WriteCapacityUnits"], 1.0);
    // TOTAL must not include the breakdown.
    assert!(b["ConsumedCapacity"].get("Table").is_none());
}

#[test]
fn put_item_consumed_capacity_indexes_includes_breakdown() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": {"pk": {"S": "cc"}, "v": {"N": "1"}},
            "ReturnConsumedCapacity": "INDEXES",
        }),
    );
    let resp = svc.put_item(&req).unwrap();
    let b = body_json(&resp);
    assert_eq!(b["ConsumedCapacity"]["Table"]["CapacityUnits"], 1.0);
    assert!(b["ConsumedCapacity"]["GlobalSecondaryIndexes"].is_object());
    assert!(b["ConsumedCapacity"]["LocalSecondaryIndexes"].is_object());
}

#[test]
fn put_item_consumed_capacity_omitted_by_default() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": {"pk": {"S": "cc"}, "v": {"N": "1"}},
        }),
    );
    let resp = svc.put_item(&req).unwrap();
    let b = body_json(&resp);
    assert!(b.get("ConsumedCapacity").is_none());
}

#[test]
fn get_item_emits_consumed_capacity_when_requested() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": {"pk": {"S": "g"}, "v": {"N": "1"}},
        }),
    );
    svc.put_item(&req).unwrap();

    let req = make_request(
        "GetItem",
        json!({
            "TableName": "test-table",
            "Key": {"pk": {"S": "g"}},
            "ReturnConsumedCapacity": "TOTAL",
        }),
    );
    let resp = svc.get_item(&req).unwrap();
    let b = body_json(&resp);
    assert_eq!(b["ConsumedCapacity"]["TableName"], "test-table");
    assert_eq!(b["ConsumedCapacity"]["ReadCapacityUnits"], 0.5);
}

#[test]
fn query_emits_consumed_capacity_when_requested() {
    let svc = make_service();
    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "qcc",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }),
    );
    svc.create_table(&req).unwrap();
    let put = make_request(
        "PutItem",
        json!({
            "TableName": "qcc",
            "Item": {"pk": {"S": "p"}, "sk": {"S": "s"}},
        }),
    );
    svc.put_item(&put).unwrap();

    let req = make_request(
        "Query",
        json!({
            "TableName": "qcc",
            "KeyConditionExpression": "pk = :p",
            "ExpressionAttributeValues": {":p": {"S": "p"}},
            "ReturnConsumedCapacity": "TOTAL",
        }),
    );
    let resp = svc.query(&req).unwrap();
    let b = body_json(&resp);
    assert_eq!(b["ConsumedCapacity"]["TableName"], "qcc");
    assert!(
        b["ConsumedCapacity"]["CapacityUnits"]
            .as_f64()
            .unwrap_or(0.0)
            >= 0.5
    );
}

#[test]
fn batch_get_item_emits_consumed_capacity() {
    let svc = make_service();
    create_test_table(&svc);
    let put = make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": {"pk": {"S": "bg"}},
        }),
    );
    svc.put_item(&put).unwrap();

    let req = make_request(
        "BatchGetItem",
        json!({
            "RequestItems": {
                "test-table": {"Keys": [{"pk": {"S": "bg"}}]},
            },
            "ReturnConsumedCapacity": "TOTAL",
        }),
    );
    let resp = svc.batch_get_item(&req).unwrap();
    let b = body_json(&resp);
    assert!(b["ConsumedCapacity"].is_array());
    assert_eq!(b["ConsumedCapacity"][0]["TableName"], "test-table");
}

#[test]
fn transact_write_items_emits_consumed_capacity() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "TransactWriteItems",
        json!({
            "TransactItems": [
                {"Put": {"TableName": "test-table", "Item": {"pk": {"S": "tw"}}}},
            ],
            "ReturnConsumedCapacity": "TOTAL",
        }),
    );
    let resp = svc.transact_write_items(&req).unwrap();
    let b = body_json(&resp);
    assert!(b["ConsumedCapacity"].is_array());
    assert_eq!(b["ConsumedCapacity"][0]["TableName"], "test-table");
}

// ── UpdateItem ──

#[test]
fn update_item_set_attribute() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": {"pk": {"S": "upd"}, "count": {"N": "0"}},
        }),
    );
    svc.put_item(&req).unwrap();

    let req = make_request(
        "UpdateItem",
        json!({
            "TableName": "test-table",
            "Key": {"pk": {"S": "upd"}},
            "UpdateExpression": "SET #c = :val",
            "ExpressionAttributeNames": {"#c": "count"},
            "ExpressionAttributeValues": {":val": {"N": "42"}},
            "ReturnValues": "ALL_NEW",
        }),
    );
    let resp = svc.update_item(&req).unwrap();
    let b = body_json(&resp);
    assert_eq!(b["Attributes"]["count"]["N"], "42");
}

// ── Query ──

#[test]
fn query_returns_matching_items() {
    let svc = make_service();
    // Table with hash+range
    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "query-table",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }),
    );
    svc.create_table(&req).unwrap();

    for i in 0..3 {
        let req = make_request(
            "PutItem",
            json!({
                "TableName": "query-table",
                "Item": {
                    "pk": {"S": "user1"},
                    "sk": {"S": format!("item-{i}")},
                },
            }),
        );
        svc.put_item(&req).unwrap();
    }
    // Different partition
    let req = make_request(
        "PutItem",
        json!({
            "TableName": "query-table",
            "Item": {"pk": {"S": "user2"}, "sk": {"S": "item-0"}},
        }),
    );
    svc.put_item(&req).unwrap();

    let req = make_request(
        "Query",
        json!({
            "TableName": "query-table",
            "KeyConditionExpression": "pk = :pk",
            "ExpressionAttributeValues": {":pk": {"S": "user1"}},
        }),
    );
    let resp = svc.query(&req).unwrap();
    let b = body_json(&resp);
    assert_eq!(b["Count"], 3);
    assert_eq!(b["Items"].as_array().unwrap().len(), 3);
}

// ── Scan ──

#[test]
fn scan_returns_all_items() {
    let svc = make_service();
    create_test_table(&svc);

    for i in 0..5 {
        let req = make_request(
            "PutItem",
            json!({
                "TableName": "test-table",
                "Item": {"pk": {"S": format!("scan-{i}")}},
            }),
        );
        svc.put_item(&req).unwrap();
    }

    let req = make_request("Scan", json!({"TableName": "test-table"}));
    let resp = svc.scan(&req).unwrap();
    let b = body_json(&resp);
    assert_eq!(b["Count"], 5);
}

// ── BatchWriteItem / BatchGetItem ──

#[test]
fn batch_write_and_get_items() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "BatchWriteItem",
        json!({
            "RequestItems": {
                "test-table": [
                    {"PutRequest": {"Item": {"pk": {"S": "b1"}, "val": {"S": "v1"}}}},
                    {"PutRequest": {"Item": {"pk": {"S": "b2"}, "val": {"S": "v2"}}}},
                    {"PutRequest": {"Item": {"pk": {"S": "b3"}, "val": {"S": "v3"}}}},
                ]
            }
        }),
    );
    let resp = svc.batch_write_item(&req).unwrap();
    let b = body_json(&resp);
    // Unprocessed should be empty
    assert!(
        b["UnprocessedItems"].as_object().unwrap().is_empty()
            || b["UnprocessedItems"]["test-table"]
                .as_array()
                .is_none_or(|a| a.is_empty())
    );

    // BatchGetItem
    let req = make_request(
        "BatchGetItem",
        json!({
            "RequestItems": {
                "test-table": {
                    "Keys": [
                        {"pk": {"S": "b1"}},
                        {"pk": {"S": "b2"}},
                        {"pk": {"S": "b3"}},
                    ]
                }
            }
        }),
    );
    let resp = svc.batch_get_item(&req).unwrap();
    let b = body_json(&resp);
    let items = b["Responses"]["test-table"].as_array().unwrap();
    assert_eq!(items.len(), 3);
}

// ── TransactWriteItems / TransactGetItems ──

#[test]
fn transact_write_and_get() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "TransactWriteItems",
        json!({
            "TransactItems": [
                {"Put": {"TableName": "test-table", "Item": {"pk": {"S": "tx1"}}}},
                {"Put": {"TableName": "test-table", "Item": {"pk": {"S": "tx2"}}}},
            ]
        }),
    );
    svc.transact_write_items(&req).unwrap();

    let req = make_request(
        "TransactGetItems",
        json!({
            "TransactItems": [
                {"Get": {"TableName": "test-table", "Key": {"pk": {"S": "tx1"}}}},
                {"Get": {"TableName": "test-table", "Key": {"pk": {"S": "tx2"}}}},
            ]
        }),
    );
    let resp = svc.transact_get_items(&req).unwrap();
    let b = body_json(&resp);
    let responses = b["Responses"].as_array().unwrap();
    assert_eq!(responses.len(), 2);
}

// ── TagResource / UntagResource / ListTagsOfResource ──

#[test]
fn tag_operations() {
    let svc = make_service();
    create_test_table(&svc);
    let arn = {
        let s = svc.state.read();
        s.default_ref()
            .tables
            .get("test-table")
            .unwrap()
            .arn
            .clone()
    };

    let req = make_request(
        "TagResource",
        json!({
            "ResourceArn": arn,
            "Tags": [{"Key": "env", "Value": "test"}],
        }),
    );
    svc.tag_resource(&req).unwrap();

    let req = make_request("ListTagsOfResource", json!({"ResourceArn": arn}));
    let resp = svc.list_tags_of_resource(&req).unwrap();
    let b = body_json(&resp);
    let tags = b["Tags"].as_array().unwrap();
    assert_eq!(tags.len(), 1);
    assert_eq!(tags[0]["Key"], "env");

    let req = make_request(
        "UntagResource",
        json!({
            "ResourceArn": arn,
            "TagKeys": ["env"],
        }),
    );
    svc.untag_resource(&req).unwrap();

    let req = make_request("ListTagsOfResource", json!({"ResourceArn": arn}));
    let resp = svc.list_tags_of_resource(&req).unwrap();
    let b = body_json(&resp);
    assert!(b["Tags"].as_array().unwrap().is_empty());
}

// ── UpdateTable ──

#[test]
fn update_table_add_gsi() {
    let svc = make_service();
    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "upd-table",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gk", "AttributeType": "S"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }),
    );
    svc.create_table(&req).unwrap();

    let req = make_request(
        "UpdateTable",
        json!({
            "TableName": "upd-table",
            "GlobalSecondaryIndexUpdates": [{
                "Create": {
                    "IndexName": "new-gsi",
                    "KeySchema": [{"AttributeName": "gk", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "ALL"},
                }
            }],
        }),
    );
    let resp = svc.update_table(&req).unwrap();
    let b = body_json(&resp);
    let gsi = b["TableDescription"]["GlobalSecondaryIndexes"]
        .as_array()
        .unwrap();
    assert_eq!(gsi.len(), 1);
    assert_eq!(gsi[0]["IndexName"], "new-gsi");
}

// ── Scan with FilterExpression ──

#[test]
fn scan_with_filter_expression() {
    let svc = make_service();
    create_test_table(&svc);

    for i in 0..5 {
        let req = make_request(
            "PutItem",
            json!({
                "TableName": "test-table",
                "Item": {
                    "pk": {"S": format!("f-{i}")},
                    "status": {"S": if i % 2 == 0 { "active" } else { "inactive" }},
                },
            }),
        );
        svc.put_item(&req).unwrap();
    }

    let req = make_request(
        "Scan",
        json!({
            "TableName": "test-table",
            "FilterExpression": "#s = :val",
            "ExpressionAttributeNames": {"#s": "status"},
            "ExpressionAttributeValues": {":val": {"S": "active"}},
        }),
    );
    let resp = svc.scan(&req).unwrap();
    let b = body_json(&resp);
    assert_eq!(b["Count"], 3);
}

// ── PartiQL operations (batch.rs coverage) ──

#[test]
fn execute_statement_select() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "PutItem",
        json!({"TableName": "test-table", "Item": {"pk": {"S": "qs1"}, "val": {"S": "hello"}}}),
    );
    svc.put_item(&req).unwrap();

    let req = make_request(
        "ExecuteStatement",
        json!({"Statement": "SELECT * FROM \"test-table\" WHERE pk='qs1'"}),
    );
    let resp = svc.execute_statement(&req).unwrap();
    let b = body_json(&resp);
    assert!(!b["Items"].as_array().unwrap().is_empty());
}

#[test]
fn execute_statement_insert() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "ExecuteStatement",
        json!({"Statement": "INSERT INTO \"test-table\" VALUE {'pk': 'ins1', 'data': 'val'}"}),
    );
    svc.execute_statement(&req).unwrap();

    let req = make_request(
        "GetItem",
        json!({"TableName": "test-table", "Key": {"pk": {"S": "ins1"}}}),
    );
    let resp = svc.get_item(&req).unwrap();
    let b = body_json(&resp);
    assert_eq!(b["Item"]["data"]["S"], "val");
}

#[test]
fn batch_execute_statement() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "PutItem",
        json!({"TableName": "test-table", "Item": {"pk": {"S": "be1"}}}),
    );
    svc.put_item(&req).unwrap();

    let req = make_request(
        "BatchExecuteStatement",
        json!({
            "Statements": [
                {"Statement": "SELECT * FROM \"test-table\" WHERE pk='be1'"},
            ]
        }),
    );
    let resp = svc.batch_execute_statement(&req).unwrap();
    let b = body_json(&resp);
    assert!(b["Responses"].as_array().is_some());
}

#[test]
fn execute_transaction() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "ExecuteTransaction",
        json!({
            "TransactStatements": [
                {"Statement": "INSERT INTO \"test-table\" VALUE {'pk': 'tx1'}"},
                {"Statement": "INSERT INTO \"test-table\" VALUE {'pk': 'tx2'}"},
            ]
        }),
    );
    svc.execute_transaction(&req).unwrap();

    let req = make_request(
        "GetItem",
        json!({"TableName": "test-table", "Key": {"pk": {"S": "tx1"}}}),
    );
    let resp = svc.get_item(&req).unwrap();
    let b = body_json(&resp);
    assert!(b["Item"].is_object());
}

// L4: PartiQL comparator + schema validation coverage. Each test
// seeds a small set of items and exercises one of the WHERE forms
// added in L4 (numeric/lexicographic comparators, BETWEEN, IN,
// LIKE, contains/begins_with/attribute_*) so future regressions
// surface even before an SDK roundtrip.

fn seed_partiql_corpus(svc: &DynamoDbService) {
    create_test_table(svc);
    for (pk, score, name) in [
        ("a", 10, "alpha"),
        ("b", 20, "beta"),
        ("c", 30, "gamma"),
        ("d", 40, "delta"),
    ] {
        let req = make_request(
            "PutItem",
            json!({
                "TableName": "test-table",
                "Item": {
                    "pk": {"S": pk},
                    "score": {"N": score.to_string()},
                    "name": {"S": name},
                },
            }),
        );
        svc.put_item(&req).unwrap();
    }
}

fn pks_from_select(svc: &DynamoDbService, statement: &str) -> Vec<String> {
    let req = make_request("ExecuteStatement", json!({ "Statement": statement }));
    let resp = svc.execute_statement(&req).unwrap();
    let b = body_json(&resp);
    let mut pks: Vec<String> = b["Items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|it| it["pk"]["S"].as_str().unwrap().to_string())
        .collect();
    pks.sort();
    pks
}

#[test]
fn partiql_select_lt_gt_le_ge_ne_numeric() {
    let svc = make_service();
    seed_partiql_corpus(&svc);

    assert_eq!(
        pks_from_select(&svc, "SELECT * FROM \"test-table\" WHERE score < 25"),
        vec!["a", "b"]
    );
    assert_eq!(
        pks_from_select(&svc, "SELECT * FROM \"test-table\" WHERE score > 25"),
        vec!["c", "d"]
    );
    assert_eq!(
        pks_from_select(&svc, "SELECT * FROM \"test-table\" WHERE score <= 20"),
        vec!["a", "b"]
    );
    assert_eq!(
        pks_from_select(&svc, "SELECT * FROM \"test-table\" WHERE score >= 30"),
        vec!["c", "d"]
    );
    assert_eq!(
        pks_from_select(&svc, "SELECT * FROM \"test-table\" WHERE score <> 20"),
        vec!["a", "c", "d"]
    );
}

#[test]
fn partiql_select_between_in_like() {
    let svc = make_service();
    seed_partiql_corpus(&svc);

    assert_eq!(
        pks_from_select(
            &svc,
            "SELECT * FROM \"test-table\" WHERE score BETWEEN 15 AND 35"
        ),
        vec!["b", "c"]
    );
    assert_eq!(
        pks_from_select(&svc, "SELECT * FROM \"test-table\" WHERE pk IN ('a','c')"),
        vec!["a", "c"]
    );
    assert_eq!(
        pks_from_select(&svc, "SELECT * FROM \"test-table\" WHERE name LIKE 'al%'"),
        vec!["a"]
    );
    assert_eq!(
        pks_from_select(&svc, "SELECT * FROM \"test-table\" WHERE name LIKE '_eta'"),
        vec!["b"]
    );
}

#[test]
fn partiql_select_function_predicates() {
    let svc = make_service();
    seed_partiql_corpus(&svc);

    assert_eq!(
        pks_from_select(
            &svc,
            "SELECT * FROM \"test-table\" WHERE begins_with(name, 'g')"
        ),
        vec!["c"]
    );
    assert_eq!(
        pks_from_select(
            &svc,
            "SELECT * FROM \"test-table\" WHERE contains(name, 'lt')"
        ),
        vec!["d"]
    );
    assert_eq!(
        pks_from_select(
            &svc,
            "SELECT * FROM \"test-table\" WHERE attribute_exists(score)"
        ),
        vec!["a", "b", "c", "d"]
    );
    assert_eq!(
        pks_from_select(
            &svc,
            "SELECT * FROM \"test-table\" WHERE attribute_not_exists(missing)"
        ),
        vec!["a", "b", "c", "d"]
    );
}

#[test]
fn partiql_insert_rejects_missing_partition_key() {
    let svc = make_service();
    create_test_table(&svc);
    let req = make_request(
        "ExecuteStatement",
        json!({
            "Statement": "INSERT INTO \"test-table\" VALUE {'data': 'no-pk'}"
        }),
    );
    let err = match svc.execute_statement(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected INSERT without pk to fail"),
    };
    let dbg = format!("{err:?}");
    // A key/validation error on an existing table is a ValidationException,
    // not a ResourceNotFoundException (only a missing table maps to NotFound).
    assert!(dbg.contains("ValidationException"), "got {dbg}");
    assert!(dbg.contains("Missing the key pk"), "got {dbg}");
}

#[test]
fn partiql_insert_rejects_wrong_key_type() {
    let svc = make_service();
    create_test_table(&svc);
    // Table key `pk` is declared as type `S` — try to insert a numeric.
    let req = make_request(
        "ExecuteStatement",
        json!({
            "Statement": "INSERT INTO \"test-table\" VALUE {'pk': 42}"
        }),
    );
    let err = match svc.execute_statement(&req) {
        Err(e) => e,
        Ok(_) => panic!("expected INSERT with wrong-type pk to fail"),
    };
    let dbg = format!("{err:?}");
    assert!(dbg.contains("ValidationException"), "got {dbg}");
    assert!(dbg.contains("Type mismatch for key pk"), "got {dbg}");
}

#[test]
fn partiql_select_and_or_not_parens() {
    // L4: WHERE composition with AND/OR/NOT and parens. The AND-only
    // legacy splitter cannot express these, so this exercises the
    // recursive expression parser.
    let svc = make_service();
    seed_partiql_corpus(&svc);

    // OR — match the lower and upper edges.
    assert_eq!(
        pks_from_select(
            &svc,
            "SELECT * FROM \"test-table\" WHERE score < 15 OR score > 35"
        ),
        vec!["a", "d"]
    );
    // NOT inverts a comparator predicate.
    assert_eq!(
        pks_from_select(&svc, "SELECT * FROM \"test-table\" WHERE NOT score >= 30"),
        vec!["a", "b"]
    );
    // Parens force OR to bind tighter than AND.
    assert_eq!(
        pks_from_select(
            &svc,
            "SELECT * FROM \"test-table\" WHERE (score < 15 OR score > 35) AND name <> 'unused'",
        ),
        vec!["a", "d"]
    );
    // Mixed AND/OR without parens — AND binds tighter.
    assert_eq!(
        pks_from_select(
            &svc,
            "SELECT * FROM \"test-table\" WHERE score = 10 OR score = 20 AND name = 'beta'",
        ),
        vec!["a", "b"]
    );
}

#[test]
fn partiql_select_filter_with_like_and_gt() {
    // L4 spec: `SELECT * FROM "T" WHERE n > 5 AND s LIKE 'foo%'`
    // returns the right subset.
    let svc = make_service();
    create_test_table(&svc);
    for (pk, n, s) in [
        ("k1", 1, "foobar"),
        ("k2", 6, "foobar"),
        ("k3", 6, "barfoo"),
        ("k4", 9, "fooz"),
    ] {
        svc.put_item(&make_request(
            "PutItem",
            json!({
                "TableName": "test-table",
                "Item": {
                    "pk": {"S": pk},
                    "n": {"N": n.to_string()},
                    "s": {"S": s},
                },
            }),
        ))
        .unwrap();
    }
    assert_eq!(
        pks_from_select(
            &svc,
            "SELECT * FROM \"test-table\" WHERE n > 5 AND s LIKE 'foo%'"
        ),
        vec!["k2", "k4"]
    );
}

#[test]
fn partiql_update_emits_stream_record() {
    // L4: UPDATE statements on a stream-enabled table must emit a
    // MODIFY stream record mirroring the UpdateItem path.
    let svc = make_service();
    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "T",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            }
        }),
    );
    svc.create_table(&req).unwrap();
    svc.put_item(&make_request(
        "PutItem",
        json!({"TableName": "T", "Item": {"pk": {"S": "u1"}, "v": {"N": "1"}}}),
    ))
    .unwrap();

    let baseline = {
        let s = svc.state.read();
        let n = s
            .get("123456789012")
            .unwrap()
            .tables
            .get("T")
            .unwrap()
            .stream_records
            .read()
            .len();
        n
    };

    svc.execute_statement(&make_request(
        "ExecuteStatement",
        json!({"Statement": "UPDATE \"T\" SET v = 99 WHERE pk = 'u1'"}),
    ))
    .unwrap();

    let after = {
        let s = svc.state.read();
        let t = s.get("123456789012").unwrap().tables.get("T").unwrap();
        let recs = t.stream_records.read();
        let last = recs.last().cloned();
        (recs.len(), last)
    };
    assert_eq!(after.0, baseline + 1);
    assert_eq!(after.1.unwrap().event_name, "MODIFY");
}

#[test]
fn partiql_delete_emits_stream_record() {
    // L4: DELETE statements must emit a REMOVE stream record.
    let svc = make_service();
    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "T",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST",
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES"
            }
        }),
    );
    svc.create_table(&req).unwrap();
    svc.put_item(&make_request(
        "PutItem",
        json!({"TableName": "T", "Item": {"pk": {"S": "d1"}}}),
    ))
    .unwrap();

    let baseline = {
        let s = svc.state.read();
        let n = s
            .get("123456789012")
            .unwrap()
            .tables
            .get("T")
            .unwrap()
            .stream_records
            .read()
            .len();
        n
    };

    svc.execute_statement(&make_request(
        "ExecuteStatement",
        json!({"Statement": "DELETE FROM \"T\" WHERE pk = 'd1'"}),
    ))
    .unwrap();

    let after = {
        let s = svc.state.read();
        let t = s.get("123456789012").unwrap().tables.get("T").unwrap();
        let recs = t.stream_records.read();
        let last = recs.last().cloned();
        (recs.len(), last)
    };
    assert_eq!(after.0, baseline + 1);
    assert_eq!(after.1.unwrap().event_name, "REMOVE");
}

#[test]
fn partiql_insert_rejects_missing_sort_key() {
    // L4: composite-key tables must reject INSERTs that omit the sort
    // key, matching the partition-key-only check.
    let svc = make_service();
    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "T",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    );
    svc.create_table(&req).unwrap();
    let err = svc
        .execute_statement(&make_request(
            "ExecuteStatement",
            json!({"Statement": "INSERT INTO \"T\" VALUE {'pk': 'a'}"}),
        ))
        .err()
        .expect("missing sort key");
    let dbg = format!("{err:?}");
    assert!(dbg.contains("ValidationException"), "got {dbg}");
    assert!(dbg.contains("Missing the key sk"), "got {dbg}");
}

// ── Batch write with delete ──

#[test]
fn batch_write_with_delete_requests() {
    let svc = make_service();
    create_test_table(&svc);

    // Put items first
    for key in &["bwd1", "bwd2", "bwd3"] {
        let req = make_request(
            "PutItem",
            json!({"TableName": "test-table", "Item": {"pk": {"S": key}}}),
        );
        svc.put_item(&req).unwrap();
    }

    // Batch delete two
    let req = make_request(
        "BatchWriteItem",
        json!({
            "RequestItems": {
                "test-table": [
                    {"DeleteRequest": {"Key": {"pk": {"S": "bwd1"}}}},
                    {"DeleteRequest": {"Key": {"pk": {"S": "bwd2"}}}},
                ]
            }
        }),
    );
    svc.batch_write_item(&req).unwrap();

    // bwd3 should still exist
    let req = make_request(
        "GetItem",
        json!({"TableName": "test-table", "Key": {"pk": {"S": "bwd3"}}}),
    );
    let resp = svc.get_item(&req).unwrap();
    let b = body_json(&resp);
    assert!(b["Item"].is_object());

    // bwd1 should be gone
    let req = make_request(
        "GetItem",
        json!({"TableName": "test-table", "Key": {"pk": {"S": "bwd1"}}}),
    );
    let resp = svc.get_item(&req).unwrap();
    let b = body_json(&resp);
    assert!(b.get("Item").is_none() || b["Item"].is_null());
}

// ── Query with sort key condition ──

#[test]
fn query_with_sort_key_begins_with() {
    let svc = make_service();
    // Table with hash+range
    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "sk-table",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"},
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }),
    );
    svc.create_table(&req).unwrap();

    for sk in &["order#001", "order#002", "profile#main"] {
        let req = make_request(
            "PutItem",
            json!({"TableName": "sk-table", "Item": {"pk": {"S": "u1"}, "sk": {"S": sk}}}),
        );
        svc.put_item(&req).unwrap();
    }

    let req = make_request(
        "Query",
        json!({
            "TableName": "sk-table",
            "KeyConditionExpression": "pk = :pk AND begins_with(sk, :prefix)",
            "ExpressionAttributeValues": {":pk": {"S": "u1"}, ":prefix": {"S": "order#"}},
        }),
    );
    let resp = svc.query(&req).unwrap();
    let b = body_json(&resp);
    assert_eq!(b["Count"], 2);
}

// ── Scan with limit ──

#[test]
fn scan_with_limit() {
    let svc = make_service();
    create_test_table(&svc);

    for i in 0..10 {
        let req = make_request(
            "PutItem",
            json!({"TableName": "test-table", "Item": {"pk": {"S": format!("lim{i}")}}}),
        );
        svc.put_item(&req).unwrap();
    }

    let req = make_request("Scan", json!({"TableName": "test-table", "Limit": 3}));
    let resp = svc.scan(&req).unwrap();
    let b = body_json(&resp);
    assert_eq!(b["Count"], 3);
    assert!(b["LastEvaluatedKey"].is_object());
}

// ── Error branches ──

#[test]
fn batch_get_item_table_not_found() {
    let svc = make_service();
    let req = make_request(
        "BatchGetItem",
        json!({"RequestItems": {"ghost": {"Keys": [{"pk": {"S": "k"}}]}}}),
    );
    assert!(svc.batch_get_item(&req).is_err());
}

#[test]
fn batch_write_item_table_not_found() {
    let svc = make_service();
    let req = make_request(
        "BatchWriteItem",
        json!({"RequestItems": {"ghost": [{"PutRequest": {"Item": {"pk": {"S": "k"}}}}]}}),
    );
    assert!(svc.batch_write_item(&req).is_err());
}

// ── Global tables ──

#[test]
fn create_and_describe_global_table() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "CreateGlobalTable",
        json!({
            "GlobalTableName": "test-table",
            "ReplicationGroup": [{"RegionName": "us-east-1"}, {"RegionName": "eu-west-1"}],
        }),
    );
    svc.create_global_table(&req).unwrap();

    let req = make_request(
        "DescribeGlobalTable",
        json!({"GlobalTableName": "test-table"}),
    );
    let resp = svc.describe_global_table(&req).unwrap();
    let b = body_json(&resp);
    assert!(b["GlobalTableDescription"].is_object());
}

#[test]
fn list_global_tables() {
    let svc = make_service();
    let req = make_request("ListGlobalTables", json!({}));
    let resp = svc.list_global_tables(&req).unwrap();
    let b = body_json(&resp);
    assert!(b["GlobalTables"].as_array().is_some());
}

// ── Backup operations ──

#[test]
fn create_and_list_backups() {
    let svc = make_service();
    create_test_table(&svc);

    let req = make_request(
        "CreateBackup",
        json!({"TableName": "test-table", "BackupName": "bak1"}),
    );
    let resp = svc.create_backup(&req).unwrap();
    let b = body_json(&resp);
    assert!(b["BackupDetails"]["BackupArn"].as_str().is_some());

    let req = make_request("ListBackups", json!({}));
    let resp = svc.list_backups(&req).unwrap();
    let b = body_json(&resp);
    assert!(!b["BackupSummaries"].as_array().unwrap().is_empty());
}

// ── Import/Export ──

#[test]
fn describe_import_not_found() {
    let svc = make_service();
    let req = make_request(
        "DescribeImport",
        json!({"ImportArn": "arn:aws:dynamodb:us-east-1:123:table/t/import/ghost"}),
    );
    assert!(svc.describe_import(&req).is_err());
}

#[test]
fn describe_export_not_found() {
    let svc = make_service();
    let req = make_request(
        "DescribeExport",
        json!({"ExportArn": "arn:aws:dynamodb:us-east-1:123:table/t/export/ghost"}),
    );
    assert!(svc.describe_export(&req).is_err());
}

// ── tables.rs error branches ──

#[test]
fn create_table_missing_name_errors() {
    let svc = make_service();
    let req = make_request(
        "CreateTable",
        json!({
            "AttributeDefinitions": [{"AttributeName": "k", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "k", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    );
    assert!(svc.create_table(&req).is_err());
}

#[test]
fn create_table_duplicate_errors() {
    let svc = make_service();
    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "dup",
            "AttributeDefinitions": [{"AttributeName": "k", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "k", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    );
    svc.create_table(&req).unwrap();
    assert!(svc.create_table(&req).is_err());
}

#[test]
fn delete_table_missing_name_errors() {
    let svc = make_service();
    let req = make_request("DeleteTable", json!({}));
    assert!(svc.delete_table(&req).is_err());
}

#[test]
fn delete_table_not_found_errors() {
    let svc = make_service();
    let req = make_request("DeleteTable", json!({"TableName": "ghost"}));
    assert!(svc.delete_table(&req).is_err());
}

#[test]
fn describe_table_missing_name_errors() {
    let svc = make_service();
    let req = make_request("DescribeTable", json!({}));
    assert!(svc.describe_table(&req).is_err());
}

#[test]
fn describe_table_not_found_errors() {
    let svc = make_service();
    let req = make_request("DescribeTable", json!({"TableName": "ghost"}));
    assert!(svc.describe_table(&req).is_err());
}

#[test]
fn update_table_missing_name_errors() {
    let svc = make_service();
    let req = make_request("UpdateTable", json!({}));
    assert!(svc.update_table(&req).is_err());
}

#[test]
fn update_table_not_found_errors() {
    let svc = make_service();
    let req = make_request("UpdateTable", json!({"TableName": "ghost"}));
    assert!(svc.update_table(&req).is_err());
}

#[test]
fn list_tables_pagination() {
    let svc = make_service();
    for i in 0..5 {
        let req = make_request(
            "CreateTable",
            json!({
                "TableName": format!("pt{i}"),
                "AttributeDefinitions": [{"AttributeName": "k", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "k", "KeyType": "HASH"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        );
        svc.create_table(&req).unwrap();
    }
    let req = make_request("ListTables", json!({"Limit": 2}));
    let resp = svc.list_tables(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["TableNames"].as_array().unwrap().len(), 2);
    assert!(body["LastEvaluatedTableName"].is_string());
}

#[test]
fn list_tables_start_exclusive() {
    let svc = make_service();
    for i in 0..3 {
        let req = make_request(
            "CreateTable",
            json!({
                "TableName": format!("pt{i}"),
                "AttributeDefinitions": [{"AttributeName": "k", "AttributeType": "S"}],
                "KeySchema": [{"AttributeName": "k", "KeyType": "HASH"}],
                "BillingMode": "PAY_PER_REQUEST"
            }),
        );
        svc.create_table(&req).unwrap();
    }
    let req = make_request("ListTables", json!({"ExclusiveStartTableName": "pt0"}));
    let resp = svc.list_tables(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let names = body["TableNames"].as_array().unwrap();
    assert!(!names.iter().any(|n| n == "pt0"));
}

#[test]
fn update_time_to_live_unknown_table_errors() {
    let svc = make_service();
    let req = make_request(
        "UpdateTimeToLive",
        json!({
            "TableName": "ghost",
            "TimeToLiveSpecification": {"Enabled": true, "AttributeName": "ttl"}
        }),
    );
    assert!(svc.update_time_to_live(&req).is_err());
}

#[test]
fn describe_time_to_live_unknown_table_errors() {
    let svc = make_service();
    let req = make_request("DescribeTimeToLive", json!({"TableName": "ghost"}));
    assert!(svc.describe_time_to_live(&req).is_err());
}

// ── resource policy ──

#[test]
fn put_resource_policy_missing_policy_errors() {
    let svc = make_service();
    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "rp",
            "AttributeDefinitions": [{"AttributeName": "k", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "k", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    );
    svc.create_table(&req).unwrap();
    let req = make_request(
        "PutResourcePolicy",
        json!({"ResourceArn": "arn:aws:dynamodb:us-east-1:123456789012:table/rp"}),
    );
    assert!(svc.put_resource_policy(&req).is_err());
}

#[test]
fn get_resource_policy_unknown_table_errors() {
    let svc = make_service();
    let req = make_request(
        "GetResourcePolicy",
        json!({"ResourceArn": "arn:aws:dynamodb:us-east-1:123456789012:table/ghost"}),
    );
    assert!(svc.get_resource_policy(&req).is_err());
}

// ── tags ──

#[test]
fn tag_resource_unknown_table_errors() {
    let svc = make_service();
    let req = make_request(
        "TagResource",
        json!({
            "ResourceArn": "arn:aws:dynamodb:us-east-1:123456789012:table/ghost",
            "Tags": [{"Key": "k", "Value": "v"}]
        }),
    );
    assert!(svc.tag_resource(&req).is_err());
}

#[test]
fn list_tags_unknown_table_errors() {
    let svc = make_service();
    let req = make_request(
        "ListTagsOfResource",
        json!({"ResourceArn": "arn:aws:dynamodb:us-east-1:123456789012:table/ghost"}),
    );
    assert!(svc.list_tags_of_resource(&req).is_err());
}

// ── backups ──

#[test]
fn create_backup_unknown_table_errors() {
    let svc = make_service();
    let req = make_request(
        "CreateBackup",
        json!({"TableName": "ghost", "BackupName": "b1"}),
    );
    assert!(svc.create_backup(&req).is_err());
}

#[test]
fn delete_backup_not_found_errors() {
    let svc = make_service();
    let req = make_request(
        "DeleteBackup",
        json!({"BackupArn": "arn:aws:dynamodb:us-east-1:123:table/t/backup/ghost"}),
    );
    assert!(svc.delete_backup(&req).is_err());
}

#[test]
fn describe_backup_not_found_errors() {
    let svc = make_service();
    let req = make_request(
        "DescribeBackup",
        json!({"BackupArn": "arn:aws:dynamodb:us-east-1:123:table/t/backup/ghost"}),
    );
    assert!(svc.describe_backup(&req).is_err());
}

#[test]
fn create_backup_round_trip_preserves_gsi_lsi_tags_ttl_sse_stream() {
    let svc = make_service();
    // Table with GSI + tags + TTL + KMS-encrypted + streams enabled.
    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "rich",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gsi_pk", "AttributeType": "S"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": [{
                "IndexName": "by-gsi",
                "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }],
            "Tags": [{"Key": "env", "Value": "prod"}],
            "SSESpecification": {"Enabled": true, "SSEType": "KMS"},
            "StreamSpecification": {
                "StreamEnabled": true,
                "StreamViewType": "NEW_AND_OLD_IMAGES",
            },
        }),
    );
    svc.create_table(&req).unwrap();

    // Enable TTL.
    svc.update_time_to_live(&make_request(
        "UpdateTimeToLive",
        json!({
            "TableName": "rich",
            "TimeToLiveSpecification": {"Enabled": true, "AttributeName": "expire_at"},
        }),
    ))
    .unwrap();

    let resp = svc
        .create_backup(&make_request(
            "CreateBackup",
            json!({"TableName": "rich", "BackupName": "snap"}),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let backup_arn = body["BackupDetails"]["BackupArn"]
        .as_str()
        .unwrap()
        .to_string();

    svc.restore_table_from_backup(&make_request(
        "RestoreTableFromBackup",
        json!({"TargetTableName": "restored", "BackupArn": backup_arn}),
    ))
    .unwrap();

    let desc = svc
        .describe_table(&make_request(
            "DescribeTable",
            json!({"TableName": "restored"}),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(desc.body.expect_bytes()).unwrap();
    let table = &body["Table"];
    assert!(
        table["GlobalSecondaryIndexes"]
            .as_array()
            .map(|a| !a.is_empty())
            .unwrap_or(false),
        "restored table must keep GSI definitions"
    );
    assert!(
        table["StreamSpecification"]["StreamEnabled"]
            .as_bool()
            .unwrap_or(false),
        "restored table must keep streams enabled"
    );
    assert_eq!(
        table["SSEDescription"]["Status"].as_str(),
        Some("ENABLED"),
        "restored table must keep SSE enabled"
    );

    // Tags survive (returned by ListTagsOfResource).
    let arn = table["TableArn"].as_str().unwrap().to_string();
    let tags_resp = svc
        .list_tags_of_resource(&make_request(
            "ListTagsOfResource",
            json!({"ResourceArn": arn}),
        ))
        .unwrap();
    let tags_body: Value = serde_json::from_slice(tags_resp.body.expect_bytes()).unwrap();
    let tags = tags_body["Tags"].as_array().unwrap();
    assert!(tags
        .iter()
        .any(|t| t["Key"] == "env" && t["Value"] == "prod"));
}

#[test]
fn scan_with_consistent_read_on_gsi_rejected() {
    let svc = make_service();
    svc.create_table(&make_request(
        "CreateTable",
        json!({
            "TableName": "t",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "gsi_pk", "AttributeType": "S"},
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": [{
                "IndexName": "by-gsi",
                "KeySchema": [{"AttributeName": "gsi_pk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"},
            }],
        }),
    ))
    .unwrap();
    let err = svc
        .scan(&make_request(
            "Scan",
            json!({"TableName": "t", "IndexName": "by-gsi", "ConsistentRead": true}),
        ))
        .err()
        .expect("scan with ConsistentRead on GSI must fail");
    assert!(format!("{err:?}").contains("Consistent reads are not supported"));
}

#[test]
fn restore_table_from_backup_not_found_errors() {
    let svc = make_service();
    let req = make_request(
        "RestoreTableFromBackup",
        json!({
            "TargetTableName": "restored",
            "BackupArn": "arn:aws:dynamodb:us-east-1:123:table/t/backup/ghost"
        }),
    );
    assert!(svc.restore_table_from_backup(&req).is_err());
}

#[test]
fn update_continuous_backups_unknown_table_errors() {
    let svc = make_service();
    let req = make_request(
        "UpdateContinuousBackups",
        json!({
            "TableName": "ghost",
            "PointInTimeRecoverySpecification": {"PointInTimeRecoveryEnabled": true}
        }),
    );
    assert!(svc.update_continuous_backups(&req).is_err());
}

// ── items.rs: put_item error branches ──

#[test]
fn put_item_accepts_table_arn() {
    let svc = make_service();
    create_test_table(&svc);
    let req = make_request(
        "PutItem",
        json!({
            "TableName": "arn:aws:dynamodb:us-east-1:123456789012:table/test-table",
            "Item": {"pk": {"S": "from-arn"}}
        }),
    );
    svc.put_item(&req).unwrap();

    let req = make_request(
        "GetItem",
        json!({
            "TableName": "arn:aws:dynamodb:us-east-1:123456789012:table/test-table/index/idx",
            "Key": {"pk": {"S": "from-arn"}},
        }),
    );
    let resp = svc.get_item(&req).unwrap();
    let b = body_json(&resp);
    assert_eq!(b["Item"]["pk"]["S"], "from-arn");
}

#[test]
fn resolve_table_name_strips_arn_and_subresources() {
    use super::resolve_table_name;
    assert_eq!(resolve_table_name("plain-name"), "plain-name");
    assert_eq!(
        resolve_table_name("arn:aws:dynamodb:us-east-1:123:table/my-tbl"),
        "my-tbl"
    );
    assert_eq!(
        resolve_table_name("arn:aws:dynamodb:us-east-1:123:table/my-tbl/index/by-foo"),
        "my-tbl"
    );
    assert_eq!(
        resolve_table_name(
            "arn:aws:dynamodb:us-east-1:123:table/my-tbl/stream/2025-01-01T00:00:00.000"
        ),
        "my-tbl"
    );
    assert_eq!(
        resolve_table_name(
            "arn:aws:dynamodb:us-east-1:123:table/my-tbl/backup/01700000000000-deadbeef"
        ),
        "my-tbl"
    );
}

#[test]
fn put_item_unknown_table_errors() {
    let svc = make_service();
    let req = make_request(
        "PutItem",
        json!({
            "TableName": "ghost",
            "Item": {"k": {"S": "v"}}
        }),
    );
    assert!(svc.put_item(&req).is_err());
}

#[test]
fn put_item_missing_key_attribute_errors() {
    let svc = make_service();
    svc.create_table(&make_request(
        "CreateTable",
        json!({
            "TableName": "pmk",
            "AttributeDefinitions": [{"AttributeName": "k", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "k", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    ))
    .unwrap();
    let req = make_request(
        "PutItem",
        json!({
            "TableName": "pmk",
            "Item": {"other": {"S": "v"}}
        }),
    );
    assert!(svc.put_item(&req).is_err());
}

#[test]
fn get_item_unknown_table_errors() {
    let svc = make_service();
    let req = make_request(
        "GetItem",
        json!({"TableName": "ghost", "Key": {"k": {"S": "1"}}}),
    );
    assert!(svc.get_item(&req).is_err());
}

#[test]
fn delete_item_unknown_table_errors() {
    let svc = make_service();
    let req = make_request(
        "DeleteItem",
        json!({"TableName": "ghost", "Key": {"k": {"S": "1"}}}),
    );
    assert!(svc.delete_item(&req).is_err());
}

#[test]
fn update_item_unknown_table_errors() {
    let svc = make_service();
    let req = make_request(
        "UpdateItem",
        json!({
            "TableName": "ghost",
            "Key": {"k": {"S": "1"}},
            "UpdateExpression": "SET x = :v",
            "ExpressionAttributeValues": {":v": {"S": "val"}}
        }),
    );
    assert!(svc.update_item(&req).is_err());
}

#[test]
fn query_unknown_table_errors() {
    let svc = make_service();
    let req = make_request(
        "Query",
        json!({
            "TableName": "ghost",
            "KeyConditionExpression": "k = :v",
            "ExpressionAttributeValues": {":v": {"S": "x"}}
        }),
    );
    assert!(svc.query(&req).is_err());
}

#[test]
fn scan_unknown_table_errors() {
    let svc = make_service();
    let req = make_request("Scan", json!({"TableName": "ghost"}));
    assert!(svc.scan(&req).is_err());
}

#[test]
fn scan_with_limit_returns_ok() {
    let svc = make_service();
    svc.create_table(&make_request(
        "CreateTable",
        json!({
            "TableName": "slt",
            "AttributeDefinitions": [{"AttributeName": "k", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "k", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    ))
    .unwrap();
    for i in 0..5 {
        svc.put_item(&make_request(
            "PutItem",
            json!({
                "TableName": "slt",
                "Item": {"k": {"S": format!("key-{i}")}}
            }),
        ))
        .unwrap();
    }
    let req = make_request("Scan", json!({"TableName": "slt", "Limit": 2}));
    let resp = svc.scan(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["Count"], 2);
}

#[test]
fn batch_get_item_unknown_table_errors() {
    let svc = make_service();
    let req = make_request(
        "BatchGetItem",
        json!({
            "RequestItems": {
                "ghost": {"Keys": [{"k": {"S": "1"}}]}
            }
        }),
    );
    assert!(svc.batch_get_item(&req).is_err());
}

#[test]
fn batch_write_item_unknown_table_errors() {
    let svc = make_service();
    let req = make_request(
        "BatchWriteItem",
        json!({
            "RequestItems": {
                "ghost": [{"PutRequest": {"Item": {"k": {"S": "1"}}}}]
            }
        }),
    );
    assert!(svc.batch_write_item(&req).is_err());
}

#[test]
fn transact_write_items_unknown_table_errors() {
    let svc = make_service();
    let req = make_request(
        "TransactWriteItems",
        json!({
            "TransactItems": [{
                "Put": {"TableName": "ghost", "Item": {"k": {"S": "1"}}}
            }]
        }),
    );
    assert!(svc.transact_write_items(&req).is_err());
}

#[test]
fn transact_get_items_unknown_table_errors() {
    let svc = make_service();
    let req = make_request(
        "TransactGetItems",
        json!({
            "TransactItems": [{
                "Get": {"TableName": "ghost", "Key": {"k": {"S": "1"}}}
            }]
        }),
    );
    assert!(svc.transact_get_items(&req).is_err());
}

#[test]
fn describe_global_table_not_found_b() {
    let svc = make_service();
    let req = make_request("DescribeGlobalTable", json!({"GlobalTableName": "ghost"}));
    assert!(svc.describe_global_table(&req).is_err());
}

#[test]
fn list_global_tables_empty_ok() {
    let svc = make_service();
    let req = make_request("ListGlobalTables", json!({}));
    let resp = svc.list_global_tables(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body["GlobalTables"].is_array());
}

#[test]
fn split_on_top_level_keyword_between_swallows_inner_and() {
    let parts = split_on_top_level_keyword("x = :a AND y BETWEEN :lo AND :hi", "AND");
    assert_eq!(
        parts.len(),
        2,
        "BETWEEN's inner AND must not split; got parts = {parts:?}"
    );
}

#[test]
fn split_on_top_level_keyword_between_nested_parens() {
    let parts = split_on_top_level_keyword("(x = :a) AND (y BETWEEN :lo AND :hi)", "AND");
    assert_eq!(parts.len(), 2);
}

#[test]
fn split_on_top_level_keyword_whitespace_variants() {
    for expr in [
        "x = :a AND y = :b",
        "x=:a AND y=:b",
        "  x = :a   AND   y = :b  ",
        "x\t=\t:a\tAND\ty\t=\t:b",
        "x = :a\nAND\ny = :b",
    ] {
        let parts = split_on_top_level_keyword(expr, "AND");
        assert_eq!(parts.len(), 2, "whitespace variant failed: {expr:?}");
    }
}

#[test]
fn split_on_top_level_keyword_case_insensitive() {
    let parts = split_on_top_level_keyword("x = :a and y = :b", "AND");
    assert_eq!(parts.len(), 2);
    let parts = split_on_top_level_keyword("x = :a OR y = :b", "OR");
    assert_eq!(parts.len(), 2);
}

#[test]
fn split_on_top_level_keyword_does_not_match_inside_identifiers() {
    // `land` contains "AND" but isn't word-bounded — must not split.
    let parts = split_on_top_level_keyword("land = :a", "AND");
    assert_eq!(parts.len(), 1);
}

// ── Smithy-declared wire error code regression tests ─────────────────────
//
// These ops' Smithy error_shapes lists do not include the generic codes our
// shared helpers used to emit. Each test pins the per-op wire code so the
// strict-mode conformance probe stays happy.

fn assert_error_code(err: AwsServiceError, expected: &str) {
    match err {
        AwsServiceError::AwsError { code, .. } => {
            assert_eq!(code, expected, "wrong wire error code");
        }
        other => panic!("expected AwsError, got {other:?}"),
    }
}

#[test]
fn create_backup_unknown_table_emits_table_not_found() {
    let svc = make_service();
    let err = svc
        .create_backup(&make_request(
            "CreateBackup",
            json!({"TableName": "ghost", "BackupName": "b1"}),
        ))
        .err()
        .unwrap();
    assert_error_code(err, "TableNotFoundException");
}

#[test]
fn describe_continuous_backups_unknown_table_emits_table_not_found() {
    let svc = make_service();
    let err = svc
        .describe_continuous_backups(&make_request(
            "DescribeContinuousBackups",
            json!({"TableName": "ghost"}),
        ))
        .err()
        .unwrap();
    assert_error_code(err, "TableNotFoundException");
}

#[test]
fn update_continuous_backups_unknown_table_emits_table_not_found() {
    let svc = make_service();
    let err = svc
        .update_continuous_backups(&make_request(
            "UpdateContinuousBackups",
            json!({
                "TableName": "ghost",
                "PointInTimeRecoverySpecification": {"PointInTimeRecoveryEnabled": true}
            }),
        ))
        .err()
        .unwrap();
    assert_error_code(err, "TableNotFoundException");
}

#[test]
fn restore_table_to_point_in_time_unknown_source_emits_table_not_found() {
    let svc = make_service();
    let err = svc
        .restore_table_to_point_in_time(&make_request(
            "RestoreTableToPointInTime",
            json!({"TargetTableName": "t2", "SourceTableName": "ghost"}),
        ))
        .err()
        .unwrap();
    assert_error_code(err, "TableNotFoundException");
}

#[test]
fn export_table_unknown_arn_emits_table_not_found() {
    let svc = make_service();
    let err = svc
        .export_table_to_point_in_time(&make_request(
            "ExportTableToPointInTime",
            json!({
                "TableArn": "arn:aws:dynamodb:us-east-1:000000000000:table/ghost",
                "S3Bucket": "b"
            }),
        ))
        .err()
        .unwrap();
    assert_error_code(err, "TableNotFoundException");
}

#[test]
fn list_backups_rejects_out_of_range_optional_params() {
    let svc = make_service();
    // Limit must be 1..=100, BackupType must be one of the documented enum
    // values, and ExclusiveStartBackupArn must be non-empty / <= 1024 chars.
    // Real AWS surfaces ValidationException for any of these; we match.
    let err = svc
        .list_backups(&make_request("ListBackups", json!({"Limit": 0})))
        .err()
        .expect("Limit=0 must be rejected");
    assert_error_code(err, "ValidationException");

    let err = svc
        .list_backups(&make_request("ListBackups", json!({"Limit": 101})))
        .err()
        .expect("Limit=101 must be rejected");
    assert_error_code(err, "ValidationException");

    let err = svc
        .list_backups(&make_request("ListBackups", json!({"BackupType": "BOGUS"})))
        .err()
        .expect("BackupType=BOGUS must be rejected");
    assert_error_code(err, "ValidationException");

    let err = svc
        .list_backups(&make_request(
            "ListBackups",
            json!({"ExclusiveStartBackupArn": ""}),
        ))
        .err()
        .expect("empty BackupArn must be rejected");
    assert_error_code(err, "ValidationException");
}

#[test]
fn list_imports_rejects_out_of_range_optional_params() {
    let svc = make_service();
    let err = svc
        .list_imports(&make_request("ListImports", json!({"PageSize": 0})))
        .err()
        .expect("PageSize=0 must be rejected");
    assert_error_code(err, "ValidationException");

    let err = svc
        .list_imports(&make_request("ListImports", json!({"PageSize": 26})))
        .err()
        .expect("PageSize=26 must be rejected");
    assert_error_code(err, "ValidationException");

    let err = svc
        .list_imports(&make_request("ListImports", json!({"NextToken": ""})))
        .err()
        .expect("empty NextToken must be rejected");
    assert_error_code(err, "ValidationException");
}

#[test]
fn create_table_missing_table_name_emits_limit_exceeded() {
    let svc = make_service();
    let err = svc
        .create_table(&make_request(
            "CreateTable",
            json!({"BillingMode": "PAY_PER_REQUEST"}),
        ))
        .err()
        .unwrap();
    assert_error_code(err, "LimitExceededException");
}

#[test]
fn execute_statement_partiql_error_emits_resource_not_found() {
    let svc = make_service();
    let err = svc
        .execute_statement(&make_request(
            "ExecuteStatement",
            json!({"Statement": "SELECT FROM"}),
        ))
        .err()
        .unwrap();
    assert_error_code(err, "ResourceNotFoundException");
}

/// No snapshot store (memory mode) -> no persist hook for the CFN provisioner.
#[test]
fn snapshot_hook_is_none_without_store() {
    let svc = make_service();
    assert!(svc.snapshot_hook().is_none());
}

/// With a store, the hook is present and invoking it runs the whole-state
/// persist path the CloudFormation provisioner uses after mutating DynamoDB
/// state directly.
#[tokio::test]
async fn snapshot_hook_fires_with_store() {
    let store: Arc<dyn fakecloud_persistence::SnapshotStore> =
        Arc::new(fakecloud_persistence::MemorySnapshotStore::new());
    let svc = make_service().with_snapshot_store(store);
    let hook = svc
        .snapshot_hook()
        .expect("hook present when a store is set");
    hook().await;
}

#[test]
fn put_item_rejects_wrong_key_attribute_type() {
    // The `pk` key is declared S; PutItem with `pk: {N: ...}` must be a
    // ValidationException, not a silent store of a malformed key that a
    // correctly-typed GetItem can never find (bug-audit 2026-06-20, 1.13).
    let svc = make_service();
    create_test_table(&svc);

    let bad = make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": { "pk": { "N": "1" } }
        }),
    );
    match svc.put_item(&bad) {
        Err(e) => assert_eq!(e.code(), "ValidationException"),
        Ok(_) => panic!("a wrong-typed key must be rejected"),
    }

    // The correctly-typed key still works.
    let good = make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": { "pk": { "S": "ok" } }
        }),
    );
    svc.put_item(&good).unwrap();
}

#[test]
fn get_item_rejects_wrong_key_attribute_type() {
    let svc = make_service();
    create_test_table(&svc);
    let bad = make_request(
        "GetItem",
        json!({
            "TableName": "test-table",
            "Key": { "pk": { "N": "1" } }
        }),
    );
    match svc.get_item(&bad) {
        Err(e) => assert_eq!(e.code(), "ValidationException"),
        Ok(_) => panic!("a wrong-typed key must be rejected on GetItem"),
    }
}

#[test]
fn update_item_applies_legacy_attribute_updates() {
    // Legacy AttributeUpdates (no UpdateExpression) used to write nothing and
    // leave a key-only stub -- silent data loss (bug-audit 2026-06-20, 1.2).
    let svc = make_service();
    create_test_table(&svc);
    svc.put_item(&make_request(
        "PutItem",
        json!({ "TableName": "test-table", "Item": { "pk": { "S": "u1" }, "n": { "N": "5" } } }),
    ))
    .unwrap();

    // PUT a new attr, ADD to the number, DELETE an attr -- all via the legacy API.
    svc.update_item(&make_request(
        "UpdateItem",
        json!({
            "TableName": "test-table",
            "Key": { "pk": { "S": "u1" } },
            "AttributeUpdates": {
                "name": { "Value": { "S": "alice" }, "Action": "PUT" },
                "n": { "Value": { "N": "3" }, "Action": "ADD" },
            }
        }),
    ))
    .unwrap();

    let resp = svc
        .get_item(&make_request(
            "GetItem",
            json!({ "TableName": "test-table", "Key": { "pk": { "S": "u1" } } }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let item = &body["Item"];
    assert_eq!(item["name"]["S"], "alice", "PUT must store the attribute");
    assert_eq!(item["n"]["N"], "8", "ADD must increment 5 + 3 = 8");
}

#[test]
fn put_item_honors_legacy_expected() {
    // Legacy Expected (pre-2014 conditional writes) was ignored on Put/Delete
    // (bug-audit 2026-06-20, 1.2): a put-if-absent guard silently overwrote.
    let svc = make_service();
    create_test_table(&svc);

    // Exists:false guard succeeds for a brand-new key.
    svc.put_item(&make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": { "pk": { "S": "u1" }, "v": { "N": "1" } },
            "Expected": { "pk": { "Exists": false } }
        }),
    ))
    .expect("first put-if-absent should succeed");

    // Same guard now fails because the item exists.
    let err = svc.put_item(&make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": { "pk": { "S": "u1" }, "v": { "N": "2" } },
            "Expected": { "pk": { "Exists": false } }
        }),
    ));
    assert!(err.is_err(), "put-if-absent must fail when key exists");

    // Value equality guard (implicit EQ) succeeds when it matches.
    svc.put_item(&make_request(
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": { "pk": { "S": "u1" }, "v": { "N": "3" } },
            "Expected": { "v": { "Value": { "N": "1" } } }
        }),
    ))
    .expect("equality guard should match stored value");
}

#[test]
fn delete_item_legacy_expected_with_or_operator() {
    // ConditionalOperator:OR was hard-coded to AND (bug-audit 2026-06-20, 1.2).
    let svc = make_service();
    create_test_table(&svc);
    svc.put_item(&make_request(
        "PutItem",
        json!({ "TableName": "test-table", "Item": { "pk": { "S": "u1" }, "status": { "S": "active" }, "n": { "N": "5" } } }),
    ))
    .unwrap();

    // Only the status condition holds; with OR the delete must still proceed.
    svc.delete_item(&make_request(
        "DeleteItem",
        json!({
            "TableName": "test-table",
            "Key": { "pk": { "S": "u1" } },
            "ConditionalOperator": "OR",
            "Expected": {
                "status": { "Value": { "S": "active" } },
                "n": { "Value": { "N": "999" } }
            }
        }),
    ))
    .expect("OR condition should pass when one branch holds");

    let resp = svc
        .get_item(&make_request(
            "GetItem",
            json!({ "TableName": "test-table", "Key": { "pk": { "S": "u1" } } }),
        ))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert!(body.get("Item").is_none(), "item should have been deleted");
}

#[test]
fn update_table_prunes_attributes_orphaned_by_gsi_delete() {
    // Real DynamoDB keeps AttributeDefinitions equal to the set of attributes
    // referenced by the table key + every index key. When a GSI (and the
    // attribute that backed its key) is deleted, DescribeTable no longer lists
    // that attribute. Terraform's `aws_dynamodb_table` refresh treats a
    // lingering orphan as drift, so update_table must prune it.
    let svc = make_service();
    svc.create_table(&make_request(
        "CreateTable",
        json!({
            "TableName": "t",
            "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
            "AttributeDefinitions": [
                { "AttributeName": "pk", "AttributeType": "S" },
                { "AttributeName": "gsiKey", "AttributeType": "S" }
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": [{
                "IndexName": "gsi",
                "KeySchema": [{ "AttributeName": "gsiKey", "KeyType": "HASH" }],
                "Projection": { "ProjectionType": "ALL" }
            }]
        }),
    ))
    .unwrap();

    // Delete the GSI; the provider sends the trimmed AttributeDefinitions too.
    svc.update_table(&make_request(
        "UpdateTable",
        json!({
            "TableName": "t",
            "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
            "GlobalSecondaryIndexUpdates": [{ "Delete": { "IndexName": "gsi" } }]
        }),
    ))
    .unwrap();

    let resp = svc
        .describe_table(&make_request("DescribeTable", json!({ "TableName": "t" })))
        .unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let attrs: Vec<&str> = body["Table"]["AttributeDefinitions"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|a| a["AttributeName"].as_str())
        .collect();
    assert_eq!(attrs, ["pk"], "orphaned gsiKey attribute should be pruned");
}

#[test]
fn filter_comparison_missing_attribute_is_false() {
    // An item without the `status` attribute. AWS: every comparison against a
    // missing attribute is false (1.5).
    let item: HashMap<String, AttributeValue> = [("pk".to_string(), json!({"S": "a"}))]
        .into_iter()
        .collect();
    let names: HashMap<String, String> = HashMap::new();
    let values: HashMap<String, Value> = [(":s".to_string(), json!({"S": "done"}))]
        .into_iter()
        .collect();

    for op in ["status <> :s", "status < :s", "status <= :s", "status = :s"] {
        assert!(
            !evaluate_filter_expression(op, &item, &names, &values),
            "`{op}` must be false when `status` is missing"
        );
    }
}

#[test]
fn filter_equality_is_numeric_aware() {
    // 3.10 and 3.1 are the same DynamoDB number; `=` must match and `<>` must
    // not (1.16).
    let item: HashMap<String, AttributeValue> = [("n".to_string(), json!({"N": "3.10"}))]
        .into_iter()
        .collect();
    let names: HashMap<String, String> = HashMap::new();
    let values: HashMap<String, Value> = [(":v".to_string(), json!({"N": "3.1"}))]
        .into_iter()
        .collect();

    assert!(evaluate_filter_expression("n = :v", &item, &names, &values));
    assert!(!evaluate_filter_expression(
        "n <> :v", &item, &names, &values
    ));
}

#[test]
fn set_arithmetic_on_missing_operand_errors() {
    // `a = b + :v` where `b` does not exist: AWS rejects with ValidationException
    // rather than treating the missing operand as 0 (1.10).
    let item: HashMap<String, AttributeValue> =
        [("a".to_string(), json!({"N": "1"}))].into_iter().collect();
    let names: HashMap<String, String> = HashMap::new();
    let values: HashMap<String, Value> = [(":v".to_string(), json!({"N": "5"}))]
        .into_iter()
        .collect();

    let res = evaluate_arithmetic_rhs("b", ":v", true, &item, &names, &values);
    assert!(
        res.is_err(),
        "missing SET operand must error, not zero-default"
    );

    // Existing operand still works.
    let ok = evaluate_arithmetic_rhs("a", ":v", true, &item, &names, &values);
    assert!(ok.is_ok());
}
