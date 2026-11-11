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
    let snapshot: DynamoDbSnapshot = crate::state::parse_dynamodb_snapshot(bytes).unwrap();
    assert_eq!(snapshot.schema_version, DYNAMODB_SNAPSHOT_SCHEMA_VERSION);

    let state: SharedDynamoDbState = Arc::new(parking_lot::RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    if let Some(accounts) = snapshot.accounts {
        *state.write() = accounts;
    } else if let Some(single_state) = snapshot.state {
        let account_id = single_state.account_id().to_string();
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

    let result = resolve_path("info.address.city", &item, &HashMap::new());
    assert_eq!(result, Some(json!({"S": "NYC"})));
}

#[test]
fn test_resolve_nested_path_list_then_map() {
    let mut item = HashMap::new();
    item.insert(
        "items".to_string(),
        json!({"L": [{"M": {"sku": {"S": "ABC"}}}]}),
    );

    let result = resolve_path("items[0].sku", &item, &HashMap::new());
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
        let state = __mas.regional("123456789012", "us-east-1").unwrap();
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
        Err(e) => {
            assert_eq!(e.code(), "PolicyNotFoundException");
            // awsJson1.0: client errors are HTTP 400, never 404.
            assert_eq!(e.status(), http::StatusCode::BAD_REQUEST);
        }
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
    // The start call reports the export as accepted; DescribeExport shows
    // the finished export.
    assert_eq!(body["ExportDescription"]["ExportStatus"], "IN_PROGRESS");

    // Describe
    let req = make_request("DescribeExport", json!({ "ExportArn": export_arn }));
    let resp = svc.describe_export(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ExportDescription"]["S3Bucket"], "my-bucket");
    assert_eq!(body["ExportDescription"]["ExportStatus"], "COMPLETED");

    // List
    let req = make_request("ListExports", json!({}));
    let resp = svc.list_exports(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ExportSummaries"].as_array().unwrap().len(), 1);
}

/// An export to a missing bucket is accepted IN_PROGRESS and then described
/// as FAILED with the reason.
#[test]
fn failed_export_reports_failure_on_describe() {
    let s3: fakecloud_s3::SharedS3State = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    let svc = make_service().with_s3(s3);
    create_test_table(&svc);
    let req = make_request(
        "ExportTableToPointInTime",
        json!({
            "TableArn": "arn:aws:dynamodb:us-east-1:123456789012:table/test-table",
            "S3Bucket": "missing-bucket"
        }),
    );
    let resp = svc.export_table_to_point_in_time(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ExportDescription"]["ExportStatus"], "IN_PROGRESS");
    let export_arn = body["ExportDescription"]["ExportArn"].as_str().unwrap();

    let req = make_request("DescribeExport", json!({ "ExportArn": export_arn }));
    let resp = svc.describe_export(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(body["ExportDescription"]["ExportStatus"], "FAILED");
    assert_eq!(body["ExportDescription"]["FailureCode"], "S3NoSuchBucket");
}

/// A KMS hook with a readable envelope: `enc:<key>:<plaintext>`.
struct EnvelopeKms;

impl fakecloud_core::delivery::KmsHook for EnvelopeKms {
    fn encrypt(
        &self,
        _account_id: &str,
        _region: &str,
        key_id: &str,
        plaintext: &[u8],
        _caller: fakecloud_core::delivery::KmsCaller<'_>,
        _encryption_context: std::collections::HashMap<String, String>,
    ) -> Result<String, fakecloud_core::delivery::KmsError> {
        use base64::Engine as _;
        Ok(format!(
            "enc:{key_id}:{}",
            base64::engine::general_purpose::STANDARD.encode(plaintext)
        ))
    }

    fn decrypt(
        &self,
        _account_id: &str,
        ciphertext_b64: &str,
        _caller: fakecloud_core::delivery::KmsCaller<'_>,
        _encryption_context: std::collections::HashMap<String, String>,
    ) -> Result<Vec<u8>, fakecloud_core::delivery::KmsError> {
        use base64::Engine as _;
        let (_, data) = ciphertext_b64
            .strip_prefix("enc:")
            .and_then(|r| r.split_once(':'))
            .ok_or_else(|| {
                fakecloud_core::delivery::KmsError::new(
                    fakecloud_core::delivery::KmsErrorKind::InvalidCiphertext,
                    "not an envelope",
                )
            })?;
        base64::engine::general_purpose::STANDARD
            .decode(data)
            .map_err(|e| {
                fakecloud_core::delivery::KmsError::new(
                    fakecloud_core::delivery::KmsErrorKind::InvalidCiphertext,
                    e.to_string(),
                )
            })
    }
}

/// An export's `S3SseAlgorithm` / `S3SseKmsKeyId` decide how every object it
/// writes is encrypted, overriding the bucket's default encryption; an export
/// naming no algorithm takes the bucket default.
#[test]
fn export_objects_take_the_requested_encryption() {
    let s3: fakecloud_s3::SharedS3State = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    {
        let mut accounts = s3.write();
        let mut bucket = fakecloud_s3::S3Bucket::new("exports", "us-east-1", "owner");
        bucket.encryption_config = Some(
            "<ServerSideEncryptionConfiguration><Rule><ApplyServerSideEncryptionByDefault>\
             <SSEAlgorithm>aws:kms</SSEAlgorithm><KMSMasterKeyID>bucket-key</KMSMasterKeyID>\
             </ApplyServerSideEncryptionByDefault></Rule></ServerSideEncryptionConfiguration>"
                .to_string(),
        );
        accounts
            .get_or_create("123456789012")
            .buckets
            .insert("exports".to_string(), bucket);
    }
    // The S3 service installs the KMS SSE-KMS objects are encrypted with.
    let _s3_service = fakecloud_s3::S3Service::new(
        s3.clone(),
        Arc::new(fakecloud_core::delivery::DeliveryBus::new()),
    )
    .with_kms_hook(Arc::new(EnvelopeKms));
    let svc = make_service().with_s3(s3.clone());
    create_test_table(&svc);
    svc.put_item(&make_request(
        "PutItem",
        json!({ "TableName": "test-table", "Item": { "pk": { "S": "row-1" } } }),
    ))
    .unwrap();

    for (prefix, request, expected_sse, expected_key) in [
        (
            "kms",
            json!({ "S3SseAlgorithm": "KMS", "S3SseKmsKeyId": "export-key" }),
            "aws:kms",
            Some("export-key"),
        ),
        ("aes", json!({ "S3SseAlgorithm": "AES256" }), "AES256", None),
        ("default", json!({}), "aws:kms", Some("bucket-key")),
    ] {
        let mut body = json!({
            "TableArn": "arn:aws:dynamodb:us-east-1:123456789012:table/test-table",
            "S3Bucket": "exports",
            "S3Prefix": prefix,
        });
        for (k, v) in request.as_object().unwrap() {
            body[k] = v.clone();
        }
        let req = make_request("ExportTableToPointInTime", body);
        let resp = svc.export_table_to_point_in_time(&req).unwrap();
        let started: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let export_arn = started["ExportDescription"]["ExportArn"].as_str().unwrap();
        let req = make_request("DescribeExport", json!({ "ExportArn": export_arn }));
        let resp = svc.describe_export(&req).unwrap();
        let described: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        assert_eq!(
            described["ExportDescription"]["ExportStatus"], "COMPLETED",
            "{described}"
        );

        let accounts = s3.read();
        let objects: Vec<_> = accounts.get("123456789012").unwrap().buckets["exports"]
            .objects
            .iter()
            .filter(|(k, _)| k.starts_with(&format!("{prefix}/")))
            .map(|(_, o)| o.clone())
            .collect();
        assert_eq!(objects.len(), 4, "{prefix}: _started, data, two manifests");
        for obj in objects {
            assert_eq!(
                obj.sse_algorithm.as_deref(),
                Some(expected_sse),
                "{}",
                obj.key
            );
            assert_eq!(obj.sse_kms_key_id.as_deref(), expected_key, "{}", obj.key);
        }
        drop(accounts);

        // ImportTable reads the export back through S3's object reader, so
        // an SSE-KMS export decrypts and re-imports to the same items.
        let export_id = export_arn.rsplit('/').next().unwrap();
        let table = format!("reimported-{prefix}");
        let req = make_request(
            "ImportTable",
            json!({
                "InputFormat": "DYNAMODB_JSON",
                "InputCompressionType": "GZIP",
                "S3BucketSource": {
                    "S3Bucket": "exports",
                    "S3KeyPrefix": format!("{prefix}/AWSDynamoDB/{export_id}/data/"),
                },
                "TableCreationParameters": {
                    "TableName": table,
                    "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
                    "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
                    "BillingMode": "PAY_PER_REQUEST"
                }
            }),
        );
        let resp = svc.import_table(&req).unwrap();
        let started: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let import_arn = started["ImportTableDescription"]["ImportArn"]
            .as_str()
            .unwrap()
            .to_string();
        let req = make_request("DescribeImport", json!({ "ImportArn": import_arn }));
        let resp = svc.describe_import(&req).unwrap();
        let described: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
        let d = &described["ImportTableDescription"];
        assert_eq!(d["ImportStatus"], "COMPLETED", "{prefix}: {described}");
        assert_eq!(d["ErrorCount"], 0, "{prefix}: {described}");
        assert_eq!(d["ImportedItemCount"], 1, "{prefix}: {described}");
    }
}

/// A KMS whose keys encrypt but refuse to decrypt (a disabled key, a key
/// policy that denies the reader).
struct DenyingKms;

impl fakecloud_core::delivery::KmsHook for DenyingKms {
    fn encrypt(
        &self,
        _account_id: &str,
        _region: &str,
        key_id: &str,
        _plaintext: &[u8],
        _caller: fakecloud_core::delivery::KmsCaller<'_>,
        _encryption_context: std::collections::HashMap<String, String>,
    ) -> Result<String, fakecloud_core::delivery::KmsError> {
        Ok(format!("envelope-under-{key_id}"))
    }

    fn decrypt(
        &self,
        _account_id: &str,
        _ciphertext_b64: &str,
        _caller: fakecloud_core::delivery::KmsCaller<'_>,
        _encryption_context: std::collections::HashMap<String, String>,
    ) -> Result<Vec<u8>, fakecloud_core::delivery::KmsError> {
        Err(fakecloud_core::delivery::KmsError::new(
            fakecloud_core::delivery::KmsErrorKind::Disabled,
            "kms key is disabled",
        ))
    }
}

/// Install an S3 service on `s3` whose KMS refuses every decrypt, and
/// store `body` as an SSE-KMS object at `bucket/key` in `account`.
fn put_undecryptable(
    s3: &fakecloud_s3::SharedS3State,
    account: &str,
    bucket: &str,
    key: &str,
    body: &[u8],
) -> fakecloud_s3::S3Service {
    let svc = fakecloud_s3::S3Service::new(
        s3.clone(),
        std::sync::Arc::new(fakecloud_core::delivery::DeliveryBus::new()),
    )
    .with_kms_hook(std::sync::Arc::new(DenyingKms));
    fakecloud_s3::delivery::put_object_via_store_with_encryption(
        s3,
        None,
        account,
        bucket,
        key,
        body.to_vec(),
        None,
        fakecloud_s3::delivery::DeliveredEncryption::SseKms { key_id: None },
    )
    .unwrap();
    svc
}

/// An import whose source object cannot be decrypted (its SSE-KMS key
/// refuses) fails as a whole with the S3 access failure, and the table it
/// was creating is dropped, instead of completing with row errors.
#[test]
fn import_of_an_undecryptable_source_fails() {
    let s3: fakecloud_s3::SharedS3State = Arc::new(RwLock::new(
        fakecloud_core::multi_account::MultiAccountState::new("123456789012", "us-east-1", ""),
    ));
    s3.write().get_or_create("123456789012").buckets.insert(
        "src".to_string(),
        fakecloud_s3::S3Bucket::new("src", "us-east-1", "owner"),
    );
    let _s3_service = put_undecryptable(
        &s3,
        "123456789012",
        "src",
        "data/part-0.json",
        b"{\"Item\":{\"pk\":{\"S\":\"a\"}}}\n",
    );
    let svc = make_service().with_s3(s3);
    let req = make_request(
        "ImportTable",
        json!({
            "InputFormat": "DYNAMODB_JSON",
            "S3BucketSource": { "S3Bucket": "src", "S3KeyPrefix": "data/" },
            "TableCreationParameters": {
                "TableName": "from-kms",
                "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
                "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
                "BillingMode": "PAY_PER_REQUEST"
            }
        }),
    );
    let resp = svc.import_table(&req).unwrap();
    let started: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let import_arn = started["ImportTableDescription"]["ImportArn"]
        .as_str()
        .unwrap()
        .to_string();
    let req = make_request("DescribeImport", json!({ "ImportArn": import_arn }));
    let resp = svc.describe_import(&req).unwrap();
    let described: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let d = &described["ImportTableDescription"];
    assert_eq!(d["ImportStatus"], "FAILED", "{described}");
    assert_eq!(d["FailureCode"], "S3AccessDenied", "{described}");
    let message = d["FailureMessage"].as_str().unwrap();
    assert!(message.contains("src/data/part-0.json"), "{message}");
    assert!(message.contains("kms key is disabled"), "{message}");
    let req = make_request("DescribeTable", json!({ "TableName": "from-kms" }));
    assert!(svc.describe_table(&req).is_err(), "the table is dropped");
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
    assert_eq!(
        body["ImportTableDescription"]["ImportStatus"],
        "IN_PROGRESS"
    );

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
fn gsi_query_excludes_items_missing_index_sort_key() {
    // Regression: an item lacking the index's sort key is not part of a
    // sparse GSI and must not appear in (or be counted by) an index Query.
    // Before the fix, query() filtered only by the key condition (which
    // constrains the hash key) and returned the phantom item.
    let svc = make_service();
    create_gsi_table(&svc);

    // Two full items carry both GSI key attributes.
    for i in 0..2 {
        let req = make_request(
            "PutItem",
            json!({
                "TableName": "gsi-table",
                "Item": {
                    "pk": { "S": format!("full{i}") },
                    "gsi_pk": { "S": "shared" },
                    "gsi_sk": { "S": format!("sort{i}") }
                }
            }),
        );
        svc.put_item(&req).unwrap();
    }
    // One item carries the GSI hash key but NOT the GSI sort key, so it is
    // absent from the sparse index.
    let req = make_request(
        "PutItem",
        json!({
            "TableName": "gsi-table",
            "Item": {
                "pk": { "S": "phantom" },
                "gsi_pk": { "S": "shared" }
            }
        }),
    );
    svc.put_item(&req).unwrap();

    let req = make_request(
        "Query",
        json!({
            "TableName": "gsi-table",
            "IndexName": "gsi-index",
            "KeyConditionExpression": "gsi_pk = :v",
            "ExpressionAttributeValues": { ":v": { "S": "shared" } }
        }),
    );
    let resp = svc.query(&req).unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        body["Count"], 2,
        "phantom item missing gsi_sk must be excluded"
    );
    assert_eq!(body["ScannedCount"], 2);
    let pks: Vec<&str> = body["Items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|it| it["pk"]["S"].as_str().unwrap())
        .collect();
    assert!(
        !pks.contains(&"phantom"),
        "phantom must not be returned: {pks:?}"
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
    // A missing attribute equals nothing: `=` fails the condition and `<>`
    // passes it.
    assert!(evaluate_condition("#s <> :v", None, &names, &values).is_ok());
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
fn test_set_list_index_past_end_appends() {
    // SET list[N] where N > len appends the value to the end of the list,
    // as AWS does.
    let mut item = HashMap::new();
    item.insert("items".to_string(), json!({"L": [{"S": "a"}, {"S": "b"}]}));

    let names: HashMap<String, String> = HashMap::new();
    let mut values = HashMap::new();
    values.insert(":v".to_string(), json!({"S": "z"}));

    apply_update_expression(&mut item, "SET items[5] = :v", &names, &values).unwrap();
    let list = item
        .get("items")
        .and_then(|v| v.get("L"))
        .and_then(|v| v.as_array())
        .unwrap();
    assert_eq!(list.len(), 3);
    assert_eq!(list[2], json!({"S": "z"}));
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
    // A base-key attribute missing from AttributeDefinitions is a
    // ValidationException, matching real DynamoDB (accepted by the conformance
    // probe via service_common_errors for dynamodb).
    assert!(err.to_string().contains("ValidationException"));
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
    // A single-item write reports the aggregate alone, with no write split.
    assert!(b["ConsumedCapacity"].get("WriteCapacityUnits").is_none());
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
    // No index was charged, so neither index map is present.
    assert!(b["ConsumedCapacity"]
        .get("GlobalSecondaryIndexes")
        .is_none());
    assert!(b["ConsumedCapacity"].get("LocalSecondaryIndexes").is_none());
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
    assert_eq!(b["ConsumedCapacity"]["CapacityUnits"], 0.5);
    assert!(b["ConsumedCapacity"].get("ReadCapacityUnits").is_none());
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
        s.regional("123456789012", "us-east-1")
            .unwrap()
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
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }),
    );
    svc.create_table(&req).unwrap();

    let req = make_request(
        "UpdateTable",
        json!({
            "TableName": "upd-table",
            "AttributeDefinitions": [{"AttributeName": "gk", "AttributeType": "S"}],
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
            "TableName": "Tbl",
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
        json!({"TableName": "Tbl", "Item": {"pk": {"S": "u1"}, "v": {"N": "1"}}}),
    ))
    .unwrap();

    let baseline = {
        let s = svc.state.read();
        let n = s
            .regional("123456789012", "us-east-1")
            .unwrap()
            .tables
            .get("Tbl")
            .unwrap()
            .stream_records
            .read()
            .len();
        n
    };

    svc.execute_statement(&make_request(
        "ExecuteStatement",
        json!({"Statement": "UPDATE \"Tbl\" SET v = 99 WHERE pk = 'u1'"}),
    ))
    .unwrap();

    let after = {
        let s = svc.state.read();
        let t = s
            .regional("123456789012", "us-east-1")
            .unwrap()
            .tables
            .get("Tbl")
            .unwrap();
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
            "TableName": "Tbl",
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
        json!({"TableName": "Tbl", "Item": {"pk": {"S": "d1"}}}),
    ))
    .unwrap();

    let baseline = {
        let s = svc.state.read();
        let n = s
            .regional("123456789012", "us-east-1")
            .unwrap()
            .tables
            .get("Tbl")
            .unwrap()
            .stream_records
            .read()
            .len();
        n
    };

    svc.execute_statement(&make_request(
        "ExecuteStatement",
        json!({"Statement": "DELETE FROM \"Tbl\" WHERE pk = 'd1'"}),
    ))
    .unwrap();

    let after = {
        let s = svc.state.read();
        let t = s
            .regional("123456789012", "us-east-1")
            .unwrap()
            .tables
            .get("Tbl")
            .unwrap();
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
            "TableName": "Tbl",
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
            json!({"Statement": "INSERT INTO \"Tbl\" VALUE {'pk': 'a'}"}),
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
            "TableName": "rp-table",
            "AttributeDefinitions": [{"AttributeName": "k", "AttributeType": "S"}],
            "KeySchema": [{"AttributeName": "k", "KeyType": "HASH"}],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    );
    svc.create_table(&req).unwrap();
    let req = make_request(
        "PutResourcePolicy",
        json!({"ResourceArn": "arn:aws:dynamodb:us-east-1:123456789012:table/rp-table"}),
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
            "TableName": "tbl",
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
            json!({"TableName": "tbl", "IndexName": "by-gsi", "ConsistentRead": true}),
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

#[test]
fn split_on_top_level_keyword_skips_quoted_spans() {
    // A separator inside a single-quoted literal or a double-quoted identifier
    // is data, not a boundary.
    let parts = split_on_top_level_keyword("addr = 'City, State', code = :c", ",");
    assert_eq!(parts, vec!["addr = 'City, State'", " code = :c"]);
    let parts = split_on_top_level_keyword("\"a,b\" = :a AND c = :b", "AND");
    assert_eq!(parts.len(), 2);
    // A keyword inside a quoted literal must not split either.
    let parts = split_on_top_level_keyword("note = 'x AND y'", "AND");
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
fn create_table_missing_table_name_is_a_validation_error() {
    let svc = make_service();
    let err = svc
        .create_table(&make_request(
            "CreateTable",
            json!({"BillingMode": "PAY_PER_REQUEST"}),
        ))
        .err()
        .unwrap();
    assert_error_code(err, "ValidationException");
}

#[test]
fn execute_statement_partiql_error_emits_validation_exception() {
    // A statement that does not parse is rejected before any table is looked
    // up, so it is a ValidationException rather than a missing table.
    let svc = make_service();
    let err = svc
        .execute_statement(&make_request(
            "ExecuteStatement",
            json!({"Statement": "SELECT FROM"}),
        ))
        .err()
        .unwrap();
    assert_error_code(err, "ValidationException");
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
            "TableName": "tbl",
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
            "TableName": "tbl",
            "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
            "GlobalSecondaryIndexUpdates": [{ "Delete": { "IndexName": "gsi" } }]
        }),
    ))
    .unwrap();

    let resp = svc
        .describe_table(&make_request(
            "DescribeTable",
            json!({ "TableName": "tbl" }),
        ))
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
fn filter_comparison_missing_attribute_semantics() {
    // An item without the `status` attribute. AWS: `=` and the ordering
    // comparisons against a missing attribute are false, while `<>` is true
    // (a missing value is not equal to anything).
    let item: HashMap<String, AttributeValue> = [("pk".to_string(), json!({"S": "a"}))]
        .into_iter()
        .collect();
    let names: HashMap<String, String> = HashMap::new();
    let values: HashMap<String, Value> = [(":s".to_string(), json!({"S": "done"}))]
        .into_iter()
        .collect();

    for op in ["status < :s", "status <= :s", "status = :s"] {
        assert!(
            !evaluate_filter_expression(op, &item, &names, &values),
            "`{op}` must be false when `status` is missing"
        );
    }
    assert!(evaluate_filter_expression(
        "status <> :s",
        &item,
        &names,
        &values
    ));
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

/// An UpdateExpression is applied clause by clause, so one that writes an
/// attribute and *then* fails would leave that write behind. UpdateItem is all
/// or nothing on AWS, so the row must come back exactly as it was (#2502
/// follow-up).
#[tokio::test]
async fn update_item_failing_partway_rolls_the_row_back() {
    let svc = make_service();
    create_test_table(&svc);

    call_dynamodb(
        &svc,
        "PutItem",
        json!({
            "TableName": "test-table",
            "Item": {"pk": {"S": "old"}, "count": {"S": "not-a-number"}}
        }),
    )
    .await;

    // `SET marker = :new` lands, then the arithmetic on a string operand fails.
    let err = svc
        .handle(make_request(
            "UpdateItem",
            json!({
                "TableName": "test-table",
                "Key": {"pk": {"S": "old"}},
                "UpdateExpression": "SET marker = :new, #c = #c + :one",
                "ExpressionAttributeNames": {"#c": "count"},
                "ExpressionAttributeValues": {":new": {"S": "new"}, ":one": {"N": "1"}}
            }),
        ))
        .await
        .err()
        .expect("the arithmetic on a string operand must be rejected");
    assert!(
        err.to_string().contains("incorrect data type"),
        "unexpected error: {err}"
    );

    let got = call_dynamodb(
        &svc,
        "GetItem",
        json!({"TableName": "test-table", "Key": {"pk": {"S": "old"}}}),
    )
    .await;
    assert_eq!(
        got["Item"],
        json!({"pk": {"S": "old"}, "count": {"S": "not-a-number"}}),
        "the rejected update was not rolled back: {got}"
    );
}

/// DynamoDB rejects any update that writes a primary-key attribute -- SET,
/// REMOVE, ADD or DELETE, by name or placeholder, nested or not, and through
/// the legacy AttributeUpdates -- and writes nothing.
#[tokio::test]
async fn update_item_rejects_writing_a_key_attribute() {
    let svc = make_service();
    svc.create_table(&make_request(
        "CreateTable",
        json!({
            "TableName": "composite",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "N"}
            ],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    ))
    .unwrap();
    let row = json!({"pk": {"S": "a"}, "sk": {"N": "1"}, "v": {"S": "x"}});
    call_dynamodb(
        &svc,
        "PutItem",
        json!({"TableName": "composite", "Item": row}),
    )
    .await;
    let key = json!({"pk": {"S": "a"}, "sk": {"N": "1"}});

    let cases: Vec<(Value, &str)> = vec![
        (
            json!({"UpdateExpression": "SET pk = :v", "ExpressionAttributeValues": {":v": {"S": "b"}}}),
            "pk",
        ),
        // Rewriting it to the value it already has is still rejected.
        (
            json!({"UpdateExpression": "SET v = :v, pk = :same", "ExpressionAttributeValues": {":v": {"S": "y"}, ":same": {"S": "a"}}}),
            "pk",
        ),
        (
            json!({"UpdateExpression": "SET #k = :v", "ExpressionAttributeNames": {"#k": "sk"}, "ExpressionAttributeValues": {":v": {"N": "2"}}}),
            "sk",
        ),
        (json!({"UpdateExpression": "REMOVE sk"}), "sk"),
        (
            json!({"UpdateExpression": "ADD sk :one", "ExpressionAttributeValues": {":one": {"N": "1"}}}),
            "sk",
        ),
        (
            json!({"UpdateExpression": "DELETE #p :s", "ExpressionAttributeNames": {"#p": "pk"}, "ExpressionAttributeValues": {":s": {"SS": ["a"]}}}),
            "pk",
        ),
        (
            json!({"AttributeUpdates": {"sk": {"Action": "PUT", "Value": {"N": "5"}}}}),
            "sk",
        ),
    ];
    for (update, attr) in cases {
        let mut body = json!({"TableName": "composite", "Key": key});
        for (k, v) in update.as_object().unwrap() {
            body[k] = v.clone();
        }
        let err = svc
            .handle(make_request("UpdateItem", body.clone()))
            .await
            .err()
            .unwrap_or_else(|| panic!("key write accepted: {body}"));
        assert_eq!(err.code(), "ValidationException", "{body}");
        assert_eq!(
            err.to_string(),
            format!(
                "ValidationException: One or more parameter values were invalid: Cannot update attribute {attr}. \
                 This attribute is part of the key"
            ),
            "{body}"
        );
    }

    // A nested path under a key attribute is rejected too. Only the error
    // code is pinned: the exact wording for this shape is not confirmed.
    let err = svc
        .handle(make_request(
            "UpdateItem",
            json!({
                "TableName": "composite", "Key": key,
                "UpdateExpression": "SET pk.nested = :v",
                "ExpressionAttributeValues": {":v": {"S": "b"}}
            }),
        ))
        .await
        .err()
        .expect("nested key write accepted");
    assert_eq!(err.code(), "ValidationException");

    let scan = call_dynamodb(&svc, "Scan", json!({"TableName": "composite"})).await;
    assert_eq!(
        scan["Items"],
        json!([row]),
        "a rejected update wrote something"
    );

    // A non-key attribute whose name merely starts with a key's name is fine.
    call_dynamodb(
        &svc,
        "UpdateItem",
        json!({
            "TableName": "composite", "Key": key,
            "UpdateExpression": "SET pk_copy = :v, skew = :v",
            "ExpressionAttributeValues": {":v": {"S": "ok"}}
        }),
    )
    .await;
}

/// The same rule holds inside a transaction and through PartiQL.
#[tokio::test]
async fn transact_and_partiql_updates_reject_writing_a_key_attribute() {
    let svc = make_service();
    create_test_table(&svc);
    call_dynamodb(
        &svc,
        "PutItem",
        json!({"TableName": "test-table", "Item": {"pk": {"S": "a"}}}),
    )
    .await;

    let err = svc
        .handle(make_request(
            "TransactWriteItems",
            json!({"TransactItems": [{"Update": {
                "TableName": "test-table",
                "Key": {"pk": {"S": "a"}},
                "UpdateExpression": "SET #k = :v",
                "ExpressionAttributeNames": {"#k": "pk"},
                "ExpressionAttributeValues": {":v": {"S": "b"}}
            }}]}),
        ))
        .await
        .err()
        .expect("transactional key write accepted");
    assert_eq!(err.code(), "ValidationException");
    assert!(
        err.to_string().contains("Cannot update attribute pk"),
        "{err}"
    );

    // Rejected as a malformed request even when another operation's condition
    // fails: DynamoDB validates the request before evaluating conditions.
    let err = svc
        .handle(make_request(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"ConditionCheck": {
                    "TableName": "test-table",
                    "Key": {"pk": {"S": "a"}},
                    "ConditionExpression": "attribute_not_exists(pk)"
                }},
                {"Update": {
                    "TableName": "test-table",
                    "Key": {"pk": {"S": "other"}},
                    "UpdateExpression": "SET pk = :v",
                    "ExpressionAttributeValues": {":v": {"S": "b"}}
                }}
            ]}),
        ))
        .await
        .err()
        .expect("a key write with a failing condition must be a ValidationException");
    assert_eq!(err.code(), "ValidationException");
    assert!(
        err.to_string().contains("Cannot update attribute pk"),
        "{err}"
    );

    let err = svc
        .handle(make_request(
            "ExecuteStatement",
            json!({"Statement": "UPDATE \"test-table\" SET \"pk\" = 'b' WHERE pk = 'a'"}),
        ))
        .await
        .err()
        .expect("PartiQL key write accepted");
    assert_eq!(err.code(), "ValidationException");
    assert!(
        err.to_string().contains("Cannot update attribute pk"),
        "{err}"
    );

    let scan = call_dynamodb(&svc, "Scan", json!({"TableName": "test-table"})).await;
    assert_eq!(scan["Items"], json!([{"pk": {"S": "a"}}]));
}

/// An UpdateItem on a key that does not exist registers a key-only row before
/// applying the expression. When the expression is then rejected, that row
/// must go with it: AWS stores nothing for a failed UpdateItem.
#[tokio::test]
async fn update_item_failing_on_a_missing_key_stores_nothing() {
    let svc = make_service();
    create_test_table(&svc);

    let err = svc
        .handle(make_request(
            "UpdateItem",
            json!({
                "TableName": "test-table",
                "Key": {"pk": {"S": "absent"}},
                "UpdateExpression": "SET #c = #c + :one",
                "ExpressionAttributeNames": {"#c": "count"},
                "ExpressionAttributeValues": {":one": {"N": "1"}}
            }),
        ))
        .await
        .err()
        .expect("arithmetic on a missing operand must be rejected");
    assert!(!err.to_string().is_empty());

    let got = call_dynamodb(
        &svc,
        "GetItem",
        json!({"TableName": "test-table", "Key": {"pk": {"S": "absent"}}}),
    )
    .await;
    assert!(
        got.get("Item").is_none(),
        "a failed upsert left a key-only stub: {got}"
    );
    let scan = call_dynamodb(&svc, "Scan", json!({"TableName": "test-table"})).await;
    assert_eq!(
        scan["Items"].as_array().map(|r| r.len()),
        Some(0),
        "a failed upsert left a row behind: {scan}"
    );

    // The table still writes correctly afterwards: one row, not two.
    call_dynamodb(
        &svc,
        "PutItem",
        json!({"TableName": "test-table", "Item": {"pk": {"S": "absent"}, "v": {"S": "1"}}}),
    )
    .await;
    call_dynamodb(
        &svc,
        "PutItem",
        json!({"TableName": "test-table", "Item": {"pk": {"S": "absent"}, "v": {"S": "2"}}}),
    )
    .await;
    let scan = call_dynamodb(&svc, "Scan", json!({"TableName": "test-table"})).await;
    let rows = scan["Items"].as_array().unwrap();
    assert_eq!(rows.len(), 1, "the write path duplicated a row: {scan}");
    assert_eq!(rows[0]["v"], json!({"S": "2"}));
}

// ---------------------------------------------------------------------
// Vector indexes and SearchVectors
// ---------------------------------------------------------------------

/// An item attribute holding a vector: a DynamoDB `L` of `N`.
fn vec_attr(values: &[f64]) -> Value {
    json!({ "L": values.iter().map(|v| json!({ "N": v.to_string() })).collect::<Vec<Value>>() })
}

/// A request `SearchVector`: `SearchVectorList` is a bare list of
/// `AttributeValue`, not an `L`-wrapped attribute.
fn search_vec(values: &[f64]) -> Value {
    json!(values
        .iter()
        .map(|v| json!({ "N": v.to_string() }))
        .collect::<Vec<Value>>())
}

fn create_vector_table(svc: &DynamoDbService, distance_function: &str) {
    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "vec-table",
            "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
            "AttributeDefinitions": [
                { "AttributeName": "pk", "AttributeType": "S" },
                { "AttributeName": "tenant", "AttributeType": "S" },
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [
                {
                    "IndexName": "embedding-index",
                    "VectorAttribute": { "AttributeName": "embedding" },
                    "Dimensions": 2,
                    "DistanceFunction": distance_function,
                    "Projection": { "ProjectionType": "ALL" },
                },
                {
                    "IndexName": "tenant-index",
                    "VectorAttribute": { "AttributeName": "embedding" },
                    "Dimensions": 2,
                    "DistanceFunction": distance_function,
                    "SearchSchema": [{ "AttributeName": "tenant", "SearchSchemaElementType": "HASH" }],
                    "Projection": { "ProjectionType": "KEYS_ONLY" },
                },
            ],
        }),
    );
    svc.create_table(&req).unwrap();
}

fn put_vector_item(svc: &DynamoDbService, pk: &str, embedding: &[f64]) {
    let req = make_request(
        "PutItem",
        json!({
            "TableName": "vec-table",
            "Item": { "pk": { "S": pk }, "embedding": vec_attr(embedding) },
        }),
    );
    svc.put_item(&req).unwrap();
}

/// `AwsResponse` isn't `Debug`, so `unwrap_err` can't be used directly.
fn err_of(r: Result<AwsResponse, AwsServiceError>) -> AwsServiceError {
    match r {
        Ok(_) => panic!("expected an error"),
        Err(e) => e,
    }
}

fn err_message(err: AwsServiceError) -> String {
    match err {
        AwsServiceError::AwsError { message, .. } => message,
        other => panic!("expected AwsError, got {other:?}"),
    }
}

fn search(svc: &DynamoDbService, body: Value) -> Value {
    let resp = svc
        .search_vectors(&make_request("SearchVectors", body))
        .unwrap();
    serde_json::from_slice(resp.body.expect_bytes()).unwrap()
}

fn describe(svc: &DynamoDbService, table: &str) -> Value {
    let resp = svc
        .describe_table(&make_request(
            "DescribeTable",
            json!({ "TableName": table }),
        ))
        .unwrap();
    serde_json::from_slice::<Value>(resp.body.expect_bytes()).unwrap()["Table"].clone()
}

#[test]
fn create_table_stores_vector_indexes_and_describe_returns_them() {
    let svc = make_service();
    create_vector_table(&svc, "COSINE");

    let table = describe(&svc, "vec-table");
    let idx = &table["VectorIndexes"][0];
    assert_eq!(idx["IndexName"], "embedding-index");
    assert_eq!(idx["Dimensions"], 2);
    assert_eq!(idx["DistanceFunction"], "COSINE");
    assert_eq!(idx["VectorAttribute"]["AttributeName"], "embedding");
    // An index created with its table is ACTIVE with it, and never reports
    // Backfilling.
    assert_eq!(idx["IndexStatus"], "ACTIVE");
    assert!(idx.get("Backfilling").is_none());
    assert_eq!(idx["ItemCount"], 0);
    assert!(idx["IndexArn"]
        .as_str()
        .unwrap()
        .ends_with(":table/vec-table/index/embedding-index"));
    assert_eq!(
        table["VectorIndexes"][1]["SearchSchema"][0]["SearchSchemaElementType"],
        "HASH"
    );

    put_vector_item(&svc, "a", &[1.0, 0.0]);
    let table = describe(&svc, "vec-table");
    assert_eq!(table["VectorIndexes"][0]["ItemCount"], 1);
    // The item has no tenant, so it is not in the HASH-schema index.
    assert_eq!(table["VectorIndexes"][1]["ItemCount"], 0);

    // A table with no vector index omits the member rather than sending [].
    create_test_table(&svc);
    assert!(describe(&svc, "test-table").get("VectorIndexes").is_none());
}

/// Rewind an online-built index's creation clock by `ms`.
fn age_vector_index(svc: &DynamoDbService, table: &str, index: &str, ms: i64) {
    let mut accounts = svc.state.write();
    let state = accounts.regional_mut("123456789012", "us-east-1");
    let idx = state
        .tables
        .get_mut(table)
        .unwrap()
        .vector_indexes
        .iter_mut()
        .find(|v| v.index_name == index)
        .unwrap();
    idx.online_created_at =
        Some(idx.online_created_at.unwrap() - chrono::Duration::milliseconds(ms));
}

#[test]
fn update_table_builds_a_vector_index_online() {
    let svc = make_service();
    create_test_table(&svc);
    let create = |name: &str| {
        svc.update_table(&make_request(
            "UpdateTable",
            json!({
                "TableName": "test-table",
                "VectorIndexUpdates": [{ "Create": {
                    "IndexName": name,
                    "VectorAttribute": { "AttributeName": "vec" },
                    "Dimensions": 3,
                    "DistanceFunction": "EUCLIDEAN",
                    "Projection": { "ProjectionType": "ALL" },
                }}],
            }),
        ))
    };
    let delete = |name: &str| {
        svc.update_table(&make_request(
            "UpdateTable",
            json!({
                "TableName": "test-table",
                "VectorIndexUpdates": [{ "Delete": { "IndexName": name } }],
            }),
        ))
    };
    let search_added = || {
        svc.search_vectors(&make_request(
            "SearchVectors",
            json!({
                "TableName": "test-table",
                "IndexName": "added",
                "SearchVector": search_vec(&[1.0, 0.0, 0.0]),
                "TopK": 1,
            }),
        ))
    };
    create("added").unwrap();

    // Allocating: the table is UPDATING, the index CREATING with Backfilling
    // false, and neither a cancel nor a second index is taken.
    let table = describe(&svc, "test-table");
    assert_eq!(table["TableStatus"], "UPDATING");
    assert_eq!(table["VectorIndexes"][0]["IndexStatus"], "CREATING");
    assert_eq!(table["VectorIndexes"][0]["Backfilling"], false);
    assert!(err_message(err_of(delete("added"))).contains("resource allocation phase"));
    assert_eq!(err_of(create("second")).code(), "LimitExceededException");
    assert!(err_message(err_of(search_added()))
        .contains("The table does not have the specified index: added"));
    let err = err_of(svc.delete_table(&make_request(
        "DeleteTable",
        json!({ "TableName": "test-table" }),
    )));
    assert_eq!(err.code(), "ResourceInUseException");

    // Backfilling: the table is ACTIVE again while the index still builds.
    age_vector_index(
        &svc,
        "test-table",
        "added",
        crate::state::VECTOR_INDEX_ALLOCATION_MS,
    );
    let table = describe(&svc, "test-table");
    assert_eq!(table["TableStatus"], "ACTIVE");
    assert_eq!(table["VectorIndexes"][0]["IndexStatus"], "CREATING");
    assert_eq!(table["VectorIndexes"][0]["Backfilling"], true);
    assert!(err_message(err_of(search_added()))
        .contains("Cannot search backfilling vector index: added"));

    // Built: ACTIVE, Backfilling gone, searches served.
    age_vector_index(
        &svc,
        "test-table",
        "added",
        crate::state::VECTOR_INDEX_BACKFILL_MS,
    );
    let table = describe(&svc, "test-table");
    assert_eq!(table["VectorIndexes"][0]["IndexStatus"], "ACTIVE");
    assert!(table["VectorIndexes"][0].get("Backfilling").is_none());
    search_added().unwrap();

    // Two online actions in one request are refused; one is taken.
    let both = svc.update_table(&make_request(
        "UpdateTable",
        json!({
            "TableName": "test-table",
            "VectorIndexUpdates": [
                { "Delete": { "IndexName": "added" } },
                { "Delete": { "IndexName": "added" } },
            ],
        }),
    ));
    assert_eq!(err_of(both).code(), "LimitExceededException");
    delete("added").unwrap();
    assert!(describe(&svc, "test-table").get("VectorIndexes").is_none());
    assert_eq!(err_of(delete("added")).code(), "ResourceNotFoundException");
}

#[test]
fn search_vectors_scores_cosine_as_a_distance() {
    let svc = make_service();
    create_vector_table(&svc, "COSINE");
    put_vector_item(&svc, "near", &[1.0, 0.0]);
    put_vector_item(&svc, "diag", &[1.0, 1.0]);
    put_vector_item(&svc, "far", &[-1.0, 0.0]);

    let body = search(
        &svc,
        json!({
            "TableName": "vec-table",
            "IndexName": "embedding-index",
            "SearchVector": search_vec(&[1.0, 0.0]),
            "TopK": 2,
        }),
    );
    let results = body["SearchResults"].as_array().unwrap();
    assert_eq!(results.len(), 2, "TopK caps the result set");
    assert_eq!(results[0]["Item"]["pk"]["S"], "near");
    assert_eq!(results[1]["Item"]["pk"]["S"], "diag");
    // An identical direction is at distance 0; lower is closer.
    assert_eq!(results[0]["Score"].as_f64().unwrap(), 0.0);
    assert!(results[1]["Score"].as_f64().unwrap() > 0.0);
    // The vector itself is left out unless projected for.
    assert!(results[0]["Item"].get("embedding").is_none());
}

#[test]
fn search_vectors_ranks_euclidean_nearest_first() {
    let svc = make_service();
    create_vector_table(&svc, "EUCLIDEAN");
    put_vector_item(&svc, "close", &[0.0, 1.0]);
    put_vector_item(&svc, "distant", &[10.0, 10.0]);

    let body = search(
        &svc,
        json!({
            "TableName": "vec-table",
            "IndexName": "embedding-index",
            "SearchVector": search_vec(&[0.0, 0.0]),
            "TopK": 5,
        }),
    );
    let results = body["SearchResults"].as_array().unwrap();
    assert_eq!(results[0]["Item"]["pk"]["S"], "close");
    assert_eq!(results[0]["Score"].as_f64().unwrap(), 1.0);
    assert!(results[1]["Score"].as_f64().unwrap() > results[0]["Score"].as_f64().unwrap());
}

#[test]
fn search_vectors_scopes_a_hash_schema_index_to_one_partition() {
    let svc = make_service();
    create_vector_table(&svc, "COSINE");
    for (pk, tenant) in [("a", Some("t1")), ("b", Some("t2")), ("c", None)] {
        let mut item = json!({ "pk": { "S": pk }, "embedding": vec_attr(&[1.0, 0.0]) });
        if let Some(t) = tenant {
            item["tenant"] = json!({ "S": t });
        }
        svc.put_item(&make_request(
            "PutItem",
            json!({ "TableName": "vec-table", "Item": item }),
        ))
        .unwrap();
    }
    let base = json!({
        "TableName": "vec-table",
        "IndexName": "tenant-index",
        "SearchVector": search_vec(&[1.0, 0.0]),
        "TopK": 10,
    });
    let err = err_of(svc.search_vectors(&make_request("SearchVectors", base.clone())));
    assert!(err_message(err).contains("must be provided when SearchSchema has a HASH key"));

    let mut scoped = base.clone();
    scoped["SearchConditionExpression"] = json!("#t = :t");
    scoped["ExpressionAttributeNames"] = json!({ "#t": "tenant" });
    scoped["ExpressionAttributeValues"] = json!({ ":t": { "S": "t1" } });
    let results = search(&svc, scoped)["SearchResults"].clone();
    let pks: Vec<&str> = results
        .as_array()
        .unwrap()
        .iter()
        .map(|r| r["Item"]["pk"]["S"].as_str().unwrap())
        .collect();
    assert_eq!(pks, ["a"]);

    let mut ranged = base;
    ranged["SearchConditionExpression"] = json!("tenant > :t");
    ranged["ExpressionAttributeValues"] = json!({ ":t": { "S": "t1" } });
    let err = err_of(svc.search_vectors(&make_request("SearchVectors", ranged)));
    assert!(err_message(err).contains("Invalid comparator"));
}

#[test]
fn search_vectors_returns_the_index_f32_copy_when_projected() {
    let svc = make_service();
    create_vector_table(&svc, "COSINE");
    svc.put_item(&make_request(
        "PutItem",
        json!({
            "TableName": "vec-table",
            "Item": {
                "pk": { "S": "p" },
                "embedding": { "L": [{ "N": "16777217" }, { "N": "0.1" }] },
            },
        }),
    ))
    .unwrap();

    let body = search(
        &svc,
        json!({
            "TableName": "vec-table",
            "IndexName": "embedding-index",
            "SearchVector": search_vec(&[1.0, 0.0]),
            "TopK": 1,
            "ProjectionExpression": "pk, embedding",
            "ReturnConsumedCapacity": "TOTAL",
        }),
    );
    let item = &body["SearchResults"][0]["Item"];
    assert_eq!(item["embedding"]["L"][0]["N"], "16777216");
    assert_eq!(item["embedding"]["L"][1]["N"], "0.1");
    let cc = &body["ConsumedCapacity"];
    assert!(cc["VectorSearchRequestBytes"].as_f64().unwrap() > 0.0);
    assert!(cc.get("CapacityUnits").is_none());
}

/// An overlapping ProjectionExpression is rejected, as on Query and Scan,
/// rather than returning items with nothing projected.
#[test]
fn search_vectors_rejects_overlapping_projection() {
    let svc = make_service();
    create_vector_table(&svc, "COSINE");
    put_vector_item(&svc, "only", &[1.0, 0.0]);
    let err = err_of(svc.search_vectors(&make_request(
        "SearchVectors",
        json!({
            "TableName": "vec-table",
            "IndexName": "embedding-index",
            "SearchVector": search_vec(&[1.0, 0.0]),
            "TopK": 1,
            "ProjectionExpression": "a, a.b",
        }),
    )));
    assert_eq!(err.code(), "ValidationException");
    assert_eq!(
        err.message(),
        "Invalid ProjectionExpression: Two document paths overlap with each other; must remove \
         or rewrite one of these paths; path one: [a], path two: [a, b]"
    );
}

#[test]
fn search_vectors_validates_index_and_vector() {
    let svc = make_service();
    create_vector_table(&svc, "COSINE");
    let request = |index: &str, vector: Value, top_k: Value| {
        let mut body = json!({
            "TableName": "vec-table",
            "IndexName": index,
            "SearchVector": vector,
        });
        if !top_k.is_null() {
            body["TopK"] = top_k;
        }
        svc.search_vectors(&make_request("SearchVectors", body))
    };

    let msg = err_message(err_of(request(
        "no-such-index",
        search_vec(&[1.0, 0.0]),
        json!(1),
    )));
    assert_eq!(
        msg,
        "The table does not have the specified index: no-such-index"
    );
    let msg = err_message(err_of(request(
        "embedding-index",
        search_vec(&[1.0, 0.0, 0.0]),
        json!(1),
    )));
    assert_eq!(
        msg,
        "Input search vector dimension 3 does not match vector index dimension 2"
    );
    let msg = err_message(err_of(request(
        "embedding-index",
        json!([{ "L": search_vec(&[1.0, 0.0]) }]),
        json!(1),
    )));
    assert!(msg.starts_with("Search vector contains invalid values"));
    let msg = err_message(err_of(request(
        "embedding-index",
        search_vec(&[1.0, 0.0]),
        json!(101),
    )));
    assert!(msg.starts_with("Provided TopK value '101' is out of valid range"));
    for top_k in [json!(0), Value::Null] {
        let err = err_of(request("embedding-index", search_vec(&[1.0, 0.0]), top_k));
        assert!(err_message(err).starts_with("1 validation error detected"));
    }
}

#[test]
fn vector_writes_are_validated_and_charged_per_index() {
    let svc = make_service();
    create_vector_table(&svc, "COSINE");
    let put = |item: Value| {
        svc.put_item(&make_request(
            "PutItem",
            json!({ "TableName": "vec-table", "Item": item, "ReturnConsumedCapacity": "INDEXES" }),
        ))
    };
    let msg = err_message(err_of(put(json!({
        "pk": { "S": "x" }, "embedding": vec_attr(&[1.0, 0.0, 0.0]),
    }))));
    assert_eq!(
        msg,
        "One or more parameter values were invalid. Invalid size for parameter embedding, \
         Expected: 2, Actual: 3 IndexName: embedding-index"
    );
    let msg = err_message(err_of(put(json!({
        "pk": { "S": "x" }, "embedding": { "L": [{ "N": "1" }, { "S": "no" }] },
    }))));
    assert!(
        msg.contains("Invalid type for parameter embedding[1]"),
        "{msg}"
    );
    let msg = err_message(err_of(put(json!({
        "pk": { "S": "x" }, "tenant": { "S": "" }, "embedding": vec_attr(&[1.0, 0.0]),
    }))));
    assert!(
        msg.contains("IndexName: tenant-index, IndexKey: tenant"),
        "{msg}"
    );

    let item = json!({
        "pk": { "S": "x" }, "tenant": { "S": "t" }, "embedding": vec_attr(&[1.0, 0.0]),
    });
    let first: Value =
        serde_json::from_slice(put(item.clone()).unwrap().body.expect_bytes()).unwrap();
    let charged = &first["ConsumedCapacity"]["VectorIndexes"];
    assert_eq!(
        charged["embedding-index"]["VectorWriteRequestBytes"],
        1024.0
    );
    assert_eq!(charged["tenant-index"]["VectorWriteRequestBytes"], 1024.0);
    // Replication is delta-based: rewriting the same item charges nothing.
    let again: Value = serde_json::from_slice(put(item).unwrap().body.expect_bytes()).unwrap();
    assert!(again["ConsumedCapacity"].get("VectorIndexes").is_none());
}

#[test]
fn create_table_rejects_a_malformed_vector_index() {
    let svc = make_service();
    for bad in [
        json!({ "VectorAttribute": { "AttributeName": "v" }, "Dimensions": 2 }),
        json!({ "IndexName": "idx", "Dimensions": 2 }),
        json!({ "IndexName": "idx", "VectorAttribute": { "AttributeName": "v" }, "Dimensions": 0 }),
        json!({ "IndexName": "idx", "VectorAttribute": { "AttributeName": "v" }, "Dimensions": 2,
                "DistanceFunction": "MANHATTAN" }),
        json!({ "IndexName": "idx", "VectorAttribute": { "AttributeName": "v" }, "Dimensions": 4097,
                "DistanceFunction": "COSINE", "Projection": { "ProjectionType": "ALL" } }),
    ] {
        let err = err_of(svc.create_table(&make_request(
            "CreateTable",
            json!({
                "TableName": "bad-vec",
                "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
                "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
                "BillingMode": "PAY_PER_REQUEST",
                "VectorIndexes": [bad.clone()],
            }),
        )));
        assert_eq!(err.code(), "ValidationException", "{bad}");
    }
}

/// A paged Scan of a GSI resumes by the table key and survives deleting each
/// page's rows; its LastEvaluatedKey carries the index key attributes as well,
/// as on AWS.
#[tokio::test]
async fn index_scan_pages_survive_deletes_and_carry_index_keys() {
    let svc = make_service();
    svc.create_table(&make_request(
        "CreateTable",
        json!({
            "TableName": "gsi-table",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "g", "AttributeType": "S"}
            ],
            "GlobalSecondaryIndexes": [{
                "IndexName": "by-g",
                "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "ALL"}
            }],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    ))
    .unwrap();
    let mut indexed = Vec::new();
    for i in 0..15 {
        let mut item = json!({"pk": {"S": format!("r{i:02}")}});
        // Every third row is missing the index key, so it is not in the index.
        if i % 3 != 0 {
            item["g"] = json!({"S": format!("g{}", i % 4)});
            indexed.push(format!("r{i:02}"));
        }
        call_dynamodb(
            &svc,
            "PutItem",
            json!({"TableName": "gsi-table", "Item": item}),
        )
        .await;
    }

    let mut seen: Vec<String> = Vec::new();
    let mut page_sizes: Vec<usize> = Vec::new();
    let mut start: Option<Value> = None;
    loop {
        let mut body = json!({"TableName": "gsi-table", "IndexName": "by-g", "Limit": 3});
        if let Some(s) = &start {
            body["ExclusiveStartKey"] = s.clone();
        }
        let page = call_dynamodb(&svc, "Scan", body).await;
        page_sizes.push(page["Items"].as_array().unwrap().len());
        for item in page["Items"].as_array().unwrap() {
            let pk = item["pk"]["S"].as_str().unwrap().to_string();
            call_dynamodb(
                &svc,
                "DeleteItem",
                json!({"TableName": "gsi-table", "Key": {"pk": {"S": pk}}}),
            )
            .await;
            seen.push(pk);
        }
        match page.get("LastEvaluatedKey") {
            Some(lek) => {
                assert!(lek.get("pk").is_some() && lek.get("g").is_some(), "{lek}");
                start = Some(lek.clone());
            }
            None => break,
        }
    }
    // Rows without the index key are not examined, so every full page holds
    // Limit indexed rows: 10 of them page as 3, 3, 3, 1.
    assert_eq!(page_sizes, vec![3, 3, 3, 1]);
    seen.sort();
    assert_eq!(seen, indexed);
}

/// A key attribute cannot hold an empty String or Binary. DynamoDB rejects it
/// on writes and on key lookups alike.
#[tokio::test]
async fn empty_string_or_binary_key_values_are_rejected() {
    let svc = make_service();
    create_test_table(&svc);
    svc.create_table(&make_request(
        "CreateTable",
        json!({
            "TableName": "bin-table",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "B"}],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    ))
    .unwrap();

    let cases = [
        (
            "PutItem",
            json!({"TableName": "test-table", "Item": {"pk": {"S": ""}}}),
            "string",
        ),
        (
            "GetItem",
            json!({"TableName": "test-table", "Key": {"pk": {"S": ""}}}),
            "string",
        ),
        (
            "PutItem",
            json!({"TableName": "bin-table", "Item": {"pk": {"B": ""}}}),
            "binary",
        ),
        (
            "DeleteItem",
            json!({"TableName": "bin-table", "Key": {"pk": {"B": ""}}}),
            "binary",
        ),
    ];
    for (action, body, kind) in cases {
        let err = svc
            .handle(make_request(action, body.clone()))
            .await
            .err()
            .unwrap_or_else(|| panic!("{action} accepted an empty key: {body}"));
        assert_eq!(err.code(), "ValidationException");
        assert_eq!(
            err.to_string(),
            format!(
                "ValidationException: One or more parameter values are not valid. The \
                 AttributeValue for a key attribute cannot contain an empty {kind} value. Key: pk"
            )
        );
    }
    let scan = call_dynamodb(&svc, "Scan", json!({"TableName": "test-table"})).await;
    assert_eq!(scan["Count"], 0);
}

/// Query orders a binary sort key by its bytes, the same order Scan uses,
/// not by the base64 text.
#[tokio::test]
async fn query_orders_binary_sort_keys_by_bytes() {
    let svc = make_service();
    svc.create_table(&make_request(
        "CreateTable",
        json!({
            "TableName": "bin-sort",
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "B"}
            ],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    ))
    .unwrap();
    // 0xff, 0x00 0x01, 0x7f.
    for b in ["/w==", "AAE=", "fw=="] {
        call_dynamodb(
            &svc,
            "PutItem",
            json!({"TableName": "bin-sort", "Item": {"pk": {"S": "p"}, "sk": {"B": b}}}),
        )
        .await;
    }
    let order = |resp: &Value| -> Vec<String> {
        resp["Items"]
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["sk"]["B"].as_str().unwrap().to_string())
            .collect()
    };
    let query = call_dynamodb(
        &svc,
        "Query",
        json!({
            "TableName": "bin-sort",
            "KeyConditionExpression": "pk = :p",
            "ExpressionAttributeValues": {":p": {"S": "p"}}
        }),
    )
    .await;
    assert_eq!(order(&query), ["AAE=", "fw==", "/w=="]);
    let scan = call_dynamodb(&svc, "Scan", json!({"TableName": "bin-sort"})).await;
    assert_eq!(order(&scan), order(&query));
}

const TEST_TABLE_ARN: &str = "arn:aws:dynamodb:us-east-1:123456789012:table/test-table";

/// Every operation that takes a TableName accepts the table's ARN. These
/// paths looked the table up by the raw string, so an ARN was "not found" or,
/// worse, silently skipped a step.
#[tokio::test]
async fn batch_operations_accept_a_table_arn() {
    let svc = make_service();
    create_test_table(&svc);

    call_dynamodb(
        &svc,
        "BatchWriteItem",
        json!({"RequestItems": {TEST_TABLE_ARN: [
            {"PutRequest": {"Item": {"pk": {"S": "a"}}}},
            {"PutRequest": {"Item": {"pk": {"S": "b"}}}}
        ]}}),
    )
    .await;
    let got = call_dynamodb(
        &svc,
        "BatchGetItem",
        json!({"RequestItems": {TEST_TABLE_ARN: {"Keys": [{"pk": {"S": "a"}}, {"pk": {"S": "b"}}]}}}),
    )
    .await;
    assert_eq!(
        got["Responses"][TEST_TABLE_ARN].as_array().unwrap().len(),
        2,
        "{got}"
    );

    call_dynamodb(
        &svc,
        "BatchWriteItem",
        json!({"RequestItems": {TEST_TABLE_ARN: [
            {"DeleteRequest": {"Key": {"pk": {"S": "a"}}}}
        ]}}),
    )
    .await;
    let scan = call_dynamodb(&svc, "Scan", json!({"TableName": "test-table"})).await;
    assert_eq!(scan["Items"], json!([{"pk": {"S": "b"}}]));
}

/// A transaction naming its table by ARN must still be all or nothing: the
/// snapshot it reverts from was taken under the raw ARN, which matched no
/// table, so a failing transaction kept the writes before the failure.
#[tokio::test]
async fn failed_transaction_naming_the_table_by_arn_reverts() {
    let svc = make_service();
    create_test_table(&svc);
    call_dynamodb(
        &svc,
        "PutItem",
        json!({"TableName": "test-table", "Item": {"pk": {"S": "kept"}, "n": {"S": "text"}}}),
    )
    .await;

    let resp = svc
        .handle(make_request(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": TEST_TABLE_ARN, "Item": {"pk": {"S": "new"}}}},
                {"Update": {
                    "TableName": TEST_TABLE_ARN,
                    "Key": {"pk": {"S": "kept"}},
                    "UpdateExpression": "SET n = n + :one",
                    "ExpressionAttributeValues": {":one": {"N": "1"}}
                }}
            ]}),
        ))
        .await;
    let failed = match resp {
        Ok(r) => r.status != StatusCode::OK,
        Err(_) => true,
    };
    assert!(
        failed,
        "the arithmetic on a string must fail the transaction"
    );

    let scan = call_dynamodb(&svc, "Scan", json!({"TableName": "test-table"})).await;
    assert_eq!(
        scan["Items"],
        json!([{"pk": {"S": "kept"}, "n": {"S": "text"}}]),
        "the transaction's Put survived its failure"
    );
}

/// UpdateTable and DeleteTable (including its deletion-protection check)
/// accept the ARN, and so does contributor-insights accounting on reads.
#[tokio::test]
async fn table_operations_and_insights_accept_a_table_arn() {
    let svc = make_service();
    create_test_table(&svc);
    call_dynamodb(
        &svc,
        "PutItem",
        json!({"TableName": "test-table", "Item": {"pk": {"S": "a"}}}),
    )
    .await;

    call_dynamodb(
        &svc,
        "UpdateContributorInsights",
        json!({"TableName": "test-table", "ContributorInsightsAction": "ENABLE"}),
    )
    .await;
    call_dynamodb(
        &svc,
        "GetItem",
        json!({"TableName": TEST_TABLE_ARN, "Key": {"pk": {"S": "a"}}}),
    )
    .await;
    call_dynamodb(&svc, "Scan", json!({"TableName": TEST_TABLE_ARN})).await;
    call_dynamodb(
        &svc,
        "Query",
        json!({
            "TableName": TEST_TABLE_ARN,
            "KeyConditionExpression": "pk = :p",
            "ExpressionAttributeValues": {":p": {"S": "a"}}
        }),
    )
    .await;
    {
        let accounts = svc.state.read();
        let table = &accounts
            .regional("123456789012", "us-east-1")
            .unwrap()
            .tables["test-table"];
        assert_eq!(
            table.contributor_insights_counters.values().sum::<u64>(),
            3,
            "reads by ARN were not counted"
        );
    }

    call_dynamodb(
        &svc,
        "UpdateTable",
        json!({"TableName": TEST_TABLE_ARN, "DeletionProtectionEnabled": true}),
    )
    .await;
    let err = svc
        .handle(make_request(
            "DeleteTable",
            json!({"TableName": TEST_TABLE_ARN}),
        ))
        .await
        .err()
        .expect("deletion protection must hold when the table is named by ARN");
    assert_eq!(err.code(), "ValidationException");

    call_dynamodb(
        &svc,
        "UpdateTable",
        json!({"TableName": TEST_TABLE_ARN, "DeletionProtectionEnabled": false}),
    )
    .await;
    call_dynamodb(&svc, "DeleteTable", json!({"TableName": TEST_TABLE_ARN})).await;
    assert!(svc
        .state
        .read()
        .regional("123456789012", "us-east-1")
        .unwrap()
        .tables
        .is_empty());
}

/// Naming one table by name in one operation and by ARN in another is still
/// the same item: transactions must reject touching it twice.
#[tokio::test]
async fn transactions_see_a_table_named_by_name_and_arn_as_one_table() {
    let svc = make_service();
    create_test_table(&svc);
    call_dynamodb(
        &svc,
        "PutItem",
        json!({"TableName": "test-table", "Item": {"pk": {"S": "a"}}}),
    )
    .await;

    let err = svc
        .handle(make_request(
            "TransactWriteItems",
            json!({"TransactItems": [
                {"Put": {"TableName": "test-table", "Item": {"pk": {"S": "a"}, "v": {"S": "1"}}}},
                {"Put": {"TableName": TEST_TABLE_ARN, "Item": {"pk": {"S": "a"}, "v": {"S": "2"}}}}
            ]}),
        ))
        .await
        .err()
        .expect("two writes to one item must be rejected");
    assert!(
        err.to_string().contains("multiple operations on one item"),
        "{err}"
    );

    let err = svc
        .handle(make_request(
            "TransactGetItems",
            json!({"TransactItems": [
                {"Get": {"TableName": "test-table", "Key": {"pk": {"S": "a"}}}},
                {"Get": {"TableName": TEST_TABLE_ARN, "Key": {"pk": {"S": "a"}}}}
            ]}),
        ))
        .await
        .err()
        .expect("two reads of one item must be rejected");
    assert!(
        err.to_string().contains("multiple operations on one item"),
        "{err}"
    );
}

/// A backup of a table named by ARN records the table's name, and the list
/// filters accept either form.
#[tokio::test]
async fn backups_and_insights_listings_accept_a_table_arn() {
    let svc = make_service();
    create_test_table(&svc);

    let created = call_dynamodb(
        &svc,
        "CreateBackup",
        json!({"TableName": TEST_TABLE_ARN, "BackupName": "b1"}),
    )
    .await;
    let details = &created["BackupDetails"];
    let arn = details["BackupArn"].as_str().unwrap();
    assert!(
        arn.starts_with("arn:aws:dynamodb:us-east-1:123456789012:table/test-table/backup/"),
        "{arn}"
    );

    for filter in ["test-table", TEST_TABLE_ARN] {
        let listed = call_dynamodb(&svc, "ListBackups", json!({"TableName": filter})).await;
        let summaries = listed["BackupSummaries"].as_array().unwrap();
        assert_eq!(summaries.len(), 1, "filter {filter}: {listed}");
        assert_eq!(summaries[0]["TableName"], "test-table");
    }

    let listed = call_dynamodb(
        &svc,
        "ListContributorInsights",
        json!({"TableName": TEST_TABLE_ARN}),
    )
    .await;
    assert_eq!(
        listed["ContributorInsightsSummaries"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "{listed}"
    );
}

fn create_streamed_gsi_table(svc: &DynamoDbService, name: &str) -> Value {
    let resp = svc
        .create_table(&make_request(
            "CreateTable",
            json!({
                "TableName": name,
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [
                    {"AttributeName": "pk", "AttributeType": "S"},
                    {"AttributeName": "g", "AttributeType": "S"}
                ],
                "GlobalSecondaryIndexes": [{
                    "IndexName": "by-g",
                    "KeySchema": [{"AttributeName": "g", "KeyType": "HASH"}],
                    "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["shown"]}
                }],
                "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"},
                "BillingMode": "PAY_PER_REQUEST"
            }),
        ))
        .unwrap();
    serde_json::from_slice::<Value>(resp.body.expect_bytes()).unwrap()["TableDescription"].clone()
}

async fn err_code(svc: &DynamoDbService, action: &str, body: Value) -> Option<String> {
    svc.handle(make_request(action, body))
        .await
        .err()
        .map(|e| e.code().to_string())
}

const POLICY: &str = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::123456789012:root"},"Action":"dynamodb:GetItem","Resource":"*"}]}"#;

/// PutResourcePolicy / DeleteResourcePolicy honor `ExpectedRevisionId`
/// (`NO_POLICY` meaning "only if none is attached"), a delete reports the
/// revision it removed and is idempotent without one, and a stream carries a
/// policy of its own.
#[tokio::test]
async fn resource_policy_revisions_and_stream_policies() {
    let svc = make_service();
    let desc = create_streamed_gsi_table(&svc, "Orders");
    let table_arn = desc["TableArn"].as_str().unwrap().to_string();
    let stream_arn = desc["LatestStreamArn"].as_str().unwrap().to_string();

    let put = call_dynamodb(
        &svc,
        "PutResourcePolicy",
        json!({"ResourceArn": table_arn, "Policy": POLICY, "ExpectedRevisionId": "NO_POLICY"}),
    )
    .await;
    let revision = put["RevisionId"].as_str().unwrap().to_string();
    // Idempotent: the same document keeps its revision.
    let again = call_dynamodb(
        &svc,
        "PutResourcePolicy",
        json!({"ResourceArn": table_arn, "Policy": POLICY, "ExpectedRevisionId": revision}),
    )
    .await;
    assert_eq!(again["RevisionId"], put["RevisionId"]);
    assert_eq!(
        err_code(
            &svc,
            "PutResourcePolicy",
            json!({"ResourceArn": table_arn, "Policy": POLICY, "ExpectedRevisionId": "NO_POLICY"})
        )
        .await
        .as_deref(),
        Some("PolicyNotFoundException"),
        "NO_POLICY with a policy attached"
    );
    assert_eq!(
        err_code(
            &svc,
            "DeleteResourcePolicy",
            json!({"ResourceArn": table_arn, "ExpectedRevisionId": "stale"})
        )
        .await
        .as_deref(),
        Some("PolicyNotFoundException")
    );

    // The stream's policy is separate from the table's.
    let stream_policy = POLICY.replace("GetItem", "DescribeStream");
    call_dynamodb(
        &svc,
        "PutResourcePolicy",
        json!({"ResourceArn": stream_arn, "Policy": stream_policy}),
    )
    .await;
    let got = call_dynamodb(
        &svc,
        "GetResourcePolicy",
        json!({"ResourceArn": stream_arn}),
    )
    .await;
    assert_eq!(got["Policy"], json!(stream_policy));
    let got = call_dynamodb(&svc, "GetResourcePolicy", json!({"ResourceArn": table_arn})).await;
    assert_eq!(got["Policy"], json!(POLICY));

    let deleted = call_dynamodb(
        &svc,
        "DeleteResourcePolicy",
        json!({"ResourceArn": table_arn, "ExpectedRevisionId": revision}),
    )
    .await;
    assert_eq!(deleted["RevisionId"], json!(revision));
    let deleted = call_dynamodb(
        &svc,
        "DeleteResourcePolicy",
        json!({"ResourceArn": table_arn}),
    )
    .await;
    assert_eq!(deleted, json!({}), "an unconditional delete is idempotent");

    // Only tables and their current streams carry policies.
    for arn in [
        format!("{table_arn}/index/by-g"),
        format!("{table_arn}/stream/2000-01-01T00:00:00.000"),
    ] {
        assert_eq!(
            err_code(
                &svc,
                "PutResourcePolicy",
                json!({"ResourceArn": arn, "Policy": POLICY})
            )
            .await
            .as_deref(),
            Some("ResourceNotFoundException"),
            "{arn}"
        );
    }
    // Not JSON, and over 20 KB, are rejected.
    for policy in [
        "not json".to_string(),
        format!("{{\"a\":\"{}\"}}", "x".repeat(21 * 1024)),
    ] {
        assert_eq!(
            err_code(
                &svc,
                "PutResourcePolicy",
                json!({"ResourceArn": table_arn, "Policy": policy})
            )
            .await
            .as_deref(),
            Some("ValidationException")
        );
    }

    // Deleting the table drops its streams' policies.
    call_dynamodb(&svc, "DeleteTable", json!({"TableName": "Orders"})).await;
    assert!(svc
        .state
        .read()
        .regional("123456789012", "us-east-1")
        .unwrap()
        .stream_policies
        .is_empty());
}

/// A policy given to CreateTable is attached to the new table.
#[tokio::test]
async fn create_table_attaches_its_resource_policy() {
    let svc = make_service();
    svc.create_table(&make_request(
        "CreateTable",
        json!({
            "TableName": "WithPolicy",
            "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
            "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
            "BillingMode": "PAY_PER_REQUEST",
            "ResourcePolicy": POLICY
        }),
    ))
    .unwrap();
    let arn = svc
        .state
        .read()
        .regional("123456789012", "us-east-1")
        .unwrap()
        .tables["WithPolicy"]
        .arn
        .clone();
    let got = call_dynamodb(&svc, "GetResourcePolicy", json!({"ResourceArn": arn})).await;
    assert_eq!(got["Policy"], json!(POLICY));
}

/// `SELECT ... FROM "table"."index"` reads the index: only rows carrying its
/// key, with only its projected attributes, and the WHERE clause applies. It
/// used to read the whole base table and skip the WHERE clause.
#[tokio::test]
async fn partiql_select_from_an_index_reads_the_index() {
    let svc = make_service();
    create_streamed_gsi_table(&svc, "Orders");
    for item in [
        json!({"pk": {"S": "a"}, "g": {"S": "x"}, "shown": {"S": "1"}, "hidden": {"S": "h"}}),
        json!({"pk": {"S": "b"}, "g": {"S": "y"}, "shown": {"S": "2"}}),
        json!({"pk": {"S": "c"}, "hidden": {"S": "no index key"}}),
    ] {
        call_dynamodb(
            &svc,
            "PutItem",
            json!({"TableName": "Orders", "Item": item}),
        )
        .await;
    }

    let all = call_dynamodb(
        &svc,
        "ExecuteStatement",
        json!({"Statement": "SELECT * FROM \"Orders\".\"by-g\""}),
    )
    .await;
    let mut pks: Vec<&str> = all["Items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|i| i["pk"]["S"].as_str().unwrap())
        .collect();
    pks.sort();
    assert_eq!(pks, ["a", "b"], "only rows carrying the index key");
    for item in all["Items"].as_array().unwrap() {
        assert!(
            item.get("hidden").is_none(),
            "unprojected attribute: {item}"
        );
    }

    let filtered = call_dynamodb(
        &svc,
        "ExecuteStatement",
        json!({"Statement": "SELECT * FROM \"Orders\".\"by-g\" WHERE g = 'x'"}),
    )
    .await;
    assert_eq!(
        filtered["Items"],
        json!([{"pk": {"S": "a"}, "g": {"S": "x"}, "shown": {"S": "1"}}])
    );

    let err = svc
        .handle(make_request(
            "ExecuteStatement",
            json!({"Statement": "SELECT * FROM \"Orders\".\"nope\""}),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), "ValidationException");
}

/// A PartiQL SELECT returns only the columns it names (document paths and
/// quoted names included), each under the name of its last path step; `*`
/// returns the whole item.
#[tokio::test]
async fn partiql_select_returns_only_the_named_columns() {
    let svc = make_service();
    create_test_table(&svc);
    call_dynamodb(
        &svc,
        "PutItem",
        json!({"TableName": "test-table", "Item": {
            "pk": {"S": "a"},
            "public": {"S": "p"},
            "secret": {"S": "s"},
            "a.b": {"S": "dotted"},
            "addr": {"M": {"city": {"S": "c"}, "zip": {"S": "z"}}}
        }}),
    )
    .await;
    let got = call_dynamodb(
        &svc,
        "ExecuteStatement",
        json!({"Statement": "SELECT pk, public, \"a.b\", addr.city FROM \"test-table\" WHERE pk = 'a'"}),
    )
    .await;
    assert_eq!(
        got["Items"],
        json!([{
            "pk": {"S": "a"},
            "public": {"S": "p"},
            "a.b": {"S": "dotted"},
            "city": {"S": "c"}
        }])
    );
    let all = call_dynamodb(
        &svc,
        "ExecuteStatement",
        json!({"Statement": "SELECT * FROM \"test-table\" WHERE pk = 'a'"}),
    )
    .await;
    assert_eq!(all["Items"][0]["secret"], json!({"S": "s"}));
}

/// Column projection happens after paging, so the cursor still has each
/// row's full key: a column list without the key attributes pages through
/// every row once. An empty or unreadable column list is rejected instead of
/// returning more than was asked for.
#[tokio::test]
async fn partiql_select_columns_page_and_validate() {
    let svc = make_service();
    svc.create_table(&make_request(
        "CreateTable",
        json!({
            "TableName": "Scores",
            "KeySchema": [
                {"AttributeName": "u", "KeyType": "HASH"},
                {"AttributeName": "g", "KeyType": "RANGE"}
            ],
            "AttributeDefinitions": [
                {"AttributeName": "u", "AttributeType": "S"},
                {"AttributeName": "g", "AttributeType": "S"}
            ],
            "BillingMode": "PAY_PER_REQUEST"
        }),
    ))
    .unwrap();
    for g in ["a", "b", "c"] {
        call_dynamodb(
            &svc,
            "PutItem",
            json!({"TableName": "Scores", "Item": {"u": {"S": "u1"}, "g": {"S": g}, "score": {"N": g.len().to_string()}}}),
        )
        .await;
    }
    let mut seen = 0;
    let mut token: Option<String> = None;
    loop {
        let mut body = json!({"Statement": "SELECT score FROM \"Scores\"", "Limit": 1});
        if let Some(t) = &token {
            body["NextToken"] = json!(t);
        }
        let page = call_dynamodb(&svc, "ExecuteStatement", body).await;
        for item in page["Items"].as_array().unwrap() {
            assert!(item.get("u").is_none() && item.get("g").is_none(), "{item}");
            seen += 1;
        }
        token = page["NextToken"].as_str().map(str::to_string);
        if token.is_none() {
            break;
        }
    }
    assert_eq!(seen, 3, "every row once");

    for statement in [
        "SELECT FROM \"Scores\"",
        "SELECT tags[-1] FROM \"Scores\"",
        "SELECT \"addr ,x FROM \"Scores\"",
    ] {
        assert_eq!(
            err_code(&svc, "ExecuteStatement", json!({"Statement": statement}))
                .await
                .as_deref(),
            Some("ValidationException"),
            "{statement}"
        );
    }
    // Whitespace around path separators is fine.
    call_dynamodb(
        &svc,
        "ExecuteStatement",
        json!({"Statement": "SELECT \"u\" . \"x\", score [ 0 ] FROM \"Scores\""}),
    )
    .await;
}

/// A one-value IN list on the partition key is a point read like an
/// equality, and finds the item.
#[tokio::test]
async fn partiql_point_read_through_a_one_value_in_list() {
    let svc = make_service();
    svc.create_table(&make_request(
        "CreateTable",
        json!({
            "TableName": "Pairs",
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
    ))
    .unwrap();
    create_test_table(&svc);
    call_dynamodb(
        &svc,
        "PutItem",
        json!({"TableName": "Pairs", "Item": {"pk": {"S": "a"}, "sk": {"S": "x"}}}),
    )
    .await;
    call_dynamodb(
        &svc,
        "PutItem",
        json!({"TableName": "test-table", "Item": {"pk": {"S": "a"}}}),
    )
    .await;
    for statement in [
        "SELECT * FROM \"test-table\" WHERE pk IN ['a']",
        "SELECT * FROM \"Pairs\" WHERE pk IN ['a'] AND sk = 'x'",
    ] {
        let got = call_dynamodb(&svc, "ExecuteStatement", json!({"Statement": statement})).await;
        assert_eq!(got["Items"].as_array().unwrap().len(), 1, "{statement}");
    }
}

/// A written value is built only from constants, paths and the update
/// functions: anything else is refused rather than written as some default.
#[tokio::test]
async fn partiql_refuses_values_it_cannot_build() {
    let svc = make_service();
    create_test_table(&svc);
    call_dynamodb(
        &svc,
        "PutItem",
        json!({"TableName": "test-table", "Item": {"pk": {"S": "a"}, "s": {"SS": ["x"]}}}),
    )
    .await;
    for statement in [
        "UPDATE \"test-table\" SET v = lower(s) WHERE pk = 'a'",
        "UPDATE \"test-table\" SET v = set_add(s, <<'y'>>) WHERE pk = 'a'",
        "UPDATE \"test-table\" SET s = set_add(s) WHERE pk = 'a'",
        "UPDATE \"test-table\" SET v = [missing] WHERE pk = 'a'",
        "UPDATE \"test-table\" SET v = (1 = 1) WHERE pk = 'a'",
        "SELECT * FROM \"test-table\" WHERE upper(pk) = 'A'",
        "INSERT INTO \"test-table\" VALUE {'pk': 'b', 's': <<'x', 1>>}",
        "INSERT INTO \"test-table\" VALUE {'pk': 'b', 'pk': 'c'}",
    ] {
        assert_eq!(
            err_code(&svc, "ExecuteStatement", json!({"Statement": statement}))
                .await
                .as_deref(),
            Some("ValidationException"),
            "{statement}"
        );
    }
    let got = call_dynamodb(
        &svc,
        "GetItem",
        json!({"TableName": "test-table", "Key": {"pk": {"S": "a"}}}),
    )
    .await;
    assert_eq!(got["Item"], json!({"pk": {"S": "a"}, "s": {"SS": ["x"]}}));
    assert_eq!(
        call_dynamodb(&svc, "Scan", json!({"TableName": "test-table"})).await["Count"],
        1
    );
}

/// PartiQL writes are measured and stored like the item APIs: INSERT has
/// PutItem's size rule and wording, UPDATE the flat update rule, and every
/// written number -- literal or bound parameter -- is stored canonically.
#[tokio::test]
async fn partiql_writes_share_the_item_size_and_number_rules() {
    let svc = make_service();
    create_test_table(&svc);
    // "pk" + "a" + "p" + padding: exactly one byte over the limit.
    let pad = "x".repeat(409_600 - "pk".len() - "a".len() - "p".len() + 1);
    let err = svc
        .handle(make_request(
            "ExecuteStatement",
            json!({
                "Statement": "INSERT INTO \"test-table\" VALUE {'pk': 'a', 'p': ?}",
                "Parameters": [{"S": pad}]
            }),
        ))
        .await
        .err()
        .expect("oversize INSERT");
    assert_eq!(err.code(), "ValidationException");
    assert_eq!(
        err.message(),
        "Item size has exceeded the maximum allowed size"
    );
    // One byte less fits.
    call_dynamodb(
        &svc,
        "ExecuteStatement",
        json!({
            "Statement": "INSERT INTO \"test-table\" VALUE {'pk': 'a', 'p': ?}",
            "Parameters": [{"S": &pad[1..]}]
        }),
    )
    .await;
    let err = svc
        .handle(make_request(
            "ExecuteStatement",
            json!({
                "Statement": "UPDATE \"test-table\" SET q = 'y' WHERE pk = 'a'"
            }),
        ))
        .await
        .err()
        .expect("oversize UPDATE");
    assert_eq!(
        err.message(),
        "Item size to update has exceeded the maximum allowed size"
    );

    call_dynamodb(
        &svc,
        "ExecuteStatement",
        json!({
            "Statement": "INSERT INTO \"test-table\" VALUE {'pk': 'n', 'lit': 01.50, 'p': ?, 'l': [?]}",
            "Parameters": [{"N": "+1.5E+3"}, {"N": "0042.1200"}]
        }),
    )
    .await;
    call_dynamodb(
        &svc,
        "ExecuteStatement",
        json!({
            "Statement": "UPDATE \"test-table\" SET u = ? WHERE pk = 'n'",
            "Parameters": [{"NS": ["1.0", "-0"]}]
        }),
    )
    .await;
    let got = call_dynamodb(
        &svc,
        "GetItem",
        json!({"TableName": "test-table", "Key": {"pk": {"S": "n"}}}),
    )
    .await;
    assert_eq!(got["Item"]["lit"], json!({"N": "1.5"}));
    assert_eq!(got["Item"]["p"], json!({"N": "1500"}));
    assert_eq!(got["Item"]["l"], json!({"L": [{"N": "42.12"}]}));
    assert_eq!(got["Item"]["u"], json!({"NS": ["1", "0"]}));
}

/// A value an UPDATE writes is validated like an UpdateItem value before it
/// is normalized, so it can never store a malformed number or set.
#[tokio::test]
async fn partiql_update_validates_written_values() {
    let svc = make_service();
    create_test_table(&svc);
    call_dynamodb(
        &svc,
        "PutItem",
        json!({"TableName": "test-table", "Item": {"pk": {"S": "a"}}}),
    )
    .await;
    for (value, message) in [
        (
            json!({"NS": ["1", "1.0"]}),
            "One or more parameter values were invalid: Input collection [1, 1.0] contains duplicates",
        ),
        (
            json!({"N": "abc"}),
            "The parameter cannot be converted to a numeric value: abc",
        ),
        (
            json!({"SS": []}),
            "One or more parameter values were invalid: An string set  may not be empty",
        ),
        (
            json!({"N": "1234567890123456789012345678901234567890"}),
            "",
        ),
    ] {
        let expected = svc
            .handle(make_request(
                "UpdateItem",
                json!({
                    "TableName": "test-table",
                    "Key": {"pk": {"S": "a"}},
                    "UpdateExpression": "SET u = :v",
                    "ExpressionAttributeValues": {":v": value}
                }),
            ))
            .await
            .err()
            .expect("UpdateItem refuses the value");
        let err = svc
            .handle(make_request(
                "ExecuteStatement",
                json!({
                    "Statement": "UPDATE \"test-table\" SET u = ? WHERE pk = 'a'",
                    "Parameters": [value]
                }),
            ))
            .await
            .err()
            .expect("PartiQL UPDATE refuses the value");
        assert_eq!(err.code(), "ValidationException", "{value}");
        assert_eq!(err.message(), expected.message(), "{value}");
        if !message.is_empty() {
            assert_eq!(err.message(), message, "{value}");
        }
    }
    let got = call_dynamodb(
        &svc,
        "GetItem",
        json!({"TableName": "test-table", "Key": {"pk": {"S": "a"}}}),
    )
    .await;
    assert_eq!(got["Item"], json!({"pk": {"S": "a"}}));
}

/// A transaction that reads and writes one table reports each as what it
/// was, not every unit as whichever came first.
#[tokio::test]
async fn partiql_transaction_capacity_splits_reads_from_writes() {
    let svc = make_service();
    create_test_table(&svc);
    call_dynamodb(
        &svc,
        "PutItem",
        json!({"TableName": "test-table", "Item": {"pk": {"S": "a"}}}),
    )
    .await;
    let got = call_dynamodb(
        &svc,
        "ExecuteTransaction",
        json!({
            "TransactStatements": [
                {"Statement": "EXISTS(SELECT * FROM \"test-table\" WHERE pk = 'a')"},
                {"Statement": "UPDATE \"test-table\" SET v = 1 WHERE pk = 'a'"}
            ],
            "ReturnConsumedCapacity": "TOTAL"
        }),
    )
    .await;
    let cc = &got["ConsumedCapacity"][0];
    assert_eq!(cc["ReadCapacityUnits"], json!(1.0), "{cc}");
    assert_eq!(cc["WriteCapacityUnits"], json!(2.0), "{cc}");
    assert_eq!(cc["CapacityUnits"], json!(3.0), "{cc}");
}

// ── Cross-account table ARNs ───────────────────────────────────────────

const OWNER: &str = "444455556666";
const OWNER_ARN: &str = "arn:aws:dynamodb:us-east-1:444455556666:table/Shared";

async fn call_as(
    svc: &DynamoDbService,
    account: &str,
    action: &str,
    body: Value,
) -> (StatusCode, Value) {
    let mut req = make_request(action, body);
    req.account_id = account.to_string();
    match svc.handle(req).await {
        Ok(resp) => (
            resp.status,
            serde_json::from_slice(resp.body.expect_bytes()).unwrap_or(Value::Null),
        ),
        Err(err) => (err.status(), json!({ "__type": err.code() })),
    }
}

/// A table named `Shared` in both the caller's account and [`OWNER`], each
/// holding one item marking whose it is.
async fn two_account_tables() -> DynamoDbService {
    let svc = make_service();
    for account in ["123456789012", OWNER] {
        let (status, body) = call_as(
            &svc,
            account,
            "CreateTable",
            json!({
                "TableName": "Shared",
                "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
                "AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
                "BillingMode": "PAY_PER_REQUEST",
                "StreamSpecification": {"StreamEnabled": true, "StreamViewType": "NEW_IMAGE"}
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let (status, body) = call_as(
            &svc,
            account,
            "PutItem",
            json!({"TableName": "Shared", "Item": {"pk": {"S": "owner"}, "who": {"S": account}}}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
    }
    svc
}

fn item_count(svc: &DynamoDbService, account: &str) -> usize {
    svc.state
        .read()
        .regional(account, "us-east-1")
        .unwrap()
        .tables["Shared"]
        .items
        .len()
}

#[tokio::test]
async fn cross_account_item_operations_act_on_the_owners_table() {
    let svc = two_account_tables().await;
    let (status, body) = call_as(
        &svc,
        "123456789012",
        "GetItem",
        json!({"TableName": OWNER_ARN, "Key": {"pk": {"S": "owner"}}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["Item"]["who"]["S"], OWNER);

    let (status, _) = call_as(
        &svc,
        "123456789012",
        "PutItem",
        json!({"TableName": OWNER_ARN, "Item": {"pk": {"S": "written"}}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(item_count(&svc, OWNER), 2);
    assert_eq!(item_count(&svc, "123456789012"), 1);

    let (_, body) = call_as(
        &svc,
        "123456789012",
        "DescribeTable",
        json!({"TableName": OWNER_ARN}),
    )
    .await;
    assert_eq!(body["Table"]["TableArn"], OWNER_ARN);

    let (_, body) = call_as(
        &svc,
        "123456789012",
        "Scan",
        json!({"TableName": OWNER_ARN}),
    )
    .await;
    assert_eq!(body["Count"], 2);

    let (status, _) = call_as(
        &svc,
        "123456789012",
        "TagResource",
        json!({"ResourceArn": OWNER_ARN, "Tags": [{"Key": "team", "Value": "blue"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        svc.state
            .read()
            .regional(OWNER, "us-east-1")
            .unwrap()
            .tables["Shared"]
            .tags["team"],
        "blue"
    );
}

#[tokio::test]
async fn cross_account_batches_resolve_each_tables_account() {
    let svc = two_account_tables().await;
    // The same key in two accounts' tables is two different items.
    let (status, body) = call_as(
        &svc,
        "123456789012",
        "BatchWriteItem",
        json!({"RequestItems": {
            "Shared": [{"PutRequest": {"Item": {"pk": {"S": "k"}, "v": {"S": "mine"}}}}],
            OWNER_ARN: [{"PutRequest": {"Item": {"pk": {"S": "k"}, "v": {"S": "theirs"}}}}]
        }}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");

    let (status, body) = call_as(
        &svc,
        "123456789012",
        "BatchGetItem",
        json!({"RequestItems": {
            "Shared": {"Keys": [{"pk": {"S": "k"}}]},
            OWNER_ARN: {"Keys": [{"pk": {"S": "k"}}]}
        }}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["Responses"]["Shared"][0]["v"]["S"], "mine");
    assert_eq!(body["Responses"][OWNER_ARN][0]["v"]["S"], "theirs");

    let (status, body) = call_as(
        &svc,
        "123456789012",
        "TransactGetItems",
        json!({"TransactItems": [
            {"Get": {"TableName": "Shared", "Key": {"pk": {"S": "k"}}}},
            {"Get": {"TableName": OWNER_ARN, "Key": {"pk": {"S": "k"}}}}
        ]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["Responses"][0]["Item"]["v"]["S"], "mine");
    assert_eq!(body["Responses"][1]["Item"]["v"]["S"], "theirs");
}

#[tokio::test]
async fn cross_account_transactions_are_atomic_across_accounts() {
    let svc = two_account_tables().await;
    let own_arn = "arn:aws:dynamodb:us-east-1:123456789012:table/Shared";
    let (status, body) = call_as(
        &svc,
        "123456789012",
        "TransactWriteItems",
        json!({"TransactItems": [
            {"Put": {"TableName": "Shared", "Item": {"pk": {"S": "t"}}}},
            {"Put": {"TableName": OWNER_ARN, "Item": {"pk": {"S": "t"}}}}
        ]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(item_count(&svc, "123456789012"), 2);
    assert_eq!(item_count(&svc, OWNER), 2);
    // The writes landed on each table's stream.
    for account in ["123456789012", OWNER] {
        let accounts = svc.state.read();
        let records = accounts.regional(account, "us-east-1").unwrap().tables["Shared"]
            .stream_records
            .read()
            .len();
        assert_eq!(records, 2, "{account}");
    }

    // The same item named by name and by the caller's own ARN is one item.
    let (status, body) = call_as(
        &svc,
        "123456789012",
        "TransactWriteItems",
        json!({"TransactItems": [
            {"Put": {"TableName": "Shared", "Item": {"pk": {"S": "d"}}}},
            {"Delete": {"TableName": own_arn, "Key": {"pk": {"S": "d"}}}}
        ]}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["__type"], "ValidationException");

    // A failure applying a later write reverts the writes already applied
    // in both accounts.
    let (status, body) = call_as(
        &svc,
        "123456789012",
        "TransactWriteItems",
        json!({"TransactItems": [
            {"Put": {"TableName": OWNER_ARN, "Item": {"pk": {"S": "r"}}}},
            {"Put": {"TableName": "Shared", "Item": {"pk": {"S": "r"}}}},
            {"Update": {
                "TableName": OWNER_ARN,
                "Key": {"pk": {"S": "r2"}},
                "UpdateExpression": "BOGUS expression that won't parse"
            }}
        ]}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["__type"], "TransactionCanceledException");
    assert_eq!(item_count(&svc, "123456789012"), 2);
    assert_eq!(item_count(&svc, OWNER), 2);

    // A condition failing on the foreign table cancels the whole transaction.
    let (status, body) = call_as(
        &svc,
        "123456789012",
        "TransactWriteItems",
        json!({"TransactItems": [
            {"Put": {"TableName": "Shared", "Item": {"pk": {"S": "c"}}}},
            {"ConditionCheck": {
                "TableName": OWNER_ARN,
                "Key": {"pk": {"S": "owner"}},
                "ConditionExpression": "attribute_not_exists(pk)"
            }}
        ]}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["__type"], "TransactionCanceledException");
    assert_eq!(item_count(&svc, "123456789012"), 2);
}

#[tokio::test]
async fn other_accounts_tables_are_not_found_without_cross_account_support() {
    let svc = two_account_tables().await;
    for (action, body, code) in [
        (
            "CreateBackup",
            json!({"TableName": OWNER_ARN, "BackupName": "b"}),
            "TableNotFoundException",
        ),
        (
            "DescribeTimeToLive",
            json!({"TableName": OWNER_ARN}),
            "ResourceNotFoundException",
        ),
        (
            "GetResourcePolicy",
            json!({"ResourceArn": OWNER_ARN}),
            "ResourceNotFoundException",
        ),
        (
            "ExecuteStatement",
            json!({"Statement": format!("SELECT * FROM \"{OWNER_ARN}\"")}),
            "ResourceNotFoundException",
        ),
        (
            "DescribeContinuousBackups",
            json!({"TableName": OWNER_ARN}),
            "TableNotFoundException",
        ),
    ] {
        let (status, got) = call_as(&svc, "123456789012", action, body).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{action}: {got}");
        assert_eq!(got["__type"], code, "{action}");
    }

    // A listing filtered by another account's or region's table matches
    // nothing, even when both accounts hold backups of a table of that name.
    for account in [OWNER, "123456789012"] {
        let (status, got) = call_as(
            &svc,
            account,
            "CreateBackup",
            json!({"TableName": "Shared", "BackupName": "b"}),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{got}");
    }
    // The caller's own table still filters by name and by its own ARN.
    for filter in [
        "Shared",
        "arn:aws:dynamodb:us-east-1:123456789012:table/Shared",
    ] {
        let (_, got) = call_as(
            &svc,
            "123456789012",
            "ListBackups",
            json!({"TableName": filter}),
        )
        .await;
        assert_eq!(
            got["BackupSummaries"].as_array().map(Vec::len),
            Some(1),
            "{filter}"
        );
    }
    for (action, body, field) in [
        (
            "ListExports",
            json!({"TableArn": OWNER_ARN}),
            "ExportSummaries",
        ),
        (
            "ListBackups",
            json!({"TableName": OWNER_ARN}),
            "BackupSummaries",
        ),
        (
            "ListBackups",
            json!({"TableName": "arn:aws:dynamodb:us-west-2:123456789012:table/Shared"}),
            "BackupSummaries",
        ),
    ] {
        let (status, got) = call_as(&svc, "123456789012", action, body).await;
        assert_eq!(status, StatusCode::OK, "{action}: {got}");
        assert!(
            got[field].as_array().is_none_or(|a| a.is_empty()),
            "{action}: {got}"
        );
    }

    // Another region's table is not found, whoever owns it.
    for arn in [
        "arn:aws:dynamodb:us-west-2:123456789012:table/Shared",
        "arn:aws:dynamodb:us-west-2:444455556666:table/Shared",
    ] {
        let (status, got) = call_as(
            &svc,
            "123456789012",
            "GetItem",
            json!({"TableName": arn, "Key": {"pk": {"S": "owner"}}}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{arn}");
        assert_eq!(got["__type"], "ResourceNotFoundException", "{arn}");
    }

    // A batch naming another region's table fails as a whole.
    let (status, got) = call_as(
        &svc,
        "123456789012",
        "BatchGetItem",
        json!({"RequestItems": {
            "Shared": {"Keys": [{"pk": {"S": "owner"}}]},
            "arn:aws:dynamodb:eu-west-1:444455556666:table/Shared": {"Keys": [{"pk": {"S": "owner"}}]}
        }}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(got["__type"], "ResourceNotFoundException");
}

/// A table with a GSI (INCLUDE projection of `proj`) and an ALL-projected LSI,
/// for the index write-capacity cases.
fn create_indexed_capacity_table(svc: &DynamoDbService) {
    let req = make_request(
        "CreateTable",
        json!({
            "TableName": "idx-wcu",
            "AttributeDefinitions": [
                {"AttributeName": "pk", "AttributeType": "S"},
                {"AttributeName": "sk", "AttributeType": "S"},
                {"AttributeName": "gsiPk", "AttributeType": "S"},
                {"AttributeName": "lsiSk", "AttributeType": "S"}
            ],
            "KeySchema": [
                {"AttributeName": "pk", "KeyType": "HASH"},
                {"AttributeName": "sk", "KeyType": "RANGE"}
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": [{
                "IndexName": "gsi-inc",
                "KeySchema": [{"AttributeName": "gsiPk", "KeyType": "HASH"}],
                "Projection": {"ProjectionType": "INCLUDE", "NonKeyAttributes": ["proj"]}
            }],
            "LocalSecondaryIndexes": [{
                "IndexName": "lsi1",
                "KeySchema": [
                    {"AttributeName": "pk", "KeyType": "HASH"},
                    {"AttributeName": "lsiSk", "KeyType": "RANGE"}
                ],
                "Projection": {"ProjectionType": "ALL"}
            }]
        }),
    );
    svc.create_table(&req).unwrap();
}

#[test]
fn write_capacity_charges_only_the_indexes_a_write_changes() {
    let svc = make_service();
    create_indexed_capacity_table(&svc);
    let put = |item: Value| {
        let req = make_request(
            "PutItem",
            json!({"TableName": "idx-wcu", "Item": item, "ReturnConsumedCapacity": "INDEXES"}),
        );
        body_json(&svc.put_item(&req).unwrap())["ConsumedCapacity"].clone()
    };
    let full = json!({
        "pk": {"S": "a"}, "sk": {"S": "1"}, "gsiPk": {"S": "g"},
        "lsiSk": {"S": "l"}, "proj": {"S": "p"}, "other": {"S": "o"}
    });
    let cc = put(full.clone());
    assert_eq!(cc["CapacityUnits"], 3.0);
    assert_eq!(cc["Table"]["CapacityUnits"], 1.0);
    assert_eq!(
        cc["GlobalSecondaryIndexes"]["gsi-inc"]["CapacityUnits"],
        1.0
    );
    assert_eq!(cc["LocalSecondaryIndexes"]["lsi1"]["CapacityUnits"], 1.0);
    assert!(cc["Table"].get("WriteCapacityUnits").is_none());

    // An identical overwrite changes no index.
    let cc = put(full);
    assert_eq!(cc["CapacityUnits"], 1.0);
    assert!(cc.get("GlobalSecondaryIndexes").is_none());
    assert!(cc.get("LocalSecondaryIndexes").is_none());

    // Moving the GSI key is a delete plus an insert on the GSI; the LSI's
    // ALL projection sees the changed attribute too.
    let req = make_request(
        "UpdateItem",
        json!({
            "TableName": "idx-wcu",
            "Key": {"pk": {"S": "a"}, "sk": {"S": "1"}},
            "UpdateExpression": "SET gsiPk = :g",
            "ExpressionAttributeValues": {":g": {"S": "moved"}},
            "ReturnConsumedCapacity": "INDEXES",
        }),
    );
    let cc = body_json(&svc.update_item(&req).unwrap())["ConsumedCapacity"].clone();
    assert_eq!(cc["CapacityUnits"], 4.0);
    assert_eq!(
        cc["GlobalSecondaryIndexes"]["gsi-inc"]["CapacityUnits"],
        2.0
    );

    // A non-projected attribute leaves the INCLUDE GSI alone.
    let req = make_request(
        "UpdateItem",
        json!({
            "TableName": "idx-wcu",
            "Key": {"pk": {"S": "a"}, "sk": {"S": "1"}},
            "UpdateExpression": "SET #o = :o",
            "ExpressionAttributeNames": {"#o": "other"},
            "ExpressionAttributeValues": {":o": {"S": "o2"}},
            "ReturnConsumedCapacity": "INDEXES",
        }),
    );
    let cc = body_json(&svc.update_item(&req).unwrap())["ConsumedCapacity"].clone();
    assert_eq!(cc["CapacityUnits"], 2.0);
    assert!(cc.get("GlobalSecondaryIndexes").is_none());

    // A delete charges one write per index the item occupied.
    let req = make_request(
        "DeleteItem",
        json!({
            "TableName": "idx-wcu",
            "Key": {"pk": {"S": "a"}, "sk": {"S": "1"}},
            "ReturnConsumedCapacity": "TOTAL",
        }),
    );
    let cc = body_json(&svc.delete_item(&req).unwrap())["ConsumedCapacity"].clone();
    assert_eq!(cc["CapacityUnits"], 3.0);
    assert!(cc.get("Table").is_none());
}

#[test]
fn read_capacity_follows_item_size_and_consistency() {
    let svc = make_service();
    create_test_table(&svc);
    let req = make_request(
        "PutItem",
        json!({"TableName": "test-table", "Item": {"pk": {"S": "big"}, "v": {"S": "x".repeat(5000)}}}),
    );
    svc.put_item(&req).unwrap();
    let get = |consistent: bool| {
        let req = make_request(
            "GetItem",
            json!({
                "TableName": "test-table",
                "Key": {"pk": {"S": "big"}},
                "ConsistentRead": consistent,
                "ReturnConsumedCapacity": "TOTAL",
            }),
        );
        body_json(&svc.get_item(&req).unwrap())["ConsumedCapacity"]["CapacityUnits"].clone()
    };
    // Just over 4KB: two read units, halved when eventually consistent.
    assert_eq!(get(true), 2.0);
    assert_eq!(get(false), 1.0);
}

#[test]
fn put_item_enforces_the_400kb_gate_to_the_byte() {
    let svc = make_service();
    create_test_table(&svc);
    // pk (2) + "k" (1) + "p" (1) + padding.
    let put = |padding: usize| {
        let req = make_request(
            "PutItem",
            json!({"TableName": "test-table", "Item": {"pk": {"S": "k"}, "p": {"S": "x".repeat(padding)}}}),
        );
        svc.put_item(&req)
    };
    assert!(put(409_600 - 4).is_ok());
    let err = expect_err(put(409_600 - 3));
    assert_eq!(err.code(), "ValidationException");
    assert_eq!(
        err.message(),
        "Item size has exceeded the maximum allowed size"
    );
}

#[test]
fn update_item_charges_what_it_writes_plus_its_clauses() {
    let svc = make_service();
    create_test_table(&svc);
    let update = |key: &str, padding: usize| {
        let req = make_request(
            "UpdateItem",
            json!({
                "TableName": "test-table",
                "Key": {"pk": {"S": key}},
                "UpdateExpression": "SET b = :p",
                "ExpressionAttributeValues": {":p": {"S": "x".repeat(padding)}},
            }),
        );
        svc.update_item(&req)
    };
    // A one-byte key: the ceiling is 409,600 - 19 (22 for the clause, less
    // the 3 key bytes the update never writes). `b` costs 1 + padding.
    let ceiling = 409_600 - 19;
    assert!(update("K", ceiling - 3 - 1).is_ok());
    let err = expect_err(update("L", ceiling - 3));
    assert_eq!(
        err.message(),
        "Item size to update has exceeded the maximum allowed size"
    );
    // The refused update left nothing behind.
    let req = make_request(
        "GetItem",
        json!({"TableName": "test-table", "Key": {"pk": {"S": "L"}}}),
    );
    assert!(body_json(&svc.get_item(&req).unwrap())
        .get("Item")
        .is_none());

    // A transacted Update measures the finished item flat and cancels.
    let req = make_request(
        "TransactWriteItems",
        json!({"TransactItems": [{"Update": {
            "TableName": "test-table",
            "Key": {"pk": {"S": "T"}},
            "UpdateExpression": "SET b = :p",
            "ExpressionAttributeValues": {":p": {"S": "x".repeat(409_600 - 3 - 1)}},
        }}]}),
    );
    let resp = svc.transact_write_items(&req).unwrap();
    assert_eq!(resp.status, StatusCode::OK);
    let req = make_request(
        "TransactWriteItems",
        json!({"TransactItems": [{"Update": {
            "TableName": "test-table",
            "Key": {"pk": {"S": "U"}},
            "UpdateExpression": "SET b = :p",
            "ExpressionAttributeValues": {":p": {"S": "x".repeat(409_600 - 3)}},
        }}]}),
    );
    let resp = svc.transact_write_items(&req).unwrap();
    let b = body_json(&resp);
    assert_eq!(b["__type"], "TransactionCanceledException");
    assert_eq!(b["CancellationReasons"][0]["Code"], "ValidationError");
    assert_eq!(
        b["CancellationReasons"][0]["Message"],
        "Item size to update has exceeded the maximum allowed size"
    );
}

#[test]
fn numbers_are_stored_in_canonical_form_and_range_checked() {
    let svc = make_service();
    create_test_table(&svc);
    let req = make_request(
        "PutItem",
        json!({"TableName": "test-table", "Item": {
            "pk": {"S": "n"},
            "a": {"N": "+1.5E+3"},
            "b": {"N": "0042.1200"},
            "c": {"N": "-0"},
            "d": {"NS": [".5", "1e2"]},
        }}),
    );
    svc.put_item(&req).unwrap();
    let req = make_request(
        "GetItem",
        json!({"TableName": "test-table", "Key": {"pk": {"S": "n"}}}),
    );
    let item = body_json(&svc.get_item(&req).unwrap())["Item"].clone();
    assert_eq!(item["a"]["N"], "1500");
    assert_eq!(item["b"]["N"], "42.12");
    assert_eq!(item["c"]["N"], "0");
    assert_eq!(item["d"]["NS"], json!(["0.5", "100"]));

    for bad in [" 5", "5 ", "1 5"] {
        let req = make_request(
            "PutItem",
            json!({"TableName": "test-table", "Item": {"pk": {"S": "bad"}, "v": {"N": bad}}}),
        );
        let err = expect_err(svc.put_item(&req));
        assert!(err.message().contains("numeric value"), "{bad}");
    }
    let req = make_request(
        "PutItem",
        json!({"TableName": "test-table", "Item": {"pk": {"S": "bad"}, "v": {"N": "1E+126"}}}),
    );
    assert!(expect_err(svc.put_item(&req))
        .message()
        .starts_with("Number overflow"));

    let req = make_request(
        "UpdateItem",
        json!({
            "TableName": "test-table",
            "Key": {"pk": {"S": "overflow"}},
            "UpdateExpression": "SET n = :a + :b",
            "ExpressionAttributeValues": {":a": {"N": "9.9e125"}, ":b": {"N": "9.9e125"}},
        }),
    );
    assert!(expect_err(svc.update_item(&req))
        .message()
        .starts_with("Number overflow"));
}

#[test]
fn transact_write_capacity_is_doubled_and_replays_as_a_read() {
    let svc = make_service();
    create_test_table(&svc);
    let body = json!({
        "ClientRequestToken": "replay-token",
        "ReturnConsumedCapacity": "TOTAL",
        "TransactItems": [{"Put": {
            "TableName": "test-table",
            "Item": {"pk": {"S": "r"}, "big": {"S": "x".repeat(1536)}},
        }}],
    });
    let first = body_json(
        &svc.transact_write_items(&make_request("TransactWriteItems", body.clone()))
            .unwrap(),
    );
    assert_eq!(first["ConsumedCapacity"][0]["WriteCapacityUnits"], 4.0);
    assert!(first["ConsumedCapacity"][0]
        .get("ReadCapacityUnits")
        .is_none());
    let replay = body_json(
        &svc.transact_write_items(&make_request("TransactWriteItems", body))
            .unwrap(),
    );
    assert_eq!(replay["ConsumedCapacity"][0]["ReadCapacityUnits"], 2.0);
    assert!(replay["ConsumedCapacity"][0]
        .get("WriteCapacityUnits")
        .is_none());
}

#[test]
fn put_item_reports_every_invalid_enum_together() {
    let svc = make_service();
    let req = make_request(
        "PutItem",
        json!({
            "TableName": "missing-table",
            "Item": {"pk": {"S": "x"}},
            "ReturnConsumedCapacity": "INVALID",
            "ReturnItemCollectionMetrics": "INVALID",
            "ReturnValues": "INVALID",
        }),
    );
    let err = expect_err(svc.put_item(&req));
    assert_eq!(err.code(), "ValidationException");
    assert!(err.message().starts_with("3 validation errors detected: "));
}

/// Only the values an update writes are normalized. A number stored in a
/// non-canonical spelling (written by an older build and restored from its
/// snapshot) is left alone when the update does not touch it, so it never
/// shows up in UPDATED_NEW/UPDATED_OLD or a stream MODIFY image as changed.
#[test]
fn update_item_normalizes_only_the_values_it_writes() {
    let svc = make_service();
    create_test_table(&svc);
    {
        let mut accounts = svc.state.write();
        let state = accounts.regional_mut("123456789012", "us-east-1");
        let table = state.tables.get_mut("test-table").unwrap();
        let legacy: HashMap<String, Value> = serde_json::from_value(json!({
            "pk": {"S": "legacy"},
            "old": {"N": "1.50"},
        }))
        .unwrap();
        table.put_item_at_key(legacy);
    }
    let req = make_request(
        "UpdateItem",
        json!({
            "TableName": "test-table",
            "Key": {"pk": {"S": "legacy"}},
            "UpdateExpression": "SET n = :v",
            "ExpressionAttributeValues": {":v": {"N": "+5.0"}},
            "ReturnValues": "UPDATED_NEW",
        }),
    );
    let b = body_json(&svc.update_item(&req).unwrap());
    assert_eq!(b["Attributes"], json!({"n": {"N": "5"}}));

    let req = make_request(
        "GetItem",
        json!({"TableName": "test-table", "Key": {"pk": {"S": "legacy"}}}),
    );
    let item = body_json(&svc.get_item(&req).unwrap())["Item"].clone();
    assert_eq!(item["old"]["N"], "1.50");
    assert_eq!(item["n"]["N"], "5");

    // Legacy AttributeUpdates values are normalized the same way.
    let req = make_request(
        "UpdateItem",
        json!({
            "TableName": "test-table",
            "Key": {"pk": {"S": "legacy"}},
            "AttributeUpdates": {"m": {"Value": {"N": "1e2"}, "Action": "PUT"}},
            "ReturnValues": "UPDATED_OLD",
        }),
    );
    let b = body_json(&svc.update_item(&req).unwrap());
    assert!(b.get("Attributes").is_none());
    let req = make_request(
        "GetItem",
        json!({"TableName": "test-table", "Key": {"pk": {"S": "legacy"}}}),
    );
    let item = body_json(&svc.get_item(&req).unwrap())["Item"].clone();
    assert_eq!(item["m"]["N"], "100");
    assert_eq!(item["old"]["N"], "1.50");
}

/// Without ReturnConsumedCapacity no capacity block is reported by any of
/// the operations that only size items to build one.
#[test]
fn capacity_is_omitted_when_not_requested() {
    let svc = make_service();
    create_test_table(&svc);
    let req = make_request(
        "BatchWriteItem",
        json!({"RequestItems": {"test-table": [
            {"PutRequest": {"Item": {"pk": {"S": "a"}}}},
            {"PutRequest": {"Item": {"pk": {"S": "b"}}}},
        ]}}),
    );
    assert!(body_json(&svc.batch_write_item(&req).unwrap())
        .get("ConsumedCapacity")
        .is_none());
    let req = make_request(
        "DeleteItem",
        json!({"TableName": "test-table", "Key": {"pk": {"S": "a"}}}),
    );
    assert!(body_json(&svc.delete_item(&req).unwrap())
        .get("ConsumedCapacity")
        .is_none());
    let req = make_request(
        "Query",
        json!({
            "TableName": "test-table",
            "KeyConditionExpression": "pk = :p",
            "ExpressionAttributeValues": {":p": {"S": "b"}},
        }),
    );
    let b = body_json(&svc.query(&req).unwrap());
    assert_eq!(b["Count"], 1);
    assert!(b.get("ConsumedCapacity").is_none());
    let req = make_request("Scan", json!({"TableName": "test-table"}));
    assert!(body_json(&svc.scan(&req).unwrap())
        .get("ConsumedCapacity")
        .is_none());
}

#[tokio::test]
async fn china_region_arns_use_the_china_partition_and_resolve_by_arn() {
    let svc = make_service();
    let call_cn = |action: &str, body: Value| {
        let mut r = make_request(action, body);
        r.region = "cn-north-1".to_string();
        r
    };
    let resp = svc
        .handle(call_cn(
            "CreateTable",
            json!({
                "TableName": "cn-table",
                "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
                "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
                "BillingMode": "PAY_PER_REQUEST",
                "StreamSpecification": { "StreamEnabled": true, "StreamViewType": "NEW_IMAGE" }
            }),
        ))
        .await
        .unwrap();
    let created: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let arn = created["TableDescription"]["TableArn"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        arn,
        "arn:aws-cn:dynamodb:cn-north-1:123456789012:table/cn-table"
    );
    let stream = created["TableDescription"]["LatestStreamArn"]
        .as_str()
        .unwrap();
    assert!(stream.starts_with(&format!("{arn}/stream/")), "{stream}");

    // The ARN resolves wherever a TableName is accepted, and for tagging.
    let resp = svc
        .handle(call_cn("DescribeTable", json!({ "TableName": arn })))
        .await
        .unwrap();
    let described: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(described["Table"]["TableName"], "cn-table");
    svc.handle(call_cn(
        "TagResource",
        json!({ "ResourceArn": arn, "Tags": [{ "Key": "k", "Value": "v" }] }),
    ))
    .await
    .unwrap();
    let resp = svc
        .handle(call_cn("ListTagsOfResource", json!({ "ResourceArn": arn })))
        .await
        .unwrap();
    let tags: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(tags["Tags"][0]["Key"], "k");

    let resp = svc
        .handle(call_cn(
            "CreateGlobalTable",
            json!({ "GlobalTableName": "cn-table", "ReplicationGroup": [{ "RegionName": "cn-north-1" }] }),
        ))
        .await
        .unwrap();
    let global: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    assert_eq!(
        global["GlobalTableDescription"]["GlobalTableArn"],
        "arn:aws-cn:dynamodb::123456789012:global-table/cn-table"
    );
}

#[test]
fn table_arn_parsers_accept_any_partition() {
    let arn = "arn:aws-cn:dynamodb:cn-north-1:123456789012:table/T/stream/2026";
    assert_eq!(super::helpers::resolve_table_name(arn), "T");
    assert_eq!(
        super::cross_account::arn_scope(arn),
        Some(("cn-north-1", "123456789012"))
    );
    assert_eq!(
        super::helpers::resolve_table_name("arn:bogus:dynamodb:r:1:table/T"),
        "arn:bogus:dynamodb:r:1:table/T"
    );
}

// ---------------------------------------------------------------------
// Table control-plane validation
// ---------------------------------------------------------------------

#[test]
fn create_table_semantic_rejections_carry_aws_messages() {
    let svc = make_service();
    let base = json!({
        "TableName": "sem-table",
        "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
        "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
        "BillingMode": "PAY_PER_REQUEST",
    });
    let reject = |patch: &dyn Fn(&mut Value)| {
        let mut body = base.clone();
        patch(&mut body);
        err_message(err_of(svc.create_table(&make_request("CreateTable", body))))
    };
    assert_eq!(
        reject(&|b| b["ProvisionedThroughput"] =
            json!({"ReadCapacityUnits": 5, "WriteCapacityUnits": 5})),
        "One or more parameter values were invalid: Neither ReadCapacityUnits nor \
         WriteCapacityUnits can be specified when BillingMode is PAY_PER_REQUEST"
    );
    assert_eq!(
        reject(&|b| b["StreamSpecification"] =
            json!({"StreamEnabled": false, "StreamViewType": "NEW_IMAGE"})),
        "One or more parameter values were invalid: Table is being created with a stream \
         disabled, UpdateViewType should not be specified"
    );
    assert_eq!(
        reject(&|b| b["KeySchema"] = json!([
            { "AttributeName": "pk", "KeyType": "HASH" },
            { "AttributeName": "pk", "KeyType": "RANGE" },
        ])),
        "Invalid KeySchema: Some index key attribute have no definition"
    );
    assert!(reject(&|b| b["LocalSecondaryIndexes"] = json!([{
        "IndexName": "lsi1",
        "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
        "Projection": { "ProjectionType": "ALL" },
    }]))
    .contains("Table KeySchema does not have a range key"));
    assert_eq!(
        reject(&|b| {
            b["AttributeDefinitions"] = json!([
                { "AttributeName": "pk", "AttributeType": "S" },
                { "AttributeName": "g", "AttributeType": "S" },
            ]);
            b["GlobalSecondaryIndexes"] = json!([{
                "IndexName": "gsi",
                "KeySchema": [{ "AttributeName": "g", "KeyType": "HASH" }],
                "Projection": { "ProjectionType": "INCLUDE" },
            }]);
        }),
        "One or more parameter values were invalid: ProjectionType is INCLUDE, but \
         NonKeyAttributes is not specified"
    );
    assert!(reject(&|b| b["TableName"] = json!("a".repeat(256)))
        .contains("Member must have length less than or equal to 255"));
}

fn create_provisioned_table(svc: &DynamoDbService) {
    svc.create_table(&make_request(
        "CreateTable",
        json!({
            "TableName": "prov-table",
            "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
            "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
            "ProvisionedThroughput": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 5 },
        }),
    ))
    .unwrap();
}

#[test]
fn update_table_rejects_invalid_throughput_and_index_changes() {
    let svc = make_service();
    create_provisioned_table(&svc);
    let update = |body: Value| {
        let mut body = body;
        body["TableName"] = json!("prov-table");
        svc.update_table(&make_request("UpdateTable", body))
    };

    let msg = err_message(err_of(update(json!({
        "ProvisionedThroughput": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 5 },
    }))));
    assert!(msg.starts_with("The provisioned throughput for the table will not change."));
    let msg = err_message(err_of(update(json!({
        "ProvisionedThroughput": { "ReadCapacityUnits": 0, "WriteCapacityUnits": 5 },
    }))));
    assert!(
        msg.contains("'provisionedThroughput.readCapacityUnits'"),
        "{msg}"
    );
    let msg = err_message(err_of(update(json!({
        "BillingMode": "PAY_PER_REQUEST",
        "ProvisionedThroughput": { "ReadCapacityUnits": 6, "WriteCapacityUnits": 6 },
    }))));
    assert!(msg.contains("Neither ReadCapacityUnits nor WriteCapacityUnits"));

    // A new GSI's keys must be defined in the request itself.
    let msg = err_message(err_of(update(json!({
        "GlobalSecondaryIndexUpdates": [{ "Create": {
            "IndexName": "by-pk",
            "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
            "Projection": { "ProjectionType": "ALL" },
            "ProvisionedThroughput": { "ReadCapacityUnits": 1, "WriteCapacityUnits": 1 },
        }}],
    }))));
    assert!(
        msg.contains("Some index key attributes are not defined"),
        "{msg}"
    );
    let err = err_of(update(json!({
        "GlobalSecondaryIndexUpdates": [{ "Delete": { "IndexName": "missing" } }],
    })));
    assert_eq!(err.code(), "ResourceNotFoundException");

    update(json!({
        "AttributeDefinitions": [{ "AttributeName": "g", "AttributeType": "S" }],
        "GlobalSecondaryIndexUpdates": [{ "Create": {
            "IndexName": "gsi",
            "KeySchema": [{ "AttributeName": "g", "KeyType": "HASH" }],
            "Projection": { "ProjectionType": "ALL" },
            "ProvisionedThroughput": { "ReadCapacityUnits": 1, "WriteCapacityUnits": 1 },
        }}],
    }))
    .unwrap();
    let msg = err_message(err_of(update(json!({
        "AttributeDefinitions": [{ "AttributeName": "g", "AttributeType": "S" }],
        "GlobalSecondaryIndexUpdates": [{ "Create": {
            "IndexName": "gsi",
            "KeySchema": [{ "AttributeName": "g", "KeyType": "HASH" }],
            "Projection": { "ProjectionType": "ALL" },
            "ProvisionedThroughput": { "ReadCapacityUnits": 1, "WriteCapacityUnits": 1 },
        }}],
    }))));
    assert_eq!(msg, "Attempting to create an index which already exists");
}

#[test]
fn ttl_and_tagging_validate_their_inputs() {
    let svc = make_service();
    create_test_table(&svc);
    let err = err_of(svc.update_time_to_live(&make_request(
        "UpdateTimeToLive",
        json!({
            "TableName": "test-table",
            "TimeToLiveSpecification": { "AttributeName": "", "Enabled": true },
        }),
    )));
    assert!(err_message(err).contains("'timeToLiveSpecification.attributeName'"));

    let err = err_of(svc.tag_resource(&make_request(
        "TagResource",
        json!({ "ResourceArn": "not-a-valid-arn", "Tags": [{ "Key": "k", "Value": "v" }] }),
    )));
    assert_eq!(err.code(), "ValidationException");
}

#[tokio::test]
async fn a_missing_table_in_another_account_is_access_denied() {
    let svc = make_service();
    let err = svc
        .handle(make_request(
            "ListTagsOfResource",
            json!({ "ResourceArn": "arn:aws:dynamodb:us-east-1:000000000000:table/nope" }),
        ))
        .await
        .err()
        .expect("a foreign table without a policy cannot be read");
    assert_eq!(err.code(), "AccessDeniedException");
}

#[test]
fn index_reads_reject_unknown_indexes_and_foreign_start_keys() {
    let svc = make_service();
    svc.create_table(&make_request(
        "CreateTable",
        json!({
            "TableName": "idx-table",
            "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
            "AttributeDefinitions": [
                { "AttributeName": "pk", "AttributeType": "S" },
                { "AttributeName": "g", "AttributeType": "S" },
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "GlobalSecondaryIndexes": [{
                "IndexName": "gsi",
                "KeySchema": [{ "AttributeName": "g", "KeyType": "HASH" }],
                "Projection": { "ProjectionType": "ALL" },
            }],
        }),
    ))
    .unwrap();
    let msg = err_message(err_of(svc.scan(&make_request(
        "Scan",
        json!({ "TableName": "idx-table", "IndexName": "nope" }),
    ))));
    assert_eq!(msg, "The table does not have the specified index: nope");
    let msg = err_message(err_of(svc.query(&make_request(
        "Query",
        json!({
            "TableName": "idx-table",
            "IndexName": "gsi",
            "KeyConditionExpression": "g = :g",
            "ExpressionAttributeValues": { ":g": { "S": "x" } },
            "ExclusiveStartKey": { "pk": { "S": "x" } },
        }),
    ))));
    assert!(
        msg.starts_with("The provided starting key is invalid"),
        "{msg}"
    );
}

#[test]
fn vector_index_arns_take_the_table_partition() {
    let svc = make_service();
    let mut req = make_request(
        "CreateTable",
        json!({
            "TableName": "cn-vec",
            "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
            "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [{
                "IndexName": "vix",
                "VectorAttribute": { "AttributeName": "embedding" },
                "Dimensions": 2,
                "DistanceFunction": "COSINE",
                "Projection": { "ProjectionType": "ALL" },
            }],
        }),
    );
    req.region = "cn-north-1".to_string();
    let body = body_json(&svc.create_table(&req).unwrap());
    assert_eq!(
        body["TableDescription"]["VectorIndexes"][0]["IndexArn"],
        "arn:aws-cn:dynamodb:cn-north-1:123456789012:table/cn-vec/index/vix"
    );
}

#[test]
fn update_table_keeps_vector_tables_on_demand_and_serialises_index_builds() {
    let svc = make_service();
    create_vector_table(&svc, "COSINE");
    let update = |body: Value| {
        let mut body = body;
        body["TableName"] = json!("vec-table");
        svc.update_table(&make_request("UpdateTable", body))
    };
    let msg = err_message(err_of(update(json!({
        "BillingMode": "PROVISIONED",
        "ProvisionedThroughput": { "ReadCapacityUnits": 5, "WriteCapacityUnits": 5 },
    }))));
    assert!(msg.contains("Vector indexes are only supported for PAY_PER_REQUEST tables"));

    update(json!({
        "VectorIndexUpdates": [{ "Create": {
            "IndexName": "online",
            "VectorAttribute": { "AttributeName": "other" },
            "Dimensions": 4,
            "DistanceFunction": "COSINE",
            "Projection": { "ProjectionType": "KEYS_ONLY" },
        }}],
    }))
    .unwrap();
    let gsi_create = json!({
        "AttributeDefinitions": [{ "AttributeName": "g", "AttributeType": "S" }],
        "GlobalSecondaryIndexUpdates": [{ "Create": {
            "IndexName": "gsi",
            "KeySchema": [{ "AttributeName": "g", "KeyType": "HASH" }],
            "Projection": { "ProjectionType": "ALL" },
        }}],
    });
    // While the vector index allocates: no second online index, and no other
    // change to the UPDATING table.
    assert_eq!(
        err_of(update(gsi_create.clone())).code(),
        "LimitExceededException"
    );
    let err = err_of(update(json!({ "DeletionProtectionEnabled": true })));
    assert_eq!(err.code(), "ResourceInUseException");
    assert!(err_message(err).contains("Table is being updated"));

    // Backfilling: the table is ACTIVE and takes other changes, but the
    // online index action is still held.
    age_vector_index(
        &svc,
        "vec-table",
        "online",
        crate::state::VECTOR_INDEX_ALLOCATION_MS,
    );
    update(json!({ "DeletionProtectionEnabled": false })).unwrap();
    assert_eq!(
        err_of(update(gsi_create.clone())).code(),
        "LimitExceededException"
    );

    age_vector_index(
        &svc,
        "vec-table",
        "online",
        crate::state::VECTOR_INDEX_BACKFILL_MS,
    );
    update(gsi_create).unwrap();
}

#[test]
fn a_search_schema_element_missing_a_member_is_rejected() {
    let svc = make_service();
    let err = err_of(svc.create_table(&make_request(
        "CreateTable",
        json!({
            "TableName": "vec-missing",
            "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
            "AttributeDefinitions": [
                { "AttributeName": "pk", "AttributeType": "S" },
                { "AttributeName": "tenant", "AttributeType": "S" },
            ],
            "BillingMode": "PAY_PER_REQUEST",
            "VectorIndexes": [{
                "IndexName": "vix",
                "VectorAttribute": { "AttributeName": "embedding" },
                "Dimensions": 3,
                "DistanceFunction": "COSINE",
                "SearchSchema": [{ "AttributeName": "tenant" }],
                "Projection": { "ProjectionType": "ALL" },
            }],
        }),
    )));
    assert_eq!(
        err_message(err),
        "1 validation error detected: Value null at \
         'vectorIndexes.1.member.searchSchema.1.member.searchSchemaElementType' failed to \
         satisfy constraint: Member must not be null"
    );
}

/// A KMS hook that counts key resolutions, and checks that none happens while
/// the DynamoDB state lock is held.
struct CountingKmsHook(std::sync::atomic::AtomicUsize, SharedDynamoDbState);

impl fakecloud_core::delivery::KmsHook for CountingKmsHook {
    fn encrypt(
        &self,
        _: &str,
        _: &str,
        _: &str,
        _: &[u8],
        _caller: fakecloud_core::delivery::KmsCaller<'_>,
        _: HashMap<String, String>,
    ) -> Result<String, fakecloud_core::delivery::KmsError> {
        Ok(String::new())
    }

    fn decrypt(
        &self,
        _: &str,
        _: &str,
        _caller: fakecloud_core::delivery::KmsCaller<'_>,
        _: HashMap<String, String>,
    ) -> Result<Vec<u8>, fakecloud_core::delivery::KmsError> {
        Ok(Vec::new())
    }

    fn resolve_key_arn(&self, _: &str, _: &str, _: &str, _: &str) -> Result<String, String> {
        self.count_resolution()
    }

    fn aws_managed_key_arn(&self, _: &str, _: &str, _: &str, _: &str) -> Result<String, String> {
        self.count_resolution()
    }
}

impl CountingKmsHook {
    fn count_resolution(&self) -> Result<String, String> {
        assert!(
            self.1.try_write().is_some(),
            "the KMS key must be resolved with the DynamoDB lock released"
        );
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok("arn:aws:kms:us-east-1:123456789012:key/managed".to_string())
    }
}

#[test]
fn sse_key_is_resolved_only_for_a_request_that_succeeds() {
    let svc = make_service();
    let hook = Arc::new(CountingKmsHook(
        std::sync::atomic::AtomicUsize::new(0),
        svc.state.clone(),
    ));
    let svc = svc.with_kms_hook(hook.clone());
    let resolutions = || hook.0.load(std::sync::atomic::Ordering::SeqCst);
    create_test_table(&svc);
    let create = || {
        svc.create_table(&make_request(
            "CreateTable",
            json!({
                "TableName": "test-table",
                "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
                "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
                "BillingMode": "PAY_PER_REQUEST",
                "SSESpecification": { "Enabled": true },
            }),
        ))
    };
    // The table already exists: rejected, and no key is touched.
    assert_eq!(err_of(create()).code(), "ResourceInUseException");
    let err = err_of(svc.update_table(&make_request(
        "UpdateTable",
        json!({ "TableName": "missing-table", "SSESpecification": { "Enabled": true } }),
    )));
    assert_eq!(err.code(), "ResourceNotFoundException");
    assert_eq!(resolutions(), 0);

    svc.update_table(&make_request(
        "UpdateTable",
        json!({ "TableName": "test-table", "SSESpecification": { "Enabled": true } }),
    ))
    .unwrap();
    assert_eq!(resolutions(), 1);
    svc.create_table(&make_request(
        "CreateTable",
        json!({
            "TableName": "sse-created",
            "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
            "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
            "BillingMode": "PAY_PER_REQUEST",
            "SSESpecification": { "Enabled": true },
        }),
    ))
    .unwrap();
    assert_eq!(resolutions(), 2);
    assert_eq!(
        describe(&svc, "test-table")["SSEDescription"]["KMSMasterKeyArn"],
        "arn:aws:kms:us-east-1:123456789012:key/managed"
    );
}

#[test]
fn a_wrong_typed_member_fails_to_deserialize_and_changes_nothing() {
    let svc = make_service();
    create_vector_table(&svc, "COSINE");
    let before = describe(&svc, "vec-table");
    let err = err_of(svc.update_table(&make_request(
        "UpdateTable",
        json!({
            "TableName": "vec-table",
            "OnDemandThroughput": { "MaxReadRequestUnits": 10 },
            "AttributeDefinitions": [{ "AttributeName": "cat", "AttributeType": "S" }],
            "VectorIndexUpdates": [{ "Create": {
                "IndexName": "typed",
                "VectorAttribute": { "AttributeName": "other" },
                "Dimensions": 2,
                "DistanceFunction": "COSINE",
                "SearchSchema": [{ "AttributeName": 7, "SearchSchemaElementType": "HASH" }],
                "Projection": { "ProjectionType": "ALL" },
            }}],
        }),
    )));
    assert_eq!(err.code(), "SerializationException");
    assert_eq!(
        err_message(err),
        "NUMBER_VALUE can not be converted to a String"
    );
    let after = describe(&svc, "vec-table");
    assert_eq!(
        before["AttributeDefinitions"],
        after["AttributeDefinitions"]
    );
    assert_eq!(
        before.get("OnDemandThroughput"),
        after.get("OnDemandThroughput")
    );
    assert_eq!(before["VectorIndexes"], after["VectorIndexes"]);

    // Deserialization fails before the table name is validated, so a bad,
    // or absent, name does not change the answer.
    for name in [json!("typed-table"), json!("ab"), Value::Null] {
        let mut body = json!({
            "KeySchema": [{ "AttributeName": "pk", "KeyType": true }],
            "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
            "BillingMode": "PAY_PER_REQUEST",
        });
        if !name.is_null() {
            body["TableName"] = name.clone();
        }
        let err = err_of(svc.create_table(&make_request("CreateTable", body)));
        assert_eq!(err.code(), "SerializationException", "{name}");
    }
}

#[test]
fn partiql_writes_validate_vector_attributes() {
    let svc = make_service();
    create_vector_table(&svc, "COSINE");
    let run = |statement: &str| {
        svc.execute_statement(&make_request(
            "ExecuteStatement",
            json!({ "Statement": statement }),
        ))
    };
    let msg = err_message(err_of(run(
        "INSERT INTO \"vec-table\" VALUE {'pk': 'a', 'embedding': [1, 2, 3]}",
    )));
    assert!(
        msg.contains("Invalid size for parameter embedding, Expected: 2, Actual: 3"),
        "{msg}"
    );

    // An accepted INSERT reports the vector index's write under INDEXES.
    let inserted = body_json(
        &svc.execute_statement(&make_request(
            "ExecuteStatement",
            json!({
                "Statement": "INSERT INTO \"vec-table\" VALUE {'pk': 'a', 'embedding': [1, 2]}",
                "ReturnConsumedCapacity": "INDEXES",
            }),
        ))
        .unwrap(),
    );
    assert_eq!(
        inserted["ConsumedCapacity"]["VectorIndexes"]["embedding-index"]["VectorWriteRequestBytes"],
        1024.0
    );
    let msg = err_message(err_of(run(
        "UPDATE \"vec-table\" SET embedding = [1, 2, 3] WHERE pk = 'a'",
    )));
    assert!(
        msg.contains("Invalid size for parameter embedding, Expected: 2, Actual: 3"),
        "{msg}"
    );
    // The rejected UPDATE left the item as it was.
    let item = &body_json(
        &svc.get_item(&make_request(
            "GetItem",
            json!({ "TableName": "vec-table", "Key": { "pk": { "S": "a" } } }),
        ))
        .unwrap(),
    )["Item"];
    assert_eq!(item["embedding"]["L"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn transact_put_reports_a_bad_number_before_the_vector_shape() {
    let svc = make_service();
    create_vector_table(&svc, "COSINE");
    let item = json!({
        "pk": { "S": "a" },
        "embedding": { "L": [{ "N": "abc" }, { "N": "1" }] },
    });
    let put = err_of(svc.put_item(&make_request(
        "PutItem",
        json!({ "TableName": "vec-table", "Item": item.clone() }),
    )));
    let transact = svc
        .handle(make_request(
            "TransactWriteItems",
            json!({ "TransactItems": [{ "Put": { "TableName": "vec-table", "Item": item } }] }),
        ))
        .await
        .err()
        .expect("the malformed number is rejected");
    assert_eq!(transact.code(), put.code());
    assert_eq!(err_message(transact), err_message(put));
}

/// A KMS-encrypted table with no named key reports its region's AWS-managed
/// `aws/dynamodb` key: exactly the key `alias/aws/dynamodb` resolves to in
/// that region, and a different key in every region.
#[test]
fn default_sse_key_is_what_the_regions_dynamodb_alias_resolves_to() {
    let (kms_state, hook) = fakecloud_kms::test_support::kms_hook("123456789012");
    let svc = make_service().with_kms_hook(hook.clone());
    let create = |name: &str, region: &str| {
        let mut r = make_request(
            "CreateTable",
            json!({
                "TableName": name,
                "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
                "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
                "BillingMode": "PAY_PER_REQUEST",
                "SSESpecification": { "Enabled": true, "SSEType": "KMS" },
            }),
        );
        r.region = region.to_string();
        svc.create_table(&r).unwrap();
        let mut d = make_request("DescribeTable", json!({ "TableName": name }));
        d.region = region.to_string();
        let resp = svc.describe_table(&d).unwrap();
        serde_json::from_slice::<Value>(resp.body.expect_bytes()).unwrap()["Table"]
            ["SSEDescription"]["KMSMasterKeyArn"]
            .as_str()
            .unwrap()
            .to_string()
    };
    let alias_in = |region: &str| {
        hook.resolve_key_arn(
            "123456789012",
            region,
            "alias/aws/dynamodb",
            "dynamodb.amazonaws.com",
        )
        .unwrap()
    };
    let east = create("table-one", "us-east-1");
    // eu-west-1's key minted first through the regional path is the one a
    // eu-west-1 table then reports.
    let minted = hook
        .aws_managed_key_arn(
            "123456789012",
            "eu-west-1",
            "dynamodb",
            "dynamodb.amazonaws.com",
        )
        .unwrap();
    let west = create("table-two", "eu-west-1");
    assert_eq!(west, minted);
    assert_ne!(east, west);
    assert_eq!(east, alias_in("us-east-1"));
    assert_eq!(west, alias_in("eu-west-1"));
    for (region, arn) in [("us-east-1", &east), ("eu-west-1", &west)] {
        fakecloud_kms::test_support::assert_aws_managed_key(
            &kms_state,
            "123456789012",
            region,
            arn,
            "alias/aws/dynamodb",
        );
    }
}

// ── Region scoping ─────────────────────────────────────────────────────

/// Call `action` in `region`, returning the status and JSON body.
async fn call_in(
    svc: &DynamoDbService,
    region: &str,
    action: &str,
    body: Value,
) -> (StatusCode, Value) {
    let mut req = make_request(action, body);
    req.region = region.to_string();
    match svc.handle(req).await {
        Ok(resp) => (
            resp.status,
            serde_json::from_slice(resp.body.expect_bytes()).unwrap(),
        ),
        Err(e) => (
            e.status(),
            json!({ "__type": e.code(), "message": format!("{e:?}") }),
        ),
    }
}

fn simple_table(name: &str) -> Value {
    json!({
        "TableName": name,
        "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
        "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
        "BillingMode": "PAY_PER_REQUEST",
        "StreamSpecification": { "StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES" },
    })
}

#[tokio::test]
async fn the_same_table_name_is_a_separate_table_in_each_region() {
    let svc = make_service();
    for region in ["us-east-1", "eu-west-1"] {
        let (status, body) = call_in(&svc, region, "CreateTable", simple_table("Orders")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(
            body["TableDescription"]["TableArn"],
            format!("arn:aws:dynamodb:{region}:123456789012:table/Orders")
        );
        call_in(
            &svc,
            region,
            "PutItem",
            json!({"TableName": "Orders", "Item": {"pk": {"S": "k"}, "region": {"S": region}}}),
        )
        .await;
    }
    call_in(&svc, "eu-west-1", "CreateTable", simple_table("OnlyWest")).await;

    for region in ["us-east-1", "eu-west-1"] {
        let (_, item) = call_in(
            &svc,
            region,
            "GetItem",
            json!({"TableName": "Orders", "Key": {"pk": {"S": "k"}}}),
        )
        .await;
        assert_eq!(item["Item"]["region"]["S"], region);
        let (_, d) = call_in(
            &svc,
            region,
            "DescribeTable",
            json!({"TableName": "Orders"}),
        )
        .await;
        assert_eq!(d["Table"]["ItemCount"], 1);
        assert!(d["Table"]["LatestStreamArn"]
            .as_str()
            .unwrap()
            .starts_with(&format!("arn:aws:dynamodb:{region}:")));
    }
    let (_, east) = call_in(&svc, "us-east-1", "ListTables", json!({})).await;
    assert_eq!(east["TableNames"], json!(["Orders"]));
    let (_, west) = call_in(&svc, "eu-west-1", "ListTables", json!({})).await;
    assert_eq!(west["TableNames"], json!(["OnlyWest", "Orders"]));

    // A table ARN naming another region is not found, even when the
    // request's region has a table of that name.
    let (status, err) = call_in(
        &svc,
        "us-east-1",
        "DescribeTable",
        json!({"TableName": "arn:aws:dynamodb:eu-west-1:123456789012:table/OnlyWest"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(err["__type"], "ResourceNotFoundException");
    let (status, err) = call_in(
        &svc,
        "us-east-1",
        "DescribeTable",
        json!({"TableName": "arn:aws:dynamodb:eu-west-1:123456789012:table/Orders"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(err["__type"], "ResourceNotFoundException");
    // By its own region's ARN it is found.
    let (status, _) = call_in(
        &svc,
        "eu-west-1",
        "DescribeTable",
        json!({"TableName": "arn:aws:dynamodb:eu-west-1:123456789012:table/OnlyWest"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Deleting one region's table leaves the other's.
    call_in(
        &svc,
        "us-east-1",
        "DeleteTable",
        json!({"TableName": "Orders"}),
    )
    .await;
    let (status, _) = call_in(
        &svc,
        "eu-west-1",
        "DescribeTable",
        json!({"TableName": "Orders"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // A read in a region nobody has used leaves no state behind.
    call_in(&svc, "ap-south-1", "ListTables", json!({})).await;
    call_in(
        &svc,
        "ap-south-1",
        "DescribeTable",
        json!({"TableName": "Orders"}),
    )
    .await;
    assert!(svc
        .state
        .read()
        .regional("123456789012", "ap-south-1")
        .is_none());
}

#[tokio::test]
async fn backups_and_streams_live_in_their_tables_region() {
    let svc = make_service();
    for region in ["us-east-1", "eu-west-1"] {
        call_in(&svc, region, "CreateTable", simple_table("Tbl")).await;
    }
    let (_, backup) = call_in(
        &svc,
        "eu-west-1",
        "CreateBackup",
        json!({"TableName": "Tbl", "BackupName": "b"}),
    )
    .await;
    let backup_arn = backup["BackupDetails"]["BackupArn"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(backup_arn.starts_with("arn:aws:dynamodb:eu-west-1:123456789012:table/Tbl/backup/"));
    let (_, east) = call_in(&svc, "us-east-1", "ListBackups", json!({})).await;
    assert_eq!(east["BackupSummaries"], json!([]));
    let (_, west) = call_in(&svc, "eu-west-1", "ListBackups", json!({})).await;
    assert_eq!(west["BackupSummaries"].as_array().unwrap().len(), 1);
    let (status, _) = call_in(
        &svc,
        "us-east-1",
        "DescribeBackup",
        json!({ "BackupArn": backup_arn }),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // Tags by ARN reach the ARN's table only.
    let west_arn = "arn:aws:dynamodb:eu-west-1:123456789012:table/Tbl";
    call_in(
        &svc,
        "eu-west-1",
        "TagResource",
        json!({"ResourceArn": west_arn, "Tags": [{"Key": "r", "Value": "west"}]}),
    )
    .await;
    let (_, tags) = call_in(
        &svc,
        "us-east-1",
        "ListTagsOfResource",
        json!({"ResourceArn": "arn:aws:dynamodb:us-east-1:123456789012:table/Tbl"}),
    )
    .await;
    assert_eq!(tags["Tags"], json!([]));
    let (_, tags) = call_in(
        &svc,
        "eu-west-1",
        "ListTagsOfResource",
        json!({"ResourceArn": west_arn}),
    )
    .await;
    assert_eq!(tags["Tags"], json!([{"Key": "r", "Value": "west"}]));

    // The DynamoDB Streams data plane sees each region's streams.
    let streams = crate::DynamoDbStreamsService::new(svc.state.clone());
    let mut req = make_request("ListStreams", json!({}));
    req.service = "dynamodbstreams".into();
    req.region = "eu-west-1".into();
    let resp = streams.handle(req).await.unwrap();
    let body: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    let arns: Vec<&str> = body["Streams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["StreamArn"].as_str().unwrap())
        .collect();
    assert_eq!(arns.len(), 1);
    assert!(arns[0].starts_with(west_arn));
}

#[tokio::test]
async fn replica_updates_create_a_live_replica_in_the_other_region() {
    let svc = make_service();
    call_in(&svc, "us-east-1", "CreateTable", simple_table("Glob")).await;
    call_in(
        &svc,
        "us-east-1",
        "PutItem",
        json!({"TableName": "Glob", "Item": {"pk": {"S": "seed"}}}),
    )
    .await;
    let (status, body) = call_in(
        &svc,
        "us-east-1",
        "UpdateTable",
        json!({"TableName": "Glob", "ReplicaUpdates": [{"Create": {"RegionName": "eu-west-1"}}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["TableDescription"]["GlobalTableVersion"], "2019.11.21");
    assert_eq!(
        body["TableDescription"]["Replicas"],
        json!([{"RegionName": "eu-west-1", "ReplicaStatus": "ACTIVE"}])
    );

    // The replica is a table of eu-west-1, with the source's rows.
    let (status, d) = call_in(
        &svc,
        "eu-west-1",
        "DescribeTable",
        json!({"TableName": "Glob"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        d["Table"]["TableArn"],
        "arn:aws:dynamodb:eu-west-1:123456789012:table/Glob"
    );
    assert_eq!(
        d["Table"]["Replicas"],
        json!([{"RegionName": "us-east-1", "ReplicaStatus": "ACTIVE"}])
    );
    assert_eq!(d["Table"]["ItemCount"], 1);
    let (_, list) = call_in(&svc, "eu-west-1", "ListTables", json!({})).await;
    assert_eq!(list["TableNames"], json!(["Glob"]));

    // Writes in either region reach the other.
    call_in(
        &svc,
        "eu-west-1",
        "PutItem",
        json!({"TableName": "Glob", "Item": {"pk": {"S": "w"}, "v": {"S": "from-west"}}}),
    )
    .await;
    let (_, got) = call_in(
        &svc,
        "us-east-1",
        "GetItem",
        json!({"TableName": "Glob", "Key": {"pk": {"S": "w"}}}),
    )
    .await;
    assert_eq!(got["Item"]["v"]["S"], "from-west");
    call_in(
        &svc,
        "us-east-1",
        "BatchWriteItem",
        json!({"RequestItems": {"Glob": [{"DeleteRequest": {"Key": {"pk": {"S": "seed"}}}}]}}),
    )
    .await;
    let (_, got) = call_in(
        &svc,
        "eu-west-1",
        "GetItem",
        json!({"TableName": "Glob", "Key": {"pk": {"S": "seed"}}}),
    )
    .await;
    assert!(got.get("Item").is_none(), "{got}");

    // Creating it again, or in the table's own region, is refused.
    for region in ["eu-west-1", "us-east-1"] {
        let (status, err) = call_in(
            &svc,
            "us-east-1",
            "UpdateTable",
            json!({"TableName": "Glob", "ReplicaUpdates": [{"Create": {"RegionName": region}}]}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{region}");
        assert_eq!(err["__type"], "ValidationException");
    }

    // Removing the replica deletes the eu-west-1 table.
    let (status, body) = call_in(
        &svc,
        "us-east-1",
        "UpdateTable",
        json!({"TableName": "Glob", "ReplicaUpdates": [{"Delete": {"RegionName": "eu-west-1"}}]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["TableDescription"].get("Replicas").is_none());
    let (status, _) = call_in(
        &svc,
        "eu-west-1",
        "DescribeTable",
        json!({"TableName": "Glob"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn legacy_global_tables_are_visible_from_each_replica_region() {
    let svc = make_service();
    for region in ["us-east-1", "eu-west-1"] {
        call_in(&svc, region, "CreateTable", simple_table("Legacy")).await;
    }
    let (status, body) = call_in(
        &svc,
        "us-east-1",
        "CreateGlobalTable",
        json!({"GlobalTableName": "Legacy", "ReplicationGroup": [
            {"RegionName": "us-east-1"}, {"RegionName": "eu-west-1"}
        ]}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (status, d) = call_in(
        &svc,
        "eu-west-1",
        "DescribeGlobalTable",
        json!({"GlobalTableName": "Legacy"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(d["GlobalTableDescription"]["GlobalTableName"], "Legacy");
    let (status, _) = call_in(
        &svc,
        "ap-south-1",
        "DescribeGlobalTable",
        json!({"GlobalTableName": "Legacy"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // A second CreateGlobalTable from another member region is a duplicate.
    let (status, err) = call_in(
        &svc,
        "eu-west-1",
        "CreateGlobalTable",
        json!({"GlobalTableName": "Legacy", "ReplicationGroup": [{"RegionName": "eu-west-1"}]}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(err["__type"], "GlobalTableAlreadyExistsException");
    let (_, list) = call_in(
        &svc,
        "eu-west-1",
        "ListGlobalTables",
        json!({"RegionName": "ap-south-1"}),
    )
    .await;
    assert_eq!(list["GlobalTables"], json!([]));
    let (_, list) = call_in(
        &svc,
        "eu-west-1",
        "ListGlobalTables",
        json!({"RegionName": "eu-west-1"}),
    )
    .await;
    assert_eq!(list["GlobalTables"].as_array().unwrap().len(), 1);

    // A write to one member table replicates to the other.
    call_in(
        &svc,
        "eu-west-1",
        "PutItem",
        json!({"TableName": "Legacy", "Item": {"pk": {"S": "x"}}}),
    )
    .await;
    let (_, got) = call_in(
        &svc,
        "us-east-1",
        "GetItem",
        json!({"TableName": "Legacy", "Key": {"pk": {"S": "x"}}}),
    )
    .await;
    assert_eq!(got["Item"]["pk"]["S"], "x");
}

#[tokio::test]
async fn regional_state_survives_a_snapshot_round_trip() {
    let store = Arc::new(RecordingSnapshotStore::default());
    let svc = make_service().with_snapshot_store(store.clone());
    for region in ["us-east-1", "eu-west-1"] {
        call_in(&svc, region, "CreateTable", simple_table("Snap")).await;
        call_in(
            &svc,
            region,
            "PutItem",
            json!({"TableName": "Snap", "Item": {"pk": {"S": region}}}),
        )
        .await;
    }
    // A read-only visit to another region is not persisted.
    call_in(
        &svc,
        "ap-south-1",
        "DeleteTable",
        json!({"TableName": "Snap"}),
    )
    .await;
    let bytes = load_recorded_snapshot(&store);
    let restored = service_from_snapshot_bytes(&bytes);
    for region in ["us-east-1", "eu-west-1"] {
        let (_, got) = call_in(
            &restored,
            region,
            "GetItem",
            json!({"TableName": "Snap", "Key": {"pk": {"S": region}}}),
        )
        .await;
        assert_eq!(got["Item"]["pk"]["S"], region);
    }
    let snapshot: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(snapshot["schema_version"], 3);
    let regions = snapshot["accounts"]["accounts"]["123456789012"]["regions"]
        .as_object()
        .unwrap();
    assert!(regions.contains_key("eu-west-1"));
    assert!(!regions.contains_key("ap-south-1"));
}

#[tokio::test]
async fn global_table_settings_follow_to_every_replica() {
    let svc = make_service();
    call_in(
        &svc,
        "us-east-1",
        "CreateTable",
        json!({
            "TableName": "Synced",
            "KeySchema": [{ "AttributeName": "pk", "KeyType": "HASH" }],
            "AttributeDefinitions": [{ "AttributeName": "pk", "AttributeType": "S" }],
            "BillingMode": "PAY_PER_REQUEST",
        }),
    )
    .await;
    call_in(
        &svc,
        "us-east-1",
        "UpdateTable",
        json!({"TableName": "Synced", "ReplicaUpdates": [{"Create": {"RegionName": "eu-west-1"}}]}),
    )
    .await;
    let (status, body) = call_in(
        &svc,
        "us-east-1",
        "UpdateTable",
        json!({
            "TableName": "Synced",
            "AttributeDefinitions": [
                { "AttributeName": "pk", "AttributeType": "S" },
                { "AttributeName": "g", "AttributeType": "S" }
            ],
            "GlobalSecondaryIndexUpdates": [{"Create": {
                "IndexName": "by-g",
                "KeySchema": [{ "AttributeName": "g", "KeyType": "HASH" }],
                "Projection": { "ProjectionType": "ALL" }
            }}],
            "StreamSpecification": { "StreamEnabled": true, "StreamViewType": "NEW_AND_OLD_IMAGES" },
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    call_in(
        &svc,
        "eu-west-1",
        "UpdateTimeToLive",
        json!({"TableName": "Synced", "TimeToLiveSpecification": {"AttributeName": "exp", "Enabled": true}}),
    )
    .await;

    let (_, west) = call_in(
        &svc,
        "eu-west-1",
        "DescribeTable",
        json!({"TableName": "Synced"}),
    )
    .await;
    let west = &west["Table"];
    assert_eq!(west["GlobalSecondaryIndexes"][0]["IndexName"], "by-g");
    assert_eq!(
        west["StreamSpecification"]["StreamViewType"],
        "NEW_AND_OLD_IMAGES"
    );
    assert!(west["LatestStreamArn"]
        .as_str()
        .unwrap()
        .starts_with("arn:aws:dynamodb:eu-west-1:123456789012:table/Synced/stream/"));
    // TTL set from the replica reaches the original table.
    let (_, ttl) = call_in(
        &svc,
        "us-east-1",
        "DescribeTimeToLive",
        json!({"TableName": "Synced"}),
    )
    .await;
    assert_eq!(ttl["TimeToLiveDescription"]["TimeToLiveStatus"], "ENABLED");
    assert_eq!(ttl["TimeToLiveDescription"]["AttributeName"], "exp");
    // The index serves queries on the replica, over replicated rows.
    call_in(
        &svc,
        "us-east-1",
        "PutItem",
        json!({"TableName": "Synced", "Item": {"pk": {"S": "1"}, "g": {"S": "x"}}}),
    )
    .await;
    let (status, q) = call_in(
        &svc,
        "eu-west-1",
        "Query",
        json!({
            "TableName": "Synced",
            "IndexName": "by-g",
            "KeyConditionExpression": "g = :g",
            "ExpressionAttributeValues": {":g": {"S": "x"}}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{q}");
    assert_eq!(q["Count"], 1);
}

#[tokio::test]
async fn point_in_time_recovery_is_per_region() {
    let svc = make_service();
    for region in ["us-east-1", "eu-west-1"] {
        call_in(&svc, region, "CreateTable", simple_table("Pitr")).await;
        call_in(
            &svc,
            region,
            "PutItem",
            json!({"TableName": "Pitr", "Item": {"pk": {"S": region}}}),
        )
        .await;
    }
    let (status, body) = call_in(
        &svc,
        "eu-west-1",
        "UpdateContinuousBackups",
        json!({"TableName": "Pitr", "PointInTimeRecoverySpecification": {"PointInTimeRecoveryEnabled": true}}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, east) = call_in(
        &svc,
        "us-east-1",
        "DescribeContinuousBackups",
        json!({"TableName": "Pitr"}),
    )
    .await;
    assert_eq!(
        east["ContinuousBackupsDescription"]["PointInTimeRecoveryDescription"]
            ["PointInTimeRecoveryStatus"],
        "DISABLED"
    );
    call_in(
        &svc,
        "eu-west-1",
        "PutItem",
        json!({"TableName": "Pitr", "Item": {"pk": {"S": "later"}}}),
    )
    .await;
    {
        let accounts = svc.state.read();
        let west = &accounts
            .regional("123456789012", "eu-west-1")
            .unwrap()
            .tables["Pitr"];
        assert_eq!(west.pitr_history.changes.len(), 1);
        let east = &accounts
            .regional("123456789012", "us-east-1")
            .unwrap()
            .tables["Pitr"];
        assert!(east.pitr_history.changes.is_empty());
    }
    let (status, body) = call_in(
        &svc,
        "eu-west-1",
        "RestoreTableToPointInTime",
        json!({"SourceTableName": "Pitr", "TargetTableName": "PitrRestored", "UseLatestRestorableTime": true}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body["TableDescription"]["TableArn"]
        .as_str()
        .unwrap()
        .starts_with("arn:aws:dynamodb:eu-west-1:"));
    let (_, got) = call_in(
        &svc,
        "eu-west-1",
        "GetItem",
        json!({"TableName": "PitrRestored", "Key": {"pk": {"S": "eu-west-1"}}}),
    )
    .await;
    assert_eq!(got["Item"]["pk"]["S"], "eu-west-1");
    let (status, _) = call_in(
        &svc,
        "us-east-1",
        "DescribeTable",
        json!({"TableName": "PitrRestored"}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

// ---- Service Quotas: maximum number of tables ----

fn quota_service(limit: f64) -> DynamoDbService {
    make_service().with_quota_provider(Some(Arc::new(
        fakecloud_core::quota::FixedQuotas::default().with("dynamodb", "L-F98FE922", limit),
    )))
}

fn assert_table_limit(err: AwsServiceError, limit: usize) {
    assert_eq!(err.status(), StatusCode::BAD_REQUEST);
    assert_eq!(err.code(), "LimitExceededException");
    assert_eq!(
        err.message(),
        format!("Subscriber limit exceeded: There is a limit of {limit} tables per subscriber")
    );
}

#[test]
fn table_quota_is_not_enforced_without_a_provider() {
    let svc = make_service();
    for i in 0..3 {
        svc.create_table(&make_request(
            "CreateTable",
            simple_table(&format!("tbl{i}")),
        ))
        .unwrap();
    }
}

#[test]
fn table_quota_refuses_creates_and_restores_past_the_limit() {
    let svc = quota_service(2.0);
    svc.create_table(&make_request("CreateTable", simple_table("tbl1")))
        .unwrap();
    svc.create_table(&make_request("CreateTable", simple_table("tbl2")))
        .unwrap();
    assert_table_limit(
        svc.create_table(&make_request("CreateTable", simple_table("tbl3")))
            .err()
            .unwrap(),
        2,
    );
    // An existing name still answers ResourceInUseException.
    assert_eq!(
        svc.create_table(&make_request("CreateTable", simple_table("tbl1")))
            .err()
            .unwrap()
            .code(),
        "ResourceInUseException"
    );

    // Restores create tables too.
    let backup = body_json(
        &svc.create_backup(&make_request(
            "CreateBackup",
            json!({"TableName": "tbl1", "BackupName": "b"}),
        ))
        .unwrap(),
    );
    let backup_arn = backup["BackupDetails"]["BackupArn"].as_str().unwrap();
    assert_table_limit(
        svc.restore_table_from_backup(&make_request(
            "RestoreTableFromBackup",
            json!({"BackupArn": backup_arn, "TargetTableName": "rst1"}),
        ))
        .err()
        .unwrap(),
        2,
    );
    assert_table_limit(
        svc.restore_table_to_point_in_time(&make_request(
            "RestoreTableToPointInTime",
            json!({"SourceTableName": "tbl1", "TargetTableName": "rst2"}),
        ))
        .err()
        .unwrap(),
        2,
    );

    // Deleting a table frees the slot.
    svc.delete_table(&make_request("DeleteTable", json!({"TableName": "tbl2"})))
        .unwrap();
    svc.restore_table_from_backup(&make_request(
        "RestoreTableFromBackup",
        json!({"BackupArn": backup_arn, "TargetTableName": "rst1"}),
    ))
    .unwrap();
}

#[test]
fn table_quota_is_per_region() {
    let svc = quota_service(1.0);
    svc.create_table(&make_request("CreateTable", simple_table("tbl1")))
        .unwrap();
    let mut west = make_request("CreateTable", simple_table("tbl2"));
    west.region = "us-west-2".to_string();
    svc.create_table(&west).unwrap();
}

#[tokio::test]
async fn table_quota_counts_a_new_replica_in_its_own_region() {
    let svc = quota_service(1.0);
    call_in(&svc, "us-east-1", "CreateTable", simple_table("Glob")).await;
    call_in(&svc, "eu-west-1", "CreateTable", simple_table("Other")).await;
    let (status, body) = call_in(
        &svc,
        "us-east-1",
        "UpdateTable",
        json!({"TableName": "Glob", "ReplicaUpdates": [{"Create": {"RegionName": "eu-west-1"}}]}),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert_eq!(body["__type"], "LimitExceededException");
    // Nothing changed: eu-west-1 still holds only its own table.
    let (_, list) = call_in(&svc, "eu-west-1", "ListTables", json!({})).await;
    assert_eq!(list["TableNames"], json!(["Other"]));
}

#[test]
fn table_usage_source_counts_tables_per_region() {
    use fakecloud_core::quota::QuotaUsageSource;
    let svc = make_service();
    svc.create_table(&make_request("CreateTable", simple_table("tbl1")))
        .unwrap();
    svc.create_table(&make_request("CreateTable", simple_table("tbl2")))
        .unwrap();
    let usage = crate::quota::DynamoDbQuotaUsage::new(svc.state.clone());
    assert_eq!(
        usage.usage("123456789012", "us-east-1", "dynamodb", "L-F98FE922"),
        Some(2.0)
    );
    assert_eq!(
        usage.usage("123456789012", "us-west-2", "dynamodb", "L-F98FE922"),
        Some(0.0)
    );
    assert_eq!(
        usage.usage("123456789012", "us-east-1", "dynamodb", "L-0000"),
        None
    );
}

#[test]
fn table_quota_refuses_an_import_into_a_new_table_past_the_limit() {
    let svc = quota_service(1.0);
    svc.create_table(&make_request("CreateTable", simple_table("tbl1")))
        .unwrap();
    let err = svc
        .import_table(&make_request(
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
        ))
        .err()
        .unwrap();
    assert_table_limit(err, 1);
}
