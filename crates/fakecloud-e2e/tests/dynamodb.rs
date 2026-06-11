mod helpers;

use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, ContributorInsightsAction, Delete,
    DeleteRequest, Get, GlobalSecondaryIndex, KeySchemaElement, KeyType, OnDemandThroughput,
    PointInTimeRecoverySpecification, Projection, ProjectionType, ProvisionedThroughput, Put,
    PutRequest, Replica, ScalarAttributeType, SseSpecification, SseType, StreamSpecification,
    StreamViewType, Tag, TimeToLiveSpecification, TransactGetItem, TransactWriteItem, WriteRequest,
};
use helpers::TestServer;
use std::collections::HashMap;

#[tokio::test]
async fn dynamodb_create_describe_delete_table() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("TestTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_table()
        .table_name("TestTable")
        .send()
        .await
        .unwrap();
    let table = resp.table().unwrap();
    assert_eq!(table.table_name().unwrap(), "TestTable");
    assert_eq!(table.table_status().unwrap().as_str(), "ACTIVE");
    assert!(table.table_arn().unwrap().contains("TestTable"));

    let resp = client.list_tables().send().await.unwrap();
    assert!(resp.table_names().contains(&"TestTable".to_string()));

    client
        .delete_table()
        .table_name("TestTable")
        .send()
        .await
        .unwrap();

    let result = client.describe_table().table_name("TestTable").send().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn dynamodb_create_table_with_range_key() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("CompositeTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("sk")
                .key_type(KeyType::Range)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("sk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .provisioned_throughput(
            ProvisionedThroughput::builder()
                .read_capacity_units(5)
                .write_capacity_units(5)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_table()
        .table_name("CompositeTable")
        .send()
        .await
        .unwrap();
    let table = resp.table().unwrap();
    assert_eq!(table.key_schema().len(), 2);
}

#[tokio::test]
async fn dynamodb_put_get_delete_item() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("Items")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    client
        .put_item()
        .table_name("Items")
        .item("id", AttributeValue::S("item1".to_string()))
        .item("name", AttributeValue::S("Widget".to_string()))
        .item("count", AttributeValue::N("42".to_string()))
        .send()
        .await
        .unwrap();

    let resp = client
        .get_item()
        .table_name("Items")
        .key("id", AttributeValue::S("item1".to_string()))
        .send()
        .await
        .unwrap();

    let item = resp.item().unwrap();
    assert_eq!(item.get("name").unwrap().as_s().unwrap(), "Widget");
    assert_eq!(item.get("count").unwrap().as_n().unwrap(), "42");

    client
        .delete_item()
        .table_name("Items")
        .key("id", AttributeValue::S("item1".to_string()))
        .send()
        .await
        .unwrap();

    let resp = client
        .get_item()
        .table_name("Items")
        .key("id", AttributeValue::S("item1".to_string()))
        .send()
        .await
        .unwrap();
    assert!(resp.item().is_none());
}

#[tokio::test]
async fn dynamodb_update_item() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("Updates")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    client
        .put_item()
        .table_name("Updates")
        .item("id", AttributeValue::S("u1".to_string()))
        .item("count", AttributeValue::N("10".to_string()))
        .send()
        .await
        .unwrap();

    client
        .update_item()
        .table_name("Updates")
        .key("id", AttributeValue::S("u1".to_string()))
        .update_expression("SET #c = :newval")
        .expression_attribute_names("#c", "count")
        .expression_attribute_values(":newval", AttributeValue::N("20".to_string()))
        .send()
        .await
        .unwrap();

    let resp = client
        .get_item()
        .table_name("Updates")
        .key("id", AttributeValue::S("u1".to_string()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.item().unwrap().get("count").unwrap().as_n().unwrap(),
        "20"
    );
}

#[tokio::test]
async fn dynamodb_query_by_partition_key() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("Orders")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("userId")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("orderId")
                .key_type(KeyType::Range)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("userId")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("orderId")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    for i in 1..=3 {
        client
            .put_item()
            .table_name("Orders")
            .item("userId", AttributeValue::S("user1".to_string()))
            .item("orderId", AttributeValue::S(format!("order{i}")))
            .item("total", AttributeValue::N(format!("{}", i * 10)))
            .send()
            .await
            .unwrap();
    }
    client
        .put_item()
        .table_name("Orders")
        .item("userId", AttributeValue::S("user2".to_string()))
        .item("orderId", AttributeValue::S("order1".to_string()))
        .item("total", AttributeValue::N("99".to_string()))
        .send()
        .await
        .unwrap();

    let resp = client
        .query()
        .table_name("Orders")
        .key_condition_expression("userId = :uid")
        .expression_attribute_values(":uid", AttributeValue::S("user1".to_string()))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 3);
    let items = resp.items();
    assert_eq!(items[0].get("orderId").unwrap().as_s().unwrap(), "order1");
    assert_eq!(items[2].get("orderId").unwrap().as_s().unwrap(), "order3");
}

#[tokio::test]
async fn dynamodb_scan() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("ScanTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    for i in 1..=5 {
        client
            .put_item()
            .table_name("ScanTable")
            .item("id", AttributeValue::S(format!("item{i}")))
            .item("value", AttributeValue::N(format!("{i}")))
            .send()
            .await
            .unwrap();
    }

    let resp = client.scan().table_name("ScanTable").send().await.unwrap();

    assert_eq!(resp.count(), 5);
    assert_eq!(resp.scanned_count(), 5);
}

#[tokio::test]
async fn dynamodb_scan_with_index_name_applies_projection() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    // Table with a GSI on 'category' projecting only KEYS_ONLY.
    client
        .create_table()
        .table_name("ScanIndexTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("category")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name("ByCategory")
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("category")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::KeysOnly)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    client
        .put_item()
        .table_name("ScanIndexTable")
        .item("id", AttributeValue::S("a".into()))
        .item("category", AttributeValue::S("books".into()))
        .item("title", AttributeValue::S("Rust".into()))
        .send()
        .await
        .unwrap();

    let resp = client
        .scan()
        .table_name("ScanIndexTable")
        .index_name("ByCategory")
        .send()
        .await
        .unwrap();

    let items = resp.items();
    assert_eq!(items.len(), 1);
    let item = &items[0];
    // KEYS_ONLY projection: table PK ('id') + index PK ('category'); 'title' must be absent.
    assert!(item.contains_key("id"));
    assert!(item.contains_key("category"));
    assert!(!item.contains_key("title"));
}

#[tokio::test]
async fn dynamodb_scan_with_unknown_index_name_errors() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;
    client
        .create_table()
        .table_name("NoIdxTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();
    let err = client
        .scan()
        .table_name("NoIdxTable")
        .index_name("DoesNotExist")
        .send()
        .await
        .expect_err("unknown index must fail");
    assert!(format!("{err:?}").contains("DoesNotExist"));
}

#[tokio::test]
async fn dynamodb_scan_with_filter() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("FilterTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    for i in 1..=10 {
        client
            .put_item()
            .table_name("FilterTable")
            .item("id", AttributeValue::S(format!("item{i}")))
            .item("score", AttributeValue::N(format!("{}", i * 10)))
            .send()
            .await
            .unwrap();
    }

    let resp = client
        .scan()
        .table_name("FilterTable")
        .filter_expression("score > :min")
        .expression_attribute_values(":min", AttributeValue::N("50".to_string()))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 5);
}

#[tokio::test]
async fn dynamodb_batch_write_and_get() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("BatchTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    let mut item1 = HashMap::new();
    item1.insert("id".to_string(), AttributeValue::S("b1".to_string()));
    item1.insert("data".to_string(), AttributeValue::S("first".to_string()));
    let mut item2 = HashMap::new();
    item2.insert("id".to_string(), AttributeValue::S("b2".to_string()));
    item2.insert("data".to_string(), AttributeValue::S("second".to_string()));
    let mut item3 = HashMap::new();
    item3.insert("id".to_string(), AttributeValue::S("b3".to_string()));
    item3.insert("data".to_string(), AttributeValue::S("third".to_string()));

    let mut items = HashMap::new();
    items.insert(
        "BatchTable".to_string(),
        vec![
            WriteRequest::builder()
                .put_request(PutRequest::builder().set_item(Some(item1)).build().unwrap())
                .build(),
            WriteRequest::builder()
                .put_request(PutRequest::builder().set_item(Some(item2)).build().unwrap())
                .build(),
            WriteRequest::builder()
                .put_request(PutRequest::builder().set_item(Some(item3)).build().unwrap())
                .build(),
        ],
    );

    client
        .batch_write_item()
        .set_request_items(Some(items))
        .send()
        .await
        .unwrap();

    let mut key1 = HashMap::new();
    key1.insert("id".to_string(), AttributeValue::S("b1".to_string()));
    let mut key2 = HashMap::new();
    key2.insert("id".to_string(), AttributeValue::S("b3".to_string()));

    let mut keys_to_get = HashMap::new();
    keys_to_get.insert(
        "BatchTable".to_string(),
        aws_sdk_dynamodb::types::KeysAndAttributes::builder()
            .keys(key1)
            .keys(key2)
            .build()
            .unwrap(),
    );

    let resp = client
        .batch_get_item()
        .set_request_items(Some(keys_to_get))
        .send()
        .await
        .unwrap();

    let responses = resp.responses().unwrap();
    let batch_results = responses.get("BatchTable").unwrap();
    assert_eq!(batch_results.len(), 2);

    let mut del_key = HashMap::new();
    del_key.insert("id".to_string(), AttributeValue::S("b2".to_string()));
    let mut del_items = HashMap::new();
    del_items.insert(
        "BatchTable".to_string(),
        vec![WriteRequest::builder()
            .delete_request(
                DeleteRequest::builder()
                    .set_key(Some(del_key))
                    .build()
                    .unwrap(),
            )
            .build()],
    );

    client
        .batch_write_item()
        .set_request_items(Some(del_items))
        .send()
        .await
        .unwrap();

    let resp = client.scan().table_name("BatchTable").send().await.unwrap();
    assert_eq!(resp.count(), 2);
}

#[tokio::test]
async fn dynamodb_tags() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    let create_resp = client
        .create_table()
        .table_name("TagTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    let arn = create_resp
        .table_description()
        .unwrap()
        .table_arn()
        .unwrap()
        .to_string();

    client
        .tag_resource()
        .resource_arn(&arn)
        .tags(Tag::builder().key("env").value("test").build().unwrap())
        .tags(
            Tag::builder()
                .key("project")
                .value("fakecloud")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .list_tags_of_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tags().len(), 2);

    client
        .untag_resource()
        .resource_arn(&arn)
        .tag_keys("env")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_tags_of_resource()
        .resource_arn(&arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tags().len(), 1);
    assert_eq!(resp.tags()[0].key(), "project");
}

#[tokio::test]
async fn dynamodb_condition_expression() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("CondTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    client
        .put_item()
        .table_name("CondTable")
        .item("id", AttributeValue::S("c1".to_string()))
        .item("data", AttributeValue::S("original".to_string()))
        .condition_expression("attribute_not_exists(id)")
        .send()
        .await
        .unwrap();

    let result = client
        .put_item()
        .table_name("CondTable")
        .item("id", AttributeValue::S("c1".to_string()))
        .item("data", AttributeValue::S("duplicate".to_string()))
        .condition_expression("attribute_not_exists(id)")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn dynamodb_condition_expression_not_attribute_exists() {
    // python_dynamodb_lock acquires with `NOT(attribute_exists(...))` (no space).
    // On a missing key this must succeed; on an existing key it must fail with
    // ConditionalCheckFailedException.
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("NotCondTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Missing key: NOT(attribute_exists(id)) => NOT false => true => succeeds.
    client
        .put_item()
        .table_name("NotCondTable")
        .item("id", AttributeValue::S("lock".to_string()))
        .condition_expression("NOT(attribute_exists(id))")
        .send()
        .await
        .unwrap();

    // Now the key exists: same condition => NOT true => false => fails.
    let result = client
        .put_item()
        .table_name("NotCondTable")
        .item("id", AttributeValue::S("lock".to_string()))
        .condition_expression("NOT(attribute_exists(id))")
        .send()
        .await;
    let err =
        result.expect_err("put on existing key must fail the NOT(attribute_exists) condition");
    assert!(
        err.into_service_error()
            .is_conditional_check_failed_exception(),
        "expected ConditionalCheckFailedException"
    );
}

#[tokio::test]
async fn dynamodb_nested_projection_on_list_element() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("NestedProj")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Put item with a list of maps
    client
        .put_item()
        .table_name("NestedProj")
        .item("pk", AttributeValue::S("k1".to_string()))
        .item(
            "people",
            AttributeValue::L(vec![
                AttributeValue::M(HashMap::from([
                    ("name".to_string(), AttributeValue::S("Alice".to_string())),
                    ("age".to_string(), AttributeValue::N("30".to_string())),
                ])),
                AttributeValue::M(HashMap::from([
                    ("name".to_string(), AttributeValue::S("Bob".to_string())),
                    ("age".to_string(), AttributeValue::N("25".to_string())),
                ])),
            ]),
        )
        .send()
        .await
        .unwrap();

    // Project only people[0].name — should NOT return the whole element
    let resp = client
        .get_item()
        .table_name("NestedProj")
        .key("pk", AttributeValue::S("k1".to_string()))
        .projection_expression("people[0].#n")
        .expression_attribute_names("#n", "name")
        .send()
        .await
        .unwrap();

    let item = resp.item().unwrap();
    let people = item.get("people").unwrap().as_l().unwrap();
    let first = people[0].as_m().unwrap();
    // Should have "name"
    assert_eq!(
        first.get("name").unwrap().as_s().unwrap(),
        "Alice",
        "projected name should be Alice"
    );
    // Should NOT have "age" (that was the bug: returning entire element)
    assert!(
        first.get("age").is_none(),
        "age should not be present in projection of people[0].name"
    );
}

#[tokio::test]
async fn dynamodb_filter_with_parenthesized_and_or() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("ParenFilter")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Item 1: color=red, size=large
    client
        .put_item()
        .table_name("ParenFilter")
        .item("pk", AttributeValue::S("i1".to_string()))
        .item("color", AttributeValue::S("red".to_string()))
        .item("size", AttributeValue::S("large".to_string()))
        .send()
        .await
        .unwrap();

    // Item 2: color=blue, size=small
    client
        .put_item()
        .table_name("ParenFilter")
        .item("pk", AttributeValue::S("i2".to_string()))
        .item("color", AttributeValue::S("blue".to_string()))
        .item("size", AttributeValue::S("small".to_string()))
        .send()
        .await
        .unwrap();

    // Item 3: color=red, size=small, premium=yes
    client
        .put_item()
        .table_name("ParenFilter")
        .item("pk", AttributeValue::S("i3".to_string()))
        .item("color", AttributeValue::S("red".to_string()))
        .item("size", AttributeValue::S("small".to_string()))
        .item("premium", AttributeValue::S("yes".to_string()))
        .send()
        .await
        .unwrap();

    // Filter: (color = red AND size = large) OR premium = yes
    // Should match i1 (red+large) and i3 (premium=yes), not i2
    let resp = client
        .scan()
        .table_name("ParenFilter")
        .filter_expression("(color = :red AND #s = :large) OR premium = :yes")
        .expression_attribute_names("#s", "size")
        .expression_attribute_values(":red", AttributeValue::S("red".to_string()))
        .expression_attribute_values(":large", AttributeValue::S("large".to_string()))
        .expression_attribute_values(":yes", AttributeValue::S("yes".to_string()))
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.count(),
        2,
        "should match 2 items: (red AND large) OR premium=yes"
    );
}

#[tokio::test]
async fn dynamodb_transact_get_items() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("TransactGet")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Put two items
    client
        .put_item()
        .table_name("TransactGet")
        .item("pk", AttributeValue::S("a".to_string()))
        .item("val", AttributeValue::S("alpha".to_string()))
        .send()
        .await
        .unwrap();

    client
        .put_item()
        .table_name("TransactGet")
        .item("pk", AttributeValue::S("b".to_string()))
        .item("val", AttributeValue::S("beta".to_string()))
        .send()
        .await
        .unwrap();

    // TransactGetItems for both + a missing one
    let resp = client
        .transact_get_items()
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name("TransactGet")
                        .key("pk", AttributeValue::S("a".to_string()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name("TransactGet")
                        .key("pk", AttributeValue::S("b".to_string()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactGetItem::builder()
                .get(
                    Get::builder()
                        .table_name("TransactGet")
                        .key("pk", AttributeValue::S("missing".to_string()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let responses = resp.responses();
    assert_eq!(responses.len(), 3);
    let first_item = responses[0].item().unwrap();
    assert_eq!(first_item.get("val").unwrap().as_s().unwrap(), "alpha");
    let second_item = responses[1].item().unwrap();
    assert_eq!(second_item.get("val").unwrap().as_s().unwrap(), "beta");
    // Third should be empty (missing item)
    assert!(responses[2].item().is_none());
}

#[tokio::test]
async fn dynamodb_transact_write_items() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("TransactWrite")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Put an item to delete later
    client
        .put_item()
        .table_name("TransactWrite")
        .item("pk", AttributeValue::S("to-delete".to_string()))
        .item("val", AttributeValue::S("bye".to_string()))
        .send()
        .await
        .unwrap();

    // TransactWriteItems: put new + delete existing
    client
        .transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name("TransactWrite")
                        .item("pk", AttributeValue::S("new-item".to_string()))
                        .item("val", AttributeValue::S("hello".to_string()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactWriteItem::builder()
                .delete(
                    Delete::builder()
                        .table_name("TransactWrite")
                        .key("pk", AttributeValue::S("to-delete".to_string()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Verify new item exists
    let resp = client
        .get_item()
        .table_name("TransactWrite")
        .key("pk", AttributeValue::S("new-item".to_string()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.item().unwrap().get("val").unwrap().as_s().unwrap(),
        "hello"
    );

    // Verify deleted item is gone
    let resp = client
        .get_item()
        .table_name("TransactWrite")
        .key("pk", AttributeValue::S("to-delete".to_string()))
        .send()
        .await
        .unwrap();
    assert!(resp.item().is_none());
}

/// TransactWriteItems must be all-or-nothing: when a later operation
/// fails its ConditionExpression, no earlier writes commit. The SDK
/// surfaces failures via `TransactionCanceledException` whose
/// `CancellationReasons` array aligns 1:1 with the input
/// `TransactItems` and tags each entry with `None` or
/// `ConditionalCheckFailed`.
#[tokio::test]
async fn dynamodb_transact_write_items_atomic_rollback_on_condition_failure() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("TransactRollback")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Two-op transaction: a Put that would succeed, followed by a
    // ConditionCheck guaranteed to fail. The whole transaction must be
    // rejected and the first Put must NOT commit.
    let err = client
        .transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name("TransactRollback")
                        .item("pk", AttributeValue::S("first".to_string()))
                        .item("v", AttributeValue::S("written".to_string()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactWriteItem::builder()
                .condition_check(
                    aws_sdk_dynamodb::types::ConditionCheck::builder()
                        .table_name("TransactRollback")
                        .key("pk", AttributeValue::S("never-existed".to_string()))
                        .condition_expression("attribute_exists(pk)")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect_err("transaction must fail");

    let svc = err.into_service_error();
    let cancelled = match svc {
        aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError::TransactionCanceledException(e) => e,
        other => panic!("expected TransactionCanceledException, got {other:?}"),
    };
    let reasons = cancelled.cancellation_reasons();
    assert_eq!(reasons.len(), 2, "one reason per TransactItem");
    assert_eq!(reasons[0].code(), Some("None"));
    assert_eq!(reasons[1].code(), Some("ConditionalCheckFailed"));

    // Verify the first Put did NOT commit.
    let resp = client
        .get_item()
        .table_name("TransactRollback")
        .key("pk", AttributeValue::S("first".to_string()))
        .send()
        .await
        .unwrap();
    assert!(
        resp.item().is_none(),
        "TransactWriteItems must be all-or-nothing — the first Put must not have committed"
    );
}

/// When a Put/Update/Delete carries
/// `ReturnValuesOnConditionCheckFailure=ALL_OLD` and its
/// `ConditionExpression` rejects the request, the offending item
/// surfaces back through `CancellationReasons[i].Item` so SDK callers
/// can branch on the existing state without an extra round-trip.
#[tokio::test]
async fn dynamodb_transact_write_items_returns_old_item_on_condition_failure() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("TransactReturnOld")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Seed an existing item.
    client
        .put_item()
        .table_name("TransactReturnOld")
        .item("pk", AttributeValue::S("k".to_string()))
        .item("v", AttributeValue::S("old-value".to_string()))
        .send()
        .await
        .unwrap();

    // attribute_not_exists guard fails because the item is already
    // there; ALL_OLD asks fakecloud to attach the live item.
    let err = client
        .transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name("TransactReturnOld")
                        .item("pk", AttributeValue::S("k".to_string()))
                        .item("v", AttributeValue::S("new-value".to_string()))
                        .condition_expression("attribute_not_exists(pk)")
                        .return_values_on_condition_check_failure(
                            aws_sdk_dynamodb::types::ReturnValuesOnConditionCheckFailure::AllOld,
                        )
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect_err("attribute_not_exists must fail");

    let cancelled = match err.into_service_error() {
        aws_sdk_dynamodb::operation::transact_write_items::TransactWriteItemsError::TransactionCanceledException(e) => e,
        other => panic!("expected TransactionCanceledException, got {other:?}"),
    };
    let reason = &cancelled.cancellation_reasons()[0];
    assert_eq!(reason.code(), Some("ConditionalCheckFailed"));
    let item = reason
        .item()
        .expect("ALL_OLD must surface the existing item");
    assert_eq!(
        item.get("v").unwrap().as_s().unwrap(),
        "old-value",
        "the surfaced item must reflect the live state pre-transaction"
    );
}

/// Each successful Put/Update/Delete inside a TransactWriteItems must
/// emit a DynamoDB Streams record so consumers (Lambda
/// EventSourceMapping, Kinesis adapters, change-data-capture
/// pipelines) see the same event-stream regardless of whether the
/// write came in via PutItem or via a transaction.
#[tokio::test]
async fn dynamodb_transact_write_items_emits_stream_records() {
    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let streams = server.dynamodb_streams_client().await;

    let table_name = "TransactStreams";
    ddb.create_table()
        .table_name(table_name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewAndOldImages)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let table = ddb
        .describe_table()
        .table_name(table_name)
        .send()
        .await
        .unwrap();
    let stream_arn = table
        .table()
        .unwrap()
        .latest_stream_arn()
        .unwrap()
        .to_string();

    // Seed a row so we can both Put a new key and Delete an existing
    // key inside the same transaction.
    ddb.put_item()
        .table_name(table_name)
        .item("pk", AttributeValue::S("seed".to_string()))
        .send()
        .await
        .unwrap();

    ddb.transact_write_items()
        .transact_items(
            TransactWriteItem::builder()
                .put(
                    Put::builder()
                        .table_name(table_name)
                        .item("pk", AttributeValue::S("inserted".to_string()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .transact_items(
            TransactWriteItem::builder()
                .delete(
                    Delete::builder()
                        .table_name(table_name)
                        .key("pk", AttributeValue::S("seed".to_string()))
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let desc = streams
        .describe_stream()
        .stream_arn(&stream_arn)
        .send()
        .await
        .unwrap();
    let shard_id = desc
        .stream_description()
        .unwrap()
        .shards()
        .first()
        .unwrap()
        .shard_id()
        .unwrap()
        .to_string();
    let it = streams
        .get_shard_iterator()
        .stream_arn(&stream_arn)
        .shard_id(&shard_id)
        .shard_iterator_type(aws_sdk_dynamodbstreams::types::ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap();
    let records = streams
        .get_records()
        .shard_iterator(it.shard_iterator().unwrap())
        .send()
        .await
        .unwrap();
    let r = records.records();

    // Seed PutItem -> 1 INSERT, transaction -> 1 INSERT + 1 REMOVE = 3.
    assert_eq!(
        r.len(),
        3,
        "one stream record per write, including each transact-item"
    );
    assert_eq!(r[0].event_name().unwrap().as_str(), "INSERT"); // seed
    assert_eq!(r[1].event_name().unwrap().as_str(), "INSERT"); // transact Put
    assert_eq!(r[2].event_name().unwrap().as_str(), "REMOVE"); // transact Delete
}

#[tokio::test]
async fn dynamodb_ttl_lifecycle() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("TtlTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Enable TTL
    let resp = client
        .update_time_to_live()
        .table_name("TtlTable")
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .attribute_name("ttl")
                .enabled(true)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let spec = resp.time_to_live_specification().unwrap();
    assert_eq!(spec.attribute_name(), "ttl");
    assert!(spec.enabled());

    // Describe TTL
    let resp = client
        .describe_time_to_live()
        .table_name("TtlTable")
        .send()
        .await
        .unwrap();

    let desc = resp.time_to_live_description().unwrap();
    assert_eq!(desc.time_to_live_status().unwrap().as_str(), "ENABLED");
    assert_eq!(desc.attribute_name().unwrap(), "ttl");

    // Disable TTL
    client
        .update_time_to_live()
        .table_name("TtlTable")
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .attribute_name("ttl")
                .enabled(false)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_time_to_live()
        .table_name("TtlTable")
        .send()
        .await
        .unwrap();

    let desc = resp.time_to_live_description().unwrap();
    assert_eq!(desc.time_to_live_status().unwrap().as_str(), "DISABLED");
}

#[tokio::test]
async fn dynamodb_resource_policy_lifecycle() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("PolicyTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Get the table ARN
    let desc = client
        .describe_table()
        .table_name("PolicyTable")
        .send()
        .await
        .unwrap();
    let table_arn = desc.table().unwrap().table_arn().unwrap().to_string();

    let policy_doc = r#"{"Version":"2012-10-17","Statement":[]}"#;

    // Put resource policy
    let resp = client
        .put_resource_policy()
        .resource_arn(&table_arn)
        .policy(policy_doc)
        .send()
        .await
        .unwrap();
    assert!(resp.revision_id().is_some());

    // Get resource policy
    let resp = client
        .get_resource_policy()
        .resource_arn(&table_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.policy().unwrap(), policy_doc);

    // Delete resource policy
    client
        .delete_resource_policy()
        .resource_arn(&table_arn)
        .send()
        .await
        .unwrap();

    // Get should now error with PolicyNotFoundException, matching real DynamoDB.
    let err = client
        .get_resource_policy()
        .resource_arn(&table_arn)
        .send()
        .await
        .expect_err("GetResourcePolicy after delete must error");
    assert!(format!("{err:?}").contains("PolicyNotFound"));
}

#[tokio::test]
async fn dynamodb_describe_endpoints() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    let resp = client.describe_endpoints().send().await.unwrap();
    let endpoints = resp.endpoints();
    assert!(!endpoints.is_empty());
    assert_eq!(endpoints[0].cache_period_in_minutes(), 1440);
}

#[tokio::test]
async fn dynamodb_describe_limits() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    let resp = client.describe_limits().send().await.unwrap();
    assert_eq!(resp.table_max_read_capacity_units().unwrap(), 40000);
    assert_eq!(resp.table_max_write_capacity_units().unwrap(), 40000);
}

#[tokio::test]
async fn dynamodb_backup_lifecycle() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("BackupTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Create backup
    let resp = client
        .create_backup()
        .table_name("BackupTable")
        .backup_name("test-backup")
        .send()
        .await
        .unwrap();
    let backup_arn = resp.backup_details().unwrap().backup_arn().to_string();

    // List backups
    let resp = client.list_backups().send().await.unwrap();
    assert!(!resp.backup_summaries().is_empty());

    // Describe backup
    let resp = client
        .describe_backup()
        .backup_arn(&backup_arn)
        .send()
        .await
        .unwrap();
    assert!(resp.backup_description().is_some());

    // Restore from backup
    client
        .restore_table_from_backup()
        .target_table_name("RestoredFromBackup")
        .backup_arn(&backup_arn)
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_table()
        .table_name("RestoredFromBackup")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.table().unwrap().table_status().unwrap().as_str(),
        "ACTIVE"
    );

    // Delete backup
    client
        .delete_backup()
        .backup_arn(&backup_arn)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn dynamodb_continuous_backups() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("PITRTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Enable PITR
    client
        .update_continuous_backups()
        .table_name("PITRTable")
        .point_in_time_recovery_specification(
            PointInTimeRecoverySpecification::builder()
                .point_in_time_recovery_enabled(true)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Describe
    let resp = client
        .describe_continuous_backups()
        .table_name("PITRTable")
        .send()
        .await
        .unwrap();
    let desc = resp.continuous_backups_description().unwrap();
    assert_eq!(desc.continuous_backups_status().as_str(), "ENABLED");
}

#[tokio::test]
async fn dynamodb_restore_table_to_point_in_time() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("SourceTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    client
        .restore_table_to_point_in_time()
        .source_table_name("SourceTable")
        .target_table_name("PITRRestored")
        .use_latest_restorable_time(true)
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_table()
        .table_name("PITRRestored")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.table().unwrap().table_status().unwrap().as_str(),
        "ACTIVE"
    );
}

#[tokio::test]
async fn dynamodb_contributor_insights() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("CITable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Enable
    client
        .update_contributor_insights()
        .table_name("CITable")
        .contributor_insights_action(ContributorInsightsAction::Enable)
        .send()
        .await
        .unwrap();

    // Describe
    let resp = client
        .describe_contributor_insights()
        .table_name("CITable")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.contributor_insights_status().unwrap().as_str(),
        "ENABLED"
    );

    // List
    let resp = client.list_contributor_insights().send().await.unwrap();
    assert!(!resp.contributor_insights_summaries().is_empty());
}

#[tokio::test]
async fn dynamodb_kinesis_streaming_destination() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("KinesisTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    let stream_arn = "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream";

    // Enable
    let resp = client
        .enable_kinesis_streaming_destination()
        .table_name("KinesisTable")
        .stream_arn(stream_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.destination_status().unwrap().as_str(), "ACTIVE");

    // Describe
    let resp = client
        .describe_kinesis_streaming_destination()
        .table_name("KinesisTable")
        .send()
        .await
        .unwrap();
    assert!(!resp.kinesis_data_stream_destinations().is_empty());

    // Disable
    let resp = client
        .disable_kinesis_streaming_destination()
        .table_name("KinesisTable")
        .stream_arn(stream_arn)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.destination_status().unwrap().as_str(), "DISABLED");
}

#[tokio::test]
async fn dynamodb_backup_restore_preserves_data() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    // Create table
    client
        .create_table()
        .table_name("BackupTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Put 3 items
    for i in 1..=3 {
        client
            .put_item()
            .table_name("BackupTable")
            .item("pk", AttributeValue::S(format!("key{i}")))
            .item("data", AttributeValue::S(format!("value{i}")))
            .send()
            .await
            .unwrap();
    }

    // Create backup
    let backup_resp = client
        .create_backup()
        .table_name("BackupTable")
        .backup_name("my-backup")
        .send()
        .await
        .unwrap();
    let backup_arn = backup_resp
        .backup_details()
        .unwrap()
        .backup_arn()
        .to_string();

    // Delete all items from original table
    for i in 1..=3 {
        client
            .delete_item()
            .table_name("BackupTable")
            .key("pk", AttributeValue::S(format!("key{i}")))
            .send()
            .await
            .unwrap();
    }

    // Verify original table is empty
    let scan = client
        .scan()
        .table_name("BackupTable")
        .send()
        .await
        .unwrap();
    assert_eq!(scan.count(), 0);

    // Restore from backup
    client
        .restore_table_from_backup()
        .backup_arn(&backup_arn)
        .target_table_name("RestoredTable")
        .send()
        .await
        .unwrap();

    // Scan restored table — should have 3 items
    let scan = client
        .scan()
        .table_name("RestoredTable")
        .send()
        .await
        .unwrap();
    assert_eq!(scan.count(), 3);
    assert_eq!(scan.items().len(), 3);
}

#[tokio::test]
async fn dynamodb_restore_to_point_in_time_preserves_data() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    // Create table
    client
        .create_table()
        .table_name("PitrTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Put 3 items
    for i in 1..=3 {
        client
            .put_item()
            .table_name("PitrTable")
            .item("pk", AttributeValue::S(format!("key{i}")))
            .item("data", AttributeValue::S(format!("value{i}")))
            .send()
            .await
            .unwrap();
    }

    // Enable PITR
    client
        .update_continuous_backups()
        .table_name("PitrTable")
        .point_in_time_recovery_specification(
            PointInTimeRecoverySpecification::builder()
                .point_in_time_recovery_enabled(true)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Restore to point in time
    client
        .restore_table_to_point_in_time()
        .source_table_name("PitrTable")
        .target_table_name("PitrRestored")
        .use_latest_restorable_time(true)
        .send()
        .await
        .unwrap();

    // Scan restored table — should have 3 items
    let scan = client
        .scan()
        .table_name("PitrRestored")
        .send()
        .await
        .unwrap();
    assert_eq!(scan.count(), 3);
    assert_eq!(scan.items().len(), 3);
}

#[tokio::test]
async fn dynamodb_global_table_replicates_writes() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    // Create the table first
    client
        .create_table()
        .table_name("GlobalTestTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Create global table with replicas
    client
        .create_global_table()
        .global_table_name("GlobalTestTable")
        .replication_group(Replica::builder().region_name("us-east-1").build())
        .replication_group(Replica::builder().region_name("eu-west-1").build())
        .send()
        .await
        .unwrap();

    // Write an item
    client
        .put_item()
        .table_name("GlobalTestTable")
        .item("pk", AttributeValue::S("global-key".to_string()))
        .item("data", AttributeValue::S("global-value".to_string()))
        .send()
        .await
        .unwrap();

    // Read it back (all replicas share the same table in fakecloud)
    let resp = client
        .get_item()
        .table_name("GlobalTestTable")
        .key("pk", AttributeValue::S("global-key".to_string()))
        .send()
        .await
        .unwrap();

    let item = resp.item().unwrap();
    assert_eq!(item.get("pk").unwrap().as_s().unwrap(), "global-key");
    assert_eq!(item.get("data").unwrap().as_s().unwrap(), "global-value");

    // Verify the global table is described correctly
    let gt = client
        .describe_global_table()
        .global_table_name("GlobalTestTable")
        .send()
        .await
        .unwrap();
    let desc = gt.global_table_description().unwrap();
    assert_eq!(desc.replication_group().len(), 2);
}

#[tokio::test]
async fn dynamodb_contributor_insights_tracks_access() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    // Create table
    client
        .create_table()
        .table_name("InsightsTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Enable contributor insights
    client
        .update_contributor_insights()
        .table_name("InsightsTable")
        .contributor_insights_action(ContributorInsightsAction::Enable)
        .send()
        .await
        .unwrap();

    // Put items with different partition keys
    for key in &["alpha", "beta", "alpha", "gamma", "alpha"] {
        client
            .put_item()
            .table_name("InsightsTable")
            .item("pk", AttributeValue::S(key.to_string()))
            .item("data", AttributeValue::S("value".to_string()))
            .send()
            .await
            .unwrap();
    }

    // Get items to also track read access
    for _ in 0..2 {
        client
            .get_item()
            .table_name("InsightsTable")
            .key("pk", AttributeValue::S("beta".to_string()))
            .send()
            .await
            .unwrap();
    }

    // Describe contributor insights
    let resp = client
        .describe_contributor_insights()
        .table_name("InsightsTable")
        .send()
        .await
        .unwrap();

    assert_eq!(
        resp.contributor_insights_status().unwrap().as_str(),
        "ENABLED"
    );

    // Verify that rules list is non-empty
    let rules = resp.contributor_insights_rule_list();
    assert!(
        !rules.is_empty(),
        "ContributorInsightsRuleList should not be empty"
    );
}

#[tokio::test]
async fn dynamodb_scan_pagination() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("ScanPagTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    for i in 0..5 {
        client
            .put_item()
            .table_name("ScanPagTable")
            .item("pk", AttributeValue::S(format!("item{i}")))
            .item("data", AttributeValue::S(format!("value{i}")))
            .send()
            .await
            .unwrap();
    }

    // Scan with limit=2: should return 2 items and LastEvaluatedKey
    let resp = client
        .scan()
        .table_name("ScanPagTable")
        .limit(2)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.count(), 2);
    let lek = resp
        .last_evaluated_key()
        .expect("should have LastEvaluatedKey");
    assert!(lek.contains_key("pk"), "LastEvaluatedKey should contain pk");

    // Page through all items using ExclusiveStartKey
    let mut all_items = resp.items().to_vec();
    let mut start_key = Some(lek.clone());

    while let Some(ref sk) = start_key {
        let resp = client
            .scan()
            .table_name("ScanPagTable")
            .limit(2)
            .set_exclusive_start_key(Some(sk.clone()))
            .send()
            .await
            .unwrap();
        all_items.extend(resp.items().to_vec());
        start_key = resp.last_evaluated_key().map(|m| m.to_owned());
    }

    assert_eq!(all_items.len(), 5, "should have retrieved all 5 items");

    let mut pks: Vec<String> = all_items
        .iter()
        .map(|item| item["pk"].as_s().unwrap().clone())
        .collect();
    pks.sort();
    pks.dedup();
    assert_eq!(pks.len(), 5, "all items should be unique");
}

#[tokio::test]
async fn dynamodb_scan_no_pagination_when_all_fit() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("ScanNoPagTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    for i in 0..3 {
        client
            .put_item()
            .table_name("ScanNoPagTable")
            .item("pk", AttributeValue::S(format!("item{i}")))
            .send()
            .await
            .unwrap();
    }

    // Scan with limit > item count: no LastEvaluatedKey
    let resp = client
        .scan()
        .table_name("ScanNoPagTable")
        .limit(10)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.count(), 3);
    assert!(
        resp.last_evaluated_key().is_none(),
        "LastEvaluatedKey should be absent when all items fit"
    );

    // Scan without limit: no LastEvaluatedKey
    let resp = client
        .scan()
        .table_name("ScanNoPagTable")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.count(), 3);
    assert!(
        resp.last_evaluated_key().is_none(),
        "LastEvaluatedKey should be absent without limit"
    );
}

#[tokio::test]
async fn dynamodb_query_pagination() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("QueryPagTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("sk")
                .key_type(KeyType::Range)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("sk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    for i in 0..5 {
        client
            .put_item()
            .table_name("QueryPagTable")
            .item("pk", AttributeValue::S("user1".to_string()))
            .item("sk", AttributeValue::S(format!("item{i:03}")))
            .item("data", AttributeValue::S(format!("value{i}")))
            .send()
            .await
            .unwrap();
    }

    // Query with limit=2
    let resp = client
        .query()
        .table_name("QueryPagTable")
        .key_condition_expression("pk = :pk")
        .expression_attribute_values(":pk", AttributeValue::S("user1".to_string()))
        .limit(2)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.count(), 2);
    let lek = resp
        .last_evaluated_key()
        .expect("should have LastEvaluatedKey");
    assert!(lek.contains_key("pk"), "LastEvaluatedKey should contain pk");
    assert!(lek.contains_key("sk"), "LastEvaluatedKey should contain sk");

    // Page through all items
    let mut all_items = resp.items().to_vec();
    let mut start_key = Some(lek.clone());

    while let Some(ref sk) = start_key {
        let resp = client
            .query()
            .table_name("QueryPagTable")
            .key_condition_expression("pk = :pk")
            .expression_attribute_values(":pk", AttributeValue::S("user1".to_string()))
            .limit(2)
            .set_exclusive_start_key(Some(sk.clone()))
            .send()
            .await
            .unwrap();
        all_items.extend(resp.items().to_vec());
        start_key = resp.last_evaluated_key().map(|m| m.to_owned());
    }

    assert_eq!(all_items.len(), 5, "should have retrieved all 5 items");

    // Verify items came back sorted by sort key
    let sks: Vec<String> = all_items
        .iter()
        .map(|item| item["sk"].as_s().unwrap().clone())
        .collect();
    let mut sorted_sks = sks.clone();
    sorted_sks.sort();
    assert_eq!(sks, sorted_sks, "items should be sorted by sort key");
}

#[tokio::test]
async fn dynamodb_query_no_pagination_when_all_fit() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("QueryNoPagTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("sk")
                .key_type(KeyType::Range)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("sk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    for i in 0..2 {
        client
            .put_item()
            .table_name("QueryNoPagTable")
            .item("pk", AttributeValue::S("user1".to_string()))
            .item("sk", AttributeValue::S(format!("item{i}")))
            .send()
            .await
            .unwrap();
    }

    let resp = client
        .query()
        .table_name("QueryNoPagTable")
        .key_condition_expression("pk = :pk")
        .expression_attribute_values(":pk", AttributeValue::S("user1".to_string()))
        .limit(10)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.count(), 2);
    assert!(
        resp.last_evaluated_key().is_none(),
        "LastEvaluatedKey should be absent when all items fit"
    );
}

#[tokio::test]
async fn dynamodb_gsi_query_pagination() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    // Create a table with a GSI
    client
        .create_table()
        .table_name("GsiPagTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("gsi_pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("gsi_sk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name("gsi-index")
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("gsi_pk")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("gsi_sk")
                        .key_type(KeyType::Range)
                        .build()
                        .unwrap(),
                )
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::All)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Insert 4 items with the same GSI key but different table PKs
    for i in 0..4 {
        client
            .put_item()
            .table_name("GsiPagTable")
            .item("pk", AttributeValue::S(format!("item{i:03}")))
            .item("gsi_pk", AttributeValue::S("shared".to_string()))
            .item("gsi_sk", AttributeValue::S("sort".to_string()))
            .send()
            .await
            .unwrap();
    }

    // First page: query GSI with limit=2
    let resp = client
        .query()
        .table_name("GsiPagTable")
        .index_name("gsi-index")
        .key_condition_expression("gsi_pk = :v")
        .expression_attribute_values(":v", AttributeValue::S("shared".to_string()))
        .limit(2)
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 2);
    let lek = resp
        .last_evaluated_key()
        .expect("should have LastEvaluatedKey");
    // LEK must include both index keys AND table PK
    assert!(
        lek.contains_key("gsi_pk"),
        "LastEvaluatedKey should contain gsi_pk"
    );
    assert!(
        lek.contains_key("gsi_sk"),
        "LastEvaluatedKey should contain gsi_sk"
    );
    assert!(
        lek.contains_key("pk"),
        "LastEvaluatedKey should contain table PK for GSI queries"
    );

    // Paginate through all items and collect PKs
    let mut all_pks: Vec<String> = resp
        .items()
        .iter()
        .map(|item| item["pk"].as_s().unwrap().clone())
        .collect();

    let mut exclusive_start_key: Option<HashMap<String, AttributeValue>> =
        resp.last_evaluated_key().map(|m| m.to_owned());

    while exclusive_start_key.is_some() {
        let mut q = client
            .query()
            .table_name("GsiPagTable")
            .index_name("gsi-index")
            .key_condition_expression("gsi_pk = :v")
            .expression_attribute_values(":v", AttributeValue::S("shared".to_string()))
            .limit(2);

        for (k, v) in exclusive_start_key.as_ref().unwrap() {
            q = q.exclusive_start_key(k.clone(), v.clone());
        }

        let resp = q.send().await.unwrap();

        for item in resp.items() {
            all_pks.push(item["pk"].as_s().unwrap().clone());
        }

        exclusive_start_key = resp.last_evaluated_key().map(|m| m.to_owned());
    }

    all_pks.sort();
    assert_eq!(
        all_pks,
        vec!["item000", "item001", "item002", "item003"],
        "GSI pagination should return all items without duplicates"
    );
}

/// TTL processor simulation: expired items are deleted, future items remain.
#[tokio::test]
async fn dynamodb_ttl_processor_tick() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    // Create table
    client
        .create_table()
        .table_name("TtlTickTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Enable TTL on "ttl" attribute
    client
        .update_time_to_live()
        .table_name("TtlTickTable")
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .attribute_name("ttl")
                .enabled(true)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Put item with TTL far in the past (epoch 0)
    client
        .put_item()
        .table_name("TtlTickTable")
        .item("pk", AttributeValue::S("expired".to_string()))
        .item("ttl", AttributeValue::N("0".to_string()))
        .send()
        .await
        .unwrap();

    // Put item with TTL far in the future (year ~2100)
    client
        .put_item()
        .table_name("TtlTickTable")
        .item("pk", AttributeValue::S("future".to_string()))
        .item("ttl", AttributeValue::N("4102444800".to_string()))
        .send()
        .await
        .unwrap();

    // Put item without TTL attribute
    client
        .put_item()
        .table_name("TtlTickTable")
        .item("pk", AttributeValue::S("no-ttl".to_string()))
        .send()
        .await
        .unwrap();

    // Call the TTL processor tick endpoint
    let http = reqwest::Client::new();
    let resp = http
        .post(format!(
            "{}/_fakecloud/dynamodb/ttl-processor/tick",
            server.endpoint()
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(
        body["expiredItems"], 1,
        "one expired item should be deleted"
    );

    // Verify the expired item is gone
    let resp = client
        .get_item()
        .table_name("TtlTickTable")
        .key("pk", AttributeValue::S("expired".to_string()))
        .send()
        .await
        .unwrap();
    assert!(resp.item().is_none(), "expired item should be deleted");

    // Verify the future item still exists
    let resp = client
        .get_item()
        .table_name("TtlTickTable")
        .key("pk", AttributeValue::S("future".to_string()))
        .send()
        .await
        .unwrap();
    assert!(resp.item().is_some(), "future item should still exist");

    // Verify the no-ttl item still exists
    let resp = client
        .get_item()
        .table_name("TtlTickTable")
        .key("pk", AttributeValue::S("no-ttl".to_string()))
        .send()
        .await
        .unwrap();
    assert!(
        resp.item().is_some(),
        "item without TTL attribute should still exist"
    );

    // Second tick should find nothing to expire
    let resp = http
        .post(format!(
            "{}/_fakecloud/dynamodb/ttl-processor/tick",
            server.endpoint()
        ))
        .send()
        .await
        .unwrap();
    let body: serde_json::Value = resp.json().await.unwrap();
    assert_eq!(body["expiredItems"], 0, "no more items to expire");
}

#[tokio::test]
async fn dynamodb_sse_specification_kms() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    let kms_key_id = "arn:aws:kms:us-east-1:123456789012:key/test-key-id";

    // Create table with KMS SSE
    client
        .create_table()
        .table_name("SseTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .sse_specification(
            SseSpecification::builder()
                .enabled(true)
                .sse_type(SseType::Kms)
                .kms_master_key_id(kms_key_id)
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Describe table and verify SSEDescription
    let desc = client
        .describe_table()
        .table_name("SseTable")
        .send()
        .await
        .unwrap();
    let table = desc.table().unwrap();
    let sse = table.sse_description().unwrap();
    assert_eq!(sse.status().unwrap().as_str(), "ENABLED");
    assert_eq!(sse.sse_type().unwrap().as_str(), "KMS");
    assert_eq!(sse.kms_master_key_arn().unwrap(), kms_key_id);
}

#[tokio::test]
async fn dynamodb_sse_default_omitted() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    // A table without an explicit SSE spec uses the default AWS-owned key.
    // Real AWS DescribeTable omits SSEDescription entirely in that case, and
    // the Terraform provider enforces this (server_side_encryption.# == 0).
    client
        .create_table()
        .table_name("DefaultSseTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_table()
        .table_name("DefaultSseTable")
        .send()
        .await
        .unwrap();
    let table = desc.table().unwrap();
    assert!(table.sse_description().is_none());
}

#[tokio::test]
async fn dynamodb_kinesis_streaming_delivers_records() {
    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let kinesis = server.kinesis_client().await;

    // Create Kinesis stream
    kinesis
        .create_stream()
        .stream_name("ddb-changes")
        .shard_count(1)
        .send()
        .await
        .unwrap();

    // Create DynamoDB table
    ddb.create_table()
        .table_name("StreamedTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Enable Kinesis streaming destination
    let stream_arn = "arn:aws:kinesis:us-east-1:123456789012:stream/ddb-changes";
    ddb.enable_kinesis_streaming_destination()
        .table_name("StreamedTable")
        .stream_arn(stream_arn)
        .send()
        .await
        .unwrap();

    // Get a shard iterator (TRIM_HORIZON to read from beginning)
    let desc = kinesis
        .describe_stream()
        .stream_name("ddb-changes")
        .send()
        .await
        .unwrap();
    let shard_id = desc
        .stream_description()
        .unwrap()
        .shards()
        .first()
        .unwrap()
        .shard_id()
        .to_string();

    let iter_resp = kinesis
        .get_shard_iterator()
        .stream_name("ddb-changes")
        .shard_id(&shard_id)
        .shard_iterator_type(aws_sdk_kinesis::types::ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap();
    let shard_iterator = iter_resp.shard_iterator().unwrap().to_string();

    // Put an item into DynamoDB
    ddb.put_item()
        .table_name("StreamedTable")
        .item("pk", AttributeValue::S("item1".to_string()))
        .item("data", AttributeValue::S("hello".to_string()))
        .send()
        .await
        .unwrap();

    // Delete the item
    ddb.delete_item()
        .table_name("StreamedTable")
        .key("pk", AttributeValue::S("item1".to_string()))
        .send()
        .await
        .unwrap();

    // Read records from Kinesis
    let records_resp = kinesis
        .get_records()
        .shard_iterator(&shard_iterator)
        .send()
        .await
        .unwrap();

    let records = records_resp.records();
    assert!(
        records.len() >= 2,
        "expected at least 2 Kinesis records (INSERT + REMOVE), got {}",
        records.len()
    );

    // Parse first record (INSERT) — data is raw JSON bytes
    let insert_event: serde_json::Value =
        serde_json::from_slice(records[0].data().as_ref()).unwrap();
    assert_eq!(insert_event["eventName"], "INSERT");
    assert_eq!(insert_event["eventSource"], "aws:dynamodb");
    assert!(insert_event["dynamodb"]["NewImage"].is_object());

    // Parse second record (REMOVE)
    let remove_event: serde_json::Value =
        serde_json::from_slice(records[1].data().as_ref()).unwrap();
    assert_eq!(remove_event["eventName"], "REMOVE");
    assert!(remove_event["dynamodb"]["OldImage"].is_object());
}

#[tokio::test]
async fn dynamodb_deletion_protection_blocks_delete_table() {
    // Regression guard for `TestAccDynamoDBTable_deletion_protection`:
    // CreateTable accepts `DeletionProtectionEnabled`, DescribeTable
    // returns it, UpdateTable can toggle it, and DeleteTable refuses
    // with `ResourceInUseException` while it's enabled.
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("Protected")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .deletion_protection_enabled(true)
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_table()
        .table_name("Protected")
        .send()
        .await
        .unwrap();
    assert_eq!(
        desc.table().unwrap().deletion_protection_enabled(),
        Some(true)
    );

    let err = client.delete_table().table_name("Protected").send().await;
    assert!(err.is_err(), "DeleteTable should refuse while protected");

    // Disable protection via UpdateTable; delete now succeeds.
    client
        .update_table()
        .table_name("Protected")
        .deletion_protection_enabled(false)
        .send()
        .await
        .unwrap();
    client
        .delete_table()
        .table_name("Protected")
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn dynamodb_update_table_processes_global_secondary_index_updates() {
    // Regression guard for `TestAccDynamoDBTable_gsiUpdateCapacity`:
    // UpdateTable must process `GlobalSecondaryIndexUpdates` to add new
    // GSIs (Create), change capacity (Update), and drop GSIs (Delete).
    // Real AWS supports all three op types; previously fakecloud silently
    // ignored every entry in the list.
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("GsiUpdates")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("att1")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::Provisioned)
        .provisioned_throughput(
            ProvisionedThroughput::builder()
                .read_capacity_units(1)
                .write_capacity_units(1)
                .build()
                .unwrap(),
        )
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name("att1-index")
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("att1")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::All)
                        .build(),
                )
                .provisioned_throughput(
                    ProvisionedThroughput::builder()
                        .read_capacity_units(1)
                        .write_capacity_units(1)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    client
        .update_table()
        .table_name("GsiUpdates")
        .global_secondary_index_updates(
            aws_sdk_dynamodb::types::GlobalSecondaryIndexUpdate::builder()
                .update(
                    aws_sdk_dynamodb::types::UpdateGlobalSecondaryIndexAction::builder()
                        .index_name("att1-index")
                        .provisioned_throughput(
                            ProvisionedThroughput::builder()
                                .read_capacity_units(5)
                                .write_capacity_units(7)
                                .build()
                                .unwrap(),
                        )
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_table()
        .table_name("GsiUpdates")
        .send()
        .await
        .unwrap();
    let gsi = &desc.table().unwrap().global_secondary_indexes()[0];
    let pt = gsi.provisioned_throughput().unwrap();
    assert_eq!(pt.read_capacity_units(), Some(5));
    assert_eq!(pt.write_capacity_units(), Some(7));
}

// Regression guard for the "billing-mode transitions with GSI" gap that
// used to deny four `TestAccDynamoDBTable_BillingMode*` upstream tests.
// Real DynamoDB returns `{read: 0, write: 0}` for every GSI on a
// PAY_PER_REQUEST table, and the Terraform provider's flatten code keys
// `name`/`read_capacity`/`write_capacity` off the presence of that field.
#[tokio::test]
async fn dynamodb_create_pay_per_request_with_gsi_zeroes_throughput() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("PprGsi")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("gsipk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name("gsi1")
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("gsipk")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::KeysOnly)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_table()
        .table_name("PprGsi")
        .send()
        .await
        .unwrap();
    let table = desc.table().unwrap();
    let table_pt = table.provisioned_throughput().unwrap();
    assert_eq!(table_pt.read_capacity_units(), Some(0));
    assert_eq!(table_pt.write_capacity_units(), Some(0));
    let gsi = &table.global_secondary_indexes()[0];
    let pt = gsi.provisioned_throughput().unwrap();
    assert_eq!(pt.read_capacity_units(), Some(0));
    assert_eq!(pt.write_capacity_units(), Some(0));
}

#[tokio::test]
async fn dynamodb_update_table_provisioned_to_pay_per_request_zeroes_gsi() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("ProvToPpr")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("gsipk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::Provisioned)
        .provisioned_throughput(
            ProvisionedThroughput::builder()
                .read_capacity_units(3)
                .write_capacity_units(4)
                .build()
                .unwrap(),
        )
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name("gsi1")
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("gsipk")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::KeysOnly)
                        .build(),
                )
                .provisioned_throughput(
                    ProvisionedThroughput::builder()
                        .read_capacity_units(2)
                        .write_capacity_units(2)
                        .build()
                        .unwrap(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    client
        .update_table()
        .table_name("ProvToPpr")
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_table()
        .table_name("ProvToPpr")
        .send()
        .await
        .unwrap();
    let table = desc.table().unwrap();
    assert_eq!(
        table
            .billing_mode_summary()
            .unwrap()
            .billing_mode()
            .unwrap(),
        &BillingMode::PayPerRequest
    );
    let table_pt = table.provisioned_throughput().unwrap();
    assert_eq!(table_pt.read_capacity_units(), Some(0));
    assert_eq!(table_pt.write_capacity_units(), Some(0));
    let gsi = &table.global_secondary_indexes()[0];
    let pt = gsi.provisioned_throughput().unwrap();
    assert_eq!(pt.read_capacity_units(), Some(0));
    assert_eq!(pt.write_capacity_units(), Some(0));
}

#[tokio::test]
async fn dynamodb_update_table_gsi_create_on_pay_per_request_zeroes_throughput() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("PprAddGsi")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    client
        .update_table()
        .table_name("PprAddGsi")
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("gsipk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .global_secondary_index_updates(
            aws_sdk_dynamodb::types::GlobalSecondaryIndexUpdate::builder()
                .create(
                    aws_sdk_dynamodb::types::CreateGlobalSecondaryIndexAction::builder()
                        .index_name("gsi1")
                        .key_schema(
                            KeySchemaElement::builder()
                                .attribute_name("gsipk")
                                .key_type(KeyType::Hash)
                                .build()
                                .unwrap(),
                        )
                        .projection(
                            Projection::builder()
                                .projection_type(ProjectionType::KeysOnly)
                                .build(),
                        )
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_table()
        .table_name("PprAddGsi")
        .send()
        .await
        .unwrap();
    let table = desc.table().unwrap();
    let gsi = &table.global_secondary_indexes()[0];
    let pt = gsi.provisioned_throughput().unwrap();
    assert_eq!(pt.read_capacity_units(), Some(0));
    assert_eq!(pt.write_capacity_units(), Some(0));
    // AttributeDefinitions provided on UpdateTable must be merged into the
    // table schema, otherwise a follow-up DescribeTable would omit the new
    // GSI hash key and Terraform would replan the same update.
    assert!(table
        .attribute_definitions()
        .iter()
        .any(|a| a.attribute_name() == "gsipk"));
}

// Regression guard for `TestAccDynamoDBTable_onDemandThroughput`: the
// table-level `OnDemandThroughput` block must round-trip through
// CreateTable, DescribeTable, and UpdateTable. Real AWS echoes `-1` for
// an unset axis, and the Terraform provider asserts on the literal
// numeric values; fakecloud previously dropped the field entirely.
#[tokio::test]
async fn dynamodb_create_table_with_on_demand_throughput_round_trip() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("OdtBasic")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .on_demand_throughput(
            OnDemandThroughput::builder()
                .max_read_request_units(5)
                .max_write_request_units(5)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_table()
        .table_name("OdtBasic")
        .send()
        .await
        .unwrap();
    let odt = desc.table().unwrap().on_demand_throughput().unwrap();
    assert_eq!(odt.max_read_request_units(), Some(5));
    assert_eq!(odt.max_write_request_units(), Some(5));

    // Terraform's provider issues UpdateTable with a new OnDemandThroughput
    // block to bump the caps — both axes must be writable, and leaving one
    // axis as the `-1` sentinel must round-trip untouched.
    client
        .update_table()
        .table_name("OdtBasic")
        .on_demand_throughput(
            OnDemandThroughput::builder()
                .max_read_request_units(-1)
                .max_write_request_units(5)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_table()
        .table_name("OdtBasic")
        .send()
        .await
        .unwrap();
    let odt = desc.table().unwrap().on_demand_throughput().unwrap();
    assert_eq!(odt.max_read_request_units(), Some(-1));
    assert_eq!(odt.max_write_request_units(), Some(5));
}

// Regression guard for `TestAccDynamoDBTable_gsiOnDemandThroughput`:
// per-GSI `OnDemandThroughput` blocks must round-trip independently from
// the table-level block, including across an UpdateTable that targets a
// GSI via `GlobalSecondaryIndexUpdates`.
#[tokio::test]
async fn dynamodb_gsi_on_demand_throughput_round_trip() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("OdtGsi")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("att1")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .on_demand_throughput(
            OnDemandThroughput::builder()
                .max_read_request_units(10)
                .max_write_request_units(10)
                .build(),
        )
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name("att1-index")
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("att1")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::All)
                        .build(),
                )
                .on_demand_throughput(
                    OnDemandThroughput::builder()
                        .max_read_request_units(5)
                        .max_write_request_units(5)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_table()
        .table_name("OdtGsi")
        .send()
        .await
        .unwrap();
    let table = desc.table().unwrap();
    let table_odt = table.on_demand_throughput().unwrap();
    assert_eq!(table_odt.max_read_request_units(), Some(10));
    assert_eq!(table_odt.max_write_request_units(), Some(10));
    let gsi_odt = table.global_secondary_indexes()[0]
        .on_demand_throughput()
        .unwrap();
    assert_eq!(gsi_odt.max_read_request_units(), Some(5));
    assert_eq!(gsi_odt.max_write_request_units(), Some(5));

    // UpdateTable -> GlobalSecondaryIndexUpdates -> Update must bump the
    // per-GSI OnDemandThroughput independently of the table-level caps.
    client
        .update_table()
        .table_name("OdtGsi")
        .global_secondary_index_updates(
            aws_sdk_dynamodb::types::GlobalSecondaryIndexUpdate::builder()
                .update(
                    aws_sdk_dynamodb::types::UpdateGlobalSecondaryIndexAction::builder()
                        .index_name("att1-index")
                        .on_demand_throughput(
                            OnDemandThroughput::builder()
                                .max_read_request_units(20)
                                .max_write_request_units(20)
                                .build(),
                        )
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .unwrap();

    let desc = client
        .describe_table()
        .table_name("OdtGsi")
        .send()
        .await
        .unwrap();
    let gsi_odt = desc.table().unwrap().global_secondary_indexes()[0]
        .on_demand_throughput()
        .unwrap();
    assert_eq!(gsi_odt.max_read_request_units(), Some(20));
    assert_eq!(gsi_odt.max_write_request_units(), Some(20));
    // Table-level caps are untouched.
    let table_odt = desc.table().unwrap().on_demand_throughput().unwrap();
    assert_eq!(table_odt.max_read_request_units(), Some(10));
    assert_eq!(table_odt.max_write_request_units(), Some(10));
}

// `LatestStreamLabel` is a timestamp of the form `YYYY-MM-DDTHH:MM:SS.mmm`
// — real AWS uses millisecond precision, no timezone suffix, no extra
// separators. The upstream Terraform acceptance suite asserts on exactly
// this regex (`\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}`), so fakecloud
// must match byte-for-byte.
fn assert_stream_label_format(label: &str) {
    let (date, time) = label.split_once('T').expect("label has T separator");
    assert_eq!(date.len(), 10, "date prefix yyyy-mm-dd: {label}");
    let parts: Vec<&str> = date.split('-').collect();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0].len(), 4);
    assert_eq!(parts[1].len(), 2);
    assert_eq!(parts[2].len(), 2);
    let (hms, ms) = time.split_once('.').expect("label has fractional seconds");
    let hms_parts: Vec<&str> = hms.split(':').collect();
    assert_eq!(hms_parts.len(), 3);
    assert!(hms_parts.iter().all(|p| p.len() == 2));
    assert_eq!(ms.len(), 3, "millisecond precision only: {label}");
    assert!(label
        .chars()
        .all(|c| c.is_ascii_digit() || "-T:.".contains(c)));
}

#[tokio::test]
async fn dynamodb_stream_specification_lifecycle() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    // CreateTable with StreamSpecification must return the block plus
    // LatestStreamArn/LatestStreamLabel right from the create response —
    // Terraform's Read runs immediately after apply and asserts on both.
    let create = client
        .create_table()
        .table_name("StreamTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::KeysOnly)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let created = create.table_description().unwrap();
    let spec = created.stream_specification().unwrap();
    assert!(spec.stream_enabled());
    assert_eq!(spec.stream_view_type(), Some(&StreamViewType::KeysOnly));
    let arn_create = created.latest_stream_arn().unwrap();
    let label_create = created.latest_stream_label().unwrap();
    assert!(arn_create.ends_with(label_create));
    assert!(arn_create.contains(":table/StreamTable/stream/"));
    assert_stream_label_format(label_create);

    // DescribeTable returns the same shape.
    let described = client
        .describe_table()
        .table_name("StreamTable")
        .send()
        .await
        .unwrap();
    let table = described.table().unwrap();
    assert_eq!(
        table.stream_specification().unwrap().stream_view_type(),
        Some(&StreamViewType::KeysOnly),
    );
    assert_eq!(table.latest_stream_arn(), Some(arn_create));
    assert_eq!(table.latest_stream_label(), Some(label_create));

    // UpdateTable disabling streams clears StreamSpecification but keeps
    // LatestStreamArn/LatestStreamLabel — AWS retains the ARN post-disable
    // so Terraform's Read falls through to the previous stream_view_type.
    let upd = client
        .update_table()
        .table_name("StreamTable")
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(false)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let disabled = upd.table_description().unwrap();
    assert!(
        disabled.stream_specification().is_none(),
        "StreamSpecification must be omitted while disabled; got {:?}",
        disabled.stream_specification(),
    );
    assert_eq!(disabled.latest_stream_arn(), Some(arn_create));
    assert_eq!(disabled.latest_stream_label(), Some(label_create));

    // Re-enabling with a different view type mints a fresh stream ARN
    // (and therefore a fresh label) — the upstream `_diffs` test walks
    // through exactly this kind of disable→re-enable transition.
    let upd2 = client
        .update_table()
        .table_name("StreamTable")
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewImage)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let reenabled = upd2.table_description().unwrap();
    let spec2 = reenabled.stream_specification().unwrap();
    assert!(spec2.stream_enabled());
    assert_eq!(spec2.stream_view_type(), Some(&StreamViewType::NewImage));
    // The ARN is retained (AWS keeps the same stream ARN across toggles
    // as long as fakecloud's single-process lifetime holds it), which is
    // fine for the provider's Read — it just needs a non-empty label.
    let label2 = reenabled.latest_stream_label().unwrap();
    assert_stream_label_format(label2);
}

// End-to-end guard for the expression-evaluator bugs fixed in PR #660.
//
// The unit corpus at `fakecloud_dynamodb::expression_corpus_tests`
// exercises the grammar at the evaluator boundary. These two tests run the
// exact SDK-emitted shapes through serde + routing + evaluator, so any
// regression anywhere in that chain trips the suite.

/// Query with parenthesised KeyCondition clauses — the shape every
/// `aws-sdk-go-v2` KeyConditionBuilder emits (`(#0 = :0) AND (#1 > :1)`).
/// Before the fix this returned zero items; real DynamoDB returns all
/// matching rows.
#[tokio::test]
async fn dynamodb_query_paren_wrapped_key_condition() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("ParenOrders")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("store_id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("order_id")
                .key_type(KeyType::Range)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("store_id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("order_id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    for i in 1..=3 {
        client
            .put_item()
            .table_name("ParenOrders")
            .item("store_id", AttributeValue::S("s".to_string()))
            .item("order_id", AttributeValue::S(format!("order{i}")))
            .send()
            .await
            .unwrap();
    }

    // Baseline: unparenthesised expression returns all 3.
    let bare = client
        .query()
        .table_name("ParenOrders")
        .key_condition_expression("store_id = :s AND order_id > :a")
        .expression_attribute_values(":s", AttributeValue::S("s".to_string()))
        .expression_attribute_values(":a", AttributeValue::S("aaa".to_string()))
        .send()
        .await
        .unwrap();
    assert_eq!(bare.count(), 3, "bare KeyCondition baseline");

    // SDK-builder shape: each clause wrapped in parens. Must match baseline.
    let paren = client
        .query()
        .table_name("ParenOrders")
        .key_condition_expression("(store_id = :s) AND (order_id > :a)")
        .expression_attribute_values(":s", AttributeValue::S("s".to_string()))
        .expression_attribute_values(":a", AttributeValue::S("aaa".to_string()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        paren.count(),
        3,
        "parenthesised KeyCondition must return same rows as bare"
    );

    // Placeholder shape the SDK actually emits on the wire.
    let placeholder = client
        .query()
        .table_name("ParenOrders")
        .key_condition_expression("(#0 = :0) AND (#1 > :1)")
        .expression_attribute_names("#0", "store_id")
        .expression_attribute_names("#1", "order_id")
        .expression_attribute_values(":0", AttributeValue::S("s".to_string()))
        .expression_attribute_values(":1", AttributeValue::S("aaa".to_string()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        placeholder.count(),
        3,
        "SDK placeholder-shape KeyCondition must return same rows"
    );
}

/// UpdateItem with a dotted-path SET target — `SET #a.#b = :v`. Before the
/// fix this silently created a top-level `"#a.#b"` attribute instead of
/// updating the nested map; sibling keys under the parent were preserved by
/// accident. The guard here covers both: nested write lands, siblings stay.
#[tokio::test]
async fn dynamodb_update_nested_set_path() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("NestedSet")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("id")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("id")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    let mut initial_web = HashMap::new();
    initial_web.insert(
        "tab_id".to_string(),
        AttributeValue::S("old-tab".to_string()),
    );
    initial_web.insert(
        "keep_me".to_string(),
        AttributeValue::S("sibling".to_string()),
    );

    client
        .put_item()
        .table_name("NestedSet")
        .item("id", AttributeValue::S("row1".to_string()))
        .item("web", AttributeValue::M(initial_web))
        .send()
        .await
        .unwrap();

    client
        .update_item()
        .table_name("NestedSet")
        .key("id", AttributeValue::S("row1".to_string()))
        .update_expression("SET #web.#tab_id = :tab")
        .expression_attribute_names("#web", "web")
        .expression_attribute_names("#tab_id", "tab_id")
        .expression_attribute_values(":tab", AttributeValue::S("new-tab".to_string()))
        .send()
        .await
        .unwrap();

    let resp = client
        .get_item()
        .table_name("NestedSet")
        .key("id", AttributeValue::S("row1".to_string()))
        .send()
        .await
        .unwrap();
    let item = resp.item().unwrap();

    let web = item.get("web").unwrap().as_m().unwrap();
    assert_eq!(
        web.get("tab_id").unwrap().as_s().unwrap(),
        "new-tab",
        "nested SET must update the child key in place"
    );
    assert_eq!(
        web.get("keep_me").unwrap().as_s().unwrap(),
        "sibling",
        "nested SET must leave sibling keys untouched"
    );
    assert!(
        item.get("#web.#tab_id").is_none(),
        "nested SET must not leak a literal dotted-name top-level attribute"
    );
}

#[tokio::test]
async fn query_consistent_read_on_gsi_rejected() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("ConsistentGsi")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("gsi_pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .global_secondary_indexes(
            GlobalSecondaryIndex::builder()
                .index_name("gsi-idx")
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("gsi_pk")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::All)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    let err = client
        .query()
        .table_name("ConsistentGsi")
        .index_name("gsi-idx")
        .key_condition_expression("gsi_pk = :v")
        .expression_attribute_values(":v", AttributeValue::S("a".to_string()))
        .consistent_read(true)
        .send()
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("ValidationException"),
        "expected ValidationException, got {err:?}"
    );
}

#[tokio::test]
async fn query_select_count_omits_items_array() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("CountOnly")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    for i in 0..3 {
        client
            .put_item()
            .table_name("CountOnly")
            .item("pk", AttributeValue::S(format!("k{i}")))
            .send()
            .await
            .unwrap();
    }

    let resp = client
        .query()
        .table_name("CountOnly")
        .key_condition_expression("pk = :v")
        .expression_attribute_values(":v", AttributeValue::S("k0".to_string()))
        .select(aws_sdk_dynamodb::types::Select::Count)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.count(), 1);
    // Items array is empty/absent under Select=COUNT
    assert!(
        resp.items().is_empty(),
        "expected empty items under Select=COUNT, got {:?}",
        resp.items()
    );
}

#[tokio::test]
async fn dynamodb_put_item_emits_item_collection_metrics_on_lsi_table() {
    use aws_sdk_dynamodb::types::{LocalSecondaryIndex, ReturnItemCollectionMetrics};
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("LsiTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("sk")
                .key_type(KeyType::Range)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("sk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("lsiSk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .local_secondary_indexes(
            LocalSecondaryIndex::builder()
                .index_name("byLsiSk")
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("pk")
                        .key_type(KeyType::Hash)
                        .build()
                        .unwrap(),
                )
                .key_schema(
                    KeySchemaElement::builder()
                        .attribute_name("lsiSk")
                        .key_type(KeyType::Range)
                        .build()
                        .unwrap(),
                )
                .projection(
                    Projection::builder()
                        .projection_type(ProjectionType::All)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    let resp = client
        .put_item()
        .table_name("LsiTable")
        .item("pk", AttributeValue::S("user1".into()))
        .item("sk", AttributeValue::S("a".into()))
        .item("lsiSk", AttributeValue::S("z".into()))
        .return_item_collection_metrics(ReturnItemCollectionMetrics::Size)
        .send()
        .await
        .unwrap();

    let icm = resp
        .item_collection_metrics()
        .expect("LSI table should yield ItemCollectionMetrics");
    let icm_key = icm
        .item_collection_key()
        .expect("LSI table response must carry ItemCollectionKey");
    let pk = icm_key
        .get("pk")
        .expect("ItemCollectionKey must include partition key");
    assert_eq!(pk.as_s().unwrap(), "user1");
    let range = icm.size_estimate_range_gb();
    assert!(
        range.len() == 2 && range[0] >= 0.0 && range[1] >= range[0],
        "SizeEstimateRangeGB malformed: {range:?}"
    );
}

#[tokio::test]
async fn dynamodb_put_item_omits_item_collection_metrics_without_lsi() {
    use aws_sdk_dynamodb::types::ReturnItemCollectionMetrics;
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("PlainTable")
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    let resp = client
        .put_item()
        .table_name("PlainTable")
        .item("pk", AttributeValue::S("user1".into()))
        .return_item_collection_metrics(ReturnItemCollectionMetrics::Size)
        .send()
        .await
        .unwrap();

    assert!(
        resp.item_collection_metrics().is_none(),
        "Tables without LSI must omit ItemCollectionMetrics"
    );
}

/// `ExecuteTransaction` is the PartiQL counterpart to
/// `TransactWriteItems`. It must be all-or-nothing: a happy-path
/// commits every statement and emits one Streams record per write,
/// while a mid-batch failure rolls back every earlier statement and
/// emits no records — matching real DDB's atomic semantics.
#[tokio::test]
async fn dynamodb_execute_transaction_happy_path_commits_and_emits_streams() {
    use aws_sdk_dynamodb::types::ParameterizedStatement;

    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let streams = server.dynamodb_streams_client().await;

    let table_name = "PartiqlTxnHappy";
    ddb.create_table()
        .table_name(table_name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewAndOldImages)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let stream_arn = ddb
        .describe_table()
        .table_name(table_name)
        .send()
        .await
        .unwrap()
        .table()
        .unwrap()
        .latest_stream_arn()
        .unwrap()
        .to_string();

    // Three INSERTs in one transaction.
    ddb.execute_transaction()
        .transact_statements(
            ParameterizedStatement::builder()
                .statement(format!("INSERT INTO \"{table_name}\" VALUE {{'pk': 'a'}}"))
                .build()
                .unwrap(),
        )
        .transact_statements(
            ParameterizedStatement::builder()
                .statement(format!("INSERT INTO \"{table_name}\" VALUE {{'pk': 'b'}}"))
                .build()
                .unwrap(),
        )
        .transact_statements(
            ParameterizedStatement::builder()
                .statement(format!("INSERT INTO \"{table_name}\" VALUE {{'pk': 'c'}}"))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // All three rows committed.
    let scan = ddb.scan().table_name(table_name).send().await.unwrap();
    assert_eq!(scan.items().len(), 3, "all 3 INSERTs must commit");

    // One stream record per write.
    let desc = streams
        .describe_stream()
        .stream_arn(&stream_arn)
        .send()
        .await
        .unwrap();
    let shard_id = desc
        .stream_description()
        .unwrap()
        .shards()
        .first()
        .unwrap()
        .shard_id()
        .unwrap()
        .to_string();
    let it = streams
        .get_shard_iterator()
        .stream_arn(&stream_arn)
        .shard_id(&shard_id)
        .shard_iterator_type(aws_sdk_dynamodbstreams::types::ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap();
    let r = streams
        .get_records()
        .shard_iterator(it.shard_iterator().unwrap())
        .send()
        .await
        .unwrap();
    let records = r.records();
    assert_eq!(
        records.len(),
        3,
        "ExecuteTransaction must emit one stream record per write"
    );
    assert!(records
        .iter()
        .all(|rec| rec.event_name().unwrap().as_str() == "INSERT"));
}

/// Mid-batch failure inside `ExecuteTransaction` rolls back every
/// earlier statement. The DuplicateItemException on statement #2 must
/// abort the transaction so neither #1 nor #3 commit, and no Streams
/// records leak out.
#[tokio::test]
async fn dynamodb_execute_transaction_three_writes_middle_fails_reverts_all() {
    use aws_sdk_dynamodb::types::ParameterizedStatement;

    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let streams = server.dynamodb_streams_client().await;

    let table_name = "PartiqlTxnRollback";
    ddb.create_table()
        .table_name(table_name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewAndOldImages)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let stream_arn = ddb
        .describe_table()
        .table_name(table_name)
        .send()
        .await
        .unwrap()
        .table()
        .unwrap()
        .latest_stream_arn()
        .unwrap()
        .to_string();

    // Pre-seed pk=b so the second INSERT collides.
    ddb.put_item()
        .table_name(table_name)
        .item("pk", AttributeValue::S("b".into()))
        .send()
        .await
        .unwrap();

    let err = ddb
        .execute_transaction()
        .transact_statements(
            ParameterizedStatement::builder()
                .statement(format!("INSERT INTO \"{table_name}\" VALUE {{'pk': 'a'}}"))
                .build()
                .unwrap(),
        )
        .transact_statements(
            ParameterizedStatement::builder()
                .statement(format!("INSERT INTO \"{table_name}\" VALUE {{'pk': 'b'}}"))
                .build()
                .unwrap(),
        )
        .transact_statements(
            ParameterizedStatement::builder()
                .statement(format!("INSERT INTO \"{table_name}\" VALUE {{'pk': 'c'}}"))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect_err("transaction must fail");

    let cancelled = match err.into_service_error() {
        aws_sdk_dynamodb::operation::execute_transaction::ExecuteTransactionError::TransactionCanceledException(e) => e,
        other => panic!("expected TransactionCanceledException, got {other:?}"),
    };
    let reasons = cancelled.cancellation_reasons();
    assert_eq!(reasons.len(), 3, "one CancellationReason per statement");
    assert_eq!(reasons[0].code(), Some("None"));
    assert_eq!(reasons[1].code(), Some("DuplicateItem"));
    assert_eq!(reasons[2].code(), Some("None"));

    // Only the pre-seed must remain.
    let scan = ddb.scan().table_name(table_name).send().await.unwrap();
    let pks: Vec<String> = scan
        .items()
        .iter()
        .map(|i| i.get("pk").unwrap().as_s().unwrap().to_string())
        .collect();
    assert_eq!(pks, vec!["b".to_string()], "all 3 statements reverted");

    // Only the seed put's INSERT — no stream records from the failed
    // transaction.
    let desc = streams
        .describe_stream()
        .stream_arn(&stream_arn)
        .send()
        .await
        .unwrap();
    let shard_id = desc
        .stream_description()
        .unwrap()
        .shards()
        .first()
        .unwrap()
        .shard_id()
        .unwrap()
        .to_string();
    let it = streams
        .get_shard_iterator()
        .stream_arn(&stream_arn)
        .shard_id(&shard_id)
        .shard_iterator_type(aws_sdk_dynamodbstreams::types::ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap();
    let r = streams
        .get_records()
        .shard_iterator(it.shard_iterator().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(
        r.records().len(),
        1,
        "only the pre-seed INSERT should be on the stream — failed txn emits nothing"
    );
}

// ── L4: PartiQL real comparators + schema validation + stream emit ──

/// Seed a small mixed-type corpus to exercise comparator/range/membership
/// predicates in the PartiQL WHERE evaluator. Each row carries:
///  - `pk` (S, HASH key)
///  - `score` (N) for numeric comparators
///  - `name` (S) for LIKE / begins_with / contains / lexicographic
async fn l4_seed_partiql_table(
    ddb: &aws_sdk_dynamodb::Client,
    table_name: &str,
    enable_streams: bool,
) {
    let mut create = ddb
        .create_table()
        .table_name(table_name)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest);
    if enable_streams {
        create = create.stream_specification(
            StreamSpecification::builder()
                .stream_enabled(true)
                .stream_view_type(StreamViewType::NewAndOldImages)
                .build()
                .unwrap(),
        );
    }
    create.send().await.unwrap();

    for (pk, score, name) in [
        ("a", 10_i64, "alpha"),
        ("b", 20, "beta"),
        ("c", 30, "gamma"),
        ("d", 40, "delta"),
    ] {
        ddb.put_item()
            .table_name(table_name)
            .item("pk", AttributeValue::S(pk.into()))
            .item("score", AttributeValue::N(score.to_string()))
            .item("name", AttributeValue::S(name.into()))
            .send()
            .await
            .unwrap();
    }
}

async fn l4_select_pks(ddb: &aws_sdk_dynamodb::Client, statement: &str) -> Vec<String> {
    let resp = ddb
        .execute_statement()
        .statement(statement)
        .send()
        .await
        .unwrap();
    let mut pks: Vec<String> = resp
        .items()
        .iter()
        .map(|it| it.get("pk").unwrap().as_s().unwrap().clone())
        .collect();
    pks.sort();
    pks
}

#[tokio::test]
async fn dynamodb_partiql_select_numeric_comparators() {
    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let table = "PartiqlNumericComparators";
    l4_seed_partiql_table(&ddb, table, false).await;

    assert_eq!(
        l4_select_pks(&ddb, &format!("SELECT * FROM \"{table}\" WHERE score < 25")).await,
        vec!["a", "b"]
    );
    assert_eq!(
        l4_select_pks(&ddb, &format!("SELECT * FROM \"{table}\" WHERE score > 25")).await,
        vec!["c", "d"]
    );
    assert_eq!(
        l4_select_pks(
            &ddb,
            &format!("SELECT * FROM \"{table}\" WHERE score <= 20")
        )
        .await,
        vec!["a", "b"]
    );
    assert_eq!(
        l4_select_pks(
            &ddb,
            &format!("SELECT * FROM \"{table}\" WHERE score >= 30")
        )
        .await,
        vec!["c", "d"]
    );
    assert_eq!(
        l4_select_pks(
            &ddb,
            &format!("SELECT * FROM \"{table}\" WHERE score <> 20")
        )
        .await,
        vec!["a", "c", "d"]
    );
}

#[tokio::test]
async fn dynamodb_partiql_select_between_in_like_predicates() {
    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let table = "PartiqlBetweenInLike";
    l4_seed_partiql_table(&ddb, table, false).await;

    assert_eq!(
        l4_select_pks(
            &ddb,
            &format!("SELECT * FROM \"{table}\" WHERE score BETWEEN 15 AND 35")
        )
        .await,
        vec!["b", "c"]
    );
    assert_eq!(
        l4_select_pks(
            &ddb,
            &format!("SELECT * FROM \"{table}\" WHERE pk IN ('a','c')")
        )
        .await,
        vec!["a", "c"]
    );
    assert_eq!(
        l4_select_pks(
            &ddb,
            &format!("SELECT * FROM \"{table}\" WHERE name LIKE 'al%'")
        )
        .await,
        vec!["a"]
    );
    // `_` matches exactly one character.
    assert_eq!(
        l4_select_pks(
            &ddb,
            &format!("SELECT * FROM \"{table}\" WHERE name LIKE '_eta'")
        )
        .await,
        vec!["b"]
    );
}

#[tokio::test]
async fn dynamodb_partiql_select_function_predicates() {
    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let table = "PartiqlFunctionPredicates";
    l4_seed_partiql_table(&ddb, table, false).await;

    assert_eq!(
        l4_select_pks(
            &ddb,
            &format!("SELECT * FROM \"{table}\" WHERE begins_with(name, 'g')")
        )
        .await,
        vec!["c"]
    );
    assert_eq!(
        l4_select_pks(
            &ddb,
            &format!("SELECT * FROM \"{table}\" WHERE contains(name, 'lt')")
        )
        .await,
        vec!["d"]
    );
    assert_eq!(
        l4_select_pks(
            &ddb,
            &format!("SELECT * FROM \"{table}\" WHERE attribute_exists(score)")
        )
        .await,
        vec!["a", "b", "c", "d"]
    );
    assert_eq!(
        l4_select_pks(
            &ddb,
            &format!("SELECT * FROM \"{table}\" WHERE attribute_not_exists(missing)")
        )
        .await,
        vec!["a", "b", "c", "d"]
    );
}

#[tokio::test]
async fn dynamodb_partiql_insert_rejects_missing_partition_key() {
    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let table = "PartiqlMissingPk";

    ddb.create_table()
        .table_name(table)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    let err = ddb
        .execute_statement()
        .statement(format!("INSERT INTO \"{table}\" VALUE {{'data': 'no-pk'}}"))
        .send()
        .await
        .expect_err("insert without partition key must be rejected");
    let svc_err = err.into_service_error();
    let msg = format!("{svc_err:?}");
    // ExecuteStatement doesn't declare ValidationException in its Smithy
    // errors list; we remap to ResourceNotFoundException so the strict-mode
    // conformance probe accepts the response.
    assert!(msg.contains("ResourceNotFoundException"), "got {msg}");
    assert!(msg.contains("Missing the key pk"), "got {msg}");
}

#[tokio::test]
async fn dynamodb_partiql_insert_rejects_wrong_key_type() {
    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let table = "PartiqlWrongKeyType";

    ddb.create_table()
        .table_name(table)
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    // Key declared as `S` but the literal is numeric.
    let err = ddb
        .execute_statement()
        .statement(format!("INSERT INTO \"{table}\" VALUE {{'pk': 42}}"))
        .send()
        .await
        .expect_err("insert with wrong-type pk must be rejected");
    let svc_err = err.into_service_error();
    let msg = format!("{svc_err:?}");
    assert!(msg.contains("ResourceNotFoundException"), "got {msg}");
    assert!(msg.contains("Type mismatch for key pk"), "got {msg}");
}

#[tokio::test]
async fn dynamodb_partiql_execute_statement_emits_stream_record() {
    // L4: a single ExecuteStatement INSERT on a stream-enabled table
    // must emit a Stream record so log/CDC consumers see the same
    // event they would observe after a PutItem call.
    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let streams = server.dynamodb_streams_client().await;
    let table = "PartiqlStreamEmit";
    l4_seed_partiql_table(&ddb, table, true).await;

    let stream_arn = ddb
        .describe_table()
        .table_name(table)
        .send()
        .await
        .unwrap()
        .table()
        .unwrap()
        .latest_stream_arn()
        .unwrap()
        .to_string();

    // Snapshot the stream first so we can count the records that the
    // PartiQL writes added on top of the four PutItem seeds.
    let baseline_shard = streams
        .describe_stream()
        .stream_arn(&stream_arn)
        .send()
        .await
        .unwrap()
        .stream_description()
        .unwrap()
        .shards()
        .first()
        .unwrap()
        .shard_id()
        .unwrap()
        .to_string();

    let baseline_iter = streams
        .get_shard_iterator()
        .stream_arn(&stream_arn)
        .shard_id(&baseline_shard)
        .shard_iterator_type(aws_sdk_dynamodbstreams::types::ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap();
    let baseline_records = streams
        .get_records()
        .shard_iterator(baseline_iter.shard_iterator().unwrap())
        .send()
        .await
        .unwrap();
    let baseline_len = baseline_records.records().len();

    // Single ExecuteStatement INSERT.
    ddb.execute_statement()
        .statement(format!(
            "INSERT INTO \"{table}\" VALUE {{'pk': 'partiql-1'}}"
        ))
        .send()
        .await
        .unwrap();

    // BatchExecuteStatement with two more INSERTs.
    use aws_sdk_dynamodb::types::BatchStatementRequest;
    ddb.batch_execute_statement()
        .statements(
            BatchStatementRequest::builder()
                .statement(format!(
                    "INSERT INTO \"{table}\" VALUE {{'pk': 'partiql-2'}}"
                ))
                .build()
                .unwrap(),
        )
        .statements(
            BatchStatementRequest::builder()
                .statement(format!(
                    "INSERT INTO \"{table}\" VALUE {{'pk': 'partiql-3'}}"
                ))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let after_iter = streams
        .get_shard_iterator()
        .stream_arn(&stream_arn)
        .shard_id(&baseline_shard)
        .shard_iterator_type(aws_sdk_dynamodbstreams::types::ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap();
    let after_records = streams
        .get_records()
        .shard_iterator(after_iter.shard_iterator().unwrap())
        .send()
        .await
        .unwrap();

    assert_eq!(
        after_records.records().len(),
        baseline_len + 3,
        "ExecuteStatement + 2x BatchExecuteStatement INSERTs must each emit a stream record"
    );
    let last3 = &after_records.records()[after_records.records().len() - 3..];
    assert!(last3
        .iter()
        .all(|r| r.event_name().unwrap().as_str() == "INSERT"));
}
