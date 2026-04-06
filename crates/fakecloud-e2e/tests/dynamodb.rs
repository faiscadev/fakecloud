mod helpers;

use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, ContributorInsightsAction, Delete,
    DeleteRequest, Get, KeySchemaElement, KeyType, PointInTimeRecoverySpecification,
    ProvisionedThroughput, Put, PutRequest, Replica, ScalarAttributeType, Tag,
    TimeToLiveSpecification, TransactGetItem, TransactWriteItem, WriteRequest,
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

    // Get should return no policy
    let resp = client
        .get_resource_policy()
        .resource_arn(&table_arn)
        .send()
        .await
        .unwrap();
    assert!(resp.policy().is_none());
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
