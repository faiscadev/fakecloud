mod helpers;

use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ProvisionedThroughput, PutRequest, ScalarAttributeType, Tag, WriteRequest,
};
use fakecloud_conformance_macros::test_action;
use helpers::TestServer;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Table lifecycle
// ---------------------------------------------------------------------------

#[test_action("dynamodb", "CreateTable", checksum = "871827e1")]
#[test_action("dynamodb", "DescribeTable", checksum = "543aeed6")]
#[test_action("dynamodb", "DeleteTable", checksum = "609d7442")]
#[tokio::test]
async fn dynamodb_create_describe_delete_table() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("ConfTable")
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
        .table_name("ConfTable")
        .send()
        .await
        .unwrap();
    let table = resp.table().unwrap();
    assert_eq!(table.table_name().unwrap(), "ConfTable");
    assert_eq!(table.table_status().unwrap().as_str(), "ACTIVE");

    client
        .delete_table()
        .table_name("ConfTable")
        .send()
        .await
        .unwrap();

    let result = client.describe_table().table_name("ConfTable").send().await;
    assert!(result.is_err());
}

#[test_action("dynamodb", "ListTables", checksum = "9871be61")]
#[tokio::test]
async fn dynamodb_list_tables() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("ListMe")
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

    let resp = client.list_tables().send().await.unwrap();
    assert!(resp.table_names().contains(&"ListMe".to_string()));
}

#[test_action("dynamodb", "UpdateTable", checksum = "5862b42d")]
#[tokio::test]
async fn dynamodb_update_table() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("UpdTable")
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

    client
        .update_table()
        .table_name("UpdTable")
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    let resp = client
        .describe_table()
        .table_name("UpdTable")
        .send()
        .await
        .unwrap();
    assert!(resp.table().is_some());
}

// ---------------------------------------------------------------------------
// Item operations
// ---------------------------------------------------------------------------

#[test_action("dynamodb", "PutItem", checksum = "65656c32")]
#[test_action("dynamodb", "GetItem", checksum = "bfb4efce")]
#[test_action("dynamodb", "DeleteItem", checksum = "1f2be9ef")]
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

#[test_action("dynamodb", "UpdateItem", checksum = "d29893e3")]
#[tokio::test]
async fn dynamodb_update_item() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    client
        .create_table()
        .table_name("UpdItems")
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
        .table_name("UpdItems")
        .item("id", AttributeValue::S("u1".to_string()))
        .item("count", AttributeValue::N("10".to_string()))
        .send()
        .await
        .unwrap();

    client
        .update_item()
        .table_name("UpdItems")
        .key("id", AttributeValue::S("u1".to_string()))
        .update_expression("SET #c = :newval")
        .expression_attribute_names("#c", "count")
        .expression_attribute_values(":newval", AttributeValue::N("20".to_string()))
        .send()
        .await
        .unwrap();

    let resp = client
        .get_item()
        .table_name("UpdItems")
        .key("id", AttributeValue::S("u1".to_string()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.item().unwrap().get("count").unwrap().as_n().unwrap(),
        "20"
    );
}

// ---------------------------------------------------------------------------
// Query and Scan
// ---------------------------------------------------------------------------

#[test_action("dynamodb", "Query", checksum = "0cd83e93")]
#[tokio::test]
async fn dynamodb_query() {
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
            .send()
            .await
            .unwrap();
    }

    let resp = client
        .query()
        .table_name("Orders")
        .key_condition_expression("userId = :uid")
        .expression_attribute_values(":uid", AttributeValue::S("user1".to_string()))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.count(), 3);
}

#[test_action("dynamodb", "Scan", checksum = "282511c3")]
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
            .send()
            .await
            .unwrap();
    }

    let resp = client.scan().table_name("ScanTable").send().await.unwrap();
    assert_eq!(resp.count(), 5);
}

// ---------------------------------------------------------------------------
// Batch operations
// ---------------------------------------------------------------------------

#[test_action("dynamodb", "BatchWriteItem", checksum = "20b0040e")]
#[test_action("dynamodb", "BatchGetItem", checksum = "5eb50c02")]
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

    let mut write_items = HashMap::new();
    write_items.insert(
        "BatchTable".to_string(),
        vec![
            WriteRequest::builder()
                .put_request(PutRequest::builder().set_item(Some(item1)).build().unwrap())
                .build(),
            WriteRequest::builder()
                .put_request(PutRequest::builder().set_item(Some(item2)).build().unwrap())
                .build(),
        ],
    );

    client
        .batch_write_item()
        .set_request_items(Some(write_items))
        .send()
        .await
        .unwrap();

    let mut key1 = HashMap::new();
    key1.insert("id".to_string(), AttributeValue::S("b1".to_string()));
    let mut key2 = HashMap::new();
    key2.insert("id".to_string(), AttributeValue::S("b2".to_string()));

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
}

// ---------------------------------------------------------------------------
// Tags
// ---------------------------------------------------------------------------

#[test_action("dynamodb", "TagResource", checksum = "5a731507")]
#[test_action("dynamodb", "UntagResource", checksum = "0438d51b")]
#[test_action("dynamodb", "ListTagsOfResource", checksum = "e42e6834")]
#[tokio::test]
async fn dynamodb_tag_untag_list_tags() {
    let server = TestServer::start().await;
    let client = server.dynamodb_client().await;

    let create_resp = client
        .create_table()
        .table_name("TagTable")
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
                .value("conformance")
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
