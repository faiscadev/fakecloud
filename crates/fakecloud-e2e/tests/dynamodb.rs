mod helpers;

use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, DeleteRequest, KeySchemaElement, KeyType,
    ProvisionedThroughput, PutRequest, ScalarAttributeType, Tag, WriteRequest,
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
