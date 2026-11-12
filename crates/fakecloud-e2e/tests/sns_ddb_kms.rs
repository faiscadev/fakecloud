//! SNS encrypted topic + DynamoDB encrypted table KMS audit-trail tests.
//!
//! SNS server-side encrypts at rest with the topic's KMS key; DynamoDB
//! does the same per item with the table's KMS key. fakecloud's
//! in-process fan-out and item store don't actually round-trip
//! ciphertext, but they emit the same `GenerateDataKey` /
//! `Decrypt` audit records the AWS API would, so callers can assert
//! KMS usage via `/_fakecloud/kms/usage`.

mod helpers;

use helpers::TestServer;

#[tokio::test]
async fn sns_encrypted_topic_publish_records_kms_usage() {
    let server = TestServer::start().await;
    let sns = server.sns_client().await;
    let http = reqwest::Client::new();

    let topic = sns
        .create_topic()
        .name("encrypted-topic")
        .attributes("KmsMasterKeyId", "alias/aws/sns")
        .send()
        .await
        .unwrap();
    let topic_arn = topic.topic_arn().unwrap().to_string();

    sns.publish()
        .topic_arn(&topic_arn)
        .message("hello world")
        .send()
        .await
        .unwrap();

    let usage: serde_json::Value = http
        .get(format!("{}/_fakecloud/kms/usage", server.endpoint()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let records = usage["records"].as_array().expect("records array");
    let sns_records: Vec<&serde_json::Value> = records
        .iter()
        .filter(|r| r["servicePrincipal"].as_str() == Some("sns.amazonaws.com"))
        .collect();
    assert!(
        sns_records.iter().any(|r| {
            r["operation"].as_str() == Some("GenerateDataKey")
                && r["encryptionContext"]["aws:sns:topicArn"].as_str() == Some(topic_arn.as_str())
        }),
        "expected GenerateDataKey bound to topic arn, got: {sns_records:?}"
    );
    assert!(
        sns_records.iter().any(|r| {
            r["operation"].as_str() == Some("Decrypt")
                && r["encryptionContext"]["aws:sns:topicArn"].as_str() == Some(topic_arn.as_str())
        }),
        "expected paired Decrypt bound to topic arn, got: {sns_records:?}"
    );
}

#[tokio::test]
async fn dynamodb_encrypted_table_records_kms_usage_on_put_and_get() {
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType, SseSpecification, SseType,
    };

    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let http = reqwest::Client::new();

    ddb.create_table()
        .table_name("encrypted-table")
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .sse_specification(
            SseSpecification::builder()
                .enabled(true)
                .sse_type(SseType::Kms)
                .kms_master_key_id("alias/aws/dynamodb")
                .build(),
        )
        .send()
        .await
        .unwrap();

    let mut item = std::collections::HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("row-1".to_string()));
    ddb.put_item()
        .table_name("encrypted-table")
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    let mut key = std::collections::HashMap::new();
    key.insert("pk".to_string(), AttributeValue::S("row-1".to_string()));
    ddb.get_item()
        .table_name("encrypted-table")
        .set_key(Some(key))
        .send()
        .await
        .unwrap();

    let usage: serde_json::Value = http
        .get(format!("{}/_fakecloud/kms/usage", server.endpoint()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let records = usage["records"].as_array().expect("records array");
    let ddb_records: Vec<&serde_json::Value> = records
        .iter()
        .filter(|r| r["servicePrincipal"].as_str() == Some("dynamodb.amazonaws.com"))
        .collect();
    assert!(
        ddb_records
            .iter()
            .any(|r| r["operation"].as_str() == Some("GenerateDataKey")),
        "expected GenerateDataKey on PutItem, got: {ddb_records:?}"
    );
    assert!(
        ddb_records
            .iter()
            .any(|r| r["operation"].as_str() == Some("Decrypt")),
        "expected Decrypt on GetItem, got: {ddb_records:?}"
    );
}

#[tokio::test]
async fn dynamodb_unencrypted_table_does_not_record_kms_usage() {
    use aws_sdk_dynamodb::types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType,
    };

    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let http = reqwest::Client::new();

    ddb.create_table()
        .table_name("plain-table")
        .attribute_definitions(
            AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(BillingMode::PayPerRequest)
        .send()
        .await
        .unwrap();

    let mut item = std::collections::HashMap::new();
    item.insert("pk".to_string(), AttributeValue::S("row-1".to_string()));
    ddb.put_item()
        .table_name("plain-table")
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    let usage: serde_json::Value = http
        .get(format!("{}/_fakecloud/kms/usage", server.endpoint()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let records = usage["records"].as_array().expect("records array");
    assert!(
        !records
            .iter()
            .any(|r| r["servicePrincipal"].as_str() == Some("dynamodb.amazonaws.com")),
        "table without SSE-KMS must not record KMS usage, got: {records:?}"
    );
}
