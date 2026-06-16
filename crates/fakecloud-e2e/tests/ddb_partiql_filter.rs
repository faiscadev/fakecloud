//! L4 — DynamoDB PartiQL WHERE-clause + INSERT validation + stream
//! emission end-to-end coverage. Drives the live AWS Rust SDK against
//! `ExecuteStatement` so any drift between our PartiQL evaluator and
//! the SDK's wire format surfaces immediately. The unit tests in
//! `crates/fakecloud-dynamodb/src/service/tests.rs` cover the same
//! behaviors against the in-process service; this file proves the
//! flow round-trips through the real HTTP layer.

mod helpers;

use aws_sdk_dynamodb::types::{
    AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
    ScalarAttributeType, StreamSpecification, StreamViewType,
};
use helpers::TestServer;

async fn create_streamed_table(ddb: &aws_sdk_dynamodb::Client, name: &str) {
    ddb.create_table()
        .table_name(name)
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
}

async fn put_row(ddb: &aws_sdk_dynamodb::Client, table: &str, pk: &str, n: i64, s: &str) {
    ddb.put_item()
        .table_name(table)
        .item("pk", AttributeValue::S(pk.into()))
        .item("n", AttributeValue::N(n.to_string()))
        .item("s", AttributeValue::S(s.into()))
        .send()
        .await
        .unwrap();
}

async fn select_pks(ddb: &aws_sdk_dynamodb::Client, statement: &str) -> Vec<String> {
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
async fn ddb_partiql_select_n_gt_5_and_s_like_foo() {
    // L4 spec example: SELECT * FROM "T" WHERE n > 5 AND s LIKE 'foo%'
    // returns the right subset.
    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let table = "L4PartiqlSelectAnd";
    create_streamed_table(&ddb, table).await;

    put_row(&ddb, table, "k1", 1, "foobar").await; // n too low
    put_row(&ddb, table, "k2", 6, "foobar").await; // matches
    put_row(&ddb, table, "k3", 6, "barfoo").await; // wrong prefix
    put_row(&ddb, table, "k4", 9, "fooz").await; // matches

    let pks = select_pks(
        &ddb,
        &format!("SELECT * FROM \"{table}\" WHERE n > 5 AND s LIKE 'foo%'"),
    )
    .await;
    assert_eq!(pks, vec!["k2", "k4"]);
}

#[tokio::test]
async fn ddb_partiql_select_or_not_parens() {
    // L4 spec: WHERE composition with AND/OR/NOT and parens. Each
    // sub-form exercises a different branch of the recursive parser.
    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let table = "L4PartiqlSelectOrNotParens";
    create_streamed_table(&ddb, table).await;

    for (pk, n) in [("a", 10_i64), ("b", 20), ("c", 30), ("d", 40)] {
        ddb.put_item()
            .table_name(table)
            .item("pk", AttributeValue::S(pk.into()))
            .item("n", AttributeValue::N(n.to_string()))
            .send()
            .await
            .unwrap();
    }

    // OR — match the lower and upper edges.
    assert_eq!(
        select_pks(
            &ddb,
            &format!("SELECT * FROM \"{table}\" WHERE n < 15 OR n > 35")
        )
        .await,
        vec!["a", "d"]
    );
    // NOT inverts a comparator.
    assert_eq!(
        select_pks(
            &ddb,
            &format!("SELECT * FROM \"{table}\" WHERE NOT n >= 30")
        )
        .await,
        vec!["a", "b"]
    );
    // Parens force OR to bind tighter than AND.
    assert_eq!(
        select_pks(
            &ddb,
            &format!("SELECT * FROM \"{table}\" WHERE (n < 15 OR n > 35) AND attribute_exists(pk)"),
        )
        .await,
        vec!["a", "d"]
    );
}

#[tokio::test]
async fn ddb_partiql_insert_missing_sort_key_validation() {
    // L4 spec: INSERT with missing sort key returns ValidationException.
    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let table = "L4PartiqlMissingSk";

    ddb.create_table()
        .table_name(table)
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

    let err = ddb
        .execute_statement()
        .statement(format!("INSERT INTO \"{table}\" VALUE {{'pk': 'a'}}"))
        .send()
        .await
        .expect_err("insert without sort key must fail");
    let msg = format!("{:?}", err.into_service_error());
    // PartiQL INSERT with a missing required key is a ValidationException,
    // matching AWS — the prior remap to ResourceNotFoundException returned the
    // wrong __type to clients and is no longer applied.
    assert!(msg.contains("ValidationException"), "got {msg}");
    assert!(msg.contains("Missing the key sk"), "got {msg}");
}

#[tokio::test]
async fn ddb_partiql_update_emits_stream_record() {
    // L4 spec: UPDATE statement on a stream-enabled table emits a
    // MODIFY stream record visible via the Streams data plane.
    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let streams = server.dynamodb_streams_client().await;
    let table = "L4PartiqlUpdateStream";

    create_streamed_table(&ddb, table).await;
    put_row(&ddb, table, "u1", 1, "x").await;

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
    let shard_id = streams
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

    // Snapshot baseline shard length so we count only what the UPDATE
    // adds on top of the seeding PutItem.
    let baseline_iter = streams
        .get_shard_iterator()
        .stream_arn(&stream_arn)
        .shard_id(&shard_id)
        .shard_iterator_type(aws_sdk_dynamodbstreams::types::ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap();
    let baseline_len = streams
        .get_records()
        .shard_iterator(baseline_iter.shard_iterator().unwrap())
        .send()
        .await
        .unwrap()
        .records()
        .len();

    ddb.execute_statement()
        .statement(format!("UPDATE \"{table}\" SET n = 99 WHERE pk = 'u1'"))
        .send()
        .await
        .unwrap();

    let after_iter = streams
        .get_shard_iterator()
        .stream_arn(&stream_arn)
        .shard_id(&shard_id)
        .shard_iterator_type(aws_sdk_dynamodbstreams::types::ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap();
    let after = streams
        .get_records()
        .shard_iterator(after_iter.shard_iterator().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(
        after.records().len(),
        baseline_len + 1,
        "UPDATE must emit exactly one stream record"
    );
    assert_eq!(
        after
            .records()
            .last()
            .unwrap()
            .event_name()
            .unwrap()
            .as_str(),
        "MODIFY",
    );
}

#[tokio::test]
async fn ddb_partiql_delete_emits_stream_record() {
    // L4 spec: DELETE statement on a stream-enabled table emits a
    // REMOVE stream record visible via the Streams data plane.
    let server = TestServer::start().await;
    let ddb = server.dynamodb_client().await;
    let streams = server.dynamodb_streams_client().await;
    let table = "L4PartiqlDeleteStream";

    create_streamed_table(&ddb, table).await;
    put_row(&ddb, table, "d1", 1, "x").await;

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
    let shard_id = streams
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
        .shard_id(&shard_id)
        .shard_iterator_type(aws_sdk_dynamodbstreams::types::ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap();
    let baseline_len = streams
        .get_records()
        .shard_iterator(baseline_iter.shard_iterator().unwrap())
        .send()
        .await
        .unwrap()
        .records()
        .len();

    ddb.execute_statement()
        .statement(format!("DELETE FROM \"{table}\" WHERE pk = 'd1'"))
        .send()
        .await
        .unwrap();

    let after_iter = streams
        .get_shard_iterator()
        .stream_arn(&stream_arn)
        .shard_id(&shard_id)
        .shard_iterator_type(aws_sdk_dynamodbstreams::types::ShardIteratorType::TrimHorizon)
        .send()
        .await
        .unwrap();
    let after = streams
        .get_records()
        .shard_iterator(after_iter.shard_iterator().unwrap())
        .send()
        .await
        .unwrap();
    assert_eq!(
        after.records().len(),
        baseline_len + 1,
        "DELETE must emit exactly one stream record"
    );
    assert_eq!(
        after
            .records()
            .last()
            .unwrap()
            .event_name()
            .unwrap()
            .as_str(),
        "REMOVE",
    );
}
