//! Regression coverage for the region-in-ARN family (#2356 siblings).
//!
//! `MultiAccountState` freezes `region` at server startup and several services
//! stamped that frozen region into ARNs/ids RETURNED to the client instead of
//! the request's SigV4 credential-scope region (`req.region`). A resource
//! created by a provider configured for e.g. `eu-central-1` came back with a
//! `us-east-1` ARN, which the client stores and reuses — breaking Terraform
//! drift/refresh and cross-service IAM ARN matching.
//!
//! Each check signs its requests for a NON-default region and asserts the
//! returned ARN/id carries that region, not the server default `us-east-1`.

mod helpers;

use helpers::TestServer;

const REGION: &str = "eu-central-1";

fn assert_region(arn: &str, what: &str) {
    assert!(
        arn.contains(REGION),
        "{what} should carry the request region {REGION}, got: {arn}"
    );
    assert!(
        !arn.contains("us-east-1"),
        "{what} must not carry the server-default region us-east-1, got: {arn}"
    );
}

#[tokio::test]
async fn returned_arns_use_request_region() {
    let server = TestServer::start().await;
    let cfg = server.aws_config_in(REGION).await;

    // DynamoDB table ARN
    let ddb = aws_sdk_dynamodb::Client::new(&cfg);
    let table = ddb
        .create_table()
        .table_name("region-tbl")
        .attribute_definitions(
            aws_sdk_dynamodb::types::AttributeDefinition::builder()
                .attribute_name("pk")
                .attribute_type(aws_sdk_dynamodb::types::ScalarAttributeType::S)
                .build()
                .unwrap(),
        )
        .key_schema(
            aws_sdk_dynamodb::types::KeySchemaElement::builder()
                .attribute_name("pk")
                .key_type(aws_sdk_dynamodb::types::KeyType::Hash)
                .build()
                .unwrap(),
        )
        .billing_mode(aws_sdk_dynamodb::types::BillingMode::PayPerRequest)
        .send()
        .await
        .expect("create_table");
    assert_region(
        table
            .table_description()
            .and_then(|t| t.table_arn())
            .expect("table_arn"),
        "DynamoDB TableArn",
    );

    // Kinesis stream ARN (read back via DescribeStreamSummary)
    let kinesis = aws_sdk_kinesis::Client::new(&cfg);
    kinesis
        .create_stream()
        .stream_name("region-stream")
        .shard_count(1)
        .send()
        .await
        .expect("create_stream");
    let summary = kinesis
        .describe_stream_summary()
        .stream_name("region-stream")
        .send()
        .await
        .expect("describe_stream_summary");
    assert_region(
        summary
            .stream_description_summary()
            .map(|s| s.stream_arn())
            .expect("stream_arn"),
        "Kinesis StreamArn",
    );

    // ECR repository ARN
    let ecr = aws_sdk_ecr::Client::new(&cfg);
    let repo = ecr
        .create_repository()
        .repository_name("region-repo")
        .send()
        .await
        .expect("create_repository");
    assert_region(
        repo.repository()
            .and_then(|r| r.repository_arn())
            .expect("repository_arn"),
        "ECR repositoryArn",
    );

    // ECS cluster ARN
    let ecs = aws_sdk_ecs::Client::new(&cfg);
    let cluster = ecs
        .create_cluster()
        .cluster_name("region-cluster")
        .send()
        .await
        .expect("create_cluster");
    assert_region(
        cluster
            .cluster()
            .and_then(|c| c.cluster_arn())
            .expect("cluster_arn"),
        "ECS clusterArn",
    );

    // RDS DB instance ARN (the create response carries the ARN immediately)
    let rds = aws_sdk_rds::Client::new(&cfg);
    let db = rds
        .create_db_instance()
        .db_instance_identifier("region-db")
        .db_instance_class("db.t3.micro")
        .engine("mysql")
        .allocated_storage(20)
        .master_username("admin")
        .master_user_password("Sup3rSecret!")
        .send()
        .await
        .expect("create_db_instance");
    assert_region(
        db.db_instance()
            .and_then(|d| d.db_instance_arn())
            .expect("db_instance_arn"),
        "RDS DBInstanceArn",
    );

    // Cognito user pool id + ARN: the id encodes the region as a prefix, so it
    // must be `eu-central-1_...` and the ARN must match.
    let cognito = aws_sdk_cognitoidentityprovider::Client::new(&cfg);
    let pool = cognito
        .create_user_pool()
        .pool_name("region-pool")
        .send()
        .await
        .expect("create_user_pool")
        .user_pool()
        .cloned()
        .expect("user_pool");
    let pool_id = pool.id().expect("pool id");
    assert!(
        pool_id.starts_with(&format!("{REGION}_")),
        "Cognito pool id should be prefixed with {REGION}, got: {pool_id}"
    );
    assert_region(pool.arn().expect("pool arn"), "Cognito user pool ARN");
}
