mod helpers;

use fakecloud_conformance_macros::test_action;
use helpers::TestServer;

#[test_action("s3tables", "CreateNamespace", checksum = "47b215a6")]
#[test_action("s3tables", "CreateTable", checksum = "9d39dc98")]
#[test_action("s3tables", "CreateTableBucket", checksum = "0d08fdb7")]
#[test_action("s3tables", "DeleteNamespace", checksum = "1aae4974")]
#[test_action("s3tables", "DeleteTable", checksum = "43e632f0")]
#[test_action("s3tables", "DeleteTableBucket", checksum = "780e2316")]
#[test_action("s3tables", "DeleteTableBucketEncryption", checksum = "dc7368be")]
#[test_action(
    "s3tables",
    "DeleteTableBucketMetricsConfiguration",
    checksum = "f0a6a369"
)]
#[test_action("s3tables", "DeleteTableBucketPolicy", checksum = "8ec56541")]
#[test_action("s3tables", "DeleteTableBucketReplication", checksum = "22119a28")]
#[test_action("s3tables", "DeleteTablePolicy", checksum = "940939cf")]
#[test_action("s3tables", "DeleteTableReplication", checksum = "a06180fa")]
#[test_action("s3tables", "GetNamespace", checksum = "9583982d")]
#[test_action("s3tables", "GetTable", checksum = "a676aa41")]
#[test_action("s3tables", "GetTableBucket", checksum = "301bcddb")]
#[test_action("s3tables", "GetTableBucketEncryption", checksum = "55a942c1")]
#[test_action(
    "s3tables",
    "GetTableBucketMaintenanceConfiguration",
    checksum = "06297c7b"
)]
#[test_action(
    "s3tables",
    "GetTableBucketMetricsConfiguration",
    checksum = "1d8417ca"
)]
#[test_action("s3tables", "GetTableBucketPolicy", checksum = "2dc1043c")]
#[test_action("s3tables", "GetTableBucketReplication", checksum = "6af3e126")]
#[test_action("s3tables", "GetTableBucketStorageClass", checksum = "1641f325")]
#[test_action("s3tables", "GetTableEncryption", checksum = "2c689013")]
#[test_action("s3tables", "GetTableMaintenanceConfiguration", checksum = "ccdff99b")]
#[test_action("s3tables", "GetTableMaintenanceJobStatus", checksum = "abe90675")]
#[test_action("s3tables", "GetTableMetadataLocation", checksum = "71378512")]
#[test_action("s3tables", "GetTablePolicy", checksum = "96574f20")]
#[test_action(
    "s3tables",
    "GetTableRecordExpirationConfiguration",
    checksum = "7f2db7e6"
)]
#[test_action("s3tables", "GetTableRecordExpirationJobStatus", checksum = "211529df")]
#[test_action("s3tables", "GetTableReplication", checksum = "8b8e312c")]
#[test_action("s3tables", "GetTableReplicationStatus", checksum = "97ce4e4b")]
#[test_action("s3tables", "GetTableStorageClass", checksum = "2cabe9b2")]
#[test_action("s3tables", "ListNamespaces", checksum = "d4e5db12")]
#[test_action("s3tables", "ListTableBuckets", checksum = "56b2ac2f")]
#[test_action("s3tables", "ListTables", checksum = "54a93802")]
#[test_action("s3tables", "ListTagsForResource", checksum = "d3069893")]
#[test_action("s3tables", "PutTableBucketEncryption", checksum = "4f08f10c")]
#[test_action(
    "s3tables",
    "PutTableBucketMaintenanceConfiguration",
    checksum = "afae9dc1"
)]
#[test_action(
    "s3tables",
    "PutTableBucketMetricsConfiguration",
    checksum = "ebb1c54b"
)]
#[test_action("s3tables", "PutTableBucketPolicy", checksum = "2781c9da")]
#[test_action("s3tables", "PutTableBucketReplication", checksum = "5b2e2326")]
#[test_action("s3tables", "PutTableBucketStorageClass", checksum = "15e94a8a")]
#[test_action("s3tables", "PutTableMaintenanceConfiguration", checksum = "a713ac3f")]
#[test_action("s3tables", "PutTablePolicy", checksum = "0b578762")]
#[test_action(
    "s3tables",
    "PutTableRecordExpirationConfiguration",
    checksum = "318be759"
)]
#[test_action("s3tables", "PutTableReplication", checksum = "bb11d74c")]
#[test_action("s3tables", "RenameTable", checksum = "9ec9a6fa")]
#[test_action("s3tables", "TagResource", checksum = "f0c67126")]
#[test_action("s3tables", "UntagResource", checksum = "465065d7")]
#[test_action("s3tables", "UpdateTableMetadataLocation", checksum = "f0b371b1")]
#[tokio::test]
async fn s3tables_probe() {
    let _server = TestServer::start().await;
}

fn encode(s: &str) -> String {
    s.bytes()
        .map(|b| {
            if b.is_ascii_alphanumeric() {
                (b as char).to_string()
            } else {
                format!("%{b:02X}")
            }
        })
        .collect()
}

/// A Glue table migrated into a table bucket, reviewed, queued, synced and
/// completed. The vendored SDKs predate S3 Tables migrations, so the S3
/// Tables calls go over raw restJson1 requests.
#[test_action("s3tables", "StartTableMigration", checksum = "d7b4d9ec")]
#[test_action("s3tables", "GetTableMigration", checksum = "faa3bdc0")]
#[test_action("s3tables", "ListTableMigrations", checksum = "08049298")]
#[test_action("s3tables", "UpdateTableMigration", checksum = "a56202ec")]
#[test_action("s3tables", "SyncTableMigration", checksum = "5867237a")]
#[test_action("s3tables", "CompleteTableMigration", checksum = "f6b533bc")]
#[tokio::test]
async fn s3tables_table_migration_lifecycle() {
    use aws_sdk_glue::types::{Column, DatabaseInput, SerDeInfo, StorageDescriptor, TableInput};

    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    for bucket in ["conf-src", "conf-reports"] {
        s3.create_bucket().bucket(bucket).send().await.unwrap();
    }
    s3.put_object()
        .bucket("conf-src")
        .key("events/part-0.parquet")
        .body(b"PAR1conformancePAR1".to_vec().into())
        .send()
        .await
        .unwrap();
    let glue = server.glue_client().await;
    glue.create_database()
        .database_input(DatabaseInput::builder().name("confdb").build().unwrap())
        .send()
        .await
        .unwrap();
    glue.create_table()
        .database_name("confdb")
        .table_input(
            TableInput::builder()
                .name("events")
                .table_type("EXTERNAL_TABLE")
                .storage_descriptor(
                    StorageDescriptor::builder()
                        .columns(
                            Column::builder()
                                .name("id")
                                .r#type("bigint")
                                .build()
                                .unwrap(),
                        )
                        .location("s3://conf-src/events/")
                        .input_format(
                            "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                        )
                        .serde_info(
                            SerDeInfo::builder()
                                .serialization_library(
                                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                                )
                                .build(),
                        )
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let auth = "AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/s3tables/aws4_request, SignedHeaders=host, Signature=0";
    let http = reqwest::Client::new();
    let call = |method: reqwest::Method, path: String, body: serde_json::Value| {
        let url = format!("{}{path}", server.endpoint());
        let http = http.clone();
        async move {
            let resp = http
                .request(method, &url)
                .header("Authorization", auth)
                .header("Content-Type", "application/json")
                .body(body.to_string())
                .send()
                .await
                .unwrap();
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap();
            let json = if text.is_empty() {
                serde_json::Value::Null
            } else {
                serde_json::from_str(&text).unwrap()
            };
            (status, json)
        }
    };

    let (status, bucket) = call(
        reqwest::Method::PUT,
        "/buckets".to_string(),
        serde_json::json!({ "name": "conf-mig" }),
    )
    .await;
    assert_eq!(status, 200, "{bucket}");
    let bucket_arn = bucket["arn"].as_str().unwrap().to_string();
    call(
        reqwest::Method::PUT,
        format!("/namespaces/{}", encode(&bucket_arn)),
        serde_json::json!({ "namespace": ["ns"] }),
    )
    .await;

    let account = bucket_arn.split(':').nth(4).unwrap().to_string();
    let role = format!("arn:aws:iam::{account}:role/migration");
    let (status, started) = call(
        reqwest::Method::POST,
        "/start-table-migration".to_string(),
        serde_json::json!({
            "roleARN": role,
            "destinationTable": { "tableBucketARN": bucket_arn, "namespace": "ns", "name": "events" },
            "sourceType": "glueTable",
            "source": { "glueTable": { "catalogId": account, "databaseName": "confdb", "tableName": "events" } },
            "reviewMigrationPlan": true,
            "migrationOutput": { "bucket": "arn:aws:s3:::conf-reports" },
        }),
    )
    .await;
    assert_eq!(status, 202, "{started}");
    assert_eq!(started["state"], "pendingReview");
    let id = started["migrationId"].as_str().unwrap().to_string();

    let (status, migration) = call(
        reqwest::Method::GET,
        format!("/get-table-migration?migrationId={id}"),
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200, "{migration}");
    assert_eq!(migration["hasValidPlan"], true);
    assert_eq!(migration["planningStats"]["objectsPlanned"], 1);
    let table_arn = migration["tableARN"].as_str().unwrap().to_string();

    let (status, listed) = call(
        reqwest::Method::GET,
        format!(
            "/list-table-migrations?tableBucketARN={}",
            encode(&bucket_arn)
        ),
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(listed["migrations"][0]["migrationId"], id.as_str());

    let (status, updated) = call(
        reqwest::Method::POST,
        format!("/update-table-migration?migrationId={id}"),
        serde_json::json!({ "requestedState": "queued" }),
    )
    .await;
    assert_eq!(status, 200, "{updated}");
    assert_eq!(updated["state"], "succeeded");

    let (status, synced) = call(
        reqwest::Method::POST,
        "/sync-table-migration".to_string(),
        serde_json::json!({
            "roleARN": role,
            "tableARN": table_arn,
            "migrationOutput": { "bucket": "arn:aws:s3:::conf-reports" },
        }),
    )
    .await;
    assert_eq!(status, 202, "{synced}");
    assert_eq!(synced["state"], "succeeded");

    let (status, _) = call(
        reqwest::Method::POST,
        format!("/complete-table-migration?tableARN={}", encode(&table_arn)),
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 204);
    let (status, _) = call(
        reqwest::Method::POST,
        format!("/complete-table-migration?tableARN={}", encode(&table_arn)),
        serde_json::Value::Null,
    )
    .await;
    assert_eq!(status, 409);
}
