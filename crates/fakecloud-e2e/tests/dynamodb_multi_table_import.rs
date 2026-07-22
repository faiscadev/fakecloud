mod helpers;

use std::io::Write as _;
use std::path::Path;

use flate2::write::GzEncoder;
use flate2::Compression;
use helpers::TestServer;

/// Real-SDK end-to-end proof of multi-table import: `FAKECLOUD_DYNAMODB_IMPORT_PATH`
/// set alone (no `FAKECLOUD_DYNAMODB_DESCRIBE_TABLE`) treats the path as a root
/// directory of self-contained per-table subdirectories. Builds two such
/// subdirectories at test time and asserts both tables are queryable after boot.
#[tokio::test]
async fn startup_import_loads_every_table_from_root_dir() {
    let root = tempfile::tempdir().unwrap();
    write_table_export(root.path(), "widgets", "Widgets");
    write_table_export(root.path(), "gadgets", "Gadgets");

    let server = TestServer::start_with_env(&[
        ("FAKECLOUD_CONTAINER_CLI", "false"),
        (
            "FAKECLOUD_DYNAMODB_IMPORT_PATH",
            root.path().to_str().unwrap(),
        ),
    ])
    .await;
    let client = server.dynamodb_client().await;

    for table in ["Widgets", "Gadgets"] {
        let scan = client.scan().table_name(table).send().await.unwrap();
        assert_eq!(scan.count(), 1, "table {table} should have its one item");
        let item = &scan.items()[0];
        assert_eq!(item["Id"].as_s().unwrap().as_str(), format!("{table}-row"));
    }
}

/// Writes a minimal self-contained single-item table export under
/// `root/<subdir_name>`: `describe-table.json` plus the manifest/data files
/// the importer expects.
fn write_table_export(root: &Path, subdir_name: &str, table_name: &str) {
    let dir = root.join(subdir_name);
    std::fs::create_dir_all(dir.join("data")).unwrap();
    std::fs::write(
        dir.join("describe-table.json"),
        format!(
            r#"{{"Table":{{"TableName":"{table_name}","KeySchema":[{{"AttributeName":"Id","KeyType":"HASH"}}],"AttributeDefinitions":[{{"AttributeName":"Id","AttributeType":"S"}}],"BillingMode":"PAY_PER_REQUEST"}}}}"#
        ),
    )
    .unwrap();
    std::fs::write(
        dir.join("manifest-summary.json"),
        r#"{"version":"2020-06-30","exportFormat":"DYNAMODB_JSON","itemCount":1}"#,
    )
    .unwrap();
    std::fs::write(
        dir.join("manifest-files.json"),
        r#"{"itemCount":1,"dataFileS3Key":"AWSDynamoDB/01700000000000-abcd/data/0001.json.gz"}"#,
    )
    .unwrap();
    let data_file = std::fs::File::create(dir.join("data/0001.json.gz")).unwrap();
    let mut enc = GzEncoder::new(data_file, Compression::default());
    enc.write_all(format!(r#"{{"Item":{{"Id":{{"S":"{table_name}-row"}}}}}}"#).as_bytes())
        .unwrap();
    enc.write_all(b"\n").unwrap();
    enc.finish().unwrap();
}
