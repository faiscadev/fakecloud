mod helpers;

use std::path::PathBuf;

use aws_sdk_dynamodb::types::AttributeValue;
use helpers::TestServer;

/// Real-SDK end-to-end proof that the startup DynamoDB export importer works
/// through the actual boot path: launch a fakecloud process configured (via
/// env vars) to bulk-load the AWS-export fixture at startup, then read the
/// imported `Music` table back through `aws_sdk_dynamodb` and verify the data
/// round-tripped and is queryable by its key schema.
///
/// The fixtures live in the `fakecloud-dynamodb` crate (not this e2e crate),
/// so the paths are built from this crate's manifest dir and navigated across
/// with `..` — same trick `distribution_dockerfile.rs` uses for the repo-root
/// `Dockerfile`.
#[tokio::test]
async fn startup_import_loads_music_table_from_aws_export() {
    let fixtures: PathBuf = [
        env!("CARGO_MANIFEST_DIR"),
        "..",
        "fakecloud-dynamodb",
        "tests",
        "fixtures",
    ]
    .iter()
    .collect();
    let export_dir = fixtures.join("export");
    let describe_table = fixtures.join("describe-table.json");
    assert!(
        export_dir.join("manifest-summary.json").exists(),
        "export fixture missing at {}",
        export_dir.display()
    );
    assert!(
        describe_table.exists(),
        "describe-table.json missing at {}",
        describe_table.display()
    );

    let server = TestServer::start_with_env(&[
        ("FAKECLOUD_CONTAINER_CLI", "false"),
        (
            "FAKECLOUD_DYNAMODB_IMPORT_PATH",
            export_dir.to_str().unwrap(),
        ),
        (
            "FAKECLOUD_DYNAMODB_DESCRIBE_TABLE",
            describe_table.to_str().unwrap(),
        ),
    ])
    .await;
    let client = server.dynamodb_client().await;

    // GetItem on the composite key {Artist: "Foo", SongTitle: "Bar"} carries
    // the full type check: every AttributeValue variant must survive the
    // export → in-memory-store round-trip.
    let got = client
        .get_item()
        .table_name("Music")
        .key("Artist", AttributeValue::S("Foo".into()))
        .key("SongTitle", AttributeValue::S("Bar".into()))
        .send()
        .await
        .unwrap();
    let item = got.item().expect("Foo/Bar item should exist after import");

    // Scalars.
    assert_eq!(item["str"].as_s().unwrap(), "hello");
    assert_eq!(item["Plays"].as_n().unwrap(), "42");
    // "aGVsbG8=" base64-decodes to b"hello".
    assert_eq!(item["bin"].as_b().unwrap().as_ref(), b"hello");
    assert!(*item["flag"].as_bool().unwrap());
    assert!(item["nothing"].is_null());

    // List: all 3 elements, in order.
    let list = item["list"].as_l().unwrap();
    assert_eq!(list.len(), 3);
    assert_eq!(list[0].as_s().unwrap(), "nested");
    assert_eq!(list[1].as_n().unwrap(), "7");
    assert!(!*list[2].as_bool().unwrap());

    // Map: both fields.
    let map = item["map"].as_m().unwrap();
    assert_eq!(map["genre"].as_s().unwrap(), "rock");
    assert_eq!(map["year"].as_n().unwrap(), "1994");

    // Sets: length + membership, order-independent.
    let strset = item["strset"].as_ss().unwrap();
    assert_eq!(strset.len(), 2);
    assert!(strset.contains(&"x".to_string()));
    assert!(strset.contains(&"y".to_string()));

    let numset = item["numset"].as_ns().unwrap();
    assert_eq!(numset.len(), 2);
    assert!(numset.contains(&"1".to_string()));
    assert!(numset.contains(&"2".to_string()));

    // Binary set: compare decoded bytes ("YQ=="=b"a", "Yg=="=b"b").
    let binset: Vec<Vec<u8>> = item["binset"]
        .as_bs()
        .unwrap()
        .iter()
        .map(|b| b.as_ref().to_vec())
        .collect();
    assert_eq!(binset.len(), 2);
    assert!(binset.contains(&b"a".to_vec()));
    assert!(binset.contains(&b"b".to_vec()));

    // Query by the partition key proves the imported table's key schema is
    // live and queryable, not merely stored.
    let query = client
        .query()
        .table_name("Music")
        .key_condition_expression("Artist = :a")
        .expression_attribute_values(":a", AttributeValue::S("Foo".into()))
        .send()
        .await
        .unwrap();
    assert!(
        query
            .items()
            .iter()
            .any(|i| i.get("SongTitle") == Some(&AttributeValue::S("Bar".into()))),
        "Query on Artist=Foo should return the Foo/Bar item"
    );

    // Scan returns all 3 imported items, proving every data-file item loaded.
    let scan = client.scan().table_name("Music").send().await.unwrap();
    assert_eq!(scan.count(), 3, "all 3 exported items should be imported");
}
