mod helpers;

use aws_sdk_s3::primitives::ByteStream;
use helpers::TestServer;

#[tokio::test]
async fn s3_create_list_delete_bucket() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    // Create bucket
    client
        .create_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .unwrap();

    // List buckets
    let resp = client.list_buckets().send().await.unwrap();
    let names: Vec<&str> = resp
        .buckets()
        .iter()
        .map(|b| b.name().unwrap_or_default())
        .collect();
    assert!(names.contains(&"test-bucket"));

    // Delete bucket
    client
        .delete_bucket()
        .bucket("test-bucket")
        .send()
        .await
        .unwrap();

    // Verify deleted
    let resp = client.list_buckets().send().await.unwrap();
    assert!(resp.buckets().is_empty());
}

#[tokio::test]
async fn s3_head_bucket() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    // Head non-existent bucket should fail
    let result = client.head_bucket().bucket("nope").send().await;
    assert!(result.is_err());

    // Create and head
    client
        .create_bucket()
        .bucket("exists")
        .send()
        .await
        .unwrap();
    client.head_bucket().bucket("exists").send().await.unwrap();
}

#[tokio::test]
async fn s3_put_get_delete_object() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("my-bucket")
        .send()
        .await
        .unwrap();

    // Put object
    let put_resp = client
        .put_object()
        .bucket("my-bucket")
        .key("hello.txt")
        .body(ByteStream::from_static(b"Hello, S3!"))
        .content_type("text/plain")
        .send()
        .await
        .unwrap();
    assert!(put_resp.e_tag().is_some());

    // Get object
    let get_resp = client
        .get_object()
        .bucket("my-bucket")
        .key("hello.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(get_resp.content_type().unwrap(), "text/plain");
    let body = get_resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"Hello, S3!");

    // Delete object
    client
        .delete_object()
        .bucket("my-bucket")
        .key("hello.txt")
        .send()
        .await
        .unwrap();

    // Verify deleted
    let result = client
        .get_object()
        .bucket("my-bucket")
        .key("hello.txt")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn s3_head_object() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("head-test")
        .send()
        .await
        .unwrap();

    client
        .put_object()
        .bucket("head-test")
        .key("doc.pdf")
        .body(ByteStream::from_static(b"fake-pdf"))
        .content_type("application/pdf")
        .send()
        .await
        .unwrap();

    let head = client
        .head_object()
        .bucket("head-test")
        .key("doc.pdf")
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_length().unwrap(), 8);
    assert_eq!(head.content_type().unwrap(), "application/pdf");
    assert!(head.e_tag().is_some());
}

#[tokio::test]
async fn s3_list_objects_v2() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("list-test")
        .send()
        .await
        .unwrap();

    for key in &["a.txt", "b.txt", "dir/c.txt", "dir/d.txt"] {
        client
            .put_object()
            .bucket("list-test")
            .key(*key)
            .body(ByteStream::from_static(b"x"))
            .send()
            .await
            .unwrap();
    }

    // List all
    let resp = client
        .list_objects_v2()
        .bucket("list-test")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.key_count().unwrap(), 4);

    // List with prefix
    let resp = client
        .list_objects_v2()
        .bucket("list-test")
        .prefix("dir/")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.key_count().unwrap(), 2);

    // List with delimiter (should group dir/ as common prefix)
    let resp = client
        .list_objects_v2()
        .bucket("list-test")
        .delimiter("/")
        .send()
        .await
        .unwrap();
    // Should have 2 top-level objects + 1 common prefix
    let objects = resp.contents();
    let prefixes = resp.common_prefixes();
    assert_eq!(objects.len(), 2); // a.txt, b.txt
    assert_eq!(prefixes.len(), 1); // dir/
    assert_eq!(prefixes[0].prefix().unwrap(), "dir/");
}

#[tokio::test]
async fn s3_copy_object() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("src-bucket")
        .send()
        .await
        .unwrap();
    client
        .create_bucket()
        .bucket("dst-bucket")
        .send()
        .await
        .unwrap();

    client
        .put_object()
        .bucket("src-bucket")
        .key("original.txt")
        .body(ByteStream::from_static(b"copy me"))
        .send()
        .await
        .unwrap();

    // Copy across buckets
    client
        .copy_object()
        .bucket("dst-bucket")
        .key("copied.txt")
        .copy_source("src-bucket/original.txt")
        .send()
        .await
        .unwrap();

    // Verify copy
    let resp = client
        .get_object()
        .bucket("dst-bucket")
        .key("copied.txt")
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"copy me");
}

#[tokio::test]
async fn s3_delete_objects_batch() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("batch-del")
        .send()
        .await
        .unwrap();

    for key in &["one.txt", "two.txt", "three.txt"] {
        client
            .put_object()
            .bucket("batch-del")
            .key(*key)
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
    }

    // Batch delete
    use aws_sdk_s3::types::{Delete, ObjectIdentifier};
    let delete = Delete::builder()
        .objects(ObjectIdentifier::builder().key("one.txt").build().unwrap())
        .objects(ObjectIdentifier::builder().key("two.txt").build().unwrap())
        .build()
        .unwrap();

    let resp = client
        .delete_objects()
        .bucket("batch-del")
        .delete(delete)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.deleted().len(), 2);

    // Only three.txt should remain
    let list = client
        .list_objects_v2()
        .bucket("batch-del")
        .send()
        .await
        .unwrap();
    assert_eq!(list.key_count().unwrap(), 1);
    assert_eq!(list.contents()[0].key().unwrap(), "three.txt");
}

#[tokio::test]
async fn s3_delete_nonexistent_bucket_fails() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    let result = client.delete_bucket().bucket("ghost-bucket").send().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn s3_delete_nonempty_bucket_fails() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("full-bucket")
        .send()
        .await
        .unwrap();
    client
        .put_object()
        .bucket("full-bucket")
        .key("file.txt")
        .body(ByteStream::from_static(b"hi"))
        .send()
        .await
        .unwrap();

    let result = client.delete_bucket().bucket("full-bucket").send().await;
    assert!(result.is_err());
}

#[tokio::test]
async fn s3_nested_key_paths() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("nested")
        .send()
        .await
        .unwrap();

    let key = "a/b/c/deep.txt";
    client
        .put_object()
        .bucket("nested")
        .key(key)
        .body(ByteStream::from_static(b"deep"))
        .send()
        .await
        .unwrap();

    let resp = client
        .get_object()
        .bucket("nested")
        .key(key)
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"deep");
}

/// Verify that S3 lifecycle configuration can be put and retrieved.
#[tokio::test]
async fn s3_lifecycle_put_get_delete() {
    let server = TestServer::start().await;

    // Use AWS CLI for lifecycle operations (SDK lifecycle types are complex)
    let output = server
        .aws_cli(&["s3api", "create-bucket", "--bucket", "lifecycle-bucket"])
        .await;
    assert!(
        output.success(),
        "create bucket failed: {}",
        output.stderr_text()
    );

    // Put lifecycle configuration
    let lifecycle_json = r#"{
        "Rules": [
            {
                "ID": "expire-logs",
                "Filter": {"Prefix": "logs/"},
                "Status": "Enabled",
                "Expiration": {"Days": 30}
            },
            {
                "ID": "archive-data",
                "Filter": {"Prefix": "data/"},
                "Status": "Enabled",
                "Transitions": [
                    {"Days": 90, "StorageClass": "GLACIER"}
                ]
            }
        ]
    }"#;

    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-lifecycle-configuration",
            "--bucket",
            "lifecycle-bucket",
            "--lifecycle-configuration",
            lifecycle_json,
        ])
        .await;
    assert!(
        output.success(),
        "put lifecycle failed: {}",
        output.stderr_text()
    );

    // Get lifecycle configuration
    let output = server
        .aws_cli(&[
            "s3api",
            "get-bucket-lifecycle-configuration",
            "--bucket",
            "lifecycle-bucket",
        ])
        .await;
    assert!(
        output.success(),
        "get lifecycle failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let rules = json["Rules"].as_array().unwrap();
    assert_eq!(rules.len(), 2);

    // Delete lifecycle configuration
    let output = server
        .aws_cli(&[
            "s3api",
            "delete-bucket-lifecycle",
            "--bucket",
            "lifecycle-bucket",
        ])
        .await;
    assert!(
        output.success(),
        "delete lifecycle failed: {}",
        output.stderr_text()
    );

    // Verify deleted — should fail with NoSuchLifecycleConfiguration
    let output = server
        .aws_cli(&[
            "s3api",
            "get-bucket-lifecycle-configuration",
            "--bucket",
            "lifecycle-bucket",
        ])
        .await;
    assert!(!output.success(), "expected error after lifecycle deletion");
}
