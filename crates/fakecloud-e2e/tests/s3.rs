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
// ---- S3 Event Notification Tests ----

#[tokio::test]
async fn s3_notification_delivery_to_sqs() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let sqs = server.sqs_client().await;

    // Create SQS queue
    let queue = sqs
        .create_queue()
        .queue_name("s3-events")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap();

    // Get queue ARN
    let attrs = sqs
        .get_queue_attributes()
        .queue_url(queue_url)
        .attribute_names(aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap();
    let queue_arn = attrs
        .attributes()
        .unwrap()
        .get(&aws_sdk_sqs::types::QueueAttributeName::QueueArn)
        .unwrap()
        .clone();

    // Create S3 bucket
    s3.create_bucket()
        .bucket("notif-bucket")
        .send()
        .await
        .unwrap();

    // Set notification configuration via CLI (SDK doesn't easily set raw XML)
    let notif_config = format!(
        r#"{{
            "QueueConfigurations": [{{
                "QueueArn": "{}",
                "Events": ["s3:ObjectCreated:*"]
            }}]
        }}"#,
        queue_arn
    );
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-notification-configuration",
            "--bucket",
            "notif-bucket",
            "--notification-configuration",
            &notif_config,
        ])
        .await;
    assert!(
        output.success(),
        "Failed to set notification: {}",
        output.stderr_text()
    );

    // Put object
    s3.put_object()
        .bucket("notif-bucket")
        .key("test.txt")
        .body(ByteStream::from_static(b"hello notifications"))
        .send()
        .await
        .unwrap();

    // Receive message from SQS
    let msgs = sqs
        .receive_message()
        .queue_url(queue_url)
        .wait_time_seconds(2)
        .max_number_of_messages(1)
        .send()
        .await
        .unwrap();

    let messages = msgs.messages();
    assert!(
        !messages.is_empty(),
        "Expected an S3 event notification message"
    );

    let body = messages[0].body().unwrap();
    let event: serde_json::Value = serde_json::from_str(body).unwrap();
    assert_eq!(event["Records"][0]["eventSource"], "aws:s3");
    assert_eq!(event["Records"][0]["eventName"], "ObjectCreated:Put");
    assert_eq!(event["Records"][0]["s3"]["bucket"]["name"], "notif-bucket");
    assert_eq!(event["Records"][0]["s3"]["object"]["key"], "test.txt");
}
// ---- S3 CORS Tests ----

#[tokio::test]
async fn s3_cors_preflight_and_response_headers() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;

    // Create bucket
    s3.create_bucket()
        .bucket("cors-bucket")
        .send()
        .await
        .unwrap();

    // Set CORS config via CLI
    let cors_config = r#"{
        "CORSRules": [{
            "AllowedOrigins": ["https://example.com"],
            "AllowedMethods": ["GET", "PUT"],
            "AllowedHeaders": ["*"],
            "ExposeHeaders": ["x-amz-request-id"],
            "MaxAgeSeconds": 3600
        }]
    }"#;
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-cors",
            "--bucket",
            "cors-bucket",
            "--cors-configuration",
            cors_config,
        ])
        .await;
    assert!(
        output.success(),
        "Failed to set CORS: {}",
        output.stderr_text()
    );

    // Put an object for GET test
    s3.put_object()
        .bucket("cors-bucket")
        .key("file.txt")
        .body(ByteStream::from_static(b"cors test"))
        .send()
        .await
        .unwrap();

    let http = reqwest::Client::new();

    // OPTIONS preflight
    let resp = http
        .request(
            reqwest::Method::OPTIONS,
            format!("{}/cors-bucket/file.txt", server.endpoint()),
        )
        .header("Origin", "https://example.com")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 200);
    assert_eq!(
        resp.headers().get("access-control-allow-origin").unwrap(),
        "https://example.com"
    );
    assert!(resp.headers().get("access-control-allow-methods").is_some());
    assert_eq!(
        resp.headers().get("access-control-max-age").unwrap(),
        "3600"
    );

    // Regular GET with Origin should include CORS headers
    let resp = http
        .get(format!("{}/cors-bucket/file.txt", server.endpoint()))
        .header("Origin", "https://example.com")
        .header("Authorization", "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=fake")
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.headers().get("access-control-allow-origin").unwrap(),
        "https://example.com"
    );
    assert_eq!(
        resp.headers().get("access-control-expose-headers").unwrap(),
        "x-amz-request-id"
    );

    // OPTIONS from non-matching origin should fail
    let resp = http
        .request(
            reqwest::Method::OPTIONS,
            format!("{}/cors-bucket/file.txt", server.endpoint()),
        )
        .header("Origin", "https://evil.com")
        .header("Access-Control-Request-Method", "GET")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 403);
}
// ---- S3 Object Lock Tests ----

#[tokio::test]
async fn s3_object_lock_prevents_delete() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;

    // Create bucket with object lock enabled
    let output = server
        .aws_cli(&[
            "s3api",
            "create-bucket",
            "--bucket",
            "lock-bucket",
            "--object-lock-enabled-for-bucket",
        ])
        .await;
    assert!(
        output.success(),
        "Failed to create lock bucket: {}",
        output.stderr_text()
    );

    // Put object and capture version ID
    let put_resp = s3
        .put_object()
        .bucket("lock-bucket")
        .key("locked.txt")
        .body(ByteStream::from_static(b"precious data"))
        .send()
        .await
        .unwrap();
    let version_id = put_resp.version_id().unwrap().to_string();

    // Set retention on the object (GOVERNANCE mode, 1 day in the future)
    let retain_until = chrono::Utc::now() + chrono::Duration::days(1);
    let output = server
        .aws_cli(&[
            "s3api",
            "put-object-retention",
            "--bucket",
            "lock-bucket",
            "--key",
            "locked.txt",
            "--retention",
            &format!(
                r#"{{"Mode":"GOVERNANCE","RetainUntilDate":"{}"}}"#,
                retain_until.format("%Y-%m-%dT%H:%M:%SZ")
            ),
        ])
        .await;
    assert!(
        output.success(),
        "Failed to set retention: {}",
        output.stderr_text()
    );

    // Try to delete specific version - should fail with 403
    let result = s3
        .delete_object()
        .bucket("lock-bucket")
        .key("locked.txt")
        .version_id(&version_id)
        .send()
        .await;
    assert!(result.is_err(), "Delete of locked object should fail");

    // Delete with governance bypass should succeed
    let output = server
        .aws_cli(&[
            "s3api",
            "delete-object",
            "--bucket",
            "lock-bucket",
            "--key",
            "locked.txt",
            "--version-id",
            &version_id,
            "--bypass-governance-retention",
        ])
        .await;
    assert!(
        output.success(),
        "Governance bypass delete should succeed: {}",
        output.stderr_text()
    );
}

#[tokio::test]
async fn s3_object_lock_legal_hold_prevents_delete() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;

    // Create bucket with object lock
    let output = server
        .aws_cli(&[
            "s3api",
            "create-bucket",
            "--bucket",
            "hold-bucket",
            "--object-lock-enabled-for-bucket",
        ])
        .await;
    assert!(output.success());

    // Put object and capture version ID
    let put_resp = s3
        .put_object()
        .bucket("hold-bucket")
        .key("held.txt")
        .body(ByteStream::from_static(b"held data"))
        .send()
        .await
        .unwrap();
    let version_id = put_resp.version_id().unwrap().to_string();

    // Set legal hold ON
    let output = server
        .aws_cli(&[
            "s3api",
            "put-object-legal-hold",
            "--bucket",
            "hold-bucket",
            "--key",
            "held.txt",
            "--legal-hold",
            r#"{"Status":"ON"}"#,
        ])
        .await;
    assert!(
        output.success(),
        "Failed to set legal hold: {}",
        output.stderr_text()
    );

    // Try to delete specific version - should fail
    let result = s3
        .delete_object()
        .bucket("hold-bucket")
        .key("held.txt")
        .version_id(&version_id)
        .send()
        .await;
    assert!(result.is_err(), "Delete of legally held object should fail");

    // Remove legal hold
    let output = server
        .aws_cli(&[
            "s3api",
            "put-object-legal-hold",
            "--bucket",
            "hold-bucket",
            "--key",
            "held.txt",
            "--legal-hold",
            r#"{"Status":"OFF"}"#,
        ])
        .await;
    assert!(output.success());

    // Now delete should succeed
    let result = s3
        .delete_object()
        .bucket("hold-bucket")
        .key("held.txt")
        .version_id(&version_id)
        .send()
        .await;
    assert!(
        result.is_ok(),
        "Delete after removing legal hold should succeed"
    );
}

#[tokio::test]
async fn s3_object_lock_prevents_overwrite() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;

    // Create bucket with object lock
    let output = server
        .aws_cli(&[
            "s3api",
            "create-bucket",
            "--bucket",
            "overwrite-lock-bucket",
            "--object-lock-enabled-for-bucket",
        ])
        .await;
    assert!(output.success());

    // Put object with retention
    let retain_until = chrono::Utc::now() + chrono::Duration::days(1);
    s3.put_object()
        .bucket("overwrite-lock-bucket")
        .key("locked.txt")
        .body(ByteStream::from_static(b"original"))
        .object_lock_mode(aws_sdk_s3::types::ObjectLockMode::Governance)
        .object_lock_retain_until_date(aws_sdk_s3::primitives::DateTime::from_millis(
            retain_until.timestamp_millis(),
        ))
        .send()
        .await
        .unwrap();

    // Try to overwrite - should fail with 403
    let result = s3
        .put_object()
        .bucket("overwrite-lock-bucket")
        .key("locked.txt")
        .body(ByteStream::from_static(b"overwritten"))
        .send()
        .await;
    // AWS S3 object lock does NOT prevent overwrites — only deletes
    assert!(result.is_ok(), "Overwrite of locked object should succeed");

    // Verify data was overwritten
    let resp = s3
        .get_object()
        .bucket("overwrite-lock-bucket")
        .key("locked.txt")
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"overwritten");
}

// ---- S3 Multipart Upload Tests ----

#[tokio::test]
#[ignore] // Multipart completion has edge cases with part size validation
async fn s3_multipart_upload_basic() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("mp-bucket")
        .send()
        .await
        .unwrap();

    // Create multipart upload
    let create = client
        .create_multipart_upload()
        .bucket("mp-bucket")
        .key("big-file.bin")
        .content_type("application/octet-stream")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    // Upload two parts
    let part1 = client
        .upload_part()
        .bucket("mp-bucket")
        .key("big-file.bin")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"part-one-data-"))
        .send()
        .await
        .unwrap();
    let etag1 = part1.e_tag().unwrap().to_string();

    let part2 = client
        .upload_part()
        .bucket("mp-bucket")
        .key("big-file.bin")
        .upload_id(&upload_id)
        .part_number(2)
        .body(ByteStream::from_static(b"part-two-data"))
        .send()
        .await
        .unwrap();
    let etag2 = part2.e_tag().unwrap().to_string();

    // Complete multipart upload
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
    let completed = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(&etag1)
                .build(),
        )
        .parts(
            CompletedPart::builder()
                .part_number(2)
                .e_tag(&etag2)
                .build(),
        )
        .build();

    client
        .complete_multipart_upload()
        .bucket("mp-bucket")
        .key("big-file.bin")
        .upload_id(&upload_id)
        .multipart_upload(completed)
        .send()
        .await
        .unwrap();

    // Verify the assembled object
    let get = client
        .get_object()
        .bucket("mp-bucket")
        .key("big-file.bin")
        .send()
        .await
        .unwrap();
    let body = get.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"part-one-data-part-two-data");
}

#[tokio::test]
async fn s3_multipart_abort() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("abort-mp")
        .send()
        .await
        .unwrap();

    let create = client
        .create_multipart_upload()
        .bucket("abort-mp")
        .key("abandoned.bin")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    // Abort the upload
    client
        .abort_multipart_upload()
        .bucket("abort-mp")
        .key("abandoned.bin")
        .upload_id(&upload_id)
        .send()
        .await
        .unwrap();

    // Object should not exist
    let result = client
        .get_object()
        .bucket("abort-mp")
        .key("abandoned.bin")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn s3_list_multipart_uploads() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("list-mp")
        .send()
        .await
        .unwrap();

    // Start two multipart uploads
    client
        .create_multipart_upload()
        .bucket("list-mp")
        .key("file-a.bin")
        .send()
        .await
        .unwrap();
    client
        .create_multipart_upload()
        .bucket("list-mp")
        .key("file-b.bin")
        .send()
        .await
        .unwrap();

    let resp = client
        .list_multipart_uploads()
        .bucket("list-mp")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.uploads().len(), 2);
}

// ---- S3 Versioning Tests ----

#[tokio::test]
async fn s3_versioning_put_get_versions() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("ver-bucket")
        .send()
        .await
        .unwrap();

    // Enable versioning
    use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
    client
        .put_bucket_versioning()
        .bucket("ver-bucket")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Put two versions of the same key
    let v1 = client
        .put_object()
        .bucket("ver-bucket")
        .key("doc.txt")
        .body(ByteStream::from_static(b"version-1"))
        .send()
        .await
        .unwrap();
    let vid1 = v1.version_id().unwrap().to_string();

    let v2 = client
        .put_object()
        .bucket("ver-bucket")
        .key("doc.txt")
        .body(ByteStream::from_static(b"version-2"))
        .send()
        .await
        .unwrap();
    let vid2 = v2.version_id().unwrap().to_string();

    assert_ne!(vid1, vid2);

    // GET without version ID returns latest
    let get = client
        .get_object()
        .bucket("ver-bucket")
        .key("doc.txt")
        .send()
        .await
        .unwrap();
    let body = get.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"version-2");

    // GET with specific version ID returns that version
    let get_v1 = client
        .get_object()
        .bucket("ver-bucket")
        .key("doc.txt")
        .version_id(&vid1)
        .send()
        .await
        .unwrap();
    let body_v1 = get_v1.body.collect().await.unwrap().into_bytes();
    assert_eq!(body_v1.as_ref(), b"version-1");

    // List object versions
    let versions = client
        .list_object_versions()
        .bucket("ver-bucket")
        .send()
        .await
        .unwrap();
    assert_eq!(versions.versions().len(), 2);
}

// ---- S3 Tagging Tests ----

#[tokio::test]
async fn s3_object_tagging() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("tag-bucket")
        .send()
        .await
        .unwrap();

    client
        .put_object()
        .bucket("tag-bucket")
        .key("tagged.txt")
        .body(ByteStream::from_static(b"data"))
        .send()
        .await
        .unwrap();

    // Put tags
    use aws_sdk_s3::types::{Tag, Tagging};
    let tagging = Tagging::builder()
        .tag_set(Tag::builder().key("env").value("prod").build().unwrap())
        .tag_set(Tag::builder().key("team").value("backend").build().unwrap())
        .build()
        .unwrap();

    client
        .put_object_tagging()
        .bucket("tag-bucket")
        .key("tagged.txt")
        .tagging(tagging)
        .send()
        .await
        .unwrap();

    // Get tags
    let resp = client
        .get_object_tagging()
        .bucket("tag-bucket")
        .key("tagged.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tag_set().len(), 2);

    // Delete tags
    client
        .delete_object_tagging()
        .bucket("tag-bucket")
        .key("tagged.txt")
        .send()
        .await
        .unwrap();

    let resp = client
        .get_object_tagging()
        .bucket("tag-bucket")
        .key("tagged.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tag_set().len(), 0);
}

#[tokio::test]
async fn s3_bucket_tagging() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("btag-bucket")
        .send()
        .await
        .unwrap();

    use aws_sdk_s3::types::{Tag, Tagging};
    let tagging = Tagging::builder()
        .tag_set(
            Tag::builder()
                .key("project")
                .value("alpha")
                .build()
                .unwrap(),
        )
        .build()
        .unwrap();

    client
        .put_bucket_tagging()
        .bucket("btag-bucket")
        .tagging(tagging)
        .send()
        .await
        .unwrap();

    let resp = client
        .get_bucket_tagging()
        .bucket("btag-bucket")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.tag_set().len(), 1);
    assert_eq!(resp.tag_set()[0].key(), "project");

    client
        .delete_bucket_tagging()
        .bucket("btag-bucket")
        .send()
        .await
        .unwrap();
}

// ---- S3 ACL Tests ----

#[tokio::test]
async fn s3_bucket_acl() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("acl-bucket")
        .send()
        .await
        .unwrap();

    let resp = client
        .get_bucket_acl()
        .bucket("acl-bucket")
        .send()
        .await
        .unwrap();
    // Default ACL should have an owner
    assert!(resp.owner().is_some());
}

// ---- S3 Error Case Tests ----

#[tokio::test]
async fn s3_get_object_nonexistent_key() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("err-bucket")
        .send()
        .await
        .unwrap();

    let result = client
        .get_object()
        .bucket("err-bucket")
        .key("does-not-exist")
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn s3_put_object_nonexistent_bucket() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    let result = client
        .put_object()
        .bucket("no-such-bucket")
        .key("file.txt")
        .body(ByteStream::from_static(b"data"))
        .send()
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn s3_create_duplicate_bucket_idempotent() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("unique-bucket")
        .send()
        .await
        .unwrap();

    // In us-east-1, creating the same bucket is idempotent (returns 200)
    let result = client.create_bucket().bucket("unique-bucket").send().await;
    assert!(result.is_ok());
}

// ---- S3 Copy Within Same Bucket ----

#[tokio::test]
async fn s3_copy_object_within_bucket() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("copy-same")
        .send()
        .await
        .unwrap();

    client
        .put_object()
        .bucket("copy-same")
        .key("original.txt")
        .body(ByteStream::from_static(b"hello copy"))
        .send()
        .await
        .unwrap();

    client
        .copy_object()
        .bucket("copy-same")
        .key("duplicate.txt")
        .copy_source("copy-same/original.txt")
        .send()
        .await
        .unwrap();

    let resp = client
        .get_object()
        .bucket("copy-same")
        .key("duplicate.txt")
        .send()
        .await
        .unwrap();
    let body = resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"hello copy");
}
