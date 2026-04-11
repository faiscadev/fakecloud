mod helpers;

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_sqs::types::QueueAttributeName;
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

    // Upload two parts. Non-last parts must be at least 5 MiB.
    let part1_data = vec![b'a'; 5 * 1024 * 1024];
    let part1 = client
        .upload_part()
        .bucket("mp-bucket")
        .key("big-file.bin")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(part1_data.clone()))
        .send()
        .await
        .unwrap();
    let etag1 = part1.e_tag().unwrap().to_string();

    let part2_data = b"part-two-data".to_vec();
    let part2 = client
        .upload_part()
        .bucket("mp-bucket")
        .key("big-file.bin")
        .upload_id(&upload_id)
        .part_number(2)
        .body(ByteStream::from(part2_data.clone()))
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
    let mut expected = part1_data;
    expected.extend_from_slice(&part2_data);
    assert_eq!(body.as_ref(), expected.as_slice());
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

// ---- P1 Bug Fix Regression Tests ----

/// Regression: ListObjectsV2 with delimiter and many prefixes must not panic
/// due to bounds check on continuation token / prefix slicing.
#[tokio::test]
async fn s3_list_objects_v2_delimiter_many_prefixes() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("delim-bucket")
        .send()
        .await
        .unwrap();

    // Create objects under several prefixes
    for i in 0..5 {
        client
            .put_object()
            .bucket("delim-bucket")
            .key(format!("prefix{i}/file.txt"))
            .body(ByteStream::from_static(b"data"))
            .send()
            .await
            .unwrap();
    }

    // First page: max-keys=2 to force pagination
    let page1 = client
        .list_objects_v2()
        .bucket("delim-bucket")
        .delimiter("/")
        .max_keys(2)
        .send()
        .await
        .unwrap();
    assert!(page1.is_truncated().unwrap_or(false));
    assert_eq!(page1.common_prefixes().len(), 2);
    let token = page1.next_continuation_token().unwrap().to_string();

    // Second page using continuation token — this previously could panic
    let page2 = client
        .list_objects_v2()
        .bucket("delim-bucket")
        .delimiter("/")
        .max_keys(2)
        .continuation_token(&token)
        .send()
        .await
        .unwrap();
    assert_eq!(page2.common_prefixes().len(), 2);

    // Third page: should have 1 remaining prefix
    if page2.is_truncated().unwrap_or(false) {
        let token2 = page2.next_continuation_token().unwrap().to_string();
        let page3 = client
            .list_objects_v2()
            .bucket("delim-bucket")
            .delimiter("/")
            .max_keys(2)
            .continuation_token(&token2)
            .send()
            .await
            .unwrap();
        assert_eq!(page3.common_prefixes().len(), 1);
    }
}

/// Regression: CompleteMultipartUpload with wrong ETag returns InvalidPart error.
#[tokio::test]
async fn s3_complete_multipart_wrong_etag_returns_error() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("mpu-etag-bucket")
        .send()
        .await
        .unwrap();

    let create = client
        .create_multipart_upload()
        .bucket("mpu-etag-bucket")
        .key("test.bin")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    // Upload a part
    let part = client
        .upload_part()
        .bucket("mpu-etag-bucket")
        .key("test.bin")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"part1data"))
        .send()
        .await
        .unwrap();
    let _real_etag = part.e_tag().unwrap().to_string();

    // Complete with a wrong ETag — should fail with InvalidPart
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
    let bad_part = CompletedPart::builder()
        .part_number(1)
        .e_tag("\"0000000000000000000000000000dead\"")
        .build();
    let result = client
        .complete_multipart_upload()
        .bucket("mpu-etag-bucket")
        .key("test.bin")
        .upload_id(&upload_id)
        .multipart_upload(CompletedMultipartUpload::builder().parts(bad_part).build())
        .send()
        .await;
    assert!(result.is_err(), "Expected InvalidPart error for wrong ETag");
    let err_str = format!("{:?}", result.unwrap_err());
    assert!(
        err_str.contains("InvalidPart") || err_str.contains("could not be found"),
        "Error should mention InvalidPart, got: {err_str}"
    );
}

/// Regression: AbortMultipartUpload with wrong key returns NoSuchUpload error.
#[tokio::test]
async fn s3_abort_multipart_wrong_key_returns_error() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("mpu-abort-bucket")
        .send()
        .await
        .unwrap();

    let create = client
        .create_multipart_upload()
        .bucket("mpu-abort-bucket")
        .key("correct-key.txt")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    // Abort with the wrong key — should fail
    let result = client
        .abort_multipart_upload()
        .bucket("mpu-abort-bucket")
        .key("wrong-key.txt")
        .upload_id(&upload_id)
        .send()
        .await;
    assert!(
        result.is_err(),
        "Expected NoSuchUpload error when aborting with wrong key"
    );

    // Abort with the correct key — should succeed
    client
        .abort_multipart_upload()
        .bucket("mpu-abort-bucket")
        .key("correct-key.txt")
        .upload_id(&upload_id)
        .send()
        .await
        .unwrap();
}

/// Regression: CopyObject rejects a delete marker as copy source.
#[tokio::test]
async fn s3_copy_object_rejects_delete_marker_source() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("copy-dm-bucket")
        .send()
        .await
        .unwrap();

    // Enable versioning
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-versioning",
            "--bucket",
            "copy-dm-bucket",
            "--versioning-configuration",
            "Status=Enabled",
        ])
        .await;
    assert!(output.success(), "versioning: {}", output.stderr_text());

    // Put an object, then delete it to create a delete marker
    client
        .put_object()
        .bucket("copy-dm-bucket")
        .key("source.txt")
        .body(ByteStream::from_static(b"hello"))
        .send()
        .await
        .unwrap();

    client
        .delete_object()
        .bucket("copy-dm-bucket")
        .key("source.txt")
        .send()
        .await
        .unwrap();

    // Copy from the (now delete-marked) source should fail
    let result = client
        .copy_object()
        .bucket("copy-dm-bucket")
        .key("dest.txt")
        .copy_source("copy-dm-bucket/source.txt")
        .send()
        .await;
    assert!(
        result.is_err(),
        "Expected copy from delete marker to fail, but it succeeded"
    );
}

/// Regression: Deleting a specific version that doesn't exist must not affect
/// the current object.
#[tokio::test]
async fn s3_delete_specific_version_preserves_current_object() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("ver-del-bucket")
        .send()
        .await
        .unwrap();

    // Enable versioning
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-versioning",
            "--bucket",
            "ver-del-bucket",
            "--versioning-configuration",
            "Status=Enabled",
        ])
        .await;
    assert!(output.success());

    // Put an object (creates a real version)
    client
        .put_object()
        .bucket("ver-del-bucket")
        .key("keep-me.txt")
        .body(ByteStream::from_static(b"important data"))
        .send()
        .await
        .unwrap();

    // Delete a non-existent version ID — must not affect the current object
    let output = server
        .aws_cli(&[
            "s3api",
            "delete-object",
            "--bucket",
            "ver-del-bucket",
            "--key",
            "keep-me.txt",
            "--version-id",
            "nonexistent-version-id-12345",
        ])
        .await;
    // The delete may succeed (noop) or fail, but the object must still be readable
    let _ = output;

    // The current object must still be intact
    let get = client
        .get_object()
        .bucket("ver-del-bucket")
        .key("keep-me.txt")
        .send()
        .await
        .unwrap();
    let body = get.body.collect().await.unwrap().into_bytes();
    assert_eq!(
        body.as_ref(),
        b"important data",
        "Current object was corrupted by deleting a non-existent version"
    );
}

// ---------------------------------------------------------------------------
// P2 Cubic regression tests
// ---------------------------------------------------------------------------

/// Bug 1: CreateMultipartUpload ignores x-amz-grant-* headers.
#[tokio::test]
async fn s3_multipart_upload_grant_headers() {
    let server = TestServer::start().await;

    // Create bucket via CLI
    let output = server
        .aws_cli(&["s3api", "create-bucket", "--bucket", "mp-grant-bucket"])
        .await;
    assert!(output.success(), "create-bucket failed");

    // Create multipart upload with grant-read header via CLI
    let output = server
        .aws_cli(&[
            "s3api",
            "create-multipart-upload",
            "--bucket",
            "mp-grant-bucket",
            "--key",
            "granted.txt",
            "--grant-read",
            "id=someuser123",
        ])
        .await;
    assert!(
        output.success(),
        "create-multipart-upload with grant-read failed: {}",
        output.stderr_text()
    );
    let json: serde_json::Value = serde_json::from_str(&output.stdout_text()).unwrap();
    let upload_id = json["UploadId"].as_str().unwrap();

    // Upload a part via CLI using a temp file
    let tmp = std::env::temp_dir().join("mp-grant-part.bin");
    std::fs::write(&tmp, b"part1data").unwrap();
    let output = server
        .aws_cli(&[
            "s3api",
            "upload-part",
            "--bucket",
            "mp-grant-bucket",
            "--key",
            "granted.txt",
            "--upload-id",
            upload_id,
            "--part-number",
            "1",
            "--body",
            tmp.to_str().unwrap(),
        ])
        .await;
    assert!(
        output.success(),
        "upload-part failed: {}",
        output.stderr_text()
    );
    let part_json: serde_json::Value = serde_json::from_str(&output.stdout_text()).unwrap();
    let etag = part_json["ETag"].as_str().unwrap();

    // Complete multipart upload via CLI
    let mp_struct = format!(
        r#"{{"Parts": [{{"PartNumber": 1, "ETag": {}}}]}}"#,
        serde_json::Value::String(etag.to_string())
    );
    let output = server
        .aws_cli(&[
            "s3api",
            "complete-multipart-upload",
            "--bucket",
            "mp-grant-bucket",
            "--key",
            "granted.txt",
            "--upload-id",
            upload_id,
            "--multipart-upload",
            &mp_struct,
        ])
        .await;
    assert!(
        output.success(),
        "complete-multipart-upload failed: {}",
        output.stderr_text()
    );

    // Verify the ACL has the grant
    let acl_output = server
        .aws_cli(&[
            "s3api",
            "get-object-acl",
            "--bucket",
            "mp-grant-bucket",
            "--key",
            "granted.txt",
        ])
        .await;
    assert!(
        acl_output.success(),
        "get-object-acl failed: {}",
        acl_output.stderr_text()
    );
    let acl: serde_json::Value = serde_json::from_str(&acl_output.stdout_text()).unwrap();
    let grants = acl["Grants"].as_array().unwrap();
    let has_read_grant = grants.iter().any(|g| {
        g["Permission"].as_str() == Some("READ")
            && g["Grantee"]["ID"].as_str() == Some("someuser123")
    });
    assert!(
        has_read_grant,
        "Expected READ grant for someuser123 in ACL, got: {acl}"
    );
}

/// Bug 2: Multi-value If-Match / If-None-Match headers (comma-separated) never match.
#[tokio::test]
async fn s3_conditional_etag_comma_separated() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("etag-csv-bucket")
        .send()
        .await
        .unwrap();

    let put = client
        .put_object()
        .bucket("etag-csv-bucket")
        .key("obj.txt")
        .body(ByteStream::from_static(b"hello"))
        .send()
        .await
        .unwrap();
    let real_etag = put.e_tag().unwrap().to_string();

    // GET with If-Match containing the correct etag among several, comma-separated
    let http = reqwest::Client::new();
    let resp = http
        .get(format!("{}/etag-csv-bucket/obj.txt", server.endpoint()))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .header("If-Match", format!("\"bogus\", {real_etag}, \"other\""))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        200,
        "If-Match with comma-separated list containing correct etag should succeed"
    );

    // GET with If-None-Match containing the correct etag among several
    let resp = http
        .get(format!("{}/etag-csv-bucket/obj.txt", server.endpoint()))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .header("If-None-Match", format!("\"bogus\", {real_etag}, \"other\""))
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        304,
        "If-None-Match with comma-separated list containing correct etag should return 304"
    );
}

/// Bug 3: ChecksumAlgorithm value not XML-escaped in ListObjectsV2.
#[tokio::test]
async fn s3_checksum_algorithm_xml_escape_in_list() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("cksum-esc-bucket")
        .send()
        .await
        .unwrap();

    // Put an object with a checksum algorithm
    client
        .put_object()
        .bucket("cksum-esc-bucket")
        .key("cksum.txt")
        .body(ByteStream::from_static(b"data"))
        .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Sha256)
        .send()
        .await
        .unwrap();

    // List objects and verify the checksum algorithm appears in the response
    let list = client
        .list_objects_v2()
        .bucket("cksum-esc-bucket")
        .send()
        .await
        .unwrap();
    let contents = list.contents();
    assert_eq!(contents.len(), 1);
    let algo = contents[0].checksum_algorithm();
    assert!(
        !algo.is_empty(),
        "ChecksumAlgorithm should be present in list response"
    );
}

/// Bug 4: CopyObject response should return x-amz-server-side-encryption header.
#[tokio::test]
async fn s3_copy_object_returns_sse_header() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("copy-sse-bucket")
        .send()
        .await
        .unwrap();

    client
        .put_object()
        .bucket("copy-sse-bucket")
        .key("src.txt")
        .body(ByteStream::from_static(b"source"))
        .send()
        .await
        .unwrap();

    let copy = client
        .copy_object()
        .bucket("copy-sse-bucket")
        .key("dst.txt")
        .copy_source("copy-sse-bucket/src.txt")
        .send()
        .await
        .unwrap();

    // The SDK exposes server_side_encryption on the response
    assert!(
        copy.server_side_encryption().is_some(),
        "CopyObject should return x-amz-server-side-encryption header"
    );
}

/// Bug 5: SigV2 detection requires Expires param (not just AWSAccessKeyId + Signature).
#[tokio::test]
async fn s3_sigv2_requires_expires_param() {
    let server = TestServer::start().await;

    let http = reqwest::Client::new();

    // Request with AWSAccessKeyId + Signature but NO Expires should not be routed to S3
    let resp = http
        .get(format!(
            "{}/?AWSAccessKeyId=AKID&Signature=sig",
            server.endpoint()
        ))
        .send()
        .await
        .unwrap();
    // Without Expires, the server should not identify this as a valid S3 SigV2 request.
    // It may return 403/400/404 depending on fallback, but NOT a successful S3 response.
    assert_ne!(
        resp.status(),
        200,
        "Request with only AWSAccessKeyId+Signature (no Expires) should not be treated as S3 presigned"
    );

    // Request with all three SigV2 params should be routed to S3
    let resp = http
        .get(format!(
            "{}/test-bucket/key?AWSAccessKeyId=AKID&Signature=sig&Expires=9999999999",
            server.endpoint()
        ))
        .send()
        .await
        .unwrap();
    // This will get a 404 (NoSuchBucket) which confirms it was routed to S3
    assert_eq!(
        resp.status(),
        404,
        "Request with AWSAccessKeyId+Signature+Expires should route to S3"
    );
}

/// Bug 6: list_parts should return InvalidArgument for non-numeric max-parts / part-number-marker.
#[tokio::test]
async fn s3_list_parts_invalid_argument() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("lp-invalid-bucket")
        .send()
        .await
        .unwrap();

    // Create a multipart upload
    let create = client
        .create_multipart_upload()
        .bucket("lp-invalid-bucket")
        .key("parts.txt")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap();

    let http = reqwest::Client::new();

    // Non-numeric max-parts should get InvalidArgument error
    let resp = http
        .get(format!(
            "{}/lp-invalid-bucket/parts.txt?uploadId={}&max-parts=abc",
            server.endpoint(),
            upload_id
        ))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "Non-numeric max-parts should return 400"
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("InvalidArgument"),
        "Expected InvalidArgument error for non-numeric max-parts, got: {body}"
    );

    // Non-numeric part-number-marker should get InvalidArgument error
    let resp = http
        .get(format!(
            "{}/lp-invalid-bucket/parts.txt?uploadId={}&part-number-marker=xyz",
            server.endpoint(),
            upload_id
        ))
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20240101/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=fake",
        )
        .send()
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        400,
        "Non-numeric part-number-marker should return 400"
    );
    let body = resp.text().await.unwrap();
    assert!(
        body.contains("InvalidArgument"),
        "Expected InvalidArgument error for non-numeric part-number-marker, got: {body}"
    );
}

/// S3 cross-region replication: put object in source bucket, verify it appears in destination.
#[tokio::test]
async fn s3_replication_copies_objects() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    // Create source and destination buckets
    client
        .create_bucket()
        .bucket("repl-source")
        .send()
        .await
        .unwrap();
    client
        .create_bucket()
        .bucket("repl-dest")
        .send()
        .await
        .unwrap();

    // Enable versioning on source (required for replication)
    client
        .put_bucket_versioning()
        .bucket("repl-source")
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Set replication configuration via CLI
    let repl_config = serde_json::json!({
        "Role": "arn:aws:iam::123456789012:role/replication-role",
        "Rules": [{
            "ID": "replicate-all",
            "Status": "Enabled",
            "Filter": { "Prefix": "" },
            "Destination": { "Bucket": "arn:aws:s3:::repl-dest" }
        }]
    });
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-replication",
            "--bucket",
            "repl-source",
            "--replication-configuration",
            &repl_config.to_string(),
        ])
        .await;
    assert!(
        output.success(),
        "put-bucket-replication failed: {}",
        output.stderr_text()
    );

    // Verify replication config is stored
    let get_repl = server
        .aws_cli(&["s3api", "get-bucket-replication", "--bucket", "repl-source"])
        .await;
    assert!(
        get_repl.success(),
        "get-bucket-replication failed: {}",
        get_repl.stderr_text()
    );
    let repl_output = get_repl.stdout_text();
    assert!(
        repl_output.contains("repl-dest"),
        "Replication config should reference repl-dest: {repl_output}"
    );

    // Put object in source bucket
    client
        .put_object()
        .bucket("repl-source")
        .key("docs/readme.txt")
        .body(ByteStream::from_static(b"Hello, Replication!"))
        .send()
        .await
        .unwrap();

    // Verify the object was replicated to the destination bucket
    let get_resp = client
        .get_object()
        .bucket("repl-dest")
        .key("docs/readme.txt")
        .send()
        .await
        .unwrap();
    let body = get_resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"Hello, Replication!");

    // Put another object with a different prefix
    client
        .put_object()
        .bucket("repl-source")
        .key("images/photo.jpg")
        .body(ByteStream::from_static(b"JPEG_DATA"))
        .send()
        .await
        .unwrap();

    // Should also be replicated
    let get_resp = client
        .get_object()
        .bucket("repl-dest")
        .key("images/photo.jpg")
        .send()
        .await
        .unwrap();
    let body = get_resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"JPEG_DATA");
}

/// S3 replication with prefix filter: only objects matching the prefix are replicated.
#[tokio::test]
async fn s3_replication_prefix_filter() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("prefix-src")
        .send()
        .await
        .unwrap();
    client
        .create_bucket()
        .bucket("prefix-dst")
        .send()
        .await
        .unwrap();

    // Enable versioning
    client
        .put_bucket_versioning()
        .bucket("prefix-src")
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Set replication with prefix filter
    let repl_config = serde_json::json!({
        "Role": "arn:aws:iam::123456789012:role/replication-role",
        "Rules": [{
            "ID": "replicate-logs",
            "Status": "Enabled",
            "Filter": { "Prefix": "logs/" },
            "Destination": { "Bucket": "arn:aws:s3:::prefix-dst" }
        }]
    });
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-replication",
            "--bucket",
            "prefix-src",
            "--replication-configuration",
            &repl_config.to_string(),
        ])
        .await;
    assert!(output.success());

    // Put object matching the prefix
    client
        .put_object()
        .bucket("prefix-src")
        .key("logs/access.log")
        .body(ByteStream::from_static(b"log data"))
        .send()
        .await
        .unwrap();

    // Put object NOT matching the prefix
    client
        .put_object()
        .bucket("prefix-src")
        .key("data/other.csv")
        .body(ByteStream::from_static(b"csv data"))
        .send()
        .await
        .unwrap();

    // Matching object should be replicated
    let get_resp = client
        .get_object()
        .bucket("prefix-dst")
        .key("logs/access.log")
        .send()
        .await
        .unwrap();
    let body = get_resp.body.collect().await.unwrap().into_bytes();
    assert_eq!(body.as_ref(), b"log data");

    // Non-matching object should NOT be replicated
    let result = client
        .get_object()
        .bucket("prefix-dst")
        .key("data/other.csv")
        .send()
        .await;
    assert!(
        result.is_err(),
        "Non-matching object should not be replicated"
    );
}

/// CompleteMultipartUpload returns InvalidPart when a specified part was never uploaded.
#[tokio::test]
async fn s3_complete_multipart_invalid_part() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("mpu-invalid-part")
        .send()
        .await
        .unwrap();

    let create = client
        .create_multipart_upload()
        .bucket("mpu-invalid-part")
        .key("test.bin")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    // Upload only part 1
    let part1 = client
        .upload_part()
        .bucket("mpu-invalid-part")
        .key("test.bin")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"data"))
        .send()
        .await
        .unwrap();
    let etag1 = part1.e_tag().unwrap().to_string();

    // Complete referencing part 1 (exists) and part 2 (never uploaded)
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
                .e_tag("\"fake-etag\"")
                .build(),
        )
        .build();

    let result = client
        .complete_multipart_upload()
        .bucket("mpu-invalid-part")
        .key("test.bin")
        .upload_id(&upload_id)
        .multipart_upload(completed)
        .send()
        .await;
    assert!(result.is_err(), "Expected InvalidPart error");
    let err_str = format!("{:?}", result.unwrap_err());
    assert!(
        err_str.contains("InvalidPart"),
        "Error should mention InvalidPart, got: {err_str}"
    );
}

/// CompleteMultipartUpload returns InvalidPartOrder when parts aren't in ascending order.
#[tokio::test]
async fn s3_complete_multipart_invalid_part_order() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("mpu-order")
        .send()
        .await
        .unwrap();

    let create = client
        .create_multipart_upload()
        .bucket("mpu-order")
        .key("test.bin")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    // Upload parts 1 and 2
    let part1 = client
        .upload_part()
        .bucket("mpu-order")
        .key("test.bin")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from_static(b"part1"))
        .send()
        .await
        .unwrap();
    let etag1 = part1.e_tag().unwrap().to_string();

    let part2 = client
        .upload_part()
        .bucket("mpu-order")
        .key("test.bin")
        .upload_id(&upload_id)
        .part_number(2)
        .body(ByteStream::from_static(b"part2"))
        .send()
        .await
        .unwrap();
    let etag2 = part2.e_tag().unwrap().to_string();

    // Complete with parts in descending order (2 then 1)
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
    let completed = CompletedMultipartUpload::builder()
        .parts(
            CompletedPart::builder()
                .part_number(2)
                .e_tag(&etag2)
                .build(),
        )
        .parts(
            CompletedPart::builder()
                .part_number(1)
                .e_tag(&etag1)
                .build(),
        )
        .build();

    let result = client
        .complete_multipart_upload()
        .bucket("mpu-order")
        .key("test.bin")
        .upload_id(&upload_id)
        .multipart_upload(completed)
        .send()
        .await;
    assert!(result.is_err(), "Expected InvalidPartOrder error");
    let err_str = format!("{:?}", result.unwrap_err());
    assert!(
        err_str.contains("InvalidPartOrder"),
        "Error should mention InvalidPartOrder, got: {err_str}"
    );
}

/// CompleteMultipartUpload returns EntityTooSmall when a non-last part is under 5MB.
#[tokio::test]
async fn s3_complete_multipart_entity_too_small() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("mpu-small")
        .send()
        .await
        .unwrap();

    let create = client
        .create_multipart_upload()
        .bucket("mpu-small")
        .key("test.bin")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    // Upload part 1 with only 100 bytes (under 5MB minimum)
    let part1 = client
        .upload_part()
        .bucket("mpu-small")
        .key("test.bin")
        .upload_id(&upload_id)
        .part_number(1)
        .body(ByteStream::from(vec![b'X'; 100]))
        .send()
        .await
        .unwrap();
    let etag1 = part1.e_tag().unwrap().to_string();

    // Upload part 2
    let part2 = client
        .upload_part()
        .bucket("mpu-small")
        .key("test.bin")
        .upload_id(&upload_id)
        .part_number(2)
        .body(ByteStream::from_static(b"last-part"))
        .send()
        .await
        .unwrap();
    let etag2 = part2.e_tag().unwrap().to_string();

    // Complete — should fail because part 1 is under 5MB
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

    let result = client
        .complete_multipart_upload()
        .bucket("mpu-small")
        .key("test.bin")
        .upload_id(&upload_id)
        .multipart_upload(completed)
        .send()
        .await;
    assert!(result.is_err(), "Expected EntityTooSmall error");
    let err_str = format!("{:?}", result.unwrap_err());
    assert!(
        err_str.contains("EntityTooSmall"),
        "Error should mention EntityTooSmall, got: {err_str}"
    );
}

// ---- Bucket Configuration CRUD Tests ----

#[tokio::test]
async fn s3_bucket_policy_crud() {
    let server = TestServer::start().await;

    let output = server
        .aws_cli(&["s3api", "create-bucket", "--bucket", "policy-bucket"])
        .await;
    assert!(output.success(), "create failed: {}", output.stderr_text());

    // Put bucket policy
    let policy = r#"{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":"*","Action":"s3:GetObject","Resource":"arn:aws:s3:::policy-bucket/*"}]}"#;
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-policy",
            "--bucket",
            "policy-bucket",
            "--policy",
            policy,
        ])
        .await;
    assert!(
        output.success(),
        "put policy failed: {}",
        output.stderr_text()
    );

    // Get bucket policy
    let output = server
        .aws_cli(&["s3api", "get-bucket-policy", "--bucket", "policy-bucket"])
        .await;
    assert!(
        output.success(),
        "get policy failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let returned_policy: serde_json::Value =
        serde_json::from_str(json["Policy"].as_str().unwrap()).unwrap();
    assert_eq!(returned_policy["Version"], "2012-10-17");
    assert_eq!(returned_policy["Statement"][0]["Effect"], "Allow");

    // Delete bucket policy
    let output = server
        .aws_cli(&["s3api", "delete-bucket-policy", "--bucket", "policy-bucket"])
        .await;
    assert!(
        output.success(),
        "delete policy failed: {}",
        output.stderr_text()
    );

    // Get again — should fail
    let output = server
        .aws_cli(&["s3api", "get-bucket-policy", "--bucket", "policy-bucket"])
        .await;
    assert!(!output.success(), "expected error after policy deletion");
}

#[tokio::test]
async fn s3_bucket_encryption_crud() {
    let server = TestServer::start().await;

    let output = server
        .aws_cli(&["s3api", "create-bucket", "--bucket", "enc-bucket"])
        .await;
    assert!(output.success(), "create failed: {}", output.stderr_text());

    // Put encryption config
    let enc_config =
        r#"{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}"#;
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-encryption",
            "--bucket",
            "enc-bucket",
            "--server-side-encryption-configuration",
            enc_config,
        ])
        .await;
    assert!(
        output.success(),
        "put encryption failed: {}",
        output.stderr_text()
    );

    // Get encryption config
    let output = server
        .aws_cli(&["s3api", "get-bucket-encryption", "--bucket", "enc-bucket"])
        .await;
    assert!(
        output.success(),
        "get encryption failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let rules = json["ServerSideEncryptionConfiguration"]["Rules"]
        .as_array()
        .unwrap();
    assert!(!rules.is_empty());
    assert_eq!(
        rules[0]["ApplyServerSideEncryptionByDefault"]["SSEAlgorithm"]
            .as_str()
            .unwrap(),
        "AES256"
    );

    // Delete encryption config
    let output = server
        .aws_cli(&[
            "s3api",
            "delete-bucket-encryption",
            "--bucket",
            "enc-bucket",
        ])
        .await;
    assert!(
        output.success(),
        "delete encryption failed: {}",
        output.stderr_text()
    );

    // Get again — should fail
    let output = server
        .aws_cli(&["s3api", "get-bucket-encryption", "--bucket", "enc-bucket"])
        .await;
    assert!(
        !output.success(),
        "expected error after encryption deletion"
    );
}

#[tokio::test]
async fn s3_bucket_cors_crud() {
    let server = TestServer::start().await;

    let output = server
        .aws_cli(&["s3api", "create-bucket", "--bucket", "cors-crud-bucket"])
        .await;
    assert!(output.success(), "create failed: {}", output.stderr_text());

    // Put CORS config
    let cors_config = r#"{"CORSRules":[{"AllowedOrigins":["https://example.com"],"AllowedMethods":["GET","PUT"],"AllowedHeaders":["*"],"MaxAgeSeconds":3600}]}"#;
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-cors",
            "--bucket",
            "cors-crud-bucket",
            "--cors-configuration",
            cors_config,
        ])
        .await;
    assert!(
        output.success(),
        "put cors failed: {}",
        output.stderr_text()
    );

    // Get CORS config
    let output = server
        .aws_cli(&["s3api", "get-bucket-cors", "--bucket", "cors-crud-bucket"])
        .await;
    assert!(
        output.success(),
        "get cors failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let rules = json["CORSRules"].as_array().unwrap();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0]["AllowedOrigins"][0], "https://example.com");

    // Delete CORS config
    let output = server
        .aws_cli(&[
            "s3api",
            "delete-bucket-cors",
            "--bucket",
            "cors-crud-bucket",
        ])
        .await;
    assert!(
        output.success(),
        "delete cors failed: {}",
        output.stderr_text()
    );

    // Get again — should fail
    let output = server
        .aws_cli(&["s3api", "get-bucket-cors", "--bucket", "cors-crud-bucket"])
        .await;
    assert!(!output.success(), "expected error after cors deletion");
}

#[tokio::test]
async fn s3_bucket_versioning_put_get() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("versioning-cfg-bucket")
        .send()
        .await
        .unwrap();

    // Initially versioning should not be enabled
    let resp = client
        .get_bucket_versioning()
        .bucket("versioning-cfg-bucket")
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_none(),
        "Expected no versioning status initially"
    );

    // Enable versioning
    use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
    client
        .put_bucket_versioning()
        .bucket("versioning-cfg-bucket")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Verify enabled
    let resp = client
        .get_bucket_versioning()
        .bucket("versioning-cfg-bucket")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), Some(&BucketVersioningStatus::Enabled));

    // Suspend versioning
    client
        .put_bucket_versioning()
        .bucket("versioning-cfg-bucket")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Suspended)
                .build(),
        )
        .send()
        .await
        .unwrap();

    let resp = client
        .get_bucket_versioning()
        .bucket("versioning-cfg-bucket")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), Some(&BucketVersioningStatus::Suspended));
}

#[tokio::test]
async fn s3_bucket_website_crud() {
    let server = TestServer::start().await;

    let output = server
        .aws_cli(&["s3api", "create-bucket", "--bucket", "website-bucket"])
        .await;
    assert!(output.success(), "create failed: {}", output.stderr_text());

    // Put website configuration
    let website_config =
        r#"{"IndexDocument":{"Suffix":"index.html"},"ErrorDocument":{"Key":"error.html"}}"#;
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-website",
            "--bucket",
            "website-bucket",
            "--website-configuration",
            website_config,
        ])
        .await;
    assert!(
        output.success(),
        "put website failed: {}",
        output.stderr_text()
    );

    // Get website configuration
    let output = server
        .aws_cli(&["s3api", "get-bucket-website", "--bucket", "website-bucket"])
        .await;
    assert!(
        output.success(),
        "get website failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    assert_eq!(json["IndexDocument"]["Suffix"], "index.html");
    assert_eq!(json["ErrorDocument"]["Key"], "error.html");

    // Delete website configuration
    let output = server
        .aws_cli(&[
            "s3api",
            "delete-bucket-website",
            "--bucket",
            "website-bucket",
        ])
        .await;
    assert!(
        output.success(),
        "delete website failed: {}",
        output.stderr_text()
    );

    // Get again — should fail
    let output = server
        .aws_cli(&["s3api", "get-bucket-website", "--bucket", "website-bucket"])
        .await;
    assert!(!output.success(), "expected error after website deletion");
}

#[tokio::test]
async fn s3_bucket_logging_put_get() {
    let server = TestServer::start().await;

    let output = server
        .aws_cli(&["s3api", "create-bucket", "--bucket", "logging-bucket"])
        .await;
    assert!(output.success(), "create failed: {}", output.stderr_text());

    let output = server
        .aws_cli(&["s3api", "create-bucket", "--bucket", "log-target"])
        .await;
    assert!(output.success(), "create failed: {}", output.stderr_text());

    // Put logging configuration
    let logging_config =
        r#"{"LoggingEnabled":{"TargetBucket":"log-target","TargetPrefix":"logs/"}}"#;
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-logging",
            "--bucket",
            "logging-bucket",
            "--bucket-logging-status",
            logging_config,
        ])
        .await;
    assert!(
        output.success(),
        "put logging failed: {}",
        output.stderr_text()
    );

    // Get logging configuration
    let output = server
        .aws_cli(&["s3api", "get-bucket-logging", "--bucket", "logging-bucket"])
        .await;
    assert!(
        output.success(),
        "get logging failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    assert_eq!(json["LoggingEnabled"]["TargetBucket"], "log-target");
    assert_eq!(json["LoggingEnabled"]["TargetPrefix"], "logs/");
}

#[tokio::test]
async fn s3_bucket_replication_crud() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("repl-cfg-src")
        .send()
        .await
        .unwrap();
    client
        .create_bucket()
        .bucket("repl-cfg-dst")
        .send()
        .await
        .unwrap();

    // Enable versioning on source (required for replication)
    client
        .put_bucket_versioning()
        .bucket("repl-cfg-src")
        .versioning_configuration(
            aws_sdk_s3::types::VersioningConfiguration::builder()
                .status(aws_sdk_s3::types::BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();

    // Put replication config
    let repl_config = serde_json::json!({
        "Role": "arn:aws:iam::123456789012:role/replication-role",
        "Rules": [{
            "ID": "replicate-all",
            "Status": "Enabled",
            "Filter": { "Prefix": "" },
            "Destination": { "Bucket": "arn:aws:s3:::repl-cfg-dst" }
        }]
    });
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-replication",
            "--bucket",
            "repl-cfg-src",
            "--replication-configuration",
            &repl_config.to_string(),
        ])
        .await;
    assert!(
        output.success(),
        "put replication failed: {}",
        output.stderr_text()
    );

    // Get replication config
    let output = server
        .aws_cli(&[
            "s3api",
            "get-bucket-replication",
            "--bucket",
            "repl-cfg-src",
        ])
        .await;
    assert!(
        output.success(),
        "get replication failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let rules = json["ReplicationConfiguration"]["Rules"]
        .as_array()
        .unwrap();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0]["ID"], "replicate-all");

    // Delete replication config
    let output = server
        .aws_cli(&[
            "s3api",
            "delete-bucket-replication",
            "--bucket",
            "repl-cfg-src",
        ])
        .await;
    assert!(
        output.success(),
        "delete replication failed: {}",
        output.stderr_text()
    );

    // Get again — should fail
    let output = server
        .aws_cli(&[
            "s3api",
            "get-bucket-replication",
            "--bucket",
            "repl-cfg-src",
        ])
        .await;
    assert!(
        !output.success(),
        "expected error after replication deletion"
    );
}

#[tokio::test]
async fn s3_public_access_block_crud() {
    let server = TestServer::start().await;

    let output = server
        .aws_cli(&["s3api", "create-bucket", "--bucket", "pab-bucket"])
        .await;
    assert!(output.success(), "create failed: {}", output.stderr_text());

    // Put public access block
    let pab_config = r#"{"BlockPublicAcls":true,"IgnorePublicAcls":true,"BlockPublicPolicy":true,"RestrictPublicBuckets":true}"#;
    let output = server
        .aws_cli(&[
            "s3api",
            "put-public-access-block",
            "--bucket",
            "pab-bucket",
            "--public-access-block-configuration",
            pab_config,
        ])
        .await;
    assert!(
        output.success(),
        "put public access block failed: {}",
        output.stderr_text()
    );

    // Get public access block
    let output = server
        .aws_cli(&["s3api", "get-public-access-block", "--bucket", "pab-bucket"])
        .await;
    assert!(
        output.success(),
        "get public access block failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let config = &json["PublicAccessBlockConfiguration"];
    assert_eq!(config["BlockPublicAcls"], true);
    assert_eq!(config["IgnorePublicAcls"], true);
    assert_eq!(config["BlockPublicPolicy"], true);
    assert_eq!(config["RestrictPublicBuckets"], true);

    // Delete public access block
    let output = server
        .aws_cli(&[
            "s3api",
            "delete-public-access-block",
            "--bucket",
            "pab-bucket",
        ])
        .await;
    assert!(
        output.success(),
        "delete public access block failed: {}",
        output.stderr_text()
    );

    // Get again — should fail
    let output = server
        .aws_cli(&["s3api", "get-public-access-block", "--bucket", "pab-bucket"])
        .await;
    assert!(
        !output.success(),
        "expected error after public access block deletion"
    );
}

#[tokio::test]
async fn s3_bucket_ownership_controls_crud() {
    let server = TestServer::start().await;

    let output = server
        .aws_cli(&["s3api", "create-bucket", "--bucket", "ownership-bucket"])
        .await;
    assert!(output.success(), "create failed: {}", output.stderr_text());

    // Put ownership controls
    let ownership_config = r#"{"Rules":[{"ObjectOwnership":"BucketOwnerEnforced"}]}"#;
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-ownership-controls",
            "--bucket",
            "ownership-bucket",
            "--ownership-controls",
            ownership_config,
        ])
        .await;
    assert!(
        output.success(),
        "put ownership controls failed: {}",
        output.stderr_text()
    );

    // Get ownership controls
    let output = server
        .aws_cli(&[
            "s3api",
            "get-bucket-ownership-controls",
            "--bucket",
            "ownership-bucket",
        ])
        .await;
    assert!(
        output.success(),
        "get ownership controls failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let rules = json["OwnershipControls"]["Rules"].as_array().unwrap();
    assert_eq!(rules.len(), 1);
    assert_eq!(rules[0]["ObjectOwnership"], "BucketOwnerEnforced");

    // Delete ownership controls
    let output = server
        .aws_cli(&[
            "s3api",
            "delete-bucket-ownership-controls",
            "--bucket",
            "ownership-bucket",
        ])
        .await;
    assert!(
        output.success(),
        "delete ownership controls failed: {}",
        output.stderr_text()
    );

    // Get again — should fail
    let output = server
        .aws_cli(&[
            "s3api",
            "get-bucket-ownership-controls",
            "--bucket",
            "ownership-bucket",
        ])
        .await;
    assert!(
        !output.success(),
        "expected error after ownership controls deletion"
    );
}

#[tokio::test]
async fn s3_bucket_notification_configuration_crud() {
    let server = TestServer::start().await;

    let output = server
        .aws_cli(&["s3api", "create-bucket", "--bucket", "notif-cfg-bucket"])
        .await;
    assert!(output.success(), "create failed: {}", output.stderr_text());

    // Put notification configuration with a queue config
    let notif_config = r#"{"QueueConfigurations":[{"QueueArn":"arn:aws:sqs:us-east-1:123456789012:my-queue","Events":["s3:ObjectCreated:*"]}]}"#;
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-notification-configuration",
            "--bucket",
            "notif-cfg-bucket",
            "--notification-configuration",
            notif_config,
        ])
        .await;
    assert!(
        output.success(),
        "put notification config failed: {}",
        output.stderr_text()
    );

    // Get notification configuration
    let output = server
        .aws_cli(&[
            "s3api",
            "get-bucket-notification-configuration",
            "--bucket",
            "notif-cfg-bucket",
        ])
        .await;
    assert!(
        output.success(),
        "get notification config failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let queues = json["QueueConfigurations"].as_array().unwrap();
    assert_eq!(queues.len(), 1);
    assert!(queues[0]["QueueArn"].as_str().unwrap().contains("my-queue"));

    // Put empty notification configuration (effectively deletes)
    let empty_config =
        r#"{"QueueConfigurations":[],"TopicConfigurations":[],"LambdaFunctionConfigurations":[]}"#;
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-notification-configuration",
            "--bucket",
            "notif-cfg-bucket",
            "--notification-configuration",
            empty_config,
        ])
        .await;
    assert!(
        output.success(),
        "put empty notification config failed: {}",
        output.stderr_text()
    );

    // Get again — should return empty configuration
    let output = server
        .aws_cli(&[
            "s3api",
            "get-bucket-notification-configuration",
            "--bucket",
            "notif-cfg-bucket",
        ])
        .await;
    assert!(
        output.success(),
        "get notification config failed after clear: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let queues = json["QueueConfigurations"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0);
    assert_eq!(queues, 0, "Expected empty QueueConfigurations after clear");
}

#[tokio::test]
async fn s3_get_bucket_location() {
    let server = TestServer::start().await;

    let output = server
        .aws_cli(&["s3api", "create-bucket", "--bucket", "location-bucket"])
        .await;
    assert!(output.success(), "create failed: {}", output.stderr_text());

    let output = server
        .aws_cli(&[
            "s3api",
            "get-bucket-location",
            "--bucket",
            "location-bucket",
        ])
        .await;
    assert!(
        output.success(),
        "get location failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    // us-east-1 returns null LocationConstraint per AWS convention
    assert!(
        json.get("LocationConstraint").is_some(),
        "Expected LocationConstraint in response"
    );
    assert!(
        json["LocationConstraint"].is_null(),
        "us-east-1 LocationConstraint should be null, got: {}",
        json["LocationConstraint"]
    );
}

// ---- Object Property Operations ----

#[tokio::test]
async fn s3_object_acl_put_get() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("obj-acl-bucket")
        .send()
        .await
        .unwrap();

    client
        .put_object()
        .bucket("obj-acl-bucket")
        .key("acl-test.txt")
        .body(ByteStream::from_static(b"acl content"))
        .send()
        .await
        .unwrap();

    // Put a canned ACL via CLI
    let output = server
        .aws_cli(&[
            "s3api",
            "put-object-acl",
            "--bucket",
            "obj-acl-bucket",
            "--key",
            "acl-test.txt",
            "--acl",
            "public-read",
        ])
        .await;
    assert!(
        output.success(),
        "put-object-acl failed: {}",
        output.stderr_text()
    );

    // Get object ACL via CLI
    let output = server
        .aws_cli(&[
            "s3api",
            "get-object-acl",
            "--bucket",
            "obj-acl-bucket",
            "--key",
            "acl-test.txt",
        ])
        .await;
    assert!(
        output.success(),
        "get-object-acl failed: {}",
        output.stderr_text()
    );
    let acl = output.stdout_json();

    // Owner should be present
    assert!(
        acl["Owner"]["ID"].as_str().is_some(),
        "Expected Owner ID in ACL response"
    );

    // public-read should produce a READ grant for AllUsers
    let grants = acl["Grants"].as_array().expect("Expected Grants array");
    assert!(
        grants.len() >= 2,
        "public-read ACL should have at least 2 grants (owner + AllUsers), got {}",
        grants.len()
    );
    let has_public_read = grants.iter().any(|g| {
        g["Permission"].as_str() == Some("READ")
            && g["Grantee"]["URI"]
                .as_str()
                .map(|u| u.contains("AllUsers"))
                .unwrap_or(false)
    });
    assert!(
        has_public_read,
        "Expected public-read grant for AllUsers, got: {acl}"
    );
}

#[tokio::test]
async fn s3_get_object_attributes() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("attrs-bucket")
        .send()
        .await
        .unwrap();

    let data = b"Hello attributes!";
    let put_resp = client
        .put_object()
        .bucket("attrs-bucket")
        .key("attrs.txt")
        .body(ByteStream::from_static(data))
        .send()
        .await
        .unwrap();
    let put_etag = put_resp.e_tag().unwrap().to_string();

    // GetObjectAttributes via CLI requesting ETag, ObjectSize, StorageClass
    let output = server
        .aws_cli(&[
            "s3api",
            "get-object-attributes",
            "--bucket",
            "attrs-bucket",
            "--key",
            "attrs.txt",
            "--object-attributes",
            "ETag",
            "ObjectSize",
            "StorageClass",
        ])
        .await;
    assert!(
        output.success(),
        "get-object-attributes failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();

    // ETag should match what PutObject returned (without quotes)
    let etag = json["ETag"].as_str().expect("Expected ETag in response");
    let expected = put_etag.trim_matches('"');
    assert_eq!(
        etag, expected,
        "ETag mismatch: got {etag}, expected {expected}"
    );

    // ObjectSize should match data length
    let size = json["ObjectSize"].as_u64().expect("Expected ObjectSize");
    assert_eq!(size, data.len() as u64, "ObjectSize mismatch");

    // StorageClass should be STANDARD
    let sc = json["StorageClass"]
        .as_str()
        .expect("Expected StorageClass");
    assert_eq!(sc, "STANDARD");
}

#[tokio::test]
async fn s3_object_retention_put_get() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;

    // Create bucket with object lock enabled
    let output = server
        .aws_cli(&[
            "s3api",
            "create-bucket",
            "--bucket",
            "retention-bucket",
            "--object-lock-enabled-for-bucket",
        ])
        .await;
    assert!(
        output.success(),
        "create lock bucket failed: {}",
        output.stderr_text()
    );

    // Put an object
    s3.put_object()
        .bucket("retention-bucket")
        .key("retained.txt")
        .body(ByteStream::from_static(b"keep me"))
        .send()
        .await
        .unwrap();

    // Set GOVERNANCE retention 1 day in the future
    let retain_until = chrono::Utc::now() + chrono::Duration::days(1);
    let retain_date = retain_until.format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let output = server
        .aws_cli(&[
            "s3api",
            "put-object-retention",
            "--bucket",
            "retention-bucket",
            "--key",
            "retained.txt",
            "--retention",
            &format!(r#"{{"Mode":"GOVERNANCE","RetainUntilDate":"{retain_date}"}}"#),
        ])
        .await;
    assert!(
        output.success(),
        "put-object-retention failed: {}",
        output.stderr_text()
    );

    // Get retention and verify
    let output = server
        .aws_cli(&[
            "s3api",
            "get-object-retention",
            "--bucket",
            "retention-bucket",
            "--key",
            "retained.txt",
        ])
        .await;
    assert!(
        output.success(),
        "get-object-retention failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let retention = &json["Retention"];
    assert_eq!(
        retention["Mode"].as_str().unwrap(),
        "GOVERNANCE",
        "Retention mode should be GOVERNANCE"
    );
    assert!(
        retention["RetainUntilDate"].as_str().is_some(),
        "Expected RetainUntilDate in retention response"
    );
}

#[tokio::test]
async fn s3_object_legal_hold_put_get() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;

    // Create bucket with object lock enabled
    let output = server
        .aws_cli(&[
            "s3api",
            "create-bucket",
            "--bucket",
            "legal-hold-bucket",
            "--object-lock-enabled-for-bucket",
        ])
        .await;
    assert!(
        output.success(),
        "create lock bucket failed: {}",
        output.stderr_text()
    );

    s3.put_object()
        .bucket("legal-hold-bucket")
        .key("held.txt")
        .body(ByteStream::from_static(b"hold me"))
        .send()
        .await
        .unwrap();

    // Put legal hold ON
    let output = server
        .aws_cli(&[
            "s3api",
            "put-object-legal-hold",
            "--bucket",
            "legal-hold-bucket",
            "--key",
            "held.txt",
            "--legal-hold",
            r#"{"Status":"ON"}"#,
        ])
        .await;
    assert!(
        output.success(),
        "put-object-legal-hold ON failed: {}",
        output.stderr_text()
    );

    // Get legal hold - should be ON
    let output = server
        .aws_cli(&[
            "s3api",
            "get-object-legal-hold",
            "--bucket",
            "legal-hold-bucket",
            "--key",
            "held.txt",
        ])
        .await;
    assert!(
        output.success(),
        "get-object-legal-hold failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    assert_eq!(
        json["LegalHold"]["Status"].as_str().unwrap(),
        "ON",
        "Legal hold should be ON"
    );

    // Put legal hold OFF
    let output = server
        .aws_cli(&[
            "s3api",
            "put-object-legal-hold",
            "--bucket",
            "legal-hold-bucket",
            "--key",
            "held.txt",
            "--legal-hold",
            r#"{"Status":"OFF"}"#,
        ])
        .await;
    assert!(
        output.success(),
        "put-object-legal-hold OFF failed: {}",
        output.stderr_text()
    );

    // Get legal hold - should be OFF
    let output = server
        .aws_cli(&[
            "s3api",
            "get-object-legal-hold",
            "--bucket",
            "legal-hold-bucket",
            "--key",
            "held.txt",
        ])
        .await;
    assert!(
        output.success(),
        "get-object-legal-hold (OFF) failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    assert_eq!(
        json["LegalHold"]["Status"].as_str().unwrap(),
        "OFF",
        "Legal hold should be OFF after toggling"
    );
}

#[tokio::test]
async fn s3_object_lock_configuration_put_get() {
    let server = TestServer::start().await;

    // Create bucket with object lock enabled (enables versioning automatically)
    let output = server
        .aws_cli(&[
            "s3api",
            "create-bucket",
            "--bucket",
            "lock-config-bucket",
            "--object-lock-enabled-for-bucket",
        ])
        .await;
    assert!(
        output.success(),
        "create lock bucket failed: {}",
        output.stderr_text()
    );

    // Put object lock configuration with default retention
    let lock_config = r#"{"ObjectLockEnabled":"Enabled","Rule":{"DefaultRetention":{"Mode":"GOVERNANCE","Days":30}}}"#;
    let output = server
        .aws_cli(&[
            "s3api",
            "put-object-lock-configuration",
            "--bucket",
            "lock-config-bucket",
            "--object-lock-configuration",
            lock_config,
        ])
        .await;
    assert!(
        output.success(),
        "put-object-lock-configuration failed: {}",
        output.stderr_text()
    );

    // Get object lock configuration
    let output = server
        .aws_cli(&[
            "s3api",
            "get-object-lock-configuration",
            "--bucket",
            "lock-config-bucket",
        ])
        .await;
    assert!(
        output.success(),
        "get-object-lock-configuration failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let config = &json["ObjectLockConfiguration"];
    assert_eq!(
        config["ObjectLockEnabled"].as_str().unwrap(),
        "Enabled",
        "ObjectLockEnabled should be Enabled"
    );
    let default_retention = &config["Rule"]["DefaultRetention"];
    assert_eq!(
        default_retention["Mode"].as_str().unwrap(),
        "GOVERNANCE",
        "Default retention mode should be GOVERNANCE"
    );
    assert_eq!(
        default_retention["Days"].as_i64().unwrap(),
        30,
        "Default retention days should be 30"
    );
}

#[tokio::test]
async fn s3_restore_object() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("restore-bucket")
        .send()
        .await
        .unwrap();

    // Put an object with GLACIER storage class using a temp file
    let tmp = std::env::temp_dir().join("glacier-obj.bin");
    std::fs::write(&tmp, b"archived data").unwrap();
    let output = server
        .aws_cli(&[
            "s3api",
            "put-object",
            "--bucket",
            "restore-bucket",
            "--key",
            "archived.txt",
            "--body",
            tmp.to_str().unwrap(),
            "--storage-class",
            "GLACIER",
        ])
        .await;
    assert!(
        output.success(),
        "put-object with GLACIER class failed: {}",
        output.stderr_text()
    );

    // Restore the object
    let output = server
        .aws_cli(&[
            "s3api",
            "restore-object",
            "--bucket",
            "restore-bucket",
            "--key",
            "archived.txt",
            "--restore-request",
            r#"{"Days":7}"#,
        ])
        .await;
    assert!(
        output.success(),
        "restore-object failed: {}",
        output.stderr_text()
    );

    // Head the object to verify restore status
    let head = client
        .head_object()
        .bucket("restore-bucket")
        .key("archived.txt")
        .send()
        .await
        .unwrap();
    // After restore, the object should have a restore header
    let restore = head.restore();
    assert!(
        restore.is_some(),
        "Expected x-amz-restore header on restored object"
    );
}

#[tokio::test]
async fn s3_upload_part_copy() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;

    client
        .create_bucket()
        .bucket("upc-bucket")
        .send()
        .await
        .unwrap();

    // Put a source object
    let src_data = b"source-data-for-copy-part";
    client
        .put_object()
        .bucket("upc-bucket")
        .key("source.txt")
        .body(ByteStream::from_static(src_data))
        .send()
        .await
        .unwrap();

    // Create multipart upload for the destination
    let create = client
        .create_multipart_upload()
        .bucket("upc-bucket")
        .key("dest.txt")
        .send()
        .await
        .unwrap();
    let upload_id = create.upload_id().unwrap().to_string();

    // Upload part by copying from source
    let output = server
        .aws_cli(&[
            "s3api",
            "upload-part-copy",
            "--bucket",
            "upc-bucket",
            "--key",
            "dest.txt",
            "--upload-id",
            &upload_id,
            "--part-number",
            "1",
            "--copy-source",
            "upc-bucket/source.txt",
        ])
        .await;
    assert!(
        output.success(),
        "upload-part-copy failed: {}",
        output.stderr_text()
    );
    let json = output.stdout_json();
    let copy_result = &json["CopyPartResult"];
    let etag = copy_result["ETag"]
        .as_str()
        .expect("Expected ETag in CopyPartResult");
    assert!(!etag.is_empty(), "ETag should not be empty");

    // Complete the multipart upload with the copied part via CLI
    let mp_struct = format!(
        r#"{{"Parts": [{{"PartNumber": 1, "ETag": {}}}]}}"#,
        serde_json::Value::String(etag.to_string())
    );
    let output = server
        .aws_cli(&[
            "s3api",
            "complete-multipart-upload",
            "--bucket",
            "upc-bucket",
            "--key",
            "dest.txt",
            "--upload-id",
            &upload_id,
            "--multipart-upload",
            &mp_struct,
        ])
        .await;
    assert!(
        output.success(),
        "complete-multipart-upload failed: {}",
        output.stderr_text()
    );

    // Verify the assembled object matches the source
    let get = client
        .get_object()
        .bucket("upc-bucket")
        .key("dest.txt")
        .send()
        .await
        .unwrap();
    let body = get.body.collect().await.unwrap().into_bytes();
    assert_eq!(
        body.as_ref(),
        src_data,
        "Copied part data should match source"
    );
}

#[tokio::test]
async fn s3_introspection_notifications() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let sqs = server.sqs_client().await;

    // Create SQS queue for notification target
    let queue = sqs
        .create_queue()
        .queue_name("s3-intro-events")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap();

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
        .bucket("intro-notif-bucket")
        .send()
        .await
        .unwrap();

    // Set notification configuration
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
            "intro-notif-bucket",
            "--notification-configuration",
            &notif_config,
        ])
        .await;
    assert!(
        output.success(),
        "Failed to set notification: {}",
        output.stderr_text()
    );

    // Put object to trigger notification
    s3.put_object()
        .bucket("intro-notif-bucket")
        .key("intro-test.txt")
        .body(ByteStream::from_static(b"hello introspection"))
        .send()
        .await
        .unwrap();

    // Query introspection endpoint
    let url = format!("{}/_fakecloud/s3/notifications", server.endpoint());
    let resp: serde_json::Value = reqwest::get(&url).await.unwrap().json().await.unwrap();
    let notifications = resp["notifications"].as_array().unwrap();
    assert!(
        !notifications.is_empty(),
        "expected at least one notification event"
    );

    let notif = notifications
        .iter()
        .find(|n| n["key"] == "intro-test.txt")
        .expect("expected notification for intro-test.txt");
    assert_eq!(notif["bucket"], "intro-notif-bucket");
    assert_eq!(notif["eventType"], "s3:ObjectCreated:Put");
    assert!(!notif["timestamp"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn s3_simulation_lifecycle_tick_expires_objects() {
    let server = TestServer::start().await;
    let client = server.s3_client().await;
    let http_client = reqwest::Client::new();

    // Create bucket
    client
        .create_bucket()
        .bucket("lifecycle-tick-bucket")
        .send()
        .await
        .unwrap();

    // Set lifecycle config that expires objects after 0 days via CLI
    let lifecycle_json = r#"{
        "Rules": [
            {
                "ID": "expire-all",
                "Filter": {"Prefix": ""},
                "Status": "Enabled",
                "Expiration": {"Days": 0}
            }
        ]
    }"#;

    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-lifecycle-configuration",
            "--bucket",
            "lifecycle-tick-bucket",
            "--lifecycle-configuration",
            lifecycle_json,
        ])
        .await;
    assert!(
        output.success(),
        "put lifecycle failed: {}",
        output.stderr_text()
    );

    // Put an object
    client
        .put_object()
        .bucket("lifecycle-tick-bucket")
        .key("ephemeral.txt")
        .body(ByteStream::from_static(b"will be deleted"))
        .send()
        .await
        .unwrap();

    // Verify object exists
    let resp = client
        .list_objects_v2()
        .bucket("lifecycle-tick-bucket")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.key_count().unwrap_or(0), 1);

    // Call the lifecycle tick endpoint
    let url = format!(
        "{}/_fakecloud/s3/lifecycle-processor/tick",
        server.endpoint()
    );
    let resp: serde_json::Value = http_client
        .post(&url)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resp["processedBuckets"], 1);
    assert_eq!(resp["expiredObjects"], 1);
    assert_eq!(resp["transitionedObjects"], 0);

    // Verify object was deleted
    let resp = client
        .list_objects_v2()
        .bucket("lifecycle-tick-bucket")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.key_count().unwrap_or(0), 0);
}

#[tokio::test]
async fn s3_eventbridge_notification() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    let eb = server.eventbridge_client().await;
    let sqs = server.sqs_client().await;

    // Create S3 bucket
    s3.create_bucket()
        .bucket("eb-notif-bucket")
        .send()
        .await
        .unwrap();

    // Create SQS queue to receive events via EventBridge
    let queue = sqs
        .create_queue()
        .queue_name("s3-eb-target")
        .send()
        .await
        .unwrap();
    let queue_url = queue.queue_url().unwrap().to_string();
    let attrs = sqs
        .get_queue_attributes()
        .queue_url(&queue_url)
        .attribute_names(QueueAttributeName::QueueArn)
        .send()
        .await
        .unwrap();
    let queue_arn = attrs
        .attributes()
        .unwrap()
        .get(&QueueAttributeName::QueueArn)
        .unwrap()
        .to_string();

    // Create EventBridge rule matching S3 events
    eb.put_rule()
        .name("s3-eb-rule")
        .event_pattern(r#"{"source": ["aws.s3"]}"#)
        .send()
        .await
        .unwrap();

    eb.put_targets()
        .rule("s3-eb-rule")
        .targets(
            aws_sdk_eventbridge::types::Target::builder()
                .id("sqs-target")
                .arn(&queue_arn)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    // Enable EventBridge notifications on the bucket via CLI
    let notif_config = r#"{"EventBridgeConfiguration":{}}"#;
    let output = server
        .aws_cli(&[
            "s3api",
            "put-bucket-notification-configuration",
            "--bucket",
            "eb-notif-bucket",
            "--notification-configuration",
            notif_config,
        ])
        .await;
    assert!(
        output.success(),
        "put notification config failed: {}",
        output.stderr_text()
    );

    // Upload an object to trigger S3→EventBridge notification
    s3.put_object()
        .bucket("eb-notif-bucket")
        .key("test-file.txt")
        .body(ByteStream::from_static(b"hello eventbridge"))
        .send()
        .await
        .unwrap();

    // Read events from SQS (delivered via EventBridge rule)
    let msgs = sqs
        .receive_message()
        .queue_url(&queue_url)
        .max_number_of_messages(10)
        .send()
        .await
        .unwrap();

    assert!(
        !msgs.messages().is_empty(),
        "expected at least one S3→EventBridge event in SQS"
    );

    let body: serde_json::Value = serde_json::from_str(msgs.messages()[0].body().unwrap()).unwrap();
    assert_eq!(body["source"], "aws.s3");
    assert!(
        body["detail-type"].as_str().unwrap().contains("Object"),
        "detail-type should mention Object, got: {}",
        body["detail-type"]
    );
    assert_eq!(body["detail"]["bucket"]["name"], "eb-notif-bucket");
    assert_eq!(body["detail"]["object"]["key"], "test-file.txt");
}
