use aws_sdk_firehose::primitives::Blob;
use aws_sdk_firehose::types::{BufferingHints, ExtendedS3DestinationConfiguration, Record};
use aws_sdk_s3::types::CreateBucketConfiguration;
use fakecloud_testkit::TestServer;

async fn setup(server: &TestServer, bucket: &str) -> aws_sdk_firehose::Client {
    let s3 = server.s3_client().await;
    let _ = s3
        .create_bucket()
        .bucket(bucket)
        .create_bucket_configuration(
            CreateBucketConfiguration::builder()
                .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::UsWest2)
                .build(),
        )
        .send()
        .await;
    server.firehose_client().await
}

fn ext_s3(bucket_arn: &str) -> ExtendedS3DestinationConfiguration {
    ExtendedS3DestinationConfiguration::builder()
        .role_arn("arn:aws:iam::123456789012:role/firehose")
        .bucket_arn(bucket_arn)
        .buffering_hints(
            BufferingHints::builder()
                .size_in_mbs(5)
                .interval_in_seconds(60)
                .build(),
        )
        .build()
        .unwrap()
}

#[tokio::test]
async fn firehose_create_describe_delete_roundtrip() {
    let server = TestServer::start().await;
    let firehose = setup(&server, "fh-bucket-create").await;
    let bucket_arn = "arn:aws:s3:::fh-bucket-create";

    let create = firehose
        .create_delivery_stream()
        .delivery_stream_name("fh-stream-1")
        .extended_s3_destination_configuration(ext_s3(bucket_arn))
        .send()
        .await
        .expect("create");
    assert!(create
        .delivery_stream_arn()
        .unwrap()
        .ends_with(":deliverystream/fh-stream-1"));

    let desc = firehose
        .describe_delivery_stream()
        .delivery_stream_name("fh-stream-1")
        .send()
        .await
        .expect("describe");
    let d = desc.delivery_stream_description().unwrap();
    assert_eq!(d.delivery_stream_name(), "fh-stream-1");
    assert_eq!(d.delivery_stream_status().as_str(), "ACTIVE");
    // AWS always reports the ExtendedS3 defaults the caller omitted: the
    // CloudWatch logging block (disabled), custom_time_zone=UTC, and
    // s3_backup_mode=Disabled.
    let ext = d.destinations()[0]
        .extended_s3_destination_description()
        .expect("extended s3 description");
    let cwl = ext
        .cloud_watch_logging_options()
        .expect("cloudwatch logging options");
    assert_eq!(cwl.enabled(), Some(false));
    assert_eq!(ext.custom_time_zone(), Some("UTC"));
    assert_eq!(
        ext.s3_backup_mode(),
        Some(&aws_sdk_firehose::types::S3BackupMode::Disabled)
    );

    let listed = firehose.list_delivery_streams().send().await.expect("list");
    assert!(listed
        .delivery_stream_names()
        .iter()
        .any(|n| n == "fh-stream-1"));

    firehose
        .delete_delivery_stream()
        .delivery_stream_name("fh-stream-1")
        .send()
        .await
        .expect("delete");

    let err = firehose
        .describe_delivery_stream()
        .delivery_stream_name("fh-stream-1")
        .send()
        .await;
    assert!(err.is_err());
}

#[tokio::test]
async fn firehose_http_endpoint_destination_round_trips() {
    use aws_sdk_firehose::types::{
        HttpEndpointConfiguration, HttpEndpointDestinationConfiguration,
        HttpEndpointDestinationUpdate,
    };

    let server = TestServer::start().await;
    let firehose = setup(&server, "fh-http-bucket").await;

    // A non-S3 (HTTP endpoint) destination must round-trip through
    // DescribeDeliveryStream instead of being dropped (bug-hunt 2026-06-24,
    // 1.13 — UpdateDestination/Create only handled S3/ExtendedS3).
    firehose
        .create_delivery_stream()
        .delivery_stream_name("fh-http")
        .http_endpoint_destination_configuration(
            HttpEndpointDestinationConfiguration::builder()
                .endpoint_configuration(
                    HttpEndpointConfiguration::builder()
                        .url("https://example.com/ingest")
                        .name("my-endpoint")
                        .build()
                        .unwrap(),
                )
                .role_arn("arn:aws:iam::123456789012:role/firehose")
                .build(),
        )
        .send()
        .await
        .expect("create http endpoint stream");

    let desc = firehose
        .describe_delivery_stream()
        .delivery_stream_name("fh-http")
        .send()
        .await
        .expect("describe");
    let d = desc.delivery_stream_description().unwrap();
    let http = d.destinations()[0]
        .http_endpoint_destination_description()
        .expect("http endpoint description present");
    assert_eq!(
        http.endpoint_configuration().and_then(|e| e.url()),
        Some("https://example.com/ingest")
    );

    // UpdateDestination must also persist a changed HTTP endpoint.
    let version = d.version_id();
    let dest_id = d.destinations()[0].destination_id();
    firehose
        .update_destination()
        .delivery_stream_name("fh-http")
        .current_delivery_stream_version_id(version)
        .destination_id(dest_id)
        .http_endpoint_destination_update(
            HttpEndpointDestinationUpdate::builder()
                .endpoint_configuration(
                    HttpEndpointConfiguration::builder()
                        .url("https://example.com/v2")
                        .build()
                        .unwrap(),
                )
                .build(),
        )
        .send()
        .await
        .expect("update http endpoint");

    let desc = firehose
        .describe_delivery_stream()
        .delivery_stream_name("fh-http")
        .send()
        .await
        .expect("describe after update");
    let http = desc.delivery_stream_description().unwrap().destinations()[0]
        .http_endpoint_destination_description()
        .expect("http endpoint description after update");
    assert_eq!(
        http.endpoint_configuration().and_then(|e| e.url()),
        Some("https://example.com/v2")
    );
}

#[tokio::test]
async fn firehose_put_record_writes_to_s3() {
    let server = TestServer::start().await;
    let firehose = setup(&server, "fh-bucket-put").await;
    let bucket_arn = "arn:aws:s3:::fh-bucket-put";

    firehose
        .create_delivery_stream()
        .delivery_stream_name("fh-put-stream")
        .extended_s3_destination_configuration(ext_s3(bucket_arn))
        .send()
        .await
        .expect("create");

    firehose
        .put_record()
        .delivery_stream_name("fh-put-stream")
        .record(
            Record::builder()
                .data(Blob::new(b"hello-firehose"))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("put record");

    let s3 = server.s3_client().await;
    let listing = s3
        .list_objects_v2()
        .bucket("fh-bucket-put")
        .send()
        .await
        .expect("list");
    let key = listing
        .contents()
        .first()
        .expect("at least one object")
        .key()
        .unwrap()
        .to_string();
    let obj = s3
        .get_object()
        .bucket("fh-bucket-put")
        .key(&key)
        .send()
        .await
        .expect("get");
    let bytes = obj.body.collect().await.expect("collect").into_bytes();
    assert!(bytes.starts_with(b"hello-firehose"));
}

#[tokio::test]
async fn firehose_put_record_batch_aggregates_payloads() {
    let server = TestServer::start().await;
    let firehose = setup(&server, "fh-bucket-batch").await;
    let bucket_arn = "arn:aws:s3:::fh-bucket-batch";

    firehose
        .create_delivery_stream()
        .delivery_stream_name("fh-batch-stream")
        .extended_s3_destination_configuration(ext_s3(bucket_arn))
        .send()
        .await
        .expect("create");

    let resp = firehose
        .put_record_batch()
        .delivery_stream_name("fh-batch-stream")
        .records(
            Record::builder()
                .data(Blob::new(b"line-a"))
                .build()
                .unwrap(),
        )
        .records(
            Record::builder()
                .data(Blob::new(b"line-b"))
                .build()
                .unwrap(),
        )
        .records(
            Record::builder()
                .data(Blob::new(b"line-c"))
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("put batch");
    assert_eq!(resp.failed_put_count(), 0);
    assert_eq!(resp.request_responses().len(), 3);

    let s3 = server.s3_client().await;
    let listing = s3
        .list_objects_v2()
        .bucket("fh-bucket-batch")
        .send()
        .await
        .expect("list");
    let key = listing
        .contents()
        .first()
        .unwrap()
        .key()
        .unwrap()
        .to_string();
    let obj = s3
        .get_object()
        .bucket("fh-bucket-batch")
        .key(&key)
        .send()
        .await
        .expect("get");
    let bytes = obj.body.collect().await.expect("collect").into_bytes();
    let s = std::str::from_utf8(&bytes).unwrap();
    assert!(s.contains("line-a"));
    assert!(s.contains("line-b"));
    assert!(s.contains("line-c"));
}

#[tokio::test]
async fn firehose_put_record_batch_reports_invalid_records() {
    let server = TestServer::start().await;
    let firehose = setup(&server, "fh-bucket-invalid").await;
    let bucket_arn = "arn:aws:s3:::fh-bucket-invalid";

    firehose
        .create_delivery_stream()
        .delivery_stream_name("fh-invalid-stream")
        .extended_s3_destination_configuration(ext_s3(bucket_arn))
        .send()
        .await
        .expect("create");

    // Mix valid + valid + valid records (SDK base64-encodes Blob for us, so
    // we can't construct invalid base64 via the SDK). Validate happy-path
    // counts here; invalid-record handling is exercised at the wire level
    // below by talking directly to the JSON endpoint.
    let resp = firehose
        .put_record_batch()
        .delivery_stream_name("fh-invalid-stream")
        .records(Record::builder().data(Blob::new(b"ok-1")).build().unwrap())
        .records(Record::builder().data(Blob::new(b"ok-2")).build().unwrap())
        .send()
        .await
        .expect("put batch");
    assert_eq!(resp.failed_put_count(), 0);

    // Wire-level invalid-base64 records should bump FailedPutCount and
    // surface an ErrorCode on the corresponding response entry.
    let http = reqwest::Client::new();
    let url = format!("{}/", server.endpoint());
    let body = serde_json::json!({
        "DeliveryStreamName": "fh-invalid-stream",
        "Records": [
            {"Data": "@@@not-base64@@@"},
            {"Data": "Z29vZA=="}
        ]
    });
    let r = http
        .post(&url)
        .header("Content-Type", "application/x-amz-json-1.1")
        .header("X-Amz-Target", "Firehose_20150804.PutRecordBatch")
        .header(
            "Authorization",
            "AWS4-HMAC-SHA256 \
             Credential=AKIAIOSFODNN7EXAMPLE/20260501/us-east-1/firehose/aws4_request, \
             SignedHeaders=host;x-amz-target, Signature=deadbeef",
        )
        .body(body.to_string())
        .send()
        .await
        .unwrap();
    assert_eq!(r.status(), 200);
    let json: serde_json::Value = r.json().await.unwrap();
    assert_eq!(json["FailedPutCount"], 1);
    let resps = json["RequestResponses"].as_array().unwrap();
    assert_eq!(resps.len(), 2);
    assert_eq!(resps[0]["ErrorCode"], "InvalidArgumentException");
    assert!(resps[1].get("RecordId").is_some());
}

#[tokio::test]
async fn firehose_streams_region_isolated() {
    use aws_config::BehaviorVersion;
    use aws_credential_types::Credentials;
    use aws_sdk_firehose::config::Region;

    let server = TestServer::start().await;
    let _ = setup(&server, "fh-bucket-region").await;

    fn config_for(server: &TestServer, region: &str) -> aws_sdk_firehose::Config {
        let creds = Credentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            None,
            None,
            "test",
        );
        aws_sdk_firehose::Config::builder()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url(server.endpoint())
            .region(Region::new(region.to_string()))
            .credentials_provider(creds)
            .build()
    }

    let east = aws_sdk_firehose::Client::from_conf(config_for(&server, "us-east-1"));
    let west = aws_sdk_firehose::Client::from_conf(config_for(&server, "us-west-2"));

    let bucket_arn = "arn:aws:s3:::fh-bucket-region";
    east.create_delivery_stream()
        .delivery_stream_name("regional-stream")
        .extended_s3_destination_configuration(ext_s3(bucket_arn))
        .send()
        .await
        .expect("create east");

    let east_desc = east
        .describe_delivery_stream()
        .delivery_stream_name("regional-stream")
        .send()
        .await
        .expect("describe east");
    assert_eq!(
        east_desc
            .delivery_stream_description()
            .unwrap()
            .delivery_stream_name(),
        "regional-stream"
    );

    let west_err = west
        .describe_delivery_stream()
        .delivery_stream_name("regional-stream")
        .send()
        .await;
    assert!(
        west_err.is_err(),
        "stream from us-east-1 must not be visible in us-west-2"
    );

    let west_listed = west
        .list_delivery_streams()
        .send()
        .await
        .expect("list west");
    assert!(west_listed.delivery_stream_names().is_empty());
}

#[tokio::test]
async fn firehose_tags_roundtrip() {
    let server = TestServer::start().await;
    let firehose = setup(&server, "fh-bucket-tags").await;
    let bucket_arn = "arn:aws:s3:::fh-bucket-tags";

    firehose
        .create_delivery_stream()
        .delivery_stream_name("fh-tags-stream")
        .extended_s3_destination_configuration(ext_s3(bucket_arn))
        .send()
        .await
        .expect("create");

    firehose
        .tag_delivery_stream()
        .delivery_stream_name("fh-tags-stream")
        .tags(
            aws_sdk_firehose::types::Tag::builder()
                .key("env")
                .value("prod")
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("tag");

    let listed = firehose
        .list_tags_for_delivery_stream()
        .delivery_stream_name("fh-tags-stream")
        .send()
        .await
        .expect("list tags");
    assert_eq!(listed.tags().len(), 1);
    assert_eq!(listed.tags()[0].key(), "env");

    firehose
        .untag_delivery_stream()
        .delivery_stream_name("fh-tags-stream")
        .tag_keys("env")
        .send()
        .await
        .expect("untag");
    let listed = firehose
        .list_tags_for_delivery_stream()
        .delivery_stream_name("fh-tags-stream")
        .send()
        .await
        .expect("list tags");
    assert_eq!(listed.tags().len(), 0);
}
