use std::{io::Read, time::Duration};

use aws_sdk_firehose::{
    primitives::Blob,
    types::{
        BufferingHints, CompressionFormat, DataFormatConversionConfiguration,
        DynamicPartitioningConfiguration, ExtendedS3DestinationConfiguration,
        ExtendedS3DestinationUpdate, ProcessingConfiguration, Record,
    },
};
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use base64::Engine;
use fakecloud_testkit::TestServer;

#[path = "../../fakecloud-firehose/tests/fixtures/xerial_decoder.rs"]
mod xerial;

fn hints(size: i32, interval: i32) -> BufferingHints {
    BufferingHints::builder()
        .size_in_mbs(size)
        .interval_in_seconds(interval)
        .build()
}

fn destination(bucket: &str, interval: i32) -> ExtendedS3DestinationConfiguration {
    destination_builder(bucket, interval).build().unwrap()
}

fn destination_builder(
    bucket: &str,
    interval: i32,
) -> aws_sdk_firehose::types::builders::ExtendedS3DestinationConfigurationBuilder {
    ExtendedS3DestinationConfiguration::builder()
        .role_arn("arn:aws:iam::123456789012:role/firehose")
        .bucket_arn(format!("arn:aws:s3:::{bucket}"))
        .buffering_hints(hints(1, interval))
}

fn record(data: impl Into<Vec<u8>>) -> Record {
    let data = data.into();
    assert!(data.len() <= 1_024_000, "fixture exceeds AWS record limit");
    Record::builder().data(Blob::new(data)).build().unwrap()
}

async fn create(
    fh: &aws_sdk_firehose::Client,
    stream: &str,
    config: ExtendedS3DestinationConfiguration,
) {
    fh.create_delivery_stream()
        .delivery_stream_name(stream)
        .extended_s3_destination_configuration(config)
        .send()
        .await
        .unwrap();
}

async fn put(fh: &aws_sdk_firehose::Client, stream: &str, data: impl Into<Vec<u8>>) {
    let ack = fh
        .put_record()
        .delivery_stream_name(stream)
        .record(record(data))
        .send()
        .await
        .unwrap();
    assert!(!ack.record_id().is_empty());
}

async fn keys(s3: &aws_sdk_s3::Client, bucket: &str) -> Vec<String> {
    s3.list_objects_v2()
        .bucket(bucket)
        .send()
        .await
        .unwrap()
        .contents()
        .iter()
        .map(|object| object.key().unwrap().to_owned())
        .collect()
}

// Bound the entire poll, including individual SDK requests. Exact timer and
// retry-attempt contracts belong to the deterministic engine unit tests.
async fn wait_keys(s3: &aws_sdk_s3::Client, bucket: &str, count: usize) -> Vec<String> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let keys = keys(s3, bucket).await;
            assert!(keys.len() <= count, "unexpected extra objects: {keys:?}");
            if keys.len() == count {
                return keys;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{bucket}: expected {count} objects within 15s"))
}

fn gunzip(raw: &[u8]) -> Vec<u8> {
    assert!(raw.starts_with(&[0x1f, 0x8b, 8]));
    let mut decoded = Vec::new();
    flate2::read::GzDecoder::new(raw)
        .read_to_end(&mut decoded)
        .unwrap();
    decoded
}

/// Decode every GZIP member of `raw` in order. There is no base64 layer: each
/// member must start right where the previous one's trailer ends.
fn gzip_members(mut raw: &[u8]) -> Vec<Vec<u8>> {
    let mut members = Vec::new();
    while !raw.is_empty() {
        assert!(
            raw.starts_with(&[0x1f, 0x8b, 8]),
            "expected a raw GZIP member, got {:?}",
            &raw[..raw.len().min(8)]
        );
        let mut decoder = flate2::bufread::GzDecoder::new(raw);
        let mut decoded = Vec::new();
        decoder.read_to_end(&mut decoded).unwrap();
        raw = decoder.into_inner();
        members.push(decoded);
    }
    members
}

async fn object(s3: &aws_sdk_s3::Client, bucket: &str, key: &str, codec: &str) -> Vec<u8> {
    let object = s3
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(object.content_type(), Some("application/octet-stream"));
    assert_eq!(
        object.content_encoding(),
        match codec {
            "GZIP" => Some("gzip"),
            "Snappy" => Some("snappy-java"),
            _ => None,
        }
    );
    let length = object.content_length().unwrap();
    let raw = object.body.collect().await.unwrap().into_bytes();
    assert_eq!(length, raw.len() as i64);
    match codec {
        "GZIP" => gunzip(&raw),
        "Snappy" => xerial::decode(&raw).unwrap().concat(),
        _ => raw.to_vec(),
    }
}

async fn update(fh: &aws_sdk_firehose::Client, stream: &str, config: ExtendedS3DestinationUpdate) {
    let response = fh
        .describe_delivery_stream()
        .delivery_stream_name(stream)
        .send()
        .await
        .unwrap();
    let description = response.delivery_stream_description().unwrap();
    fh.update_destination()
        .delivery_stream_name(stream)
        .current_delivery_stream_version_id(description.version_id())
        .destination_id(description.destinations()[0].destination_id())
        .extended_s3_destination_update(config)
        .send()
        .await
        .unwrap();
}

#[tokio::test]
async fn firehose_size_trigger_aggregates_separate_calls_decoded_before_compression() {
    // The independent decoder is checked against Java-produced fixtures.
    for (fixture, expected) in [
        (
            include_bytes!("../../fakecloud-firehose/tests/fixtures/xerial_binary.snappy")
                .as_slice(),
            xerial::binary_input(),
        ),
        (
            include_bytes!("../../fakecloud-firehose/tests/fixtures/xerial_multiblock.snappy")
                .as_slice(),
            xerial::multiblock_input(),
        ),
    ] {
        assert_eq!(xerial::decode(fixture).unwrap().concat(), expected);
    }
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    for (i, codec, extension) in [
        (0, CompressionFormat::Uncompressed, ""),
        (1, CompressionFormat::Gzip, ".gz"),
        (2, CompressionFormat::Snappy, ".snappy"),
    ] {
        let bucket = format!("fh-size-{i}");
        s3.create_bucket().bucket(&bucket).send().await.unwrap();
        create(
            &fh,
            &bucket,
            destination_builder(&bucket, 300)
                .compression_format(codec.clone())
                .build()
                .unwrap(),
        )
        .await;
        let first = vec![b'a'; 512 * 1024];
        let second = vec![b'b'; 512 * 1024];
        put(&fh, &bucket, first.clone()).await;
        assert!(keys(&s3, &bucket).await.is_empty());
        put(&fh, &bucket, second.clone()).await;
        let keys = wait_keys(&s3, &bucket, 1).await;
        assert!(keys[0].ends_with(extension));
        assert_eq!(
            object(&s3, &bucket, &keys[0], codec.as_str()).await,
            [first, second].concat()
        );
        if i != 0 {
            let head = s3
                .head_object()
                .bucket(&bucket)
                .key(&keys[0])
                .send()
                .await
                .unwrap();
            assert!(head.content_length().unwrap() < 512 * 1024);
        }
    }
}

#[tokio::test]
async fn firehose_size_trigger_keeps_whole_batch_overshoot() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    let bucket = "fh-overshoot";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    create(&fh, bucket, destination(bucket, 300)).await;
    put(&fh, bucket, vec![0; 256 * 1024]).await;
    assert!(keys(&s3, bucket).await.is_empty());
    let response = fh
        .put_record_batch()
        .delivery_stream_name(bucket)
        .records(record(vec![1; 400 * 1024]))
        .records(record(vec![2; 400 * 1024]))
        .records(record(vec![255; 400 * 1024]))
        .send()
        .await
        .unwrap();
    assert_eq!(response.failed_put_count(), 0);
    assert_eq!(response.request_responses().len(), 3);
    let keys = wait_keys(&s3, bucket, 1).await;
    assert_eq!(
        object(&s3, bucket, &keys[0], "UNCOMPRESSED").await,
        [
            vec![0; 256 * 1024],
            vec![1; 400 * 1024],
            vec![2; 400 * 1024],
            vec![255; 400 * 1024]
        ]
        .concat()
    );
}

#[tokio::test]
async fn firehose_interval_flushes_after_idle_and_starts_new_window() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    let bucket = "fh-idle";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    // Each "not yet" check below has about INTERVAL seconds of margin, while
    // delivery lands within INTERVAL + one 1 s worker tick, well inside wait_keys.
    const INTERVAL: i32 = 5;
    create(&fh, bucket, destination(bucket, INTERVAL)).await;
    let before = chrono::Utc::now();
    put(&fh, bucket, b"first\x00\xff".to_vec()).await;
    let admitted = chrono::Utc::now();
    assert!(keys(&s3, bucket).await.is_empty());
    let first = wait_keys(&s3, bucket, 1).await;
    let delivered = chrono::Utc::now();
    let hierarchy = &first[0][..14];
    assert!(
        hierarchy == before.format("%Y/%m/%d/%H/").to_string()
            || hierarchy == admitted.format("%Y/%m/%d/%H/").to_string()
    );
    let suffix = first[0]
        .strip_prefix(&format!("{hierarchy}{bucket}-1-"))
        .unwrap();
    let timestamp = chrono::NaiveDateTime::parse_from_str(&suffix[..19], "%Y-%m-%d-%H-%M-%S")
        .unwrap()
        .and_utc();
    assert!((before.timestamp()..=delivered.timestamp()).contains(&timestamp.timestamp()));
    // The interval runs from first arrival, so delivery is never earlier (1 s
    // of slack for the key's whole-second timestamp and wall-clock steps).
    assert!(
        timestamp.timestamp() >= before.timestamp() + i64::from(INTERVAL) - 1,
        "delivered at {timestamp}, before the {INTERVAL}s interval from {before}"
    );
    assert_eq!(
        object(&s3, bucket, &first[0], "UNCOMPRESSED").await,
        b"first\x00\xff"
    );
    put(&fh, bucket, b"second".to_vec()).await;
    assert_eq!(keys(&s3, bucket).await, first);
    let all = wait_keys(&s3, bucket, 2).await;
    let second = all.iter().find(|key| !first.contains(key)).unwrap();
    assert_eq!(object(&s3, bucket, second, "UNCOMPRESSED").await, b"second");
}

#[tokio::test]
async fn firehose_zero_interval_delivers_binary_batch_without_delimiters() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    let bucket = "fh-zero";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    create(&fh, bucket, destination(bucket, 0)).await;
    put(&fh, bucket, Vec::new()).await;
    assert!(keys(&s3, bucket).await.is_empty());
    let ack = fh
        .put_record_batch()
        .delivery_stream_name(bucket)
        .records(record(b"abc".to_vec()))
        .records(record(b"def\n".to_vec()))
        .records(record(vec![0, 255]))
        .send()
        .await
        .unwrap();
    assert_eq!(ack.failed_put_count(), 0);
    let keys = wait_keys(&s3, bucket, 1).await;
    assert_eq!(
        object(&s3, bucket, &keys[0], "UNCOMPRESSED").await,
        b"abcdef\n\x00\xff"
    );
}

#[tokio::test]
async fn firehose_api_buffered_update_uses_latest_configuration() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    for bucket in ["fh-before-update", "fh-after-update"] {
        s3.create_bucket().bucket(bucket).send().await.unwrap();
    }
    create(&fh, "updated", destination("fh-before-update", 300)).await;
    put(&fh, "updated", b"before".to_vec()).await;
    assert!(keys(&s3, "fh-before-update").await.is_empty());
    update(
        &fh,
        "updated",
        ExtendedS3DestinationUpdate::builder()
            .bucket_arn("arn:aws:s3:::fh-after-update")
            .prefix("events")
            .file_extension(".custom.log")
            .compression_format(CompressionFormat::Gzip)
            .buffering_hints(hints(1, 0))
            .build(),
    )
    .await;
    let delivered_keys = wait_keys(&s3, "fh-after-update", 1).await;
    assert!(
        delivered_keys[0].starts_with("events2"),
        "no slash inserted: {}",
        delivered_keys[0]
    );
    assert!(delivered_keys[0].contains("/updated-2-"));
    assert!(delivered_keys[0].ends_with(".custom.log"));
    assert_eq!(
        object(&s3, "fh-after-update", &delivered_keys[0], "GZIP").await,
        b"before"
    );
    assert!(keys(&s3, "fh-before-update").await.is_empty());
}

#[tokio::test]
async fn firehose_detached_batch_keeps_captured_config_until_late_bucket_exists() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    // Creating the stream requires its bucket; it goes away before any delivery.
    s3.create_bucket()
        .bucket("fh-missing-old")
        .send()
        .await
        .unwrap();
    create(
        &fh,
        "captured",
        destination_builder("fh-missing-old", 0)
            .prefix("old/")
            .compression_format(CompressionFormat::Gzip)
            .file_extension(".old")
            .build()
            .unwrap(),
    )
    .await;
    s3.delete_bucket()
        .bucket("fh-missing-old")
        .send()
        .await
        .unwrap();
    // Zero interval detaches at admission even though the bucket is missing.
    put(&fh, "captured", b"captured-old".to_vec()).await;
    s3.create_bucket()
        .bucket("fh-present-new")
        .send()
        .await
        .unwrap();
    update(
        &fh,
        "captured",
        ExtendedS3DestinationUpdate::builder()
            .bucket_arn("arn:aws:s3:::fh-present-new")
            .prefix("new/")
            .compression_format(CompressionFormat::Snappy)
            .file_extension(".new")
            .build(),
    )
    .await;
    put(&fh, "captured", b"latest-new".to_vec()).await;
    // A healthy stream admitted last proves the worker ran during the wait, so
    // the empty successor bucket below is not just an idle worker.
    s3.create_bucket()
        .bucket("fh-captured-control")
        .send()
        .await
        .unwrap();
    create(
        &fh,
        "captured-control",
        destination("fh-captured-control", 0),
    )
    .await;
    put(&fh, "captured-control", b"control".to_vec()).await;
    // Admission wakes the worker at once, so the old batch has almost certainly
    // failed against the missing bucket by now; exact retry timing is covered by
    // the engine unit tests.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    wait_keys(&s3, "fh-captured-control", 1).await;
    assert!(
        keys(&s3, "fh-present-new").await.is_empty(),
        "a stream's successor must wait behind its failed head"
    );
    s3.create_bucket()
        .bucket("fh-missing-old")
        .send()
        .await
        .unwrap();
    let old = wait_keys(&s3, "fh-missing-old", 1).await;
    let new = wait_keys(&s3, "fh-present-new", 1).await;
    assert!(old[0].starts_with("old/"));
    assert!(old[0].contains("/captured-1-"));
    assert!(old[0].ends_with(".old"));
    assert!(new[0].starts_with("new/"));
    assert!(new[0].contains("/captured-2-"));
    assert!(new[0].ends_with(".new"));
    assert_eq!(
        object(&s3, "fh-missing-old", &old[0], "GZIP").await,
        b"captured-old"
    );
    assert_eq!(
        object(&s3, "fh-present-new", &new[0], "Snappy").await,
        b"latest-new"
    );
}

// Proves the old instance's pending data never merges into the recreated
// stream. Discarding (rather than later delivering) the old buffer is covered by
// the engine's delete/recreate pruning unit test; its 300 s window outlasts this test.
#[tokio::test]
async fn firehose_delete_recreate_does_not_merge_old_pending_data() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    let bucket = "fh-recreated";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    create(&fh, "reused", destination(bucket, 300)).await;
    put(&fh, "reused", b"discard-me".to_vec()).await;
    assert!(keys(&s3, bucket).await.is_empty());
    fh.delete_delivery_stream()
        .delivery_stream_name("reused")
        .send()
        .await
        .unwrap();
    create(&fh, "reused", destination(bucket, 0)).await;
    put(&fh, "reused", b"new-instance".to_vec()).await;
    let keys = wait_keys(&s3, bucket, 1).await;
    assert_eq!(
        object(&s3, bucket, &keys[0], "UNCOMPRESSED").await,
        b"new-instance"
    );
}

#[tokio::test]
async fn firehose_unsupported_preflight_invalid_argument_before_ack_and_control_roundtrip() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    let bucket = "fh-unsupported";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    for (i, config) in [
        destination_builder(bucket, 0)
            .compression_format(CompressionFormat::Zip)
            .build()
            .unwrap(),
        destination_builder(bucket, 0)
            .compression_format(CompressionFormat::HadoopSnappy)
            .build()
            .unwrap(),
        destination_builder(bucket, 0)
            .prefix("!{partitionKeyFromQuery:id}/")
            .error_output_prefix("errors/")
            .build()
            .unwrap(),
        destination_builder(bucket, 0)
            .custom_time_zone("America/New_York")
            .build()
            .unwrap(),
        destination_builder(bucket, 0)
            .processing_configuration(ProcessingConfiguration::builder().enabled(true).build())
            .build()
            .unwrap(),
        destination_builder(bucket, 0)
            .dynamic_partitioning_configuration(
                DynamicPartitioningConfiguration::builder()
                    .enabled(true)
                    .build(),
            )
            .build()
            .unwrap(),
        destination_builder(bucket, 0)
            .data_format_conversion_configuration(
                DataFormatConversionConfiguration::builder()
                    .enabled(true)
                    .build(),
            )
            .build()
            .unwrap(),
    ]
    .into_iter()
    .enumerate()
    {
        let stream = format!("unsupported-{i}");
        let setting = [
            "CompressionFormat",
            "CompressionFormat",
            "Prefix",
            "CustomTimeZone",
            "ProcessingConfiguration",
            "DynamicPartitioningConfiguration",
            "DataFormatConversionConfiguration",
        ][i];
        create(&fh, &stream, config.clone()).await;
        let response = fh
            .describe_delivery_stream()
            .delivery_stream_name(&stream)
            .send()
            .await
            .unwrap();
        let description = response
            .delivery_stream_description()
            .unwrap()
            .destinations()[0]
            .extended_s3_destination_description()
            .unwrap();
        assert_eq!(
            description.compression_format(),
            config
                .compression_format()
                .unwrap_or(&CompressionFormat::Uncompressed)
        );
        assert_eq!(description.prefix(), config.prefix());
        assert_eq!(
            description.custom_time_zone(),
            config.custom_time_zone().or(Some("UTC"))
        );
        assert_eq!(
            description.processing_configuration(),
            config.processing_configuration()
        );
        assert_eq!(
            description.dynamic_partitioning_configuration(),
            config.dynamic_partitioning_configuration()
        );
        assert_eq!(
            description.data_format_conversion_configuration(),
            config.data_format_conversion_configuration()
        );
        let error = fh
            .put_record()
            .delivery_stream_name(&stream)
            .record(record(b"reject".to_vec()))
            .send()
            .await
            .unwrap_err();
        assert_eq!(error.raw_response().unwrap().status().as_u16(), 400);
        assert_eq!(
            error.as_service_error().unwrap().code(),
            Some("InvalidArgumentException")
        );
        assert!(error
            .as_service_error()
            .unwrap()
            .message()
            .unwrap()
            .contains(setting));
        let error = fh
            .put_record_batch()
            .delivery_stream_name(&stream)
            .records(record(b"reject-batch".to_vec()))
            .send()
            .await
            .unwrap_err();
        assert_eq!(error.raw_response().unwrap().status().as_u16(), 400);
        assert_eq!(
            error.as_service_error().unwrap().code(),
            Some("InvalidArgumentException")
        );
        assert!(error
            .as_service_error()
            .unwrap()
            .message()
            .unwrap()
            .contains(setting));
    }
    assert!(keys(&s3, bucket).await.is_empty());
}

#[tokio::test]
async fn firehose_unsupported_update_pauses_accepted_data_until_supported_again() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    let bucket = "fh-paused";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    create(&fh, bucket, destination(bucket, 300)).await;
    put(&fh, bucket, b"accepted".to_vec()).await;
    update(
        &fh,
        bucket,
        ExtendedS3DestinationUpdate::builder()
            .compression_format(CompressionFormat::Zip)
            .buffering_hints(hints(1, 0))
            .build(),
    )
    .await;
    let error = fh
        .put_record()
        .delivery_stream_name(bucket)
        .record(record(b"rejected".to_vec()))
        .send()
        .await
        .unwrap_err();
    assert_eq!(error.raw_response().unwrap().status().as_u16(), 400);
    // A healthy stream admitted after the pause proves the worker scanned
    // during the wait, so the empty bucket below is not just an idle worker.
    let control = "fh-paused-control";
    s3.create_bucket().bucket(control).send().await.unwrap();
    create(&fh, control, destination(control, 0)).await;
    put(&fh, control, b"control".to_vec()).await;
    // The paused data is already due (interval 0). Wait past at least two worker
    // ticks so a scan that ignored the pause would have written it by now.
    tokio::time::sleep(Duration::from_millis(2500)).await;
    wait_keys(&s3, control, 1).await;
    assert!(keys(&s3, bucket).await.is_empty());
    update(
        &fh,
        bucket,
        ExtendedS3DestinationUpdate::builder()
            .compression_format(CompressionFormat::Uncompressed)
            .build(),
    )
    .await;
    let keys = wait_keys(&s3, bucket, 1).await;
    // Delivered under the restored third version, not the original or paused one.
    assert!(
        keys[0].contains(&format!("/{bucket}-3-")),
        "unexpected version in {}",
        keys[0]
    );
    assert_eq!(
        object(&s3, bucket, &keys[0], "UNCOMPRESSED").await,
        b"accepted"
    );
}

#[tokio::test]
async fn firehose_cloudformation_provisioned_stream_delivers_compressed_custom_extension() {
    let server = TestServer::start().await;
    let s3 = server.s3_client().await;
    s3.create_bucket().bucket("fh-cfn").send().await.unwrap();
    let cfn = server.cloudformation_client().await;
    cfn.create_stack().stack_name("buffered-firehose").template_body(serde_json::json!({
        "Resources": {"Delivery": {"Type": "AWS::KinesisFirehose::DeliveryStream", "Properties": {
            "DeliveryStreamName": "cfn-buffered",
            "ExtendedS3DestinationConfiguration": {
                "RoleARN": "arn:aws:iam::123456789012:role/firehose", "BucketARN": "arn:aws:s3:::fh-cfn",
                "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 0},
                "CompressionFormat": "GZIP", "Prefix": "events/", "FileExtension": ".cfn.log"
            }
        }}}
    }).to_string()).send().await.unwrap();
    let fh = server.firehose_client().await;
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let response = cfn
                .describe_stacks()
                .stack_name("buffered-firehose")
                .send()
                .await
                .unwrap();
            let status = response.stacks()[0].stack_status().unwrap().as_str();
            if status == "CREATE_COMPLETE" {
                break;
            }
            assert_eq!(status, "CREATE_IN_PROGRESS");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("CFN completion");
    put(&fh, "cfn-buffered", b"cfn\x00\xff\n".to_vec()).await;
    let keys = wait_keys(&s3, "fh-cfn", 1).await;
    assert!(keys[0].starts_with("events/"));
    assert!(keys[0].ends_with(".cfn.log"));
    assert_eq!(
        object(&s3, "fh-cfn", &keys[0], "GZIP").await,
        b"cfn\x00\xff\n"
    );
}

#[tokio::test]
async fn firehose_logs_and_api_share_one_size_buffer() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    let bucket = "fh-shared";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    create(&fh, bucket, destination(bucket, 300)).await;
    let seed = vec![b'A'; 500 * 1024];
    fh.put_record_batch()
        .delivery_stream_name(bucket)
        .records(record(seed.clone()))
        .records(record(seed.clone()))
        .send()
        .await
        .unwrap();
    assert!(keys(&s3, bucket).await.is_empty());
    let logs = server.logs_client().await;
    logs.create_log_group()
        .log_group_name("buffer-source")
        .send()
        .await
        .unwrap();
    logs.create_log_stream()
        .log_group_name("buffer-source")
        .log_stream_name("events")
        .send()
        .await
        .unwrap();
    logs.put_subscription_filter()
        .log_group_name("buffer-source")
        .filter_name("to-firehose")
        .filter_pattern("")
        .destination_arn("arn:aws:firehose:us-east-1:123456789012:deliverystream/fh-shared")
        .send()
        .await
        .unwrap();
    let message = hex::encode(xerial::incompressible_input(70000));
    logs.put_log_events()
        .log_group_name("buffer-source")
        .log_stream_name("events")
        .log_events(
            aws_sdk_cloudwatchlogs::types::InputLogEvent::builder()
                .timestamp(chrono::Utc::now().timestamp_millis())
                .message(&message)
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();
    let keys = wait_keys(&s3, bucket, 1).await;
    let bytes = object(&s3, bucket, &keys[0], "UNCOMPRESSED").await;
    assert!(bytes.len() >= 1_048_576);
    assert_eq!(&bytes[..1_024_000], [seed.clone(), seed].concat());
    // The Logs record is the GZIP-compressed envelope itself, appended
    // byte-for-byte after the API records.
    let members = gzip_members(&bytes[1_024_000..]);
    assert_eq!(members.len(), 1, "one Logs delivery, one GZIP member");
    let event: serde_json::Value = serde_json::from_slice(&members[0]).unwrap();
    assert_eq!(event["messageType"], "DATA_MESSAGE");
    assert_eq!(event["logGroup"], "buffer-source");
    assert_eq!(event["logEvents"][0]["message"], message);
}

// CloudWatch Logs hands Firehose each subscription delivery as one record of
// GZIP-compressed bytes, and buffering concatenates records byte-for-byte. Two
// separate PutLogEvents deliveries buffered together therefore form one object
// of two GZIP members, which any multi-member reader (`zcat`, MultiGzDecoder)
// turns back into two DATA_MESSAGE envelopes.
#[tokio::test]
async fn firehose_buffers_separate_logs_deliveries_into_one_multi_member_gzip_object() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    let logs = server.logs_client().await;
    let bucket = "fh-logs-members";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    // The 300 s interval outlasts the test: only the 1 MiB threshold delivers.
    create(&fh, "logs-members", destination(bucket, 300)).await;
    logs.create_log_group()
        .log_group_name("member-source")
        .send()
        .await
        .unwrap();
    logs.create_log_stream()
        .log_group_name("member-source")
        .log_stream_name("events")
        .send()
        .await
        .unwrap();
    logs.put_subscription_filter()
        .log_group_name("member-source")
        .filter_name("to-firehose")
        .filter_pattern("")
        .destination_arn("arn:aws:firehose:us-east-1:123456789012:deliverystream/logs-members")
        .send()
        .await
        .unwrap();

    // Base64 text of incompressible bytes carries 6 bits per character, so a
    // call's GZIP member is at least 750,000 bytes and at most about its
    // 1,000,000 JSON bytes: one delivery stays below 1 MiB, two cross it. Each
    // 250,000-character event is within the 256 KiB event limit, and each call
    // (4 events plus 26 bytes of overhead each) within the 1 MiB batch limit.
    let messages: Vec<String> = xerial::incompressible_input(8 * 187_500)
        .chunks(187_500)
        .map(|chunk| base64::engine::general_purpose::STANDARD.encode(chunk))
        .collect();
    for (call, batch) in messages.chunks(4).enumerate() {
        let now = chrono::Utc::now().timestamp_millis();
        let mut request = logs
            .put_log_events()
            .log_group_name("member-source")
            .log_stream_name("events");
        for (i, message) in batch.iter().enumerate() {
            request = request.log_events(
                aws_sdk_cloudwatchlogs::types::InputLogEvent::builder()
                    .timestamp(now + i as i64)
                    .message(message)
                    .build()
                    .unwrap(),
            );
        }
        request.send().await.unwrap();
        if call == 0 {
            assert!(
                keys(&s3, bucket).await.is_empty(),
                "one delivery alone is below the size threshold"
            );
        }
    }

    let keys = wait_keys(&s3, bucket, 1).await;
    let bytes = object(&s3, bucket, &keys[0], "UNCOMPRESSED").await;
    assert!(bytes.len() >= 1_048_576);
    let members = gzip_members(&bytes);
    assert_eq!(
        members.len(),
        2,
        "one GZIP member per PutLogEvents delivery"
    );

    let mut text = Vec::new();
    flate2::read::MultiGzDecoder::new(&bytes[..])
        .read_to_end(&mut text)
        .unwrap();
    assert!(text == members.concat(), "multi-member reader output");
    let envelopes: Vec<serde_json::Value> = serde_json::Deserializer::from_slice(&text)
        .into_iter()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(envelopes.len(), 2);
    for (i, (envelope, batch)) in envelopes.iter().zip(messages.chunks(4)).enumerate() {
        assert_eq!(envelope["messageType"], "DATA_MESSAGE");
        assert_eq!(envelope["logGroup"], "member-source");
        assert_eq!(envelope["logStream"], "events");
        assert_eq!(
            envelope["subscriptionFilters"],
            serde_json::json!(["to-firehose"])
        );
        let events = envelope["logEvents"].as_array().unwrap();
        assert!(
            events
                .iter()
                .map(|event| event["message"].as_str().unwrap())
                .eq(batch.iter().map(String::as_str)),
            "envelope {i} must carry exactly its delivery's messages in order"
        );
    }
}

async fn assert_invalid_create(
    fh: &aws_sdk_firehose::Client,
    stream: &str,
    config: ExtendedS3DestinationConfiguration,
) {
    let error = fh
        .create_delivery_stream()
        .delivery_stream_name(stream)
        .extended_s3_destination_configuration(config)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error.as_service_error().unwrap().code(),
        Some("InvalidArgumentException"),
        "{stream}"
    );
    let missing = fh
        .describe_delivery_stream()
        .delivery_stream_name(stream)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        missing.as_service_error().unwrap().code(),
        Some("ResourceNotFoundException"),
        "{stream} must not be created"
    );
}

#[tokio::test]
async fn firehose_sdk_enforces_buffering_hint_and_destination_string_limits() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let bucket = "fh-limits";
    server
        .s3_client()
        .await
        .create_bucket()
        .bucket(bucket)
        .send()
        .await
        .unwrap();
    // Model ranges: SizeInMBs 1-128 and IntervalInSeconds 0-900, set together.
    for (stream, hints) in [
        ("size-zero", hints(0, 60)),
        ("size-129", hints(129, 60)),
        ("interval-negative", hints(1, -1)),
        ("interval-901", hints(1, 901)),
        (
            "size-only",
            BufferingHints::builder().size_in_mbs(1).build(),
        ),
        (
            "interval-only",
            BufferingHints::builder().interval_in_seconds(60).build(),
        ),
    ] {
        let config = destination_builder(bucket, 0)
            .buffering_hints(hints)
            .build()
            .unwrap();
        assert_invalid_create(&fh, stream, config).await;
    }
    for (stream, size, interval) in [
        ("interval-zero", 1, 0),
        ("interval-two", 1, 2),
        ("hints-max", 128, 900),
    ] {
        create(
            &fh,
            stream,
            destination_builder(bucket, interval)
                .buffering_hints(hints(size, interval))
                .build()
                .unwrap(),
        )
        .await;
        let response = fh
            .describe_delivery_stream()
            .delivery_stream_name(stream)
            .send()
            .await
            .unwrap();
        let description = response
            .delivery_stream_description()
            .unwrap()
            .destinations()[0]
            .extended_s3_destination_description()
            .unwrap()
            .buffering_hints()
            .unwrap();
        assert_eq!(description.size_in_mbs(), Some(size));
        assert_eq!(description.interval_in_seconds(), Some(interval));
    }

    // Prefix is limited to 1,024 characters on create and update alike.
    assert_invalid_create(
        &fh,
        "prefix-too-long",
        destination_builder(bucket, 0)
            .prefix("p".repeat(1025))
            .build()
            .unwrap(),
    )
    .await;
    let longest = "p".repeat(1024);
    create(
        &fh,
        "prefix-longest",
        destination_builder(bucket, 0)
            .prefix(&longest)
            .build()
            .unwrap(),
    )
    .await;
    let response = fh
        .describe_delivery_stream()
        .delivery_stream_name("prefix-longest")
        .send()
        .await
        .unwrap();
    let description = response.delivery_stream_description().unwrap();
    let error = fh
        .update_destination()
        .delivery_stream_name("prefix-longest")
        .current_delivery_stream_version_id(description.version_id())
        .destination_id(description.destinations()[0].destination_id())
        .extended_s3_destination_update(
            ExtendedS3DestinationUpdate::builder()
                .prefix("p".repeat(1025))
                .build(),
        )
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        error.as_service_error().unwrap().code(),
        Some("InvalidArgumentException")
    );
    let after = fh
        .describe_delivery_stream()
        .delivery_stream_name("prefix-longest")
        .send()
        .await
        .unwrap();
    let after = after.delivery_stream_description().unwrap();
    assert_eq!(after.version_id(), "1");
    assert_eq!(
        after.destinations()[0]
            .extended_s3_destination_description()
            .unwrap()
            .prefix(),
        Some(longest.as_str())
    );
}

#[tokio::test]
async fn firehose_service_reset_discards_pending_buffers() {
    let server = TestServer::start().await;
    let fh = server.firehose_client().await;
    let s3 = server.s3_client().await;
    let fc = fakecloud_sdk::FakeCloud::new(server.endpoint());
    let bucket = "fh-reset";
    s3.create_bucket().bucket(bucket).send().await.unwrap();
    // Long enough that a loaded CI runner still resets before the old buffer
    // could legitimately become due.
    const OLD_INTERVAL: u64 = 5;
    create(&fh, bucket, destination(bucket, OLD_INTERVAL as i32)).await;
    let admitted = std::time::Instant::now();
    put(&fh, bucket, b"before reset".to_vec()).await;
    // Resetting Firehose alone keeps the bucket, so the old buffer could still
    // be delivered if reset did not discard it.
    fc.reset_service("firehose").await.unwrap();
    create(&fh, bucket, destination(bucket, 0)).await;
    put(&fh, bucket, b"after reset".to_vec()).await;
    let delivered = wait_keys(&s3, bucket, 1).await;
    assert_eq!(
        object(&s3, bucket, &delivered[0], "UNCOMPRESSED").await,
        b"after reset"
    );
    // Wait until the discarded buffer would have been due, plus two worker
    // ticks, to show it never surfaces.
    let due = admitted + Duration::from_secs(OLD_INTERVAL + 2);
    tokio::time::sleep_until(due.into()).await;
    assert_eq!(keys(&s3, bucket).await, delivered);
}
