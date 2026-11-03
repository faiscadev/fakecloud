use std::time::Duration;

use aws_sdk_firehose::types::{
    BufferingHints, DeliveryStreamType, ExtendedS3DestinationConfiguration,
    KinesisStreamSourceConfiguration,
};
use aws_sdk_kinesis::primitives::Blob;
use aws_sdk_kinesis::types::PutRecordsRequestEntry;
use fakecloud_testkit::TestServer;

const ROLE: &str = "arn:aws:iam::123456789012:role/firehose";

async fn wait_objects(s3: &aws_sdk_s3::Client, bucket: &str) -> Vec<String> {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let listed = s3.list_objects_v2().bucket(bucket).send().await.unwrap();
            let keys: Vec<String> = listed
                .contents()
                .iter()
                .map(|object| object.key().unwrap().to_owned())
                .collect();
            if !keys.is_empty() {
                return keys;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("Kinesis records delivered to S3 within 15s")
}

#[tokio::test]
async fn kinesis_source_delivers_records_put_after_creation() {
    let server = TestServer::start().await;
    let kinesis = server.kinesis_client().await;
    let firehose = server.firehose_client().await;
    let s3 = server.s3_client().await;

    kinesis
        .create_stream()
        .stream_name("activity")
        .shard_count(1)
        .send()
        .await
        .unwrap();
    let stream_arn = kinesis
        .describe_stream_summary()
        .stream_name("activity")
        .send()
        .await
        .unwrap()
        .stream_description_summary()
        .unwrap()
        .stream_arn()
        .to_owned();
    kinesis
        .put_record()
        .stream_name("activity")
        .partition_key("pk")
        .data(Blob::new("before-creation\n"))
        .send()
        .await
        .unwrap();
    s3.create_bucket().bucket("sink").send().await.unwrap();
    // The source starts at LATEST as of creation, so let the earlier record age.
    tokio::time::sleep(Duration::from_millis(1100)).await;

    firehose
        .create_delivery_stream()
        .delivery_stream_name("activity-to-s3")
        .delivery_stream_type(DeliveryStreamType::KinesisStreamAsSource)
        .kinesis_stream_source_configuration(
            KinesisStreamSourceConfiguration::builder()
                .kinesis_stream_arn(&stream_arn)
                .role_arn(ROLE)
                .build()
                .unwrap(),
        )
        .extended_s3_destination_configuration(
            ExtendedS3DestinationConfiguration::builder()
                .role_arn(ROLE)
                .bucket_arn("arn:aws:s3:::sink")
                .buffering_hints(
                    BufferingHints::builder()
                        .size_in_mbs(1)
                        .interval_in_seconds(0)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .unwrap();

    let source = firehose
        .describe_delivery_stream()
        .delivery_stream_name("activity-to-s3")
        .send()
        .await
        .unwrap()
        .delivery_stream_description()
        .unwrap()
        .source()
        .unwrap()
        .kinesis_stream_source_description()
        .unwrap()
        .clone();
    assert_eq!(source.kinesis_stream_arn(), Some(stream_arn.as_str()));
    assert_eq!(source.role_arn(), Some(ROLE));

    let entry = |data: &str| {
        PutRecordsRequestEntry::builder()
            .partition_key("pk")
            .data(Blob::new(data))
            .build()
            .unwrap()
    };
    kinesis
        .put_records()
        .stream_name("activity")
        .records(entry("{\"n\":1}\n"))
        .records(entry("{\"n\":2}\n"))
        .send()
        .await
        .unwrap();

    let keys = wait_objects(&s3, "sink").await;
    assert_eq!(keys.len(), 1, "{keys:?}");
    let body = s3
        .get_object()
        .bucket("sink")
        .key(&keys[0])
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
    assert_eq!(&body[..], b"{\"n\":1}\n{\"n\":2}\n");
}
