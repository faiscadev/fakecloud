//! Public Firehose admission contract, using real HTTP and the official AWS SDK.
//! Burst repayment/concurrency are covered by deterministic engine tests;
//! HTTP assertions use single-call costs or zero capacity, not clock races.

use std::future::Future;
use std::io::Read;
use std::time::Duration;

use aws_sdk_firehose::config::retry::RetryConfig;
use aws_sdk_firehose::error::ProvideErrorMetadata;
use aws_sdk_firehose::error::SdkError;
use aws_sdk_firehose::operation::put_record::PutRecordError;
use aws_sdk_firehose::operation::put_record_batch::PutRecordBatchError;
use aws_sdk_firehose::primitives::Blob;
use aws_sdk_firehose::types::{
    BufferingHints, CompressionFormat, DeliveryStreamType, ExtendedS3DestinationConfiguration,
    Record,
};
use fakecloud_sdk::types::{PutServiceQuotaRequest, QuotaEnforcement};
use fakecloud_sdk::FakeCloud;
use fakecloud_testkit::TestServer;

const REQUESTS: &str = "L-7681C790";
const RECORDS: &str = "L-17D4EADE";
const MIB: &str = "L-937C879B";
const REGION: &str = "us-west-2";

async fn bounded(test: impl Future<Output = ()>) {
    tokio::time::timeout(Duration::from_secs(60), test)
        .await
        .expect("Firehose throughput E2E exceeded 60s");
}

fn client(sdk: &aws_config::SdkConfig, attempts: u32) -> aws_sdk_firehose::Client {
    aws_sdk_firehose::Client::from_conf(
        aws_sdk_firehose::config::Builder::from(sdk)
            .retry_config(RetryConfig::standard().with_max_attempts(attempts))
            .build(),
    )
}

fn record(bytes: &[u8]) -> Record {
    Record::builder().data(Blob::new(bytes)).build().unwrap()
}

async fn setup(
    server: &TestServer,
    bucket: &str,
    gzip: bool,
) -> (aws_sdk_firehose::Client, aws_sdk_s3::Client) {
    let sdk = server.aws_config_in(REGION).await;
    let firehose = client(&sdk, 1);
    let s3 = aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&sdk)
            .force_path_style(true)
            .build(),
    );
    s3.create_bucket()
        .bucket(bucket)
        .create_bucket_configuration(
            aws_sdk_s3::types::CreateBucketConfiguration::builder()
                .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::UsWest2)
                .build(),
        )
        .send()
        .await
        .expect("create destination bucket");
    create_stream(&firehose, bucket, bucket, gzip).await;
    (firehose, s3)
}

async fn create_stream(
    firehose: &aws_sdk_firehose::Client,
    stream: &str,
    bucket: &str,
    gzip: bool,
) {
    firehose
        .create_delivery_stream()
        .delivery_stream_name(stream)
        .delivery_stream_type(DeliveryStreamType::DirectPut)
        .extended_s3_destination_configuration(
            ExtendedS3DestinationConfiguration::builder()
                .role_arn("arn:aws:iam::123456789012:role/firehose")
                .bucket_arn(format!("arn:aws:s3:::{bucket}"))
                .compression_format(if gzip {
                    CompressionFormat::Gzip
                } else {
                    CompressionFormat::Uncompressed
                })
                .buffering_hints(
                    BufferingHints::builder()
                        .size_in_mbs(5)
                        .interval_in_seconds(0)
                        .build(),
                )
                .build()
                .unwrap(),
        )
        .send()
        .await
        .expect("create DirectPut delivery stream");
}

async fn limit(fc: &FakeCloud, code: &str, value: f64) {
    let quota = fc
        .service_quotas()
        .put_quota(
            "firehose",
            code,
            &PutServiceQuotaRequest {
                region: Some(REGION.into()),
                value: Some(value),
                enforce: Some(QuotaEnforcement::Enforce),
                ..Default::default()
            },
        )
        .await
        .expect("set enforced quota through local SDK");
    assert_eq!(quota.applied_value, value);
    assert!(quota.enforced);
}

fn assert_single_unavailable(err: SdkError<PutRecordError>) {
    let SdkError::ServiceError(context) = err else {
        panic!("expected modeled PutRecord service error, got {err:?}");
    };
    assert_eq!(context.raw().status().as_u16(), 500);
    assert_eq!(context.err().message(), Some("Slow down."));
    assert!(
        matches!(
            context.err(),
            PutRecordError::ServiceUnavailableException(_)
        ),
        "expected modeled ServiceUnavailableException, got {:?}",
        context.err()
    );
}

fn assert_batch_unavailable(err: SdkError<PutRecordBatchError>) {
    let SdkError::ServiceError(context) = err else {
        panic!("expected modeled PutRecordBatch service error, got {err:?}");
    };
    assert_eq!(context.raw().status().as_u16(), 500);
    assert_eq!(context.err().message(), Some("Slow down."));
    assert!(
        matches!(
            context.err(),
            PutRecordBatchError::ServiceUnavailableException(_)
        ),
        "expected modeled ServiceUnavailableException, got {:?}",
        context.err()
    );
}

async fn put(firehose: &aws_sdk_firehose::Client, stream: &str, bytes: &[u8]) {
    let response = firehose
        .put_record()
        .delivery_stream_name(stream)
        .record(record(bytes))
        .send()
        .await
        .expect("PutRecord succeeds");
    assert!(!response.record_id().is_empty());
}

async fn batch(firehose: &aws_sdk_firehose::Client, stream: &str, records: Vec<Record>) {
    let count = records.len();
    let response = firehose
        .put_record_batch()
        .delivery_stream_name(stream)
        .set_records(Some(records))
        .send()
        .await
        .expect("whole batch succeeds");
    assert_eq!(response.failed_put_count(), 0);
    assert_eq!(response.request_responses().len(), count);
    assert!(response
        .request_responses()
        .iter()
        .all(|r| r.record_id().is_some_and(|id| !id.is_empty()) && r.error_code().is_none()));
}

async fn assert_no_objects(s3: &aws_sdk_s3::Client, bucket: &str) {
    // Observe across a worker tick, not just before an asynchronous write starts.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let listing = s3.list_objects_v2().bucket(bucket).send().await.unwrap();
        assert!(
            listing.contents().is_empty(),
            "rejection must not deliver any records"
        );
        if tokio::time::Instant::now() >= deadline {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn payloads(s3: &aws_sdk_s3::Client, bucket: &str, count: usize, gzip: bool) -> Vec<Vec<u8>> {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let listing = s3.list_objects_v2().bucket(bucket).send().await.unwrap();
            assert!(
                listing.contents().len() <= count,
                "unexpected extra delivery objects"
            );
            if listing.contents().len() == count {
                let mut payloads = Vec::new();
                for object in listing.contents() {
                    let object = s3
                        .get_object()
                        .bucket(bucket)
                        .key(object.key().unwrap())
                        .send()
                        .await
                        .expect("read delivered object");
                    let bytes = object.body.collect().await.unwrap().into_bytes();
                    let bytes = if gzip {
                        assert!(
                            bytes.len() < 512,
                            "fixture must actually compress on output"
                        );
                        let mut raw = Vec::new();
                        flate2::read::GzDecoder::new(bytes.as_ref())
                            .read_to_end(&mut raw)
                            .expect("decode delivered GZIP");
                        raw
                    } else {
                        bytes.to_vec()
                    };
                    payloads.push(bytes);
                }
                payloads.sort();
                return payloads;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("delivery objects within 10s")
}

#[tokio::test]
async fn zero_request_capacity_returns_http_500_for_both_apis_then_live_raise_recovers() {
    bounded(async {
        let server = TestServer::start().await;
        let bucket = "fh-throughput-requests";
        let (firehose, s3) = setup(&server, bucket, false).await;
        let fc = FakeCloud::new(server.endpoint());
        limit(&fc, REQUESTS, 0.0).await;
        assert_single_unavailable(
            firehose
                .put_record()
                .delivery_stream_name(bucket)
                .record(record(b"single"))
                .send()
                .await
                .expect_err("zero request capacity rejects PutRecord"),
        );
        assert_batch_unavailable(
            firehose
                .put_record_batch()
                .delivery_stream_name(bucket)
                .records(record(b"batch-a"))
                .records(record(b"batch-b"))
                .send()
                .await
                .expect_err("zero request capacity rejects whole batch"),
        );
        assert_no_objects(&s3, bucket).await;
        limit(&fc, REQUESTS, 100.0).await;
        put(&firehose, bucket, b"single").await;
        batch(
            &firehose,
            bucket,
            vec![record(b"batch-a"), record(b"batch-b")],
        )
        .await;
        assert_eq!(
            payloads(&s3, bucket, 2, false).await,
            vec![b"batch-abatch-b".to_vec(), b"single".to_vec()]
        );
    })
    .await;
}

#[tokio::test]
async fn record_capacity_partially_accepts_batch_and_only_failed_entries_are_resubmitted() {
    bounded(async {
        let server = TestServer::start().await;
        let bucket = "fh-throughput-records";
        let (firehose, s3) = setup(&server, bucket, false).await;
        let fc = FakeCloud::new(server.endpoint());
        limit(&fc, RECORDS, 1.0).await;
        let response = firehose
            .put_record_batch()
            .delivery_stream_name(bucket)
            .records(record(b"first"))
            .records(record(b"second"))
            .send()
            .await
            .expect("record exhaustion returns HTTP 200");
        assert_eq!(response.failed_put_count(), 1);
        let entries = response.request_responses();
        assert!(entries[0].record_id().is_some());
        assert!(entries[0].error_code().is_none());
        assert!(entries[1].record_id().is_none());
        assert_eq!(entries[1].error_code(), Some("ServiceUnavailableException"));
        assert_eq!(entries[1].error_message(), Some("Slow down."));
        assert_eq!(
            payloads(&s3, bucket, 1, false).await,
            vec![b"first".to_vec()]
        );
        // A successful batch response does not cause SDK retries of failed
        // entries. The producer explicitly resubmits only that entry.
        limit(&fc, RECORDS, 2.0).await;
        batch(&firehose, bucket, vec![record(b"second")]).await;
        assert_eq!(
            payloads(&s3, bucket, 2, false).await,
            vec![b"first".to_vec(), b"second".to_vec()]
        );
    })
    .await;
}

#[tokio::test]
async fn zero_record_or_byte_capacity_fails_single_calls_but_returns_all_failed_batches() {
    bounded(async {
        for (code, bucket) in [
            (RECORDS, "fh-throughput-zero-records"),
            (MIB, "fh-throughput-zero-bytes"),
        ] {
            let server = TestServer::start().await;
            let (firehose, s3) = setup(&server, bucket, false).await;
            limit(&FakeCloud::new(server.endpoint()), code, 0.0).await;
            assert_single_unavailable(
                firehose
                    .put_record()
                    .delivery_stream_name(bucket)
                    .record(record(b"single"))
                    .send()
                    .await
                    .unwrap_err(),
            );
            let response = firehose
                .put_record_batch()
                .delivery_stream_name(bucket)
                .records(record(b"first"))
                .records(record(b"second"))
                .send()
                .await
                .expect("record/byte limits must not reject a batch as a whole-call error");
            assert_eq!(response.failed_put_count(), 2);
            assert_eq!(response.request_responses().len(), 2);
            for entry in response.request_responses() {
                assert!(entry.record_id().is_none());
                assert_eq!(entry.error_code(), Some("ServiceUnavailableException"));
                assert_eq!(entry.error_message(), Some("Slow down."));
            }
            assert_no_objects(&s3, bucket).await;
        }
    })
    .await;
}

#[tokio::test]
async fn byte_capacity_counts_decoded_bytes_and_admits_one_boundary_crossing_record() {
    bounded(async {
        let server = TestServer::start().await;
        let bucket = "fh-throughput-bytes";
        let (firehose, s3) = setup(&server, bucket, true).await;
        let fc = FakeCloud::new(server.endpoint());
        let a = vec![b'a'; 4096];
        let b = vec![b'b'; 4096];
        // The first 4096-byte record crosses 3072 bytes of capacity. Its base64
        // encoding is larger and GZIP output is tiny; neither sets the budget.
        limit(&fc, MIB, 3072.0 / 1_048_576.0).await;
        let response = firehose
            .put_record_batch()
            .delivery_stream_name(bucket)
            .records(record(&a))
            .records(record(&b))
            .send()
            .await
            .expect("byte exhaustion is per-entry");
        assert_eq!(response.failed_put_count(), 1);
        assert!(response.request_responses()[0].record_id().is_some());
        assert_eq!(
            response.request_responses()[1].error_code(),
            Some("ServiceUnavailableException")
        );
        assert_eq!(payloads(&s3, bucket, 1, true).await, vec![a.clone()]);
        limit(&fc, MIB, 8192.0 / 1_048_576.0).await;
        batch(&firehose, bucket, vec![record(&b)]).await;
        assert_eq!(payloads(&s3, bucket, 2, true).await, vec![a, b]);
    })
    .await;
}

#[tokio::test]
async fn sustained_zero_capacity_exhausts_bounded_standard_sdk_retries_for_both_apis() {
    bounded(async {
        let server = TestServer::start().await;
        let bucket = "fh-throughput-retries";
        let (_, s3) = setup(&server, bucket, false).await;
        let fc = FakeCloud::new(server.endpoint());
        limit(&fc, REQUESTS, 0.0).await;
        let firehose = client(&server.aws_config_in(REGION).await, 3);
        tokio::time::timeout(Duration::from_secs(20), async {
            assert_single_unavailable(
                firehose
                    .put_record()
                    .delivery_stream_name(bucket)
                    .record(record(b"single"))
                    .send()
                    .await
                    .expect_err("sustained throttling outlasts SDK attempts"),
            );
            assert_batch_unavailable(
                firehose
                    .put_record_batch()
                    .delivery_stream_name(bucket)
                    .records(record(b"batch"))
                    .send()
                    .await
                    .expect_err("sustained throttling outlasts batch SDK attempts"),
            );
        })
        .await
        .expect("three-attempt SDK clients must finish within 20s");
        assert_no_objects(&s3, bucket).await;
    })
    .await;
}

#[tokio::test]
async fn configured_zero_values_are_not_enforced_by_default() {
    bounded(async {
        let server = TestServer::start().await;
        let bucket = "fh-throughput-disabled";
        let (firehose, s3) = setup(&server, bucket, false).await;
        let fc = FakeCloud::new(server.endpoint());
        for code in [REQUESTS, RECORDS, MIB] {
            let quota = fc
                .service_quotas()
                .put_quota(
                    "firehose",
                    code,
                    &PutServiceQuotaRequest {
                        region: Some(REGION.into()),
                        value: Some(0.0),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(quota.applied_value, 0.0);
            assert!(!quota.enforced);
        }
        put(&firehose, bucket, b"single").await;
        batch(&firehose, bucket, vec![record(b"batch")]).await;
        assert_eq!(
            payloads(&s3, bucket, 2, false).await,
            vec![b"batch".to_vec(), b"single".to_vec()]
        );
    })
    .await;
}

#[tokio::test]
async fn enabling_default_throughput_does_not_impose_the_reported_aws_limit() {
    bounded(async {
        let server = TestServer::start_full(&[], &["--enforce-quotas"]).await;
        let region = "us-east-2"; // Reported default is 1 MiB/s.
        let firehose = client(&server.aws_config_in(region).await, 1);
        let fc = FakeCloud::new(server.endpoint());
        let quotas = fc
            .service_quotas()
            .get_quotas(None, Some(region), Some("firehose"))
            .await
            .unwrap();
        for quota in quotas
            .quotas
            .iter()
            .filter(|q| [REQUESTS, RECORDS, MIB].contains(&q.quota_code.as_str()))
        {
            assert!(!quota.enforced);
            assert_eq!(quota.enforcement_source, "auto_scaling_default");
        }
        create_stream(
            &firehose,
            "default-throughput",
            "unused-default-bucket",
            false,
        )
        .await;
        // Several maximum-size batches exceed the AWS default without explicit
        // applied values. Limits alone must not introduce failed entries.
        let bytes = vec![b'x'; 800_000];
        for _ in 0..3 {
            batch(&firehose, "default-throughput", vec![record(&bytes); 5]).await;
        }
    })
    .await;
}

#[tokio::test]
async fn same_named_streams_in_two_enforced_accounts_each_get_their_own_allowance() {
    bounded(async {
        let server = TestServer::start().await;
        let bucket = "fh-throughput-two-accounts";
        let (firehose, s3) = setup(&server, bucket, false).await;
        let fc = FakeCloud::new(server.endpoint());
        limit(&fc, RECORDS, 1.0).await;
        let other = "210987654321";
        let (access, secret) = server.create_admin(other, "throughput-account").await;
        let sdk = server
            .aws_config_in(REGION)
            .await
            .into_builder()
            .credentials_provider(
                aws_credential_types::provider::SharedCredentialsProvider::new(
                    aws_credential_types::Credentials::new(access, secret, None, None, "test"),
                ),
            )
            .build();
        let other_firehose = client(&sdk, 1);
        let other_s3 = aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Builder::from(&sdk)
                .force_path_style(true)
                .build(),
        );
        let other_bucket = "fh-throughput-other-account";
        other_s3
            .create_bucket()
            .bucket(other_bucket)
            .create_bucket_configuration(
                aws_sdk_s3::types::CreateBucketConfiguration::builder()
                    .location_constraint(aws_sdk_s3::types::BucketLocationConstraint::UsWest2)
                    .build(),
            )
            .send()
            .await
            .unwrap();
        create_stream(&other_firehose, bucket, other_bucket, false).await;
        fc.service_quotas()
            .put_quota(
                "firehose",
                RECORDS,
                &PutServiceQuotaRequest {
                    account_id: Some(other.into()),
                    region: Some(REGION.into()),
                    value: Some(1.0),
                    enforce: Some(QuotaEnforcement::Enforce),
                },
            )
            .await
            .unwrap();
        for (producer, accepted, rejected) in [
            (
                &firehose,
                b"first-account".as_slice(),
                b"reject-first".as_slice(),
            ),
            (
                &other_firehose,
                b"second-account".as_slice(),
                b"reject-second".as_slice(),
            ),
        ] {
            let response = producer
                .put_record_batch()
                .delivery_stream_name(bucket)
                .records(record(accepted))
                .records(record(rejected))
                .send()
                .await
                .unwrap();
            assert_eq!(response.failed_put_count(), 1);
            assert!(response.request_responses()[0].record_id().is_some());
            assert_eq!(
                response.request_responses()[1].error_code(),
                Some("ServiceUnavailableException")
            );
        }
        assert_eq!(
            payloads(&s3, bucket, 1, false).await,
            vec![b"first-account".to_vec()]
        );
        assert_eq!(
            payloads(&other_s3, other_bucket, 1, false).await,
            vec![b"second-account".to_vec()]
        );
    })
    .await;
}

#[tokio::test]
async fn decoded_byte_budget_excludes_base64_and_compression() {
    bounded(async {
        let server = TestServer::start().await;
        let bucket = "fh-throughput-decoded";
        let (firehose, s3) = setup(&server, bucket, true).await;
        limit(
            &FakeCloud::new(server.endpoint()),
            MIB,
            8192.0 / 1_048_576.0,
        )
        .await;
        let a = vec![b'a'; 4096];
        let b = vec![b'b'; 2048];
        let c = vec![b'c'; 2048];
        let response = firehose
            .put_record_batch()
            .delivery_stream_name(bucket)
            .records(record(&a))
            .records(record(&b))
            .records(record(&c))
            .records(record(b"d"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.failed_put_count(), 1);
        assert!(response.request_responses()[..3]
            .iter()
            .all(|r| r.record_id().is_some()));
        assert_eq!(
            response.request_responses()[3].error_code(),
            Some("ServiceUnavailableException")
        );
        assert_eq!(
            payloads(&s3, bucket, 1, true).await,
            vec![[a, b, c].concat()]
        );
    })
    .await;
}

#[tokio::test]
async fn other_account_and_region_quota_values_do_not_reject_the_producer() {
    bounded(async {
        let server = TestServer::start().await;
        let bucket = "fh-throughput-scope";
        let (firehose, s3) = setup(&server, bucket, false).await;
        let fc = FakeCloud::new(server.endpoint());
        let account = fc
            .service_quotas()
            .get_quotas(None, Some(REGION), Some("firehose"))
            .await
            .unwrap()
            .account_id;
        let other = "210987654321";
        fc.service_quotas()
            .put_quota(
                "firehose",
                REQUESTS,
                &PutServiceQuotaRequest {
                    account_id: Some(other.into()),
                    region: Some(REGION.into()),
                    value: Some(0.0),
                    enforce: Some(QuotaEnforcement::Enforce),
                },
            )
            .await
            .unwrap();
        put(&firehose, bucket, b"other-account-isolated").await;

        let (access, secret) = server.create_admin(other, "firehose-producer").await;
        let sdk = server.aws_config_in(REGION).await;
        let other_sdk = sdk
            .into_builder()
            .credentials_provider(
                aws_credential_types::provider::SharedCredentialsProvider::new(
                    aws_credential_types::Credentials::new(access, secret, None, None, "test"),
                ),
            )
            .build();
        let other_firehose = client(&other_sdk, 1);
        create_stream(&other_firehose, bucket, bucket, false).await;
        assert_single_unavailable(
            other_firehose
                .put_record()
                .delivery_stream_name(bucket)
                .record(record(b"other-account-rejected"))
                .send()
                .await
                .unwrap_err(),
        );

        fc.service_quotas()
            .put_quota(
                "firehose",
                REQUESTS,
                &PutServiceQuotaRequest {
                    account_id: Some(account),
                    region: Some("us-east-2".into()),
                    value: Some(0.0),
                    enforce: Some(QuotaEnforcement::Enforce),
                },
            )
            .await
            .unwrap();
        put(&firehose, bucket, b"other-region-isolated").await;
        let regional = client(&server.aws_config_in("us-east-2").await, 1);
        create_stream(&regional, bucket, bucket, false).await;
        assert_batch_unavailable(
            regional
                .put_record_batch()
                .delivery_stream_name(bucket)
                .records(record(b"other-region-rejected"))
                .send()
                .await
                .unwrap_err(),
        );
        assert_eq!(
            payloads(&s3, bucket, 2, false).await,
            vec![
                b"other-account-isolated".to_vec(),
                b"other-region-isolated".to_vec()
            ]
        );
    })
    .await;
}

#[tokio::test]
async fn service_quotas_reports_regional_firehose_defaults_over_official_sdk() {
    bounded(async {
        let server = TestServer::start().await;
        for (region, values) in [
            ("us-west-2", [2000.0, 500_000.0, 5.0]),
            ("eu-west-1", [2000.0, 500_000.0, 5.0]),
            ("us-east-2", [1000.0, 100_000.0, 1.0]),
        ] {
            let sq = aws_sdk_servicequotas::Client::new(&server.aws_config_in(region).await);
            for (code, expected) in [REQUESTS, RECORDS, MIB].into_iter().zip(values) {
                let default = sq
                    .get_aws_default_service_quota()
                    .service_code("firehose")
                    .quota_code(code)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(
                    default.quota().unwrap().value(),
                    Some(expected),
                    "{region}/{code}"
                );
                let applied = sq
                    .get_service_quota()
                    .service_code("firehose")
                    .quota_code(code)
                    .send()
                    .await
                    .unwrap();
                assert_eq!(
                    applied.quota().unwrap().value(),
                    Some(expected),
                    "{region}/{code}"
                );
            }
        }
    })
    .await;
}
