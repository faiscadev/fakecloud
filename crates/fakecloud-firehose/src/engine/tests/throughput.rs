use super::*;
use crate::throughput::ThroughputLimits;
use fakecloud_core::quota::{
    QuotaProvider, FIREHOSE_MIB_PER_SECOND, FIREHOSE_PUT_REQUESTS_PER_SECOND,
    FIREHOSE_RECORDS_PER_SECOND, FIREHOSE_SERVICE_CODE,
};

struct LimitsProvider {
    limits: RwLock<ThroughputLimits>,
    engine: Weak<FirehoseEngine>,
    probe_locks: std::sync::atomic::AtomicBool,
}

impl QuotaProvider for LimitsProvider {
    fn applied_value(&self, _: &str, _: &str, _: &str, _: &str) -> Option<f64> {
        panic!("admission must use enforced_limit")
    }

    fn enforced_limit(&self, _: &str, _: &str, _: &str, _: &str) -> Option<f64> {
        panic!("admission must resolve all limits in one batched lookup")
    }

    fn enforced_limits(
        &self,
        account: &str,
        region: &str,
        service: &str,
        codes: &[&str],
    ) -> Vec<Option<f64>> {
        assert_eq!(service, FIREHOSE_SERVICE_CODE);
        if self.probe_locks.load(std::sync::atomic::Ordering::Relaxed) {
            let engine = self.engine.upgrade().unwrap();
            // Fail promptly rather than deadlocking if resolution happens under
            // either Firehose lock. Taking write also detects an existing read guard.
            let state = engine
                .core
                .state
                .try_write()
                .expect("provider under config lock");
            assert!(state.get(account).and_then(|s| s.streams(region)).is_some());
            let _registry = engine
                .core
                .registry
                .try_lock()
                .expect("provider under registry lock");
        }
        let limits = *self.limits.read();
        codes
            .iter()
            .map(|code| match *code {
                FIREHOSE_PUT_REQUESTS_PER_SECOND => limits.requests,
                FIREHOSE_RECORDS_PER_SECOND => limits.records,
                FIREHOSE_MIB_PER_SECOND => limits.mib,
                _ => panic!("unexpected quota {code}"),
            })
            .collect()
    }
}

fn enforce(
    f: &Fixture,
    requests: Option<f64>,
    records: Option<f64>,
    mib: Option<f64>,
) -> Arc<LimitsProvider> {
    let provider = Arc::new(LimitsProvider {
        limits: RwLock::new(ThroughputLimits {
            requests,
            records,
            mib,
        }),
        engine: Arc::downgrade(&f.engine),
        probe_locks: std::sync::atomic::AtomicBool::new(false),
    });
    f.engine.set_quota_provider(provider.clone());
    provider
}

fn put(f: &Fixture, records: Vec<Bytes>) -> Result<(), AwsServiceError> {
    let admitted = f.engine.enqueue_put(ACCOUNT, REGION, NAME, records)?;
    if admitted.iter().all(|admitted| *admitted) {
        Ok(())
    } else {
        Err(throughput_error())
    }
}

fn throttled(result: Result<(), AwsServiceError>) {
    let error = result.unwrap_err();
    assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(error.code(), "ServiceUnavailableException");
}

fn buffered(f: &Fixture) -> Vec<Bytes> {
    f.engine
        .core
        .registry
        .lock()
        .streams
        .values()
        .flat_map(|buffer| {
            buffer
                .queue
                .iter()
                .flat_map(|batch| batch.batch.records.iter())
                .chain(
                    buffer
                        .active
                        .iter()
                        .flat_map(|active| active.records.iter()),
                )
                .cloned()
        })
        .collect()
}

#[test]
fn provider_resolution_precedes_firehose_locks() {
    let f = Fixture::new(60);
    let provider = enforce(&f, Some(1.0), None, None);
    provider
        .probe_locks
        .store(true, std::sync::atomic::Ordering::Relaxed);
    put(&f, vec![record(b'a', 1)]).unwrap();
    throttled(put(&f, vec![record(b'x', 1)]));
}

#[tokio::test]
async fn single_and_batch_share_all_dimensions_with_ordered_admission() {
    for dimension in ["requests", "records", "bytes"] {
        let f = Fixture::new(60);
        enforce(
            &f,
            Some(3.0),
            Some(if dimension == "bytes" { 5.0 } else { 4.0 }),
            Some(10.0 / 1_048_576.0),
        );
        put(&f, vec![record(b'a', 4)]).unwrap();
        put(&f, vec![record(b'b', 2), record(b'c', 2)]).unwrap();
        let before = buffered(&f);
        let rejected = match dimension {
            "requests" => vec![], // Consume the third request, then exceed it below.
            "records" => vec![record(b'x', 1), record(b'y', 1)],
            _ => vec![record(b'x', 3), record(b'y', 2)],
        };
        if dimension == "requests" {
            put(&f, rejected).unwrap();
            throttled(put(&f, vec![record(b'x', 1)]));
            assert_eq!(buffered(&f), before);
        } else {
            assert_eq!(
                f.engine
                    .enqueue_put(ACCOUNT, REGION, NAME, rejected)
                    .unwrap(),
                vec![true, false]
            );
            // The request was charged once, even though its second entry failed.
            throttled(put(&f, vec![record(b'd', 1)]));
        }
        f.advance(60);
        f.turn().await;
        let records = f.batches()[0].records.clone();
        let mut expected = vec![record(b'a', 4), record(b'b', 2), record(b'c', 2)];
        if dimension != "requests" {
            expected.push(record(b'x', if dimension == "records" { 1 } else { 3 }));
        }
        assert_eq!(records, expected);
    }
}

#[test]
fn budgets_are_stream_account_region_isolated_and_concurrent() {
    let f = Fixture::new(60);
    f.add_stream(ACCOUNT, REGION, "other", 60);
    f.add_stream("222222222222", REGION, NAME, 60);
    f.add_stream(ACCOUNT, "us-west-2", NAME, 60);
    enforce(&f, Some(7.0), Some(7.0), None);
    let barrier = std::sync::Barrier::new(40);
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..40)
            .map(|_| {
                scope.spawn(|| {
                    barrier.wait();
                    put(&f, vec![record(b'a', 1)]).is_ok()
                })
            })
            .collect();
        assert_eq!(
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .filter(|ok| *ok)
                .count(),
            7
        );
    });
    for (account, region, name) in [
        (ACCOUNT, REGION, "other"),
        ("222222222222", REGION, NAME),
        (ACCOUNT, "us-west-2", NAME),
    ] {
        assert_eq!(
            f.engine
                .enqueue_put(account, region, name, vec![record(b'a', 1); 7])
                .unwrap(),
            vec![true; 7]
        );
        assert_eq!(
            f.engine
                .enqueue_put(account, region, name, vec![record(b'x', 1)])
                .unwrap(),
            vec![false]
        );
    }
}

#[test]
fn empty_records_and_dynamic_limits_preserve_all_usage() {
    let f = Fixture::new(60);
    let provider = enforce(&f, Some(10.0), None, Some(0.0));
    put(&f, vec![Bytes::new(), Bytes::new()]).unwrap();
    provider.limits.write().records = Some(2.0);
    throttled(put(&f, vec![Bytes::new()]));
    provider.limits.write().records = Some(3.0);
    put(&f, vec![Bytes::new()]).unwrap();
    throttled(put(&f, vec![Bytes::new()]));
    f.advance(1);
    put(&f, vec![Bytes::new(); 3]).unwrap();
    throttled(put(&f, vec![record(b'a', 1)]));
}

#[test]
fn validation_precedes_debit_and_config_only_and_missing_backend_are_metered() {
    let f = Fixture::new(60);
    enforce(&f, Some(1.0), None, None);
    let svc = crate::FirehoseService::default().with_engine(f.engine.clone());
    assert_eq!(
        svc.deliver_records(ACCOUNT, REGION, "missing", vec![vec![1]])
            .unwrap_err()
            .code(),
        "ResourceNotFoundException"
    );
    f.update(NAME, |s| s.stream_type = "KinesisStreamAsSource".into());
    assert_eq!(
        put(&f, vec![record(b'a', 1)]).unwrap_err().code(),
        "InvalidArgumentException"
    );
    f.update(NAME, |s| {
        s.stream_type = "DirectPut".into();
        s.destination.as_mut().unwrap().compression_format = Some("unsupported".into());
    });
    assert_eq!(
        put(&f, vec![record(b'a', 1)]).unwrap_err().code(),
        "InvalidArgumentException"
    );
    f.update(NAME, |s| s.destination = None);
    put(&f, vec![record(b'a', 1)]).unwrap();
    throttled(put(&f, vec![record(b'a', 1)]));
    f.advance(1);
    f.update(NAME, |s| s.destination = Some(destination(60)));
    *f.engine.core.writer.write() = None;
    put(&f, vec![record(b'a', 1)]).unwrap();
    throttled(put(&f, vec![record(b'a', 1)]));
}

#[tokio::test]
async fn zero_public_capacity_does_not_drop_internal_enqueue_and_disabled_bypasses() {
    let f = Fixture::new(60);
    let provider = enforce(&f, Some(0.0), Some(0.0), Some(0.0));
    throttled(put(&f, vec![record(b'x', 1)]));
    f.enqueue(vec![record(b'i', 1)]);
    *provider.limits.write() = ThroughputLimits::default();
    for _ in 0..20 {
        put(&f, vec![record(b'p', 1)]).unwrap();
    }
    provider.limits.write().requests = Some(1.0);
    put(&f, vec![record(b'z', 1)]).unwrap();
    throttled(put(&f, vec![record(b'x', 1)]));
    assert_eq!(f.engine.shutdown().await.undelivered_records, 0);
    let records = &f.batches()[0].records;
    assert_eq!(records.len(), 22);
    assert_eq!(records[0], record(b'i', 1));
    assert_eq!(records[21], record(b'z', 1));
}

#[tokio::test]
async fn retries_do_not_redebit_ingestion() {
    let f = Fixture::new(0);
    enforce(&f, Some(1.0), Some(1.0), Some(1.0 / 1_048_576.0));
    let mut entered = f.controlled();
    put(&f, vec![record(b'a', 1)]).unwrap();
    let mut jobs = Writes::default();
    f.schedule(&mut jobs, false);
    let first = next_attempt(&mut entered).await;
    first.reply.send(Action::Fail).unwrap();
    f.finish(&mut jobs).await;
    throttled(put(&f, vec![record(b'x', 1)]));
    f.advance(5);
    f.schedule(&mut jobs, true);
    let retry = next_attempt(&mut entered).await;
    assert_eq!(first.batch.object_key, retry.batch.object_key);
    retry.reply.send(Action::Commit).unwrap();
    f.finish(&mut jobs).await;
    put(&f, vec![record(b'b', 1)]).unwrap();
    throttled(put(&f, vec![record(b'x', 1)]));
}

#[tokio::test]
async fn delete_reset_restore_start_new_generation_budgets() {
    for operation in ["delete", "reset", "restore"] {
        let f = Fixture::new(60);
        enforce(&f, Some(1.0), None, None);
        put(&f, vec![record(b'a', 1)]).unwrap();
        throttled(put(&f, vec![record(b'x', 1)]));
        match operation {
            "delete" => {
                f.engine
                    .core
                    .state
                    .write()
                    .get_or_create(ACCOUNT, REGION)
                    .streams_mut(REGION)
                    .remove(NAME);
            }
            "reset" => f.engine.core.state.write().accounts.clear(),
            _ => {
                let serialized = serde_json::to_vec(&*f.engine.core.state.read()).unwrap();
                *f.engine.core.state.write() = serde_json::from_slice(&serialized).unwrap();
            }
        }
        if operation != "restore" {
            f.add_stream(ACCOUNT, REGION, NAME, 60);
        }
        put(&f, vec![record(b'b', 1)]).unwrap();
        throttled(put(&f, vec![record(b'x', 1)]));
        f.turn().await;
        assert_eq!(f.engine.shutdown().await.undelivered_records, 0);
        assert_eq!(f.batches()[0].records, vec![record(b'b', 1)]);
    }
}

fn request(action: &str, body: serde_json::Value) -> fakecloud_core::service::AwsRequest {
    fakecloud_core::service::AwsRequest {
        service: "firehose".into(),
        action: action.into(),
        region: REGION.into(),
        account_id: ACCOUNT.into(),
        request_id: "throughput-test".into(),
        headers: http::HeaderMap::new(),
        query_params: HashMap::new(),
        body: Bytes::from(serde_json::to_vec(&body).unwrap()),
        body_stream: Mutex::new(None),
        path_segments: Vec::new(),
        raw_path: "/".into(),
        raw_query: String::new(),
        method: http::Method::POST,
        is_query_protocol: false,
        access_key_id: None,
        principal: None,
    }
}

#[tokio::test]
async fn public_batch_burst_is_ordered_and_debt_survives_window_rollover() {
    use fakecloud_core::service::AwsService;
    let f = Fixture::new(60);
    enforce(&f, None, None, Some(2.0 / 1_048_576.0));
    let svc = crate::FirehoseService::default().with_engine(f.engine.clone());
    let send = || {
        request(
            "PutRecordBatch",
            json!({"DeliveryStreamName": NAME,
        "Records": [{"Data": "YWJjZGU="}, {"Data": "Zg=="}, {"Data": ""}]}),
        )
    };
    let response = svc.handle(send()).await.unwrap();
    assert_eq!(response.status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_slice(response.body.expect_bytes()).unwrap();
    assert_eq!(body["FailedPutCount"], 1);
    assert!(body["RequestResponses"][0]["RecordId"].is_string());
    assert_eq!(
        body["RequestResponses"][1],
        json!({"ErrorCode": "ServiceUnavailableException", "ErrorMessage": "Slow down."})
    );
    assert!(body["RequestResponses"][2]["RecordId"].is_string());
    assert_eq!(
        buffered(&f),
        vec![Bytes::from_static(b"abcde"), Bytes::new()]
    );

    // Five bytes against two bytes/s leave three bytes carried at t+1.
    // At t+2 only one byte remains charged, leaving room for the retry.
    {
        f.advance(1);
        let response = svc.handle(send()).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(response.body.expect_bytes()).unwrap();
        assert_eq!(body["FailedPutCount"], 2);
    }
    f.advance(1);
    let response = svc
        .handle(request(
            "PutRecordBatch",
            json!({"DeliveryStreamName": NAME,
        "Records": [{"Data": "Zg=="}]}),
        ))
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(response.body.expect_bytes()).unwrap();
    assert_eq!(body["FailedPutCount"], 0);
    assert_eq!(buffered(&f).last().unwrap(), &Bytes::from_static(b"f"));
}

#[tokio::test]
async fn public_record_data_validation_is_consistent_and_never_consumes_capacity() {
    use fakecloud_core::service::AwsService;

    for (invalid, expected_code) in [
        (json!({}), "InvalidArgumentException"),
        (json!({"Data": null}), "InvalidArgumentException"),
        (json!({"Data": 1}), "SerializationException"),
        (json!({"Data": false}), "SerializationException"),
        (json!({"Data": []}), "SerializationException"),
        (json!({"Data": {}}), "SerializationException"),
        (json!({"Data": "%%%"}), "SerializationException"),
    ] {
        for action in ["PutRecord", "PutRecordBatch"] {
            let f = Fixture::new(60);
            enforce(&f, Some(1.0), Some(1.0), Some(1.0 / 1_048_576.0));
            let svc = crate::FirehoseService::default().with_engine(f.engine.clone());
            let body = if action == "PutRecord" {
                json!({"DeliveryStreamName": NAME, "Record": invalid})
            } else {
                // A valid leading entry must not be admitted before validation finishes.
                json!({"DeliveryStreamName": NAME, "Records": [{"Data": "YQ=="}, invalid]})
            };
            let error = svc.handle(request(action, body)).await.err().unwrap();
            assert_eq!(error.status(), StatusCode::BAD_REQUEST);
            assert_eq!(error.code(), expected_code, "{action}: {invalid}");
            assert!(buffered(&f).is_empty(), "{action}: {invalid}");

            let valid = if action == "PutRecord" {
                json!({"DeliveryStreamName": NAME, "Record": {"Data": "Yg=="}})
            } else {
                json!({"DeliveryStreamName": NAME, "Records": [{"Data": "Yg=="}]})
            };
            let response = svc.handle(request(action, valid.clone())).await.unwrap();
            assert_eq!(response.status, StatusCode::OK);
            let body: serde_json::Value =
                serde_json::from_slice(response.body.expect_bytes()).unwrap();
            if action == "PutRecord" {
                assert!(body["RecordId"].is_string());
            } else {
                assert_eq!(body["FailedPutCount"], 0);
                assert!(body["RequestResponses"][0]["RecordId"].is_string());
            }
            assert_eq!(buffered(&f), vec![Bytes::from_static(b"b")]);

            // Successful validation still debits the shared request budget.
            let error = svc.handle(request(action, valid)).await.err().unwrap();
            assert_eq!(error.code(), "ServiceUnavailableException");
            assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
            assert_eq!(buffered(&f), vec![Bytes::from_static(b"b")]);
        }
    }
}

#[tokio::test]
async fn malformed_base64_rejects_whole_call_without_consuming_capacity() {
    use fakecloud_core::service::AwsService;
    let f = Fixture::new(60);
    enforce(&f, Some(1.0), Some(1.0), None);
    let svc = crate::FirehoseService::default().with_engine(f.engine.clone());
    for (action, body) in [
        (
            "PutRecord",
            json!({"DeliveryStreamName": NAME, "Record": {"Data": "%%%"}}),
        ),
        (
            "PutRecordBatch",
            json!({"DeliveryStreamName": NAME, "Records": [{"Data": "YQ=="}, {"Data": "%%%"}]}),
        ),
    ] {
        let error = svc.handle(request(action, body)).await.err().unwrap();
        assert_eq!(error.status(), StatusCode::BAD_REQUEST);
        assert_eq!(error.code(), "SerializationException");
        assert!(buffered(&f).is_empty());
    }
    svc.handle(request(
        "PutRecord",
        json!({"DeliveryStreamName": NAME,
        "Record": {"Data": "Yg=="}}),
    ))
    .await
    .unwrap();
    let error = svc
        .handle(request(
            "PutRecordBatch",
            json!({"DeliveryStreamName": NAME,
        "Records": [{"Data": "Yw=="}]}),
        ))
        .await
        .err()
        .unwrap();
    assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(error.code(), "ServiceUnavailableException");
    assert_eq!(error.message(), "Slow down.");
    assert_eq!(buffered(&f), vec![Bytes::from_static(b"b")]);
}

#[tokio::test]
async fn public_decoding_charges_only_valid_records_and_decoded_bytes() {
    use fakecloud_core::service::AwsService;
    let f = Fixture::new(60);
    enforce(&f, Some(2.0), Some(2.0), Some(4.0 / 1_048_576.0));
    let svc = crate::FirehoseService::default().with_engine(f.engine.clone());
    svc.handle(request(
        "PutRecord",
        json!({"DeliveryStreamName": NAME, "Record": {"Data": "YQ=="}}),
    ))
    .await
    .unwrap();
    let response = svc
        .handle(request(
            "PutRecordBatch",
            json!({"DeliveryStreamName": NAME,
        "Records": [{"Data": "YmNk"}]}),
        ))
        .await
        .unwrap();
    let body: serde_json::Value = serde_json::from_slice(response.body.expect_bytes()).unwrap();
    assert_eq!(body["FailedPutCount"], 0);
    let error = svc
        .handle(request(
            "PutRecord",
            json!({"DeliveryStreamName": NAME, "Record": {"Data": ""}}),
        ))
        .await
        .err()
        .expect("must throttle");
    assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(error.code(), "ServiceUnavailableException");
    f.advance(60);
    f.turn().await;
    assert_eq!(
        f.batches()[0].records,
        vec![Bytes::from_static(b"a"), Bytes::from_static(b"bcd")]
    );
}

#[tokio::test]
async fn public_request_validation_never_debits_or_partially_admits() {
    use base64::Engine;
    use fakecloud_core::service::AwsService;
    let f = Fixture::new(60);
    enforce(&f, Some(1.0), Some(1.0), None);
    let svc = crate::FirehoseService::default().with_engine(f.engine.clone());
    let oversized = base64::engine::general_purpose::STANDARD.encode(vec![0; 1_024_001]);
    for (action, body) in [
        (
            "PutRecord",
            json!({"DeliveryStreamName": NAME, "Record": {"Data": oversized}}),
        ),
        (
            "PutRecordBatch",
            json!({"DeliveryStreamName": NAME, "Records": []}),
        ),
        (
            "PutRecordBatch",
            json!({"DeliveryStreamName": NAME, "Records": [{"Data": "YQ=="}, {"Data": oversized}]}),
        ),
    ] {
        assert_eq!(
            svc.handle(request(action, body))
                .await
                .err()
                .expect("must validate")
                .code(),
            "InvalidArgumentException"
        );
    }
    svc.handle(request(
        "PutRecord",
        json!({"DeliveryStreamName": NAME, "Record": {"Data": "Yg=="}}),
    ))
    .await
    .unwrap();
    let error = svc
        .handle(request(
            "PutRecordBatch",
            json!({"DeliveryStreamName": NAME, "Records": [{"Data": "Yw=="}, {"Data": "ZA=="}]}),
        ))
        .await
        .err()
        .expect("must throttle");
    assert_eq!(error.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(error.code(), "ServiceUnavailableException");
    f.advance(60);
    f.turn().await;
    assert_eq!(f.batches()[0].records, vec![Bytes::from_static(b"b")]);
}

#[tokio::test]
async fn all_throttled_public_batch_consumes_one_request_but_no_records() {
    use fakecloud_core::service::AwsService;
    let f = Fixture::new(60);
    enforce(&f, Some(1.0), Some(0.0), Some(0.0));
    let svc = crate::FirehoseService::default().with_engine(f.engine.clone());
    svc.handle(request(
        "PutRecordBatch",
        json!({"DeliveryStreamName": NAME, "Records": [{"Data": "YQ=="}]}),
    ))
    .await
    .unwrap();
    throttled(put(&f, vec![]));
}

#[tokio::test]
async fn stale_windows_are_pruned_without_buffered_records() {
    for final_scan in [false, true] {
        let f = Fixture::new(60);
        f.update(NAME, |s| s.destination = None);
        enforce(&f, Some(1.0), None, None);
        put(&f, vec![record(b'a', 1)]).unwrap();
        let saved = f.engine.core.state.read().clone();
        f.engine.core.state.write().accounts.clear();
        if final_scan {
            assert_eq!(
                f.engine.core.finish_shutdown(&BTreeSet::new()),
                Some(DrainReport::default())
            );
        } else {
            f.turn().await;
        }
        // Reinstall the same identity to detect leaked windows independently
        // of the fresh UUID behavior tested by delete/reset/restore above.
        *f.engine.core.state.write() = saved;
        put(&f, vec![record(b'b', 1)]).unwrap();
        throttled(put(&f, vec![record(b'x', 1)]));
    }
}

#[tokio::test]
async fn successful_delivery_and_destination_updates_do_not_reset_budget() {
    let f = Fixture::new(0);
    enforce(&f, Some(1.0), None, None);
    put(&f, vec![record(b'a', 1)]).unwrap();
    f.turn().await;
    assert!(f.engine.core.registry.lock().streams.is_empty());
    f.update(NAME, |s| {
        s.version_id = "2".into();
        s.destination.as_mut().unwrap().prefix = Some("updated/".into());
    });
    throttled(put(&f, vec![record(b'x', 1)]));
    f.advance(1);
    put(&f, vec![record(b'b', 1)]).unwrap();
    f.turn().await;
    let batches = f.batches();
    assert_eq!(batches.len(), 2);
    assert_eq!(batches[0].records, vec![record(b'a', 1)]);
    assert_eq!(batches[1].records, vec![record(b'b', 1)]);
    assert!(batches[1].object_key.starts_with("updated/"));
}

/// Without S3 delivery the scheduler never runs for a stream, so its usage
/// must go when the stream is deleted (or reset away) on its own.
#[tokio::test]
async fn deleted_streams_without_s3_delivery_drop_their_usage_without_a_scan() {
    use fakecloud_core::service::AwsService;
    let windows = |f: &Fixture| f.engine.core.registry.lock().windows.len();

    // DeleteDeliveryStream prunes immediately.
    let f = Fixture::new(60);
    f.update(NAME, |s| s.destination = None);
    enforce(&f, Some(10.0), None, None);
    let svc = crate::FirehoseService::default().with_engine(f.engine.clone());
    put(&f, vec![record(b'a', 1)]).unwrap();
    assert_eq!(windows(&f), 1);
    svc.handle(request(
        "DeleteDeliveryStream",
        json!({"DeliveryStreamName": NAME}),
    ))
    .await
    .unwrap();
    assert_eq!(windows(&f), 0);

    // A reset only touches configuration; the next metered call prunes it.
    let f = Fixture::new(60);
    f.update(NAME, |s| s.destination = None);
    enforce(&f, Some(10.0), None, None);
    put(&f, vec![record(b'a', 1)]).unwrap();
    f.engine.core.state.write().accounts.clear();
    f.add_stream(ACCOUNT, REGION, "other", 60);
    f.update("other", |s| s.destination = None);
    f.engine
        .enqueue_put(ACCOUNT, REGION, "other", vec![record(b'b', 1)])
        .unwrap();
    let registry = f.engine.core.registry.lock();
    assert_eq!(
        registry
            .windows
            .keys()
            .map(|key| key.name.as_str())
            .collect::<Vec<_>>(),
        ["other"]
    );
}
