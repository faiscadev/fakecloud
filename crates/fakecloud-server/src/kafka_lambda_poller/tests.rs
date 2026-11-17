use super::*;

use std::sync::atomic::{AtomicUsize, Ordering};

use fakecloud_core::delivery::LambdaInvocationOutput;
use fakecloud_core::multi_account::{MultiAccountState, MultiRegionState};
use fakecloud_core::service::{AwsRequest, AwsService};
use fakecloud_kafka::client::records::encode;
use fakecloud_kafka::client::wire::{Decoder, Encoder};
use fakecloud_kafka::client::TimestampType;
use fakecloud_lambda::LambdaState;
use parking_lot::{Mutex, RwLock};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const ACCOUNT: &str = "123456789012";
const REGION: &str = "us-east-1";
const FUNCTION: &str = "arn:aws:lambda:us-east-1:123456789012:function:consumer";
const CLUSTER: &str =
    "arn:aws:kafka:us-east-1:123456789012:cluster/demo/0b1c2d3e-1111-2222-3333-444455556666-1";

fn esm(extra: Value) -> EventSourceMapping {
    let mut base = json!({
        "uuid": "esm-1",
        "function_arn": FUNCTION,
        "event_source_arn": "",
        "batch_size": 100,
        "enabled": true,
        "state": "Enabled",
        "last_modified": "2026-11-16T00:00:00Z",
        "topics": ["orders"],
    });
    for (k, v) in extra.as_object().unwrap() {
        base[k] = v.clone();
    }
    serde_json::from_value(base).unwrap()
}

fn self_managed(port: u16, extra: Value) -> EventSourceMapping {
    let mut fields = json!({
        "self_managed_event_source": {
            "Endpoints": { "KAFKA_BOOTSTRAP_SERVERS": [format!("127.0.0.1:{port}")] }
        },
        "starting_position": "TRIM_HORIZON",
        "maximum_batching_window_in_seconds": 0,
    });
    for (k, v) in extra.as_object().unwrap() {
        fields[k] = v.clone();
    }
    esm(fields)
}

fn lambda_state_with(mapping: &EventSourceMapping) -> SharedLambdaState {
    let mut lambda: MultiRegionState<LambdaState> = MultiRegionState::new(ACCOUNT, REGION, "");
    lambda
        .regional_mut(ACCOUNT, REGION)
        .event_source_mappings
        .insert(mapping.uuid.clone(), mapping.clone());
    Arc::new(RwLock::new(lambda))
}

fn kafka_state() -> SharedKafkaState {
    Arc::new(RwLock::new(MultiAccountState::new(ACCOUNT, REGION, "")))
}

fn secrets_state() -> SharedSecretsManagerState {
    Arc::new(RwLock::new(MultiRegionState::new(ACCOUNT, REGION, "")))
}

/// Create a secret through the Secrets Manager API; its ARN.
async fn create_secret(secrets: &SharedSecretsManagerState, name: &str, value: Value) -> String {
    let svc = fakecloud_secretsmanager::SecretsManagerService::new(secrets.clone());
    let body = json!({ "Name": name, "SecretString": value.to_string() });
    let resp = svc
        .handle(AwsRequest {
            service: "secretsmanager".to_string(),
            action: "CreateSecret".to_string(),
            region: REGION.to_string(),
            account_id: ACCOUNT.to_string(),
            request_id: "test".to_string(),
            headers: axum::http::HeaderMap::new(),
            query_params: HashMap::new(),
            body: serde_json::to_vec(&body).unwrap().into(),
            body_stream: Mutex::new(None),
            path_segments: vec![],
            raw_path: "/".to_string(),
            raw_query: String::new(),
            method: axum::http::Method::POST,
            is_query_protocol: false,
            access_key_id: None,
            principal: None,
        })
        .await
        .expect("CreateSecret");
    let out: Value = serde_json::from_slice(resp.body.expect_bytes()).unwrap();
    out["ARN"].as_str().unwrap().to_string()
}

fn poller(
    lambda_state: SharedLambdaState,
    kafka: SharedKafkaState,
    secrets: SharedSecretsManagerState,
    delivery: Option<Arc<dyn LambdaDelivery>>,
) -> Arc<KafkaLambdaPoller> {
    let mut p = KafkaLambdaPoller::new(lambda_state, kafka, secrets);
    if let Some(d) = delivery {
        p = p.with_lambda_delivery(d);
    }
    Arc::new(p)
}

fn mapping_of(m: &EventSourceMapping) -> KafkaMapping {
    KafkaMapping::of(m, ACCOUNT, REGION).expect("a Kafka mapping")
}

fn buffered(topic: &str, partition: i32, offset: i64) -> Buffered {
    Buffered {
        topic: topic.to_string(),
        partition,
        record: ConsumedRecord {
            offset,
            timestamp_ms: 1_700_000_000_000 + offset,
            timestamp_type: TimestampType::CreateTime,
            key: Some(b"key".to_vec()),
            value: Some(format!("{{\"n\":{offset}}}").into_bytes()),
            headers: vec![("trace".to_string(), Some(vec![0x61, 0xff]))],
        },
    }
}

#[test]
fn mapping_settings_follow_the_kafka_defaults() {
    let m = mapping_of(&esm(json!({
        "event_source_arn": CLUSTER,
        "amazon_managed_kafka_event_source_config": { "ConsumerGroupId": "esm-1" },
        "destination_config": { "OnFailure": { "Destination": "arn:aws:sqs:us-east-1:123456789012:dlq" } },
        "function_response_types": ["ReportBatchItemFailures"],
        "starting_position": "AT_TIMESTAMP",
        "starting_position_timestamp": 1_700_000_000.5,
    })));
    assert_eq!(
        m.source,
        KafkaSource::Msk {
            cluster_arn: CLUSTER.to_string()
        }
    );
    assert_eq!(m.group_id, "esm-1");
    assert_eq!(m.batching_window, DEFAULT_BATCHING_WINDOW);
    assert_eq!(
        m.starting_position,
        StartingPosition::AtTimestamp(1_700_000_000_500)
    );
    // Retries are capped at 10 with a destination; batches succeed or fail
    // whole.
    assert_eq!(m.policy.max_retries, Some(RETRIES_WITH_DESTINATION));
    assert!(!m.policy.report_batch_item_failures);
    assert_eq!(m.event_source(), "aws:kafka");

    let sm = mapping_of(&self_managed(
        9092,
        json!({
            "self_managed_kafka_event_source_config": { "ConsumerGroupId": "orders-group", "ConsumptionMode": "Queue" },
            "maximum_batching_window_in_seconds": 3,
            "starting_position": "LATEST",
        }),
    ));
    assert_eq!(
        sm.source,
        KafkaSource::SelfManaged {
            bootstrap: vec!["127.0.0.1:9092".to_string()]
        }
    );
    assert_eq!(sm.group_id, "orders-group");
    assert_eq!(sm.batching_window, Duration::from_secs(3));
    assert_eq!(sm.starting_position, StartingPosition::Latest);
    assert!(sm.unsupported.as_deref().unwrap().contains("Queue"));
    assert_eq!(sm.policy.max_retries, None);

    // A mapping with no Kafka config groups under its UUID; other sources
    // are not Kafka mappings.
    let bare = mapping_of(&esm(json!({ "event_source_arn": CLUSTER })));
    assert_eq!(bare.group_id, "esm-1");
    assert!(KafkaMapping::of(
        &esm(json!({ "event_source_arn": "arn:aws:sqs:us-east-1:123456789012:q" })),
        ACCOUNT,
        REGION
    )
    .is_none());
}

#[test]
fn the_event_is_lambdas_kafka_event() {
    let m = mapping_of(&esm(json!({ "event_source_arn": CLUSTER })));
    let records = [
        buffered("orders", 0, 7),
        buffered("orders", 1, 3),
        buffered("orders", 0, 8),
    ];
    let shaped: Vec<(&Buffered, Value)> = records.iter().map(|b| (b, event_record(b))).collect();
    let event = kafka_event(&m, "127.0.0.1:19092", &shaped);
    assert_eq!(event["eventSource"], "aws:kafka");
    assert_eq!(event["eventSourceArn"], CLUSTER);
    assert_eq!(event["bootstrapServers"], "127.0.0.1:19092");
    let p0 = event["records"]["orders-0"].as_array().unwrap();
    assert_eq!(p0.len(), 2);
    assert_eq!(event["records"]["orders-1"].as_array().unwrap().len(), 1);
    assert_eq!(
        p0[0],
        json!({
            "topic": "orders",
            "partition": 0,
            "offset": 7,
            "timestamp": 1_700_000_000_007i64,
            "timestampType": "CREATE_TIME",
            "key": "a2V5",
            "value": B64.encode(br#"{"n":7}"#),
            "headers": [{ "trace": [97, -1] }],
        })
    );

    // Self-managed: no ARN; a null key and value are left out.
    let sm = mapping_of(&self_managed(9092, json!({})));
    let mut bare = buffered("orders", 0, 1);
    bare.record.key = None;
    bare.record.value = None;
    bare.record.headers.clear();
    let rec = event_record(&bare);
    let event = kafka_event(&sm, "a:1,b:2", &[(&bare, rec)]);
    assert_eq!(event["eventSource"], "SelfManagedKafka");
    assert!(event.get("eventSourceArn").is_none());
    let rec = &event["records"]["orders-0"][0];
    assert!(rec.get("key").is_none() && rec.get("value").is_none());
    assert_eq!(rec["headers"], json!([]));
}

#[test]
fn filters_see_the_decoded_value() {
    let b = buffered("orders", 0, 4);
    let view = filter_view(&b, &event_record(&b));
    assert_eq!(view["value"], json!({ "n": 4 }));
    let filter = FilterSet::from_strings([r#"{"value":{"n":[4]}}"#]);
    assert!(filter.matches(&view));
    let mut text = buffered("orders", 0, 5);
    text.record.value = Some(b"plain".to_vec());
    assert_eq!(filter_view(&text, &event_record(&text))["value"], "plain");
}

#[test]
fn batches_are_cut_at_the_batch_size_and_payload_limit() {
    assert_eq!(batch_len_within(&[10; 50], 20, MAX_PAYLOAD_BYTES), 20);
    assert_eq!(batch_len_within(&[400; 10], 100, 1000), 2);
    // One record larger than the limit still goes alone.
    assert_eq!(batch_len_within(&[5000, 10], 100, 1000), 1);
    assert_eq!(batch_len_within(&[], 100, 1000), 0);
}

#[test]
fn handled_records_commit_each_partitions_next_offset() {
    let handled = [
        buffered("orders", 0, 7),
        buffered("orders", 1, 3),
        buffered("orders", 0, 8),
        buffered("audit", 0, 0),
    ];
    let offsets = consumed_offsets(&handled);
    assert_eq!(offsets[&("orders".to_string(), 0)], 9);
    assert_eq!(offsets[&("orders".to_string(), 1)], 4);
    assert_eq!(offsets[&("audit".to_string(), 0)], 1);
}

#[test]
fn a_discarded_batch_is_described_with_kafka_batch_info() {
    let m = mapping_of(&esm(json!({ "event_source_arn": CLUSTER })));
    let batch = [
        buffered("orders", 0, 7),
        buffered("orders", 0, 8),
        buffered("orders", 1, 3),
    ];
    let record = failure_record(
        &m,
        "127.0.0.1:19092",
        &batch,
        4,
        &DiscardCause::RetriesExhausted {
            function_error: Some("Unhandled".to_string()),
        },
        "{\"records\":{}}",
        false,
    );
    assert_eq!(
        record["requestContext"]["condition"],
        "RetryAttemptsExhausted"
    );
    assert_eq!(record["requestContext"]["approximateInvokeCount"], 4);
    assert_eq!(record["requestContext"]["functionArn"], FUNCTION);
    assert_eq!(record["responseContext"]["functionError"], "Unhandled");
    let info = &record["KafkaBatchInfo"];
    assert_eq!(info["batchSize"], 3);
    assert_eq!(info["eventSourceArn"], CLUSTER);
    assert_eq!(info["bootstrapServers"], "127.0.0.1:19092");
    assert_eq!(info["payloadSize"], 14);
    assert_eq!(
        info["recordsInfo"]["orders-0"],
        json!({
            "firstRecordOffset": "7",
            "lastRecordOffset": "8",
            "firstRecordTimestamp": "2023-11-14T22:13:20.007Z",
            "lastRecordTimestamp": "2023-11-14T22:13:20.008Z",
        })
    );
    assert!(record.get("payload").is_none());
    let aged = failure_record(
        &m,
        "b",
        &batch[..1],
        1,
        &DiscardCause::RecordAge,
        "{}",
        true,
    );
    assert_eq!(aged["requestContext"]["condition"], "RecordAgeExceeded");
    assert!(aged.get("responseContext").is_none());
    assert_eq!(aged["payload"], "{}");
}

/// Put an MSK cluster (its stored wire shape) with a running broker at
/// `bootstrap` into the Kafka state.
fn put_cluster(kafka: &SharedKafkaState, client_auth: Value, bootstrap: Option<u16>) {
    let mut state = kafka.write();
    let data = state.get_or_create(ACCOUNT);
    data.clusters.insert(
        CLUSTER.to_string(),
        json!({ "clusterArn": CLUSTER, "clientAuthentication": client_auth }),
    );
    if let Some(port) = bootstrap {
        data.data_plane.insert(
            CLUSTER.to_string(),
            fakecloud_kafka::ClusterDataPlane {
                container_id: "c".to_string(),
                host: "127.0.0.1".to_string(),
                ports: [("plaintext".to_string(), port)].into_iter().collect(),
            },
        );
    }
}

#[tokio::test]
async fn msk_mappings_reach_the_clusters_broker_and_check_its_auth() {
    let kafka = kafka_state();
    let secrets = secrets_state();
    let plain = mapping_of(&esm(json!({ "event_source_arn": CLUSTER })));
    let make = |m: &KafkaMapping| {
        poller(
            lambda_state_with(&esm(json!({}))),
            kafka.clone(),
            secrets.clone(),
            None,
        )
        .endpoint(m)
    };

    assert!(make(&plain).unwrap_err().contains("does not exist"));
    put_cluster(&kafka, json!({}), None);
    assert_eq!(make(&plain).unwrap_err(), NO_BROKER);

    put_cluster(
        &kafka,
        json!({ "sasl": { "scram": { "enabled": true } } }),
        Some(19092),
    );
    assert_eq!(
        make(&plain).unwrap(),
        Endpoint {
            bootstrap: vec!["127.0.0.1:19092".to_string()],
            bootstrap_servers: "127.0.0.1:19092".to_string(),
            sasl: None,
        }
    );

    let secret = create_secret(
        &secrets,
        "AmazonMSK_lambda",
        json!({ "username": "u", "password": "p" }),
    )
    .await;
    let scram = mapping_of(&esm(json!({
        "event_source_arn": CLUSTER,
        "source_access_configurations": [{ "Type": "SASL_SCRAM_512_AUTH", "URI": secret }],
    })));
    // The secret must be associated with the cluster, as MSK's SCRAM users are.
    assert!(make(&scram).unwrap_err().contains("not associated"));
    kafka
        .write()
        .get_or_create(ACCOUNT)
        .scram_secrets
        .insert(CLUSTER.to_string(), vec![secret.clone()]);
    assert!(make(&scram).is_ok());

    let tls = mapping_of(&esm(json!({
        "event_source_arn": CLUSTER,
        "source_access_configurations": [{ "Type": "CLIENT_CERTIFICATE_TLS_AUTH", "URI": secret }],
    })));
    assert!(make(&tls)
        .unwrap_err()
        .starts_with("PROBLEM: Server failed to authenticate Lambda"));
}

#[tokio::test]
async fn self_managed_mappings_present_sasl_credentials_or_refuse_tls() {
    let secrets = secrets_state();
    let secret = create_secret(
        &secrets,
        "kafka-creds",
        json!({ "username": "alice", "password": "s3cret" }),
    )
    .await;
    let p = poller(
        lambda_state_with(&esm(json!({}))),
        kafka_state(),
        secrets.clone(),
        None,
    );
    let scram = mapping_of(&self_managed(
        9092,
        json!({ "source_access_configurations": [
            { "Type": "VPC_SUBNET", "URI": "subnet:subnet-1" },
            { "Type": "SASL_SCRAM_256_AUTH", "URI": secret },
        ] }),
    ));
    assert_eq!(
        p.endpoint(&scram).unwrap().sasl,
        Some(SaslCredentials {
            mechanism: SaslMechanism::Scram(ScramAlgorithm::Sha256),
            username: "alice".to_string(),
            password: "s3cret".to_string(),
        })
    );
    let basic = mapping_of(&self_managed(
        9092,
        json!({ "source_access_configurations": [{ "Type": "BASIC_AUTH", "URI": secret }] }),
    ));
    assert_eq!(
        p.endpoint(&basic).unwrap().sasl.unwrap().mechanism,
        SaslMechanism::Plain
    );
    let missing = mapping_of(&self_managed(
        9092,
        json!({ "source_access_configurations": [{ "Type": "BASIC_AUTH", "URI": "arn:aws:secretsmanager:us-east-1:123456789012:secret:nope-AbCdEf" }] }),
    ));
    assert!(p
        .endpoint(&missing)
        .unwrap_err()
        .starts_with("PROBLEM: SASL authentication failed."));
    let tls = mapping_of(&self_managed(
        9092,
        json!({ "source_access_configurations": [{ "Type": "SERVER_ROOT_CA_CERTIFICATE", "URI": secret }] }),
    ));
    assert!(p
        .endpoint(&tls)
        .unwrap_err()
        .contains("SERVER_ROOT_CA_CERTIFICATE is not supported"));
}

// ---- an in-process broker double, speaking the client's API versions ----

/// A stored record's `(key, value)`.
type KeyValue = (Option<Vec<u8>>, Option<Vec<u8>>);

#[derive(Default)]
struct BrokerState {
    /// Topic `orders`, partition 0, per offset.
    records: Vec<KeyValue>,
    /// `(group, topic, partition)` -> committed offset.
    committed: HashMap<(String, String, i32), i64>,
    /// Every commit, in order.
    commits: Vec<(String, i32, i64)>,
}

const BASE_TS: i64 = 1_700_000_000_000;

async fn read_frame(sock: &mut TcpStream) -> Option<(i16, i32, Vec<u8>)> {
    let size = sock.read_i32().await.ok()?;
    let mut buf = vec![0u8; size as usize];
    sock.read_exact(&mut buf).await.ok()?;
    let mut d = Decoder::new(&buf);
    let key = d.i16().ok()?;
    let _version = d.i16().ok()?;
    let correlation = d.i32().ok()?;
    let _client = d.nullable_string().ok()?;
    Some((key, correlation, d.rest().to_vec()))
}

fn answer(state: &Mutex<BrokerState>, port: u16, key: i16, body: &[u8]) -> (Vec<u8>, bool) {
    let mut d = Decoder::new(body);
    let mut e = Encoder::new();
    let mut idle = false;
    match key {
        3 => {
            let topics = d.array(|d| d.string()).unwrap();
            e.i32(0)
                .i32(1)
                .i32(1)
                .string("127.0.0.1")
                .i32(i32::from(port))
                .nullable_string(None)
                .nullable_string(None)
                .i32(1)
                .array(&topics, |e, t| {
                    if t == "orders" {
                        e.i16(0).string(t).bool(false).i32(1);
                        e.i16(0).i32(0).i32(1).i32(1).i32(1).i32(1).i32(1);
                    } else {
                        e.i16(3).string(t).bool(false).i32(0);
                    }
                });
        }
        10 => {
            e.i32(0)
                .i16(0)
                .nullable_string(None)
                .i32(1)
                .string("127.0.0.1")
                .i32(i32::from(port));
        }
        9 => {
            let group = d.string().unwrap();
            let topics = d
                .array(|d| Ok((d.string()?, d.array(|d| d.i32())?)))
                .unwrap();
            let st = state.lock();
            e.array(&topics, |e, (name, parts)| {
                e.string(name).array(parts, |e, p| {
                    let committed = st
                        .committed
                        .get(&(group.clone(), name.clone(), *p))
                        .copied()
                        .unwrap_or(-1);
                    e.i32(*p).i64(committed).nullable_string(None).i16(0);
                });
            });
        }
        8 => {
            let group = d.string().unwrap();
            let _generation = d.i32().unwrap();
            let _member = d.string().unwrap();
            let _retention = d.i64().unwrap();
            let topics = d
                .array(|d| {
                    let name = d.string()?;
                    let parts = d.array(|d| {
                        let p = d.i32()?;
                        let o = d.i64()?;
                        let _meta = d.nullable_string()?;
                        Ok((p, o))
                    })?;
                    Ok((name, parts))
                })
                .unwrap();
            let mut st = state.lock();
            for (name, parts) in &topics {
                for (p, o) in parts {
                    st.committed.insert((group.clone(), name.clone(), *p), *o);
                    st.commits.push((name.clone(), *p, *o));
                }
            }
            e.array(&topics, |e, (name, parts)| {
                e.string(name).array(parts, |e, (p, _)| {
                    e.i32(*p).i16(0);
                });
            });
        }
        2 => {
            let _replica = d.i32().unwrap();
            let topics = d
                .array(|d| Ok((d.string()?, d.array(|d| Ok((d.i32()?, d.i64()?)))?)))
                .unwrap();
            let len = state.lock().records.len() as i64;
            e.array(&topics, |e, (name, parts)| {
                e.string(name).array(parts, |e, (p, ts)| {
                    let offset = match *ts {
                        -2 => 0,
                        -1 => len,
                        ts => {
                            let first = ts - BASE_TS;
                            if first < len {
                                first.max(0)
                            } else {
                                -1
                            }
                        }
                    };
                    e.i32(*p).i16(0).i64(-1).i64(offset);
                });
            });
        }
        1 => {
            let _replica = d.i32().unwrap();
            let _max_wait = d.i32().unwrap();
            let _min_bytes = d.i32().unwrap();
            let _max_bytes = d.i32().unwrap();
            let _isolation = d.i8().unwrap();
            let topics = d
                .array(|d| {
                    Ok((
                        d.string()?,
                        d.array(|d| Ok((d.i32()?, d.i64()?, d.i32()?)))?,
                    ))
                })
                .unwrap();
            let st = state.lock();
            let hw = st.records.len() as i64;
            e.i32(0).array(&topics, |e, (name, parts)| {
                e.string(name).array(parts, |e, (p, offset, _)| {
                    e.i32(*p).i16(0).i64(hw).i64(hw).i32(-1);
                    let from = (*offset).clamp(0, hw) as usize;
                    if from as i64 >= hw {
                        idle = true;
                        e.bytes(&[]);
                        return;
                    }
                    let recs: Vec<encode::TestRecord<'_>> = st.records[from..]
                        .iter()
                        .enumerate()
                        .map(|(i, (k, v))| (i as i64, k.as_deref(), v.as_deref(), vec![]))
                        .collect();
                    e.bytes(&encode::batch(
                        from as i64,
                        BASE_TS + from as i64,
                        0,
                        0,
                        &recs,
                    ));
                });
            });
        }
        other => panic!("unexpected api key {other}"),
    }
    (e.into_bytes(), idle)
}

/// A broker serving topic `orders` (one partition) on a loopback port.
async fn broker(records: usize) -> (u16, Arc<Mutex<BrokerState>>) {
    let state = Arc::new(Mutex::new(BrokerState::default()));
    for i in 0..records {
        state.lock().records.push((
            Some(format!("k{i}").into_bytes()),
            Some(format!("{{\"n\":{i}}}").into_bytes()),
        ));
    }
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let shared = state.clone();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let state = shared.clone();
            tokio::spawn(async move {
                while let Some((key, correlation, body)) = read_frame(&mut sock).await {
                    let (resp, idle) = answer(&state, port, key, &body);
                    if idle {
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    let mut e = Encoder::new();
                    e.i32(resp.len() as i32 + 4).i32(correlation).raw(&resp);
                    if sock.write_all(&e.into_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    (port, state)
}

/// A function that fails its first `fail` invocations.
struct ScriptedDelivery {
    fail: usize,
    calls: Mutex<Vec<Value>>,
    count: AtomicUsize,
}

impl ScriptedDelivery {
    fn new(fail: usize) -> Arc<Self> {
        Arc::new(Self {
            fail,
            calls: Mutex::new(Vec::new()),
            count: AtomicUsize::new(0),
        })
    }

    fn offsets(&self, call: usize) -> Vec<i64> {
        let calls = self.calls.lock();
        calls[call]["records"]
            .as_object()
            .unwrap()
            .values()
            .flat_map(|v| {
                v.as_array()
                    .unwrap()
                    .iter()
                    .map(|r| r["offset"].as_i64().unwrap())
            })
            .collect()
    }
}

impl LambdaDelivery for ScriptedDelivery {
    fn invoke_lambda(
        &self,
        _function_arn: &str,
        payload: &str,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<LambdaInvocationOutput, String>> + Send>,
    > {
        self.calls
            .lock()
            .push(serde_json::from_str(payload).unwrap());
        let n = self.count.fetch_add(1, Ordering::SeqCst);
        let fail = n < self.fail;
        Box::pin(async move {
            Ok(if fail {
                LambdaInvocationOutput::unhandled(
                    br#"{"errorType":"Error","errorMessage":"boom"}"#.to_vec(),
                )
            } else {
                LambdaInvocationOutput::success(b"null".to_vec())
            })
        })
    }
}

async fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !cond() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn last_result(lambda: &SharedLambdaState) -> Option<String> {
    lambda
        .read()
        .regional(ACCOUNT, REGION)
        .unwrap()
        .event_source_mappings["esm-1"]
        .last_processing_result
        .clone()
}

/// A failing batch is retried whole; its offsets are committed only once it
/// succeeded, and the mapping then reports OK.
#[tokio::test]
async fn a_batch_is_retried_and_committed_only_after_it_succeeds() {
    let (port, broker) = broker(3).await;
    let mapping = self_managed(port, json!({}));
    let lambda = lambda_state_with(&mapping);
    let delivery = ScriptedDelivery::new(1);
    let p = poller(
        lambda.clone(),
        kafka_state(),
        secrets_state(),
        Some(delivery.clone()),
    );
    let task = tokio::spawn(p.consume(mapping_of(&mapping)));

    wait_until("the retry", || delivery.count.load(Ordering::SeqCst) >= 2).await;
    wait_until("the commit", || {
        broker
            .lock()
            .committed
            .get(&("esm-1".to_string(), "orders".to_string(), 0))
            == Some(&3)
    })
    .await;
    task.abort();

    assert_eq!(delivery.offsets(0), vec![0, 1, 2]);
    assert_eq!(delivery.offsets(1), vec![0, 1, 2]);
    let call = delivery.calls.lock()[0].clone();
    assert_eq!(call["eventSource"], "SelfManagedKafka");
    assert_eq!(call["bootstrapServers"], format!("127.0.0.1:{port}"));
    assert_eq!(
        call["records"]["orders-0"][1]["value"],
        B64.encode(br#"{"n":1}"#)
    );
    assert_eq!(call["records"]["orders-0"][1]["key"], B64.encode(b"k1"));
    assert_eq!(call["records"]["orders-0"][1]["timestamp"], BASE_TS + 1);
    // The start position was pinned (0), then the batch committed once.
    assert_eq!(
        broker.lock().commits,
        vec![("orders".to_string(), 0, 0), ("orders".to_string(), 0, 3)]
    );
    assert_eq!(last_result(&lambda).as_deref(), Some("OK"));
}

/// A new mapping in a group with committed offsets resumes after them, and
/// BatchSize splits what is left.
#[tokio::test]
async fn a_consumer_resumes_after_its_groups_committed_offsets() {
    let (port, broker) = broker(5).await;
    broker
        .lock()
        .committed
        .insert(("orders-group".to_string(), "orders".to_string(), 0), 2);
    let mapping = self_managed(
        port,
        json!({
            "batch_size": 2,
            "self_managed_kafka_event_source_config": { "ConsumerGroupId": "orders-group" },
        }),
    );
    let delivery = ScriptedDelivery::new(0);
    let p = poller(
        lambda_state_with(&mapping),
        kafka_state(),
        secrets_state(),
        Some(delivery.clone()),
    );
    let task = tokio::spawn(p.consume(mapping_of(&mapping)));
    wait_until("both batches", || {
        delivery.count.load(Ordering::SeqCst) >= 2
    })
    .await;
    wait_until("the commit", || {
        broker
            .lock()
            .committed
            .get(&("orders-group".to_string(), "orders".to_string(), 0))
            == Some(&5)
    })
    .await;
    task.abort();
    assert_eq!(delivery.offsets(0), vec![2, 3]);
    assert_eq!(delivery.offsets(1), vec![4]);
}

/// LATEST skips what the topic already holds; a record produced afterwards
/// is delivered.
#[tokio::test]
async fn latest_starts_after_the_existing_records() {
    let (port, broker) = broker(3).await;
    let mapping = self_managed(port, json!({ "starting_position": "LATEST" }));
    let delivery = ScriptedDelivery::new(0);
    let p = poller(
        lambda_state_with(&mapping),
        kafka_state(),
        secrets_state(),
        Some(delivery.clone()),
    );
    let task = tokio::spawn(p.consume(mapping_of(&mapping)));
    wait_until("the pinned start", || {
        broker
            .lock()
            .committed
            .get(&("esm-1".to_string(), "orders".to_string(), 0))
            == Some(&3)
    })
    .await;
    broker
        .lock()
        .records
        .push((None, Some(b"{\"n\":3}".to_vec())));
    wait_until("the new record", || {
        delivery.count.load(Ordering::SeqCst) >= 1
    })
    .await;
    task.abort();
    assert_eq!(delivery.offsets(0), vec![3]);
    assert!(delivery.calls.lock()[0]["records"]["orders-0"][0]
        .get("key")
        .is_none());
}

/// Records the filter drops are not delivered but are committed.
#[tokio::test]
async fn filtered_out_records_are_committed_without_an_invocation() {
    let (port, broker) = broker(3).await;
    let mapping = self_managed(
        port,
        json!({ "filter_patterns": [r#"{"value":{"n":[1]}}"#] }),
    );
    let delivery = ScriptedDelivery::new(0);
    let p = poller(
        lambda_state_with(&mapping),
        kafka_state(),
        secrets_state(),
        Some(delivery.clone()),
    );
    let task = tokio::spawn(p.consume(mapping_of(&mapping)));
    wait_until("the commit", || {
        broker
            .lock()
            .committed
            .get(&("esm-1".to_string(), "orders".to_string(), 0))
            == Some(&3)
    })
    .await;
    task.abort();
    assert_eq!(delivery.count.load(Ordering::SeqCst), 1);
    assert_eq!(delivery.offsets(0), vec![1]);
}

/// A topic the cluster does not have is reported on the mapping.
#[tokio::test]
async fn a_missing_topic_is_reported() {
    let (port, _broker) = broker(0).await;
    let mapping = self_managed(port, json!({ "topics": ["nope"] }));
    let lambda = lambda_state_with(&mapping);
    let p = poller(lambda.clone(), kafka_state(), secrets_state(), None);
    let task = tokio::spawn(p.consume(mapping_of(&mapping)));
    wait_until("the problem", || last_result(&lambda).is_some()).await;
    task.abort();
    assert_eq!(
        last_result(&lambda).as_deref(),
        Some("PROBLEM: Topic not found: nope")
    );
}

/// With retries used up, the batch goes to the OnFailure destination and its
/// offsets are committed.
#[tokio::test]
async fn exhausted_retries_discard_and_commit_the_batch() {
    let (port, broker) = broker(2).await;
    let mapping = self_managed(port, json!({ "maximum_retry_attempts": 1 }));
    let delivery = ScriptedDelivery::new(usize::MAX);
    let p = poller(
        lambda_state_with(&mapping),
        kafka_state(),
        secrets_state(),
        Some(delivery.clone()),
    );
    let task = tokio::spawn(p.consume(mapping_of(&mapping)));
    wait_until("the discard", || {
        broker
            .lock()
            .committed
            .get(&("esm-1".to_string(), "orders".to_string(), 0))
            == Some(&2)
    })
    .await;
    task.abort();
    // The first invocation and one retry.
    assert_eq!(delivery.count.load(Ordering::SeqCst), 2);
}
