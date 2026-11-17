//! Background poller that bridges Amazon MQ brokers -> Lambda event source
//! mappings.
//!
//! Every enabled mapping whose `EventSourceArn` is an Amazon MQ broker gets one
//! consumer on the broker's real engine container (the data plane
//! `fakecloud_mq` runs): STOMP 1.2 for ActiveMQ, AMQP 0-9-1 for RabbitMQ, on the
//! mapping's queue (`Queues`, one name) and, for RabbitMQ, the virtual host of
//! the `VIRTUAL_HOST` source access configuration (`/` when there is none). It
//! logs in with the `username` / `password` of the `BASIC_AUTH` Secrets Manager
//! secret, read through `GetSecretValue`'s path as the mapping's account.
//!
//! Messages are batched up to `BatchSize`, for at most
//! `MaximumBatchingWindowInSeconds` (500 ms by default, as on AWS) after the
//! first one, and below Lambda's 6 MB payload limit, then the function is
//! invoked synchronously with AWS's event shape (`aws:mq` `messages` /
//! `aws:rmq` `rmqMessagesByQueue`). A batch is acknowledged only once the
//! invocation succeeds. On a function error or failed invocation nothing of it
//! is acknowledged and the broker redelivers it (marked redelivered): RabbitMQ
//! deliveries are rejected with requeue, and the ActiveMQ session is dropped
//! (a STOMP `NACK` would poison the message into the DLQ), so the broker returns
//! its unacknowledged messages to the queue. Messages a `FilterCriteria`
//! rejects are acknowledged without invoking, as Lambda drops them.
//!
//! `LastProcessingResult` reports the outcome of the latest batch or connection
//! attempt. Unacknowledged messages stay on the broker, so after a restart the
//! poller resumes from whatever the (re-attached) broker still holds.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use base64::Engine;
use serde_json::{json, Map, Value};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use fakecloud_core::delivery::{KmsHook, LambdaDelivery};
use fakecloud_lambda::filter::FilterSet;
use fakecloud_lambda::{LambdaInvocation, SharedLambdaState};
use fakecloud_mq::client::amqp::{AmqpConnection, Delivery, FieldValue};
use fakecloud_mq::client::stomp::{StompConnection, StompFrame};
use fakecloud_mq::client::ClientError;
use fakecloud_mq::SharedMqState;
use fakecloud_secretsmanager::SharedSecretsManagerState;

/// `MaximumBatchingWindowInSeconds` when the mapping sets none.
const DEFAULT_BATCHING_WINDOW: Duration = Duration::from_millis(500);
/// Lambda's synchronous invocation payload limit.
const MAX_PAYLOAD_BYTES: usize = 6 * 1024 * 1024;
/// Room kept in the payload for the event around the messages (event source,
/// broker ARN, queue key).
const PAYLOAD_ENVELOPE_BYTES: usize = 4096;
/// Wait before reconnecting after a connection or configuration problem; it
/// doubles while the same problem repeats, up to [`MAX_RECONNECT_DELAY`].
const RECONNECT_DELAY: Duration = Duration::from_secs(2);
const MAX_RECONNECT_DELAY: Duration = Duration::from_secs(60);
/// Wait before the broker redelivers a batch the function failed.
const RETRY_DELAY: Duration = Duration::from_secs(1);
/// How often the set of mappings is reconciled with the running consumers.
const RECONCILE_INTERVAL: Duration = Duration::from_secs(1);
/// The STOMP subscription id of a mapping's consumer.
const SUBSCRIPTION_ID: &str = "fakecloud-esm";

const RESULT_OK: &str = "OK";
const RESULT_FUNCTION_FAILED: &str = "PROBLEM: Function call failed.";
const RESULT_CONNECTION_ERROR: &str =
    "PROBLEM: Connection error. Please check your event source connection configuration.";
/// A message that alone does not fit Lambda's 6 MB invocation payload is never
/// delivered; it stays on the broker (handed back unacknowledged) until the
/// broker's own policies move it.
const RESULT_MESSAGE_TOO_LARGE: &str =
    "PROBLEM: A message exceeds Lambda's 6 MB invocation payload limit and cannot be delivered.";

/// STOMP headers that are message fields or protocol framing, not JMS
/// properties.
const STOMP_STANDARD_HEADERS: &[&str] = &[
    "message-id",
    "destination",
    "timestamp",
    "expires",
    "priority",
    "persistent",
    "redelivered",
    "correlation-id",
    "reply-to",
    "type",
    "subscription",
    "ack",
    "content-length",
    "content-type",
    "transformation",
];

/// What a mapping's consumer needs; a change to any of it restarts the
/// consumer.
#[derive(Debug, Clone, PartialEq)]
struct MqMapping {
    uuid: String,
    function_arn: String,
    broker_arn: String,
    queue: String,
    virtual_host: String,
    secret_arn: Option<String>,
    batch_size: usize,
    window: Duration,
    filter_patterns: Vec<String>,
}

/// Where and how to reach a broker's engine.
#[derive(Debug, Clone, PartialEq)]
struct BrokerTarget {
    rabbit: bool,
    addr: String,
}

/// Why a consumer session ended.
#[derive(Debug)]
enum SessionEnd {
    /// The function failed a batch; the session was dropped so the broker
    /// redelivers it.
    Redeliver,
    /// A problem to report as the mapping's `LastProcessingResult`.
    Problem(String),
}

impl From<ClientError> for SessionEnd {
    fn from(e: ClientError) -> Self {
        SessionEnd::Problem(match e {
            ClientError::Auth(m) => format!(
                "PROBLEM: Authentication failed. Please check the BASIC_AUTH secret of your event source mapping. ({m})"
            ),
            ClientError::Broker(m) => format!("PROBLEM: Broker error: {m}"),
            ClientError::FrameTooLarge(_) => RESULT_MESSAGE_TOO_LARGE.to_string(),
            ClientError::Io(_) | ClientError::Protocol(_) | ClientError::Closed => {
                RESULT_CONNECTION_ERROR.to_string()
            }
        })
    }
}

struct Inner {
    lambda_state: SharedLambdaState,
    mq_state: SharedMqState,
    secrets_state: SharedSecretsManagerState,
    kms_hook: Option<Arc<dyn KmsHook>>,
    lambda_delivery: Option<Arc<dyn LambdaDelivery>>,
}

/// Amazon MQ -> Lambda event source mapping poller.
pub struct MqLambdaPoller {
    inner: Inner,
}

impl MqLambdaPoller {
    pub fn new(
        lambda_state: SharedLambdaState,
        mq_state: SharedMqState,
        secrets_state: SharedSecretsManagerState,
    ) -> Self {
        Self {
            inner: Inner {
                lambda_state,
                mq_state,
                secrets_state,
                kms_hook: None,
                lambda_delivery: None,
            },
        }
    }

    pub fn with_lambda_delivery(mut self, delivery: Arc<dyn LambdaDelivery>) -> Self {
        self.inner.lambda_delivery = Some(delivery);
        self
    }

    /// KMS hook that opens a `BASIC_AUTH` secret stored under a KMS key.
    pub fn with_kms_hook(mut self, hook: Arc<dyn KmsHook>) -> Self {
        self.inner.kms_hook = Some(hook);
        self
    }

    pub async fn run(self) {
        let inner = Arc::new(self.inner);
        let mut workers: HashMap<String, (MqMapping, JoinHandle<()>)> = HashMap::new();
        let mut tick = tokio::time::interval(RECONCILE_INTERVAL);
        loop {
            tick.tick().await;
            reconcile(&inner, &mut workers);
        }
    }
}

/// Start a consumer for every MQ mapping that has none, and stop the consumers
/// of mappings that are gone, disabled or changed.
fn reconcile(inner: &Arc<Inner>, workers: &mut HashMap<String, (MqMapping, JoinHandle<()>)>) {
    let wanted = mq_mappings(&inner.lambda_state);
    workers.retain(|_, (spec, handle)| {
        let keep = !handle.is_finished() && wanted.contains(spec);
        if !keep {
            handle.abort();
        }
        keep
    });
    for spec in wanted {
        if workers.contains_key(&spec.uuid) {
            continue;
        }
        let handle = tokio::spawn(consume(inner.clone(), spec.clone()));
        workers.insert(spec.uuid.clone(), (spec, handle));
    }
}

/// The enabled mappings reading an Amazon MQ broker.
fn mq_mappings(lambda_state: &SharedLambdaState) -> Vec<MqMapping> {
    let accounts = lambda_state.read();
    accounts
        .iter_regional()
        .flat_map(|(_, _, lambda)| lambda.event_source_mappings.values())
        .filter(|m| m.enabled && m.event_source_arn.split(':').nth(2) == Some("mq"))
        .filter_map(|m| {
            let queue = m.queues.first()?.clone();
            let access = |kind: &str| {
                m.source_access_configurations
                    .iter()
                    .find(|c| c["Type"].as_str() == Some(kind))
                    .and_then(|c| c["URI"].as_str())
                    .map(str::to_string)
            };
            let window = m
                .maximum_batching_window_in_seconds
                .map(|s| Duration::from_secs(s.max(0) as u64))
                .unwrap_or(DEFAULT_BATCHING_WINDOW);
            Some(MqMapping {
                uuid: m.uuid.clone(),
                function_arn: m.function_arn.clone(),
                broker_arn: m.event_source_arn.clone(),
                queue,
                virtual_host: access("VIRTUAL_HOST").unwrap_or_else(|| "/".to_string()),
                secret_arn: access("BASIC_AUTH"),
                batch_size: m.batch_size.max(1) as usize,
                window,
                filter_patterns: m.filter_patterns.clone(),
            })
        })
        .collect()
}

/// A mapping's consumer: connect, consume until the session ends, report why,
/// and reconnect.
async fn consume(inner: Arc<Inner>, spec: MqMapping) {
    let filter = FilterSet::from_strings(spec.filter_patterns.iter());
    let mut backoff = Backoff::default();
    loop {
        let end = session(&inner, &spec, &filter).await;
        // A session that delivered a batch ended a run of problems.
        if processing_result(&inner.lambda_state, &spec.uuid).as_deref() == Some(RESULT_OK) {
            backoff = Backoff::default();
        }
        let (result, delay) = match end {
            SessionEnd::Redeliver => (RESULT_FUNCTION_FAILED.to_string(), RETRY_DELAY),
            SessionEnd::Problem(p) => {
                let delay = backoff.next(&p);
                (p, delay)
            }
        };
        tracing::debug!(mapping = %spec.uuid, result = %result, "MQ->Lambda: consumer session ended");
        set_processing_result(&inner.lambda_state, &spec.uuid, &result);
        tokio::time::sleep(delay).await;
    }
}

/// The wait before reconnecting after a problem: [`RECONNECT_DELAY`],
/// doubling while the same problem keeps ending sessions (an undeliverable
/// message, a broker that stays down), up to [`MAX_RECONNECT_DELAY`].
#[derive(Default)]
struct Backoff {
    last: Option<String>,
    delay: Duration,
}

impl Backoff {
    fn next(&mut self, problem: &str) -> Duration {
        self.delay = if self.last.as_deref() == Some(problem) {
            (self.delay * 2).min(MAX_RECONNECT_DELAY)
        } else {
            RECONNECT_DELAY
        };
        self.last = Some(problem.to_string());
        self.delay
    }
}

/// Mapping `uuid`'s current `LastProcessingResult`.
fn processing_result(lambda_state: &SharedLambdaState, uuid: &str) -> Option<String> {
    let accounts = lambda_state.read();
    let result = accounts
        .iter_regional()
        .find_map(|(_, _, lambda)| lambda.event_source_mappings.get(uuid))
        .and_then(|m| m.last_processing_result.clone());
    result
}

async fn session(inner: &Inner, spec: &MqMapping, filter: &FilterSet) -> SessionEnd {
    let Some(target) = resolve_broker(&inner.mq_state, &spec.broker_arn) else {
        return SessionEnd::Problem(RESULT_CONNECTION_ERROR.to_string());
    };
    let (username, password) = match credentials(inner, spec) {
        Ok(c) => c,
        Err(problem) => return SessionEnd::Problem(problem),
    };
    if target.rabbit {
        rabbit_session(inner, spec, filter, &target.addr, &username, &password).await
    } else {
        activemq_session(inner, spec, filter, &target.addr, &username, &password).await
    }
}

/// The broker an MQ ARN (`arn:aws:mq:<region>:<account>:broker:<name>:<id>`)
/// names, when it is `RUNNING` with a live data plane.
fn resolve_broker(mq_state: &SharedMqState, broker_arn: &str) -> Option<BrokerTarget> {
    let account = broker_arn.split(':').nth(4)?;
    let id = broker_arn.rsplit(':').next()?;
    let guard = mq_state.read();
    let data = guard.get(account)?;
    let broker = data.brokers.get(id)?;
    if broker["brokerState"].as_str() != Some("RUNNING") {
        return None;
    }
    let rabbit = fakecloud_mq::shared::is_rabbit(broker["engineType"].as_str().unwrap_or(""));
    let plane = data.data_plane.get(id)?;
    let port = plane.ports.get(if rabbit { "amqp" } else { "stomp" })?;
    Some(BrokerTarget {
        rabbit,
        addr: format!("{}:{port}", plane.host),
    })
}

/// The `username` / `password` of the mapping's `BASIC_AUTH` secret.
fn credentials(inner: &Inner, spec: &MqMapping) -> Result<(String, String), String> {
    let secret = spec.secret_arn.as_deref().ok_or_else(|| {
        "PROBLEM: The event source mapping has no BASIC_AUTH source access configuration."
            .to_string()
    })?;
    let (account, region) = {
        let accounts = inner.lambda_state.read();
        let (account, region, _) = fakecloud_lambda::function_location(
            &spec.function_arn,
            accounts.default_account_id(),
            accounts.region(),
        );
        (account.to_string(), region.to_string())
    };
    let value = fakecloud_secretsmanager::value::read_secret_value(
        &inner.secrets_state,
        inner.kms_hook.as_deref(),
        &account,
        &region,
        secret,
        None,
        None,
        None,
        None,
    )
    .map_err(|e| {
        format!(
            "PROBLEM: Lambda could not read the BASIC_AUTH secret: {}: {}",
            e.code(),
            e.message()
        )
    })?;
    parse_basic_auth(value.secret_string.as_deref().unwrap_or(""))
}

fn parse_basic_auth(secret: &str) -> Result<(String, String), String> {
    let invalid = || {
        "PROBLEM: The BASIC_AUTH secret must be a JSON object with \"username\" and \"password\"."
            .to_string()
    };
    let v: Value = serde_json::from_str(secret).map_err(|_| invalid())?;
    let field = |k: &str| v.get(k).and_then(Value::as_str).map(str::to_string);
    Ok((
        field("username").ok_or_else(invalid)?,
        field("password").ok_or_else(invalid)?,
    ))
}

/// Record `result` as mapping `uuid`'s `LastProcessingResult`.
fn set_processing_result(lambda_state: &SharedLambdaState, uuid: &str, result: &str) {
    let mut accounts = lambda_state.write();
    for (_, _, lambda) in accounts.iter_regional_mut() {
        if let Some(mapping) = lambda.event_source_mappings.get_mut(uuid) {
            if mapping.last_processing_result.as_deref() != Some(result) {
                mapping.last_processing_result = Some(result.to_string());
            }
            return;
        }
    }
}

/// A broker consumer yielding event records, each with its acknowledgement
/// handle.
trait MessageSource {
    type Ack: Send;
    async fn next(&mut self) -> Result<(Value, Self::Ack), ClientError>;
}

struct StompSource<'a>(&'a mut StompConnection);

impl MessageSource for StompSource<'_> {
    type Ack = String;
    async fn next(&mut self) -> Result<(Value, String), ClientError> {
        let frame = self.0.next_message().await?;
        let ack = frame
            .header("ack")
            .or_else(|| frame.header("message-id"))
            .unwrap_or_default()
            .to_string();
        Ok((activemq_record(&frame, now_millis()), ack))
    }
}

struct AmqpSource<'a>(&'a mut AmqpConnection);

impl MessageSource for AmqpSource<'_> {
    type Ack = u64;
    async fn next(&mut self) -> Result<(Value, u64), ClientError> {
        let delivery = self.0.next_delivery().await?;
        Ok((rabbit_record(&delivery), delivery.delivery_tag))
    }
}

/// Collect the next batch: block for its first message (or start from the one
/// `carry` holds over), then take more until `batch_size`, until `window` has
/// passed since the first, or until the next one would push the payload over
/// Lambda's limit (it is then held over in `carry` for the next batch). A
/// message too large to fit the payload even alone is
/// [`ClientError::FrameTooLarge`]: the session ends without acknowledging it,
/// so the broker keeps it.
async fn collect_batch<S: MessageSource>(
    source: &mut S,
    carry: &mut Option<(Value, S::Ack)>,
    batch_size: usize,
    window: Duration,
) -> Result<Vec<(Value, S::Ack)>, ClientError> {
    let budget = MAX_PAYLOAD_BYTES - PAYLOAD_ENVELOPE_BYTES;
    let mut items = Vec::new();
    let mut size = 0;
    let mut deadline = None;
    if let Some(held) = carry.take() {
        let held_size = record_size(&held.0);
        if held_size > budget {
            return Err(ClientError::FrameTooLarge(held_size));
        }
        size += held_size;
        items.push(held);
        deadline = Some(Instant::now() + window);
    }
    while items.len() < batch_size {
        let next = match deadline {
            None => source.next().await?,
            Some(at) => match tokio::time::timeout_at(at, source.next()).await {
                Ok(next) => next?,
                Err(_) => break,
            },
        };
        let next_size = record_size(&next.0);
        if size + next_size > budget {
            if items.is_empty() {
                return Err(ClientError::FrameTooLarge(next_size));
            }
            *carry = Some(next);
            break;
        }
        size += next_size;
        items.push(next);
        if deadline.is_none() {
            deadline = Some(Instant::now() + window);
        }
    }
    Ok(items)
}

fn record_size(record: &Value) -> usize {
    serde_json::to_vec(record).map(|v| v.len() + 1).unwrap_or(0)
}

/// A batch split by the mapping's filter: records for the function and the
/// acknowledgement handles of everything (matched and dropped).
struct Split<A> {
    matched: Vec<Value>,
    matched_acks: Vec<A>,
    dropped_acks: Vec<A>,
}

fn split_batch<A>(batch: Vec<(Value, A)>, filter: &FilterSet) -> Split<A> {
    let mut split = Split {
        matched: Vec::new(),
        matched_acks: Vec::new(),
        dropped_acks: Vec::new(),
    };
    for (record, ack) in batch {
        if filter.is_empty() || filter.matches(&filter_target(&record)) {
            split.matched.push(record);
            split.matched_acks.push(ack);
        } else {
            split.dropped_acks.push(ack);
        }
    }
    split
}

/// The value a filter pattern is evaluated against: the message, with `data`
/// decoded when it holds JSON.
fn filter_target(record: &Value) -> Value {
    let mut target = record.clone();
    if let Some(decoded) = record["data"]
        .as_str()
        .and_then(|d| base64::engine::general_purpose::STANDARD.decode(d).ok())
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
    {
        target["data"] = decoded;
    }
    target
}

async fn activemq_session(
    inner: &Inner,
    spec: &MqMapping,
    filter: &FilterSet,
    addr: &str,
    username: &str,
    password: &str,
) -> SessionEnd {
    let mut conn = match StompConnection::connect(addr, username, password).await {
        Ok(conn) => conn,
        Err(e) => return e.into(),
    };
    let destination = format!("/queue/{}", spec.queue);
    let prefetch = spec.batch_size.to_string();
    if let Err(e) = conn
        .subscribe(
            SUBSCRIPTION_ID,
            &destination,
            "client-individual",
            &[("activemq.prefetchSize", prefetch.as_str())],
        )
        .await
    {
        return e.into();
    }
    let mut carry = None;
    loop {
        let batch = match collect_batch(
            &mut StompSource(&mut conn),
            &mut carry,
            spec.batch_size,
            spec.window,
        )
        .await
        {
            Ok(batch) => batch,
            Err(e) => return e.into(),
        };
        let split = split_batch(batch, filter);
        for ack in &split.dropped_acks {
            if let Err(e) = conn.ack(ack).await {
                return e.into();
            }
        }
        if split.matched.is_empty() {
            continue;
        }
        let payload = activemq_payload(&spec.broker_arn, split.matched);
        if !invoke(inner, spec, "aws:mq", payload).await {
            // Dropping the session hands every unacknowledged message back to
            // the queue, redelivered.
            return SessionEnd::Redeliver;
        }
        for ack in &split.matched_acks {
            if let Err(e) = conn.ack(ack).await {
                return e.into();
            }
        }
        set_processing_result(&inner.lambda_state, &spec.uuid, RESULT_OK);
    }
}

async fn rabbit_session(
    inner: &Inner,
    spec: &MqMapping,
    filter: &FilterSet,
    addr: &str,
    username: &str,
    password: &str,
) -> SessionEnd {
    let mut conn = match AmqpConnection::connect(addr, &spec.virtual_host, username, password).await
    {
        Ok(conn) => conn,
        Err(e) => return e.into(),
    };
    let prefetch = spec.batch_size.min(u16::MAX as usize) as u16;
    if let Err(e) = conn.qos(prefetch).await {
        return e.into();
    }
    if let Err(e) = conn.consume(&spec.queue).await {
        return e.into();
    }
    let mut carry = None;
    loop {
        let batch = match collect_batch(
            &mut AmqpSource(&mut conn),
            &mut carry,
            spec.batch_size,
            spec.window,
        )
        .await
        {
            Ok(batch) => batch,
            Err(e) => return e.into(),
        };
        let split = split_batch(batch, filter);
        for tag in &split.dropped_acks {
            if let Err(e) = conn.ack(*tag).await {
                return e.into();
            }
        }
        if split.matched.is_empty() {
            continue;
        }
        let payload = rabbit_payload(
            &spec.broker_arn,
            &spec.queue,
            &spec.virtual_host,
            split.matched,
        );
        let ok = invoke(inner, spec, "aws:rmq", payload).await;
        for tag in &split.matched_acks {
            let sent = if ok {
                conn.ack(*tag).await
            } else {
                conn.nack(*tag, true).await
            };
            if let Err(e) = sent {
                return e.into();
            }
        }
        let result = if ok {
            RESULT_OK
        } else {
            RESULT_FUNCTION_FAILED
        };
        set_processing_result(&inner.lambda_state, &spec.uuid, result);
        if !ok {
            tokio::time::sleep(RETRY_DELAY).await;
        }
    }
}

/// Invoke the mapping's function with `payload`; `true` when it succeeded.
async fn invoke(inner: &Inner, spec: &MqMapping, source: &str, payload: Value) -> bool {
    let payload = payload.to_string();
    let ok = match &inner.lambda_delivery {
        Some(delivery) => match delivery.invoke_lambda(&spec.function_arn, &payload).await {
            Ok(output) if output.is_function_error() => {
                tracing::warn!(
                    function_arn = %spec.function_arn,
                    error = %String::from_utf8_lossy(&output.payload),
                    "MQ->Lambda: function returned an error; the broker will redeliver the batch"
                );
                false
            }
            Ok(_) => true,
            Err(err) => {
                tracing::warn!(
                    function_arn = %spec.function_arn,
                    error = %err,
                    "MQ->Lambda: function invocation failed; the broker will redeliver the batch"
                );
                false
            }
        },
        None => true,
    };
    let mut accounts = inner.lambda_state.write();
    let default_account = accounts.default_account_id().to_string();
    let default_region = accounts.region().to_string();
    let (account, region, _) =
        fakecloud_lambda::function_location(&spec.function_arn, &default_account, &default_region);
    accounts
        .regional_mut(account, region)
        .invocations
        .push(LambdaInvocation {
            function_arn: spec.function_arn.clone(),
            payload,
            timestamp: chrono::Utc::now(),
            source: source.to_string(),
        });
    ok
}

fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn b64(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

/// An ActiveMQ message as Lambda's `aws:mq` event carries it. `now_ms` is when
/// the broker handed it out (`brokerOutTime`).
fn activemq_record(frame: &StompFrame, now_ms: i64) -> Value {
    let header = |k: &str| frame.header(k);
    let number = |k: &str| header(k).and_then(|v| v.trim().parse::<i64>().ok());
    let timestamp = number("timestamp").unwrap_or(now_ms);
    let destination = header("destination").map(|d| {
        d.strip_prefix("/queue/")
            .or_else(|| d.strip_prefix("/topic/"))
            .unwrap_or(d)
            .to_string()
    });
    let mut properties = Map::new();
    for (k, v) in &frame.headers {
        if STOMP_STANDARD_HEADERS.contains(&k.as_str()) || properties.contains_key(k) {
            continue;
        }
        properties.insert(k.clone(), Value::String(v.clone()));
    }
    json!({
        "messageID": header("message-id"),
        // ActiveMQ hands a bytes message to STOMP with its `content-length`.
        "messageType": if header("content-length").is_some() {
            "jms/bytes-message"
        } else {
            "jms/text-message"
        },
        "timestamp": timestamp,
        "deliveryMode": if header("persistent") == Some("true") { 2 } else { 1 },
        "correlationID": header("correlation-id"),
        "replyTo": header("reply-to"),
        "destination": { "physicalName": destination },
        "redelivered": header("redelivered") == Some("true"),
        "type": header("type"),
        "expiration": number("expires").unwrap_or(0),
        "priority": number("priority").unwrap_or(4),
        "data": b64(&frame.body),
        "brokerInTime": timestamp,
        "brokerOutTime": now_ms,
        "properties": properties,
    })
}

fn activemq_payload(broker_arn: &str, messages: Vec<Value>) -> Value {
    json!({
        "eventSource": "aws:mq",
        "eventSourceArn": broker_arn,
        "messages": messages,
    })
}

/// A RabbitMQ delivery as Lambda's `aws:rmq` event carries it.
fn rabbit_record(delivery: &Delivery) -> Value {
    let p = &delivery.properties;
    json!({
        "basicProperties": {
            "contentType": p.content_type,
            "contentEncoding": p.content_encoding,
            "headers": p.headers.as_ref().map(|h| table_json(h)),
            "deliveryMode": p.delivery_mode,
            "priority": p.priority,
            "correlationId": p.correlation_id,
            "replyTo": p.reply_to,
            "expiration": p.expiration,
            "messageId": p.message_id,
            "timestamp": p.timestamp.map(java_date),
            "type": p.kind,
            "userId": p.user_id,
            "appId": p.app_id,
            "clusterId": p.cluster_id,
            "bodySize": delivery.body.len(),
        },
        "redelivered": delivery.redelivered,
        "data": b64(&delivery.body),
    })
}

fn rabbit_payload(
    broker_arn: &str,
    queue: &str,
    virtual_host: &str,
    messages: Vec<Value>,
) -> Value {
    let mut by_queue = Map::new();
    by_queue.insert(format!("{queue}::{virtual_host}"), Value::Array(messages));
    json!({
        "eventSource": "aws:rmq",
        "eventSourceArn": broker_arn,
        "rmqMessagesByQueue": by_queue,
    })
}

/// A header table as Lambda renders it: AMQP long strings as `{"bytes": [..]}`
/// (signed bytes, as the Java client serializes them), numbers and booleans as
/// themselves, timestamps as dates.
fn table_json(table: &[(String, FieldValue)]) -> Value {
    let mut out = Map::new();
    for (k, v) in table {
        out.entry(k.clone()).or_insert_with(|| field_json(v));
    }
    Value::Object(out)
}

fn field_json(v: &FieldValue) -> Value {
    let float = |f: f64| serde_json::Number::from_f64(f).map_or(Value::Null, Value::Number);
    match v {
        FieldValue::Bool(b) => json!(b),
        FieldValue::I8(n) => json!(n),
        FieldValue::U8(n) => json!(n),
        FieldValue::I16(n) => json!(n),
        FieldValue::U16(n) => json!(n),
        FieldValue::I32(n) => json!(n),
        FieldValue::U32(n) => json!(n),
        FieldValue::I64(n) => json!(n),
        FieldValue::F32(n) => float(f64::from(*n)),
        FieldValue::F64(n) => float(*n),
        FieldValue::Decimal(scale, n) => float(f64::from(*n) / 10f64.powi(i32::from(*scale))),
        FieldValue::LongString(b) | FieldValue::Bytes(b) => {
            json!({ "bytes": b.iter().map(|x| *x as i8).collect::<Vec<i8>>() })
        }
        FieldValue::Array(items) => Value::Array(items.iter().map(field_json).collect()),
        FieldValue::Timestamp(t) => json!(java_date(*t)),
        FieldValue::Table(t) => table_json(t),
        FieldValue::Void => Value::Null,
    }
}

/// An AMQP timestamp (seconds since the epoch) the way Lambda prints it
/// (`Jan 1, 1970, 12:33:41 AM`, UTC).
fn java_date(secs: u64) -> String {
    chrono::DateTime::from_timestamp(secs.min(i64::MAX as u64) as i64, 0)
        .map(|d| d.format("%b %-d, %Y, %-I:%M:%S %p").to_string())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, VecDeque};
    use std::pin::Pin;
    use std::sync::Mutex;

    use fakecloud_core::delivery::LambdaInvocationOutput;
    use fakecloud_core::multi_account::{MultiAccountState, MultiRegionState};
    use fakecloud_lambda::{EventSourceMapping, LambdaState};
    use fakecloud_mq::client::amqp::BasicProperties;
    use parking_lot::RwLock;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const ACCOUNT: &str = "123456789012";
    const REGION: &str = "us-east-1";
    const BROKER_ID: &str = "b-1234";

    fn broker_arn() -> String {
        format!("arn:aws:mq:{REGION}:{ACCOUNT}:broker:orders:{BROKER_ID}")
    }

    fn frame(headers: &[(&str, &str)], body: &[u8]) -> StompFrame {
        StompFrame {
            command: "MESSAGE".to_string(),
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            body: body.to_vec(),
        }
    }

    #[test]
    fn activemq_record_has_the_aws_mq_message_shape() {
        let f = frame(
            &[
                ("message-id", "ID:broker-1:1:1:1:1"),
                ("destination", "/queue/orders"),
                ("timestamp", "1598827811958"),
                ("expires", "1598827871958"),
                ("priority", "6"),
                ("persistent", "true"),
                ("correlation-id", "myJMSCoID"),
                ("reply-to", "/queue/replies"),
                ("type", "order"),
                ("redelivered", "true"),
                ("subscription", "fakecloud-esm"),
                ("ack", "ID:ack-1"),
                ("myCustomProperty", "value"),
                ("myCustomProperty", "second"),
            ],
            b"ABC:AAAA",
        );
        let r = activemq_record(&f, 1598827811959);
        assert_eq!(r["messageID"], "ID:broker-1:1:1:1:1");
        assert_eq!(r["messageType"], "jms/text-message");
        assert_eq!(r["timestamp"], 1598827811958i64);
        assert_eq!(r["deliveryMode"], 2);
        assert_eq!(r["correlationID"], "myJMSCoID");
        assert_eq!(r["replyTo"], "/queue/replies");
        assert_eq!(r["destination"]["physicalName"], "orders");
        assert_eq!(r["redelivered"], true);
        assert_eq!(r["type"], "order");
        assert_eq!(r["expiration"], 1598827871958i64);
        assert_eq!(r["priority"], 6);
        assert_eq!(r["data"], "QUJDOkFBQUE=");
        assert_eq!(r["brokerInTime"], 1598827811958i64);
        assert_eq!(r["brokerOutTime"], 1598827811959i64);
        // Only non-standard headers are properties; a repeated one keeps its
        // first value.
        assert_eq!(r["properties"], json!({"myCustomProperty": "value"}));

        let payload = activemq_payload(&broker_arn(), vec![r]);
        assert_eq!(payload["eventSource"], "aws:mq");
        assert_eq!(payload["eventSourceArn"], broker_arn());
        assert_eq!(payload["messages"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn activemq_record_defaults_for_a_bare_bytes_message() {
        let f = frame(
            &[
                ("message-id", "ID:2"),
                ("destination", "/queue/orders"),
                ("content-length", "2"),
            ],
            b"\x00\x01",
        );
        let r = activemq_record(&f, 42);
        assert_eq!(r["messageType"], "jms/bytes-message");
        assert_eq!(r["deliveryMode"], 1);
        assert_eq!(r["redelivered"], false);
        assert_eq!(r["correlationID"], Value::Null);
        assert_eq!(r["replyTo"], Value::Null);
        assert_eq!(r["type"], Value::Null);
        assert_eq!(r["expiration"], 0);
        assert_eq!(r["priority"], 4);
        assert_eq!(r["timestamp"], 42);
        assert_eq!(r["data"], "AAE=");
        assert_eq!(r["properties"], json!({}));
    }

    #[test]
    fn rabbit_record_has_the_aws_rmq_message_shape() {
        let delivery = Delivery {
            delivery_tag: 7,
            redelivered: true,
            exchange: String::new(),
            routing_key: "pizzaQueue".to_string(),
            properties: BasicProperties {
                content_type: Some("text/plain".to_string()),
                headers: Some(vec![
                    (
                        "header1".to_string(),
                        FieldValue::LongString(b"value1".to_vec()),
                    ),
                    ("numberInHeader".to_string(), FieldValue::I32(10)),
                    ("flag".to_string(), FieldValue::Bool(true)),
                ]),
                delivery_mode: Some(1),
                priority: Some(34),
                expiration: Some("60000".to_string()),
                timestamp: Some(2021),
                user_id: Some("fcrabbit".to_string()),
                ..Default::default()
            },
            body: br#"{"timeout":0}"#.to_vec(),
        };
        let r = rabbit_record(&delivery);
        let p = &r["basicProperties"];
        assert_eq!(p["contentType"], "text/plain");
        assert_eq!(p["contentEncoding"], Value::Null);
        assert_eq!(
            p["headers"],
            json!({
                "header1": {"bytes": [118, 97, 108, 117, 101, 49]},
                "numberInHeader": 10,
                "flag": true,
            })
        );
        assert_eq!(p["deliveryMode"], 1);
        assert_eq!(p["priority"], 34);
        assert_eq!(p["correlationId"], Value::Null);
        assert_eq!(p["replyTo"], Value::Null);
        assert_eq!(p["expiration"], "60000");
        assert_eq!(p["messageId"], Value::Null);
        assert_eq!(p["timestamp"], "Jan 1, 1970, 12:33:41 AM");
        assert_eq!(p["type"], Value::Null);
        assert_eq!(p["userId"], "fcrabbit");
        assert_eq!(p["appId"], Value::Null);
        assert_eq!(p["clusterId"], Value::Null);
        assert_eq!(p["bodySize"], 13);
        assert_eq!(r["redelivered"], true);
        assert_eq!(r["data"], b64(br#"{"timeout":0}"#));

        let payload = rabbit_payload(&broker_arn(), "pizzaQueue", "/", vec![r]);
        assert_eq!(payload["eventSource"], "aws:rmq");
        assert_eq!(payload["eventSourceArn"], broker_arn());
        assert_eq!(
            payload["rmqMessagesByQueue"]["pizzaQueue::/"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn long_string_headers_render_as_signed_java_bytes() {
        assert_eq!(
            field_json(&FieldValue::LongString(vec![0xC3, 0xA9])),
            json!({"bytes": [-61, -87]})
        );
        assert_eq!(field_json(&FieldValue::Decimal(2, 1234)), json!(12.34));
        assert_eq!(field_json(&FieldValue::Void), Value::Null);
    }

    #[test]
    fn basic_auth_secret_needs_username_and_password() {
        assert_eq!(
            parse_basic_auth(r#"{"username":"u","password":"p"}"#).unwrap(),
            ("u".to_string(), "p".to_string())
        );
        assert!(parse_basic_auth(r#"{"username":"u"}"#).is_err());
        assert!(parse_basic_auth("not json").is_err());
    }

    #[test]
    fn filters_see_json_data_decoded() {
        let filter = FilterSet::from_strings([r#"{"data": {"kind": ["keep"]}}"#]);
        let keep = json!({"data": b64(br#"{"kind":"keep"}"#)});
        let drop = json!({"data": b64(br#"{"kind":"drop"}"#)});
        let split = split_batch(vec![(keep, 1u64), (drop, 2u64)], &filter);
        assert_eq!(split.matched.len(), 1);
        assert_eq!(split.matched_acks, vec![1]);
        assert_eq!(split.dropped_acks, vec![2]);
    }

    /// A source that yields its queued records, then waits forever.
    struct QueuedSource(VecDeque<(Value, u64)>);

    impl MessageSource for QueuedSource {
        type Ack = u64;
        async fn next(&mut self) -> Result<(Value, u64), ClientError> {
            match self.0.pop_front() {
                Some(next) => Ok(next),
                None => std::future::pending().await,
            }
        }
    }

    fn queued(n: u64, data_len: usize) -> QueuedSource {
        QueuedSource(
            (0..n)
                .map(|i| (json!({"data": "x".repeat(data_len)}), i))
                .collect(),
        )
    }

    #[tokio::test]
    async fn batches_fill_up_to_batch_size() {
        let mut source = queued(5, 1);
        let mut carry = None;
        let batch = collect_batch(&mut source, &mut carry, 3, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(batch.iter().map(|(_, a)| *a).collect::<Vec<_>>(), [0, 1, 2]);
    }

    #[tokio::test]
    async fn a_partial_batch_goes_out_when_the_window_closes() {
        let mut source = queued(2, 1);
        let mut carry = None;
        let started = Instant::now();
        let batch = collect_batch(&mut source, &mut carry, 10, Duration::from_millis(200))
            .await
            .unwrap();
        assert_eq!(batch.len(), 2);
        assert!(started.elapsed() >= Duration::from_millis(200));
    }

    #[tokio::test]
    async fn batches_stay_under_the_payload_limit() {
        let mut source = queued(3, MAX_PAYLOAD_BYTES / 2);
        let mut carry = None;
        let first = collect_batch(&mut source, &mut carry, 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(first.len(), 1, "a second half-limit record would overflow");
        assert_eq!(carry.as_ref().map(|(_, a)| *a), Some(1));
        let second = collect_batch(&mut source, &mut carry, 1, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(second.iter().map(|(_, a)| *a).collect::<Vec<_>>(), [1]);
    }

    #[tokio::test]
    async fn a_message_too_large_for_any_payload_is_refused() {
        let mut source = queued(1, MAX_PAYLOAD_BYTES);
        let mut carry = None;
        let err = collect_batch(&mut source, &mut carry, 10, Duration::from_secs(60))
            .await
            .unwrap_err();
        assert!(matches!(err, ClientError::FrameTooLarge(_)));
        match SessionEnd::from(err) {
            SessionEnd::Problem(p) => assert_eq!(p, RESULT_MESSAGE_TOO_LARGE),
            SessionEnd::Redeliver => panic!("expected a problem"),
        }
        // Held over behind a smaller message, it is refused once it leads.
        let mut source = QueuedSource(
            vec![
                (json!({"data": "small"}), 0u64),
                (json!({"data": "x".repeat(MAX_PAYLOAD_BYTES)}), 1u64),
            ]
            .into(),
        );
        let first = collect_batch(&mut source, &mut carry, 10, Duration::from_secs(60))
            .await
            .unwrap();
        assert_eq!(first.len(), 1);
        assert!(
            collect_batch(&mut source, &mut carry, 10, Duration::from_secs(60))
                .await
                .is_err()
        );
    }

    #[test]
    fn a_repeating_problem_backs_off_up_to_a_minute() {
        let mut backoff = Backoff::default();
        assert_eq!(backoff.next("a"), RECONNECT_DELAY);
        assert_eq!(backoff.next("a"), RECONNECT_DELAY * 2);
        assert_eq!(backoff.next("a"), RECONNECT_DELAY * 4);
        for _ in 0..10 {
            backoff.next("a");
        }
        assert_eq!(backoff.next("a"), MAX_RECONNECT_DELAY);
        // A different problem starts over.
        assert_eq!(backoff.next("b"), RECONNECT_DELAY);
    }

    #[tokio::test]
    async fn an_oversized_activemq_message_is_never_acked_or_invoked() {
        let state = lambda_state_with(mapping("esm-1"));
        let (inner, payloads) = inner_with(
            state.clone(),
            LambdaInvocationOutput::success(b"{}".to_vec()),
        );
        let s = spec(&state);
        let big = format!(
            "MESSAGE\nsubscription:fakecloud-esm\nmessage-id:ID:big\nack:big\ndestination:/queue/orders\n\n{}\0",
            "x".repeat(5 * 1024 * 1024)
        );
        let (addr, broker) = scripted_stomp_broker(vec![big]).await;
        let end = tokio::time::timeout(
            Duration::from_secs(10),
            activemq_session(&inner, &s, &FilterSet::default(), &addr, "u", "p"),
        )
        .await
        .expect("the oversized message ends the session");
        match end {
            SessionEnd::Problem(p) => assert_eq!(p, RESULT_MESSAGE_TOO_LARGE),
            SessionEnd::Redeliver => panic!("expected the size problem"),
        }
        let sent = broker.await.unwrap();
        assert!(payloads.lock().unwrap().is_empty());
        assert!(!sent.contains("ACK"), "nothing acked: {sent}");
    }

    struct AnsweringLambda {
        output: LambdaInvocationOutput,
        payloads: Arc<Mutex<Vec<Value>>>,
    }

    impl LambdaDelivery for AnsweringLambda {
        fn invoke_lambda(
            &self,
            _function_arn: &str,
            payload: &str,
        ) -> Pin<Box<dyn std::future::Future<Output = Result<LambdaInvocationOutput, String>> + Send>>
        {
            self.payloads
                .lock()
                .unwrap()
                .push(serde_json::from_str(payload).unwrap());
            let output = self.output.clone();
            Box::pin(async move { Ok(output) })
        }
    }

    fn mapping(uuid: &str) -> EventSourceMapping {
        EventSourceMapping {
            uuid: uuid.to_string(),
            function_arn: format!("arn:aws:lambda:{REGION}:{ACCOUNT}:function:mq-fn"),
            event_source_arn: broker_arn(),
            batch_size: 10,
            enabled: true,
            state: "Enabled".to_string(),
            last_modified: chrono::Utc::now(),
            filter_patterns: Vec::new(),
            maximum_batching_window_in_seconds: Some(0),
            starting_position: None,
            starting_position_timestamp: None,
            parallelization_factor: None,
            function_response_types: Vec::new(),
            kms_key_arn: None,
            metrics_config: None,
            destination_config: None,
            maximum_retry_attempts: None,
            maximum_record_age_in_seconds: None,
            bisect_batch_on_function_error: None,
            tumbling_window_in_seconds: None,
            topics: Vec::new(),
            queues: vec!["orders".to_string()],
            source_access_configurations: vec![json!({
                "Type": "BASIC_AUTH",
                "URI": format!("arn:aws:secretsmanager:{REGION}:{ACCOUNT}:secret:mq-creds"),
            })],
            self_managed_event_source: None,
            self_managed_kafka_event_source_config: None,
            amazon_managed_kafka_event_source_config: None,
            document_db_event_source_config: None,
            scaling_config: None,
            provisioned_poller_config: None,
            logging_config: None,
            tags: Default::default(),
            last_processing_result: None,
        }
    }

    fn lambda_state_with(m: EventSourceMapping) -> SharedLambdaState {
        let mut lambda: MultiRegionState<LambdaState> =
            MultiRegionState::new(ACCOUNT, REGION, "http://localhost:4566");
        lambda
            .regional_mut(ACCOUNT, REGION)
            .event_source_mappings
            .insert(m.uuid.clone(), m);
        Arc::new(RwLock::new(lambda))
    }

    fn inner_with(
        lambda_state: SharedLambdaState,
        output: LambdaInvocationOutput,
    ) -> (Inner, Arc<Mutex<Vec<Value>>>) {
        let payloads = Arc::new(Mutex::new(Vec::new()));
        let inner = Inner {
            lambda_state,
            mq_state: Arc::new(RwLock::new(MultiAccountState::new(
                ACCOUNT,
                REGION,
                "http://localhost:4566",
            ))),
            secrets_state: Arc::new(RwLock::new(MultiRegionState::new(
                ACCOUNT,
                REGION,
                "http://localhost:4566",
            ))),
            kms_hook: None,
            lambda_delivery: Some(Arc::new(AnsweringLambda {
                output,
                payloads: payloads.clone(),
            })),
        };
        (inner, payloads)
    }

    fn spec(lambda_state: &SharedLambdaState) -> MqMapping {
        mq_mappings(lambda_state).pop().expect("the MQ mapping")
    }

    #[test]
    fn mappings_take_queue_vhost_secret_and_defaults() {
        let mut m = mapping("esm-1");
        m.maximum_batching_window_in_seconds = None;
        m.batch_size = 100;
        m.source_access_configurations
            .push(json!({"Type": "VIRTUAL_HOST", "URI": "orders-vh"}));
        let state = lambda_state_with(m);
        let s = spec(&state);
        assert_eq!(s.queue, "orders");
        assert_eq!(s.virtual_host, "orders-vh");
        assert_eq!(
            s.secret_arn.as_deref(),
            Some("arn:aws:secretsmanager:us-east-1:123456789012:secret:mq-creds")
        );
        assert_eq!(s.batch_size, 100);
        assert_eq!(s.window, DEFAULT_BATCHING_WINDOW);

        // A disabled mapping, or one without a queue, gets no consumer.
        let mut disabled = mapping("esm-2");
        disabled.enabled = false;
        assert!(mq_mappings(&lambda_state_with(disabled)).is_empty());
        let mut no_queue = mapping("esm-3");
        no_queue.queues.clear();
        assert!(mq_mappings(&lambda_state_with(no_queue)).is_empty());
        let mut default_vhost = mapping("esm-4");
        default_vhost.source_access_configurations.clear();
        assert_eq!(spec(&lambda_state_with(default_vhost)).virtual_host, "/");
    }

    #[test]
    fn a_broker_without_a_running_data_plane_is_unreachable() {
        let (inner, _) = inner_with(
            lambda_state_with(mapping("esm-1")),
            LambdaInvocationOutput::success(b"{}".to_vec()),
        );
        assert_eq!(resolve_broker(&inner.mq_state, &broker_arn()), None);
        {
            let mut mq = inner.mq_state.write();
            let data = mq.get_or_create(ACCOUNT);
            data.brokers.insert(
                BROKER_ID.to_string(),
                json!({"brokerState": "CREATION_IN_PROGRESS", "engineType": "RABBITMQ"}),
            );
            let mut ports = BTreeMap::new();
            ports.insert("amqp".to_string(), 49152u16);
            data.data_plane.insert(
                BROKER_ID.to_string(),
                fakecloud_mq::BrokerDataPlane {
                    container_id: "c".to_string(),
                    host: "127.0.0.1".to_string(),
                    ports,
                },
            );
        }
        assert_eq!(resolve_broker(&inner.mq_state, &broker_arn()), None);
        inner
            .mq_state
            .write()
            .get_or_create(ACCOUNT)
            .brokers
            .get_mut(BROKER_ID)
            .unwrap()["brokerState"] = json!("RUNNING");
        assert_eq!(
            resolve_broker(&inner.mq_state, &broker_arn()),
            Some(BrokerTarget {
                rabbit: true,
                addr: "127.0.0.1:49152".to_string()
            })
        );
    }

    /// A scripted STOMP broker on loopback: accepts one session, delivers
    /// `messages`, and reports every frame the client sent after its
    /// SUBSCRIBE until the client goes away.
    async fn scripted_stomp_broker<M: Into<String>>(
        messages: Vec<M>,
    ) -> (String, tokio::task::JoinHandle<String>) {
        let messages: Vec<String> = messages.into_iter().map(Into::into).collect();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let handle = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut seen = Vec::new();
            let mut chunk = [0u8; 4096];
            // CONNECT
            while !seen.contains(&0u8) {
                let n = sock.read(&mut chunk).await.unwrap();
                seen.extend_from_slice(&chunk[..n]);
            }
            sock.write_all(b"CONNECTED\nversion:1.2\n\n\0")
                .await
                .unwrap();
            seen.clear();
            // SUBSCRIBE
            while !seen.contains(&0u8) {
                let n = sock.read(&mut chunk).await.unwrap();
                seen.extend_from_slice(&chunk[..n]);
            }
            assert!(String::from_utf8_lossy(&seen).contains("ack:client-individual"));
            seen.clear();
            for m in messages {
                sock.write_all(m.as_bytes()).await.unwrap();
            }
            loop {
                match sock.read(&mut chunk).await {
                    Ok(0) | Err(_) => break,
                    Ok(n) => seen.extend_from_slice(&chunk[..n]),
                }
            }
            String::from_utf8_lossy(&seen).into_owned()
        });
        (addr, handle)
    }

    const MESSAGE_1: &str = "MESSAGE\nsubscription:fakecloud-esm\nmessage-id:ID:m1\nack:a1\ndestination:/queue/orders\ntimestamp:1\n\nfirst\0";
    const MESSAGE_2: &str = "MESSAGE\nsubscription:fakecloud-esm\nmessage-id:ID:m2\nack:a2\ndestination:/queue/orders\ntimestamp:2\n\nsecond\0";

    #[tokio::test]
    async fn activemq_batch_is_acked_after_the_function_succeeds() {
        // The batch closes on size, not on the window: with a zero window a
        // slow reader can see the second message only after the first batch
        // has gone out.
        let mut m = mapping("esm-1");
        m.batch_size = 2;
        m.maximum_batching_window_in_seconds = Some(5);
        let state = lambda_state_with(m);
        let (inner, payloads) = inner_with(
            state.clone(),
            LambdaInvocationOutput::success(b"{}".to_vec()),
        );
        let s = spec(&state);
        let (addr, broker) = scripted_stomp_broker(vec![MESSAGE_1, MESSAGE_2]).await;
        let filter = FilterSet::default();
        let session = activemq_session(&inner, &s, &filter, &addr, "u", "p");
        // The session runs until the broker goes away; give it time to
        // deliver one batch.
        let _ = tokio::time::timeout(Duration::from_secs(2), session).await;
        let sent = broker.await.unwrap();
        let payloads = payloads.lock().unwrap();
        assert_eq!(payloads.len(), 1);
        let messages = payloads[0]["messages"].as_array().unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0]["data"], b64(b"first"));
        assert_eq!(messages[1]["messageID"], "ID:m2");
        assert!(sent.contains("ACK\nid:a1\n"), "first message acked: {sent}");
        assert!(
            sent.contains("ACK\nid:a2\n"),
            "second message acked: {sent}"
        );
        let accounts = state.read();
        let m = &accounts
            .regional(ACCOUNT, REGION)
            .unwrap()
            .event_source_mappings["esm-1"];
        assert_eq!(m.last_processing_result.as_deref(), Some(RESULT_OK));
    }

    #[tokio::test]
    async fn activemq_batch_the_function_fails_is_never_acked() {
        let state = lambda_state_with(mapping("esm-1"));
        let (inner, payloads) = inner_with(
            state.clone(),
            LambdaInvocationOutput::unhandled(br#"{"errorMessage":"boom"}"#.to_vec()),
        );
        let s = spec(&state);
        let (addr, broker) = scripted_stomp_broker(vec![MESSAGE_1]).await;
        let filter = FilterSet::default();
        let end = tokio::time::timeout(
            Duration::from_secs(5),
            activemq_session(&inner, &s, &filter, &addr, "u", "p"),
        )
        .await
        .expect("a failed batch ends the session");
        assert!(matches!(end, SessionEnd::Redeliver));
        let sent = broker.await.unwrap();
        assert_eq!(payloads.lock().unwrap().len(), 1);
        assert!(!sent.contains("ACK"), "nothing acked: {sent}");
        assert!(!sent.contains("NACK"), "no poison NACK: {sent}");
    }

    #[tokio::test]
    async fn activemq_messages_the_filter_drops_are_acked_without_invoking() {
        let mut m = mapping("esm-1");
        m.filter_patterns = vec![r#"{"data": {"kind": ["keep"]}}"#.to_string()];
        let state = lambda_state_with(m);
        let (inner, payloads) = inner_with(
            state.clone(),
            LambdaInvocationOutput::success(b"{}".to_vec()),
        );
        let s = spec(&state);
        let filter = FilterSet::from_strings(s.filter_patterns.iter());
        let (addr, broker) = scripted_stomp_broker(vec![MESSAGE_1]).await;
        let _ = tokio::time::timeout(
            Duration::from_secs(2),
            activemq_session(&inner, &s, &filter, &addr, "u", "p"),
        )
        .await;
        let sent = broker.await.unwrap();
        assert!(payloads.lock().unwrap().is_empty());
        assert!(
            sent.contains("ACK\nid:a1\n"),
            "dropped message acked: {sent}"
        );
    }

    #[tokio::test]
    async fn a_missing_broker_is_reported_as_a_connection_error() {
        let state = lambda_state_with(mapping("esm-1"));
        let (inner, _) = inner_with(
            state.clone(),
            LambdaInvocationOutput::success(b"{}".to_vec()),
        );
        let s = spec(&state);
        let end = session(&inner, &s, &FilterSet::default()).await;
        match end {
            SessionEnd::Problem(p) => assert_eq!(p, RESULT_CONNECTION_ERROR),
            SessionEnd::Redeliver => panic!("expected a connection problem"),
        }
    }
}
