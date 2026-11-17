//! A consumer-side Kafka protocol client: what an event source mapping
//! needs to read a topic as a member of a consumer group, over plain TCP with
//! optional SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512).
//!
//! It speaks fixed, non-flexible API versions every broker from Kafka 1.0 to
//! 4.x serves: Metadata v4, ListOffsets v1, Fetch v4, FindCoordinator v1,
//! OffsetFetch v1, OffsetCommit v2, SaslHandshake v1 and SaslAuthenticate v0.
//! Offsets are committed the way a standalone (manually assigned) consumer
//! commits them -- generation `-1`, no member id -- so the group's committed
//! offsets live on the broker, visible to `kafka-consumer-groups.sh`, and
//! survive the client.

pub mod records;
pub mod scram;
pub mod wire;

use std::collections::HashMap;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

pub use records::{ConsumedRecord, TimestampType};
pub use scram::ScramAlgorithm;
use wire::{Decoder, Encoder, WireError};

const API_FETCH: i16 = 1;
const API_LIST_OFFSETS: i16 = 2;
const API_METADATA: i16 = 3;
const API_OFFSET_COMMIT: i16 = 8;
const API_OFFSET_FETCH: i16 = 9;
const API_FIND_COORDINATOR: i16 = 10;
const API_SASL_HANDSHAKE: i16 = 17;
const API_SASL_AUTHENTICATE: i16 = 36;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// Bounds every request, a Fetch's long poll included.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RESPONSE_BYTES: usize = 128 * 1024 * 1024;

/// `ListOffsets` timestamp for the offset after the last record.
pub const OFFSET_LATEST: i64 = -1;
/// `ListOffsets` timestamp for the first retained record.
pub const OFFSET_EARLIEST: i64 = -2;

pub const ERROR_OFFSET_OUT_OF_RANGE: i16 = 1;
pub const ERROR_UNKNOWN_TOPIC_OR_PARTITION: i16 = 3;
pub const ERROR_LEADER_NOT_AVAILABLE: i16 = 5;
pub const ERROR_NOT_LEADER_OR_FOLLOWER: i16 = 6;
pub const ERROR_COORDINATOR_LOAD_IN_PROGRESS: i16 = 14;
pub const ERROR_COORDINATOR_NOT_AVAILABLE: i16 = 15;
pub const ERROR_NOT_COORDINATOR: i16 = 16;
pub const ERROR_TOPIC_AUTHORIZATION_FAILED: i16 = 29;
pub const ERROR_GROUP_AUTHORIZATION_FAILED: i16 = 30;

/// The name of a Kafka protocol error code.
pub fn error_name(code: i16) -> &'static str {
    match code {
        -1 => "UNKNOWN_SERVER_ERROR",
        1 => "OFFSET_OUT_OF_RANGE",
        2 => "CORRUPT_MESSAGE",
        3 => "UNKNOWN_TOPIC_OR_PARTITION",
        5 => "LEADER_NOT_AVAILABLE",
        6 => "NOT_LEADER_OR_FOLLOWER",
        7 => "REQUEST_TIMED_OUT",
        14 => "COORDINATOR_LOAD_IN_PROGRESS",
        15 => "COORDINATOR_NOT_AVAILABLE",
        16 => "NOT_COORDINATOR",
        22 => "ILLEGAL_GENERATION",
        25 => "UNKNOWN_MEMBER_ID",
        27 => "REBALANCE_IN_PROGRESS",
        29 => "TOPIC_AUTHORIZATION_FAILED",
        30 => "GROUP_AUTHORIZATION_FAILED",
        31 => "CLUSTER_AUTHORIZATION_FAILED",
        33 => "UNSUPPORTED_SASL_MECHANISM",
        34 => "ILLEGAL_SASL_STATE",
        35 => "UNSUPPORTED_VERSION",
        58 => "SASL_AUTHENTICATION_FAILED",
        _ => "UNKNOWN_ERROR_CODE",
    }
}

/// How the client authenticates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SaslMechanism {
    Plain,
    Scram(ScramAlgorithm),
}

impl SaslMechanism {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Plain => "PLAIN",
            Self::Scram(alg) => alg.mechanism(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SaslCredentials {
    pub mechanism: SaslMechanism,
    pub username: String,
    pub password: String,
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    /// The broker could not be reached, or the connection broke.
    #[error("broker {addr}: {reason}")]
    Connection { addr: String, reason: String },
    #[error("SASL authentication failed: {0}")]
    Authentication(String),
    #[error(transparent)]
    Wire(#[from] WireError),
    /// The broker answered with an error code.
    #[error("{} (Kafka error code {code}){context}", error_name(*.code))]
    Broker { code: i16, context: String },
}

impl ClientError {
    /// The Kafka error code of a broker error.
    pub fn broker_code(&self) -> Option<i16> {
        match self {
            Self::Broker { code, .. } => Some(*code),
            _ => None,
        }
    }

    fn broker(code: i16, context: impl Into<String>) -> Self {
        let context = context.into();
        Self::Broker {
            code,
            context: if context.is_empty() {
                context
            } else {
                format!(": {context}")
            },
        }
    }
}

pub type ClientResult<T> = Result<T, ClientError>;

/// A partition's placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMetadata {
    pub partition: i32,
    pub leader: i32,
    pub error_code: i16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMetadata {
    pub name: String,
    pub error_code: i16,
    pub partitions: Vec<PartitionMetadata>,
}

/// One partition of a Fetch response.
#[derive(Debug, Clone)]
pub struct FetchedPartition {
    pub topic: String,
    pub partition: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    pub records: Vec<ConsumedRecord>,
    /// The offset after the last complete batch returned.
    pub next_offset: Option<i64>,
}

/// `host:port`, bracketing an IPv6 host.
pub fn broker_address(host: &str, port: i32) -> String {
    if host.contains(':') && !host.starts_with('[') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

#[derive(Debug)]
struct Connection {
    stream: TcpStream,
    correlation: i32,
}

impl Connection {
    async fn open(
        addr: &str,
        client_id: &str,
        sasl: Option<&SaslCredentials>,
    ) -> ClientResult<Self> {
        let stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr))
            .await
            .map_err(|_| ClientError::Connection {
                addr: addr.to_string(),
                reason: "connection timed out".to_string(),
            })?
            .map_err(|e| ClientError::Connection {
                addr: addr.to_string(),
                reason: e.to_string(),
            })?;
        let _ = stream.set_nodelay(true);
        let mut conn = Self {
            stream,
            correlation: 0,
        };
        if let Some(creds) = sasl {
            conn.authenticate(addr, client_id, creds).await?;
        }
        Ok(conn)
    }

    async fn authenticate(
        &mut self,
        addr: &str,
        client_id: &str,
        creds: &SaslCredentials,
    ) -> ClientResult<()> {
        let mechanism = creds.mechanism.name();
        let mut body = Encoder::new();
        body.string(mechanism);
        let resp = self
            .send(addr, client_id, API_SASL_HANDSHAKE, 1, &body.into_bytes())
            .await?;
        let mut d = Decoder::new(&resp);
        let code = d.i16()?;
        let enabled = d.array(|d| d.string())?;
        if code != 0 {
            return Err(ClientError::Authentication(format!(
                "the broker does not enable {mechanism} (enabled mechanisms: {})",
                if enabled.is_empty() {
                    "none".to_string()
                } else {
                    enabled.join(", ")
                }
            )));
        }
        match &creds.mechanism {
            SaslMechanism::Plain => {
                let mut token = vec![0u8];
                token.extend_from_slice(creds.username.as_bytes());
                token.push(0);
                token.extend_from_slice(creds.password.as_bytes());
                self.sasl_step(addr, client_id, &token).await?;
            }
            SaslMechanism::Scram(alg) => {
                let nonce = uuid::Uuid::new_v4().simple().to_string();
                let mut scram =
                    scram::ScramClient::new(*alg, &creds.username, &creds.password, nonce);
                let server_first = self
                    .sasl_step(addr, client_id, scram.client_first().as_bytes())
                    .await?;
                let client_final = scram
                    .client_final(&String::from_utf8_lossy(&server_first))
                    .map_err(ClientError::Authentication)?;
                let server_final = self
                    .sasl_step(addr, client_id, client_final.as_bytes())
                    .await?;
                scram
                    .verify_server_final(&String::from_utf8_lossy(&server_final))
                    .map_err(ClientError::Authentication)?;
            }
        }
        Ok(())
    }

    /// One `SaslAuthenticate` round trip: the broker's answer.
    async fn sasl_step(
        &mut self,
        addr: &str,
        client_id: &str,
        token: &[u8],
    ) -> ClientResult<Vec<u8>> {
        let mut body = Encoder::new();
        body.bytes(token);
        let resp = self
            .send(
                addr,
                client_id,
                API_SASL_AUTHENTICATE,
                0,
                &body.into_bytes(),
            )
            .await?;
        let mut d = Decoder::new(&resp);
        let code = d.i16()?;
        let message = d.nullable_string()?;
        let answer = d.nullable_bytes()?.unwrap_or_default().to_vec();
        if code != 0 {
            return Err(ClientError::Authentication(message.unwrap_or_else(|| {
                format!("{} (Kafka error code {code})", error_name(code))
            })));
        }
        Ok(answer)
    }

    /// Send a request and read its response body (after the correlation id).
    async fn send(
        &mut self,
        addr: &str,
        client_id: &str,
        api_key: i16,
        api_version: i16,
        body: &[u8],
    ) -> ClientResult<Vec<u8>> {
        self.correlation = self.correlation.wrapping_add(1);
        let correlation = self.correlation;
        let mut header = Encoder::new();
        header
            .i16(api_key)
            .i16(api_version)
            .i32(correlation)
            .string(client_id);
        let header = header.into_bytes();
        let mut frame = Encoder::new();
        frame
            .i32((header.len() + body.len()) as i32)
            .raw(&header)
            .raw(body);
        let frame = frame.into_bytes();
        let io_err = |reason: String| ClientError::Connection {
            addr: addr.to_string(),
            reason,
        };
        let exchange = async {
            self.stream.write_all(&frame).await?;
            let size = self.stream.read_i32().await?;
            if size < 4 || size as usize > MAX_RESPONSE_BYTES {
                return Err(std::io::Error::other(format!(
                    "invalid response size {size}"
                )));
            }
            let mut buf = vec![0u8; size as usize];
            self.stream.read_exact(&mut buf).await?;
            Ok(buf)
        };
        let buf = tokio::time::timeout(REQUEST_TIMEOUT, exchange)
            .await
            .map_err(|_| io_err("request timed out".to_string()))?
            .map_err(|e| io_err(e.to_string()))?;
        let got = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
        if got != correlation {
            return Err(io_err(format!(
                "response correlation id {got} does not match request {correlation}"
            )));
        }
        Ok(buf[4..].to_vec())
    }
}

/// Group `(topic, partition, value)` triples by topic, keeping first-seen
/// order.
fn by_topic<T: Copy>(items: &[(String, i32, T)]) -> Vec<(String, Vec<(i32, T)>)> {
    let mut out: Vec<(String, Vec<(i32, T)>)> = Vec::new();
    for (topic, partition, value) in items {
        match out.iter_mut().find(|(t, _)| t == topic) {
            Some((_, parts)) => parts.push((*partition, *value)),
            None => out.push((topic.clone(), vec![(*partition, *value)])),
        }
    }
    out
}

/// A connection pool to one cluster's brokers.
#[derive(Debug)]
pub struct KafkaClient {
    bootstrap: Vec<String>,
    client_id: String,
    sasl: Option<SaslCredentials>,
    /// Broker node id -> `host:port`, from the last Metadata response.
    brokers: HashMap<i32, String>,
    conns: HashMap<String, Connection>,
}

impl KafkaClient {
    pub fn new(
        bootstrap: Vec<String>,
        client_id: impl Into<String>,
        sasl: Option<SaslCredentials>,
    ) -> Self {
        Self {
            bootstrap,
            client_id: client_id.into(),
            sasl,
            brokers: HashMap::new(),
            conns: HashMap::new(),
        }
    }

    async fn call(
        &mut self,
        addr: &str,
        api_key: i16,
        api_version: i16,
        body: Vec<u8>,
    ) -> ClientResult<Vec<u8>> {
        if !self.conns.contains_key(addr) {
            let conn = Connection::open(addr, &self.client_id, self.sasl.as_ref()).await?;
            self.conns.insert(addr.to_string(), conn);
        }
        let conn = self.conns.get_mut(addr).expect("connection just opened");
        let result = conn
            .send(addr, &self.client_id, api_key, api_version, &body)
            .await;
        if result.is_err() {
            self.conns.remove(addr);
        }
        result
    }

    /// Bootstrap servers, then every broker the cluster reported.
    fn any_broker_candidates(&self) -> Vec<String> {
        let mut out = self.bootstrap.clone();
        for addr in self.brokers.values() {
            if !out.contains(addr) {
                out.push(addr.clone());
            }
        }
        out
    }

    /// Send to the first broker that answers. An authentication failure is
    /// final: another broker of the cluster refuses the same credentials.
    async fn call_any(
        &mut self,
        api_key: i16,
        api_version: i16,
        body: Vec<u8>,
    ) -> ClientResult<Vec<u8>> {
        let mut last = None;
        for addr in self.any_broker_candidates() {
            match self.call(&addr, api_key, api_version, body.clone()).await {
                Ok(resp) => return Ok(resp),
                Err(e @ ClientError::Authentication(_)) => return Err(e),
                Err(e) => last = Some(e),
            }
        }
        Err(last.unwrap_or_else(|| ClientError::Connection {
            addr: String::new(),
            reason: "no bootstrap servers".to_string(),
        }))
    }

    fn broker_addr(&self, node: i32) -> ClientResult<String> {
        self.brokers
            .get(&node)
            .cloned()
            .ok_or_else(|| ClientError::broker(ERROR_LEADER_NOT_AVAILABLE, format!("node {node}")))
    }

    /// Topic placement for `topics`, refreshing the known brokers.
    pub async fn metadata(&mut self, topics: &[String]) -> ClientResult<Vec<TopicMetadata>> {
        let mut body = Encoder::new();
        body.array(topics, |e, t| {
            e.string(t);
        })
        .bool(false);
        let resp = self.call_any(API_METADATA, 4, body.into_bytes()).await?;
        let mut d = Decoder::new(&resp);
        let _throttle = d.i32()?;
        let brokers = d.array(|d| {
            let node = d.i32()?;
            let host = d.string()?;
            let port = d.i32()?;
            let _rack = d.nullable_string()?;
            Ok((node, broker_address(&host, port)))
        })?;
        let _cluster_id = d.nullable_string()?;
        let _controller = d.i32()?;
        let topics = d.array(|d| {
            let error_code = d.i16()?;
            let name = d.string()?;
            let _internal = d.bool()?;
            let partitions = d.array(|d| {
                let error_code = d.i16()?;
                let partition = d.i32()?;
                let leader = d.i32()?;
                let _replicas = d.array(|d| d.i32())?;
                let _isr = d.array(|d| d.i32())?;
                Ok(PartitionMetadata {
                    partition,
                    leader,
                    error_code,
                })
            })?;
            Ok(TopicMetadata {
                name,
                error_code,
                partitions,
            })
        })?;
        if !brokers.is_empty() {
            self.brokers = brokers.into_iter().collect();
        }
        Ok(topics)
    }

    /// The offset of a partition at `timestamp` (milliseconds, or
    /// [`OFFSET_LATEST`] / [`OFFSET_EARLIEST`]): for a time, the first record
    /// at or after it, `None` when no record is that recent.
    pub async fn list_offset(
        &mut self,
        leader: i32,
        topic: &str,
        partition: i32,
        timestamp: i64,
    ) -> ClientResult<Option<i64>> {
        let addr = self.broker_addr(leader)?;
        let mut body = Encoder::new();
        body.i32(-1)
            .i32(1)
            .string(topic)
            .i32(1)
            .i32(partition)
            .i64(timestamp);
        let resp = self
            .call(&addr, API_LIST_OFFSETS, 1, body.into_bytes())
            .await?;
        let mut d = Decoder::new(&resp);
        let topics = d.array(|d| {
            let name = d.string()?;
            let parts = d.array(|d| {
                let index = d.i32()?;
                let code = d.i16()?;
                let _ts = d.i64()?;
                let offset = d.i64()?;
                Ok((index, code, offset))
            })?;
            Ok((name, parts))
        })?;
        let (_, code, offset) = topics
            .into_iter()
            .filter(|(name, _)| name == topic)
            .flat_map(|(_, parts)| parts)
            .find(|(index, _, _)| *index == partition)
            .ok_or_else(|| WireError(format!("ListOffsets response has no {topic}-{partition}")))?;
        if code != 0 {
            return Err(ClientError::broker(code, format!("{topic}-{partition}")));
        }
        Ok((offset >= 0).then_some(offset))
    }

    /// Fetch `(topic, partition, offset)` from their leader `leader`.
    pub async fn fetch(
        &mut self,
        leader: i32,
        partitions: &[(String, i32, i64)],
        max_wait_ms: i32,
        max_bytes: i32,
        partition_max_bytes: i32,
    ) -> ClientResult<Vec<FetchedPartition>> {
        let addr = self.broker_addr(leader)?;
        let grouped = by_topic(partitions);
        let mut body = Encoder::new();
        body.i32(-1)
            .i32(max_wait_ms)
            .i32(1)
            .i32(max_bytes)
            // READ_UNCOMMITTED, the consumer default.
            .i8(0)
            .array(&grouped, |e, (topic, parts)| {
                e.string(topic).array(parts, |e, (p, offset)| {
                    e.i32(*p).i64(*offset).i32(partition_max_bytes);
                });
            });
        let resp = self.call(&addr, API_FETCH, 4, body.into_bytes()).await?;
        let mut d = Decoder::new(&resp);
        let _throttle = d.i32()?;
        let responses = d.array(|d| {
            let topic = d.string()?;
            let parts = d.array(|d| {
                let partition = d.i32()?;
                let error_code = d.i16()?;
                let high_watermark = d.i64()?;
                let _last_stable = d.i64()?;
                let _aborted = d.array(|d| Ok((d.i64()?, d.i64()?)))?;
                let records = d.nullable_bytes()?.unwrap_or_default();
                Ok((partition, error_code, high_watermark, records))
            })?;
            Ok((topic, parts))
        })?;
        let mut out = Vec::new();
        for (topic, parts) in responses {
            for (partition, error_code, high_watermark, data) in parts {
                let from = partitions
                    .iter()
                    .find(|(t, p, _)| *t == topic && *p == partition)
                    .map(|(_, _, o)| *o)
                    .unwrap_or_default();
                let (records, next_offset) = if error_code == 0 {
                    records::decode_record_set(data, from)?
                } else {
                    (Vec::new(), None)
                };
                out.push(FetchedPartition {
                    topic: topic.clone(),
                    partition,
                    error_code,
                    high_watermark,
                    records,
                    next_offset,
                });
            }
        }
        Ok(out)
    }

    /// The `host:port` of a consumer group's coordinator.
    pub async fn find_coordinator(&mut self, group: &str) -> ClientResult<String> {
        let mut body = Encoder::new();
        body.string(group).i8(0);
        let resp = self
            .call_any(API_FIND_COORDINATOR, 1, body.into_bytes())
            .await?;
        let mut d = Decoder::new(&resp);
        let _throttle = d.i32()?;
        let code = d.i16()?;
        let message = d.nullable_string()?;
        let _node = d.i32()?;
        let host = d.string()?;
        let port = d.i32()?;
        if code != 0 {
            return Err(ClientError::broker(code, message.unwrap_or_default()));
        }
        Ok(broker_address(&host, port))
    }

    /// The group's committed offsets for `partitions`: `None` where it has
    /// none.
    pub async fn offset_fetch(
        &mut self,
        coordinator: &str,
        group: &str,
        partitions: &[(String, i32)],
    ) -> ClientResult<HashMap<(String, i32), Option<i64>>> {
        let triples: Vec<(String, i32, ())> = partitions
            .iter()
            .map(|(t, p)| (t.clone(), *p, ()))
            .collect();
        let grouped = by_topic(&triples);
        let mut body = Encoder::new();
        body.string(group).array(&grouped, |e, (topic, parts)| {
            e.string(topic).array(parts, |e, (p, ())| {
                e.i32(*p);
            });
        });
        let resp = self
            .call(coordinator, API_OFFSET_FETCH, 1, body.into_bytes())
            .await?;
        let mut d = Decoder::new(&resp);
        let topics = d.array(|d| {
            let name = d.string()?;
            let parts = d.array(|d| {
                let index = d.i32()?;
                let offset = d.i64()?;
                let _metadata = d.nullable_string()?;
                let code = d.i16()?;
                Ok((index, offset, code))
            })?;
            Ok((name, parts))
        })?;
        let mut out = HashMap::new();
        for (name, parts) in topics {
            for (index, offset, code) in parts {
                if code != 0 {
                    return Err(ClientError::broker(code, format!("group {group}")));
                }
                out.insert((name.clone(), index), (offset >= 0).then_some(offset));
            }
        }
        Ok(out)
    }

    /// Commit `(topic, partition, next offset)` for the group.
    pub async fn offset_commit(
        &mut self,
        coordinator: &str,
        group: &str,
        offsets: &[(String, i32, i64)],
    ) -> ClientResult<()> {
        let grouped = by_topic(offsets);
        let mut body = Encoder::new();
        body.string(group)
            .i32(-1)
            .string("")
            .i64(-1)
            .array(&grouped, |e, (topic, parts)| {
                e.string(topic).array(parts, |e, (p, offset)| {
                    e.i32(*p).i64(*offset).nullable_string(Some(""));
                });
            });
        let resp = self
            .call(coordinator, API_OFFSET_COMMIT, 2, body.into_bytes())
            .await?;
        let mut d = Decoder::new(&resp);
        let topics = d.array(|d| {
            let name = d.string()?;
            let parts = d.array(|d| Ok((d.i32()?, d.i16()?)))?;
            Ok((name, parts))
        })?;
        for (name, parts) in topics {
            if let Some((index, code)) = parts.into_iter().find(|(_, code)| *code != 0) {
                return Err(ClientError::broker(
                    code,
                    format!("committing {name}-{index} for group {group}"),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    #[test]
    fn broker_addresses_bracket_ipv6() {
        assert_eq!(broker_address("127.0.0.1", 9092), "127.0.0.1:9092");
        assert_eq!(broker_address("::1", 9092), "[::1]:9092");
    }

    #[test]
    fn requests_group_partitions_by_topic() {
        let grouped = by_topic(&[
            ("a".to_string(), 0, 1i64),
            ("b".to_string(), 0, 2),
            ("a".to_string(), 1, 3),
        ]);
        assert_eq!(
            grouped,
            vec![
                ("a".to_string(), vec![(0, 1), (1, 3)]),
                ("b".to_string(), vec![(0, 2)])
            ]
        );
    }

    /// Read one framed request: `(api key, version, correlation, body)`.
    async fn read_request(sock: &mut TcpStream) -> (i16, i16, i32, Vec<u8>) {
        let size = sock.read_i32().await.unwrap();
        let mut buf = vec![0u8; size as usize];
        sock.read_exact(&mut buf).await.unwrap();
        let mut d = Decoder::new(&buf);
        let key = d.i16().unwrap();
        let version = d.i16().unwrap();
        let correlation = d.i32().unwrap();
        let _client = d.nullable_string().unwrap();
        (key, version, correlation, d.rest().to_vec())
    }

    async fn respond(sock: &mut TcpStream, correlation: i32, body: Vec<u8>) {
        let mut e = Encoder::new();
        e.i32(body.len() as i32 + 4).i32(correlation).raw(&body);
        sock.write_all(&e.into_bytes()).await.unwrap();
    }

    /// The client's requests decode as the broker reads them, and its
    /// responses are read back: Metadata, then a group commit and fetch.
    #[tokio::test]
    async fn speaks_metadata_offset_commit_and_offset_fetch() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let broker = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            // Metadata v4.
            let (key, version, corr, body) = read_request(&mut sock).await;
            assert_eq!((key, version), (API_METADATA, 4));
            let mut d = Decoder::new(&body);
            assert_eq!(d.array(|d| d.string()).unwrap(), vec!["orders".to_string()]);
            assert!(!d.bool().unwrap());
            let mut e = Encoder::new();
            e.i32(0)
                .i32(1)
                .i32(7)
                .string("127.0.0.1")
                .i32(i32::from(port))
                .nullable_string(None)
                .nullable_string(Some("cluster"))
                .i32(7)
                .i32(1)
                .i16(0)
                .string("orders")
                .bool(false)
                .i32(2);
            for p in 0..2 {
                e.i16(0).i32(p).i32(7).i32(1).i32(7).i32(1).i32(7);
            }
            respond(&mut sock, corr, e.into_bytes()).await;
            // OffsetCommit v2.
            let (key, version, corr, body) = read_request(&mut sock).await;
            assert_eq!((key, version), (API_OFFSET_COMMIT, 2));
            let mut d = Decoder::new(&body);
            assert_eq!(d.string().unwrap(), "group-1");
            assert_eq!(d.i32().unwrap(), -1);
            assert_eq!(d.string().unwrap(), "");
            assert_eq!(d.i64().unwrap(), -1);
            assert_eq!(d.i32().unwrap(), 1);
            assert_eq!(d.string().unwrap(), "orders");
            assert_eq!(d.i32().unwrap(), 1);
            assert_eq!((d.i32().unwrap(), d.i64().unwrap()), (1, 42));
            let mut e = Encoder::new();
            e.i32(1).string("orders").i32(1).i32(1).i16(0);
            respond(&mut sock, corr, e.into_bytes()).await;
            // OffsetFetch v1.
            let (key, version, corr, _) = read_request(&mut sock).await;
            assert_eq!((key, version), (API_OFFSET_FETCH, 1));
            let mut e = Encoder::new();
            e.i32(1)
                .string("orders")
                .i32(2)
                .i32(0)
                .i64(-1)
                .nullable_string(None)
                .i16(0)
                .i32(1)
                .i64(42)
                .nullable_string(Some(""))
                .i16(0);
            respond(&mut sock, corr, e.into_bytes()).await;
        });
        let addr = format!("127.0.0.1:{port}");
        let mut client = KafkaClient::new(vec![addr.clone()], "test", None);
        let topics = client.metadata(&["orders".to_string()]).await.unwrap();
        assert_eq!(topics[0].partitions.len(), 2);
        assert_eq!(client.broker_addr(7).unwrap(), addr);
        client
            .offset_commit(&addr, "group-1", &[("orders".to_string(), 1, 42)])
            .await
            .unwrap();
        let committed = client
            .offset_fetch(
                &addr,
                "group-1",
                &[("orders".to_string(), 0), ("orders".to_string(), 1)],
            )
            .await
            .unwrap();
        assert_eq!(committed[&("orders".to_string(), 0)], None);
        assert_eq!(committed[&("orders".to_string(), 1)], Some(42));
        broker.await.unwrap();
    }

    /// SASL/PLAIN: the handshake names the mechanism and a refusal is an
    /// authentication error carrying the broker's message.
    #[tokio::test]
    async fn sasl_plain_refusal_is_an_authentication_error() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let broker = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let (key, _, corr, body) = read_request(&mut sock).await;
            assert_eq!(key, API_SASL_HANDSHAKE);
            assert_eq!(Decoder::new(&body).string().unwrap(), "PLAIN");
            let mut e = Encoder::new();
            e.i16(0).i32(1).string("PLAIN");
            respond(&mut sock, corr, e.into_bytes()).await;
            let (key, _, corr, body) = read_request(&mut sock).await;
            assert_eq!(key, API_SASL_AUTHENTICATE);
            assert_eq!(
                Decoder::new(&body).nullable_bytes().unwrap(),
                Some(&b"\0alice\0wrong"[..])
            );
            let mut e = Encoder::new();
            e.i16(58)
                .nullable_string(Some("Invalid username or password"))
                .bytes(b"");
            respond(&mut sock, corr, e.into_bytes()).await;
        });
        let mut client = KafkaClient::new(
            vec![addr],
            "test",
            Some(SaslCredentials {
                mechanism: SaslMechanism::Plain,
                username: "alice".to_string(),
                password: "wrong".to_string(),
            }),
        );
        let err = client.metadata(&[]).await.unwrap_err();
        assert!(
            matches!(&err, ClientError::Authentication(m) if m == "Invalid username or password"),
            "{err}"
        );
        broker.await.unwrap();
    }
}
