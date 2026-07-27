//! Primitives shared across the Amazon MSK (`kafka`) handlers: ARN formatting,
//! deterministic broker-node / bootstrap-broker synthesis, the supported Kafka
//! version catalog, and timestamp helpers.

use serde_json::{json, Value};

/// FNV-1a hash for deterministic synthesis of ids / IPs from a cluster's ARN.
pub fn hash_str(s: &str) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in s.as_bytes() {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

pub fn now_iso() -> String {
    chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.3fZ")
        .to_string()
}

/// MSK cluster ARN: `arn:aws:kafka:{region}:{account}:cluster/{name}/{uuid}-{n}`.
pub fn cluster_arn(region: &str, account: &str, name: &str, uuid: &str, n: u64) -> String {
    format!("arn:aws:kafka:{region}:{account}:cluster/{name}/{uuid}-{n}")
}

/// MSK configuration ARN:
/// `arn:aws:kafka:{region}:{account}:configuration/{name}/{uuid}-{n}`.
pub fn config_arn(region: &str, account: &str, name: &str, uuid: &str, n: u64) -> String {
    format!("arn:aws:kafka:{region}:{account}:configuration/{name}/{uuid}-{n}")
}

/// MSK cluster-operation ARN, derived from the cluster ARN by swapping the
/// `:cluster/` resource prefix for `:cluster-operation/` and appending a unique
/// operation UUID (matching the real MSK cluster-operation ARN shape).
pub fn operation_arn_from_cluster(cluster_arn: &str, op_uuid: &str) -> String {
    let tail = cluster_arn.replacen(":cluster/", ":cluster-operation/", 1);
    format!("{tail}/{op_uuid}")
}

/// MSK replicator ARN:
/// `arn:aws:kafka:{region}:{account}:replicator/{name}/{uuid}-{n}`.
pub fn replicator_arn(region: &str, account: &str, name: &str, uuid: &str, n: u64) -> String {
    format!("arn:aws:kafka:{region}:{account}:replicator/{name}/{uuid}-{n}")
}

/// MSK VPC-connection ARN. Real MSK embeds the TARGET cluster's account, name,
/// and a connection UUID in the resource part:
/// `arn:aws:kafka:{region}:{account}:vpc-connection/{targetAccount}/{targetClusterName}/{uuid}-{n}`.
/// `target_account` / `target_cluster_name` are derived from the target cluster
/// ARN (falling back to the caller's own account / `cluster` when it can't be
/// parsed), so the provider's ARN-shape assertion matches.
pub fn vpc_connection_arn(
    region: &str,
    account: &str,
    target_account: &str,
    target_cluster_name: &str,
    uuid: &str,
    n: u64,
) -> String {
    format!(
        "arn:aws:kafka:{region}:{account}:vpc-connection/{target_account}/{target_cluster_name}/{uuid}-{n}"
    )
}

/// The account id embedded in a `kafka` ARN (the 5th colon-delimited field).
pub fn arn_account(arn: &str) -> Option<&str> {
    arn.split(':').nth(4)
}

/// The region embedded in a `kafka` ARN (the 4th colon-delimited field). Used
/// to scope `List*` results to the request's region without a schema change.
pub fn arn_region(arn: &str) -> Option<&str> {
    arn.split(':').nth(3)
}

/// The cluster name embedded in a cluster ARN (`.../cluster/{name}/{uuid}-{n}`).
pub fn cluster_name_from_arn(arn: &str) -> Option<&str> {
    arn.rsplit_once(":cluster/")
        .and_then(|(_, tail)| tail.split('/').next())
}

/// The supported Kafka versions Amazon MSK offers, newest first, all `ACTIVE`.
///
/// This mirrors the real `ListKafkaVersions` catalog (the current Apache Kafka
/// and KRaft/tiered-storage variants plus the still-selectable older lines) so
/// the `aws_msk_kafka_version` data source -- which filters `ListKafkaVersions`
/// by an exact `version` (e.g. `2.4.1.1`) and reads its `status` -- resolves,
/// and so a cluster can be created at, and upgraded between, any of these.
pub const KAFKA_VERSIONS: &[&str] = &[
    "3.9.x.kraft",
    "3.9.x",
    "3.8.x.kraft",
    "3.8.x",
    "3.7.x.kraft",
    "3.7.x",
    "3.6.0",
    "3.5.1",
    "3.4.0",
    "3.3.2",
    "3.3.1",
    "3.2.0",
    "3.1.1",
    "2.8.2.tiered",
    "2.8.1",
    "2.8.0",
    "2.7.2",
    "2.7.1",
    "2.7.0",
    "2.6.3",
    "2.6.2",
    "2.6.1",
    "2.6.0",
    "2.5.1",
    "2.4.1.1",
    "2.4.1",
    "2.3.1",
    "2.2.1",
    "2.1.0",
    "1.1.1",
];

/// The `ListKafkaVersions` payload: each supported version with `ACTIVE` status.
pub fn kafka_versions_list() -> Vec<Value> {
    KAFKA_VERSIONS
        .iter()
        .map(|v| json!({ "version": v, "status": "ACTIVE" }))
        .collect()
}

/// Compatible upgrade targets for a source version: every strictly-newer
/// supported version. `GetCompatibleKafkaVersions` returns one entry per known
/// source (or just the requested cluster's source when a cluster is given).
pub fn compatible_versions_for(source: &str) -> Vec<Value> {
    // Preserve the catalog's newest-first order but restrict to versions that
    // sort as valid upgrade targets (everything listed before `source`).
    let targets: Vec<Value> = KAFKA_VERSIONS
        .iter()
        .take_while(|v| **v != source)
        .map(|v| json!(v))
        .collect();
    vec![json!({ "sourceVersion": source, "targetVersions": targets })]
}

/// The default Kafka version MSK selects when a caller omits one.
pub const DEFAULT_KAFKA_VERSION: &str = "3.6.0";

/// Synthesize the `BrokerNodeInfo`-shaped `NodeInfoList` for a provisioned
/// cluster with `num_brokers` broker nodes, deriving stable ids/IPs/endpoints
/// from the cluster ARN. The control-plane view; when a real Kafka broker backs
/// the cluster, the service overrides node 1's endpoint with the live one.
pub fn synthesize_nodes(cluster_arn: &str, num_brokers: i64, kafka_version: &str) -> Vec<Value> {
    let name = cluster_name_from_arn(cluster_arn).unwrap_or("cluster");
    let region = arn_region(cluster_arn).unwrap_or("us-east-1");
    let h = hash_str(cluster_arn);
    let account = cluster_arn.split(':').nth(4).unwrap_or("000000000000");
    let mut nodes = Vec::new();
    for id in 1..=num_brokers.max(0) {
        let host = format!(
            "b-{id}.{name}.{h:x}.c2.kafka.{region}.amazonaws.com",
            h = h & 0xffff_ffff
        );
        let ip = format!(
            "10.{}.{}.{}",
            (h >> 16) & 0xff,
            (h >> 8) & 0xff,
            (h.wrapping_add(id as u64)) & 0xff
        );
        nodes.push(json!({
            "addedToClusterTime": now_iso(),
            "instanceType": "kafka.m5.large",
            "nodeARN": format!(
                "arn:aws:kafka:{region}:{account}:cluster/{name}/broker/{id}"
            ),
            "nodeType": "BROKER",
            "brokerNodeInfo": {
                "attachedENIId": format!("eni-{:012x}", h.wrapping_add(id as u64) & 0xffff_ffff_ffff),
                "brokerId": id as f64,
                "clientSubnet": format!("subnet-{:017x}", h.wrapping_add(id as u64)),
                "clientVpcIpAddress": ip,
                "currentBrokerSoftwareInfo": { "kafkaVersion": kafka_version },
                "endpoints": [host],
            },
        }));
    }
    nodes
}

/// The comma-joined bootstrap-broker connection string for `num_brokers` nodes
/// on the given `port`, derived from the cluster ARN. The control-plane-only
/// fallback used when no real Kafka broker backs the cluster (no runtime, or a
/// serverless cluster); a live broker returns its real reachable `host:port`.
pub fn bootstrap_broker_string(cluster_arn: &str, num_brokers: i64, port: u16) -> String {
    let name = cluster_name_from_arn(cluster_arn).unwrap_or("cluster");
    let region = arn_region(cluster_arn).unwrap_or("us-east-1");
    let h = hash_str(cluster_arn) & 0xffff_ffff;
    (1..=num_brokers.max(1))
        .map(|id| format!("b-{id}.{name}.{h:x}.c2.kafka.{region}.amazonaws.com:{port}"))
        .collect::<Vec<_>>()
        .join(",")
}
