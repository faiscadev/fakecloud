//! Shared Amazon MSK record builders used by BOTH the direct restJson1 API
//! handlers (`service.rs`) and the CloudFormation `AWS::MSK::*` provisioner, so
//! a CFN-created cluster / configuration / replicator / VPC connection is
//! byte-for-byte identical to its direct-API equivalent -- the two paths cannot
//! diverge (#1766). Each builder mutates a per-account [`KafkaData`] in place
//! (inserting the same wire object the direct `Create*` handler stores) and
//! returns the new resource ARN; the caller layers on its own response shape /
//! container-backing on top.

use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::shared;
use crate::state::KafkaData;

fn now_iso() -> String {
    shared::now_iso()
}

/// A synthesized cluster `currentVersion` token (MSK's opaque `K...` form).
pub(crate) fn gen_version(seq: u64) -> String {
    format!("K{:013X}", 0x1_0000_0000_u64 + seq)
}

/// The default `BrokerNodeGroupInfo` MSK fills in when the caller supplies none.
pub(crate) fn default_bngi() -> Value {
    json!({
        "brokerAZDistribution": "DEFAULT",
        "clientSubnets": ["subnet-0123456789abcdef0", "subnet-0123456789abcdef1"],
        "instanceType": "kafka.m5.large",
        "securityGroups": ["sg-0123456789abcdef0"],
        "storageInfo": { "ebsStorageInfo": { "volumeSize": 100 } },
    })
}

/// The default `EncryptionInfo` MSK fills in when the caller supplies none.
pub(crate) fn default_encryption(region: &str, account: &str) -> Value {
    json!({
        "encryptionAtRest": {
            "dataVolumeKMSKeyId": format!(
                "arn:aws:kms:{region}:{account}:key/msk-default-key"
            )
        },
        "encryptionInTransit": { "clientBroker": "TLS", "inCluster": true },
    })
}

/// The ZooKeeper connect strings synthesized for a provisioned cluster.
pub(crate) fn zk_strings(cluster_arn: &str) -> (String, String) {
    let name = shared::cluster_name_from_arn(cluster_arn).unwrap_or("cluster");
    let region = shared::arn_region(cluster_arn).unwrap_or("us-east-1");
    let h = shared::hash_str(cluster_arn) & 0xffff_ffff;
    let mk = |port: u16| {
        (1..=3)
            .map(|z| format!("z-{z}.{name}.{h:x}.c2.kafka.{region}.amazonaws.com:{port}"))
            .collect::<Vec<_>>()
            .join(",")
    };
    (mk(2181), mk(2182))
}

/// Copy a `tags` JSON object (`{k: v}`) from `body` into the ARN-keyed tag map.
fn insert_tags(data: &mut KafkaData, arn: &str, body: &Value) {
    if let Some(tags) = body.get("tags").and_then(Value::as_object) {
        let mut map = std::collections::BTreeMap::new();
        for (k, val) in tags {
            if let Some(s) = val.as_str() {
                map.insert(k.clone(), s.to_string());
            }
        }
        if !map.is_empty() {
            data.tags.insert(arn.to_string(), map);
        }
    }
}

/// Build + insert an MSK cluster record from a `CreateCluster` / `CreateClusterV2`
/// body (camelCase members) and return its ARN. `v2` selects the CreateClusterV2
/// shape (a `serverless` or `provisioned` sub-block); a V1 body carries the flat
/// provisioned fields. The cluster is inserted `CREATING` -- the caller settles
/// it to `ACTIVE` (via `reconcile`) or backs it with a real Kafka container.
/// Returns `Err(message)` when the cluster name already exists in the region.
pub fn insert_cluster(
    data: &mut KafkaData,
    region: &str,
    account: &str,
    body: &Value,
    v2: bool,
) -> Result<String, String> {
    let name = body
        .get("clusterName")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();

    // A serverless V2 request carries a `serverless` block; otherwise the
    // cluster is provisioned (from `provisioned` in V2, or the flat fields in
    // V1).
    let serverless = v2 && body.get("serverless").is_some();
    let provisioned_src = if v2 {
        body.get("provisioned")
            .cloned()
            .unwrap_or_else(|| body.clone())
    } else {
        body.clone()
    };

    // Cluster names are unique within an account + region.
    if data.clusters.values().any(|c| {
        c.get("clusterName").and_then(Value::as_str) == Some(name.as_str())
            && c.get("clusterArn")
                .and_then(Value::as_str)
                .and_then(shared::arn_region)
                == Some(region)
    }) {
        return Err(format!(
            "A cluster with the name '{name}' already exists in this account and region."
        ));
    }

    let n = data.next_seq();
    let uuid = Uuid::new_v4().to_string();
    let arn = shared::cluster_arn(region, account, &name, &uuid, n);
    let version = gen_version(n);
    let (zk, zk_tls) = zk_strings(&arn);

    let mut cluster = Map::new();
    cluster.insert("clusterArn".into(), json!(arn));
    cluster.insert("clusterName".into(), json!(name));
    cluster.insert("state".into(), json!("CREATING"));
    cluster.insert("stateInfo".into(), json!({ "code": "NONE", "message": "" }));
    cluster.insert("creationTime".into(), json!(now_iso()));
    cluster.insert("currentVersion".into(), json!(version));

    if serverless {
        cluster.insert("_clusterType".into(), json!("SERVERLESS"));
        cluster.insert(
            "_serverless".into(),
            body.get("serverless")
                .cloned()
                .unwrap_or(json!({ "vpcConfigs": [] })),
        );
    } else {
        let kv = provisioned_src
            .get("kafkaVersion")
            .and_then(Value::as_str)
            .unwrap_or(shared::DEFAULT_KAFKA_VERSION)
            .to_string();
        let num = provisioned_src
            .get("numberOfBrokerNodes")
            .and_then(Value::as_i64)
            .unwrap_or(3);
        cluster.insert("_clusterType".into(), json!("PROVISIONED"));
        cluster.insert("numberOfBrokerNodes".into(), json!(num));
        cluster.insert(
            "brokerNodeGroupInfo".into(),
            provisioned_src
                .get("brokerNodeGroupInfo")
                .cloned()
                .unwrap_or_else(default_bngi),
        );
        cluster.insert(
            "currentBrokerSoftwareInfo".into(),
            json!({ "kafkaVersion": kv }),
        );
        cluster.insert(
            "enhancedMonitoring".into(),
            provisioned_src
                .get("enhancedMonitoring")
                .cloned()
                .unwrap_or(json!("DEFAULT")),
        );
        cluster.insert(
            "encryptionInfo".into(),
            provisioned_src
                .get("encryptionInfo")
                .cloned()
                .unwrap_or_else(|| default_encryption(region, account)),
        );
        cluster.insert(
            "storageMode".into(),
            provisioned_src
                .get("storageMode")
                .cloned()
                .unwrap_or(json!("LOCAL")),
        );
        cluster.insert("zookeeperConnectString".into(), json!(zk));
        cluster.insert("zookeeperConnectStringTls".into(), json!(zk_tls));
    }

    data.clusters.insert(arn.clone(), Value::Object(cluster));
    insert_tags(data, &arn, body);
    Ok(arn)
}

/// Build + insert an MSK configuration (revision 1) from a `CreateConfiguration`
/// body and return its ARN.
pub fn insert_configuration(
    data: &mut KafkaData,
    region: &str,
    account: &str,
    body: &Value,
) -> String {
    let name = body
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let description = body
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let server_properties = body
        .get("serverProperties")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    // MSK echoes back exactly the KafkaVersions the caller supplied; when
    // omitted the configuration carries an empty list (the provider asserts
    // `kafka_versions.# == 0` on a version-less configuration), NOT the whole
    // catalog.
    let kafka_versions: Vec<Value> = body
        .get("kafkaVersions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let n = data.next_seq();
    let uuid = Uuid::new_v4().to_string();
    let arn = shared::config_arn(region, account, &name, &uuid, n);
    let created = now_iso();
    let rev = json!({ "creationTime": created, "description": description, "revision": 1 });
    data.configurations.insert(
        arn.clone(),
        json!({
            "arn": arn,
            "creationTime": created,
            "description": description,
            "kafkaVersions": kafka_versions,
            "latestRevision": rev,
            "name": name,
            "state": "ACTIVE",
        }),
    );
    data.configuration_revisions.insert(
        arn.clone(),
        vec![json!({
            "creationTime": created,
            "description": description,
            "revision": 1,
            "serverProperties": server_properties,
        })],
    );
    arn
}

/// Build + insert an MSK replicator record from a `CreateReplicator` body and
/// return its ARN. Returns `Err(message)` on a duplicate replicator name.
pub fn insert_replicator(
    data: &mut KafkaData,
    region: &str,
    account: &str,
    body: &Value,
) -> Result<String, String> {
    let name = body
        .get("replicatorName")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if data.replicators.values().any(|r| {
        r.get("replicatorName").and_then(Value::as_str) == Some(name.as_str())
            && r.get("replicatorArn")
                .and_then(Value::as_str)
                .and_then(shared::arn_region)
                == Some(region)
    }) {
        return Err(format!(
            "A replicator with the name '{name}' already exists."
        ));
    }
    let n = data.next_seq();
    let uuid = Uuid::new_v4().to_string();
    let arn = shared::replicator_arn(region, account, &name, &uuid, n);
    let created = now_iso();
    let record = json!({
        "replicatorArn": arn,
        "replicatorName": name,
        "replicatorState": "CREATING",
        "replicatorResourceArn": arn,
        "creationTime": created,
        "currentVersion": gen_version(n),
        "isReplicatorReference": false,
        "serviceExecutionRoleArn": body.get("serviceExecutionRoleArn").cloned().unwrap_or(json!("")),
        "kafkaClusters": body.get("kafkaClusters").cloned().unwrap_or(json!([])),
        "replicationInfoList": body.get("replicationInfoList").cloned().unwrap_or(json!([])),
        "replicatorDescription": body.get("description").cloned().unwrap_or(json!("")),
    });
    data.replicators.insert(arn.clone(), record);
    insert_tags(data, &arn, body);
    Ok(arn)
}

/// Build + insert an MSK VPC connection record from a `CreateVpcConnection` body
/// and return its ARN.
pub fn insert_vpc_connection(
    data: &mut KafkaData,
    region: &str,
    account: &str,
    body: &Value,
) -> String {
    let n = data.next_seq();
    let uuid = Uuid::new_v4().to_string();
    let arn = shared::vpc_connection_arn(region, account, &uuid, n);
    let created = now_iso();
    let subnets = body.get("clientSubnets").cloned().unwrap_or(json!([]));
    let sgs = body.get("securityGroups").cloned().unwrap_or(json!([]));
    let auth = body.get("authentication").cloned().unwrap_or(json!(""));
    let vpc_id = body.get("vpcId").cloned().unwrap_or(json!(""));
    let target = body.get("targetClusterArn").cloned().unwrap_or(json!(""));
    let record = json!({
        "vpcConnectionArn": arn,
        "targetClusterArn": target,
        "state": "CREATING",
        "authentication": auth,
        "vpcId": vpc_id,
        "subnets": subnets,
        "securityGroups": sgs,
        "creationTime": created,
    });
    data.vpc_connections.insert(arn.clone(), record);
    insert_tags(data, &arn, body);
    arn
}
